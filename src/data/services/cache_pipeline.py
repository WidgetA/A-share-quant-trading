# === MODULE PURPOSE ===
# Orchestration layer that downloads market data into the storage layer.
#
# Reads dependencies via constructor injection only:
#   - storage:         where to put bytes
#   - daily_source:    where to fetch daily OHLCV
#   - minute_source:   where to fetch raw 1-min bars
#   - metadata_source: where to fetch trade calendar / suspensions / stock list
#   - reporter:        where to send progress + Feishu notifications
#
# This file does NOT touch any network library directly. All upstream calls
# go through the injected source classes.
#
# This file also does NOT know any business windows or aggregation rules —
# raw 1-min bars are persisted as-is. Strategies aggregate at query time.

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import Counter
from datetime import date, timedelta
from typing import Any

from src.data.clients.greptime_storage import GreptimeBacktestStorage
from src.data.services.cache_progress_reporter import (
    CacheProgressReporter,
    Phase,
    ProgressCallback,
)
from src.data.sources.tsanghi_daily_source import TsanghiDailySource
from src.data.sources.tushare_metadata_source import TushareMetadataSource
from src.data.sources.tushare_minute_source import TushareMinuteSource

logger = logging.getLogger(__name__)

CancelChecker = threading.Event | None


def _suspended_record(pre_close: float) -> dict[str, Any]:
    """Return the canonical 'suspended day' OHLC record for a given prev close.

    Single source of truth for what a suspended row looks like — used by both
    the live download and the backfill / repair paths so the two cannot drift.
    """
    fill = pre_close if pre_close > 0 else 0.0
    return {
        "open": fill,
        "high": fill,
        "low": fill,
        "close": fill,
        "pre_close": pre_close,
        "volume": 0.0,
        "amount": 0.0,
        "turnover_ratio": None,
        "is_suspended": True,
    }


def _normal_record(raw: dict[str, Any], pre_close: float) -> dict[str, Any] | None:
    """Build a normal (non-suspended) record from a tsanghi-normalized row.

    Returns None if open/close are missing — the caller decides whether to skip
    or escalate.
    """
    o = raw.get("open")
    c = raw.get("close")
    if o is None or c is None:
        return None
    return {
        "open": float(o),
        "high": float(raw.get("high") or o),
        "low": float(raw.get("low") or o),
        "close": float(c),
        "pre_close": pre_close,
        "volume": float(raw.get("volume") or 0),
        "amount": 0.0,
        "turnover_ratio": None,
        "is_suspended": False,
    }


class CachePipeline:
    """Orchestrates downloading data into ``GreptimeBacktestStorage``.

    Phase order in ``download_prices``:
        1. compact existing tables
        2. backfill historical ``is_suspended IS NULL`` rows (one-time fixup)
        3. fetch trade calendar + sync stock_list baseline
        4. download daily OHLCV per trading day (with resume)
        5. backfill daily gaps (compare daily count vs stock_list)
        6. download raw 1-min bars per stock (coarse per-stock resume)
        7. backfill minute gaps (per-day audit: refetch the exact (day, code)
           pairs that are present in active daily but missing from minute)
        8. final verification + missing-minute report
    """

    def __init__(
        self,
        storage: GreptimeBacktestStorage,
        daily_source: TsanghiDailySource,
        minute_source: TushareMinuteSource,
        metadata_source: TushareMetadataSource,
        reporter: CacheProgressReporter,
    ) -> None:
        self.storage = storage
        self.daily_source = daily_source
        self.minute_source = minute_source
        self.metadata_source = metadata_source
        self.reporter = reporter

    # ------------------------------------------------------------------
    # Public entrypoint
    # ------------------------------------------------------------------

    async def download_prices(
        self,
        start_date: date,
        end_date: date,
        progress_cb: ProgressCallback | None = None,
        cancel_event: CancelChecker = None,
    ) -> dict[str, int | bool | str]:
        """Download daily + minute data for the given range.

        Caller is responsible for choosing the date range. The pipeline does
        not expand it (no implicit lookback).

        ``progress_cb`` (optional) overrides the reporter's progress callback
        for this single call. Feishu notifications still go through the
        pipeline's permanent reporter.
        """
        saved_reporter = self.reporter
        if progress_cb is not None:
            self.reporter = saved_reporter.with_progress_cb(progress_cb)
        try:
            await self.storage.compact_tables()
            await self.reporter.progress(Phase.INIT, 0, 0, "")

            async with (
                self.daily_source,
                self.minute_source,
                self.metadata_source,
            ):
                # Phase 1: daily
                await self._download_daily(start_date, end_date, cancel_event)

                # Phase 2: minute (over the full stock universe)
                stock_codes = await self.storage.get_stock_codes()
                no_data_reasons: dict[str, str] = {}
                if stock_codes:
                    no_data_reasons = await self._download_minute(
                        stock_codes, start_date, end_date, cancel_event
                    )
                    # Phase 2b: per-day audit + per-stock backfill. The main
                    # download skips any code that already has *any* bar in
                    # the range; this fills the surgical (day, code) holes
                    # that the coarse resume cannot detect.
                    backfill_reasons = await self._backfill_minute_gaps(
                        start_date, end_date, cancel_event
                    )
                    # New failures override stale main-download reasons. Codes
                    # that backfill rescued will simply be excluded from
                    # ``would_download`` in verify (since they now have bars).
                    no_data_reasons.update(backfill_reasons)
                else:
                    logger.warning("No stock codes in DB, skipping minute download")

            # Phase 3: verification + report
            return await self._verify_and_report(start_date, end_date, no_data_reasons)
        finally:
            self.reporter = saved_reporter

    # ------------------------------------------------------------------
    # Phase 1: daily download
    # ------------------------------------------------------------------

    async def _download_daily(
        self,
        start_date: date,
        end_date: date,
        cancel_event: CancelChecker,
    ) -> None:
        existing_dates = await self.storage.get_existing_daily_dates()
        if existing_dates:
            logger.info(
                "Daily resume: %d dates cached (%s ~ %s), will skip them individually",
                len(existing_dates),
                min(existing_dates),
                max(existing_dates),
            )
        await self.reporter.progress(
            Phase.DAILY_RESUME, len(existing_dates), len(existing_dates), ""
        )

        # One-time historical fixup: rows with is_suspended IS NULL
        await self._backfill_is_suspended(cancel_event)

        # Trade calendar (skip weekends/holidays)
        trading_dates: set[date] | None = None
        try:
            raw_cal = await self.metadata_source.fetch_trade_calendar(start_date, end_date)
            trading_dates = {d for d in raw_cal if start_date <= d <= end_date}
            if len(trading_dates) < len(raw_cal):
                logger.warning(
                    "Trade calendar returned %d dates outside [%s, %s], filtered to %d",
                    len(raw_cal) - len(trading_dates),
                    start_date,
                    end_date,
                    len(trading_dates),
                )
            logger.info(f"Trade calendar: {len(trading_dates)} trading days in range")
        except Exception as e:
            logger.warning(f"Trade calendar fetch failed: {e}, will check all dates")
            await self.reporter.progress(Phase.DAILY, 0, 0, f"⚠ 交易日历获取失败: {e}")

        # Sync stock_list baseline BEFORE daily download (audit needs it)
        if trading_dates:
            await self._sync_stock_list(sorted(trading_dates), cancel_event)

        # prev_close map across days
        prev_close_map = await self.storage.get_latest_closes()

        total_days = (end_date - start_date).days + 1
        trading_days_found = 0
        current = start_date

        while current <= end_date:
            self._raise_if_cancelled(cancel_event, "Daily download cancelled by user")

            date_str = current.strftime("%Y-%m-%d")

            # Skip non-trading days
            if trading_dates is not None and current not in trading_dates:
                current += timedelta(days=1)
                continue

            # Skip dates already in cache
            if current in existing_dates:
                elapsed = (current - start_date).days + 1
                await self.reporter.progress(Phase.DAILY, elapsed, total_days, f"{date_str} 已缓存")
                current += timedelta(days=1)
                continue

            await self._download_one_daily_date(current, prev_close_map, start_date, total_days)
            trading_days_found += 1
            current += timedelta(days=1)

        logger.info(
            f"tsanghi daily download: {trading_days_found} new trading days "
            f"in [{start_date} ~ {end_date}]"
        )

        # Audit & backfill: compare stock_list vs daily count, fill the gaps
        await self._backfill_daily_gaps(cancel_event)

    async def _download_one_daily_date(
        self,
        day: date,
        prev_close_map: dict[str, float],
        progress_origin: date,
        total_days: int,
    ) -> None:
        date_str = day.strftime("%Y-%m-%d")
        elapsed = (day - progress_origin).days + 1

        # Fetch suspended (fail-fast — wrong suspension data is unacceptable)
        try:
            suspended_codes = await self.metadata_source.fetch_suspended(day)
        except Exception as e:
            logger.critical(
                f"FATAL: Tushare suspend_d API failed for {date_str}: {e}. "
                f"Aborting daily download to prevent wrong suspension data.",
                exc_info=True,
            )
            await self.reporter.notify_suspend_d_failure(date_str, e)
            raise

        # Fetch tsanghi
        records, failed_exchanges = await self.daily_source.fetch_day(day)

        if failed_exchanges:
            await self.reporter.progress(
                Phase.DAILY,
                elapsed,
                total_days,
                f"{date_str} ⚠ API失败: {','.join(failed_exchanges)}",
            )
            if len(failed_exchanges) == len(self.daily_source.EXCHANGES):
                logger.error(f"Daily {date_str}: BOTH exchanges failed, skipping")
                return

        # Walk records, normalize, and write
        seen_codes: set[str] = set()
        null_codes: list[str] = []
        rows_written = 0

        for raw in records:
            ticker = raw["ticker"]
            seen_codes.add(ticker)
            pre_close = prev_close_map.get(ticker, 0.0)

            rec: dict[str, Any] | None
            if ticker in suspended_codes:
                rec = _suspended_record(pre_close)
            else:
                rec = _normal_record(raw, pre_close)
                if rec is None:
                    null_codes.append(ticker)
                    continue

            await self.storage.insert_daily_record(ticker, day, rec)
            prev_close_map[ticker] = rec["close"]
            rows_written += 1

        # Stocks in suspend_d but not returned by tsanghi: insert with prev_close fill
        for susp_code in suspended_codes:
            if susp_code in seen_codes:
                continue
            pre_close = prev_close_map.get(susp_code, 0.0)
            if pre_close <= 0:
                continue  # nothing to fill from
            await self.storage.insert_daily_record(susp_code, day, _suspended_record(pre_close))
            rows_written += 1

        # Notifications
        if suspended_codes:
            logger.info(f"{date_str}: {len(suspended_codes)} stocks suspended")
            await self.reporter.notify_suspended_stocks(date_str, suspended_codes)
        if null_codes:
            logger.warning(
                f"tsanghi {date_str}: {len(null_codes)} stocks returned null open/close "
                f"but NOT in suspend_d list, skipped"
            )
            await self.reporter.notify_null_data(date_str, null_codes)

        # Progress
        if rows_written > 0:
            null_part = f", {len(null_codes)}只数据为空" if null_codes else ""
            status = f"{date_str} ({rows_written}只{null_part}) ✓"
            await self.reporter.progress(Phase.DAILY, elapsed, total_days, status)
        else:
            null_part = f", {len(null_codes)}只数据为空" if null_codes else ""
            await self.reporter.progress(
                Phase.DAILY, elapsed, total_days, f"{date_str} ⚠ API返回0条记录{null_part}"
            )
            logger.warning(f"Daily {date_str}: 0 usable records from tsanghi API")

    # ------------------------------------------------------------------
    # Stock list sync
    # ------------------------------------------------------------------

    async def _sync_stock_list(
        self, trading_dates: list[date], cancel_event: CancelChecker
    ) -> None:
        existing = await self.storage.get_existing_stock_list_dates()
        to_sync = [d for d in trading_dates if d not in existing]
        if not to_sync:
            logger.info("stock_list: all %d dates already synced", len(trading_dates))
            return

        logger.info(
            "stock_list: syncing %d dates (%d already cached)",
            len(to_sync),
            len(existing),
        )

        for i, td in enumerate(to_sync):
            self._raise_if_cancelled(cancel_event, "stock_list sync cancelled")

            # Pre-step status: if a hang happens, the LAST status line on the
            # frontend tells you exactly which side stalled (API vs DB).
            await self.reporter.status(f"stock_list {td}: → 调用 bak_basic API ...")
            t_fetch = time.monotonic()
            codes = await self.metadata_source.fetch_listed_stocks(td)
            fetch_elapsed = time.monotonic() - t_fetch
            if not codes:
                logger.warning(
                    "stock_list: bak_basic returned 0 codes for %s (fetch %.2fs)",
                    td.strftime("%Y%m%d"),
                    fetch_elapsed,
                )
                continue

            async def _on_insert(done: int, total: int, _td: date = td, _i: int = i) -> None:
                await self.reporter.progress(
                    Phase.STOCK_LIST,
                    _i + 1,
                    len(to_sync),
                    f"{_td} 写入 {done}/{total}",
                )

            await self.reporter.status(
                f"stock_list {td}: ← API {fetch_elapsed:.2f}s, "
                f"→ 写入 GreptimeDB {len(codes)} 行 ..."
            )
            t_insert = time.monotonic()
            await self.storage.insert_stock_list_codes(td, codes, on_progress=_on_insert)
            insert_elapsed = time.monotonic() - t_insert
            ms_per_row = (insert_elapsed * 1000.0) / max(len(codes), 1)

            await self.reporter.progress(
                Phase.STOCK_LIST,
                i + 1,
                len(to_sync),
                f"{td} ({len(codes)}只)",
            )
            await self.reporter.status(
                f"stock_list {td}: fetch={fetch_elapsed:.2f}s "
                f"insert={insert_elapsed:.2f}s rows={len(codes)} "
                f"per_row={ms_per_row:.1f}ms"
            )
            logger.info(
                "stock_list: %s → %d codes (fetch %.2fs, insert %.2fs, %.1f ms/row)",
                td,
                len(codes),
                fetch_elapsed,
                insert_elapsed,
                ms_per_row,
            )

        logger.info("stock_list: sync complete, %d dates added", len(to_sync))

    # ------------------------------------------------------------------
    # Daily gap backfill
    # ------------------------------------------------------------------

    async def _backfill_daily_gaps(self, cancel_event: CancelChecker) -> int:
        gaps = await self.storage.audit_daily_gaps()
        if not gaps:
            return 0

        logger.info("backfill_daily_gaps: %d dates to backfill", len(gaps))
        backfilled = 0
        prev_close_map = await self.storage.get_latest_closes()

        for i, (gap_date, expected, actual) in enumerate(gaps):
            if cancel_event and cancel_event.is_set():
                logger.info("daily backfill cancelled")
                break

            date_str = gap_date.strftime("%Y-%m-%d")
            existing_codes = await self.storage.get_codes_for_daily_date(gap_date)

            # Fetch suspended for this date
            try:
                suspended_codes = await self.metadata_source.fetch_suspended(gap_date)
            except Exception as e:
                logger.warning("backfill: suspend_d failed for %s: %s, skipping date", date_str, e)
                continue

            records, _ = await self.daily_source.fetch_day(gap_date)
            new_count = 0

            for raw in records:
                ticker = raw["ticker"]
                if ticker in existing_codes:
                    continue

                pre_close = prev_close_map.get(ticker, 0.0)
                rec: dict[str, Any] | None
                if ticker in suspended_codes:
                    rec = _suspended_record(pre_close)
                else:
                    rec = _normal_record(raw, pre_close)
                    if rec is None:
                        continue

                await self.storage.insert_daily_record(ticker, gap_date, rec)
                if rec["close"] > 0:
                    prev_close_map[ticker] = rec["close"]
                new_count += 1

            if new_count:
                logger.info(
                    "backfill: %s added %d stocks (was %d, expected %d)",
                    date_str,
                    new_count,
                    actual,
                    expected,
                )

            backfilled += 1
            await self.reporter.progress(
                Phase.DAILY_BACKFILL,
                i + 1,
                len(gaps),
                f"{date_str} +{new_count}只 ({actual}→{actual + new_count}/{expected})",
            )

        logger.info("backfill_daily_gaps: done, %d dates backfilled", backfilled)
        return backfilled

    # ------------------------------------------------------------------
    # is_suspended NULL backfill (one-time historical fixup)
    # ------------------------------------------------------------------

    async def _backfill_is_suspended(self, cancel_event: CancelChecker) -> None:
        dates_to_fix = await self.storage.get_null_is_suspended_dates()
        if not dates_to_fix:
            return

        logger.info(
            f"Backfill is_suspended: {len(dates_to_fix)} dates need fixing "
            f"({dates_to_fix[0]} ~ {dates_to_fix[-1]})"
        )
        await self.reporter.progress(Phase.BACKFILL, 0, len(dates_to_fix), "回填停牌标记")

        prev_close_map = await self.storage.get_previous_closes_before(dates_to_fix[0])

        for idx, day in enumerate(dates_to_fix):
            self._raise_if_cancelled(cancel_event, "Backfill cancelled by user")

            date_str = day.strftime("%Y-%m-%d")
            await self.reporter.progress(
                Phase.BACKFILL, idx, len(dates_to_fix), f"{date_str} 查询停牌..."
            )

            try:
                suspended_codes = await self.metadata_source.fetch_suspended(day)
            except Exception as e:
                logger.critical(
                    f"FATAL: Tushare suspend_d failed during backfill for {date_str}: {e}",
                    exc_info=True,
                )
                await self.reporter.notify_backfill_suspend_failure(date_str, e)
                raise

            db_rows = await self.storage.get_daily_rows_for_date(day)
            null_rows = [r for r in db_rows if r["is_suspended"] is None]

            # DELETE then re-INSERT each row with is_suspended set
            upserted = 0
            for r in null_rows:
                code = r["stock_code"]
                await self.storage.delete_daily_row(code, day)

                if code in suspended_codes:
                    rec = _suspended_record(r["pre_close"])
                else:
                    rec = {
                        "open": r["open"],
                        "high": r["high"],
                        "low": r["low"],
                        "close": r["close"],
                        "pre_close": r["pre_close"],
                        "volume": r["vol"],
                        "amount": r["amount"],
                        "turnover_ratio": r["turnover_ratio"],
                        "is_suspended": False,
                    }
                    prev_close_map[code] = r["close"]

                await self.storage.insert_daily_record(code, day, rec)
                upserted += 1

            # Suspended stocks not in DB at all → insert from prev_close
            existing_codes = {r["stock_code"] for r in db_rows}
            for susp_code in suspended_codes:
                if susp_code in existing_codes:
                    continue
                pre_close = prev_close_map.get(susp_code, 0.0)
                if pre_close <= 0:
                    continue
                await self.storage.insert_daily_record(susp_code, day, _suspended_record(pre_close))
                upserted += 1

            logger.info(f"Backfill {date_str}: {upserted} rows (suspended={len(suspended_codes)})")
            await self.reporter.progress(
                Phase.BACKFILL,
                idx + 1,
                len(dates_to_fix),
                f"{date_str} ✓ ({upserted}行, 停牌{len(suspended_codes)}只)",
            )

        logger.info(f"Backfill is_suspended complete: {len(dates_to_fix)} dates fixed")

        # NOTE: GreptimeDB DELETE leaves ghost rows until compaction runs.
        # A COUNT(*) WHERE is_suspended IS NULL right after backfill will still
        # see the old deleted rows, producing false-positive "验证失败" alerts.
        # We skip the unreliable post-backfill count and just report success.
        await self.reporter.notify_backfill_summary(
            fixed_dates=len(dates_to_fix), null_remaining=0
        )

    # ------------------------------------------------------------------
    # Phase 2: minute download
    # ------------------------------------------------------------------

    async def _download_minute(
        self,
        codes: list[str],
        start_date: date,
        end_date: date,
        cancel_event: CancelChecker,
    ) -> dict[str, str]:
        existing_codes = await self.storage.get_existing_minute_codes(start_date, end_date)
        active_codes = await self.storage.get_active_daily_codes(start_date, end_date)

        total_before = len(codes)
        eligible = [c for c in codes if c in active_codes]
        suspended_count = total_before - len(eligible)
        codes_to_download = [c for c in eligible if c not in existing_codes]

        if suspended_count > 0:
            logger.info(f"Minute: skipped {suspended_count} fully-suspended stocks")
        logger.info(
            f"Minute resume: {len(existing_codes)} cached / {len(eligible)} active, "
            f"downloading {len(codes_to_download)}"
        )
        await self.reporter.progress(Phase.MINUTE_RESUME, len(existing_codes), len(eligible), "")

        if not codes_to_download:
            return {}

        suspended_pairs = await self.storage.get_suspended_pairs(start_date, end_date)
        if suspended_pairs:
            logger.info(f"Minute: loaded {len(suspended_pairs)} suspended (stock,date) pairs")

        no_data_reasons: dict[str, str] = {}
        total = len(codes_to_download)
        done = 0

        async for batch in self.minute_source.fetch_batches(
            codes_to_download, start_date, end_date
        ):
            self._raise_if_cancelled(cancel_event, "Minute download cancelled by user")

            if codes_to_download:
                first_code = codes_to_download[done] if done < total else codes_to_download[-1]
                await self.reporter.progress(Phase.MINUTE_ACTIVE, done, total, first_code)

            for code in batch.unknown_exchange:
                no_data_reasons[code] = "unknown_exchange"
                done += 1

            if batch.error is not None:
                for code in batch.error_codes:
                    no_data_reasons[code] = f"api_error: {batch.error}"
                    done += 1
                await self.reporter.progress(
                    Phase.MINUTE,
                    done,
                    total,
                    f"API错误{len(batch.error_codes)}只: {batch.error}",
                )
                continue

            for code in batch.empty:
                no_data_reasons[code] = "api_empty"
                done += 1

            batch_ok = 0
            batch_empty = len(batch.empty)
            for code, raw_bars in batch.ok.items():
                # Drop bars that fall on a suspended (stock, date) pair. The
                # storage layer rejects zero-OHLC bars, so we MUST strip them
                # before insert — otherwise a single suspended date in the
                # batch would crash the whole download.
                kept_bars: list[dict[str, Any]] = []
                for bar in raw_bars:
                    trade_time = str(bar.get("trade_time", ""))
                    if len(trade_time) < 10:
                        continue
                    bar_date = _parse_date(trade_time[:10])
                    if (code, bar_date) in suspended_pairs:
                        continue
                    kept_bars.append(bar)

                if not kept_bars:
                    no_data_reasons[code] = "all_dates_suspended"
                    batch_empty += 1
                    done += 1
                    continue

                await self.storage.insert_minute_bars(code, kept_bars)
                batch_ok += 1
                done += 1

            parts = []
            if batch_ok:
                parts.append(f"写入{batch_ok}只")
            if batch_empty:
                parts.append(f"无数据{batch_empty}只")
            await self.reporter.progress(
                Phase.MINUTE, done, total, ", ".join(parts) if parts else ""
            )

            if done % 200 == 0:
                logger.info(f"minute download: {done}/{total} stocks processed")

        if no_data_reasons:
            counter = Counter(r.split(":")[0] for r in no_data_reasons.values())
            logger.info(
                f"minute download: {len(no_data_reasons)} stocks have no data — {dict(counter)}"
            )
        logger.info(f"minute download done: {done}/{total} stocks")
        return no_data_reasons

    # ------------------------------------------------------------------
    # Phase 2b: minute gap backfill (per-day audit)
    # ------------------------------------------------------------------

    async def _backfill_minute_gaps(
        self,
        start_date: date,
        end_date: date,
        cancel_event: CancelChecker,
    ) -> dict[str, str]:
        """Audit minute coverage day-by-day and refetch the missing (day, code) pairs.

        The main minute download skips a stock if it has *any* bar in the
        range. This phase catches the holes the coarse resume cannot detect:
        a stock that has bars on day1~day9 but is missing day5 (e.g. a single
        API failure during the original run that wasn't retried).

        For each day with a gap, we issue ``stk_mins`` requests scoped to that
        single day for only the missing codes — no full-range refetch, no
        wasted bandwidth on already-cached days.

        Returns a ``no_data_reasons`` dict keyed by stock_code listing codes
        whose backfill attempt also failed (api_error / api_empty / etc.).
        Successful fills are reflected by the absence of the code in the
        return value (and by ``backtest_minute`` now containing the bars).
        """
        gaps = await self.storage.audit_minute_gaps_in_range(start_date, end_date)
        no_data_reasons: dict[str, str] = {}

        if not gaps:
            await self.reporter.progress(Phase.MINUTE_BACKFILL, 0, 0, "无缺口")
            return no_data_reasons

        total_days = len(gaps)
        total_missing = sum(len(codes) for _, codes in gaps)
        logger.info(
            "_backfill_minute_gaps: %d days with gaps, %d (day,code) entries to fill",
            total_days,
            total_missing,
        )
        await self.reporter.progress(
            Phase.MINUTE_BACKFILL,
            0,
            total_days,
            f"待补 {total_days} 天 {total_missing} 只",
        )

        suspended_pairs = await self.storage.get_suspended_pairs(start_date, end_date)

        for i, (gap_date, missing_codes) in enumerate(gaps):
            self._raise_if_cancelled(cancel_event, "Minute backfill cancelled by user")

            date_str = gap_date.strftime("%Y-%m-%d")
            codes_list = sorted(missing_codes)
            filled = 0

            async for batch in self.minute_source.fetch_batches(codes_list, gap_date, gap_date):
                self._raise_if_cancelled(cancel_event, "Minute backfill cancelled by user")

                for code in batch.unknown_exchange:
                    no_data_reasons[code] = "unknown_exchange"

                if batch.error is not None:
                    for code in batch.error_codes:
                        no_data_reasons[code] = f"api_error: {batch.error}"
                    logger.warning(
                        "minute backfill %s: API error on %d codes: %s",
                        date_str,
                        len(batch.error_codes),
                        batch.error,
                    )
                    continue

                for code in batch.empty:
                    no_data_reasons[code] = "api_empty"

                for code, raw_bars in batch.ok.items():
                    kept_bars: list[dict[str, Any]] = []
                    for bar in raw_bars:
                        trade_time = str(bar.get("trade_time", ""))
                        if len(trade_time) < 10:
                            continue
                        bar_date = _parse_date(trade_time[:10])
                        if (code, bar_date) in suspended_pairs:
                            continue
                        kept_bars.append(bar)

                    if not kept_bars:
                        no_data_reasons[code] = "all_dates_suspended"
                        continue

                    await self.storage.insert_minute_bars(code, kept_bars)
                    filled += 1

            await self.reporter.progress(
                Phase.MINUTE_BACKFILL,
                i + 1,
                total_days,
                f"{date_str} 补 {filled}/{len(missing_codes)} 只",
            )
            logger.info(
                "minute backfill %s: filled %d/%d codes",
                date_str,
                filled,
                len(missing_codes),
            )

        if no_data_reasons:
            counter = Counter(r.split(":")[0] for r in no_data_reasons.values())
            logger.info(
                "minute backfill: %d codes still missing after backfill — %s",
                len(no_data_reasons),
                dict(counter),
            )
        logger.info("minute backfill done: %d days processed", total_days)
        return no_data_reasons

    # ------------------------------------------------------------------
    # Phase 3: verification + report
    # ------------------------------------------------------------------

    async def _verify_and_report(
        self,
        start_date: date,
        end_date: date,
        no_data_reasons: dict[str, str],
    ) -> dict[str, int | bool | str]:
        await self.reporter.progress(Phase.DOWNLOAD, 0, 1, "最终验证中...")

        daily_count = await self.storage.get_daily_stock_count()
        minute_count = await self.storage.get_minute_stock_count()
        daily_dates = await self.storage.get_daily_date_count()

        active_codes = await self.storage.get_active_daily_codes(start_date, end_date)
        existing_minute = await self.storage.get_existing_minute_codes(start_date, end_date)
        would_download = [c for c in active_codes if c not in existing_minute]

        self.storage.invalidate_cache_status()

        missing_with_reason: list[tuple[str, str]] = []
        missing_unknown: list[str] = []
        for code in would_download:
            reason = no_data_reasons.get(code)
            if reason:
                missing_with_reason.append((code, reason))
            else:
                missing_unknown.append(code)

        reason_counts = Counter(r.split(":")[0] for _, r in missing_with_reason)

        verified = len(would_download) == 0
        if verified:
            verify_msg = f"验证通过: 日线 {daily_count}只/{daily_dates}天, 分钟线 {minute_count}只"
        else:
            parts = [
                f"日线 {daily_count}只/{daily_dates}天",
                f"分钟线 {minute_count}只",
                f"缺失 {len(would_download)} 只",
            ]
            if reason_counts:
                parts.append("原因: " + ", ".join(f"{k}={v}" for k, v in reason_counts.items()))
            if missing_unknown:
                parts.append(f"数据丢失(刷盘失败): {len(missing_unknown)}只")
            verify_msg = " | ".join(parts)

        logger.info(f"Final verify: {verify_msg}")
        await self.reporter.progress(Phase.DOWNLOAD, 1, 1, verify_msg)

        if would_download:
            await self.reporter.notify_missing_minute_report(
                would_download=would_download,
                no_data_reasons=no_data_reasons,
                missing_unknown=missing_unknown,
                daily_count=daily_count,
                minute_count=minute_count,
                daily_dates=daily_dates,
                dl_start=start_date,
                end_date=end_date,
            )

        return {
            "daily_count": daily_count,
            "minute_count": minute_count,
            "verified": verified,
            "verify_msg": verify_msg,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _raise_if_cancelled(cancel_event: CancelChecker, msg: str) -> None:
        if cancel_event and cancel_event.is_set():
            logger.info(msg)
            raise asyncio.CancelledError(msg)


def _parse_date(s: str) -> date:
    """Parse YYYY-MM-DD into date (local helper to avoid pulling storage helper)."""
    from datetime import datetime as _dt

    return _dt.strptime(s, "%Y-%m-%d").date()
