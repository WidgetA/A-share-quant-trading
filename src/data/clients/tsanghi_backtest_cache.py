# === MODULE PURPOSE ===
# Pre-downloads A-share daily + minute price data for backtesting.
# Provides an adapter that mimics IFinDHttpClient interface so
# MomentumSectorScanner can run without any code changes.

# === DEPENDENCIES ===
# - tsanghi (沧海数据): REST API for daily OHLCV + 5-min bars (9:40 snapshot)
# - baostock: Trading calendar only (get_trade_dates)
# - IFinDHttpClient interface: Adapter returns data in iFinD response format

# === KEY CONCEPTS ===
# - TsanghiBacktestCache: Downloads and stores all price data in memory + OSS
# - TsanghiHistoricalAdapter: Duck-types IFinDHttpClient for the scanner
# - Daily OHLCV: tsanghi /daily/latest (batch per-date, fast)
# - Minute bars: tsanghi /5min (per-stock, async concurrent), for 9:40 snapshot only
# - OSS cache: Alibaba Cloud OSS — survives container redeployment
# - Data is NOT used for live trading (backtest only)
# - Daily volume stored in 手 (lots) — adapter converts ×100 at read time
# - Minute volume stored in 股 (shares) — converted ×100 from 手 at download time

from __future__ import annotations

import asyncio
import gzip
import logging
import os
import pickle
import tempfile
from datetime import date, datetime, timedelta
from typing import Any, Callable

logger = logging.getLogger(__name__)

# OSS cache key prefix
_OSS_PREFIX = "akshare-cache/"


def _get_oss_bucket():
    """Get OSS bucket instance from environment variables. Returns None if not configured."""
    key_id = os.environ.get("OSS_ACCESS_KEY_ID")
    key_secret = os.environ.get("OSS_ACCESS_KEY_SECRET")
    endpoint = os.environ.get("OSS_ENDPOINT")
    bucket_name = os.environ.get("OSS_BUCKET_NAME")
    if not all([key_id, key_secret, endpoint, bucket_name]):
        logger.warning("OSS not configured — cache persistence disabled")
        return None
    import oss2

    auth = oss2.Auth(key_id, key_secret)
    return oss2.Bucket(auth, endpoint, bucket_name)


def check_oss_available() -> str | None:
    """Pre-flight check: verify OSS is configured and reachable.

    Returns None if OK, or an error message string.
    """
    bucket = _get_oss_bucket()
    if bucket is None:
        oss_vars = (
            "OSS_ACCESS_KEY_ID",
            "OSS_ACCESS_KEY_SECRET",
            "OSS_ENDPOINT",
            "OSS_BUCKET_NAME",
        )
        missing = [v for v in oss_vars if not os.environ.get(v)]
        return f"OSS 环境变量缺失: {', '.join(missing)}"
    try:
        # Lightweight call to verify credentials + bucket access
        bucket.get_bucket_info()
        return None
    except Exception as e:
        return f"OSS 连接失败: {e}"


class TsanghiBacktestCache:
    """
    Pre-downloads daily OHLCV and 09:30-09:40 minute bar data for all
    main-board A-share stocks, keyed by (stock_code, date).

    OSS persistence:
        After download, data is saved to Alibaba Cloud OSS as pickle files.
        On next request, if the cached range covers the requested range,
        data is loaded from OSS instead of re-downloading (~25 min).

    Usage:
        cache = TsanghiBacktestCache()
        await cache.download_prices(start_date, end_date, progress_cb)
        snap = cache.get_daily(code, trade_date)
        p940 = cache.get_940_price(code, trade_date)
    """

    def __init__(self) -> None:
        # daily[code][date_str] = {open, high, low, close, preClose, volume, amount, turnoverRatio}
        # turnoverRatio may be None when sourced from tsanghi (which doesn't provide it)
        self._daily: dict[str, dict[str, dict[str, float | None]]] = {}
        # minute[code][date_str] = (close_at_940, cum_volume, max_high, min_low)
        self._minute: dict[str, dict[str, tuple[float, float, float, float]]] = {}
        # All stock codes (bare 6-digit) that were downloaded
        self._stock_codes: list[str] = []
        self._is_ready = False
        self._start_date: date | None = None
        self._end_date: date | None = None

    @property
    def is_ready(self) -> bool:
        return self._is_ready

    @property
    def stock_codes(self) -> list[str]:
        return self._stock_codes

    def get_daily(self, code: str, date_str: str) -> dict[str, float | None] | None:
        """Get daily OHLCV for a stock on a date. date_str is YYYY-MM-DD."""
        return self._daily.get(code, {}).get(date_str)

    def get_940_price(self, code: str, date_str: str) -> tuple[float, float, float, float] | None:
        """Get 9:40 price data: (close, cum_volume, max_high, min_low)."""
        return self._minute.get(code, {}).get(date_str)

    def get_all_codes_with_daily(self, date_str: str) -> dict[str, dict[str, float | None]]:
        """Get daily data for ALL stocks on a specific date."""
        result: dict[str, dict[str, float | None]] = {}
        for code, dates in self._daily.items():
            day_data = dates.get(date_str)
            if day_data:
                result[code] = day_data
        return result

    def find_minute_gaps(self) -> list[tuple[date, date]]:
        """Find date ranges where daily data exists but minute data is sparse/missing.

        For each trading date, counts how many stocks have daily vs minute data.
        A date is considered a "gap" if daily_count > 100 and minute_count < 50%
        of daily_count.  Returns a sorted list of contiguous gap ranges.
        """
        daily_date_counts: dict[str, int] = {}
        minute_date_counts: dict[str, int] = {}

        for _code, dates in self._daily.items():
            for ds in dates:
                daily_date_counts[ds] = daily_date_counts.get(ds, 0) + 1
        for _code, min_dates in self._minute.items():
            for ds in min_dates:
                minute_date_counts[ds] = minute_date_counts.get(ds, 0) + 1

        gap_dates: list[date] = []
        for ds, daily_count in daily_date_counts.items():
            if daily_count <= 100:
                continue
            minute_count = minute_date_counts.get(ds, 0)
            if minute_count < daily_count * 0.5:
                try:
                    gap_dates.append(datetime.strptime(ds, "%Y-%m-%d").date())
                except ValueError:
                    continue

        if not gap_dates:
            return []

        # Group consecutive dates into ranges
        gap_dates.sort()
        ranges: list[tuple[date, date]] = []
        range_start = gap_dates[0]
        prev = gap_dates[0]
        for d in gap_dates[1:]:
            if (d - prev).days <= 3:  # Allow small weekday gaps (weekends)
                prev = d
            else:
                ranges.append((range_start, prev))
                range_start = d
                prev = d
        ranges.append((range_start, prev))
        return ranges

    def missing_ranges(self, start_date: date, end_date: date) -> list[tuple[date, date]]:
        """Return date ranges not covered by this cache (boundary + internal gaps).

        Checks both:
        1. Head/tail boundary gaps (original logic)
        2. Internal gaps where minute data is missing/sparse

        E.g. cache has daily 9/1-2/10 but minute only 1/1-2/10:
        request 9/1-2/10 → [(9/1, 12/31)] for the minute gap.
        """
        if not self._is_ready or not self._start_date or not self._end_date:
            return [(start_date, end_date)]

        gaps: list[tuple[date, date]] = []

        # 1) Boundary gaps
        if start_date < self._start_date:
            gaps.append((start_date, self._start_date - timedelta(days=1)))
        if end_date > self._end_date:
            gaps.append((self._end_date + timedelta(days=1), end_date))

        # 2) Internal minute-data gaps (within cached range)
        for gap_start, gap_end in self.find_minute_gaps():
            # Only include gaps that overlap with the requested range
            clipped_start = max(gap_start, start_date)
            clipped_end = min(gap_end, end_date)
            if clipped_start <= clipped_end:
                gaps.append((clipped_start, clipped_end))

        # Sort and merge overlapping ranges
        if len(gaps) <= 1:
            return gaps
        gaps.sort()
        merged: list[tuple[date, date]] = [gaps[0]]
        for s, e in gaps[1:]:
            prev_s, prev_e = merged[-1]
            if s <= prev_e + timedelta(days=1):
                merged[-1] = (prev_s, max(prev_e, e))
            else:
                merged.append((s, e))
        return merged

    def copy(self) -> TsanghiBacktestCache:
        """Create a shallow-enough copy safe for merge_from without mutating the original.

        Copies outer dicts (code-level) and inner dicts (date-level) so that
        merge_from().update() on the copy doesn't affect the original.
        Leaf date entries (dicts of floats / tuples) are shared but never mutated.
        """
        clone = TsanghiBacktestCache()
        clone._daily = {code: dict(dates) for code, dates in self._daily.items()}
        clone._minute = {code: dict(dates) for code, dates in self._minute.items()}
        clone._stock_codes = list(self._stock_codes)
        clone._start_date = self._start_date
        clone._end_date = self._end_date
        clone._is_ready = self._is_ready
        return clone

    def merge_from(self, other: TsanghiBacktestCache) -> None:
        """Merge data from another cache (e.g. a gap download) into this one."""
        for code, dates in other._daily.items():
            if code in self._daily:
                self._daily[code].update(dates)
            else:
                self._daily[code] = dates
        for code, min_dates in other._minute.items():
            if code in self._minute:
                self._minute[code].update(min_dates)
            else:
                self._minute[code] = min_dates
        existing_codes = set(self._stock_codes)
        for code in other._stock_codes:
            if code not in existing_codes:
                self._stock_codes.append(code)
        if other._start_date:
            if self._start_date is None or other._start_date < self._start_date:
                self._start_date = other._start_date
        if other._end_date:
            if self._end_date is None or other._end_date > self._end_date:
                self._end_date = other._end_date

    def covers_range(self, start_date: date, end_date: date) -> bool:
        """Check if cached data covers the requested date range.

        Returns False if boundary is not covered OR if there are minute-data
        gaps within the requested range (otherwise incremental download skips
        dates where minute data is missing).
        """
        if not self._is_ready or not self._start_date or not self._end_date:
            return False
        if not (self._start_date <= start_date and self._end_date >= end_date):
            return False
        # Also reject if minute data has gaps within the requested range
        for gap_start, gap_end in self.find_minute_gaps():
            if gap_start <= end_date and gap_end >= start_date:
                return False
        return True

    def _recalculate_date_range(self) -> None:
        """Recompute _start_date/_end_date from actual data (daily AND minute).

        Uses the INTERSECTION of daily and minute ranges so that every date
        in [_start_date, _end_date] has both daily OHLCV and 9:40 minute data.
        Without this, incremental downloads can leave gaps in minute data that
        cause gain_from_open=0% → "无初筛股" on every date.
        """

        def _scan_range(
            data: dict[str, dict],
        ) -> tuple[date | None, date | None]:
            lo: date | None = None
            hi: date | None = None
            sampled = 0
            for _code, dates in data.items():
                if not dates:
                    continue
                keys = sorted(dates.keys())
                try:
                    first = datetime.strptime(keys[0], "%Y-%m-%d").date()
                    last = datetime.strptime(keys[-1], "%Y-%m-%d").date()
                except (ValueError, IndexError):
                    continue
                if lo is None or first < lo:
                    lo = first
                if hi is None or last > hi:
                    hi = last
                sampled += 1
                if sampled >= 200:
                    break
            return lo, hi

        d_lo, d_hi = _scan_range(self._daily)
        m_lo, m_hi = _scan_range(self._minute)  # type: ignore[arg-type]

        if d_lo and d_hi:
            # Use intersection of daily and minute ranges
            if m_lo and m_hi:
                new_start = max(d_lo, m_lo)
                new_end = min(d_hi, m_hi)
            else:
                new_start, new_end = d_lo, d_hi

            old_start, old_end = self._start_date, self._end_date
            self._start_date = new_start
            self._end_date = new_end
            if old_start != new_start or old_end != new_end:
                logger.warning(
                    f"Date range recalculated: "
                    f"[{old_start} ~ {old_end}] → [{new_start} ~ {new_end}]"
                    f" (daily=[{d_lo}~{d_hi}], minute=[{m_lo}~{m_hi}])"
                )

    def _has_valid_format(self) -> bool:
        """Check if daily data has required fields (close, volume)."""
        for _code, dates in self._daily.items():
            for _ds, day in dates.items():
                return "close" in day and "volume" in day
        return False

    def _save_to_oss(self) -> str | None:
        """Persist cache to Alibaba Cloud OSS as gzip-compressed pickle files.

        Returns None on success, or an error message string on failure.
        Streams pickle → gzip → temp file to avoid holding full serialized
        data in memory (3194 stocks can be hundreds of MB when pickled).
        """
        bucket = _get_oss_bucket()
        if bucket is None:
            return "OSS 未配置（缺少环境变量）"

        meta = {
            "start_date": self._start_date,
            "end_date": self._end_date,
            "stock_codes": self._stock_codes,
        }
        try:
            # Save data FIRST, meta LAST. If upload is interrupted between
            # files, meta still points to the old range — next load will detect
            # the gap and re-download instead of silently missing data.
            for name, obj in [
                ("daily.pkl.gz", self._daily),
                ("minute.pkl.gz", self._minute),
                ("meta.pkl.gz", meta),
            ]:
                # Stream: pickle → gzip → temp file (no full copy in RAM)
                tmp_fd, tmp_path = tempfile.mkstemp(suffix=".pkl.gz")
                try:
                    with os.fdopen(tmp_fd, "wb") as raw_f:
                        with gzip.GzipFile(fileobj=raw_f, mode="wb", compresslevel=1) as gz_f:
                            pickle.dump(obj, gz_f, protocol=pickle.HIGHEST_PROTOCOL)
                    size_mb = os.path.getsize(tmp_path) / 1024 / 1024
                    logger.info(f"Uploading {name} to OSS ({size_mb:.1f} MB compressed)...")
                    bucket.put_object_from_file(f"{_OSS_PREFIX}{name}", tmp_path)
                    logger.info(f"Uploaded {name} to OSS OK")
                finally:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
            logger.info(f"Cache saved to OSS: {len(self._daily)} daily, {len(self._minute)} minute")
            return None
        except Exception as e:
            msg = f"OSS 上传失败: {e}"
            logger.error(msg)
            return msg

    @classmethod
    def load_from_oss(cls) -> TsanghiBacktestCache | None:
        """Load cache from OSS if available. Returns None if not found.

        Tries .pkl.gz (gzipped) first, falls back to legacy .pkl format.
        """
        bucket = _get_oss_bucket()
        if bucket is None:
            logger.warning("load_from_oss: _get_oss_bucket() returned None")
            return None

        try:
            files = {}
            base_names = ("meta", "daily", "minute")
            for base in base_names:
                gz_key = f"{_OSS_PREFIX}{base}.pkl.gz"
                plain_key = f"{_OSS_PREFIX}{base}.pkl"
                # Try gzipped first, fall back to legacy plain pickle
                if bucket.object_exists(gz_key):
                    key = gz_key
                elif bucket.object_exists(plain_key):
                    key = plain_key
                else:
                    logger.warning(f"load_from_oss: key not found: {gz_key} or {plain_key}")
                    return None
                logger.info(f"load_from_oss: downloading {key}...")
                # Stream to temp file to avoid holding full compressed+decompressed in RAM
                tmp_fd, tmp_path = tempfile.mkstemp(suffix=os.path.splitext(key)[1])
                try:
                    os.close(tmp_fd)
                    bucket.get_object_to_file(key, tmp_path)
                    size_mb = os.path.getsize(tmp_path) / 1024 / 1024
                    logger.info(f"load_from_oss: {key} downloaded ({size_mb:.1f} MB)")
                    if key.endswith(".gz"):
                        with gzip.open(tmp_path, "rb") as gz_f:
                            files[base] = pickle.load(gz_f)  # noqa: S301
                    else:
                        with open(tmp_path, "rb") as f:
                            files[base] = pickle.load(f)  # noqa: S301
                finally:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)

            meta = files["meta"]
            cache = cls()
            cache._start_date = meta["start_date"]
            cache._end_date = meta["end_date"]
            cache._stock_codes = meta["stock_codes"]
            cache._daily = files["daily"]
            cache._minute = files["minute"]

            # Validate cache format: must have required fields (close, volume).
            if not cache._has_valid_format():
                logger.warning(
                    "OSS cache is STALE: daily data missing required fields. "
                    "Discarding — will re-download."
                )
                return None

            # Ensure preClose is filled — defensive against old caches
            # that were saved before _compute_pre_close() existed.
            cache._compute_pre_close()

            # Recalculate actual date range from data (don't trust meta.pkl —
            # it may be out of sync if a previous OSS save was interrupted).
            cache._recalculate_date_range()

            cache._is_ready = True
            logger.info(
                f"Cache loaded from OSS: {len(cache._daily)} daily, "
                f"{len(cache._minute)} minute, "
                f"range [{cache._start_date} ~ {cache._end_date}]"
            )
            return cache
        except Exception:
            logger.error("Failed to load cache from OSS", exc_info=True)
            raise

    async def download_prices(
        self,
        start_date: date,
        end_date: date,
        progress_cb: Callable[[str, int, int], Any] | None = None,
    ) -> None:
        """
        Download daily + minute data for all main-board stocks.

        Phase 1 — Daily OHLCV via tsanghi REST API (fast, batch per-date).
        Phase 2 — 5-min bars via baostock (per-stock, for 9:40 snapshot only).

        Args:
            start_date: First trading date (inclusive).
            end_date: Last trading date (inclusive).
            progress_cb: Optional callback(phase, current, total) for progress updates.
        """
        self._start_date = start_date
        self._end_date = end_date
        # Download extra history before start_date:
        # - preClose needs 1 trading day before start
        # - QualityFilter needs 20 trading days of turnover lookback (~30 calendar)
        # - Trend lookback needs 5 additional trading days
        # Use 60 calendar days to safely cover all lookback requirements.
        dl_start = start_date - timedelta(days=60)

        if progress_cb:
            await _maybe_await(progress_cb("init", 0, 0))

        # --- Phase 1: Daily OHLCV from tsanghi ---
        await self._download_daily_tsanghi(dl_start, end_date, progress_cb)

        # Derive preClose from previous trading day's close for each stock.
        self._compute_pre_close()

        # --- Phase 2: Minute data from tsanghi 5min (9:35 + 9:40 bars) ---
        codes = list(self._daily.keys())
        self._stock_codes = codes
        if codes:
            await self._download_minute_tsanghi(codes, dl_start, end_date, progress_cb)

        total = len(self._stock_codes)
        if progress_cb:
            await _maybe_await(progress_cb("download", total, total))
        logger.info(
            f"Download complete: {len(self._daily)} daily (tsanghi), "
            f"{len(self._minute)} minute (tsanghi 5min) out of {total} stocks"
        )

        # Recalculate range from actual data so metadata matches reality.
        self._recalculate_date_range()
        self._is_ready = True

    async def _download_daily_tsanghi(
        self,
        dl_start: date,
        end_date: date,
        progress_cb: Callable[[str, int, int], Any] | None = None,
    ) -> None:
        """Download daily OHLCV from tsanghi /daily/latest (batch per-date).

        Each trading day requires 2 API calls (XSHG + XSHE). For a 90-day
        backtest with 60-day lookback, that's ~150 trading days × 2 = ~300 calls.
        """
        from src.data.clients.tsanghi_client import TsanghiClient

        client = TsanghiClient()
        await client.start()

        try:
            # Enumerate calendar dates and call API for each
            total_days = (end_date - dl_start).days + 1
            trading_days_found = 0
            current = dl_start

            while current <= end_date:
                date_str = current.strftime("%Y-%m-%d")
                day_has_data = False

                for exchange in ("XSHG", "XSHE"):
                    try:
                        records = await client.daily_latest(exchange, date_str)
                    except RuntimeError as e:
                        # Non-trading day or API error for this date — skip
                        logger.debug(f"tsanghi daily_latest({exchange}, {date_str}): {e}")
                        continue

                    if not records:
                        continue

                    day_has_data = True
                    for rec in records:
                        ticker = str(rec.get("ticker", ""))
                        if not ticker or len(ticker) != 6:
                            continue
                        # Filter to main-board: 60xxxx (SH) and 00xxxx (SZ)
                        if not (ticker.startswith("60") or ticker.startswith("00")):
                            continue

                        rec_date = rec.get("date", date_str)
                        # Normalize date format (remove time component if present)
                        if " " in rec_date:
                            rec_date = rec_date.split(" ")[0]

                        o = rec.get("open")
                        c = rec.get("close")
                        if o is None or c is None:
                            continue  # skip suspended stocks

                        if ticker not in self._daily:
                            self._daily[ticker] = {}

                        self._daily[ticker][rec_date] = {
                            "open": float(o),
                            "high": float(rec.get("high", o)),
                            "low": float(rec.get("low", o)),
                            "close": float(c),
                            "preClose": 0.0,  # filled in _compute_pre_close()
                            # tsanghi volume is in 手 (lots); stored as-is,
                            # adapter converts ×100 at read time.
                            "volume": float(rec.get("volume", 0)),
                            "amount": 0.0,  # not available from tsanghi
                            # turnoverRatio not available from tsanghi;
                            # set to None so quality filter skips turnover check
                            # while _has_turnover_ratio() still returns True (key exists).
                            "turnoverRatio": None,
                        }

                if day_has_data:
                    trading_days_found += 1

                if progress_cb:
                    elapsed = (current - dl_start).days + 1
                    await _maybe_await(progress_cb("daily", elapsed, total_days))

                current += timedelta(days=1)

            logger.info(
                f"tsanghi daily download: {len(self._daily)} stocks, "
                f"{trading_days_found} trading days in [{dl_start} ~ {end_date}]"
            )
        finally:
            await client.stop()

    def _compute_pre_close(self) -> None:
        """Fill preClose for each stock from previous trading day's close."""
        for code, dates in self._daily.items():
            sorted_dates = sorted(dates.keys())
            for i, ds in enumerate(sorted_dates):
                if i > 0:
                    prev_ds = sorted_dates[i - 1]
                    dates[ds]["preClose"] = dates[prev_ds]["close"]
                # else: first day — preClose stays 0.0

    async def _download_minute_tsanghi(
        self,
        codes: list[str],
        dl_start: date,
        end_date: date,
        progress_cb: Callable[[str, int, int], Any] | None = None,
    ) -> None:
        """Download 5-min bars (09:35 + 09:40) from tsanghi for 9:40 snapshot.

        Volume conversion: tsanghi 5min returns 手 (lots); we store 股 (shares) = lots × 100.
        """
        from src.data.clients.tsanghi_client import TsanghiClient, bare_code_to_exchange

        client = TsanghiClient()
        await client.start()

        sem = asyncio.Semaphore(5)
        done_count = [0]
        fail_count = [0]
        total = len(codes)
        start_str = dl_start.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        async def _fetch_one(
            code: str,
        ) -> tuple[str, dict[str, tuple[float, float, float, float]]]:
            async with sem:
                try:
                    exchange = bare_code_to_exchange(code)
                except ValueError:
                    done_count[0] += 1
                    return code, {}

                try:
                    records = await client.five_min(
                        exchange,
                        code,
                        start_date=start_str,
                        end_date=end_str,
                    )
                except RuntimeError as exc:
                    fail_count[0] += 1
                    if fail_count[0] <= 10:
                        logger.warning(f"tsanghi 5min({code}) failed: {exc}")
                    elif fail_count[0] == 11:
                        logger.warning(
                            "tsanghi 5min: too many failures, suppressing further warnings"
                        )
                    done_count[0] += 1
                    return code, {}

                min_data: dict[str, tuple[float, float, float, float]] = {}
                for rec in records:
                    dt_str = rec.get("date", "")
                    if len(dt_str) < 19:
                        continue
                    bar_time = dt_str[11:19]  # "HH:MM:SS"
                    if bar_time not in ("09:35:00", "09:40:00"):
                        continue

                    ds = dt_str[:10]  # "YYYY-MM-DD"
                    close_val = float(rec.get("close", 0))
                    high_val = float(rec.get("high", 0))
                    low_val = float(rec.get("low", 0))
                    # tsanghi 5min volume is in 手 (lots); convert to 股 (shares)
                    vol_val = float(rec.get("volume", 0)) * 100

                    if ds in min_data:
                        prev = min_data[ds]
                        if prev[3] <= 0 or low_val <= 0:
                            logger.warning(
                                f"Minute bar low=0 for {code} on {ds} "
                                f"(prev_low={prev[3]}, new_low={low_val}), "
                                f"skipping this date"
                            )
                            del min_data[ds]
                            continue
                        min_data[ds] = (
                            close_val,  # 09:40 close overwrites 09:35
                            prev[1] + vol_val,  # cumulative volume (股)
                            max(prev[2], high_val),
                            min(prev[3], low_val),
                        )
                    else:
                        if low_val <= 0 or high_val <= 0 or close_val <= 0:
                            logger.warning(
                                f"Minute bar invalid price for {code} on {ds} "
                                f"(close={close_val}, high={high_val}, "
                                f"low={low_val}), skipping"
                            )
                            continue
                        min_data[ds] = (close_val, vol_val, high_val, low_val)

                done_count[0] += 1
                return code, min_data

        try:
            tasks = [asyncio.create_task(_fetch_one(code)) for code in codes]

            # Progress reporting while tasks run
            while not all(t.done() for t in tasks):
                await asyncio.sleep(2)
                if progress_cb:
                    await _maybe_await(progress_cb("minute", done_count[0], total))

            # Collect results
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = 0
            for result in results:
                if isinstance(result, BaseException):
                    raise result
                code, min_data = result
                if min_data:
                    self._minute[code] = min_data
                    success_count += 1

            if fail_count[0] > 0:
                logger.warning(
                    f"tsanghi 5min download: {success_count} OK, "
                    f"{fail_count[0]} failed out of {total} stocks"
                )
            else:
                logger.info(
                    f"tsanghi 5min download: {success_count} OK out of {total} stocks"
                )
        finally:
            await client.stop()

    async def save_to_oss(self) -> str | None:
        """Save cache to OSS. Returns None on success, error message on failure."""
        return await asyncio.to_thread(self._save_to_oss)


class TsanghiHistoricalAdapter:
    """
    Duck-types IFinDHttpClient for backtest use.

    Reads from TsanghiBacktestCache and returns data in iFinD response format
    so MomentumSectorScanner works without modification.
    """

    def __init__(self, cache: TsanghiBacktestCache, trade_date: date | None = None) -> None:
        self._cache = cache
        self.trade_date: date | None = trade_date

    @property
    def is_connected(self) -> bool:
        return self._cache.is_ready

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def history_quotes(
        self,
        codes: str,
        indicators: str,
        start_date: str,
        end_date: str,
        function_para: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Return cached daily data in iFinD history_quotes format."""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        tables: list[dict[str, Any]] = []

        for full_code in code_list:
            bare = full_code.split(".")[0]
            daily_dates = self._cache._daily.get(bare, {})

            # Collect data for the date range
            time_vals: list[str] = []
            indicator_data: dict[str, list] = {}
            for ind in indicators.split(","):
                indicator_data[ind.strip()] = []

            # Iterate dates in range
            d = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_d = datetime.strptime(end_date, "%Y-%m-%d").date()
            while d <= end_d:
                ds = d.strftime("%Y-%m-%d")
                day = daily_dates.get(ds)
                if day:
                    time_vals.append(ds)
                    for ind in indicators.split(","):
                        ind = ind.strip()
                        val = day.get(ind)
                        # tsanghi volume is in 手 (lots); convert to 股 (shares)
                        # at read time so callers get the same unit as iFinD.
                        if ind == "volume" and val is not None:
                            val = val * 100
                        indicator_data[ind].append(val)
                d += timedelta(days=1)

            if time_vals:
                table = {"time": time_vals, **indicator_data}
                tables.append({"thscode": full_code, "table": table})

        return {"errorcode": 0, "tables": tables}

    async def high_frequency(
        self,
        codes: str,
        indicators: str,
        start_time: str,
        end_time: str,
        function_para: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Return cached minute data in iFinD high_frequency format."""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        tables: list[dict[str, Any]] = []

        # Extract date from start_time (format: "YYYY-MM-DD HH:MM:SS")
        date_str = start_time.split(" ")[0]

        for full_code in code_list:
            bare = full_code.split(".")[0]
            data_940 = self._cache.get_940_price(bare, date_str)
            if data_940:
                close_val, cum_vol, max_high, min_low = data_940
                # Return as single-bar arrays to match iFinD format
                table: dict[str, list] = {}
                for ind in indicators.split(","):
                    ind = ind.strip()
                    if ind == "close":
                        table["close"] = [close_val]
                    elif ind == "volume":
                        table["volume"] = [cum_vol]
                    elif ind == "high":
                        table["high"] = [max_high]
                    elif ind == "low":
                        table["low"] = [min_low]
                tables.append({"thscode": full_code, "table": table})

        return {"errorcode": 0, "tables": tables}

    async def smart_stock_picking(
        self,
        search_string: str,
        search_type: str = "stock",
    ) -> dict[str, Any]:
        """Return empty result — triggers fallback in scanner Step 6."""
        return {"errorcode": 0, "tables": []}

    async def real_time_quotation(
        self,
        codes: str,
        indicators: str,
    ) -> dict[str, Any]:
        """Return 9:40 snapshot data from cache for the backtest date."""
        if self.trade_date is None:
            return {"errorcode": 0, "tables": []}

        date_str = self.trade_date.strftime("%Y-%m-%d")
        tables: list[dict[str, Any]] = []

        for full_code in (c.strip() for c in codes.split(",") if c.strip()):
            bare = full_code.split(".")[0]
            day = self._cache.get_daily(bare, date_str)
            data_940 = self._cache.get_940_price(bare, date_str)
            if not day or not data_940:
                continue

            close_940, vol_940, high_940, low_940 = data_940
            table: dict[str, list[float | None]] = {}
            for ind in (i.strip() for i in indicators.split(",")):
                if ind == "open":
                    table["open"] = [day.get("open", 0.0)]
                elif ind == "preClose":
                    # Match live behavior: TushareRealtimeClient returns None
                    # for preClose (rt_min endpoint doesn't provide it).
                    # V15Scanner._fetch_constituent_prices() skips stocks with
                    # preClose=None, so we must replicate that to get consistent
                    # results between backtest and live scan.
                    table["preClose"] = [None]
                elif ind == "latest":
                    table["latest"] = [close_940]
                elif ind == "volume":
                    table["volume"] = [vol_940]
                elif ind == "high":
                    table["high"] = [high_940]
                elif ind == "low":
                    table["low"] = [low_940]
            tables.append({"thscode": full_code, "table": table})

        return {"errorcode": 0, "tables": tables}

    async def get_trade_dates(
        self,
        market_code: str,
        start_date: str,
        end_date: str,
    ) -> list[str]:
        """Get trading dates via baostock."""

        def _query() -> list[str]:
            import baostock as bs

            lg = bs.login()
            if lg.error_code != "0":
                raise RuntimeError(f"baostock login failed: {lg.error_msg}")
            try:
                rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
                dates: list[str] = []
                while rs.next():
                    row = rs.get_row_data()
                    # row[0] = calendar_date, row[1] = is_trading_day ("1"/"0")
                    if row[1] == "1":
                        dates.append(row[0])
                return dates
            finally:
                bs.logout()

        try:
            return await asyncio.to_thread(_query)
        except Exception:
            logger.error("Failed to get trade dates")
            raise


async def _maybe_await(result: Any) -> None:
    """Await result if it's a coroutine, otherwise do nothing."""
    if asyncio.iscoroutine(result):
        await result
