# === MODULE PURPOSE ===
# ML strategy service: data preparation + MLScanner invocation.
# Prepares data and invokes MLScanner for live and backtest pipelines.
#
# === DESIGN PRINCIPLES ===
# - Stateless: all dependencies passed as arguments
# - Returns MLScanResult (raw strategy output), caller decides what to do next
# - No signal pushing, no notifications — that's the trigger layer's job
# - Two paths: backtest (storage) and live (realtime quotes)

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from src.data.sources.local_concept_mapper import LocalConceptMapper

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


class MinuteDataMissingError(Exception):
    """Raised when minute data coverage is insufficient for reliable backtest."""

    pass


# ── Live scan ──────────────────────────────────────────────


async def run_ml_live(
    realtime_client: Any,
    storage: Any,
    concept_mapper: LocalConceptMapper,
    trade_calendar: list[date] | None = None,
    model_name: str = "full_latest",
) -> Any:
    """Run ML scan using live Tushare quotes + GreptimeDB history.

    Args:
        realtime_client: TushareRealtimeClient for fetching early quotes.
        storage: GreptimeBacktestStorage for prev_close + 37d history.
        concept_mapper: For board ↔ stock mapping.
        trade_calendar: Trading day calendar.
        model_name: Which model to load (default "full_latest").

    Returns:
        MLScanResult.

    Raises:
        RuntimeError: on data issues (trading safety: fail fast).
        FileNotFoundError: if model file doesn't exist.
    """
    from src.strategy.aggregators import EarlyWindowAggregator, Snapshot
    from src.strategy.strategies.ml_scanner import DailyBar, MLScanner, MLScanResult

    today = datetime.now(BEIJING_TZ).date()
    scanner = MLScanner(concept_mapper)

    # Step 1: Build universe
    universe = await scanner.build_universe()
    universe_codes = list(universe.codes)

    if not universe_codes:
        raise RuntimeError("ML live scan: universe is empty")

    # Step 2: Fetch raw 1-min bars from rt_min_daily, then aggregate the
    # 09:31~09:40 window in the strategy layer (same code path as backtest).
    bars_by_code = await realtime_client.batch_get_minute_bars(universe_codes)
    logger.info("ML live scan: got bars for %d stocks", len(bars_by_code))

    if not bars_by_code:
        return MLScanResult(model_name=model_name)

    aggregator = EarlyWindowAggregator()
    early_data: dict[str, tuple[float, Snapshot]] = {}
    for code, bars in bars_by_code.items():
        if not bars:
            continue
        try:
            day_open = float(bars[0].get("open") or 0.0)
        except (ValueError, TypeError):
            continue
        if day_open <= 0:
            continue
        snaps_by_day = aggregator.aggregate(bars)
        if not snaps_by_day:
            continue
        # rt_min_daily returns bars for one trading day → exactly 1 entry.
        snap = next(iter(snaps_by_day.values()))
        early_data[code] = (day_open, snap)

    if not early_data:
        return MLScanResult(model_name=model_name)

    # Step 3: Resolve prev_close
    prev_closes = await _resolve_prev_close(storage, trade_calendar, today, len(early_data))

    if not prev_closes:
        raise RuntimeError("ML live scan: failed to get any prev_close data")

    # Step 4: Fetch 37d history from GreptimeDB
    if not storage or not getattr(storage, "is_ready", False):
        raise RuntimeError("ML live scan: GreptimeDB storage not ready for history")

    prev_trade_date = _get_prev_trade_date(trade_calendar, today)
    start_37d = _get_history_start_date(trade_calendar, today, days=50)
    history_raw = await storage.get_multi_day_history(
        start_37d.strftime("%Y-%m-%d"),
        prev_trade_date.strftime("%Y-%m-%d"),
    )

    # Convert to ml_scanner.DailyBar
    history_bars: dict[str, list[DailyBar]] = {}
    for code, bars in history_raw.items():
        history_bars[code] = [
            DailyBar(date=d, open=o, high=h, low=lo, close=c, volume=v)
            for d, o, h, lo, c, v in bars
        ]

    logger.info(
        "ML live scan: history %s..%s → %d stocks",
        start_37d,
        prev_trade_date,
        len(history_bars),
    )

    # ── SAFETY: refuse to recommend if history data is severely incomplete ──
    # If <20% of stocks with early data have history, GreptimeDB cache is
    # likely missing recent dates. Better to fail loudly than recommend based
    # on garbage data.
    _MIN_HISTORY_COVERAGE = 0.20
    if early_data and len(history_bars) < len(early_data) * _MIN_HISTORY_COVERAGE:
        raise RuntimeError(
            f"GreptimeDB 历史数据严重不足: "
            f"仅 {len(history_bars)}/{len(early_data)} 只股票有37天历史 "
            f"(覆盖率 {len(history_bars) / len(early_data) * 100:.0f}% < 20%)。"
            f"请检查缓存补全是否正常运行。"
        )

    # Step 5: Build snapshots
    snapshot_result = MLScanner.build_snapshots(
        universe.codes, prev_closes, early_data, history_bars
    )

    if not snapshot_result.snapshots:
        return MLScanResult(model_name=model_name)

    # Step 6: Fetch suspended stocks (for data quality alerts)
    try:
        suspended = await realtime_client.fetch_suspended_stocks(today.strftime("%Y%m%d"))
    except Exception:
        logger.warning("ML live scan: failed to fetch suspended stocks", exc_info=True)
        suspended = set()

    # Step 7: Run full scan pipeline
    return await scanner.scan(snapshot_result.snapshots, today, model_name, suspended)


# ── Backtest scan ──────────────────────────────────────────


async def run_ml_backtest(
    storage: Any,
    concept_mapper: LocalConceptMapper,
    trade_date: date,
    model_name: str = "full_latest",
) -> Any:
    """Run ML scan using historical storage data.

    Builds StockSnapshot from GreptimeDB daily + minute storage.

    Args:
        storage: GreptimeBacktestStorage.
        concept_mapper: For board ↔ stock mapping.
        trade_date: The trading date to backtest.
        model_name: Which model to load.

    Returns:
        MLScanResult.

    Raises:
        MinuteDataMissingError: if minute data coverage < 50%.
        FileNotFoundError: if model file doesn't exist.
    """
    from src.strategy.aggregators import EarlyWindowAggregator
    from src.strategy.strategies.ml_scanner import DailyBar, MLScanner, MLScanResult

    scanner = MLScanner(concept_mapper)
    date_str = trade_date.strftime("%Y-%m-%d")

    # Step 1: Get daily data for trade_date (for prev_close + open + suspended check)
    all_daily = await storage.get_all_codes_with_daily(date_str)
    if not all_daily:
        logger.warning("ML backtest: no daily data for %s", date_str)
        return MLScanResult(model_name=model_name, skip_reason="no_daily_data")

    # Step 2: Get 37d history before trade_date
    # Go back ~60 calendar days to ensure ≥37 trading days
    start_history = (trade_date - timedelta(days=60)).strftime("%Y-%m-%d")
    prev_day = (trade_date - timedelta(days=1)).strftime("%Y-%m-%d")
    history_raw = await storage.get_multi_day_history(start_history, prev_day)

    # Convert to ml_scanner.DailyBar
    history_bars: dict[str, list[DailyBar]] = {}
    for code, bars in history_raw.items():
        history_bars[code] = [
            DailyBar(date=d, open=o, high=h, low=lo, close=c, volume=v)
            for d, o, h, lo, c, v in bars
        ]

    # Step 2.5: Get previous trading day's closes from DB as preClose source.
    # The DB pre_close column can be 0 if the pipeline lacked prior data at
    # download time, so we derive it directly from the previous day's close.
    from src.data.clients.tushare_realtime import get_tushare_trade_calendar

    cal_start = (trade_date - timedelta(days=14)).strftime("%Y-%m-%d")
    cal_end = (trade_date - timedelta(days=1)).strftime("%Y-%m-%d")
    cal = await get_tushare_trade_calendar(cal_start, cal_end)
    prev_td = cal[-1] if cal else None  # last trading day before trade_date

    prev_close_map: dict[str, float] = {}
    if prev_td:
        prev_daily = await storage.get_all_codes_with_daily(prev_td)
        for code, bar in prev_daily.items():
            if bar.close > 0:
                prev_close_map[code] = bar.close

    # Step 3: Build candidate set from daily data (gap pre-filter)
    candidates: dict[str, tuple[float, float]] = {}
    n_suspended = 0
    n_no_prev_close = 0
    n_gap_filtered = 0
    for code, day in all_daily.items():
        if day.is_suspended:
            n_suspended += 1
            continue
        open_price = day.open
        if open_price <= 0:
            continue
        prev_close = prev_close_map.get(code, 0.0)
        if prev_close <= 0:
            n_no_prev_close += 1
            continue
        gap_pct = (open_price - prev_close) / prev_close * 100
        if gap_pct < -0.5:
            n_gap_filtered += 1
            continue
        candidates[code] = (open_price, prev_close)

    daily_candidates = len(candidates)

    logger.info(
        "ML backtest %s: prev_td=%s, prev_close_map=%d, "
        "%d daily → -%d suspended -%d no_prev -%d gap → %d candidates",
        date_str,
        prev_td,
        len(prev_close_map),
        len(all_daily),
        n_suspended,
        n_no_prev_close,
        n_gap_filtered,
        daily_candidates,
    )

    # Step 4: Per-code streaming fetch + aggregate.
    # Previously pulled ALL stocks' minute bars for the day in a single query
    # (~1.2M rows → ~600 MB peak in the Python process). Now we fetch one
    # candidate at a time with bounded concurrency, build the snapshot
    # immediately, and let the raw bars go out of scope for GC.
    from src.strategy.strategies.ml_scanner import StockSnapshot

    aggregator = EarlyWindowAggregator()
    snapshots: dict[str, StockSnapshot] = {}
    minute_hits = 0
    n_no_bars = 0
    n_no_snap = 0
    debug_logged = False

    sem = asyncio.Semaphore(8)

    async def _process_candidate(code: str, open_price: float, prev_close: float) -> None:
        nonlocal minute_hits, n_no_bars, n_no_snap, debug_logged
        async with sem:
            raw_bars = await storage.get_minute_bars_for_day(code, trade_date)
        if not raw_bars:
            n_no_bars += 1
            return
        snaps_by_day = aggregator.aggregate(raw_bars)
        snap = snaps_by_day.get(date_str)
        if snap is None or snap.close <= 0:
            n_no_snap += 1
            if not debug_logged:
                # Log first failure: show trade_time samples and snap keys
                sample_times = [b.get("trade_time", "?") for b in raw_bars[:3]]
                logger.info(
                    "Aggregation miss: code=%s, bars=%d, sample_times=%s, "
                    "snap_keys=%s, date_str=%s",
                    code,
                    len(raw_bars),
                    sample_times,
                    list(snaps_by_day.keys()),
                    date_str,
                )
                debug_logged = True
            return
        minute_hits += 1
        snapshots[code] = StockSnapshot(
            stock_code=code,
            prev_close=prev_close,
            open_price=open_price,
            latest_price=snap.close,
            high_940=snap.max_high,
            low_940=snap.min_low,
            early_volume=snap.cum_volume,
            history=history_bars.get(code, []),
        )

    if candidates:
        await asyncio.gather(
            *[
                _process_candidate(code, open_price, prev_close)
                for code, (open_price, prev_close) in candidates.items()
            ]
        )

    logger.info(
        "ML backtest %s aggregation: %d no_bars, %d no_snap, %d minute_hits",
        date_str,
        n_no_bars,
        n_no_snap,
        minute_hits,
    )

    # Trading safety: halt if minute data is severely insufficient
    if daily_candidates > 0 and minute_hits < daily_candidates * 0.5:
        coverage_pct = round(minute_hits / daily_candidates * 100, 1)
        raise MinuteDataMissingError(
            f"{date_str} 分钟数据严重不足: 仅 {minute_hits}/{daily_candidates} "
            f"只股票有分钟数据 (覆盖率 {coverage_pct}%)。"
            f"请先补充下载分钟数据，否则回测结果不可靠。"
        )

    logger.info(
        "ML backtest %s: %d snapshots from %d daily (%d with minute data)",
        date_str,
        len(snapshots),
        daily_candidates,
        minute_hits,
    )

    if not snapshots:
        reason = (
            f"no_snapshots (daily={len(all_daily)}, "
            f"candidates={daily_candidates}, "
            f"minute_hits={minute_hits})"
        )
        return MLScanResult(model_name=model_name, skip_reason=reason)

    return await scanner.scan(snapshots, trade_date, model_name)


# ── Helpers ────────────────────────────────────────────────


async def _resolve_prev_close(
    storage: Any,
    trade_calendar: list[date] | None,
    today: date,
    quote_count: int,
) -> dict[str, float]:
    """Resolve prev_close from GreptimeDB (+ tsanghi fallback).

    Build live PriceSnapshot dict from realtime quotes.
    """
    prev_closes: dict[str, float] = {}

    if trade_calendar:
        prev_dates = [d for d in trade_calendar if d < today]
        if not prev_dates:
            raise RuntimeError("ML live scan: no previous trading day in calendar")
        prev_trade_date = prev_dates[-1].strftime("%Y-%m-%d")

        # Source 1: GreptimeDB storage
        if storage and getattr(storage, "is_ready", False):
            all_daily = await storage.get_all_codes_with_daily(prev_trade_date)
            for code, daily in all_daily.items():
                close_val = daily.close
                if close_val and close_val > 0:
                    prev_closes[code] = close_val

        # Source 2: tsanghi API fallback (if storage coverage < 80%)
        if len(prev_closes) < quote_count * 0.8:
            from src.data.clients.tsanghi_client import TsanghiClient

            ts_client = TsanghiClient()
            await ts_client.start()
            try:
                for exchange in ("XSHG", "XSHE"):
                    records = await ts_client.daily_latest(exchange, prev_trade_date)
                    for row in records:
                        ticker = str(row.get("ticker", ""))
                        close_val = row.get("close")
                        if ticker and len(ticker) == 6 and close_val:
                            prev_closes[ticker] = float(close_val)
            finally:
                await ts_client.stop()

        logger.info("ML live: prev_close (%s): %d stocks", prev_trade_date, len(prev_closes))
    else:
        # Look back 1-7 days in GreptimeDB storage
        if not storage or not getattr(storage, "is_ready", False):
            raise RuntimeError("ML live scan: GreptimeDB storage not ready for prev_close")

        prev_daily: dict = {}
        for days_back in range(1, 8):
            prev_date = today - timedelta(days=days_back)
            prev_date_str = prev_date.strftime("%Y-%m-%d")
            prev_daily = await storage.get_all_codes_with_daily(prev_date_str)
            if prev_daily:
                logger.info(
                    "ML live: prev_close from %s (%d stocks)",
                    prev_date_str,
                    len(prev_daily),
                )
                break

        for code, cached_day in prev_daily.items():
            if not cached_day:
                continue
            close_val = cached_day.close
            if close_val and close_val > 0:
                prev_closes[code] = close_val

    return prev_closes


def _get_prev_trade_date(trade_calendar: list[date] | None, today: date) -> date:
    """Get the previous trading day."""
    if trade_calendar:
        prev_dates = [d for d in trade_calendar if d < today]
        if prev_dates:
            return prev_dates[-1]
    # Fallback: go back 1-7 days
    for days_back in range(1, 8):
        candidate = today - timedelta(days=days_back)
        if candidate.weekday() < 5:  # not weekend
            return candidate
    return today - timedelta(days=1)


def _get_history_start_date(trade_calendar: list[date] | None, today: date, days: int = 50) -> date:
    """Get start date for history fetch (enough for 37 trading days).

    We request 50 calendar days back to ensure ≥37 trading days.
    """
    if trade_calendar:
        prev_dates = sorted(d for d in trade_calendar if d < today)
        if len(prev_dates) >= days:
            return prev_dates[-days]
        if prev_dates:
            return prev_dates[0]
    # Fallback: 70 calendar days to be safe
    return today - timedelta(days=70)
