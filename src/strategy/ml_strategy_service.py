# === MODULE PURPOSE ===
# ML strategy service: data preparation + MLScanner invocation.
# Parallel to momentum_strategy_service.py but uses ML scanner pipeline.
#
# === DESIGN PRINCIPLES ===
# - Stateless: all dependencies passed as arguments
# - Returns MLScanResult (raw strategy output), caller decides what to do next
# - No signal pushing, no notifications — that's the trigger layer's job
# - Two paths: backtest (cache) and live (realtime quotes)

from __future__ import annotations

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
    backtest_cache: Any,
    concept_mapper: LocalConceptMapper,
    trade_calendar: list[date] | None = None,
    model_name: str = "full_latest",
) -> Any:
    """Run ML scan using live Tushare quotes + GreptimeDB history.

    Args:
        realtime_client: TushareRealtimeClient for fetching early quotes.
        backtest_cache: GreptimeBacktestCache for prev_close + 37d history.
        concept_mapper: For board ↔ stock mapping.
        trade_calendar: Trading day calendar.
        model_name: Which model to load (default "full_latest").

    Returns:
        MLScanResult.

    Raises:
        RuntimeError: on data issues (trading safety: fail fast).
        FileNotFoundError: if model file doesn't exist.
    """
    from src.strategy.strategies.ml_scanner import DailyBar, MLScanResult, MLScanner

    today = datetime.now(BEIJING_TZ).date()
    scanner = MLScanner(concept_mapper)

    # Step 1: Build universe
    universe = await scanner.build_universe()
    universe_codes = list(universe.codes)

    if not universe_codes:
        raise RuntimeError("ML live scan: universe is empty")

    # Step 2: Fetch realtime quotes (9:30-9:39 bars)
    quotes = await realtime_client.batch_get_early_quotes(universe_codes)
    logger.info("ML live scan: got %d early quotes", len(quotes))

    if not quotes:
        return MLScanResult(model_name=model_name)

    # Step 3: Resolve prev_close
    prev_closes = await _resolve_prev_close(
        backtest_cache, trade_calendar, today, len(quotes)
    )

    if not prev_closes:
        raise RuntimeError("ML live scan: failed to get any prev_close data")

    # Step 4: Fetch 37d history from GreptimeDB
    if not backtest_cache or not getattr(backtest_cache, "is_ready", False):
        raise RuntimeError("ML live scan: GreptimeDB cache not ready for history")

    prev_trade_date = _get_prev_trade_date(trade_calendar, today)
    start_37d = _get_history_start_date(trade_calendar, today, days=50)
    history_raw = await backtest_cache.get_multi_day_history(
        start_37d.strftime("%Y-%m-%d"),
        prev_trade_date.strftime("%Y-%m-%d"),
    )

    # Convert to ml_scanner.DailyBar
    history_bars: dict[str, list[DailyBar]] = {}
    for code, bars in history_raw.items():
        history_bars[code] = [
            DailyBar(date=d, open=o, high=h, low=l, close=c, volume=v)
            for d, o, h, l, c, v in bars
        ]

    logger.info(
        "ML live scan: history %s..%s → %d stocks",
        start_37d, prev_trade_date, len(history_bars),
    )

    # Step 5: Build snapshots
    snapshot_result = MLScanner.build_snapshots(
        universe.codes, prev_closes, quotes, history_bars
    )

    if not snapshot_result.snapshots:
        return MLScanResult(model_name=model_name)

    # Step 6: Fetch suspended stocks (for data quality alerts)
    try:
        suspended = await realtime_client.fetch_suspended_stocks(
            today.strftime("%Y%m%d")
        )
    except Exception:
        logger.warning("ML live scan: failed to fetch suspended stocks", exc_info=True)
        suspended = set()

    # Step 7: Run full scan pipeline
    return await scanner.scan(
        snapshot_result.snapshots, today, model_name, suspended
    )


# ── Backtest scan ──────────────────────────────────────────


async def run_ml_backtest(
    backtest_cache: Any,
    concept_mapper: LocalConceptMapper,
    trade_date: date,
    model_name: str = "full_latest",
) -> Any:
    """Run ML scan using historical cache data.

    Builds StockSnapshot from GreptimeDB daily + minute cache.

    Args:
        backtest_cache: GreptimeBacktestCache.
        concept_mapper: For board ↔ stock mapping.
        trade_date: The trading date to backtest.
        model_name: Which model to load.

    Returns:
        MLScanResult.

    Raises:
        MinuteDataMissingError: if minute data coverage < 50%.
        FileNotFoundError: if model file doesn't exist.
    """
    from src.strategy.strategies.ml_scanner import DailyBar, MLScanResult, MLScanner

    scanner = MLScanner(concept_mapper)
    date_str = trade_date.strftime("%Y-%m-%d")

    # Step 1: Get daily data for trade_date (for prev_close + open + suspended check)
    all_daily = await backtest_cache.get_all_codes_with_daily(date_str)
    if not all_daily:
        logger.warning("ML backtest: no daily data for %s", date_str)
        return MLScanResult(model_name=model_name)

    # Step 2: Get 37d history before trade_date
    # Go back ~60 calendar days to ensure ≥37 trading days
    start_history = (trade_date - timedelta(days=60)).strftime("%Y-%m-%d")
    prev_day = (trade_date - timedelta(days=1)).strftime("%Y-%m-%d")
    history_raw = await backtest_cache.get_multi_day_history(start_history, prev_day)

    # Convert to ml_scanner.DailyBar
    history_bars: dict[str, list[DailyBar]] = {}
    for code, bars in history_raw.items():
        history_bars[code] = [
            DailyBar(date=d, open=o, high=h, low=l, close=c, volume=v)
            for d, o, h, l, c, v in bars
        ]

    # Step 3: Build StockSnapshot for each stock using daily + minute data
    from src.strategy.strategies.ml_scanner import StockSnapshot

    snapshots: dict[str, StockSnapshot] = {}
    daily_candidates = 0
    minute_hits = 0

    for code, day in all_daily.items():
        if day.is_suspended:
            continue

        open_price = day.open
        prev_close = day.preClose
        if prev_close <= 0 or open_price <= 0:
            continue

        # Mild pre-filter: skip if open gap < -0.5%
        open_gain = (open_price - prev_close) / prev_close * 100
        if open_gain < -0.5:
            continue

        daily_candidates += 1

        # Get 9:40 minute data
        data_940 = await backtest_cache.get_940_price(code, date_str)
        if not data_940:
            continue

        minute_hits += 1
        latest_price, cum_vol, max_high, min_low = data_940

        if latest_price <= 0:
            continue

        # Get history for this stock
        hist = history_bars.get(code, [])

        snapshots[code] = StockSnapshot(
            stock_code=code,
            prev_close=prev_close,
            open_price=open_price,
            latest_price=latest_price,
            high_940=max_high,
            low_940=min_low,
            early_volume=cum_vol,
            history=hist,
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
        date_str, len(snapshots), daily_candidates, minute_hits,
    )

    if not snapshots:
        return MLScanResult(model_name=model_name)

    return await scanner.scan(snapshots, trade_date, model_name)


# ── Helpers ────────────────────────────────────────────────


async def _resolve_prev_close(
    backtest_cache: Any,
    trade_calendar: list[date] | None,
    today: date,
    quote_count: int,
) -> dict[str, float]:
    """Resolve prev_close from GreptimeDB (+ tsanghi fallback).

    Replicates the logic from momentum_strategy_service._build_live_snapshots.
    """
    prev_closes: dict[str, float] = {}

    if trade_calendar:
        prev_dates = [d for d in trade_calendar if d < today]
        if not prev_dates:
            raise RuntimeError("ML live scan: no previous trading day in calendar")
        prev_trade_date = prev_dates[-1].strftime("%Y-%m-%d")

        # Source 1: GreptimeDB cache
        if backtest_cache and getattr(backtest_cache, "is_ready", False):
            all_daily = await backtest_cache.get_all_codes_with_daily(prev_trade_date)
            for code, daily in all_daily.items():
                close_val = daily.close
                if close_val and close_val > 0:
                    prev_closes[code] = close_val

        # Source 2: tsanghi API fallback (if cache coverage < 80%)
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

        logger.info(
            "ML live: prev_close (%s): %d stocks", prev_trade_date, len(prev_closes)
        )
    else:
        # Look back 1-7 days in GreptimeDB cache
        if not backtest_cache or not getattr(backtest_cache, "is_ready", False):
            raise RuntimeError("ML live scan: GreptimeDB cache not ready for prev_close")

        prev_daily: dict = {}
        for days_back in range(1, 8):
            prev_date = today - timedelta(days=days_back)
            prev_date_str = prev_date.strftime("%Y-%m-%d")
            prev_daily = await backtest_cache.get_all_codes_with_daily(prev_date_str)
            if prev_daily:
                logger.info(
                    "ML live: prev_close from %s (%d stocks)",
                    prev_date_str, len(prev_daily),
                )
                break

        for code, cached_day in prev_daily.items():
            if not cached_day:
                continue
            close_val = cached_day.close
            if close_val and close_val > 0:
                prev_closes[code] = close_val

    return prev_closes


def _get_prev_trade_date(
    trade_calendar: list[date] | None, today: date
) -> date:
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


def _get_history_start_date(
    trade_calendar: list[date] | None, today: date, days: int = 50
) -> date:
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
