# === MODULE PURPOSE ===
# Momentum strategy service: data preparation + strategy invocation.
# Consolidates scan logic previously duplicated in iquant_routes.py and routes.py.
#
# === DESIGN PRINCIPLES ===
# - Stateless: all dependencies passed as arguments
# - Returns MomentumScanResult (raw strategy output), caller decides what to do next
# - No signal pushing, no notifications — that's the trigger layer's job
# - Two paths: backtest (cache) and live (realtime quotes)

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from src.data.database.fundamentals_db import FundamentalsDB
    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.filters.stock_filter import StockFilter
    from src.strategy.models import HistoricalDataProvider

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


class MinuteDataMissingError(Exception):
    """Raised when minute data coverage is insufficient for reliable backtest."""

    pass


# ── Snapshot builders ────────────────────────────────────────


async def build_snapshots_from_storage(storage: Any, date_str: str) -> dict[str, Any]:
    """Build PriceSnapshot dict from GreptimeBacktestStorage for a given date.

    Replaces the iwencai pre-filter + history_quotes + 9:40 fetch pipeline.
    Local filtering: open_gain_pct > -0.5% (same as iwencai query).

    Reads RAW 1-min bars from storage and aggregates them at query time via
    the strategy-layer ``EarlyWindowAggregator``. The data layer never knows
    which window we use.

    Raises:
        MinuteDataMissingError: if minute data coverage < 50% of daily candidates.
    """
    from src.strategy.aggregators import EarlyWindowAggregator
    from src.strategy.models import PriceSnapshot

    trade_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    all_daily = await storage.get_all_codes_with_daily(date_str)
    snapshots: dict[str, PriceSnapshot] = {}

    if not all_daily:
        logger.warning(f"build_snapshots_from_storage: no data for date_str='{date_str}'")
        return snapshots

    # Step 1: filter to candidates (gap > -0.5%) using daily data only
    candidates: dict[str, tuple[float, float]] = {}
    for code, day in all_daily.items():
        if day.is_suspended:
            continue
        open_price = day.open
        prev_close = day.preClose
        if prev_close <= 0 or open_price <= 0:
            continue
        if (open_price - prev_close) / prev_close * 100 < -0.5:
            continue
        candidates[code] = (open_price, prev_close)

    daily_candidates = len(candidates)
    if daily_candidates == 0:
        return snapshots

    # Step 2: batch-fetch raw bars for ALL candidates in ONE query, then
    # aggregate in-memory. This replaces N per-stock round-trips.
    bars_by_code = await storage.get_minute_bars_for_codes_on_day(
        list(candidates.keys()), trade_date
    )
    aggregator = EarlyWindowAggregator()
    minute_hits = 0

    for code, (open_price, prev_close) in candidates.items():
        raw_bars = bars_by_code.get(code, [])
        if not raw_bars:
            continue
        snaps_by_day = aggregator.aggregate(raw_bars)
        snap = snaps_by_day.get(date_str)
        if snap is None or snap.close <= 0:
            continue
        minute_hits += 1
        snapshots[code] = PriceSnapshot(
            stock_code=code,
            stock_name="",
            open_price=open_price,
            prev_close=prev_close,
            latest_price=snap.close,
            early_volume=snap.cum_volume,
            high_price=snap.max_high,
            low_price=snap.min_low,
        )

    # Trading safety: halt if minute data is severely insufficient
    if minute_hits < daily_candidates * 0.5:
        coverage_pct = round(minute_hits / daily_candidates * 100, 1)
        raise MinuteDataMissingError(
            f"{date_str} 分钟数据严重不足: 仅 {minute_hits}/{daily_candidates} "
            f"只股票有分钟数据 (覆盖率 {coverage_pct}%)。"
            f"请先补充下载分钟数据，否则回测结果不可靠。"
        )

    return snapshots


async def _build_live_snapshots(
    realtime_client: Any,
    universe: list[str],
    storage: Any,
    trade_calendar: list[date] | None = None,
) -> dict[str, Any]:
    """Build PriceSnapshot dict from live Tushare bars + prev_close.

    Pulls raw 1-min bars from rt_min_daily and aggregates the 09:31~09:40
    window in the strategy layer via ``EarlyWindowAggregator`` (same code
    path as the backtest pipeline).

    Two prev_close strategies:
    - With trade_calendar: exact previous trading day → GreptimeDB → tsanghi fallback
    - Without trade_calendar: look back 1-7 days in GreptimeDB storage
    """
    from src.strategy.aggregators import EarlyWindowAggregator
    from src.strategy.models import PriceSnapshot

    bars_by_code = await realtime_client.batch_get_minute_bars(universe)
    logger.info(f"Momentum live scan: got bars for {len(bars_by_code)} stocks")

    if not bars_by_code:
        return {}

    # Aggregate the 09:31~09:40 window (strategy layer owns the window).
    aggregator = EarlyWindowAggregator()
    early: dict[str, tuple[float, Any]] = {}  # code -> (day_open, Snapshot)
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
        snap = next(iter(snaps_by_day.values()))
        if snap.close <= 0:
            continue
        early[code] = (day_open, snap)

    if not early:
        return {}

    # --- Resolve prev_close ---
    prev_closes: dict[str, float] = {}
    today = datetime.now(BEIJING_TZ).date()

    if trade_calendar:
        # Exact previous trading day from calendar
        prev_dates = [d for d in trade_calendar if d < today]
        if not prev_dates:
            raise RuntimeError("Momentum live scan: no previous trading day in calendar")
        prev_trade_date = prev_dates[-1].strftime("%Y-%m-%d")

        # Source 1: GreptimeDB storage
        if storage and getattr(storage, "is_ready", False):
            all_daily = await storage.get_all_codes_with_daily(prev_trade_date)
            for code, daily in all_daily.items():
                close_val = daily.close
                if close_val and close_val > 0:
                    prev_closes[code] = close_val

        # Source 2: tsanghi API fallback (if storage coverage < 80%)
        if len(prev_closes) < len(early) * 0.8:
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

        if not prev_closes:
            raise RuntimeError(
                f"Momentum live scan: failed to get prev_close for {prev_trade_date} "
                f"from both GreptimeDB storage and tsanghi API"
            )
        logger.info(f"Momentum live: prev_close ({prev_trade_date}): {len(prev_closes)} stocks")
    else:
        # Look back 1-7 days in GreptimeDB storage
        if not storage or not getattr(storage, "is_ready", False):
            raise RuntimeError("Momentum live scan: GreptimeDB storage not ready for prev_close")

        prev_daily: dict = {}
        for days_back in range(1, 8):
            prev_date = today - timedelta(days=days_back)
            prev_date_str = prev_date.strftime("%Y-%m-%d")
            prev_daily = await storage.get_all_codes_with_daily(prev_date_str)
            if prev_daily:
                logger.info(
                    f"Momentum live: prev_close from {prev_date_str} ({len(prev_daily)} stocks)"
                )
                break

        for code, cached_day in prev_daily.items():
            close_val = cached_day.close if cached_day else 0.0
            if close_val and close_val > 0:
                prev_closes[code] = close_val

    # --- Build PriceSnapshots ---
    price_snapshots: dict[str, PriceSnapshot] = {}
    for code, (day_open, snap) in early.items():
        prev_close = prev_closes.get(code, 0.0)
        if prev_close <= 0:
            continue
        price_snapshots[code] = PriceSnapshot(
            stock_code=code,
            stock_name="",
            open_price=day_open,
            prev_close=prev_close,
            latest_price=snap.close,
            early_volume=snap.cum_volume,
            high_price=snap.max_high,
            low_price=snap.min_low,
        )

    return price_snapshots


# ── Strategy runners ─────────────────────────────────────────


async def run_momentum_backtest(
    storage: Any,
    fundamentals_db: FundamentalsDB,
    trade_date: date,
    concept_mapper: LocalConceptMapper | None = None,
    stock_filter: StockFilter | None = None,
) -> Any:
    """Run Momentum scan using historical storage data.

    Returns MomentumScanResult.

    Raises:
        MinuteDataMissingError: if minute data coverage < 50%.
    """
    from src.data.clients.greptime_historical_adapter import GreptimeHistoricalAdapter
    from src.data.sources.local_concept_mapper import LocalConceptMapper as LCM
    from src.strategy.strategies.momentum_scanner import MomentumScanner

    date_str = trade_date.strftime("%Y-%m-%d")
    price_snapshots = await build_snapshots_from_storage(storage, date_str)

    if not price_snapshots:
        # Return empty result — caller decides how to handle
        from src.strategy.strategies.momentum_scanner import MomentumScanResult

        return MomentumScanResult()

    adapter = GreptimeHistoricalAdapter(storage)
    mapper = concept_mapper or LCM()
    scanner = MomentumScanner(
        historical_adapter=adapter,
        fundamentals_db=fundamentals_db,
        concept_mapper=mapper,
        stock_filter=stock_filter,
    )

    return await scanner.scan(price_snapshots, trade_date=trade_date)


async def run_momentum_live(
    realtime_client: Any,
    historical_adapter: HistoricalDataProvider,
    fundamentals_db: FundamentalsDB,
    concept_mapper: LocalConceptMapper,
    universe: list[str],
    storage: Any | None = None,
    trade_calendar: list[date] | None = None,
    stock_filter: StockFilter | None = None,
) -> Any:
    """Run Momentum scan using live Tushare quotes.

    Args:
        realtime_client: TushareRealtimeClient for fetching early quotes.
        historical_adapter: For MomentumScanner's historical data needs.
        fundamentals_db: For ST detection.
        concept_mapper: For board ↔ stock mapping.
        universe: Stock codes to scan.
        storage: GreptimeBacktestStorage for prev_close lookup.
        trade_calendar: Trading day calendar. If provided, uses exact prev_trade_date
            for prev_close. Otherwise looks back 1-7 days in storage.
        stock_filter: Optional stock filter for MomentumScanner.

    Returns:
        MomentumScanResult.

    Raises:
        RuntimeError: on data issues (trading safety: fail fast).
    """
    from src.strategy.strategies.momentum_scanner import MomentumScanner

    if not universe:
        raise RuntimeError("Momentum live scan: universe is empty")

    price_snapshots = await _build_live_snapshots(
        realtime_client=realtime_client,
        universe=universe,
        storage=storage,
        trade_calendar=trade_calendar,
    )

    if not price_snapshots:
        from src.strategy.strategies.momentum_scanner import MomentumScanResult

        return MomentumScanResult()

    scanner = MomentumScanner(
        historical_adapter=historical_adapter,
        fundamentals_db=fundamentals_db,
        concept_mapper=concept_mapper,
        stock_filter=stock_filter,
    )

    return await scanner.scan(price_snapshots)
