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


async def build_snapshots_from_cache(cache: Any, date_str: str) -> dict[str, Any]:
    """Build PriceSnapshot dict from GreptimeBacktestCache for a given date.

    Replaces the iwencai pre-filter + history_quotes + 9:40 fetch pipeline.
    Local filtering: open_gain_pct > -0.5% (same as iwencai query).

    Raises:
        MinuteDataMissingError: if minute data coverage < 50% of daily candidates.
    """
    from src.strategy.models import PriceSnapshot

    all_daily = await cache.get_all_codes_with_daily(date_str)
    snapshots: dict[str, PriceSnapshot] = {}

    if not all_daily:
        logger.warning(f"build_snapshots_from_cache: no data for date_str='{date_str}'")
        return snapshots

    daily_candidates = 0
    minute_hits = 0

    for code, day in all_daily.items():
        if day.is_suspended:
            continue

        open_price = day.open
        prev_close = day.preClose
        if prev_close <= 0 or open_price <= 0:
            continue

        open_gain = (open_price - prev_close) / prev_close * 100
        if open_gain < -0.5:
            continue

        daily_candidates += 1

        data_940 = await cache.get_940_price(code, date_str)
        if not data_940:
            continue

        minute_hits += 1
        latest_price, cum_vol, max_high, min_low = data_940

        if latest_price <= 0:
            continue

        snapshots[code] = PriceSnapshot(
            stock_code=code,
            stock_name="",
            open_price=open_price,
            prev_close=prev_close,
            latest_price=latest_price,
            early_volume=cum_vol,
            high_price=max_high,
            low_price=min_low,
        )

    # Trading safety: halt if minute data is severely insufficient
    if daily_candidates > 0 and minute_hits < daily_candidates * 0.5:
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
    backtest_cache: Any,
    trade_calendar: list[date] | None = None,
) -> dict[str, Any]:
    """Build PriceSnapshot dict from live Tushare quotes + prev_close.

    Two prev_close strategies:
    - With trade_calendar: exact previous trading day → GreptimeDB → tsanghi fallback
    - Without trade_calendar: look back 1-7 days in GreptimeDB cache
    """
    from src.strategy.models import PriceSnapshot

    quotes = await realtime_client.batch_get_early_quotes(universe)
    logger.info(f"Momentum live scan: got {len(quotes)} early quotes")

    if not quotes:
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

        # Source 1: GreptimeDB cache
        if backtest_cache and getattr(backtest_cache, "is_ready", False):
            all_daily = await backtest_cache.get_all_codes_with_daily(prev_trade_date)
            for code, daily in all_daily.items():
                close_val = daily.close
                if close_val and close_val > 0:
                    prev_closes[code] = close_val

        # Source 2: tsanghi API fallback (if cache coverage < 80%)
        if len(prev_closes) < len(quotes) * 0.8:
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
                f"from both GreptimeDB cache and tsanghi API"
            )
        logger.info(f"Momentum live: prev_close ({prev_trade_date}): {len(prev_closes)} stocks")
    else:
        # Look back 1-7 days in GreptimeDB cache
        if not backtest_cache or not getattr(backtest_cache, "is_ready", False):
            raise RuntimeError("Momentum live scan: GreptimeDB cache not ready for prev_close")

        prev_daily: dict = {}
        for days_back in range(1, 8):
            prev_date = today - timedelta(days=days_back)
            prev_date_str = prev_date.strftime("%Y-%m-%d")
            prev_daily = await backtest_cache.get_all_codes_with_daily(prev_date_str)
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
    for code, quote in quotes.items():
        if not quote.is_trading:
            continue
        prev_close = prev_closes.get(code, 0.0)
        if prev_close <= 0:
            continue
        price_snapshots[code] = PriceSnapshot(
            stock_code=code,
            stock_name="",
            open_price=quote.open_price,
            prev_close=prev_close,
            latest_price=quote.early_close,
            early_volume=quote.early_volume,
            high_price=quote.early_high,
            low_price=quote.early_low,
        )

    return price_snapshots


# ── Strategy runners ─────────────────────────────────────────


async def run_momentum_backtest(
    backtest_cache: Any,
    fundamentals_db: FundamentalsDB,
    trade_date: date,
    concept_mapper: LocalConceptMapper | None = None,
    stock_filter: StockFilter | None = None,
) -> Any:
    """Run Momentum scan using historical cache data.

    Returns MomentumScanResult.

    Raises:
        MinuteDataMissingError: if minute data coverage < 50%.
    """
    from src.data.clients.greptime_backtest_cache import GreptimeHistoricalAdapter
    from src.data.sources.local_concept_mapper import LocalConceptMapper as LCM
    from src.strategy.strategies.momentum_scanner import MomentumScanner

    date_str = trade_date.strftime("%Y-%m-%d")
    price_snapshots = await build_snapshots_from_cache(backtest_cache, date_str)

    if not price_snapshots:
        # Return empty result — caller decides how to handle
        from src.strategy.strategies.momentum_scanner import MomentumScanResult

        return MomentumScanResult()

    adapter = GreptimeHistoricalAdapter(backtest_cache)
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
    backtest_cache: Any | None = None,
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
        backtest_cache: GreptimeBacktestCache for prev_close lookup.
        trade_calendar: Trading day calendar. If provided, uses exact prev_trade_date
            for prev_close. Otherwise looks back 1-7 days in cache.
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
        backtest_cache=backtest_cache,
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
