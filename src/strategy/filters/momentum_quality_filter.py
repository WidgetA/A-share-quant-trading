# === MODULE PURPOSE ===
# Post-selection filter to remove stocks with weak momentum signals.
# Catches "fake breakouts": stocks in a declining trend that briefly spike
# at 9:40 but lack volume confirmation — the spike is not sustained.

# === KEY SIGNAL ===
# AND combination: declining trend AND low turnover amplification.
# Empirical basis: analysis of 8 trades showed losing trades (000029, 000802)
# had both declining pre-buy trend and turnover amp ~1.1x, while profitable
# trades had stable/rising trend and turnover amp 1.5-2.7x.
# AND logic prevents false kills on breakout-from-dip stocks (declining trend
# but high turnover amp = real buying interest).

# === DATA FLOW ===
# MomentumSectorScanner Step 5 → MomentumQualityFilter → Step 6

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

from src.data.clients.ifind_http_client import IFinDHttpClient

if TYPE_CHECKING:
    from src.strategy.strategies.momentum_sector_scanner import (
        PriceSnapshot,
        SelectedStock,
    )

logger = logging.getLogger(__name__)


@dataclass
class MomentumQualityConfig:
    """Configuration for momentum quality filter thresholds."""

    enabled: bool = True

    # Signal 1: Recent trend direction.
    # Number of trading days to check for declining trend.
    # Compares prev_close vs close from N days ago.
    trend_lookback_days: int = 5

    # Signal 2: Turnover amplification on buy day.
    # early_volume (9:40 cumulative) / (avg_daily_volume × 0.125).
    # Uses only 9:40 data — no full-day hindsight, consistent in backtest and live.
    min_turnover_amp: float = 1.3

    # Number of trading days for average turnover calculation.
    turnover_lookback_days: int = 20


@dataclass
class QualityAssessment:
    """Assessment result for a single stock's momentum quality."""

    stock_code: str
    filtered_out: bool
    reasons: list[str] = field(default_factory=list)
    trend_pct: float | None = None  # N-day price change %
    turnover_amp: float | None = None  # buy-day turnover / avg turnover
    consecutive_up_days: int | None = None  # consecutive days close > prev close
    avg_daily_volume: float | None = None  # avg daily volume (for Step 6 early_amp)


class MomentumQualityFilter:
    """
    Filters out stocks with weak momentum quality.

    Uses AND combination of two signals:
    - Declining trend: prev_close vs close N days ago is negative
    - Low turnover amplification: buy-day activity < min_turnover_amp × average

    Both must be true to filter (AND logic reduces false kills).
    Fail-fast: if historical data unavailable, raises error to halt trading.

    Usage:
        filter = MomentumQualityFilter(ifind_client)
        kept, assessments = await filter.filter_stocks(selected, snapshots, trade_date)
    """

    def __init__(
        self,
        ifind_client: IFinDHttpClient,
        config: MomentumQualityConfig | None = None,
    ):
        self._ifind = ifind_client
        self._config = config or MomentumQualityConfig()

    async def filter_stocks(
        self,
        selected_stocks: list[SelectedStock],
        price_snapshots: dict[str, PriceSnapshot],
        trade_date: date | None = None,
    ) -> tuple[list[SelectedStock], list[QualityAssessment]]:
        """
        Filter out stocks with weak momentum quality.

        Args:
            selected_stocks: Stocks that passed previous filters.
            price_snapshots: Price data (must include early_volume for live mode).
            trade_date: Trade date for historical fetch. None = live mode.

        Returns:
            (kept_stocks, all_assessments)
        """
        if not self._config.enabled or not selected_stocks:
            return selected_stocks, []

        codes = [s.stock_code for s in selected_stocks]
        historical = await self._fetch_historical_context(codes, trade_date)

        kept: list[SelectedStock] = []
        assessments: list[QualityAssessment] = []

        for stock in selected_stocks:
            snap = price_snapshots.get(stock.stock_code)
            hist = historical.get(stock.stock_code)
            result = self._assess(stock, snap, hist)
            assessments.append(result)

            if result.filtered_out:
                logger.info(
                    f"QualityFilter: FILTERED {stock.stock_code} {stock.stock_name} "
                    f"— {'; '.join(result.reasons)}"
                )
            else:
                kept.append(stock)

        logger.info(
            f"QualityFilter: {len(kept)}/{len(selected_stocks)} stocks passed "
            f"({len(selected_stocks) - len(kept)} filtered out)"
        )
        return kept, assessments

    def _assess(
        self,
        stock: SelectedStock,
        snap: PriceSnapshot | None,
        hist: dict | None,
    ) -> QualityAssessment:
        """Assess a single stock's momentum quality using AND logic.

        Raises RuntimeError if required data is missing (fail-fast).
        """
        # Trading safety: no data at all = data fetch failed → halt.
        if not hist:
            raise RuntimeError(
                f"QualityFilter: no historical data for {stock.stock_code} "
                f"({stock.stock_name}). Cannot assess momentum quality — halting."
            )

        # Insufficient history: must verify it's a genuine new listing.
        # New IPO → conservatively filter out (safe).
        # Old stock with missing data → data problem → halt.
        if hist.get("trend_pct") is None:
            if hist.get("is_new_listing"):
                logger.info(
                    f"QualityFilter: {stock.stock_code} ({stock.stock_name}) "
                    f"is a recent IPO with insufficient history — filtering out"
                )
                return QualityAssessment(
                    stock_code=stock.stock_code,
                    filtered_out=True,
                    reasons=["次新股，历史数据不足"],
                )
            raise RuntimeError(
                f"QualityFilter: missing trend_pct for {stock.stock_code} "
                f"({stock.stock_name}). Not a new listing — data may be corrupt. Halting."
            )

        reasons: list[str] = []
        trend_pct: float = hist["trend_pct"]
        turnover_amp: float | None = None
        consecutive_up_days: int | None = None

        trend_declining = trend_pct < 0

        # Extract consecutive up days (used in Step 6, not for filtering here)
        if hist.get("consecutive_up_days") is not None:
            consecutive_up_days = hist["consecutive_up_days"]

        # Signal 2: Turnover amplification (unified for backtest and live).
        # Uses only 9:40 data — no full-day hindsight.
        # turnover_amp = early_volume / (avg_daily_volume × 0.125)
        # 0.125 = expected fraction of daily volume traded by 9:40.
        if not snap or snap.early_volume <= 0:
            if hist.get("is_new_listing"):
                logger.info(
                    f"QualityFilter: {stock.stock_code} ({stock.stock_name}) "
                    f"is a recent IPO with no early volume data — filtering out"
                )
                return QualityAssessment(
                    stock_code=stock.stock_code,
                    filtered_out=True,
                    reasons=["次新股，无早盘成交量数据"],
                    trend_pct=trend_pct,
                )
            raise RuntimeError(
                f"QualityFilter: missing early_volume for {stock.stock_code} "
                f"({stock.stock_name}). Cannot compute turnover_amp — halting."
            )

        avg_vol = hist.get("avg_daily_volume")
        if avg_vol and avg_vol > 0:
            expected_early = avg_vol * 0.125
            if expected_early > 0:
                turnover_amp = snap.early_volume / expected_early

        amp_low = turnover_amp is not None and turnover_amp < self._config.min_turnover_amp

        # AND logic: both signals must trigger
        if trend_declining and amp_low:
            reasons.append(
                f"趋势{trend_pct:+.1f}%<0"
                f" AND 换手放大{turnover_amp:.1f}x<{self._config.min_turnover_amp}x"
            )

        # Extract avg_daily_volume for Step 6 early_turnover_amp computation.
        avg_daily_volume = hist.get("avg_daily_volume")
        if avg_daily_volume is None or avg_daily_volume <= 0:
            raise RuntimeError(
                f"QualityFilter: missing avg_daily_volume for {stock.stock_code} "
                f"({stock.stock_name}). Cannot score turnover — halting."
            )

        return QualityAssessment(
            stock_code=stock.stock_code,
            filtered_out=len(reasons) > 0,
            reasons=reasons,
            trend_pct=trend_pct,
            turnover_amp=turnover_amp,
            consecutive_up_days=consecutive_up_days,
            avg_daily_volume=avg_daily_volume,
        )

    async def _fetch_historical_context(
        self,
        stock_codes: list[str],
        trade_date: date | None = None,
    ) -> dict[str, dict]:
        """
        Fetch historical close prices and volume for trend + amplification baseline.

        Returns dict: stock_code → {
            "trend_pct": float | None,         # N-day price change %
            "avg_daily_volume": float | None,   # avg daily volume (for turnover_amp baseline)
            "consecutive_up_days": int | None,  # consecutive close > prev_close days
            "is_new_listing": bool,
        }
        """
        if not stock_codes:
            return {}

        ref_date = trade_date or date.today()
        # Need trend_lookback + turnover_lookback days of history
        max_lookback = max(self._config.trend_lookback_days, self._config.turnover_lookback_days)
        calendar_buffer = max_lookback * 2 + 10
        start = ref_date - timedelta(days=calendar_buffer)

        # Fetch up to previous day — trade_date's data is not needed.
        # Trend and volume baseline use only pre-trade-date history.
        # Buy-day turnover comes from PriceSnapshot.early_volume (minute data).
        end = ref_date - timedelta(days=1)

        result: dict[str, dict] = {}
        batch_size = 50

        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i : i + batch_size]
            codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)

            try:
                data = await self._ifind.history_quotes(
                    codes=codes_str,
                    indicators="close,volume",
                    start_date=start.strftime("%Y-%m-%d"),
                    end_date=end.strftime("%Y-%m-%d"),
                )

                for table_entry in data.get("tables", []):
                    thscode = table_entry.get("thscode", "")
                    bare_code = thscode.split(".")[0] if thscode else ""
                    if not bare_code:
                        continue

                    tbl = table_entry.get("table", {})
                    time_vals = tbl.get("time", [])
                    close_vals = tbl.get("close", [])
                    vol_vals = tbl.get("volume", [])

                    entry: dict = {}
                    lookback = self._config.turnover_lookback_days

                    # Detect new listing: if the stock's first data date is
                    # within 30 calendar days of ref_date, it's a recent IPO.
                    if time_vals:
                        first_date = datetime.strptime(time_vals[0], "%Y-%m-%d").date()
                        days_since_listing = (ref_date - first_date).days
                        entry["is_new_listing"] = days_since_listing < 30

                    # Trend: compare last close (prev_close) vs close N days earlier
                    # + consecutive up days count (for Step 6 filtering).
                    # Data ends at day before trade_date (no future data).
                    min_trend_len = self._config.trend_lookback_days + 1
                    if close_vals and len(close_vals) >= min_trend_len:
                        closes = [float(c) for c in close_vals if c is not None]
                        if len(closes) >= min_trend_len:
                            prev_close = closes[-1]
                            n_ago = closes[-(self._config.trend_lookback_days + 1)]

                            if n_ago > 0:
                                entry["trend_pct"] = (prev_close - n_ago) / n_ago * 100

                            # Consecutive up days: count backward
                            cup = 0
                            for j in range(len(closes) - 1, 0, -1):
                                if closes[j] > closes[j - 1]:
                                    cup += 1
                                else:
                                    break
                            entry["consecutive_up_days"] = cup

                    # Volume: avg daily volume over lookback (turnover_amp baseline)
                    if vol_vals:
                        volumes = [float(v) for v in vol_vals if v is not None and float(v) > 0]
                        if volumes:
                            recent = volumes[-lookback:]
                            if recent:
                                entry["avg_daily_volume"] = sum(recent) / len(recent)

                    if entry:
                        result[bare_code] = entry

            except Exception:
                logger.error(
                    f"QualityFilter: historical fetch FAILED for batch "
                    f"({len(batch)} stocks). Trading halted."
                )
                raise

        return result
