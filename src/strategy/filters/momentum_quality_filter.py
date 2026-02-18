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
# MomentumSectorScanner Step 5.5 (GapFade) → MomentumQualityFilter → Step 6

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
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
    # buy_day_turnover / avg_daily_turnover must exceed this threshold.
    # In backtest mode: uses actual daily turnover from history_quotes.
    # In live mode: uses early_volume / avg_daily_volume as proxy.
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


class MomentumQualityFilter:
    """
    Filters out stocks with weak momentum quality.

    Uses AND combination of two signals:
    - Declining trend: prev_close vs close N days ago is negative
    - Low turnover amplification: buy-day activity < min_turnover_amp × average

    Both must be true to filter (AND logic reduces false kills).
    Fail-open: if data unavailable, stock is NOT filtered.

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
            result = self._assess(stock, snap, hist, trade_date)
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
        trade_date: date | None,
    ) -> QualityAssessment:
        """Assess a single stock's momentum quality using AND logic."""
        reasons: list[str] = []
        trend_pct: float | None = None
        turnover_amp: float | None = None

        trend_declining = False
        amp_low = False

        # Signal 1: Declining trend
        if hist and hist.get("trend_pct") is not None:
            trend_pct = hist["trend_pct"]
            trend_declining = trend_pct < 0

        # Signal 2: Turnover amplification
        if trade_date is not None:
            # Backtest mode: use actual daily turnover
            if hist and hist.get("buy_day_turnover") and hist.get("avg_daily_turnover"):
                buy_turn = hist["buy_day_turnover"]
                avg_turn = hist["avg_daily_turnover"]
                if avg_turn > 0:
                    turnover_amp = buy_turn / avg_turn
                    amp_low = turnover_amp < self._config.min_turnover_amp
        else:
            # Live mode: use early_volume / avg_daily_volume as proxy
            if snap and snap.early_volume > 0 and hist and hist.get("avg_daily_volume"):
                avg_vol = hist["avg_daily_volume"]
                # Expected early volume proportion (~12.5% of daily)
                expected_early = avg_vol * 0.125
                if expected_early > 0:
                    turnover_amp = snap.early_volume / expected_early
                    amp_low = turnover_amp < self._config.min_turnover_amp

        # AND logic: both signals must trigger
        if trend_declining and amp_low:
            reasons.append(
                f"趋势{trend_pct:+.1f}%<0"
                f" AND 换手放大{turnover_amp:.1f}x<{self._config.min_turnover_amp}x"
            )

        return QualityAssessment(
            stock_code=stock.stock_code,
            filtered_out=len(reasons) > 0,
            reasons=reasons,
            trend_pct=trend_pct,
            turnover_amp=turnover_amp,
        )

    async def _fetch_historical_context(
        self,
        stock_codes: list[str],
        trade_date: date | None = None,
    ) -> dict[str, dict]:
        """
        Fetch historical close prices and turnover for trend + amplification.

        Returns dict: stock_code → {
            "trend_pct": float | None,         # N-day price change %
            "buy_day_turnover": float | None,   # trade_date turnover (backtest only)
            "avg_daily_turnover": float | None,  # avg turnover over lookback
            "avg_daily_volume": float | None,    # avg volume (for live mode proxy)
        }
        """
        if not stock_codes:
            return {}

        ref_date = trade_date or date.today()
        # Need trend_lookback + turnover_lookback days of history
        max_lookback = max(self._config.trend_lookback_days, self._config.turnover_lookback_days)
        calendar_buffer = max_lookback * 2 + 10
        start = ref_date - timedelta(days=calendar_buffer)

        # For backtest: fetch up to trade_date itself (to get buy-day turnover)
        # For live: fetch up to previous day
        if trade_date is not None:
            end = ref_date
        else:
            end = ref_date - timedelta(days=1)

        result: dict[str, dict] = {}
        batch_size = 50

        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i : i + batch_size]
            codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)

            try:
                data = await self._ifind.history_quotes(
                    codes=codes_str,
                    indicators="close,turnoverRatio,volume",
                    start_date=start.strftime("%Y-%m-%d"),
                    end_date=end.strftime("%Y-%m-%d"),
                )

                for table_entry in data.get("tables", []):
                    thscode = table_entry.get("thscode", "")
                    bare_code = thscode.split(".")[0] if thscode else ""
                    if not bare_code:
                        continue

                    tbl = table_entry.get("table", {})
                    close_vals = tbl.get("close", [])
                    turn_vals = tbl.get("turnoverRatio", [])
                    vol_vals = tbl.get("volume", [])

                    entry: dict = {}
                    lookback = self._config.turnover_lookback_days

                    # Trend: compare last close (prev_close) vs close N days earlier
                    if close_vals and len(close_vals) >= self._config.trend_lookback_days + 1:
                        closes = [float(c) for c in close_vals if c is not None]
                        if len(closes) >= self._config.trend_lookback_days + 1:
                            if trade_date is not None:
                                # Backtest: prev_close = second-to-last, N-ago = further back
                                prev_close = closes[-2]  # day before trade_date
                                n_ago = closes[-(self._config.trend_lookback_days + 2)]
                            else:
                                # Live: last close = yesterday, N-ago = further back
                                prev_close = closes[-1]
                                n_ago = closes[-(self._config.trend_lookback_days + 1)]

                            if n_ago > 0:
                                entry["trend_pct"] = (prev_close - n_ago) / n_ago * 100

                    # Turnover: avg over lookback, excluding trade_date itself
                    if turn_vals:
                        turnovers = [float(t) for t in turn_vals if t is not None and float(t) >= 0]
                        if turnovers:
                            if trade_date is not None and len(turnovers) >= 2:
                                # Last value is trade_date's turnover (buy-day)
                                entry["buy_day_turnover"] = turnovers[-1]
                                # Average from previous days
                                prev_turnovers = turnovers[:-1][-lookback:]
                                if prev_turnovers:
                                    entry["avg_daily_turnover"] = sum(prev_turnovers) / len(
                                        prev_turnovers
                                    )
                            else:
                                recent = turnovers[-lookback:]
                                if recent:
                                    entry["avg_daily_turnover"] = sum(recent) / len(recent)

                    # Volume: avg for live mode proxy
                    if vol_vals:
                        volumes = [float(v) for v in vol_vals if v is not None and float(v) > 0]
                        if volumes:
                            if trade_date is not None:
                                prev_volumes = volumes[:-1][-lookback:]
                            else:
                                prev_volumes = volumes[-lookback:]
                            if prev_volumes:
                                entry["avg_daily_volume"] = sum(prev_volumes) / len(prev_volumes)

                    if entry:
                        result[bare_code] = entry

            except Exception as e:
                logger.warning(f"QualityFilter: historical fetch failed for batch: {e}")

        return result
