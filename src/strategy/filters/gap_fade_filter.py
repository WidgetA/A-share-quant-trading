# === MODULE PURPOSE ===
# Post-selection filter to remove stocks with high "高开低走" (gap-up fade) risk.
# Applied after momentum selection to filter out stocks most likely to
# gap up then drop, causing losses when holding to next-day open.

# === KEY SIGNAL (v4 — validated across 5 time periods) ===
# AND combination: high early volume AND high turnover stock.
# Tested 13 factors × 5 periods (2024-10 ~ 2026-02), 10000 random permutations.
# Only 3 AND combos passed p<0.05 in ALL 5 periods; best is:
#   vol>2x avg AND turnover>top15% → avg false-kill 26.5%, p<0.001 all periods
# Logic: extreme volume + high turnover on gap-up day = sentiment climax / dumping,
# next-day open consistently lower.

# === DATA FLOW ===
# MomentumSectorScanner Step 5 → GapFadeFilter → Step 6 (recommend)

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
class GapFadeConfig:
    """Configuration for gap-fade filter thresholds."""

    enabled: bool = True

    # Signal 1: Early volume ratio.
    # early_volume (9:30-9:40) / avg daily volume (past N days).
    # Proxy for full-day volume ratio at scan time (~9:40).
    # Experiment validated daily vol>2x; at 9:40 first 10min ≈ 8-15% of daily,
    # so 0.25 ≈ a stock on pace for ~2x daily volume.
    max_volume_ratio: float = 0.25

    # Signal 2: Average daily turnover rate (%).
    # Stocks with consistently high turnover are "hot"/speculative names.
    # Experiment: top 15% turnover among gap-up stocks ≈ 10%;
    # we use historical avg, so threshold slightly lower.
    max_avg_turnover: float = 8.0

    # Number of trading days for average volume/turnover calculation.
    volume_lookback_days: int = 20


@dataclass
class FadeRiskResult:
    """Assessment result for a single stock's fade risk."""

    stock_code: str
    filtered_out: bool
    reasons: list[str] = field(default_factory=list)
    volume_ratio: float | None = None
    avg_turnover: float | None = None


class GapFadeFilter:
    """
    Filters out stocks with high gap-fade risk from momentum selections.

    Uses AND combination of two experiment-validated signals:
    - High early volume: early_volume / avg_daily_volume > threshold
    - High turnover stock: avg daily turnover > threshold
    Both must be true to filter (AND logic reduces false kills).

    Validated across 5 time periods (2024-10 ~ 2026-02), p<0.001 in all.

    Usage:
        filter = GapFadeFilter(ifind_client)
        kept, assessments = await filter.filter_stocks(selected, snapshots, trade_date)
    """

    def __init__(
        self,
        ifind_client: IFinDHttpClient,
        config: GapFadeConfig | None = None,
    ):
        self._ifind = ifind_client
        self._config = config or GapFadeConfig()

    async def filter_stocks(
        self,
        selected_stocks: list[SelectedStock],
        price_snapshots: dict[str, PriceSnapshot],
        trade_date: date | None = None,
    ) -> tuple[list[SelectedStock], list[FadeRiskResult]]:
        """
        Filter out stocks with high gap-fade risk.

        Args:
            selected_stocks: Stocks selected by momentum scanner (after PE filter).
            price_snapshots: Price data for all stocks (must include early_volume).
            trade_date: Trade date (for historical data fetch). None = live mode.

        Returns:
            (kept_stocks, all_assessments) — kept stocks and full risk assessments.
        """
        if not self._config.enabled or not selected_stocks:
            return selected_stocks, []

        # Fetch historical context (avg daily volume)
        codes = [s.stock_code for s in selected_stocks]
        historical = await self._fetch_historical_context(codes, trade_date)

        kept: list[SelectedStock] = []
        assessments: list[FadeRiskResult] = []

        for stock in selected_stocks:
            snap = price_snapshots.get(stock.stock_code)
            hist = historical.get(stock.stock_code)
            result = self._assess(stock, snap, hist)
            assessments.append(result)

            if result.filtered_out:
                logger.info(
                    f"GapFadeFilter: FILTERED {stock.stock_code} {stock.stock_name} "
                    f"— {'; '.join(result.reasons)}"
                )
            else:
                kept.append(stock)

        logger.info(
            f"GapFadeFilter: {len(kept)}/{len(selected_stocks)} stocks passed "
            f"({len(selected_stocks) - len(kept)} filtered out)"
        )
        return kept, assessments

    def _assess(
        self,
        stock: SelectedStock,
        snap: PriceSnapshot | None,
        hist: dict | None,
    ) -> FadeRiskResult:
        """Assess a single stock's fade risk using AND logic."""
        reasons: list[str] = []
        volume_ratio: float | None = None
        avg_turnover: float | None = hist.get("avg_daily_turnover") if hist else None

        # AND logic: both volume AND turnover must exceed thresholds.
        # Fail-open: if either metric is unavailable, don't filter.
        vol_high = False
        turn_high = False

        if snap and snap.early_volume > 0 and hist and hist.get("avg_daily_volume"):
            avg_vol = hist["avg_daily_volume"]
            volume_ratio = snap.early_volume / avg_vol
            vol_high = volume_ratio > self._config.max_volume_ratio

        if avg_turnover is not None:
            turn_high = avg_turnover > self._config.max_avg_turnover

        if vol_high and turn_high:
            reasons.append(
                f"放量{volume_ratio:.2f}x>{self._config.max_volume_ratio}x"
                f" AND 高换手{avg_turnover:.1f}%>{self._config.max_avg_turnover}%"
            )

        return FadeRiskResult(
            stock_code=stock.stock_code,
            filtered_out=len(reasons) > 0,
            reasons=reasons,
            volume_ratio=volume_ratio,
            avg_turnover=avg_turnover,
        )

    async def _fetch_historical_context(
        self,
        stock_codes: list[str],
        trade_date: date | None = None,
    ) -> dict[str, dict]:
        """
        Fetch historical daily volume and turnover for average calculations.

        Returns dict: stock_code → {
            "avg_daily_volume": float | None,
            "avg_daily_turnover": float | None,  # percentage
        }
        """
        if not stock_codes:
            return {}

        ref_date = trade_date or date.today()
        # Need enough calendar days to cover volume_lookback_days trading days
        calendar_buffer = self._config.volume_lookback_days * 2 + 5
        start = ref_date - timedelta(days=calendar_buffer)
        end = ref_date - timedelta(days=1)  # Up to previous trading day

        result: dict[str, dict] = {}
        batch_size = 50

        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i : i + batch_size]
            codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)

            try:
                data = await self._ifind.history_quotes(
                    codes=codes_str,
                    indicators="volume,turnoverRatio",
                    start_date=start.strftime("%Y-%m-%d"),
                    end_date=end.strftime("%Y-%m-%d"),
                )

                for table_entry in data.get("tables", []):
                    thscode = table_entry.get("thscode", "")
                    bare_code = thscode.split(".")[0] if thscode else ""
                    if not bare_code:
                        continue

                    tbl = table_entry.get("table", {})
                    vol_vals = tbl.get("volume", [])
                    turn_vals = tbl.get("turnoverRatio", [])

                    lookback = self._config.volume_lookback_days
                    entry: dict = {}

                    if vol_vals:
                        volumes = [float(v) for v in vol_vals if v is not None and float(v) > 0]
                        if volumes:
                            recent = volumes[-lookback:]
                            entry["avg_daily_volume"] = sum(recent) / len(recent)

                    if turn_vals:
                        turnovers = [float(t) for t in turn_vals if t is not None and float(t) >= 0]
                        if turnovers:
                            recent = turnovers[-lookback:]
                            entry["avg_daily_turnover"] = sum(recent) / len(recent)

                    if entry:
                        result[bare_code] = entry

            except Exception as e:
                logger.warning(f"GapFadeFilter: historical fetch failed for batch: {e}")

        return result
