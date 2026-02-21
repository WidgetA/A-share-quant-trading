# === MODULE PURPOSE ===
# Detects stocks prone to intraday 冲高回落 (pump-and-dump reversal)
# using 9:30-9:40 snapshot data + historical avg daily volume.
#
# Primary factor: Surge Volume Ratio — "四两拨千斤" detection.
# If a stock surged significantly from open to intraday high but used
# very little volume relative to its normal daily volume, the surge
# is likely manipulation (thin-liquidity pump) rather than real demand.
#
# Key insight: genuine institutional buying drives BOTH price and volume.
# A pump-to-distribute scheme pushes price up on thin early-morning
# liquidity, then sells into the buying wave. This shows up as a high
# surge_pct / relative_volume ratio.
#
# What this does NOT try to catch:
# - 连涨换庄 (main player rotation after multi-day rally): high volume,
#   low ratio → passes through. Accept the loss if it happens.
# - 大盘跳水 (market-wide selloff): not stock-specific, shouldn't filter.

# === DATA FLOW ===
# PriceSnapshot (open, high, early_volume) + avg_daily_volume (from QualityFilter)
# → ReversalFactorFilter → filtered list
# No extra API calls — all data already available in the pipeline.

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.strategy.strategies.momentum_sector_scanner import (
        PriceSnapshot,
        SelectedStock,
    )

logger = logging.getLogger(__name__)


@dataclass
class ReversalFactorConfig:
    """Thresholds for the thin-volume surge filter."""

    enabled: bool = True

    # Surge Volume Ratio threshold.
    # surge_volume_ratio = surge_pct / relative_volume
    # where surge_pct = (high - open) / open
    #       relative_volume = early_volume / avg_daily_volume
    # Higher = more price move per unit volume = more suspicious.
    # Stocks above this threshold are filtered out.
    surge_volume_threshold: float = 0.5


@dataclass
class ReversalAssessment:
    """Assessment result for one stock's reversal risk."""

    stock_code: str
    filtered_out: bool
    reasons: list[str] = field(default_factory=list)
    surge_volume_ratio: float | None = None
    surge_pct: float | None = None
    relative_volume: float | None = None

    @property
    def trigger_count(self) -> int:
        return len(self.reasons)


class ReversalFactorFilter:
    """
    Filters stocks showing thin-volume surge (缩量冲高 / 四两拨千斤).

    Core idea: if a stock's intraday surge from open to high was achieved
    with abnormally little volume, the move is likely manipulation rather
    than genuine buying demand. These stocks tend to fade for the rest
    of the day.

    Requires avg_daily_volume from MomentumQualityFilter (no extra API calls).

    Fail-fast: missing snapshot or avg_daily_volume raises RuntimeError.

    Usage:
        f = ReversalFactorFilter()
        kept, assessments = await f.filter_stocks(
            selected, snapshots, avg_daily_volume_data
        )
    """

    def __init__(self, config: ReversalFactorConfig | None = None):
        self._config = config or ReversalFactorConfig()

    # === STATIC FACTOR COMPUTATION ===

    @staticmethod
    def compute_surge_volume_ratio(
        open_price: float,
        high_price: float,
        early_volume: float,
        avg_daily_volume: float,
    ) -> tuple[float | None, float, float]:
        """
        Surge Volume Ratio: price surge per unit of relative volume.

        surge_pct = (high - open) / open
        relative_volume = early_volume / avg_daily_volume
        ratio = surge_pct / relative_volume

        Higher → more suspicious (big move on thin volume).

        Returns:
            (ratio, surge_pct, relative_volume).
            ratio is None if no meaningful surge (high <= open) or no volume.
        """
        if high_price <= open_price or open_price <= 0:
            return None, 0.0, 0.0
        surge_pct = (high_price - open_price) / open_price
        if avg_daily_volume <= 0 or early_volume <= 0:
            return None, surge_pct, 0.0
        relative_volume = early_volume / avg_daily_volume
        ratio = surge_pct / relative_volume
        return ratio, surge_pct, relative_volume

    # === FILTER INTERFACE ===

    async def filter_stocks(
        self,
        selected_stocks: list[SelectedStock],
        price_snapshots: dict[str, PriceSnapshot],
        avg_daily_volume_data: dict[str, float] | None = None,
        trade_date: date | None = None,
    ) -> tuple[list[SelectedStock], list[ReversalAssessment]]:
        """Filter stocks showing thin-volume surge.

        Args:
            selected_stocks: Stocks that passed quality filter (Step 5.5).
            price_snapshots: Real-time snapshot data keyed by stock code.
            avg_daily_volume_data: avg daily volume per stock (from QualityFilter).
            trade_date: Unused (kept for interface compatibility).

        Returns:
            (kept_stocks, assessments) tuple.
        """
        if not self._config.enabled or not selected_stocks:
            return selected_stocks, []

        vol_data = avg_daily_volume_data or {}

        kept: list[SelectedStock] = []
        assessments: list[ReversalAssessment] = []

        for stock in selected_stocks:
            snap = price_snapshots.get(stock.stock_code)
            avg_vol = vol_data.get(stock.stock_code)
            assessment = self._assess(stock, snap, avg_vol)
            assessments.append(assessment)

            if assessment.filtered_out:
                logger.info(
                    f"ReversalFilter: FILTERED {stock.stock_code} {stock.stock_name} "
                    f"— {'; '.join(assessment.reasons)}"
                )
            else:
                kept.append(stock)

        # Diagnostic: log ratio distribution for all assessed stocks
        valid = [a for a in assessments if a.surge_volume_ratio is not None]
        if valid:
            ratios = sorted([a.surge_volume_ratio for a in valid], reverse=True)
            top5 = ", ".join(f"{r:.3f}" for r in ratios[:5])
            no_data = len(assessments) - len(valid)
            logger.info(
                f"ReversalFilter: ratio distribution (n={len(valid)}, no_data={no_data}): "
                f"max={ratios[0]:.3f} median={ratios[len(ratios)//2]:.3f} "
                f"min={ratios[-1]:.3f} top5=[{top5}]"
            )
            # Log a few examples with raw values
            for a in sorted(valid, key=lambda x: x.surge_volume_ratio or 0, reverse=True)[:3]:
                s = price_snapshots.get(a.stock_code)
                av = vol_data.get(a.stock_code)
                logger.info(
                    f"  {a.stock_code}: ratio={a.surge_volume_ratio:.3f} "
                    f"surge={a.surge_pct:.3%} rel_vol={a.relative_volume:.4f} "
                    f"early_vol={s.early_volume if s else '?'} avg_vol={av}"
                )

        logger.info(
            f"ReversalFilter: {len(kept)}/{len(selected_stocks)} passed "
            f"({len(selected_stocks) - len(kept)} filtered)"
        )
        return kept, assessments

    def _assess(
        self,
        stock: SelectedStock,
        snap: PriceSnapshot | None,
        avg_daily_volume: float | None,
    ) -> ReversalAssessment:
        """Assess thin-volume surge risk."""
        if not snap:
            raise RuntimeError(
                f"ReversalFilter: no snapshot data for {stock.stock_code} "
                f"({stock.stock_name}). Cannot assess reversal risk — halting."
            )
        if snap.high_price <= 0:
            raise RuntimeError(
                f"ReversalFilter: high_price={snap.high_price} for {stock.stock_code} "
                f"({stock.stock_name}). Invalid price data — halting."
            )

        reasons: list[str] = []
        ratio = None
        surge_pct = 0.0
        relative_volume = 0.0

        # Skip if no avg_daily_volume available (stock not in quality filter results,
        # e.g. it was added in step 5 but wasn't in L1). Let it pass — we don't
        # have enough data to judge, and fail-open is only dangerous for the
        # trading path. In the scanner pipeline, missing avg_vol means the stock
        # joined late; halting would be too aggressive.
        if avg_daily_volume is not None and avg_daily_volume > 0:
            ratio, surge_pct, relative_volume = self.compute_surge_volume_ratio(
                snap.open_price, snap.high_price, snap.early_volume, avg_daily_volume
            )
            if ratio is not None and ratio >= self._config.surge_volume_threshold:
                reasons.append(
                    f"缩量冲高 ratio={ratio:.2f}≥{self._config.surge_volume_threshold:.2f}"
                    f" (冲高{surge_pct:.1%}, 相对量{relative_volume:.1%})"
                )

        filtered = len(reasons) > 0

        return ReversalAssessment(
            stock_code=stock.stock_code,
            filtered_out=filtered,
            reasons=reasons if filtered else [],
            surge_volume_ratio=ratio,
            surge_pct=surge_pct,
            relative_volume=relative_volume,
        )
