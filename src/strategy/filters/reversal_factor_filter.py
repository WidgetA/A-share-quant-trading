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

    # Adaptive percentile filtering: filter stocks whose surge_volume_ratio
    # is above this percentile of the daily distribution.
    # 80 = filter the top 20% most suspicious stocks.
    filter_percentile: float = 80.0

    # Minimum ratio floor — stocks below this ratio are never filtered,
    # even if they fall above the percentile cutoff. Prevents filtering
    # stocks that are genuinely fine on low-dispersion days.
    min_ratio_floor: float = 0.08

    # Minimum number of stocks with valid ratios required for percentile
    # filtering. Below this count, only the floor is used.
    min_sample_for_percentile: int = 5


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
        """Filter stocks showing thin-volume surge using adaptive percentile cutoff.

        Two-phase approach:
        1. Compute surge_volume_ratio for all stocks.
        2. Determine adaptive cutoff from percentile distribution, then filter.

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

        # --- Phase 1: Compute ratios for all stocks ---
        assessments: list[ReversalAssessment] = []
        for stock in selected_stocks:
            snap = price_snapshots.get(stock.stock_code)
            avg_vol = vol_data.get(stock.stock_code)
            assessment = self._compute_ratio(stock, snap, avg_vol)
            assessments.append(assessment)

        # --- Phase 2: Determine adaptive cutoff ---
        valid_ratios = [
            a.surge_volume_ratio for a in assessments if a.surge_volume_ratio is not None
        ]

        cutoff = self._config.min_ratio_floor
        cutoff_source = "floor"

        if len(valid_ratios) >= self._config.min_sample_for_percentile:
            sorted_ratios = sorted(valid_ratios)
            pct_idx = int(len(sorted_ratios) * self._config.filter_percentile / 100.0)
            pct_idx = min(pct_idx, len(sorted_ratios) - 1)
            pct_cutoff = sorted_ratios[pct_idx]
            # Use the higher of percentile cutoff and floor
            if pct_cutoff > cutoff:
                cutoff = pct_cutoff
                cutoff_source = f"P{self._config.filter_percentile:.0f}"

        # --- Phase 3: Apply filter ---
        kept: list[SelectedStock] = []
        for stock, assessment in zip(selected_stocks, assessments):
            ratio = assessment.surge_volume_ratio
            if ratio is not None and ratio >= cutoff:
                assessment.filtered_out = True
                assessment.reasons.append(
                    f"缩量冲高 ratio={ratio:.2f}≥{cutoff:.2f}"
                    f" ({cutoff_source}阈值,"
                    f" 冲高{assessment.surge_pct:.1%},"
                    f" 相对量{assessment.relative_volume:.1%})"
                )
                logger.info(
                    f"ReversalFilter: FILTERED {stock.stock_code} {stock.stock_name} "
                    f"— {'; '.join(assessment.reasons)}"
                )
            else:
                kept.append(stock)

        # --- Diagnostics ---
        valid = [a for a in assessments if a.surge_volume_ratio is not None]
        if valid:
            ratios = sorted(
                [a.surge_volume_ratio for a in valid if a.surge_volume_ratio is not None],
                reverse=True,
            )
            top5 = ", ".join(f"{r:.3f}" for r in ratios[:5])
            no_data = len(assessments) - len(valid)
            logger.info(
                f"ReversalFilter: ratio distribution (n={len(valid)}, no_data={no_data}): "
                f"max={ratios[0]:.3f} median={ratios[len(ratios) // 2]:.3f} "
                f"min={ratios[-1]:.3f} top5=[{top5}] "
                f"cutoff={cutoff:.3f}({cutoff_source})"
            )
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
            f"({len(selected_stocks) - len(kept)} filtered, "
            f"cutoff={cutoff:.3f} via {cutoff_source})"
        )
        return kept, assessments

    def _compute_ratio(
        self,
        stock: SelectedStock,
        snap: PriceSnapshot | None,
        avg_daily_volume: float | None,
    ) -> ReversalAssessment:
        """Compute surge volume ratio for a single stock (no filtering decision)."""
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

        ratio = None
        surge_pct = 0.0
        relative_volume = 0.0

        # Skip if no avg_daily_volume available (stock not in quality filter results,
        # e.g. it was added in step 5 but wasn't in L1). Let it pass — we don't
        # have enough data to judge. In the scanner pipeline, missing avg_vol means
        # the stock joined late; halting would be too aggressive.
        if avg_daily_volume is not None and avg_daily_volume > 0:
            ratio, surge_pct, relative_volume = self.compute_surge_volume_ratio(
                snap.open_price, snap.high_price, snap.early_volume, avg_daily_volume
            )

        return ReversalAssessment(
            stock_code=stock.stock_code,
            filtered_out=False,  # Decided at batch level in filter_stocks()
            reasons=[],
            surge_volume_ratio=ratio,
            surge_pct=surge_pct,
            relative_volume=relative_volume,
        )
