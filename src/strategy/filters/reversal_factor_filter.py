# === MODULE PURPOSE ===
# Detects stocks prone to intraday 冲高回落 (pump-and-dump reversal)
# using real-time snapshot data at ~9:40am.
#
# Primary factor: Early Fade — how much of the 9:30-9:40 peak has been
# given back by scan time. Stocks that surged then faded are likely to
# keep fading for the rest of the day.
#
# Key insight: scanner selects stocks with gain_from_open > 0.56%,
# so erosion-based filters (gap giveback) never trigger. Early Fade
# works because it measures fade FROM INTRADAY HIGH, not from open.

# === EVIDENCE ===
# Tested via scripts/optimize_early_fade.py, 2244 scanner-condition samples
# (gain_from_open >= 0.56%, main board, 199 stocks, 2025-12~2026-02):
#   Base loss rate: 23.8%, base return: +2.34%
#   Early Fade 0.55: precision 34.8%, recall 14.6%, but 65% false alarm
#   Early Fade 0.70: precision 39.3%, recall 4.1%, 61% false alarm
#   Conclusion: factor has limited standalone power for scanner candidates;
#   lowering threshold increases false alarms without meaningful gain.
#   Price Position <= 0.25: precision 55.6%, lift 2.34x (few samples, kept as-is).

# === DATA FLOW ===
# PriceSnapshot (with high_price) → ReversalFactorFilter → filtered list
# No extra iFinD API calls needed — uses data already in PriceSnapshot.

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
    """Thresholds for the reversal filter."""

    enabled: bool = True

    # Early Fade: (high - latest) / (high - open).
    # How much of the peak-to-open gain has been given back.
    # 0.7 = stock gave back 70% of its intraday surge.
    early_fade_threshold: float = 0.7

    # Price Position: (latest - low) / (high - low).
    # Where current price sits in the 10-min range.
    # 0.25 = stock is in the bottom 25% of its range.
    price_position_threshold: float = 0.25

    # How many factors must trigger to filter out (1 or 2).
    min_triggers: int = 1


@dataclass
class ReversalAssessment:
    """Assessment result for one stock's reversal risk."""

    stock_code: str
    filtered_out: bool
    reasons: list[str] = field(default_factory=list)
    early_fade: float | None = None
    price_position: float | None = None

    @property
    def trigger_count(self) -> int:
        return len(self.reasons)


class ReversalFactorFilter:
    """
    Filters stocks prone to intraday reversal (冲高回落).

    Uses real-time snapshot data at ~9:40am — no extra API calls needed.

    Primary signal: Early Fade — if a stock gapped up and surged further
    but has already given back most of its surge by 9:40, it's likely to
    keep fading. This is the only factor that works for scanner-selected
    stocks (which are above open price at 9:40).

    Fail-fast: if snapshot or high_price unavailable, raises error to halt trading.

    Usage:
        f = ReversalFactorFilter()
        kept, assessments = await f.filter_stocks(selected, snapshots)
    """

    def __init__(self, config: ReversalFactorConfig | None = None):
        self._config = config or ReversalFactorConfig()

    # === STATIC FACTOR COMPUTATIONS ===

    @staticmethod
    def compute_early_fade(
        open_price: float, high_price: float, latest_price: float
    ) -> float | None:
        """
        Early Fade: how much of the peak-to-open surge has been given back.

        early_fade = (high - latest) / (high - open)

        Range for scanner stocks (latest > open):
          0.0 = price is at the high (no fade)
          0.5 = gave back half the surge
          ~1.0 = price is back near open (almost all surge gone)

        Higher → stronger reversal signal.
        Returns None if high <= open (no surge to fade from).
        """
        if high_price <= open_price:
            return None
        surge = high_price - open_price
        fade = high_price - latest_price
        return fade / surge

    @staticmethod
    def compute_price_position(
        latest_price: float, high_price: float, low_price: float
    ) -> float | None:
        """
        Price Position: where current price sits in the 10-min range.

        position = (latest - low) / (high - low)

        0.0 = at the bottom of the range
        1.0 = at the top of the range

        Lower → weaker position → more reversal risk.
        Returns None if high == low (no range).
        """
        price_range = high_price - low_price
        if price_range <= 0:
            return None
        return (latest_price - low_price) / price_range

    # === FILTER INTERFACE ===

    async def filter_stocks(
        self,
        selected_stocks: list[SelectedStock],
        price_snapshots: dict[str, PriceSnapshot],
        trade_date: date | None = None,
    ) -> tuple[list[SelectedStock], list[ReversalAssessment]]:
        """Filter stocks prone to intraday reversal.

        Args:
            selected_stocks: Stocks that passed quality filter (Step 5.5).
            price_snapshots: Real-time snapshot data keyed by stock code.
            trade_date: Unused (kept for interface compatibility).

        Returns:
            (kept_stocks, assessments) tuple.
        """
        if not self._config.enabled or not selected_stocks:
            return selected_stocks, []

        kept: list[SelectedStock] = []
        assessments: list[ReversalAssessment] = []

        for stock in selected_stocks:
            snap = price_snapshots.get(stock.stock_code)
            assessment = self._assess(stock, snap)
            assessments.append(assessment)

            if assessment.filtered_out:
                logger.info(
                    f"ReversalFilter: FILTERED {stock.stock_code} {stock.stock_name} "
                    f"— {'; '.join(assessment.reasons)}"
                )
            else:
                kept.append(stock)

        logger.info(
            f"ReversalFilter: {len(kept)}/{len(selected_stocks)} passed "
            f"({len(selected_stocks) - len(kept)} filtered)"
        )
        return kept, assessments

    def _assess(
        self,
        stock: SelectedStock,
        snap: PriceSnapshot | None,
    ) -> ReversalAssessment:
        """Assess reversal risk from snapshot data."""
        reasons: list[str] = []
        early_fade = None
        price_position = None

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

        # Factor 1: Early Fade
        early_fade = self.compute_early_fade(snap.open_price, snap.high_price, snap.latest_price)
        if early_fade is not None and early_fade >= self._config.early_fade_threshold:
            reasons.append(f"冲高回落{early_fade:.0%}≥{self._config.early_fade_threshold:.0%}")

        # Factor 2: Price Position (only if low_price available)
        if snap.low_price > 0:
            price_position = self.compute_price_position(
                snap.latest_price, snap.high_price, snap.low_price
            )
            if (
                price_position is not None
                and price_position <= self._config.price_position_threshold
            ):
                reasons.append(
                    f"价格位置{price_position:.0%}≤{self._config.price_position_threshold:.0%}"
                )

        filtered = len(reasons) >= self._config.min_triggers

        return ReversalAssessment(
            stock_code=stock.stock_code,
            filtered_out=filtered,
            reasons=reasons if filtered else [],
            early_fade=early_fade,
            price_position=price_position,
        )
