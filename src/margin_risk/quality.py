"""Pure data-quality rules for market and security financing balances."""

from __future__ import annotations

from statistics import median
from typing import Iterable

from src.margin_risk.config import MarginRiskConfig
from src.margin_risk.models import DataStatus


def identity_tolerance(balance: float, config: MarginRiskConfig) -> float:
    return max(
        config.identity_absolute_tolerance,
        abs(balance) * config.identity_relative_tolerance,
    )


def balance_identity_error(
    current_balance: float,
    previous_balance: float,
    buy_amount: float,
    repayment_amount: float,
) -> float:
    """Return actual minus balance implied by buy and repayment flows."""

    return current_balance - (previous_balance + buy_amount - repayment_amount)


def balance_identity_is_valid(
    current_balance: float,
    previous_balance: float,
    buy_amount: float,
    repayment_amount: float,
    config: MarginRiskConfig,
) -> bool:
    error = balance_identity_error(
        current_balance,
        previous_balance,
        buy_amount,
        repayment_amount,
    )
    return abs(error) <= identity_tolerance(current_balance, config)


def detail_coverage_status(
    current: float | None,
    prior_values: Iterable[float | None],
    config: MarginRiskConfig,
) -> DataStatus:
    """Gate breadth metrics when detail coverage suddenly deteriorates.

    The comparison excludes the current observation. Before a useful history
    exists the endpoint completeness checks remain the stronger gate, so no
    coverage downgrade is inferred from a tiny startup sample.
    """

    if current is None:
        return DataStatus.PARTIAL
    valid = [float(v) for v in prior_values if v is not None]
    if len(valid) < min(config.detail_coverage_min_history, config.detail_coverage_window):
        return DataStatus.OK
    baseline = median(valid[-config.detail_coverage_window :])
    if current < baseline - config.detail_coverage_drop:
        return DataStatus.PARTIAL
    return DataStatus.OK
