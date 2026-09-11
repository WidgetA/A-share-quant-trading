"""Causal V22 entry rules; no network, orders, account state or research imports.

Reference outcomes describe the full original recommendation basket. A blocked
day still observes that basket, and its outcome may reset tomorrow's risk count.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from math import isfinite

from src.strategy.v20.models import HealthObservation, HealthSnapshot, HealthStatus


def advance_health(
    current: HealthSnapshot, observations: Sequence[HealthObservation], *, today: date
) -> HealthSnapshot:
    """Evaluate only today's latest three matured labels, once per new vintage."""
    by_day = {item.signal_date: item for item in current.recent_valid}
    for item in observations:
        if item.t2_exit_date >= today:
            raise ValueError("health input has not matured before this decision")
        if item.valid:
            existing = by_day.get(item.signal_date)
            if existing is not None and existing != item:
                raise ValueError("conflicting health observation")
            by_day[item.signal_date] = item
    recent = tuple(sorted(by_day.values(), key=lambda item: item.signal_date)[-3:])
    if len(recent) < 3:
        return HealthSnapshot(
            recent_valid=recent, last_processed_key=recent[-1].order_key if recent else None
        )
    vintage = tuple(item.signal_date for item in recent)
    previous = tuple(item.signal_date for item in current.recent_valid)
    returns = []
    for item in recent:
        if item.relative_return is None:
            raise ValueError("valid health observation must include its return")
        returns.append(item.relative_return)
    mean = sum(returns) / 3
    status, count = current.status, current.recovery_count
    if mean < 0:
        status, count = HealthStatus.PAUSED_R0, 0
    elif status in (HealthStatus.PAUSED_R0, HealthStatus.PAUSED_R1, HealthStatus.PAUSED_R2):
        if vintage != previous:
            count += 1
            status = HealthStatus.HEALTHY if count == 3 else HealthStatus(f"PAUSED_R{count}")
    elif status is HealthStatus.WARMUP:
        status = HealthStatus.HEALTHY
    return HealthSnapshot(status, count, recent, recent[-1].order_key)


def h90(current: Mapping[str, float], history: Sequence[Mapping[str, float]]) -> dict:
    """Same-stock intersections are computed separately for 10 and 20 days."""
    if len(history) != 20:
        raise ValueError("H90 requires all twenty preceding exchange sessions")
    for amounts in (*history, current):
        if not amounts or any(
            not code.startswith(("00", "60"))
            or isinstance(value, bool)
            or not isfinite(value)
            or value < 0
            for code, value in amounts.items()
        ):
            raise ValueError("invalid or missing whole-market early amount input")
    result: dict = {"known": False, "block": False}
    for horizon in (10, 20):
        window = history[-horizon:]
        common = set(current).intersection(*(set(amounts) for amounts in window))
        if not common:
            return {**result, "reason": "NO_COMMON_STOCKS"}
        codes = sorted(common)
        baseline = sum(sum(amounts[code] for code in codes) for amounts in window) / horizon
        if baseline <= 0:
            return {**result, "reason": "NONPOSITIVE_REFERENCE_AMOUNT"}
        result[f"h{horizon}_ratio"] = sum(current[code] for code in codes) / baseline
        result[f"h{horizon}_common_n"] = len(codes)
    return {
        **result,
        "known": True,
        "reason": "OK",
        "block": result["h10_ratio"] <= 0.90 and result["h20_ratio"] <= 0.90,
    }


def strong_condition(
    *, prior_amount: float, earlier_amount: float, index_closes: Sequence[float]
) -> bool:
    if len(index_closes) != 20 or any(
        isinstance(value, bool) or not isfinite(value) or value <= 0
        for value in (prior_amount, earlier_amount, *index_closes)
    ):
        raise ValueError("D0-N2 requires two completed market totals and twenty index closes")
    return prior_amount < earlier_amount and index_closes[-1] <= sum(index_closes) / 20


def close_risk_count(
    before: int, *, has_signal: bool, normal_open: bool, reference_return: float | None
) -> int:
    if type(before) is not int or before < 0:
        raise ValueError("risk count must be a nonnegative integer")
    if not has_signal:
        return before
    if reference_return is not None and not isfinite(reference_return):
        raise ValueError("invalid D0 reference return")
    if not normal_open or (reference_return is not None and reference_return < 0):
        return before + 1
    if reference_return is not None and reference_return > 0:
        return 0
    return before


@dataclass(frozen=True)
class EntryGate:
    normal_open: bool
    final_open: bool
    multiplier: float
    reason_codes: tuple[str, ...]


def entry_gate(
    *, count: int, v20_weight: float, h90_block: bool, risk_before: int, strong: bool
) -> EntryGate:
    if type(count) is not int or not 0 <= count <= 10:
        raise ValueError("invalid full recommendation count")
    if v20_weight not in (0.0, 0.25, 0.5, 1.0) or type(risk_before) is not int or risk_before < 0:
        raise ValueError("invalid frozen entry state")
    normal = count > 0 and v20_weight > 0 and not h90_block
    reasons = []
    if count == 0:
        reasons.append("NO_SIGNAL")
    if v20_weight == 0:
        reasons.append("V20_BASE_DEFENSE_BLOCK")
    if h90_block:
        reasons.append("H90_BLOCK")
    if risk_before >= 2 and strong:
        reasons.append("D0_N2_BLOCK")
    final = normal and not (risk_before >= 2 and strong)
    return EntryGate(normal, final, float(final), tuple(reasons) or ("V22_SLIM_OPEN",))
