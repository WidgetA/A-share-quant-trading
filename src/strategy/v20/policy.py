"""Deterministic V20 entry policy.

All returns in this module are gross price returns.  There is deliberately no
fee, tax, commission, or slippage argument: V20's frozen strategy profile is
``ZERO_COST_GROSS_PRICE_RETURN_V1``.
"""

from __future__ import annotations

from datetime import date
from math import ceil, floor, isfinite, sqrt
from statistics import median
from typing import Iterable, Mapping, Sequence

from .models import (
    BaseDecision,
    BreadthSnapshot,
    EntryAction,
    EntryDecision,
    GDecision,
    GStatus,
    HealthObservation,
    HealthSnapshot,
    HealthStatus,
    Q25Threshold,
    Rolling7Decision,
    Rolling7Status,
    RollingBatch,
    RollingGap,
    StockThemeInput,
    ThemeMapping,
)

WILSON_Z = 1.645
MIN_BREADTH_N = 1_000
ROLLING_WINDOW_SIZE = 7
G_TOP_N = 10
G_MAX_CLUSTER_SIZE = 3


def gross_price_return(*, entry_price: float, exit_price: float) -> float:
    """Return the frozen zero-cost price return for one model leg."""

    for name, value in (("entry_price", entry_price), ("exit_price", exit_price)):
        if not isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be finite and positive")
    return exit_price / entry_price - 1.0


def equal_weight_batch_return(leg_returns: Sequence[float]) -> float:
    """Arithmetic mean over every leg; missing legs must not be dropped."""

    if not leg_returns:
        raise ValueError("a shadow batch must contain at least one leg")
    if any(not isfinite(value) for value in leg_returns):
        raise ValueError("every leg return must be finite")
    return sum(leg_returns) / len(leg_returns)


def relative_health_return(
    *, top3_leg_returns: Sequence[float], comparison_pool_leg_returns: Sequence[float]
) -> float:
    if len(top3_leg_returns) != 3:
        raise ValueError("health probe requires exactly three ranked legs")
    if len(comparison_pool_leg_returns) < 1_000:
        raise ValueError("health comparison pool requires at least 1000 valid legs")
    return equal_weight_batch_return(top3_leg_returns) - equal_weight_batch_return(
        comparison_pool_leg_returns
    )


def advance_health_state(
    current: HealthSnapshot, observations: Iterable[HealthObservation]
) -> HealthSnapshot:
    """Consume new terminal health windows in the frozen deterministic order.

    Inputs at or behind the persisted watermark are ignored, making recovery
    replay idempotent.  A caller that needs mutation detection must compare the
    immutable input hashes in the persistence layer.
    """

    ordered = sorted(observations, key=lambda item: item.order_key)
    keys = [item.order_key for item in ordered]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate health order key")
    batch_ids = [item.batch_id for item in ordered]
    if len(batch_ids) != len(set(batch_ids)):
        raise ValueError("duplicate health batch_id")

    status = current.status
    recovery_count = current.recovery_count
    recent = list(current.recent_valid)
    watermark = current.last_processed_key

    for observation in ordered:
        if watermark is not None and observation.order_key <= watermark:
            continue
        watermark = observation.order_key
        if not observation.valid:
            continue

        recent.append(observation)
        recent = recent[-3:]
        if len(recent) < 3:
            status = HealthStatus.WARMUP
            recovery_count = 0
            continue

        mean_relative_return = sum(item.relative_return or 0.0 for item in recent) / 3.0
        if mean_relative_return < 0.0:
            status = HealthStatus.PAUSED_R0
            recovery_count = 0
            continue

        if status is HealthStatus.PAUSED_R0:
            status = HealthStatus.PAUSED_R1
            recovery_count = 1
        elif status is HealthStatus.PAUSED_R1:
            status = HealthStatus.PAUSED_R2
            recovery_count = 2
        elif status is HealthStatus.PAUSED_R2:
            status = HealthStatus.HEALTHY
            recovery_count = 3
        elif status is HealthStatus.HEALTHY:
            # A recovered HEALTHY state keeps its three-confirmation history.
            recovery_count = 3 if recovery_count == 3 else 0
        else:
            status = HealthStatus.HEALTHY
            recovery_count = 0

    return HealthSnapshot(
        status=status,
        recovery_count=recovery_count,
        recent_valid=tuple(recent),
        last_processed_key=watermark,
    )


def wilson_one_sided_lower_bound(*, declining_n: int, valid_n: int, z: float = WILSON_Z) -> float:
    if valid_n <= 0:
        raise ValueError("valid_n must be positive")
    if declining_n < 0 or declining_n > valid_n:
        raise ValueError("declining_n must be between zero and valid_n")
    if not isfinite(z) or z <= 0:
        raise ValueError("z must be finite and positive")
    p = declining_n / valid_n
    z2 = z * z
    numerator = (
        p + z2 / (2 * valid_n) - z * sqrt(p * (1 - p) / valid_n + z2 / (4 * valid_n * valid_n))
    )
    return numerator / (1 + z2 / valid_n)


def multiplier_from_wilson_lower_bound(lower_bound: float) -> float:
    """Apply the inclusive 0.50/0.60 BASE boundaries."""

    if not isfinite(lower_bound) or lower_bound < 0 or lower_bound > 1:
        raise ValueError("Wilson lower bound must be in [0, 1]")
    if lower_bound <= 0.50:
        return 1.0
    if lower_bound <= 0.60:
        return 0.5
    return 0.0


def decide_base(health: HealthSnapshot, breadth: BreadthSnapshot | None = None) -> BaseDecision:
    if health.status in (HealthStatus.WARMUP, HealthStatus.HEALTHY):
        return BaseDecision(1.0, False, None, f"BASE_{health.status.value}")
    if breadth is None:
        return BaseDecision(0.0, True, None, "BASE_PAUSED_BREADTH_MISSING")
    if breadth.valid_n < MIN_BREADTH_N:
        return BaseDecision(0.0, True, None, "BASE_HEALTH_UNIVERSE_LT_1000")
    lower_bound = wilson_one_sided_lower_bound(
        declining_n=breadth.declining_n, valid_n=breadth.valid_n
    )
    multiplier = multiplier_from_wilson_lower_bound(lower_bound)
    suffix = {1.0: "FULL", 0.5: "HALF", 0.0: "BLOCK"}[multiplier]
    return BaseDecision(multiplier, True, lower_bound, f"BASE_PAUSED_BREADTH_{suffix}")


def _gap_is_active(
    gap: RollingGap, eligible_batches: Sequence[RollingBatch], decision_date: date
) -> bool:
    if gap.closed or gap.aged_out or gap.gap_maturity_date >= decision_date:
        return False
    later_complete_n = sum(batch.signal_date > gap.signal_date for batch in eligible_batches)
    return later_complete_n < ROLLING_WINDOW_SIZE


def evaluate_rolling7(
    *,
    decision_date: date,
    complete_batches: Iterable[RollingBatch],
    gaps: Iterable[RollingGap] = (),
    information_clock_valid: bool = True,
) -> Rolling7Decision:
    batches = list(complete_batches)
    batch_ids = [batch.batch_id for batch in batches]
    if len(batch_ids) != len(set(batch_ids)):
        raise ValueError("duplicate rolling batch_id")
    signal_dates = [batch.signal_date for batch in batches]
    if len(signal_dates) != len(set(signal_dates)):
        raise ValueError("only one rolling shadow batch may exist per signal date")

    eligible = sorted(
        (batch for batch in batches if batch.t2_exit_date < decision_date),
        key=lambda batch: (batch.signal_date, batch.batch_id),
    )
    if not information_clock_valid:
        return Rolling7Decision(
            Rolling7Status.UNKNOWN,
            None,
            None,
            (),
            unknown_reason="INFORMATION_CLOCK_INVALID",
        )

    gap_list = list(gaps)
    gap_ids = [gap.gap_id for gap in gap_list]
    if len(gap_ids) != len(set(gap_ids)):
        raise ValueError("duplicate rolling gap_id")
    active_gap_ids = tuple(
        sorted(gap.gap_id for gap in gap_list if _gap_is_active(gap, eligible, decision_date))
    )
    if active_gap_ids:
        return Rolling7Decision(
            Rolling7Status.DATA_GAP,
            None,
            None,
            (),
            active_gap_ids=active_gap_ids,
            unknown_reason="DATA_GAP:" + ",".join(active_gap_ids),
        )
    if len(eligible) < ROLLING_WINDOW_SIZE:
        return Rolling7Decision(
            Rolling7Status.WARMUP,
            None,
            None,
            tuple(eligible),
            unknown_reason=f"WARMUP:{len(eligible)}/{ROLLING_WINDOW_SIZE}",
        )

    window = tuple(eligible[-ROLLING_WINDOW_SIZE:])
    r7 = sum(batch.gross_price_return for batch in window)
    l7 = sum(batch.gross_price_return < 0.0 for batch in window)
    status = Rolling7Status.BAD if r7 < 0.0 and l7 >= 5 else Rolling7Status.NON_BAD
    return Rolling7Decision(status, r7, l7, window)


def decision_half_for_date(decision_date: date) -> str:
    return f"{decision_date.year}H{1 if decision_date.month <= 6 else 2}"


def linear_quantile_25(values: Sequence[float]) -> float:
    """Frozen linear Q25 interpolation; no library defaults are consulted."""

    if not values or any(not isfinite(value) for value in values):
        raise ValueError("quantile input must be non-empty and finite")
    ordered = sorted(values)
    h = (len(ordered) - 1) * 0.25
    i, j = floor(h), ceil(h)
    return ordered[i] + (h - i) * (ordered[j] - ordered[i])


def _unknown_g(reason: str, *, amount_valid_n: int = 0) -> GDecision:
    return GDecision(GStatus.UNKNOWN, None, amount_valid_n, None, None, None, None, reason)


def evaluate_g(
    *,
    decision_date: date,
    recommendations: Sequence[StockThemeInput],
    mapping: Mapping[str, ThemeMapping],
    prior_trade_amounts: Mapping[str, float],
    threshold: Q25Threshold | None,
) -> GDecision:
    """Evaluate the BAD-only semantic-dispersion and weak-amount gate."""

    if len(recommendations) < G_TOP_N:
        return _unknown_g("TOP10_INCOMPLETE")
    top10 = tuple(recommendations[:G_TOP_N])
    codes = [stock.code for stock in top10]
    if len(codes) != len(set(codes)):
        return _unknown_g("TOP10_DUPLICATE_CODE")
    if threshold is None:
        return _unknown_g("Q25_THRESHOLD_MISSING")
    if threshold.decision_half != decision_half_for_date(decision_date):
        return _unknown_g("Q25_THRESHOLD_HALF_MISMATCH")

    allowed_themes_by_code: dict[str, set[str]] = {}
    for stock in top10:
        labels = stock.labels
        if not labels:
            return _unknown_g(f"EMPTY_LABELS:{stock.code}")
        allowed: set[str] = set()
        for raw_label in labels:
            record = mapping.get(raw_label)
            if record is None or record.raw_label != raw_label:
                return _unknown_g(f"UNMAPPED_LABEL:{raw_label}")
            if record.cluster_allowed:
                allowed.add(record.canonical_theme_id)
        allowed_themes_by_code[stock.code] = allowed

    parent = {code: code for code in codes}

    def find(code: str) -> str:
        while parent[code] != code:
            parent[code] = parent[parent[code]]
            code = parent[code]
        return code

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    theme_owner: dict[str, str] = {}
    for code in codes:
        for canonical_theme in sorted(allowed_themes_by_code[code]):
            owner = theme_owner.setdefault(canonical_theme, code)
            union(owner, code)
    component_sizes: dict[str, int] = {}
    for code in codes:
        root = find(code)
        component_sizes[root] = component_sizes.get(root, 0) + 1
    max_cluster_size = max(component_sizes.values())

    amount_candidates = [prior_trade_amounts.get(code) for code in codes]
    amount_valid_n = sum(
        amount is not None and isfinite(amount) and amount > 0 for amount in amount_candidates
    )
    if amount_valid_n < G_TOP_N:
        return GDecision(
            GStatus.UNKNOWN,
            max_cluster_size,
            amount_valid_n,
            None,
            None,
            None,
            None,
            "D1_AMOUNT_INCOMPLETE",
        )
    amounts = [float(amount) for amount in amount_candidates if amount is not None]
    amount_total = sum(amounts)
    amount_median = float(median(amounts))
    amount_bottom3 = sum(sorted(amounts)[:3])
    weak_metric_count = sum(
        (
            amount_total <= threshold.prior_amount_total,
            amount_median <= threshold.prior_amount_median,
            amount_bottom3 <= threshold.prior_amount_bottom3_sum,
        )
    )
    dispersed = max_cluster_size <= G_MAX_CLUSTER_SIZE
    weak_amount = weak_metric_count >= 2
    status = GStatus.TRIGGERED if dispersed and weak_amount else GStatus.CLEAR
    reason = "G_TRIGGERED" if status is GStatus.TRIGGERED else "G_CLEAR"
    return GDecision(
        status,
        max_cluster_size,
        G_TOP_N,
        amount_total,
        amount_median,
        amount_bottom3,
        weak_metric_count,
        reason,
    )


def combine_entry_decision(
    *,
    scan_valid: bool,
    recommendation_count: int,
    base: BaseDecision | None,
    rolling7: Rolling7Decision | None,
    g: GDecision | None = None,
) -> EntryDecision:
    """Compose BASE, rolling7, and G without account-size semantics."""

    if recommendation_count < 0:
        raise ValueError("recommendation_count cannot be negative")
    if not scan_valid:
        return EntryDecision(
            EntryAction.INPUT_INVALID,
            0.0,
            0.0,
            0.0,
            None,
            GStatus.NOT_EVALUATED,
            None,
            ("INPUT_INVALID",),
        )
    if recommendation_count == 0:
        return EntryDecision(
            EntryAction.NO_SIGNAL,
            0.0,
            0.0,
            0.0,
            None,
            GStatus.NOT_EVALUATED,
            None,
            ("NO_SIGNAL",),
        )
    if base is None or rolling7 is None or base.multiplier not in (0.0, 0.5, 1.0):
        return EntryDecision(
            EntryAction.INPUT_INVALID,
            0.0,
            0.0 if base is None else base.multiplier,
            0.0,
            None if rolling7 is None else rolling7.status,
            GStatus.NOT_EVALUATED,
            None,
            ("INPUT_INVALID", "POLICY_INPUT_MISSING"),
        )

    reasons = [base.reason]
    if base.multiplier == 0.0:
        reasons.append(f"ROLLING7_{rolling7.status.value}")
        return EntryDecision(
            EntryAction.BLOCK,
            0.0,
            0.0,
            1.0,
            rolling7.status,
            GStatus.NOT_EVALUATED if g is None else g.status,
            None,
            tuple(reasons),
        )

    defense_multiplier = 1.0
    g_status = GStatus.NOT_EVALUATED
    reasons.append(f"ROLLING7_{rolling7.status.value}")
    if rolling7.status is Rolling7Status.BAD:
        if g is None:
            return EntryDecision(
                EntryAction.INPUT_INVALID,
                0.0,
                base.multiplier,
                0.0,
                rolling7.status,
                GStatus.UNKNOWN,
                None,
                (*reasons, "INPUT_INVALID", "G_RESULT_MISSING"),
            )
        g_status = g.status
        reasons.append(g.reason)
        defense_multiplier = 0.0 if g.status is GStatus.TRIGGERED else 0.5

    final_multiplier = base.multiplier * defense_multiplier
    action = EntryAction.ENTER if final_multiplier > 0.0 else EntryAction.BLOCK
    return EntryDecision(
        action,
        final_multiplier,
        base.multiplier,
        defense_multiplier,
        rolling7.status,
        g_status,
        final_multiplier / recommendation_count if final_multiplier > 0.0 else None,
        tuple(reasons),
    )
