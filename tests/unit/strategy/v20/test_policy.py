from __future__ import annotations

from datetime import date, timedelta

import pytest

from src.strategy.v20.models import (
    BaseDecision,
    BreadthSnapshot,
    EntryAction,
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
    deserialize_health_snapshot,
    serialize_health_snapshot,
)
from src.strategy.v20.policy import (
    advance_health_state,
    combine_entry_decision,
    decide_base,
    equal_weight_batch_return,
    evaluate_g,
    evaluate_rolling7,
    gross_price_return,
    linear_quantile_25,
    multiplier_from_wilson_lower_bound,
    relative_health_return,
)


def _health(index: int, value: float, *, valid: bool = True) -> HealthObservation:
    signal = date(2026, 1, 1) + timedelta(days=index)
    return HealthObservation(
        batch_id=f"h{index}",
        signal_date=signal,
        t2_exit_date=signal + timedelta(days=2),
        relative_return=value if valid else None,
        valid=valid,
        invalid_reason=None if valid else "POOL_LT_1000",
    )


def _rolling(index: int, value: float) -> RollingBatch:
    signal = date(2026, 1, 1) + timedelta(days=index)
    return RollingBatch(f"r{index}", signal, signal + timedelta(days=2), value)


def _rolling_decision(status: Rolling7Status) -> Rolling7Decision:
    return Rolling7Decision(status, None, None, ())


def _base(multiplier: float) -> BaseDecision:
    return BaseDecision(multiplier, False, None, f"BASE_{multiplier}")


def _theme_inputs(*, transitive_cluster: int = 3, false_only: bool = False):
    mapping: dict[str, ThemeMapping] = {}
    stocks: list[StockThemeInput] = []
    for index in range(10):
        label = f"L{index}"
        canonical = f"T{index}"
        if false_only:
            canonical = "SAME_FALSE_THEME"
        elif index == 1:
            canonical = "T0"
        elif index == 2 and transitive_cluster >= 3:
            # Stock 1 also carries T2, joining 0--1--2 transitively.
            canonical = "T2"
        elif index == 3 and transitive_cluster >= 4:
            canonical = "T2"
        mapping[label] = ThemeMapping(label, canonical, label, "DIRECT", not false_only)
        extra: tuple[str, ...] = ()
        if index == 1 and transitive_cluster >= 3:
            bridge = "BRIDGE"
            mapping[bridge] = ThemeMapping(bridge, "T2", bridge, "DIRECT", True)
            extra = (bridge,)
        stocks.append(StockThemeInput(f"{index:06d}", (label,), extra))
    return stocks, mapping


def _threshold() -> Q25Threshold:
    return Q25Threshold("2026H2", 1_000.0, 100.0, 300.0, 73)


def test_zero_cost_price_and_equal_weight_formulas() -> None:
    assert gross_price_return(entry_price=100, exit_price=110) == pytest.approx(0.1)
    assert equal_weight_batch_return([0.1, -0.2, 0.4]) == pytest.approx(0.1)
    assert relative_health_return(
        top3_leg_returns=[0.1, 0.2, 0.3], comparison_pool_leg_returns=[0.1] * 1_000
    ) == pytest.approx(0.1)
    with pytest.raises(ValueError, match="at least 1000"):
        relative_health_return(
            top3_leg_returns=[0.1, 0.2, 0.3], comparison_pool_leg_returns=[0.1] * 999
        )


def test_health_invalid_label_does_not_occupy_window_and_zero_mean_is_healthy() -> None:
    result = advance_health_state(
        HealthSnapshot(), [_health(2, 0.0), _health(0, 0.1), _health(1, 0.0, valid=False)]
    )
    assert result.status is HealthStatus.WARMUP
    assert [item.batch_id for item in result.recent_valid] == ["h0", "h2"]

    result = advance_health_state(result, [_health(3, -0.1)])
    assert result.status is HealthStatus.HEALTHY
    assert result.recovery_count == 0
    assert sum(item.relative_return or 0 for item in result.recent_valid) == pytest.approx(0.0)


def test_health_pause_three_step_recovery_reset_and_idempotent_window() -> None:
    paused = advance_health_state(HealthSnapshot(), [_health(i, -0.1) for i in range(3)])
    assert (paused.status, paused.recovery_count) == (HealthStatus.PAUSED_R0, 0)

    r1 = advance_health_state(paused, [_health(3, 1.0)])
    r2 = advance_health_state(r1, [_health(4, 1.0)])
    healthy = advance_health_state(r2, [_health(5, 1.0)])
    assert (r1.status, r1.recovery_count) == (HealthStatus.PAUSED_R1, 1)
    assert (r2.status, r2.recovery_count) == (HealthStatus.PAUSED_R2, 2)
    assert (healthy.status, healthy.recovery_count) == (HealthStatus.HEALTHY, 3)
    assert advance_health_state(healthy, [_health(5, 999)]).status is HealthStatus.HEALTHY

    reset = advance_health_state(healthy, [_health(6, -10.0)])
    assert (reset.status, reset.recovery_count) == (HealthStatus.PAUSED_R0, 0)


def test_health_state_has_strict_versioned_round_trip_serialization() -> None:
    snapshot = advance_health_state(HealthSnapshot(), [_health(i, -0.1) for i in range(3)])
    payload = serialize_health_snapshot(snapshot)
    assert deserialize_health_snapshot(payload) == snapshot
    payload["unexpected"] = True
    with pytest.raises(ValueError, match="field set mismatch"):
        deserialize_health_snapshot(payload)


def test_base_breadth_fail_closed_and_inclusive_thresholds() -> None:
    healthy_items = tuple(_health(i, 0.1) for i in range(3))
    healthy = HealthSnapshot(HealthStatus.HEALTHY, 0, healthy_items, healthy_items[-1].order_key)
    assert decide_base(healthy).multiplier == 1.0

    paused_items = tuple(_health(i, -0.1) for i in range(3))
    paused = HealthSnapshot(HealthStatus.PAUSED_R0, 0, paused_items, paused_items[-1].order_key)
    assert decide_base(paused).multiplier == 0.0
    assert decide_base(paused, BreadthSnapshot(999, 1)).multiplier == 0.0
    assert multiplier_from_wilson_lower_bound(0.50) == 1.0
    assert multiplier_from_wilson_lower_bound(0.5000001) == 0.5
    assert multiplier_from_wilson_lower_bound(0.60) == 0.5
    assert multiplier_from_wilson_lower_bound(0.6000001) == 0.0


def test_rolling7_exact_bad_boundaries_and_maturity_strictness() -> None:
    decision_day = date(2026, 2, 1)
    bad = evaluate_rolling7(
        decision_date=decision_day,
        complete_batches=[_rolling(i, -0.01 if i < 5 else 0.01) for i in range(7)],
    )
    assert bad.status is Rolling7Status.BAD
    assert bad.r7 == pytest.approx(-0.03)
    assert bad.l7 == 5

    exactly_zero = evaluate_rolling7(
        decision_date=decision_day,
        complete_batches=[_rolling(i, -0.01 if i < 5 else 0.025) for i in range(7)],
    )
    assert exactly_zero.r7 == pytest.approx(0.0)
    assert exactly_zero.status is Rolling7Status.NON_BAD

    four_losses = evaluate_rolling7(
        decision_date=decision_day,
        complete_batches=[_rolling(i, -0.02 if i < 4 else 0.001) for i in range(7)],
    )
    assert four_losses.r7 < 0
    assert four_losses.l7 == 4
    assert four_losses.status is Rolling7Status.NON_BAD

    equal_maturity = RollingBatch("late", date(2026, 1, 29), decision_day, -1.0)
    result = evaluate_rolling7(
        decision_date=decision_day,
        complete_batches=[*[_rolling(i, 0.01) for i in range(6)], equal_maturity],
    )
    assert result.status is Rolling7Status.UNKNOWN


def test_rolling7_gap_activates_only_after_maturity_and_ages_after_seven_later_batches() -> None:
    gap = RollingGap("gap", date(2026, 1, 1), date(2026, 1, 3))
    before = evaluate_rolling7(decision_date=date(2026, 1, 3), complete_batches=[], gaps=[gap])
    assert before.unknown_reason == "INSUFFICIENT_MATURE_BATCHES"

    active = evaluate_rolling7(
        decision_date=date(2026, 1, 10),
        complete_batches=[_rolling(i, 0.01) for i in range(1, 7)],
        gaps=[gap],
    )
    assert active.active_gap_ids == ("gap",)

    aged = evaluate_rolling7(
        decision_date=date(2026, 2, 1),
        complete_batches=[_rolling(i, 0.01) for i in range(1, 8)],
        gaps=[gap],
    )
    assert aged.status is Rolling7Status.NON_BAD


def test_rolling7_clock_failure_is_unknown() -> None:
    result = evaluate_rolling7(
        decision_date=date(2026, 2, 1),
        complete_batches=[_rolling(i, 0.1) for i in range(7)],
        information_clock_valid=False,
    )
    assert result.status is Rolling7Status.UNKNOWN
    assert result.unknown_reason == "INFORMATION_CLOCK_INVALID"


def test_frozen_linear_q25_interpolation() -> None:
    assert linear_quantile_25([0.0, 10.0, 20.0, 30.0]) == pytest.approx(7.5)
    assert linear_quantile_25([0.0, 10.0, 20.0, 30.0, 40.0]) == pytest.approx(10.0)


def test_g_transitive_cluster_and_inclusive_weak_amount_trigger() -> None:
    stocks, mapping = _theme_inputs(transitive_cluster=3)
    result = evaluate_g(
        decision_date=date(2026, 8, 31),
        recommendations=stocks,
        mapping=mapping,
        prior_trade_amounts={stock.code: 100.0 for stock in stocks},
        threshold=_threshold(),
    )
    assert result.status is GStatus.TRIGGERED
    assert result.max_cluster_size == 3
    assert result.weak_metric_count == 3
    assert result.prior_amount_total == 1_000.0
    assert result.prior_amount_median == 100.0
    assert result.prior_amount_bottom3_sum == 300.0


def test_g_cluster_of_four_clears_and_false_labels_never_connect() -> None:
    stocks, mapping = _theme_inputs(transitive_cluster=4)
    cluster_four = evaluate_g(
        decision_date=date(2026, 8, 31),
        recommendations=stocks,
        mapping=mapping,
        prior_trade_amounts={stock.code: 100.0 for stock in stocks},
        threshold=_threshold(),
    )
    assert cluster_four.status is GStatus.CLEAR
    assert cluster_four.max_cluster_size == 4

    stocks, mapping = _theme_inputs(false_only=True)
    isolated = evaluate_g(
        decision_date=date(2026, 8, 31),
        recommendations=stocks,
        mapping=mapping,
        prior_trade_amounts={stock.code: 100.0 for stock in stocks},
        threshold=_threshold(),
    )
    assert isolated.status is GStatus.TRIGGERED
    assert isolated.max_cluster_size == 1


@pytest.mark.parametrize(
    ("mutation", "reason_prefix"),
    [
        ("short", "TOP10_INCOMPLETE"),
        ("empty", "EMPTY_LABELS"),
        ("unmapped", "UNMAPPED_LABEL"),
        ("zero_amount", "D1_AMOUNT_INCOMPLETE"),
        ("wrong_half", "Q25_THRESHOLD_HALF_MISMATCH"),
    ],
)
def test_g_incomplete_inputs_are_unknown(mutation: str, reason_prefix: str) -> None:
    stocks, mapping = _theme_inputs()
    amounts = {stock.code: 100.0 for stock in stocks}
    threshold = _threshold()
    if mutation == "short":
        stocks = stocks[:9]
    elif mutation == "empty":
        stocks[0] = StockThemeInput(stocks[0].code)
    elif mutation == "unmapped":
        stocks[0] = StockThemeInput(stocks[0].code, ("DOES_NOT_EXIST",))
    elif mutation == "zero_amount":
        amounts[stocks[4].code] = 0.0
    elif mutation == "wrong_half":
        threshold = Q25Threshold("2026H1", 1_000, 100, 300, 73)
    result = evaluate_g(
        decision_date=date(2026, 8, 31),
        recommendations=stocks,
        mapping=mapping,
        prior_trade_amounts=amounts,
        threshold=threshold,
    )
    assert result.status is GStatus.UNKNOWN
    assert result.reason.startswith(reason_prefix)
    if mutation == "zero_amount":
        assert result.amount_valid_n == 9
        assert result.max_cluster_size == 3


@pytest.mark.parametrize(
    ("base_multiplier", "rolling", "g_status", "expected"),
    [
        (1.0, Rolling7Status.NON_BAD, None, 1.0),
        (0.5, Rolling7Status.UNKNOWN, None, 0.5),
        (1.0, Rolling7Status.BAD, GStatus.CLEAR, 0.5),
        (0.5, Rolling7Status.BAD, GStatus.UNKNOWN, 0.25),
        (1.0, Rolling7Status.BAD, GStatus.TRIGGERED, 0.0),
        (0.0, Rolling7Status.NON_BAD, None, 0.0),
    ],
)
def test_entry_multiplier_matrix(
    base_multiplier: float,
    rolling: Rolling7Status,
    g_status: GStatus | None,
    expected: float,
) -> None:
    g = None
    if g_status is not None:
        g = GDecision(g_status, 1, 10, 1.0, 1.0, 1.0, 3, f"G_{g_status.value}")
    result = combine_entry_decision(
        scan_valid=True,
        recommendation_count=10,
        base=_base(base_multiplier),
        rolling7=_rolling_decision(rolling),
        g=g,
    )
    assert result.final_multiplier == expected
    assert result.action is (EntryAction.ENTER if expected else EntryAction.BLOCK)
    if expected:
        assert result.per_stock_relative_weight == pytest.approx(expected / 10)


def test_entry_no_signal_invalid_and_missing_bad_g_are_distinct() -> None:
    invalid = combine_entry_decision(
        scan_valid=False, recommendation_count=10, base=None, rolling7=None
    )
    no_signal = combine_entry_decision(
        scan_valid=True, recommendation_count=0, base=None, rolling7=None
    )
    missing_g = combine_entry_decision(
        scan_valid=True,
        recommendation_count=10,
        base=_base(1.0),
        rolling7=_rolling_decision(Rolling7Status.BAD),
    )
    assert invalid.action is EntryAction.INPUT_INVALID
    assert no_signal.action is EntryAction.NO_SIGNAL
    assert missing_g.action is EntryAction.INPUT_INVALID
