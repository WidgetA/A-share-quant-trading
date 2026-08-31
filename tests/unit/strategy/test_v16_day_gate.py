"""Unit tests for the pure V16 day-level gate."""

from datetime import datetime, timezone

import pytest

from src.strategy.v16_day_gate import (
    GateMode,
    GateReason,
    GateState,
    V16DayGate,
    V16DayGateInput,
    V16DayGatePolicy,
)


def _gate_input(
    *,
    ranked: tuple[str, ...] = ("a", "b", "c", "d", "e"),
    boards=None,
    drivers=None,
    canonical=None,
    upstream_data_complete: bool = True,
    data_quality_issues: tuple[str, ...] = (),
) -> V16DayGateInput:
    if boards is None:
        boards = {
            "a": ("optics",),
            "b": ("optics", "pcb"),
            "c": ("pcb",),
            "d": ("robot",),
            "e": ("metals",),
        }
    if drivers is None:
        drivers = {"a": True, "b": False, "c": True, "d": True, "e": False}
    return V16DayGateInput(
        cutoff_ts=datetime(2026, 8, 25, 9, 38, tzinfo=timezone.utc),
        ranked_top_k=ranked,
        stock_all_boards=boards,
        stock_is_driver=drivers,
        model_version="lgbrank-test-sha",
        canonical_theme_map=canonical,
        taxonomy_version="taxonomy-test-v1",
        upstream_data_complete=upstream_data_complete,
        data_quality_issues=data_quality_issues,
    )


def test_missing_policy_returns_watch_but_still_computes_metrics():
    decision = V16DayGate().evaluate(_gate_input())

    assert decision.state is GateState.WATCH
    assert decision.mode is GateMode.SHADOW
    assert decision.reasons == (GateReason.POLICY_UNCALIBRATED,)
    assert decision.policy_version is None
    assert decision.blocks_trade is False

    metrics = decision.metrics
    assert metrics.ranked_count == 5
    assert metrics.component_count == 3
    assert metrics.largest_cluster_codes == ("a", "b", "c")
    assert metrics.largest_cluster_size == 3
    assert metrics.largest_cluster_share == pytest.approx(3 / 5)
    assert metrics.effective_cluster_count == pytest.approx(1 / (0.6**2 + 0.2**2 + 0.2**2))
    assert metrics.top3_main_cluster_coverage == 1.0
    assert metrics.driver_count == 3
    assert metrics.driver_breadth == pytest.approx(3 / 5)


def test_canonical_theme_mapping_connects_different_raw_boards():
    evidence = _gate_input(
        ranked=("a", "b", "c"),
        boards={"a": ("cpo",), "b": ("pcb",), "c": ("robot",)},
        drivers={"a": True, "b": True, "c": False},
        canonical={"cpo": "ai-chain", "pcb": "ai-chain"},
    )

    metrics = V16DayGate().evaluate(evidence).metrics

    assert metrics.component_count == 2
    assert metrics.largest_cluster_codes == ("a", "b")
    assert metrics.largest_cluster_themes == ("ai-chain",)
    assert metrics.top3_main_cluster_coverage == pytest.approx(2 / 3)


def test_partial_canonical_mapping_falls_back_to_raw_board_name():
    evidence = _gate_input(
        ranked=("a", "b"),
        boards={"a": ("shared",), "b": ("shared",)},
        drivers={"a": True, "b": True},
        canonical={"unrelated": "other-theme"},
    )

    metrics = V16DayGate().evaluate(evidence).metrics

    assert metrics.largest_cluster_codes == ("a", "b")
    assert metrics.largest_cluster_themes == ("shared",)


def test_equal_sized_clusters_choose_the_one_with_the_highest_ranked_stock():
    evidence = _gate_input(
        ranked=("a", "b", "c", "d"),
        boards={"a": ("x",), "b": ("y",), "c": ("y",), "d": ("x",)},
        drivers={"a": True, "b": True, "c": True, "d": True},
    )

    metrics = V16DayGate().evaluate(evidence).metrics

    assert metrics.largest_cluster_codes == ("a", "d")
    assert metrics.top3_main_cluster_coverage == pytest.approx(1 / 3)


def test_explicit_live_policy_can_return_trade():
    policy = V16DayGatePolicy(
        version="test-policy-pass",
        mode=GateMode.LIVE,
        min_largest_cluster_share=0.5,
        max_effective_cluster_count=2.5,
        min_top3_main_cluster_coverage=0.75,
        min_driver_breadth=0.5,
    )

    decision = V16DayGate(policy).evaluate(_gate_input())

    assert decision.state is GateState.TRADE
    assert decision.reasons == (GateReason.PASS,)
    assert decision.policy_version == "test-policy-pass"
    assert decision.blocks_trade is False
    assert dict(decision.applied_thresholds) == {
        "min_largest_cluster_share": 0.5,
        "max_effective_cluster_count": 2.5,
        "min_top3_main_cluster_coverage": 0.75,
        "min_driver_breadth": 0.5,
    }


def test_explicit_live_policy_blocks_failed_metrics_with_all_reasons():
    policy = V16DayGatePolicy(
        version="test-policy-fail",
        mode=GateMode.LIVE,
        min_largest_cluster_share=0.8,
        max_effective_cluster_count=1.5,
        min_driver_breadth=0.8,
    )

    decision = V16DayGate(policy).evaluate(_gate_input())

    assert decision.state is GateState.NO_TRADE
    assert decision.reasons == (
        GateReason.LARGEST_CLUSTER_TOO_SMALL,
        GateReason.EFFECTIVE_CLUSTER_COUNT_TOO_HIGH,
        GateReason.DRIVER_BREADTH_TOO_LOW,
    )
    assert decision.blocks_trade is True


def test_policy_defaults_to_shadow_and_never_enforces_its_assessment():
    policy = V16DayGatePolicy(
        version="test-policy-shadow",
        min_largest_cluster_share=0.9,
    )

    decision = V16DayGate(policy).evaluate(_gate_input())

    assert decision.state is GateState.NO_TRADE
    assert decision.mode is GateMode.SHADOW
    assert decision.reasons == (
        GateReason.LARGEST_CLUSTER_TOO_SMALL,
        GateReason.SHADOW_MODE,
    )
    assert decision.blocks_trade is False


def test_policy_without_any_threshold_remains_uncalibrated():
    decision = V16DayGate(V16DayGatePolicy(version="empty-policy")).evaluate(_gate_input())

    assert decision.state is GateState.WATCH
    assert decision.reasons == (GateReason.POLICY_UNCALIBRATED,)
    assert decision.applied_thresholds == ()


def test_missing_board_and_driver_data_returns_no_trade():
    policy = V16DayGatePolicy(
        version="live-data-check",
        mode=GateMode.LIVE,
        min_largest_cluster_share=0.5,
    )
    evidence = _gate_input(
        ranked=("a", "b"),
        boards={"a": ("x",)},
        drivers={"a": True},
    )

    decision = V16DayGate(policy).evaluate(evidence)

    assert decision.state is GateState.NO_TRADE
    assert decision.reasons == (
        GateReason.DATA_INCOMPLETE,
        GateReason.MISSING_BOARD_MEMBERSHIP,
        GateReason.MISSING_DRIVER_FLAG,
    )
    assert decision.blocks_trade is True
    assert decision.data_quality_issues == (
        "missing_board_membership=b",
        "missing_driver_flag=b",
    )


def test_upstream_quality_failure_returns_no_trade_before_policy():
    evidence = _gate_input(
        upstream_data_complete=False,
        data_quality_issues=("minute_bar_coverage_below_policy",),
    )

    decision = V16DayGate().evaluate(evidence)

    assert decision.state is GateState.NO_TRADE
    assert decision.reasons == (GateReason.DATA_INCOMPLETE,)
    assert decision.data_quality_issues == ("minute_bar_coverage_below_policy",)


def test_duplicate_ranked_codes_are_reported_and_do_not_distort_metrics():
    evidence = _gate_input(
        ranked=("a", "b", "a"),
        boards={"a": ("x",), "b": ("x",)},
        drivers={"a": True, "b": False},
    )

    decision = V16DayGate().evaluate(evidence)

    assert decision.state is GateState.NO_TRADE
    assert decision.reasons == (
        GateReason.DATA_INCOMPLETE,
        GateReason.DUPLICATE_RANKED_CODE,
    )
    assert decision.metrics.ranked_count == 2
    assert decision.metrics.largest_cluster_share == 1.0


def test_empty_ranked_basket_is_an_explicit_no_trade():
    evidence = _gate_input(ranked=(), boards={}, drivers={})

    decision = V16DayGate().evaluate(evidence)

    assert decision.state is GateState.NO_TRADE
    assert decision.reasons == (GateReason.NO_RANKED_CANDIDATES,)
    assert decision.metrics.ranked_count == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("min_largest_cluster_share", -0.01),
        ("min_top3_main_cluster_coverage", 1.01),
        ("min_driver_breadth", 2.0),
        ("max_effective_cluster_count", 0.0),
        ("max_effective_cluster_count", float("nan")),
        ("max_effective_cluster_count", float("inf")),
        ("min_largest_cluster_share", float("nan")),
        ("min_driver_breadth", True),
    ],
)
def test_policy_rejects_invalid_thresholds(field, value):
    with pytest.raises(ValueError):
        V16DayGatePolicy(version="invalid", **{field: value})
