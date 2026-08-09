from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta

import pytest

from src.margin_risk.calculations import (
    aggregate_security_features,
    calculate_confirmation,
    calculate_market_metrics,
    calculate_mews,
    calculate_security_features,
    ema_adjust_false,
    prior_rolling_median,
    rolling_midrank_percentile,
)
from src.margin_risk.config import MarginRiskConfig
from src.margin_risk.quality import balance_identity_error, balance_identity_is_valid


def _days(count: int) -> list[date]:
    start = date(2024, 1, 1)
    return [start + timedelta(days=index) for index in range(count)]


def _security_rows(days: list[date], net_flows: list[float]) -> list[dict]:
    return [
        {
            "trade_date": day,
            "ts_code": "600000.SH",
            "financing_balance": 1000.0 + sum(net_flows[: index + 1]),
            "financing_buy_amount": max(flow, 0.0) + 10.0,
            "financing_repayment_amount": max(-flow, 0.0) + 10.0,
        }
        for index, (day, flow) in enumerate(zip(days, net_flows, strict=True))
    ]


def test_market_balance_identity():
    cfg = MarginRiskConfig()
    assert balance_identity_error(1100, 1000, 200, 100) == 0
    assert balance_identity_is_valid(1100, 1000, 200, 100, cfg)
    assert not balance_identity_is_valid(5000, 1000, 200, 100, cfg)


def test_ema_adjust_false_and_missing_not_zero():
    values = ema_adjust_false([1.0, 2.0, None, 3.0], span=3)
    assert values == [1.0, 1.5, None, 2.5]


def test_rolling_percentile_has_no_future_information():
    prefix = [1.0, 2.0, 3.0, 4.0]
    first = rolling_midrank_percentile(prefix + [1000.0], 3, 2)
    second = rolling_midrank_percentile(prefix + [-1000.0], 3, 2)
    assert first[:4] == second[:4]
    assert first[2] == pytest.approx((2 + 0.5) / 3 * 100)


def test_mls_denominator_excludes_current_day_ffmv():
    baseline = prior_rolling_median([100.0, 110.0, 120.0, 1.0], window=3)
    changed = prior_rolling_median([100.0, 110.0, 120.0, 1_000_000.0], window=3)
    assert baseline[-1] == changed[-1] == 110.0


def test_nib_uses_previous_balance_weight():
    result = aggregate_security_features(
        [
            {
                "eligible": True,
                "financing_balance_prev": 90.0,
                "is_negative_impulse": True,
                "net_flow_5d": 1.0,
                "is_deleveraging": False,
            },
            {
                "eligible": True,
                "financing_balance_prev": 10.0,
                "is_negative_impulse": False,
                "net_flow_5d": -1.0,
                "is_deleveraging": True,
            },
        ],
        100.0,
    )
    assert result["nib"] == 90.0
    assert result["dlb"] == 10.0


def test_dlb_uses_five_trading_day_net_flow():
    cfg = replace(MarginRiskConfig(), security_min_valid=1, security_valid_window=5)
    days = _days(7)
    rows = _security_rows(days, [1, 1, -5, -5, -5, -5, 1])
    features = calculate_security_features(days, rows, cfg)
    assert features[-1]["net_flow_5d"] == -19
    assert features[-1]["is_deleveraging"] is True


def test_missing_margin_detail_is_not_filled_with_zero():
    cfg = replace(MarginRiskConfig(), security_min_valid=1, security_valid_window=5)
    days = _days(5)
    rows = _security_rows([days[0], days[1], days[3], days[4]], [1, 1, 1, 1])
    features = calculate_security_features(days, rows, cfg)
    by_day = {row["trade_date"]: row for row in features}
    assert days[2] not in by_day
    assert by_day[days[3]]["flow_rate"] is None
    assert by_day[days[4]]["net_flow_5d"] is None


def test_new_stock_needs_twenty_valid_observations_for_nib():
    days = _days(20)
    rows = _security_rows(days, [-1.0] * 20)
    features = calculate_security_features(days, rows, MarginRiskConfig())
    assert all(not row["eligible"] for row in features[:19])
    assert features[19]["eligible"] is True


def test_mews_geometric_formula():
    expected = ((100 - 10) * 90 * 80) ** (1 / 3)
    assert calculate_mews(10, 90, 80) == pytest.approx(expected)


def test_confirmation_formula():
    assert calculate_confirmation(64, 81) == pytest.approx(72.0)


def test_partial_coverage_suppresses_breadth_and_mews():
    cfg = replace(
        MarginRiskConfig(),
        rank_window=20,
        rank_min_periods=5,
        load_base_window=3,
        detail_coverage_window=20,
    )
    days = _days(26)
    market = [
        {
            "trade_date": day,
            "financing_balance": 1000 + index,
            "financing_buy_amount": 20 - index / 10,
            "financing_repayment_amount": 10 + index / 10,
        }
        for index, day in enumerate(days)
    ]
    ffmv = {day: {"ffmv": 100_000.0, "coverage": 1.0} for day in days}
    breadth = {
        day: {
            "detail_coverage": 1.0 if index < 25 else 0.90,
            "stock_coverage": 0.99,
            "nib": 80.0,
            "dlb": 60.0,
        }
        for index, day in enumerate(days)
    }
    result = calculate_market_metrics(market, ffmv, breadth, {}, cfg)[-1]
    assert result["data_status"] == "PARTIAL"
    assert result["negative_impulse_breadth"] is None
    assert result["deleveraging_breadth"] is None
    assert result["mews_percentile"] is None
    assert result["confirmation_percentile"] is None


def test_failed_non_market_ingestion_forces_partial_publication():
    cfg = replace(
        MarginRiskConfig(),
        rank_window=10,
        rank_min_periods=3,
        load_base_window=2,
    )
    days = _days(8)
    market = [
        {
            "trade_date": day,
            "financing_balance": 1000.0,
            "financing_buy_amount": 110.0,
            "financing_repayment_amount": 100.0,
        }
        for day in days
    ]
    ffmv = {day: {"ffmv": 100_000.0, "coverage": 1.0} for day in days}
    breadth = {
        day: {
            "detail_coverage": 1.0,
            "stock_coverage": 1.0,
            "nib": 80.0,
            "dlb": 60.0,
            "force_partial": day == days[-1],
        }
        for day in days
    }
    result = calculate_market_metrics(market, ffmv, breadth, {}, cfg)[-1]
    assert result["data_status"] == "PARTIAL"
    assert result["mpi"] is not None
    assert result["negative_impulse_breadth"] is None
    assert result["mews_percentile"] is None


def test_mpi_direction_falls_when_financing_flow_decelerates():
    cfg = replace(
        MarginRiskConfig(),
        rank_window=30,
        rank_min_periods=5,
        load_base_window=3,
    )
    days = _days(40)
    flows = list(range(1, 21)) + list(range(20, 0, -1))
    market = [
        {
            "trade_date": day,
            "financing_balance": 1000.0,
            "financing_buy_amount": 100.0 + flow,
            "financing_repayment_amount": 100.0,
        }
        for day, flow in zip(days, flows, strict=True)
    ]
    ffmv = {day: {"ffmv": 100_000.0, "coverage": 1.0} for day in days}
    breadth = {
        day: {"detail_coverage": 1.0, "stock_coverage": 1.0, "nib": 50.0, "dlb": 50.0}
        for day in days
    }
    result = calculate_market_metrics(market, ffmv, breadth, {}, cfg)
    assert result[-1]["mpi"] < result[20]["mpi"]


def test_missing_market_day_is_not_published_or_skipped_in_trading_windows():
    cfg = replace(
        MarginRiskConfig(),
        rank_window=10,
        rank_min_periods=3,
        load_base_window=2,
    )
    days = _days(8)
    missing = days[4]
    market = [
        {
            "trade_date": day,
            "financing_balance": 1000.0,
            "financing_buy_amount": 110.0,
            "financing_repayment_amount": 100.0,
        }
        for day in days
        if day != missing
    ]
    ffmv = {day: {"ffmv": 100_000.0, "coverage": 1.0} for day in days}
    breadth = {
        day: {"detail_coverage": 1.0, "stock_coverage": 1.0, "nib": 50.0, "dlb": 50.0}
        for day in days
    }
    result = calculate_market_metrics(market, ffmv, breadth, {}, cfg, trading_dates=days)
    by_day = {row["trade_date"]: row for row in result}
    assert missing not in by_day
    # The day after a failed exchange day has no exact M(t-1), so FlowRate
    # remains missing instead of spanning a two-day gap.
    assert by_day[days[5]]["market_flow_rate"] is None


def test_synthetic_scenarios_a_b_c():
    scenario_a = calculate_mews(5, 95, 85)
    scenario_b = calculate_mews(5, 10, 85)
    scenario_c = calculate_mews(95, 95, 85)
    assert scenario_a is not None and scenario_a > 90
    assert scenario_b is not None and scenario_b < 50
    assert scenario_c is not None and scenario_c < 40
