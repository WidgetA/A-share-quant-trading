from __future__ import annotations

from datetime import date, timedelta

import pytest

from src.margin_risk.config import MarginRiskConfig
from src.margin_risk.models import DataStatus
from src.margin_risk.v2_calculations import (
    calculate_mews_v2_score,
    calculate_nib_v2,
    calculate_persistent_deleveraging_path,
    calculate_v2_market_metrics,
    robust_impulse_features,
)
from src.margin_risk.v2_state_machine import V2Thresholds


def test_nib_v2_dead_zone_filters_tiny_negative_impulse() -> None:
    history = [1.0, -1.0] * 30
    result = robust_impulse_features([*history, -0.10, -2.0])
    tiny = result[-2]
    severe = result[-1]

    assert tiny["impulse_z"] is not None
    assert tiny["is_negative_impulse_v2"] is False
    assert tiny["negative_impulse_magnitude"] == 0.0
    assert severe["is_negative_impulse_v2"] is True
    assert severe["negative_impulse_magnitude"] > tiny["negative_impulse_magnitude"]


def test_nib_v2_rejects_invalid_or_too_small_scale() -> None:
    result = robust_impulse_features([0.0] * 60)
    assert result[-1]["impulse_scale"] is None
    assert result[-1]["impulse_z"] is None
    assert result[-1]["is_negative_impulse_v2"] is None


def test_nib_v2_combines_breadth_and_magnitude() -> None:
    assert calculate_nib_v2(64.0, 25.0) == pytest.approx(40.0)
    assert calculate_nib_v2(None, 25.0) is None


def _v2_fixture(days: int = 900) -> tuple[list[date], dict[str, list[object]]]:
    dates = [date(2018, 1, 1) + timedelta(days=index) for index in range(days)]
    balance = [1_000.0] * days
    buy = [10.0] * days
    repay = [10.0] * days
    # The final 150 observations are a stable negative financing flow. The
    # fast/slow pulse converges, while its level remains abnormal versus the
    # preceding trailing history.
    for index in range(days - 150, days):
        buy[index] = 5.0
        repay[index] = 10.0
    values: dict[str, list[object]] = {
        "market_total_balance": balance.copy(),
        "stock_balance": balance,
        "stock_buy": buy,
        "stock_repay": repay,
        "ffmv_stock": [10_000.0] * days,
        "nib_sign_v1": [80.0] * days,
        "nib_breadth_v2": [80.0] * days,
        "nib_magnitude_v2": [60.0] * days,
        "dlb": [85.0] * days,
        "data_status": [DataStatus.OK] * days,
    }
    return dates, values


def test_persistent_negative_flow_survives_pulse_convergence() -> None:
    dates, values = _v2_fixture()
    config = MarginRiskConfig(
        development_end=date(2019, 12, 31),
        validation_start=date(2020, 1, 1),
    )
    metrics, _thresholds = calculate_v2_market_metrics(dates, values, config)
    latest = metrics[-1]

    assert abs(float(latest["pulse_raw_stock"])) < 1e-8
    assert float(latest["net_outflow_level_score"]) > 70.0
    assert float(latest["persistent_deleveraging_path"]) > 60.0


def test_buy_decline_and_repayment_increase_are_separate() -> None:
    dates, values = _v2_fixture()
    metrics, _thresholds = calculate_v2_market_metrics(dates, values, MarginRiskConfig())
    buy_decline = metrics[-1]
    assert float(buy_decline["buy_shortfall_score"]) > 70.0

    dates2, values2 = _v2_fixture()
    values2["stock_buy"] = [10.0] * len(dates2)
    values2["stock_repay"] = [10.0] * (len(dates2) - 150) + [15.0] * 150
    metrics2, _thresholds2 = calculate_v2_market_metrics(
        dates2,
        values2,
        MarginRiskConfig(),
    )
    repayment_rise = metrics2[-1]
    assert float(repayment_rise["repay_level_score"]) > 70.0
    assert float(repayment_rise["buy_shortfall_score"]) < 70.0


def test_production_call_can_use_frozen_thresholds_without_refitting() -> None:
    dates, values = _v2_fixture(days=300)
    fixed = V2Thresholds(watch=57.0, warning=68.0, clear=49.0, persistent_danger=58.0)
    config = MarginRiskConfig(
        # No observation belongs to a development sample, so this call would
        # be unable to fit thresholds. Production must still use the frozen set.
        development_end=date(2000, 1, 1),
    )

    metrics, thresholds = calculate_v2_market_metrics(
        dates,
        values,
        config,
        fixed_thresholds=fixed,
    )

    assert metrics
    assert thresholds == fixed


def test_v2_primary_score_is_path_max_not_rolling_percentile() -> None:
    assert calculate_mews_v2_score(42.0, 73.0) == 73.0
    assert calculate_persistent_deleveraging_path(80.0, 80.0, 80.0) == pytest.approx(80.0)


def test_fixed_baseline_percentile_is_not_backfilled_before_2022() -> None:
    dates, values = _v2_fixture(days=1_600)
    config = MarginRiskConfig(
        development_end=date(2021, 12, 31),
        validation_start=date(2022, 1, 1),
    )
    metrics, _thresholds = calculate_v2_market_metrics(dates, values, config)
    for metric in metrics:
        if metric["trade_date"] < config.validation_start:
            assert metric["mews_v2_fixed_baseline_percentile"] is None


def test_signal_available_date_moves_to_next_trading_observation() -> None:
    dates, values = _v2_fixture()
    metrics, _thresholds = calculate_v2_market_metrics(dates, values, MarginRiskConfig())
    assert metrics[500]["signal_available_date"] == dates[501]
    assert metrics[-1]["signal_available_date"] is None


def test_partial_data_does_not_publish_v2_paths_or_score() -> None:
    dates, values = _v2_fixture()
    values["data_status"][-1] = DataStatus.PARTIAL
    metrics, _thresholds = calculate_v2_market_metrics(dates, values, MarginRiskConfig())
    latest = metrics[-1]
    assert latest["exhaustion_path"] is None
    assert latest["persistent_deleveraging_path"] is None
    assert latest["mews_v2_score"] is None


def test_v2_thresholds_only_use_development_sample() -> None:
    dates, values = _v2_fixture(days=1_600)
    config = MarginRiskConfig(
        development_end=dates[899],
        validation_start=dates[900],
    )
    _metrics, baseline = calculate_v2_market_metrics(dates, values, config)
    altered = {name: list(series) for name, series in values.items()}
    for index in range(900, len(dates)):
        altered["stock_buy"][index] = 0.0
        altered["stock_repay"][index] = 100.0
        altered["dlb"][index] = 100.0
    _changed_metrics, changed = calculate_v2_market_metrics(dates, altered, config)
    assert changed == baseline
