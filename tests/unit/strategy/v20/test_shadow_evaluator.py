import pytest

from src.data.clients.tushare_realtime import TushareDailyBar
from src.strategy.v20.shadow_evaluator import evaluate_shadow_batch


def _daily(code: str, close: float) -> TushareDailyBar:
    return TushareDailyBar(code, "20260902", close, 1_000_000.0)


def test_rolling_shadow_uses_every_leg_and_zero_cost_price_return() -> None:
    result = evaluate_shadow_batch(
        kind="ROLLING7",
        payload={"symbols": [{"code": "000001"}, {"code": "000002"}]},
        reference_status="LOCKED",
        reference_prices={"000001": 10.0, "000002": 20.0},
        daily_bars={"000001": _daily("000001", 11.0), "000002": _daily("000002", 18.0)},
    )

    assert result.status == "COMPLETE_VALID"
    assert result.batch_return == pytest.approx(0.0)


def test_rolling_shadow_never_drops_a_missing_leg() -> None:
    result = evaluate_shadow_batch(
        kind="ROLLING7",
        payload={"symbols": [{"code": "000001"}, {"code": "000002"}]},
        reference_status="LOCKED",
        reference_prices={"000001": 10.0},
        daily_bars={"000001": _daily("000001", 11.0)},
    )

    assert result.status == "INCOMPLETE"
    assert result.batch_return is None
    assert result.payload_update["incomplete_reason"] == "ROLLING_LEG_PRICE_MISSING"


def test_health_requires_exact_top3_and_at_least_1000_comparison_prices() -> None:
    codes = [f"60{index:04d}" for index in range(1000)]
    references = {code: 10.0 for code in codes}
    daily = {code: _daily(code, 10.1) for code in codes}
    references.update({"000001": 10.0, "000002": 10.0, "000003": 10.0})
    daily.update(
        {
            "000001": _daily("000001", 10.2),
            "000002": _daily("000002", 10.2),
            "000003": _daily("000003", 10.2),
        }
    )

    result = evaluate_shadow_batch(
        kind="HEALTH",
        payload={
            "top3": [{"code": "000001"}, {"code": "000002"}, {"code": "000003"}],
            "comparison_pool_codes": codes,
        },
        reference_status="LOCKED",
        reference_prices=references,
        daily_bars=daily,
    )

    assert result.status == "COMPLETE_VALID"
    assert round(result.batch_return or 0.0, 8) == 0.01


def test_health_reference_unavailable_is_terminal_invalid() -> None:
    result = evaluate_shadow_batch(
        kind="HEALTH",
        payload={},
        reference_status="UNAVAILABLE",
        reference_prices=None,
        daily_bars={},
    )

    assert result.status == "COMPLETE_INVALID"
    assert result.payload_update["invalid_reason"] == "REFERENCE_UNAVAILABLE"
