"""Deterministic maturity evaluation for V20's two V16 shadow streams."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Any, Mapping, Sequence

from src.data.clients.tushare_realtime import TushareDailyBar

from .policy import equal_weight_batch_return, gross_price_return, relative_health_return


@dataclass(frozen=True, slots=True)
class ShadowEvaluationResult:
    status: str
    batch_return: float | None
    payload_update: Mapping[str, Any]


def _invalid(reason: str, **diagnostics: Any) -> ShadowEvaluationResult:
    return ShadowEvaluationResult(
        status="COMPLETE_INVALID",
        batch_return=None,
        payload_update={
            "evaluation_profile_id": "ZERO_COST_GROSS_PRICE_RETURN_V1",
            "evaluation_status": "INVALID",
            "invalid_reason": reason,
            **diagnostics,
        },
    )


def _incomplete(reason: str, **diagnostics: Any) -> ShadowEvaluationResult:
    return ShadowEvaluationResult(
        status="INCOMPLETE",
        batch_return=None,
        payload_update={
            "evaluation_profile_id": "ZERO_COST_GROSS_PRICE_RETURN_V1",
            "evaluation_status": "INCOMPLETE",
            "incomplete_reason": reason,
            **diagnostics,
        },
    )


def _codes(rows: object, *, field: str) -> tuple[str, ...] | None:
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        return None
    result: list[str] = []
    for item in rows:
        if not isinstance(item, Mapping):
            return None
        code = item.get("code")
        if not isinstance(code, str) or len(code) != 6 or not code.isdigit():
            return None
        result.append(code)
    if len(result) != len(set(result)):
        return None
    return tuple(result)


def _normalized_references(raw: Mapping[str, float] | None) -> dict[str, float]:
    result: dict[str, float] = {}
    for code, value in (raw or {}).items():
        try:
            price = float(value)
        except (TypeError, ValueError):
            continue
        if len(code) == 6 and code.isdigit() and isfinite(price) and price > 0:
            result[code] = price
    return result


def _leg_return(
    code: str,
    *,
    references: Mapping[str, float],
    daily_bars: Mapping[str, TushareDailyBar],
) -> float | None:
    reference = references.get(code)
    daily = daily_bars.get(code)
    if reference is None or daily is None:
        return None
    close = float(daily.close_price)
    if not isfinite(close) or close <= 0:
        return None
    return gross_price_return(entry_price=reference, exit_price=close)


def evaluate_shadow_batch(
    *,
    kind: str,
    payload: Mapping[str, Any],
    reference_status: str,
    reference_prices: Mapping[str, float] | None,
    daily_bars: Mapping[str, TushareDailyBar],
) -> ShadowEvaluationResult:
    """Evaluate one mature batch without dropping an unavailable model leg.

    The caller owns the causal maturity cutoff.  This function only applies
    the frozen gross-price formulas to the already locked D0 reference
    snapshot and the batch's T+2 daily close snapshot.
    """

    if kind not in {"HEALTH", "ROLLING7"}:
        raise ValueError(f"unsupported shadow batch kind: {kind!r}")
    if reference_status == "UNAVAILABLE":
        return _invalid("REFERENCE_UNAVAILABLE", reference_status=reference_status)
    if reference_status != "LOCKED":
        return _incomplete("REFERENCE_NOT_LOCKED", reference_status=reference_status)
    references = _normalized_references(reference_prices)
    if not references:
        return _invalid("REFERENCE_PRICES_EMPTY")

    if kind == "ROLLING7":
        codes = _codes(payload.get("symbols"), field="symbols")
        if not codes:
            return _invalid("ROLLING_SYMBOLS_INVALID")
        returns: list[float] = []
        missing: list[str] = []
        for code in codes:
            value = _leg_return(code, references=references, daily_bars=daily_bars)
            if value is None:
                missing.append(code)
            else:
                returns.append(value)
        if missing:
            return _incomplete(
                "ROLLING_LEG_PRICE_MISSING",
                expected_leg_n=len(codes),
                valid_leg_n=len(returns),
                missing_codes=missing,
            )
        batch_return = equal_weight_batch_return(returns)
        return ShadowEvaluationResult(
            status="COMPLETE_VALID",
            batch_return=batch_return,
            payload_update={
                "evaluation_profile_id": "ZERO_COST_GROSS_PRICE_RETURN_V1",
                "evaluation_status": "VALID",
                "valid_leg_n": len(returns),
                "gross_price_return": batch_return,
            },
        )

    top3 = _codes(payload.get("top3"), field="top3")
    comparison_raw = payload.get("comparison_pool_codes")
    if top3 is None or len(top3) != 3:
        return _invalid("HEALTH_TOP3_INCOMPLETE", top3_n=0 if top3 is None else len(top3))
    if not isinstance(comparison_raw, Sequence) or isinstance(comparison_raw, (str, bytes)):
        return _invalid("HEALTH_COMPARISON_POOL_INVALID")
    comparison_codes = tuple(str(code) for code in comparison_raw)
    if len(comparison_codes) != len(set(comparison_codes)) or any(
        len(code) != 6 or not code.isdigit() for code in comparison_codes
    ):
        return _invalid("HEALTH_COMPARISON_POOL_INVALID")

    top3_returns: list[float] = []
    top3_missing: list[str] = []
    for code in top3:
        value = _leg_return(code, references=references, daily_bars=daily_bars)
        if value is None:
            top3_missing.append(code)
        else:
            top3_returns.append(value)
    if top3_missing:
        return _invalid("HEALTH_TOP3_PRICE_MISSING", missing_codes=top3_missing)

    comparison_returns = [
        value
        for code in comparison_codes
        if (value := _leg_return(code, references=references, daily_bars=daily_bars)) is not None
    ]
    if len(comparison_returns) < 1_000:
        return _invalid(
            "HEALTH_COMPARISON_VALID_LT_1000",
            comparison_expected_n=len(comparison_codes),
            comparison_valid_n=len(comparison_returns),
        )
    result = relative_health_return(
        top3_leg_returns=top3_returns,
        comparison_pool_leg_returns=comparison_returns,
    )
    return ShadowEvaluationResult(
        status="COMPLETE_VALID",
        batch_return=result,
        payload_update={
            "evaluation_profile_id": "ZERO_COST_GROSS_PRICE_RETURN_V1",
            "evaluation_status": "VALID",
            "top3_valid_n": 3,
            "comparison_valid_n": len(comparison_returns),
            "top3_mean_gross_price_return": equal_weight_batch_return(top3_returns),
            "comparison_mean_gross_price_return": equal_weight_batch_return(comparison_returns),
            "relative_health_return": result,
        },
    )


__all__ = ["ShadowEvaluationResult", "evaluate_shadow_batch"]
