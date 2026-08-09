"""Deterministic MEWS v2 calculations with no price inputs in the risk score."""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Mapping, Sequence
from datetime import date
from statistics import median
from typing import Any

from src.margin_risk.calculations import (
    ema_adjust_false,
    prior_rolling_median,
    rolling_midrank_percentile,
)
from src.margin_risk.config import MarginRiskConfig
from src.margin_risk.models import DataStatus, RiskState
from src.margin_risk.v2_state_machine import (
    V2RiskObservation,
    V2Thresholds,
    compute_v2_risk_states,
)


def _finite(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def linear_quantile(values: Sequence[float], probability: float) -> float:
    valid = sorted(value for raw in values if (value := _finite(raw)) is not None)
    if not valid:
        raise ValueError("quantile requires at least one finite value")
    if len(valid) == 1:
        return valid[0]
    position = min(1.0, max(0.0, probability)) * (len(valid) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return valid[lower]
    fraction = position - lower
    return valid[lower] * (1.0 - fraction) + valid[upper] * fraction


def robust_impulse_features(
    impulses: Sequence[float | None],
    *,
    window: int = 60,
    min_periods: int = 40,
    threshold: float = -0.25,
    magnitude_normalizer: float = 2.75,
    scale_epsilon: float = 1e-15,
) -> list[dict[str, float | bool | None]]:
    """Scale impulses by trailing valid-observation MAD and apply a dead zone.

    The trailing window contains the current observation and at most ``window``
    valid observations. Missing values remain missing and are never inserted as
    zeros.
    """

    if window <= 0 or min_periods <= 0 or min_periods > window:
        raise ValueError("invalid robust impulse window")
    if magnitude_normalizer <= 0:
        raise ValueError("magnitude_normalizer must be positive")

    history: deque[float] = deque(maxlen=window)
    output: list[dict[str, float | bool | None]] = []
    for raw in impulses:
        impulse = _finite(raw)
        if impulse is None:
            output.append(
                {
                    "impulse_scale": None,
                    "impulse_z": None,
                    "is_negative_impulse_v2": None,
                    "negative_impulse_magnitude": None,
                }
            )
            continue
        history.append(impulse)
        if len(history) < min_periods:
            output.append(
                {
                    "impulse_scale": None,
                    "impulse_z": None,
                    "is_negative_impulse_v2": None,
                    "negative_impulse_magnitude": None,
                }
            )
            continue
        center = float(median(history))
        mad = float(median(abs(value - center) for value in history))
        scale = 1.4826 * mad
        if not math.isfinite(scale) or scale <= scale_epsilon:
            output.append(
                {
                    "impulse_scale": None,
                    "impulse_z": None,
                    "is_negative_impulse_v2": None,
                    "negative_impulse_magnitude": None,
                }
            )
            continue
        impulse_z = impulse / scale
        magnitude = min(
            1.0,
            max(0.0, (-impulse_z + threshold) / magnitude_normalizer),
        )
        output.append(
            {
                "impulse_scale": scale,
                "impulse_z": impulse_z,
                "is_negative_impulse_v2": impulse_z < threshold,
                "negative_impulse_magnitude": magnitude,
            }
        )
    return output


def calculate_nib_v2(breadth: float | None, magnitude: float | None) -> float | None:
    left, right = _finite(breadth), _finite(magnitude)
    if left is None or right is None:
        return None
    return math.sqrt(max(0.0, left * right))


def calculate_exhaustion_path(
    mpi_stock: float | None,
    mls_stock: float | None,
    nib_v2: float | None,
) -> float | None:
    mpi, mls, nib = _finite(mpi_stock), _finite(mls_stock), _finite(nib_v2)
    if mpi is None or mls is None or nib is None:
        return None
    return max(0.0, (100.0 - mpi) * mls * nib) ** (1.0 / 3.0)


def calculate_persistent_deleveraging_path(
    mls_stock: float | None,
    dlb: float | None,
    net_outflow_level_score: float | None,
) -> float | None:
    mls, breadth, level = (
        _finite(mls_stock),
        _finite(dlb),
        _finite(net_outflow_level_score),
    )
    if mls is None or breadth is None or level is None:
        return None
    return max(0.0, mls * breadth * level) ** (1.0 / 3.0)


def calculate_mews_v2_score(
    exhaustion_path: float | None,
    persistent_path: float | None,
) -> float | None:
    exhaustion, persistent = _finite(exhaustion_path), _finite(persistent_path)
    if exhaustion is None or persistent is None:
        return None
    return max(exhaustion, persistent)


def fixed_baseline_midrank(
    current: float | None,
    baseline: Sequence[float],
) -> float | None:
    value = _finite(current)
    valid = [item for raw in baseline if (item := _finite(raw)) is not None]
    if value is None or not valid:
        return None
    lower = sum(item < value for item in valid)
    equal = sum(item == value for item in valid)
    return (lower + 0.5 * equal) / len(valid) * 100.0


def _series(
    values: Mapping[str, Sequence[Any]],
    name: str,
    length: int,
) -> Sequence[Any]:
    result = values.get(name)
    if result is None or len(result) != length:
        raise ValueError(f"v2 input series {name!r} has the wrong length")
    return result


def _v2_reason(metric: Mapping[str, Any]) -> str:
    if metric.get("data_status") != DataStatus.OK.value:
        return "普通A股融资明细或覆盖口径异常，本日v2综合风险不作市场解释。"
    exhaustion = _finite(metric.get("exhaustion_path"))
    persistent = _finite(metric.get("persistent_deleveraging_path"))
    if exhaustion is None or persistent is None:
        return "有效历史不足，尚未形成MEWS v2双路径分数。"
    if persistent > exhaustion:
        return "持续去杠杆路径占主导：融资负债收缩扩散与持续净偿还水平较高。"
    return "融资购买力耗竭路径占主导：融资脉冲、负荷和鲁棒负脉冲扩散共同抬升。"


def calculate_v2_market_metrics(
    trading_dates: Sequence[date],
    values: Mapping[str, Sequence[Any]],
    config: MarginRiskConfig,
    *,
    fixed_thresholds: V2Thresholds | None = None,
    initial_risk_state: RiskState = RiskState.NORMAL,
) -> tuple[list[dict[str, Any]], V2Thresholds]:
    """Calculate v2 market metrics from one internally consistent stock sample."""

    count = len(trading_dates)
    if count == 0:
        raise ValueError("trading_dates must not be empty")
    market_total = [_finite(value) for value in _series(values, "market_total_balance", count)]
    stock_balance = [_finite(value) for value in _series(values, "stock_balance", count)]
    stock_buy = [_finite(value) for value in _series(values, "stock_buy", count)]
    stock_repay = [_finite(value) for value in _series(values, "stock_repay", count)]
    ffmv_stock = [_finite(value) for value in _series(values, "ffmv_stock", count)]
    nib_sign = [_finite(value) for value in _series(values, "nib_sign_v1", count)]
    nib_breadth = [_finite(value) for value in _series(values, "nib_breadth_v2", count)]
    nib_magnitude = [_finite(value) for value in _series(values, "nib_magnitude_v2", count)]
    dlb = [_finite(value) for value in _series(values, "dlb", count)]
    statuses = [
        value if isinstance(value, DataStatus) else DataStatus(str(value))
        for value in _series(values, "data_status", count)
    ]

    stock_flow_rate: list[float | None] = []
    buy_intensity: list[float | None] = []
    repay_intensity: list[float | None] = []
    for index in range(count):
        previous = stock_balance[index - 1] if index > 0 else None
        buy = stock_buy[index]
        repay = stock_repay[index]
        if previous is None or previous <= 0 or buy is None or repay is None:
            stock_flow_rate.append(None)
            buy_intensity.append(None)
            repay_intensity.append(None)
            continue
        stock_flow_rate.append((buy - repay) / previous)
        buy_intensity.append(buy / previous)
        repay_intensity.append(repay / previous)

    flow_fast = ema_adjust_false(stock_flow_rate, config.ema_fast)
    flow_slow = ema_adjust_false(stock_flow_rate, config.ema_slow)
    pulse_raw_stock = [
        fast - slow if fast is not None and slow is not None else None
        for fast, slow in zip(flow_fast, flow_slow, strict=True)
    ]
    mpi_stock = rolling_midrank_percentile(
        pulse_raw_stock,
        config.rank_window,
        config.rank_min_periods,
    )
    net_flow_level_raw = flow_fast
    net_outflow_level_raw = [
        -value if value is not None else None for value in net_flow_level_raw
    ]
    net_outflow_level_score = rolling_midrank_percentile(
        net_outflow_level_raw,
        config.rank_window,
        config.rank_min_periods,
    )

    buy_ema5 = ema_adjust_false(buy_intensity, config.ema_fast)
    buy_ema20 = ema_adjust_false(buy_intensity, config.ema_slow)
    repay_ema5 = ema_adjust_false(repay_intensity, config.ema_fast)
    repay_ema20 = ema_adjust_false(repay_intensity, config.ema_slow)
    buy_impulse = [
        fast - slow if fast is not None and slow is not None else None
        for fast, slow in zip(buy_ema5, buy_ema20, strict=True)
    ]
    repay_shock = [
        fast - slow if fast is not None and slow is not None else None
        for fast, slow in zip(repay_ema5, repay_ema20, strict=True)
    ]
    buy_level_percentile = rolling_midrank_percentile(
        buy_ema5,
        config.rank_window,
        config.rank_min_periods,
    )
    buy_shortfall_score = [
        100.0 - value if value is not None else None for value in buy_level_percentile
    ]
    repay_level_score = rolling_midrank_percentile(
        repay_ema5,
        config.rank_window,
        config.rank_min_periods,
    )

    ffmv_base = prior_rolling_median(ffmv_stock, config.load_base_window)
    leverage_load_raw = [
        balance / base if balance is not None and base is not None and base > 0 else None
        for balance, base in zip(stock_balance, ffmv_base, strict=True)
    ]
    mls_stock = rolling_midrank_percentile(
        leverage_load_raw,
        config.rank_window,
        config.rank_min_periods,
    )
    nib_v2 = [
        calculate_nib_v2(left, right)
        for left, right in zip(nib_breadth, nib_magnitude, strict=True)
    ]

    exhaustion_path: list[float | None] = []
    persistent_path: list[float | None] = []
    scores: list[float | None] = []
    for index in range(count):
        if statuses[index] != DataStatus.OK:
            exhaustion_path.append(None)
            persistent_path.append(None)
            scores.append(None)
            continue
        exhaustion = calculate_exhaustion_path(
            mpi_stock[index],
            mls_stock[index],
            nib_v2[index],
        )
        persistent = calculate_persistent_deleveraging_path(
            mls_stock[index],
            dlb[index],
            net_outflow_level_score[index],
        )
        exhaustion_path.append(exhaustion)
        persistent_path.append(persistent)
        scores.append(calculate_mews_v2_score(exhaustion, persistent))

    rolling_percentile = rolling_midrank_percentile(
        scores,
        config.rank_window,
        config.rank_min_periods,
    )
    development_scores = [
        score
        for day, score, status in zip(trading_dates, scores, statuses, strict=True)
        if day <= config.development_end and score is not None and status == DataStatus.OK
    ]
    development_persistent = [
        score
        for day, score, status in zip(trading_dates, persistent_path, statuses, strict=True)
        if day <= config.development_end and score is not None and status == DataStatus.OK
    ]
    thresholds = fixed_thresholds or V2Thresholds(
        watch=linear_quantile(development_scores, 0.85),
        warning=linear_quantile(development_scores, 0.95),
        clear=linear_quantile(development_scores, 0.75),
        persistent_danger=linear_quantile(development_persistent, 0.85),
    )
    fixed_percentile = [
        fixed_baseline_midrank(score, development_scores)
        if day >= config.validation_start
        else None
        for day, score in zip(trading_dates, scores, strict=True)
    ]
    observations = [
        V2RiskObservation(
            trade_date=day,
            score=scores[index],
            persistent_path=persistent_path[index],
            dlb=dlb[index],
            net_outflow_level_score=net_outflow_level_score[index],
            data_status=statuses[index],
        )
        for index, day in enumerate(trading_dates)
    ]
    states = compute_v2_risk_states(
        observations,
        thresholds,
        clear_days=config.clear_days,
        initial_state=initial_risk_state,
    )

    optional_names = {
        "market_total_buy",
        "market_total_repay",
        "residual_balance",
        "ordinary_coverage",
        "coverage_deviation_60d",
        "detail_coverage",
        "breadth_coverage",
        "ffmv_coverage",
        "nib_threshold_0",
        "nib_threshold_minus_05",
        "mews_v1_score",
        "mews_v1_percentile",
        "confirmation_v1",
        "rpp_v1",
        "mpi_v1",
        "mls_v1",
        "pulse_raw_v1",
        "risk_state_v1",
    }
    optional = {
        name: values.get(name, [None] * count)
        for name in optional_names
    }
    for name, series_values in optional.items():
        if len(series_values) != count:
            raise ValueError(f"v2 optional input series {name!r} has the wrong length")

    output: list[dict[str, Any]] = []
    for index, day in enumerate(trading_dates):
        metric: dict[str, Any] = {
            "trade_date": day,
            "signal_available_date": (
                trading_dates[index + 1] if index + 1 < count else None
            ),
            "market_total_margin_balance": market_total[index],
            "market_total_financing_buy_amount": optional["market_total_buy"][index],
            "market_total_financing_repayment_amount": optional["market_total_repay"][index],
            "ordinary_a_share_margin_balance": stock_balance[index],
            "ordinary_a_share_financing_buy_amount": stock_buy[index],
            "ordinary_a_share_financing_repayment_amount": stock_repay[index],
            "non_stock_or_residual_margin_balance": optional["residual_balance"][index],
            "ordinary_a_share_margin_coverage": optional["ordinary_coverage"][index],
            "coverage_deviation_60d": optional["coverage_deviation_60d"][index],
            "stock_flow_rate": stock_flow_rate[index],
            "pulse_raw_stock": pulse_raw_stock[index],
            "mpi_stock_v2": mpi_stock[index],
            "ffmv_stock": ffmv_stock[index],
            "ffmv_stock_base": ffmv_base[index],
            "leverage_load_stock_raw": leverage_load_raw[index],
            "mls_stock_v2": mls_stock[index],
            "buy_intensity": buy_intensity[index],
            "repay_intensity": repay_intensity[index],
            "buy_intensity_ema_5": buy_ema5[index],
            "repay_intensity_ema_5": repay_ema5[index],
            "buy_impulse": buy_impulse[index],
            "repay_shock": repay_shock[index],
            "buy_shortfall_score": buy_shortfall_score[index],
            "repay_level_score": repay_level_score[index],
            "net_flow_level_raw": net_flow_level_raw[index],
            "net_outflow_level_score": net_outflow_level_score[index],
            "nib_sign_v1": nib_sign[index],
            "nib_breadth_v2": nib_breadth[index],
            "nib_magnitude_v2": nib_magnitude[index],
            "nib_v2": nib_v2[index],
            "nib_threshold_0": optional["nib_threshold_0"][index],
            "nib_threshold_minus_05": optional["nib_threshold_minus_05"][index],
            "deleveraging_breadth": dlb[index],
            "exhaustion_path": exhaustion_path[index],
            "persistent_deleveraging_path": persistent_path[index],
            "mews_v1_score": optional["mews_v1_score"][index],
            "mews_v1_percentile": optional["mews_v1_percentile"][index],
            "mews_v2_score": scores[index],
            "mews_v2_rolling_percentile": rolling_percentile[index],
            "mews_v2_fixed_baseline_percentile": fixed_percentile[index],
            "confirmation_v1": optional["confirmation_v1"][index],
            "repayment_pressure_percentile_v1": optional["rpp_v1"][index],
            "mpi_v1": optional["mpi_v1"][index],
            "mls_v1": optional["mls_v1"][index],
            "pulse_raw_v1": optional["pulse_raw_v1"][index],
            "risk_state_v1": optional["risk_state_v1"][index],
            "risk_state_v2": states[index].value,
            "detail_coverage": optional["detail_coverage"][index],
            "breadth_coverage": optional["breadth_coverage"][index],
            "ffmv_coverage": optional["ffmv_coverage"][index],
            "data_status": statuses[index].value,
            "index_version": config.v2_index_version,
        }
        metric["signal_reason_v2"] = _v2_reason(metric)
        output.append(metric)
    return output, thresholds
