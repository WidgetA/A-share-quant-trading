"""Deterministic MEWS calculations; no I/O and no price inputs in core risk formulas."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date
from statistics import median
from typing import Any

from src.margin_risk.config import MarginRiskConfig
from src.margin_risk.models import DataStatus, RiskObservation, RiskState
from src.margin_risk.quality import detail_coverage_status
from src.margin_risk.state_machine import compute_risk_states


def _finite(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def ema_adjust_false(values: Sequence[float | None], span: int) -> list[float | None]:
    """Recursive EMA equivalent to pandas ``adjust=False, ignore_na=False``.

    Missing observations are not converted to zero and produce ``None`` on
    that date. Their trading-day positions still decay the old observation's
    weight, matching the default EWM semantics when the next value arrives.
    """

    if span <= 0:
        raise ValueError("EMA span must be positive")
    alpha = 2.0 / (span + 1.0)
    state: float | None = None
    old_weight = 1.0
    output: list[float | None] = []
    for raw in values:
        value = _finite(raw)
        if state is None:
            if value is not None:
                state = value
                old_weight = 1.0
            output.append(state)
            continue
        old_weight *= 1.0 - alpha
        if value is None:
            output.append(None)
            continue
        if state != value:
            state = (old_weight * state + alpha * value) / (old_weight + alpha)
        old_weight = 1.0
        output.append(state)
    return output


def rolling_midrank_percentile(
    values: Sequence[float | None],
    window: int,
    min_periods: int,
) -> list[float | None]:
    """Trailing-only mid-rank percentile including the current observation."""

    if window <= 0 or min_periods <= 0:
        raise ValueError("rolling window and min_periods must be positive")
    output: list[float | None] = []
    for index, raw in enumerate(values):
        current = _finite(raw)
        if current is None:
            output.append(None)
            continue
        start = max(0, index - window + 1)
        sample = [v for v in (_finite(x) for x in values[start : index + 1]) if v is not None]
        if len(sample) < min_periods:
            output.append(None)
            continue
        lower = sum(value < current for value in sample)
        equal = sum(value == current for value in sample)
        output.append((lower + 0.5 * equal) / len(sample) * 100.0)
    return output


def prior_rolling_median(
    values: Sequence[float | None],
    window: int,
    min_periods: int | None = None,
) -> list[float | None]:
    """Rolling median of t-window ... t-1; the current value is excluded."""

    required = window if min_periods is None else min_periods
    output: list[float | None] = []
    for index in range(len(values)):
        start = max(0, index - window)
        sample = [v for v in (_finite(x) for x in values[start:index]) if v is not None]
        output.append(float(median(sample)) if len(sample) >= required else None)
    return output


def calculate_mews(mpi: float | None, mls: float | None, nib: float | None) -> float | None:
    values = (_finite(mpi), _finite(mls), _finite(nib))
    if any(value is None for value in values):
        return None
    product = max(0.0, (100.0 - values[0]) * values[1] * values[2])  # type: ignore[operator]
    return product ** (1.0 / 3.0)


def calculate_confirmation(dlb: float | None, rpp: float | None) -> float | None:
    left, right = _finite(dlb), _finite(rpp)
    if left is None or right is None:
        return None
    return math.sqrt(max(0.0, left * right))


def calculate_security_features(
    trading_dates: Sequence[date],
    rows: Sequence[Mapping[str, Any]],
    config: MarginRiskConfig,
) -> list[dict[str, Any]]:
    """Calculate one security's sparse daily financing features.

    ``rows`` must contain actual margin_detail observations only. Missing
    trading-day rows remain absent/None and are never interpreted as zero.
    """

    by_date = {row["trade_date"]: row for row in rows}
    net_flows: list[float | None] = []
    flow_rates: list[float | None] = []
    valid_rows: list[bool] = []

    for index, day in enumerate(trading_dates):
        current = by_date.get(day)
        valid = bool(
            current
            and _finite(current.get("financing_balance")) is not None
            and _finite(current.get("financing_buy_amount")) is not None
            and _finite(current.get("financing_repayment_amount")) is not None
        )
        valid_rows.append(valid)
        if not valid or current is None:
            net_flows.append(None)
            flow_rates.append(None)
            continue

        buy = float(current["financing_buy_amount"])
        repayment = float(current["financing_repayment_amount"])
        net_flow = buy - repayment
        net_flows.append(net_flow)

        previous = by_date.get(trading_dates[index - 1]) if index > 0 else None
        previous_balance = _finite(previous.get("financing_balance")) if previous else None
        flow_rates.append(
            net_flow / previous_balance
            if previous_balance is not None and previous_balance > 0
            else None
        )

    ema_fast = ema_adjust_false(flow_rates, config.ema_fast)
    ema_slow = ema_adjust_false(flow_rates, config.ema_slow)
    output: list[dict[str, Any]] = []

    for index, day in enumerate(trading_dates):
        current = by_date.get(day)
        if current is None:
            continue
        start = max(0, index - config.security_valid_window + 1)
        valid_count = sum(valid_rows[start : index + 1])
        previous = by_date.get(trading_dates[index - 1]) if index > 0 else None
        previous_balance = _finite(previous.get("financing_balance")) if previous else None
        eligible = (
            previous_balance is not None
            and previous_balance > 0
            and valid_count >= config.security_min_valid
            and ema_fast[index] is not None
            and ema_slow[index] is not None
            and flow_rates[index] is not None
        )
        impulse = (
            ema_fast[index] - ema_slow[index]  # type: ignore[operator]
            if eligible
            else None
        )

        net_flow_5d: float | None = None
        if index + 1 >= config.deleveraging_window:
            window_values = net_flows[index - config.deleveraging_window + 1 : index + 1]
            observed = [value for value in window_values if value is not None]
            if len(observed) == len(window_values):
                net_flow_5d = sum(observed, 0.0)

        output.append(
            {
                "trade_date": day,
                "ts_code": current["ts_code"],
                "financing_balance_prev": previous_balance,
                "net_flow": net_flows[index],
                "flow_rate": flow_rates[index],
                "flow_rate_ema_5": ema_fast[index],
                "flow_rate_ema_20": ema_slow[index],
                "impulse_raw": impulse,
                "net_flow_5d": net_flow_5d,
                "is_negative_impulse": impulse is not None and impulse < 0,
                "is_deleveraging": eligible and net_flow_5d is not None and net_flow_5d < 0,
                "balance_weight": None,
                "valid_observation_count_25d": valid_count,
                "eligible": eligible,
            }
        )
    return output


def aggregate_security_features(
    features: Sequence[Mapping[str, Any]],
    total_stock_margin_balance: float,
) -> dict[str, float | int | None]:
    """Balance-weighted NIB/DLB and their explicit coverage numerators."""

    impulse_valid = [
        row
        for row in features
        if row.get("eligible") and (_finite(row.get("financing_balance_prev")) or 0.0) > 0
    ]
    valid_margin_balance = sum(float(row["financing_balance_prev"]) for row in impulse_valid)
    negative_balance = sum(
        float(row["financing_balance_prev"])
        for row in impulse_valid
        if bool(row.get("is_negative_impulse"))
    )
    dlb_valid = [row for row in impulse_valid if _finite(row.get("net_flow_5d")) is not None]
    dlb_denominator = sum(float(row["financing_balance_prev"]) for row in dlb_valid)
    deleveraging_balance = sum(
        float(row["financing_balance_prev"])
        for row in dlb_valid
        if bool(row.get("is_deleveraging"))
    )

    return {
        "nib": 100.0 * negative_balance / valid_margin_balance
        if valid_margin_balance > 0
        else None,
        "dlb": 100.0 * deleveraging_balance / dlb_denominator if dlb_denominator > 0 else None,
        "valid_margin_balance": valid_margin_balance,
        "negative_impulse_balance": negative_balance,
        "deleveraging_balance": deleveraging_balance,
        "total_stock_margin_balance": total_stock_margin_balance,
        "breadth_coverage": (
            valid_margin_balance / total_stock_margin_balance
            if total_stock_margin_balance > 0
            else None
        ),
        "valid_stock_count": len(impulse_valid),
    }


def concentration_metrics(balances: Sequence[float]) -> dict[str, float | None]:
    valid = sorted((float(v) for v in balances if _finite(v) is not None and v > 0), reverse=True)
    total = sum(valid)
    if total <= 0:
        return {"top20_margin_share": None, "top50_margin_share": None, "margin_hhi": None}
    return {
        "top20_margin_share": sum(valid[:20]) / total,
        "top50_margin_share": sum(valid[:50]) / total,
        "margin_hhi": sum((value / total) ** 2 for value in valid),
    }


def _signal_reason(metric: Mapping[str, Any]) -> str:
    if metric["data_status"] == DataStatus.PARTIAL.value:
        return (
            "逐股票融资明细或自由流通市值覆盖不完整，本日仅发布可靠市场级指标，扩散与综合风险暂停。"
        )
    if metric.get("mews_percentile") is None:
        return "有效历史不足，尚未形成可发布的融资耗竭历史分位。"
    parts: list[str] = []
    mpi = _finite(metric.get("mpi"))
    mls = _finite(metric.get("mls"))
    nib = _finite(metric.get("negative_impulse_breadth"))
    dlb = _finite(metric.get("deleveraging_breadth"))
    rpp = _finite(metric.get("repayment_pressure_percentile"))
    if mpi is not None and mpi <= 15:
        parts.append("近期融资扩张速度处于历史弱位")
    if mls is not None and mls >= 85:
        parts.append("融资存量相对自由流通市值处于历史高位")
    if nib is not None and nib >= 60:
        parts.append(f"{nib:.1f}%的有效融资余额已进入负脉冲")
    if dlb is not None and rpp is not None and (dlb >= 60 or rpp >= 90):
        parts.append("实际净偿还正在广泛扩散")
    if not parts:
        parts.append("融资脉冲、融资负荷与扩散尚未同时形成高风险组合")
    return "；".join(parts) + "。"


def calculate_market_metrics(
    market_rows: Sequence[Mapping[str, Any]],
    ffmv_by_date: Mapping[date, Mapping[str, Any]],
    breadth_by_date: Mapping[date, Mapping[str, Any]],
    concentration_by_date: Mapping[date, Mapping[str, Any]],
    config: MarginRiskConfig,
    trading_dates: Sequence[date] | None = None,
    initial_risk_state: RiskState = RiskState.NORMAL,
) -> list[dict[str, Any]]:
    """Calculate the complete market series in chronological order."""

    source_rows = sorted(market_rows, key=lambda item: item["trade_date"])
    if trading_dates is None:
        rows: list[Mapping[str, Any]] = source_rows
    else:
        by_date = {row["trade_date"]: row for row in source_rows}
        rows = [
            by_date.get(day, {"trade_date": day, "_market_missing": True}) for day in trading_dates
        ]
    balances = [_finite(row.get("financing_balance")) for row in rows]
    flow_rates: list[float | None] = []
    repayment_rates: list[float | None] = []
    for index, row in enumerate(rows):
        previous = balances[index - 1] if index > 0 else None
        buy = _finite(row.get("financing_buy_amount"))
        repayment = _finite(row.get("financing_repayment_amount"))
        if previous is None or previous <= 0 or buy is None or repayment is None:
            flow_rates.append(None)
            repayment_rates.append(None)
        else:
            flow_rates.append((buy - repayment) / previous)
            repayment_rates.append((repayment - buy) / previous)

    fast = ema_adjust_false(flow_rates, config.ema_fast)
    slow = ema_adjust_false(flow_rates, config.ema_slow)
    pulse_raw = [
        left - right if left is not None and right is not None else None
        for left, right in zip(fast, slow, strict=True)
    ]
    mpi = rolling_midrank_percentile(pulse_raw, config.rank_window, config.rank_min_periods)

    ffmv_values = [_finite(ffmv_by_date.get(row["trade_date"], {}).get("ffmv")) for row in rows]
    ffmv_base = prior_rolling_median(ffmv_values, config.load_base_window)
    leverage_raw = [
        balance / base if balance is not None and base is not None and base > 0 else None
        for balance, base in zip(balances, ffmv_base, strict=True)
    ]
    mls = rolling_midrank_percentile(leverage_raw, config.rank_window, config.rank_min_periods)
    repayment_raw = ema_adjust_false(repayment_rates, config.ema_fast)
    rpp = rolling_midrank_percentile(
        repayment_raw,
        config.rank_window,
        config.rank_min_periods,
    )

    detail_coverages: list[float | None] = []
    statuses: list[DataStatus] = []
    mews_raw: list[float | None] = []
    confirmation_raw: list[float | None] = []
    for index, row in enumerate(rows):
        day = row["trade_date"]
        breadth = breadth_by_date.get(day, {})
        detail_coverage = _finite(breadth.get("detail_coverage"))
        ffmv_coverage = _finite(ffmv_by_date.get(day, {}).get("coverage"))
        if row.get("_market_missing"):
            status = DataStatus.FAILED
        elif breadth.get("force_partial") or ffmv_coverage is None or ffmv_coverage <= 0:
            status = DataStatus.PARTIAL
        else:
            status = detail_coverage_status(
                detail_coverage,
                detail_coverages[max(0, index - config.detail_coverage_window) : index],
                config,
            )
        detail_coverages.append(detail_coverage)
        statuses.append(status)
        if status == DataStatus.OK:
            mews_raw.append(calculate_mews(mpi[index], mls[index], breadth.get("nib")))
            confirmation_raw.append(calculate_confirmation(breadth.get("dlb"), rpp[index]))
        else:
            mews_raw.append(None)
            confirmation_raw.append(None)

    mews_percentile = rolling_midrank_percentile(
        mews_raw,
        config.rank_window,
        config.rank_min_periods,
    )
    confirmation_percentile = rolling_midrank_percentile(
        confirmation_raw,
        config.rank_window,
        config.rank_min_periods,
    )
    observations = [
        RiskObservation(
            trade_date=row["trade_date"],
            mews_percentile=mews_percentile[index],
            confirmation_percentile=confirmation_percentile[index],
            dlb=_finite(breadth_by_date.get(row["trade_date"], {}).get("dlb")),
            rpp=rpp[index],
            data_status=statuses[index],
        )
        for index, row in enumerate(rows)
    ]
    states = compute_risk_states(observations, config, initial_state=initial_risk_state)

    output: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if row.get("_market_missing"):
            # Exchange-incomplete dates participate as missing trading-day
            # slots in all windows/state continuity, but are never published.
            continue
        day = row["trade_date"]
        breadth = breadth_by_date.get(day, {})
        concentration = concentration_by_date.get(day, {})
        ffmv = ffmv_by_date.get(day, {})
        publish_breadth = statuses[index] == DataStatus.OK
        buy_amount = _finite(row.get("financing_buy_amount"))
        repayment_amount = _finite(row.get("financing_repayment_amount"))
        metric: dict[str, Any] = {
            "trade_date": day,
            "market_financing_balance": balances[index],
            "market_financing_buy_amount": buy_amount,
            "market_financing_repayment_amount": repayment_amount,
            "market_net_flow": (
                buy_amount - repayment_amount
                if buy_amount is not None and repayment_amount is not None
                else None
            ),
            "market_flow_rate": flow_rates[index],
            "pulse_raw": pulse_raw[index],
            "mpi": mpi[index],
            "ffmv_base": ffmv_base[index],
            "leverage_load_raw": leverage_raw[index],
            "mls": mls[index],
            "negative_impulse_breadth": breadth.get("nib") if publish_breadth else None,
            "deleveraging_breadth": breadth.get("dlb") if publish_breadth else None,
            "repayment_pressure_raw": repayment_raw[index],
            "repayment_pressure_percentile": rpp[index],
            "mews_raw": mews_raw[index] if publish_breadth else None,
            "mews_percentile": mews_percentile[index] if publish_breadth else None,
            "confirmation_raw": confirmation_raw[index] if publish_breadth else None,
            "confirmation_percentile": (
                confirmation_percentile[index] if publish_breadth else None
            ),
            "top20_margin_share": concentration.get("top20_margin_share"),
            "top50_margin_share": concentration.get("top50_margin_share"),
            "margin_hhi": concentration.get("margin_hhi"),
            "detail_coverage": breadth.get("detail_coverage"),
            "stock_coverage": breadth.get("stock_coverage"),
            "breadth_coverage": breadth.get("breadth_coverage") if publish_breadth else None,
            "valid_margin_balance": (
                breadth.get("valid_margin_balance") if publish_breadth else None
            ),
            "total_stock_margin_balance": breadth.get("total_stock_margin_balance"),
            "negative_impulse_balance": (
                breadth.get("negative_impulse_balance") if publish_breadth else None
            ),
            "deleveraging_balance": (
                breadth.get("deleveraging_balance") if publish_breadth else None
            ),
            "ffmv_coverage": ffmv.get("coverage"),
            "market_identity_error": row.get("market_identity_error"),
            "security_identity_anomaly_count": breadth.get("security_identity_anomaly_count", 0),
            "sse_complete": bool(row.get("sse_complete", True)),
            "szse_complete": bool(row.get("szse_complete", True)),
            "risk_state": states[index].value,
            "data_status": statuses[index].value,
            "index_version": config.index_version,
        }
        metric["signal_reason"] = _signal_reason(metric)
        output.append(metric)
    return output
