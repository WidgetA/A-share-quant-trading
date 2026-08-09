"""Pure MEWS v2 state machine using development-sample thresholds."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date

from src.margin_risk.models import DataStatus, RiskState


@dataclass(frozen=True, slots=True)
class V2Thresholds:
    watch: float
    warning: float
    clear: float
    persistent_danger: float


@dataclass(frozen=True, slots=True)
class V2RiskObservation:
    trade_date: date
    score: float | None
    persistent_path: float | None
    dlb: float | None
    net_outflow_level_score: float | None
    data_status: DataStatus = DataStatus.OK


_LEVEL = {
    RiskState.NORMAL: 0,
    RiskState.WATCH: 1,
    RiskState.WARNING: 2,
    RiskState.DANGER: 3,
}
_ONE_DOWN = {
    RiskState.DANGER: RiskState.WARNING,
    RiskState.WARNING: RiskState.WATCH,
    RiskState.WATCH: RiskState.WATCH,
    RiskState.NORMAL: RiskState.NORMAL,
}


def _two_of_three(values: Sequence[float | None], threshold: float) -> bool:
    return sum(value is not None and value >= threshold for value in values) >= 2


def _candidate(
    observations: Sequence[V2RiskObservation],
    index: int,
    thresholds: V2Thresholds,
) -> RiskState:
    current = observations[index]
    if current.data_status != DataStatus.OK or current.score is None:
        return RiskState.NORMAL

    emergency = (
        current.dlb is not None
        and current.net_outflow_level_score is not None
        and current.dlb >= 75.0
        and current.net_outflow_level_score >= 99.0
    )
    if emergency:
        return RiskState.DANGER

    if index >= 1:
        previous = observations[index - 1]
        confirmed_today = (
            current.score >= thresholds.warning
            and current.persistent_path is not None
            and current.persistent_path >= thresholds.persistent_danger
        )
        confirmed_previous = (
            previous.data_status == DataStatus.OK
            and previous.score is not None
            and previous.score >= thresholds.warning
            and previous.persistent_path is not None
            and previous.persistent_path >= thresholds.persistent_danger
        )
        if confirmed_today and confirmed_previous:
            return RiskState.DANGER

    recent = [item.score for item in observations[max(0, index - 2) : index + 1]]
    if _two_of_three(recent, thresholds.warning):
        return RiskState.WARNING
    if _two_of_three(recent, thresholds.watch):
        return RiskState.WATCH
    return RiskState.NORMAL


def compute_v2_risk_states(
    observations: Sequence[V2RiskObservation],
    thresholds: V2Thresholds,
    *,
    clear_days: int = 5,
    initial_state: RiskState = RiskState.NORMAL,
) -> list[RiskState]:
    """Evaluate v2 states in trading-day order with the v1 hysteresis policy."""

    state = initial_state
    clear_streak = 0
    output: list[RiskState] = []
    for index, observation in enumerate(observations):
        if observation.data_status != DataStatus.OK or observation.score is None:
            clear_streak = 0
            output.append(state)
            continue

        if observation.score < thresholds.clear:
            clear_streak += 1
        else:
            clear_streak = 0

        candidate = _candidate(observations, index, thresholds)
        if clear_streak >= clear_days:
            state = RiskState.NORMAL
        elif _LEVEL[candidate] >= _LEVEL[state]:
            state = candidate
        elif candidate == RiskState.NORMAL:
            state = _ONE_DOWN[state]
        else:
            one_down = _ONE_DOWN[state]
            state = candidate if _LEVEL[candidate] >= _LEVEL[one_down] else one_down
        output.append(state)
    return output
