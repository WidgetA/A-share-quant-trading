"""Pure, deterministic MEWS risk-state machine with hysteresis."""

from __future__ import annotations

from collections.abc import Sequence

from src.margin_risk.config import MarginRiskConfig
from src.margin_risk.models import DataStatus, RiskObservation, RiskState

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


def _at_least_two_of_three(values: Sequence[float | None], threshold: float) -> bool:
    return sum(value is not None and value >= threshold for value in values[-3:]) >= 2


def _candidate_state(
    observations: Sequence[RiskObservation],
    index: int,
    config: MarginRiskConfig,
) -> RiskState:
    current = observations[index]
    emergency = (
        current.dlb is not None
        and current.rpp is not None
        and current.dlb >= config.emergency_dlb
        and current.rpp >= config.emergency_rpp
    )
    if emergency:
        return RiskState.DANGER

    if index >= 1:
        previous = observations[index - 1]
        confirmed_today = (
            current.mews_percentile is not None
            and current.confirmation_percentile is not None
            and current.mews_percentile >= config.warning_percentile
            and current.confirmation_percentile >= config.confirm_percentile
        )
        confirmed_previous = (
            previous.data_status == DataStatus.OK
            and previous.mews_percentile is not None
            and previous.confirmation_percentile is not None
            and previous.mews_percentile >= config.warning_percentile
            and previous.confirmation_percentile >= config.confirm_percentile
        )
        if confirmed_today and confirmed_previous:
            return RiskState.DANGER

    recent_mews = [item.mews_percentile for item in observations[max(0, index - 2) : index + 1]]
    if _at_least_two_of_three(recent_mews, config.warning_percentile):
        return RiskState.WARNING
    if _at_least_two_of_three(recent_mews, config.watch_percentile):
        return RiskState.WATCH
    return RiskState.NORMAL


def compute_risk_states(
    observations: Sequence[RiskObservation],
    config: MarginRiskConfig,
    initial_state: RiskState = RiskState.NORMAL,
) -> list[RiskState]:
    """Evaluate observations in trading-day order.

    Missing/PARTIAL observations freeze state and reset the clearance streak;
    they never masquerade as low market risk. Outside a five-day clean clear,
    a state may fall by only one level per trading day and WATCH remains sticky.
    """

    states: list[RiskState] = []
    state = initial_state
    clear_streak = 0

    for index, observation in enumerate(observations):
        if observation.data_status != DataStatus.OK or observation.mews_percentile is None:
            clear_streak = 0
            states.append(state)
            continue

        if observation.mews_percentile < config.clear_percentile:
            clear_streak += 1
        else:
            clear_streak = 0

        candidate = _candidate_state(observations, index, config)
        if clear_streak >= config.clear_days:
            state = RiskState.NORMAL
        elif _LEVEL[candidate] >= _LEVEL[state]:
            state = candidate
        elif candidate == RiskState.NORMAL:
            state = _ONE_DOWN[state]
        else:
            # Downgrade by at most one level even if current conditions imply a
            # much lower non-normal state.
            one_down = _ONE_DOWN[state]
            state = candidate if _LEVEL[candidate] >= _LEVEL[one_down] else one_down
        states.append(state)

    return states
