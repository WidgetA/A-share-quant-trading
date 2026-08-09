from __future__ import annotations

from datetime import date, timedelta

from src.margin_risk.config import MarginRiskConfig
from src.margin_risk.models import DataStatus, RiskObservation, RiskState
from src.margin_risk.state_machine import compute_risk_states


def _obs(
    day: int,
    mews: float | None,
    confirmation: float | None = 0,
    dlb: float | None = 0,
    rpp: float | None = 0,
    status: DataStatus = DataStatus.OK,
) -> RiskObservation:
    return RiskObservation(
        date(2024, 1, 1) + timedelta(days=day),
        mews,
        confirmation,
        dlb,
        rpp,
        status,
    )


def test_two_of_three_watch_and_warning_rules():
    cfg = MarginRiskConfig()
    states = compute_risk_states([_obs(0, 86), _obs(1, 20), _obs(2, 87)], cfg)
    assert states[-1] == RiskState.WATCH
    states = compute_risk_states([_obs(0, 96), _obs(1, 20), _obs(2, 97)], cfg)
    assert states[-1] == RiskState.WARNING


def test_danger_needs_two_consecutive_confirmed_days():
    cfg = MarginRiskConfig()
    states = compute_risk_states(
        [_obs(0, 96, 90), _obs(1, 96, 84), _obs(2, 96, 90), _obs(3, 97, 91)],
        cfg,
    )
    assert states[2] != RiskState.DANGER
    assert states[3] == RiskState.DANGER


def test_emergency_danger_rule_is_immediate():
    states = compute_risk_states([_obs(0, 20, 10, dlb=75, rpp=99)], MarginRiskConfig())
    assert states == [RiskState.DANGER]


def test_five_low_days_clear_and_no_direct_danger_to_normal():
    cfg = MarginRiskConfig()
    observations = [_obs(0, 99, 90), _obs(1, 99, 90)] + [
        _obs(index, 10, 10) for index in range(2, 7)
    ]
    states = compute_risk_states(observations, cfg)
    assert states[1] == RiskState.DANGER
    assert states[2] == RiskState.WARNING
    assert states[3] == RiskState.WATCH
    assert states[6] == RiskState.NORMAL


def test_partial_day_freezes_state_instead_of_false_clear():
    states = compute_risk_states(
        [
            _obs(0, 90),
            _obs(1, 90),
            _obs(2, None, status=DataStatus.PARTIAL),
        ],
        MarginRiskConfig(),
    )
    assert states[-1] == RiskState.WATCH


def test_incremental_window_inherits_sticky_prior_state():
    states = compute_risk_states(
        [_obs(index, 80) for index in range(10)],
        MarginRiskConfig(),
        initial_state=RiskState.WARNING,
    )
    assert states[0] == RiskState.WATCH
    assert states[-1] == RiskState.WATCH


def test_synthetic_scenario_d_progresses_watch_warning_danger():
    observations = [
        _obs(0, 86, 20),
        _obs(1, 88, 20),
        _obs(2, 96, 30),
        _obs(3, 97, 40),
        _obs(4, 98, 90, dlb=65, rpp=95),
        _obs(5, 99, 92, dlb=70, rpp=98),
    ]
    states = compute_risk_states(observations, MarginRiskConfig())
    assert states[1] == RiskState.WATCH
    assert states[3] == RiskState.WARNING
    assert states[5] == RiskState.DANGER
