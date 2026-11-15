from datetime import date, timedelta

import pytest

from src.strategy.v24_5.policy import genesis_state, prepare_transition


def step(state, raw, day, previous=None):
    return prepare_transition(
        state,
        trade_date=day,
        previous_trade_date=previous or day - timedelta(days=1),
        raw_open=raw,
    )


def test_initial_turtle_requires_three_original_opens():
    state = genesis_state()
    permits = []
    for n in range(3):
        result = step(state, True, date(2026, 1, 5) + timedelta(days=n))
        permits.append(result["permit"])
        state = result["state_after"]
    assert permits == [False, False, True]
    assert state["mode"] == "NORMAL" and state["open_streak"] == 3


@pytest.mark.parametrize("closed_count", [1, 2, 3, 4, 5, 6])
def test_normal_recovery_depends_on_fifth_original_closed(closed_count):
    state = {**genesis_state(), "mode": "NORMAL", "last_trade_date": "2026-01-01"}
    for n in range(closed_count):
        result = step(state, False, date(2026, 1, 2) + timedelta(days=n))
        assert not result["permit"]
        state = result["state_after"]
    result = step(state, True, date(2026, 1, 2) + timedelta(days=closed_count))
    assert result["permit"] is (closed_count < 5)
    assert result["state_after"]["open_streak"] == 1


def test_unknown_is_not_closed_and_breaks_both_streaks():
    state = {
        **genesis_state(),
        "mode": "NORMAL",
        "closed_streak": 4,
        "last_trade_date": "2026-01-05",
    }
    result = step(state, None, date(2026, 1, 6))
    assert result["signal_status"] == "UNKNOWN" and not result["permit"]
    assert result["state_after"]["mode"] == "NORMAL"
    assert result["state_after"]["closed_streak"] == 0
    assert result["state_after"]["open_streak"] == 0


def test_missing_exchange_session_breaks_consecutive_opens():
    state = {**genesis_state(), "open_streak": 2, "last_trade_date": "2026-01-05"}
    result = step(state, True, date(2026, 1, 7), date(2026, 1, 6))
    assert result["continuity_broken"] and not result["permit"]
    assert result["state_after"]["open_streak"] == 1


def test_exchange_weekend_is_not_a_missing_session():
    state = {**genesis_state(), "open_streak": 2, "last_trade_date": "2026-01-09"}
    result = step(state, True, date(2026, 1, 12), date(2026, 1, 9))
    assert not result["continuity_broken"] and result["permit"]


def test_same_day_recompute_from_frozen_before_is_deterministic_without_mutation():
    state = {**genesis_state(), "open_streak": 1, "last_trade_date": "2026-01-05"}
    before = dict(state)
    first = step(state, True, date(2026, 1, 6))
    assert step(state, True, date(2026, 1, 6)) == first
    assert state == before and first["state_after"]["open_streak"] == 2
    with pytest.raises(ValueError, match="before"):
        step(first["state_after"], True, date(2026, 1, 6))


@pytest.mark.parametrize("raw", [0, 1, "false", "true", [], {}])
def test_raw_gate_requires_bool_or_unknown(raw):
    with pytest.raises(ValueError):
        step(genesis_state(), raw, date(2026, 1, 5))


def test_invalid_state_and_future_calendar_are_rejected():
    with pytest.raises(ValueError):
        step({**genesis_state(), "open_streak": True}, True, date(2026, 1, 5))
    with pytest.raises(ValueError):
        step(genesis_state(), True, date(2026, 1, 5), date(2026, 1, 5))
