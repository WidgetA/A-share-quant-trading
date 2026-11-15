"""Historical state expectations are independent of persistence and delivery."""

from datetime import date

import pytest

from src.strategy.v24_5.history import original_gate, replay_history
from src.strategy.v24_5.policy import genesis_state, prepare_transition


def signal(day, prior, raw):
    return {
        "trade_date": day,
        "prior_trade_date": prior,
        "raw_open": raw,
        "source": {"event_id": f"source-{day}"},
    }


def test_two_legacy_opens_make_next_original_open_the_third():
    result = replay_history(
        [signal("2026-11-12", "2026-11-11", True), signal("2026-11-13", "2026-11-12", True)],
        target_trade_date=date(2026, 11, 16),
    )
    assert result["state"]["open_streak"] == 2
    next_day = prepare_transition(
        result["state"],
        trade_date=date(2026, 11, 16),
        previous_trade_date=date(2026, 11, 13),
        raw_open=True,
    )
    assert next_day["permit"] is True
    assert next_day["state_after"]["mode"] == "NORMAL"


@pytest.mark.parametrize("unknown", [None, "missing"])
def test_unknown_or_missing_exchange_day_breaks_streak_not_as_closed(unknown):
    rows = [
        signal("2026-11-09", "2026-11-06", True),
        signal("2026-11-10", "2026-11-09", True),
        signal("2026-11-11", "2026-11-10", True),
    ]
    if unknown is None:
        rows.append(signal("2026-11-12", None, None))
    rows.append(signal("2026-11-13", "2026-11-12", False))
    result = replay_history(rows, target_trade_date=date(2026, 11, 16))
    assert result["state"]["mode"] == "NORMAL"
    assert result["state"]["closed_streak"] == 1
    assert result["state"]["open_streak"] == 0


def test_five_known_closes_reset_mode_and_short_pause_does_not():
    dates = [
        "2026-09-21",
        "2026-09-22",
        "2026-09-23",
        "2026-09-24",
        "2026-09-25",
        "2026-09-28",
        "2026-09-29",
        "2026-09-30",
    ]
    rows = [signal(day, dates[i - 1] if i else "2026-09-18", i < 3) for i, day in enumerate(dates)]
    four = replay_history(rows[:-1], target_trade_date=date(2026, 9, 30))
    five = replay_history(rows, target_trade_date=date(2026, 10, 8))
    assert four["state"]["mode"] == "NORMAL"
    assert five["state"]["mode"] == "TURTLE"
    assert five["state"]["closed_streak"] == 5


@pytest.mark.parametrize(
    "action,multiplier,expected",
    [
        ("ENTER", 1, True),
        ("BLOCK", 0, False),
        ("NO_SIGNAL", 0, False),
        ("INPUT_INVALID", 0, None),
    ],
)
def test_original_final_gate_is_action_not_normal_open(action, multiplier, expected):
    semantic = {
        "strategy_version": "V22-slim",
        "action": action,
        "final_multiplier": multiplier,
        "normal_open": True,
    }
    assert (
        original_gate(
            semantic, strategy_version="V22-slim", action=action, final_multiplier=multiplier
        )
        is expected
    )


def test_v245_uses_original_open_not_overlay_block():
    transition = prepare_transition(
        genesis_state(),
        trade_date=date(2026, 11, 12),
        previous_trade_date=date(2026, 11, 11),
        raw_open=True,
    )
    semantic = {
        "strategy_version": "V24.5",
        "action": "BLOCK",
        "final_multiplier": 0,
        "original_action": "ENTER",
        "original_final_multiplier": 1,
        "original_final_open": True,
        "v245_transition": transition,
    }
    assert (
        original_gate(semantic, strategy_version="V24.5", action="BLOCK", final_multiplier=0)
        is True
    )
    semantic["original_final_open"] = False
    with pytest.raises(ValueError):
        original_gate(semantic, strategy_version="V24.5", action="BLOCK", final_multiplier=0)


@pytest.mark.parametrize(
    "semantic,action,multiplier",
    [
        ({"strategy_version": "V20", "action": "ENTER", "final_multiplier": 1}, "ENTER", 1),
        ({"strategy_version": "V22-slim", "action": "ENTER", "final_multiplier": 1}, "BLOCK", 0),
        ({"strategy_version": "V22-slim", "action": "ENTER", "final_multiplier": 0}, "ENTER", 0),
        ({"strategy_version": "V22-slim", "action": "BLOCK", "final_multiplier": 1}, "BLOCK", 1),
    ],
)
def test_inconsistent_source_gate_cannot_seed_state(semantic, action, multiplier):
    with pytest.raises(ValueError):
        original_gate(
            semantic, strategy_version="V22-slim", action=action, final_multiplier=multiplier
        )


@pytest.mark.parametrize(
    "rows",
    [
        [signal("2026-11-16", "2026-11-13", True)],
        [signal("2026-11-13", "2026-11-12", True)] * 2,
        [signal("2026-11-13", "2026-11-13", True)],
        [signal("2026-11-13", None, True)],
    ],
)
def test_future_duplicate_or_unverifiable_known_session_rejected(rows):
    with pytest.raises(ValueError):
        replay_history(rows, target_trade_date=date(2026, 11, 16))


def test_no_history_is_genuine_genesis():
    assert replay_history([], target_trade_date=date(2026, 11, 16))["state"] == genesis_state()
