"""Pure turtle state transitions driven only by known original V22 daily gates."""

from collections.abc import Mapping
from datetime import date
from typing import Any

STATE_SCHEMA = "v24.5-policy-state/v1"
TRANSITION_SCHEMA = "v24.5-transition/v1"
_STATE_KEYS = {"schema", "mode", "open_streak", "closed_streak", "last_trade_date"}


def genesis_state() -> dict[str, Any]:
    return {
        "schema": STATE_SCHEMA,
        "mode": "TURTLE",
        "open_streak": 0,
        "closed_streak": 0,
        "last_trade_date": None,
    }


def validate_state(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _STATE_KEYS:
        raise ValueError("invalid V24.5 state fields")
    result = dict(value)
    if result["schema"] != STATE_SCHEMA or result["mode"] not in {"TURTLE", "NORMAL"}:
        raise ValueError("invalid V24.5 state identity")
    for key in ("open_streak", "closed_streak"):
        if type(result[key]) is not int or result[key] < 0:
            raise ValueError("invalid V24.5 consecutive count")
    if result["open_streak"] and result["closed_streak"]:
        raise ValueError("V24.5 open and closed streaks cannot coexist")
    last = result["last_trade_date"]
    if last is not None:
        if not isinstance(last, str) or date.fromisoformat(last).isoformat() != last:
            raise ValueError("invalid V24.5 last trade date")
    elif result["open_streak"] or result["closed_streak"]:
        raise ValueError("unstarted V24.5 state cannot contain a streak")
    return result


def prepare_transition(
    state_before: Mapping[str, Any],
    *,
    trade_date: date,
    previous_trade_date: date | None,
    raw_open: bool | None,
) -> dict[str, Any]:
    """Prepare one day from its immutable prestate, without mutating any input.

    A missing exchange session or unknown signal breaks consecutive counts.
    Unknown is never counted as closed and does not by itself switch the mode.
    Same-day reruns must use that day's frozen *before* state, not its after state.
    Persistence, not repeated calls to this pure function, makes advancement once/day.
    """
    before = validate_state(state_before)
    if type(trade_date) is not date or (
        previous_trade_date is not None
        and (type(previous_trade_date) is not date or previous_trade_date >= trade_date)
    ):
        raise ValueError("invalid V24.5 exchange session dates")
    if raw_open is not None and type(raw_open) is not bool:
        raise ValueError("original gate must be bool or unknown")
    last = before["last_trade_date"]
    if last is not None and date.fromisoformat(last) >= trade_date:
        raise ValueError("V24.5 requires the frozen state before the target day")
    previous = previous_trade_date.isoformat() if previous_trade_date else None
    broken = last is not None and last != previous
    opened = 0 if broken else before["open_streak"]
    closed = 0 if broken else before["closed_streak"]
    mode = before["mode"]
    if raw_open is None:
        opened = closed = 0
    elif raw_open:
        opened, closed = opened + 1, 0
        if mode == "TURTLE" and opened >= 3:
            mode = "NORMAL"
    else:
        opened, closed = 0, closed + 1
        if mode == "NORMAL" and closed >= 5:
            mode = "TURTLE"
    after = {
        "schema": STATE_SCHEMA,
        "mode": mode,
        "open_streak": opened,
        "closed_streak": closed,
        "last_trade_date": trade_date.isoformat(),
    }
    return {
        "schema": TRANSITION_SCHEMA,
        "trade_date": trade_date.isoformat(),
        "previous_trade_date": previous,
        "raw_open": raw_open,
        "signal_status": "UNKNOWN" if raw_open is None else "KNOWN",
        "state_before": before,
        "state_after": after,
        "permit": raw_open is True and mode == "NORMAL",
        "continuity_broken": broken,
    }
