"""Pure replay of verified original gates; no trades, messages or database writes."""

import math
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

from .policy import genesis_state, prepare_transition

HISTORY_SCHEMA = "v24.5-policy-bootstrap/v1"
FAMILY = frozenset({"V22-slim", "V24.5"})


def _multiplier(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("gate multiplier cannot be bool")
    result = float(value)
    if not math.isfinite(result) or not 0 <= result <= 1:
        raise ValueError("invalid gate multiplier")
    return result


def original_gate(
    semantic: Mapping[str, Any],
    *,
    strategy_version: str,
    action: str,
    final_multiplier: Any,
) -> bool | None:
    """Validate source identity and recover the original gate, never the overlay.

    A valid original BLOCK/NO_SIGNAL is closed. INPUT_INVALID is unknown. The
    original normal_open flag is not the final gate (it omits the D0 risk veto).
    """
    if strategy_version not in FAMILY or semantic.get("strategy_version") != strategy_version:
        raise ValueError("historical strategy identity mismatch")
    weight = _multiplier(final_multiplier)
    if semantic.get("action") != action or _multiplier(semantic.get("final_multiplier")) != weight:
        raise ValueError("historical action/multiplier identity mismatch")
    if action not in {"ENTER", "BLOCK", "NO_SIGNAL", "INPUT_INVALID"}:
        raise ValueError("unsupported historical action")
    if (weight > 0) != (action == "ENTER"):
        raise ValueError("historical action contradicts multiplier")
    if action == "INPUT_INVALID":
        return None
    if strategy_version == "V22-slim":
        return action == "ENTER"

    original_action = semantic.get("original_action")
    original_weight = _multiplier(semantic.get("original_final_multiplier"))
    raw = semantic.get("original_final_open")
    if (
        original_action not in {"ENTER", "BLOCK", "NO_SIGNAL"}
        or type(raw) is not bool
        or raw != (original_action == "ENTER")
        or (original_weight > 0) != raw
    ):
        raise ValueError("V24.5 original gate identity mismatch")
    transition = semantic.get("v245_transition")
    if not isinstance(transition, Mapping) or transition.get("raw_open") is not raw:
        raise ValueError("V24.5 original gate lacks its transition")
    previous = transition.get("previous_trade_date")
    canonical = prepare_transition(
        transition["state_before"],
        trade_date=date.fromisoformat(transition["trade_date"]),
        previous_trade_date=date.fromisoformat(previous) if previous else None,
        raw_open=raw,
    )
    # Revision is persistence metadata; every actual transition field must agree.
    if {k: v for k, v in transition.items() if k != "expected_revision"} != canonical:
        raise ValueError("V24.5 stored transition is not its original gate transition")
    expected_action = "BLOCK" if raw and not canonical["permit"] else original_action
    expected_weight = original_weight if canonical["permit"] else 0.0
    if action != expected_action or weight != expected_weight:
        raise ValueError("V24.5 overlay output contradicts its transition")
    return raw


def replay_history(
    records: Sequence[Mapping[str, Any]],
    *,
    target_trade_date: date,
) -> dict[str, Any]:
    """Replay one verified source per exchange day, strictly before the target.

    The snapshot's actual predecessor defines continuity. Missing/unknown days
    reset counts, not the current mode; weekends never need invented records.
    """
    if type(target_trade_date) is not date:
        raise ValueError("invalid historical replay target")
    state = genesis_state()
    trace = []
    sources = []
    for record in records:
        day = date.fromisoformat(record["trade_date"])
        previous_value = record["prior_trade_date"]
        previous = date.fromisoformat(previous_value) if previous_value else None
        raw = record["raw_open"]
        if day >= target_trade_date or (raw is not None and previous is None):
            raise ValueError("history must be prior and known gates need a verified predecessor")
        transition = prepare_transition(
            state, trade_date=day, previous_trade_date=previous, raw_open=raw
        )
        source = dict(record["source"])
        trace.append({"source": source, "transition": transition})
        sources.append(source)
        state = transition["state_after"]
    return {
        "schema": HISTORY_SCHEMA,
        "target_trade_date": target_trade_date.isoformat(),
        "history_until": state["last_trade_date"],
        "state": state,
        "source_events": sources,
        "trace": trace,
    }
