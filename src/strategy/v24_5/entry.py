"""Apply turtle permission after the original V22 calculation is complete."""

from dataclasses import replace
from datetime import date
from typing import Any, Mapping

from src.data.database.v20_repository import sha256_json
from src.strategy.v20.decision_engine import PreparedEntry
from src.strategy.v20.identity import batch_id, decision_id, event_id, named_hash

from .policy import prepare_transition


def apply_turtle(
    prepared: PreparedEntry,
    state_context: Mapping[str, Any],
    previous_trade_date: date | None,
) -> PreparedEntry:
    """Keep original risk/health/reference calculations independent of permission."""
    commit = prepared.commit
    if commit.strategy_version != "V24.5" or commit.action not in {"ENTER", "BLOCK", "NO_SIGNAL"}:
        raise ValueError("turtle overlay requires a valid V24.5 original decision")
    if commit.semantic.get("entry_only") is not True or commit.model_batch is not None:
        raise ValueError("turtle overlay requires the V22 selection contract")
    if "v245_transition" in commit.semantic:
        raise ValueError("turtle overlay must not consume its own output as original signal")
    transition = prepare_transition(
        state_context["state_before"],
        trade_date=commit.trade_date,
        previous_trade_date=previous_trade_date,
        raw_open=commit.action == "ENTER",
    )
    transition["expected_revision"] = state_context["expected_revision"]
    permitted = transition["permit"]
    action = commit.action if permitted or commit.action != "ENTER" else "BLOCK"
    multiplier = commit.final_multiplier if permitted else 0.0
    snapshot = {
        **commit.snapshot,
        "v245_state_input": {
            "state_before": transition["state_before"],
            "expected_revision": transition["expected_revision"],
            "previous_trade_date": transition["previous_trade_date"],
        },
    }
    snapshot_hash = sha256_json(snapshot)
    decision = decision_id(
        commit.slot_id, commit.config_hash, snapshot_hash, commit.expected_state_hash
    )
    event = event_id("ENTRY_DECISION", decision)
    semantic = {
        **commit.semantic,
        "event_id": event,
        "decision_id": decision,
        "original_action": commit.action,
        "original_final_multiplier": commit.final_multiplier,
        "original_final_open": commit.action == "ENTER",
        "v245_transition": transition,
        "action": action,
        "final_multiplier": multiplier,
        "symbols": list(commit.semantic["symbols"]) if permitted else [],
        "reason_codes": [
            *commit.semantic["reason_codes"],
            *(
                ["TURTLE_WAIT_THREE_OPEN_DAYS"]
                if commit.action == "ENTER" and not permitted
                else []
            ),
        ],
    }
    updated = replace(
        commit,
        snapshot=snapshot,
        snapshot_hash=snapshot_hash,
        snapshot_id=named_hash(
            "V20_V16_SNAPSHOT_ID_V1",
            {"trade_date": commit.trade_date.isoformat(), "snapshot_hash": snapshot_hash},
        ),
        decision_id=decision,
        event_id=event,
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
        action=action,
        final_multiplier=multiplier,
        shadow_batches=tuple(
            replace(shadow, batch_id=batch_id(decision, shadow.kind))
            for shadow in commit.shadow_batches
        ),
    )
    return PreparedEntry(updated, action, multiplier)
