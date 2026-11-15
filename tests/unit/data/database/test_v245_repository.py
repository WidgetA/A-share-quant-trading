import json
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from functools import lru_cache

import pytest

from src.data.database.v20_repository import (
    ShadowBatchWrite,
    V20SemanticConflict,
    V20StateConflict,
    sha256_json,
)
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.decision_engine import prepare_entry, prepare_invalid_entry
from src.strategy.v20.runtime_config import load_v20_runtime_config
from src.strategy.v24_5.entry import apply_turtle
from src.strategy.v24_5.policy import genesis_state, prepare_transition
from tests.unit.data.database.test_v20_repository_contract import (
    _entry,
    _FakeConnection,
    _outbox_event_row,
    _repository,
)
from tests.unit.strategy.test_v22_slim_entry import ROOT, fixture


def transition(raw=True, before=None, revision=0):
    return {
        **prepare_transition(
            before or genesis_state(),
            trade_date=date(2026, 8, 31),
            previous_trade_date=date(2026, 8, 28),
            raw_open=raw,
        ),
        "expected_revision": revision,
    }


@lru_cache(maxsize=3)
def original_prepared(raw):
    _, state, bundle = fixture()
    config = load_v20_runtime_config(ROOT, ROOT / "config/v24-5.yaml")
    day = date(2026, 8, 31)
    calendar = tuple(day + timedelta(days=n) for n in range(-3, 4))
    if raw is None:
        return prepare_invalid_entry(
            config=config,
            state=state,
            trade_date=day,
            calendar=calendar,
            reason_code="NO_CURRENT_DATA",
            detail="fixture unavailable",
            invalid_commit_not_before_ts=datetime.combine(
                day, time(9, 45), tzinfo=bundle.frozen_at.tzinfo
            ),
        )
    snapshot = {
        **bundle.snapshot,
        "trade_date": str(day),
        "v22_slim_inputs": {
            **bundle.snapshot["v22_slim_inputs"],
            "h90": {"known": True, "block": not raw},
        },
    }
    bundle = replace(
        bundle,
        trade_date=day,
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
        computation_calendar=calendar,
    )
    return prepare_entry(
        config=config,
        state=state,
        bundle=bundle,
        calendar=calendar,
        completed_health=(),
        completed_rolling=(),
        maturity_gaps=(),
        artifacts=load_g_artifacts(
            config.artifact_manifest_path.parent,
            expected_manifest_sha256=config.artifact_manifest_sha256,
        ),
    )


def commit(payload):
    prepared = original_prepared(payload["raw_open"])
    context = {
        "state_before": payload["state_before"],
        "expected_revision": payload["expected_revision"],
    }
    if payload["raw_open"] is not None:
        original = apply_turtle(
            prepared, context, date.fromisoformat(payload["previous_trade_date"])
        ).commit
    else:
        original = prepared.commit
        snapshot = {
            **original.snapshot,
            "v245_state_input": {**context, "previous_trade_date": payload["previous_trade_date"]},
        }
        original = replace(original, snapshot=snapshot, snapshot_hash=sha256_json(snapshot))
    semantic = {**original.semantic, "v245_transition": payload}
    return replace(
        original,
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
    )


def state_row(state, revision):
    return {"revision": revision, "state_json": state, "state_hash": sha256_json(state)}


def day_row(payload):
    return {
        "signal_status": payload["signal_status"],
        "raw_open": payload["raw_open"],
        "state_before_json": payload["state_before"],
        "state_before_hash": sha256_json(payload["state_before"]),
        "transition_json": payload,
        "transition_hash": sha256_json(payload),
    }


async def test_read_absent_starts_turtle_without_writing():
    connection = _FakeConnection(fetchrows=[None, None])
    value = await _repository(connection).load_v245_state_before(
        official_stream_id="official", lineage_id="lineage-1", trade_date=date(2026, 8, 31)
    )
    assert value == {
        "state_before": genesis_state(),
        "expected_revision": 0,
        "day_status": "ABSENT",
    }
    assert not any(c[0] == "execute" for c in connection.calls)


async def test_read_rerun_uses_frozen_before_not_current_after():
    payload = transition()
    connection = _FakeConnection(fetchrows=[state_row(payload["state_after"], 1), day_row(payload)])
    result = await _repository(connection).load_v245_state_before(
        official_stream_id="official", lineage_id="lineage-1", trade_date=date(2026, 8, 31)
    )
    assert result["state_before"] == genesis_state()
    assert result["expected_revision"] == 1 and result["day_status"] == "KNOWN"


async def test_same_day_known_rerun_does_not_advance_again():
    first = transition()
    rerun = transition(revision=1)
    connection = _FakeConnection(fetchrows=[state_row(first["state_after"], 1), day_row(first)])
    assert (
        await _repository(connection)._commit_v245_transition(connection, commit(rerun), "run-2")
        is False
    )
    assert not any("UPDATE" in c[1] and c[0] == "execute" for c in connection.calls)


@pytest.mark.parametrize("first_raw,rerun_raw", [(False, True), (True, False)])
async def test_same_day_recalculation_may_change_target_without_recounting(first_raw, rerun_raw):
    before = {**genesis_state(), "open_streak": 2, "last_trade_date": "2026-08-28"}
    first = transition(first_raw, before)
    rerun = transition(rerun_raw, before, revision=1)
    assert first["permit"] is not rerun["permit"]
    connection = _FakeConnection(fetchrows=[state_row(first["state_after"], 1), day_row(first)])
    assert (
        await _repository(connection)._commit_v245_transition(connection, commit(rerun), "new-run")
        is False
    )
    assert not any(c[0] == "execute" and "UPDATE" in c[1] for c in connection.calls)


async def test_first_valid_after_unknown_uses_day_before_once():
    unknown = transition(None)
    valid = transition(True, revision=1)
    connection = _FakeConnection(
        fetchrows=[state_row(unknown["state_after"], 1), day_row(unknown)],
        executes=["INSERT 0 0", "INSERT 0 1", "UPDATE 1"],
    )
    assert await _repository(connection)._commit_v245_transition(connection, commit(valid), "valid")
    updates = [c for c in connection.calls if c[0] == "execute" and "UPDATE" in c[1]]
    assert updates and valid["state_after"]["open_streak"] == 1


async def test_stale_state_and_fabricated_transition_are_rejected():
    current = {**genesis_state(), "last_trade_date": "2026-08-28", "open_streak": 1}
    connection = _FakeConnection(fetchrows=[state_row(current, 1), None])
    with pytest.raises(V20StateConflict):
        await _repository(connection)._commit_v245_transition(
            connection, commit(transition()), "stale"
        )
    invalid = transition()
    invalid["state_after"] = {**invalid["state_after"], "mode": "NORMAL"}
    connection = _FakeConnection()
    with pytest.raises(V20SemanticConflict):
        await _repository(connection)._commit_v245_transition(connection, commit(invalid), "forged")
    assert connection.calls == []


async def test_old_unknown_cannot_rewrite_already_processed_later_days():
    unknown = transition(None)
    current = {**genesis_state(), "last_trade_date": "2026-09-01", "open_streak": 1}
    connection = _FakeConnection(fetchrows=[state_row(current, 2), day_row(unknown)])
    with pytest.raises(V20StateConflict, match="later"):
        await _repository(connection)._commit_v245_transition(
            connection, commit(transition(revision=2)), "late"
        )


async def test_v245_requires_explicit_transition_but_legacy_does_not():
    connection = _FakeConnection()
    bad = replace(_entry("BLOCK"), strategy_version="V24.5")
    with pytest.raises(V20SemanticConflict):
        await _repository(connection)._commit_v245_transition(connection, bad, "missing")
    assert (
        await _repository(connection)._commit_v245_transition(connection, _entry("BLOCK"), "legacy")
        is False
    )
    assert connection.calls == []


async def test_valid_reference_absent_does_not_manufacture_a_closed_signal():
    connection = _FakeConnection(fetchrows=[None])
    assert (
        await _repository(connection).get_valid_selection_reference(
            "official", date(2026, 8, 31), lineage_id="lineage-1"
        )
        is None
    )


async def test_valid_reference_checks_proposal_and_real_outbox_snapshot_binding():
    original = _entry("BLOCK")
    semantic = {
        **original.semantic,
        "strategy_version": "V24.5",
        "event_id": "new-run-event",
        "decision_id": original.decision_id,
        "action": original.action,
        "final_multiplier": original.final_multiplier,
    }
    proposal = {
        "semantic": {**semantic, "event_id": original.event_id},
        "snapshot_hash": original.snapshot_hash,
    }
    row = {
        "official_stream_id": "official",
        "lineage_id": "lineage-1",
        "trade_date": original.trade_date,
        "config_hash": original.config_hash,
        "slot_id": original.slot_id,
        "slot_status": "FAILED",
        "slot_revision": 1,
        "event_id": "new-run-event",
        "action_expiry_ts": original.action_expiry_ts,
        "proposal_json": proposal,
        "proposal_hash": sha256_json(proposal),
        "semantic_json": semantic,
        "semantic_content_hash": sha256_json(semantic),
        "snapshot_id": original.snapshot_id,
        "snapshot_json": original.snapshot,
        "snapshot_hash": original.snapshot_hash,
    }
    connection = _FakeConnection(fetchrows=[row])
    found = await _repository(connection).get_valid_selection_reference(
        "official", original.trade_date, lineage_id="lineage-1"
    )
    assert found.event_id == "new-run-event"
    assert found.slot_status == "FAILED" and found.action == "BLOCK"
    assert found.config_id == original.config_hash[:24]
    assert found.semantic == semantic
    corrupted = {**row, "proposal_hash": "0" * 64}
    with pytest.raises(V20SemanticConflict):
        await _repository(_FakeConnection(fetchrows=[corrupted])).get_valid_selection_reference(
            "official", original.trade_date, lineage_id="lineage-1"
        )


async def test_first_valid_shadow_repair_keeps_original_payload_and_event():
    shadow = ShadowBatchWrite(
        "shadow-1",
        "HEALTH",
        date(2026, 8, 31),
        date(2026, 9, 2),
        {"top3": [{"code": "600001"}], "comparison_pool_codes": ["600001"]},
    )
    actual = replace(commit(transition()), shadow_batches=(shadow,))
    row = {
        "batch_id": shadow.batch_id,
        "kind": shadow.kind,
        "signal_date": shadow.signal_date,
        "t2_date": shadow.t2_date,
        "batch_json": shadow.payload,
        "official_stream_id": actual.official_stream_id,
        "lineage_id": actual.lineage_id,
    }
    connection = _FakeConnection(fetchvals=["valid-event"], fetchrows=[row])
    await _repository(connection)._repair_v245_reference_shadows(connection, actual, "valid-event")
    inserts = [c for c in connection.calls if c[0] == "execute"]
    assert len(inserts) == 2
    assert json.loads(inserts[0][2][-1]) == shadow.payload
    assert "valid-event" in inserts[1][2]
    # A later fresh input cannot overwrite the first valid reference universe.
    connection = _FakeConnection(fetchvals=["first-event"])
    await _repository(connection)._repair_v245_reference_shadows(connection, actual, "later-event")
    assert not any(c[0] == "execute" for c in connection.calls)


@pytest.mark.parametrize("kind", ["ENTRY_DECISION", "V245_SWITCH_EXIT_ALERT"])
async def test_obsolete_target_is_suppressed_before_any_delivery_attempt(kind):
    semantic = {
        "strategy_version": "V24.5",
        "v245_switch_plan": {"target": "BANK"},
        "alert_code": kind,
        "switch_target": "BANK",
        "position_id": "old-stock",
    }
    row = {
        "event_type": "ENTRY_DECISION" if kind == "ENTRY_DECISION" else "DATA_ALERT",
        "semantic_json": semantic,
        "attempt_count": 0,
        "action_expiry_ts": None,
        "db_now": datetime.now().astimezone(),
    }
    latest = {"intent_id": "new-target", "target": "STOCKS", "last_event_id": "new-event"}
    connection = _FakeConnection(fetchrows=[row, latest], executes=["SELECT 1"])
    result = await _repository(connection).begin_delivery_attempt(
        "old-message",
        worker_id="worker",
        route_id="route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    assert result is None
    assert not any("INSERT INTO v20.delivery_attempts" in c[1] for c in connection.calls)
    assert any("INSERT INTO v20.v245_delivery_suppressions" in c[1] for c in connection.calls)
    locks = [c[1] for c in connection.calls if "FOR UPDATE" in c[1]]
    assert "official_state" in locks[0] and "outbox_events" in locks[1]


@pytest.mark.parametrize("quantity,status", [(0, "MONITORING"), (100, "CLOSED")])
async def test_closed_or_zero_position_cannot_receive_a_switch_exit(quantity, status):
    row = {
        "event_type": "DATA_ALERT",
        "semantic_json": {
            "alert_code": "V245_SWITCH_EXIT_ALERT",
            "strategy_version": "V24.5",
            "switch_target": "BANK",
            "position_id": "old-stock",
        },
        "attempt_count": 0,
        "action_expiry_ts": None,
        "db_now": datetime.now().astimezone(),
    }
    latest = {"intent_id": "same-target", "target": "BANK", "last_event_id": "new-event"}
    position = {"asset_kind": "STOCK", "quantity": quantity, "status": status}
    connection = _FakeConnection(fetchrows=[row, latest, position], executes=["SELECT 1"])
    assert (
        await _repository(connection).begin_delivery_attempt(
            "old-message",
            worker_id="worker",
            route_id="route",
            official_stream_id="official",
            lineage_id="lineage-1",
        )
        is None
    )
    assert not any("INSERT INTO v20.delivery_attempts" in c[1] for c in connection.calls)


@pytest.mark.parametrize("direction_sent", [False, True])
async def test_cleanup_lease_cannot_bypass_direction_receipt_after_target_swings_back(
    direction_sent,
):
    row = {
        "event_type": "DATA_ALERT",
        "semantic_json": {
            "alert_code": "V245_SWITCH_EXIT_ALERT",
            "strategy_version": "V24.5",
            "switch_target": "BANK",
            "position_id": "old-stock",
            "target_event_id": "main",
        },
        "attempt_count": 0,
        "action_expiry_ts": None,
        "db_now": datetime.now().astimezone(),
    }
    latest = {"intent_id": "new-bank-phase", "target": "BANK", "last_event_id": "new-event"}
    position = {"asset_kind": "STOCK", "quantity": 100, "status": "MONITORING"}
    connection = _FakeConnection(
        fetchrows=[
            row,
            latest,
            position,
            {"attempt_number": 1, "delivery_variant": "PRIMARY"},
            {"event_id": "old-message"},
        ],
        fetchvals=[direction_sent],
        executes=["SELECT 1"],
    )
    result = await _repository(connection).begin_delivery_attempt(
        "old-message",
        worker_id="worker",
        route_id="route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    if direction_sent:
        assert result is not None and result.attempt_number == 1
    else:
        assert result is None
        assert not any("INSERT INTO v20.delivery_attempts" in c[1] for c in connection.calls)
        assert not any(
            "INSERT INTO v20.v245_delivery_suppressions" in c[1] for c in connection.calls
        )
        assert any("delivery_status='PENDING'" in c[1] for c in connection.calls)


async def test_suppressed_events_are_visible_but_not_reported_as_dispatch_failures():
    row = dict.fromkeys(
        (
            "unsealed_n",
            "pending_delivery_n",
            "leased_n",
            "dispatching_n",
            "stale_started_n",
            "terminal_unknown_n",
            "unknown_n",
            "seal_error_n",
            "pending_delivery_error_n",
            "unknown_error_n",
            "max_seal_attempt_count",
            "max_delivery_attempt_count",
        ),
        0,
    )
    row.update(
        dict.fromkeys(
            ("last_seal_attempt_at", "oldest_unsent_at", "oldest_unknown_at", "last_delivered_at")
        )
    )
    row["suppressed_count"] = 3
    connection = _FakeConnection(fetchrows=[row])
    health = await _repository(connection).get_outbox_health(
        route_id="route", official_stream_id="official", lineage_id="lineage-1"
    )
    assert health["suppressed_count"] == 3
    assert health["pending_delivery_n"] == health["delivery_error_n"] == 0
    assert "v245_delivery_suppressions" in connection.calls[0][1]
    message = _outbox_event_row()
    message["last_error"] = "V245_TARGET_SUPERSEDED"
    record = _repository(connection)._outbox_from_row(message)
    assert record.last_error == "V245_TARGET_SUPERSEDED"
    assert record.delivery_status == "PENDING" and record.attempt_count == 0


@pytest.mark.parametrize(
    "corruption",
    [
        "enter_while_blocked",
        "retained_symbols",
        "raw_flag",
        "original_action",
        "original_multiplier",
        "snapshot_before",
    ],
)
async def test_real_entry_overlay_cannot_be_committed_with_contradictory_permission(corruption):
    actual = commit(transition(True))
    assert actual.action == "BLOCK" and actual.semantic["original_action"] == "ENTER"
    semantic = dict(actual.semantic)
    if corruption == "enter_while_blocked":
        semantic.update(action="ENTER", final_multiplier=1.0)
        actual = replace(actual, action="ENTER", final_multiplier=1.0)
    elif corruption == "retained_symbols":
        semantic["symbols"] = semantic["reference_symbols"][:3]
    elif corruption == "raw_flag":
        semantic["original_final_open"] = False
    elif corruption == "original_action":
        semantic["original_action"] = "BLOCK"
    elif corruption == "original_multiplier":
        semantic["original_final_multiplier"] = 0.0
    else:
        snapshot = {
            **actual.snapshot,
            "v245_state_input": {**actual.snapshot["v245_state_input"], "expected_revision": 10},
        }
        actual = replace(actual, snapshot=snapshot, snapshot_hash=sha256_json(snapshot))
    actual = replace(actual, semantic=semantic, semantic_content_hash=sha256_json(semantic))
    connection = _FakeConnection()
    with pytest.raises(V20SemanticConflict):
        await _repository(connection)._commit_v245_transition(connection, actual, "bad")
    assert connection.calls == []
