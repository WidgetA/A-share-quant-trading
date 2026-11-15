"""History must reach the production loader, not a mocked ready-made turtle state."""

from dataclasses import replace
from datetime import date, datetime, time
from functools import lru_cache

import pytest

from src.data.database.v20_repository import V20SemanticConflict, sha256_json
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.decision_engine import prepare_entry, prepare_invalid_entry
from tests.unit.data.database.test_v20_repository_contract import _FakeConnection, _repository
from tests.unit.strategy.test_v22_slim_entry import TZ, fixture

TARGET = date(2026, 11, 12)
CALENDAR = (
    date(2026, 11, 6),
    date(2026, 11, 9),
    date(2026, 11, 10),
    date(2026, 11, 11),
    TARGET,
    date(2026, 11, 13),
    date(2026, 11, 16),
)


@lru_cache(maxsize=None)
def original_row(day, raw_open=True):
    """A real original entry result, including the persisted previous session."""
    config, state, template = fixture()
    prior = CALENDAR[CALENDAR.index(day) - 1]
    snapshot = {
        **template.snapshot,
        "trade_date": day.isoformat(),
        "prior_trade_date": prior.isoformat(),
        "v22_slim_inputs": {
            **template.snapshot["v22_slim_inputs"],
            "h90": {"known": True, "block": not raw_open},
        },
    }
    bundle = replace(
        template,
        trade_date=day,
        frozen_at=datetime.combine(day, time(9, 40), tzinfo=TZ),
        prior_trade_date=prior,
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
        computation_calendar=CALENDAR,
    )
    prepared = prepare_entry(
        config=config,
        state=state,
        bundle=bundle,
        completed_health=(),
        completed_rolling=(),
        maturity_gaps=(),
        calendar=CALENDAR,
        artifacts=load_g_artifacts(
            config.artifact_manifest_path.parent,
            expected_manifest_sha256=config.artifact_manifest_sha256,
        ),
    )
    if raw_open is None:
        prepared = prepare_invalid_entry(
            config=config,
            state=state,
            trade_date=day,
            calendar=CALENDAR,
            reason_code="NO_CURRENT_DATA",
            detail="Missing realtime data is not a closed gate.",
            invalid_commit_not_before_ts=datetime.combine(day, time(9, 45), tzinfo=TZ),
        )
    commit = prepared.commit
    if raw_open is not None:
        assert (commit.action == "ENTER") is raw_open
    return {
        "trade_date": day,
        "slot_id": commit.slot_id,
        "slot_status": "FAILED" if raw_open is None else "COMPLETED",
        "official_stream_id": commit.official_stream_id,
        "lineage_id": commit.lineage_id,
        "strategy_version": "V22-slim",
        "slot_strategy_version": "V22-slim",
        "config_hash": commit.config_hash,
        "decision_id": commit.decision_id,
        "terminal_decision_id": commit.decision_id,
        "terminal_event_id": commit.event_id,
        "action": commit.action,
        "final_multiplier": commit.final_multiplier,
        "event_id": commit.event_id,
        "semantic_json": commit.semantic,
        "semantic_content_hash": commit.semantic_content_hash,
        "snapshot_id": commit.snapshot_id,
        "snapshot_json": commit.snapshot,
        "snapshot_hash": commit.snapshot_hash,
        "snapshot_trade_date": day,
        "event_semantic_json": commit.semantic,
        "event_semantic_content_hash": commit.semantic_content_hash,
        "event_type": "ENTRY_DECISION",
        "event_stream_id": commit.official_stream_id,
        "event_lineage_id": commit.lineage_id,
    }


@pytest.mark.parametrize("count,mode", [(2, "TURTLE"), (3, "NORMAL")])
async def test_empty_new_policy_inherits_real_original_gate_history(count, mode):
    rows = [original_row(day) for day in CALENDAR[4 - count : 4]]
    connection = _FakeConnection(
        fetchrows=[None, None, {"revision": 3}, None, None], fetches=[rows]
    )
    scope = {key: rows[0][key] for key in ("official_stream_id", "lineage_id")}
    result = await _repository(connection).load_v245_state_before(**scope, trade_date=TARGET)
    assert result["state_before"] == {
        "schema": "v24.5-policy-state/v1",
        "mode": mode,
        "open_streak": count,
        "closed_streak": 0,
        "last_trade_date": "2026-11-11",
    }
    assert result["expected_revision"] == 0
    writes = [call for call in connection.calls if call[0] == "execute"]
    assert len(writes) == 2
    assert ".v245_policy_state " in writes[0][1]
    assert ".v245_policy_bootstraps " in writes[1][1]
    assert not any("v245_policy_days" in call[1] for call in writes)


@pytest.mark.parametrize("unknown", [False, True])
async def test_missing_or_unknown_exchange_day_breaks_streak_without_becoming_closed(unknown):
    rows = [original_row(date(2026, 11, 9))]
    if unknown:
        rows.append(original_row(date(2026, 11, 10), None))
    rows.append(original_row(date(2026, 11, 11)))
    connection = _FakeConnection(
        fetchrows=[None, None, {"revision": 3}, None, None] + ([None] if unknown else []),
        fetches=[rows],
    )
    scope = {key: rows[0][key] for key in ("official_stream_id", "lineage_id")}
    result = await _repository(connection).load_v245_state_before(**scope, trade_date=TARGET)
    assert result["state_before"]["mode"] == "TURTLE"
    assert result["state_before"]["open_streak"] == 1
    assert result["state_before"]["closed_streak"] == 0
    assert result["state_before"]["last_trade_date"] == "2026-11-11"


@pytest.mark.parametrize(
    "field,value",
    [
        ("event_semantic_content_hash", "0" * 64),
        ("snapshot_hash", "0" * 64),
        ("event_stream_id", "wrong-stream"),
        ("event_lineage_id", "wrong-lineage"),
        ("terminal_event_id", "wrong-terminal"),
        ("snapshot_trade_date", date(2026, 11, 10)),
        ("slot_status", "COMMITTED"),
    ],
)
async def test_damaged_original_identity_fails_without_bootstrapping(field, value):
    good = original_row(date(2026, 11, 11))
    bad = {**good, field: value}
    connection = _FakeConnection(
        fetchrows=[None, None, {"revision": 3}, None, None], fetches=[[bad]]
    )
    scope = {key: good[key] for key in ("official_stream_id", "lineage_id")}
    with pytest.raises(V20SemanticConflict, match="historical"):
        await _repository(connection).load_v245_state_before(**scope, trade_date=TARGET)
    assert not any(call[0] == "execute" for call in connection.calls)


async def test_initializer_rechecks_after_lineage_lock_and_keeps_other_workers_state():
    row = original_row(date(2026, 11, 11))
    state = {
        "schema": "v24.5-policy-state/v1",
        "mode": "NORMAL",
        "open_streak": 3,
        "closed_streak": 0,
        "last_trade_date": "2026-11-11",
    }
    stored = {"revision": 0, "state_json": state, "state_hash": sha256_json(state)}
    connection = _FakeConnection(fetchrows=[None, None, {"revision": 3}, stored, None], fetches=[])
    scope = {key: row[key] for key in ("official_stream_id", "lineage_id")}
    result = await _repository(connection).load_v245_state_before(**scope, trade_date=TARGET)
    assert result["state_before"] == state
    assert not any(call[0] in {"execute", "fetch"} for call in connection.calls)
