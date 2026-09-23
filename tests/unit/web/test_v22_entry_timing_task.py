"""The advisory belongs to the same timer/button selection transaction."""

from dataclasses import dataclass, replace
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

from src.data.database.v20_repository import sha256_json
from src.web.v20_service import V20Service

TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 9, 23, 9, 41, tzinfo=TZ)


@dataclass(frozen=True)
class _Commit:
    semantic: dict
    semantic_content_hash: str
    trade_date: date = NOW.date()


def _task():
    service = object.__new__(V20Service)
    service._clock = lambda: NOW
    service.config = SimpleNamespace(
        official_stream_id="stream",
        state_lineage_id="lineage",
        config_hash="a" * 64,
        strategy_version="V22-slim",
    )
    service._repository = SimpleNamespace(
        assert_runtime_leader=AsyncMock(),
        get_selection_run_event_id=AsyncMock(return_value=None),
        commit_selection_run=AsyncMock(return_value="entry"),
        seal_event=AsyncMock(),
        get_entry_status=AsyncMock(return_value=None),
    )
    service._reconcile_missed_slots = AsyncMock()
    service._expire_reference_gaps = AsyncMock()
    service._process_mature_shadow = AsyncMock()
    service.kick_mews_for_selection_trigger = lambda now: None
    semantic = {
        "strategy_version": "V22-slim",
        "action": "ENTER",
        "symbols": [{"code": "600001", "name": "测试股票"}],
    }
    commit = _Commit(semantic, sha256_json(semantic))
    calculation = SimpleNamespace(
        prepared=SimpleNamespace(commit=commit),
        bundle=SimpleNamespace(),
        canonical_first_received_at=NOW,
    )
    service._orchestrate_morning_selection = AsyncMock(return_value=calculation)
    record = SimpleNamespace(semantic={"entry_timing_alert_event_id": "alert"})
    service._repository.seal_event.return_value = record

    async def add_advisory(result):
        assert result is calculation
        enriched = {**commit.semantic, "entry_timing_advisory": {"status": "WAIT"}}
        return replace(commit, semantic=enriched, semantic_content_hash=sha256_json(enriched))

    service._prepare_entry_timing_commit = AsyncMock(side_effect=add_advisory)
    context = SimpleNamespace(trade_date=date(2026, 9, 23), calendar=())
    return service, context


@pytest.mark.parametrize("trigger", ["timer", "button"])
async def test_complete_task_adds_advisory_before_commit_and_seals_both(trigger):
    service, context = _task()
    if trigger == "timer":
        await service._commit_entry_from_bundle(context, NOW)
    else:
        await service._execute_selection_task(context, NOW, request_id="button-new")
    committed = service._repository.commit_selection_run.await_args.args[0]
    assert committed.semantic.get("entry_timing_advisory") == {"status": "WAIT"}
    assert committed.semantic_content_hash == sha256_json(committed.semantic)
    assert [c.args[0] for c in service._repository.seal_event.await_args_list] == ["entry", "alert"]


async def test_same_request_retry_does_not_recompute_advisory_but_recovers_sealing():
    service, context = _task()
    service._repository.get_selection_run_event_id.return_value = "entry"
    _, created = await service._execute_selection_task(context, NOW, request_id="same-request")
    assert created is False
    service._prepare_entry_timing_commit.assert_not_awaited()
    service._orchestrate_morning_selection.assert_not_awaited()
    service._repository.commit_selection_run.assert_not_awaited()
    assert [c.args[0] for c in service._repository.seal_event.await_args_list] == ["entry", "alert"]


@pytest.mark.parametrize(
    "action,status,reason",
    [
        ("BLOCK", "NO_WAIT", "ORIGINAL_GATE_BLOCKED"),
        ("NO_SIGNAL", "NO_WAIT", "NO_CANDIDATES"),
        ("INPUT_INVALID", "UNAVAILABLE", "ORIGINAL_INPUT_INVALID"),
    ],
)
async def test_unallowed_selection_reports_status_without_fetching_timing_data(
    action, status, reason, monkeypatch
):
    import src.strategy.v22_slim.entry_timing as timing

    service, _ = _task()
    semantic = {
        "strategy_version": "V22-slim",
        "action": action,
        "reason_codes": ["ORIGINAL_REASON"],
        "symbols": [],
        "final_multiplier": 0,
    }
    commit = _Commit(semantic, sha256_json(semantic))
    calculation = SimpleNamespace(prepared=SimpleNamespace(commit=commit))
    # No client or bundle is needed when the original rule does not allow entry.
    evaluator = AsyncMock()
    monkeypatch.setattr(timing, "evaluate_entry_timing", evaluator)
    actual = await V20Service._prepare_entry_timing_commit(service, calculation)
    advisory = actual.semantic.get("entry_timing_advisory")
    assert advisory is not None
    assert advisory["schema"] == "v22-entry-timing/v1"
    assert advisory["status"] == status
    assert advisory["reason"] == reason
    assert advisory["original_action"] == action
    assert advisory["original_reason_codes"] == semantic["reason_codes"]
    assert advisory["trade_date"] == "2026-09-23"
    assert advisory["evaluated_at"] == NOW.isoformat()
    assert advisory["symbols"] == []
    assert advisory["index"] == {}
    assert {
        key: value for key, value in actual.semantic.items() if key != "entry_timing_advisory"
    } == semantic
    assert actual.semantic_content_hash == sha256_json(actual.semantic)
    evaluator.assert_not_awaited()


@pytest.mark.parametrize("trigger", ["timer", "button"])
@pytest.mark.parametrize(
    "action,reason", [("BLOCK", "ORIGINAL_GATE_BLOCKED"), ("NO_SIGNAL", "NO_CANDIDATES")]
)
async def test_complete_task_persists_nonmatching_status_for_both_triggers(trigger, action, reason):
    service, context = _task()
    del service._prepare_entry_timing_commit
    calculation = service._orchestrate_morning_selection.return_value
    semantic = {
        "strategy_version": "V22-slim",
        "action": action,
        "reason_codes": ["ORIGINAL_REASON"],
        "symbols": [],
    }
    calculation.prepared.commit = _Commit(semantic, sha256_json(semantic))
    if trigger == "timer":
        await service._commit_entry_from_bundle(context, NOW)
    else:
        await service._execute_selection_task(context, NOW, request_id="button-new")
    committed = service._repository.commit_selection_run.await_args.args[0]
    advisory = committed.semantic.get("entry_timing_advisory")
    assert advisory is not None
    assert advisory["status"] == "NO_WAIT"
    assert advisory["reason"] == reason
    assert committed.semantic["action"] == action
    assert committed.semantic["reason_codes"] == ["ORIGINAL_REASON"]
    assert committed.semantic_content_hash == sha256_json(committed.semantic)
    assert [c.args[0] for c in service._repository.seal_event.await_args_list] == ["entry", "alert"]


@pytest.mark.parametrize("action", ["ENTER", "BLOCK", "NO_SIGNAL", "INPUT_INVALID"])
async def test_v20_selection_keeps_original_commit_without_advisory(action):
    service, _ = _task()
    service.config.strategy_version = "V20"
    semantic = {"strategy_version": "V20", "action": action}
    commit = _Commit(semantic, sha256_json(semantic))
    calculation = SimpleNamespace(prepared=SimpleNamespace(commit=commit))
    actual = await V20Service._prepare_entry_timing_commit(service, calculation)
    assert actual is commit


@pytest.mark.parametrize("trigger", ["timer", "button"])
async def test_missing_current_data_still_does_not_create_tickets_or_advisory(trigger):
    from src.web.v20_service import _NoCurrentSelectionData

    service, context = _task()
    service._orchestrate_morning_selection.side_effect = _NoCurrentSelectionData(
        "empty current data"
    )
    if trigger == "timer":
        await service._commit_entry_from_bundle(context, NOW)
    else:
        result = await service._execute_selection_task(context, NOW, request_id="button-new")
        assert result == (None, False)
    service._prepare_entry_timing_commit.assert_not_awaited()
    service._repository.commit_selection_run.assert_not_awaited()
    service._repository.seal_event.assert_not_awaited()
    assert context.last_phase == "NO_CURRENT_DATA"


@pytest.mark.parametrize(
    "status,reason",
    [
        ("WAIT", "FIXED_RULE_MATCHED"),
        ("NO_WAIT", "INDEX_NOT_GREEN"),
        ("UNAVAILABLE", "STOCK_FEATURES_UNAVAILABLE"),
    ],
)
async def test_advisory_preserves_decision_and_uses_current_request_features(
    status, reason, monkeypatch
):
    import src.strategy.v22_slim.entry_timing as timing

    service, _ = _task()
    calculation = service._orchestrate_morning_selection.return_value
    features = {
        "600001": {
            "max_drop_3m_pct": 2.0,
            "prefix_amount_yuan": 1_000_000.0,
            "last3_amount_share": 0.2,
        }
    }
    calculation.bundle = SimpleNamespace(
        entry_timing_features=features,
        trade_date=NOW.date(),
        prior_trade_date=date(2026, 9, 22),
    )
    service._clock = lambda: NOW
    service._scan_state = SimpleNamespace(realtime_client=object())
    evaluator = AsyncMock(
        return_value={
            "schema": "v22-entry-timing/v1",
            "status": status,
            "reason": reason,
        }
    )
    monkeypatch.setattr(timing, "evaluate_entry_timing", evaluator)
    actual = await V20Service._prepare_entry_timing_commit(service, calculation)
    assert {
        key: value for key, value in actual.semantic.items() if key != "entry_timing_advisory"
    } == calculation.prepared.commit.semantic
    assert actual.semantic_content_hash == sha256_json(actual.semantic)
    advisory = actual.semantic["entry_timing_advisory"]
    assert advisory["status"] == status
    assert advisory["reason"] == reason
    assert advisory["original_action"] == "ENTER"
    assert advisory["original_reason_codes"] == []
    assert evaluator.await_args.kwargs["features"] is features
    assert evaluator.await_args.kwargs["now"] == NOW


def test_fresh_canonical_features_do_not_change_frozen_artifact_contract(monkeypatch):
    from src.web.v20_v16_canonical_artifact import encode, hydrate
    from tests.unit.web.test_v20_canonical_projection_acceptance import FULL_EXCHANGE_CALENDAR
    from tests.unit.web.test_v20_service import _service
    from tests.unit.web.test_v22_canonical_artifact import v22_canonical

    service = _service(monkeypatch, object())
    canonical = v22_canonical()
    bundle = service._project_canonical_v16(canonical, calendar=FULL_EXCHANGE_CALENDAR)
    assert set(bundle.entry_timing_features) == {
        s.code for s in canonical.scan_result.recommended[:3]
    }
    for fact in bundle.entry_timing_features.values():
        assert fact == {
            "max_drop_3m_pct": 0.0,
            "prefix_amount_yuan": 10_000.0,
            "last3_amount_share": 0.3,
        }
    restored = hydrate(
        encode(
            bundle,
            calendar=FULL_EXCHANGE_CALENDAR,
            canonical_integrity_hash=canonical._integrity_hash,
        )
    ).bundle
    assert restored.snapshot == bundle.snapshot
    assert restored.snapshot_hash == bundle.snapshot_hash
