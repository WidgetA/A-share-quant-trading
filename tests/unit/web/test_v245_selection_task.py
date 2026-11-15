"""The actual shared task applies turtle policy before persisting either trigger."""

from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.data.database.v20_repository import sha256_json
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v24_5.policy import genesis_state
from src.web.v20_service import V20Service, _DayContext
from tests.unit.strategy.test_v22_slim_entry import CALENDAR, DAY, TZ, fixture


def service_fixture(monkeypatch, opens=0):
    config, state, bundle = fixture()
    service = object.__new__(V20Service)
    service.config = replace(config, strategy_version="V24.5")
    service._clock = lambda: datetime.combine(DAY, datetime.min.time(), TZ).replace(hour=10)
    service._artifacts = load_g_artifacts(
        config.artifact_manifest_path.parent,
        expected_manifest_sha256=config.artifact_manifest_sha256,
    )
    turtle = genesis_state()
    if opens:
        turtle.update(open_streak=opens, last_trade_date=bundle.prior_trade_date.isoformat())
    service._repository = SimpleNamespace(
        get_entry_status=AsyncMock(return_value=None),
        load_state=AsyncMock(return_value=state),
        load_v245_state_before=AsyncMock(
            return_value={
                "state_before": turtle,
                "expected_revision": opens,
                "day_status": "ABSENT",
            }
        ),
        assert_runtime_leader=AsyncMock(),
        get_selection_run_event_id=AsyncMock(return_value=None),
        commit_selection_run=AsyncMock(return_value="entry"),
        seal_event=AsyncMock(
            return_value=SimpleNamespace(semantic={"entry_timing_alert_event_id": "timing"})
        ),
    )
    service._scheduled_exits_today = AsyncMock(return_value=[])
    service._policy_inputs = AsyncMock(return_value=([], [], []))
    service._resolve_canonical_morning_bundle = AsyncMock(
        return_value=(bundle, bundle.frozen_at, CALENDAR, "CURRENT", False, None)
    )
    monkeypatch.setattr(
        "src.strategy.v22_slim.runtime_inputs.build_inputs",
        AsyncMock(return_value=(bundle, [], [], [])),
    )
    service._load_trade_calendar = AsyncMock(return_value=CALENDAR)
    service._reconcile_missed_slots = AsyncMock()
    service._expire_reference_gaps = AsyncMock()
    service._process_mature_shadow = AsyncMock()
    service.kick_mews_for_selection_trigger = lambda now: None
    store = SimpleNamespace(list=AsyncMock(return_value=[]))
    service._v22_monitor = lambda: SimpleNamespace(store=store)
    service._scan_state = SimpleNamespace(realtime_client=None)
    return service


@pytest.mark.parametrize("opens,expected", [(0, "BLOCK"), (1, "BLOCK"), (2, "ENTER")])
async def test_real_calculation_uses_original_open_not_its_turtle_output(
    monkeypatch, opens, expected
):
    service = service_fixture(monkeypatch, opens)
    computation = await service._compute_morning_selection(DAY)
    semantic = computation.prepared.commit.semantic
    assert semantic["original_final_open"] is True
    assert semantic["action"] == expected
    assert semantic["v245_transition"]["state_after"]["open_streak"] == opens + 1
    assert len(semantic["reference_symbols"]) == 10
    assert len(semantic["symbols"]) == (3 if expected == "ENTER" else 0)
    assert computation.prepared.commit.model_batch is None
    assert sha256_json(semantic) == computation.prepared.commit.semantic_content_hash
    # A new calculation uses frozen prestate, so it is not a second confirmation day.
    again = await service._compute_morning_selection(DAY)
    assert again.prepared == computation.prepared


@pytest.mark.parametrize("trigger", ["timer", "button"])
async def test_shared_task_saves_bank_plan_and_both_messages(monkeypatch, trigger):
    service = service_fixture(monkeypatch)
    context = _DayContext(trade_date=DAY, calendar=CALENDAR)
    if trigger == "timer":
        await service._commit_entry_from_bundle(context, service._aware_now())
    else:
        await service._execute_selection_task(
            context, service._aware_now(), request_id="new-button"
        )
    commit = service._repository.commit_selection_run.await_args.args[0]
    assert commit.action == "BLOCK"
    assert commit.semantic["v245_switch_plan"]["target"] == "BANK"
    assert commit.semantic["entry_timing_advisory"]["status"] == "NO_WAIT"
    assert [c.args[0] for c in service._repository.seal_event.await_args_list] == [
        "entry",
        "timing",
    ]
    service._repository.get_selection_run_event_id.return_value = "entry"
    await service._execute_selection_task(context, service._aware_now(), request_id="same-request")
    assert service._repository.commit_selection_run.await_count == 1
    assert service._repository.load_v245_state_before.await_count == 1


@pytest.mark.parametrize("opens", [0, 2])
async def test_real_v245_commit_survives_production_message_sealing(monkeypatch, opens):
    from src.common.v20_feishu import seal_v20_payload
    from src.data.database.v20_repository import OutboxRecord, V20Repository
    from src.data.database.v245_switch_positions import build_switch_plan

    service = service_fixture(monkeypatch, opens)
    calculation = await service._compute_morning_selection(DAY)
    monkeypatch.setattr(
        "src.strategy.v22_slim.entry_timing.evaluate_entry_timing",
        AsyncMock(
            return_value={
                "schema": "v22-entry-timing/v1",
                "status": "NO_WAIT",
                "reason": "NO_STOCK_RULE_MATCH",
            }
        ),
    )
    commit = await service._prepare_entry_timing_commit(calculation)
    semantic = {
        **commit.semantic,
        "v245_switch_plan": build_switch_plan([], day=DAY, calendar=CALENDAR, permit=opens == 2),
    }
    commit = replace(commit, semantic=semantic, semantic_content_hash=sha256_json(semantic))
    V20Repository._validate_entry_commit(commit)
    V20Repository._v245_transition_payload(commit)
    record = OutboxRecord(
        event_id=commit.event_id,
        event_type="ENTRY_DECISION",
        route_id=commit.route_id,
        official_stream_id=commit.official_stream_id,
        lineage_id=commit.lineage_id,
        semantic=semantic,
        semantic_content_hash=commit.semantic_content_hash,
        payload=None,
        payload_hash=None,
        generated_at=None,
        commit_marker=None,
        action_expiry_ts=commit.action_expiry_ts,
        delivery_status="PENDING",
        attempt_count=0,
    )
    payload = seal_v20_payload(record, service._aware_now(), 1, True)
    assert "V24.5" in payload["message"] and "工商银行" in payload["message"]
