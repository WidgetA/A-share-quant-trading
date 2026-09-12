"""User requirements from 2026-09-12; never derive expectations from old wrappers.

Keep the real route, scheduler, calculator, persistence boundary and renderer.
The fixture substitutes market facts and the database, not the task behavior.
Real PostgreSQL coverage is a separate required release check.
"""

from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import src.web.v20_service as service_module
from src.common.v20_feishu import _render_entry_strategy_body, render_exit_message
from src.data.clients.tushare_realtime import TushareDailyBar, TushareRealtimeClient
from src.web.v20_routes import _dispatch_manual_trigger
from src.web.v20_service import V20Service
from tests.unit.web.test_v20_auto_manual_exact_parity_acceptance import (
    POST_CUTOFF_AT,
    RUN_AT,
    TZ,
    _service_and_artifact,
)


@pytest.mark.parametrize("day", [date(2026, 9, 12), date(2026, 9, 14)])
async def test_empty_current_realtime_returns_no_tickets_for_both_triggers(monkeypatch, day):
    """The provider is called on Saturday too; no calendar-based shortcut or Friday replay."""
    now = datetime.combine(day, datetime.min.time(), tzinfo=TZ).replace(hour=10)
    service, repository, _ = _service_and_artifact(monkeypatch, now=now)
    calendar = [date(2026, 9, 11), date(2026, 9, 14), date(2026, 9, 15), date(2026, 9, 16)]
    service._calendar_provider = AsyncMock(return_value=calendar)
    service._canonical_artifact_store = None
    service._mews_cached_for = day
    service._scan_state.historical_adapter = SimpleNamespace()
    client = TushareRealtimeClient("test-only")
    client._client = SimpleNamespace()
    client._api_call = AsyncMock(return_value={"data": {"fields": [], "items": []}})
    codes = ("000001", "600001")
    client.fetch_daily_bars = AsyncMock(
        return_value={code: TushareDailyBar(code, "20260911", 10.0, 100000.0) for code in codes}
    )
    service._scan_state.realtime_client = client
    monkeypatch.setattr(
        service_module,
        "derive_canonical_v16_universe",
        lambda state, **kwargs: (
            None,
            None,
            {"board": tuple((code, code) for code in codes)},
            codes,
        ),
    )
    service._compute_canonical_v16_from_persisted_raw = (
        V20Service._compute_canonical_v16_from_persisted_raw.__get__(service)
    )
    repository.get_entry_status = AsyncMock(return_value=None)
    repository.list_raw_minute_bar_records = AsyncMock(return_value=[])

    async def daily_snapshot(day, payload):
        return SimpleNamespace(payload=payload)

    repository.record_daily_bar_snapshot = daily_snapshot
    repository.commit_selection_run = AsyncMock(
        side_effect=AssertionError("empty input wrote tickets")
    )
    repository.seal_event = AsyncMock(side_effect=AssertionError("empty input sealed tickets"))
    repository.enqueue_alert = AsyncMock(side_effect=AssertionError("empty input generated a push"))
    await service._run_decision_iteration_with_cutoff(now)
    assert client._api_call.await_count == len(codes), "timer must fetch current realtime"
    assert service._context.last_phase == "NO_CURRENT_DATA"
    result = await _dispatch_manual_trigger(service, f"current-data-{day.isoformat()}")
    assert result["trade_date"] == day.isoformat()
    assert result["cycle_result"] == "NO_CURRENT_DATA"
    assert result["symbols"] == result["reference_symbols"] == []
    assert result["entry_event_id"] is None and result["created"] is False
    assert result["delivery_status"] == "NOT_REQUIRED"
    assert result["calculation_result"] == "NOT_RUN"
    # Both requests share the existing provider-minute acquisition, even empty data.
    assert client._api_call.await_count == len(codes)
    assert all(call.args[0] == "rt_min_daily" for call in client._api_call.await_args_list)
    assert all("start_date" not in call.args[1] for call in client._api_call.await_args_list)
    assert {call.args[1]["ts_code"] for call in client._api_call.await_args_list} == {
        "000001.SZ",
        "600001.SH",
    }
    repository.commit_selection_run.assert_not_awaited()
    repository.seal_event.assert_not_awaited()
    repository.enqueue_alert.assert_not_awaited()
    assert not repository.selection_runs and repository.state.revision == 0


@pytest.mark.parametrize("hour", [9, 14, 19])
async def test_new_button_click_after_daily_run_recomputes_and_publishes_normal_result(
    monkeypatch, hour
):
    service, repository, _ = _service_and_artifact(monkeypatch)
    calls = []
    original = service._compute_morning_selection

    async def calculate(*args, **kwargs):
        calls.append((args, kwargs))
        return await original(*args, **kwargs)

    monkeypatch.setattr(service, "_compute_morning_selection", calculate)
    await service._run_decision_iteration_with_cutoff(RUN_AT)
    assert repository.outbox is not None
    first = repository.outbox
    assert first.payload is not None
    assert len(calls) == 1

    rerun_at = POST_CUTOFF_AT.replace(hour=hour, minute=45)
    service._clock = lambda: rerun_at
    repository.seal_at = rerun_at
    result = await _dispatch_manual_trigger(service, f"contract-rerun-{hour}-0001")
    event_id = result.get("entry_event_id") or result.get("operator_event_id")
    record = await repository.get_outbox_event(
        event_id,
        route_id=service.config.route_id,
        official_stream_id=service.config.official_stream_id,
        lineage_id=service.config.state_lineage_id,
    )
    assert len(calls) == 2, "a new button click must actually recompute"
    assert record is not None and record.event_id != first.event_id
    assert record.event_type == first.event_type == "ENTRY_DECISION", (
        "the button must persist the normal selection result, not an operator/check-only alert"
    )
    assert record.semantic["action"] == first.semantic["action"]
    assert record.semantic["symbols"] == first.semantic["symbols"]
    assert record.payload is not None
    assert "手工触发" not in record.payload["message"]
    assert "仅核查" not in record.payload["message"]
    assert record.payload["message"].splitlines()[0] == first.payload["message"].splitlines()[0]
    assert _render_entry_strategy_body(record.semantic) == _render_entry_strategy_body(
        first.semantic
    )
    assert result["task_success"] is False, "PENDING delivery must never mean task success"
    assert repository.state.revision == 1
    retry = await _dispatch_manual_trigger(service, f"contract-rerun-{hour}-0001")
    assert retry["entry_event_id"] == record.event_id and retry["created"] is False
    assert len(calls) == 2 and repository.state.revision == 1


@pytest.mark.parametrize("stage", ["calculation", "persistence", "sealing"])
async def test_failed_stage_cannot_be_reported_as_success(monkeypatch, stage):
    service, repository, _ = _service_and_artifact(monkeypatch)
    await service._run_decision_iteration_with_cutoff(RUN_AT)
    initial_state = repository.state

    async def fail(*_args, **_kwargs):
        raise RuntimeError(f"injected {stage} failure")

    owner, method = {
        "calculation": (service, "_compute_morning_selection"),
        "persistence": (repository, "commit_selection_run"),
        "sealing": (repository, "seal_event"),
    }[stage]
    monkeypatch.setattr(owner, method, fail)
    with pytest.raises(RuntimeError, match=f"injected {stage} failure"):
        await _dispatch_manual_trigger(service, f"contract-{stage}-failed")
    assert repository.state == initial_state
    assert all(record.delivery_status != "SENT" for record in repository.alerts.values())


def test_exit_instruction_uses_stock_and_recommendation_date_in_plain_language():
    message = render_exit_message(
        {
            "event_id": "example-exit",
            "deployment_mode": "forward_shadow",
            "exit_signal_type": "PLAN_1457",
            "code": "600345",
            "stock_name": "长江通信",
            "signal_date": "2026-09-09",
            "rank": 7,
            "model_leg_id": "opaque-internal-model-id",
            "reference_entry_price": None,
            "observed_close": None,
            "wealth_factor": None,
            "origin_final_relative_weight": 0.05,
            "rule_actionable_from": "2026-09-11T14:57:00+08:00",
            "reason_codes": [],
        },
        generated_at=datetime(2026, 9, 11, 14, 57, 1, tzinfo=TZ),
        commit_marker=1,
    )
    assert "长江通信" in message and "2026-09-09" in message
    assert "卖出" in message
    for internal in ("模型腿", "整腿", "监控腿", "D0=", "rank=", "opaque-internal"):
        assert internal not in message
