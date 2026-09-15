from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from src.web.v20_routes import create_v20_router


async def test_user_can_report_legacy_stock_sold_without_an_exit_signal(monkeypatch):
    monkeypatch.setenv("V20_INGEST_API_KEY", "test-key")
    service = SimpleNamespace(
        list_legacy_positions=AsyncMock(return_value=[{"position_id": "old-1", "revision": 0}]),
        calibrate_legacy_position=AsyncMock(return_value={"status": "CLOSED", "quantity": 0}),
    )
    app = FastAPI()
    app.state.v20_service = service
    app.include_router(create_v20_router())
    headers = {"X-V20-API-Key": "test-key", "Idempotency-Key": "sold-20260915-001"}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        result = await client.post(
            "/api/v20/legacy-positions/old-1/calibrate",
            headers=headers,
            json={"expected_revision": 0, "quantity": 0},
        )
        assert result.status_code == 200, result.text
        assert result.json() == {"status": "CLOSED", "quantity": 0}
        assert (await client.get("/api/v20/legacy-positions")).status_code == 401
        assert (await client.get("/api/v20/legacy-positions", headers=headers)).json() == [
            {"position_id": "old-1", "revision": 0}
        ]
    service.calibrate_legacy_position.assert_awaited_once_with(
        "old-1", "sold-20260915-001", {"expected_revision": 0, "quantity": 0}
    )


@pytest.mark.parametrize(
    "change",
    [
        {},
        {"quantity": -1},
        {"quantity": 1.5},
        {"status": "SOLD"},
        {"sold_price": 1},
        {"quantity": None},
        {"quantity": 3, "status": "CLOSED"},
        {"quantity": 0, "status": "MONITORING"},
    ],
)
async def test_legacy_sync_rejects_invalid_or_inconsistent_corrections(monkeypatch, change):
    monkeypatch.setenv("V20_INGEST_API_KEY", "test-key")
    app = FastAPI()
    service = SimpleNamespace(calibrate_legacy_position=AsyncMock())
    app.state.v20_service = service
    app.include_router(create_v20_router())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v20/legacy-positions/old-1/calibrate",
            headers={"X-V20-API-Key": "test-key", "Idempotency-Key": "invalid-001"},
            json={"expected_revision": 0, **change},
        )
    assert response.status_code == 422
    service.calibrate_legacy_position.assert_not_awaited()


async def test_new_full_task_refreshes_sold_list_when_today_already_has_a_result(monkeypatch):
    from src.web.v20_routes import _dispatch_manual_trigger
    from tests.unit.web.test_v20_auto_manual_exact_parity_acceptance import _service_and_artifact
    from tests.unit.web.test_v20_morning_selection_unified_contract import (
        AT_CUTOFF,
        JUST_BEFORE_CUTOFF,
        _drain_mews_kicks,
    )

    now = [JUST_BEFORE_CUTOFF]
    service, repository, _ = _service_and_artifact(monkeypatch, now=now[0])
    service._clock = lambda: now[0]
    sold = {
        "model_leg_id": "sold-old-recommendation",
        "code": "603052",
        "stock_name": "可川科技",
        "signal_date": "2026-08-28",
        "rank": 1,
        "relative_weight": 0.05,
        "plan_time": "14:57",
    }
    schedules = AsyncMock(return_value=[sold])
    monkeypatch.setattr(service, "_scheduled_exits_today", schedules)
    await service._run_decision_iteration_with_cutoff(now[0])
    original = repository.status
    state = repository.state
    assert original.semantic["scheduled_exits_today"] == [sold]
    schedules.return_value = []  # User closed the position after the first daily message.
    now[0] = AT_CUTOFF
    repository.seal_at = AT_CUTOFF
    result = await _dispatch_manual_trigger(service, "sold-sync-after-existing-result")
    await _drain_mews_kicks(service)
    event = repository.alerts[result["entry_event_id"]]
    assert event.semantic["scheduled_exits_today"] == []
    assert "推荐日期=2026-08-28" not in event.payload["message"]
    assert repository.status == original and repository.state == state
    assert schedules.await_count == 2
