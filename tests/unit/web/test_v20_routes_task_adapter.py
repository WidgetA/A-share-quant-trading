"""Replace the rejected 'post-cutoff must not run morning task' contract."""

from datetime import datetime
from types import SimpleNamespace

import pytest

from src.web.v20_routes import _dispatch_manual_trigger


@pytest.mark.parametrize("hour,minute", [(8, 0), (9, 38), (9, 39), (9, 40), (14, 0), (19, 0)])
@pytest.mark.parametrize("daily_action", [None, "ENTER", "BLOCK", "NO_SIGNAL", "INPUT_INVALID"])
async def test_route_always_invokes_complete_task_and_does_not_choose_a_mode(
    hour, minute, daily_action
):
    calls = []
    expected = {"accepted": True, "task_success": False, "delivery_status": "PENDING"}

    class Service:
        now = datetime(2026, 9, 11, hour, minute)
        status = SimpleNamespace(action=daily_action)

        async def trigger_morning_selection(self, request_id):
            calls.append(request_id)
            return expected

        def __getattr__(self, name):
            raise AssertionError(f"HTTP adapter must not select a separate task: {name}")

    result = await _dispatch_manual_trigger(Service(), "one-complete-task-001")
    assert result is expected and calls == ["one-complete-task-001"]


@pytest.mark.parametrize("error", [ValueError("bad input"), RuntimeError("storage failed")])
async def test_route_does_not_convert_task_failure_into_success(error):
    class Service:
        async def trigger_morning_selection(self, _request_id):
            raise error

    with pytest.raises(type(error), match=str(error)):
        await _dispatch_manual_trigger(Service(), "one-failed-task-001")


@pytest.mark.parametrize("kind", ["empty", "coverage", "timeout", "calculation", "storage"])
def test_selection_http_failures_are_explicit_errors(kind):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.data.database.v20_repository import V20RepositoryError
    from src.web.v20_canonical_selection import CanonicalV16ScanError
    from src.web.v20_routes import create_v20_router
    from src.web.v20_service import _NoCurrentSelectionData

    errors = {
        "empty": _NoCurrentSelectionData("empty current realtime"),
        "coverage": CanonicalV16ScanError(
            "coverage failed", notify_title="行情覆盖不足", notify_detail="缺少必要行情"
        ),
        "timeout": TimeoutError("provider timed out"),
        "calculation": RuntimeError("scorer failed"),
        "storage": V20RepositoryError("database unavailable"),
    }

    class Service:
        async def trigger_morning_selection(self, _request_id):
            raise errors[kind]

    app = FastAPI()
    app.state.v20_service = Service()
    app.include_router(create_v20_router())
    with TestClient(app) as client:
        response = client.post(
            "/api/v20/trigger-scan", headers={"Idempotency-Key": "failure-business-001"}
        )
    assert response.status_code == 503
    body = response.json()
    assert body["cycle_result"] == body["entry_action"] == body["calculation_result"] == "ERROR"
    assert body["task_success"] is False and body["accepted"] is False
    assert body["detail"] and "NO_SIGNAL" not in body["detail"]
    if kind == "empty":
        assert "实时" in body["detail"] and "数据" in body["detail"]
    elif kind == "coverage":
        assert "行情覆盖不足" in body["detail"]


@pytest.mark.parametrize("action", ["NO_SIGNAL", "BLOCK", "ENTER"])
def test_selection_http_keeps_normal_business_results(action):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.web.v20_routes import create_v20_router

    expected = {"accepted": True, "entry_action": action, "calculation_result": "SUCCESS"}

    class Service:
        async def trigger_morning_selection(self, _request_id):
            return expected

    app = FastAPI()
    app.state.v20_service = Service()
    app.include_router(create_v20_router())
    with TestClient(app) as client:
        response = client.post("/api/v20/trigger-scan")
    assert response.status_code == 202 and response.json() == expected
