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
