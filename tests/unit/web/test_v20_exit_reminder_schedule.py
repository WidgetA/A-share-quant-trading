from datetime import date, datetime
from unittest.mock import AsyncMock

import pytest

from src.web.v20_service import V20Service, _DayContext
from tests.unit.web.test_v20_auto_manual_exact_parity_acceptance import TZ, _service_and_artifact


@pytest.mark.parametrize("day,expected_calls", [(date(2026, 9, 12), 0), (date(2026, 9, 14), 1)])
async def test_realtime_selection_change_preserves_existing_exit_reminder_schedule(
    monkeypatch, day, expected_calls
):
    now = datetime(day.year, day.month, day.day, 10, tzinfo=TZ)
    service, repository, _ = _service_and_artifact(monkeypatch, now=now)
    service._run_reminders = V20Service._run_reminders.__get__(service)
    repository.enqueue_due_exit_reminders = AsyncMock(return_value=())
    context = _DayContext(day, (date(2026, 9, 11), date(2026, 9, 14), date(2026, 9, 15)))
    await service._run_reminders(context, now)
    assert repository.enqueue_due_exit_reminders.await_count == expected_calls
