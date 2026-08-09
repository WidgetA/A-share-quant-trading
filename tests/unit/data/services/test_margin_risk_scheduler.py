from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from src.data.services import margin_risk_scheduler as scheduler_module
from src.data.services.margin_risk_scheduler import MarginRiskRefreshScheduler


class _MarginRiskService:
    def __init__(self) -> None:
        self.max_days_calls: list[int | None] = []

    async def audit_and_fill(self, *, max_days: int | None = None) -> dict:
        self.max_days_calls.append(max_days)
        return {"status": "OK", "filled": 0, "remaining": 0}


@pytest.mark.asyncio
async def test_scheduler_bootstraps_full_history_before_waiting_for_daily_run(
    monkeypatch,
) -> None:
    service = _MarginRiskService()
    state = SimpleNamespace(margin_risk_service=service, cache_fill_running=False)
    scheduler = MarginRiskRefreshScheduler(state)
    sleeps = 0

    async def fake_sleep(_seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps > 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(scheduler_module, "_STARTUP_DELAY_SECONDS", 0)
    monkeypatch.setattr(scheduler_module.asyncio, "sleep", fake_sleep)

    await scheduler.run()

    assert service.max_days_calls == [None]
    assert scheduler.last_result == {"status": "OK", "filled": 0, "remaining": 0}


@pytest.mark.asyncio
async def test_scheduled_refresh_remains_bounded() -> None:
    service = _MarginRiskService()
    state = SimpleNamespace(margin_risk_service=service, cache_fill_running=False)
    scheduler = MarginRiskRefreshScheduler(state)

    result = await scheduler._refresh_once(trigger="08:50", max_days=5)

    assert result == {"status": "OK", "filled": 0, "remaining": 0}
    assert service.max_days_calls == [5]


@pytest.mark.asyncio
async def test_startup_backfill_waits_until_reconnected_service_is_available(
    monkeypatch,
) -> None:
    service = _MarginRiskService()
    state = SimpleNamespace(margin_risk_service=None, cache_fill_running=False)
    scheduler = MarginRiskRefreshScheduler(state)
    sleeps = 0

    async def fake_sleep(_seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps == 2:
            state.margin_risk_service = service

    monkeypatch.setattr(scheduler_module.asyncio, "sleep", fake_sleep)

    await scheduler._bootstrap_history()

    assert sleeps == 2
    assert service.max_days_calls == [None]


@pytest.mark.asyncio
async def test_startup_backfill_retries_after_refresh_error(monkeypatch) -> None:
    class _FlakyService(_MarginRiskService):
        async def audit_and_fill(self, *, max_days: int | None = None) -> dict:
            self.max_days_calls.append(max_days)
            if len(self.max_days_calls) == 1:
                raise RuntimeError("temporary Greptime read limit")
            return {"status": "OK", "filled": 0, "remaining": 0}

    service = _FlakyService()
    state = SimpleNamespace(margin_risk_service=service, cache_fill_running=False)
    scheduler = MarginRiskRefreshScheduler(state)
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(scheduler_module, "_STARTUP_DELAY_SECONDS", 0)
    monkeypatch.setattr(scheduler_module, "_STARTUP_ERROR_RETRY_SECONDS", 1)
    monkeypatch.setattr(scheduler_module.asyncio, "sleep", fake_sleep)

    await scheduler._bootstrap_history()

    assert service.max_days_calls == [None, None]
    assert sleeps == [0, 1]
    assert scheduler.last_result == {"status": "OK", "filled": 0, "remaining": 0}
