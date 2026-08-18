from __future__ import annotations

import asyncio
from datetime import time
from types import SimpleNamespace

import pytest

from src.data.services import margin_risk_scheduler as scheduler_module
from src.data.services.margin_risk_scheduler import (
    MarginRiskRefreshScheduler,
    _is_caught_up,
)
from src.margin_risk.publication import MARGIN_PUBLISH_TIME


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

    result = await scheduler._refresh_once(trigger="09:15", max_days=5)

    assert result == {"status": "OK", "filled": 0, "remaining": 0}
    assert service.max_days_calls == [5]


def test_refresh_scheduled_after_the_upstream_publication_time() -> None:
    # Upstream publishes the previous session at 09:10; the refresh must sit
    # after it, not race it.
    assert scheduler_module._RUN_AT > MARGIN_PUBLISH_TIME
    assert scheduler_module._RUN_AT == time(9, 15)


def test_caught_up_means_the_published_day_is_stored_not_merely_no_error() -> None:
    published = {"status": "OK", "published_through": "2026-08-10"}
    assert not _is_caught_up({**published, "latest_complete": "2026-08-07"})
    assert not _is_caught_up({**published, "latest_complete": None})
    assert _is_caught_up({**published, "latest_complete": "2026-08-10"})
    # A PARTIAL run that still stored the newest published day is caught up.
    assert _is_caught_up(
        {"status": "PARTIAL", "published_through": "2026-08-10", "latest_complete": "2026-08-10"}
    )
    assert not _is_caught_up({"status": "ERROR", "message": "boom"})
    assert not _is_caught_up(None)
    # Nothing published yet → no retry can help.
    assert _is_caught_up({"status": "OK", "published_through": None, "latest_complete": None})


@pytest.mark.asyncio
async def test_late_publication_is_retried_until_the_newest_day_lands(monkeypatch) -> None:
    class _LateService:
        def __init__(self) -> None:
            self.calls = 0

        async def audit_and_fill(self, *, max_days: int | None = None) -> dict:
            self.calls += 1
            latest = "2026-08-10" if self.calls >= 3 else "2026-08-07"
            return {
                "status": "OK",
                "published_through": "2026-08-10",
                "latest_complete": latest,
            }

    service = _LateService()
    state = SimpleNamespace(margin_risk_service=service, cache_fill_running=False)
    scheduler = MarginRiskRefreshScheduler(state)
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(scheduler_module.asyncio, "sleep", fake_sleep)

    result = await scheduler._refresh_until_published_day_is_stored(trigger="09:15")

    assert service.calls == 3
    assert sleeps == [scheduler_module._RETRY_INTERVAL_SECONDS] * 2
    assert result["latest_complete"] == "2026-08-10"


@pytest.mark.asyncio
async def test_retries_are_bounded_when_upstream_never_publishes(monkeypatch) -> None:
    class _NeverService:
        def __init__(self) -> None:
            self.calls = 0

        async def audit_and_fill(self, *, max_days: int | None = None) -> dict:
            self.calls += 1
            return {
                "status": "OK",
                "published_through": "2026-08-10",
                "latest_complete": "2026-08-07",
            }

    service = _NeverService()
    state = SimpleNamespace(margin_risk_service=service, cache_fill_running=False)
    scheduler = MarginRiskRefreshScheduler(state)

    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(scheduler_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(scheduler_module, "_MAX_RETRIES", 2)

    result = await scheduler._refresh_until_published_day_is_stored(trigger="09:15")

    # Hand back to the next scheduled pass instead of hammering upstream forever.
    assert service.calls == 3
    assert result["latest_complete"] == "2026-08-07"


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
