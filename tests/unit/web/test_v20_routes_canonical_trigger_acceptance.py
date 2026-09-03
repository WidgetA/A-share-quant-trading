from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.web import v20_routes as routes
from src.web.v20_routes import _dispatch_manual_trigger
from tests.unit.web.test_v20_routes_post_cutoff_terminal_acceptance import (
    TODAY,
    Repository,
    Service,
    _source,
)


def _assert_no_alternate_probe() -> None:
    assert not hasattr(routes, "_run_fresh_0939_probe")
    assert not hasattr(routes, "_select_fresh_probe_context")


class CanonicalRepository(Repository):
    def __init__(self, status: Any, source: Any) -> None:
        super().__init__(status, source)
        self.operator_events: list[dict[str, Any]] = []
        self.operator_enqueues = 0

    async def enqueue_operator_event(
        self,
        event_id: str,
        semantic: dict[str, Any],
    ) -> bool:
        self.operator_enqueues += 1
        if self.operator_events:
            assert event_id == self.operator_events[0]["event_id"]
            assert semantic == self.operator_events[0]["semantic"]
            return False
        self.operator_events.append({"event_id": event_id, "semantic": dict(semantic)})
        return True


class CanonicalCheckService(Service):
    def __init__(self, repository: CanonicalRepository) -> None:
        super().__init__(repository)
        self.canonical_lock = asyncio.Lock()
        self.hook_calls: list[tuple[str, Any]] = []
        self.mews_started = asyncio.Event()
        self.mews_tasks: list[asyncio.Task[None]] = []
        self.mews_calculation_calls = 0

    async def _settle_mews(self) -> None:
        self.mews_calculation_calls += 1
        await asyncio.sleep(0)

    def kick_mews_for_selection_trigger(self, now: Any) -> asyncio.Task[None]:
        self.mews_kick_calls += 1
        self.mews_started.set()
        task = asyncio.create_task(self._settle_mews())
        self.mews_tasks.append(task)
        return task

    async def trigger_canonical_selection_check_only(
        self,
        request_id: str,
        now: Any,
    ) -> dict[str, Any]:
        self.hook_calls.append((request_id, now))
        assert self.mews_started.is_set()
        created = False
        async with self.canonical_lock:
            status = await self._repository.get_entry_status(
                self.config.official_stream_id,
                now.date(),
            )
            if not self._repository.operator_events:
                created = await self._repository.enqueue_operator_event(
                    "canonical-check-" + request_id,
                    {
                        "manual_request_id": request_id,
                        "official_state_changed": False,
                        "orders_changed": False,
                        "non_actionable": True,
                    },
                )
        event = self._repository.operator_events[0]
        return {
            "accepted": True,
            "created": created,
            "manual_request_id": request_id,
            "operator_event_id": event["event_id"],
            "event_trade_date": TODAY.isoformat(),
            "official_entry_action": status.action if status is not None else "MISSING",
            "official_entry_event_id": status.event_id if status is not None else None,
            "official_state_changed": False,
            "orders_changed": False,
            "non_actionable": True,
            "retrospective_expired": True,
        }


@pytest.mark.asyncio
async def test_prewarm_manual_trigger_still_runs_morning_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status, source = _source("ENTER")
    service = Service(Repository(status, source))
    service.now = service.now.replace(hour=9, minute=30)

    async def allow_mews_for_prewarm(_now: Any) -> bool:
        service.mews_calculation_calls += 1
        return False

    service.ensure_mews_for_selection_trigger = allow_mews_for_prewarm
    _assert_no_alternate_probe()

    with pytest.raises(AssertionError, match="post-cutoff must not run morning selection"):
        await asyncio.wait_for(
            _dispatch_manual_trigger(service, "prewarm-morning-001"),
            timeout=2.0,
        )

    assert service.mews_kick_calls == 1
    await asyncio.wait_for(
        asyncio.gather(*service.mews_kick_tasks, return_exceptions=True),
        timeout=2.0,
    )
    assert service.mews_calculation_calls == 1
    assert service.morning_calls == 1
    assert service.calendar_calls == 0


@pytest.mark.asyncio
async def test_post_cutoff_today_terminal_uses_current_canonical_check_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status, source = _source("NO_SIGNAL")
    repository = CanonicalRepository(status, source)
    service = CanonicalCheckService(repository)
    _assert_no_alternate_probe()

    result = await asyncio.wait_for(
        _dispatch_manual_trigger(service, "canonical-terminal-001"),
        timeout=2.0,
    )

    assert result["official_state_changed"] is False
    assert result["orders_changed"] is False
    assert result["non_actionable"] is True
    assert result["retrospective_expired"] is True
    assert result["official_entry_action"] == "NO_SIGNAL"
    assert result["official_entry_event_id"] == status.event_id
    assert len(service.hook_calls) == 1
    assert service.mews_kick_calls == 1
    assert service.mews_calculation_calls == 1
    assert service.calendar_calls == 0
    assert service.morning_calls == 0
    assert repository.official_writes == 0


@pytest.mark.asyncio
async def test_before_prewarm_never_enters_today_terminal_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status, source = _source("ENTER")
    service = Service(Repository(status, source))
    service.now = service.now.replace(hour=9)

    async def terminal_bomb(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("terminal lookup is post-cutoff only")

    async def allow_mews(_now: Any) -> bool:
        service.mews_calculation_calls += 1
        return False

    async def pending_morning(request_id: str) -> dict[str, Any]:
        service.morning_calls += 1
        return {
            "manual_request_id": request_id,
            "cycle_result": "BEFORE_WINDOW",
            "formal_decision_available": False,
        }

    service.ensure_mews_for_selection_trigger = allow_mews
    service.trigger_morning_selection = pending_morning
    monkeypatch.setattr(routes, "_today_terminal_entry", terminal_bomb)
    _assert_no_alternate_probe()

    result = await asyncio.wait_for(
        _dispatch_manual_trigger(service, "prewarm-terminal-gate-001"),
        timeout=2.0,
    )

    assert result["manual_request_id"] == "prewarm-terminal-gate-001"
    assert result["cycle_result"] == "BEFORE_WINDOW"
    assert service.morning_calls == 1
    assert service.mews_kick_calls == 1
    await asyncio.wait_for(
        asyncio.gather(*service.mews_kick_tasks, return_exceptions=True),
        timeout=2.0,
    )
    assert service.mews_calculation_calls == 1
    assert service.calendar_calls == 0


@pytest.mark.asyncio
async def test_post_cutoff_terminal_miss_uses_canonical_hook_and_one_durable_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status, source = _source("INPUT_INVALID")
    repository = CanonicalRepository(status, source)
    repository.status_by_date.clear()
    service = CanonicalCheckService(repository)
    _assert_no_alternate_probe()
    tasks = [
        asyncio.create_task(_dispatch_manual_trigger(service, "canonical-check-same-key"))
        for _ in range(2)
    ]
    try:
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)
        assert len(results) == 2
        assert sorted(result["created"] for result in results) == [False, True]
        assert {result["operator_event_id"] for result in results} == {
            "canonical-check-canonical-check-same-key"
        }
        assert all(result["non_actionable"] is True for result in results)
        assert all(result["retrospective_expired"] is True for result in results)
        assert all(result["official_state_changed"] is False for result in results)
        assert all(result["orders_changed"] is False for result in results)
        assert len(service.hook_calls) == 2
        assert all(call[0] == "canonical-check-same-key" for call in service.hook_calls)
        assert service.mews_kick_calls == 2
        assert service.mews_calculation_calls == 2
        assert repository.operator_enqueues == 1
        assert len(repository.operator_events) == 1
        assert repository.official_writes == 0
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*service.mews_tasks, return_exceptions=True)


class _SerializationConflict(RuntimeError):
    sqlstate = "40001"


@pytest.mark.asyncio
async def test_post_cutoff_same_key_serialization_conflict_retries_check_only_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status, source = _source("NO_SIGNAL")
    repository = CanonicalRepository(status, source)
    service = CanonicalCheckService(repository)
    _assert_no_alternate_probe()
    original = service.trigger_canonical_selection_check_only
    attempts = 0

    async def conflict_then_converge(request_id: str, now: Any) -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise _SerializationConflict("could not serialize access due to concurrent update")
        return await original(request_id, now)

    service.trigger_canonical_selection_check_only = conflict_then_converge

    result = await _dispatch_manual_trigger(service, "canonical-serialization-001")

    assert result["accepted"] is True
    assert result["created"] is True
    assert result["official_state_changed"] is False
    assert result["orders_changed"] is False
    assert attempts == 3
    assert repository.operator_enqueues == 1
    assert repository.official_writes == 0
    await asyncio.gather(*service.mews_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_post_cutoff_non_serialization_failure_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status, source = _source("NO_SIGNAL")
    repository = CanonicalRepository(status, source)
    service = CanonicalCheckService(repository)
    _assert_no_alternate_probe()
    attempts = 0

    async def fail_once(_request_id: str, _now: Any) -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("not a PostgreSQL serialization conflict")

    service.trigger_canonical_selection_check_only = fail_once

    with pytest.raises(RuntimeError, match="not a PostgreSQL serialization conflict"):
        await _dispatch_manual_trigger(service, "canonical-nonserialization-001")

    assert attempts == 1
    assert repository.operator_enqueues == 0
    assert repository.official_writes == 0
    await asyncio.gather(*service.mews_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_post_cutoff_serialization_retry_is_finite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status, source = _source("NO_SIGNAL")
    repository = CanonicalRepository(status, source)
    service = CanonicalCheckService(repository)
    _assert_no_alternate_probe()
    attempts = 0

    async def always_conflict(_request_id: str, _now: Any) -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        raise _SerializationConflict("persistent serialization conflict")

    service.trigger_canonical_selection_check_only = always_conflict

    with pytest.raises(_SerializationConflict, match="persistent serialization conflict"):
        await _dispatch_manual_trigger(service, "canonical-serialization-exhausted-001")

    assert attempts == routes._POST_CUTOFF_SERIALIZATION_RETRY_LIMIT
    assert repository.operator_enqueues == 0
    assert repository.official_writes == 0
    await asyncio.gather(*service.mews_tasks, return_exceptions=True)
