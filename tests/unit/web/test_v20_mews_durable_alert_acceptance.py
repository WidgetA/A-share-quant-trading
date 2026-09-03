from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import date, datetime
from typing import Any

import pytest

from src.data.clients.mews_snapshot import MewsSnapshotSourceError
from src.data.database.v20_repository import (
    OutboxRecord,
    V20LeadershipLost,
    V20SemanticConflict,
    sha256_json,
)
from tests.unit.web.test_v20_service import (
    TZ,
    _AfterCutoffMewsRepository,
    _late_mews_payload,
    _service,
)


def _mews_repository() -> _AfterCutoffMewsRepository:
    repository = _AfterCutoffMewsRepository()

    async def assert_runtime_leader() -> None:
        return None

    repository.assert_runtime_leader = assert_runtime_leader
    repository.events: dict[str, Any] = {}
    repository.enqueue_calls = 0
    repository.seal_calls = 0

    async def enqueue_alert(
        event_id: str,
        route_id: str,
        semantic: dict[str, Any],
        semantic_hash: str,
        **kwargs: Any,
    ) -> bool:
        repository.enqueue_calls += 1
        if event_id in repository.events:
            return False
        repository.events[event_id] = OutboxRecord(
            event_id=event_id,
            event_type="DATA_ALERT",
            route_id=route_id,
            semantic=dict(semantic),
            semantic_content_hash=semantic_hash,
            payload=None,
            payload_hash=None,
            generated_at=None,
            commit_marker=None,
            action_expiry_ts=None,
            delivery_status="PENDING",
            attempt_count=0,
            **kwargs,
        )
        return True

    async def seal_event(event_id: str, builder: Any) -> OutboxRecord:
        repository.seal_calls += 1
        current = repository.events[event_id]
        generated_at = datetime(2026, 9, 1, 14, 5, tzinfo=TZ)
        payload = dict(builder(current, generated_at, 91, True))
        sealed = replace(
            current,
            payload=payload,
            payload_hash=sha256_json(payload),
            generated_at=generated_at,
            commit_marker=91,
        )
        repository.events[event_id] = sealed
        return sealed

    repository.enqueue_alert = enqueue_alert
    repository.seal_event = seal_event

    async def close() -> None:
        return None

    repository.close = close
    return repository


def _start_mews_service(
    monkeypatch: pytest.MonkeyPatch,
    repository: _AfterCutoffMewsRepository,
    source: Any,
) -> Any:
    service = _service(monkeypatch, repository)
    service._repository_started = True
    service._mews_source = source
    monkeypatch.setattr(
        service,
        "_load_trade_calendar",
        lambda _day: asyncio.sleep(
            0, result=(date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2))
        ),
    )
    return service


class _FailingSource:
    def __init__(self, details: list[str]) -> None:
        self.details = details
        self.calls = 0

    async def fetch_snapshot(self, **_kwargs: Any) -> dict[str, Any]:
        detail = self.details[min(self.calls, len(self.details) - 1)]
        self.calls += 1
        raise MewsSnapshotSourceError(detail)


class _StrictScopedMewsRepository:
    def __init__(self) -> None:
        self.events: dict[str, OutboxRecord] = {}
        self.payloads: list[dict[str, Any]] = []
        self.find_calls: list[dict[str, Any]] = []
        self.enqueue_calls = 0
        self.seal_calls = 0
        self.scope_conflicts: list[str] = []
        self.semantic_conflicts: list[str] = []

    def _scope(self, kwargs: dict[str, Any]) -> tuple[str, str, str]:
        return (
            str(kwargs["official_stream_id"]),
            str(kwargs["lineage_id"]),
        )

    async def assert_runtime_leader(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def find_eligible_mews_snapshot(self, **kwargs: Any) -> None:
        self.find_calls.append(kwargs)
        return None

    async def record_mews_snapshot(self, payload: dict[str, Any]) -> str:
        self.payloads.append(dict(payload))
        return "a" * 64

    async def mews_snapshot_is_eligible(self, *_args: Any, **_kwargs: Any) -> bool:
        return False

    async def get_outbox_event(self, event_id: str, **kwargs: Any) -> OutboxRecord | None:
        current = self.events.get(event_id)
        if current is None:
            return None
        expected_scope = (current.route_id, *self._scope(kwargs))
        actual_scope = self._event_scope(current)
        if actual_scope != expected_scope:
            self.scope_conflicts.append(f"get:{event_id}")
            raise V20SemanticConflict("event scope conflict")
        return current

    def _event_scope(self, event: OutboxRecord) -> tuple[str, str, str]:
        return (
            event.route_id,
            str(event.official_stream_id),
            str(event.lineage_id),
        )

    async def enqueue_alert(
        self,
        event_id: str,
        route_id: str,
        semantic: dict[str, Any],
        semantic_hash: str,
        **kwargs: Any,
    ) -> bool:
        self.enqueue_calls += 1
        current = self.events.get(event_id)
        if current is None:
            self.events[event_id] = OutboxRecord(
                event_id=event_id,
                event_type="DATA_ALERT",
                route_id=route_id,
                semantic=dict(semantic),
                semantic_content_hash=semantic_hash,
                payload=None,
                payload_hash=None,
                generated_at=None,
                commit_marker=None,
                action_expiry_ts=None,
                delivery_status="PENDING",
                attempt_count=0,
                **kwargs,
            )
            return True
        if self._event_scope(current) != (route_id, *self._scope(kwargs)):
            self.scope_conflicts.append(f"enqueue:{event_id}")
            raise V20SemanticConflict("event scope conflict")
        if current.semantic_content_hash != semantic_hash:
            self.semantic_conflicts.append(f"enqueue:{event_id}")
            raise V20SemanticConflict("event semantic conflict")
        return False

    async def seal_event(self, event_id: str, builder: Any) -> OutboxRecord:
        self.seal_calls += 1
        current = self.events[event_id]
        if current.payload is not None:
            return current
        generated_at = datetime(2026, 9, 1, 14, 5, tzinfo=TZ)
        payload = dict(builder(current, generated_at, 91, True))
        sealed = replace(
            current,
            payload=payload,
            payload_hash=sha256_json(payload),
            generated_at=generated_at,
            commit_marker=91,
        )
        self.events[event_id] = sealed
        return sealed


async def test_same_date_failure_details_and_stages_use_one_durable_event_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _mews_repository()
    calendar = (date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2))
    now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)

    first = _start_mews_service(monkeypatch, repository, _FailingSource(["margin SSE missing"]))
    first._clock = lambda: now
    assert await first._recover_mews_after_cutoff_once(now, calendar) is False

    second = _start_mews_service(monkeypatch, repository, _FailingSource(["margin SZSE missing"]))
    second._clock = lambda: now
    assert await second.ensure_mews_for_selection_trigger(now) is False

    assert [source.calls for source in (first._mews_source, second._mews_source)] == [1, 1]
    assert len(repository.events) == 1
    event_ids = list(repository.events)
    assert repository.events[event_ids[0]].payload is not None


async def test_alert_persistence_failure_does_not_latch_away_the_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _mews_repository()
    source = _FailingSource(["first outage", "second outage"])
    service = _start_mews_service(monkeypatch, repository, source)
    now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)
    service._clock = lambda: now
    original_enqueue = repository.enqueue_alert

    async def enqueue_once_then_succeed(*args: Any, **kwargs: Any) -> bool:
        if repository.enqueue_calls == 0:
            repository.enqueue_calls += 1
            raise RuntimeError("outbox temporarily unavailable")
        return await original_enqueue(*args, **kwargs)

    repository.enqueue_alert = enqueue_once_then_succeed

    assert await service.ensure_mews_for_selection_trigger(now) is False
    assert source.calls == 1
    assert repository.events == {}

    assert await service.ensure_mews_for_selection_trigger(now) is False
    assert source.calls == 2
    assert len(repository.events) == 1
    event = next(iter(repository.events.values()))
    assert event.semantic["alert_code"] == "MEWS_CALCULATION_FAILED"
    assert event.payload is not None


async def test_cutoff_waits_for_and_preserves_started_mews_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _mews_repository()
    master_started = asyncio.Event()
    release_master = asyncio.Event()
    calls = 0

    class _SlowSource:
        async def fetch_snapshot(self, **_kwargs: Any) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            master_started.set()
            await release_master.wait()
            return _late_mews_payload()

    service = _start_mews_service(monkeypatch, repository, _SlowSource())
    before = datetime(2026, 9, 1, 9, 39, 59, 990000, tzinfo=TZ)
    current = before
    service._clock = lambda: current
    joins: list[asyncio.Task[bool]] = []
    cutoff_calls: list[datetime] = []
    real_join = service._mews_singleflight_join

    async def join_master(current: datetime, *, stage: str) -> asyncio.Task[bool]:
        task = await real_join(current, stage=stage)
        joins.append(task)
        return task

    monkeypatch.setattr(service, "_mews_singleflight_join", join_master)

    async def run_once(*_args: Any, **_kwargs: Any) -> None:
        await service.ensure_mews_for_selection_trigger(before)

    async def enforce_cutoff(*_args: Any, **kwargs: Any) -> bool:
        cutoff_calls.append(kwargs["now"])
        return True

    monkeypatch.setattr(service, "run_once", run_once)
    monkeypatch.setattr(service, "_enforce_or_alert_entry_cutoff", enforce_cutoff)

    watchdog = asyncio.create_task(service._run_decision_iteration_with_cutoff(before))
    await asyncio.wait_for(master_started.wait(), timeout=1.0)

    master = service._mews_singleflight_task
    assert master is not None
    assert master.done() is False
    assert master.cancelled() is False
    current = datetime(2026, 9, 1, 9, 40, 0, 10000, tzinfo=TZ)
    await asyncio.sleep(0)
    assert watchdog.done() is False

    release_master.set()
    await asyncio.wait_for(watchdog, timeout=1.0)
    assert await service.ensure_mews_for_selection_trigger(before) is True
    assert joins == [master]
    assert cutoff_calls == [current]
    assert service._mews_singleflight_task is None
    assert calls == 1
    assert len(repository.payloads) == 1


async def test_date_rollover_scheduler_and_trigger_race_shares_one_new_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _mews_repository()
    old_date = date(2026, 9, 1)
    new_date = date(2026, 9, 2)
    old_started = asyncio.Event()
    release_old = asyncio.Event()
    new_started = asyncio.Event()
    release_new = asyncio.Event()
    source_trade_dates: list[date] = []
    source_calls = 0

    class _CrossDateFailingSource:
        async def fetch_snapshot(self, **kwargs: Any) -> dict[str, Any]:
            nonlocal source_calls
            source_trade_date = kwargs["source_trade_date"]
            source_trade_dates.append(source_trade_date)
            source_calls += 1
            if source_calls == 1:
                old_started.set()
                await asyncio.wait_for(release_old.wait(), timeout=1)
            else:
                new_started.set()
                await asyncio.wait_for(release_new.wait(), timeout=1)
            raise MewsSnapshotSourceError(f"outage {source_trade_date.isoformat()}")

    calendar = (date(2026, 8, 31), old_date, new_date, date(2026, 9, 3))
    service = _start_mews_service(monkeypatch, repository, _CrossDateFailingSource())
    old_now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)
    new_now = datetime(2026, 9, 2, 9, 20, tzinfo=TZ)
    service._clock = lambda: old_now

    old_waiter = asyncio.create_task(service.ensure_mews_for_selection_trigger(old_now))
    await asyncio.wait_for(old_started.wait(), timeout=1)
    old_master = service._mews_singleflight_task
    assert old_master is not None
    assert old_master.done() is False

    service._clock = lambda: new_now
    joins: list[asyncio.Task[bool]] = []
    real_join = service._mews_singleflight_join

    async def join_master(current: datetime, *, stage: str) -> asyncio.Task[bool]:
        task = await real_join(current, stage=stage)
        joins.append(task)
        return task

    monkeypatch.setattr(service, "_mews_singleflight_join", join_master)
    rollover = asyncio.gather(
        service._refresh_mews_cache_once(new_now, calendar),
        service.ensure_mews_for_selection_trigger(new_now),
    )
    await asyncio.wait_for(new_started.wait(), timeout=1)
    new_master = service._mews_singleflight_task
    assert new_master is not None
    assert new_master is not old_master

    release_old.set()
    release_new.set()
    assert await rollover == [False, False]
    assert await old_waiter is False
    assert old_master.cancelled() is True
    assert source_trade_dates.count(date(2026, 8, 31)) == 1
    assert source_trade_dates.count(old_date) == 1
    assert len(joins) == 2
    assert joins[0] is new_master
    assert joins[1] is new_master
    assert service._mews_singleflight_task is None


async def test_stop_racing_date_rollover_join_leaves_no_new_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _mews_repository()
    old_started = asyncio.Event()
    old_cancel_entered = asyncio.Event()
    allow_old_cancel = asyncio.Event()
    reject_new = asyncio.Event()
    source_calls = 0

    class _CancelHoldingSource:
        async def fetch_snapshot(self, **_kwargs: Any) -> dict[str, Any]:
            nonlocal source_calls
            source_calls += 1
            if source_calls == 1:
                old_started.set()
                try:
                    await asyncio.wait_for(asyncio.Event().wait(), timeout=1)
                except asyncio.CancelledError:
                    old_cancel_entered.set()
                    await asyncio.wait_for(allow_old_cancel.wait(), timeout=1)
                    raise
            new_started = asyncio.Event()
            new_started.set()
            await asyncio.wait_for(reject_new.wait(), timeout=1)
            raise MewsSnapshotSourceError("unauthorized rollover attempt")

    calendar = (date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2))
    service = _start_mews_service(monkeypatch, repository, _CancelHoldingSource())
    old_now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)
    new_now = datetime(2026, 9, 2, 9, 20, tzinfo=TZ)
    service._clock = lambda: old_now

    old_waiter = asyncio.create_task(service.ensure_mews_for_selection_trigger(old_now))
    await asyncio.wait_for(old_started.wait(), timeout=1)
    service._clock = lambda: new_now

    first_rollover = asyncio.create_task(service._refresh_mews_cache_once(new_now, calendar))
    await asyncio.wait_for(old_cancel_entered.wait(), timeout=1)
    second_rollover = asyncio.create_task(service.ensure_mews_for_selection_trigger(new_now))
    await asyncio.sleep(0)
    stopping = asyncio.create_task(service.stop())
    await asyncio.sleep(0)
    allow_old_cancel.set()
    reject_new.set()

    await asyncio.wait_for(stopping, timeout=1)
    orphan = service._mews_singleflight_task
    assert orphan is None
    assert not any(
        task.get_name().startswith("v20-mews-singleflight-")
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
    )

    await asyncio.wait_for(
        asyncio.gather(first_rollover, second_rollover, return_exceptions=True),
        timeout=1,
    )
    assert await asyncio.wait_for(old_waiter, timeout=1) is False
    assert source_calls == 1
    assert not any(
        task.get_name().startswith("v20-mews-singleflight-")
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
    )


async def test_meWS_failure_event_ids_scope_route_and_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _StrictScopedMewsRepository()
    calendar = (date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2))
    now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)

    first = _start_mews_service(monkeypatch, repository, _FailingSource(["same scoped outage"]))
    first._clock = lambda: now
    assert await first._recover_mews_after_cutoff_once(now, calendar) is False

    second = _start_mews_service(monkeypatch, repository, _FailingSource(["same scoped outage"]))
    second.config = replace(
        second.config,
        route_id="route-formal-test",
        state_lineage_id="lineage-formal-test",
    )
    second._clock = lambda: now
    assert await second.ensure_mews_for_selection_trigger(now) is False

    third = _start_mews_service(monkeypatch, repository, _FailingSource(["same scoped outage"]))
    third.config = replace(
        third.config,
        route_id="route-formal-test",
        state_lineage_id="lineage-formal-test",
    )
    third._clock = lambda: now
    assert await third.ensure_mews_for_selection_trigger(now) is False

    assert len(repository.events) == 2
    assert {event.route_id for event in repository.events.values()} == {
        first.config.route_id,
        second.config.route_id,
    }
    assert repository.scope_conflicts == []
    assert repository.semantic_conflicts == []
    formal_events = [
        event for event in repository.events.values() if event.route_id == "route-formal-test"
    ]
    assert len(formal_events) == 1
    assert formal_events[0].payload is not None
    assert repository.seal_calls == 2


async def test_scheduler_retries_unpersisted_meWS_alert_after_failure_latch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _mews_repository()
    source = _FailingSource(["persistent outage"])
    service = _start_mews_service(monkeypatch, repository, source)
    now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)
    service._clock = lambda: now
    original_enqueue = repository.enqueue_alert

    async def fail_first_enqueue(*args: Any, **kwargs: Any) -> bool:
        if repository.enqueue_calls == 0:
            repository.enqueue_calls += 1
            raise RuntimeError("alert outbox unavailable")
        return await original_enqueue(*args, **kwargs)

    repository.enqueue_alert = fail_first_enqueue
    calendar = (date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2))

    assert await service._recover_mews_after_cutoff_once(now, calendar) is False
    assert await service._recover_mews_after_cutoff_once(now, calendar) is False

    assert source.calls == 2
    assert len(repository.events) == 1
    event = next(iter(repository.events.values()))
    assert event.semantic["alert_code"] == "MEWS_CALCULATION_FAILED"
    assert event.payload is not None


async def test_meWS_alert_latch_accepts_only_strict_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _mews_repository()
    service = _start_mews_service(monkeypatch, repository, _FailingSource(["persistent outage"]))
    results = iter((False, None, True))

    async def safe_alert(*_args: Any, **_kwargs: Any) -> bool | None:
        return next(results)

    monkeypatch.setattr(service, "_safe_alert", safe_alert)
    exception = MewsSnapshotSourceError("persistent outage")
    calendar = (date(2026, 8, 31), date(2026, 9, 1))
    now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)

    await service._alert_mews_calculation_failure(
        exception, now, stage="SCHEDULED_AFTER_CUTOFF_RECOVERY", calendar=calendar
    )
    assert service._mews_alerted_for is None

    await service._alert_mews_calculation_failure(
        exception, now, stage="SCHEDULED_AFTER_CUTOFF_RECOVERY", calendar=calendar
    )
    assert service._mews_alerted_for is None

    await service._alert_mews_calculation_failure(
        exception, now, stage="SCHEDULED_AFTER_CUTOFF_RECOVERY", calendar=calendar
    )
    assert service._mews_alerted_for == now.date()


async def test_mews_leadership_lost_penetrates_singleflight_waiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _mews_repository()
    service = _start_mews_service(monkeypatch, repository, _FailingSource(["leader replaced"]))

    async def assert_runtime_leader() -> None:
        raise V20LeadershipLost("leader session was replaced")

    repository.assert_runtime_leader = assert_runtime_leader
    now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)
    service._clock = lambda: now

    with pytest.raises(V20LeadershipLost, match="leader session was replaced"):
        await service.ensure_mews_for_selection_trigger(now)
    assert service._mews_singleflight_task is None
