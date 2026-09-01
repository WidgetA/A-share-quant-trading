from __future__ import annotations

import asyncio
import hashlib
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

import src.strategy.v20.runtime_config as runtime_config_module
import src.web.v20_service as service_module
from src.data.clients.tushare_realtime import TushareMinuteBar
from src.data.database.v20_repository import (
    ActiveModelLeg,
    MinuteBarRecord,
    V20SemanticConflict,
    sha256_json,
)
from src.strategy.v20.identity import named_hash
from src.web.v20_service import FULL_EXIT_LABELS, V20LiveExitStageTimeout, _DayContext
from tests.unit.web.test_v20_service import _bar, _service

TZ = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 9, 1)


class _VirtualClock:
    def __init__(self) -> None:
        self.current = 100.0

    def time(self) -> float:
        return self.current


class _TimeoutSpy:
    def __init__(self, clock: _VirtualClock) -> None:
        self.clock = clock
        self.timeouts: list[float] = []
        self.deadlines: list[float] = []
        self.cancel_boundaries: list[float] = []
        self.original: Any = None

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.original = service_module.asyncio.wait_for

        async def spying_wait_for(awaitable: Any, timeout: float) -> Any:
            started_at = self.clock.time()
            requested = float(timeout)
            deadline = started_at + requested
            self.timeouts.append(requested)
            self.deadlines.append(deadline)
            try:
                return await self.original(awaitable, timeout=timeout)
            except asyncio.TimeoutError:
                self.cancel_boundaries.append(deadline)
                raise

        monkeypatch.setattr(service_module.asyncio, "wait_for", spying_wait_for)


def _bind_virtual_clock() -> _VirtualClock:
    clock = _VirtualClock()
    loop = asyncio.get_running_loop()
    loop.time = clock.time  # type: ignore[method-assign]
    return clock


class _DeadlineRepository:
    def __init__(self, legs: list[ActiveModelLeg]) -> None:
        self.legs = legs
        self.record_calls = 0
        self.persisted_rows: list[dict[str, Any]] = []
        self.accepted_codes: set[str] | None = None
        self.exit_commits: list[Any] = []
        self.sealed_event_ids: list[str] = []
        self.alerts: list[tuple[str, dict[str, Any]]] = []
        self.alert_ids: list[str] = []
        self.alert_attempts: list[tuple[str, dict[str, Any]]] = []
        self.enqueue_alert_calls: list[str] = []

    async def list_active_legs(self, _trade_date: date, **_kwargs: Any) -> list[ActiveModelLeg]:
        return list(self.legs)

    async def record_minute_bars(self, rows: list[dict[str, Any]]) -> frozenset[str]:
        self.record_calls += 1
        admitted = [
            row
            for row in rows
            if self.accepted_codes is None or str(row["stock_code"]) in self.accepted_codes
        ]
        self.persisted_rows.extend(admitted)
        return frozenset(sha256_json(row) for row in admitted)

    async def assert_runtime_leader(self) -> None:
        return None

    async def select_mews_for_leg(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def load_selected_mews_for_leg(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def list_minute_bars(
        self, code: str, *, trade_dates: tuple[date, date], **_kwargs: Any
    ) -> list[MinuteBarRecord]:
        records: list[MinuteBarRecord] = []
        for row in self.persisted_rows:
            if str(row["stock_code"]) != code:
                continue
            bar_end = datetime.fromisoformat(str(row["bar_end"]))
            if bar_end.date() not in trade_dates:
                continue
            records.append(
                MinuteBarRecord(
                    code=code,
                    bar_end=bar_end,
                    end_label=str(row["end_label"]),
                    source_hash=sha256_json(row),
                    payload=row,
                    first_received_at=datetime.now(TZ),
                )
            )
        return records

    async def get_exit_scan_watermarks(self, model_leg_id: str, **_kwargs: Any) -> dict[date, str]:
        leg = next(item for item in self.legs if item.model_leg_id == model_leg_id)
        return {leg.d1: "14:57", leg.d2: "14:56"}

    async def commit_exit(self, commit: Any) -> bool:
        self.exit_commits.append(commit)
        return True

    async def seal_event(self, event_id: str, *_args: Any, **_kwargs: Any) -> bool:
        self.sealed_event_ids.append(event_id)
        return True

    async def enqueue_alert(
        self,
        alert_id: str,
        _route_id: str,
        semantic: dict[str, Any],
        *_args: Any,
        **_kwargs: Any,
    ) -> bool:
        # Mirror the real immutable outbox contract: a repeat of the same
        # event_id is only a no-op when the semantics are byte-identical;
        # any drift is a conflict, never a silent swallow.
        self.enqueue_alert_calls.append(alert_id)
        self.alert_attempts.append((alert_id, dict(semantic)))
        if alert_id in self.alert_ids:
            index = self.alert_ids.index(alert_id)
            if self.alerts[index][1] != semantic:
                raise V20SemanticConflict("alert event_id already has different semantics")
            return False
        self.alert_ids.append(alert_id)
        self.alerts.append((alert_id, dict(semantic)))
        return True

    async def record_exit_scan_watermark(self, *_args: Any, **_kwargs: Any) -> bool:
        return True


class _DeadlineClient:
    def __init__(
        self,
        *,
        latest: dict[str, TushareMinuteBar] | BaseException,
        history: dict[str, tuple[TushareMinuteBar, ...]] | BaseException,
    ) -> None:
        self.latest = latest
        self.history = history
        self.latest_calls: list[list[str]] = []
        self.history_calls: list[list[str]] = []
        self.closed_calls: list[tuple[list[str], date]] = []

    async def batch_get_latest_minute_bars(self, codes: list[str]) -> dict[str, TushareMinuteBar]:
        self.latest_calls.append(list(codes))
        if isinstance(self.latest, BaseException):
            raise self.latest
        return dict(self.latest)

    async def batch_get_minute_history(
        self, codes: list[str]
    ) -> dict[str, tuple[TushareMinuteBar, ...]]:
        self.history_calls.append(list(codes))
        if isinstance(self.history, BaseException):
            raise self.history
        return dict(self.history)

    async def batch_get_minute_history_for_date(
        self, codes: list[str], trade_date: date
    ) -> dict[str, tuple[TushareMinuteBar, ...]]:
        self.closed_calls.append((list(codes), trade_date))
        return {}


def _leg(code: str = "000001") -> ActiveModelLeg:
    return ActiveModelLeg(
        model_leg_id=f"leg-{code}",
        model_batch_id=f"batch-{code}",
        decision_id=f"decision-{code}",
        signal_date=date(2026, 8, 28),
        code=code,
        stock_name=f"stock-{code}",
        rank=1,
        relative_weight=1.0,
        d1=date(2026, 8, 31),
        d2=TRADE_DATE,
        reference_status="LOCKED",
        reference_price=10.0,
        reference_snapshot_hash="a" * 64,
        evaluation_only=False,
        mews_snapshot_id=None,
        mews_fast_state=None,
        exit_intent_id=None,
    )


def _prepare(
    monkeypatch: pytest.MonkeyPatch,
    repository: _DeadlineRepository,
    client: _DeadlineClient,
) -> tuple[Any, _DayContext]:
    project_root = Path(__file__).resolve().parents[3]
    for relative, reviewed_hashes in runtime_config_module._MIXED_STATE_SOURCE_CLASSES.items():
        source_hash = hashlib.sha256((project_root / relative).read_bytes()).hexdigest()
        monkeypatch.setitem(
            reviewed_hashes,
            source_hash,
            "V20_TEST_ISOLATION_DYNAMIC_SOURCE",
        )
    service = _service(monkeypatch, repository, client)
    return service, _DayContext(trade_date=TRADE_DATE, calendar=(TRADE_DATE,))


async def _advance_until(clock: _VirtualClock, target: float, limit: float) -> None:
    while clock.time() < target:
        remaining = min(target - clock.time(), 0.05, max(0.0, limit - clock.time()))
        if remaining <= 0:
            return
        await asyncio.sleep(0)
        clock.current += remaining


async def _wait_until(condition: Any) -> None:
    for _ in range(10_000):
        if condition():
            return
        await asyncio.sleep(0)


def _seed_complete_history(context: _DayContext, code: str) -> None:
    for label in FULL_EXIT_LABELS:
        bar = _bar(code, label, trade_date=TRADE_DATE)
        context.minute_rows[(TRADE_DATE, code, label)] = bar


def _expected_live_history(code: str) -> tuple[TushareMinuteBar, ...]:
    return tuple(
        _bar(code, label, trade_date=TRADE_DATE) for label in FULL_EXIT_LABELS if label < "10:00"
    )


def _seed_warm_live_history(context: _DayContext, code: str) -> None:
    for bar in _expected_live_history(code):
        context.minute_rows[(TRADE_DATE, code, bar.end_label)] = bar


async def test_stage_timeouts_share_one_monotonic_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    healthy = _leg("000001")
    bad = _leg("001306")
    repository = _DeadlineRepository([healthy, bad])
    client = _DeadlineClient(
        latest={healthy.code: _bar(healthy.code, "10:00", trade_date=TRADE_DATE)},
        history={bad.code: _expected_live_history(bad.code)},
    )
    service, context = _prepare(monkeypatch, repository, client)
    _seed_warm_live_history(context, healthy.code)
    clock = _bind_virtual_clock()
    spy = _TimeoutSpy(clock)
    spy.install(monkeypatch)
    started = clock.time()
    original_latest = client.batch_get_latest_minute_bars
    original_history = client.batch_get_minute_history
    original_persist = repository.record_minute_bars
    original_evaluate = service._evaluate_active_exits
    rule_calls = 0

    async def delayed_latest(codes: list[str]) -> dict[str, TushareMinuteBar]:
        await _advance_until(clock, started + 4.0, started + 13.0)
        return await original_latest(codes)

    async def delayed_history(codes: list[str]) -> dict[str, tuple[TushareMinuteBar, ...]]:
        await _advance_until(clock, started + 7.0, started + 13.0)
        return await original_history(codes)

    async def delayed_persist(rows: list[dict[str, Any]]) -> frozenset[str]:
        if repository.record_calls:
            await _advance_until(clock, started + 13.0, started + 14.0)
        return await original_persist(rows)

    async def slow_rules(*args: Any, **kwargs: Any) -> None:
        nonlocal rule_calls
        rule_calls += 1
        if rule_calls == 3:
            await _advance_until(clock, started + 11.0, started + 13.0)
        await original_evaluate(*args, **kwargs)

    async def quiet_alert(**_kwargs: Any) -> None:
        return None

    async def no_recovery(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(client, "batch_get_latest_minute_bars", delayed_latest)
    monkeypatch.setattr(client, "batch_get_minute_history", delayed_history)
    monkeypatch.setattr(repository, "record_minute_bars", delayed_persist)
    monkeypatch.setattr(service, "_evaluate_active_exits", slow_rules)
    monkeypatch.setattr(service, "_safe_alert", quiet_alert)
    monkeypatch.setattr(service, "_recover_closed_exit_windows", no_recovery)

    task = asyncio.create_task(
        service._run_live_exit_tick(context, datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ))
    )
    with pytest.raises(V20LiveExitStageTimeout):
        await task

    assert spy.deadlines[0] == started + 12.0
    assert all(boundary <= started + 12.0 for boundary in spy.cancel_boundaries)
    assert client.latest_calls == [[healthy.code, bad.code]]
    assert client.history_calls == [[bad.code]]
    assert repository.record_calls == 1
    assert spy.timeouts[0] == 12.0
    assert spy.timeouts[4] == 8.0
    assert any(7.0 < timeout <= 8.0 for timeout in spy.timeouts[5:])
    assert 15.0 not in spy.timeouts
    assert len(spy.timeouts) >= 5
    assert all(timeout <= 8.0 for timeout in spy.timeouts[3:5])


async def test_cold_missing_history_bypasses_latest_and_warm_polls_latest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    leg = _leg()
    repository = _DeadlineRepository([leg])
    client = _DeadlineClient(latest={}, history={leg.code: _expected_live_history(leg.code)})
    service, context = _prepare(monkeypatch, repository, client)

    async def evaluate(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def quiet_alert(**_kwargs: Any) -> None:
        return None

    async def no_recovery(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(service, "_evaluate_active_exits", evaluate)
    monkeypatch.setattr(service, "_safe_alert", quiet_alert)
    monkeypatch.setattr(service, "_recover_closed_exit_windows", no_recovery)
    now = datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ)
    await service._run_exit_cycle(context, now, include_stale=False)
    assert client.latest_calls == []
    assert client.history_calls == [[leg.code]]

    _seed_warm_live_history(context, leg.code)
    repository.record_calls = 0
    client.latest_calls.clear()
    client.history_calls.clear()
    context.last_exit_poll_at = None
    context.live_exit_market_data_outage = False
    await service._run_exit_cycle(context, now, include_stale=False)
    assert client.latest_calls == [[leg.code]]
    assert client.history_calls == []


async def test_pg_rejected_hash_cannot_stop_healthy_sibling_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    healthy = _leg("000001")
    bad = _leg("001306")
    repository = _DeadlineRepository([healthy, bad])
    repository.accepted_codes = {healthy.code}
    client = _DeadlineClient(
        latest={healthy.code: _bar(healthy.code, "10:00", close=8.0, trade_date=TRADE_DATE)},
        history={bad.code: _expected_live_history(bad.code)},
    )
    service, context = _prepare(monkeypatch, repository, client)
    _seed_warm_live_history(context, healthy.code)
    clock = _bind_virtual_clock()
    started = clock.time()
    original_history = client.batch_get_minute_history

    async def delayed_history(codes: list[str]) -> dict[str, tuple[TushareMinuteBar, ...]]:
        await _advance_until(clock, clock.time() + 20.0, clock.time() + 21.0)
        return await original_history(codes)

    async def quiet_alert(**_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(client, "batch_get_minute_history", delayed_history)
    monkeypatch.setattr(service, "_safe_alert", quiet_alert)
    task = asyncio.create_task(
        service._run_live_exit_tick(context, datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ))
    )
    await _wait_until(lambda: len(repository.exit_commits) == 1)

    assert repository.exit_commits[0].model_leg_id == healthy.model_leg_id
    assert all(commit.model_leg_id != bad.model_leg_id for commit in repository.exit_commits)
    assert clock.time() == pytest.approx(started)
    with pytest.raises(V20LiveExitStageTimeout):
        await task


async def test_closed_recovery_lane_cannot_delay_live_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    leg = _leg()
    repository = _DeadlineRepository([leg])
    client = _DeadlineClient(
        latest={leg.code: _bar(leg.code, "10:00", trade_date=TRADE_DATE)}, history={}
    )
    service, context = _prepare(monkeypatch, repository, client)
    _seed_warm_live_history(context, leg.code)
    clock = _bind_virtual_clock()
    original_closed = client.batch_get_minute_history_for_date

    async def delayed_closed(
        codes: list[str], trade_date: date
    ) -> dict[str, tuple[TushareMinuteBar, ...]]:
        await _advance_until(clock, clock.time() + 30.0, clock.time() + 31.0)
        return await original_closed(codes, trade_date)

    async def quiet_alert(**_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(client, "batch_get_minute_history_for_date", delayed_closed)
    monkeypatch.setattr(service, "_safe_alert", quiet_alert)
    task = asyncio.create_task(
        service._run_live_exit_tick(context, datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ))
    )
    await _wait_until(lambda: bool(client.closed_calls))
    await _advance_until(clock, clock.time() + 3.0, clock.time() + 4.0)
    assert task.done()
    assert client.closed_calls == []
    await _advance_until(clock, clock.time() + 14.0, clock.time() + 15.0)
    if not task.done():
        await task


async def test_scheduler_watchdog_is_fourteen_seconds_and_normal_tick_survives_twelve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    leg = _leg()
    repository = _DeadlineRepository([leg])
    client = _DeadlineClient(
        latest={leg.code: _bar(leg.code, "10:00", trade_date=TRADE_DATE)}, history={}
    )
    service, context = _prepare(monkeypatch, repository, client)
    _seed_warm_live_history(context, leg.code)
    clock = _bind_virtual_clock()
    spy = _TimeoutSpy(clock)
    spy.install(monkeypatch)
    original_latest = client.batch_get_latest_minute_bars
    monkeypatch.setattr(service, "_exit_context_for", lambda *_args, **_kwargs: context)
    monkeypatch.setattr(
        service,
        "_aware_now",
        lambda *_args, **_kwargs: datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ),
    )

    async def completing_latest(codes: list[str]) -> dict[str, TushareMinuteBar]:
        result = await original_latest(codes)
        service._stop_event.set()
        return result

    async def quiet_alert(**_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(client, "batch_get_latest_minute_bars", completing_latest)
    monkeypatch.setattr(service, "_safe_alert", quiet_alert)
    task = asyncio.create_task(service._run_live_exit_scheduler())
    await _wait_until(task.done)
    assert service._lane_health["live_exit"].last_error != "LIVE_EXIT_CYCLE_TIMEOUT"
    assert 14.0 in spy.timeouts
    assert 13.0 not in spy.timeouts


async def test_timeout_incident_is_structured_stable_and_retry_is_terminal_only(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    leg = _leg()
    repository = _DeadlineRepository([leg])
    client = _DeadlineClient(latest=RuntimeError("provider down"), history={})
    service, context = _prepare(monkeypatch, repository, client)
    service._repository_started = True
    clock = _bind_virtual_clock()

    async def evaluate(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def no_recovery(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def delayed_history(codes: list[str]) -> dict[str, tuple[TushareMinuteBar, ...]]:
        await _advance_until(clock, clock.time() + 20.0, clock.time() + 21.0)
        return {}

    monkeypatch.setattr(client, "batch_get_minute_history", delayed_history)
    monkeypatch.setattr(service, "_evaluate_active_exits", evaluate)
    monkeypatch.setattr(service, "_recover_closed_exit_windows", no_recovery)
    now = datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ)
    with caplog.at_level("WARNING", logger="src.web.v20_service"):
        succeeded = await service._run_phase_isolated(
            context,
            now,
            "LIVE_EXIT_CYCLE_FAILED",
            service._run_live_exit_tick(context, now),
            lane_name="live_exit",
        )
    assert succeeded is False
    assert context.exit_history_last_attempt[(leg.code, TRADE_DATE)] == now

    incident_id, semantic = repository.alerts[-1]
    # The incident id is a pure function of the stable incident identity.
    assert incident_id == named_hash(
        "V20_LIVE_EXIT_STAGE_INCIDENT_ID_V1",
        {
            "trade_date": TRADE_DATE.isoformat(),
            "stage": "history",
            "symbols": (leg.code,),
            "provider": "tushare_rt",
        },
    )
    # The immutable semantic carries only replay-stable diagnostics.
    assert semantic["error"] == "V20LiveExitStageTimeout"
    assert semantic["stage"] == "history"
    assert semantic["symbol"] == leg.code
    assert semantic["symbols"] == [leg.code]
    assert semantic["provider"] == "tushare_rt"
    assert semantic["incident_id"] == incident_id
    assert semantic["message"] == "live-exit stage history exceeded its budget"
    # No absolute monotonic deadline and no per-tick elapsed/remaining may
    # leak into the immutable semantic; they would break same-day dedup.
    assert "deadline" not in semantic
    assert "elapsed_seconds" not in semantic
    assert "remaining_seconds" not in semantic

    # Dynamic per-tick diagnostics live only in the log record.
    timeout_logs = [
        record.getMessage()
        for record in caplog.records
        if "live-exit stage timeout" in record.getMessage()
    ]
    assert len(timeout_logs) == 1
    assert "stage=history" in timeout_logs[0]
    assert "provider=tushare_rt" in timeout_logs[0]
    assert "deadline=" in timeout_logs[0]
    assert "elapsed=" in timeout_logs[0]
    assert "remaining=" in timeout_logs[0]

    # A same-day repeat of the same stage/provider/symbols incident must
    # re-enqueue the identical semantic; under real immutable outbox
    # semantics the second attempt is a terminal benign conflict, never a
    # silent overwrite and never a new event.
    context.exit_history_last_attempt.pop((leg.code, TRADE_DATE), None)
    await service._run_phase_isolated(
        context,
        now,
        "LIVE_EXIT_CYCLE_FAILED",
        service._run_live_exit_tick(context, now),
        lane_name="live_exit",
    )
    attempts = [attempt for attempt in repository.alert_attempts if attempt[0] == incident_id]
    assert len(attempts) == 2
    assert attempts[0][1] == attempts[1][1] == semantic
    assert repository.alert_ids.count(incident_id) == 1
    assert repository.alert_ids[-1] == incident_id


@pytest.mark.parametrize(
    ("stage", "provider", "symbols"),
    [
        ("lock", "internal", ()),
        ("db_list_active_legs", "postgres", ()),
        ("rules_initial", "rules", ("000001", "001306")),
        ("db_list_after_initial_rules", "postgres", ()),
        ("latest", "tushare_rt", ("000001", "001306")),
        ("db_persist_latest", "postgres", ("000001",)),
        ("db_list_after_latest", "postgres", ()),
        ("rules_after_latest", "rules", ("000001",)),
        ("db_list_before_history", "postgres", ()),
        ("history", "tushare_rt", ("001306",)),
        ("db_persist_history", "postgres", ("001306",)),
        ("db_exit_scan_watermark", "postgres", ("001306",)),
        ("db_list_final", "postgres", ()),
        ("rules_final", "rules", ("000001", "001306")),
    ],
)
async def test_every_stage_timeout_incident_is_structured_and_replay_stable(
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    provider: str,
    symbols: tuple[str, ...],
) -> None:
    repository = _DeadlineRepository([_leg()])
    client = _DeadlineClient(latest={}, history={})
    service, context = _prepare(monkeypatch, repository, client)
    service._repository_started = True
    now = datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ)
    exc = V20LiveExitStageTimeout(
        stage=stage,
        elapsed_seconds=3.25,
        remaining_seconds=8.75,
        deadline=112.0,
        symbols=symbols,
        provider=provider,
    )

    await service._record_live_exit_stage_incident(context, now, exc)
    assert exc.diagnostic_alert_emitted is True

    assert len(repository.alerts) == 1
    incident_id, semantic = repository.alerts[0]
    assert incident_id == named_hash(
        "V20_LIVE_EXIT_STAGE_INCIDENT_ID_V1",
        {
            "trade_date": TRADE_DATE.isoformat(),
            "stage": stage,
            "symbols": tuple(sorted(set(symbols))),
            "provider": provider,
        },
    )
    assert semantic["error"] == "V20LiveExitStageTimeout"
    assert semantic["stage"] == stage
    assert semantic["provider"] == provider
    assert semantic["symbol"] == ",".join(symbols)
    assert semantic["symbols"] == list(symbols)
    assert semantic["incident_id"] == incident_id
    assert "deadline" not in semantic
    assert "elapsed_seconds" not in semantic
    assert "remaining_seconds" not in semantic

    # Recording the same incident again is a terminal benign conflict: the
    # fake outbox enforces immutable semantics instead of swallowing by id.
    again = V20LiveExitStageTimeout(
        stage=stage,
        elapsed_seconds=9.5,
        remaining_seconds=0.5,
        deadline=150.0,
        symbols=symbols,
        provider=provider,
    )
    await service._record_live_exit_stage_incident(context, now, again)
    assert len(repository.alerts) == 1
    attempts = [attempt for attempt in repository.alert_attempts if attempt[0] == incident_id]
    assert len(attempts) == 2
    assert attempts[0][1] == attempts[1][1]


async def test_provider_gap_outbox_dedup_does_not_depend_on_poll_throttle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    leg = _leg()
    repository = _DeadlineRepository([leg])
    client = _DeadlineClient(latest={}, history={leg.code: ()})
    service, context = _prepare(monkeypatch, repository, client)
    service._repository_started = True

    async def evaluate(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def no_recovery(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(service, "_evaluate_active_exits", evaluate)
    monkeypatch.setattr(service, "_recover_closed_exit_windows", no_recovery)
    now = datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ)
    for _ in range(2):
        context.last_exit_poll_at = None
        context.exit_history_last_attempt.clear()
        context.live_exit_market_data_outage = False
        await service._run_phase_isolated(
            context,
            now,
            "LIVE_EXIT_CYCLE_FAILED",
            service._run_exit_cycle(context, now, include_stale=False),
            lane_name="live_exit",
        )

    gap_calls = [
        value
        for value in repository.enqueue_alert_calls
        if repository.alerts and repository.alert_ids[repository.alert_ids.index(value)]
    ]
    assert len(gap_calls) >= 2
    assert len(repository.alert_ids) == len(repository.alerts)
    assert all(alert_id in repository.alert_ids for alert_id in set(gap_calls))
    assert all(
        semantic.get("alert_code") != "LIVE_EXIT_CYCLE_FAILED"
        for _alert_id, semantic in repository.alerts
    )
