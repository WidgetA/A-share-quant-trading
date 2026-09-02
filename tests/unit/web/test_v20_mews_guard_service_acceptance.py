from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.data.database.v20_mews_guard_store import V20MewsGuardStore
from src.data.database.v20_repository import (
    ActiveModelLeg,
    sha256_json,
)
from src.web.v20_service import V20Service
from tests.unit.web.test_v20_service import _service

TZ = ZoneInfo("Asia/Shanghai")
SOURCE_DATE = date(2026, 8, 31)
D1 = date(2026, 9, 1)
D2 = date(2026, 9, 2)
CALENDAR = (SOURCE_DATE, D1, D2, date(2026, 9, 3))
T_0910 = datetime(2026, 9, 1, 9, 10, tzinfo=TZ)
T_0915 = datetime(2026, 9, 1, 9, 15, tzinfo=TZ)
T_1404 = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)
T_D2_MORNING = datetime(2026, 9, 2, 9, tzinfo=TZ)


class _AsyncContext:
    def __init__(self, value: Any) -> None:
        self.value = value

    async def __aenter__(self) -> Any:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


class _FakePool:
    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def acquire(self) -> _AsyncContext:
        return _AsyncContext(self.connection)


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_args: object) -> bool:
        return False


class _AsyncpgLikeRecord:
    def __init__(self, values: dict[str, Any]) -> None:
        self.values = values

    def __getitem__(self, key: str) -> Any:
        return self.values[key]


class _StrictPGConnection:
    def __init__(self, *, existing: Any = None) -> None:
        self.existing = existing
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def transaction(self, *, isolation: str) -> _FakeTransaction:
        assert isolation == "serializable"
        return _FakeTransaction()

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        self.calls.append((sql, args))
        if "FOR UPDATE OF leg" in sql:
            return {"d1": D1}
        if "FROM v20.leg_mews_selection" in sql:
            return self.existing
        if "INSERT INTO v20.leg_mews_selection" in sql:
            return {
                "model_leg_id": args[0],
                "cutoff_ts": args[3],
                "selection_reason": args[4],
                "selected_at": T_0915 + timedelta(minutes=2),
                "selected_snapshot_id": args[1],
                "selected_fast_state": args[2],
            }
        if "FROM v20.mews_snapshots AS snapshot" in sql:
            return _snapshot_row()
        raise AssertionError(f"unexpected strict PostgreSQL SQL: {sql}")


class _StrictRepository:
    """Repository surface that makes legacy MEWS selection/read paths fail."""

    schema = "v20"

    def __init__(self, connection: _StrictPGConnection) -> None:
        self.pool = _FakePool(connection)
        self.record_calls: list[dict[str, Any]] = []
        self.old_select_calls = 0
        self.old_load_calls = 0
        self.closed = False

    async def assert_runtime_leader(self) -> None:
        return None

    async def record_mews_snapshot(self, payload: dict[str, Any]) -> str:
        self.record_calls.append(dict(payload))
        return sha256_json(payload)

    async def mews_snapshot_is_eligible(self, *_args: Any, **_kwargs: Any) -> bool:
        return True

    async def find_eligible_mews_snapshot(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def select_mews_for_leg(self, *_args: Any, **_kwargs: Any) -> None:
        self.old_select_calls += 1
        raise AssertionError("legacy repository select_mews_for_leg is forbidden")

    async def load_selected_mews_for_leg(self, *_args: Any, **_kwargs: Any) -> None:
        self.old_load_calls += 1
        raise AssertionError("legacy repository load_selected_mews_for_leg is forbidden")

    async def get_exit_scan_watermarks(self, *_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {D1.isoformat(): "14:57", D2.isoformat(): "14:56"}

    async def close(self) -> None:
        self.closed = True


class _LocalMewsSource:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def fetch_snapshot(
        self,
        *,
        source_trade_date: date,
        availability_date: date,
    ) -> dict[str, Any]:
        self.calls += 1
        assert source_trade_date == SOURCE_DATE
        assert availability_date == D1
        self.started.set()
        await asyncio.wait_for(self.release.wait(), timeout=2)
        generated_at = T_0915 if self.calls == 1 else T_1404
        return _payload(generated_at=generated_at)


def _payload(*, generated_at: datetime = T_0915) -> dict[str, Any]:
    return {
        "snapshot_id": "service-mews-v2",
        "source_trade_date": SOURCE_DATE.isoformat(),
        "generated_at": generated_at.isoformat(),
        "fast_state": "NORMAL",
        "model_version": "mews_v2",
        "data_version": "d" * 64,
        "evidence": {
            "profile": "LOCAL_TUSHARE_MEWS_V2_0910_V1",
            "signal_available_date": D1.isoformat(),
        },
    }


def _snapshot_row(*, sealed: datetime | None = T_0915 + timedelta(minutes=1)) -> Any:
    payload = _payload()
    return _AsyncpgLikeRecord(
        {
            "snapshot_id": payload["snapshot_id"],
            "source_trade_date": SOURCE_DATE,
            "generated_at": T_0915,
            "received_at": T_0915 + timedelta(seconds=1),
            "receipt_sealed_at": sealed,
            "fast_state": payload["fast_state"],
            "model_version": payload["model_version"],
            "data_version": payload["data_version"],
            "content_hash": sha256_json(payload),
            "snapshot_json": json.dumps(payload),
        }
    )


def _illegal_existing_selection() -> dict[str, Any]:
    snapshot = _snapshot_row(sealed=None).values
    return {
        "model_leg_id": "leg-strict",
        "cutoff_ts": datetime(2026, 9, 1, 9, 40, tzinfo=TZ),
        "selection_reason": "ELIGIBLE",
        "selected_at": T_0915 + timedelta(minutes=2),
        "selected_snapshot_id": snapshot["snapshot_id"],
        "selected_fast_state": snapshot["fast_state"],
        **snapshot,
    }


def _service_from_repository(
    monkeypatch: pytest.MonkeyPatch,
    repository: _StrictRepository,
    source: Any,
    *,
    now: datetime = T_0910,
) -> V20Service:
    service = _service(monkeypatch, repository)
    service._started = True
    service._repository_started = True
    service._mews_source = source
    service._clock = lambda: now

    async def calendar_provider() -> list[date]:
        return list(CALENDAR)

    monkeypatch.setattr(service, "_calendar_provider", calendar_provider)
    return service


def _active_leg() -> ActiveModelLeg:
    return ActiveModelLeg(
        model_leg_id="leg-strict",
        model_batch_id="batch-strict",
        decision_id=None,
        signal_date=SOURCE_DATE,
        code="600000",
        stock_name="strict-stock",
        rank=1,
        relative_weight=1.0,
        d1=D1,
        d2=D2,
        reference_status="LOCKED",
        reference_price=10.0,
        reference_snapshot_hash="r" * 64,
        evaluation_only=False,
        mews_snapshot_id=None,
        mews_fast_state=None,
        exit_intent_id=None,
    )


async def test_service_owns_strict_postgres_guard_store(monkeypatch: pytest.MonkeyPatch) -> None:
    repository = _StrictRepository(_StrictPGConnection())
    service = _service_from_repository(monkeypatch, repository, _LocalMewsSource())

    assert isinstance(getattr(service, "_mews_guard_store", None), V20MewsGuardStore)
    assert service._mews_guard_store._repository is repository


async def test_0910_scheduler_starts_local_mews_and_persists_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _StrictRepository(_StrictPGConnection())
    source = _LocalMewsSource()
    source.release.set()
    service = _service_from_repository(monkeypatch, repository, source)

    result = await service._refresh_mews_cache_once(T_0910, CALENDAR)
    assert result is True
    assert service._mews_last_failure is None
    assert source.calls == 1
    assert len(repository.record_calls) == 1
    assert service._mews_cached_for == D1
    assert service._mews_source_trade_date == SOURCE_DATE
    assert service._mews_snapshot_id == "service-mews-v2"


async def test_missing_cache_trigger_kicks_singleflight_without_awaiting_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _StrictRepository(_StrictPGConnection())
    source = _LocalMewsSource()
    service = _service_from_repository(
        monkeypatch, repository, source, now=datetime(2026, 9, 1, 9, 39, tzinfo=TZ)
    )
    first = service.kick_mews_for_selection_trigger(T_1404)
    second = service.kick_mews_for_selection_trigger(T_1404)

    try:
        # ``kick`` is the production route/selection surface: both calls return
        # owned tasks immediately while the shared provider attempt is still
        # blocked.  Callers are never required to await MEWS before selecting.
        await asyncio.wait_for(source.started.wait(), timeout=1)
        await asyncio.sleep(0)
        assert first.done() is False
        assert second.done() is False
        assert source.calls == 1
        assert service._mews_trigger_tasks == {first, second}
    finally:
        source.release.set()
        assert await asyncio.wait_for(asyncio.gather(first, second), timeout=1) == [True, True]

    await asyncio.sleep(0)
    assert service._mews_trigger_tasks == set()
    assert service._mews_singleflight_task is None


async def test_concurrent_missing_cache_triggers_compute_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _StrictRepository(_StrictPGConnection())
    source = _LocalMewsSource()
    service = _service_from_repository(monkeypatch, repository, source, now=T_1404)
    triggers = [
        asyncio.create_task(service.ensure_mews_for_selection_trigger(T_1404)) for _ in range(2)
    ]

    await asyncio.wait_for(source.started.wait(), timeout=1)
    source.release.set()
    first, second = await asyncio.wait_for(asyncio.gather(*triggers), timeout=1)

    assert (first, second) == (True, True)
    assert source.calls == 1
    assert len(repository.record_calls) == 1
    assert service._mews_singleflight_task is None


async def test_stop_cancels_and_drains_service_owned_mews_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _StrictRepository(_StrictPGConnection())
    source = _LocalMewsSource()
    service = _service_from_repository(monkeypatch, repository, source, now=T_1404)
    trigger = asyncio.create_task(service.ensure_mews_for_selection_trigger(T_1404))
    await asyncio.wait_for(source.started.wait(), timeout=1)
    master = service._mews_singleflight_task
    assert master is not None

    await asyncio.wait_for(service.stop(), timeout=1)

    assert master.cancelled() is True
    assert service._mews_singleflight_task is None
    assert repository.closed is True
    assert await asyncio.wait_for(trigger, timeout=1) is False
    assert not any(
        task.get_name().startswith("v20-mews-singleflight-")
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
    )


@pytest.mark.parametrize(
    ("existing", "expected_alerts", "expected_sql_count", "expect_insert"),
    [
        (None, [], 4, True),
        (_illegal_existing_selection(), ["MEWS_UNAVAILABLE_FALLBACK_12"], 2, False),
    ],
    ids=["strict-success", "illegal-existing-freeze"],
)
async def test_exit_path_uses_only_guard_store_select_freeze_and_load(
    monkeypatch: pytest.MonkeyPatch,
    existing: Any,
    expected_alerts: list[str],
    expected_sql_count: int,
    expect_insert: bool,
) -> None:
    connection = _StrictPGConnection(existing=existing)
    repository = _StrictRepository(connection)
    source = _LocalMewsSource()
    source.release.set()
    service = _service_from_repository(monkeypatch, repository, source, now=T_D2_MORNING)
    service._mews_guard_store = V20MewsGuardStore(repository)
    alerts: list[str] = []

    async def record_alert(*, code: str, **_kwargs: Any) -> None:
        alerts.append(code)

    async def no_bars(_record: Any, _now: datetime) -> list[Any]:
        return []

    monkeypatch.setattr(service, "_safe_alert", record_alert)
    monkeypatch.setattr(service, "_load_exit_bar_records", no_bars)

    await service._evaluate_one_exit(
        _active_leg(),
        T_D2_MORNING,
        detection_calendar_status="CONFIRMED_TRADING",
        detection_is_trading_day=True,
        next_trade_date=date(2026, 9, 3),
        calendar=CALENDAR,
    )

    assert repository.old_select_calls == 0
    assert repository.old_load_calls == 0
    assert alerts == expected_alerts
    assert len(connection.calls) == expected_sql_count
    assert any("FOR UPDATE OF leg" in sql for sql, _args in connection.calls)
    assert (
        any("INSERT INTO v20.leg_mews_selection" in sql for sql, _args in connection.calls)
        is expect_insert
    )
    assert (
        any("FROM v20.mews_snapshots AS snapshot" in sql for sql, _args in connection.calls)
        is expect_insert
    )
    assert all("SELECT " in sql or "INSERT " in sql for sql, _args in connection.calls)
