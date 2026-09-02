from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.data.database.v20_mews_guard_store import V20MewsGuardStore
from src.data.database.v20_repository import ActiveModelLeg, SelectedMewsRecord, sha256_json
from src.web.v20_service import V20Service, _mews_snapshot
from tests.unit.web.test_v20_service import _service

TZ = ZoneInfo("Asia/Shanghai")
SOURCE_DATE = date(2026, 8, 31)
D1 = date(2026, 9, 1)
D2 = date(2026, 9, 2)
NEXT_DAY = date(2026, 9, 3)
CALENDAR = (SOURCE_DATE, D1, D2, NEXT_DAY)
T_0915 = datetime(2026, 9, 1, 9, 15, tzinfo=TZ)
T_D2_0910 = datetime(2026, 9, 2, 9, 10, tzinfo=TZ)
T_D2_0945 = datetime(2026, 9, 2, 9, 45, tzinfo=TZ)
D2_CUTOFF = datetime(2026, 9, 2, 9, 40, tzinfo=TZ)


def test_selected_mews_maps_durable_seal_into_exit_policy_snapshot() -> None:
    generated_at = datetime(2026, 9, 2, 9, 10, tzinfo=TZ)
    received_at = generated_at + timedelta(seconds=1)
    sealed_at = generated_at + timedelta(minutes=1)
    record = SelectedMewsRecord(
        model_leg_id="leg-1",
        d1=D1,
        cutoff_ts=D2_CUTOFF,
        selection_reason="ELIGIBLE",
        selected_at=sealed_at,
        snapshot_id="mews-d2",
        source_trade_date=D1,
        generated_at=generated_at,
        received_at=received_at,
        receipt_sealed_at=sealed_at,
        fast_state="DANGER",
        model_version="mews_v2",
        data_version="d2",
        content_hash="a" * 64,
        payload={"evidence": {"signal_available_date": D2.isoformat()}},
    )

    snapshot = _mews_snapshot(record)[0]

    assert snapshot.source_trade_date == D1
    assert snapshot.availability_date == D2
    assert snapshot.generated_at == generated_at
    assert snapshot.received_at == sealed_at
    assert snapshot.received_at != received_at


class _AsyncContext:
    def __init__(self, value: Any) -> None:
        self.value = value

    async def __aenter__(self) -> Any:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


class _FakePool:
    def __init__(self, connection: _StrictPGConnection) -> None:
        self.connection = connection

    def acquire(self) -> _AsyncContext[_StrictPGConnection]:
        return _AsyncContext(self.connection)


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_args: object) -> bool:
        return False


class _Row:
    def __init__(self, values: dict[str, Any]) -> None:
        self.values = values

    def __getitem__(self, key: str) -> Any:
        return self.values[key]


def _payload(
    *,
    source_trade_date: date,
    availability_date: date,
) -> dict[str, Any]:
    generated_at = datetime.combine(availability_date, time(9, 45), tzinfo=TZ)
    return {
        "snapshot_id": f"mews-{source_trade_date:%Y%m%d}-{availability_date:%Y%m%d}",
        "source_trade_date": source_trade_date.isoformat(),
        "generated_at": generated_at.isoformat(),
        "fast_state": "NORMAL",
        "model_version": "mews_v2",
        "data_version": "d" * 64,
        "evidence": {
            "profile": "LOCAL_TUSHARE_MEWS_V2_0910_V1",
            "signal_available_date": availability_date.isoformat(),
        },
    }


def _snapshot_values(payload: dict[str, Any]) -> dict[str, Any]:
    generated_at = datetime.fromisoformat(payload["generated_at"])
    return {
        "snapshot_id": payload["snapshot_id"],
        "source_trade_date": date.fromisoformat(payload["source_trade_date"]),
        "generated_at": generated_at,
        "received_at": generated_at + timedelta(seconds=1),
        "receipt_sealed_at": generated_at + timedelta(minutes=1),
        "fast_state": payload["fast_state"],
        "model_version": payload["model_version"],
        "data_version": payload["data_version"],
        "content_hash": sha256_json(payload),
        "snapshot_json": json.dumps(payload),
    }


def _snapshot(source_trade_date: date, availability_date: date) -> dict[str, Any]:
    return _snapshot_values(
        _payload(
            source_trade_date=source_trade_date,
            availability_date=availability_date,
        )
    )


def _legacy_selection() -> dict[str, Any]:
    return {
        "model_leg_id": "leg-strict",
        "cutoff_ts": datetime(2026, 9, 1, 9, 40, tzinfo=TZ),
        "selection_reason": "ELIGIBLE",
        "selected_at": T_0915,
        "selected_snapshot_id": _snapshot(SOURCE_DATE, D1)["snapshot_id"],
        "selected_fast_state": "NORMAL",
        **_snapshot(SOURCE_DATE, D1),
    }


def _fallback_values() -> dict[str, None]:
    return {
        field: None
        for field in (
            "source_trade_date",
            "generated_at",
            "received_at",
            "receipt_sealed_at",
            "fast_state",
            "model_version",
            "data_version",
            "content_hash",
            "snapshot_json",
        )
    }


class _StrictPGConnection:
    def __init__(
        self,
        *,
        selections: dict[str, dict[str, Any]] | None = None,
        intent: str | None = None,
    ) -> None:
        self.selections = selections or {}
        self.intent = intent
        self.snapshots: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def transaction(self, *, isolation: str) -> _FakeTransaction:
        assert isolation == "serializable"
        return _FakeTransaction()

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        self.calls.append((sql, args))
        if "FOR UPDATE OF leg" in sql:
            return {"d1": D1, "d2": D2}
        if "FROM v20.exit_intents" in sql:
            return {"exit_intent_id": self.intent} if self.intent else None
        if "FROM v20.leg_mews_selection" in sql:
            selection = self.selections.get(args[0])
            return _Row(selection) if selection else None
        if "INSERT INTO v20.leg_mews_selection" in sql:
            return self._persist_selection(*args)
        if "UPDATE v20.leg_mews_selection" in sql or "ON CONFLICT (model_leg_id) DO UPDATE" in sql:
            return self._persist_selection(*args)
        if "FROM v20.mews_snapshots AS snapshot" in sql:
            return self._matching_snapshot(*args)
        raise AssertionError(f"unexpected strict SQL: {sql}")

    def _persist_selection(self, *args: Any) -> Any:
        model_leg_id = args[0]
        selected = args[1]
        selection = self.selections.setdefault(model_leg_id, {})
        selection.update(
            {
                "model_leg_id": model_leg_id,
                "selected_snapshot_id": selected,
                "selected_fast_state": args[2],
                "cutoff_ts": args[3],
                "selection_reason": args[4],
                "selected_at": T_D2_0945,
            }
        )
        snapshot = self.snapshots.get(selected) if selected else None
        selection.update(snapshot or _fallback_values())
        return _Row(selection)

    def _matching_snapshot(self, *args: Any) -> Any:
        source = args[0] if len(args) == 2 else args[2]
        availability = args[1] if len(args) == 2 else args[3]
        for snapshot in self.snapshots.values():
            payload = json.loads(snapshot["snapshot_json"])
            evidence_date = payload["evidence"]["signal_available_date"]
            if (
                snapshot["source_trade_date"] == source
                and evidence_date == availability.isoformat()
            ):
                return _Row(snapshot)
        return None


class _StrictRepository:
    schema = "v20"

    def __init__(self, connection: _StrictPGConnection) -> None:
        self.pool = _FakePool(connection)
        self.recorded: list[dict[str, Any]] = []

    async def assert_runtime_leader(self) -> None:
        return None

    async def record_mews_snapshot(self, payload: dict[str, Any]) -> str:
        self.recorded.append(dict(payload))
        values = _snapshot_values(payload)
        self.pool.connection.snapshots[values["snapshot_id"]] = values
        return values["snapshot_id"]

    async def enqueue_alert(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def seal_event(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def get_exit_scan_watermarks(
        self,
        *_args: Any,
        **_kwargs: Any,
    ) -> dict[str, str]:
        return {D1.isoformat(): "14:57", D2.isoformat(): "14:56"}


class _LocalMewsSource:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.requests: list[tuple[date, date]] = []

    async def fetch_snapshot(
        self,
        *,
        source_trade_date: date,
        availability_date: date,
    ) -> dict[str, Any]:
        self.requests.append((source_trade_date, availability_date))
        self.started.set()
        await self.release.wait()
        return _payload(
            source_trade_date=source_trade_date,
            availability_date=availability_date,
        )


class _FailingMewsSource:
    def __init__(self) -> None:
        self.calls = 0

    async def fetch_snapshot(self, **_kwargs: Any) -> dict[str, Any]:
        self.calls += 1
        raise RuntimeError("D2 calculation failed")


def _active_leg(*, exit_intent_id: str | None = None) -> ActiveModelLeg:
    return ActiveModelLeg(
        model_leg_id="leg-strict",
        model_batch_id="batch-strict",
        decision_id=None,
        signal_date=SOURCE_DATE,
        code="600000",
        stock_name="strict",
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
        exit_intent_id=exit_intent_id,
    )


def _make_service(
    monkeypatch: pytest.MonkeyPatch,
    connection: _StrictPGConnection,
    source: Any,
) -> tuple[V20Service, _StrictRepository]:
    repository = _StrictRepository(connection)
    service = _service(monkeypatch, repository)
    service._started = True
    service._repository_started = True
    service._mews_source = source
    service._clock = lambda: T_D2_0945
    service._mews_guard_store = V20MewsGuardStore(repository)
    return service, repository


def _fixed_calendar(calendar: tuple[date, ...]) -> Any:
    async def load(_current_date: date) -> tuple[date, ...]:
        return calendar

    return load


async def _evaluate_exit(
    monkeypatch: pytest.MonkeyPatch,
    service: V20Service,
    *,
    leg: ActiveModelLeg | None = None,
    calendar: tuple[date, ...] = CALENDAR,
    calendar_error: bool = False,
) -> list[str]:
    alerts: list[str] = []

    async def alert(*, code: str, **_kwargs: Any) -> None:
        alerts.append(code)

    async def bars(_record: Any, _now: datetime) -> list[Any]:
        return []

    monkeypatch.setattr(service, "_safe_alert", alert)
    monkeypatch.setattr(service, "_load_exit_bar_records", bars)
    if calendar_error:

        async def fail_calendar(_current_date: date) -> tuple[date, ...]:
            raise RuntimeError("calendar unavailable")

        monkeypatch.setattr(service, "_load_trade_calendar", fail_calendar)
    else:
        monkeypatch.setattr(
            service,
            "_load_trade_calendar",
            _fixed_calendar(calendar),
        )
    await service._evaluate_one_exit(
        leg or _active_leg(),
        T_D2_0945,
        detection_calendar_status="CONFIRMED_TRADING",
        detection_is_trading_day=True,
        next_trade_date=NEXT_DAY,
        calendar=calendar,
    )
    return alerts


def _snapshot_queries(
    connection: _StrictPGConnection,
) -> list[tuple[str, tuple[Any, ...]]]:
    return [
        call
        for call in connection.calls
        if "mews_snapshots AS snapshot" in call[0] and len(call[1]) in (2, 4)
    ]


async def test_first_exit_miss_calculates_exact_d2_then_freezes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _StrictPGConnection()
    source = _LocalMewsSource()
    source.release.set()
    service, repository = _make_service(monkeypatch, connection, source)

    await _evaluate_exit(monkeypatch, service)

    assert source.requests == [(D1, D2)]
    assert len(repository.recorded) == 1
    queries = _snapshot_queries(connection)
    assert queries[0][1] == (D1, D2)
    assert queries[-1][1] == (D2, D2_CUTOFF, D1, D2)
    selection = connection.selections["leg-strict"]
    assert selection["cutoff_ts"] == D2_CUTOFF
    assert selection["selection_reason"] == "ELIGIBLE_LATE_SAME_DAY"
    assert selection["source_trade_date"] == D1


async def test_failed_calculation_persists_fallback_and_restart_reads_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _StrictPGConnection()
    source = _FailingMewsSource()
    service, _repository = _make_service(monkeypatch, connection, source)

    first_alerts = await _evaluate_exit(monkeypatch, service)
    second_alerts = await _evaluate_exit(monkeypatch, service)

    assert source.calls == 1
    assert first_alerts == [
        "MEWS_CALCULATION_FAILED",
        "MEWS_UNAVAILABLE_FALLBACK_12",
    ]
    assert second_alerts == ["MEWS_UNAVAILABLE_FALLBACK_12"]
    selection = connection.selections["leg-strict"]
    assert selection["selection_reason"] == "MEWS_UNAVAILABLE_FALLBACK_12"
    assert selection["cutoff_ts"] == D2_CUTOFF
    exact_queries = [query for query in _snapshot_queries(connection) if len(query[1]) == 2]
    freeze_queries = [query for query in _snapshot_queries(connection) if len(query[1]) == 4]
    assert [query[1] for query in exact_queries] == [
        (D1, D2),
        (D1, D2),
    ]
    assert len(freeze_queries) == 1


@pytest.mark.parametrize(
    ("calendar", "calendar_error"),
    [((SOURCE_DATE, D2, NEXT_DAY), False), ((), False), ((), True)],
    ids=["calendar-omits-d1", "calendar-empty", "calendar-unavailable"],
)
async def test_d2_source_is_model_leg_d1_even_when_calendar_is_incomplete(
    monkeypatch: pytest.MonkeyPatch,
    calendar: tuple[date, ...],
    calendar_error: bool,
) -> None:
    connection = _StrictPGConnection()
    source = _LocalMewsSource()
    source.release.set()
    service, _repository = _make_service(monkeypatch, connection, source)
    service._mews_cached_for = D2
    service._mews_source_trade_date = SOURCE_DATE
    service._mews_snapshot_id = "mews-wrong-source"

    await _evaluate_exit(
        monkeypatch,
        service,
        calendar=calendar,
        calendar_error=calendar_error,
    )

    assert source.requests == [(D1, D2)]
    queries = _snapshot_queries(connection)
    assert queries[0][1] == (D1, D2)
    assert queries[-1][1] == (D2, D2_CUTOFF, D1, D2)
    assert connection.selections["leg-strict"]["source_trade_date"] == D1


async def test_concurrent_legs_share_one_calculation_and_freeze_separately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _StrictPGConnection()
    source = _LocalMewsSource()
    service, _repository = _make_service(monkeypatch, connection, source)
    legs = [replace(_active_leg(), model_leg_id=f"leg-{index}") for index in range(2)]
    evaluations = [
        asyncio.create_task(_evaluate_exit(monkeypatch, service, leg=leg)) for leg in legs
    ]

    await asyncio.wait_for(source.started.wait(), timeout=1)
    source.release.set()
    await asyncio.wait_for(asyncio.gather(*evaluations), timeout=1)

    assert source.requests == [(D1, D2)]
    assert set(connection.selections) == {"leg-0", "leg-1"}
    assert all(selection["source_trade_date"] == D1 for selection in connection.selections.values())


async def test_existing_exit_intent_skips_all_mews_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _StrictPGConnection()
    source = _FailingMewsSource()
    service, _repository = _make_service(monkeypatch, connection, source)

    await _evaluate_exit(
        monkeypatch,
        service,
        leg=_active_leg(exit_intent_id="exit-strict"),
    )

    assert source.calls == 0
    assert connection.calls == []


async def test_legacy_selection_with_db_intent_is_reused_without_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _StrictPGConnection(
        selections={"leg-strict": _legacy_selection()},
        intent="exit-strict",
    )
    source = _FailingMewsSource()
    service, _repository = _make_service(monkeypatch, connection, source)

    await _evaluate_exit(monkeypatch, service)

    assert source.calls == 0
    assert _snapshot_queries(connection) == []
    assert connection.selections["leg-strict"]["source_trade_date"] == SOURCE_DATE


async def test_legacy_selection_without_intent_is_upgraded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _StrictPGConnection(
        selections={"leg-strict": _legacy_selection()},
    )
    source = _LocalMewsSource()
    source.release.set()
    service, _repository = _make_service(monkeypatch, connection, source)

    await _evaluate_exit(monkeypatch, service)

    assert source.requests == [(D1, D2)]
    selection = connection.selections["leg-strict"]
    assert selection["cutoff_ts"] == D2_CUTOFF
    assert selection["source_trade_date"] == D1
    assert selection["selection_reason"] == "ELIGIBLE_LATE_SAME_DAY"


async def test_entry_selection_trigger_keeps_its_own_date_singleflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _StrictPGConnection()
    source = _LocalMewsSource()
    service, _repository = _make_service(monkeypatch, connection, source)
    monkeypatch.setattr(service, "_load_trade_calendar", _fixed_calendar(CALENDAR))
    triggers = [
        asyncio.create_task(service.ensure_mews_for_selection_trigger(T_D2_0910)) for _ in range(2)
    ]

    await asyncio.wait_for(source.started.wait(), timeout=1)
    source.release.set()
    assert await asyncio.wait_for(asyncio.gather(*triggers), timeout=1) == [True, True]
    assert source.requests == [(D1, D2)]
    entry_queries = _snapshot_queries(connection)
    assert len(entry_queries) == 1
    assert entry_queries[0][1] == (D1, D2)
