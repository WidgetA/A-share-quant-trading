from __future__ import annotations

import copy
import json
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.data.database.v20_mews_guard_store import V20MewsGuardStore
from src.data.database.v20_repository import (
    V20SemanticConflict,
    _model_batch_authorization_sql,
    sha256_json,
)

TZ = ZoneInfo("Asia/Shanghai")
SOURCE_DATE = date(2026, 8, 31)
D1 = date(2026, 9, 1)
D2 = date(2026, 9, 2)
CUTOFF = datetime(2026, 9, 1, 9, 40, tzinfo=TZ)
D2_CUTOFF = datetime(2026, 9, 2, 9, 40, tzinfo=TZ)
ON_TIME = datetime(2026, 9, 1, 9, 15, tzinfo=TZ)
LATE = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)
D2_ON_TIME = datetime(2026, 9, 2, 9, 15, tzinfo=TZ)


def _null_snapshot_row() -> dict[str, None]:
    return {
        "snapshot_id": None,
        "source_trade_date": None,
        "generated_at": None,
        "received_at": None,
        "receipt_sealed_at": None,
        "fast_state": None,
        "model_version": None,
        "data_version": None,
        "content_hash": None,
        "snapshot_json": None,
    }


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
    def __init__(self, connection: Any) -> None:
        self.connection = connection

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        _traceback: object,
    ) -> bool:
        if exc_type is None:
            self.connection.transaction_events.append(("commit", None))
        else:
            self.connection.transaction_events.append(("rollback", exc))
        return False


class _FakeConnection:
    def __init__(
        self,
        *,
        leg: Any = None,
        existing: Any = None,
        candidate: Any = None,
        intent: Any = None,
        insert_error: Exception | None = None,
        insert_errors: list[Exception] | None = None,
    ) -> None:
        self.leg = leg
        self.existing = existing
        self.candidate = candidate
        self.intent = intent
        self.insert_error = insert_error
        self.insert_errors = list(insert_errors or ())
        self.calls: list[tuple[str, str, tuple[Any, ...]]] = []
        self.transaction_isolation: str | None = None
        self.transaction_events: list[tuple[str, object]] = []

    def transaction(self, *, isolation: str) -> _FakeTransaction:
        self.transaction_isolation = isolation
        return _FakeTransaction(self)

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        self.calls.append(("fetchrow", sql, args))
        is_selection_write = (
            "UPDATE v20.leg_mews_selection" in sql or "INSERT INTO v20.leg_mews_selection" in sql
        ) and "RETURNING" in sql
        if is_selection_write:
            if self.insert_errors:
                raise self.insert_errors.pop(0)
            if self.insert_error is not None:
                raise self.insert_error
            return {
                "model_leg_id": args[0],
                "cutoff_ts": args[3],
                "selection_reason": args[4],
                "selected_at": args[3] + timedelta(hours=5),
                "selected_snapshot_id": args[1],
                "selected_fast_state": args[2],
            }
        if "FOR UPDATE OF leg" in sql:
            return self.leg
        if "FROM v20.exit_intents" in sql:
            return self.intent
        if "FROM v20.leg_mews_selection" in sql:
            return self.existing
        if "FROM v20.mews_snapshots" in sql:
            return self.candidate
        raise AssertionError(f"unexpected fetchrow SQL: {sql}")

    async def execute(self, sql: str, *args: Any) -> str:
        self.calls.append(("execute", sql, args))
        if "INSERT INTO v20.leg_mews_selection" in sql:
            if self.insert_error is not None:
                raise self.insert_error
            return "INSERT 0 1"
        raise AssertionError(f"unexpected execute SQL: {sql}")


class _AsyncpgLikeRecord:
    def __init__(self, values: dict[str, Any]) -> None:
        self.values = values

    def __getitem__(self, key: str) -> Any:
        return self.values[key]


def _payload(
    *,
    source_trade_date: date = SOURCE_DATE,
    generated_at: datetime = ON_TIME,
    availability_date: date = D1,
) -> dict[str, Any]:
    return {
        "snapshot_id": "mews-snapshot",
        "source_trade_date": source_trade_date.isoformat(),
        "generated_at": generated_at.isoformat(),
        "fast_state": "DANGER",
        "model_version": "mews_v2",
        "data_version": "d" * 64,
        "evidence": {
            "profile": "LOCAL_TUSHARE_MEWS_V2_0910_V1",
            "signal_available_date": availability_date.isoformat(),
        },
    }


def _row(
    *,
    payload: dict[str, Any] | None = None,
    generated_at: datetime = ON_TIME,
    sealed_at: datetime | None = ON_TIME + timedelta(minutes=1),
    source_trade_date: date = SOURCE_DATE,
    model_version: str = "mews_v2",
    content_hash: str | None = None,
) -> dict[str, Any]:
    resolved = payload if payload is not None else _payload(generated_at=generated_at)
    return {
        "snapshot_id": resolved["snapshot_id"],
        "source_trade_date": source_trade_date,
        "generated_at": generated_at,
        "received_at": generated_at + timedelta(seconds=1),
        "receipt_sealed_at": sealed_at,
        "fast_state": resolved["fast_state"],
        "model_version": model_version,
        "data_version": resolved["data_version"],
        "content_hash": content_hash or sha256_json(resolved),
        "snapshot_json": json.dumps(resolved),
    }


def _repository(connection: Any, schema: str = "v20") -> SimpleNamespace:
    return SimpleNamespace(schema=schema, pool=_FakePool(connection))


def test_constructor_validates_schema_and_pool() -> None:
    with pytest.raises(ValueError, match="invalid PostgreSQL schema identifier"):
        V20MewsGuardStore(SimpleNamespace(schema="v20; DROP SCHEMA v20", pool=object()))
    with pytest.raises(ValueError, match="repository pool is invalid"):
        V20MewsGuardStore(SimpleNamespace(schema="v20", pool=SimpleNamespace(acquire=None)))


@pytest.mark.parametrize(
    ("generated_at", "availability_date"),
    [(ON_TIME, D1), (LATE, D1)],
    ids=["on-time", "late-same-day"],
)
async def test_find_accepts_only_strictly_sealed_asyncpg_like_rows(
    generated_at: datetime,
    availability_date: date,
) -> None:
    connection = _FakeConnection(
        candidate=_AsyncpgLikeRecord(
            _row(
                generated_at=generated_at,
                sealed_at=generated_at + timedelta(minutes=1),
            )
        )
    )
    store = V20MewsGuardStore(_repository(connection))

    assert (
        await store.find_eligible_snapshot(
            source_trade_date=SOURCE_DATE,
            cutoff=CUTOFF,
            availability_date=availability_date,
        )
        == "mews-snapshot"
    )
    sql = connection.calls[0][1]
    assert "snapshot.receipt_sealed_at IS NOT NULL" in sql
    assert "snapshot.model_version='mews_v2'" in sql
    assert "LOCAL_TUSHARE_MEWS_V2_0910_V1" in sql
    assert "snapshot.generated_at <= snapshot.received_at" in sql
    assert "snapshot.received_at <= snapshot.receipt_sealed_at" in sql
    assert "timezone('Asia/Shanghai',snapshot.generated_at)::date::text" in sql
    assert "NULLS LAST" in sql
    assert connection.calls[0][2] == (SOURCE_DATE, CUTOFF, availability_date)


async def test_find_unsealed_or_wrong_candidate_is_excluded_by_sql() -> None:
    connection = _FakeConnection(candidate=None)
    store = V20MewsGuardStore(_repository(connection))

    assert (
        await store.find_eligible_snapshot(
            source_trade_date=SOURCE_DATE,
            cutoff=CUTOFF,
            availability_date=D1,
        )
        is None
    )
    sql = connection.calls[0][1]
    assert "snapshot.receipt_sealed_at IS NOT NULL" in sql
    assert "snapshot.source_trade_date=$1::date" in sql
    assert "timezone('Asia/Shanghai',snapshot.generated_at)::date=$3::date" in sql
    assert "timezone('Asia/Shanghai',snapshot.receipt_sealed_at)::date=$3::date" in sql
    ordering = sql[sql.index("ORDER BY") :]
    assert ordering.index("receipt_sealed_at DESC NULLS LAST") < ordering.index(
        "generated_at DESC NULLS LAST"
    )


@pytest.mark.parametrize(
    "mutator",
    [
        lambda row: row.__setitem__("receipt_sealed_at", None),
        lambda row: row.__setitem__("source_trade_date", SOURCE_DATE - timedelta(days=1)),
        lambda row: row.__setitem__("model_version", "mews_v1"),
        lambda row: row.__setitem__("content_hash", "0" * 64),
        lambda row: row.__setitem__("generated_at", ON_TIME.replace(tzinfo=None)),
        lambda row: row.__setitem__("receipt_sealed_at", ON_TIME - timedelta(minutes=1)),
        lambda row: row.__setitem__("received_at", ON_TIME + timedelta(minutes=2)),
    ],
    ids=[
        "unsealed",
        "wrong-source",
        "wrong-model",
        "wrong-hash",
        "naive-time",
        "sealed-before-generated",
        "received-after-sealed",
    ],
)
async def test_python_rechecks_fail_closed_on_dirty_rows(mutator: Any) -> None:
    row = _row(generated_at=ON_TIME, sealed_at=ON_TIME + timedelta(minutes=1))
    mutator(row)
    connection = _FakeConnection(candidate=row)
    store = V20MewsGuardStore(_repository(connection))

    with pytest.raises(V20SemanticConflict):
        await store.find_eligible_snapshot(
            source_trade_date=SOURCE_DATE,
            cutoff=CUTOFF,
            availability_date=D1,
        )


@pytest.mark.parametrize(
    "payload_mutator",
    [
        lambda payload: payload.__setitem__("snapshot_id", "different"),
        lambda payload: payload.__setitem__("source_trade_date", "2026-08-30"),
        lambda payload: payload.__setitem__("fast_state", "NORMAL"),
        lambda payload: payload.__setitem__("model_version", "mews_v1"),
        lambda payload: payload.__setitem__("data_version", "different"),
        lambda payload: payload.__setitem__(
            "generated_at", (LATE + timedelta(minutes=1)).isoformat()
        ),
        lambda payload: payload["evidence"].__setitem__("profile", "REMOTE_UNREVIEWED_PROFILE"),
        lambda payload: payload["evidence"].__setitem__("signal_available_date", "2026-08-31"),
    ],
)
async def test_payload_column_conflicts_fail_closed(payload_mutator: Any) -> None:
    baseline = _payload(generated_at=LATE)
    payload = copy.deepcopy(baseline)
    payload_mutator(payload)
    row = _row(
        payload=baseline,
        generated_at=LATE,
        sealed_at=LATE + timedelta(minutes=1),
        content_hash=sha256_json(payload),
    )
    row["snapshot_json"] = json.dumps(payload)
    connection = _FakeConnection(candidate=row)
    store = V20MewsGuardStore(_repository(connection))

    with pytest.raises(V20SemanticConflict):
        await store.find_eligible_snapshot(
            source_trade_date=SOURCE_DATE,
            cutoff=CUTOFF,
            availability_date=D1,
        )


async def test_unsupported_fast_state_fails_closed_even_with_consistent_payload() -> None:
    payload = _payload(generated_at=ON_TIME)
    payload["fast_state"] = "UNKNOWN"
    row = _row(
        generated_at=ON_TIME,
        sealed_at=ON_TIME + timedelta(minutes=1),
        content_hash=sha256_json(payload),
    )
    row["fast_state"] = "UNKNOWN"
    row["snapshot_json"] = json.dumps(payload)
    connection = _FakeConnection(candidate=row)
    store = V20MewsGuardStore(_repository(connection))

    with pytest.raises(V20SemanticConflict, match="fast_state is unsupported"):
        await store.find_eligible_snapshot(
            source_trade_date=SOURCE_DATE,
            cutoff=CUTOFF,
            availability_date=D1,
        )


async def test_select_freezes_authorized_leg_exactly_once() -> None:
    connection = _FakeConnection(
        leg={"d1": D1},
        candidate=_row(generated_at=ON_TIME, sealed_at=ON_TIME + timedelta(minutes=1)),
    )
    store = V20MewsGuardStore(_repository(connection))

    result = await store.select_and_freeze_for_leg(
        "leg-1",
        d1=D1,
        cutoff=CUTOFF,
        late_source_trade_date=SOURCE_DATE,
        late_availability_date=D1,
    )

    assert result == ("mews-snapshot", "DANGER", "ELIGIBLE")
    assert connection.transaction_isolation == "serializable"
    authorization = _model_batch_authorization_sql("v20")
    assert authorization in connection.calls[0][1]
    assert "FOR UPDATE OF leg" in connection.calls[0][1]
    candidate_call = next(
        call for call in connection.calls if "FROM v20.mews_snapshots AS snapshot" in call[1]
    )
    assert candidate_call[2] == (D1, CUTOFF, SOURCE_DATE, D1)
    insert_call = next(
        call
        for call in connection.calls
        if call[0] == "fetchrow" and "INSERT INTO v20.leg_mews_selection" in call[1]
    )
    assert insert_call[2] == (
        "leg-1",
        "mews-snapshot",
        "DANGER",
        CUTOFF,
        "ELIGIBLE",
    )


async def test_existing_selection_with_same_cutoff_is_returned_without_rewrite() -> None:
    existing = {
        "model_leg_id": "leg-1",
        "cutoff_ts": CUTOFF,
        "selection_reason": "MEWS_UNAVAILABLE_FALLBACK_12",
        "selected_at": ON_TIME + timedelta(minutes=2),
        "selected_snapshot_id": None,
        "selected_fast_state": None,
        **_null_snapshot_row(),
    }
    connection = _FakeConnection(leg={"d1": D1}, existing=existing)
    store = V20MewsGuardStore(_repository(connection))

    assert await store.select_and_freeze_for_leg(
        "leg-1",
        d1=D1,
        cutoff=CUTOFF,
        late_source_trade_date=SOURCE_DATE,
        late_availability_date=D1,
    ) == (None, None, "MEWS_UNAVAILABLE_FALLBACK_12")
    assert not any(call[0] == "execute" for call in connection.calls)
    assert not any("FROM v20.mews_snapshots" in call[1] for call in connection.calls)


async def test_select_freeze_and_load_returns_verified_selected_record() -> None:
    connection = _FakeConnection(
        leg={"d1": D1},
        candidate=_AsyncpgLikeRecord(
            _row(generated_at=LATE, sealed_at=LATE + timedelta(minutes=1))
        ),
    )
    store = V20MewsGuardStore(_repository(connection))

    record = await store.select_freeze_and_load(
        "leg-1",
        d1=D1,
        cutoff=CUTOFF,
        late_source_trade_date=SOURCE_DATE,
        late_availability_date=D1,
    )

    assert record.model_leg_id == "leg-1"
    assert record.d1 == D1
    assert record.cutoff_ts == CUTOFF
    assert record.snapshot_id == "mews-snapshot"
    assert record.source_trade_date == SOURCE_DATE
    assert record.generated_at == LATE
    assert record.fast_state == "DANGER"
    assert record.model_version == "mews_v2"
    assert record.payload["snapshot_id"] == "mews-snapshot"
    assert record.selection_reason == "ELIGIBLE_LATE_SAME_DAY"
    assert not any(
        call[0] == "fetchrow" and "load_selected_mews_for_leg" in call[1]
        for call in connection.calls
    )


async def test_existing_snapshot_selection_is_revalidated_in_transaction() -> None:
    snapshot = _row(generated_at=ON_TIME, sealed_at=ON_TIME + timedelta(minutes=1))
    existing = {
        "model_leg_id": "leg-1",
        "cutoff_ts": CUTOFF,
        "selection_reason": "ELIGIBLE",
        "selected_at": ON_TIME + timedelta(minutes=2),
        "selected_snapshot_id": "mews-snapshot",
        "selected_fast_state": "DANGER",
        **snapshot,
    }
    connection = _FakeConnection(leg={"d1": D1}, existing=existing)
    store = V20MewsGuardStore(_repository(connection))

    record = await store.select_freeze_and_load(
        "leg-1",
        d1=D1,
        cutoff=CUTOFF,
        late_source_trade_date=SOURCE_DATE,
        late_availability_date=D1,
    )
    assert record.snapshot_id == "mews-snapshot"
    assert record.selection_reason == "ELIGIBLE"
    assert not any(call[0] == "fetchrow" and "INSERT INTO" in call[1] for call in connection.calls)


async def test_existing_illegal_freeze_fails_closed_without_rewrite() -> None:
    invalid_snapshot = _row(generated_at=ON_TIME, sealed_at=None)
    existing = {
        "model_leg_id": "leg-1",
        "cutoff_ts": CUTOFF,
        "selection_reason": "ELIGIBLE",
        "selected_at": ON_TIME + timedelta(minutes=2),
        "selected_snapshot_id": "mews-snapshot",
        "selected_fast_state": "DANGER",
        **invalid_snapshot,
    }
    connection = _FakeConnection(
        leg={"d1": D1},
        existing=existing,
        candidate=_row(generated_at=ON_TIME, sealed_at=ON_TIME + timedelta(minutes=1)),
    )
    store = V20MewsGuardStore(_repository(connection))

    with pytest.raises(V20SemanticConflict):
        await store.select_freeze_and_load(
            "leg-1",
            d1=D1,
            cutoff=CUTOFF,
            late_source_trade_date=SOURCE_DATE,
            late_availability_date=D1,
        )
    assert not any("INSERT INTO" in call[1] for call in connection.calls)
    assert not any(call[1].lstrip().upper().startswith("UPDATE") for call in connection.calls)


async def test_existing_late_freeze_requires_exact_late_source_from_call() -> None:
    snapshot = _row(generated_at=LATE, sealed_at=LATE + timedelta(minutes=1))
    existing = {
        "model_leg_id": "leg-1",
        "cutoff_ts": CUTOFF,
        "selection_reason": "ELIGIBLE_LATE_SAME_DAY",
        "selected_at": LATE + timedelta(minutes=2),
        "selected_snapshot_id": "mews-snapshot",
        "selected_fast_state": "DANGER",
        **snapshot,
    }
    connection = _FakeConnection(leg={"d1": D1}, existing=existing)
    store = V20MewsGuardStore(_repository(connection))

    with pytest.raises(V20SemanticConflict, match="exact late source"):
        await store.select_freeze_and_load(
            "leg-1",
            d1=D1,
            cutoff=CUTOFF,
        )
    assert not any("INSERT INTO" in call[1] for call in connection.calls)


async def test_existing_late_freeze_rejects_wrong_supplied_late_source() -> None:
    snapshot = _row(generated_at=LATE, sealed_at=LATE + timedelta(minutes=1))
    existing = {
        "model_leg_id": "leg-1",
        "cutoff_ts": CUTOFF,
        "selection_reason": "ELIGIBLE_LATE_SAME_DAY",
        "selected_at": LATE + timedelta(minutes=2),
        "selected_snapshot_id": "mews-snapshot",
        "selected_fast_state": "DANGER",
        **snapshot,
    }
    connection = _FakeConnection(leg={"d1": D1}, existing=existing)
    store = V20MewsGuardStore(_repository(connection))

    with pytest.raises(V20SemanticConflict, match="exact late source"):
        await store.select_freeze_and_load(
            "leg-1",
            d1=D1,
            cutoff=CUTOFF,
            late_source_trade_date=SOURCE_DATE - timedelta(days=1),
            late_availability_date=D1,
        )


async def test_existing_selection_selected_at_cannot_precede_sealed_receipt() -> None:
    snapshot = _row(generated_at=ON_TIME, sealed_at=ON_TIME + timedelta(minutes=1))
    existing = {
        "model_leg_id": "leg-1",
        "cutoff_ts": CUTOFF,
        "selection_reason": "ELIGIBLE",
        "selected_at": ON_TIME,
        "selected_snapshot_id": "mews-snapshot",
        "selected_fast_state": "DANGER",
        **snapshot,
    }
    connection = _FakeConnection(leg={"d1": D1}, existing=existing)
    store = V20MewsGuardStore(_repository(connection))

    with pytest.raises(V20SemanticConflict, match="selected_at precedes"):
        await store.select_freeze_and_load(
            "leg-1",
            d1=D1,
            cutoff=CUTOFF,
            late_source_trade_date=SOURCE_DATE,
            late_availability_date=D1,
        )


async def test_different_cutoff_does_not_flip_an_existing_selection() -> None:
    existing = {
        "model_leg_id": "leg-1",
        "cutoff_ts": CUTOFF,
        "selection_reason": "ELIGIBLE",
        "selected_at": ON_TIME + timedelta(minutes=2),
        "selected_snapshot_id": "old-snapshot",
        "selected_fast_state": "NORMAL",
        **_null_snapshot_row(),
    }
    connection = _FakeConnection(leg={"d1": D1}, existing=existing)
    store = V20MewsGuardStore(_repository(connection))

    with pytest.raises(V20SemanticConflict, match="different cutoff"):
        await store.select_and_freeze_for_leg(
            "leg-1",
            d1=D1,
            cutoff=CUTOFF + timedelta(minutes=1),
            late_source_trade_date=SOURCE_DATE,
            late_availability_date=D1,
        )


async def test_no_candidate_writes_null_fallback() -> None:
    connection = _FakeConnection(leg={"d1": D1}, candidate=None)
    store = V20MewsGuardStore(_repository(connection))

    assert await store.select_and_freeze_for_leg(
        "leg-1",
        d1=D1,
        cutoff=CUTOFF,
        late_source_trade_date=SOURCE_DATE,
        late_availability_date=D1,
    ) == (None, None, "MEWS_UNAVAILABLE_FALLBACK_12")
    insert_call = next(
        call
        for call in connection.calls
        if call[0] == "fetchrow" and "INSERT INTO v20.leg_mews_selection" in call[1]
    )
    assert insert_call[2] == (
        "leg-1",
        None,
        None,
        CUTOFF,
        "MEWS_UNAVAILABLE_FALLBACK_12",
    )


async def test_insert_failure_propagates_from_serializable_transaction() -> None:
    failure = RuntimeError("serialization failure")
    connection = _FakeConnection(
        leg={"d1": D1},
        candidate=_row(generated_at=ON_TIME, sealed_at=ON_TIME + timedelta(minutes=1)),
        insert_error=failure,
    )
    store = V20MewsGuardStore(_repository(connection))

    with pytest.raises(RuntimeError, match="serialization failure"):
        await store.select_and_freeze_for_leg(
            "leg-1",
            d1=D1,
            cutoff=CUTOFF,
            late_source_trade_date=SOURCE_DATE,
            late_availability_date=D1,
        )
    assert connection.transaction_events == [("rollback", failure)]


@pytest.mark.parametrize("pgcode", ["40001", "40P01", "23505"])
async def test_retryable_transaction_failure_rolls_back_and_retries(pgcode: str) -> None:
    class RetryableInsertError(RuntimeError):
        pass

    failure = RetryableInsertError("database transaction race")
    failure.pgcode = pgcode  # type: ignore[attr-defined]
    connection = _FakeConnection(
        leg={"d1": D1},
        candidate=_row(generated_at=ON_TIME, sealed_at=ON_TIME + timedelta(minutes=1)),
        insert_errors=[failure],
    )
    store = V20MewsGuardStore(_repository(connection))

    record = await store.select_freeze_and_load(
        "leg-1",
        d1=D1,
        cutoff=CUTOFF,
        late_source_trade_date=SOURCE_DATE,
        late_availability_date=D1,
    )

    assert record.snapshot_id == "mews-snapshot"
    assert connection.transaction_events == [("rollback", failure), ("commit", None)]
    assert (
        len([call for call in connection.calls if "INSERT INTO v20.leg_mews_selection" in call[1]])
        == 2
    )


async def test_d2_selection_uses_exact_predecessor_source_and_d2_availability() -> None:
    payload = _payload(
        source_trade_date=D1,
        generated_at=D2_ON_TIME,
        availability_date=D2,
    )
    candidate = _row(
        source_trade_date=D1,
        generated_at=D2_ON_TIME,
        sealed_at=D2_ON_TIME + timedelta(minutes=1),
        payload=payload,
    )
    connection = _FakeConnection(leg={"d1": D1, "d2": D2}, candidate=candidate)
    store = V20MewsGuardStore(_repository(connection))

    record = await store.select_freeze_and_load(
        "leg-1",
        d1=D1,
        cutoff=D2_CUTOFF,
        late_source_trade_date=D1,
        late_availability_date=D2,
        evaluation_date=D2,
    )

    assert record.snapshot_id == "mews-snapshot"
    assert record.source_trade_date == D1
    assert record.selection_reason == "ELIGIBLE"
    candidate_call = next(
        call for call in connection.calls if "FROM v20.mews_snapshots AS snapshot" in call[1]
    )
    assert candidate_call[2] == (D2, D2_CUTOFF, D1, D2)
    assert connection.calls[0][1].strip().endswith("FOR UPDATE OF leg")


async def test_d2_selection_rejects_older_d0_source_snapshot() -> None:
    payload = _payload(
        source_trade_date=SOURCE_DATE,
        generated_at=D2_ON_TIME,
        availability_date=D2,
    )
    candidate = _row(
        source_trade_date=SOURCE_DATE,
        generated_at=D2_ON_TIME,
        sealed_at=D2_ON_TIME + timedelta(minutes=1),
        payload=payload,
    )
    connection = _FakeConnection(leg={"d1": D1, "d2": D2}, candidate=candidate)
    store = V20MewsGuardStore(_repository(connection))

    with pytest.raises(V20SemanticConflict, match="source_trade_date differs from request"):
        await store.select_freeze_and_load(
            "leg-1",
            d1=D1,
            cutoff=D2_CUTOFF,
            late_source_trade_date=D1,
            late_availability_date=D2,
            evaluation_date=D2,
        )
    assert not any("INSERT INTO" in call[1] for call in connection.calls)


async def test_d2_missing_candidate_persists_fallback_exactly_once() -> None:
    connection = _FakeConnection(leg={"d1": D1, "d2": D2}, candidate=None)
    store = V20MewsGuardStore(_repository(connection))

    record = await store.select_freeze_and_load(
        "leg-1",
        d1=D1,
        cutoff=D2_CUTOFF,
        late_source_trade_date=D1,
        late_availability_date=D2,
        evaluation_date=D2,
    )

    assert record.snapshot_id is None
    assert record.selection_reason == "MEWS_UNAVAILABLE_FALLBACK_12"
    insert_calls = [
        call for call in connection.calls if "INSERT INTO v20.leg_mews_selection" in call[1]
    ]
    assert len(insert_calls) == 1
    assert "ON CONFLICT" not in insert_calls[0][1]
    assert insert_calls[0][2] == (
        "leg-1",
        None,
        None,
        D2_CUTOFF,
        "MEWS_UNAVAILABLE_FALLBACK_12",
    )


async def test_load_frozen_returns_current_d2_fallback_after_restart() -> None:
    existing = {
        "model_leg_id": "leg-1",
        "cutoff_ts": D2_CUTOFF,
        "selection_reason": "MEWS_UNAVAILABLE_FALLBACK_12",
        "selected_at": D2_ON_TIME + timedelta(minutes=2),
        "selected_snapshot_id": None,
        "selected_fast_state": None,
        **_null_snapshot_row(),
    }
    connection = _FakeConnection(
        leg={"d1": D1, "d2": D2},
        existing=existing,
        intent={"exit_intent_id": "intent-1"},
    )
    store = V20MewsGuardStore(_repository(connection))

    record = await store.load_frozen_for_leg(
        "leg-1",
        d1=D1,
        cutoff=D2_CUTOFF,
        late_source_trade_date=D1,
        evaluation_date=D2,
    )

    assert record is not None
    assert record.selection_reason == "MEWS_UNAVAILABLE_FALLBACK_12"
    assert not any("FROM v20.mews_snapshots" in call[1] for call in connection.calls)
    assert not any("INSERT INTO" in call[1] or "UPDATE v20" in call[1] for call in connection.calls)


async def test_d2_upgrades_legacy_selection_without_intent_using_explicit_update() -> None:
    payload = _payload(
        source_trade_date=D1,
        generated_at=D2_ON_TIME,
        availability_date=D2,
    )
    candidate = _row(
        source_trade_date=D1,
        generated_at=D2_ON_TIME,
        sealed_at=D2_ON_TIME + timedelta(minutes=1),
        payload=payload,
    )
    existing = {
        "model_leg_id": "leg-1",
        "cutoff_ts": CUTOFF,
        "selection_reason": "MEWS_UNAVAILABLE_FALLBACK_12",
        "selected_at": ON_TIME + timedelta(minutes=2),
        "selected_snapshot_id": None,
        "selected_fast_state": None,
        **_null_snapshot_row(),
    }
    connection = _FakeConnection(
        leg={"d1": D1, "d2": D2},
        existing=existing,
        candidate=candidate,
    )
    store = V20MewsGuardStore(_repository(connection))

    record = await store.select_freeze_and_load(
        "leg-1",
        d1=D1,
        cutoff=D2_CUTOFF,
        late_source_trade_date=D1,
        late_availability_date=D2,
        evaluation_date=D2,
    )

    assert record.snapshot_id == "mews-snapshot"
    assert record.cutoff_ts == D2_CUTOFF
    update_calls = [call for call in connection.calls if "UPDATE v20.leg_mews_selection" in call[1]]
    assert len(update_calls) == 1
    assert "ON CONFLICT" not in update_calls[0][1]
    assert update_calls[0][2] == ("leg-1", "mews-snapshot", "DANGER", D2_CUTOFF, "ELIGIBLE")
    assert not any("INSERT INTO" in call[1] for call in connection.calls)


async def test_d2_preserves_legacy_selection_when_intent_exists() -> None:
    snapshot = _row(generated_at=ON_TIME, sealed_at=ON_TIME + timedelta(minutes=1))
    existing = {
        "model_leg_id": "leg-1",
        "cutoff_ts": CUTOFF,
        "selection_reason": "ELIGIBLE",
        "selected_at": ON_TIME + timedelta(minutes=2),
        "selected_snapshot_id": "mews-snapshot",
        "selected_fast_state": "DANGER",
        **snapshot,
    }
    connection = _FakeConnection(
        leg={"d1": D1, "d2": D2},
        existing=existing,
        intent={"exit_intent_id": "intent-1"},
    )
    store = V20MewsGuardStore(_repository(connection))

    record = await store.select_freeze_and_load(
        "leg-1",
        d1=D1,
        cutoff=D2_CUTOFF,
        late_source_trade_date=D1,
        late_availability_date=D2,
        evaluation_date=D2,
    )

    assert record.cutoff_ts == CUTOFF
    assert record.selection_reason == "ELIGIBLE"
    assert not any("FROM v20.mews_snapshots" in call[1] for call in connection.calls)
    assert not any("INSERT INTO" in call[1] or "UPDATE v20" in call[1] for call in connection.calls)


async def test_load_frozen_returns_none_for_legacy_selection_without_intent() -> None:
    existing = {
        "model_leg_id": "leg-1",
        "cutoff_ts": CUTOFF,
        "selection_reason": "MEWS_UNAVAILABLE_FALLBACK_12",
        "selected_at": ON_TIME + timedelta(minutes=2),
        "selected_snapshot_id": None,
        "selected_fast_state": None,
        **_null_snapshot_row(),
    }
    connection = _FakeConnection(
        leg={"d1": D1, "d2": D2},
        existing=existing,
    )
    store = V20MewsGuardStore(_repository(connection))

    assert (
        await store.load_frozen_for_leg(
            "leg-1",
            d1=D1,
            cutoff=D2_CUTOFF,
            late_source_trade_date=D1,
            evaluation_date=D2,
        )
        is None
    )


async def test_legacy_mode_never_queries_exit_intent_before_conflict_or_rewrite() -> None:
    connection = _FakeConnection(
        leg={"d1": D1},
        existing={
            "model_leg_id": "leg-1",
            "cutoff_ts": CUTOFF,
            "selection_reason": "MEWS_UNAVAILABLE_FALLBACK_12",
            "selected_at": ON_TIME + timedelta(minutes=2),
            "selected_snapshot_id": None,
            "selected_fast_state": None,
            **_null_snapshot_row(),
        },
    )
    store = V20MewsGuardStore(_repository(connection))

    record = await store.select_freeze_and_load(
        "leg-1",
        d1=D1,
        cutoff=CUTOFF,
        late_source_trade_date=SOURCE_DATE,
        late_availability_date=D1,
    )

    assert record.cutoff_ts == CUTOFF
    assert not any("FROM v20.exit_intents" in call[1] for call in connection.calls)
    assert not any("FROM v20.mews_snapshots" in call[1] for call in connection.calls)


@pytest.mark.parametrize("pgcode", ["40001", "40P01", "23505"])
async def test_d2_retry_relocks_and_rechecks_intent_selection_and_candidate(pgcode: str) -> None:
    class RetryableInsertError(RuntimeError):
        pass

    failure = RetryableInsertError("first writer race")
    failure.pgcode = pgcode  # type: ignore[attr-defined]
    payload = _payload(
        source_trade_date=D1,
        generated_at=D2_ON_TIME,
        availability_date=D2,
    )
    candidate = _row(
        source_trade_date=D1,
        generated_at=D2_ON_TIME,
        sealed_at=D2_ON_TIME + timedelta(minutes=1),
        payload=payload,
    )
    connection = _FakeConnection(
        leg={"d1": D1, "d2": D2},
        candidate=candidate,
        insert_errors=[failure],
    )
    store = V20MewsGuardStore(_repository(connection))

    record = await store.select_freeze_and_load(
        "leg-1",
        d1=D1,
        cutoff=D2_CUTOFF,
        late_source_trade_date=D1,
        late_availability_date=D2,
        evaluation_date=D2,
    )

    assert record.snapshot_id == "mews-snapshot"
    assert connection.transaction_events == [("rollback", failure), ("commit", None)]
    for attempt in range(2):
        offset = attempt * 5
        assert "FOR UPDATE OF leg" in connection.calls[offset][1]
        assert "FROM v20.leg_mews_selection" in connection.calls[offset + 1][1]
        assert "FROM v20.exit_intents" in connection.calls[offset + 2][1]
