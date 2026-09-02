from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest

from src.data.database.v16_canonical_artifact_store import (
    SNAPSHOT_TYPE,
    V16CanonicalArtifactStore,
)
from src.data.database.v20_repository import (
    V20SemanticConflict,
    canonical_json,
    sha256_json,
)

TRADE_DATE = date(2026, 8, 31)
OTHER_DATE = TRADE_DATE + timedelta(days=1)
PAYLOAD = {
    "schema_version": "v16-canonical-master/v1",
    "universe": ["000001", "600000"],
    "raw_evidence_codes": ["000001", "000002", "600000"],
    "nested": {"count": 2, "values": [1, 2.5, None, True]},
}


class _AsyncContext:
    def __init__(self, value: Any) -> None:
        self.value = value

    async def __aenter__(self) -> Any:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


class _UniqueViolation(RuntimeError):
    pass


class _FakeConnection:
    def __init__(self, store: dict[str, dict[str, Any]]) -> None:
        self.store = store
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.transactions = 0

    def transaction(self) -> _AsyncContext[None]:
        self.transactions += 1
        return _AsyncContext(None)

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        if (
            "INSERT INTO v20.input_snapshots" not in sql
            or "ON CONFLICT (snapshot_id) DO NOTHING" not in sql
        ):
            raise AssertionError("unexpected SQL statement")
        snapshot_id = str(args[0])
        if snapshot_id not in self.store:
            for existing in self.store.values():
                duplicate_content = (
                    existing["snapshot_type"] == args[1]
                    and existing["trade_date"] == args[2]
                    and existing["snapshot_hash"] == args[3]
                )
                duplicate_trade_date = (
                    existing["snapshot_type"] == args[1] and existing["trade_date"] == args[2]
                )
                if duplicate_content or duplicate_trade_date:
                    raise _UniqueViolation("duplicate V16 canonical master trade date")
            self.store[snapshot_id] = {
                "snapshot_id": snapshot_id,
                "snapshot_type": args[1],
                "trade_date": args[2],
                "snapshot_hash": args[3],
                "snapshot_json": args[4],
                "first_received_at": datetime(2026, 8, 31, 9, 39, tzinfo=timezone.utc),
            }
        return "OK"

    async def fetchrow(self, sql: str, *args: object) -> dict[str, Any] | None:
        self.fetchrow_calls.append((sql, args))
        selected_columns = (
            "snapshot_id,snapshot_type,trade_date,snapshot_hash,snapshot_json,first_received_at"
        )
        if f"SELECT {selected_columns}" not in sql:
            raise AssertionError("unexpected read SQL statement")
        return self.store.get(str(args[0]))


class _FakePool:
    def __init__(self) -> None:
        self.store: dict[str, dict[str, Any]] = {}
        self.connections: list[_FakeConnection] = []

    def acquire(self) -> _AsyncContext[_FakeConnection]:
        connection = _FakeConnection(self.store)
        self.connections.append(connection)
        return _AsyncContext(connection)


class _FakeRepository:
    def __init__(self, schema: str = "v20") -> None:
        self.schema = schema
        self.pool = _FakePool()


class _AsyncpgLikeRow:
    def __init__(self, values: dict[str, Any]) -> None:
        self._values = values

    def __getitem__(self, key: str) -> Any:
        return self._values[key]


def _store() -> tuple[V16CanonicalArtifactStore, _FakePool]:
    repository = _FakeRepository()
    return V16CanonicalArtifactStore(repository), repository.pool


async def test_save_and_load_accept_asyncpg_like_non_mapping_row() -> None:
    store, pool = _store()
    connection = pool.connections[0] if pool.connections else _FakeConnection(pool.store)
    pool.connections.append(connection)
    original_fetchrow = connection.fetchrow

    async def fetchrow_asyncpg_like(sql: str, *args: object) -> _AsyncpgLikeRow | None:
        row = await original_fetchrow(sql, *args)
        return _AsyncpgLikeRow(row) if row is not None else None

    connection.fetchrow = fetchrow_asyncpg_like

    record = await store.save_once(
        PAYLOAD,
        event=SNAPSHOT_TYPE,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )
    loaded = await store.load(
        event=SNAPSHOT_TYPE,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )

    assert loaded == record
    assert loaded is not None
    assert loaded.payload == PAYLOAD
    assert loaded.first_received_at.tzinfo is not None


async def test_save_and_load_roundtrip_with_durable_slot_and_isolated_payload() -> None:
    store, pool = _store()

    record = await store.save_once(
        PAYLOAD,
        event=SNAPSHOT_TYPE,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )
    loaded = await store.load(
        event=SNAPSHOT_TYPE,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )

    assert record.snapshot_id == loaded.snapshot_id
    assert record.snapshot_type == SNAPSHOT_TYPE == "V16_CANONICAL_MASTER_V1"
    assert record.trade_date == TRADE_DATE
    assert record.snapshot_hash == sha256_json(PAYLOAD)
    assert record.payload == PAYLOAD
    assert record.first_received_at.tzinfo is not None

    isolated = record.payload
    isolated["nested"]["values"].append(999)
    isolated["universe"].append("999999")
    assert record.payload == PAYLOAD
    assert loaded is not record
    assert loaded.payload == PAYLOAD

    connection = pool.connections[0]
    assert connection.transactions == 1
    assert len(pool.connections) == 2
    insert_sql, insert_args = connection.execute_calls[0]
    assert insert_sql.count("v20.input_snapshots") == 1
    assert insert_args[:4] == (
        record.snapshot_id,
        SNAPSHOT_TYPE,
        TRADE_DATE,
        sha256_json(PAYLOAD),
    )
    assert json.loads(str(insert_args[4])) == PAYLOAD
    select_sql, select_args = connection.fetchrow_calls[0]
    assert "FROM v20.input_snapshots" in select_sql
    assert select_args == (record.snapshot_id,)


async def test_save_once_is_idempotent_for_same_slot_and_content() -> None:
    store, pool = _store()

    first = await store.save_once(
        PAYLOAD,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )
    second = await store.save_once(
        PAYLOAD,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )

    assert first == second
    assert len(pool.store) == 1
    assert len(pool.connections) == 2
    assert all(connection.transactions == 1 for connection in pool.connections)


async def test_store_hash_binds_the_complete_raw_evidence_union() -> None:
    store, _pool = _store()
    first = await store.save_once(
        PAYLOAD,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )

    assert first.payload["raw_evidence_codes"] == ["000001", "000002", "600000"]
    with pytest.raises(V20SemanticConflict, match="slot collision"):
        await store.save_once(
            {**PAYLOAD, "raw_evidence_codes": ["000001", "600000"]},
            official_stream_id="official-stream",
            trade_date=TRADE_DATE,
        )


@pytest.mark.parametrize("conflicting_stream", ["official-stream", "other-stream"])
async def test_save_once_rejects_same_trade_date_with_different_content(
    conflicting_stream: str,
) -> None:
    store, pool = _store()
    first = await store.save_once(
        PAYLOAD,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )

    with pytest.raises(V20SemanticConflict, match="canonical artifact slot collision"):
        await store.save_once(
            {**PAYLOAD, "universe": ["999999"]},
            official_stream_id=conflicting_stream,
            trade_date=TRADE_DATE,
        )

    retained = pool.store[first.snapshot_id]
    assert retained["snapshot_hash"] == sha256_json(PAYLOAD)
    assert retained["snapshot_json"] == canonical_json(PAYLOAD)


async def test_load_returns_none_when_slot_is_missing() -> None:
    store, _pool = _store()

    assert (
        await store.load(
            official_stream_id="official-stream",
            trade_date=TRADE_DATE,
        )
        is None
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("snapshot_hash", "f" * 64),
        ("snapshot_type", "V16"),
        ("trade_date", OTHER_DATE),
        ("snapshot_json", "{changed:true}"),
        ("first_received_at", datetime(2026, 8, 31, 9, 39)),
        ("first_received_at", "2026-08-31T09:39:00Z"),
    ],
)
async def test_load_fails_closed_for_corrupted_or_mismatched_rows(
    field: str, value: object
) -> None:
    store, pool = _store()
    record = await store.save_once(
        PAYLOAD,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )
    pool.store[record.snapshot_id][field] = value

    with pytest.raises(V20SemanticConflict, match="canonical artifact row is invalid"):
        await store.load(
            official_stream_id="official-stream",
            trade_date=TRADE_DATE,
        )


async def test_stream_and_date_are_distinct_slots() -> None:
    store, pool = _store()

    stream_a = await store.save_once(
        PAYLOAD,
        official_stream_id="stream-a",
        trade_date=TRADE_DATE,
    )
    stream_b = await store.save_once(
        PAYLOAD,
        official_stream_id="stream-b",
        trade_date=TRADE_DATE,
    )
    next_day = await store.save_once(
        PAYLOAD,
        official_stream_id="stream-a",
        trade_date=OTHER_DATE,
    )

    assert stream_a.snapshot_id == stream_b.snapshot_id
    assert stream_a == stream_b
    assert stream_a.snapshot_id != next_day.snapshot_id
    assert stream_a.snapshot_hash == next_day.snapshot_hash
    assert len(pool.store) == 2


async def test_concurrent_same_slot_and_content_saves_are_idempotent() -> None:
    store, pool = _store()

    records = await asyncio.gather(
        *(
            store.save_once(
                PAYLOAD,
                official_stream_id="official-stream",
                trade_date=TRADE_DATE,
            )
            for _ in range(12)
        )
    )

    assert len(set(records)) == 1
    assert len(pool.store) == 1
    assert len(pool.connections) == 12
    assert all(connection.transactions == 1 for connection in pool.connections)


@pytest.mark.parametrize(
    ("event", "stream", "trade_date", "payload"),
    [
        ("V16", "official", TRADE_DATE, PAYLOAD),
        (SNAPSHOT_TYPE, "", TRADE_DATE, PAYLOAD),
        (SNAPSHOT_TYPE, 123, TRADE_DATE, PAYLOAD),
        (SNAPSHOT_TYPE, "official", datetime(2026, 8, 31), PAYLOAD),
        (SNAPSHOT_TYPE, "official", TRADE_DATE, float("nan")),
    ],
)
async def test_inputs_are_strictly_validated(
    event: object, stream: object, trade_date: object, payload: object
) -> None:
    store, _pool = _store()

    with pytest.raises((TypeError, ValueError)):
        await store.save_once(
            payload,
            event=event,
            official_stream_id=stream,
            trade_date=trade_date,
        )


def test_repository_schema_is_strictly_validated() -> None:
    with pytest.raises(ValueError, match="invalid PostgreSQL schema identifier"):
        V16CanonicalArtifactStore(_FakeRepository("bad schema"))
