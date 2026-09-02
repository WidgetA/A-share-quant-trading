from __future__ import annotations

import asyncio
import os
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

import asyncpg
import pytest

from src.data.database.v16_canonical_artifact_store import (
    V16CanonicalArtifactStore,
)
from src.data.database.v20_repository import (
    V20DatabaseConfig,
    V20Repository,
    V20SemanticConflict,
    sha256_json,
)

DSN = os.environ.get("V20_TEST_POSTGRES_DSN", "")
SAFE_PREFIX = "v20_test_v16_artifact_"
TRADE_DATE = date(2026, 8, 31)
OTHER_TRADE_DATE = TRADE_DATE + timedelta(days=1)
PAYLOAD = {
    "schema_version": "v16-canonical-master/v1",
    "universe": ["000001", "600000"],
    "raw_evidence_codes": ["000001", "000002", "600000"],
    "nested": {"count": 2, "values": [1, 2.5, None, True]},
}

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        not DSN,
        reason="V20_TEST_POSTGRES_DSN is required for real PostgreSQL V16 artifact tests",
    ),
]


def _schema() -> str:
    value = SAFE_PREFIX + uuid.uuid4().hex
    assert value.startswith(SAFE_PREFIX)
    return value


def _config(schema: str) -> V20DatabaseConfig:
    return V20DatabaseConfig(
        schema=schema,
        pool_min_size=1,
        pool_max_size=24,
        ssl_mode="disable",
        connection_profile="legacy_embedded",
    )


async def _drop_schema(pool: asyncpg.Pool, schema: str) -> None:
    assert schema.startswith(SAFE_PREFIX)
    async with pool.acquire() as connection:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture
async def artifact_store():
    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=1, max_size=24)
    repository = V20Repository(_config(schema), shared_pool=pool)
    try:
        await repository.connect(migrate=True)
        yield V16CanonicalArtifactStore(repository), pool, schema
    finally:
        await _drop_schema(pool, schema)
        await pool.close()


async def _row(pool: asyncpg.Pool, schema: str, snapshot_id: str) -> asyncpg.Record:
    async with pool.acquire() as connection:
        return await connection.fetchrow(
            f"""
            SELECT snapshot_id,snapshot_type,trade_date,snapshot_hash,snapshot_json,
                   first_received_at
            FROM {schema}.input_snapshots
            WHERE snapshot_id=$1
            """,
            snapshot_id,
        )


async def _row_count(pool: asyncpg.Pool, schema: str) -> int:
    async with pool.acquire() as connection:
        return await connection.fetchval(f"SELECT count(*) FROM {schema}.input_snapshots")


async def test_postgres_roundtrip_preserves_payload_hash_and_aware_receipt(
    artifact_store: tuple[V16CanonicalArtifactStore, asyncpg.Pool, str],
) -> None:
    store, pool, schema = artifact_store

    saved = await store.save_once(
        PAYLOAD,
        official_stream_id="official-stream-a",
        trade_date=TRADE_DATE,
    )
    loaded = await store.load(
        official_stream_id="official-stream-a",
        trade_date=TRADE_DATE,
    )

    assert loaded == saved
    assert loaded is not None
    assert loaded.payload == PAYLOAD
    assert loaded.snapshot_hash == sha256_json(PAYLOAD)
    assert loaded.first_received_at.tzinfo is not None
    assert loaded.first_received_at.utcoffset() is not None
    assert await _row_count(pool, schema) == 1


async def test_postgres_concurrent_streams_share_one_slot_and_row(
    artifact_store: tuple[V16CanonicalArtifactStore, asyncpg.Pool, str],
) -> None:
    store, pool, schema = artifact_store

    records = await asyncio.gather(
        *(
            store.save_once(
                PAYLOAD,
                official_stream_id=f"official-stream-{index}",
                trade_date=TRADE_DATE,
            )
            for index in range(24)
        )
    )

    assert len(records) == 24
    assert len(set(records)) == 1
    assert await _row_count(pool, schema) == 1
    assert (
        await store.load(
            official_stream_id="official-stream-23",
            trade_date=TRADE_DATE,
        )
        == records[0]
    )


@pytest.mark.parametrize("conflicting_stream", ["official-stream-a", "official-stream-b"])
async def test_postgres_same_trade_date_different_content_fails_closed(
    artifact_store: tuple[V16CanonicalArtifactStore, asyncpg.Pool, str],
    conflicting_stream: str,
) -> None:
    store, pool, schema = artifact_store
    original = await store.save_once(
        PAYLOAD,
        official_stream_id="official-stream-a",
        trade_date=TRADE_DATE,
    )
    original_row = await _row(pool, schema, original.snapshot_id)
    conflicting_payload = {**PAYLOAD, "universe": ["999999"]}

    with pytest.raises(V20SemanticConflict, match="canonical artifact slot collision"):
        await store.save_once(
            conflicting_payload,
            official_stream_id=conflicting_stream,
            trade_date=TRADE_DATE,
        )

    assert await _row_count(pool, schema) == 1
    assert await _row(pool, schema, original.snapshot_id) == original_row
    assert (
        await store.load(
            official_stream_id="official-stream-a",
            trade_date=TRADE_DATE,
        )
        == original
    )


async def test_postgres_same_payload_on_different_trade_dates_creates_two_rows(
    artifact_store: tuple[V16CanonicalArtifactStore, asyncpg.Pool, str],
) -> None:
    store, pool, schema = artifact_store

    first = await store.save_once(
        PAYLOAD,
        official_stream_id="official-stream-a",
        trade_date=TRADE_DATE,
    )
    second = await store.save_once(
        PAYLOAD,
        official_stream_id="official-stream-a",
        trade_date=OTHER_TRADE_DATE,
    )

    assert first.snapshot_id != second.snapshot_id
    assert first.snapshot_hash == second.snapshot_hash
    assert await _row_count(pool, schema) == 2
    assert (
        await store.load(
            official_stream_id="official-stream-a",
            trade_date=OTHER_TRADE_DATE,
        )
        == second
    )


async def test_postgres_volatile_computed_at_is_excluded_from_one_semantic_slot(
    artifact_store: tuple[V16CanonicalArtifactStore, asyncpg.Pool, str],
) -> None:
    store, pool, schema = artifact_store
    first_payload = {
        **PAYLOAD,
        "computed_at": datetime(2026, 8, 31, 9, 39, tzinfo=timezone.utc).isoformat(),
    }
    second_payload = {
        **PAYLOAD,
        "computed_at": datetime(2026, 8, 31, 9, 40, tzinfo=timezone.utc).isoformat(),
    }

    first = await store.save_once(
        first_payload,
        official_stream_id="official-stream-a",
        trade_date=TRADE_DATE,
    )
    second = await store.save_once(
        second_payload,
        official_stream_id="official-stream-b",
        trade_date=TRADE_DATE,
    )

    assert second == first
    assert await _row_count(pool, schema) == 1
    row = await _row(pool, schema, first.snapshot_id)
    assert "computed_at" not in row["snapshot_json"]


@pytest.mark.parametrize(
    ("assignment", "parameters"),
    [
        ("snapshot_hash=$2", "f" * 64),
        ("snapshot_json=$2::jsonb", {"changed": True}),
    ],
    ids=["hash", "json"],
)
async def test_postgres_load_fails_closed_for_direct_row_corruption(
    artifact_store: tuple[V16CanonicalArtifactStore, asyncpg.Pool, str],
    assignment: str,
    parameters: Any,
) -> None:
    store, pool, schema = artifact_store
    saved = await store.save_once(
        PAYLOAD,
        official_stream_id="official-stream-a",
        trade_date=TRADE_DATE,
    )

    async with pool.acquire() as connection:
        await connection.execute(
            f"UPDATE {schema}.input_snapshots SET {assignment} WHERE snapshot_id=$1",
            saved.snapshot_id,
            parameters,
        )

    with pytest.raises(V20SemanticConflict, match="canonical artifact row is invalid"):
        await store.load(
            official_stream_id="official-stream-a",
            trade_date=TRADE_DATE,
        )
