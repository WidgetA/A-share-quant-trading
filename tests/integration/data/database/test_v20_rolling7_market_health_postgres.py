from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio

from src.data.database.v20_repository import (
    V20DatabaseConfig,
    V20Repository,
    V20SemanticConflict,
)
from src.strategy.v20.rolling7_market_health import (
    BatchStatus,
    CanonicalRecommendation,
    Rolling7Batch,
    Rolling7Leg,
    SignalKind,
    make_batch,
    make_missing_canonical_batch,
)

DSN = os.environ.get("V20_TEST_POSTGRES_DSN", "")
SAFE_PREFIX = "v20_test_"
pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        not DSN,
        reason="V20_TEST_POSTGRES_DSN is required for real PostgreSQL Rolling7 tests",
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
        pool_max_size=7,
        ssl_mode="disable",
        connection_profile="legacy_embedded",
    )


async def _drop_schema(pool: asyncpg.Pool, schema: str) -> None:
    assert schema.startswith(SAFE_PREFIX)
    async with pool.acquire() as connection:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest_asyncio.fixture
async def repository():
    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=1, max_size=7)
    instance = V20Repository(_config(schema), shared_pool=pool)
    try:
        await instance.connect(migrate=True)
        yield instance, pool, schema
    finally:
        await _drop_schema(pool, schema)
        await pool.close()


def _batch(
    *,
    signal_date=date(2026, 8, 1),
    canonical_snapshot_id="snapshot-01",
    canonical_snapshot_hash="1" * 64,
    t2_date=date(2026, 8, 4),
    d0_references=None,
    d2_closes=None,
):
    recommendations = (
        CanonicalRecommendation(rank=1, code="000001"),
        CanonicalRecommendation(rank=2, code="000002"),
    )
    return make_batch(
        signal_date=signal_date,
        canonical_snapshot_id=canonical_snapshot_id,
        canonical_snapshot_hash=canonical_snapshot_hash,
        recommendations=recommendations,
        t2_date=t2_date,
        d0_references=d0_references or {},
        d2_closes=d2_closes or {},
    )


def _null_t2_gap_batch(d0_references=None):
    recommendations = (
        CanonicalRecommendation(rank=1, code="000001"),
        CanonicalRecommendation(rank=2, code="000002"),
    )
    references = d0_references or {}
    return Rolling7Batch(
        signal_date=date(2026, 8, 1),
        canonical_snapshot_id="snapshot-01",
        canonical_snapshot_hash="1" * 64,
        canonical_available=True,
        signal_kind=SignalKind.SIGNAL,
        recommendations=recommendations,
        t2_date=None,
        legs=tuple(
            Rolling7Leg(
                rank=item.rank,
                code=item.code,
                d0_reference=references.get(item.code),
                d2_close=None,
            )
            for item in recommendations
        ),
        status=BatchStatus.DATA_GAP,
        reason="INVALID_T2_SESSION",
    )


@pytest.mark.asyncio
async def test_rolling7_market_health_single_table_lifecycle_and_restart(repository):
    instance, pool, schema = repository
    now = datetime.now(timezone.utc)
    full_d0 = {"000001": 100.0, "000002": 200.0}
    progression = (
        _null_t2_gap_batch(),
        _null_t2_gap_batch(d0_references={"000001": 100.0}),
        _batch(d0_references=full_d0),
        _batch(d0_references=full_d0, d2_closes={"000001": 101.0}),
        _batch(
            d0_references=full_d0,
            d2_closes={"000001": 101.0, "000002": 202.0},
        ),
    )
    for batch in progression:
        assert await instance.save_rolling7_market_health(batch, updated_at=now)
        assert await instance.save_rolling7_market_health(batch, updated_at=now)

    partial_before_restart = await instance.load_rolling7_market_health(
        before_t2=date(2026, 8, 5), limit=1
    )
    assert partial_before_restart == (progression[-2],)

    restart = V20Repository(_config(schema), shared_pool=pool)
    assert restart.uses_shared_pool is True
    assert restart.pool is pool
    try:
        await restart.connect(migrate=True)
        replay = await restart.save_rolling7_market_health(progression[-1], updated_at=now)
        loaded = await restart.load_rolling7_market_health(before_t2=date(2026, 8, 5), limit=1)
        boundary = await restart.load_rolling7_market_health(before_t2=date(2026, 8, 4), limit=1)
        changed_t2 = _batch(
            t2_date=date(2026, 8, 5),
            d0_references=full_d0,
            d2_closes={"000001": 101.0, "000002": 202.0},
        )
        changed_d0 = _batch(
            d0_references={"000001": 99.0, "000002": 200.0},
            d2_closes={"000001": 101.0, "000002": 202.0},
        )
        changed_d2 = _batch(
            d0_references=full_d0,
            d2_closes={"000001": 101.0, "000002": 999.0},
        )
        removed_d0 = _batch(
            d0_references={"000001": 100.0},
            d2_closes={"000001": 101.0, "000002": 202.0},
        )
        removed_d2 = _batch(
            d0_references=full_d0,
            d2_closes={"000001": 101.0},
        )
        changed_identity = _batch(
            canonical_snapshot_id="snapshot-02",
            canonical_snapshot_hash="2" * 64,
            d0_references=full_d0,
            d2_closes={"000001": 101.0, "000002": 202.0},
        )
        for changed in (
            removed_d0,
            removed_d2,
            changed_t2,
            changed_d0,
            changed_d2,
            changed_identity,
        ):
            with pytest.raises(V20SemanticConflict, match="exists differently"):
                await restart.save_rolling7_market_health(changed, updated_at=now)
    finally:
        await restart.close()

    assert replay.batch == progression[-1]
    assert [batch.signal_kind for batch in loaded] == [SignalKind.SIGNAL]
    assert [batch.status for batch in loaded] == [BatchStatus.COMPLETE]
    assert boundary == ()


@pytest.mark.asyncio
async def test_rolling7_market_health_missing_placeholder_transitions(repository):
    instance, _pool, _schema = repository
    now = datetime.now(timezone.utc)
    null_t2 = make_missing_canonical_batch(signal_date=date(2026, 8, 1), t2_date=None)
    filled_t2 = make_missing_canonical_batch(signal_date=date(2026, 8, 1), t2_date=date(2026, 8, 4))
    changed_t2 = make_missing_canonical_batch(
        signal_date=date(2026, 8, 1), t2_date=date(2026, 8, 5)
    )
    canonical_gap = _batch(d0_references={"000001": 100.0})

    assert await instance.save_rolling7_market_health(null_t2, updated_at=now)
    assert await instance.save_rolling7_market_health(null_t2, updated_at=now)
    assert await instance.save_rolling7_market_health(filled_t2, updated_at=now)
    with pytest.raises(V20SemanticConflict, match="exists differently"):
        await instance.save_rolling7_market_health(changed_t2, updated_at=now)
    assert await instance.save_rolling7_market_health(canonical_gap, updated_at=now)


@pytest.mark.asyncio
async def test_rolling7_market_health_partial_gap_rejects_regression(repository):
    instance, _pool, _schema = repository
    now = datetime.now(timezone.utc)
    full_d0 = {"000001": 100.0, "000002": 200.0}
    partial = _batch(d0_references=full_d0, d2_closes={"000001": 101.0})
    changed_t2_with_growth = _batch(
        t2_date=date(2026, 8, 5),
        d0_references=full_d0,
        d2_closes={"000001": 101.0, "000002": 202.0},
    )
    removed = _batch(d0_references={"000001": 100.0}, d2_closes={"000001": 101.0})

    assert await instance.save_rolling7_market_health(partial, updated_at=now)
    for changed in (changed_t2_with_growth, removed):
        with pytest.raises(V20SemanticConflict, match="exists differently"):
            await instance.save_rolling7_market_health(changed, updated_at=now)


@pytest.mark.asyncio
async def test_rolling7_market_health_schema_migration_is_idempotent(repository):
    _instance, pool, schema = repository

    async with pool.acquire() as connection:
        migration = (
            (
                Path(__file__).resolve().parents[4]
                / "migrations"
                / "v20"
                / "003_rolling7_market_health.sql"
            )
            .read_text(encoding="utf-8")
            .replace("v20.", f'"{schema}".')
        )
        await connection.execute(migration)
        table_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_schema=$1 AND table_name LIKE 'rolling7_market_health%'
            """,
            schema,
        )
        column_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM information_schema.columns
            WHERE table_schema=$1 AND table_name='rolling7_market_health'
            """,
            schema,
        )
        primary_keys = await connection.fetch(
            """
            SELECT columns.column_name
            FROM information_schema.table_constraints AS constraints
            JOIN information_schema.key_column_usage AS columns
              ON columns.constraint_schema=constraints.constraint_schema
             AND columns.constraint_name=constraints.constraint_name
            WHERE constraints.table_schema=$1
              AND constraints.table_name='rolling7_market_health'
              AND constraints.constraint_type='PRIMARY KEY'
            ORDER BY columns.ordinal_position
            """,
            schema,
        )
        columns = await connection.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=$1 AND table_name='rolling7_market_health'
            ORDER BY ordinal_position
            """,
            schema,
        )
        maturity_index = await connection.fetchrow(
            """
            SELECT array_agg(attribute.attname ORDER BY key_order.ordinality) AS columns,
                   max(index.indisprimary::int) AS primary_key,
                   max(index.indisunique::int) AS unique_index
            FROM pg_index AS index
            JOIN pg_class AS class ON class.oid=index.indexrelid
            JOIN pg_namespace AS namespace ON namespace.oid=class.relnamespace
            CROSS JOIN LATERAL unnest(index.indkey) WITH ORDINALITY AS key_order(attnum,ordinality)
            JOIN pg_attribute AS attribute
              ON attribute.attrelid=index.indrelid AND attribute.attnum=key_order.attnum
            WHERE namespace.nspname=$1 AND class.relname='idx_rolling7_market_health_maturity'
            GROUP BY class.relname
            """,
            schema,
        )
        constraint_names = await connection.fetch(
            """
            SELECT constraints.constraint_name
            FROM information_schema.table_constraints AS constraints
            WHERE constraints.table_schema=$1
              AND constraints.table_name='rolling7_market_health'
              AND constraints.constraint_type='CHECK'
            ORDER BY constraints.constraint_name
            """,
            schema,
        )
        snapshot_hash_type = await connection.fetchrow(
            """
            SELECT data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema=$1 AND table_name='rolling7_market_health'
              AND column_name='canonical_snapshot_hash'
            """,
            schema,
        )

    assert table_count == 1
    assert column_count == 13
    assert [row["column_name"] for row in primary_keys] == ["signal_date"]
    assert [row["column_name"] for row in columns] == [
        "signal_date",
        "canonical_available",
        "canonical_snapshot_id",
        "canonical_snapshot_hash",
        "signal_kind",
        "recommendations",
        "t2_date",
        "d0_references",
        "d2_closes",
        "batch_return",
        "status",
        "reason",
        "updated_at",
    ]
    assert maturity_index["columns"] == ["t2_date", "signal_date"]
    assert maturity_index["primary_key"] == 0
    assert maturity_index["unique_index"] == 0
    assert snapshot_hash_type["data_type"] == "text"
    assert snapshot_hash_type["character_maximum_length"] is None
    assert [row["constraint_name"] for row in constraint_names] == [
        "ck_rolling7_market_health_canonical_identity",
        "ck_rolling7_market_health_canonical_shape",
        "ck_rolling7_market_health_complete_signal",
        "ck_rolling7_market_health_d0_references_json",
        "ck_rolling7_market_health_d0_references_positive",
        "ck_rolling7_market_health_d2_closes_json",
        "ck_rolling7_market_health_d2_closes_positive",
        "ck_rolling7_market_health_data_gap_return",
        "ck_rolling7_market_health_kinds",
        "ck_rolling7_market_health_no_signal",
        "ck_rolling7_market_health_recommendations_json",
        "ck_rolling7_market_health_signal_recommendations",
        "ck_rolling7_market_health_status",
    ]


@pytest.mark.parametrize(
    "column,invalid_json",
    [
        ("d0_references", '{"000001": "100", "000002": 200}'),
        ("d0_references", '{"000001": 0, "000002": 200}'),
        ("d0_references", '{"000001": -1, "000002": 200}'),
        ("d0_references", '{"000001": null, "000002": 200}'),
        ("d0_references", '{"000001": {}, "000002": 200}'),
        ("d2_closes", '{"000001": "101", "000002": 202}'),
        ("d2_closes", '{"000001": 0, "000002": 202}'),
        ("d2_closes", '{"000001": -1, "000002": 202}'),
        ("d2_closes", '{"000001": null, "000002": 202}'),
        ("d2_closes", '{"000001": {}, "000002": 202}'),
    ],
)
@pytest.mark.asyncio
async def test_rolling7_market_health_rejects_bypassed_invalid_evidence(
    repository, column, invalid_json
):
    instance, pool, _schema = repository
    batch = _batch(
        d0_references={"000001": 100.0, "000002": 200.0},
        d2_closes={"000001": 101.0, "000002": 202.0},
    )
    await instance.save_rolling7_market_health(batch, updated_at=datetime.now(timezone.utc))

    async with pool.acquire() as connection:
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"""
                UPDATE "{_schema}".rolling7_market_health
                SET {column}=$1::jsonb
                WHERE signal_date=$2
                """,
                invalid_json,
                batch.signal_date,
            )


@pytest.mark.asyncio
async def test_rolling7_market_health_readback_rejects_huge_json_number(repository):
    instance, pool, _schema = repository
    batch = _batch(
        signal_date=date(2026, 8, 2),
        d0_references={"000001": 100.0, "000002": 200.0},
        d2_closes={"000001": 101.0, "000002": 202.0},
    )
    await instance.save_rolling7_market_health(batch, updated_at=datetime.now(timezone.utc))

    async with pool.acquire() as connection:
        update_count = await connection.execute(
            f"""
            UPDATE "{_schema}".rolling7_market_health
            SET d2_closes=$1::jsonb
            WHERE signal_date=$2
            """,
            '{"000001": 1e400, "000002": 202}',
            batch.signal_date,
        )

    assert update_count == "UPDATE 1"
    with pytest.raises(V20SemanticConflict, match="market evidence is invalid"):
        await instance.load_rolling7_market_health(before_t2=date(2026, 8, 5), limit=1)
