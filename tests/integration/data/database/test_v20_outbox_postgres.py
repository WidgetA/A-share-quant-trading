from __future__ import annotations

import asyncio
import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import pytest

from src.data.database.v20_repository import (
    V20DatabaseConfig,
    V20Repository,
    V20StateConflict,
    migration_sql,
    sha256_json,
)

DSN = os.environ.get("V20_TEST_POSTGRES_DSN", "")
if not DSN:
    pytest.fail(
        "V20_TEST_POSTGRES_DSN is required for real PostgreSQL V20 outbox tests",
        pytrace=False,
    )

SAFE_PREFIX = "v20_test_"
pytestmark = pytest.mark.postgres


def _baseline_for_schema(schema: str) -> str:
    root = Path(__file__).resolve().parents[4]
    baseline = (root / "migrations" / "v20" / "001_v20.sql").read_text(encoding="utf-8")
    baseline = baseline.replace(
        "CREATE SCHEMA IF NOT EXISTS v20;",
        f'CREATE SCHEMA IF NOT EXISTS "{schema}";',
        1,
    )
    return baseline.replace("v20.", f"{schema}.")


def _schema() -> str:
    value = SAFE_PREFIX + uuid.uuid4().hex
    assert value.startswith(SAFE_PREFIX)
    return value


def _config(schema: str) -> V20DatabaseConfig:
    return V20DatabaseConfig(
        schema=schema,
        pool_min_size=1,
        pool_max_size=12,
        ssl_mode="disable",
        connection_profile="legacy_embedded",
    )


async def _drop_schema(pool: asyncpg.Pool, schema: str) -> None:
    assert schema.startswith(SAFE_PREFIX)
    async with pool.acquire() as connection:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture
async def repository():
    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=1, max_size=12)
    instance = V20Repository(_config(schema), shared_pool=pool)
    try:
        await instance.connect(migrate=True)
        yield instance, pool, schema
    finally:
        await _drop_schema(pool, schema)
        await pool.close()


async def _insert_outbox(
    pool: asyncpg.Pool,
    schema: str,
    event_id: str,
    *,
    status: str = "PENDING",
    expiry: datetime | None = None,
    attempt_count: int = 0,
    lease_owner: str | None = None,
    lease_until: datetime | None = None,
    delivered_at: datetime | None = None,
) -> None:
    semantic = {"event_id": event_id, "kind": "DATA_ALERT"}
    payload = {"event_id": event_id, "message": "ok"}
    semantic_json = json.dumps(semantic, separators=(",", ":"), ensure_ascii=False)
    payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {schema}.outbox_events
                (event_id,event_type,route_id,official_stream_id,lineage_id,
                 semantic_content_hash,semantic_json,payload_json,payload_hash,
                 seal_status,action_expiry_ts,generated_at,commit_marker,
                 delivery_status,attempt_count,lease_owner,lease_until,delivered_at)
            VALUES (
                $1,'DATA_ALERT','formal-route','official','lineage-1',
                $6,$7::jsonb,$8::jsonb,$9,'SEALED',$2,clock_timestamp(),
                nextval($3::regclass),$4,$5,$10,$11,$12
            )
            """,
            event_id,
            expiry,
            f"{schema}.commit_marker_seq",
            status,
            attempt_count,
            sha256_json(semantic),
            semantic_json,
            payload_json,
            sha256_json(payload),
            lease_owner,
            lease_until,
            delivered_at,
        )


async def _outbox(pool: asyncpg.Pool, schema: str, event_id: str) -> asyncpg.Record:
    async with pool.acquire() as connection:
        return await connection.fetchrow(
            f"SELECT * FROM {schema}.outbox_events WHERE event_id=$1", event_id
        )


async def _attempt(pool: asyncpg.Pool, schema: str, event_id: str, number: int):
    async with pool.acquire() as connection:
        return await connection.fetchrow(
            f"SELECT * FROM {schema}.delivery_attempts WHERE event_id=$1 AND attempt_number=$2",
            event_id,
            number,
        )


async def test_fresh_migration_is_idempotent_and_receipted(repository) -> None:
    instance, _, schema = repository
    for _ in range(3):
        await instance.migrate()
    async with instance.pool.acquire() as connection:
        receipt = await connection.fetchrow(
            f"SELECT * FROM {schema}.migration_receipts WHERE version='002_outbox_at_most_once'"
        )
    assert receipt is not None
    assert re.fullmatch(r"[0-9a-f]{64}", receipt["checksum"])


async def test_repository_and_standalone_migration_receipts_use_same_checksum(
    repository,
) -> None:
    runtime_instance, _, runtime_schema = repository
    runtime_receipt = await _get_receipt(runtime_instance, runtime_schema)

    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=1, max_size=2)
    try:
        standalone_002 = (
            (
                Path(__file__).resolve().parents[4]
                / "migrations"
                / "v20"
                / "002_outbox_at_most_once.sql"
            )
            .read_text(encoding="utf-8")
            .replace("v20.", f"{schema}.")
        )
        async with pool.acquire() as connection:
            await connection.execute(f'CREATE SCHEMA "{schema}"')
            await connection.execute(_baseline_for_schema(schema))
            await connection.execute(standalone_002)
            standalone_receipt = await connection.fetchrow(
                f"SELECT checksum FROM {schema}.migration_receipts "
                "WHERE version='002_outbox_at_most_once'"
            )
        assert standalone_receipt is not None
        assert standalone_receipt["checksum"] == runtime_receipt["checksum"]
    finally:
        await _drop_schema(pool, schema)
        await pool.close()


async def _get_receipt(instance: V20Repository, schema: str) -> asyncpg.Record:
    async with instance.pool.acquire() as connection:
        return await connection.fetchrow(
            f"SELECT checksum FROM {schema}.migration_receipts "
            "WHERE version='002_outbox_at_most_once'"
        )


async def test_existing_receipt_with_wrong_checksum_fails_closed(repository) -> None:
    instance, pool, schema = repository
    async with pool.acquire() as connection:
        await connection.execute(
            f"UPDATE {schema}.migration_receipts "
            "SET checksum='wrong-checksum' WHERE version='002_outbox_at_most_once'"
        )

    with pytest.raises(asyncpg.PostgresError, match="checksum mismatch"):
        await instance.migrate()

    async with pool.acquire() as connection:
        checksum = await connection.fetchval(
            f"SELECT checksum FROM {schema}.migration_receipts "
            "WHERE version='002_outbox_at_most_once'"
        )
    assert checksum == "wrong-checksum"


async def test_unknown_same_named_constraint_definition_fails_closed() -> None:
    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=1, max_size=2)
    try:
        async with pool.acquire() as connection:
            await connection.execute(f'CREATE SCHEMA "{schema}"')
            await connection.execute(_baseline_for_schema(schema))
            original_definitions = {
                row["constraint_name"]: row["definition"]
                for row in await connection.fetch(
                    f"""
                    SELECT conname AS constraint_name,
                           pg_get_constraintdef(oid) AS definition
                    FROM pg_constraint
                    WHERE conrelid = '{schema}.outbox_events'::regclass
                      AND conname IN ('outbox_events_check', 'outbox_events_check1')
                    """
                )
            }
            assert "payload_json" in original_definitions["outbox_events_check"]
            assert "seal_status" in original_definitions["outbox_events_check"]
            assert "delivery_status" in original_definitions["outbox_events_check1"]
            assert "lease_owner" in original_definitions["outbox_events_check1"]
            await connection.execute(
                f"""
                ALTER TABLE {schema}.outbox_events
                    DROP CONSTRAINT outbox_events_check1,
                    ADD CONSTRAINT outbox_events_check1 CHECK (
                        delivery_status='LEASED' AND lease_owner IS NOT NULL
                    )
                """
            )

        instance = V20Repository(_config(schema), shared_pool=pool)
        with pytest.raises(asyncpg.PostgresError, match="unknown V20 outbox"):
            await instance.migrate()

        async with pool.acquire() as connection:
            definitions_after = {
                row["constraint_name"]: row["definition"]
                for row in await connection.fetch(
                    f"""
                    SELECT conname AS constraint_name,
                           pg_get_constraintdef(oid) AS definition
                    FROM pg_constraint
                    WHERE conrelid = '{schema}.outbox_events'::regclass
                      AND conname IN ('outbox_events_check', 'outbox_events_check1')
                    """
                )
            }
            receipt_table = await connection.fetchval(
                "SELECT to_regclass($1)", f"{schema}.migration_receipts"
            )
        assert (
            definitions_after["outbox_events_check"]
            == (original_definitions["outbox_events_check"])
        )
        assert "lease_until" not in definitions_after["outbox_events_check1"]
        assert receipt_table is None
    finally:
        await _drop_schema(pool, schema)
        await pool.close()


@pytest.mark.parametrize(
    "rogue_expression",
    [
        "delivery_status <> 'LEASED' OR lease_owner IS NOT NULL",
        "delivered_at IS NULL OR delivery_status = 'SENT'",
        "attempt_count = 0",
    ],
    ids=["lease-columns", "delivered-at", "attempt-count"],
)
async def test_rogue_delivery_constraint_fails_closed_without_partial_migration(
    rogue_expression,
) -> None:
    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=1, max_size=2)
    try:
        async with pool.acquire() as connection:
            await connection.execute(f'CREATE SCHEMA "{schema}"')
            await connection.execute(_baseline_for_schema(schema))
            await connection.execute(
                f"""
                ALTER TABLE {schema}.outbox_events
                    ADD CONSTRAINT rogue_v20_delivery_check CHECK (
                        {rogue_expression}
                    )
                """
            )

        instance = V20Repository(_config(schema), shared_pool=pool)
        with pytest.raises(asyncpg.PostgresError, match="unknown V20 outbox"):
            await instance.migrate()

        async with pool.acquire() as connection:
            receipt_table = await connection.fetchval(
                "SELECT to_regclass($1)", f"{schema}.migration_receipts"
            )
            quarantine_table = await connection.fetchval(
                "SELECT to_regclass($1)", f"{schema}.delivery_quarantine"
            )
            phase_column = await connection.fetchval(
                """
                SELECT a.attname
                FROM pg_attribute AS a
                WHERE a.attrelid = $1::regclass
                  AND a.attname = 'phase'
                  AND NOT a.attisdropped
                """,
                f"{schema}.delivery_attempts",
            )
            rogue_constraint = await connection.fetchval(
                """
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = $1::regclass
                  AND conname = 'rogue_v20_delivery_check'
                """,
                f"{schema}.outbox_events",
            )
        assert receipt_table is None
        assert quarantine_table is None
        assert phase_column is None
        assert rogue_constraint is not None
    finally:
        await _drop_schema(pool, schema)
        await pool.close()


@pytest.mark.parametrize(
    "wrong_definition",
    ["wrong-table", "wrong-predicate", "not-unique", "wrong-order"],
)
async def test_wrong_same_named_index_definition_fails_closed_without_receipt(
    wrong_definition,
) -> None:
    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=1, max_size=2)
    try:
        async with pool.acquire() as connection:
            await connection.execute(f'CREATE SCHEMA "{schema}"')
            await connection.execute(_baseline_for_schema(schema))
            if wrong_definition == "wrong-table":
                await connection.execute(
                    f"CREATE INDEX idx_v20_outbox_ready_v2 ON {schema}.delivery_attempts(event_id)"
                )
            elif wrong_definition == "wrong-predicate":
                await connection.execute(
                    f"""
                    CREATE INDEX idx_v20_outbox_unknown_v2
                    ON {schema}.outbox_events(
                        route_id, official_stream_id, lineage_id, created_at, event_id
                    )
                    WHERE seal_status = 'SEALED' AND delivery_status = 'PENDING'
                    """
                )
            elif wrong_definition == "not-unique":
                await connection.execute(
                    f"""
                    CREATE INDEX uq_v20_delivery_attempt_started
                    ON {schema}.delivery_attempts(event_id)
                    WHERE phase = 'STARTED'
                    """
                )
            else:
                await connection.execute(
                    f"""
                    CREATE INDEX idx_v20_outbox_ready_v2
                    ON {schema}.outbox_events(
                        delivery_status DESC, seal_status, available_at
                    )
                    WHERE seal_status = 'SEALED' AND delivery_status = 'PENDING'
                    """
                )

        instance = V20Repository(_config(schema), shared_pool=pool)
        with pytest.raises(asyncpg.PostgresError, match="index definition catalog mismatch"):
            await instance.migrate()

        async with pool.acquire() as connection:
            receipt_table = await connection.fetchval(
                "SELECT to_regclass($1)", f"{schema}.migration_receipts"
            )
            quarantine_table = await connection.fetchval(
                "SELECT to_regclass($1)", f"{schema}.delivery_quarantine"
            )
            wrong_index_name = {
                "wrong-table": "idx_v20_outbox_ready_v2",
                "wrong-predicate": "idx_v20_outbox_unknown_v2",
                "not-unique": "uq_v20_delivery_attempt_started",
                "wrong-order": "idx_v20_outbox_ready_v2",
            }[wrong_definition]
            wrong_index = await connection.fetchval(
                """
                SELECT 1
                FROM pg_class AS index_class
                JOIN pg_namespace AS index_namespace
                  ON index_namespace.oid = index_class.relnamespace
                WHERE index_namespace.nspname = $1
                  AND index_class.relname = $2
                """,
                schema,
                wrong_index_name,
            )
        assert receipt_table is None
        assert quarantine_table is None
        assert wrong_index is not None
    finally:
        await _drop_schema(pool, schema)
        await pool.close()


async def test_legacy_upgrade_quarantines_ambiguous_and_preserves_clean_rows() -> None:
    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=1, max_size=2)
    now = datetime.now(timezone.utc)
    try:
        baseline = _baseline_for_schema(schema)
        async with pool.acquire() as connection:
            await connection.execute(f'CREATE SCHEMA "{schema}"')
            await connection.execute(baseline)
            for event_id, status, count, lease_owner, lease_until, delivered_at in (
                ("clean-pending", "PENDING", 0, None, None, None),
                ("old-sent", "SENT", 1, None, None, now),
                ("old-failed", "PENDING", 1, None, None, None),
                ("old-leased", "LEASED", 0, "old-owner", now + timedelta(hours=1), None),
            ):
                await _insert_outbox(
                    pool,
                    schema,
                    event_id,
                    status=status,
                    attempt_count=count,
                    lease_owner=lease_owner,
                    lease_until=lease_until,
                    delivered_at=delivered_at,
                )
            await connection.execute(
                f"""
                INSERT INTO {schema}.delivery_attempts
                    (event_id,attempt_number,attempted_at,succeeded)
                VALUES
                    ('old-sent',1,clock_timestamp(),TRUE),
                    ('old-failed',1,clock_timestamp(),FALSE)
                """
            )

        instance = V20Repository(_config(schema), shared_pool=pool)
        for _ in range(3):
            await instance.migrate()
        async with pool.acquire() as connection:
            constraint_definitions = [
                row["definition"]
                for row in await connection.fetch(
                    f"""
                    SELECT pg_get_constraintdef(oid) AS definition
                    FROM pg_constraint
                    WHERE conrelid = '{schema}.outbox_events'::regclass
                      AND contype = 'c'
                    """
                )
            ]
            statuses = {
                row["event_id"]: row["delivery_status"]
                for row in await connection.fetch(
                    f"SELECT event_id,delivery_status FROM {schema}.outbox_events"
                )
            }
            quarantine_before = await connection.fetch(
                f"SELECT event_id,migrated_at FROM {schema}.delivery_quarantine ORDER BY event_id"
            )
            attempts = {
                (
                    row["event_id"],
                    row["phase"],
                    row["succeeded"],
                    row["completed_at"] is not None,
                    row["error_text"],
                    row["delivery_variant"],
                )
                for row in await connection.fetch(
                    f"""
                    SELECT event_id,phase,succeeded,completed_at,error_text,delivery_variant
                    FROM {schema}.delivery_attempts
                    """
                )
            }
        async with pool.acquire() as connection:
            quarantine_after = await connection.fetch(
                f"SELECT event_id,migrated_at FROM {schema}.delivery_quarantine ORDER BY event_id"
            )

        assert statuses == {
            "clean-pending": "PENDING",
            "old-sent": "SENT",
            "old-failed": "DELIVERY_UNKNOWN",
            "old-leased": "DELIVERY_UNKNOWN",
        }
        assert [row["event_id"] for row in quarantine_before] == ["old-failed", "old-leased"]
        assert quarantine_before == quarantine_after
        assert attempts == {
            ("old-sent", "DELIVERED", True, True, None, "LEGACY_UNKNOWN"),
            (
                "old-failed",
                "UNKNOWN",
                None,
                True,
                "legacy delivery outcome unknown",
                "LEGACY_UNKNOWN",
            ),
        }
        assert all(row[-1] == "LEGACY_UNKNOWN" for row in attempts)
        assert "CHECK (attempt_count >= 0)" in constraint_definitions
        assert any("payload_json" in definition for definition in constraint_definitions)
        delivery_constraints = [
            definition for definition in constraint_definitions if "delivery_status" in definition
        ]
        assert len(delivery_constraints) == 1
        assert "DELIVERY_UNKNOWN" in delivery_constraints[0]
    finally:
        await _drop_schema(pool, schema)
        await pool.close()


async def test_ten_workers_have_single_lease_winner(repository) -> None:
    instance, _, schema = repository
    await _insert_outbox(instance.pool, schema, "one-event")

    results = await asyncio.gather(
        *(
            instance.lease_outbox(
                worker_id=f"worker-{index}",
                route_id="formal-route",
                official_stream_id="official",
                lineage_id="lineage-1",
                limit=1,
            )
            for index in range(10)
        ),
        return_exceptions=True,
    )
    winners = [result for result in results if result]
    assert len(winners) == 1
    assert len(winners[0]) == 1


async def test_concurrent_begin_creates_exactly_one_attempt(repository) -> None:
    instance, pool, schema = repository
    await _insert_outbox(pool, schema, "begin-event")
    record = (
        await instance.lease_outbox(
            worker_id="owner",
            route_id="formal-route",
            official_stream_id="official",
            lineage_id="lineage-1",
        )
    )[0]
    assert record.event_id == "begin-event"

    results = await asyncio.gather(
        *(
            instance.begin_delivery_attempt(
                "begin-event",
                worker_id="owner",
                route_id="formal-route",
                official_stream_id="official",
                lineage_id="lineage-1",
            )
            for _ in range(10)
        ),
        return_exceptions=True,
    )
    successes = [result for result in results if not isinstance(result, BaseException)]
    assert len(successes) == 1
    assert successes[0].attempt_number == 1
    assert sum(isinstance(result, V20StateConflict) for result in results) == 9
    assert (await _outbox(pool, schema, "begin-event"))["attempt_count"] == 1


async def test_wrong_worker_stale_attempt_and_duplicate_complete_fail_without_mutation(
    repository,
) -> None:
    instance, pool, schema = repository
    await _insert_outbox(pool, schema, "cas-event")
    await instance.lease_outbox(
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    attempt = await instance.begin_delivery_attempt(
        "cas-event",
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    before = await _outbox(pool, schema, "cas-event")
    before_attempt = await _attempt(pool, schema, "cas-event", attempt.attempt_number)

    for worker_id, attempt_number in (("other", 1), ("owner", 2), ("other", 2)):
        with pytest.raises(V20StateConflict):
            await instance.complete_delivery(
                "cas-event",
                attempt_number=attempt_number,
                worker_id=worker_id,
                route_id="formal-route",
                official_stream_id="official",
                lineage_id="lineage-1",
                outcome="DELIVERED",
            )
    assert dict(await _outbox(pool, schema, "cas-event")) == dict(before)
    assert dict(await _attempt(pool, schema, "cas-event", 1)) == dict(before_attempt)

    await instance.complete_delivery(
        "cas-event",
        attempt_number=attempt.attempt_number,
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
        outcome="DELIVERED",
    )
    with pytest.raises(V20StateConflict):
        await instance.complete_delivery(
            "cas-event",
            attempt_number=attempt.attempt_number,
            worker_id="owner",
            route_id="formal-route",
            official_stream_id="official",
            lineage_id="lineage-1",
            outcome="DELIVERED",
        )


async def test_attempt_phase_constraints_reject_invalid_combinations(repository) -> None:
    instance, pool, schema = repository
    await _insert_outbox(pool, schema, "constraint-event")
    await instance.lease_outbox(
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    attempt = await instance.begin_delivery_attempt(
        "constraint-event",
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    started = await _attempt(pool, schema, "constraint-event", attempt.attempt_number)
    assert started["succeeded"] is None
    assert started["completed_at"] is None
    assert started["error_text"] is None
    active = await _outbox(pool, schema, "constraint-event")
    assert active["delivery_status"] == "DELIVERY_UNKNOWN"
    assert active["lease_owner"] == "owner"
    assert active["lease_until"] is not None

    async with pool.acquire() as connection:
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"UPDATE {schema}.delivery_attempts SET succeeded=FALSE "
                "WHERE event_id='constraint-event'"
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"UPDATE {schema}.delivery_attempts SET worker_id=NULL "
                "WHERE event_id='constraint-event'"
            )
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                f"""
                INSERT INTO {schema}.delivery_attempts
                    (event_id,attempt_number,phase,worker_id,delivery_variant)
                VALUES
                    ('constraint-event',2,'STARTED','other-owner','PRIMARY')
                """
            )
        with pytest.raises(asyncpg.PostgresError, match="half lease"):
            await connection.execute(
                f"UPDATE {schema}.outbox_events SET lease_owner=NULL "
                "WHERE event_id='constraint-event'"
            )
        with pytest.raises(asyncpg.PostgresError, match="active STARTED attempt"):
            await connection.execute(
                f"UPDATE {schema}.outbox_events SET lease_owner='wrong-owner' "
                "WHERE event_id='constraint-event'"
            )


async def test_active_unknown_requires_current_attempt_number(repository) -> None:
    instance, pool, schema = repository
    await _insert_outbox(pool, schema, "current-attempt-event")
    await instance.lease_outbox(
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    attempt = await instance.begin_delivery_attempt(
        "current-attempt-event",
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    assert attempt.attempt_number == 1
    async with pool.acquire() as connection:
        with pytest.raises(asyncpg.PostgresError, match="immutable"):
            await connection.execute(
                f"UPDATE {schema}.delivery_attempts "
                "SET attempt_number=2 "
                "WHERE event_id='current-attempt-event' AND attempt_number=1"
            )

        with pytest.raises(asyncpg.PostgresError, match="active STARTED attempt"):
            await connection.execute(
                f"UPDATE {schema}.outbox_events SET lease_owner='wrong-owner' "
                "WHERE event_id='current-attempt-event'"
            )
        with pytest.raises(asyncpg.PostgresError, match="attempt_count"):
            await connection.execute(
                f"UPDATE {schema}.outbox_events SET attempt_count=2 "
                "WHERE event_id='current-attempt-event'"
            )


async def test_begin_delivery_attempt_passes_explicit_dispatch_cas(repository) -> None:
    instance, pool, schema = repository
    await _insert_outbox(pool, schema, "begin-cas-event")
    await instance.lease_outbox(
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )

    attempt = await instance.begin_delivery_attempt(
        "begin-cas-event",
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )

    outbox = await _outbox(pool, schema, "begin-cas-event")
    started = await _attempt(pool, schema, "begin-cas-event", attempt.attempt_number)
    assert attempt.attempt_number == 1
    assert outbox["delivery_status"] == "DELIVERY_UNKNOWN"
    assert outbox["attempt_count"] == 1
    assert outbox["lease_owner"] == "owner"
    assert started["phase"] == "STARTED"
    assert started["delivery_variant"] == "PRIMARY"


async def test_delivery_attempt_variant_is_required_and_immutable_identity(
    repository,
) -> None:
    instance, pool, schema = repository
    await _insert_outbox(pool, schema, "variant-event")
    await instance.lease_outbox(
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    attempt = await instance.begin_delivery_attempt(
        "variant-event",
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    assert attempt.delivery_variant == "PRIMARY"
    async with pool.acquire() as connection:
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"UPDATE {schema}.delivery_attempts SET delivery_variant=NULL "
                "WHERE event_id='variant-event'"
            )
        with pytest.raises(asyncpg.PostgresError, match="immutable"):
            await connection.execute(
                f"UPDATE {schema}.delivery_attempts SET event_id='other-event' "
                "WHERE event_id='variant-event'"
            )


async def test_pre_dispatch_release_makes_event_leaseable_again(repository) -> None:
    instance, pool, schema = repository
    await _insert_outbox(pool, schema, "defer-event")
    await instance.lease_outbox(
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    await instance.defer_before_dispatch(
        "defer-event",
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
        error="pre-dispatch validation failed",
        retry_after_seconds=0,
    )
    record = (
        await instance.lease_outbox(
            worker_id="next-owner",
            route_id="formal-route",
            official_stream_id="official",
            lineage_id="lineage-1",
        )
    )[0]
    assert record.event_id == "defer-event"
    async with pool.acquire() as connection:
        count = await connection.fetchval(f"SELECT count(*) FROM {schema}.delivery_attempts")
        attempt_count = await connection.fetchval(
            f"SELECT attempt_count FROM {schema}.outbox_events WHERE event_id='defer-event'"
        )
    assert count == 0
    assert attempt_count == 0


async def test_expired_pre_boundary_lease_is_recoverable(repository) -> None:
    instance, pool, schema = repository
    await _insert_outbox(pool, schema, "pre-boundary-crash")
    await instance.lease_outbox(
        worker_id="old-owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    async with pool.acquire() as connection:
        await connection.execute(
            f"UPDATE {schema}.outbox_events "
            "SET lease_until=clock_timestamp()-interval '1 second' "
            "WHERE event_id='pre-boundary-crash'"
        )

    record = (
        await instance.lease_outbox(
            worker_id="new-owner",
            route_id="formal-route",
            official_stream_id="official",
            lineage_id="lineage-1",
        )
    )[0]
    assert record.event_id == "pre-boundary-crash"


async def test_after_begin_other_workers_can_never_lease(repository) -> None:
    instance, pool, schema = repository
    await _insert_outbox(pool, schema, "crash-event")
    await instance.lease_outbox(
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    await instance.begin_delivery_attempt(
        "crash-event",
        worker_id="owner",
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    async with pool.acquire() as connection:
        await connection.execute(
            f"UPDATE {schema}.outbox_events "
            "SET lease_until=clock_timestamp()-interval '1 second' "
            "WHERE event_id='crash-event'"
        )
    for index in range(10):
        assert (
            await instance.lease_outbox(
                worker_id=f"restart-{index}",
                route_id="formal-route",
                official_stream_id="official",
                lineage_id="lineage-1",
            )
            == []
        )


async def test_concurrent_002_migrations_execute_once_under_lock() -> None:
    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=2, max_size=4)
    try:
        baseline = _baseline_for_schema(schema)
        async with pool.acquire() as connection:
            await connection.execute(f'CREATE SCHEMA "{schema}"')
            await connection.execute(baseline)

        repositories = [V20Repository(_config(schema), shared_pool=pool) for _ in range(2)]
        await asyncio.gather(*(repository.migrate() for repository in repositories))
        async with pool.acquire() as connection:
            receipts = await connection.fetch(f"SELECT applied_at FROM {schema}.migration_receipts")
        assert len(receipts) == 1
    finally:
        await _drop_schema(pool, schema)
        await pool.close()


async def test_concurrent_fresh_migrations_acquire_lock_before_001_and_002() -> None:
    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=2, max_size=4)
    try:
        repositories = [V20Repository(_config(schema), shared_pool=pool) for _ in range(2)]
        await asyncio.gather(*(repository.migrate() for repository in repositories))
        async with pool.acquire() as connection:
            receipts = await connection.fetch(f"SELECT applied_at FROM {schema}.migration_receipts")
            outbox_table = await connection.fetchval(
                "SELECT to_regclass($1)", f"{schema}.outbox_events"
            )
        assert len(receipts) == 1
        assert outbox_table is not None
    finally:
        await _drop_schema(pool, schema)
        await pool.close()


async def test_database_clock_chooses_delivery_variant(repository) -> None:
    instance, pool, schema = repository
    for suffix, offset, expected in (
        ("future", 5.5, "ACTIONABLE"),
        ("equal", 5.0, "EXPIRED_NOTICE"),
        ("past", 4.5, "EXPIRED_NOTICE"),
    ):
        event_id = f"variant-{suffix}"
        async with pool.acquire() as connection:
            expiry = await connection.fetchval(
                "SELECT clock_timestamp() + make_interval(secs => $1::double precision)",
                offset,
            )
        await _insert_outbox(pool, schema, event_id, expiry=expiry)
        await instance.lease_outbox(
            worker_id=f"owner-{suffix}",
            route_id="formal-route",
            official_stream_id="official",
            lineage_id="lineage-1",
        )
        attempt = await instance.begin_delivery_attempt(
            event_id,
            worker_id=f"owner-{suffix}",
            route_id="formal-route",
            official_stream_id="official",
            lineage_id="lineage-1",
            action_reserve_seconds=5.0,
        )
        assert attempt.delivery_variant == expected


async def test_health_counts_unknown_even_without_last_error(repository) -> None:
    instance, pool, schema = repository
    await _insert_outbox(pool, schema, "unknown-null-error", status="DELIVERY_UNKNOWN")
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {schema}.delivery_attempts
                (event_id,attempt_number,phase,worker_id,completed_at,error_text,delivery_variant)
            VALUES
                (
                    'unknown-null-error',1,'UNKNOWN','owner',clock_timestamp(),
                    'terminal','LEGACY_UNKNOWN'
                )
            """
        )
    health = await instance.get_outbox_health(
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    assert health["unknown_n"] == 1
    assert health["terminal_unknown_n"] == 1
    assert health["delivery_error_n"] == 1


async def test_runtime_dynamic_schema_matches_standalone_001_plus_002(repository) -> None:
    _, _, schema = repository
    assert f"CREATE SCHEMA IF NOT EXISTS {schema}" in migration_sql(schema)
    assert f"{schema}.delivery_quarantine" in migration_sql(schema)
