from __future__ import annotations

import asyncio
import os
import uuid
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import asyncpg
import pytest

from src.data.database.v20_mews_guard_store import V20MewsGuardStore
from src.data.database.v20_repository import (
    V20DatabaseConfig,
    V20Repository,
    V20SemanticConflict,
    canonical_json,
    sha256_json,
)

DSN = os.environ.get("V20_TEST_POSTGRES_DSN", "")
SAFE_PREFIX = "v20_test_mews_guard_store_"
SHANGHAI = ZoneInfo("Asia/Shanghai")
SOURCE_DATE = date(2026, 8, 31)
D1 = date(2026, 9, 1)
D2 = date(2026, 9, 2)
CUTOFF = datetime(2026, 9, 1, 9, 40, tzinfo=SHANGHAI)
ON_TIME = datetime(2026, 9, 1, 9, 15, tzinfo=SHANGHAI)
LATE = datetime(2026, 9, 1, 14, 4, tzinfo=SHANGHAI)
D2_ON_TIME = datetime(2026, 9, 2, 9, 15, tzinfo=SHANGHAI)
D2_CUTOFF = datetime(2026, 9, 2, 9, 40, tzinfo=SHANGHAI)

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        not DSN,
        reason="V20_TEST_POSTGRES_DSN is required for real PostgreSQL MEWS guard-store tests",
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


def _payload(
    snapshot_id: str,
    *,
    source_trade_date: date = SOURCE_DATE,
    generated_at: datetime = LATE,
    availability_date: date = D1,
    profile: str = "LOCAL_TUSHARE_MEWS_V2_0910_V1",
) -> dict[str, Any]:
    return {
        "snapshot_id": snapshot_id,
        "source_trade_date": source_trade_date.isoformat(),
        "generated_at": generated_at.isoformat(),
        "fast_state": "DANGER",
        "model_version": "mews_v2",
        "data_version": f"data-{snapshot_id}",
        "evidence": {
            "profile": profile,
            "signal_available_date": availability_date.isoformat(),
        },
    }


async def _drop_schema(pool: asyncpg.Pool, schema: str) -> None:
    assert schema.startswith(SAFE_PREFIX)
    async with pool.acquire() as connection:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def _insert_snapshot(
    pool: asyncpg.Pool,
    schema: str,
    snapshot_id: str,
    *,
    source_trade_date: date = SOURCE_DATE,
    generated_at: datetime = LATE,
    receipt_sealed_at: datetime | None,
    signal_available_date: date = D1,
    profile: str = "LOCAL_TUSHARE_MEWS_V2_0910_V1",
    content_hash: str | None = None,
) -> None:
    payload = _payload(
        snapshot_id,
        source_trade_date=source_trade_date,
        generated_at=generated_at,
        availability_date=signal_available_date,
        profile=profile,
    )
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {schema}.mews_snapshots
                (snapshot_id,source_trade_date,generated_at,received_at,
                 receipt_sealed_at,fast_state,model_version,data_version,
                 content_hash,snapshot_json)
            VALUES ($1,$2,$3,$4,$5,'DANGER','mews_v2',$6,$7,$8::jsonb)
            """,
            snapshot_id,
            source_trade_date,
            generated_at,
            generated_at + timedelta(seconds=1),
            receipt_sealed_at,
            payload["data_version"],
            content_hash or sha256_json(payload),
            canonical_json(payload),
        )


async def _insert_event(
    pool: asyncpg.Pool,
    schema: str,
    event_id: str,
    event_type: str,
    *,
    action_expiry_ts: datetime | None = None,
) -> None:
    if event_type == "ENTRY_DECISION" and action_expiry_ts is None:
        raise ValueError("ENTRY_DECISION fixtures require an action expiry")
    event_ts = (
        action_expiry_ts - timedelta(minutes=1)
        if event_type == "ENTRY_DECISION" and action_expiry_ts is not None
        else ON_TIME
    )
    semantic = {"event_type": event_type, "event_id": event_id}
    payload = {"event_id": event_id}
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {schema}.outbox_events
                (event_id,event_type,route_id,official_stream_id,lineage_id,
                 semantic_content_hash,semantic_json,payload_json,payload_hash,
                 seal_status,delivery_status,action_expiry_ts,generated_at,
                 commit_marker,available_at,created_at)
            VALUES ($1,$2,'test-route','test-stream','test-lineage',$3,$4::jsonb,
                    $5::jsonb,$6,'SEALED','PENDING',$7,$8,
                    nextval('{schema}.commit_marker_seq'),$8,$8)
            """,
            event_id,
            event_type,
            sha256_json(semantic),
            canonical_json(semantic),
            canonical_json(payload),
            sha256_json(payload),
            action_expiry_ts,
            event_ts,
        )


async def _insert_manual_leg(
    pool: asyncpg.Pool,
    schema: str,
    model_leg_id: str,
    *,
    d1: date,
) -> None:
    signal_date = d1 - timedelta(days=1)
    source_event = f"{model_leg_id}-source"
    official_event = f"{model_leg_id}-official"
    batch_id = f"{model_leg_id}-batch"
    await _insert_event(
        pool,
        schema,
        official_event,
        "ENTRY_DECISION",
        action_expiry_ts=datetime(
            signal_date.year,
            signal_date.month,
            signal_date.day,
            9,
            40,
            tzinfo=SHANGHAI,
        ),
    )
    await _insert_event(pool, schema, source_event, "DATA_ALERT")
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {schema}.model_batches
                (model_batch_id,decision_id,origin_kind,source_event_id,
                 official_stream_id,lineage_id,signal_date,multiplier,
                 evaluation_only,reference_profile_id,created_at)
            VALUES ($1,NULL,'MANUAL_MONITOR',$2,'test-stream','test-lineage',
                    $3,1.0,FALSE,'test-profile',$4)
            """,
            batch_id,
            source_event,
            signal_date,
            ON_TIME,
        )
        enrollment = {"model_leg_id": model_leg_id}
        await connection.execute(
            f"""
            INSERT INTO {schema}.manual_monitor_enrollments
                (enrollment_id,source_event_id,official_entry_event_id,model_batch_id,
                 request_id,official_stream_id,lineage_id,signal_date,d1,d2,
                 activation_cutoff_ts,source_semantic_content_hash,source_payload_hash,
                 calendar_evidence_hash,enrollment_semantic_hash,enrollment_fingerprint,
                 enrollment_json,created_at)
            VALUES ($1,$2,$3,$4,$5,'test-stream','test-lineage',$6,$7,$8,$9,
                    $10,$11,$12,$13,$14,$15::jsonb,$16)
            """,
            f"{model_leg_id}-enrollment",
            source_event,
            official_event,
            batch_id,
            f"{model_leg_id}-request",
            signal_date,
            d1,
            d1 + timedelta(days=1),
            CUTOFF,
            *([sha256_json(enrollment)] * 5),
            canonical_json(enrollment),
            ON_TIME,
        )
        await connection.execute(
            f"""
            INSERT INTO {schema}.model_legs
                (model_leg_id,model_batch_id,code,stock_name,rank,relative_weight,
                 d1,d2,created_at)
            VALUES ($1,$2,'600000','stock',1,1.0,$3,$4,$5)
            """,
            model_leg_id,
            batch_id,
            d1,
            d1 + timedelta(days=1),
            ON_TIME,
        )


class _CoordinatedTransaction:
    def __init__(self, inner: object) -> None:
        self.inner = inner

    async def __aenter__(self) -> None:
        await self.inner.__aenter__()  # type: ignore[attr-defined]

    async def __aexit__(self, *args: object) -> bool:
        return await self.inner.__aexit__(*args)  # type: ignore[attr-defined]


class _CoordinatedConnection:
    def __init__(
        self,
        inner: asyncpg.Connection,
        first_insert_started: asyncio.Event,
        second_started: asyncio.Event,
        overlap_confirmed: asyncio.Event,
        insert_attempts: list[int],
    ) -> None:
        self.inner = inner
        self.first_insert_started = first_insert_started
        self.second_started = second_started
        self.overlap_confirmed = overlap_confirmed
        self.insert_attempts = insert_attempts

    def transaction(self, *, isolation: str) -> _CoordinatedTransaction:
        return _CoordinatedTransaction(self.inner.transaction(isolation=isolation))

    async def fetchrow(self, sql: str, *args: object) -> Any:
        if "FOR UPDATE OF leg" in sql and (
            self.first_insert_started.is_set() and not self.second_started.is_set()
        ):
            self.second_started.set()
        if "INSERT INTO" in sql and "leg_mews_selection" in sql:
            if not self.first_insert_started.is_set():
                self.first_insert_started.set()
                await asyncio.wait_for(self.overlap_confirmed.wait(), timeout=3)
            self.insert_attempts[0] += 1
        return await self.inner.fetchrow(sql, *args)


class _CoordinatedPool:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool
        self.first_insert_started = asyncio.Event()
        self.second_started = asyncio.Event()
        self.overlap_confirmed = asyncio.Event()
        self.insert_attempts = [0]

    def acquire(self) -> _AsyncContextLike:
        return _AsyncContextLike(self)


class _AsyncContextLike:
    def __init__(self, pool: _CoordinatedPool) -> None:
        self.pool = pool

    async def __aenter__(self) -> _CoordinatedConnection:
        self.inner_context = self.pool.pool.acquire()
        inner = await self.inner_context.__aenter__()
        self.connection = _CoordinatedConnection(
            inner,
            self.pool.first_insert_started,
            self.pool.second_started,
            self.pool.overlap_confirmed,
            self.pool.insert_attempts,
        )
        return self.connection

    async def __aexit__(self, *args: object) -> None:
        await self.inner_context.__aexit__(*args)


@pytest.fixture
async def guard_store():
    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=1, max_size=4)
    repository = V20Repository(_config(schema), shared_pool=pool)
    try:
        await repository.connect(migrate=True)
        await _insert_snapshot(
            pool,
            schema,
            "on-time",
            generated_at=ON_TIME,
            receipt_sealed_at=ON_TIME + timedelta(minutes=1),
        )
        await _insert_snapshot(
            pool,
            schema,
            "late",
            generated_at=LATE,
            receipt_sealed_at=LATE + timedelta(minutes=1),
        )
        await _insert_snapshot(
            pool,
            schema,
            "unsealed",
            generated_at=LATE,
            receipt_sealed_at=None,
        )
        await _insert_snapshot(
            pool,
            schema,
            "wrong-profile",
            generated_at=LATE,
            receipt_sealed_at=LATE + timedelta(minutes=1),
            profile="REMOTE_UNREVIEWED",
        )
        await _insert_snapshot(
            pool,
            schema,
            "wrong-generated-date",
            generated_at=LATE - timedelta(days=1),
            receipt_sealed_at=LATE + timedelta(minutes=1),
        )
        await _insert_snapshot(
            pool,
            schema,
            "d2-current-day",
            source_trade_date=D1,
            generated_at=D2_ON_TIME,
            receipt_sealed_at=D2_ON_TIME + timedelta(minutes=1),
            signal_available_date=D2,
        )
        yield V20MewsGuardStore(repository), repository, pool, schema
    finally:
        await _drop_schema(pool, schema)
        await pool.close()


async def _selection_row(
    pool: asyncpg.Pool,
    schema: str,
    model_leg_id: str,
) -> dict[str, Any]:
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            f"""
            SELECT ctid::text AS ctid,xmin::text::bigint AS xmin,
                   model_leg_id,snapshot_id,fast_state,cutoff_ts,
                   selection_reason,selected_at
            FROM {schema}.leg_mews_selection
            WHERE model_leg_id=$1
            """,
            model_leg_id,
        )
    assert row is not None
    return dict(row)


async def _insert_exit_intent(
    pool: asyncpg.Pool,
    schema: str,
    model_leg_id: str,
) -> None:
    event_id = f"{model_leg_id}-exit-event"
    await _insert_event(pool, schema, event_id, "EXIT_SIGNAL")
    semantic = {"model_leg_id": model_leg_id, "signal_type": "MEWS"}
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {schema}.exit_intents
                (exit_intent_id,model_leg_id,event_id,signal_type,trigger_ts,
                 rule_actionable_from,semantic_content_hash,commit_fingerprint,
                 semantic_json,initial_exit_persisted_local_date)
            VALUES ($1,$2,$3,'MEWS',$4,$5,$6,$7,$8::jsonb,$9)
            """,
            f"{model_leg_id}-intent",
            model_leg_id,
            event_id,
            ON_TIME,
            ON_TIME,
            sha256_json(semantic),
            sha256_json({"commit": model_leg_id}),
            canonical_json(semantic),
            D1,
        )


async def test_postgres_on_time_and_late_discovery_boundaries(
    guard_store: tuple[V20MewsGuardStore, V20Repository, asyncpg.Pool, str],
) -> None:
    store, _repository, pool, schema = guard_store
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            UPDATE {schema}.mews_snapshots SET receipt_sealed_at=NULL
            WHERE snapshot_id='late'
            """
        )
    assert (
        await store.find_eligible_snapshot(
            source_trade_date=SOURCE_DATE,
            cutoff=CUTOFF,
            availability_date=D1,
        )
        == "on-time"
    )
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            UPDATE {schema}.mews_snapshots
            SET receipt_sealed_at=$1,received_at=$2
            WHERE snapshot_id='late'
            """,
            LATE + timedelta(minutes=1),
            LATE + timedelta(seconds=1),
        )
    assert (
        await store.find_eligible_snapshot(
            source_trade_date=SOURCE_DATE,
            cutoff=CUTOFF,
            availability_date=D1,
        )
        == "late"
    )


async def test_postgres_hash_conflict_fails_closed(
    guard_store: tuple[V20MewsGuardStore, V20Repository, asyncpg.Pool, str],
) -> None:
    store, _repository, pool, schema = guard_store
    payload = _payload("dirty-hash", generated_at=LATE)
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {schema}.mews_snapshots
                (snapshot_id,source_trade_date,generated_at,received_at,
                 receipt_sealed_at,fast_state,model_version,data_version,
                 content_hash,snapshot_json)
            VALUES ('dirty-hash',$1,$2,$3,$4,'DANGER','mews_v2',$5,$6,$7::jsonb)
            """,
            SOURCE_DATE,
            LATE,
            LATE + timedelta(seconds=1),
            LATE + timedelta(minutes=2),
            payload["data_version"],
            "0" * 64,
            canonical_json(payload),
        )

    with pytest.raises(V20SemanticConflict, match="content_hash"):
        await store.find_eligible_snapshot(
            source_trade_date=SOURCE_DATE,
            cutoff=CUTOFF,
            availability_date=D1,
        )


async def test_postgres_selection_existing_cutoff_and_concurrency(
    guard_store: tuple[V20MewsGuardStore, V20Repository, asyncpg.Pool, str],
) -> None:
    store, _repository, pool, schema = guard_store
    await _insert_manual_leg(pool, schema, "leg-concurrent", d1=D1)

    async def forbidden_legacy_load(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("guard store must construct its return row in the same transaction")

    repository = guard_store[1]
    repository.load_selected_mews_for_leg = forbidden_legacy_load  # type: ignore[method-assign]
    await _insert_manual_leg(pool, schema, "leg-on-time", d1=D1)
    on_time_record = await store.select_freeze_and_load(
        "leg-on-time",
        d1=D1,
        cutoff=CUTOFF,
    )
    assert on_time_record.snapshot_id == "on-time"
    assert on_time_record.source_trade_date == SOURCE_DATE
    assert on_time_record.selection_reason == "ELIGIBLE"
    first, second = await asyncio.gather(
        store.select_and_freeze_for_leg(
            "leg-concurrent",
            d1=D1,
            cutoff=CUTOFF,
            late_source_trade_date=SOURCE_DATE,
            late_availability_date=D1,
        ),
        store.select_and_freeze_for_leg(
            "leg-concurrent",
            d1=D1,
            cutoff=CUTOFF,
            late_source_trade_date=SOURCE_DATE,
            late_availability_date=D1,
        ),
    )
    expected = ("late", "DANGER", "ELIGIBLE_LATE_SAME_DAY")
    assert first == expected
    assert second == expected
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            f"""
            SELECT snapshot_id,fast_state,cutoff_ts,selection_reason
            FROM {schema}.leg_mews_selection
            WHERE model_leg_id='leg-concurrent'
            """
        )
    assert len(rows) == 1
    assert rows[0]["snapshot_id"] == "late"
    assert rows[0]["selection_reason"] == "ELIGIBLE_LATE_SAME_DAY"

    with pytest.raises(V20SemanticConflict, match="different cutoff"):
        await store.select_and_freeze_for_leg(
            "leg-concurrent",
            d1=D1,
            cutoff=CUTOFF + timedelta(minutes=1),
            late_source_trade_date=SOURCE_DATE,
            late_availability_date=D1,
        )


async def test_postgres_no_candidate_writes_one_null_fallback(
    guard_store: tuple[V20MewsGuardStore, V20Repository, asyncpg.Pool, str],
) -> None:
    store, _repository, pool, schema = guard_store
    d1 = date(2026, 8, 30)
    await _insert_manual_leg(pool, schema, "leg-fallback", d1=d1)
    result = await store.select_and_freeze_for_leg(
        "leg-fallback",
        d1=d1,
        cutoff=CUTOFF,
        late_source_trade_date=d1 - timedelta(days=1),
        late_availability_date=d1,
    )
    assert result == (None, None, "MEWS_UNAVAILABLE_FALLBACK_12")
    async with pool.acquire() as connection:
        count = await connection.fetchval(
            f"""
            SELECT count(*) FROM {schema}.leg_mews_selection
            WHERE model_leg_id='leg-fallback'
            """
        )
    assert count == 1


async def test_postgres_d2_current_day_selection_and_conflict_idempotency(
    guard_store: tuple[V20MewsGuardStore, V20Repository, asyncpg.Pool, str],
) -> None:
    store, _repository, pool, schema = guard_store
    await _insert_manual_leg(pool, schema, "leg-d2-current-day", d1=D1)

    first, second = await asyncio.gather(
        store.select_and_freeze_for_leg(
            "leg-d2-current-day",
            d1=D1,
            cutoff=D2_CUTOFF,
            late_source_trade_date=D1,
            late_availability_date=D2,
            evaluation_date=D2,
        ),
        store.select_and_freeze_for_leg(
            "leg-d2-current-day",
            d1=D1,
            cutoff=D2_CUTOFF,
            late_source_trade_date=D1,
            late_availability_date=D2,
            evaluation_date=D2,
        ),
    )
    assert first == ("d2-current-day", "DANGER", "ELIGIBLE")
    assert second == first

    frozen = await _selection_row(pool, schema, "leg-d2-current-day")
    assert frozen["snapshot_id"] == "d2-current-day"
    assert frozen["cutoff_ts"] == D2_CUTOFF
    assert frozen["selection_reason"] == "ELIGIBLE"

    with pytest.raises(V20SemanticConflict, match="invalid cutoff"):
        await store.select_and_freeze_for_leg(
            "leg-d2-current-day",
            d1=D1,
            cutoff=D2_CUTOFF + timedelta(minutes=1),
            late_source_trade_date=D1,
            late_availability_date=D2,
            evaluation_date=D2,
        )
    assert await _selection_row(pool, schema, "leg-d2-current-day") == frozen


async def test_postgres_d2_upgrades_legacy_selection_without_intent_once(
    guard_store: tuple[V20MewsGuardStore, V20Repository, asyncpg.Pool, str],
) -> None:
    store, _repository, pool, schema = guard_store
    await _insert_manual_leg(pool, schema, "leg-d2-legacy-upgrade", d1=D1)
    legacy = await store.select_freeze_and_load(
        "leg-d2-legacy-upgrade",
        d1=D1,
        cutoff=CUTOFF,
    )
    assert legacy.snapshot_id == "on-time"
    assert legacy.source_trade_date == SOURCE_DATE
    assert legacy.selection_reason == "ELIGIBLE"
    legacy_row = await _selection_row(pool, schema, "leg-d2-legacy-upgrade")

    upgraded = await store.select_freeze_and_load(
        "leg-d2-legacy-upgrade",
        d1=D1,
        cutoff=D2_CUTOFF,
        late_source_trade_date=D1,
        late_availability_date=D2,
        evaluation_date=D2,
    )
    assert upgraded.snapshot_id == "d2-current-day"
    assert upgraded.source_trade_date == D1
    assert upgraded.generated_at == D2_ON_TIME
    assert upgraded.received_at == D2_ON_TIME + timedelta(seconds=1)
    assert upgraded.receipt_sealed_at == D2_ON_TIME + timedelta(minutes=1)
    assert upgraded.cutoff_ts == D2_CUTOFF
    assert upgraded.selection_reason == "ELIGIBLE"

    upgraded_row = await _selection_row(pool, schema, "leg-d2-legacy-upgrade")
    replay = await store.select_freeze_and_load(
        "leg-d2-legacy-upgrade",
        d1=D1,
        cutoff=D2_CUTOFF,
        late_source_trade_date=D1,
        late_availability_date=D2,
        evaluation_date=D2,
    )
    assert replay == upgraded
    assert await _selection_row(pool, schema, "leg-d2-legacy-upgrade") == upgraded_row
    assert upgraded_row != legacy_row


async def test_postgres_d2_preserves_legacy_selection_with_exit_intent(
    guard_store: tuple[V20MewsGuardStore, V20Repository, asyncpg.Pool, str],
) -> None:
    store, _repository, pool, schema = guard_store
    await _insert_manual_leg(pool, schema, "leg-d2-existing-intent", d1=D1)
    legacy = await store.select_freeze_and_load(
        "leg-d2-existing-intent",
        d1=D1,
        cutoff=CUTOFF,
    )
    assert legacy.snapshot_id == "on-time"
    assert legacy.source_trade_date == SOURCE_DATE
    assert legacy.selection_reason == "ELIGIBLE"
    await _insert_exit_intent(pool, schema, "leg-d2-existing-intent")
    before = await _selection_row(pool, schema, "leg-d2-existing-intent")

    preserved = await store.select_freeze_and_load(
        "leg-d2-existing-intent",
        d1=D1,
        cutoff=D2_CUTOFF,
        late_source_trade_date=D1,
        late_availability_date=D2,
        evaluation_date=D2,
    )
    assert preserved == legacy
    assert await _selection_row(pool, schema, "leg-d2-existing-intent") == before


async def test_postgres_d2_no_candidate_fallback_survives_repository_restart(
    guard_store: tuple[V20MewsGuardStore, V20Repository, asyncpg.Pool, str],
) -> None:
    store, _repository, pool, schema = guard_store
    async with pool.acquire() as connection:
        deleted = await connection.execute(
            f"DELETE FROM {schema}.mews_snapshots WHERE snapshot_id=$1",
            "d2-current-day",
        )
    assert deleted == "DELETE 1"
    await _insert_manual_leg(pool, schema, "leg-d2-fallback", d1=D1)
    result = await store.select_and_freeze_for_leg(
        "leg-d2-fallback",
        d1=D1,
        cutoff=D2_CUTOFF,
        late_source_trade_date=D1,
        late_availability_date=D2,
        evaluation_date=D2,
    )
    assert result == (None, None, "MEWS_UNAVAILABLE_FALLBACK_12")
    before = await _selection_row(pool, schema, "leg-d2-fallback")

    restart_repository = V20Repository(_config(schema), shared_pool=pool)
    await restart_repository.connect(migrate=True)
    try:
        restart_store = V20MewsGuardStore(restart_repository)
        replay = await restart_store.select_and_freeze_for_leg(
            "leg-d2-fallback",
            d1=D1,
            cutoff=D2_CUTOFF,
            late_source_trade_date=D1,
            late_availability_date=D2,
            evaluation_date=D2,
        )
        loaded = await restart_store.load_frozen_for_leg(
            "leg-d2-fallback",
            d1=D1,
            cutoff=D2_CUTOFF,
            late_source_trade_date=D1,
            evaluation_date=D2,
        )
    finally:
        await restart_repository.close()

    assert replay == result
    assert loaded is not None
    assert loaded.snapshot_id is None
    assert loaded.selection_reason == "MEWS_UNAVAILABLE_FALLBACK_12"
    assert await _selection_row(pool, schema, "leg-d2-fallback") == before


async def test_postgres_rejects_illegal_existing_freeze_without_mutation(
    guard_store: tuple[V20MewsGuardStore, V20Repository, asyncpg.Pool, str],
) -> None:
    store, _repository, pool, schema = guard_store
    await _insert_manual_leg(pool, schema, "leg-illegal-existing", d1=D1)
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {schema}.leg_mews_selection
                (model_leg_id,snapshot_id,fast_state,cutoff_ts,selection_reason)
            VALUES ('leg-illegal-existing','unsealed','DANGER',$1,'ELIGIBLE')
            """,
            CUTOFF,
        )
        before = await connection.fetchrow(
            f"""
            SELECT ctid::text AS ctid,xmin::text::bigint AS xmin,
                   model_leg_id,snapshot_id,fast_state,cutoff_ts,
                   selection_reason,selected_at
            FROM {schema}.leg_mews_selection
            WHERE model_leg_id='leg-illegal-existing'
            """
        )

    with pytest.raises(V20SemanticConflict):
        await store.select_freeze_and_load(
            "leg-illegal-existing",
            d1=D1,
            cutoff=CUTOFF,
            late_source_trade_date=SOURCE_DATE,
            late_availability_date=D1,
        )

    async with pool.acquire() as connection:
        after = await connection.fetchrow(
            f"""
            SELECT ctid::text AS ctid,xmin::text::bigint AS xmin,
                   model_leg_id,snapshot_id,fast_state,cutoff_ts,
                   selection_reason,selected_at
            FROM {schema}.leg_mews_selection
            WHERE model_leg_id='leg-illegal-existing'
            """
        )
    assert dict(after) == dict(before)
