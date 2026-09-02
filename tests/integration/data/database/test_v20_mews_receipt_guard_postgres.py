from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import asyncpg
import pytest

from src.data.database.v20_mews_receipt_guard import V20MewsReceiptGuard
from src.data.database.v20_repository import (
    V20DatabaseConfig,
    V20Repository,
    canonical_json,
    sha256_json,
)

DSN = os.environ.get("V20_TEST_POSTGRES_DSN", "")
SAFE_PREFIX = "v20_test_mews_guard_"
SHANGHAI = ZoneInfo("Asia/Shanghai")
SOURCE_DATE = date(2026, 8, 31)
AVAILABILITY_DATE = date(2026, 9, 1)
CUTOFF = datetime(2026, 9, 1, 9, 40, tzinfo=SHANGHAI)
ON_TIME = datetime(2026, 9, 1, 9, 15, tzinfo=SHANGHAI)
LATE = datetime(2026, 9, 1, 14, 4, tzinfo=SHANGHAI)

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        not DSN,
        reason="V20_TEST_POSTGRES_DSN is required for real PostgreSQL MEWS receipt tests",
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


def _payload(
    snapshot_id: str,
    *,
    source_trade_date: date = SOURCE_DATE,
    signal_available_date: date = AVAILABILITY_DATE,
) -> dict[str, Any]:
    return {
        "snapshot_id": snapshot_id,
        "source_trade_date": source_trade_date.isoformat(),
        "generated_at": LATE.isoformat(),
        "fast_state": "DANGER",
        "model_version": "mews_v2",
        "data_version": f"data-{snapshot_id}",
        "evidence": {"signal_available_date": signal_available_date.isoformat()},
    }


async def _insert_snapshot(
    pool: asyncpg.Pool,
    schema: str,
    snapshot_id: str,
    *,
    source_trade_date: date = SOURCE_DATE,
    generated_at: datetime = LATE,
    receipt_sealed_at: datetime | None,
    signal_available_date: date = AVAILABILITY_DATE,
) -> None:
    payload = _payload(
        snapshot_id,
        source_trade_date=source_trade_date,
        signal_available_date=signal_available_date,
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
            generated_at + timedelta(minutes=1),
            receipt_sealed_at,
            f"data-{snapshot_id}",
            sha256_json(payload),
            canonical_json(payload),
        )


@pytest.fixture
async def receipt_guard():
    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=1, max_size=4)
    repository = V20Repository(_config(schema), shared_pool=pool)
    try:
        await repository.connect(migrate=True)
        snapshots = {
            "on-time-sealed": dict(
                source_trade_date=SOURCE_DATE,
                generated_at=ON_TIME,
                receipt_sealed_at=ON_TIME + timedelta(minutes=1),
                signal_available_date=AVAILABILITY_DATE,
            ),
            "late-sealed": dict(
                source_trade_date=SOURCE_DATE,
                generated_at=LATE,
                receipt_sealed_at=LATE + timedelta(minutes=1),
                signal_available_date=AVAILABILITY_DATE,
            ),
            "late-unsealed": dict(
                source_trade_date=SOURCE_DATE,
                generated_at=LATE,
                receipt_sealed_at=None,
                signal_available_date=AVAILABILITY_DATE,
            ),
            "wrong-source": dict(
                source_trade_date=SOURCE_DATE - timedelta(days=1),
                generated_at=ON_TIME,
                receipt_sealed_at=ON_TIME + timedelta(minutes=1),
                signal_available_date=AVAILABILITY_DATE,
            ),
            "wrong-evidence": dict(
                source_trade_date=SOURCE_DATE,
                generated_at=LATE,
                receipt_sealed_at=LATE + timedelta(minutes=1),
                signal_available_date=SOURCE_DATE,
            ),
            "wrong-generated-date": dict(
                source_trade_date=SOURCE_DATE,
                generated_at=LATE - timedelta(days=1),
                receipt_sealed_at=LATE + timedelta(minutes=1),
                signal_available_date=AVAILABILITY_DATE,
            ),
            "wrong-sealed-date": dict(
                source_trade_date=SOURCE_DATE,
                generated_at=LATE,
                receipt_sealed_at=LATE - timedelta(days=1),
                signal_available_date=AVAILABILITY_DATE,
            ),
        }
        for snapshot_id, fields in snapshots.items():
            await _insert_snapshot(pool, schema, snapshot_id, **fields)
        yield V20MewsReceiptGuard(repository), repository, pool, schema
    finally:
        await _drop_schema(pool, schema)
        await pool.close()


async def _rows(
    pool: asyncpg.Pool,
    schema: str,
) -> dict[str, tuple[Any, ...]]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            f"""
            SELECT snapshot_id,source_trade_date,generated_at,received_at,
                   receipt_sealed_at,fast_state,model_version,data_version,
                   content_hash,snapshot_json,ctid::text AS ctid
            FROM {schema}.mews_snapshots
            ORDER BY snapshot_id
            """
        )
    return {row["snapshot_id"]: tuple(row.values()) for row in rows}


async def test_postgres_record_path_and_readonly_receipt_boundaries(
    receipt_guard: tuple[V20MewsReceiptGuard, V20Repository, asyncpg.Pool, str],
) -> None:
    guard, _repository, pool, schema = receipt_guard
    before = await _rows(pool, schema)
    assert len(before) == 7

    expected = {
        "on-time-sealed": True,
        "late-sealed": True,
        "late-unsealed": False,
        "wrong-source": False,
        "wrong-evidence": False,
        "wrong-generated-date": False,
        "wrong-sealed-date": False,
    }
    for snapshot_id, eligible in expected.items():
        assert (
            await guard.is_eligible(
                snapshot_id,
                source_trade_date=SOURCE_DATE,
                cutoff=CUTOFF,
                availability_date=AVAILABILITY_DATE,
            )
            is eligible
        )

    assert (
        await guard.is_eligible(
            "missing-snapshot",
            source_trade_date=SOURCE_DATE,
            cutoff=CUTOFF,
            availability_date=AVAILABILITY_DATE,
        )
        is False
    )
    assert await _rows(pool, schema) == before


async def test_postgres_record_mews_snapshot_to_restore_candidate_guard_chain(
    receipt_guard: tuple[V20MewsReceiptGuard, V20Repository, asyncpg.Pool, str],
) -> None:
    guard, repository, _pool, _schema = receipt_guard
    availability_date = datetime.now(SHANGHAI).date()
    generated_at = datetime.now(SHANGHAI).replace(microsecond=0)
    payload = _payload("record-method-sealed")
    payload["generated_at"] = generated_at.isoformat()
    payload["evidence"]["signal_available_date"] = availability_date.isoformat()

    content_hash = await repository.record_mews_snapshot(payload)
    assert content_hash == sha256_json(payload)

    restored_snapshot_id = await repository.find_eligible_mews_snapshot(
        source_trade_date=SOURCE_DATE,
        cutoff=CUTOFF,
        availability_date=availability_date,
    )
    assert restored_snapshot_id == "record-method-sealed"
    assert await guard.is_eligible(
        restored_snapshot_id,
        source_trade_date=SOURCE_DATE,
        cutoff=CUTOFF,
        availability_date=availability_date,
    )
