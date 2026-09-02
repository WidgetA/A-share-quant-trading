from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.data.database.v16_canonical_artifact_store import V16CanonicalArtifactStore
from src.data.database.v20_repository import V20SemanticConflict, canonical_json, sha256_json

TRADE_DATE = date(2026, 8, 31)
TZ = ZoneInfo("Asia/Shanghai")
RECEIVED_AT = datetime(2026, 8, 31, 9, 39, 59, tzinfo=TZ)


class AsyncpgLikeRecord:
    """Indexable row shape without inheriting ``collections.abc.Mapping``."""

    def __init__(self, values: dict[str, Any]) -> None:
        self._values = values

    def __getitem__(self, key: str) -> Any:
        return self._values[key]


class AsyncContext:
    def __init__(self, value: Any) -> None:
        self.value = value

    async def __aenter__(self) -> Any:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


class FakeConnection:
    def __init__(self) -> None:
        self.rows: dict[str, AsyncpgLikeRecord] = {}
        self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []

    def transaction(self) -> AsyncContext:
        return AsyncContext(None)

    async def execute(self, sql: str, *args: Any) -> str:
        self.execute_calls.append((sql, args))
        if "ON CONFLICT (snapshot_id) DO NOTHING" not in sql:
            raise AssertionError("canonical artifact writes must be idempotent")
        snapshot_id = str(args[0])
        if snapshot_id not in self.rows:
            payload = json_loads_param(args[4])
            self.rows[snapshot_id] = AsyncpgLikeRecord(
                {
                    "snapshot_id": snapshot_id,
                    "snapshot_type": args[1],
                    "trade_date": args[2],
                    "snapshot_hash": args[3],
                    "snapshot_json": canonical_json(payload),
                    "first_received_at": RECEIVED_AT,
                }
            )
        return "OK"

    async def fetchrow(self, _sql: str, snapshot_id: str) -> AsyncpgLikeRecord | None:
        return self.rows.get(snapshot_id)


def json_loads_param(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value.removesuffix("::jsonb"))
    return value


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def acquire(self) -> AsyncContext:
        return AsyncContext(self.connection)


def _repository(connection: FakeConnection) -> Any:
    return SimpleNamespace(schema="v16", pool=FakePool(connection))


def _portable(
    recommendations: int,
    *,
    computed_at: datetime = RECEIVED_AT,
) -> dict[str, Any]:
    codes = tuple(f"{index:06d}" for index in range(recommendations))
    return {
        "schema_version": "v16-canonical-portable-artifact/v1",
        "trade_date": TRADE_DATE.isoformat(),
        "input_hash": "c" * 64,
        "model_sha256": "a" * 64,
        "feature_list_sha256": "b" * 64,
        "raw_evidence_codes": ["000001", "000002", "600000"],
        "recommended": [
            {"rank": rank, "code": code, "name": f"stock-{code}"}
            for rank, code in enumerate(codes, start=1)
        ],
        "computed_at": computed_at.isoformat(),
    }


def _row_for(payload: dict[str, Any], **overrides: Any) -> AsyncpgLikeRecord:
    values = {
        "snapshot_id": "5dd890d0c865311fb71a7d2de63b587fa2e95d57e4e339b1c924830832b2e30",
        "snapshot_type": "V16_CANONICAL_MASTER_V1",
        "trade_date": TRADE_DATE,
        "snapshot_hash": sha256_json(payload),
        "snapshot_json": canonical_json(payload),
        "first_received_at": RECEIVED_AT,
    }
    values.update(overrides)
    row = AsyncpgLikeRecord(values)
    assert not isinstance(row, Mapping)
    return row


async def test_asyncpg_record_row_hydrates_portable_payload_and_requires_aware_receipt() -> None:
    connection = FakeConnection()
    portable = _portable(recommendations=3)
    first = await V16CanonicalArtifactStore(_repository(connection)).save_once(
        portable,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )

    assert isinstance(first.first_received_at, datetime)
    assert first.first_received_at.tzinfo is not None
    assert first.first_received_at.utcoffset() is not None
    assert first.payload == {key: value for key, value in portable.items() if key != "computed_at"}

    naive_row = _row_for(portable, first_received_at=RECEIVED_AT.replace(tzinfo=None))
    connection.rows[first.snapshot_id] = naive_row
    with pytest.raises(V20SemanticConflict, match="canonical artifact row is invalid"):
        await V16CanonicalArtifactStore(_repository(connection)).load(
            official_stream_id="official-stream",
            trade_date=TRADE_DATE,
        )


async def test_same_semantic_artifact_reuses_slot_when_only_computed_at_changes() -> None:
    connection = FakeConnection()
    store = V16CanonicalArtifactStore(_repository(connection))
    original = _portable(recommendations=10)
    recomputed = _portable(
        recommendations=10,
        computed_at=RECEIVED_AT + timedelta(seconds=17),
    )

    first = await store.save_once(
        original,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )
    second = await store.save_once(
        recomputed,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )

    assert second == first
    assert first.snapshot_hash == second.snapshot_hash
    assert len(connection.rows) == 1
    assert connection.rows[first.snapshot_id]["snapshot_hash"] == first.snapshot_hash


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("snapshot_hash", "f" * 64),
        ("snapshot_json", canonical_json({"forged": True})),
        ("snapshot_type", "V16_FORGED"),
        ("trade_date", TRADE_DATE + timedelta(days=1)),
    ],
    ids=["hash", "json", "type", "date"],
)
async def test_hash_and_slot_corruption_fail_closed(field: str, value: Any) -> None:
    connection = FakeConnection()
    portable = _portable(recommendations=1)
    first = await V16CanonicalArtifactStore(_repository(connection)).save_once(
        portable,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )
    original = dict(connection.rows[first.snapshot_id]._values)

    connection.rows[first.snapshot_id] = _row_for(portable, **{field: value})
    with pytest.raises(V20SemanticConflict, match="canonical artifact row is invalid"):
        await V16CanonicalArtifactStore(_repository(connection)).load(
            official_stream_id="official-stream",
            trade_date=TRADE_DATE,
        )
    assert connection.rows[first.snapshot_id]._values != original

    connection.rows[first.snapshot_id] = AsyncpgLikeRecord(original)
    with pytest.raises(V20SemanticConflict, match="canonical artifact slot collision"):
        await V16CanonicalArtifactStore(_repository(connection)).save_once(
            {**portable, "input_hash": "d" * 64},
            official_stream_id="official-stream",
            trade_date=TRADE_DATE,
        )


@pytest.mark.parametrize("recommendations", [0, 1, 9, 10])
async def test_portable_zero_one_sub_ten_and_ten_recommendations_round_trip_without_raw(
    recommendations: int,
) -> None:
    connection = FakeConnection()
    portable = _portable(recommendations=recommendations)
    record = await V16CanonicalArtifactStore(_repository(connection)).save_once(
        portable,
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )
    loaded = await V16CanonicalArtifactStore(_repository(connection)).load(
        official_stream_id="official-stream",
        trade_date=TRADE_DATE,
    )

    assert loaded == record
    assert loaded is not None
    assert loaded.payload == {key: value for key, value in portable.items() if key != "computed_at"}
    assert len(loaded.payload["recommended"]) == recommendations
    assert loaded.payload["raw_evidence_codes"] == ["000001", "000002", "600000"]
    assert not {"early_bars", "history_raw", "stock_data", "scan_result"} & set(loaded.payload)
