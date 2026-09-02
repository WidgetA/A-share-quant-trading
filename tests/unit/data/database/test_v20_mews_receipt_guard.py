from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.data.database.v20_mews_receipt_guard import V20MewsReceiptGuard

TZ = ZoneInfo("Asia/Shanghai")
SOURCE_DATE = date(2026, 8, 31)
AVAILABILITY_DATE = date(2026, 9, 1)
CUTOFF = datetime(2026, 9, 1, 9, 40, tzinfo=TZ)
ON_TIME = datetime(2026, 9, 1, 9, 15, tzinfo=TZ)
LATE = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)


class _AsyncContext:
    def __init__(self, value: Any) -> None:
        self.value = value

    async def __aenter__(self) -> Any:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


class _FakeConnection:
    def __init__(self, row: Any = None) -> None:
        self.row = row
        self.calls: list[tuple[str, str, tuple[Any, ...]]] = []

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        self.calls.append(("fetchrow", sql, args))
        return self.row


class _FakePool:
    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection

    def acquire(self) -> _AsyncContext:
        return _AsyncContext(self.connection)


def _repository(connection: _FakeConnection, schema: str = "v20") -> SimpleNamespace:
    return SimpleNamespace(schema=schema, pool=_FakePool(connection))


def _row(
    *,
    source_trade_date: date = SOURCE_DATE,
    generated_at: datetime = ON_TIME,
    receipt_sealed_at: datetime | None = ON_TIME + timedelta(minutes=1),
    signal_available_date: str | None = AVAILABILITY_DATE.isoformat(),
) -> dict[str, Any]:
    return {
        "source_trade_date": source_trade_date,
        "generated_at": generated_at,
        "receipt_sealed_at": receipt_sealed_at,
        "signal_available_date": signal_available_date,
    }


async def test_on_time_generated_and_sealed_receipt_is_eligible() -> None:
    connection = _FakeConnection(_row())
    guard = V20MewsReceiptGuard(_repository(connection))

    assert await guard.is_eligible(
        "mews-on-time",
        source_trade_date=SOURCE_DATE,
        cutoff=CUTOFF,
        availability_date=AVAILABILITY_DATE,
    )
    assert connection.calls[0][1].lstrip().startswith("SELECT source_trade_date")
    assert "FROM v20.mews_snapshots" in connection.calls[0][1]
    assert connection.calls[0][2] == ("mews-on-time",)


async def test_asyncpg_like_indexable_record_is_eligible() -> None:
    class AsyncpgLikeRecord:
        def __init__(self, values: dict[str, Any]) -> None:
            self._values = values

        def __getitem__(self, key: str) -> Any:
            return self._values[key]

    row = AsyncpgLikeRecord(_row())
    assert not isinstance(row, Mapping)
    connection = _FakeConnection(row)
    guard = V20MewsReceiptGuard(_repository(connection))

    assert await guard.is_eligible(
        "mews-record",
        source_trade_date=SOURCE_DATE,
        cutoff=CUTOFF,
        availability_date=AVAILABILITY_DATE,
    )

    unsealed = AsyncpgLikeRecord(_row(generated_at=LATE, receipt_sealed_at=None))
    guard = V20MewsReceiptGuard(_repository(_FakeConnection(unsealed)))
    assert (
        await guard.is_eligible(
            "mews-record",
            source_trade_date=SOURCE_DATE,
            cutoff=CUTOFF,
            availability_date=AVAILABILITY_DATE,
        )
        is False
    )


async def test_indexable_record_with_missing_field_fails_closed() -> None:
    class MissingFieldRecord:
        def __getitem__(self, key: str) -> Any:
            raise KeyError(key)

    guard = V20MewsReceiptGuard(_repository(_FakeConnection(MissingFieldRecord())))
    assert (
        await guard.is_eligible(
            "mews-missing-field",
            source_trade_date=SOURCE_DATE,
            cutoff=CUTOFF,
            availability_date=AVAILABILITY_DATE,
        )
        is False
    )


async def test_same_day_late_repair_is_eligible_only_when_sealed_and_available() -> None:
    sealed_late = _row(generated_at=LATE, receipt_sealed_at=LATE + timedelta(minutes=1))
    guard = V20MewsReceiptGuard(_repository(_FakeConnection(sealed_late)))
    assert await guard.is_eligible(
        "mews-late",
        source_trade_date=SOURCE_DATE,
        cutoff=CUTOFF,
        availability_date=AVAILABILITY_DATE,
    )

    unsealed_late = _row(generated_at=LATE, receipt_sealed_at=None)
    guard = V20MewsReceiptGuard(_repository(_FakeConnection(unsealed_late)))
    eligible = await guard.is_eligible(
        "mews-late",
        source_trade_date=SOURCE_DATE,
        cutoff=CUTOFF,
        availability_date=AVAILABILITY_DATE,
    )
    assert eligible is False


@pytest.mark.parametrize(
    "row",
    [
        _row(
            generated_at=LATE,
            receipt_sealed_at=LATE,
            source_trade_date=SOURCE_DATE + timedelta(days=1),
        ),
        _row(
            generated_at=LATE,
            receipt_sealed_at=LATE,
            signal_available_date=SOURCE_DATE.isoformat(),
        ),
        _row(generated_at=LATE, receipt_sealed_at=LATE, signal_available_date=None),
        _row(generated_at=LATE, receipt_sealed_at=LATE, signal_available_date="not-a-date"),
        _row(
            generated_at=LATE - timedelta(days=1),
            receipt_sealed_at=LATE,
            signal_available_date=AVAILABILITY_DATE.isoformat(),
        ),
        _row(
            generated_at=LATE,
            receipt_sealed_at=LATE - timedelta(days=1),
            signal_available_date=AVAILABILITY_DATE.isoformat(),
        ),
    ],
    ids=[
        "wrong-source",
        "wrong-availability",
        "missing-availability",
        "bad-availability",
        "generated-on-wrong-date",
        "sealed-on-wrong-date",
    ],
)
async def test_inexact_source_or_availability_is_rejected(row: dict[str, Any]) -> None:
    guard = V20MewsReceiptGuard(_repository(_FakeConnection(row)))
    eligible = await guard.is_eligible(
        "mews-candidate",
        source_trade_date=SOURCE_DATE,
        cutoff=CUTOFF,
        availability_date=AVAILABILITY_DATE,
    )
    assert eligible is False


async def test_missing_row_is_rejected_without_mutation() -> None:
    connection = _FakeConnection(None)
    guard = V20MewsReceiptGuard(_repository(connection))

    eligible = await guard.is_eligible(
        "missing",
        source_trade_date=SOURCE_DATE,
        cutoff=CUTOFF,
        availability_date=AVAILABILITY_DATE,
    )
    assert eligible is False
    assert len(connection.calls) == 1
    sql = connection.calls[0][1]
    assert "SELECT " in sql
    assert "UPDATE " not in sql
    assert "INSERT " not in sql
    assert "DELETE " not in sql


@pytest.mark.parametrize("generated_at", [ON_TIME.replace(tzinfo=None), "2026-09-01T09:15:00Z"])
@pytest.mark.parametrize("receipt_sealed_at", [None, ON_TIME.replace(tzinfo=None), "sealed"])
async def test_invalid_database_datetime_types_fail_closed(
    generated_at: Any,
    receipt_sealed_at: Any,
) -> None:
    row = _row(generated_at=generated_at, receipt_sealed_at=receipt_sealed_at)
    guard = V20MewsReceiptGuard(_repository(_FakeConnection(row)))
    eligible = await guard.is_eligible(
        "mews-invalid",
        source_trade_date=SOURCE_DATE,
        cutoff=CUTOFF,
        availability_date=AVAILABILITY_DATE,
    )
    assert eligible is False


async def test_naive_cutoff_is_rejected_before_database_access() -> None:
    connection = _FakeConnection(_row())
    guard = V20MewsReceiptGuard(_repository(connection))
    with pytest.raises(ValueError, match="MEWS cutoff must be timezone-aware"):
        await guard.is_eligible(
            "mews-on-time",
            source_trade_date=SOURCE_DATE,
            cutoff=CUTOFF.replace(tzinfo=None),
        )
    assert connection.calls == []


@pytest.mark.parametrize(
    ("snapshot_id", "source_trade_date", "availability_date"),
    [(123, SOURCE_DATE, None), ("id", datetime.now(TZ), None), ("id", SOURCE_DATE, "2026-09-01")],
)
async def test_strict_input_types_are_rejected(
    snapshot_id: Any,
    source_trade_date: Any,
    availability_date: Any,
) -> None:
    connection = _FakeConnection(_row())
    guard = V20MewsReceiptGuard(_repository(connection))
    with pytest.raises((TypeError, ValueError)):
        await guard.is_eligible(
            snapshot_id,
            source_trade_date=source_trade_date,
            cutoff=CUTOFF,
            availability_date=availability_date,
        )
    assert connection.calls == []


async def test_database_exception_is_not_swallowed() -> None:
    class FailingConnection:
        async def fetchrow(self, *_args: object) -> Any:
            raise RuntimeError("database unavailable")

    class FailingPool:
        def acquire(self) -> _AsyncContext:
            return _AsyncContext(FailingConnection())

    guard = V20MewsReceiptGuard(SimpleNamespace(schema="v20", pool=FailingPool()))
    with pytest.raises(RuntimeError, match="database unavailable"):
        await guard.is_eligible(
            "mews-error",
            source_trade_date=SOURCE_DATE,
            cutoff=CUTOFF,
            availability_date=AVAILABILITY_DATE,
        )


def test_schema_is_validated_before_sql_rendering() -> None:
    with pytest.raises(ValueError, match="invalid PostgreSQL schema identifier"):
        V20MewsReceiptGuard(_repository(_FakeConnection(), schema="v20; DROP SCHEMA v20"))
