"""Read-only receipt validation for persisted V20 MEWS snapshots."""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_ROW_FIELDS = (
    "source_trade_date",
    "generated_at",
    "receipt_sealed_at",
    "signal_available_date",
)


def _require_aware_datetime(value: object, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value


class V20MewsReceiptGuard:
    """Validate one MEWS receipt without mutating repository state.

    The repository's discovery query may return an unsealed same-day repair
    candidate.  Callers must use this exact-snapshot check before installing a
    candidate into an in-memory cache: a candidate is valid only when storage
    owns a sealed receipt and either both timestamps predate the cutoff or its
    payload explicitly marks it as the requested availability date's repair.
    """

    def __init__(self, repository: Any) -> None:
        schema = getattr(repository, "schema", None)
        if not isinstance(schema, str) or not _IDENTIFIER.fullmatch(schema):
            raise ValueError(f"invalid PostgreSQL schema identifier: {schema!r}")
        self._repository = repository
        self._schema = schema

    async def is_eligible(
        self,
        snapshot_id: str,
        *,
        source_trade_date: date,
        cutoff: datetime,
        availability_date: date | None = None,
    ) -> bool:
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise ValueError("MEWS snapshot_id is invalid")
        if type(source_trade_date) is not date:
            raise TypeError("MEWS source_trade_date must be a date")
        cutoff = _require_aware_datetime(cutoff, "MEWS cutoff")
        if availability_date is not None and type(availability_date) is not date:
            raise TypeError("MEWS availability_date must be a date")

        async with self._repository.pool.acquire() as connection:
            row = await connection.fetchrow(
                f"""
                SELECT source_trade_date,generated_at,receipt_sealed_at,
                       snapshot_json->'evidence'->>'signal_available_date'
                           AS signal_available_date
                FROM {self._schema}.mews_snapshots
                WHERE snapshot_id=$1
                """,
                snapshot_id,
            )
        if row is None:
            return False
        try:
            record = {field: row[field] for field in _ROW_FIELDS}
        except (KeyError, IndexError, TypeError):
            return False
        if record["source_trade_date"] != source_trade_date:
            return False

        generated_at = record["generated_at"]
        receipt_sealed_at = record["receipt_sealed_at"]
        if (
            not isinstance(generated_at, datetime)
            or generated_at.tzinfo is None
            or generated_at.utcoffset() is None
            or not isinstance(receipt_sealed_at, datetime)
            or receipt_sealed_at.tzinfo is None
            or receipt_sealed_at.utcoffset() is None
        ):
            return False
        if generated_at < cutoff and receipt_sealed_at < cutoff:
            return True
        if availability_date is None:
            return False
        if (
            generated_at.astimezone(_SHANGHAI_TZ).date() != availability_date
            or receipt_sealed_at.astimezone(_SHANGHAI_TZ).date() != availability_date
        ):
            return False

        available = record["signal_available_date"]
        if not isinstance(available, str):
            return False
        try:
            evidence_availability = date.fromisoformat(available)
        except ValueError:
            return False
        return evidence_availability == availability_date
