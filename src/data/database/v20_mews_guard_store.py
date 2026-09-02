"""Strict PostgreSQL selection boundary for durable V20 MEWS snapshots."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from src.data.database.v20_repository import (
    SelectedMewsRecord,
    V20RepositoryError,
    V20SemanticConflict,
    _model_batch_authorization_sql,
    sha256_json,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_MODEL_VERSION = "mews_v2"
_EVIDENCE_PROFILE = "LOCAL_TUSHARE_MEWS_V2_0910_V1"
_FALLBACK_REASON = "MEWS_UNAVAILABLE_FALLBACK_12"
_TRANSACTION_RETRY_SQLSTATES = frozenset(("40001", "40P01", "23505"))
_TRANSACTION_RETRY_LIMIT = 8

_SNAPSHOT_COLUMNS = """
    snapshot.snapshot_id,snapshot.source_trade_date,snapshot.generated_at,
    snapshot.received_at,snapshot.receipt_sealed_at,snapshot.fast_state,
    snapshot.model_version,snapshot.data_version,snapshot.content_hash,
    snapshot.snapshot_json
"""

_STRICT_CANDIDATE_SQL = f"""
    SELECT {_SNAPSHOT_COLUMNS}
    FROM {{schema}}.mews_snapshots AS snapshot
    WHERE snapshot.receipt_sealed_at IS NOT NULL
      AND snapshot.source_trade_date=$1::date
      AND snapshot.model_version='{_MODEL_VERSION}'
      AND snapshot.snapshot_json->>'model_version'=snapshot.model_version
      AND snapshot.snapshot_json->>'source_trade_date'=snapshot.source_trade_date::text
      AND snapshot.snapshot_json->'evidence'->>'profile'='{_EVIDENCE_PROFILE}'
      AND snapshot.generated_at <= snapshot.received_at
      AND snapshot.received_at <= snapshot.receipt_sealed_at
      AND (
          (
            snapshot.generated_at < $2
            AND snapshot.receipt_sealed_at < $2
            AND snapshot.snapshot_json->'evidence'->>'signal_available_date' IS NOT NULL
            AND snapshot.snapshot_json->'evidence'->>'signal_available_date'
                =timezone('Asia/Shanghai',snapshot.generated_at)::date::text
            AND timezone('Asia/Shanghai',snapshot.generated_at)::date <= $3::date
          )
          OR (
            snapshot.source_trade_date=$1::date
            AND snapshot.snapshot_json->'evidence'->>'signal_available_date'=$3::text
            AND timezone('Asia/Shanghai',snapshot.generated_at)::date=$3::date
            AND timezone('Asia/Shanghai',snapshot.receipt_sealed_at)::date=$3::date
          )
      )
"""


def _aware(value: Any, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise V20SemanticConflict(f"MEWS snapshot {field} is not a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise V20SemanticConflict(f"MEWS snapshot {field} lacks a timezone")
    return value


def _retryable_transaction_failure(exc: BaseException) -> bool:
    code = getattr(exc, "pgcode", None) or getattr(exc, "sqlstate", None)
    return isinstance(code, str) and code in _TRANSACTION_RETRY_SQLSTATES


def _payload_json(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise V20SemanticConflict("MEWS snapshot_json is invalid JSON") from exc
    elif isinstance(value, (bytes, bytearray)):
        try:
            decoded = json.loads(value.decode("utf-8"))
        except (UnicodeDecodeError, TypeError, ValueError) as exc:
            raise V20SemanticConflict("MEWS snapshot_json is invalid JSON") from exc
    else:
        decoded = value
    if not isinstance(decoded, dict):
        raise V20SemanticConflict("MEWS snapshot_json is not an object")
    return decoded


def _null_snapshot_row() -> dict[str, None]:
    return {
        field: None
        for field in (
            "snapshot_id",
            "source_trade_date",
            "generated_at",
            "received_at",
            "receipt_sealed_at",
            "fast_state",
            "model_version",
            "data_version",
            "content_hash",
            "snapshot_json",
        )
    }


def _snapshot_from_row(
    row: Any,
    *,
    cutoff: datetime,
    source_trade_date: date | None,
    availability_date: date | None,
    source_must_precede: date | None,
) -> tuple[str, str, str]:
    if not hasattr(row, "__getitem__"):
        raise V20SemanticConflict("MEWS guard returned a non-Mapping database row")
    try:
        values = {
            field: row[field]
            for field in (
                "snapshot_id",
                "source_trade_date",
                "generated_at",
                "received_at",
                "receipt_sealed_at",
                "fast_state",
                "model_version",
                "data_version",
                "content_hash",
                "snapshot_json",
            )
        }
    except (KeyError, IndexError, TypeError) as exc:
        raise V20SemanticConflict("MEWS guard database row is incomplete") from exc
    if any(value is None for value in values.values()):
        raise V20SemanticConflict("MEWS guard database row contains NULL")
    if not isinstance(values["snapshot_id"], str) or not values["snapshot_id"]:
        raise V20SemanticConflict("MEWS snapshot_id is invalid")
    if type(values["source_trade_date"]) is not date:
        raise V20SemanticConflict("MEWS source_trade_date is invalid")
    if source_trade_date is not None and values["source_trade_date"] != source_trade_date:
        raise V20SemanticConflict("MEWS source_trade_date differs from request")
    if source_must_precede is not None and values["source_trade_date"] >= source_must_precede:
        raise V20SemanticConflict("MEWS source_trade_date is not before d1")

    generated_at = _aware(values["generated_at"], "generated_at")
    received_at = _aware(values["received_at"], "received_at")
    sealed_at = _aware(values["receipt_sealed_at"], "receipt_sealed_at")
    if not generated_at <= received_at <= sealed_at:
        raise V20SemanticConflict("MEWS generated/received/sealed timestamps are out of order")

    payload = _payload_json(values["snapshot_json"])
    evidence = payload.get("evidence")
    if not isinstance(evidence, Mapping):
        raise V20SemanticConflict("MEWS snapshot evidence is not an object")
    expected_evidence_source = values["source_trade_date"].isoformat()
    if evidence.get("source_trade_date") not in (None, expected_evidence_source):
        raise V20SemanticConflict("MEWS evidence source_trade_date differs from column")
    if evidence.get("profile") != _EVIDENCE_PROFILE:
        raise V20SemanticConflict("MEWS evidence profile is invalid")
    if payload.get("snapshot_id") != values["snapshot_id"]:
        raise V20SemanticConflict("MEWS snapshot_id differs from payload")
    if payload.get("source_trade_date") != expected_evidence_source:
        raise V20SemanticConflict("MEWS payload source_trade_date differs from column")
    if payload.get("fast_state") != values["fast_state"]:
        raise V20SemanticConflict("MEWS fast_state differs from payload")
    if values["fast_state"] not in ("NORMAL", "DANGER"):
        raise V20SemanticConflict("MEWS fast_state is unsupported")
    if payload.get("model_version") != values["model_version"]:
        raise V20SemanticConflict("MEWS model_version differs from payload")
    if values["model_version"] != _MODEL_VERSION:
        raise V20SemanticConflict("MEWS model_version is unsupported")
    if payload.get("data_version") != values["data_version"]:
        raise V20SemanticConflict("MEWS data_version differs from payload")
    if sha256_json(payload) != values["content_hash"]:
        raise V20SemanticConflict("MEWS content_hash differs from snapshot_json")
    payload_generated_at = payload.get("generated_at")
    if not isinstance(payload_generated_at, str):
        raise V20SemanticConflict("MEWS payload generated_at is invalid")
    try:
        parsed_generated_at = datetime.fromisoformat(payload_generated_at)
    except ValueError as exc:
        raise V20SemanticConflict("MEWS payload generated_at is invalid") from exc
    if parsed_generated_at != generated_at:
        raise V20SemanticConflict("MEWS payload generated_at differs from column")

    generated_shanghai_date = generated_at.astimezone(_SHANGHAI_TZ).date()
    on_time = generated_at < cutoff and sealed_at < cutoff
    late = False
    if availability_date is not None:
        raw_availability = evidence.get("signal_available_date")
        try:
            evidence_availability = (
                date.fromisoformat(raw_availability) if isinstance(raw_availability, str) else None
            )
        except ValueError as exc:
            raise V20SemanticConflict("MEWS evidence availability date is invalid") from exc
        on_time_evidence = evidence_availability == generated_shanghai_date
        late = (
            evidence_availability == availability_date
            and generated_shanghai_date == availability_date
            and sealed_at.astimezone(_SHANGHAI_TZ).date() == availability_date
        )
        if on_time and not on_time_evidence:
            raise V20SemanticConflict("MEWS evidence availability differs from generated_at date")
        if on_time and generated_shanghai_date > availability_date:
            raise V20SemanticConflict("MEWS on-time evidence is after its availability date")
    if not (on_time or late):
        raise V20SemanticConflict("MEWS snapshot violates receipt cutoff")
    if received_at < generated_at:
        raise V20SemanticConflict("MEWS received_at precedes generated_at")
    reason = "ELIGIBLE" if on_time else "ELIGIBLE_LATE_SAME_DAY"
    return values["snapshot_id"], str(values["fast_state"]), reason


class V20MewsGuardStore:
    """Discover and freeze only repository-owned, PIT-safe MEWS evidence."""

    def __init__(self, repository: Any) -> None:
        schema = getattr(repository, "schema", None)
        if not isinstance(schema, str) or not _IDENTIFIER.fullmatch(schema):
            raise ValueError(f"invalid PostgreSQL schema identifier: {schema!r}")
        pool = getattr(repository, "pool", None)
        if pool is None or not callable(getattr(pool, "acquire", None)):
            raise ValueError("V20 repository pool is invalid")
        self._repository = repository
        self._schema = schema

    async def find_eligible_snapshot(
        self,
        *,
        source_trade_date: date,
        cutoff: datetime,
        availability_date: date,
    ) -> str | None:
        if type(source_trade_date) is not date:
            raise TypeError("MEWS source_trade_date must be a date")
        cutoff = _aware(cutoff, "cutoff")
        if type(availability_date) is not date:
            raise TypeError("MEWS availability_date must be a date")
        sql = (
            _STRICT_CANDIDATE_SQL.format(schema=self._schema)
            + """
              ORDER BY snapshot.source_trade_date DESC NULLS LAST,
                       snapshot.receipt_sealed_at DESC NULLS LAST,
                       snapshot.generated_at DESC NULLS LAST,
                       snapshot.snapshot_id DESC
              LIMIT 1
            """
        )
        async with self._repository.pool.acquire() as connection:
            row = await connection.fetchrow(
                sql,
                source_trade_date,
                cutoff,
                availability_date,
            )
        if row is None:
            return None
        snapshot_id, _fast_state, _reason = _snapshot_from_row(
            row,
            cutoff=cutoff,
            source_trade_date=source_trade_date,
            availability_date=availability_date,
            source_must_precede=None,
        )
        return snapshot_id

    async def select_and_freeze_for_leg(
        self,
        model_leg_id: str,
        *,
        d1: date,
        cutoff: datetime,
        late_source_trade_date: date | None = None,
        late_availability_date: date | None = None,
    ) -> tuple[str | None, str | None, str]:
        record = await self.select_freeze_and_load(
            model_leg_id,
            d1=d1,
            cutoff=cutoff,
            late_source_trade_date=late_source_trade_date,
            late_availability_date=late_availability_date,
        )
        return record.snapshot_id, record.fast_state, record.selection_reason

    async def select_freeze_and_load(
        self,
        model_leg_id: str,
        *,
        d1: date,
        cutoff: datetime,
        late_source_trade_date: date | None = None,
        late_availability_date: date | None = None,
    ) -> SelectedMewsRecord:
        for attempt in range(_TRANSACTION_RETRY_LIMIT):
            try:
                return await self._select_freeze_once(
                    model_leg_id,
                    d1=d1,
                    cutoff=cutoff,
                    late_source_trade_date=late_source_trade_date,
                    late_availability_date=late_availability_date,
                )
            except Exception as exc:
                if attempt + 1 < _TRANSACTION_RETRY_LIMIT and _retryable_transaction_failure(exc):
                    continue
                raise
        raise AssertionError("unreachable transaction retry loop")

    async def _select_freeze_once(
        self,
        model_leg_id: str,
        *,
        d1: date,
        cutoff: datetime,
        late_source_trade_date: date | None = None,
        late_availability_date: date | None = None,
    ) -> SelectedMewsRecord:
        if not isinstance(model_leg_id, str) or not model_leg_id:
            raise ValueError("model_leg_id is invalid")
        if type(d1) is not date:
            raise TypeError("MEWS selection d1 must be a date")
        cutoff = _aware(cutoff, "cutoff")
        if (late_source_trade_date is None) != (late_availability_date is None):
            raise ValueError("late source and availability must be supplied together")
        if late_source_trade_date is not None:
            if type(late_source_trade_date) is not date:
                raise TypeError("MEWS late_source_trade_date must be a date")
            if type(late_availability_date) is not date:
                raise TypeError("MEWS late_availability_date must be a date")
            if late_source_trade_date >= d1 or late_availability_date != d1:
                raise ValueError("MEWS late selection dates violate d1")

        candidate_sql = f"""
            SELECT {_SNAPSHOT_COLUMNS}
            FROM {self._schema}.mews_snapshots AS snapshot
            WHERE snapshot.receipt_sealed_at IS NOT NULL
              AND snapshot.model_version='{_MODEL_VERSION}'
              AND snapshot.snapshot_json->>'model_version'=snapshot.model_version
              AND snapshot.snapshot_json->>'source_trade_date'
                  =snapshot.source_trade_date::text
              AND snapshot.snapshot_json->'evidence'->>'profile'
                  ='{_EVIDENCE_PROFILE}'
              AND snapshot.generated_at <= snapshot.received_at
              AND snapshot.received_at <= snapshot.receipt_sealed_at
              AND snapshot.source_trade_date < $1::date
              AND (
                (
                  snapshot.generated_at < $2
                  AND snapshot.receipt_sealed_at < $2
                  AND snapshot.snapshot_json->'evidence'->>'signal_available_date'
                      =timezone('Asia/Shanghai',snapshot.generated_at)::date::text
                  AND timezone('Asia/Shanghai',snapshot.generated_at)::date <= $1::date
                )
                OR (
                  $3::date IS NOT NULL
                  AND $4::date IS NOT NULL
                  AND snapshot.source_trade_date=$3::date
                  AND snapshot.snapshot_json->'evidence'->>'signal_available_date'
                      =$4::text
                  AND timezone('Asia/Shanghai',snapshot.generated_at)::date
                      =$4::date
                  AND timezone('Asia/Shanghai',snapshot.receipt_sealed_at)::date
                      =$4::date
                )
              )
            ORDER BY snapshot.source_trade_date DESC NULLS LAST,
                     snapshot.receipt_sealed_at DESC NULLS LAST,
                     snapshot.generated_at DESC NULLS LAST,
                     snapshot.snapshot_id DESC
            LIMIT 1
        """
        async with self._repository.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                leg = await connection.fetchrow(
                    f"""
                    SELECT leg.d1
                    FROM {self._schema}.model_legs AS leg
                    JOIN {self._schema}.model_batches AS batch USING (model_batch_id)
                    JOIN {self._schema}.outbox_events AS source
                      ON source.event_id=batch.source_event_id
                    WHERE leg.model_leg_id=$1
                      AND source.seal_status='SEALED'
                      AND {_model_batch_authorization_sql(self._schema)}
                    FOR UPDATE OF leg
                    """,
                    model_leg_id,
                )
                if leg is None:
                    raise V20RepositoryError(f"unknown model leg {model_leg_id!r}")
                if leg["d1"] != d1:
                    raise V20SemanticConflict("MEWS selection d1 does not match model leg")
                existing = await connection.fetchrow(
                    f"""
                    SELECT selection.model_leg_id,selection.cutoff_ts,
                           selection.selection_reason,selection.selected_at,
                           selection.snapshot_id AS selected_snapshot_id,
                           selection.fast_state AS selected_fast_state,
                           snapshot.snapshot_id,snapshot.source_trade_date,
                           snapshot.generated_at,snapshot.received_at,
                           snapshot.receipt_sealed_at,snapshot.fast_state,
                           snapshot.model_version,snapshot.data_version,
                           snapshot.content_hash,snapshot.snapshot_json
                    FROM {self._schema}.leg_mews_selection AS selection
                    LEFT JOIN {self._schema}.mews_snapshots AS snapshot
                      ON snapshot.snapshot_id=selection.snapshot_id
                    WHERE selection.model_leg_id=$1
                    FOR UPDATE OF selection
                    """,
                    model_leg_id,
                )
                if existing is not None:
                    if existing["cutoff_ts"] != cutoff:
                        raise V20SemanticConflict(
                            "MEWS selection was already frozen with a different cutoff"
                        )
                    return self._record_from_selection(
                        existing,
                        d1=d1,
                        cutoff=cutoff,
                        late_source_trade_date=late_source_trade_date,
                    )
                row = await connection.fetchrow(
                    candidate_sql,
                    d1,
                    cutoff,
                    late_source_trade_date,
                    late_availability_date,
                )
                if row is None:
                    snapshot_id = None
                    fast_state = None
                    reason = _FALLBACK_REASON
                else:
                    snapshot_id, fast_state, reason = _snapshot_from_row(
                        row,
                        cutoff=cutoff,
                        source_trade_date=None,
                        availability_date=d1,
                        source_must_precede=d1,
                    )
                inserted = await connection.fetchrow(
                    f"""
                    INSERT INTO {self._schema}.leg_mews_selection
                        (model_leg_id,snapshot_id,fast_state,cutoff_ts,selection_reason)
                    VALUES ($1,$2,$3,$4,$5)
                    RETURNING model_leg_id,cutoff_ts,selection_reason,selected_at,
                              snapshot_id AS selected_snapshot_id,
                              fast_state AS selected_fast_state
                    """,
                    model_leg_id,
                    snapshot_id,
                    fast_state,
                    cutoff,
                    reason,
                )
                snapshot_for_record = row if row is not None else _null_snapshot_row()
                snapshot_values = {
                    field: snapshot_for_record[field]
                    for field in (
                        "snapshot_id",
                        "source_trade_date",
                        "generated_at",
                        "received_at",
                        "receipt_sealed_at",
                        "fast_state",
                        "model_version",
                        "data_version",
                        "content_hash",
                        "snapshot_json",
                    )
                }
                return self._record_from_selection(
                    {**dict(inserted), **snapshot_values},
                    d1=d1,
                    cutoff=cutoff,
                    late_source_trade_date=late_source_trade_date,
                )

    def _record_from_selection(
        self,
        row: Any,
        *,
        d1: date,
        cutoff: datetime,
        late_source_trade_date: date | None,
    ) -> SelectedMewsRecord:
        if not hasattr(row, "__getitem__"):
            raise V20SemanticConflict("MEWS selection row is not indexable")
        selected_at = _aware(row["selected_at"], "selected_at")
        stored_cutoff = _aware(row["cutoff_ts"], "selection cutoff_ts")
        if stored_cutoff != cutoff:
            raise V20SemanticConflict("MEWS selection cutoff is invalid")
        selected_snapshot = row["selected_snapshot_id"]
        selected_fast_state = row["selected_fast_state"]
        reason = row["selection_reason"]
        if selected_snapshot is None:
            if selected_fast_state is not None or reason != _FALLBACK_REASON:
                raise V20SemanticConflict("MEWS fallback selection is inconsistent")
            for field in (
                "snapshot_id",
                "source_trade_date",
                "generated_at",
                "received_at",
                "receipt_sealed_at",
                "fast_state",
                "model_version",
                "data_version",
                "content_hash",
                "snapshot_json",
            ):
                if row[field] is not None:
                    raise V20SemanticConflict("MEWS fallback selection joined a snapshot")
            return SelectedMewsRecord(
                model_leg_id=row["model_leg_id"],
                d1=d1,
                cutoff_ts=stored_cutoff,
                selection_reason=reason,
                selected_at=selected_at,
                snapshot_id=None,
                source_trade_date=None,
                generated_at=None,
                received_at=None,
                fast_state=None,
                model_version=None,
                data_version=None,
                content_hash=None,
                payload=None,
            )

        validated_id, validated_fast_state, validated_reason = _snapshot_from_row(
            row,
            cutoff=cutoff,
            source_trade_date=None,
            availability_date=d1,
            source_must_precede=d1,
        )
        if selected_snapshot != validated_id:
            raise V20SemanticConflict("MEWS selection snapshot_id is inconsistent")
        if selected_fast_state != validated_fast_state:
            raise V20SemanticConflict("MEWS selected fast_state differs from snapshot")
        if selected_at < row["receipt_sealed_at"]:
            raise V20SemanticConflict("MEWS selected_at precedes receipt_sealed_at")
        if validated_reason == "ELIGIBLE_LATE_SAME_DAY" and (
            late_source_trade_date is None or row["source_trade_date"] != late_source_trade_date
        ):
            raise V20SemanticConflict("MEWS late selection lacks the exact late source trade date")
        if reason != validated_reason:
            raise V20SemanticConflict("MEWS selection_reason differs from receipt evidence")
        return SelectedMewsRecord(
            model_leg_id=row["model_leg_id"],
            d1=d1,
            cutoff_ts=stored_cutoff,
            selection_reason=reason,
            selected_at=selected_at,
            snapshot_id=validated_id,
            source_trade_date=row["source_trade_date"],
            generated_at=row["generated_at"],
            received_at=row["received_at"],
            fast_state=validated_fast_state,
            model_version=row["model_version"],
            data_version=row["data_version"],
            content_hash=row["content_hash"],
            payload=_payload_json(row["snapshot_json"]),
        )


__all__ = ["V20MewsGuardStore"]
