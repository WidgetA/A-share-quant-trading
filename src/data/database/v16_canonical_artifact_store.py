from __future__ import annotations

import copy
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from types import MappingProxyType
from typing import Any, cast

from src.data.database.v20_repository import (
    V20SemanticConflict,
    canonical_json,
    sha256_json,
)

SNAPSHOT_TYPE = "V16_CANONICAL_MASTER_V1"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_HASH = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class V16CanonicalArtifactRecord:
    snapshot_id: str
    snapshot_type: str
    trade_date: date
    snapshot_hash: str
    first_received_at: datetime
    _payload: Any = field(repr=False, compare=False)

    @property
    def payload(self) -> Any:
        payload = copy.deepcopy(self._payload)
        if isinstance(payload, Mapping):
            return MappingProxyType(payload)
        return payload


class V16CanonicalArtifactStore:
    def __init__(self, repository: Any) -> None:
        schema = getattr(repository, "schema", None)
        if not isinstance(schema, str) or not _IDENTIFIER.fullmatch(schema):
            raise ValueError(f"invalid PostgreSQL schema identifier: {schema!r}")
        self._repository = repository
        self._schema = schema

    async def save_once(
        self,
        canonical: object,
        *,
        official_stream_id: str,
        trade_date: date,
        event: str = SNAPSHOT_TYPE,
    ) -> V16CanonicalArtifactRecord:
        expected = _identity(
            canonical=canonical,
            event=event,
            official_stream_id=official_stream_id,
            trade_date=trade_date,
        )
        async with self._repository.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    f"""
                    INSERT INTO {self._schema}.input_snapshots
                        (snapshot_id,snapshot_type,trade_date,snapshot_hash,snapshot_json)
                    VALUES ($1,$2,$3,$4,$5::jsonb)
                    ON CONFLICT (snapshot_id) DO NOTHING
                    """,
                    expected.snapshot_id,
                    SNAPSHOT_TYPE,
                    trade_date,
                    expected.snapshot_hash,
                    expected.canonical_json,
                )
                row = await connection.fetchrow(_SELECT_SQL(self._schema), expected.snapshot_id)
                if row is None:
                    raise V20SemanticConflict("canonical artifact readback is missing")
                try:
                    return _record_from_row(
                        row,
                        snapshot_id=expected.snapshot_id,
                        trade_date=trade_date,
                        snapshot_hash=expected.snapshot_hash,
                    )
                except V20SemanticConflict as exc:
                    raise V20SemanticConflict("canonical artifact slot collision") from exc

    async def load(
        self,
        *,
        official_stream_id: str,
        trade_date: date,
        event: str = SNAPSHOT_TYPE,
    ) -> V16CanonicalArtifactRecord | None:
        expected = _identity(
            canonical=None,
            event=event,
            official_stream_id=official_stream_id,
            trade_date=trade_date,
            content_required=False,
        )
        async with self._repository.pool.acquire() as connection:
            row = await connection.fetchrow(_SELECT_SQL(self._schema), expected.snapshot_id)
        if row is None:
            return None
        try:
            return _record_from_row(
                row,
                snapshot_id=expected.snapshot_id,
                trade_date=trade_date,
                snapshot_hash=None,
            )
        except V20SemanticConflict as exc:
            raise V20SemanticConflict("canonical artifact row is invalid") from exc


@dataclass(frozen=True)
class _ExpectedArtifact:
    snapshot_id: str
    snapshot_hash: str | None
    canonical_json: str | None


def _identity(
    *,
    canonical: object,
    event: object,
    official_stream_id: object,
    trade_date: object,
    content_required: bool = True,
) -> _ExpectedArtifact:
    if not isinstance(event, str) or event != SNAPSHOT_TYPE:
        raise ValueError("canonical artifact event contract is invalid")
    if not isinstance(official_stream_id, str):
        raise TypeError("official_stream_id must be a string")
    if (
        not official_stream_id
        or official_stream_id != official_stream_id.strip()
        or any(ord(char) < 32 or ord(char) == 127 for char in official_stream_id)
    ):
        raise ValueError("official_stream_id is invalid")
    if type(trade_date) is not date:
        raise TypeError("trade_date must be a date")

    snapshot_id = sha256_json(
        [
            "V16_CANONICAL_ARTIFACT_SLOT_ID_V1",
            SNAPSHOT_TYPE,
            trade_date.isoformat(),
        ]
    )
    if not content_required:
        return _ExpectedArtifact(snapshot_id, None, None)
    # ``computed_at`` is process timing, not strategy meaning.  Persisting it
    # in the semantic JSON would make an otherwise exact retry collide with
    # the one immutable trade-date slot.  Receipt/actionability is bound by the
    # database-owned ``first_received_at`` column instead.
    if isinstance(canonical, Mapping):
        semantic: object = copy.deepcopy(dict(canonical))
    else:
        semantic = copy.deepcopy(canonical)
    if isinstance(semantic, dict):
        semantic.pop("computed_at", None)
    try:
        encoded = canonical_json(semantic)
        snapshot_hash = sha256_json(semantic)
    except (TypeError, ValueError) as exc:
        raise ValueError("canonical artifact payload is not canonical JSON") from exc
    return _ExpectedArtifact(snapshot_id, snapshot_hash, encoded)


def _SELECT_SQL(schema: str) -> str:
    return f"""
        SELECT snapshot_id,snapshot_type,trade_date,snapshot_hash,snapshot_json,first_received_at
        FROM {schema}.input_snapshots
        WHERE snapshot_id=$1
    """


def _record_from_row(
    row: object,
    *,
    snapshot_id: str,
    trade_date: date,
    snapshot_hash: str | None,
) -> V16CanonicalArtifactRecord:
    row_mapping = cast(Mapping[str, object], row)
    try:
        row_snapshot_id = row_mapping["snapshot_id"]
        row_snapshot_type = row_mapping["snapshot_type"]
        row_trade_date = row_mapping["trade_date"]
        stored_hash = row_mapping["snapshot_hash"]
        received_at = row_mapping["first_received_at"]
        raw_json = row_mapping["snapshot_json"]
    except (TypeError, KeyError, IndexError):
        raise V20SemanticConflict("canonical artifact row shape is invalid")
    if row_snapshot_id != snapshot_id:
        raise V20SemanticConflict("canonical artifact row shape is invalid")
    if row_snapshot_type != SNAPSHOT_TYPE:
        raise V20SemanticConflict("canonical artifact snapshot type is invalid")
    if type(row_trade_date) is not date or row_trade_date != trade_date:
        raise V20SemanticConflict("canonical artifact trade date is invalid")

    if not isinstance(stored_hash, str) or not _HASH.fullmatch(stored_hash):
        raise V20SemanticConflict("canonical artifact hash is invalid")
    if snapshot_hash is not None and stored_hash != snapshot_hash:
        raise V20SemanticConflict("canonical artifact content differs")

    if (
        not isinstance(received_at, datetime)
        or received_at.tzinfo is None
        or received_at.utcoffset() is None
    ):
        raise V20SemanticConflict("canonical artifact receipt timestamp is invalid")

    if isinstance(raw_json, str):
        try:
            payload = json.loads(raw_json)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise V20SemanticConflict("canonical artifact JSON is invalid") from exc
    else:
        payload = raw_json
    if isinstance(payload, Mapping) and "computed_at" in payload:
        raise V20SemanticConflict("canonical artifact JSON contains volatile timing")
    try:
        actual_hash = sha256_json(payload)
    except (TypeError, ValueError) as exc:
        raise V20SemanticConflict("canonical artifact JSON is invalid") from exc
    if actual_hash != stored_hash:
        raise V20SemanticConflict("canonical artifact JSON hash differs")
    return V16CanonicalArtifactRecord(
        snapshot_id=snapshot_id,
        snapshot_type=SNAPSHOT_TYPE,
        trade_date=trade_date,
        snapshot_hash=stored_hash,
        first_received_at=received_at,
        _payload=copy.deepcopy(payload),
    )
