"""Transactional PostgreSQL repository for V20 decisions and Feishu outbox.

This repository intentionally contains no order, account, holding, or fill
tables.  It persists model decisions and notifications only.  An outbox row is
created in the same transaction as the decision/exit intent and becomes
publishable only after a second transaction seals it with a post-commit clock
receipt.  The receipt is conservative: it cannot be earlier than visibility of
the originating transaction, so a boundary race expires rather than leaks a
late buy suggestion.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import asyncpg

from src.data.clients.tushare_realtime import BEIJING_TZ
from src.data.database.tls import verified_postgres_ssl_context
from src.strategy.v20.models import (
    V20_DATA_ALERT_SEMANTIC_SCHEMA,
    V20_ENTRY_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
)
from src.strategy.v20.rolling7_market_health import (
    BatchStatus,
    CanonicalRecommendation,
    Rolling7Batch,
    Rolling7Leg,
    SignalKind,
)

logger = logging.getLogger(__name__)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ENV_VALUE = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}$")
_ENTRY_NORMAL_DEADLINE_WALL = time(9, 40)
_ENTRY_FINALIZATION_WALL = time(9, 45)
_MANUAL_MONITOR_ENROLLMENT_DEADLINE_WALL = time(9, 30)
_BOOTSTRAP_CHECKPOINT_SCHEMA = "v20-bootstrap-checkpoint/v3"
_BOOTSTRAP_BATCH_ID_PROFILE = "V20_BOOTSTRAP_TARGET_BATCH_ID_V1"
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_OUTBOX_002_CHECKSUM_DECLARATION = re.compile(r"migration_checksum text := '[^']*';")


def _outbox_002_contract_checksum(standalone: str) -> str:
    canonical_contract = _OUTBOX_002_CHECKSUM_DECLARATION.sub(
        "migration_checksum text := '__V20_002_CONTRACT_SHA256__';",
        standalone,
        count=1,
    )
    return hashlib.sha256(canonical_contract.encode("utf-8")).hexdigest()


def _render_outbox_at_most_once_migration(schema: str) -> str:
    standalone = (_PROJECT_ROOT / "migrations" / "v20" / "002_outbox_at_most_once.sql").read_text(
        encoding="utf-8"
    )
    declaration = _OUTBOX_002_CHECKSUM_DECLARATION.search(standalone)
    if declaration is None:
        raise RuntimeError("V20 002 migration checksum declaration is missing")
    declared_checksum = declaration.group(0)
    contract_checksum = _outbox_002_contract_checksum(standalone)
    canonical_declaration = f"migration_checksum text := '{contract_checksum}';"
    if declared_checksum != canonical_declaration:
        raise RuntimeError("V20 002 migration checksum does not match its canonical contract")
    if schema == "v20":
        return standalone
    return standalone.replace("v20.", f"{schema}.").replace(
        "table_schema='v20'", f"table_schema='{schema}'"
    )


def _render_rolling7_market_health_migration(schema: str) -> str:
    migration = _PROJECT_ROOT / "migrations" / "v20" / "003_rolling7_market_health.sql"
    standalone = migration.read_text(encoding="utf-8")
    return standalone.replace("v20.", f"{schema}.")


class V20RepositoryError(RuntimeError):
    pass


class V20StateConflict(V20RepositoryError):
    pass


class V20LeadershipLost(V20StateConflict):
    """The session that owns the stream/lineage advisory lock was lost."""


class V20EntryDeadlineExceeded(V20StateConflict):
    """The database clock rejected a normal decision at the 09:40 boundary."""


class V20SemanticConflict(V20RepositoryError):
    pass


def canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_json(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _optional_json_value(value: Any) -> Any | None:
    if value is None:
        return None
    return _json_value(value)


def _optional_finite_float(value: Any, field_name: str) -> float | None:
    """Decode a nullable numeric database field without a zero fallback."""

    if value is None:
        return None
    if isinstance(value, bool):
        raise V20SemanticConflict(f"{field_name} must be numeric or null")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise V20SemanticConflict(f"{field_name} must be numeric or null") from exc
    if not math.isfinite(parsed):
        raise V20SemanticConflict(f"{field_name} must be finite")
    return parsed


def _require_sha256(value: str, field_name: str) -> None:
    if not _SHA256.fullmatch(value):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 hex digest")


def _require_aware(value: datetime | None, field_name: str) -> None:
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError(f"{field_name} must be timezone-aware")


def _is_legal_complete_minute_payload(
    payload: Mapping[str, Any],
    *,
    expected_code: str,
    expected_bar_end: datetime,
    expected_label: str,
) -> bool:
    """Apply the frozen exit-bar validity rule before revision arbitration."""

    try:
        raw_end = payload.get("bar_end")
        parsed_end = (
            raw_end if isinstance(raw_end, datetime) else datetime.fromisoformat(str(raw_end))
        )
        if parsed_end.tzinfo is None or parsed_end.utcoffset() is None:
            return False
        local_end = parsed_end.astimezone(BEIJING_TZ)
        expected_local = expected_bar_end.astimezone(BEIJING_TZ)
        if (
            payload.get("stock_code") != expected_code
            or payload.get("end_label") != expected_label
            or local_end != expected_local
            or expected_label != local_end.strftime("%H:%M")
            or local_end.second != 0
            or local_end.microsecond != 0
            or payload.get("source_confirms_complete") is not True
        ):
            return False
        raw_values = tuple(
            payload.get(field) for field in ("open", "high", "low", "close", "volume", "amount")
        )
        if any(value is None or isinstance(value, bool) for value in raw_values):
            return False
        parsed_values: list[float] = []
        for value in raw_values:
            assert value is not None
            parsed_values.append(float(value))
        open_price, high_price, low_price, close_price, volume, amount = parsed_values
    except (TypeError, ValueError, OverflowError):
        return False
    if not all(
        math.isfinite(value) and value > 0
        for value in (open_price, high_price, low_price, close_price, volume, amount)
    ):
        return False
    return (
        low_price <= min(open_price, close_price)
        and high_price >= max(open_price, close_price)
        and low_price <= high_price
    )


def _require_scope(official_stream_id: str, lineage_id: str) -> None:
    """Reject unscoped operational reads before they can reach PostgreSQL.

    V20 keeps forward-shadow and production decisions in the same normalized
    ledger. Entity identifiers are globally unique, but that alone is not a
    safe routing boundary: a caller must explicitly bind every operational
    history/leg/reminder query to both the official stream and its state
    lineage.
    """
    if not official_stream_id:
        raise ValueError("official_stream_id cannot be empty")
    if not lineage_id:
        raise ValueError("lineage_id cannot be empty")


def _require_outbox_scope(route_id: str, official_stream_id: str, lineage_id: str) -> None:
    """Require the complete transport and ledger boundary for outbox work."""

    if not route_id:
        raise ValueError("route_id cannot be empty")
    _require_scope(official_stream_id, lineage_id)


_BOOTSTRAP_SHADOW_KEYS = {
    "batch_id",
    "source_batch_id",
    "kind",
    "signal_date",
    "t2_date",
    "status",
    "payload",
    "batch_return",
    "reference_status",
    "reference_prices",
    "reference_snapshot_hash",
}


def _normalize_bootstrap_shadow(raw: Mapping[str, Any]) -> dict[str, Any]:
    if set(raw) != _BOOTSTRAP_SHADOW_KEYS:
        raise ValueError("bootstrap shadow batch field set mismatch")
    if not isinstance(raw["batch_id"], str) or not isinstance(raw["source_batch_id"], str):
        raise ValueError("bootstrap shadow batch IDs must be strings")
    batch_id_value = raw["batch_id"]
    source_batch_id = raw["source_batch_id"]
    if not batch_id_value or not source_batch_id:
        raise ValueError("bootstrap shadow batch IDs cannot be empty")
    if not isinstance(raw["kind"], str):
        raise ValueError("bootstrap shadow kind must be a string")
    kind = raw["kind"]
    if kind != "HEALTH":
        raise ValueError("invalid bootstrap shadow kind")
    if not isinstance(raw["signal_date"], str) or not isinstance(raw["t2_date"], str):
        raise ValueError("bootstrap shadow dates must be ISO strings")
    try:
        signal_date = date.fromisoformat(raw["signal_date"])
        t2_date = date.fromisoformat(raw["t2_date"])
    except ValueError as exc:
        raise ValueError("invalid bootstrap shadow date") from exc
    if t2_date <= signal_date:
        raise ValueError("bootstrap shadow dates must satisfy signal_date < t2_date")
    if not isinstance(raw["status"], str):
        raise ValueError("bootstrap shadow status must be a string")
    status = raw["status"]
    if status not in {"PENDING", "COMPLETE_VALID", "COMPLETE_INVALID"}:
        raise ValueError("invalid bootstrap shadow status")
    batch_return = raw["batch_return"]
    if status == "COMPLETE_VALID":
        if isinstance(batch_return, bool):
            raise ValueError("valid bootstrap shadow return must be numeric")
        batch_return = float(batch_return)
        if not math.isfinite(batch_return):
            raise ValueError("valid bootstrap shadow return must be finite")
    elif batch_return is not None:
        raise ValueError("non-valid bootstrap shadow cannot carry a batch return")
    payload = raw["payload"]
    if not isinstance(payload, Mapping):
        raise ValueError("bootstrap shadow payload must be an object")
    payload = dict(payload)
    canonical_json(payload)
    if not isinstance(raw["reference_status"], str):
        raise ValueError("bootstrap shadow reference status must be a string")
    reference_status = raw["reference_status"]
    if reference_status not in {"PENDING", "LOCKED", "UNAVAILABLE"}:
        raise ValueError("invalid bootstrap shadow reference status")
    reference_prices_raw = raw["reference_prices"]
    reference_hash = raw["reference_snapshot_hash"]
    reference_prices: dict[str, float] | None = None
    if reference_status == "LOCKED":
        if not isinstance(reference_prices_raw, Mapping) or not reference_prices_raw:
            raise ValueError("locked bootstrap shadow requires reference prices")
        reference_prices = {}
        for code, raw_price in reference_prices_raw.items():
            if not isinstance(code, str) or isinstance(raw_price, bool):
                raise ValueError("invalid bootstrap shadow reference price")
            code_text = code
            price = float(raw_price)
            if not re.fullmatch(r"\d{6}", code_text) or not math.isfinite(price) or price <= 0:
                raise ValueError("invalid bootstrap shadow reference price")
            reference_prices[code_text] = price
        if not isinstance(reference_hash, str):
            raise ValueError("locked bootstrap shadow requires reference hash")
        _require_sha256(reference_hash, "reference_snapshot_hash")
    elif reference_status == "UNAVAILABLE":
        if reference_prices_raw is not None or not isinstance(reference_hash, str):
            raise ValueError("unavailable bootstrap shadow reference fields are invalid")
        _require_sha256(reference_hash, "reference_snapshot_hash")
    elif reference_prices_raw is not None or reference_hash is not None:
        raise ValueError("pending bootstrap shadow cannot carry reference evidence")
    if status == "COMPLETE_VALID" and reference_status != "LOCKED":
        raise ValueError("valid bootstrap shadow requires locked reference evidence")
    if status != "PENDING" and reference_status == "PENDING":
        raise ValueError("terminal bootstrap shadow cannot have pending reference evidence")
    return {
        "batch_id": batch_id_value,
        "source_batch_id": source_batch_id,
        "kind": kind,
        "signal_date": signal_date,
        "t2_date": t2_date,
        "status": status,
        "payload": payload,
        "batch_return": batch_return,
        "reference_status": reference_status,
        "reference_prices": reference_prices,
        "reference_snapshot_hash": reference_hash,
    }


def _normalize_bootstrap_shadows(
    rows: Sequence[Mapping[str, Any]],
    *,
    checkpoint_import: bool,
) -> tuple[dict[str, Any], ...]:
    if not checkpoint_import:
        if rows:
            raise ValueError("empty forward-shadow genesis cannot import shadow batches")
        return ()
    # Older checkpoint schemas carried ROLLING7 shadow rows.  Rolling7 now
    # restores exclusively from its lineage-independent fact table, so those
    # legacy rows are accepted as inert input and deliberately not imported.
    normalized = tuple(
        _normalize_bootstrap_shadow(row) for row in rows if row.get("kind") != "ROLLING7"
    )
    batch_ids = [str(row["batch_id"]) for row in normalized]
    source_ids = [str(row["source_batch_id"]) for row in normalized]
    if len(batch_ids) != len(set(batch_ids)):
        raise ValueError("bootstrap shadow batch_id values must be unique")
    if len(source_ids) != len(set(source_ids)):
        raise ValueError("bootstrap shadow source_batch_id values must be unique")
    return normalized


def _bootstrap_target_batch_id(
    *,
    source_official_stream_id: str,
    source_lineage_id: str,
    source_batch_id: str,
    target_official_stream_id: str,
    target_lineage_id: str,
) -> str:
    return "bootstrap-batch:" + sha256_json(
        {
            "profile": _BOOTSTRAP_BATCH_ID_PROFILE,
            "source_official_stream_id": source_official_stream_id,
            "source_lineage_id": source_lineage_id,
            "source_batch_id": source_batch_id,
            "target_official_stream_id": target_official_stream_id,
            "target_lineage_id": target_lineage_id,
        }
    )


def _bootstrap_shadow_matches(
    row: Mapping[str, Any],
    expected: Mapping[str, Any],
    *,
    official_stream_id: str,
    lineage_id: str,
) -> bool:
    stored_prices = _optional_json_value(row["reference_prices_json"])
    return (
        row["batch_id"] == expected["batch_id"]
        and row["decision_id"] is None
        and row["official_stream_id"] == official_stream_id
        and row["lineage_id"] == lineage_id
        and row["source_batch_id"] == expected["source_batch_id"]
        and row["kind"] == expected["kind"]
        and row["signal_date"] == expected["signal_date"]
        and row["t2_date"] == expected["t2_date"]
        and row["status"] == expected["status"]
        and _json_value(row["batch_json"]) == expected["payload"]
        and _optional_finite_float(row["batch_return"], "bootstrap batch_return")
        == expected["batch_return"]
        and row["reference_status"] == expected["reference_status"]
        and stored_prices == expected["reference_prices"]
        and row["reference_snapshot_hash"] == expected["reference_snapshot_hash"]
    )


def _bootstrap_shadow_is_legal_successor(
    row: Mapping[str, Any],
    expected: Mapping[str, Any],
    *,
    official_stream_id: str,
    lineage_id: str,
) -> bool:
    """Allow only monotonic runtime evolution of an imported shadow identity."""

    immutable_matches = (
        row["batch_id"] == expected["batch_id"]
        and row["decision_id"] is None
        and row["official_stream_id"] == official_stream_id
        and row["lineage_id"] == lineage_id
        and row["source_batch_id"] == expected["source_batch_id"]
        and row["kind"] == expected["kind"]
        and row["signal_date"] == expected["signal_date"]
        and row["t2_date"] == expected["t2_date"]
    )
    if not immutable_matches:
        return False
    stored_payload = _json_value(row["batch_json"])
    if not isinstance(stored_payload, Mapping) or any(
        stored_payload.get(key) != value for key, value in expected["payload"].items()
    ):
        return False
    if expected["status"] != "PENDING" and (
        row["status"] != expected["status"]
        or _optional_finite_float(row["batch_return"], "bootstrap batch_return")
        != expected["batch_return"]
    ):
        return False
    if expected["reference_status"] != "PENDING":
        return (
            row["reference_status"] == expected["reference_status"]
            and _optional_json_value(row["reference_prices_json"]) == expected["reference_prices"]
            and row["reference_snapshot_hash"] == expected["reference_snapshot_hash"]
        )
    return row["reference_status"] in {"PENDING", "LOCKED", "UNAVAILABLE"}


def _validate_official_state_contract(
    state: Mapping[str, Any],
    *,
    revision: int,
) -> None:
    expected_state_keys = {
        "schema_version",
        "state_revision",
        "health",
        "official_rolling_gaps",
        "last_terminal_slot_id",
        "last_terminal_trade_date",
    }
    if set(state) != expected_state_keys or state.get("schema_version") != "v20-official-state/v1":
        raise V20SemanticConflict("official state schema/field set mismatch")
    state_revision = state.get("state_revision")
    if (
        isinstance(state_revision, bool)
        or not isinstance(state_revision, int)
        or state_revision != revision
    ):
        raise V20SemanticConflict("official state revision mismatch")


def _validate_checkpoint_state_facts(
    state: Mapping[str, Any],
    shadows: Sequence[Mapping[str, Any]],
) -> None:
    expected_state_keys = {
        "schema_version",
        "state_revision",
        "health",
        "official_rolling_gaps",
        "last_terminal_slot_id",
        "last_terminal_trade_date",
    }
    if set(state) != expected_state_keys or state.get("schema_version") != "v20-official-state/v1":
        raise ValueError("checkpoint official state schema/field set mismatch")
    if state.get("state_revision") != 0:
        raise ValueError("checkpoint official state must start at revision zero")
    if (
        state.get("last_terminal_slot_id") is not None
        or state.get("last_terminal_trade_date") is not None
    ):
        raise ValueError("checkpoint target state cannot contain a source predecessor")

    health = state.get("health")
    if not isinstance(health, Mapping):
        raise ValueError("checkpoint health state must be an object")
    expected_health_keys = {
        "schema_version",
        "status",
        "recovery_count",
        "recent_valid",
        "last_processed_key",
    }
    if (
        set(health) != expected_health_keys
        or health.get("schema_version") != "v20-health-snapshot/v1"
    ):
        raise ValueError("checkpoint health state schema/field set mismatch")
    if health.get("status") not in {"WARMUP", "HEALTHY", "PAUSED_R0", "PAUSED_R1", "PAUSED_R2"}:
        raise ValueError("checkpoint health status is invalid")
    recovery_count = health.get("recovery_count")
    if isinstance(recovery_count, bool) or not isinstance(recovery_count, int):
        raise ValueError("checkpoint health recovery_count is invalid")
    recent_valid = health.get("recent_valid")
    if not isinstance(recent_valid, list) or len(recent_valid) > 3:
        raise ValueError("checkpoint health recent_valid is invalid")
    status = str(health["status"])
    valid_recovery_counts = {
        "WARMUP": {0},
        "HEALTHY": {0, 3},
        "PAUSED_R0": {0},
        "PAUSED_R1": {1},
        "PAUSED_R2": {2},
    }
    if recovery_count not in valid_recovery_counts[status]:
        raise ValueError("checkpoint health recovery_count is inconsistent with status")
    if (status == "WARMUP" and len(recent_valid) >= 3) or (
        status != "WARMUP" and len(recent_valid) != 3
    ):
        raise ValueError("checkpoint health window length is inconsistent with status")

    by_id = {str(row["batch_id"]): row for row in shadows}
    previous_order: tuple[date, date, str] | None = None
    recent_ids: set[str] = set()
    observation_keys = {"batch_id", "signal_date", "t2_exit_date", "relative_return"}
    for observation in recent_valid:
        if not isinstance(observation, Mapping) or set(observation) != observation_keys:
            raise ValueError("checkpoint health observation field set mismatch")
        if (
            not isinstance(observation["batch_id"], str)
            or not isinstance(observation["signal_date"], str)
            or not isinstance(observation["t2_exit_date"], str)
            or isinstance(observation["relative_return"], bool)
        ):
            raise ValueError("checkpoint health observation types are invalid")
        batch_id_value = observation["batch_id"]
        if not batch_id_value or batch_id_value in recent_ids:
            raise ValueError("checkpoint health observation IDs must be non-empty and unique")
        recent_ids.add(batch_id_value)
        try:
            signal_date = date.fromisoformat(observation["signal_date"])
            t2_date = date.fromisoformat(observation["t2_exit_date"])
            relative_return = float(observation["relative_return"])
        except (TypeError, ValueError) as exc:
            raise ValueError("checkpoint health observation is malformed") from exc
        if not math.isfinite(relative_return) or t2_date <= signal_date:
            raise ValueError("checkpoint health observation values are invalid")
        order = (t2_date, signal_date, batch_id_value)
        if previous_order is not None and order <= previous_order:
            raise ValueError("checkpoint health observations are not strictly ordered")
        previous_order = order
        fact = by_id.get(batch_id_value)
        if fact is None or not (
            fact["kind"] == "HEALTH"
            and fact["status"] == "COMPLETE_VALID"
            and fact["signal_date"] == signal_date
            and fact["t2_date"] == t2_date
            and fact["batch_return"] == relative_return
        ):
            raise ValueError("checkpoint health observation lacks its exact shadow fact")

    watermark = health.get("last_processed_key")
    if watermark is not None:
        if (
            not isinstance(watermark, list)
            or len(watermark) != 3
            or not all(isinstance(value, str) for value in watermark)
        ):
            raise ValueError("checkpoint health watermark is malformed")
        try:
            watermark_t2 = date.fromisoformat(watermark[0])
            watermark_signal = date.fromisoformat(watermark[1])
        except ValueError as exc:
            raise ValueError("checkpoint health watermark dates are malformed") from exc
        watermark_id = watermark[2]
        fact = by_id.get(watermark_id)
        if (
            not watermark_id
            or fact is None
            or not (
                fact["kind"] == "HEALTH"
                and fact["status"] != "PENDING"
                and fact["signal_date"] == watermark_signal
                and fact["t2_date"] == watermark_t2
            )
        ):
            raise ValueError("checkpoint health watermark lacks its exact terminal shadow fact")
        if previous_order is not None and previous_order > (
            watermark_t2,
            watermark_signal,
            watermark_id,
        ):
            raise ValueError("checkpoint health observation exceeds its watermark")

    # ``official_rolling_gaps`` remains in v1 state JSON only as a retired
    # schema field.  New checkpoints normalize it to empty and never bind it
    # to lineage shadow facts.
    if state.get("official_rolling_gaps") != []:
        raise ValueError("retired checkpoint rolling gaps must be empty")


def _model_batch_semantics(batch: ModelBatchWrite | None) -> object:
    if batch is None:
        return None
    return {
        "model_batch_id": batch.model_batch_id,
        "multiplier": batch.multiplier,
        "evaluation_only": batch.evaluation_only,
        "reference_profile_id": batch.reference_profile_id,
        "legs": [
            {
                "model_leg_id": leg.model_leg_id,
                "code": leg.code,
                "stock_name": leg.stock_name,
                "rank": leg.rank,
                "relative_weight": leg.relative_weight,
                "d1": leg.d1.isoformat(),
                "d2": leg.d2.isoformat(),
            }
            for leg in batch.legs
        ],
    }


def _entry_commit_fingerprint(commit: EntryCommit) -> str:
    """Hash every immutable caller-supplied field used by ``commit_entry``.

    The semantic hash alone is deliberately insufficient for idempotency: a
    buggy retry could otherwise reuse a decision ID while changing the bound
    slot, snapshot, or state transition and be accepted as a harmless retry.
    """
    return sha256_json(
        {
            "official_stream_id": commit.official_stream_id,
            "slot_id": commit.slot_id,
            "trade_date": commit.trade_date.isoformat(),
            "strategy_version": commit.strategy_version,
            "config_id": commit.config_id,
            "config_hash": commit.config_hash,
            "lineage_id": commit.lineage_id,
            "expected_state_revision": commit.expected_state_revision,
            "expected_state_hash": commit.expected_state_hash,
            "next_state_hash": commit.next_state_hash,
            "snapshot_id": commit.snapshot_id,
            "snapshot_hash": commit.snapshot_hash,
            "decision_id": commit.decision_id,
            "event_id": commit.event_id,
            "action": commit.action,
            "final_multiplier": commit.final_multiplier,
            "semantic_content_hash": commit.semantic_content_hash,
            "action_expiry_ts": (
                commit.action_expiry_ts.isoformat() if commit.action_expiry_ts else None
            ),
            "invalid_commit_not_before_ts": (
                commit.invalid_commit_not_before_ts.isoformat()
                if commit.invalid_commit_not_before_ts
                else None
            ),
            "route_id": commit.route_id,
            "shadow_batches": [
                {
                    "batch_id": batch.batch_id,
                    "kind": batch.kind,
                    "signal_date": batch.signal_date.isoformat(),
                    "t2_date": batch.t2_date.isoformat(),
                    "payload_hash": sha256_json(batch.payload),
                }
                for batch in commit.shadow_batches
            ],
            "model_batch": _model_batch_semantics(commit.model_batch),
        }
    )


def _exit_commit_fingerprint(commit: ExitCommit) -> str:
    return sha256_json(
        {
            "exit_intent_id": commit.exit_intent_id,
            "event_id": commit.event_id,
            "model_leg_id": commit.model_leg_id,
            "signal_type": commit.signal_type,
            "trigger_ts": commit.trigger_ts.isoformat(),
            "rule_actionable_from": commit.rule_actionable_from.isoformat(),
            "semantic_content_hash": commit.semantic_content_hash,
            "route_id": commit.route_id,
        }
    )


@dataclass(frozen=True)
class V20DatabaseConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "messages"
    user: str = "v20_writer"
    password: str = ""
    schema: str = "v20"
    pool_min_size: int = 1
    pool_max_size: int = 8
    ssl_mode: str = "verify-full"
    ssl_root_cert: str = ""
    ssl_root_cert_sha256: str = ""
    connect_timeout_seconds: float = 5.0
    command_timeout_seconds: float = 15.0
    connection_profile: Literal["dedicated", "legacy_embedded"] = "dedicated"

    def __post_init__(self) -> None:
        if not _IDENTIFIER.fullmatch(self.schema):
            raise ValueError(f"invalid PostgreSQL schema identifier: {self.schema!r}")
        if not self.host or not self.database or not self.user:
            raise ValueError("database host, database, and user cannot be empty")
        if not 1 <= self.port <= 65535:
            raise ValueError("database port must be between 1 and 65535")
        if (
            self.pool_min_size < 1
            or self.pool_max_size < self.pool_min_size
            or self.pool_max_size < 7
        ):
            raise ValueError("database pool sizes must satisfy 1 <= min <= max and max >= 7")
        if self.connection_profile == "dedicated":
            if self.ssl_mode != "verify-full":
                raise ValueError("V20 database SSL mode must be verify-full")
        elif self.connection_profile == "legacy_embedded":
            if self.ssl_mode not in {"disable", "require", "verify-ca", "verify-full"}:
                raise ValueError("embedded V20 database SSL mode is unsupported")
        else:
            raise ValueError("unsupported V20 database connection profile")
        if not 0 < self.connect_timeout_seconds <= 60:
            raise ValueError("V20 database connect timeout must be in (0, 60]")
        if not 0 < self.command_timeout_seconds <= 60:
            raise ValueError("V20 database command timeout must be in (0, 60]")


@dataclass(frozen=True)
class StateRecord:
    lineage_id: str
    revision: int
    state_hash: str
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class EntryStatus:
    official_stream_id: str
    trade_date: date
    slot_id: str
    slot_status: str
    slot_revision: int
    strategy_version: str
    config_id: str
    config_hash: str
    lineage_id: str
    decision_id: str
    event_id: str
    action: str
    final_multiplier: float
    semantic_content_hash: str
    semantic: Mapping[str, Any]
    snapshot_id: str
    snapshot_hash: str
    snapshot: Mapping[str, Any]
    action_expiry_ts: datetime | None


@dataclass(frozen=True)
class ShadowBatchWrite:
    batch_id: str
    kind: str
    signal_date: date
    t2_date: date
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class ShadowBatchRecord:
    batch_id: str
    decision_id: str | None
    kind: str
    signal_date: date
    t2_date: date
    status: str
    payload: Mapping[str, Any]
    batch_return: float | None
    reference_status: str
    reference_prices: Mapping[str, float] | None
    reference_snapshot_hash: str | None


@dataclass(frozen=True)
class PendingReferenceLeg:
    model_leg_id: str
    model_batch_id: str
    signal_date: date
    code: str
    reference_profile_id: str


@dataclass(frozen=True)
class MinuteBarRecord:
    code: str
    bar_end: datetime
    end_label: str
    source_hash: str
    payload: Mapping[str, Any]
    first_received_at: datetime


class V20MinuteBarIntegrityConflict(V20SemanticConflict):
    """One or more raw labels are corrupt while independent labels remain usable."""

    def __init__(
        self,
        message: str,
        *,
        partial_records: Sequence[MinuteBarRecord],
        corrupt_labels: Sequence[tuple[str, date, str]],
    ) -> None:
        super().__init__(message)
        self.partial_records = tuple(partial_records)
        self.corrupt_labels = tuple(corrupt_labels)


@dataclass(frozen=True)
class DailyBarSnapshotRecord:
    snapshot_id: str
    trade_date: date
    source_hash: str
    payload: Mapping[str, Any]
    first_received_at: datetime
    receipt_sequence: int


@dataclass(frozen=True)
class SelectedMewsRecord:
    model_leg_id: str
    d1: date
    cutoff_ts: datetime
    selection_reason: str
    selected_at: datetime
    snapshot_id: str | None
    source_trade_date: date | None
    generated_at: datetime | None
    received_at: datetime | None
    fast_state: str | None
    model_version: str | None
    data_version: str | None
    content_hash: str | None
    payload: Mapping[str, Any] | None


@dataclass(frozen=True)
class ModelLegWrite:
    model_leg_id: str
    code: str
    stock_name: str
    rank: int
    relative_weight: float
    d1: date
    d2: date


@dataclass(frozen=True)
class ModelBatchWrite:
    model_batch_id: str
    multiplier: float
    evaluation_only: bool
    reference_profile_id: str
    legs: Sequence[ModelLegWrite]


@dataclass(frozen=True)
class ManualMonitorEnrollmentCommit:
    enrollment_id: str
    source_event_id: str
    official_entry_event_id: str
    request_id: str
    route_id: str
    official_stream_id: str
    lineage_id: str
    strategy_version: str
    source_config_hash: str
    state_semantics_hash: str
    signal_date: date
    d1: date
    d2: date
    activation_cutoff_ts: datetime
    source_semantic_content_hash: str
    source_payload_hash: str
    calendar_evidence_hash: str
    enrollment_semantic: Mapping[str, Any]
    enrollment_semantic_hash: str
    model_batch: ModelBatchWrite


@dataclass(frozen=True)
class ManualMonitorEnrollmentRecord:
    enrollment_id: str
    source_event_id: str
    official_entry_event_id: str
    model_batch_id: str
    request_id: str
    signal_date: date
    d1: date
    d2: date
    activation_cutoff_ts: datetime
    source_semantic_content_hash: str
    source_payload_hash: str
    calendar_evidence_hash: str
    semantic: Mapping[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class EntryCommit:
    official_stream_id: str
    slot_id: str
    trade_date: date
    strategy_version: str
    config_id: str
    config_hash: str
    lineage_id: str
    expected_state_revision: int
    expected_state_hash: str
    next_state: Mapping[str, Any]
    next_state_hash: str
    snapshot_id: str
    snapshot_hash: str
    snapshot: Mapping[str, Any]
    decision_id: str
    event_id: str
    action: str
    final_multiplier: float
    semantic: Mapping[str, Any]
    semantic_content_hash: str
    action_expiry_ts: datetime | None
    route_id: str
    shadow_batches: Sequence[ShadowBatchWrite] = ()
    model_batch: ModelBatchWrite | None = None
    invalid_commit_not_before_ts: datetime | None = None


@dataclass(frozen=True)
class ExitCommit:
    exit_intent_id: str
    event_id: str
    model_leg_id: str
    signal_type: str
    trigger_ts: datetime
    rule_actionable_from: datetime
    semantic: Mapping[str, Any]
    semantic_content_hash: str
    route_id: str
    official_stream_id: str
    lineage_id: str


@dataclass(frozen=True)
class OutboxRecord:
    event_id: str
    event_type: str
    route_id: str
    official_stream_id: str
    lineage_id: str
    semantic: Mapping[str, Any]
    semantic_content_hash: str
    payload: Mapping[str, Any] | None
    payload_hash: str | None
    generated_at: datetime | None
    commit_marker: int | None
    action_expiry_ts: datetime | None
    delivery_status: str
    attempt_count: int
    lease_db_ts: datetime | None = None


@dataclass(frozen=True)
class DeliveryAttempt:
    attempt_number: int
    delivery_variant: str


@dataclass(frozen=True)
class ActiveModelLeg:
    model_leg_id: str
    model_batch_id: str
    decision_id: str | None
    signal_date: date
    code: str
    stock_name: str
    rank: int
    relative_weight: float
    d1: date
    d2: date
    reference_status: str
    reference_price: float | None
    reference_snapshot_hash: str | None
    evaluation_only: bool
    mews_snapshot_id: str | None
    mews_fast_state: str | None
    exit_intent_id: str | None
    origin_kind: str = "OFFICIAL_ENTRY"
    source_event_id: str = ""


def _active_model_leg_from_row(row: Mapping[str, Any]) -> ActiveModelLeg:
    return ActiveModelLeg(
        model_leg_id=row["model_leg_id"],
        model_batch_id=row["model_batch_id"],
        decision_id=row["decision_id"],
        origin_kind=row["origin_kind"],
        source_event_id=row["source_event_id"],
        signal_date=row["signal_date"],
        code=row["code"],
        stock_name=row["stock_name"],
        rank=int(row["rank"]),
        relative_weight=float(row["relative_weight"]),
        d1=row["d1"],
        d2=row["d2"],
        reference_status=row["reference_status"],
        reference_price=(
            float(row["reference_price"]) if row["reference_price"] is not None else None
        ),
        reference_snapshot_hash=row["reference_snapshot_hash"],
        evaluation_only=bool(row["evaluation_only"]),
        mews_snapshot_id=row["mews_snapshot_id"],
        mews_fast_state=row["mews_fast_state"],
        exit_intent_id=row["exit_intent_id"],
    )


def _model_batch_authorization_sql(
    schema: str,
    *,
    batch_alias: str = "batch",
    source_alias: str = "source",
) -> str:
    """Return the single dual-origin authorization predicate for model-leg reads."""

    return f"""
        (
            ({batch_alias}.origin_kind='OFFICIAL_ENTRY'
                AND {source_alias}.event_type='ENTRY_DECISION'
                AND EXISTS (
                    SELECT 1
                    FROM {schema}.entry_decisions AS origin_decision
                    JOIN {schema}.decision_slots AS origin_slot
                      ON origin_slot.slot_id=origin_decision.slot_id
                    WHERE origin_decision.decision_id={batch_alias}.decision_id
                      AND origin_decision.event_id={batch_alias}.source_event_id
                      AND origin_slot.official_stream_id={batch_alias}.official_stream_id
                      AND origin_slot.lineage_id={batch_alias}.lineage_id
                ))
            OR ({batch_alias}.origin_kind='MANUAL_MONITOR'
                AND {source_alias}.event_type='DATA_ALERT'
                AND EXISTS (
                    SELECT 1
                    FROM {schema}.manual_monitor_enrollments AS enrollment
                    WHERE enrollment.model_batch_id={batch_alias}.model_batch_id
                      AND enrollment.source_event_id={batch_alias}.source_event_id
                      AND enrollment.official_stream_id={batch_alias}.official_stream_id
                      AND enrollment.lineage_id={batch_alias}.lineage_id
                ))
        )
    """


def _manual_monitor_enrollment_fingerprint(
    commit: ManualMonitorEnrollmentCommit,
) -> str:
    return sha256_json(
        {
            "enrollment_id": commit.enrollment_id,
            "source_event_id": commit.source_event_id,
            "official_entry_event_id": commit.official_entry_event_id,
            "route_id": commit.route_id,
            "official_stream_id": commit.official_stream_id,
            "lineage_id": commit.lineage_id,
            "strategy_version": commit.strategy_version,
            "source_config_hash": commit.source_config_hash,
            "state_semantics_hash": commit.state_semantics_hash,
            "signal_date": commit.signal_date.isoformat(),
            "d1": commit.d1.isoformat(),
            "d2": commit.d2.isoformat(),
            "activation_cutoff_ts": commit.activation_cutoff_ts.isoformat(),
            "source_semantic_content_hash": commit.source_semantic_content_hash,
            "source_payload_hash": commit.source_payload_hash,
            "calendar_evidence_hash": commit.calendar_evidence_hash,
            "enrollment_semantic_hash": commit.enrollment_semantic_hash,
            "model_batch": _model_batch_semantics(commit.model_batch),
        }
    )


def _manual_enrollment_from_row(row: Mapping[str, Any]) -> ManualMonitorEnrollmentRecord:
    return ManualMonitorEnrollmentRecord(
        enrollment_id=str(row["enrollment_id"]),
        source_event_id=str(row["source_event_id"]),
        official_entry_event_id=str(row["official_entry_event_id"]),
        model_batch_id=str(row["model_batch_id"]),
        request_id=str(row["request_id"]),
        signal_date=row["signal_date"],
        d1=row["d1"],
        d2=row["d2"],
        activation_cutoff_ts=row["activation_cutoff_ts"],
        source_semantic_content_hash=str(row["source_semantic_content_hash"]),
        source_payload_hash=str(row["source_payload_hash"]),
        calendar_evidence_hash=str(row["calendar_evidence_hash"]),
        semantic=_json_value(row["enrollment_json"]),
        created_at=row["created_at"],
    )


def _validate_manual_monitor_commit(commit: ManualMonitorEnrollmentCommit) -> None:
    _require_outbox_scope(commit.route_id, commit.official_stream_id, commit.lineage_id)
    if (
        not commit.enrollment_id
        or not commit.source_event_id
        or not commit.official_entry_event_id
        or not commit.request_id
    ):
        raise ValueError(
            "manual monitor enrollment/source/official-entry/request IDs cannot be empty"
        )
    if not commit.strategy_version:
        raise ValueError("manual monitor strategy_version cannot be empty")
    if not commit.signal_date < commit.d1 < commit.d2:
        raise ValueError("manual monitor dates must satisfy signal_date < d1 < d2")
    _require_aware(commit.activation_cutoff_ts, "manual monitor activation_cutoff_ts")
    local_cutoff = commit.activation_cutoff_ts.astimezone(BEIJING_TZ)
    if (
        local_cutoff.date() != commit.d1
        or local_cutoff.timetz().replace(tzinfo=None) != _MANUAL_MONITOR_ENROLLMENT_DEADLINE_WALL
    ):
        raise ValueError("manual monitor activation cutoff must be D1 09:30 Asia/Shanghai")
    for value, field_name in (
        (commit.source_semantic_content_hash, "source_semantic_content_hash"),
        (commit.source_payload_hash, "source_payload_hash"),
        (commit.calendar_evidence_hash, "calendar_evidence_hash"),
        (commit.enrollment_semantic_hash, "enrollment_semantic_hash"),
        (commit.source_config_hash, "source_config_hash"),
        (commit.state_semantics_hash, "state_semantics_hash"),
    ):
        _require_sha256(value, field_name)
    if sha256_json(commit.enrollment_semantic) != commit.enrollment_semantic_hash:
        raise V20SemanticConflict("manual monitor enrollment semantic hash mismatch")

    batch = commit.model_batch
    if not batch.model_batch_id or not batch.reference_profile_id:
        raise ValueError("manual monitor model batch IDs/profile cannot be empty")
    if batch.evaluation_only:
        raise ValueError("manual monitor batch must be actionable for exit notifications")
    if not math.isfinite(batch.multiplier) or not 0 < batch.multiplier <= 1:
        raise ValueError("manual monitor multiplier must be finite and in (0, 1]")
    if not batch.legs:
        raise ValueError("manual monitor batch must contain at least one leg")
    seen_codes: set[str] = set()
    seen_ranks: set[int] = set()
    seen_leg_ids: set[str] = set()
    for leg in batch.legs:
        if not leg.model_leg_id or not re.fullmatch(r"\d{6}", leg.code):
            raise ValueError("manual monitor model leg ID/code is invalid")
        if not leg.stock_name:
            raise ValueError("manual monitor stock_name cannot be empty")
        if leg.rank <= 0 or leg.rank in seen_ranks:
            raise ValueError("manual monitor ranks must be unique positive integers")
        if leg.code in seen_codes or leg.model_leg_id in seen_leg_ids:
            raise ValueError("manual monitor leg codes and IDs must be unique")
        if not math.isfinite(leg.relative_weight) or not 0 < leg.relative_weight <= 1:
            raise ValueError("manual monitor leg weight must be finite and in (0, 1]")
        if (leg.d1, leg.d2) != (commit.d1, commit.d2):
            raise ValueError("manual monitor leg dates must match enrollment D1/D2")
        seen_codes.add(leg.code)
        seen_ranks.add(leg.rank)
        seen_leg_ids.add(leg.model_leg_id)
    if not math.isclose(
        sum(leg.relative_weight for leg in batch.legs),
        batch.multiplier,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        raise ValueError("manual monitor leg weights must sum to the batch multiplier")


_MIGRATION_TEMPLATE = r"""
CREATE SCHEMA IF NOT EXISTS {schema};
CREATE SEQUENCE IF NOT EXISTS {schema}.commit_marker_seq;

CREATE TABLE IF NOT EXISTS {schema}.runtime_configs (
    config_id TEXT PRIMARY KEY,
    config_hash CHAR(64) NOT NULL UNIQUE,
    strategy_version TEXT NOT NULL,
    deployment_mode TEXT NOT NULL,
    effective_trade_date DATE NOT NULL,
    config_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS {schema}.official_state (
    lineage_id TEXT PRIMARY KEY,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    state_hash CHAR(64) NOT NULL,
    state_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS {schema}.state_lineage_registry (
    lineage_id TEXT PRIMARY KEY,
    official_stream_id TEXT NOT NULL,
    genesis_state_hash CHAR(64) NOT NULL,
    state_semantics_hash CHAR(64) NOT NULL,
    bootstrap_mode TEXT NOT NULL
        CHECK (bootstrap_mode IN ('EMPTY_FORWARD_SHADOW','CHECKPOINT')),
    bootstrap_checkpoint_hash CHAR(64),
    bootstrap_predecessor_trade_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (bootstrap_mode='EMPTY_FORWARD_SHADOW' AND bootstrap_checkpoint_hash IS NULL)
        OR (bootstrap_mode='CHECKPOINT' AND bootstrap_checkpoint_hash IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS {schema}.state_semantics_compatibility (
    lineage_id TEXT NOT NULL REFERENCES {schema}.state_lineage_registry(lineage_id),
    official_stream_id TEXT NOT NULL,
    legacy_state_semantics_hash CHAR(64) NOT NULL,
    core_state_semantics_hash CHAR(64) NOT NULL,
    evidence_config_id TEXT NOT NULL REFERENCES {schema}.runtime_configs(config_id),
    evidence_config_hash CHAR(64) NOT NULL,
    accepted_config_id TEXT NOT NULL REFERENCES {schema}.runtime_configs(config_id),
    accepted_config_hash CHAR(64) NOT NULL,
    evidence_json JSONB NOT NULL,
    evidence_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
        lineage_id, legacy_state_semantics_hash,
        core_state_semantics_hash, accepted_config_hash
    )
);

ALTER TABLE {schema}.state_lineage_registry
    ADD COLUMN IF NOT EXISTS bootstrap_predecessor_trade_date DATE;
ALTER TABLE {schema}.state_lineage_registry
    ADD COLUMN IF NOT EXISTS state_semantics_hash CHAR(64);
UPDATE {schema}.state_lineage_registry
SET bootstrap_predecessor_trade_date=
    (created_at AT TIME ZONE 'Asia/Shanghai')::date - 1
WHERE bootstrap_predecessor_trade_date IS NULL
  AND bootstrap_mode='EMPTY_FORWARD_SHADOW';

CREATE TABLE IF NOT EXISTS {schema}.decision_slots (
    official_stream_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    slot_id TEXT NOT NULL UNIQUE,
    strategy_version TEXT NOT NULL,
    config_id TEXT NOT NULL REFERENCES {schema}.runtime_configs(config_id),
    config_hash CHAR(64) NOT NULL,
    lineage_id TEXT NOT NULL,
    slot_status TEXT NOT NULL CHECK (slot_status IN ('OPEN','COMPLETED','FAILED')),
    slot_revision BIGINT NOT NULL DEFAULT 0,
    terminal_event_id TEXT,
    terminal_decision_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (official_stream_id, trade_date)
);

CREATE TABLE IF NOT EXISTS {schema}.input_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    snapshot_type TEXT NOT NULL,
    trade_date DATE NOT NULL,
    snapshot_hash CHAR(64) NOT NULL,
    snapshot_json JSONB NOT NULL,
    first_received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (snapshot_type, trade_date, snapshot_hash)
);

CREATE TABLE IF NOT EXISTS {schema}.entry_decisions (
    decision_id TEXT PRIMARY KEY,
    slot_id TEXT NOT NULL UNIQUE REFERENCES {schema}.decision_slots(slot_id),
    event_id TEXT NOT NULL UNIQUE,
    snapshot_id TEXT NOT NULL REFERENCES {schema}.input_snapshots(snapshot_id),
    action TEXT NOT NULL CHECK (action IN ('ENTER','BLOCK','NO_SIGNAL','INPUT_INVALID')),
    final_multiplier DOUBLE PRECISION NOT NULL
        CHECK (final_multiplier >= 0 AND final_multiplier <= 1),
    semantic_content_hash CHAR(64) NOT NULL,
    commit_fingerprint CHAR(64) NOT NULL,
    semantic_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS {schema}.shadow_batches (
    batch_id TEXT PRIMARY KEY,
    decision_id TEXT REFERENCES {schema}.entry_decisions(decision_id),
    official_stream_id TEXT NOT NULL,
    lineage_id TEXT NOT NULL,
    source_batch_id TEXT,
    kind TEXT NOT NULL CHECK (kind IN ('HEALTH','ROLLING7')),
    signal_date DATE NOT NULL,
    t2_date DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','COMPLETE_VALID','COMPLETE_INVALID')),
    batch_json JSONB NOT NULL,
    batch_return DOUBLE PRECISION,
    reference_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (reference_status IN ('PENDING','LOCKED','UNAVAILABLE')),
    reference_prices_json JSONB,
    reference_snapshot_hash CHAR(64),
    reference_locked_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    UNIQUE (decision_id, kind),
    CHECK (
        (reference_status='PENDING' AND reference_prices_json IS NULL
            AND reference_snapshot_hash IS NULL AND reference_locked_at IS NULL)
        OR (reference_status='LOCKED' AND reference_prices_json IS NOT NULL
            AND reference_snapshot_hash IS NOT NULL AND reference_locked_at IS NOT NULL)
        OR (reference_status='UNAVAILABLE' AND reference_prices_json IS NULL
            AND reference_snapshot_hash IS NOT NULL AND reference_locked_at IS NOT NULL)
    ),
    CHECK (
        (status='PENDING' AND completed_at IS NULL)
        OR (status='COMPLETE_VALID' AND batch_return IS NOT NULL AND completed_at IS NOT NULL)
        OR (status='COMPLETE_INVALID' AND batch_return IS NULL AND completed_at IS NOT NULL)
    )
);

ALTER TABLE {schema}.shadow_batches
    ADD COLUMN IF NOT EXISTS official_stream_id TEXT;
ALTER TABLE {schema}.shadow_batches
    ADD COLUMN IF NOT EXISTS lineage_id TEXT;
ALTER TABLE {schema}.shadow_batches
    ADD COLUMN IF NOT EXISTS source_batch_id TEXT;
UPDATE {schema}.shadow_batches AS shadow
SET official_stream_id=slot.official_stream_id,
    lineage_id=slot.lineage_id
FROM {schema}.entry_decisions AS decision,
     {schema}.decision_slots AS slot
WHERE shadow.decision_id=decision.decision_id
  AND decision.slot_id=slot.slot_id
  AND (shadow.official_stream_id IS NULL OR shadow.lineage_id IS NULL);
ALTER TABLE {schema}.shadow_batches
    ALTER COLUMN official_stream_id SET NOT NULL;
ALTER TABLE {schema}.shadow_batches
    ALTER COLUMN lineage_id SET NOT NULL;
ALTER TABLE {schema}.shadow_batches
    ALTER COLUMN decision_id DROP NOT NULL;

CREATE INDEX IF NOT EXISTS idx_v20_shadow_scope_maturity
    ON {schema}.shadow_batches(official_stream_id,lineage_id,t2_date,kind,status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_v20_shadow_source_mapping
    ON {schema}.shadow_batches(lineage_id,source_batch_id)
    WHERE source_batch_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS {schema}.model_batches (
    model_batch_id TEXT PRIMARY KEY,
    decision_id TEXT UNIQUE REFERENCES {schema}.entry_decisions(decision_id),
    origin_kind TEXT NOT NULL DEFAULT 'OFFICIAL_ENTRY',
    source_event_id TEXT,
    official_stream_id TEXT,
    lineage_id TEXT,
    signal_date DATE NOT NULL,
    multiplier DOUBLE PRECISION NOT NULL CHECK (multiplier > 0 AND multiplier <= 1),
    evaluation_only BOOLEAN NOT NULL,
    reference_profile_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS {schema}.model_legs (
    model_leg_id TEXT PRIMARY KEY,
    model_batch_id TEXT NOT NULL REFERENCES {schema}.model_batches(model_batch_id),
    code VARCHAR(6) NOT NULL,
    stock_name TEXT NOT NULL,
    rank SMALLINT NOT NULL CHECK (rank > 0),
    relative_weight DOUBLE PRECISION NOT NULL
        CHECK (relative_weight > 0 AND relative_weight <= 1),
    d1 DATE NOT NULL,
    d2 DATE NOT NULL,
    reference_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (reference_status IN ('PENDING','LOCKED','UNAVAILABLE')),
    reference_price DOUBLE PRECISION,
    reference_snapshot_hash CHAR(64),
    reference_locked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (model_batch_id, rank),
    UNIQUE (model_batch_id, code),
    CHECK (d2 > d1),
    CHECK (
        (reference_status='PENDING' AND reference_price IS NULL
            AND reference_snapshot_hash IS NULL AND reference_locked_at IS NULL)
        OR (reference_status='LOCKED' AND reference_price > 0
            AND reference_price < 'Infinity'::double precision
            AND reference_snapshot_hash IS NOT NULL
            AND reference_locked_at IS NOT NULL)
        OR (reference_status='UNAVAILABLE' AND reference_price IS NULL
            AND reference_snapshot_hash IS NOT NULL AND reference_locked_at IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS {schema}.mews_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    source_trade_date DATE NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    receipt_sealed_at TIMESTAMPTZ,
    fast_state TEXT NOT NULL,
    model_version TEXT NOT NULL,
    data_version TEXT NOT NULL,
    content_hash CHAR(64) NOT NULL UNIQUE,
    snapshot_json JSONB NOT NULL
);

ALTER TABLE {schema}.mews_snapshots
    ADD COLUMN IF NOT EXISTS receipt_sealed_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS {schema}.mews_calculation_state (
    state_key TEXT PRIMARY KEY CHECK (state_key='mews_v2'),
    state_date DATE NOT NULL,
    model_version TEXT NOT NULL,
    content_hash CHAR(64) NOT NULL,
    state_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS {schema}.leg_mews_selection (
    model_leg_id TEXT PRIMARY KEY REFERENCES {schema}.model_legs(model_leg_id),
    snapshot_id TEXT REFERENCES {schema}.mews_snapshots(snapshot_id),
    fast_state TEXT,
    cutoff_ts TIMESTAMPTZ NOT NULL,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    selection_reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {schema}.exit_intents (
    exit_intent_id TEXT PRIMARY KEY,
    model_leg_id TEXT NOT NULL UNIQUE REFERENCES {schema}.model_legs(model_leg_id),
    event_id TEXT NOT NULL UNIQUE,
    signal_type TEXT NOT NULL,
    trigger_ts TIMESTAMPTZ NOT NULL,
    rule_actionable_from TIMESTAMPTZ NOT NULL,
    semantic_content_hash CHAR(64) NOT NULL,
    commit_fingerprint CHAR(64) NOT NULL,
    semantic_json JSONB NOT NULL,
    initial_exit_persisted_local_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS {schema}.outbox_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    route_id TEXT NOT NULL,
    official_stream_id TEXT NOT NULL,
    lineage_id TEXT NOT NULL,
    semantic_content_hash CHAR(64) NOT NULL,
    semantic_json JSONB NOT NULL,
    payload_json JSONB,
    payload_hash CHAR(64),
    seal_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (seal_status IN ('PENDING','SEALED')),
    seal_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (seal_attempt_count >= 0),
    seal_last_attempt_at TIMESTAMPTZ,
    seal_last_error TEXT,
    delivery_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (delivery_status IN ('PENDING','LEASED','SENT')),
    action_expiry_ts TIMESTAMPTZ,
    generated_at TIMESTAMPTZ,
    commit_marker BIGINT UNIQUE,
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error TEXT,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((seal_status='PENDING' AND payload_json IS NULL AND payload_hash IS NULL)
        OR (seal_status='SEALED' AND payload_json IS NOT NULL AND payload_hash IS NOT NULL
            AND generated_at IS NOT NULL AND commit_marker IS NOT NULL)),
    CHECK ((delivery_status='LEASED' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
        OR (delivery_status<>'LEASED' AND lease_owner IS NULL AND lease_until IS NULL)),
    CHECK (event_type<>'ENTRY_DECISION' OR action_expiry_ts IS NOT NULL)
);

ALTER TABLE {schema}.model_batches
    ADD COLUMN IF NOT EXISTS origin_kind TEXT NOT NULL DEFAULT 'OFFICIAL_ENTRY';
ALTER TABLE {schema}.model_batches
    ADD COLUMN IF NOT EXISTS source_event_id TEXT;
ALTER TABLE {schema}.model_batches
    ADD COLUMN IF NOT EXISTS official_stream_id TEXT;
ALTER TABLE {schema}.model_batches
    ADD COLUMN IF NOT EXISTS lineage_id TEXT;
ALTER TABLE {schema}.model_batches
    ALTER COLUMN decision_id DROP NOT NULL;
UPDATE {schema}.model_batches AS batch
SET source_event_id=decision.event_id,
    official_stream_id=slot.official_stream_id,
    lineage_id=slot.lineage_id,
    origin_kind='OFFICIAL_ENTRY'
FROM {schema}.entry_decisions AS decision
JOIN {schema}.decision_slots AS slot USING (slot_id)
WHERE batch.decision_id=decision.decision_id
  AND (
      batch.source_event_id IS NULL OR batch.official_stream_id IS NULL
      OR batch.lineage_id IS NULL
  );
ALTER TABLE {schema}.model_batches
    ALTER COLUMN source_event_id SET NOT NULL;
ALTER TABLE {schema}.model_batches
    ALTER COLUMN official_stream_id SET NOT NULL;
ALTER TABLE {schema}.model_batches
    ALTER COLUMN lineage_id SET NOT NULL;
DO $v20_model_batch_constraints$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname='ck_v20_model_batch_origin'
          AND conrelid='{schema}.model_batches'::regclass
    ) THEN
        ALTER TABLE {schema}.model_batches
            ADD CONSTRAINT ck_v20_model_batch_origin CHECK (
                (origin_kind='OFFICIAL_ENTRY' AND decision_id IS NOT NULL)
                OR (origin_kind='MANUAL_MONITOR' AND decision_id IS NULL)
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname='fk_v20_model_batch_source_event'
          AND conrelid='{schema}.model_batches'::regclass
    ) THEN
        ALTER TABLE {schema}.model_batches
            ADD CONSTRAINT fk_v20_model_batch_source_event
            FOREIGN KEY (source_event_id) REFERENCES {schema}.outbox_events(event_id)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$v20_model_batch_constraints$;
CREATE UNIQUE INDEX IF NOT EXISTS uq_v20_model_batch_source_event
    ON {schema}.model_batches(source_event_id);

CREATE TABLE IF NOT EXISTS {schema}.manual_monitor_enrollments (
    enrollment_id TEXT PRIMARY KEY,
    source_event_id TEXT NOT NULL UNIQUE REFERENCES {schema}.outbox_events(event_id),
    official_entry_event_id TEXT NOT NULL,
    model_batch_id TEXT NOT NULL UNIQUE REFERENCES {schema}.model_batches(model_batch_id),
    request_id TEXT NOT NULL,
    official_stream_id TEXT NOT NULL,
    lineage_id TEXT NOT NULL,
    signal_date DATE NOT NULL,
    d1 DATE NOT NULL,
    d2 DATE NOT NULL,
    activation_cutoff_ts TIMESTAMPTZ NOT NULL,
    source_semantic_content_hash CHAR(64) NOT NULL,
    source_payload_hash CHAR(64) NOT NULL,
    calendar_evidence_hash CHAR(64) NOT NULL,
    enrollment_semantic_hash CHAR(64) NOT NULL,
    enrollment_fingerprint CHAR(64) NOT NULL,
    enrollment_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (signal_date < d1 AND d1 < d2),
    CHECK (request_id <> ''),
    UNIQUE (official_stream_id,lineage_id,request_id)
);
ALTER TABLE {schema}.manual_monitor_enrollments
    ADD COLUMN IF NOT EXISTS official_entry_event_id TEXT;
UPDATE {schema}.manual_monitor_enrollments AS enrollment
SET official_entry_event_id=source.semantic_json->>'official_entry_event_id'
FROM {schema}.outbox_events AS source
WHERE source.event_id=enrollment.source_event_id
  AND enrollment.official_entry_event_id IS NULL;
ALTER TABLE {schema}.manual_monitor_enrollments
    ALTER COLUMN official_entry_event_id SET NOT NULL;
DO $v20_manual_monitor_constraints$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname='fk_v20_manual_monitor_official_entry'
          AND conrelid='{schema}.manual_monitor_enrollments'::regclass
    ) THEN
        ALTER TABLE {schema}.manual_monitor_enrollments
            ADD CONSTRAINT fk_v20_manual_monitor_official_entry
            FOREIGN KEY (official_entry_event_id) REFERENCES {schema}.outbox_events(event_id);
    END IF;
END
$v20_manual_monitor_constraints$;
CREATE UNIQUE INDEX IF NOT EXISTS uq_v20_manual_monitor_official_entry
    ON {schema}.manual_monitor_enrollments
        (official_stream_id,lineage_id,official_entry_event_id);

-- Upgrade shared schemas without assigning legacy events to a live worker by
-- guesswork. Relationally bound rows are recovered; anything else is retained
-- in an explicit quarantine scope that no configured runtime may claim.
ALTER TABLE {schema}.outbox_events
    ADD COLUMN IF NOT EXISTS official_stream_id TEXT;
ALTER TABLE {schema}.outbox_events
    ADD COLUMN IF NOT EXISTS lineage_id TEXT;
ALTER TABLE {schema}.outbox_events
    ADD COLUMN IF NOT EXISTS action_expiry_ts TIMESTAMPTZ;
ALTER TABLE {schema}.outbox_events
    ADD COLUMN IF NOT EXISTS seal_attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE {schema}.outbox_events
    ADD COLUMN IF NOT EXISTS seal_last_attempt_at TIMESTAMPTZ;
ALTER TABLE {schema}.outbox_events
    ADD COLUMN IF NOT EXISTS seal_last_error TEXT;
UPDATE {schema}.outbox_events AS outbox
SET official_stream_id=slot.official_stream_id,
    lineage_id=slot.lineage_id
FROM {schema}.entry_decisions AS decision
JOIN {schema}.decision_slots AS slot ON slot.slot_id=decision.slot_id
WHERE outbox.event_id=decision.event_id
  AND (outbox.official_stream_id IS NULL OR outbox.lineage_id IS NULL);
UPDATE {schema}.outbox_events AS outbox
SET action_expiry_ts=(slot.trade_date + TIME '09:40') AT TIME ZONE 'Asia/Shanghai'
FROM {schema}.entry_decisions AS decision
JOIN {schema}.decision_slots AS slot ON slot.slot_id=decision.slot_id
WHERE outbox.event_id=decision.event_id
  AND outbox.event_type='ENTRY_DECISION' AND outbox.action_expiry_ts IS NULL;
UPDATE {schema}.outbox_events
SET action_expiry_ts=clock_timestamp()
WHERE event_type='ENTRY_DECISION' AND action_expiry_ts IS NULL;
UPDATE {schema}.outbox_events AS outbox
SET official_stream_id=slot.official_stream_id,
    lineage_id=slot.lineage_id
FROM {schema}.exit_intents AS intent
JOIN {schema}.model_legs AS leg ON leg.model_leg_id=intent.model_leg_id
JOIN {schema}.model_batches AS batch ON batch.model_batch_id=leg.model_batch_id
JOIN {schema}.entry_decisions AS decision ON decision.decision_id=batch.decision_id
JOIN {schema}.decision_slots AS slot ON slot.slot_id=decision.slot_id
WHERE outbox.event_id=intent.event_id
  AND (outbox.official_stream_id IS NULL OR outbox.lineage_id IS NULL);
CREATE TABLE IF NOT EXISTS {schema}.delivery_attempts (
    event_id TEXT NOT NULL REFERENCES {schema}.outbox_events(event_id),
    attempt_number INTEGER NOT NULL,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    succeeded BOOLEAN NOT NULL,
    error_text TEXT,
    PRIMARY KEY (event_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS {schema}.exit_reminders (
    reminder_id TEXT PRIMARY KEY,
    exit_intent_id TEXT NOT NULL REFERENCES {schema}.exit_intents(exit_intent_id),
    original_exit_event_id TEXT NOT NULL,
    reminder_trade_date DATE NOT NULL,
    event_id TEXT NOT NULL UNIQUE,
    semantic_content_hash CHAR(64) NOT NULL,
    semantic_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (exit_intent_id, reminder_trade_date)
);

UPDATE {schema}.outbox_events AS outbox
SET official_stream_id=slot.official_stream_id,
    lineage_id=slot.lineage_id
FROM {schema}.exit_reminders AS reminder
JOIN {schema}.exit_intents AS intent ON intent.exit_intent_id=reminder.exit_intent_id
JOIN {schema}.model_legs AS leg ON leg.model_leg_id=intent.model_leg_id
JOIN {schema}.model_batches AS batch ON batch.model_batch_id=leg.model_batch_id
JOIN {schema}.entry_decisions AS decision ON decision.decision_id=batch.decision_id
JOIN {schema}.decision_slots AS slot ON slot.slot_id=decision.slot_id
WHERE outbox.event_id=reminder.event_id
  AND (outbox.official_stream_id IS NULL OR outbox.lineage_id IS NULL);
UPDATE {schema}.outbox_events
SET official_stream_id=COALESCE(
        NULLIF(official_stream_id,''),
        NULLIF(semantic_json->>'official_stream_id',''),
        'LEGACY_UNSCOPED'
    ),
    lineage_id=COALESCE(
        NULLIF(lineage_id,''),
        NULLIF(semantic_json->>'state_lineage_id',''),
        'LEGACY_UNSCOPED'
    )
WHERE official_stream_id IS NULL OR official_stream_id=''
   OR lineage_id IS NULL OR lineage_id='';
ALTER TABLE {schema}.outbox_events
    ALTER COLUMN official_stream_id SET NOT NULL;
ALTER TABLE {schema}.outbox_events
    ALTER COLUMN lineage_id SET NOT NULL;

CREATE TABLE IF NOT EXISTS {schema}.minute_bars (
    code VARCHAR(6) NOT NULL,
    bar_end TIMESTAMPTZ NOT NULL,
    end_label CHAR(5) NOT NULL,
    source_hash CHAR(64) NOT NULL,
    bar_json JSONB NOT NULL,
    first_received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    receipt_sealed_at TIMESTAMPTZ,
    PRIMARY KEY (code, bar_end, source_hash)
);

ALTER TABLE {schema}.minute_bars
    ADD COLUMN IF NOT EXISTS receipt_sealed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_v20_minute_bar_time_code_label
    ON {schema}.minute_bars(bar_end,code,end_label)
    WHERE receipt_sealed_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS {schema}.daily_bar_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    trade_date DATE NOT NULL,
    source_hash CHAR(64) NOT NULL,
    snapshot_json JSONB NOT NULL,
    first_received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    receipt_sealed_at TIMESTAMPTZ,
    receipt_sequence BIGSERIAL NOT NULL UNIQUE,
    UNIQUE (trade_date, source_hash)
);

CREATE INDEX IF NOT EXISTS idx_v20_daily_snapshot_receipt
    ON {schema}.daily_bar_snapshots(trade_date,first_received_at,receipt_sequence);

CREATE TABLE IF NOT EXISTS {schema}.exit_scan_watermarks (
    model_leg_id TEXT NOT NULL REFERENCES {schema}.model_legs(model_leg_id),
    trade_date DATE NOT NULL,
    scanned_through_label CHAR(5) NOT NULL,
    source_hash CHAR(64) NOT NULL,
    first_scanned_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (model_leg_id, trade_date),
    CHECK (scanned_through_label ~ '^[0-9]{{2}}:[0-9]{{2}}$')
);

CREATE TABLE IF NOT EXISTS {schema}.reminder_stop_acks (
    ack_id TEXT PRIMARY KEY,
    original_exit_event_id TEXT NOT NULL REFERENCES {schema}.outbox_events(event_id),
    consumer_id TEXT NOT NULL,
    ack_ts TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    receipt_sealed_at TIMESTAMPTZ,
    auth_evidence_hash CHAR(64) NOT NULL,
    UNIQUE (original_exit_event_id, consumer_id)
);

ALTER TABLE {schema}.reminder_stop_acks
    ADD COLUMN IF NOT EXISTS receipt_sealed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_v20_outbox_ready
    ON {schema}.outbox_events(delivery_status, seal_status, available_at);
CREATE INDEX IF NOT EXISTS idx_v20_outbox_scope_ready
    ON {schema}.outbox_events(
        route_id,official_stream_id,lineage_id,available_at,created_at,event_id
    ) WHERE seal_status='SEALED' AND delivery_status <> 'SENT';
CREATE INDEX IF NOT EXISTS idx_v20_outbox_scope_unsealed
    ON {schema}.outbox_events(route_id,official_stream_id,lineage_id,event_id)
    WHERE seal_status='PENDING';
CREATE INDEX IF NOT EXISTS idx_v20_slots_scope_date
    ON {schema}.decision_slots(official_stream_id, lineage_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_v20_legs_dates ON {schema}.model_legs(d1, d2);
CREATE INDEX IF NOT EXISTS idx_v20_shadow_maturity ON {schema}.shadow_batches(status, t2_date);
CREATE INDEX IF NOT EXISTS idx_v20_exit_reminder_date
    ON {schema}.exit_reminders(reminder_trade_date, exit_intent_id);
"""


def migration_sql(schema: str = "v20") -> str:
    """Render the schema-qualified migration after validating the identifier."""
    if not _IDENTIFIER.fullmatch(schema):
        raise ValueError(f"invalid PostgreSQL schema identifier: {schema!r}")
    return (
        _MIGRATION_TEMPLATE.format(schema=schema)
        + "\n\n"
        + _render_outbox_at_most_once_migration(schema)
        + "\n\n"
        + _render_rolling7_market_health_migration(schema)
        + "\n"
    )


@dataclass(frozen=True)
class Rolling7MarketHealthRecord:
    batch: Rolling7Batch
    updated_at: datetime


def _legacy_shadow_rows_valid(rows):
    kinds = [str(row["kind"]) for row in rows]
    valid = kinds.count("HEALTH") == 1
    valid = valid and kinds.count("ROLLING7") <= 1
    valid = valid and set(kinds) <= {"HEALTH", "ROLLING7"}
    return valid


def _rolling7_market_health_evidence(values) -> dict[str, float]:
    if not isinstance(values, dict):
        raise V20SemanticConflict("rolling7 market evidence is invalid")
    evidence: dict[str, float] = {}
    try:
        for code, value in values.items():
            if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
                raise ValueError
            number = float(value)
            if not math.isfinite(number) or number <= 0:
                raise ValueError
            evidence[str(code)] = number
    except (OverflowError, TypeError, ValueError) as error:
        raise V20SemanticConflict("rolling7 market evidence is invalid") from error
    return evidence


class V20Repository:
    def __init__(
        self,
        config: V20DatabaseConfig,
        *,
        shared_pool: Any | None = None,
    ) -> None:
        if shared_pool is not None and config.connection_profile != "legacy_embedded":
            raise ValueError("only embedded V20 may borrow a shared database pool")
        self.config = config
        self.schema = config.schema
        self._pool: asyncpg.Pool | None = shared_pool
        self._owns_pool = shared_pool is None
        self._connection_ready = False
        self._leader_connection: Any | None = None
        self._leader_key: int | None = None
        self._leader_scope: tuple[str, str, str] | None = None
        self._leader_probe_lock = asyncio.Lock()

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise V20RepositoryError("V20 repository is not connected")
        return self._pool

    @property
    def uses_shared_pool(self) -> bool:
        """Whether pool lifetime belongs to the legacy main runtime."""

        return not self._owns_pool

    async def connect(self, *, migrate: bool = True) -> None:
        if self._connection_ready:
            return
        created_pool = False
        if self._pool is None:
            if self.config.ssl_mode == "verify-full":
                ssl_context: bool | str | object = verified_postgres_ssl_context(
                    ssl_mode=self.config.ssl_mode,
                    ssl_root_cert=self.config.ssl_root_cert,
                    expected_sha256=self.config.ssl_root_cert_sha256,
                )
            elif self.config.ssl_mode == "disable":
                # The deployed legacy trading/state pools omit asyncpg's
                # ``ssl`` argument, whose effective value is False.  The
                # embedded fallback mirrors that connection contract exactly.
                ssl_context = False
            else:
                # This branch is reachable only for the explicit
                # legacy_embedded profile when trading opts into TLS.
                ssl_context = self.config.ssl_mode
            self._pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                min_size=self.config.pool_min_size,
                max_size=self.config.pool_max_size,
                ssl=ssl_context,
                timeout=self.config.connect_timeout_seconds,
                command_timeout=self.config.command_timeout_seconds,
                server_settings={
                    "lock_timeout": "3000",
                    "idle_in_transaction_session_timeout": "15000",
                },
            )
            created_pool = True
        try:
            if migrate:
                await self.migrate()
        except BaseException as migration_error:
            # A borrowed pool remains live for main and for a later bounded V20
            # retry.  Only the pool allocated by this connect attempt belongs
            # to the repository and may be closed here.
            if created_pool:
                pool = self._pool
                self._pool = None
                if pool is not None:
                    try:
                        await pool.close()
                    except Exception as close_error:
                        logger.exception("failed to close V20 pool after migration failure")
                        # Preserve the migration failure as the public error while
                        # retaining the cleanup failure as explicit exception
                        # context.  Neither failure is allowed to look successful.
                        raise migration_error from close_error
            raise
        self._connection_ready = True

    async def close(self) -> None:
        pool = self._pool
        if pool is None:
            return

        leader = self._leader_connection
        leader_key = self._leader_key
        self._leader_connection = None
        self._leader_key = None
        self._leader_scope = None
        try:
            if leader is not None:
                try:
                    if leader_key is not None and not leader.is_closed():
                        await leader.fetchval("SELECT pg_advisory_unlock($1::bigint)", leader_key)
                finally:
                    await pool.release(leader)
        finally:
            self._connection_ready = False
            if self._owns_pool:
                try:
                    await pool.close()
                finally:
                    self._pool = None

    async def acquire_runtime_leader(
        self,
        *,
        route_id: str,
        official_stream_id: str,
        lineage_id: str,
    ) -> None:
        """Hold a PostgreSQL session lock for the sole official V20 worker."""

        _require_outbox_scope(route_id, official_stream_id, lineage_id)
        scope = (route_id, official_stream_id, lineage_id)
        if self._leader_connection is not None:
            if self._leader_scope != scope:
                raise V20StateConflict("repository already leads a different V20 scope")
            await self.assert_runtime_leader()
            return
        digest = hashlib.sha256(
            # A lineage or stream change must not create a second publisher to
            # the same public route.  Stream/lineage remain immutable ledger
            # identity but deliberately do not weaken the side-effect singleton.
            canonical_json(["V20_RUNTIME_PUBLIC_ROUTE_LEADER_V3", route_id]).encode("utf-8")
        ).digest()
        key = int.from_bytes(digest[:8], "big", signed=True)
        connection = await self.pool.acquire()
        try:
            locked = await connection.fetchval("SELECT pg_try_advisory_lock($1::bigint)", key)
            if not bool(locked):
                raise V20StateConflict("another V20 worker already owns this public route")
        except Exception:
            await self.pool.release(connection)
            raise
        self._leader_connection = connection
        self._leader_key = key
        self._leader_scope = scope

    async def assert_runtime_leader(self) -> None:
        """Fail closed if the session carrying the singleton lock was lost."""

        async with self._leader_probe_lock:
            connection = self._leader_connection
            if connection is None or self._leader_scope is None:
                raise V20LeadershipLost("V20 runtime leader lock is not held")
            if connection.is_closed():
                raise V20LeadershipLost("V20 runtime leader connection was lost")
            try:
                await connection.fetchval("SELECT 1")
            except Exception as exc:
                raise V20LeadershipLost("V20 runtime leader session probe failed") from exc

    async def database_cutoff_reached(self, cutoff: datetime) -> bool:
        """Evaluate an irreversible boundary using PostgreSQL's wall clock."""

        _require_aware(cutoff, "database cutoff")
        async with self.pool.acquire() as connection:
            reached = await connection.fetchval("SELECT clock_timestamp() >= $1", cutoff)
        if not isinstance(reached, bool):
            raise V20RepositoryError("database cutoff query returned a non-boolean value")
        return reached

    async def migrate(self) -> None:
        sql = migration_sql(self.schema)
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL lock_timeout = '3s'")
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    f"{self.schema}:002_outbox_at_most_once",
                )
                await connection.execute(sql)

    async def register_config(
        self,
        *,
        config_id: str,
        config_hash: str,
        strategy_version: str,
        deployment_mode: str,
        effective_trade_date: date,
        payload: Mapping[str, Any],
    ) -> None:
        _require_sha256(config_hash, "config_hash")
        sql = f"""
            INSERT INTO {self.schema}.runtime_configs
                (config_id, config_hash, strategy_version, deployment_mode,
                 effective_trade_date, config_json)
            VALUES ($1,$2,$3,$4,$5,$6::jsonb)
            ON CONFLICT (config_id) DO NOTHING
        """
        async with self.pool.acquire() as connection:
            await connection.execute(
                sql,
                config_id,
                config_hash,
                strategy_version,
                deployment_mode,
                effective_trade_date,
                canonical_json(payload),
            )
            row = await connection.fetchrow(
                f"SELECT config_hash,strategy_version,deployment_mode,effective_trade_date,"
                f"config_json FROM {self.schema}.runtime_configs "
                "WHERE config_id=$1",
                config_id,
            )
        if (
            row is None
            or row["config_hash"] != config_hash
            or row["strategy_version"] != strategy_version
            or row["deployment_mode"] != deployment_mode
            or _json_value(row["config_json"]) != payload
        ):
            raise V20SemanticConflict(f"config_id {config_id!r} already has different semantics")

    async def ensure_genesis_state(
        self,
        lineage_id: str,
        state: Mapping[str, Any],
        state_hash: str,
        *,
        official_stream_id: str,
        state_semantics_hash: str,
        bootstrap_mode: str,
        bootstrap_checkpoint_hash: str | None,
        bootstrap_predecessor_trade_date: date,
        bootstrap_shadow_batches: Sequence[Mapping[str, Any]] = (),
    ) -> StateRecord:
        _require_scope(official_stream_id, lineage_id)
        _require_sha256(state_hash, "state_hash")
        _require_sha256(state_semantics_hash, "state_semantics_hash")
        if sha256_json(state) != state_hash:
            raise V20SemanticConflict("genesis state_hash mismatch")
        if state.get("state_revision") != 0:
            raise ValueError("genesis state must have state_revision=0")
        if type(bootstrap_predecessor_trade_date) is not date:
            raise ValueError("bootstrap predecessor trade date must be a date")
        if bootstrap_mode not in {"EMPTY_FORWARD_SHADOW", "CHECKPOINT"}:
            raise ValueError("unsupported state bootstrap mode")
        if bootstrap_mode == "CHECKPOINT":
            if bootstrap_checkpoint_hash is None:
                raise ValueError("checkpoint bootstrap requires its hash")
            _require_sha256(bootstrap_checkpoint_hash, "bootstrap_checkpoint_hash")
        elif bootstrap_checkpoint_hash is not None:
            raise ValueError("empty shadow bootstrap cannot carry a checkpoint hash")
        normalized_shadows = _normalize_bootstrap_shadows(
            bootstrap_shadow_batches,
            checkpoint_import=bootstrap_mode == "CHECKPOINT",
        )
        _validate_checkpoint_state_facts(state, normalized_shadows)
        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.state_lineage_registry
                        (lineage_id,official_stream_id,genesis_state_hash,
                         state_semantics_hash,bootstrap_mode,bootstrap_checkpoint_hash,
                         bootstrap_predecessor_trade_date)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    ON CONFLICT (lineage_id) DO NOTHING
                    """,
                    lineage_id,
                    official_stream_id,
                    state_hash,
                    state_semantics_hash,
                    bootstrap_mode,
                    bootstrap_checkpoint_hash,
                    bootstrap_predecessor_trade_date,
                )
                registry = await connection.fetchrow(
                    f"""
                    SELECT official_stream_id,genesis_state_hash,bootstrap_mode,
                           bootstrap_checkpoint_hash,bootstrap_predecessor_trade_date,
                           created_at
                    FROM {self.schema}.state_lineage_registry
                    WHERE lineage_id=$1
                    FOR UPDATE
                    """,
                    lineage_id,
                )
                if registry is None:
                    raise V20StateConflict("state lineage registry insert did not become visible")
                registry_predecessor = registry["bootstrap_predecessor_trade_date"]
                expected_predecessor = (
                    registry_predecessor
                    if bootstrap_mode == "EMPTY_FORWARD_SHADOW"
                    else bootstrap_predecessor_trade_date
                )
                if registry_predecessor is None:
                    # Upgrade a lineage created before the predecessor boundary
                    # became explicit.  Empty-shadow inception is derived from
                    # its immutable creation receipt; a checkpoint boundary is
                    # authenticated by the already-bound checkpoint hash.
                    if bootstrap_mode == "EMPTY_FORWARD_SHADOW":
                        created_at = registry["created_at"]
                        if not isinstance(created_at, datetime):
                            raise V20SemanticConflict(
                                "legacy V20 lineage has an invalid creation receipt"
                            )
                        registry_predecessor = created_at.astimezone(BEIJING_TZ).date() - timedelta(
                            days=1
                        )
                        expected_predecessor = registry_predecessor
                    else:
                        registry_predecessor = bootstrap_predecessor_trade_date
                    updated_predecessor = await connection.execute(
                        f"""
                        UPDATE {self.schema}.state_lineage_registry
                        SET bootstrap_predecessor_trade_date=$1
                        WHERE lineage_id=$2
                          AND bootstrap_predecessor_trade_date IS NULL
                        """,
                        registry_predecessor,
                        lineage_id,
                    )
                    if updated_predecessor != "UPDATE 1":
                        raise V20StateConflict("bootstrap predecessor boundary CAS lost")
                expected_registry = (
                    official_stream_id,
                    state_hash,
                    bootstrap_mode,
                    bootstrap_checkpoint_hash,
                    expected_predecessor,
                )
                actual_registry = (
                    registry["official_stream_id"],
                    registry["genesis_state_hash"],
                    registry["bootstrap_mode"],
                    registry["bootstrap_checkpoint_hash"],
                    registry_predecessor,
                )
                if actual_registry != expected_registry:
                    raise V20SemanticConflict(
                        "existing V20 lineage has different bootstrap/stream semantics"
                    )
                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.official_state
                        (lineage_id, revision, state_hash, state_json)
                    VALUES ($1,0,$2,$3::jsonb)
                    ON CONFLICT (lineage_id) DO NOTHING
                    """,
                    lineage_id,
                    state_hash,
                    canonical_json(state),
                )
                state_row = await connection.fetchrow(
                    f"""
                    SELECT revision,state_hash,state_json
                    FROM {self.schema}.official_state
                    WHERE lineage_id=$1
                    FOR UPDATE
                    """,
                    lineage_id,
                )
                if state_row is None:
                    raise V20StateConflict("genesis state insert did not become visible")
                stored_payload = _json_value(state_row["state_json"])
                if sha256_json(stored_payload) != state_row["state_hash"]:
                    raise V20SemanticConflict("persisted official state hash mismatch")
                if not isinstance(stored_payload, Mapping) or stored_payload.get(
                    "state_revision"
                ) != int(state_row["revision"]):
                    raise V20SemanticConflict("persisted official state revision mismatch")
                _validate_official_state_contract(
                    stored_payload,
                    revision=int(state_row["revision"]),
                )
                if int(state_row["revision"]) == 0 and (
                    state_row["state_hash"] != state_hash or stored_payload != state
                ):
                    raise V20SemanticConflict(
                        "existing genesis state differs from configured checkpoint"
                    )
                for shadow in normalized_shadows:
                    existing = await connection.fetch(
                        f"""
                        SELECT * FROM {self.schema}.shadow_batches
                        WHERE batch_id=$1
                           OR (lineage_id=$2 AND source_batch_id=$3)
                        ORDER BY batch_id
                        FOR UPDATE
                        """,
                        shadow["batch_id"],
                        lineage_id,
                        shadow["source_batch_id"],
                    )
                    if len(existing) > 1 or (
                        existing
                        and not _bootstrap_shadow_is_legal_successor(
                            existing[0],
                            shadow,
                            official_stream_id=official_stream_id,
                            lineage_id=lineage_id,
                        )
                    ):
                        raise V20SemanticConflict(
                            "bootstrap shadow source/target mapping already has different semantics"
                        )
                    if existing:
                        continue
                    if int(state_row["revision"]) > 0:
                        raise V20SemanticConflict(
                            "advanced checkpoint lineage is missing an imported shadow mapping"
                        )
                    await connection.execute(
                        f"""
                        INSERT INTO {self.schema}.shadow_batches
                            (batch_id,decision_id,official_stream_id,lineage_id,
                             source_batch_id,kind,signal_date,t2_date,status,batch_json,
                             batch_return,reference_status,reference_prices_json,
                             reference_snapshot_hash,reference_locked_at,completed_at)
                        VALUES ($1,NULL,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,
                                $12::jsonb,$13,
                                CASE WHEN $11='PENDING' THEN NULL ELSE clock_timestamp() END,
                                CASE WHEN $8='PENDING' THEN NULL ELSE clock_timestamp() END)
                        """,
                        shadow["batch_id"],
                        official_stream_id,
                        lineage_id,
                        shadow["source_batch_id"],
                        shadow["kind"],
                        shadow["signal_date"],
                        shadow["t2_date"],
                        shadow["status"],
                        canonical_json(shadow["payload"]),
                        shadow["batch_return"],
                        shadow["reference_status"],
                        (
                            canonical_json(shadow["reference_prices"])
                            if shadow["reference_prices"] is not None
                            else None
                        ),
                        shadow["reference_snapshot_hash"],
                    )
        return StateRecord(
            lineage_id=lineage_id,
            revision=int(state_row["revision"]),
            state_hash=str(state_row["state_hash"]),
            payload=stored_payload,
        )

    async def load_state(self, lineage_id: str) -> StateRecord:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                f"SELECT revision,state_hash,state_json FROM {self.schema}.official_state "
                "WHERE lineage_id=$1",
                lineage_id,
            )
        if row is None:
            raise V20RepositoryError(f"missing state lineage {lineage_id!r}")
        payload = _json_value(row["state_json"])
        if sha256_json(payload) != row["state_hash"]:
            raise V20SemanticConflict(f"state hash mismatch for lineage {lineage_id!r}")
        if not isinstance(payload, Mapping):
            raise V20SemanticConflict(f"official state is not an object for lineage {lineage_id!r}")
        _validate_official_state_contract(payload, revision=int(row["revision"]))
        return StateRecord(
            lineage_id=lineage_id,
            revision=int(row["revision"]),
            state_hash=str(row["state_hash"]),
            payload=payload,
        )

    async def load_bootstrap_predecessor_trade_date(
        self,
        *,
        official_stream_id: str,
        lineage_id: str,
    ) -> date:
        """Return the immutable date boundary immediately before revision zero."""

        _require_scope(official_stream_id, lineage_id)
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                f"""
                SELECT bootstrap_predecessor_trade_date
                FROM {self.schema}.state_lineage_registry
                WHERE official_stream_id=$1 AND lineage_id=$2
                """,
                official_stream_id,
                lineage_id,
            )
        if row is None or type(row["bootstrap_predecessor_trade_date"]) is not date:
            raise V20RepositoryError("V20 genesis predecessor boundary is missing")
        return row["bootstrap_predecessor_trade_date"]

    async def export_bootstrap_checkpoint(
        self,
        *,
        source_official_stream_id: str,
        source_lineage_id: str,
        target_official_stream_id: str,
        target_lineage_id: str,
        as_of_trade_date: date,
    ) -> Mapping[str, Any]:
        """Export a deterministic forward-shadow to production checkpoint.

        The cut must be taken after the source stream has committed its
        terminal slot for ``as_of_trade_date``.  This avoids exporting a state
        that has not consumed already-mature shadow facts.  The target starts
        at revision zero with no fabricated target-stream predecessor, while
        the health watermark and required HEALTH shadow facts are migrated
        into the new lineage.  Rolling7 restores from its independent table.
        """

        _require_scope(source_official_stream_id, source_lineage_id)
        _require_scope(target_official_stream_id, target_lineage_id)
        if (
            source_official_stream_id == target_official_stream_id
            or source_lineage_id == target_lineage_id
        ):
            raise ValueError("checkpoint export requires distinct source and target identities")

        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable", readonly=True):
                source = await connection.fetchrow(
                    f"""
                    SELECT registry.bootstrap_mode,registry.bootstrap_checkpoint_hash,
                           state.revision,state.state_hash,state.state_json,
                           terminal_slot.slot_id AS source_terminal_slot_id,
                           terminal_slot.trade_date AS source_terminal_trade_date,
                           terminal_slot.slot_status AS source_terminal_slot_status,
                           config.deployment_mode AS source_deployment_mode,
                           (SELECT COUNT(*)
                            FROM {self.schema}.state_lineage_registry AS target_registry
                            WHERE target_registry.lineage_id=$3) AS target_lineage_count
                    FROM {self.schema}.state_lineage_registry AS registry
                    JOIN {self.schema}.official_state AS state USING (lineage_id)
                    JOIN {self.schema}.decision_slots AS terminal_slot
                      ON terminal_slot.slot_id=state.state_json->>'last_terminal_slot_id'
                    JOIN {self.schema}.runtime_configs AS config
                      ON config.config_id=terminal_slot.config_id
                    WHERE registry.official_stream_id=$1 AND registry.lineage_id=$2
                      AND terminal_slot.official_stream_id=$1
                      AND terminal_slot.lineage_id=$2
                    """,
                    source_official_stream_id,
                    source_lineage_id,
                    target_lineage_id,
                )
                if source is None:
                    raise V20RepositoryError("source V20 stream/lineage does not exist")
                source_state = _json_value(source["state_json"])
                if not isinstance(source_state, Mapping):
                    raise V20SemanticConflict("source official state must be an object")
                if sha256_json(source_state) != source["state_hash"]:
                    raise V20SemanticConflict("source official state hash mismatch")
                _validate_official_state_contract(
                    source_state,
                    revision=int(source["revision"]),
                )
                if source_state["last_terminal_trade_date"] != as_of_trade_date.isoformat():
                    raise V20StateConflict(
                        "checkpoint as-of date must equal the source state's last terminal date"
                    )
                if not source_state["last_terminal_slot_id"]:
                    raise V20StateConflict("checkpoint source has no terminal predecessor slot")
                if (
                    source["source_terminal_slot_id"] != source_state["last_terminal_slot_id"]
                    or source["source_terminal_trade_date"] != as_of_trade_date
                    or source["source_terminal_slot_status"] not in {"COMPLETED", "FAILED"}
                ):
                    raise V20StateConflict("checkpoint source terminal slot is not a settled cut")
                if source["source_deployment_mode"] != "forward_shadow":
                    raise V20StateConflict("checkpoint source is not a forward-shadow deployment")
                if int(source["target_lineage_count"]) != 0:
                    raise V20StateConflict("checkpoint target lineage already exists")

                health = source_state["health"]
                if not isinstance(health, Mapping):
                    raise V20SemanticConflict("source health state is malformed")
                recent_valid = health.get("recent_valid")
                last_processed = health.get("last_processed_key")
                if not isinstance(recent_valid, list):
                    raise V20SemanticConflict("source health recent_valid is malformed")
                health_batch_ids: set[str] = set()
                for observation in recent_valid:
                    if not isinstance(observation, Mapping) or not observation.get("batch_id"):
                        raise V20SemanticConflict("source health observation is malformed")
                    health_batch_ids.add(str(observation["batch_id"]))
                if last_processed is not None:
                    if not isinstance(last_processed, list) or len(last_processed) != 3:
                        raise V20SemanticConflict("source health watermark is malformed")
                    health_batch_ids.add(str(last_processed[2]))
                    try:
                        health_watermark_t2 = date.fromisoformat(str(last_processed[0]))
                        health_watermark_signal = date.fromisoformat(str(last_processed[1]))
                    except ValueError as exc:
                        raise V20SemanticConflict(
                            "source health watermark dates are malformed"
                        ) from exc
                    health_watermark_batch = str(last_processed[2])
                else:
                    health_watermark_t2 = None
                    health_watermark_signal = None
                    health_watermark_batch = None

                referenced_ids = sorted(health_batch_ids)
                shadow_rows = await connection.fetch(
                    f"""
                    SELECT shadow.*
                    FROM {self.schema}.shadow_batches AS shadow
                    WHERE shadow.official_stream_id=$1 AND shadow.lineage_id=$2
                      AND shadow.kind='HEALTH'
                      AND (
                        (shadow.status='PENDING' AND shadow.signal_date <= $3)
                        OR shadow.batch_id=ANY($4::text[])
                        OR (
                            shadow.status IN ('COMPLETE_VALID','COMPLETE_INVALID')
                            AND shadow.t2_date <= $3
                            AND (
                                $5::date IS NULL
                                OR (shadow.t2_date,shadow.signal_date,shadow.batch_id)
                                   > ($5::date,$6::date,$7::text)
                            )
                        )
                      )
                    ORDER BY shadow.signal_date,shadow.t2_date,shadow.kind,shadow.batch_id
                    """,
                    source_official_stream_id,
                    source_lineage_id,
                    as_of_trade_date,
                    referenced_ids,
                    health_watermark_t2,
                    health_watermark_signal,
                    health_watermark_batch,
                )

        rows_by_id = {str(row["batch_id"]): row for row in shadow_rows}
        if len(rows_by_id) != len(shadow_rows):
            raise V20SemanticConflict("source checkpoint contains duplicate shadow batch IDs")
        if any(row["kind"] != "HEALTH" for row in shadow_rows):
            raise V20SemanticConflict("source checkpoint query returned a non-HEALTH shadow fact")
        missing_references = health_batch_ids - set(rows_by_id)
        if missing_references:
            raise V20StateConflict(
                "source checkpoint is missing referenced health shadow facts: "
                + ",".join(sorted(missing_references))
            )

        migration_source_ids = set(rows_by_id) | health_batch_ids
        id_mapping = {
            source_batch_id: _bootstrap_target_batch_id(
                source_official_stream_id=source_official_stream_id,
                source_lineage_id=source_lineage_id,
                source_batch_id=source_batch_id,
                target_official_stream_id=target_official_stream_id,
                target_lineage_id=target_lineage_id,
            )
            for source_batch_id in sorted(migration_source_ids)
        }

        target_state = json.loads(canonical_json(source_state))
        target_state["state_revision"] = 0
        target_state["last_terminal_slot_id"] = None
        target_state["last_terminal_trade_date"] = None
        target_state["official_rolling_gaps"] = []
        target_health = target_state["health"]
        for observation in target_health["recent_valid"]:
            observation["batch_id"] = id_mapping[observation["batch_id"]]
        if target_health["last_processed_key"] is not None:
            target_health["last_processed_key"][2] = id_mapping[
                target_health["last_processed_key"][2]
            ]
        exported_batches: list[dict[str, Any]] = []
        for source_batch_id, row in sorted(rows_by_id.items()):
            exported_batches.append(
                {
                    "batch_id": id_mapping[source_batch_id],
                    "source_batch_id": source_batch_id,
                    "kind": str(row["kind"]),
                    "signal_date": row["signal_date"].isoformat(),
                    "t2_date": row["t2_date"].isoformat(),
                    "status": str(row["status"]),
                    "payload": _json_value(row["batch_json"]),
                    "batch_return": (
                        float(row["batch_return"]) if row["batch_return"] is not None else None
                    ),
                    "reference_status": str(row["reference_status"]),
                    "reference_prices": (
                        _json_value(row["reference_prices_json"])
                        if row["reference_prices_json"] is not None
                        else None
                    ),
                    "reference_snapshot_hash": row["reference_snapshot_hash"],
                }
            )
        normalized_batches = _normalize_bootstrap_shadows(
            exported_batches,
            checkpoint_import=True,
        )
        _validate_checkpoint_state_facts(target_state, normalized_batches)
        serialized_batches = [
            {
                **row,
                "signal_date": row["signal_date"].isoformat(),
                "t2_date": row["t2_date"].isoformat(),
            }
            for row in normalized_batches
        ]
        target_state_hash = sha256_json(target_state)
        return {
            "schema_version": _BOOTSTRAP_CHECKPOINT_SCHEMA,
            "target_official_stream_id": target_official_stream_id,
            "state_lineage_id": target_lineage_id,
            "source_official_stream_id": source_official_stream_id,
            "source_lineage_id": source_lineage_id,
            "as_of_trade_date": as_of_trade_date.isoformat(),
            "source_state_revision": int(source["revision"]),
            "source_state_hash": str(source["state_hash"]),
            "source_bootstrap_mode": str(source["bootstrap_mode"]),
            "source_bootstrap_checkpoint_hash": source["bootstrap_checkpoint_hash"],
            "source_last_terminal_slot_id": source_state["last_terminal_slot_id"],
            "source_last_terminal_trade_date": source_state["last_terminal_trade_date"],
            "batch_id_migration": id_mapping,
            "official_state": target_state,
            "official_state_hash": target_state_hash,
            "state_shadow_batches": serialized_batches,
        }

    async def get_entry_status(
        self,
        official_stream_id: str,
        trade_date: date,
    ) -> EntryStatus | None:
        """Load a terminal daily decision with enough inputs for restart recovery."""
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                f"""
                SELECT slot.official_stream_id,slot.trade_date,slot.slot_id,
                       slot.slot_status,slot.slot_revision,slot.strategy_version,
                       slot.config_id,slot.config_hash,slot.lineage_id,
                       decision.decision_id,decision.event_id,decision.action,
                       decision.final_multiplier,decision.semantic_content_hash,
                       decision.semantic_json,decision.snapshot_id,
                       snapshot.snapshot_hash,snapshot.snapshot_json,
                       outbox.action_expiry_ts
                FROM {self.schema}.decision_slots AS slot
                JOIN {self.schema}.entry_decisions AS decision USING (slot_id)
                JOIN {self.schema}.input_snapshots AS snapshot USING (snapshot_id)
                JOIN {self.schema}.outbox_events AS outbox
                  ON outbox.event_id=decision.event_id
                WHERE slot.official_stream_id=$1 AND slot.trade_date=$2
                  AND slot.slot_status IN ('COMPLETED','FAILED')
                """,
                official_stream_id,
                trade_date,
            )
        if row is None:
            return None
        semantic = _json_value(row["semantic_json"])
        snapshot = _json_value(row["snapshot_json"])
        if sha256_json(semantic) != row["semantic_content_hash"]:
            raise V20SemanticConflict("persisted entry semantic hash mismatch")
        if sha256_json(snapshot) != row["snapshot_hash"]:
            raise V20SemanticConflict("persisted entry snapshot hash mismatch")
        return EntryStatus(
            official_stream_id=row["official_stream_id"],
            trade_date=row["trade_date"],
            slot_id=row["slot_id"],
            slot_status=row["slot_status"],
            slot_revision=int(row["slot_revision"]),
            strategy_version=row["strategy_version"],
            config_id=row["config_id"],
            config_hash=row["config_hash"],
            lineage_id=row["lineage_id"],
            decision_id=row["decision_id"],
            event_id=row["event_id"],
            action=row["action"],
            final_multiplier=float(row["final_multiplier"]),
            semantic_content_hash=row["semantic_content_hash"],
            semantic=semantic,
            snapshot_id=row["snapshot_id"],
            snapshot_hash=row["snapshot_hash"],
            snapshot=snapshot,
            action_expiry_ts=row["action_expiry_ts"],
        )

    async def get_manual_monitor_enrollment(
        self,
        source_event_id: str,
        *,
        official_stream_id: str,
        lineage_id: str,
    ) -> ManualMonitorEnrollmentRecord | None:
        """Load one durable manual-monitor enrollment inside its ledger scope."""

        _require_scope(official_stream_id, lineage_id)
        if not source_event_id:
            raise ValueError("source_event_id cannot be empty")
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                f"""
                SELECT enrollment.*
                FROM {self.schema}.manual_monitor_enrollments AS enrollment
                JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
                WHERE enrollment.source_event_id=$1
                  AND batch.origin_kind='MANUAL_MONITOR'
                  AND batch.official_stream_id=$2 AND batch.lineage_id=$3
                  AND enrollment.official_stream_id=$2 AND enrollment.lineage_id=$3
                """,
                source_event_id,
                official_stream_id,
                lineage_id,
            )
        if row is None:
            return None
        record = _manual_enrollment_from_row(row)
        for value, field_name in (
            (record.source_semantic_content_hash, "source_semantic_content_hash"),
            (record.source_payload_hash, "source_payload_hash"),
            (record.calendar_evidence_hash, "calendar_evidence_hash"),
        ):
            try:
                _require_sha256(value, field_name)
            except ValueError as exc:
                raise V20SemanticConflict(
                    f"persisted manual monitor {field_name} is invalid"
                ) from exc
        if sha256_json(record.semantic) != row["enrollment_semantic_hash"]:
            raise V20SemanticConflict("persisted manual monitor enrollment hash mismatch")
        if not record.signal_date < record.d1 < record.d2:
            raise V20SemanticConflict("persisted manual monitor dates are invalid")
        return record

    async def enroll_manual_monitor(self, commit: ManualMonitorEnrollmentCommit) -> bool:
        """Atomically enroll a sealed retrospective PASS in the live exit engine.

        This deliberately creates no decision slot, entry decision, official-state
        revision, shadow batch, holding, order, or fill.  Its model batch has an
        explicit ``MANUAL_MONITOR`` origin and is authorized only by the sealed
        probe event named by ``source_event_id``.
        """

        _validate_manual_monitor_commit(commit)
        fingerprint = _manual_monitor_enrollment_fingerprint(commit)
        batch = commit.model_batch
        async with self.pool.acquire() as connection:
            # READ COMMITTED plus the durable source-row lock gives same-source
            # requests a fresh snapshot after waiting, so retries converge on the
            # enrollment row instead of surfacing a uniqueness race.
            async with connection.transaction(isolation="read_committed"):
                source = await connection.fetchrow(
                    f"""
                    SELECT event_id,event_type,route_id,official_stream_id,lineage_id,
                           semantic_content_hash,semantic_json,payload_hash,payload_json,
                           seal_status
                    FROM {self.schema}.outbox_events
                    WHERE event_id=$1
                    FOR UPDATE
                    """,
                    commit.source_event_id,
                )
                if source is None:
                    raise V20RepositoryError("manual monitor source event does not exist")
                source_semantic = _json_value(source["semantic_json"])
                source_payload = _optional_json_value(source["payload_json"])
                if (
                    source["event_type"] != "DATA_ALERT"
                    or source["route_id"] != commit.route_id
                    or source["official_stream_id"] != commit.official_stream_id
                    or source["lineage_id"] != commit.lineage_id
                    or source["seal_status"] != "SEALED"
                    or source_payload is None
                    or source["semantic_content_hash"] != commit.source_semantic_content_hash
                    or source["payload_hash"] != commit.source_payload_hash
                    or sha256_json(source_semantic) != commit.source_semantic_content_hash
                    or sha256_json(source_payload) != commit.source_payload_hash
                ):
                    raise V20SemanticConflict(
                        "manual monitor source is not the expected sealed event in scope"
                    )

                if not isinstance(source_semantic, Mapping):
                    raise V20SemanticConflict("manual monitor source semantic is malformed")
                entry_render = source_semantic.get("entry_render_semantic")
                symbols = source_semantic.get("symbols")
                source_multiplier = source_semantic.get("final_multiplier")
                if (
                    source_semantic.get("event_id") != commit.source_event_id
                    or source_semantic.get("schema_version") != V20_DATA_ALERT_SEMANTIC_SCHEMA
                    or source_semantic.get("feishu_formatter_profile")
                    != V20_FEISHU_FORMATTER_PROFILE
                    or source_semantic.get("alert_code") != "MANUAL_0939_CHAIN_PROBE_RESULT"
                    or source_semantic.get("probe_profile")
                    != "CURRENT_DEPLOYED_CODE_EXACT_0939_ENTRY_RENDER_V2"
                    or source_semantic.get("probe_result") != "PASS"
                    or source_semantic.get("current_version_recomputed") is not True
                    or source_semantic.get("replay_reused") is not False
                    or source_semantic.get("visible_message_mode") != "MANUAL_OPERATOR_RENDER"
                    or source_semantic.get("strategy_version") != commit.strategy_version
                    or source_semantic.get("config_hash") != commit.source_config_hash
                    or source_semantic.get("state_semantics_hash") != commit.state_semantics_hash
                    or source_semantic.get("official_stream_id") != commit.official_stream_id
                    or source_semantic.get("state_lineage_id") != commit.lineage_id
                    or source_semantic.get("official_entry_action") != "INPUT_INVALID"
                    or source_semantic.get("official_entry_event_id")
                    != commit.official_entry_event_id
                    or source_semantic.get("official_entry_event_id_before")
                    != commit.official_entry_event_id
                    or source_semantic.get("official_entry_event_id_after")
                    != commit.official_entry_event_id
                    or source_semantic.get("v20_action") != "ENTER"
                    or source_semantic.get("replay_action") != "ENTER"
                    or source_semantic.get("official_state_changed") is not False
                    or source_semantic.get("orders_changed") is not False
                    or source_semantic.get("non_actionable") is not True
                    or source_semantic.get("retrospective_expired") is not True
                    or source_semantic.get("event_trade_date") != commit.signal_date.isoformat()
                    or not isinstance(entry_render, Mapping)
                    or entry_render.get("schema_version") != V20_ENTRY_SEMANTIC_SCHEMA
                    or entry_render.get("feishu_formatter_profile") != V20_FEISHU_FORMATTER_PROFILE
                    or entry_render.get("action") != "ENTER"
                    or entry_render.get("trade_date") != commit.signal_date.isoformat()
                    or entry_render.get("final_multiplier") != source_multiplier
                    or entry_render.get("strategy_version") != commit.strategy_version
                    or entry_render.get("config_hash") != commit.source_config_hash
                    or entry_render.get("state_semantics_hash") != commit.state_semantics_hash
                    or not isinstance(symbols, list)
                    or entry_render.get("symbols") != symbols
                    or isinstance(source_multiplier, bool)
                    or not isinstance(source_multiplier, (int, float))
                    or not math.isfinite(float(source_multiplier))
                    or not math.isclose(
                        float(source_multiplier), batch.multiplier, rel_tol=0.0, abs_tol=1e-12
                    )
                    or entry_render.get("reference_profile_id") != batch.reference_profile_id
                ):
                    raise V20SemanticConflict(
                        "manual monitor source is not an eligible current ENTER probe"
                    )

                normalized_source_symbols: list[tuple[int, str, str]] = []
                for item in symbols:
                    if not isinstance(item, Mapping):
                        raise V20SemanticConflict("manual monitor source symbols are malformed")
                    rank = item.get("rank")
                    code = item.get("code")
                    name = item.get("name")
                    if (
                        isinstance(rank, bool)
                        or not isinstance(rank, int)
                        or rank <= 0
                        or not isinstance(code, str)
                        or re.fullmatch(r"\d{6}", code) is None
                        or not isinstance(name, str)
                        or not name
                    ):
                        raise V20SemanticConflict("manual monitor source symbols are malformed")
                    normalized_source_symbols.append((rank, code, name))
                expected_symbols = sorted(normalized_source_symbols)
                actual_symbols = sorted((leg.rank, leg.code, leg.stock_name) for leg in batch.legs)
                if (
                    len(expected_symbols) != len(set(expected_symbols))
                    or expected_symbols != actual_symbols
                ):
                    raise V20SemanticConflict(
                        "manual monitor model legs do not exactly match the sealed source tickets"
                    )

                official_entry = await connection.fetchrow(
                    f"""
                    SELECT official.event_id,official.event_type,official.route_id,
                           official.official_stream_id,official.lineage_id,
                           official.seal_status,decision.action,
                           slot.trade_date,slot.slot_status,
                           slot.official_stream_id AS slot_official_stream_id,
                           slot.lineage_id AS slot_lineage_id
                    FROM {self.schema}.outbox_events AS official
                    JOIN {self.schema}.entry_decisions AS decision
                      ON decision.event_id=official.event_id
                    JOIN {self.schema}.decision_slots AS slot
                      ON slot.slot_id=decision.slot_id
                    WHERE official.event_id=$1
                    FOR UPDATE OF official,decision,slot
                    """,
                    commit.official_entry_event_id,
                )
                if (
                    official_entry is None
                    or official_entry["event_id"] != commit.official_entry_event_id
                    or official_entry["event_type"] != "ENTRY_DECISION"
                    or official_entry["route_id"] != commit.route_id
                    or official_entry["official_stream_id"] != commit.official_stream_id
                    or official_entry["lineage_id"] != commit.lineage_id
                    or official_entry["seal_status"] != "SEALED"
                    or official_entry["action"] != "INPUT_INVALID"
                    or official_entry["trade_date"] != commit.signal_date
                    or official_entry["slot_status"] != "FAILED"
                    or official_entry["slot_official_stream_id"] != commit.official_stream_id
                    or official_entry["slot_lineage_id"] != commit.lineage_id
                ):
                    raise V20SemanticConflict(
                        "manual monitor source is not bound to the sealed failed official slot"
                    )

                existing = await connection.fetchrow(
                    f"""
                    SELECT * FROM {self.schema}.manual_monitor_enrollments
                    WHERE source_event_id=$1 OR enrollment_id=$2 OR model_batch_id=$3
                       OR (official_stream_id=$4 AND lineage_id=$5 AND request_id=$6)
                       OR (official_stream_id=$4 AND lineage_id=$5
                           AND official_entry_event_id=$7)
                    FOR UPDATE
                    """,
                    commit.source_event_id,
                    commit.enrollment_id,
                    batch.model_batch_id,
                    commit.official_stream_id,
                    commit.lineage_id,
                    commit.request_id,
                    commit.official_entry_event_id,
                )
                if existing is not None:
                    if (
                        existing["source_event_id"] == commit.source_event_id
                        and existing["enrollment_id"] == commit.enrollment_id
                        and existing["model_batch_id"] == batch.model_batch_id
                        and existing["enrollment_fingerprint"] == fingerprint
                    ):
                        return False
                    raise V20SemanticConflict(
                        "manual monitor enrollment/source/model-batch ID collision"
                    )

                cutoff_open = await connection.fetchval(
                    "SELECT clock_timestamp() < $1::timestamptz",
                    commit.activation_cutoff_ts,
                )
                if cutoff_open is not True:
                    raise V20StateConflict(
                        "manual monitor enrollment is closed at D1 09:30 Asia/Shanghai"
                    )

                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.model_batches
                        (model_batch_id,decision_id,origin_kind,source_event_id,
                         official_stream_id,lineage_id,signal_date,multiplier,
                         evaluation_only,reference_profile_id)
                    VALUES ($1,NULL,'MANUAL_MONITOR',$2,$3,$4,$5,$6,FALSE,$7)
                    """,
                    batch.model_batch_id,
                    commit.source_event_id,
                    commit.official_stream_id,
                    commit.lineage_id,
                    commit.signal_date,
                    batch.multiplier,
                    batch.reference_profile_id,
                )
                for leg in batch.legs:
                    await connection.execute(
                        f"""
                        INSERT INTO {self.schema}.model_legs
                            (model_leg_id,model_batch_id,code,stock_name,rank,
                             relative_weight,d1,d2)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        """,
                        leg.model_leg_id,
                        batch.model_batch_id,
                        leg.code,
                        leg.stock_name,
                        leg.rank,
                        leg.relative_weight,
                        leg.d1,
                        leg.d2,
                    )
                enrollment_insert = await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.manual_monitor_enrollments
                        (enrollment_id,source_event_id,official_entry_event_id,
                         model_batch_id,request_id,
                         official_stream_id,lineage_id,signal_date,d1,d2,activation_cutoff_ts,
                         source_semantic_content_hash,source_payload_hash,
                         calendar_evidence_hash,enrollment_semantic_hash,
                         enrollment_fingerprint,enrollment_json)
                    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17::jsonb
                    WHERE clock_timestamp() < $11::timestamptz
                    """,
                    commit.enrollment_id,
                    commit.source_event_id,
                    commit.official_entry_event_id,
                    batch.model_batch_id,
                    commit.request_id,
                    commit.official_stream_id,
                    commit.lineage_id,
                    commit.signal_date,
                    commit.d1,
                    commit.d2,
                    commit.activation_cutoff_ts,
                    commit.source_semantic_content_hash,
                    commit.source_payload_hash,
                    commit.calendar_evidence_hash,
                    commit.enrollment_semantic_hash,
                    fingerprint,
                    canonical_json(commit.enrollment_semantic),
                )
                if enrollment_insert != "INSERT 0 1":
                    raise V20StateConflict(
                        "manual monitor enrollment crossed the D1 09:30 database cutoff"
                    )
                return True

    async def commit_entry(self, commit: EntryCommit) -> None:
        _require_outbox_scope(commit.route_id, commit.official_stream_id, commit.lineage_id)
        allowed_actions = {"ENTER", "BLOCK", "NO_SIGNAL", "INPUT_INVALID"}
        if commit.action not in allowed_actions:
            raise ValueError(f"unsupported entry action: {commit.action!r}")
        if not math.isfinite(commit.final_multiplier) or not 0 <= commit.final_multiplier <= 1:
            raise ValueError("final_multiplier must be between 0 and 1")
        if commit.action == "ENTER" and commit.final_multiplier <= 0:
            raise ValueError("ENTER requires a positive final_multiplier")
        if commit.action != "ENTER" and commit.final_multiplier != 0:
            raise ValueError("non-ENTER actions require final_multiplier=0")
        if commit.action_expiry_ts is None:
            raise ValueError("entry action_expiry_ts is required")
        _require_aware(commit.action_expiry_ts, "action_expiry_ts")
        local_expiry = commit.action_expiry_ts.astimezone(BEIJING_TZ)
        if local_expiry.date() != commit.trade_date:
            raise ValueError("entry action_expiry_ts must belong to its trade date")
        if local_expiry.timetz().replace(tzinfo=None) != _ENTRY_NORMAL_DEADLINE_WALL:
            raise ValueError("entry action_expiry_ts must be the frozen 09:40 deadline")
        if commit.action == "INPUT_INVALID":
            if commit.invalid_commit_not_before_ts is None:
                raise ValueError("INPUT_INVALID requires an explicit commit-not-before clock")
            _require_aware(
                commit.invalid_commit_not_before_ts,
                "invalid_commit_not_before_ts",
            )
            local_not_before = commit.invalid_commit_not_before_ts.astimezone(BEIJING_TZ)
            if local_not_before.date() != commit.trade_date:
                raise ValueError("invalid_commit_not_before_ts must belong to its trade date")
            if local_not_before.timetz().replace(tzinfo=None) not in {
                _ENTRY_NORMAL_DEADLINE_WALL,
                _ENTRY_FINALIZATION_WALL,
            }:
                raise ValueError(
                    "INPUT_INVALID commit clock must be the 09:40 late-candidate "
                    "or 09:45 finalization boundary"
                )
        elif commit.invalid_commit_not_before_ts is not None:
            raise ValueError("normal entry actions cannot carry an invalid commit clock")
        _require_sha256(commit.config_hash, "config_hash")
        _require_sha256(commit.expected_state_hash, "expected_state_hash")
        _require_sha256(commit.next_state_hash, "next_state_hash")
        _require_sha256(commit.snapshot_hash, "snapshot_hash")
        _require_sha256(commit.semantic_content_hash, "semantic_content_hash")
        if sha256_json(commit.semantic) != commit.semantic_content_hash:
            raise V20SemanticConflict("entry semantic_content_hash mismatch")
        if isinstance(commit.expected_state_revision, bool) or not isinstance(
            commit.expected_state_revision, int
        ):
            raise V20SemanticConflict("invalid expected state revision type")
        if sha256_json(commit.next_state) != commit.next_state_hash:
            raise V20SemanticConflict("next_state_hash mismatch")
        if not isinstance(commit.next_state, Mapping):
            raise V20SemanticConflict("next official state is not an object")
        _validate_official_state_contract(
            commit.next_state,
            revision=commit.expected_state_revision + 1,
        )
        if sha256_json(commit.snapshot) != commit.snapshot_hash:
            raise V20SemanticConflict("snapshot_hash mismatch")
        if commit.model_batch is None:
            if commit.action == "ENTER":
                raise ValueError("ENTER requires a model batch")
        else:
            batch = commit.model_batch
            if commit.action != "ENTER":
                raise ValueError("only ENTER may create a model batch")
            if not math.isfinite(batch.multiplier) or batch.multiplier != commit.final_multiplier:
                raise ValueError("model batch multiplier must equal final_multiplier")
            if not batch.reference_profile_id:
                raise ValueError("model batch reference_profile_id cannot be empty")
            if not batch.legs:
                raise ValueError("model batch must contain at least one leg")
            seen_codes: set[str] = set()
            seen_ranks: set[int] = set()
            seen_leg_ids: set[str] = set()
            for leg in batch.legs:
                if not re.fullmatch(r"\d{6}", leg.code):
                    raise ValueError(f"invalid model leg code: {leg.code!r}")
                if leg.rank <= 0 or leg.rank in seen_ranks:
                    raise ValueError("model leg ranks must be unique positive integers")
                if leg.code in seen_codes or leg.model_leg_id in seen_leg_ids:
                    raise ValueError("model leg codes and IDs must be unique within a batch")
                if not math.isfinite(leg.relative_weight) or not 0 < leg.relative_weight <= 1:
                    raise ValueError("model leg relative_weight must be finite and in (0, 1]")
                if leg.d2 <= leg.d1 or leg.d1 <= commit.trade_date:
                    raise ValueError("model leg dates must satisfy trade_date < d1 < d2")
                seen_codes.add(leg.code)
                seen_ranks.add(leg.rank)
                seen_leg_ids.add(leg.model_leg_id)
            if not math.isclose(
                sum(leg.relative_weight for leg in batch.legs),
                batch.multiplier,
                rel_tol=0.0,
                abs_tol=1e-9,
            ):
                raise ValueError("model leg relative weights must sum to model batch multiplier")
        shadow_kinds: set[str] = set()
        shadow_ids: set[str] = set()
        for shadow in commit.shadow_batches:
            if shadow.kind not in {"HEALTH", "ROLLING7"}:
                raise ValueError(f"unsupported shadow batch kind: {shadow.kind!r}")
            if shadow.kind in shadow_kinds or shadow.batch_id in shadow_ids:
                raise ValueError("shadow batch kinds and IDs must be unique per decision")
            if shadow.signal_date != commit.trade_date or shadow.t2_date <= shadow.signal_date:
                raise ValueError("shadow batch dates are inconsistent with the entry trade date")
            shadow_kinds.add(shadow.kind)
            shadow_ids.add(shadow.batch_id)
        if commit.action in {"NO_SIGNAL", "INPUT_INVALID"} and commit.shadow_batches:
            raise ValueError(f"{commit.action} cannot create shadow batches")

        commit_fingerprint = _entry_commit_fingerprint(commit)

        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                existing = await connection.fetchrow(
                    f"SELECT decision_id,event_id,commit_fingerprint FROM "
                    f"{self.schema}.entry_decisions WHERE decision_id=$1 OR event_id=$2",
                    commit.decision_id,
                    commit.event_id,
                )
                if existing is not None:
                    if (
                        existing["decision_id"] == commit.decision_id
                        and existing["event_id"] == commit.event_id
                        and existing["commit_fingerprint"] == commit_fingerprint
                    ):
                        return
                    raise V20SemanticConflict("decision/event ID already has different semantics")

                registered_config = await connection.fetchrow(
                    f"""
                    SELECT config_hash,strategy_version,effective_trade_date
                    FROM {self.schema}.runtime_configs
                    WHERE config_id=$1
                    """,
                    commit.config_id,
                )
                if registered_config is None:
                    raise V20StateConflict(f"unregistered config_id {commit.config_id!r}")
                if (
                    registered_config["config_hash"] != commit.config_hash
                    or registered_config["strategy_version"] != commit.strategy_version
                    or registered_config["effective_trade_date"] > commit.trade_date
                ):
                    raise V20SemanticConflict("entry config does not match runtime registry")

                state = await connection.fetchrow(
                    f"SELECT revision,state_hash FROM {self.schema}.official_state "
                    "WHERE lineage_id=$1 FOR UPDATE",
                    commit.lineage_id,
                )
                if state is None:
                    raise V20StateConflict("state lineage does not exist")
                if (
                    int(state["revision"]) != commit.expected_state_revision
                    or state["state_hash"] != commit.expected_state_hash
                ):
                    raise V20StateConflict("stale state revision/hash")

                slot = await connection.fetchrow(
                    f"""
                    INSERT INTO {self.schema}.decision_slots
                        (official_stream_id,trade_date,slot_id,strategy_version,
                         config_id,config_hash,lineage_id,slot_status)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,'OPEN')
                    ON CONFLICT (official_stream_id,trade_date) DO UPDATE
                        SET official_stream_id=EXCLUDED.official_stream_id
                    RETURNING slot_id,strategy_version,config_id,config_hash,lineage_id,
                              slot_status,slot_revision
                    """,
                    commit.official_stream_id,
                    commit.trade_date,
                    commit.slot_id,
                    commit.strategy_version,
                    commit.config_id,
                    commit.config_hash,
                    commit.lineage_id,
                )
                if slot is None:
                    raise V20StateConflict("failed to bind decision slot")
                binding = (
                    slot["slot_id"],
                    slot["strategy_version"],
                    slot["config_id"],
                    slot["config_hash"],
                    slot["lineage_id"],
                )
                expected_binding = (
                    commit.slot_id,
                    commit.strategy_version,
                    commit.config_id,
                    commit.config_hash,
                    commit.lineage_id,
                )
                if binding != expected_binding or slot["slot_status"] != "OPEN":
                    raise V20StateConflict("decision slot is bound or terminal")

                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.input_snapshots
                        (snapshot_id,snapshot_type,trade_date,snapshot_hash,snapshot_json)
                    VALUES ($1,'V16',$2,$3,$4::jsonb)
                    ON CONFLICT (snapshot_id) DO NOTHING
                    """,
                    commit.snapshot_id,
                    commit.trade_date,
                    commit.snapshot_hash,
                    canonical_json(commit.snapshot),
                )
                snapshot = await connection.fetchrow(
                    f"SELECT snapshot_hash,snapshot_json FROM {self.schema}.input_snapshots "
                    "WHERE snapshot_id=$1",
                    commit.snapshot_id,
                )
                if (
                    snapshot is None
                    or snapshot["snapshot_hash"] != commit.snapshot_hash
                    or sha256_json(_json_value(snapshot["snapshot_json"])) != commit.snapshot_hash
                ):
                    raise V20SemanticConflict("snapshot_id collision")

                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.entry_decisions
                        (decision_id,slot_id,event_id,snapshot_id,action,final_multiplier,
                         semantic_content_hash,commit_fingerprint,semantic_json)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
                    """,
                    commit.decision_id,
                    commit.slot_id,
                    commit.event_id,
                    commit.snapshot_id,
                    commit.action,
                    commit.final_multiplier,
                    commit.semantic_content_hash,
                    commit_fingerprint,
                    canonical_json(commit.semantic),
                )

                for shadow_batch in commit.shadow_batches:
                    await connection.execute(
                        f"""
                        INSERT INTO {self.schema}.shadow_batches
                            (batch_id,decision_id,official_stream_id,lineage_id,
                             kind,signal_date,t2_date,batch_json)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
                        """,
                        shadow_batch.batch_id,
                        commit.decision_id,
                        commit.official_stream_id,
                        commit.lineage_id,
                        shadow_batch.kind,
                        shadow_batch.signal_date,
                        shadow_batch.t2_date,
                        canonical_json(shadow_batch.payload),
                    )

                if commit.model_batch is not None:
                    model_batch = commit.model_batch
                    await connection.execute(
                        f"""
                        INSERT INTO {self.schema}.model_batches
                            (model_batch_id,decision_id,origin_kind,source_event_id,
                             official_stream_id,lineage_id,signal_date,multiplier,
                             evaluation_only,reference_profile_id)
                        VALUES ($1,$2,'OFFICIAL_ENTRY',$3,$4,$5,$6,$7,$8,$9)
                        """,
                        model_batch.model_batch_id,
                        commit.decision_id,
                        commit.event_id,
                        commit.official_stream_id,
                        commit.lineage_id,
                        commit.trade_date,
                        model_batch.multiplier,
                        model_batch.evaluation_only,
                        model_batch.reference_profile_id,
                    )
                    for leg in model_batch.legs:
                        await connection.execute(
                            f"""
                            INSERT INTO {self.schema}.model_legs
                                (model_leg_id,model_batch_id,code,stock_name,rank,
                                 relative_weight,d1,d2)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                            """,
                            leg.model_leg_id,
                            model_batch.model_batch_id,
                            leg.code,
                            leg.stock_name,
                            leg.rank,
                            leg.relative_weight,
                            leg.d1,
                            leg.d2,
                        )

                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.outbox_events
                        (event_id,event_type,route_id,official_stream_id,lineage_id,
                         semantic_content_hash,semantic_json,action_expiry_ts)
                    VALUES ($1,'ENTRY_DECISION',$2,$3,$4,$5,$6::jsonb,$7)
                    """,
                    commit.event_id,
                    commit.route_id,
                    commit.official_stream_id,
                    commit.lineage_id,
                    commit.semantic_content_hash,
                    canonical_json(commit.semantic),
                    commit.action_expiry_ts,
                )
                updated = await connection.execute(
                    f"""
                    UPDATE {self.schema}.official_state
                    SET revision=revision+1,state_hash=$1,state_json=$2::jsonb,
                        updated_at=clock_timestamp()
                    WHERE lineage_id=$3 AND revision=$4 AND state_hash=$5
                    """,
                    commit.next_state_hash,
                    canonical_json(commit.next_state),
                    commit.lineage_id,
                    commit.expected_state_revision,
                    commit.expected_state_hash,
                )
                if updated != "UPDATE 1":
                    raise V20StateConflict("state CAS lost")
                terminal_status = "FAILED" if commit.action == "INPUT_INVALID" else "COMPLETED"
                finalized = await connection.execute(
                    f"""
                    WITH terminal_receipt AS MATERIALIZED (
                        SELECT clock_timestamp() AS terminal_at
                    )
                    UPDATE {self.schema}.decision_slots AS slot
                    SET slot_status=$1,slot_revision=slot_revision+1,
                        terminal_event_id=$2,terminal_decision_id=$3,
                        completed_at=terminal_receipt.terminal_at
                    FROM terminal_receipt
                    WHERE slot.slot_id=$4 AND slot.slot_status='OPEN'
                      AND slot.slot_revision=$5
                      AND (
                        ($6='INPUT_INVALID' AND $8::timestamptz IS NOT NULL
                            AND terminal_receipt.terminal_at >= $8)
                        OR ($6<>'INPUT_INVALID' AND terminal_receipt.terminal_at < $7)
                      )
                    """,
                    terminal_status,
                    commit.event_id,
                    commit.decision_id,
                    commit.slot_id,
                    int(slot["slot_revision"]),
                    commit.action,
                    commit.action_expiry_ts,
                    commit.invalid_commit_not_before_ts,
                )
                if finalized != "UPDATE 1":
                    if commit.action != "INPUT_INVALID":
                        raise V20EntryDeadlineExceeded(
                            "database clock reached the normal-entry deadline"
                        )
                    raise V20StateConflict("slot CAS/deadline guard rejected entry commit")

    async def seal_event(
        self,
        event_id: str,
        payload_builder: Callable[[OutboxRecord, datetime, int, bool], Mapping[str, Any]],
    ) -> OutboxRecord:
        """Seal an outbox event using a clock sampled after core commit visibility."""
        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                row = await connection.fetchrow(
                    f"SELECT * FROM {self.schema}.outbox_events WHERE event_id=$1 FOR UPDATE",
                    event_id,
                )
                if row is None:
                    raise V20RepositoryError(f"unknown outbox event {event_id!r}")
                current = self._outbox_from_row(row)
                self._verify_outbox_integrity(current)
                if row["seal_status"] == "SEALED":
                    return current
                receipt = await connection.fetchrow(
                    f"SELECT clock_timestamp() AS durable_at, "
                    f"nextval('{self.schema}.commit_marker_seq') AS marker"
                )
                durable_at = receipt["durable_at"]
                marker = int(receipt["marker"])
                on_time = current.action_expiry_ts is None or durable_at < current.action_expiry_ts
                payload = dict(payload_builder(current, durable_at, marker, on_time))
                if current.event_type == "ENTRY_DECISION":
                    expected_timeliness = "ON_TIME" if on_time else "LATE"
                    if payload.get("timeliness_status") != expected_timeliness:
                        raise V20SemanticConflict(
                            "entry payload timeliness_status disagrees with durable seal clock"
                        )
                payload_hash = sha256_json(payload)
                sealed = await connection.execute(
                    f"""
                    UPDATE {self.schema}.outbox_events
                    SET payload_json=$1::jsonb,payload_hash=$2,seal_status='SEALED',
                        generated_at=$3,commit_marker=$4,seal_last_error=NULL
                    WHERE event_id=$5 AND seal_status='PENDING'
                    """,
                    canonical_json(payload),
                    payload_hash,
                    durable_at,
                    marker,
                    event_id,
                )
                if sealed != "UPDATE 1":
                    raise V20StateConflict("outbox seal CAS lost")
                return OutboxRecord(
                    event_id=current.event_id,
                    event_type=current.event_type,
                    route_id=current.route_id,
                    official_stream_id=current.official_stream_id,
                    lineage_id=current.lineage_id,
                    semantic=current.semantic,
                    semantic_content_hash=current.semantic_content_hash,
                    payload=payload,
                    payload_hash=payload_hash,
                    generated_at=durable_at,
                    commit_marker=marker,
                    action_expiry_ts=current.action_expiry_ts,
                    delivery_status=current.delivery_status,
                    attempt_count=current.attempt_count,
                )

    async def get_outbox_event(
        self,
        event_id: str,
        *,
        route_id: str,
        official_stream_id: str,
        lineage_id: str,
    ) -> OutboxRecord | None:
        """Read one immutable event only inside the caller's exact public scope."""

        if not event_id:
            raise ValueError("event_id cannot be empty")
        _require_outbox_scope(route_id, official_stream_id, lineage_id)
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                f"SELECT * FROM {self.schema}.outbox_events WHERE event_id=$1",
                event_id,
            )
        if row is None:
            return None
        record = self._outbox_from_row(row)
        self._verify_outbox_integrity(record)
        if (
            record.route_id,
            record.official_stream_id,
            record.lineage_id,
        ) != (route_id, official_stream_id, lineage_id):
            raise V20SemanticConflict("outbox event_id belongs to another V20 scope")
        return record

    async def enqueue_alert(
        self,
        event_id: str,
        route_id: str,
        semantic: Mapping[str, Any],
        semantic_hash: str,
        *,
        official_stream_id: str,
        lineage_id: str,
    ) -> bool:
        """Persist an immutable DATA_ALERT outbox skeleton without recomputation."""
        if not event_id:
            raise ValueError("event_id cannot be empty")
        _require_outbox_scope(route_id, official_stream_id, lineage_id)
        _require_sha256(semantic_hash, "semantic_hash")
        if sha256_json(semantic) != semantic_hash:
            raise V20SemanticConflict("alert semantic hash mismatch")
        encoded = canonical_json(semantic)
        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                existing = await connection.fetchrow(
                    f"SELECT event_type,route_id,official_stream_id,lineage_id,"
                    f"semantic_content_hash,semantic_json "
                    f"FROM {self.schema}.outbox_events WHERE event_id=$1 FOR UPDATE",
                    event_id,
                )
                if existing is not None:
                    if (
                        existing["event_type"] == "DATA_ALERT"
                        and existing["route_id"] == route_id
                        and existing["official_stream_id"] == official_stream_id
                        and existing["lineage_id"] == lineage_id
                        and existing["semantic_content_hash"] == semantic_hash
                        and _json_value(existing["semantic_json"]) == semantic
                    ):
                        return False
                    raise V20SemanticConflict("alert event_id already has different semantics")
                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.outbox_events
                        (event_id,event_type,route_id,official_stream_id,lineage_id,
                         semantic_content_hash,semantic_json)
                    VALUES ($1,'DATA_ALERT',$2,$3,$4,$5,$6::jsonb)
                    """,
                    event_id,
                    route_id,
                    official_stream_id,
                    lineage_id,
                    semantic_hash,
                    encoded,
                )
                return True

    async def list_unsealed_outbox_event_ids(
        self,
        *,
        route_id: str,
        official_stream_id: str,
        lineage_id: str,
        limit: int = 100,
        after_event_id: str | None = None,
    ) -> tuple[str, ...]:
        """Return durable core events whose post-commit payload still needs sealing.

        Core commit and sealing are deliberately separate transactions.  A
        process crash between them must therefore be recoverable without
        reconstructing the decision or exit intent.
        """
        _require_outbox_scope(route_id, official_stream_id, lineage_id)
        if limit < 1 or limit > 1_000:
            raise ValueError("limit must be between 1 and 1000")
        if after_event_id is not None and not after_event_id:
            raise ValueError("after_event_id cannot be empty")
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                WITH candidates AS (
                    SELECT event_id,
                           CASE
                             WHEN event_type='ENTRY_DECISION'
                                  AND action_expiry_ts > clock_timestamp() THEN 0
                             WHEN event_type='EXIT_SIGNAL'
                                  AND semantic_json->>'delivery_priority_class'='LIVE_EXIT' THEN 1
                             WHEN event_type='DATA_ALERT'
                                  AND semantic_json->>'delivery_priority_class'=
                                      'RUNTIME_CRITICAL_ALERT' THEN 2
                             WHEN event_type='EXIT_REMINDER' THEN 3
                             WHEN event_type='EXIT_SIGNAL' THEN 4
                             WHEN event_type='ENTRY_DECISION' THEN 6
                             ELSE 5
                           END AS delivery_priority,
                           seal_attempt_count AS prior_seal_attempt_count,
                           created_at AS candidate_created_at
                    FROM {self.schema}.outbox_events
                    WHERE seal_status='PENDING'
                      AND route_id=$1 AND official_stream_id=$2 AND lineage_id=$3
                      AND ($5::text IS NULL OR event_id > $5)
                    ORDER BY delivery_priority,seal_attempt_count,created_at DESC,event_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT $4
                ), updated AS (
                UPDATE {self.schema}.outbox_events AS outbox
                SET seal_attempt_count=outbox.seal_attempt_count+1,
                    seal_last_attempt_at=clock_timestamp()
                FROM candidates
                WHERE outbox.event_id=candidates.event_id
                RETURNING outbox.event_id
                )
                SELECT updated.event_id
                FROM updated
                JOIN candidates USING (event_id)
                ORDER BY candidates.delivery_priority,
                         candidates.prior_seal_attempt_count,
                         candidates.candidate_created_at DESC,
                         updated.event_id
                """,
                route_id,
                official_stream_id,
                lineage_id,
                limit,
                after_event_id,
            )
        return tuple(str(row["event_id"]) for row in rows)

    async def record_outbox_seal_error(
        self,
        event_id: str,
        error: str,
        *,
        route_id: str,
        official_stream_id: str,
        lineage_id: str,
    ) -> bool:
        """Attach diagnostics to one failed seal attempt without blocking siblings."""

        if not event_id or not error:
            raise ValueError("event_id and seal error cannot be empty")
        _require_outbox_scope(route_id, official_stream_id, lineage_id)
        async with self.pool.acquire() as connection:
            updated = await connection.execute(
                f"""
                UPDATE {self.schema}.outbox_events
                SET seal_last_error=$1
                WHERE event_id=$2 AND seal_status='PENDING'
                  AND route_id=$3 AND official_stream_id=$4 AND lineage_id=$5
                """,
                error[:4_000],
                event_id,
                route_id,
                official_stream_id,
                lineage_id,
            )
        return updated == "UPDATE 1"

    async def get_outbox_health(
        self,
        *,
        route_id: str,
        official_stream_id: str,
        lineage_id: str,
    ) -> Mapping[str, Any]:
        """Return non-secret delivery backlog and unknown-outcome counters."""
        _require_outbox_scope(route_id, official_stream_id, lineage_id)
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                f"""
                SELECT
                    count(*) FILTER (WHERE seal_status='PENDING') AS unsealed_n,
                    count(*) FILTER (
                        WHERE seal_status='SEALED' AND delivery_status='PENDING'
                    ) AS pending_delivery_n,
                    count(*) FILTER (WHERE delivery_status='LEASED') AS leased_n,
                    count(*) FILTER (
                        WHERE delivery_status='DELIVERY_UNKNOWN'
                          AND lease_owner IS NOT NULL
                          AND lease_until >= clock_timestamp()
                          AND EXISTS (
                              SELECT 1 FROM {self.schema}.delivery_attempts AS attempt
                              WHERE attempt.event_id=outbox.event_id
                                AND attempt.phase='STARTED'
                                AND attempt.worker_id=outbox.lease_owner
                          )
                    ) AS dispatching_n,
                    count(*) FILTER (
                        WHERE delivery_status='DELIVERY_UNKNOWN'
                          AND EXISTS (
                              SELECT 1 FROM {self.schema}.delivery_attempts AS attempt
                              WHERE attempt.event_id=outbox.event_id
                                AND attempt.phase='STARTED'
                          )
                          AND NOT (
                              lease_owner IS NOT NULL
                              AND lease_until >= clock_timestamp()
                              AND EXISTS (
                                  SELECT 1
                                  FROM {self.schema}.delivery_attempts AS attempt
                                  WHERE attempt.event_id=outbox.event_id
                                    AND attempt.phase='STARTED'
                                    AND attempt.worker_id=outbox.lease_owner
                              )
                          )
                    ) AS stale_started_n,
                    count(*) FILTER (
                        WHERE delivery_status='DELIVERY_UNKNOWN'
                          AND NOT EXISTS (
                              SELECT 1 FROM {self.schema}.delivery_attempts AS attempt
                              WHERE attempt.event_id=outbox.event_id
                                AND attempt.phase='STARTED'
                          )
                    ) AS terminal_unknown_n,
                    count(*) FILTER (WHERE delivery_status='DELIVERY_UNKNOWN') AS unknown_n,
                    count(*) FILTER (
                        WHERE seal_status='PENDING' AND seal_last_error IS NOT NULL
                    ) AS seal_error_n,
                    count(*) FILTER (
                        WHERE delivery_status='PENDING' AND last_error IS NOT NULL
                    ) AS pending_delivery_error_n,
                    count(*) FILTER (WHERE delivery_status='DELIVERY_UNKNOWN') AS unknown_error_n,
                    max(seal_attempt_count) FILTER (
                        WHERE seal_status='PENDING'
                    ) AS max_seal_attempt_count,
                    max(attempt_count) FILTER (
                        WHERE delivery_status <> 'SENT' AND last_error IS NOT NULL
                    ) AS max_delivery_attempt_count,
                    max(seal_last_attempt_at) FILTER (
                        WHERE seal_status='PENDING'
                    ) AS last_seal_attempt_at,
                    min(created_at) FILTER (
                        WHERE delivery_status <> 'SENT'
                    ) AS oldest_unsent_at,
                    min(
                        COALESCE(
                            (
                                SELECT min(attempt.attempted_at)
                                FROM {self.schema}.delivery_attempts AS attempt
                                WHERE attempt.event_id=outbox.event_id
                                  AND attempt.phase IN ('STARTED','UNKNOWN')
                            ),
                            outbox.created_at
                        )
                    ) FILTER (WHERE delivery_status='DELIVERY_UNKNOWN') AS oldest_unknown_at,
                    max(delivered_at) AS last_delivered_at
                FROM {self.schema}.outbox_events AS outbox
                WHERE route_id=$1 AND official_stream_id=$2 AND lineage_id=$3
                """,
                route_id,
                official_stream_id,
                lineage_id,
            )
        if row is None:
            raise V20RepositoryError("cannot read V20 outbox health")
        delivery_error_n = int(row["pending_delivery_error_n"] or 0) + int(
            row["unknown_error_n"] or 0
        )
        return {
            "unsealed_n": int(row["unsealed_n"] or 0),
            "pending_delivery_n": int(row["pending_delivery_n"] or 0),
            "leased_n": int(row["leased_n"] or 0),
            "dispatching_n": int(row["dispatching_n"] or 0),
            "stale_started_n": int(row["stale_started_n"] or 0),
            "terminal_unknown_n": int(row["terminal_unknown_n"] or 0),
            "unknown_n": int(row["unknown_n"] or 0),
            "seal_error_n": int(row["seal_error_n"] or 0),
            "delivery_error_n": delivery_error_n,
            "max_seal_attempt_count": int(row["max_seal_attempt_count"] or 0),
            "max_delivery_attempt_count": int(row["max_delivery_attempt_count"] or 0),
            "last_seal_attempt_at": (
                row["last_seal_attempt_at"].isoformat()
                if row["last_seal_attempt_at"] is not None
                else None
            ),
            "oldest_unsent_at": (
                row["oldest_unsent_at"].isoformat() if row["oldest_unsent_at"] is not None else None
            ),
            "oldest_unknown_at": (
                row["oldest_unknown_at"].isoformat()
                if row["oldest_unknown_at"] is not None
                else None
            ),
            "last_delivered_at": (
                row["last_delivered_at"].isoformat()
                if row["last_delivered_at"] is not None
                else None
            ),
        }

    async def lease_outbox(
        self,
        *,
        worker_id: str,
        route_id: str,
        official_stream_id: str,
        lineage_id: str,
        lease_seconds: int = 60,
        limit: int = 20,
    ) -> list[OutboxRecord]:
        _require_outbox_scope(route_id, official_stream_id, lineage_id)
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        if not worker_id:
            raise ValueError("worker_id cannot be empty")
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    f"""
                    WITH lease_clock AS (
                        SELECT clock_timestamp() AS leased_at
                    ), candidates AS (
                        SELECT event_id FROM {self.schema}.outbox_events,lease_clock
                        WHERE seal_status='SEALED'
                          AND route_id=$1 AND official_stream_id=$2 AND lineage_id=$3
                          AND available_at <= clock_timestamp()
                          AND (
                              delivery_status='PENDING'
                              OR (
                                  delivery_status='LEASED'
                                  AND lease_until < clock_timestamp()
                                  AND NOT EXISTS (
                                      SELECT 1
                                      FROM {self.schema}.delivery_attempts AS attempt
                                      WHERE attempt.event_id=outbox_events.event_id
                                        AND attempt.phase='STARTED'
                                  )
                              )
                          )
                        ORDER BY
                          CASE
                            WHEN event_type='ENTRY_DECISION'
                                 AND action_expiry_ts > lease_clock.leased_at THEN 0
                            WHEN event_type='EXIT_SIGNAL'
                                 AND semantic_json->>'delivery_priority_class'='LIVE_EXIT' THEN 1
                            WHEN event_type='DATA_ALERT'
                                 AND semantic_json->>'delivery_priority_class'=
                                     'RUNTIME_CRITICAL_ALERT' THEN 2
                            WHEN event_type='EXIT_REMINDER' THEN 3
                            WHEN event_type='EXIT_SIGNAL' THEN 4
                            WHEN event_type='ENTRY_DECISION' THEN 6
                            ELSE 5
                          END,
                          action_expiry_ts NULLS LAST,created_at,event_id
                        FOR UPDATE SKIP LOCKED
                        LIMIT $4
                    )
                    UPDATE {self.schema}.outbox_events AS outbox
                    SET delivery_status='LEASED',lease_owner=$5,
                        lease_until=clock_timestamp()+($6::double precision * interval '1 second')
                    FROM candidates
                    WHERE outbox.event_id=candidates.event_id
                    RETURNING outbox.*,(SELECT leased_at FROM lease_clock) AS lease_db_ts
                    """,
                    route_id,
                    official_stream_id,
                    lineage_id,
                    limit,
                    worker_id,
                    lease_seconds,
                )
        records = [self._outbox_from_row(row) for row in rows]
        for record in records:
            self._verify_outbox_integrity(record)

        def delivery_priority(record: OutboxRecord) -> tuple[int, datetime, str]:
            leased_at = record.lease_db_ts
            live_entry = (
                record.event_type == "ENTRY_DECISION"
                and record.action_expiry_ts is not None
                and leased_at is not None
                and record.action_expiry_ts > leased_at
            )
            live_exit = (
                record.event_type == "EXIT_SIGNAL"
                and record.semantic.get("delivery_priority_class") == "LIVE_EXIT"
            )
            critical_alert = (
                record.event_type == "DATA_ALERT"
                and record.semantic.get("delivery_priority_class") == "RUNTIME_CRITICAL_ALERT"
            )
            priority = (
                0
                if live_entry
                else 1
                if live_exit
                else 2
                if critical_alert
                else 3
                if record.event_type == "EXIT_REMINDER"
                else 4
                if record.event_type == "EXIT_SIGNAL"
                else 6
                if record.event_type == "ENTRY_DECISION"
                else 5
            )
            return (
                priority,
                record.action_expiry_ts or datetime.max.replace(tzinfo=BEIJING_TZ),
                record.event_id,
            )

        records.sort(key=delivery_priority)
        return records

    async def begin_delivery_attempt(
        self,
        event_id: str,
        *,
        worker_id: str,
        route_id: str,
        official_stream_id: str,
        lineage_id: str,
        action_reserve_seconds: float = 2.0,
        relay_enforced: bool = False,
    ) -> DeliveryAttempt:
        """Atomically cross the outward side-effect boundary before HTTP I/O."""

        _require_outbox_scope(route_id, official_stream_id, lineage_id)
        if not worker_id:
            raise ValueError("worker_id cannot be empty")
        if action_reserve_seconds < 0:
            raise ValueError("action_reserve_seconds cannot be negative")
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    f"""
                    SELECT attempt_count,action_expiry_ts,clock_timestamp() AS db_now
                    FROM {self.schema}.outbox_events
                    WHERE event_id=$1 AND delivery_status='LEASED' AND lease_owner=$2
                      AND route_id=$3 AND official_stream_id=$4 AND lineage_id=$5
                    FOR UPDATE
                    """,
                    event_id,
                    worker_id,
                    route_id,
                    official_stream_id,
                    lineage_id,
                )
                if row is None:
                    raise V20StateConflict("outbox dispatch lease is missing or not owned")

                action_expiry = row["action_expiry_ts"]
                if action_expiry is None:
                    delivery_variant = "PRIMARY"
                elif relay_enforced and row["db_now"] < action_expiry:
                    delivery_variant = "RELAY_ENFORCED"
                elif row["db_now"] + timedelta(seconds=action_reserve_seconds) < action_expiry:
                    delivery_variant = "ACTIONABLE"
                else:
                    delivery_variant = "EXPIRED_NOTICE"
                attempt_number = int(row["attempt_count"]) + 1

                inserted = await connection.fetchrow(
                    f"""
                    INSERT INTO {self.schema}.delivery_attempts
                        (event_id,attempt_number,phase,worker_id,delivery_variant)
                    VALUES ($1,$2,'STARTED',$3,$4)
                    RETURNING attempt_number,delivery_variant
                    """,
                    event_id,
                    attempt_number,
                    worker_id,
                    delivery_variant,
                )
                if inserted is None:
                    raise V20StateConflict("outbox dispatch attempt insert lost")

                updated = await connection.fetchrow(
                    f"""
                    UPDATE {self.schema}.outbox_events
                    SET delivery_status='DELIVERY_UNKNOWN',attempt_count=$1,last_error=NULL
                    WHERE event_id=$2 AND delivery_status='LEASED' AND lease_owner=$3
                      AND route_id=$4 AND official_stream_id=$5 AND lineage_id=$6
                      AND attempt_count=$7
                    RETURNING event_id
                    """,
                    attempt_number,
                    event_id,
                    worker_id,
                    route_id,
                    official_stream_id,
                    lineage_id,
                    int(row["attempt_count"]),
                )
                if updated is None:
                    raise V20StateConflict("outbox dispatch boundary CAS lost")
                return DeliveryAttempt(
                    attempt_number=attempt_number,
                    delivery_variant=delivery_variant,
                )

    async def defer_before_dispatch(
        self,
        event_id: str,
        *,
        worker_id: str,
        route_id: str,
        official_stream_id: str,
        lineage_id: str,
        error: str,
        retry_after_seconds: int = 30,
    ) -> None:
        """Release an un-dispatched lease without creating an attempt."""

        _require_outbox_scope(route_id, official_stream_id, lineage_id)
        if not worker_id:
            raise ValueError("worker_id cannot be empty")
        if retry_after_seconds < 0:
            raise ValueError("retry_after_seconds cannot be negative")
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                updated = await connection.fetchrow(
                    f"""
                    UPDATE {self.schema}.outbox_events
                    SET delivery_status='PENDING',last_error=$1,
                        available_at=clock_timestamp()+(
                            $2::double precision * interval '1 second'
                        ),
                        lease_owner=NULL,lease_until=NULL
                    WHERE event_id=$3 AND delivery_status='LEASED' AND lease_owner=$4
                      AND route_id=$5 AND official_stream_id=$6 AND lineage_id=$7
                    RETURNING event_id
                    """,
                    error,
                    retry_after_seconds,
                    event_id,
                    worker_id,
                    route_id,
                    official_stream_id,
                    lineage_id,
                )
                if updated is None:
                    raise V20StateConflict("outbox pre-dispatch release CAS lost")

    async def complete_delivery(
        self,
        event_id: str,
        *,
        attempt_number: int,
        worker_id: str,
        route_id: str,
        official_stream_id: str,
        lineage_id: str,
        outcome: Literal["DELIVERED", "SAFE_RETRY", "UNKNOWN"],
        error: str | None = None,
        retry_after_seconds: int = 30,
    ) -> None:
        """Finalize exactly one STARTED attempt and its DELIVERY_UNKNOWN event."""

        _require_outbox_scope(route_id, official_stream_id, lineage_id)
        if not worker_id:
            raise ValueError("worker_id cannot be empty")
        if attempt_number < 1:
            raise ValueError("attempt_number must be positive")
        if retry_after_seconds < 0:
            raise ValueError("retry_after_seconds cannot be negative")
        if outcome == "DELIVERED" and error is not None:
            raise ValueError("DELIVERED attempt cannot carry an error")
        if outcome in {"SAFE_RETRY", "UNKNOWN"} and not error:
            raise ValueError("SAFE_RETRY and UNKNOWN attempts require an error")
        if outcome == "UNKNOWN":
            error = error or "delivery outcome unknown"

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                attempt_updated = await connection.fetchrow(
                    f"""
                    UPDATE {self.schema}.delivery_attempts
                    SET succeeded=$3,error_text=$4,phase=$5,completed_at=clock_timestamp()
                    WHERE event_id=$1 AND attempt_number=$6 AND worker_id=$2
                      AND phase='STARTED'
                    RETURNING event_id
                    """,
                    event_id,
                    worker_id,
                    True
                    if outcome == "DELIVERED"
                    else (False if outcome == "SAFE_RETRY" else None),
                    error,
                    outcome,
                    attempt_number,
                )
                if attempt_updated is None:
                    raise V20StateConflict(
                        "outbox delivery attempt is missing, stale, or owned by another worker"
                    )

                if outcome == "DELIVERED":
                    event_updated = await connection.fetchrow(
                        f"""
                        UPDATE {self.schema}.outbox_events
                        SET delivery_status='SENT',last_error=NULL,
                            delivered_at=clock_timestamp(),
                            lease_owner=NULL,lease_until=NULL
                        WHERE event_id=$1 AND delivery_status='DELIVERY_UNKNOWN'
                          AND attempt_count=$2 AND lease_owner=$3
                          AND route_id=$4 AND official_stream_id=$5 AND lineage_id=$6
                        RETURNING event_id
                        """,
                        event_id,
                        attempt_number,
                        worker_id,
                        route_id,
                        official_stream_id,
                        lineage_id,
                    )
                elif outcome == "SAFE_RETRY":
                    event_updated = await connection.fetchrow(
                        f"""
                        UPDATE {self.schema}.outbox_events
                        SET delivery_status='PENDING',last_error=$1,
                            available_at=clock_timestamp()+(
                                $2::double precision * interval '1 second'
                            ),
                            lease_owner=NULL,lease_until=NULL
                        WHERE event_id=$3 AND delivery_status='DELIVERY_UNKNOWN'
                          AND attempt_count=$4 AND lease_owner=$5
                          AND route_id=$6 AND official_stream_id=$7 AND lineage_id=$8
                        RETURNING event_id
                        """,
                        error,
                        retry_after_seconds,
                        event_id,
                        attempt_number,
                        worker_id,
                        route_id,
                        official_stream_id,
                        lineage_id,
                    )
                else:
                    event_updated = await connection.fetchrow(
                        f"""
                        UPDATE {self.schema}.outbox_events
                        SET delivery_status='DELIVERY_UNKNOWN',last_error=$1,
                            lease_owner=NULL,lease_until=NULL
                        WHERE event_id=$2 AND delivery_status='DELIVERY_UNKNOWN'
                          AND attempt_count=$3 AND lease_owner=$4
                          AND route_id=$5 AND official_stream_id=$6 AND lineage_id=$7
                        RETURNING event_id
                        """,
                        error,
                        event_id,
                        attempt_number,
                        worker_id,
                        route_id,
                        official_stream_id,
                        lineage_id,
                    )
                if event_updated is None:
                    raise V20StateConflict("outbox delivery finalize CAS lost")

    async def list_pending_shadow_batches(
        self,
        before_date: date,
        *,
        official_stream_id: str,
        lineage_id: str,
    ) -> list[ShadowBatchRecord]:
        """Return unmatured batches whose T+2 date is strictly in the past."""
        _require_scope(official_stream_id, lineage_id)
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT shadow.* FROM {self.schema}.shadow_batches AS shadow
                WHERE shadow.status='PENDING' AND shadow.t2_date < $1
                  AND shadow.official_stream_id=$2 AND shadow.lineage_id=$3
                ORDER BY shadow.t2_date,shadow.signal_date,shadow.kind,shadow.batch_id
                """,
                before_date,
                official_stream_id,
                lineage_id,
            )
        return [self._shadow_from_row(row) for row in rows]

    async def list_pending_shadow_reference_batches(
        self,
        before_signal_date: date,
        *,
        official_stream_id: str,
        lineage_id: str,
    ) -> list[ShadowBatchRecord]:
        """Return older shadow streams whose D0 reference is not terminal."""

        _require_scope(official_stream_id, lineage_id)
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT shadow.* FROM {self.schema}.shadow_batches AS shadow
                WHERE shadow.reference_status='PENDING' AND shadow.signal_date < $1
                  AND shadow.official_stream_id=$2 AND shadow.lineage_id=$3
                ORDER BY shadow.signal_date,shadow.kind,shadow.batch_id
                """,
                before_signal_date,
                official_stream_id,
                lineage_id,
            )
        return [self._shadow_from_row(row) for row in rows]

    async def update_shadow_references(
        self,
        signal_date: date,
        *,
        official_stream_id: str,
        lineage_id: str,
        reference_prices: Mapping[str, float],
        snapshot_hash: str,
        not_before_ts: datetime | None = None,
    ) -> tuple[str, ...]:
        """Atomically lock the same D0 reference snapshot on both shadow streams."""
        _require_scope(official_stream_id, lineage_id)
        normalized: dict[str, float] = {}
        for code, raw_price in reference_prices.items():
            if not re.fullmatch(r"\d{6}", code):
                raise ValueError(f"invalid shadow reference code: {code!r}")
            price = float(raw_price)
            if not math.isfinite(price) or price <= 0:
                raise ValueError(f"invalid shadow reference price for {code}")
            normalized[code] = price
        if not normalized:
            raise ValueError("reference_prices cannot be empty")
        _require_sha256(snapshot_hash, "snapshot_hash")
        _require_aware(not_before_ts, "shadow reference not_before_ts")
        encoded = canonical_json(normalized)

        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                rows = await connection.fetch(
                    f"""
                    SELECT shadow.batch_id,shadow.kind,shadow.reference_status,
                           shadow.reference_prices_json,shadow.reference_snapshot_hash
                    FROM {self.schema}.shadow_batches AS shadow
                    WHERE shadow.signal_date=$1
                      AND shadow.official_stream_id=$2 AND shadow.lineage_id=$3
                    ORDER BY shadow.kind,shadow.batch_id
                    FOR UPDATE OF shadow
                    """,
                    signal_date,
                    official_stream_id,
                    lineage_id,
                )
                if not rows:
                    raise V20RepositoryError(
                        f"no shadow batches exist for signal date {signal_date.isoformat()}"
                    )
                if not _legacy_shadow_rows_valid(rows):
                    raise V20StateConflict(
                        "shadow reference lock requires one HEALTH and at most one legacy ROLLING7"
                    )
                pending_ids: list[str] = []
                for row in rows:
                    if row["reference_status"] == "PENDING":
                        pending_ids.append(row["batch_id"])
                        continue
                    if (
                        row["reference_status"] == "LOCKED"
                        and _json_value(row["reference_prices_json"]) == normalized
                        and row["reference_snapshot_hash"] == snapshot_hash
                    ):
                        continue
                    raise V20SemanticConflict(
                        f"shadow reference already finalized differently: {row['batch_id']}"
                    )
                if not pending_ids:
                    return ()
                result = await connection.execute(
                    f"""
                    UPDATE {self.schema}.shadow_batches AS shadow
                    SET reference_status='LOCKED',reference_prices_json=$1::jsonb,
                        reference_snapshot_hash=$2,reference_locked_at=clock_timestamp()
                    WHERE shadow.batch_id=ANY($3::text[])
                      AND shadow.reference_status='PENDING'
                      AND shadow.official_stream_id=$4 AND shadow.lineage_id=$5
                      AND ($6::timestamptz IS NULL OR clock_timestamp() >= $6)
                    """,
                    encoded,
                    snapshot_hash,
                    pending_ids,
                    official_stream_id,
                    lineage_id,
                    not_before_ts,
                )
                if result != f"UPDATE {len(pending_ids)}":
                    raise V20StateConflict("shadow reference CAS lost")
                return tuple(pending_ids)

    async def get_shadow_reference_status(
        self,
        signal_date: date,
        *,
        official_stream_id: str,
        lineage_id: str,
    ) -> str | None:
        """Return the shared shadow-reference status, rejecting split state."""

        _require_scope(official_stream_id, lineage_id)
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT shadow.kind,shadow.reference_status
                FROM {self.schema}.shadow_batches AS shadow
                WHERE shadow.signal_date=$1
                  AND shadow.official_stream_id=$2 AND shadow.lineage_id=$3
                ORDER BY shadow.kind,shadow.batch_id
                """,
                signal_date,
                official_stream_id,
                lineage_id,
            )
        if not rows:
            return None
        if not _legacy_shadow_rows_valid(rows):
            raise V20StateConflict("shadow reference status requires valid legacy streams")
        statuses = {str(row["reference_status"]) for row in rows}
        if len(statuses) != 1:
            raise V20StateConflict("shadow reference streams have split status")
        return statuses.pop()

    async def finalize_shadow_references_unavailable(
        self,
        signal_date: date,
        *,
        official_stream_id: str,
        lineage_id: str,
        snapshot_hash: str,
        not_before_ts: datetime | None = None,
    ) -> tuple[str, ...]:
        """Atomically mark both shadow streams unavailable at their fixed deadline."""
        _require_scope(official_stream_id, lineage_id)
        _require_sha256(snapshot_hash, "snapshot_hash")
        _require_aware(not_before_ts, "shadow unavailable not_before_ts")
        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                rows = await connection.fetch(
                    f"""
                    SELECT shadow.batch_id,shadow.kind,shadow.reference_status,
                           shadow.reference_snapshot_hash
                    FROM {self.schema}.shadow_batches AS shadow
                    WHERE shadow.signal_date=$1
                      AND shadow.official_stream_id=$2 AND shadow.lineage_id=$3
                    ORDER BY shadow.kind,shadow.batch_id
                    FOR UPDATE OF shadow
                    """,
                    signal_date,
                    official_stream_id,
                    lineage_id,
                )
                if not rows:
                    raise V20RepositoryError(
                        f"no shadow batches exist for signal date {signal_date.isoformat()}"
                    )
                if not _legacy_shadow_rows_valid(rows):
                    raise V20StateConflict(
                        "shadow reference finalization requires one HEALTH "
                        "and at most one legacy ROLLING7"
                    )
                pending_ids: list[str] = []
                for row in rows:
                    if row["reference_status"] == "PENDING":
                        pending_ids.append(row["batch_id"])
                    elif not (
                        row["reference_status"] == "UNAVAILABLE"
                        and row["reference_snapshot_hash"] == snapshot_hash
                    ):
                        raise V20SemanticConflict(
                            f"shadow reference already finalized differently: {row['batch_id']}"
                        )
                if not pending_ids:
                    return ()
                result = await connection.execute(
                    f"""
                    UPDATE {self.schema}.shadow_batches AS shadow
                    SET reference_status='UNAVAILABLE',reference_prices_json=NULL,
                        reference_snapshot_hash=$1,reference_locked_at=clock_timestamp()
                    WHERE shadow.batch_id=ANY($2::text[])
                      AND shadow.reference_status='PENDING'
                      AND shadow.official_stream_id=$3 AND shadow.lineage_id=$4
                      AND ($5::timestamptz IS NULL OR clock_timestamp() >= $5)
                    """,
                    snapshot_hash,
                    pending_ids,
                    official_stream_id,
                    lineage_id,
                    not_before_ts,
                )
                if result != f"UPDATE {len(pending_ids)}":
                    raise V20StateConflict("shadow unavailable CAS lost")
                return tuple(pending_ids)

    async def complete_shadow_batch(
        self,
        batch_id: str,
        batch_return: float | None,
        status: str,
        payload_update: Mapping[str, Any],
        *,
        official_stream_id: str,
        lineage_id: str,
        not_before_ts: datetime | None = None,
    ) -> bool:
        """Complete one stream independently; an exact retry is a no-op."""
        _require_scope(official_stream_id, lineage_id)
        _require_aware(not_before_ts, "shadow completion not_before_ts")
        if status not in {"COMPLETE_VALID", "COMPLETE_INVALID"}:
            raise ValueError("invalid terminal shadow batch status")
        if status == "COMPLETE_VALID":
            if batch_return is None or not math.isfinite(batch_return):
                raise ValueError("COMPLETE_VALID requires a finite batch_return")
        elif batch_return is not None:
            raise ValueError("COMPLETE_INVALID requires batch_return=None")
        update = dict(payload_update)
        canonical_json(update)

        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                row = await connection.fetchrow(
                    f"""
                    SELECT shadow.* FROM {self.schema}.shadow_batches AS shadow
                    WHERE shadow.batch_id=$1
                      AND shadow.official_stream_id=$2 AND shadow.lineage_id=$3
                    FOR UPDATE OF shadow
                    """,
                    batch_id,
                    official_stream_id,
                    lineage_id,
                )
                if row is None:
                    raise V20RepositoryError(f"unknown shadow batch in scope {batch_id!r}")
                current = self._shadow_from_row(row)
                merged = dict(current.payload)
                merged.update(update)
                if current.status != "PENDING":
                    if (
                        current.status == status
                        and current.batch_return == batch_return
                        and merged == current.payload
                    ):
                        return False
                    raise V20SemanticConflict("shadow batch is already completed differently")
                if status == "COMPLETE_VALID" and current.reference_status != "LOCKED":
                    raise V20StateConflict("valid shadow batch requires locked reference prices")
                result = await connection.execute(
                    f"""
                    UPDATE {self.schema}.shadow_batches AS shadow
                    SET status=$1,batch_return=$2,batch_json=$3::jsonb,
                        completed_at=clock_timestamp()
                    WHERE shadow.batch_id=$4 AND shadow.status='PENDING'
                      AND shadow.official_stream_id=$5 AND shadow.lineage_id=$6
                      AND ($7::timestamptz IS NULL OR clock_timestamp() >= $7)
                    """,
                    status,
                    batch_return,
                    canonical_json(merged),
                    batch_id,
                    official_stream_id,
                    lineage_id,
                    not_before_ts,
                )
                if result != "UPDATE 1":
                    raise V20StateConflict("shadow completion CAS lost")
                return True

    async def load_recent_completed(
        self,
        kind: str,
        before_t2: date,
        limit: int,
        *,
        official_stream_id: str,
        lineage_id: str,
    ) -> list[ShadowBatchRecord]:
        _require_scope(official_stream_id, lineage_id)
        if kind not in {"HEALTH", "ROLLING7"}:
            raise ValueError(f"unsupported shadow batch kind: {kind!r}")
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT shadow.* FROM {self.schema}.shadow_batches AS shadow
                WHERE shadow.kind=$1
                  AND shadow.status IN ('COMPLETE_VALID','COMPLETE_INVALID')
                  AND shadow.t2_date < $2
                  AND shadow.official_stream_id=$4 AND shadow.lineage_id=$5
                ORDER BY shadow.t2_date DESC,shadow.signal_date DESC,shadow.batch_id DESC
                LIMIT $3
                """,
                kind,
                before_t2,
                limit,
                official_stream_id,
                lineage_id,
            )
        return [self._shadow_from_row(row) for row in rows]

    async def lock_reference_price(
        self,
        model_leg_id: str,
        *,
        official_stream_id: str,
        lineage_id: str,
        reference_profile_id: str,
        price: float | None,
        snapshot_hash: str,
        unavailable: bool = False,
        not_before_ts: datetime | None = None,
    ) -> bool:
        _require_scope(official_stream_id, lineage_id)
        if unavailable and price is not None:
            raise ValueError("provide a price or mark unavailable, but not both")
        if not unavailable and price is None:
            raise ValueError("provide a price or mark unavailable")
        if price is not None and (not math.isfinite(price) or price <= 0):
            raise ValueError("reference price must be finite and positive")
        if not reference_profile_id:
            raise ValueError("reference_profile_id cannot be empty")
        _require_sha256(snapshot_hash, "snapshot_hash")
        _require_aware(not_before_ts, "model reference not_before_ts")
        status = "UNAVAILABLE" if unavailable else "LOCKED"
        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                row = await connection.fetchrow(
                    f"""
                    SELECT leg.reference_status,leg.reference_price,
                           leg.reference_snapshot_hash,batch.reference_profile_id
                    FROM {self.schema}.model_legs AS leg
                    JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
                    JOIN {self.schema}.outbox_events AS source
                      ON source.event_id=batch.source_event_id
                    WHERE leg.model_leg_id=$1
                      AND batch.official_stream_id=$2 AND batch.lineage_id=$3
                      AND source.official_stream_id=$2 AND source.lineage_id=$3
                      AND source.seal_status='SEALED'
                      AND {_model_batch_authorization_sql(self.schema)}
                    FOR UPDATE OF leg
                    """,
                    model_leg_id,
                    official_stream_id,
                    lineage_id,
                )
                if row is None:
                    raise V20RepositoryError(f"unknown model leg in scope {model_leg_id!r}")
                if row["reference_profile_id"] != reference_profile_id:
                    raise V20SemanticConflict("reference profile does not match model batch")
                if row["reference_status"] != "PENDING":
                    stored_price = _optional_finite_float(
                        row["reference_price"], "model leg reference_price"
                    )
                    if (
                        row["reference_status"] == status
                        and stored_price == price
                        and row["reference_snapshot_hash"] == snapshot_hash
                    ):
                        return False
                    raise V20SemanticConflict(
                        "model leg reference is already finalized differently"
                    )
                result = await connection.execute(
                    f"""
                    UPDATE {self.schema}.model_legs AS leg
                    SET reference_status=$1,reference_price=$2,reference_snapshot_hash=$3,
                        reference_locked_at=clock_timestamp()
                    FROM {self.schema}.model_batches AS batch,
                         {self.schema}.outbox_events AS source
                    WHERE leg.model_leg_id=$4 AND leg.reference_status='PENDING'
                      AND leg.model_batch_id=batch.model_batch_id
                      AND source.event_id=batch.source_event_id
                      AND batch.official_stream_id=$5 AND batch.lineage_id=$6
                      AND source.official_stream_id=$5 AND source.lineage_id=$6
                      AND source.seal_status='SEALED'
                      AND {_model_batch_authorization_sql(self.schema)}
                      AND ($7::timestamptz IS NULL OR clock_timestamp() >= $7)
                    """,
                    status,
                    price,
                    snapshot_hash,
                    model_leg_id,
                    official_stream_id,
                    lineage_id,
                    not_before_ts,
                )
                if result != "UPDATE 1":
                    raise V20StateConflict("reference lock CAS lost")
                return True

    async def list_pending_reference_legs(
        self,
        signal_date: date,
        *,
        official_stream_id: str,
        lineage_id: str,
    ) -> list[PendingReferenceLeg]:
        _require_scope(official_stream_id, lineage_id)
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT leg.model_leg_id,leg.model_batch_id,batch.signal_date,
                       leg.code,batch.reference_profile_id
                FROM {self.schema}.model_legs AS leg
                JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
                JOIN {self.schema}.outbox_events AS source
                  ON source.event_id=batch.source_event_id
                WHERE batch.signal_date=$1 AND leg.reference_status='PENDING'
                  AND batch.official_stream_id=$2 AND batch.lineage_id=$3
                  AND source.official_stream_id=$2 AND source.lineage_id=$3
                  AND source.seal_status='SEALED'
                  AND {_model_batch_authorization_sql(self.schema)}
                ORDER BY leg.rank,leg.model_leg_id
                """,
                signal_date,
                official_stream_id,
                lineage_id,
            )
        return [
            PendingReferenceLeg(
                model_leg_id=row["model_leg_id"],
                model_batch_id=row["model_batch_id"],
                signal_date=row["signal_date"],
                code=row["code"],
                reference_profile_id=row["reference_profile_id"],
            )
            for row in rows
        ]

    async def finalize_pending_references_unavailable(
        self,
        signal_date: date,
        *,
        official_stream_id: str,
        lineage_id: str,
        reference_profile_id: str,
        snapshot_hash: str,
        not_before_ts: datetime | None = None,
    ) -> tuple[str, ...]:
        """Finalize every still-pending leg for a signal date as unavailable."""
        _require_scope(official_stream_id, lineage_id)
        if not reference_profile_id:
            raise ValueError("reference_profile_id cannot be empty")
        _require_sha256(snapshot_hash, "snapshot_hash")
        _require_aware(not_before_ts, "model unavailable not_before_ts")
        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                rows = await connection.fetch(
                    f"""
                    SELECT leg.model_leg_id,leg.reference_status,
                           leg.reference_snapshot_hash,batch.reference_profile_id
                    FROM {self.schema}.model_legs AS leg
                    JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
                    JOIN {self.schema}.outbox_events AS source
                      ON source.event_id=batch.source_event_id
                    WHERE batch.signal_date=$1
                      AND batch.official_stream_id=$2 AND batch.lineage_id=$3
                      AND source.official_stream_id=$2 AND source.lineage_id=$3
                      AND source.seal_status='SEALED'
                      AND {_model_batch_authorization_sql(self.schema)}
                    ORDER BY leg.model_leg_id
                    FOR UPDATE OF leg
                    """,
                    signal_date,
                    official_stream_id,
                    lineage_id,
                )
                mismatched = [
                    row["model_leg_id"]
                    for row in rows
                    if row["reference_profile_id"] != reference_profile_id
                ]
                if mismatched:
                    raise V20SemanticConflict(
                        "reference profile does not match pending model legs: "
                        + ", ".join(mismatched[:5])
                    )
                for row in rows:
                    if row["reference_status"] == "UNAVAILABLE":
                        if row["reference_snapshot_hash"] != snapshot_hash:
                            raise V20SemanticConflict(
                                "model leg reference was finalized unavailable with "
                                f"different evidence: {row['model_leg_id']}"
                            )
                    elif row["reference_status"] != "PENDING":
                        # A successfully locked sibling is expected when only part of
                        # the recommended list was present in the reference snapshot.
                        continue
                leg_ids = tuple(
                    row["model_leg_id"] for row in rows if row["reference_status"] == "PENDING"
                )
                if not leg_ids:
                    return ()
                result = await connection.execute(
                    f"""
                    UPDATE {self.schema}.model_legs AS leg
                    SET reference_status='UNAVAILABLE',reference_price=NULL,
                        reference_snapshot_hash=$1,reference_locked_at=clock_timestamp()
                    FROM {self.schema}.model_batches AS batch
                    WHERE leg.model_batch_id=batch.model_batch_id
                      AND batch.signal_date=$2
                      AND batch.reference_profile_id=$3
                      AND leg.reference_status='PENDING'
                      AND batch.official_stream_id=$4 AND batch.lineage_id=$5
                      AND EXISTS (
                          SELECT 1 FROM {self.schema}.outbox_events AS source
                          WHERE source.event_id=batch.source_event_id
                            AND source.official_stream_id=$4
                            AND source.lineage_id=$5
                            AND source.seal_status='SEALED'
                            AND {_model_batch_authorization_sql(self.schema)}
                      )
                      AND ($6::timestamptz IS NULL OR clock_timestamp() >= $6)
                    """,
                    snapshot_hash,
                    signal_date,
                    reference_profile_id,
                    official_stream_id,
                    lineage_id,
                    not_before_ts,
                )
                if result != f"UPDATE {len(leg_ids)}":
                    raise V20StateConflict("bulk reference finalization CAS lost")
                return leg_ids

    async def list_active_legs(
        self,
        trade_date: date,
        *,
        official_stream_id: str,
        lineage_id: str,
    ) -> list[ActiveModelLeg]:
        _require_scope(official_stream_id, lineage_id)
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT l.*,b.decision_id,b.origin_kind,b.source_event_id,
                       b.signal_date,b.evaluation_only,
                       ms.snapshot_id AS mews_snapshot_id,ms.fast_state AS mews_fast_state,
                       x.exit_intent_id
                FROM {self.schema}.model_legs l
                JOIN {self.schema}.model_batches b USING (model_batch_id)
                JOIN {self.schema}.outbox_events source ON source.event_id=b.source_event_id
                LEFT JOIN {self.schema}.leg_mews_selection s USING (model_leg_id)
                LEFT JOIN {self.schema}.mews_snapshots ms ON ms.snapshot_id=s.snapshot_id
                LEFT JOIN {self.schema}.exit_intents x USING (model_leg_id)
                WHERE l.d1 <= $1 AND x.exit_intent_id IS NULL
                  AND b.evaluation_only=FALSE AND source.seal_status='SEALED'
                  AND b.official_stream_id=$2 AND b.lineage_id=$3
                  AND source.official_stream_id=$2 AND source.lineage_id=$3
                  AND {
                    _model_batch_authorization_sql(
                        self.schema,
                        batch_alias="b",
                        source_alias="source",
                    )
                }
                ORDER BY b.signal_date,l.rank,l.model_leg_id
                """,
                trade_date,
                official_stream_id,
                lineage_id,
            )
        return [_active_model_leg_from_row(row) for row in rows]

    async def list_manual_monitor_batch_legs(
        self,
        model_batch_id: str,
        *,
        official_stream_id: str,
        lineage_id: str,
    ) -> list[ActiveModelLeg]:
        """Read every durable leg in one manual batch, including exited legs."""

        _require_scope(official_stream_id, lineage_id)
        if not model_batch_id:
            raise ValueError("model_batch_id cannot be empty")
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT leg.*,batch.decision_id,batch.origin_kind,batch.source_event_id,
                       batch.signal_date,batch.evaluation_only,
                       mews.snapshot_id AS mews_snapshot_id,
                       mews.fast_state AS mews_fast_state,
                       exit_intent.exit_intent_id
                FROM {self.schema}.model_legs AS leg
                JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
                JOIN {self.schema}.manual_monitor_enrollments AS enrollment
                  ON enrollment.model_batch_id=batch.model_batch_id
                 AND enrollment.source_event_id=batch.source_event_id
                JOIN {self.schema}.outbox_events AS source
                  ON source.event_id=batch.source_event_id
                LEFT JOIN {self.schema}.leg_mews_selection AS selection USING (model_leg_id)
                LEFT JOIN {self.schema}.mews_snapshots AS mews
                  ON mews.snapshot_id=selection.snapshot_id
                LEFT JOIN {self.schema}.exit_intents AS exit_intent USING (model_leg_id)
                WHERE batch.model_batch_id=$1
                  AND batch.origin_kind='MANUAL_MONITOR'
                  AND batch.evaluation_only=FALSE
                  AND batch.official_stream_id=$2 AND batch.lineage_id=$3
                  AND enrollment.official_stream_id=$2 AND enrollment.lineage_id=$3
                  AND source.official_stream_id=$2 AND source.lineage_id=$3
                  AND source.event_type='DATA_ALERT' AND source.seal_status='SEALED'
                ORDER BY leg.rank,leg.model_leg_id
                """,
                model_batch_id,
                official_stream_id,
                lineage_id,
            )
        return [_active_model_leg_from_row(row) for row in rows]

    async def load_mews_calculation_state(self) -> dict[str, Any] | None:
        """Load the compact, source-derived MEWS state used for daily extension."""

        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                f"""
                SELECT state_date,model_version,content_hash,state_json
                FROM {self.schema}.mews_calculation_state
                WHERE state_key='mews_v2'
                """
            )
        if row is None:
            return None
        payload = _json_value(row["state_json"])
        if not isinstance(payload, dict):
            raise V20SemanticConflict("MEWS calculation state is not a JSON object")
        if (
            str(payload.get("state_date")) != row["state_date"].isoformat()
            or payload.get("model_version") != row["model_version"]
            or sha256_json(payload) != row["content_hash"]
        ):
            raise V20SemanticConflict("MEWS calculation state integrity check failed")
        return payload

    async def save_mews_calculation_state(self, state: Mapping[str, Any]) -> str:
        """Monotonically checkpoint one locally calculated MEWS trading day."""

        if state.get("schema") != "v20-mews-incremental-state/v1":
            raise ValueError("MEWS calculation state schema is invalid")
        if state.get("model_version") != "mews_v2":
            raise ValueError("MEWS calculation state model_version is invalid")
        try:
            state_date = date.fromisoformat(str(state["state_date"]))
        except (KeyError, ValueError) as exc:
            raise ValueError("MEWS calculation state_date is invalid") from exc
        payload = dict(state)
        content_hash = sha256_json(payload)
        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                existing = await connection.fetchrow(
                    f"""
                    SELECT state_date,content_hash
                    FROM {self.schema}.mews_calculation_state
                    WHERE state_key='mews_v2'
                    FOR UPDATE
                    """
                )
                if existing is None:
                    await connection.execute(
                        f"""
                        INSERT INTO {self.schema}.mews_calculation_state
                            (state_key,state_date,model_version,content_hash,state_json)
                        VALUES ('mews_v2',$1,$2,$3,$4::jsonb)
                        """,
                        state_date,
                        "mews_v2",
                        content_hash,
                        canonical_json(payload),
                    )
                elif existing["state_date"] > state_date:
                    raise V20SemanticConflict("MEWS calculation state cannot regress")
                elif existing["state_date"] == state_date:
                    if existing["content_hash"] != content_hash:
                        raise V20SemanticConflict(
                            "MEWS calculation state changed for an already sealed date"
                        )
                else:
                    await connection.execute(
                        f"""
                        UPDATE {self.schema}.mews_calculation_state
                        SET state_date=$1,model_version=$2,content_hash=$3,
                            state_json=$4::jsonb,updated_at=clock_timestamp()
                        WHERE state_key='mews_v2'
                        """,
                        state_date,
                        "mews_v2",
                        content_hash,
                        canonical_json(payload),
                    )
        return content_hash

    async def record_mews_snapshot(self, payload: Mapping[str, Any]) -> str:
        required = {
            "snapshot_id",
            "source_trade_date",
            "generated_at",
            "fast_state",
            "model_version",
            "data_version",
        }
        missing = sorted(required - set(payload))
        if missing:
            raise ValueError(f"MEWS snapshot missing fields: {', '.join(missing)}")
        for field_name in ("snapshot_id", "fast_state", "model_version", "data_version"):
            if not str(payload[field_name]).strip():
                raise ValueError(f"MEWS {field_name} cannot be empty")
        if payload["fast_state"] not in {"NORMAL", "DANGER"}:
            raise ValueError("MEWS fast_state must be NORMAL or DANGER")
        content_hash = sha256_json(payload)
        source_date = date.fromisoformat(str(payload["source_trade_date"]))
        generated_at = datetime.fromisoformat(str(payload["generated_at"]))
        _require_aware(generated_at, "MEWS generated_at")
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.mews_snapshots
                        (snapshot_id,source_trade_date,generated_at,fast_state,
                         model_version,data_version,content_hash,snapshot_json)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
                    ON CONFLICT (snapshot_id) DO NOTHING
                    """,
                    str(payload["snapshot_id"]),
                    source_date,
                    generated_at,
                    str(payload["fast_state"]),
                    str(payload["model_version"]),
                    str(payload["data_version"]),
                    content_hash,
                    canonical_json(payload),
                )
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    f"""
                    UPDATE {self.schema}.mews_snapshots
                    SET receipt_sealed_at=clock_timestamp()
                    WHERE snapshot_id=$1 AND content_hash=$2
                      AND receipt_sealed_at IS NULL
                    """,
                    str(payload["snapshot_id"]),
                    content_hash,
                )
                row = await connection.fetchrow(
                    f"SELECT content_hash,receipt_sealed_at FROM "
                    f"{self.schema}.mews_snapshots WHERE snapshot_id=$1",
                    str(payload["snapshot_id"]),
                )
        if row is None or row["content_hash"] != content_hash or row["receipt_sealed_at"] is None:
            raise V20SemanticConflict("MEWS snapshot_id collision")
        return content_hash

    async def mews_snapshot_is_eligible(
        self,
        snapshot_id: str,
        *,
        source_trade_date: date,
        cutoff: datetime,
    ) -> bool:
        """Verify the database-owned receipt crossed storage before cutoff."""

        _require_aware(cutoff, "MEWS cutoff")
        async with self.pool.acquire() as connection:
            value = await connection.fetchval(
                f"""
                SELECT EXISTS (
                    SELECT 1 FROM {self.schema}.mews_snapshots
                    WHERE snapshot_id=$1 AND source_trade_date=$2
                      AND generated_at < $3 AND receipt_sealed_at < $3
                )
                """,
                snapshot_id,
                source_trade_date,
                cutoff,
            )
        return value is True

    async def find_eligible_mews_snapshot(
        self,
        *,
        source_trade_date: date,
        cutoff: datetime,
        availability_date: date | None = None,
    ) -> str | None:
        """Recover a qualified daily cache after a V20 process restart.

        A snapshot qualifies on time (generated and sealed before the cutoff)
        or as the same day's late local repair (its evidence availability date
        matches ``availability_date``); a late repair is never invalid merely
        because it was calculated after the cutoff.
        """

        _require_aware(cutoff, "MEWS cutoff")
        availability = availability_date.isoformat() if availability_date is not None else None
        async with self.pool.acquire() as connection:
            value = await connection.fetchval(
                f"""
                SELECT snapshot_id FROM {self.schema}.mews_snapshots
                WHERE source_trade_date=$1
                  AND (
                    (generated_at < $2 AND receipt_sealed_at < $2)
                    OR (snapshot_json->'evidence'->>'signal_available_date' = $3)
                  )
                ORDER BY generated_at DESC,receipt_sealed_at DESC,snapshot_id DESC
                LIMIT 1
                """,
                source_trade_date,
                cutoff,
                availability,
            )
        return str(value) if value is not None else None

    async def select_mews_for_leg(
        self,
        model_leg_id: str,
        *,
        d1: date,
        cutoff: datetime,
        late_source_trade_date: date | None = None,
        late_availability_date: date | None = None,
    ) -> tuple[str | None, str | None, str]:
        """Freeze the leg's MEWS selection exactly once.

        Candidates qualify on time (generated and sealed before ``cutoff``) or
        as the leg's late-repaired daily value: generated after the cutoff but
        with ``late_source_trade_date`` as source (the correct predecessor of
        ``d1``) and evidence availability equal to ``late_availability_date``
        (``d1``).  An already frozen selection — including a fallback freeze —
        is returned unchanged and is never rewritten.
        """

        _require_aware(cutoff, "MEWS cutoff")
        late_availability = (
            late_availability_date.isoformat() if late_availability_date is not None else None
        )
        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                leg = await connection.fetchrow(
                    f"""
                    SELECT leg.d1
                    FROM {self.schema}.model_legs AS leg
                    JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
                    JOIN {self.schema}.outbox_events AS source
                      ON source.event_id=batch.source_event_id
                    WHERE leg.model_leg_id=$1
                      AND source.seal_status='SEALED'
                      AND {_model_batch_authorization_sql(self.schema)}
                    FOR UPDATE OF leg
                    """,
                    model_leg_id,
                )
                if leg is None:
                    raise V20RepositoryError(f"unknown model leg {model_leg_id!r}")
                if leg["d1"] != d1:
                    raise V20SemanticConflict("MEWS selection d1 does not match model leg")
                existing = await connection.fetchrow(
                    f"SELECT snapshot_id,fast_state,selection_reason,cutoff_ts FROM "
                    f"{self.schema}.leg_mews_selection WHERE model_leg_id=$1",
                    model_leg_id,
                )
                if existing is not None:
                    if existing["cutoff_ts"] != cutoff:
                        raise V20SemanticConflict(
                            "MEWS selection was already frozen with a different cutoff"
                        )
                    return (
                        existing["snapshot_id"],
                        existing["fast_state"],
                        existing["selection_reason"],
                    )
                row = await connection.fetchrow(
                    f"""
                    SELECT snapshot_id,fast_state,
                           (generated_at < $2 AND receipt_sealed_at < $2) AS on_time
                    FROM {self.schema}.mews_snapshots
                    WHERE source_trade_date < $1
                      AND (
                        (generated_at < $2 AND receipt_sealed_at < $2)
                        OR (
                          $3::date IS NOT NULL
                          AND source_trade_date = $3
                          AND snapshot_json->'evidence'->>'signal_available_date' = $4
                        )
                      )
                    ORDER BY source_trade_date DESC,generated_at DESC,
                             receipt_sealed_at DESC,snapshot_id DESC
                    LIMIT 1
                    """,
                    d1,
                    cutoff,
                    late_source_trade_date,
                    late_availability,
                )
                snapshot_id = row["snapshot_id"] if row else None
                fast_state = row["fast_state"] if row else None
                if row is None:
                    reason = "MEWS_UNAVAILABLE_FALLBACK_12"
                elif row["on_time"]:
                    reason = "ELIGIBLE"
                else:
                    reason = "ELIGIBLE_LATE_SAME_DAY"
                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.leg_mews_selection
                        (model_leg_id,snapshot_id,fast_state,cutoff_ts,selection_reason)
                    VALUES ($1,$2,$3,$4,$5)
                    """,
                    model_leg_id,
                    snapshot_id,
                    fast_state,
                    cutoff,
                    reason,
                )
                return snapshot_id, fast_state, reason

    async def load_selected_mews_for_leg(
        self,
        model_leg_id: str,
    ) -> SelectedMewsRecord | None:
        """Load the frozen selection and verify its complete PIT evidence."""
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                f"""
                SELECT leg.model_leg_id,leg.d1,selection.cutoff_ts,
                       selection.selection_reason,selection.selected_at,
                       selection.snapshot_id,selection.fast_state AS selected_fast_state,
                       snapshot.source_trade_date,snapshot.generated_at,
                       snapshot.receipt_sealed_at AS received_at,
                       snapshot.fast_state,snapshot.model_version,snapshot.data_version,
                       snapshot.content_hash,snapshot.snapshot_json
                FROM {self.schema}.model_legs AS leg
                JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
                JOIN {self.schema}.outbox_events AS source
                  ON source.event_id=batch.source_event_id
                JOIN {self.schema}.leg_mews_selection AS selection USING (model_leg_id)
                LEFT JOIN {self.schema}.mews_snapshots AS snapshot
                    ON snapshot.snapshot_id=selection.snapshot_id
                WHERE leg.model_leg_id=$1
                  AND source.seal_status='SEALED'
                  AND {_model_batch_authorization_sql(self.schema)}
                """,
                model_leg_id,
            )
        if row is None:
            return None
        snapshot_id = row["snapshot_id"]
        payload = _json_value(row["snapshot_json"]) if row["snapshot_json"] is not None else None
        if snapshot_id is None:
            if row["selected_fast_state"] is not None or payload is not None:
                raise V20SemanticConflict("MEWS unavailable selection contains snapshot data")
        else:
            required_values = (
                row["source_trade_date"],
                row["generated_at"],
                row["received_at"],
                row["fast_state"],
                row["model_version"],
                row["data_version"],
                row["content_hash"],
                payload,
            )
            if any(value is None for value in required_values):
                raise V20SemanticConflict("selected MEWS snapshot is incomplete")
            if row["selected_fast_state"] != row["fast_state"]:
                raise V20SemanticConflict("selected MEWS fast_state does not match snapshot")
            if sha256_json(payload) != row["content_hash"]:
                raise V20SemanticConflict("selected MEWS snapshot hash mismatch")
            on_time = (
                row["generated_at"] < row["cutoff_ts"] and row["received_at"] < row["cutoff_ts"]
            )
            evidence = payload.get("evidence") if isinstance(payload, Mapping) else None
            availability = (
                evidence.get("signal_available_date") if isinstance(evidence, Mapping) else None
            )
            late_same_day = availability == row["d1"].isoformat()
            if not (row["source_trade_date"] < row["d1"] and (on_time or late_same_day)):
                raise V20SemanticConflict("selected MEWS snapshot violates PIT cutoff")
        return SelectedMewsRecord(
            model_leg_id=row["model_leg_id"],
            d1=row["d1"],
            cutoff_ts=row["cutoff_ts"],
            selection_reason=row["selection_reason"],
            selected_at=row["selected_at"],
            snapshot_id=snapshot_id,
            source_trade_date=row["source_trade_date"],
            generated_at=row["generated_at"],
            received_at=row["received_at"],
            fast_state=row["fast_state"],
            model_version=row["model_version"],
            data_version=row["data_version"],
            content_hash=row["content_hash"],
            payload=payload,
        )

    async def commit_exit(self, commit: ExitCommit) -> bool:
        _require_outbox_scope(commit.route_id, commit.official_stream_id, commit.lineage_id)
        _require_aware(commit.trigger_ts, "trigger_ts")
        _require_aware(commit.rule_actionable_from, "rule_actionable_from")
        _require_sha256(commit.semantic_content_hash, "semantic_content_hash")
        if sha256_json(commit.semantic) != commit.semantic_content_hash:
            raise V20SemanticConflict("exit semantic_content_hash mismatch")
        commit_fingerprint = _exit_commit_fingerprint(commit)
        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                existing = await connection.fetchrow(
                    f"SELECT exit_intent_id,event_id,model_leg_id,commit_fingerprint FROM "
                    f"{self.schema}.exit_intents "
                    "WHERE model_leg_id=$1 OR exit_intent_id=$2 OR event_id=$3 FOR UPDATE",
                    commit.model_leg_id,
                    commit.exit_intent_id,
                    commit.event_id,
                )
                if existing is not None:
                    if (
                        existing["exit_intent_id"] == commit.exit_intent_id
                        and existing["event_id"] == commit.event_id
                        and existing["model_leg_id"] == commit.model_leg_id
                        and existing["commit_fingerprint"] == commit_fingerprint
                    ):
                        return False
                    raise V20SemanticConflict("exit/model-leg/event ID collision")
                trigger_reached = await connection.fetchval(
                    "SELECT clock_timestamp() >= $1::timestamptz",
                    commit.trigger_ts,
                )
                if trigger_reached is not True:
                    raise V20StateConflict("database clock has not reached exit trigger_ts")
                leg = await connection.fetchrow(
                    f"""
                    SELECT batch.evaluation_only,source.seal_status,
                           batch.origin_kind,source.event_type
                    FROM {self.schema}.model_legs AS leg
                    JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
                    JOIN {self.schema}.outbox_events AS source
                      ON source.event_id=batch.source_event_id
                    WHERE leg.model_leg_id=$1
                      AND batch.official_stream_id=$2 AND batch.lineage_id=$3
                      AND source.official_stream_id=$2 AND source.lineage_id=$3
                      AND {_model_batch_authorization_sql(self.schema)}
                    FOR UPDATE OF leg
                    """,
                    commit.model_leg_id,
                    commit.official_stream_id,
                    commit.lineage_id,
                )
                if leg is None:
                    raise V20RepositoryError(f"unknown model leg {commit.model_leg_id!r}")
                if leg["seal_status"] != "SEALED" or bool(leg["evaluation_only"]):
                    raise V20StateConflict(
                        "exit notification requires a sealed, non-evaluation model batch"
                    )
                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.exit_intents
                        (exit_intent_id,model_leg_id,event_id,signal_type,trigger_ts,
                         rule_actionable_from,semantic_content_hash,semantic_json,
                         commit_fingerprint,initial_exit_persisted_local_date)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,
                            (clock_timestamp() AT TIME ZONE 'Asia/Shanghai')::date)
                    """,
                    commit.exit_intent_id,
                    commit.model_leg_id,
                    commit.event_id,
                    commit.signal_type,
                    commit.trigger_ts,
                    commit.rule_actionable_from,
                    commit.semantic_content_hash,
                    canonical_json(commit.semantic),
                    commit_fingerprint,
                )
                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.outbox_events
                        (event_id,event_type,route_id,official_stream_id,lineage_id,
                         semantic_content_hash,semantic_json)
                    VALUES ($1,'EXIT_SIGNAL',$2,$3,$4,$5,$6::jsonb)
                    """,
                    commit.event_id,
                    commit.route_id,
                    commit.official_stream_id,
                    commit.lineage_id,
                    commit.semantic_content_hash,
                    canonical_json(commit.semantic),
                )
                return True

    async def record_reminder_stop_ack(
        self,
        original_exit_event_id: str,
        consumer_id: str,
        *,
        official_stream_id: str,
        lineage_id: str,
        ack_ts: datetime,
        auth_evidence_hash: str,
        ack_id: str | None = None,
    ) -> bool:
        """Persist one authenticated stop-ack per exit event and consumer."""
        _require_scope(official_stream_id, lineage_id)
        if not original_exit_event_id or not consumer_id:
            raise ValueError("original_exit_event_id and consumer_id cannot be empty")
        _require_aware(ack_ts, "ack_ts")
        _require_sha256(auth_evidence_hash, "auth_evidence_hash")
        if ack_id is not None and not ack_id:
            raise ValueError("ack_id cannot be empty")
        effective_ack_id = ack_id or (
            "ack:"
            + sha256_json(
                {
                    "profile": "V20_REMINDER_STOP_ACK_ID_V1",
                    "original_exit_event_id": original_exit_event_id,
                    "consumer_id": consumer_id,
                }
            )
        )
        created = False
        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                intent = await connection.fetchrow(
                    f"""
                    SELECT intent.exit_intent_id
                    FROM {self.schema}.exit_intents AS intent
                    JOIN {self.schema}.outbox_events AS outbox
                      ON outbox.event_id=intent.event_id
                    JOIN {self.schema}.model_legs AS leg USING (model_leg_id)
                    JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
                    JOIN {self.schema}.outbox_events AS source
                      ON source.event_id=batch.source_event_id
                    WHERE intent.event_id=$1 AND outbox.event_type='EXIT_SIGNAL'
                      AND batch.official_stream_id=$2 AND batch.lineage_id=$3
                      AND outbox.official_stream_id=$2 AND outbox.lineage_id=$3
                      AND source.official_stream_id=$2 AND source.lineage_id=$3
                      AND source.seal_status='SEALED'
                      AND {_model_batch_authorization_sql(self.schema)}
                    FOR UPDATE OF intent
                    """,
                    original_exit_event_id,
                    official_stream_id,
                    lineage_id,
                )
                if intent is None:
                    raise V20RepositoryError(
                        f"unknown original exit event {original_exit_event_id!r}"
                    )
                existing = await connection.fetchrow(
                    f"""
                    SELECT ack.ack_id,ack.ack_ts,ack.auth_evidence_hash
                    FROM {self.schema}.reminder_stop_acks AS ack
                    JOIN {self.schema}.exit_intents AS intent
                      ON intent.event_id=ack.original_exit_event_id
                    JOIN {self.schema}.model_legs AS leg USING (model_leg_id)
                    JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
                    JOIN {self.schema}.outbox_events AS source
                      ON source.event_id=batch.source_event_id
                    WHERE ack.original_exit_event_id=$1 AND ack.consumer_id=$2
                      AND batch.official_stream_id=$3 AND batch.lineage_id=$4
                      AND source.official_stream_id=$3 AND source.lineage_id=$4
                      AND source.seal_status='SEALED'
                      AND {_model_batch_authorization_sql(self.schema)}
                    """,
                    original_exit_event_id,
                    consumer_id,
                    official_stream_id,
                    lineage_id,
                )
                if existing is not None:
                    if not (
                        existing["ack_id"] == effective_ack_id
                        and existing["ack_ts"] == ack_ts
                        and existing["auth_evidence_hash"] == auth_evidence_hash
                    ):
                        raise V20SemanticConflict(
                            "consumer already acknowledged this exit with different evidence"
                        )
                else:
                    await connection.execute(
                        f"""
                        INSERT INTO {self.schema}.reminder_stop_acks
                            (ack_id,original_exit_event_id,consumer_id,ack_ts,auth_evidence_hash)
                        VALUES ($1,$2,$3,$4,$5)
                        """,
                        effective_ack_id,
                        original_exit_event_id,
                        consumer_id,
                        ack_ts,
                        auth_evidence_hash,
                    )
                    created = True
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    f"""
                    UPDATE {self.schema}.reminder_stop_acks
                    SET receipt_sealed_at=clock_timestamp()
                    WHERE ack_id=$1 AND receipt_sealed_at IS NULL
                    """,
                    effective_ack_id,
                )
        return created

    async def enqueue_due_exit_reminders(
        self,
        reminder_trade_date: date,
        *,
        official_stream_id: str,
        lineage_id: str,
        cutoff: datetime,
        route_id: str,
    ) -> tuple[str, ...]:
        """Create one deterministic reminder/outbox row per due, unacked exit."""
        _require_scope(official_stream_id, lineage_id)
        _require_aware(cutoff, "reminder cutoff")
        if cutoff.astimezone(BEIJING_TZ).date() != reminder_trade_date:
            raise ValueError("reminder cutoff must fall on reminder_trade_date in Asia/Shanghai")
        if not route_id:
            raise ValueError("route_id cannot be empty")
        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                rows = await connection.fetch(
                    f"""
                    SELECT intent.exit_intent_id,intent.event_id AS original_exit_event_id,
                           intent.model_leg_id,intent.signal_type,
                           intent.semantic_content_hash AS original_semantic_content_hash,
                           intent.semantic_json AS original_semantic_json,
                           outbox.payload_json AS original_payload_json
                    FROM {self.schema}.exit_intents AS intent
                    JOIN {self.schema}.outbox_events AS outbox
                      ON outbox.event_id=intent.event_id
                    JOIN {self.schema}.model_legs AS leg USING (model_leg_id)
                    JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
                    JOIN {self.schema}.outbox_events AS source
                      ON source.event_id=batch.source_event_id
                    WHERE intent.initial_exit_persisted_local_date < $1
                      AND outbox.event_type='EXIT_SIGNAL'
                      AND outbox.seal_status='SEALED'
                      AND batch.official_stream_id=$3 AND batch.lineage_id=$4
                      AND outbox.official_stream_id=$3 AND outbox.lineage_id=$4
                      AND source.official_stream_id=$3 AND source.lineage_id=$4
                      AND source.seal_status='SEALED'
                      AND {_model_batch_authorization_sql(self.schema)}
                      AND NOT EXISTS (
                          SELECT 1 FROM {self.schema}.reminder_stop_acks AS ack
                          WHERE ack.original_exit_event_id=intent.event_id
                            AND ack.ack_ts <= $2 AND ack.receipt_sealed_at <= $2
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM {self.schema}.exit_reminders AS reminder
                          WHERE reminder.exit_intent_id=intent.exit_intent_id
                            AND reminder.reminder_trade_date=$1
                      )
                    ORDER BY intent.exit_intent_id
                    FOR UPDATE OF intent SKIP LOCKED
                    """,
                    reminder_trade_date,
                    cutoff,
                    official_stream_id,
                    lineage_id,
                )
                event_ids: list[str] = []
                for row in rows:
                    identity = {
                        "profile": "V20_EXIT_REMINDER_ID_V1",
                        "exit_intent_id": row["exit_intent_id"],
                        "reminder_trade_date": reminder_trade_date.isoformat(),
                    }
                    digest = sha256_json(identity)
                    reminder_id = f"reminder:{digest}"
                    event_id = f"exit-reminder:{digest}"
                    original_semantic = _json_value(row["original_semantic_json"])
                    if sha256_json(original_semantic) != row["original_semantic_content_hash"]:
                        raise V20SemanticConflict(
                            f"original exit semantic hash mismatch: {row['original_exit_event_id']}"
                        )
                    original_payload = _json_value(row["original_payload_json"])
                    actionable_from = original_payload.get("actionable_from")
                    if not actionable_from:
                        raise V20SemanticConflict(
                            "sealed original exit payload lacks actionable_from"
                        )
                    semantic = dict(original_semantic)
                    for runtime_key in (
                        "event_id",
                        "audit_record_id",
                        "generated_at",
                        "durable_commit_marker",
                        "payload_hash",
                        "delivery_status",
                    ):
                        semantic.pop(runtime_key, None)
                    semantic.update(
                        {
                            "event_type": "EXIT_REMINDER",
                            "exit_intent_id": row["exit_intent_id"],
                            "original_exit_event_id": row["original_exit_event_id"],
                            "original_exit_semantic_content_hash": row[
                                "original_semantic_content_hash"
                            ],
                            "model_leg_id": row["model_leg_id"],
                            "exit_signal_type": row["signal_type"],
                            "reminder_trade_date": reminder_trade_date.isoformat(),
                            "event_input_cutoff_ts": cutoff.isoformat(),
                            "actionable_from": actionable_from,
                        }
                    )
                    semantic_hash = sha256_json(semantic)
                    await connection.execute(
                        f"""
                        INSERT INTO {self.schema}.exit_reminders
                            (reminder_id,exit_intent_id,original_exit_event_id,
                             reminder_trade_date,event_id,semantic_content_hash,semantic_json)
                        VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)
                        """,
                        reminder_id,
                        row["exit_intent_id"],
                        row["original_exit_event_id"],
                        reminder_trade_date,
                        event_id,
                        semantic_hash,
                        canonical_json(semantic),
                    )
                    await connection.execute(
                        f"""
                        INSERT INTO {self.schema}.outbox_events
                            (event_id,event_type,route_id,official_stream_id,lineage_id,
                             semantic_content_hash,semantic_json)
                        VALUES ($1,'EXIT_REMINDER',$2,$3,$4,$5,$6::jsonb)
                        """,
                        event_id,
                        route_id,
                        official_stream_id,
                        lineage_id,
                        semantic_hash,
                        canonical_json(semantic),
                    )
                    event_ids.append(event_id)
                return tuple(event_ids)

    async def record_daily_bar_snapshot(
        self,
        trade_date: date,
        payload: Mapping[str, Any],
    ) -> DailyBarSnapshotRecord:
        """Persist one immutable market-wide daily response and seal its receipt.

        ``receipt_sealed_at`` is written only after the candidate row's insert
        transaction has committed.  Fixed-cutoff readers use that conservative
        post-commit receipt, never the timestamp sampled inside the original
        insert transaction.
        """

        normalized = dict(payload)
        if normalized.get("trade_date") != trade_date.isoformat():
            raise ValueError("daily snapshot trade_date does not match payload")
        bars = normalized.get("bars")
        if not isinstance(bars, Mapping):
            raise ValueError("daily snapshot bars must be an object")
        source_hash = sha256_json(normalized)
        snapshot_id = f"daily:{source_hash}"
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.daily_bar_snapshots
                        (snapshot_id,trade_date,source_hash,snapshot_json)
                    VALUES ($1,$2,$3,$4::jsonb)
                    ON CONFLICT DO NOTHING
                    """,
                    snapshot_id,
                    trade_date,
                    source_hash,
                    canonical_json(normalized),
                )
        # This is deliberately a second transaction.  PostgreSQL samples the
        # seal clock only after the candidate insert returned committed, so an
        # application-host clock skew cannot make an uncommitted/late candidate
        # appear eligible at a historical cutoff.
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    f"""
                    UPDATE {self.schema}.daily_bar_snapshots
                    SET receipt_sealed_at=clock_timestamp()
                    WHERE snapshot_id=$1 AND receipt_sealed_at IS NULL
                    """,
                    snapshot_id,
                )
                row = await connection.fetchrow(
                    f"""
                    SELECT snapshot_id,trade_date,source_hash,snapshot_json,
                           receipt_sealed_at,receipt_sequence
                    FROM {self.schema}.daily_bar_snapshots
                    WHERE snapshot_id=$1
                    """,
                    snapshot_id,
                )
        if row is None:
            raise V20RepositoryError("daily snapshot disappeared after insert")
        stored_payload = _json_value(row["snapshot_json"])
        if (
            row["trade_date"] != trade_date
            or row["source_hash"] != source_hash
            or stored_payload != normalized
            or row["receipt_sealed_at"] is None
        ):
            raise V20SemanticConflict("daily snapshot identity/content mismatch")
        return DailyBarSnapshotRecord(
            snapshot_id=str(row["snapshot_id"]),
            trade_date=row["trade_date"],
            source_hash=str(row["source_hash"]),
            payload=stored_payload,
            first_received_at=row["receipt_sealed_at"],
            receipt_sequence=int(row["receipt_sequence"]),
        )

    async def load_latest_daily_bar_snapshot(
        self,
        trade_date: date,
        *,
        received_before: datetime | None = None,
    ) -> DailyBarSnapshotRecord | None:
        """Load the last durably visible daily candidate at a fixed cutoff."""

        if received_before is not None:
            _require_aware(received_before, "received_before")
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                f"""
                SELECT snapshot_id,trade_date,source_hash,snapshot_json,
                       receipt_sealed_at,receipt_sequence
                FROM {self.schema}.daily_bar_snapshots
                WHERE trade_date=$1 AND receipt_sealed_at IS NOT NULL
                  AND ($2::timestamptz IS NULL OR receipt_sealed_at <= $2)
                ORDER BY receipt_sealed_at DESC,receipt_sequence DESC,snapshot_id DESC
                LIMIT 1
                """,
                trade_date,
                received_before,
            )
        if row is None:
            return None
        payload = _json_value(row["snapshot_json"])
        source_hash = str(row["source_hash"])
        if sha256_json(payload) != source_hash:
            raise V20SemanticConflict("daily snapshot payload hash mismatch")
        if payload.get("trade_date") != trade_date.isoformat():
            raise V20SemanticConflict("daily snapshot payload date mismatch")
        return DailyBarSnapshotRecord(
            snapshot_id=str(row["snapshot_id"]),
            trade_date=row["trade_date"],
            source_hash=source_hash,
            payload=payload,
            first_received_at=row["receipt_sealed_at"],
            receipt_sequence=int(row["receipt_sequence"]),
        )

    async def list_daily_bar_snapshots(
        self,
        trade_date: date,
        *,
        received_before: datetime | None = None,
    ) -> tuple[list[DailyBarSnapshotRecord], tuple[str, ...]]:
        """Return all integrity-valid candidates, newest first, plus corrupt IDs."""

        if received_before is not None:
            _require_aware(received_before, "received_before")
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT snapshot_id,trade_date,source_hash,snapshot_json,
                       receipt_sealed_at,receipt_sequence
                FROM {self.schema}.daily_bar_snapshots
                WHERE trade_date=$1 AND receipt_sealed_at IS NOT NULL
                  AND ($2::timestamptz IS NULL OR receipt_sealed_at <= $2)
                ORDER BY receipt_sealed_at DESC,receipt_sequence DESC,snapshot_id DESC
                """,
                trade_date,
                received_before,
            )
        result: list[DailyBarSnapshotRecord] = []
        corrupt_ids: list[str] = []
        for row in rows:
            snapshot_id = str(row["snapshot_id"])
            try:
                payload = _json_value(row["snapshot_json"])
            except (TypeError, ValueError, json.JSONDecodeError):
                corrupt_ids.append(snapshot_id)
                continue
            source_hash = str(row["source_hash"])
            if (
                not isinstance(payload, Mapping)
                or sha256_json(payload) != source_hash
                or payload.get("trade_date") != trade_date.isoformat()
            ):
                corrupt_ids.append(snapshot_id)
                continue
            result.append(
                DailyBarSnapshotRecord(
                    snapshot_id=snapshot_id,
                    trade_date=row["trade_date"],
                    source_hash=source_hash,
                    payload=payload,
                    first_received_at=row["receipt_sealed_at"],
                    receipt_sequence=int(row["receipt_sequence"]),
                )
            )
        return result, tuple(corrupt_ids)

    async def record_minute_bars(self, rows: Sequence[Mapping[str, Any]]) -> frozenset[str]:
        if not rows:
            return frozenset()
        normalized_rows: list[tuple[str, datetime, str, str, Mapping[str, Any]]] = []
        required = {"stock_code", "bar_end", "end_label"}
        for row in rows:
            missing = required - set(row)
            if missing:
                raise ValueError(f"minute bar missing fields: {', '.join(sorted(missing))}")
            code = str(row["stock_code"])
            if not re.fullmatch(r"\d{6}", code):
                raise ValueError(f"invalid minute-bar code: {code!r}")
            raw_bar_end = row["bar_end"]
            bar_end = (
                raw_bar_end
                if isinstance(raw_bar_end, datetime)
                else datetime.fromisoformat(str(raw_bar_end))
            )
            _require_aware(bar_end, "minute bar_end")
            local_bar_end = bar_end.astimezone(BEIJING_TZ)
            end_label = str(row["end_label"])
            if end_label != local_bar_end.strftime("%H:%M"):
                raise ValueError("minute end_label does not match bar_end in Asia/Shanghai")
            if local_bar_end.second != 0 or local_bar_end.microsecond != 0:
                raise ValueError("minute bar_end must be minute-aligned")
            payload = dict(row)
            payload["stock_code"] = code
            payload["bar_end"] = local_bar_end.isoformat()
            payload["end_label"] = end_label
            source_hash = sha256_json(payload)
            normalized_rows.append((code, local_bar_end, end_label, source_hash, payload))
        batch_payload = canonical_json(
            [
                {
                    "code": code,
                    "bar_end": bar_end.isoformat(),
                    "end_label": end_label,
                    "source_hash": source_hash,
                    "bar_json": payload,
                }
                for code, bar_end, end_label, source_hash, payload in normalized_rows
            ]
        )
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.minute_bars
                        (code,bar_end,end_label,source_hash,bar_json)
                    SELECT input.code,input.bar_end,input.end_label,
                           input.source_hash,input.bar_json
                    FROM jsonb_to_recordset($1::jsonb) AS input(
                        code text,bar_end timestamptz,end_label text,
                        source_hash text,bar_json jsonb
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    batch_payload,
                )
        # Seal the receipt only after the immutable source rows are committed.
        # Re-observing an old unsealed row gives it today's conservative receipt
        # instead of pretending it was visible at the historical insert clock.
        # The database clock is authoritative for every PIT cutoff.
        sealed_hashes: set[str] = set()
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    f"""
                    WITH receipt AS MATERIALIZED (
                        SELECT clock_timestamp() AS received_at
                    ), input AS (
                        SELECT * FROM jsonb_to_recordset($1::jsonb) AS item(
                            code text,bar_end timestamptz,end_label text,
                            source_hash text,bar_json jsonb
                        )
                    ), attempted AS (
                        UPDATE {self.schema}.minute_bars AS bar
                        SET receipt_sealed_at=receipt.received_at
                        FROM input,receipt
                        WHERE bar.code=input.code
                          AND bar.bar_end=input.bar_end
                          AND bar.source_hash=input.source_hash
                          AND bar.receipt_sealed_at IS NULL
                          AND receipt.received_at > bar.bar_end
                        RETURNING bar.source_hash
                    ), previously_sealed AS (
                        SELECT DISTINCT bar.source_hash
                        FROM {self.schema}.minute_bars AS bar
                        JOIN input ON bar.code=input.code
                                  AND bar.bar_end=input.bar_end
                                  AND bar.source_hash=input.source_hash
                        WHERE bar.receipt_sealed_at IS NOT NULL
                          AND bar.receipt_sealed_at > bar.bar_end
                    )
                    SELECT source_hash FROM attempted
                    UNION
                    SELECT source_hash FROM previously_sealed
                    """,
                    batch_payload,
                )
                sealed_hashes.update(str(row["source_hash"]) for row in rows)
        return frozenset(sealed_hashes)

    async def list_minute_bars(
        self,
        code: str,
        trade_dates: Sequence[date],
        end_cutoff: datetime,
    ) -> list[MinuteBarRecord]:
        """Load the first durably received *legal* revision per minute label.

        Raw/illegal candidates remain in the immutable audit table.  They do
        not consume the frozen label slot, so a later first legal observation
        can still protect the model leg.  Later legal corrections cannot
        retroactively manufacture or erase an intraday exit trigger.
        """
        if not re.fullmatch(r"\d{6}", code):
            raise ValueError(f"invalid minute-bar code: {code!r}")
        _require_aware(end_cutoff, "end_cutoff")
        unique_dates = sorted(set(trade_dates))
        if not unique_dates:
            return []
        range_start = datetime.combine(unique_dates[0], time.min, tzinfo=BEIJING_TZ)
        range_end = datetime.combine(
            unique_dates[-1] + timedelta(days=1), time.min, tzinfo=BEIJING_TZ
        )
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT code,bar_end,end_label,source_hash,bar_json,
                       receipt_sealed_at AS first_received_at
                FROM {self.schema}.minute_bars
                WHERE code=$1
                  AND (bar_end AT TIME ZONE 'Asia/Shanghai')::date=ANY($2::date[])
                  AND bar_end <= $3
                  AND bar_end >= $4
                  AND bar_end < $5
                  AND receipt_sealed_at IS NOT NULL
                  AND receipt_sealed_at > bar_end
                ORDER BY bar_end,end_label,receipt_sealed_at,source_hash
                """,
                code,
                unique_dates,
                end_cutoff,
                range_start,
                range_end,
            )
        # Exit protection follows the first durable legal observation of each
        # raw label.  Later vendor corrections remain in the immutable inbox
        # but can neither erase an already visible stop nor manufacture a new
        # one.  An illegal first observation does not poison the label forever.
        # A same-receipt tie is resolved by source hash, making bulk inserts
        # deterministic without depending on result order or worker timing.
        selected: dict[tuple[date, str], MinuteBarRecord] = {}
        corrupt_keys: set[tuple[date, str]] = set()
        for row in rows:
            bar_end = row["bar_end"]
            key = (bar_end.astimezone(BEIJING_TZ).date(), str(row["end_label"]))
            try:
                payload = _json_value(row["bar_json"])
            except (TypeError, ValueError, json.JSONDecodeError):
                corrupt_keys.add(key)
                continue
            source_hash = str(row["source_hash"])
            if sha256_json(payload) != source_hash:
                corrupt_keys.add(key)
                continue
            if not _is_legal_complete_minute_payload(
                payload,
                expected_code=code,
                expected_bar_end=bar_end,
                expected_label=str(row["end_label"]),
            ):
                continue
            record = MinuteBarRecord(
                code=row["code"],
                bar_end=bar_end,
                end_label=row["end_label"],
                source_hash=source_hash,
                payload=payload,
                first_received_at=row["first_received_at"],
            )
            previous = selected.get(key)
            if previous is None or (
                record.first_received_at,
                record.source_hash,
            ) < (
                previous.first_received_at,
                previous.source_hash,
            ):
                selected[key] = record
        result = sorted(selected.values(), key=lambda item: (item.bar_end, item.end_label))
        if corrupt_keys:
            raise V20MinuteBarIntegrityConflict(
                f"minute bar integrity failed for {code} at {len(corrupt_keys)} label(s)",
                partial_records=result,
                corrupt_labels=[(code, day, label) for day, label in sorted(corrupt_keys)],
            )
        return result

    async def list_raw_minute_bar_records(
        self,
        codes: Sequence[str],
        *,
        trade_date: date,
        end_labels: Sequence[str],
        received_before: datetime | None = None,
    ) -> list[MinuteBarRecord]:
        """Load every immutable revision for a bounded code/label set.

        Unlike :meth:`list_minute_bars`, this method intentionally does not
        collapse or reject conflicting revisions.  Entry/reference collectors
        ingest every returned revision so a process restart cannot forget a
        conflict that was persisted by the previous process.
        """

        normalized_codes = tuple(sorted(set(codes)))
        normalized_labels = tuple(sorted(set(end_labels)))
        if not normalized_codes or not normalized_labels:
            return []
        if len(normalized_codes) > 10_000:
            raise ValueError("raw minute-bar query exceeds 10000 codes")
        if any(not re.fullmatch(r"\d{6}", code) for code in normalized_codes):
            raise ValueError("raw minute-bar query contains an invalid code")
        if any(not re.fullmatch(r"\d{2}:\d{2}", label) for label in normalized_labels):
            raise ValueError("raw minute-bar query contains an invalid end label")
        if received_before is not None:
            _require_aware(received_before, "received_before")
        range_start = datetime.combine(trade_date, time.min, tzinfo=BEIJING_TZ)
        range_end = datetime.combine(trade_date + timedelta(days=1), time.min, tzinfo=BEIJING_TZ)
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT code,bar_end,end_label,source_hash,bar_json,
                       receipt_sealed_at AS first_received_at
                FROM {self.schema}.minute_bars
                WHERE code=ANY($1::text[])
                  AND bar_end >= $2
                  AND bar_end < $3
                  AND (bar_end AT TIME ZONE 'Asia/Shanghai')::date=$4
                  AND end_label=ANY($5::text[])
                  AND receipt_sealed_at IS NOT NULL
                  AND receipt_sealed_at > bar_end
                  AND ($6::timestamptz IS NULL OR receipt_sealed_at < $6)
                ORDER BY code,bar_end,end_label,receipt_sealed_at,source_hash
                """,
                list(normalized_codes),
                range_start,
                range_end,
                trade_date,
                list(normalized_labels),
                received_before,
            )
        by_key: dict[tuple[str, date, str], list[MinuteBarRecord]] = {}
        corrupt_keys: set[tuple[str, date, str]] = set()
        for row in rows:
            bar_end = row["bar_end"]
            key = (
                str(row["code"]),
                bar_end.astimezone(BEIJING_TZ).date(),
                str(row["end_label"]),
            )
            try:
                payload = _json_value(row["bar_json"])
            except (TypeError, ValueError, json.JSONDecodeError):
                corrupt_keys.add(key)
                continue
            source_hash = str(row["source_hash"])
            if not isinstance(payload, Mapping) or sha256_json(payload) != source_hash:
                corrupt_keys.add(key)
                continue
            by_key.setdefault(key, []).append(
                MinuteBarRecord(
                    code=row["code"],
                    bar_end=bar_end,
                    end_label=row["end_label"],
                    source_hash=source_hash,
                    payload=payload,
                    first_received_at=row["first_received_at"],
                )
            )
        result = [record for key in sorted(by_key) for record in by_key[key]]
        if corrupt_keys:
            raise V20MinuteBarIntegrityConflict(
                f"raw minute-bar integrity failed at {len(corrupt_keys)} identity/label(s)",
                partial_records=result,
                corrupt_labels=sorted(corrupt_keys),
            )
        return result

    async def record_exit_scan_watermark(
        self,
        model_leg_id: str,
        *,
        trade_date: date,
        scanned_through_label: str,
        source_hash: str,
        official_stream_id: str,
        lineage_id: str,
    ) -> bool:
        """Persist a monotonic proof that one leg/day source window was scanned."""

        _require_scope(official_stream_id, lineage_id)
        if not re.fullmatch(r"\d{2}:\d{2}", scanned_through_label):
            raise ValueError("invalid exit scan watermark label")
        try:
            time.fromisoformat(scanned_through_label)
        except ValueError as exc:
            raise ValueError("invalid exit scan watermark label") from exc
        _require_sha256(source_hash, "source_hash")
        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="serializable"):
                leg = await connection.fetchrow(
                    f"""
                    SELECT model_leg.d1,model_leg.d2
                    FROM {self.schema}.model_legs AS model_leg
                    JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
                    JOIN {self.schema}.outbox_events AS source
                      ON source.event_id=batch.source_event_id
                    WHERE model_leg.model_leg_id=$1
                      AND batch.official_stream_id=$2 AND batch.lineage_id=$3
                      AND source.official_stream_id=$2 AND source.lineage_id=$3
                      AND source.seal_status='SEALED'
                      AND {_model_batch_authorization_sql(self.schema)}
                    FOR UPDATE OF model_leg
                    """,
                    model_leg_id,
                    official_stream_id,
                    lineage_id,
                )
                if leg is None:
                    raise V20RepositoryError("unknown model leg in scoped exit scan")
                if trade_date not in {leg["d1"], leg["d2"]}:
                    raise V20SemanticConflict("exit scan date is not the model leg D1/D2")
                current = await connection.fetchrow(
                    f"""
                    SELECT scanned_through_label,source_hash
                    FROM {self.schema}.exit_scan_watermarks
                    WHERE model_leg_id=$1 AND trade_date=$2
                    FOR UPDATE
                    """,
                    model_leg_id,
                    trade_date,
                )
                if current is not None:
                    current_label = str(current["scanned_through_label"])
                    if scanned_through_label < current_label:
                        return False
                    if scanned_through_label == current_label:
                        if current["source_hash"] == source_hash:
                            return False
                        raise V20SemanticConflict(
                            "same exit scan watermark has different source evidence"
                        )
                    updated = await connection.execute(
                        f"""
                        UPDATE {self.schema}.exit_scan_watermarks
                        SET scanned_through_label=$1,source_hash=$2,
                            updated_at=clock_timestamp()
                        WHERE model_leg_id=$3 AND trade_date=$4
                          AND scanned_through_label=$5
                        """,
                        scanned_through_label,
                        source_hash,
                        model_leg_id,
                        trade_date,
                        current_label,
                    )
                    if updated != "UPDATE 1":
                        raise V20StateConflict("exit scan watermark CAS lost")
                    return True
                await connection.execute(
                    f"""
                    INSERT INTO {self.schema}.exit_scan_watermarks
                        (model_leg_id,trade_date,scanned_through_label,source_hash)
                    VALUES ($1,$2,$3,$4)
                    """,
                    model_leg_id,
                    trade_date,
                    scanned_through_label,
                    source_hash,
                )
                return True

    async def get_exit_scan_watermarks(
        self,
        model_leg_id: str,
        *,
        official_stream_id: str,
        lineage_id: str,
    ) -> Mapping[date, str]:
        _require_scope(official_stream_id, lineage_id)
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT watermark.trade_date,watermark.scanned_through_label
                FROM {self.schema}.exit_scan_watermarks AS watermark
                JOIN {self.schema}.model_legs AS model_leg USING (model_leg_id)
                JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
                JOIN {self.schema}.outbox_events AS source
                  ON source.event_id=batch.source_event_id
                WHERE watermark.model_leg_id=$1
                  AND batch.official_stream_id=$2 AND batch.lineage_id=$3
                  AND source.official_stream_id=$2 AND source.lineage_id=$3
                  AND source.seal_status='SEALED'
                  AND {_model_batch_authorization_sql(self.schema)}
                ORDER BY watermark.trade_date
                """,
                model_leg_id,
                official_stream_id,
                lineage_id,
            )
        return {row["trade_date"]: str(row["scanned_through_label"]).strip() for row in rows}

    async def save_rolling7_market_health(
        self,
        batch: Rolling7Batch,
        *,
        updated_at: datetime,
    ) -> Rolling7MarketHealthRecord:
        _require_aware(updated_at, "rolling7 updated_at")
        recommendations = [{"rank": item.rank, "code": item.code} for item in batch.recommendations]
        references = {
            leg.code: leg.d0_reference for leg in batch.legs if leg.d0_reference is not None
        }
        closes = {leg.code: leg.d2_close for leg in batch.legs if leg.d2_close is not None}
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                f"""
                INSERT INTO {self.schema}.rolling7_market_health (
                    signal_date,canonical_available,canonical_snapshot_id,
                    canonical_snapshot_hash,signal_kind,recommendations,t2_date,
                    d0_references,d2_closes,batch_return,status,reason,updated_at
                )
                VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8::jsonb,$9::jsonb,$10,$11,$12,$13)
                ON CONFLICT (signal_date) DO UPDATE SET
                    canonical_available=EXCLUDED.canonical_available,
                    canonical_snapshot_id=EXCLUDED.canonical_snapshot_id,
                    canonical_snapshot_hash=EXCLUDED.canonical_snapshot_hash,
                    signal_kind=EXCLUDED.signal_kind,
                    recommendations=EXCLUDED.recommendations,
                    t2_date=EXCLUDED.t2_date,
                    d0_references=EXCLUDED.d0_references,
                    d2_closes=EXCLUDED.d2_closes,
                    batch_return=EXCLUDED.batch_return,
                    status=EXCLUDED.status,
                    reason=EXCLUDED.reason,
                    updated_at=EXCLUDED.updated_at
                WHERE (
                    -- Exact replay is idempotent, including a completed row.
                    (
                      rolling7_market_health.canonical_available
                        IS NOT DISTINCT FROM EXCLUDED.canonical_available
                      AND rolling7_market_health.canonical_snapshot_id=
                        EXCLUDED.canonical_snapshot_id
                      AND rolling7_market_health.canonical_snapshot_hash
                        IS NOT DISTINCT FROM EXCLUDED.canonical_snapshot_hash
                      AND rolling7_market_health.signal_kind=EXCLUDED.signal_kind
                      AND rolling7_market_health.recommendations=
                        EXCLUDED.recommendations
                      AND rolling7_market_health.t2_date IS NOT DISTINCT FROM
                        EXCLUDED.t2_date
                      AND rolling7_market_health.d0_references=EXCLUDED.d0_references
                      AND rolling7_market_health.d2_closes=EXCLUDED.d2_closes
                      AND rolling7_market_health.batch_return IS NOT DISTINCT FROM
                        EXCLUDED.batch_return
                      AND rolling7_market_health.status=EXCLUDED.status
                      AND rolling7_market_health.reason=EXCLUDED.reason
                    )
                    OR
                    -- A missing placeholder may fill its undetermined T2 once.
                    (
                      rolling7_market_health.canonical_available=false
                      AND EXCLUDED.canonical_available=false
                      AND rolling7_market_health.d0_references
                        IS NOT DISTINCT FROM EXCLUDED.d0_references
                      AND rolling7_market_health.d2_closes
                        IS NOT DISTINCT FROM EXCLUDED.d2_closes
                      AND rolling7_market_health.batch_return
                        IS NOT DISTINCT FROM EXCLUDED.batch_return
                      AND rolling7_market_health.status=EXCLUDED.status
                      AND rolling7_market_health.reason=EXCLUDED.reason
                      AND rolling7_market_health.t2_date IS NULL
                      AND EXCLUDED.t2_date IS NOT NULL
                    )
                    -- A missing placeholder may become any known canonical row; its
                    -- old empty identity and recommendations are not authoritative.
                    -- NO_SIGNAL is complete and intentionally discards placeholder T2.
                    OR (
                      rolling7_market_health.canonical_available=false
                      AND EXCLUDED.canonical_available=true
                      AND (
                        EXCLUDED.signal_kind='NO_SIGNAL'
                        OR rolling7_market_health.t2_date IS NULL
                        OR rolling7_market_health.t2_date=EXCLUDED.t2_date
                      )
                    )
                    -- A known canonical gap may only retain or first establish T2,
                    -- add evidence monotonically, and then complete.
                    OR (
                      rolling7_market_health.canonical_available=true
                      AND EXCLUDED.canonical_available=true
                      AND rolling7_market_health.status='DATA_GAP'
                      AND rolling7_market_health.canonical_snapshot_id=
                        EXCLUDED.canonical_snapshot_id
                      AND rolling7_market_health.canonical_snapshot_hash=
                        EXCLUDED.canonical_snapshot_hash
                      AND rolling7_market_health.signal_kind=EXCLUDED.signal_kind
                      AND rolling7_market_health.recommendations=
                        EXCLUDED.recommendations
                      AND (
                        rolling7_market_health.t2_date IS NOT DISTINCT FROM
                          EXCLUDED.t2_date
                        OR (
                          rolling7_market_health.t2_date IS NULL
                          AND EXCLUDED.t2_date IS NOT NULL
                        )
                      )
                      AND EXCLUDED.d0_references @>
                        rolling7_market_health.d0_references
                      AND EXCLUDED.d2_closes @>
                        rolling7_market_health.d2_closes
                      AND (
                        EXCLUDED.status='COMPLETE'
                        OR (
                          EXCLUDED.status='DATA_GAP'
                          AND (
                            (
                              rolling7_market_health.t2_date IS NULL
                              AND EXCLUDED.t2_date IS NOT NULL
                            )
                            OR rolling7_market_health.d0_references
                              <> EXCLUDED.d0_references
                            OR rolling7_market_health.d2_closes
                              <> EXCLUDED.d2_closes
                          )
                        )
                      )
                    )
                  )
                RETURNING *
                """,
                batch.signal_date,
                batch.canonical_available,
                batch.canonical_snapshot_id,
                batch.canonical_snapshot_hash,
                batch.signal_kind.value,
                canonical_json(recommendations),
                batch.t2_date,
                canonical_json(references),
                canonical_json(closes),
                batch.batch_return,
                batch.status.value,
                batch.reason,
                updated_at,
            )
            if row is None:
                row = await connection.fetchrow(
                    f"""
                    SELECT * FROM {self.schema}.rolling7_market_health
                    WHERE signal_date=$1
                    """,
                    batch.signal_date,
                )
        if row is None:
            raise V20StateConflict("rolling7 market health row disappeared")
        existing = self._rolling7_market_health_from_row(row)
        if existing.batch != batch:
            raise V20SemanticConflict(
                f"rolling7 market health row for {batch.signal_date.isoformat()} exists differently"
            )
        return existing

    async def load_rolling7_market_health(
        self,
        *,
        before_t2: date,
        limit: int = 1000,
    ) -> tuple[Rolling7Batch, ...]:
        if type(before_t2) is not date:
            raise ValueError("before_t2 must be a date")
        if type(limit) is not int or limit < 1:
            raise ValueError("limit must be a positive integer")
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                f"""
                SELECT recent.*
                FROM (
                    SELECT * FROM {self.schema}.rolling7_market_health
                    WHERE t2_date IS NOT NULL AND t2_date < $1
                    ORDER BY signal_date DESC
                    LIMIT $2
                ) AS recent
                ORDER BY recent.signal_date ASC
                """,
                before_t2,
                limit,
            )
        return tuple(self._rolling7_market_health_from_row(row).batch for row in rows)

    async def get_rolling7_market_health_for_date(
        self,
        signal_date: date,
    ) -> Rolling7Batch | None:
        """Load the single durable market-health fact for ``signal_date``."""

        if type(signal_date) is not date:
            raise ValueError("signal_date must be a date")
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                f"""
                SELECT * FROM {self.schema}.rolling7_market_health
                WHERE signal_date=$1
                """,
                signal_date,
            )
        if row is None:
            return None
        return self._rolling7_market_health_from_row(row).batch

    @staticmethod
    def _rolling7_market_health_from_row(row: asyncpg.Record) -> Rolling7MarketHealthRecord:
        signal_date = row["signal_date"]
        t2_date = row["t2_date"]
        if not isinstance(signal_date, date):
            raise V20SemanticConflict("rolling7 signal_date is invalid")
        if t2_date is not None and not isinstance(t2_date, date):
            raise V20SemanticConflict("rolling7 t2_date is invalid")
        try:
            kind = SignalKind(row["signal_kind"])
            status = BatchStatus(row["status"])
        except ValueError as error:
            raise V20SemanticConflict("rolling7 enum value is invalid") from error

        canonical_available = row["canonical_available"]
        snapshot_id = row["canonical_snapshot_id"]
        snapshot_hash = row["canonical_snapshot_hash"]
        recommendations_raw = _json_value(row["recommendations"])
        if not isinstance(recommendations_raw, list):
            raise V20SemanticConflict("rolling7 recommendations are invalid")
        try:
            recommendations = tuple(
                CanonicalRecommendation(
                    rank=int(item["rank"]),
                    code=str(item["code"]),
                )
                for item in recommendations_raw
            )
        except (KeyError, TypeError, ValueError) as error:
            raise V20SemanticConflict("rolling7 recommendations are invalid") from error

        references_raw = _rolling7_market_health_evidence(_json_value(row["d0_references"]))
        closes_raw = _rolling7_market_health_evidence(_json_value(row["d2_closes"]))
        try:
            legs = tuple(
                Rolling7Leg(
                    rank=item.rank,
                    code=item.code,
                    d0_reference=(
                        float(references_raw[item.code]) if item.code in references_raw else None
                    ),
                    d2_close=float(closes_raw[item.code]) if item.code in closes_raw else None,
                )
                for item in recommendations
            )
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            raise V20SemanticConflict("rolling7 market evidence is invalid") from error
        batch_return = _optional_finite_float(row["batch_return"], "rolling7 batch_return")

        if kind is SignalKind.MISSING_CANONICAL:
            valid = (
                canonical_available is False
                and snapshot_id == ""
                and snapshot_hash == ""
                and not recommendations
                and not legs
                and status is BatchStatus.DATA_GAP
                and row["reason"] == "MISSING_CANONICAL"
                and batch_return is None
            )
        else:
            valid = (
                canonical_available is True
                and bool(snapshot_id)
                and _SHA256.fullmatch(snapshot_hash) is not None
                and (bool(recommendations) == (kind is SignalKind.SIGNAL))
            )
            if valid and kind is SignalKind.SIGNAL:
                valid = (status is BatchStatus.COMPLETE or status is BatchStatus.DATA_GAP) and (
                    status is not BatchStatus.COMPLETE
                    or (
                        t2_date is not None
                        and t2_date > signal_date
                        and len(legs) == len(recommendations)
                        and all(
                            leg.d0_reference is not None
                            and leg.d2_close is not None
                            and leg.d0_reference > 0
                            and leg.d2_close > 0
                            and math.isfinite(leg.d0_reference)
                            and math.isfinite(leg.d2_close)
                            for leg in legs
                        )
                        and batch_return is not None
                    )
                )
            elif valid and kind is SignalKind.NO_SIGNAL:
                valid = (
                    not recommendations
                    and t2_date is None
                    and not legs
                    and status is BatchStatus.COMPLETE
                    and row["reason"] == "NO_SIGNAL"
                    and batch_return is None
                )
        if valid and status is BatchStatus.DATA_GAP and batch_return is not None:
            valid = False
        if not valid:
            raise V20SemanticConflict("rolling7 market health row is semantically invalid")

        return Rolling7MarketHealthRecord(
            batch=Rolling7Batch(
                signal_date=signal_date,
                canonical_snapshot_id=snapshot_id,
                canonical_snapshot_hash=snapshot_hash,
                canonical_available=canonical_available,
                signal_kind=kind,
                recommendations=recommendations,
                t2_date=t2_date,
                legs=legs,
                status=status,
                reason=row["reason"],
                batch_return=batch_return,
            ),
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _shadow_from_row(row: asyncpg.Record) -> ShadowBatchRecord:
        prices = _optional_json_value(row["reference_prices_json"])
        return ShadowBatchRecord(
            batch_id=row["batch_id"],
            decision_id=row["decision_id"],
            kind=row["kind"],
            signal_date=row["signal_date"],
            t2_date=row["t2_date"],
            status=row["status"],
            payload=_json_value(row["batch_json"]),
            batch_return=_optional_finite_float(row["batch_return"], "shadow batch_return"),
            reference_status=row["reference_status"],
            reference_prices=prices,
            reference_snapshot_hash=row["reference_snapshot_hash"],
        )

    @staticmethod
    def _outbox_from_row(row: asyncpg.Record) -> OutboxRecord:
        return OutboxRecord(
            event_id=row["event_id"],
            event_type=row["event_type"],
            route_id=row["route_id"],
            official_stream_id=row["official_stream_id"],
            lineage_id=row["lineage_id"],
            semantic=_json_value(row["semantic_json"]),
            semantic_content_hash=row["semantic_content_hash"],
            payload=_json_value(row["payload_json"]) if row["payload_json"] is not None else None,
            payload_hash=row["payload_hash"],
            generated_at=row["generated_at"],
            commit_marker=int(row["commit_marker"]) if row["commit_marker"] is not None else None,
            action_expiry_ts=row["action_expiry_ts"],
            delivery_status=row["delivery_status"],
            attempt_count=int(row["attempt_count"]),
            lease_db_ts=row.get("lease_db_ts"),
        )

    @staticmethod
    def _verify_outbox_integrity(record: OutboxRecord) -> None:
        try:
            _require_outbox_scope(
                record.route_id,
                record.official_stream_id,
                record.lineage_id,
            )
        except ValueError as exc:
            raise V20SemanticConflict(f"outbox scope is invalid for {record.event_id}") from exc
        if sha256_json(record.semantic) != record.semantic_content_hash:
            raise V20SemanticConflict(f"outbox semantic hash mismatch for {record.event_id}")
        if record.lease_db_ts is not None and (
            record.lease_db_ts.tzinfo is None or record.lease_db_ts.utcoffset() is None
        ):
            raise V20SemanticConflict(f"outbox lease clock is naive for {record.event_id}")
        if record.payload is None:
            if record.payload_hash is not None:
                raise V20SemanticConflict(f"outbox payload/hash mismatch for {record.event_id}")
            return
        if record.payload_hash is None or sha256_json(record.payload) != record.payload_hash:
            raise V20SemanticConflict(f"outbox payload hash mismatch for {record.event_id}")


def create_v20_repository_from_config(
    config_path: str | Path = "config/database-config.yaml",
) -> V20Repository:
    """Create the V20 ledger repository from ``database.v20`` configuration."""
    from src.common.config import load_config

    config = load_config(config_path)
    raw = config.get_dict("database.v20", {})
    if not raw:
        raise ValueError(f"V20 database configuration not found in {config_path}")

    def resolve_env(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        match = _ENV_VALUE.fullmatch(value)
        if match is None:
            if value.startswith("${"):
                raise ValueError(f"invalid environment placeholder: {value!r}")
            return value
        variable, default = match.groups()
        return os.environ.get(variable, default or "")

    repository_config = V20DatabaseConfig(
        host=str(resolve_env(raw.get("host", "localhost"))),
        port=int(resolve_env(raw.get("port", 5432))),
        database=str(resolve_env(raw.get("database", "messages"))),
        user=str(resolve_env(raw.get("user", "v20_writer"))),
        password=str(resolve_env(raw.get("password", ""))),
        schema=str(resolve_env(raw.get("schema", "v20"))),
        pool_min_size=int(resolve_env(raw.get("pool_min_size", 1))),
        pool_max_size=int(resolve_env(raw.get("pool_max_size", 8))),
        ssl_mode=str(resolve_env(raw.get("ssl_mode", "verify-full"))),
        ssl_root_cert=str(resolve_env(raw.get("ssl_root_cert", ""))),
        ssl_root_cert_sha256=str(resolve_env(raw.get("ssl_root_cert_sha256", ""))),
        connect_timeout_seconds=float(resolve_env(raw.get("connect_timeout_seconds", 5))),
        command_timeout_seconds=float(resolve_env(raw.get("command_timeout_seconds", 15))),
    )
    return V20Repository(repository_config)


def create_embedded_v20_repository_from_config(
    config_path: str | Path = "config/database-config.yaml",
    *,
    shared_pool: Any | None = None,
) -> V20Repository:
    """Create a V20 ledger beside the legacy main runtime.

    Embedded V20 deliberately mirrors ``database.trading``'s endpoint,
    principal, and transport semantics for its fallback pool, and always owns
    an isolated ``v20`` schema.  It may instead borrow main's connected
    fundamentals pool without taking lifecycle ownership.  The dedicated V20
    factory above never falls back to this profile.
    """
    from src.common.config import load_config

    config = load_config(config_path)
    trading_raw = config.get_dict("database.trading", {})
    if not trading_raw:
        raise ValueError(f"embedded V20 requires database.trading in {config_path}")

    def resolve_env(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        match = _ENV_VALUE.fullmatch(value)
        if match is None:
            if value.startswith("${"):
                raise ValueError(f"invalid environment placeholder: {value!r}")
            return value
        variable, default = match.groups()
        return os.environ.get(variable, default or "")

    connection_raw = trading_raw
    if shared_pool is not None:
        fundamentals_raw = config.get_dict("database.fundamentals", {})
        if not fundamentals_raw:
            raise ValueError(f"shared embedded V20 requires database.fundamentals in {config_path}")
        identity_fields = ("host", "port", "database", "user", "password")
        trading_identity = {
            field: resolve_env(trading_raw.get(field, "")) for field in identity_fields
        }
        fundamentals_identity = {
            field: resolve_env(fundamentals_raw.get(field, "")) for field in identity_fields
        }
        if trading_identity != fundamentals_identity:
            raise ValueError("shared embedded V20 database identities disagree")
        connection_raw = fundamentals_raw

    repository_config = V20DatabaseConfig(
        host=str(resolve_env(connection_raw.get("host", "localhost"))),
        port=int(resolve_env(connection_raw.get("port", 5432))),
        database=str(resolve_env(connection_raw.get("database", "messages"))),
        user=str(resolve_env(connection_raw.get("user", "reader"))),
        password=str(resolve_env(connection_raw.get("password", ""))),
        schema="v20",
        pool_min_size=1,
        pool_max_size=8,
        ssl_mode=str(
            resolve_env(
                connection_raw.get(
                    "ssl_mode",
                    "disable",
                )
            )
        ),
        ssl_root_cert=str(resolve_env(connection_raw.get("ssl_root_cert", ""))),
        ssl_root_cert_sha256=str(resolve_env(connection_raw.get("ssl_root_cert_sha256", ""))),
        connect_timeout_seconds=float(
            resolve_env(connection_raw.get("connect_timeout_seconds", 5))
        ),
        command_timeout_seconds=float(
            resolve_env(connection_raw.get("command_timeout_seconds", 15))
        ),
        connection_profile="legacy_embedded",
    )
    return V20Repository(repository_config, shared_pool=shared_pool)


__all__ = [
    "ActiveModelLeg",
    "DeliveryAttempt",
    "EntryCommit",
    "EntryStatus",
    "ExitCommit",
    "ManualMonitorEnrollmentCommit",
    "ManualMonitorEnrollmentRecord",
    "MinuteBarRecord",
    "ModelBatchWrite",
    "ModelLegWrite",
    "OutboxRecord",
    "PendingReferenceLeg",
    "Rolling7MarketHealthRecord",
    "SelectedMewsRecord",
    "ShadowBatchRecord",
    "ShadowBatchWrite",
    "StateRecord",
    "V20DatabaseConfig",
    "V20EntryDeadlineExceeded",
    "V20LeadershipLost",
    "V20MinuteBarIntegrityConflict",
    "V20Repository",
    "V20RepositoryError",
    "V20SemanticConflict",
    "V20StateConflict",
    "canonical_json",
    "create_embedded_v20_repository_from_config",
    "create_v20_repository_from_config",
    "migration_sql",
    "sha256_json",
]
