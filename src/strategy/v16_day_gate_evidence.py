"""Append-only local evidence records for the V16 day gate.

This module has no network, model, or trading dependencies.  It serializes a
frozen gate input/decision pair plus caller-owned snapshot data, hashes the
canonical JSON, and appends it beneath a caller-selected base directory.

Writes are completed and fsynced in a same-directory unique temporary file,
then published with a no-overwrite hard link.  Readers therefore never see an
empty or partially written final record.  Callers intentionally own fail-soft
behaviour: validation, filesystem, and serialization errors are allowed to
propagate.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import re
import uuid
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from src.strategy.v16_day_gate import (
    INPUT_SCHEMA_VERSION,
    OUTPUT_SCHEMA_VERSION,
    GateMode,
    GateReason,
    GateState,
    V16DayGateDecision,
    V16DayGateInput,
    V16DayGateMetrics,
)

EVIDENCE_SCHEMA_VERSION = "v16-day-gate-shadow-evidence/v1"
EVIDENCE_RECORD_TYPE = "v16_day_gate_shadow_evidence"
HASH_ALGORITHM = "sha256"

_CONTENT_HASH_KEY = "content_sha256"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TOP_LEVEL_KEYS = {
    "schema_version",
    "record_type",
    "hash_algorithm",
    "evaluated_at",
    "versions",
    "gate_input",
    "gate_decision",
    "frozen_snapshot",
    _CONTENT_HASH_KEY,
}
_VERSION_KEYS = {"scanner", "model", "taxonomy", "policy"}
_GATE_INPUT_KEYS = {field.name for field in fields(V16DayGateInput)}
_GATE_DECISION_KEYS = {field.name for field in fields(V16DayGateDecision)}
_GATE_METRICS_KEYS = {field.name for field in fields(V16DayGateMetrics)}


class EvidenceValidationError(ValueError):
    """Raised when an evidence record violates its schema or content hash."""


class EvidencePathError(ValueError):
    """Raised when an evidence path is outside its configured base directory."""


class EvidenceCollisionError(FileExistsError):
    """Raised for the improbable case of a filename-prefix hash collision."""


def build_v16_day_gate_evidence(
    *,
    gate_input: V16DayGateInput,
    decision: V16DayGateDecision,
    frozen_snapshot: Mapping[str, object],
    evaluated_at: datetime,
    scanner_version: str,
    model_version: str,
    taxonomy_version: str | None,
    policy_version: str | None,
) -> dict[str, Any]:
    """Build and validate one JSON-safe, content-addressed evidence record."""

    _require_aware_datetime(evaluated_at, "evaluated_at")
    _require_aware_datetime(gate_input.cutoff_ts, "gate_input.cutoff_ts")
    if evaluated_at < gate_input.cutoff_ts:
        raise EvidenceValidationError("evaluated_at must not precede gate_input.cutoff_ts")

    _require_version(scanner_version, "scanner_version")
    _require_version(model_version, "model_version")
    _require_optional_version(taxonomy_version, "taxonomy_version")
    _require_optional_version(policy_version, "policy_version")
    _require_matching_versions(
        gate_input=gate_input,
        decision=decision,
        model_version=model_version,
        taxonomy_version=taxonomy_version,
        policy_version=policy_version,
    )

    snapshot_json = _to_json_value(frozen_snapshot)
    if not isinstance(snapshot_json, dict):
        raise TypeError("frozen_snapshot must serialize to a JSON object")

    body: dict[str, Any] = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "record_type": EVIDENCE_RECORD_TYPE,
        "hash_algorithm": HASH_ALGORITHM,
        "evaluated_at": _datetime_to_json(evaluated_at),
        "versions": {
            "scanner": scanner_version,
            "model": model_version,
            "taxonomy": taxonomy_version,
            "policy": policy_version,
        },
        "gate_input": _to_json_value(gate_input),
        "gate_decision": _to_json_value(decision),
        "frozen_snapshot": snapshot_json,
    }
    body[_CONTENT_HASH_KEY] = compute_v16_day_gate_evidence_hash(body)
    return validate_v16_day_gate_evidence(body)


def compute_v16_day_gate_evidence_hash(record: Mapping[str, object]) -> str:
    """Return SHA-256 over canonical JSON, excluding ``content_sha256`` itself."""

    normalized = _to_json_value(record)
    if not isinstance(normalized, dict):
        raise TypeError("evidence record must serialize to a JSON object")
    hash_body = dict(normalized)
    hash_body.pop(_CONTENT_HASH_KEY, None)
    return hashlib.sha256(_canonical_json_bytes(hash_body)).hexdigest()


def append_v16_day_gate_evidence(
    base_dir: str | Path,
    record: Mapping[str, object],
) -> Path:
    """Append one record below ``base_dir`` using atomic no-overwrite publication.

    The layout is ``<base>/<YYYYMMDD>/<HHMMSS>_<hash12>.json``.  Re-appending
    the exact same record is idempotent.  A different record that somehow
    collides on the timestamp and first twelve hash characters raises rather
    than overwriting the existing file.
    """

    normalized = validate_v16_day_gate_evidence(record)
    base = _resolve_base(base_dir, create=True)
    target = _record_path(base, normalized, create_date_dir=True)
    payload = _canonical_json_bytes(normalized) + b"\n"

    temp = _resolve_inside_base(
        base,
        target.parent / f".{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp",
        strict=False,
    )
    try:
        with temp.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())

        try:
            # Same-directory hard-link publication is atomic and refuses to
            # overwrite a record another process has already published.
            os.link(temp, target)
        except FileExistsError as exc:
            existing = read_v16_day_gate_evidence(base, target)
            if existing == normalized:
                return target
            raise EvidenceCollisionError(
                f"evidence path already holds different content: {target}"
            ) from exc
    finally:
        temp.unlink(missing_ok=True)

    return target


def read_v16_day_gate_evidence(
    base_dir: str | Path,
    path: str | Path,
) -> dict[str, Any]:
    """Read and validate one evidence file without permitting base escape."""

    base = _resolve_base(base_dir, create=False)
    supplied = Path(path)
    candidate = supplied if supplied.is_absolute() else base / supplied
    candidate = _resolve_inside_base(base, candidate, strict=True)
    if not candidate.is_file():
        raise IsADirectoryError(f"evidence path is not a file: {candidate}")

    with candidate.open("r", encoding="utf-8") as handle:
        parsed = json.load(handle)
    record = validate_v16_day_gate_evidence(parsed)

    expected = _record_path(base, record, create_date_dir=False)
    if candidate != expected:
        raise EvidencePathError(
            f"evidence file does not match schema path; expected {expected}, got {candidate}"
        )
    return record


def validate_v16_day_gate_evidence(record: Mapping[str, object]) -> dict[str, Any]:
    """Validate schema, provenance consistency, and content hash.

    The returned object is a detached JSON-safe representation suitable for
    canonical serialization.  Validation never repairs or silently drops data.
    """

    normalized = _to_json_value(record)
    if not isinstance(normalized, dict):
        raise EvidenceValidationError("evidence record must be a JSON object")
    _require_exact_keys(normalized, _TOP_LEVEL_KEYS, "evidence record")

    if normalized["schema_version"] != EVIDENCE_SCHEMA_VERSION:
        raise EvidenceValidationError(
            f"unsupported evidence schema {normalized['schema_version']!r}; "
            f"expected {EVIDENCE_SCHEMA_VERSION!r}"
        )
    if normalized["record_type"] != EVIDENCE_RECORD_TYPE:
        raise EvidenceValidationError(f"unsupported record_type {normalized['record_type']!r}")
    if normalized["hash_algorithm"] != HASH_ALGORITHM:
        raise EvidenceValidationError(
            f"unsupported hash_algorithm {normalized['hash_algorithm']!r}"
        )

    evaluated_at = _parse_aware_datetime(normalized["evaluated_at"], "evaluated_at")
    versions = _require_dict(normalized["versions"], "versions")
    _require_exact_keys(versions, _VERSION_KEYS, "versions")
    _require_json_version(versions["scanner"], "versions.scanner")
    model_version = _require_json_version(versions["model"], "versions.model")
    taxonomy_version = _require_json_optional_version(versions["taxonomy"], "versions.taxonomy")
    policy_version = _require_json_optional_version(versions["policy"], "versions.policy")
    gate_input = _require_dict(normalized["gate_input"], "gate_input")
    _require_exact_keys(gate_input, _GATE_INPUT_KEYS, "gate_input")
    _validate_gate_input_fields(gate_input)
    if gate_input["schema_version"] != INPUT_SCHEMA_VERSION:
        raise EvidenceValidationError(f"gate_input.schema_version must be {INPUT_SCHEMA_VERSION!r}")
    cutoff_ts = _parse_aware_datetime(gate_input["cutoff_ts"], "gate_input.cutoff_ts")
    if evaluated_at < cutoff_ts:
        raise EvidenceValidationError("evaluated_at must not precede gate_input.cutoff_ts")
    if gate_input["model_version"] != model_version:
        raise EvidenceValidationError("gate_input.model_version does not match versions.model")
    if gate_input["taxonomy_version"] != taxonomy_version:
        raise EvidenceValidationError(
            "gate_input.taxonomy_version does not match versions.taxonomy"
        )

    decision = _require_dict(normalized["gate_decision"], "gate_decision")
    _require_exact_keys(decision, _GATE_DECISION_KEYS, "gate_decision")
    _validate_gate_decision_fields(decision)
    if decision["input_schema_version"] != INPUT_SCHEMA_VERSION:
        raise EvidenceValidationError(
            f"gate_decision.input_schema_version must be {INPUT_SCHEMA_VERSION!r}"
        )
    if decision["output_schema_version"] != OUTPUT_SCHEMA_VERSION:
        raise EvidenceValidationError(
            f"gate_decision.output_schema_version must be {OUTPUT_SCHEMA_VERSION!r}"
        )
    if decision["policy_version"] != policy_version:
        raise EvidenceValidationError("gate_decision.policy_version does not match versions.policy")

    _require_dict(normalized["frozen_snapshot"], "frozen_snapshot")
    stored_hash = normalized[_CONTENT_HASH_KEY]
    if not isinstance(stored_hash, str) or not _SHA256_RE.fullmatch(stored_hash):
        raise EvidenceValidationError("content_sha256 must be 64 lowercase hexadecimal chars")
    computed_hash = compute_v16_day_gate_evidence_hash(normalized)
    if not hmac.compare_digest(stored_hash, computed_hash):
        raise EvidenceValidationError(
            f"content hash mismatch: stored={stored_hash}, computed={computed_hash}"
        )
    return normalized


def _to_json_value(value: object) -> Any:
    if isinstance(value, Enum):
        return _to_json_value(value.value)
    if isinstance(value, datetime):
        return _datetime_to_json(value)
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _to_json_value(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"JSON object keys must be strings, got {type(key).__name__}")
            result[key] = _to_json_value(item)
        return result
    if isinstance(value, (tuple, list)):
        return [_to_json_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        items = [_to_json_value(item) for item in value]
        return sorted(items, key=lambda item: _canonical_json_bytes(item))
    if isinstance(value, Path):
        return str(value)
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite floats are not valid evidence JSON")
        return value
    raise TypeError(f"unsupported evidence JSON value: {type(value).__name__}")


def _canonical_json_bytes(value: object) -> bytes:
    normalized = _to_json_value(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _datetime_to_json(value: datetime) -> str:
    _require_aware_datetime(value, "datetime")
    return value.isoformat(timespec="microseconds")


def _require_aware_datetime(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise EvidenceValidationError(f"{field_name} must be a timezone-aware datetime")


def _parse_aware_datetime(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise EvidenceValidationError(f"{field_name} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise EvidenceValidationError(f"{field_name} is not valid ISO-8601: {value!r}") from exc
    _require_aware_datetime(parsed, field_name)
    return parsed


def _require_matching_versions(
    *,
    gate_input: V16DayGateInput,
    decision: V16DayGateDecision,
    model_version: str,
    taxonomy_version: str | None,
    policy_version: str | None,
) -> None:
    if gate_input.model_version != model_version:
        raise EvidenceValidationError("model_version does not match gate_input.model_version")
    if gate_input.taxonomy_version != taxonomy_version:
        raise EvidenceValidationError("taxonomy_version does not match gate_input.taxonomy_version")
    if decision.policy_version != policy_version:
        raise EvidenceValidationError("policy_version does not match decision.policy_version")


def _require_version(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise EvidenceValidationError(f"{field_name} must be a non-empty string")


def _require_optional_version(value: str | None, field_name: str) -> None:
    if value is not None:
        _require_version(value, field_name)


def _require_json_version(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EvidenceValidationError(f"{field_name} must be a non-empty string")
    return value


def _require_json_optional_version(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_json_version(value, field_name)


def _require_dict(value: object, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise EvidenceValidationError(f"{field_name} must be a JSON object")
    return value


def _validate_gate_input_fields(gate_input: Mapping[str, object]) -> None:
    _require_string_list(gate_input["ranked_top_k"], "gate_input.ranked_top_k")
    boards = _require_dict(gate_input["stock_all_boards"], "gate_input.stock_all_boards")
    for code, values in boards.items():
        if not isinstance(code, str):
            raise EvidenceValidationError("gate_input.stock_all_boards keys must be strings")
        _require_string_list(values, f"gate_input.stock_all_boards[{code!r}]")

    drivers = _require_dict(gate_input["stock_is_driver"], "gate_input.stock_is_driver")
    if not all(
        isinstance(code, str) and isinstance(value, bool) for code, value in drivers.items()
    ):
        raise EvidenceValidationError("gate_input.stock_is_driver must map strings to booleans")

    _require_json_version(gate_input["model_version"], "gate_input.model_version")
    canonical = gate_input["canonical_theme_map"]
    if canonical is not None:
        canonical_dict = _require_dict(canonical, "gate_input.canonical_theme_map")
        if not all(
            isinstance(board, str) and isinstance(theme, str)
            for board, theme in canonical_dict.items()
        ):
            raise EvidenceValidationError(
                "gate_input.canonical_theme_map must map strings to strings"
            )
    _require_json_optional_version(gate_input["taxonomy_version"], "gate_input.taxonomy_version")
    if not isinstance(gate_input["upstream_data_complete"], bool):
        raise EvidenceValidationError("gate_input.upstream_data_complete must be boolean")
    _require_string_list(gate_input["data_quality_issues"], "gate_input.data_quality_issues")


def _validate_gate_decision_fields(decision: Mapping[str, object]) -> None:
    _require_enum_value(decision["state"], GateState, "gate_decision.state")
    _require_enum_value(decision["mode"], GateMode, "gate_decision.mode")
    _validate_reason_codes(decision["reasons"])
    _require_json_optional_version(decision["policy_version"], "gate_decision.policy_version")

    metrics = _require_dict(decision["metrics"], "gate_decision.metrics")
    _require_exact_keys(metrics, _GATE_METRICS_KEYS, "gate_decision.metrics")
    for name in (
        "ranked_count",
        "themed_stock_count",
        "component_count",
        "largest_cluster_size",
        "driver_count",
    ):
        _require_nonnegative_int(metrics[name], f"gate_decision.metrics.{name}")
    for name in (
        "theme_coverage",
        "largest_cluster_share",
        "top3_main_cluster_coverage",
        "driver_breadth",
    ):
        _require_fraction(metrics[name], f"gate_decision.metrics.{name}")
    _require_nonnegative_number(
        metrics["effective_cluster_count"],
        "gate_decision.metrics.effective_cluster_count",
    )
    _require_string_list(
        metrics["largest_cluster_codes"],
        "gate_decision.metrics.largest_cluster_codes",
    )
    _require_string_list(
        metrics["largest_cluster_themes"],
        "gate_decision.metrics.largest_cluster_themes",
    )

    thresholds = decision["applied_thresholds"]
    if not isinstance(thresholds, list):
        raise EvidenceValidationError("gate_decision.applied_thresholds must be a list")
    for index, threshold in enumerate(thresholds):
        if (
            not isinstance(threshold, list)
            or len(threshold) != 2
            or not isinstance(threshold[0], str)
        ):
            raise EvidenceValidationError(
                f"gate_decision.applied_thresholds[{index}] must be [name, value]"
            )
        _require_finite_number(
            threshold[1],
            f"gate_decision.applied_thresholds[{index}][1]",
        )
    _require_string_list(
        decision["data_quality_issues"],
        "gate_decision.data_quality_issues",
    )


def _require_string_list(value: object, field_name: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise EvidenceValidationError(f"{field_name} must be a list of strings")
    return value


def _require_nonnegative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EvidenceValidationError(f"{field_name} must be a non-negative integer")
    return value


def _require_finite_number(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvidenceValidationError(f"{field_name} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise EvidenceValidationError(f"{field_name} must be a finite number")
    return number


def _require_nonnegative_number(value: object, field_name: str) -> float:
    number = _require_finite_number(value, field_name)
    if number < 0:
        raise EvidenceValidationError(f"{field_name} must be non-negative")
    return number


def _require_fraction(value: object, field_name: str) -> float:
    number = _require_finite_number(value, field_name)
    if not 0 <= number <= 1:
        raise EvidenceValidationError(f"{field_name} must be between zero and one")
    return number


def _require_exact_keys(value: Mapping[str, object], expected: set[str], field_name: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise EvidenceValidationError(
            f"{field_name} keys mismatch; missing={missing}, extra={extra}"
        )


def _require_enum_value(value: object, enum_type: type[Enum], field_name: str) -> None:
    allowed = {item.value for item in enum_type}
    if value not in allowed:
        raise EvidenceValidationError(f"{field_name} must be one of {sorted(allowed)}")


def _validate_reason_codes(value: object) -> None:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise EvidenceValidationError("gate_decision.reasons must be a list of strings")
    allowed = {reason.value for reason in GateReason}
    invalid = [reason for reason in value if reason not in allowed]
    if invalid:
        raise EvidenceValidationError(f"unknown gate_decision reason codes: {invalid}")


def _resolve_base(base_dir: str | Path, *, create: bool) -> Path:
    base = Path(base_dir)
    if create:
        base.mkdir(parents=True, exist_ok=True)
    resolved = base.resolve(strict=True)
    if not resolved.is_dir():
        raise NotADirectoryError(f"evidence base is not a directory: {resolved}")
    return resolved


def _resolve_inside_base(base: Path, candidate: Path, *, strict: bool) -> Path:
    resolved = candidate.resolve(strict=strict)
    try:
        resolved.relative_to(base)
    except ValueError as exc:
        raise EvidencePathError(f"evidence path escapes base {base}: {resolved}") from exc
    return resolved


def _record_path(
    base: Path,
    record: Mapping[str, object],
    *,
    create_date_dir: bool,
) -> Path:
    evaluated_at = _parse_aware_datetime(record["evaluated_at"], "evaluated_at")
    content_hash = record[_CONTENT_HASH_KEY]
    if not isinstance(content_hash, str) or not _SHA256_RE.fullmatch(content_hash):
        raise EvidenceValidationError("record has no valid content_sha256")

    date_dir = _resolve_inside_base(
        base,
        base / evaluated_at.strftime("%Y%m%d"),
        strict=False,
    )
    if create_date_dir:
        date_dir.mkdir(parents=False, exist_ok=True)
    date_dir = _resolve_inside_base(base, date_dir, strict=True)
    if not date_dir.is_dir():
        raise NotADirectoryError(f"evidence date path is not a directory: {date_dir}")

    filename = f"{evaluated_at:%H%M%S}_{content_hash[:12]}.json"
    return _resolve_inside_base(base, date_dir / filename, strict=False)
