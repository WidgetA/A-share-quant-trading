"""Strict loader for the frozen V20 G semantic and Q25 artifacts."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from .models import Q25Threshold, ThemeMapping
from .policy import decision_half_for_date

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_HALF_RE = re.compile(r"^(\d{4})H([12])$")
_DATE8_RE = re.compile(r"^\d{8}$")

_MANIFEST_KEYS = {
    "schema_version",
    "artifact_id",
    "artifact_frozen_date",
    "runtime_files",
    "semantic_equivalence_checks",
    "research_provenance",
}
_MAPPING_COLUMNS = (
    "raw_label",
    "canonical_theme_id",
    "canonical_theme_name_cn",
    "label_role",
    "cluster_allowed",
)
_Q25_COLUMNS = (
    "decision_half",
    "prior_amount_total",
    "prior_amount_median",
    "prior_amount_bottom3_sum",
    "sample_n",
    "train_start",
    "train_end",
    "calibration_as_of_ts",
    "calibration_time_evidence",
    "calibration_input_snapshot_hash",
    "threshold_source_hash",
    "historical_pit_status",
)
_SAMPLE_COLUMNS = (
    "decision_half",
    "trade_date",
    "prior_trade_date",
    "prior_amount_total",
    "prior_amount_median",
    "prior_amount_bottom3_sum",
)
_MAPPING_SPEC_KEYS = {
    "path",
    "sha256",
    "projection_columns",
    "row_n",
    "unique_raw_label_n",
    "cluster_allowed_true_n",
    "unique_canonical_theme_n",
}
_Q25_SPEC_KEYS = {
    "path",
    "sha256",
    "required_columns",
    "forbidden_runtime_columns",
    "row_n",
    "unique_decision_half_n",
    "latest_decision_half",
    "latest_sample_n",
    "latest_calibration_snapshot_frozen_at",
    "latest_calibration_time_evidence",
    "late_rebuilt_available_sample_n",
    "late_rebuild_is_diagnostic_only",
}
_SAMPLE_SPEC_KEYS = {
    "path",
    "sha256",
    "columns",
    "row_n",
    "row_n_by_decision_half",
    "unique_decision_half_trade_date",
    "selection_rule",
    "2026H2_late_rows_excluded",
}
_EQUIVALENCE_CHECK_KEYS = {
    "mapping_projection_matches_four_frozen_research_sources",
    "q25_rows_match_frozen_research_source",
    "q25_recomputes_from_exact_calibration_samples",
}
_PROVENANCE_KEYS = {
    "gate_manifest_sha256",
    "q25_source_sha256",
    "q25_2026h2_original_daily_panel_sha256",
    "q25_2026h2_threshold_candidates_sha256",
    "mapping_source_sha256",
}


class ArtifactValidationError(ValueError):
    """The frozen artifact set failed closed validation."""


@dataclass(frozen=True, slots=True)
class GArtifactBundle:
    artifact_id: str
    manifest_sha256: str
    mapping: Mapping[str, ThemeMapping]
    thresholds: Mapping[str, Q25Threshold]

    def threshold_for(self, decision_date: date) -> Q25Threshold:
        half = decision_half_for_date(decision_date)
        try:
            return self.thresholds[half]
        except KeyError as exc:
            raise ArtifactValidationError(f"no Q25 threshold for {half}") from exc


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ArtifactValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _require_exact_keys(value: Mapping[str, Any], expected: set[str], context: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise ArtifactValidationError(f"{context} keys mismatch; missing={missing}, extra={extra}")


def _require_non_empty_string(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise ArtifactValidationError(f"{context} must be a non-empty string")
    return value


def _require_int(value: Any, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ArtifactValidationError(f"{context} must be an integer")
    return value


def _require_sha256(value: Any, context: str) -> str:
    text = _require_non_empty_string(value, context)
    if _SHA256_RE.fullmatch(text) is None:
        raise ArtifactValidationError(f"{context} must be a lowercase SHA-256")
    return text


def _read_bound_file(root: Path, spec: Mapping[str, Any], context: str) -> tuple[Path, bytes]:
    filename = _require_non_empty_string(spec.get("path"), f"{context}.path")
    relative = Path(filename)
    if relative.is_absolute() or relative.name != filename or ".." in relative.parts:
        raise ArtifactValidationError(f"{context}.path must be a direct relative filename")
    target = (root / relative).resolve()
    if target.parent != root:
        raise ArtifactValidationError(f"{context}.path escapes the artifact directory")
    try:
        data = target.read_bytes()
    except OSError as exc:
        raise ArtifactValidationError(f"cannot read {context}: {target}") from exc
    expected_hash = _require_sha256(spec.get("sha256"), f"{context}.sha256")
    actual_hash = _sha256(data)
    if actual_hash != expected_hash:
        raise ArtifactValidationError(
            f"{context} SHA-256 mismatch: expected {expected_hash}, got {actual_hash}"
        )
    return target, data


def _read_csv(data: bytes, expected_columns: Sequence[str], context: str) -> list[dict[str, str]]:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ArtifactValidationError(f"{context} must be UTF-8") from exc
    reader = csv.DictReader(io.StringIO(text, newline=""))
    if reader.fieldnames != list(expected_columns):
        raise ArtifactValidationError(
            f"{context} columns mismatch: expected {list(expected_columns)}, "
            f"got {reader.fieldnames}"
        )
    rows: list[dict[str, str]] = []
    try:
        for raw in reader:
            if None in raw or any(value is None for value in raw.values()):
                raise ArtifactValidationError(f"{context} contains a malformed CSV row")
            rows.append(
                {key: value for key, value in raw.items() if key is not None and value is not None}
            )
    except csv.Error as exc:
        raise ArtifactValidationError(f"{context} is not valid CSV") from exc
    return rows


def _positive_decimal(value: str, context: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ArtifactValidationError(f"{context} is not decimal") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ArtifactValidationError(f"{context} must be finite and positive")
    return parsed


def _positive_int_text(value: str, context: str) -> int:
    if not value.isdigit() or int(value) <= 0:
        raise ArtifactValidationError(f"{context} must be a positive integer")
    return int(value)


def _parse_date8(value: str, context: str) -> date:
    if _DATE8_RE.fullmatch(value) is None:
        raise ArtifactValidationError(f"{context} must use YYYYMMDD")
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        raise ArtifactValidationError(f"{context} is not a valid date") from exc


def _validate_decision_half(value: str, context: str) -> tuple[int, int]:
    match = _HALF_RE.fullmatch(value)
    if match is None:
        raise ArtifactValidationError(f"{context} must use YYYYH1 or YYYYH2")
    return int(match.group(1)), int(match.group(2))


def _expected_training_range(decision_half: str) -> tuple[date, date]:
    year, half = _validate_decision_half(decision_half, "decision_half")
    if half == 1:
        return date(year - 1, 7, 1), date(year - 1, 12, 31)
    return date(year, 1, 1), date(year, 6, 30)


def _decimal_q25(values: Sequence[Decimal]) -> Decimal:
    if not values:
        raise ArtifactValidationError("cannot compute Q25 from an empty sample")
    ordered = sorted(values)
    numerator = len(ordered) - 1
    index, remainder = divmod(numerator, 4)
    if remainder == 0:
        return ordered[index]
    fraction = Decimal(remainder) / Decimal(4)
    return ordered[index] + fraction * (ordered[index + 1] - ordered[index])


def _validate_mapping(data: bytes, spec: Mapping[str, Any]) -> Mapping[str, ThemeMapping]:
    if spec.get("projection_columns") != list(_MAPPING_COLUMNS):
        raise ArtifactValidationError("semantic_mapping.projection_columns is not frozen V1")
    rows = _read_csv(data, _MAPPING_COLUMNS, "semantic_mapping")
    if len(rows) != _require_int(spec.get("row_n"), "semantic_mapping.row_n"):
        raise ArtifactValidationError("semantic_mapping row_n mismatch")
    mapping: dict[str, ThemeMapping] = {}
    for row_number, row in enumerate(rows, start=2):
        raw_label = row["raw_label"]
        if not raw_label or raw_label in mapping:
            raise ArtifactValidationError(
                f"semantic_mapping raw_label is empty or duplicated at row {row_number}"
            )
        allowed_text = row["cluster_allowed"]
        if allowed_text not in ("true", "false"):
            raise ArtifactValidationError(
                f"semantic_mapping cluster_allowed must be true/false at row {row_number}"
            )
        try:
            mapping[raw_label] = ThemeMapping(
                raw_label=raw_label,
                canonical_theme_id=row["canonical_theme_id"],
                canonical_theme_name_cn=row["canonical_theme_name_cn"],
                label_role=row["label_role"],
                cluster_allowed=allowed_text == "true",
            )
        except ValueError as exc:
            raise ArtifactValidationError(f"invalid semantic_mapping row {row_number}") from exc
    metrics = {
        "unique_raw_label_n": len(mapping),
        "cluster_allowed_true_n": sum(item.cluster_allowed for item in mapping.values()),
        "unique_canonical_theme_n": len({item.canonical_theme_id for item in mapping.values()}),
    }
    for key, actual in metrics.items():
        if actual != _require_int(spec.get(key), f"semantic_mapping.{key}"):
            raise ArtifactValidationError(f"semantic_mapping {key} mismatch")
    return MappingProxyType(mapping)


def _validate_samples(
    data: bytes, spec: Mapping[str, Any]
) -> tuple[dict[str, list[dict[str, str]]], str]:
    if spec.get("columns") != list(_SAMPLE_COLUMNS):
        raise ArtifactValidationError("calibration sample columns are not frozen V1")
    rows = _read_csv(data, _SAMPLE_COLUMNS, "half_year_q25_calibration_samples")
    if len(rows) != _require_int(spec.get("row_n"), "calibration_samples.row_n"):
        raise ArtifactValidationError("calibration sample row_n mismatch")
    grouped: dict[str, list[dict[str, str]]] = {}
    seen: set[tuple[str, str]] = set()
    for row_number, row in enumerate(rows, start=2):
        half = row["decision_half"]
        _validate_decision_half(half, f"calibration row {row_number} decision_half")
        trade_date = _parse_date8(row["trade_date"], f"calibration row {row_number} trade_date")
        prior_date = _parse_date8(
            row["prior_trade_date"], f"calibration row {row_number} prior_trade_date"
        )
        train_start, train_end = _expected_training_range(half)
        if not train_start <= trade_date <= train_end or prior_date >= trade_date:
            raise ArtifactValidationError(f"calibration row {row_number} violates causal dates")
        identity = (half, row["trade_date"])
        if identity in seen:
            raise ArtifactValidationError(f"duplicate calibration identity {identity}")
        seen.add(identity)
        for column in _SAMPLE_COLUMNS[3:]:
            _positive_decimal(row[column], f"calibration row {row_number} {column}")
        grouped.setdefault(half, []).append(row)
    if spec.get("unique_decision_half_trade_date") is not True:
        raise ArtifactValidationError("manifest must assert unique calibration identities")
    declared_counts = spec.get("row_n_by_decision_half")
    if not isinstance(declared_counts, dict):
        raise ArtifactValidationError("row_n_by_decision_half must be an object")
    actual_counts = {half: len(items) for half, items in grouped.items()}
    if declared_counts != actual_counts:
        raise ArtifactValidationError("calibration sample counts by half mismatch")
    if spec.get("selection_rule") != (
        "exact rows in this artifact where decision_half equals the threshold row decision_half"
    ):
        raise ArtifactValidationError("calibration selection_rule mismatch")
    excluded = spec.get("2026H2_late_rows_excluded")
    if not isinstance(excluded, list) or len(excluded) != len(set(excluded)):
        raise ArtifactValidationError("2026H2_late_rows_excluded must be a unique array")
    present_2026h2 = {row["trade_date"] for row in grouped.get("2026H2", [])}
    for value in excluded:
        _parse_date8(value, "2026H2_late_rows_excluded item")
        if value in present_2026h2:
            raise ArtifactValidationError("a declared late-excluded row is present in samples")
    return grouped, _sha256(data)


def _validate_thresholds(
    data: bytes,
    spec: Mapping[str, Any],
    samples: Mapping[str, list[dict[str, str]]],
    samples_sha256: str,
) -> Mapping[str, Q25Threshold]:
    if spec.get("required_columns") != list(_Q25_COLUMNS):
        raise ArtifactValidationError("half_year_q25.required_columns is not frozen V1")
    forbidden = spec.get("forbidden_runtime_columns")
    if not isinstance(forbidden, list) or any(column in _Q25_COLUMNS for column in forbidden):
        raise ArtifactValidationError("half_year_q25 forbidden column declaration is invalid")
    rows = _read_csv(data, _Q25_COLUMNS, "half_year_q25")
    if len(rows) != _require_int(spec.get("row_n"), "half_year_q25.row_n"):
        raise ArtifactValidationError("half_year_q25 row_n mismatch")
    thresholds: dict[str, Q25Threshold] = {}
    raw_by_half: dict[str, dict[str, str]] = {}
    for row_number, row in enumerate(rows, start=2):
        half = row["decision_half"]
        _validate_decision_half(half, f"Q25 row {row_number} decision_half")
        if half in thresholds:
            raise ArtifactValidationError(f"duplicate Q25 decision_half {half}")
        raw_by_half[half] = row
        sample_rows = samples.get(half)
        if sample_rows is None:
            raise ArtifactValidationError(f"Q25 row {half} has no calibration samples")
        sample_n = _positive_int_text(row["sample_n"], f"Q25 row {row_number} sample_n")
        if sample_n != len(sample_rows):
            raise ArtifactValidationError(f"Q25 row {half} sample_n mismatch")
        train_start = _parse_date8(row["train_start"], f"Q25 row {half} train_start")
        train_end = _parse_date8(row["train_end"], f"Q25 row {half} train_end")
        if (train_start, train_end) != _expected_training_range(half):
            raise ArtifactValidationError(f"Q25 row {half} training half mismatch")
        try:
            calibration_as_of = datetime.fromisoformat(row["calibration_as_of_ts"])
        except ValueError as exc:
            raise ArtifactValidationError(f"Q25 row {half} invalid calibration_as_of_ts") from exc
        if calibration_as_of.tzinfo is None or calibration_as_of.utcoffset() is None:
            raise ArtifactValidationError(f"Q25 row {half} calibration_as_of_ts lacks timezone")
        if not row["calibration_time_evidence"] or not row["historical_pit_status"]:
            raise ArtifactValidationError(f"Q25 row {half} missing calibration evidence")
        if (
            _require_sha256(
                row["calibration_input_snapshot_hash"],
                f"Q25 row {half} calibration_input_snapshot_hash",
            )
            != samples_sha256
        ):
            raise ArtifactValidationError(f"Q25 row {half} calibration input hash mismatch")
        _require_sha256(row["threshold_source_hash"], f"Q25 row {half} threshold_source_hash")

        decimal_values: dict[str, Decimal] = {}
        for column in (
            "prior_amount_total",
            "prior_amount_median",
            "prior_amount_bottom3_sum",
        ):
            declared = _positive_decimal(row[column], f"Q25 row {half} {column}")
            recomputed = _decimal_q25(
                [_positive_decimal(item[column], f"sample {half} {column}") for item in sample_rows]
            )
            if declared != recomputed:
                raise ArtifactValidationError(
                    f"Q25 row {half} {column} mismatch: {declared} != {recomputed}"
                )
            decimal_values[column] = declared
        thresholds[half] = Q25Threshold(
            decision_half=half,
            prior_amount_total=float(decimal_values["prior_amount_total"]),
            prior_amount_median=float(decimal_values["prior_amount_median"]),
            prior_amount_bottom3_sum=float(decimal_values["prior_amount_bottom3_sum"]),
            sample_n=sample_n,
            calibration_as_of_ts=row["calibration_as_of_ts"],
            threshold_source_hash=row["threshold_source_hash"],
        )
    if len(thresholds) != _require_int(
        spec.get("unique_decision_half_n"), "half_year_q25.unique_decision_half_n"
    ):
        raise ArtifactValidationError("half_year_q25 unique_decision_half_n mismatch")
    latest_half = _require_non_empty_string(
        spec.get("latest_decision_half"), "half_year_q25.latest_decision_half"
    )
    if latest_half != max(thresholds):
        raise ArtifactValidationError("half_year_q25 latest_decision_half mismatch")
    if thresholds[latest_half].sample_n != _require_int(
        spec.get("latest_sample_n"), "half_year_q25.latest_sample_n"
    ):
        raise ArtifactValidationError("half_year_q25 latest_sample_n mismatch")
    latest_row = raw_by_half[latest_half]
    if spec.get("latest_calibration_snapshot_frozen_at") != latest_row["calibration_as_of_ts"]:
        raise ArtifactValidationError("latest calibration timestamp mismatch")
    if spec.get("latest_calibration_time_evidence") != latest_row["calibration_time_evidence"]:
        raise ArtifactValidationError("latest calibration time evidence mismatch")
    late_n = _require_int(
        spec.get("late_rebuilt_available_sample_n"),
        "half_year_q25.late_rebuilt_available_sample_n",
    )
    if late_n < thresholds[latest_half].sample_n:
        raise ArtifactValidationError(
            "late rebuilt sample count cannot be smaller than frozen count"
        )
    if spec.get("late_rebuild_is_diagnostic_only") is not True:
        raise ArtifactValidationError("late Q25 rebuild must be diagnostic only")
    return MappingProxyType(thresholds)


def load_g_artifacts(
    artifact_directory: str | Path,
    *,
    manifest_filename: str = "manifest-v1.json",
    expected_manifest_sha256: str | None = None,
) -> GArtifactBundle:
    """Load and recursively validate the complete frozen G artifact set."""

    root = Path(artifact_directory).resolve()
    if not root.is_dir():
        raise ArtifactValidationError(f"artifact directory does not exist: {root}")
    if Path(manifest_filename).name != manifest_filename:
        raise ArtifactValidationError("manifest_filename must be a direct filename")
    manifest_path = (root / manifest_filename).resolve()
    if manifest_path.parent != root:
        raise ArtifactValidationError("manifest path escapes artifact directory")
    try:
        manifest_bytes = manifest_path.read_bytes()
    except OSError as exc:
        raise ArtifactValidationError(f"cannot read manifest: {manifest_path}") from exc
    manifest_hash = _sha256(manifest_bytes)
    if expected_manifest_sha256 is not None:
        _require_sha256(expected_manifest_sha256, "expected_manifest_sha256")
        if manifest_hash != expected_manifest_sha256:
            raise ArtifactValidationError("manifest SHA-256 does not match active configuration")
    try:
        manifest = json.loads(
            manifest_bytes.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_keys
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactValidationError("manifest must be valid UTF-8 JSON") from exc
    if not isinstance(manifest, dict):
        raise ArtifactValidationError("manifest root must be an object")
    _require_exact_keys(manifest, _MANIFEST_KEYS, "manifest")
    if manifest["schema_version"] != "v20-g-artifact-manifest/v1":
        raise ArtifactValidationError("unsupported manifest schema_version")
    artifact_id = _require_non_empty_string(manifest["artifact_id"], "artifact_id")
    try:
        date.fromisoformat(
            _require_non_empty_string(manifest["artifact_frozen_date"], "frozen date")
        )
    except ValueError as exc:
        raise ArtifactValidationError("artifact_frozen_date must be an ISO date") from exc

    runtime_files = manifest["runtime_files"]
    if not isinstance(runtime_files, dict):
        raise ArtifactValidationError("runtime_files must be an object")
    _require_exact_keys(
        runtime_files,
        {"semantic_mapping", "half_year_q25", "half_year_q25_calibration_samples"},
        "runtime_files",
    )
    for name, value in runtime_files.items():
        if not isinstance(value, dict):
            raise ArtifactValidationError(f"runtime_files.{name} must be an object")
    _require_exact_keys(
        runtime_files["semantic_mapping"], _MAPPING_SPEC_KEYS, "semantic_mapping spec"
    )
    _require_exact_keys(runtime_files["half_year_q25"], _Q25_SPEC_KEYS, "Q25 spec")
    _require_exact_keys(
        runtime_files["half_year_q25_calibration_samples"],
        _SAMPLE_SPEC_KEYS,
        "calibration sample spec",
    )

    _, mapping_bytes = _read_bound_file(root, runtime_files["semantic_mapping"], "semantic_mapping")
    _, q25_bytes = _read_bound_file(root, runtime_files["half_year_q25"], "half_year_q25")
    _, sample_bytes = _read_bound_file(
        root,
        runtime_files["half_year_q25_calibration_samples"],
        "half_year_q25_calibration_samples",
    )
    mapping = _validate_mapping(mapping_bytes, runtime_files["semantic_mapping"])
    samples, samples_hash = _validate_samples(
        sample_bytes, runtime_files["half_year_q25_calibration_samples"]
    )
    thresholds = _validate_thresholds(
        q25_bytes, runtime_files["half_year_q25"], samples, samples_hash
    )

    checks = manifest["semantic_equivalence_checks"]
    if (
        not isinstance(checks, dict)
        or not checks
        or any(value is not True for value in checks.values())
    ):
        raise ArtifactValidationError("all semantic equivalence checks must be explicitly true")
    _require_exact_keys(checks, _EQUIVALENCE_CHECK_KEYS, "semantic_equivalence_checks")
    provenance = manifest["research_provenance"]
    if not isinstance(provenance, dict) or not provenance:
        raise ArtifactValidationError("research_provenance must be a non-empty object")
    _require_exact_keys(provenance, _PROVENANCE_KEYS, "research_provenance")
    for key, value in provenance.items():
        if isinstance(value, str):
            _require_sha256(value, f"research_provenance.{key}")
        elif isinstance(value, list):
            if not value:
                raise ArtifactValidationError(f"research_provenance.{key} cannot be empty")
            for index, item in enumerate(value):
                _require_sha256(item, f"research_provenance.{key}[{index}]")
        else:
            raise ArtifactValidationError(f"research_provenance.{key} has invalid type")

    return GArtifactBundle(artifact_id, manifest_hash, mapping, thresholds)
