from __future__ import annotations

import hashlib
import json
import shutil
from datetime import date
from pathlib import Path

import pytest

from src.strategy.v20.artifacts import ArtifactValidationError, load_g_artifacts

ARTIFACTS = Path(__file__).resolve().parents[4] / "docs" / "strategy-v20-artifacts"
MANIFEST_HASH = "377cf1181539ad7d7b2e0407c27e6529e1c911e06052c7968caf057cb0131d32"


def _copy_artifacts(tmp_path: Path) -> Path:
    target = tmp_path / "artifacts"
    shutil.copytree(ARTIFACTS, target)
    return target


def _rewrite_manifest(root: Path, mutate) -> None:
    path = root / "manifest-v1.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    mutate(manifest)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _update_runtime_hash(root: Path, runtime_key: str, filename: str) -> None:
    digest = hashlib.sha256((root / filename).read_bytes()).hexdigest()

    def mutate(manifest) -> None:
        manifest["runtime_files"][runtime_key]["sha256"] = digest

    _rewrite_manifest(root, mutate)


def test_loads_and_recursively_validates_frozen_bundle() -> None:
    bundle = load_g_artifacts(ARTIFACTS, expected_manifest_sha256=MANIFEST_HASH)
    assert bundle.artifact_id == "V20_G_ARTIFACTS_20260831_V1"
    assert bundle.manifest_sha256 == MANIFEST_HASH
    assert len(bundle.mapping) == 301
    assert sum(item.cluster_allowed for item in bundle.mapping.values()) == 230
    assert len(bundle.thresholds) == 6
    assert bundle.threshold_for(date(2026, 8, 31)).sample_n == 73
    assert bundle.threshold_for(date(2026, 8, 31)).prior_amount_total == 11_338_548_013.96


def test_bundle_mappings_are_immutable() -> None:
    bundle = load_g_artifacts(ARTIFACTS)
    with pytest.raises(TypeError):
        bundle.mapping["new"] = bundle.mapping["3D打印"]  # type: ignore[index]


def test_expected_manifest_hash_is_a_hard_configuration_binding() -> None:
    with pytest.raises(ArtifactValidationError, match="active configuration"):
        load_g_artifacts(ARTIFACTS, expected_manifest_sha256="0" * 64)


def test_runtime_file_byte_change_fails_hash_validation(tmp_path: Path) -> None:
    root = _copy_artifacts(tmp_path)
    path = root / "g-theme-mapping-v1.csv"
    path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(ArtifactValidationError, match="SHA-256 mismatch"):
        load_g_artifacts(root)


def test_manifest_duplicate_json_key_is_rejected(tmp_path: Path) -> None:
    root = _copy_artifacts(tmp_path)
    path = root / "manifest-v1.json"
    raw = path.read_text(encoding="utf-8")
    path.write_text(raw.replace("{", '{\n  "artifact_id": "duplicate",', 1), encoding="utf-8")
    with pytest.raises(ArtifactValidationError, match="duplicate JSON key"):
        load_g_artifacts(root)


def test_manifest_path_traversal_is_rejected(tmp_path: Path) -> None:
    root = _copy_artifacts(tmp_path)

    def mutate(manifest) -> None:
        manifest["runtime_files"]["semantic_mapping"]["path"] = "../outside.csv"

    _rewrite_manifest(root, mutate)
    with pytest.raises(ArtifactValidationError, match="direct relative filename"):
        load_g_artifacts(root)


def test_duplicate_mapping_label_is_rejected_even_with_updated_file_hash(tmp_path: Path) -> None:
    root = _copy_artifacts(tmp_path)
    path = root / "g-theme-mapping-v1.csv"
    lines = path.read_text(encoding="utf-8").splitlines()
    path.write_text("\n".join([*lines, lines[1]]) + "\n", encoding="utf-8")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()

    def mutate(manifest) -> None:
        spec = manifest["runtime_files"]["semantic_mapping"]
        spec["sha256"] = digest
        spec["row_n"] = 302

    _rewrite_manifest(root, mutate)
    with pytest.raises(ArtifactValidationError, match="empty or duplicated"):
        load_g_artifacts(root)


def test_mapping_schema_rejects_extra_runtime_column(tmp_path: Path) -> None:
    root = _copy_artifacts(tmp_path)
    path = root / "g-theme-mapping-v1.csv"
    lines = path.read_text(encoding="utf-8").splitlines()
    lines[0] += ",future_outcome"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    _update_runtime_hash(root, "semantic_mapping", path.name)
    with pytest.raises(ArtifactValidationError, match="columns mismatch"):
        load_g_artifacts(root)


def test_q25_is_recomputed_from_exact_bound_samples(tmp_path: Path) -> None:
    root = _copy_artifacts(tmp_path)
    path = root / "causal-half-year-q25-v1.csv"
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace("2945899637.5", "2945899637.6", 1), encoding="utf-8")
    _update_runtime_hash(root, "half_year_q25", path.name)
    with pytest.raises(ArtifactValidationError, match="mismatch"):
        load_g_artifacts(root)


def test_calibration_sample_hash_is_recursively_bound_by_threshold_rows(tmp_path: Path) -> None:
    root = _copy_artifacts(tmp_path)
    path = root / "causal-half-year-q25-samples-v1.csv"
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace("5069443235.0", "5069443236.0", 1), encoding="utf-8")
    _update_runtime_hash(root, "half_year_q25_calibration_samples", path.name)
    with pytest.raises(ArtifactValidationError, match="calibration input hash mismatch"):
        load_g_artifacts(root)


def test_missing_half_fails_closed_at_lookup() -> None:
    bundle = load_g_artifacts(ARTIFACTS)
    with pytest.raises(ArtifactValidationError, match="no Q25 threshold"):
        bundle.threshold_for(date(2027, 1, 4))
