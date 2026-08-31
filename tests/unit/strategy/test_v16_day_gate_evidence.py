"""Tests for append-only V16 day-gate shadow evidence."""

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.strategy import v16_day_gate_evidence as evidence_module
from src.strategy.v16_day_gate import (
    GateMode,
    GateState,
    V16DayGate,
    V16DayGateInput,
    V16DayGatePolicy,
)
from src.strategy.v16_day_gate_evidence import (
    EVIDENCE_SCHEMA_VERSION,
    HASH_ALGORITHM,
    EvidencePathError,
    EvidenceValidationError,
    append_v16_day_gate_evidence,
    build_v16_day_gate_evidence,
    compute_v16_day_gate_evidence_hash,
    read_v16_day_gate_evidence,
    validate_v16_day_gate_evidence,
)


@dataclass(frozen=True)
class _FrozenNestedSnapshot:
    label: str
    observed_at: datetime
    ranks: tuple[int, ...]


def _bundle(*, frozen_snapshot=None, evaluated_at=None):
    cutoff = datetime(2026, 8, 25, 9, 38, tzinfo=timezone.utc)
    gate_input = V16DayGateInput(
        cutoff_ts=cutoff,
        ranked_top_k=("a", "b", "c"),
        stock_all_boards={"a": ("ai",), "b": ("ai",), "c": ("robot",)},
        stock_is_driver={"a": True, "b": False, "c": True},
        model_version="lgbrank-sha-test",
        canonical_theme_map={"ai": "ai-chain"},
        taxonomy_version="taxonomy-test-v1",
    )
    policy = V16DayGatePolicy(
        version="policy-test-v1",
        min_largest_cluster_share=0.5,
    )
    decision = V16DayGate(policy).evaluate(gate_input)
    evaluated_at = evaluated_at or cutoff + timedelta(seconds=39, microseconds=123456)
    frozen_snapshot = frozen_snapshot or {
        "nested": _FrozenNestedSnapshot(
            label="盘面",
            observed_at=cutoff,
            ranks=(1, 2, 3),
        ),
        "enum": GateState.WATCH,
        "tuple": ("a", "b"),
        "set": {"robot", "ai"},
    }
    record = build_v16_day_gate_evidence(
        gate_input=gate_input,
        decision=decision,
        frozen_snapshot=frozen_snapshot,
        evaluated_at=evaluated_at,
        scanner_version="v16-scanner-main-06c2c1d",
        model_version="lgbrank-sha-test",
        taxonomy_version="taxonomy-test-v1",
        policy_version="policy-test-v1",
    )
    return gate_input, decision, record


def test_build_serializes_dataclasses_enums_datetimes_and_tuples():
    gate_input, decision, record = _bundle()

    assert record["schema_version"] == EVIDENCE_SCHEMA_VERSION
    assert record["hash_algorithm"] == HASH_ALGORITHM
    assert record["gate_input"]["ranked_top_k"] == ["a", "b", "c"]
    assert record["gate_input"]["cutoff_ts"] == gate_input.cutoff_ts.isoformat(
        timespec="microseconds"
    )
    assert record["gate_decision"]["state"] == decision.state.value
    assert record["gate_decision"]["mode"] == GateMode.SHADOW.value
    assert record["frozen_snapshot"]["enum"] == GateState.WATCH.value
    assert record["frozen_snapshot"]["tuple"] == ["a", "b"]
    assert record["frozen_snapshot"]["set"] == ["ai", "robot"]
    assert record["frozen_snapshot"]["nested"]["ranks"] == [1, 2, 3]
    assert re.fullmatch(r"[0-9a-f]{64}", record["content_sha256"])
    assert record["content_sha256"] == compute_v16_day_gate_evidence_hash(record)


def test_hash_and_record_are_stable_across_mapping_insertion_order():
    first_snapshot = {"z": {"b": 2, "a": 1}, "a": (3, 4)}
    second_snapshot = {"a": (3, 4), "z": {"a": 1, "b": 2}}

    _, _, first = _bundle(frozen_snapshot=first_snapshot)
    _, _, second = _bundle(frozen_snapshot=second_snapshot)

    assert first == second
    assert first["content_sha256"] == second["content_sha256"]


def test_append_uses_safe_layout_and_read_validates_round_trip(tmp_path):
    _, _, record = _bundle()
    base = tmp_path / "evidence"

    path = append_v16_day_gate_evidence(base, record)

    assert path.parent == (base / "20260825").resolve()
    assert re.fullmatch(r"093839_[0-9a-f]{12}\.json", path.name)
    assert path.read_bytes().endswith(b"\n")
    assert read_v16_day_gate_evidence(base, path) == record
    assert read_v16_day_gate_evidence(base, path.relative_to(base)) == record


def test_final_record_is_published_only_after_complete_temp_is_fsynced(
    tmp_path,
    monkeypatch,
):
    _, _, record = _bundle()
    base = tmp_path / "evidence"
    real_link = evidence_module.os.link
    observed_temp: Path | None = None

    def inspect_publish(source, target):
        nonlocal observed_temp
        observed_temp = Path(source)
        assert observed_temp.suffix == ".tmp"
        assert not Path(target).exists()
        assert json.loads(observed_temp.read_text(encoding="utf-8")) == record
        real_link(source, target)

    monkeypatch.setattr(evidence_module.os, "link", inspect_publish)

    final_path = append_v16_day_gate_evidence(base, record)

    assert final_path.exists()
    assert observed_temp is not None
    assert not observed_temp.exists()


def test_reappending_identical_record_is_idempotent(tmp_path):
    _, _, record = _bundle()
    base = tmp_path / "evidence"

    first = append_v16_day_gate_evidence(base, record)
    second = append_v16_day_gate_evidence(base, record)

    assert second == first
    assert list((base / "20260825").glob("*.json")) == [first]


def test_same_second_different_content_appends_a_second_file(tmp_path):
    evaluated_at = datetime(2026, 8, 25, 9, 38, 39, 999999, tzinfo=timezone.utc)
    _, _, first_record = _bundle(
        frozen_snapshot={"market": "coherent"},
        evaluated_at=evaluated_at,
    )
    _, _, second_record = _bundle(
        frozen_snapshot={"market": "dispersed"},
        evaluated_at=evaluated_at,
    )
    base = tmp_path / "evidence"

    first = append_v16_day_gate_evidence(base, first_record)
    second = append_v16_day_gate_evidence(base, second_record)

    assert first != second
    assert first.exists() and second.exists()
    assert len(list((base / "20260825").glob("*.json"))) == 2


def test_existing_corrupt_target_is_not_overwritten_or_silently_accepted(tmp_path):
    _, _, record = _bundle()
    base = tmp_path / "evidence"
    target = append_v16_day_gate_evidence(base, record)
    target.write_text("{}", encoding="utf-8")

    with pytest.raises(EvidenceValidationError):
        append_v16_day_gate_evidence(base, record)

    assert target.read_text(encoding="utf-8") == "{}"


def test_validate_rejects_tampering_via_content_hash():
    _, _, record = _bundle()
    tampered = json.loads(json.dumps(record))
    tampered["frozen_snapshot"]["enum"] = GateState.TRADE.value

    with pytest.raises(EvidenceValidationError, match="content hash mismatch"):
        validate_v16_day_gate_evidence(tampered)


def test_validate_rejects_wrong_schema_even_before_hash_check():
    _, _, record = _bundle()
    record["schema_version"] = "unknown/v99"

    with pytest.raises(EvidenceValidationError, match="unsupported evidence schema"):
        validate_v16_day_gate_evidence(record)


def test_read_rejects_path_outside_base(tmp_path):
    base = tmp_path / "base"
    base.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")

    with pytest.raises(EvidencePathError):
        read_v16_day_gate_evidence(base, outside)


def test_read_rejects_valid_record_moved_to_wrong_schema_path(tmp_path):
    _, _, record = _bundle()
    base = tmp_path / "evidence"
    original = append_v16_day_gate_evidence(base, record)
    misplaced = base / "misplaced.json"
    misplaced.write_bytes(original.read_bytes())

    with pytest.raises(EvidencePathError, match="does not match schema path"):
        read_v16_day_gate_evidence(base, misplaced)


def test_build_rejects_provenance_version_mismatch():
    gate_input, decision, _record = _bundle()

    with pytest.raises(EvidenceValidationError, match="model_version does not match"):
        build_v16_day_gate_evidence(
            gate_input=gate_input,
            decision=decision,
            frozen_snapshot={},
            evaluated_at=gate_input.cutoff_ts + timedelta(seconds=1),
            scanner_version="scanner-v1",
            model_version="different-model",
            taxonomy_version=gate_input.taxonomy_version,
            policy_version=decision.policy_version,
        )


def test_build_rejects_naive_datetime():
    gate_input, decision, _record = _bundle()

    with pytest.raises(EvidenceValidationError, match="timezone-aware"):
        build_v16_day_gate_evidence(
            gate_input=gate_input,
            decision=decision,
            frozen_snapshot={},
            evaluated_at=datetime(2026, 8, 25, 9, 39),
            scanner_version="scanner-v1",
            model_version=gate_input.model_version,
            taxonomy_version=gate_input.taxonomy_version,
            policy_version=decision.policy_version,
        )


def test_build_rejects_non_json_snapshot_values():
    gate_input, decision, _record = _bundle()

    with pytest.raises(TypeError, match="unsupported evidence JSON value"):
        build_v16_day_gate_evidence(
            gate_input=gate_input,
            decision=decision,
            frozen_snapshot={"bad": object()},
            evaluated_at=gate_input.cutoff_ts + timedelta(seconds=1),
            scanner_version="scanner-v1",
            model_version=gate_input.model_version,
            taxonomy_version=gate_input.taxonomy_version,
            policy_version=decision.policy_version,
        )
