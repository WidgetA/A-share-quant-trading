"""Tests for the non-interfering V16 DayGate shadow adapter."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from src.strategy import v16_day_gate_shadow as shadow_module
from src.strategy.v16_day_gate import GateState, V16DayGatePolicy
from src.strategy.v16_day_gate_shadow import (
    BoardRelevanceConfig,
    V16DayGateShadowConfig,
    V16DayGateShadowError,
    freeze_v16_day_gate_runtime,
    freeze_v16_scan_snapshot,
    load_shadow_config,
    prepare_shadow_decision,
    prepared_to_metadata,
    shadow_message,
)
from src.strategy.v16_theme_semantics import (
    TAXONOMY_APPROVAL_SCHEMA_VERSION,
    UnapprovedThemeSemanticsError,
    approved_taxonomy_artifact_sha256,
    taxonomy_sha256,
)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


def _scored(code: str, rank: int, score: float = 0.5) -> SimpleNamespace:
    return SimpleNamespace(
        code=code,
        name=f"stock-{code}",
        score=score,
        rank=rank,
        buy_price=20.0 + rank,
    )


def _stock_data() -> SimpleNamespace:
    return SimpleNamespace(
        open_price=20.0,
        prev_close=19.0,
        price_940=21.0,
        high_940=21.2,
        low_940=19.8,
        volume_940=100_000.0,
        volume_937=70_000.0,
    )


def _scan_result() -> SimpleNamespace:
    ranked = [_scored("300835", 1), _scored("600549", 2, 0.4)]
    return SimpleNamespace(
        recommended=ranked,
        all_scored=ranked,
        stock_best_board={"300835": "稀土永磁", "600549": "稀土永磁"},
        stock_all_boards={
            "300835": ["稀土永磁"],
            "600549": ["稀土永磁"],
        },
        stock_gain_from_open={"300835": 1.2, "600549": 0.6},
        stock_is_driver={"300835": True, "600549": False},
        stock_cci={"300835": 100.0},
        stock_early_vol={"300835": 70_000.0},
        step2_board_avg_gains={"稀土永磁": 1.1},
        step2_all_board_avg_gains={"稀土永磁": 1.1},
        step2_boards_detail={"稀土永磁": ["300835", "600549"]},
        step0_universe_count=100,
        step2_hot_board_count=1,
        step2_filtered_by_avg_gain=0,
        step3_count=20,
        step4_count=15,
        step5_count=10,
        step6_count=8,
        step6_5_count=7,
        step6_6_count=2,
        final_candidates=2,
        step0_codes=["300835", "600549"],
        step2_codes=["300835", "600549"],
        step3_codes=["300835", "600549"],
        step4_codes=["300835", "600549"],
        step5_codes=["300835", "600549"],
        step6_codes=["300835", "600549"],
        step6_5_codes=["300835", "600549"],
        step6_6_codes=["300835", "600549"],
    )


def _freeze() -> dict:
    stock_data = {"300835": _stock_data(), "600549": _stock_data()}
    recommendation = {"stock_code": "300835", "latest_price": 21.0}
    return freeze_v16_scan_snapshot(
        _scan_result(),
        stock_data,
        recommendation,
        frozen_at=datetime(2026, 8, 25, 9, 40, tzinfo=BEIJING_TZ),
    )


def _config(
    root: Path,
    cache_path: Path,
    *,
    exclude_unrated: bool = False,
) -> V16DayGateShadowConfig:
    return V16DayGateShadowConfig(
        mode="shadow",
        top_k=10,
        evidence_dir=root / "data" / "v16_day_gate",
        send_feishu=False,
        approved_taxonomy_artifact_path=None,
        approved_taxonomy_artifact_sha256=None,
        board_relevance=BoardRelevanceConfig(
            cache_path=cache_path,
            allowed_levels=("高",),
            exclude_unrated=exclude_unrated,
            exclude_broad_boards=True,
        ),
        policy=V16DayGatePolicy(version="unfitted"),
        config_path=root / "config" / "v16-day-gate.yaml",
        config_hash="a" * 64,
    )


def _write_provenance_files(root: Path) -> None:
    paths = (
        root / "models" / "lgbrank_latest.txt",
        root / "models" / "feature_list.json",
        root / "src" / "strategy" / "strategies" / "v16_scanner.py",
        root / "data" / "board_constituents.json",
    )
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("test", encoding="utf-8")


def _runtime(
    root: Path,
    config: V16DayGateShadowConfig,
):
    runtime = freeze_v16_day_gate_runtime(
        root,
        ranking_model_sha256="1" * 64,
        ranking_feature_list_sha256="2" * 64,
        captured_at=datetime(2026, 8, 25, 9, 40, tzinfo=BEIJING_TZ),
        config=config,
    )
    assert runtime is not None
    return runtime


def test_freeze_is_json_native_and_detached_from_scanner_objects():
    result = _scan_result()
    stock_data = {"300835": _stock_data(), "600549": _stock_data()}
    recommendation = {"stock_code": "300835", "latest_price": 21.0}
    snapshot = freeze_v16_scan_snapshot(
        result,
        stock_data,
        recommendation,
        frozen_at=datetime(2026, 8, 25, 9, 40, tzinfo=BEIJING_TZ),
    )

    result.stock_all_boards["300835"].append("later mutation")
    recommendation["stock_code"] = "changed"

    assert snapshot["top_k"][0]["all_hot_boards"] == ["稀土永磁"]
    assert snapshot["recommendation_payload"]["stock_code"] == "300835"
    assert snapshot["effective_action"] == "pass_through"
    assert snapshot["mews"]["status"] == "unknown"
    assert "all_scored" not in snapshot
    assert snapshot["hot_board_member_counts"] == {"稀土永磁": 2}
    json.dumps(snapshot, ensure_ascii=False, allow_nan=False)


def test_prepare_uses_high_relevance_edges_and_keeps_policy_uncalibrated(
    tmp_path: Path,
):
    _write_provenance_files(tmp_path)
    cache_path = tmp_path / "data" / "board_relevance_cache.json"
    cache_path.write_text(
        json.dumps(
            {
                "稀土永磁::300835": {"level": "高", "reason": "core"},
                "稀土永磁::600549": {"level": "高", "reason": "core"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    prepared = prepare_shadow_decision(
        _freeze(),
        _runtime(tmp_path, _config(tmp_path, cache_path)),
        tmp_path,
    )

    assert prepared.decision.state is GateState.WATCH
    assert prepared.decision.blocks_trade is False
    assert prepared.decision.metrics.largest_cluster_share == 1.0
    assert {row["relevance_status"] for row in prepared.edge_audit} == {"allowed_relevance"}
    assert prepared.edge_coverage["relevance_stock_coverage"] == 1.0
    assert prepared.relevance_cache_hash is not None
    assert prepared.provenance["model_hash"] is not None


def test_unrated_edges_are_not_treated_as_negative_or_missing(
    tmp_path: Path,
):
    _write_provenance_files(tmp_path)
    cache_path = tmp_path / "data" / "board_relevance_cache.json"
    cache_path.write_text("{}", encoding="utf-8")

    prepared = prepare_shadow_decision(
        _freeze(),
        _runtime(tmp_path, _config(tmp_path, cache_path)),
        tmp_path,
    )

    assert prepared.decision.state is GateState.WATCH
    assert prepared.decision.blocks_trade is False
    assert {row["relevance_status"] for row in prepared.edge_audit} == {"unrated_retained"}
    assert prepared.decision.metrics.largest_cluster_share == 1.0
    assert prepared.edge_coverage["unrated_edges"] == 2


def test_relevance_exclusion_changes_only_parallel_diagnostic_graph(tmp_path: Path):
    _write_provenance_files(tmp_path)
    cache_path = tmp_path / "data" / "board_relevance_cache.json"
    cache_path.write_text("{}", encoding="utf-8")

    prepared = prepare_shadow_decision(
        _freeze(),
        _runtime(
            tmp_path,
            _config(tmp_path, cache_path, exclude_unrated=True),
        ),
        tmp_path,
    )

    assert prepared.decision.state is GateState.WATCH
    assert prepared.decision.metrics.largest_cluster_share == 1.0
    assert prepared.relevance_filtered_metrics.largest_cluster_share == 0.5
    assert prepared.edge_coverage["relevance_stock_coverage"] == 0.0


def test_runtime_freeze_defers_legacy_relevance_io_to_background(
    tmp_path: Path,
    monkeypatch,
):
    _write_provenance_files(tmp_path)
    cache_path = tmp_path / "data" / "board_relevance_cache.json"
    cache_path.write_text("{}", encoding="utf-8")
    calls = 0

    def fail_on_relevance(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise RuntimeError("diagnostic relevance read")

    monkeypatch.setattr(shadow_module, "_load_relevance_cache", fail_on_relevance)
    runtime = _runtime(tmp_path, _config(tmp_path, cache_path))
    assert calls == 0

    with pytest.raises(RuntimeError, match="diagnostic relevance read"):
        prepare_shadow_decision(_freeze(), runtime, tmp_path)
    assert calls == 1


def test_watch_is_never_reported_as_a_hypothetical_block(tmp_path: Path):
    _write_provenance_files(tmp_path)
    cache_path = tmp_path / "data" / "board_relevance_cache.json"
    cache_path.write_text("{}", encoding="utf-8")
    snapshot = _freeze()
    prepared = prepare_shadow_decision(
        snapshot,
        _runtime(tmp_path, _config(tmp_path, cache_path)),
        tmp_path,
    )

    metadata = prepared_to_metadata(prepared)
    message = shadow_message(snapshot, prepared, None)

    assert metadata["hypothetical_action"] == "watch_undecided"
    assert "hypothetical_action: WATCH_UNDECIDED" in message
    assert "BLOCK_NEW_ENTRY" not in message
    json.dumps(metadata, ensure_ascii=False, allow_nan=False)


def test_taxonomy_requires_an_exact_externally_approved_hash(tmp_path: Path):
    _write_provenance_files(tmp_path)
    cache_path = tmp_path / "data" / "board_relevance_cache.json"
    cache_path.write_text("{}", encoding="utf-8")
    taxonomy = {
        "taxonomy_version": "reviewed-test-v1",
        "themes": [
            {
                "canonical_theme_id": "theme:rare-earth-magnets",
                "canonical_name": "rare earth magnets",
                "label": "theme",
                "aliases": ["稀土永磁"],
            }
        ],
    }
    approval_artifact = {
        "schema_version": TAXONOMY_APPROVAL_SCHEMA_VERSION,
        "artifact_type": "v16_theme_taxonomy_approval",
        "approval": {
            "status": "human_approved",
            "reviewed_by": "unit-test-reviewer",
            "reviewed_at": "2026-08-25T12:00:00+08:00",
            "review_ref": "unit-test-review",
            "source_candidate_manifest_sha256": None,
            "taxonomy_sha256": taxonomy_sha256(taxonomy),
        },
        "taxonomy": taxonomy,
    }
    taxonomy_path = tmp_path / "config" / "approved-taxonomy-artifact.json"
    taxonomy_path.parent.mkdir(parents=True, exist_ok=True)
    taxonomy_path.write_text(
        json.dumps(approval_artifact, ensure_ascii=False),
        encoding="utf-8",
    )
    base_config = _config(tmp_path, cache_path)

    bare_candidate_path = tmp_path / "config" / "candidate-taxonomy.json"
    bare_candidate_path.write_text(
        json.dumps(taxonomy, ensure_ascii=False),
        encoding="utf-8",
    )
    bare_candidate = replace(
        base_config,
        approved_taxonomy_artifact_path=bare_candidate_path,
        approved_taxonomy_artifact_sha256=taxonomy_sha256(taxonomy),
    )
    with pytest.raises(UnapprovedThemeSemanticsError, match="human-approval artifact"):
        _runtime(tmp_path, bare_candidate)

    wrong = replace(
        base_config,
        approved_taxonomy_artifact_path=taxonomy_path,
        approved_taxonomy_artifact_sha256="0" * 64,
    )

    with pytest.raises(UnapprovedThemeSemanticsError, match="approved artifact hash"):
        _runtime(tmp_path, wrong)

    approved = replace(
        base_config,
        approved_taxonomy_artifact_path=taxonomy_path,
        approved_taxonomy_artifact_sha256=approved_taxonomy_artifact_sha256(approval_artifact),
    )
    runtime = _runtime(tmp_path, approved)

    # Once frozen, queued work cannot drift to replacement config/taxonomy
    # files.  Only the diagnostic relevance cache remains evaluation-time.
    taxonomy_path.write_text("{}", encoding="utf-8")
    approved.config_path.write_text("mode: live", encoding="utf-8")
    prepared = prepare_shadow_decision(_freeze(), runtime, tmp_path)

    assert prepared.gate_input.taxonomy_version == "reviewed-test-v1"
    assert prepared.gate_input.model_version == "1" * 64
    assert prepared.provenance["feature_hash"] == "2" * 64
    assert prepared.provenance["context_id"] == runtime.context_id
    assert prepared.provenance["context_captured_at"] == _freeze()["decision_cutoff"]
    assert (
        prepared.provenance["taxonomy_approval_artifact_hash"]
        == approved.approved_taxonomy_artifact_sha256
    )
    assert prepared.edge_coverage["taxonomy_mapped_stock_coverage"] == 1.0


def test_repository_config_is_shadow_and_has_no_fitted_thresholds():
    project_root = Path(__file__).resolve().parents[3]

    config = load_shadow_config(project_root)

    assert config.mode == "shadow"
    assert config.policy.has_rules is False
    assert config.board_relevance.allowed_levels == ("高",)
    assert config.board_relevance.exclude_unrated is False


def test_off_runtime_returns_before_snapshot_or_relevance_work(tmp_path: Path):
    cache_path = tmp_path / "data" / "missing-relevance.json"
    config = replace(_config(tmp_path, cache_path), mode="off")

    runtime = freeze_v16_day_gate_runtime(
        tmp_path,
        ranking_model_sha256="1" * 64,
        ranking_feature_list_sha256="2" * 64,
        captured_at=datetime(2026, 8, 25, 9, 40, tzinfo=BEIJING_TZ),
        config=config,
    )

    assert runtime is None


def test_config_rejects_enforce_even_if_somebody_edits_yaml(tmp_path: Path):
    source_root = Path(__file__).resolve().parents[3]
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    text = (source_root / "config" / "v16-day-gate.yaml").read_text(encoding="utf-8")
    (config_dir / "v16-day-gate.yaml").write_text(
        text.replace("mode: shadow", "mode: enforce"),
        encoding="utf-8",
    )

    with pytest.raises(V16DayGateShadowError, match="off.*shadow"):
        load_shadow_config(tmp_path)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ("top_k: 10", "top_k: 11", "1 to 10"),
        (
            "min_largest_cluster_share: null",
            "min_largest_cluster_share: .nan",
            "finite",
        ),
        ('    - "高"', '    - "typo"', "unknown values"),
        (
            "approved_taxonomy_artifact_sha256: null",
            f'approved_taxonomy_artifact_sha256: "{"0" * 64}"',
            "must be set together",
        ),
        (
            "mode: shadow",
            "mode: shadow\nmode: off",
            "duplicate key",
        ),
    ],
)
def test_config_rejects_ambiguous_or_non_finite_values(
    tmp_path: Path,
    old: str,
    new: str,
    message: str,
):
    source_root = Path(__file__).resolve().parents[3]
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    text = (source_root / "config" / "v16-day-gate.yaml").read_text(encoding="utf-8")
    assert old in text
    (config_dir / "v16-day-gate.yaml").write_text(
        text.replace(old, new, 1),
        encoding="utf-8",
    )

    with pytest.raises(V16DayGateShadowError, match=message):
        load_shadow_config(tmp_path)
