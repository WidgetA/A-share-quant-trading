"""Loaded-artifact provenance for the production LGBRank scorer."""

from __future__ import annotations

import hashlib
from pathlib import Path

from src.strategy.lgbrank_scorer import LGBRankScorer


def test_scorer_hashes_the_exact_model_and_feature_bytes_it_loaded(tmp_path: Path):
    project_root = Path(__file__).resolve().parents[3]
    model_bytes = (project_root / "models" / "lgbrank_latest.txt").read_bytes()
    feature_bytes = (project_root / "models" / "feature_list.json").read_bytes()
    model_path = tmp_path / "model.txt"
    feature_path = tmp_path / "features.json"
    model_path.write_bytes(model_bytes)
    feature_path.write_bytes(feature_bytes)

    scorer = LGBRankScorer(model_path, feature_path)

    expected_model_hash = hashlib.sha256(model_bytes).hexdigest()
    expected_feature_hash = hashlib.sha256(feature_bytes).hexdigest()
    assert scorer.model_sha256 == expected_model_hash
    assert scorer.feature_list_sha256 == expected_feature_hash
    assert scorer.model.num_feature() == len(scorer.features)

    # A deployment replacement after construction cannot rewrite the loaded
    # scorer identity later consumed by the frozen DayGate runtime.
    model_path.write_text("replacement", encoding="utf-8")
    feature_path.write_text("{}", encoding="utf-8")
    assert scorer.model_sha256 == expected_model_hash
    assert scorer.feature_list_sha256 == expected_feature_hash
