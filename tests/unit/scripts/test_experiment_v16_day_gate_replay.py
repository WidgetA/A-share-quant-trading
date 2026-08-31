"""Tests for the research-only V16DayGate historical replay."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from scripts.experiment_v16_day_gate_replay import (
    BOARD_PROXY_LABEL,
    ReplayCandidate,
    build_day_metrics,
    build_diagnostic_candidates,
    candidate_pass_mask,
    generate_candidates,
    load_stock_panel,
    load_taxonomy,
    portfolio_stats,
    select_development_champions,
    select_diagnostic_champions,
)


def _write_panel_and_cache(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cache_path = cache_dir / "20250102.json"
    boards = ["A", "A", "A", "A", "B", "B", "B", "C", "D", "E"]
    gains = [0.8, 0.799, 1.2, 0.1, 0.9, 0.2, 1.0, 0.3, 0.8, 0.4]
    picks = [
        {
            "rank": rank,
            "code": f"{rank:06d}",
            "board": board,
            "gain_0938": gain,
        }
        for rank, (board, gain) in enumerate(zip(boards, gains, strict=True), start=1)
    ]
    cache_path.write_text(
        json.dumps(
            {
                "picks": picks,
                "source": "unit-test-cache",
                "final_candidates": 42,
                "funnel": {"hot_boards": 7},
            }
        ),
        encoding="utf-8",
    )
    rows = [
        {
            "date": "20250102",
            "rank": pick["rank"],
            "code": pick["code"],
            "board": pick["board"],
            "score": 1.0 / pick["rank"],
            "net_return_t2": (pick["rank"] - 5) / 100,
            "v16_cache_path": "cache\\20250102.json",
        }
        for pick in picks
    ]
    panel_path = tmp_path / "stock_panel.csv"
    pd.DataFrame(rows).to_csv(panel_path, index=False)
    return panel_path


def test_build_day_metrics_uses_best_board_proxy_and_gain_0938_driver(tmp_path):
    panel = load_stock_panel(_write_panel_and_cache(tmp_path))
    metrics = build_day_metrics(panel, tmp_path)

    assert len(metrics) == 1
    row = metrics.iloc[0]
    assert row["board_structure"] == BOARD_PROXY_LABEL
    assert row["largest_cluster_size"] == 4
    assert row["largest_cluster_share"] == pytest.approx(0.4)
    assert row["effective_cluster_count"] == pytest.approx(1 / 0.28)
    # Exactly 0.8 is a driver; 0.799 is not.
    assert row["driver_count"] == 5
    assert row["driver_breadth"] == pytest.approx(0.5)
    assert row["driver_flags"].split("|")[:2] == ["1", "0"]
    assert row["hot_board_count"] == 7
    assert row["final_candidates"] == 42
    expected_gains = [0.8, 0.799, 1.2, 0.1, 0.9, 0.2, 1.0, 0.3, 0.8, 0.4]
    assert row["gain_0938_mean"] == pytest.approx(sum(expected_gains) / 10)
    assert row["top1_net_return_t2"] == pytest.approx(-0.04)
    assert row["top3_net_return_t2"] == pytest.approx(-0.03)


def test_optional_taxonomy_can_join_raw_best_boards(tmp_path):
    panel = load_stock_panel(_write_panel_and_cache(tmp_path))
    metrics = build_day_metrics(
        panel,
        tmp_path,
        canonical_theme_map={"A": "theme-x", "B": "theme-x"},
        taxonomy_version="test-v1",
    )

    row = metrics.iloc[0]
    assert row["largest_cluster_size"] == 7
    assert row["largest_cluster_share"] == pytest.approx(0.7)
    assert row["raw_largest_cluster_share"] == pytest.approx(0.4)
    assert row["taxonomy_version"] == "test-v1"


def test_load_taxonomy_supports_simple_and_alias_formats(tmp_path):
    simple = tmp_path / "simple.json"
    simple.write_text(json.dumps({"A": "theme-x", "B": "theme-x"}), encoding="utf-8")
    mapping, meta = load_taxonomy(simple)
    assert mapping == {"A": "theme-x", "B": "theme-x"}
    assert meta["format"] == "simple_board_to_theme"

    aliases = tmp_path / "aliases.json"
    aliases.write_text(
        json.dumps(
            {
                "taxonomy_version": "approved-v1",
                "themes": [
                    {
                        "canonical_theme_id": "theme:x",
                        "aliases": ["A", "B"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    mapping, meta = load_taxonomy(aliases)
    assert mapping == {"A": "theme:x", "B": "theme:x"}
    assert meta["taxonomy_version"] == "approved-v1"
    assert meta["format"] == "themes_aliases"


def test_portfolio_stats_assigns_zero_return_to_skipped_days():
    returns = pd.Series([0.10, -0.20, -0.03, 0.05])
    execute = pd.Series([True, False, True, True])

    stats = portfolio_stats(returns, execute)

    assert stats["n_days"] == 4
    assert stats["executed_days"] == 3
    assert stats["coverage"] == pytest.approx(0.75)
    assert stats["compound_return"] == pytest.approx(1.10 * 0.97 * 1.05 - 1)
    assert stats["max_drawdown"] == pytest.approx(-0.03)
    assert stats["mean_return"] == pytest.approx(0.04)
    assert stats["win_rate"] == pytest.approx(2 / 3)
    assert stats["severe_loss_count"] == 1
    assert stats["severe_loss_rate"] == pytest.approx(1 / 3)


def test_declared_grid_contains_baseline_negative_control_and_combinations():
    candidates = generate_candidates()

    assert len(candidates) == 720
    named = {candidate.rule_name: candidate for candidate in candidates if candidate.rule_name}
    assert named["baseline_all_days"].is_baseline
    assert named["simple_40pct_cluster"].min_largest_cluster_share == 0.4
    assert named["all_four_predeclared"].max_effective_cluster_count == 4.0
    assert named["all_four_predeclared"].min_top3_main_cluster_coverage == pytest.approx(2 / 3)


def test_candidate_thresholds_are_inclusive():
    frame = pd.DataFrame(
        {
            "largest_cluster_share": [0.4, 0.399],
            "effective_cluster_count": [4.0, 4.0],
            "top3_main_cluster_coverage": [2 / 3, 2 / 3],
            "driver_breadth": [0.4, 0.4],
        }
    )
    candidate = ReplayCandidate(
        candidate_id="boundary",
        min_largest_cluster_share=0.4,
        max_effective_cluster_count=4.0,
        min_top3_main_cluster_coverage=2 / 3,
        min_driver_breadth=0.4,
    )

    assert candidate_pass_mask(frame, candidate).tolist() == [True, False]


def test_selection_rejects_any_validation_rows_and_uses_development_only():
    rows = []
    for basket in ("top1", "top3", "top10"):
        for candidate_id, compound in (("candidate-a", 0.20), ("candidate-b", 0.30)):
            rows.append(
                {
                    "partition": "development_through_2025",
                    "basket": basket,
                    "candidate_id": candidate_id,
                    "rule_name": "",
                    "family": "grid_exploratory",
                    "coverage": 0.5,
                    "executed_days": 150,
                    "compound_return": compound,
                    "max_drawdown": -0.1,
                }
            )
    development = pd.DataFrame(rows)

    selected = select_development_champions(development)
    assert {value["candidate_id"] for value in selected.values()} == {"candidate-b"}

    contaminated = pd.concat(
        [development, development.assign(partition="validation_2026", compound_return=99.0)],
        ignore_index=True,
    )
    with pytest.raises(ValueError, match="development_through_2025"):
        select_development_champions(contaminated)


def test_diagnostic_thresholds_use_development_features_not_returns():
    development = pd.DataFrame(
        {
            "partition": ["development_through_2025"] * 10,
            "gain_0938_mean": range(10),
            "gain_0938_median": range(10),
            "gain_0938_ge_3_share": [value / 10 for value in range(10)],
            "gain_0938_ge_5_share": [value / 10 for value in range(10)],
            "hot_board_count": range(10, 20),
            "final_candidates": range(20, 30),
            "score_softmax_hhi": [0.1 + value / 100 for value in range(10)],
            "score_top1_softmax_share": [0.2 + value / 100 for value in range(10)],
            "top10_net_return_t2": [99.0] * 10,
        }
    )
    first = build_diagnostic_candidates(development)
    second = build_diagnostic_candidates(development.assign(top10_net_return_t2=[-99.0] * 10))

    assert first == second
    assert len(first) == 16
    gain_q75 = next(
        candidate for candidate in first if candidate.diagnostic_id == "gain_0938_mean_ge_dev_q75"
    )
    assert gain_q75.threshold == 7.0


def test_diagnostic_selection_rejects_validation_rows():
    rows = []
    for basket in ("top1", "top3", "top10"):
        rows.append(
            {
                "partition": "development_through_2025",
                "basket": basket,
                "diagnostic_id": "gain_q75",
                "feature": "gain_0938_mean",
                "development_quantile": 0.75,
                "threshold": 3.0,
                "coverage": 0.75,
                "risk_days": 100,
                "compound_return": 0.2,
                "max_drawdown": -0.1,
            }
        )
    development = pd.DataFrame(rows)
    selected = select_diagnostic_champions(development)
    assert {item["diagnostic_id"] for item in selected.values()} == {"gain_q75"}

    with pytest.raises(ValueError, match="development metrics only"):
        select_diagnostic_champions(
            pd.concat(
                [development, development.assign(partition="validation_2026")],
                ignore_index=True,
            )
        )
