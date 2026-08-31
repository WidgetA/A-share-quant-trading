"""Replay V16DayGate on the frozen historical V16 Top10 research panel.

This script is research-only.  It consumes the existing 09:40 -> T+2 net
returns (already net of 0.20% round-trip cost) and reconstructs only the
historical information that is actually available:

* one ``best board`` per stock, used as a proxy for the production
  ``stock_all_boards`` graph; and
* ``gain_0938 >= 0.8`` as a proxy driver flag.

Threshold selection is structurally restricted to observations dated no later
than 2025-12-31.  The 2026 partition is evaluated only after development
champions have been frozen.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

MAIN_WORKTREE = Path(__file__).resolve().parents[1]
REPO_ROOT = MAIN_WORKTREE.parents[2]
if str(MAIN_WORKTREE) not in sys.path:
    sys.path.insert(0, str(MAIN_WORKTREE))

from src.strategy.v16_day_gate import (  # noqa: E402
    GateMode,
    V16DayGate,
    V16DayGateInput,
    V16DayGatePolicy,
)

DEFAULT_STOCK_PANEL = (
    REPO_ROOT
    / "strategy-research"
    / "kangdie"
    / "explore"
    / "v16_sector_concentration"
    / "results"
    / "stock_panel.csv"
)
DEFAULT_OUTPUT = REPO_ROOT / "strategy-research" / "kangdie" / "explore" / "v16_day_gate_replay"
GATE_SOURCE = MAIN_WORKTREE / "src" / "strategy" / "v16_day_gate.py"

DEVELOPMENT_END = "20251231"
VALIDATION_START = "20260101"
VALIDATION_END = "20261231"
DRIVER_GAIN_0938_THRESHOLD = 0.8
ROUND_TRIP_COST = 0.002
SEVERE_LOSS_THRESHOLD = -0.02
MIN_SELECTION_COVERAGE = 0.30
MIN_SELECTION_EXECUTIONS = 100
BOARD_PROXY_LABEL = "historical_best_board_only_proxy"
BASKETS = ("top1", "top3", "top10")
FOCUS_DATES = ("20260817", "20260818")
DIAGNOSTIC_QUANTILES = (0.75, 0.90)
MIN_DIAGNOSTIC_RISK_DAYS = 20
DIAGNOSTIC_FEATURES = (
    "gain_0938_mean",
    "gain_0938_median",
    "gain_0938_ge_3_share",
    "gain_0938_ge_5_share",
    "hot_board_count",
    "final_candidates",
    "score_softmax_hhi",
    "score_top1_softmax_share",
)

# Exhaustive Cartesian sweep over this grid.  The grid is declared in code and
# does not depend on 2026 outcomes.  None means that dimension is inactive.
LARGEST_SHARE_GRID = (None, 0.3, 0.4, 0.5, 0.6, 0.7)
EFFECTIVE_CLUSTER_GRID = (None, 2.0, 3.0, 4.0, 5.0, 6.0)
TOP3_COVERAGE_GRID = (None, 1 / 3, 2 / 3, 1.0)
DRIVER_BREADTH_GRID = (None, 0.2, 0.4, 0.6, 0.8)


@dataclass(frozen=True)
class ReplayCandidate:
    """One frozen DayGate threshold combination."""

    candidate_id: str
    min_largest_cluster_share: float | None = None
    max_effective_cluster_count: float | None = None
    min_top3_main_cluster_coverage: float | None = None
    min_driver_breadth: float | None = None
    rule_name: str = ""
    family: str = "grid_exploratory"

    @property
    def is_baseline(self) -> bool:
        return all(
            value is None
            for value in (
                self.min_largest_cluster_share,
                self.max_effective_cluster_count,
                self.min_top3_main_cluster_coverage,
                self.min_driver_breadth,
            )
        )

    def policy(self) -> V16DayGatePolicy | None:
        """Return the matching shadow policy, or None for the baseline."""

        if self.is_baseline:
            return None
        return V16DayGatePolicy(
            version=f"historical-grid/{self.candidate_id}",
            mode=GateMode.SHADOW,
            min_largest_cluster_share=self.min_largest_cluster_share,
            max_effective_cluster_count=self.max_effective_cluster_count,
            min_top3_main_cluster_coverage=self.min_top3_main_cluster_coverage,
            min_driver_breadth=self.min_driver_breadth,
        )


@dataclass(frozen=True)
class DiagnosticCandidate:
    """High-tail crowding/exhaustion diagnostic frozen on development features."""

    diagnostic_id: str
    feature: str
    development_quantile: float
    threshold: float


PREDECLARED_RULES: dict[tuple[float | None, ...], str] = {
    (None, None, None, None): "baseline_all_days",
    (0.4, None, None, None): "simple_40pct_cluster",
    (None, 4.0, None, None): "effective_clusters_le_4",
    (None, None, 2 / 3, None): "top3_main_cluster_ge_2of3",
    (None, None, None, 0.4): "driver_breadth_ge_40pct",
    (0.4, None, 2 / 3, None): "cluster40_and_top3_2of3",
    (0.4, None, None, 0.4): "cluster40_and_driver40",
    (None, None, 2 / 3, 0.4): "top3_2of3_and_driver40",
    (0.4, 4.0, 2 / 3, 0.4): "all_four_predeclared",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _number_token(value: float | None) -> str:
    if value is None:
        return "none"
    return f"{value:.6f}".rstrip("0").rstrip(".").replace(".", "p")


def candidate_id(
    largest_share: float | None,
    effective_clusters: float | None,
    top3_coverage: float | None,
    driver_breadth: float | None,
) -> str:
    return "__".join(
        (
            f"lcs_{_number_token(largest_share)}",
            f"ecc_{_number_token(effective_clusters)}",
            f"t3_{_number_token(top3_coverage)}",
            f"drv_{_number_token(driver_breadth)}",
        )
    )


def generate_candidates() -> list[ReplayCandidate]:
    """Generate every combination in the pre-specified Cartesian grid."""

    candidates: list[ReplayCandidate] = []
    for largest, effective, top3, driver in product(
        LARGEST_SHARE_GRID,
        EFFECTIVE_CLUSTER_GRID,
        TOP3_COVERAGE_GRID,
        DRIVER_BREADTH_GRID,
    ):
        thresholds = (largest, effective, top3, driver)
        rule_name = PREDECLARED_RULES.get(thresholds, "")
        family = "grid_exploratory"
        if rule_name == "baseline_all_days":
            family = "baseline"
        elif rule_name:
            family = "predeclared"
        candidate = ReplayCandidate(
            candidate_id=candidate_id(*thresholds),
            min_largest_cluster_share=largest,
            max_effective_cluster_count=effective,
            min_top3_main_cluster_coverage=top3,
            min_driver_breadth=driver,
            rule_name=rule_name,
            family=family,
        )
        candidate.policy()  # Validate against the production-independent policy schema.
        candidates.append(candidate)
    return candidates


def load_taxonomy(
    path: Path | None,
) -> tuple[dict[str, str], dict[str, Any]]:
    """Load an optional board-to-theme map.

    Supported formats are a simple ``{"board": "theme"}`` object, an object
    containing ``board_to_theme``, or the versioned ``themes/aliases`` example
    format.  No taxonomy is auto-discovered: callers must opt in explicitly.
    """

    if path is None:
        return {}, {
            "enabled": False,
            "format": "raw_board",
            "taxonomy_version": None,
            "path": None,
            "sha256": None,
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("taxonomy JSON must be an object")

    mapping: dict[str, str] = {}
    taxonomy_format = "simple_board_to_theme"
    if isinstance(payload.get("board_to_theme"), dict):
        raw_mapping = payload["board_to_theme"]
        mapping = _clean_mapping(raw_mapping)
        taxonomy_format = "board_to_theme_field"
    elif isinstance(payload.get("themes"), list):
        taxonomy_format = "themes_aliases"
        for theme in payload["themes"]:
            if not isinstance(theme, dict):
                raise ValueError("each taxonomy themes entry must be an object")
            canonical = str(
                theme.get("canonical_theme_id") or theme.get("canonical_name") or ""
            ).strip()
            if not canonical:
                raise ValueError("taxonomy theme is missing a canonical id/name")
            aliases = theme.get("aliases", [])
            if not isinstance(aliases, list):
                raise ValueError("taxonomy aliases must be a list")
            for alias in aliases:
                board = str(alias).strip()
                if not board:
                    continue
                previous = mapping.get(board)
                if previous is not None and previous != canonical:
                    raise ValueError(f"taxonomy alias {board!r} maps to conflicting themes")
                mapping[board] = canonical
    else:
        mapping = _clean_mapping(payload)

    if not mapping:
        raise ValueError("taxonomy contains no usable board-to-theme mappings")
    return mapping, {
        "enabled": True,
        "format": taxonomy_format,
        "taxonomy_version": str(payload.get("taxonomy_version") or sha256(path)[:12]),
        "path": str(path.resolve()),
        "sha256": sha256(path),
        "mapped_boards": len(mapping),
    }


def _clean_mapping(payload: dict[Any, Any]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    ignored_metadata = {"taxonomy_version", "approved", "description"}
    for raw_board, raw_theme in payload.items():
        if str(raw_board) in ignored_metadata:
            continue
        if not isinstance(raw_theme, str):
            raise ValueError("simple taxonomy values must all be strings")
        board = str(raw_board).strip()
        theme = raw_theme.strip()
        if board and theme:
            mapping[board] = theme
    return mapping


def resolve_cache_path(repo_root: Path, raw_path: str) -> Path:
    """Resolve the panel's repository-relative Windows path portably."""

    normalized = raw_path.strip().replace("\\", "/")
    path = Path(normalized)
    return path if path.is_absolute() else repo_root / path


def load_stock_panel(path: Path) -> pd.DataFrame:
    required = {
        "date",
        "rank",
        "code",
        "board",
        "score",
        "net_return_t2",
        "v16_cache_path",
    }
    frame = pd.read_csv(
        path,
        dtype={"date": "string", "code": "string", "v16_cache_path": "string"},
    )
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"stock panel missing columns: {', '.join(missing)}")
    frame = frame.copy()
    frame["date"] = frame["date"].astype(str).str.replace("-", "", regex=False)
    frame["code"] = frame["code"].astype(str).str.zfill(6)
    frame["rank"] = pd.to_numeric(frame["rank"], errors="raise").astype(int)
    frame["score"] = pd.to_numeric(frame["score"], errors="raise")
    frame["net_return_t2"] = pd.to_numeric(frame["net_return_t2"], errors="raise")
    frame["board"] = frame["board"].fillna("").astype(str).str.strip()
    frame = frame.sort_values(["date", "rank"]).reset_index(drop=True)

    if frame.empty:
        raise ValueError("stock panel is empty")
    if frame["date"].str.fullmatch(r"\d{8}").eq(False).any():
        raise ValueError("stock panel dates must be YYYYMMDD")
    if frame["board"].eq("").any():
        raise ValueError("stock panel contains blank best-board values")
    if not np.isfinite(frame["net_return_t2"]).all():
        raise ValueError("stock panel contains non-finite net_return_t2")
    if not np.isfinite(frame["score"]).all():
        raise ValueError("stock panel contains non-finite score")
    if frame["net_return_t2"].le(-1).any():
        raise ValueError("net_return_t2 <= -100% cannot be compounded")

    expected_ranks = tuple(range(1, 11))
    for day, group in frame.groupby("date", sort=True):
        ranks = tuple(sorted(group["rank"].tolist()))
        if ranks != expected_ranks:
            raise ValueError(f"{day}: expected exactly ranks 1..10, got {ranks}")
        if group["code"].duplicated().any():
            raise ValueError(f"{day}: duplicate stock code in Top10")
        if group["v16_cache_path"].nunique(dropna=False) != 1:
            raise ValueError(f"{day}: expected one v16_cache_path")
    return frame


def _cache_picks(repo_root: Path, raw_path: str) -> tuple[dict[int, dict], dict]:
    cache_path = resolve_cache_path(repo_root, raw_path)
    if not cache_path.exists():
        raise FileNotFoundError(f"V16 cache does not exist: {cache_path}")
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    picks = payload.get("picks")
    if not isinstance(picks, list) or len(picks) != 10:
        raise ValueError(f"{cache_path}: expected exactly 10 cached picks")
    by_rank: dict[int, dict] = {}
    for pick in picks:
        rank = int(pick["rank"])
        if rank in by_rank:
            raise ValueError(f"{cache_path}: duplicate rank {rank}")
        by_rank[rank] = pick
    return by_rank, payload


def build_day_metrics(
    panel: pd.DataFrame,
    repo_root: Path,
    canonical_theme_map: dict[str, str] | None = None,
    taxonomy_version: str | None = None,
) -> pd.DataFrame:
    """Call V16DayGate once per day and retain its raw graph metrics."""

    gate = V16DayGate()
    rows: list[dict[str, Any]] = []
    for day, group in panel.groupby("date", sort=True):
        if day > VALIDATION_END:
            raise ValueError(f"{day}: protocol only defines validation through 2026")
        ordered = group.sort_values("rank")
        raw_cache_path = str(ordered["v16_cache_path"].iloc[0])
        cached_by_rank, payload = _cache_picks(repo_root, raw_cache_path)
        codes = tuple(ordered["code"].tolist())
        boards: dict[str, tuple[str, ...]] = {}
        drivers: dict[str, bool] = {}
        gains: list[float] = []

        for row in ordered.itertuples(index=False):
            cached = cached_by_rank[int(row.rank)]
            cached_code = str(cached.get("code", "")).zfill(6)
            if cached_code != row.code:
                raise ValueError(
                    f"{day} rank {row.rank}: panel code {row.code} != cache {cached_code}"
                )
            gain = float(cached.get("gain_0938", math.nan))
            if not math.isfinite(gain):
                raise ValueError(f"{day} rank {row.rank}: missing/non-finite gain_0938")
            gains.append(gain)
            boards[row.code] = (str(row.board),)
            drivers[row.code] = gain >= DRIVER_GAIN_0938_THRESHOLD

        cutoff = datetime.strptime(day, "%Y%m%d").replace(
            hour=9,
            minute=40,
            tzinfo=ZoneInfo("Asia/Shanghai"),
        )
        model_version = str(payload.get("source") or "historical-v16-proxy")
        raw_decision = gate.evaluate(
            V16DayGateInput(
                cutoff_ts=cutoff,
                ranked_top_k=codes,
                stock_all_boards=boards,
                stock_is_driver=drivers,
                model_version=model_version,
            )
        )
        decision = raw_decision
        if canonical_theme_map:
            decision = gate.evaluate(
                V16DayGateInput(
                    cutoff_ts=cutoff,
                    ranked_top_k=codes,
                    stock_all_boards=boards,
                    stock_is_driver=drivers,
                    model_version=model_version,
                    canonical_theme_map=canonical_theme_map,
                    taxonomy_version=taxonomy_version,
                )
            )
        raw_metrics = raw_decision.metrics
        metrics = decision.metrics
        returns = ordered["net_return_t2"].to_numpy(dtype=float)
        scores = ordered["score"].to_numpy(dtype=float)
        score_exp = np.exp(scores - scores.max())
        score_weights = score_exp / score_exp.sum()
        funnel = payload.get("funnel")
        if not isinstance(funnel, dict) or "hot_boards" not in funnel:
            raise ValueError(f"{day}: cache is missing funnel.hot_boards")
        hot_board_count = int(funnel["hot_boards"])
        final_candidates = int(payload.get("final_candidates", -1))
        if hot_board_count < 0 or final_candidates < 0:
            raise ValueError(f"{day}: invalid hot_board_count/final_candidates")
        gains_array = np.asarray(gains, dtype=float)
        rows.append(
            {
                "date": day,
                "partition": (
                    "development_through_2025" if day <= DEVELOPMENT_END else "validation_2026"
                ),
                "board_structure": BOARD_PROXY_LABEL,
                "metric_basis": "taxonomy_proxy" if canonical_theme_map else "raw_best_board",
                "taxonomy_version": taxonomy_version or "raw_board",
                "v16_source": str(payload.get("source") or ""),
                "v16_cache_path": raw_cache_path,
                "ranked_codes": "|".join(codes),
                "best_boards": "|".join(str(value) for value in ordered["board"]),
                "gain_0938": "|".join(f"{value:.8f}" for value in gains),
                "driver_flags": "|".join("1" if drivers[code] else "0" for code in codes),
                "ranked_count": metrics.ranked_count,
                "themed_stock_count": metrics.themed_stock_count,
                "theme_coverage": metrics.theme_coverage,
                "component_count": metrics.component_count,
                "largest_cluster_size": metrics.largest_cluster_size,
                "largest_cluster_share": metrics.largest_cluster_share,
                "largest_cluster_codes": "|".join(metrics.largest_cluster_codes),
                "largest_cluster_themes": "|".join(metrics.largest_cluster_themes),
                "effective_cluster_count": metrics.effective_cluster_count,
                "top3_main_cluster_coverage": metrics.top3_main_cluster_coverage,
                "driver_count": metrics.driver_count,
                "driver_breadth": metrics.driver_breadth,
                "gain_0938_mean": float(gains_array.mean()),
                "gain_0938_median": float(np.median(gains_array)),
                "gain_0938_ge_3_share": float((gains_array >= 3.0).mean()),
                "gain_0938_ge_5_share": float((gains_array >= 5.0).mean()),
                "hot_board_count": hot_board_count,
                "final_candidates": final_candidates,
                "score_softmax_hhi": float(np.square(score_weights).sum()),
                "score_top1_softmax_share": float(score_weights[0]),
                "raw_component_count": raw_metrics.component_count,
                "raw_largest_cluster_size": raw_metrics.largest_cluster_size,
                "raw_largest_cluster_share": raw_metrics.largest_cluster_share,
                "raw_effective_cluster_count": raw_metrics.effective_cluster_count,
                "raw_top3_main_cluster_coverage": raw_metrics.top3_main_cluster_coverage,
                "top1_net_return_t2": float(returns[:1].mean()),
                "top3_net_return_t2": float(returns[:3].mean()),
                "top10_net_return_t2": float(returns[:10].mean()),
            }
        )
    result = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    if result.empty:
        raise ValueError("no day metrics were built")
    return result


def candidate_pass_mask(frame: pd.DataFrame, candidate: ReplayCandidate) -> pd.Series:
    """Apply the same inclusive threshold semantics as V16DayGatePolicy."""

    mask = pd.Series(True, index=frame.index)
    if candidate.min_largest_cluster_share is not None:
        mask &= frame["largest_cluster_share"].ge(candidate.min_largest_cluster_share)
    if candidate.max_effective_cluster_count is not None:
        mask &= frame["effective_cluster_count"].le(candidate.max_effective_cluster_count)
    if candidate.min_top3_main_cluster_coverage is not None:
        mask &= frame["top3_main_cluster_coverage"].ge(candidate.min_top3_main_cluster_coverage)
    if candidate.min_driver_breadth is not None:
        mask &= frame["driver_breadth"].ge(candidate.min_driver_breadth)
    return mask


def portfolio_stats(
    returns: pd.Series,
    execute_mask: pd.Series,
) -> dict[str, float | int | None]:
    """Calculate an event-sequence proxy, assigning zero return to skipped days."""

    values = returns.to_numpy(dtype=float)
    mask = execute_mask.to_numpy(dtype=bool)
    if len(values) != len(mask):
        raise ValueError("returns and execute_mask length mismatch")
    if not np.isfinite(values).all():
        raise ValueError("returns contain non-finite values")
    executed = values[mask]
    strategy_returns = np.where(mask, values, 0.0)
    wealth = np.cumprod(1.0 + strategy_returns)
    wealth_with_initial = np.concatenate(([1.0], wealth))
    peaks = np.maximum.accumulate(wealth_with_initial)
    drawdowns = wealth_with_initial / peaks - 1.0

    if len(executed) == 0:
        mean_return: float | None = None
        win_rate: float | None = None
        severe_loss_rate: float | None = None
    else:
        mean_return = float(executed.mean())
        win_rate = float((executed > 0).mean())
        severe_loss_rate = float((executed <= SEVERE_LOSS_THRESHOLD).mean())
    return {
        "n_days": int(len(values)),
        "executed_days": int(mask.sum()),
        "coverage": float(mask.mean()) if len(mask) else 0.0,
        "compound_return": float(wealth_with_initial[-1] - 1.0),
        "max_drawdown": float(drawdowns.min()),
        "mean_return": mean_return,
        "mean_all_days_return": float(strategy_returns.mean()) if len(values) else None,
        "win_rate": win_rate,
        "severe_loss_count": int((executed <= SEVERE_LOSS_THRESHOLD).sum()),
        "severe_loss_rate": severe_loss_rate,
    }


def evaluate_candidates(
    frame: pd.DataFrame,
    candidates: list[ReplayCandidate],
    partition: str,
) -> pd.DataFrame:
    """Evaluate one already-frozen time partition."""

    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        mask = candidate_pass_mask(frame, candidate)
        for basket in BASKETS:
            stats = portfolio_stats(frame[f"{basket}_net_return_t2"], mask)
            rows.append(
                {
                    "partition": partition,
                    "basket": basket,
                    **asdict(candidate),
                    **stats,
                }
            )
    return pd.DataFrame(rows)


def select_development_champions(development_metrics: pd.DataFrame) -> dict[str, dict]:
    """Select exploratory champions using development metrics only.

    The function deliberately has no validation-frame argument.  Eligibility,
    objective, and tie breaks are fixed before the 2026 results are evaluated.
    """

    if set(development_metrics["partition"].unique()) != {"development_through_2025"}:
        raise ValueError("selection accepts development_through_2025 metrics only")
    selections: dict[str, dict] = {}
    for basket in BASKETS:
        eligible = development_metrics.loc[
            development_metrics["basket"].eq(basket)
            & development_metrics["family"].ne("baseline")
            & development_metrics["coverage"].ge(MIN_SELECTION_COVERAGE)
            & development_metrics["executed_days"].ge(MIN_SELECTION_EXECUTIONS)
        ].copy()
        if eligible.empty:
            raise ValueError(f"no eligible development candidate for {basket}")
        # Higher compound return wins; then prefer the shallower drawdown and a
        # stable lexical id.  2026 is not present in this frame.
        winner = eligible.sort_values(
            ["compound_return", "max_drawdown", "candidate_id"],
            ascending=[False, False, True],
            kind="mergesort",
        ).iloc[0]
        selections[basket] = {
            "candidate_id": str(winner["candidate_id"]),
            "rule_name": str(winner["rule_name"]),
            "family": str(winner["family"]),
            "development_metrics": _json_record(winner.to_dict()),
        }
    return selections


def _json_record(record: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, np.generic):
            value = value.item()
        if isinstance(value, float) and not math.isfinite(value):
            value = None
        result[key] = value
    return result


def candidate_definitions(candidates: list[ReplayCandidate]) -> pd.DataFrame:
    return pd.DataFrame(asdict(candidate) for candidate in candidates)


def build_diagnostic_candidates(development: pd.DataFrame) -> list[DiagnosticCandidate]:
    """Freeze high-tail thresholds from development features, never outcomes."""

    if set(development["partition"].unique()) != {"development_through_2025"}:
        raise ValueError("diagnostic thresholds require development_through_2025 only")
    candidates: list[DiagnosticCandidate] = []
    for feature in DIAGNOSTIC_FEATURES:
        values = development[feature]
        if values.isna().any() or not np.isfinite(values).all():
            raise ValueError(f"diagnostic feature {feature} contains missing/non-finite values")
        for quantile in DIAGNOSTIC_QUANTILES:
            threshold = float(values.quantile(quantile, interpolation="higher"))
            candidates.append(
                DiagnosticCandidate(
                    diagnostic_id=f"{feature}_ge_dev_q{int(quantile * 100)}",
                    feature=feature,
                    development_quantile=quantile,
                    threshold=threshold,
                )
            )
    return candidates


def diagnostic_risk_mask(
    frame: pd.DataFrame,
    candidate: DiagnosticCandidate,
) -> pd.Series:
    """High feature values are the predeclared crowding/exhaustion direction."""

    return frame[candidate.feature].ge(candidate.threshold)


def evaluate_diagnostics(
    frame: pd.DataFrame,
    candidates: list[DiagnosticCandidate],
    partition: str,
) -> pd.DataFrame:
    """Evaluate hypothetical removal of high-tail diagnostic days."""

    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        risk_mask = diagnostic_risk_mask(frame, candidate)
        execute_mask = ~risk_mask
        for basket in BASKETS:
            returns = frame[f"{basket}_net_return_t2"]
            risk_returns = returns.loc[risk_mask]
            nonrisk_returns = returns.loc[execute_mask]
            stats = portfolio_stats(returns, execute_mask)
            rows.append(
                {
                    "partition": partition,
                    "basket": basket,
                    **asdict(candidate),
                    **stats,
                    "risk_days": int(risk_mask.sum()),
                    "risk_rate": float(risk_mask.mean()),
                    "risk_mean_return": (
                        float(risk_returns.mean()) if not risk_returns.empty else None
                    ),
                    "risk_win_rate": (
                        float((risk_returns > 0).mean()) if not risk_returns.empty else None
                    ),
                    "risk_severe_loss_rate": (
                        float((risk_returns <= SEVERE_LOSS_THRESHOLD).mean())
                        if not risk_returns.empty
                        else None
                    ),
                    "nonrisk_mean_return": (
                        float(nonrisk_returns.mean()) if not nonrisk_returns.empty else None
                    ),
                    "risk_minus_nonrisk_mean": (
                        float(risk_returns.mean() - nonrisk_returns.mean())
                        if not risk_returns.empty and not nonrisk_returns.empty
                        else None
                    ),
                }
            )
    return pd.DataFrame(rows)


def select_diagnostic_champions(development_metrics: pd.DataFrame) -> dict[str, dict]:
    """Choose exploratory diagnostics from development results only."""

    if set(development_metrics["partition"].unique()) != {"development_through_2025"}:
        raise ValueError("diagnostic selection accepts development metrics only")
    selections: dict[str, dict] = {}
    for basket in BASKETS:
        eligible = development_metrics.loc[
            development_metrics["basket"].eq(basket)
            & development_metrics["coverage"].ge(MIN_SELECTION_COVERAGE)
            & development_metrics["risk_days"].ge(MIN_DIAGNOSTIC_RISK_DAYS)
        ].copy()
        if eligible.empty:
            raise ValueError(f"no eligible development diagnostic for {basket}")
        winner = eligible.sort_values(
            ["compound_return", "max_drawdown", "diagnostic_id"],
            ascending=[False, False, True],
            kind="mergesort",
        ).iloc[0]
        selections[basket] = {
            "diagnostic_id": str(winner["diagnostic_id"]),
            "feature": str(winner["feature"]),
            "development_quantile": float(winner["development_quantile"]),
            "threshold": float(winner["threshold"]),
            "development_metrics": _json_record(winner.to_dict()),
        }
    return selections


def build_focus_date_diagnostics(
    day_metrics: pd.DataFrame,
    diagnostic_candidates: list[DiagnosticCandidate],
    cluster_driver_candidate: ReplayCandidate,
    all_four_candidate: ReplayCandidate,
) -> pd.DataFrame:
    """Report focus dates only after all development choices are frozen."""

    focus = day_metrics.loc[day_metrics["date"].isin(FOCUS_DATES)].copy()
    if set(focus["date"]) != set(FOCUS_DATES):
        missing = sorted(set(FOCUS_DATES).difference(focus["date"]))
        raise ValueError(f"focus dates missing from panel: {missing}")
    focus["cluster40_driver40_pass"] = candidate_pass_mask(
        focus,
        cluster_driver_candidate,
    ).astype(int)
    focus["all_four_coherence_pass"] = candidate_pass_mask(
        focus,
        all_four_candidate,
    ).astype(int)
    for candidate in diagnostic_candidates:
        focus[f"hit__{candidate.diagnostic_id}"] = diagnostic_risk_mask(
            focus,
            candidate,
        ).astype(int)
    return focus.reset_index(drop=True)


def _metric_row(
    metrics: pd.DataFrame,
    candidate: ReplayCandidate,
    partition: str,
    basket: str,
) -> pd.Series:
    rows = metrics.loc[
        metrics["candidate_id"].eq(candidate.candidate_id)
        & metrics["partition"].eq(partition)
        & metrics["basket"].eq(basket)
    ]
    if len(rows) != 1:
        raise ValueError(
            f"expected one metric row for {candidate.candidate_id}/{partition}/{basket}"
        )
    return rows.iloc[0]


def _pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "NA"
    return f"{float(value):+.2%}"


def _rule_table(
    metrics: pd.DataFrame,
    candidates: list[ReplayCandidate],
    partition: str,
    basket: str,
) -> list[str]:
    lines = [
        "| 规则 | 覆盖 | 执行日 | 复利代理 | MDD | 均值 | 胜率 | 严重亏损率 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    shown = [candidate for candidate in candidates if candidate.rule_name]
    for candidate in shown:
        row = _metric_row(metrics, candidate, partition, basket)
        lines.append(
            "| {rule} | {coverage:.1%} | {executed} | {compound} | {mdd} | "
            "{mean} | {win} | {severe} |".format(
                rule=candidate.rule_name,
                coverage=float(row["coverage"]),
                executed=int(row["executed_days"]),
                compound=_pct(row["compound_return"]),
                mdd=_pct(row["max_drawdown"]),
                mean=_pct(row["mean_return"]),
                win=_pct(row["win_rate"]),
                severe=_pct(row["severe_loss_rate"]),
            )
        )
    return lines


def _diagnostic_metric_row(
    metrics: pd.DataFrame,
    candidate: DiagnosticCandidate,
    partition: str,
    basket: str,
) -> pd.Series:
    rows = metrics.loc[
        metrics["diagnostic_id"].eq(candidate.diagnostic_id)
        & metrics["partition"].eq(partition)
        & metrics["basket"].eq(basket)
    ]
    if len(rows) != 1:
        raise ValueError(
            f"expected one diagnostic row for {candidate.diagnostic_id}/{partition}/{basket}"
        )
    return rows.iloc[0]


def _diagnostic_table(
    metrics: pd.DataFrame,
    candidates: list[DiagnosticCandidate],
    partition: str,
    basket: str,
) -> list[str]:
    lines = [
        "| 高尾诊断（命中日假设空仓） | 阈值 | 风险日 | 风险日均值 | 非风险日均值 | 执行覆盖 | 复利代理 | MDD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for candidate in candidates:
        row = _diagnostic_metric_row(metrics, candidate, partition, basket)
        lines.append(
            "| {name} | {threshold:.6g} | {risk_days} | {risk_mean} | {nonrisk_mean} | "
            "{coverage:.1%} | {compound} | {mdd} |".format(
                name=candidate.diagnostic_id,
                threshold=candidate.threshold,
                risk_days=int(row["risk_days"]),
                risk_mean=_pct(row["risk_mean_return"]),
                nonrisk_mean=_pct(row["nonrisk_mean_return"]),
                coverage=float(row["coverage"]),
                compound=_pct(row["compound_return"]),
                mdd=_pct(row["max_drawdown"]),
            )
        )
    return lines


def build_report(
    day_metrics: pd.DataFrame,
    all_metrics: pd.DataFrame,
    candidates: list[ReplayCandidate],
    selections: dict[str, dict],
    diagnostic_metrics: pd.DataFrame,
    diagnostic_candidates: list[DiagnosticCandidate],
    diagnostic_selections: dict[str, dict],
    focus_dates: pd.DataFrame,
    taxonomy_meta: dict[str, Any],
    taxonomy_approved: bool,
) -> str:
    development = day_metrics.loc[day_metrics["partition"].eq("development_through_2025")]
    validation = day_metrics.loc[day_metrics["partition"].eq("validation_2026")]
    lines = [
        "# V16DayGate 历史代理回放",
        "",
        "## 结论与使用边界",
        "",
        "本实验只用于否证/筛查 DayGate 假设，**不能直接上线**。历史缓存只有每只股票的 "
        "`best board`，没有生产所需的 `stock_all_boards`；因此图指标是单标签历史代理，不是生产全图。",
        "`driver` 也只是严格按任务定义使用 `gain_0938 >= 0.8`，并非重新构造的 09:40 生产字段。",
        "",
        "此前已被否证的简单命题保持否证状态：**不能因为历史代理中最大簇占比低于 40% 就空仓**。"
        "本报告保留 `simple_40pct_cluster` 作为负对照，不依据 2026 结果移动 40%、50% 等阈值来抢救规则。",
        "",
        "开发集只含 2025-12-31 及以前；开发集冠军在计算任何 2026 统计前冻结。2026 仅作时间验证。"
        "穷举结果属于多重比较下的探索，不因一次验证改善而获得上线资格。",
        "",
        "## 数据与口径",
        "",
        f"- 样本：{len(day_metrics)} 日，开发集 {len(development)} 日，2026 验证集 {len(validation)} 日。",
        "- 收益：直接复用 `stock_panel.csv` 的 `net_return_t2`；入场为 09:40 minute-bar OPEN，"
        "退出为 T+2 close，已扣 0.20% 往返成本，本脚本不重算或改写收益。",
        "- Top1/Top3/Top10：分别取对应排名的单票或等权日收益；同一 DayGate Top10 指标应用于三种组合。",
        "- 复利与 MDD：按信号日顺序、空仓日收益记 0 的事件序列代理。因 T+2 持仓会重叠，"
        "它不是带资金占用约束的真实账户净值。",
        f"- 板块结构：`{BOARD_PROXY_LABEL}`。",
        f"- taxonomy：{taxonomy_meta.get('taxonomy_version') or '未使用（raw board）'}；"
        f"调用者声明 approved={str(taxonomy_approved).lower()}。即使 approved，也不能补回历史多板块成员关系。",
        "- 无论是否传 taxonomy，`day_metrics.csv` 都保留 `raw_*` 指标；通用 gate 列仅在显式传入"
        "taxonomy 时切到 taxonomy proxy。默认产物完全使用 raw best-board。",
        "",
        "## 预声明规则",
        "",
        "预声明单条件为：最大簇占比≥40%、有效簇数≤4、Top3 主簇覆盖≥2/3、driver breadth≥40%；"
        "并报告固定二项/四项组合及 baseline。表内 `复利代理` 与 MDD 均把被 gate 删除的日期记为 0 收益。",
    ]

    for basket in BASKETS:
        lines.extend(("", f"### {basket.upper()} — 开发集至 2025", ""))
        lines.extend(_rule_table(all_metrics, candidates, "development_through_2025", basket))
        lines.extend(("", f"### {basket.upper()} — 2026 时间验证", ""))
        lines.extend(_rule_table(all_metrics, candidates, "validation_2026", basket))

    lines.extend(("", "## 仅用开发集选出的探索冠军", ""))
    lines.append(
        "| 组合 | candidate_id | 开发覆盖 | 开发复利代理 | 2026覆盖 | 2026复利代理 | 2026 MDD |"
    )
    lines.append("|---|---|---:|---:|---:|---:|---:|")
    for basket in BASKETS:
        selected = selections[basket]
        candidate = next(row for row in candidates if row.candidate_id == selected["candidate_id"])
        dev = _metric_row(all_metrics, candidate, "development_through_2025", basket)
        val = _metric_row(all_metrics, candidate, "validation_2026", basket)
        lines.append(
            f"| {basket} | `{candidate.candidate_id}` | {dev['coverage']:.1%} | "
            f"{_pct(dev['compound_return'])} | {val['coverage']:.1%} | "
            f"{_pct(val['compound_return'])} | {_pct(val['max_drawdown'])} |"
        )

    lines.extend(
        (
            "",
            "选参资格预先固定为开发覆盖率≥30%且开发执行日≥100；目标为开发集复利代理最高，"
            "再以较浅 MDD 和稳定 candidate_id 破同分。冠军是探索产物，不属于预声明规则。",
            "",
            "## 独立 crowding / exhaustion 诊断",
            "",
            "这一轨不进入 `V16DayGatePolicy`。特征在看到 2026 前固定为：Top10 `gain_0938` 的"
            "均值/中位数、≥3%/≥5%占比、hot-board 数、final candidates，以及 score softmax "
            "HHI/Top1 share。每个阈值仅由开发集特征的 75%/90% 分位点（`higher` 插值）冻结，"
            "不读取收益，也不读取 2026-08-17/18；高尾命中只作为潜在拥挤/耗竭诊断。",
        )
    )

    lines.extend(("", "### Top10 开发集诊断全表", ""))
    lines.extend(
        _diagnostic_table(
            diagnostic_metrics,
            diagnostic_candidates,
            "development_through_2025",
            "top10",
        )
    )
    lines.extend(("", "### Top10 2026 验证诊断全表", ""))
    lines.extend(
        _diagnostic_table(
            diagnostic_metrics,
            diagnostic_candidates,
            "validation_2026",
            "top10",
        )
    )

    lines.extend(("", "### 开发集选择的诊断冠军", ""))
    lines.append(
        "| 组合 | diagnostic | 阈值 | 开发覆盖 | 开发复利代理 | 2026覆盖 | 2026复利代理 | 2026 MDD |"
    )
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|")
    for basket in BASKETS:
        selected = diagnostic_selections[basket]
        diagnostic = next(
            item
            for item in diagnostic_candidates
            if item.diagnostic_id == selected["diagnostic_id"]
        )
        dev = _diagnostic_metric_row(
            diagnostic_metrics,
            diagnostic,
            "development_through_2025",
            basket,
        )
        val = _diagnostic_metric_row(
            diagnostic_metrics,
            diagnostic,
            "validation_2026",
            basket,
        )
        lines.append(
            f"| {basket} | `{diagnostic.diagnostic_id}` | {diagnostic.threshold:.6g} | "
            f"{dev['coverage']:.1%} | {_pct(dev['compound_return'])} | {val['coverage']:.1%} | "
            f"{_pct(val['compound_return'])} | {_pct(val['max_drawdown'])} |"
        )

    lines.extend(("", "### 2026-08-17 / 08-18 是否命中", ""))
    lines.append(
        "| 日期 | LCS | 有效簇 | Top3主簇覆盖 | driver breadth | 集中+driver | 四项组合 | "
        "Top1 | Top3 | Top10 | gain均值 | gain中位数 | ≥3% | ≥5% | hot boards | final candidates | score HHI |"
    )
    lines.append(
        "|---|---:|---:|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    )
    for row in focus_dates.itertuples(index=False):
        lines.append(
            f"| {row.date} | {row.largest_cluster_share:.1%} | "
            f"{row.effective_cluster_count:.3f} | {row.top3_main_cluster_coverage:.1%} | "
            f"{row.driver_breadth:.1%} | {'通过' if row.cluster40_driver40_pass else '不通过'} | "
            f"{'通过' if row.all_four_coherence_pass else '不通过'} | "
            f"{row.top1_net_return_t2:+.2%} | {row.top3_net_return_t2:+.2%} | "
            f"{row.top10_net_return_t2:+.2%} | "
            f"{row.gain_0938_mean:.2f}% | {row.gain_0938_median:.2f}% | "
            f"{row.gain_0938_ge_3_share:.1%} | {row.gain_0938_ge_5_share:.1%} | "
            f"{row.hot_board_count} | {row.final_candidates} | {row.score_softmax_hhi:.3f} |"
        )
    lines.append("")
    for _, row in focus_dates.iterrows():
        hit_names = [
            diagnostic.diagnostic_id
            for diagnostic in diagnostic_candidates
            if int(row[f"hit__{diagnostic.diagnostic_id}"]) == 1
        ]
        lines.append(f"- {row['date']} 开发集冻结阈值命中：{', '.join(hit_names) or '无'}。")
    lines.extend(
        (
            "",
            "两天都通过“最大簇≥40% + driver≥40%”，所以集中度与 driver 下限无法识别本次亏损。"
            "四项组合会因 Top3 主簇覆盖仅 1/3 而拦 8/17，但 8/18 四项全部通过，仍不能覆盖整个亏损段。",
            "",
            "gain 高尾多数组合确实命中这两天，但在开发集 Top10 中，高 gain 风险组的均值反而普遍"
            "高于非风险组，方向不支持把它直接当耗竭门。hot-board/final-candidates 的开发集 q75 阈值"
            "分别为 36/167，两天都未命中。score HHI q75 命中两天，却在 2026 的 Top1/Top3 上"
            "出现方向翻转；这些都只能继续 shadow，不能因焦点日期命中而倒推阈值。",
            "",
            "## 为什么仍不能上线",
            "",
            "1. 历史缓存只有 best-board，无法恢复 production `stock_all_boards` 的跨板块连通关系。",
            "2. current-board replay 与逐日官方输出并不完全一致，存在幸存者、概念漂移和候选重建误差。",
            "3. `gain_0938` driver 是可用的历史代理，但不等价于完整生产时点的 driver/expanded 证据。",
            "4. 可选 taxonomy 若不是在收益揭晓前冻结且获批，会引入语义后见偏差；taxonomy 也不能创造缺失的成员边。",
            "5. 2026 是留出时间验证，不是真正未参与研究流程的线上前瞻；穷举还存在多重检验。",
            "6. 复利/MDD 是重叠 T+2 事件代理，不处理真实账户资金占用、成交滑点和持仓冲突。",
            "",
            "任何候选升级只能进入 shadow，并需用逐日冻结的官方 Top10 + `stock_all_boards` 做新的前瞻验证。",
        )
    )
    return "\n".join(lines) + "\n"


def run_experiment(
    stock_panel_path: Path,
    output_dir: Path,
    taxonomy_path: Path | None = None,
    taxonomy_approved: bool = False,
) -> dict[str, Path]:
    panel = load_stock_panel(stock_panel_path)
    taxonomy_map, taxonomy_meta = load_taxonomy(taxonomy_path)
    taxonomy_meta["caller_declared_approved"] = taxonomy_approved
    day_metrics = build_day_metrics(
        panel,
        REPO_ROOT,
        canonical_theme_map=taxonomy_map,
        taxonomy_version=taxonomy_meta.get("taxonomy_version"),
    )
    development = day_metrics.loc[day_metrics["partition"].eq("development_through_2025")].copy()
    validation = day_metrics.loc[day_metrics["partition"].eq("validation_2026")].copy()
    if development.empty or validation.empty:
        raise ValueError("both development-through-2025 and validation-2026 are required")

    candidates = generate_candidates()
    # Freeze selections before evaluating validation outcomes.
    development_metrics = evaluate_candidates(
        development,
        candidates,
        "development_through_2025",
    )
    selections = select_development_champions(development_metrics)

    # Diagnostic feature sets and thresholds are frozen from development before
    # any validation outcomes or focus dates are inspected.
    diagnostic_candidates = build_diagnostic_candidates(development)
    development_diagnostic_metrics = evaluate_diagnostics(
        development,
        diagnostic_candidates,
        "development_through_2025",
    )
    diagnostic_selections = select_diagnostic_champions(development_diagnostic_metrics)

    validation_metrics = evaluate_candidates(validation, candidates, "validation_2026")
    descriptive_metrics = evaluate_candidates(day_metrics, candidates, "all_periods_descriptive")
    all_metrics = pd.concat(
        [development_metrics, validation_metrics, descriptive_metrics],
        ignore_index=True,
    )
    validation_diagnostic_metrics = evaluate_diagnostics(
        validation,
        diagnostic_candidates,
        "validation_2026",
    )
    descriptive_diagnostic_metrics = evaluate_diagnostics(
        day_metrics,
        diagnostic_candidates,
        "all_periods_descriptive",
    )
    all_diagnostic_metrics = pd.concat(
        [
            development_diagnostic_metrics,
            validation_diagnostic_metrics,
            descriptive_diagnostic_metrics,
        ],
        ignore_index=True,
    )

    for basket, selection in selections.items():
        selected_id = selection["candidate_id"]
        validation_row = validation_metrics.loc[
            validation_metrics["basket"].eq(basket)
            & validation_metrics["candidate_id"].eq(selected_id)
        ].iloc[0]
        selection["validation_metrics"] = _json_record(validation_row.to_dict())
    for basket, selection in diagnostic_selections.items():
        selected_id = selection["diagnostic_id"]
        validation_row = validation_diagnostic_metrics.loc[
            validation_diagnostic_metrics["basket"].eq(basket)
            & validation_diagnostic_metrics["diagnostic_id"].eq(selected_id)
        ].iloc[0]
        selection["validation_metrics"] = _json_record(validation_row.to_dict())
    selection_payload = {
        "schema_version": "v16-day-gate-replay-selection/v1",
        "selection_data_end": DEVELOPMENT_END,
        "validation_start": VALIDATION_START,
        "validation_end": VALIDATION_END,
        "selection_uses_2026": False,
        "minimum_coverage": MIN_SELECTION_COVERAGE,
        "minimum_executions": MIN_SELECTION_EXECUTIONS,
        "objective": "max development compound_return; then shallower MDD; then candidate_id",
        "champions": selections,
        "diagnostic_protocol": {
            "threshold_source": "development feature distribution only; no outcomes",
            "quantiles": list(DIAGNOSTIC_QUANTILES),
            "risk_direction": "high_tail",
            "minimum_risk_days": MIN_DIAGNOSTIC_RISK_DAYS,
            "selection_uses_2026_or_focus_dates": False,
            "objective": (
                "max development compound_return after removing diagnostic hits; "
                "then shallower MDD; then diagnostic_id"
            ),
            "champions": diagnostic_selections,
        },
    }

    simple_40 = next(
        candidate for candidate in candidates if candidate.rule_name == "simple_40pct_cluster"
    )
    baseline = next(
        candidate for candidate in candidates if candidate.rule_name == "baseline_all_days"
    )
    all_four = next(
        candidate for candidate in candidates if candidate.rule_name == "all_four_predeclared"
    )
    cluster_driver = next(
        candidate for candidate in candidates if candidate.rule_name == "cluster40_and_driver40"
    )
    focus_dates = build_focus_date_diagnostics(
        day_metrics,
        diagnostic_candidates,
        cluster_driver,
        all_four,
    )
    summary = {
        "schema_version": "v16-day-gate-replay-summary/v1",
        "provenance": {
            "stock_panel": str(stock_panel_path.resolve()),
            "stock_panel_sha256": sha256(stock_panel_path),
            "gate_source": str(GATE_SOURCE.resolve()),
            "gate_source_sha256": sha256(GATE_SOURCE),
            "board_structure": BOARD_PROXY_LABEL,
            "driver_definition": f"gain_0938 >= {DRIVER_GAIN_0938_THRESHOLD}",
            "entry": "09:40 minute-bar OPEN (inherited net_return_t2)",
            "exit": "T+2 close (inherited net_return_t2)",
            "round_trip_cost": ROUND_TRIP_COST,
            "taxonomy": taxonomy_meta,
        },
        "sample": {
            "days": len(day_metrics),
            "stock_rows": len(panel),
            "start": str(day_metrics["date"].min()),
            "end": str(day_metrics["date"].max()),
            "development_days": len(development),
            "validation_2026_days": len(validation),
        },
        "threshold_grid": {
            "candidate_count": len(candidates),
            "largest_cluster_share": list(LARGEST_SHARE_GRID),
            "effective_cluster_count": list(EFFECTIVE_CLUSTER_GRID),
            "top3_main_cluster_coverage": list(TOP3_COVERAGE_GRID),
            "driver_breadth": list(DRIVER_BREADTH_GRID),
            "cartesian_exhaustive_within_declared_grid": True,
        },
        "anti_data_mining": selection_payload,
        "crowding_exhaustion_diagnostics": {
            "features": list(DIAGNOSTIC_FEATURES),
            "thresholds": [asdict(candidate) for candidate in diagnostic_candidates],
            "focus_dates": [
                _json_record(record) for record in focus_dates.to_dict(orient="records")
            ],
            "policy_status": "diagnostic_only_not_part_of_V16DayGatePolicy",
        },
        "simple_40pct_negative_control": {
            basket: {
                partition: {
                    "baseline": _json_record(
                        _metric_row(all_metrics, baseline, partition, basket).to_dict()
                    ),
                    "simple_40pct_cluster": _json_record(
                        _metric_row(all_metrics, simple_40, partition, basket).to_dict()
                    ),
                }
                for partition in ("development_through_2025", "validation_2026")
            }
            for basket in BASKETS
        },
        "deployment_status": "research_only_not_approved_for_live_or_hard_gate",
        "limitations": [
            "historical caches contain best_board only, not stock_all_boards",
            "driver is proxied by gain_0938 >= 0.8",
            "current-board replay is not exact official point-in-time output",
            "T+2 event returns overlap and are not a capital-constrained account simulation",
            "2026 is temporal validation, not untouched prospective production evidence",
        ],
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "day_metrics": output_dir / "day_metrics.csv",
        "threshold_grid": output_dir / "threshold_grid.csv",
        "rule_metrics": output_dir / "rule_metrics.csv",
        "diagnostic_thresholds": output_dir / "diagnostic_thresholds.csv",
        "diagnostic_rule_metrics": output_dir / "diagnostic_rule_metrics.csv",
        "focus_dates": output_dir / "focus_dates.csv",
        "selection": output_dir / "selection.json",
        "summary": output_dir / "summary.json",
        "report": output_dir / "REPORT.md",
    }
    day_metrics.to_csv(paths["day_metrics"], index=False, encoding="utf-8-sig")
    candidate_definitions(candidates).to_csv(
        paths["threshold_grid"], index=False, encoding="utf-8-sig"
    )
    all_metrics.to_csv(paths["rule_metrics"], index=False, encoding="utf-8-sig")
    pd.DataFrame(asdict(candidate) for candidate in diagnostic_candidates).to_csv(
        paths["diagnostic_thresholds"], index=False, encoding="utf-8-sig"
    )
    all_diagnostic_metrics.to_csv(
        paths["diagnostic_rule_metrics"], index=False, encoding="utf-8-sig"
    )
    focus_dates.to_csv(paths["focus_dates"], index=False, encoding="utf-8-sig")
    paths["selection"].write_text(
        json.dumps(selection_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    paths["summary"].write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    paths["report"].write_text(
        build_report(
            day_metrics,
            all_metrics,
            candidates,
            selections,
            all_diagnostic_metrics,
            diagnostic_candidates,
            diagnostic_selections,
            focus_dates,
            taxonomy_meta,
            taxonomy_approved,
        ),
        encoding="utf-8",
    )
    return paths


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stock-panel", type=Path, default=DEFAULT_STOCK_PANEL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--taxonomy",
        type=Path,
        default=None,
        help="Optional approved/frozen board->theme JSON; never auto-discovered.",
    )
    parser.add_argument(
        "--taxonomy-approved",
        action="store_true",
        help="Record that the caller independently approved the supplied taxonomy.",
    )
    args = parser.parse_args()
    if args.taxonomy_approved and args.taxonomy is None:
        parser.error("--taxonomy-approved requires --taxonomy")
    paths = run_experiment(
        args.stock_panel,
        args.output,
        taxonomy_path=args.taxonomy,
        taxonomy_approved=args.taxonomy_approved,
    )
    for label, path in paths.items():
        print(f"{label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
