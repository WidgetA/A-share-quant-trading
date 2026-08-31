"""Experimental: LGBRankScorer wrapper for models with embedded feature names.

This module deliberately lives under scripts/ so that the rename map and
auto-detection logic stay isolated to research / experiment scripts
(run_v16_today.py, dump_funnel.py). The production V16 daily scan
(src/web/v15_scan_service.py) keeps using the plain LGBRankScorer in
src/strategy/lgbrank_scorer.py and never sees this code.

When you train a new LightGBM model with real feature names baked into
the booster (instead of the legacy Column_0/Column_1/... placeholders),
the names follow a different convention than what the V16 feature
computation functions emit (e.g. compute side says `gain_from_open_pct`
while the new model expects `open_gain`). This subclass auto-detects the
model variant on load and inserts a rename step between feature
computation and z-score normalization so old + new models both work.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import lightgbm as lgb
import numpy as np

from src.strategy.lgbrank_scorer import (
    CandidateSnapshot,
    LGBRankScorer,
    ScoredStock,
    _add_zscore,
    _compute_advanced_features,
    _compute_base_features,
    _compute_engineered_features,
)

logger = logging.getLogger(__name__)


# Compute-side feature name → name baked into the new model.
_COMPUTE_TO_MODEL: dict[str, str] = {
    "gain_from_open_pct": "open_gain",
    "turnover_amp": "volume_amp",
    "trend_pct": "trend_5d",
    "avg_daily_return_20d": "avg_return_20d",
    "intraday_range_940": "early_price_range",
    "avg_market_open_gain": "market_open_gain",
    "open_gap_pct": "gap",
    "upper_shadow": "upper_shadow_ratio",
    "volume_to_avg": "volume_ratio",
    "open_pattern_consistency": "open_position_consistency",
    "vol_price_divergence_5d": "volume_price_divergence",
    "intraday_momentum_continuation": "intraday_momentum_cont",
    "volume_concentration_ratio": "volume_concentration",
    "relative_strength_vs_high": "relative_strength",
    "gain_consistency_score": "return_consistency",
    "amplitude_decay_trend": "amplitude_decay",
    "turnover_stability_index": "volume_stability",
    "close_to_vwap_position": "close_vs_vwap",
    "vol_weighted_gain_ratio": "volume_weighted_return",
    "price_channel_pct": "price_channel_position",
    "up_days_pct_20d": "up_day_ratio_20d",
    "vol_ratio_5d_20d": "volume_ratio_5d_20d",
    "momentum_contrarian": "momentum_x_mean_reversion",
    "vol_normalized_return": "volatility_adj_return",
    "range_to_vol": "strength_persistence",
    "volume_price_efficiency": "volume_price_momentum",
    "gap_to_trend": "gap_reversion",
    "gap_quality": "gap_volume_interaction",
    "trend_x_vol": "volume_trend_interaction",
    "consec_x_range": "momentum_stability",
    "return_x_turnover": "trend_volume_divergence",
}


class NewModelLGBRankScorer(LGBRankScorer):
    """LGBRankScorer that handles both legacy (Column_N) and new (real-named)
    LightGBM models. Falls back to the parent's behavior verbatim when the
    loaded model uses Column_N placeholders.

    Use this in research scripts only. Production V16 scan should keep
    using LGBRankScorer directly.
    """

    def __init__(self, model_path: Path | str, feature_list_path: Path | str) -> None:
        # Override the entire load flow — we don't call super().__init__() because
        # the parent always reads feature_list.json, which is the wrong source of
        # truth for new models that embed names in the booster itself.
        model_path = Path(model_path)
        feature_list_path = Path(feature_list_path)

        self.model = lgb.Booster(model_file=str(model_path))
        size_kb = model_path.stat().st_size / 1024
        logger.info(f"[LGBRank] Loaded model: {model_path.name} ({size_kb:.1f} KB)")

        model_names = self.model.feature_name()
        has_real_names = bool(model_names) and not model_names[0].startswith("Column_")

        if has_real_names:
            # New model: take feature names + order from the booster itself.
            model_name_set = set(model_names)
            self._rename_map: dict[str, str] = {
                k: v for k, v in _COMPUTE_TO_MODEL.items() if v in model_name_set
            }
            self.features = list(model_names)
            self.raw_features = [n for n in model_names if not n.startswith("z_")]
            logger.info(
                f"[LGBRank] Model has embedded feature names, "
                f"{len(self._rename_map)} renames needed"
            )
        else:
            # Legacy model: parent's behavior — read feature_list.json.
            self._rename_map = {}
            with open(feature_list_path, "r", encoding="utf-8") as f:
                feat_info = json.load(f)
            self.features = feat_info["features"]
            self.raw_features = feat_info["raw_features"]

        logger.info(f"[LGBRank] {len(self.features)} features loaded")

    def score_and_rank(
        self,
        candidates: list[CandidateSnapshot],
        history_map,
        avg_market_open_gain: float,
    ) -> list[ScoredStock]:
        """Same pipeline as LGBRankScorer.score_and_rank, with a rename step
        inserted between feature computation and z-score normalization.
        """
        if not candidates:
            return []

        # Step 1: per-stock feature computation (identical to parent).
        feature_dicts = []
        for s in candidates:
            base = _compute_base_features(s, avg_market_open_gain)
            hist = history_map.get(s.code)
            advanced = _compute_advanced_features(hist)
            merged = {**base, **advanced}
            engineered = _compute_engineered_features(merged)
            merged.update(engineered)
            feature_dicts.append(merged)

        # Step 1.5: rename compute-side names to model-side names. No-op for
        # legacy models (rename_map is empty there).
        if self._rename_map:
            for fd in feature_dicts:
                for old_name, new_name in self._rename_map.items():
                    if old_name in fd:
                        fd[new_name] = fd.pop(old_name)

        # Step 2: in-pool z-score normalization.
        _add_zscore(feature_dicts, self.raw_features)

        # Step 3: build feature matrix in model's expected order.
        n = len(candidates)
        X = np.zeros((n, len(self.features)))
        for i, fd in enumerate(feature_dicts):
            for j, fname in enumerate(self.features):
                X[i, j] = fd.get(fname, 0.0)

        # Step 4: NaN/inf check.
        if np.isnan(X).any() or np.isinf(X).any():
            bad = []
            for j, fname in enumerate(self.features):
                col = X[:, j]
                n_na, n_inf = int(np.isnan(col).sum()), int(np.isinf(col).sum())
                if n_na > 0 or n_inf > 0:
                    bad.append(f"{fname}: {n_na} NaN, {n_inf} inf")
            raise RuntimeError(f"Feature matrix contains NaN/inf: {bad}")

        # Step 5: model inference.
        scores = self.model.predict(X)

        # Step 6: deterministic rank (score desc, code asc tiebreaker).
        scores_arr = np.asarray(scores, dtype=float)
        indexed = [(float(scores_arr[i]), candidates[i].code, i) for i in range(len(candidates))]
        indexed.sort(key=lambda t: (-t[0], t[1]))

        results = []
        for rank_0, (_sc, _code, idx) in enumerate(indexed):
            s = candidates[idx]
            results.append(
                ScoredStock(
                    code=s.code,
                    name=s.name,
                    score=float(scores_arr[idx]),
                    rank=rank_0 + 1,
                    buy_price=s.price_at_940,
                )
            )

        return results
