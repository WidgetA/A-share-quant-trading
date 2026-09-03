"""Ranking-order regressions for the production scorer implementation."""

from __future__ import annotations

from src.strategy.lgbrank_scorer import CandidateSnapshot, LGBRankScorer


class _EqualScoreModel:
    def predict(self, matrix):
        return [0.0] * len(matrix)


def _snapshot(code: str) -> CandidateSnapshot:
    return CandidateSnapshot(
        code=code,
        name=f"stock-{code}",
        open_price=10.0,
        prev_close=9.9,
        price_at_940=10.1,
        high_price=10.2,
        low_price=9.8,
        early_volume=1_000.0,
        avg_daily_volume=10_000.0,
        trend_pct=0.01,
        trend_10d=0.02,
        avg_daily_return_20d=0.001,
        volatility_20d=0.01,
        consecutive_up_days=1,
    )


def test_equal_scores_use_code_tiebreak_independent_of_candidate_order() -> None:
    """Sorting V16 candidates cannot alter tied Top-10 membership or order."""
    scorer = object.__new__(LGBRankScorer)
    scorer.model = _EqualScoreModel()
    scorer.features = []
    scorer.raw_features = []

    input_codes = [
        "600000",
        "000001",
        "300750",
        "002594",
        "688981",
        "000858",
        "601318",
        "002415",
        "600519",
        "000333",
        "601166",
        "300059",
    ]
    candidates = [_snapshot(code) for code in input_codes]

    original = scorer.score_and_rank(candidates, {}, avg_market_open_gain=0.0)
    reordered = scorer.score_and_rank(list(reversed(candidates)), {}, avg_market_open_gain=0.0)

    expected = sorted(input_codes)
    assert [stock.code for stock in original] == expected
    assert [stock.code for stock in reordered] == expected
    assert [stock.code for stock in original[:10]] == expected[:10]
    assert [stock.code for stock in reordered[:10]] == expected[:10]
    assert [stock.rank for stock in original] == list(range(1, len(input_codes) + 1))
    assert {stock.score for stock in original} == {0.0}
