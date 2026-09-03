from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from src.data.clients.tushare_realtime import TushareQuote
from src.data.database.v20_repository import StateRecord, V20SemanticConflict, sha256_json
from src.strategy.lgbrank_scorer import ScoredStock
from src.strategy.strategies.v16_scanner import V16ScanResult, V16StockData
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.decision_engine import (
    CompletedRolling,
    genesis_state,
    prepare_entry,
)
from src.web.v20_canonical_selection import CanonicalV16ScanBundle, _bundle_fingerprint
from src.web.v20_v16_canonical_artifact import encode, hydrate
from tests.unit.web.test_v20_service import PROJECT_ROOT, _service

TZ = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 9, 1)
FULL_EXCHANGE_CALENDAR = (
    date(2026, 8, 31),
    TRADE_DATE,
    date(2026, 9, 2),
    date(2026, 9, 3),
)
ARTIFACT_CALENDAR = (TRADE_DATE, date(2026, 9, 2), date(2026, 9, 3))
CODES = ("000001", "600000", "603068")
BREADTH_ONLY_CODE = "000002"
RAW_EVIDENCE_CODES = tuple(sorted((*CODES, BREADTH_ONLY_CODE)))
RECOMMENDED = (
    ScoredStock(
        code="603068",
        name="recommended-one",
        score=9.75,
        rank=1,
        buy_price=20.25,
    ),
    ScoredStock(
        code="600000",
        name="recommended-two",
        score=8.5,
        rank=2,
        buy_price=10.5,
    ),
)
# Keep exactly 37 weekdays, ending on D-1.
HISTORY_DATES = tuple(
    date(2026, 8, 31) - timedelta(days=offset)
    for offset in range(100)
    if (date(2026, 8, 31) - timedelta(days=offset)).weekday() < 5
)[:37][::-1]
HISTORY_COUNTS = {day.isoformat(): len(CODES) for day in HISTORY_DATES}


def _quote(code: str, latest: float) -> TushareQuote:
    return TushareQuote(
        stock_code=code,
        open_price=10.0,
        latest_price=latest,
        high_price=max(10.0, latest) + 0.1,
        low_price=min(10.0, latest) - 0.1,
        volume=1_000.0,
        amount=10_000.0,
    )


def _stock_data(code: str) -> V16StockData:
    return V16StockData(
        code=code,
        name=f"name-{code}",
        open_price=10.0,
        prev_close=10.0,
        price_940=10.5,
        high_940=10.8,
        low_940=9.8,
        volume_940=100_000.0,
        volume_937=90_000.0,
        avg_daily_volume=80_000.0,
        trend_5d=0.02,
        trend_10d=0.04,
        avg_daily_return_20d=0.001,
        volatility_20d=0.01,
        consecutive_up_days=1,
        history_df=pd.DataFrame(
            {
                "open": [10.0] * 37,
                "high": [10.2] * 37,
                "low": [9.8] * 37,
                "close": [10.0 + index * 0.01 for index in range(37)],
                "volume": [50_000.0] * 37,
            }
        ),
    )


def _canonical(
    *,
    reverse_inputs: bool = False,
    recommended: tuple[ScoredStock, ...] = RECOMMENDED,
) -> CanonicalV16ScanBundle:
    codes = tuple(sorted(CODES, reverse=reverse_inputs))
    stage_codes = list(reversed(codes))
    candidate_codes = [stock.code for stock in recommended]
    if reverse_inputs:
        candidate_codes.reverse()
    candidate_count = len(candidate_codes)
    board_z = ["603068", "600000"]
    board_a = ["600000", "000001"]
    if reverse_inputs:
        board_z.reverse()
        board_a.reverse()
    result = V16ScanResult(
        recommended=list(recommended),
        all_scored=list(recommended),
        step0_universe_count=3,
        step2_hot_board_count=2,
        step2_filtered_by_avg_gain=1,
        step3_count=candidate_count,
        step4_count=candidate_count,
        step5_count=candidate_count,
        step6_count=candidate_count,
        step6_5_count=candidate_count,
        step6_6_count=candidate_count,
        final_candidates=candidate_count,
        step0_codes=stage_codes,
        step2_boards_detail={
            "board-z": board_z,
            "board-a": board_a,
        },
        step2_codes=stage_codes,
        st_eligible_codes=candidate_codes,
        step3_codes=candidate_codes,
        step4_codes=candidate_codes,
        step5_codes=candidate_codes,
        step6_codes=candidate_codes,
        step6_5_codes=candidate_codes,
        step6_6_codes=candidate_codes,
        stock_best_board={code: f"board-{code}" for code in codes},
        stock_all_boards={code: [f"board-{code}", "board-shared"] for code in codes},
        step2_board_avg_gains={
            **{f"board-{code}": 1.0 for code in CODES},
            "board-shared": 0.9,
        },
        stock_is_driver={code: code == "603068" for code in CODES},
        stock_cci={"000001": 98.0, "600000": 99.0, "603068": 100.0},
        stock_early_vol={
            "000001": 1_002.0,
            "600000": 1_001.0,
            "603068": 1_000.0,
        },
    )
    histories = {
        code: {
            "time": [day.isoformat() for day in HISTORY_DATES],
            "open": [10.0] * 37,
            "high": [10.1] * 37,
            "low": [9.9] * 37,
            "close": [10.05] * 37,
            "volume": [1_000.0] * 37,
        }
        for code in CODES
    }
    quotes = {
        "603068": _quote("603068", 10.5),
        "600000": _quote("600000", 9.5),
        "000001": _quote("000001", 10.1),
    }
    pre_hash = CanonicalV16ScanBundle(
        trade_date=TRADE_DATE,
        scan_result=result,
        stock_data={code: _stock_data(code) for code in codes},
        clean_boards={f"board-{code}": [(code, f"name-{code}")] for code in reversed(codes)},
        universe=codes,
        quotes=quotes,
        prev_closes={code: 10.0 for code in RAW_EVIDENCE_CODES},
        history_raw=histories,
        early_bars={code: () for code in RAW_EVIDENCE_CODES},
        early_source_hashes={
            code: ("e" * 63 + str(index)) for index, code in enumerate(RAW_EVIDENCE_CODES)
        },
        failed_no_prev_close=(),
        failed_no_history=(),
        failed_build=(),
        skipped_new_listings=(),
        model_sha256="1" * 64,
        feature_list_sha256="3" * 64,
        computed_at=datetime(2026, 9, 1, 9, 39, 59, tzinfo=TZ),
        input_hash="a" * 64,
        _integrity_hash="",
        computation_calendar=FULL_EXCHANGE_CALENDAR,
        prior_trade_date=date(2026, 8, 31),
        prior_amount_yuan=(
            {}
            if not recommended
            else {
                "603068": 123_456_789.0,
                "600000": 987_654_321.0,
            }
        ),
        breadth_valid_n=3,
        breadth_down_n=1,
        breadth_market_source_hash="b" * 64,
        breadth_market_missing_codes=(),
        breadth_market_conflict_codes=(),
        history_date_valid_counts=HISTORY_COUNTS,
        history_min_date_coverage=1.0,
        external_market_fact_hash="f" * 64,
    )
    return replace(pre_hash, _integrity_hash=_bundle_fingerprint(pre_hash))


def _rehash(bundle: CanonicalV16ScanBundle) -> CanonicalV16ScanBundle:
    return replace(bundle, _integrity_hash=_bundle_fingerprint(bundle))


@pytest.fixture
def service(monkeypatch: pytest.MonkeyPatch) -> Any:
    return _service(monkeypatch, SimpleNamespace())


def _prepared(bundle: Any, *, rolling: tuple[CompletedRolling, ...] = ()) -> Any:
    from src.strategy.v20.runtime_config import load_v20_runtime_config

    config = load_v20_runtime_config(PROJECT_ROOT)
    artifacts = load_g_artifacts(
        Path(config.artifact_manifest_path).parent,
        expected_manifest_sha256=config.artifact_manifest_sha256,
    )
    genesis = genesis_state()
    state = StateRecord(
        config.state_lineage_id,
        0,
        sha256_json(genesis),
        genesis,
    )
    return prepare_entry(
        config=config,
        state=state,
        bundle=bundle,
        completed_health=[],
        completed_rolling=rolling,
        maturity_gaps=[],
        artifacts=artifacts,
        calendar=FULL_EXCHANGE_CALENDAR,
    )


def test_projection_portable_hydration_and_prepare_entry_are_equivalent(
    service: Any,
) -> None:
    canonical = _canonical()
    projected = service._project_canonical_v16(canonical, calendar=FULL_EXCHANGE_CALENDAR)
    hydrated = hydrate(
        encode(
            projected,
            calendar=ARTIFACT_CALENDAR,
            canonical_integrity_hash=canonical._integrity_hash,
        )
    ).bundle

    assert hydrated.scan_result.recommended == canonical.scan_result.recommended
    for live, portable in zip(projected.scan_result.recommended, hydrated.scan_result.recommended):
        assert live == portable
    for item, stock in zip(projected.snapshot["symbols"], canonical.scan_result.recommended):
        assert item["code"] == stock.code
        assert item["rank"] == stock.rank
        assert item["score"] == stock.score
        assert item["snapshot_price"] == stock.buy_price
        assert item["boards"] == canonical.scan_result.stock_all_boards[stock.code]
        assert item["best_board"] == canonical.scan_result.stock_best_board[stock.code]
        assert item["cci"] == canonical.scan_result.stock_cci[stock.code]
        assert item["volume_937"] == canonical.scan_result.stock_early_vol[stock.code]

    assert _prepared(projected).commit == _prepared(hydrated).commit


def test_unordered_codes_stages_maps_and_derived_pools_are_canonicalized(
    service: Any,
) -> None:
    forward = service._project_canonical_v16(_canonical(), calendar=FULL_EXCHANGE_CALENDAR)
    reverse = service._project_canonical_v16(
        _canonical(reverse_inputs=True), calendar=FULL_EXCHANGE_CALENDAR
    )
    failures: list[str] = []
    expected_codes = list(sorted(CODES))
    assert forward.snapshot == reverse.snapshot
    assert forward.snapshot_hash == reverse.snapshot_hash
    if forward.snapshot["comparison_pool_codes"] != expected_codes:
        failures.append("comparison pool is not the sorted static canonical universe")
    if forward.comparison_pool_codes != tuple(expected_codes):
        failures.append("frozen comparison pool is not the sorted static universe")
    if forward.snapshot["scan_input_codes"] != expected_codes:
        failures.append("scan input codes are not canonicalized")
    if forward.snapshot["raw_evidence_codes"] != list(RAW_EVIDENCE_CODES):
        failures.append("raw evidence union is not canonicalized")
    for name in ("step0_codes", "step2_codes"):
        if forward.snapshot["stages"][name] != expected_codes:
            failures.append(f"{name} are not canonicalized")
    for board, codes in forward.snapshot["stages"]["step2_boards_detail"].items():
        if codes != sorted(codes):
            failures.append(f"board {board} members are not canonicalized")

    assert not failures, "\n".join(failures)


def test_breadth_and_history_are_copied_from_frozen_canonical_facts(
    service: Any,
) -> None:
    projected = service._project_canonical_v16(_canonical(), calendar=FULL_EXCHANGE_CALENDAR)

    assert projected.breadth_valid_n == 3
    assert projected.breadth_down_n == 1
    assert projected.snapshot["breadth_valid_n"] == 3
    assert projected.snapshot["breadth_down_n"] == 1
    assert projected.snapshot["breadth_market_source_hash"] == "b" * 64
    assert projected.snapshot["breadth_market_missing_codes"] == []
    assert projected.snapshot["breadth_market_conflict_codes"] == []
    assert projected.snapshot["raw_evidence_codes"] == list(RAW_EVIDENCE_CODES)
    assert BREADTH_ONLY_CODE not in projected.snapshot["comparison_pool_codes"]
    assert BREADTH_ONLY_CODE not in projected.snapshot["scan_input_codes"]
    assert projected.snapshot["history_date_valid_counts"] == HISTORY_COUNTS
    assert projected.snapshot["history_min_date_coverage"] == 1.0


@pytest.mark.parametrize("missing_side", ["bars", "hashes"])
def test_projection_rejects_mismatched_raw_evidence_key_sets(
    service: Any,
    missing_side: str,
) -> None:
    canonical = _canonical()
    if missing_side == "bars":
        canonical = replace(
            canonical,
            early_bars={
                code: bars
                for code, bars in canonical.early_bars.items()
                if code != BREADTH_ONLY_CODE
            },
        )
    else:
        canonical = replace(
            canonical,
            early_source_hashes={
                code: source_hash
                for code, source_hash in canonical.early_source_hashes.items()
                if code != BREADTH_ONLY_CODE
            },
        )
    canonical = _rehash(canonical)

    with pytest.raises(V20SemanticConflict, match="raw evidence"):
        service._project_canonical_v16(canonical, calendar=FULL_EXCHANGE_CALENDAR)


def test_bad_rolling_g_evaluation_receives_all_recommendation_prior_amounts(
    service: Any,
) -> None:
    expected = {"603068": 123_456_789.0, "600000": 987_654_321.0}
    canonical = _canonical()
    projected = service._project_canonical_v16(canonical, calendar=FULL_EXCHANGE_CALENDAR)
    rolling = tuple(
        CompletedRolling(
            batch_id=f"batch-{index}",
            signal_date=date(2026, 8, index + 1),
            t2_date=date(2026, 8, index + 3),
            batch_return=-0.05 if index < 5 else 0.02,
        )
        for index in range(7)
    )
    assert projected.prior_amount_yuan == expected
    assert projected.snapshot["prior_amount_yuan"] == expected
    assert _prepared(projected, rolling=rolling).commit is not None


def test_zero_recommendations_project_encode_hydrate_and_prepare_no_signal(
    service: Any,
) -> None:
    canonical = _canonical(recommended=())
    canonical = replace(
        canonical,
        scan_result=replace(canonical.scan_result, recommended=[], final_candidates=0),
    )
    canonical = _rehash(canonical)
    projected = service._project_canonical_v16(canonical, calendar=FULL_EXCHANGE_CALENDAR)
    hydrated = hydrate(
        encode(
            projected,
            calendar=ARTIFACT_CALENDAR,
            canonical_integrity_hash=canonical._integrity_hash,
        )
    ).bundle

    assert projected.scan_result.recommended == []
    assert hydrated.scan_result.recommended == []
    assert hydrated.snapshot["list_n"] == 0
    assert _prepared(hydrated).commit.semantic["action"] == "NO_SIGNAL"


def test_portable_artifact_omits_full_raw_stock_and_history(service: Any) -> None:
    projected = service._project_canonical_v16(_canonical(), calendar=FULL_EXCHANGE_CALENDAR)
    payload = encode(
        projected,
        calendar=ARTIFACT_CALENDAR,
        canonical_integrity_hash=_canonical()._integrity_hash,
    )
    serialized = repr(payload)

    assert "history_df" not in serialized
    assert "avg_daily_return_20d" not in serialized
    assert "history_raw" not in serialized
    assert set(payload) == {
        "schema_version",
        "trade_date",
        "frozen_at",
        "canonical_integrity_hash",
        "calendar",
        "v20_snapshot",
        "v20_snapshot_hash",
        "legacy_recommendation",
    }
    assert payload["legacy_recommendation"] == {
        "stock_code": "603068",
        "stock_name": "recommended-one",
        "board_name": "board-603068",
        "open_price": 10.0,
        "prev_close": 10.0,
        "latest_price": 20.25,
        "lgb_score": 9.75,
        "hot_board_count": 2,
        "final_candidates": 2,
    }


def test_encode_rejects_v20_calendar_disagreement_with_canonical_master(
    service: Any,
) -> None:
    projected = service._project_canonical_v16(_canonical(), calendar=FULL_EXCHANGE_CALENDAR)
    mismatched = replace(
        projected,
        computation_calendar=(
            date(2026, 8, 31),
            TRADE_DATE,
            date(2026, 9, 3),
            date(2026, 9, 4),
        ),
    )

    with pytest.raises(V20SemanticConflict, match="disagrees with canonical"):
        encode(
            mismatched,
            calendar=ARTIFACT_CALENDAR,
            canonical_integrity_hash=_canonical()._integrity_hash,
        )
