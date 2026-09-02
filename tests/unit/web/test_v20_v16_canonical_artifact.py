from __future__ import annotations

import copy
import math
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.data.database.v20_repository import (
    StateRecord,
    V20SemanticConflict,
    sha256_json,
)
from src.strategy.lgbrank_scorer import ScoredStock
from src.strategy.strategies.v16_scanner import V16ScanResult
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.decision_engine import CompletedRolling, genesis_state, prepare_entry
from src.strategy.v20.models import V20_V16_SNAPSHOT_SCHEMA
from src.strategy.v20.runtime_config import load_v20_runtime_config
from src.web.v15_scan_service import (
    V15ScanState,
    _build_v16_recommendation_payload,
    _restore_canonical_artifact,
)
from src.web.v20_scan_pipeline import FrozenV16ScanBundle
from src.web.v20_v16_canonical_artifact import (
    PORTABLE_FROZEN_V16_SCHEMA_V1,
    encode,
    hydrate,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]

TZ = ZoneInfo("Asia/Shanghai")
D0 = date(2026, 8, 31)
D1 = date(2026, 9, 1)
D2 = date(2026, 9, 2)
CALENDAR = (D0, D1, D2)
CANONICAL_INTEGRITY_HASH = "d" * 64
HISTORY_DATES = tuple(
    D0 - timedelta(days=offset)
    for offset in range(1, 101)
    if (D0 - timedelta(days=offset)).weekday() < 5
)[:37][::-1]
STOCKS = [
    ScoredStock(
        code="000001",
        name="stock-one",
        score=9.5,
        rank=1,
        buy_price=12.5,
    ),
    ScoredStock(
        code="600000",
        name="stock-two",
        score=8.25,
        rank=2,
        buy_price=20.0,
    ),
]


def _snapshot(recommendations: list[ScoredStock]) -> dict[str, Any]:
    symbols = [
        {
            "rank": stock.rank,
            "code": stock.code,
            "name": stock.name,
            "score": stock.score,
            "snapshot_price": stock.buy_price,
            "boards": ["board-a", "board-b"] if stock.rank == 1 else ["board-c"],
            "best_board": "board-a" if stock.rank == 1 else "board-c",
            "is_driver": stock.rank == 1,
            "cci": 101.5 if stock.rank == 1 else -12.25,
            "volume_937": 1000.0 + stock.rank,
            "history_hash": "a" * 64 if stock.rank == 1 else "b" * 64,
            "early_source_hash": "c" * 64 if stock.rank == 1 else "e" * 64,
        }
        for stock in recommendations
    ]
    return {
        "schema_version": V20_V16_SNAPSHOT_SCHEMA,
        "trade_date": D0.isoformat(),
        "last_complete_bar": "09:39",
        "early_market_source_hash": "1" * 64,
        "early_market_conflict_codes": [],
        "breadth_market_source_hash": "2" * 64,
        "breadth_market_missing_codes": [],
        "breadth_market_conflict_codes": [],
        "scorer_model_sha256": "3" * 64,
        "scorer_feature_sha256": "4" * 64,
        "list_complete": True,
        "list_n": len(symbols),
        "symbols": symbols,
        # 000002 is breadth-only raw evidence and must survive portability.
        "raw_evidence_codes": ["000001", "000002", "600000"],
        "scan_input_codes": ["000001", "600000"],
        "scan_input_failure_codes": [],
        "scan_input_coverage": 1.0,
        "history_profile_id": "CANONICAL_V16_V1",
        "history_input_hashes": {
            "000001": "a" * 64,
            "600000": "b" * 64,
        },
        "history_date_valid_counts": {day.isoformat(): 2 for day in HISTORY_DATES},
        "history_min_date_coverage": 1.0,
        "comparison_pool_codes": ["000001", "600000"],
        "comparison_pool_hash": sha256_json(["000001", "600000"]),
        "breadth_valid_n": 1800,
        "breadth_down_n": 700,
        "prior_trade_date": date(2026, 8, 28).isoformat(),
        "prior_amount_yuan": {stock.code: 1_000_000.0 + stock.rank for stock in recommendations},
        "funnel": {
            "step0_universe_count": 2,
            "step2_hot_board_count": 2,
            "step2_filtered_by_avg_gain": 0,
            "step3_count": 2,
            "step4_count": 2,
            "step5_count": 2,
            "step6_count": 2,
            "step6_5_count": 2,
            "step6_6_count": 2,
            "final_candidates": 2,
        },
        "stages": {
            "step0_codes": ["000001", "600000"],
            "step2_boards_detail": {"board-a": ["000001"], "board-c": ["600000"]},
            "step2_codes": ["000001", "600000"],
            "st_eligible_codes": ["000001", "600000"],
            "step3_codes": ["000001", "600000"],
            "step4_codes": ["000001", "600000"],
            "step5_codes": ["000001", "600000"],
            "step6_codes": ["000001", "600000"],
            "step6_5_codes": ["000001", "600000"],
            "step6_6_codes": ["000001", "600000"],
        },
        "board_avg_gains": {"board-a": 1.2, "board-b": 0.8, "board-c": -0.2},
    }


def _scan(recommendations: list[ScoredStock]) -> V16ScanResult:
    return V16ScanResult(
        recommended=recommendations,
        all_scored=[],
        step0_universe_count=2,
        step2_hot_board_count=2,
        step2_filtered_by_avg_gain=0,
        step3_count=2,
        step4_count=2,
        step5_count=2,
        step6_count=2,
        step6_5_count=2,
        step6_6_count=2,
        final_candidates=2,
        step0_codes=["000001", "600000"],
        step2_boards_detail={"board-a": ["000001"], "board-c": ["600000"]},
        step2_codes=["000001", "600000"],
        st_eligible_codes=["000001", "600000"],
        step3_codes=["000001", "600000"],
        step4_codes=["000001", "600000"],
        step5_codes=["000001", "600000"],
        step6_codes=["000001", "600000"],
        step6_5_codes=["000001", "600000"],
        step6_6_codes=["000001", "600000"],
        stock_best_board={
            stock.code: "board-a" if stock.rank == 1 else "board-c" for stock in recommendations
        },
        stock_all_boards={
            stock.code: ["board-a", "board-b"] if stock.rank == 1 else ["board-c"]
            for stock in recommendations
        },
        step2_board_avg_gains={"board-a": 1.2, "board-b": 0.8, "board-c": -0.2},
        stock_gain_from_open={stock.code: 1.0 + stock.rank for stock in recommendations},
        stock_is_driver={stock.code: stock.rank == 1 for stock in recommendations},
        stock_cci={stock.code: 101.5 if stock.rank == 1 else -12.25 for stock in recommendations},
        stock_early_vol={stock.code: 1000.0 + stock.rank for stock in recommendations},
        step2_all_board_avg_gains={"board-a": 1.2, "board-z": 0.1},
    )


def _bundle(recommendations: list[ScoredStock] = STOCKS) -> FrozenV16ScanBundle:
    snapshot = _snapshot(recommendations)
    scan_result = _scan(recommendations)
    stock_data = {
        stock.code: SimpleNamespace(
            open_price=10.12345 + stock.rank,
            prev_close=9.98765 + stock.rank,
        )
        for stock in recommendations
    }
    return FrozenV16ScanBundle(
        trade_date=D0,
        frozen_at=datetime(2026, 8, 31, 9, 39, 10, tzinfo=TZ),
        scan_result=scan_result,
        stock_data=stock_data,
        comparison_pool_codes=("000001", "600000"),
        breadth_valid_n=1800,
        breadth_down_n=700,
        prior_trade_date=date(2026, 8, 28),
        prior_amount_yuan={stock.code: 1_000_000.0 + stock.rank for stock in recommendations},
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
        legacy_recommendation=_build_v16_recommendation_payload(scan_result, stock_data),
    )


def test_live_encode_hydrate_preserves_prepare_entry_inputs_and_hashes() -> None:
    original = _bundle()
    payload = encode(
        original,
        calendar=CALENDAR,
        canonical_integrity_hash=CANONICAL_INTEGRITY_HASH,
    )
    hydrated = hydrate(payload)
    bundle = hydrated.bundle

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
    assert payload["v20_snapshot"] == original.snapshot
    assert payload["v20_snapshot_hash"] == original.snapshot_hash
    assert hydrated.calendar == CALENDAR
    assert hydrated.canonical_integrity_hash == CANONICAL_INTEGRITY_HASH
    assert hydrated.payload == payload
    assert bundle.trade_date == original.trade_date
    assert bundle.frozen_at == original.frozen_at
    assert bundle.snapshot == original.snapshot
    assert bundle.snapshot["raw_evidence_codes"] == ["000001", "000002", "600000"]
    assert bundle.snapshot_hash == original.snapshot_hash
    assert bundle.stock_data == {}
    assert bundle.comparison_pool_codes == original.comparison_pool_codes
    assert bundle.breadth_valid_n == original.breadth_valid_n
    assert bundle.breadth_down_n == original.breadth_down_n
    assert bundle.prior_trade_date == original.prior_trade_date
    assert bundle.prior_amount_yuan == original.prior_amount_yuan
    assert bundle.scan_result.recommended == original.scan_result.recommended
    assert bundle.stock_data == {}
    assert bundle.legacy_recommendation == payload["legacy_recommendation"]
    assert bundle.legacy_recommendation == {
        "stock_code": "000001",
        "stock_name": "stock-one",
        "board_name": "board-a",
        "open_price": 11.1235,
        "prev_close": 10.9877,
        "latest_price": 12.5,
        "lgb_score": 9.5,
        "hot_board_count": 2,
        "final_candidates": 2,
    }
    assert bundle.scan_result.stock_best_board == original.scan_result.stock_best_board
    assert bundle.scan_result.stock_all_boards == original.scan_result.stock_all_boards
    assert bundle.scan_result.stock_is_driver == original.scan_result.stock_is_driver
    assert bundle.scan_result.stock_cci == original.scan_result.stock_cci
    assert bundle.scan_result.stock_early_vol == original.scan_result.stock_early_vol
    assert bundle.scan_result.step2_board_avg_gains == original.scan_result.step2_board_avg_gains
    assert (
        bundle.scan_result.step0_universe_count,
        bundle.scan_result.step2_hot_board_count,
        bundle.scan_result.step2_filtered_by_avg_gain,
        bundle.scan_result.step3_count,
        bundle.scan_result.step4_count,
        bundle.scan_result.step5_count,
        bundle.scan_result.step6_count,
        bundle.scan_result.step6_5_count,
        bundle.scan_result.step6_6_count,
        bundle.scan_result.final_candidates,
    ) == (
        original.scan_result.step0_universe_count,
        original.scan_result.step2_hot_board_count,
        original.scan_result.step2_filtered_by_avg_gain,
        original.scan_result.step3_count,
        original.scan_result.step4_count,
        original.scan_result.step5_count,
        original.scan_result.step6_count,
        original.scan_result.step6_5_count,
        original.scan_result.step6_6_count,
        original.scan_result.final_candidates,
    )

    isolated = hydrated.payload
    isolated["v20_snapshot"]["symbols"].clear()
    assert hydrated.payload["v20_snapshot"]["symbols"] == original.snapshot["symbols"]


def test_zero_recommendations_are_a_valid_portable_no_signal() -> None:
    payload = encode(
        _bundle([]),
        calendar=CALENDAR,
        canonical_integrity_hash=CANONICAL_INTEGRITY_HASH,
    )
    hydrated = hydrate(payload)

    assert hydrated.bundle.snapshot["list_n"] == 0
    assert hydrated.bundle.scan_result.recommended == []
    assert hydrated.bundle.legacy_recommendation is None


def test_v1_payload_hydrates_without_fabricating_legacy_recommendation() -> None:
    payload = encode(
        _bundle(),
        calendar=CALENDAR,
        canonical_integrity_hash=CANONICAL_INTEGRITY_HASH,
    )
    payload["schema_version"] = PORTABLE_FROZEN_V16_SCHEMA_V1
    payload.pop("legacy_recommendation")

    hydrated = hydrate(payload)

    assert hydrated.bundle.scan_result.recommended == STOCKS
    assert hydrated.bundle.stock_data == {}
    assert hydrated.bundle.legacy_recommendation is None
    assert hydrated.payload == payload


def test_v2_restart_restores_the_exact_computed_legacy_projection() -> None:
    original = _bundle()
    expected = _build_v16_recommendation_payload(
        original.scan_result,
        original.stock_data,
    )
    hydrated = hydrate(
        encode(
            original,
            calendar=CALENDAR,
            canonical_integrity_hash=CANONICAL_INTEGRITY_HASH,
        )
    )
    state = V15ScanState(today_recommendation={"stock_code": "stale"})

    _restore_canonical_artifact(
        state,
        D0,
        hydrated.bundle,
        datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
    )

    assert state.today_recommendation == expected
    assert state.today_recommendation is not hydrated.bundle.legacy_recommendation


def test_encode_rejects_legacy_projection_different_from_canonical_stock_data() -> None:
    original = _bundle()
    mismatched = copy.deepcopy(dict(original.legacy_recommendation or {}))
    mismatched["open_price"] += 1.0

    with pytest.raises(
        V20SemanticConflict,
        match="differs from canonical stock data",
    ):
        encode(
            replace(original, legacy_recommendation=mismatched),
            calendar=CALENDAR,
            canonical_integrity_hash=CANONICAL_INTEGRITY_HASH,
        )


def test_prepare_entry_commit_is_identical_after_portable_hydration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_v20_runtime_config(PROJECT_ROOT)
    artifacts = load_g_artifacts(
        config.artifact_manifest_path.parent,
        expected_manifest_sha256=config.artifact_manifest_sha256,
    )
    original = _bundle()
    hydrated = hydrate(
        encode(
            original,
            calendar=CALENDAR,
            canonical_integrity_hash=CANONICAL_INTEGRITY_HASH,
        )
    )
    genesis = genesis_state()
    state = StateRecord(config.state_lineage_id, 0, sha256_json(genesis), genesis)
    rolling = [
        CompletedRolling(
            batch_id=f"old-{index}",
            signal_date=date(2026, 7, index + 1),
            t2_date=date(2026, 7, index + 3),
            batch_return=0.01,
        )
        for index in range(7)
    ]
    common = {
        "config": config,
        "state": state,
        "completed_health": [],
        "completed_rolling": rolling,
        "maturity_gaps": [],
        "artifacts": artifacts,
        "calendar": CALENDAR,
    }
    live = prepare_entry(bundle=original, **common)
    portable = prepare_entry(bundle=hydrated.bundle, **common)

    assert live.commit == portable.commit


def _rehash(payload: dict[str, Any]) -> dict[str, Any]:
    payload["v20_snapshot_hash"] = sha256_json(payload["v20_snapshot"])
    return payload


def _payload() -> dict[str, Any]:
    return encode(
        _bundle(),
        calendar=CALENDAR,
        canonical_integrity_hash=CANONICAL_INTEGRITY_HASH,
    )


@pytest.mark.parametrize(
    "mutator",
    [
        lambda payload: payload.__setitem__("canonical_integrity_hash", "not-a-hash"),
        lambda payload: payload.__setitem__("v20_snapshot_hash", "f" * 64),
        lambda payload: payload.__setitem__("frozen_at", "2026-08-31T09:39:10"),
        lambda payload: payload.__setitem__("calendar", [D0, D1]),
        lambda payload: payload.__setitem__("calendar", [D0, D2, D1]),
        lambda payload: payload.__setitem__("unknown", 1),
        lambda payload: payload.__setitem__("stock_data", {}),
        lambda payload: payload.__setitem__("history_raw", {}),
        lambda payload: payload.__setitem__("quotes", {}),
        lambda payload: payload.__setitem__("early_bars", {}),
        lambda payload: payload.__setitem__("legacy_recommendation", None),
        lambda payload: payload.__setitem__("legacy_recommendation", []),
        lambda payload: payload["legacy_recommendation"].__setitem__("stock_code", "600000"),
        lambda payload: payload["legacy_recommendation"].__setitem__("open_price", 0.0),
        lambda payload: payload["legacy_recommendation"].pop("prev_close"),
        lambda payload: payload["v20_snapshot"].__setitem__("unknown", 1),
        lambda payload: payload["v20_snapshot"].__setitem__("trade_date", D1.isoformat()),
        lambda payload: payload["v20_snapshot"]["symbols"][0].__setitem__("score", True),
        lambda payload: payload["v20_snapshot"]["symbols"][0].__setitem__("score", math.nan),
        lambda payload: payload["v20_snapshot"]["symbols"].__setitem__(
            1,
            {**payload["v20_snapshot"]["symbols"][1], "code": "000001"},
        ),
        lambda payload: payload["v20_snapshot"]["symbols"][1].__setitem__("rank", 3),
        lambda payload: payload["v20_snapshot"]["raw_evidence_codes"].pop(1),
    ],
)
def test_hydrate_rejects_strict_contract_violations(mutator: Any) -> None:
    payload = _payload()
    mutator(payload)
    with pytest.raises(V20SemanticConflict):
        hydrate(payload)


def test_encode_rejects_naive_frozen_at_snapshot_hash_or_calendar_drift() -> None:
    naive = replace(_bundle(), frozen_at=datetime(2026, 8, 31, 9, 39, 10))
    with pytest.raises(V20SemanticConflict):
        encode(naive, calendar=CALENDAR, canonical_integrity_hash=CANONICAL_INTEGRITY_HASH)

    bad_hash = replace(_bundle(), snapshot_hash="f" * 64)
    with pytest.raises(V20SemanticConflict):
        encode(bad_hash, calendar=CALENDAR, canonical_integrity_hash=CANONICAL_INTEGRITY_HASH)

    wrong_day = copy.deepcopy(_bundle())
    with pytest.raises(V20SemanticConflict):
        encode(
            wrong_day,
            calendar=(D1, D2, date(2026, 9, 3)),
            canonical_integrity_hash=CANONICAL_INTEGRITY_HASH,
        )


@pytest.mark.parametrize(
    "mutator",
    [
        lambda snapshot: snapshot.__setitem__(
            "history_date_valid_counts",
            {"000001": 37, **dict(snapshot["history_date_valid_counts"])},
        ),
        lambda snapshot: snapshot["history_date_valid_counts"].__setitem__(
            next(iter(snapshot["history_date_valid_counts"])), 3
        ),
        lambda snapshot: snapshot.__setitem__("history_min_date_coverage", 0.5),
        lambda snapshot: snapshot.__setitem__("prior_trade_date", "2026-08-27"),
        lambda snapshot: snapshot["prior_amount_yuan"].pop("000001"),
        lambda snapshot: snapshot["prior_amount_yuan"].__setitem__("000001", 0.0),
        lambda snapshot: snapshot.__setitem__("scan_input_failure_codes", ["000001"]),
        lambda snapshot: snapshot.__setitem__("raw_evidence_codes", ["000002", "600000"]),
        lambda snapshot: snapshot.__setitem__(
            "raw_evidence_codes", ["000001", "000002", "000002", "600000"]
        ),
        lambda snapshot: snapshot.__setitem__(
            "raw_evidence_codes", ["000001", "000002", "300001", "600000"]
        ),
        lambda snapshot: snapshot.__setitem__("breadth_market_missing_codes", ["000002"]),
        lambda snapshot: snapshot["funnel"].__setitem__("step3_count", 1),
        lambda snapshot: snapshot["stages"].__setitem__("step3_codes", ["000001"]),
    ],
)
def test_hydrate_rejects_rehashed_semantic_contradictions(mutator: Any) -> None:
    payload = _payload()
    mutator(payload["v20_snapshot"])
    payload["v20_snapshot_hash"] = sha256_json(payload["v20_snapshot"])
    with pytest.raises(V20SemanticConflict):
        hydrate(payload)
