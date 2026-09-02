import hashlib
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from src.data.database.v20_repository import StateRecord, sha256_json
from src.strategy.lgbrank_scorer import ScoredStock
from src.strategy.strategies.v16_scanner import V16ScanResult
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.decision_engine import (
    ActiveRollingGap,
    CompletedRolling,
    genesis_state,
    prepare_entry,
)
from src.strategy.v20.models import (
    V20_ENTRY_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
    V20_V16_SNAPSHOT_SCHEMA,
)
from src.strategy.v20.runtime_config import load_v20_runtime_config
from src.web.v20_scan_pipeline import FrozenV16ScanBundle

PROJECT_ROOT = Path(__file__).resolve().parents[4]
TZ = ZoneInfo("Asia/Shanghai")


def test_prepare_entry_keeps_full_list_and_builds_two_shadow_streams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_v20_runtime_config(PROJECT_ROOT)
    artifacts = load_g_artifacts(
        config.artifact_manifest_path.parent,
        expected_manifest_sha256=config.artifact_manifest_sha256,
    )
    stocks = [
        ScoredStock(
            code=f"0000{index:02d}",
            name=f"股票{index}",
            score=10.0 - index,
            rank=index,
            buy_price=10.0 + index,
        )
        for index in range(1, 11)
    ]
    scan = V16ScanResult(
        recommended=stocks,
        final_candidates=10,
        stock_all_boards={stock.code: ["BOARD"] for stock in stocks},
    )
    snapshot = {
        "schema_version": V20_V16_SNAPSHOT_SCHEMA,
        "trade_date": "2026-08-31",
        "last_complete_bar": "09:39",
        "funnel": {
            "step0_universe_count": 3200,
            "step2_hot_board_count": 9,
            "final_candidates": 10,
        },
        "board_avg_gains": {"BOARD": 1.2},
        "symbols": [
            {
                "rank": stock.rank,
                "code": stock.code,
                "name": stock.name,
                "score": stock.score,
                "snapshot_price": stock.buy_price,
                "boards": ["BOARD"],
                "best_board": "BOARD",
                "is_driver": True,
                "cci": None,
                "volume_937": None,
                "history_hash": "a" * 64,
                "early_source_hash": hashlib.sha256(
                    f"early-source:{stock.code}".encode()
                ).hexdigest(),
            }
            for stock in stocks
        ],
    }
    bundle = FrozenV16ScanBundle(
        trade_date=date(2026, 8, 31),
        frozen_at=datetime(2026, 8, 31, 9, 39, 10, tzinfo=TZ),
        scan_result=scan,
        stock_data={},
        comparison_pool_codes=tuple(f"60{index:04d}" for index in range(1000)),
        breadth_valid_n=2000,
        breadth_down_n=900,
        prior_trade_date=date(2026, 8, 28),
        prior_amount_yuan={stock.code: 2_000_000_000.0 for stock in stocks},
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
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

    prepared = prepare_entry(
        config=config,
        state=state,
        bundle=bundle,
        completed_health=[],
        completed_rolling=rolling,
        maturity_gaps=[],
        artifacts=artifacts,
        calendar=[
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
        ],
    )

    assert prepared.action == "ENTER"
    assert prepared.final_multiplier == 1.0
    assert len(prepared.commit.shadow_batches) == 2
    assert prepared.commit.model_batch is not None
    assert len(prepared.commit.model_batch.legs) == 10
    assert all(leg.relative_weight == 0.1 for leg in prepared.commit.model_batch.legs)
    assert [item["code"] for item in prepared.commit.semantic["symbols"]] == [
        stock.code for stock in stocks
    ]
    assert prepared.commit.snapshot["v16_snapshot_hash"] == bundle.snapshot_hash
    assert (
        prepared.commit.snapshot["policy_input_hash"]
        == prepared.commit.semantic["policy_input_hash"]
    )
    assert prepared.commit.semantic["state_semantics_hash"] == config.state_semantics_hash
    assert prepared.commit.semantic["schema_version"] == V20_ENTRY_SEMANTIC_SCHEMA
    assert prepared.commit.semantic["feishu_formatter_profile"] == V20_FEISHU_FORMATTER_PROFILE
    assert prepared.commit.semantic["v16_funnel"] == snapshot["funnel"]
    assert prepared.commit.semantic["v16_board_avg_gains"] == {"BOARD": 1.2}
    assert all(item["boards"] == ["BOARD"] for item in prepared.commit.semantic["symbols"])
    assert all(item["cci"] is None for item in prepared.commit.semantic["symbols"])
    assert all(item["volume_937"] is None for item in prepared.commit.semantic["symbols"])
    assert prepared.commit.next_state["state_revision"] == 1

    legacy_snapshot = {**snapshot, "schema_version": "v20-v16-snapshot/v1"}
    with pytest.raises(ValueError, match="unsupported V16 snapshot schema_version"):
        prepare_entry(
            config=config,
            state=state,
            bundle=replace(
                bundle,
                snapshot=legacy_snapshot,
                snapshot_hash=sha256_json(legacy_snapshot),
            ),
            completed_health=[],
            completed_rolling=rolling,
            maturity_gaps=[],
            artifacts=artifacts,
            calendar=[date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2)],
        )

    partial_snapshot = dict(snapshot)
    partial_snapshot.pop("board_avg_gains")
    with pytest.raises(ValueError, match="board_avg_gains"):
        prepare_entry(
            config=config,
            state=state,
            bundle=replace(
                bundle,
                snapshot=partial_snapshot,
                snapshot_hash=sha256_json(partial_snapshot),
            ),
            completed_health=[],
            completed_rolling=rolling,
            maturity_gaps=[],
            artifacts=artifacts,
            calendar=[date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2)],
        )

    missing_hash_snapshot = dict(snapshot)
    missing_hash_snapshot["symbols"] = [
        {key: value for key, value in item.items() if key != "early_source_hash"}
        for item in snapshot["symbols"]
    ]
    with pytest.raises(ValueError, match="formatter evidence is incomplete"):
        prepare_entry(
            config=config,
            state=state,
            bundle=replace(
                bundle,
                snapshot=missing_hash_snapshot,
                snapshot_hash=sha256_json(missing_hash_snapshot),
            ),
            completed_health=[],
            completed_rolling=rolling,
            maturity_gaps=[],
            artifacts=artifacts,
            calendar=[date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2)],
        )

    invalid_hash_snapshot = dict(snapshot)
    invalid_hash_snapshot["symbols"] = [
        {**item, "early_source_hash": "not-a-64-hex-digest"} for item in snapshot["symbols"]
    ]
    with pytest.raises(ValueError, match="early_source_hash is invalid"):
        prepare_entry(
            config=config,
            state=state,
            bundle=replace(
                bundle,
                snapshot=invalid_hash_snapshot,
                snapshot_hash=sha256_json(invalid_hash_snapshot),
            ),
            completed_health=[],
            completed_rolling=rolling,
            maturity_gaps=[],
            artifacts=artifacts,
            calendar=[date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2)],
        )

    revised_rolling = [
        *rolling[:-1],
        CompletedRolling(
            batch_id=rolling[-1].batch_id,
            signal_date=rolling[-1].signal_date,
            t2_date=rolling[-1].t2_date,
            batch_return=-0.01,
        ),
    ]
    revised = prepare_entry(
        config=config,
        state=state,
        bundle=bundle,
        completed_health=[],
        completed_rolling=revised_rolling,
        maturity_gaps=[],
        artifacts=artifacts,
        calendar=[date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2)],
    )
    assert revised.commit.snapshot_hash != prepared.commit.snapshot_hash
    assert revised.commit.decision_id != prepared.commit.decision_id

    future_date = date(2027, 1, 4)
    future_snapshot = {**snapshot, "trade_date": future_date.isoformat()}
    future_bundle = replace(
        bundle,
        trade_date=future_date,
        frozen_at=datetime(2027, 1, 4, 9, 39, 10, tzinfo=TZ),
        prior_trade_date=date(2026, 12, 31),
        snapshot=future_snapshot,
        snapshot_hash=sha256_json(future_snapshot),
    )
    bad_rolling = [
        CompletedRolling(
            batch_id=f"bad-{index}",
            signal_date=date(2026, 12, index + 1),
            t2_date=date(2026, 12, index + 3),
            batch_return=-0.01,
        )
        for index in range(7)
    ]
    missing_half_threshold = prepare_entry(
        config=config,
        state=state,
        bundle=future_bundle,
        completed_health=[],
        completed_rolling=bad_rolling,
        maturity_gaps=[],
        artifacts=artifacts,
        calendar=[future_date, date(2027, 1, 5), date(2027, 1, 6)],
    )
    assert missing_half_threshold.action == "ENTER"
    assert missing_half_threshold.final_multiplier == 0.5
    assert missing_half_threshold.commit.semantic["g_state"] == "UNKNOWN"
    assert "Q25_THRESHOLD_MISSING" in missing_half_threshold.commit.semantic["reason_codes"]

    # A still-incomplete rolling batch is present in both yesterday's durable
    # state and today's maturity query.  It remains one active gap instead of
    # crashing the daily slot on a duplicate identity.
    gap = ActiveRollingGap(
        gap_id="pending-gap",
        signal_date=date(2026, 8, 27),
        maturity_date=date(2026, 8, 28),
    )
    gap_state_payload = {
        **genesis,
        "official_rolling_gaps": [
            {
                "gap_id": gap.gap_id,
                "signal_date": gap.signal_date.isoformat(),
                "maturity_date": gap.maturity_date.isoformat(),
                "closed": False,
                "aged_out": False,
            }
        ],
    }
    gap_state = StateRecord(
        config.state_lineage_id,
        0,
        sha256_json(gap_state_payload),
        gap_state_payload,
    )
    with_duplicate_gap = prepare_entry(
        config=config,
        state=gap_state,
        bundle=bundle,
        completed_health=[],
        completed_rolling=rolling,
        maturity_gaps=[gap],
        artifacts=artifacts,
        calendar=[date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2)],
    )
    assert with_duplicate_gap.commit.semantic["rolling7_state"] == "UNKNOWN"
    assert with_duplicate_gap.commit.next_state["official_rolling_gaps"] == [
        {
            "gap_id": "pending-gap",
            "signal_date": "2026-08-27",
            "maturity_date": "2026-08-28",
            "closed": False,
            "aged_out": False,
        }
    ]

    completed_gap_payload = {
        **genesis,
        "official_rolling_gaps": [
            {
                "gap_id": rolling[0].batch_id,
                "signal_date": rolling[0].signal_date.isoformat(),
                "maturity_date": rolling[0].t2_date.isoformat(),
                "closed": False,
                "aged_out": False,
            }
        ],
    }
    completed_gap_state = StateRecord(
        config.state_lineage_id,
        0,
        sha256_json(completed_gap_payload),
        completed_gap_payload,
    )
    completed_same_day = prepare_entry(
        config=config,
        state=completed_gap_state,
        bundle=bundle,
        completed_health=[],
        completed_rolling=rolling,
        maturity_gaps=[],
        artifacts=artifacts,
        calendar=[date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2)],
    )
    assert completed_same_day.commit.semantic["rolling7_state"] == "NON_BAD"
    assert completed_same_day.commit.next_state["official_rolling_gaps"][0]["closed"] is True
