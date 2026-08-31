"""The DayGate shadow path must be unable to interfere with iQuant."""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from src.strategy.v16_day_gate_shadow import (
    freeze_v16_day_gate_runtime,
    freeze_v16_scan_snapshot,
)
from src.web import v15_scan_service


def test_legacy_recommendation_payload_is_unchanged_and_has_no_gate_fields():
    top1 = SimpleNamespace(
        code="600000",
        name="example",
        buy_price=12.34567,
        score=0.12345678,
    )
    result = SimpleNamespace(
        recommended=[top1],
        stock_best_board={"600000": "board-a"},
        step2_hot_board_count=7,
        final_candidates=19,
    )
    stock_data = {"600000": SimpleNamespace(open_price=11.11119, prev_close=10.98765)}

    payload = v15_scan_service._build_v16_recommendation_payload(result, stock_data)

    assert payload == {
        "stock_code": "600000",
        "stock_name": "example",
        "board_name": "board-a",
        "open_price": 11.1112,
        "prev_close": 10.9877,
        "latest_price": 12.3457,
        "lgb_score": 0.123457,
        "hot_board_count": 7,
        "final_candidates": 19,
    }
    assert not any("gate" in key for key in payload)


def test_no_pick_payload_remains_none():
    result = SimpleNamespace(recommended=[])

    assert v15_scan_service._build_v16_recommendation_payload(result, {}) is None


@pytest.mark.asyncio
async def test_scheduling_returns_before_a_never_finishing_shadow_worker(monkeypatch):
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_worker(_snapshot, _runtime):
        started.set()
        await release.wait()

    monkeypatch.setattr(v15_scan_service, "_run_v16_day_gate_shadow", slow_worker)
    snapshot = {"run_id": "test-run"}

    v15_scan_service._schedule_v16_day_gate_shadow(snapshot, object())
    await asyncio.wait_for(started.wait(), timeout=1)

    assert any(not task.done() for task in v15_scan_service._DAY_GATE_SHADOW_TASKS)
    release.set()
    await asyncio.gather(*tuple(v15_scan_service._DAY_GATE_SHADOW_TASKS))


@pytest.mark.asyncio
async def test_shadow_failure_is_swallowed_and_does_not_touch_scan_state(
    monkeypatch,
    caplog,
):
    scan_state = v15_scan_service.V15ScanState(
        today_recommendation={"stock_code": "600000"},
        scan_error=None,
    )

    def fail(_snapshot, _runtime):
        raise RuntimeError("evidence disk unavailable")

    monkeypatch.setattr(v15_scan_service, "_execute_v16_day_gate_shadow_sync", fail)

    await v15_scan_service._run_v16_day_gate_shadow(
        {"run_id": "failed-run"},
        object(),
    )

    assert scan_state.today_recommendation == {"stock_code": "600000"}
    assert scan_state.scan_error is None
    assert "shadow worker failed" in caplog.text


@pytest.mark.asyncio
async def test_cleanup_drains_a_running_shadow_thread(monkeypatch):
    started = threading.Event()
    release = threading.Event()

    def blocking_worker(_snapshot, _runtime):
        started.set()
        assert release.wait(timeout=2)
        return None

    monkeypatch.setattr(
        v15_scan_service,
        "_execute_v16_day_gate_shadow_sync",
        blocking_worker,
    )
    v15_scan_service._schedule_v16_day_gate_shadow(
        {"run_id": "cleanup-run"},
        object(),
    )
    assert await asyncio.to_thread(started.wait, 1)
    threading.Timer(0.05, release.set).start()

    await v15_scan_service.cleanup_scan_resources(v15_scan_service.V15ScanState())

    assert release.is_set()
    assert not v15_scan_service._DAY_GATE_SHADOW_TASKS
    assert v15_scan_service._DAY_GATE_SHADOW_EXECUTOR is None


def test_sync_worker_appends_evidence_without_changing_recommendation(
    tmp_path: Path,
    monkeypatch,
):
    config_path = tmp_path / "config" / "v16-day-gate.yaml"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        """\
schema_version: v16-day-gate-runtime/v1
mode: shadow
top_k: 10
evidence_dir: data/v16_day_gate
send_feishu: false
approved_taxonomy_artifact_path: null
approved_taxonomy_artifact_sha256: null
board_relevance:
  cache_path: data/board_relevance_cache.json
  allowed_levels: ["高"]
  exclude_unrated: true
  exclude_broad_boards: true
policy:
  version: test-unfitted
  min_largest_cluster_share: null
  max_effective_cluster_count: null
  min_top3_main_cluster_coverage: null
  min_driver_breadth: null
""",
        encoding="utf-8",
    )
    cache_path = tmp_path / "data" / "board_relevance_cache.json"
    cache_path.parent.mkdir(parents=True)
    cache_path.write_text(
        json.dumps(
            {"稀土永磁::600549": {"level": "高", "reason": "core"}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    for relative in (
        "models/lgbrank_latest.txt",
        "models/feature_list.json",
        "src/strategy/strategies/v16_scanner.py",
        "data/board_constituents.json",
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("versioned test artifact", encoding="utf-8")

    ranked = SimpleNamespace(
        code="600549",
        name="test-stock",
        score=0.5,
        rank=1,
        buy_price=20.0,
    )
    scan_result = SimpleNamespace(
        recommended=[ranked],
        all_scored=[ranked],
        stock_best_board={"600549": "稀土永磁"},
        stock_all_boards={"600549": ["稀土永磁"]},
        stock_gain_from_open={"600549": 1.0},
        stock_is_driver={"600549": True},
        step2_board_avg_gains={"稀土永磁": 1.0},
    )
    market = SimpleNamespace(
        open_price=19.0,
        prev_close=18.0,
        price_940=20.0,
        high_940=20.1,
        low_940=18.9,
        volume_940=100_000.0,
        volume_937=70_000.0,
    )
    recommendation = {"stock_code": "600549", "latest_price": 20.0}
    snapshot = freeze_v16_scan_snapshot(
        scan_result,
        {"600549": market},
        recommendation,
        frozen_at=datetime(2026, 8, 25, 9, 40, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    monkeypatch.setattr(v15_scan_service, "_PROJECT_ROOT", tmp_path)
    runtime = freeze_v16_day_gate_runtime(
        tmp_path,
        ranking_model_sha256="1" * 64,
        ranking_feature_list_sha256="2" * 64,
        captured_at=datetime(2026, 8, 25, 9, 40, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    assert runtime is not None

    outcome = v15_scan_service._execute_v16_day_gate_shadow_sync(snapshot, runtime)

    assert outcome is not None
    send_feishu, message, evidence_path = outcome
    assert send_feishu is False
    assert "effective_action: PASS_THROUGH" in message
    evidence = json.loads(Path(evidence_path).read_text(encoding="utf-8"))
    assert evidence["gate_decision"]["state"] == "watch"
    assert evidence["gate_input"]["model_version"] == "1" * 64
    assert (
        evidence["frozen_snapshot"]["shadow_evaluation"]["provenance"]["feature_hash"] == "2" * 64
    )
    assert evidence["frozen_snapshot"]["recommendation_payload"] == recommendation
    assert recommendation == {"stock_code": "600549", "latest_price": 20.0}
