from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from src.common.v20_feishu import render_entry_message
from src.data.database.v20_repository import StateRecord, V20StateConflict, sha256_json
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.decision_engine import genesis_state, prepare_entry
from src.strategy.v20.models import V20_V16_SNAPSHOT_SCHEMA
from src.strategy.v20.runtime_config import load_v20_runtime_config
from src.strategy.v20.selection_scanner import V16ScanResult
from src.strategy.v20.selection_scorer import ScoredStock
from src.strategy.v22_slim.runtime_inputs import build_inputs
from src.web.v20_scan_pipeline import FrozenV16ScanBundle
from src.web.v20_service import V20Service

ROOT = Path(__file__).resolve().parents[3]
DAY = date(2026, 9, 14)
TZ = ZoneInfo("Asia/Shanghai")
CALENDAR = tuple(DAY + timedelta(days=i) for i in range(-40, 4))


@pytest.mark.asyncio
async def test_entry_only_profile_cannot_create_exit_lots_through_manual_monitor():
    service = object.__new__(V20Service)
    service.config = SimpleNamespace(strategy_version="V22-slim")
    with pytest.raises(V20StateConflict, match="entry-only"):
        await service.enroll_manual_monitor("a" * 64, "slim-test")


def fixture():
    config = load_v20_runtime_config(ROOT, ROOT / "config/v22-slim.yaml")
    payload = genesis_state()
    state = StateRecord(config.state_lineage_id, 0, sha256_json(payload), payload)
    stocks = [ScoredStock(f"600{i:03}", f"Stock {i}", 10.0 - i, i, 20.0) for i in range(1, 11)]
    scan = V16ScanResult(recommended=stocks, final_candidates=10)
    snapshot = {
        "schema_version": V20_V16_SNAPSHOT_SCHEMA,
        "trade_date": DAY.isoformat(),
        "last_complete_bar": "09:39",
        "funnel": {
            "step0_universe_count": 1000,
            "step2_hot_board_count": 1,
            "final_candidates": 10,
        },
        "board_avg_gains": {"BOARD": 1.0},
        "symbols": [
            {
                "rank": s.rank,
                "code": s.code,
                "name": s.name,
                "score": s.score,
                "snapshot_price": s.buy_price,
                "boards": ["BOARD"],
                "best_board": "BOARD",
                "is_driver": True,
                "cci": None,
                "volume_937": None,
                "history_hash": "a" * 64,
                "early_source_hash": "b" * 64,
            }
            for s in stocks
        ],
        "v22_market": {f"600{i:03}": {"close": 20.0, "amount": 100.0} for i in range(1000)},
        "v22_slim_inputs": {
            "schema": "v22-slim-inputs/v1",
            "health_before": payload["health"],
            "risk_before": 0,
            "h90": {"block": False},
            "strong": False,
        },
    }
    bundle = FrozenV16ScanBundle(
        DAY,
        datetime(2026, 9, 14, 9, 40, tzinfo=TZ),
        scan,
        {},
        tuple(snapshot["v22_market"]),
        1000,
        100,
        DAY - timedelta(days=1),
        {},
        snapshot,
        sha256_json(snapshot),
        CALENDAR,
    )
    return config, state, bundle


@pytest.mark.parametrize("block", [False, True])
def test_full_reference_list_top3_output_no_new_exit_legs_and_deterministic_replay(block):
    config, state, bundle = fixture()
    inputs = {**bundle.snapshot["v22_slim_inputs"], "h90": {"known": True, "block": block}}
    snapshot = {**bundle.snapshot, "v22_slim_inputs": inputs}
    bundle = replace(bundle, snapshot=snapshot, snapshot_hash=sha256_json(snapshot))
    kwargs = dict(
        config=config,
        state=state,
        bundle=bundle,
        completed_health=[],
        completed_rolling=[],
        maturity_gaps=[],
        calendar=CALENDAR,
        artifacts=load_g_artifacts(
            config.artifact_manifest_path.parent,
            expected_manifest_sha256=config.artifact_manifest_sha256,
        ),
        scheduled_exits_today=[{"code": "000001", "stock_name": "Legacy", "plan_time": "14:57"}],
    )
    first, retry = prepare_entry(**kwargs), prepare_entry(**kwargs)
    assert first.commit == retry.commit
    semantic = first.commit.semantic
    assert first.action == ("BLOCK" if block else "ENTER")
    assert len(semantic["symbols"]) == (0 if block else 3)
    assert len(semantic["reference_symbols"]) == 10
    assert len(first.commit.snapshot["symbols"]) == 10
    assert first.commit.model_batch is None
    assert len(first.commit.shadow_batches[0].payload["top3"]) == 3
    assert semantic["scheduled_exits_today"][0]["code"] == "000001"
    message = render_entry_message(
        semantic, generated_at=bundle.frozen_at, commit_marker="test", on_time=True
    )
    assert "V22-slim" in message and "D0-N2" in message and "H90" in message
    if block:
        assert "原始候选 10 只" in message and "合法无票" not in message
    else:
        assert "推荐前3" in message and "600004" not in message


@pytest.mark.asyncio
async def test_checkpoint_bootstrap_is_repeatable_and_only_consumes_prior_daily_inputs(monkeypatch):
    config, state, bundle = fixture()
    anchor = DAY - timedelta(days=1)
    market = {code: value["amount"] for code, value in bundle.snapshot["v22_market"].items()}
    seed = {
        "as_of": anchor.isoformat(),
        "risk_after": 3,
        "health": state.payload["health"],
        "batches": [],
        "market_history": {d.isoformat(): market for d in CALENDAR if d <= anchor},
    }
    monkeypatch.setattr("src.strategy.v22_slim.runtime_inputs.checkpoint", lambda: seed)
    calls = []

    class Client:
        async def _api_call(self, api, params, **kwargs):
            calls.append((api, dict(params), kwargs))
            if api == "index_daily":
                rows = [
                    {"ts_code": "932000.CSI", "trade_date": d.strftime("%Y%m%d"), "close": 100.0}
                    for d in CALENDAR[-24:-4]
                ]
            elif api == "daily":
                assert params["trade_date"] < DAY.strftime("%Y%m%d")
                rows = [
                    {
                        "ts_code": code + ".SH",
                        "trade_date": params["trade_date"],
                        "amount": 100.0,
                        "close": 20.0,
                    }
                    for code in market
                ]
            else:
                assert api == "stk_limit" and "pre_close" in kwargs["fields"]
                rows = [
                    {"ts_code": code + ".SH", "trade_date": params["trade_date"], "pre_close": 21.0}
                    for code in market
                ]
            fields = list(rows[0])
            return {"data": {"fields": fields, "items": [[r[f] for f in fields] for r in rows]}}

    def service():
        return SimpleNamespace(config=config, _scan_state=SimpleNamespace(realtime_client=Client()))

    a = await build_inputs(service(), bundle, [], [], [])
    b = await build_inputs(service(), bundle, [], [], [])
    assert a[0].snapshot_hash == b[0].snapshot_hash
    assert a[0].snapshot["v22_slim_inputs"]["risk_before"] == 3
    assert a[0].breadth_down_n == 1000  # today's pre_close, not yesterday's close
    assert all(api in {"daily", "stk_limit", "index_daily"} for api, _, _ in calls)
    seed["market_history"].pop(CALENDAR[-24].isoformat())
    with pytest.raises(ValueError, match="lacks market session"):
        await build_inputs(service(), bundle, [], [], [])


@pytest.mark.asyncio
async def test_missing_intervening_session_never_resets_reference_state(monkeypatch):
    config, state, bundle = fixture()
    seed = {
        "as_of": (DAY - timedelta(days=2)).isoformat(),
        "risk_after": 3,
        "health": state.payload["health"],
        "batches": [],
        "market_history": {d.isoformat(): {"600001": 100.0} for d in CALENDAR if d < DAY},
    }
    monkeypatch.setattr("src.strategy.v22_slim.runtime_inputs.checkpoint", lambda: seed)

    class Repo:
        async def get_entry_status(self, stream, day):
            return None

    service = SimpleNamespace(
        config=config, _scan_state=SimpleNamespace(realtime_client=object()), _repository=Repo()
    )
    with pytest.raises(ValueError, match="reference-state recovery"):
        await build_inputs(service, bundle, [], [], [])
