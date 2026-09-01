from __future__ import annotations

import gzip
import json
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.data.clients.mews_snapshot import (
    LocalMewsSnapshotCalculator,
    MewsSnapshotSourceError,
)

TZ = ZoneInfo("Asia/Shanghai")
PROJECT_ROOT = Path(__file__).resolve().parents[4]


class _Repository:
    def __init__(self, state: dict[str, Any] | None = None) -> None:
        self.state = state
        self.saved: list[dict[str, Any]] = []

    async def load_mews_calculation_state(self):
        return self.state

    async def save_mews_calculation_state(self, state):
        self.state = json.loads(json.dumps(state))
        self.saved.append(self.state)
        return "a" * 64


class _RawClient:
    def __init__(self, *, missing_exchange: bool = False) -> None:
        self.calls: list[str] = []
        self.started = False
        self.missing_exchange = missing_exchange

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def query(self, api_name, params, fields, *, allow_empty=False):
        assert self.started
        self.calls.append(api_name)
        if api_name == "trade_cal":
            return [
                {
                    "exchange": params["exchange"],
                    "cal_date": "20260831",
                    "is_open": "1",
                    "pretrade_date": "20260828",
                }
            ]
        if api_name == "stock_basic":
            if params["list_status"] in {"P", "G"}:
                assert allow_empty
                return []
            if params["list_status"] == "D":
                return [
                    {
                        "ts_code": f"900001.{'SH' if params['exchange'] == 'SSE' else 'SZ'}",
                        "symbol": "900001",
                        "name": "old",
                        "market": "\u4e3b\u677f",
                        "exchange": params["exchange"],
                        "list_status": "D",
                        "list_date": "20000101",
                        "delist_date": "20010101",
                    }
                ]
            if params["exchange"] == "SZSE":
                return [
                    {
                        "ts_code": "000001.SZ",
                        "symbol": "000001",
                        "name": "Ping An Bank",
                        "market": "\u4e3b\u677f",
                        "exchange": "SZSE",
                        "list_status": "L",
                        "list_date": "19910403",
                        "delist_date": None,
                    }
                ]
            return [
                {
                    "ts_code": "600001.SH",
                    "symbol": "600001",
                    "name": "SSE sample",
                    "market": "\u4e3b\u677f",
                    "exchange": "SSE",
                    "list_status": "L",
                    "list_date": "20000101",
                    "delist_date": None,
                }
            ]
        if api_name == "margin":
            rows = [
                {"exchange_id": "SSE", "rzye": 60, "rzmre": 2, "rzche": 3},
                {"exchange_id": "SZSE", "rzye": 60, "rzmre": 2, "rzche": 3},
            ]
            return rows[:1] if self.missing_exchange else rows
        if api_name == "margin_detail":
            return [
                {"ts_code": "000001.SZ", "rzye": 50, "rzmre": 1, "rzche": 3},
                {"ts_code": "600001.SH", "rzye": 50, "rzmre": 1, "rzche": 3},
            ]
        if api_name == "daily_basic":
            return [
                {"ts_code": "000001.SZ", "close": 10, "free_share": 100},
                {"ts_code": "600001.SH", "close": 10, "free_share": 100},
            ]
        raise AssertionError(f"unexpected raw API {api_name}")


def _state() -> dict[str, Any]:
    history = []
    for index in range(550):
        stock_balance = 95.0 + (index % 17) * 0.5
        market_balance = stock_balance / (5.0 / 6.0)
        buy = 3.0 + (index % 9) * 0.1
        repay = 3.2 + ((index * 7) % 11) * 0.1
        history.append(
            {
                "trade_date": f"2024-01-{(index % 28) + 1:02d}",
                "market_total_margin_balance": market_balance,
                "market_total_financing_buy_amount": buy * 1.2,
                "market_total_financing_repayment_amount": repay * 1.2,
                "ordinary_a_share_margin_balance": stock_balance,
                "ordinary_a_share_financing_buy_amount": buy,
                "ordinary_a_share_financing_repayment_amount": repay,
                "ordinary_a_share_margin_coverage": 5.0 / 6.0,
                "ffmv_stock": 20_000_000.0 + index * 10_000,
                "ffmv_coverage": 1.0,
                "nib_breadth_v2": 40.0,
                "nib_magnitude_v2": 30.0,
                "deleveraging_breadth": 55.0,
                "data_status": "OK",
                "mews_v2_score": 50.0 + (index % 13),
                "exhaustion_path": 48.0,
                "persistent_deleveraging_path": 45.0,
                "net_outflow_level_score": 60.0,
                "risk_state_v2": "WATCH",
            }
        )
    history[-1]["trade_date"] = "2026-08-28"
    security_state = {
        "current_balance": 50.0,
        "ema_fast_state": -0.01,
        "ema_fast_old_weight": 1.0,
        "ema_slow_state": -0.005,
        "ema_slow_old_weight": 1.0,
        "valid_history": [True] * 25,
        "net_flow_history": [-1.0] * 4,
        "impulse_history": [(-0.02 + (index % 7) * 0.002) for index in range(59)],
    }
    return {
        "schema": "v20-mews-incremental-state/v1",
        "model_version": "mews_v2",
        "state_date": "2026-08-28",
        "market_history": history,
        "security_states": {
            "000001.SZ": json.loads(json.dumps(security_state)),
            "600001.SH": json.loads(json.dumps(security_state)),
        },
        "risk_state": "WATCH",
        "clear_streak": 0,
    }


def _calculator(repository, raw):
    return LocalMewsSnapshotCalculator(
        "raw-tushare-token",
        repository,
        bootstrap_path=PROJECT_ROOT / "data" / "v20_mews_bootstrap.json.gz",
        client_factory=lambda: raw,
        clock=lambda: datetime(2026, 9, 1, 9, 15, tzinfo=TZ),
    )


def test_frozen_compact_bootstrap_is_packaged_and_valid() -> None:
    path = PROJECT_ROOT / "data" / "v20_mews_bootstrap.json.gz"
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        state = json.load(handle)

    assert state["schema"] == "v20-mews-incremental-state/v1"
    assert state["model_version"] == "mews_v2"
    assert state["state_date"] == "2026-08-06"
    assert len(state["market_history"]) == 550
    assert len(state["security_states"]) > 4_000


def test_local_formula_reproduces_frozen_canonical_latest_metric() -> None:
    path = PROJECT_ROOT / "data" / "v20_mews_bootstrap.json.gz"
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        state = json.load(handle)
    expected = deepcopy(state["market_history"][-1])

    LocalMewsSnapshotCalculator._calculate_latest(state)

    actual = state["market_history"][-1]
    assert actual["exhaustion_path"] == pytest.approx(expected["exhaustion_path"], abs=1e-12)
    assert actual["persistent_deleveraging_path"] == pytest.approx(
        expected["persistent_deleveraging_path"], abs=1e-12
    )
    assert actual["mews_v2_score"] == pytest.approx(expected["mews_v2_score"], abs=1e-12)
    assert actual["net_outflow_level_score"] == pytest.approx(
        expected["net_outflow_level_score"], abs=1e-12
    )
    assert actual["risk_state_v2"] == expected["risk_state_v2"]


async def test_missing_mews_is_calculated_from_raw_tushare_and_checkpointed() -> None:
    repository = _Repository(_state())
    raw = _RawClient()

    snapshot = await _calculator(repository, raw).fetch_snapshot(
        source_trade_date=date(2026, 8, 31),
        availability_date=date(2026, 9, 1),
    )
    retry = await _calculator(repository, raw).fetch_snapshot(
        source_trade_date=date(2026, 8, 31),
        availability_date=date(2026, 9, 1),
    )

    assert raw.calls.count("trade_cal") == 2
    assert raw.calls.count("margin") == 1
    assert raw.calls.count("margin_detail") == 1
    assert raw.calls.count("daily_basic") == 1
    assert "margin-risk-curve" not in raw.calls
    assert len(repository.saved) == 1
    assert repository.saved[0]["state_date"] == "2026-08-31"
    assert snapshot["model_version"] == "mews_v2"
    assert snapshot["source_trade_date"] == "2026-08-31"
    assert snapshot["evidence"]["profile"] == "LOCAL_TUSHARE_MEWS_V2_0910_V1"
    assert 0 <= snapshot["evidence"]["mews"] <= 100
    assert retry == snapshot


async def test_calculated_state_is_reused_without_any_raw_refetch() -> None:
    state = _state()
    state["state_date"] = "2026-08-31"
    state["market_history"][-1]["trade_date"] = "2026-08-31"
    repository = _Repository(state)
    raw = _RawClient()

    snapshot = await _calculator(repository, raw).fetch_snapshot(
        source_trade_date=date(2026, 8, 31),
        availability_date=date(2026, 9, 1),
    )

    assert raw.calls == []
    assert snapshot["source_trade_date"] == "2026-08-31"


async def test_calculation_has_no_wall_clock_gate() -> None:
    state = _state()
    state["state_date"] = "2026-08-31"
    state["market_history"][-1]["trade_date"] = "2026-08-31"
    state["calculated_at"] = "2026-09-01T08:00:00+08:00"
    repository = _Repository(state)
    raw = _RawClient()

    snapshot = await _calculator(repository, raw).fetch_snapshot(
        source_trade_date=date(2026, 8, 31),
        availability_date=date(2026, 9, 1),
    )

    assert raw.calls == []
    assert snapshot["generated_at"] == "2026-09-01T08:00:00+08:00"


async def test_same_source_facts_yield_identical_value_level_and_hash_at_any_wall_clock() -> None:
    def build(wall: datetime) -> LocalMewsSnapshotCalculator:
        return LocalMewsSnapshotCalculator(
            "raw-tushare-token",
            _Repository(_state()),
            bootstrap_path=PROJECT_ROOT / "data" / "v20_mews_bootstrap.json.gz",
            client_factory=lambda: _RawClient(),
            clock=lambda: wall,
        )

    early = await build(datetime(2026, 9, 1, 9, 15, tzinfo=TZ)).fetch_snapshot(
        source_trade_date=date(2026, 8, 31),
        availability_date=date(2026, 9, 1),
    )
    late = await build(datetime(2026, 9, 1, 14, 4, tzinfo=TZ)).fetch_snapshot(
        source_trade_date=date(2026, 8, 31),
        availability_date=date(2026, 9, 1),
    )

    assert early["snapshot_id"] == late["snapshot_id"]
    assert early["data_version"] == late["data_version"]
    assert early["fast_state"] == late["fast_state"]
    assert early["evidence"] == late["evidence"]
    assert early["generated_at"] != late["generated_at"]


async def test_incomplete_raw_exchange_data_never_becomes_a_safe_snapshot() -> None:
    repository = _Repository(_state())
    raw = _RawClient(missing_exchange=True)

    with pytest.raises(MewsSnapshotSourceError, match="missing SSE or SZSE"):
        await _calculator(repository, raw).fetch_snapshot(
            source_trade_date=date(2026, 8, 31),
            availability_date=date(2026, 9, 1),
        )

    assert repository.saved == []
