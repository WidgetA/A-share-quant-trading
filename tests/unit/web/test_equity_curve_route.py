# === MODULE PURPOSE ===
# Route tests for TRD-001 account overview endpoints:
# - GET /api/trading/equity-curve — snapshots + weekly returns + live current block
# - GET /api/trading/holdings — enriched cost/last_price/market_value/pnl fields

from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.web.routes import create_trading_router

_KEY = {"X-API-Key": "secret-key"}


def _client(state: dict, monkeypatch) -> TestClient:
    monkeypatch.setattr("src.common.config.get_trading_api_key", lambda: "secret-key")
    app = FastAPI()
    for k, v in state.items():
        setattr(app.state, k, v)
    app.include_router(create_trading_router())
    return TestClient(app)


class _FakeDB:
    def __init__(self, rows):
        self._rows = rows

    async def fetch(self, sql: str):
        return self._rows


def test_equity_curve_returns_snapshots_weekly_and_today_pnl(monkeypatch):
    tz = ZoneInfo("Asia/Shanghai")
    today = datetime.now(tz).strftime("%Y-%m-%d")
    prev = (datetime.now(tz) - timedelta(days=1)).strftime("%Y-%m-%d")
    # 库返回 DESC(与 list_snapshots 的 ORDER BY 一致)
    rows = [
        {"trade_date": today, "total_asset": 105000.0, "cash": 5000.0, "market_value": 100000.0},
        {"trade_date": prev, "total_asset": 100000.0, "cash": 100000.0, "market_value": 0.0},
    ]
    client = _client(
        {
            "storage": SimpleNamespace(db=_FakeDB(rows)),
            "broker_account_id": "acct",
            "broker_total_asset": 105000.0,
            "available_cash": 5000.0,
            "broker_market_value": 100000.0,
        },
        monkeypatch,
    )

    resp = client.get("/api/trading/equity-curve", headers=_KEY)

    assert resp.status_code == 200
    data = resp.json()
    assert [s["date"] for s in data["snapshots"]] == [prev, today]  # 升序
    assert isinstance(data["weekly"], list) and data["weekly"]
    cur = data["current"]
    assert cur["total_asset"] == 105000.0
    assert cur["prev_close_asset"] == 100000.0
    assert cur["today_pnl"] == 5000.0
    assert cur["today_pnl_pct"] == 5.0


def test_equity_curve_503_when_storage_missing(monkeypatch):
    client = _client({"storage": None}, monkeypatch)
    resp = client.get("/api/trading/equity-curve", headers=_KEY)
    assert resp.status_code == 503


def test_holdings_enriched_with_cost_and_pnl(monkeypatch):
    client = _client(
        {
            "broker_positions": [
                {
                    "code": "000001.SZ",
                    "volume": 100,
                    "can_use_volume": 100,
                    "avg_price": 10.0,
                    "market_value": 1100.0,
                    "last_price": 11.0,
                },
                # 老缓存行(部署交替期):没有 last_price 键也不崩
                {"code": "600000.SH", "volume": 200, "avg_price": None, "market_value": None},
            ],
            "storage": None,
        },
        monkeypatch,
    )

    resp = client.get("/api/trading/holdings", headers=_KEY)

    assert resp.status_code == 200
    h = resp.json()["holdings"]
    assert h[0]["quantity"] == 100
    assert h[0]["avg_price"] == 10.0
    assert h[0]["last_price"] == 11.0
    assert h[0]["pnl"] == 100.0  # 1100 - 10*100
    assert h[0]["pnl_pct"] == 10.0
    assert h[1]["pnl"] is None and h[1]["pnl_pct"] is None and h[1]["last_price"] is None
