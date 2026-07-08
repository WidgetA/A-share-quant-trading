# === MODULE PURPOSE ===
# Route tests for TRD-001 account overview endpoints:
# - GET /api/trading/equity-curve — snapshots + weekly returns + live current block,
#   0.22.2 起含手动基准点 + 账本重建填空档 + 校准差警示
# - POST/DELETE /api/trading/equity-baseline — 手动校准基准点
# - GET /api/trading/holdings — enriched cost/last_price/market_value/pnl fields

from __future__ import annotations

import calendar
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.web.routes import create_trading_router

_KEY = {"X-API-Key": "secret-key"}
_TZ = ZoneInfo("Asia/Shanghai")


def _client(state: dict, monkeypatch) -> TestClient:
    monkeypatch.setattr("src.common.config.get_trading_api_key", lambda: "secret-key")
    app = FastAPI()
    for k, v in state.items():
        setattr(app.state, k, v)
    app.include_router(create_trading_router())
    return TestClient(app)


def _bj(days_ago: int) -> str:
    return (datetime.now(_TZ) - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def _snap_row(d: str, asset: float, source: str | None = None) -> dict:
    return {
        "trade_date": d,
        "total_asset": asset,
        "cash": None,
        "market_value": None,
        "source": source,
    }


class _FakeDB:
    """快照查询回 rows;trade_notes 查询回空;fetchrow 回单行(baseline 端点用)。"""

    def __init__(self, rows, row=None):
        self._rows = rows
        self._row = row
        self.executed: list[str] = []

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)

    async def fetch(self, sql: str):
        if "trade_notes" in sql:
            return []
        return self._rows

    async def fetchrow(self, sql: str):
        return self._row


def _day_ts_ms(date_str: str) -> int:
    y, m, d = (int(p) for p in date_str.split("-"))
    return calendar.timegm(datetime(y, m, d).timetuple()) * 1000


class _FakeStorage:
    """带日线的 fake storage(重建空档用)。"""

    def __init__(self, rows, daily: dict[str, list[tuple[str, float]]] | None = None):
        self.db = _FakeDB(rows)
        self._daily = daily or {}

    async def get_daily_for_code(self, code: str, start: str, end: str):
        return [
            {"ts": _day_ts_ms(d), "close_price": c}
            for d, c in self._daily.get(code, [])
            if start <= d <= end
        ]


def test_equity_curve_returns_snapshots_weekly_and_today_pnl(monkeypatch):
    today, prev = _bj(0), _bj(1)
    # 库返回 DESC(与 list_snapshots 的 ORDER BY 一致)
    rows = [
        _snap_row(today, 105000.0),
        _snap_row(prev, 100000.0),
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
    assert "reconstruction" in data
    cur = data["current"]
    assert cur["total_asset"] == 105000.0
    assert cur["frozen_cash"] == 0.0
    assert cur["prev_close_asset"] == 100000.0
    assert cur["today_pnl"] == 5000.0
    assert cur["today_pnl_pct"] == 5.0


def test_equity_curve_manual_baseline_gap_filled_by_ledger(monkeypatch):
    """手动基准(6天前) + broker 快照(1天前):中间空档由账本重建填,来源打标。

    持仓 1000 股,无成交,收盘价 10.5 → 10.2 → 10.1 → 10.0:
    锚点(1天前)=100000 → 倒推 3天前=100100, 5天前=100200, 6天前=100500(=校准值,无警示)
    """
    t0, t1, t2, t3, today = _bj(6), _bj(5), _bj(3), _bj(1), _bj(0)
    rows = [
        _snap_row(t3, 100000.0),  # broker(source NULL 视同 broker)
        _snap_row(t0, 100500.0, source="manual"),
    ]
    storage = _FakeStorage(rows, daily={"000001": [(t0, 10.5), (t1, 10.2), (t2, 10.1), (t3, 10.0)]})
    client = _client(
        {
            "storage": storage,
            "broker_account_id": "acct",
            "broker_total_asset": 99900.0,
            "broker_positions": [{"code": "000001.SZ", "volume": 1000}],
        },
        monkeypatch,
    )

    data = client.get("/api/trading/equity-curve", headers=_KEY).json()

    series = data["snapshots"]
    assert [s["date"] for s in series] == [t0, t1, t2, t3, today]
    assert [s["total_asset"] for s in series] == [100500.0, 100200.0, 100100.0, 100000.0, 99900.0]
    assert [s["source"] for s in series] == ["manual", "recon", "recon", "broker", "live"]
    recon = data["reconstruction"]
    assert recon["window_start"] == t0
    assert recon["anchor_date"] == t3
    assert recon["truncated_at"] is None
    assert recon["warnings"] == []


def test_equity_curve_warns_when_ledger_disagrees_with_baseline(monkeypatch):
    """账本推算 t0=100500,校准值填 100600 → 差 -100 警示;曲线以校准值为准。"""
    t0, t1, t3 = _bj(6), _bj(5), _bj(1)
    rows = [
        _snap_row(t3, 100000.0),
        _snap_row(t0, 100600.0, source="manual"),
    ]
    storage = _FakeStorage(rows, daily={"000001": [(t0, 10.5), (t1, 10.2), (t3, 10.0)]})
    client = _client(
        {
            "storage": storage,
            "broker_account_id": "acct",
            "broker_total_asset": 0.0,
            "broker_positions": [{"code": "000001.SZ", "volume": 1000}],
        },
        monkeypatch,
    )

    data = client.get("/api/trading/equity-curve", headers=_KEY).json()

    assert any("与手动校准值差" in w for w in data["reconstruction"]["warnings"])
    t0_point = next(s for s in data["snapshots"] if s["date"] == t0)
    assert t0_point["total_asset"] == 100600.0  # 校准值为准
    assert t0_point["source"] == "manual"


def test_equity_curve_503_when_storage_missing(monkeypatch):
    client = _client({"storage": None}, monkeypatch)
    resp = client.get("/api/trading/equity-curve", headers=_KEY)
    assert resp.status_code == 503


# ---------- 手动校准基准点端点 ----------


def _baseline_client(monkeypatch, row=None, account_id="acct"):
    db = _FakeDB(rows=[], row=row)
    return (
        _client(
            {"storage": SimpleNamespace(db=db), "broker_account_id": account_id},
            monkeypatch,
        ),
        db,
    )


def test_set_baseline_writes_manual_snapshot(monkeypatch):
    client, db = _baseline_client(monkeypatch)
    resp = client.post(
        "/api/trading/equity-baseline",
        headers=_KEY,
        json={"date": _bj(6), "total_asset": 1002872.83},
    )
    assert resp.status_code == 200
    insert_sqls = [s for s in db.executed if "INSERT INTO account_equity_snapshot" in s]
    assert len(insert_sqls) == 1
    assert "'manual'" in insert_sqls[0]
    assert "1002872.83" in insert_sqls[0]


def test_set_baseline_surfaces_db_error_as_detail(monkeypatch):
    """数据库炸了 → 500 且 detail 带真实异常原文,绝不裸 'Internal Server Error'。"""

    class _BoomDB:
        async def fetchrow(self, sql: str):
            raise RuntimeError("column source not found")

    client = _client(
        {"storage": SimpleNamespace(db=_BoomDB()), "broker_account_id": "acct"},
        monkeypatch,
    )
    resp = client.post(
        "/api/trading/equity-baseline",
        headers=_KEY,
        json={"date": _bj(6), "total_asset": 100.0},
    )
    assert resp.status_code == 500
    assert "column source not found" in resp.json()["detail"]


def test_set_baseline_rejects_today_and_future_and_nonpositive(monkeypatch):
    client, db = _baseline_client(monkeypatch)
    assert (
        client.post(
            "/api/trading/equity-baseline",
            headers=_KEY,
            json={"date": _bj(0), "total_asset": 1.0},
        ).status_code
        == 400
    )
    assert (
        client.post(
            "/api/trading/equity-baseline",
            headers=_KEY,
            json={"date": _bj(3), "total_asset": 0},
        ).status_code
        == 400
    )
    assert not any("INSERT" in s for s in db.executed)


def test_set_baseline_refuses_overwriting_broker_snapshot(monkeypatch):
    d = _bj(1)
    client, db = _baseline_client(
        monkeypatch,
        row={
            "trade_date": d,
            "total_asset": 100.0,
            "cash": None,
            "market_value": None,
            "source": "broker",
        },
    )
    resp = client.post(
        "/api/trading/equity-baseline", headers=_KEY, json={"date": d, "total_asset": 200.0}
    )
    assert resp.status_code == 409
    assert not any("INSERT" in s for s in db.executed)


def test_delete_baseline_manual_only(monkeypatch):
    d = _bj(6)
    # manual → 删除成功
    client, db = _baseline_client(
        monkeypatch,
        row={
            "trade_date": d,
            "total_asset": 100.0,
            "cash": None,
            "market_value": None,
            "source": "manual",
        },
    )
    assert client.delete(f"/api/trading/equity-baseline/{d}", headers=_KEY).status_code == 200
    assert any("DELETE FROM account_equity_snapshot" in s for s in db.executed)
    # broker → 400
    client2, db2 = _baseline_client(
        monkeypatch,
        row={
            "trade_date": d,
            "total_asset": 100.0,
            "cash": None,
            "market_value": None,
            "source": "broker",
        },
    )
    assert client2.delete(f"/api/trading/equity-baseline/{d}", headers=_KEY).status_code == 400
    # 不存在 → 404
    client3, _ = _baseline_client(monkeypatch, row=None)
    assert client3.delete(f"/api/trading/equity-baseline/{d}", headers=_KEY).status_code == 404


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
