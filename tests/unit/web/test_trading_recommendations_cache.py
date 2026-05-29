from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.web.ml_routes import create_ml_router
from src.web.routes import create_trading_router


def test_today_recommendations_read_background_monitor_cache(monkeypatch):
    today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
    recs = [{"stock_code": "000001.SZ", "stock_name": "平安银行", "rank": 1}]

    monkeypatch.setattr("src.common.config.get_trading_api_key", lambda: "secret-key")
    monkeypatch.setattr("src.common.config.get_recommendations_enabled", lambda: True)

    async def fail_compute(*args, **kwargs):
        raise AssertionError("today recommendations must not compute on GET")

    monkeypatch.setattr("src.web.routes._compute_ml_scan", fail_compute)

    app = FastAPI()
    app.state.storage = None
    app.state.momentum_monitor_state = {
        "today_recommendations_date": today,
        "today_recommendations": recs,
        "last_scan_message": "ok",
    }
    app.include_router(create_trading_router())

    response = TestClient(app).get(
        f"/api/trading/recommendations?date={today}",
        headers={"X-API-Key": "secret-key"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "date": today,
        "recommendations": recs,
        "source": "background_monitor",
    }


async def test_broker_position_fetch_records_cache_timestamp():
    from src.web.app import _broker_fetch_once

    class Broker:
        async def get_positions(self):
            return [
                SimpleNamespace(
                    code="000001.SZ",
                    volume=100,
                    can_use_volume=100,
                    avg_price=12.3,
                    market_value=1230,
                )
            ]

        async def get_account(self):
            return SimpleNamespace(cash=1000, total_asset=2230, account_id="acct")

    app = SimpleNamespace(state=SimpleNamespace(broker=Broker()))

    err = await _broker_fetch_once(app)

    assert err is None
    assert app.state.broker_positions_updated_at > 0


async def test_ml_resource_init_does_not_start_signal_scheduler(monkeypatch):
    class Client:
        async def start(self):
            return None

        async def stop(self):
            return None

    monkeypatch.setattr("src.common.config.get_tushare_token", lambda: "token")
    monkeypatch.setattr("src.web.ml_routes._get_trade_calendar", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        "src.data.clients.tushare_realtime.TushareRealtimeClient", lambda token: Client()
    )

    router = create_ml_router()
    state = await router._ml_init()

    assert state["initialized"] is True
    assert "scheduler_task" not in state

    await router._ml_cleanup()


def test_ml_ping_is_pure_status_read(monkeypatch):
    monkeypatch.setattr("src.common.config.get_stock_api_key", lambda: "secret-key")

    app = FastAPI()
    app.state.broker_positions = []
    app.include_router(create_ml_router())

    response = TestClient(app).get("/api/stock/ping", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["initialized"] is False
