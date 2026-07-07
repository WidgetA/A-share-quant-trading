# TRD-001 持仓明细成本价三级来源:
#   broker avg_price(>0) → trade_notes 买入记录加权摊算 → null(前端 --)。
# 起因: QMT 部分券商通道 avg_price 回 0,0 被当真成本把亏损仓显示成
# "+整个市值"的假盈利(002851 实测)。

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.notes.note_store import NoteEvent, TradeNoteStore
from src.web.routes import create_trading_router


class _StubMapper:
    def get_stock_name(self, code):
        return None


def _buy(ts, price, qty):
    return _event(ts, "买入", "buy", price, qty)


def _sell(ts, price, qty):
    return _event(ts, "卖出", "sell", price, qty)


def _event(ts, event_type, side, price, qty):
    return NoteEvent(
        ts=ts,
        code="002851",
        event_id=f"e{ts.timestamp()}",
        event_type=event_type,
        source="broker",
        title="",
        price=price,
        qty=qty,
        side=side,
        content="",
        content_external="",
        author="system",
        deleted=False,
        commission=None,
        transfer_fee=None,
        stamp_tax=None,
        dividend=None,
        realized_pnl=None,
        repo_income=None,
    )


@pytest.fixture
def make_client(monkeypatch):
    """Returns a factory: make_client(positions, note_events) -> TestClient."""
    monkeypatch.setattr("src.common.config.get_trading_api_key", lambda: None)
    monkeypatch.setattr("src.data.sources.local_concept_mapper.LocalConceptMapper", _StubMapper)

    def _make(positions, note_events, storage_ready=True):
        captured = {}

        async def fake_list_events(self, code):
            captured["code"] = code
            return note_events

        monkeypatch.setattr(TradeNoteStore, "list_events", fake_list_events)
        app = FastAPI()
        app.include_router(create_trading_router())
        app.state.broker_positions = positions
        app.state.storage = SimpleNamespace(db=object()) if storage_ready else None
        client = TestClient(app)
        client.captured = captured
        return client

    return _make


_TS0 = datetime(2026, 7, 1, 1, 40, tzinfo=timezone.utc)


def _pos(avg_price, volume=1200, market_value=179460.0, last_price=None):
    return {
        "code": "002851.SZ",
        "volume": volume,
        "can_use_volume": volume,
        "avg_price": avg_price,
        "market_value": market_value,
        "last_price": last_price,
    }


def test_broker_avg_price_used_when_valid(make_client):
    client = make_client([_pos(avg_price=150.0)], [])
    h = client.get("/api/trading/holdings").json()["holdings"][0]
    assert h["avg_price"] == 150.0
    assert h["pnl"] == pytest.approx(179460.0 - 150.0 * 1200)
    # notes were not consulted
    assert "code" not in client.captured


def test_zero_avg_price_falls_back_to_notes_weighted_cost(make_client):
    # 时间升序: 买2000@160 → 卖800 → 买400@155;持仓 1200。
    # 倒着摊: 400@155 + 800@160 → 成本 (400*155 + 800*160)/1200 = 158.333
    # 成本 190,000 > 市值 179,460 → 浮亏,盈亏必须为负。
    events = [
        _buy(_TS0, 160.0, 2000),
        _sell(_TS0 + timedelta(days=1), 150.0, 800),
        _buy(_TS0 + timedelta(days=2), 155.0, 400),
    ]
    client = make_client([_pos(avg_price=0.0)], events)
    h = client.get("/api/trading/holdings").json()["holdings"][0]
    assert client.captured["code"] == "002851"  # bare code, no .SZ suffix
    assert h["avg_price"] == pytest.approx(158.333)
    assert h["pnl"] == pytest.approx(179460.0 - 158.333 * 1200, abs=1.0)
    assert h["pnl_pct"] is not None
    # 亏损必须显示为负——这正是 0 成本假盈利 bug 的反例
    assert h["pnl"] < 0


def test_no_buy_records_shows_unknown_not_fake_profit(make_client):
    client = make_client([_pos(avg_price=0.0)], [])
    h = client.get("/api/trading/holdings").json()["holdings"][0]
    assert h["avg_price"] is None
    assert h["pnl"] is None
    assert h["pnl_pct"] is None


def test_last_price_derived_from_market_value(make_client):
    client = make_client([_pos(avg_price=0.0)], [])
    h = client.get("/api/trading/holdings").json()["holdings"][0]
    assert h["last_price"] == pytest.approx(149.55)


def test_storage_down_degrades_to_unknown_cost(make_client):
    client = make_client([_pos(avg_price=0.0)], [], storage_ready=False)
    h = client.get("/api/trading/holdings").json()["holdings"][0]
    assert h["avg_price"] is None
    assert h["pnl"] is None
