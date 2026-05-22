from datetime import date
from types import SimpleNamespace

from src.web.app import _broker_fetch_orders_once, _filter_orders_for_beijing_date
from src.web.routes import create_trading_router


class _Broker:
    def __init__(self, orders):
        self.orders = orders

    async def get_orders(self):
        return self.orders


class _Storage:
    is_ready = True


def test_filter_orders_for_beijing_date_removes_stale_submitted_orders():
    orders = [
        {"code": "old", "submit_time": "2026-05-21T09:32:44+08:00"},
        {"code": "today", "submit_time": "2026-05-22T09:32:44+08:00"},
        {"code": "utc-today", "submit_time": "2026-05-22T01:32:44Z"},
        {"code": "unknown"},
        {"code": "bad", "submit_time": "not-a-time"},
    ]

    filtered = _filter_orders_for_beijing_date(orders, target_date=date(2026, 5, 22))

    assert [order["code"] for order in filtered] == ["today", "utc-today", "unknown", "bad"]


async def test_broker_order_sync_caches_orders_and_imports_fills(monkeypatch):
    orders = [{"order_id": 1, "code": "000001.SZ", "status": "FILLED"}]
    imported = {}

    class Store:
        def __init__(self, storage):
            imported["storage"] = storage

        async def import_filled_orders_from_list(self, rows):
            imported["orders"] = rows
            return 1, 0

    monkeypatch.setattr("src.notes.note_store.TradeNoteStore", Store)
    app = SimpleNamespace(state=SimpleNamespace(broker=_Broker(orders), storage=_Storage()))

    err = await _broker_fetch_orders_once(app)

    assert err is None
    assert app.state.broker_orders == orders
    assert imported == {"storage": app.state.storage, "orders": orders}


async def test_broker_order_sync_only_caches_when_storage_not_ready(monkeypatch):
    orders = [{"order_id": 1, "code": "000001.SZ", "status": "FILLED"}]

    class Store:
        def __init__(self, storage):
            raise AssertionError("TradeNoteStore should not be created")

    monkeypatch.setattr("src.notes.note_store.TradeNoteStore", Store)
    app = SimpleNamespace(state=SimpleNamespace(broker=_Broker(orders), storage=None))

    err = await _broker_fetch_orders_once(app)

    assert err is None
    assert app.state.broker_orders == orders


def test_trading_router_does_not_register_legacy_orders_get():
    router = create_trading_router()

    assert not any(
        getattr(route, "path", None) == "/api/trading/orders"
        and "GET" in (getattr(route, "methods", set()) or set())
        for route in router.routes
    )
