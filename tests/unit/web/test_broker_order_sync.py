from types import SimpleNamespace

from src.web.app import _broker_fetch_orders_once


class _Broker:
    def __init__(self, orders):
        self.orders = orders

    async def get_orders(self):
        return self.orders


class _Storage:
    is_ready = True


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
