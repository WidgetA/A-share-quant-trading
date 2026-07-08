from types import SimpleNamespace

from src.web.app import _broker_fetch_once, _broker_fetch_orders_once
from src.web.routes import create_trading_router


class _Broker:
    def __init__(self, orders, positions=None):
        self.orders = orders
        self.positions = positions or [
            SimpleNamespace(
                code="000001.SZ",
                volume=100,
                can_use_volume=100,
                avg_price=12.3,
                market_value=1230.0,
                last_price=12.3,
            )
        ]
        self.position_fetches = 0

    async def get_orders(self):
        return self.orders

    async def get_positions(self):
        self.position_fetches += 1
        return self.positions

    async def get_account(self):
        return SimpleNamespace(
            cash=1000.0, total_asset=2230.0, market_value=1230.0, account_id="acct"
        )


class _Storage:
    is_ready = True


class _NotReadyBroker(_Broker):
    async def readiness_error(self):
        return "trader not ready"


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


async def test_broker_order_sync_refreshes_positions_when_fills_change():
    orders = [{"order_id": 1, "code": "000001.SZ", "status": "FILLED"}]
    broker = _Broker(orders)
    app = SimpleNamespace(state=SimpleNamespace(broker=broker, storage=None))

    err = await _broker_fetch_orders_once(app)

    assert err is None
    assert broker.position_fetches == 1
    assert app.state.broker_positions == [
        {
            "code": "000001.SZ",
            "volume": 100,
            "can_use_volume": 100,
            "avg_price": 12.3,
            "market_value": 1230.0,
            "last_price": 12.3,
        }
    ]

    err = await _broker_fetch_orders_once(app)

    assert err is None
    assert broker.position_fetches == 1


async def test_broker_position_sync_clears_stale_holdings_when_not_ready():
    app = SimpleNamespace(
        state=SimpleNamespace(
            broker=_NotReadyBroker([]),
            broker_positions=[{"code": "old", "volume": 100}],
            available_cash=123.0,
            broker_total_asset=456.0,
            broker_account_id="old-acct",
            broker_positions_updated_at=1.0,
        )
    )

    err = await _broker_fetch_once(app)

    assert err == "broker not ready: trader not ready"
    assert app.state.broker_positions == []
    assert app.state.available_cash == 0.0
    assert app.state.broker_total_asset == 0.0
    assert app.state.broker_account_id is None
    assert app.state.broker_positions_updated_at is None


async def test_broker_order_sync_clears_stale_orders_when_not_ready():
    app = SimpleNamespace(
        state=SimpleNamespace(
            broker=_NotReadyBroker([]),
            broker_orders=[{"code": "old"}],
            broker_filled_orders_fingerprint=(("old",),),
            storage=None,
        )
    )

    err = await _broker_fetch_orders_once(app)

    assert err == "broker not ready: trader not ready"
    assert app.state.broker_orders == []
    assert app.state.broker_filled_orders_fingerprint == ()


def test_trading_router_does_not_register_legacy_orders_get():
    router = create_trading_router()

    assert not any(
        getattr(route, "path", None) == "/api/trading/orders"
        and "GET" in (getattr(route, "methods", set()) or set())
        for route in router.routes
    )
