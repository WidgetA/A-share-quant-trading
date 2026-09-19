import asyncio
import hashlib
import hmac
import json
import time
from dataclasses import asdict
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from src.trading.broker_client import AccountInfo, BrokerError, OrderRecord
from src.trading.broker_switch import BrokerSwitch
from src.trading.channel_store import ChannelStore
from src.trading.qmt_http_client import QmtHttpClient, business_id, order_intent, to_order
from src.web.app import _broker_fetch_once
from src.web.broker_channel_routes import create_broker_channel_router
from src.web.routes import create_trading_router

SPEC = {
    "backend": "qmt",
    "url": "https://qmt.example.invalid",
    "instance_id": "test-instance",
    "key_id": "ingress",
    "secret": "test-signing-secret",
    "ca_file": None,
}


@pytest.mark.parametrize("state", [None, 123])
def test_missing_or_invalid_qmt_state_remains_unknown(state):
    intent = order_intent("601988.SH", "BUY", 100, "MARKET", None)
    result = to_order({"state": state}, intent, "test-order")
    assert result.status == "UNKNOWN"


class Mini:
    def __init__(self):
        self.orders = []
        self.cancels = []

    async def readiness_error(self):
        return None

    async def stop(self):
        pass

    async def place_order(self, **intent):
        self.orders.append(intent)
        return OrderRecord(
            321,
            None,
            intent["code"],
            intent["side"],
            intent["price"] or 0,
            intent["qty"],
            0,
            0,
            "ACCEPTED",
        )

    async def get_orders(self):
        return [
            {"order_id": 321, "status": "ACCEPTED", "code": "601988.SH", "side": "BUY", "qty": 100}
        ]

    async def cancel_order(self, order_id):
        self.cancels.append(order_id)
        return {"status": "CANCELLED"}


class Gateway:
    """HTTP boundary fake verifies every signed byte; it never calls a broker."""

    def __init__(self):
        self.requests = []
        self.orders = {}
        self.cancels = {}
        self.nonces = set()
        self.lose_response = False
        self.lose_cancel = False
        self.unavailable_query = False
        self.snapshot_age = 0
        self.session = "live-session"
        self.observed = True
        self.snapshot_missing = False
        self.post_state = "RECEIVED"

    def __call__(self, req):
        nonce = req.headers["X-Qmt-Nonce"]
        assert nonce not in self.nonces
        self.nonces.add(nonce)
        assert len(nonce) == 32
        assert req.headers.get("X-API-Key") is None
        content_type = "application/json" if req.method == "POST" else ""
        assert req.headers.get("Content-Type", "") == content_type
        message = "\n".join(
            (
                "QMT-HMAC-SHA256",
                SPEC["instance_id"],
                req.method,
                req.url.raw_path.decode(),
                content_type,
                req.headers["X-Qmt-Timestamp"],
                nonce,
                hashlib.sha256(req.content).hexdigest(),
            )
        )
        signature = hmac.new(SPEC["secret"].encode(), message.encode(), hashlib.sha256).hexdigest()
        assert req.headers["Authorization"] == f"QMT-HMAC-SHA256 ingress:{signature}"
        path = req.url.path
        self.requests.append((req.method, path, req.content))
        if path == "/v1/agent":
            data = {"runtime": {"trading_session": "live-session"}}
        elif path == "/v1/trading/status":
            data = {"trade_ready": False, "error_code": "TRADING_READ_ONLY"}
        elif path == "/v1/trading/execution":
            data = {"production_authorized": False}
        elif path == "/v1/trading/snapshot":
            data = {
                "snapshot": None
                if self.snapshot_missing
                else {
                    "entry_session": self.session,
                    "updated_at": time.time() - self.snapshot_age,
                    "account_observed": self.observed,
                    "reconciled": False,
                    "data": {
                        "account": {
                            "available_cash": "1000.50",
                            "total_assets": "2000.50",
                            "market_value": "1000",
                            "net_assets": "2000.50",
                        },
                        "positions": [
                            {
                                "symbol": "601988.SH",
                                "quantity": 100,
                                "available": 0,
                                "market_value": "1000",
                            }
                        ],
                        "orders": [],
                        "trades": [],
                    },
                }
            }
        elif path == "/v1/trading/orders" and req.method == "POST":
            body = json.loads(req.content)
            assert set(body) <= {
                "order_id",
                "symbol",
                "side",
                "quantity",
                "order_type",
                "limit_price",
            }
            key = body["order_id"]
            assert key not in self.orders, "The client sent a duplicate POST"
            self.orders[key] = {
                "order_id": key,
                "intent": body,
                "state": self.post_state,
                "filled_quantity": None,
                "filled_average_price": None,
            }
            if self.lose_response:
                raise httpx.ReadTimeout("sensitive upstream content must not escape", request=req)
            return httpx.Response(202, json=self.orders[key])
        elif path.startswith("/v1/trading/orders/"):
            key = path.rsplit("/", 1)[-1]
            if self.unavailable_query or key not in self.orders:
                return httpx.Response(404, json={"error_code": "NOT_FOUND"})
            data = self.orders[key]
        elif path == "/v1/trading/cancels":
            body = json.loads(req.content)
            assert set(body) == {"cancel_id", "order_id"}
            assert body["cancel_id"] not in self.cancels
            data = {"state": "RECEIVED", "command": body}
            self.cancels[body["cancel_id"]] = data
            if self.lose_cancel:
                raise httpx.ReadTimeout("lost", request=req)
            return httpx.Response(202, json=data)
        elif path.startswith("/v1/trading/commands/"):
            data = self.cancels[path.rsplit("/", 1)[-1]]
        else:
            raise AssertionError((req.method, path))
        return httpx.Response(200, json=data)


@pytest.fixture
def setup(tmp_path):
    store = ChannelStore(tmp_path / "channels.sqlite3")
    mini_route = store.save_profile(
        {"backend": "miniqmt", "url": "http://mini.example", "api_key": "mini-key"}
    )
    qmt_route = store.save_profile(SPEC)
    gateway = Gateway()
    qmt = QmtHttpClient(SPEC)
    qmt._client = httpx.AsyncClient(base_url=SPEC["url"], transport=httpx.MockTransport(gateway))
    switch = BrokerSwitch(store)
    mini = Mini()
    switch._clients = {mini_route: mini, qmt_route: qmt}
    return SimpleNamespace(
        store=store,
        switch=switch,
        mini=mini,
        gateway=gateway,
        mini_route=mini_route,
        qmt_route=qmt_route,
        qmt=qmt,
    )


async def test_select_route_sign_bytes_and_preserve_choice_after_restart(setup):
    s = setup
    await s.switch.select("qmt")
    order = await s.switch.place_order("601988", "BUY", 100, "LIMIT", 6.5, request_id="buy-1")
    assert order.status == "RECEIVED"
    assert order.traded_qty is None
    body = json.loads(next(raw for method, _, raw in s.gateway.requests if method == "POST"))
    assert body == {
        "order_id": business_id("buy-1"),
        "symbol": "601988.SH",
        "side": "buy",
        "quantity": 100,
        "order_type": "limit",
        "limit_price": "6.50",
    }
    assert not s.mini.orders
    reopened = BrokerSwitch(ChannelStore(s.store.path))
    assert reopened.backend == "qmt"
    assert reopened.route == s.qmt_route
    assert await s.switch.is_ready() is False  # read-only queries are still usable
    await s.switch.stop()


async def test_lost_response_restart_and_switch_retry_queries_original_route(setup):
    s = setup
    await s.switch.select("qmt")
    s.gateway.lose_response = True
    first = await s.switch.place_order("601988", "BUY", 100, request_id="lost")
    assert first.status == "UNKNOWN"
    s.switch = BrokerSwitch(ChannelStore(s.store.path))
    s.switch._clients = {s.mini_route: s.mini, s.qmt_route: s.qmt}
    await s.switch.select("miniqmt")
    result = await s.switch.place_order(
        "601988", "BUY", 100, request_id="lost", expected_channel=s.qmt_route
    )
    assert result.status == "RECEIVED"
    assert not s.mini.orders
    assert len(s.gateway.orders) == 1
    assert sum(method == "POST" for method, _, _ in s.gateway.requests) == 1
    await s.switch.stop()


async def test_unknown_404_never_resubmits_and_changed_intent_rejected(setup):
    s = setup
    await s.switch.select("qmt")
    s.gateway.lose_response = True
    await s.switch.place_order("601988", "BUY", 100, request_id="lost")
    s.gateway.unavailable_query = True
    assert (await s.switch.place_order("601988", "BUY", 100, request_id="lost")).status == "UNKNOWN"
    with pytest.raises(BrokerError, match="交易内容不同"):
        await s.switch.place_order("601988", "BUY", 200, request_id="lost")
    assert sum(method == "POST" for method, _, _ in s.gateway.requests) == 1
    assert s.store.orders(s.qmt_route)[0]["original_error"] == "SUBMISSION_UNCERTAIN"
    await s.switch.stop()


async def test_concurrent_identical_orders_dispatch_once_and_old_cancel_uses_old_target(setup):
    s = setup
    await s.switch.select("qmt")
    await asyncio.gather(
        *(s.switch.place_order("601988", "BUY", 100, request_id="same") for _ in range(2))
    )
    assert len(s.gateway.orders) == 1
    await s.switch.select("miniqmt")
    cancel_id = s.qmt_route + ":" + business_id("same")
    s.gateway.lose_cancel = True
    assert (await s.switch.cancel_order(cancel_id))["status"] == "UNKNOWN"
    result = await s.switch.cancel_order(cancel_id)
    assert result["status"] == "RECEIVED"
    assert len(s.gateway.cancels) == 1
    assert not s.mini.cancels
    await s.switch.stop()


async def test_mini_still_uses_existing_contract_and_deduplicates(setup):
    s = setup
    await s.switch.select("miniqmt")
    first = await s.switch.place_order("601988", "BUY", 100, request_id="mini")
    again = await s.switch.place_order("601988", "BUY", 100, request_id="mini")
    assert asdict(first) == asdict(again)
    assert len(s.mini.orders) == 1
    assert s.gateway.requests == []
    rows = await s.switch.get_orders()
    await s.switch.select("qmt")
    await s.switch.cancel_order(rows[0]["cancel_id"])
    assert s.mini.cancels == [321]
    await s.switch.stop()


@pytest.mark.parametrize(
    "field,value",
    [
        ("snapshot_age", 31),
        ("snapshot_age", -5),
        ("session", "old-session"),
        ("observed", False),
        ("snapshot_missing", True),
    ],
)
async def test_stale_or_unobserved_snapshot_is_unavailable_not_zero(setup, field, value):
    s = setup
    setattr(s.gateway, field, value)
    with pytest.raises(BrokerError, match="当前账户数据不可用"):
        await s.qmt.get_account()
    with pytest.raises(BrokerError):
        await s.switch.select("qmt")
    assert s.switch.backend == "miniqmt"
    await s.switch.stop()


async def test_missing_fields_are_not_fabricated(setup):
    s = setup
    account = await s.qmt.get_account()
    positions = await s.qmt.get_positions()
    assert account.cash == 1000.5
    assert account.frozen_cash is None
    assert positions[0].avg_price is None
    assert positions[0].frozen_volume is None
    assert positions[0].can_use_volume == 0
    await s.switch.stop()


async def test_partial_cancelled_order_keeps_fill_amount_and_never_becomes_fully_filled(setup):
    s = setup
    await s.switch.select("qmt")
    await s.switch.place_order("601988", "BUY", 200, request_id="partial")
    s.gateway.orders[business_id("partial")].update(
        state="PART_CANCELLED", filled_quantity=100, filled_average_price="6.51"
    )
    rows = await s.switch.get_orders()
    assert rows[0]["status"] == "PARTIALLY_CANCELLED"
    assert rows[0]["traded_qty"] == 100
    assert rows[0]["avg_traded_price"] == 6.51
    assert len(s.gateway.orders) == 1
    await s.switch.stop()


async def test_batch_has_one_durable_order_per_stock_and_no_implicit_fallback(setup):
    s = setup
    await s.switch.select("qmt")
    orders = [
        {"code": "601988", "amount": 1500, "ref_price": 6.5},
        {"code": "000001", "amount": 1500, "ref_price": 10},
    ]
    first = await s.switch.place_batch_by_amount(orders, request_id="batch")
    again = await s.switch.place_batch_by_amount(orders, request_id="batch")
    assert first == again
    assert [row["quantity"] for row in first["results"]] == [200, 100]
    assert all(row["status"] == "RECEIVED" for row in first["results"])
    assert len(s.gateway.orders) == 2
    assert all(row["intent"]["order_type"] == "market" for row in s.gateway.orders.values())
    await s.switch.stop()


async def test_stale_page_cannot_send_new_order_to_another_account(setup):
    s = setup
    await s.switch.select("qmt")
    with pytest.raises(BrokerError, match="交易通道已改变"):
        await s.switch.place_order(
            "601988", "BUY", 100, request_id="stale", expected_channel=s.mini_route
        )
    assert not s.gateway.orders
    await s.switch.stop()


async def test_late_poll_does_not_overwrite_new_channel_cache():
    class ChangingBroker:
        revision = 0

        async def get_positions(self):
            self.revision += 1
            return []

        async def get_account(self):
            return AccountInfo("old-account", 0, 0, 0, 0)

    app = SimpleNamespace(
        state=SimpleNamespace(
            broker=ChangingBroker(),
            broker_positions=["new"],
            available_cash=500,
            broker_account_id="new-account",
        )
    )
    assert await _broker_fetch_once(app) is None
    assert app.state.available_cash == 500
    assert app.state.broker_positions == ["new"]


async def test_web_selection_and_real_trade_route_use_qmt_transport(setup, monkeypatch):
    s = setup
    monkeypatch.setattr("src.common.config.get_trading_api_key", lambda: "web-key")
    monkeypatch.setattr("src.web.broker_channel_routes.default_store", lambda: s.store)
    monkeypatch.setattr("src.web.app.schedule_broker_post_order_refresh", lambda app: None)
    app = FastAPI()
    app.state.broker = s.switch
    app.state.broker_positions = [{"code": "old"}]
    app.include_router(create_broker_channel_router())
    app.include_router(create_trading_router())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://web", headers={"X-API-Key": "web-key"}
    ) as client:
        selection = await client.post("/api/settings/trading-channel", json={"backend": "qmt"})
        assert selection.status_code == 200
        assert app.state.broker_positions == []
        payload = {
            "stock_code": "601988",
            "quantity": 100,
            "price": 6.5,
            "request_id": "web-one",
            "channel": s.qmt_route,
        }
        response = await client.post("/api/trading/buy", json=payload)
        assert response.status_code == 200
        assert response.json()["status"] == "RECEIVED"
        again = await client.post("/api/trading/buy", json=payload)
        assert again.json() == response.json()
        status = (await client.get("/api/settings/trading-channel")).json()
        assert "secret" not in json.dumps(status)
        assert len(s.gateway.orders) == 1
        denied = await client.post(
            "/api/settings/trading-channel",
            json={"backend": "miniqmt"},
            headers={"X-API-Key": "wrong"},
        )
        assert denied.status_code == 401
        for bad_quantity in (100.2, "100", True):
            invalid = await client.post(
                "/api/trading/buy", json={**payload, "quantity": bad_quantity}
            )
            assert invalid.status_code == 400
        assert len(s.gateway.orders) == 1
    await s.switch.stop()


@pytest.mark.parametrize("price", [float("nan"), float("inf"), 0, -1, 6.501, 10000])
def test_invalid_limit_prices_rejected_before_dispatch(price):
    with pytest.raises(BrokerError):
        order_intent("601988", "BUY", 100, "LIMIT", price)


async def test_account_and_holdings_use_one_current_snapshot(setup):
    s = setup
    await s.switch.select("qmt")
    s.gateway.requests.clear()
    positions, account = await s.switch.get_account_and_positions()
    assert positions[0].code == "601988.SH"
    assert account.cash == 1000.5
    assert [path for _, path, _ in s.gateway.requests] == ["/v1/agent", "/v1/trading/snapshot"]
    await s.switch.stop()


async def test_failed_poll_retains_known_fill(setup):
    s = setup
    await s.switch.select("qmt")
    await s.switch.place_order("601988", "BUY", 100, request_id="filled")
    s.gateway.orders[business_id("filled")].update(
        state="FILLED", filled_quantity=100, filled_average_price="6.51"
    )
    result = await s.switch.place_order("601988", "BUY", 100, request_id="filled")
    assert result.status == "FILLED"
    s.gateway.unavailable_query = True
    again = await s.switch.place_order("601988", "BUY", 100, request_id="filled")
    assert again.status == "FILLED"
    assert again.traded_qty == 100
    assert again.avg_traded_price == 6.51
    await s.switch.stop()


async def test_saved_config_does_not_silently_switch_active_target_after_restart(setup):
    s = setup
    await s.switch.select("qmt")
    candidate = s.store.save_profile({**SPEC, "url": "https://different.example.invalid"})
    restored = BrokerSwitch(ChannelStore(s.store.path))
    assert restored.route == s.qmt_route
    assert restored.route != candidate
    await s.switch.stop()


async def test_wrong_order_identity_cannot_claim_fill(setup):
    s = setup
    await s.switch.select("qmt")
    await s.switch.place_order("601988", "BUY", 100, request_id="identity")
    s.gateway.orders[business_id("identity")].update(
        state="FILLED", filled_quantity=100, filled_average_price="6.51", order_id="wrong-id"
    )
    result = await s.switch.place_order("601988", "BUY", 100, request_id="identity")
    assert result.status != "FILLED"
    assert s.store.orders(s.qmt_route)[0]["original_error"] == "INVALID_RESPONSE"
    await s.switch.stop()


async def test_service_unavailable_after_post_is_uncertain_and_never_falls_back(setup):
    s = setup
    await s.switch.select("qmt")
    old = s.qmt._client

    def unavailable(req):
        return httpx.Response(503, json={"error_code": "GATEWAY_DRAINING"})

    s.qmt._client = httpx.AsyncClient(
        base_url=SPEC["url"], transport=httpx.MockTransport(unavailable)
    )
    result = await s.switch.place_order("601988", "BUY", 100, request_id="503")
    assert result.status == "UNKNOWN"
    assert s.store.orders(s.qmt_route)[0]["original_error"] == "GATEWAY_DRAINING"
    assert not s.mini.orders
    await old.aclose()
    await s.switch.stop()


async def test_failed_durable_reservation_prevents_any_post(setup, monkeypatch):
    s = setup
    await s.switch.select("qmt")

    def unavailable(*args, **kwargs):
        raise OSError("disk unavailable")

    monkeypatch.setattr(s.store, "reserve", unavailable)
    with pytest.raises(OSError):
        await s.switch.place_order("601988", "BUY", 100, request_id="disk")
    assert not s.gateway.orders
    await s.switch.stop()
