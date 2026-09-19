"""Web-selected broker, with durable ownership of requests across channel switches."""

from __future__ import annotations

import asyncio
import re
import uuid
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from src.trading.broker_client import BrokerClient, BrokerError, OrderRecord
from src.trading.channel_store import ChannelStore, default_store
from src.trading.qmt_http_client import QmtHttpClient, business_id, order_intent, to_order

_TERMINAL = {"FILLED", "CANCELLED", "PARTIALLY_CANCELLED", "REJECTED", "EXPIRED", "NOT_SENT"}


class BrokerSwitch:
    def __init__(self, store: ChannelStore | None = None):
        self.store = store or default_store()
        self._clients = {}
        self._lock = asyncio.Lock()
        self.route = self.store.preference(
            "active_route", self.store.preference(self.store.backend, "")
        )
        self.revision = 0

    @property
    def backend(self):
        return self.store.profile(self.route)["backend"] if self.route else self.store.backend

    def import_legacy_config(self):
        from src.common.config import get_xtquant_api_key, get_xtquant_server_url

        try:
            spec = {
                "backend": "miniqmt",
                "url": get_xtquant_server_url(),
                "api_key": get_xtquant_api_key(),
            }
        except ValueError:
            return
        self.store.save_profile(spec)

    async def _client(self, route: str):
        if not route:
            raise BrokerError("NOT_CONFIGURED", "请先在设置页配置交易通道")
        if route not in self._clients:
            spec = self.store.profile(route)
            client = (
                QmtHttpClient(spec)
                if spec["backend"] == "qmt"
                else BrokerClient(spec["url"], spec["api_key"])
            )
            await client.start()
            self._clients[route] = client
        return self._clients[route]

    async def start(self):
        self.import_legacy_config()
        self.route = self.store.preference(
            "active_route", self.store.preference(self.store.backend, "")
        )
        await self._client(self.route)

    async def stop(self):
        for client in self._clients.values():
            await client.stop()
        self._clients.clear()

    async def select(self, backend: str):
        async with self._lock:
            route = self.store.preference(backend)
            if not route:
                raise BrokerError("NOT_CONFIGURED", "请先配置所选交易通道")
            client = await self._client(route)
            error = await client.readiness_error()
            if error:
                raise BrokerError("CHANNEL_UNAVAILABLE", "所选通道暂不可用，保留当前选择")
            self.store.select(backend)
            self.route = route
            self.revision += 1

    async def readiness_error(self):
        async with self._lock:
            return await (await self._client(self.route)).readiness_error()

    async def is_ready(self):
        async with self._lock:
            return await (await self._client(self.route)).is_ready()

    async def get_positions(self):
        async with self._lock:
            return await (await self._client(self.route)).get_positions()

    async def get_account(self):
        async with self._lock:
            return await (await self._client(self.route)).get_account()

    async def get_account_and_positions(self):
        async with self._lock:
            client = await self._client(self.route)
            if isinstance(client, QmtHttpClient):
                return await client.get_account_and_positions()
            return await client.get_positions(), await client.get_account()

    async def get_trades(self):
        async with self._lock:
            return await (await self._client(self.route)).get_trades()

    def _reserve(self, key, kind, intent, expected_channel=None, route=None):
        if key is None:
            if self.backend == "qmt":
                raise BrokerError("REQUEST_ID_REQUIRED", "请刷新页面后提交（缺少请求编号）")
            key = uuid.uuid4().hex
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", key):
            raise BrokerError("INVALID_REQUEST_ID", "请求编号无效")
        target = route or self.route
        if expected_channel and expected_channel != target:
            # Existing retries retain their original route; a new click on a stale page is rejected.
            with self.store.db() as db:
                exists = db.execute("SELECT 1 FROM requests WHERE id=?", (key,)).fetchone()
            if not exists:
                raise BrokerError("CHANNEL_CHANGED", "交易通道已改变，请刷新页面后重新操作")
        return (*self.store.reserve(key, target, kind, intent), key)

    async def place_order(
        self,
        code,
        side,
        qty,
        price_type="MARKET",
        price=None,
        remark="",
        request_id=None,
        expected_channel=None,
    ):
        async with self._lock:
            return await self._place(
                code, side, qty, price_type, price, remark, request_id, expected_channel
            )

    async def _place(
        self,
        code,
        side,
        qty,
        price_type,
        price,
        remark,
        request_id,
        expected_channel=None,
        route=None,
        allow_new=True,
    ):
        intent = {
            "code": code,
            "side": side,
            "qty": qty,
            "price_type": price_type,
            "price": price,
            "remark": remark,
        }
        selected = route or self.route
        row, first, key = self._reserve(request_id, "order", intent, expected_channel, selected)
        client = await self._client(row["route"])
        if isinstance(client, QmtHttpClient):
            payload = order_intent(code, side, qty, price_type, price)
            native_id = business_id(key)
            try:
                data = await client.submit(native_id, payload, first and allow_new)
                result = to_order(data, payload, native_id)
            except BrokerError as exc:
                self.store.record_error(key, str(exc.code))
                # Preserve uncertainty, including 404 after a lost response. Never re-POST.
                if (
                    not first
                    or exc.code in ("SUBMISSION_UNCERTAIN", "NOT_FOUND", "INVALID_RESPONSE")
                    or getattr(exc, "http_status", 0) >= 500
                ):
                    result = (
                        OrderRecord(**row["result"])
                        if row["result"] and "status" in row["result"]
                        else to_order({"state": "UNKNOWN"}, payload, native_id)
                    )
                else:
                    self.store.complete(key, {"error": exc.code, "message": exc.message})
                    raise
        elif not first:
            if not row["result"]:
                raise BrokerError("SUBMISSION_UNCERTAIN", "miniQMT 原请求结果未确认，请查询委托")
            if "error" in row["result"]:
                raise BrokerError(row["result"]["error"], row["result"]["message"])
            return OrderRecord(**row["result"])
        else:
            result = await client.place_order(**intent)
        self.store.complete(key, asdict(result))
        return result

    async def place_batch_by_amount(
        self,
        orders,
        side="BUY",
        max_retries=3,
        fallback="limit_at_ref",
        request_id=None,
        expected_channel=None,
    ):
        async with self._lock:
            intent = {
                "orders": orders,
                "side": side,
                "max_retries": max_retries,
                "fallback": fallback,
            }
            row, first, key = self._reserve(request_id, "batch", intent, expected_channel)
            client = await self._client(row["route"])
            if not isinstance(client, QmtHttpClient):
                if not first:
                    if row["result"] is None:
                        raise BrokerError(
                            "SUBMISSION_UNCERTAIN", "原批量请求结果未确认，请查询委托"
                        )
                    return row["result"]
                result = await client.place_batch_by_amount(**intent)
                self.store.complete(key, result)
                return result
            # QMT accepts explicit quantities only. One ordinary market order per stock.
            # Its native immediate-or-cancel remainder is never silently resubmitted.
            planned = []
            for order in orders:
                amount, price = Decimal(str(order["amount"])), Decimal(str(order["ref_price"]))
                if not amount.is_finite() or not price.is_finite() or amount <= 0 or price <= 0:
                    raise BrokerError("INVALID_AMOUNT", "金额和参考价必须为正数")
                qty = int(amount / price / 100) * 100
                order_intent(order["code"], side, qty, "MARKET", None)
                planned.append((order["code"], qty))
            results = []
            for index, (code, qty) in enumerate(planned):
                try:
                    result = await self._place(
                        code,
                        side,
                        qty,
                        "MARKET",
                        None,
                        "Web批量买入",
                        business_id(f"{key}-{index}", "b"),
                        route=row["route"],
                        allow_new=first,
                    )
                    results.append(
                        {
                            "code": result.code,
                            "order_id": result.order_id,
                            "quantity": qty,
                            "filled_quantity": result.traded_qty,
                            "avg_price": result.avg_traded_price,
                            "status": result.status,
                        }
                    )
                except BrokerError as exc:
                    results.append(
                        {
                            "code": code,
                            "quantity": qty,
                            "filled_quantity": None,
                            "status": "UNCONFIRMED",
                            "error": exc.message,
                        }
                    )
            result = {
                "backend": "qmt",
                "results": results,
                "summary": {
                    "total": len(results),
                    "filled": sum(r["status"] == "FILLED" for r in results),
                },
            }
            self.store.complete(key, result)
            return result

    async def cancel_order(self, order_id, request_id=None):
        async with self._lock:
            value = str(order_id)
            if ":" in value:
                route, native_id = value.split(":", 1)
            elif self.backend == "miniqmt" and value.isdigit():
                route, native_id = self.route, value
            else:
                raise BrokerError("ORDER_CHANNEL_REQUIRED", "请刷新委托列表后撤单")
            client = await self._client(route)
            key = business_id(route + ":" + native_id, "c")
            row, first = self.store.reserve(key, route, "cancel", {"order_id": native_id})
            if isinstance(client, QmtHttpClient):
                if not any(business_id(o["id"]) == native_id for o in self.store.orders(route)):
                    raise BrokerError("EXTERNAL_ORDER", "该委托不属于此 Web 的 QMT 订单")
                try:
                    result = await client.cancel(key, native_id, first)
                    result = {"status": result.get("state", "UNKNOWN"), "order_id": native_id}
                except BrokerError as exc:
                    self.store.record_error(key, str(exc.code))
                    if (
                        not first
                        or exc.code == "SUBMISSION_UNCERTAIN"
                        or getattr(exc, "http_status", 0) >= 500
                    ):
                        result = {"status": "UNKNOWN", "order_id": native_id}
                    else:
                        raise
            elif not first:
                result = row["result"] or {"status": "UNKNOWN", "order_id": native_id}
            else:
                result = await client.cancel_order(int(native_id))
            self.store.complete(key, result)
            return result

    async def get_orders(self):
        async with self._lock:
            route = self.route
            client = await self._client(route)
            if not isinstance(client, QmtHttpClient):
                rows = await client.get_orders()
                return [
                    {**r, "cancel_id": f"{route}:{r['order_id']}" if r.get("order_id") else None}
                    for r in rows
                ]
            snapshot = await client.snapshot()
            today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
            known = self.store.orders(route)
            rows, matched = [], set()
            for saved in known:
                previous = saved["result"] or {}
                created = datetime.fromtimestamp(saved["created"], ZoneInfo("Asia/Shanghai"))
                if created.strftime("%Y%m%d") != today and previous.get("status") in _TERMINAL:
                    continue
                native_id = business_id(saved["id"])
                intent = saved["intent"]
                payload = order_intent(
                    intent["code"],
                    intent["side"],
                    intent["qty"],
                    intent["price_type"],
                    intent["price"],
                )
                try:
                    data = await client.submit(native_id, payload, False)
                    if data.get("broker_order_id"):
                        matched.add(str(data["broker_order_id"]))
                    record = to_order(data, payload, native_id)
                    self.store.complete(saved["id"], asdict(record))
                    error = data.get("error_code")
                except BrokerError as exc:
                    # A failed poll cannot turn previously observed fills into zero/unknown.
                    record = (
                        OrderRecord(**previous)
                        if "status" in previous
                        else to_order({"state": "UNKNOWN"}, payload, native_id)
                    )
                    error = exc.code
                rows.append(
                    {
                        **asdict(record),
                        "order_id": f"{route}:{native_id}",
                        "cancel_id": f"{route}:{native_id}",
                        "source": "qmt",
                        "submit_time": created.isoformat(),
                        "query_error": error,
                    }
                )
            for order in snapshot["orders"]:
                if str(order.get("broker_order_id")) in matched:
                    continue
                if order.get("trading_day") != today:
                    continue
                rows.append(
                    {
                        "order_id": None,
                        "cancel_id": None,
                        "source": "qmt_external",
                        "code": order["symbol"],
                        "side": order["side"].upper(),
                        "qty": order["quantity"],
                        "price": order.get("price"),
                        "traded_qty": order.get("filled"),
                        "avg_traded_price": None,
                        "status": order["state"],
                    }
                )
            return rows
