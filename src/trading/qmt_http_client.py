"""Complete-QMT HTTP protocol adapter. No administration or automatic POST retries."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
import secrets
import ssl
import time
from decimal import Decimal, InvalidOperation
from urllib.parse import urlsplit

import httpx

from src.trading.broker_client import AccountInfo, BrokerError, OrderRecord, Position
from src.trading.channel_store import canonical


def validate_qmt_config(spec: dict) -> dict:
    try:
        base = urlsplit(spec["url"])
        if (
            base.scheme != "https"
            or not base.hostname
            or base.username is not None
            or base.password is not None
            or base.path not in ("", "/")
            or base.query
            or base.fragment
        ):
            raise ValueError
        for key in ("instance_id", "key_id", "secret"):
            value = spec[key]
            if (
                not isinstance(value, str)
                or not value
                or any(ord(c) < 33 or ord(c) > 126 for c in value)
            ):
                raise ValueError
        if ":" in spec["key_id"]:
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise BrokerError(
            "INVALID_CONFIG", "QMT 需要 HTTPS 源地址及有效的实例、密钥标识和签名密钥"
        ) from None
    return {
        "backend": "qmt",
        "url": spec["url"].rstrip("/"),
        "instance_id": spec["instance_id"],
        "key_id": spec["key_id"],
        "secret": spec["secret"],
        "ca_file": spec.get("ca_file") or None,
    }


def business_id(key: str, prefix="w") -> str:
    return prefix + hashlib.sha256(key.encode()).hexdigest()[:48]


def order_intent(code: str, side: str, qty: int, price_type: str, price) -> dict:
    code = code.upper()
    if "." not in code:
        suffix = "SH" if code.startswith(("6", "9")) else "SZ"
        code = code + "." + suffix
    if (
        not re.fullmatch(r"[0-9]{6}\.(SH|SZ)", code)
        or side not in ("BUY", "SELL")
        or type(qty) is not int
        or not 1 <= qty <= 1000000
        or price_type not in ("LIMIT", "MARKET")
    ):
        raise BrokerError("INVALID_ORDER", "QMT 股票、方向、股数或委托类型无效")
    result = {
        "symbol": code,
        "side": side.lower(),
        "quantity": qty,
        "order_type": price_type.lower(),
    }
    if price_type == "LIMIT":
        try:
            number = Decimal(str(price))
            if (
                not number.is_finite()
                or not 0 < number <= 9999
                or number != number.quantize(Decimal(".01"))
            ):
                raise ValueError
            result["limit_price"] = format(number, ".2f")
        except (InvalidOperation, ValueError):
            raise BrokerError(
                "INVALID_PRICE", "QMT 限价须大于 0、最多两位小数且不超过 9999"
            ) from None
    return result


_STATES = {
    "PART_FILLED": "PARTIALLY_FILLED",
    "PART_CANCELLED": "PARTIALLY_CANCELLED",
    "CANCEL_PENDING": "PENDING_CANCEL",
    "PART_CANCEL_PENDING": "PARTIALLY_FILLED_PENDING_CANCEL",
}


def to_order(data: dict, intent: dict, key: str) -> OrderRecord:
    filled = data.get("filled_quantity")
    average = data.get("filled_average_price")
    return OrderRecord(
        order_id=key,
        seq=None,
        code=intent["symbol"],
        side=intent["side"].upper(),
        price=float(intent.get("limit_price", 0)),
        qty=intent["quantity"],
        traded_qty=filled,
        avg_traded_price=float(average) if average is not None else None,
        status=_STATES.get(data.get("state"), data.get("state", "UNKNOWN")),
        backend="qmt",
    )


class QmtHttpClient:
    backend = "qmt"

    def __init__(self, spec: dict):
        self.spec = validate_qmt_config(spec)
        self._client: httpx.AsyncClient | None = None
        self._request_lock = asyncio.Lock()
        self._last_call = 0.0

    async def start(self):
        try:
            context = ssl.create_default_context(cafile=self.spec["ca_file"])
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            self._client = httpx.AsyncClient(
                base_url=self.spec["url"],
                verify=context,
                trust_env=False,
                follow_redirects=False,
                timeout=httpx.Timeout(10, connect=5),
            )
        except (OSError, ssl.SSLError):
            raise BrokerError("TLS_CONFIG", "QMT CA 证书不可用，请检查服务端证书文件") from None

    async def stop(self):
        if self._client:
            await self._client.aclose()

    async def request(self, path: str, body: dict | None = None) -> dict:
        if self._client is None:
            raise BrokerError("NOT_STARTED", "QMT 客户端尚未初始化")
        if not re.fullmatch(
            r"/v1/(agent|trading/(status|execution|snapshot|orders|cancels|(?:orders|commands)/[A-Za-z0-9_-]{1,64}))",
            path,
        ):
            raise BrokerError("INVALID_TARGET", "QMT 请求路径无效")
        method = "GET" if body is None else "POST"
        raw = b"" if body is None else canonical(body).encode("ascii")
        async with self._request_lock:
            await asyncio.sleep(max(0, 0.06 - (time.monotonic() - self._last_call)))
            self._last_call = time.monotonic()
            timestamp, nonce = str(int(time.time())), secrets.token_hex(16)
            content_type = "application/json" if body is not None else ""
            message = "\n".join(
                (
                    "QMT-HMAC-SHA256",
                    self.spec["instance_id"],
                    method,
                    path,
                    content_type,
                    timestamp,
                    nonce,
                    hashlib.sha256(raw).hexdigest(),
                )
            )
            signature = hmac.new(
                self.spec["secret"].encode("ascii"), message.encode("ascii"), hashlib.sha256
            ).hexdigest()
            headers = {
                "Authorization": f"QMT-HMAC-SHA256 {self.spec['key_id']}:{signature}",
                "X-Qmt-Timestamp": timestamp,
                "X-Qmt-Nonce": nonce,
            }
            if content_type:
                headers["Content-Type"] = content_type
            try:
                async with self._client.stream(
                    method, path, content=raw, headers=headers
                ) as response:
                    payload = bytearray()
                    async for chunk in response.aiter_bytes():
                        payload.extend(chunk)
                        if len(payload) > 262144:
                            raise ValueError
                    data = json.loads(payload)
                    if not isinstance(data, dict):
                        raise ValueError
                    if response.status_code not in (200, 202):
                        error = str(data.get("error_code") or f"HTTP_{response.status_code}")
                        # All POST errors remain queryable through the durable original ID.
                        failure = BrokerError(error, f"QMT 返回 {error}")
                        failure.http_status = response.status_code
                        raise failure
                    return data
            except (httpx.HTTPError, ValueError):
                raise BrokerError(
                    "SUBMISSION_UNCERTAIN" if body is not None else "QUERY_UNAVAILABLE",
                    "QMT 提交结果未确认，请查询原订单"
                    if body is not None
                    else "QMT 当前查询不可用",
                ) from None

    async def readiness_error(self):
        try:
            await self.snapshot()
            return None
        except BrokerError as exc:
            return exc.message

    async def is_ready(self):
        try:
            status = await self.request("/v1/trading/status")
            execution = await self.request("/v1/trading/execution")
            return (
                status.get("trade_ready") is True and execution.get("production_authorized") is True
            )
        except BrokerError:
            return False

    async def snapshot(self):
        agent = await self.request("/v1/agent")
        result = await self.request("/v1/trading/snapshot")
        snapshot = result.get("snapshot")
        session = agent.get("runtime", {}).get("trading_session")
        try:
            age = time.time() - float(snapshot["updated_at"])
            valid = (
                session
                and snapshot["entry_session"] == session
                and snapshot["account_observed"] is True
                and 0 <= age <= 30
                and isinstance(snapshot["data"], dict)
            )
        except (KeyError, TypeError, ValueError):
            valid = False
        if not valid:
            raise BrokerError("SNAPSHOT_UNAVAILABLE", "QMT 当前账户数据不可用")
        return snapshot["data"]

    def _positions(self, data):
        return [
            Position(
                code=p["symbol"],
                volume=p["quantity"],
                can_use_volume=p["available"],
                frozen_volume=None,
                avg_price=None,
                market_value=float(p["market_value"]),
            )
            for p in data["positions"]
        ]

    def _account(self, snapshot):
        data = snapshot["account"]
        return AccountInfo(
            account_id="qmt:" + self.spec["instance_id"],
            cash=float(data["available_cash"]),
            frozen_cash=None,
            market_value=float(data["market_value"]),
            total_asset=float(data["total_assets"]),
        )

    async def get_positions(self):
        return self._positions(await self.snapshot())

    async def get_account(self):
        return self._account(await self.snapshot())

    async def get_account_and_positions(self):
        snapshot = await self.snapshot()
        return self._positions(snapshot), self._account(snapshot)

    async def submit(self, key: str, intent: dict, first: bool) -> dict:
        path = "/v1/trading/orders"
        if first:
            data = await self.request(path, {"order_id": key, **intent})
        else:
            data = await self.request(path + "/" + key)
        if (
            data.get("order_id") != key
            or data.get("intent") != {"order_id": key, **intent}
            or (data.get("state") == "FILLED" and data.get("filled_quantity") != intent["quantity"])
        ):
            raise BrokerError("INVALID_RESPONSE", "QMT 订单回报未匹配原请求，结果未确认")
        return data

    async def cancel(self, key: str, order_id: str, first: bool) -> dict:
        if first:
            return await self.request(
                "/v1/trading/cancels", {"cancel_id": key, "order_id": order_id}
            )
        return await self.request("/v1/trading/commands/" + key)

    async def get_trades(self):
        return (await self.snapshot())["trades"]
