from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_CONNECT_TIMEOUT = 5.0
_REQUEST_TIMEOUT = 30.0


def _normalize_code(code: str) -> str:
    """Convert bare stock code to xtquant format: '601398' → '601398.SH'."""
    if "." in code:
        return code
    if code.startswith(("60", "68", "90")):
        return f"{code}.SH"
    return f"{code}.SZ"


@dataclass
class Position:
    code: str
    volume: int
    can_use_volume: int
    frozen_volume: int
    avg_price: float
    market_value: float
    last_price: float | None = None


@dataclass
class AccountInfo:
    account_id: str
    cash: float
    frozen_cash: float
    market_value: float
    total_asset: float


@dataclass
class OrderRecord:
    order_id: int | None
    seq: int | None
    code: str
    side: str
    price: float
    qty: int
    traded_qty: int
    avg_traded_price: float
    status: str


class BrokerError(Exception):
    """Business error from xtquant-trade-server (envelope code != 0)."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class BrokerClient:
    """Async HTTP client wrapping xtquant-trade-server REST API.

    Stock codes passed as bare codes ('601398') are auto-normalized to
    'NNNNNN.SH' / 'NNNNNN.SZ' before sending.
    """

    def __init__(self, base_url: str, api_key: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={"X-API-Key": self._api_key},
            timeout=httpx.Timeout(_REQUEST_TIMEOUT, connect=_CONNECT_TIMEOUT),
        )

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _c(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("BrokerClient not started — call start() first")
        return self._client

    def _unwrap(self, resp_json: dict) -> Any:
        """Raise BrokerError if envelope code != 0, else return data field."""
        if resp_json.get("code", 0) != 0:
            raise BrokerError(resp_json["code"], resp_json.get("message", "unknown error"))
        return resp_json.get("data")

    async def is_ready(self) -> bool:
        """Return True if xtquant-trade-server broker connection is ready."""
        try:
            resp = await self._c().get("/readyz")
            return resp.status_code == 200
        except Exception:
            return False

    async def get_account(self) -> AccountInfo:
        resp = await self._c().get("/v1/account")
        resp.raise_for_status()
        d = self._unwrap(resp.json())
        return AccountInfo(
            account_id=d["account_id"],
            cash=d["cash"],
            frozen_cash=d["frozen_cash"],
            market_value=d["market_value"],
            total_asset=d["total_asset"],
        )

    async def get_positions(self) -> list[Position]:
        resp = await self._c().get("/v1/positions")
        resp.raise_for_status()
        items = self._unwrap(resp.json()) or []
        return [
            Position(
                code=p["code"],
                volume=p["volume"],
                can_use_volume=p["can_use_volume"],
                frozen_volume=p["frozen_volume"],
                avg_price=p["avg_price"],
                market_value=p["market_value"],
                last_price=p.get("last_price"),
            )
            for p in items
        ]

    async def place_order(
        self,
        code: str,
        side: str,
        qty: int,
        price_type: str = "MARKET",
        price: float | None = None,
        remark: str = "",
    ) -> OrderRecord:
        """Place an order. side='BUY'|'SELL', price_type='LIMIT'|'MARKET'."""
        payload: dict[str, Any] = {
            "code": _normalize_code(code),
            "side": side,
            "qty": qty,
            "price_type": price_type,
            "remark": remark,
        }
        if price is not None:
            payload["price"] = price

        resp = await self._c().post("/v1/orders", json=payload)
        resp.raise_for_status()
        d = self._unwrap(resp.json())
        return OrderRecord(
            order_id=d.get("order_id"),
            seq=d.get("seq"),
            code=d.get("code", payload["code"]),
            side=d.get("side", side),
            price=d.get("price", price or 0.0),
            qty=d.get("qty", qty),
            traded_qty=d.get("traded_qty", 0),
            avg_traded_price=d.get("avg_traded_price", 0.0),
            status=d.get("status", ""),
        )

    async def place_batch_by_amount(
        self,
        orders: list[dict],
        side: str = "BUY",
        max_retries: int = 3,
        fallback: str = "limit_at_ref",
    ) -> dict:
        """Amount-driven batch market order via /v1/orders/batch-by-amount.

        Each entry in `orders` must have keys `code`, `amount`, `ref_price`.
        Server computes lots = floor(amount / ref_price / 100) per leg and
        submits MARKET_CONVERT_5_CANCEL with retry; residuals fall back to
        passive limits at `ref_price` (or are skipped if `fallback="skip"`).
        Returns the raw envelope `data` field (results[] + summary).
        """
        normalized = [
            {
                "code": _normalize_code(o["code"]),
                "amount": float(o["amount"]),
                "ref_price": float(o["ref_price"]),
            }
            for o in orders
        ]
        payload = {
            "side": side,
            "max_retries": max_retries,
            "fallback": fallback,
            "orders": normalized,
        }
        resp = await self._c().post("/v1/orders/batch-by-amount", json=payload)
        resp.raise_for_status()
        return self._unwrap(resp.json()) or {}

    async def cancel_order(self, order_id: int) -> dict:
        resp = await self._c().delete(f"/v1/orders/{order_id}")
        resp.raise_for_status()
        return self._unwrap(resp.json()) or {}

    async def cancel_all(self) -> dict:
        resp = await self._c().delete("/v1/orders")
        resp.raise_for_status()
        return self._unwrap(resp.json()) or {}

    async def get_orders(self) -> list[dict]:
        """Return today's open orders."""
        resp = await self._c().get("/v1/orders")
        resp.raise_for_status()
        return self._unwrap(resp.json()) or []

    async def get_trades(self) -> list[dict]:
        """Return today's fills."""
        resp = await self._c().get("/v1/trades")
        resp.raise_for_status()
        return self._unwrap(resp.json()) or []
