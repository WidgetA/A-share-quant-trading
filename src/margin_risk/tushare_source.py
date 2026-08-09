"""Tushare Pro source adapter for daily production MEWS ingestion."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any, Protocol

import httpx

from src.margin_risk.config import MarginRiskConfig

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXPERIMENT_ENV_PATH = PROJECT_ROOT / "strategy-research" / ".env"


class TushareExperimentError(Exception):
    """Backward-compatible redacted failure raised by the MEWS adapter."""


class _TushareClient(Protocol):
    async def query(
        self,
        api_name: str,
        params: Mapping[str, Any],
        fields: Sequence[str],
    ) -> dict[str, Any]: ...


class _LocalTushareClient:
    """Small isolated HTTP client owned by the MEWS ingestion adapter."""

    API_URL = "http://api.tushare.pro"

    def __init__(self, token: str) -> None:
        self._token = token
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(60.0))

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def query(
        self,
        api_name: str,
        params: Mapping[str, Any],
        fields: Sequence[str],
    ) -> dict[str, Any]:
        if self._client is None:
            raise TushareExperimentError("local Tushare client is not started")
        try:
            response = await self._client.post(
                self.API_URL,
                json={
                    "api_name": api_name,
                    "token": self._token,
                    "params": dict(params),
                    "fields": ",".join(fields),
                },
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise TushareExperimentError(f"Tushare request failed: {exc}") from None
        if payload.get("code") not in (None, 0):
            raise TushareExperimentError(str(payload.get("msg") or "Tushare API error"))
        return payload


def _load_experiment_token() -> str:
    """Resolve a token for legacy research callers that do not pass one explicitly.

    Production always supplies the application-configured token explicitly.
    """

    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if token:
        return token
    if EXPERIMENT_ENV_PATH.exists():
        for raw_line in EXPERIMENT_ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() == "TUSHARE_TOKEN":
                token = value.strip().strip("\"'")
                if token:
                    return token
    raise TushareExperimentError(
        "TUSHARE_TOKEN is required in the process environment or strategy-research/.env"
    )


def _parse_date(value: Any) -> date | None:
    raw = str(value or "").replace("-", "")
    if len(raw) != 8 or not raw.isdigit():
        return None
    return datetime.strptime(raw, "%Y%m%d").date()


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _scaled_float(value: Any, factor: float) -> float | None:
    parsed = _float(value)
    return parsed * factor if parsed is not None else None


class TushareMarginRiskSource:
    """Rate-limited, retrying per-date access to all MEWS source endpoints."""

    def __init__(
        self,
        config: MarginRiskConfig,
        client: _TushareClient | None = None,
        token: str | None = None,
    ) -> None:
        resolved_token = token if token is not None else _load_experiment_token()
        self._token = resolved_token
        self._client = client or _LocalTushareClient(token=resolved_token)
        self._owns_client = client is None
        self._config = config
        self._started = False
        self._request_lock = asyncio.Lock()
        self._last_request_at = 0.0

    async def __aenter__(self) -> "TushareMarginRiskSource":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._started:
            return
        if self._owns_client:
            await self._client.start()  # type: ignore[attr-defined]
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        if self._owns_client:
            await self._client.stop()  # type: ignore[attr-defined]
        self._started = False

    def _safe_error(self, error: BaseException) -> str:
        message = str(error)
        if self._token:
            message = message.replace(self._token, "***")
        return message[:500]

    async def _throttle(self) -> None:
        async with self._request_lock:
            elapsed = time.monotonic() - self._last_request_at
            wait = self._config.request_interval_seconds - elapsed
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_at = time.monotonic()

    async def _query(
        self,
        api_name: str,
        params: Mapping[str, Any],
        fields: Sequence[str],
        *,
        allow_empty: bool = False,
        context: str = "",
    ) -> list[dict[str, Any]]:
        if not self._started:
            raise RuntimeError("TushareMarginRiskSource not started")
        last_error: BaseException | None = None
        for attempt in range(1, self._config.max_retries + 1):
            try:
                await self._throttle()
                response = await self._client.query(
                    api_name,
                    dict(params),
                    fields,
                )
                payload = response.get("data") or {}
                response_fields = payload.get("fields") or []
                items = payload.get("items") or []
                rows = [
                    dict(zip(response_fields, item, strict=False))
                    for item in items
                    if response_fields
                ]
                if rows or allow_empty:
                    return rows
                raise TushareExperimentError(f"{api_name} returned an empty result")
            except (TushareExperimentError, TimeoutError, OSError) as exc:
                last_error = exc
                if attempt < self._config.max_retries:
                    wait = self._config.retry_backoff_seconds * (2 ** (attempt - 1))
                    logger.warning(
                        "Tushare %s %s attempt %d/%d failed; retrying in %.1fs: %s",
                        api_name,
                        context,
                        attempt,
                        self._config.max_retries,
                        wait,
                        self._safe_error(exc),
                    )
                    await asyncio.sleep(wait)
        safe = self._safe_error(last_error or RuntimeError("unknown error"))
        logger.error("Tushare %s %s failed after retries: %s", api_name, context, safe)
        # Suppress the original exception chain: even if an upstream proxy
        # echoed credentials in its error text, later ``exc_info`` logging can
        # only see this redacted message.
        raise TushareExperimentError(f"{api_name} failed after retries: {safe}") from None

    async def fetch_trade_calendar(
        self,
        exchange: str,
        start: date,
        end: date,
    ) -> list[dict[str, Any]]:
        rows = await self._query(
            "trade_cal",
            {
                "exchange": exchange,
                "start_date": start.strftime("%Y%m%d"),
                "end_date": end.strftime("%Y%m%d"),
            },
            ("exchange", "cal_date", "is_open", "pretrade_date"),
            context=f"{exchange} {start:%Y%m%d}-{end:%Y%m%d}",
        )
        output: list[dict[str, Any]] = []
        for row in rows:
            cal_date = _parse_date(row.get("cal_date"))
            if cal_date is None:
                continue
            output.append(
                {
                    "exchange": exchange,
                    "cal_date": cal_date,
                    "is_open": str(row.get("is_open")) == "1",
                    "pretrade_date": _parse_date(row.get("pretrade_date")),
                }
            )
        return output

    async def fetch_stock_basic(self) -> list[dict[str, Any]]:
        output: dict[str, dict[str, Any]] = {}
        fields = (
            "ts_code",
            "symbol",
            "name",
            "market",
            "exchange",
            "list_status",
            "list_date",
            "delist_date",
        )
        for exchange in ("SSE", "SZSE"):
            for status in ("L", "D", "P", "G"):
                rows = await self._query(
                    "stock_basic",
                    {"exchange": exchange, "list_status": status},
                    fields,
                    # P/G can legitimately be empty. L/D are foundational to
                    # the point-in-time universe, so an empty response must go
                    # through the normal retry/failure path.
                    allow_empty=status in {"P", "G"},
                    context=f"{exchange}/{status}",
                )
                for row in rows:
                    ts_code = str(row.get("ts_code") or "")
                    if not ts_code:
                        continue
                    output[ts_code] = {
                        "ts_code": ts_code,
                        "symbol": str(row.get("symbol") or ts_code.split(".")[0]),
                        "name": str(row.get("name") or ""),
                        "market": str(row.get("market") or ""),
                        "exchange": str(row.get("exchange") or exchange),
                        "list_status": str(row.get("list_status") or status),
                        "list_date": _parse_date(row.get("list_date")),
                        "delist_date": _parse_date(row.get("delist_date")),
                    }
        return sorted(output.values(), key=lambda row: row["ts_code"])

    async def fetch_margin(self, day: date) -> list[dict[str, Any]]:
        return await self._query(
            "margin",
            {"trade_date": day.strftime("%Y%m%d")},
            ("trade_date", "exchange_id", "rzye", "rzmre", "rzche"),
            context=day.strftime("%Y%m%d"),
        )

    async def fetch_margin_detail(self, day: date) -> list[dict[str, Any]]:
        return await self._query(
            "margin_detail",
            {"trade_date": day.strftime("%Y%m%d")},
            ("trade_date", "ts_code", "rzye", "rzmre", "rzche"),
            context=day.strftime("%Y%m%d"),
        )

    async def fetch_daily(self, day: date) -> list[dict[str, Any]]:
        rows = await self._query(
            "daily",
            {"trade_date": day.strftime("%Y%m%d")},
            ("trade_date", "ts_code", "close", "pre_close", "pct_chg", "amount"),
            context=day.strftime("%Y%m%d"),
        )
        return [
            {
                "trade_date": day,
                "ts_code": str(row.get("ts_code") or ""),
                "close": _float(row.get("close")),
                "pre_close": _float(row.get("pre_close")),
                "pct_chg": _float(row.get("pct_chg")),
                # Tushare daily.amount is 千元; normalize to yuan.
                "amount": _scaled_float(row.get("amount"), 1000.0),
            }
            for row in rows
        ]

    async def fetch_daily_basic(self, day: date) -> list[dict[str, Any]]:
        rows = await self._query(
            "daily_basic",
            {"trade_date": day.strftime("%Y%m%d")},
            ("trade_date", "ts_code", "close", "free_share", "circ_mv", "total_mv"),
            context=day.strftime("%Y%m%d"),
        )
        return [
            {
                "trade_date": day,
                "ts_code": str(row.get("ts_code") or ""),
                "close": _float(row.get("close")),
                "free_share": _float(row.get("free_share")),
                "circ_mv": _float(row.get("circ_mv")),
                # Tushare total_mv is 万元; normalize for coverage diagnostics.
                "total_mv": _scaled_float(row.get("total_mv"), 10000.0),
            }
            for row in rows
        ]
