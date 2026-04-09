# === MODULE PURPOSE ===
# REST API client for Fake Tushare (tushare.xyz) historical minute data service.
# Provides stk_mins API for historical 1min/5min OHLCV data.
# Used as the data source for backtest minute bars (replacing tsanghi 5min).

# === KEY CONCEPTS ===
# - API endpoint: http://tushare.xyz (NOT api.tushare.pro)
# - Stock codes: tushare format "600519.SH" / "000001.SZ"
# - Volume unit: 股 (shares), returned as string
# - Token: independent from Tushare Pro, configured separately
# - Only has stk_mins permission (no trade_cal, no pro_bar)

import asyncio
import logging
from typing import Any

import httpx

from src.common.config import get_fake_tushare_token

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 60.0
_MAX_RETRIES = 3


class FakeTushareClient:
    """Async HTTP client for tushare.xyz stk_mins API.

    Usage:
        client = FakeTushareClient()
        await client.start()
        bars = await client.stk_mins(
            "600519.SH", "5min", "2026-01-01 09:00:00", "2026-04-08 15:00:00"
        )
        await client.stop()
    """

    API_URL = "http://tushare.xyz"

    def __init__(self, token: str | None = None) -> None:
        self._token = token
        self._client: httpx.AsyncClient | None = None

    @property
    def token(self) -> str:
        if self._token:
            return self._token
        return get_fake_tushare_token()

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _post(self, api_name: str, params: dict[str, Any]) -> list[dict]:
        """Execute POST request with retry logic.

        Returns:
            List of data records (each dict with field names as keys).

        Raises:
            RuntimeError: If the API returns an error or retries are exhausted.
        """
        if self._client is None:
            await self.start()
        assert self._client is not None

        body = {
            "api_name": api_name,
            "token": self.token,
            "params": params,
            "fields": "",
        }

        last_error: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = await self._client.post(self.API_URL, json=body)
                resp.raise_for_status()
                result = resp.json()

                code = result.get("code")
                if code != 0:
                    msg = result.get("msg", "unknown error")
                    raise RuntimeError(f"FakeTushare API error: {msg} (code={code})")

                data = result.get("data")
                if data is None:
                    return []

                # Convert columnar format to list of dicts
                fields = data.get("fields", [])
                items = data.get("items", [])
                return [dict(zip(fields, row)) for row in items]

            except (httpx.TimeoutException, httpx.HTTPStatusError) as exc:
                last_error = exc
                if attempt < _MAX_RETRIES:
                    wait = 2**attempt
                    logger.warning(
                        f"FakeTushare request failed (attempt {attempt}/{_MAX_RETRIES}), "
                        f"retrying in {wait}s: {exc}"
                    )
                    await asyncio.sleep(wait)

        raise RuntimeError(f"FakeTushare request failed after {_MAX_RETRIES} retries: {last_error}")

    async def stk_mins(
        self,
        ts_code: str,
        freq: str = "5min",
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 100000,
    ) -> list[dict]:
        """Fetch minute-level OHLCV for one or more stocks.

        Args:
            ts_code: Tushare-style code(s), e.g. "600519.SH" or
                     comma-separated "600519.SH,000001.SZ"
            freq: "1min" or "5min"
            start_date: "YYYY-MM-DD HH:MM:SS" (optional)
            end_date: "YYYY-MM-DD HH:MM:SS" (optional)
            limit: Max rows to return (default 100000)

        Returns:
            List of dicts with keys: ts_code, trade_time, open, close, high, low, vol, amount.
            vol is in 股 (shares), returned as string from API — caller should convert.
        """
        params: dict[str, Any] = {
            "ts_code": ts_code,
            "freq": freq,
            "limit": str(limit),
        }
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return await self._post("stk_mins", params)


def bare_code_to_ts_code(code: str) -> str:
    """Convert bare stock code to tushare format.

    "600519" → "600519.SH", "000001" → "000001.SZ"
    """
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    if code.startswith(("0", "3")):
        return f"{code}.SZ"
    raise ValueError(f"Cannot determine exchange for stock code: {code}")
