# === MODULE PURPOSE ===
# REST API client for Tsanghi (沧海数据) financial data service.
# Provides historical daily OHLCV data for A-share stocks.
# Used as the free data source for backtesting (replaces akshare daily download).

# === KEY CONCEPTS ===
# - Exchange codes: XSHG (Shanghai), XSHE (Shenzhen)
# - Stock codes: bare digits (e.g. "600519"), no suffix
# - Volume unit from API: 手 (lots). Caller must ×100 for 股 (shares).
# - Token: obtained from https://tsanghi.com after registration

import asyncio
import logging
from typing import Any

import httpx

from src.common.config import get_tsanghi_token

logger = logging.getLogger(__name__)

# Max concurrent requests to avoid overwhelming the API
_DEFAULT_CONCURRENCY = 10
_DEFAULT_TIMEOUT = 30.0
_MAX_RETRIES = 3


class TsanghiClient:
    """Async HTTP client for tsanghi.com REST API.

    Usage:
        client = TsanghiClient()
        await client.start()
        data = await client.daily("XSHG", "600519", "2026-01-01", "2026-03-01")
        await client.stop()
    """

    BASE_URL = "https://tsanghi.com/api/fin/stock"

    def __init__(self, token: str | None = None) -> None:
        self._token = token
        self._client: httpx.AsyncClient | None = None

    @property
    def token(self) -> str:
        if self._token:
            return self._token
        return get_tsanghi_token()

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(_DEFAULT_TIMEOUT),
                limits=httpx.Limits(
                    max_connections=_DEFAULT_CONCURRENCY,
                    max_keepalive_connections=_DEFAULT_CONCURRENCY,
                ),
            )

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _get(self, url: str, params: dict[str, Any]) -> list[dict]:
        """Execute GET request with retry logic.

        Returns:
            List of data records from the API response.

        Raises:
            RuntimeError: If the API returns an error or retries are exhausted.
        """
        if self._client is None:
            await self.start()
        assert self._client is not None

        params["token"] = self.token
        last_error: Exception | None = None

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = await self._client.get(url, params=params)
                resp.raise_for_status()
                body = resp.json()

                code = body.get("code")
                if code != 200:
                    msg = body.get("msg", "unknown error")
                    raise RuntimeError(f"Tsanghi API error: {msg} (code={code})")

                data = body.get("data")
                if data is None:
                    return []
                return data

            except (httpx.TimeoutException, httpx.HTTPStatusError) as exc:
                last_error = exc
                if attempt < _MAX_RETRIES:
                    wait = 2**attempt
                    logger.warning(
                        f"Tsanghi request failed (attempt {attempt}/{_MAX_RETRIES}), "
                        f"retrying in {wait}s: {exc}"
                    )
                    await asyncio.sleep(wait)
                else:
                    break

        raise RuntimeError(f"Tsanghi API request failed after {_MAX_RETRIES} retries: {last_error}")

    async def daily(
        self,
        exchange: str,
        ticker: str,
        start_date: str | None = None,
        end_date: str | None = None,
        order: int = 1,
    ) -> list[dict]:
        """Fetch daily OHLCV for a single stock.

        Args:
            exchange: "XSHG" or "XSHE"
            ticker: Bare stock code, e.g. "600519"
            start_date: "YYYY-MM-DD" (optional)
            end_date: "YYYY-MM-DD" (optional)
            order: 0=unordered, 1=ascending, 2=descending

        Returns:
            List of dicts with keys: ticker, date, open, high, low, close, volume.
            Volume is in 手 (lots); caller should ×100 for 股 (shares).
        """
        url = f"{self.BASE_URL}/{exchange}/daily"
        params: dict[str, Any] = {"ticker": ticker, "order": order}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return await self._get(url, params)

    async def shares(
        self,
        exchange: str,
        ticker: str,
    ) -> dict | None:
        """Fetch shares outstanding / float for a single stock.

        Args:
            exchange: "XSHG" or "XSHE"
            ticker: Bare stock code, e.g. "600519"

        Returns:
            Dict with keys: ticker, shares_outstanding, shares_float.
            Returns None if no data available.
        """
        url = f"{self.BASE_URL}/{exchange}/shares"
        params: dict[str, Any] = {"ticker": ticker}
        data = await self._get(url, params)
        if data:
            return data[0]
        return None

    async def daily_latest(
        self,
        exchange: str,
        date: str | None = None,
    ) -> list[dict]:
        """Fetch daily OHLCV for ALL stocks on a given date.

        This is the batch endpoint — one API call returns the entire market
        for a single trading day. Used for efficient pre-download.

        Args:
            exchange: "XSHG" or "XSHE"
            date: "YYYY-MM-DD" (optional, defaults to latest trading day)

        Returns:
            List of dicts for all stocks on that date.
            Volume is in 手 (lots); caller should ×100 for 股 (shares).
        """
        url = f"{self.BASE_URL}/{exchange}/daily/latest"
        params: dict[str, Any] = {}
        if date:
            params["date"] = date
        return await self._get(url, params)


def bare_code_to_exchange(code: str) -> str:
    """Map a bare stock code to its tsanghi exchange code.

    Rules:
        6xx, 9xx → XSHG (Shanghai)
        0xx, 3xx → XSHE (Shenzhen)

    Raises:
        ValueError: For unrecognized code prefixes.
    """
    if code.startswith(("6", "9")):
        return "XSHG"
    if code.startswith(("0", "3")):
        return "XSHE"
    raise ValueError(f"Cannot determine exchange for stock code: {code}")


def full_code_to_bare(full_code: str) -> str:
    """Convert '600519.SH' → '600519'."""
    return full_code.split(".")[0]


def bare_to_full_code(bare: str) -> str:
    """Convert '600519' → '600519.SH'."""
    exchange = bare_code_to_exchange(bare)
    suffix = "SH" if exchange == "XSHG" else "SZ"
    return f"{bare}.{suffix}"
