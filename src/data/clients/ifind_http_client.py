# === MODULE PURPOSE ===
# iFinD HTTP API client for A-share market data.
# Replaces the legacy iFinDPy SDK with HTTP-based API calls.

# === DEPENDENCIES ===
# - httpx: Async HTTP client
# - config: For refresh_token management

# === KEY CONCEPTS ===
# - refresh_token: Long-lived token from iFinD client (like a password)
# - access_token: Short-lived token (7 days) for API calls
# - Token lifecycle: refresh_token -> access_token -> API calls

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

import httpx

from src.common.config import get_ifind_refresh_token

logger = logging.getLogger(__name__)


class IFinDHttpError(Exception):
    """Exception for iFinD HTTP API errors."""

    def __init__(self, error_code: int, error_msg: str):
        self.error_code = error_code
        self.error_msg = error_msg
        super().__init__(f"iFinD HTTP error {error_code}: {error_msg}")


class IFinDHttpClient:
    """
    iFinD HTTP API client.

    Provides async methods for all iFinD HTTP API endpoints used by this project.
    Handles token management and request retry logic.

    Usage:
        client = IFinDHttpClient()
        await client.start()

        # Fetch historical quotes
        data = await client.history_quotes(
            "600519.SH", "open,high,low,close", "2026-01-01", "2026-01-31"
        )

        # Smart stock picking (iwencai)
        data = await client.smart_stock_picking("今日涨停", "stock")

        await client.stop()
    """

    BASE_URL = "https://quantapi.51ifind.com/api/v1"

    # Token refresh before expiry (1 day before the 7-day expiry)
    TOKEN_REFRESH_BUFFER = timedelta(days=1)
    TOKEN_VALIDITY = timedelta(days=7)

    # Request timeout
    REQUEST_TIMEOUT = 30.0

    # Rate limiting: max 600 requests per minute
    RATE_LIMIT_REQUESTS = 600
    RATE_LIMIT_WINDOW = 60.0  # seconds

    def __init__(self, refresh_token: str | None = None):
        """
        Initialize the iFinD HTTP client.

        Args:
            refresh_token: iFinD refresh token (if None, reads from config)
        """
        self._refresh_token = refresh_token
        self._access_token: str | None = None
        self._token_expiry: datetime | None = None
        self._client: httpx.AsyncClient | None = None

        # Rate limiting state
        self._request_times: list[float] = []
        self._rate_limit_lock = asyncio.Lock()

    async def start(self) -> None:
        """
        Initialize the client and obtain access token.

        Raises:
            ValueError: If refresh_token is not configured
            IFinDHttpError: If token acquisition fails
        """
        # Get refresh_token from config if not provided
        if self._refresh_token is None:
            self._refresh_token = get_ifind_refresh_token()

        # Create HTTP client
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.REQUEST_TIMEOUT),
            headers={"Content-Type": "application/json"},
        )

        # Get initial access token
        await self._ensure_access_token()
        logger.info("iFinD HTTP client started")

    async def stop(self) -> None:
        """Cleanup resources."""
        if self._client:
            await self._client.aclose()
            self._client = None

        self._access_token = None
        self._token_expiry = None
        logger.info("iFinD HTTP client stopped")

    async def _ensure_access_token(self) -> None:
        """Ensure we have a valid access token, refreshing if needed."""
        if self._access_token and self._token_expiry:
            # Check if token is still valid (with buffer)
            if datetime.now() < self._token_expiry - self.TOKEN_REFRESH_BUFFER:
                return

        await self._refresh_access_token()

    async def _refresh_access_token(self) -> None:
        """
        Obtain a new access token using refresh_token.

        Uses /api/v1/get_access_token endpoint.
        """
        if not self._client:
            raise RuntimeError("Client not started. Call start() first.")

        if not self._refresh_token:
            raise RuntimeError("refresh_token not configured")

        url = f"{self.BASE_URL}/get_access_token"
        headers = {
            "Content-Type": "application/json",
            "refresh_token": self._refresh_token,
        }

        try:
            response = await self._client.post(url, headers=headers)
            response.raise_for_status()

            data = response.json()

            if data.get("errorcode", 0) != 0:
                raise IFinDHttpError(
                    data.get("errorcode", -1),
                    data.get("errmsg", "Failed to get access token"),
                )

            self._access_token = data.get("data", {}).get("access_token")
            if not self._access_token:
                raise IFinDHttpError(-1, "No access_token in response")

            self._token_expiry = datetime.now() + self.TOKEN_VALIDITY
            logger.info("iFinD access token refreshed")

        except httpx.HTTPError as e:
            logger.error(f"HTTP error getting access token: {e}")
            raise IFinDHttpError(-1, f"HTTP error: {e}")

    async def _rate_limit_wait(self) -> None:
        """Wait if rate limit would be exceeded."""
        async with self._rate_limit_lock:
            now = asyncio.get_event_loop().time()

            # Remove old request times
            self._request_times = [
                t for t in self._request_times if now - t < self.RATE_LIMIT_WINDOW
            ]

            # Check if we're at the limit
            if len(self._request_times) >= self.RATE_LIMIT_REQUESTS:
                # Wait until the oldest request is outside the window
                oldest = self._request_times[0]
                wait_time = self.RATE_LIMIT_WINDOW - (now - oldest)
                if wait_time > 0:
                    logger.warning(f"Rate limit reached, waiting {wait_time:.1f}s")
                    await asyncio.sleep(wait_time)

            # Record this request
            self._request_times.append(now)

    async def _request(
        self,
        endpoint: str,
        data: dict[str, Any],
        retry_on_token_error: bool = True,
    ) -> dict[str, Any]:
        """
        Make an authenticated request to iFinD API.

        Args:
            endpoint: API endpoint (without base URL)
            data: Request body data
            retry_on_token_error: Whether to retry once on token errors

        Returns:
            Response data as dict

        Raises:
            IFinDHttpError: On API errors
        """
        if not self._client:
            raise RuntimeError("Client not started. Call start() first.")

        await self._ensure_access_token()
        await self._rate_limit_wait()

        # access_token is guaranteed to be set after _ensure_access_token()
        assert self._access_token is not None

        url = f"{self.BASE_URL}/{endpoint}"
        headers = {
            "Content-Type": "application/json",
            "access_token": self._access_token,
        }

        try:
            response = await self._client.post(url, json=data, headers=headers)
            response.raise_for_status()

            result = response.json()

            error_code = result.get("errorcode", 0)
            if error_code != 0:
                # Check for token-related errors
                if error_code in (-1302, -1300) and retry_on_token_error:
                    # Token invalid, try refreshing
                    logger.warning("Access token invalid, refreshing...")
                    self._access_token = None
                    self._token_expiry = None
                    await self._refresh_access_token()
                    return await self._request(endpoint, data, retry_on_token_error=False)

                raise IFinDHttpError(error_code, result.get("errmsg", "Unknown error"))

            return result

        except httpx.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            raise IFinDHttpError(-1, f"HTTP error: {e}")

    async def history_quotes(
        self,
        codes: str,
        indicators: str,
        start_date: str,
        end_date: str,
        function_para: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Fetch historical quotes data.

        Replaces THS_HistoryQuotes SDK function.
        Endpoint: /api/v1/cmd_history_quotation

        Args:
            codes: Comma-separated stock codes (e.g., "600519.SH,000001.SZ")
            indicators: Comma-separated indicators (e.g., "open,high,low,close")
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            function_para: Optional parameters (e.g., {"Interval": "D", "CPS": "1"})

        Returns:
            API response dict with structure:
            {
                "errorcode": 0,
                "tables": [
                    {
                        "thscode": "600519.SH",
                        "table": {
                            "time": ["2026-01-01"],
                            "open": [1850.0],
                            ...
                        }
                    }
                ]
            }
        """
        data: dict[str, Any] = {
            "codes": codes,
            "indicators": indicators,
            "startdate": start_date,
            "enddate": end_date,
        }

        if function_para:
            data["functionpara"] = function_para

        return await self._request("cmd_history_quotation", data)

    async def smart_stock_picking(
        self,
        search_string: str,
        search_type: str = "stock",
    ) -> dict[str, Any]:
        """
        Smart stock picking using natural language query.

        Replaces THS_iwencai SDK function.
        Endpoint: /api/v1/smart_stock_picking

        Args:
            search_string: Natural language query (e.g., "今日涨停", "概念板块")
            search_type: Search type - "stock" for stocks, "zhishu" for indices

        Returns:
            API response dict with structure:
            {
                "errorcode": 0,
                "tables": [
                    {
                        "table": {
                            "股票代码": ["300033.SZ", ...],
                            "股票简称": ["同花顺", ...],
                            ...
                        }
                    }
                ]
            }
        """
        data = {
            "searchstring": search_string,
            "searchtype": search_type,
        }

        return await self._request("smart_stock_picking", data)

    async def real_time_quotation(
        self,
        codes: str,
        indicators: str,
    ) -> dict[str, Any]:
        """
        Fetch real-time quotation data.

        Endpoint: /api/v1/real_time_quotation

        Args:
            codes: Comma-separated stock codes
            indicators: Comma-separated indicators (e.g., "open,high,low,latest")

        Returns:
            API response dict
        """
        data = {
            "codes": codes,
            "indicators": indicators,
        }

        return await self._request("real_time_quotation", data)

    @property
    def is_connected(self) -> bool:
        """Check if client is connected with valid token."""
        return (
            self._client is not None
            and self._access_token is not None
            and self._token_expiry is not None
            and datetime.now() < self._token_expiry
        )
