# === MODULE PURPOSE ===
# Fetches real-time A-share price data from Tushare Pro (rt_min API).
# Replaces the defunct SinaRealtimeClient for the monitor/live scan subsystem.

# === DEPENDENCIES ===
# - httpx: Async HTTP client for Tushare Pro REST API
# - No iFinD or shared resources — fully isolated

# === KEY CONCEPTS ===
# - Tushare Pro API: POST http://api.tushare.pro with JSON body
# - api_name "rt_min": real-time minute bars (1MIN/5MIN/15MIN/30MIN/60MIN)
# - Returns all minute bars for the current trading session
# - Aggregated to day-level snapshot: first open, last close, max high, min low, sum vol
# - Volume (vol field) is in 股 (shares), matching iFinD/baostock convention
# - preClose NOT available from rt_min — must be supplemented by caller
# - Fail-fast: API errors raise TushareRealtimeError (no silent fallback)

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class TushareRealtimeError(Exception):
    """Error from Tushare Pro API (network, auth, or data issue)."""

    pass


@dataclass
class TushareQuote:
    """Aggregated day-level snapshot from Tushare minute bars."""

    stock_code: str  # bare 6-digit code
    open_price: float
    latest_price: float
    high_price: float
    low_price: float
    volume: float  # in shares (股), cumulative
    amount: float  # in yuan, cumulative

    @property
    def is_trading(self) -> bool:
        """Check if the stock has valid trading data (not suspended)."""
        return self.open_price > 0 and self.latest_price > 0


class TushareRealtimeClient:
    """
    Fetches real-time A-share quotes from Tushare Pro (rt_min endpoint).

    Uses minute bars aggregated to day-level snapshots:
    - open_price: first bar's open (= day open)
    - latest_price: last bar's close (= current price)
    - high_price: max of all bars' high (= day high)
    - low_price: min of all bars' low (= day low)
    - volume: sum of all bars' vol (= cumulative volume in shares)
    - amount: sum of all bars' amount (= cumulative turnover)

    NOTE: preClose is NOT available from rt_min.
    The caller must supplement it from historical cache.

    Fail-fast: API errors raise TushareRealtimeError (no fallback).

    Usage:
        client = TushareRealtimeClient(token="your_tushare_pro_token")
        await client.start()
        quotes = await client.batch_get_quotes(["600519", "000001"])
        await client.stop()
    """

    API_URL = "http://api.tushare.pro"
    BATCH_SIZE = 300  # max codes per request (user's subscription limit)
    TIMEOUT = 30.0
    BATCH_DELAY = 0.3  # seconds between batches
    MAX_RETRIES = 3
    RETRY_BACKOFF = 1.0  # base seconds; doubles each attempt

    def __init__(self, token: str) -> None:
        self._token = token
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        """Initialize the HTTP client."""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.TIMEOUT),
        )

    async def stop(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def batch_get_quotes(self, stock_codes: list[str]) -> dict[str, TushareQuote]:
        """
        Fetch real-time quotes for multiple stocks via rt_min.

        Aggregates minute bars into day-level snapshots.

        Args:
            stock_codes: List of bare 6-digit codes (e.g., ["600519", "000001"])

        Returns:
            Dict: stock_code -> TushareQuote (only stocks with valid data)

        Raises:
            TushareRealtimeError: On API failure (fail-fast for trading safety)
        """
        if not self._client:
            raise TushareRealtimeError("Client not started — call start() first")

        if not stock_codes:
            return {}

        all_quotes: dict[str, TushareQuote] = {}

        for i in range(0, len(stock_codes), self.BATCH_SIZE):
            batch = stock_codes[i : i + self.BATCH_SIZE]
            ts_codes = [self._to_ts_code(c) for c in batch]
            ts_code_str = ",".join(ts_codes)

            data = await self._api_call(
                "rt_min",
                {"ts_code": ts_code_str, "freq": "1MIN"},
                fields="ts_code,time,open,close,high,low,vol,amount",
            )

            batch_quotes = self._aggregate_minute_bars(data)
            all_quotes.update(batch_quotes)

            # Delay between batches to stay within rate limits
            if i + self.BATCH_SIZE < len(stock_codes):
                await asyncio.sleep(self.BATCH_DELAY)

        return all_quotes

    async def as_ifind_format(
        self, stock_codes: list[str], indicators: str
    ) -> dict[str, Any]:
        """
        Fetch quotes and return in iFinD real_time_quotation response format.

        This allows MomentumSectorScanner._fetch_prices_realtime()
        to work unchanged by duck-typing the iFinD response shape.

        Args:
            stock_codes: Bare 6-digit codes (or "600519.SH" format)
            indicators: Comma-separated indicator names

        Returns:
            {"errorcode": 0, "tables": [{"thscode": "600519.SH", "table": {...}}, ...]}
        """
        bare_codes = [c.split(".")[0] for c in stock_codes]
        quotes = await self.batch_get_quotes(bare_codes)

        indicator_list = [ind.strip() for ind in indicators.split(",")]
        tables: list[dict[str, Any]] = []

        for bare_code, quote in quotes.items():
            if not quote.is_trading:
                continue

            table_data: dict[str, list] = {}
            for ind in indicator_list:
                val = self._quote_to_indicator(quote, ind)
                table_data[ind] = [val]

            suffix = ".SH" if bare_code.startswith("6") else ".SZ"
            tables.append({"thscode": f"{bare_code}{suffix}", "table": table_data})

        return {"errorcode": 0, "tables": tables}

    async def fetch_prev_closes(self, trade_date: str) -> dict[str, float]:
        """
        Fetch previous trading day's close prices via Tushare 'daily' API.

        Used to supplement preClose when OSS cache is not available.

        Args:
            trade_date: Trade date in YYYYMMDD format (e.g., "20260226").
                Pass the PREVIOUS trading day (not today).

        Returns:
            Dict: bare_code -> close_price (e.g., {"600519": 1850.0})

        Raises:
            TushareRealtimeError: On API failure
        """
        data = await self._api_call(
            "daily",
            {"trade_date": trade_date},
            fields="ts_code,close",
        )

        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])

        if not fields or not items:
            return {}

        idx = {f: i for i, f in enumerate(fields)}
        result: dict[str, float] = {}
        for row in items:
            ts_code = row[idx["ts_code"]]
            close = row[idx["close"]]
            if ts_code and close is not None:
                bare = ts_code.split(".")[0]
                result[bare] = float(close)

        logger.info(
            f"Tushare daily: fetched prev_close for {len(result)} stocks (date={trade_date})"
        )
        return result

    async def get_exchange_time(self) -> tuple[str, str] | None:
        """Not available from Tushare rt_min. Returns None."""
        return None

    async def _api_call(
        self, api_name: str, params: dict[str, Any], fields: str = ""
    ) -> dict[str, Any]:
        """Make a single Tushare Pro HTTP API call with retry."""
        if not self._client:
            raise TushareRealtimeError("Client not started — call start() first")

        body: dict[str, Any] = {
            "api_name": api_name,
            "token": self._token,
            "params": params,
        }
        if fields:
            body["fields"] = fields

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = await self._client.post(self.API_URL, json=body)
                resp.raise_for_status()
                data = resp.json()

                code = data.get("code")
                if code != 0:
                    msg = data.get("msg", "unknown error")
                    raise TushareRealtimeError(
                        f"Tushare API error: code={code}, msg={msg}"
                    )
                return data

            except TushareRealtimeError:
                raise  # Don't retry API-level errors (e.g. auth failure)

            except (httpx.HTTPError, ConnectionError, OSError) as e:
                if attempt < self.MAX_RETRIES:
                    wait = self.RETRY_BACKOFF * (2 ** (attempt - 1))
                    logger.warning(
                        f"Tushare API attempt {attempt}/{self.MAX_RETRIES} "
                        f"failed: {e}; retrying in {wait:.1f}s"
                    )
                    await asyncio.sleep(wait)
                else:
                    raise TushareRealtimeError(
                        f"Tushare API request failed after {self.MAX_RETRIES} "
                        f"attempts: {e}"
                    ) from e

        raise TushareRealtimeError("unreachable")  # all paths raise or return above

    @staticmethod
    def _aggregate_minute_bars(data: dict[str, Any]) -> dict[str, TushareQuote]:
        """
        Aggregate minute bars into day-level snapshots.

        Groups rows by ts_code, then:
        - open = first bar's open
        - latest = last bar's close
        - high = max of all bars' high
        - low = min of all bars' low
        - volume = sum of all bars' vol (cumulative, in shares)
        - amount = sum of all bars' amount (cumulative, in yuan)
        """
        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])

        if not fields or not items:
            return {}

        # Build field index for positional access
        idx = {f: i for i, f in enumerate(fields)}
        required = {"ts_code", "open", "close", "high", "low", "vol", "amount"}
        if not required.issubset(idx.keys()):
            missing = required - idx.keys()
            logger.error(f"Tushare rt_min response missing fields: {missing}")
            return {}

        # Group rows by ts_code
        by_code: dict[str, list[list]] = {}
        for row in items:
            ts_code = row[idx["ts_code"]]
            by_code.setdefault(ts_code, []).append(row)

        quotes: dict[str, TushareQuote] = {}
        for ts_code, rows in by_code.items():
            bare = ts_code.split(".")[0]

            try:
                first_open = rows[0][idx["open"]]
                last_close = rows[-1][idx["close"]]
                max_high = max(
                    r[idx["high"]] for r in rows if r[idx["high"]] is not None
                )
                min_low = min(
                    r[idx["low"]] for r in rows if r[idx["low"]] is not None
                )
                total_vol = sum(
                    r[idx["vol"]] for r in rows if r[idx["vol"]] is not None
                )
                total_amount = sum(
                    r[idx["amount"]] for r in rows if r[idx["amount"]] is not None
                )

                if first_open and last_close:
                    quotes[bare] = TushareQuote(
                        stock_code=bare,
                        open_price=float(first_open),
                        latest_price=float(last_close),
                        high_price=float(max_high) if max_high else 0.0,
                        low_price=float(min_low) if min_low else 0.0,
                        volume=float(total_vol),
                        amount=float(total_amount),
                    )
            except (ValueError, TypeError, IndexError) as e:
                logger.warning(f"Failed to aggregate Tushare bars for {ts_code}: {e}")
                continue

        return quotes

    @staticmethod
    def _quote_to_indicator(quote: TushareQuote, indicator: str) -> float | None:
        """Map iFinD indicator name to TushareQuote field value."""
        mapping: dict[str, float] = {
            "open": quote.open_price,
            "latest": quote.latest_price,
            "close": quote.latest_price,  # alias for real-time
            "high": quote.high_price,
            "low": quote.low_price,
            "volume": quote.volume,
            "amount": quote.amount,
        }
        val = mapping.get(indicator)
        if val is not None:
            return val

        # preClose not available from rt_min
        if indicator == "preClose":
            return None
        if indicator == "changeRatio":
            return None  # Can't compute without preClose
        if indicator == "change":
            return None
        if indicator == "turnoverRatio":
            return None
        if indicator == "upperLimit":
            return None  # Can't compute without preClose
        if indicator == "downLimit":
            return None

        return None

    @staticmethod
    def _to_ts_code(bare_code: str) -> str:
        """Convert bare code to Tushare format: 600519 -> 600519.SH."""
        if bare_code.startswith("6"):
            return f"{bare_code}.SH"
        return f"{bare_code}.SZ"
