# === MODULE PURPOSE ===
# Fetches real-time A-share price data from Tushare Pro.
# Replaces the defunct SinaRealtimeClient for the monitor/live scan subsystem.

# === DEPENDENCIES ===
# - httpx: Async HTTP client for Tushare Pro REST API

# === KEY CONCEPTS ===
# - Tushare Pro API: POST http://api.tushare.pro with JSON body
# - TWO minute-bar endpoints:
#   * rt_min: returns 1 bar per stock (latest snapshot), supports batch query
#   * rt_min_daily: returns ALL bars for the day, single stock per call
# - Volume (vol field) is in 股 (shares) for both endpoints
# - preClose NOT available — must be supplemented by caller
# - Fail-fast: API errors raise TushareRealtimeError (no silent fallback)
#
# === DATA / STRATEGY DECOUPLING ===
# This module is the DATA LAYER. It returns raw bars in standard
# Tushare ``stk_mins`` format (``trade_time``, ``open``, ``high``, ``low``,
# ``close``, ``vol``, ``amount``). It does NOT aggregate any window — that
# is the strategy layer's job (see ``src.strategy.aggregators``).

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
    """Real-time snapshot for a single stock from rt_min (1 bar/stock).

    Pure full-day OHLCV — no strategy-window aggregation. Strategies that
    need an early-window snapshot (e.g. 09:31~09:40) should fetch raw bars
    via ``batch_get_minute_bars()`` and run an aggregator from
    ``src.strategy.aggregators``.
    """

    stock_code: str  # bare 6-digit code
    open_price: float  # day open (first bar's open)
    latest_price: float  # current price (last bar's close)
    high_price: float  # day high
    low_price: float  # day low
    volume: float  # cumulative volume in shares (股)
    amount: float  # cumulative turnover in yuan

    @property
    def is_trading(self) -> bool:
        """Check if the stock has valid trading data (not suspended)."""
        return self.open_price > 0 and self.latest_price > 0


class TushareRealtimeClient:
    """
    Fetches real-time A-share quotes from Tushare Pro.

    Two modes:
    1. batch_get_quotes(): Uses rt_min (batch, 1 bar/stock) for current snapshot.
       Used by as_standard_quote_format() for realtime quotation adapter.
    2. batch_get_minute_bars(): Uses rt_min_daily (per-stock, all bars).
       Returns RAW 1-min bars in stk_mins format. Strategy layer aggregates
       whatever window it needs (e.g. via EarlyWindowAggregator).

    NOTE: preClose is NOT available from either endpoint.
    The caller must supplement it from historical cache.

    Fail-fast: API errors raise TushareRealtimeError (no fallback).
    """

    API_URL = "http://api.tushare.pro"
    BATCH_SIZE = 500  # rt_min: 1 row per stock, limit 1000
    TIMEOUT = 60.0
    MAX_CONCURRENCY = 40
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

    # ------------------------------------------------------------------
    # rt_min: batch current snapshot (1 bar per stock)
    # ------------------------------------------------------------------

    async def batch_get_quotes(self, stock_codes: list[str]) -> dict[str, TushareQuote]:
        """
        Fetch current snapshot for multiple stocks via rt_min.

        rt_min returns exactly 1 bar per stock (the latest minute bar).
        Volume/amount in that bar are for that single minute only, NOT cumulative.

        Args:
            stock_codes: List of bare 6-digit codes (e.g., ["600519", "000001"])

        Returns:
            Dict: stock_code -> TushareQuote (only stocks with valid data)
        """
        if not self._client:
            raise TushareRealtimeError("Client not started — call start() first")

        if not stock_codes:
            return {}

        all_quotes: dict[str, TushareQuote] = {}
        sem = asyncio.Semaphore(self.MAX_CONCURRENCY)

        async def _fetch_batch(batch: list[str]) -> dict[str, TushareQuote]:
            ts_codes = [self._to_ts_code(c) for c in batch]
            ts_code_str = ",".join(ts_codes)
            async with sem:
                data = await self._api_call(
                    "rt_min",
                    {"ts_code": ts_code_str, "freq": "1MIN"},
                    fields="ts_code,time,open,close,high,low,vol,amount",
                )
            return self._parse_rt_min(data)

        batches = [
            stock_codes[i : i + self.BATCH_SIZE]
            for i in range(0, len(stock_codes), self.BATCH_SIZE)
        ]
        results = await asyncio.gather(*[_fetch_batch(b) for b in batches])
        for batch_quotes in results:
            all_quotes.update(batch_quotes)

        return all_quotes

    @staticmethod
    def _parse_rt_min(data: dict[str, Any]) -> dict[str, TushareQuote]:
        """Parse rt_min response (1 bar per stock) into TushareQuote dict."""
        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])

        if not fields or not items:
            return {}

        idx = {f: i for i, f in enumerate(fields)}
        required = {"ts_code", "open", "close", "high", "low", "vol", "amount"}
        if not required.issubset(idx.keys()):
            missing = required - idx.keys()
            logger.error(f"Tushare rt_min response missing fields: {missing}")
            return {}

        quotes: dict[str, TushareQuote] = {}
        for row in items:
            ts_code = row[idx["ts_code"]]
            bare = ts_code.split(".")[0]
            try:
                o = row[idx["open"]]
                c = row[idx["close"]]
                if not o or not c:
                    continue
                quotes[bare] = TushareQuote(
                    stock_code=bare,
                    open_price=float(o),
                    latest_price=float(c),
                    high_price=float(row[idx["high"]]) if row[idx["high"]] else 0.0,
                    low_price=float(row[idx["low"]]) if row[idx["low"]] else 0.0,
                    volume=float(row[idx["vol"]]) if row[idx["vol"]] else 0.0,
                    amount=float(row[idx["amount"]]) if row[idx["amount"]] else 0.0,
                )
            except (ValueError, TypeError, IndexError) as e:
                logger.warning(f"Failed to parse rt_min bar for {ts_code}: {e}")
                continue

        return quotes

    # ------------------------------------------------------------------
    # rt_min_daily: per-stock full-day raw 1-min bars
    # ------------------------------------------------------------------

    async def batch_get_minute_bars(
        self, stock_codes: list[str]
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Fetch all 1-min bars for the current trading day via rt_min_daily.

        rt_min_daily returns ALL minute bars for the day (single stock per call).
        This method returns RAW bars only — no window aggregation. Strategy
        services aggregate whichever window they need (e.g. 09:31~09:40) via
        ``src.strategy.aggregators.EarlyWindowAggregator``.

        Args:
            stock_codes: List of bare 6-digit codes

        Returns:
            Dict: stock_code -> list of bar dicts in standard stk_mins format
                  (``trade_time``, ``open``, ``high``, ``low``, ``close``,
                  ``vol``, ``amount``). Stocks with no data are omitted.
        """
        if not self._client:
            raise TushareRealtimeError("Client not started — call start() first")

        if not stock_codes:
            return {}

        all_bars: dict[str, list[dict[str, Any]]] = {}
        sem = asyncio.Semaphore(self.MAX_CONCURRENCY)

        async def _fetch_one(bare_code: str) -> tuple[str, list[dict[str, Any]]]:
            ts_code = self._to_ts_code(bare_code)
            async with sem:
                data = await self._api_call(
                    "rt_min_daily",
                    {"ts_code": ts_code, "freq": "1MIN"},
                    fields="time,open,close,high,low,vol,amount",
                )
            return bare_code, self._parse_rt_min_daily_bars(data)

        results = await asyncio.gather(
            *[_fetch_one(c) for c in stock_codes], return_exceptions=True
        )

        for result in results:
            if isinstance(result, TushareRealtimeError):
                raise result
            if isinstance(result, BaseException):
                raise TushareRealtimeError(f"rt_min_daily failed: {result}") from result
            bare_code, bars = result
            if bars:
                all_bars[bare_code] = bars

        logger.info(f"rt_min_daily: fetched bars for {len(all_bars)}/{len(stock_codes)} stocks")
        return all_bars

    @staticmethod
    def _parse_rt_min_daily_bars(data: dict[str, Any]) -> list[dict[str, Any]]:
        """Parse rt_min_daily response into a list of raw 1-min bars.

        Returns bars in standard Tushare ``stk_mins`` format so the same
        downstream aggregator code works for both live and backtest paths:
        ``{trade_time, open, high, low, close, vol, amount}``.
        """
        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])

        if not fields or not items:
            return []

        idx = {f: i for i, f in enumerate(fields)}
        required = {"time", "open", "close", "high", "low", "vol", "amount"}
        if not required.issubset(idx.keys()):
            return []

        bars: list[dict[str, Any]] = []
        for row in items:
            try:
                bars.append(
                    {
                        "trade_time": row[idx["time"]],
                        "open": row[idx["open"]],
                        "high": row[idx["high"]],
                        "low": row[idx["low"]],
                        "close": row[idx["close"]],
                        "vol": row[idx["vol"]],
                        "amount": row[idx["amount"]],
                    }
                )
            except IndexError:
                continue

        return bars

    # ------------------------------------------------------------------
    # Response format adapter (used by IQuantHistoricalAdapter)
    # ------------------------------------------------------------------

    async def as_standard_quote_format(
        self, stock_codes: list[str], indicators: str
    ) -> dict[str, Any]:
        """
        Fetch quotes and return in standard quotation response format.

        Uses rt_min (batch, current snapshot) for real-time quotes.
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

    # ------------------------------------------------------------------
    # Tushare daily API (for prev_close)
    # ------------------------------------------------------------------

    async def fetch_prev_closes(self, trade_date: str) -> dict[str, float]:
        """
        Fetch previous trading day's close prices via Tushare 'daily' API.

        Args:
            trade_date: Trade date in YYYYMMDD format (the PREVIOUS trading day).

        Returns:
            Dict: bare_code -> close_price

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
        """Not available from Tushare. Returns None."""
        return None

    # ------------------------------------------------------------------
    # stk_mins: historical minute bars (for backtest cache)
    # ------------------------------------------------------------------

    async def stk_mins(
        self,
        ts_code: str,
        freq: str = "1min",
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 100000,
    ) -> list[dict[str, Any]]:
        """Fetch historical minute OHLCV for one or more stocks.

        Args:
            ts_code: Tushare-style code(s), e.g. "600519.SH" or
                     comma-separated "600519.SH,000001.SZ"
            freq: "1min" or "5min"
            start_date: "YYYY-MM-DD HH:MM:SS" (optional)
            end_date: "YYYY-MM-DD HH:MM:SS" (optional)
            limit: Max rows to return (default 100000)

        Returns:
            List of dicts with keys: ts_code, trade_time, open, close,
            high, low, vol, amount.
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

        data = await self._api_call("stk_mins", params)
        raw = data.get("data")
        if raw is None:
            return []
        fields = raw.get("fields", [])
        items = raw.get("items", [])
        return [dict(zip(fields, row)) for row in items]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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
                    raise TushareRealtimeError(f"Tushare API error: code={code}, msg={msg}")
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
                        f"Tushare API request failed after {self.MAX_RETRIES} attempts: {e}"
                    ) from e

        raise TushareRealtimeError("unreachable")  # all paths raise or return above

    @staticmethod
    def _quote_to_indicator(quote: TushareQuote, indicator: str) -> float | None:
        """Map indicator name to TushareQuote field value."""
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

        # Indicators not available from rt_min
        if indicator in (
            "preClose",
            "changeRatio",
            "change",
            "turnoverRatio",
            "upperLimit",
            "downLimit",
        ):
            return None

        return None

    async def fetch_trade_calendar(self, start_date: str, end_date: str) -> list[str]:
        """Fetch trading dates via Tushare trade_cal API.

        Args:
            start_date: YYYYMMDD format
            end_date: YYYYMMDD format

        Returns:
            Sorted list of trading dates in YYYY-MM-DD format.
        """
        data = await self._api_call(
            "trade_cal",
            {"start_date": start_date, "end_date": end_date, "is_open": "1"},
            fields="cal_date",
        )

        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])

        if not fields or not items:
            return []

        idx = fields.index("cal_date")
        dates: list[str] = []
        for row in items:
            raw = str(row[idx])
            # Tushare returns YYYYMMDD — convert to YYYY-MM-DD
            if len(raw) == 8:
                dates.append(f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}")
            else:
                dates.append(raw)

        return sorted(dates)

    async def fetch_suspended_stocks(self, trade_date: str) -> set[str]:
        """Fetch all suspended stock codes for a given date via Tushare suspend_d.

        Args:
            trade_date: YYYYMMDD format

        Returns:
            Set of bare 6-digit stock codes that are suspended on that date.
        """
        data = await self._api_call(
            "suspend_d",
            {"trade_date": trade_date, "suspend_type": "S"},
            fields="ts_code",
        )

        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])

        if not fields or not items:
            return set()

        idx = fields.index("ts_code")
        codes: set[str] = set()
        for row in items:
            ts_code = str(row[idx])
            bare = ts_code.split(".")[0]
            if len(bare) == 6:
                codes.add(bare)

        return codes

    async def fetch_stock_list(self) -> list[str]:
        """Fetch all listed A-share stock codes via Tushare stock_basic.

        Returns:
            List of bare 6-digit stock codes (e.g. ["000001", "600519", ...]).
        """
        data = await self._api_call(
            "stock_basic",
            {"list_status": "L"},
            fields="ts_code",
        )

        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])

        if not fields or not items:
            return []

        idx = fields.index("ts_code")
        codes: list[str] = []
        for row in items:
            ts_code = str(row[idx])
            bare = ts_code.split(".")[0]
            if len(bare) == 6:
                codes.append(bare)

        return codes

    async def fetch_bak_basic(self, trade_date: str) -> list[str]:
        """Fetch all listed stock codes for a given date via Tushare bak_basic.

        This is the authoritative stock list for any historical trading date
        (from 2016). Used as the ground truth for cache completeness audits.

        Args:
            trade_date: YYYYMMDD format

        Returns:
            List of bare 6-digit stock codes listed on that date.
        """
        data = await self._api_call(
            "bak_basic",
            {"trade_date": trade_date},
            fields="ts_code",
        )

        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])

        if not fields or not items:
            return []

        idx = fields.index("ts_code")
        codes: list[str] = []
        for row in items:
            ts_code = str(row[idx])
            bare = ts_code.split(".")[0]
            if len(bare) == 6:
                codes.append(bare)

        return codes

    @staticmethod
    def _to_ts_code(bare_code: str) -> str:
        """Convert bare code to Tushare format: 600519 -> 600519.SH."""
        if bare_code.startswith("6"):
            return f"{bare_code}.SH"
        return f"{bare_code}.SZ"


async def get_tushare_trade_calendar(
    start_date: str, end_date: str, token: str | None = None
) -> list[str]:
    """Standalone helper to fetch trading calendar via Tushare trade_cal.

    Creates a temporary TushareRealtimeClient, fetches the calendar, and closes.

    Args:
        start_date: YYYY-MM-DD format (converted internally to YYYYMMDD)
        end_date: YYYY-MM-DD format (converted internally to YYYYMMDD)
        token: Tushare token. If None, reads from config.

    Returns:
        Sorted list of trading dates in YYYY-MM-DD format.
    """
    if token is None:
        from src.common.config import get_tushare_token

        token = get_tushare_token()

    # Convert YYYY-MM-DD → YYYYMMDD for Tushare API
    sd = start_date.replace("-", "")
    ed = end_date.replace("-", "")

    client = TushareRealtimeClient(token=token)
    await client.start()
    try:
        return await client.fetch_trade_calendar(sd, ed)
    finally:
        await client.stop()


async def get_tushare_suspended_stocks(trade_date: str, token: str | None = None) -> set[str]:
    """Standalone helper to fetch suspended stocks for a date via Tushare suspend_d.

    Args:
        trade_date: YYYY-MM-DD format (converted internally to YYYYMMDD)
        token: Tushare token. If None, reads from config.

    Returns:
        Set of bare 6-digit stock codes suspended on that date.
    """
    if token is None:
        from src.common.config import get_tushare_token

        token = get_tushare_token()

    td = trade_date.replace("-", "")

    client = TushareRealtimeClient(token=token)
    await client.start()
    try:
        return await client.fetch_suspended_stocks(td)
    finally:
        await client.stop()


async def get_bak_basic_stocks(trade_date: str, token: str | None = None) -> list[str]:
    """Standalone helper to fetch all listed stock codes for a date via bak_basic.

    Args:
        trade_date: YYYY-MM-DD format (converted internally to YYYYMMDD)
        token: Tushare token. If None, reads from config.

    Returns:
        List of bare 6-digit stock codes listed on that date.
    """
    if token is None:
        from src.common.config import get_tushare_token

        token = get_tushare_token()

    td = trade_date.replace("-", "")

    client = TushareRealtimeClient(token=token)
    await client.start()
    try:
        return await client.fetch_bak_basic(td)
    finally:
        await client.stop()
