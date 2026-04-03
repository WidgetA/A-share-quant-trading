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
    """Real-time snapshot for a single stock."""

    stock_code: str  # bare 6-digit code
    open_price: float  # day open (first bar's open)
    latest_price: float  # current price (last bar's close)
    high_price: float  # day high
    low_price: float  # day low
    volume: float  # cumulative volume in shares (股)
    amount: float  # cumulative turnover in yuan
    # 9:30-9:40 snapshot (aggregated from rt_min_daily bars)
    early_close: float = 0.0  # last early bar's close (= 9:40 price)
    early_high: float = 0.0  # max high in 9:30-9:40
    early_low: float = 0.0  # min low in 9:30-9:40
    early_volume: float = 0.0  # cumulative volume 9:30-9:40 in shares (股)

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
    2. batch_get_early_quotes(): Uses rt_min_daily (per-stock, all bars) and
       aggregates 9:30-9:40 bars. Used by V15 scan which needs stable early data.

    NOTE: preClose is NOT available from either endpoint.
    The caller must supplement it from historical cache.

    Fail-fast: API errors raise TushareRealtimeError (no fallback).
    """

    API_URL = "http://api.tushare.pro"
    BATCH_SIZE = 500  # rt_min: 1 row per stock, limit 1000
    TIMEOUT = 30.0
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
    # rt_min_daily: per-stock full-day bars, aggregated to early snapshot
    # ------------------------------------------------------------------

    async def batch_get_early_quotes(self, stock_codes: list[str]) -> dict[str, TushareQuote]:
        """
        Fetch 9:30-9:40 aggregated snapshot for multiple stocks via rt_min_daily.

        rt_min_daily returns ALL minute bars for the day (single stock per call).
        This method aggregates bars with time <= 09:40 to produce stable early data
        that is identical regardless of when the call is made.

        Args:
            stock_codes: List of bare 6-digit codes

        Returns:
            Dict: stock_code -> TushareQuote with early_* fields populated
        """
        if not self._client:
            raise TushareRealtimeError("Client not started — call start() first")

        if not stock_codes:
            return {}

        all_quotes: dict[str, TushareQuote] = {}
        sem = asyncio.Semaphore(self.MAX_CONCURRENCY)

        async def _fetch_one(bare_code: str) -> tuple[str, TushareQuote | None]:
            ts_code = self._to_ts_code(bare_code)
            async with sem:
                data = await self._api_call(
                    "rt_min_daily",
                    {"ts_code": ts_code, "freq": "1MIN"},
                    fields="time,open,close,high,low,vol,amount",
                )
            quote = self._parse_rt_min_daily(bare_code, data)
            return bare_code, quote

        results = await asyncio.gather(
            *[_fetch_one(c) for c in stock_codes], return_exceptions=True
        )

        for result in results:
            if isinstance(result, TushareRealtimeError):
                raise result
            if isinstance(result, BaseException):
                raise TushareRealtimeError(f"rt_min_daily failed: {result}") from result
            bare_code, quote = result
            if quote is not None:
                all_quotes[bare_code] = quote

        logger.info(f"rt_min_daily: fetched {len(all_quotes)}/{len(stock_codes)} stocks")
        return all_quotes

    @staticmethod
    def _parse_rt_min_daily(bare_code: str, data: dict[str, Any]) -> TushareQuote | None:
        """
        Parse rt_min_daily response (all bars for one stock) into TushareQuote.

        Produces:
        - Full-day aggregated OHLCV (open/latest/high/low/volume/amount)
        - 9:30-9:40 early snapshot (early_close/early_high/early_low/early_volume)
        """
        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])

        if not fields or not items:
            return None

        idx = {f: i for i, f in enumerate(fields)}
        required = {"open", "close", "high", "low", "vol", "amount"}
        if not required.issubset(idx.keys()):
            return None

        has_time = "time" in idx

        # Full-day aggregation
        try:
            first_open = items[0][idx["open"]]
            last_close = items[-1][idx["close"]]
            if not first_open or not last_close:
                return None

            max_high = max(r[idx["high"]] for r in items if r[idx["high"]] is not None)
            min_low = min(r[idx["low"]] for r in items if r[idx["low"]] is not None)
            total_vol = sum(r[idx["vol"]] for r in items if r[idx["vol"]] is not None)
            total_amount = sum(r[idx["amount"]] for r in items if r[idx["amount"]] is not None)
        except (ValueError, TypeError, IndexError) as e:
            logger.warning(f"Failed to aggregate rt_min_daily for {bare_code}: {e}")
            return None

        # 9:30-9:40 early snapshot
        early_bars = []
        if has_time:
            for r in items:
                t = str(r[idx["time"]])
                # Format: "2026-03-17 09:31:00"
                if " " in t:
                    t = t.split(" ")[-1]
                hhmm = t.replace(":", "")[:4]
                if hhmm <= "0939":
                    early_bars.append(r)

        if early_bars:
            e_close = float(early_bars[-1][idx["close"]])
            e_high = float(max(r[idx["high"]] for r in early_bars if r[idx["high"]] is not None))
            e_low = float(min(r[idx["low"]] for r in early_bars if r[idx["low"]] is not None))
            e_vol = float(sum(r[idx["vol"]] for r in early_bars if r[idx["vol"]] is not None))
        else:
            # Called before 9:30 or no time field — use whatever we have
            e_close = float(last_close)
            e_high = float(max_high) if max_high else 0.0
            e_low = float(min_low) if min_low else 0.0
            e_vol = float(total_vol)

        return TushareQuote(
            stock_code=bare_code,
            open_price=float(first_open),
            latest_price=float(last_close),
            high_price=float(max_high) if max_high else 0.0,
            low_price=float(min_low) if min_low else 0.0,
            volume=float(total_vol),
            amount=float(total_amount),
            early_close=e_close,
            early_high=e_high,
            early_low=e_low,
            early_volume=e_vol,
        )

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
