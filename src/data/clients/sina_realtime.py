# === MODULE PURPOSE ===
# Fetches real-time A-share price data from Sina Finance (新浪财经).
# Used exclusively by the iQuant subsystem; the main online system uses iFinD.

# === DEPENDENCIES ===
# - httpx: Async HTTP client for Sina hq API
# - No iFinD or shared resources — fully isolated

# === KEY CONCEPTS ===
# - Sina hq API: GET https://hq.sinajs.cn/list=sh600519,sz000001
# - Response: JS variable assignments with comma-separated fields
# - Field indices: 0=name, 1=open, 2=pre_close, 3=current, 4=high, 5=low,
#                  6=buy1_price, 7=sell1_price, 8=volume(shares), 9=amount(yuan)
# - Volume is in 股 (shares), matching iFinD/baostock convention
# - Fail-fast: network errors raise SinaRealtimeError (no silent fallback)

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class SinaRealtimeError(Exception):
    """Error from Sina Finance API (network, parse, or data issue)."""

    pass


@dataclass
class SinaQuote:
    """Parsed real-time quote from Sina Finance."""

    stock_code: str  # bare 6-digit code
    stock_name: str
    open_price: float
    prev_close: float
    latest_price: float
    high_price: float
    low_price: float
    volume: float  # in shares (股)
    amount: float  # in yuan

    @property
    def is_trading(self) -> bool:
        """Check if the stock has valid trading data (not suspended)."""
        return self.open_price > 0 and self.latest_price > 0


# Regex to parse Sina response lines:
# var hq_str_sh600519="field0,field1,...";
_SINA_LINE_RE = re.compile(r'var hq_str_(\w+)="(.*)";')


class SinaRealtimeClient:
    """
    Fetches real-time A-share quotes from Sina Finance.

    Replaces iFinD real_time_quotation for the iQuant subsystem.
    Sina returns volume in shares (股), matching iFinD convention.

    Fail-fast: network errors raise SinaRealtimeError (no fallback).

    Usage:
        client = SinaRealtimeClient()
        await client.start()
        quotes = await client.batch_get_quotes(["600519", "000001"])
        await client.stop()
    """

    BASE_URL = "https://hq.sinajs.cn"
    BATCH_SIZE = 400  # Keep URL under ~4 KB to avoid HTTP 431
    TIMEOUT = 15.0
    BATCH_DELAY = 0.1  # seconds between batches to avoid rate limiting

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        """Initialize the HTTP client."""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.TIMEOUT),
            headers={
                "Referer": "https://finance.sina.com.cn",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            },
        )

    async def stop(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def get_exchange_time(self) -> tuple[str, str] | None:
        """Probe Sina for the exchange's current date and time.

        Fetches sz000001 (平安银行) and reads the date/time fields
        embedded in the response (last 3rd / 2nd comma-separated fields).
        These come from the exchange, independent of local clock.

        Returns:
            (date_str, time_str) e.g. ("2026-02-24", "09:42:15"),
            or None if the probe fails.
        """
        if not self._client:
            return None

        try:
            resp = await self._client.get(f"{self.BASE_URL}/list=sz000001")
            resp.raise_for_status()
            text = resp.content.decode("gbk", errors="replace")
        except httpx.HTTPError:
            return None

        match = _SINA_LINE_RE.search(text)
        if not match or not match.group(2):
            return None

        fields = match.group(2).split(",")
        if len(fields) < 3:
            return None

        try:
            date_str = fields[-3]  # "2026-02-24"
            time_str = fields[-2]  # "09:42:15"
            # Validate format
            parts = time_str.split(":")
            if len(parts) != 3:
                return None
            return (date_str, time_str)
        except (ValueError, IndexError):
            return None

    async def batch_get_quotes(self, stock_codes: list[str]) -> dict[str, SinaQuote]:
        """
        Fetch real-time quotes for multiple stocks.

        Args:
            stock_codes: List of bare 6-digit codes (e.g., ["600519", "000001"])

        Returns:
            Dict: stock_code -> SinaQuote (only successfully parsed stocks)

        Raises:
            SinaRealtimeError: On network failure (fail-fast for trading safety)
        """
        if not self._client:
            raise SinaRealtimeError("Client not started — call start() first")

        if not stock_codes:
            return {}

        all_quotes: dict[str, SinaQuote] = {}

        for i in range(0, len(stock_codes), self.BATCH_SIZE):
            batch = stock_codes[i : i + self.BATCH_SIZE]
            sina_codes = [self._code_to_sina(c) for c in batch]
            codes_param = ",".join(sina_codes)

            try:
                # Build URL directly — Sina expects /list=code1,code2 with
                # raw commas.  Using params= would URL-encode commas as %2C,
                # tripling separator size and causing HTTP 431 for large lists.
                url = f"{self.BASE_URL}/list={codes_param}"
                resp = await self._client.get(url)
                resp.raise_for_status()
            except httpx.HTTPError as e:
                raise SinaRealtimeError(f"Sina HTTP request failed: {e}") from e

            # Sina returns GBK-encoded content
            text = resp.content.decode("gbk", errors="replace")

            batch_quotes = self._parse_response(text)
            all_quotes.update(batch_quotes)

            # Delay between batches to avoid rate limiting
            if i + self.BATCH_SIZE < len(stock_codes):
                await asyncio.sleep(self.BATCH_DELAY)

        return all_quotes

    async def as_ifind_format(self, stock_codes: list[str], indicators: str) -> dict[str, Any]:
        """
        Fetch quotes and return in iFinD real_time_quotation response format.

        This allows MomentumSectorScanner._fetch_prices_realtime()
        to work unchanged by duck-typing the iFinD response shape.

        Args:
            stock_codes: Bare 6-digit codes
            indicators: Comma-separated indicator names (e.g., "open,preClose,latest")

        Returns:
            {"errorcode": 0, "tables": [{"thscode": "600519.SH", "table": {...}}, ...]}
        """
        # Extract bare codes from "600519.SH" format if needed
        bare_codes = [c.split(".")[0] for c in stock_codes]
        quotes = await self.batch_get_quotes(bare_codes)

        indicator_list = [ind.strip() for ind in indicators.split(",")]
        tables: list[dict[str, Any]] = []

        for bare_code, quote in quotes.items():
            if not quote.is_trading:
                continue

            # Map iFinD indicator names to SinaQuote fields
            table_data: dict[str, list] = {}
            for ind in indicator_list:
                val = self._quote_to_indicator(quote, ind)
                table_data[ind] = [val]

            # Convert bare code to iFinD thscode format
            suffix = ".SH" if bare_code.startswith("6") else ".SZ"
            tables.append({"thscode": f"{bare_code}{suffix}", "table": table_data})

        return {"errorcode": 0, "tables": tables}

    @staticmethod
    def _code_to_sina(bare_code: str) -> str:
        """Convert bare code to Sina format: 600519 -> sh600519."""
        if bare_code.startswith("6"):
            return f"sh{bare_code}"
        return f"sz{bare_code}"

    @staticmethod
    def _sina_to_bare(sina_code: str) -> str:
        """Convert Sina format to bare code: sh600519 -> 600519."""
        return sina_code[2:]

    @staticmethod
    def _parse_response(text: str) -> dict[str, SinaQuote]:
        """Parse Sina hq response text into SinaQuote dict."""
        quotes: dict[str, SinaQuote] = {}

        for line in text.strip().split("\n"):
            line = line.strip()
            if not line:
                continue

            match = _SINA_LINE_RE.match(line)
            if not match:
                continue

            sina_code = match.group(1)
            fields_str = match.group(2)
            if not fields_str:
                # Empty response means stock is suspended or invalid
                continue

            fields = fields_str.split(",")
            if len(fields) < 10:
                logger.warning(f"Sina response too few fields for {sina_code}: {len(fields)}")
                continue

            try:
                bare_code = sina_code[2:]  # sh600519 -> 600519
                quote = SinaQuote(
                    stock_code=bare_code,
                    stock_name=fields[0],
                    open_price=float(fields[1]) if fields[1] else 0.0,
                    prev_close=float(fields[2]) if fields[2] else 0.0,
                    latest_price=float(fields[3]) if fields[3] else 0.0,
                    high_price=float(fields[4]) if fields[4] else 0.0,
                    low_price=float(fields[5]) if fields[5] else 0.0,
                    volume=float(fields[8]) if fields[8] else 0.0,
                    amount=float(fields[9]) if fields[9] else 0.0,
                )
                quotes[bare_code] = quote
            except (ValueError, IndexError) as e:
                logger.warning(f"Failed to parse Sina quote for {sina_code}: {e}")
                continue

        return quotes

    @staticmethod
    def _quote_to_indicator(quote: SinaQuote, indicator: str) -> float | None:
        """Map iFinD indicator name to SinaQuote field value."""
        mapping = {
            "open": quote.open_price,
            "preClose": quote.prev_close,
            "latest": quote.latest_price,
            "high": quote.high_price,
            "low": quote.low_price,
            "volume": quote.volume,
            "amount": quote.amount,
            "close": quote.latest_price,  # alias for real-time
        }
        val = mapping.get(indicator)
        if val is None:
            # Computed indicators
            if indicator == "changeRatio" and quote.prev_close > 0:
                return (quote.latest_price - quote.prev_close) / quote.prev_close * 100
            if indicator == "change":
                return quote.latest_price - quote.prev_close
            if indicator == "turnoverRatio":
                return None  # Not available from Sina basic API
            if indicator == "upperLimit" and quote.prev_close > 0:
                return round(quote.prev_close * 1.1, 2)
            if indicator == "downLimit" and quote.prev_close > 0:
                return round(quote.prev_close * 0.9, 2)
        return val
