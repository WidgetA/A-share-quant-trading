"""Yahoo index charts through an explicitly configured outbound proxy."""

from __future__ import annotations

import asyncio
import hashlib
import math
import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx


class YahooIndexError(RuntimeError):
    """A failed request or a response that cannot be used as index prices."""


def _number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise YahooIndexError("Non-numeric price in chart")
    if not math.isfinite(value):
        raise YahooIndexError("Non-finite price in chart")
    return float(value)


def parse_chart(
    payload: dict, *, symbol: str, market: str, fetched_at: int,
    capability: str | None = None,
) -> tuple[dict, list[dict]]:
    """Validate identity and preserve source clocks; never turn quotes into bars."""
    chart = payload.get("chart", {})
    if chart.get("error"):
        raise YahooIndexError(f"Yahoo chart error: {chart['error']}")
    results = chart.get("result")
    if not results or len(results) != 1:
        raise YahooIndexError("No unique chart result")
    result = results[0]
    meta = result.get("meta", {})
    if meta.get("symbol") != symbol or meta.get("instrumentType") != "INDEX":
        raise YahooIndexError("Chart identity does not match the requested index")
    expected = {"US": ("USD", "America/"), "KR": ("KRW", "Asia/Seoul")}
    if market not in expected:
        raise YahooIndexError("Unsupported market")
    currency, timezone_prefix = expected[market]
    timezone_name = meta.get("exchangeTimezoneName", "")
    if meta.get("currency") != currency or not timezone_name.startswith(timezone_prefix):
        raise YahooIndexError("Currency/timezone does not match the mapped market")
    timezone = ZoneInfo(timezone_name)
    timestamps = result.get("timestamp") or []
    indicators = result.get("indicators", {})
    quotes = indicators.get("quote") or []
    if not timestamps or len(quotes) != 1:
        raise YahooIndexError("No usable chart observations")
    if any(isinstance(stamp, bool) or not isinstance(stamp, int) for stamp in timestamps):
        raise YahooIndexError("Invalid source timestamp")
    if timestamps != sorted(timestamps):
        raise YahooIndexError("Source timestamps are not chronological")
    series = quotes[0]
    for key in ("open", "high", "low", "close", "volume"):
        values = series.get(key)
        if values is not None and len(values) != len(timestamps):
            raise YahooIndexError(f"Misaligned {key} array")
    adjusted = (indicators.get("adjclose") or [{}])[0].get("adjclose")
    if adjusted is not None and len(adjusted) != len(timestamps):
        raise YahooIndexError("Misaligned adjusted-close array")

    def value(key: str, index: int) -> float | None:
        array = series.get(key)
        return _number(array[index]) if array is not None else None

    placeholder_ohl = len(timestamps) == 1 and all(
        value(key, 0) in (None, 0.0) for key in ("open", "high", "low")
    )
    snapshot = len(timestamps) == 1 and capability != "daily_history" and (
        placeholder_ohl or capability == "snapshot_only" or (
            meta.get("firstTradeDate") is None
            and meta.get("regularMarketTime") == timestamps[0]
            and not ({"1mo", "max"} & set(meta.get("validRanges", [])))
        )
    )
    if not snapshot and meta.get("dataGranularity") != "1d":
        raise YahooIndexError(
            "Returned granularity is not daily; requested interval is insufficient"
        )
    regular = meta.get("currentTradingPeriod", {}).get("regular", {})
    current_start, current_end = regular.get("start"), regular.get("end")
    fetched_seconds = fetched_at / 1000
    quote_time = meta.get("regularMarketTime")
    quote_date = (
        datetime.fromtimestamp(quote_time, timezone).date()
        if isinstance(quote_time, int) else None
    )
    points = []
    seen = set()
    for index, stamp in enumerate(timestamps):
        if isinstance(stamp, bool) or not isinstance(stamp, int) or stamp <= 0:
            raise YahooIndexError("Invalid source timestamp")
        if stamp in seen:
            raise YahooIndexError("Duplicate source timestamp")
        seen.add(stamp)
        close = value("close", index)
        if close is not None and close <= 0:
            raise YahooIndexError("Invalid index close")
        ohl = [value(key, index) for key in ("open", "high", "low")]
        if not snapshot and any(item is not None and item <= 0 for item in ohl):
            raise YahooIndexError("Invalid OHLC; zero placeholders are not historical bars")
        if all(item is not None for item in ohl):
            opening, high, low = ohl
            bounds = [item for item in (opening, close, low) if item is not None]
            if not snapshot and (high < max(bounds) or low > min(bounds)):
                raise YahooIndexError("Inconsistent OHLC")
        volume = value("volume", index)
        if volume is not None and volume < 0:
            raise YahooIndexError("Negative volume")
        adj = _number(adjusted[index]) if adjusted is not None else None
        if adj is not None and adj <= 0:
            raise YahooIndexError("Invalid adjusted close")
        trade_date = datetime.fromtimestamp(stamp, timezone).date()
        # A later source session establishes that older bars are historical.
        # Wall-clock session end alone cannot prove a delayed latest bar is final.
        final = None
        if not snapshot and close is not None:
            if quote_date is not None and trade_date < quote_date:
                final = True
            elif (isinstance(current_start, int) and isinstance(current_end, int)
                  and current_start <= fetched_seconds < current_end
                  and trade_date == datetime.fromtimestamp(current_start, timezone).date()):
                final = False
        points.append({
            "provider": "yahoo", "market": market, "symbol": symbol,
            "interval": "quote" if snapshot else "1d",
            "data_kind": "quote_snapshot" if snapshot else "daily_bar",
            "ts": stamp * 1000,
            "name": meta.get("longName") or meta.get("shortName") or symbol,
            "currency": currency, "exchange_timezone": timezone_name,
            "trade_date": trade_date.isoformat(),
            "open": None if snapshot and placeholder_ohl else ohl[0],
            "high": None if snapshot and placeholder_ohl else ohl[1],
            "low": None if snapshot and placeholder_ohl else ohl[2], "close": close,
            "adjusted_close": None if snapshot else adj,
            "volume": None if snapshot and placeholder_ohl else volume,
            "is_final": final, "fetched_at": fetched_at,
        })
    if not points or (
        capability != "daily_history" and all(point["close"] is None for point in points)
    ):
        raise YahooIndexError("Chart contains no non-null prices")
    return meta, points


class YahooChartTransport:
    """Reusable chart HTTP requests with shared cooldown and host failover."""

    def __init__(self, *, proxy: str | None, client: httpx.AsyncClient | None = None):
        self.client = client or httpx.AsyncClient(
            proxy=proxy, trust_env=False, timeout=httpx.Timeout(30, connect=10),
            headers={"User-Agent": "Mozilla/5.0"},
        )
        self._owns_client = client is None
        self._cooldown_until = 0.0
        self._lock = asyncio.Lock()

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def _wait_cooldown(self) -> None:
        while (remaining := self._cooldown_until - time.monotonic()) > 0:
            await asyncio.sleep(min(remaining, 30))

    async def fetch_chart(self, symbol: str, params: dict[str, str]) -> dict:
        last_error = None
        for attempt in range(4):
            await self._wait_cooldown()
            host = ("query1", "query2")[attempt % 2]
            url = f"https://{host}.finance.yahoo.com/v8/finance/chart/{quote(symbol, safe='')}"
            try:
                response = await self.client.get(url, params=params)
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After", "")
                    try:
                        delay = max(1, float(retry_after))
                    except ValueError:
                        try:
                            delay = max(
                                1, parsedate_to_datetime(retry_after).timestamp() - time.time()
                            )
                        except (ValueError, TypeError):
                            delay = 30 * (attempt + 1)
                    async with self._lock:
                        self._cooldown_until = max(self._cooldown_until, time.monotonic() + delay)
                    last_error = YahooIndexError("Yahoo rate limit; shared cooldown applied")
                    continue
                if response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"Yahoo HTTP {response.status_code}",
                        request=response.request, response=response,
                    )
                if response.status_code != 200:
                    raise YahooIndexError(f"Yahoo HTTP {response.status_code} for {symbol}")
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise YahooIndexError("Yahoo returned non-JSON content") from exc
                fetched_at = int(datetime.now(UTC).timestamp() * 1000)
                return {
                    "payload": payload,
                    "raw_json": response.text,
                    "payload_sha256": hashlib.sha256(response.content).hexdigest(),
                    "url": str(response.url), "fetched_at": fetched_at,
                }
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                last_error = exc
                await asyncio.sleep(min(2 ** attempt, 8))
        raise YahooIndexError(f"Request failed for {symbol}: {last_error}") from last_error


class YahooIndexClient(YahooChartTransport):
    """Daily/quote charts using the shared transport and unchanged daily parser."""

    async def fetch(
        self, symbol: str, market: str, *, start: int | None = None,
        capability: str | None = None,
    ) -> dict:
        params = {"interval": "1d"}
        # range=max silently returns monthly data even with interval=1d.
        params.update(period1=str(0 if start is None else start), period2=str(int(time.time())))
        source = await self.fetch_chart(symbol, params)
        metadata, points = parse_chart(
            source.pop("payload"), symbol=symbol, market=market,
            fetched_at=source["fetched_at"], capability=capability,
        )
        return {
            "metadata": metadata, "points": points,
            "missing_close_timestamps": [p["ts"] for p in points if p["close"] is None],
            **source,
        }
