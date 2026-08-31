# === MODULE PURPOSE ===
# Fetches real-time A-share price data from Tushare Pro.
# Replaces the defunct SinaRealtimeClient for the monitor/live scan subsystem.

# === DEPENDENCIES ===
# - httpx: Async HTTP client for Tushare Pro REST API
# - No iFinD or shared resources — fully isolated

# === KEY CONCEPTS ===
# - Tushare Pro API: POST https://api.tushare.pro with JSON body
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
from datetime import date, datetime, timedelta
from math import isfinite
from typing import Any
from zoneinfo import ZoneInfo

import httpx

logger = logging.getLogger(__name__)
BEIJING_TZ = ZoneInfo("Asia/Shanghai")


class TushareRealtimeError(Exception):
    """Error from Tushare Pro API (network, auth, or data issue)."""

    pass


def _strict_float(value: Any) -> float:
    """Convert a vendor number without accepting JSON booleans as 0/1."""

    if isinstance(value, bool):
        raise ValueError("boolean is not a numeric market-data value")
    return float(value)


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
    volume_937: float = 0.0  # call auction + first 7min (≤09:37) in shares (股)

    @property
    def is_trading(self) -> bool:
        """Check if the stock has valid trading data (not suspended)."""
        return self.open_price > 0 and self.latest_price > 0


@dataclass(frozen=True)
class TushareMinuteBar:
    """One immutable Tushare minute bar with its raw end label preserved.

    V20 deliberately does not infer a label from the request time.  ``bar_end``
    is the timestamp returned by Tushare and ``end_label`` is its ``HH:MM``
    component.  Volume is in shares and amount is in yuan, matching ``rt_min``.
    """

    stock_code: str
    bar_end: datetime
    end_label: str
    open_price: float
    close_price: float
    high_price: float
    low_price: float
    volume: float
    amount: float

    @property
    def is_valid(self) -> bool:
        prices = (
            self.open_price,
            self.close_price,
            self.high_price,
            self.low_price,
        )
        flows = (self.volume, self.amount)
        aware = self.bar_end.tzinfo is not None and self.bar_end.utcoffset() is not None
        return (
            bool(self.stock_code)
            and aware
            and self.end_label == self.bar_end.astimezone(BEIJING_TZ).strftime("%H:%M")
            and all(isfinite(value) and value > 0 for value in prices)
            and all(isfinite(value) and value >= 0 for value in flows)
            and self.low_price <= min(self.open_price, self.close_price)
            and self.high_price >= max(self.open_price, self.close_price)
            and self.low_price <= self.high_price
        )


@dataclass(frozen=True)
class TushareDailyBar:
    """Daily price/turnover row normalized to yuan for V20."""

    stock_code: str
    trade_date: str
    close_price: float
    amount_yuan: float


class TushareRealtimeClient:
    """
    Fetches real-time A-share quotes from Tushare Pro.

    Two modes:
    1. batch_get_quotes(): Uses rt_min (batch, 1 bar/stock) for current snapshot.
       Used by as_ifind_format() for MomentumSectorScanner.
    2. batch_get_early_quotes(): Uses rt_min_daily (per-stock, all bars) and
       aggregates 9:30-9:40 bars. Used by V15 scan which needs stable early data.

    NOTE: preClose is NOT available from either endpoint.
    The caller must supplement it from historical cache.

    Fail-fast: API errors raise TushareRealtimeError (no fallback).
    """

    API_URL = "https://api.tushare.pro"
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

    async def batch_get_latest_minute_bars(
        self,
        stock_codes: list[str],
    ) -> dict[str, TushareMinuteBar]:
        """Fetch one latest raw minute bar per stock without dropping its label.

        This is the batch primitive used by V20's early-bar collector.  A
        duplicate code with conflicting content, a missing timestamp, or an
        invalid OHLCV row is rejected rather than resolved by response order.
        Callers still have to require the exact expected label (for example
        ``09:39``); this method never treats an older row as current.
        """
        if not self._client:
            raise TushareRealtimeError("Client not started — call start() first")
        stock_codes = list(dict.fromkeys(stock_codes))
        if not stock_codes:
            return {}

        sem = asyncio.Semaphore(self.MAX_CONCURRENCY)

        async def _fetch_batch(batch: list[str]) -> dict[str, TushareMinuteBar]:
            ts_codes = [self._to_ts_code(c) for c in batch]
            async with sem:
                data = await self._api_call(
                    "rt_min",
                    {"ts_code": ",".join(ts_codes), "freq": "1MIN"},
                    fields="ts_code,time,open,close,high,low,vol,amount",
                )
            return self._parse_minute_bars(data)

        batches = [
            stock_codes[index : index + self.BATCH_SIZE]
            for index in range(0, len(stock_codes), self.BATCH_SIZE)
        ]
        combined: dict[str, TushareMinuteBar] = {}
        responses = await asyncio.gather(
            *[_fetch_batch(batch) for batch in batches],
            return_exceptions=True,
        )
        successful_batches = 0
        for batch, parsed in zip(batches, responses, strict=True):
            if isinstance(parsed, BaseException):
                logger.warning(
                    "rt_min batch failed for %d codes; successful sibling batches retained: %s",
                    len(batch),
                    parsed,
                )
                continue
            successful_batches += 1
            for code, bar in parsed.items():
                previous = combined.get(code)
                if previous is not None and previous != bar:
                    raise TushareRealtimeError(
                        f"rt_min returned conflicting duplicate rows for {code}"
                    )
                combined[code] = bar
        if successful_batches == 0:
            raise TushareRealtimeError("all rt_min minute-bar batches failed")
        return combined

    async def batch_get_minute_history(
        self,
        stock_codes: list[str],
    ) -> dict[str, tuple[TushareMinuteBar, ...]]:
        """Fetch each stock's current-day minute history with raw labels intact."""
        if not self._client:
            raise TushareRealtimeError("Client not started — call start() first")
        stock_codes = list(dict.fromkeys(stock_codes))
        if not stock_codes:
            return {}

        sem = asyncio.Semaphore(self.MAX_CONCURRENCY)

        async def _fetch_one(code: str) -> tuple[str, tuple[TushareMinuteBar, ...]]:
            async with sem:
                data = await self._api_call(
                    "rt_min_daily",
                    {"ts_code": self._to_ts_code(code), "freq": "1MIN"},
                    fields="time,open,close,high,low,vol,amount",
                )
            return code, self._parse_minute_history(code, data)

        rows = await asyncio.gather(
            *[_fetch_one(code) for code in stock_codes],
            return_exceptions=True,
        )
        result: dict[str, tuple[TushareMinuteBar, ...]] = {}
        successful_codes = 0
        for row in rows:
            if isinstance(row, BaseException):
                logger.warning(
                    "rt_min_daily code failed; successful sibling histories retained: %s",
                    row,
                )
                continue
            successful_codes += 1
            code, bars = row
            result[code] = bars
        if successful_codes == 0:
            raise TushareRealtimeError("all rt_min_daily minute-history requests failed")
        return result

    async def batch_get_minute_history_for_date(
        self,
        stock_codes: list[str],
        trade_date: date,
    ) -> dict[str, tuple[TushareMinuteBar, ...]]:
        """Fetch a closed historical trade day's 1-minute bars via ``stk_mins``.

        This is the restart-recovery path for D1 exit scans.  It is deliberately
        separate from ``rt_min_daily``, whose contract only covers the current
        exchange day.
        """

        if not self._client:
            raise TushareRealtimeError("Client not started — call start() first")
        stock_codes = list(dict.fromkeys(stock_codes))
        if not stock_codes:
            return {}
        start = f"{trade_date.isoformat()} 09:30:00"
        end = f"{trade_date.isoformat()} 15:01:00"
        sem = asyncio.Semaphore(self.MAX_CONCURRENCY)

        async def _fetch_one(code: str) -> tuple[str, tuple[TushareMinuteBar, ...]]:
            async with sem:
                data = await self._api_call(
                    "stk_mins",
                    {
                        "ts_code": self._to_ts_code(code),
                        "freq": "1min",
                        "start_date": start,
                        "end_date": end,
                    },
                    fields="ts_code,trade_time,open,close,high,low,vol,amount",
                )
            return code, self._parse_historical_minute_history(code, trade_date, data)

        rows = await asyncio.gather(
            *[_fetch_one(code) for code in stock_codes],
            return_exceptions=True,
        )
        result: dict[str, tuple[TushareMinuteBar, ...]] = {}
        for row in rows:
            if isinstance(row, BaseException):
                logger.warning(
                    "stk_mins code failed; successful sibling histories retained: %s",
                    row,
                )
                continue
            code, bars = row
            result[code] = bars
        return result

    @staticmethod
    def _parse_minute_bars(
        data: dict[str, Any],
        *,
        default_code: str | None = None,
    ) -> dict[str, TushareMinuteBar]:
        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])
        if not fields or not items:
            return {}
        index = {field: position for position, field in enumerate(fields)}
        required = {"time", "open", "close", "high", "low", "vol", "amount"}
        if not required.issubset(index):
            missing = ", ".join(sorted(required - set(index)))
            raise TushareRealtimeError(f"minute response missing fields: {missing}")
        if default_code is None and "ts_code" not in index:
            raise TushareRealtimeError("minute response missing ts_code")

        parsed: dict[str, TushareMinuteBar] = {}
        conflicted_codes: set[str] = set()
        for item in items:
            try:
                code = default_code or str(item[index["ts_code"]]).split(".")[0]
                if len(code) != 6 or not code.isdigit():
                    raise ValueError(f"invalid ts_code: {code!r}")
                raw_time = str(item[index["time"]]).strip()
                bar_end = TushareRealtimeClient._parse_bar_end(raw_time)
                bar = TushareMinuteBar(
                    stock_code=code,
                    bar_end=bar_end,
                    end_label=bar_end.strftime("%H:%M"),
                    open_price=_strict_float(item[index["open"]]),
                    close_price=_strict_float(item[index["close"]]),
                    high_price=_strict_float(item[index["high"]]),
                    low_price=_strict_float(item[index["low"]]),
                    volume=_strict_float(item[index["vol"]]),
                    amount=_strict_float(item[index["amount"]]),
                )
            except (IndexError, TypeError, ValueError) as exc:
                logger.warning("ignored invalid rt_min row %r: %s", item, exc)
                continue
            if not bar.is_valid:
                logger.warning(
                    "ignored invalid OHLCV rt_min row for %s at %s",
                    code,
                    bar.end_label,
                )
                continue
            if code in conflicted_codes:
                continue
            previous = parsed.get(code)
            if previous is not None and previous != bar:
                # A single response has no durable arrival order with which to
                # arbitrate two legal candidates.  Drop only that code; valid
                # sibling stocks in the same vendor batch remain usable.
                parsed.pop(code, None)
                conflicted_codes.add(code)
                logger.warning(
                    "ignored conflicting duplicate rt_min rows for %s",
                    code,
                )
                continue
            parsed[code] = bar
        return parsed

    @staticmethod
    def _parse_minute_history(
        code: str,
        data: dict[str, Any],
    ) -> tuple[TushareMinuteBar, ...]:
        """Parse a single-stock daily response without collapsing its rows."""
        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])
        if not fields or not items:
            return ()
        if len(code) != 6 or not code.isdigit():
            raise ValueError(f"invalid bare A-share code: {code!r}")
        index = {field: position for position, field in enumerate(fields)}
        required = {"time", "open", "close", "high", "low", "vol", "amount"}
        if not required.issubset(index):
            missing = ", ".join(sorted(required - set(index)))
            raise TushareRealtimeError(f"minute response missing fields: {missing}")

        by_timestamp: dict[datetime, TushareMinuteBar] = {}
        conflicted_timestamps: set[datetime] = set()
        for item in items:
            try:
                bar_end = TushareRealtimeClient._parse_bar_end(str(item[index["time"]]).strip())
                bar = TushareMinuteBar(
                    stock_code=code,
                    bar_end=bar_end,
                    end_label=bar_end.strftime("%H:%M"),
                    open_price=_strict_float(item[index["open"]]),
                    close_price=_strict_float(item[index["close"]]),
                    high_price=_strict_float(item[index["high"]]),
                    low_price=_strict_float(item[index["low"]]),
                    volume=_strict_float(item[index["vol"]]),
                    amount=_strict_float(item[index["amount"]]),
                )
            except (IndexError, TypeError, ValueError) as exc:
                logger.warning(
                    "ignored invalid minute-history row for %s: %r (%s)",
                    code,
                    item,
                    exc,
                )
                continue
            if not bar.is_valid:
                logger.warning(
                    "ignored invalid OHLCV minute-history row for %s at %s",
                    code,
                    bar.end_label,
                )
                continue
            if bar_end in conflicted_timestamps:
                continue
            previous = by_timestamp.get(bar_end)
            if previous is not None and previous != bar:
                by_timestamp.pop(bar_end, None)
                conflicted_timestamps.add(bar_end)
                logger.warning(
                    "ignored conflicting minute-history label for %s at %s",
                    code,
                    bar_end.isoformat(),
                )
                continue
            by_timestamp[bar_end] = bar
        return tuple(by_timestamp[key] for key in sorted(by_timestamp))

    @staticmethod
    def _parse_historical_minute_history(
        code: str,
        expected_trade_date: date,
        data: dict[str, Any],
    ) -> tuple[TushareMinuteBar, ...]:
        raw_data = data.get("data", {})
        fields = list(raw_data.get("fields", []))
        if not fields or not raw_data.get("items"):
            return ()
        if "trade_time" not in fields:
            raise TushareRealtimeError("stk_mins response missing trade_time")
        normalized_fields = ["time" if field == "trade_time" else field for field in fields]
        normalized = {
            "data": {
                "fields": normalized_fields,
                "items": raw_data.get("items", []),
            }
        }
        bars = TushareRealtimeClient._parse_minute_history(code, normalized)
        wrong_dates = sorted(
            {
                bar.bar_end.astimezone(BEIJING_TZ).date()
                for bar in bars
                if bar.bar_end.astimezone(BEIJING_TZ).date() != expected_trade_date
            }
        )
        if wrong_dates:
            logger.warning(
                "ignored stk_mins rows outside requested trade date %s: %s",
                expected_trade_date.isoformat(),
                ", ".join(item.isoformat() for item in wrong_dates),
            )
        return tuple(
            bar for bar in bars if bar.bar_end.astimezone(BEIJING_TZ).date() == expected_trade_date
        )

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
                    open_price=_strict_float(o),
                    latest_price=_strict_float(c),
                    high_price=_strict_float(row[idx["high"]]) if row[idx["high"]] else 0.0,
                    low_price=_strict_float(row[idx["low"]]) if row[idx["low"]] else 0.0,
                    volume=_strict_float(row[idx["vol"]]) if row[idx["vol"]] else 0.0,
                    amount=_strict_float(row[idx["amount"]]) if row[idx["amount"]] else 0.0,
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

        failed_codes: list[str] = []
        for result in results:
            if isinstance(result, TushareRealtimeError):
                raise result
            if isinstance(result, BaseException):
                raise TushareRealtimeError(f"rt_min_daily failed: {result}") from result
            bare_code, quote = result
            if quote is not None:
                all_quotes[bare_code] = quote
            else:
                failed_codes.append(bare_code)

        if failed_codes:
            logger.warning(
                f"rt_min_daily: {len(failed_codes)} stocks returned empty/unparseable data "
                f"(first 20: {', '.join(failed_codes[:20])})"
            )
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

        try:
            if any(isinstance(row[idx[field]], bool) for row in items for field in required):
                logger.warning("ignored boolean rt_min_daily value for %s", bare_code)
                return None
        except (IndexError, TypeError):
            return None

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

        # 9:30-9:39 early snapshot (use 0939 so data is identical whether
        # the API is called at 09:39 or any time later in the day)
        early_bars = []
        bars_937: list[list] = []  # bars ≤09:37 (call auction + first 7min)
        if has_time:
            for r in items:
                t = str(r[idx["time"]])
                # Format: "2026-03-17 09:31:00"
                if " " in t:
                    t = t.split(" ")[-1]
                hhmm = t.replace(":", "")[:4]
                if hhmm <= "0939":
                    early_bars.append(r)
                if hhmm <= "0937":
                    bars_937.append(r)

        if early_bars:
            e_close = _strict_float(early_bars[-1][idx["close"]])
            e_high = _strict_float(
                max(r[idx["high"]] for r in early_bars if r[idx["high"]] is not None)
            )
            e_low = _strict_float(
                min(r[idx["low"]] for r in early_bars if r[idx["low"]] is not None)
            )
            e_vol = _strict_float(
                sum(r[idx["vol"]] for r in early_bars if r[idx["vol"]] is not None)
            )
        else:
            # Called before 9:30 or no time field — use whatever we have
            e_close = _strict_float(last_close)
            e_high = _strict_float(max_high) if max_high else 0.0
            e_low = _strict_float(min_low) if min_low else 0.0
            e_vol = _strict_float(total_vol)

        if bars_937:
            vol_937 = _strict_float(
                sum(r[idx["vol"]] for r in bars_937 if r[idx["vol"]] is not None)
            )
        else:
            vol_937 = e_vol

        return TushareQuote(
            stock_code=bare_code,
            open_price=_strict_float(first_open),
            latest_price=_strict_float(last_close),
            high_price=_strict_float(max_high) if max_high else 0.0,
            low_price=_strict_float(min_low) if min_low else 0.0,
            volume=_strict_float(total_vol),
            amount=_strict_float(total_amount),
            early_close=e_close,
            early_high=e_high,
            early_low=e_low,
            early_volume=e_vol,
            volume_937=vol_937,
        )

    # ------------------------------------------------------------------
    # iFinD format adapter (used by MomentumSectorScanner)
    # ------------------------------------------------------------------

    async def as_ifind_format(self, stock_codes: list[str], indicators: str) -> dict[str, Any]:
        """
        Fetch quotes and return in iFinD real_time_quotation response format.

        Uses rt_min (batch, current snapshot) since MomentumSectorScanner
        only needs current price, not historical bars.
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
    # Tushare calendar/daily APIs
    # ------------------------------------------------------------------

    async def fetch_trade_calendar(
        self,
        start_date: date,
        end_date: date,
    ) -> tuple[date, ...]:
        """Return SSE open dates for a bounded inclusive calendar range.

        V20 uses the authenticated Tushare connection it already requires
        instead of the legacy process-global AkShare/Sina cache.  Keeping this
        adapter bounded lets the service refresh before its future horizon is
        exhausted and fail closed when no D1/D2 successor can be proven.
        """

        if type(start_date) is not date or type(end_date) is not date:
            raise TypeError("trade-calendar bounds must be dates")
        if start_date > end_date:
            raise ValueError("trade-calendar start_date must not exceed end_date")
        data = await self._api_call(
            "trade_cal",
            {
                "exchange": "SSE",
                "start_date": start_date.strftime("%Y%m%d"),
                "end_date": end_date.strftime("%Y%m%d"),
            },
            fields="exchange,cal_date,is_open",
        )
        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])
        if not fields:
            raise TushareRealtimeError("trade_cal response has no fields")
        index = {field: position for position, field in enumerate(fields)}
        required = {"cal_date", "is_open"}
        if not required.issubset(index):
            missing = ", ".join(sorted(required - set(index)))
            raise TushareRealtimeError(f"trade_cal response missing fields: {missing}")

        open_dates: set[date] = set()
        observed_dates: set[date] = set()
        for item in items:
            try:
                calendar_date = datetime.strptime(str(item[index["cal_date"]]), "%Y%m%d").date()
                raw_is_open = item[index["is_open"]]
                if isinstance(raw_is_open, bool):
                    raise ValueError("boolean is_open is invalid")
                is_open = int(raw_is_open)
            except (IndexError, TypeError, ValueError) as exc:
                raise TushareRealtimeError(f"invalid trade_cal row: {item!r} ({exc})") from exc
            if calendar_date < start_date or calendar_date > end_date:
                raise TushareRealtimeError(
                    "trade_cal response contains a date outside the requested range"
                )
            if is_open not in {0, 1}:
                raise TushareRealtimeError(f"invalid trade_cal is_open value: {raw_is_open!r}")
            if calendar_date in observed_dates:
                raise TushareRealtimeError(
                    f"duplicate trade_cal row for {calendar_date.isoformat()}"
                )
            observed_dates.add(calendar_date)
            if is_open:
                open_dates.add(calendar_date)
        expected_dates = {
            start_date + timedelta(days=offset)
            for offset in range((end_date - start_date).days + 1)
        }
        missing_dates = expected_dates - observed_dates
        if missing_dates:
            first_missing = min(missing_dates)
            raise TushareRealtimeError(
                "trade_cal response does not cover every requested calendar date; "
                f"first missing={first_missing.isoformat()}"
            )
        if not open_dates:
            raise TushareRealtimeError("trade_cal response contains no open dates")
        return tuple(sorted(open_dates))

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
        self._parse_trade_date(trade_date)
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
        required = {"ts_code", "close"}
        if not required.issubset(idx):
            missing = ", ".join(sorted(required - set(idx)))
            raise TushareRealtimeError(f"daily response missing fields: {missing}")
        result: dict[str, float] = {}
        for row in items:
            try:
                ts_code = row[idx["ts_code"]]
                close = row[idx["close"]]
                if ts_code and close is not None:
                    bare = str(ts_code).split(".")[0]
                    if isinstance(close, bool):
                        logger.warning("ignored boolean daily close for %s", bare)
                        continue
                    parsed_close = _strict_float(close)
                    if isfinite(parsed_close) and parsed_close > 0:
                        previous = result.get(bare)
                        if previous is not None and previous != parsed_close:
                            raise TushareRealtimeError(
                                f"conflicting duplicate daily rows for {bare}"
                            )
                        result[bare] = parsed_close
            except (IndexError, TypeError, ValueError) as exc:
                raise TushareRealtimeError(f"invalid daily row: {row!r}") from exc

        logger.info(
            f"Tushare daily: fetched prev_close for {len(result)} stocks (date={trade_date})"
        )
        return result

    async def fetch_daily_bars(self, trade_date: str) -> dict[str, TushareDailyBar]:
        """Fetch one market-wide daily snapshot for a YYYYMMDD trade date.

        Tushare's ``daily.amount`` unit is thousand yuan.  V20's G rule is an
        absolute-yuan rule, so the adapter performs the explicit ``×1000``
        normalization here and nowhere else.
        """
        self._parse_trade_date(trade_date)
        data = await self._api_call(
            "daily",
            {"trade_date": trade_date},
            fields="ts_code,trade_date,close,amount",
        )
        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])
        if not fields or not items:
            return {}
        index = {field: position for position, field in enumerate(fields)}
        required = {"ts_code", "trade_date", "close", "amount"}
        if not required.issubset(index):
            missing = ", ".join(sorted(required - set(index)))
            raise TushareRealtimeError(f"daily response missing fields: {missing}")

        result: dict[str, TushareDailyBar] = {}
        for item in items:
            try:
                code = str(item[index["ts_code"]]).split(".")[0]
                if len(code) != 6 or not code.isdigit():
                    raise ValueError(f"invalid ts_code: {code!r}")
                raw_close = item[index["close"]]
                raw_amount = item[index["amount"]]
                if isinstance(raw_close, bool) or isinstance(raw_amount, bool):
                    logger.warning("ignored boolean daily bar for %s", code)
                    continue
                row = TushareDailyBar(
                    stock_code=code,
                    trade_date=str(item[index["trade_date"]]),
                    close_price=_strict_float(raw_close),
                    amount_yuan=_strict_float(raw_amount) * 1000.0,
                )
            except (IndexError, TypeError, ValueError) as exc:
                raise TushareRealtimeError(f"invalid daily row: {item!r}") from exc
            if row.trade_date != trade_date:
                raise TushareRealtimeError(
                    f"daily row date {row.trade_date!r} does not match request {trade_date!r}"
                )
            if (
                not isfinite(row.close_price)
                or row.close_price <= 0
                or not isfinite(row.amount_yuan)
                or row.amount_yuan <= 0
            ):
                continue
            previous = result.get(code)
            if previous is not None and previous != row:
                raise TushareRealtimeError(f"conflicting duplicate daily rows for {code}")
            result[code] = row
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

    @staticmethod
    def _to_ts_code(bare_code: str) -> str:
        """Convert bare code to Tushare format: 600519 -> 600519.SH."""
        if len(bare_code) != 6 or not bare_code.isdigit():
            raise ValueError(f"invalid bare A-share code: {bare_code!r}")
        if bare_code.startswith("6"):
            return f"{bare_code}.SH"
        if bare_code.startswith(("4", "8", "92")):
            return f"{bare_code}.BJ"
        return f"{bare_code}.SZ"

    @staticmethod
    def _parse_bar_end(raw_time: str) -> datetime:
        """Parse Tushare's exchange-local timestamp into an aware Beijing time."""
        bar_end = datetime.fromisoformat(raw_time)
        if bar_end.tzinfo is None:
            bar_end = bar_end.replace(tzinfo=BEIJING_TZ)
        else:
            bar_end = bar_end.astimezone(BEIJING_TZ)
        if bar_end.second != 0 or bar_end.microsecond != 0:
            raise ValueError(f"minute bar timestamp is not minute-aligned: {raw_time!r}")
        return bar_end

    @staticmethod
    def _parse_trade_date(value: str) -> date:
        try:
            return datetime.strptime(value, "%Y%m%d").date()
        except ValueError as exc:
            raise ValueError(f"trade_date must be YYYYMMDD: {value!r}") from exc
