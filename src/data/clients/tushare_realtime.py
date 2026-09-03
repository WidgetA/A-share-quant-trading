# === MODULE PURPOSE ===
# Fetches real-time A-share price data from Tushare Pro.
# Replaces the defunct SinaRealtimeClient for the monitor/live scan subsystem.

# === DEPENDENCIES ===
# - httpx: Async HTTP client for Tushare Pro REST API
# - No iFinD or shared resources — fully isolated

# === KEY CONCEPTS ===
# - Tushare Pro API: POST https://api.tushare.pro with JSON body
# - THREE minute-bar access patterns:
#   * rt_min: returns 1 bar per stock (latest snapshot), supports batch query
#   * rt_min_daily: returns ALL bars for the day, single stock per call
#   * stk_mins: accepts comma-separated stocks for a bounded minute window
# - Volume (vol field) is in 股 (shares) for both endpoints
# - preClose NOT available — must be supplemented by caller
# - Fail-fast: API errors raise TushareRealtimeError (no silent fallback)

from __future__ import annotations

import asyncio
import hashlib
import json
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
class TushareEarlyMarketData:
    """Frozen snapshot produced from one canonical early-minute history.

    Holds the aggregated ``TushareQuote`` and the parsed 09:31..09:39 minute
    bars, plus a stable hash of the *canonical normalized evidence*. The hash
    covers every selection-relevant early bar (code, Asia/Shanghai bar_end,
    OHLCV) up to and including 09:39; raw response field order and transport
    metadata are ignored. For legacy responses without a ``time`` column, the
    hash falls back to the normalized full-day OHLCV aggregates and the stock
    code.
    """

    quote: TushareQuote
    early_bars: tuple[TushareMinuteBar, ...]
    source_hash: str


@dataclass(frozen=True)
class TushareDailyBar:
    """Daily price/turnover row normalized to yuan for V20."""

    stock_code: str
    trade_date: str
    close_price: float
    amount_yuan: float


def tushare_minute_bars_to_early_market_data(
    bare_code: str,
    bars: tuple[TushareMinuteBar, ...],
    expected_trade_date: date,
) -> TushareEarlyMarketData | None:
    """Convert normalized minute bars into the frozen early-market snapshot.

    Wrong-date rows are ignored. Every returned component is derived only from
    target-date bars ending at or before 09:39. A 09:39 bar is not required
    here; canonical readiness owns that policy.
    """
    target_bars: dict[datetime, TushareMinuteBar] = {}
    conflicted_timestamps: set[datetime] = set()
    for bar in bars:
        if bar.bar_end.astimezone(BEIJING_TZ).date() != expected_trade_date:
            continue
        if bar.stock_code != bare_code or not bar.is_valid:
            return None
        if bar.bar_end in conflicted_timestamps:
            continue
        previous = target_bars.get(bar.bar_end)
        if previous is not None and previous != bar:
            target_bars.pop(bar.bar_end, None)
            conflicted_timestamps.add(bar.bar_end)
            continue
        target_bars[bar.bar_end] = bar

    if not target_bars:
        return None

    valid_bars = tuple(target_bars[key] for key in sorted(target_bars))
    early_bars = tuple(bar for bar in valid_bars if bar.end_label <= "09:39")
    if not early_bars:
        return None

    quote = TushareRealtimeClient._aggregate_quote_from_bars(bare_code, early_bars)
    source_hash = TushareRealtimeClient._canonical_early_source_hash(bare_code, early_bars)
    return TushareEarlyMarketData(
        quote=quote,
        early_bars=early_bars,
        source_hash=source_hash,
    )


class TushareRealtimeClient:
    """
    Fetches real-time A-share quotes from Tushare Pro.

    Two modes:
    1. batch_get_quotes(): Uses rt_min (batch, 1 bar/stock) for current snapshot.
       Used by as_ifind_format() for MomentumSectorScanner.
    2. batch_get_early_quotes(): Uses batched, narrow-window stk_mins requests and
       aggregates bars through 09:39. Used by V15 scan which needs stable early data.

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
    # ``stk_mins`` accepts comma-separated equity codes even though the public
    # examples show one code.  Keep the early window below the endpoint's
    # 8,000-row response cap: 400 symbols x at most 16 minute labels leaves
    # deterministic headroom and turns a main-board replay into a handful of
    # physical requests instead of one request per symbol.
    HISTORICAL_EARLY_BATCH_SIZE = 400
    STK_MINS_MAX_ROWS = 8_000

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

        tasks = [
            asyncio.create_task(_fetch_one(code), name=f"rt-minute-history-{code}")
            for code in stock_codes
        ]
        try:
            done, _pending = await asyncio.wait(tasks, timeout=self.TIMEOUT)
        finally:
            unfinished = [task for task in tasks if not task.done()]
            for task in unfinished:
                task.cancel()
            if unfinished:
                await asyncio.gather(*unfinished, return_exceptions=True)
        result: dict[str, tuple[TushareMinuteBar, ...]] = {}
        successful_codes = 0
        for task in done:
            try:
                row = task.result()
            except BaseException as exc:
                logger.warning(
                    "rt_min_daily code failed; successful sibling histories retained: %s",
                    exc,
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
        start = f"{trade_date.isoformat()} 09:15:00"
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

    async def batch_get_early_minute_history_for_date(
        self,
        stock_codes: list[str],
        trade_date: date,
    ) -> dict[str, tuple[TushareMinuteBar, ...]]:
        """Fetch one trade day's selection window with batched ``stk_mins`` calls.

        This is deliberately narrower than :meth:`batch_get_minute_history_for_date`.
        Rolling7 canonical recovery only needs the auction/early labels used by
        V16, so the request spans ``09:24 < t < 09:40`` and can safely carry
        hundreds of comma-separated symbols without approaching Tushare's
        8,000-row cap.  The full-day method remains per-symbol because batching
        a complete session would be truncated and is still required by exits.

        A successful batch returns a key for every requested symbol; an absent
        row set is therefore an explicit empty response (for example a suspended
        stock).  A failed batch returns no keys for its symbols so callers keep
        those targets pending instead of confusing transport failure with no data.
        """

        histories, _failures, _successful_batches = await self._fetch_early_history_batches(
            stock_codes,
            trade_date,
        )
        return histories

    async def _fetch_early_history_batches(
        self,
        stock_codes: list[str],
        trade_date: date,
        *,
        isolate_symbol_data_errors: bool = False,
    ) -> tuple[
        dict[str, tuple[TushareMinuteBar, ...]],
        list[BaseException],
        int,
    ]:
        """Fetch narrow histories and retain batch-level failure evidence.

        Missing keys identify transport/parser failures and remain retryable.
        A successful empty response retains an explicit ``code: ()`` entry so
        callers can distinguish it from a failed physical request.
        """

        if not self._client:
            raise TushareRealtimeError("Client not started - call start() first")
        unique_codes = list(dict.fromkeys(stock_codes))
        if not unique_codes:
            return {}, [], 0
        if type(trade_date) is not date:
            raise TypeError("trade_date must be a date")

        start = f"{trade_date.isoformat()} 09:24:00"
        end = f"{trade_date.isoformat()} 09:40:00"
        batches = [
            unique_codes[index : index + self.HISTORICAL_EARLY_BATCH_SIZE]
            for index in range(0, len(unique_codes), self.HISTORICAL_EARLY_BATCH_SIZE)
        ]
        sem = asyncio.Semaphore(self.MAX_CONCURRENCY)

        async def _fetch_batch(
            batch: list[str],
        ) -> dict[str, tuple[TushareMinuteBar, ...]]:
            async with sem:
                data = await self._api_call(
                    "stk_mins",
                    {
                        "ts_code": ",".join(self._to_ts_code(code) for code in batch),
                        "freq": "1min",
                        "start_date": start,
                        "end_date": end,
                    },
                    fields="ts_code,trade_time,open,close,high,low,vol,amount",
                )
            return self._parse_historical_minute_history_batch(
                batch,
                trade_date,
                data,
                isolate_symbol_data_errors=isolate_symbol_data_errors,
            )

        rows = await asyncio.gather(
            *[_fetch_batch(batch) for batch in batches],
            return_exceptions=True,
        )
        result: dict[str, tuple[TushareMinuteBar, ...]] = {}
        failures: list[BaseException] = []
        successful_batches = 0
        for batch, row in zip(batches, rows, strict=True):
            if isinstance(row, asyncio.CancelledError):
                raise row
            if isinstance(row, BaseException):
                failures.append(row)
                logger.warning(
                    "batched early stk_mins failed for %d codes; successful sibling "
                    "batches retained: %s",
                    len(batch),
                    row,
                )
                continue
            successful_batches += 1
            result.update(row)
        return result, failures, successful_batches

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
        expected_trade_date: date | None = None,
    ) -> tuple[TushareMinuteBar, ...]:
        """Parse a single-stock daily response into sorted, validated, canonical bars.

        When ``expected_trade_date`` is supplied, observing any row on a different
        exchange date invalidates the per-symbol response.
        Identical duplicate rows are folded; conflicting rows for the same timestamp
        are dropped so the result is independent of raw item order.
        """
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
        wrong_trade_date_observed = False
        for item in items:
            try:
                if "ts_code" in index:
                    row_code = str(item[index["ts_code"]]).strip().upper()
                    if row_code.split(".")[0] != code:
                        raise TushareRealtimeError(
                            f"minute-history row ts_code {row_code!r} "
                            f"does not match requested {code!r}"
                        )
                bar_end = TushareRealtimeClient._parse_bar_end(str(item[index["time"]]).strip())
                if (
                    expected_trade_date is not None
                    and bar_end.astimezone(BEIJING_TZ).date() != expected_trade_date
                ):
                    wrong_trade_date_observed = True
                    continue
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
            except TushareRealtimeError:
                # A wrong instrument is not a locally malformed row.  It
                # invalidates the whole per-symbol response because otherwise
                # another stock's bars could be bound to the requested code.
                raise
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
        if wrong_trade_date_observed:
            logger.warning(
                "discarded minute-history response for %s because it mixed trade dates",
                code,
            )
            return ()
        return tuple(by_timestamp[key] for key in sorted(by_timestamp))

    @staticmethod
    def _parse_historical_minute_history(
        code: str,
        expected_trade_date: date,
        data: dict[str, Any],
    ) -> tuple[TushareMinuteBar, ...]:
        raw_data = data.get("data", {})
        fields = list(raw_data.get("fields", []))
        items = raw_data.get("items", [])
        if not fields or not items:
            return ()
        # Both columns are mandatory evidence bindings: trade_time anchors each
        # bar and ts_code proves every row belongs to the requested instrument.
        for required_column in ("ts_code", "trade_time"):
            if required_column not in fields:
                raise TushareRealtimeError(f"stk_mins response missing {required_column}")
        code_index = fields.index("ts_code")
        for item in items:
            try:
                row_code = str(item[code_index]).strip().upper()
            except (IndexError, TypeError) as exc:
                raise TushareRealtimeError(
                    f"stk_mins row has no ts_code for requested {code!r}"
                ) from exc
            bare = row_code.split(".")[0]
            # A wrong or mixed instrument fails closed; it is never skipped.
            if bare != code:
                raise TushareRealtimeError(
                    f"stk_mins row ts_code {row_code!r} does not match requested {code!r}"
                )
        normalized_fields = ["time" if field == "trade_time" else field for field in fields]
        normalized = {
            "data": {
                "fields": normalized_fields,
                "items": items,
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
    def _parse_historical_minute_history_batch(
        codes: list[str],
        expected_trade_date: date,
        data: dict[str, Any],
        *,
        isolate_symbol_data_errors: bool = False,
    ) -> dict[str, tuple[TushareMinuteBar, ...]]:
        """Split one batched ``stk_mins`` response into validated symbol histories.

        Historical replay uses the strict default: any wrong-date or invalid
        symbol history rejects the physical batch.  Live coverage collection
        enables ``isolate_symbol_data_errors`` so one stale/malformed symbol is
        returned as an empty history without discarding healthy siblings.
        """

        requested = tuple(dict.fromkeys(codes))
        requested_set = set(requested)
        if any(len(code) != 6 or not code.isdigit() for code in requested):
            raise ValueError("invalid bare A-share code in historical minute batch")
        raw_data = data.get("data", {})
        fields = list(raw_data.get("fields", []))
        items = list(raw_data.get("items", []))
        required_columns = {
            "ts_code",
            "trade_time",
            "open",
            "close",
            "high",
            "low",
            "vol",
            "amount",
        }
        missing_columns = required_columns - set(fields)
        if missing_columns:
            raise TushareRealtimeError(
                "batched stk_mins response missing fields: " + ", ".join(sorted(missing_columns))
            )
        if not items:
            return {code: () for code in requested}
        if len(items) >= TushareRealtimeClient.STK_MINS_MAX_ROWS:
            raise TushareRealtimeError(
                "batched early stk_mins response reached the 8000-row cap and may be truncated"
            )

        code_index = fields.index("ts_code")
        time_index = fields.index("trade_time")
        grouped: dict[str, list[Any]] = {code: [] for code in requested}
        for item in items:
            try:
                row_code = str(item[code_index]).strip().upper()
            except (IndexError, TypeError) as exc:
                raise TushareRealtimeError("batched stk_mins row has an invalid identity") from exc
            bare = row_code.split(".")[0]
            if bare not in requested_set:
                raise TushareRealtimeError(
                    f"batched stk_mins row ts_code {row_code!r} was not requested"
                )
            if not isolate_symbol_data_errors:
                try:
                    row_time = TushareRealtimeClient._parse_bar_end(str(item[time_index]).strip())
                except (IndexError, TypeError, ValueError) as exc:
                    raise TushareRealtimeError(
                        "batched stk_mins row has an invalid identity"
                    ) from exc
                if row_time.astimezone(BEIJING_TZ).date() != expected_trade_date:
                    raise TushareRealtimeError(
                        "batched stk_mins response contains a row outside the requested trade date"
                    )
            grouped[bare].append(item)

        parsed: dict[str, tuple[TushareMinuteBar, ...]] = {}
        for code in requested:
            symbol_data = {"data": {"fields": fields, "items": grouped[code]}}
            if isolate_symbol_data_errors:
                normalized_fields = ["time" if field == "trade_time" else field for field in fields]
                bars = TushareRealtimeClient._parse_minute_history(
                    code,
                    {
                        "data": {
                            "fields": normalized_fields,
                            "items": grouped[code],
                        }
                    },
                    expected_trade_date=expected_trade_date,
                )
            else:
                bars = TushareRealtimeClient._parse_historical_minute_history(
                    code,
                    expected_trade_date,
                    symbol_data,
                )
            if not isolate_symbol_data_errors and grouped[code] and not bars:
                raise TushareRealtimeError(
                    f"batched stk_mins rows for {code} produced no valid canonical bars"
                )
            parsed[code] = bars
        return parsed

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

    @staticmethod
    def _bar_trade_date(raw_time: str) -> date | None:
        """Extract the Shanghai trade date from a Tushare timestamp if present."""
        try:
            bar_end = datetime.fromisoformat(raw_time.strip())
        except ValueError:
            return None
        if bar_end.tzinfo is None:
            bar_end = bar_end.replace(tzinfo=BEIJING_TZ)
        else:
            bar_end = bar_end.astimezone(BEIJING_TZ)
        return bar_end.date()

    @staticmethod
    def _canonical_early_source_hash(
        bare_code: str,
        bars: tuple[TushareMinuteBar, ...],
    ) -> str:
        """Stable SHA-256 over the canonical early-minute evidence.

        Hashes every selection-relevant bar up to and including 09:39 (covers
        call-auction/09:31前 bars). Each row includes the stock code, the
        normalized Asia/Shanghai bar_end, and OHLCV. Field order and raw response
        order are ignored; ISO-T/+08:00/UTC timestamps that denote the same instant
        produce the same hash. 09:40-or-later bars are excluded. Even when no early
        bars are present, the stock code and schema are part of the hash so that
        different symbols never share an empty-row hash.
        """
        early_bars = [bar for bar in bars if bar.end_label <= "09:39"]
        rows: list[dict[str, Any]] = []
        for bar in early_bars:
            rows.append(
                {
                    "code": bar.stock_code,
                    "end": bar.bar_end.astimezone(BEIJING_TZ).replace(microsecond=0).isoformat(),
                    "o": bar.open_price,
                    "h": bar.high_price,
                    "l": bar.low_price,
                    "c": bar.close_price,
                    "v": bar.volume,
                    "a": bar.amount,
                }
            )
        rows.sort(key=lambda r: (r["end"], r["o"], r["h"], r["l"], r["c"], r["v"], r["a"]))
        canonical = {
            "schema": "tushare-early-v1",
            "code": bare_code,
            "rows": rows,
        }
        canonical_json = json.dumps(
            canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

    @staticmethod
    def _legacy_source_hash(
        code: str,
        items: list[list],
        index: dict[str, int],
    ) -> str:
        """Fallback source hash for legacy responses without a ``time`` field."""
        try:
            first_open = _strict_float(items[0][index["open"]])
            last_close = _strict_float(items[-1][index["close"]])
            max_high = _strict_float(
                max(r[index["high"]] for r in items if r[index["high"]] is not None)
            )
            min_low = _strict_float(
                min(r[index["low"]] for r in items if r[index["low"]] is not None)
            )
            total_vol = _strict_float(
                sum(r[index["vol"]] for r in items if r[index["vol"]] is not None)
            )
            total_amount = _strict_float(
                sum(r[index["amount"]] for r in items if r[index["amount"]] is not None)
            )
            quote_norm = {
                "schema": "tushare-legacy-v1",
                "code": code,
                "o": first_open,
                "h": max_high,
                "l": min_low,
                "c": last_close,
                "v": total_vol,
                "a": total_amount,
            }
            canonical = json.dumps(
                quote_norm, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            )
        except (IndexError, TypeError, ValueError):
            canonical = ""
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    async def batch_get_early_market_data(
        self,
        stock_codes: list[str],
        expected_trade_date: date | None = None,
    ) -> dict[str, TushareEarlyMarketData]:
        """Fetch frozen early data with bounded batched ``stk_mins`` requests.

        * ``stock_codes`` is deduplicated while preserving first occurrence.
        * At most ``ceil(unique_codes / HISTORICAL_EARLY_BATCH_SIZE)`` physical
          calls are made; responses are split locally by instrument.
        * Quote and all selection-relevant early bars (≤09:39 on
          ``expected_trade_date``) are derived from the same response.
        * Returned mapping iteration order follows deduplicated input order.
        * Failed batches stay absent for caller retry/coverage accounting while
          successful sibling batches remain usable.

        ``expected_trade_date`` defaults to today in Asia/Shanghai for backwards
        compatibility with ``batch_get_early_quotes``.
        """
        if not self._client:
            raise TushareRealtimeError("Client not started — call start() first")

        if not stock_codes:
            return {}

        if expected_trade_date is None:
            expected_trade_date = datetime.now(BEIJING_TZ).date()

        unique_codes = list(dict.fromkeys(stock_codes))
        histories, exceptions, successful_batches = await self._fetch_early_history_batches(
            unique_codes,
            expected_trade_date,
            isolate_symbol_data_errors=True,
        )

        all_data: dict[str, TushareEarlyMarketData] = {}
        failed_codes: list[str] = []
        for bare_code in unique_codes:
            bars = histories.get(bare_code)
            if bars is None:
                failed_codes.append(bare_code)
                continue
            data = tushare_minute_bars_to_early_market_data(
                bare_code,
                bars,
                expected_trade_date,
            )
            if data is None:
                failed_codes.append(bare_code)
                continue
            all_data[bare_code] = data

        if successful_batches == 0 and exceptions:
            first = exceptions[0]
            if isinstance(first, TushareRealtimeError):
                raise first
            raise TushareRealtimeError(f"stk_mins failed: {first}") from first

        if failed_codes:
            logger.warning(
                "batched early stk_mins: %d stocks returned empty/unparseable data (first 20: %s)",
                len(failed_codes),
                ", ".join(failed_codes[:20]),
            )
        logger.info(
            "batched early stk_mins: fetched %d/%d stocks",
            len(all_data),
            len(unique_codes),
        )
        return all_data

    async def batch_get_early_quotes(
        self,
        stock_codes: list[str],
        expected_trade_date: date | None = None,
    ) -> dict[str, TushareQuote]:
        """Thin compatibility wrapper over ``batch_get_early_market_data``.

        Returns the same ``TushareQuote`` mapping that callers already expect.
        The optional ``expected_trade_date`` is forwarded for deterministic tests;
        real-time callers should leave it as ``None`` (defaults to today in Shanghai).
        """
        early_data = await self.batch_get_early_market_data(
            stock_codes, expected_trade_date=expected_trade_date
        )
        return {code: data.quote for code, data in early_data.items()}

    @staticmethod
    def _aggregate_quote_from_bars(
        bare_code: str,
        bars: tuple[TushareMinuteBar, ...],
    ) -> TushareQuote:
        """Aggregate a full-day quote and early snapshot from canonical minute bars."""
        open_price = bars[0].open_price
        latest_price = bars[-1].close_price
        high_price = max(bar.high_price for bar in bars)
        low_price = min(bar.low_price for bar in bars)
        volume = sum(bar.volume for bar in bars)
        amount = sum(bar.amount for bar in bars)

        early_bars = [bar for bar in bars if bar.end_label <= "09:39"]
        bars_937 = [bar for bar in early_bars if bar.end_label <= "09:37"]

        if early_bars:
            early_close = early_bars[-1].close_price
            early_high = max(bar.high_price for bar in early_bars)
            early_low = min(bar.low_price for bar in early_bars)
            early_volume = sum(bar.volume for bar in early_bars)
        else:
            early_close = latest_price
            early_high = high_price
            early_low = low_price
            early_volume = volume

        volume_937 = sum(bar.volume for bar in bars_937) if bars_937 else early_volume

        return TushareQuote(
            stock_code=bare_code,
            open_price=open_price,
            latest_price=latest_price,
            high_price=high_price,
            low_price=low_price,
            volume=volume,
            amount=amount,
            early_close=early_close,
            early_high=early_high,
            early_low=early_low,
            early_volume=early_volume,
            volume_937=volume_937,
        )

    @staticmethod
    def _parse_early_market_data(
        bare_code: str,
        data: dict[str, Any],
        expected_trade_date: date | None = None,
    ) -> TushareEarlyMarketData | None:
        """Parse one rt_min_daily response into canonical quote, bars, and source hash.

        All selection-relevant outputs are derived from the same set of canonical
        minute bars. Legacy responses without a ``time`` field still produce a quote
        and a stable hash, but no minute bars.
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

        if has_time:
            if expected_trade_date is None:
                expected_trade_date = datetime.now(BEIJING_TZ).date()
            bars = TushareRealtimeClient._parse_minute_history(
                bare_code, data, expected_trade_date=expected_trade_date
            )
            return tushare_minute_bars_to_early_market_data(bare_code, bars, expected_trade_date)

        # Legacy response without a time column: aggregate the full-day OHLCV rows.
        # Every selection-relevant field in every row must be a finite, strictly
        # positive price (or non-negative flow); booleans are rejected so they are
        # not silently treated as 0/1. Any illegal row rejects the whole symbol.
        try:
            validated: list[tuple[float, float, float, float, float, float]] = []
            for row in items:
                if len(row) < len(fields):
                    raise ValueError("short no-time row")
                open_price = _strict_float(row[idx["open"]])
                close_price = _strict_float(row[idx["close"]])
                high_price = _strict_float(row[idx["high"]])
                low_price = _strict_float(row[idx["low"]])
                volume = _strict_float(row[idx["vol"]])
                amount = _strict_float(row[idx["amount"]])
                if not (
                    isfinite(open_price)
                    and open_price > 0
                    and isfinite(close_price)
                    and close_price > 0
                    and isfinite(high_price)
                    and high_price > 0
                    and isfinite(low_price)
                    and low_price > 0
                    and low_price <= min(open_price, close_price)
                    and high_price >= max(open_price, close_price)
                    and isfinite(volume)
                    and volume >= 0
                    and isfinite(amount)
                    and amount >= 0
                ):
                    raise ValueError("invalid no-time OHLCV row")
                validated.append((open_price, close_price, high_price, low_price, volume, amount))
        except (ValueError, TypeError, IndexError) as e:
            logger.warning("ignored invalid no-time rt_min_daily row(s) for %s: %s", bare_code, e)
            return None

        first_open = validated[0][0]
        last_close = validated[-1][1]
        max_high = max(row[2] for row in validated)
        min_low = min(row[3] for row in validated)
        total_vol = sum(row[4] for row in validated)
        total_amount = sum(row[5] for row in validated)

        quote = TushareQuote(
            stock_code=bare_code,
            open_price=first_open,
            latest_price=last_close,
            high_price=max_high,
            low_price=min_low,
            volume=total_vol,
            amount=total_amount,
            early_close=last_close,
            early_high=max_high,
            early_low=min_low,
            early_volume=total_vol,
            volume_937=total_vol,
        )
        source_hash = TushareRealtimeClient._legacy_source_hash(bare_code, items, idx)
        return TushareEarlyMarketData(quote=quote, early_bars=(), source_hash=source_hash)

    @staticmethod
    def _parse_rt_min_daily(
        bare_code: str,
        data: dict[str, Any],
        expected_trade_date: date | None = None,
    ) -> TushareQuote | None:
        """Backward-compatible alias that returns only the aggregated quote."""
        emd = TushareRealtimeClient._parse_early_market_data(
            bare_code, data, expected_trade_date=expected_trade_date
        )
        return emd.quote if emd is not None else None

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

    async def fetch_stock_names_for_date(self, trade_date: str) -> dict[str, str]:
        """Fetch the official market-wide stock-name snapshot for one trade date.

        Historical canonical replay must not use today's ``stock_basic`` names
        to decide whether a past candidate was ST.  Tushare ``bak_basic`` is a
        date-addressed market snapshot, so one response supplies both the
        frozen display names and the exact-day ST eligibility input.
        """

        self._parse_trade_date(trade_date)
        data = await self._api_call(
            "bak_basic",
            {"trade_date": trade_date},
            fields="trade_date,ts_code,name",
        )
        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])
        if not fields or not items:
            return {}
        index = {field: position for position, field in enumerate(fields)}
        required = {"trade_date", "ts_code", "name"}
        if not required.issubset(index):
            missing = ", ".join(sorted(required - set(index)))
            raise TushareRealtimeError(f"bak_basic response missing fields: {missing}")

        result: dict[str, str] = {}
        for item in items:
            try:
                raw_date = item[index["trade_date"]]
                raw_code = item[index["ts_code"]]
                raw_name = item[index["name"]]
            except (IndexError, TypeError, ValueError) as exc:
                raise TushareRealtimeError(f"invalid bak_basic row: {item!r}") from exc
            if (
                not isinstance(raw_date, str)
                or not isinstance(raw_code, str)
                or not isinstance(raw_name, str)
            ):
                raise TushareRealtimeError(f"invalid bak_basic row: {item!r}")
            row_date = raw_date
            code = raw_code.split(".")[0]
            name = raw_name.strip()
            if row_date != trade_date:
                raise TushareRealtimeError(
                    f"bak_basic row date {row_date!r} does not match request {trade_date!r}"
                )
            if len(code) != 6 or not code.isdigit() or not name:
                raise TushareRealtimeError(f"invalid bak_basic row: {item!r}")
            previous = result.get(code)
            if previous is not None and previous != name:
                raise TushareRealtimeError(f"conflicting duplicate bak_basic rows for {code}")
            result[code] = name
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
        """Parse Tushare's exchange-local timestamp into an aware Beijing time.

        The input must contain both a date and a time component. Bare dates or
        bare times are rejected so they cannot be mis-bound to expected_trade_date.
        """
        if " " not in raw_time and "T" not in raw_time:
            raise ValueError(f"timestamp must contain date and time: {raw_time!r}")
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
