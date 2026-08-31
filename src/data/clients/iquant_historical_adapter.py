# === MODULE PURPOSE ===
# Historical data adapter for the monitor/live scan subsystem.
# Duck-types IFinDHttpClient so V15Scanner / MomentumSectorScanner work unchanged.

# === DATA FLOW ===
# - history_quotes(): Downloads from Tushare Pro `daily` API (one call per trade_date,
#   returns ALL stocks). Data is held in memory for the current trading day so
#   repeated calls within one scan don't re-download.
# - real_time_quotation(): Delegates to realtime client (TushareRealtimeClient).
# - Volume: tushare `daily` returns 手 (lots); converted to 股 (shares) at read time.

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_TUSHARE_API_URL = "https://api.tushare.pro"
_TUSHARE_DAILY_TIMEOUT = 30.0


class IQuantHistoricalAdapterError(RuntimeError):
    """A requested daily source date could not be fetched authoritatively."""


class IQuantHistoricalAdapter:
    """
    Duck-types IFinDHttpClient for monitor/live scan mode.

    Data source: Tushare Pro `daily` API (one call per trade_date — returns
    all stocks). Data is held in memory for the current trading day so repeated
    calls within one scan don't re-download. Cleared automatically on new day.

    Methods implemented:
        - history_quotes(): From Tushare `daily` API
        - real_time_quotation(): Delegates to TushareRealtimeClient
        - high_frequency(): Returns empty (live mode uses real_time_quotation)

    Volume convention: tushare `daily` returns 手 (lots); converted to 股
    (shares) at read time so callers see the same unit as iFinD / live snapshots.
    """

    def __init__(
        self,
        realtime_client: Any,
        cache: Any = None,
        *,
        tushare_token: str | None = None,
    ) -> None:
        """
        Args:
            realtime_client: Duck-typed realtime client for real-time data delegation.
                Must implement as_ifind_format(stock_codes, indicators) -> dict.
                Typically TushareRealtimeClient.
            cache: Optional TushareBacktestCache (unused, kept for backward compat).
        """
        if not hasattr(realtime_client, "as_ifind_format"):
            raise TypeError(
                "realtime_client must implement as_ifind_format(). Use TushareRealtimeClient."
            )
        self._realtime = realtime_client
        # Production V20 injects its already validated environment token.
        # ``None`` preserves the legacy file-first resolver for existing users.
        self._tushare_token = tushare_token

        # In-memory daily data downloaded from Tushare API.
        # Keyed by date_str -> {bare_code -> {close, volume, ...}}
        # Populated on first history_quotes() call, reused within the same day.
        self._daily_data: dict[str, dict[str, dict]] = {}
        self._daily_data_loaded_date: str = ""  # YYYY-MM-DD of last load
        self._exchange_trade_dates: frozenset[date] | None = None

        logger.info("IQuantHistoricalAdapter: using Tushare Pro daily for history_quotes")

    @property
    def is_connected(self) -> bool:
        client = getattr(self._realtime, "_client", None)
        return client is not None

    async def start(self) -> None:
        pass  # Realtime client is managed externally

    async def stop(self) -> None:
        pass  # Realtime client is managed externally

    def set_exchange_trade_calendar(self, calendar: list[date] | tuple[date, ...]) -> None:
        """Bind authoritative open dates used to classify empty daily responses.

        Tushare legitimately returns an empty ``daily`` response for an exchange
        holiday, but a transient empty response on an open date must be retried.
        V20 supplies the already validated exchange calendar before prewarming.
        """

        normalized = tuple(calendar)
        if (
            not normalized
            or any(type(item) is not date for item in normalized)
            or tuple(sorted(set(normalized))) != normalized
        ):
            raise ValueError("exchange trade calendar must be sorted, unique dates")
        self._exchange_trade_dates = frozenset(normalized)

    async def history_quotes(
        self,
        codes: str,
        indicators: str,
        start_date: str,
        end_date: str,
        function_para: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Fetch historical daily data from Tushare `daily` API.

        Downloads all trading dates in [start_date, end_date] via Tushare
        `daily` (one call per trade_date returns ALL stocks).
        Data is held in memory for the current trading day so repeated
        calls within one scan don't re-download.
        """
        today_str = datetime.now().strftime("%Y-%m-%d")
        if self._daily_data_loaded_date != today_str:
            # New day — clear stale in-memory data
            self._daily_data.clear()
            self._daily_data_loaded_date = today_str

        # Download any missing dates
        await self._ensure_daily_range(start_date, end_date)

        # Build iFinD-format response from in-memory data
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        indicator_list = [ind.strip() for ind in indicators.split(",")]
        tables: list[dict[str, Any]] = []

        for full_code in code_list:
            bare = full_code.split(".")[0]
            time_vals: list[str] = []
            indicator_data: dict[str, list] = {ind: [] for ind in indicator_list}

            d = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_d = datetime.strptime(end_date, "%Y-%m-%d").date()
            while d <= end_d:
                ds = d.strftime("%Y-%m-%d")
                day = self._daily_data.get(ds, {}).get(bare)
                if day:
                    time_vals.append(ds)
                    for ind in indicator_list:
                        val = day.get(ind)
                        # tushare `daily` volume is in 手; convert to 股 at read time
                        if ind == "volume" and val is not None:
                            val = val * 100
                        indicator_data[ind].append(val)
                d += timedelta(days=1)

            if time_vals:
                table = {"time": time_vals, **indicator_data}
                tables.append({"thscode": full_code, "table": table})

        return {"errorcode": 0, "tables": tables}

    async def _ensure_daily_range(self, start_date: str, end_date: str) -> None:
        """Download missing dates from Tushare `daily` API (one call per trade_date)."""
        from src.common.config import get_tushare_token

        d = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_d = datetime.strptime(end_date, "%Y-%m-%d").date()

        dates_needed: list = []
        while d <= end_d:
            ds = d.strftime("%Y-%m-%d")
            if ds not in self._daily_data:
                # Skip weekends — Tushare returns empty so cache the marker.
                if d.weekday() >= 5:
                    self._daily_data[ds] = {}
                else:
                    dates_needed.append(d)
            d += timedelta(days=1)

        if not dates_needed:
            return

        logger.info(
            f"Downloading {len(dates_needed)} dates from Tushare `daily` "
            f"({dates_needed[0]} ~ {dates_needed[-1]})"
        )

        token = self._tushare_token
        if token is None:
            token = get_tushare_token()

        failures: list[str] = []
        async with httpx.AsyncClient(timeout=httpx.Timeout(_TUSHARE_DAILY_TIMEOUT)) as client:
            for i, day_date in enumerate(dates_needed):
                ds_iso = day_date.strftime("%Y-%m-%d")
                ts_date = day_date.strftime("%Y%m%d")
                day_data: dict[str, dict] = {}
                conflicted_tickers: set[str] = set()

                body = {
                    "api_name": "daily",
                    "token": token,
                    "params": {"trade_date": ts_date},
                    "fields": "ts_code,open,high,low,close,pre_close,vol",
                }

                try:
                    resp = await client.post(_TUSHARE_API_URL, json=body)
                    resp.raise_for_status()
                    data = resp.json()
                except (httpx.HTTPError, ValueError, TypeError) as e:
                    logger.warning(f"tushare daily {ts_date}: {e}")
                    failures.append(f"{ts_date}:TRANSPORT_OR_JSON")
                    continue

                if not isinstance(data, Mapping):
                    logger.warning("tushare daily %s returned a non-object payload", ts_date)
                    failures.append(f"{ts_date}:SCHEMA")
                    continue
                if data.get("code") != 0:
                    logger.warning(f"tushare daily {ts_date} error: {data.get('msg', 'unknown')}")
                    failures.append(f"{ts_date}:API_{data.get('code')}")
                    continue

                data_payload = data.get("data")
                if not isinstance(data_payload, Mapping):
                    logger.warning("tushare daily %s returned an invalid data object", ts_date)
                    failures.append(f"{ts_date}:SCHEMA")
                    continue
                fields = data_payload.get("fields", [])
                items = data_payload.get("items", [])
                required_fields = {"ts_code", "open", "high", "low", "close", "vol"}
                if (
                    not isinstance(fields, list)
                    or not required_fields.issubset(fields)
                    or not isinstance(items, list)
                ):
                    logger.warning("tushare daily %s returned an invalid schema", ts_date)
                    failures.append(f"{ts_date}:SCHEMA")
                    continue
                if items:
                    idx = {f: i for i, f in enumerate(fields)}
                    for row in items:
                        if not isinstance(row, list) or len(row) < len(fields):
                            failures.append(f"{ts_date}:ROW_SCHEMA")
                            day_data = {}
                            break
                        ts_code = row[idx["ts_code"]]
                        if not ts_code:
                            continue
                        ticker = ts_code.split(".")[0]
                        if not ticker or len(ticker) != 6:
                            continue
                        if ticker in conflicted_tickers:
                            continue
                        o = row[idx["open"]]
                        c = row[idx["close"]]
                        if o is None or c is None:
                            continue
                        h = row[idx["high"]]
                        lo = row[idx["low"]]
                        v = row[idx["vol"]]
                        if any(isinstance(item, bool) for item in (o, h, lo, c, v)):
                            logger.warning(
                                "tushare daily %s ignored a boolean numeric row for %s",
                                ts_date,
                                ticker,
                            )
                            continue
                        try:
                            parsed_row = {
                                "open": float(o),
                                "high": float(h),
                                "low": float(lo),
                                "close": float(c),
                                # Tushare `daily` vol is 手; history_quotes converts it.
                                "volume": float(v),
                            }
                            previous_row = day_data.get(ticker)
                            if previous_row is not None and previous_row != parsed_row:
                                day_data.pop(ticker, None)
                                conflicted_tickers.add(ticker)
                                logger.warning(
                                    "tushare daily %s dropped conflicting duplicate rows for %s",
                                    ts_date,
                                    ticker,
                                )
                                continue
                            day_data[ticker] = parsed_row
                        except (TypeError, ValueError, OverflowError):
                            # One corrupt security must not erase otherwise valid siblings.
                            logger.warning(
                                "tushare daily %s ignored an invalid row for %s",
                                ts_date,
                                ticker,
                            )

                if f"{ts_date}:ROW_SCHEMA" not in failures:
                    if day_data:
                        self._daily_data[ds_iso] = day_data
                    elif (
                        self._exchange_trade_dates is not None
                        and day_date not in self._exchange_trade_dates
                    ):
                        # A validated exchange calendar makes this an authoritative
                        # closed weekday, so the empty marker is safe to cache.
                        self._daily_data[ds_iso] = {}
                    elif (
                        self._exchange_trade_dates is not None
                        and day_date in self._exchange_trade_dates
                    ):
                        # ``code=0`` is not sufficient evidence that an open day's
                        # market-wide payload was complete.  Fail and retry instead
                        # of poisoning the in-process cache for the rest of D0.
                        failures.append(f"{ts_date}:EMPTY_OPEN_DATE")
                    else:
                        # Legacy callers do not provide an authoritative calendar.
                        # Preserve their non-failing behavior, but never cache an
                        # ambiguous weekday empty response across calls.
                        logger.warning(
                            "tushare daily %s returned empty without an exchange calendar; "
                            "leaving it uncached for retry",
                            ts_date,
                        )

                if (i + 1) % 20 == 0:
                    logger.info(f"  tushare daily progress: {i + 1}/{len(dates_needed)}")

        trading_days = sum(1 for d in self._daily_data.values() if d)
        logger.info(f"tushare daily data ready: {trading_days} trading days")
        if failures:
            sample = ",".join(failures[:5])
            raise IQuantHistoricalAdapterError(
                f"tushare daily source failed for {len(failures)} requested dates: {sample}"
            )

    async def real_time_quotation(
        self,
        codes: str,
        indicators: str,
    ) -> dict[str, Any]:
        """Delegate to realtime client (TushareRealtimeClient)."""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        return await self._realtime.as_ifind_format(code_list, indicators)

    async def high_frequency(
        self,
        codes: str,
        indicators: str,
        start_time: str,
        end_time: str,
        function_para: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Not used in live mode — scanner uses real_time_quotation instead."""
        return {"errorcode": 0, "tables": []}

    async def smart_stock_picking(
        self,
        search_string: str,
        search_type: str = "stock",
    ) -> dict[str, Any]:
        """Return empty result — the scan endpoint provides candidates directly."""
        return {"errorcode": 0, "tables": []}

    async def get_trade_dates(
        self,
        market_code: str,
        start_date: str,
        end_date: str,
    ) -> list[str]:
        """Get trading dates via akshare."""
        import asyncio

        import akshare as ak

        df = await asyncio.to_thread(ak.tool_trade_date_hist_sina)
        all_dates = df["trade_date"].dt.date
        sd = datetime.strptime(start_date, "%Y-%m-%d").date()
        ed = datetime.strptime(end_date, "%Y-%m-%d").date()
        return [d.strftime("%Y-%m-%d") for d in sorted(all_dates) if sd <= d <= ed]
