# === MODULE PURPOSE ===
# Pre-downloads A-share daily + minute price data via akshare for backtesting.
# Provides an adapter that mimics IFinDHttpClient interface so
# MomentumSectorScanner can run without any code changes.

# === DEPENDENCIES ===
# - akshare: Free A-share data source (daily OHLCV, minute bars)
# - IFinDHttpClient interface: Adapter returns data in iFinD response format

# === KEY CONCEPTS ===
# - AkshareBacktestCache: Downloads and stores all price data in memory
# - AkshareHistoricalAdapter: Duck-types IFinDHttpClient for the scanner
# - Pre-download strategy: ~3500 stocks × (daily + minute) ≈ 25-30 min
# - Data is NOT used for live trading (backtest only)

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Any, Callable

import pandas as pd

logger = logging.getLogger(__name__)

# Akshare calls are synchronous; we limit concurrency to avoid hammering the API
_DOWNLOAD_WORKERS = 8


class AkshareBacktestCache:
    """
    Pre-downloads daily OHLCV and 09:30-09:40 minute bar data for all
    main-board A-share stocks, keyed by (stock_code, date).

    Usage:
        cache = AkshareBacktestCache()
        await cache.download_prices(start_date, end_date, progress_cb)
        snap = cache.get_daily(code, trade_date)  # (open, high, low, close, preClose, volume)
        p940 = cache.get_940_price(code, trade_date)  # (close_at_940, cum_vol, max_high, min_low)
    """

    def __init__(self) -> None:
        # daily[code][date_str] = {open, high, low, close, preClose, volume, amount}
        self._daily: dict[str, dict[str, dict[str, float]]] = {}
        # minute[code][date_str] = (close_at_940, cum_volume, max_high, min_low)
        self._minute: dict[str, dict[str, tuple[float, float, float, float]]] = {}
        # All stock codes (bare 6-digit) that were downloaded
        self._stock_codes: list[str] = []
        self._is_ready = False
        self._start_date: date | None = None
        self._end_date: date | None = None

    @property
    def is_ready(self) -> bool:
        return self._is_ready

    @property
    def stock_codes(self) -> list[str]:
        return self._stock_codes

    def get_daily(self, code: str, date_str: str) -> dict[str, float] | None:
        """Get daily OHLCV for a stock on a date. date_str is YYYY-MM-DD."""
        return self._daily.get(code, {}).get(date_str)

    def get_940_price(self, code: str, date_str: str) -> tuple[float, float, float, float] | None:
        """Get 9:40 price data: (close, cum_volume, max_high, min_low)."""
        return self._minute.get(code, {}).get(date_str)

    def get_all_codes_with_daily(self, date_str: str) -> dict[str, dict[str, float]]:
        """Get daily data for ALL stocks on a specific date."""
        result: dict[str, dict[str, float]] = {}
        for code, dates in self._daily.items():
            day_data = dates.get(date_str)
            if day_data:
                result[code] = day_data
        return result

    async def download_prices(
        self,
        start_date: date,
        end_date: date,
        progress_cb: Callable[[str, int, int], Any] | None = None,
    ) -> None:
        """
        Download daily + minute data for all main-board stocks.

        Args:
            start_date: First trading date (inclusive).
            end_date: Last trading date (inclusive).
            progress_cb: Optional callback(phase, current, total) for progress updates.
                phase is "daily" or "minute".
        """
        import akshare as ak

        self._start_date = start_date
        self._end_date = end_date
        # Download one extra week before start_date to get preClose for first day
        dl_start = start_date - timedelta(days=10)
        start_str = dl_start.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        # Get all A-share stock codes
        if progress_cb:
            await _maybe_await(progress_cb("init", 0, 0))

        all_stocks_df = await asyncio.to_thread(ak.stock_info_a_code_name)
        # Filter main board only (60xxxx SH, 00xxxx SZ)
        codes = [
            row["code"]
            for _, row in all_stocks_df.iterrows()
            if isinstance(row["code"], str)
            and len(row["code"]) == 6
            and (row["code"].startswith("60") or row["code"].startswith("00"))
        ]
        self._stock_codes = codes
        total = len(codes)
        logger.info(f"Downloading data for {total} main-board stocks [{start_date} ~ {end_date}]")

        # Phase 1: Daily OHLCV
        sem = asyncio.Semaphore(_DOWNLOAD_WORKERS)
        done_daily = 0

        async def _download_daily(code: str) -> None:
            nonlocal done_daily
            async with sem:
                try:
                    df = await asyncio.to_thread(
                        ak.stock_zh_a_hist,
                        symbol=code,
                        period="daily",
                        start_date=start_str,
                        end_date=end_str,
                        adjust="qfq",
                    )
                    if df is not None and not df.empty:
                        code_data: dict[str, dict[str, float]] = {}
                        prev_close = 0.0
                        for _, row in df.iterrows():
                            d = row["日期"]
                            if isinstance(d, str):
                                ds = d
                            else:
                                ds = pd.Timestamp(d).strftime("%Y-%m-%d")
                            code_data[ds] = {
                                "open": float(row["开盘"]),
                                "high": float(row["最高"]),
                                "low": float(row["最低"]),
                                "close": float(row["收盘"]),
                                "preClose": prev_close,
                                "volume": float(row["成交量"]),
                                "amount": float(row["成交额"]),
                            }
                            prev_close = float(row["收盘"])

                        # Fix preClose: first day has 0, need to fetch one extra day
                        # We'll fix this after all downloads
                        self._daily[code] = code_data
                except Exception as e:
                    logger.debug(f"Daily download failed for {code}: {e}")

                done_daily += 1
                if progress_cb and done_daily % 50 == 0:
                    await _maybe_await(progress_cb("daily", done_daily, total))

        tasks = [_download_daily(c) for c in codes]
        await asyncio.gather(*tasks)
        if progress_cb:
            await _maybe_await(progress_cb("daily", total, total))
        logger.info(f"Daily download complete: {len(self._daily)}/{total} stocks")

        # Fix preClose for first date in each stock's data
        # preClose of first row is 0, which is wrong. We set it from the previous
        # day's close if available, otherwise we'll leave it (scanner checks prev_close > 0).
        # For backtesting purposes this means the first date might have fewer candidates.

        # Phase 2: Minute data (09:30-09:40 for 9:40 price)
        done_minute = 0

        async def _download_minute(code: str) -> None:
            nonlocal done_minute
            async with sem:
                try:
                    df = await asyncio.to_thread(
                        ak.stock_zh_a_hist_min_em,
                        symbol=code,
                        start_date=start_str + " 09:30:00",
                        end_date=end_str + " 09:40:00",
                        period="1",
                        adjust="qfq",
                    )
                    if df is not None and not df.empty:
                        code_data: dict[str, tuple[float, float, float, float]] = {}
                        # Group by date, extract 09:31~09:40 bars
                        for _, row in df.iterrows():
                            ts = row["时间"]
                            if isinstance(ts, str):
                                dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                            else:
                                dt = pd.Timestamp(ts).to_pydatetime()

                            ds = dt.strftime("%Y-%m-%d")
                            ts_time = dt.strftime("%H:%M")

                            # Only keep bars between 09:31 and 09:40
                            if ts_time < "09:31" or ts_time > "09:40":
                                continue

                            close_val = float(row["收盘"])
                            vol_val = float(row["成交量"])
                            high_val = float(row["最高"])
                            low_val = float(row["最低"])

                            if ds in code_data:
                                prev = code_data[ds]
                                code_data[ds] = (
                                    close_val,  # latest close (overwrite with later bar)
                                    prev[1] + vol_val,  # cumulative volume
                                    max(prev[2], high_val),  # max high
                                    min(prev[3], low_val) if prev[3] > 0 else low_val,  # min low
                                )
                            else:
                                code_data[ds] = (close_val, vol_val, high_val, low_val)

                        self._minute[code] = code_data
                except Exception as e:
                    logger.debug(f"Minute download failed for {code}: {e}")

                done_minute += 1
                if progress_cb and done_minute % 50 == 0:
                    await _maybe_await(progress_cb("minute", done_minute, total))

        tasks = [_download_minute(c) for c in codes]
        await asyncio.gather(*tasks)
        if progress_cb:
            await _maybe_await(progress_cb("minute", total, total))
        logger.info(f"Minute download complete: {len(self._minute)}/{total} stocks")

        self._is_ready = True


class AkshareHistoricalAdapter:
    """
    Duck-types IFinDHttpClient for backtest use.

    Reads from AkshareBacktestCache and returns data in iFinD response format
    so MomentumSectorScanner works without modification.
    """

    def __init__(self, cache: AkshareBacktestCache) -> None:
        self._cache = cache

    @property
    def is_connected(self) -> bool:
        return self._cache.is_ready

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def history_quotes(
        self,
        codes: str,
        indicators: str,
        start_date: str,
        end_date: str,
        function_para: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Return cached daily data in iFinD history_quotes format."""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        tables: list[dict[str, Any]] = []

        for full_code in code_list:
            bare = full_code.split(".")[0]
            daily_dates = self._cache._daily.get(bare, {})

            # Collect data for the date range
            time_vals: list[str] = []
            indicator_data: dict[str, list] = {}
            for ind in indicators.split(","):
                indicator_data[ind.strip()] = []

            # Iterate dates in range
            d = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_d = datetime.strptime(end_date, "%Y-%m-%d").date()
            while d <= end_d:
                ds = d.strftime("%Y-%m-%d")
                day = daily_dates.get(ds)
                if day:
                    time_vals.append(ds)
                    for ind in indicators.split(","):
                        ind = ind.strip()
                        indicator_data[ind].append(day.get(ind))
                d += timedelta(days=1)

            if time_vals:
                table = {"time": time_vals, **indicator_data}
                tables.append({"thscode": full_code, "table": table})

        return {"errorcode": 0, "tables": tables}

    async def high_frequency(
        self,
        codes: str,
        indicators: str,
        start_time: str,
        end_time: str,
        function_para: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Return cached minute data in iFinD high_frequency format."""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        tables: list[dict[str, Any]] = []

        # Extract date from start_time (format: "YYYY-MM-DD HH:MM:SS")
        date_str = start_time.split(" ")[0]

        for full_code in code_list:
            bare = full_code.split(".")[0]
            data_940 = self._cache.get_940_price(bare, date_str)
            if data_940:
                close_val, cum_vol, max_high, min_low = data_940
                # Return as single-bar arrays to match iFinD format
                table: dict[str, list] = {}
                for ind in indicators.split(","):
                    ind = ind.strip()
                    if ind == "close":
                        table["close"] = [close_val]
                    elif ind == "volume":
                        table["volume"] = [cum_vol]
                    elif ind == "high":
                        table["high"] = [max_high]
                    elif ind == "low":
                        table["low"] = [min_low]
                tables.append({"thscode": full_code, "table": table})

        return {"errorcode": 0, "tables": tables}

    async def smart_stock_picking(
        self,
        search_string: str,
        search_type: str = "stock",
    ) -> dict[str, Any]:
        """Return empty result — triggers fallback in scanner Step 6."""
        return {"errorcode": 0, "tables": []}

    async def real_time_quotation(
        self,
        codes: str,
        indicators: str,
    ) -> dict[str, Any]:
        """Not supported in backtest mode."""
        return {"errorcode": 0, "tables": []}

    async def get_trade_dates(
        self,
        market_code: str,
        start_date: str,
        end_date: str,
    ) -> list[str]:
        """Get trading dates via akshare."""
        import akshare as ak

        try:
            df = await asyncio.to_thread(ak.tool_trade_date_hist_sina)
            all_dates = df["trade_date"].dt.date
            sd = datetime.strptime(start_date, "%Y-%m-%d").date()
            ed = datetime.strptime(end_date, "%Y-%m-%d").date()
            return [d.strftime("%Y-%m-%d") for d in sorted(all_dates) if sd <= d <= ed]
        except Exception as e:
            logger.error(f"Failed to get trade dates: {e}")
            return []


async def _maybe_await(result: Any) -> None:
    """Await result if it's a coroutine, otherwise do nothing."""
    if asyncio.iscoroutine(result):
        await result
