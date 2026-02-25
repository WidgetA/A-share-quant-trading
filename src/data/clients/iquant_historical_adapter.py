# === MODULE PURPOSE ===
# Historical data adapter for the monitor/live scan subsystem.
# Provides historical lookback data for MomentumQualityFilter.
# Duck-types IFinDHttpClient so MomentumSectorScanner works unchanged.

# === DEPENDENCIES ===
# - AkshareBacktestCache (OSS): Primary source — pre-downloaded, zero network calls
# - akshare: Fallback only — on-the-fly download if no OSS cache available
# - SinaRealtimeClient: Delegates real_time_quotation to Sina Finance

# === KEY CONCEPTS ===
# - OSS cache first: If AkshareBacktestCache is provided, history_quotes reads
#   from the in-memory cache (loaded from Alibaba Cloud OSS at startup).
#   This avoids all akshare/East Money API calls, which are unreliable from cloud.
# - Fallback: If no cache, downloads per-stock via akshare (with retry).
# - Volume: akshare returns 手 (lots); converted to 股 (shares) at read time.
# - Fail-fast: download errors raise immediately (trading safety).

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Concurrency limit for parallel akshare downloads (fallback only).
# Keep low (4) to avoid triggering East Money rate limits / connection resets.
_DOWNLOAD_SEMAPHORE = asyncio.Semaphore(4)

# Retry config for transient network errors (ConnectionError, RemoteDisconnected)
_MAX_RETRIES = 3
_RETRY_BACKOFF = 2.0  # seconds, doubles each retry

# akshare column name → iFinD indicator name mapping
_AKSHARE_TO_IFIND: dict[str, str] = {
    "日期": "time",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",  # in 手, needs ×100
    "成交额": "amount",
    "换手率": "turnoverRatio",
    "涨跌幅": "changeRatio",
    "涨跌额": "change",
    "振幅": "swing",
}

# Reverse: iFinD indicator name → akshare column name
_IFIND_TO_AKSHARE: dict[str, str] = {v: k for k, v in _AKSHARE_TO_IFIND.items() if v != "time"}


class IQuantHistoricalAdapter:
    """
    Duck-types IFinDHttpClient for monitor/live scan mode.

    Data sources (in priority order):
        1. OSS cache (AkshareBacktestCache) — zero network calls, instant
        2. akshare on-the-fly download — fallback with retry

    Methods implemented:
        - history_quotes(): From OSS cache or akshare fallback
        - real_time_quotation(): Delegates to SinaRealtimeClient
        - high_frequency(): Returns empty (live mode uses real_time_quotation)

    Volume convention: akshare returns 手 (lots); converted to 股 (shares).
    """

    def __init__(self, sina_client: Any, cache: Any = None) -> None:
        """
        Args:
            sina_client: SinaRealtimeClient instance for real-time data delegation.
            cache: Optional AkshareBacktestCache for history lookback (avoids akshare API).
        """
        from src.data.clients.sina_realtime import SinaRealtimeClient

        if not isinstance(sina_client, SinaRealtimeClient):
            raise TypeError("sina_client must be a SinaRealtimeClient instance")
        self._sina = sina_client

        # Build a cache-backed adapter for history_quotes if cache is available
        self._cached_adapter: Any = None
        if cache is not None and getattr(cache, "is_ready", False):
            from src.data.clients.akshare_backtest_cache import AkshareHistoricalAdapter

            self._cached_adapter = AkshareHistoricalAdapter(cache)
            logger.info("IQuantHistoricalAdapter: using OSS cache for history_quotes")
        else:
            logger.info("IQuantHistoricalAdapter: no OSS cache, will use akshare on-the-fly")

    @property
    def is_connected(self) -> bool:
        return self._sina._client is not None

    async def start(self) -> None:
        pass  # Sina client is managed externally

    async def stop(self) -> None:
        pass  # Sina client is managed externally

    async def history_quotes(
        self,
        codes: str,
        indicators: str,
        start_date: str,
        end_date: str,
        function_para: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Fetch historical daily data.

        Primary: reads from OSS cache (zero network calls).
        Fallback: downloads on-the-fly from akshare (with retry).
        """
        # Use OSS cache if available — no network call
        if self._cached_adapter is not None:
            return await self._cached_adapter.history_quotes(
                codes, indicators, start_date, end_date, function_para
            )

        # Fallback: on-the-fly akshare download
        return await self._history_quotes_akshare(codes, indicators, start_date, end_date)

    async def _history_quotes_akshare(
        self,
        codes: str,
        indicators: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        """Fallback: fetch historical daily data via akshare on-the-fly."""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        indicator_list = [ind.strip() for ind in indicators.split(",")]

        # Fetch all stocks concurrently with semaphore
        tasks = [
            self._fetch_single_stock(full_code, indicator_list, start_date, end_date)
            for full_code in code_list
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        tables: list[dict[str, Any]] = []
        for full_code, result in zip(code_list, results):
            if isinstance(result, BaseException):
                logger.error(f"akshare fetch failed for {full_code}: {result}")
                raise result  # fail-fast: propagate first error
            if result is not None:
                tables.append(result)

        return {"errorcode": 0, "tables": tables}

    async def _fetch_single_stock(
        self,
        full_code: str,
        indicator_list: list[str],
        start_date: str,
        end_date: str,
    ) -> dict[str, Any] | None:
        """Fetch daily data for a single stock via akshare (with retry)."""
        import akshare as ak

        bare = full_code.split(".")[0]

        async with _DOWNLOAD_SEMAPHORE:
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    df = await asyncio.to_thread(
                        ak.stock_zh_a_hist,
                        symbol=bare,
                        period="daily",
                        start_date=start_date.replace("-", ""),
                        end_date=end_date.replace("-", ""),
                        adjust="qfq",
                    )
                    break  # success
                except (ConnectionError, OSError) as e:
                    if attempt < _MAX_RETRIES:
                        wait = _RETRY_BACKOFF * (2 ** (attempt - 1))
                        logger.warning(
                            f"akshare {bare} attempt {attempt}/{_MAX_RETRIES} failed "
                            f"({type(e).__name__}), retrying in {wait:.0f}s"
                        )
                        await asyncio.sleep(wait)
                    else:
                        logger.error(
                            f"akshare stock_zh_a_hist failed for {bare} "
                            f"after {_MAX_RETRIES} attempts: {e}"
                        )
                        raise
                except Exception:
                    logger.error(f"akshare stock_zh_a_hist failed for {bare}")
                    raise  # non-retryable error, fail-fast

        if df is None or df.empty:
            return None

        # Build indicator arrays in iFinD format
        time_vals: list[str] = []
        indicator_data: dict[str, list] = {ind: [] for ind in indicator_list}
        prev_close_val: float = 0.0

        for _, row in df.iterrows():
            d = row["日期"]
            ds = pd.Timestamp(d).strftime("%Y-%m-%d") if not isinstance(d, str) else d
            time_vals.append(ds)

            for ind in indicator_list:
                val = self._extract_indicator(ind, row, prev_close_val)
                # Volume: convert 手→股
                if ind == "volume" and val is not None:
                    val = val * 100
                indicator_data[ind].append(val)

            # Track previous close for computing next row's preClose indicator.
            # 0.0 sentinel means "unknown" — _extract_indicator returns None in that case.
            prev_close_val = float(row["收盘"]) if pd.notna(row["收盘"]) else 0.0

        if not time_vals:
            return None

        table = {"time": time_vals, **indicator_data}
        return {"thscode": full_code, "table": table}

    @staticmethod
    def _extract_indicator(indicator: str, row: pd.Series, prev_close: float) -> float | None:
        """Extract an iFinD-named indicator value from an akshare DataFrame row."""
        ak_col = _IFIND_TO_AKSHARE.get(indicator)

        if ak_col and ak_col in row.index:
            val = row[ak_col]
            return float(val) if pd.notna(val) else None

        # Special cases
        if indicator == "preClose":
            # akshare doesn't have preClose directly; use previous row's close
            return prev_close if prev_close > 0 else None

        if indicator == "avgPrice":
            # Approximate: amount / volume (in 手, before conversion)
            vol = row.get("成交量")
            amt = row.get("成交额")
            if pd.notna(vol) and pd.notna(amt) and float(vol) > 0:
                return float(amt) / (float(vol) * 100)  # amount / volume_in_shares
            return None

        if indicator in ("pe_ttm", "pe", "pb", "ps", "pcf"):
            # Fundamental indicators not available from akshare daily API
            return None

        return None

    async def real_time_quotation(
        self,
        codes: str,
        indicators: str,
    ) -> dict[str, Any]:
        """Delegate to SinaRealtimeClient."""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        return await self._sina.as_ifind_format(code_list, indicators)

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
        """Get trading dates via akshare (with retry)."""
        import akshare as ak

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                df = await asyncio.to_thread(ak.tool_trade_date_hist_sina)
                all_dates = df["trade_date"].dt.date
                sd = datetime.strptime(start_date, "%Y-%m-%d").date()
                ed = datetime.strptime(end_date, "%Y-%m-%d").date()
                return [d.strftime("%Y-%m-%d") for d in sorted(all_dates) if sd <= d <= ed]
            except (ConnectionError, OSError) as e:
                if attempt < _MAX_RETRIES:
                    wait = _RETRY_BACKOFF * (2 ** (attempt - 1))
                    logger.warning(
                        f"akshare trade_dates attempt {attempt}/{_MAX_RETRIES} "
                        f"failed ({type(e).__name__}), retrying in {wait:.0f}s"
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"Failed to get trade dates after {_MAX_RETRIES} attempts")
                    raise
            except Exception:
                logger.error("Failed to get trade dates from akshare")
                raise
        raise RuntimeError("unreachable")  # all paths raise or return above
