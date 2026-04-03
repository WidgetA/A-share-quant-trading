# === MODULE PURPOSE ===
# Historical data adapter for the monitor/live scan subsystem.
# Implements HistoricalDataProvider protocol for MomentumScanner.
#
# === DATA FLOW ===
# - history_quotes(): Downloads from tsanghi daily_latest API (per-date batch).
#   Data is held in memory for the current trading day so repeated calls
#   within one scan don't re-download.
# - real_time_quotation(): Delegates to realtime client (Tushare/Sina).
# - Volume: tsanghi returns 手 (lots); converted to 股 (shares) at read time.

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)


class IQuantHistoricalAdapter:
    """Implements HistoricalDataProvider for monitor/live scan mode.

    Data source: tsanghi daily_latest API (per-date batch, 2 calls per date).
    Data is held in memory for the current trading day so repeated calls
    within one scan don't re-download. Cleared automatically on new day.

    Volume convention: tsanghi returns 手 (lots); converted to 股 (shares) at read time.
    """

    def __init__(self, realtime_client: Any) -> None:
        """
        Args:
            realtime_client: Duck-typed realtime client for real-time data delegation.
                Must implement as_standard_quote_format(stock_codes, indicators) -> dict.
                Typically TushareRealtimeClient or SinaRealtimeClient.
        """
        if not hasattr(realtime_client, "as_standard_quote_format"):
            raise TypeError(
                "realtime_client must implement as_standard_quote_format(). "
                "Use TushareRealtimeClient or SinaRealtimeClient."
            )
        self._realtime = realtime_client

        # In-memory daily data downloaded from tsanghi API.
        # Keyed by date_str -> {bare_code -> {close, volume, ...}}
        # Populated on first history_quotes() call, reused within the same day.
        self._daily_data: dict[str, dict[str, dict]] = {}
        self._daily_data_loaded_date: str = ""  # YYYY-MM-DD of last load

        logger.info("IQuantHistoricalAdapter: using tsanghi API for history_quotes")

    @property
    def is_connected(self) -> bool:
        client = getattr(self._realtime, "_client", None)
        return client is not None

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
        """Fetch historical daily data from tsanghi API (daily_latest).

        Downloads all trading dates in [start_date, end_date] via
        tsanghi daily_latest (2 calls per date: XSHG + XSHE).
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

        # Build standard-format response from in-memory data
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
                        # tsanghi volume is in 手; convert to 股 at read time
                        if ind == "volume" and val is not None:
                            val = val * 100
                        indicator_data[ind].append(val)
                d += timedelta(days=1)

            if time_vals:
                table = {"time": time_vals, **indicator_data}
                tables.append({"thscode": full_code, "table": table})

        return {"errorcode": 0, "tables": tables}

    async def _ensure_daily_range(self, start_date: str, end_date: str) -> None:
        """Download missing dates from tsanghi daily_latest API."""
        from src.data.clients.tsanghi_client import TsanghiClient

        d = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_d = datetime.strptime(end_date, "%Y-%m-%d").date()

        dates_needed: list = []
        while d <= end_d:
            ds = d.strftime("%Y-%m-%d")
            if ds not in self._daily_data:
                dates_needed.append(d)
            d += timedelta(days=1)

        if not dates_needed:
            return

        logger.info(
            f"Downloading {len(dates_needed)} dates from tsanghi API "
            f"({dates_needed[0]} ~ {dates_needed[-1]})"
        )

        client = TsanghiClient()
        await client.start()
        try:
            for i, day_date in enumerate(dates_needed):
                ds = day_date.strftime("%Y-%m-%d")
                day_data: dict[str, dict] = {}

                for exchange in ("XSHG", "XSHE"):
                    try:
                        records = await client.daily_latest(exchange, ds)
                    except RuntimeError:
                        continue  # non-trading day
                    for rec in records or []:
                        ticker = str(rec.get("ticker", ""))
                        if not ticker or len(ticker) != 6:
                            continue
                        o = rec.get("open")
                        c = rec.get("close")
                        if o is None or c is None:
                            continue
                        day_data[ticker] = {
                            "open": float(o),
                            "high": float(rec.get("high", o)),
                            "low": float(rec.get("low", o)),
                            "close": float(c),
                            "volume": float(rec.get("volume", 0)),
                        }

                # Store even if empty (marks date as checked, avoids re-fetch)
                self._daily_data[ds] = day_data

                if (i + 1) % 20 == 0:
                    logger.info(f"  tsanghi progress: {i + 1}/{len(dates_needed)}")

            trading_days = sum(1 for d in self._daily_data.values() if d)
            logger.info(f"tsanghi daily data ready: {trading_days} trading days")
        finally:
            await client.stop()

    async def real_time_quotation(
        self,
        codes: str,
        indicators: str,
    ) -> dict[str, Any]:
        """Delegate to realtime client (Tushare or Sina)."""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        return await self._realtime.as_standard_quote_format(code_list, indicators)

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
        """Get trading dates via Tushare trade_cal."""
        from src.data.clients.tushare_realtime import get_tushare_trade_calendar

        return await get_tushare_trade_calendar(start_date, end_date)
