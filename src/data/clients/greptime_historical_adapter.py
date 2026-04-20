# === MODULE PURPOSE ===
# Adapter that exposes ``GreptimeBacktestStorage`` through the
# ``HistoricalDataProvider`` Protocol used by ML scanner.
#
# This file is a thin read-only translator:
#   - It does NOT call upstream APIs
#   - It does NOT write data
#   - It does NOT know about download orchestration
#
# All it does is project the storage layer's columns into the THS-shaped
# response format the scanner code expects.

from __future__ import annotations

import logging
from typing import Any

from src.data.clients.greptime_storage import (
    GreptimeBacktestStorage,
    ts_to_date,
)

logger = logging.getLogger(__name__)


class GreptimeHistoricalAdapter:
    """Implements ``HistoricalDataProvider`` for backtest mode.

    Reads from ``GreptimeBacktestStorage`` and returns data in the
    ``history_quotes`` response shape that the scanner code already
    understands.
    """

    def __init__(self, storage: GreptimeBacktestStorage) -> None:
        self._storage = storage

    @property
    def is_connected(self) -> bool:
        return self._storage.is_ready

    async def start(self) -> None:
        # No-op: storage lifecycle is owned by the application.
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
        """Return cached daily data in ``history_quotes`` format."""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        indicator_list = [ind.strip() for ind in indicators.split(",")]
        tables: list[dict[str, Any]] = []

        # Map indicator names to DB column names
        ind_to_col = {
            "open": "open_price",
            "high": "high_price",
            "low": "low_price",
            "close": "close_price",
            "preClose": "pre_close",
            "volume": "vol",
            "amount": "amount",
            "turnoverRatio": "turnover_ratio",
            "is_suspended": "is_suspended",
        }

        for full_code in code_list:
            bare = full_code.split(".")[0]
            rows = await self._storage.get_daily_for_code(bare, start_date, end_date)

            if not rows:
                continue

            time_vals: list[str] = []
            indicator_data: dict[str, list] = {ind: [] for ind in indicator_list}

            for r in rows:
                ts_date = ts_to_date(r["ts"])
                time_vals.append(ts_date.strftime("%Y-%m-%d"))
                for ind in indicator_list:
                    col = ind_to_col.get(ind, ind)
                    val = r[col] if col in r.keys() else None
                    # tsanghi volume is in 手; convert to 股 at read time
                    if ind == "volume" and val is not None:
                        val = val * 100
                    indicator_data[ind].append(val)

            tables.append({"thscode": full_code, "table": {"time": time_vals, **indicator_data}})

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
        """Return trading dates via Tushare ``trade_cal``.

        Falls back to inferring from existing daily data in storage when the
        Tushare call fails (e.g. token quota exhausted).
        """
        from src.data.clients.tushare_realtime import get_tushare_trade_calendar

        try:
            return await get_tushare_trade_calendar(start_date, end_date)
        except Exception as e:  # safety: ignore — DB inference is the fallback
            logger.warning("Tushare trade_cal failed: %s, falling back to DB inference", e)

        all_dates = await self._storage.list_distinct_daily_dates()
        sd = start_date if isinstance(start_date, str) else start_date.strftime("%Y-%m-%d")
        ed = end_date if isinstance(end_date, str) else end_date.strftime("%Y-%m-%d")
        return [d.strftime("%Y-%m-%d") for d in all_dates if sd <= d.strftime("%Y-%m-%d") <= ed]
