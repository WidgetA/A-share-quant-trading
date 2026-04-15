# === MODULE PURPOSE ===
# Wrap the tsanghi `daily/latest` API into a single per-date fetch that merges
# the two A-share exchanges (XSHG + XSHE).
#
# Concerns owned here:
#   - Lifecycle of the underlying TsanghiClient (start/stop)
#   - Calling both exchanges and merging the result
#   - Returning normalized records with float values
#
# Concerns NOT here:
#   - Storage / persistence
#   - prev_close / suspended fill (data normalization done by pipeline)
#   - Retry strategy beyond what TsanghiClient already implements

from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Any

from src.data.clients.tsanghi_client import TsanghiClient

logger = logging.getLogger(__name__)


class TsanghiDailyFetchError(RuntimeError):
    """Raised when both exchanges fail to return any data for a date."""


class TsanghiDailySource:
    """Async source for daily OHLCV from tsanghi.

    Usage:
        async with TsanghiDailySource() as src:
            records = await src.fetch_day(date(2024, 6, 1))
    """

    EXCHANGES: tuple[str, ...] = ("XSHG", "XSHE")

    def __init__(self, client: TsanghiClient | None = None) -> None:
        self._client = client or TsanghiClient()
        self._owns_client = client is None
        self._started = False

    async def __aenter__(self) -> "TsanghiDailySource":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._started:
            return
        if self._owns_client:
            await self._client.start()
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        if self._owns_client:
            await self._client.stop()
        self._started = False

    async def fetch_day(self, trade_date: date) -> tuple[list[dict[str, Any]], list[str]]:
        """Fetch all daily records for a single trade date.

        Returns:
            ``(records, failed_exchanges)``

            Each record is a normalized dict with keys: ``ticker``, ``open``,
            ``high``, ``low``, ``close``, ``volume``. ``open``/``close`` may be
            ``None`` when tsanghi returns the row but with empty prices —
            callers decide how to handle this.

            ``failed_exchanges`` lists exchange codes whose API call failed,
            each entry formatted as ``"EXCHANGE: error detail"`` so callers
            can surface the original error reason.
        """
        date_str = trade_date.strftime("%Y-%m-%d")

        async def _fetch_exchange(
            exchange: str,
        ) -> tuple[list[dict], str | None]:
            try:
                rows = await self._client.daily_latest(exchange, date_str)
            except RuntimeError as e:
                logger.warning(
                    "tsanghi daily_latest(%s, %s) FAILED: %s",
                    exchange,
                    date_str,
                    e,
                )
                return [], f"{exchange}: {e}"
            return rows or [], None

        results = await asyncio.gather(
            *[_fetch_exchange(ex) for ex in self.EXCHANGES]
        )

        records: list[dict[str, Any]] = []
        failed: list[str] = []
        for rows, error in results:
            if error:
                failed.append(error)
                continue
            for raw in rows:
                ticker = str(raw.get("ticker", ""))
                if not ticker or len(ticker) != 6:
                    continue
                o = raw.get("open")
                h = raw.get("high")
                lo = raw.get("low")
                c = raw.get("close")
                records.append(
                    {
                        "ticker": ticker,
                        "open": float(o) if o is not None else None,
                        "high": float(h) if h is not None else None,
                        "low": float(lo) if lo is not None else None,
                        "close": float(c) if c is not None else None,
                        "volume": float(raw.get("volume", 0) or 0),
                    }
                )

        return records, failed
