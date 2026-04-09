# === MODULE PURPOSE ===
# Wrap the Tushare Pro metadata APIs (`bak_basic`, `suspend_d`, `trade_cal`)
# behind a small async interface that returns plain Python types.
#
# Concerns owned here:
#   - Lifecycle of the underlying TushareRealtimeClient (start/stop)
#   - Date format conversion (`date` ↔ "YYYYMMDD" / "YYYY-MM-DD")
#   - Mapping API responses to plain types (set / list / dict)
#
# Concerns NOT here:
#   - Storage / persistence
#   - Caching results across calls (callers decide)
#   - Business decisions about what counts as "complete" data

from __future__ import annotations

import logging
from datetime import date

from src.common.config import get_tushare_token
from src.data.clients.tushare_realtime import TushareRealtimeClient

logger = logging.getLogger(__name__)


class TushareMetadataSource:
    """Async metadata source for the trade calendar / suspensions / stock list.

    Usage:
        async with TushareMetadataSource() as src:
            cal = await src.fetch_trade_calendar(start, end)
            susp = await src.fetch_suspended(date(2024, 6, 1))
            listed = await src.fetch_listed_stocks(date(2024, 6, 1))
    """

    def __init__(self, client: TushareRealtimeClient | None = None) -> None:
        self._client = client or TushareRealtimeClient(token=get_tushare_token())
        self._owns_client = client is None
        self._started = False

    async def __aenter__(self) -> "TushareMetadataSource":
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

    async def fetch_trade_calendar(self, start: date, end: date) -> set[date]:
        """Return the set of trading dates inclusive of both ends."""
        from datetime import datetime as _dt

        date_strs = await self._client.fetch_trade_calendar(
            start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        )
        result: set[date] = set()
        for s in date_strs:
            try:
                result.add(_dt.strptime(s, "%Y-%m-%d").date())
            except ValueError:
                logger.warning("Skipping unparseable trade_cal entry: %r", s)
        return result

    async def fetch_suspended(self, trade_date: date) -> set[str]:
        """Return the set of bare 6-digit stock codes suspended on a date."""
        return await self._client.fetch_suspended_stocks(trade_date.strftime("%Y%m%d"))

    async def fetch_listed_stocks(self, trade_date: date) -> list[str]:
        """Return the authoritative listed stock list for a date (from bak_basic)."""
        return await self._client.fetch_bak_basic(trade_date.strftime("%Y%m%d"))
