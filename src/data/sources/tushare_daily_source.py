# === MODULE PURPOSE ===
# Wrap Tushare Pro `daily` API into a per-date fetch with the same shape as
# the previous tsanghi-backed source. Single Tushare call returns full-market
# OHLCV for one trading day, replacing tsanghi's two-exchange split.
#
# Concerns owned here:
#   - Lifecycle of the underlying TushareRealtimeClient (start/stop)
#   - Calling Tushare and normalizing each row
#   - Routing API failures through `logger.error` so FeishuLogHandler alerts
#
# Concerns NOT here:
#   - Storage / persistence
#   - prev_close / suspended fill (data normalization handled by pipeline)
#   - Retry strategy beyond what TushareRealtimeClient implements

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from src.common.config import get_tushare_token
from src.data.clients.tushare_realtime import TushareRealtimeClient

logger = logging.getLogger(__name__)


class TushareDailySource:
    """Async source for daily OHLCV from Tushare Pro `daily` API.

    Drop-in replacement for ``TsanghiDailySource``: same ``fetch_day`` return
    shape ``(records, failed_exchanges)``, same ``EXCHANGES`` length used by
    cache_pipeline to detect "all sources failed".

    Tushare is a single endpoint covering both XSHG + XSHE in one call, so
    ``EXCHANGES`` has length 1 — ``failed_exchanges`` either contains that
    one entry on failure, or is empty on success / non-trading-day.

    Usage:
        async with TushareDailySource() as src:
            records, failed = await src.fetch_day(date(2026, 5, 8))
    """

    EXCHANGES: tuple[str, ...] = ("TUSHARE",)

    def __init__(self, client: TushareRealtimeClient | None = None) -> None:
        self._client = client or TushareRealtimeClient(token=get_tushare_token())
        self._owns_client = client is None
        self._started = False

    async def __aenter__(self) -> "TushareDailySource":
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

            Each record: ``{ticker, open, high, low, close, pre_close, volume}``.
            ``open``/``close`` may be ``None`` for halted stocks. Volume is in
            **手** (lots, 1 手=100 股) — same convention as the prior tsanghi
            source, so downstream ×100 conversions stay correct.

            ``failed_exchanges`` is ``[]`` on success or for non-trading days
            (empty results are a normal signal that the date isn't in the
            calendar, not an error). On API error it contains a single
            ``"TUSHARE: <reason>"`` entry; the pipeline's "all sources failed"
            check then triggers exactly like before.
        """
        td_str = trade_date.strftime("%Y%m%d")
        try:
            records = await self._client.fetch_daily(td_str)
        except Exception as e:
            # logger.error → picked up by FeishuLogHandler (5-min dedupe per
            # call site, so a multi-date download with repeated failures
            # alerts once, not N times).
            logger.error(
                "Tushare daily(%s) FAILED: %s",
                td_str,
                e,
            )
            return [], [f"TUSHARE: {e}"]

        return records, []
