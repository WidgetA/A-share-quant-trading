# === MODULE PURPOSE ===
# Wrap Tushare Pro `stk_mins` into a batched async iterator. Owns its own
# TushareRealtimeClient lifecycle and enforces the rate limit (200 req/min).
#
# Concerns owned here:
#   - 20 codes per request batching (reduces 3000+ calls to ~160)
#   - Rate limiting (sleep 0.5s between requests)
#   - Symbol normalization (bare code → ts_code)
#   - Returning raw bar dicts grouped by stock
#
# Concerns NOT here:
#   - Aggregation (the business-layer aggregator decides which window)
#   - Storage / persistence
#   - Cancellation logic above the per-batch level (caller checks between yields)

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import date
from typing import Any

from src.common.config import get_tushare_token
from src.data.clients.tushare_realtime import TushareRealtimeClient

logger = logging.getLogger(__name__)


class MinuteBatchResult:
    """Result of one batch fetch.

    Attributes:
        ok: dict[code, list[bar_dict]] — codes that received bars
        empty: list[code] — codes the API returned but with no bars
        unknown_exchange: list[code] — codes whose ts_code conversion failed
        error: Optional[str] — set if the entire batch raised; ok/empty are then empty
        error_codes: list[code] — codes affected by the error
    """

    __slots__ = ("ok", "empty", "unknown_exchange", "error", "error_codes", "api_bar_count")

    def __init__(self) -> None:
        self.ok: dict[str, list[dict[str, Any]]] = {}
        self.empty: list[str] = []
        self.unknown_exchange: list[str] = []
        self.error: str | None = None
        self.error_codes: list[str] = []
        self.api_bar_count: int = 0


class TushareMinuteSource:
    """Async source for raw 1-min bars from Tushare Pro stk_mins.

    Usage:
        async with TushareMinuteSource() as src:
            async for batch in src.fetch_batches(codes, start, end):
                ... # process batch.ok / batch.empty / batch.error
    """

    BATCH_SIZE = 100
    REQUEST_DELAY = 0.5  # seconds; ≤ 200 req/min

    def __init__(
        self,
        client: TushareRealtimeClient | None = None,
        *,
        batch_size: int | None = None,
        request_delay: float | None = None,
    ) -> None:
        self._client = client or TushareRealtimeClient(token=get_tushare_token())
        self._owns_client = client is None
        self._started = False
        if batch_size is not None:
            self.BATCH_SIZE = batch_size
        if request_delay is not None:
            self.REQUEST_DELAY = request_delay

    async def __aenter__(self) -> "TushareMinuteSource":
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

    async def fetch_batches(
        self,
        codes: list[str],
        start: date,
        end: date,
    ) -> AsyncIterator[MinuteBatchResult]:
        """Yield results in batches of ``BATCH_SIZE`` codes.

        Each yielded ``MinuteBatchResult`` corresponds to one HTTP call.
        After each yield, sleeps ``REQUEST_DELAY`` seconds.
        """
        start_str = start.strftime("%Y-%m-%d") + " 09:00:00"
        end_str = end.strftime("%Y-%m-%d") + " 15:00:00"

        for i in range(0, len(codes), self.BATCH_SIZE):
            batch_codes = codes[i : i + self.BATCH_SIZE]
            result = MinuteBatchResult()

            ts_to_bare: dict[str, str] = {}
            for code in batch_codes:
                try:
                    ts_to_bare[TushareRealtimeClient._to_ts_code(code)] = code
                except ValueError:
                    result.unknown_exchange.append(code)

            if not ts_to_bare:
                yield result
                await asyncio.sleep(self.REQUEST_DELAY)
                continue

            ts_codes_str = ",".join(ts_to_bare.keys())
            try:
                bars = await self._client.stk_mins(
                    ts_codes_str,
                    freq="1min",
                    start_date=start_str,
                    end_date=end_str,
                    limit=5_000_000,
                )
            except Exception as e:  # TushareRealtimeError or network
                result.error = str(e)
                result.error_codes = list(ts_to_bare.values())
                yield result
                await asyncio.sleep(self.REQUEST_DELAY)
                continue

            result.api_bar_count = len(bars)
            grouped: dict[str, list[dict[str, Any]]] = {ts: [] for ts in ts_to_bare}
            for bar in bars:
                ts = str(bar.get("ts_code", ""))
                if ts in grouped:
                    grouped[ts].append(bar)

            for ts_code, bare in ts_to_bare.items():
                code_bars = grouped.get(ts_code, [])
                if code_bars:
                    result.ok[bare] = code_bars
                else:
                    result.empty.append(bare)

            yield result
            await asyncio.sleep(self.REQUEST_DELAY)
