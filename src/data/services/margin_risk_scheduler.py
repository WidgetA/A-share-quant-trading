"""Daily post-publication refresh for the production MEWS series.

两融数据由交易所盘后汇总、上游在**下一交易日 09:10（北京）**才发布上一交易日的数据。
调度器因此排在发布之后而不是压着发布时点跑，并以「最新已发布交易日是否已入库」
(``latest_complete == published_through``) 作为追平判据；上游偶尔晚点就按固定间隔
重试有限次，之后交给下一轮（凌晨 3 点流水线 / 次日刷新），绝不无限重试。
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from src.margin_risk.publication import MARGIN_PUBLISH_TIME

logger = logging.getLogger(__name__)

_TZ = ZoneInfo("Asia/Shanghai")
# Start just after the upstream publication (09:10 + 5min = 09:15) so an on-time
# publication is picked up on the first attempt without racing it.  Derived from
# the publication constant so the two can never drift apart.
_PUBLISH_LAG = timedelta(minutes=5)
_RUN_AT = (datetime.combine(date(2000, 1, 1), MARGIN_PUBLISH_TIME) + _PUBLISH_LAG).time()
_RETRY_INTERVAL_SECONDS = 600
_MAX_RETRIES = 6  # ~10:15 at the latest, then the next scheduled pass takes over
_STARTUP_DELAY_SECONDS = 5
_STARTUP_RETRY_SECONDS = 15
_STARTUP_ERROR_RETRY_SECONDS = 300


def _is_caught_up(result: dict | None) -> bool:
    """Whether the newest published trading day is actually stored.

    A call can return OK while the latest published day is still missing (the
    upstream file was late, a single day failed), so "no exception" is not the
    completion test — the stored end matching the published end is.
    """

    if not result or result.get("status") not in {"OK", "PARTIAL"}:
        return False
    published = result.get("published_through")
    if published is None:
        # Nothing published yet (or a caller-supplied window with no target
        # days) — there is nothing left for a retry to fetch.
        return True
    return result.get("latest_complete") == published


class MarginRiskRefreshScheduler:
    """Bootstrap MEWS history, then refresh after the upstream publication."""

    def __init__(self, app_state) -> None:
        self._app_state = app_state
        self.last_result: dict | None = None

    async def _refresh_once(self, *, trigger: str, max_days: int | None) -> dict | None:
        service = getattr(self._app_state, "margin_risk_service", None)
        if service is None:
            logger.warning("MEWS %s refresh waiting: service unavailable", trigger)
            return None
        if getattr(self._app_state, "cache_fill_running", False):
            logger.info("MEWS %s refresh waiting: data maintenance is running", trigger)
            return {"status": "BUSY", "message": "data maintenance is running"}
        try:
            result = await service.audit_and_fill(max_days=max_days)
            self.last_result = result
            logger.info("MEWS %s refresh result: %s", trigger, result)
            return result
        except Exception as exc:  # noqa: BLE001 - scheduler survives the next run
            logger.error("MEWS %s refresh failed: %s", trigger, exc, exc_info=True)
            self.last_result = {"status": "ERROR", "message": str(exc)[:180]}
            return self.last_result

    async def _refresh_until_published_day_is_stored(self, *, trigger: str) -> dict | None:
        """Refresh, then retry a bounded number of times while still behind."""

        result = await self._refresh_once(trigger=trigger, max_days=5)
        for attempt in range(1, _MAX_RETRIES + 1):
            if _is_caught_up(result):
                return result
            logger.info(
                "MEWS %s refresh not caught up (latest_complete=%s, published_through=%s), "
                "retry %d/%d in %ds",
                trigger,
                (result or {}).get("latest_complete"),
                (result or {}).get("published_through"),
                attempt,
                _MAX_RETRIES,
                _RETRY_INTERVAL_SECONDS,
            )
            await asyncio.sleep(_RETRY_INTERVAL_SECONDS)
            result = await self._refresh_once(
                trigger=f"{trigger}+retry{attempt}",
                max_days=5,
            )
        if not _is_caught_up(result):
            logger.warning(
                "MEWS %s refresh still behind after %d retries; leaving it to the next pass",
                trigger,
                _MAX_RETRIES,
            )
        return result

    async def _bootstrap_history(self) -> None:
        """Run the first full, idempotent backfill as soon as storage is ready."""

        await asyncio.sleep(_STARTUP_DELAY_SECONDS)
        while True:
            result = await self._refresh_once(trigger="startup", max_days=None)
            if result is None or result.get("status") == "BUSY":
                await asyncio.sleep(_STARTUP_RETRY_SECONDS)
                continue
            if result.get("status") == "ERROR":
                logger.warning(
                    "MEWS startup refresh will retry in %s seconds",
                    _STARTUP_ERROR_RETRY_SECONDS,
                )
                await asyncio.sleep(_STARTUP_ERROR_RETRY_SECONDS)
                continue
            return

    async def run(self) -> None:
        logger.info("MEWS refresh scheduler started (startup bootstrap + 09:15 Asia/Shanghai)")
        try:
            await self._bootstrap_history()
            while True:
                now = datetime.now(_TZ)
                target = datetime.combine(now.date(), _RUN_AT, tzinfo=_TZ)
                if now >= target:
                    target += timedelta(days=1)
                await asyncio.sleep((target - now).total_seconds())
                await self._refresh_until_published_day_is_stored(trigger="09:15")
        except asyncio.CancelledError:
            logger.info("MEWS refresh scheduler cancelled")
