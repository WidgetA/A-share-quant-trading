"""Daily post-publication refresh for the production MEWS series."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_TZ = ZoneInfo("Asia/Shanghai")
_RUN_AT = time(8, 50)  # Tushare documents margin data as updating around 08:30.
_STARTUP_DELAY_SECONDS = 5
_STARTUP_RETRY_SECONDS = 15
_STARTUP_ERROR_RETRY_SECONDS = 300


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
        logger.info("MEWS refresh scheduler started (startup bootstrap + 08:50 Asia/Shanghai)")
        try:
            await self._bootstrap_history()
            while True:
                now = datetime.now(_TZ)
                target = datetime.combine(now.date(), _RUN_AT, tzinfo=_TZ)
                if now >= target:
                    target += timedelta(days=1)
                await asyncio.sleep((target - now).total_seconds())
                await self._refresh_once(trigger="08:50", max_days=5)
        except asyncio.CancelledError:
            logger.info("MEWS refresh scheduler cancelled")
