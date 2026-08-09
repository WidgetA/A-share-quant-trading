"""Daily post-publication refresh for the production MEWS series."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_TZ = ZoneInfo("Asia/Shanghai")
_RUN_AT = time(8, 50)  # Tushare documents margin data as updating around 08:30.


class MarginRiskRefreshScheduler:
    """Refresh recent missing MEWS days after the upstream margin publication."""

    def __init__(self, app_state) -> None:
        self._app_state = app_state

    async def run(self) -> None:
        logger.info("MEWS refresh scheduler started (08:50 Asia/Shanghai)")
        try:
            while True:
                now = datetime.now(_TZ)
                target = datetime.combine(now.date(), _RUN_AT, tzinfo=_TZ)
                if now >= target:
                    target += timedelta(days=1)
                await asyncio.sleep((target - now).total_seconds())
                service = getattr(self._app_state, "margin_risk_service", None)
                if service is None:
                    logger.warning("MEWS 08:50 refresh skipped: service unavailable")
                    continue
                if getattr(self._app_state, "cache_fill_running", False):
                    logger.info("MEWS 08:50 refresh skipped: data maintenance is running")
                    continue
                try:
                    result = await service.audit_and_fill(max_days=5)
                    logger.info("MEWS 08:50 refresh result: %s", result)
                except Exception as exc:  # noqa: BLE001 - scheduler survives next day
                    logger.error("MEWS 08:50 refresh failed: %s", exc, exc_info=True)
        except asyncio.CancelledError:
            logger.info("MEWS refresh scheduler cancelled")
