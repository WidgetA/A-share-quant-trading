# === MODULE PURPOSE ===
# Background scheduler that auto-fills missing dates in TsanghiBacktestCache.
# Runs daily at 3am Beijing time: checks for gaps from 2024-01-01 to yesterday,
# downloads missing dates, and saves to OSS.

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
CACHE_START_DATE = date(2024, 1, 1)
SCHEDULE_HOUR = 3  # 3am Beijing time


def _get_trading_calendar(start_date: date, end_date: date) -> list[date]:
    """Get trading days via AKShare, fallback to weekdays."""
    try:
        import akshare as ak

        df = ak.tool_trade_date_hist_sina()
        all_dates = set(df["trade_date"].dt.date)
        return sorted(d for d in all_dates if start_date <= d <= end_date)
    except Exception as e:
        logger.warning(f"AKShare trading calendar failed: {e}, using weekday fallback")
        days = []
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:
                days.append(current)
            current += timedelta(days=1)
        return days


class CacheScheduler:
    """Background task that auto-fills TsanghiBacktestCache gaps at 3am daily.

    Usage:
        scheduler = CacheScheduler(app.state)
        task = asyncio.create_task(scheduler.run())
        # On shutdown:
        task.cancel()
    """

    def __init__(self, app_state) -> None:
        self._app_state = app_state

    def _get_cache(self):
        return getattr(self._app_state, "tsanghi_cache", None)

    async def run(self) -> None:
        """Main loop: sleep until 3am, check gaps, download, save to OSS."""
        logger.info("CacheScheduler started, will run daily at 3am Beijing time")
        try:
            while True:
                now = datetime.now(BEIJING_TZ)
                target = datetime.combine(now.date(), time(SCHEDULE_HOUR, 0), tzinfo=BEIJING_TZ)
                if now >= target:
                    target += timedelta(days=1)

                wait_secs = (target - now).total_seconds()
                logger.info(
                    f"CacheScheduler: next run at {target.strftime('%Y-%m-%d %H:%M')} "
                    f"({wait_secs / 3600:.1f}h from now)"
                )
                await asyncio.sleep(wait_secs)

                try:
                    await self.check_and_fill_gaps()
                except Exception as e:
                    logger.error(f"CacheScheduler gap-fill failed: {e}", exc_info=True)

        except asyncio.CancelledError:
            logger.info("CacheScheduler cancelled")

    async def check_and_fill_gaps(
        self,
        progress_callback=None,
    ) -> dict:
        """Find missing trading dates from CACHE_START_DATE to yesterday, download them.

        Returns:
            dict with keys: gaps_found, dates_downloaded, error (if any)
        """
        cache = self._get_cache()
        if cache is None:
            msg = "tsanghi_cache not available, skipping gap check"
            logger.warning(msg)
            return {"gaps_found": 0, "dates_downloaded": 0, "error": msg}

        yesterday = datetime.now(BEIJING_TZ).date() - timedelta(days=1)
        all_trading_days = _get_trading_calendar(CACHE_START_DATE, yesterday)
        if not all_trading_days:
            return {"gaps_found": 0, "dates_downloaded": 0, "error": "No trading days found"}

        # Find dates missing from cache
        cached_dates = set()
        for code_dates in cache._daily.values():
            for ds in code_dates:
                try:
                    cached_dates.add(datetime.strptime(ds, "%Y-%m-%d").date())
                except ValueError:
                    pass

        missing = [d for d in all_trading_days if d not in cached_dates]
        if not missing:
            logger.info("CacheScheduler: no gaps found, cache is complete")
            return {"gaps_found": 0, "dates_downloaded": 0}

        logger.info(f"CacheScheduler: found {len(missing)} missing dates, downloading...")

        # Group contiguous dates into ranges to minimize download calls
        ranges = _group_contiguous_dates(missing)

        total_downloaded = 0
        for range_start, range_end in ranges:
            try:
                logger.info(f"CacheScheduler: downloading {range_start} ~ {range_end}")

                async def _progress(phase, current, total):
                    if progress_callback:
                        await progress_callback(
                            f"Downloading {range_start}~{range_end}: {phase} {current}/{total}"
                        )

                await cache.download_prices(range_start, range_end, _progress)
                range_days = len(_get_trading_calendar(range_start, range_end))
                total_downloaded += range_days
            except Exception as e:
                logger.error(
                    f"CacheScheduler: failed to download {range_start}~{range_end}: {e}",
                    exc_info=True,
                )

        # Save to OSS after downloading
        if total_downloaded > 0:
            try:
                oss_key = await cache.save_to_oss()
                if oss_key:
                    logger.info(f"CacheScheduler: saved to OSS, key={oss_key}")
                else:
                    logger.warning("CacheScheduler: OSS save returned None (OSS not configured?)")
            except Exception as e:
                logger.error(f"CacheScheduler: OSS save failed: {e}", exc_info=True)

        logger.info(
            f"CacheScheduler: done. {len(missing)} gaps found, {total_downloaded} dates downloaded."
        )
        return {"gaps_found": len(missing), "dates_downloaded": total_downloaded}


def _group_contiguous_dates(dates: list[date]) -> list[tuple[date, date]]:
    """Group sorted dates into contiguous ranges."""
    if not dates:
        return []

    sorted_dates = sorted(dates)
    ranges: list[tuple[date, date]] = []
    range_start = sorted_dates[0]
    range_end = sorted_dates[0]

    for d in sorted_dates[1:]:
        if (d - range_end).days <= 3:  # Allow small gaps (weekends)
            range_end = d
        else:
            ranges.append((range_start, range_end))
            range_start = d
            range_end = d

    ranges.append((range_start, range_end))
    return ranges
