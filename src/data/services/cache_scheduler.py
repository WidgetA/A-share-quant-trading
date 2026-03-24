# === MODULE PURPOSE ===
# Background scheduler that auto-fills missing dates in GreptimeDB backtest cache.
# Runs daily at 3am Beijing time: checks for gaps from 2024-01-01 to yesterday,
# downloads missing dates into GreptimeDB.

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
CACHE_START_DATE = date(2024, 1, 1)
SCHEDULE_HOUR = 3  # 3am Beijing time


async def _notify_feishu(message: str) -> None:
    """Best-effort Feishu notification, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_message(message)
    except Exception:
        logger.warning("Failed to send Feishu cache scheduler notification", exc_info=True)


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
    """Background task that auto-fills GreptimeDB backtest cache gaps at 3am daily.

    Usage:
        scheduler = CacheScheduler(app.state)
        task = asyncio.create_task(scheduler.run())
        # On shutdown:
        task.cancel()
    """

    def __init__(self, app_state) -> None:
        self._app_state = app_state
        # Status tracking for dashboard display
        self.next_run_time: str | None = None
        self.last_run_time: str | None = None
        self.last_run_result: str | None = None  # "success" | "failed" | "skipped" | "no_gaps"
        self.last_run_message: str | None = None

    def _get_cache(self):
        return getattr(self._app_state, "backtest_cache", None)

    def get_status(self) -> dict:
        """Return scheduler status for dashboard display."""
        from src.common.config import get_cache_scheduler_enabled

        return {
            "enabled": get_cache_scheduler_enabled(),
            "next_run_time": self.next_run_time,
            "last_run_time": self.last_run_time,
            "last_run_result": self.last_run_result,
            "last_run_message": self.last_run_message,
        }

    async def run(self) -> None:
        """Main loop: sleep until 3am, check gaps, download."""
        logger.info("CacheScheduler started, will run daily at 3am Beijing time")
        try:
            while True:
                now = datetime.now(BEIJING_TZ)
                target = datetime.combine(now.date(), time(SCHEDULE_HOUR, 0), tzinfo=BEIJING_TZ)
                if now >= target:
                    target += timedelta(days=1)

                self.next_run_time = target.strftime("%Y-%m-%d %H:%M")
                wait_secs = (target - now).total_seconds()
                logger.info(
                    f"CacheScheduler: next run at {target.strftime('%Y-%m-%d %H:%M')} "
                    f"({wait_secs / 3600:.1f}h from now)"
                )
                await asyncio.sleep(wait_secs)

                run_time = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

                # Check if scheduler is enabled
                from src.common.config import get_cache_scheduler_enabled

                if not get_cache_scheduler_enabled():
                    logger.info("CacheScheduler: disabled via settings, skipping this run")
                    self.last_run_time = run_time
                    self.last_run_result = "skipped"
                    self.last_run_message = "已关闭，跳过本次执行"
                    continue

                try:
                    result = await self.check_and_fill_gaps()
                    self.last_run_time = run_time
                    if result.get("error"):
                        self.last_run_result = "failed"
                        self.last_run_message = result["error"]
                    elif result["gaps_found"] == 0:
                        self.last_run_result = "no_gaps"
                        self.last_run_message = "无缺失数据"
                    else:
                        self.last_run_result = "success"
                        self.last_run_message = f"已补全 {result['dates_downloaded']} 段"
                except Exception as e:
                    logger.error(f"CacheScheduler gap-fill failed: {e}", exc_info=True)
                    self.last_run_time = run_time
                    self.last_run_result = "failed"
                    self.last_run_message = str(e)[:100]
                    await _notify_feishu(f"[缓存补全] 执行异常\n{e}")

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
        if cache is None or not cache.is_ready:
            msg = "backtest_cache not available, skipping gap check"
            logger.warning(msg)
            return {"gaps_found": 0, "dates_downloaded": 0, "error": msg}

        yesterday = datetime.now(BEIJING_TZ).date() - timedelta(days=1)
        all_trading_days = _get_trading_calendar(CACHE_START_DATE, yesterday)
        if not all_trading_days:
            return {"gaps_found": 0, "dates_downloaded": 0, "error": "No trading days found"}

        # Find dates missing from cache (query existing dates from GreptimeDB)
        existing_dates = await cache._get_existing_daily_dates()
        missing = [d for d in all_trading_days if d not in existing_dates]

        # Also check for minute data gaps (daily exists but minute sparse/missing)
        minute_gaps = await cache.find_minute_gaps()

        if not missing and not minute_gaps:
            logger.info("CacheScheduler: no gaps found, cache is complete")
            return {"gaps_found": 0, "dates_downloaded": 0}

        # Merge daily gaps + minute gaps into unified download ranges
        all_gaps: list[tuple[date, date]] = []
        if missing:
            all_gaps.extend(_group_contiguous_dates(missing))
        if minute_gaps:
            all_gaps.extend(minute_gaps)

        # Deduplicate and sort
        if len(all_gaps) > 1:
            all_gaps.sort()
            merged: list[tuple[date, date]] = [all_gaps[0]]
            for s, e in all_gaps[1:]:
                prev_s, prev_e = merged[-1]
                if s <= prev_e + timedelta(days=3):
                    merged[-1] = (prev_s, max(prev_e, e))
                else:
                    merged.append((s, e))
            all_gaps = merged

        parts = []
        if missing:
            parts.append(f"日线缺失: {len(missing)} 天")
        if minute_gaps:
            parts.append(f"分钟线缺失: {len(minute_gaps)} 段")
        gap_summary = ", ".join(parts)

        logger.info(f"CacheScheduler: {gap_summary}, downloading...")
        await _notify_feishu(f"[缓存补全] 开始补全\n{gap_summary}\n下载范围: {len(all_gaps)} 段")

        total_downloaded = 0
        for range_start, range_end in all_gaps:
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

        total_ranges = len(all_gaps)
        logger.info(
            f"CacheScheduler: done. {gap_summary}, "
            f"{total_downloaded}/{total_ranges} ranges downloaded."
        )

        # Re-check minute gaps after download to see if they're resolved
        remaining_minute_gaps = await cache.find_minute_gaps()

        # Data integrity validation
        integrity_warnings = await cache.validate_integrity()

        failed_ranges = total_ranges - total_downloaded
        if failed_ranges > 0 or remaining_minute_gaps or integrity_warnings:
            fail_parts = []
            if failed_ranges > 0:
                fail_parts.append(f"下载失败: {failed_ranges}/{total_ranges} 段")
            if remaining_minute_gaps:
                fail_parts.append(f"分钟线仍缺失: {len(remaining_minute_gaps)} 段")
            for w in integrity_warnings:
                fail_parts.append(w)
            await _notify_feishu("[缓存补全] 部分失败\n" + "\n".join(fail_parts))
        else:
            await _notify_feishu(f"[缓存补全] 补全成功\n已补全: {total_downloaded} 段")

        total_gaps = len(missing) + len(minute_gaps)
        return {"gaps_found": total_gaps, "dates_downloaded": total_downloaded}


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
