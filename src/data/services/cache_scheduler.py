# === MODULE PURPOSE ===
# Background scheduler that auto-fills missing dates in GreptimeDB storage.
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
DOWNLOAD_TIMEOUT_SECONDS = 4 * 3600  # 4 hours max per range


async def _notify_feishu(message: str) -> None:
    """Best-effort Feishu notification, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_message(message)
    except Exception:
        logger.warning("Failed to send Feishu cache scheduler notification", exc_info=True)


async def _get_trading_calendar(start_date: date, end_date: date) -> list[date]:
    """Get trading days via Tushare trade_cal, fallback to weekdays."""
    try:
        from src.data.clients.tushare_realtime import get_tushare_trade_calendar

        sd = start_date.strftime("%Y-%m-%d")
        ed = end_date.strftime("%Y-%m-%d")
        date_strs = await get_tushare_trade_calendar(sd, ed)
        return sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in date_strs)
    except Exception as e:
        logger.warning(f"Tushare trade_cal failed: {e}, using weekday fallback")
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

    def _get_storage(self):
        return getattr(self._app_state, "storage", None)

    def _get_pipeline(self):
        return getattr(self._app_state, "pipeline", None)

    def _next_run_str(self) -> str:
        """Compute next 3am Beijing time as a human-readable string."""
        now = datetime.now(BEIJING_TZ)
        target = datetime.combine(now.date(), time(SCHEDULE_HOUR, 0), tzinfo=BEIJING_TZ)
        if now >= target:
            target += timedelta(days=1)
        return target.strftime("%m-%d %H:%M")

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
                    await _notify_feishu("[缓存补全·定时任务] 调度器已关闭，跳过本次执行")
                    continue

                # Skip if a manual trigger is already running
                if getattr(self._app_state, "cache_fill_running", False):
                    logger.info(
                        "CacheScheduler: gap-fill already in progress (manual trigger), skipping"
                    )
                    self.last_run_time = run_time
                    self.last_run_result = "skipped"
                    self.last_run_message = "手动补全正在运行，跳过本次执行"
                    await _notify_feishu("[缓存补全·定时任务] 手动补全正在运行，跳过本次执行")
                    continue

                self._app_state.cache_fill_running = True
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
                    next_retry = self._next_run_str()
                    await _notify_feishu(
                        f"[缓存补全·定时任务] 执行异常\n{e}\n\n下次重试: {next_retry}"
                    )
                finally:
                    self._app_state.cache_fill_running = False

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
        storage = self._get_storage()
        pipeline = self._get_pipeline()
        if storage is None or pipeline is None or not storage.is_ready:
            msg = "GreptimeDB storage/pipeline not available, skipping gap check"
            logger.warning(msg)
            return {"gaps_found": 0, "dates_downloaded": 0, "error": msg}

        yesterday = datetime.now(BEIJING_TZ).date() - timedelta(days=1)
        all_trading_days = await _get_trading_calendar(CACHE_START_DATE, yesterday)
        if not all_trading_days:
            return {"gaps_found": 0, "dates_downloaded": 0, "error": "No trading days found"}

        # Find dates missing from storage (query existing dates from GreptimeDB)
        existing_dates = await storage.get_existing_daily_dates()
        missing = [d for d in all_trading_days if d not in existing_dates]

        # Also check for minute data gaps (daily exists but minute sparse/missing)
        minute_gaps = await storage.find_minute_gaps()

        if not missing and not minute_gaps:
            logger.info("CacheScheduler: no gaps found, cache is complete")
            await _notify_feishu("[缓存补全] 缓存完整，无缺失数据")
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

        # Pre-download integrity check
        try:
            integrity_issues = await storage.check_data_integrity()
            if integrity_issues:
                error_issues = [i for i in integrity_issues if i["level"] == "error"]
                if error_issues:
                    issue_lines = [
                        f"  [{i['level']}] {i['message']}" for i in integrity_issues[:10]
                    ]
                    await _notify_feishu(
                        f"[缓存补全] 下载前完整性检查\n"
                        f"发现 {len(integrity_issues)} 个问题:\n" + "\n".join(issue_lines)
                    )
        except Exception as e:
            logger.warning(f"Pre-download integrity check failed: {e}", exc_info=True)

        total_downloaded = 0
        total_ranges = len(all_gaps)
        for idx, (range_start, range_end) in enumerate(all_gaps, 1):
            try:
                logger.info(
                    f"CacheScheduler: downloading {range_start} ~ {range_end} "
                    f"({idx}/{total_ranges})"
                )

                async def _progress(phase, current, total, detail=""):
                    if progress_callback:
                        await progress_callback(
                            f"Downloading {range_start}~{range_end}: {phase} {current}/{total}"
                        )

                await asyncio.wait_for(
                    pipeline.download_prices(range_start, range_end, progress_cb=_progress),
                    timeout=DOWNLOAD_TIMEOUT_SECONDS,
                )
                range_days = len(await _get_trading_calendar(range_start, range_end))
                total_downloaded += range_days
                await _notify_feishu(
                    f"[缓存补全] 进度 {idx}/{total_ranges}\n已完成: {range_start} ~ {range_end}"
                )
            except asyncio.TimeoutError:
                logger.error(
                    f"CacheScheduler: download {range_start}~{range_end} timed out "
                    f"after {DOWNLOAD_TIMEOUT_SECONDS}s"
                )
                await _notify_feishu(
                    f"[缓存补全·定时任务] 超时 ({idx}/{total_ranges})\n"
                    f"{range_start} ~ {range_end} "
                    f"超过{DOWNLOAD_TIMEOUT_SECONDS // 3600}小时未完成，已跳过"
                )
            except Exception as e:
                logger.error(
                    f"CacheScheduler: failed to download {range_start}~{range_end}: {e}",
                    exc_info=True,
                )
                await _notify_feishu(
                    f"[缓存补全·定时任务] 失败 ({idx}/{total_ranges})\n"
                    f"{range_start} ~ {range_end}: {str(e)[:100]}"
                )

        logger.info(
            f"CacheScheduler: done. {gap_summary}, "
            f"{total_downloaded}/{total_ranges} ranges downloaded."
        )

        # Re-check minute gaps after download to see if they're resolved
        remaining_minute_gaps = await storage.find_minute_gaps()

        # Data integrity validation
        integrity_warnings = await storage.validate_integrity()

        failed_ranges = total_ranges - total_downloaded
        if failed_ranges > 0 or remaining_minute_gaps or integrity_warnings:
            fail_parts = []
            if failed_ranges > 0:
                fail_parts.append(f"下载失败: {failed_ranges}/{total_ranges} 段")
            if remaining_minute_gaps:
                fail_parts.append(f"分钟线仍缺失: {len(remaining_minute_gaps)} 段")
            for w in integrity_warnings:
                fail_parts.append(w)
            next_retry = self._next_run_str()
            await _notify_feishu(
                "[缓存补全·定时任务] 部分失败\n"
                + "\n".join(fail_parts)
                + f"\n\n下次重试: {next_retry}"
            )
        else:
            await _notify_feishu(f"[缓存补全·定时任务] 补全成功\n已补全: {total_downloaded} 段")

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
