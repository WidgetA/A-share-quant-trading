# === MODULE PURPOSE ===
# Background scheduler that auto-fills missing dates in GreptimeDB storage.
# Checks gaps on startup (so restarts don't lose a day), then daily at 3am.
# All run status is persisted to GreptimeDB scheduler_log table so it
# survives container restarts.

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
_STARTUP_DELAY_SECONDS = 60  # wait for storage/pipeline to initialize


async def _notify_feishu(message: str) -> None:
    """Best-effort Feishu notification, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_message(message)
    except Exception:
        logger.warning("Failed to send Feishu cache scheduler notification", exc_info=True)


async def _send_gap_detail_report(storage) -> None:
    """Send the detailed per-day gap diagnosis after the cache job finishes.

    Bounded for the 1.58G box: only the most-recent 30 daily-gap days and 30
    minute-gap days are classified per run, and both Tushare calls — suspend_d
    (one per daily-gap day) and stk_mins source-check (one per missing minute
    stock) — are kept well under the 500/min limit. Beyond the caps, items show
    "待核对" (honest), and the full per-day detail is always written to
    data/audit/ regardless.
    """
    try:
        from scripts.diagnose_gaps import run_diagnosis_report

        await run_diagnosis_report(
            storage,
            feishu=True,
            daily_detail_days=30,
            minute_detail_days=30,
            max_minute_source_checks=60,
        )
    except Exception as e:
        logger.error("Failed to send detailed gap diagnosis report: %s", e, exc_info=True)
        await _notify_feishu(f"[数据诊断报告] 自动生成失败\n{e}")


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
    """Background task that auto-fills GreptimeDB backtest cache gaps.

    - On startup: check gaps after a short delay, fill immediately.
    - Then: sleep until 3am daily, check gaps, fill.
    - All run results are persisted to GreptimeDB ``scheduler_log`` table.

    Usage:
        scheduler = CacheScheduler(app.state)
        task = asyncio.create_task(scheduler.run())
        # On shutdown:
        task.cancel()
    """

    _LOG_NAME = "cache_scheduler"

    def __init__(self, app_state) -> None:
        self._app_state = app_state
        # Status tracking for dashboard display (restored from DB on startup)
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

    # ------------------------------------------------------------------
    # Persistent status (GreptimeDB scheduler_log)
    # ------------------------------------------------------------------

    async def _restore_status_from_db(self) -> None:
        """Load last run status from GreptimeDB so dashboard shows history
        even after container restart."""
        storage = self._get_storage()
        if not storage or not storage.is_ready:
            return
        last = await storage.get_last_scheduler_run(self._LOG_NAME)
        if last:
            self.last_run_time = last["time"]
            self.last_run_result = last["result"]
            self.last_run_message = last["message"]
            logger.info(
                "CacheScheduler: restored last run from DB: %s %s",
                self.last_run_time,
                self.last_run_result,
            )

    async def _persist_status(self, trigger: str) -> None:
        """Write current run status to GreptimeDB."""
        storage = self._get_storage()
        if not storage or not storage.is_ready:
            return
        await storage.log_scheduler_run(
            self._LOG_NAME,
            trigger,
            self.last_run_result or "unknown",
            self.last_run_message or "",
        )

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Main loop: check gaps on startup, then daily at 3am.

        The startup check ensures that if the container restarts after 3am
        (OOM, watchtower, crash), gaps are still filled that day instead of
        waiting until the next 3am — which may also be missed.
        """
        logger.info("CacheScheduler started, will run daily at 3am Beijing time")
        try:
            # Wait for storage to be ready, then restore last status from DB
            await asyncio.sleep(_STARTUP_DELAY_SECONDS)
            await self._restore_status_from_db()

            # ── Startup check: fill gaps immediately ──
            await self._run_once("startup")

            # ── Daily 3am loop ──
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
                await self._run_once("scheduled")

        except asyncio.CancelledError:
            logger.info("CacheScheduler cancelled")

    async def _run_once(self, trigger: str) -> None:
        """Execute one gap-check-and-fill cycle.

        Args:
            trigger: 'startup' | 'scheduled' | 'manual'
        """
        run_time = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

        from src.common.config import get_cache_scheduler_enabled

        if not get_cache_scheduler_enabled():
            logger.info("CacheScheduler: disabled via settings, skipping (%s)", trigger)
            self.last_run_time = run_time
            self.last_run_result = "skipped"
            self.last_run_message = "已关闭，跳过本次执行"
            await self._persist_status(trigger)
            await _notify_feishu("[缓存补全·定时任务] 调度器已关闭，跳过本次执行")
            return

        # Skip if any download is already running. Also defer when a snapshot
        # backfill is in flight — it shares pipeline.daily_source/metadata_source,
        # and entering those httpx-client contexts twice concurrently corrupts them.
        active = getattr(self._app_state, "active_download", None)
        if (
            getattr(self._app_state, "cache_fill_running", False)
            or getattr(self._app_state, "snapshot_backfill_running", False)
            or (active is not None and active.state.value == "running")
        ):
            logger.info("CacheScheduler: download already in progress, skipping (%s)", trigger)
            self.last_run_time = run_time
            self.last_run_result = "skipped"
            self.last_run_message = "下载正在运行，跳过本次执行"
            await self._persist_status(trigger)
            await _notify_feishu("[缓存补全·定时任务] 下载正在运行，跳过本次执行")
            return

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
                f"[缓存补全·定时任务] 执行异常 ({trigger})\n{e}\n\n下次重试: {next_retry}"
            )
        finally:
            self._app_state.cache_fill_running = False
            await self._persist_status(trigger)

    # ------------------------------------------------------------------
    # Gap detection + download
    # ------------------------------------------------------------------

    async def check_and_fill_gaps(
        self,
        progress_callback=None,
    ) -> dict:
        """Find missing trading dates from CACHE_START_DATE to yesterday, download them.

        Sends ONE Feishu summary at the end covering: detected gaps, per-range
        result, actual rows written, post-task integrity. No per-range progress
        spam.

        Returns:
            dict with keys: gaps_found, dates_downloaded, error (if any)
        """
        storage = self._get_storage()
        pipeline = self._get_pipeline()
        if storage is None or pipeline is None or not storage.is_ready:
            msg = "GreptimeDB storage/pipeline not available, skipping gap check"
            logger.warning(msg)
            return {"gaps_found": 0, "dates_downloaded": 0, "error": msg}

        task_start = datetime.now(BEIJING_TZ)
        yesterday = task_start.date() - timedelta(days=1)
        all_trading_days = await _get_trading_calendar(CACHE_START_DATE, yesterday)
        if not all_trading_days:
            return {"gaps_found": 0, "dates_downloaded": 0, "error": "No trading days found"}

        # Find dates missing from storage (query existing dates from GreptimeDB)
        existing_dates = await storage.get_existing_daily_dates()
        missing = [d for d in all_trading_days if d not in existing_dates]

        # Also check for minute data gaps (daily exists but minute sparse/missing)
        minute_gaps = await storage.find_minute_gaps()

        # Pre-download row-level integrity (NULL fields, OHLC violations) —
        # collected but only reported at the end as part of the single summary.
        integrity_issues: list[dict] = []
        try:
            integrity_issues = await storage.check_data_integrity()
        except Exception as e:
            logger.warning(f"Pre-download integrity check failed: {e}", exc_info=True)

        if not missing and not minute_gaps:
            logger.info("CacheScheduler: no gaps found, cache is complete")
            await self._send_summary(
                task_start=task_start,
                missing_daily_days=0,
                missing_minute_segments=0,
                range_results=[],
                integrity_issues_pre=integrity_issues,
                storage=storage,
            )
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
            for gap_s, gap_e in all_gaps[1:]:
                prev_s, prev_e = merged[-1]
                if gap_s <= prev_e + timedelta(days=3):
                    merged[-1] = (prev_s, max(prev_e, gap_e))
                else:
                    merged.append((gap_s, gap_e))
            all_gaps = merged

        logger.info(
            f"CacheScheduler: daily missing={len(missing)} days, "
            f"minute gaps={len(minute_gaps)}, "
            f"merged into {len(all_gaps)} download ranges"
        )

        total_downloaded = 0
        total_ranges = len(all_gaps)
        range_results: list[dict] = []
        for idx, (range_start, range_end) in enumerate(all_gaps, 1):
            # Snapshot row counts before/after — detects "no exception but 0
            # rows written" (e.g. stock_snapshot B∪D∪S returned empty).
            daily_before = await storage.count_daily_rows_in_range(range_start, range_end)
            minute_before = await storage.count_minute_rows_in_range(range_start, range_end)

            entry: dict = {
                "range": f"{range_start} ~ {range_end}",
                "status": "ok",
                "error": None,
            }
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
            except asyncio.TimeoutError:
                logger.error(
                    f"CacheScheduler: download {range_start}~{range_end} timed out "
                    f"after {DOWNLOAD_TIMEOUT_SECONDS}s"
                )
                entry["status"] = "timeout"
                entry["error"] = f"超过 {DOWNLOAD_TIMEOUT_SECONDS // 3600}h 未完成"
            except Exception as e:
                logger.error(
                    f"CacheScheduler: failed to download {range_start}~{range_end}: {e}",
                    exc_info=True,
                )
                entry["status"] = "error"
                entry["error"] = str(e)[:120]

            daily_after = await storage.count_daily_rows_in_range(range_start, range_end)
            minute_after = await storage.count_minute_rows_in_range(range_start, range_end)
            entry["daily_rows_added"] = max(0, daily_after - daily_before)
            entry["minute_rows_added"] = max(0, minute_after - minute_before)
            range_results.append(entry)

        logger.info(f"CacheScheduler: done. {total_downloaded}/{total_ranges} ranges ok.")

        await self._send_summary(
            task_start=task_start,
            missing_daily_days=len(missing),
            missing_minute_segments=len(minute_gaps),
            range_results=range_results,
            integrity_issues_pre=integrity_issues,
            storage=storage,
        )

        total_gaps = len(missing) + len(minute_gaps)
        return {"gaps_found": total_gaps, "dates_downloaded": total_downloaded}

    async def _send_summary(
        self,
        *,
        task_start: datetime,
        missing_daily_days: int,
        missing_minute_segments: int,
        range_results: list[dict],
        integrity_issues_pre: list[dict],
        storage,
    ) -> None:
        """Build the single end-of-task Feishu summary and send it."""
        now = datetime.now(BEIJING_TZ)
        elapsed_min = max(1, int((now - task_start).total_seconds() / 60))
        total_ranges = len(range_results)

        # Re-scan post-download (best-effort; failures shouldn't block the report)
        remaining_minute_gaps: list = []
        integrity_warnings: list[str] = []
        try:
            remaining_minute_gaps = await storage.find_minute_gaps()
        except Exception as e:
            logger.warning(f"Post-download find_minute_gaps failed: {e}", exc_info=True)
        try:
            integrity_warnings = await storage.validate_integrity()
        except Exception as e:
            logger.warning(f"Post-download validate_integrity failed: {e}", exc_info=True)

        failed = [r for r in range_results if r["status"] != "ok"]
        zero_row_ok = [
            r
            for r in range_results
            if r["status"] == "ok" and r["daily_rows_added"] == 0 and r["minute_rows_added"] == 0
        ]
        pre_errors = [i for i in integrity_issues_pre if i.get("level") == "error"]

        if total_ranges == 0:
            header = "[缓存补全] ✅ 无缺失数据"
        elif failed or zero_row_ok:
            header = "[缓存补全] ⚠️ 部分失败"
        else:
            header = "[缓存补全] ✅ 补全成功"

        lines = [
            header,
            f"时间: {task_start.strftime('%Y-%m-%d %H:%M')} ~ "
            f"{now.strftime('%H:%M')} ({elapsed_min}min)",
            "",
            "本次任务:",
        ]

        if total_ranges == 0:
            lines.append("  • 未检测到任何缺口")
        else:
            gap_desc = []
            if missing_daily_days:
                gap_desc.append(f"日线 {missing_daily_days} 天")
            if missing_minute_segments:
                gap_desc.append(f"分钟线 {missing_minute_segments} 段")
            lines.append(f"  • 检测缺口: {', '.join(gap_desc) or '仅合并'} → {total_ranges} 段下载")

            success_count = total_ranges - len(failed)
            lines.append(
                f"  • 下载结果: {success_count}/{total_ranges} 段成功"
                + (f"  ({len(failed)} 段失败)" if failed else "")
            )
            for r in failed:
                lines.append(f"      - {r['range']}: {r['status']} {r['error'] or ''}")

            total_daily = sum(r["daily_rows_added"] for r in range_results)
            total_minute = sum(r["minute_rows_added"] for r in range_results)
            flag = "  ⚠️ 有段 0 行入库" if zero_row_ok else ""
            lines.append(f"  • 实际入库: 日线 +{total_daily} 行, 分钟线 +{total_minute} 行{flag}")
            for r in zero_row_ok:
                lines.append(
                    f"      - {r['range']}: 下载未抛错但 0 行入库 "
                    f"(可能 stock_snapshot 三源都拉空 / 非交易日)"
                )

        # Library-wide health
        library_lines: list[str] = []
        if pre_errors:
            sample = [f"[{i['level']}] {i['message']}" for i in pre_errors[:3]]
            suffix = f" ...+{len(pre_errors) - 3}" if len(pre_errors) > 3 else ""
            library_lines.append(
                f"  • 行级错误: {len(pre_errors)} 项 — {'; '.join(sample)}{suffix}"
            )
        for w in integrity_warnings:
            library_lines.append(f"  • {w}")
        if remaining_minute_gaps:
            library_lines.append(f"  • 分钟线下载后剩余: {len(remaining_minute_gaps)} 段")

        if library_lines:
            lines.append("")
            lines.append("整库状态 (含历史遗留):")
            lines.extend(library_lines)

        lines.append("")
        lines.append(f"下次执行: {self._next_run_str()}")

        await _notify_feishu("\n".join(lines))
        await _send_gap_detail_report(storage)


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
