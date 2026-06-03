# === MODULE PURPOSE ===
# CacheScheduler runs the unified DAILY DATA-MAINTENANCE pipeline at 3am Beijing
# (see docs/data-integrity-pipeline.md §4.1). One sequential pass:
#   ① load-tushare 刷名单 → ② kimi 核新代码 + 喂换号对应表 → ③ 重建真值表(全历史·查漏)
#   → ④ 索引驱动补缺 → ⑤ 重建补过的天(确认) → 一条飞书汇总.
# "先全量查漏、再精准补缺." Steps are failure-isolated; only 3am (no startup run);
# all run status persisted to GreptimeDB scheduler_log so it survives restarts.
# (The 4am ListingVerifyScheduler loop is retired — kimi is step ② here.)
# `check_and_fill_gaps` (the old coarse whole-day fill) is kept ONLY for the
# model-training pre-fill + the manual /api/cache/trigger; the nightly path no
# longer uses it.

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
# Daily coverage starts 2023-01-01 by default (backfill/rebuild endpoints reuse
# this); pass ?start= to an endpoint to go earlier.
CACHE_START_DATE = date(2023, 1, 1)
SCHEDULE_HOUR = 3  # 3am Beijing time
DOWNLOAD_TIMEOUT_SECONDS = 4 * 3600  # 4h max per range (coarse check_and_fill_gaps)
_STARTUP_DELAY_SECONDS = 60  # wait for storage/pipeline to initialize

# Per-step time bounds for the daily-maintenance pipeline. A step that exceeds its
# bound is recorded as a timeout + the pipeline continues (failure isolation).
_STEP_TIMEOUT_LOAD = 15 * 60  # ① load-tushare (2 Tushare calls + writes)
_STEP_TIMEOUT_KIMI = 6 * 3600  # ② kimi serial, up to MAX_CODES_PER_RUN
_STEP_TIMEOUT_REBUILD = 90 * 60  # ③ full-history rebuild (~1640 Tushare calls)
_STEP_TIMEOUT_FILL = 3 * 3600  # ④ index-driven daily fill
_STEP_TIMEOUT_CONFIRM = 30 * 60  # ⑤ rebuild only the touched dates
# How many of kimi's per-code "怎么回事" lines to inline in the unified summary.
_KIMI_FINDINGS_IN_SUMMARY = 15


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
    """Runs the unified daily data-maintenance pipeline (see _run_pipeline / docs §4.1).

    - Daily at 3am ONLY — no startup run (watchtower restarts must not re-trigger it).
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
        """Main loop: fill gaps daily at 3am (recent window only).

        Deliberately NO startup fill: with frequent watchtower redeploys a
        startup fill would run on every restart, holding cache_fill_running and
        blocking kimi/path-B. Recent freshness is the 3am run's job; historical
        backfill is deliberate (index-driven endpoints).
        """
        logger.info("CacheScheduler started, will run daily at 3am Beijing time")
        try:
            # Wait for storage to be ready, then restore last status from DB
            await asyncio.sleep(_STARTUP_DELAY_SECONDS)
            await self._restore_status_from_db()

            # NO startup fill. watchtower auto-deploys often, so a startup fill
            # would run on EVERY restart — holding cache_fill_running (and thus
            # blocking kimi/path-B) while it scans/downloads. Recent freshness is
            # the 3am run's job; historical backfill is deliberate (endpoints).
            # A restart no longer triggers any fill → kimi is free immediately.

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

    # Data-maintenance flags the pipeline OWNS while it runs, so a manually-
    # triggered op (rebuild / backfill / load-listing / cache fill) defers.
    _OWNED_FLAGS = (
        "cache_fill_running",
        "calendar_rebuild_running",
        "backfill_daily_running",
        "load_listing_running",
    )

    async def _run_once(self, trigger: str, force_enabled: bool = False) -> None:
        """Run one daily-maintenance pass (see _run_pipeline). Wraps it with the
        enable check, the busy-guard, the owned flags, and status persistence.

        Args:
            trigger: 'scheduled' (3am) | 'manual' — this scheduler has no startup run.
            force_enabled: bypass the on/off toggle (manual trigger asked explicitly).
        """
        run_time = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

        from src.common.config import get_cache_scheduler_enabled

        if not force_enabled and not get_cache_scheduler_enabled():
            logger.info("CacheScheduler: disabled via settings, skipping (%s)", trigger)
            self.last_run_time = run_time
            self.last_run_result = "skipped"
            self.last_run_message = "已关闭，跳过本次执行"
            await self._persist_status(trigger)
            await _notify_feishu("【每日数据维护】调度器已关闭，跳过本次执行")
            return

        # Defer if ANY data op is already in flight (manual rebuild / backfill /
        # load-listing / snapshot, or a running download) — the pipeline owns all
        # of them below, so manual triggers correctly wait their turn.
        active = getattr(self._app_state, "active_download", None)
        busy = (*self._OWNED_FLAGS, "snapshot_backfill_running")
        if any(getattr(self._app_state, f, False) for f in busy) or (
            active is not None and active.state.value == "running"
        ):
            logger.info("CacheScheduler: a data op is already running, skipping (%s)", trigger)
            self.last_run_time = run_time
            self.last_run_result = "skipped"
            self.last_run_message = "已有数据任务在运行，跳过本次执行"
            await self._persist_status(trigger)
            await _notify_feishu("【每日数据维护】已有数据任务在运行，跳过本次执行")
            return

        for f in self._OWNED_FLAGS:
            setattr(self._app_state, f, True)
        try:
            result, message = await self._run_pipeline(trigger)
            self.last_run_time = run_time
            self.last_run_result = result
            self.last_run_message = message
        except Exception as e:
            logger.error("CacheScheduler daily maintenance failed: %s", e, exc_info=True)
            self.last_run_time = run_time
            self.last_run_result = "failed"
            self.last_run_message = str(e)[:120]
            next_retry = self._next_run_str()
            await _notify_feishu(
                f"【每日数据维护】执行异常 ({trigger})\n{e}\n\n下次重试: {next_retry}"
            )
        finally:
            for f in self._OWNED_FLAGS:
                setattr(self._app_state, f, False)
            await self._persist_status(trigger)

    # ------------------------------------------------------------------
    # Daily data-maintenance pipeline (docs/data-integrity-pipeline.md §4.1)
    # ------------------------------------------------------------------

    @staticmethod
    async def _bounded(coro, timeout: int):
        """Run one pipeline step time-bounded + failure-isolated → (ok, value|errmsg).

        On timeout/exception the coro's effect is whatever completed before the
        cancel; the caller records the step as failed and (for ③) skips downstream.
        """
        try:
            return True, await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            return False, f"超时(>{timeout // 60}分钟未完成)"
        except Exception as e:  # noqa: BLE001 — isolate per-step; pipeline continues
            logger.error("maintenance step failed: %s", e, exc_info=True)
            return False, str(e)[:120]

    async def _run_pipeline(self, trigger: str) -> tuple[str, str]:
        """The unified daily data-maintenance pass — 先全量查漏、再精准补缺:
        ① load-tushare 刷名单 → ② kimi 核新代码 + 喂换号对应表 →
        ③ 重建真值表(全历史·查漏) → ④ 索引驱动补缺 → ⑤ 重建补过的天(确认).

        Failure-isolated: a failed step is logged + recorded + the pass continues —
        EXCEPT ④/⑤ are skipped if ③ (rebuild) failed (never fill on a stale index).
        Sends ONE Feishu summary + the per-day gap diagnosis. Returns (result, message).
        """
        from scripts.load_listing_from_tushare import run_load_listing
        from src.common.config import get_listing_verify_enabled, get_tushare_token
        from src.data.clients.tushare_realtime import TushareRealtimeClient
        from src.data.services.kimi_listing_verifier import kimi_available
        from src.data.services.trading_calendar import rebuild_calendar

        storage = self._get_storage()
        pipeline = self._get_pipeline()
        if storage is None or pipeline is None or not storage.is_ready:
            await _notify_feishu("【每日数据维护】GreptimeDB 未就绪，跳过本次执行")
            return "failed", "GreptimeDB 未就绪"

        task_start = datetime.now(BEIJING_TZ)
        # daily interface only goes to T-1 — ending at today would falsely mark all
        # of today's roster as missing (today's daily isn't published yet).
        yesterday = task_start.date() - timedelta(days=1)
        steps: list[tuple[str, str, str]] = []  # (label, status, detail)
        kimi_findings: list[dict] = []

        client = TushareRealtimeClient(token=get_tushare_token())
        await client.start()
        try:
            # ① 刷名单 (Tushare stock_basic → 新股/退市进 roster;保留 kimi 占位)
            ok, r = await self._bounded(
                run_load_listing(storage, feishu=False, client=client), _STEP_TIMEOUT_LOAD
            )
            if ok:
                steps.append(
                    (
                        "① 刷名单",
                        "成功",
                        f"在市 {r['listed']} + 退市 {r['delisted']} = {r['total_entries']} 只"
                        + (
                            f"(保留 kimi 占位 {r['preserved_kimi']})"
                            if r.get("preserved_kimi")
                            else ""
                        ),
                    )
                )
            else:
                steps.append(("① 刷名单", "失败", r))

            # ② kimi 核没核过的新代码 + 回填换号对应表 (gated by the verify toggle)
            lv = getattr(self._app_state, "listing_verify_scheduler", None)
            if not get_listing_verify_enabled():
                steps.append(("② 核身份(kimi)", "跳过", "核身份开关关闭"))
            elif lv is None or not kimi_available():
                steps.append(("② 核身份(kimi)", "跳过", "kimi 不可用"))
            else:
                # Mark in_progress so the status card + manual endpoints reflect
                # that kimi is running (verify_unverified itself doesn't set it).
                lv.in_progress = True
                try:
                    ok, r = await self._bounded(
                        lv.verify_unverified(quiet=True), _STEP_TIMEOUT_KIMI
                    )
                finally:
                    lv.in_progress = False
                if not ok:
                    steps.append(("② 核身份(kimi)", "失败", r))
                else:
                    kimi_findings = r.get("findings", []) or []
                    te = r.get("tool_errors", 0)
                    if r.get("error"):
                        steps.append(("② 核身份(kimi)", "失败", r["error"][:100]))
                    elif r.get("checked", 0) == 0:
                        steps.append(("② 核身份(kimi)", "成功", "无新代码需核"))
                    else:
                        detail = (
                            f"核 {r['checked']} 只 / 写入上市日 {r['verified']} / "
                            f"查不到 {r['failed']} / 还剩 {r['remaining']}"
                        )
                        # Surface sub-threshold kimi tool/auth errors — never report a
                        # partially-broken kimi as plain 成功 (CLAUDE.md §12). They are
                        # NOT 查不到; those codes stay unverified and retry next night.
                        if te:
                            steps.append(
                                (
                                    "② 核身份(kimi)",
                                    "警告",
                                    detail + f" / 出错(超时或认证·未写) {te} 只 — 请检查 kimi 凭证",
                                )
                            )
                        else:
                            steps.append(("② 核身份(kimi)", "成功", detail))

            # ③ 查漏 — 重建真值表 全历史 2023→T-1
            ok, r = await self._bounded(
                rebuild_calendar(storage, start=CACHE_START_DATE, end=yesterday, client=client),
                _STEP_TIMEOUT_REBUILD,
            )
            rebuild_ok = ok
            if ok:
                steps.append(
                    (
                        "③ 查漏·重建真值表",
                        "成功",
                        f"{r['days']} 个交易日 / {r['rows']:,} 行 / 待修 {r['problem_rows']:,}",
                    )
                )
            else:
                steps.append(("③ 查漏·重建真值表", "失败", r))

            # ④ 补缺 — 索引驱动,只补 missing/wrong_suspended (skip if ③ failed)
            processed_dates: list = []
            if not rebuild_ok:
                steps.append(("④ 补缺", "跳过", "重建失败，不在过期索引上补"))
            else:
                ok, r = await self._bounded(
                    pipeline.fill_daily_from_calendar(quiet=True), _STEP_TIMEOUT_FILL
                )
                if not ok:
                    steps.append(("④ 补缺", "失败", r))
                else:
                    processed_dates = r.get("processed_dates", []) or []
                    if r["dates"] == 0:
                        steps.append(("④ 补缺", "成功", "无缺口可补"))
                    else:
                        steps.append(
                            (
                                "④ 补缺",
                                "成功",
                                f"补回 {r['filled']:,} 行 / 跨 {r['dates']} 个缺口日",
                            )
                        )

            # ⑤ 确认 — 只重建 ④ 动过的那几天 (skip if nothing was touched)
            if processed_dates:
                ok, r = await self._bounded(
                    rebuild_calendar(storage, trading_days=processed_dates, client=client),
                    _STEP_TIMEOUT_CONFIRM,
                )
                if ok:
                    steps.append(
                        (
                            "⑤ 确认·重建补过的天",
                            "成功",
                            f"{r['days']} 天 / 仍待修 {r['problem_rows']:,}",
                        )
                    )
                else:
                    steps.append(("⑤ 确认·重建补过的天", "失败", r))
            else:
                steps.append(("⑤ 确认", "跳过", "无补缺，免重建"))
        finally:
            await client.stop()

        # final library snapshot (read-only — the same view /calendar/status returns)
        final_state: dict = {}
        try:
            summary = await storage.get_trading_calendar_summary()
            final_state = summary.get("by_daily_state", {})
        except Exception as e:  # noqa: BLE001
            logger.warning("maintenance: final calendar summary failed: %s", e)

        result, message = await self._send_pipeline_summary(
            task_start, steps, kimi_findings, final_state
        )
        # the per-day gap diagnosis (incl. minute) is still surfaced for observability
        await _send_gap_detail_report(storage)
        return result, message

    async def _send_pipeline_summary(
        self,
        task_start: datetime,
        steps: list[tuple[str, str, str]],
        kimi_findings: list[dict],
        final_state: dict,
    ) -> tuple[str, str]:
        """Build + send the ONE unified Feishu summary. Returns (result, message)."""
        now = datetime.now(BEIJING_TZ)
        elapsed_min = max(1, int((now - task_start).total_seconds() / 60))
        failed_steps = [s for s in steps if s[1] in ("失败", "超时")]
        warn_steps = [s for s in steps if s[1] == "警告"]
        if failed_steps:
            header = "【每日数据维护】⚠️ 部分步骤异常"
        elif warn_steps:
            header = "【每日数据维护】⚠️ 完成(有告警)"
        else:
            header = "【每日数据维护】✅ 完成"

        marks = {"成功": "✅", "失败": "❌", "超时": "⏱", "跳过": "⏭", "警告": "⚠️"}
        lines = [
            header,
            f"时间: {task_start.strftime('%Y-%m-%d %H:%M')} ~ {now.strftime('%H:%M')} "
            f"({elapsed_min}min) · 先全量查漏、再精准补缺",
            "",
        ]
        lines += [f"{marks.get(st, '·')} {label}: {detail}" for label, st, detail in steps]

        if final_state:
            _labels = {
                "ok": "正常",
                "missing": "真缺待补",
                "wrong_suspended": "标错停牌",
                "wrong_traded": "标错交易",
                "orphan": "有数据却不在册",
                "source_none": "源头也无(接口不全·可接受)",
            }
            picture = " / ".join(
                f"{_labels.get(k, k)} {v:,}" for k, v in sorted(final_state.items()) if v
            )
            lines += ["", f"最终真值表: {picture}"]

        if kimi_findings:
            lines += ["", "本轮 kimi 查证(逐只):"]
            for f in kimi_findings[:_KIMI_FINDINGS_IN_SUMMARY]:
                name = f.get("name") or "(名字未知)"
                lines.append(f"· {f.get('code')} {name}｜{f.get('status')}｜{f.get('note')}")
            extra = len(kimi_findings) - min(len(kimi_findings), _KIMI_FINDINGS_IN_SUMMARY)
            if extra > 0:
                lines.append(f"…另有 {extra} 只，完整见「逐只情况」接口")

        lines += ["", f"下次执行: {self._next_run_str()}"]
        await _notify_feishu("\n".join(lines))

        if failed_steps:
            return "failed", "异常步骤: " + "、".join(s[0] for s in failed_steps)
        if warn_steps:
            return "success", "完成(有告警: " + "、".join(s[0] for s in warn_steps) + ")"
        return "success", "维护完成(查漏→补缺)"

    # ------------------------------------------------------------------
    # Gap detection + download
    # ------------------------------------------------------------------

    async def check_and_fill_gaps(
        self,
        progress_callback=None,
        start_date: date | None = None,
    ) -> dict:
        """Coarse whole-day gap fill (the OLD path). Find trading dates from
        ``start_date`` to yesterday with NO rows in backtest_daily, download them.

        RETIRED from the nightly path — the 3am pipeline now does the precise
        truth-table 查漏→补缺 (rebuild + fill_daily_from_calendar). Kept only for its
        SOLE remaining caller: the model-training pre-fill (model_training_scheduler),
        which calls it with no args ⇒ ``start_date=None`` ⇒ CACHE_START_DATE full range.

        Sends ONE Feishu summary at the end. Returns {gaps_found, dates_downloaded, error?}.
        """
        storage = self._get_storage()
        pipeline = self._get_pipeline()
        if storage is None or pipeline is None or not storage.is_ready:
            msg = "GreptimeDB storage/pipeline not available, skipping gap check"
            logger.warning(msg)
            return {"gaps_found": 0, "dates_downloaded": 0, "error": msg}

        start = start_date or CACHE_START_DATE
        task_start = datetime.now(BEIJING_TZ)
        yesterday = task_start.date() - timedelta(days=1)
        all_trading_days = await _get_trading_calendar(start, yesterday)
        if not all_trading_days:
            return {"gaps_found": 0, "dates_downloaded": 0, "error": "No trading days found"}

        # Find dates missing from storage (query existing dates from GreptimeDB)
        existing_dates = await storage.get_existing_daily_dates()
        missing = [d for d in all_trading_days if d not in existing_dates]

        # Minute gaps, bounded to the same window — otherwise an auto run would
        # try to fill ALL historical minute (2023→now), the exact hours-long
        # blocker we're avoiding. Historical minute = deliberate 阶段3.
        minute_gaps = [(s, e) for (s, e) in await storage.find_minute_gaps() if e >= start]

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
