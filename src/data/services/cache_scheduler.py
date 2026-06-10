# === MODULE PURPOSE ===
# CacheScheduler runs the unified DAILY DATA-MAINTENANCE pipeline at 3am Beijing
# (see docs/data-integrity-pipeline.md §4.1). One sequential, failure-isolated pass:
#   ① load-tushare 刷名单 → ② kimi 核新代码 + 喂换号对应表 → ③ 增量重建真值表(只新交易日·含分钟状态)
#   → ④ 索引驱动补日线 → ⑤ 索引驱动补分钟(带上限+逐批心跳) → ⑥ 重建补过的天(确认日线+分钟)
#   → 一条飞书汇总.
# 增量查漏(不每晚全表重扫历史)、精准补缺. Steps are failure-isolated; only 3am (no startup run);
# all run status persisted to GreptimeDB scheduler_log so it survives restarts.
# 分钟补全 2026-06 曾因「大规模·无上限」下载占锁撤出;现以「每晚上限 NIGHTLY_MINUTE_MAX_CODES
# + fill_minute 逐批心跳日志」放回——慢≠挂、且无人值守不会跑几小时(超额下次续补,飞书报)。
# 一次性历史建底仍走手动 /backfill-minute(无上限·后台 create_task)。
# (The 4am ListingVerifyScheduler loop is retired — kimi is step ② here.)
# `check_and_fill_gaps` (the old coarse whole-day fill) is kept ONLY for the
# model-training pre-fill; the nightly path no longer uses it.

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
_STEP_TIMEOUT_MINUTE_FILL = 4 * 3600  # ⑤ index-driven minute fill (capped, per-stock stk_mins)
_STEP_TIMEOUT_CONFIRM = 60 * 60  # ⑥ rebuild only the touched dates (daily+minute)
# Nightly minute-fill cap: bound the unattended pass so a surprise backlog (minute never
# ran ⇒ thousands of missing code-days) can't run for hours. ~1 trading day ≈ ~5000
# code-days; 12000 ≈ ~2.4 days of catch-up per night, the remainder reported + carried to
# the next run. Manual /backfill-minute is UNCAPPED (deliberate full historical clear).
NIGHTLY_MINUTE_MAX_CODES = 12_000
# How many of kimi's per-code "怎么回事" lines to inline in the unified summary.
_KIMI_FINDINGS_IN_SUMMARY = 15
# How many 待修 codes (orphan/missing/wrong_suspended) to name in the summary.
_PROBLEM_CODES_IN_SUMMARY = 30


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
        """Main loop: run the daily data-maintenance pipeline at 3am (see _run_pipeline).

        Deliberately NO startup run: with frequent watchtower redeploys a startup
        run would re-fire the whole pipeline on every restart. The 3am run is the
        only auto-trigger; historical backfill is deliberate (index-driven endpoints).
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
        "backfill_minute_running",
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
        """The unified daily data-maintenance pass — 先查漏、再精准补缺:
        ① load-tushare 刷名单 → ② kimi 核新代码 + 喂换号对应表 →
        ③ 增量重建真值表(新交易日·含分钟状态) → ④ 索引驱动补日线 →
        ⑤ 索引驱动补分钟(带上限+逐批心跳) → ⑥ 重建补过的天(确认日线+分钟).

        Failure-isolated: a failed step is logged + recorded + the pass continues —
        EXCEPT ④/⑤/⑥ are skipped if ③ (rebuild) failed (never fill on a stale index).
        Sends ONE Feishu summary. Returns (result, message).
        """
        from scripts.load_listing_from_tushare import run_load_listing
        from src.common.config import get_tushare_token
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
            # load_ok 被 ③ 依赖:刷名单是 truncate→重灌,失败/超时可能把名单留成残缺半截;
            # 在残名单上 reconcile 会把全市场判成 orphan(而 orphan 是 purge 可删的)。
            load_ok, r = await self._bounded(
                run_load_listing(storage, feishu=False, client=client), _STEP_TIMEOUT_LOAD
            )
            if load_ok:
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

            # ② kimi 核没核过的新代码 + 回填换号对应表. No on/off toggle: kimi runs
            # whenever available; if it can't, that's a WARNING (alert), not a silent
            # skip — a missing capability should be visible, not switched off quietly.
            lv = getattr(self._app_state, "listing_verify_scheduler", None)
            if lv is None or not kimi_available():
                steps.append(
                    (
                        "② 核身份(kimi)",
                        "警告",
                        "kimi 不可用(未安装或无密钥)——新代码/换号无法自动核,请检查",
                    )
                )
            else:
                # Mark in_progress so the status card + manual endpoints reflect
                # that kimi is running (verify_unverified itself doesn't set it).
                lv.in_progress = True
                try:
                    # TIME-budget the batch, not an arbitrary count: ② verifies as MANY codes
                    # as fit ~85% of the step bound, then stops gracefully (the rest queue for
                    # next run). Clears a big backlog in ~one long night instead of dribbling a
                    # tiny count/night, and stays inside the hard _bounded() cut (no false fail).
                    kimi_budget = _STEP_TIMEOUT_KIMI * 0.85
                    ok, r = await self._bounded(
                        lv.verify_unverified(quiet=True, time_budget_sec=kimi_budget),
                        _STEP_TIMEOUT_KIMI,
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
                            # Report the ACTUAL reason (超时 / 解析失败 / 认证被拒…), not a
                            # hardcoded "请检查凭证" that cried wolf about the token.
                            sample = r.get("tool_error_sample") or "原因未知"
                            steps.append(
                                (
                                    "② 核身份(kimi)",
                                    "警告",
                                    detail + f" / 出错 {te} 只(未写,原因:{sample})",
                                )
                            )
                        else:
                            steps.append(("② 核身份(kimi)", "成功", detail))

            # ③ 查漏 — 增量重建:只 reconcile 还没进真值表的新交易日(基本就昨天),
            # 信任已物化的历史索引,绝不每晚全表重扫历史。全表重扫又慢、又会把历史里
            # GreptimeDB 没落地删除留下的"幽灵行"反复重造成 orphan(300114 那案)。
            # 全量/历史重建是手动端点(建底 / 重组换号 / 修索引)的事。
            incr_start: date | None = None
            try:
                pre = await storage.get_trading_calendar_summary()
                md = pre.get("max_date")
                if md:
                    incr_start = date.fromisoformat(md) + timedelta(days=1)
            except Exception as e:  # noqa: BLE001
                logger.warning("maintenance: 读真值表 max_date 失败: %s", e)

            if not load_ok:
                # ① 失败 → 名单可能残缺(truncate 后没灌完),绝不在残名单上 reconcile:
                # 全市场会被判成 orphan(purge 可删)。④⑤⑥ 随 rebuild_ok=False 一并跳过。
                rebuild_ok = False
                steps.append(
                    ("③ 查漏·增量重建", "跳过", "① 刷名单失败,名单可能残缺,不在残名单上重建")
                )
            elif incr_start is None:
                # 真值表为空 → 不在 3 点偷偷做全量重建(那是手动建底的事)。
                rebuild_ok = False
                steps.append(
                    ("③ 查漏·增量重建", "警告", "真值表为空,请先手动全量重建建底;本次跳过")
                )
            elif incr_start > yesterday:
                rebuild_ok = True
                steps.append(("③ 查漏·增量重建", "成功", "历史索引已最新,无新交易日"))
            else:
                ok, r = await self._bounded(
                    rebuild_calendar(
                        storage,
                        start=incr_start,
                        end=yesterday,
                        client=client,
                        with_minute=True,  # 新交易日同时算 minute_state → ⑤ 才知道要补哪些
                    ),
                    _STEP_TIMEOUT_REBUILD,
                )
                rebuild_ok = ok
                if ok:
                    detail = (
                        f"新增 {incr_start}~{yesterday}: {r['days']} 个交易日 / "
                        f"写入 {r['rows']:,} 行 / 待修 {r['problem_rows']:,}"
                    )
                    sk = r.get("skipped_days") or []
                    if sk:
                        # 源头(Tushare daily)空响应被守卫拦下、没覆盖好数据 → 必须可见(警告)
                        steps.append(
                            (
                                "③ 查漏·增量重建",
                                "警告",
                                detail
                                + f" / ⚠️跳过 {len(sk)} 天(疑似 Tushare 取数失败,未覆盖好数据,"
                                f"待源头恢复重试): {'、'.join(sk)}",
                            )
                        )
                    else:
                        steps.append(("③ 查漏·增量重建", "成功", detail))
                else:
                    steps.append(("③ 查漏·增量重建", "失败", r))

            # ④ 补日线 — 索引驱动,只补 missing/wrong_suspended (skip if ③ failed)
            daily_dates: list = []
            if not rebuild_ok:
                steps.append(("④ 补日线", "跳过", "③ 未成功，不在过期/残缺索引上补"))
            else:
                ok, r = await self._bounded(
                    pipeline.fill_daily_from_calendar(quiet=True), _STEP_TIMEOUT_FILL
                )
                if not ok:
                    steps.append(("④ 补日线", "失败", r))
                else:
                    daily_dates = r.get("processed_dates", []) or []
                    if r["dates"] == 0:
                        steps.append(("④ 补日线", "成功", "无缺口可补"))
                    else:
                        steps.append(
                            (
                                "④ 补日线",
                                "成功",
                                f"补回 {r['filled']:,} 行 / 跨 {r['dates']} 个缺口日",
                            )
                        )

            # ⑤ 补分钟 — 索引驱动,只补 minute_state=missing 的(天·股)。带每晚上限
            # (NIGHTLY_MINUTE_MAX_CODES)防无人值守下载几小时;fill_minute 内部逐批心跳日志,
            # 慢能看出在动(≠挂)。超额本次不补完、报告还剩多少,下次续补。skip if ③ failed。
            minute_dates: list = []
            minute_source_short: dict = {}
            if not rebuild_ok:
                steps.append(("⑤ 补分钟", "跳过", "③ 未成功，不在过期/残缺索引上补"))
            else:
                ok, r = await self._bounded(
                    pipeline.fill_minute_from_calendar(
                        quiet=True, max_codes=NIGHTLY_MINUTE_MAX_CODES
                    ),
                    _STEP_TIMEOUT_MINUTE_FILL,
                )
                if not ok:
                    steps.append(("⑤ 补分钟", "失败", r))
                else:
                    minute_dates = r.get("processed_dates", []) or []
                    minute_source_short = r.get("source_short", {}) or {}
                    if r["dates"] == 0:
                        steps.append(("⑤ 补分钟", "成功", "无缺口可补"))
                    else:
                        detail = f"补回 {r['filled']:,} 只(天) / 跨 {r['dates']} 个缺口日"
                        ss_n = sum(len(c) for c in minute_source_short.values())
                        if ss_n:
                            detail += f" / 源头半天 {ss_n} 只(已标记不再补)"
                        if r.get("truncated"):
                            # 还有 backlog 没补完 → 当告警(可见),不是纯成功静默吞掉
                            detail += f" / 达本次上限,还剩 {r['remaining']:,} 只(天)下次续补"
                            steps.append(("⑤ 补分钟", "警告", detail))
                        else:
                            steps.append(("⑤ 补分钟", "成功", detail))

            # ⑥ 确认 — 重建 ④/⑤ 动过的那几天。**分钟 reconcile 很贵**(每天一次 GROUP BY 扫
            # ~1.8B 行 backtest_minute,~15-30s/天),所以只对 ⑤ 真补过分钟的天 with_minute=True
            # (那批天数已被 ⑤ 的 NIGHTLY_MINUTE_MAX_CODES 上限 bound 住);④ 补过日线但 ⑤ 没碰分钟
            # 的天只确认日线(with_minute=False·便宜)。否则:无上限的历史**日线**缺口集喂进
            # with_minute=True,会做几百次分钟扫描、超 _STEP_TIMEOUT_CONFIRM 把每晚拖垮——这正是
            # 分钟上限保护不到的 wedge 点(评审高危项)。无补缺则整步跳过。
            minute_set = set(minute_dates)
            daily_only_days = sorted(set(daily_dates) - minute_set)
            confirm_days_total = 0
            confirm_problem_total = 0
            confirm_skipped: list[str] = []
            confirm_fail: str | None = None
            # ⑥a 分钟确认(带 source_short)— 仅 ⑤ 动过的天,被 ⑤ 上限 bound
            if minute_set:
                ok, r = await self._bounded(
                    rebuild_calendar(
                        storage,
                        trading_days=sorted(minute_set),
                        client=client,
                        with_minute=True,
                        extra_minute_source_short=minute_source_short,
                    ),
                    _STEP_TIMEOUT_CONFIRM,
                )
                if ok:
                    confirm_days_total += r["days"]
                    confirm_problem_total += r["problem_rows"]
                    confirm_skipped += r.get("skipped_days") or []
                else:
                    confirm_fail = f"分钟确认失败: {r}"
            # ⑥b 日线确认(便宜·with_minute=False)— ④ 动过但 ⑤ 没碰分钟的天
            if daily_only_days and confirm_fail is None:
                ok, r = await self._bounded(
                    rebuild_calendar(storage, trading_days=daily_only_days, client=client),
                    _STEP_TIMEOUT_CONFIRM,
                )
                if ok:
                    confirm_days_total += r["days"]
                    confirm_problem_total += r["problem_rows"]
                    confirm_skipped += r.get("skipped_days") or []
                else:
                    confirm_fail = f"日线确认失败: {r}"

            if not minute_set and not daily_only_days:
                steps.append(("⑥ 确认", "跳过", "无补缺，免重建"))
            elif confirm_fail is not None:
                steps.append(("⑥ 确认·重建补过的天", "失败", confirm_fail))
            elif confirm_skipped:
                # 守卫拦下了源头空响应的天(没覆盖)→ 可见告警,别让人以为全确认干净了
                steps.append(
                    (
                        "⑥ 确认·重建补过的天",
                        "警告",
                        f"{confirm_days_total} 天 / 仍待修 {confirm_problem_total:,} / "
                        f"⚠️跳过 {len(confirm_skipped)} 天(疑似 Tushare 取数失败): "
                        f"{'、'.join(confirm_skipped)}",
                    )
                )
            else:
                steps.append(
                    (
                        "⑥ 确认·重建补过的天",
                        "成功",
                        f"{confirm_days_total} 天 / 仍待修 {confirm_problem_total:,}",
                    )
                )
        finally:
            await client.stop()

        # Final library snapshot (read-only, truth-table = single source). Pull the
        # actual problem codes (orphan / missing / wrong_suspended) so the ONE summary
        # names "怎么回事" concisely — instead of the old per-day snapshot-audit report,
        # which spammed the same codes across dozens of near-identical day blocks and
        # disagreed with the truth-table. (Deep per-day digging stays on the manual
        # /api/audit/diagnose-gaps endpoint.)
        final_state: dict = {}
        minute_state: dict = {}
        problem_codes: list[str] = []
        align_suspects: list[str] = []
        try:
            summary = await storage.get_trading_calendar_summary()
            final_state = summary.get("by_daily_state", {})
            minute_state = summary.get("by_minute_state", {})
            codes = await storage.get_calendar_problem_codes(
                {"orphan", "missing", "wrong_suspended"}
            )
            problem_codes = sorted(codes)
            # Fix-4 guard: kimi-rostered codes that are source_none (kimi gave a list_date but
            # Tushare serves no data) = likely alignment defect (wrong date / missed migration).
            # After the alignment fix this is empty; anything here = a NEW divergence → surface.
            from src.data.services.trading_calendar import alignment_suspects

            sn = await storage.get_calendar_problem_codes({"source_none"})
            if sn:
                info = await storage.get_listing_info_all()
                align_suspects = alignment_suspects(sn, info)
        except Exception as e:  # noqa: BLE001
            logger.warning("maintenance: final calendar summary failed: %s", e)

        return await self._send_pipeline_summary(
            task_start,
            steps,
            kimi_findings,
            final_state,
            minute_state,
            problem_codes,
            align_suspects,
        )

    async def _send_pipeline_summary(
        self,
        task_start: datetime,
        steps: list[tuple[str, str, str]],
        kimi_findings: list[dict],
        final_state: dict,
        minute_state: dict,
        problem_codes: list[str],
        align_suspects: list[str] | None = None,
    ) -> tuple[str, str]:
        """Build + send the ONE unified Feishu summary. Returns (result, message)."""
        now = datetime.now(BEIJING_TZ)
        elapsed_min = max(1, int((now - task_start).total_seconds() / 60))
        failed_steps = [s for s in steps if s[1] in ("失败", "超时")]
        warn_steps = [s for s in steps if s[1] == "警告"]
        suspects = align_suspects or []
        if failed_steps:
            header = "【每日数据维护】⚠️ 部分步骤异常"
        elif warn_steps or suspects:
            header = "【每日数据维护】⚠️ 完成(有告警)"
        else:
            header = "【每日数据维护】✅ 完成"

        marks = {"成功": "✅", "失败": "❌", "超时": "⏱", "跳过": "⏭", "警告": "⚠️"}
        lines = [
            header,
            f"时间: {task_start.strftime('%Y-%m-%d %H:%M')} ~ {now.strftime('%H:%M')} "
            f"({elapsed_min}min) · 增量查漏、再精准补缺",
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
            lines += ["", f"最终真值表(日线): {picture}"]

        if minute_state:
            _mlabels = {
                "ok": "完整(241根)",
                "missing": "缺/不满待补",
                "source_short": "源头半天(正常)",
            }
            mpic = " / ".join(
                f"{_mlabels.get(k, k)} {v:,}" for k, v in sorted(minute_state.items()) if v
            )
            if mpic:
                lines += [f"最终真值表(分钟): {mpic}"]

        # Name the actual待修 codes (真缺/标错停牌/有数据却不在册) so the operator sees
        # 怎么回事 without the old per-day spam. source_none is intentionally NOT listed
        # (接口不全·可接受, often 100+). 修法见 /api/audit/calendar/problems。
        if problem_codes:
            shown = problem_codes[:_PROBLEM_CODES_IN_SUMMARY]
            line = "待修代码(真缺/标错停牌/不在册): " + "、".join(shown)
            if len(problem_codes) > len(shown):
                line += f" …共 {len(problem_codes)} 只(完整见 calendar/problems 接口)"
            lines += ["", line]

        # Fix-4 guard: kimi rostered these (gave a list_date) but Tushare serves no data →
        # 上市日错 / 该走迁号对应表而非当成上市码。正常情况下应为空,出现即"下次有别的"的新偏差。
        if suspects:
            shown = suspects[:_PROBLEM_CODES_IN_SUMMARY]
            line = (
                "⚠️ 疑似对齐缺陷(kimi 给了上市日·Tushare 却无数据 → 上市日错/该走迁号表): "
                + "、".join(shown)
            )
            if len(suspects) > len(shown):
                line += f" …共 {len(suspects)} 只"
            lines += ["", line]

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
        if warn_steps or suspects:
            parts = [s[0] for s in warn_steps]
            if suspects:
                parts.append(f"疑似对齐缺陷 {len(suspects)} 只")
            return "success", "完成(有告警: " + "、".join(parts) + ")"
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
