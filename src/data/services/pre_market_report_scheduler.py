# === MODULE PURPOSE ===
# Pre-market holdings report scheduler.
# Trigger: 8:00 Beijing on trading days (auto), or via POST /api/pre-market-report/run.
# Action: read app.state.broker_positions → for each holding, run analyze_kline()
#         → push K-line image URL + 技术面分析 to Feishu, one message per stock.
#
# Why 8am instead of 15:00: tsanghi/Tushare daily endpoints are T-1, and
# CacheScheduler fills the previous trading day's data at 3am. Running at 8am
# means we always have a complete latest bar without forcing an extra refill.

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from src.data.clients.greptime_storage import GreptimeBacktestStorage

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
SCHEDULE_HOUR = 8  # 8am Beijing
_STARTUP_DELAY_SECONDS = 60
_ANALYZE_DAYS = 30
_AS_OF_LOOKBACK_DAYS = 14  # how far back to search for the latest cached trading day


async def _notify_feishu(message: str) -> None:
    """Best-effort Feishu text notification (status / errors). Never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_message(message)
    except Exception:
        logger.warning("Failed to send Feishu pre-market notification", exc_info=True)


async def _notify_feishu_image(image_url: str, filename: str = "kline.png") -> bool:
    """Send a native Feishu image message. Returns True on success."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if not bot.is_configured():
            return False
        return await bot.send_image(image_url, filename=filename)
    except Exception:
        logger.warning("Failed to send Feishu pre-market image", exc_info=True)
        return False


async def _notify_feishu_markdown(markdown: str, title: str | None = None) -> bool:
    """Send a Feishu markdown rich-text message. Returns True on success."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if not bot.is_configured():
            return False
        return await bot.send_markdown(markdown, title=title)
    except Exception:
        logger.warning("Failed to send Feishu pre-market markdown", exc_info=True)
        return False


async def _is_trading_day(d: date) -> bool:
    """Check if d is a trading day via Tushare trade_cal, fallback to weekday."""
    try:
        from src.data.clients.tushare_realtime import get_tushare_trade_calendar

        days = await get_tushare_trade_calendar(d.strftime("%Y-%m-%d"), d.strftime("%Y-%m-%d"))
        return d.strftime("%Y-%m-%d") in set(days or [])
    except Exception as e:
        logger.warning(f"Tushare trade_cal lookup failed for {d}: {e}, using weekday fallback")
        return d.weekday() < 5


async def _latest_trading_day_with_data(
    storage: GreptimeBacktestStorage, today: date
) -> date | None:
    """Find the most recent date on or before today that has rows in backtest_daily.

    Holiday-safe: looks back up to ``_AS_OF_LOOKBACK_DAYS`` to skip non-trading days
    and any data lag from CacheScheduler. Uses the actual presence of rows rather
    than trusting Tushare alone — if the row isn't there, we can't render a chart.
    """
    existing = await storage.get_existing_daily_dates()
    if not existing:
        return None
    candidate = today
    for _ in range(_AS_OF_LOOKBACK_DAYS):
        if candidate in existing:
            return candidate
        candidate -= timedelta(days=1)
    return None


class PreMarketReportScheduler:
    """Background task that sends a pre-market holdings report to Feishu daily.

    Lifecycle:
        scheduler = PreMarketReportScheduler(app.state)
        task = asyncio.create_task(scheduler.run())
        # On shutdown:
        task.cancel()

    Manual trigger:
        await scheduler.trigger_manual()  # ignores trading-day check
    """

    _LOG_NAME = "pre_market_report"

    def __init__(self, app_state: Any) -> None:
        self._app_state = app_state
        # Serializes manual + scheduled runs so a slow LLM batch doesn't get
        # double-fired if the user clicks the manual button mid-run, and 8am
        # auto-trigger doesn't stomp on a manual run.
        self._lock = asyncio.Lock()
        self.next_run_time: str | None = None
        self.last_run_time: str | None = None
        self.last_run_result: str | None = None
        self.last_run_message: str | None = None

    # ------------------------------------------------------------------
    # State accessors
    # ------------------------------------------------------------------

    def _get_storage(self) -> GreptimeBacktestStorage | None:
        return getattr(self._app_state, "storage", None)

    def is_running(self) -> bool:
        """True while a report run holds the lock."""
        return self._lock.locked()

    def _next_run_str(self) -> str:
        now = datetime.now(BEIJING_TZ)
        target = datetime.combine(now.date(), time(SCHEDULE_HOUR, 0), tzinfo=BEIJING_TZ)
        if now >= target:
            target += timedelta(days=1)
        return target.strftime("%m-%d %H:%M")

    def get_status(self) -> dict[str, Any]:
        from src.common.config import get_pre_market_report_enabled

        return {
            "enabled": get_pre_market_report_enabled(),
            "running": self.is_running(),
            "next_run_time": self.next_run_time,
            "last_run_time": self.last_run_time,
            "last_run_result": self.last_run_result,
            "last_run_message": self.last_run_message,
        }

    # ------------------------------------------------------------------
    # Persistent status
    # ------------------------------------------------------------------

    async def _restore_status_from_db(self) -> None:
        storage = self._get_storage()
        if not storage or not storage.is_ready:
            return
        last = await storage.get_last_scheduler_run(self._LOG_NAME)
        if last:
            self.last_run_time = last["time"]
            self.last_run_result = last["result"]
            self.last_run_message = last["message"]

    async def _persist_status(self, trigger: str) -> None:
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
    # Main loop / triggers
    # ------------------------------------------------------------------

    async def run(self) -> None:
        logger.info("PreMarketReportScheduler started, will run daily at 8am Beijing time")
        try:
            await asyncio.sleep(_STARTUP_DELAY_SECONDS)
            await self._restore_status_from_db()

            while True:
                now = datetime.now(BEIJING_TZ)
                target = datetime.combine(now.date(), time(SCHEDULE_HOUR, 0), tzinfo=BEIJING_TZ)
                if now >= target:
                    target += timedelta(days=1)
                self.next_run_time = target.strftime("%Y-%m-%d %H:%M")
                wait_secs = (target - now).total_seconds()
                logger.info(
                    "PreMarketReportScheduler: next run at %s (%.1fh from now)",
                    target.strftime("%Y-%m-%d %H:%M"),
                    wait_secs / 3600,
                )
                await asyncio.sleep(wait_secs)
                await self._run_once("scheduled")
        except asyncio.CancelledError:
            logger.info("PreMarketReportScheduler cancelled")

    async def trigger_manual(self) -> None:
        """Run once immediately, ignoring the trading-day filter.

        Used by `POST /api/pre-market-report/run` for testing / 补发. Caller
        typically wraps this in `asyncio.create_task` so the HTTP request
        returns immediately while the report runs in the background.
        """
        await self._run_once("manual")

    # ------------------------------------------------------------------
    # Core dispatch
    # ------------------------------------------------------------------

    async def _run_once(self, trigger: str) -> None:
        from src.common.config import get_pre_market_report_enabled

        run_time = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

        # Toggle and trading-day filter ONLY apply to the scheduled 8am
        # trigger — manual triggers always run so the user can 补发 a missed
        # day, test on a Sunday, or fire one-off even with daily auto disabled.
        if trigger == "scheduled":
            if not get_pre_market_report_enabled():
                logger.info("PreMarketReportScheduler: disabled, skipping scheduled run")
                self.last_run_time = run_time
                self.last_run_result = "skipped"
                self.last_run_message = "定时任务已关闭"
                await self._persist_status(trigger)
                return

            today = datetime.now(BEIJING_TZ).date()
            if not await _is_trading_day(today):
                logger.info("PreMarketReportScheduler: %s is non-trading day, skipping", today)
                self.last_run_time = run_time
                self.last_run_result = "skipped"
                self.last_run_message = f"{today} 非交易日"
                await self._persist_status(trigger)
                return

        # If a previous run still holds the lock, don't queue — log and bail.
        # Without this, manual triggers fired during a slow scheduled run would
        # all queue up and fire back-to-back, spamming the Feishu group.
        if self._lock.locked():
            logger.info(
                "PreMarketReportScheduler: previous run in progress, skipping (%s)", trigger
            )
            self.last_run_time = run_time
            self.last_run_result = "skipped"
            self.last_run_message = "上一次执行尚未结束"
            await self._persist_status(trigger)
            return

        async with self._lock:
            try:
                await self._do_report(trigger, run_time)
            except Exception as e:
                logger.error("PreMarketReportScheduler failed: %s", e, exc_info=True)
                self.last_run_time = run_time
                self.last_run_result = "failed"
                self.last_run_message = str(e)[:200]
                await _notify_feishu(f"[盘前持仓日报] 异常 ({trigger})\n{e}")
            finally:
                await self._persist_status(trigger)

    # ------------------------------------------------------------------
    # Report body
    # ------------------------------------------------------------------

    async def _do_report(self, trigger: str, run_time: str) -> None:
        storage = self._get_storage()
        if storage is None or not storage.is_ready:
            self.last_run_time = run_time
            self.last_run_result = "failed"
            self.last_run_message = "GreptimeDB 未就绪"
            await _notify_feishu(f"[盘前持仓日报] 失败 ({trigger})\nGreptimeDB 未就绪")
            return

        today = datetime.now(BEIJING_TZ).date()
        as_of = await _latest_trading_day_with_data(storage, today)
        if as_of is None:
            self.last_run_time = run_time
            self.last_run_result = "failed"
            self.last_run_message = "未找到任何已缓存的日线交易日"
            await _notify_feishu(
                f"[盘前持仓日报] 失败 ({trigger})\n未找到已缓存的日线交易日，"
                f"请先确保 CacheScheduler 已补全数据"
            )
            return

        broker = getattr(self._app_state, "broker", None)
        broker_err = getattr(self._app_state, "broker_last_error", None)
        positions = list(getattr(self._app_state, "broker_positions", []) or [])

        if broker is None:
            self.last_run_time = run_time
            self.last_run_result = "failed"
            self.last_run_message = "Broker 未初始化"
            await _notify_feishu(f"[盘前持仓日报] 失败 ({trigger})\nBroker 未初始化，无法获取持仓")
            return

        # Genuine "no positions" vs broker error: positions list is updated only
        # on successful poll; if last poll failed AND list is empty, we can't
        # tell. Treat empty-with-error as failure to avoid silent-skip on broker
        # outage (trading-safety: stop rather than fail silently).
        if not positions and broker_err:
            self.last_run_time = run_time
            self.last_run_result = "failed"
            self.last_run_message = f"Broker 错误: {broker_err}"
            await _notify_feishu(f"[盘前持仓日报] 失败 ({trigger})\nBroker 错误: {broker_err}")
            return

        if not positions:
            self.last_run_time = run_time
            self.last_run_result = "no_positions"
            self.last_run_message = f"截至 {as_of} 无持仓"
            logger.info("PreMarketReportScheduler: no positions, silent skip")
            return

        # Header (plain text — short, no need for markdown)
        await _notify_feishu(
            f"📊 盘前持仓日报 ({as_of}) · 触发: {trigger}\n"
            f"持仓数: {len(positions)} 只，开始逐只生成技术分析..."
        )

        from src.analysis.kline_llm import analyze_kline
        from src.data.sources.local_concept_mapper import LocalConceptMapper

        mapper = LocalConceptMapper()

        ok = 0
        failed: list[tuple[str, str]] = []
        for pos in positions:
            code = str(pos.get("code", "")).strip()
            if not code:
                continue
            bare = code.split(".")[0]
            name = mapper.get_stock_name(bare) or "?"
            volume = pos.get("volume", 0) or 0
            avg_price = float(pos.get("avg_price", 0.0) or 0.0)
            market_value = float(pos.get("market_value", 0.0) or 0.0)

            try:
                result = await analyze_kline(
                    storage=storage,
                    code=code,
                    days=_ANALYZE_DAYS,
                )
            except Exception as e:
                err_msg = f"{type(e).__name__}: {str(e)[:200]}"
                logger.warning("analyze_kline failed for %s: %s", code, err_msg)
                failed.append((code, err_msg))
                await _notify_feishu(f"⚠️ {code} {name} 分析失败\n{err_msg}")
                continue

            # Send K-line as a native Feishu image (not a URL link), then send
            # the analysis as a Markdown rich-text card so headings / bullets
            # / bold render properly in the chat. Two messages per stock.
            title = f"📊 {code} {name} ({as_of})"
            await _notify_feishu_image(result["image_url"], filename=f"{bare}_{as_of}.png")

            md = (
                f"## {title}\n\n"
                f"| | |\n"
                f"|---|---|\n"
                f"| 持仓 | **{volume}** 股 |\n"
                f"| 成本 | ¥{avg_price:.2f} |\n"
                f"| 市值 | ¥{market_value:,.2f} |\n\n"
                f"---\n\n"
                f"{result['analysis']}"
            )
            await _notify_feishu_markdown(md, title=title)
            ok += 1

        self.last_run_time = run_time
        if failed and ok == 0:
            self.last_run_result = "failed"
            self.last_run_message = f"全部失败 ({len(failed)}/{len(positions)})"
        elif failed:
            self.last_run_result = "success"
            self.last_run_message = f"成功 {ok}/{len(positions)}, 失败 {len(failed)}"
        else:
            self.last_run_result = "success"
            self.last_run_message = f"成功 {ok}/{len(positions)}"
