# === MODULE PURPOSE ===
# API endpoints for iQuant scripts running on a Windows server.
# Fully isolated from the main online system — creates own DB pool,
# own Tushare client, own historical adapter. No shared app.state resources.
#
# === ARCHITECTURE ===
# Push-based: server runs background tasks that produce signals at the right time.
# iQuant polls /pending-signals every bar, executes immediately, then acks.
#
# === MOMENTUM STRATEGY ===
# NOTE: Auto buy/sell signal pushing is DISABLED. Scheduler only scans + reports.
#       Manual orders via /manual-order endpoint still work.
# Signal flow (T+2 adaptive sell):
#   09:31-09:35  → GAP CHECK: T+1 gap < -3% → mark early sell; T+2 → mark sell
#   09:39-10:00  → SCAN: run momentum 7-layer funnel → Feishu report (no auto BUY)
#   14:50-14:58  → SELL: log marked holdings (no auto SELL)
#   iQuant       → polls /pending-signals → passorder() → POST /ack-signal
#
# === AUTHENTICATION ===
# All endpoints require X-API-Key header matching IQUANT_API_KEY env var.

from __future__ import annotations

import asyncio
import logging
import traceback
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


# --- Authentication ---

_API_KEY_HEADER = APIKeyHeader(name="X-API-Key")


def _verify_api_key(api_key: str = Depends(_API_KEY_HEADER)) -> str:
    """Verify the API key from X-API-Key header."""
    from src.common.config import get_iquant_api_key

    try:
        expected = get_iquant_api_key()
    except ValueError:
        raise HTTPException(
            status_code=500, detail="IQUANT_API_KEY not configured — set via Settings page"
        )
    if api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key


# --- Request models ---


class AckRequest(BaseModel):
    """Request body for /api/iquant/ack-signal."""

    signal_id: str


class QuoteRequest(BaseModel):
    """Request body for /api/iquant/quote."""

    stock_codes: list[str]


class ManualOrderRequest(BaseModel):
    """Request body for /api/iquant/manual-order (live testing)."""

    stock_code: str  # e.g. "601398"
    direction: str = "buy"  # "buy" or "sell"
    quantity: int = 100  # 股数 (1手=100股)
    price: float | None = None  # 指定价格; None=市价
    price_type: str = "market"  # "market" or "limit"
    reason: str = "手动测试单"


class BacktestScanRequest(BaseModel):
    """Request body for /api/iquant/backtest-scan."""

    trade_date: str  # YYYY-MM-DD
    data_source: str = "tsanghi"


# --- Feishu notification helpers ---


async def _notify_feishu_error(title: str, detail: str) -> None:
    """Send error alert to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_alert(f"[测试服] {title}", detail)
    except Exception:
        logger.warning("Failed to send Feishu error notification", exc_info=True)


async def _notify_feishu_ack(signal: dict) -> None:
    """Send execution confirmation to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if not bot.is_configured():
            return

        direction = "买入" if signal["type"] == "buy" else "卖出"
        pushed = signal.get("created_at", "?")
        acked = signal.get("acked_at", "?")
        lines = [
            f"[测试服] {direction}已执行",
            f"股票: {signal['stock_code']} {signal.get('stock_name', '')}",
        ]
        if signal["type"] == "buy":
            lines.append(f"价格: {signal.get('latest_price', '-')}")
            lines.append(f"板块: {signal.get('board_name', '-')}")
        if signal["type"] == "sell":
            lines.append(f"原因: {signal.get('reason', '-')}")
        lines.append(f"推送→执行: {pushed} → {acked}")

        await bot.send_message("\n".join(lines))
    except Exception:
        logger.warning("Failed to send Feishu ack notification", exc_info=True)


async def _notify_feishu_top5(scan_result) -> None:
    """Send momentum top-5 scored report to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_top5_report(scan_result)
    except Exception:
        logger.warning("Failed to send Feishu top-5 report", exc_info=True)


async def _notify_feishu_signal(signal: dict) -> None:
    """Send signal notification to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if not bot.is_configured():
            return

        direction = "买入" if signal["type"] == "buy" else "卖出"
        lines = [
            f"[测试服] {direction}信号",
            f"股票: {signal['stock_code']} {signal.get('stock_name', '')}",
        ]
        if signal["type"] == "buy":
            lines.append(f"板块: {signal.get('board_name', '-')}")
            lines.append(f"价格: {signal.get('latest_price', '-')}")
            lines.append(f"V3评分: {signal.get('v3_score', '-')}")
        if signal["type"] == "sell":
            lines.append(f"原因: {signal.get('reason', '-')}")
        lines.append(f"时间: {signal.get('created_at', '')}")

        await bot.send_message("\n".join(lines))
    except Exception:
        logger.warning("Failed to send Feishu signal notification", exc_info=True)


# --- Trade calendar ---

_trade_calendar_cache: list[date] | None = None


async def _get_trade_calendar() -> list[date]:
    """Get A-share trade calendar (cached). Uses Tushare trade_cal."""
    global _trade_calendar_cache
    if _trade_calendar_cache is not None:
        return _trade_calendar_cache

    from src.data.clients.tushare_realtime import get_tushare_trade_calendar

    # Fetch a wide range covering all needed dates
    date_strs = await get_tushare_trade_calendar("2020-01-01", "2030-12-31")
    _trade_calendar_cache = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in date_strs)
    logger.info(f"Trade calendar cached: {len(_trade_calendar_cache)} dates")
    return _trade_calendar_cache


def _count_trading_days(calendar: list[date], from_date: date, to_date: date) -> int:
    """Count trading days between two dates (exclusive from, inclusive to)."""
    return sum(1 for d in calendar if from_date < d <= to_date)


# --- Router factory ---


def create_iquant_router() -> APIRouter:
    """Create the iQuant API router with momentum strategy.

    Signal scheduler:
    - 09:25-09:35: GAP CHECK (T+1 gap <-3% → early sell, T+2 → sell)
    - 09:39-10:00: MOMENTUM SCAN (if no holdings → BUY signal)
    - 14:50-14:58: SELL (push sell signals for marked holdings)
    """
    router = APIRouter(prefix="/api/iquant", tags=["iquant"])

    from src.strategy.signal_store import SignalStore

    signal_store = SignalStore()

    # Isolated state (not shared with main app.state)
    _state: dict[str, Any] = {
        "initialized": False,
        "broker_positions": [],  # Actual broker positions: [{code, volume}]
        "scheduler_task": None,
        "universe_cache": None,
        "backtest_cache": None,  # injected from app.py after GreptimeDB connect
        # --- Monitoring ---
        "last_poll_time": None,  # datetime: last time iQuant polled /pending-signals
    }

    # --- Resource management ---

    async def _ensure_resources() -> dict[str, Any]:
        """Lazily initialize iQuant-specific resources and start scheduler."""
        if _state["initialized"]:
            return _state

        from src.common.config import get_tushare_token
        from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter
        from src.data.clients.tushare_realtime import TushareRealtimeClient
        from src.data.database.fundamentals_db import create_fundamentals_db_from_config
        from src.data.database.momentum_scan_db import create_momentum_scan_db_from_config
        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig

        tushare_token = get_tushare_token()
        tushare = TushareRealtimeClient(token=tushare_token)
        try:
            await tushare.start()
        except Exception as e:
            await _notify_feishu_error(
                "Tushare连接失败",
                f"Tushare实时行情客户端启动失败\n错误: {e}\n"
                f"交易功能不可用，请检查Tushare token和网络",
            )
            raise
        _state["realtime_client"] = tushare

        fdb = create_fundamentals_db_from_config()
        try:
            await fdb.connect()
        except Exception as e:
            await _notify_feishu_error(
                "数据库连接失败",
                f"PostgreSQL基本面数据库连接失败\n错误: {e}\n交易功能不可用，请检查数据库配置",
            )
            raise
        _state["fundamentals_db"] = fdb

        # Momentum scan history DB (non-critical — log and continue on failure)
        try:
            scan_db = create_momentum_scan_db_from_config()
            await scan_db.connect()
            _state["momentum_scan_db"] = scan_db
        except Exception as e:
            logger.warning(f"MomentumScanDB init failed (scan history disabled): {e}")
            _state["momentum_scan_db"] = None

        _state["historical_adapter"] = IQuantHistoricalAdapter(tushare)
        _state["concept_mapper"] = LocalConceptMapper()
        # Stock filter: main board + SME (002), exclude ChiNext (300) + STAR (688) + BSE
        _state["stock_filter"] = StockFilter(
            StockFilterConfig(
                exclude_bse=True,
                exclude_chinext=True,
                exclude_star=True,
                exclude_sme=False,
            )
        )

        # Pre-load trade calendar
        try:
            await _get_trade_calendar()
        except Exception as e:
            await _notify_feishu_error(
                "交易日历加载失败",
                f"无法加载交易日历(Tushare)\n错误: {e}\n跳空检测将无法判断交易日",
            )
            raise

        # Start momentum background scheduler
        _state["scheduler_task"] = asyncio.create_task(_signal_scheduler())

        _state["initialized"] = True
        logger.info("Momentum iQuant resources initialized + scheduler started")
        return _state

    async def _cleanup_resources() -> None:
        """Cleanup on shutdown."""
        for key in ("scheduler_task", "monitoring_task"):
            task = _state.get(key)
            if task and not task.done():
                task.cancel()
        rt_client = _state.get("realtime_client")
        if rt_client:
            await rt_client.stop()
        fdb = _state.get("fundamentals_db")
        if fdb:
            await fdb.close()
        _state["initialized"] = False
        logger.info("Momentum iQuant resources cleaned up")

    router._iquant_cleanup = _cleanup_resources  # type: ignore[attr-defined]
    router._iquant_init = _ensure_resources  # type: ignore[attr-defined]

    # --- Cache injection (called from app.py after GreptimeDB connect) ---

    def _inject_cache(cache: Any) -> None:
        """Inject GreptimeDB backtest cache for preClose lookups."""
        _state["backtest_cache"] = cache
        logger.info("Momentum: GreptimeDB backtest cache injected")

    router._inject_cache = _inject_cache  # type: ignore[attr-defined]

    # --- Universe ---

    async def _get_universe() -> list[str]:
        """Get stock codes for momentum universe (main board + SME, cached)."""
        if _state["universe_cache"]:
            return _state["universe_cache"]

        rt_client = _state["realtime_client"]
        all_codes = await rt_client.fetch_stock_list()
        stock_filter = _state["stock_filter"]
        codes = [c for c in all_codes if stock_filter.is_allowed(c)]
        _state["universe_cache"] = codes
        logger.info(f"Momentum universe cached: {len(codes)} codes")
        return codes

    # --- Momentum scan (delegates to strategy service) ---

    async def _run_momentum_scan() -> dict[str, Any] | None:
        """Run momentum scan via strategy service. Returns recommendation dict or None."""
        from src.strategy.momentum_strategy_service import run_momentum_live

        universe = await _get_universe()
        calendar = await _get_trade_calendar()

        scan_result = await run_momentum_live(
            realtime_client=_state["realtime_client"],
            historical_adapter=_state["historical_adapter"],
            fundamentals_db=_state["fundamentals_db"],
            concept_mapper=_state["concept_mapper"],
            universe=universe,
            backtest_cache=_state.get("backtest_cache"),
            trade_calendar=calendar,
            stock_filter=_state.get("stock_filter"),
        )

        today = datetime.now(BEIJING_TZ).date()

        # Persist top-10 scored stocks (non-critical, never blocks trading)
        if scan_result.all_scored and _state.get("momentum_scan_db"):
            try:
                await _state["momentum_scan_db"].save_top_n(
                    today, scan_result.all_scored, scan_result.final_candidates, n=10
                )
            except Exception as e:
                logger.warning(f"MomentumScanDB save failed: {e}")

        # Push top-5 report to Feishu (non-critical)
        await _notify_feishu_top5(scan_result)

        rec = scan_result.recommended
        if not rec:
            return None

        return {
            "stock_code": rec.stock_code,
            "stock_name": rec.stock_name,
            "board_name": rec.board_name,
            "open_price": round(rec.open_price, 4),
            "prev_close": round(rec.prev_close, 4),
            "latest_price": round(rec.latest_price, 4),
            "gain_from_open_pct": round(rec.gain_from_open_pct, 2),
            "turnover_amp": round(rec.turnover_amp, 4),
            "v3_score": round(rec.v3_score, 6),
            "hot_board_count": scan_result.hot_board_count,
            "final_candidates": scan_result.final_candidates,
        }

    # --- Monitoring helpers ---

    HEARTBEAT_TIMEOUT_SECONDS = 60  # consider offline after this many seconds without heartbeat
    TRADING_HOURS = (time(9, 30), time(15, 0))

    async def _check_signal_timeout(now_bj: datetime) -> None:
        """Alert if any pending signal has not been acked within timeout."""
        for sig in signal_store.get_timed_out(now_bj):
            direction = "买入" if sig.type == "buy" else "卖出"
            age = (now_bj - sig.pushed_at).total_seconds() / 60 if sig.pushed_at else 0
            detail = (
                f"{direction}信号未执行!\n"
                f"股票: {sig.stock_code} {sig.stock_name}\n"
                f"推送时间: {sig.created_at}\n"
                f"已等待: {age:.0f}分钟\n"
                f"可能原因: iQuant/QMT掉线或未运行"
            )
            await _notify_feishu_error("信号超时未执行", detail)

    async def _expire_stale_signals(now_bj: datetime) -> None:
        """Remove expired signals and alert."""
        for sig in signal_store.expire_stale(now_bj):
            direction = "买入" if sig.type == "buy" else "卖出"
            age = (now_bj - sig.pushed_at).total_seconds() / 60 if sig.pushed_at else 0
            detail = (
                f"{direction}信号已过期作废!\n"
                f"股票: {sig.stock_code} {sig.stock_name}\n"
                f"推送时间: {sig.created_at}\n"
                f"过期时长: {age:.0f}分钟\n"
                f"信号已自动移除，如需交易请手动下单"
            )
            await _notify_feishu_error("信号过期作废", detail)

    async def _check_heartbeat(now_bj: datetime, last_alert_ts: float) -> float:
        """Alert if iQuant has not polled during trading hours.

        Uses cooldown-based dedup: re-alert every 30 min while offline.
        Returns updated last_alert_ts (unix timestamp).
        """
        ex_time = now_bj.time()
        ALERT_COOLDOWN = 30 * 60  # re-alert every 30 minutes

        # Only check during trading hours
        if not (TRADING_HOURS[0] <= ex_time <= TRADING_HOURS[1]):
            return last_alert_ts

        last_poll = _state.get("last_poll_time")
        now_ts = now_bj.timestamp()

        # If iQuant is currently online, reset cooldown so next disconnect alerts immediately
        if last_poll is not None:
            gap_seconds = (now_bj - last_poll).total_seconds()
            if gap_seconds < HEARTBEAT_TIMEOUT_SECONDS:
                return 0  # reset — next disconnect will alert immediately

        # Cooldown: don't spam alerts
        if last_alert_ts > 0 and (now_ts - last_alert_ts) < ALERT_COOLDOWN:
            return last_alert_ts

        if last_poll is None:
            # Never polled — alert after 09:33 (give 3min for startup)
            if ex_time >= time(9, 33):
                logger.error("Heartbeat: iQuant has NEVER polled today")
                await _notify_feishu_error(
                    "iQuant未连接",
                    "iQuant脚本今天从未连接服务器\n请检查QMT是否已启动并运行iquant_live.py",
                )
                return now_ts
        else:
            gap_minutes = (now_bj - last_poll).total_seconds() / 60
            if (now_bj - last_poll).total_seconds() >= HEARTBEAT_TIMEOUT_SECONDS:
                last_str = last_poll.strftime("%H:%M:%S")
                logger.error(f"Heartbeat: iQuant offline {gap_minutes:.0f}min (last={last_str})")
                await _notify_feishu_error(
                    "iQuant掉线",
                    f"iQuant已失联 {gap_minutes:.0f} 分钟\n"
                    f"最后心跳: {last_str}\n"
                    f"请检查QMT是否正常运行",
                )
                return now_ts

        return last_alert_ts

    async def _send_readiness_report(now_bj: datetime) -> None:
        """Send daily readiness report at 09:30."""
        last_poll = _state.get("last_poll_time")
        poll_status = "未连接"
        if last_poll:
            gap = (now_bj - last_poll).total_seconds()
            if gap < 120:
                poll_status = f"在线 (最近{gap:.0f}秒前)"
            else:
                poll_status = f"离线 ({gap / 60:.0f}分钟未响应)"

        broker_pos = _state.get("broker_positions", [])
        lines = [
            "[测试服] 每日就绪报告",
            f"日期: {now_bj.strftime('%Y-%m-%d %H:%M')}",
            f"iQuant状态: {poll_status}",
            f"券商持仓: {len(broker_pos)}只",
            "今日将执行动量扫描(09:39-10:00)",
        ]

        msg = "\n".join(lines)
        logger.info("Readiness report sent")

        try:
            from src.common.feishu_bot import FeishuBot

            bot = FeishuBot()
            if bot.is_configured():
                await bot.send_message(msg)
        except Exception:
            logger.warning("Failed to send readiness report", exc_info=True)

    # --- Monitoring scheduler (independent, no trading resources needed) ---

    async def _monitoring_scheduler() -> None:
        """Lightweight monitoring loop — runs from server startup.

        Does NOT need Tushare, DB, or any trading resources.
        Checks heartbeat, signal timeout/expiry, readiness report.
        """
        readiness_done_date = ""
        heartbeat_alert_ts: float = 0

        logger.info("Monitoring scheduler started")

        try:
            while True:
                try:
                    now_bj = datetime.now(BEIJING_TZ)
                    ex_date = now_bj.strftime("%Y-%m-%d")
                    ex_time = now_bj.time().replace(second=0, microsecond=0)

                    # --- READINESS REPORT: 09:30 ---
                    if (
                        readiness_done_date != ex_date
                        and ex_time >= time(9, 30)
                        and ex_time <= time(9, 35)
                    ):
                        readiness_done_date = ex_date
                        await _send_readiness_report(now_bj)

                    if readiness_done_date != ex_date and ex_time > time(9, 35):
                        readiness_done_date = ex_date

                    # --- Clear stale broker data when offline ---
                    last_poll = _state.get("last_poll_time")
                    is_offline = last_poll is None or (
                        (now_bj - last_poll).total_seconds() >= HEARTBEAT_TIMEOUT_SECONDS
                    )
                    if is_offline:
                        if _state["broker_positions"] or _state.get("available_cash", 0) > 0:
                            logger.info("iQuant offline — clearing broker positions/cash")
                            _state["broker_positions"] = []
                            _state["available_cash"] = 0

                    # --- CONTINUOUS MONITORING ---
                    await _expire_stale_signals(now_bj)
                    await _check_signal_timeout(now_bj)
                    heartbeat_alert_ts = await _check_heartbeat(now_bj, heartbeat_alert_ts)
                except asyncio.CancelledError:
                    raise  # propagate for clean shutdown
                except Exception as e:
                    logger.error(f"Monitoring iteration error: {e}", exc_info=True)

                await asyncio.sleep(30)

        except asyncio.CancelledError:
            logger.info("Monitoring scheduler stopped")

    def _start_monitoring() -> None:
        """Start the monitoring scheduler. Safe to call at any time."""
        if _state.get("monitoring_task") and not _state["monitoring_task"].done():
            return  # already running
        _state["monitoring_task"] = asyncio.create_task(_monitoring_scheduler())

    router._start_monitoring = _start_monitoring  # type: ignore[attr-defined]

    def _get_status() -> dict:
        """Return iQuant connection status (no auth required)."""
        now = datetime.now(BEIJING_TZ)
        last_poll = _state.get("last_poll_time")
        if last_poll is not None:
            gap_seconds = (now - last_poll).total_seconds()
            connected = gap_seconds < HEARTBEAT_TIMEOUT_SECONDS
            last_poll_str = last_poll.strftime("%Y-%m-%d %H:%M:%S")
        else:
            gap_seconds = None
            connected = False
            last_poll_str = None
        return {
            "connected": connected,
            "last_poll_time": last_poll_str,
            "gap_seconds": round(gap_seconds) if gap_seconds is not None else None,
            "holdings_count": (
                sum(1 for p in _state["broker_positions"] if p.get("volume", 0) > 0)
                if connected
                else 0
            ),
            "pending_count": signal_store.pending_count,
            "available_cash": _state.get("available_cash", 0) if connected else 0,
        }

    router._get_status = _get_status  # type: ignore[attr-defined]

    def _get_broker_positions() -> list[dict]:
        """Return actual broker positions synced from iQuant.

        Returns empty list when iQuant is offline to avoid showing stale data.
        """
        last_poll = _state.get("last_poll_time")
        if last_poll is None:
            return []
        now = datetime.now(BEIJING_TZ)
        if (now - last_poll).total_seconds() >= HEARTBEAT_TIMEOUT_SECONDS:
            return []
        return _state["broker_positions"]

    router._get_broker_positions = _get_broker_positions  # type: ignore[attr-defined]

    def _push_order(signal: dict) -> dict:
        """Push a manual order signal from dashboard (no auth).

        Same as /manual-order but callable internally without HTTP.
        Returns the signal dict with assigned ID.
        """
        now = datetime.now(BEIJING_TZ)
        pushed = signal_store.push_dict(signal, now=now)
        return pushed.to_wire_dict()

    router._push_order = _push_order  # type: ignore[attr-defined]

    def _get_pending_signals() -> list[dict]:
        """Return pending signals for dashboard display."""
        return signal_store.get_pending_dicts()

    router._get_pending_signals = _get_pending_signals  # type: ignore[attr-defined]

    def _cancel_signal(signal_id: str) -> bool:
        """Remove a signal from pending queue. Returns True if found."""
        return signal_store.cancel(signal_id)

    router._cancel_signal = _cancel_signal  # type: ignore[attr-defined]

    # --- Momentum background scheduler (trading operations only) ---

    async def _signal_scheduler() -> None:
        """Momentum trading scheduler — scan only.

        Trading window:
        - SCAN (09:39-10:00): Run momentum scan → push Feishu report

        Monitoring (heartbeat, timeout, readiness) runs in _monitoring_scheduler.
        """
        SCAN_WINDOW = (time(9, 39), time(10, 0))

        logger.info("Momentum signal scheduler started")

        scan_done_date = ""

        try:
            while True:
                now_bj = datetime.now(BEIJING_TZ)
                ex_date = now_bj.strftime("%Y-%m-%d")
                ex_time = now_bj.time().replace(second=0, microsecond=0)

                # --- Skip non-trading days (weekends + holidays) ---
                if now_bj.weekday() >= 5:
                    await asyncio.sleep(3600)
                    continue
                try:
                    cal = await _get_trade_calendar()
                    if now_bj.date() not in cal:
                        if scan_done_date != ex_date:
                            logger.info(f"Momentum: {ex_date} is not a trading day")
                            scan_done_date = ex_date
                        await asyncio.sleep(3600)
                        continue
                except Exception as e:
                    logger.warning(f"Trade calendar check failed, proceeding: {e}")

                # --- SCAN: 09:39-10:00 ---
                if scan_done_date != ex_date and SCAN_WINDOW[0] <= ex_time <= SCAN_WINDOW[1]:
                    if not _state["initialized"]:
                        await asyncio.sleep(10)
                        continue

                    scan_done_date = ex_date

                    try:
                        rec = await _run_momentum_scan()
                        if rec:
                            logger.info(
                                f"Momentum: scan recommends {rec['stock_code']} "
                                f"(auto-trading disabled, no signal pushed)"
                            )
                        else:
                            logger.info("Momentum scan: no recommendation today")
                            await _notify_feishu_error(
                                "动量扫描结果",
                                "今日动量扫描完成，无符合条件的推荐股票",
                            )
                    except Exception as e:
                        error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                        logger.error(f"Momentum scan failed: {error_detail}")
                        await _notify_feishu_error("动量扫描失败", error_detail)

                # Scan deadline
                if scan_done_date != ex_date and ex_time > SCAN_WINDOW[1]:
                    scan_done_date = ex_date

                await asyncio.sleep(120 if scan_done_date == ex_date else 30)

        except asyncio.CancelledError:
            logger.info("Momentum signal scheduler stopped")
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            logger.critical(f"Signal scheduler CRASHED: {error_detail}")
            await _notify_feishu_error(
                "交易调度器崩溃",
                f"信号调度器意外退出!\n{error_detail}\n今日将无法自动交易，请立即检查",
            )

    # --- Endpoints ---

    @router.post("/heartbeat")
    async def heartbeat(
        request: Request,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Heartbeat + silent state sync (no Feishu).

        Called by a dedicated thread in iQuant script every 30s,
        independent of handlebar / trading hours.
        Accepts optional positions and cash to keep dashboard up-to-date.
        """
        body = await request.json()
        _state["last_poll_time"] = datetime.now(BEIJING_TZ)
        if "positions" in body:
            _state["broker_positions"] = body["positions"]
        if "available_cash" in body:
            _state["available_cash"] = body["available_cash"]
        return {"status": "ok"}

    @router.get("/ping")
    async def ping(api_key: str = Depends(_verify_api_key)) -> dict:
        """Health check + trigger lazy init."""
        await _ensure_resources()
        now = datetime.now(BEIJING_TZ)
        return {
            "status": "ok",
            "service": "iquant-momentum",
            "server_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "pending_count": signal_store.pending_count,
            "broker_positions": len(_state["broker_positions"]),
        }

    @router.get("/pending-signals")
    async def pending_signals(api_key: str = Depends(_verify_api_key)) -> dict:
        """Return all pending (unacknowledged) signals.

        Safety: filters out expired signals so QMT never executes stale orders.
        """
        now = datetime.now(BEIJING_TZ)
        _state["last_poll_time"] = now
        active = signal_store.get_active_wire(now)
        return {
            "signals": active,
            "count": len(active),
        }

    @router.post("/ack-signal")
    async def ack_signal(
        body: AckRequest,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Acknowledge a signal after iQuant has executed it.

        For BUY signals: stock is added to holdings with entry_price.
        For SELL signals: stock is removed from holdings.
        """
        now = datetime.now(BEIJING_TZ)
        found = signal_store.ack(body.signal_id, now=now)

        if not found:
            raise HTTPException(status_code=404, detail=f"Signal {body.signal_id} not found")

        wire = found.to_wire_dict()
        await _notify_feishu_ack(wire)

        return {"success": True, "signal": wire}

    @router.post("/report-status")
    async def report_status(
        request: Request,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Receive iQuant startup status (balance) and notify via Feishu."""
        body = await request.json()
        cash = body.get("available_cash", 0)
        _state["available_cash"] = cash
        now_str = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

        msg = f"[测试服] iQuant脚本已启动\n可用资金: {cash:,.2f}\n时间: {now_str}"
        logger.info(f"iQuant report-status: cash={cash:.2f}")

        try:
            from src.common.feishu_bot import FeishuBot

            bot = FeishuBot()
            if bot.is_configured():
                await bot.send_message(msg)
        except Exception:
            logger.warning("Failed to send Feishu startup notification", exc_info=True)

        return {"success": True, "message": msg}

    @router.post("/report-trade")
    async def report_trade(
        request: Request,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Receive trade execution from iQuant and notify via Feishu."""
        body = await request.json()
        msg = body.get("message", "")
        stock_name = body.get("stock_name", "")
        reason = body.get("reason", "")
        now_str = datetime.now(BEIJING_TZ).strftime("%H:%M:%S")

        text = f"[测试服] 下单已执行\n{msg}"
        if stock_name:
            text += f"\n名称: {stock_name}"
        if reason:
            text += f"\n原因: {reason}"
        text += f"\n时间: {now_str}"

        logger.info(f"iQuant report-trade: {msg}")

        try:
            from src.common.feishu_bot import FeishuBot

            bot = FeishuBot()
            if bot.is_configured():
                await bot.send_message(text)
        except Exception:
            logger.warning("Failed to send Feishu trade notification", exc_info=True)

        return {"success": True}

    @router.post("/report-error")
    async def report_error(
        request: Request,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Receive error from iQuant script and notify via Feishu."""
        body = await request.json()
        error_msg = body.get("error", "unknown")
        logger.error(f"iQuant report-error: {error_msg}")
        await _notify_feishu_error("iQuant执行异常", error_msg)
        return {"success": True}

    @router.post("/sync-positions")
    async def sync_positions(
        request: Request,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Receive actual broker positions from iQuant script."""
        body = await request.json()
        positions = body.get("positions", [])
        _state["broker_positions"] = positions
        logger.info(f"iQuant sync-positions: {len(positions)} positions")
        return {"success": True, "count": len(positions)}

    @router.post("/manual-order")
    async def manual_order(
        body: ManualOrderRequest,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Push a manual BUY/SELL signal for live trading tests.

        The signal enters the same pending queue as momentum signals.
        iQuant polls /pending-signals and executes via passorder().

        Example: POST /api/iquant/manual-order
        {
            "stock_code": "601398",
            "direction": "buy",
            "quantity": 100,
            "price_type": "market",
            "reason": "手动测试单"
        }
        """
        if body.direction not in ("buy", "sell"):
            raise HTTPException(status_code=400, detail="direction must be 'buy' or 'sell'")
        if body.quantity <= 0 or body.quantity % 100 != 0:
            raise HTTPException(status_code=400, detail="quantity must be positive multiple of 100")
        if body.price_type == "limit" and (body.price is None or body.price <= 0):
            raise HTTPException(status_code=400, detail="limit order requires positive price")

        now = datetime.now(BEIJING_TZ)
        sig = signal_store.push_dict(
            {
                "type": body.direction,
                "stock_code": body.stock_code,
                "stock_name": "",
                "quantity": body.quantity,
                "price": body.price,
                "price_type": body.price_type,
                "reason": body.reason,
                "manual": True,
            },
            now=now,
        )

        logger.info(
            f"Manual order pushed: {body.direction} {body.stock_code} "
            f"qty={body.quantity} price_type={body.price_type} price={body.price}"
        )

        return {
            "success": True,
            "signal": sig.to_wire_dict(),
            "message": "Signal pushed. iQuant will pick it up on next /pending-signals poll.",
        }

    @router.post("/trigger-scan")
    async def trigger_scan(api_key: str = Depends(_verify_api_key)) -> dict:
        """Manually trigger momentum scan + Feishu top-5 report (bypasses time window)."""
        await _ensure_resources()

        try:
            rec = await _run_momentum_scan()
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}"
            raise HTTPException(status_code=500, detail=error_detail)

        result: dict = {"success": True, "recommendation": None}
        if rec:
            result["recommendation"] = rec
            result["signal_pushed"] = False
            result["reason"] = "auto-trading disabled"
        return result

    @router.get("/universe")
    async def universe(api_key: str = Depends(_verify_api_key)) -> dict:
        """Return all stock codes in momentum universe (cached)."""
        await _ensure_resources()
        codes = await _get_universe()
        return {"codes": codes, "count": len(codes)}

    @router.post("/quote")
    async def quote(
        body: QuoteRequest,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Get Tushare real-time quotes for specific stocks."""
        await _ensure_resources()

        if not body.stock_codes:
            raise HTTPException(status_code=400, detail="stock_codes is required")

        try:
            rt_client = _state["realtime_client"]
            quotes = await rt_client.batch_get_quotes(body.stock_codes)
            return {
                "success": True,
                "quotes": {
                    code: {
                        "name": "",
                        "open": q.open_price,
                        "prev_close": 0.0,
                        "latest": q.latest_price,
                        "high": q.high_price,
                        "low": q.low_price,
                        "volume": q.volume,
                        "amount": q.amount,
                    }
                    for code, q in quotes.items()
                },
            }
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}"
            logger.error(f"Quote failed: {error_detail}")
            await _notify_feishu_error("行情获取失败", error_detail)
            raise HTTPException(status_code=500, detail=error_detail)

    @router.post("/backtest-scan")
    async def backtest_scan(
        request: Request,
        body: BacktestScanRequest,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Run momentum scan for a specific historical date."""
        from src.strategy.momentum_strategy_service import (
            MinuteDataMissingError,
            run_momentum_backtest,
        )

        try:
            trade_date = datetime.strptime(body.trade_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid date: {body.trade_date}")

        await _ensure_resources()

        if body.data_source != "tsanghi":
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported data_source: {body.data_source}. Use 'tsanghi'.",
            )

        bt_cache = getattr(request.app.state, "backtest_cache", None)
        if not bt_cache or not bt_cache.is_ready:
            raise HTTPException(
                status_code=503,
                detail="GreptimeDB 缓存未连接。请先在 web 页面的回测页下载数据。",
            )

        try:
            scan_result = await run_momentum_backtest(
                backtest_cache=bt_cache,
                fundamentals_db=_state["fundamentals_db"],
                trade_date=trade_date,
            )
        except MinuteDataMissingError as e:
            raise HTTPException(status_code=400, detail=str(e))

        rec = scan_result.recommended
        if not rec:
            return {"recommendation": None, "reason": "No recommendation for this date"}

        return {
            "recommendation": {
                "stock_code": rec.stock_code,
                "stock_name": rec.stock_name,
                "board_name": rec.board_name,
                "latest_price": round(rec.latest_price, 4),
                "open_price": round(rec.open_price, 4),
                "prev_close": round(rec.prev_close, 4),
                "composite_score": round(rec.v3_score, 4),
            }
        }

    @router.get("/status")
    async def iquant_status() -> JSONResponse:
        """Public iQuant connection status (no auth required)."""
        return JSONResponse(
            content=_get_status(),
            headers={"Cache-Control": "no-store"},
        )

    return router
