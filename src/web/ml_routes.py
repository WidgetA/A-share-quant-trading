# === MODULE PURPOSE ===
# API endpoints for the ML strategy: scan, backtest, quote, universe.
# Trading execution has moved to BrokerClient (xtquant-trade-server).
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


class QuoteRequest(BaseModel):
    """Request body for /api/iquant/quote."""

    stock_codes: list[str]


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


async def _notify_feishu_ml_top5(scan_result) -> None:
    """Send ML scan top-5 report to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_ml_top5_report(scan_result)
    except Exception:
        logger.warning("Failed to send Feishu ML top-5 report", exc_info=True)


# --- Trade calendar ---

_trade_calendar_cache: list[date] | None = None


async def _get_trade_calendar() -> list[date]:
    """Get A-share trade calendar (cached). Uses Tushare trade_cal."""
    global _trade_calendar_cache
    if _trade_calendar_cache is not None:
        return _trade_calendar_cache

    from src.data.clients.tushare_realtime import get_tushare_trade_calendar

    date_strs = await get_tushare_trade_calendar("2020-01-01", "2030-12-31")
    _trade_calendar_cache = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in date_strs)
    logger.info(f"Trade calendar cached: {len(_trade_calendar_cache)} dates")
    return _trade_calendar_cache


def _count_trading_days(calendar: list[date], from_date: date, to_date: date) -> int:
    """Count trading days between two dates (exclusive from, inclusive to)."""
    return sum(1 for d in calendar if from_date < d <= to_date)


# --- Router factory ---


def create_ml_router() -> APIRouter:
    """Create the ML strategy router.

    Signal scheduler:
    - 09:39-10:00: MOMENTUM SCAN (Feishu top-5 report only, no auto-trading)

    URL prefix kept as /api/iquant/ for backward compat with backtest scripts.
    """
    router = APIRouter(prefix="/api/iquant", tags=["ml-strategy"])

    # Isolated state (not shared with main app.state)
    _state: dict[str, Any] = {
        "initialized": False,
        "scheduler_task": None,
        "universe_cache": None,
        "storage": None,  # injected from app.py after GreptimeDB connect
    }

    # --- Resource management ---

    async def _ensure_resources() -> dict[str, Any]:
        """Lazily initialize iQuant-specific resources and start scheduler."""
        if _state["initialized"]:
            return _state

        from src.common.config import get_tushare_token
        from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter
        from src.data.clients.tushare_realtime import TushareRealtimeClient
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
        logger.info("iQuant resources initialized + scheduler started")
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
        _state["initialized"] = False
        logger.info("iQuant resources cleaned up")

    router._ml_cleanup = _cleanup_resources  # type: ignore[attr-defined]
    router._ml_init = _ensure_resources  # type: ignore[attr-defined]

    # --- Storage injection (called from app.py after GreptimeDB connect) ---

    def _inject_storage(storage: Any) -> None:
        """Inject GreptimeDB storage for preClose / minute lookups."""
        _state["storage"] = storage
        logger.info("iQuant: GreptimeDB storage injected")

    router._inject_storage = _inject_storage  # type: ignore[attr-defined]

    # --- Universe ---

    async def _get_universe() -> list[str]:
        """Get stock codes for universe (main board + SME, cached)."""
        if _state["universe_cache"]:
            return _state["universe_cache"]

        rt_client = _state["realtime_client"]
        all_codes = await rt_client.fetch_stock_list()
        stock_filter = _state["stock_filter"]
        codes = [c for c in all_codes if stock_filter.is_allowed(c)]
        _state["universe_cache"] = codes
        logger.info(f"iQuant universe cached: {len(codes)} codes")
        return codes

    # --- ML scan (delegates to strategy service) ---

    async def _run_ml_scan() -> dict[str, Any] | None:
        """Run ML scan via strategy service. Returns recommendation dict or None."""
        from src.strategy.ml_strategy_service import run_ml_live

        calendar = await _get_trade_calendar()

        scan_result = await run_ml_live(
            realtime_client=_state["realtime_client"],
            storage=_state.get("storage"),
            concept_mapper=_state["concept_mapper"],
            trade_calendar=calendar,
        )

        # Push top-5 report to Feishu (non-critical)
        await _notify_feishu_ml_top5(scan_result)

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
            "gain_pct": round(rec.gain_pct, 2),
            "early_gain_pct": round(rec.early_gain_pct, 2),
            "turnover_amp": round(rec.turnover_amp, 4),
            "ml_score": round(rec.ml_score, 6),
            "hot_board_count": scan_result.hot_board_count,
            "final_candidates": scan_result.final_candidates,
        }

    # --- Monitoring scheduler ---

    TRADING_HOURS = (time(9, 30), time(15, 0))

    async def _send_readiness_report(now_bj: datetime, app_state: Any) -> None:
        """Send daily readiness report at 09:30."""
        broker = getattr(app_state, "broker", None)
        broker_ready = False
        if broker:
            try:
                broker_ready = await broker.is_ready()
            except Exception:
                pass

        broker_pos = getattr(app_state, "broker_positions", [])
        lines = [
            "[测试服] 每日就绪报告",
            f"日期: {now_bj.strftime('%Y-%m-%d %H:%M')}",
            f"Broker状态: {'就绪' if broker_ready else '未就绪'}",
            f"当前持仓: {len(broker_pos)}只",
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

    async def _monitoring_scheduler(app_state: Any) -> None:
        """Lightweight monitoring: daily readiness report + broker health check."""
        readiness_done_date = ""
        broker_alert_ts: float = 0
        ALERT_COOLDOWN = 30 * 60

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
                        and time(9, 30) <= ex_time <= time(9, 35)
                    ):
                        readiness_done_date = ex_date
                        await _send_readiness_report(now_bj, app_state)

                    if readiness_done_date != ex_date and ex_time > time(9, 35):
                        readiness_done_date = ex_date

                    # --- BROKER HEALTH CHECK during trading hours ---
                    broker = getattr(app_state, "broker", None)
                    if broker and TRADING_HOURS[0] <= ex_time <= TRADING_HOURS[1]:
                        try:
                            ready = await broker.is_ready()
                        except Exception:
                            ready = False

                        now_ts = now_bj.timestamp()
                        if not ready:
                            if broker_alert_ts == 0 or (now_ts - broker_alert_ts) >= ALERT_COOLDOWN:
                                broker_alert_ts = now_ts
                                await _notify_feishu_error(
                                    "Broker未就绪",
                                    f"xtquant-trade-server /readyz 检查失败\n"
                                    f"时间: {now_bj.strftime('%H:%M:%S')}\n"
                                    f"请检查Windows服务器上的xtquant-trade-server是否正常运行",
                                )
                        else:
                            broker_alert_ts = 0  # reset so next disconnect alerts immediately

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Monitoring iteration error: {e}", exc_info=True)

                await asyncio.sleep(30)

        except asyncio.CancelledError:
            logger.info("Monitoring scheduler stopped")

    def _start_monitoring(app_state: Any) -> None:
        """Start the monitoring scheduler. Safe to call at any time."""
        if _state.get("monitoring_task") and not _state["monitoring_task"].done():
            return
        _state["monitoring_task"] = asyncio.create_task(_monitoring_scheduler(app_state))

    router._start_monitoring = _start_monitoring  # type: ignore[attr-defined]

    def _get_status(app_state: Any) -> dict:
        """Return broker connection status."""
        broker = getattr(app_state, "broker", None)
        broker_positions = getattr(app_state, "broker_positions", [])
        available_cash = getattr(app_state, "available_cash", 0)
        return {
            "broker_configured": broker is not None,
            "holdings_count": len(broker_positions),
            "available_cash": available_cash,
        }

    router._get_status = _get_status  # type: ignore[attr-defined]

    # --- ML background scheduler (trading operations only) ---

    async def _signal_scheduler() -> None:
        """ML trading scheduler — scan + Feishu report only.

        Trading window:
        - SCAN (09:39-10:00): Run ML scan → push Feishu report
        """
        SCAN_WINDOW = (time(9, 39), time(10, 0))

        logger.info("ML signal scheduler started")

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
                            logger.info(f"iQuant: {ex_date} is not a trading day")
                            scan_done_date = ex_date
                        await asyncio.sleep(3600)
                        continue
                except Exception as e:
                    logger.warning(f"Trade calendar check failed, proceeding: {e}")

                # --- SCAN: 09:39-10:00 ---
                if scan_done_date != ex_date and SCAN_WINDOW[0] <= ex_time <= SCAN_WINDOW[1]:
                    from src.common.config import get_daily_scan_enabled

                    if not get_daily_scan_enabled():
                        logger.info("iQuant: daily scan disabled, skipping")
                        scan_done_date = ex_date
                        await asyncio.sleep(120)
                        continue

                    if not _state["initialized"]:
                        await asyncio.sleep(10)
                        continue

                    scan_done_date = ex_date

                    try:
                        rec = await _run_ml_scan()
                        if rec:
                            logger.info(
                                f"ML scan: recommends {rec['stock_code']} "
                                f"(auto-trading disabled, no order placed)"
                            )
                        else:
                            logger.info("ML scan: no recommendation today")
                            await _notify_feishu_error(
                                "ML扫描结果",
                                "今日ML扫描完成，无符合条件的推荐股票",
                            )
                    except Exception as e:
                        error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                        logger.error(f"ML scan failed: {error_detail}")
                        await _notify_feishu_error("ML扫描失败", error_detail)

                # Scan deadline
                if scan_done_date != ex_date and ex_time > SCAN_WINDOW[1]:
                    scan_done_date = ex_date

                await asyncio.sleep(120 if scan_done_date == ex_date else 30)

        except asyncio.CancelledError:
            logger.info("ML signal scheduler stopped")
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            logger.critical(f"Signal scheduler CRASHED: {error_detail}")
            await _notify_feishu_error(
                "交易调度器崩溃",
                f"信号调度器意外退出!\n{error_detail}\n今日将无法自动交易，请立即检查",
            )

    # --- Endpoints ---

    @router.get("/ping")
    async def ping(
        request: Request,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Health check + trigger lazy init."""
        await _ensure_resources()
        now = datetime.now(BEIJING_TZ)
        broker_positions = getattr(request.app.state, "broker_positions", [])
        return {
            "status": "ok",
            "service": "iquant-ml",
            "server_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "broker_positions": len(broker_positions),
        }

    @router.post("/trigger-scan")
    async def trigger_scan(api_key: str = Depends(_verify_api_key)) -> dict:
        """Manually trigger ML scan + Feishu top-5 report (bypasses time window)."""
        await _ensure_resources()

        try:
            rec = await _run_ml_scan()
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
        """Return all stock codes in universe (cached)."""
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
        """Run ML scan for a specific historical date."""
        from src.strategy.ml_strategy_service import (
            MinuteDataMissingError,
            run_ml_backtest,
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

        storage = getattr(request.app.state, "storage", None)
        if not storage or not storage.is_ready:
            raise HTTPException(
                status_code=503,
                detail="GreptimeDB 缓存未连接。请先在 web 页面的回测页下载数据。",
            )

        try:
            scan_result = await run_ml_backtest(
                storage=storage,
                concept_mapper=_state["concept_mapper"],
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
                "ml_score": round(rec.ml_score, 4),
            }
        }

    @router.get("/status")
    async def iquant_status(request: Request) -> JSONResponse:
        """Public broker connection status (no auth required)."""
        return JSONResponse(
            content=_get_status(request.app.state),
            headers={"Cache-Control": "no-store"},
        )

    return router
