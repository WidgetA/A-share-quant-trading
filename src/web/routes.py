# === MODULE PURPOSE ===
# API routes and page handlers for trading confirmations, backtesting, and settings.
#
# === ENDPOINTS ===
# GET  /                     - Main dashboard (HTML)
# GET  /confirm/{id}         - Confirmation page (HTML)
# GET  /api/pending          - List pending confirmations (JSON)
# GET  /api/pending/{id}     - Get confirmation details (JSON)
# POST /api/pending/{id}/submit - Submit user decision (JSON)
# GET  /api/status           - Health check (JSON)
# GET  /api/positions        - Get current positions (JSON)
#
# === MOMENTUM BACKTEST ENDPOINTS ===
# POST /api/momentum/backtest          - Run single-day momentum scan (JSON)
# POST /api/momentum/combined-analysis - Run range backtest with SSE streaming
# GET  /api/momentum/monitor-status    - Get intraday monitor status (JSON)
# POST /api/momentum/tsanghi-prepare   - Pre-download tsanghi cache (SSE)
# GET  /api/momentum/tsanghi-cache-status - Cache status (JSON)

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from starlette.responses import StreamingResponse

from src.common.pending_store import PendingConfirmationStore

logger = logging.getLogger(__name__)


class SubmitRequest(BaseModel):
    """Request body for submitting a confirmation result."""

    selection: Any  # Can be list[int], str ("all", "skip"), bool, etc.


class SubmitResponse(BaseModel):
    """Response for confirmation submission."""

    success: bool
    message: str


class TokenUpdateRequest(BaseModel):
    """Request body for updating API tokens."""

    token: str


def create_router() -> APIRouter:
    """Create API router with all endpoints."""
    router = APIRouter()

    def get_store(request: Request) -> PendingConfirmationStore:
        """Get pending store from app state."""
        return request.app.state.pending_store

    # ==================== HTML Pages ====================

    @router.get("/", response_class=HTMLResponse)
    async def index_page(request: Request):
        """Main dashboard with status cards + trading module."""
        from datetime import datetime
        from zoneinfo import ZoneInfo

        templates = request.app.state.templates

        # Get iQuant connection status
        iquant_rtr = getattr(request.app.state, "iquant_router", None)
        if iquant_rtr and hasattr(iquant_rtr, "_get_status"):
            iquant_status = iquant_rtr._get_status()
        else:
            iquant_status = {
                "connected": False,
                "last_poll_time": None,
                "gap_seconds": None,
                "holdings_count": 0,
                "pending_count": 0,
            }

        # Get cache scheduler status
        scheduler = getattr(request.app.state, "cache_scheduler", None)
        scheduler_status = (
            scheduler.get_status()
            if scheduler
            else {
                "enabled": False,
                "next_run_time": None,
                "last_run_time": None,
                "last_run_result": None,
                "last_run_message": None,
            }
        )

        # Get model training scheduler status
        model_sched = getattr(request.app.state, "model_scheduler", None)
        model_scheduler_status = (
            model_sched.get_status()
            if model_sched
            else {
                "next_run_time": None,
                "last_run_time": None,
                "last_run_result": None,
                "last_run_message": None,
                "has_full_model": False,
                "current_model": None,
            }
        )

        today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")

        # Get daily scan status
        from src.common.config import get_daily_scan_enabled, get_recommendations_enabled

        monitor_state = getattr(request.app.state, "momentum_monitor_state", {})
        daily_scan_status = {
            "enabled": get_daily_scan_enabled(),
            "running": monitor_state.get("running", False),
            "last_scan_time": monitor_state.get("last_scan_time"),
        }

        resp = templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "iquant_status": iquant_status,
                "scheduler": scheduler_status,
                "model_scheduler": model_scheduler_status,
                "daily_scan": daily_scan_status,
                "recommendations_enabled": get_recommendations_enabled(),
                "today": today,
            },
        )
        resp.headers["Cache-Control"] = "no-cache"
        return resp

    @router.get("/confirm/{confirm_id}", response_class=HTMLResponse)
    async def confirm_page(request: Request, confirm_id: str):
        """Confirmation page for a specific pending item."""
        store = get_store(request)
        confirm = store.get_confirmation(confirm_id)
        templates = request.app.state.templates

        if not confirm:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": "not_found",
                    "message": f"确认项 {confirm_id} 不存在或已过期",
                },
                status_code=404,
            )

        return templates.TemplateResponse(
            "confirm.html",
            {
                "request": request,
                "confirm": confirm.to_detail_dict(),
            },
        )

    @router.get("/momentum", response_class=HTMLResponse)
    async def backtest_page(request: Request):
        """Unified backtest page with 3 tabs."""
        templates = request.app.state.templates
        return templates.TemplateResponse("backtest.html", {"request": request})

    # ==================== API Endpoints ====================

    @router.get("/api/status")
    async def api_status(request: Request) -> dict:
        """Health check endpoint."""
        import os

        store = get_store(request)
        return {
            "status": "ok",
            "pending_count": len(store),
            "git_commit": os.environ.get("GIT_COMMIT", "unknown"),
            "git_branch": os.environ.get("GIT_BRANCH", "unknown"),
            "build_time": os.environ.get("BUILD_TIME", "unknown"),
        }

    @router.get("/api/pending")
    async def api_pending_list(request: Request) -> dict:
        """List all pending confirmations."""
        store = get_store(request)
        pending = store.get_pending_list()
        return {
            "pending": pending,
            "count": len(pending),
        }

    @router.get("/api/pending/{confirm_id}")
    async def api_pending_detail(request: Request, confirm_id: str) -> dict:
        """Get details of a specific confirmation."""
        store = get_store(request)
        confirm = store.get_confirmation(confirm_id)

        if not confirm:
            raise HTTPException(
                status_code=404,
                detail=f"Confirmation {confirm_id} not found or expired",
            )

        return confirm.to_detail_dict()

    @router.post("/api/pending/{confirm_id}/submit")
    async def api_submit(
        request: Request,
        confirm_id: str,
        body: SubmitRequest,
    ) -> SubmitResponse:
        """Submit user's decision for a confirmation."""
        store = get_store(request)
        confirm = store.get_confirmation(confirm_id)

        if not confirm:
            raise HTTPException(
                status_code=404,
                detail=f"Confirmation {confirm_id} not found or expired",
            )

        # Parse and validate selection based on confirmation type
        result = _parse_selection(confirm.confirm_type.value, body.selection, confirm.data)

        success = store.submit_result(confirm_id, result)

        if success:
            logger.info(f"User submitted selection for {confirm_id}: {result}")
            return SubmitResponse(success=True, message="选择已确认")
        else:
            raise HTTPException(
                status_code=400,
                detail="Failed to submit selection",
            )

    return router


def _parse_selection(confirm_type: str, selection: Any, data: dict) -> Any:
    """Parse and validate user selection based on confirmation type."""
    if confirm_type == "premarket":
        if selection == "all":
            return "all"
        if selection in ("skip", None, []):
            return []
        if isinstance(selection, list):
            max_idx = len(data.get("signals", []))
            return [i for i in selection if isinstance(i, int) and 1 <= i <= max_idx]
        return []

    elif confirm_type == "intraday":
        if isinstance(selection, bool):
            return selection
        if selection in ("yes", "y", "buy", True):
            return True
        return False

    elif confirm_type == "morning":
        if selection == "all":
            return "all"
        if selection in ("hold", None, []):
            return []
        if isinstance(selection, list):
            return selection
        return []

    elif confirm_type == "limit_up":
        if selection == "all":
            return "all"
        if selection in ("skip", None, []):
            return None
        if isinstance(selection, list):
            max_idx = len(data.get("available_stocks", []))
            return [i for i in selection if isinstance(i, int) and 1 <= i <= max_idx]
        return None

    else:
        return selection


# ==================== Momentum Backtest Pydantic Models ====================


class BacktestScanRequest(BaseModel):
    """Request body for single-day momentum scan."""

    trade_date: str  # YYYY-MM-DD format
    notify: bool = False  # Send Feishu notification


class RangeBacktestRequest(BaseModel):
    """Request body for range backtest with capital simulation."""

    start_date: str  # YYYY-MM-DD format
    end_date: str  # YYYY-MM-DD format
    initial_capital: float  # Starting capital in yuan


class TsanghiPrepareRequest(BaseModel):
    """Request body for tsanghi data pre-download."""

    start_date: str  # YYYY-MM-DD format
    end_date: str  # YYYY-MM-DD format
    force: bool = False  # Force full re-download (clears existing cache)


# ==================== Momentum Backtest Router ====================


def create_momentum_router() -> APIRouter:
    """Create router for momentum backtest and intraday monitor endpoints."""
    import asyncio
    import json
    from datetime import datetime

    router = APIRouter(tags=["momentum"])

    def _sse(data: dict) -> str:
        return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

    def _get_monitor_state(request: Request) -> dict[str, Any]:
        """Get monitor state from app.state (created at startup)."""
        return getattr(
            request.app.state,
            "momentum_monitor_state",
            {
                "running": False,
                "last_scan_time": None,
                "last_result": None,
                "today_results": [],
                "task": None,
            },
        )

    def _get_fundamentals_db(request: Request):
        """Get shared fundamentals DB from app.state."""
        db = getattr(request.app.state, "fundamentals_db", None)
        if db is None:
            raise HTTPException(status_code=503, detail="基本面数据库未就绪")
        return db

    # === Tsanghi cache endpoints ===

    @router.get("/api/momentum/tsanghi-cache-status")
    async def tsanghi_cache_status(request: Request):
        """Return backtest cache state for frontend polling."""
        cache = getattr(request.app.state, "backtest_cache", None)
        if cache is None:
            return {"status": "empty"}
        return await cache.get_cache_status()

    @router.post("/api/momentum/tsanghi-prepare")
    async def tsanghi_prepare(request: Request, body: TsanghiPrepareRequest):
        """Pre-download backtest data as SSE stream (incremental)."""
        # Check tsanghi token first — most common misconfiguration on new servers
        from src.common.config import get_tsanghi_token_source

        if get_tsanghi_token_source() == "not_configured":
            raise HTTPException(
                status_code=503,
                detail="沧海数据 token 未配置，请先在设置页面配置 tsanghi token",
            )

        try:
            start_date = datetime.strptime(body.start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(body.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误")

        cache = getattr(request.app.state, "backtest_cache", None)
        if cache is None:
            raise HTTPException(status_code=503, detail="GreptimeDB 缓存未连接")

        # Check if already covered (boundaries + minute gaps)
        if not body.force:
            gaps = await cache.missing_ranges(start_date, end_date)
            if not gaps:
                status = await cache.get_cache_status()

                async def cached_stream():
                    msg = {
                        "type": "complete",
                        "daily_count": status.get("daily_stocks", 0),
                        "minute_count": status.get("minute_stocks", 0),
                        "cached": True,
                    }
                    yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"

                return StreamingResponse(cached_stream(), media_type="text/event-stream")

        # Download missing data (real-time progress via asyncio.Queue)
        def _fmt_progress(phase: str, current: int, total: int, detail: str = "") -> str:
            if phase == "integrity_check":
                if detail:
                    return f"完整性检查: {detail}"
                return f"数据完整性检查中... ({current}/{total})"
            elif phase == "init":
                return "正在初始化..."
            elif phase == "daily_resume":
                if current > 0:
                    return f"日线已缓存 {current} 天，检查剩余..."
                return "检查日线缓存..."
            elif phase == "daily":
                if detail:
                    return f"日线 {current}/{total}: {detail}"
                return f"下载日线数据: {current}/{total} 天"
            elif phase == "minute_resume":
                remaining = total - current
                if current > 0:
                    return f"分钟线已缓存 {current}/{total} 只，需下载 {remaining} 只"
                return f"分钟线需下载 {total} 只"
            elif phase == "backfill":
                if detail:
                    return f"回填 {current}/{total}: {detail}"
                return f"回填停牌标记: {current}/{total} 天"
            elif phase == "minute_active":
                return f"正在下载: {detail} ({current}/{total} 已完成)"
            elif phase == "minute":
                if detail:
                    return f"分钟线 {current}/{total} 已写入"
                return f"下载分钟线数据: {current}/{total} 只"
            elif phase == "download":
                return f"下载完成: 共 {total} 只股票"
            elif phase == "post_integrity":
                if current == 0:
                    return "下载后完整性检查通过"
                return f"下载后完整性检查: {current} 个警告"
            return f"{phase}: {current}/{total}"

        async def download_stream():
            import threading

            queue: asyncio.Queue[dict | None] = asyncio.Queue()
            cancel_event = threading.Event()

            # Cancel any existing download task
            prev_task = getattr(request.app.state, "cache_download_task", None)
            if prev_task and not prev_task.done():
                prev_task.cancel()
                try:
                    await prev_task
                except (asyncio.CancelledError, Exception):
                    pass

            def on_progress(phase: str, current: int, total: int, detail: str = ""):
                if phase == "integrity_check":
                    overall = 0.01 + 0.04 * (current / total) if total > 0 else 0.01
                elif phase == "init":
                    overall = 0.05
                elif phase == "daily_resume":
                    overall = 0.1
                elif phase == "backfill":
                    overall = 0.1 + 0.05 * (current / total) if total > 0 else 0.1
                elif phase == "daily":
                    overall = 0.15 + 0.05 * (current / total) if total > 0 else 0.15
                elif phase == "minute_resume":
                    overall = 0.2
                elif phase == "minute_active":
                    overall = 0.2 + 0.8 * (current / total) if total > 0 else 0.2
                elif phase == "minute":
                    overall = 0.2 + 0.8 * (current / total) if total > 0 else 0.2
                elif phase == "post_integrity":
                    # Push a post-download integrity report so the frontend shows it
                    warnings = [w for w in detail.split("\n") if w] if detail else []
                    if warnings:
                        queue.put_nowait(
                            {
                                "type": "integrity_report",
                                "issues": [
                                    {"level": "warning", "message": w, "count": 0, "samples": []}
                                    for w in warnings
                                ],
                                "message": f"下载后完整性检查: {len(warnings)} 个警告",
                            }
                        )
                    else:
                        queue.put_nowait(
                            {
                                "type": "integrity_report",
                                "issues": [],
                                "message": "下载后完整性检查通过",
                            }
                        )
                    return  # already pushed to queue, skip normal progress message
                else:
                    overall = 1.0
                queue.put_nowait(
                    {
                        "type": "progress",
                        "progress": overall,
                        "message": _fmt_progress(phase, current, total, detail),
                        "phase": phase,
                    }
                )

            async def _run_download():
                try:
                    # Pre-download integrity check
                    on_progress("integrity_check", 0, 1, "开始检查...")
                    issues = await cache.check_data_integrity()
                    on_progress("integrity_check", 1, 1, "检查完成")
                    if issues:
                        error_count = sum(1 for i in issues if i["level"] == "error")
                        warn_count = sum(1 for i in issues if i["level"] == "warning")
                        parts = []
                        if error_count:
                            parts.append(f"{error_count} 个错误")
                        if warn_count:
                            parts.append(f"{warn_count} 个警告")
                        queue.put_nowait(
                            {
                                "type": "integrity_report",
                                "issues": [
                                    {
                                        "level": i["level"],
                                        "message": i["message"],
                                        "count": i["count"],
                                        "samples": i.get("samples", []),
                                    }
                                    for i in issues
                                ],
                                "message": f"完整性检查: {', '.join(parts)}",
                            }
                        )
                    else:
                        queue.put_nowait(
                            {
                                "type": "integrity_report",
                                "issues": [],
                                "message": "数据完整性检查通过",
                            }
                        )

                    await cache.download_prices(
                        start_date,
                        end_date,
                        progress_cb=on_progress,
                        cancel_event=cancel_event,
                    )
                    queue.put_nowait(None)  # sentinel: success
                except asyncio.CancelledError:
                    cancel_event.set()
                    queue.put_nowait({"type": "cancelled", "message": "下载已取消"})
                except Exception as e:
                    queue.put_nowait({"type": "error", "message": str(e)[:200]})

            download_task = asyncio.create_task(_run_download())
            request.app.state.cache_download_task = download_task
            request.app.state.cache_download_cancel = cancel_event

            try:
                yield _sse(
                    {
                        "type": "status",
                        "message": f"开始下载 {body.start_date} ~ {body.end_date} ...",
                    }
                )

                while True:
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=30)
                    except asyncio.TimeoutError:
                        yield _sse({"type": "heartbeat"})
                        continue

                    if event is None:
                        break  # download done
                    if event.get("type") in ("error", "cancelled"):
                        yield _sse(event)
                        return

                    yield _sse(event)

                status = await cache.get_cache_status()
                yield _sse(
                    {
                        "type": "complete",
                        "daily_count": status.get("daily_stocks", 0),
                        "minute_count": status.get("minute_stocks", 0),
                        "cached": False,
                    }
                )
            except Exception as e:
                logger.error(f"Cache download error: {e}", exc_info=True)
                yield _sse({"type": "error", "message": str(e)[:200]})
            finally:
                request.app.state.cache_download_task = None
                request.app.state.cache_download_cancel = None

        return StreamingResponse(
            download_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @router.post("/api/momentum/tsanghi-cancel")
    async def tsanghi_cancel(request: Request):
        """Cancel an in-progress cache download."""
        task = getattr(request.app.state, "cache_download_task", None)
        if task and not task.done():
            cancel_event = getattr(request.app.state, "cache_download_cancel", None)
            if cancel_event:
                cancel_event.set()
            task.cancel()
            return {"success": True, "message": "取消请求已发送"}
        return {"success": False, "message": "没有正在进行的下载"}

    # === Single-day momentum scan ===

    @router.post("/api/momentum/backtest")
    async def run_backtest(request: Request, body: BacktestScanRequest) -> dict:
        """Run momentum strategy scan for a specific date using backtest cache."""
        from src.common.feishu_bot import FeishuBot
        from src.strategy.momentum_strategy_service import (
            MinuteDataMissingError,
            run_momentum_backtest,
        )

        try:
            trade_date = datetime.strptime(body.trade_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"日期格式错误: {body.trade_date}，请使用 YYYY-MM-DD",
            )

        fundamentals_db = _get_fundamentals_db(request)

        cache = getattr(request.app.state, "backtest_cache", None)
        if not cache or not cache.is_ready:
            raise HTTPException(status_code=400, detail="请先预下载回测数据")

        try:
            try:
                result = await run_momentum_backtest(
                    backtest_cache=cache,
                    fundamentals_db=fundamentals_db,
                    trade_date=trade_date,
                )
            except MinuteDataMissingError as e:
                return {
                    "success": False,
                    "trade_date": body.trade_date,
                    "error": str(e),
                }
            if not result.recommended and not result.all_scored:
                return {
                    "success": False,
                    "trade_date": body.trade_date,
                    "error": f"沧海缓存中无 {trade_date.strftime('%Y-%m-%d')} 的日线数据",
                }

            rec = result.recommended
            response_data: dict[str, Any] = {
                "success": True,
                "trade_date": body.trade_date,
                "initial_gainers": result.initial_gainers_count,
                "hot_boards": result.hot_board_count,
                "l4_count": result.l4_count,
                "l5_count": result.l5_count,
                "l6_count": result.l6_count,
                "final_candidates": result.final_candidates,
                "scored_stocks": [
                    {
                        "stock_code": s.stock_code,
                        "stock_name": s.stock_name,
                        "board_name": s.board_name,
                        "v3_score": round(s.v3_score, 4),
                        "gain_from_open_pct": round(s.gain_from_open_pct, 2),
                        "turnover_amp": round(s.turnover_amp, 2),
                        "latest_price": round(s.latest_price, 2),
                    }
                    for s in result.all_scored
                ],
                "recommended_stock": {
                    "stock_code": rec.stock_code,
                    "stock_name": rec.stock_name,
                    "board_name": rec.board_name,
                    "v3_score": round(rec.v3_score, 4),
                    "gain_from_open_pct": round(rec.gain_from_open_pct, 2),
                    "turnover_amp": round(rec.turnover_amp, 2),
                    "open_price": round(rec.open_price, 2),
                    "prev_close": round(rec.prev_close, 2),
                    "latest_price": round(rec.latest_price, 2),
                }
                if rec
                else None,
            }

            # Send Feishu if requested
            if body.notify and rec:
                bot = FeishuBot()
                if bot.is_configured():
                    from src.strategy.models import RecommendedStock

                    # Adapt MomentumScoredStock to RecommendedStock for Feishu
                    adapted_rec = RecommendedStock(
                        stock_code=rec.stock_code,
                        stock_name=rec.stock_name,
                        board_name=rec.board_name,
                        board_stock_count=result.final_candidates,
                        open_gain_pct=round(
                            (rec.open_price - rec.prev_close) / rec.prev_close * 100
                            if rec.prev_close > 0
                            else 0,
                            2,
                        ),
                        pe_ttm=0.0,
                        board_avg_pe=0.0,
                        open_price=rec.open_price,
                        prev_close=rec.prev_close,
                        latest_price=rec.latest_price,
                        gain_from_open_pct=rec.gain_from_open_pct,
                        turnover_amp=rec.turnover_amp,
                        composite_score=rec.v3_score,
                    )
                    await bot.send_momentum_scan_result(
                        selected_stocks=[],
                        hot_boards={},
                        initial_gainer_count=result.initial_gainers_count,
                        scan_time=result.scan_time,
                        recommended_stock=adapted_rec,
                    )
                    response_data["feishu_sent"] = True

            return response_data

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Momentum backtest error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"回测出错: {str(e)}")

    # === Range backtest with capital simulation ===

    @router.post("/api/momentum/combined-analysis")
    async def run_combined_analysis(request: Request, body: RangeBacktestRequest):
        """Run momentum range backtest with capital simulation (SSE streaming)."""
        import json
        import math
        from datetime import datetime

        from src.strategy.momentum_strategy_service import (
            MinuteDataMissingError,
            run_momentum_backtest,
        )

        try:
            start_date = datetime.strptime(body.start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(body.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD")

        if end_date <= start_date:
            raise HTTPException(status_code=400, detail="结束日期必须晚于起始日期")

        if body.initial_capital < 1000:
            raise HTTPException(status_code=400, detail="起始资金不能低于 1000 元")

        backtest_cache = getattr(request.app.state, "backtest_cache", None)
        if not backtest_cache or not backtest_cache.is_ready:
            raise HTTPException(status_code=400, detail="请先预下载回测数据")

        fundamentals_db = _get_fundamentals_db(request)

        async def event_stream():
            def sse(data: dict) -> str:
                return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

            try:
                from datetime import timedelta

                cal_end = end_date + timedelta(days=10)
                trading_days_all = await _get_trading_calendar(start_date, cal_end)
                if not trading_days_all:
                    yield sse({"type": "error", "message": "所选日期范围内无交易日"})
                    return

                days_in_range = [d for d in trading_days_all if start_date <= d <= end_date]
                if not days_in_range:
                    yield sse({"type": "error", "message": "所选日期范围内无交易日"})
                    return

                # Build T+1 map
                next_day_map = {}
                for i, d in enumerate(trading_days_all):
                    if d > end_date:
                        break
                    if d >= start_date and i + 1 < len(trading_days_all):
                        next_day_map[d] = trading_days_all[i + 1]

                if len(days_in_range) > 250:
                    days_in_range = days_in_range[:250]
                    yield sse(
                        {
                            "type": "warning",
                            "message": f"已截断至前 250 个交易日"
                            f" ({days_in_range[0]} ~ {days_in_range[-1]})",
                        }
                    )

                yield sse(
                    {
                        "type": "init",
                        "total_days": len(days_in_range),
                        "start_date": str(days_in_range[0]),
                        "end_date": str(days_in_range[-1]),
                        "initial_capital": body.initial_capital,
                    }
                )

                capital = body.initial_capital
                day_results: list[dict] = []

                for day_idx, trade_date in enumerate(days_in_range):
                    next_trade_date = next_day_map.get(trade_date)
                    if not next_trade_date:
                        continue

                    yield sse(
                        {
                            "type": "progress",
                            "day": day_idx + 1,
                            "total": len(days_in_range),
                            "trade_date": str(trade_date),
                        }
                    )

                    try:
                        try:
                            scan_result = await run_momentum_backtest(
                                backtest_cache=backtest_cache,
                                fundamentals_db=fundamentals_db,
                                trade_date=trade_date,
                            )
                        except MinuteDataMissingError as e:
                            day_results.append(
                                {
                                    "trade_date": str(trade_date),
                                    "has_trade": False,
                                    "skip_reason": str(e),
                                    "capital": round(capital, 2),
                                }
                            )
                            yield sse(
                                {
                                    "type": "day_skip",
                                    "trade_date": str(trade_date),
                                    "reason": str(e),
                                }
                            )
                            await asyncio.sleep(0.05)
                            continue

                        # Empty result means no data for that date
                        if not scan_result.recommended and not scan_result.all_scored:
                            date_key = trade_date.strftime("%Y-%m-%d")
                            day_results.append(
                                {
                                    "trade_date": str(trade_date),
                                    "has_trade": False,
                                    "skip_reason": f"沧海缓存中无 {date_key} 的日线数据",
                                    "capital": round(capital, 2),
                                }
                            )
                            yield sse(
                                {
                                    "type": "day_skip",
                                    "trade_date": str(trade_date),
                                    "reason": f"沧海缓存中无 {date_key} 的日线数据",
                                }
                            )
                            await asyncio.sleep(0.05)
                            continue

                        rec = scan_result.recommended

                        # Build day backtest entry
                        day_backtest: dict = {
                            "trade_date": str(trade_date),
                            "has_trade": False,
                            "capital": round(capital, 2),
                            "funnel": {
                                "l1": scan_result.initial_gainers_count,
                                "l3": scan_result.hot_board_count,
                                "l4": scan_result.l4_count,
                                "l5": scan_result.l5_count,
                                "l6": scan_result.l6_count,
                                "final": scan_result.final_candidates,
                            },
                        }

                        if rec:
                            buy_price = rec.latest_price
                            if buy_price <= 0:
                                buy_price = rec.open_price

                            # Get T+1 open from cache
                            next_date_key = next_trade_date.strftime("%Y-%m-%d")
                            next_day_data = await backtest_cache.get_daily(
                                rec.stock_code,
                                next_date_key,
                            )
                            sell_price_val = (
                                float(next_day_data.open)
                                if next_day_data and next_day_data.open
                                else 0.0
                            )

                            if buy_price > 0 and sell_price_val > 0:
                                lots = math.floor(capital / (buy_price * 100))
                                if lots > 0:
                                    buy_amount = lots * 100 * buy_price
                                    buy_commission = max(buy_amount * 0.003, 5.0)
                                    buy_transfer = buy_amount * 0.00001
                                    total_buy_cost = buy_amount + buy_commission + buy_transfer

                                    while total_buy_cost > capital and lots > 0:
                                        lots -= 1
                                        buy_amount = lots * 100 * buy_price
                                        buy_commission = max(buy_amount * 0.003, 5.0)
                                        buy_transfer = buy_amount * 0.00001
                                        total_buy_cost = buy_amount + buy_commission + buy_transfer

                                if lots > 0:
                                    sell_amount = lots * 100 * sell_price_val
                                    sell_commission = max(sell_amount * 0.003, 5.0)
                                    sell_transfer = sell_amount * 0.00001
                                    sell_stamp = sell_amount * 0.0005
                                    net_sell = (
                                        sell_amount - sell_commission - sell_transfer - sell_stamp
                                    )

                                    capital_before = capital
                                    capital = capital - total_buy_cost + net_sell
                                    trade_profit = net_sell - total_buy_cost
                                    trade_return_pct = (
                                        trade_profit / total_buy_cost * 100
                                        if total_buy_cost > 0
                                        else 0
                                    )

                                    day_backtest = {
                                        "trade_date": str(trade_date),
                                        "has_trade": True,
                                        "stock_code": rec.stock_code,
                                        "stock_name": rec.stock_name,
                                        "board_name": rec.board_name,
                                        "buy_price": round(buy_price, 2),
                                        "sell_price": round(sell_price_val, 2),
                                        "sell_date": str(next_trade_date),
                                        "lots": lots,
                                        "profit": round(trade_profit, 2),
                                        "return_pct": round(trade_return_pct, 2),
                                        "capital_before": round(capital_before, 2),
                                        "capital": round(capital, 2),
                                        "v3_score": round(rec.v3_score, 4),
                                        "funnel": {
                                            "l1": scan_result.initial_gainers_count,
                                            "l3": scan_result.hot_board_count,
                                            "l4": scan_result.l4_count,
                                            "l5": scan_result.l5_count,
                                            "l6": scan_result.l6_count,
                                            "final": scan_result.final_candidates,
                                        },
                                    }
                                else:
                                    day_backtest["skip_reason"] = (
                                        f"资金不足 (需 {buy_price * 100:.0f} 元/手)"
                                    )
                                    day_backtest["stock_code"] = rec.stock_code
                                    day_backtest["stock_name"] = rec.stock_name
                            else:
                                reason_parts = []
                                if buy_price <= 0:
                                    reason_parts.append("无买入价")
                                if sell_price_val <= 0:
                                    reason_parts.append("无次日开盘卖出价")
                                day_backtest["skip_reason"] = "、".join(reason_parts)
                                day_backtest["stock_code"] = rec.stock_code
                                day_backtest["stock_name"] = rec.stock_name
                        else:
                            day_backtest["skip_reason"] = "无推荐"

                        day_backtest["capital"] = round(capital, 2)
                        day_results.append(day_backtest)

                        yield sse(
                            {
                                "type": "day_result",
                                "trade_date": str(trade_date),
                                "backtest": day_backtest,
                            }
                        )
                        await asyncio.sleep(0.05)

                    except Exception as e:
                        logger.error(
                            f"Range backtest error on {trade_date}: {e}",
                            exc_info=True,
                        )
                        day_results.append(
                            {
                                "trade_date": str(trade_date),
                                "has_trade": False,
                                "skip_reason": f"出错: {str(e)[:60]}",
                                "capital": round(capital, 2),
                            }
                        )
                        yield sse(
                            {
                                "type": "day_skip",
                                "trade_date": str(trade_date),
                                "reason": f"出错: {str(e)[:60]}",
                            }
                        )
                        await asyncio.sleep(0.05)
                        continue

                # === Summary ===
                trade_results = [d for d in day_results if d.get("has_trade")]
                wins = [d for d in trade_results if d["profit"] > 0]
                losses = [d for d in trade_results if d["profit"] < 0]
                total_return_pct = (capital - body.initial_capital) / body.initial_capital * 100

                backtest_summary = {
                    "initial_capital": body.initial_capital,
                    "final_capital": round(capital, 2),
                    "total_return_pct": round(total_return_pct, 2),
                    "total_days": len(days_in_range),
                    "trade_days": len(trade_results),
                    "skip_days": len(days_in_range) - len(trade_results),
                    "win_days": len(wins),
                    "lose_days": len(losses),
                    "even_days": len(trade_results) - len(wins) - len(losses),
                    "win_rate": round(
                        len(wins) / len(trade_results) * 100 if trade_results else 0,
                        1,
                    ),
                    "max_win": round(max((d["profit"] for d in trade_results), default=0), 2),
                    "max_loss": round(min((d["profit"] for d in trade_results), default=0), 2),
                }

                yield sse(
                    {
                        "type": "complete",
                        "days_processed": len(days_in_range),
                        "backtest_summary": backtest_summary,
                    }
                )

            except Exception as e:
                logger.error(f"Range backtest error: {e}", exc_info=True)
                yield sse({"type": "error", "message": f"分析出错: {str(e)}"})

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # === Intraday monitor endpoints ===

    @router.get("/api/momentum/monitor-status")
    async def get_monitor_status(request: Request) -> dict:
        """Get intraday monitor status and latest results."""
        monitor_state = _get_monitor_state(request)
        return {
            "running": monitor_state["running"],
            "last_scan_time": monitor_state["last_scan_time"],
            "today_results": monitor_state["today_results"],
        }

    @router.post("/api/momentum/monitor/start")
    async def start_monitor(request: Request) -> dict:
        """Manually start the intraday monitor."""
        monitor_state = _get_monitor_state(request)
        if monitor_state["running"]:
            return {"success": True, "message": "监控已在运行中"}

        monitor_state["_app_state"] = request.app.state
        task = asyncio.create_task(_run_intraday_monitor(monitor_state))
        monitor_state["task"] = task
        return {"success": True, "message": "监控已启动"}

    @router.post("/api/momentum/monitor/stop")
    async def stop_monitor(request: Request) -> dict:
        """Manually stop the intraday monitor."""
        monitor_state = _get_monitor_state(request)
        task = monitor_state.get("task")
        if task and not task.done():
            task.cancel()
        monitor_state["running"] = False
        monitor_state["task"] = None
        return {"success": True, "message": "监控已停止"}

    @router.post("/api/momentum/monitor/trigger")
    async def trigger_monitor_scan(request: Request) -> dict:
        """Manually trigger a momentum scan right now (any time of day)."""
        monitor_state = _get_monitor_state(request)
        backtest_cache = getattr(request.app.state, "backtest_cache", None)
        try:
            result = await _execute_monitor_scan(monitor_state, backtest_cache=backtest_cache)
            if result is None:
                return {"success": False, "message": "扫描未产生结果"}
            return {"success": True, "result": result}
        except Exception as e:
            logger.error(f"Manual scan trigger error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"扫描失败: {str(e)}")

    # === Cache scheduler endpoints ===

    @router.get("/api/cache/status")
    async def cache_scheduler_status(request: Request) -> dict:
        """Return cache scheduler status and coverage info."""
        cache = getattr(request.app.state, "backtest_cache", None)
        scheduler_task = getattr(request.app.state, "cache_scheduler_task", None)
        scheduler = getattr(request.app.state, "cache_scheduler", None)

        scheduler_info = (
            scheduler.get_status()
            if scheduler
            else {
                "enabled": False,
                "next_run_time": None,
                "last_run_time": None,
                "last_run_result": None,
                "last_run_message": None,
            }
        )

        if not cache or not cache.is_ready:
            return {
                "cache_ready": False,
                "start_date": None,
                "end_date": None,
                "daily_stock_count": 0,
                "minute_stock_count": 0,
                "scheduler_running": scheduler_task is not None and not scheduler_task.done()
                if scheduler_task
                else False,
                **scheduler_info,
            }

        db_start, db_end = await cache.get_date_range()
        daily_count = await cache.get_daily_stock_count()
        minute_count = await cache.get_minute_stock_count()
        return {
            "cache_ready": True,
            "start_date": str(db_start) if db_start else None,
            "end_date": str(db_end) if db_end else None,
            "daily_stock_count": daily_count,
            "minute_stock_count": minute_count,
            "scheduler_running": scheduler_task is not None and not scheduler_task.done()
            if scheduler_task
            else False,
            **scheduler_info,
        }

    @router.post("/api/cache/trigger")
    async def trigger_cache_fill(request: Request) -> dict:
        """Manually trigger the cache gap-fill process."""
        from src.data.services.cache_scheduler import CacheScheduler

        cache = getattr(request.app.state, "backtest_cache", None)
        if not cache:
            raise HTTPException(503, "GreptimeDB 缓存未连接")

        scheduler = CacheScheduler(request.app.state)
        result = await scheduler.check_and_fill_gaps()
        return {"success": True, **result}

    return router


# ==================== Helper Functions ====================


def _scan_result_to_recs(date_str: str, scan_result, n: int = 10) -> list[dict]:
    """Convert MomentumScanResult.all_scored to the same dict format as MomentumScanDB.query()."""

    def _f(val):
        return round(float(val), 4) if val is not None else None

    top = scan_result.all_scored[:n]
    return [
        {
            "trade_date": date_str,
            "rank": i + 1,
            "stock_code": s.stock_code,
            "stock_name": s.stock_name,
            "board_name": s.board_name,
            "v3_score": _f(s.v3_score),
            "open_price": _f(s.open_price),
            "prev_close": _f(s.prev_close),
            "latest_price": _f(s.latest_price),
            "gain_from_open_pct": _f(s.gain_from_open_pct),
            "turnover_amp": _f(s.turnover_amp),
            "consecutive_up_days": s.consecutive_up_days,
            "trend_10d": _f(s.trend_10d),
            "final_candidates": scan_result.final_candidates,
        }
        for i, s in enumerate(top)
    ]


async def _compute_momentum_scan(date_str: str, fundamentals_db, backtest_cache):
    """Compute momentum scan on-demand. Returns (recs_list, MomentumScanResult | None).

    Two paths:
    - Past dates: GreptimeDB cache via strategy service (fast, ~1-3s)
    - Today: Tushare batch_get_early_quotes via strategy service (slow, ~30-60s)
    """
    from datetime import date, datetime
    from zoneinfo import ZoneInfo

    from src.strategy.momentum_strategy_service import run_momentum_backtest, run_momentum_live

    beijing_tz = ZoneInfo("Asia/Shanghai")
    now_bj = datetime.now(beijing_tz)
    today_str = now_bj.strftime("%Y-%m-%d")
    is_today = date_str == today_str

    if not is_today:
        # --- Past date: use GreptimeDB cache ---
        if not backtest_cache or not getattr(backtest_cache, "is_ready", False):
            raise ValueError("GreptimeDB 缓存未就绪，无法计算历史推荐")

        trade_date = date.fromisoformat(date_str)
        result = await run_momentum_backtest(
            backtest_cache=backtest_cache,
            fundamentals_db=fundamentals_db,
            trade_date=trade_date,
        )
        if not result.recommended and not result.all_scored:
            return [], None
        recs = _scan_result_to_recs(date_str, result, n=10)
        return recs, result

    # --- Today: live Tushare quotes ---
    from src.data.clients.tushare_realtime import get_tushare_trade_calendar

    today_s = now_bj.strftime("%Y-%m-%d")
    cal_dates = await get_tushare_trade_calendar(today_s, today_s)
    if today_s not in cal_dates:
        raise ValueError(f"今天非交易日（{now_bj.strftime('%m/%d')}），A股不开市")

    if now_bj.hour < 9 or (now_bj.hour == 9 and now_bj.minute < 39):
        raise ValueError("今日扫描需在 09:39 之后执行（早盘数据尚未就绪）")

    from src.common.config import get_tushare_token
    from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter
    from src.data.clients.tushare_realtime import TushareRealtimeClient
    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.filters.stock_filter import create_main_board_only_filter

    stock_filter = create_main_board_only_filter()
    all_codes = await fundamentals_db.get_all_stock_codes()
    universe = [c for c in all_codes if stock_filter.is_allowed(c)]
    logger.info(f"On-demand scan: universe has {len(universe)} codes")

    tushare_token = get_tushare_token()
    tushare = TushareRealtimeClient(token=tushare_token)
    await tushare.start()
    try:
        adapter = IQuantHistoricalAdapter(tushare)
        concept_mapper = LocalConceptMapper()
        result = await run_momentum_live(
            realtime_client=tushare,
            historical_adapter=adapter,
            fundamentals_db=fundamentals_db,
            concept_mapper=concept_mapper,
            universe=universe,
            backtest_cache=backtest_cache,
            stock_filter=stock_filter,
        )
    finally:
        await tushare.stop()

    if not result.recommended and not result.all_scored:
        return [], None
    recs = _scan_result_to_recs(date_str, result, n=10)
    return recs, result


def _calc_net_return_pct(buy_price: float, sell_price: float) -> float:
    """Calculate net return percentage after transaction costs (assuming 1 lot = 100 shares)."""
    shares = 100
    buy_amount = shares * buy_price
    buy_commission = max(buy_amount * 0.003, 5.0)
    buy_transfer = buy_amount * 0.00001
    total_buy_cost = buy_amount + buy_commission + buy_transfer

    sell_amount = shares * sell_price
    sell_commission = max(sell_amount * 0.003, 5.0)
    sell_transfer = sell_amount * 0.00001
    sell_stamp = sell_amount * 0.0005
    net_sell = sell_amount - sell_commission - sell_transfer - sell_stamp

    return (net_sell - total_buy_cost) / total_buy_cost * 100 if total_buy_cost > 0 else 0.0


async def _get_trading_calendar(start_date, end_date) -> list:
    """Get trading days via Tushare trade_cal, fallback to weekdays."""
    from datetime import datetime as dt
    from datetime import timedelta

    try:
        from src.data.clients.tushare_realtime import get_tushare_trade_calendar

        sd = start_date.strftime("%Y-%m-%d")
        ed = end_date.strftime("%Y-%m-%d")
        date_strs = await get_tushare_trade_calendar(sd, ed)
        days = sorted(
            dt.strptime(d, "%Y-%m-%d").date()
            for d in date_strs
            if start_date <= dt.strptime(d, "%Y-%m-%d").date() < end_date
        )
        if days:
            logger.info(f"Tushare trading calendar: {len(days)} days in [{start_date}, {end_date})")
            return days
        logger.warning("Tushare trading calendar returned no dates in range")
    except Exception as e:
        logger.warning(f"Tushare trade_cal failed: {e}")

    # Fallback: weekdays
    logger.warning("Falling back to weekday generation for trading calendar")
    days = []
    current = start_date
    while current < end_date:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


async def _execute_monitor_scan(state: dict, backtest_cache: Any = None) -> dict | None:
    """Execute a single momentum scan using Tushare + MomentumScanner.

    Returns result_entry dict on success, None on failure.
    """
    import time as time_module
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    from src.common.config import get_tushare_token
    from src.common.feishu_bot import FeishuBot
    from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter
    from src.data.clients.tushare_realtime import TushareRealtimeClient
    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.filters.stock_filter import create_main_board_only_filter
    from src.strategy.models import PriceSnapshot
    from src.strategy.strategies.momentum_scanner import MomentumScanner

    beijing_tz = ZoneInfo("Asia/Shanghai")
    logger.info("Monitor scan starting (tushare + MomentumScanner)")

    start_time = time_module.monotonic()

    # Quick trading day check (weekends + holidays)
    now_bj = datetime.now(beijing_tz)
    if now_bj.weekday() >= 5:
        day_name = "周六" if now_bj.weekday() == 5 else "周日"
        logger.warning(f"Monitor: 今天是{day_name}，A股不开市")
        raise RuntimeError(f"今天是{day_name}，A股不开市，无法扫描")
    try:
        cal = await _get_trading_calendar(now_bj.date(), now_bj.date() + timedelta(days=1))
        if not cal:
            logger.warning(f"Monitor: {now_bj.date()} is a holiday, A股不开市")
            raise RuntimeError(f"今天是节假日({now_bj.date()})，A股不开市，无法扫描")
    except RuntimeError:
        raise
    except Exception as e:
        logger.warning(f"Trade calendar check failed, proceeding: {e}")

    fundamentals_db = state.get("fundamentals_db")
    if not fundamentals_db:
        logger.error("Monitor: fundamentals DB not available")
        return None

    # Get universe
    stock_filter = create_main_board_only_filter()
    cache_ready = backtest_cache and getattr(backtest_cache, "is_ready", False)
    if cache_ready:
        all_codes = await backtest_cache.get_stock_codes()
        if all_codes:
            logger.info(f"Monitor: universe from GreptimeDB cache ({len(all_codes)} codes)")
        else:
            all_codes = await fundamentals_db.get_all_stock_codes()
            logger.info(f"Monitor: universe from PG ({len(all_codes)} codes)")
    else:
        all_codes = await fundamentals_db.get_all_stock_codes()
        logger.info(f"Monitor: universe from PG ({len(all_codes)} codes)")
    universe = [c for c in all_codes if stock_filter.is_allowed(c)]
    logger.info(f"Monitor: universe has {len(universe)} codes")

    tushare_token = get_tushare_token()
    tushare = TushareRealtimeClient(token=tushare_token)
    await tushare.start()
    try:
        quotes = await tushare.batch_get_quotes(universe)
        logger.info(f"Monitor: got {len(quotes)} quotes")

        # Supplement preClose from GreptimeDB cache
        if not (backtest_cache and getattr(backtest_cache, "is_ready", False)):
            raise RuntimeError("GreptimeDB 缓存未连接，无法进行扫描。请先下载回测数据。")

        today = datetime.now(beijing_tz).date()
        prev_daily: dict = {}
        for days_back in range(1, 8):
            prev_date = today - timedelta(days=days_back)
            prev_date_str = prev_date.strftime("%Y-%m-%d")
            prev_daily = await backtest_cache.get_all_codes_with_daily(prev_date_str)
            if prev_daily:
                logger.info(
                    f"Monitor: preClose from cache date {prev_date_str} ({len(prev_daily)} stocks)"
                )
                break

        price_snapshots: dict[str, PriceSnapshot] = {}
        skipped_no_prev = 0
        for code, q in quotes.items():
            if not q.is_trading:
                continue
            cached_day = prev_daily.get(code)
            prev_close = cached_day.close if cached_day else 0.0
            if prev_close <= 0:
                skipped_no_prev += 1
                continue
            price_snapshots[code] = PriceSnapshot(
                stock_code=code,
                stock_name="",
                open_price=q.open_price,
                prev_close=prev_close,
                latest_price=q.latest_price,
                early_volume=q.volume,
                high_price=q.high_price,
                low_price=q.low_price,
            )

        if skipped_no_prev:
            logger.warning(f"Monitor: skipped {skipped_no_prev} stocks (no preClose in cache)")

        if not price_snapshots:
            not_trading = sum(1 for q in quotes.values() if not q.is_trading)
            raise RuntimeError(
                f"盘中扫描数据异常：构建快照为空。"
                f"quotes={len(quotes)}, 停牌/无数据={not_trading}, "
                f"缺prev_close={skipped_no_prev}, prev_daily={len(prev_daily)}。"
                f"请检查Tushare数据和GreptimeDB缓存是否正常。"
            )

        adapter = IQuantHistoricalAdapter(tushare)
        concept_mapper = LocalConceptMapper()
        scanner = MomentumScanner(
            historical_adapter=adapter,
            fundamentals_db=fundamentals_db,
            concept_mapper=concept_mapper,
            stock_filter=stock_filter,
        )
        scan_result = await scanner.scan(price_snapshots)
    finally:
        await tushare.stop()

    elapsed = time_module.monotonic() - start_time
    scan_time = datetime.now(beijing_tz)

    rec = scan_result.recommended
    result_entry = {
        "scan_time": scan_time.strftime("%Y-%m-%d %H:%M"),
        "initial_gainers": scan_result.initial_gainers_count,
        "hot_boards": scan_result.hot_board_count,
        "l4_count": scan_result.l4_count,
        "l5_count": scan_result.l5_count,
        "l6_count": scan_result.l6_count,
        "final_candidates": scan_result.final_candidates,
        "elapsed_seconds": round(elapsed, 1),
        "scored_top5": [
            {
                "stock_code": s.stock_code,
                "stock_name": s.stock_name,
                "board_name": s.board_name,
                "v3_score": round(s.v3_score, 4),
                "gain_from_open_pct": round(s.gain_from_open_pct, 2),
            }
            for s in scan_result.all_scored[:5]
        ],
        "recommended_stock": {
            "stock_code": rec.stock_code,
            "stock_name": rec.stock_name,
            "board_name": rec.board_name,
            "v3_score": round(rec.v3_score, 4),
            "gain_from_open_pct": round(rec.gain_from_open_pct, 2),
            "turnover_amp": round(rec.turnover_amp, 2),
            "latest_price": round(rec.latest_price, 2),
        }
        if rec
        else None,
    }

    state["last_result"] = result_entry
    state["last_scan_time"] = scan_time.strftime("%Y-%m-%d %H:%M")
    state["today_results"].append(result_entry)

    # Send Feishu notification
    bot = FeishuBot()
    if bot.is_configured() and rec:
        from src.strategy.models import RecommendedStock, ScanResult

        adapted_rec = RecommendedStock(
            stock_code=rec.stock_code,
            stock_name=rec.stock_name,
            board_name=rec.board_name,
            board_stock_count=scan_result.final_candidates,
            open_gain_pct=round(
                (rec.open_price - rec.prev_close) / rec.prev_close * 100
                if rec.prev_close > 0
                else 0,
                2,
            ),
            pe_ttm=0.0,
            board_avg_pe=0.0,
            open_price=rec.open_price,
            prev_close=rec.prev_close,
            latest_price=rec.latest_price,
            gain_from_open_pct=rec.gain_from_open_pct,
            turnover_amp=rec.turnover_amp,
            composite_score=rec.v3_score,
        )
        adapted_result = ScanResult(
            initial_gainers=[],
            hot_boards={},
            recommended_stock=adapted_rec,
        )
        await bot.send_daily_pick_report(
            scan_result=adapted_result,
            elapsed_seconds=elapsed,
            scan_time=scan_time,
        )
        logger.info("Monitor: Feishu daily pick report sent")
    elif not bot.is_configured():
        logger.warning(
            "Monitor: Feishu bot NOT configured — set FEISHU_APP_ID, "
            "FEISHU_APP_SECRET, FEISHU_CHAT_ID environment variables"
        )

    logger.info(
        f"Monitor scan complete: {scan_result.final_candidates} candidates, {elapsed:.1f}s elapsed"
    )
    return result_entry


async def _run_intraday_monitor(state: dict) -> None:
    """Background task: intraday momentum monitor.

    Runs every trading day at ~9:40, executes momentum scan, sends Feishu notification.
    """
    import asyncio
    from datetime import datetime, time, timedelta
    from zoneinfo import ZoneInfo

    beijing_tz = ZoneInfo("Asia/Shanghai")
    SCAN_TIME = time(9, 39)

    state["running"] = True
    logger.info("Intraday momentum monitor started")

    try:
        while state["running"]:
            now = datetime.now(beijing_tz)
            current_time = now.time()

            # Check if daily scan is enabled
            from src.common.config import get_daily_scan_enabled

            if not get_daily_scan_enabled():
                state["scan_enabled"] = False
                await asyncio.sleep(30)
                continue
            state["scan_enabled"] = True

            # Skip non-trading days (weekends + holidays)
            if now.weekday() >= 5:
                await asyncio.sleep(3600)
                continue
            try:
                cal = await _get_trading_calendar(now.date(), now.date() + timedelta(days=1))
                if not cal:
                    logger.info(f"Monitor: {now.date()} is not a trading day, skipping")
                    await asyncio.sleep(3600)
                    continue
            except Exception as e:
                logger.warning(f"Trade calendar check failed, proceeding: {e}")

            # Before scan time — wait
            if current_time < SCAN_TIME:
                delta = datetime.combine(now.date(), SCAN_TIME) - datetime.combine(
                    now.date(), current_time
                )
                wait_secs = max(delta.total_seconds(), 10)
                logger.debug(f"Monitor waiting {wait_secs:.0f}s until {SCAN_TIME}")
                await asyncio.sleep(min(wait_secs, 60))
                continue

            # After scan window — wait for tomorrow
            if current_time > time(9, 50):
                tomorrow = now + timedelta(days=1)
                target = datetime.combine(tomorrow.date(), time(9, 25), tzinfo=beijing_tz)
                wait_secs = (target - now).total_seconds()
                logger.debug(f"Monitor done for today, sleeping {wait_secs:.0f}s")
                await asyncio.sleep(min(wait_secs, 3600))
                continue

            # 9:40-9:50 window — run scan once
            logger.info("Monitor: entering scan window")
            try:
                app_state = state.get("_app_state")
                backtest_cache = getattr(app_state, "backtest_cache", None) if app_state else None
                result = await _execute_monitor_scan(state, backtest_cache=backtest_cache)
                if result is None:
                    logger.warning("Monitor scan returned None (no result)")
                    try:
                        from src.common.feishu_bot import FeishuBot

                        bot = FeishuBot()
                        if bot.is_configured():
                            await bot.send_alert(
                                "盘中监控扫描无结果",
                                "扫描完成但未产生结果（数据源不可用或无合格股票）",
                            )
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"Monitor scan error: {e}", exc_info=True)
                try:
                    from src.common.feishu_bot import FeishuBot

                    bot = FeishuBot()
                    if bot.is_configured():
                        await bot.send_alert(
                            "盘中监控扫描失败",
                            f"{type(e).__name__}: {e}",
                        )
                except Exception:
                    pass

            # After scan, wait until next day (keep today_results for status endpoint)
            tomorrow = now + timedelta(days=1)
            target = datetime.combine(tomorrow.date(), time(9, 25), tzinfo=beijing_tz)
            wait_secs = (target - datetime.now(beijing_tz)).total_seconds()
            await asyncio.sleep(min(max(wait_secs, 10), 3600 * 18))
            state["today_results"] = []

    except asyncio.CancelledError:
        logger.info("Intraday momentum monitor cancelled")
    except Exception as e:
        logger.error(f"Intraday momentum monitor error: {e}", exc_info=True)
    finally:
        state["running"] = False
        state["task"] = None
        logger.info("Intraday momentum monitor stopped")


# === SAFETY AUDIT ENDPOINT ===


# === SETTINGS ENDPOINTS ===


def create_settings_router() -> APIRouter:
    """Create router for settings page (API key management)."""
    router = APIRouter(tags=["settings"])

    @router.get("/settings", response_class=HTMLResponse)
    async def settings_page(request: Request):
        """Settings page."""
        templates = request.app.state.templates
        return templates.TemplateResponse("settings.html", {"request": request})

    # === TUSHARE TOKEN SETTINGS ===

    @router.get("/api/settings/tushare-token")
    async def get_tushare_token_status():
        """Get current Tushare Pro token status (masked)."""
        from src.common.config import get_tushare_token, get_tushare_token_source

        source = get_tushare_token_source()
        source_labels = {
            "web_ui": "Web UI (当前会话)",
            "persisted_file": "Web UI (已持久化)",
            "env_var": "环境变量",
            "secrets_yaml": "secrets.yaml",
            "not_configured": "未配置",
        }

        try:
            token = get_tushare_token()
            if len(token) > 20:
                masked = token[:8] + "..." + token[-8:]
            else:
                masked = "***"
            return {
                "configured": True,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_token": masked,
                "token_length": len(token),
            }
        except ValueError:
            return {
                "configured": False,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_token": "",
                "token_length": 0,
            }

    @router.post("/api/settings/tushare-token")
    async def update_tushare_token(body: TokenUpdateRequest):
        """Save a new Tushare Pro token."""
        from src.common.config import set_tushare_token

        token = body.token.strip()
        if not token:
            raise HTTPException(status_code=400, detail="Token 不能为空")

        set_tushare_token(token)
        return {"success": True, "message": "Token 已保存，新的 API 调用将使用此 token"}

    @router.post("/api/settings/tushare-token/test")
    async def test_tushare_token(body: TokenUpdateRequest):
        """Test a Tushare Pro token by calling a simple API."""
        import httpx

        token = body.token.strip()
        if not token:
            raise HTTPException(status_code=400, detail="Token 不能为空")

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    "http://api.tushare.pro",
                    json={
                        "api_name": "rt_min",
                        "token": token,
                        "params": {"ts_code": "000001.SZ", "freq": "1MIN"},
                        "fields": "ts_code,trade_time,close",
                    },
                )
                response.raise_for_status()
                data = response.json()

                code = data.get("code", -1)
                if code == 0:
                    items = data.get("data", {}).get("items", [])
                    return {
                        "success": True,
                        "message": f"Token 验证成功 (rt_min)，获取到 {len(items)} 条分钟数据",
                    }
                else:
                    msg = data.get("msg", "未知错误")
                    return {
                        "success": False,
                        "message": f"Token 验证失败: {msg} (错误码: {code})",
                    }
        except httpx.TimeoutException:
            return {"success": False, "message": "请求超时，请检查网络连接"}
        except httpx.HTTPError as e:
            return {"success": False, "message": f"HTTP 请求失败: {e}"}

    # === IQUANT API KEY SETTINGS ===

    @router.get("/api/settings/iquant-key")
    async def get_iquant_key_status():
        """Get current iQuant API key status (masked)."""
        from src.common.config import get_iquant_api_key, get_iquant_key_source

        source = get_iquant_key_source()
        source_labels = {
            "web_ui": "Web UI (当前会话)",
            "persisted_file": "Web UI (已持久化)",
            "env_var": "环境变量",
            "not_configured": "未配置",
        }

        try:
            key = get_iquant_api_key()
            if len(key) > 16:
                masked = key[:4] + "..." + key[-4:]
            else:
                masked = "***"
            return {
                "configured": True,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_key": masked,
                "key_length": len(key),
            }
        except ValueError:
            return {
                "configured": False,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_key": "",
                "key_length": 0,
            }

    @router.post("/api/settings/iquant-key")
    async def update_iquant_key(body: TokenUpdateRequest):
        """Save a new iQuant API key."""
        from src.common.config import set_iquant_api_key

        key = body.token.strip()
        if not key:
            raise HTTPException(status_code=400, detail="API Key 不能为空")

        set_iquant_api_key(key)
        return {"success": True, "message": "iQuant API Key 已保存"}

    # === TSANGHI TOKEN SETTINGS ===

    @router.get("/api/settings/tsanghi-token")
    async def get_tsanghi_token_status():
        """Get current Tsanghi token status (masked)."""
        from src.common.config import get_tsanghi_token, get_tsanghi_token_source

        source = get_tsanghi_token_source()
        source_labels = {
            "web_ui": "Web UI (当前会话)",
            "persisted_file": "Web UI (已持久化)",
            "env_var": "环境变量",
            "secrets_yaml": "secrets.yaml",
            "not_configured": "未配置",
        }

        try:
            token = get_tsanghi_token()
            if len(token) > 20:
                masked = token[:8] + "..." + token[-8:]
            else:
                masked = "***"
            return {
                "configured": True,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_token": masked,
                "token_length": len(token),
            }
        except ValueError:
            return {
                "configured": False,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_token": "",
                "token_length": 0,
            }

    @router.post("/api/settings/tsanghi-token")
    async def update_tsanghi_token(body: TokenUpdateRequest):
        """Save a new Tsanghi token."""
        from src.common.config import set_tsanghi_token

        token = body.token.strip()
        if not token:
            raise HTTPException(status_code=400, detail="Token 不能为空")

        set_tsanghi_token(token)
        return {"success": True, "message": "Token 已保存，回测数据将使用此 token"}

    @router.post("/api/settings/tsanghi-token/test")
    async def test_tsanghi_token(body: TokenUpdateRequest):
        """Test a Tsanghi token by fetching one stock's daily data."""
        import httpx

        token = body.token.strip()
        if not token:
            raise HTTPException(status_code=400, detail="Token 不能为空")

        try:
            url = "https://tsanghi.com/api/fin/stock/XSHG/daily"
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    url,
                    params={"token": token, "ticker": "600519", "order": 2},
                )
                resp.raise_for_status()
                data = resp.json()

                code = data.get("code")
                if code == 200 and data.get("data"):
                    count = len(data["data"])
                    return {
                        "success": True,
                        "message": f"Token 验证成功，获取到 {count} 条日线数据",
                    }
                else:
                    msg = data.get("msg", "未知错误")
                    return {
                        "success": False,
                        "message": f"Token 验证失败: {msg} (code={code})",
                    }
        except httpx.TimeoutException:
            return {"success": False, "message": "请求超时，请检查网络连接"}
        except httpx.HTTPError as e:
            return {"success": False, "message": f"HTTP 请求失败: {e}"}

    # === CACHE SCHEDULER TOGGLE ===

    @router.get("/api/settings/cache-scheduler")
    async def get_cache_scheduler_status():
        from src.common.config import get_cache_scheduler_enabled

        return {"enabled": get_cache_scheduler_enabled()}

    @router.post("/api/settings/cache-scheduler")
    async def set_cache_scheduler_status(request: Request):
        from src.common.config import set_cache_scheduler_enabled

        body = await request.json()
        enabled = bool(body.get("enabled", True))
        set_cache_scheduler_enabled(enabled)
        return {
            "success": True,
            "enabled": enabled,
            "message": f"缓存定时更新已{'开启' if enabled else '关闭'}",
        }

    # === DAILY SCAN TOGGLE ===

    @router.get("/api/settings/daily-scan")
    async def get_daily_scan_status():
        from src.common.config import get_daily_scan_enabled

        return {"enabled": get_daily_scan_enabled()}

    @router.post("/api/settings/daily-scan")
    async def set_daily_scan_status(request: Request):
        from src.common.config import set_daily_scan_enabled

        body = await request.json()
        enabled = bool(body.get("enabled", True))
        set_daily_scan_enabled(enabled)
        return {
            "success": True,
            "enabled": enabled,
            "message": f"每日扫描已{'开启' if enabled else '关闭'}",
        }

    # === RECOMMENDATIONS TOGGLE ===

    @router.get("/api/settings/recommendations")
    async def get_recommendations_status():
        from src.common.config import get_recommendations_enabled

        return {"enabled": get_recommendations_enabled()}

    @router.post("/api/settings/recommendations")
    async def set_recommendations_status(request: Request):
        from src.common.config import set_recommendations_enabled

        body = await request.json()
        enabled = bool(body.get("enabled", True))
        set_recommendations_enabled(enabled)
        return {
            "success": True,
            "enabled": enabled,
            "message": f"今日推荐已{'开启' if enabled else '关闭'}",
        }

    # === FC TRAINING ENDPOINT URL ===

    @router.get("/api/settings/fc-url")
    async def get_fc_url_status():
        from src.common.config import get_fc_url

        url = get_fc_url()
        if url:
            # Mask middle of URL for display
            masked = url[:20] + "..." + url[-10:] if len(url) > 35 else url
            return {
                "configured": True,
                "masked_token": masked,
                "token_length": len(url),
                "source_label": "文件配置",
            }
        return {"configured": False}

    @router.post("/api/settings/fc-url")
    async def update_fc_url(request: Request):
        from src.common.config import set_fc_url

        body = await request.json()
        url = (body.get("token") or "").strip()
        if not url:
            raise HTTPException(status_code=400, detail="URL 不能为空")

        set_fc_url(url)
        return {"success": True, "message": "FC 训练端点已保存"}

    @router.post("/api/settings/fc-url/test")
    async def test_fc_url(request: Request):
        import httpx

        body = await request.json()
        url = (body.get("token") or "").strip()
        if not url:
            raise HTTPException(status_code=400, detail="URL 不能为空")

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url)
                data = resp.json()
                if data.get("status") == "ok":
                    return {"success": True, "message": "FC 端点连接成功"}
                return {"success": False, "message": f"意外响应: {data}"}
        except httpx.TimeoutException:
            return {"success": False, "message": "连接超时"}
        except Exception as e:
            return {"success": False, "message": f"连接失败: {e}"}

    # === ALL KEYS STATUS ===

    @router.get("/api/settings/keys-status")
    async def get_all_keys_status():
        """Get status of all API keys needed for live trading."""
        from src.common.config import (
            get_iquant_key_source,
            get_tsanghi_token_source,
        )

        iquant_ok = get_iquant_key_source() != "not_configured"
        tsanghi_ok = get_tsanghi_token_source() != "not_configured"

        return {
            "iquant": {"configured": iquant_ok, "source": get_iquant_key_source()},
            "tsanghi": {"configured": tsanghi_ok, "source": get_tsanghi_token_source()},
        }

    return router


# ---------------------------------------------------------------------------
# Trade Backtest Router — CSV upload → aggregate stats + equity curve
# ---------------------------------------------------------------------------


def create_trade_backtest_router() -> APIRouter:
    """Router for CSV trade record analysis."""
    import csv
    import io
    import statistics

    router = APIRouter(tags=["trade-backtest"])

    @router.post("/api/trade-backtest/analyze")
    async def analyze_trades(request: Request):
        import traceback as _tb

        try:
            return await _analyze_trades_impl(request)
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"trade-backtest analyze error: {exc}\n{_tb.format_exc()}")
            raise HTTPException(500, f"分析失败: {exc}")

    async def _analyze_trades_impl(request: Request) -> dict[str, Any]:
        content = await request.body()

        text = None
        for encoding in ("utf-8-sig", "gbk", "gb2312"):
            try:
                text = content.decode(encoding)
                break
            except (UnicodeDecodeError, LookupError):
                continue
        if text is None:
            raise HTTPException(400, "无法解码 CSV 文件，请使用 UTF-8 或 GBK 编码")

        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            raise HTTPException(400, "CSV 文件为空或格式错误")

        cols = set(reader.fieldnames)
        required = {"股票代码", "买入时间", "卖出时间"}
        missing = required - cols
        if missing:
            raise HTTPException(400, f"CSV 缺少必要列: {', '.join(missing)}")

        rows_raw: list[dict[str, str]] = []
        for row in reader:
            code = row["股票代码"].strip()
            if not code:
                continue
            rows_raw.append(row)

        if not rows_raw:
            raise HTTPException(400, "CSV 中没有交易记录")

        backtest_cache = getattr(request.app.state, "backtest_cache", None)
        if backtest_cache is None or not backtest_cache.is_ready:
            raise HTTPException(503, "GreptimeDB 缓存未连接，请稍后再试")

        all_dates: list[str] = []
        for row in rows_raw:
            all_dates.append(row["买入时间"].strip()[:10])
            all_dates.append(row["卖出时间"].strip()[:10])
        all_dates.sort()
        from datetime import date

        csv_start = date.fromisoformat(all_dates[0])
        csv_end = date.fromisoformat(all_dates[-1])

        if not await backtest_cache.covers_range(csv_start, csv_end):
            db_start, db_end = await backtest_cache.get_date_range()
            cache_start = str(db_start) if db_start else "无"
            cache_end = str(db_end) if db_end else "无"
            gaps = await backtest_cache.missing_ranges(csv_start, csv_end)
            gap_strs = [f"{s}~{e}" for s, e in gaps] if gaps else [f"{csv_start}~{csv_end}"]
            return {
                "needs_cache_update": True,
                "csv_start": str(csv_start),
                "csv_end": str(csv_end),
                "cache_start": cache_start,
                "cache_end": cache_end,
                "missing_ranges": gap_strs,
                "message": (
                    f"CSV 交易区间 {csv_start}~{csv_end}，"
                    f"缓存覆盖 {cache_start}~{cache_end}，"
                    f"需要增量下载: {', '.join(gap_strs)}"
                ),
            }

        import math

        initial_capital = 100000.0
        cap_param = request.query_params.get("initial_capital")
        if cap_param:
            try:
                initial_capital = float(cap_param)
            except ValueError:
                raise HTTPException(400, f"initial_capital 参数无效: {cap_param}")
            if initial_capital <= 0:
                raise HTTPException(400, "initial_capital 必须为正数")

        capital = initial_capital
        trades: list[dict[str, Any]] = []
        returns: list[float] = []
        buy_times: list[str] = []
        sell_times: list[str] = []
        hold_days_list: list[float] = []
        total_commission = 0.0
        total_stamp_tax = 0.0
        capital_history: list[float] = [capital]

        for i, row in enumerate(rows_raw, 1):
            try:
                code = row["股票代码"].strip()
                buy_time = row["买入时间"].strip()
                sell_time = row["卖出时间"].strip()
                buy_date = buy_time[:10]
                sell_date = sell_time[:10]

                buy_day = await backtest_cache.get_daily(code, buy_date)
                sell_day = await backtest_cache.get_daily(code, sell_date)
                if not buy_day or buy_day.open is None:
                    raise HTTPException(
                        400,
                        f"第 {i} 行: 缓存中无 {code} 在 {buy_date} 的价格数据",
                    )
                if not sell_day or sell_day.close is None:
                    raise HTTPException(
                        400,
                        f"第 {i} 行: 缓存中无 {code} 在 {sell_date} 的价格数据",
                    )
                buy_price = float(buy_day.open)
                sell_price = float(sell_day.close)
                if buy_price <= 0:
                    raise HTTPException(
                        400,
                        f"第 {i} 行: {code} 在 {buy_date} 的开盘价为 0",
                    )

                lots = math.floor(capital / (buy_price * 100))
                if lots <= 0:
                    trades.append(
                        {
                            "idx": i,
                            "stock_code": code,
                            "stock_name": row.get("股票名称", "").strip(),
                            "board": row.get("所属板块", "").strip(),
                            "buy_time": buy_time,
                            "buy_price": buy_price,
                            "sell_time": sell_time,
                            "sell_price": sell_price,
                            "hold_days": None,
                            "return_pct": 0.0,
                            "sell_reason": "资金不足",
                            "lots": 0,
                        }
                    )
                    returns.append(0.0)
                    buy_times.append(buy_time)
                    sell_times.append(sell_time)
                    capital_history.append(capital)
                    continue

                buy_amount = lots * 100 * buy_price
                buy_comm = max(buy_amount * 0.003, 5.0)
                buy_transfer = buy_amount * 0.00001
                total_buy_cost = buy_amount + buy_comm + buy_transfer

                while total_buy_cost > capital and lots > 0:
                    lots -= 1
                    buy_amount = lots * 100 * buy_price
                    buy_comm = max(buy_amount * 0.003, 5.0)
                    buy_transfer = buy_amount * 0.00001
                    total_buy_cost = buy_amount + buy_comm + buy_transfer

                if lots <= 0:
                    trades.append(
                        {
                            "idx": i,
                            "stock_code": code,
                            "stock_name": row.get("股票名称", "").strip(),
                            "board": row.get("所属板块", "").strip(),
                            "buy_time": buy_time,
                            "buy_price": buy_price,
                            "sell_time": sell_time,
                            "sell_price": sell_price,
                            "hold_days": None,
                            "return_pct": 0.0,
                            "sell_reason": "资金不足（含手续费）",
                            "lots": 0,
                        }
                    )
                    returns.append(0.0)
                    buy_times.append(buy_time)
                    sell_times.append(sell_time)
                    capital_history.append(capital)
                    continue

                sell_amount = lots * 100 * sell_price
                sell_comm = max(sell_amount * 0.003, 5.0)
                sell_transfer = sell_amount * 0.00001
                sell_stamp = sell_amount * 0.0005
                net_sell = sell_amount - sell_comm - sell_transfer - sell_stamp

                trade_profit = net_sell - total_buy_cost
                ret = round(trade_profit / total_buy_cost * 100, 2)
                capital = capital - total_buy_cost + net_sell
                total_commission += buy_comm + sell_comm
                total_stamp_tax += sell_stamp

                hd = int(row["持有天数"]) if row.get("持有天数") else None
                trades.append(
                    {
                        "idx": i,
                        "stock_code": code,
                        "stock_name": row.get("股票名称", "").strip(),
                        "board": row.get("所属板块", "").strip(),
                        "buy_time": buy_time,
                        "buy_price": buy_price,
                        "sell_time": sell_time,
                        "sell_price": sell_price,
                        "hold_days": hd,
                        "return_pct": ret,
                        "sell_reason": row.get("卖出原因", "").strip(),
                        "lots": lots,
                    }
                )
                returns.append(ret)
                buy_times.append(buy_time)
                sell_times.append(sell_time)
                capital_history.append(capital)
                if hd is not None:
                    hold_days_list.append(float(hd))
            except HTTPException:
                raise
            except (ValueError, KeyError) as exc:
                raise HTTPException(400, f"第 {i} 行数据格式错误: {exc}")

        if not trades:
            raise HTTPException(400, "CSV 中没有交易记录")

        total = len(trades)
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r < 0]

        equity = [c / initial_capital for c in capital_history]
        cumulative_return_pct = (capital - initial_capital) / initial_capital * 100

        peak = equity[0]
        max_dd = 0.0
        for eq in equity:
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak * 100
            if dd > max_dd:
                max_dd = dd

        max_cw = max_cl = 0
        streak = 0
        for r in returns:
            if r > 0:
                streak = streak + 1 if streak > 0 else 1
                max_cw = max(max_cw, streak)
            elif r < 0:
                streak = streak - 1 if streak < 0 else -1
                max_cl = max(max_cl, abs(streak))
            else:
                streak = 0

        profit_amounts = [r for r in returns if r > 0]
        loss_amounts = [abs(r) for r in returns if r < 0]
        sum_w = sum(profit_amounts) if profit_amounts else 0.0
        sum_l = sum(loss_amounts) if loss_amounts else 0.0
        profit_factor = round(sum_w / sum_l, 2) if sum_l > 0 else float("inf")

        avg_hold = round(statistics.mean(hold_days_list), 1) if hold_days_list else None

        summary = {
            "total_trades": total,
            "win_count": len(wins),
            "lose_count": len(losses),
            "flat_count": total - len(wins) - len(losses),
            "win_rate": round(len(wins) / total * 100, 2),
            "avg_return_pct": round(statistics.mean(returns), 2),
            "median_return_pct": round(statistics.median(returns), 2),
            "cumulative_return_pct": round(cumulative_return_pct, 2),
            "max_single_win_pct": round(max(returns), 2) if returns else 0,
            "max_single_loss_pct": round(min(returns), 2) if returns else 0,
            "max_consecutive_wins": max_cw,
            "max_consecutive_losses": max_cl,
            "max_drawdown_pct": round(max_dd, 2),
            "profit_factor": profit_factor,
            "avg_holding_days": avg_hold,
            "date_range": f"{buy_times[0][:10]} ~ {sell_times[-1][:10]}",
            "initial_capital": initial_capital,
            "final_capital": round(capital, 2),
            "total_commission": round(total_commission, 2),
            "total_stamp_tax": round(total_stamp_tax, 2),
        }

        monthly: dict[str, list[float]] = defaultdict(list)
        for idx, ret in enumerate(returns):
            month = buy_times[idx][:7]
            monthly[month].append(ret)

        monthly_returns = []
        for month in sorted(monthly.keys()):
            rets = monthly[month]
            compound = 1.0
            for r in rets:
                compound *= 1 + r / 100
            w = [r for r in rets if r > 0]
            monthly_returns.append(
                {
                    "month": month,
                    "trades": len(rets),
                    "return_pct": round((compound - 1) * 100, 2),
                    "win_rate": round(len(w) / len(rets) * 100, 1),
                }
            )

        equity_curve: list[dict[str, Any]] = [
            {"trade_idx": 0, "date": buy_times[0][:10], "equity": round(equity[0], 4)}
        ]
        for idx in range(total):
            equity_curve.append(
                {
                    "trade_idx": idx + 1,
                    "date": sell_times[idx][:10],
                    "equity": round(equity[idx + 1], 4),
                }
            )

        return {
            "success": True,
            "summary": summary,
            "monthly_returns": monthly_returns,
            "equity_curve": equity_curve,
            "trades": trades,
        }

    return router


def create_trading_router() -> APIRouter:
    """Router for the dashboard trading module (buy/sell + recommendations).

    API-only — the trading UI is embedded in the main dashboard (index.html).
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    router = APIRouter(tags=["trading"])
    BEIJING_TZ = ZoneInfo("Asia/Shanghai")

    @router.get("/api/trading/holdings")
    async def get_holdings(request: Request) -> dict:
        """Get current broker positions (synced from iQuant)."""
        iquant_rtr = getattr(request.app.state, "iquant_router", None)
        if not iquant_rtr or not hasattr(iquant_rtr, "_get_broker_positions"):
            return {"holdings": []}

        broker_pos = iquant_rtr._get_broker_positions()
        # Filter out sold positions (volume=0)
        active = [p for p in broker_pos if p.get("volume", 0) > 0]

        # Look up company names from fundamentals DB
        fundamentals_db = getattr(request.app.state, "fundamentals_db", None)
        name_map: dict[str, str] = {}
        if fundamentals_db and fundamentals_db.is_connected:
            codes = [p["code"] for p in active]
            if codes:
                fund_map = await fundamentals_db.batch_get_fundamentals(codes)
                name_map = {c: f.company_name for c, f in fund_map.items()}

        holdings = [
            {
                "code": pos["code"],
                "name": name_map.get(pos["code"], ""),
                "quantity": pos.get("volume", 0),
            }
            for pos in active
        ]
        return {"holdings": holdings}

    @router.get("/api/trading/recommendations")
    async def get_recommendations(request: Request, date: str | None = None) -> dict:
        """Get top-10 recommendations by computing momentum scan on-demand.

        Past dates use GreptimeDB cache (~1-3s).
        Today uses Tushare rt_min_daily (~30-60s, only after 09:39).
        No DB caching — strategy is actively being iterated.
        """
        from src.common.config import get_recommendations_enabled

        if date is None:
            date = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")

        if not get_recommendations_enabled():
            return {"date": date, "recommendations": [], "error": "推荐功能已关闭"}

        fundamentals_db = getattr(request.app.state, "fundamentals_db", None)
        backtest_cache = getattr(request.app.state, "backtest_cache", None)

        if not fundamentals_db:
            return {"date": date, "recommendations": [], "error": "基本面数据库未就绪"}

        try:
            recs, _scan_result = await _compute_momentum_scan(
                date_str=date,
                fundamentals_db=fundamentals_db,
                backtest_cache=backtest_cache,
            )
        except ValueError as e:
            return {"date": date, "recommendations": [], "error": str(e)}
        except Exception as e:
            from src.strategy.momentum_strategy_service import MinuteDataMissingError

            if isinstance(e, MinuteDataMissingError):
                return {"date": date, "recommendations": [], "error": str(e)}
            logger.error(f"On-demand momentum scan failed for {date}: {e}", exc_info=True)
            return {"date": date, "recommendations": [], "error": str(e)}

        return {"date": date, "recommendations": recs}

    @router.post("/api/trading/buy")
    async def submit_buy(request: Request) -> dict:
        """Push a BUY signal into iQuant pending queue."""
        body = await request.json()
        stock_code = body.get("stock_code", "")
        stock_name = body.get("stock_name", "")
        quantity = int(body.get("quantity", 0))
        price = body.get("price")

        if not stock_code or quantity <= 0 or quantity % 100 != 0:
            raise HTTPException(status_code=400, detail="stock_code 或 quantity 无效")

        iquant_rtr = getattr(request.app.state, "iquant_router", None)
        if not iquant_rtr or not hasattr(iquant_rtr, "_push_order"):
            raise HTTPException(status_code=503, detail="iQuant 路由未就绪")

        signal = {
            "type": "buy",
            "stock_code": stock_code,
            "stock_name": stock_name,
            "quantity": quantity,
            "price": float(price) if price else None,
            "price_type": "limit" if price else "market",
            "latest_price": float(price) if price else 0.0,
            "reason": "Dashboard手动买入",
            "manual": True,
        }

        pushed = iquant_rtr._push_order(signal)
        return {"success": True, "signal_id": pushed.get("id"), "quantity": quantity}

    @router.post("/api/trading/sell")
    async def submit_sell(request: Request) -> dict:
        """Push a SELL signal into iQuant pending queue."""
        body = await request.json()
        stock_code = body.get("stock_code", "")
        stock_name = body.get("stock_name", "")
        quantity = int(body.get("quantity", 0))

        if not stock_code or quantity <= 0 or quantity % 100 != 0:
            raise HTTPException(status_code=400, detail="stock_code 或 quantity 无效")

        iquant_rtr = getattr(request.app.state, "iquant_router", None)
        if not iquant_rtr or not hasattr(iquant_rtr, "_push_order"):
            raise HTTPException(status_code=503, detail="iQuant 路由未就绪")

        signal = {
            "type": "sell",
            "stock_code": stock_code,
            "stock_name": stock_name,
            "quantity": quantity,
            "price": None,
            "price_type": "market",
            "latest_price": 0.0,
            "reason": "Dashboard手动卖出",
            "manual": True,
        }

        pushed = iquant_rtr._push_order(signal)
        return {"success": True, "signal_id": pushed.get("id"), "quantity": quantity}

    @router.get("/api/trading/pending-signals")
    async def dashboard_pending_signals(request: Request) -> dict:
        """Return pending signals for dashboard display (no auth)."""
        iquant_rtr = getattr(request.app.state, "iquant_router", None)
        if not iquant_rtr or not hasattr(iquant_rtr, "_get_pending_signals"):
            return {"signals": []}
        return {"signals": iquant_rtr._get_pending_signals()}

    @router.post("/api/trading/cancel-signal")
    async def cancel_signal(request: Request) -> dict:
        """Cancel a pending signal by ID (dashboard, no auth)."""
        body = await request.json()
        signal_id = body.get("signal_id", "")
        if not signal_id:
            raise HTTPException(status_code=400, detail="signal_id required")

        iquant_rtr = getattr(request.app.state, "iquant_router", None)
        if not iquant_rtr or not hasattr(iquant_rtr, "_cancel_signal"):
            raise HTTPException(status_code=503, detail="iQuant 路由未就绪")

        removed = iquant_rtr._cancel_signal(signal_id)
        if not removed:
            raise HTTPException(status_code=404, detail=f"Signal {signal_id} not found")
        return {"success": True, "signal_id": signal_id}

    return router


def create_model_router() -> APIRouter:
    """Router for ML model management (training, fine-tuning, status)."""
    import asyncio
    import json

    router = APIRouter(tags=["models"])

    def _sse(data: dict) -> str:
        return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

    def _get_scheduler(request: Request):
        scheduler = getattr(request.app.state, "model_scheduler", None)
        if scheduler is None:
            raise HTTPException(status_code=503, detail="模型调度器未就绪")
        return scheduler

    @router.get("/api/model/status")
    async def get_model_status(request: Request) -> dict:
        """Get model scheduler status for dashboard polling."""
        scheduler = _get_scheduler(request)
        return scheduler.get_status()

    @router.get("/api/model/logs")
    async def get_model_logs(request: Request) -> dict:
        """Get recent training log lines."""
        scheduler = _get_scheduler(request)
        return {"logs": scheduler.training_log[-100:]}

    @router.post("/api/model/full-train")
    async def full_train(request: Request):
        """Start full model training with SSE progress stream."""
        scheduler = _get_scheduler(request)

        if scheduler.training_in_progress:
            return StreamingResponse(
                iter([_sse({"type": "error", "message": "训练正在进行中"})]),
                media_type="text/event-stream",
            )

        queue: asyncio.Queue[dict | None] = asyncio.Queue()

        async def _run():
            try:

                async def _progress(msg: str):
                    queue.put_nowait({"type": "progress", "message": msg})

                result = await scheduler.run_full_training(progress_cb=_progress)
                queue.put_nowait({"type": "result", "data": result})
            except Exception as e:
                queue.put_nowait({"type": "error", "message": str(e)[:200]})
            finally:
                queue.put_nowait(None)

        async def event_stream():
            task = asyncio.create_task(_run())
            request.app.state.model_train_task = task
            try:
                yield _sse({"type": "status", "message": "全量训练开始..."})
                while True:
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=30)
                    except asyncio.TimeoutError:
                        yield _sse({"type": "heartbeat"})
                        continue

                    if event is None:
                        break
                    if event.get("type") == "error":
                        yield _sse(event)
                        return
                    yield _sse(event)

                yield _sse({"type": "complete", "message": "全量训练完成"})
            except Exception as e:
                logger.error(f"Full train SSE error: {e}", exc_info=True)
                yield _sse({"type": "error", "message": str(e)[:200]})
            finally:
                request.app.state.model_train_task = None

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @router.post("/api/model/finetune")
    async def finetune(request: Request):
        """Start model fine-tuning with SSE progress stream."""
        scheduler = _get_scheduler(request)

        if scheduler.training_in_progress:
            return StreamingResponse(
                iter([_sse({"type": "error", "message": "训练正在进行中"})]),
                media_type="text/event-stream",
            )

        from src.data.services.model_training_scheduler import FULL_MODEL_NAME, MODEL_DIR

        full_path = MODEL_DIR / f"{FULL_MODEL_NAME}.lgb"
        if not full_path.exists():
            return StreamingResponse(
                iter([_sse({"type": "error", "message": "无全量训练模型，请先执行全量训练"})]),
                media_type="text/event-stream",
            )

        queue: asyncio.Queue[dict | None] = asyncio.Queue()

        async def _run():
            try:

                async def _progress(msg: str):
                    queue.put_nowait({"type": "progress", "message": msg})

                result = await scheduler.run_finetune(progress_cb=_progress)
                queue.put_nowait({"type": "result", "data": result})
            except Exception as e:
                queue.put_nowait({"type": "error", "message": str(e)[:200]})
            finally:
                queue.put_nowait(None)

        async def event_stream():
            task = asyncio.create_task(_run())
            request.app.state.model_finetune_task = task
            try:
                yield _sse({"type": "status", "message": "微调开始..."})
                while True:
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=30)
                    except asyncio.TimeoutError:
                        yield _sse({"type": "heartbeat"})
                        continue

                    if event is None:
                        break
                    if event.get("type") == "error":
                        yield _sse(event)
                        return
                    yield _sse(event)

                yield _sse({"type": "complete", "message": "微调完成"})
            except Exception as e:
                logger.error(f"Finetune SSE error: {e}", exc_info=True)
                yield _sse({"type": "error", "message": str(e)[:200]})
            finally:
                request.app.state.model_finetune_task = None

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @router.get("/api/model/training-data")
    async def stream_training_data(request: Request, token: str):
        """Stream daily OHLCV as NDJSON for FC callback. Auth via one-time token."""
        scheduler = _get_scheduler(request)
        mode = scheduler.validate_and_consume_token(token)
        if mode is None:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        cache = getattr(request.app.state, "backtest_cache", None)
        if cache is None or not cache.is_ready:
            raise HTTPException(status_code=503, detail="Backtest cache not available")

        async def generate_ndjson():
            existing_dates = sorted(await cache._get_existing_daily_dates())
            if mode == "finetune":
                existing_dates = existing_dates[-120:]

            yield (
                json.dumps(
                    {
                        "__meta__": True,
                        "total_days": len(existing_dates),
                        "date_range": [str(existing_dates[0]), str(existing_dates[-1])],
                    }
                )
                + "\n"
            )

            for trade_date in existing_dates:
                date_str = (
                    trade_date.strftime("%Y-%m-%d")
                    if hasattr(trade_date, "strftime")
                    else str(trade_date)
                )
                try:
                    bar_map = await cache.get_all_codes_with_daily(date_str)
                except (asyncio.TimeoutError, TimeoutError):
                    logger.warning("Stream export timeout %s, skip", date_str)
                    continue
                except Exception:
                    logger.warning("Stream export error %s, skip", date_str, exc_info=True)
                    continue

                if not bar_map:
                    continue

                day_dict: dict[str, dict] = {}
                for code, bar in bar_map.items():
                    day_dict[code] = {
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                        "amount": bar.amount,
                        "is_suspended": bar.is_suspended,
                    }
                yield json.dumps({"date": date_str, "stocks": day_dict}) + "\n"

        return StreamingResponse(
            generate_ndjson(),
            media_type="application/x-ndjson",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return router
