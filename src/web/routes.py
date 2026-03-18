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
# GET  /api/strategy/state   - Get strategy state (JSON)
# POST /api/strategy/start   - Start strategy (JSON)
# POST /api/strategy/stop    - Stop strategy (JSON)
# GET  /api/positions        - Get current positions (JSON)
#
# === MOMENTUM BACKTEST ENDPOINTS ===
# POST /api/momentum/backtest          - Run single-day V15 scan (JSON)
# POST /api/momentum/combined-analysis - Run range backtest with SSE streaming
# GET  /api/momentum/monitor-status    - Get intraday monitor status (JSON)
# POST /api/momentum/tsanghi-prepare   - Pre-download tsanghi cache (SSE)
# GET  /api/momentum/tsanghi-cache-status - Cache status (JSON)

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from starlette.responses import StreamingResponse

from src.common.pending_store import PendingConfirmationStore

if TYPE_CHECKING:
    from src.common.strategy_controller import StrategyController
    from src.trading.position_manager import PositionManager

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

    def get_strategy_controller(request: Request) -> StrategyController | None:
        """Get strategy controller from app state."""
        return getattr(request.app.state, "strategy_controller", None)

    def get_position_manager(request: Request) -> PositionManager | None:
        """Get position manager from app state."""
        return getattr(request.app.state, "position_manager", None)

    # ==================== HTML Pages ====================

    @router.get("/", response_class=HTMLResponse)
    async def index_page(request: Request):
        """Main dashboard showing strategy status, positions, and pending confirmations."""
        store = get_store(request)
        pending = store.get_pending_list()
        templates = request.app.state.templates

        # Get strategy state
        controller = get_strategy_controller(request)
        if controller:
            strategy_state = controller.to_dict()
        else:
            strategy_state = {"state": "unknown", "is_running": False}

        # Get positions (always reload from database)
        manager = get_position_manager(request)
        if manager:
            if manager.has_repository:
                try:
                    await manager.load_from_db()
                except Exception as e:
                    logger.warning(f"Failed to reload positions from DB: {e}")
            positions = {
                "slots": manager.get_state().get("slots", []),
                "summary": manager.get_summary(),
            }
        else:
            positions = {"slots": [], "summary": {}}

        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "pending": pending,
                "count": len(pending),
                "strategy_state": strategy_state,
                "positions": positions,
            },
        )

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

    # ==================== Strategy Control API ====================

    @router.get("/api/strategy/state")
    async def api_strategy_state(request: Request) -> dict:
        """Get current strategy state."""
        controller = get_strategy_controller(request)
        if not controller:
            return {
                "state": "unavailable",
                "is_running": False,
                "message": "Strategy controller not initialized",
            }
        return controller.to_dict()

    @router.post("/api/strategy/start")
    async def api_strategy_start(request: Request) -> dict:
        """Start strategy execution."""
        controller = get_strategy_controller(request)
        if not controller:
            raise HTTPException(status_code=503, detail="Strategy controller not available")

        success = await controller.start()
        return {
            "success": success,
            "state": controller.state.value,
            "message": "Strategy started" if success else "Strategy already running",
        }

    @router.post("/api/strategy/stop")
    async def api_strategy_stop(request: Request) -> dict:
        """Stop strategy execution."""
        controller = get_strategy_controller(request)
        if not controller:
            raise HTTPException(status_code=503, detail="Strategy controller not available")

        success = await controller.stop()
        return {
            "success": success,
            "state": controller.state.value,
            "message": "Strategy stopped" if success else "Strategy already stopped",
        }

    # ==================== Position API ====================

    @router.get("/api/positions")
    async def api_positions(request: Request) -> dict:
        """Get all current positions (always reads from database)."""
        manager = get_position_manager(request)
        if not manager:
            return {"positions": [], "summary": {}, "message": "Position manager not initialized"}

        # Always reload from database for fresh data
        if manager.has_repository:
            try:
                await manager.load_from_db()
            except Exception as e:
                logger.warning(f"Failed to reload positions from DB: {e}")

        return {
            "positions": manager.get_state().get("slots", []),
            "summary": manager.get_summary(),
        }

    _register_safety_audit_endpoint(router)

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
    """Request body for single-day V15 scan."""

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
        """Return tsanghi cache state for frontend polling."""
        loading = getattr(request.app.state, "tsanghi_cache_loading", False)
        cache = getattr(request.app.state, "tsanghi_cache", None)
        if loading and cache is None:
            return {"status": "loading"}
        if cache is None:
            return {"status": "empty"}
        sample_dates: list[str] = []
        sample_code = ""
        for code, dates in cache._daily.items():
            if dates:
                sample_code = code
                sorted_keys = sorted(dates.keys())
                sample_dates = (
                    sorted_keys[:3] + sorted_keys[-3:] if len(sorted_keys) > 6 else sorted_keys
                )
                break

        minute_gaps = cache.find_minute_gaps()
        gap_ranges = [[str(s), str(e)] for s, e in minute_gaps]

        return {
            "status": "ready",
            "start_date": str(cache._start_date) if cache._start_date else None,
            "end_date": str(cache._end_date) if cache._end_date else None,
            "daily_stocks": len(cache._daily),
            "minute_stocks": len(cache._minute),
            "minute_gaps": gap_ranges,
            "has_gaps": len(gap_ranges) > 0,
            "debug_sample_code": sample_code,
            "debug_sample_dates": sample_dates,
        }

    @router.post("/api/momentum/tsanghi-prepare")
    async def tsanghi_prepare(request: Request, body: TsanghiPrepareRequest):
        """Pre-download tsanghi data as SSE stream (incremental)."""
        from src.data.clients.tsanghi_backtest_cache import (
            TsanghiBacktestCache,
            check_oss_available,
        )

        try:
            start_date = datetime.strptime(body.start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(body.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误")

        if body.force:
            request.app.state.tsanghi_cache = None
            existing = None
        else:
            existing = getattr(request.app.state, "tsanghi_cache", None)

        # 1) Try in-memory cache
        if existing and existing.covers_range(start_date, end_date):

            async def mem_cached_stream():
                msg = {
                    "type": "complete",
                    "daily_count": len(existing._daily),
                    "minute_count": len(existing._minute),
                    "cached": True,
                }
                yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"

            return StreamingResponse(mem_cached_stream(), media_type="text/event-stream")

        # 2) Try OSS cache (skipped when force=True)
        if not existing and not body.force:
            try:
                existing = await asyncio.wait_for(
                    asyncio.to_thread(TsanghiBacktestCache.load_from_oss), timeout=60
                )
            except asyncio.TimeoutError:
                logger.warning("load_from_oss timed out (60s), will re-download")
                existing = None
            except Exception:
                logger.warning("load_from_oss failed, will re-download", exc_info=True)
                existing = None
        if existing and existing.covers_range(start_date, end_date):
            request.app.state.tsanghi_cache = existing

            async def cached_stream():
                msg = {
                    "type": "complete",
                    "daily_count": len(existing._daily),
                    "minute_count": len(existing._minute),
                    "cached": True,
                }
                yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"

            return StreamingResponse(cached_stream(), media_type="text/event-stream")

        # 3) Download missing data
        async def download_stream():
            try:
                from src.data.clients.tsanghi_backtest_cache import TsanghiBacktestCache

                cache = existing or TsanghiBacktestCache()
                yield f"data: {json.dumps({'type': 'status', 'message': '开始下载...'}, ensure_ascii=False)}\n\n"

                progress_events: list[dict] = []

                def progress_cb(event: dict):
                    progress_events.append(event)

                await asyncio.to_thread(
                    cache.download_prices,
                    start_date,
                    end_date,
                    progress_callback=progress_cb,
                )

                for ev in progress_events:
                    yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"

                # Save to OSS if available
                if check_oss_available():
                    yield f"data: {json.dumps({'type': 'status', 'message': '保存到 OSS...'}, ensure_ascii=False)}\n\n"
                    await asyncio.to_thread(cache.save_to_oss)

                request.app.state.tsanghi_cache = cache
                request.app.state.tsanghi_cache_loading = False

                # Inject cache into iQuant router
                iquant_rtr = getattr(request.app.state, "iquant_router", None)
                if iquant_rtr and hasattr(iquant_rtr, "_inject_cache"):
                    iquant_rtr._inject_cache(cache)

                msg = {
                    "type": "complete",
                    "daily_count": len(cache._daily),
                    "minute_count": len(cache._minute),
                    "cached": False,
                }
                yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"

            except Exception as e:
                logger.error(f"Tsanghi download error: {e}", exc_info=True)
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)[:200]}, ensure_ascii=False)}\n\n"

        return StreamingResponse(
            download_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # === Single-day V15 scan ===

    @router.post("/api/momentum/backtest")
    async def run_backtest(request: Request, body: BacktestScanRequest) -> dict:
        """Run V15 strategy scan for a specific date using tsanghi cache."""
        from src.common.feishu_bot import FeishuBot
        from src.data.clients.tsanghi_backtest_cache import TsanghiHistoricalAdapter
        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.strategies.v15_scanner import V15Scanner

        try:
            trade_date = datetime.strptime(body.trade_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"日期格式错误: {body.trade_date}，请使用 YYYY-MM-DD",
            )

        fundamentals_db = _get_fundamentals_db(request)

        ak_cache = getattr(request.app.state, "tsanghi_cache", None)
        if not ak_cache or not ak_cache.is_ready:
            raise HTTPException(status_code=400, detail="请先预下载沧海数据")

        try:
            date_key = trade_date.strftime("%Y-%m-%d")
            try:
                price_snapshots = _build_snapshots_from_cache(ak_cache, date_key)
            except MinuteDataMissingError as e:
                return {
                    "success": False,
                    "trade_date": body.trade_date,
                    "error": str(e),
                }
            if not price_snapshots:
                return {
                    "success": False,
                    "trade_date": body.trade_date,
                    "error": f"沧海缓存中无 {date_key} 的日线数据",
                }

            adapter = TsanghiHistoricalAdapter(ak_cache)
            concept_mapper = LocalConceptMapper()
            scanner = V15Scanner(
                historical_adapter=adapter,
                fundamentals_db=fundamentals_db,
                concept_mapper=concept_mapper,
            )

            result = await scanner.scan(price_snapshots, trade_date=trade_date)

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

                    # Adapt V15ScoredStock to RecommendedStock for Feishu
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
            logger.error(f"V15 backtest error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"回测出错: {str(e)}")

    # === Range backtest with capital simulation ===

    @router.post("/api/momentum/combined-analysis")
    async def run_combined_analysis(request: Request, body: RangeBacktestRequest):
        """Run V15 range backtest with capital simulation (SSE streaming)."""
        import json
        import math
        from datetime import datetime

        from src.data.clients.tsanghi_backtest_cache import TsanghiHistoricalAdapter
        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.strategies.v15_scanner import V15Scanner

        try:
            start_date = datetime.strptime(body.start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(body.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD")

        if end_date <= start_date:
            raise HTTPException(status_code=400, detail="结束日期必须晚于起始日期")

        if body.initial_capital < 1000:
            raise HTTPException(status_code=400, detail="起始资金不能低于 1000 元")

        tsanghi_cache = getattr(request.app.state, "tsanghi_cache", None)
        if not tsanghi_cache or not tsanghi_cache.is_ready:
            raise HTTPException(status_code=400, detail="请先预下载沧海数据")

        fundamentals_db = _get_fundamentals_db(request)
        adapter = TsanghiHistoricalAdapter(tsanghi_cache)

        async def event_stream():
            def sse(data: dict) -> str:
                return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

            try:
                from datetime import timedelta

                trading_days_all = _get_trading_calendar(
                    start_date, end_date + timedelta(days=10)
                )
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
                            "message": f"已截断至前 250 个交易日 ({days_in_range[0]} ~ {days_in_range[-1]})",
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

                concept_mapper = LocalConceptMapper()
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
                        date_key = trade_date.strftime("%Y-%m-%d")
                        try:
                            price_snapshots = _build_snapshots_from_cache(
                                tsanghi_cache, date_key
                            )
                        except MinuteDataMissingError as e:
                            price_snapshots = {}
                            price_err = str(e)
                        else:
                            price_err = (
                                "" if price_snapshots else f"沧海缓存中无 {date_key} 的日线数据"
                            )

                        if not price_snapshots:
                            day_results.append(
                                {
                                    "trade_date": str(trade_date),
                                    "has_trade": False,
                                    "skip_reason": price_err or "无价格数据",
                                    "capital": round(capital, 2),
                                }
                            )
                            yield sse(
                                {
                                    "type": "day_skip",
                                    "trade_date": str(trade_date),
                                    "reason": price_err or "无价格数据",
                                }
                            )
                            await asyncio.sleep(0.05)
                            continue

                        scanner = V15Scanner(
                            historical_adapter=adapter,
                            fundamentals_db=fundamentals_db,
                            concept_mapper=concept_mapper,
                        )
                        scan_result = await scanner.scan(
                            price_snapshots, trade_date=trade_date
                        )

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
                            next_day_data = tsanghi_cache.get_daily(
                                rec.stock_code, next_date_key
                            )
                            sell_price_val = (
                                float(next_day_data["open"])
                                if next_day_data and next_day_data.get("open")
                                else 0.0
                            )

                            if buy_price > 0 and sell_price_val > 0:
                                lots = math.floor(capital / (buy_price * 100))
                                if lots > 0:
                                    buy_amount = lots * 100 * buy_price
                                    buy_commission = max(buy_amount * 0.003, 5.0)
                                    buy_transfer = buy_amount * 0.00001
                                    total_buy_cost = (
                                        buy_amount + buy_commission + buy_transfer
                                    )

                                    while total_buy_cost > capital and lots > 0:
                                        lots -= 1
                                        buy_amount = lots * 100 * buy_price
                                        buy_commission = max(buy_amount * 0.003, 5.0)
                                        buy_transfer = buy_amount * 0.00001
                                        total_buy_cost = (
                                            buy_amount + buy_commission + buy_transfer
                                        )

                                if lots > 0:
                                    sell_amount = lots * 100 * sell_price_val
                                    sell_commission = max(sell_amount * 0.003, 5.0)
                                    sell_transfer = sell_amount * 0.00001
                                    sell_stamp = sell_amount * 0.0005
                                    net_sell = (
                                        sell_amount
                                        - sell_commission
                                        - sell_transfer
                                        - sell_stamp
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

                    concept_mapper.clear_cache()

                # === Summary ===
                trade_results = [d for d in day_results if d.get("has_trade")]
                wins = [d for d in trade_results if d["profit"] > 0]
                losses = [d for d in trade_results if d["profit"] < 0]
                total_return_pct = (
                    (capital - body.initial_capital) / body.initial_capital * 100
                )

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
                    "max_win": round(
                        max((d["profit"] for d in trade_results), default=0), 2
                    ),
                    "max_loss": round(
                        min((d["profit"] for d in trade_results), default=0), 2
                    ),
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
        tsanghi_cache = getattr(request.app.state, "tsanghi_cache", None)
        try:
            result = await _execute_monitor_scan(monitor_state, tsanghi_cache=tsanghi_cache)
            if result is None:
                return {"success": False, "message": "扫描未产生结果"}
            return {"success": True, "result": result}
        except Exception as e:
            logger.error(f"Manual scan trigger error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"扫描失败: {str(e)}")

    return router


# ==================== Helper Functions ====================


class MinuteDataMissingError(Exception):
    """Raised when minute data coverage is insufficient for reliable backtest."""

    pass


def _build_snapshots_from_cache(tsanghi_cache, date_str: str) -> dict:
    """Build PriceSnapshot dict from TsanghiBacktestCache for a given date.

    Replaces the iwencai pre-filter + history_quotes + 9:40 fetch pipeline.
    Local filtering: open_gain_pct > -0.5% (same as iwencai query).

    Raises:
        MinuteDataMissingError: if minute data coverage < 50% of daily candidates.
    """
    from src.strategy.models import PriceSnapshot

    all_daily = tsanghi_cache.get_all_codes_with_daily(date_str)
    snapshots: dict[str, PriceSnapshot] = {}

    if not all_daily:
        for code, dates in tsanghi_cache._daily.items():
            if dates:
                sample_keys = sorted(dates.keys())[:5]
                logger.warning(
                    f"_build_snapshots_from_cache: no data for date_str='{date_str}', "
                    f"but stock {code} has {len(dates)} dates, "
                    f"sample keys: {sample_keys}"
                )
                break
        else:
            logger.warning(
                f"_build_snapshots_from_cache: no data for date_str='{date_str}', "
                f"_daily has {len(tsanghi_cache._daily)} stocks but all date dicts are empty"
            )
        return snapshots

    daily_candidates = 0
    minute_hits = 0

    for code, day in all_daily.items():
        open_price = day.get("open", 0)
        prev_close = day.get("preClose", 0)
        if prev_close <= 0 or open_price <= 0:
            continue

        open_gain = (open_price - prev_close) / prev_close * 100
        if open_gain < -0.5:
            continue

        daily_candidates += 1

        data_940 = tsanghi_cache.get_940_price(code, date_str)
        if not data_940:
            continue

        minute_hits += 1
        latest_price, cum_vol, max_high, min_low = data_940

        if latest_price <= 0:
            continue

        snapshots[code] = PriceSnapshot(
            stock_code=code,
            stock_name="",
            open_price=open_price,
            prev_close=prev_close,
            latest_price=latest_price,
            early_volume=cum_vol,
            high_price=max_high,
            low_price=min_low,
        )

    # Trading safety: halt if minute data is severely insufficient
    if daily_candidates > 0 and minute_hits < daily_candidates * 0.5:
        coverage_pct = round(minute_hits / daily_candidates * 100, 1)
        raise MinuteDataMissingError(
            f"{date_str} 分钟数据严重不足: 仅 {minute_hits}/{daily_candidates} 只股票有分钟数据 "
            f"(覆盖率 {coverage_pct}%)。请先补充下载分钟数据，否则回测结果不可靠。"
        )

    return snapshots


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


def _get_trading_calendar(start_date, end_date) -> list:
    """Get trading days via AKShare (tool_trade_date_hist_sina)."""
    from datetime import timedelta

    try:
        import akshare as ak

        df = ak.tool_trade_date_hist_sina()
        all_dates = set(df["trade_date"].dt.date)
        days = sorted(d for d in all_dates if start_date <= d < end_date)
        if days:
            logger.info(f"AKShare trading calendar: {len(days)} days in [{start_date}, {end_date})")
            return days
        logger.warning("AKShare trading calendar returned no dates in range")
    except Exception as e:
        logger.warning(f"AKShare trading calendar failed: {e}")

    # Fallback: weekdays
    logger.warning("Falling back to weekday generation for trading calendar")
    days = []
    current = start_date
    while current < end_date:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _get_llm_api_key() -> str:
    """Get Silicon Flow API key from env var (Docker) or secrets.yaml (local)."""
    import os

    api_key = os.environ.get("SILICONFLOW_API_KEY", "")
    if api_key:
        return api_key

    try:
        from src.common.config import load_secrets

        secrets = load_secrets()
        return secrets.get_str("siliconflow.api_key", "")
    except Exception:
        return ""


def _create_board_relevance_filter_global():
    """Module-level factory for BoardRelevanceFilter. Raises on failure."""
    from src.strategy.filters.board_relevance_filter import (
        create_board_relevance_filter,
    )

    return create_board_relevance_filter()


async def _execute_monitor_scan(state: dict, tsanghi_cache: Any = None) -> dict | None:
    """Execute a single momentum scan using Tushare + V15Scanner.

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
    from src.strategy.strategies.v15_scanner import V15Scanner

    beijing_tz = ZoneInfo("Asia/Shanghai")
    logger.info("Monitor scan starting (tushare + V15Scanner)")

    start_time = time_module.monotonic()

    # Quick weekday check
    now_bj = datetime.now(beijing_tz)
    if now_bj.weekday() >= 5:
        day_name = "周六" if now_bj.weekday() == 5 else "周日"
        logger.warning(f"Monitor: 今天是{day_name}，A股不开市")
        raise RuntimeError(f"今天是{day_name}，A股不开市，无法扫描")

    fundamentals_db = state.get("fundamentals_db")
    if not fundamentals_db:
        logger.error("Monitor: fundamentals DB not available")
        return None

    # Get universe
    stock_filter = create_main_board_only_filter()
    cache_ready = tsanghi_cache and getattr(tsanghi_cache, "is_ready", False)
    if cache_ready and tsanghi_cache.stock_codes:
        all_codes = tsanghi_cache.stock_codes
        logger.info(f"Monitor: universe from OSS cache ({len(all_codes)} codes)")
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

        # Supplement preClose from OSS cache
        if not (tsanghi_cache and getattr(tsanghi_cache, "is_ready", False)):
            raise RuntimeError("OSS 缓存未加载，无法进行扫描。请先在回测页面加载缓存数据。")

        today = datetime.now(beijing_tz).date()
        prev_daily: dict[str, dict[str, float]] = {}
        for days_back in range(1, 8):
            prev_date = today - timedelta(days=days_back)
            prev_date_str = prev_date.strftime("%Y-%m-%d")
            prev_daily = tsanghi_cache.get_all_codes_with_daily(prev_date_str)
            if prev_daily:
                logger.info(
                    f"Monitor: preClose from cache date {prev_date_str} "
                    f"({len(prev_daily)} stocks)"
                )
                break

        price_snapshots: dict[str, PriceSnapshot] = {}
        skipped_no_prev = 0
        for code, q in quotes.items():
            if not q.is_trading:
                continue
            cached_day = prev_daily.get(code)
            prev_close = cached_day["close"] if cached_day else 0.0
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
                f"请检查Tushare数据和OSS缓存是否正常。"
            )

        adapter = IQuantHistoricalAdapter(tushare, cache=tsanghi_cache)
        concept_mapper = LocalConceptMapper()
        scanner = V15Scanner(
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
        f"Monitor scan complete: {scan_result.final_candidates} candidates, "
        f"{elapsed:.1f}s elapsed"
    )
    return result_entry


async def _run_intraday_monitor(state: dict) -> None:
    """Background task: intraday momentum monitor.

    Runs every trading day at ~9:40, executes V15 scan, sends Feishu notification.
    """
    import asyncio
    from datetime import datetime, time, timedelta
    from zoneinfo import ZoneInfo

    beijing_tz = ZoneInfo("Asia/Shanghai")
    SCAN_TIME = time(9, 40)

    state["running"] = True
    logger.info("Intraday momentum monitor started")

    try:
        while state["running"]:
            now = datetime.now(beijing_tz)
            current_time = now.time()

            # Only run on weekdays
            if now.weekday() >= 5:
                await asyncio.sleep(3600)
                continue

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
                tsanghi_cache = getattr(app_state, "tsanghi_cache", None) if app_state else None
                result = await _execute_monitor_scan(state, tsanghi_cache=tsanghi_cache)
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

            # After scan, wait until next day
            tomorrow = now + timedelta(days=1)
            target = datetime.combine(tomorrow.date(), time(9, 25), tzinfo=beijing_tz)
            wait_secs = (target - datetime.now(beijing_tz)).total_seconds()
            state["today_results"] = []
            await asyncio.sleep(min(max(wait_secs, 10), 3600 * 18))

    except asyncio.CancelledError:
        logger.info("Intraday momentum monitor cancelled")
    except Exception as e:
        logger.error(f"Intraday momentum monitor error: {e}", exc_info=True)
    finally:
        state["running"] = False
        state["task"] = None
        logger.info("Intraday momentum monitor stopped")


# === SAFETY AUDIT ENDPOINT ===


def _register_safety_audit_endpoint(router: APIRouter) -> None:
    """Register the /api/safety-audit endpoint on the given router."""

    @router.get("/api/safety-audit")
    async def safety_audit(request: Request):
        """Return trading safety audit results (computed at startup)."""
        audit = getattr(request.app.state, "safety_audit", None)
        if audit is None:
            return {"critical_count": 0, "warning_count": 0, "violations": []}
        return audit


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

    # === ALIYUN DASHSCOPE API KEY ===

    @router.get("/api/settings/aliyun-key")
    async def get_aliyun_key_status():
        """Get current Aliyun DashScope API key status (masked)."""
        from src.common.config import get_aliyun_api_key, get_aliyun_api_key_source

        source = get_aliyun_api_key_source()
        source_labels = {
            "web_ui": "Web UI (当前会话)",
            "persisted_file": "Web UI (已持久化)",
            "env_var": "环境变量",
            "secrets_yaml": "secrets.yaml",
            "not_configured": "未配置",
        }

        try:
            key = get_aliyun_api_key()
            if len(key) > 20:
                masked = key[:8] + "..." + key[-8:]
            else:
                masked = "***"
            return {
                "configured": True,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_token": masked,
                "token_length": len(key),
            }
        except ValueError:
            return {
                "configured": False,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_token": "",
                "token_length": 0,
            }

    @router.post("/api/settings/aliyun-key")
    async def update_aliyun_key(body: TokenUpdateRequest):
        """Save a new Aliyun DashScope API key."""
        from src.common.config import set_aliyun_api_key

        key = body.token.strip()
        if not key:
            raise HTTPException(status_code=400, detail="API Key 不能为空")

        set_aliyun_api_key(key)
        return {
            "success": True,
            "message": "API Key 已保存，板块相关度过滤将使用此 key",
        }

    @router.post("/api/settings/aliyun-key/test")
    async def test_aliyun_key(body: TokenUpdateRequest):
        """Test an Aliyun DashScope API key with a simple chat call."""
        import httpx

        key = body.token.strip()
        if not key:
            raise HTTPException(status_code=400, detail="API Key 不能为空")

        try:
            base = "https://dashscope.aliyuncs.com/compatible-mode/v1"
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(
                    f"{base}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "qwen-plus",
                        "messages": [{"role": "user", "content": "回复OK"}],
                        "max_tokens": 10,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"]["content"]
                return {"success": True, "message": f"验证成功，模型回复: {content}"}
        except httpx.TimeoutException:
            return {"success": False, "message": "请求超时，请检查网络连接"}
        except httpx.HTTPStatusError as e:
            return {"success": False, "message": f"API 验证失败 (HTTP {e.response.status_code})"}
        except Exception as e:
            return {"success": False, "message": f"验证失败: {e}"}

    # === ALL KEYS STATUS ===

    @router.get("/api/settings/keys-status")
    async def get_all_keys_status():
        """Get status of all API keys needed for live trading."""
        from src.common.config import (
            get_aliyun_api_key_source,
            get_iquant_key_source,
            get_tsanghi_token_source,
            load_secrets,
        )

        iquant_ok = get_iquant_key_source() != "not_configured"
        tsanghi_ok = get_tsanghi_token_source() != "not_configured"
        aliyun_src = get_aliyun_api_key_source()
        aliyun_ok = aliyun_src != "not_configured"

        sf_ok = False
        try:
            secrets = load_secrets()
            sf_ok = bool(secrets.get_str("siliconflow.api_key", ""))
        except FileNotFoundError:
            pass

        return {
            "siliconflow": {"configured": sf_ok},
            "iquant": {"configured": iquant_ok, "source": get_iquant_key_source()},
            "tsanghi": {"configured": tsanghi_ok, "source": get_tsanghi_token_source()},
            "aliyun": {"configured": aliyun_ok, "source": aliyun_src},
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

        tsanghi_cache = getattr(request.app.state, "tsanghi_cache", None)
        if tsanghi_cache is None:
            raise HTTPException(503, "沧海缓存尚未加载完成，请稍后再试")

        all_dates: list[str] = []
        for row in rows_raw:
            all_dates.append(row["买入时间"].strip()[:10])
            all_dates.append(row["卖出时间"].strip()[:10])
        all_dates.sort()
        from datetime import date

        csv_start = date.fromisoformat(all_dates[0])
        csv_end = date.fromisoformat(all_dates[-1])

        if not tsanghi_cache.covers_range(csv_start, csv_end):
            cache_start = str(tsanghi_cache._start_date) if tsanghi_cache._start_date else "无"
            cache_end = str(tsanghi_cache._end_date) if tsanghi_cache._end_date else "无"
            gaps = tsanghi_cache.missing_ranges(csv_start, csv_end)
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
            initial_capital = float(cap_param)

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

                buy_day = tsanghi_cache.get_daily(code, buy_date)
                sell_day = tsanghi_cache.get_daily(code, sell_date)
                if not buy_day or buy_day.get("open") is None:
                    raise HTTPException(
                        400,
                        f"第 {i} 行: 缓存中无 {code} 在 {buy_date} 的价格数据",
                    )
                if not sell_day or sell_day.get("close") is None:
                    raise HTTPException(
                        400,
                        f"第 {i} 行: 缓存中无 {code} 在 {sell_date} 的价格数据",
                    )
                buy_price = float(buy_day["open"])
                sell_price = float(sell_day["close"])

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
