# === MODULE PURPOSE ===
# API routes and page handlers for trading confirmations.

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
# === SIMULATION ENDPOINTS ===
# POST /api/simulation/start   - Start new simulation
# GET  /api/simulation/state   - Get simulation state
# POST /api/simulation/advance - Advance to next phase
# POST /api/simulation/select  - Submit signal selection
# POST /api/simulation/sell    - Submit sell decision
# GET  /api/simulation/result  - Get final result
# DELETE /api/simulation       - Cancel simulation
#
# === ORDER ASSISTANT ENDPOINTS ===
# GET  /order-assistant                      - Order assistant page (HTML)
# GET  /api/order-assistant/state            - Current phase and time info (JSON)
# GET  /api/order-assistant/messages         - Messages with pagination (JSON)
# POST /api/order-assistant/feishu-notify    - Push new messages to Feishu (JSON)
#
# === MOMENTUM BACKTEST ENDPOINTS ===
# GET  /momentum              - Momentum backtest page (HTML)
# POST /api/momentum/backtest - Run single-day backtest (JSON)
# POST /api/momentum/range-backtest - Run range backtest with SSE streaming
# GET  /api/momentum/monitor-status - Get intraday monitor status (JSON)
# POST /api/momentum/loss-analysis  - Analyze losing trades with LLM (SSE streaming)
# POST /api/momentum/backfill         - Backfill scan stocks to DB (SSE streaming)
# GET  /api/momentum/scan-stocks/csv  - Export scan selected stocks as CSV download

from __future__ import annotations

import logging
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
    """Request body for updating iFinD token."""

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

    @router.get("/simulation", response_class=HTMLResponse)
    async def simulation_page(request: Request):
        """Historical simulation page."""
        from src.simulation import get_simulation_manager

        templates = request.app.state.templates
        manager = get_simulation_manager()
        state = manager.get_state()

        return templates.TemplateResponse(
            "simulation.html",
            {
                "request": request,
                "state": state.to_dict(),
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
    """
    Parse and validate user selection based on confirmation type.

    Args:
        confirm_type: Type of confirmation.
        selection: User's selection input.
        data: Confirmation data for validation.

    Returns:
        Parsed and validated selection.
    """
    if confirm_type == "premarket":
        # Premarket: list of indices, "all", or "skip"
        if selection == "all":
            return "all"
        if selection in ("skip", None, []):
            return []
        if isinstance(selection, list):
            # Validate indices
            max_idx = len(data.get("signals", []))
            return [i for i in selection if isinstance(i, int) and 1 <= i <= max_idx]
        return []

    elif confirm_type == "intraday":
        # Intraday: boolean (buy or skip)
        if isinstance(selection, bool):
            return selection
        if selection in ("yes", "y", "buy", True):
            return True
        return False

    elif confirm_type == "morning":
        # Morning: list of slot_ids, "all", or "hold"
        if selection == "all":
            return "all"
        if selection in ("hold", None, []):
            return []
        if isinstance(selection, list):
            return selection
        return []

    elif confirm_type == "limit_up":
        # Limit-up: list of indices, "all", or "skip"
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


# ==================== Simulation API Models ====================


class SimulationStartRequest(BaseModel):
    """Request body for starting a simulation."""

    start_date: str  # YYYY-MM-DD format
    num_days: int = 1
    initial_capital: float = 10_000_000.0
    load_holdings_from: str | None = None  # Optional date to load holdings from


class SimulationSelectRequest(BaseModel):
    """Request body for signal selection."""

    selected_indices: list[int]  # 1-based indices


class SimulationSellRequest(BaseModel):
    """Request body for sell decision."""

    slots_to_sell: list[int]  # Slot IDs to sell


def create_simulation_router() -> APIRouter:
    """Create router for simulation endpoints."""
    from datetime import datetime

    from src.simulation import (
        SimulationSettings,
        get_simulation_manager,
        reset_simulation_manager,
    )

    router = APIRouter(prefix="/api/simulation", tags=["simulation"])

    @router.post("/start")
    async def start_simulation(body: SimulationStartRequest) -> dict:
        """Start a new simulation."""
        manager = get_simulation_manager()

        # Cancel any existing simulation
        if manager.is_running:
            await manager.cleanup()

        # Parse start date
        try:
            start_date = datetime.strptime(body.start_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid date format: {body.start_date}. Use YYYY-MM-DD.",
            )

        # Parse load holdings date if specified
        load_holdings_date = None
        if body.load_holdings_from:
            try:
                load_holdings_date = datetime.strptime(body.load_holdings_from, "%Y-%m-%d").date()
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid load_holdings_from date: {body.load_holdings_from}",
                )

        # Create settings
        settings = SimulationSettings(
            start_date=start_date,
            num_days=body.num_days,
            initial_capital=body.initial_capital,
            load_holdings_from_date=load_holdings_date,
        )

        # Initialize simulation
        try:
            await manager.initialize(settings)
        except Exception as e:
            logger.error(f"Failed to start simulation: {e}")
            raise HTTPException(status_code=500, detail=str(e))

        return {
            "success": True,
            "message": f"Simulation started for {start_date}",
            "state": manager.get_state().to_dict(),
        }

    @router.get("/state")
    async def get_simulation_state() -> dict:
        """Get current simulation state."""
        manager = get_simulation_manager()
        state = manager.get_state()
        return state.to_dict()

    @router.post("/advance")
    async def advance_simulation() -> dict:
        """Advance simulation to next phase."""
        manager = get_simulation_manager()

        if not manager.is_initialized:
            raise HTTPException(
                status_code=400,
                detail="No simulation is running. Start one first.",
            )

        try:
            new_phase = await manager.advance_to_next_phase()
        except Exception as e:
            import traceback

            logger.error(f"Error advancing simulation: {e}")
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=500,
                detail=f"推进模拟时出错: {str(e)}",
            )

        return {
            "success": True,
            "phase": new_phase.value,
            "state": manager.get_state().to_dict(),
        }

    @router.post("/select")
    async def submit_selection(body: SimulationSelectRequest) -> dict:
        """Submit signal selection."""
        manager = get_simulation_manager()

        if not manager.is_initialized:
            raise HTTPException(
                status_code=400,
                detail="No simulation is running.",
            )

        await manager.process_selection(body.selected_indices)

        return {
            "success": True,
            "selected": body.selected_indices,
            "state": manager.get_state().to_dict(),
        }

    @router.post("/sell")
    async def submit_sell_decision(body: SimulationSellRequest) -> dict:
        """Submit sell decision for morning confirmation."""
        manager = get_simulation_manager()

        if not manager.is_initialized:
            raise HTTPException(
                status_code=400,
                detail="No simulation is running.",
            )

        await manager.process_sell_decision(body.slots_to_sell)

        return {
            "success": True,
            "sold_slots": body.slots_to_sell,
            "state": manager.get_state().to_dict(),
        }

    @router.post("/intraday/select")
    async def submit_intraday_selection(body: SimulationSelectRequest) -> dict:
        """Submit intraday signal selection."""
        manager = get_simulation_manager()

        if not manager.is_initialized:
            raise HTTPException(status_code=400, detail="No simulation is running.")

        await manager.process_intraday_selection(body.selected_indices)

        return {
            "success": True,
            "selected": body.selected_indices,
            "state": manager.get_state().to_dict(),
        }

    @router.post("/intraday/skip")
    async def skip_intraday() -> dict:
        """Skip intraday messages and continue."""
        manager = get_simulation_manager()

        if not manager.is_initialized:
            raise HTTPException(status_code=400, detail="No simulation is running.")

        await manager.skip_intraday()

        return {
            "success": True,
            "state": manager.get_state().to_dict(),
        }

    @router.post("/sync")
    async def sync_to_database(confirm: bool = False) -> dict:
        """
        Sync simulation results to trading database.

        Requires explicit confirmation to prevent accidental syncs.
        """
        if not confirm:
            raise HTTPException(
                status_code=400,
                detail="请确认同步操作 (设置 confirm=true)",
            )

        manager = get_simulation_manager()

        if not manager.is_initialized:
            raise HTTPException(status_code=400, detail="No simulation is running.")

        try:
            result = await manager.sync_to_database()
            return {
                "success": True,
                "message": "模拟结果已同步到数据库",
                **result,
            }
        except RuntimeError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @router.get("/result")
    async def get_simulation_result() -> dict:
        """Get final simulation result."""
        manager = get_simulation_manager()

        result = manager.get_result()
        if not result:
            raise HTTPException(
                status_code=400,
                detail="No simulation result available.",
            )

        return result.to_dict()

    @router.get("/messages")
    async def get_simulation_messages(
        only_positive: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> dict:
        """
        Get messages for the current simulation date with pagination.

        Returns messages with pagination metadata for "Load More" functionality.
        """
        manager = get_simulation_manager()

        if not manager.is_initialized:
            raise HTTPException(
                status_code=400,
                detail="Simulation not initialized.",
            )

        # Access internal components
        hist_reader = manager._hist_message_reader
        clock = manager._clock

        if not hist_reader or not clock:
            raise HTTPException(
                status_code=500,
                detail="Simulation components not available.",
            )

        # Get total count for pagination
        total_count = await hist_reader.count_premarket_messages(
            trade_date=clock.current_time,
            only_positive=only_positive,
        )

        # Get paginated premarket messages
        messages = await hist_reader.get_premarket_messages(
            trade_date=clock.current_time,
            only_positive=only_positive,
            limit=limit,
            offset=offset,
        )

        # Collect all unique stock codes to look up names
        all_stock_codes: set[str] = set()
        for msg in messages:
            all_stock_codes.update(msg.stock_codes or [])
            if msg.analysis and msg.analysis.affected_stocks:
                all_stock_codes.update(msg.analysis.affected_stocks)

        # Look up stock names using sector mapper
        stock_names: dict[str, str] = {}
        if all_stock_codes:
            stock_names = manager._get_stock_names_dict(list(all_stock_codes))

        # Convert to dict format
        result = []
        for msg in messages:
            msg_dict: dict[str, Any] = {
                "id": msg.id,
                "source_type": msg.source_type,
                "source_name": msg.source_name,
                "title": msg.title,
                "content": msg.content[:500] if msg.content else "",  # Truncate
                "publish_time": msg.publish_time.isoformat() if msg.publish_time else None,
                "stock_codes": msg.stock_codes,
                "url": msg.url,
            }

            # Add analysis if available
            if msg.analysis:
                msg_dict["analysis"] = {
                    "sentiment": msg.analysis.sentiment.value,
                    "confidence": msg.analysis.confidence,
                    "reasoning": msg.analysis.reasoning,
                    "affected_stocks": msg.analysis.affected_stocks,
                }
            else:
                msg_dict["analysis"] = None

            result.append(msg_dict)

        return {
            "success": True,
            "sim_date": clock.current_date.isoformat(),
            "count": len(result),
            "total_count": total_count,
            "offset": offset,
            "limit": limit,
            "has_more": offset + len(result) < total_count,
            "messages": result,
            "stock_names": stock_names,
        }

    @router.post("/messages/select")
    async def select_messages(request: Request) -> dict:
        """
        Set pending signals from selected messages.

        This endpoint allows users to select messages from the messages viewer.
        The selected messages become pending signals for confirmation on the main page.
        """
        manager = get_simulation_manager()

        if not manager.is_initialized:
            raise HTTPException(status_code=400, detail="No simulation is running.")

        data = await request.json()
        messages = data.get("messages", [])

        if not messages:
            raise HTTPException(status_code=400, detail="No messages selected.")

        try:
            result = manager.set_signals_from_messages(messages)
            return {
                "success": True,
                "count": result.get("count", 0),
                "message": f"已选择 {result.get('count', 0)} 条消息",
            }
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @router.delete("")
    async def cancel_simulation() -> dict:
        """Cancel current simulation."""
        manager = get_simulation_manager()

        if manager.is_running:
            await manager.cleanup()

        await reset_simulation_manager()

        return {
            "success": True,
            "message": "Simulation cancelled",
        }

    return router


# ==================== Order Assistant ====================


# Lazy singletons for MessageReader, SectorMapper, and FeishuBot used by order assistant
_oa_message_reader = None
_oa_sector_mapper = None
_oa_feishu_bot = None
# Track message IDs already sent to Feishu (dedup across refreshes/tabs)
_oa_feishu_sent_ids: set[str] = set()


async def _get_oa_reader():
    """Get or create the order assistant MessageReader (lazy singleton)."""
    global _oa_message_reader
    from src.data.readers.message_reader import create_message_reader_from_config

    if _oa_message_reader is None or not _oa_message_reader.is_connected:
        _oa_message_reader = create_message_reader_from_config()
        await _oa_message_reader.connect()
    return _oa_message_reader


async def _get_oa_sector_mapper():
    """Get or create the order assistant SectorMapper (lazy singleton)."""
    global _oa_sector_mapper
    from src.data.sources.sector_mapper import SectorMapper

    if _oa_sector_mapper is None:
        _oa_sector_mapper = SectorMapper()
    if not _oa_sector_mapper.is_loaded:
        await _oa_sector_mapper.load_sector_data()
    return _oa_sector_mapper


def _get_oa_stock_names(codes: list[str], mapper) -> dict[str, str]:
    """Get stock names for a list of codes using sector mapper."""
    result: dict[str, str] = {}
    for code in codes:
        clean_code = code.split(".")[0] if "." in code else code
        name = mapper.get_stock_name(clean_code)
        result[clean_code] = name or ""
    return result


def _get_oa_feishu_bot():
    """Get or create the order assistant FeishuBot (lazy singleton)."""
    global _oa_feishu_bot
    if _oa_feishu_bot is None:
        from src.common.feishu_bot import FeishuBot

        _oa_feishu_bot = FeishuBot()
    return _oa_feishu_bot


def create_order_assistant_router() -> APIRouter:
    """Create router for order assistant endpoints."""
    from datetime import date, datetime, time, timedelta
    from zoneinfo import ZoneInfo

    beijing_tz = ZoneInfo("Asia/Shanghai")

    router = APIRouter(tags=["order-assistant"])

    def _get_current_phase(now: datetime) -> str:
        """Determine market phase from current Beijing time."""
        t = now.time()
        if t < time(9, 30):
            return "premarket"
        elif t < time(15, 0):
            return "trading"
        else:
            return "closed"

    def _get_prev_trading_day_close(today: date) -> datetime:
        """Get 15:00 of the last trading day (skip weekends)."""
        prev = today - timedelta(days=1)
        while prev.weekday() >= 5:
            prev -= timedelta(days=1)
        return datetime.combine(prev, time(15, 0))

    @router.get("/order-assistant", response_class=HTMLResponse)
    async def order_assistant_page(request: Request):
        """Order assistant page — real-time news dashboard."""
        templates = request.app.state.templates
        now = datetime.now(beijing_tz)
        phase = _get_current_phase(now)

        return templates.TemplateResponse(
            "order_assistant.html",
            {
                "request": request,
                "phase": phase,
                "beijing_time": now.strftime("%Y-%m-%d %H:%M:%S"),
                "today": now.strftime("%Y-%m-%d"),
            },
        )

    @router.get("/api/order-assistant/state")
    async def order_assistant_state() -> dict:
        """Get current phase and time info."""
        now = datetime.now(beijing_tz)
        phase = _get_current_phase(now)

        return {
            "phase": phase,
            "beijing_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "today": now.strftime("%Y-%m-%d"),
        }

    @router.get("/api/order-assistant/messages")
    async def order_assistant_messages(
        mode: str = "premarket",
        only_positive: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> dict:
        """
        Get messages for order assistant.

        Args:
            mode: "premarket" for overnight messages, "intraday" for trading-hours messages.
            only_positive: Filter to positive sentiment only.
            limit: Page size.
            offset: Pagination offset.
        """
        if mode not in ("premarket", "intraday"):
            raise HTTPException(status_code=400, detail="mode must be 'premarket' or 'intraday'")

        now = datetime.now(beijing_tz)
        today = now.date()

        try:
            reader = await _get_oa_reader()
        except Exception as e:
            logger.error(f"Failed to connect to message database: {e}")
            raise HTTPException(status_code=503, detail=f"数据库连接失败: {e}")

        # DB stores UTC timestamps (timestamptz). asyncpg interprets naive
        # datetimes using the system TZ (Asia/Shanghai in container).
        # Use timezone-aware Beijing datetimes so asyncpg converts to UTC
        # correctly regardless of the system TZ setting.
        _BJ_UTC = timedelta(hours=8)
        now_bj = now.replace(tzinfo=None)

        if mode == "premarket":
            start_bj = _get_prev_trading_day_close(today)
            end_bj = datetime.combine(today, time(9, 30))
            # Cap end at current Beijing time if before 9:30
            if now_bj < end_bj:
                end_bj = now_bj
        else:
            # Intraday: from 9:30 today to now (or 15:00 if after hours)
            start_bj = datetime.combine(today, time(9, 30))
            end_bj = min(now_bj, datetime.combine(today, time(15, 0)))

        # Tag with Beijing timezone — asyncpg converts to UTC automatically
        start_time = start_bj.replace(tzinfo=beijing_tz)
        end_time = end_bj.replace(tzinfo=beijing_tz)

        try:
            # Get total count
            total_count = await reader.count_messages_in_range(
                start_time=start_time,
                end_time=end_time,
                only_positive=only_positive,
            )

            # Get paginated messages
            messages = await reader.get_messages_in_range(
                start_time=start_time,
                end_time=end_time,
                only_positive=only_positive,
                limit=limit,
                offset=offset,
                order_desc=True,
            )
        except Exception as e:
            logger.error(f"Failed to query messages ({mode}): {e}")
            raise HTTPException(status_code=500, detail=f"查询消息失败: {e}")

        # Collect stock codes for name lookup
        all_stock_codes: set[str] = set()
        for msg in messages:
            all_stock_codes.update(msg.stock_codes or [])
            if msg.analysis and msg.analysis.affected_stocks:
                all_stock_codes.update(msg.analysis.affected_stocks)

        # Look up stock names
        stock_names: dict[str, str] = {}
        if all_stock_codes:
            try:
                mapper = await _get_oa_sector_mapper()
                stock_names = _get_oa_stock_names(list(all_stock_codes), mapper)
            except Exception as e:
                logger.warning(f"Failed to load stock names: {e}")

        # Convert to response format (same as simulation)
        result = []
        for msg in messages:
            msg_dict: dict[str, Any] = {
                "id": msg.id,
                "source_type": msg.source_type,
                "source_name": msg.source_name,
                "title": msg.title,
                "content": msg.content[:500] if msg.content else "",
                "publish_time": (
                    (msg.publish_time + _BJ_UTC).replace(tzinfo=None).isoformat()
                    if msg.publish_time
                    else None
                ),
                "stock_codes": msg.stock_codes,
                "url": msg.url,
            }

            if msg.analysis:
                msg_dict["analysis"] = {
                    "sentiment": msg.analysis.sentiment.value,
                    "confidence": msg.analysis.confidence,
                    "reasoning": msg.analysis.reasoning,
                    "affected_stocks": msg.analysis.affected_stocks,
                }
            else:
                msg_dict["analysis"] = None

            result.append(msg_dict)

        import os

        return {
            "success": True,
            "mode": mode,
            "date": today.isoformat(),
            "beijing_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(result),
            "total_count": total_count,
            "offset": offset,
            "limit": limit,
            "has_more": offset + len(result) < total_count,
            "messages": result,
            "stock_names": stock_names,
            "debug": {
                "git_commit": os.environ.get("GIT_COMMIT", "unknown"),
                "query_start": start_time.isoformat(),
                "query_end": end_time.isoformat(),
                "note": "query times are Beijing (asyncpg converts to UTC)",
            },
        }

    @router.post("/api/order-assistant/feishu-notify")
    async def order_assistant_feishu_notify(request: Request) -> dict:
        """
        Send new intraday messages to Feishu.

        Expects JSON body:
        {
            "messages": [
                {
                    "id": "msg_123",
                    "title": "...",
                    "sentiment": "bullish",
                    "confidence": "85%",
                    "stocks": "600519.SH(贵州茅台)",
                    "reasoning": "...",
                    "publish_time": "2026-02-06T10:32"
                }
            ]
        }

        Deduplicates by message ID — safe to call repeatedly.
        """
        data = await request.json()
        incoming = data.get("messages", [])

        if not incoming:
            return {"success": True, "sent": 0, "message": "无新消息"}

        # Filter out already-sent messages
        new_messages = [m for m in incoming if m.get("id") and m["id"] not in _oa_feishu_sent_ids]

        if not new_messages:
            return {"success": True, "sent": 0, "message": "消息已发送过"}

        bot = _get_oa_feishu_bot()
        if not bot.is_configured():
            return {"success": False, "sent": 0, "message": "飞书未配置"}

        # Build a single batched message
        sentiment_icons = {
            "strong_bullish": "📈📈",
            "bullish": "📈",
            "bearish": "📉",
            "strong_bearish": "📉📉",
        }
        sentiment_labels = {
            "strong_bullish": "强看多",
            "bullish": "看多",
            "bearish": "看空",
            "strong_bearish": "强看空",
        }

        lines = [f"📊 盘中消息推送 ({len(new_messages)} 条新消息)", ""]

        for msg in new_messages:
            sentiment = msg.get("sentiment", "unknown")
            icon = sentiment_icons.get(sentiment, "📌")
            label = sentiment_labels.get(sentiment, sentiment)
            confidence = msg.get("confidence", "")
            pub_time = msg.get("publish_time", "")
            if pub_time and len(pub_time) >= 16:
                pub_time = pub_time[11:16]

            lines.append(f"{icon} {label} {confidence} | {pub_time}")
            lines.append(msg.get("title", "(无标题)"))
            stocks = msg.get("stocks", "")
            if stocks:
                lines.append(f"股票: {stocks}")
            reasoning = msg.get("reasoning", "")
            if reasoning:
                lines.append(f"分析: {reasoning[:80]}")
            lines.append("")

        feishu_message = "\n".join(lines).rstrip()
        sent_ok = await bot.send_message(feishu_message, max_retries=3)

        if sent_ok:
            # Mark as sent to avoid duplicates
            for msg in new_messages:
                _oa_feishu_sent_ids.add(msg["id"])
            logger.info(f"Sent {len(new_messages)} intraday messages to Feishu")
        else:
            logger.warning("Failed to send intraday messages to Feishu")

        return {
            "success": sent_ok,
            "sent": len(new_messages) if sent_ok else 0,
            "message": f"已推送 {len(new_messages)} 条消息" if sent_ok else "推送失败",
        }

    return router


# ==================== Momentum Backtest ====================


class MomentumBacktestRequest(BaseModel):
    """Request body for momentum backtest."""

    trade_date: str  # YYYY-MM-DD format
    notify: bool = False  # Send Feishu notification
    data_source: str = "ifind"  # "ifind" or "akshare"
    news_check: bool = False  # Enable negative news check (Tavily + LLM)


class MomentumRangeBacktestRequest(BaseModel):
    """Request body for momentum range backtest."""

    start_date: str  # YYYY-MM-DD format
    end_date: str  # YYYY-MM-DD format
    initial_capital: float  # Starting capital in yuan
    news_check: bool = False  # Enable negative news check (Tavily + LLM)


class FunnelAnalysisRequest(BaseModel):
    """Request body for funnel layer analysis."""

    start_date: str  # YYYY-MM-DD format
    end_date: str  # YYYY-MM-DD format


class CombinedAnalysisRequest(BaseModel):
    """Request body for combined range backtest + funnel analysis."""

    start_date: str  # YYYY-MM-DD format
    end_date: str  # YYYY-MM-DD format
    initial_capital: float  # Starting capital in yuan
    quality_filter: bool = True  # Enable momentum quality filter (动量质量过滤)
    data_source: str = "ifind"  # "ifind" or "akshare"
    news_check: bool = False  # Enable negative news check (Tavily + LLM)


class AksharePrepareRequest(BaseModel):
    """Request body for akshare data pre-download."""

    start_date: str  # YYYY-MM-DD format
    end_date: str  # YYYY-MM-DD format
    force: bool = False  # Force full re-download (clears existing cache)


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

    def _get_ifind_client(request: Request):
        """Get shared iFinD HTTP client from app.state."""
        client = getattr(request.app.state, "ifind_client", None)
        if client is None:
            raise HTTPException(status_code=503, detail="iFinD 客户端未就绪")
        return client

    def _get_fundamentals_db(request: Request):
        """Get shared fundamentals DB from app.state."""
        db = getattr(request.app.state, "fundamentals_db", None)
        if db is None:
            raise HTTPException(status_code=503, detail="基本面数据库未就绪")
        return db

    async def _create_news_checker():
        """Create and start a NegativeNewsChecker if both API keys are available.

        Returns None if keys are missing (graceful degradation for backtest).
        Caller is responsible for calling _stop_news_checker() when done.
        """
        from src.common.config import get_tavily_api_key
        from src.common.siliconflow_client import SiliconFlowClient, SiliconFlowConfig
        from src.common.tavily_client import TavilyClient
        from src.strategy.analyzers.negative_news_checker import NegativeNewsChecker

        try:
            tavily_key = get_tavily_api_key()
        except (ValueError, FileNotFoundError):
            logger.warning("News checker: Tavily API key not configured")
            return None

        sf_key = _get_llm_api_key()
        if not sf_key:
            logger.warning("News checker: SiliconFlow API key not configured")
            return None

        tavily = TavilyClient(api_key=tavily_key)
        sf = SiliconFlowClient(SiliconFlowConfig(api_key=sf_key))
        await tavily.start()
        await sf.start()
        return NegativeNewsChecker(tavily, sf)

    async def _stop_news_checker(checker) -> None:
        """Stop the Tavily and SiliconFlow clients inside a NegativeNewsChecker."""
        if checker is None:
            return
        try:
            await checker._tavily.stop()
        except Exception:
            pass
        try:
            await checker._llm.stop()
        except Exception:
            pass

    @router.get("/momentum", response_class=HTMLResponse)
    async def momentum_page(request: Request):
        """Momentum backtest and monitor page."""
        templates = request.app.state.templates
        monitor_state = _get_monitor_state(request)
        resp = templates.TemplateResponse(
            "momentum_backtest.html",
            {
                "request": request,
                "monitor_running": monitor_state["running"],
            },
        )
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return resp

    @router.get("/api/momentum/akshare-cache-status")
    async def akshare_cache_status(request: Request):
        """Return akshare cache state for frontend polling."""
        loading = getattr(request.app.state, "akshare_cache_loading", False)
        cache = getattr(request.app.state, "akshare_cache", None)
        if loading and cache is None:
            return {"status": "loading"}
        if cache is None:
            return {"status": "empty"}
        return {
            "status": "ready",
            "start_date": str(cache._start_date) if cache._start_date else None,
            "end_date": str(cache._end_date) if cache._end_date else None,
            "daily_stocks": len(cache._daily),
            "minute_stocks": len(cache._minute),
        }

    @router.post("/api/momentum/akshare-prepare")
    async def akshare_prepare(request: Request, body: AksharePrepareRequest):
        """Pre-download akshare data as SSE stream (incremental).

        Loads existing cache from memory / OSS, calculates which date
        ranges are missing, and only downloads the gaps.
        """
        from src.data.clients.akshare_backtest_cache import (
            AkshareBacktestCache,
            check_oss_available,
        )

        try:
            start_date = datetime.strptime(body.start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(body.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误")

        # Force re-download: clear existing cache so it downloads fresh
        if body.force:
            request.app.state.akshare_cache = None

        # 1) Try in-memory cache
        existing: AkshareBacktestCache | None = getattr(request.app.state, "akshare_cache", None)
        if existing and existing.covers_range(start_date, end_date):

            async def mem_cached_stream():
                msg = {
                    "type": "complete",
                    "daily_count": len(existing._daily),  # type: ignore[union-attr]
                    "minute_count": len(existing._minute),  # type: ignore[union-attr]
                    "cached": True,
                }
                yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"

            return StreamingResponse(mem_cached_stream(), media_type="text/event-stream")

        # 2) Try OSS cache
        if not existing:
            existing = await asyncio.to_thread(AkshareBacktestCache.load_from_oss)
        if existing and existing.covers_range(start_date, end_date):
            request.app.state.akshare_cache = existing

            async def cached_stream():
                msg = {
                    "type": "complete",
                    "daily_count": len(existing._daily),  # type: ignore[union-attr]
                    "minute_count": len(existing._minute),  # type: ignore[union-attr]
                    "cached": True,
                }
                yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"

            return StreamingResponse(cached_stream(), media_type="text/event-stream")

        # 3) Pre-flight: verify OSS is reachable BEFORE downloading
        oss_err = await asyncio.to_thread(check_oss_available)
        if oss_err:
            raise HTTPException(
                status_code=500,
                detail=f"OSS 不可用，请先修复再下载: {oss_err}",
            )

        # 4) Calculate gaps to download (incremental).
        # Work on a COPY so partial failures don't corrupt the live in-memory cache.
        if existing:
            gaps = existing.missing_ranges(start_date, end_date)
            working = existing.copy()
        else:
            working = AkshareBacktestCache()
            gaps = [(start_date, end_date)]

        if not gaps:
            # Shouldn't happen (covers_range check above), but just in case
            request.app.state.akshare_cache = working

            async def nogap_stream():
                msg = {
                    "type": "complete",
                    "daily_count": len(working._daily),
                    "minute_count": len(working._minute),
                    "cached": True,
                }
                yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"

            return StreamingResponse(nogap_stream(), media_type="text/event-stream")

        async def generate():
            def sse(data: dict) -> str:
                return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

            progress_queue: asyncio.Queue = asyncio.Queue()

            async def queue_progress(phase: str, current: int, total: int):
                await progress_queue.put(
                    {"type": "progress", "phase": phase, "current": current, "total": total}
                )

            download_error = None

            async def do_download():
                nonlocal download_error
                try:
                    for i, (gap_start, gap_end) in enumerate(gaps):
                        label = f"{gap_start}~{gap_end}"
                        await progress_queue.put(
                            {"type": "info", "message": f"增量下载 {label} ({i + 1}/{len(gaps)})"}
                        )
                        gap_cache = AkshareBacktestCache()
                        await gap_cache.download_prices(gap_start, gap_end, queue_progress)
                        working.merge_from(gap_cache)
                    working._is_ready = True
                    await progress_queue.put(None)
                except Exception as e:
                    download_error = str(e)
                    await progress_queue.put(None)

            task = asyncio.create_task(do_download())

            while True:
                item = await progress_queue.get()
                if item is None:
                    break
                yield sse(item)

            await task

            if download_error:
                yield sse({"type": "error", "message": download_error})
            else:
                # Only assign to app.state on complete success (no partial mutation)
                request.app.state.akshare_cache = working

                # Fire-and-forget OSS save — don't block SSE and won't be
                # cancelled if the client disconnects
                async def _bg_oss_save():
                    try:
                        err = await working.save_to_oss()
                        if err:
                            logger.warning(f"akshare OSS save failed: {err}")
                        else:
                            logger.info("akshare cache saved to OSS OK")
                    except Exception as exc:
                        logger.error(f"akshare OSS save exception: {exc}")

                asyncio.create_task(_bg_oss_save())
                yield sse(
                    {
                        "type": "complete",
                        "daily_count": len(working._daily),
                        "minute_count": len(working._minute),
                    }
                )

        return StreamingResponse(generate(), media_type="text/event-stream")

    @router.post("/api/momentum/backtest")
    async def run_backtest(request: Request, body: MomentumBacktestRequest) -> dict:
        """Run momentum sector strategy backtest for a specific date."""
        from src.common.feishu_bot import FeishuBot
        from src.data.clients.ifind_http_client import IFinDHttpError
        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.strategies.momentum_sector_scanner import (
            MomentumSectorScanner,
        )

        # Parse date
        try:
            trade_date = datetime.strptime(body.trade_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"日期格式错误: {body.trade_date}，请使用 YYYY-MM-DD",
            )

        fundamentals_db = _get_fundamentals_db(request)
        news_checker = await _create_news_checker() if body.news_check else None

        try:
            concept_mapper = LocalConceptMapper()
            if body.data_source == "akshare":
                # --- Akshare path: read from pre-downloaded cache ---
                from src.data.clients.akshare_backtest_cache import (
                    AkshareHistoricalAdapter,
                )

                ak_cache = getattr(request.app.state, "akshare_cache", None)
                if not ak_cache or not ak_cache.is_ready:
                    raise HTTPException(status_code=400, detail="请先预下载 akshare 数据")

                adapter = AkshareHistoricalAdapter(ak_cache)
                scanner = MomentumSectorScanner(
                    ifind_client=adapter,  # type: ignore[arg-type]
                    fundamentals_db=fundamentals_db,
                    concept_mapper=concept_mapper,
                    negative_news_checker=news_checker,
                )

                date_key = trade_date.strftime("%Y-%m-%d")
                price_snapshots = _build_snapshots_from_cache(ak_cache, date_key)
                if not price_snapshots:
                    return {
                        "success": True,
                        "trade_date": body.trade_date,
                        "initial_gainers": 0,
                        "hot_boards": {},
                        "selected_stocks": [],
                        "message": f"akshare缓存中无 {date_key} 的数据",
                    }
            else:
                # --- iFinD path: original logic ---
                ifind_client = _get_ifind_client(request)
                scanner = MomentumSectorScanner(
                    ifind_client=ifind_client,
                    fundamentals_db=fundamentals_db,
                    concept_mapper=concept_mapper,
                    negative_news_checker=news_checker,
                )

                date_str = trade_date.strftime("%Y%m%d")
                query = f"{date_str}开盘涨幅大于-0.5%的沪深主板非ST股票"
                logger.info(f"Momentum backtest iwencai query: {query}")

                try:
                    iwencai_result = await ifind_client.smart_stock_picking(query, "stock")
                except IFinDHttpError as e:
                    raise HTTPException(status_code=502, detail=f"iFinD查询失败: {e}")

                price_snapshots, price_err = await _parse_iwencai_and_fetch_prices(
                    ifind_client, iwencai_result, trade_date
                )

                if not price_snapshots:
                    return {
                        "success": True,
                        "trade_date": body.trade_date,
                        "initial_gainers": 0,
                        "hot_boards": {},
                        "selected_stocks": [],
                        "message": price_err or "未找到符合条件的股票",
                    }

            # Run scan (pass trade_date so constituent prices use history_quotes)
            result = await scanner.scan(price_snapshots, trade_date=trade_date)

            # Format response
            rec = result.recommended_stock
            response_data = {
                "success": True,
                "trade_date": body.trade_date,
                "initial_gainers": len(result.initial_gainers),
                "hot_boards": {name: codes for name, codes in result.hot_boards.items()},
                "selected_stocks": [
                    {
                        "stock_code": s.stock_code,
                        "stock_name": s.stock_name,
                        "board_name": s.board_name,
                        "open_gain_pct": round(s.open_gain_pct, 2),
                        "pe_ttm": round(s.pe_ttm, 2),
                        "board_avg_pe": round(s.board_avg_pe, 2),
                    }
                    for s in result.selected_stocks
                ],
                "recommended_stock": {
                    "stock_code": rec.stock_code,
                    "stock_name": rec.stock_name,
                    "board_name": rec.board_name,
                    "board_stock_count": rec.board_stock_count,
                    "growth_rate": round(rec.growth_rate, 2),
                    "open_gain_pct": round(rec.open_gain_pct, 2),
                    "pe_ttm": round(rec.pe_ttm, 2),
                    "board_avg_pe": round(rec.board_avg_pe, 2),
                    "open_price": round(rec.open_price, 2),
                    "prev_close": round(rec.prev_close, 2),
                    "news_check_passed": rec.news_check_passed,
                    "news_check_detail": rec.news_check_detail,
                }
                if rec
                else None,
            }

            # Send Feishu if requested
            if body.notify and result.has_results:
                bot = FeishuBot()
                if bot.is_configured():
                    await bot.send_momentum_scan_result(
                        selected_stocks=result.selected_stocks,
                        hot_boards=result.hot_boards,
                        initial_gainer_count=len(result.initial_gainers),
                        scan_time=result.scan_time,
                        recommended_stock=result.recommended_stock,
                    )
                    response_data["feishu_sent"] = True

            return response_data

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Momentum backtest error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"回测出错: {str(e)}")
        finally:
            await _stop_news_checker(news_checker)

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

    @router.post("/api/momentum/range-backtest")
    async def run_range_backtest(request: Request, body: MomentumRangeBacktestRequest):
        """[DEPRECATED] Use /api/momentum/combined-analysis instead.

        Run momentum range backtest with SSE streaming progress."""
        import asyncio
        import json
        import math
        from datetime import datetime

        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.strategies.momentum_sector_scanner import (
            MomentumSectorScanner,
        )

        # Validate dates
        try:
            start_date = datetime.strptime(body.start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(body.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD")

        if end_date <= start_date:
            raise HTTPException(status_code=400, detail="结束日期必须晚于起始日期")

        if body.initial_capital < 1000:
            raise HTTPException(status_code=400, detail="起始资金不能低于 1000 元")

        ifind_client = _get_ifind_client(request)
        fundamentals_db = _get_fundamentals_db(request)
        news_checker = await _create_news_checker() if body.news_check else None

        async def event_stream():
            """SSE event generator for range backtest."""

            def sse(data: dict) -> str:
                return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

            try:
                concept_mapper = LocalConceptMapper()
                scanner = MomentumSectorScanner(
                    ifind_client=ifind_client,
                    fundamentals_db=fundamentals_db,
                    concept_mapper=concept_mapper,
                    negative_news_checker=news_checker,
                )

                # Get trading calendar from AKShare (avoid iFinD to prevent session conflict)
                # Include extra days beyond end_date for T+1 sell price lookups.
                from datetime import timedelta as _td

                trading_days_all = _get_trading_calendar_akshare(
                    start_date, end_date + _td(days=10)
                )
                trading_days = [d for d in trading_days_all if start_date <= d <= end_date]
                # Build T+1 map from the full list (needed for selling on T+1)
                _next_day_map: dict = {}
                for _idx, _d in enumerate(trading_days_all):
                    if _idx + 1 < len(trading_days_all):
                        _next_day_map[_d] = trading_days_all[_idx + 1]
                if not trading_days:
                    yield sse({"type": "error", "message": "所选日期范围内无交易日"})
                    return

                if len(trading_days) > 250:
                    trading_days = trading_days[:250]
                    first, last = trading_days[0], trading_days[-1]
                    yield sse(
                        {
                            "type": "warning",
                            "message": f"已截断至前 250 个交易日 ({first} ~ {last})",
                        }
                    )

                yield sse(
                    {
                        "type": "init",
                        "total_days": len(trading_days),
                        "start_date": str(trading_days[0]),
                        "end_date": str(trading_days[-1]),
                        "initial_capital": body.initial_capital,
                    }
                )

                capital = body.initial_capital
                day_results: list[dict] = []

                for i, day in enumerate(trading_days):
                    yield sse(
                        {
                            "type": "progress",
                            "day": i + 1,
                            "total": len(trading_days),
                            "trade_date": str(day),
                        }
                    )

                    # Run single-day scan
                    try:
                        scan_result = await _run_momentum_scan_for_date(ifind_client, scanner, day)
                    except Exception as e:
                        logger.error(f"Range backtest scan error on {day}: {e}")
                        day_results.append(
                            {
                                "trade_date": str(day),
                                "has_trade": False,
                                "skip_reason": f"策略出错: {str(e)[:50]}",
                                "capital": round(capital, 2),
                            }
                        )
                        yield sse(
                            {
                                "type": "day_result",
                                **day_results[-1],
                            }
                        )
                        await asyncio.sleep(0.05)
                        continue

                    rec = (
                        scan_result.recommended_stock
                        if scan_result and scan_result.recommended_stock
                        else None
                    )

                    if not rec:
                        day_results.append(
                            {
                                "trade_date": str(day),
                                "has_trade": False,
                                "skip_reason": "无推荐",
                                "capital": round(capital, 2),
                            }
                        )
                        yield sse({"type": "day_result", **day_results[-1]})
                        await asyncio.sleep(0.05)
                        continue

                    # Fetch buy price: use 9:40 price (from scan), fallback to open
                    buy_price = rec.latest_price
                    if buy_price <= 0:
                        buy_price = rec.open_price
                    if buy_price <= 0:
                        # Fallback: fetch 9:40 price directly
                        try:
                            prices = await _fetch_stock_940_price(ifind_client, rec.stock_code, day)
                            if prices:
                                buy_price = prices[0][1]
                        except Exception as e:
                            logger.warning(f"Buy 9:40 fallback failed for {rec.stock_code}: {e}")
                    if buy_price <= 0:
                        # Last resort: use open price
                        try:
                            prices = await _fetch_stock_open_prices(
                                ifind_client, rec.stock_code, day
                            )
                            if prices:
                                buy_price = prices[0][1]
                        except Exception as e:
                            logger.warning(f"Buy open fallback failed for {rec.stock_code}: {e}")

                    if buy_price <= 0:
                        day_results.append(
                            {
                                "trade_date": str(day),
                                "has_trade": False,
                                "skip_reason": "无法获取买入价",
                                "stock_code": rec.stock_code,
                                "stock_name": rec.stock_name,
                                "capital": round(capital, 2),
                            }
                        )
                        yield sse({"type": "day_result", **day_results[-1]})
                        await asyncio.sleep(0.05)
                        continue

                    # Fetch next trading day open price for selling (次日开盘卖)
                    sell_price = 0.0
                    sell_date_str = ""
                    sell_fetch_error = ""
                    next_day = _next_day_map.get(day)

                    if next_day:
                        try:
                            sell_prices = await _fetch_stock_open_prices(
                                ifind_client, rec.stock_code, next_day
                            )
                            if sell_prices:
                                sell_price = sell_prices[0][1]
                                sell_date_str = str(sell_prices[0][0])
                        except Exception as e:
                            sell_fetch_error = str(e)
                            logger.error(
                                f"Sell price fetch error for {rec.stock_code} on {next_day}: {e}"
                            )
                    else:
                        sell_fetch_error = "无下一交易日"

                    if sell_price <= 0:
                        detail = sell_fetch_error or "次日开盘价为0或无数据"
                        day_results.append(
                            {
                                "trade_date": str(day),
                                "has_trade": False,
                                "skip_reason": f"无法获取次日开盘卖出价: {detail}",
                                "stock_code": rec.stock_code,
                                "stock_name": rec.stock_name,
                                "capital": round(capital, 2),
                            }
                        )
                        yield sse({"type": "day_result", **day_results[-1]})
                        await asyncio.sleep(0.05)
                        continue

                    # Calculate lots
                    lots = math.floor(capital / (buy_price * 100))
                    if lots <= 0:
                        day_results.append(
                            {
                                "trade_date": str(day),
                                "has_trade": False,
                                "skip_reason": f"资金不足 (需 {buy_price * 100:.0f} 元/手)",
                                "stock_code": rec.stock_code,
                                "stock_name": rec.stock_name,
                                "capital": round(capital, 2),
                            }
                        )
                        yield sse({"type": "day_result", **day_results[-1]})
                        await asyncio.sleep(0.05)
                        continue

                    # Check total buy cost fits within capital
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

                    if lots <= 0:
                        day_results.append(
                            {
                                "trade_date": str(day),
                                "has_trade": False,
                                "skip_reason": "资金不足（含手续费）",
                                "stock_code": rec.stock_code,
                                "stock_name": rec.stock_name,
                                "capital": round(capital, 2),
                            }
                        )
                        yield sse({"type": "day_result", **day_results[-1]})
                        await asyncio.sleep(0.05)
                        continue

                    # Calculate sell proceeds
                    sell_amount = lots * 100 * sell_price
                    sell_commission = max(sell_amount * 0.003, 5.0)
                    sell_transfer = sell_amount * 0.00001
                    sell_stamp = sell_amount * 0.0005
                    net_sell = sell_amount - sell_commission - sell_transfer - sell_stamp

                    # Update capital
                    capital_before = capital
                    capital = capital - total_buy_cost + net_sell
                    trade_profit = net_sell - total_buy_cost
                    trade_return_pct = (
                        trade_profit / total_buy_cost * 100 if total_buy_cost > 0 else 0
                    )

                    day_result = {
                        "trade_date": str(day),
                        "has_trade": True,
                        "stock_code": rec.stock_code,
                        "stock_name": rec.stock_name,
                        "board_name": rec.board_name,
                        "buy_price": round(buy_price, 2),
                        "sell_price": round(sell_price, 2),
                        "sell_date": sell_date_str,
                        "lots": lots,
                        "buy_amount": round(buy_amount, 2),
                        "buy_commission": round(buy_commission, 2),
                        "buy_transfer": round(buy_transfer, 2),
                        "sell_amount": round(sell_amount, 2),
                        "sell_commission": round(sell_commission, 2),
                        "sell_transfer": round(sell_transfer, 2),
                        "sell_stamp": round(sell_stamp, 2),
                        "profit": round(trade_profit, 2),
                        "return_pct": round(trade_return_pct, 2),
                        "capital_before": round(capital_before, 2),
                        "capital": round(capital, 2),
                        "hot_boards": {
                            name: codes
                            for name, codes in (
                                scan_result.hot_boards if scan_result else {}
                            ).items()
                        },
                    }
                    if rec.news_check_passed is not None:
                        day_result["news_check_passed"] = rec.news_check_passed
                        day_result["news_check_detail"] = rec.news_check_detail
                    day_results.append(day_result)
                    yield sse({"type": "day_result", **day_result})
                    await asyncio.sleep(0.05)

                # Summary
                trade_results = [d for d in day_results if d.get("has_trade")]
                wins = [d for d in trade_results if d["profit"] > 0]
                losses = [d for d in trade_results if d["profit"] < 0]
                total_return_pct = (capital - body.initial_capital) / body.initial_capital * 100

                summary = {
                    "initial_capital": body.initial_capital,
                    "final_capital": round(capital, 2),
                    "total_return_pct": round(total_return_pct, 2),
                    "total_days": len(trading_days),
                    "trade_days": len(trade_results),
                    "skip_days": len(trading_days) - len(trade_results),
                    "win_days": len(wins),
                    "lose_days": len(losses),
                    "even_days": len(trade_results) - len(wins) - len(losses),
                    "win_rate": round(
                        len(wins) / len(trade_results) * 100 if trade_results else 0, 1
                    ),
                    "max_win": round(max((d["profit"] for d in trade_results), default=0), 2),
                    "max_loss": round(min((d["profit"] for d in trade_results), default=0), 2),
                    "total_commission": round(
                        sum(
                            d.get("buy_commission", 0) + d.get("sell_commission", 0)
                            for d in trade_results
                        ),
                        2,
                    ),
                    "total_stamp_tax": round(sum(d.get("sell_stamp", 0) for d in trade_results), 2),
                }
                yield sse({"type": "complete", "summary": summary})

            except Exception as e:
                logger.error(f"Range backtest error: {e}", exc_info=True)
                yield sse({"type": "error", "message": f"回测出错: {str(e)}"})
            finally:
                await _stop_news_checker(news_checker)

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    @router.post("/api/momentum/loss-analysis")
    async def run_loss_analysis(request: Request):
        """Analyze losing trades from range backtest with board trend data and LLM."""
        import json

        body = await request.json()
        losing_trades = body.get("losing_trades", [])
        data_source = body.get("data_source", "ifind")

        if data_source == "akshare":
            from src.data.clients.akshare_backtest_cache import (
                AkshareHistoricalAdapter,
            )

            ak_cache = getattr(request.app.state, "akshare_cache", None)
            if ak_cache and ak_cache.is_ready:
                quote_client = AkshareHistoricalAdapter(ak_cache)
            else:
                quote_client = _get_ifind_client(request)
        else:
            quote_client = _get_ifind_client(request)

        if not losing_trades:
            raise HTTPException(status_code=400, detail="没有亏损交易数据")

        async def analysis_stream():
            def sse(data: dict) -> str:
                return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

            yield sse({"type": "init", "total": len(losing_trades)})

            try:
                analyses = []
                for idx, trade in enumerate(losing_trades):
                    trade_date_str = trade["trade_date"]
                    board_name = trade.get("board_name", "")
                    hot_boards = trade.get("hot_boards", {})
                    trigger_codes = hot_boards.get(board_name, [])

                    yield sse(
                        {
                            "type": "progress",
                            "index": idx,
                            "total": len(losing_trades),
                            "stock_code": trade["stock_code"],
                            "stock_name": trade.get("stock_name", ""),
                            "message": (
                                f"正在分析 {trade['stock_code']} {trade.get('stock_name', '')}..."
                            ),
                        }
                    )

                    # Fetch board trend data (iFinD only — akshare has no board index)
                    board_data = {}
                    if board_name and data_source != "akshare":
                        try:
                            board_data = await _fetch_board_trend(
                                quote_client, board_name, trade_date_str
                            )
                        except Exception as e:
                            logger.warning(
                                "Board trend fetch failed for"
                                f" {board_name} on {trade_date_str}: {e}"
                            )

                    # Fetch individual stock full-day data for context
                    stock_day_data = {}
                    try:
                        stock_day_data = await _fetch_stock_day_trend(
                            quote_client, trade["stock_code"], trade_date_str
                        )
                    except Exception as e:
                        logger.warning(
                            f"Stock day trend fetch failed for {trade['stock_code']}: {e}"
                        )

                    # Call LLM for per-stock analysis
                    llm_analysis = ""
                    try:
                        llm_analysis = await _call_llm_stock_analysis(
                            trade, board_data, stock_day_data, trigger_codes
                        )
                    except Exception as e:
                        logger.error(f"LLM analysis failed for {trade['stock_code']}: {e}")
                        llm_analysis = f"LLM 分析失败: {str(e)[:100]}"

                    analysis_entry = {
                        "stock_code": trade["stock_code"],
                        "stock_name": trade.get("stock_name", ""),
                        "board_name": board_name,
                        "trade_date": trade_date_str,
                        "sell_date": trade.get("sell_date", ""),
                        "buy_price": trade.get("buy_price", 0),
                        "sell_price": trade.get("sell_price", 0),
                        "profit": trade.get("profit", 0),
                        "return_pct": trade.get("return_pct", 0),
                        "trigger_codes": trigger_codes,
                        "board_data": board_data,
                        "stock_day_data": stock_day_data,
                        "llm_analysis": llm_analysis,
                    }
                    analyses.append(analysis_entry)

                    yield sse({"type": "stock_analysis", "index": idx, **analysis_entry})

                # Strategy-level LLM summary
                yield sse(
                    {
                        "type": "progress",
                        "index": len(losing_trades),
                        "total": len(losing_trades),
                        "message": "正在生成策略总结...",
                    }
                )

                strategy_summary = ""
                try:
                    strategy_summary = await _call_llm_strategy_summary(analyses)
                except Exception as e:
                    logger.error(f"LLM strategy summary failed: {e}")
                    strategy_summary = f"策略总结生成失败: {str(e)[:100]}"

                yield sse({"type": "strategy_summary", "summary": strategy_summary})
                yield sse({"type": "complete"})

            except Exception as e:
                logger.error(f"Loss analysis error: {e}", exc_info=True)
                yield sse({"type": "error", "message": f"分析出错: {str(e)}"})

        return StreamingResponse(
            analysis_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    @router.post("/api/momentum/funnel-analysis")
    async def run_funnel_analysis(request: Request, body: FunnelAnalysisRequest):
        """[DEPRECATED] Use /api/momentum/combined-analysis instead.

        Run funnel layer analysis with SSE streaming.

        Evaluates each filter layer's effectiveness by calculating next-day
        returns for stocks at every stage of the selection pipeline.
        """
        import json
        from datetime import datetime
        from statistics import median as stat_median

        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.filters.stock_filter import create_main_board_only_filter
        from src.strategy.strategies.momentum_sector_scanner import (
            MomentumSectorScanner,
            SelectedStock,
        )

        # Validate dates
        try:
            start_date = datetime.strptime(body.start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(body.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD")

        if end_date < start_date:
            raise HTTPException(status_code=400, detail="结束日期不能早于起始日期")

        LAYER_NAMES = [
            "L0: 全部成分股",
            "L1: 涨幅>0.56%",
            "L2: 动量质量过滤",
            "L3: 冲高回落过滤",
            "L4: 最终推荐",
        ]

        ifind_client = _get_ifind_client(request)
        fundamentals_db = _get_fundamentals_db(request)

        async def event_stream():
            def sse(data: dict) -> str:
                return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

            try:
                # Get trading calendar (include extra days for T+1)
                from datetime import timedelta

                trading_days = _get_trading_calendar_akshare(
                    start_date, end_date + timedelta(days=10)
                )
                if not trading_days:
                    yield sse({"type": "error", "message": "所选日期范围内无交易日"})
                    return

                days_in_range = [d for d in trading_days if start_date <= d <= end_date]
                if not days_in_range:
                    yield sse({"type": "error", "message": "所选日期范围内无交易日"})
                    return

                # Build T+1 map
                next_day_map = {}
                for i, d in enumerate(trading_days):
                    if d > end_date:
                        break
                    if d >= start_date and i + 1 < len(trading_days):
                        next_day_map[d] = trading_days[i + 1]

                if len(days_in_range) > 250:
                    days_in_range = days_in_range[:250]
                    yield sse(
                        {
                            "type": "warning",
                            "message": (
                                "已截断至前 250 个交易日"
                                f" ({days_in_range[0]} ~ {days_in_range[-1]})"
                            ),
                        }
                    )

                yield sse(
                    {
                        "type": "init",
                        "total_days": len(days_in_range),
                        "start_date": str(days_in_range[0]),
                        "end_date": str(days_in_range[-1]),
                    }
                )

                concept_mapper = LocalConceptMapper()
                stock_filter = create_main_board_only_filter()

                # Accumulate all returns per layer across all days
                all_layer_returns: dict[str, list[float]] = {n: [] for n in LAYER_NAMES}
                all_layer_counts: dict[str, list[int]] = {n: [] for n in LAYER_NAMES}
                # Per-stock detail for filtered-out analysis
                all_layer_detail: dict[str, list[dict]] = {n: [] for n in LAYER_NAMES}
                days_processed = 0

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
                        # Fetch price data
                        price_snapshots, price_err = await _parse_iwencai_and_fetch_prices_for_date(
                            ifind_client, trade_date
                        )
                        if not price_snapshots:
                            yield sse(
                                {
                                    "type": "day_skip",
                                    "trade_date": str(trade_date),
                                    "reason": price_err or "无价格数据",
                                }
                            )
                            await asyncio.sleep(0.05)
                            continue

                        # Steps 1-4 via scanner
                        scanner = MomentumSectorScanner(
                            ifind_client=ifind_client,
                            fundamentals_db=fundamentals_db,
                            concept_mapper=concept_mapper,
                            stock_filter=stock_filter,
                        )
                        scanner._trade_date = trade_date

                        gainers = await scanner._step1_filter_gainers(price_snapshots)
                        if not gainers:
                            yield sse(
                                {
                                    "type": "day_skip",
                                    "trade_date": str(trade_date),
                                    "reason": "无初筛股",
                                }
                            )
                            await asyncio.sleep(0.05)
                            continue

                        stock_boards = await scanner._step2_reverse_lookup(list(gainers.keys()))
                        hot_boards = scanner._step3_find_hot_boards(stock_boards)
                        if not hot_boards:
                            yield sse(
                                {
                                    "type": "day_skip",
                                    "trade_date": str(trade_date),
                                    "reason": "无热门板块",
                                }
                            )
                            await asyncio.sleep(0.05)
                            continue

                        board_constituents = await scanner._step4_get_constituents(
                            list(hot_boards.keys())
                        )

                        # Step 5: layer capture (no PE filter)
                        all_constituent_codes: set[str] = set()
                        filtered_bc: dict[str, list[tuple[str, str]]] = {}
                        for bn, stocks in board_constituents.items():
                            allowed = [(c, n) for c, n in stocks if stock_filter.is_allowed(c)]
                            filtered_bc[bn] = allowed
                            for c, _ in allowed:
                                all_constituent_codes.add(c)

                        missing = [c for c in all_constituent_codes if c not in price_snapshots]
                        if missing:
                            extra = await scanner._fetch_constituent_prices(missing)
                            price_snapshots = {**price_snapshots, **extra}

                        # Build layers: L0 (all constituents), L1 (gain filter)
                        l0: list[SelectedStock] = []
                        l1: list[SelectedStock] = []

                        for bn, stocks in filtered_bc.items():
                            for code, name in stocks:
                                snap = price_snapshots.get(code)
                                if not snap:
                                    continue

                                ss = SelectedStock(
                                    stock_code=code,
                                    stock_name=name,
                                    board_name=bn,
                                    open_gain_pct=snap.open_gain_pct,
                                    pe_ttm=0.0,
                                    board_avg_pe=0.0,
                                )
                                l0.append(ss)

                                threshold = MomentumSectorScanner.GAIN_FROM_OPEN_THRESHOLD
                                if snap.gain_from_open_pct >= threshold:
                                    l1.append(ss)

                        # Deduplicate
                        def _dedup(stocks):
                            seen = {}
                            for s in stocks:
                                ex = seen.get(s.stock_code)
                                if ex is None or s.open_gain_pct > ex.open_gain_pct:
                                    seen[s.stock_code] = s
                            return list(seen.values())

                        l0 = _dedup(l0)
                        l1 = _dedup(l1)

                        # L2: momentum quality filter
                        from src.strategy.filters.momentum_quality_filter import (
                            MomentumQualityConfig,
                            MomentumQualityFilter,
                        )

                        quality_config = MomentumQualityConfig(enabled=True)
                        quality_filter_inst = MomentumQualityFilter(ifind_client, quality_config)
                        if l1:
                            l2, qa = await quality_filter_inst.filter_stocks(
                                l1, price_snapshots, trade_date
                            )
                        else:
                            l2 = list(l1)
                            qa = []

                        # Build trend_data from quality assessments for Step 6 scoring
                        trend_data_l = {
                            a.stock_code: a.trend_pct for a in qa if a.trend_pct is not None
                        }

                        # L3: reversal factor filter (冲高回落)
                        from src.strategy.filters.reversal_factor_filter import (
                            ReversalFactorConfig,
                            ReversalFactorFilter,
                        )

                        reversal_config = ReversalFactorConfig(enabled=True)
                        reversal_filter_inst = ReversalFactorFilter(reversal_config)
                        if l2:
                            l3, _ = await reversal_filter_inst.filter_stocks(
                                l2, price_snapshots, trade_date
                            )
                        else:
                            l3 = list(l2)

                        # L4: recommendation
                        l4 = []
                        if l3:
                            rec = await scanner._step6_recommend(l3, price_snapshots, trend_data_l)
                            if rec:
                                l4 = [
                                    SelectedStock(
                                        stock_code=rec.stock_code,
                                        stock_name=rec.stock_name,
                                        board_name=rec.board_name,
                                        open_gain_pct=rec.open_gain_pct,
                                        pe_ttm=rec.pe_ttm,
                                        board_avg_pe=rec.board_avg_pe,
                                    )
                                ]

                        all_layers = [l0, l1, l2, l3, l4]

                        # Fetch revenue growth for L3 stocks from DB
                        day_revenue_growth: dict[str, float] = {}
                        if l3:
                            l3_codes = [s.stock_code for s in l3]
                            day_revenue_growth = await fundamentals_db.batch_get_revenue_growth(
                                l3_codes
                            )

                        # Collect all codes for T+1 fetch
                        all_codes = set()
                        for layer in all_layers:
                            for s in layer:
                                all_codes.add(s.stock_code)

                        if not all_codes:
                            yield sse(
                                {
                                    "type": "day_skip",
                                    "trade_date": str(trade_date),
                                    "reason": "成分股无价格",
                                }
                            )
                            await asyncio.sleep(0.05)
                            continue

                        # Fetch T+1 open (consistent with interval backtest: 次日开盘卖)
                        next_open = await _fetch_batch_prices(
                            ifind_client, list(all_codes), next_trade_date, indicator="open"
                        )

                        # Calculate returns per layer (with costs, same as backtest)
                        day_layers = {}
                        for lname, lstocks in zip(LAYER_NAMES, all_layers):
                            returns = []
                            for s in lstocks:
                                snap = price_snapshots.get(s.stock_code)
                                sell_price = next_open.get(s.stock_code)
                                if snap and sell_price and snap.latest_price > 0:
                                    ret = _calc_net_return_pct(snap.latest_price, sell_price)
                                    ret_rounded = round(ret, 2)
                                    returns.append(ret_rounded)
                                    detail_entry = {
                                        "trade_date": str(trade_date),
                                        "stock_code": s.stock_code,
                                        "stock_name": s.stock_name,
                                        "board_name": s.board_name,
                                        "return_pct": ret_rounded,
                                    }
                                    rg = day_revenue_growth.get(s.stock_code)
                                    if rg is not None:
                                        detail_entry["revenue_growth"] = round(rg, 2)
                                    all_layer_detail[lname].append(detail_entry)

                            count = len(lstocks)
                            all_layer_counts[lname].append(count)
                            all_layer_returns[lname].extend(returns)

                            if returns:
                                avg_r = sum(returns) / len(returns)
                                win_r = sum(1 for r in returns if r > 0) / len(returns) * 100
                                med_r = stat_median(returns)
                            else:
                                avg_r = win_r = med_r = 0.0

                            day_layers[lname] = {
                                "count": count,
                                "avg_return": round(avg_r, 2),
                                "win_rate": round(win_r, 1),
                                "median_return": round(med_r, 2),
                            }

                        days_processed += 1
                        yield sse(
                            {
                                "type": "day_result",
                                "trade_date": str(trade_date),
                                "layers": day_layers,
                            }
                        )
                        await asyncio.sleep(0.05)

                    except Exception as e:
                        logger.error(f"Funnel analysis error on {trade_date}: {e}", exc_info=True)
                        yield sse(
                            {
                                "type": "day_skip",
                                "trade_date": str(trade_date),
                                "reason": f"出错: {str(e)[:60]}",
                            }
                        )
                        await asyncio.sleep(0.05)
                        continue

                    # Clear concept cache between days
                    concept_mapper.clear_cache()

                # Summary
                summary_layers = {}
                for lname in LAYER_NAMES:
                    rets = all_layer_returns[lname]
                    counts = all_layer_counts[lname]
                    if rets:
                        avg_r = sum(rets) / len(rets)
                        win_r = sum(1 for r in rets if r > 0) / len(rets) * 100
                        med_r = stat_median(rets)
                    else:
                        avg_r = win_r = med_r = 0.0
                    avg_count = sum(counts) / len(counts) if counts else 0.0

                    summary_layers[lname] = {
                        "avg_count": round(avg_count, 1),
                        "avg_return": round(avg_r, 2),
                        "win_rate": round(win_r, 1),
                        "median_return": round(med_r, 2),
                    }

                # Conclusions
                conclusions = []
                pairs = [
                    (LAYER_NAMES[0], LAYER_NAMES[1], "涨幅筛选"),
                    (LAYER_NAMES[1], LAYER_NAMES[2], "动量质量过滤"),
                    (LAYER_NAMES[2], LAYER_NAMES[3], "冲高回落过滤"),
                    (LAYER_NAMES[3], LAYER_NAMES[4], "最终推荐"),
                ]
                for prev_n, curr_n, label in pairs:
                    prev_r = summary_layers[prev_n]["avg_return"]
                    curr_r = summary_layers[curr_n]["avg_return"]
                    diff = curr_r - prev_r
                    if all_layer_returns[prev_n] and all_layer_returns[curr_n]:
                        if diff > 0.05:
                            verdict = "positive"
                        elif diff < -0.05:
                            verdict = "negative"
                        else:
                            verdict = "neutral"
                    else:
                        verdict = "no_data"
                    conclusions.append(
                        {
                            "filter": label,
                            "prev_return": round(prev_r, 2),
                            "curr_return": round(curr_r, 2),
                            "diff": round(diff, 2),
                            "verdict": verdict,
                        }
                    )

                # Filtered-out best stocks per layer transition (by day)
                filtered_out_best = []
                transitions = [
                    (LAYER_NAMES[0], LAYER_NAMES[1], "L0→L1 涨幅筛选"),
                    (LAYER_NAMES[1], LAYER_NAMES[2], "L1→L2 动量质量"),
                    (LAYER_NAMES[2], LAYER_NAMES[3], "L2→L3 冲高回落"),
                    (LAYER_NAMES[3], LAYER_NAMES[4], "L3→L4 最终推荐"),
                ]
                for prev_n, curr_n, label in transitions:
                    from collections import defaultdict

                    prev_by_day: dict[str, list[dict]] = defaultdict(list)
                    curr_codes_by_day: dict[str, set[str]] = defaultdict(set)
                    for item in all_layer_detail[prev_n]:
                        prev_by_day[item["trade_date"]].append(item)
                    for item in all_layer_detail[curr_n]:
                        curr_codes_by_day[item["trade_date"]].add(item["stock_code"])

                    # Build per-day breakdown
                    daily_data: list[dict] = []
                    all_filtered: list[dict] = []
                    for day_str in sorted(prev_by_day.keys()):
                        items = prev_by_day[day_str]
                        curr_codes = curr_codes_by_day.get(day_str, set())
                        day_filtered = [it for it in items if it["stock_code"] not in curr_codes]
                        all_filtered.extend(day_filtered)
                        if not day_filtered:
                            daily_data.append(
                                {
                                    "trade_date": day_str,
                                    "filtered_count": 0,
                                    "avg_return": 0,
                                    "positive_count": 0,
                                    "top_stocks": [],
                                }
                            )
                            continue
                        d_rets = [f["return_pct"] for f in day_filtered]
                        d_avg = sum(d_rets) / len(d_rets)
                        d_pos = sum(1 for r in d_rets if r > 0)
                        d_top3 = sorted(
                            day_filtered,
                            key=lambda x: x["return_pct"],
                            reverse=True,
                        )[:3]
                        daily_data.append(
                            {
                                "trade_date": day_str,
                                "filtered_count": len(day_filtered),
                                "avg_return": round(d_avg, 2),
                                "positive_count": d_pos,
                                "top_stocks": d_top3,
                            }
                        )

                    # Overall summary for this layer
                    if not all_filtered:
                        filtered_out_best.append(
                            {
                                "label": label,
                                "total_filtered": 0,
                                "avg_return": 0,
                                "positive_count": 0,
                                "days": daily_data,
                            }
                        )
                    else:
                        rets = [f["return_pct"] for f in all_filtered]
                        avg_r = sum(rets) / len(rets)
                        pos_count = sum(1 for r in rets if r > 0)
                        filtered_out_best.append(
                            {
                                "label": label,
                                "total_filtered": len(all_filtered),
                                "avg_return": round(avg_r, 2),
                                "positive_count": pos_count,
                                "days": daily_data,
                            }
                        )

                yield sse(
                    {
                        "type": "complete",
                        "days_processed": days_processed,
                        "summary": summary_layers,
                        "conclusions": conclusions,
                        "filtered_out_best": filtered_out_best,
                    }
                )

            except Exception as e:
                logger.error(f"Funnel analysis error: {e}", exc_info=True)
                yield sse({"type": "error", "message": f"分析出错: {str(e)}"})

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    @router.post("/api/momentum/combined-analysis")
    async def run_combined_analysis(request: Request, body: CombinedAnalysisRequest):
        """Run combined range backtest + funnel analysis with SSE streaming.

        Single pass: builds funnel layers per day AND simulates capital trading
        on the L3 recommendation. Eliminates duplicate iFinD calls.
        """
        import json
        import math
        from datetime import datetime
        from statistics import median as stat_median

        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.filters.momentum_quality_filter import (
            MomentumQualityConfig,
            MomentumQualityFilter,
        )
        from src.strategy.filters.stock_filter import create_main_board_only_filter
        from src.strategy.strategies.momentum_sector_scanner import (
            MomentumSectorScanner,
            SelectedStock,
        )

        # Validate dates
        try:
            start_date = datetime.strptime(body.start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(body.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD")

        if end_date <= start_date:
            raise HTTPException(status_code=400, detail="结束日期必须晚于起始日期")

        if body.initial_capital < 1000:
            raise HTTPException(status_code=400, detail="起始资金不能低于 1000 元")

        LAYER_NAMES = [
            "L0: 全部成分股",
            "L1: 涨幅>0.56%",
            "L2: 动量质量过滤",
            "L3: 冲高回落过滤",
            "L4: 最终推荐",
        ]

        concept_mapper = LocalConceptMapper()
        use_akshare = body.data_source == "akshare"
        if use_akshare:
            from src.data.clients.akshare_backtest_cache import (
                AkshareHistoricalAdapter,
            )

            akshare_cache = getattr(request.app.state, "akshare_cache", None)
            if not akshare_cache or not akshare_cache.is_ready:
                raise HTTPException(status_code=400, detail="请先预下载 akshare 数据")
            ifind_client = AkshareHistoricalAdapter(akshare_cache)
        else:
            ifind_client = _get_ifind_client(request)
            akshare_cache = None

        fundamentals_db = _get_fundamentals_db(request)
        news_checker = await _create_news_checker() if body.news_check else None

        async def event_stream():
            def sse(data: dict) -> str:
                return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

            try:
                from datetime import timedelta

                trading_days_all = _get_trading_calendar_akshare(
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
                            "message": (
                                "已截断至前 250 个交易日"
                                f" ({days_in_range[0]} ~ {days_in_range[-1]})"
                            ),
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

                # concept_mapper already created above (LocalConceptMapper)
                stock_filter = create_main_board_only_filter()

                # Funnel accumulators
                all_layer_returns: dict[str, list[float]] = {n: [] for n in LAYER_NAMES}
                all_layer_counts: dict[str, list[int]] = {n: [] for n in LAYER_NAMES}
                all_layer_detail: dict[str, list[dict]] = {n: [] for n in LAYER_NAMES}
                days_processed = 0

                # Backtest state
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
                        if use_akshare:
                            date_key = trade_date.strftime("%Y-%m-%d")
                            price_snapshots = _build_snapshots_from_cache(akshare_cache, date_key)
                            price_err = (
                                "" if price_snapshots else f"akshare缓存中无 {date_key} 的数据"
                            )
                        else:
                            (
                                price_snapshots,
                                price_err,
                            ) = await _parse_iwencai_and_fetch_prices_for_date(
                                ifind_client, trade_date
                            )
                        if not price_snapshots:
                            skip_reason = price_err or "无价格数据"
                            day_results.append(
                                {
                                    "trade_date": str(trade_date),
                                    "has_trade": False,
                                    "skip_reason": skip_reason,
                                    "capital": round(capital, 2),
                                }
                            )
                            yield sse(
                                {
                                    "type": "day_skip",
                                    "trade_date": str(trade_date),
                                    "reason": skip_reason,
                                }
                            )
                            await asyncio.sleep(0.05)
                            continue

                        scanner = MomentumSectorScanner(
                            ifind_client=ifind_client,
                            fundamentals_db=fundamentals_db,
                            concept_mapper=concept_mapper,
                            stock_filter=stock_filter,
                            negative_news_checker=news_checker,
                        )
                        scanner._trade_date = trade_date

                        gainers = await scanner._step1_filter_gainers(price_snapshots)
                        if not gainers:
                            day_results.append(
                                {
                                    "trade_date": str(trade_date),
                                    "has_trade": False,
                                    "skip_reason": "无初筛股",
                                    "capital": round(capital, 2),
                                }
                            )
                            yield sse(
                                {
                                    "type": "day_skip",
                                    "trade_date": str(trade_date),
                                    "reason": "无初筛股",
                                }
                            )
                            await asyncio.sleep(0.05)
                            continue

                        stock_boards = await scanner._step2_reverse_lookup(list(gainers.keys()))
                        hot_boards = scanner._step3_find_hot_boards(stock_boards)
                        if not hot_boards:
                            day_results.append(
                                {
                                    "trade_date": str(trade_date),
                                    "has_trade": False,
                                    "skip_reason": "无热门板块",
                                    "capital": round(capital, 2),
                                }
                            )
                            yield sse(
                                {
                                    "type": "day_skip",
                                    "trade_date": str(trade_date),
                                    "reason": "无热门板块",
                                }
                            )
                            await asyncio.sleep(0.05)
                            continue

                        board_constituents = await scanner._step4_get_constituents(
                            list(hot_boards.keys())
                        )

                        # Build layers L0, L1
                        all_constituent_codes: set[str] = set()
                        filtered_bc: dict[str, list[tuple[str, str]]] = {}
                        for bn, stocks in board_constituents.items():
                            allowed = [(c, n) for c, n in stocks if stock_filter.is_allowed(c)]
                            filtered_bc[bn] = allowed
                            for c, _ in allowed:
                                all_constituent_codes.add(c)

                        # Filter out ST stocks
                        if all_constituent_codes:
                            non_st = set(
                                await fundamentals_db.batch_filter_st(list(all_constituent_codes))
                            )
                            filtered_bc = {
                                bn: [(c, n) for c, n in stocks if c in non_st]
                                for bn, stocks in filtered_bc.items()
                            }
                            all_constituent_codes &= non_st

                        missing = [c for c in all_constituent_codes if c not in price_snapshots]
                        if missing:
                            extra = await scanner._fetch_constituent_prices(missing)
                            price_snapshots = {**price_snapshots, **extra}

                        l0: list[SelectedStock] = []
                        l1: list[SelectedStock] = []

                        for bn, stocks in filtered_bc.items():
                            for code, name in stocks:
                                snap = price_snapshots.get(code)
                                if not snap:
                                    continue

                                ss = SelectedStock(
                                    stock_code=code,
                                    stock_name=name,
                                    board_name=bn,
                                    open_gain_pct=snap.open_gain_pct,
                                    pe_ttm=0.0,
                                    board_avg_pe=0.0,
                                )
                                l0.append(ss)

                                threshold = MomentumSectorScanner.GAIN_FROM_OPEN_THRESHOLD
                                if snap.gain_from_open_pct >= threshold:
                                    l1.append(ss)

                        def _dedup(stocks):
                            seen = {}
                            for s in stocks:
                                ex = seen.get(s.stock_code)
                                if ex is None or s.open_gain_pct > ex.open_gain_pct:
                                    seen[s.stock_code] = s
                            return list(seen.values())

                        l0 = _dedup(l0)
                        l1 = _dedup(l1)

                        # L2: momentum quality filter
                        quality_config = MomentumQualityConfig(enabled=body.quality_filter)
                        quality_filter_inst = MomentumQualityFilter(ifind_client, quality_config)
                        if body.quality_filter and l1:
                            l2, qa2 = await quality_filter_inst.filter_stocks(
                                l1, price_snapshots, trade_date
                            )
                        else:
                            l2 = list(l1)
                            qa2 = []

                        # Always fetch trend data for Step 6 scoring, even if quality filter is off
                        if not qa2 and l1:
                            qa2_data = await quality_filter_inst._fetch_historical_data(
                                [s.stock_code for s in l1], trade_date
                            )
                            trend_data_l2 = {
                                code: v["trend_pct"]
                                for code, v in qa2_data.items()
                                if v.get("trend_pct") is not None
                            }
                        else:
                            trend_data_l2 = {
                                a.stock_code: a.trend_pct for a in qa2 if a.trend_pct is not None
                            }

                        # L3: reversal factor filter (冲高回落)
                        from src.strategy.filters.reversal_factor_filter import (
                            ReversalFactorConfig,
                            ReversalFactorFilter,
                        )

                        reversal_config = ReversalFactorConfig(enabled=True)
                        reversal_filter_inst = ReversalFactorFilter(reversal_config)
                        if l2:
                            l3, _ = await reversal_filter_inst.filter_stocks(
                                l2, price_snapshots, trade_date
                            )
                        else:
                            l3 = list(l2)

                        # L4: recommendation
                        l4 = []
                        rec = None
                        if l3:
                            rec = await scanner._step6_recommend(l3, price_snapshots, trend_data_l2)
                            if rec:
                                l4 = [
                                    SelectedStock(
                                        stock_code=rec.stock_code,
                                        stock_name=rec.stock_name,
                                        board_name=rec.board_name,
                                        open_gain_pct=rec.open_gain_pct,
                                        pe_ttm=rec.pe_ttm,
                                        board_avg_pe=rec.board_avg_pe,
                                    )
                                ]

                        all_layers = [l0, l1, l2, l3, l4]

                        # Fetch revenue growth for L3 stocks from DB
                        day_revenue_growth: dict[str, float] = {}
                        if l3:
                            l3_codes = [s.stock_code for s in l3]
                            day_revenue_growth = await fundamentals_db.batch_get_revenue_growth(
                                l3_codes
                            )

                        # Collect all codes for T+1 fetch
                        all_codes = set()
                        for layer in all_layers:
                            for s in layer:
                                all_codes.add(s.stock_code)

                        if not all_codes:
                            day_results.append(
                                {
                                    "trade_date": str(trade_date),
                                    "has_trade": False,
                                    "skip_reason": "成分股无价格",
                                    "capital": round(capital, 2),
                                }
                            )
                            yield sse(
                                {
                                    "type": "day_skip",
                                    "trade_date": str(trade_date),
                                    "reason": "成分股无价格",
                                }
                            )
                            await asyncio.sleep(0.05)
                            continue

                        # Fetch T+1 open prices for all layer stocks
                        next_open = await _fetch_batch_prices(
                            ifind_client,
                            list(all_codes),
                            next_trade_date,
                            indicator="open",
                        )

                        # === Funnel: calculate returns per layer ===
                        day_layers = {}
                        for lname, lstocks in zip(LAYER_NAMES, all_layers):
                            returns = []
                            for s in lstocks:
                                snap = price_snapshots.get(s.stock_code)
                                sell_p = next_open.get(s.stock_code)
                                if snap and sell_p and snap.latest_price > 0:
                                    ret = _calc_net_return_pct(snap.latest_price, sell_p)
                                    ret_rounded = round(ret, 2)
                                    returns.append(ret_rounded)
                                    detail_entry = {
                                        "trade_date": str(trade_date),
                                        "stock_code": s.stock_code,
                                        "stock_name": s.stock_name,
                                        "board_name": s.board_name,
                                        "return_pct": ret_rounded,
                                    }
                                    rg = day_revenue_growth.get(s.stock_code)
                                    if rg is not None:
                                        detail_entry["revenue_growth"] = round(rg, 2)
                                    all_layer_detail[lname].append(detail_entry)

                            count = len(lstocks)
                            all_layer_counts[lname].append(count)
                            all_layer_returns[lname].extend(returns)

                            if returns:
                                avg_r = sum(returns) / len(returns)
                                win_r = sum(1 for r in returns if r > 0) / len(returns) * 100
                                med_r = stat_median(returns)
                            else:
                                avg_r = win_r = med_r = 0.0

                            day_layers[lname] = {
                                "count": count,
                                "avg_return": round(avg_r, 2),
                                "win_rate": round(win_r, 1),
                                "median_return": round(med_r, 2),
                            }

                        # === Backtest: capital tracking on L4 recommendation ===
                        day_backtest: dict = {
                            "trade_date": str(trade_date),
                            "has_trade": False,
                            "capital": round(capital, 2),
                        }

                        if rec:
                            rec_snap = price_snapshots.get(rec.stock_code)
                            buy_price = 0.0
                            if rec_snap:
                                buy_price = rec_snap.latest_price
                            if buy_price <= 0 and rec_snap:
                                buy_price = rec_snap.open_price

                            sell_price_val = next_open.get(rec.stock_code, 0.0)
                            if sell_price_val <= 0:
                                try:
                                    sell_prices = await _fetch_stock_open_prices(
                                        ifind_client, rec.stock_code, next_trade_date
                                    )
                                    if sell_prices:
                                        sell_price_val = sell_prices[0][1]
                                except Exception:
                                    pass

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
                                        "buy_amount": round(buy_amount, 2),
                                        "buy_commission": round(buy_commission, 2),
                                        "buy_transfer": round(buy_transfer, 2),
                                        "sell_amount": round(sell_amount, 2),
                                        "sell_commission": round(sell_commission, 2),
                                        "sell_transfer": round(sell_transfer, 2),
                                        "sell_stamp": round(sell_stamp, 2),
                                        "profit": round(trade_profit, 2),
                                        "return_pct": round(trade_return_pct, 2),
                                        "capital_before": round(capital_before, 2),
                                        "capital": round(capital, 2),
                                        "hot_boards": {
                                            name: list(codes) for name, codes in hot_boards.items()
                                        },
                                    }
                                else:
                                    day_backtest["skip_reason"] = (
                                        f"资金不足 (需 {buy_price * 100:.0f} 元/手)"
                                    )
                                    day_backtest["stock_code"] = rec.stock_code
                                    day_backtest["stock_name"] = rec.stock_name
                                    day_backtest["capital"] = round(capital, 2)
                            else:
                                reason_parts = []
                                if buy_price <= 0:
                                    reason_parts.append("无买入价")
                                if sell_price_val <= 0:
                                    reason_parts.append("无次日开盘卖出价")
                                day_backtest["skip_reason"] = "、".join(reason_parts)
                                day_backtest["stock_code"] = rec.stock_code
                                day_backtest["stock_name"] = rec.stock_name
                                day_backtest["capital"] = round(capital, 2)
                        else:
                            day_backtest["skip_reason"] = "无推荐"
                            day_backtest["capital"] = round(capital, 2)

                        if rec and rec.news_check_passed is not None:
                            day_backtest["news_check_passed"] = rec.news_check_passed
                            day_backtest["news_check_detail"] = rec.news_check_detail

                        day_results.append(day_backtest)
                        days_processed += 1

                        yield sse(
                            {
                                "type": "day_result",
                                "trade_date": str(trade_date),
                                "backtest": day_backtest,
                                "funnel": day_layers,
                            }
                        )
                        await asyncio.sleep(0.05)

                    except Exception as e:
                        logger.error(
                            f"Combined analysis error on {trade_date}: {e}",
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

                # === Funnel summary ===
                summary_layers = {}
                for lname in LAYER_NAMES:
                    rets = all_layer_returns[lname]
                    counts = all_layer_counts[lname]
                    if rets:
                        avg_r = sum(rets) / len(rets)
                        win_r = sum(1 for r in rets if r > 0) / len(rets) * 100
                        med_r = stat_median(rets)
                    else:
                        avg_r = win_r = med_r = 0.0
                    avg_count = sum(counts) / len(counts) if counts else 0.0

                    summary_layers[lname] = {
                        "avg_count": round(avg_count, 1),
                        "avg_return": round(avg_r, 2),
                        "win_rate": round(win_r, 1),
                        "median_return": round(med_r, 2),
                    }

                conclusions = []
                pairs = [
                    (LAYER_NAMES[0], LAYER_NAMES[1], "涨幅筛选"),
                    (LAYER_NAMES[1], LAYER_NAMES[2], "动量质量过滤"),
                    (LAYER_NAMES[2], LAYER_NAMES[3], "冲高回落过滤"),
                    (LAYER_NAMES[3], LAYER_NAMES[4], "最终推荐"),
                ]
                for prev_n, curr_n, label in pairs:
                    prev_r = summary_layers[prev_n]["avg_return"]
                    curr_r = summary_layers[curr_n]["avg_return"]
                    diff = curr_r - prev_r
                    if all_layer_returns[prev_n] and all_layer_returns[curr_n]:
                        if diff > 0.05:
                            verdict = "positive"
                        elif diff < -0.05:
                            verdict = "negative"
                        else:
                            verdict = "neutral"
                    else:
                        verdict = "no_data"
                    conclusions.append(
                        {
                            "filter": label,
                            "prev_return": round(prev_r, 2),
                            "curr_return": round(curr_r, 2),
                            "diff": round(diff, 2),
                            "verdict": verdict,
                        }
                    )

                # Filtered-out best stocks
                filtered_out_best = []
                transitions = [
                    (LAYER_NAMES[0], LAYER_NAMES[1], "L0→L1 涨幅筛选"),
                    (LAYER_NAMES[1], LAYER_NAMES[2], "L1→L2 动量质量"),
                    (LAYER_NAMES[2], LAYER_NAMES[3], "L2→L3 冲高回落"),
                    (LAYER_NAMES[3], LAYER_NAMES[4], "L3→L4 最终推荐"),
                ]
                for prev_n, curr_n, label in transitions:
                    from collections import defaultdict

                    prev_by_day: dict[str, list[dict]] = defaultdict(list)
                    curr_codes_by_day: dict[str, set[str]] = defaultdict(set)
                    for item in all_layer_detail[prev_n]:
                        prev_by_day[item["trade_date"]].append(item)
                    for item in all_layer_detail[curr_n]:
                        curr_codes_by_day[item["trade_date"]].add(item["stock_code"])

                    daily_data: list[dict] = []
                    all_filtered: list[dict] = []
                    for day_str in sorted(prev_by_day.keys()):
                        items = prev_by_day[day_str]
                        curr_codes = curr_codes_by_day.get(day_str, set())
                        day_filtered = [it for it in items if it["stock_code"] not in curr_codes]
                        all_filtered.extend(day_filtered)
                        if not day_filtered:
                            daily_data.append(
                                {
                                    "trade_date": day_str,
                                    "filtered_count": 0,
                                    "avg_return": 0,
                                    "positive_count": 0,
                                    "top_stocks": [],
                                }
                            )
                            continue
                        d_rets = [f["return_pct"] for f in day_filtered]
                        d_avg = sum(d_rets) / len(d_rets)
                        d_pos = sum(1 for r in d_rets if r > 0)
                        d_top3 = sorted(
                            day_filtered,
                            key=lambda x: x["return_pct"],
                            reverse=True,
                        )[:3]
                        daily_data.append(
                            {
                                "trade_date": day_str,
                                "filtered_count": len(day_filtered),
                                "avg_return": round(d_avg, 2),
                                "positive_count": d_pos,
                                "top_stocks": d_top3,
                            }
                        )

                    if not all_filtered:
                        filtered_out_best.append(
                            {
                                "label": label,
                                "total_filtered": 0,
                                "avg_return": 0,
                                "positive_count": 0,
                                "days": daily_data,
                            }
                        )
                    else:
                        rets = [f["return_pct"] for f in all_filtered]
                        avg_r = sum(rets) / len(rets)
                        pos_count = sum(1 for r in rets if r > 0)
                        filtered_out_best.append(
                            {
                                "label": label,
                                "total_filtered": len(all_filtered),
                                "avg_return": round(avg_r, 2),
                                "positive_count": pos_count,
                                "days": daily_data,
                            }
                        )

                # === Backtest summary ===
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
                    "total_commission": round(
                        sum(
                            d.get("buy_commission", 0) + d.get("sell_commission", 0)
                            for d in trade_results
                        ),
                        2,
                    ),
                    "total_stamp_tax": round(sum(d.get("sell_stamp", 0) for d in trade_results), 2),
                }

                yield sse(
                    {
                        "type": "complete",
                        "days_processed": days_processed,
                        "backtest_summary": backtest_summary,
                        "funnel_summary": summary_layers,
                        "conclusions": conclusions,
                        "filtered_out_best": filtered_out_best,
                    }
                )

            except Exception as e:
                logger.error(f"Combined analysis error: {e}", exc_info=True)
                yield sse({"type": "error", "message": f"分析出错: {str(e)}"})
            finally:
                await _stop_news_checker(news_checker)

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    return router


async def _fetch_board_trend(ifind_client, board_name: str, trade_date_str: str) -> dict:
    """Fetch board/concept index intraday trend data for a given date.

    Tries to find the concept board index via iwencai, then fetches
    intraday high-frequency data. Falls back to history_quotes for
    open/close if high_frequency fails.

    Returns dict with: open_gain, gain_940, day_gain, max_gain, min_gain
    """
    # Try to find board index code via iwencai
    board_index_code = ""
    try:
        result = await ifind_client.smart_stock_picking(f"{board_name} 板块指数代码", "zhishu")
        tables = result.get("tables", [])
        if tables:
            for tw in tables:
                table = tw.get("table", tw) if isinstance(tw, dict) else {}
                for col_name, col_data in table.items():
                    if "代码" in col_name and col_data:
                        code = str(col_data[0]).strip()
                        if code:
                            board_index_code = code
                            break
                if board_index_code:
                    break
    except Exception as e:
        logger.warning(f"iwencai board index lookup failed for {board_name}: {e}")

    if not board_index_code:
        return {"error": "无法获取板块指数代码", "board_name": board_name}

    # Fetch intraday high-frequency data for the board index
    start_time = f"{trade_date_str} 09:30:00"
    end_time = f"{trade_date_str} 15:00:00"

    try:
        hf_data = await ifind_client.high_frequency(
            codes=board_index_code,
            indicators="close",
            start_time=start_time,
            end_time=end_time,
            function_para={"Interval": "1"},
        )

        tables = hf_data.get("tables", [])
        if not tables:
            raise ValueError(f"No high_frequency data for {board_index_code}")

        tbl = tables[0].get("table", {})
        close_vals = tbl.get("close", [])

        if not close_vals:
            raise ValueError(f"Empty close data for {board_index_code}")

    except Exception as e:
        logger.warning(f"Board high_frequency failed for {board_index_code}: {e}")
        # Fallback: use history_quotes for basic open/close data
        try:
            hist_data = await ifind_client.history_quotes(
                codes=board_index_code,
                indicators="open,close,preClose,high,low",
                start_date=trade_date_str,
                end_date=trade_date_str,
            )
            tables = hist_data.get("tables", [])
            if tables:
                tbl = tables[0].get("table", {})
                open_p = float(tbl.get("open", [0])[0] or 0)
                close_p = float(tbl.get("close", [0])[0] or 0)
                prev_c = float(tbl.get("preClose", [0])[0] or 0)
                high_p = float(tbl.get("high", [0])[0] or 0)
                low_p = float(tbl.get("low", [0])[0] or 0)
                if prev_c > 0:
                    return {
                        "board_index_code": board_index_code,
                        "open_gain": round((open_p / prev_c - 1) * 100, 2),
                        "gain_940": None,
                        "day_gain": round((close_p / prev_c - 1) * 100, 2),
                        "max_gain": round((high_p / prev_c - 1) * 100, 2),
                        "min_gain": round((low_p / prev_c - 1) * 100, 2),
                    }
        except Exception as e2:
            logger.warning(f"Board history_quotes also failed: {e2}")
        return {"error": f"板块数据获取失败: {str(e)[:50]}", "board_name": board_name}

    # Get prev_close for the board index
    prev_close = 0.0
    try:
        hist_data = await ifind_client.history_quotes(
            codes=board_index_code,
            indicators="preClose,open",
            start_date=trade_date_str,
            end_date=trade_date_str,
        )
        tables_h = hist_data.get("tables", [])
        if tables_h:
            tbl_h = tables_h[0].get("table", {})
            prev_vals = tbl_h.get("preClose", [])
            if prev_vals:
                prev_close = float(prev_vals[0])
    except Exception:
        pass

    if prev_close <= 0:
        return {"error": "无法获取板块前收盘价", "board_index_code": board_index_code}

    # Calculate gains from intraday data
    prices = [float(v) for v in close_vals if v is not None]
    if not prices:
        return {"error": "板块分钟线数据为空"}

    open_price = prices[0]
    close_price = prices[-1]
    max_price = max(prices)
    min_price = min(prices)

    # Find 9:40 price (approximately the 10th minute bar)
    price_940 = prices[min(9, len(prices) - 1)]

    return {
        "board_index_code": board_index_code,
        "open_gain": round((open_price / prev_close - 1) * 100, 2),
        "gain_940": round((price_940 / prev_close - 1) * 100, 2),
        "day_gain": round((close_price / prev_close - 1) * 100, 2),
        "max_gain": round((max_price / prev_close - 1) * 100, 2),
        "min_gain": round((min_price / prev_close - 1) * 100, 2),
    }


async def _fetch_stock_day_trend(ifind_client, stock_code: str, trade_date_str: str) -> dict:
    """Fetch individual stock's full-day data: open, high, low, close, prev_close."""
    suffix = ".SH" if stock_code.startswith("6") else ".SZ"
    code = f"{stock_code}{suffix}"

    data = await ifind_client.history_quotes(
        codes=code,
        indicators="open,high,low,close,preClose",
        start_date=trade_date_str,
        end_date=trade_date_str,
    )
    tables = data.get("tables", [])
    if not tables:
        return {}

    tbl = tables[0].get("table", {})
    open_p = float(tbl.get("open", [0])[0] or 0)
    high_p = float(tbl.get("high", [0])[0] or 0)
    low_p = float(tbl.get("low", [0])[0] or 0)
    close_p = float(tbl.get("close", [0])[0] or 0)
    prev_c = float(tbl.get("preClose", [0])[0] or 0)

    if prev_c <= 0:
        return {"open": open_p, "high": high_p, "low": low_p, "close": close_p}

    return {
        "open": open_p,
        "high": high_p,
        "low": low_p,
        "close": close_p,
        "prev_close": prev_c,
        "open_gain": round((open_p / prev_c - 1) * 100, 2),
        "day_gain": round((close_p / prev_c - 1) * 100, 2),
        "max_gain": round((high_p / prev_c - 1) * 100, 2),
        "min_gain": round((low_p / prev_c - 1) * 100, 2),
    }


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


async def _call_llm_stock_analysis(
    trade: dict, board_data: dict, stock_day_data: dict, trigger_codes: list[str]
) -> str:
    """Call Silicon Flow LLM to analyze why a specific trade lost money."""
    import httpx

    api_key = _get_llm_api_key()
    if not api_key:
        return "LLM API key 未配置"

    system_prompt = (
        "你是一个A股量化交易分析师。请分析以下亏损交易的原因。\n\n"
        "策略说明：动量板块策略\n"
        "1. 预筛开盘涨幅>-0.5%的沪深主板非ST股票\n"
        "2. 9:40时筛选(9:40价-开盘价)/开盘价>0.56%的股票\n"
        "3. 反查概念板块，找到有>=2只入选股的【热门概念板块】\n"
        "4. 拉取热门板块全部成分股，再筛9:40 vs开盘>0.56%\n"
        "5. 过滤假突破（下跌趋势+低换手）和冲高回落（从高点大幅回落）\n"
        "6. 在最大板块中排除涨停股，按 Z(开盘涨幅)-Z(营收增长率) 打分，"
        "选开盘涨幅高+营收增长率低的（纯资金/题材驱动，短线延续性更强）\n"
        "7. 买入价为9:40价格，次日开盘卖出。"
    )

    # Build board trend text (only when data available)
    board_text = ""
    if board_data and "error" not in board_data:
        parts = []
        if board_data.get("open_gain") is not None:
            parts.append(f"开盘涨幅: {board_data['open_gain']:+.2f}%")
        if board_data.get("gain_940") is not None:
            parts.append(f"9:40涨幅: {board_data['gain_940']:+.2f}%")
        if board_data.get("day_gain") is not None:
            parts.append(f"全天涨幅: {board_data['day_gain']:+.2f}%")
        if board_data.get("max_gain") is not None:
            parts.append(f"盘中最高: {board_data['max_gain']:+.2f}%")
        if board_data.get("min_gain") is not None:
            parts.append(f"盘中最低: {board_data['min_gain']:+.2f}%")
        if parts:
            board_text = "\n板块当日走势：\n" + "\n".join(f"- {p}" for p in parts)

    # Build stock day trend text
    stock_text = ""
    if stock_day_data and stock_day_data.get("prev_close"):
        stock_text = (
            f"\n个股当日走势：\n"
            f"- 开盘涨幅: {stock_day_data.get('open_gain', 0):+.2f}%\n"
            f"- 全天涨幅: {stock_day_data.get('day_gain', 0):+.2f}%\n"
            f"- 盘中最高: {stock_day_data.get('max_gain', 0):+.2f}%\n"
            f"- 盘中最低: {stock_day_data.get('min_gain', 0):+.2f}%"
        )

    trigger_text = ", ".join(trigger_codes) if trigger_codes else "未知"

    user_prompt = (
        f"亏损交易信息：\n"
        f"- 股票：{trade['stock_code']} {trade.get('stock_name', '')}\n"
        f"- 所属板块：{trade.get('board_name', '')}\n"
        f"- 买入日期：{trade['trade_date']}，买入价：{trade.get('buy_price', 0)}\n"
        f"- 卖出日期：{trade.get('sell_date', '')}，卖出价：{trade.get('sell_price', 0)}\n"
        f"- 亏损：{trade.get('profit', 0):.2f}元（{trade.get('return_pct', 0):+.2f}%）\n"
        f"- 该板块触发股票（9:40 vs 开盘>0.56%）：{trigger_text}\n"
        f"{board_text}"
        f"{stock_text}\n\n"
        f"请分析这笔交易亏损的可能原因（2-3句话），重点关注：\n"
        f"1. 个股当日走势是否冲高回落？\n"
        f"2. 个股是否有特殊情况？\n"
        f"3. 选股逻辑在这个场景下的缺陷？"
    )

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            "https://api.siliconflow.cn/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "Qwen/Qwen2.5-72B-Instruct",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "max_tokens": 800,
                "temperature": 0.3,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]


async def _call_llm_strategy_summary(analyses: list[dict]) -> str:
    """Call Silicon Flow LLM to summarize strategy weaknesses from all losing trades."""
    import httpx

    api_key = _get_llm_api_key()
    if not api_key:
        return "LLM API key 未配置"

    # Build summary of all analyses
    trade_summaries = []
    for a in analyses:
        board_info = ""
        bd = a.get("board_data", {})
        if bd and "error" not in bd:
            board_info = f"板块全天{bd.get('day_gain', '?')}%(最高{bd.get('max_gain', '?')}%)"

        trade_summaries.append(
            f"- {a['stock_code']} {a.get('stock_name', '')} | "
            f"板块:{a.get('board_name', '')} | "
            f"亏损{a.get('return_pct', 0):+.2f}% | "
            f"{board_info}\n"
            f"  分析: {a.get('llm_analysis', '无')[:200]}"
        )

    system_prompt = (
        "你是一个A股量化交易策略分析师。请基于以下亏损交易的逐笔分析，"
        "总结该策略的问题和改进建议。\n\n"
        "策略说明：动量板块策略\n"
        "1. 预筛开盘涨幅>-0.5%的沪深主板非ST股票\n"
        "2. 9:40时筛选(9:40价-开盘价)/开盘价>0.56%的股票\n"
        "3. 反查概念板块，找到有>=2只入选股的【热门概念板块】\n"
        "4. 拉取热门板块全部成分股，再筛9:40 vs开盘>0.56%\n"
        "5. 过滤假突破（下跌趋势+低换手）和冲高回落（从高点大幅回落）\n"
        "6. 在最大板块中排除涨停股，按 Z(开盘涨幅)-Z(营收增长率) 打分，"
        "选开盘涨幅高+营收增长率低的（纯资金/题材驱动，短线延续性更强）\n"
        "7. 买入价为9:40价格，次日开盘卖出。"
    )

    user_prompt = (
        f"回测期间共 {len(analyses)} 笔亏损交易：\n\n"
        + "\n\n".join(trade_summaries)
        + "\n\n请总结：\n"
        "1. 这些亏损交易的共性问题（2-3点）\n"
        "2. 策略的主要弱点\n"
        "3. 可能的改进方向（2-3条具体建议）"
    )

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            "https://api.siliconflow.cn/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "Qwen/Qwen2.5-72B-Instruct",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "max_tokens": 1500,
                "temperature": 0.3,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]


async def _parse_iwencai_and_fetch_prices(
    ifind_client, iwencai_result: dict, trade_date
) -> tuple[dict, str]:
    """Parse iwencai response and fetch historical prices via history_quotes.

    Returns:
        (snapshots_dict, error_reason) — error_reason is empty on success.
    """
    from src.data.clients.ifind_http_client import IFinDHttpError
    from src.strategy.strategies.momentum_sector_scanner import PriceSnapshot

    tables = iwencai_result.get("tables", [])
    if not tables:
        return {}, "iwencai返回无tables"

    raw_codes: list[str] = []
    names_map: dict[str, str] = {}

    for table_wrapper in tables:
        if not isinstance(table_wrapper, dict):
            continue
        table = table_wrapper.get("table", table_wrapper)
        if not isinstance(table, dict):
            continue

        codes = None
        names = None
        for col_name, col_data in table.items():
            if "代码" in col_name:
                codes = col_data
            elif "简称" in col_name or "名称" in col_name:
                names = col_data

        if not codes:
            continue

        for i in range(len(codes)):
            raw = str(codes[i]).strip()
            bare = raw
            for suffix in (".SZ", ".SH", ".BJ", ".sz", ".sh", ".bj"):
                if bare.endswith(suffix):
                    bare = bare[: -len(suffix)]
                    break
            if len(bare) == 6 and bare.isdigit():
                raw_codes.append(raw)
                name = str(names[i]).strip() if names and i < len(names) else ""
                names_map[bare] = name

    if not raw_codes:
        return {}, "iwencai结果中无有效股票代码"

    # Batch fetch prices
    snapshots: dict[str, PriceSnapshot] = {}
    batch_size = 50
    date_fmt = trade_date.strftime("%Y-%m-%d")
    last_price_err = ""

    for i in range(0, len(raw_codes), batch_size):
        batch = raw_codes[i : i + batch_size]
        formatted = []
        for raw in batch:
            if "." in raw:
                formatted.append(raw)
            else:
                suffix = ".SH" if raw.startswith("6") else ".SZ"
                formatted.append(f"{raw}{suffix}")

        codes_str = ",".join(formatted)

        try:
            data = await ifind_client.history_quotes(
                codes=codes_str,
                indicators="open,preClose",
                start_date=date_fmt,
                end_date=date_fmt,
            )

            for table_entry in data.get("tables", []):
                thscode = table_entry.get("thscode", "")
                bare = thscode.split(".")[0] if thscode else ""
                if not bare:
                    continue

                tbl = table_entry.get("table", {})
                open_vals = tbl.get("open", [])
                prev_vals = tbl.get("preClose", [])

                if open_vals and prev_vals:
                    open_price = float(open_vals[0])
                    prev_close = float(prev_vals[0])
                    if prev_close > 0:
                        snapshots[bare] = PriceSnapshot(
                            stock_code=bare,
                            stock_name=names_map.get(bare, ""),
                            open_price=open_price,
                            prev_close=prev_close,
                            latest_price=open_price,
                        )

        except IFinDHttpError as e:
            last_price_err = str(e)
            logger.error(f"history_quotes batch failed: {e}")
        except Exception as e:
            last_price_err = str(e)
            logger.error(f"history_quotes unexpected error: {e}")

    if not snapshots:
        reason = f"iwencai返回{len(raw_codes)}只股票，history_quotes失败"
        if last_price_err:
            reason += f": {last_price_err[:80]}"
        return {}, reason

    # Fetch 9:40 price and volume (for gain check, buy price, and gap-fade filter)
    data_940 = await _fetch_940_data_batch(ifind_client, list(snapshots.keys()), trade_date)
    for code, (price, volume, high, low) in data_940.items():
        if code in snapshots and price > 0:
            snapshots[code].latest_price = price
            snapshots[code].early_volume = volume
            snapshots[code].high_price = high
            snapshots[code].low_price = low

    return snapshots, ""


def _get_trading_calendar_akshare(start_date, end_date) -> list:
    """Get trading days via AKShare (tool_trade_date_hist_sina).

    Returns list of datetime.date in [start_date, end_date).
    Falls back to weekday generation on failure.
    """
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


async def _fetch_stock_open_prices(ifind_client, stock_code: str, target_date) -> list:
    """Fetch open price for a stock on a specific date (single-day query).

    Returns list of (date, open_price) tuples (always one element).
    """
    suffix = ".SH" if stock_code.startswith("6") else ".SZ"
    code = f"{stock_code}{suffix}"
    date_str = target_date.strftime("%Y-%m-%d")

    # Must use multiple indicators — iFinD returns empty tables for single-indicator queries.
    data = await ifind_client.history_quotes(
        codes=code,
        indicators="open,preClose",
        start_date=date_str,
        end_date=date_str,
    )
    tables = data.get("tables", [])
    if not tables:
        raise ValueError(f"iFinD returned empty tables for {code} ({date_str}): {data}")
    tbl = tables[0].get("table", {})
    opens = tbl.get("open", [])
    if not opens:
        raise ValueError(f"No open data for {code} ({date_str}): table={tbl}")
    return [(target_date, float(opens[0]))]


async def _fetch_stock_close_prices(ifind_client, stock_code: str, target_date) -> list:
    """Fetch close price for a stock on a specific date (single-day query).

    Returns list of (date, close_price) tuples (always one element).
    """
    suffix = ".SH" if stock_code.startswith("6") else ".SZ"
    code = f"{stock_code}{suffix}"
    date_str = target_date.strftime("%Y-%m-%d")

    # Must use multiple indicators — iFinD returns empty tables for single-indicator queries.
    data = await ifind_client.history_quotes(
        codes=code,
        indicators="close,preClose",
        start_date=date_str,
        end_date=date_str,
    )
    tables = data.get("tables", [])
    if not tables:
        raise ValueError(f"iFinD returned empty tables for {code} ({date_str}): {data}")
    tbl = tables[0].get("table", {})
    closes = tbl.get("close", [])
    if not closes:
        raise ValueError(f"No close data for {code} ({date_str}): table={tbl}")
    return [(target_date, float(closes[0]))]


async def _fetch_940_data_batch(
    ifind_client, stock_codes: list[str], trade_date
) -> dict[str, tuple[float, float, float, float]]:
    """Fetch 9:40 price, volume, high, low via high_frequency API (1-min bars).

    Returns:
        dict: stock_code → (price_at_940, cumulative_volume, max_high, min_low)
    """
    result: dict[str, tuple[float, float, float, float]] = {}
    batch_size = 50
    start_time = f"{trade_date} 09:30:00"
    end_time = f"{trade_date} 09:40:00"

    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i : i + batch_size]
        codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)

        try:
            data = await ifind_client.high_frequency(
                codes=codes_str,
                indicators="close,volume,high,low",
                start_time=start_time,
                end_time=end_time,
                function_para={"Interval": "1"},
            )

            for table_entry in data.get("tables", []):
                thscode = table_entry.get("thscode", "")
                bare_code = thscode.split(".")[0] if thscode else ""
                if not bare_code:
                    continue

                tbl = table_entry.get("table", {})
                close_vals = tbl.get("close", [])
                vol_vals = tbl.get("volume", [])
                high_vals = tbl.get("high", [])
                low_vals = tbl.get("low", [])

                price = 0.0
                cum_volume = 0.0
                max_high = 0.0
                min_low = 0.0

                if close_vals:
                    last_close = close_vals[-1]
                    if last_close is not None:
                        price = float(last_close)

                if vol_vals:
                    cum_volume = sum(float(v) for v in vol_vals if v is not None)

                if high_vals:
                    valid_highs = [float(v) for v in high_vals if v is not None]
                    if valid_highs:
                        max_high = max(valid_highs)

                if low_vals:
                    valid_lows = [float(v) for v in low_vals if v is not None]
                    if valid_lows:
                        min_low = min(valid_lows)

                if price > 0:
                    result[bare_code] = (price, cum_volume, max_high, min_low)

        except Exception as e:
            logger.warning(f"high_frequency 9:40 fetch failed for batch: {e}")

    return result


async def _fetch_stock_940_price(ifind_client, stock_code: str, target_date) -> list:
    """Fetch 9:40 price for a single stock. Returns [(date, price)] or empty list."""
    data = await _fetch_940_data_batch(ifind_client, [stock_code], target_date)
    if stock_code in data and data[stock_code][0] > 0:
        return [(target_date, data[stock_code][0])]
    return []


def _build_snapshots_from_cache(akshare_cache, date_str: str) -> dict:
    """Build PriceSnapshot dict from AkshareBacktestCache for a given date.

    Replaces the iwencai pre-filter + history_quotes + 9:40 fetch pipeline.
    Local filtering: open_gain_pct > -0.5% (same as iwencai query).
    """
    from src.strategy.strategies.momentum_sector_scanner import PriceSnapshot

    all_daily = akshare_cache.get_all_codes_with_daily(date_str)
    snapshots: dict[str, PriceSnapshot] = {}

    for code, day in all_daily.items():
        open_price = day.get("open", 0)
        prev_close = day.get("preClose", 0)
        if prev_close <= 0 or open_price <= 0:
            continue

        # Pre-filter: open gain > -0.5% (same as iwencai query)
        open_gain = (open_price - prev_close) / prev_close * 100
        if open_gain < -0.5:
            continue

        # 9:40 price from minute cache
        data_940 = akshare_cache.get_940_price(code, date_str)
        if data_940:
            latest_price, cum_vol, max_high, min_low = data_940
        else:
            latest_price = open_price
            cum_vol = 0.0
            max_high = 0.0
            min_low = 0.0

        snapshots[code] = PriceSnapshot(
            stock_code=code,
            stock_name="",
            open_price=open_price,
            prev_close=prev_close,
            latest_price=latest_price if latest_price > 0 else open_price,
            early_volume=cum_vol,
            high_price=max_high,
            low_price=min_low,
        )

    return snapshots


async def _parse_iwencai_and_fetch_prices_for_date(ifind_client, trade_date) -> tuple[dict, str]:
    """Convenience wrapper: run iwencai pre-filter + fetch prices for a date.

    Returns:
        (snapshots_dict, error_reason) — snapshots is empty dict on failure,
        error_reason is empty string on success or describes the failure.
    """
    from src.data.clients.ifind_http_client import IFinDHttpError

    date_str = trade_date.strftime("%Y%m%d")
    query = f"{date_str}开盘涨幅大于-0.5%的沪深主板非ST股票"
    try:
        iwencai_result = await ifind_client.smart_stock_picking(query, "stock")
    except IFinDHttpError as e:
        logger.error(f"iwencai query failed for {trade_date}: {e}")
        return {}, f"iwencai查询失败: {e}"
    except Exception as e:
        logger.error(f"iwencai query unexpected error for {trade_date}: {e}")
        return {}, f"iwencai异常: {e}"

    return await _parse_iwencai_and_fetch_prices(ifind_client, iwencai_result, trade_date)


def _calc_net_return_pct(buy_price: float, sell_price: float) -> float:
    """Calculate net return percentage after transaction costs (assuming 1 lot = 100 shares).

    Uses the same cost model as interval backtest:
    - Buy commission: max(0.3%, ¥5)
    - Sell commission: max(0.3%, ¥5)
    - Stamp tax (sell): 0.05%
    - Transfer fee: 0.001% each way
    """
    shares = 100  # 1 lot
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


async def _fetch_batch_prices(
    ifind_client, stock_codes: list[str], target_date, indicator: str = "close"
) -> dict[str, float]:
    """Fetch a single price indicator for multiple stocks on a single date.

    Args:
        indicator: Price field to fetch, e.g. "close" or "open".
    """
    from src.data.clients.ifind_http_client import IFinDHttpError

    result: dict[str, float] = {}
    batch_size = 50
    date_str = target_date.strftime("%Y-%m-%d")

    # iFinD returns empty tables for single-indicator queries, so always include preClose
    indicators_str = f"{indicator},preClose" if indicator != "preClose" else "close,preClose"

    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i : i + batch_size]
        codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)
        try:
            data = await ifind_client.history_quotes(
                codes=codes_str,
                indicators=indicators_str,
                start_date=date_str,
                end_date=date_str,
            )
            for table_entry in data.get("tables", []):
                thscode = table_entry.get("thscode", "")
                bare = thscode.split(".")[0] if thscode else ""
                if not bare:
                    continue
                tbl = table_entry.get("table", {})
                vals = tbl.get(indicator, [])
                if vals and vals[0] is not None:
                    result[bare] = float(vals[0])
        except IFinDHttpError as e:
            logger.warning(f"Batch {indicator} fetch failed for {target_date}: {e}")

    return result


async def _fetch_batch_close(ifind_client, stock_codes: list[str], target_date) -> dict[str, float]:
    """Fetch close prices for multiple stocks on a single date."""
    return await _fetch_batch_prices(ifind_client, stock_codes, target_date, indicator="close")


async def _run_momentum_scan_for_date(ifind_client, scanner, trade_date):
    """Run full momentum scan for a specific date. Returns ScanResult or None."""
    from src.data.clients.ifind_http_client import IFinDHttpError

    date_str = trade_date.strftime("%Y%m%d")
    query = f"{date_str}开盘涨幅大于-0.5%的沪深主板非ST股票"

    try:
        iwencai_result = await ifind_client.smart_stock_picking(query, "stock")
    except IFinDHttpError as e:
        logger.error(f"iwencai query failed for {trade_date}: {e}")
        return None

    price_snapshots, _price_err = await _parse_iwencai_and_fetch_prices(
        ifind_client, iwencai_result, trade_date
    )

    if not price_snapshots:
        return None

    return await scanner.scan(price_snapshots, trade_date=trade_date)


async def _run_intraday_monitor(state: dict) -> None:
    """
    Background task: intraday momentum monitor.

    Runs every trading day 9:30-9:40, polls for stocks with gain > -0.5%,
    then runs the full strategy scan (9:40 vs open >0.56%) and sends Feishu notification.

    Uses shared iFinD client and fundamentals DB from app.state (passed via state dict).
    """
    import asyncio
    from datetime import datetime, time, timedelta
    from zoneinfo import ZoneInfo

    from src.common.feishu_bot import FeishuBot
    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.strategies.momentum_sector_scanner import (
        MomentumSectorScanner,
        PriceSnapshot,
    )

    beijing_tz = ZoneInfo("Asia/Shanghai")
    POLL_INTERVAL = 30  # seconds
    MONITOR_START = time(9, 30)
    MONITOR_END = time(9, 40)

    state["running"] = True
    logger.info("Intraday momentum monitor started")

    try:
        while state["running"]:
            now = datetime.now(beijing_tz)
            current_time = now.time()

            # Only run on weekdays
            if now.weekday() >= 5:
                # Weekend — sleep until Monday
                await asyncio.sleep(3600)
                continue

            # Before monitoring window — wait
            if current_time < MONITOR_START:
                delta = datetime.combine(now.date(), MONITOR_START) - datetime.combine(
                    now.date(), current_time
                )
                wait_secs = max(delta.total_seconds(), 10)
                logger.debug(f"Monitor waiting {wait_secs:.0f}s until {MONITOR_START}")
                await asyncio.sleep(min(wait_secs, 60))
                continue

            # After monitoring window — wait for tomorrow
            if current_time > time(9, 50):
                # Sleep until next day 9:25
                tomorrow = now + timedelta(days=1)
                target = datetime.combine(tomorrow.date(), time(9, 25), tzinfo=beijing_tz)
                wait_secs = (target - now).total_seconds()
                logger.debug(f"Monitor done for today, sleeping {wait_secs:.0f}s")
                await asyncio.sleep(min(wait_secs, 3600))
                continue

            # We're in the monitoring window (9:30-9:40)
            logger.info("Monitor entering active polling window")
            accumulated: dict[str, PriceSnapshot] = {}
            poll_count = 0

            ifind_client = state.get("ifind_client")
            fundamentals_db = state.get("fundamentals_db")

            if not ifind_client or not fundamentals_db:
                logger.error("Monitor: shared iFinD client or fundamentals DB not available")
                await asyncio.sleep(60)
                continue

            try:
                while state["running"]:
                    current_time = datetime.now(beijing_tz).time()
                    if current_time >= MONITOR_END:
                        break

                    poll_count += 1
                    logger.info(f"Monitor poll #{poll_count}")

                    try:
                        result = await ifind_client.smart_stock_picking(
                            "涨幅大于-0.5%的沪深主板非ST股票", "stock"
                        )
                        snapshots = await _parse_iwencai_realtime(ifind_client, result)
                        accumulated.update(snapshots)
                        logger.info(
                            f"Poll #{poll_count}: {len(snapshots)} pre-filtered stocks "
                            f"(accumulated: {len(accumulated)})"
                        )
                    except Exception as e:
                        logger.error(f"Monitor poll error: {e}")

                    await asyncio.sleep(POLL_INTERVAL)

                # Run full strategy scan
                if accumulated:
                    logger.info(f"Running strategy scan on {len(accumulated)} accumulated stocks")
                    concept_mapper = LocalConceptMapper()
                    scanner = MomentumSectorScanner(
                        ifind_client=ifind_client,
                        fundamentals_db=fundamentals_db,
                        concept_mapper=concept_mapper,
                    )

                    scan_result = await scanner.scan(accumulated)
                    scan_time = datetime.now(beijing_tz)

                    # Store result
                    rec = scan_result.recommended_stock
                    result_entry = {
                        "scan_time": scan_time.strftime("%Y-%m-%d %H:%M"),
                        "initial_gainers": len(scan_result.initial_gainers),
                        "hot_boards": len(scan_result.hot_boards),
                        "selected_count": len(scan_result.selected_stocks),
                        "selected_stocks": [
                            {
                                "stock_code": s.stock_code,
                                "stock_name": s.stock_name,
                                "board_name": s.board_name,
                                "open_gain_pct": round(s.open_gain_pct, 2),
                                "pe_ttm": round(s.pe_ttm, 2),
                                "board_avg_pe": round(s.board_avg_pe, 2),
                            }
                            for s in scan_result.selected_stocks
                        ],
                        "recommended_stock": {
                            "stock_code": rec.stock_code,
                            "stock_name": rec.stock_name,
                            "board_name": rec.board_name,
                            "board_stock_count": rec.board_stock_count,
                            "growth_rate": round(rec.growth_rate, 2),
                            "open_gain_pct": round(rec.open_gain_pct, 2),
                        }
                        if rec
                        else None,
                    }
                    state["last_result"] = result_entry
                    state["last_scan_time"] = scan_time.strftime("%Y-%m-%d %H:%M")
                    state["today_results"].append(result_entry)

                    # Send Feishu notification
                    bot = FeishuBot()
                    if bot.is_configured():
                        await bot.send_momentum_scan_result(
                            selected_stocks=scan_result.selected_stocks,
                            hot_boards=scan_result.hot_boards,
                            initial_gainer_count=len(scan_result.initial_gainers),
                            scan_time=scan_time,
                            recommended_stock=scan_result.recommended_stock,
                        )
                        logger.info("Monitor: Feishu notification sent")

                    logger.info(
                        f"Monitor scan complete: {len(scan_result.selected_stocks)} stocks selected"
                    )
                else:
                    logger.info("Monitor: no pre-filtered stocks found during window")

            except Exception as e:
                logger.error(f"Monitor active window error: {e}", exc_info=True)

            # After scan, wait until next day
            tomorrow = now + timedelta(days=1)
            target = datetime.combine(tomorrow.date(), time(9, 25), tzinfo=beijing_tz)
            wait_secs = (target - datetime.now(beijing_tz)).total_seconds()
            state["today_results"] = []  # Reset for next day
            await asyncio.sleep(min(max(wait_secs, 10), 3600 * 18))

    except asyncio.CancelledError:
        logger.info("Intraday momentum monitor cancelled")
    except Exception as e:
        logger.error(f"Intraday momentum monitor error: {e}", exc_info=True)
    finally:
        state["running"] = False
        state["task"] = None
        logger.info("Intraday momentum monitor stopped")


async def _parse_iwencai_realtime(ifind_client, iwencai_result: dict) -> dict:
    """Parse iwencai response and fetch real-time prices."""
    from src.data.clients.ifind_http_client import IFinDHttpError
    from src.strategy.strategies.momentum_sector_scanner import PriceSnapshot

    tables = iwencai_result.get("tables", [])
    if not tables:
        return {}

    raw_codes: list[str] = []
    names_map: dict[str, str] = {}

    for table_wrapper in tables:
        if not isinstance(table_wrapper, dict):
            continue
        table = table_wrapper.get("table", table_wrapper)
        if not isinstance(table, dict):
            continue

        codes = None
        names = None
        for col_name, col_data in table.items():
            if "代码" in col_name:
                codes = col_data
            elif "简称" in col_name or "名称" in col_name:
                names = col_data

        if codes:
            for i in range(len(codes)):
                raw = str(codes[i]).strip()
                bare = raw
                for suffix in (".SZ", ".SH", ".BJ"):
                    if bare.upper().endswith(suffix):
                        bare = bare[: -len(suffix)]
                        break
                if len(bare) == 6 and bare.isdigit():
                    raw_codes.append(raw)
                    name = str(names[i]).strip() if names and i < len(names) else ""
                    names_map[bare] = name

    if not raw_codes:
        return {}

    snapshots: dict[str, PriceSnapshot] = {}
    batch_size = 50

    for i in range(0, len(raw_codes), batch_size):
        batch = raw_codes[i : i + batch_size]
        formatted = []
        for raw in batch:
            if "." in raw:
                formatted.append(raw)
            else:
                suffix = ".SH" if raw.startswith("6") else ".SZ"
                formatted.append(f"{raw}{suffix}")

        codes_str = ",".join(formatted)

        try:
            data = await ifind_client.real_time_quotation(
                codes=codes_str,
                indicators="open,preClose,latest",
            )

            for table_entry in data.get("tables", []):
                thscode = table_entry.get("thscode", "")
                bare = thscode.split(".")[0] if thscode else ""
                if not bare:
                    continue

                tbl = table_entry.get("table", {})
                open_vals = tbl.get("open", [])
                prev_vals = tbl.get("preClose", [])
                latest_vals = tbl.get("latest", [])

                if open_vals and prev_vals and latest_vals:
                    open_price = float(open_vals[0]) if open_vals[0] else 0.0
                    prev_close = float(prev_vals[0]) if prev_vals[0] else 0.0
                    latest = float(latest_vals[0]) if latest_vals[0] else 0.0
                    if prev_close > 0:
                        snapshots[bare] = PriceSnapshot(
                            stock_code=bare,
                            stock_name=names_map.get(bare, ""),
                            open_price=open_price,
                            prev_close=prev_close,
                            latest_price=latest,
                        )

        except IFinDHttpError as e:
            logger.error(f"real_time_quotation batch failed: {e}")

    return snapshots


# === SAFETY AUDIT ENDPOINT ===
# Included in the main router (create_router) so it's always available.
# The audit runs once at startup; this endpoint returns cached results.


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
    """Create router for settings page (iFinD token management)."""
    router = APIRouter(tags=["settings"])

    @router.get("/settings", response_class=HTMLResponse)
    async def settings_page(request: Request):
        """Settings page."""
        templates = request.app.state.templates
        return templates.TemplateResponse("settings.html", {"request": request})

    @router.get("/api/settings/ifind-token")
    async def get_ifind_token_status():
        """Get current iFinD token status (masked)."""
        from src.common.config import get_ifind_refresh_token, get_ifind_token_source

        source = get_ifind_token_source()
        source_labels = {
            "web_ui": "Web UI (当前会话)",
            "persisted_file": "Web UI (已持久化)",
            "env_var": "环境变量",
            "secrets_yaml": "secrets.yaml",
            "not_configured": "未配置",
        }

        try:
            token = get_ifind_refresh_token()
            # Mask token: show first 8 and last 8 chars
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

    @router.post("/api/settings/ifind-token")
    async def update_ifind_token(body: TokenUpdateRequest):
        """Save a new iFinD refresh_token."""
        from src.common.config import set_ifind_refresh_token

        token = body.token.strip()
        if not token:
            raise HTTPException(status_code=400, detail="Token 不能为空")

        set_ifind_refresh_token(token)
        return {"success": True, "message": "Token 已保存，新的 API 调用将使用此 token"}

    @router.post("/api/settings/ifind-token/test")
    async def test_ifind_token(body: TokenUpdateRequest):
        """Test an iFinD refresh_token by trying to obtain an access_token."""
        import httpx

        token = body.token.strip()
        if not token:
            raise HTTPException(status_code=400, detail="Token 不能为空")

        url = "https://quantapi.51ifind.com/api/v1/get_access_token"
        headers = {
            "Content-Type": "application/json",
            "refresh_token": token,
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, headers=headers)
                response.raise_for_status()
                data = response.json()

                error_code = data.get("errorcode", -1)
                if error_code == 0:
                    return {"success": True, "message": "Token 验证成功，可以正常获取 access_token"}
                else:
                    err_msg = data.get("errmsg", "未知错误")
                    return {
                        "success": False,
                        "message": f"Token 验证失败: {err_msg} (错误码: {error_code})",
                    }
        except httpx.TimeoutException:
            return {"success": False, "message": "请求超时，请检查网络连接"}
        except httpx.HTTPError as e:
            return {"success": False, "message": f"HTTP 请求失败: {e}"}

    # === TAVILY API KEY SETTINGS ===

    @router.get("/api/settings/tavily-key")
    async def get_tavily_key_status():
        """Get current Tavily API key status (masked)."""
        from src.common.config import get_tavily_api_key, get_tavily_key_source

        source = get_tavily_key_source()
        source_labels = {
            "web_ui": "Web UI (当前会话)",
            "persisted_file": "Web UI (已持久化)",
            "env_var": "环境变量",
            "secrets_yaml": "secrets.yaml",
            "not_configured": "未配置",
        }

        try:
            key = get_tavily_api_key()
            if len(key) > 16:
                masked = key[:8] + "..." + key[-4:]
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

    @router.post("/api/settings/tavily-key")
    async def update_tavily_key(body: TokenUpdateRequest):
        """Save a new Tavily API key."""
        from src.common.config import set_tavily_api_key

        key = body.token.strip()
        if not key:
            raise HTTPException(status_code=400, detail="API Key 不能为空")

        set_tavily_api_key(key)
        return {"success": True, "message": "Tavily API Key 已保存"}

    @router.post("/api/settings/tavily-key/test")
    async def test_tavily_key(body: TokenUpdateRequest):
        """Test a Tavily API key by running a simple search."""
        import httpx

        key = body.token.strip()
        if not key:
            raise HTTPException(status_code=400, detail="API Key 不能为空")

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(
                    "https://api.tavily.com/search",
                    json={
                        "api_key": key,
                        "query": "test",
                        "max_results": 1,
                        "search_depth": "basic",
                    },
                )
                if resp.status_code == 200:
                    return {"success": True, "message": "Tavily API Key 验证成功"}
                else:
                    detail = ""
                    try:
                        detail = resp.text[:200]
                    except Exception:
                        pass
                    return {
                        "success": False,
                        "message": f"验证失败: HTTP {resp.status_code} — {detail}",
                    }
        except httpx.TimeoutException:
            return {"success": False, "message": "请求超时，请检查网络连接"}
        except httpx.HTTPError as e:
            return {"success": False, "message": f"HTTP 请求失败: {e}"}

    @router.get("/api/settings/keys-status")
    async def get_all_keys_status():
        """Get status of all API keys needed for live trading."""
        from src.common.config import (
            get_ifind_token_source,
            get_tavily_key_source,
            load_secrets,
        )

        ifind_ok = get_ifind_token_source() != "not_configured"
        tavily_ok = get_tavily_key_source() != "not_configured"

        # Check Silicon Flow from secrets.yaml
        sf_ok = False
        try:
            secrets = load_secrets()
            sf_ok = bool(secrets.get_str("siliconflow.api_key", ""))
        except FileNotFoundError:
            pass

        return {
            "ifind": {"configured": ifind_ok, "source": get_ifind_token_source()},
            "tavily": {"configured": tavily_ok, "source": get_tavily_key_source()},
            "siliconflow": {"configured": sf_ok},
            "news_check_ready": tavily_ok and sf_ok,
        }

    # === SCAN STOCKS BACKFILL (SSE) ===

    @router.post("/api/momentum/backfill")
    async def run_backfill(request: Request):
        """Backfill momentum scan selected stocks with SSE streaming progress."""
        import asyncio
        import json
        from datetime import datetime, timedelta

        from src.data.database.momentum_scan_db import create_momentum_scan_db_from_config
        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.strategies.momentum_sector_scanner import MomentumSectorScanner

        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="请求体不是有效 JSON")

        start_date_str = body.get("start_date", "")
        end_date_str = body.get("end_date", "")
        if not start_date_str or not end_date_str:
            raise HTTPException(
                status_code=400,
                detail=f"缺少日期参数 (received keys: {list(body.keys())})",
            )

        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD")

        if end_date <= start_date:
            raise HTTPException(status_code=400, detail="结束日期必须晚于起始日期")

        ifind_client = getattr(request.app.state, "ifind_client", None)
        fundamentals_db = getattr(request.app.state, "fundamentals_db", None)
        if not ifind_client:
            raise HTTPException(status_code=503, detail="iFinD 客户端未就绪")
        if not fundamentals_db:
            raise HTTPException(status_code=503, detail="基本面数据库未就绪")

        async def event_stream():
            def sse(data: dict) -> str:
                return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

            scan_db = None
            try:
                yield sse({"type": "status", "message": "连接数据库..."})
                scan_db = create_momentum_scan_db_from_config()
                await scan_db.connect()

                concept_mapper = LocalConceptMapper()
                scanner = MomentumSectorScanner(
                    ifind_client=ifind_client,
                    fundamentals_db=fundamentals_db,
                    concept_mapper=concept_mapper,
                )

                # Trading calendar
                yield sse({"type": "status", "message": "获取交易日历..."})
                full_cal = _get_trading_calendar_akshare(start_date, end_date + timedelta(days=10))
                trading_days = [d for d in full_cal if start_date <= d <= end_date]

                if not trading_days:
                    yield sse({"type": "error", "message": "所选日期范围内无交易日"})
                    return

                # T+1 map
                next_day_map: dict = {}
                for idx, d in enumerate(full_cal):
                    if idx + 1 < len(full_cal):
                        next_day_map[d] = full_cal[idx + 1]

                # Skip already-backfilled dates
                yield sse({"type": "status", "message": "检查已有数据..."})
                existing = set(
                    await scan_db.get_dates_with_data(
                        start_date=start_date_str,
                        end_date=end_date_str,
                    )
                )
                remaining = [d for d in trading_days if d not in existing]

                yield sse(
                    {
                        "type": "init",
                        "total_days": len(remaining),
                        "skipped_days": len(trading_days) - len(remaining),
                    }
                )

                if not remaining:
                    yield sse({"type": "complete", "message": "所有日期已回填，无需操作"})
                    return

                success = 0
                errors = 0
                total_stocks = 0

                for i, day in enumerate(remaining):
                    yield sse(
                        {
                            "type": "progress",
                            "day": i + 1,
                            "total": len(remaining),
                            "trade_date": str(day),
                        }
                    )

                    try:
                        price_snapshots, price_err = await _parse_iwencai_and_fetch_prices_for_date(
                            ifind_client, day
                        )

                        if not price_snapshots:
                            yield sse(
                                {
                                    "type": "day_result",
                                    "trade_date": str(day),
                                    "stocks": 0,
                                    "status": "skip",
                                    "message": price_err or "无价格数据",
                                }
                            )
                            success += 1
                            await asyncio.sleep(0.05)
                            continue

                        result = await scanner.scan(price_snapshots, trade_date=day)
                        selected = result.selected_stocks
                        all_snapshots = result.all_snapshots

                        if not selected:
                            yield sse(
                                {
                                    "type": "day_result",
                                    "trade_date": str(day),
                                    "stocks": 0,
                                    "status": "ok",
                                    "message": "无选股",
                                }
                            )
                            success += 1
                            await asyncio.sleep(0.05)
                            continue

                        unique_codes = list({s.stock_code for s in selected})

                        # T+1 open prices
                        next_day = next_day_map.get(day)
                        ndo_map: dict[str, float] = {}
                        if next_day:
                            ndo_map = await _fetch_batch_prices(
                                ifind_client, unique_codes, next_day, indicator="open"
                            )

                        # Growth rates from DB
                        growth_map = await fundamentals_db.batch_get_revenue_growth(unique_codes)

                        # Build rows
                        db_rows = []
                        for s in selected:
                            snap = all_snapshots.get(s.stock_code)
                            bp = snap.latest_price if snap else 0.0
                            ndo = ndo_map.get(s.stock_code)
                            ret = None
                            if ndo and bp > 0:
                                ret = round(_calc_net_return_pct(bp, ndo), 4)
                            db_rows.append(
                                {
                                    "stock_code": s.stock_code,
                                    "stock_name": s.stock_name,
                                    "board_name": s.board_name,
                                    "open_gain_pct": s.open_gain_pct,
                                    "pe_ttm": s.pe_ttm,
                                    "board_avg_pe": s.board_avg_pe,
                                    "open_price": snap.open_price if snap else 0.0,
                                    "prev_close": snap.prev_close if snap else 0.0,
                                    "buy_price": bp,
                                    "next_day_open": ndo,
                                    "return_pct": ret,
                                    "growth_rate": growth_map.get(s.stock_code),
                                }
                            )

                        saved = await scan_db.save_day(day, db_rows)
                        total_stocks += saved
                        success += 1

                        yield sse(
                            {
                                "type": "day_result",
                                "trade_date": str(day),
                                "stocks": saved,
                                "status": "ok",
                            }
                        )

                    except Exception as e:
                        errors += 1
                        logger.error(f"Backfill error on {day}: {e}")
                        yield sse(
                            {
                                "type": "day_result",
                                "trade_date": str(day),
                                "stocks": 0,
                                "status": "error",
                                "message": str(e)[:80],
                            }
                        )

                    await asyncio.sleep(0.05)

                yield sse(
                    {
                        "type": "complete",
                        "success": success,
                        "errors": errors,
                        "total_stocks": total_stocks,
                    }
                )

            except Exception as e:
                logger.error(f"Backfill fatal error: {e}", exc_info=True)
                yield sse({"type": "error", "message": str(e)[:200]})
            finally:
                if scan_db:
                    await scan_db.close()

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # === SCAN STOCKS CSV EXPORT ===

    @router.get("/api/momentum/scan-stocks/csv")
    async def export_scan_stocks_csv(start_date: str, end_date: str):
        """Export momentum scan selected stocks as CSV file download.

        Query params:
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
        """
        import csv
        import io as _io
        from datetime import datetime

        from src.data.database.momentum_scan_db import create_momentum_scan_db_from_config

        # Validate dates
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD")

        scan_db = create_momentum_scan_db_from_config()
        try:
            await scan_db.connect()
            rows = await scan_db.query(start_date=start_date, end_date=end_date)
        finally:
            await scan_db.close()

        if not rows:
            raise HTTPException(status_code=404, detail="所选日期范围内无数据")

        # Build CSV in memory
        output = _io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=[
                "trade_date",
                "stock_code",
                "stock_name",
                "board_name",
                "open_gain_pct",
                "pe_ttm",
                "board_avg_pe",
                "open_price",
                "prev_close",
                "buy_price",
                "next_day_open",
                "return_pct",
                "growth_rate",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

        csv_bytes = output.getvalue().encode("utf-8-sig")  # BOM for Excel compatibility
        filename = f"momentum_scan_stocks_{start_date}_{end_date}.csv"

        return StreamingResponse(
            _io.BytesIO(csv_bytes),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return router
