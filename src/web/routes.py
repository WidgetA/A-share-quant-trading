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

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

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

        # DB stores UTC timestamps; compute boundaries in Beijing time
        # then convert to UTC for queries.
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

        # Convert Beijing time boundaries to UTC for DB queries
        start_time = start_bj - _BJ_UTC
        end_time = end_bj - _BJ_UTC

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
                "query_start_utc": start_time.isoformat(),
                "query_end_utc": end_time.isoformat(),
                "query_start_bj": start_bj.isoformat(),
                "query_end_bj": end_bj.isoformat(),
                "timezone_offset_hours": 8,
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
