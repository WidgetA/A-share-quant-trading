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
# POST /api/momentum/backtest - Run backtest for a date (JSON)
# GET  /api/momentum/monitor-status - Get intraday monitor status (JSON)

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


def create_momentum_router() -> APIRouter:
    """Create router for momentum backtest and intraday monitor endpoints."""
    import asyncio
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

    @router.get("/momentum", response_class=HTMLResponse)
    async def momentum_page(request: Request):
        """Momentum backtest and monitor page."""
        templates = request.app.state.templates
        monitor_state = _get_monitor_state(request)
        return templates.TemplateResponse(
            "momentum_backtest.html",
            {
                "request": request,
                "monitor_running": monitor_state["running"],
            },
        )

    @router.post("/api/momentum/backtest")
    async def run_backtest(body: MomentumBacktestRequest) -> dict:
        """Run momentum sector strategy backtest for a specific date."""
        from src.common.feishu_bot import FeishuBot
        from src.data.clients.ifind_http_client import IFinDHttpClient, IFinDHttpError
        from src.data.database.fundamentals_db import create_fundamentals_db_from_config
        from src.data.sources.concept_mapper import ConceptMapper
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

        # Initialize components
        ifind_client = IFinDHttpClient()
        fundamentals_db = create_fundamentals_db_from_config()

        try:
            await ifind_client.start()
            await fundamentals_db.connect()

            concept_mapper = ConceptMapper(ifind_client)
            scanner = MomentumSectorScanner(
                ifind_client=ifind_client,
                fundamentals_db=fundamentals_db,
                concept_mapper=concept_mapper,
            )

            # Step 1: Use iwencai to get >5% gainers
            date_str = trade_date.strftime("%Y%m%d")
            query = f"{date_str}开盘涨幅大于5%的沪深主板非ST股票"
            logger.info(f"Momentum backtest iwencai query: {query}")

            try:
                iwencai_result = await ifind_client.smart_stock_picking(query, "stock")
            except IFinDHttpError as e:
                raise HTTPException(status_code=502, detail=f"iFinD查询失败: {e}")

            # Parse iwencai response → fetch prices
            price_snapshots = await _parse_iwencai_and_fetch_prices(
                ifind_client, iwencai_result, trade_date
            )

            if not price_snapshots:
                return {
                    "success": True,
                    "trade_date": body.trade_date,
                    "initial_gainers": 0,
                    "hot_boards": {},
                    "selected_stocks": [],
                    "message": "未找到符合条件的股票",
                }

            # Run scan (pass trade_date so constituent prices use history_quotes)
            result = await scanner.scan(price_snapshots, trade_date=trade_date)

            # Format response
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
                    )
                    response_data["feishu_sent"] = True

            return response_data

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Momentum backtest error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"回测出错: {str(e)}")
        finally:
            await fundamentals_db.close()
            await ifind_client.stop()

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

    return router


async def _parse_iwencai_and_fetch_prices(ifind_client, iwencai_result: dict, trade_date) -> dict:
    """Parse iwencai response and fetch historical prices via history_quotes."""
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
        return {}

    # Batch fetch prices
    snapshots: dict[str, PriceSnapshot] = {}
    batch_size = 50
    date_fmt = trade_date.strftime("%Y-%m-%d")

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
            logger.error(f"history_quotes batch failed: {e}")

    return snapshots


async def _run_intraday_monitor(state: dict) -> None:
    """
    Background task: intraday momentum monitor.

    Runs every trading day 9:30-9:40, polls for >5% gainers,
    then runs the full strategy scan and sends Feishu notification.
    """
    import asyncio
    from datetime import datetime, time, timedelta
    from zoneinfo import ZoneInfo

    from src.common.feishu_bot import FeishuBot
    from src.data.clients.ifind_http_client import IFinDHttpClient
    from src.data.database.fundamentals_db import create_fundamentals_db_from_config
    from src.data.sources.concept_mapper import ConceptMapper
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

            ifind_client = IFinDHttpClient()
            fundamentals_db = create_fundamentals_db_from_config()

            try:
                await ifind_client.start()
                await fundamentals_db.connect()

                while state["running"]:
                    current_time = datetime.now(beijing_tz).time()
                    if current_time >= MONITOR_END:
                        break

                    poll_count += 1
                    logger.info(f"Monitor poll #{poll_count}")

                    try:
                        result = await ifind_client.smart_stock_picking(
                            "涨幅大于5%的沪深主板非ST股票", "stock"
                        )
                        snapshots = await _parse_iwencai_realtime(ifind_client, result)
                        accumulated.update(snapshots)
                        logger.info(
                            f"Poll #{poll_count}: {len(snapshots)} >5% stocks "
                            f"(accumulated: {len(accumulated)})"
                        )
                    except Exception as e:
                        logger.error(f"Monitor poll error: {e}")

                    await asyncio.sleep(POLL_INTERVAL)

                # Run full strategy scan
                if accumulated:
                    logger.info(f"Running strategy scan on {len(accumulated)} accumulated stocks")
                    concept_mapper = ConceptMapper(ifind_client)
                    scanner = MomentumSectorScanner(
                        ifind_client=ifind_client,
                        fundamentals_db=fundamentals_db,
                        concept_mapper=concept_mapper,
                    )

                    scan_result = await scanner.scan(accumulated)
                    scan_time = datetime.now(beijing_tz)

                    # Store result
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
                        )
                        logger.info("Monitor: Feishu notification sent")

                    logger.info(
                        f"Monitor scan complete: {len(scan_result.selected_stocks)} stocks selected"
                    )
                else:
                    logger.info("Monitor: no >5% gainers found during window")

            finally:
                await fundamentals_db.close()
                await ifind_client.stop()

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
