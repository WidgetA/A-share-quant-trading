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
        store = get_store(request)
        return {
            "status": "ok",
            "pending_count": len(store),
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

        new_phase = await manager.advance_to_next_phase()

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
        limit: int = 100,
    ) -> dict:
        """
        Get messages for the current simulation date.

        Returns all messages (or only positive ones) with their analysis results.
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

        # Get premarket messages (all or only positive)
        messages = await hist_reader.get_premarket_messages(
            trade_date=clock.current_time,
            only_positive=only_positive,
            limit=limit,
        )

        # Convert to dict format
        result = []
        for msg in messages:
            msg_dict = {
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
            "messages": result,
        }

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
