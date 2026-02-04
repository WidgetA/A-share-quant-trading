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

        # Get positions
        manager = get_position_manager(request)
        if manager:
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
        """Get all current positions."""
        manager = get_position_manager(request)
        if not manager:
            return {"positions": [], "summary": {}, "message": "Position manager not initialized"}

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
