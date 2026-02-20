# === MODULE PURPOSE ===
# FastAPI application for trading confirmations.
# Provides REST API and serves HTML templates.

# === DEPENDENCIES ===
# - pending_store: For accessing pending confirmations
# - strategy_controller: For strategy start/stop control
# - position_manager: For displaying current positions
# - jinja2: For HTML template rendering

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.common.pending_store import PendingConfirmationStore, get_pending_store
from src.web.routes import (
    create_momentum_router,
    create_order_assistant_router,
    create_router,
    create_settings_router,
    create_simulation_router,
)

if TYPE_CHECKING:
    from src.common.strategy_controller import StrategyController
    from src.trading.position_manager import PositionManager

logger = logging.getLogger(__name__)

# Template and static file directories
WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"


def create_app(
    store: PendingConfirmationStore | None = None,
    web_base_url: str = "http://localhost:8000",
    strategy_controller: StrategyController | None = None,
    position_manager: PositionManager | None = None,
) -> FastAPI:
    """
    Create FastAPI application.

    Args:
        store: Pending confirmation store. Uses global singleton if not provided.
        web_base_url: Base URL for generating links in notifications.
        strategy_controller: Controller for strategy start/stop.
        position_manager: Manager for position data.

    Returns:
        Configured FastAPI app.
    """
    app = FastAPI(
        title="A-Share Trading Confirmation",
        description="Web UI for trading signal confirmations",
        version="1.0.0",
    )

    # Use provided store or global singleton
    if store is None:
        store = get_pending_store()

    # Store reference for routes
    app.state.pending_store = store
    app.state.web_base_url = web_base_url
    app.state.strategy_controller = strategy_controller
    app.state.position_manager = position_manager

    # Set up templates
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    app.state.templates = templates

    # Create and include routers
    router = create_router()
    app.include_router(router)

    # Add simulation router
    simulation_router = create_simulation_router()
    app.include_router(simulation_router)

    # Add order assistant router
    oa_router = create_order_assistant_router()
    app.include_router(oa_router)

    # Add momentum backtest/monitor router
    momentum_router = create_momentum_router()
    app.include_router(momentum_router)

    # Add settings router
    settings_router = create_settings_router()
    app.include_router(settings_router)

    # Mount static files if directory exists
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.on_event("startup")
    async def startup():
        import asyncio

        from src.data.clients.ifind_http_client import IFinDHttpClient
        from src.data.database.fundamentals_db import create_fundamentals_db_from_config
        from src.web.routes import _run_intraday_monitor

        logger.info("Web UI started")
        store.start_cleanup_task()

        # Shared iFinD HTTP client (token obtained once, reused by all endpoints)
        ifind_client = IFinDHttpClient()
        try:
            await ifind_client.start()
            app.state.ifind_client = ifind_client
            logger.info("Shared iFinD HTTP client started")
        except Exception as e:
            logger.error(f"Failed to start shared iFinD client: {e}")
            app.state.ifind_client = None

        # Shared fundamentals DB connection pool
        fundamentals_db = create_fundamentals_db_from_config()
        try:
            await fundamentals_db.connect()
            app.state.fundamentals_db = fundamentals_db
            logger.info("Shared fundamentals DB connected")
        except Exception as e:
            logger.error(f"Failed to connect shared fundamentals DB: {e}")
            app.state.fundamentals_db = None

        # Akshare backtest cache (populated on demand by /api/momentum/akshare-prepare)
        app.state.akshare_cache = None

        # Auto-start intraday momentum monitor as background task
        # Pass shared clients via state dict so monitor doesn't create its own
        app.state.momentum_monitor_state = {
            "running": False,
            "last_scan_time": None,
            "last_result": None,
            "today_results": [],
            "task": None,
            "ifind_client": app.state.ifind_client,
            "fundamentals_db": app.state.fundamentals_db,
        }
        task = asyncio.create_task(_run_intraday_monitor(app.state.momentum_monitor_state))
        app.state.momentum_monitor_state["task"] = task
        logger.info("Intraday momentum monitor auto-started")

    @app.on_event("shutdown")
    async def shutdown():
        logger.info("Web UI stopped")
        store.stop_cleanup_task()

        # Stop momentum monitor
        monitor_state = getattr(app.state, "momentum_monitor_state", None)
        if monitor_state:
            task = monitor_state.get("task")
            if task and not task.done():
                task.cancel()
            monitor_state["running"] = False
            logger.info("Intraday momentum monitor stopped")

        # Close shared iFinD client
        ifind_client = getattr(app.state, "ifind_client", None)
        if ifind_client:
            await ifind_client.stop()
            logger.info("Shared iFinD HTTP client stopped")

        # Close shared fundamentals DB
        fundamentals_db = getattr(app.state, "fundamentals_db", None)
        if fundamentals_db:
            await fundamentals_db.close()
            logger.info("Shared fundamentals DB closed")

    return app


def run_server(
    host: str = "0.0.0.0",
    port: int = 8000,
    store: PendingConfirmationStore | None = None,
    web_base_url: str | None = None,
) -> None:
    """
    Run the web server (blocking).

    This is mainly for testing. In production, use uvicorn directly
    or start the server in a background task.

    Args:
        host: Bind host.
        port: Bind port.
        store: Pending confirmation store.
        web_base_url: Base URL for links.
    """
    import uvicorn

    if web_base_url is None:
        web_base_url = f"http://{host}:{port}"

    app = create_app(store=store, web_base_url=web_base_url)
    uvicorn.run(app, host=host, port=port)
