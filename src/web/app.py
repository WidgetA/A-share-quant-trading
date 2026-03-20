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
from src.web.iquant_routes import create_iquant_router
from src.web.routes import (
    create_momentum_router,
    create_order_assistant_router,
    create_router,
    create_settings_router,
    create_simulation_router,
    create_trade_backtest_router,
)
from src.web.v15_scan_service import V15ScanState, inject_cache, start_scan_scheduler

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

    # Add trade backtest router (CSV upload → stats)
    trade_bt_router = create_trade_backtest_router()
    app.include_router(trade_bt_router)

    # Add iQuant API router (trading only, scan is separate)
    iquant_router = create_iquant_router()
    app.include_router(iquant_router)
    app.state.iquant_router = iquant_router  # for shutdown cleanup

    # V15 scan state (shared between scan service and trading router)
    scan_state = V15ScanState()
    app.state.v15_scan_state = scan_state
    # Inject scan state into trading router
    if hasattr(iquant_router, "_inject_scan_state"):
        iquant_router._inject_scan_state(scan_state)

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

        # Tsanghi backtest cache — load from OSS in background (non-blocking)
        app.state.tsanghi_cache = None
        app.state.tsanghi_cache_loading = True  # frontend polls this

        async def _bg_load_oss_cache():
            try:
                from src.data.clients.tsanghi_backtest_cache import TsanghiBacktestCache

                logger.info("Loading tsanghi cache from OSS (background)...")
                oss_cache = await asyncio.to_thread(TsanghiBacktestCache.load_from_oss)
                if oss_cache:
                    app.state.tsanghi_cache = oss_cache
                    logger.info(
                        f"Tsanghi cache pre-loaded from OSS: "
                        f"{len(oss_cache._daily)} daily, {len(oss_cache._minute)} minute, "
                        f"range [{oss_cache._start_date} ~ {oss_cache._end_date}]"
                    )
                    # Inject cache into V15 scan service for historical data
                    v15_ss = getattr(app.state, "v15_scan_state", None)
                    if v15_ss:
                        inject_cache(v15_ss, oss_cache)
                else:
                    logger.warning("load_from_oss returned None — check OSS config/logs")
            except Exception as e:
                logger.warning(f"Failed to pre-load tsanghi cache from OSS: {e}")
            finally:
                app.state.tsanghi_cache_loading = False

        asyncio.create_task(_bg_load_oss_cache())

        # Run trading safety audit at startup
        from scripts.audit_trading_safety import run_audit

        audit_result = run_audit()
        app.state.safety_audit = {
            "critical_count": len([v for v in audit_result.violations if v.severity == "CRITICAL"]),
            "warning_count": len([v for v in audit_result.violations if v.severity == "WARNING"]),
            "violations": [
                {
                    "file": v.file,
                    "line": v.line,
                    "category": v.category,
                    "detail": v.detail,
                    "severity": v.severity,
                }
                for v in audit_result.violations
            ],
            "files_scanned": audit_result.files_scanned,
        }
        if audit_result.violations:
            critical = app.state.safety_audit["critical_count"]
            logger.warning(
                f"Trading safety audit: {critical} CRITICAL violations found! "
                f"Run 'uv run python scripts/audit_trading_safety.py' for details."
            )

        # Auto-start iQuant monitoring (heartbeat, signal timeout, readiness)
        # This is independent of trading resources — must always run.
        iquant_rtr = getattr(app.state, "iquant_router", None)
        if iquant_rtr and hasattr(iquant_rtr, "_start_monitoring"):
            iquant_rtr._start_monitoring()
            logger.info("iQuant V15 monitoring scheduler started")

        # Auto-start V15 scan scheduler (autonomous, independent of iQuant)
        v15_ss = getattr(app.state, "v15_scan_state", None)
        if v15_ss:
            start_scan_scheduler(v15_ss)
            logger.info("V15 scan scheduler started (autonomous)")

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
            "_app_state": app.state,  # needed for tsanghi_cache access
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

        # Cleanup iQuant trading resources
        iquant_rtr = getattr(app.state, "iquant_router", None)
        if iquant_rtr and hasattr(iquant_rtr, "_iquant_cleanup"):
            await iquant_rtr._iquant_cleanup()

        # Cleanup V15 scan resources
        from src.web.v15_scan_service import cleanup_scan_resources

        v15_ss = getattr(app.state, "v15_scan_state", None)
        if v15_ss:
            await cleanup_scan_resources(v15_ss)
            logger.info("V15 scan resources cleaned up")

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
