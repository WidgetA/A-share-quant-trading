# === MODULE PURPOSE ===
# FastAPI application for trading confirmations.
# Provides REST API and serves HTML templates.

# === DEPENDENCIES ===
# - pending_store: For accessing pending confirmations
# - position_manager: For displaying current positions
# - jinja2: For HTML template rendering

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.common.pending_store import PendingConfirmationStore, get_pending_store
from src.web.iquant_routes import create_iquant_router
from src.web.routes import (
    create_model_router,
    create_momentum_router,
    create_router,
    create_settings_router,
    create_trade_backtest_router,
    create_trading_router,
)

logger = logging.getLogger(__name__)

# Template and static file directories
WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"


def create_app(
    store: PendingConfirmationStore | None = None,
    web_base_url: str = "http://localhost:8000",
) -> FastAPI:
    """
    Create FastAPI application.

    Args:
        store: Pending confirmation store. Uses global singleton if not provided.
        web_base_url: Base URL for generating links in notifications.

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

    # Set up templates
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    app.state.templates = templates

    # Create and include routers
    router = create_router()
    app.include_router(router)

    # Add momentum backtest/monitor router
    momentum_router = create_momentum_router()
    app.include_router(momentum_router)

    # Add settings router
    settings_router = create_settings_router()
    app.include_router(settings_router)

    # Add trade backtest router (CSV upload → stats)
    trade_bt_router = create_trade_backtest_router()
    app.include_router(trade_bt_router)

    # Add trading module router (dashboard buy/sell)
    trading_router = create_trading_router()
    app.include_router(trading_router)

    # Add model management router
    model_router = create_model_router()
    app.include_router(model_router)

    # Add iQuant API router (isolated from main system, lazily initialized)
    iquant_router = create_iquant_router()
    app.include_router(iquant_router)
    app.state.iquant_router = iquant_router  # for shutdown cleanup

    # Mount static files if directory exists
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.on_event("startup")
    async def startup():
        import asyncio

        from src.data.database.fundamentals_db import create_fundamentals_db_from_config
        from src.web.routes import _run_intraday_monitor

        logger.info("Web UI started")
        store.start_cleanup_task()

        # Send Feishu startup notification
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            import os

            await bot.send_startup_notification(
                git_commit=os.environ.get("GIT_COMMIT"),
                git_branch=os.environ.get("GIT_BRANCH"),
            )

        # Shared fundamentals DB connection pool
        fundamentals_db = create_fundamentals_db_from_config()
        try:
            await fundamentals_db.connect()
            app.state.fundamentals_db = fundamentals_db
            logger.info("Shared fundamentals DB connected")
        except Exception as e:
            logger.error(f"Failed to connect shared fundamentals DB: {e}")
            app.state.fundamentals_db = None

        # GreptimeDB backtest cache — single shared instance
        from src.data.clients.greptime_backtest_cache import create_backtest_cache_from_config

        backtest_cache = create_backtest_cache_from_config()
        try:
            await backtest_cache.start()
            app.state.backtest_cache = backtest_cache
            logger.info("GreptimeDB backtest cache connected")

            # Inject shared cache into iQuant router
            iquant_rtr = getattr(app.state, "iquant_router", None)
            if iquant_rtr and hasattr(iquant_rtr, "_inject_cache"):
                iquant_rtr._inject_cache(backtest_cache)
        except Exception as e:
            logger.error(f"Failed to connect GreptimeDB backtest cache: {e}")
            app.state.backtest_cache = None

        # Auto-start iQuant monitoring (heartbeat, signal timeout, readiness)
        # CRITICAL: must start before anything else — safety monitoring cannot be skipped
        iquant_rtr = getattr(app.state, "iquant_router", None)
        if iquant_rtr and hasattr(iquant_rtr, "_start_monitoring"):
            iquant_rtr._start_monitoring()
            logger.info("iQuant monitoring scheduler started")

        # Run trading safety audit at startup → notify Feishu if CRITICAL
        try:
            from scripts.audit_trading_safety import run_audit

            audit_result = run_audit()
            critical_violations = [v for v in audit_result.violations if v.severity == "CRITICAL"]
            if critical_violations:
                logger.warning(
                    f"Trading safety audit: {len(critical_violations)} CRITICAL violations! "
                    f"Run 'uv run python scripts/audit_trading_safety.py' for details."
                )
                try:
                    from src.common.feishu_bot import FeishuBot

                    bot = FeishuBot()
                    if bot.is_configured():
                        details = "\n".join(
                            f"  [{v.category}] {v.file}:{v.line}" for v in critical_violations[:10]
                        )
                        await bot.send_message(
                            f"[交易安全审计] {len(critical_violations)} 个严重问题\n{details}"
                        )
                except Exception:
                    logger.warning("Failed to send Feishu safety audit alert")
        except Exception as e:
            logger.error(f"Trading safety audit failed: {e}", exc_info=True)

        # Auto-start cache scheduler (3am daily gap-fill)
        from src.data.services.cache_scheduler import CacheScheduler

        cache_scheduler = CacheScheduler(app.state)
        app.state.cache_scheduler = cache_scheduler
        app.state.cache_scheduler_task = asyncio.create_task(cache_scheduler.run())
        logger.info("Cache scheduler started (3am daily)")

        # Auto-start model training scheduler (finetune every 20 trading days)
        from src.data.services.model_training_scheduler import ModelTrainingScheduler

        model_scheduler = ModelTrainingScheduler(app.state)
        app.state.model_scheduler = model_scheduler
        app.state.model_scheduler_task = asyncio.create_task(model_scheduler.run())
        logger.info("Model training scheduler started")

        # Auto-start intraday momentum monitor as background task
        app.state.momentum_monitor_state = {
            "running": False,
            "last_scan_time": None,
            "last_result": None,
            "today_results": [],
            "task": None,
            "fundamentals_db": app.state.fundamentals_db,
            "_app_state": app.state,  # needed for backtest_cache access
        }
        task = asyncio.create_task(_run_intraday_monitor(app.state.momentum_monitor_state))
        app.state.momentum_monitor_state["task"] = task
        logger.info("Intraday momentum monitor auto-started")

    @app.on_event("shutdown")
    async def shutdown():
        logger.info("Web UI stopped")

        # Send Feishu shutdown notification
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_shutdown_notification()

        store.stop_cleanup_task()

        # Stop cache scheduler
        cache_task = getattr(app.state, "cache_scheduler_task", None)
        if cache_task and not cache_task.done():
            cache_task.cancel()
            logger.info("Cache scheduler stopped")

        # Stop model training scheduler
        model_task = getattr(app.state, "model_scheduler_task", None)
        if model_task and not model_task.done():
            model_task.cancel()
            logger.info("Model training scheduler stopped")

        # Stop momentum monitor
        monitor_state = getattr(app.state, "momentum_monitor_state", None)
        if monitor_state:
            task = monitor_state.get("task")
            if task and not task.done():
                task.cancel()
            monitor_state["running"] = False
            logger.info("Intraday momentum monitor stopped")

        # Close shared fundamentals DB
        fundamentals_db = getattr(app.state, "fundamentals_db", None)
        if fundamentals_db:
            await fundamentals_db.close()
            logger.info("Shared fundamentals DB closed")

        # Close GreptimeDB backtest cache
        backtest_cache = getattr(app.state, "backtest_cache", None)
        if backtest_cache:
            await backtest_cache.stop()
            logger.info("GreptimeDB backtest cache closed")

        # Cleanup iQuant isolated resources
        iquant_rtr = getattr(app.state, "iquant_router", None)
        if iquant_rtr and hasattr(iquant_rtr, "_iquant_cleanup"):
            await iquant_rtr._iquant_cleanup()

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
