# === MODULE PURPOSE ===
# FastAPI application for trading confirmations.
# Provides REST API and serves HTML templates.

# === DEPENDENCIES ===
# - pending_store: For accessing pending confirmations
# - position_manager: For displaying current positions
# - jinja2: For HTML template rendering

from __future__ import annotations

import logging
import os
import sys
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

# Configure root logger so all project loggers (src.data.*, src.strategy.*, etc.)
# emit to stdout. Without this, uvicorn only configures its own `uvicorn` /
# `uvicorn.access` loggers, and every other `logging.getLogger(__name__)` call
# in the codebase goes to a no-op handler — which silently swallowed the slow
# SQL watchdog warnings we added to diagnose download hangs.
#
# Idempotent: if root already has handlers (e.g. when imported by pytest or
# another entry point that already configured logging), do nothing.
_root_logger = logging.getLogger()
if not _root_logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    _root_logger.addHandler(_handler)
    _root_logger.setLevel(logging.INFO)

logger = logging.getLogger(__name__)

# Template and static file directories
WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"


def create_app(
    store: PendingConfirmationStore | None = None,
    web_base_url: str | None = None,
) -> FastAPI:
    """
    Create FastAPI application.

    Args:
        store: Pending confirmation store. Uses global singleton if not provided.
        web_base_url: Base URL for generating links in notifications.
            Falls back to WEB_BASE_URL env var, then "http://localhost:8000".

    Returns:
        Configured FastAPI app.
    """
    if web_base_url is None:
        web_base_url = os.getenv("WEB_BASE_URL", "http://172.19.248.223:8000")
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

        from src.web.routes import _run_intraday_monitor

        logger.info("Web UI started")
        store.start_cleanup_task()
        app.state.active_download = None  # download_task.ActiveDownload | None
        app.state.download_lock = asyncio.Lock()  # guards check+start as atomic op
        app.state.cache_fill_running = False  # True while check_and_fill_gaps is active

        # Send Feishu startup notification
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            import os

            await bot.send_startup_notification(
                git_commit=os.environ.get("GIT_COMMIT"),
                git_branch=os.environ.get("GIT_BRANCH"),
            )

        # GreptimeDB storage + cache pipeline — wired here so the dependency
        # graph (storage → sources → aggregator → reporter → pipeline) lives
        # in one place. Routes / schedulers consume them via app.state.
        from src.data.clients.greptime_storage import create_storage_from_config
        from src.data.services.cache_pipeline import CachePipeline
        from src.data.services.cache_progress_reporter import CacheProgressReporter
        from src.data.sources.tsanghi_daily_source import TsanghiDailySource
        from src.data.sources.tushare_metadata_source import TushareMetadataSource
        from src.data.sources.tushare_minute_source import TushareMinuteSource

        storage = create_storage_from_config()
        try:
            await storage.start()
            app.state.storage = storage
            app.state.pipeline = CachePipeline(
                storage=storage,
                daily_source=TsanghiDailySource(),
                minute_source=TushareMinuteSource(),
                metadata_source=TushareMetadataSource(),
                reporter=CacheProgressReporter(),
            )
            logger.info("GreptimeDB storage + cache pipeline ready")

            # Inject shared storage into iQuant router
            iquant_rtr = getattr(app.state, "iquant_router", None)
            if iquant_rtr and hasattr(iquant_rtr, "_inject_storage"):
                iquant_rtr._inject_storage(storage)
        except Exception as e:
            logger.error(f"Failed to connect GreptimeDB storage: {e}")
            app.state.storage = None
            app.state.pipeline = None

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
        await model_scheduler.sync_model_from_s3()
        app.state.model_scheduler = model_scheduler
        app.state.model_scheduler_task = asyncio.create_task(model_scheduler.run())
        logger.info("Model training scheduler started")

        # Auto-start intraday momentum monitor as background task
        app.state.momentum_monitor_state = {
            "running": False,
            "last_scan_time": None,
            "last_scan_result": None,  # "success" | "failed" | "no_result"
            "last_scan_message": None,
            "last_result": None,
            "today_results": [],
            "task": None,
            "_app_state": app.state,  # needed for storage access
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

        # Close GreptimeDB storage pool
        storage = getattr(app.state, "storage", None)
        if storage:
            await storage.stop()
            logger.info("GreptimeDB storage closed")

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
