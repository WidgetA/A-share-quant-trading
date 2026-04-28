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
from src.trading.broker_client import BrokerClient
from src.web.ml_routes import create_ml_router
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


async def _try_connect_greptime(app: FastAPI) -> bool:
    """Try to connect GreptimeDB and wire up storage + pipeline.

    On success: sets app.state.storage, app.state.pipeline, injects into
    ML router, and returns True.
    On failure: leaves app.state.storage/pipeline as None and returns False.
    """
    from src.data.clients.greptime_storage import create_storage_from_config
    from src.data.services.cache_pipeline import CachePipeline
    from src.data.services.cache_progress_reporter import CacheProgressReporter
    from src.data.sources.tsanghi_daily_source import TsanghiDailySource
    from src.data.sources.tushare_metadata_source import TushareMetadataSource
    from src.data.sources.tushare_minute_source import TushareMinuteSource

    storage = create_storage_from_config()
    try:
        await storage.start()
    except Exception as e:
        logger.warning(f"GreptimeDB not available: {e}")
        return False

    app.state.storage = storage
    app.state.pipeline = CachePipeline(
        storage=storage,
        daily_source=TsanghiDailySource(),
        minute_source=TushareMinuteSource(),
        metadata_source=TushareMetadataSource(),
        reporter=CacheProgressReporter(),
    )
    logger.info("GreptimeDB storage + cache pipeline ready")

    # Inject shared storage into ML router
    ml_rtr = getattr(app.state, "ml_router", None)
    if ml_rtr and hasattr(ml_rtr, "_inject_storage"):
        ml_rtr._inject_storage(storage)
    return True


async def _broker_fetch_once(app: FastAPI) -> str | None:
    """Fetch positions + account once and update app.state. Returns error string or None."""
    broker: BrokerClient | None = getattr(app.state, "broker", None)
    if broker is None:
        return "broker not initialized"
    try:
        positions = await broker.get_positions()
        account = await broker.get_account()
    except Exception as e:
        return f"{type(e).__name__}: {e}"
    app.state.broker_positions = [
        {
            "code": p.code,
            "volume": p.volume,
            "can_use_volume": p.can_use_volume,
            "avg_price": p.avg_price,
            "market_value": p.market_value,
        }
        for p in positions
        if p.volume > 0
    ]
    app.state.available_cash = account.cash
    app.state.broker_total_asset = account.total_asset
    app.state.broker_account_id = account.account_id
    return None


async def _init_broker(app: FastAPI) -> tuple[bool, str]:
    """(Re)initialize BrokerClient from persisted config.

    Stops any existing broker, loads config, creates+starts a new client,
    runs a synchronous warmup fetch, and ensures the poll loop is running.
    Returns (ok, message). `ok` reflects connection (not just construction).
    """
    import asyncio

    from src.common.config import get_xtquant_api_key, get_xtquant_server_url

    old: BrokerClient | None = getattr(app.state, "broker", None)
    if old is not None:
        app.state.broker = None
        try:
            await old.stop()
        except Exception as e:
            logger.warning(f"Failed to stop old BrokerClient: {e}")

    try:
        url = get_xtquant_server_url()
        key = get_xtquant_api_key()
    except ValueError as e:
        app.state.broker_last_error = f"配置缺失: {e}"
        return False, f"配置缺失: {e}"

    broker = BrokerClient(url, key)
    try:
        await broker.start()
    except Exception as e:
        app.state.broker_last_error = f"启动 BrokerClient 失败: {e}"
        return False, f"启动 BrokerClient 失败: {e}"
    app.state.broker = broker

    # Synchronous warmup — verifies the URL is actually reachable and returns
    # data so the user sees an immediate result instead of waiting 30s.
    err = await _broker_fetch_once(app)
    if err:
        app.state.broker_last_error = err
        logger.warning(f"BrokerClient warmup fetch failed: {err}")
    else:
        app.state.broker_last_error = None

    poll_task = getattr(app.state, "_broker_poll_task", None)
    if poll_task is None or poll_task.done():
        app.state._broker_poll_task = asyncio.create_task(_broker_position_poll_loop(app))
    logger.info(f"BrokerClient (re)initialized: {url} (warmup_err={err})")
    if err:
        return False, f"已配置但无法获取数据: {err}"
    return True, url


async def _broker_position_poll_loop(app: FastAPI) -> None:
    """Poll xtquant-trade-server for positions/cash every 30s and cache in app.state."""
    import asyncio

    while True:
        broker: BrokerClient | None = getattr(app.state, "broker", None)
        if broker is None:
            await asyncio.sleep(30)
            continue
        err = await _broker_fetch_once(app)
        if err:
            app.state.broker_last_error = err
            logger.warning(f"Broker position poll failed: {err}")
        else:
            app.state.broker_last_error = None
        await asyncio.sleep(30)


async def _greptime_reconnect_loop(app: FastAPI) -> None:
    """Background task: retry connecting GreptimeDB with exponential backoff.

    Runs until connection succeeds or the task is cancelled (shutdown).
    """
    import asyncio

    delay = 5
    while True:
        await asyncio.sleep(delay)
        if await _try_connect_greptime(app):
            logger.info("GreptimeDB connected via background retry")
            return
        delay = min(delay * 2, 60)
        logger.warning(f"GreptimeDB still unavailable, next retry in {delay}s")


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

    # Add ML strategy router (scan, backtest, quote, universe)
    ml_router = create_ml_router()
    app.include_router(ml_router)
    app.state.ml_router = ml_router  # for shutdown cleanup

    # Mount static files if directory exists
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.on_event("startup")
    async def startup():
        import asyncio

        from src.web.routes import _run_intraday_monitor

        logger.info("Web UI started")
        store.start_cleanup_task()

        # Initialize xtquant-trade-server broker client
        app.state.broker = None
        app.state.broker_positions = []
        app.state.available_cash = 0.0
        app.state._broker_poll_task = None
        app.state.init_broker = _init_broker  # exposed for runtime reload via Settings UI
        ok, msg = await _init_broker(app)
        if not ok:
            logger.warning(f"Broker not initialized, trading disabled: {msg}")

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
        #
        # Uses a background retry loop: if GreptimeDB is unavailable at startup,
        # a background task keeps retrying with exponential backoff. Once connected
        # it sets app.state.storage / pipeline so all consumers pick it up.
        app.state.storage = None
        app.state.pipeline = None

        if not await _try_connect_greptime(app):
            app.state._greptime_reconnect_task = asyncio.create_task(_greptime_reconnect_loop(app))

        # Auto-start ML monitoring scheduler (readiness, broker alerts)
        # CRITICAL: must start before anything else — safety monitoring cannot be skipped
        ml_rtr = getattr(app.state, "ml_router", None)
        if ml_rtr and hasattr(ml_rtr, "_start_monitoring"):
            ml_rtr._start_monitoring(app.state)
            logger.info("ML monitoring scheduler started")

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

        # Cancel GreptimeDB reconnect task if still running
        reconnect_task = getattr(app.state, "_greptime_reconnect_task", None)
        if reconnect_task and not reconnect_task.done():
            reconnect_task.cancel()
            logger.info("GreptimeDB reconnect task cancelled")

        # Close GreptimeDB storage pool
        storage = getattr(app.state, "storage", None)
        if storage:
            await storage.stop()
            logger.info("GreptimeDB storage closed")

        # Stop broker position poll
        broker_poll = getattr(app.state, "_broker_poll_task", None)
        if broker_poll and not broker_poll.done():
            broker_poll.cancel()

        # Stop broker client
        broker = getattr(app.state, "broker", None)
        if broker:
            await broker.stop()
            logger.info("BrokerClient stopped")

        # Cleanup ML router resources
        ml_rtr = getattr(app.state, "ml_router", None)
        if ml_rtr and hasattr(ml_rtr, "_ml_cleanup"):
            await ml_rtr._ml_cleanup()

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
