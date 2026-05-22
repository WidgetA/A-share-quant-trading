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
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.common.pending_store import PendingConfirmationStore, get_pending_store
from src.trading.broker_client import BrokerClient
from src.web.analysis_routes import create_analysis_router
from src.web.audit_routes import create_audit_router
from src.web.broker_order_routes import create_broker_order_cache_router
from src.web.ml_routes import create_ml_router
from src.web.notes_routes import create_notes_router
from src.web.routes import (
    create_model_router,
    create_momentum_router,
    create_router,
    create_settings_router,
    create_trade_backtest_router,
    create_trading_router,
)
from src.web.trading_key_cookie import install_trading_api_key_cookie_middleware

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

# Forward ERROR/CRITICAL log records to Feishu so silent failures (tsanghi
# 余额不足, API hangs, etc.) page out instead of staying in container logs.
# See src/common/feishu_log_handler.py for throttling details.
from src.common.feishu_log_handler import install_root_handler as _install_feishu  # noqa: E402

_install_feishu()

logger = logging.getLogger(__name__)
BEIJING_TZ = ZoneInfo("Asia/Shanghai")
_POSITION_AFFECTING_ORDER_STATUSES = {"FILLED", "PARTIALLY_FILLED", "PARTIAL_FILLED"}

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
    from src.data.sources.tushare_daily_source import TushareDailySource
    from src.data.sources.tushare_metadata_source import TushareMetadataSource
    from src.data.sources.tushare_minute_source import TushareMinuteSource

    storage = create_storage_from_config()
    try:
        await storage.start()
    except Exception as e:
        logger.warning(f"GreptimeDB not available: {e}")
        return False

    app.state.storage = storage

    # Ensure trade_notes table exists (NOTE-001). Failure here is non-fatal —
    # the notes UI will return 503 until the table appears, but the rest of
    # the app keeps working.
    try:
        from src.notes.note_store import TradeNoteStore

        await TradeNoteStore(storage).ensure_schema()
    except Exception as e:
        logger.exception(f"trade_notes schema ensure failed: {e}")

    app.state.pipeline = CachePipeline(
        storage=storage,
        daily_source=TushareDailySource(),
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
    app.state.broker_positions_updated_at = time.time()
    return None


def _filter_orders_for_beijing_date(
    orders: list[dict],
    target_date: date | None = None,
) -> list[dict]:
    """Keep orders for a Beijing trading date; retain rows with no parseable submit_time."""
    if target_date is None:
        target_date = datetime.now(BEIJING_TZ).date()

    filtered: list[dict] = []
    for order in orders:
        raw_submit_time = order.get("submit_time")
        if not raw_submit_time:
            filtered.append(order)
            continue

        try:
            if isinstance(raw_submit_time, datetime):
                submit_dt = raw_submit_time
            else:
                submit_text = str(raw_submit_time).strip()
                if submit_text.endswith("Z"):
                    submit_text = f"{submit_text[:-1]}+00:00"
                submit_dt = datetime.fromisoformat(submit_text)
            if submit_dt.tzinfo is None:
                submit_dt = submit_dt.replace(tzinfo=BEIJING_TZ)
            submit_date = submit_dt.astimezone(BEIJING_TZ).date()
        except (TypeError, ValueError):
            filtered.append(order)
            continue

        if submit_date == target_date:
            filtered.append(order)

    return filtered


def _filled_order_fingerprint(orders: list[dict]) -> tuple[tuple[str, ...], ...]:
    """Return a stable signature for filled orders that should affect positions."""
    items: list[tuple[str, ...]] = []
    for order in orders:
        status = str(order.get("status") or "").upper()
        if status not in _POSITION_AFFECTING_ORDER_STATUSES:
            continue
        order_key = order.get("order_id") or order.get("seq")
        if order_key is None:
            order_key = ":".join(
                str(order.get(key) or "")
                for key in ("code", "side", "qty", "traded_qty", "submit_time")
            )
        items.append(
            (
                str(order_key),
                str(order.get("code") or ""),
                str(order.get("side") or ""),
                str(order.get("qty") or ""),
                str(order.get("traded_qty") or ""),
                str(order.get("avg_traded_price") or ""),
                str(order.get("submit_time") or ""),
            )
        )
    return tuple(sorted(items))


async def _refresh_broker_positions_for_order_change(app: FastAPI) -> None:
    """Refresh position/account cache after order state changes."""
    err = await _broker_fetch_once(app)
    if err:
        app.state.broker_last_error = err
        logger.warning(f"Broker position refresh after order sync failed: {err}")
    else:
        app.state.broker_last_error = None


async def _broker_fetch_orders_once(app: FastAPI) -> str | None:
    """Fetch today's broker orders, cache them, and import filled orders into notes."""
    broker: BrokerClient | None = getattr(app.state, "broker", None)
    if broker is None:
        return "broker not initialized"
    try:
        raw_orders = await broker.get_orders()
    except Exception as e:
        return f"{type(e).__name__}: {e}"

    orders = _filter_orders_for_beijing_date(raw_orders)
    if len(orders) != len(raw_orders):
        logger.info(
            "Broker order sync filtered non-today orders: kept=%d raw=%d",
            len(orders),
            len(raw_orders),
        )
    app.state.broker_orders = orders

    filled_fingerprint = _filled_order_fingerprint(orders)
    previous_fingerprint = getattr(app.state, "broker_filled_orders_fingerprint", ())
    app.state.broker_filled_orders_fingerprint = filled_fingerprint
    if filled_fingerprint != previous_fingerprint:
        await _refresh_broker_positions_for_order_change(app)

    storage = getattr(app.state, "storage", None)
    if storage is None or not getattr(storage, "is_ready", False):
        return None

    try:
        from src.notes.note_store import TradeNoteStore

        written, skipped = await TradeNoteStore(storage).import_filled_orders_from_list(orders)
        if written:
            logger.info(
                "Broker order sync imported filled orders into trade_notes: written=%d skipped=%d",
                written,
                skipped,
            )
    except Exception as e:
        logger.warning(f"Broker order sync failed to import trade notes: {e}", exc_info=True)
    return None


async def _broker_post_order_refresh_loop(app: FastAPI) -> None:
    """Refresh broker snapshots shortly after an explicit order/cancel request."""
    import asyncio

    for delay in (0.5, 2.0, 5.0, 10.0):
        await asyncio.sleep(delay)
        order_err = await _broker_fetch_orders_once(app)
        if order_err:
            app.state.broker_orders_last_error = order_err
            logger.warning(f"Broker post-order order refresh failed: {order_err}")
        else:
            app.state.broker_orders_last_error = None

        # Fill/cancel settlement can update cash and can-use volume even before
        # the filled-order fingerprint changes, so always refresh positions too.
        await _refresh_broker_positions_for_order_change(app)


def schedule_broker_post_order_refresh(app: FastAPI) -> None:
    """Schedule backend broker cache refresh after a submitted order."""
    import asyncio

    task = getattr(app.state, "_broker_post_order_refresh_task", None)
    if task is not None and not task.done():
        return
    app.state._broker_post_order_refresh_task = asyncio.create_task(
        _broker_post_order_refresh_loop(app)
    )


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

    order_err = None
    if err is None:
        order_err = await _broker_fetch_orders_once(app)
    if err is not None:
        app.state.broker_orders_last_error = err
    elif order_err:
        app.state.broker_orders_last_error = order_err
        logger.warning(f"BrokerClient warmup order fetch failed: {order_err}")
    else:
        app.state.broker_orders_last_error = None

    poll_task = getattr(app.state, "_broker_poll_task", None)
    if poll_task is None or poll_task.done():
        app.state._broker_poll_task = asyncio.create_task(_broker_position_poll_loop(app))
    order_poll_task = getattr(app.state, "_broker_order_poll_task", None)
    if order_poll_task is None or order_poll_task.done():
        app.state._broker_order_poll_task = asyncio.create_task(_broker_order_poll_loop(app))
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


async def _broker_order_poll_loop(app: FastAPI) -> None:
    """Poll xtquant-trade-server orders and import fills independent of browser views."""
    import asyncio

    while True:
        broker: BrokerClient | None = getattr(app.state, "broker", None)
        if broker is None:
            await asyncio.sleep(30)
            continue
        err = await _broker_fetch_orders_once(app)
        if err:
            app.state.broker_orders_last_error = err
            logger.warning(f"Broker order poll failed: {err}")
        else:
            app.state.broker_orders_last_error = None
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
    install_trading_api_key_cookie_middleware(app)

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

    # Dashboard order display is cache-only; register before the trading router's
    # legacy broker-backed route so browser polling cannot own order sync.
    broker_order_cache_router = create_broker_order_cache_router()
    app.include_router(broker_order_cache_router)

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

    # Add analysis router (vision LLM K-line analysis via overseas Lambda)
    analysis_router = create_analysis_router()
    app.include_router(analysis_router)

    # Add trade notes router (NOTE-001: per-stock event log + UI)
    notes_router = create_notes_router()
    app.include_router(notes_router)

    # Add audit router (kimi credentials + listing info uploads)
    audit_router = create_audit_router()
    app.include_router(audit_router)

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
        app.state.broker_positions_updated_at = None
        app.state.broker_orders = []
        app.state.broker_filled_orders_fingerprint = ()
        app.state.available_cash = 0.0
        app.state._broker_poll_task = None
        app.state._broker_order_poll_task = None
        app.state._broker_post_order_refresh_task = None
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

        # Auto-start pre-market holdings report scheduler (8am daily, ANA-002)
        from src.data.services.pre_market_report_scheduler import PreMarketReportScheduler

        pre_market_scheduler = PreMarketReportScheduler(app.state)
        app.state.pre_market_report_scheduler = pre_market_scheduler
        app.state.pre_market_report_scheduler_task = asyncio.create_task(pre_market_scheduler.run())
        logger.info("Pre-market report scheduler started (8am daily)")

        # Auto-start intraday momentum monitor as background task
        app.state.momentum_monitor_state = {
            "running": False,
            "last_scan_time": None,
            "last_scan_result": None,  # "success" | "failed" | "no_result"
            "last_scan_message": None,
            "last_result": None,
            "today_recommendations_date": None,
            "today_recommendations": [],
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

        # Stop pre-market report scheduler
        pre_market_task = getattr(app.state, "pre_market_report_scheduler_task", None)
        if pre_market_task and not pre_market_task.done():
            pre_market_task.cancel()
            logger.info("Pre-market report scheduler stopped")

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

        # Stop broker order sync poll
        broker_order_poll = getattr(app.state, "_broker_order_poll_task", None)
        if broker_order_poll and not broker_order_poll.done():
            broker_order_poll.cancel()

        broker_post_order_refresh = getattr(app.state, "_broker_post_order_refresh_task", None)
        if broker_post_order_refresh and not broker_post_order_refresh.done():
            broker_post_order_refresh.cancel()

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
