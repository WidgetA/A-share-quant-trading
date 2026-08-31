# === MODULE PURPOSE ===
# FastAPI application for trading confirmations.
# Provides REST API and serves HTML templates.

# === DEPENDENCIES ===
# - pending_store: For accessing pending confirmations
# - strategy_controller: For strategy start/stop control
# - position_manager: For displaying current positions
# - jinja2: For HTML template rendering

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
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
    create_v15_backtest_router,
)
from src.web.v15_scan_service import V15ScanState, inject_cache, start_scan_scheduler
from src.web.v20_routes import create_v20_router

if TYPE_CHECKING:
    from src.common.strategy_controller import StrategyController
    from src.trading.position_manager import PositionManager

logger = logging.getLogger(__name__)

# Template and static file directories
WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"
PROJECT_ROOT = WEB_DIR.parent.parent

_V20_FORWARD_SHADOW = "forward_shadow"
_V20_PRODUCTION_PUSH = "production_push"


def _create_default_v20_service() -> object:
    """Create V20 lazily so importing the legacy web app stays side-effect free."""

    from src.web.v20_service import V20Service

    return V20Service.from_default_config()


def _default_v20_mode_hint() -> str | None:
    """Read only enough intent to make a safe legacy-scheduler ownership choice.

    The real service factory still performs complete schema, artifact, activation,
    and checkpoint validation. This probe exists for the failure path: if a
    requested production configuration is invalid, V15 must remain off rather
    than silently taking over.
    """

    env_mode = os.environ.get("V20_MODE")
    if env_mode is not None:
        mode = env_mode.strip()
        return mode if mode in {_V20_FORWARD_SHADOW, _V20_PRODUCTION_PUSH} else None

    try:
        raw = yaml.safe_load((PROJECT_ROOT / "config" / "v20.yaml").read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return None
    if not isinstance(raw, dict):
        return None
    mode = str(raw.get("deployment_mode", "")).strip()
    return mode if mode in {_V20_FORWARD_SHADOW, _V20_PRODUCTION_PUSH} else None


def _v20_lifecycle_config(service: object) -> tuple[bool, str]:
    config = getattr(service, "config", None)
    enabled = getattr(config, "enabled", None)
    mode = getattr(config, "deployment_mode", None)
    if not isinstance(enabled, bool):
        raise ValueError("V20 service config.enabled must be bool")
    if mode not in {_V20_FORWARD_SHADOW, _V20_PRODUCTION_PUSH}:
        raise ValueError("V20 service deployment_mode is invalid")
    return enabled, mode


async def _start_v20_lifecycle(app: FastAPI) -> bool:
    """Start V20 and return whether the legacy V15 scan may also run.

    Production mode claims scan ownership before ``start`` is attempted. A
    failed production start therefore leaves both scanners inactive (visible as
    a V20 startup error) instead of falling back to an unintended strategy.
    """

    app.state.v20_service_started = False
    app.state.v20_service_lifecycle_owned = False
    app.state.v20_start_error = None

    service: Any = getattr(app.state, "v20_service", None)
    if service is None:
        mode_hint = _default_v20_mode_hint()
        try:
            service = _create_default_v20_service()
            app.state.v20_service = service
        except Exception as exc:
            app.state.v20_deployment_mode = mode_hint
            app.state.v20_start_error = f"{type(exc).__name__}: {exc}"
            logger.exception("Failed to create default V20 service")
            # Unknown intent is treated like production: absence of a verified
            # shadow declaration cannot authorize the legacy scanner.
            return mode_hint == _V20_FORWARD_SHADOW

    try:
        enabled, mode = _v20_lifecycle_config(service)
    except Exception as exc:
        app.state.v20_deployment_mode = None
        app.state.v20_start_error = f"{type(exc).__name__}: {exc}"
        logger.exception("Invalid V20 lifecycle configuration")
        return False

    app.state.v20_deployment_mode = mode
    legacy_scan_allowed = mode == _V20_FORWARD_SHADOW
    if not enabled:
        logger.info("V20 service disabled (mode=%s)", mode)
        return legacy_scan_allowed
    if mode == _V20_PRODUCTION_PUSH:
        app.state.v20_start_error = (
            "V20 production_push requires the dedicated src.web.v20_app host"
        )
        logger.critical(app.state.v20_start_error)
        return False

    app.state.v20_service_lifecycle_owned = True
    try:
        await service.start()
    except Exception as exc:
        app.state.v20_start_error = f"{type(exc).__name__}: {exc}"
        if mode == _V20_PRODUCTION_PUSH:
            logger.critical(
                "V20 production service failed to start; legacy V15 scan remains disabled",
                exc_info=True,
            )
        else:
            logger.exception("V20 forward-shadow service failed; keeping legacy V15 scan")
        return legacy_scan_allowed

    app.state.v20_service_started = True
    logger.info("V20 service started (mode=%s)", mode)
    return legacy_scan_allowed


async def _stop_v20_lifecycle(app: FastAPI) -> None:
    """Stop any enabled V20 service whose lifecycle this app attempted to own."""

    if not getattr(app.state, "v20_service_lifecycle_owned", False):
        return
    service = getattr(app.state, "v20_service", None)
    if service is None:
        return
    stop_result = (await asyncio.gather(service.stop(), return_exceptions=True))[0]
    if isinstance(stop_result, asyncio.CancelledError):
        raise stop_result
    if isinstance(stop_result, BaseException):
        app.state.v20_stop_error = f"{type(stop_result).__name__}: {stop_result}"
        logger.error(
            "Failed to stop V20 service cleanly",
            exc_info=(type(stop_result), stop_result, stop_result.__traceback__),
        )
    else:
        app.state.v20_stop_error = None
        logger.info("V20 service stopped")
    app.state.v20_service_started = False


async def _start_strategy_services(app: FastAPI) -> None:
    """Start iQuant monitoring plus the single permitted scan owner(s)."""

    # iQuant trading monitoring is independent of scan ownership and must
    # remain active in every V20 deployment mode.
    iquant_router = getattr(app.state, "iquant_router", None)
    if iquant_router and hasattr(iquant_router, "_start_monitoring"):
        iquant_router._start_monitoring()
        logger.info("iQuant V15 monitoring scheduler started")

    legacy_scan_allowed = await _start_v20_lifecycle(app)
    app.state.legacy_v15_scan_allowed = legacy_scan_allowed
    v15_scan_state = getattr(app.state, "v15_scan_state", None)
    if legacy_scan_allowed and v15_scan_state:
        start_scan_scheduler(v15_scan_state)
        logger.info("V15 scan scheduler started alongside V20 shadow/disabled mode")
    elif not legacy_scan_allowed:
        logger.warning("Legacy V15 scan scheduler disabled by V20 ownership policy")


def create_app(
    store: PendingConfirmationStore | None = None,
    web_base_url: str = "http://localhost:8000",
    strategy_controller: StrategyController | None = None,
    position_manager: PositionManager | None = None,
    v20_service: object | None = None,
) -> FastAPI:
    """
    Create FastAPI application.

    Args:
        store: Pending confirmation store. Uses global singleton if not provided.
        web_base_url: Base URL for generating links in notifications.
        strategy_controller: Controller for strategy start/stop.
        position_manager: Manager for position data.
        v20_service: Optional V20 decision-notification service. When omitted,
            startup creates the service from ``config/v20.yaml``.

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
    app.state.v20_service = v20_service

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

    # Add momentum backtest router
    momentum_router = create_momentum_router()
    app.include_router(momentum_router)

    # Add settings router
    settings_router = create_settings_router()
    app.include_router(settings_router)

    # Add trade backtest router (CSV upload → stats)
    trade_bt_router = create_trade_backtest_router()
    app.include_router(trade_bt_router)

    # Add V15 backtest router
    v15_bt_router = create_v15_backtest_router()
    app.include_router(v15_bt_router)

    # Add iQuant API router (trading only, scan is separate)
    iquant_router = create_iquant_router()
    app.include_router(iquant_router)
    app.state.iquant_router = iquant_router  # for shutdown cleanup

    # V20 is a decision/notification boundary only; scan ownership and service
    # lifecycle are resolved explicitly by the startup hook below.
    app.include_router(create_v20_router())

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
        from src.data.clients.ifind_http_client import IFinDHttpClient
        from src.data.database.fundamentals_db import create_fundamentals_db_from_config

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

        # Tushare backtest cache — load from OSS in background (non-blocking)
        app.state.tushare_cache = None
        app.state.tushare_cache_loading = True  # frontend polls this

        async def _bg_load_oss_cache():
            try:
                from src.data.clients.tushare_backtest_cache import TushareBacktestCache

                logger.info("Loading tushare cache from OSS (background)...")
                oss_cache = await asyncio.to_thread(TushareBacktestCache.load_from_oss)
                if oss_cache:
                    app.state.tushare_cache = oss_cache
                    logger.info(
                        f"Tushare cache pre-loaded from OSS: "
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
                logger.warning(f"Failed to pre-load tushare cache from OSS: {e}")
            finally:
                app.state.tushare_cache_loading = False

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

        # V20 production owns scan decisions exclusively; forward shadow may
        # coexist with the legacy V15 scanner. iQuant monitoring always runs.
        await _start_strategy_services(app)

    @app.on_event("shutdown")
    async def shutdown():
        logger.info("Web UI stopped")
        store.stop_cleanup_task()

        await _stop_v20_lifecycle(app)

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
