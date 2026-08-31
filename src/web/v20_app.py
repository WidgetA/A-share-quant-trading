"""Minimal ASGI host for the V20 decision-notification boundary.

This module intentionally does not import the legacy web application, iQuant
routes, PositionManager, or any order/holding API.  The V20 service itself is
loaded lazily inside the application lifespan.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from src.web.v20_routes import create_v20_router

ServiceFactory = Callable[[], Any]


def _create_default_v20_service() -> Any:
    from src.web.v20_service import V20Service

    return V20Service.from_default_config()


def _service_enabled(service: Any) -> bool:
    enabled = getattr(getattr(service, "config", None), "enabled", None)
    if not isinstance(enabled, bool):
        raise ValueError("V20 service config.enabled must be bool")
    return enabled


def create_v20_app(
    *,
    v20_service: Any | None = None,
    service_factory: ServiceFactory | None = None,
) -> FastAPI:
    """Create the four-route V20 host with no platform execution surface."""

    if v20_service is not None and service_factory is not None:
        raise ValueError("provide either v20_service or service_factory, not both")

    factory = service_factory or _create_default_v20_service

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        service = v20_service if v20_service is not None else factory()
        app.state.v20_service = service
        started = False
        if _service_enabled(service):
            await service.start()
            started = True
        try:
            yield
        finally:
            if started:
                await service.stop()

    app = FastAPI(
        title="V20 Decision Notifications",
        version="20",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.v20_service = v20_service
    app.include_router(create_v20_router())
    return app


app = create_v20_app()

__all__ = ["app", "create_v20_app"]
