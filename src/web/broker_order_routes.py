"""Read-only dashboard routes backed by the broker order sync cache."""

from fastapi import APIRouter, Depends, Request

from src.web.routes import verify_trading_api_key


def create_broker_order_cache_router() -> APIRouter:
    """Expose cached broker orders without doing broker I/O or note imports."""
    router = APIRouter(tags=["trading"], dependencies=[Depends(verify_trading_api_key)])

    @router.get("/api/trading/orders")
    async def get_orders(request: Request) -> dict:
        orders = getattr(request.app.state, "broker_orders", [])
        return {"orders": orders}

    return router
