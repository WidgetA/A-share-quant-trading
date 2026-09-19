"""Authenticated configuration and selection of the Web trading channel."""

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, SecretStr

from src.trading.broker_client import BrokerError
from src.trading.broker_switch import BrokerSwitch
from src.trading.channel_store import default_store
from src.trading.qmt_http_client import QmtHttpClient, validate_qmt_config


class QmtSettings(BaseModel):
    url: str
    instance_id: str
    key_id: str
    secret: SecretStr = SecretStr("")
    ca_file: str | None = None


class ChannelSelection(BaseModel):
    backend: str


def clear_broker_cache(app):
    for name in ("broker_positions", "broker_orders"):
        setattr(app.state, name, [])
    for name in (
        "available_cash",
        "broker_total_asset",
        "broker_market_value",
        "broker_frozen_cash",
    ):
        setattr(app.state, name, None)
    app.state.broker_account_id = None
    app.state.broker_positions_updated_at = None
    app.state.broker_filled_orders_fingerprint = ()
    app.state._equity_snapshot_last_write = 0.0
    app.state.broker_last_error = "正在刷新所选交易通道"
    app.state.broker_orders_last_error = "正在刷新所选交易通道"


def create_broker_channel_router():
    from src.web.routes import verify_trading_api_key

    router = APIRouter(dependencies=[Depends(verify_trading_api_key)])

    def config(body):
        store = default_store()
        spec = body.model_dump()
        spec["secret"] = body.secret.get_secret_value()
        if not spec["secret"]:
            route = store.preference("qmt")
            if route:
                previous = store.profile(route)
                if all(previous[k] == spec[k] for k in ("url", "instance_id", "key_id")):
                    spec["secret"] = previous["secret"]
        return validate_qmt_config(spec)

    @router.get("/api/settings/trading-channel")
    async def status(request: Request):
        store = default_store()
        active = getattr(request.app.state, "broker", None)
        profiles = {}
        for backend in ("miniqmt", "qmt"):
            route = store.preference(backend)
            spec = store.profile(route) if route else {}
            profiles[backend] = {k: v for k, v in spec.items() if k not in ("secret", "api_key")}
            profiles[backend]["configured"] = bool(route)
        return {
            "backend": active.backend if isinstance(active, BrokerSwitch) else store.backend,
            "channel": active.route
            if isinstance(active, BrokerSwitch)
            else store.preference("active_route", ""),
            "profiles": profiles,
        }

    @router.post("/api/settings/trading-channel/qmt")
    async def save_qmt(body: QmtSettings):
        try:
            spec = config(body)
            # Validate CA loading before persisting; this performs no network or trade operation.
            client = QmtHttpClient(spec)
            await client.start()
            await client.stop()
            default_store().save_profile(spec)
            return {"success": True, "message": "QMT 配置已保存，选择 QMT 后生效"}
        except BrokerError as exc:
            raise HTTPException(400, exc.message) from None

    @router.post("/api/settings/trading-channel/qmt/test")
    async def test_qmt(body: QmtSettings):
        client = None
        try:
            client = QmtHttpClient(config(body))
            await client.start()
            await client.snapshot()
            ready = await client.is_ready()
            return {
                "success": True,
                "trade_ready": ready,
                "message": "QMT 已连接，可交易" if ready else "QMT 已连接，当前未开放交易",
            }
        except BrokerError as exc:
            return {"success": False, "message": exc.message}
        finally:
            if client:
                await client.stop()

    @router.post("/api/settings/trading-channel")
    async def select(request: Request, body: ChannelSelection):
        if body.backend not in ("miniqmt", "qmt"):
            raise HTTPException(400, "请选择 miniQMT 或 QMT")
        # Serialize selections, including cache invalidation/warmup, across browser tabs.
        if not hasattr(request.app.state, "broker_selection_lock"):
            request.app.state.broker_selection_lock = asyncio.Lock()
        async with request.app.state.broker_selection_lock:
            active = getattr(request.app.state, "broker", None)
            created = not isinstance(active, BrokerSwitch)
            if isinstance(active, BrokerSwitch):
                broker = active
            else:
                broker = BrokerSwitch()
                broker.import_legacy_config()
            try:
                await broker.select(body.backend)
            except BrokerError as exc:
                if created:
                    await broker.stop()
                raise HTTPException(409, exc.message) from None
            request.app.state.broker = broker
            clear_broker_cache(request.app)
            init = getattr(request.app.state, "init_broker", None)
            if init:
                await init(request.app)
            return {
                "success": True,
                "backend": broker.backend,
                "channel": broker.route,
                "message": f"已切换到 {'QMT' if broker.backend == 'qmt' else 'miniQMT'}",
            }

    return router
