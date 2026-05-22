import httpx

from src.trading.broker_client import BrokerClient


def _broker_with_readyz(payload: dict, status_code: int = 200) -> BrokerClient:
    broker = BrokerClient("http://broker.example", "secret")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/readyz"
        return httpx.Response(status_code, json=payload)

    broker._client = httpx.AsyncClient(
        base_url="http://broker.example",
        headers={"X-API-Key": "secret"},
        transport=httpx.MockTransport(handler),
    )
    return broker


async def test_readiness_error_accepts_ready_envelope():
    broker = _broker_with_readyz({"code": 0, "message": "ok", "data": {"ready": True}})
    try:
        assert await broker.readiness_error() is None
        assert await broker.is_ready() is True
    finally:
        await broker.stop()


async def test_readiness_error_rejects_business_not_ready():
    broker = _broker_with_readyz(
        {"code": 1001, "message": "trader not ready", "data": {"ready": False}}
    )
    try:
        assert await broker.readiness_error() == "trader not ready"
        assert await broker.is_ready() is False
    finally:
        await broker.stop()
