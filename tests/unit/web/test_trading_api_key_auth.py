from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from src.web.routes import verify_trading_api_key
from src.web.trading_key_cookie import (
    TRADING_API_KEY_COOKIE,
    install_trading_api_key_cookie_middleware,
)


def _client_with_key(monkeypatch, configured_key: str | None = "secret-key") -> TestClient:
    monkeypatch.setattr("src.common.config.get_trading_api_key", lambda: configured_key)

    app = FastAPI()
    install_trading_api_key_cookie_middleware(app)

    @app.get("/api/trading/protected", dependencies=[Depends(verify_trading_api_key)])
    async def protected_route():
        return {"ok": True}

    return TestClient(app)


def test_trading_route_accepts_matching_cookie(monkeypatch):
    client = _client_with_key(monkeypatch)
    client.cookies.set(TRADING_API_KEY_COOKIE, "secret-key")

    response = client.get("/api/trading/protected")

    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_trading_route_still_rejects_missing_key(monkeypatch):
    client = _client_with_key(monkeypatch)

    response = client.get("/api/trading/protected")

    assert response.status_code == 401


def test_trading_route_still_accepts_matching_header(monkeypatch):
    client = _client_with_key(monkeypatch)

    response = client.get("/api/trading/protected", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_middleware_sets_persisted_key_cookie(monkeypatch):
    client = _client_with_key(monkeypatch)

    response = client.get("/api/trading/protected", headers={"X-API-Key": "secret-key"})

    assert response.status_code == 200
    assert response.cookies.get(TRADING_API_KEY_COOKIE) == "secret-key"
