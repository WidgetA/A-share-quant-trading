# === MODULE PURPOSE ===
# AST-001 safety gate: the assistant's dedicated read-only key must open ONLY
# the allowlisted GET endpoints (holdings) and NOTHING else — order endpoints
# reject it in every combination. This is the property that makes it safe to
# hand the key to kimi: prompt injection cannot turn it into a trading key.

from __future__ import annotations

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from src.web.routes import verify_trading_api_key

TRADING_KEY = "trading-secret"
ASSISTANT_KEY = "assistant-ro"


def _client(monkeypatch, assistant_key: str | None = ASSISTANT_KEY) -> TestClient:
    monkeypatch.setattr("src.common.config.get_trading_api_key", lambda: TRADING_KEY)
    monkeypatch.setattr("src.common.config.get_assistant_readonly_key", lambda: assistant_key)

    app = FastAPI()
    dep = [Depends(verify_trading_api_key)]

    @app.get("/api/trading/holdings", dependencies=dep)
    async def holdings():
        return {"holdings": []}

    @app.post("/api/trading/holdings", dependencies=dep)
    async def holdings_post():  # same path, wrong method — must stay closed
        return {"ok": True}

    @app.post("/api/trading/buy", dependencies=dep)
    async def buy():
        return {"ok": True}

    @app.get("/api/trading/orders", dependencies=dep)
    async def orders():  # GET but NOT in the allowlist
        return {"orders": []}

    return TestClient(app)


def test_assistant_key_opens_holdings_get(monkeypatch):
    client = _client(monkeypatch)
    r = client.get("/api/trading/holdings", headers={"X-API-Key": ASSISTANT_KEY})
    assert r.status_code == 200


def test_assistant_key_rejected_on_order_endpoint(monkeypatch):
    client = _client(monkeypatch)
    r = client.post("/api/trading/buy", headers={"X-API-Key": ASSISTANT_KEY})
    assert r.status_code == 401


def test_assistant_key_rejected_on_non_allowlisted_get(monkeypatch):
    client = _client(monkeypatch)
    r = client.get("/api/trading/orders", headers={"X-API-Key": ASSISTANT_KEY})
    assert r.status_code == 401


def test_assistant_key_rejected_on_post_to_allowlisted_path(monkeypatch):
    client = _client(monkeypatch)
    r = client.post("/api/trading/holdings", headers={"X-API-Key": ASSISTANT_KEY})
    assert r.status_code == 401


def test_trading_key_still_works_everywhere(monkeypatch):
    client = _client(monkeypatch)
    assert (
        client.get("/api/trading/holdings", headers={"X-API-Key": TRADING_KEY}).status_code == 200
    )
    assert client.post("/api/trading/buy", headers={"X-API-Key": TRADING_KEY}).status_code == 200
    assert client.get("/api/trading/orders", headers={"X-API-Key": TRADING_KEY}).status_code == 200


def test_wrong_or_missing_key_rejected(monkeypatch):
    client = _client(monkeypatch)
    assert client.get("/api/trading/holdings", headers={"X-API-Key": "nope"}).status_code == 401
    assert client.get("/api/trading/holdings").status_code == 401


def test_assistant_key_unconfigured_means_closed(monkeypatch):
    client = _client(monkeypatch, assistant_key=None)
    r = client.get("/api/trading/holdings", headers={"X-API-Key": ASSISTANT_KEY})
    assert r.status_code == 401
