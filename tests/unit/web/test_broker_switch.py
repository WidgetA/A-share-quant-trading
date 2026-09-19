from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.web.routes import create_settings_router


def test_web_exposes_the_selected_trading_channel(monkeypatch, tmp_path):
    monkeypatch.setattr("src.common.config.PROJECT_ROOT", tmp_path)
    monkeypatch.setattr("src.common.config.get_trading_api_key", lambda: "web-test-key")
    app = FastAPI()
    app.include_router(create_settings_router())
    with TestClient(app) as client:
        response = client.get(
            "/api/settings/trading-channel", headers={"X-API-Key": "web-test-key"}
        )
    assert response.status_code == 200
    assert response.json()["backend"] == "miniqmt"
