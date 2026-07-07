# === MODULE PURPOSE ===
# AST-001 config layer: assistant settings live in data/assistant_config.json
# (written from the Settings page, volume-mounted in prod) with env vars as a
# bootstrap fallback — per-field priority file > env, partial updates keep
# untouched fields. Plus the Settings endpoints: status shape, save semantics,
# hot-start attempt on save.

from __future__ import annotations

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.common.config as config

_ENV_VARS = (
    "FEISHU_ASSISTANT_APP_ID",
    "FEISHU_ASSISTANT_APP_SECRET",
    "FEISHU_ASSISTANT_ALLOWED_USERS",
    "ASSISTANT_READONLY_KEY",
)


@pytest.fixture
def isolated_config(monkeypatch: pytest.MonkeyPatch, tmp_path):
    monkeypatch.setattr(config, "ASSISTANT_CONFIG_FILE", tmp_path / "assistant_config.json")
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    return tmp_path / "assistant_config.json"


# ── config priority ──────────────────────────────────────────────────────


def test_env_is_fallback_when_no_file(isolated_config, monkeypatch):
    monkeypatch.setenv("FEISHU_ASSISTANT_APP_ID", "cli_env")
    monkeypatch.setenv("ASSISTANT_READONLY_KEY", "env-key")
    assert config.get_assistant_feishu_config()["app_id"] == "cli_env"
    assert config.get_assistant_readonly_key() == "env-key"
    assert config.get_assistant_allowed_users() == frozenset()


def test_file_beats_env_per_field(isolated_config, monkeypatch):
    monkeypatch.setenv("FEISHU_ASSISTANT_APP_ID", "cli_env")
    monkeypatch.setenv("ASSISTANT_READONLY_KEY", "env-key")
    config.set_assistant_config(app_id="cli_file", allowed_users="ou_a, ou_b,")
    assert config.get_assistant_feishu_config()["app_id"] == "cli_file"
    assert config.get_assistant_allowed_users() == frozenset({"ou_a", "ou_b"})
    # field NOT in the file still falls back to env
    assert config.get_assistant_readonly_key() == "env-key"


def test_partial_update_keeps_other_fields(isolated_config):
    config.set_assistant_config(app_id="cli_x", app_secret="s1")
    config.set_assistant_config(readonly_key="k1")
    assert config.get_assistant_feishu_config() == {"app_id": "cli_x", "app_secret": "s1"}
    assert config.get_assistant_readonly_key() == "k1"


def test_unknown_field_rejected_loudly(isolated_config):
    with pytest.raises(ValueError):
        config.set_assistant_config(trading_key="nope")


def test_broken_file_falls_back_to_env(isolated_config, monkeypatch):
    isolated_config.write_text("{not json", encoding="utf-8")
    monkeypatch.setenv("FEISHU_ASSISTANT_APP_ID", "cli_env")
    assert config.get_assistant_feishu_config()["app_id"] == "cli_env"


# ── Settings endpoints ───────────────────────────────────────────────────


def _settings_client() -> TestClient:
    from src.web.routes import create_settings_router

    app = FastAPI()
    app.include_router(create_settings_router())
    app.state.assistant_dispatcher = None
    return TestClient(app)


def test_status_endpoint_shape(isolated_config):
    client = _settings_client()
    data = client.get("/api/settings/assistant").json()
    assert data["running"] is False
    assert data["app_id"] == ""
    assert data["app_secret_configured"] is False
    assert data["readonly_key_masked"] == ""
    assert any("凭证" in item for item in data["missing"])
    # 白名单已屏蔽(用户拍板),绝不能出现在启动必要条件里
    assert not any("白名单" in item for item in data["missing"])


def test_save_persists_and_reports_missing(isolated_config, monkeypatch):
    # Keep the hot-start attempt from ever spawning a real ws thread.
    monkeypatch.setattr(
        "src.assistant.dispatcher.start_assistant_if_ready",
        lambda app_state: ["使用者白名单(open_id)"],
    )
    client = _settings_client()
    resp = client.post("/api/settings/assistant", json={"app_id": "cli_new"})
    data = resp.json()
    assert resp.status_code == 200 and data["success"] is True
    assert "还缺" in data["message"]
    stored = json.loads(isolated_config.read_text(encoding="utf-8"))
    assert stored == {"app_id": "cli_new"}


def test_save_reports_started_when_complete(isolated_config, monkeypatch):
    monkeypatch.setattr("src.assistant.dispatcher.start_assistant_if_ready", lambda app_state: [])
    client = _settings_client()
    resp = client.post("/api/settings/assistant", json={"readonly_key": "k1"})
    assert "助手已启动" in resp.json()["message"]


def test_save_empty_body_rejected(isolated_config):
    client = _settings_client()
    resp = client.post("/api/settings/assistant", json={})
    assert resp.status_code == 400


def test_masked_key_in_status(isolated_config):
    config.set_assistant_config(readonly_key="abcd1234efgh5678")
    client = _settings_client()
    data = client.get("/api/settings/assistant").json()
    assert data["readonly_key_masked"] == "abcd...5678"
    assert "abcd1234efgh5678" not in json.dumps(data)
