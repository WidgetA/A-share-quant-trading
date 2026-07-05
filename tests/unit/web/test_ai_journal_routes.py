# Tests for NOTE-002 AI 写日志的 HTTP 端点:
#   POST /api/notes/ai-journal/run    — 守卫(kimi 缺失/key 缺失/已在跑/存储不可用)+ 启动
#   GET  /api/notes/ai-journal/status — 进度透出
# 跑批本体 monkeypatch 掉——这里只测路由层。

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.testclient import TestClient

from src.notes import ai_journal
from src.web.notes_routes import create_notes_router


@pytest.fixture
def app_client(monkeypatch):
    app = FastAPI()
    app.include_router(create_notes_router())
    app.state.storage = SimpleNamespace(db=object())
    app.state.templates = Jinja2Templates(directory="src/web/templates")
    return TestClient(app), monkeypatch


def _arm(monkeypatch, kimi=True, key=True, running=False):
    monkeypatch.setattr("src.data.services.kimi_listing_verifier.kimi_available", lambda: kimi)
    if key:
        monkeypatch.setenv("KIMI_API_KEY", "sk-kimi-test")
    else:
        monkeypatch.delenv("KIMI_API_KEY", raising=False)
    monkeypatch.setitem(ai_journal._state, "running", running)

    calls = {}

    async def fake_batch(storage, event_ids=None, time_budget_sec=0, max_events=0):
        calls["args"] = {
            "event_ids": event_ids,
            "time_budget_sec": time_budget_sec,
            "max_events": max_events,
        }
        return {}

    monkeypatch.setattr(ai_journal, "run_ai_journal_batch", fake_batch)
    return calls


def test_run_starts_batch(app_client):
    client, monkeypatch = app_client
    calls = _arm(monkeypatch)
    resp = client.post("/api/notes/ai-journal/run", json={"max_events": 7})
    assert resp.status_code == 200
    assert resp.json() == {"started": True}
    assert calls["args"]["max_events"] == 7


def test_run_conflicts_when_already_running(app_client):
    client, monkeypatch = app_client
    _arm(monkeypatch, running=True)
    resp = client.post("/api/notes/ai-journal/run", json={})
    assert resp.status_code == 409


def test_run_requires_kimi_binary(app_client):
    client, monkeypatch = app_client
    _arm(monkeypatch, kimi=False)
    resp = client.post("/api/notes/ai-journal/run", json={})
    assert resp.status_code == 503
    assert "kimi" in resp.json()["detail"]


def test_run_requires_api_key(app_client):
    client, monkeypatch = app_client
    _arm(monkeypatch, key=False)
    resp = client.post("/api/notes/ai-journal/run", json={})
    assert resp.status_code == 503
    assert "KIMI_API_KEY" in resp.json()["detail"]


def test_status_reports_state(app_client):
    client, monkeypatch = app_client
    monkeypatch.setitem(ai_journal._state, "running", False)
    monkeypatch.setitem(ai_journal._state, "ok", 3)
    resp = client.get("/api/notes/ai-journal/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] == 3
    assert body["running"] is False
