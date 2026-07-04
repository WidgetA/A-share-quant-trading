# Tests for the 补作业 (backfill) additions to notes_routes:
#   GET /trade-notes/backfill        — HTML page renders
#   GET /api/notes/events-range      — JSON events in a Beijing date range
#   GET /api/notes/stock-name/{code} — code → name lookup
#
# TradeNoteStore / LocalConceptMapper are stubbed — these tests cover routing,
# validation, and response shaping only (store SQL is exercised against a real
# GreptimeDB elsewhere).

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from fastapi.testclient import TestClient

from src.notes.note_store import NoteEvent, TradeNoteStore
from src.web.notes_routes import create_notes_router


class _StubMapper:
    def get_stock_name(self, code):
        return {"002008": "大族激光"}.get(code)


def _sample_event() -> NoteEvent:
    return NoteEvent(
        # 2026-06-03 09:41 Beijing == 01:41 UTC
        ts=datetime(2026, 6, 3, 1, 41, 0, tzinfo=timezone.utc),
        code="002008",
        event_id="abc123",
        event_type="买入",
        source="user",
        title="买入 @18.30 x 500",
        price=18.30,
        qty=500,
        side="buy",
        content="补录",
        content_external="",
        author="user",
        deleted=False,
        commission=5.0,
        transfer_fee=0.09,
        stamp_tax=None,
        dividend=None,
        realized_pnl=None,
    )


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr("src.data.sources.local_concept_mapper.LocalConceptMapper", _StubMapper)

    captured = {}

    async def fake_list_events_in_range(self, start=None, end=None):
        captured["range"] = (start, end)
        return [_sample_event()]

    monkeypatch.setattr(TradeNoteStore, "list_events_in_range", fake_list_events_in_range)
    # TradeNoteStore.__init__ only reads storage.db — a bare namespace suffices.
    app = FastAPI()
    app.include_router(create_notes_router())
    app.state.storage = SimpleNamespace(db=object())
    app.state.templates = Jinja2Templates(directory="src/web/templates")
    c = TestClient(app)
    c.captured = captured
    return c


def test_backfill_page_renders(client):
    resp = client.get("/trade-notes/backfill")
    assert resp.status_code == 200
    assert "补作业" in resp.text
    assert "tnbBody" in resp.text
    # 内嵌 JS 的页面不许被浏览器启发式缓存——部署后拿旧脚本会看不到新行为。
    assert resp.headers["cache-control"] == "no-cache"


def test_trade_notes_page_no_cache(client):
    resp = client.get("/trade-notes")
    assert resp.status_code == 200
    assert resp.headers["cache-control"] == "no-cache"


def test_events_range_returns_beijing_iso_and_name(client):
    resp = client.get("/api/notes/events-range?start=2026-06-01&end=2026-06-30")
    assert resp.status_code == 200
    data = resp.json()
    assert client.captured["range"] == ("2026-06-01", "2026-06-30")
    assert data["count"] == 1
    ev = data["events"][0]
    assert ev["ts"] == "2026-06-03T09:41:00+08:00"  # Beijing local
    assert ev["code"] == "002008"
    assert ev["name"] == "大族激光"
    assert ev["event_id"] == "abc123"
    assert ev["commission"] == 5.0
    assert ev["content"] == "补录"  # internal content included (export omits it)


def test_events_range_without_params_returns_all(client):
    resp = client.get("/api/notes/events-range")
    assert resp.status_code == 200
    assert client.captured["range"] == (None, None)
    assert resp.json()["count"] == 1


@pytest.mark.parametrize(
    "qs",
    [
        "start=2026-6-1&end=2026-06-30",  # bad format
        "start=2026-06-30&end=2026-06-01",  # start > end
        "start=2026-06-01",  # only one bound
        "end=2026-06-30",  # only one bound
    ],
)
def test_events_range_rejects_bad_dates(client, qs):
    resp = client.get(f"/api/notes/events-range?{qs}")
    assert resp.status_code == 400


def test_stock_name_lookup(client):
    resp = client.get("/api/notes/stock-name/002008")
    assert resp.status_code == 200
    assert resp.json() == {"code": "002008", "name": "大族激光"}


def test_stock_name_unknown_code_returns_null_name(client):
    resp = client.get("/api/notes/stock-name/999999")
    assert resp.status_code == 200
    assert resp.json()["name"] is None


def test_stock_name_rejects_non_six_digit(client):
    resp = client.get("/api/notes/stock-name/2008")
    assert resp.status_code == 400
