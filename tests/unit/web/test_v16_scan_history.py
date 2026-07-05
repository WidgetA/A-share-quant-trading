# Tests for V16 scan-history persistence + 选股接口 (src/web/v16_scan_history.py):
#   persist/load 落盘回读、build_history_rows_from_scan 字段映射、
#   GET /api/v16/scan-history 200/404/400、POST backfill 鉴权 + 覆盖守卫。

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.web.v16_scan_history as vsh
from src.web.v16_scan_history import (
    build_history_rows_from_scan,
    create_v16_scan_history_router,
    load_scan_history,
    persist_scan_history,
)


@pytest.fixture
def history_dir(tmp_path, monkeypatch):
    d = tmp_path / "v16_scan_history"
    monkeypatch.setattr(vsh, "SCAN_HISTORY_DIR", d)
    return d


@pytest.fixture
def client(history_dir, monkeypatch):
    monkeypatch.setattr("src.common.config.get_iquant_api_key", lambda: "k-test")
    app = FastAPI()
    app.include_router(create_v16_scan_history_router())
    return TestClient(app)


ROWS = [
    {
        "rank": 1,
        "stock_code": "002384",
        "stock_name": "东山精密",
        "board_name": "CPO",
        "score": 0.2145,
        "buy_price": 223.86,
        "final_candidates": 164,
    },
    {
        "rank": 2,
        "stock_code": "600246",
        "stock_name": "万通发展",
        "board_name": "存储芯片",
        "score": 0.2145,
        "buy_price": 16.64,
        "final_candidates": 164,
    },
]


def test_persist_and_load_roundtrip(history_dir):
    persist_scan_history("2026-06-03", ROWS, source="live")
    payload = load_scan_history("2026-06-03")
    assert payload["source"] == "live"
    assert payload["rows"][0]["stock_code"] == "002384"
    assert load_scan_history("2026-06-04") is None


def test_build_rows_maps_scan_result_fields():
    scan_result = SimpleNamespace(
        recommended=[SimpleNamespace(code="002407", name="多氟多", score=0.1906, buy_price=33.35)],
        stock_best_board={"002407": "存储芯片"},
        cci={"002407": -106.4},
        final_candidates=164,
    )
    stock_data = {"002407": SimpleNamespace(open_price=33.01, prev_close=33.10)}
    rows = build_history_rows_from_scan(scan_result, stock_data)
    assert rows == [
        {
            "rank": 1,
            "stock_code": "002407",
            "stock_name": "多氟多",
            "board_name": "存储芯片",
            "score": 0.1906,
            "buy_price": 33.35,
            "open_price": 33.01,
            "prev_close": 33.1,
            "gain_from_open_pct": round((33.35 - 33.01) / 33.01 * 100, 4),
            "cci14": -106.4,
            "final_candidates": 164,
        }
    ]


def test_get_returns_payload_or_404(client):
    persist_scan_history("2026-06-03", ROWS, source="live")
    ok = client.get("/api/v16/scan-history", params={"date": "2026-06-03"})
    assert ok.status_code == 200
    assert len(ok.json()["rows"]) == 2

    missing = client.get("/api/v16/scan-history", params={"date": "2026-06-04"})
    assert missing.status_code == 404

    bad = client.get("/api/v16/scan-history", params={"date": "20260603"})
    assert bad.status_code == 400


def test_backfill_requires_key_and_writes(client):
    noauth = client.post(
        "/api/v16/scan-history/backfill",
        json={"date": "2026-05-28", "rows": ROWS},
    )
    assert noauth.status_code in (401, 403)

    ok = client.post(
        "/api/v16/scan-history/backfill",
        json={"date": "2026-05-28", "rows": ROWS},
        headers={"X-API-Key": "k-test"},
    )
    assert ok.status_code == 200
    assert load_scan_history("2026-05-28")["source"] == "backfill"


def test_backfill_wont_clobber_without_force(client):
    persist_scan_history("2026-06-03", ROWS, source="live")
    resp = client.post(
        "/api/v16/scan-history/backfill",
        json={"date": "2026-06-03", "rows": ROWS},
        headers={"X-API-Key": "k-test"},
    )
    assert resp.status_code == 409

    forced = client.post(
        "/api/v16/scan-history/backfill",
        json={"date": "2026-06-03", "rows": ROWS, "force": True},
        headers={"X-API-Key": "k-test"},
    )
    assert forced.status_code == 200
    assert load_scan_history("2026-06-03")["source"] == "backfill"
