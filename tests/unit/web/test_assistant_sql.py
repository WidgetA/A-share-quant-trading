# === MODULE PURPOSE ===
# AST-001 Phase 2 safety gate: the assistant's SQL proxy must be physically
# read-only — SELECT family only, single statement, forced LIMIT, bounded
# size. These tests pin the validator (pure fn) and the endpoint behaviour
# (auth via readonly key, honest 503/400).

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.web.assistant_sql import (
    DEFAULT_LIMIT,
    validate_readonly_sql,
)

# ── validator ────────────────────────────────────────────────────────────


def test_select_gets_default_limit():
    assert validate_readonly_sql("SELECT * FROM backtest_daily") == (
        f"SELECT * FROM backtest_daily LIMIT {DEFAULT_LIMIT}"
    )


def test_existing_limit_kept_and_trailing_semicolon_stripped():
    assert validate_readonly_sql("select code from stock_list limit 50;") == (
        "select code from stock_list limit 50"
    )


def test_show_and_describe_pass_without_limit():
    assert validate_readonly_sql("SHOW TABLES") == "SHOW TABLES"
    assert validate_readonly_sql("DESC backtest_daily") == "DESC backtest_daily"


def test_cte_allowed():
    out = validate_readonly_sql("WITH t AS (SELECT 1 AS x) SELECT x FROM t")
    assert out.endswith(f"LIMIT {DEFAULT_LIMIT}")


def test_writes_rejected():
    for bad in (
        "INSERT INTO t VALUES (1)",
        "DROP TABLE backtest_daily",
        "UPDATE t SET a=1",
        "DELETE FROM t",
        "TRUNCATE t",
        "CREATE TABLE x (a INT)",
        "SELECT 1; DROP TABLE t",  # multi-statement smuggling
    ):
        with pytest.raises(ValueError):
            validate_readonly_sql(bad)


def test_huge_limit_rejected_and_empty_rejected():
    with pytest.raises(ValueError):
        validate_readonly_sql("SELECT 1 LIMIT 999999")
    with pytest.raises(ValueError):
        validate_readonly_sql("   ")


def test_column_names_containing_keywords_are_fine():
    # "updated_at" / "created_at" must NOT trip the whole-word write filter
    out = validate_readonly_sql("SELECT updated_at, created_at FROM t WHERE updated_at > 0")
    assert out.endswith(f"LIMIT {DEFAULT_LIMIT}")


# ── endpoint ─────────────────────────────────────────────────────────────

ASSISTANT_KEY = "assistant-ro"


class _FakeDB:
    def __init__(self):
        self.last_sql: str | None = None

    async def fetch(self, sql: str):
        self.last_sql = sql
        return [{"code": "600519", "close": 1700.5}]


def _client(monkeypatch, with_storage: bool = True) -> tuple[TestClient, _FakeDB]:
    monkeypatch.setattr("src.common.config.get_trading_api_key", lambda: "trading-secret")
    monkeypatch.setattr("src.common.config.get_assistant_readonly_key", lambda: ASSISTANT_KEY)

    from src.web.routes import create_trading_router

    app = FastAPI()
    app.include_router(create_trading_router())
    fake = _FakeDB()
    app.state.storage = SimpleNamespace(db=fake) if with_storage else None
    return TestClient(app), fake


def test_endpoint_runs_readonly_sql_with_assistant_key(monkeypatch):
    client, fake = _client(monkeypatch)
    r = client.get(
        "/api/trading/assistant-sql",
        params={"sql": "SELECT code, close FROM backtest_daily"},
        headers={"X-API-Key": ASSISTANT_KEY},
    )
    assert r.status_code == 200
    assert r.json()["row_count"] == 1
    assert fake.last_sql.endswith(f"LIMIT {DEFAULT_LIMIT}")  # 强制 LIMIT 真的落到执行层


def test_endpoint_rejects_write_sql(monkeypatch):
    client, fake = _client(monkeypatch)
    r = client.get(
        "/api/trading/assistant-sql",
        params={"sql": "DROP TABLE backtest_daily"},
        headers={"X-API-Key": ASSISTANT_KEY},
    )
    assert r.status_code == 400
    assert fake.last_sql is None  # 连数据库都没碰


def test_endpoint_503_when_storage_down(monkeypatch):
    client, _ = _client(monkeypatch, with_storage=False)
    r = client.get(
        "/api/trading/assistant-sql",
        params={"sql": "SELECT 1"},
        headers={"X-API-Key": ASSISTANT_KEY},
    )
    assert r.status_code == 503


def test_endpoint_rejects_wrong_key(monkeypatch):
    client, _ = _client(monkeypatch)
    r = client.get(
        "/api/trading/assistant-sql",
        params={"sql": "SELECT 1"},
        headers={"X-API-Key": "nope"},
    )
    assert r.status_code == 401
