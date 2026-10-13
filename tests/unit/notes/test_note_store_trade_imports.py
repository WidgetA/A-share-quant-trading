# === MODULE PURPOSE ===
# Tests for QMT fill journaling (2026-10-13 user decision: the QMT and
# miniQMT routes share one capital account 8889993600, so QMT fills belong in
# trade_notes too). QMT fills are imported from per-fill trade records via
# import_filled_trades_from_list; the order-based import keeps skipping
# qmt-sourced orders so a web-channel fill is never recorded twice.

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from src.notes.note_store import TradeNoteStore


class _FakeDB:
    def __init__(self, row: dict | None = None):
        self._row = row
        self.executed: list[str] = []

    async def fetchrow(self, sql: str):
        return self._row

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)


def _store(row: dict | None = None) -> TradeNoteStore:
    return TradeNoteStore(SimpleNamespace(db=_FakeDB(row)))


def _inserts(st: TradeNoteStore) -> list[str]:
    return [s for s in st._db.executed if s.startswith("INSERT INTO trade_notes")]


def test_orders_import_still_skips_qmt_sourced_orders():
    """QMT fills come from the trades path; the orders path must not double-record."""
    st = _store()
    orders = [
        {
            "source": "qmt",
            "order_id": "qmt-f50f:abc123",
            "code": "002407.SZ",
            "side": "SELL",
            "qty": 14700,
            "avg_traded_price": 28.91,
            "status": "FILLED",
            "submit_time": "2026-10-13T09:25:00+08:00",
        }
    ]
    written, skipped = asyncio.run(st.import_filled_orders_from_list(orders))
    assert (written, skipped) == (0, 0)
    assert _inserts(st) == []


def test_trades_import_records_qmt_buy_and_sell():
    st = _store()
    trades = [
        {
            "symbol": "002407.SZ",
            "side": "BUY",
            "quantity": 14700,
            "price": 28.91,
            "traded_time": "2026-10-12T14:55:00+08:00",
            "trade_id": "T10001",
        },
        {
            "symbol": "002407.SZ",
            "side": "SELL",
            "quantity": 14700,
            "price": 28.92,
            "traded_time": "2026-10-13T09:25:00+08:00",
            "trade_id": "T10002",
        },
    ]
    written, _ = asyncio.run(st.import_filled_trades_from_list(trades))
    assert written == 2
    inserts = _inserts(st)
    assert len(inserts) == 2
    assert "broker_trade_T10001" in inserts[0] and "'buy'" in inserts[0]
    assert "买入 @28.91 x 14700" in inserts[0]
    assert "broker_trade_T10002" in inserts[1] and "'sell'" in inserts[1]
    assert "卖出 @28.92 x 14700" in inserts[1]
    # Fill time must be the traded time, not the import time.
    from datetime import datetime, timedelta, timezone

    expected_ms = int(
        datetime(2026, 10, 12, 14, 55, tzinfo=timezone(timedelta(hours=8))).timestamp() * 1000
    )
    assert str(expected_ms) in inserts[0]


def test_trades_import_is_idempotent_without_trade_id():
    trades = [
        {
            "symbol": "603538",
            "side": "sell",
            "quantity": 3800,
            "price": 28.30,
            "traded_time": "2026-09-23 09:30:00",
        }
    ]
    st1 = _store()
    st2 = _store()
    asyncio.run(st1.import_filled_trades_from_list(trades))
    asyncio.run(st2.import_filled_trades_from_list(trades))
    # Same synthesized dedupe key and same ts → the re-import overwrites the
    # same (code, event_id, ts) primary key instead of duplicating.
    assert _inserts(st1) == _inserts(st2)
    (sql,) = _inserts(st1)
    assert "broker_trade_603538_sell_3800_28.3_" in sql


def test_trades_import_maps_xtquant_numeric_sides():
    st = _store()
    trades = [
        {
            "code": "600460",
            "order_type": 23,
            "volume": 100,
            "price": 10.5,
            "time": 1781629200,  # epoch seconds
        },
        {
            "code": "600460",
            "order_type": 24,
            "volume": 100,
            "price": 10.6,
            "time": 1781629260,
        },
    ]
    written, _ = asyncio.run(st.import_filled_trades_from_list(trades))
    assert written == 2
    inserts = _inserts(st)
    assert "'buy'" in inserts[0] and "1781629200000" in inserts[0]
    assert "'sell'" in inserts[1]


def test_trades_import_records_repo_code_as_repo_type():
    st = _store()
    trades = [
        {
            "symbol": "204001.SH",
            "side": "SELL",
            "quantity": 10020,
            "price": 1.46,
            "traded_time": "2026-10-13T14:00:00+08:00",
            "trade_id": "T20001",
        }
    ]
    written, _ = asyncio.run(st.import_filled_trades_from_list(trades))
    assert written == 1
    (sql,) = _inserts(st)
    assert "'逆回购'" in sql
    assert "'sell'" not in sql  # repo rows carry no buy/sell side


def test_trades_import_skips_incomplete_rows_without_fabricating():
    st = _store()
    trades = [
        {"symbol": "002407.SZ", "side": "SELL", "quantity": 100, "price": 28.9},  # no time
        {
            "symbol": "002407.SZ",
            "side": "SELL",
            "quantity": 100,
            "traded_time": 1781629200,
        },  # no price
        {"symbol": "002407.SZ", "side": "SELL", "price": 28.9, "traded_time": 1781629200},  # no qty
        {"symbol": "", "side": "SELL", "quantity": 100, "price": 28.9, "traded_time": 1781629200},
        {
            "symbol": "002407.SZ",
            "side": "??",
            "quantity": 100,
            "price": 28.9,
            "traded_time": 1781629200,
        },
    ]
    written, _ = asyncio.run(st.import_filled_trades_from_list(trades))
    assert written == 0
    assert _inserts(st) == []


def test_trades_import_code_filter_scopes_rows():
    st = _store()
    trades = [
        {
            "symbol": "002407.SZ",
            "side": "SELL",
            "quantity": 100,
            "price": 28.9,
            "traded_time": 1781629200,
        },
        {
            "symbol": "600460.SH",
            "side": "BUY",
            "quantity": 100,
            "price": 10.5,
            "traded_time": 1781629200,
        },
    ]
    written, _ = asyncio.run(st.import_filled_trades_from_list(trades, code_filter={"002407"}))
    assert written == 1
    (sql,) = _inserts(st)
    assert "002407" in sql and "600460" not in sql
