# === MODULE PURPOSE ===
# Tests for 逆回购 (reverse repo) event handling (0.19.0). QMT delivers repo
# fills as ordinary sell trades; the note-writing layer must recognize repo
# codes (SH 204xxx / SZ 1318xx) and record event_type='逆回购' with side NULL —
# repo is not a buy/sell, only its income (realized_pnl) gets registered.

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from src.notes.note_store import TradeNoteStore, is_reverse_repo_code


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


def test_is_reverse_repo_code():
    assert is_reverse_repo_code("204001")  # 沪 GC001
    assert is_reverse_repo_code("204007")
    assert is_reverse_repo_code("131810")  # 深 R-001
    assert is_reverse_repo_code("204001.SH")
    assert not is_reverse_repo_code("002407")
    assert not is_reverse_repo_code("600460")
    assert not is_reverse_repo_code("131")  # too short
    assert not is_reverse_repo_code("120401")  # 可转债段，非回购


def test_broker_upsert_repo_code_records_repo_type_without_side():
    st = _store(row=None)  # no existing row → fresh INSERT
    asyncio.run(
        st.upsert_broker_event_by_order_id(
            order_id=42, code="204001", side="sell", qty=10020, price=1.46, ts_ms=1782956409000
        )
    )
    (insert_sql,) = [s for s in st._db.executed if s.startswith("INSERT INTO trade_notes")]
    assert "'逆回购'" in insert_sql
    assert "逆回购 @1.46 x 10020" in insert_sql
    assert "'sell'" not in insert_sql  # side must be NULL, not the broker's sell


def test_broker_upsert_normal_code_still_records_sell():
    st = _store(row=None)
    asyncio.run(
        st.upsert_broker_event_by_order_id(
            order_id=43, code="002407", side="sell", qty=800, price=33.04, ts_ms=1749024300000
        )
    )
    (insert_sql,) = [s for s in st._db.executed if s.startswith("INSERT INTO trade_notes")]
    assert "'卖出'" in insert_sql
    assert "'sell'" in insert_sql


_SELL_ROW = {
    "ts": 1780368533000,
    "code": "204001",
    "event_id": "broker_manual_204001_sell_3900_1780368533000",
    "event_type": "卖出",
    "event_source": "broker",
    "title": "卖出 @100.00 x 3900",
    "price": 100.0,
    "qty": 3900,
    "side": "sell",
    "content": "",
    "content_external": "",
    "author": "system",
    "deleted": False,
    "commission": None,
    "transfer_fee": None,
    "stamp_tax": None,
    "dividend": None,
    "realized_pnl": None,
}


def test_update_event_to_repo_type_clears_side():
    st = _store(row=_SELL_ROW)
    ok = asyncio.run(
        st.update_event(
            "204001",
            _SELL_ROW["event_id"],
            event_type="逆回购",
            title="逆回购 @100.00 x 3900",
        )
    )
    assert ok is True
    (insert_sql,) = [s for s in st._db.executed if s.startswith("INSERT INTO trade_notes")]
    assert "'逆回购'" in insert_sql
    assert "'sell'" not in insert_sql  # side cleared to NULL on conversion


def test_create_manual_repo_event_no_side_auto_title():
    st = _store(row=None)
    asyncio.run(
        st.create_event(
            code="204001",
            event_type="逆回购",
            title="",
            content="",
            price=1.26,
            qty=5510,
            realized_pnl=3.9,
            ts_ms=1780624772000,
        )
    )
    (insert_sql,) = [s for s in st._db.executed if s.startswith("INSERT INTO trade_notes")]
    assert "'逆回购'" in insert_sql
    assert "逆回购 @1.26 x 5510" in insert_sql
    assert "'sell'" not in insert_sql
    assert "3.9" in insert_sql  # 收益进 realized_pnl
