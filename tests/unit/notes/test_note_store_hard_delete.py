# === MODULE PURPOSE ===
# Tests for TradeNoteStore.delete_event / delete_card hard-delete semantics
# (0.18.10): user-facing delete issues a physical DELETE FROM keyed by the
# primary key only (code + event_id/card_id, no ts filter) so tombstone/shadow
# rows from earlier ts-moving edits are purged too. Wrong records (e.g.
# duplicated QMT fills) must actually leave the table.
#
# The fake db records SQL strings; store SQL runs against a real GreptimeDB
# in deployment (same stubbing approach as test_notes_backfill_routes.py).

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from src.notes.note_store import TradeNoteStore


class _FakeDB:
    """Returns a canned row for the existence-check SELECT, records all SQL."""

    def __init__(self, row: dict | None):
        self._row = row
        self.executed: list[str] = []

    async def fetchrow(self, sql: str):
        return self._row

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)


_EVENT_ROW = {
    "ts": 1749024300000,  # 2026-06-04 06:05 UTC = 14:05 Beijing
    "code": "002407",
    "event_id": "broker_1042",
    "event_type": "卖出",
    "event_source": "broker",
    "title": "卖出 @33.04 x 800",
    "price": 33.04,
    "qty": 800,
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

_CARD_ROW = {
    "ts": 1749024300000,
    "code": "002407",
    "card_id": "cafe01",
    "content": "盘中观察",
    "deleted": False,
}


def _store(row: dict | None) -> TradeNoteStore:
    return TradeNoteStore(SimpleNamespace(db=_FakeDB(row)))


def test_delete_event_issues_physical_delete_by_pk_only():
    st = _store(_EVENT_ROW)
    ok = asyncio.run(st.delete_event("002407", "broker_1042"))
    assert ok is True
    deletes = [s for s in st._db.executed if s.startswith("DELETE FROM trade_notes")]
    assert len(deletes) == 1
    assert "code = '002407'" in deletes[0]
    assert "event_id = 'broker_1042'" in deletes[0]
    # PK-only: no ts filter, so shadow/tombstone rows at other ts go too.
    assert "AND ts" not in deletes[0]
    # Hard delete must not write a tombstone INSERT like the old soft delete.
    assert not any(s.startswith("INSERT") for s in st._db.executed)


def test_delete_event_missing_row_returns_false_and_deletes_nothing():
    st = _store(None)
    ok = asyncio.run(st.delete_event("002407", "broker_9999"))
    assert ok is False
    assert st._db.executed == []


def test_delete_event_escapes_quotes_in_ids():
    row = dict(_EVENT_ROW, event_id="a'b")
    st = _store(row)
    ok = asyncio.run(st.delete_event("002407", "a'b"))
    assert ok is True
    (delete_sql,) = [s for s in st._db.executed if s.startswith("DELETE")]
    assert "event_id = 'a''b'" in delete_sql


def test_delete_card_issues_physical_delete_by_pk_only():
    st = _store(_CARD_ROW)
    ok = asyncio.run(st.delete_card("002407", "cafe01"))
    assert ok is True
    deletes = [s for s in st._db.executed if s.startswith("DELETE FROM note_cards")]
    assert len(deletes) == 1
    assert "code = '002407'" in deletes[0]
    assert "card_id = 'cafe01'" in deletes[0]
    assert "AND ts" not in deletes[0]
    assert not any(s.startswith("INSERT") for s in st._db.executed)


def test_delete_card_missing_row_returns_false_and_deletes_nothing():
    st = _store(None)
    ok = asyncio.run(st.delete_card("002407", "nope"))
    assert ok is False
    assert st._db.executed == []
