# === MODULE PURPOSE ===
# Tests for GreptimeBacktestStorage.upsert_listing_info — the roster writer.
# It must write multi-row VALUES in batches of ≤200 (GreptimeDB silently drops
# bigger batches), not one INSERT per row: load_listing rebuilds the roster as
# truncate → re-insert, and the shorter that window the smaller the chance a
# crash/timeout leaves a truncated roster (the all-orphan hazard).

from __future__ import annotations

import asyncio

from src.data.clients.greptime_storage import GreptimeBacktestStorage


class _RecordingDB:
    def __init__(self) -> None:
        self.executed: list[str] = []

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)


def _storage() -> GreptimeBacktestStorage:
    st = object.__new__(GreptimeBacktestStorage)
    st.db = _RecordingDB()  # type: ignore[attr-defined]
    return st


def _entry(i: int) -> dict:
    return {
        "code": f"{600000 + i:06d}",
        "name": f"股票{i}",
        "list_date": "2020-01-02",
        "delist_date": None,
        "verified": True,
        "source": "tushare_stock_basic",
    }


def test_batches_at_most_200_rows_per_insert():
    st = _storage()
    written = asyncio.run(st.upsert_listing_info([_entry(i) for i in range(450)]))
    assert written == 450
    executed = st.db.executed  # type: ignore[attr-defined]
    assert len(executed) == 3  # 200 + 200 + 50, NOT 450 single-row INSERTs
    row_counts = [sql.count("('6") for sql in executed]
    assert row_counts == [200, 200, 50]
    assert all(sql.startswith("INSERT INTO stock_listing_info") for sql in executed)


def test_skips_invalid_codes_and_escapes_quotes():
    st = _storage()
    entries = [
        {"code": "abc", "name": "bad"},  # invalid → skipped
        {"code": None, "name": "bad"},  # invalid → skipped
        {"code": "600000", "name": "O'Neil 实业", "list_date": "1999-11-10", "verified": True},
    ]
    written = asyncio.run(st.upsert_listing_info(entries))
    assert written == 1
    executed = st.db.executed  # type: ignore[attr-defined]
    assert len(executed) == 1
    assert "O''Neil" in executed[0]  # single quote escaped


def test_empty_entries_write_nothing():
    st = _storage()
    assert asyncio.run(st.upsert_listing_info([])) == 0
    assert st.db.executed == []  # type: ignore[attr-defined]
