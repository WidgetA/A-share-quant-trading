# === MODULE PURPOSE ===
# Tests for GreptimeBacktestStorage.upsert_trading_calendar — the rebuild writer.
# It must DELETE the day's existing rows before inserting, so a code that left
# the roster (e.g. a dead 北交所 老代码 replaced by its 920 successor) doesn't
# linger as a stale row. A plain upsert only overwrites codes it re-emits.

from __future__ import annotations

import asyncio
from datetime import date

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


def test_deletes_day_before_inserting():
    st = _storage()
    rows = [
        {
            "code": "600000",
            "listed": True,
            "trade_status": "trading",
            "daily_state": "ok",
            "reason": None,
        },
    ]
    asyncio.run(st.upsert_trading_calendar(date(2024, 1, 2), rows))
    executed = st.db.executed  # type: ignore[attr-defined]
    # the FIRST statement must clear the day
    assert executed[0].startswith("DELETE FROM trading_calendar WHERE ts =")
    assert any("INSERT INTO trading_calendar" in s for s in executed)


def test_empty_rows_still_clears_the_day():
    # A day that now reconciles to nothing must still be cleared (no stale rows).
    st = _storage()
    asyncio.run(st.upsert_trading_calendar(date(2024, 1, 2), []))
    executed = st.db.executed  # type: ignore[attr-defined]
    assert len(executed) == 1
    assert executed[0].startswith("DELETE FROM trading_calendar WHERE ts =")
