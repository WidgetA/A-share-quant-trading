# === MODULE PURPOSE ===
# Tests for GreptimeBacktestStorage code_alias methods (old_code → new_code
# mapping for 重组改名换号). upsert must only accept 6-digit old/new codes; the
# map round-trips for the ingestion re-key.

from __future__ import annotations

import asyncio
from datetime import date

from src.data.clients.greptime_storage import GreptimeBacktestStorage, date_to_epoch_ms


class _DB:
    def __init__(self, rows: list[dict]):
        self._rows = rows
        self.executed: list[str] = []

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)

    async def fetch(self, sql: str):
        return self._rows


def test_upsert_code_alias_only_six_digit():
    st = object.__new__(GreptimeBacktestStorage)
    st.db = _DB([])  # type: ignore[attr-defined]
    n = asyncio.run(
        st.upsert_code_alias(
            [
                {"old_code": "300114", "new_code": "302132", "change_date": date(2025, 2, 17)},
                {"old_code": "bad", "new_code": "302132"},  # invalid old
                {"old_code": "300379", "new_code": "12"},  # invalid new
            ]
        )
    )
    assert n == 1  # only the 300114→302132 row
    sql = st.db.executed[0]  # type: ignore[attr-defined]
    assert "INSERT INTO code_alias" in sql
    assert str(date_to_epoch_ms(date(2025, 2, 17))) in sql  # change_date written


def test_get_code_alias_map_round_trip():
    st = object.__new__(GreptimeBacktestStorage)
    st.db = _DB(  # type: ignore[attr-defined]
        [
            {"old_code": "300114", "new_code": "302132"},
            {"old_code": "830964", "new_code": "920964"},
            {"old_code": "999999", "new_code": None},  # ignored
        ]
    )
    m = asyncio.run(st.get_code_alias_map())
    assert m == {"300114": "302132", "830964": "920964"}
