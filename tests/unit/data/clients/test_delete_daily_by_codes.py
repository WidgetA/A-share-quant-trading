# === MODULE PURPOSE ===
# Tests for GreptimeBacktestStorage.delete_daily_by_codes — the purge used to
# remove dead-alias 北交所 老代码 (migrated to 920x). It must only act on 6-digit
# numeric codes (keeps arbitrary input out of the SQL) and issue one DELETE per
# code that actually has rows.

from __future__ import annotations

import asyncio
from datetime import date

from src.data.clients.greptime_storage import GreptimeBacktestStorage, date_to_epoch_ms


class _OrphanDB:
    """Fake db: returns the given orphan (code, ts_ms) rows for the orphan SELECT,
    records DELETEs."""

    def __init__(self, orphan: list[tuple[str, int]]):
        self._orphan = orphan
        self.executed: list[str] = []

    async def fetch(self, sql: str):
        if "daily_state = 'orphan'" in sql:
            return [{"stock_code": c, "ts": ts} for c, ts in self._orphan]
        return []

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)


def _storage_orphan(orphan: list[tuple[str, int]]) -> GreptimeBacktestStorage:
    st = object.__new__(GreptimeBacktestStorage)
    st.db = _OrphanDB(orphan)  # type: ignore[attr-defined]
    return st


def test_purge_orphan_dry_run_does_not_delete():
    ts = date_to_epoch_ms(date(2026, 4, 1))
    st = _storage_orphan([("002231", ts), ("430198", ts)])
    res = asyncio.run(st.purge_orphan_daily_rows(execute=False))
    assert res["executed"] is False
    assert res["codes"] == 2
    assert res["rows"] == 2
    assert st.db.executed == []  # type: ignore[attr-defined]


def test_purge_orphan_execute_deletes_grouped_by_code():
    ts1 = date_to_epoch_ms(date(2026, 4, 1))
    ts2 = date_to_epoch_ms(date(2026, 4, 2))
    st = _storage_orphan([("002231", ts1), ("002231", ts2), ("430198", ts1)])
    res = asyncio.run(st.purge_orphan_daily_rows(execute=True))
    assert res["executed"] is True
    assert res["deleted"] == 3
    db = st.db  # type: ignore[attr-defined]
    assert len(db.executed) == 2  # one DELETE per code
    assert all("DELETE FROM backtest_daily" in s for s in db.executed)


class _FakeDB:
    def __init__(self, counts: dict[str, int]):
        self._counts = counts
        self.executed: list[str] = []

    async def fetch(self, sql: str):
        # extract the code from "... stock_code = '<code>'"
        code = sql.split("stock_code = '")[1].split("'")[0]
        return [{"n": self._counts.get(code, 0)}]

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)


def _storage_with(counts: dict[str, int]) -> GreptimeBacktestStorage:
    st = object.__new__(GreptimeBacktestStorage)
    st.db = _FakeDB(counts)  # type: ignore[attr-defined]
    return st


def test_only_six_digit_codes_acted_on():
    st = _storage_with({"430198": 5, "830964": 7})
    res = asyncio.run(st.delete_daily_by_codes(["430198", "abc", "12345", "9201980", "830964", ""]))
    # only 430198 + 830964 are valid 6-digit
    assert res["codes"] == 2
    assert res["deleted"] == 12  # 5 + 7
    db = st.db  # type: ignore[attr-defined]
    assert len(db.executed) == 2
    assert all("DELETE FROM backtest_daily" in s for s in db.executed)


def test_skips_delete_when_code_has_no_rows():
    st = _storage_with({"430198": 0})
    res = asyncio.run(st.delete_daily_by_codes(["430198"]))
    assert res["codes"] == 1
    assert res["deleted"] == 0
    # no rows → no DELETE issued (only the COUNT fetch happened)
    assert st.db.executed == []  # type: ignore[attr-defined]


def test_empty_list_is_noop():
    st = _storage_with({})
    res = asyncio.run(st.delete_daily_by_codes([]))
    assert res == {"codes": 0, "deleted": 0}
