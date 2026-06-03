# === MODULE PURPOSE ===
# Tests for building the authoritative listing index from Tushare stock_basic
# (scripts/load_listing_from_tushare.py). Pure transform + the load orchestration
# with a fake client/storage — no network/DB.

from __future__ import annotations

import pytest

from scripts.load_listing_from_tushare import build_entries, load_listing


def test_build_entries_marks_verified_and_keeps_dates():
    listed = [
        {"code": "000001", "name": "平安银行", "list_date": "1991-04-03", "delist_date": None},
        {"code": "920964", "name": "某北交所", "list_date": "2020-07-27", "delist_date": None},
    ]
    delisted = [
        {
            "code": "000003",
            "name": "PT金田A",
            "list_date": "1991-07-03",
            "delist_date": "2002-06-14",
        },
    ]
    entries = build_entries(listed, delisted)
    assert len(entries) == 3
    by_code = {e["code"]: e for e in entries}
    # 北交所 920xxx is included, with its real list date
    assert by_code["920964"]["list_date"] == "2020-07-27"
    # delisted keeps both dates
    assert by_code["000003"]["delist_date"] == "2002-06-14"
    # everything from stock_basic is authoritative → verified=True
    assert all(e["verified"] is True for e in entries)
    assert all(e["source"] == "tushare_stock_basic" for e in entries)


def test_build_entries_skips_bad_codes():
    entries = build_entries([{"code": "abc", "list_date": None}, {"code": "600000"}], [])
    assert [e["code"] for e in entries] == ["600000"]


class _FakeClient:
    async def fetch_stock_basic_full(self, list_status):
        if list_status == "L":
            return [
                {"code": "600000", "name": "浦发", "list_date": "1999-11-10", "delist_date": None}
            ]
        return [
            {"code": "000003", "name": "PT", "list_date": "1991-07-03", "delist_date": "2002-06-14"}
        ]


class _FakeStorage:
    def __init__(self, existing: dict | None = None):
        self.written: list[dict] = []
        self.truncated = False
        self.existing = existing or {}

    async def get_listing_info_all(self):
        return self.existing

    async def truncate_listing_info(self):
        self.truncated = True
        self.written = []

    async def upsert_listing_info(self, entries):
        self.written.extend(entries)
        return len(entries)


@pytest.mark.asyncio
async def test_load_listing_writes_listed_and_delisted():
    storage = _FakeStorage()
    result = await load_listing(storage, _FakeClient())
    assert result == {
        "listed": 1,
        "delisted": 1,
        "total_entries": 2,
        "written": 2,
        "preserved_kimi": 0,
    }
    codes = {e["code"] for e in storage.written}
    assert codes == {"600000", "000003"}
    # full rebuild must clear stale rows first (one clean row per code)
    assert storage.truncated is True


@pytest.mark.asyncio
async def test_load_listing_preserves_kimi_rows_drops_migrated_tushare():
    """kimi's path-B rows survive the reload; an old Tushare row for a code Tushare
    no longer lists (migrated 北交所 老码) is dropped; Tushare stays authoritative."""
    from datetime import date

    existing = {
        # kimi 查不到 placeholder for a dead code → must survive (no re-burn nightly)
        "830964": {
            "name": None,
            "list_date": None,
            "delist_date": None,
            "verified": False,
            "source": "kimi-not-found",
        },
        # kimi-confirmed code Tushare doesn't return here → must survive
        "920964": {
            "name": "国义招标",
            "list_date": date(2020, 7, 27),
            "delist_date": None,
            "verified": True,
            "source": "kimi",
        },
        # OLD tushare row for a migrated code, gone from fresh Tushare → must be dropped
        "430198": {
            "name": "旧码",
            "list_date": date(2015, 1, 1),
            "delist_date": None,
            "verified": True,
            "source": "tushare_stock_basic",
        },
        # old tushare row still in fresh Tushare → replaced by reload, not preserved
        "600000": {
            "name": "浦发",
            "list_date": date(1999, 11, 10),
            "delist_date": None,
            "verified": True,
            "source": "tushare_stock_basic",
        },
    }
    storage = _FakeStorage(existing=existing)
    result = await load_listing(storage, _FakeClient())
    assert result["preserved_kimi"] == 2  # 830964 + 920964
    codes = {e["code"] for e in storage.written}
    # fresh Tushare (600000, 000003) + preserved kimi (830964, 920964); 430198 dropped
    assert codes == {"600000", "000003", "830964", "920964"}
    assert "430198" not in codes
