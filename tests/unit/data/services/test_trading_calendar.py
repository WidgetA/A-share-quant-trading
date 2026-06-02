# === MODULE PURPOSE ===
# Tests for the trading-calendar truth-table reconcile (DAT-006). Locks the status
# rules the user specified: 标了停牌却有数据 / 没标停牌却没数据 / 有数据却不在册,
# plus 源头也无 and the clean cases. Pure logic — no DB / no network.

from __future__ import annotations

from datetime import date

import pytest

from src.data.services.trading_calendar import (
    MISSING,
    OK,
    ORPHAN,
    SOURCE_NONE,
    SUSPENDED,
    TRADING,
    UNKNOWN,
    WRONG_SUSPENDED,
    WRONG_TRADED,
    build_calendar,
    reconcile_day,
    roster_for_day,
)


def _by_code(rows):
    return {r["code"]: r for r in rows}


def test_normal_traded_stock_with_real_data_is_ok():
    rows = _by_code(
        reconcile_day(
            roster={"600000"},
            suspended=set(),
            traded={"600000"},
            db_normal={"600000"},
            db_suspended=set(),
        )
    )
    assert rows["600000"]["trade_status"] == TRADING
    assert rows["600000"]["daily_state"] == OK
    assert rows["600000"]["listed"] is True


def test_traded_but_missing_from_db_is_real_gap():
    # 创业板/科创板/北交所 historical hole: traded per Tushare, absent in DB.
    rows = _by_code(
        reconcile_day(
            roster={"300001"},
            suspended=set(),
            traded={"300001"},
            db_normal=set(),
            db_suspended=set(),
        )
    )
    assert rows["300001"]["trade_status"] == TRADING
    assert rows["300001"]["daily_state"] == MISSING


def test_marked_suspended_but_actually_traded_is_wrong_suspended():
    # user rule: 标了停牌却有数据
    rows = _by_code(
        reconcile_day(
            roster={"000001"},
            suspended=set(),
            traded={"000001"},
            db_normal=set(),
            db_suspended={"000001"},
        )
    )
    assert rows["000001"]["daily_state"] == WRONG_SUSPENDED


def test_suspended_with_placeholder_is_ok():
    rows = _by_code(
        reconcile_day(
            roster={"000002"},
            suspended={"000002"},
            traded=set(),
            db_normal=set(),
            db_suspended={"000002"},
        )
    )
    assert rows["000002"]["trade_status"] == SUSPENDED
    assert rows["000002"]["daily_state"] == OK


def test_suspended_without_placeholder_is_missing():
    # user rule: 没标停牌却没数据 (here: suspended at source, no placeholder row)
    rows = _by_code(
        reconcile_day(
            roster={"000003"},
            suspended={"000003"},
            traded=set(),
            db_normal=set(),
            db_suspended=set(),
        )
    )
    assert rows["000003"]["trade_status"] == SUSPENDED
    assert rows["000003"]["daily_state"] == MISSING


def test_suspended_but_db_has_real_data_is_wrong_traded():
    rows = _by_code(
        reconcile_day(
            roster={"000004"},
            suspended={"000004"},
            traded=set(),
            db_normal={"000004"},
            db_suspended=set(),
        )
    )
    assert rows["000004"]["daily_state"] == WRONG_TRADED


def test_listed_but_no_source_data_is_source_none():
    rows = _by_code(
        reconcile_day(
            roster={"900001"},
            suspended=set(),
            traded=set(),
            db_normal=set(),
            db_suspended=set(),
        )
    )
    assert rows["900001"]["trade_status"] == UNKNOWN
    assert rows["900001"]["daily_state"] == SOURCE_NONE


def test_data_present_but_not_in_roster_is_orphan():
    # user rule: 有数据却不在当天在册名单
    rows = _by_code(
        reconcile_day(
            roster=set(),
            suspended=set(),
            traded={"688001"},
            db_normal={"688001"},
            db_suspended=set(),
        )
    )
    assert rows["688001"]["listed"] is False
    assert rows["688001"]["daily_state"] == ORPHAN
    assert rows["688001"]["trade_status"] == TRADING


def test_roster_for_day_respects_list_and_delist_dates():
    info = {
        "600000": {"list_date": date(1999, 11, 10), "delist_date": None},
        "920001": {"list_date": date(2022, 12, 27), "delist_date": None},  # not yet listed early
        "000003": {"list_date": date(1991, 7, 3), "delist_date": date(2002, 6, 14)},  # delisted
    }
    early = roster_for_day(info, date(2020, 1, 2))
    assert early == {"600000"}  # 920001 not listed yet, 000003 already delisted
    later = roster_for_day(info, date(2023, 1, 3))
    assert later == {"600000", "920001"}


class _FakeCalStorage:
    """Minimal storage for build_calendar: 600000 has real data, 300001 missing."""

    def __init__(self):
        self.upserts: list = []

    async def get_listing_info_all(self):
        return {
            "600000": {"list_date": date(1999, 1, 1), "delist_date": None},
            "300001": {"list_date": date(2009, 1, 1), "delist_date": None},
        }

    async def get_daily_split_for_date(self, day):
        return ({"600000"}, set())  # 600000 normal; 300001 absent

    async def upsert_trading_calendar(self, day, rows):
        self.upserts.append((day, rows))
        return len(rows)


@pytest.mark.asyncio
async def test_build_calendar_aggregates_by_state():
    storage = _FakeCalStorage()

    async def fetch_suspended(day):
        return set()

    async def fetch_traded(day):
        return {"600000", "300001"}  # both traded per Tushare

    result = await build_calendar(
        storage,
        trading_days=[date(2024, 1, 2)],
        fetch_suspended=fetch_suspended,
        fetch_traded=fetch_traded,
    )
    assert result["days"] == 1
    assert result["rows"] == 2
    # 600000 traded + has real → ok; 300001 traded + absent → missing (真缺)
    assert result["by_state"][OK] == 1
    assert result["by_state"][MISSING] == 1
    assert result["problem_rows"] == 1
    assert len(storage.upserts) == 1
