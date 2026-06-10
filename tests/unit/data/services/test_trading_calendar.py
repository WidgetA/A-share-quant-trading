# === MODULE PURPOSE ===
# Tests for the trading-calendar truth-table reconcile (DAT-006). Locks the status
# rules the user specified: 标了停牌却有数据 / 没标停牌却没数据 / 有数据却不在册,
# plus 源头也无 and the clean cases. Pure logic — no DB / no network.

from __future__ import annotations

from datetime import date

import pytest

from src.data.services.trading_calendar import (
    MINUTE_MISSING,
    MINUTE_OK,
    MINUTE_SOURCE_SHORT,
    MISSING,
    OK,
    ORPHAN,
    SOURCE_NONE,
    SUSPENDED,
    TRADING,
    UNKNOWN,
    WRONG_SUSPENDED,
    WRONG_TRADED,
    alignment_suspects,
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


# --- vol=0 真实 bar(一字板无成交等):写入端存真实行,reconcile 必须同口径 ---


def test_zero_vol_real_bar_not_suspended_counts_as_traded_ok():
    # Tushare daily has a real-price bar with vol=0, not in suspend_d → the write path
    # stores a REAL row → reconcile must judge it trading/ok, NOT wrong_traded forever.
    rows = _by_code(
        reconcile_day(
            roster={"600001"},
            suspended=set(),
            traded=set(),  # vol>0 set: empty
            db_normal={"600001"},
            db_suspended=set(),
            traded_zero_vol={"600001"},
        )
    )
    assert rows["600001"]["trade_status"] == TRADING
    assert rows["600001"]["daily_state"] == OK


def test_zero_vol_real_bar_not_suspended_missing_from_db_is_missing():
    rows = _by_code(
        reconcile_day(
            roster={"600001"},
            suspended=set(),
            traded=set(),
            db_normal=set(),
            db_suspended=set(),
            traded_zero_vol={"600001"},
        )
    )
    assert rows["600001"]["trade_status"] == TRADING
    assert rows["600001"]["daily_state"] == MISSING


def test_zero_vol_bar_while_suspended_is_judged_suspended():
    # vol=0 bar + listed in suspend_d → the write path writes a PLACEHOLDER → reconcile
    # must expect db_suspended (mirror of _process_daily_date's volume>0-wins rule).
    rows = _by_code(
        reconcile_day(
            roster={"600001"},
            suspended={"600001"},
            traded=set(),
            db_normal=set(),
            db_suspended={"600001"},
            traded_zero_vol={"600001"},
        )
    )
    assert rows["600001"]["trade_status"] == SUSPENDED
    assert rows["600001"]["daily_state"] == OK


def test_zero_vol_traded_stock_has_no_minute_expectation():
    # vol=0 → no minute activity to demand: minute_state stays None even when the
    # minute reconcile is on (otherwise it would re-download these forever).
    rows = _by_code(
        reconcile_day(
            roster={"600001", "600002"},
            suspended=set(),
            traded={"600002"},  # 600002 has volume → minute expected
            db_normal={"600001", "600002"},
            db_suspended=set(),
            minute_counts={"600002": 241},
            traded_zero_vol={"600001"},
        )
    )
    assert rows["600001"]["minute_state"] is None
    assert rows["600002"]["minute_state"] == MINUTE_OK


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


def test_roster_for_day_excludes_codes_without_list_date():
    # kimi "查不到" placeholders carry list_date=None. They must NOT be rostered —
    # otherwise dead/migrated codes (which Tushare daily never returns) linger as
    # source_none forever. No confirmed list_date ⇒ not on the roster.
    info = {
        "600000": {"list_date": date(1999, 11, 10), "delist_date": None},
        "830964": {"list_date": None, "delist_date": None},  # kimi 查不到 placeholder
    }
    assert roster_for_day(info, date(2024, 1, 2)) == {"600000"}


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
        # legacy plain-set shape (no zero-vol info) — must keep working
        return {"600000", "300001"}  # both traded per Tushare

    result = await build_calendar(
        storage,
        trading_days=[date(2024, 1, 2)],
        fetch_suspended=fetch_suspended,
        fetch_traded=fetch_traded,
        min_listing=0,  # tiny fake roster — disable the truncated-roster guard
    )
    assert result["days"] == 1
    assert result["rows"] == 2
    # 600000 traded + has real → ok; 300001 traded + absent → missing (真缺)
    assert result["by_state"][OK] == 1
    assert result["by_state"][MISSING] == 1
    assert result["problem_rows"] == 1
    assert len(storage.upserts) == 1
    assert result["skipped_days"] == []  # normal day, nothing skipped


@pytest.mark.asyncio
async def test_build_calendar_zero_vol_bar_flows_through_tuple_fetch_traded():
    # fetch_traded's tuple shape: (vol>0, real-price vol=0). The zero-vol code with a
    # real DB row must come out trading/ok (not wrong_traded).
    storage = _FakeCalStorage()  # 600000 real in DB; 300001 absent

    async def fetch_suspended(day):
        return set()

    async def fetch_traded(day):
        return {"300001"}, {"600000"}  # 600000 = 一字板 vol=0 real bar

    result = await build_calendar(
        storage,
        trading_days=[date(2024, 1, 2)],
        fetch_suspended=fetch_suspended,
        fetch_traded=fetch_traded,
        min_listing=0,
    )
    assert result["by_state"][OK] == 1  # 600000: zero-vol bar + db real → ok
    assert result["by_state"][MISSING] == 1  # 300001: traded + db absent → missing


@pytest.mark.asyncio
async def test_build_calendar_skips_day_when_traded_empty_but_db_has_real_rows():
    # FAIL-SAFE: an empty fetch_traded on a trade_cal day is a transient Tushare daily
    # failure, NOT a no-trade day → SKIP + surface, never mass-flip good rows to
    # wrong_traded (the 2026-06-03 corruption that motivated this guard).
    big = {str(600000 + i) for i in range(150)}

    class _EmptyTradedStorage:
        def __init__(self):
            self.upserts: list = []

        async def get_listing_info_all(self):
            return {c: {"list_date": date(1999, 1, 1), "delist_date": None} for c in big}

        async def get_daily_split_for_date(self, day):
            return (set(big), set())  # 库里有 150 行真实日线

        async def upsert_trading_calendar(self, day, rows):
            self.upserts.append((day, rows))
            return len(rows)

    storage = _EmptyTradedStorage()

    async def fetch_suspended(day):
        return set()

    async def fetch_traded(day):
        return set()  # Tushare daily 空响应(取数失败)

    result = await build_calendar(
        storage,
        trading_days=[date(2024, 1, 2)],
        fetch_suspended=fetch_suspended,
        fetch_traded=fetch_traded,
        min_listing=0,
    )
    assert result["skipped_days"] == ["2024-01-02"]
    assert result["rows"] == 0  # nothing upserted → existing good rows left intact
    assert storage.upserts == []  # the day was NOT reconciled/overwritten


@pytest.mark.asyncio
async def test_build_calendar_skips_empty_traded_even_on_brand_new_day():
    # THE key fix (2026-06 二次收紧): on a brand-new day (nightly ③, fill not run yet,
    # DB EMPTY) an empty Tushare daily must STILL be skipped. The old ≥100-DB-rows guard
    # let it through → whole roster written source_none ("可接受") → the incremental
    # nightly never revisits the day → silently never filled.
    class _NewDayStorage:
        def __init__(self):
            self.upserts: list = []

        async def get_listing_info_all(self):
            return {"600000": {"list_date": date(1999, 1, 1), "delist_date": None}}

        async def get_daily_split_for_date(self, day):
            return (set(), set())  # 新交易日,补全还没跑,库里 0 行

        async def upsert_trading_calendar(self, day, rows):
            self.upserts.append((day, rows))
            return len(rows)

    storage = _NewDayStorage()

    async def fetch_suspended(day):
        return set()

    async def fetch_traded(day):
        return set()  # 源头空响应

    result = await build_calendar(
        storage,
        trading_days=[date(2024, 1, 2)],
        fetch_suspended=fetch_suspended,
        fetch_traded=fetch_traded,
        min_listing=0,
    )
    assert result["skipped_days"] == ["2024-01-02"]  # protected even with an empty DB
    assert storage.upserts == []  # nothing written → next nightly retries this day


@pytest.mark.asyncio
async def test_build_calendar_empty_roster_day_reconciles_orphans_without_guard():
    # Empty roster (e.g. all codes listed after this day) + empty traded must NOT trip the
    # empty-traded guard — orphan-only reconcile still proceeds.
    class _OrphanOnlyStorage:
        def __init__(self):
            self.upserts: list = []

        async def get_listing_info_all(self):
            return {"600000": {"list_date": date(2025, 1, 1), "delist_date": None}}

        async def get_daily_split_for_date(self, day):
            return ({"688001"}, set())  # a stray DB row, not rostered that day

        async def upsert_trading_calendar(self, day, rows):
            self.upserts.append((day, rows))
            return len(rows)

    storage = _OrphanOnlyStorage()

    async def fetch_suspended(day):
        return set()

    async def fetch_traded(day):
        return set()

    result = await build_calendar(
        storage,
        trading_days=[date(2024, 1, 2)],  # before 600000's list_date → roster empty
        fetch_suspended=fetch_suspended,
        fetch_traded=fetch_traded,
        min_listing=0,
    )
    assert result["skipped_days"] == []
    assert result["by_state"][ORPHAN] == 1


@pytest.mark.asyncio
async def test_build_calendar_raises_on_truncated_roster():
    # ROSTER GUARD: a near-empty stock_listing_info (truncate→re-insert interrupted)
    # must abort the whole build — reconciling against it would mark the whole market
    # orphan (purge-eligible). Default min_listing applies.
    storage = _FakeCalStorage()  # only 2 listing rows ≪ MIN_LISTING_GUARD

    async def fetch_suspended(day):
        return set()

    async def fetch_traded(day):
        return {"600000"}, set()

    with pytest.raises(RuntimeError, match="残名单|stock_listing_info"):
        await build_calendar(
            storage,
            trading_days=[date(2024, 1, 2)],
            fetch_suspended=fetch_suspended,
            fetch_traded=fetch_traded,
        )
    assert storage.upserts == []  # nothing was written


def test_alignment_suspects_flags_only_kimi_rostered_source_none():
    # Fix-4 guard: a source_none code rostered by a KIMI row WITH a list_date = likely
    # alignment defect (kimi gave a date but Tushare has no data). A Tushare-listed
    # source_none = genuine 源头也无 (not flagged). No list_date / no row = not rostered.
    listing = {
        "888888": {"list_date": date(2020, 1, 1), "source": "kimi"},  # suspect
        "920039": {"list_date": date(2021, 8, 18), "source": "tushare_stock_basic"},  # genuine gap
        "999999": {"list_date": None, "source": "kimi-not-found"},  # not rostered
    }
    sn = {"888888", "920039", "999999"}
    assert alignment_suspects(sn, listing) == ["888888"]
    # a source_none code with no listing row at all → not a suspect
    assert alignment_suspects({"777777"}, listing) == []
    # nothing source_none → empty
    assert alignment_suspects(set(), listing) == []


# --- minute_state reconcile (阶段3) ---


def test_minute_state_only_for_traded_stocks():
    # minute_counts given → traded stocks get a minute_state (≥241 ok, else missing);
    # a stock that didn't trade (suspended) gets minute_state=None (no minute expected).
    rows = _by_code(
        reconcile_day(
            roster={"600000", "300001", "688001"},
            suspended={"688001"},
            traded={"600000", "300001"},
            db_normal={"600000", "300001"},
            db_suspended={"688001"},
            minute_counts={"600000": 241, "300001": 120},  # 300001 partial
        )
    )
    assert rows["600000"]["minute_state"] == MINUTE_OK  # full day
    assert rows["300001"]["minute_state"] == MINUTE_MISSING  # <241 → fillable
    assert rows["688001"]["minute_state"] is None  # suspended → no minute expected


def test_minute_state_none_when_no_minute_counts():
    # daily-only path (minute_counts None) → minute_state stays NULL for everyone.
    rows = _by_code(
        reconcile_day(
            roster={"600000"},
            suspended=set(),
            traded={"600000"},
            db_normal={"600000"},
            db_suspended=set(),
        )
    )
    assert rows["600000"]["minute_state"] is None


def test_minute_source_short_preserved_over_missing():
    # a traded stock with <241 bars the fill already confirmed source-short must stay
    # source_short on re-reconcile (not flip back to missing → no re-burn loop).
    rows = _by_code(
        reconcile_day(
            roster={"600000"},
            suspended=set(),
            traded={"600000"},
            db_normal={"600000"},
            db_suspended=set(),
            minute_counts={"600000": 120},  # half day
            minute_source_short={"600000"},
        )
    )
    assert rows["600000"]["minute_state"] == MINUTE_SOURCE_SHORT


class _FakeMinuteStorage(_FakeCalStorage):
    async def get_minute_bar_counts(self, day):
        return {"600000": 241, "300001": 0}  # 600000 full minute, 300001 none

    async def get_minute_source_short_codes(self, day):
        return set()


@pytest.mark.asyncio
async def test_build_calendar_with_minute_populates_minute_states():
    storage = _FakeMinuteStorage()

    async def fetch_suspended(day):
        return set()

    async def fetch_traded(day):
        return {"600000", "300001"}

    result = await build_calendar(
        storage,
        trading_days=[date(2024, 1, 2)],
        fetch_suspended=fetch_suspended,
        fetch_traded=fetch_traded,
        with_minute=True,
        min_listing=0,
    )
    # 600000 traded + 241 bars → minute ok; 300001 traded + 0 bars → minute missing
    assert result["by_minute"][MINUTE_OK] == 1
    assert result["by_minute"][MINUTE_MISSING] == 1


@pytest.mark.asyncio
async def test_build_calendar_extra_source_short_persists_over_missing():
    # The minute fill found 300001 source-short → the confirm re-reconcile gets it via
    # extra_minute_source_short and must mark it source_short (not missing → no re-burn).
    storage = _FakeMinuteStorage()  # 300001 has 0 bars (would be missing without the flag)

    async def fetch_suspended(day):
        return set()

    async def fetch_traded(day):
        return {"600000", "300001"}

    result = await build_calendar(
        storage,
        trading_days=[date(2024, 1, 2)],
        fetch_suspended=fetch_suspended,
        fetch_traded=fetch_traded,
        with_minute=True,
        extra_minute_source_short={date(2024, 1, 2): {"300001"}},
        min_listing=0,
    )
    assert result["by_minute"][MINUTE_OK] == 1  # 600000
    assert result["by_minute"][MINUTE_SOURCE_SHORT] == 1  # 300001 (was the half-day)
    assert result["by_minute"].get(MINUTE_MISSING, 0) == 0
