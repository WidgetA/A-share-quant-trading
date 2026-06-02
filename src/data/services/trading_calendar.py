# === MODULE PURPOSE ===
# Build the trading-calendar truth table (DAT-006): for one trading day, reconcile
# the authoritative sources into a per-stock status, then materialize it so every
# gap check / backfill references ONE queryable truth instead of recomputing.
#
# reconcile_day() is a PURE function (no I/O) — fully unit-tested. build_calendar()
# is the thin runner that pulls the day's inputs (suspend_d + Tushare daily +
# backtest_daily) and writes the reconciled rows.
#
# Design (see docs/features.md DAT-006): the table stores EVERY code that shows up
# in any source that day, tagged with status. No filtering at write time (that is
# exactly the class of bug — silently dropped rows — we are eliminating). Filtering
# happens only when the table is READ as an index (status views).

from __future__ import annotations

from datetime import date
from typing import Awaitable, Callable

# --- trade_status (this stock, this day — shared across data types) ---
TRADING = "trading"  # 正常交易 (Tushare daily has a real bar)
SUSPENDED = "suspended"  # 停牌 (in suspend_d, no real bar)
UNKNOWN = "unknown"  # 源头无法定性 (listed, neither traded nor suspended at source)

# --- daily_state (compare what the DB SHOULD have vs what it HAS) ---
OK = "ok"
MISSING = "missing"  # 真缺/停牌没占位待补
WRONG_SUSPENDED = "wrong_suspended"  # 库标停牌、源头当天却有成交 → 该是真实行
WRONG_TRADED = "wrong_traded"  # 库有真实行、源头当天却查无 → 该是占位/不该存在
ORPHAN = "orphan"  # 有数据却不在当天在册名单
SOURCE_NONE = "source_none"  # 在册、不停牌、源头也查无 (长期挂着,不假装解决)


def reconcile_day(
    roster: set[str],
    suspended: set[str],
    traded: set[str],
    db_normal: set[str],
    db_suspended: set[str],
) -> list[dict]:
    """Reconcile one trading day into per-stock truth rows. Pure function.

    Args:
        roster:       codes listed on the exchange that day (list_date ≤ D < delist_date).
        suspended:    codes in Tushare suspend_d that day.
        traded:       codes in Tushare daily that day (real bar / volume > 0).
        db_normal:    codes in backtest_daily that day with is_suspended=false.
        db_suspended: codes in backtest_daily that day with is_suspended=true.

    Returns one dict per (code) with: code, listed, trade_status, daily_state, reason.
    `roster` codes are `listed=True`; codes only present in the DB (not in roster)
    are `listed=False` orphans.
    """
    rows: list[dict] = []

    for code in sorted(roster):
        is_traded = code in traded
        is_susp = code in suspended
        has_real = code in db_normal
        has_susp = code in db_suspended

        if is_traded:
            trade_status = TRADING
            if has_real:
                daily_state = OK
            elif has_susp:
                daily_state = WRONG_SUSPENDED
            else:
                daily_state = MISSING
        elif is_susp:
            trade_status = SUSPENDED
            if has_susp:
                daily_state = OK
            elif has_real:
                daily_state = WRONG_TRADED
            else:
                daily_state = MISSING
        else:
            # listed, but source has neither a trade nor a suspension record
            trade_status = SUSPENDED if has_susp else UNKNOWN
            daily_state = WRONG_TRADED if has_real else SOURCE_NONE

        rows.append(
            {
                "code": code,
                "listed": True,
                "trade_status": trade_status,
                "daily_state": daily_state,
                "reason": None,
            }
        )

    # Orphans: present in the DB but NOT in the day's roster (not-listed/delisted).
    for code in sorted((db_normal | db_suspended) - roster):
        if code in traded:
            trade_status = TRADING
        elif code in suspended:
            trade_status = SUSPENDED
        else:
            trade_status = UNKNOWN
        rows.append(
            {
                "code": code,
                "listed": False,
                "trade_status": trade_status,
                "daily_state": ORPHAN,
                "reason": None,
            }
        )

    return rows


def roster_for_day(listing_info: dict[str, dict], day: date) -> set[str]:
    """Codes on the exchange on `day` = list_date ≤ day < delist_date.

    `listing_info` is `get_listing_info_all()` output: code → {list_date, delist_date, ...}.
    A code with no list_date is treated as listed (conservative — keep it visible).
    """
    out: set[str] = set()
    for code, meta in listing_info.items():
        ld = meta.get("list_date")
        dd = meta.get("delist_date")
        if ld is not None and day < ld:
            continue
        if dd is not None and day >= dd:
            continue
        out.add(code)
    return out


async def build_calendar(
    storage,
    *,
    trading_days: list[date],
    fetch_suspended: Callable[[date], Awaitable[set[str]]],
    fetch_traded: Callable[[date], Awaitable[set[str]]],
) -> dict:
    """Reconcile each day in `trading_days` and upsert into trading_calendar.

    `fetch_suspended(day)` → Tushare suspend_d codes; `fetch_traded(day)` → Tushare
    daily codes that actually traded. backtest_daily + listing_info come from storage.
    Returns counts for the run summary.
    """
    listing_info = await storage.get_listing_info_all()
    total_rows = 0
    by_state: dict[str, int] = {}
    for day in trading_days:
        roster = roster_for_day(listing_info, day)
        suspended = await fetch_suspended(day)
        traded = await fetch_traded(day)
        db_normal, db_suspended = await storage.get_daily_split_for_date(day)
        rows = reconcile_day(roster, suspended, traded, db_normal, db_suspended)
        total_rows += await storage.upsert_trading_calendar(day, rows)
        for r in rows:
            st = r["daily_state"]
            by_state[st] = by_state.get(st, 0) + 1
    # "problem" = anything that isn't clean-ok or the known-OK source_none gap.
    problem_rows = sum(v for k, v in by_state.items() if k not in (OK, SOURCE_NONE))
    return {
        "days": len(trading_days),
        "rows": total_rows,
        "problem_rows": problem_rows,
        "by_state": by_state,
    }
