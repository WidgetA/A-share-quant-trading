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

import logging
from datetime import date, datetime
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)

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

# --- minute_state (does a stock that TRADED today have a full minute day?) ---
# Only set for codes that traded (有真实日线 bar);others are NULL (no minute expected).
MINUTE_OK = "ok"  # backtest_minute 当天该票 bar 数 ≥ 241
MINUTE_MISSING = "missing"  # <241 且源头未确认短 → 可补 (阶段3)
MINUTE_SOURCE_SHORT = "source_short"  # 重下后源头真给不满 241 (半天交易) → 不再补
MINUTE_BARS_FULL = 241  # 9:30-11:30 + 13:00-15:00 = 240min + 收盘 = 241 根

# Fail-safe guard: an EMPTY `traded` (Tushare daily) for a day the DB already holds many
# real bars for is almost certainly a transient SOURCE failure (empty/garbled response),
# NOT a real "nobody traded" day. Reconciling it anyway flips every such code to
# wrong_traded (源头查无 → 该不该存在), corrupting good state from a one-off hiccup
# (observed 2026-06-03: a momentary empty `daily` turned 5511 ok rows into wrong_traded).
# Per CLAUDE.md trading-safety (incomplete data → skip; never silent-empty fallback) we
# SKIP that day's reconcile (leave its rows intact) + surface it loudly. A genuine
# non-trading day has 0 traded AND ~0 db rows, so it sails through.
_EMPTY_TRADED_GUARD_MIN_DB_ROWS = 100


def _minute_state(
    code: str,
    is_traded: bool,
    minute_counts: dict[str, int] | None,
    minute_source_short: set[str],
) -> str | None:
    """minute_state for one code on this day. None unless minute reconcile is on
    (minute_counts given) AND the stock actually traded (only traded stocks should
    have minute bars). ``source_short`` is PRESERVED (set by the fill once it confirms
    the source genuinely gives <241) so a re-reconcile doesn't flip it back to missing."""
    if minute_counts is None or not is_traded:
        return None
    if minute_counts.get(code, 0) >= MINUTE_BARS_FULL:
        return MINUTE_OK
    if code in minute_source_short:
        return MINUTE_SOURCE_SHORT
    return MINUTE_MISSING


def reconcile_day(
    roster: set[str],
    suspended: set[str],
    traded: set[str],
    db_normal: set[str],
    db_suspended: set[str],
    minute_counts: dict[str, int] | None = None,
    minute_source_short: set[str] | None = None,
) -> list[dict]:
    """Reconcile one trading day into per-stock truth rows. Pure function.

    Args:
        roster:       codes listed on the exchange that day (list_date ≤ D < delist_date).
        suspended:    codes in Tushare suspend_d that day.
        traded:       codes in Tushare daily that day (real bar / volume > 0).
        db_normal:    codes in backtest_daily that day with is_suspended=false.
        db_suspended: codes in backtest_daily that day with is_suspended=true.
        minute_counts: backtest_minute bar count per code that day (None ⇒ skip minute
            reconcile, minute_state left NULL — the daily-only path).
        minute_source_short: codes already confirmed source-short that day (preserve).

    Returns one dict per (code) with: code, listed, trade_status, daily_state,
    minute_state, reason. `roster` codes are `listed=True`; codes only present in the
    DB (not in roster) are `listed=False` orphans.
    """
    rows: list[dict] = []
    mss = minute_source_short or set()

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
                "minute_state": _minute_state(code, is_traded, minute_counts, mss),
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
                "minute_state": None,  # orphans aren't tracked for minute
                "reason": None,
            }
        )

    return rows


def roster_for_day(listing_info: dict[str, dict], day: date) -> set[str]:
    """Codes on the exchange on `day` = list_date ≤ day < delist_date.

    `listing_info` is `get_listing_info_all()` output: code → {list_date, delist_date, ...}.
    A code with NO list_date is treated as NOT listed (excluded). The only rows
    without a list_date are kimi "查不到" placeholders (verified=false) for dead/
    migrated codes — keeping them in the roster is exactly what made those codes
    linger as `source_none` forever (no list_date ⇒ never excluded, always rostered,
    Tushare daily never returns them). No confirmed list_date ⇒ not on the roster.
    Every authoritative row (Tushare stock_basic / kimi-confirmed) has a list_date.
    """
    out: set[str] = set()
    for code, meta in listing_info.items():
        ld = meta.get("list_date")
        if ld is None or day < ld:
            continue
        dd = meta.get("delist_date")
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
    with_minute: bool = False,
    extra_minute_source_short: dict[date, set[str]] | None = None,
) -> dict:
    """Reconcile each day in `trading_days` and upsert into trading_calendar.

    `fetch_suspended(day)` → Tushare suspend_d codes; `fetch_traded(day)` → Tushare
    daily codes that actually traded. backtest_daily + listing_info come from storage.

    `with_minute=True` ALSO reconciles `minute_state` (per traded stock: ≥241 bars=ok,
    else missing; preserving any existing source_short). It reads backtest_minute bar
    counts per day — ONE GROUP BY per day, ~15-30s on the ~1.8B-row table — so it's for
    the incremental nightly (1 new day) + the manual minute bootstrap, NOT a blanket
    full rebuild. Returns counts for the run summary.
    """
    listing_info = await storage.get_listing_info_all()
    total_rows = 0
    by_state: dict[str, int] = {}
    by_minute: dict[str, int] = {}
    skipped_days: list[date] = []  # days skipped because fetch_traded looked like a源头失败
    for day in trading_days:
        roster = roster_for_day(listing_info, day)
        suspended = await fetch_suspended(day)
        traded = await fetch_traded(day)
        db_normal, db_suspended = await storage.get_daily_split_for_date(day)
        # FAIL-SAFE: empty traded + many real DB rows ⇒ almost certainly a transient
        # Tushare `daily` failure, not a real no-trade day. Reconciling would mass-flip
        # those rows to wrong_traded (corrupting good state). Skip + surface, don't corrupt.
        if not traded and len(db_normal) >= _EMPTY_TRADED_GUARD_MIN_DB_ROWS:
            logger.error(
                "build_calendar: %s — fetch_traded 空但库里有 %d 行真实日线,几乎可断定是 Tushare "
                "daily 取数失败(空响应);跳过这天 reconcile,不把好状态刷成 wrong_traded",
                day,
                len(db_normal),
            )
            skipped_days.append(day)
            continue
        minute_counts = None
        minute_source_short = None
        if with_minute:
            # A minute-table query hiccup must NOT break the daily reconcile — degrade
            # to daily-only for that day (minute_state left NULL) and keep going.
            try:
                minute_counts = await storage.get_minute_bar_counts(day)
                minute_source_short = await storage.get_minute_source_short_codes(day)
                if extra_minute_source_short:
                    minute_source_short = minute_source_short | extra_minute_source_short.get(
                        day, set()
                    )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "build_calendar: minute counts for %s failed, daily-only: %s", day, e
                )
                minute_counts = None
                minute_source_short = None
        rows = reconcile_day(
            roster, suspended, traded, db_normal, db_suspended, minute_counts, minute_source_short
        )
        total_rows += await storage.upsert_trading_calendar(day, rows)
        for r in rows:
            by_state[r["daily_state"]] = by_state.get(r["daily_state"], 0) + 1
            ms = r.get("minute_state")
            if ms is not None:
                by_minute[ms] = by_minute.get(ms, 0) + 1
    # "problem" = anything that isn't clean-ok or the known-OK source_none gap.
    problem_rows = sum(v for k, v in by_state.items() if k not in (OK, SOURCE_NONE))
    return {
        "days": len(trading_days),
        "rows": total_rows,
        "problem_rows": problem_rows,
        "by_state": by_state,
        "by_minute": by_minute,
        "skipped_days": [d.isoformat() for d in skipped_days],
    }


async def rebuild_calendar(
    storage,
    *,
    trading_days: list[date] | None = None,
    start: date | None = None,
    end: date | None = None,
    client=None,
    with_minute: bool = False,
    extra_minute_source_short: dict[date, set[str]] | None = None,
) -> dict:
    """Rebuild ``trading_calendar`` — the shared runner used by both the
    ``/api/audit/calendar/rebuild`` endpoint and the daily maintenance pipeline.

    Wires the Tushare inputs build_calendar() needs (suspend_d + daily-traded)
    and owns a ``TushareRealtimeClient`` unless one is passed in (so a caller can
    reuse one client across several steps — e.g. load-tushare + rebuild + rebuild).

    Pass EITHER ``trading_days`` (an explicit list, e.g. only the dates a fill
    just touched — used by the pipeline's confirm step) OR ``start``+``end`` (a
    range, resolved to trading days via Tushare ``trade_cal``). Returns the
    build_calendar() summary dict (days / rows / problem_rows / by_state).
    """
    from src.common.config import get_tushare_token
    from src.data.clients.tushare_realtime import (
        TushareRealtimeClient,
        get_tushare_trade_calendar,
    )

    own_client = client is None
    if own_client:
        client = TushareRealtimeClient(token=get_tushare_token())
        await client.start()
    try:
        if trading_days is None:
            if start is None or end is None:
                raise ValueError("rebuild_calendar 需要 trading_days 或 (start, end)")
            cal = await get_tushare_trade_calendar(
                start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), token=get_tushare_token()
            )
            # get_tushare_trade_calendar returns YYYY-MM-DD; tolerate YYYYMMDD too.
            trading_days = [datetime.strptime(d.replace("-", ""), "%Y%m%d").date() for d in cal]

        async def fetch_suspended(day: date) -> set[str]:
            return await client.fetch_suspended_stocks(day.strftime("%Y%m%d"))

        async def fetch_traded(day: date) -> set[str]:
            recs = await client.fetch_daily(day.strftime("%Y%m%d"))
            return {r["ticker"] for r in recs if (r.get("volume") or 0) > 0}

        return await build_calendar(
            storage,
            trading_days=trading_days,
            fetch_suspended=fetch_suspended,
            fetch_traded=fetch_traded,
            with_minute=with_minute,
            extra_minute_source_short=extra_minute_source_short,
        )
    finally:
        if own_client:
            await client.stop()
