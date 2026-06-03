# === MODULE PURPOSE ===
# Regression test for the audit-path consistency fix: get_effective_universe_for_date
# (and audit_daily_gaps, which inlines the same blocklist) must EXCLUDE codes whose
# listing_info row has list_date=None — kimi "查不到" placeholders for dead/migrated
# codes — exactly like roster_for_day. Otherwise the audit counts them as expected and
# reports a permanent phantom gap. Codes with NO listing_info row at all still pass
# (tracked separately as "unverified").

from __future__ import annotations

import asyncio
from datetime import date

from src.data.clients.greptime_storage import GreptimeBacktestStorage


def _storage(snapshot: set[str], info: dict[str, dict]) -> GreptimeBacktestStorage:
    st = object.__new__(GreptimeBacktestStorage)

    async def _snap(day):
        return snapshot

    async def _info():
        return info

    # Instance attrs shadow the real methods the function calls on self.
    st.get_snapshot_codes_for_date = _snap  # type: ignore[attr-defined]
    st.get_listing_info_all = _info  # type: ignore[attr-defined]
    return st


def test_effective_universe_excludes_list_date_none_placeholder():
    day = date(2024, 1, 2)
    info = {
        # real listed stock → kept
        "600000": {"list_date": date(1999, 11, 10), "delist_date": None},
        # kimi 查不到 placeholder (no list_date) → must be excluded (mirror roster)
        "830964": {"list_date": None, "delist_date": None},
        # delisted before the day → excluded
        "000003": {"list_date": date(1991, 7, 3), "delist_date": date(2002, 6, 14)},
    }
    # "999999" is in snapshot but has NO listing_info row → must still pass through
    snapshot = {"600000", "830964", "000003", "999999"}
    st = _storage(snapshot, info)
    universe = asyncio.run(st.get_effective_universe_for_date(day))
    assert universe == {"600000", "999999"}
