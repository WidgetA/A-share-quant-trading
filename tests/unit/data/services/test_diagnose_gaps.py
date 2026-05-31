# === MODULE PURPOSE ===
# Tests for the gap-diagnosis report (scripts/diagnose_gaps.py): the pure
# report builder (problem → root cause → what it should be → how to fix) and
# the orchestration wiring (classify each missing stock) with a fake store.

from __future__ import annotations

from datetime import date

import pytest

from scripts.diagnose_gaps import build_report, diagnose_gaps


def _diag(**over):
    base = {
        "all_time_count": 5659,
        "per_day_min": 4900,
        "per_day_max": 5527,
        "per_day_recent": 5523,
        "delisted_in_listing": 140,
        "listing_total": 5659,
        "listing_verified": 5659,
        "daily_gap_days": 2,
        "daily_items": [
            {"date": "2024-01-03", "code": "000001", "cls": "A", "reason": "已退市"},
            {"date": "2024-01-03", "code": "600000", "cls": "B", "reason": "真缺"},
        ],
        "minute_gap_days": 740,
        "minute_detail_days": 50,
        "minute_items": [
            {"date": "2023-01-03", "code": "300001", "cls": "C", "reason": "上市首日"},
        ],
    }
    base.update(over)
    return base


def test_report_has_three_problems_and_fix_lines():
    r = build_report(_diag())
    # three problems present
    assert "问题1" in r and "问题2" in r and "问题3" in r
    # each problem states root cause + what-it-should-be + how-to-fix
    assert "根因" in r and "真实应该" in r and "怎么修" in r
    # the headline numbers appear
    assert "5659" in r and "5527" in r and "740" in r
    # class breakdown for daily gaps
    assert "A 名单错 1" in r and "B 真缺 1" in r


def test_report_shows_sampling_transparency():
    r = build_report(_diag())
    assert "抽样最近 50 天" in r  # minute detail sampling disclosed, not silent


class _FakeStore:
    def __init__(self):
        self._listing = {
            "000001": {
                "list_date": date(2010, 1, 1),
                "delist_date": date(2023, 6, 1),
                "verified": True,
            },  # delisted before the gap day -> A
            "600000": {"list_date": date(2000, 1, 1), "delist_date": None, "verified": True},
        }

    async def get_listing_info_all(self):
        return self._listing

    async def get_daily_stock_count(self):
        return 5659

    async def get_daily_counts_per_day(self):
        return {date(2024, 1, 3): 5519}

    async def audit_daily_gaps(self):
        return [(date(2024, 1, 3), 5521, 5519)]

    async def get_effective_universe_for_date(self, d):
        return {"000001", "600000", "000002"}

    async def get_codes_for_daily_date(self, d):
        return {"000002"}  # so 000001 (delisted->A) and 600000 (listed->B) are missing

    async def audit_minute_gaps(self):
        return [(date(2023, 1, 3), 4900, 4899)]

    async def find_missing_minute_stocks(self, d):
        return {"600000"}, 4900

    async def get_minute_bar_counts(self, d):
        return {"600000": 0}  # zero bars -> B


@pytest.mark.asyncio
async def test_diagnose_classifies_each_missing_stock():
    store = _FakeStore()

    async def fetch_suspended(day):
        return set()

    diag = await diagnose_gaps(store, fetch_suspended, minute_detail_days=10)

    by_code = {it["code"]: it["cls"] for it in diag["daily_items"]}
    assert by_code["000001"] == "A"  # delisted -> name list wrong
    assert by_code["600000"] == "B"  # listed, not suspended, missing -> real gap
    # minute: zero bars on a non-IPO day -> B
    assert diag["minute_items"][0]["cls"] == "B"
    # report renders without error
    assert "问题1" in build_report(diag)
