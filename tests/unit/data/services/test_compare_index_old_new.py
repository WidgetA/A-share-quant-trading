# === MODULE PURPOSE ===
# Tests for the TEMPORARY old-vs-new index compare tool
# (scripts/compare_index_old_new.py). Pure logic — mock Tushare client +
# fake storage, no network / no DB. Verifies the set-difference per day,
# aggregate counts, and that the Feishu summary names both directions.

from __future__ import annotations

from datetime import date

import pytest

from scripts.compare_index_old_new import compare_index_range, format_feishu_summary


class _FakeStorage:
    def __init__(self, effective: dict[date, set[str]], listing_info=None):
        self._effective = effective
        self._listing_info = listing_info or {}

    async def get_listing_info_all(self):
        return self._listing_info

    async def get_effective_universe_for_date(self, day: date):
        return set(self._effective.get(day, set()))


class _FakeClient:
    def __init__(self, bak: dict[str, set[str]]):
        self._bak = bak  # keyed by "YYYYMMDD"

    async def fetch_bak_basic(self, yyyymmdd: str):
        return sorted(self._bak.get(yyyymmdd, set()))


@pytest.mark.asyncio
async def test_per_day_diffs_and_aggregate():
    d1 = date(2026, 5, 6)
    d2 = date(2026, 5, 7)
    client = _FakeClient(
        {
            "20260506": {"600519", "000001", "830001"},  # 830001 only in old
            "20260507": {"600519", "000001"},
        }
    )
    storage = _FakeStorage(
        {
            d1: {"600519", "000001", "920001"},  # 920001 (北交所) only in new
            d2: {"600519", "000001"},
        }
    )

    result = await compare_index_range(storage, client, [d1, d2], concurrency=2)

    assert result["days"] == 2
    by_date = {r["date"]: r for r in result["per_day"]}
    assert by_date["2026-05-06"]["only_in_old"] == ["830001"]
    assert by_date["2026-05-06"]["only_in_new"] == ["920001"]
    assert by_date["2026-05-07"]["only_in_old"] == []
    assert by_date["2026-05-07"]["only_in_new"] == []

    agg = result["agg"]
    assert agg["distinct_only_old"] == 1
    assert agg["distinct_only_new"] == 1
    assert agg["total_only_old"] == 1
    assert agg["total_only_new"] == 1


@pytest.mark.asyncio
async def test_reason_annotation_from_listing_info():
    d = date(2026, 5, 6)
    client = _FakeClient({"20260506": {"830001"}})
    storage = _FakeStorage(
        {d: set()},
        listing_info={
            "830001": {"list_date": date(2027, 1, 1), "delist_date": None, "verified": True}
        },
    )
    result = await compare_index_range(storage, client, [d])
    sample = result["agg"]["sample_only_old"]
    assert sample and sample[0][1] == "830001"
    assert "未上市" in sample[0][2]


@pytest.mark.asyncio
async def test_feishu_summary_mentions_both_directions():
    d = date(2026, 5, 6)
    client = _FakeClient({"20260506": {"830001"}})
    storage = _FakeStorage({d: {"920001"}})
    result = await compare_index_range(storage, client, [d])
    summary = format_feishu_summary(result)
    assert "新旧索引对照" in summary
    assert "830001" in summary  # only-in-old sample
    assert "920001" in summary  # only-in-new sample


def test_feishu_summary_empty():
    assert "无可对照" in format_feishu_summary({"days": 0})
