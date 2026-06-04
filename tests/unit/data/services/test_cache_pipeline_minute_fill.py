# === MODULE PURPOSE ===
# Tests for CachePipeline.fill_minute_from_calendar — the index-driven minute fill
# (DAT-006 阶段3). Mirrors the daily fill: only (day, code) the truth-table marks
# minute_state='missing' get re-downloaded; a code the source returns <241 bars for is
# reported as source_short (半天交易) so the re-reconcile stops retrying it; a code with
# 0 bars (empty/error) stays missing. No DB / no network — fakes for storage + source.

from __future__ import annotations

import asyncio
from datetime import date

import pytest

from src.data.services.cache_pipeline import CachePipeline


class _FakeBatch:
    def __init__(self, ok):
        self.ok = ok
        self.empty: list = []
        self.error = None
        self.error_codes: list = []
        self.unknown_exchange: list = []


class _FakeMinuteSource:
    def __init__(self, bars_by_code):
        self._bars = bars_by_code
        self.entered = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, *a):
        self.entered = False

    async def fetch_batches(self, codes, start, end):
        yield _FakeBatch({c: self._bars[c] for c in codes if c in self._bars})


class _FakeStorage:
    def __init__(self, fillable):
        self._fillable = fillable
        self.inserted: dict[str, list] = {}

    async def get_calendar_minute_fillable_by_date(self):
        return self._fillable

    async def insert_minute_bars(self, code, bars):
        self.inserted[code] = bars
        return len(bars)


def _bars(n):
    # n raw 1-min bars in stk_mins shape (trade_time long enough to pass the >=10 filter).
    return [{"trade_time": f"2024-01-02 {9 + i // 60:02d}:{i % 60:02d}:00"} for i in range(n)]


def _pipe(storage, source):
    pipe = object.__new__(CachePipeline)
    pipe.storage = storage
    pipe.minute_source = source
    pipe.reporter = None
    return pipe


@pytest.mark.asyncio
async def test_fill_minute_full_partial_and_empty():
    day = date(2024, 1, 2)
    # 600000: full 241 → ok (just inserted); 300001: half day 120 → source_short;
    # 688001: source returns nothing → stays missing (not inserted, not source_short).
    src = _FakeMinuteSource({"600000": _bars(241), "300001": _bars(120)})
    storage = _FakeStorage({day: {"600000", "300001", "688001"}})
    result = await asyncio.get_event_loop().create_task(
        _pipe(storage, src).fill_minute_from_calendar(quiet=False)
    )
    assert result["filled"] == 2  # 600000 + 300001 (688001 had no bars)
    assert result["processed_dates"] == [day]
    assert result["source_short"] == {day: {"300001"}}  # only the genuine half-day
    assert set(storage.inserted) == {"600000", "300001"}  # 688001 NOT written
    assert src.entered is False  # context exited cleanly


@pytest.mark.asyncio
async def test_fill_minute_no_gaps_short_circuits():
    storage = _FakeStorage({})  # nothing marked missing
    src = _FakeMinuteSource({})
    result = await _pipe(storage, src).fill_minute_from_calendar()
    assert result == {
        "dates": 0,
        "filled": 0,
        "processed_dates": [],
        "source_short": {},
        "processed": 0,
        "truncated": False,
        "remaining": 0,
    }
    assert storage.inserted == {}


@pytest.mark.asyncio
async def test_fill_minute_max_codes_caps_and_reports_remaining():
    # The nightly passes max_codes so an unattended pass can't run for hours on a
    # surprise backlog. When the cap is hit it stops at a batch boundary, touches only
    # the days it reached, and reports truncated + remaining (never silent).
    day1, day2 = date(2024, 1, 2), date(2024, 1, 3)
    src = _FakeMinuteSource({"600000": _bars(241), "600001": _bars(241)})
    storage = _FakeStorage({day1: {"600000"}, day2: {"600001"}})
    result = await _pipe(storage, src).fill_minute_from_calendar(max_codes=1)
    assert result["truncated"] is True
    assert result["filled"] == 1
    assert result["processed_dates"] == [day1]  # day2 never reached (capped first)
    assert result["remaining"] == 1  # total 2 − processed 1
    assert set(storage.inserted) == {"600000"}  # 600001 (day2) NOT downloaded
