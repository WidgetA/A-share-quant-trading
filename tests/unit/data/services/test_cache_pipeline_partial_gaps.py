# === MODULE PURPOSE ===
# Tests for CachePipeline._fill_partial_gaps after the fix: a real-trading
# (non-suspended) stock missing from a partially-filled date must be
# RE-DOWNLOADED (e.g. 北交所 920xxx added to Tushare daily later), not given up
# on. Suspended-only gaps still take the fast local placeholder path.

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from src.data.services.cache_pipeline import CachePipeline


class _Reporter:
    async def progress(self, *a: Any, **k: Any) -> None: ...
    async def status(self, *a: Any, **k: Any) -> None: ...
    async def notify_suspended_stocks(self, *a: Any, **k: Any) -> None: ...
    async def notify_null_data(self, *a: Any, **k: Any) -> None: ...
    async def notify_suspend_d_failure(self, *a: Any, **k: Any) -> None: ...


class _DailySource:
    EXCHANGES = ("TUSHARE",)

    def __init__(self, records: list[dict]):
        self._records = records
        self.fetch_calls = 0

    async def fetch_day(self, day: date) -> tuple[list[dict], list[str]]:
        self.fetch_calls += 1
        return self._records, []


class _MetadataSource:
    def __init__(self, suspended: set[str]):
        self._suspended = suspended

    async def fetch_suspended(self, day: date) -> set[str]:
        return set(self._suspended)


class _Storage:
    def __init__(self, expected: set[str], existing: set[str]):
        self._expected = set(expected)
        self.daily: set[str] = set(existing)
        self.inserted: list[tuple[str, dict]] = []

    async def get_effective_universe_for_date(self, day: date) -> set[str]:
        return set(self._expected)

    async def get_codes_for_daily_date(self, day: date) -> set[str]:
        return set(self.daily)

    async def get_previous_closes_before(self, day: date) -> dict[str, float]:
        return {}

    async def insert_daily_record(self, code: str, day: date, rec: dict) -> None:
        self.inserted.append((code, rec))
        self.daily.add(code)


def _pipeline(storage, daily_source, metadata_source):
    return CachePipeline(
        storage=storage,  # type: ignore[arg-type]
        daily_source=daily_source,  # type: ignore[arg-type]
        minute_source=object(),  # type: ignore[arg-type]
        metadata_source=metadata_source,  # type: ignore[arg-type]
        reporter=_Reporter(),  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_real_trading_missing_stock_is_redownloaded():
    """北交所-style real-trading stock missing from a partial date → re-download."""
    day = date(2026, 4, 15)
    storage = _Storage(expected={"600000", "920001"}, existing={"600000"})
    daily = _DailySource(
        records=[
            {"ticker": "600000", "open": 10, "high": 10, "low": 10, "close": 10, "volume": 1},
            {"ticker": "920001", "open": 5, "high": 5.2, "low": 4.9, "close": 5.1, "volume": 7},
        ]
    )
    meta = _MetadataSource(suspended=set())
    pipe = _pipeline(storage, daily, meta)

    await pipe._fill_partial_gaps([(day, 2, 1)], prev_close_map={}, cancel_event=None)

    assert daily.fetch_calls == 1  # it DID re-fetch the day's daily
    assert "920001" in storage.daily  # the real-missing stock got inserted
    by_code = {c: r for c, r in storage.inserted}
    assert by_code["920001"]["is_suspended"] is False  # real OHLCV, not a placeholder
    assert "600000" not in by_code  # already had it → skipped, not re-inserted


@pytest.mark.asyncio
async def test_suspended_only_gap_takes_fast_path_no_daily_call():
    """Only suspended stocks missing → fast placeholder fill, no daily API call."""
    day = date(2026, 4, 15)
    storage = _Storage(expected={"600000", "300001"}, existing={"600000"})
    daily = _DailySource(records=[])
    meta = _MetadataSource(suspended={"300001"})
    pipe = _pipeline(storage, daily, meta)

    await pipe._fill_partial_gaps([(day, 2, 1)], prev_close_map={}, cancel_event=None)

    assert daily.fetch_calls == 0  # suspended-only → no daily re-fetch
    by_code = {c: r for c, r in storage.inserted}
    assert by_code["300001"]["is_suspended"] is True  # placeholder
