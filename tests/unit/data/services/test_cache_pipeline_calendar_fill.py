# === MODULE PURPOSE ===
# Tests for CachePipeline.fill_daily_from_calendar — the index-driven daily fill
# (DAT-006 阶段2). The key behaviour after the wrong_suspended fix: a code the
# truth-table marks `wrong_suspended` (a bogus suspended PLACEHOLDER in the DB
# while the source actually traded that day) must be RE-DOWNLOADED so the real
# bar upserts over the placeholder — even though the code already "exists".
# Codes we already have correctly must still be skipped.

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
    def silent_feishu(self) -> "_Reporter":
        return self


class _DailySource:
    EXCHANGES = ("TUSHARE",)

    def __init__(self, records: list[dict]):
        self._records = records
        self.fetch_calls = 0

    async def __aenter__(self) -> "_DailySource":
        return self

    async def __aexit__(self, *a: Any) -> bool:
        return False

    async def fetch_day(self, day: date) -> tuple[list[dict], list[str]]:
        self.fetch_calls += 1
        return self._records, []


class _MetadataSource:
    def __init__(self, suspended: set[str]):
        self._suspended = suspended

    async def __aenter__(self) -> "_MetadataSource":
        return self

    async def __aexit__(self, *a: Any) -> bool:
        return False

    async def fetch_suspended(self, day: date) -> set[str]:
        return set(self._suspended)


class _Storage:
    def __init__(self, fillable: dict[date, set[str]], existing: set[str]):
        self._fillable = fillable
        self.daily: set[str] = set(existing)
        self.inserted: list[tuple[str, dict]] = []

    async def get_calendar_fillable_by_date(self) -> dict[date, set[str]]:
        return {d: set(c) for d, c in self._fillable.items()}

    async def get_listing_info_all(self) -> dict:
        # Roster every code that appears (list_date None = always listed) — these
        # tests use suspended=set(), so roster only needs to exist, not gate.
        codes = set(self.daily)
        for c in self._fillable.values():
            codes |= c
        return {c: {"list_date": None, "delist_date": None} for c in codes}

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
async def test_wrong_suspended_placeholder_is_rewritten_not_skipped():
    """A wrong_suspended code already exists (placeholder) but must be re-written
    with the real bar — i.e. NOT in skip_codes."""
    day = date(2023, 1, 19)
    # 600355 is the bogus placeholder (exists in DB) AND fillable (wrong_suspended).
    storage = _Storage(fillable={day: {"600355"}}, existing={"688435", "600355"})
    daily = _DailySource(
        records=[
            {"ticker": "600355", "open": 9, "high": 9.5, "low": 8.9, "close": 9.3, "volume": 100},
            {"ticker": "688435", "open": 5, "high": 5, "low": 5, "close": 5, "volume": 1},
        ]
    )
    meta = _MetadataSource(suspended=set())  # source says it traded, not suspended
    pipe = _pipeline(storage, daily, meta)

    await pipe.fill_daily_from_calendar()

    assert daily.fetch_calls == 1
    by_code = {c: r for c, r in storage.inserted}
    # the wrong_suspended placeholder got overwritten with a real (non-suspended) bar
    assert "600355" in by_code
    assert by_code["600355"]["is_suspended"] is False
    # 688435 was already correct (exists, not fillable) → skipped, not rewritten
    assert "688435" not in by_code


@pytest.mark.asyncio
async def test_missing_code_filled_and_correct_existing_skipped():
    day = date(2025, 2, 11)
    # 920001 missing (not in DB); 600000 already correct (exists, not fillable).
    storage = _Storage(fillable={day: {"920001"}}, existing={"600000"})
    daily = _DailySource(
        records=[
            {"ticker": "600000", "open": 10, "high": 10, "low": 10, "close": 10, "volume": 1},
            {"ticker": "920001", "open": 5, "high": 5.2, "low": 4.9, "close": 5.1, "volume": 7},
        ]
    )
    meta = _MetadataSource(suspended=set())
    pipe = _pipeline(storage, daily, meta)

    result = await pipe.fill_daily_from_calendar()

    by_code = {c: r for c, r in storage.inserted}
    assert "920001" in by_code  # missing → filled
    assert "600000" not in by_code  # correct existing → skipped
    assert result["dates"] == 1
