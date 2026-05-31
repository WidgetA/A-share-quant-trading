from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from src.data.services.cache_pipeline import CachePipeline


class _Reporter:
    async def progress(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def status(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def notify_suspended_stocks(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def notify_null_data(self, *args: Any, **kwargs: Any) -> None:
        pass

    async def notify_suspend_d_failure(self, *args: Any, **kwargs: Any) -> None:
        pass


class _DailySource:
    EXCHANGES = ("TUSHARE",)


class _Storage:
    def __init__(self) -> None:
        self.prev_queries: list[date] = []
        self.inserted: list[tuple[str, date, dict[str, Any]]] = []

    async def get_previous_closes_before(self, day: date) -> dict[str, float]:
        self.prev_queries.append(day)
        return {"600000": 9.9}

    async def insert_daily_record(self, code: str, day: date, record: dict[str, Any]) -> None:
        self.inserted.append((code, day, record))


@pytest.mark.asyncio
async def test_process_daily_date_uses_prev_close_before_that_day_not_latest_cache() -> None:
    storage = _Storage()
    pipeline = CachePipeline(
        storage=storage,  # type: ignore[arg-type]
        daily_source=_DailySource(),  # type: ignore[arg-type]
        minute_source=object(),  # type: ignore[arg-type]
        metadata_source=object(),  # type: ignore[arg-type]
        reporter=_Reporter(),  # type: ignore[arg-type]
    )

    await pipeline._process_daily_date(
        date(2024, 1, 3),
        suspended_codes=set(),
        records=[
            {
                "ticker": "600000",
                "open": 10.0,
                "high": 10.2,
                "low": 9.8,
                "close": 10.1,
                "volume": 123.0,
            }
        ],
        failed_exchanges=[],
        skip_codes=None,
        prev_close_map={"600000": 99.9},
        current=1,
        total=1,
    )

    assert storage.prev_queries == [date(2024, 1, 3)]
    assert storage.inserted[0][2]["pre_close"] == 9.9
