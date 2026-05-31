from __future__ import annotations

import pytest

from src.data.clients.greptime_storage import GreptimeBacktestStorage


class _FakeDB:
    async def fetchrow(self, sql: str):
        if "COUNT(DISTINCT stock_code)" in sql:
            return {"cnt": 1}
        if "SELECT 1 as x FROM backtest_minute" in sql:
            return {"x": 1}
        if "amount < 0" in sql:
            return {"cnt": 2}
        if "open_price <= 0" in sql and "backtest_daily" in sql:
            return {"cnt": 1}
        return {"cnt": 0}

    async def fetch(self, sql: str):
        if "HAVING COUNT(*) > 1" in sql:
            return []
        return []


@pytest.mark.asyncio
async def test_check_data_integrity_flags_daily_nonpositive_ohlc_and_negative_amount() -> None:
    storage = GreptimeBacktestStorage()
    storage.db = _FakeDB()  # type: ignore[assignment]

    issues = await storage.check_data_integrity()
    checks = {issue["check"] for issue in issues}

    assert "zero_price_active" in checks
    assert "negative_amount" in checks
    assert "negative_minute_amount" in checks
