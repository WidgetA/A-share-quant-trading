from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from src.data.clients.greptime_margin_risk import GreptimeMarginRiskStore
from src.data.clients.greptime_storage import date_to_epoch_ms


class _FakeDB:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.rows: list[dict] = []
        self.row: dict | None = None

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)

    async def fetch(self, sql: str):
        return self.rows

    async def fetchrow(self, sql: str):
        return self.row


@pytest.mark.asyncio
async def test_ensure_schema_creates_raw_and_metric_tables() -> None:
    db = _FakeDB()
    store = GreptimeMarginRiskStore(SimpleNamespace(db=db))

    await store.ensure_schema()

    ddl = "\n".join(db.executed)
    assert "margin_risk_security_daily" in ddl
    assert "margin_risk_market_daily" in ddl
    assert "margin_risk_metric_daily" in ddl
    assert "financing_repayment_amount FLOAT64" in ddl
    assert "persistent_deleveraging_path FLOAT64" in ddl
    assert "data_status STRING" in ddl


@pytest.mark.asyncio
async def test_security_day_write_is_idempotent_and_keeps_all_three_margin_facts() -> None:
    db = _FakeDB()
    store = GreptimeMarginRiskStore(SimpleNamespace(db=db))
    day = date(2026, 8, 6)

    written = await store.replace_security_day(
        day,
        [
            {
                "stock_code": "000001.SZ",
                "financing_balance": 100.0,
                "financing_buy_amount": 12.0,
                "financing_repayment_amount": 9.0,
            }
        ],
    )

    assert written == 1
    assert db.executed[0] == (
        "DELETE FROM margin_risk_security_daily WHERE ts = " + str(date_to_epoch_ms(day))
    )
    insert = db.executed[1]
    assert "000001.SZ" in insert
    assert "100.0,12.0,9.0" in insert


@pytest.mark.asyncio
async def test_security_code_read_splits_wide_range_and_deduplicates_codes() -> None:
    class _WindowDB:
        def __init__(self) -> None:
            self.queries: list[str] = []

        async def fetch(self, sql: str):
            self.queries.append(sql)
            if len(self.queries) == 1:
                return [{"stock_code": "600000.SH"}, {"stock_code": "000001.SZ"}]
            return [{"stock_code": "000001.SZ"}, {"stock_code": "300001.SZ"}]

    db = _WindowDB()
    store = GreptimeMarginRiskStore(SimpleNamespace(db=db))

    codes = await store.get_security_codes(date(2026, 1, 1), date(2026, 4, 30))

    assert codes == ["000001.SZ", "300001.SZ", "600000.SH"]
    assert len(db.queries) == 2
    assert f"ts >= {date_to_epoch_ms(date(2026, 1, 1))}" in db.queries[0]
    assert f"ts <= {date_to_epoch_ms(date(2026, 3, 1))}" in db.queries[0]
    assert f"ts >= {date_to_epoch_ms(date(2026, 3, 2))}" in db.queries[1]
    assert f"ts <= {date_to_epoch_ms(date(2026, 4, 30))}" in db.queries[1]


@pytest.mark.asyncio
async def test_security_row_read_splits_wide_range_and_restores_global_order() -> None:
    class _WindowDB:
        def __init__(self) -> None:
            self.queries: list[str] = []

        async def fetch(self, sql: str):
            self.queries.append(sql)
            if len(self.queries) == 1:
                return [
                    {
                        "stock_code": "600000.SH",
                        "ts": date_to_epoch_ms(date(2026, 2, 27)),
                        "financing_balance": 2.0,
                        "financing_buy_amount": 1.0,
                        "financing_repayment_amount": 0.5,
                    }
                ]
            return [
                {
                    "stock_code": "000001.SZ",
                    "ts": date_to_epoch_ms(date(2026, 3, 2)),
                    "financing_balance": 3.0,
                    "financing_buy_amount": 1.5,
                    "financing_repayment_amount": 0.8,
                }
            ]

    db = _WindowDB()
    store = GreptimeMarginRiskStore(SimpleNamespace(db=db))

    rows = await store.get_security_rows(
        date(2026, 1, 1),
        date(2026, 4, 30),
        ["000001.SZ", "600000.SH"],
    )

    assert len(db.queries) == 2
    assert [row["stock_code"] for row in rows] == ["000001.SZ", "600000.SH"]
    assert [row["ts_code"] for row in rows] == ["000001.SZ", "600000.SH"]
    assert [row["trade_date"] for row in rows] == [date(2026, 3, 2), date(2026, 2, 27)]


@pytest.mark.asyncio
async def test_metric_list_is_oldest_first_and_serializes_availability_date() -> None:
    db = _FakeDB()
    first = date(2026, 8, 5)
    second = date(2026, 8, 6)
    # Greptime query is DESC; the public curve contract is chronological.
    db.rows = [
        {
            "index_version": "mews_v2",
            "ts": date_to_epoch_ms(second),
            "signal_available_ts": date_to_epoch_ms(date(2026, 8, 7)),
            "mews": 60.0,
        },
        {
            "index_version": "mews_v2",
            "ts": date_to_epoch_ms(first),
            "signal_available_ts": date_to_epoch_ms(second),
            "mews": 55.0,
        },
    ]
    store = GreptimeMarginRiskStore(SimpleNamespace(db=db))

    points = await store.list_metrics(days=99_999)

    assert [point["date"] for point in points] == ["2026-08-05", "2026-08-06"]
    assert points[0]["signal_available_date"] == "2026-08-06"
    assert points[1]["signal_available_date"] == "2026-08-07"


@pytest.mark.asyncio
async def test_status_exposes_newest_failed_ingestion_separately_from_last_good_raw_day() -> None:
    good_day = date(2026, 8, 6)
    failed_day = date(2026, 8, 7)

    class _StatusDB:
        async def fetchrow(self, sql: str):
            if "MIN(ts)" in sql:
                return {
                    "min_ts": date_to_epoch_ms(date(2014, 9, 22)),
                    "max_ts": date_to_epoch_ms(good_day),
                }
            if "margin_risk_metric_daily" in sql:
                return {"ts": date_to_epoch_ms(good_day)}
            if "COUNT(*)" in sql:
                return {"count": 1}
            if "ORDER BY ts DESC LIMIT 1" in sql:
                return {
                    "ts": date_to_epoch_ms(failed_day),
                    "ingestion_status": "FAILED",
                }
            raise AssertionError(sql)

    store = GreptimeMarginRiskStore(SimpleNamespace(db=_StatusDB()))

    status = await store.status()

    assert status["raw_end"] == "2026-08-06"
    assert status["metric_end"] == "2026-08-06"
    assert status["latest_raw_date"] == "2026-08-07"
    assert status["latest_ingestion_status"] == "FAILED"
    assert status["failed_days"] == 1
