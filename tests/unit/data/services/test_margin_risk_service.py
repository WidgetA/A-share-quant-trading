from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from src.data.services.margin_risk_service import (
    MarginRiskDataError,
    MarginRiskProductionService,
)
from src.margin_risk.config import MarginRiskConfig


class _IngestStore:
    def __init__(self) -> None:
        self.security: tuple[date, list[dict]] | None = None
        self.market: tuple[date, dict] | None = None

    async def replace_security_day(self, day: date, rows: list[dict]) -> None:
        self.security = (day, rows)

    async def upsert_market_day(self, day: date, row: dict) -> None:
        self.market = (day, row)


class _IngestSource:
    def __init__(self, *, complete: bool = True) -> None:
        self.complete = complete

    async def fetch_margin(self, day: date):
        rows = [{"exchange_id": "SSE", "rzye": 600.0, "rzmre": 60.0, "rzche": 50.0}]
        if self.complete:
            rows.append({"exchange_id": "SZSE", "rzye": 400.0, "rzmre": 40.0, "rzche": 35.0})
        return rows

    async def fetch_margin_detail(self, day: date):
        return [
            {"ts_code": "000001.SZ", "rzye": 200.0, "rzmre": 20.0, "rzche": 18.0},
            {"ts_code": "510300.SH", "rzye": 300.0, "rzmre": 30.0, "rzche": 25.0},
        ]

    async def fetch_daily_basic(self, day: date):
        return [{"ts_code": "000001.SZ", "close": 10.0, "free_share": 2.0}]


def _ordinary_stock() -> dict:
    return {
        "ts_code": "000001.SZ",
        "symbol": "000001",
        "name": "平安银行",
        "market": "主板",
        "exchange": "SZSE",
        "list_date": date(1991, 4, 3),
        "delist_date": None,
    }


@pytest.mark.asyncio
async def test_ingest_day_requires_both_exchanges_and_writes_normalized_facts() -> None:
    service = MarginRiskProductionService(SimpleNamespace(db=object()))
    store = _IngestStore()
    service.store = store  # type: ignore[assignment]
    day = date(2026, 8, 6)
    stock = _ordinary_stock()

    await service._ingest_day(_IngestSource(), day, [stock], {"000001.SZ"})

    assert store.security is not None
    assert store.security[1] == [
        {
            "stock_code": "000001.SZ",
            "financing_balance": 200.0,
            "financing_buy_amount": 20.0,
            "financing_repayment_amount": 18.0,
        }
    ]
    assert store.market is not None
    market = store.market[1]
    assert market["market_financing_balance"] == 1000.0
    assert market["stock_financing_balance"] == 200.0
    assert market["free_float_market_cap"] == 200_000.0
    assert market["ordinary_margin_coverage"] == pytest.approx(0.2)
    assert market["ffmv_coverage"] == pytest.approx(1.0)
    assert market["ingestion_status"] == "OK"

    with pytest.raises(MarginRiskDataError, match="SZSE"):
        await service._ingest_day(
            _IngestSource(complete=False),
            day,
            [stock],
            {"000001.SZ"},
        )


class _AuditSource:
    def __init__(self, dates: list[date]) -> None:
        self.dates = dates
        self.started = False
        self.stopped = False
        self.stock_basic_calls = 0

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def fetch_trade_calendar(self, exchange: str, start: date, end: date):
        return [
            {"exchange": exchange, "cal_date": day, "is_open": True}
            for day in self.dates
        ]

    async def fetch_stock_basic(self):
        self.stock_basic_calls += 1
        return [_ordinary_stock()]


class _AuditStore:
    def __init__(self, raw_end: date) -> None:
        self.raw_end = raw_end

    async def ensure_schema(self) -> None:
        return None

    async def get_complete_dates(self, start: date, end: date):
        return set()

    async def get_raw_date_range(self):
        return self.raw_end, self.raw_end

    async def get_latest_metric(self):
        return None


@pytest.mark.asyncio
async def test_bounded_backfill_marks_latest_metric_available_on_next_actual_open_day(
    monkeypatch,
) -> None:
    dates = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5)]
    source = _AuditSource(dates)
    service = MarginRiskProductionService(
        SimpleNamespace(db=object()),
        config=MarginRiskConfig(history_start=dates[0]),
        source_factory=lambda: source,  # type: ignore[arg-type]
    )
    service.store = _AuditStore(dates[1])  # type: ignore[assignment]
    ingested: list[date] = []
    recompute_args: dict = {}

    async def fake_ingest(source_arg, day, ordinary_stocks, ordinary_codes):
        ingested.append(day)

    async def fake_recompute(*, changed_from, next_open=None):
        recompute_args.update(changed_from=changed_from, next_open=next_open)
        return 1

    monkeypatch.setattr(service, "_ingest_day", fake_ingest)
    monkeypatch.setattr(service, "recompute", fake_recompute)

    result = await service.audit_and_fill(start=dates[0], end=dates[1], max_days=1)

    assert source.started and source.stopped
    assert ingested == [dates[1]]
    assert recompute_args == {"changed_from": dates[1], "next_open": dates[2]}
    assert result["filled"] == 1
    assert result["remaining"] == 1


@pytest.mark.asyncio
async def test_audit_retries_stale_metric_calculation_even_when_raw_days_are_complete(
    monkeypatch,
) -> None:
    dates = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5)]
    source = _AuditSource(dates)

    class _CompleteButStaleStore(_AuditStore):
        async def get_complete_dates(self, start: date, end: date):
            return {dates[0], dates[1]}

        async def get_raw_date_range(self):
            return dates[0], dates[1]

        async def get_latest_metric(self):
            return {"trade_date": dates[0]}

    service = MarginRiskProductionService(
        SimpleNamespace(db=object()),
        config=MarginRiskConfig(history_start=dates[0]),
        source_factory=lambda: source,  # type: ignore[arg-type]
    )
    service.store = _CompleteButStaleStore(dates[1])  # type: ignore[assignment]
    recompute_args: dict = {}

    async def fake_recompute(*, changed_from, next_open=None):
        recompute_args.update(changed_from=changed_from, next_open=next_open)
        return 1

    monkeypatch.setattr(service, "recompute", fake_recompute)

    result = await service.audit_and_fill(start=dates[0], end=dates[1])

    assert source.stock_basic_calls == 0
    assert recompute_args == {"changed_from": dates[1], "next_open": dates[2]}
    assert result["filled"] == 0
    assert result["metrics"] == 1
