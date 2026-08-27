from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest

from src.data.services.margin_risk_service import (
    BEIJING_TZ,
    MarginRiskDataError,
    MarginRiskProductionService,
    _empty_security_state,
    _encode_security_state,
)
from src.margin_risk.calculations import calculate_security_features
from src.margin_risk.config import MarginRiskConfig
from src.margin_risk.publication import latest_published_trade_date
from src.margin_risk.v2_calculations import robust_impulse_features


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
        return [{"exchange": exchange, "cal_date": day, "is_open": True} for day in self.dates]

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

    async def get_latest_aggregate(self):
        return {"trade_date": self.raw_end}


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


@pytest.mark.asyncio
async def test_audit_bootstraps_materialization_when_final_metric_is_already_current(
    monkeypatch,
) -> None:
    dates = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5)]
    source = _AuditSource(dates)

    class _CurrentMetricWithoutMaterializationStore(_AuditStore):
        async def get_complete_dates(self, start: date, end: date):
            return {dates[0], dates[1]}

        async def get_raw_date_range(self):
            return dates[0], dates[1]

        async def get_latest_metric(self):
            return {"trade_date": dates[1]}

        async def get_latest_aggregate(self):
            return None

    service = MarginRiskProductionService(
        SimpleNamespace(db=object()),
        config=MarginRiskConfig(history_start=dates[0]),
        source_factory=lambda: source,  # type: ignore[arg-type]
    )
    service.store = _CurrentMetricWithoutMaterializationStore(  # type: ignore[assignment]
        dates[1]
    )
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


@pytest.mark.asyncio
async def test_audit_stops_at_the_last_trading_day_upstream_has_published(monkeypatch) -> None:
    """Margin data only exists from 09:10 of the next trading day.

    Unattended runs start well before that (3am pipeline, restart bootstrap), so
    the target end must follow the publication boundary rather than "today - 1
    day" — an unpublished session is not a gap and must never be ingested or
    recorded as a failure.
    """

    now = datetime.now(BEIJING_TZ)
    days = [now.date() - timedelta(days=offset) for offset in (3, 2, 1, 0)]
    published_through = latest_published_trade_date(days, now=now)
    assert published_through is not None and published_through < now.date()

    source = _AuditSource(days)
    service = MarginRiskProductionService(
        SimpleNamespace(db=object()),
        config=MarginRiskConfig(history_start=days[0]),
        source_factory=lambda: source,  # type: ignore[arg-type]
    )
    service.store = _AuditStore(days[0])  # type: ignore[assignment]
    ingested: list[date] = []

    async def fake_ingest(source_arg, day, ordinary_stocks, ordinary_codes):
        ingested.append(day)

    async def fake_recompute(*, changed_from, next_open=None):
        return 1

    monkeypatch.setattr(service, "_ingest_day", fake_ingest)
    monkeypatch.setattr(service, "recompute", fake_recompute)

    result = await service.audit_and_fill()

    assert ingested == [day for day in days if day <= published_through]
    assert now.date() not in ingested
    assert result["published_through"] == published_through.isoformat()
    assert result["latest_complete"] == published_through.isoformat()
    assert result["remaining"] == 0
    assert result["failed"] == []


def test_incremental_security_state_matches_existing_batch_formula() -> None:
    config = MarginRiskConfig()
    service = MarginRiskProductionService(SimpleNamespace(db=object()), config=config)
    first = date(2026, 1, 1)
    dates = [first + timedelta(days=index) for index in range(100)]
    rows = []
    for index, day in enumerate(dates):
        if index % 17 == 0:
            continue
        buy_amount = 30.0 + (index % 9) * 2.5
        repayment_amount = 24.0 + (index % 7) * 3.0
        # Incomplete rows must not become valid robust-window observations,
        # while their balance remains the next row's denominator.
        if index in {45, 70, 82}:
            buy_amount = None
        if index in {54, 76}:
            repayment_amount = None
        rows.append(
            {
                "trade_date": day,
                "ts_code": "000001.SZ",
                "stock_code": "000001.SZ",
                "financing_balance": 1000.0 + index * 3.0,
                "financing_buy_amount": buy_amount,
                "financing_repayment_amount": repayment_amount,
            }
        )

    batch_features = calculate_security_features(dates, rows, config)
    batch_robust = robust_impulse_features(
        [feature["impulse_raw"] for feature in batch_features],
        window=config.nib_scale_window,
        min_periods=config.nib_scale_min_periods,
        threshold=config.negative_impulse_z_threshold,
        magnitude_normalizer=config.nib_magnitude_normalizer,
    )
    expected = {
        feature["trade_date"]: {**feature, **robust}
        for feature, robust in zip(batch_features, batch_robust, strict=True)
    }

    by_date = {row["trade_date"]: row for row in rows}
    state = _empty_security_state("000001.SZ")
    for day in dates:
        actual = service._advance_security_state(state, by_date.get(day))
        if actual is None:
            assert day not in expected
            continue
        wanted = expected[day]
        for field in (
            "financing_balance_prev",
            "net_flow_5d",
            "impulse_z",
            "negative_impulse_magnitude",
        ):
            if wanted[field] is None:
                assert actual[field] is None
            else:
                assert actual[field] == pytest.approx(wanted[field])
        assert actual["is_negative_impulse_v2"] == wanted["is_negative_impulse_v2"]


@pytest.mark.asyncio
async def test_materialization_appends_one_day_from_ready_state_without_historical_scan() -> None:
    previous = date(2026, 8, 5)
    current = date(2026, 8, 6)
    state = _empty_security_state("000001.SZ")
    state.update(
        {
            "current_balance": 100.0,
            "valid_history": [True] * 25,
            "net_flow_history": [1.0] * 4,
            "ema_fast_state": 0.01,
            "ema_slow_state": 0.005,
            "impulse_history": [0.001 + index * 0.0001 for index in range(60)],
        }
    )

    class _IncrementalStore:
        def __init__(self) -> None:
            self.state_days: list[date] = []
            self.aggregate_days: list[date] = []

        async def get_aggregate_rows(self, start: date, end: date):
            return [
                {
                    "trade_date": previous,
                    "source_updated_at": 1,
                    "state_count": 1,
                }
            ]

        async def get_latest_aggregate(self):
            return {"trade_date": previous, "state_count": 1}

        async def get_security_states(self, day: date):
            assert day == previous
            return [_encode_security_state(state)]

        async def get_all_security_rows(self, start: date, end: date):
            assert (start, end) == (current, current)
            return [
                {
                    "trade_date": current,
                    "stock_code": "000001.SZ",
                    "financing_balance": 101.0,
                    "financing_buy_amount": 12.0,
                    "financing_repayment_amount": 10.0,
                }
            ]

        async def replace_security_states(self, day: date, rows):
            self.state_days.append(day)
            assert len(rows) == 1

        async def replace_aggregate_day(self, day: date, row):
            self.aggregate_days.append(day)
            assert row["source_updated_at"] == 2

        async def prune_security_states_before(self, day: date):
            assert day == current

        async def get_security_codes(self, start: date, end: date):
            raise AssertionError("incremental path must not discover historical codes")

        async def get_security_rows(self, start: date, end: date, codes):
            raise AssertionError("incremental path must not scan historical security rows")

    store = _IncrementalStore()
    service = MarginRiskProductionService(SimpleNamespace(db=object()))
    service.store = store  # type: ignore[assignment]
    market_rows = [
        {"trade_date": previous, "updated_at": 1},
        {"trade_date": current, "updated_at": 2},
    ]

    await service._ensure_security_materialization(
        previous,
        current,
        [previous, current],
        market_rows,
    )

    assert store.state_days == [current]
    assert store.aggregate_days == [current]
