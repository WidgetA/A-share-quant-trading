from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

import src.web.v20_service as service_module
from src.data.clients.tushare_realtime import (
    TushareDailyBar,
    TushareMinuteBar,
    tushare_minute_bars_to_early_market_data,
)
from src.data.database.v20_repository import (
    MinuteBarRecord,
    V20RepositoryError,
    V20SemanticConflict,
    sha256_json,
)
from src.strategy.strategies.v16_scanner import V16ScanResult
from src.web.v20_canonical_selection import _stable_external_market_fact_hash
from src.web.v20_scan_pipeline import FrozenV16ScanBundle
from src.web.v20_service import V20Service, _bar_payload, _daily_snapshot_payload, _DayContext
from src.web.v20_v16_canonical_artifact import encode, hydrate

TZ = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 9, 3)
ARTIFACT_RECEIPT = datetime(2026, 9, 3, 9, 39, 20, tzinfo=TZ)


def _bar(code: str, label: str, *, close: float = 10.0) -> TushareMinuteBar:
    hour, minute = (int(part) for part in label.split(":"))
    return TushareMinuteBar(
        stock_code=code,
        bar_end=datetime(2026, 9, 3, hour, minute, tzinfo=TZ),
        end_label=label,
        open_price=close,
        high_price=close,
        low_price=close,
        close_price=close,
        volume=100.0,
        amount=close * 100.0,
    )


def _record(
    code: str,
    label: str,
    *,
    close: float = 10.0,
    received_at: datetime,
) -> MinuteBarRecord:
    bar = _bar(code, label, close=close)
    payload = _bar_payload(bar)
    return MinuteBarRecord(
        code=code,
        bar_end=bar.bar_end,
        end_label=label,
        source_hash=sha256_json(payload),
        payload=payload,
        first_received_at=received_at,
    )


def _seed_service(
    monkeypatch: pytest.MonkeyPatch,
    *,
    universe: tuple[str, ...],
    records: list[MinuteBarRecord],
) -> tuple[V20Service, Any, Any]:
    class Repository:
        def __init__(self) -> None:
            self.calls: list[tuple[tuple[str, ...], datetime | None]] = []

        async def list_raw_minute_bar_records(
            self,
            codes,
            *,
            trade_date,
            end_labels,
            received_before=None,
        ):
            assert trade_date == TRADE_DATE
            assert "09:39" in end_labels
            self.calls.append((tuple(codes), received_before))
            # Deliberately return post-cutoff rows too: the service must keep a
            # defensive application-side boundary even when a test adapter or
            # alternate repository does not enforce the SQL predicate.
            return [record for record in records if record.code in set(codes)]

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        async def batch_get_early_market_data(self, *_args, **_kwargs):
            self.calls += 1
            raise AssertionError("a frozen artifact boundary must never backfill")

    repository = Repository()
    client = Client()
    service = object.__new__(V20Service)
    service._repository = repository
    service._scan_state = SimpleNamespace(realtime_client=client)
    service._clock = lambda: datetime(2026, 9, 3, 22, 0, tzinfo=TZ)

    boards = {"board": tuple((code, code) for code in universe)}

    def derive(
        _state,
        *,
        universe_override=None,
        clean_boards_override=None,
    ):
        selected_universe = universe if universe_override is None else universe_override
        selected_boards = boards if clean_boards_override is None else clean_boards_override
        return None, None, selected_boards, tuple(selected_universe)

    monkeypatch.setattr(service_module, "derive_canonical_v16_universe", derive)
    return service, repository, client


@pytest.mark.asyncio
async def test_frozen_seed_uses_artifact_receipt_and_ignores_late_raw(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code = "000001"
    records = [
        _record(
            code,
            "09:39",
            received_at=ARTIFACT_RECEIPT - timedelta(seconds=5),
        ),
        _record(
            code,
            "09:38",
            close=99.0,
            received_at=ARTIFACT_RECEIPT + timedelta(minutes=1),
        ),
    ]
    service, repository, client = _seed_service(
        monkeypatch,
        universe=(code,),
        records=records,
    )

    seed, universe, _boards = await service._historical_early_evidence_seed(
        TRADE_DATE,
        universe_override=(code,),
        clean_boards_override={"board": ((code, code),)},
        exact_evidence_codes=(code,),
        received_before=ARTIFACT_RECEIPT,
        allow_backfill=False,
    )

    assert repository.calls == [((code,), ARTIFACT_RECEIPT)]
    assert universe == (code,)
    assert [bar.end_label for bar in seed[code].early_bars] == ["09:39"]
    assert client.calls == 0


@pytest.mark.asyncio
async def test_frozen_seed_keeps_formal_missing_code_missing_without_rt_backfill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    visible = "000001"
    formally_missing = "000002"
    records = [
        _record(
            visible,
            "09:39",
            received_at=ARTIFACT_RECEIPT - timedelta(seconds=5),
        ),
        _record(
            formally_missing,
            "09:39",
            close=88.0,
            received_at=ARTIFACT_RECEIPT + timedelta(minutes=1),
        ),
    ]
    service, repository, client = _seed_service(
        monkeypatch,
        universe=(visible, formally_missing),
        records=records,
    )

    seed, universe, _boards = await service._historical_early_evidence_seed(
        TRADE_DATE,
        universe_override=(visible, formally_missing),
        clean_boards_override={"board": ((visible, visible), (formally_missing, formally_missing))},
        exact_evidence_codes=(visible,),
        received_before=ARTIFACT_RECEIPT,
        allow_backfill=False,
    )

    assert repository.calls == [((visible,), ARTIFACT_RECEIPT)]
    assert universe == (visible, formally_missing)
    assert set(seed) == {visible}
    assert client.calls == 0


@pytest.mark.asyncio
async def test_frozen_seed_fails_closed_when_artifact_evidence_was_not_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code = "000001"
    service, _repository, client = _seed_service(
        monkeypatch,
        universe=(code,),
        records=[
            _record(
                code,
                "09:39",
                received_at=ARTIFACT_RECEIPT,
            )
        ],
    )

    with pytest.raises(V20SemanticConflict, match="frozen raw barrier is incomplete"):
        await service._historical_early_evidence_seed(
            TRADE_DATE,
            universe_override=(code,),
            clean_boards_override={"board": ((code, code),)},
            exact_evidence_codes=(code,),
            received_before=ARTIFACT_RECEIPT,
            allow_backfill=False,
        )

    assert client.calls == 0


@pytest.mark.asyncio
async def test_artifact_hydration_completion_does_not_expand_raw_receipt_cutoff() -> None:
    sentinel = object()
    record = SimpleNamespace(first_received_at=ARTIFACT_RECEIPT)

    class Store:
        async def load(self, **_kwargs):
            return record

        async def hydrate(self, loaded):
            assert loaded is record
            return sentinel

    service = object.__new__(V20Service)
    service._canonical_artifact_store = Store()
    service._canonical_barrier_completed_at = {TRADE_DATE: ARTIFACT_RECEIPT + timedelta(minutes=5)}
    service.config = SimpleNamespace(official_stream_id="shadow")

    loaded = await service._load_canonical_artifact(TRADE_DATE)

    assert loaded == (sentinel, ARTIFACT_RECEIPT)


@pytest.mark.asyncio
async def test_artifact_hydration_rejects_raw_received_at_exact_artifact_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code = "000001"
    bundle = _frozen_bundle()
    payload = {"portable": "artifact"}
    record = SimpleNamespace(
        payload=payload,
        snapshot_hash=sha256_json(payload),
        trade_date=TRADE_DATE,
        first_received_at=ARTIFACT_RECEIPT,
    )

    class Repository:
        def __init__(self) -> None:
            self.received_before = None

        async def list_raw_minute_bar_records(self, *_args, received_before=None, **_kwargs):
            self.received_before = received_before
            return [
                _record(
                    code,
                    "09:39",
                    received_at=ARTIFACT_RECEIPT,
                )
            ]

    service = object.__new__(V20Service)
    service._canonical_artifact_store = object()
    repository = Repository()
    service._repository = repository
    monkeypatch.setattr(
        service_module,
        "hydrate_v16_canonical_artifact",
        lambda _payload: SimpleNamespace(bundle=bundle),
    )

    with pytest.raises(V20SemanticConflict, match="complete durable raw barrier"):
        await service._hydrate_canonical_artifact_record(record)
    assert repository.received_before == ARTIFACT_RECEIPT


@pytest.mark.asyncio
async def test_frozen_replay_reuses_d1_snapshot_at_artifact_cutoff_without_source_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prior_date = TRADE_DATE - timedelta(days=1)
    calendar = tuple(TRADE_DATE - timedelta(days=offset) for offset in range(37, 0, -1)) + (
        TRADE_DATE,
        TRADE_DATE + timedelta(days=1),
        TRADE_DATE + timedelta(days=2),
    )
    daily = {
        "000001": TushareDailyBar(
            stock_code="000001",
            trade_date=prior_date.strftime("%Y%m%d"),
            close_price=10.0,
            amount_yuan=100_000.0,
        )
    }

    class Repository:
        def __init__(self) -> None:
            self.cutoffs: list[datetime] = []

        async def list_daily_bar_snapshots(self, trade_date, *, received_before):
            assert trade_date == prior_date
            self.cutoffs.append(received_before)
            return (
                [
                    SimpleNamespace(
                        payload=_daily_snapshot_payload(prior_date, daily),
                        first_received_at=ARTIFACT_RECEIPT - timedelta(minutes=1),
                    )
                ],
                (),
            )

    service = object.__new__(V20Service)
    repository = Repository()
    service._repository = repository
    service._clock = lambda: datetime(2026, 9, 3, 22, 0, tzinfo=TZ)
    service._scan_state = SimpleNamespace(
        realtime_client=None,
        historical_adapter=object(),
        fundamentals_db=SimpleNamespace(
            batch_current_names=AsyncMock(return_value={"000001": "name"})
        ),
    )
    service._historical_early_evidence_seed = AsyncMock(
        return_value=(
            {"000001": SimpleNamespace(quote=SimpleNamespace(is_trading=True))},
            ("000001",),
            {"board": (("000001", "name"),)},
        )
    )
    monkeypatch.setattr(
        service_module,
        "derive_canonical_v16_universe",
        lambda *_args, **_kwargs: (
            None,
            None,
            {"board": (("000001", "name"),)},
            ("000001",),
        ),
    )
    monkeypatch.setattr(
        service_module,
        "_fetch_history_ohlcv",
        AsyncMock(return_value={"000001": {"time": [], "close": []}}),
    )
    context = _DayContext(
        trade_date=TRADE_DATE,
        calendar=calendar,
        canonical_fact_received_before=ARTIFACT_RECEIPT,
        canonical_fact_universe=("000001",),
        canonical_fact_evidence_codes=("000001",),
        canonical_fact_allow_backfill=False,
        canonical_fact_persist_raw=False,
    )

    frozen = await service._historical_canonical_inputs(context)

    assert repository.cutoffs == [ARTIFACT_RECEIPT]
    assert frozen.prior_daily == daily


@pytest.mark.asyncio
async def test_frozen_replay_rejects_d1_snapshot_at_exact_artifact_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prior_date = TRADE_DATE - timedelta(days=1)
    calendar = tuple(TRADE_DATE - timedelta(days=offset) for offset in range(37, 0, -1)) + (
        TRADE_DATE,
        TRADE_DATE + timedelta(days=1),
        TRADE_DATE + timedelta(days=2),
    )
    daily = {
        "000001": TushareDailyBar(
            stock_code="000001",
            trade_date=prior_date.strftime("%Y%m%d"),
            close_price=10.0,
            amount_yuan=100_000.0,
        )
    }

    class Repository:
        async def list_daily_bar_snapshots(self, *_args, **_kwargs):
            return (
                [
                    SimpleNamespace(
                        payload=_daily_snapshot_payload(prior_date, daily),
                        first_received_at=ARTIFACT_RECEIPT,
                    )
                ],
                (),
            )

    service = object.__new__(V20Service)
    service._repository = Repository()
    service._clock = lambda: datetime(2026, 9, 3, 22, 0, tzinfo=TZ)
    service._scan_state = SimpleNamespace(
        realtime_client=None,
        historical_adapter=object(),
        fundamentals_db=object(),
    )
    monkeypatch.setattr(
        service_module,
        "derive_canonical_v16_universe",
        lambda *_args, **_kwargs: (None, None, {"board": ()}, ("000001",)),
    )
    context = _DayContext(
        trade_date=TRADE_DATE,
        calendar=calendar,
        canonical_fact_received_before=ARTIFACT_RECEIPT,
        canonical_fact_universe=("000001",),
        canonical_fact_evidence_codes=("000001",),
        canonical_fact_calendar=calendar,
        canonical_fact_allow_backfill=False,
        canonical_fact_persist_raw=False,
    )

    with pytest.raises(V20SemanticConflict, match="lacks a D1 daily fact"):
        await service._historical_canonical_inputs(context)


def _frozen_bundle(
    *,
    market_fact_hash: str = "1" * 64,
    model_hash: str = "3" * 64,
    feature_hash: str = "4" * 64,
    history_hash: str = "5" * 64,
) -> FrozenV16ScanBundle:
    previous_dates = tuple(TRADE_DATE - timedelta(days=offset) for offset in range(37, 0, -1))
    future = (TRADE_DATE + timedelta(days=1), TRADE_DATE + timedelta(days=2))
    snapshot = {
        "early_market_source_hash": market_fact_hash,
        "early_market_conflict_codes": [],
        "breadth_market_source_hash": "2" * 64,
        "breadth_market_missing_codes": [],
        "breadth_market_conflict_codes": [],
        "scorer_model_sha256": model_hash,
        "scorer_feature_sha256": feature_hash,
        "raw_evidence_codes": ["000001"],
        "scan_input_codes": ["000001"],
        "scan_input_failure_codes": [],
        "history_profile_id": "CANONICAL_V16_V1",
        "history_input_hashes": {"000001": history_hash},
        "history_date_valid_counts": {day.isoformat(): 1 for day in previous_dates},
        "history_min_date_coverage": 1.0,
        "comparison_pool_codes": ["000001"],
        "comparison_pool_hash": sha256_json(["000001"]),
        "breadth_valid_n": 1,
        "breadth_down_n": 0,
        "prior_trade_date": previous_dates[-1].isoformat(),
        "prior_amount_yuan": {},
    }
    return FrozenV16ScanBundle(
        trade_date=TRADE_DATE,
        frozen_at=datetime(2026, 9, 3, 9, 39, 10, tzinfo=TZ),
        scan_result=V16ScanResult(recommended=[]),
        stock_data={},
        comparison_pool_codes=("000001",),
        breadth_valid_n=1,
        breadth_down_n=0,
        prior_trade_date=previous_dates[-1],
        prior_amount_yuan={},
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
        computation_calendar=(TRADE_DATE, *future),
    )


@pytest.mark.asyncio
async def test_resolver_passes_artifact_exact_boundary_to_scanner_recomputation() -> None:
    expected = _frozen_bundle()
    canonical = SimpleNamespace(
        trade_date=TRADE_DATE,
        computed_at=datetime(2026, 9, 3, 22, 0, tzinfo=TZ),
        computation_calendar=tuple(
            date.fromisoformat(day) for day in expected.snapshot["history_date_valid_counts"]
        )
        + expected.computation_calendar,
    )
    compute = AsyncMock(return_value=canonical)
    service = SimpleNamespace(
        _canonical_artifact_store=object(),
        _context=None,
        _calendar_cache=(),
        _calendar_loaded_for=None,
        _load_canonical_artifact=AsyncMock(return_value=(expected, ARTIFACT_RECEIPT)),
        _compute_canonical_v16_from_persisted_raw=compute,
        _project_canonical_v16=lambda _canonical, *, calendar: expected,
        _verify_frozen_canonical_input_identity=(
            V20Service._verify_frozen_canonical_input_identity
        ),
        config=SimpleNamespace(
            official_stream_id="shadow",
            strategy_dependency_hashes={
                "models/lgbrank_latest.txt": "3" * 64,
                "models/feature_list.json": "4" * 64,
            },
        ),
    )

    await V20Service._resolve_canonical_morning_bundle(
        service,
        TRADE_DATE,
        terminal_status=SimpleNamespace(),
    )

    _context = compute.await_args.args[0]
    assert _context.trade_date == TRADE_DATE
    assert _context.canonical_fact_universe == ("000001",)
    assert _context.canonical_fact_evidence_codes == ("000001",)
    assert _context.canonical_fact_received_before == ARTIFACT_RECEIPT
    assert _context.canonical_fact_calendar is not None
    assert tuple(_context.canonical_fact_calendar)[-3:] == expected.computation_calendar
    assert _context.canonical_fact_allow_backfill is False
    assert _context.canonical_fact_persist_raw is False
    assert compute.await_args.kwargs == {}


@pytest.mark.asyncio
async def test_terminal_without_artifact_fails_closed_but_no_terminal_can_still_compute() -> None:
    actual = _frozen_bundle()
    calendar = (
        tuple(date.fromisoformat(day) for day in actual.snapshot["history_date_valid_counts"])
        + actual.computation_calendar
    )
    canonical = SimpleNamespace(
        trade_date=TRADE_DATE,
        computed_at=datetime(2026, 9, 3, 9, 39, 10, tzinfo=TZ),
        computation_calendar=calendar,
    )
    compute = AsyncMock(return_value=canonical)
    service = SimpleNamespace(
        _canonical_artifact_store=object(),
        _context=None,
        _calendar_cache=calendar,
        _calendar_loaded_for=TRADE_DATE,
        _load_canonical_artifact=AsyncMock(return_value=None),
        _compute_canonical_v16_from_persisted_raw=compute,
        _project_canonical_v16=lambda _canonical, *, calendar: actual,
        config=SimpleNamespace(
            official_stream_id="shadow",
            strategy_dependency_hashes={
                "models/lgbrank_latest.txt": "3" * 64,
                "models/feature_list.json": "4" * 64,
            },
        ),
    )

    with pytest.raises(V20SemanticConflict, match="lacks its canonical V16 artifact"):
        await V20Service._resolve_canonical_morning_bundle(
            service,
            TRADE_DATE,
            terminal_status=SimpleNamespace(action="ENTER"),
        )
    compute.assert_not_awaited()

    service._load_canonical_artifact = AsyncMock(side_effect=(None, (actual, ARTIFACT_RECEIPT)))
    service._persist_canonical_artifact_barrier = AsyncMock()
    await V20Service._resolve_canonical_morning_bundle(
        service,
        TRADE_DATE,
        terminal_status=SimpleNamespace(action="INPUT_INVALID"),
    )
    service._persist_canonical_artifact_barrier.assert_awaited_once_with(canonical)

    compute.reset_mock()
    service._canonical_artifact_store = None
    await V20Service._resolve_canonical_morning_bundle(service, TRADE_DATE)
    live_context = compute.await_args.args[0]
    assert live_context.canonical_fact_universe is None
    assert live_context.canonical_fact_evidence_codes is None
    assert live_context.canonical_fact_received_before is None
    assert live_context.canonical_fact_allow_backfill is True
    assert live_context.canonical_fact_persist_raw is True
    assert compute.await_args.kwargs == {}


@pytest.mark.asyncio
async def test_input_invalid_ignores_legacy_artifact_and_keeps_comparison_unavailable() -> None:
    legacy_artifact = _frozen_bundle(market_fact_hash="1" * 64)
    current = _frozen_bundle(market_fact_hash="2" * 64)
    calendar = (
        tuple(date.fromisoformat(day) for day in current.snapshot["history_date_valid_counts"])
        + current.computation_calendar
    )
    canonical = SimpleNamespace(
        trade_date=TRADE_DATE,
        computed_at=datetime(2026, 9, 3, 22, 0, tzinfo=TZ),
        computation_calendar=calendar,
    )
    compute = AsyncMock(return_value=canonical)
    persist = AsyncMock()
    service = SimpleNamespace(
        _canonical_artifact_store=object(),
        _context=None,
        _calendar_cache=calendar,
        _calendar_loaded_for=TRADE_DATE,
        _load_canonical_artifact=AsyncMock(return_value=(legacy_artifact, ARTIFACT_RECEIPT)),
        _compute_canonical_v16_from_persisted_raw=compute,
        _project_canonical_v16=lambda _canonical, *, calendar: current,
        _persist_canonical_artifact_barrier=persist,
        _verify_frozen_canonical_input_identity=(
            V20Service._verify_frozen_canonical_input_identity
        ),
        config=SimpleNamespace(
            official_stream_id="shadow",
            strategy_dependency_hashes={
                "models/lgbrank_latest.txt": "3" * 64,
                "models/feature_list.json": "4" * 64,
            },
        ),
    )

    result = await V20Service._resolve_canonical_morning_bundle(
        service,
        TRADE_DATE,
        terminal_status=SimpleNamespace(action="INPUT_INVALID"),
    )

    replay_context = compute.await_args.args[0]
    assert replay_context.canonical_fact_received_before is None
    assert replay_context.canonical_fact_universe is None
    assert replay_context.canonical_fact_evidence_codes is None
    assert replay_context.canonical_fact_allow_backfill is True
    assert replay_context.canonical_fact_persist_raw is True
    assert result[4:] == (False, None)
    persist.assert_not_awaited()


def test_input_identity_drift_fails_closed_instead_of_reporting_successful_comparison() -> None:
    expected = _frozen_bundle(history_hash="5" * 64)
    drifted = _frozen_bundle(history_hash="9" * 64)

    with pytest.raises(V20SemanticConflict, match="input identity differs"):
        V20Service._verify_frozen_canonical_input_identity(expected, drifted)


def test_model_and_feature_changes_are_output_differences() -> None:
    expected = _frozen_bundle()
    current_code = _frozen_bundle(
        model_hash="a" * 64,
        feature_hash="b" * 64,
    )

    V20Service._verify_frozen_canonical_input_identity(expected, current_code)
    assert current_code.snapshot_hash != expected.snapshot_hash


def _external_fact_hash(
    *,
    early_source_hash: str,
    close_price: float = 10.0,
    amount_yuan: float = 1_000_000.0,
    calendar: tuple[date, ...] | None = None,
) -> str:
    previous_dates = tuple(TRADE_DATE - timedelta(days=offset) for offset in range(37, 0, -1))
    fact_calendar = calendar or (
        *previous_dates,
        TRADE_DATE,
        TRADE_DATE + timedelta(days=1),
        TRADE_DATE + timedelta(days=2),
    )
    return _stable_external_market_fact_hash(
        TRADE_DATE,
        ("000001",),
        {"board": (("000001", "示例公司"),)},
        {"000001": early_source_hash},
        {
            "000001": TushareDailyBar(
                stock_code="000001",
                trade_date=previous_dates[-1].strftime("%Y%m%d"),
                close_price=close_price,
                amount_yuan=amount_yuan,
            )
        },
        {"000001": "示例公司"},
        fact_calendar,
    )


def test_full_calendar_projects_to_portable_replay_window_and_relevant_drift_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.unit.web.test_v20_canonical_projection_acceptance import (
        ARTIFACT_CALENDAR,
        FULL_EXCHANGE_CALENDAR,
        HISTORY_DATES,
        _canonical,
        _rehash,
    )
    from tests.unit.web.test_v20_canonical_projection_acceptance import (
        TRADE_DATE as PROJECTION_DATE,
    )
    from tests.unit.web.test_v20_service import _service

    base = _canonical()
    prior_daily = {
        code: TushareDailyBar(
            stock_code=code,
            trade_date=HISTORY_DATES[-1].strftime("%Y%m%d"),
            close_price=float(base.prev_closes[code]),
            amount_yuan=1_000_000.0,
        )
        for code in base.prev_closes
    }
    names = {code: data.name for code, data in base.stock_data.items()}

    def calendar_hash(calendar: tuple[date, ...]) -> str:
        return _stable_external_market_fact_hash(
            PROJECTION_DATE,
            base.universe,
            base.clean_boards,
            base.early_source_hashes,
            prior_daily,
            names,
            calendar,
        )

    extra_history = tuple(HISTORY_DATES[0] - timedelta(days=offset) for offset in range(20, 0, -1))
    full_calendar = (
        *extra_history,
        *HISTORY_DATES,
        PROJECTION_DATE,
        *(PROJECTION_DATE + timedelta(days=offset) for offset in range(1, 8)),
    )
    compact_calendar = (
        *HISTORY_DATES,
        PROJECTION_DATE,
        PROJECTION_DATE + timedelta(days=1),
        PROJECTION_DATE + timedelta(days=2),
    )
    full_hash = calendar_hash(full_calendar)
    compact_hash = calendar_hash(compact_calendar)
    assert full_hash == compact_hash

    service = _service(monkeypatch, SimpleNamespace())
    formal_canonical = _rehash(replace(base, external_market_fact_hash=full_hash))
    formal = service._project_canonical_v16(
        formal_canonical,
        calendar=FULL_EXCHANGE_CALENDAR,
    )
    hydrated = hydrate(
        encode(
            formal,
            calendar=ARTIFACT_CALENDAR,
            canonical_integrity_hash=formal_canonical._integrity_hash,
        )
    ).bundle
    replay_canonical = _rehash(replace(base, external_market_fact_hash=compact_hash))
    replay = service._project_canonical_v16(
        replay_canonical,
        calendar=FULL_EXCHANGE_CALENDAR,
    )
    V20Service._verify_frozen_canonical_input_identity(hydrated, replay)

    drifted_calendar = (
        *compact_calendar[:-2],
        PROJECTION_DATE + timedelta(days=2),
        PROJECTION_DATE + timedelta(days=3),
    )
    drifted_hash = calendar_hash(drifted_calendar)
    assert drifted_hash != full_hash
    drifted_canonical = _rehash(replace(base, external_market_fact_hash=drifted_hash))
    with pytest.raises(V20SemanticConflict, match="early_market_source_hash"):
        V20Service._verify_frozen_canonical_input_identity(
            hydrated,
            service._project_canonical_v16(
                drifted_canonical,
                calendar=FULL_EXCHANGE_CALENDAR,
            ),
        )


async def _replay_with_market_fact_hashes(
    expected_hash: str,
    actual_hash: str,
) -> None:
    expected = _frozen_bundle(market_fact_hash=expected_hash)
    actual = _frozen_bundle(market_fact_hash=actual_hash)
    canonical = SimpleNamespace(
        trade_date=TRADE_DATE,
        computed_at=datetime(2026, 9, 3, 22, 0, tzinfo=TZ),
        computation_calendar=tuple(
            date.fromisoformat(day) for day in expected.snapshot["history_date_valid_counts"]
        )
        + expected.computation_calendar,
    )
    service = SimpleNamespace(
        _canonical_artifact_store=object(),
        _context=None,
        _calendar_cache=(),
        _calendar_loaded_for=None,
        _load_canonical_artifact=AsyncMock(return_value=(expected, ARTIFACT_RECEIPT)),
        _compute_canonical_v16_from_persisted_raw=AsyncMock(return_value=canonical),
        _project_canonical_v16=lambda _canonical, *, calendar: actual,
        _verify_frozen_canonical_input_identity=(
            V20Service._verify_frozen_canonical_input_identity
        ),
        config=SimpleNamespace(
            official_stream_id="shadow",
            strategy_dependency_hashes={
                "models/lgbrank_latest.txt": "3" * 64,
                "models/feature_list.json": "4" * 64,
            },
        ),
    )
    await V20Service._resolve_canonical_morning_bundle(
        service,
        TRADE_DATE,
        terminal_status=SimpleNamespace(action="ENTER"),
    )


@pytest.mark.asyncio
async def test_raw_label_arriving_after_formal_compute_before_artifact_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    code = "000001"
    formal_early = tushare_minute_bars_to_early_market_data(
        code,
        (_bar(code, "09:39"),),
        TRADE_DATE,
    )
    service, _repository, client = _seed_service(
        monkeypatch,
        universe=(code,),
        records=[
            _record(
                code,
                "09:39",
                received_at=ARTIFACT_RECEIPT - timedelta(seconds=10),
            ),
            # This previously absent label arrives after formal computation but
            # before the artifact row obtains its receipt timestamp.
            _record(
                code,
                "09:38",
                received_at=ARTIFACT_RECEIPT - timedelta(seconds=2),
            ),
        ],
    )
    replay_seed, _universe, _boards = await service._historical_early_evidence_seed(
        TRADE_DATE,
        universe_override=(code,),
        clean_boards_override={"board": ((code, code),)},
        exact_evidence_codes=(code,),
        received_before=ARTIFACT_RECEIPT,
        allow_backfill=False,
    )
    replay_early = replay_seed[code]
    assert formal_early is not None
    assert formal_early.source_hash != replay_early.source_hash
    assert client.calls == 0
    with pytest.raises(V20SemanticConflict, match="early_market_source_hash"):
        await _replay_with_market_fact_hashes(
            _external_fact_hash(early_source_hash=formal_early.source_hash),
            _external_fact_hash(early_source_hash=replay_early.source_hash),
        )


@pytest.mark.asyncio
async def test_d1_candidate_arriving_after_formal_compute_before_artifact_is_rejected() -> None:
    code = "000001"
    prior_date = TRADE_DATE - timedelta(days=1)
    calendar = tuple(TRADE_DATE - timedelta(days=offset) for offset in range(37, 0, -1)) + (
        TRADE_DATE,
        TRADE_DATE + timedelta(days=1),
        TRADE_DATE + timedelta(days=2),
    )
    formal_daily = {
        code: TushareDailyBar(
            stock_code=code,
            trade_date=prior_date.strftime("%Y%m%d"),
            close_price=10.0,
            amount_yuan=1_000_000.0,
        )
    }
    revised_daily = {
        code: replace(
            formal_daily[code],
            close_price=10.01,
            amount_yuan=1_000_001.0,
        )
    }

    class Repository:
        async def list_daily_bar_snapshots(self, trade_date, *, received_before):
            assert trade_date == prior_date
            assert received_before == ARTIFACT_RECEIPT
            return (
                [
                    SimpleNamespace(
                        payload=_daily_snapshot_payload(prior_date, revised_daily),
                        first_received_at=ARTIFACT_RECEIPT - timedelta(seconds=2),
                    ),
                    SimpleNamespace(
                        payload=_daily_snapshot_payload(prior_date, formal_daily),
                        first_received_at=ARTIFACT_RECEIPT - timedelta(seconds=10),
                    ),
                ],
                (),
            )

    early = tushare_minute_bars_to_early_market_data(
        code,
        (_bar(code, "09:39"),),
        TRADE_DATE,
    )
    assert early is not None
    service = object.__new__(V20Service)
    service._repository = Repository()
    service._clock = lambda: datetime(2026, 9, 3, 22, 0, tzinfo=TZ)
    service._scan_state = SimpleNamespace(
        realtime_client=None,
        historical_adapter=object(),
        fundamentals_db=SimpleNamespace(batch_current_names=AsyncMock(return_value={code: "name"})),
    )
    service._historical_early_evidence_seed = AsyncMock(
        return_value=(
            {code: early},
            (code,),
            {"board": ((code, "name"),)},
        )
    )
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            service_module,
            "derive_canonical_v16_universe",
            lambda *_args, **_kwargs: (
                None,
                None,
                {"board": ((code, "name"),)},
                (code,),
            ),
        )
        monkeypatch.setattr(
            service_module,
            "_fetch_history_ohlcv",
            AsyncMock(return_value={code: {"time": [], "close": []}}),
        )
        frozen = await service._historical_canonical_inputs(
            _DayContext(
                trade_date=TRADE_DATE,
                calendar=calendar,
                canonical_fact_received_before=ARTIFACT_RECEIPT,
                canonical_fact_universe=(code,),
                canonical_fact_evidence_codes=(code,),
                canonical_fact_calendar=calendar,
                canonical_fact_allow_backfill=False,
                canonical_fact_persist_raw=False,
            )
        )
    assert frozen.prior_daily == revised_daily

    def fact_hash(daily: dict[str, TushareDailyBar]) -> str:
        return _stable_external_market_fact_hash(
            TRADE_DATE,
            frozen.universe,
            frozen.clean_boards,
            {code: early.source_hash},
            daily,
            frozen.names,
            frozen.calendar,
        )

    with pytest.raises(V20SemanticConflict, match="early_market_source_hash"):
        await _replay_with_market_fact_hashes(
            fact_hash(formal_daily),
            fact_hash(frozen.prior_daily),
        )


@pytest.mark.asyncio
async def test_postcutoff_model_feature_and_scanner_output_change_is_success_different_readonly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.unit.web.test_v20_auto_manual_exact_parity_acceptance import (
        POST_CUTOFF_AT,
        RUN_AT,
        _canonical_master,
        _rehash,
        _service_and_artifact,
    )

    automatic, automatic_repo, _automatic_artifact = _service_and_artifact(monkeypatch)
    await automatic._run_decision_iteration_with_cutoff(RUN_AT)
    assert automatic_repo.status is not None

    check_only, check_repo, _artifact = _service_and_artifact(
        monkeypatch,
        now=POST_CUTOFF_AT,
    )
    check_repo.status = automatic_repo.status
    check_repo.state = automatic_repo.state
    state_before = check_repo.state
    status_before = check_repo.status

    changed_model_hash = "a" * 64
    changed_feature_hash = "b" * 64
    baseline = _canonical_master()
    # This is a real scanner-output change, not merely a forged snapshot hash:
    # the current scanner returns one fewer recommendation while consuming the
    # exact same raw minutes, D1 values, universe, calendar, and history facts.
    changed_scan = replace(
        baseline.scan_result,
        recommended=list(baseline.scan_result.recommended[1:]),
        all_scored=list(baseline.scan_result.all_scored[1:]),
        st_eligible_codes=list(baseline.scan_result.st_eligible_codes[1:]),
        final_candidates=max(0, baseline.scan_result.final_candidates - 1),
    )
    changed_canonical = _rehash(
        replace(
            baseline,
            scan_result=changed_scan,
            model_sha256=changed_model_hash,
            feature_list_sha256=changed_feature_hash,
            # The legacy composite input hash also changes because it contains
            # model/feature and derived scanner structures.  It must not veto
            # comparison of the current output with the formal result.
            input_hash="9" * 64,
        )
    )
    check_only.config = replace(
        check_only.config,
        strategy_dependency_hashes={
            **check_only.config.strategy_dependency_hashes,
            "models/lgbrank_latest.txt": changed_model_hash,
            "models/feature_list.json": changed_feature_hash,
        },
    )
    observed_contexts: list[_DayContext] = []

    async def changed_scanner(context: _DayContext) -> Any:
        observed_contexts.append(context)
        return changed_canonical

    monkeypatch.setattr(
        check_only,
        "_compute_canonical_v16_from_persisted_raw",
        changed_scanner,
    )

    result = await check_only.trigger_canonical_selection_check_only(
        "postcutoff-current-model-output-change-001",
        POST_CUTOFF_AT,
    )

    assert result["calculation_result"] == "SUCCESS"
    assert result["official_comparison_result"] == "DIFFERENT"
    assert result["probe_result"] == "PASS"
    assert "symbols" in result["official_mismatch_fields"]
    assert observed_contexts
    assert observed_contexts[0].canonical_fact_received_before is not None
    assert observed_contexts[0].canonical_fact_allow_backfill is False
    assert check_repo.state == state_before
    assert check_repo.status == status_before
    assert check_repo.commit_entry_calls == 0
    assert check_repo.forbidden_write_calls == []
    assert check_repo.alert_write_calls == 1
    assert check_repo.raw_write_calls == 0


@pytest.mark.asyncio
async def test_postcutoff_history_provider_outage_fails_closed_without_success_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.unit.web.test_v20_auto_manual_exact_parity_acceptance import (
        POST_CUTOFF_AT,
        RUN_AT,
        _service_and_artifact,
    )

    automatic, automatic_repo, _automatic_artifact = _service_and_artifact(monkeypatch)
    await automatic._run_decision_iteration_with_cutoff(RUN_AT)
    assert automatic_repo.status is not None

    check_only, check_repo, _artifact = _service_and_artifact(
        monkeypatch,
        now=POST_CUTOFF_AT,
    )
    check_repo.status = automatic_repo.status
    check_repo.state = automatic_repo.state
    state_before = check_repo.state
    status_before = check_repo.status

    async def unavailable_history(_context: _DayContext) -> Any:
        raise V20RepositoryError("OHLCV history provider is unavailable")

    monkeypatch.setattr(
        check_only,
        "_compute_canonical_v16_from_persisted_raw",
        unavailable_history,
    )

    with pytest.raises(V20RepositoryError, match="history provider is unavailable"):
        await check_only.trigger_canonical_selection_check_only(
            "postcutoff-history-provider-outage-001",
            POST_CUTOFF_AT,
        )

    assert check_repo.state == state_before
    assert check_repo.status == status_before
    assert check_repo.commit_entry_calls == 0
    assert check_repo.forbidden_write_calls == []
    assert check_repo.alert_write_calls == 0
    assert check_repo.raw_write_calls == 0
