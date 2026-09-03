from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import date, datetime, time, timezone
from types import MappingProxyType, SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.web import v15_scan_service, v20_service
from src.web.v15_scan_service import CanonicalV16ScanBundle, V15ScanState, _bundle_fingerprint
from src.web.v20_scan_pipeline import FrozenV16ScanBundle

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 3, 2)
RECEIVED_AT = datetime(2026, 3, 2, 1, 40, tzinfo=timezone.utc)
RECEIVED_AT_BEIJING = RECEIVED_AT.astimezone(BEIJING_TZ)


def _stock_data() -> Any:
    return SimpleNamespace(
        code="600000",
        name="Bank",
        open_price=10.0,
        prev_close=9.9,
        price_940=10.1,
        high_940=10.2,
        low_940=9.9,
        volume_940=1000.0,
        volume_937=900.0,
        avg_daily_volume=800.0,
        trend_5d=0.1,
        trend_10d=0.2,
        avg_daily_return_20d=0.001,
        volatility_20d=0.01,
        consecutive_up_days=1,
        history_df=None,
    )


def _scan_result(*, recommendations: int = 1) -> Any:
    recommended = tuple(
        SimpleNamespace(code="600000", name="Bank", buy_price=10.1, score=0.9)
        for _ in range(recommendations)
    )
    return SimpleNamespace(
        recommended=recommended,
        stock_best_board={"600000": "bank"},
        step2_hot_board_count=1,
        final_candidates=max(recommendations, 1),
    )


def _canonical(*, recommendations: int = 1) -> CanonicalV16ScanBundle:
    bundle = CanonicalV16ScanBundle(
        trade_date=TRADE_DATE,
        scan_result=_scan_result(recommendations=recommendations),
        stock_data={"600000": _stock_data()},
        clean_boards={"bank": (("600000", "Bank"),)},
        universe=("600000",),
        quotes={},
        prev_closes={"600000": 9.9},
        history_raw={},
        early_bars={},
        early_source_hashes={},
        failed_no_prev_close=(),
        failed_no_history=(),
        failed_build=(),
        skipped_new_listings=(),
        model_sha256="a" * 64,
        feature_list_sha256="b" * 64,
        computed_at=RECEIVED_AT,
        input_hash="c" * 64,
        _integrity_hash="",
    )
    return replace(bundle, _integrity_hash=_bundle_fingerprint(bundle))


def _frozen(*, recommendations: int = 1) -> FrozenV16ScanBundle:
    scan_result = _scan_result(recommendations=recommendations)
    stock_data = {"600000": _stock_data()}
    return FrozenV16ScanBundle(
        trade_date=TRADE_DATE,
        frozen_at=datetime.combine(TRADE_DATE, time(9, 39), tzinfo=BEIJING_TZ),
        scan_result=scan_result,
        stock_data=stock_data,
        comparison_pool_codes=("600000",),
        breadth_valid_n=1,
        breadth_down_n=0,
        prior_trade_date=TRADE_DATE,
        prior_amount_yuan=MappingProxyType({}),
        snapshot=MappingProxyType({}),
        snapshot_hash="d" * 64,
        legacy_recommendation=v15_scan_service._build_v16_recommendation_payload(
            scan_result,
            stock_data,
        ),
    )


def _at(hour: int, minute: int) -> datetime:
    return datetime.combine(TRADE_DATE, time(hour, minute), tzinfo=BEIJING_TZ)


@pytest.mark.asyncio
async def test_deadline_helper_refuses_premature_or_wrong_date_now(monkeypatch):
    state = V15ScanState(today_recommendation={"stock_code": "prior"})
    alerts: list[tuple[str, str]] = []
    evidence = v15_scan_service._CanonicalV16NotReadyEvidence(
        TRADE_DATE,
        _at(10, 0).replace(second=45),
    )

    async def error(title: str, detail: str) -> None:
        alerts.append((title, detail))

    monkeypatch.setattr(v15_scan_service, "_notify_feishu_error", error)
    await v15_scan_service._fail_not_ready_deadline(
        state,
        TRADE_DATE,
        _at(10, 0).replace(second=45),
        evidence,
    )
    await v15_scan_service._fail_not_ready_deadline(
        state,
        TRADE_DATE,
        _at(10, 1).replace(day=TRADE_DATE.day + 1),
        evidence,
    )

    assert alerts == []
    assert state.today_recommendation == {"stock_code": "prior"}
    assert state.scan_error is None
    assert state.scan_done_date == ""


@pytest.mark.asyncio
async def test_frozen_restore_never_populates_canonical_cache():
    state = V15ScanState(today_recommendation={"stock_code": "prior"})
    v15_scan_service._restore_canonical_artifact(
        state,
        TRADE_DATE,
        _frozen(),
        RECEIVED_AT,
    )

    assert state.canonical_coordinator is None
    assert state.today_recommendation == _frozen().legacy_recommendation
    assert state.scan_done_date == ""


def test_naive_receipt_is_rejected_without_partial_mutation():
    state = V15ScanState(
        today_recommendation={"stock_code": "prior"},
        scan_error="existing diagnostic",
    )

    with pytest.raises(
        v15_scan_service.CanonicalV16ArtifactProbeError,
        match="timestamp lacks a timezone",
    ):
        v15_scan_service._restore_canonical_artifact(
            state,
            TRADE_DATE,
            _canonical(),
            RECEIVED_AT.replace(tzinfo=None),
        )

    assert state.today_recommendation == {"stock_code": "prior"}
    assert state.scan_error == "existing diagnostic"
    assert state.scan_done_date == ""
    assert state.canonical_durable_received_at == {}
    assert state.canonical_coordinator is None


@pytest.mark.asyncio
async def test_v20_probe_carries_receipt_without_attaching_to_v16_runtime(monkeypatch):
    bundle = _frozen()

    class FakeStore:
        async def load(self, **_kwargs):
            return SimpleNamespace(
                trade_date=TRADE_DATE,
                payload=MappingProxyType({}),
                first_received_at=RECEIVED_AT,
            )

        async def hydrate(self, _record):
            return bundle

    service = object.__new__(v20_service.V20Service)
    service._canonical_artifact_store = FakeStore()
    service._canonical_barrier_completed_at = {}
    service.config = SimpleNamespace(official_stream_id="official-stream")

    loaded = await service._probe_canonical_artifact(TRADE_DATE)
    assert loaded == (bundle, RECEIVED_AT_BEIJING)

    class StoreFactory:
        def __init__(self, _repository):
            pass

    monkeypatch.setattr(v20_service, "V16CanonicalArtifactStore", StoreFactory)
    service = object.__new__(v20_service.V20Service)
    service._repository = SimpleNamespace(schema="public")
    service._scan_state = V15ScanState()
    service.config = SimpleNamespace(official_stream_id="official-stream")
    service._canonical_artifact_store = None
    service._canonical_sink_callback = None
    service._canonical_artifact_probe_callback = None
    service._canonical_callbacks_open = False
    service._canonical_barrier_completed_at = {}

    await service._initialize_canonical_artifact_boundary()
    assert service._canonical_artifact_store is not None
    assert service._canonical_callbacks_open is True
    assert service._scan_state.canonical_artifact_probe is None
    assert service._scan_state.canonical_sink is None

    service._detach_canonical_artifact_boundary()
    assert service._canonical_callbacks_open is False
    assert service._scan_state.canonical_artifact_probe is None


@pytest.mark.asyncio
async def test_v20_private_artifact_boundary_ignores_foreign_v16_callbacks(monkeypatch):
    class StoreFactory:
        def __init__(self, _repository):
            pass

    monkeypatch.setattr(v20_service, "V16CanonicalArtifactStore", StoreFactory)
    service = object.__new__(v20_service.V20Service)
    service._repository = SimpleNamespace(schema="public")
    service._scan_state = V15ScanState()
    service.config = SimpleNamespace(official_stream_id="official-stream")

    async def foreign_probe(_trade_date: date):
        return None

    async def owned_probe(_trade_date: date):
        return None

    async def sink(_bundle: Any) -> None:
        return None

    old_store = object()
    old_sink = sink
    service._canonical_artifact_store = old_store
    service._canonical_callbacks_open = True
    service._canonical_sink_callback = old_sink
    service._canonical_artifact_probe_callback = owned_probe
    service._scan_state.canonical_sink = old_sink
    service._scan_state.canonical_artifact_probe = foreign_probe

    await service._initialize_canonical_artifact_boundary()

    assert isinstance(service._canonical_artifact_store, StoreFactory)
    assert service._canonical_callbacks_open is True
    assert service._canonical_sink_callback is old_sink
    assert service._canonical_artifact_probe_callback is owned_probe
    assert service._scan_state.canonical_sink is old_sink
    assert service._scan_state.canonical_artifact_probe is foreign_probe


def test_zero_recommendation_artifact_remains_valid_nonactionable():
    state = V15ScanState()
    v15_scan_service._restore_canonical_artifact(
        state,
        TRADE_DATE,
        _frozen(recommendations=0),
        RECEIVED_AT,
    )

    assert state.today_recommendation is None
    assert state.scan_error is None
    assert state.scan_done_date == ""
    assert state.canonical_durable_received_at == {TRADE_DATE: RECEIVED_AT_BEIJING}
    assert state.canonical_coordinator is None


def test_restoration_accepts_verified_canonical_and_frozen_shapes():
    canonical_state = V15ScanState()
    v15_scan_service._restore_canonical_artifact(
        canonical_state,
        TRADE_DATE,
        _canonical(),
        RECEIVED_AT,
    )
    assert canonical_state.today_recommendation is not None
    assert canonical_state.scan_done_date == ""
    assert canonical_state.canonical_coordinator is None

    frozen_state = V15ScanState()
    v15_scan_service._restore_canonical_artifact(
        frozen_state,
        TRADE_DATE,
        _frozen(),
        RECEIVED_AT,
    )
    assert frozen_state.today_recommendation == _frozen().legacy_recommendation
    assert frozen_state.scan_done_date == ""
    assert frozen_state.canonical_coordinator is None


@pytest.mark.asyncio
async def test_direct_deadline_call_without_observation_is_refused(monkeypatch):
    state = V15ScanState(today_recommendation={"stock_code": "prior"})
    alerts: list[tuple[str, str]] = []

    async def error(title: str, detail: str) -> None:
        alerts.append((title, detail))

    monkeypatch.setattr(v15_scan_service, "_notify_feishu_error", error)
    await v15_scan_service._fail_not_ready_deadline(state, TRADE_DATE, _at(10, 1))

    assert alerts == []
    assert state.today_recommendation == {"stock_code": "prior"}
    assert state.scan_error is None


@pytest.mark.asyncio
async def test_deadline_terminal_state_survives_notification_cancellation(monkeypatch):
    state = V15ScanState(today_recommendation={"stock_code": "prior"})

    async def cancel_notification(_title: str, _detail: str) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(v15_scan_service, "_notify_feishu_error", cancel_notification)
    evidence = v15_scan_service._CanonicalV16NotReadyEvidence(TRADE_DATE, _at(9, 59))
    with pytest.raises(asyncio.CancelledError):
        await v15_scan_service._fail_not_ready_deadline(
            state,
            TRADE_DATE,
            _at(10, 1),
            evidence,
        )

    assert state.today_recommendation is None
    assert "CanonicalV16NotReadyError" in state.scan_error
    assert state.scan_done_date == TRADE_DATE.isoformat()
    assert state.canonical_coordinator is not None
    assert state.canonical_coordinator.not_ready_alert_sent == {TRADE_DATE}
