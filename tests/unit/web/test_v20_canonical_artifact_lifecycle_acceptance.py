from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from types import MappingProxyType, SimpleNamespace
from typing import Any

import pytest

import src.web.v15_scan_service as scan_module
import src.web.v20_service as service_module
from src.data.database.v20_repository import V20RepositoryError, V20SemanticConflict, sha256_json
from src.strategy.v20.decision_engine import genesis_state
from src.web.v15_scan_service import CanonicalV16ScanBundle, _bundle_fingerprint
from src.web.v20_service import _DayContext
from tests.unit.web.test_v20_service import TZ, _bar, _bar_payload, _service

TRADE_DATE = date(2026, 8, 31)
CALENDAR = (TRADE_DATE, date(2026, 9, 1), date(2026, 9, 2))
CUTOFF = datetime.combine(TRADE_DATE, time(9, 40), tzinfo=TZ)
LABELS = ("09:25", "09:30", *(f"09:{minute:02d}" for minute in range(31, 40)))


class StartupRepository:
    def __init__(self) -> None:
        self.timeline: list[str] = []
        self.raw_barriers: set[date] = set()
        self.raw_completed_at: dict[date, datetime] = {}
        self.artifact_completed_at: dict[date, datetime] = {}
        self.hydrated_canonicals: dict[date, Any] = {}
        self.raw_rows: dict[date, list[dict[str, Any]]] = {}

    async def connect(self) -> None:
        self.timeline.append("repository-connected")

    async def acquire_runtime_leader(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def register_config(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def ensure_genesis_state(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    @property
    def compatible_entry_bindings(self) -> frozenset[Any]:
        return frozenset()

    async def close(self) -> None:
        self.timeline.append("repository-closed")

    async def record_minute_bars(self, rows: list[dict[str, Any]]) -> frozenset[str]:
        assert rows
        payload_rows = [dict(row) for row in rows]
        trade_dates = {
            datetime.fromisoformat(str(row["bar_end"])).astimezone(TZ).date()
            for row in payload_rows
        }
        assert len(trade_dates) == 1
        trade_date = trade_dates.pop()
        self.timeline.append("durable-raw")
        self.raw_rows.setdefault(trade_date, []).extend(payload_rows)
        raw_receipt = CUTOFF - timedelta(milliseconds=2)
        self.raw_barriers.add(trade_date)
        self.raw_completed_at[trade_date] = raw_receipt
        self.hydrated_canonicals.setdefault(trade_date, None)
        return frozenset(sha256_json(row) for row in payload_rows)

    def complete_raw_barrier(self, canonical: Any, received_at: datetime) -> None:
        self.raw_barriers.add(canonical.trade_date)
        self.raw_completed_at[canonical.trade_date] = received_at
        self.hydrated_canonicals[canonical.trade_date] = canonical

    def complete_artifact_barrier(self, trade_date: date) -> datetime:
        completed_at = CUTOFF - timedelta(milliseconds=1)
        self.artifact_completed_at[trade_date] = completed_at
        return completed_at


class FakeArtifactStore:
    instances: list["FakeArtifactStore"] = []

    def __init__(self, repository: Any) -> None:
        self.repository = repository
        self.save_calls: list[dict[str, Any]] = []
        self.load_calls: list[dict[str, Any]] = []
        self.calls: list[str] = []
        self.record: Any = None
        self.pending_record: Any = None
        self.instances.append(self)

    async def save_once(
        self,
        canonical: Any,
        *,
        official_stream_id: str,
        trade_date: date,
        event: str = "V16_CANONICAL_MASTER_V1",
    ) -> Any:
        if trade_date not in self.repository.raw_barriers:
            raise AssertionError("artifact save must wait for the durable raw barrier")
        portable = _portable_payload(canonical)
        self.repository.hydrated_canonicals[trade_date] = canonical
        self.calls.append("save_once")
        self.save_calls.append(
            {
                "canonical": canonical,
                "official_stream_id": official_stream_id,
                "trade_date": trade_date,
                "event": event,
            }
        )
        existing_payload = getattr(self.record, "payload", None)
        if existing_payload is not None and existing_payload != portable:
            raise V20SemanticConflict("canonical artifact slot collision")
        self.pending_record = SimpleNamespace(
            snapshot_id=f"canonical-artifact-{trade_date.isoformat()}",
            payload=portable,
            first_received_at=self.repository.raw_completed_at[trade_date],
        )
        return self.pending_record

    async def load(
        self,
        *,
        official_stream_id: str,
        trade_date: date,
        event: str = "V16_CANONICAL_MASTER_V1",
    ) -> Any:
        self.calls.append("load")
        self.load_calls.append(
            {
                "official_stream_id": official_stream_id,
                "trade_date": trade_date,
                "event": event,
            }
        )
        if self.record is None and self.pending_record is not None:
            self.record = self.pending_record
            self.pending_record = None
        return self.record

    async def hydrate(self, record: Any) -> Any:
        self.calls.append("hydrate")
        payload = record.payload
        assert isinstance(payload, dict)
        assert "early_bars" not in payload
        trade_date = date.fromisoformat(payload["trade_date"])
        if trade_date not in self.repository.raw_barriers:
            raise V20SemanticConflict("canonical artifact exists without its durable raw barrier")
        record.first_received_at = self.repository.complete_artifact_barrier(trade_date)
        return self.repository.hydrated_canonicals[trade_date]


def _portable_payload(canonical: Any) -> dict[str, Any]:
    recommendations = tuple(
        str(item.code) for item in getattr(canonical.scan_result, "recommended", ())
    )
    assert 0 <= len(recommendations) <= 10
    return {
        "schema_version": "v16-canonical-portable-artifact/v1",
        "trade_date": canonical.trade_date.isoformat(),
        "input_hash": canonical.input_hash,
        "model_sha256": canonical.model_sha256,
        "feature_list_sha256": canonical.feature_list_sha256,
        "recommended": recommendations,
        "raw_evidence_codes": tuple(sorted(canonical.early_bars)),
    }


def _install_durable_artifact(
    store: FakeArtifactStore,
    canonical: Any,
    *,
    received_at: datetime,
    include_raw: bool = True,
) -> None:
    if include_raw:
        store.repository.complete_raw_barrier(canonical, received_at)
    store.record = SimpleNamespace(
        snapshot_id=f"canonical-artifact-{canonical.trade_date.isoformat()}",
        payload=_portable_payload(canonical),
        first_received_at=received_at,
    )


def _canonical(*, recommendations: int = 10) -> CanonicalV16ScanBundle:
    codes = (
        "000001",
        "600000",
        "000002",
        "600001",
        "000003",
        "600002",
        "000004",
        "600003",
        "000005",
        "600004",
    )
    early_bars = {
        code: tuple(_bar(code, label, close=10 + index / 100) for index, label in enumerate(LABELS))
        for code in codes
    }
    recommended = tuple(
        SimpleNamespace(code=code, rank=index + 1, buy_price=10.0, score=0.9 - index * 0.01)
        for index, code in enumerate(codes[:recommendations])
    )
    result = CanonicalV16ScanBundle(
        trade_date=TRADE_DATE,
        scan_result=SimpleNamespace(recommended=recommended),
        stock_data={},
        clean_boards={},
        universe=codes,
        quotes={},
        prev_closes={"000001": 10.0, "600000": 10.0},
        history_raw={},
        early_bars=early_bars,
        early_source_hashes={
            code: sha256_json([_bar_payload(bar) for bar in bars])
            for code, bars in early_bars.items()
        },
        failed_no_prev_close=(),
        failed_no_history=(),
        failed_build=(),
        skipped_new_listings=(),
        model_sha256="a" * 64,
        feature_list_sha256="b" * 64,
        computed_at=CUTOFF - timedelta(minutes=1),
        input_hash="c" * 64,
        _integrity_hash="",
        prior_amount_yuan=MappingProxyType({}),
        history_date_valid_counts=MappingProxyType({}),
    )
    return replace(result, _integrity_hash=_bundle_fingerprint(result))


async def _start_service(
    monkeypatch: pytest.MonkeyPatch,
    repository: StartupRepository,
) -> Any:
    FakeArtifactStore.instances.clear()
    monkeypatch.setattr(
        service_module,
        "V16CanonicalArtifactStore",
        FakeArtifactStore,
        raising=False,
    )
    service = _service(monkeypatch, repository)
    service._routes = {
        service.config.route_id: SimpleNamespace(
            is_configured=lambda: True,
            destination_fingerprint=service.config.route_binding.destination_fingerprint,
            chat_id="chat",
            app_id="app",
            app_secret="secret",
        )
    }

    async def refresh_status() -> None:
        return None

    async def initialize_resources(scan_state: Any) -> None:
        scan_state.initialized = True

    async def idle_lane() -> None:
        await service._stop_event.wait()

    monkeypatch.setattr(service, "_refresh_status_snapshot", refresh_status)
    monkeypatch.setattr(service, "_initialize_resources", initialize_resources)
    for lane in (
        "_run_scheduler",
        "_run_live_exit_scheduler",
        "_run_stale_exit_scheduler",
        "_run_outbox_recovery_scheduler",
        "_run_publisher_scheduler",
        "_run_mews_cache_scheduler",
    ):
        monkeypatch.setattr(service, lane, idle_lane)
    await asyncio.wait_for(service.start(), timeout=2.0)
    return service


async def _stop_started_service(service: Any) -> None:
    service._stop_event.set()
    tasks, service._tasks = service._tasks, []
    await asyncio.gather(*tasks, return_exceptions=True)
    service._resources_started = False
    service._repository_started = False
    service._started = False


async def test_repo_ready_attaches_shared_canonical_sink_that_verifies_before_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = StartupRepository()
    service = await _start_service(monkeypatch, repository)
    try:
        assert service._repository_started is True
        assert service._scan_state.canonical_sink is not None
        assert FakeArtifactStore.instances
        store = FakeArtifactStore.instances[0]

        canonical = _canonical()
        await asyncio.wait_for(service._scan_state.canonical_sink(canonical), timeout=1.0)
        assert repository.timeline[-1] == "durable-raw"
        assert repository.raw_rows[TRADE_DATE]
        assert store.save_calls == [
            {
                "canonical": canonical,
                "official_stream_id": service.config.official_stream_id,
                "trade_date": TRADE_DATE,
                "event": "V16_CANONICAL_MASTER_V1",
            }
        ]
        assert store.calls == ["save_once", "load", "hydrate"]
        assert store.load_calls[0]["trade_date"] == TRADE_DATE
        assert repository.raw_completed_at[TRADE_DATE] == CUTOFF - timedelta(milliseconds=2)
        assert store.record.first_received_at == CUTOFF - timedelta(milliseconds=1)
        assert store.record.payload == _portable_payload(canonical)
        assert "early_bars" not in store.record.payload
        assert store.record.payload["raw_evidence_codes"] == tuple(sorted(canonical.early_bars))
    finally:
        await _stop_started_service(service)


@pytest.mark.parametrize("recommendations", [0, 1, 9])
async def test_real_raw_persistence_accepts_zero_one_and_sub_ten_recommendations(
    monkeypatch: pytest.MonkeyPatch,
    recommendations: int,
) -> None:
    repository = StartupRepository()
    service = await _start_service(monkeypatch, repository)
    try:
        canonical = _canonical(recommendations=recommendations)
        repository.timeline.clear()

        await asyncio.wait_for(service._persist_canonical_raw_minute_bars(canonical), timeout=1.0)

        assert repository.timeline == ["durable-raw"]
        expected_payloads = [
            _bar_payload(bar)
            for code in sorted(canonical.early_bars)
            for bar in canonical.early_bars[code]
        ]
        assert repository.raw_rows[TRADE_DATE] == expected_payloads
        assert len(expected_payloads) == len(canonical.early_bars) * len(LABELS)
        assert TRADE_DATE in repository.raw_barriers
    finally:
        await _stop_started_service(service)


async def test_entry_auto_consumes_durable_artifact_before_joining_canonical_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = StartupRepository()
    service = await _start_service(monkeypatch, repository)
    try:
        canonical = _canonical()
        assert FakeArtifactStore.instances, (
            "production did not construct the canonical artifact store"
        )
        store = FakeArtifactStore.instances[0]
        _install_durable_artifact(store, canonical, received_at=CUTOFF - timedelta(milliseconds=1))
        context = _DayContext(trade_date=TRADE_DATE, calendar=CALENDAR)
        service._context = context
        service._calendar_cache = CALENDAR
        service._calendar_loaded_for = TRADE_DATE
        service._mews_cached_for = TRADE_DATE
        projected = SimpleNamespace(
            snapshot_hash="projected",
            symbols=[{"code": "000001"}, {"code": "600000"}],
            metrics={"input_hash": canonical.input_hash},
        )

        async def persist_restart_raw(_candidate: Any) -> None:
            raise AssertionError("artifact hit must not rewrite durable raw from portable payload")

        async def coordinator_bomb(*_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("durable canonical artifact must be consumed first")

        def project(candidate: Any, *, calendar: Any = None) -> Any:
            assert candidate == canonical
            return projected

        monkeypatch.setattr(service_module, "get_or_compute_canonical_v16", coordinator_bomb)
        monkeypatch.setattr(service, "_persist_canonical_raw_minute_bars", persist_restart_raw)
        monkeypatch.setattr(service, "_project_canonical_v16", project)

        await asyncio.wait_for(
            service._run_entry_collection_cycle(
                context,
                datetime.combine(TRADE_DATE, time(9, 39), tzinfo=TZ),
            ),
            timeout=1.0,
        )

        assert FakeArtifactStore.instances[0].load_calls == [
            {
                "official_stream_id": service.config.official_stream_id,
                "trade_date": TRADE_DATE,
                "event": "V16_CANONICAL_MASTER_V1",
            }
        ]
        assert store.calls == ["load", "hydrate"]
        assert store.record.payload == _portable_payload(canonical)
        assert context.canonical_bundle is projected
    finally:
        await _stop_started_service(service)


async def test_same_semantic_artifact_reuses_slot_when_computed_at_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = StartupRepository()
    service = await _start_service(monkeypatch, repository)
    try:
        original = _canonical()
        recomputed = replace(original, computed_at=original.computed_at + timedelta(seconds=1))
        assert FakeArtifactStore.instances, (
            "production did not construct the canonical artifact store"
        )
        store = FakeArtifactStore.instances[0]
        _install_durable_artifact(store, original, received_at=CUTOFF - timedelta(milliseconds=1))
        repository.hydrated_canonicals[TRADE_DATE] = recomputed
        context = _DayContext(trade_date=TRADE_DATE, calendar=CALENDAR)
        service._context = context
        service._calendar_cache = CALENDAR
        service._calendar_loaded_for = TRADE_DATE
        service._mews_cached_for = TRADE_DATE
        projected = SimpleNamespace(
            snapshot_hash="projected",
            symbols=[{"code": item.code} for item in recomputed.scan_result.recommended],
            metrics={"input_hash": recomputed.input_hash},
        )

        async def coordinator_bomb(*_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("semantic-equivalent artifact must be reused")

        async def persist_restart_raw(_candidate: Any) -> None:
            raise AssertionError("artifact hit must not rewrite durable raw")

        def project(candidate: Any, *, calendar: Any = None) -> Any:
            assert candidate == recomputed
            return projected

        monkeypatch.setattr(service_module, "get_or_compute_canonical_v16", coordinator_bomb)
        monkeypatch.setattr(service, "_persist_canonical_raw_minute_bars", persist_restart_raw)
        monkeypatch.setattr(service, "_project_canonical_v16", project)

        await asyncio.wait_for(
            service._run_entry_collection_cycle(
                context,
                datetime.combine(TRADE_DATE, time(9, 39), tzinfo=TZ),
            ),
            timeout=1.0,
        )

        assert store.calls == ["load", "hydrate"]
        assert store.record.payload == _portable_payload(original)
        assert _portable_payload(recomputed) == _portable_payload(original)
        assert context.canonical_bundle is projected
    finally:
        await _stop_started_service(service)


@pytest.mark.parametrize(
    ("delta", "expected_mode"),
    [
        (timedelta(milliseconds=-1), "ACTIONABLE"),
        (timedelta(0), "CHECK_ONLY"),
        (timedelta(milliseconds=1), "CHECK_ONLY"),
    ],
)
async def test_canonical_receipt_boundary_controls_actionability_without_dropping_ticket(
    monkeypatch: pytest.MonkeyPatch,
    delta: timedelta,
    expected_mode: str,
) -> None:
    repository = StartupRepository()
    service = await _start_service(monkeypatch, repository)
    try:
        canonical = _canonical()
        assert FakeArtifactStore.instances, (
            "production did not construct the canonical artifact store"
        )
        _install_durable_artifact(
            FakeArtifactStore.instances[0],
            canonical,
            received_at=CUTOFF + delta,
        )
        context = _DayContext(trade_date=TRADE_DATE, calendar=CALENDAR)
        service._context = context
        service._calendar_cache = CALENDAR
        service._calendar_loaded_for = TRADE_DATE
        service._mews_cached_for = TRADE_DATE
        symbols = [{"code": "000001"}, {"code": "600000"}]
        projected = SimpleNamespace(
            snapshot_hash="projected", symbols=symbols, metrics={"input_hash": "c" * 64}
        )

        async def coordinator_bomb(*_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("receipt-boundary case must consume its durable artifact")

        async def persist(_candidate: Any) -> None:
            return None

        monkeypatch.setattr(service_module, "get_or_compute_canonical_v16", coordinator_bomb)
        monkeypatch.setattr(service, "_persist_canonical_raw_minute_bars", persist)
        monkeypatch.setattr(service, "_project_canonical_v16", lambda *_args, **_kwargs: projected)

        await asyncio.wait_for(
            service._run_entry_collection_cycle(
                context,
                datetime.combine(TRADE_DATE, time(9, 39), tzinfo=TZ),
            ),
            timeout=1.0,
        )

        assert context.canonical_entry_mode == expected_mode
        assert context.canonical_first_received_at == CUTOFF + delta
        assert context.canonical_bundle is projected
        assert context.canonical_bundle.symbols == symbols
        assert context.canonical_bundle.metrics == {"input_hash": "c" * 64}
    finally:
        await _stop_started_service(service)


async def test_artifact_without_durable_raw_barrier_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = StartupRepository()
    service = await _start_service(monkeypatch, repository)
    try:
        canonical = _canonical()
        assert FakeArtifactStore.instances, (
            "production did not construct the canonical artifact store"
        )
        store = FakeArtifactStore.instances[0]
        _install_durable_artifact(
            store,
            canonical,
            received_at=CUTOFF - timedelta(milliseconds=1),
            include_raw=False,
        )
        context = _DayContext(trade_date=TRADE_DATE, calendar=CALENDAR)
        service._context = context
        service._calendar_cache = CALENDAR
        service._calendar_loaded_for = TRADE_DATE
        service._mews_cached_for = TRADE_DATE

        async def coordinator_bomb(*_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("missing durable raw must fail before canonical compute")

        async def persist_missing_raw(_candidate: Any) -> None:
            raise AssertionError("portable artifact must never be rewritten as full raw")

        monkeypatch.setattr(service_module, "get_or_compute_canonical_v16", coordinator_bomb)
        monkeypatch.setattr(service, "_persist_canonical_raw_minute_bars", persist_missing_raw)

        with pytest.raises((V20RepositoryError, V20SemanticConflict)):
            await asyncio.wait_for(
                service._run_entry_collection_cycle(
                    context,
                    datetime.combine(TRADE_DATE, time(9, 39), tzinfo=TZ),
                ),
                timeout=1.0,
            )

        assert TRADE_DATE not in repository.raw_barriers
        assert context.canonical_bundle is None
        assert context.canonical_entry_mode is None
        assert store.calls == ["load", "hydrate"]
    finally:
        await _stop_started_service(service)


async def test_zero_recommendations_remain_legal_durable_no_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = StartupRepository()
    service = await _start_service(monkeypatch, repository)
    try:
        canonical = _canonical(recommendations=0)
        assert FakeArtifactStore.instances, (
            "production did not construct the canonical artifact store"
        )
        store = FakeArtifactStore.instances[0]
        receipt = CUTOFF - timedelta(milliseconds=1)
        _install_durable_artifact(store, canonical, received_at=receipt)
        context = _DayContext(trade_date=TRADE_DATE, calendar=CALENDAR)
        service._context = context
        service._calendar_cache = CALENDAR
        service._calendar_loaded_for = TRADE_DATE
        service._mews_cached_for = TRADE_DATE
        projected = SimpleNamespace(
            snapshot_hash="projected",
            symbols=[],
            metrics={"input_hash": canonical.input_hash},
        )

        async def coordinator_bomb(*_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("zero-ticket durable artifact must be consumed first")

        async def persist_restart_raw(_candidate: Any) -> None:
            raise AssertionError("artifact hit must not rewrite durable raw")

        def project(candidate: Any, *, calendar: Any = None) -> Any:
            assert candidate == canonical
            return projected

        monkeypatch.setattr(service_module, "get_or_compute_canonical_v16", coordinator_bomb)
        monkeypatch.setattr(service, "_persist_canonical_raw_minute_bars", persist_restart_raw)
        monkeypatch.setattr(service, "_project_canonical_v16", project)

        await asyncio.wait_for(
            service._run_entry_collection_cycle(
                context,
                datetime.combine(TRADE_DATE, time(9, 39), tzinfo=TZ),
            ),
            timeout=1.0,
        )

        assert store.record.payload["recommended"] == ()
        assert context.canonical_entry_mode == "ACTIONABLE"
        assert context.canonical_entry_action == "NO_SIGNAL"
        assert context.canonical_first_received_at == receipt
        assert context.canonical_bundle is projected
        assert context.canonical_bundle.symbols == []
        assert TRADE_DATE in repository.raw_barriers
    finally:
        await _stop_started_service(service)


class CutoffRepository:
    def __init__(self) -> None:
        self.service: Any = None
        self.status: Any = None
        self.state: Any = None
        self.events: dict[str, Any] = {}
        self.raw: dict[tuple[str, str], Any] = {}

    async def assert_runtime_leader(self) -> None:
        return None

    async def database_cutoff_reached(self, deadline: datetime) -> bool:
        return CUTOFF >= deadline

    async def get_entry_status(self, _stream: str, trade_date: date) -> Any:
        assert trade_date == TRADE_DATE
        return self.status

    async def get_outbox_event(self, event_id: str, **_kwargs: Any) -> Any:
        return self.events.get(event_id)

    async def load_state(self, _lineage: str) -> Any:
        if self.state is None:
            payload = genesis_state()
            self.state = SimpleNamespace(
                lineage_id=self.service.config.state_lineage_id,
                revision=0,
                state_hash=sha256_json(payload),
                payload=payload,
            )
        return self.state

    async def load_bootstrap_predecessor_trade_date(self, **_kwargs: Any) -> date:
        return date(2026, 8, 28)

    async def load_recent_completed(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []

    async def list_pending_shadow_batches(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []

    async def list_pending_shadow_reference_batches(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []

    async def list_pending_reference_legs(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []

    async def list_active_legs(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []

    async def list_raw_minute_bar_records(
        self, codes: Any, *, trade_date: date, end_labels: Any
    ) -> list[Any]:
        return []

    async def record_minute_bars(self, rows: list[Any]) -> frozenset[str]:
        for payload in rows:
            payload = dict(payload)
            key = (payload["stock_code"], payload["end_label"])
            self.raw[key] = SimpleNamespace(
                code=payload["stock_code"],
                bar_end=datetime.fromisoformat(payload["bar_end"]),
                end_label=payload["end_label"],
                source_hash=sha256_json(payload),
                payload=payload,
                first_received_at=datetime.combine(TRADE_DATE, time(9, 39), tzinfo=TZ),
            )
        return frozenset(sha256_json(row) for row in rows)

    async def commit_entry(self, commit: Any) -> None:
        self.status = SimpleNamespace(
            action="INPUT_INVALID",
            slot_status="FAILED",
            event_id=commit.event_id,
            semantic={"state_after_hash": self.state.state_hash},
        )

    async def commit_exit(self, _commit: Any) -> None:
        raise AssertionError("cutoff must not create exit facts")

    async def write_model_batch(self, _batch: Any) -> None:
        raise AssertionError("cutoff must not create model facts")

    async def enqueue_alert(
        self,
        event_id: str,
        _route_id: str,
        semantic: dict[str, Any],
        digest: str,
        **_scope: Any,
    ) -> bool:
        assert sha256_json(semantic) == digest
        self.events[event_id] = SimpleNamespace(
            event_id=event_id,
            event_type="DATA_ALERT",
            semantic=semantic,
            payload={"sealed": True},
        )
        return True

    async def seal_event(self, event_id: str, _builder: Any) -> Any:
        return self.events[event_id]

    async def enqueue_due_exit_reminders(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []


async def test_cutoff_action_waiter_cancellation_shields_canonical_master_and_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = CutoffRepository()
    service = _service(monkeypatch, repository)
    repository.service = service
    service._repository = repository
    context = _DayContext(trade_date=TRADE_DATE, calendar=CALENDAR)
    service._context = context
    service._calendar_cache = CALENDAR
    service._calendar_loaded_for = TRADE_DATE
    service._mews_cached_for = TRADE_DATE
    service._repository_started = True
    service._started = True
    service._stop_event.clear()
    now = datetime.combine(TRADE_DATE, time(9, 39, 59, 990000), tzinfo=TZ)
    service._clock = lambda: now
    compute_entered = asyncio.Event()
    release_compute = asyncio.Event()
    sink_entered = asyncio.Event()
    release_sink = asyncio.Event()
    calls = 0

    async def blocked_compute(_state: Any, requested: date, *_args: Any, **_kwargs: Any) -> Any:
        nonlocal calls
        assert requested == TRADE_DATE
        calls += 1
        compute_entered.set()
        await release_compute.wait()
        return _canonical()

    async def shielded_sink(_bundle: Any) -> None:
        sink_entered.set()
        await release_sink.wait()

    monkeypatch.setattr(scan_module, "compute_canonical_v16_scan", blocked_compute)
    monkeypatch.setattr(service_module, "compute_canonical_v16_scan", blocked_compute)
    monkeypatch.setattr(
        service_module,
        "derive_canonical_v16_universe",
        lambda _state: (
            None,
            None,
            {"board-a": (("000001", "bank"), ("600000", "bank"))},
            ("000001", "600000"),
        ),
    )
    service._scan_state.canonical_sink = shielded_sink

    watchdog = asyncio.create_task(service._run_decision_iteration_with_cutoff(now))
    await asyncio.wait_for(compute_entered.wait(), timeout=1.0)
    now = datetime.combine(TRADE_DATE, time(9, 40, 0, 10000), tzinfo=TZ)
    await asyncio.wait_for(watchdog, timeout=1.0)

    coordinator = service._scan_state.canonical_coordinator
    assert coordinator is not None
    master = coordinator.inflight[TRADE_DATE]
    assert master.cancelled() is False
    assert calls == 1
    release_compute.set()
    await asyncio.wait_for(sink_entered.wait(), timeout=1.0)
    release_sink.set()
    assert await asyncio.wait_for(master, timeout=1.0) == _canonical()


async def test_stop_drains_master_detaches_sink_then_closes_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = StartupRepository()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    service._resources_started = True
    service._scan_state.resource_owner = "V20"
    timeline: list[str] = []
    release = asyncio.Event()

    async def sink(_bundle: Any) -> None:
        timeline.append("sink-called")

    async def master() -> None:
        try:
            await release.wait()
        except asyncio.CancelledError:
            timeline.append("master-cancelled")
            raise

    service._scan_state.canonical_sink = sink
    service._scan_state.canonical_coordinator = scan_module._CanonicalV16Coordinator()
    service._scan_state.canonical_coordinator.inflight[TRADE_DATE] = asyncio.create_task(
        master(), name="canonical-master"
    )
    original_close = repository.close

    async def ordered_close() -> None:
        timeline.append("repository-closed")
        await original_close()

    repository.close = ordered_close

    await asyncio.wait_for(service.stop(), timeout=1.0)

    assert timeline == ["master-cancelled", "repository-closed"]
    assert repository.timeline == ["repository-closed"]
    assert service._scan_state.canonical_sink is None
    assert service._scan_state.canonical_coordinator is None
