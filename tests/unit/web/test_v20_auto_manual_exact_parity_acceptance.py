from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import date, datetime, time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pytest

from src.data.clients.tushare_realtime import TushareMinuteBar
from src.data.database.v16_canonical_artifact_store import (
    SNAPSHOT_TYPE,
    V16CanonicalArtifactRecord,
)
from src.data.database.v20_repository import (
    EntryCommit,
    EntryStatus,
    MinuteBarRecord,
    OutboxRecord,
    StateRecord,
    V20SemanticConflict,
    sha256_json,
)
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.decision_engine import genesis_state
from src.strategy.v20.runtime_config import load_v20_runtime_config
from src.web.v15_scan_service import (
    V15ScanState,
    _CanonicalV16Coordinator,
    get_or_compute_canonical_v16,
)
from src.web.v20_routes import _dispatch_manual_trigger
from src.web.v20_service import V20Service, _bar_payload
from src.web.v20_v16_canonical_artifact import encode
from tests.unit.web.test_v20_canonical_projection_acceptance import (
    ARTIFACT_CALENDAR,
    BREADTH_ONLY_CODE,
    CODES,
    FULL_EXCHANGE_CALENDAR,
    RAW_EVIDENCE_CODES,
    TRADE_DATE,
    _canonical,
    _rehash,
)

TZ = ZoneInfo("Asia/Shanghai")
PROJECT_ROOT = Path(__file__).resolve().parents[3]
FROZEN_AT = datetime(2026, 9, 1, 9, 39, 10, tzinfo=TZ)
RAW_RECEIVED_AT = datetime(2026, 9, 1, 9, 39, 15, tzinfo=TZ)
ARTIFACT_RECEIVED_AT = datetime(2026, 9, 1, 9, 39, 20, tzinfo=TZ)
RUN_AT = datetime(2026, 9, 1, 9, 39, 30, tzinfo=TZ)
POST_CUTOFF_AT = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)


class _NoRealtimeFallback:
    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"durable canonical decision tried realtime fallback: {name}")


class _NoMewsFetch:
    async def fetch_snapshot(self, **_kwargs: Any) -> Any:
        raise AssertionError("a cached MEWS day must not fetch during entry parity")


class _ImmutableArtifactReader:
    """Artifact boundary adapter; codec, hydration, and raw proof stay production code."""

    def __init__(
        self,
        record: V16CanonicalArtifactRecord | None,
        official_stream_id: str,
    ) -> None:
        self.record = record
        self.official_stream_id = official_stream_id
        self.load_calls: list[tuple[str, date, str]] = []
        self.save_calls: list[Mapping[str, Any]] = []

    async def save_once(
        self,
        canonical: Mapping[str, Any],
        *,
        official_stream_id: str,
        trade_date: date,
        event: str,
    ) -> V16CanonicalArtifactRecord:
        assert official_stream_id == self.official_stream_id
        assert trade_date == TRADE_DATE
        assert event == SNAPSHOT_TYPE
        payload = dict(canonical)
        self.save_calls.append(payload)
        candidate = _artifact_record(payload)
        if self.record is None:
            self.record = candidate
        else:
            assert self.record.payload == candidate.payload
            assert self.record.snapshot_hash == candidate.snapshot_hash
        return self.record

    async def load(
        self,
        *,
        official_stream_id: str,
        trade_date: date,
        event: str,
    ) -> V16CanonicalArtifactRecord | None:
        self.load_calls.append((official_stream_id, trade_date, event))
        assert official_stream_id == self.official_stream_id
        assert trade_date == TRADE_DATE
        assert event == SNAPSHOT_TYPE
        return self.record


class _DecisionRepository:
    """Deterministic transactional boundary for comparing the two real entry lanes."""

    def __init__(
        self,
        raw_records: tuple[MinuteBarRecord, ...],
        *,
        seal_at: datetime,
    ) -> None:
        state = genesis_state()
        self.state = StateRecord(
            lineage_id="unbound",
            revision=0,
            state_hash=sha256_json(state),
            payload=state,
        )
        self.raw_by_key = {(row.code, row.end_label): row for row in raw_records}
        self.raw_read_calls: list[tuple[str, ...]] = []
        self.seal_at = seal_at
        self.status: EntryStatus | None = None
        self.commit: EntryCommit | None = None
        self.outbox: OutboxRecord | None = None
        self.alerts: dict[str, OutboxRecord] = {}
        self.seal_calls = 0
        self.commit_entry_calls = 0
        self.alert_write_calls = 0
        self.raw_write_calls = 0
        self.forbidden_write_calls: list[str] = []

    def bind_lineage(self, lineage_id: str) -> None:
        self.state = replace(self.state, lineage_id=lineage_id)

    async def assert_runtime_leader(self) -> None:
        return None

    async def get_entry_status(
        self,
        official_stream_id: str,
        trade_date: date,
    ) -> EntryStatus | None:
        if self.status is not None:
            assert self.status.official_stream_id == official_stream_id
            assert self.status.trade_date == trade_date
        return self.status

    async def load_state(self, lineage_id: str) -> StateRecord:
        assert self.state.lineage_id == lineage_id
        return self.state

    async def load_bootstrap_predecessor_trade_date(self, **_scope: Any) -> date:
        return FULL_EXCHANGE_CALENDAR[0]

    async def list_raw_minute_bar_records(
        self,
        codes: tuple[str, ...],
        *,
        trade_date: date,
        end_labels: tuple[str, ...],
    ) -> tuple[MinuteBarRecord, ...]:
        requested_codes = tuple(codes)
        assert requested_codes in (tuple(sorted(CODES)), RAW_EVIDENCE_CODES)
        self.raw_read_calls.append(requested_codes)
        assert trade_date == TRADE_DATE
        assert "09:39" in end_labels
        selected = set(codes)
        labels = set(end_labels)
        return tuple(
            row
            for key, row in sorted(self.raw_by_key.items())
            if key[0] in selected and key[1] in labels
        )

    async def record_minute_bars(self, rows: list[Mapping[str, Any]]) -> frozenset[str]:
        self.raw_write_calls += 1
        hashes: set[str] = set()
        for row in rows:
            payload = dict(row)
            code = str(payload["stock_code"])
            label = str(payload["end_label"])
            bar_end = datetime.fromisoformat(str(payload["bar_end"]))
            digest = sha256_json(payload)
            hashes.add(digest)
            self.raw_by_key[(code, label)] = MinuteBarRecord(
                code=code,
                bar_end=bar_end,
                end_label=label,
                source_hash=digest,
                payload=payload,
                first_received_at=RAW_RECEIVED_AT,
            )
        return frozenset(hashes)

    async def load_recent_completed(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []

    async def load_rolling7_market_health(self, **_kwargs: Any) -> tuple[Any, ...]:
        return ()

    async def list_pending_shadow_batches(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []

    async def list_active_legs(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        return []

    async def commit_entry(self, commit: EntryCommit) -> None:
        self.commit_entry_calls += 1
        assert self.commit is None
        assert commit.expected_state_revision == self.state.revision
        assert commit.expected_state_hash == self.state.state_hash
        assert commit.next_state_hash == sha256_json(commit.next_state)
        self.commit = commit
        self.state = StateRecord(
            lineage_id=commit.lineage_id,
            revision=commit.expected_state_revision + 1,
            state_hash=commit.next_state_hash,
            payload=commit.next_state,
        )
        self.status = EntryStatus(
            official_stream_id=commit.official_stream_id,
            trade_date=commit.trade_date,
            slot_id=commit.slot_id,
            slot_status="COMPLETED",
            slot_revision=1,
            strategy_version=commit.strategy_version,
            config_id=commit.config_id,
            config_hash=commit.config_hash,
            lineage_id=commit.lineage_id,
            decision_id=commit.decision_id,
            event_id=commit.event_id,
            action=commit.action,
            final_multiplier=commit.final_multiplier,
            semantic_content_hash=commit.semantic_content_hash,
            semantic=commit.semantic,
            snapshot_id=commit.snapshot_id,
            snapshot_hash=commit.snapshot_hash,
            snapshot=commit.snapshot,
            action_expiry_ts=commit.action_expiry_ts,
        )
        self.outbox = OutboxRecord(
            event_id=commit.event_id,
            event_type="ENTRY_DECISION",
            route_id=commit.route_id,
            official_stream_id=commit.official_stream_id,
            lineage_id=commit.lineage_id,
            semantic=commit.semantic,
            semantic_content_hash=commit.semantic_content_hash,
            payload=None,
            payload_hash=None,
            generated_at=None,
            commit_marker=None,
            action_expiry_ts=commit.action_expiry_ts,
            delivery_status="PENDING",
            attempt_count=0,
        )

    async def enqueue_alert(
        self,
        event_id: str,
        route_id: str,
        semantic: Mapping[str, Any],
        semantic_hash: str,
        *,
        official_stream_id: str,
        lineage_id: str,
    ) -> bool:
        assert sha256_json(semantic) == semantic_hash
        existing = self.alerts.get(event_id)
        if existing is not None:
            assert existing.semantic == semantic
            assert existing.semantic_content_hash == semantic_hash
            return False
        self.alert_write_calls += 1
        self.alerts[event_id] = OutboxRecord(
            event_id=event_id,
            event_type="DATA_ALERT",
            route_id=route_id,
            official_stream_id=official_stream_id,
            lineage_id=lineage_id,
            semantic=dict(semantic),
            semantic_content_hash=semantic_hash,
            payload=None,
            payload_hash=None,
            generated_at=None,
            commit_marker=None,
            action_expiry_ts=None,
            delivery_status="PENDING",
            attempt_count=0,
        )
        return True

    async def seal_event(self, event_id: str, payload_builder: Any) -> OutboxRecord:
        current = (
            self.outbox
            if self.outbox is not None and self.outbox.event_id == event_id
            else self.alerts[event_id]
        )
        if current.payload is not None:
            return current
        self.seal_calls += 1
        payload = dict(payload_builder(current, self.seal_at, self.seal_calls, True))
        sealed = replace(
            current,
            payload=payload,
            payload_hash=sha256_json(payload),
            generated_at=self.seal_at,
            commit_marker=self.seal_calls,
        )
        if self.outbox is not None and self.outbox.event_id == event_id:
            self.outbox = sealed
        else:
            self.alerts[event_id] = sealed
        return sealed

    async def get_outbox_event(
        self,
        event_id: str,
        *,
        route_id: str,
        official_stream_id: str,
        lineage_id: str,
    ) -> OutboxRecord | None:
        current = (
            self.outbox
            if self.outbox is not None and self.outbox.event_id == event_id
            else self.alerts.get(event_id)
        )
        if current is None:
            return None
        assert (
            current.event_id,
            current.route_id,
            current.official_stream_id,
            current.lineage_id,
        ) == (event_id, route_id, official_stream_id, lineage_id)
        return current

    async def write_model_batch(self, *_args: Any, **_kwargs: Any) -> None:
        self.forbidden_write_calls.append("write_model_batch")
        raise AssertionError("check-only must not write model facts")

    async def commit_exit(self, *_args: Any, **_kwargs: Any) -> None:
        self.forbidden_write_calls.append("commit_exit")
        raise AssertionError("check-only must not write exit/order facts")


def _raw_bars() -> dict[str, tuple[TushareMinuteBar, ...]]:
    rows: dict[str, tuple[TushareMinuteBar, ...]] = {}
    for offset, code in enumerate(RAW_EVIDENCE_CODES):
        bar = TushareMinuteBar(
            stock_code=code,
            bar_end=datetime.combine(TRADE_DATE, time(9, 39), tzinfo=TZ),
            end_label="09:39",
            open_price=10.0,
            close_price=10.1 + offset / 10,
            high_price=10.5 + offset / 10,
            low_price=9.9,
            volume=1_000.0 + offset,
            amount=10_000.0 + offset,
        )
        rows[code] = (bar,)
    return rows


def _canonical_master() -> Any:
    early_bars = _raw_bars()
    canonical = replace(
        _canonical(),
        computed_at=FROZEN_AT,
        early_bars=early_bars,
        early_source_hashes={
            code: sha256_json([_bar_payload(bar) for bar in bars])
            for code, bars in early_bars.items()
        },
    )
    return _rehash(canonical)


def _raw_records() -> tuple[MinuteBarRecord, ...]:
    rows: list[MinuteBarRecord] = []
    for code, bars in sorted(_raw_bars().items()):
        (bar,) = bars
        payload = _bar_payload(bar)
        rows.append(
            MinuteBarRecord(
                code=code,
                bar_end=bar.bar_end,
                end_label=bar.end_label,
                source_hash=sha256_json(payload),
                payload=payload,
                first_received_at=RAW_RECEIVED_AT,
            )
        )
    return tuple(rows)


def _artifact_record(payload: Mapping[str, Any]) -> V16CanonicalArtifactRecord:
    portable = dict(payload)
    return V16CanonicalArtifactRecord(
        snapshot_id=sha256_json(
            ["V16_CANONICAL_ARTIFACT_SLOT_ID_V1", SNAPSHOT_TYPE, TRADE_DATE.isoformat()]
        ),
        snapshot_type=SNAPSHOT_TYPE,
        trade_date=TRADE_DATE,
        snapshot_hash=sha256_json(portable),
        first_received_at=ARTIFACT_RECEIVED_AT,
        _payload=portable,
    )


async def _calendar() -> tuple[date, ...]:
    return FULL_EXCHANGE_CALENDAR


async def _no_op(*_args: Any, **_kwargs: Any) -> None:
    return None


async def _maturity_complete(context: Any, *_args: Any, **_kwargs: Any) -> None:
    context.maturity_done = True


def _service_and_artifact(
    monkeypatch: pytest.MonkeyPatch,
    *,
    now: datetime = RUN_AT,
    artifact_hit: bool = True,
) -> tuple[V20Service, _DecisionRepository, _ImmutableArtifactReader]:
    # Runtime config loading verifies the manifest and retained non-code
    # strategy artifacts without authorizing implementation source bytes.
    monkeypatch.setenv("V20_MODE", "forward_shadow")
    monkeypatch.setenv("DB_SSLROOTCERT_SHA256", "c" * 64)
    monkeypatch.setenv("V20_INGEST_API_KEY", "i" * 32)
    monkeypatch.setenv("V20_STATUS_API_KEY", "s" * 32)
    monkeypatch.delenv("V20_ALLOW_PRODUCTION_PUSH", raising=False)
    config = replace(load_v20_runtime_config(PROJECT_ROOT), enabled=True)
    artifacts = load_g_artifacts(
        config.artifact_manifest_path.parent,
        expected_manifest_sha256=config.artifact_manifest_sha256,
    )
    repository = _DecisionRepository(_raw_records(), seal_at=now)
    repository.bind_lineage(config.state_lineage_id)
    service = V20Service(
        config=config,
        repository=repository,
        scan_state=V15ScanState(initialized=True, realtime_client=_NoRealtimeFallback()),
        artifacts=artifacts,
        publisher=SimpleNamespace(),
        routes={},
        clock=lambda: now,
        calendar_provider=_calendar,
        mews_source=_NoMewsFetch(),
    )
    canonical = _canonical_master()
    projected = service._project_canonical_v16(
        canonical,
        calendar=FULL_EXCHANGE_CALENDAR,
    )
    payload = encode(
        projected,
        calendar=ARTIFACT_CALENDAR,
        canonical_integrity_hash=canonical._integrity_hash,
    )
    record = _artifact_record(payload) if artifact_hit else None
    artifact_reader = _ImmutableArtifactReader(record, config.official_stream_id)
    service._canonical_artifact_store = artifact_reader
    service._canonical_callbacks_open = True
    service._scan_state.canonical_sink = service._persist_canonical_artifact_barrier
    service._repository_started = True
    service._started = True
    service._mews_cached_for = TRADE_DATE

    # Keep unrelated maturity/reference/reminder lanes out of this focused
    # acceptance.  Entry collection, portable hydration, raw-barrier proof,
    # policy preparation, commit, sealing, cutoff, and manual dispatch remain
    # their production implementations.
    monkeypatch.setattr(service, "_reconcile_missed_slots", _no_op)
    monkeypatch.setattr(service, "_expire_reference_gaps", _no_op)
    monkeypatch.setattr(service, "_process_mature_shadow", _maturity_complete)
    monkeypatch.setattr(service, "_run_reference_cycle", _no_op)
    monkeypatch.setattr(service, "_run_reminders", _no_op)

    async def ready() -> None:
        return None

    monkeypatch.setattr(service, "_require_manual_trigger_ready", ready)
    return service, repository, artifact_reader


@pytest.mark.asyncio
async def test_automatic_scheduler_and_manual_route_commit_exact_same_durable_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    automatic, automatic_repo, automatic_artifact = _service_and_artifact(monkeypatch)
    manual, manual_repo, manual_artifact = _service_and_artifact(monkeypatch)

    await automatic._run_decision_iteration_with_cutoff(RUN_AT)
    manual_result = await _dispatch_manual_trigger(manual, "manual-exact-parity-001")

    assert automatic_repo.commit is not None
    assert manual_repo.commit is not None
    assert automatic_repo.commit == manual_repo.commit
    assert automatic_repo.status == manual_repo.status
    assert automatic_repo.outbox is not None
    assert manual_repo.outbox is not None
    assert automatic_repo.outbox.semantic == manual_repo.outbox.semantic
    assert automatic_repo.outbox.payload == manual_repo.outbox.payload
    assert automatic_repo.outbox.payload_hash == manual_repo.outbox.payload_hash
    assert automatic_repo.seal_calls == manual_repo.seal_calls == 1

    expected_codes = [item["code"] for item in automatic_repo.commit.semantic["symbols"]]
    assert expected_codes == ["603068", "600000"]
    assert [item["code"] for item in manual_result["symbols"]] == expected_codes
    assert manual_result["entry_event_id"] == automatic_repo.commit.event_id
    assert manual_result["entry_action"] == automatic_repo.commit.action
    assert manual_result["formal_decision_available"] is True
    assert manual_result["exact_automatic_message"] is True
    assert manual_result["official_state_changed"] is True
    assert manual_result["orders_changed"] is False

    expected_load = (
        automatic.config.official_stream_id,
        TRADE_DATE,
        SNAPSHOT_TYPE,
    )
    assert automatic_artifact.load_calls
    assert manual_artifact.load_calls
    assert set(automatic_artifact.load_calls) == {expected_load}
    assert set(manual_artifact.load_calls) == {expected_load}

    # Drain the non-blocking MEWS kick owned by the manual route.  It must not
    # affect either decision, and no service-owned task may leak from the test.
    if manual._mews_trigger_tasks:
        await asyncio.gather(*tuple(manual._mews_trigger_tasks))
    await asyncio.sleep(0)
    assert manual._mews_trigger_tasks == set()


@pytest.mark.asyncio
async def test_check_only_artifact_hit_is_read_only_and_binds_exact_automatic_prepare(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    automatic, automatic_repo, _automatic_artifact = _service_and_artifact(monkeypatch)
    check_only, check_repo, check_artifact = _service_and_artifact(
        monkeypatch,
        now=POST_CUTOFF_AT,
    )
    await automatic._run_decision_iteration_with_cutoff(RUN_AT)
    assert automatic_repo.commit is not None

    state_before = check_repo.state
    first = await check_only.trigger_canonical_selection_check_only(
        "check-only-artifact-hit-001",
        POST_CUTOFF_AT,
    )
    load_calls_after_first = tuple(check_artifact.load_calls)
    second = await check_only.trigger_canonical_selection_check_only(
        "check-only-artifact-hit-001",
        POST_CUTOFF_AT,
    )

    assert first["created"] is True
    assert second == {**first, "created": False}
    assert first["formal_decision_available"] is False
    assert first["non_actionable"] is True
    assert first["retrospective_expired"] is True
    assert first["official_state_changed"] is False
    assert first["orders_changed"] is False
    assert first["exact_automatic_message"] is False

    alert = check_repo.alerts[first["operator_event_id"]]
    embedded = dict(alert.semantic["entry_render_semantic"])
    automatic_semantic = dict(automatic_repo.commit.semantic)
    assert set(embedded) == set(automatic_semantic)
    assert {**embedded, "event_id": automatic_semantic["event_id"]} == automatic_semantic
    assert alert.semantic["v20_action"] == automatic_repo.commit.action
    assert alert.semantic["final_multiplier"] == automatic_repo.commit.final_multiplier
    assert alert.semantic["symbols"] == automatic_semantic["symbols"]
    assert first["symbols"] == automatic_semantic["symbols"]
    assert alert.payload is not None
    assert alert.payload_hash == sha256_json(alert.payload)
    assert alert.payload["semantic_content_hash"] == alert.semantic_content_hash
    assert all(item["code"] in alert.payload["message"] for item in first["symbols"])

    assert check_repo.state == state_before
    assert check_repo.status is None
    assert check_repo.commit is None
    assert check_repo.commit_entry_calls == 0
    assert check_repo.forbidden_write_calls == []
    assert check_repo.alert_write_calls == 1
    assert check_repo.seal_calls == 1
    assert check_repo.raw_write_calls == 0
    assert check_artifact.save_calls == []
    assert tuple(check_artifact.load_calls) == load_calls_after_first


@pytest.mark.asyncio
async def test_check_only_terminal_compares_old_official_and_current_canonical_hashes_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    automatic, automatic_repo, _automatic_artifact = _service_and_artifact(monkeypatch)
    check_only, check_repo, check_artifact = _service_and_artifact(
        monkeypatch,
        now=POST_CUTOFF_AT,
    )
    await automatic._run_decision_iteration_with_cutoff(RUN_AT)
    assert automatic_repo.status is not None

    # Model a terminal written by the previous deployment: its official V16
    # identity is internally sound but deliberately differs from the durable
    # artifact consumed by the currently deployed code.
    old_v16_hash = "0" * 64
    old_semantic = {
        **dict(automatic_repo.status.semantic),
        "v16_snapshot_hash": old_v16_hash,
    }
    old_snapshot = {
        **dict(automatic_repo.status.snapshot),
        "v16_snapshot_hash": old_v16_hash,
    }
    check_repo.status = replace(
        automatic_repo.status,
        semantic=old_semantic,
        semantic_content_hash=sha256_json(old_semantic),
        snapshot=old_snapshot,
        snapshot_hash=sha256_json(old_snapshot),
    )
    check_repo.state = automatic_repo.state
    state_before = check_repo.state
    status_before = check_repo.status

    result = await check_only.trigger_canonical_selection_check_only(
        "check-only-terminal-version-diff-001",
        POST_CUTOFF_AT,
    )

    assert result["current_version_recomputed"] is True
    assert result["official_entry_action"] == status_before.action
    assert result["official_entry_event_id"] == status_before.event_id
    assert result["official_v16_snapshot_hash"] == old_v16_hash
    assert (
        result["current_v16_snapshot_hash"] == (check_artifact.record.payload["v20_snapshot_hash"])
    )
    assert result["current_v16_snapshot_hash"] != result["official_v16_snapshot_hash"]
    assert result["official_state_changed"] is False
    assert result["orders_changed"] is False
    assert result["non_actionable"] is True
    assert check_repo.state == state_before
    assert check_repo.status == status_before
    assert check_repo.commit is None
    assert check_repo.commit_entry_calls == 0
    assert check_repo.forbidden_write_calls == []
    assert check_repo.alert_write_calls == 1


@pytest.mark.asyncio
async def test_check_only_artifact_miss_joins_one_shared_master_then_consumes_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, artifact = _service_and_artifact(
        monkeypatch,
        now=POST_CUTOFF_AT,
        artifact_hit=False,
    )
    state_before = repository.state
    canonical = _canonical_master()
    coordinator = _CanonicalV16Coordinator()
    coordinator.pending_persist[TRADE_DATE] = canonical
    service._scan_state.canonical_coordinator = coordinator
    sink_entered = asyncio.Event()
    release_sink = asyncio.Event()
    sink_calls = 0
    production_sink = service._persist_canonical_artifact_barrier

    async def gated_production_sink(bundle: Any) -> None:
        nonlocal sink_calls
        sink_calls += 1
        sink_entered.set()
        await release_sink.wait()
        await production_sink(bundle)

    service._scan_state.canonical_sink = gated_production_sink
    owner_waiter = asyncio.create_task(
        get_or_compute_canonical_v16(service._scan_state, TRADE_DATE),
        name="canonical-owner-waiter",
    )
    await asyncio.wait_for(sink_entered.wait(), timeout=1)
    master = coordinator.inflight[TRADE_DATE]

    check_waiter = asyncio.create_task(
        service.trigger_canonical_selection_check_only(
            "check-only-artifact-miss-001",
            POST_CUTOFF_AT,
        ),
        name="canonical-check-only-waiter",
    )
    for _ in range(100):
        if artifact.load_calls:
            break
        await asyncio.sleep(0)
    else:
        raise AssertionError("check-only path did not probe the missing artifact")

    assert coordinator.inflight[TRADE_DATE] is master
    assert master.cancelled() is False
    assert check_waiter.done() is False
    assert artifact.record is None
    release_sink.set()

    owner_result, check_result = await asyncio.wait_for(
        asyncio.gather(owner_waiter, check_waiter),
        timeout=2,
    )
    await asyncio.sleep(0)

    assert owner_result.trade_date == canonical.trade_date
    assert owner_result._integrity_hash == canonical._integrity_hash
    assert check_result["created"] is True
    assert check_result["non_actionable"] is True
    assert sink_calls == 1
    assert len(artifact.save_calls) == 1
    assert artifact.record is not None
    assert repository.raw_write_calls == 1
    assert coordinator.inflight == {}
    assert coordinator.cache[TRADE_DATE]._integrity_hash == canonical._integrity_hash

    alert = repository.alerts[check_result["operator_event_id"]]
    assert check_result["symbols"] == alert.semantic["entry_render_semantic"]["symbols"]
    assert alert.semantic["symbols"] == alert.semantic["entry_render_semantic"]["symbols"]
    assert repository.state == state_before
    assert repository.status is None
    assert repository.commit is None
    assert repository.commit_entry_calls == 0
    assert repository.forbidden_write_calls == []
    assert repository.alert_write_calls == 1

    # Cutoff-waiter shielding and service-stop draining of this same master are
    # asserted at the lifecycle boundary, where cancellation ordering is owned.


@pytest.mark.asyncio
async def test_artifact_hydration_requires_breadth_only_raw_evidence_union(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, artifact = _service_and_artifact(monkeypatch)
    record = artifact.record
    assert record is not None

    # The portable ticket names the complete raw universe, including a symbol
    # used only by market breadth and absent from the V16 scan-input universe.
    hydrated = await service._hydrate_canonical_artifact_record(record)
    assert tuple(hydrated.snapshot["raw_evidence_codes"]) == RAW_EVIDENCE_CODES
    assert BREADTH_ONLY_CODE not in hydrated.snapshot["scan_input_codes"]
    assert (BREADTH_ONLY_CODE, "09:39") in repository.raw_by_key
    assert repository.raw_read_calls == [RAW_EVIDENCE_CODES]

    breadth_key = (BREADTH_ONLY_CODE, "09:39")
    breadth_record = repository.raw_by_key.pop(breadth_key)
    with pytest.raises(
        V20SemanticConflict,
        match="complete durable raw barrier",
    ):
        await service._hydrate_canonical_artifact_record(record)
    repository.raw_by_key[breadth_key] = breadth_record

    original_list = repository.list_raw_minute_bar_records

    async def list_with_breadth_conflict(
        codes: tuple[str, ...],
        *,
        trade_date: date,
        end_labels: tuple[str, ...],
    ) -> tuple[MinuteBarRecord, ...]:
        rows = await original_list(codes, trade_date=trade_date, end_labels=end_labels)
        conflicting_payload = {**dict(breadth_record.payload), "close": 999.0}
        conflicting = replace(
            breadth_record,
            source_hash=sha256_json(conflicting_payload),
            payload=conflicting_payload,
        )
        return (*rows, conflicting)

    monkeypatch.setattr(repository, "list_raw_minute_bar_records", list_with_breadth_conflict)
    with pytest.raises(
        V20SemanticConflict,
        match="complete durable raw barrier",
    ):
        await service._hydrate_canonical_artifact_record(record)
