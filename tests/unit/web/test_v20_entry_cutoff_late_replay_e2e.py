from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import date, datetime, time

import pytest

from src.data.database.v20_repository import OutboxRecord, StateRecord, sha256_json
from src.strategy.v20.decision_engine import genesis_state
from src.web import v15_scan_service as scan_module
from src.web import v20_service as service_module
from src.web.v15_scan_service import CanonicalV16ScanBundle, _bundle_fingerprint
from src.web.v20_service import _DayContext
from tests.unit.web.test_v20_service import (
    TZ,
    _bar,
    _bar_payload,
    _late_replay_scan_result,
    _late_replay_status_and_state,
    _service,
)

TRADE_DATE = date(2026, 8, 31)
CALENDAR = (TRADE_DATE, date(2026, 9, 1), date(2026, 9, 2))
CODES = (
    "603068",
    "605299",
    "603990",
    "603232",
    "605098",
    "603193",
    "001238",
    "002368",
    "600486",
    "600557",
)
LABELS = ("09:25", "09:30", *(f"09:{minute:02d}" for minute in range(31, 40)))


class Repository:
    def __init__(self):
        self.status = None
        self.state = None
        self.service = None
        self.events = {}
        self.raw = {}
        self.cutoff_checks = []
        self.raw_reads = []
        self.commit_entry_calls = []
        self.commit_exit_calls = []
        self.model_write_calls = []

    async def assert_runtime_leader(self):
        return None

    async def database_cutoff_reached(self, deadline):
        self.cutoff_checks.append(deadline)
        return datetime.combine(TRADE_DATE, time(9, 40), tzinfo=TZ) >= deadline

    async def get_entry_status(self, _stream, trade_date):
        assert trade_date == TRADE_DATE
        return self.status

    async def get_outbox_event(self, event_id, **_kwargs):
        return self.events.get(event_id)

    async def load_state(self, _lineage):
        if self.state is None:
            payload = genesis_state()
            lineage = self.service.config.state_lineage_id
            self.state = StateRecord(lineage, 0, sha256_json(payload), payload)
        return self.state

    async def load_bootstrap_predecessor_trade_date(self, **_scope):
        return date(2026, 8, 28)

    async def load_recent_completed(self, *_args, **_kwargs):
        return []

    async def load_rolling7_market_health(self, *_args, **_kwargs):
        return ()

    async def list_pending_shadow_batches(self, *_args, **_kwargs):
        return []

    async def list_pending_shadow_reference_batches(self, *_args, **_kwargs):
        return []

    async def list_pending_reference_legs(self, *_args, **_kwargs):
        return []

    async def list_active_legs(self, *_args, **_kwargs):
        return []

    async def list_raw_minute_bar_records(self, codes, *, trade_date, end_labels):
        self.raw_reads.append((tuple(sorted(codes)), trade_date, tuple(end_labels)))
        return [
            row
            for (code, label), row in sorted(self.raw.items())
            if code in codes
            and label in end_labels
            and row.bar_end.astimezone(TZ).date() == trade_date
        ]

    async def record_minute_bars(self, rows):
        for payload in rows:
            payload = dict(payload)
            code = payload["stock_code"]
            label = payload["end_label"]
            received = datetime.combine(TRADE_DATE, time(9, 39), tzinfo=TZ)
            self.raw[(code, label)] = RawRecord(
                code, payload["bar_end"], label, sha256_json(payload), payload, received
            )
        return frozenset(sha256_json(row) for row in rows)

    async def commit_entry(self, commit):
        self.commit_entry_calls.append(commit)
        status, state = _late_replay_status_and_state(self.service)
        self.status = replace(status, event_id=commit.event_id)
        self.state = state
        self.events[commit.event_id] = OutboxRecord(
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

    async def commit_exit(self, commit):
        self.commit_exit_calls.append(commit)
        raise AssertionError

    async def write_model_batch(self, batch):
        self.model_write_calls.append(batch)
        raise AssertionError

    async def enqueue_alert(self, event_id, route_id, semantic, digest, **scope):
        assert sha256_json(semantic) == digest
        if event_id in self.events:
            return False
        self.events[event_id] = OutboxRecord(
            event_id=event_id,
            event_type="DATA_ALERT",
            route_id=route_id,
            official_stream_id=scope["official_stream_id"],
            lineage_id=scope["lineage_id"],
            semantic=semantic,
            semantic_content_hash=digest,
            payload=None,
            payload_hash=None,
            generated_at=None,
            commit_marker=None,
            action_expiry_ts=None,
            delivery_status="PENDING",
            attempt_count=0,
        )
        return True

    async def seal_event(self, event_id, builder):
        current = self.events[event_id]
        if current.payload is not None:
            return current
        generated = datetime.combine(TRADE_DATE, time(9, 40), tzinfo=TZ)
        payload = dict(builder(current, generated, 91, True))
        sealed = replace(
            current,
            payload=payload,
            payload_hash=sha256_json(payload),
            generated_at=generated,
            commit_marker=91,
        )
        self.events[event_id] = sealed
        return sealed

    async def enqueue_due_exit_reminders(self, *_args, **_kwargs):
        return []


class RawRecord:
    def __init__(self, code, bar_end, end_label, source_hash, payload, received):
        self.code = code
        self.bar_end = datetime.fromisoformat(bar_end)
        self.end_label = end_label
        self.source_hash = source_hash
        self.payload = payload
        self.first_received_at = received


class HistoricalClient:
    def __init__(self):
        self.calls = []

    async def batch_get_minute_history_for_date(self, codes, trade_date):
        self.calls.append((tuple(codes), trade_date))
        return {
            code: tuple(
                _bar(code, label, close=10 + index / 100, trade_date=trade_date)
                for index, label in enumerate(LABELS)
            )
            for code in codes
        }


def bundle(early_bars, computed_at):
    result = CanonicalV16ScanBundle(
        trade_date=TRADE_DATE,
        scan_result=_late_replay_scan_result(),
        stock_data={},
        clean_boards={},
        universe=CODES,
        quotes={},
        prev_closes={code: 10.0 for code in CODES},
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
        computed_at=computed_at,
        input_hash="c" * 64,
        _integrity_hash="",
    )
    return replace(result, _integrity_hash=_bundle_fingerprint(result))


@pytest.mark.asyncio
async def test_cutoff_shields_master_then_terminal_run_never_schedules_replay(monkeypatch):
    repository = Repository()
    historical_client = HistoricalClient()
    service = _service(monkeypatch, repository, historical_client)
    repository.service = service
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
    entered = asyncio.Event()
    release = asyncio.Event()
    state = {"master": 0, "replay": 0}
    attestation_calls = []

    async def master_compute(_state, requested, partial=None, **_kwargs):
        assert requested == TRADE_DATE
        assert not partial
        state["master"] += 1
        entered.set()
        await release.wait()
        bars = {
            code: tuple(
                _bar(code, label, close=10 + index / 100) for index, label in enumerate(LABELS)
            )
            for code in CODES
        }
        return bundle(bars, datetime.combine(TRADE_DATE, time(9, 40), tzinfo=TZ))

    async def replay_compute(_state, requested, partial=None, **kwargs):
        assert requested == TRADE_DATE
        assert partial is None
        assert kwargs["allow_realtime_fetch"] is False
        state["replay"] += 1
        seed = kwargs["early_data_seed"]
        return bundle({code: seed[code].early_bars for code in CODES}, service._aware_now())

    async def compute(_state, requested, partial=None, **kwargs):
        if kwargs.get("allow_realtime_fetch") is False:
            return await replay_compute(_state, requested, partial, **kwargs)
        return await master_compute(_state, requested, partial, **kwargs)

    monkeypatch.setattr(scan_module, "compute_canonical_v16_scan", compute)
    monkeypatch.setattr(service_module, "compute_canonical_v16_scan", replay_compute)
    monkeypatch.setattr(
        service_module,
        "derive_canonical_v16_universe",
        lambda _state: (
            None,
            None,
            {"board-a": tuple((code, f"name-{code}") for code in CODES)},
            CODES,
        ),
    )

    def daygate_attestation(_project_root, canonical, trade_date, current_date):
        attestation_calls.append((canonical, trade_date, current_date))
        assert canonical.trade_date == TRADE_DATE
        assert trade_date == TRADE_DATE
        return {
            "status": "PASS",
            "schema_version": "v16-day-gate-attestation/v1",
            "trade_date": TRADE_DATE.isoformat(),
            "evidence_content_sha256": "e" * 64,
            "frozen_at": "2026-08-31T09:39:00+08:00",
            "evaluated_at": "2026-08-31T09:39:00+08:00",
            "evidence_relative_path": "daygate/2026-08-31.json",
            "limitation": {
                "code": ("V16_DAY_GATE_EVIDENCE_ATTESTS_ORDERED_OUTPUT_NOT_FULL_READY_UNIVERSE"),
                "text": "ordered output attestation fixture",
            },
        }

    monkeypatch.setattr(
        service_module,
        "attest_post_cutoff_v16_day_gate",
        daygate_attestation,
    )

    watchdog = asyncio.create_task(service._run_decision_iteration_with_cutoff(now))
    await asyncio.wait_for(entered.wait(), timeout=1.0)
    now = datetime.combine(TRADE_DATE, time(9, 40, 0, 10000), tzinfo=TZ)
    await asyncio.wait_for(watchdog, timeout=1.0)
    assert repository.cutoff_checks[-1] == datetime.combine(TRADE_DATE, time(9, 40), tzinfo=TZ)
    assert context.entry_status is not None
    assert context.entry_status.action == "INPUT_INVALID"
    assert context.entry_status.slot_status == "FAILED"
    assert len(repository.commit_entry_calls) == 1
    assert repository.commit_entry_calls[0].action == "INPUT_INVALID"
    assert repository.commit_exit_calls == []
    assert repository.model_write_calls == []
    terminal_status = repository.status
    terminal_state = repository.state
    terminal_event_ids = set(repository.events)
    coordinator = service._scan_state.canonical_coordinator
    assert coordinator is not None
    master = coordinator.inflight[TRADE_DATE]
    assert not master.done()
    replay_probe = asyncio.create_task(
        service._run_decision_iteration_with_cutoff(
            datetime.combine(TRADE_DATE, time(9, 40), tzinfo=TZ)
        )
    )
    await asyncio.sleep(0)
    assert service._late_0939_replay_task is None
    release.set()
    completed = await asyncio.wait_for(master, timeout=1.0)
    assert state["master"] == 1
    assert coordinator.cache[TRADE_DATE] is completed
    await asyncio.wait_for(replay_probe, timeout=1.0)
    # A terminal slot is final for the automatic scheduler.  Completing the
    # shielded 09:39 master later must not schedule a retrospective selection,
    # read/backfill raw evidence, or create another public event.
    assert service._late_0939_replay_task is None
    assert context.late_0939_replay_completed is False
    assert context.late_0939_replay_automatic_attempts == 0
    assert state["replay"] == 0
    assert attestation_calls == []
    assert repository.raw_reads == []
    assert historical_client.calls == []
    replay_alerts = [
        event
        for event in repository.events.values()
        if event.event_type == "DATA_ALERT"
        and event.semantic.get("alert_code") == "LATE_0939_REPLAY_RESULT"
    ]
    assert replay_alerts == []
    assert repository.status == terminal_status
    assert repository.state == terminal_state
    assert set(repository.events) == terminal_event_ids
    assert len(repository.commit_entry_calls) == 1
    assert repository.commit_entry_calls[0].action == "INPUT_INVALID"
    assert repository.commit_exit_calls == []
    assert repository.model_write_calls == []
    current = asyncio.current_task()
    orphans = [task for task in asyncio.all_tasks() if task is not current and not task.done()]
    assert orphans == []
