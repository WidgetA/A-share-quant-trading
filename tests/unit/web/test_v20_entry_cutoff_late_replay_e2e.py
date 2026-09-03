from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import date, datetime, time

import pytest

from src.data.database.v20_repository import OutboxRecord, StateRecord, sha256_json
from src.strategy.v20.decision_engine import genesis_state
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
async def test_cutoff_waits_for_started_v20_calculation_and_never_schedules_replay(monkeypatch):
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
    cutoff_calls: list[datetime] = []

    async def blocked_run_once(
        sampled: datetime,
        *,
        include_exit_cycles: bool,
        include_outbox_recovery: bool,
    ) -> None:
        assert sampled < datetime.combine(TRADE_DATE, time(9, 40), tzinfo=TZ)
        assert include_exit_cycles is False
        assert include_outbox_recovery is False
        entered.set()
        await release.wait()

    async def record_cutoff(requested: date, *, now: datetime) -> bool:
        assert requested == TRADE_DATE
        cutoff_calls.append(now)
        return True

    monkeypatch.setattr(service, "run_once", blocked_run_once)
    monkeypatch.setattr(service, "_enforce_or_alert_entry_cutoff", record_cutoff)

    watchdog = asyncio.create_task(service._run_decision_iteration_with_cutoff(now))
    await asyncio.wait_for(entered.wait(), timeout=1.0)
    now = datetime.combine(TRADE_DATE, time(9, 40, 0, 10000), tzinfo=TZ)
    await asyncio.sleep(0)
    assert watchdog.done() is False
    assert service._late_0939_replay_task is None
    release.set()
    await asyncio.wait_for(watchdog, timeout=1.0)

    assert cutoff_calls == [now]
    assert service._late_0939_replay_task is None
    assert context.late_0939_replay_completed is False
    assert context.late_0939_replay_automatic_attempts == 0
    assert repository.raw_reads == []
    assert historical_client.calls == []
    assert repository.commit_entry_calls == []
    assert repository.commit_exit_calls == []
    assert repository.model_write_calls == []
    current = asyncio.current_task()
    orphans = [task for task in asyncio.all_tasks() if task is not current and not task.done()]
    assert orphans == []
