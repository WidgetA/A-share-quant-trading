"""Executable acceptance contract for theoretical V20 Rolling7 market health.

These tests intentionally cross the canonical-artifact, durable fact, policy,
entry-decision, and production formatter boundaries.  Rolling7 describes the
market, never the account or whether an operator actually bought anything.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import textwrap
from datetime import date, datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

import pytest

import src.web.v20_service as service_module
from src.common.v20_feishu import seal_v20_payload
from src.data.clients.tushare_realtime import (
    TushareDailyBar,
    TushareMinuteBar,
    TushareRealtimeClient,
)
from src.data.database.v20_repository import (
    OutboxRecord,
    StateRecord,
    V20Repository,
    V20SemanticConflict,
    sha256_json,
)
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.decision_engine import (
    ActiveRollingGap,
    CompletedRolling,
    genesis_state,
    prepare_entry,
    prepare_invalid_entry,
)
from src.strategy.v20.models import Rolling7Status, RollingBatch
from src.strategy.v20.policy import evaluate_rolling7
from src.strategy.v20.rolling7_market_health import (
    BatchStatus,
    CanonicalRecommendation,
    Rolling7Batch,
    SignalKind,
    make_batch,
    make_missing_canonical_batch,
)
from src.strategy.v20.runtime_config import load_v20_runtime_config
from src.web.v20_canonical_selection import CanonicalV16ScanBundle
from src.web.v20_service import SHANGHAI, V20Service, _bar_payload
from tests.unit.web.test_v20_canonical_projection_acceptance import _canonical
from tests.unit.web.test_v20_v16_canonical_artifact import CALENDAR as ENTRY_CALENDAR
from tests.unit.web.test_v20_v16_canonical_artifact import _bundle

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TOP10 = tuple(f"{index:06d}" for index in range(1, 11))
SNAPSHOT_ID = "a" * 64
SNAPSHOT_HASH = "b" * 64
FORBIDDEN_LEDGER_CALLS = {
    "list_orders",
    "get_fills",
    "get_positions",
    "load_model_legs",
    "list_exit_lineages",
    "load_stream_lineage",
}
NINE_FROZEN_CANONICAL_INPUTS = {
    "universe_override",
    "clean_boards_override",
    "prev_closes_override",
    "history_raw_override",
    "names_override",
    "calendar_override",
    "prior_daily_override",
    "st_eligible_codes_override",
    "early_data_seed",
}


def _recommendations(codes: Sequence[str] = TOP10) -> tuple[CanonicalRecommendation, ...]:
    return tuple(
        CanonicalRecommendation(rank=rank, code=code) for rank, code in enumerate(codes, start=1)
    )


def _artifact_record(
    signal_date: date,
    *,
    recommendations: Sequence[CanonicalRecommendation] | None = None,
    t2_date: date | None = None,
    identity: str | None = None,
) -> SimpleNamespace:
    selected = _recommendations() if recommendations is None else tuple(recommendations)
    t2 = t2_date or signal_date + timedelta(days=2)
    calendar = (signal_date, signal_date + timedelta(days=1), t2)
    return SimpleNamespace(
        trade_date=signal_date,
        snapshot_id=identity or sha256_json({"rolling7-signal-date": signal_date.isoformat()}),
        snapshot_hash=SNAPSHOT_HASH,
        first_received_at=datetime.combine(signal_date, time(16, 0), tzinfo=SHANGHAI),
        payload={
            "canonical_integrity_hash": "c" * 64,
            "calendar": [item.isoformat() for item in calendar],
            "v20_snapshot": {
                "symbols": [{"rank": item.rank, "code": item.code} for item in selected]
            },
        },
    )


def _minute_bar(
    code: str,
    trade_date: date,
    *,
    label: str = "09:41",
    open_price: float = 100.0,
) -> TushareMinuteBar:
    bar_end = datetime.combine(trade_date, time.fromisoformat(label), tzinfo=SHANGHAI)
    return TushareMinuteBar(
        stock_code=code,
        bar_end=bar_end,
        end_label=label,
        open_price=open_price,
        high_price=max(open_price, open_price + 1.0),
        low_price=min(open_price, open_price - 1.0),
        close_price=open_price + 0.25,
        volume=1_000.0,
        amount=100_000.0,
    )


def _complete_batch(
    signal_date: date,
    batch_return: float,
    *,
    identity_index: int,
) -> Rolling7Batch:
    reference = {code: 100.0 for code in TOP10}
    close = {code: 100.0 * (1.0 + batch_return) for code in TOP10}
    return make_batch(
        signal_date=signal_date,
        canonical_snapshot_id=f"{identity_index:064x}",
        canonical_snapshot_hash=f"{identity_index + 100:064x}",
        recommendations=_recommendations(),
        t2_date=signal_date + timedelta(days=2),
        d0_references=reference,
        d2_closes=close,
    )


def _evidence(batch: Rolling7Batch) -> tuple[dict[str, float], dict[str, float]]:
    return (
        {leg.code: float(leg.d0_reference) for leg in batch.legs if leg.d0_reference is not None},
        {leg.code: float(leg.d2_close) for leg in batch.legs if leg.d2_close is not None},
    )


class StrictRollingRepository:
    """Stateful seam matching the repository's one-way Rolling7 contract."""

    def __init__(self, *, now: datetime) -> None:
        self.now = now
        self.facts: dict[date, Rolling7Batch] = {}
        self.raw_records: list[SimpleNamespace] = []
        self.daily_snapshots: list[SimpleNamespace] = []
        self.calls: list[tuple[str, Any]] = []

    def __getattr__(self, name: str) -> Any:
        if name in FORBIDDEN_LEDGER_CALLS:
            raise AssertionError(f"Rolling7 attempted forbidden account API {name}")
        raise AttributeError(name)

    async def save_rolling7_market_health(
        self,
        batch: Rolling7Batch,
        *,
        updated_at: datetime,
    ) -> SimpleNamespace:
        self.calls.append(("save_rolling7_market_health", batch.signal_date))
        existing = self.facts.get(batch.signal_date)
        if existing is None:
            self.facts[batch.signal_date] = batch
            return SimpleNamespace(batch=batch, updated_at=updated_at)
        if existing == batch:
            return SimpleNamespace(batch=existing, updated_at=updated_at)
        if existing.status is BatchStatus.COMPLETE:
            raise V20SemanticConflict("COMPLETE Rolling7 fact is immutable")
        if not existing.canonical_available:
            if not batch.canonical_available:
                allowed = existing.t2_date is None and batch.t2_date is not None
                if not allowed:
                    raise V20SemanticConflict("missing canonical placeholder changed")
            self.facts[batch.signal_date] = batch
            return SimpleNamespace(batch=batch, updated_at=updated_at)

        identity_before = (
            existing.canonical_snapshot_id,
            existing.canonical_snapshot_hash,
            existing.signal_kind,
            existing.recommendations,
        )
        identity_after = (
            batch.canonical_snapshot_id,
            batch.canonical_snapshot_hash,
            batch.signal_kind,
            batch.recommendations,
        )
        if identity_before != identity_after:
            raise V20SemanticConflict("known canonical identity changed")
        if existing.t2_date not in (None, batch.t2_date):
            raise V20SemanticConflict("established T2 changed")
        old_d0, old_d2 = _evidence(existing)
        new_d0, new_d2 = _evidence(batch)
        if any(new_d0.get(code) != value for code, value in old_d0.items()):
            raise V20SemanticConflict("D0 evidence was removed or changed")
        if any(new_d2.get(code) != value for code, value in old_d2.items()):
            raise V20SemanticConflict("D2 evidence was removed or changed")
        progressed = (
            (existing.t2_date is None and batch.t2_date is not None)
            or len(new_d0) > len(old_d0)
            or len(new_d2) > len(old_d2)
        )
        if batch.status is BatchStatus.DATA_GAP and not progressed:
            raise V20SemanticConflict("DATA_GAP replay did not add evidence")
        self.facts[batch.signal_date] = batch
        return SimpleNamespace(batch=batch, updated_at=updated_at)

    async def load_rolling7_market_health(
        self,
        *,
        before_t2: date,
        limit: int = 1_000,
    ) -> tuple[Rolling7Batch, ...]:
        self.calls.append(("load_rolling7_market_health", before_t2))
        rows = sorted(
            (
                row
                for row in self.facts.values()
                if row.t2_date is not None and row.t2_date < before_t2
            ),
            key=lambda row: row.signal_date,
        )
        return tuple(rows[:limit])

    async def get_rolling7_market_health_for_date(
        self,
        signal_date: date,
    ) -> Rolling7Batch | None:
        self.calls.append(("get_rolling7_market_health_for_date", signal_date))
        return self.facts.get(signal_date)

    async def load_recent_completed(self, kind: str, **kwargs: Any) -> tuple[Any, ...]:
        self.calls.append(("load_recent_completed", (kind, kwargs)))
        return ()

    async def list_raw_minute_bar_records(
        self,
        codes: Sequence[str],
        *,
        trade_date: date,
        end_labels: Sequence[str],
        received_before: datetime | None = None,
    ) -> list[SimpleNamespace]:
        code_set = set(codes)
        label_set = set(end_labels)
        return [
            row
            for row in self.raw_records
            if row.payload["stock_code"] in code_set
            and row.payload["end_label"] in label_set
            and datetime.fromisoformat(row.payload["bar_end"]).astimezone(SHANGHAI).date()
            == trade_date
            and (received_before is None or row.first_received_at <= received_before)
        ]

    async def record_minute_bars(self, payloads: Sequence[Mapping[str, Any]]) -> frozenset[str]:
        sealed: set[str] = set()
        for payload in payloads:
            payload_copy = dict(payload)
            digest = sha256_json(payload_copy)
            if all(sha256_json(row.payload) != digest for row in self.raw_records):
                self.raw_records.append(
                    SimpleNamespace(
                        payload=payload_copy,
                        first_received_at=self.now,
                    )
                )
            sealed.add(digest)
        return frozenset(sealed)

    async def list_daily_bar_snapshots(
        self,
        trade_date: date,
        *,
        received_before: datetime | None = None,
    ) -> tuple[list[SimpleNamespace], tuple[Any, ...]]:
        return (
            [
                row
                for row in self.daily_snapshots
                if row.trade_date == trade_date
                and (received_before is None or row.first_received_at <= received_before)
            ],
            (),
        )

    async def record_daily_bar_snapshot(
        self,
        trade_date: date,
        payload: Mapping[str, Any],
    ) -> SimpleNamespace:
        record = SimpleNamespace(
            snapshot_id=f"daily-{len(self.daily_snapshots)}",
            trade_date=trade_date,
            source_hash=sha256_json(payload),
            payload=dict(payload),
            first_received_at=self.now,
            receipt_sequence=len(self.daily_snapshots),
        )
        self.daily_snapshots.append(record)
        return record


class ArtifactStore:
    def __init__(self, records: Sequence[SimpleNamespace] = ()) -> None:
        self.records = {record.trade_date: record for record in records}
        self.save_calls: list[date] = []

    async def load(self, *, trade_date: date, **_kwargs: Any) -> SimpleNamespace | None:
        return self.records.get(trade_date)

    async def save_once(
        self,
        payload: Mapping[str, Any],
        *,
        trade_date: date,
        **_kwargs: Any,
    ) -> None:
        if trade_date in self.records:
            raise V20SemanticConflict("canonical artifact changed")
        self.save_calls.append(trade_date)
        self.records[trade_date] = SimpleNamespace(
            trade_date=trade_date,
            snapshot_id=sha256_json({"date": trade_date.isoformat()}),
            snapshot_hash=sha256_json(payload),
            payload=dict(payload),
            first_received_at=datetime.combine(trade_date, time(16), tzinfo=SHANGHAI),
        )


class HistoricalMarketClient:
    def __init__(self, *, trace: list[tuple[str, date]] | None = None) -> None:
        self.trace = trace if trace is not None else []
        self.history_calls: list[tuple[tuple[str, ...], date]] = []
        self.daily_calls: list[date] = []
        self.changed = False

    async def batch_get_minute_history_for_date(
        self,
        codes: Sequence[str],
        trade_date: date,
    ) -> dict[str, tuple[TushareMinuteBar, ...]]:
        ordered = tuple(codes)
        self.history_calls.append((ordered, trade_date))
        self.trace.append(("history", trade_date))
        exact_open = 999.0 if self.changed else 100.0
        return {
            code: (
                _minute_bar(code, trade_date, label="09:40", open_price=7.0),
                _minute_bar(
                    code,
                    trade_date - timedelta(days=1),
                    label="09:41",
                    open_price=8.0,
                ),
                _minute_bar(code, trade_date, label="09:41", open_price=exact_open),
            )
            for code in ordered
        }

    async def fetch_daily_bars(self, trade_date_text: str) -> dict[str, TushareDailyBar]:
        trade_date = datetime.strptime(trade_date_text, "%Y%m%d").date()
        self.daily_calls.append(trade_date)
        self.trace.append(("daily", trade_date))
        close = 50.0 if self.changed else 110.0
        return {
            code: TushareDailyBar(
                stock_code=code,
                trade_date=trade_date_text,
                close_price=close,
                amount_yuan=1_000_000.0,
            )
            for code in TOP10
        }

    async def batch_get_early_market_data(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("historical bootstrap touched current-day RT early data")

    async def batch_get_minute_history(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("historical bootstrap touched current-day rt_min_daily")

    async def batch_get_latest_minute_bars(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("historical bootstrap touched current-day rt_min")


def _service(
    repository: StrictRollingRepository,
    store: ArtifactStore,
    *,
    now: datetime,
    calendar: Sequence[date],
    client: Any = None,
) -> V20Service:
    instance = V20Service.__new__(V20Service)
    instance.config = SimpleNamespace(
        official_stream_id="rolling-official",
        state_lineage_id="rolling-lineage",
        clock=SimpleNamespace(reference_bar_label="09:41"),
    )
    instance._repository = repository
    instance._canonical_artifact_store = store
    instance._scan_state = SimpleNamespace(realtime_client=client)
    instance._clock = lambda: now
    instance._rolling7_recovery_cursor = None
    instance._rolling7_recovery_last_at = None
    instance._rolling7_canonical_bootstrap_lock = asyncio.Lock()

    async def load_calendar(_current: date) -> tuple[date, ...]:
        return tuple(calendar)

    instance._load_trade_calendar = load_calendar
    return instance


def _policy_fixture(
    *,
    completed: Sequence[CompletedRolling],
    gaps: Sequence[ActiveRollingGap] = (),
    state_gaps: Sequence[ActiveRollingGap] = (),
    invalid: bool = False,
) -> tuple[Any, str]:
    config = load_v20_runtime_config(PROJECT_ROOT)
    artifacts = load_g_artifacts(
        config.artifact_manifest_path.parent,
        expected_manifest_sha256=config.artifact_manifest_sha256,
    )
    payload = genesis_state()
    payload["official_rolling_gaps"] = [
        {
            "gap_id": item.gap_id,
            "signal_date": item.signal_date.isoformat(),
            "maturity_date": item.maturity_date.isoformat(),
            "closed": item.closed,
            "aged_out": item.aged_out,
        }
        for item in state_gaps
    ]
    state = StateRecord(
        lineage_id=config.state_lineage_id,
        revision=0,
        state_hash=sha256_json(payload),
        payload=payload,
    )
    if invalid:
        prepared = prepare_invalid_entry(
            config=config,
            state=state,
            trade_date=ENTRY_CALENDAR[0],
            calendar=ENTRY_CALENDAR,
            reason_code="ENTRY_INPUT_UNAVAILABLE_BY_0940",
            detail="canonical input unavailable",
            invalid_commit_not_before_ts=datetime.combine(
                ENTRY_CALENDAR[0], time(9, 40), tzinfo=SHANGHAI
            ),
            completed_rolling=tuple(completed),
            maturity_gaps=tuple(gaps),
        )
    else:
        prepared = prepare_entry(
            config=config,
            state=state,
            bundle=_bundle(),
            completed_health=(),
            completed_rolling=tuple(completed),
            maturity_gaps=tuple(gaps),
            artifacts=artifacts,
            calendar=ENTRY_CALENDAR,
        )
    commit = prepared.commit
    record = OutboxRecord(
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
    sealed = seal_v20_payload(
        record,
        generated_at=datetime.combine(ENTRY_CALENDAR[0], time(9, 39, 30), tzinfo=SHANGHAI),
        commit_marker=17,
        on_time=True,
    )
    return prepared, str(sealed["message"])


def _completed_returns(values: Sequence[float]) -> tuple[CompletedRolling, ...]:
    origin = date(2026, 7, 1)
    return tuple(
        CompletedRolling(
            batch_id=f"batch-{index}",
            signal_date=origin + timedelta(days=index),
            t2_date=origin + timedelta(days=index + 2),
            batch_return=value,
        )
        for index, value in enumerate(values)
    )


def test_full_canonical_batch_is_equal_weight_and_r7_is_sum_not_average() -> None:
    references = {code: 100.0 for code in TOP10}
    leg_returns = (-0.10, -0.08, -0.06, -0.04, -0.02, 0.01, 0.03, 0.05, 0.07, 0.09)
    closes = {
        code: references[code] * (1.0 + leg_return)
        for code, leg_return in zip(TOP10, leg_returns, strict=True)
    }
    batch = make_batch(
        signal_date=date(2026, 7, 1),
        canonical_snapshot_id=SNAPSHOT_ID,
        canonical_snapshot_hash=SNAPSHOT_HASH,
        recommendations=_recommendations(),
        t2_date=date(2026, 7, 3),
        d0_references=references,
        d2_closes=closes,
    )
    assert batch.status is BatchStatus.COMPLETE
    assert tuple((leg.rank, leg.code) for leg in batch.legs) == tuple(enumerate(TOP10, start=1))
    assert batch.batch_return == pytest.approx(sum(leg_returns) / len(TOP10))

    returns = (-0.10, -0.10, -0.10, -0.10, -0.10, 0.20, 0.20)
    rolling = evaluate_rolling7(
        decision_date=date(2026, 8, 1),
        complete_batches=tuple(
            RollingBatch(
                batch_id=f"r7-{index}",
                signal_date=date(2026, 7, index + 1),
                t2_exit_date=date(2026, 7, index + 3),
                gross_price_return=value,
            )
            for index, value in enumerate(returns)
        ),
    )
    assert rolling.status is Rolling7Status.BAD
    assert rolling.r7 == pytest.approx(sum(returns))
    assert rolling.r7 != pytest.approx(sum(returns) / 7)
    assert rolling.l7 == 5
    assert len(rolling.window) == 7


def test_warmup_and_data_gap_are_explicit_in_semantic_reason_and_render() -> None:
    warmup, warmup_message = _policy_fixture(completed=_completed_returns((0.01,) * 6))
    warmup_semantic = warmup.commit.semantic
    assert warmup_semantic["rolling7_state"] == "WARMUP"
    assert warmup_semantic["rolling7_reason"] == "WARMUP:6/7"
    assert "ROLLING7_WARMUP" in warmup_semantic["reason_codes"]
    assert "WARMUP" in warmup_message
    assert "6/7" in warmup_message
    assert "UNKNOWN" not in warmup_message
    assert "R7=-" not in warmup_message
    assert "None/7" not in warmup_message

    gap = ActiveRollingGap(
        gap_id="rolling7:2026-08-01",
        signal_date=date(2026, 8, 1),
        maturity_date=date(2026, 8, 3),
    )
    data_gap, data_gap_message = _policy_fixture(
        completed=_completed_returns((0.01,) * 7),
        gaps=(gap,),
    )
    gap_semantic = data_gap.commit.semantic
    assert gap_semantic["rolling7_state"] == "DATA_GAP"
    assert gap_semantic["rolling7_reason"] == "DATA_GAP:rolling7:2026-08-01"
    assert "ROLLING7_DATA_GAP" in gap_semantic["reason_codes"]
    assert "DATA_GAP" in data_gap_message
    assert "rolling7:2026-08-01" in data_gap_message
    assert "UNKNOWN" not in data_gap_message
    assert "R7=-" not in data_gap_message
    assert "None/7" not in data_gap_message


@pytest.mark.parametrize(
    ("returns", "expected_state", "expected_r7", "expected_l7"),
    (
        ((-0.10, -0.10, -0.10, -0.10, 0.10, 0.10, 0.10), "NON_BAD", -0.10, 4),
        ((-0.10, -0.10, -0.10, -0.10, -0.10, 0.10, 0.10), "BAD", -0.30, 5),
    ),
)
def test_terminal_non_bad_and_bad_flow_through_prepare_and_renderer(
    returns: tuple[float, ...],
    expected_state: str,
    expected_r7: float,
    expected_l7: int,
) -> None:
    prepared, message = _policy_fixture(completed=_completed_returns(returns))
    semantic = prepared.commit.semantic
    assert semantic["rolling7_state"] == expected_state
    assert semantic["rolling7_r7"] == pytest.approx(expected_r7)
    assert semantic["rolling7_l7"] == expected_l7
    assert f"ROLLING7_{expected_state}" in semantic["reason_codes"]
    assert expected_state in message
    assert f"{expected_l7}/7" in message


def test_state_legacy_gap_cannot_change_independent_rolling_fact_result() -> None:
    completed = _completed_returns((0.02,) * 7)
    baseline, _ = _policy_fixture(completed=completed)
    legacy_gap = ActiveRollingGap(
        gap_id="legacy-shadow-gap-that-is-not-a-market-fact",
        signal_date=date(2026, 8, 1),
        maturity_date=date(2026, 8, 3),
    )
    contaminated, _ = _policy_fixture(
        completed=completed,
        state_gaps=(legacy_gap,),
    )
    for field in ("rolling7_state", "rolling7_r7", "rolling7_l7", "rolling7_window_ids"):
        assert contaminated.commit.semantic[field] == baseline.commit.semantic[field]
    assert contaminated.commit.next_state["official_rolling_gaps"] == [
        {
            "gap_id": legacy_gap.gap_id,
            "signal_date": legacy_gap.signal_date.isoformat(),
            "maturity_date": legacy_gap.maturity_date.isoformat(),
            "closed": False,
            "aged_out": False,
        }
    ]


def test_invalid_entry_reads_same_independent_rolling_facts_without_writing_gap() -> None:
    completed = _completed_returns((0.02,) * 7)
    valid, _ = _policy_fixture(completed=completed)
    invalid, _ = _policy_fixture(completed=completed, invalid=True)
    assert invalid.commit.semantic["rolling7_state"] == valid.commit.semantic["rolling7_state"]
    assert invalid.commit.semantic["rolling7_r7"] == valid.commit.semantic["rolling7_r7"]
    assert invalid.commit.semantic["rolling7_l7"] == valid.commit.semantic["rolling7_l7"]
    assert invalid.commit.next_state["official_rolling_gaps"] == []
    assert invalid.commit.shadow_batches == ()
    assert all(item.kind == "HEALTH" for item in valid.commit.shadow_batches)


async def test_canonical_repo_policy_prepare_and_render_form_one_real_seam() -> None:
    signal_date = date(2026, 8, 1)
    t2_date = date(2026, 8, 3)
    now = datetime(2026, 8, 4, 16, 0, tzinfo=SHANGHAI)
    artifact = _artifact_record(signal_date, t2_date=t2_date)
    repository = StrictRollingRepository(now=now)
    store = ArtifactStore((artifact,))
    client = HistoricalMarketClient()
    service = _service(
        repository,
        store,
        now=now,
        calendar=(signal_date, signal_date + timedelta(days=1), t2_date, now.date()),
        client=client,
    )

    intent = await service._record_rolling7_intent_from_artifact(artifact)
    assert intent.status is BatchStatus.DATA_GAP
    assert tuple((item.rank, item.code) for item in intent.recommendations) == tuple(
        enumerate(TOP10, start=1)
    )
    completed = await service._finalize_rolling7_market_health(
        signal_date,
        t2_date,
        now=now,
    )
    assert completed.status is BatchStatus.COMPLETE
    assert completed.batch_return == pytest.approx(0.10)
    assert {leg.code: leg.d0_reference for leg in completed.legs} == {code: 100.0 for code in TOP10}
    assert {leg.code: leg.d2_close for leg in completed.legs} == {code: 110.0 for code in TOP10}
    assert client.history_calls == [(TOP10, signal_date)]

    for index in range(6):
        batch = _complete_batch(
            date(2026, 7, 1) + timedelta(days=index),
            0.01,
            identity_index=index + 1,
        )
        await repository.save_rolling7_market_health(batch, updated_at=now)
    no_signal = make_batch(
        signal_date=date(2026, 7, 20),
        canonical_snapshot_id="e" * 64,
        canonical_snapshot_hash="f" * 64,
        recommendations=(),
        t2_date=date(2026, 7, 22),
    )
    await repository.save_rolling7_market_health(no_signal, updated_at=now)

    _health, rolling, gaps = await service._policy_inputs(ENTRY_CALENDAR[0])
    assert len(rolling) == 7
    assert not gaps
    assert all(item.batch_id != no_signal.canonical_snapshot_id for item in rolling)
    prepared, message = _policy_fixture(completed=rolling, gaps=gaps)
    assert prepared.commit.semantic["rolling7_state"] == "NON_BAD"
    assert prepared.commit.semantic["rolling7_r7"] == pytest.approx(0.16)
    assert prepared.commit.semantic["rolling7_l7"] == 0
    assert prepared.commit.semantic["symbols"] == _bundle().snapshot["symbols"]
    assert "NON_BAD" in message
    assert "R7=16.00%" in message
    assert all(call[0] not in FORBIDDEN_LEDGER_CALLS for call in repository.calls)

    calls_before_freeze = (len(client.history_calls), len(client.daily_calls))
    client.changed = True
    restarted = _service(
        repository,
        store,
        now=now + timedelta(days=1),
        calendar=(signal_date, signal_date + timedelta(days=1), t2_date, now.date()),
        client=client,
    )
    replay = await restarted._finalize_rolling7_market_health(
        signal_date,
        t2_date,
        now=now + timedelta(days=1),
    )
    assert replay == completed
    assert (len(client.history_calls), len(client.daily_calls)) == calls_before_freeze


async def test_gap_evidence_is_monotonic_and_complete_is_frozen() -> None:
    signal_date = date(2026, 7, 1)
    now = datetime(2026, 7, 5, 16, tzinfo=SHANGHAI)
    repository = StrictRollingRepository(now=now)
    recommendations = _recommendations((TOP10[0], TOP10[1]))
    empty = make_batch(
        signal_date=signal_date,
        canonical_snapshot_id=SNAPSHOT_ID,
        canonical_snapshot_hash=SNAPSHOT_HASH,
        recommendations=recommendations,
        t2_date=date(2026, 7, 3),
    )
    await repository.save_rolling7_market_health(empty, updated_at=now)
    partial = make_batch(
        signal_date=signal_date,
        canonical_snapshot_id=SNAPSHOT_ID,
        canonical_snapshot_hash=SNAPSHOT_HASH,
        recommendations=recommendations,
        t2_date=date(2026, 7, 3),
        d0_references={TOP10[0]: 100.0},
    )
    await repository.save_rolling7_market_health(partial, updated_at=now)
    with pytest.raises(V20SemanticConflict, match="removed or changed"):
        await repository.save_rolling7_market_health(
            make_batch(
                signal_date=signal_date,
                canonical_snapshot_id=SNAPSHOT_ID,
                canonical_snapshot_hash=SNAPSHOT_HASH,
                recommendations=recommendations,
                t2_date=date(2026, 7, 3),
                d0_references={TOP10[0]: 99.0},
            ),
            updated_at=now,
        )
    complete = make_batch(
        signal_date=signal_date,
        canonical_snapshot_id=SNAPSHOT_ID,
        canonical_snapshot_hash=SNAPSHOT_HASH,
        recommendations=recommendations,
        t2_date=date(2026, 7, 3),
        d0_references={TOP10[0]: 100.0, TOP10[1]: 100.0},
        d2_closes={TOP10[0]: 110.0, TOP10[1]: 90.0},
    )
    await repository.save_rolling7_market_health(complete, updated_at=now)
    assert repository.facts[signal_date] == complete
    with pytest.raises(V20SemanticConflict, match="immutable"):
        await repository.save_rolling7_market_health(
            make_missing_canonical_batch(
                signal_date=signal_date,
                t2_date=date(2026, 7, 3),
            ),
            updated_at=now,
        )


async def test_exact_0941_bar_must_be_observed_strictly_after_bar_end() -> None:
    signal_date = date(2026, 8, 1)
    t2_date = date(2026, 8, 3)
    now = datetime(2026, 8, 4, 16, tzinfo=SHANGHAI)
    artifact = _artifact_record(signal_date, t2_date=t2_date)
    repository = StrictRollingRepository(now=now)
    store = ArtifactStore((artifact,))
    bar = _minute_bar(TOP10[0], signal_date)
    repository.raw_records.append(
        SimpleNamespace(
            payload=_bar_payload(bar),
            first_received_at=bar.bar_end,
        )
    )
    service = _service(
        repository,
        store,
        now=now,
        calendar=(signal_date, signal_date + timedelta(days=1), t2_date, now.date()),
        client=None,
    )
    result = await service._finalize_rolling7_market_health(
        signal_date,
        t2_date,
        now=now,
    )
    assert result.status is BatchStatus.DATA_GAP
    assert all(leg.d0_reference is None for leg in result.legs)


async def test_no_signal_is_durable_but_excluded_without_market_or_account_calls() -> None:
    signal_date = date(2026, 8, 1)
    now = datetime(2026, 8, 4, 16, tzinfo=SHANGHAI)
    artifact = _artifact_record(signal_date, recommendations=())
    repository = StrictRollingRepository(now=now)
    store = ArtifactStore((artifact,))
    client = HistoricalMarketClient()
    service = _service(
        repository,
        store,
        now=now,
        calendar=(signal_date, signal_date + timedelta(days=1), signal_date + timedelta(days=2)),
        client=client,
    )
    fact = await service._record_rolling7_intent_from_artifact(artifact)
    assert fact.signal_kind is SignalKind.NO_SIGNAL
    assert fact.status is BatchStatus.COMPLETE
    assert fact.t2_date is None
    _health, rolling, gaps = await service._policy_inputs(date(2026, 8, 31))
    assert rolling == []
    assert gaps == []
    assert client.history_calls == []
    assert client.daily_calls == []
    assert all(call[0] not in FORBIDDEN_LEDGER_CALLS for call in repository.calls)


async def test_equivalent_existing_canonical_artifact_still_records_intent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical = _canonical()
    artifact = _artifact_record(
        canonical.trade_date,
        recommendations=tuple(
            CanonicalRecommendation(rank=stock.rank, code=stock.code)
            for stock in canonical.scan_result.recommended
        ),
    )
    artifact.payload["canonical_integrity_hash"] = canonical._integrity_hash

    class ExistingOnlyStore(ArtifactStore):
        async def save_once(self, *_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("equivalent canonical retry must reuse the durable artifact")

    now = datetime.combine(canonical.trade_date, time(16), tzinfo=SHANGHAI)
    repository = StrictRollingRepository(now=now)
    service = _service(
        repository,
        ExistingOnlyStore((artifact,)),
        now=now,
        calendar=canonical.computation_calendar,
    )
    service._canonical_callbacks_open = True
    service._canonical_artifact_lock = asyncio.Lock()
    service._canonical_raw_persisted_dates = set()

    async def no_raw(_canonical: CanonicalV16ScanBundle) -> None:
        return None

    async def hydrate(record: Any) -> SimpleNamespace:
        return SimpleNamespace(trade_date=record.trade_date)

    monkeypatch.setattr(service, "_persist_canonical_raw_minute_bars", no_raw)
    monkeypatch.setattr(service, "_hydrate_canonical_artifact_record", hydrate)
    await service._persist_canonical_artifact_barrier(canonical)
    fact = repository.facts[canonical.trade_date]
    assert fact.signal_kind is SignalKind.SIGNAL
    assert tuple(item.code for item in fact.recommendations) == tuple(
        stock.code for stock in canonical.scan_result.recommended
    )


def test_historical_canonical_replay_supplies_all_nine_frozen_inputs() -> None:
    source = textwrap.dedent(
        inspect.getsource(V20Service._compute_canonical_v16_from_persisted_raw)
    )
    tree = ast.parse(source)
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and (isinstance(node.func, ast.Name) and node.func.id == "compute_canonical_v16_scan")
    ]
    assert len(calls) == 1
    keywords = {item.arg: item.value for item in calls[0].keywords if item.arg is not None}
    assert NINE_FROZEN_CANONICAL_INPUTS <= set(keywords)
    assert isinstance(keywords.get("allow_realtime_fetch"), ast.Constant)
    assert keywords["allow_realtime_fetch"].value is False


async def test_historical_bootstrap_uses_target_date_names_for_st_eligibility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal_date = date(2026, 8, 28)
    calendar = (
        date(2026, 8, 27),
        signal_date,
        date(2026, 8, 31),
        date(2026, 9, 1),
    )
    now = datetime(2026, 9, 2, 16, tzinfo=SHANGHAI)

    class HistoricalNamesClient(HistoricalMarketClient):
        def __init__(self) -> None:
            super().__init__()
            self.names_calls: list[str] = []

        async def fetch_stock_names_for_date(self, trade_date: str) -> dict[str, str]:
            self.names_calls.append(trade_date)
            return {
                code: (
                    "ST历史样本"
                    if code == TOP10[1]
                    else "*ST历史样本"
                    if code == TOP10[2]
                    else f"历史名称{code}"
                )
                for code in TOP10
            }

    class ForbiddenCurrentFundamentals:
        async def batch_current_names(self, *_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("historical bootstrap read current stock_basic names")

        async def batch_filter_st(self, *_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("historical bootstrap called current batch_filter_st")

    client = HistoricalNamesClient()
    repository = StrictRollingRepository(now=now)
    service = _service(
        repository,
        ArtifactStore(),
        now=now,
        calendar=calendar,
        client=client,
    )
    service._scan_state = SimpleNamespace(
        realtime_client=client,
        historical_adapter=SimpleNamespace(),
        fundamentals_db=ForbiddenCurrentFundamentals(),
    )

    monkeypatch.setattr(
        service_module,
        "derive_canonical_v16_universe",
        lambda _state: (SimpleNamespace(), SimpleNamespace(), {}, TOP10),
    )

    async def historical_seed(
        trade_date: date,
        **_kwargs: Any,
    ) -> tuple[dict[str, Any], tuple[str, ...], dict[str, Any]]:
        assert trade_date == signal_date
        return (
            {code: SimpleNamespace(quote=SimpleNamespace(is_trading=True)) for code in TOP10},
            TOP10,
            {},
        )

    async def historical_ohlcv(
        _adapter: Any,
        codes: list[str],
        trade_date: date,
    ) -> dict[str, dict[str, list[Any]]]:
        assert tuple(codes) == TOP10
        assert trade_date == signal_date
        return {code: {"time": [signal_date.isoformat()]} for code in codes}

    captured: dict[str, Any] = {}

    async def canonical_scan(
        _state: Any,
        trade_date: date,
        **kwargs: Any,
    ) -> SimpleNamespace:
        assert trade_date == signal_date
        captured.update(kwargs)
        return SimpleNamespace(trade_date=trade_date)

    async def no_raw(_canonical: Any) -> None:
        return None

    monkeypatch.setattr(service, "_historical_early_evidence_seed", historical_seed)
    monkeypatch.setattr(service_module, "_fetch_history_ohlcv", historical_ohlcv)
    monkeypatch.setattr(service_module, "compute_canonical_v16_scan", canonical_scan)
    monkeypatch.setattr(service, "_persist_canonical_raw_minute_bars", no_raw)

    await service._compute_canonical_v16_from_persisted_raw(
        SimpleNamespace(trade_date=signal_date, calendar=calendar)
    )

    assert client.names_calls == ["20260828"]
    assert captured["names_override"][TOP10[1]] == "ST历史样本"
    assert captured["names_override"][TOP10[2]] == "*ST历史样本"
    assert tuple(captured["st_eligible_codes_override"]) == tuple(
        code for code in TOP10 if code not in {TOP10[1], TOP10[2]}
    )
    assert captured["allow_realtime_fetch"] is False


async def test_first_deploy_backfill_rebuilds_seven_missing_canonical_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = tuple(date(2026, 7, 1) + timedelta(days=index) for index in range(14))
    signals = calendar[:7]
    now = datetime.combine(calendar[-1] + timedelta(days=1), time(16), tzinfo=SHANGHAI)
    trace: list[tuple[str, date]] = []
    repository = StrictRollingRepository(now=now)
    store = ArtifactStore()
    client = HistoricalMarketClient(trace=trace)
    service = _service(
        repository,
        store,
        now=now,
        calendar=calendar,
        client=client,
    )

    async def rebuild(context_or_date: Any) -> SimpleNamespace:
        trade_date = (
            context_or_date if isinstance(context_or_date, date) else context_or_date.trade_date
        )
        trace.append(("compute", trade_date))
        return SimpleNamespace(trade_date=trade_date)

    async def persist(canonical: Any) -> None:
        trace.append(("persist", canonical.trade_date))
        store.records[canonical.trade_date] = _artifact_record(canonical.trade_date)

    monkeypatch.setattr(service, "_compute_canonical_v16_from_persisted_raw", rebuild)
    monkeypatch.setattr(service, "_persist_canonical_artifact_barrier", persist)
    result = await service.backfill_rolling7_market_health(
        signal_dates=signals,
        limit=7,
        overall_cap=30,
    )
    assert len(result) == 7
    assert all(item.status is BatchStatus.COMPLETE for item in result)
    assert sum(kind == "compute" for kind, _day in trace) == 7
    assert sum(kind == "persist" for kind, _day in trace) == 7
    for signal_date in signals:
        ordered_kinds = [kind for kind, day in trace if day == signal_date]
        assert ordered_kinds.index("compute") < ordered_kinds.index("persist")
        assert ordered_kinds.index("persist") < ordered_kinds.index("history")
        assert repository.facts[signal_date].status is BatchStatus.COMPLETE
    assert not FORBIDDEN_LEDGER_CALLS.intersection(name for name, _value in repository.calls)


async def test_recovery_does_not_stop_for_seven_old_batches_when_recent_gap_is_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = tuple(date(2026, 6, 1) + timedelta(days=index) for index in range(80))
    now = datetime.combine(calendar[-1] + timedelta(days=1), time(16), tzinfo=SHANGHAI)
    repository = StrictRollingRepository(now=now)
    store = ArtifactStore()
    for index in range(7):
        batch = _complete_batch(calendar[index], 0.01, identity_index=index + 1)
        await repository.save_rolling7_market_health(batch, updated_at=now)
    gap_date = calendar[-10]
    gap = make_batch(
        signal_date=gap_date,
        canonical_snapshot_id=SNAPSHOT_ID,
        canonical_snapshot_hash=SNAPSHOT_HASH,
        recommendations=_recommendations(),
        t2_date=gap_date + timedelta(days=2),
    )
    await repository.save_rolling7_market_health(gap, updated_at=now)
    service = _service(repository, store, now=now, calendar=calendar)
    finalized: list[date] = []

    async def finalize(
        signal_date: date,
        t2_date: date,
        now: datetime | None = None,
        *,
        calendar: Sequence[date] | None = None,
    ) -> Rolling7Batch:
        assert calendar is not None
        finalized.append(signal_date)
        complete = make_batch(
            signal_date=signal_date,
            canonical_snapshot_id=SNAPSHOT_ID,
            canonical_snapshot_hash=SNAPSHOT_HASH,
            recommendations=_recommendations(),
            t2_date=t2_date,
            d0_references={code: 100.0 for code in TOP10},
            d2_closes={code: 101.0 for code in TOP10},
        )
        stored = await repository.save_rolling7_market_health(
            complete,
            updated_at=now or service._clock(),
        )
        return stored.batch

    monkeypatch.setattr(service, "_finalize_rolling7_market_health", finalize)
    result = await service.backfill_rolling7_market_health(
        signal_dates=(gap_date,), limit=1, overall_cap=5
    )
    assert finalized == [gap_date]
    assert len(result) == 1 and result[0].status is BatchStatus.COMPLETE


async def test_recovery_cursor_crosses_multiple_no_signal_caps_to_reach_old_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = tuple(date(2026, 4, 1) + timedelta(days=index) for index in range(35))
    now = datetime.combine(calendar[-1] + timedelta(days=1), time(16), tzinfo=SHANGHAI)
    signal_dates = calendar[8:15]
    no_signal_dates = calendar[15:-2]
    assert len(no_signal_dates) > 5
    store = ArtifactStore(
        tuple(_artifact_record(day) for day in signal_dates)
        + tuple(_artifact_record(day, recommendations=()) for day in no_signal_dates)
    )
    repository = StrictRollingRepository(now=now)
    service = _service(repository, store, now=now, calendar=calendar)
    finalized: list[date] = []

    async def finalize(
        signal_date: date,
        t2_date: date,
        now: datetime | None = None,
        *,
        calendar: Sequence[date] | None = None,
    ) -> Rolling7Batch:
        assert now is not None
        assert calendar is not None
        finalized.append(signal_date)
        complete = make_batch(
            signal_date=signal_date,
            canonical_snapshot_id=store.records[signal_date].snapshot_id,
            canonical_snapshot_hash=store.records[signal_date].snapshot_hash,
            recommendations=_recommendations(),
            t2_date=t2_date,
            d0_references={code: 100.0 for code in TOP10},
            d2_closes={code: 101.0 for code in TOP10},
        )
        return (await repository.save_rolling7_market_health(complete, updated_at=now)).batch

    monkeypatch.setattr(service, "_finalize_rolling7_market_health", finalize)
    cursors: list[date | None] = []
    recovered: list[Rolling7Batch] = []
    for _tick in range(6):
        recovered.extend(await service.backfill_rolling7_market_health(limit=7, overall_cap=5))
        cursors.append(service._rolling7_recovery_cursor)
        if len(recovered) == 7:
            break

    assert cursors[:3] == [calendar[27], calendar[22], calendar[17]]
    assert set(finalized) == set(signal_dates)
    assert len(recovered) == 7
    _health, rolling, gaps = await service._policy_inputs(now.date())
    assert len(rolling) == 7
    assert gaps == []


async def test_recovery_retries_timeout_cursor_and_walks_beyond_no_signal_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = tuple(date(2026, 5, 1) + timedelta(days=index) for index in range(70))
    now = datetime.combine(calendar[-1] + timedelta(days=1), time(16), tzinfo=SHANGHAI)
    repository = StrictRollingRepository(now=now)
    store = ArtifactStore()
    for index in range(3):
        batch = _complete_batch(calendar[index], 0.01, identity_index=index + 1)
        await repository.save_rolling7_market_health(batch, updated_at=now)
    no_signal_dates = calendar[-22:-2]
    for signal_date in no_signal_dates:
        store.records[signal_date] = _artifact_record(signal_date, recommendations=())
    # Three healthy recoveries leave the window at 6/7, so the one transient
    # failure is still required after the bounded cursor wraps around.
    recovery_dates = calendar[-26:-22]
    for signal_date in recovery_dates:
        store.records[signal_date] = _artifact_record(signal_date)
    service = _service(repository, store, now=now, calendar=calendar)
    attempts: list[date] = []
    fail_once = True

    async def finalize(
        signal_date: date,
        t2_date: date,
        now: datetime | None = None,
        *,
        calendar: Sequence[date] | None = None,
    ) -> Rolling7Batch:
        nonlocal fail_once
        assert calendar is not None
        attempts.append(signal_date)
        if fail_once:
            fail_once = False
            raise TimeoutError("bounded historical recovery")
        complete = make_batch(
            signal_date=signal_date,
            canonical_snapshot_id=store.records[signal_date].snapshot_id,
            canonical_snapshot_hash=store.records[signal_date].snapshot_hash,
            recommendations=_recommendations(),
            t2_date=t2_date,
            d0_references={code: 100.0 for code in TOP10},
            d2_closes={code: 101.0 for code in TOP10},
        )
        stored = await repository.save_rolling7_market_health(
            complete,
            updated_at=now or service._clock(),
        )
        return stored.batch

    monkeypatch.setattr(service, "_finalize_rolling7_market_health", finalize)
    first = await service.backfill_rolling7_market_health(limit=4, overall_cap=50)
    failed_date = recovery_dates[-1]
    assert attempts[0] == failed_date
    assert failed_date not in repository.facts
    assert len(first) == 3
    assert all(item.signal_date < failed_date for item in first)

    later: list[Rolling7Batch] = []
    for _tick in range(3):
        later.extend(await service.backfill_rolling7_market_health(limit=4, overall_cap=50))
        if failed_date in repository.facts:
            break
    assert repository.facts[failed_date].status is BatchStatus.COMPLETE
    assert attempts.count(failed_date) == 2
    assert any(item.signal_date == failed_date for item in later)
    assert all(item.status is BatchStatus.COMPLETE for item in (*first, *later))
    assert (calendar[-1] - min(item.signal_date for item in first)).days > 14
    _health, rolling, gaps = await service._policy_inputs(now.date())
    assert len(rolling) >= 7
    assert not gaps


async def test_historical_no_signal_is_persisted_once_and_excluded_after_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = tuple(date(2026, 8, 1) + timedelta(days=index) for index in range(8))
    signal_date = calendar[1]
    t2_date = calendar[3]
    now = datetime.combine(calendar[-1] + timedelta(days=1), time(16), tzinfo=SHANGHAI)
    artifact = _artifact_record(
        signal_date,
        recommendations=(),
        t2_date=t2_date,
    )
    repository = StrictRollingRepository(now=now)
    store = ArtifactStore((artifact,))
    service = _service(repository, store, now=now, calendar=calendar)
    compute_calls = 0

    async def forbidden_compute(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal compute_calls
        compute_calls += 1
        raise AssertionError("durable NO_SIGNAL artifact was recomputed")

    monkeypatch.setattr(
        service,
        "_compute_canonical_v16_from_persisted_raw",
        forbidden_compute,
    )

    await service.backfill_rolling7_market_health(
        signal_dates=(signal_date,),
        limit=1,
        overall_cap=2,
    )

    fact = repository.facts[signal_date]
    assert fact.signal_kind is SignalKind.NO_SIGNAL
    assert fact.status is BatchStatus.COMPLETE
    assert fact.batch_return is None
    _health, rolling, gaps = await service._policy_inputs(now.date())
    assert not rolling
    assert not gaps

    restarted = _service(repository, store, now=now, calendar=calendar)
    monkeypatch.setattr(
        restarted,
        "_compute_canonical_v16_from_persisted_raw",
        forbidden_compute,
    )
    await restarted.backfill_rolling7_market_health(
        signal_dates=(signal_date,),
        limit=1,
        overall_cap=2,
    )
    assert repository.facts[signal_date] == fact
    assert compute_calls == 0


async def test_one_failed_recovery_date_cannot_starve_an_older_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = tuple(date(2026, 8, 1) + timedelta(days=index) for index in range(12))
    healthy_date = calendar[2]
    failing_date = calendar[3]
    now = datetime.combine(calendar[-1] + timedelta(days=1), time(16), tzinfo=SHANGHAI)
    store = ArtifactStore(
        (
            _artifact_record(healthy_date, t2_date=calendar[4]),
            _artifact_record(failing_date, t2_date=calendar[5]),
        )
    )
    repository = StrictRollingRepository(now=now)
    service = _service(repository, store, now=now, calendar=calendar)
    attempts: list[date] = []

    async def finalize(
        signal_date: date,
        t2_date: date,
        now: datetime | None = None,
        *,
        calendar: Sequence[date] | None = None,
    ) -> Rolling7Batch:
        assert calendar is not None
        attempts.append(signal_date)
        if signal_date == failing_date:
            raise RuntimeError("one historical date remains unavailable")
        complete = make_batch(
            signal_date=signal_date,
            canonical_snapshot_id=store.records[signal_date].snapshot_id,
            canonical_snapshot_hash=store.records[signal_date].snapshot_hash,
            recommendations=_recommendations(),
            t2_date=t2_date,
            d0_references={code: 100.0 for code in TOP10},
            d2_closes={code: 101.0 for code in TOP10},
        )
        return (
            await repository.save_rolling7_market_health(
                complete,
                updated_at=now or service._clock(),
            )
        ).batch

    monkeypatch.setattr(service, "_finalize_rolling7_market_health", finalize)

    processed: list[Rolling7Batch] = []
    for _tick in range(2):
        processed.extend(
            await service.backfill_rolling7_market_health(
                signal_dates=(healthy_date, failing_date),
                limit=2,
                overall_cap=2,
            )
        )

    assert attempts[0] == failing_date
    assert any(item.signal_date == healthy_date for item in processed)
    assert repository.facts[healthy_date].status is BatchStatus.COMPLETE


async def test_recovery_cancellation_propagates_and_restores_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = tuple(date(2026, 8, 1) + timedelta(days=index) for index in range(8))
    signal_date = calendar[2]
    previous_cursor = calendar[0]
    now = datetime.combine(calendar[-1] + timedelta(days=1), time(16), tzinfo=SHANGHAI)
    repository = StrictRollingRepository(now=now)
    service = _service(
        repository,
        ArtifactStore((_artifact_record(signal_date, t2_date=calendar[4]),)),
        now=now,
        calendar=calendar,
    )
    service._rolling7_recovery_cursor = previous_cursor

    async def cancelled(
        _signal_date: date,
        _t2_date: date,
        now: datetime | None = None,
        *,
        calendar: Sequence[date] | None = None,
    ) -> Rolling7Batch:
        assert now is not None
        assert calendar is not None
        raise asyncio.CancelledError

    monkeypatch.setattr(service, "_finalize_rolling7_market_health", cancelled)

    with pytest.raises(asyncio.CancelledError):
        await service.backfill_rolling7_market_health(
            signal_dates=(signal_date,),
            limit=1,
            overall_cap=1,
        )
    assert service._rolling7_recovery_cursor == previous_cursor
    assert repository.facts == {}


async def test_rolling7_d0_uses_one_physical_rt_min_batch_and_no_rt_min_daily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal_date = date(2026, 9, 2)
    now = datetime(2026, 9, 2, 9, 42, tzinfo=SHANGHAI)
    client = TushareRealtimeClient("unit-token")
    client._client = SimpleNamespace()
    calls: list[tuple[str, Mapping[str, Any], str | None]] = []

    async def api_call(
        api_name: str,
        params: Mapping[str, Any],
        fields: str | None = None,
    ) -> dict[str, Any]:
        calls.append((api_name, dict(params), fields))
        requested = str(params["ts_code"]).split(",")
        return {
            "data": {
                "fields": ["ts_code", "time", "open", "close", "high", "low", "vol", "amount"],
                "items": [
                    [code, "2026-09-02 09:41:00", 10.0, 10.1, 10.2, 9.9, 1000.0, 10000.0]
                    for code in requested
                ],
            }
        }

    monkeypatch.setattr(client, "_api_call", api_call)
    artifact = _artifact_record(signal_date)
    repository = StrictRollingRepository(now=now)
    service = _service(
        repository,
        ArtifactStore((artifact,)),
        now=now,
        calendar=(signal_date, signal_date + timedelta(days=1), signal_date + timedelta(days=2)),
        client=client,
    )
    context = SimpleNamespace(
        trade_date=signal_date,
        last_rolling7_d0_history_at=None,
    )

    await service._acquire_rolling7_d0_evidence(context, now)

    assert len(calls) == 1
    api_name, params, fields = calls[0]
    assert api_name == "rt_min"
    assert str(params["ts_code"]).split(",") == [client._to_ts_code(code) for code in TOP10]
    assert fields is not None and "ts_code" in fields
    assert all(call[0] != "rt_min_daily" for call in calls)
    fact = repository.facts[signal_date]
    assert fact.status is BatchStatus.DATA_GAP
    assert {leg.code: leg.d0_reference for leg in fact.legs} == {code: 10.0 for code in TOP10}

    await service._acquire_rolling7_d0_evidence(
        context,
        now + timedelta(seconds=1),
    )

    assert len(calls) == 1
    assert repository.facts[signal_date] == fact


@pytest.mark.parametrize("persisted_d0_n", (3, len(TOP10)))
async def test_d2_finalize_reuses_persisted_d0_without_evidence_regression(
    persisted_d0_n: int,
) -> None:
    signal_date = date(2026, 8, 1)
    t2_date = date(2026, 8, 3)
    now = datetime(2026, 8, 4, 16, tzinfo=SHANGHAI)
    artifact = _artifact_record(signal_date, t2_date=t2_date)
    repository = StrictRollingRepository(now=now)
    existing = make_batch(
        signal_date=signal_date,
        canonical_snapshot_id=artifact.snapshot_id,
        canonical_snapshot_hash=artifact.snapshot_hash,
        recommendations=_recommendations(),
        t2_date=t2_date,
        d0_references={code: 100.0 for code in TOP10[:persisted_d0_n]},
    )
    await repository.save_rolling7_market_health(existing, updated_at=now)
    client = HistoricalMarketClient()
    service = _service(
        repository,
        ArtifactStore((artifact,)),
        now=now,
        calendar=(signal_date, signal_date + timedelta(days=1), t2_date, now.date()),
        client=client,
    )

    completed = await service._finalize_rolling7_market_health(
        signal_date,
        t2_date,
        now=now,
    )

    assert completed.status is BatchStatus.COMPLETE
    assert completed.batch_return == pytest.approx(0.10)
    assert {leg.code: leg.d0_reference for leg in completed.legs} == {code: 100.0 for code in TOP10}
    assert {leg.code: leg.d2_close for leg in completed.legs} == {code: 110.0 for code in TOP10}
    expected_missing = TOP10[persisted_d0_n:]
    assert client.history_calls == ([(expected_missing, signal_date)] if expected_missing else [])


def test_startup_genesis_call_binds_to_repository_without_obsolete_kwargs() -> None:
    source = textwrap.dedent(inspect.getsource(V20Service.start))
    tree = ast.parse(source)
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "ensure_genesis_state"
    ]
    assert len(calls) == 1
    keyword_names = {item.arg for item in calls[0].keywords if item.arg is not None}
    assert not {
        "current_config_id",
        "current_config_hash",
        "current_config_payload",
    }.intersection(keyword_names)
    signature = inspect.signature(V20Repository.ensure_genesis_state)
    signature.bind_partial(
        None,
        "lineage",
        {},
        "state-hash",
        **{name: None for name in keyword_names},
    )
