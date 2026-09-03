"""Production orchestration for V20 decisions and Feishu notifications.

The service owns a model ledger, not a brokerage account.  It never reads
positions and never creates orders or fills.  Its only externally visible
effects are immutable decision evidence and durable Feishu outbox events.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import math
import os
import re
import socket
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from src.common.v20_feishu import (
    V20FeishuRoute,
    V20OutboxPublisher,
    load_legacy_embedded_v20_route,
    load_v20_feishu_routes,
    seal_v20_payload,
)
from src.data.clients.mews_snapshot import (
    MEWS_PUBLISH_TIME,
    LocalMewsSnapshotCalculator,
    MewsSnapshotSourceError,
)
from src.data.clients.tushare_realtime import (
    TushareDailyBar,
    TushareEarlyMarketData,
    TushareMinuteBar,
    tushare_minute_bars_to_early_market_data,
)
from src.data.clients.v20_market_data import V20EarlyBarCollector, exact_reference_prices
from src.data.database.v16_canonical_artifact_store import (
    SNAPSHOT_TYPE as V16_CANONICAL_ARTIFACT_EVENT,
)
from src.data.database.v16_canonical_artifact_store import (
    V16CanonicalArtifactStore,
)
from src.data.database.v20_mews_guard_store import V20MewsGuardStore
from src.data.database.v20_mews_receipt_guard import V20MewsReceiptGuard
from src.data.database.v20_repository import (
    ActiveModelLeg,
    EntryStatus,
    ExitCommit,
    ManualMonitorEnrollmentCommit,
    ManualMonitorEnrollmentRecord,
    MinuteBarRecord,
    ModelBatchWrite,
    ModelLegWrite,
    OutboxRecord,
    SelectedMewsRecord,
    StateRecord,
    V20EntryDeadlineExceeded,
    V20LeadershipLost,
    V20MinuteBarIntegrityConflict,
    V20Repository,
    V20RepositoryError,
    V20SemanticConflict,
    V20StateConflict,
    create_embedded_v20_repository_from_config,
    create_v20_repository_from_config,
    sha256_json,
)
from src.data.sources.local_concept_mapper import resolve_concept_data_path
from src.strategy.v20.artifacts import GArtifactBundle, load_g_artifacts
from src.strategy.v20.decision_engine import (
    ActiveRollingGap,
    CompletedHealth,
    CompletedRolling,
    PreparedEntry,
    genesis_state,
    prepare_entry,
    prepare_invalid_entry,
    restore_state_before,
)
from src.strategy.v20.exit_policy import (
    derive_model_leg_id,
    evaluate_exit,
    is_valid_complete_minute_bar,
)
from src.strategy.v20.identity import event_id, named_hash, official_slot_id
from src.strategy.v20.models import (
    V20_DATA_ALERT_SEMANTIC_SCHEMA,
    V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
    V20_ENTRY_SEMANTIC_SCHEMA,
    V20_EXIT_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
    V20_INVALID_INPUT_SNAPSHOT_SCHEMA,
    V20_V16_SNAPSHOT_SCHEMA,
    MewsSnapshot,
    MinuteBar,
    ModelLeg,
    ReferenceStatus,
    RollingBatch,
    RollingGap,
)
from src.strategy.v20.policy import evaluate_rolling7
from src.strategy.v20.rolling7_market_health import (
    BatchStatus,
    CanonicalRecommendation,
    Rolling7Batch,
    SignalKind,
    make_batch,
    make_missing_canonical_batch,
)
from src.strategy.v20.runtime_config import (
    V20ConfigError,
    V20RouteBinding,
    V20RuntimeConfig,
    load_v20_runtime_config,
    validate_v20_api_keys,
    validate_v20_database_consumers,
    validated_v20_tushare_token,
)
from src.strategy.v20.shadow_evaluator import evaluate_shadow_batch
from src.web.v20_canonical_selection import (
    CanonicalV16ScanBundle,
    V20CanonicalSelectionState,
    _fetch_history_ohlcv,
    _initialize_v20_selection_resources_once,
    cleanup_v20_selection_resources,
    compute_canonical_v16_scan,
    derive_canonical_v16_universe,
    get_or_compute_canonical_v16,  # noqa: F401 - retained test/adapter compatibility symbol
)
from src.web.v20_scan_pipeline import (
    FrozenV16ScanBundle,
    V20PrewarmedScan,
)
from src.web.v20_v16_canonical_artifact import (
    encode as encode_v16_canonical_artifact,
)
from src.web.v20_v16_canonical_artifact import (
    hydrate as hydrate_v16_canonical_artifact,
)

logger = logging.getLogger(__name__)
_LATE_0939_REPLAY_ACTIONS = (
    "ENTER",
    "BLOCK",
    "NO_SIGNAL",
    "INPUT_INVALID",
)
SHANGHAI = ZoneInfo("Asia/Shanghai")
# Every end label a canonical early (<=09:39) raw bar can possibly carry:
# each minute-aligned label from 00:00 through the 09:39 decision bar.  The
# 09:25/09:30 strategy inputs are inside this set.  Database reads for
# persisted early evidence must cover this whole set, and every early-evidence
# filter must test membership in it — never a bare string comparison.
EARLY_RAW_BAR_LABELS: tuple[str, ...] = tuple(
    f"{hour:02d}:{minute:02d}"
    for hour in range(10)
    for minute in range(60)
    if (hour, minute) <= (9, 39)
)
EARLY_RAW_LAST_LABEL = "09:39"
# Deterministic chunk size for historical stk_mins backfill.  Each completed
# chunk is normalized and persisted immediately, so a cancellation or failure
# never loses finished work and the next attempt re-derives pending from the
# database instead of re-requesting it.
HISTORICAL_SEED_BACKFILL_CHUNK = 128
# Bounded sample of conflicted universe codes included in a seed conflict
# error; the full sorted set stays available in the fold result for callers.
HISTORICAL_SEED_CONFLICT_SAMPLE = 10
MAX_ENTRY_HISTORY_RECOVERY_CODES = 50
ENTRY_HISTORY_RECOVERY_TIMEOUT_SECONDS = 15.0
TRADE_CALENDAR_TIMEOUT_SECONDS = 15.0
PREWARM_ATTEMPT_TIMEOUT_SECONDS = 60.0
ENTRY_CUTOFF_RESERVE_SECONDS = 2.0
# Keep the daily vendor request comfortably bounded while still supporting a
# long restart gap and the next two exchange sessions.  Asking for calendars
# years into the future is unsafe because exchanges have not published them.
TRADE_CALENDAR_PAST_DAYS = 730
TRADE_CALENDAR_FUTURE_DAYS = 45
# Bounded first-load budget for the 09:40 cutoff watchdog.  It must cover one
# full provider timeout plus the cutoff reserve so a preexisting calendar
# master always fits inside the outer budget.
_CALENDAR_CUTOFF_LOAD_BUDGET_SECONDS = TRADE_CALENDAR_TIMEOUT_SECONDS + ENTRY_CUTOFF_RESERVE_SECONDS
MAX_CLOSED_EXIT_RECOVERY_TARGETS_PER_TICK = 4
CLOSED_EXIT_RECOVERY_TIMEOUT_SECONDS = 3.0
CLOSED_EXIT_RECOVERY_RETRY_SECONDS = 30.0
MAX_STALE_EXIT_LEGS_PER_TICK = 20
OUTBOX_RECOVERY_TICK_TIMEOUT_SECONDS = 3.0
LIVE_EXIT_MAX_TICK_SECONDS = 12.0
LATEST_MINUTE_POLL_TIMEOUT_SECONDS = 8.0
LIVE_EXIT_SCHEDULER_WATCHDOG_SECONDS = 14.0
LIVE_EXIT_DB_STAGE_TIMEOUT_SECONDS = 2.0
LIVE_EXIT_RULE_SYMBOL_TIMEOUT_SECONDS = 3.0
LIVE_EXIT_RULE_BATCH_TIMEOUT_SECONDS = 4.0
LIVE_EXIT_LIVE_HISTORY_TIMEOUT_SECONDS = 8.0
LIVE_EXIT_RULE_DRAIN_RESERVE_SECONDS = 1.0
LIVE_EXIT_MIN_DEADLINE_RESERVE_SECONDS = 0.1
STALE_EXIT_RULE_SYMBOL_TIMEOUT_SECONDS = 1.5
LIVE_EXIT_MORNING_CLOSE_PUBLICATION_GRACE_SECONDS = 60.0
STALE_EXIT_TICK_SECONDS = 30.0
STALE_EXIT_TICK_TIMEOUT_SECONDS = 3.0
OUTBOX_RECOVERY_TICK_SECONDS = 2.0
OUTBOX_RECOVERY_LANE_TIMEOUT_SECONDS = 1.5
ROLLING7_RECOVERY_TICK_SECONDS = 300.0
ROLLING7_RECOVERY_OVERALL_CAP = 90
ROLLING7_RECOVERY_SLICE = 3
ROLLING7_AUTOMATIC_BLACKOUT_START = time(9, 0)
ROLLING7_AUTOMATIC_BLACKOUT_END = time(15, 5)
STATUS_SNAPSHOT_MAX_AGE_SECONDS = OUTBOX_RECOVERY_TICK_SECONDS * 3.0 + 1.0
MANUAL_TRIGGER_DECISION_LOCK_TIMEOUT_SECONDS = 15.0
LATE_0939_REPLAY_TOTAL_TIMEOUT_SECONDS = 180.0
LATE_0939_REPLAY_RETRY_SECONDS = 900.0
LATE_0939_REPLAY_MAX_AUTOMATIC_ATTEMPTS = 2
MANUAL_MONITOR_HISTORY_TIMEOUT_SECONDS = 30.0
V20_RUNTIME_TASK_NAMES = frozenset(
    {
        "v20-decision-scheduler",
        "v20-live-exit-scheduler",
        "v20-stale-exit-scheduler",
        "v20-outbox-recovery-scheduler",
        "v20-rolling7-recovery-scheduler",
        "v20-outbox-publisher",
        "v20-mews-cache-scheduler",
    }
)
MEWS_CACHE_CUTOFF = time(9, 40)
MEWS_CACHE_POLL_SECONDS = 30.0
_MANUAL_REQUEST_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}")

Clock = Callable[[], datetime]
ResourceInitializer = Callable[[V20CanonicalSelectionState], Awaitable[None]]
ResourceCleanup = Callable[[V20CanonicalSelectionState], Awaitable[None]]
CalendarProvider = Callable[[], Awaitable[list[date]]]


async def _init_v20_scan_resources(scan_state: V20CanonicalSelectionState) -> None:
    """Initialize the V16 scanner inputs without legacy scheduler side effects."""
    token = validated_v20_tushare_token()
    await _initialize_v20_selection_resources_once(
        scan_state,
        lambda: _init_v20_scan_resources_with_token(
            scan_state,
            token,
        ),
    )


async def _init_owned_embedded_v20_scan_resources(
    scan_state: V20CanonicalSelectionState,
) -> None:
    """Initialize the embedded V20 runtime's private market resources."""
    from src.common.config import get_tushare_token

    token = get_tushare_token()
    await _initialize_v20_selection_resources_once(
        scan_state,
        lambda: _init_v20_scan_resources_with_token(
            scan_state,
            token,
        ),
    )


async def _init_v20_scan_resources_with_token(
    scan_state: V20CanonicalSelectionState,
    token: str,
) -> None:
    from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter
    from src.data.clients.tushare_realtime import TushareRealtimeClient
    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig

    fundamentals = scan_state.fundamentals_db
    if fundamentals is None:
        raise V20ConfigError("V20 scan requires prevalidated fundamentals resources")
    tushare = TushareRealtimeClient(token=token)
    try:
        # ``start`` may allocate sockets before reporting a failure.  Keep it
        # inside the rollback boundary so every retry owns exactly one client.
        await tushare.start()
        await fundamentals.connect()
        historical_adapter = IQuantHistoricalAdapter(
            tushare,
            cache=scan_state.tushare_cache,
            tushare_token=token,
        )
        concept_mapper = LocalConceptMapper()
        stock_filter = StockFilter(
            StockFilterConfig(
                exclude_bse=True,
                exclude_chinext=True,
                exclude_star=True,
                exclude_sme=False,
            )
        )
        # Publish the resource set only after every constructor has succeeded;
        # a failed retry must not leave stopped/partial objects on shared state.
        scan_state.realtime_client = tushare
        scan_state.historical_adapter = historical_adapter
        scan_state.concept_mapper = concept_mapper
        scan_state.stock_filter = stock_filter
        scan_state.initialized = True
    except BaseException as initialization_error:
        cleanup_labels: list[str] = ["Tushare"]
        cleanup_operations: list[Awaitable[None]] = [tushare.stop()]
        cleanup_labels.insert(0, "fundamentals")
        cleanup_operations.insert(0, fundamentals.close())
        cleanup_results = await asyncio.gather(
            *cleanup_operations,
            return_exceptions=True,
        )
        for label, cleanup_result in zip(cleanup_labels, cleanup_results, strict=True):
            if isinstance(cleanup_result, BaseException):
                initialization_error.add_note(
                    f"V20 {label} cleanup also failed: "
                    f"{type(cleanup_result).__name__}: {cleanup_result}"
                )
                logger.error(
                    "V20 %s cleanup failed during initialization rollback",
                    label,
                    exc_info=(
                        type(cleanup_result),
                        cleanup_result,
                        cleanup_result.__traceback__,
                    ),
                )
        scan_state.initialized = False
        raise


async def _cleanup_v20_scan_resources(scan_state: V20CanonicalSelectionState) -> None:
    """Close only resources owned by V20; never touch legacy global workers."""
    await cleanup_v20_selection_resources(scan_state)


@dataclass
class _DayContext:
    trade_date: date
    calendar: tuple[date, ...]
    entry_status: EntryStatus | None = None
    prewarmed: V20PrewarmedScan | None = None
    canonical_bundle: FrozenV16ScanBundle | None = None
    canonical_first_received_at: datetime | None = None
    canonical_entry_mode: str | None = None
    canonical_entry_action: str | None = None
    collector: V20EarlyBarCollector | None = None
    breadth_collector: V20EarlyBarCollector | None = None
    collector_created_at: datetime | None = None
    minute_rows: dict[tuple[date, str, str], TushareMinuteBar] = field(default_factory=dict)
    minute_conflicts: set[tuple[date, str, str]] = field(default_factory=set)
    maturity_done: bool = False
    reminders_done: bool = False
    reference_finalized: bool = False
    shadow_reference_finalized: bool = False
    early_history_attempted: bool = False
    early_stored_history_loaded: bool = False
    last_rolling7_d0_history_at: datetime | None = None
    reference_history_attempted: bool = False
    last_reference_history_at: datetime | None = None
    last_reference_poll_at: datetime | None = None
    exit_history_last_attempt: dict[tuple[str, date], datetime] = field(default_factory=dict)

    exit_history_completed: set[tuple[str, date]] = field(default_factory=set)
    live_exit_market_data_outage: bool = False
    maturity_daily_last_attempt: dict[date, datetime] = field(default_factory=dict)
    reference_gap_history_last_at: dict[date, datetime] = field(default_factory=dict)
    last_early_poll_at: datetime | None = None
    last_exit_poll_at: datetime | None = None
    last_phase: str = "WAITING"
    last_entry_failure_detail: str | None = None
    late_0939_replay_last_attempt_at: datetime | None = None
    late_0939_replay_automatic_attempts: int = 0
    late_0939_replay_completed: bool = False
    # Retrospective scanner calls carry the artifact's immutable raw-fact
    # boundary on the same context object consumed by the normal scanner path.
    # Empty values mean a first/live computation may acquire missing evidence.
    canonical_fact_received_before: datetime | None = None
    canonical_fact_universe: tuple[str, ...] | None = None
    canonical_fact_evidence_codes: tuple[str, ...] | None = None
    canonical_fact_calendar: tuple[date, ...] | None = None
    canonical_fact_allow_backfill: bool = True
    canonical_fact_persist_raw: bool = True


@dataclass(frozen=True)
class _HistoricalCanonicalInputs:
    early_data_seed: Mapping[str, TushareEarlyMarketData]
    universe: tuple[str, ...]
    clean_boards: Mapping[str, Sequence[tuple[str, str]]]
    prev_closes: Mapping[str, float]
    history_raw: Mapping[str, Mapping[str, Any] | None]
    names: Mapping[str, str]
    calendar: tuple[date, ...]
    prior_daily: Mapping[str, TushareDailyBar]
    st_eligible_codes: tuple[str, ...]


@dataclass(frozen=True)
class _MorningSelectionComputation:
    """One complete V20 morning strategy calculation proposal.

    Both the live entry lane and every retrospective operator check consume
    this exact result.  Timeliness and persistence are deliberately absent:
    callers may decide whether the already-computed proposal can be committed
    or must remain read-only only after this object has been returned.

    Resolving a missing canonical input may durably materialize raw/canonical
    evidence.  It never writes the official decision/state, model batch/legs,
    holding, order, or exit intent; those effects belong to the caller's
    post-computation branch.
    """

    prepared: PreparedEntry
    bundle: FrozenV16ScanBundle
    canonical_first_received_at: datetime
    calendar: tuple[date, ...]
    scheduled_exits_today: tuple[Mapping[str, Any], ...]
    canonical_source: str
    canonical_artifact_compared: bool
    canonical_artifact_matches: bool | None
    legacy_terminal_fresh_theoretical: bool


_ENTRY_BUSINESS_SEMANTIC_FIELDS = (
    "action",
    "base_multiplier",
    "defense_multiplier",
    "final_multiplier",
    "health_state",
    "health_recovery_count",
    "health_trailing_mean",
    "breadth_valid_n",
    "breadth_down_n",
    "breadth_wilson_lower",
    "rolling7_state",
    "rolling7_r7",
    "rolling7_l7",
    "rolling7_reason",
    "rolling7_window_ids",
    "g_state",
    "g_max_component_size",
    "g_amount_below_q25_count",
    "reason_codes",
    "last_complete_bar",
    "reference_profile_id",
    "return_profile_id",
    "v16_funnel",
    "v16_board_avg_gains",
    "symbols",
    "scheduled_exits_today",
)


class V20LiveExitStageTimeout(RuntimeError):
    """A live-exit stage exceeded its share of the tick's monotonic deadline."""

    def __init__(
        self,
        *,
        stage: str,
        elapsed_seconds: float,
        remaining_seconds: float,
        deadline: float,
        symbols: tuple[str, ...],
        provider: str,
    ) -> None:
        super().__init__(f"live-exit stage {stage} exceeded its budget")
        self.stage = stage
        self.elapsed_seconds = elapsed_seconds
        self.remaining_seconds = remaining_seconds
        self.deadline = deadline
        self.symbols = symbols
        self.provider = provider
        self.diagnostic_alert_emitted: bool | None = False


class V20LiveExitIncidentError(V20RepositoryError):
    """A live-exit failure whose more specific durable alert was attempted."""

    def __init__(
        self,
        message: str,
        *,
        diagnostic_alert_emitted: bool | None,
    ) -> None:
        super().__init__(message)
        self.diagnostic_alert_emitted = diagnostic_alert_emitted


@dataclass
class _RuntimeLaneHealth:
    last_success_at: datetime | None = None
    last_error: str | None = None
    last_error_at: datetime | None = None
    error_revision: int = 0


@dataclass(frozen=True)
class _BootstrapBundle:
    state: Mapping[str, Any]
    predecessor_trade_date: date
    shadow_batches: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True)
class _ReferenceArbitration:
    exact_bars: Mapping[str, TushareMinuteBar]
    prices: Mapping[str, float]
    missing_codes: tuple[str, ...]
    conflict_codes: tuple[str, ...]
    source_hash: str


def _now_shanghai() -> datetime:
    return datetime.now(SHANGHAI)


def _local(day: date, wall_time: time) -> datetime:
    return datetime.combine(day, wall_time, tzinfo=SHANGHAI)


def _next_trade_date(calendar: Sequence[date], current: date) -> date:
    try:
        position = calendar.index(current)
    except ValueError as exc:
        raise V20RepositoryError(
            f"trade date {current.isoformat()} is absent from the exchange calendar"
        ) from exc
    if position + 1 >= len(calendar):
        raise V20RepositoryError(f"exchange calendar has no successor for {current.isoformat()}")
    return calendar[position + 1]


def _exit_labels() -> tuple[str, ...]:
    morning = tuple(
        f"{minute // 60:02d}:{minute % 60:02d}" for minute in range(9 * 60 + 31, 11 * 60 + 31)
    )
    afternoon = tuple(
        f"{minute // 60:02d}:{minute % 60:02d}" for minute in range(13 * 60 + 1, 14 * 60 + 58)
    )
    return (*morning, *afternoon)


FULL_EXIT_LABELS = _exit_labels()


def _expected_exit_labels(trade_date: date, as_of: datetime) -> tuple[str, ...]:
    local_as_of = as_of.astimezone(SHANGHAI)
    return tuple(
        label
        for label in FULL_EXIT_LABELS
        if _local(trade_date, time.fromisoformat(label)) < local_as_of
    )


def _live_exit_health_labels(
    trade_date: date,
    as_of: datetime,
    expected_labels: Iterable[str],
) -> tuple[str, ...]:
    """Return the labels that can prove feed health at this wall-clock instant."""

    labels = frozenset(expected_labels)
    if not labels:
        return ()
    freshest = max(labels)
    morning_close = _local(trade_date, time(11, 30))
    publication_deadline = morning_close + timedelta(
        seconds=LIVE_EXIT_MORNING_CLOSE_PUBLICATION_GRACE_SECONDS
    )
    local_as_of = as_of.astimezone(SHANGHAI)
    if (
        freshest == "11:30"
        and "11:29" in labels
        and morning_close < local_as_of
        and local_as_of < publication_deadline
    ):
        # Keep staging/evaluating 11:30 immediately when it is available, but
        # do not call a feed outage solely because the vendor is still
        # publishing that terminal bar during the bounded grace window.
        return ("11:29", "11:30")
    return (freshest,)


def _exit_window_complete(
    bars: Sequence[MinuteBar],
    trade_date: date,
    *,
    as_of: datetime,
) -> bool:
    expected = frozenset(_expected_exit_labels(trade_date, as_of))
    if not expected:
        return False
    observed = {
        bar.end_ts.astimezone(SHANGHAI).strftime("%H:%M")
        for bar in bars
        if bar.end_ts.astimezone(SHANGHAI).date() == trade_date
        and is_valid_complete_minute_bar(bar)
    }
    return expected.issubset(observed)


def _complete_history_evidence(
    code: str,
    trade_date: date,
    rows: Sequence[TushareMinuteBar],
    *,
    scanned_through_label: str,
    profile: str,
) -> tuple[bool, str]:
    """Hash a source scan and prove every expected label exists exactly once."""

    expected = tuple(label for label in FULL_EXIT_LABELS if label <= scanned_through_label)
    grouped: dict[str, list[TushareMinuteBar]] = {}
    for bar in rows:
        local = bar.bar_end.astimezone(SHANGHAI)
        if (
            bar.stock_code == code
            and local.date() == trade_date
            and bar.end_label <= scanned_through_label
        ):
            grouped.setdefault(bar.end_label, []).append(bar)
    complete = all(
        len(grouped.get(label, ())) == 1
        and grouped[label][0].is_valid
        and grouped[label][0].volume > 0
        and grouped[label][0].amount > 0
        for label in expected
    )
    source_rows = [
        _bar_payload(grouped[label][0]) for label in expected if len(grouped.get(label, ())) == 1
    ]
    return complete, sha256_json(
        {
            "profile": profile,
            "code": code,
            "trade_date": trade_date.isoformat(),
            "scanned_through_label": scanned_through_label,
            "expected_label_n": len(expected),
            "complete": complete,
            "bars": source_rows,
        }
    )


def _legal_exit_evidence_codes(
    rows: Iterable[TushareMinuteBar],
    *,
    trade_date: date,
    expected_labels: Iterable[str],
) -> frozenset[str]:
    """Return codes backed by a persisted, strategy-legal current-day bar."""

    labels = frozenset(expected_labels)
    result: set[str] = set()
    for bar in rows:
        local = bar.bar_end.astimezone(SHANGHAI)
        if (
            local.date() == trade_date
            and bar.end_label in labels
            and bar.is_valid
            and bar.volume > 0
            and bar.amount > 0
        ):
            result.add(bar.stock_code)
    return frozenset(result)


def _bar_payload(bar: TushareMinuteBar) -> dict[str, Any]:
    return {
        "stock_code": bar.stock_code,
        "bar_end": bar.bar_end.astimezone(SHANGHAI).isoformat(),
        "end_label": bar.end_label,
        "open": bar.open_price,
        "high": bar.high_price,
        "low": bar.low_price,
        "close": bar.close_price,
        "volume": bar.volume,
        "amount": bar.amount,
        "source_confirms_complete": True,
        "source_adapter_id": "TUSHARE_RT_MIN_V20_V1",
    }


def _daily_snapshot_payload(
    trade_date: date,
    rows: Mapping[str, TushareDailyBar],
) -> dict[str, Any]:
    expected = trade_date.strftime("%Y%m%d")
    bars: dict[str, dict[str, Any]] = {}
    for code, row in sorted(rows.items()):
        if code != row.stock_code or row.trade_date != expected:
            raise ValueError("daily snapshot code/date binding mismatch")
        if (
            len(code) != 6
            or not code.isdigit()
            or not math.isfinite(row.close_price)
            or row.close_price <= 0
            or not math.isfinite(row.amount_yuan)
            or row.amount_yuan <= 0
        ):
            raise ValueError("daily snapshot contains an invalid bar")
        bars[code] = {
            "stock_code": code,
            "trade_date": expected,
            "close_price": float(row.close_price),
            "amount_yuan": float(row.amount_yuan),
        }
    return {
        "profile": "TUSHARE_DAILY_RAW_RECEIPT_V1",
        "trade_date": trade_date.isoformat(),
        "bars": bars,
    }


def _daily_rows_from_snapshot(payload: Mapping[str, Any]) -> dict[str, TushareDailyBar]:
    raw_bars = payload.get("bars")
    if not isinstance(raw_bars, Mapping):
        raise V20SemanticConflict("persisted daily snapshot bars are invalid")
    result: dict[str, TushareDailyBar] = {}
    for raw_code, raw in raw_bars.items():
        code = str(raw_code)
        if not isinstance(raw, Mapping) or set(raw) != {
            "stock_code",
            "trade_date",
            "close_price",
            "amount_yuan",
        }:
            raise V20SemanticConflict("persisted daily snapshot row shape is invalid")
        try:
            row = TushareDailyBar(
                stock_code=str(raw["stock_code"]),
                trade_date=str(raw["trade_date"]),
                close_price=float(raw["close_price"]),
                amount_yuan=float(raw["amount_yuan"]),
            )
        except (TypeError, ValueError, OverflowError) as exc:
            raise V20SemanticConflict("persisted daily snapshot row is invalid") from exc
        if (
            code != row.stock_code
            or len(code) != 6
            or not code.isdigit()
            or not math.isfinite(row.close_price)
            or row.close_price <= 0
            or not math.isfinite(row.amount_yuan)
            or row.amount_yuan <= 0
        ):
            raise V20SemanticConflict("persisted daily snapshot value is invalid")
        result[code] = row
    return result


def _minute_from_record(payload: Mapping[str, Any]) -> MinuteBar:
    raw_end = payload.get("bar_end")
    end_ts = raw_end if isinstance(raw_end, datetime) else datetime.fromisoformat(str(raw_end))
    return MinuteBar(
        code=str(payload["stock_code"]),
        end_ts=end_ts,
        open=float(payload["open"]) if payload.get("open") is not None else None,
        high=float(payload["high"]) if payload.get("high") is not None else None,
        low=float(payload["low"]) if payload.get("low") is not None else None,
        close=float(payload["close"]) if payload.get("close") is not None else None,
        volume=float(payload["volume"]) if payload.get("volume") is not None else None,
        amount=float(payload["amount"]) if payload.get("amount") is not None else None,
        source_confirms_complete=bool(payload.get("source_confirms_complete", False)),
    )


def _tushare_minute_from_record(payload: Mapping[str, Any]) -> TushareMinuteBar:
    raw_end = payload.get("bar_end")
    bar_end = raw_end if isinstance(raw_end, datetime) else datetime.fromisoformat(str(raw_end))
    return TushareMinuteBar(
        stock_code=str(payload["stock_code"]),
        bar_end=bar_end,
        end_label=str(payload["end_label"]),
        open_price=float(payload["open"]),
        high_price=float(payload["high"]),
        low_price=float(payload["low"]),
        close_price=float(payload["close"]),
        volume=float(payload["volume"]),
        amount=float(payload["amount"]),
    )


def _arbitrate_reference_records(
    records: Sequence[Any],
    required_codes: Sequence[str],
    *,
    trade_date: date,
    expected_label: str,
) -> _ReferenceArbitration:
    """Choose the latest durable legal reference revision for each code.

    Illegal raw candidates remain persisted for audit, but are filtered before
    ordering.  An equal-receipt tie between different legal payloads is an
    irreducible conflict and fails only that code closed.
    """

    required = tuple(sorted(set(required_codes)))
    legal: dict[str, list[tuple[datetime, str, TushareMinuteBar]]] = {}
    for record in records:
        code = str(getattr(record, "code", ""))
        if code not in required:
            continue
        payload = getattr(record, "payload", None)
        source_hash = str(getattr(record, "source_hash", ""))
        if not isinstance(payload, Mapping) or sha256_json(payload) != source_hash:
            raise V20SemanticConflict(f"reference payload hash mismatch for {code}")
        received_at = getattr(record, "first_received_at", None)
        if not isinstance(received_at, datetime) or received_at.tzinfo is None:
            raise V20SemanticConflict(f"reference receipt is invalid for {code}")
        try:
            bar = _tushare_minute_from_record(payload)
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        one_price, missing, _one_hash = exact_reference_prices(
            {code: bar},
            (code,),
            trade_date=trade_date,
            expected_label=expected_label,
        )
        if missing or code not in one_price:
            continue
        legal.setdefault(code, []).append((received_at, source_hash, bar))

    exact: dict[str, TushareMinuteBar] = {}
    conflicts: list[str] = []
    for code, candidates in legal.items():
        latest_receipt = max(item[0] for item in candidates)
        latest = [item for item in candidates if item[0] == latest_receipt]
        if len({item[1] for item in latest}) != 1:
            conflicts.append(code)
            continue
        exact[code] = min(latest, key=lambda item: item[1])[2]
    prices, missing, source_hash = exact_reference_prices(
        exact,
        required,
        trade_date=trade_date,
        expected_label=expected_label,
    )
    return _ReferenceArbitration(
        exact_bars=exact,
        prices=prices,
        missing_codes=missing,
        conflict_codes=tuple(sorted(conflicts)),
        source_hash=source_hash,
    )


def _mews_snapshot(record: SelectedMewsRecord | None) -> tuple[MewsSnapshot, ...]:
    if record is None or record.snapshot_id is None:
        return ()
    availability_date: date | None = None
    if isinstance(record.payload, Mapping):
        evidence = record.payload.get("evidence")
        if isinstance(evidence, Mapping) and evidence.get("signal_available_date") is not None:
            availability_date = date.fromisoformat(str(evidence["signal_available_date"]))
    return (
        MewsSnapshot(
            source_trade_date=record.source_trade_date,
            generated_at=record.generated_at,
            # Exit policy consumes the first durable receipt, not the earlier
            # unsealed inbox timestamp.
            received_at=record.receipt_sealed_at,
            fast_state=record.fast_state,
            model_version=record.model_version,
            data_version=record.data_version,
            snapshot_id=record.snapshot_id,
            availability_date=availability_date,
        ),
    )


# Retired checkpoint provenance fields.  Historical v2 exports still carry
# them and their values are never consulted for authorization; the v3 exporter
# no longer emits them, so a v3 checkpoint containing one is malformed.
# Early v2 exports carried only source_config_hash and
# source_state_semantics_hash; resolved_state_semantics_hash was added by the
# later v2 exporter, so both enumerated v2 keysets are accepted below.
RETIRED_CHECKPOINT_FIELDS: frozenset[str] = frozenset(
    {
        "source_config_hash",
        "source_state_semantics_hash",
        "resolved_state_semantics_hash",
    }
)
BOOTSTRAP_CHECKPOINT_V3_KEYS: frozenset[str] = frozenset(
    {
        "schema_version",
        "target_official_stream_id",
        "state_lineage_id",
        "source_official_stream_id",
        "source_lineage_id",
        "as_of_trade_date",
        "source_state_revision",
        "source_state_hash",
        "source_bootstrap_mode",
        "source_bootstrap_checkpoint_hash",
        "source_last_terminal_slot_id",
        "source_last_terminal_trade_date",
        "batch_id_migration",
        "official_state",
        "official_state_hash",
        "state_shadow_batches",
    }
)
BOOTSTRAP_CHECKPOINT_V2_EARLY_KEYS: frozenset[str] = BOOTSTRAP_CHECKPOINT_V3_KEYS | {
    "source_config_hash",
    "source_state_semantics_hash",
}
BOOTSTRAP_CHECKPOINT_V2_KEYS: frozenset[str] = BOOTSTRAP_CHECKPOINT_V2_EARLY_KEYS | {
    "resolved_state_semantics_hash",
}
BOOTSTRAP_CHECKPOINT_KEYS: Mapping[str, tuple[frozenset[str], ...]] = {
    "v20-bootstrap-checkpoint/v3": (BOOTSTRAP_CHECKPOINT_V3_KEYS,),
    "v20-bootstrap-checkpoint/v2": (
        BOOTSTRAP_CHECKPOINT_V2_EARLY_KEYS,
        BOOTSTRAP_CHECKPOINT_V2_KEYS,
    ),
}


def _bootstrap_bundle(
    config: V20RuntimeConfig,
    *,
    empty_predecessor_trade_date: date,
) -> _BootstrapBundle:
    if config.bootstrap_mode == "EMPTY_FORWARD_SHADOW":
        if config.deployment_mode != "forward_shadow":
            raise V20ConfigError("empty V20 genesis is restricted to forward shadow")
        return _BootstrapBundle(genesis_state(), empty_predecessor_trade_date)
    path = config.bootstrap_checkpoint_path
    if path is None:
        raise V20ConfigError("V20 checkpoint path is missing")
    try:
        checkpoint = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise V20ConfigError(f"cannot read V20 bootstrap checkpoint: {exc}") from exc
    if not isinstance(checkpoint, Mapping):
        raise V20ConfigError("V20 checkpoint root must be an object")
    schema_version = checkpoint.get("schema_version")
    if not isinstance(schema_version, str):
        raise V20ConfigError("unsupported V20 checkpoint schema")
    checkpoint_keysets = BOOTSTRAP_CHECKPOINT_KEYS.get(schema_version)
    if checkpoint_keysets is None:
        raise V20ConfigError("unsupported V20 checkpoint schema")
    if all(set(checkpoint) != keyset for keyset in checkpoint_keysets):
        raise V20ConfigError("V20 checkpoint top-level field set mismatch for its schema")
    if checkpoint.get("target_official_stream_id") != config.official_stream_id:
        raise V20ConfigError("V20 checkpoint target stream does not match active config")
    if checkpoint.get("state_lineage_id") != config.state_lineage_id:
        raise V20ConfigError("V20 checkpoint lineage does not match active config")
    raw_as_of = checkpoint.get("as_of_trade_date")
    if not isinstance(raw_as_of, str):
        raise V20ConfigError("V20 checkpoint as_of_trade_date is missing")
    try:
        predecessor_trade_date = date.fromisoformat(raw_as_of)
    except ValueError as exc:
        raise V20ConfigError("V20 checkpoint as_of_trade_date is invalid") from exc
    if checkpoint.get("source_last_terminal_trade_date") != raw_as_of:
        raise V20ConfigError(
            "V20 checkpoint as-of boundary does not match its source terminal date"
        )
    state = checkpoint.get("official_state")
    if not isinstance(state, Mapping):
        raise V20ConfigError("V20 checkpoint official_state is missing")
    if state.get("schema_version") != "v20-official-state/v1":
        raise V20ConfigError("V20 checkpoint official_state schema is unsupported")
    expected_hash = checkpoint.get("official_state_hash")
    if not isinstance(expected_hash, str) or sha256_json(state) != expected_hash:
        raise V20ConfigError("V20 checkpoint official_state_hash mismatch")
    if state.get("state_revision") != 0:
        raise V20ConfigError("target V20 checkpoint must start at state_revision=0")
    if set(state) != set(genesis_state()):
        raise V20ConfigError("V20 checkpoint official_state field set mismatch")
    shadow_batches = checkpoint.get("state_shadow_batches")
    if not isinstance(shadow_batches, list):
        raise V20ConfigError("V20 checkpoint state_shadow_batches is missing")
    if any(not isinstance(item, Mapping) for item in shadow_batches):
        raise V20ConfigError("V20 checkpoint shadow batch must be an object")
    # ROLLING7 now has its own durable, lineage-independent fact stream.
    # Historical checkpoints may still contain legacy rolling rows and gaps;
    # retain the schema fields for compatibility but never import those facts.
    bootstrap_state = dict(state)
    bootstrap_state["official_rolling_gaps"] = []
    health_batches = tuple(dict(item) for item in shadow_batches if item.get("kind") != "ROLLING7")
    return _BootstrapBundle(
        bootstrap_state,
        predecessor_trade_date,
        health_batches,
    )


def _late_0939_replay_body(
    *,
    official_status: EntryStatus,
    replay_semantic: Mapping[str, Any],
) -> str:
    """Render a retrospective result that can never be mistaken for a buy notice."""

    def percent(value: object) -> str:
        return f"{float(value):.0%}" if isinstance(value, (int, float)) else "-"

    r7 = replay_semantic.get("rolling7_r7")
    r7_text = f"{float(r7):.2%}" if isinstance(r7, (int, float)) else "-"
    lines = [
        "⛔ 已过期不可追买；这不是交易指令，也不会创建模型持仓、退出信号或订单。",
        "用途: 截止后重拉并截取当日 raw 早盘(≤09:39)证据，回答策略在该快照上会如何判断。",
        "口径: RETROSPECTIVE（数据在截止后取得，不宣称当时已按时形成或送达）。",
        "提示: 消息投递ON_TIME只表示复盘通知及时送达，不表示09:39决策曾按时生成。",
        (
            "状态口径: 当前已部署 runtime lineage "
            f"(bootstrap={replay_semantic.get('bootstrap_mode', '-')})；"
            "不冒充另一份研究回测或未导入的checkpoint。"
        ),
        f"正式实时结果: {official_status.action} | 正式事件: {official_status.event_id}",
        (
            f"复盘判断: {replay_semantic.get('replay_action', '-')} | "
            f"最终倍率: {percent(replay_semantic.get('final_multiplier'))}"
        ),
        (
            f"BASE: {replay_semantic.get('health_state', '-')} / "
            f"基础倍率 {percent(replay_semantic.get('base_multiplier'))}"
        ),
        (
            f"滚动7: {replay_semantic.get('rolling7_state', '-')} | "
            f"R7={r7_text} | 亏损批次={replay_semantic.get('rolling7_l7', '-')}"
        ),
        (
            f"极端门G: {replay_semantic.get('g_state', 'NOT_EVALUATED')} | "
            f"防御倍率 {percent(replay_semantic.get('defense_multiplier'))}"
        ),
    ]
    reasons = replay_semantic.get("reason_codes") or []
    if reasons:
        lines.append("原因: " + " / ".join(str(item) for item in reasons))
    symbols = replay_semantic.get("symbols") or []
    if isinstance(symbols, list) and symbols:
        lines.append(f"09:39 V16 复盘票单（{len(symbols)}只）:")
        for index, item in enumerate(symbols, start=1):
            if not isinstance(item, Mapping):
                continue
            score = item.get("score")
            price = item.get("snapshot_price")
            score_text = f"{float(score):.4f}" if isinstance(score, (int, float)) else "-"
            price_text = f"{float(price):.2f}" if isinstance(price, (int, float)) else "-"
            boards = item.get("boards") or []
            board_text = "、".join(str(board) for board in boards) if boards else "-"
            lines.append(
                f"{item.get('rank', index)}. {item.get('code', '-')} "
                f"{item.get('name', '')}  LGB={score_text}  09:39={price_text}  {board_text}"
            )
    else:
        lines.append("09:39 V16 复盘票单: 无")
    breadth_mode = replay_semantic.get("breadth_replay_mode", "-")
    lines.append(f"宽度取数: {breadth_mode}")
    lines.append(f"复盘输入哈希: {replay_semantic.get('v16_snapshot_hash', '-')}")
    return "\n".join(lines)


def _manual_trigger_receipt_body(
    *,
    request_id: str,
    cycle_result: str,
    status: EntryStatus | None,
    late_replay: Mapping[str, Any] | None = None,
    late_replay_error: str | None = None,
) -> str:
    """Render a stable, explicitly non-actionable deployment receipt."""

    lines = [
        "仅用于部署验收；不会创建或修改订单、持仓、卖出信号或券商侧状态。",
        f"幂等请求: {request_id}",
        f"本轮结果: {cycle_result}",
    ]
    if status is None:
        lines.extend(
            [
                "今日正式冻结决策: 不可用",
                "没有可读取的 exact-09:39 正式结果；本接口不会使用晚到行情补算。",
            ]
        )
        return "\n".join(lines)

    semantic = status.semantic
    reasons = semantic.get("reason_codes") or []
    lines.extend(
        [
            f"今日正式冻结决策: {status.action}",
            f"正式策略倍率: {status.final_multiplier:.0%}",
            f"正式事件: {status.event_id}",
            (
                "状态: "
                f"BASE={semantic.get('health_state', '-')} / "
                f"滚动7={semantic.get('rolling7_state', '-')} / "
                f"极端闸门={semantic.get('g_state', 'NOT_EVALUATED')}"
            ),
        ]
    )
    if reasons:
        lines.append("原因: " + " / ".join(str(item) for item in reasons))

    symbols = semantic.get("symbols") or []
    if isinstance(symbols, list) and symbols:
        lines.append(f"正式冻结 V16 票单（{len(symbols)}只）:")
        for index, item in enumerate(symbols, start=1):
            if not isinstance(item, Mapping):
                continue
            rank = item.get("rank", index)
            code = item.get("code", "-")
            name = item.get("name", "")
            score = item.get("score")
            snapshot_price = item.get("snapshot_price")
            boards = item.get("boards") or []
            score_text = f"{float(score):.4f}" if isinstance(score, (int, float)) else "-"
            price_text = (
                f"{float(snapshot_price):.2f}" if isinstance(snapshot_price, (int, float)) else "-"
            )
            board_text = "、".join(str(board) for board in boards) if boards else "-"
            lines.append(
                f"{rank}. {code} {name}  LGB={score_text}  09:39={price_text}  {board_text}"
            )
    else:
        lines.append("正式冻结 V16 票单: 无")
    if late_replay is not None:
        lines.extend(
            [
                "截止后09:39复盘: 已完成（独立审计事件，已过期不可追买）",
                (
                    f"复盘判断: {late_replay.get('replay_action', '-')} / "
                    f"最终倍率 {float(late_replay.get('final_multiplier', 0.0)):.0%}"
                ),
                f"复盘事件: {late_replay.get('event_id', '-')}",
            ]
        )
    elif late_replay_error is not None:
        lines.append(f"截止后09:39复盘: 本次未完成（{late_replay_error}）")
    lines.append("以上仅为正式持久化结果的只读验收副本；人工回执本身不是交易指令。")
    return "\n".join(lines)


def _manual_trigger_response(
    record: OutboxRecord,
    *,
    created: bool,
    request_id: str,
    config: V20RuntimeConfig,
) -> dict[str, Any]:
    semantic = record.semantic
    expected_bindings = {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": record.event_id,
        "strategy_version": config.strategy_version,
        "state_semantics_hash": config.state_semantics_hash,
        "deployment_mode": config.deployment_mode,
        "official_stream_id": config.official_stream_id,
        "state_lineage_id": config.state_lineage_id,
        "alert_code": "MANUAL_TRIGGER_RECEIPT",
        "delivery_priority_class": "OPERATOR_NOTIFICATION",
        "manual_request_id": request_id,
        "non_actionable": True,
    }
    expected_record_scope = (
        config.route_id,
        config.official_stream_id,
        config.state_lineage_id,
    )
    if (
        record.event_type != "DATA_ALERT"
        or (record.route_id, record.official_stream_id, record.lineage_id) != expected_record_scope
        or any(semantic.get(key) != value for key, value in expected_bindings.items())
        or not isinstance(semantic.get("config_hash"), str)
        or re.fullmatch(r"[0-9a-f]{64}", str(semantic.get("config_hash"))) is None
    ):
        raise V20SemanticConflict("manual trigger event has incompatible persisted semantics")
    if not isinstance(semantic.get("cycle_result"), str) or not semantic["cycle_result"]:
        raise V20SemanticConflict("manual trigger event has an invalid cycle result")
    if not isinstance(semantic.get("formal_decision_available"), bool) or not isinstance(
        semantic.get("official_state_changed"), bool
    ):
        raise V20SemanticConflict("manual trigger event has invalid decision flags")
    formal_available = semantic["formal_decision_available"]
    formal_fields = (
        semantic.get("entry_action"),
        semantic.get("entry_event_id"),
        semantic.get("formal_semantic_hash"),
    )
    if (
        (
            formal_available
            and any(not isinstance(value, str) or not value for value in formal_fields)
        )
        or (not formal_available and any(value is not None for value in formal_fields))
        or (semantic["official_state_changed"] and not formal_available)
    ):
        raise V20SemanticConflict("manual trigger event has inconsistent formal decision binding")
    try:
        date.fromisoformat(str(semantic["event_trade_date"]))
    except (KeyError, ValueError) as exc:
        raise V20SemanticConflict("manual trigger event has an invalid trade date") from exc
    replay_available = semantic.get("late_0939_replay_available", False)
    if not isinstance(replay_available, bool):
        raise V20SemanticConflict("manual trigger event has an invalid replay flag")
    replay_fields = (
        semantic.get("late_0939_replay_event_id"),
        semantic.get("late_0939_replay_action"),
    )
    if replay_available and any(not isinstance(value, str) or not value for value in replay_fields):
        raise V20SemanticConflict("manual trigger event has an invalid replay binding")
    if replay_available:
        replay_multiplier = semantic.get("late_0939_replay_multiplier")
        if (
            isinstance(replay_multiplier, bool)
            or not isinstance(replay_multiplier, (int, float))
            or not math.isfinite(float(replay_multiplier))
            or not 0 <= float(replay_multiplier) <= 1
        ):
            raise V20SemanticConflict("manual trigger event has an invalid replay multiplier")
    elif (
        any(value is not None for value in replay_fields)
        or semantic.get("late_0939_replay_multiplier") is not None
    ):
        raise V20SemanticConflict("manual trigger event has inconsistent replay fields")
    return {
        "accepted": True,
        "created": created,
        "manual_request_id": str(semantic["manual_request_id"]),
        "manual_event_id": record.event_id,
        "trade_date": str(semantic["event_trade_date"]),
        "cycle_result": str(semantic["cycle_result"]),
        "formal_decision_available": bool(semantic["formal_decision_available"]),
        "entry_action": semantic.get("entry_action"),
        "entry_event_id": semantic.get("entry_event_id"),
        "official_state_changed": bool(semantic["official_state_changed"]),
        "manual_notice_actionable": False,
        "late_0939_replay_available": replay_available,
        "late_0939_replay_event_id": semantic.get("late_0939_replay_event_id"),
        "late_0939_replay_action": semantic.get("late_0939_replay_action"),
        "late_0939_replay_multiplier": semantic.get("late_0939_replay_multiplier"),
        "late_0939_replay_error": semantic.get("late_0939_replay_error"),
        "sealed": record.payload is not None,
        "delivery_status": record.delivery_status,
        "feishu_delivery_confirmed": record.delivery_status == "SENT",
    }


def _embedded_runtime_config(
    base: V20RuntimeConfig,
    route: V20FeishuRoute,
) -> V20RuntimeConfig:
    """Activate a reviewed shadow config against the legacy main destination."""
    if route.route_id != base.route_id or base.deployment_mode != "forward_shadow":
        raise V20ConfigError("embedded V20 route does not match forward-shadow identity")
    binding = V20RouteBinding(
        route_id=route.route_id,
        expected_bot_origin=route.bot_origin,
        expected_app_id_sha256=hashlib.sha256(route.app_id.strip().encode("utf-8")).hexdigest(),
        expected_chat_id_sha256=hashlib.sha256(route.chat_id.strip().encode("utf-8")).hexdigest(),
    )
    if route.destination_fingerprint != binding.destination_fingerprint:
        raise V20ConfigError("embedded V20 route fingerprint is inconsistent")

    route_bindings = {**base.route_bindings, "forward_shadow": binding}
    frozen_payload = {
        **base.frozen_payload,
        "integration_profile": "legacy_main_embedded/v1",
        "route_id": route.route_id,
        "route_bindings": {
            name: route_binding.as_payload() for name, route_binding in route_bindings.items()
        },
    }
    return replace(
        base,
        enabled=True,
        route_bindings=route_bindings,
        route_binding=binding,
        frozen_payload=frozen_payload,
        config_hash=sha256_json(frozen_payload),
    )


class V20Service:
    """Own the causal V20 scheduler and its durable notification boundary."""

    def __init__(
        self,
        *,
        config: V20RuntimeConfig,
        repository: V20Repository,
        scan_state: V20CanonicalSelectionState,
        artifacts: GArtifactBundle,
        publisher: V20OutboxPublisher,
        routes: Mapping[str, V20FeishuRoute],
        clock: Clock = _now_shanghai,
        initialize_resources: ResourceInitializer = _init_v20_scan_resources,
        cleanup_resources: ResourceCleanup = _cleanup_v20_scan_resources,
        calendar_provider: CalendarProvider | None = None,
        mews_source: Any | None = None,
        embedded_legacy: bool = False,
    ) -> None:
        self.config = config
        self._repository = repository
        self._scan_state = scan_state
        self._artifacts = artifacts
        self._publisher = publisher
        self._routes = dict(routes)
        self._clock = clock
        self._initialize_resources = initialize_resources
        self._cleanup_resources = cleanup_resources
        self._calendar_provider = calendar_provider
        self._mews_source = mews_source
        self._embedded_legacy = embedded_legacy
        self._calendar_cache: tuple[date, ...] = ()
        self._calendar_loaded_for: date | None = None
        self._calendar_tasks: dict[date, asyncio.Task[tuple[date, ...]]] = {}
        self._calendar_tasks_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[Any]] = []
        self._decision_cycle_lock = asyncio.Lock()
        self._manual_trigger_lock = asyncio.Lock()
        self._manual_monitor_lock = asyncio.Lock()
        self._late_0939_replay_lock = asyncio.Lock()
        self._late_0939_replay_task: asyncio.Task[Any] | None = None
        self._canonical_artifact_store: Any | None = None
        self._canonical_callbacks_open = False
        self._canonical_artifact_lock = asyncio.Lock()
        self._canonical_barrier_completed_at: dict[date, datetime] = {}
        # Trade dates whose canonical early (<=09:39) raw bars are already durably
        # persisted by this process; same-process cache reuse never re-runs it.
        self._canonical_raw_persisted_dates: set[date] = set()
        self._current_day_early_attempt_lock = asyncio.Lock()
        # ``rt_min_daily`` is one physical request per symbol.  The concrete
        # client already retries each symbol, so the service must never restart
        # the complete market fan-out inside the same provider minute.  Keep a
        # completed *or failed* task until the minute changes; all same-minute
        # scheduled/manual contenders observe that one attempt.
        self._current_day_early_attempts: dict[
            tuple[date, datetime],
            tuple[
                frozenset[str],
                asyncio.Task[Mapping[str, TushareEarlyMarketData]],
            ],
        ] = {}
        self._live_exit_lock = asyncio.Lock()
        self._mews_refresh_lock = asyncio.Lock()
        self._mews_singleflight_lock = asyncio.Lock()
        # One shared per-date MEWS attempt joined by the 09:10 scheduler and by
        # every missing-cache selection trigger; cleared once finished so a
        # later trigger retries while the cache is still missing.
        self._mews_singleflight_task: asyncio.Task[bool] | None = None
        self._mews_singleflight_date: date | None = None
        self._mews_singleflight_source_trade_date: date | None = None
        self._mews_trigger_tasks: set[asyncio.Task[bool]] = set()
        try:
            self._mews_guard_store: V20MewsGuardStore | None = (
                V20MewsGuardStore(repository)
                if self._supports_strict_mews_guard(repository)
                else None
            )
        except (AttributeError, RuntimeError, TypeError, ValueError):
            # A real V20 repository exposes its pool only after ``connect``;
            # lightweight unit repositories deliberately do not expose the
            # PostgreSQL surface at all.  Startup retries construction after
            # the writer connection is established.
            self._mews_guard_store = None
        self._mews_cached_for: date | None = None
        self._mews_source_trade_date: date | None = None
        self._mews_snapshot_id: str | None = None
        self._mews_last_failure: str | None = None
        self._mews_alerted_for: date | None = None
        self._mews_failed_for: date | None = None
        self._rolling7_recovery_lock = asyncio.Lock()
        self._rolling7_canonical_bootstrap_lock = asyncio.Lock()
        self._rolling7_recovery_last_at: datetime | None = None
        self._rolling7_recovery_cursor: date | None = None
        self._exit_context: _DayContext | None = None
        self._stale_exit_context: _DayContext | None = None
        self._started = False
        self._resources_started = False
        self._repository_started = False
        self._context: _DayContext | None = None
        self._last_error: str | None = None
        self._last_error_at: datetime | None = None
        self._last_success_at: datetime | None = None
        self._status_snapshot: Mapping[str, Any] | None = None
        self._status_snapshot_error: str | None = None
        self._startup_stage = "NOT_STARTED"
        self._lane_health = {
            name: _RuntimeLaneHealth()
            for name in (
                "decision",
                "live_exit",
                "stale_exit",
                "outbox_recovery",
                "publisher",
                "mews_cache",
            )
        }

    @classmethod
    def from_default_config(cls) -> V20Service:
        from src.data.database.fundamentals_db import create_fundamentals_db_from_config

        project_root = Path(__file__).resolve().parents[2]
        config = load_v20_runtime_config(project_root)
        database_config_path = project_root / "config" / "database-config.yaml"
        repository = create_v20_repository_from_config(database_config_path)
        if (
            repository.config.schema != config.database_schema
            or repository.config.pool_min_size != config.database_pool_min_size
            or repository.config.pool_max_size != config.database_pool_max_size
        ):
            raise V20ConfigError(
                "config/v20.yaml database schema/pool settings do not match "
                "config/database-config.yaml database.v20"
            )
        resolved_scan_state = V20CanonicalSelectionState()
        fundamentals = None
        if config.enabled:
            token = validated_v20_tushare_token()
            if fundamentals is None:
                fundamentals = create_fundamentals_db_from_config(
                    database_config_path,
                    tushare_token=token,
                )
            # Factories resolve YAML literals and ${ENV} expressions. Validate
            # the resulting objects—not merely the environment—before either
            # asyncpg consumer can open a socket.
            validate_v20_database_consumers(repository.config, fundamentals.config)
        resolved_scan_state.fundamentals_db = fundamentals
        artifacts = load_g_artifacts(
            config.artifact_manifest_path.parent,
            expected_manifest_sha256=config.artifact_manifest_sha256,
        )
        routes = load_v20_feishu_routes()
        mews_source = (
            LocalMewsSnapshotCalculator(
                token,
                repository,
                bootstrap_path=LocalMewsSnapshotCalculator.default_bootstrap_path(project_root),
            )
            if config.enabled
            else None
        )
        if config.enabled:
            active_route = routes.get(config.route_id)
            if (
                active_route is None
                or active_route.destination_fingerprint
                != config.route_binding.destination_fingerprint
            ):
                raise V20ConfigError("active V20 route differs from reviewed destination")
            if repository.config.ssl_root_cert_sha256 != config.v20_db_ca_sha256:
                raise V20ConfigError("V20 writer CA differs from reviewed runtime configuration")
        worker = f"{socket.gethostname()}:{os.getpid()}:v20"
        publisher = V20OutboxPublisher(
            repository,
            routes,
            worker_id=worker,
            route_id=config.route_id,
            official_stream_id=config.official_stream_id,
            lineage_id=config.state_lineage_id,
        )
        return cls(
            config=config,
            repository=repository,
            scan_state=resolved_scan_state,
            artifacts=artifacts,
            publisher=publisher,
            routes=routes,
            mews_source=mews_source,
        )

    @classmethod
    def from_legacy_runtime(cls) -> V20Service:
        """Embed an isolated forward-shadow V20 runtime in the main process.

        Strategy semantics, the ledger, outbox, 09:39 input boundary, and
        notification rules remain V20. V20 owns its state, market clients and
        database pools; only credentials and the final relay protocol come from
        the existing main deployment. The dedicated V20 host continues to use
        ``from_default_config``.
        """
        from src.common.config import get_tushare_token
        from src.data.database.fundamentals_db import create_fundamentals_db_from_config

        project_root = Path(__file__).resolve().parents[2]
        base_config = load_v20_runtime_config(project_root)
        if base_config.deployment_mode != "forward_shadow":
            raise V20ConfigError("embedded V20 only supports forward_shadow mode")
        if base_config.enabled:
            raise V20ConfigError(
                "explicit V20 activation must use the dedicated strict runtime factory"
            )

        database_config_path = project_root / "config" / "database-config.yaml"
        token = get_tushare_token()
        resolved_scan_state = V20CanonicalSelectionState()
        fundamentals = create_fundamentals_db_from_config(
            database_config_path,
            tushare_token=token,
        )
        repository = create_embedded_v20_repository_from_config(database_config_path)
        if (
            repository.config.schema != base_config.database_schema
            or repository.config.pool_min_size != base_config.database_pool_min_size
            or repository.config.pool_max_size != base_config.database_pool_max_size
        ):
            raise V20ConfigError(
                "embedded V20 repository differs from the frozen schema/pool settings"
            )
        route = load_legacy_embedded_v20_route()
        if not route.is_configured():
            raise V20ConfigError("legacy main Feishu route is not configured")
        config = _embedded_runtime_config(base_config, route)
        routes = {route.route_id: route}
        mews_source = LocalMewsSnapshotCalculator(
            token,
            repository,
            bootstrap_path=LocalMewsSnapshotCalculator.default_bootstrap_path(project_root),
        )
        resolved_scan_state.fundamentals_db = fundamentals
        artifacts = load_g_artifacts(
            config.artifact_manifest_path.parent,
            expected_manifest_sha256=config.artifact_manifest_sha256,
        )
        worker = f"{socket.gethostname()}:{os.getpid()}:v20-embedded"
        publisher = V20OutboxPublisher(
            repository,
            routes,
            worker_id=worker,
            route_id=config.route_id,
            official_stream_id=config.official_stream_id,
            lineage_id=config.state_lineage_id,
        )
        return cls(
            config=config,
            repository=repository,
            scan_state=resolved_scan_state,
            artifacts=artifacts,
            publisher=publisher,
            routes=routes,
            mews_source=mews_source,
            initialize_resources=_init_owned_embedded_v20_scan_resources,
            cleanup_resources=_cleanup_v20_scan_resources,
            embedded_legacy=True,
        )

    @staticmethod
    def _supports_strict_mews_guard(repository: Any) -> bool:
        """Distinguish the real writer from legacy repository test doubles."""

        if isinstance(repository, V20Repository):
            return True
        # Strict acceptance repositories expose their underlying connection;
        # old receipt-guard fakes expose only ``fetchrow`` and intentionally
        # cannot model the SERIALIZABLE selection transaction.
        connection = getattr(getattr(repository, "pool", None), "connection", None)
        return callable(getattr(connection, "transaction", None))

    def _initialize_mews_guard_store(self) -> None:
        """Bind the strict PostgreSQL MEWS boundary once the pool is live."""

        if self._mews_guard_store is not None:
            return
        if not self._supports_strict_mews_guard(self._repository):
            return
        try:
            self._mews_guard_store = V20MewsGuardStore(self._repository)
        except (AttributeError, RuntimeError, TypeError, ValueError):
            # Compatibility repositories used by isolated tests do not own a
            # PostgreSQL pool.  They retain the legacy in-memory contract;
            # every connected production V20Repository constructs the strict
            # store here and never enters those compatibility branches.
            self._mews_guard_store = None

    async def _initialize_canonical_artifact_boundary(self) -> None:
        """Open V20's own optional canonical-evidence store.

        V20 deliberately keeps this boundary on its own selection state.
        The legacy V16 scheduler, cache and in-flight coordinator are an
        independent service and cannot be a prerequisite or veto point for a
        V20 calculation.
        """

        try:
            store: Any = V16CanonicalArtifactStore(self._repository)
        except (AttributeError, RuntimeError, TypeError, ValueError):
            # Minimal repositories that cannot persist input_snapshots are
            # allowed only for isolated compatibility tests.  A real writer
            # always exposes ``schema`` and a connected ``pool``.
            self._canonical_artifact_store = None
            return

        self._canonical_artifact_store = store
        self._canonical_callbacks_open = True

    async def _probe_canonical_artifact(self, trade_date: date) -> tuple[Any, datetime] | None:
        """Return one durable canonical bundle and its immutable receipt time."""

        loaded = await self._load_canonical_artifact(trade_date)
        if loaded is None:
            return None
        bundle, first_received_at = loaded
        if not isinstance(bundle, (CanonicalV16ScanBundle, FrozenV16ScanBundle)):
            raise V20SemanticConflict("canonical V16 artifact probe bundle is invalid")
        return bundle, first_received_at

    def _detach_canonical_artifact_boundary(self) -> None:
        """Close V20's evidence writer without touching independent V16 state."""

        self._canonical_callbacks_open = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._stop_event.clear()
        if not self.config.enabled:
            self._startup_stage = "DISABLED"
            logger.info("V20 is disabled by configuration")
            return
        resource_initialization_cancelled = False
        try:
            self._startup_stage = "VALIDATING_RUNTIME"
            if not self._embedded_legacy:
                validate_v20_api_keys()
            route = self._routes.get(self.config.route_id)
            if route is None or not route.is_configured():
                raise V20ConfigError(f"V20 Feishu route {self.config.route_id!r} is not configured")
            if route.destination_fingerprint != self.config.route_binding.destination_fingerprint:
                raise V20ConfigError("active V20 route differs from reviewed destination")
            if self._mews_source is None:
                raise V20ConfigError(
                    "V20 local MEWS calculator is required when the service is enabled"
                )
            if (
                not self._embedded_legacy
                and os.environ.get("DB_SSLROOTCERT_SHA256") != self.config.fundamentals_db_ca_sha256
            ):
                raise V20ConfigError("fundamentals CA differs from reviewed runtime configuration")
            configured_chat_ids = [
                item.chat_id for item in self._routes.values() if item.chat_id.strip()
            ]
            if len(configured_chat_ids) != len(set(configured_chat_ids)):
                raise V20ConfigError(
                    "forward-shadow and formal V20 routes cannot share a Feishu chat_id"
                )
            configured_credentials = [
                (item.app_id, item.app_secret)
                for item in self._routes.values()
                if item.app_id.strip() and item.app_secret.strip()
            ]
            if len(configured_credentials) != len(set(configured_credentials)):
                raise V20ConfigError(
                    "forward-shadow and formal V20 routes cannot share Feishu credentials"
                )
            self._startup_stage = "CONNECTING_LEDGER"
            await self._repository.connect()
            self._repository_started = True
            self._initialize_mews_guard_store()
            self._startup_stage = "ACQUIRING_LEADER"
            await self._repository.acquire_runtime_leader(
                route_id=self.config.route_id,
                **self._ledger_scope,
            )
            now = self._aware_now()
            self._startup_stage = "REGISTERING_CONFIG"
            await self._repository.register_config(
                config_id=self.config.config_hash[:24],
                config_hash=self.config.config_hash,
                strategy_version=self.config.strategy_version,
                deployment_mode=self.config.deployment_mode,
                effective_trade_date=now.date(),
                payload=self.config.frozen_payload,
            )
            bootstrap = _bootstrap_bundle(
                self.config,
                empty_predecessor_trade_date=now.date() - timedelta(days=1),
            )
            self._startup_stage = "ENSURING_GENESIS"
            await self._repository.ensure_genesis_state(
                self.config.state_lineage_id,
                bootstrap.state,
                sha256_json(bootstrap.state),
                official_stream_id=self.config.official_stream_id,
                bootstrap_mode=self.config.bootstrap_mode,
                state_semantics_hash=self.config.state_semantics_hash,
                bootstrap_checkpoint_hash=self.config.bootstrap_checkpoint_sha256,
                bootstrap_predecessor_trade_date=bootstrap.predecessor_trade_date,
                bootstrap_shadow_batches=bootstrap.shadow_batches,
            )
            self._startup_stage = "ATTACHING_CANONICAL_ARTIFACT"
            await self._initialize_canonical_artifact_boundary()
            self._startup_stage = "REFRESHING_STATUS"
            await self._refresh_status_snapshot()
            self._startup_stage = "INITIALIZING_MARKET_RESOURCES"
            try:
                await self._initialize_resources(self._scan_state)
            except asyncio.CancelledError:
                # The V20 initializer is a shielded singleflight waiter.  Its
                # master may still own partially opened sockets after this
                # start task is cancelled, so rollback must converge it even
                # though the success flag has not yet been published.
                resource_initialization_cancelled = True
                raise
            self._resources_started = True
            self._startup_stage = "STARTING_LANES"
            self._tasks = [
                asyncio.create_task(self._run_scheduler(), name="v20-decision-scheduler"),
                asyncio.create_task(
                    self._run_live_exit_scheduler(),
                    name="v20-live-exit-scheduler",
                ),
                asyncio.create_task(
                    self._run_stale_exit_scheduler(),
                    name="v20-stale-exit-scheduler",
                ),
                asyncio.create_task(
                    self._run_outbox_recovery_scheduler(),
                    name="v20-outbox-recovery-scheduler",
                ),
                asyncio.create_task(
                    self._run_rolling7_recovery_scheduler(),
                    name="v20-rolling7-recovery-scheduler",
                ),
                asyncio.create_task(
                    self._run_publisher_scheduler(),
                    name="v20-outbox-publisher",
                ),
                asyncio.create_task(
                    self._run_mews_cache_scheduler(),
                    name="v20-mews-cache-scheduler",
                ),
            ]
            for task in self._tasks:
                task.add_done_callback(self._runtime_task_finished)
            self._startup_stage = "RUNNING"
            logger.info("V20 service started in %s mode", self.config.deployment_mode)
        except BaseException as startup_error:
            self._record_error("STARTUP_FAILED")
            self._stop_event.set()
            self._detach_canonical_artifact_boundary()
            current_day_early_tasks = await self._take_current_day_early_attempt_tasks()
            async with self._mews_singleflight_lock:
                mews_task, self._mews_singleflight_task = (
                    self._mews_singleflight_task,
                    None,
                )
                self._mews_singleflight_date = None
                self._mews_singleflight_source_trade_date = None
            tasks, self._tasks = self._tasks, []
            tasks.extend(current_day_early_tasks)
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            cleanup_labels: list[str] = []
            cleanup_operations: list[Awaitable[object]] = []
            async with self._calendar_tasks_lock:
                calendar_tasks = list(self._calendar_tasks.values())
                self._calendar_tasks = {}
            for calendar_task in calendar_tasks:
                calendar_task.cancel()
            if calendar_tasks:
                cleanup_labels.append("trade-calendar")
                cleanup_operations.append(asyncio.gather(*calendar_tasks, return_exceptions=True))
            if mews_task is not None and not mews_task.done():
                mews_task.cancel()
                cleanup_labels.append("selection-mews")
                cleanup_operations.append(mews_task)
            if self._resources_started or resource_initialization_cancelled:
                cleanup_labels.append("resource")
                cleanup_operations.append(self._cleanup_resources(self._scan_state))
            if self._repository_started:
                cleanup_labels.append("repository")
                cleanup_operations.append(self._repository.close())
            cleanup_results = await asyncio.gather(
                *cleanup_operations,
                return_exceptions=True,
            )
            for label, cleanup_result in zip(
                cleanup_labels,
                cleanup_results,
                strict=True,
            ):
                if isinstance(cleanup_result, BaseException):
                    startup_error.add_note(
                        f"V20 {label} cleanup also failed: "
                        f"{type(cleanup_result).__name__}: {cleanup_result}"
                    )
                    logger.error(
                        "V20 %s cleanup failed during startup rollback",
                        label,
                        exc_info=(
                            type(cleanup_result),
                            cleanup_result,
                            cleanup_result.__traceback__,
                        ),
                    )
            self._resources_started = False
            self._repository_started = False
            self._started = False
            raise

    @property
    def startup_stage(self) -> str:
        """Expose a secret-free lifecycle stage for legacy host diagnostics."""
        return self._startup_stage

    async def _take_current_day_early_attempt_tasks(self) -> list[asyncio.Task[Any]]:
        """Detach V20-owned acquisition tasks so lifecycle cleanup can cancel them."""

        async with self._current_day_early_attempt_lock:
            tasks = list(
                {
                    id(task): task for _targets, task in self._current_day_early_attempts.values()
                }.values()
            )
            self._current_day_early_attempts = {}
        return tasks

    async def stop(self) -> None:
        self._stop_event.set()
        # Close only V20's private evidence callbacks.  The independent V16
        # runtime does not point at them and is not inspected here.
        self._detach_canonical_artifact_boundary()
        async with self._mews_singleflight_lock:
            mews_task, self._mews_singleflight_task = (
                self._mews_singleflight_task,
                None,
            )
            self._mews_singleflight_date = None
            self._mews_singleflight_source_trade_date = None
        async with self._calendar_tasks_lock:
            calendar_tasks = list(self._calendar_tasks.values())
            self._calendar_tasks = {}
        current_day_early_tasks = await self._take_current_day_early_attempt_tasks()
        tasks, self._tasks = self._tasks, []
        late_replay_task, self._late_0939_replay_task = self._late_0939_replay_task, None
        if late_replay_task is not None:
            tasks.append(late_replay_task)
        if mews_task is not None:
            tasks.append(mews_task)
        tasks.extend(self._mews_trigger_tasks)
        self._mews_trigger_tasks = set()
        tasks.extend(calendar_tasks)
        tasks.extend(current_day_early_tasks)
        for task in tasks:
            task.cancel()
        primary_error: BaseException | None = None
        try:
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        except BaseException as exc:
            # Shutdown cancellation must still release the advisory leader,
            # pool, Tushare client, and fundamentals connection below.
            primary_error = exc
        cleanup_error: BaseException | None = None
        try:
            if self._resources_started:
                try:
                    await self._cleanup_resources(self._scan_state)
                except BaseException as exc:
                    cleanup_error = exc
                    logger.exception("V20 resource cleanup failed during shutdown")
                finally:
                    self._resources_started = False
        finally:
            if self._repository_started:
                try:
                    await self._repository.close()
                except BaseException as exc:
                    if cleanup_error is None:
                        cleanup_error = exc
                    logger.exception("V20 repository close failed during shutdown")
                finally:
                    self._repository_started = False
            self._started = False
            self._startup_stage = "STOPPED"
        logger.info("V20 service stopped")
        if primary_error is not None:
            raise primary_error
        if cleanup_error is not None:
            raise cleanup_error

    def _runtime_task_finished(self, finished: asyncio.Task[Any]) -> None:
        """Fail the whole runtime when any production lane terminates unexpectedly."""

        if finished.cancelled():
            return
        try:
            failure = finished.exception()
        except asyncio.CancelledError:
            return
        if failure is None and self._stop_event.is_set():
            return
        detail = (
            f"RUNTIME_TASK_FAILED:{finished.get_name()}:{type(failure).__name__}: {failure}"
            if failure is not None
            else f"RUNTIME_TASK_STOPPED:{finished.get_name()}"
        )
        self._startup_stage = "RUNTIME_FAILED"
        self._record_error(detail)
        self._stop_event.set()
        for sibling in self._tasks:
            if sibling is not finished and not sibling.done():
                sibling.cancel()
        logger.critical("V20 runtime lane terminated; cancelling sibling lanes: %s", detail)

    async def status(self) -> Mapping[str, Any]:
        context = self._context
        phase = (
            context.last_phase if context else "DISABLED" if not self.config.enabled else "STARTING"
        )
        snapshot = self._status_snapshot
        ledger = dict(snapshot["ledger"]) if snapshot is not None else None
        outbox = dict(snapshot["outbox"]) if snapshot is not None else None
        running = bool(self._tasks) and all(not task.done() for task in self._tasks)
        status_now = self._aware_now()
        snapshot_sampled_at = snapshot["sampled_at"] if snapshot is not None else None
        snapshot_age_seconds = (
            (status_now - snapshot_sampled_at).total_seconds()
            if isinstance(snapshot_sampled_at, datetime)
            else None
        )
        snapshot_stale = (
            snapshot_age_seconds is None
            or snapshot_age_seconds > STATUS_SNAPSHOT_MAX_AGE_SECONDS
            or self._status_snapshot_error is not None
        )
        freshness_seconds = {
            "decision": 90.0,
            "live_exit": float(self.config.market.exit_poll_seconds) * 2.0 + 2.0,
            "stale_exit": STALE_EXIT_TICK_SECONDS * 2.0 + 5.0,
            "outbox_recovery": OUTBOX_RECOVERY_TICK_SECONDS * 2.0 + 1.0,
            "publisher": 7.0,
            "mews_cache": MEWS_CACHE_POLL_SECONDS * 2.0 + 5.0,
        }
        lane_status: dict[str, Mapping[str, Any]] = {}
        lanes_healthy = True
        for lane_name, lane in self._lane_health.items():
            age_seconds = (
                (status_now - lane.last_success_at).total_seconds()
                if lane.last_success_at is not None
                else None
            )
            fresh = age_seconds is not None and age_seconds <= freshness_seconds[lane_name]
            durable_delivery_failures = (
                int(outbox.get("delivery_error_n", 0))
                if lane_name == "publisher" and outbox is not None
                else 0
            )
            unknown_delivery_outcomes = (
                int(outbox.get("unknown_n", 0))
                if lane_name == "publisher" and outbox is not None
                else 0
            )
            healthy = fresh and lane.last_error is None and durable_delivery_failures == 0
            healthy = healthy and unknown_delivery_outcomes == 0
            lanes_healthy = lanes_healthy and healthy
            lane_status[lane_name] = {
                "healthy": healthy,
                "last_success_at": (
                    lane.last_success_at.isoformat() if lane.last_success_at is not None else None
                ),
                "success_age_seconds": age_seconds,
                "freshness_limit_seconds": freshness_seconds[lane_name],
                "last_error": lane.last_error,
                "last_error_at": (
                    lane.last_error_at.isoformat() if lane.last_error_at is not None else None
                ),
                "error_revision": lane.error_revision,
                "durable_delivery_failures": durable_delivery_failures,
                "unknown_delivery_outcomes": unknown_delivery_outcomes,
            }
        return {
            "enabled": self.config.enabled,
            "mode": self.config.deployment_mode if self.config.enabled else "disabled",
            "running": running,
            "healthy": (not self.config.enabled)
            or (running and lanes_healthy and not snapshot_stale),
            "startup_stage": self._startup_stage,
            "strategy_version": self.config.strategy_version,
            "config_hash": self.config.config_hash,
            "route_id": self.config.route_id,
            "official_stream_id": self.config.official_stream_id,
            "state_lineage_id": self.config.state_lineage_id,
            "reference_profile_id": self.config.reference_profile_id,
            "return_profile_id": self.config.return_profile_id,
            "calendar_first_date": (
                self._calendar_cache[0].isoformat() if self._calendar_cache else None
            ),
            "calendar_last_date": (
                self._calendar_cache[-1].isoformat() if self._calendar_cache else None
            ),
            "trade_date": context.trade_date.isoformat() if context else None,
            "phase": phase,
            "entry_action": (
                context.entry_status.action if context and context.entry_status else None
            ),
            "entry_event_id": (
                context.entry_status.event_id if context and context.entry_status else None
            ),
            "last_success_at": self._last_success_at.isoformat() if self._last_success_at else None,
            "last_error": self._last_error,
            "last_error_at": self._last_error_at.isoformat() if self._last_error_at else None,
            "runtime_lanes": lane_status,
            "mews_cache": {
                "availability_date": (
                    self._mews_cached_for.isoformat() if self._mews_cached_for else None
                ),
                "source_trade_date": (
                    self._mews_source_trade_date.isoformat()
                    if self._mews_source_trade_date
                    else None
                ),
                "snapshot_id": self._mews_snapshot_id,
                "last_failure": self._mews_last_failure,
            },
            "status_snapshot": {
                "sampled_at": (
                    snapshot_sampled_at.isoformat()
                    if isinstance(snapshot_sampled_at, datetime)
                    else None
                ),
                "age_seconds": snapshot_age_seconds,
                "freshness_limit_seconds": STATUS_SNAPSHOT_MAX_AGE_SECONDS,
                "stale": snapshot_stale,
                "last_error": self._status_snapshot_error,
            },
            "ledger": ledger,
            "outbox": outbox,
            "order_execution_scope": "OUT_OF_SCOPE",
            "integration_profile": (
                "legacy_main_embedded" if self._embedded_legacy else "dedicated"
            ),
        }

    async def _refresh_status_snapshot(self) -> None:
        """Sample durable health in a bounded background/startup lane, never HTTP."""

        try:
            state = await self._repository.load_state(self.config.state_lineage_id)
            outbox = await self._repository.get_outbox_health(
                route_id=self.config.route_id,
                **self._ledger_scope,
            )
        except Exception as exc:
            self._status_snapshot_error = f"{type(exc).__name__}: {exc}"
            raise
        self._status_snapshot = {
            "sampled_at": self._aware_now(),
            "ledger": {
                "lineage_id": state.lineage_id,
                "revision": state.revision,
                "state_hash": state.state_hash,
            },
            "outbox": dict(outbox),
        }
        self._status_snapshot_error = None

    async def ingest_mews_snapshot(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        self._require_running()
        if "received_at" in payload:
            raise ValueError("MEWS received_at is owned by the V20 repository")
        content_hash = await self._repository.record_mews_snapshot(payload)
        return {
            "snapshot_id": str(payload["snapshot_id"]),
            "accepted": True,
            "content_hash": content_hash,
        }

    async def record_reminder_stop_ack(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        self._require_running()
        required = {"ack_id", "original_exit_event_id", "consumer_id", "ack_ts"}
        if set(payload) != required:
            raise ValueError("reminder acknowledgement field set mismatch")
        ack_ts = datetime.fromisoformat(str(payload["ack_ts"]))
        if ack_ts.tzinfo is None or ack_ts.utcoffset() is None:
            raise ValueError("ack_ts must be timezone-aware")
        evidence = {
            "profile": "V20_AUTHENTICATED_ACK_EVIDENCE_V1",
            "ack_id": str(payload["ack_id"]),
            "original_exit_event_id": str(payload["original_exit_event_id"]),
            "consumer_id": str(payload["consumer_id"]),
            "ack_ts": ack_ts.isoformat(),
            "authentication_boundary": "X_V20_API_KEY_VALIDATED_BY_ROUTE",
        }
        created = await self._repository.record_reminder_stop_ack(
            str(payload["original_exit_event_id"]),
            str(payload["consumer_id"]),
            ack_ts=ack_ts,
            auth_evidence_hash=sha256_json(evidence),
            ack_id=str(payload["ack_id"]),
            **self._ledger_scope,
        )
        return {"ack_id": str(payload["ack_id"]), "accepted": True, "created": created}

    async def enroll_manual_monitor(
        self,
        source_event_id: str,
        request_id: str,
    ) -> Mapping[str, Any]:
        """Attach a sealed retrospective ENTER result to the ordinary exit ledger.

        This alert-only recovery lane never rewrites the failed official entry
        slot and never creates an account position, order, or fill.  The source
        replay determines every symbol and weight; callers can only name that
        immutable event and an idempotency key.
        """

        await self._require_manual_trigger_ready()
        if not isinstance(source_event_id, str) or len(source_event_id) != 64:
            raise ValueError("source_event_id must be a 64-character V20 event id")
        if any(character not in "0123456789abcdef" for character in source_event_id):
            raise ValueError("source_event_id must be lowercase hexadecimal")
        if not isinstance(request_id, str) or _MANUAL_REQUEST_ID.fullmatch(request_id) is None:
            raise ValueError(
                "Idempotency-Key must be 8-128 characters using letters, digits, . _ : or -"
            )
        await self._repository.assert_runtime_leader()
        if self._manual_monitor_lock.locked():
            raise V20StateConflict("another V20 manual monitor attachment is already running")

        async with self._manual_monitor_lock:
            await self._require_manual_trigger_ready()
            await self._repository.assert_runtime_leader()
            source = await self._repository.get_outbox_event(
                source_event_id,
                route_id=self.config.route_id,
                **self._ledger_scope,
            )
            if source is None:
                raise V20RepositoryError("manual monitor source event is unavailable")
            semantic = source.semantic
            if (
                source.event_type != "DATA_ALERT"
                or source.payload is None
                or source.payload_hash is None
                or semantic.get("event_id") != source.event_id
                or semantic.get("schema_version") != V20_DATA_ALERT_SEMANTIC_SCHEMA
                or semantic.get("feishu_formatter_profile") != V20_FEISHU_FORMATTER_PROFILE
                or semantic.get("alert_code") != "MANUAL_0939_CHAIN_PROBE_RESULT"
                or semantic.get("probe_profile")
                != "CURRENT_DEPLOYED_CODE_EXACT_0939_ENTRY_RENDER_V2"
                or semantic.get("probe_result") != "PASS"
                or semantic.get("current_version_recomputed") is not True
                or semantic.get("replay_reused") is not False
                or semantic.get("v20_action") != "ENTER"
                or semantic.get("official_entry_action") != "INPUT_INVALID"
                or semantic.get("official_state_changed") is not False
                or semantic.get("orders_changed") is not False
                or semantic.get("non_actionable") is not True
                or semantic.get("retrospective_expired") is not True
                or semantic.get("visible_message_mode") != "MANUAL_OPERATOR_RENDER"
                or not isinstance(semantic.get("state_semantics_hash"), str)
                or re.fullmatch(r"[0-9a-f]{64}", str(semantic.get("state_semantics_hash"))) is None
                or not isinstance(semantic.get("strategy_version"), str)
                or not semantic.get("strategy_version")
                or semantic.get("strategy_version") != self.config.strategy_version
                or semantic.get("state_semantics_hash") != self.config.state_semantics_hash
                or semantic.get("official_stream_id") != self.config.official_stream_id
                or semantic.get("state_lineage_id") != self.config.state_lineage_id
            ):
                raise V20SemanticConflict(
                    "manual monitor source is not a sealed compatible PASS/ENTER chain probe"
                )
            source_config_hash = semantic.get("config_hash")
            if (
                not isinstance(source_config_hash, str)
                or re.fullmatch(r"[0-9a-f]{64}", source_config_hash) is None
            ):
                raise V20SemanticConflict("manual monitor source config hash is invalid")
            if sha256_json(dict(source.semantic)) != source.semantic_content_hash:
                raise V20SemanticConflict("manual monitor source semantic hash differs")
            if sha256_json(dict(source.payload)) != source.payload_hash:
                raise V20SemanticConflict("manual monitor source payload hash differs")

            try:
                signal_date = date.fromisoformat(str(semantic["event_trade_date"]))
            except (KeyError, ValueError) as exc:
                raise V20SemanticConflict("manual monitor source trade date is invalid") from exc
            official = await self._repository.get_entry_status(
                self.config.official_stream_id,
                signal_date,
            )
            if official is None:
                raise V20RepositoryError("manual monitor source official slot is unavailable")
            self._verify_entry_binding(official)
            if official.action != "INPUT_INVALID" or official.event_id != semantic.get(
                "official_entry_event_id"
            ):
                raise V20SemanticConflict(
                    "manual monitor source is not bound to the frozen failed official slot"
                )

            entry_render = semantic.get("entry_render_semantic")
            symbols = semantic.get("symbols")
            multiplier = semantic.get("final_multiplier")
            if (
                not isinstance(entry_render, Mapping)
                or not isinstance(symbols, list)
                or not symbols
                or entry_render.get("schema_version") != V20_ENTRY_SEMANTIC_SCHEMA
                or entry_render.get("feishu_formatter_profile") != V20_FEISHU_FORMATTER_PROFILE
                or entry_render.get("strategy_version") != semantic.get("strategy_version")
                or entry_render.get("config_hash") != source_config_hash
                or entry_render.get("state_semantics_hash") != semantic.get("state_semantics_hash")
                or entry_render.get("symbols") != symbols
                or entry_render.get("action") != "ENTER"
                or entry_render.get("final_multiplier") != multiplier
                or isinstance(multiplier, bool)
                or not isinstance(multiplier, (int, float))
                or not math.isfinite(float(multiplier))
                or not 0 < float(multiplier) <= 1
            ):
                raise V20SemanticConflict("manual monitor source ticket list is inconsistent")
            normalized_symbols: list[dict[str, Any]] = []
            seen_codes: set[str] = set()
            for expected_rank, item in enumerate(symbols, start=1):
                if not isinstance(item, Mapping):
                    raise V20SemanticConflict("manual monitor source contains a malformed ticket")
                code = str(item.get("code", ""))
                name = str(item.get("name", "")).strip()
                rank = item.get("rank")
                snapshot_price = item.get("snapshot_price")
                if (
                    rank != expected_rank
                    or len(code) != 6
                    or not code.isdigit()
                    or code in seen_codes
                    or not name
                    or isinstance(snapshot_price, bool)
                    or not isinstance(snapshot_price, (int, float))
                    or not math.isfinite(float(snapshot_price))
                    or float(snapshot_price) <= 0
                ):
                    raise V20SemanticConflict("manual monitor source ticket identity is invalid")
                seen_codes.add(code)
                normalized_symbols.append(
                    {
                        "rank": expected_rank,
                        "code": code,
                        "name": name,
                        "snapshot_price": float(snapshot_price),
                    }
                )

            existing_enrollment = await self._repository.get_manual_monitor_enrollment(
                source.event_id,
                **self._ledger_scope,
            )
            if existing_enrollment is not None:
                return await self._manual_monitor_response(
                    source=source,
                    record=existing_enrollment,
                    request_id=request_id,
                    normalized_symbols=normalized_symbols,
                    created=False,
                )

            now = self._aware_now()
            calendar = tuple(await self._load_trade_calendar(now.date()))
            if signal_date not in calendar:
                raise V20SemanticConflict("manual monitor signal date is not an exchange session")
            successors = [session for session in calendar if session > signal_date]
            if len(successors) < 2:
                raise V20RepositoryError("trade calendar lacks D1/D2 for manual monitoring")
            d1, d2 = successors[:2]
            activation_cutoff = _local(
                d1,
                self.config.clock.reference_lock_deadline_next_day,
            )
            if now >= activation_cutoff:
                raise V20StateConflict("manual monitoring must be attached before D1 09:30")

            codes = tuple(item["code"] for item in normalized_symbols)

            async def load_reference_evidence() -> _ReferenceArbitration:
                try:
                    records = await self._repository.list_raw_minute_bar_records(
                        codes,
                        trade_date=signal_date,
                        end_labels=(self.config.clock.reference_bar_label,),
                        received_before=activation_cutoff,
                    )
                except V20MinuteBarIntegrityConflict as exc:
                    raise V20SemanticConflict(
                        "manual monitor reference evidence contains corrupt raw rows"
                    ) from exc
                return _arbitrate_reference_records(
                    records,
                    codes,
                    trade_date=signal_date,
                    expected_label=self.config.clock.reference_bar_label,
                )

            reference = await load_reference_evidence()
            if reference.missing_codes or reference.conflict_codes:
                client = self._scan_state.realtime_client
                if client is None or not hasattr(client, "batch_get_minute_history_for_date"):
                    raise V20RepositoryError("manual monitor minute-history adapter is unavailable")
                history = await asyncio.wait_for(
                    client.batch_get_minute_history_for_date(list(codes), signal_date),
                    timeout=MANUAL_MONITOR_HISTORY_TIMEOUT_SECONDS,
                )
                evidence_context = _DayContext(
                    trade_date=signal_date,
                    calendar=calendar,
                    entry_status=official,
                    last_phase="MANUAL_MONITOR_REFERENCE_RECOVERY",
                )
                await self._persist_history(evidence_context, history, observed_at=now)
                reference = await load_reference_evidence()
            if reference.missing_codes or reference.conflict_codes:
                raise V20StateConflict(
                    "manual monitor requires complete legal D0 09:41 reference evidence; "
                    f"missing={len(reference.missing_codes)}, "
                    f"conflict={len(reference.conflict_codes)}"
                )

            calendar_evidence_hash = sha256_json(
                {
                    "profile": "V20_MANUAL_MONITOR_D1_D2_CALENDAR_V1",
                    "signal_date": signal_date.isoformat(),
                    "d1": d1.isoformat(),
                    "d2": d2.isoformat(),
                }
            )
            model_batch_id = named_hash(
                "V20_MANUAL_MONITOR_MODEL_BATCH_ID_V1",
                {
                    "source_event_id": source.event_id,
                    "source_semantic_content_hash": source.semantic_content_hash,
                },
            )
            per_leg_weight = float(multiplier) / len(normalized_symbols)
            model_batch = ModelBatchWrite(
                model_batch_id=model_batch_id,
                multiplier=float(multiplier),
                evaluation_only=False,
                reference_profile_id=self.config.reference_profile_id,
                legs=tuple(
                    ModelLegWrite(
                        model_leg_id=derive_model_leg_id(
                            model_batch_id=model_batch_id,
                            code=item["code"],
                        ),
                        code=item["code"],
                        stock_name=item["name"],
                        rank=item["rank"],
                        relative_weight=per_leg_weight,
                        d1=d1,
                        d2=d2,
                    )
                    for item in normalized_symbols
                ),
            )
            enrollment_id = named_hash(
                "V20_MANUAL_MONITOR_ENROLLMENT_ID_V1",
                {
                    "source_event_id": source.event_id,
                    "model_batch_id": model_batch_id,
                },
            )
            enrollment_semantic: dict[str, Any] = {
                "profile": "V20_MANUAL_MONITOR_ENROLLMENT_V1",
                "enrollment_id": enrollment_id,
                "source_event_id": source.event_id,
                "official_entry_event_id": official.event_id,
                "source_semantic_content_hash": source.semantic_content_hash,
                "source_payload_hash": source.payload_hash,
                "official_stream_id": self.config.official_stream_id,
                "state_lineage_id": self.config.state_lineage_id,
                "strategy_version": self.config.strategy_version,
                "source_config_hash": source_config_hash,
                "state_semantics_hash": self.config.state_semantics_hash,
                "signal_date": signal_date.isoformat(),
                "d1": d1.isoformat(),
                "d2": d2.isoformat(),
                "activation_cutoff_ts": activation_cutoff.isoformat(),
                "calendar_evidence_hash": calendar_evidence_hash,
                "reference_profile_id": self.config.reference_profile_id,
                "reference_evidence_status": "COMPLETE_PENDING_D1_ARBITRATION",
                "reference_evidence_hash": reference.source_hash,
                "model_batch_id": model_batch_id,
                "multiplier": float(multiplier),
                "symbols": normalized_symbols,
                "official_state_changed": False,
                "orders_changed": False,
            }
            commit = ManualMonitorEnrollmentCommit(
                enrollment_id=enrollment_id,
                source_event_id=source.event_id,
                official_entry_event_id=official.event_id,
                request_id=request_id,
                route_id=self.config.route_id,
                official_stream_id=self.config.official_stream_id,
                lineage_id=self.config.state_lineage_id,
                strategy_version=self.config.strategy_version,
                source_config_hash=source_config_hash,
                state_semantics_hash=self.config.state_semantics_hash,
                signal_date=signal_date,
                d1=d1,
                d2=d2,
                activation_cutoff_ts=activation_cutoff,
                source_semantic_content_hash=source.semantic_content_hash,
                source_payload_hash=source.payload_hash,
                calendar_evidence_hash=calendar_evidence_hash,
                enrollment_semantic=enrollment_semantic,
                enrollment_semantic_hash=sha256_json(enrollment_semantic),
                model_batch=model_batch,
            )
            await self._require_manual_trigger_ready()
            await self._repository.assert_runtime_leader()
            created = await self._repository.enroll_manual_monitor(commit)
            record = await self._repository.get_manual_monitor_enrollment(
                source.event_id,
                **self._ledger_scope,
            )
            if record is None:
                raise V20RepositoryError("manual monitor enrollment is not readable")
            return await self._manual_monitor_response(
                source=source,
                record=record,
                request_id=request_id,
                normalized_symbols=normalized_symbols,
                created=created,
            )

    async def _manual_monitor_response(
        self,
        *,
        source: OutboxRecord,
        record: ManualMonitorEnrollmentRecord,
        request_id: str,
        normalized_symbols: Sequence[Mapping[str, Any]],
        created: bool,
    ) -> Mapping[str, Any]:
        """Seal a readable confirmation and prove every enrolled leg is durable."""

        if (
            record.source_event_id != source.event_id
            or record.official_entry_event_id != source.semantic.get("official_entry_event_id")
            or record.source_semantic_content_hash != source.semantic_content_hash
            or record.source_payload_hash != source.payload_hash
            or record.semantic.get("profile") != "V20_MANUAL_MONITOR_ENROLLMENT_V1"
            or record.semantic.get("source_event_id") != source.event_id
            or record.semantic.get("official_entry_event_id") != record.official_entry_event_id
            or record.semantic.get("model_batch_id") != record.model_batch_id
            or record.semantic.get("official_stream_id") != self.config.official_stream_id
            or record.semantic.get("state_lineage_id") != self.config.state_lineage_id
            or record.semantic.get("source_config_hash") != source.semantic.get("config_hash")
            or not isinstance(record.semantic.get("reference_profile_id"), str)
            or not record.semantic.get("reference_profile_id")
            or record.semantic.get("symbols") != [dict(item) for item in normalized_symbols]
            or record.signal_date >= record.d1
            or record.d1 >= record.d2
        ):
            raise V20SemanticConflict("manual monitor enrollment/source binding is invalid")
        enrolled_legs = await self._repository.list_manual_monitor_batch_legs(
            record.model_batch_id,
            **self._ledger_scope,
        )
        expected_legs = [(item["rank"], item["code"], item["name"]) for item in normalized_symbols]
        actual_legs = [(leg.rank, leg.code, leg.stock_name) for leg in enrolled_legs]
        if actual_legs != expected_legs or any(
            leg.model_batch_id != record.model_batch_id
            or leg.origin_kind != "MANUAL_MONITOR"
            or leg.source_event_id != source.event_id
            or leg.signal_date != record.signal_date
            or leg.d1 != record.d1
            or leg.d2 != record.d2
            or leg.evaluation_only
            for leg in enrolled_legs
        ):
            raise V20StateConflict("manual monitor model batch is incomplete or inconsistent")

        confirmation_event_id = named_hash(
            "V20_MANUAL_MONITOR_ARMED_EVENT_ID_V1",
            {"enrollment_id": record.enrollment_id},
        )
        confirmation_semantic = {
            "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
            "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
            "event_id": confirmation_event_id,
            "strategy_version": source.semantic["strategy_version"],
            "config_hash": record.semantic["source_config_hash"],
            "state_semantics_hash": source.semantic["state_semantics_hash"],
            "deployment_mode": source.semantic["deployment_mode"],
            "official_stream_id": self.config.official_stream_id,
            "state_lineage_id": self.config.state_lineage_id,
            "alert_code": "MANUAL_MONITOR_ARMED",
            "delivery_priority_class": "OPERATOR_NOTIFICATION",
            "event_trade_date": record.d1.isoformat(),
            "enrollment_id": record.enrollment_id,
            "source_event_id": source.event_id,
            "official_entry_event_id": record.official_entry_event_id,
            "model_batch_id": record.model_batch_id,
            "signal_date": record.signal_date.isoformat(),
            "d1": record.d1.isoformat(),
            "d2": record.d2.isoformat(),
            "activation_cutoff_ts": record.activation_cutoff_ts.isoformat(),
            "reference_profile_id": record.semantic["reference_profile_id"],
            "reference_evidence_status": "COMPLETE_PENDING_D1_ARBITRATION",
            "armed_leg_count": len(enrolled_legs),
            "symbols": [dict(item) for item in normalized_symbols],
            "official_state_changed": False,
            "orders_changed": False,
            "message": (
                f"已把 {record.signal_date.isoformat()} 的 {len(enrolled_legs)} 只冻结票单补挂到"
                " V20 卖出监控；参考价将按原始 09:41 bar.open 在 D1 09:30"
                " 仲裁锁定，D1/D2 止损和 D2 14:57 退出提醒均已接入。"
            ),
        }
        await self._repository.enqueue_alert(
            confirmation_event_id,
            self.config.route_id,
            confirmation_semantic,
            sha256_json(confirmation_semantic),
            **self._ledger_scope,
        )
        confirmation = await self._repository.seal_event(
            confirmation_event_id,
            seal_v20_payload,
        )
        return {
            "accepted": True,
            "created": created,
            "armed": True,
            "manual_request_id": request_id,
            "enrollment_id": record.enrollment_id,
            "source_event_id": source.event_id,
            "model_batch_id": record.model_batch_id,
            "signal_date": record.signal_date.isoformat(),
            "d1": record.d1.isoformat(),
            "d2": record.d2.isoformat(),
            "activation_cutoff_ts": record.activation_cutoff_ts.isoformat(),
            "reference_profile_id": record.semantic["reference_profile_id"],
            "reference_evidence_complete": True,
            "reference_locked": all(leg.reference_status == "LOCKED" for leg in enrolled_legs),
            "armed_leg_count": len(enrolled_legs),
            "symbols": [dict(item) for item in normalized_symbols],
            "official_state_changed": False,
            "orders_changed": False,
            "confirmation_event_id": confirmation.event_id,
            "delivery_status": confirmation.delivery_status,
            "feishu_delivery_confirmed": confirmation.delivery_status == "SENT",
        }

    async def trigger_manual_scan(self, request_id: str) -> Mapping[str, Any]:
        """Accelerate a legal decision cycle and queue a non-actionable receipt.

        The caller cannot supply a clock, trade date, or force flag.  Before the
        strict 09:40 boundary this may advance the same serialized decision lane
        that the scheduler already owns.  At and after the boundary it is
        strictly read-only with respect to the official strategy state.  In all
        cases the additional Feishu event is a DATA_ALERT/NOTIFICATION receipt,
        never an actionable ENTRY_DECISION.
        """

        await self._require_manual_trigger_ready()
        if not isinstance(request_id, str) or _MANUAL_REQUEST_ID.fullmatch(request_id) is None:
            raise ValueError(
                "Idempotency-Key must be 8-128 characters using letters, digits, . _ : or -"
            )
        await self._repository.assert_runtime_leader()
        manual_event_id = named_hash(
            "V20_MANUAL_TRIGGER_RECEIPT_EVENT_ID_V1",
            {
                "route_id": self.config.route_id,
                "official_stream_id": self.config.official_stream_id,
                "lineage_id": self.config.state_lineage_id,
                "state_semantics_hash": self.config.state_semantics_hash,
                "manual_request_id": request_id,
            },
        )

        existing = await self._repository.get_outbox_event(
            manual_event_id,
            route_id=self.config.route_id,
            **self._ledger_scope,
        )
        if existing is not None:
            if existing.payload is None:
                _manual_trigger_response(
                    existing,
                    created=False,
                    request_id=request_id,
                    config=self.config,
                )
                await self._require_manual_trigger_ready()
                await self._repository.assert_runtime_leader()
                existing = await self._repository.seal_event(manual_event_id, seal_v20_payload)
            return _manual_trigger_response(
                existing,
                created=False,
                request_id=request_id,
                config=self.config,
            )

        if self._manual_trigger_lock.locked():
            raise V20StateConflict("another V20 manual trigger is already running")

        async with self._manual_trigger_lock:
            await self._require_manual_trigger_ready()
            await self._repository.assert_runtime_leader()
            existing = await self._repository.get_outbox_event(
                manual_event_id,
                route_id=self.config.route_id,
                **self._ledger_scope,
            )
            if existing is not None:
                if existing.payload is None:
                    _manual_trigger_response(
                        existing,
                        created=False,
                        request_id=request_id,
                        config=self.config,
                    )
                    await self._require_manual_trigger_ready()
                    await self._repository.assert_runtime_leader()
                    existing = await self._repository.seal_event(
                        manual_event_id,
                        seal_v20_payload,
                    )
                return _manual_trigger_response(
                    existing,
                    created=False,
                    request_id=request_id,
                    config=self.config,
                )

            current = self._aware_now()
            trade_date = current.date()
            status_before = await self._repository.get_entry_status(
                self.config.official_stream_id,
                trade_date,
            )
            if status_before is not None:
                self._verify_entry_binding(status_before)
            status_after = status_before
            wall = current.timetz().replace(tzinfo=None)
            should_accelerate = (
                status_before is None
                and self.config.clock.prewarm <= wall < self.config.clock.publish_deadline
            )
            if should_accelerate:
                try:
                    await asyncio.wait_for(
                        self._decision_cycle_lock.acquire(),
                        timeout=MANUAL_TRIGGER_DECISION_LOCK_TIMEOUT_SECONDS,
                    )
                except TimeoutError as exc:
                    raise V20StateConflict("V20 decision lane is busy") from exc
                try:
                    # Re-sample inside the lock.  Waiting for the automatic
                    # scheduler must never leave a stale pre-09:40 timestamp in
                    # authority after the real boundary.
                    await self._run_decision_iteration_with_cutoff(self._aware_now())
                finally:
                    self._decision_cycle_lock.release()
                status_after = await self._repository.get_entry_status(
                    self.config.official_stream_id,
                    trade_date,
                )
                if status_after is not None:
                    self._verify_entry_binding(status_after)

            current = self._aware_now()
            wall = current.timetz().replace(tzinfo=None)
            context = (
                self._context if self._context and self._context.trade_date == trade_date else None
            )
            late_replay_record: OutboxRecord | None = None
            late_replay_error: str | None = None
            if (
                wall >= self.config.clock.publish_deadline
                and status_after is not None
                and status_after.action == "INPUT_INVALID"
            ):
                try:
                    replay_context = context
                    if replay_context is None or trade_date not in replay_context.calendar:
                        calendar = await self._load_trade_calendar(trade_date)
                        if trade_date not in calendar:
                            raise V20RepositoryError(
                                "late 09:39 replay date is not an exchange session"
                            )
                        replay_context = _DayContext(
                            trade_date=trade_date,
                            calendar=calendar,
                            entry_status=status_after,
                            last_phase="DECISION_COMMITTED",
                        )
                    late_replay_record = await self._ensure_late_0939_replay(
                        replay_context,
                        current,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    late_replay_error = f"{type(exc).__name__}: {exc}"
                    logger.exception("V20 manual late 09:39 replay failed")
            if status_after is not None:
                cycle_result = "DECISION_COMMITTED" if status_before is None else "ALREADY_TERMINAL"
            elif context is not None and context.last_phase == "NON_TRADING_DAY":
                cycle_result = "NON_TRADING_DAY"
            elif wall < self.config.clock.prewarm:
                cycle_result = "BEFORE_WINDOW"
            elif wall < time.fromisoformat(self.config.clock.decision_bar_label):
                cycle_result = "COLLECTING"
            elif wall < self.config.clock.publish_deadline:
                cycle_result = context.last_phase if context is not None else "DECISION_PENDING"
            else:
                cycle_result = "CUTOFF_WITHOUT_DURABLE_DECISION"
            if late_replay_record is not None:
                cycle_result = "LATE_0939_REPLAY_READY"

            official_state_changed = status_before is None and status_after is not None
            late_replay = late_replay_record.semantic if late_replay_record is not None else None
            message = _manual_trigger_receipt_body(
                request_id=request_id,
                cycle_result=cycle_result,
                status=status_after,
                late_replay=late_replay,
                late_replay_error=late_replay_error,
            )
            semantic = {
                "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
                "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
                "event_id": manual_event_id,
                "strategy_version": self.config.strategy_version,
                "config_hash": self.config.config_hash,
                "state_semantics_hash": self.config.state_semantics_hash,
                "deployment_mode": self.config.deployment_mode,
                "official_stream_id": self.config.official_stream_id,
                "state_lineage_id": self.config.state_lineage_id,
                "alert_code": "MANUAL_TRIGGER_RECEIPT",
                "delivery_priority_class": "OPERATOR_NOTIFICATION",
                "manual_request_id": request_id,
                "event_trade_date": trade_date.isoformat(),
                "cycle_result": cycle_result,
                "formal_decision_available": status_after is not None,
                "entry_action": status_after.action if status_after is not None else None,
                "entry_event_id": status_after.event_id if status_after is not None else None,
                "formal_semantic_hash": (
                    status_after.semantic_content_hash if status_after is not None else None
                ),
                "official_state_changed": official_state_changed,
                "late_0939_replay_available": late_replay is not None,
                "late_0939_replay_event_id": (
                    late_replay_record.event_id if late_replay_record is not None else None
                ),
                "late_0939_replay_action": (
                    late_replay.get("replay_action") if late_replay is not None else None
                ),
                "late_0939_replay_multiplier": (
                    late_replay.get("final_multiplier") if late_replay is not None else None
                ),
                "late_0939_replay_error": late_replay_error,
                "non_actionable": True,
                "message": message,
            }
            semantic_hash = sha256_json(semantic)

            # Recheck both fences immediately before each manual write.  A
            # failed runtime cannot be resurrected through this control route.
            await self._require_manual_trigger_ready()
            await self._repository.assert_runtime_leader()
            created = await self._repository.enqueue_alert(
                manual_event_id,
                self.config.route_id,
                semantic,
                semantic_hash,
                **self._ledger_scope,
            )
            await self._require_manual_trigger_ready()
            await self._repository.assert_runtime_leader()
            sealed = await self._repository.seal_event(manual_event_id, seal_v20_payload)
            return _manual_trigger_response(
                sealed,
                created=created,
                request_id=request_id,
                config=self.config,
            )

    async def trigger_morning_selection(self, request_id: str) -> Mapping[str, Any]:
        """Run the real pre-09:40 decision lane without a manual wrapper message.

        A successful new decision is the ordinary ``ENTRY_DECISION`` written by
        :meth:`_run_decision_iteration_with_cutoff`; consequently model legs and
        the intraday-exit lane are armed exactly as they are under the automatic
        scheduler.  This method never creates a second, formatter-specific
        receipt.  The route may therefore promise that the visible Feishu text
        comes only from the production entry renderer.
        """

        await self._require_manual_trigger_ready()
        if not isinstance(request_id, str) or _MANUAL_REQUEST_ID.fullmatch(request_id) is None:
            raise ValueError(
                "Idempotency-Key must be 8-128 characters using letters, digits, . _ : or -"
            )
        await self._repository.assert_runtime_leader()
        now = self._aware_now()
        wall = now.timetz().replace(tzinfo=None)
        if wall >= self.config.clock.publish_deadline:
            raise V20StateConflict("live morning selection is outside the pre-09:40 window")
        if self._manual_trigger_lock.locked():
            raise V20StateConflict("another V20 manual trigger is already running")

        async with self._manual_trigger_lock:
            await self._require_manual_trigger_ready()
            await self._repository.assert_runtime_leader()
            trade_date = self._aware_now().date()
            status_before = await self._repository.get_entry_status(
                self.config.official_stream_id,
                trade_date,
            )
            if status_before is not None:
                self._verify_entry_binding(status_before)

            if status_before is None and wall >= self.config.clock.prewarm:
                try:
                    await asyncio.wait_for(
                        self._decision_cycle_lock.acquire(),
                        timeout=MANUAL_TRIGGER_DECISION_LOCK_TIMEOUT_SECONDS,
                    )
                except TimeoutError as exc:
                    raise V20StateConflict("V20 decision lane is busy") from exc
                try:
                    # Re-sample the service clock inside the serialized lane;
                    # an HTTP request can never extend the legal entry window.
                    await self._run_decision_iteration_with_cutoff(self._aware_now())
                finally:
                    self._decision_cycle_lock.release()

            status_after = await self._repository.get_entry_status(
                self.config.official_stream_id,
                trade_date,
            )
            completed_at = self._aware_now()
            retrospective_expired = (
                completed_at.timetz().replace(tzinfo=None) >= self.config.clock.publish_deadline
            )
            if status_after is None:
                context = (
                    self._context
                    if self._context is not None and self._context.trade_date == trade_date
                    else None
                )
                completed_wall = completed_at.timetz().replace(tzinfo=None)
                if completed_wall < self.config.clock.prewarm:
                    cycle_result = "BEFORE_WINDOW"
                elif completed_wall < time.fromisoformat(self.config.clock.decision_bar_label):
                    cycle_result = "COLLECTING"
                else:
                    cycle_result = context.last_phase if context is not None else "DECISION_PENDING"
                return {
                    "accepted": True,
                    "created": False,
                    "manual_request_id": request_id,
                    "trade_date": trade_date.isoformat(),
                    "cycle_result": cycle_result,
                    "formal_decision_available": False,
                    "entry_action": None,
                    "entry_event_id": None,
                    "symbols": [],
                    "official_state_changed": False,
                    "orders_changed": False,
                    "retrospective_expired": retrospective_expired,
                    "exact_automatic_message": False,
                    "delivery_status": None,
                    "feishu_delivery_confirmed": False,
                }

            self._verify_entry_binding(status_after)
            entry_record = await self._repository.get_outbox_event(
                status_after.event_id,
                route_id=self.config.route_id,
                **self._ledger_scope,
            )
            if entry_record is None:
                raise V20RepositoryError("committed V20 entry outbox event is unreadable")
            if entry_record.payload is None:
                await self._require_manual_trigger_ready()
                await self._repository.assert_runtime_leader()
                entry_record = await self._repository.seal_event(
                    status_after.event_id,
                    seal_v20_payload,
                )
            symbols = [
                {
                    "rank": item.get("rank"),
                    "code": item.get("code"),
                    "name": item.get("name"),
                    "snapshot_price": item.get("snapshot_price"),
                }
                for item in (status_after.semantic.get("symbols") or [])
                if isinstance(item, Mapping)
            ]
            created = status_before is None
            return {
                "accepted": True,
                "created": created,
                "manual_request_id": request_id,
                "trade_date": trade_date.isoformat(),
                "cycle_result": "DECISION_COMMITTED" if created else "ALREADY_TERMINAL",
                "formal_decision_available": True,
                "entry_action": status_after.action,
                "entry_event_id": status_after.event_id,
                "symbols": symbols,
                "official_state_changed": created,
                "orders_changed": False,
                "retrospective_expired": retrospective_expired,
                "exact_automatic_message": True,
                "delivery_status": entry_record.delivery_status,
                "feishu_delivery_confirmed": entry_record.delivery_status == "SENT",
            }

    async def trigger_canonical_selection_check_only(
        self,
        request_id: str,
        now: datetime | None = None,
    ) -> Mapping[str, Any]:
        """Hydrate today's canonical master and prepare V20 without official writes.

        This is the post-cutoff operator path whether or not today's official
        slot is already terminal.  It reads the durable canonical artifact
        first and joins the one shared V16 master only when the ticket is
        absent.  The prepared strategy result is persisted solely as an
        operator notification: no entry commit, model batch, model leg, exit
        intent, holding, or order is created or changed.
        """

        await self._require_manual_trigger_ready()
        if not isinstance(request_id, str) or _MANUAL_REQUEST_ID.fullmatch(request_id) is None:
            raise ValueError(
                "Idempotency-Key must be 8-128 characters using letters, digits, . _ : or -"
            )
        current = self._aware_now(now)
        trade_date = current.date()
        if current < _local(trade_date, self.config.clock.publish_deadline):
            raise V20StateConflict("canonical selection check-only is post-cutoff only")
        event_id_value = named_hash(
            "V20_CANONICAL_SELECTION_CHECK_ONLY_EVENT_ID_V1",
            {
                "route_id": self.config.route_id,
                "official_stream_id": self.config.official_stream_id,
                "state_lineage_id": self.config.state_lineage_id,
                "config_hash": self.config.config_hash,
                "state_semantics_hash": self.config.state_semantics_hash,
                "trade_date": trade_date.isoformat(),
                "manual_request_id": request_id,
            },
        )

        def _response(record: OutboxRecord, *, created: bool) -> Mapping[str, Any]:
            semantic = record.semantic
            expected = {
                "event_id": event_id_value,
                "alert_code": "MANUAL_0939_CHAIN_PROBE_RESULT",
                "manual_request_id": request_id,
                "event_trade_date": trade_date.isoformat(),
                # This legacy field means the current V20 policy calculator
                # ran; the explicit canonical fields below also prove that the
                # scanner reran from durable raw evidence.
                "current_version_recomputed": True,
                "replay_reused": False,
                "policy_recomputed": True,
                "official_state_changed": False,
                "orders_changed": False,
                "non_actionable": True,
                "retrospective_expired": True,
            }
            if any(semantic.get(key) != value for key, value in expected.items()):
                raise V20SemanticConflict(
                    "canonical selection check-only event has incompatible semantics"
                )
            canonical_source = semantic.get("canonical_source")
            scanner_recomputed = semantic.get("canonical_selection_recomputed")
            artifact_compared = semantic.get("canonical_artifact_compared")
            artifact_matches = semantic.get("canonical_artifact_matches")
            if (
                canonical_source != "PERSISTED_RAW_SCANNER_RECOMPUTATION"
                or scanner_recomputed is not True
                or type(artifact_compared) is not bool
                or artifact_matches not in {True, False, None}
                or (artifact_matches is not None) != artifact_compared
            ):
                raise V20SemanticConflict(
                    "canonical selection check-only source disclosure is inconsistent"
                )
            return {
                "accepted": True,
                "created": created,
                "manual_request_id": request_id,
                "operator_event_id": record.event_id,
                "manual_event_id": record.event_id,
                "event_trade_date": trade_date.isoformat(),
                "trade_date": trade_date.isoformat(),
                "cycle_result": "CANONICAL_CHECK_ONLY_READY",
                "formal_decision_available": semantic.get("official_entry_event_id") is not None,
                "entry_action": semantic.get("v20_action"),
                "v20_action": semantic.get("v20_action"),
                "final_multiplier": semantic.get("final_multiplier"),
                "current_version_recomputed": True,
                "canonical_selection_recomputed": scanner_recomputed,
                "canonical_artifact_compared": artifact_compared,
                "canonical_artifact_matches": artifact_matches,
                "canonical_source": canonical_source,
                "policy_recomputed": True,
                "calculation_result": semantic.get("calculation_result"),
                "official_comparison_result": semantic.get("official_comparison_result"),
                "official_comparison_unavailable_reason": semantic.get(
                    "official_comparison_unavailable_reason"
                ),
                "official_mismatch_fields": list(semantic.get("official_mismatch_fields") or ()),
                "probe_result": semantic.get("probe_result"),
                "probe_mismatch_fields": list(semantic.get("probe_mismatch_fields") or ()),
                "replay_reused": False,
                "official_entry_action": semantic.get("official_entry_action"),
                "official_entry_event_id": semantic.get("official_entry_event_id"),
                "current_v16_snapshot_hash": semantic.get("current_v16_snapshot_hash"),
                "official_v16_snapshot_hash": semantic.get("official_v16_snapshot_hash"),
                "symbols": list(semantic.get("symbols") or ()),
                "official_state_changed": False,
                "orders_changed": False,
                "non_actionable": True,
                "retrospective_expired": True,
                "exact_automatic_message": False,
                "visible_message_mode": semantic.get("visible_message_mode"),
                "delivery_status": record.delivery_status,
                "feishu_delivery_confirmed": record.delivery_status == "SENT",
            }

        await self._repository.assert_runtime_leader()
        existing = await self._repository.get_outbox_event(
            event_id_value,
            route_id=self.config.route_id,
            **self._ledger_scope,
        )
        if existing is not None:
            if existing.payload is None:
                _response(existing, created=False)
                await self._require_manual_trigger_ready()
                await self._repository.assert_runtime_leader()
                existing = await self._repository.seal_event(
                    event_id_value,
                    seal_v20_payload,
                )
            return _response(existing, created=False)

        async with self._manual_trigger_lock:
            await self._require_manual_trigger_ready()
            await self._repository.assert_runtime_leader()
            existing = await self._repository.get_outbox_event(
                event_id_value,
                route_id=self.config.route_id,
                **self._ledger_scope,
            )
            if existing is not None:
                if existing.payload is None:
                    existing = await self._repository.seal_event(
                        event_id_value,
                        seal_v20_payload,
                    )
                return _response(existing, created=False)

            await self._decision_cycle_lock.acquire()
            try:
                status_before = await self._repository.get_entry_status(
                    self.config.official_stream_id,
                    trade_date,
                )
                if status_before is not None:
                    self._verify_entry_binding(status_before)
                status_before_fingerprint = self._entry_status_readonly_fingerprint(status_before)
                state_before = await self._repository.load_state(self.config.state_lineage_id)
                state_before_payload_hash = sha256_json(dict(state_before.payload))
                if state_before_payload_hash != state_before.state_hash:
                    raise V20SemanticConflict(
                        "official V20 state payload hash differs before check-only preparation"
                    )

                calculation = await self._orchestrate_morning_selection(
                    trade_date,
                    allow_legacy_terminal_fresh_theoretical=True,
                )
                calculation_completed_at = max(current, self._aware_now())
                bundle = calculation.bundle
                first_received_at = calculation.canonical_first_received_at
                official_v16_snapshot_hash: str | None = None
                if (
                    status_before is not None
                    and status_before.action != "INPUT_INVALID"
                    and not calculation.legacy_terminal_fresh_theoretical
                ):
                    official_semantic_v16_hash = status_before.semantic.get("v16_snapshot_hash")
                    official_snapshot_v16_hash = status_before.snapshot.get("v16_snapshot_hash")
                    if (
                        not isinstance(official_semantic_v16_hash, str)
                        or len(official_semantic_v16_hash) != 64
                        or official_semantic_v16_hash != official_snapshot_v16_hash
                    ):
                        raise V20SemanticConflict(
                            "official terminal canonical V16 identity is inconsistent"
                        )
                    official_v16_snapshot_hash = official_semantic_v16_hash

                # The shared computation always re-runs both the scanner and
                # current policy calculator from durable raw facts.  An
                # artifact hit is comparison evidence only; it is never used
                # as the calculation result or an old official message replay.
                prepared = calculation.prepared
                pure = dict(prepared.commit.semantic)
                if pure.get("action") not in {"ENTER", "BLOCK", "NO_SIGNAL"}:
                    raise V20SemanticConflict(
                        "canonical V20 check-only preparation did not produce a legal result"
                    )
                entry_render_semantic = dict(pure)
                symbols = list(pure.get("symbols") or ())
                mismatched_business_fields: list[str] = []
                if (
                    status_before is None
                    or status_before.action == "INPUT_INVALID"
                    or calculation.legacy_terminal_fresh_theoretical
                ):
                    official_comparison_result = "NOT_AVAILABLE"
                else:
                    mismatched_business_fields = [
                        field
                        for field in _ENTRY_BUSINESS_SEMANTIC_FIELDS
                        if status_before.semantic.get(field) != pure.get(field)
                    ]
                    if (
                        official_v16_snapshot_hash is not None
                        and official_v16_snapshot_hash != bundle.snapshot_hash
                    ):
                        mismatched_business_fields.append("v16_snapshot_hash")
                    mismatched_business_fields = sorted(set(mismatched_business_fields))
                    official_comparison_result = (
                        "DIFFERENT" if mismatched_business_fields else "MATCH"
                    )
                # ``probe_result`` is retained for already-deployed consumers.
                # It describes whether this calculation completed, not whether
                # an older official result happened to be identical.
                probe_result = "PASS"
                raw_codes = tuple(bundle.snapshot["scan_input_codes"])
                raw_records = await self._repository.list_raw_minute_bar_records(
                    raw_codes,
                    trade_date=trade_date,
                    end_labels=EARLY_RAW_BAR_LABELS,
                )
                state_after = await self._repository.load_state(self.config.state_lineage_id)
                status_after = await self._repository.get_entry_status(
                    self.config.official_stream_id,
                    trade_date,
                )
                if (
                    state_after.revision != state_before.revision
                    or state_after.state_hash != state_before.state_hash
                    or sha256_json(dict(state_after.payload)) != state_before_payload_hash
                    or self._entry_status_readonly_fingerprint(status_after)
                    != status_before_fingerprint
                ):
                    raise V20StateConflict(
                        "official V20 state changed during canonical check-only preparation"
                    )
            finally:
                self._decision_cycle_lock.release()

            semantic = {
                "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
                "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
                "event_id": event_id_value,
                "strategy_version": self.config.strategy_version,
                "config_hash": self.config.config_hash,
                "state_semantics_hash": self.config.state_semantics_hash,
                "deployment_mode": self.config.deployment_mode,
                "official_stream_id": self.config.official_stream_id,
                "state_lineage_id": self.config.state_lineage_id,
                "alert_code": "MANUAL_0939_CHAIN_PROBE_RESULT",
                "delivery_priority_class": "OPERATOR_NOTIFICATION",
                "manual_request_id": request_id,
                "event_trade_date": trade_date.isoformat(),
                "probe_profile": "CURRENT_DEPLOYED_CODE_EXACT_0939_ENTRY_RENDER_V2",
                "calculation_result": "SUCCESS",
                "official_comparison_result": official_comparison_result,
                "official_comparison_unavailable_reason": (
                    "LEGACY_TERMINAL_PRESTATE_UNAVAILABLE"
                    if calculation.legacy_terminal_fresh_theoretical
                    else None
                ),
                "official_mismatch_fields": mismatched_business_fields,
                "probe_result": probe_result,
                "probe_mismatch_fields": [],
                "current_version_recomputed": True,
                "replay_reused": False,
                "canonical_source": calculation.canonical_source,
                "canonical_selection_recomputed": True,
                "canonical_artifact_compared": calculation.canonical_artifact_compared,
                "canonical_artifact_matches": calculation.canonical_artifact_matches,
                "policy_recomputed": True,
                "data_source": "PERSISTED_CANONICAL_EARLY_THROUGH_09:39",
                "data_window_start": "00:00",
                "data_window_end": "09:39",
                "v16_count": len(symbols),
                "v20_action": pure["action"],
                "final_multiplier": pure["final_multiplier"],
                "symbols": symbols,
                "raw_fact_n": len(raw_records),
                "quote_coverage": bundle.snapshot.get("scan_input_coverage"),
                "computed_at": calculation_completed_at.isoformat(),
                "canonical_first_received_at": first_received_at.isoformat(),
                "current_v16_snapshot_hash": bundle.snapshot_hash,
                "official_v16_snapshot_hash": official_v16_snapshot_hash,
                "official_entry_action": (
                    status_before.action if status_before is not None else "MISSING"
                ),
                "official_entry_event_id": (
                    status_before.event_id if status_before is not None else None
                ),
                "official_entry_present": status_before is not None,
                "official_state_changed": False,
                "orders_changed": False,
                "non_actionable": True,
                "retrospective_expired": True,
                "visible_message_mode": "MANUAL_OPERATOR_RENDER",
                "entry_render_semantic": entry_render_semantic,
                "message": (
                    "当前V20策略计算器已基于持久化 canonical 09:39 事实完成一次只读核查；"
                    f"canonical来源={calculation.canonical_source}；"
                    "本次计算=SUCCESS；"
                    f"早盘正式结果对比={official_comparison_result}；"
                    "未修改正式决策、模型批次、模型腿、持仓或订单。"
                ),
            }
            await self._decision_cycle_lock.acquire()
            try:
                await self._require_manual_trigger_ready()
                await self._repository.assert_runtime_leader()
                final_state = await self._repository.load_state(self.config.state_lineage_id)
                final_status = await self._repository.get_entry_status(
                    self.config.official_stream_id,
                    trade_date,
                )
                if (
                    final_state.revision != state_before.revision
                    or final_state.state_hash != state_before.state_hash
                    or sha256_json(dict(final_state.payload)) != state_before_payload_hash
                    or self._entry_status_readonly_fingerprint(final_status)
                    != status_before_fingerprint
                ):
                    raise V20StateConflict(
                        "official V20 state changed before canonical check-only persistence"
                    )
                created = await self._repository.enqueue_alert(
                    event_id_value,
                    self.config.route_id,
                    semantic,
                    sha256_json(semantic),
                    **self._ledger_scope,
                )
                await self._require_manual_trigger_ready()
                await self._repository.assert_runtime_leader()
                sealed = await self._repository.seal_event(
                    event_id_value,
                    seal_v20_payload,
                )
            finally:
                self._decision_cycle_lock.release()
            return _response(sealed, created=created)

    async def run_once(
        self,
        now: datetime | None = None,
        *,
        include_exit_cycles: bool = True,
        include_outbox_recovery: bool = True,
    ) -> None:
        """Run one deterministic scheduler iteration (also used by tests)."""
        self._require_running()
        await self._repository.assert_runtime_leader()
        current = self._aware_now(now)
        # Today's D1/D2 dates are already frozen exchange-calendar evidence, so
        # their live protection may start before today's calendar refresh.  A
        # stale leg waits for the refresh in the normal path, avoiding a false
        # "next session" instruction on a real trading day.
        exit_context = (
            self._context
            if self._context is not None and self._context.trade_date == current.date()
            else _DayContext(trade_date=current.date(), calendar=())
        )
        exit_task: asyncio.Task[Any] | None = None
        if include_exit_cycles:
            exit_task = asyncio.create_task(
                self._run_phase_isolated(
                    exit_context,
                    current,
                    "EXIT_CYCLE_FAILED",
                    self._run_live_exit_tick(exit_context, current),
                ),
                name=f"v20-exit-cycle-{current.date().isoformat()}",
            )
        # A Friday commit still needs sealing on Saturday, but recovery is an
        # auxiliary lane: slow/corrupt old outbox rows must never sit in front
        # of today's stop checks or the 09:40 entry deadline.
        outbox_task: asyncio.Task[Any] | None = None
        if include_outbox_recovery:
            outbox_task = asyncio.create_task(
                self._run_phase_isolated(
                    exit_context,
                    current,
                    "OUTBOX_RECOVERY_FAILED",
                    asyncio.wait_for(
                        self._seal_pending_outbox(),
                        timeout=OUTBOX_RECOVERY_TICK_TIMEOUT_SECONDS,
                    ),
                ),
                name=f"v20-outbox-recovery-{current.date().isoformat()}",
            )
        pre_calendar_tasks = tuple(task for task in (exit_task, outbox_task) if task is not None)
        try:
            calendar = await self._load_trade_calendar(current.date())
        except asyncio.CancelledError:
            for task in pre_calendar_tasks:
                task.cancel()
            await asyncio.gather(*pre_calendar_tasks, return_exceptions=True)
            raise
        except Exception:
            await asyncio.gather(*pre_calendar_tasks, return_exceptions=True)
            # A prolonged calendar outage must not make old active legs immortal.
            # Recover them conservatively with an explicit NEXT_TRADING_SESSION
            # public action instead of suppressing the exit altogether.
            if include_exit_cycles:
                await self._run_phase_isolated(
                    exit_context,
                    current,
                    "STALE_EXIT_CALENDAR_UNKNOWN",
                    self._run_stale_exit_cycle(exit_context, current),
                )
            raise
        exit_context.calendar = calendar
        stale_exit_task: asyncio.Task[Any] | None = None
        if include_exit_cycles:
            stale_exit_task = asyncio.create_task(
                self._run_phase_isolated(
                    exit_context,
                    current,
                    "STALE_EXIT_CYCLE_FAILED",
                    self._run_stale_exit_cycle(exit_context, current),
                ),
                name=f"v20-stale-exit-cycle-{current.date().isoformat()}",
            )
        exit_tasks = tuple(
            task for task in (*pre_calendar_tasks, stale_exit_task) if task is not None
        )
        if current.date() not in calendar:
            self._context = _DayContext(
                trade_date=current.date(),
                calendar=calendar,
                last_phase="NON_TRADING_DAY",
            )
            await asyncio.gather(*exit_tasks)
            self._last_success_at = current
            return
        try:
            context = await self._ensure_context(current, calendar)
            bootstrap_covers_today = await self._bootstrap_anchor_covers(context.trade_date)
        except asyncio.CancelledError:
            for task in exit_tasks:
                task.cancel()
            await asyncio.gather(*exit_tasks, return_exceptions=True)
            raise
        except Exception:
            await asyncio.gather(*exit_tasks, return_exceptions=True)
            raise
        if bootstrap_covers_today:
            # A checkpoint is an end-of-day state through its as-of date.  It
            # may be installed on that same date, but the target lineage must
            # not consume the date a second time.  The first eligible slot is
            # the next exchange trading day.
            context.last_phase = "BOOTSTRAP_AS_OF_DAY"
            await asyncio.gather(*exit_tasks)
            self._last_success_at = current
            return
        try:
            # Trigger-side entry housekeeping is independent of the previous
            # state and mature-shadow gates.  Its own 09:39 boundary decides
            # when the independent MEWS repair may be scheduled.
            await self._run_phase_isolated(
                context,
                current,
                "ENTRY_COLLECTION_FAILED",
                self._run_entry_collection_cycle(context, current),
            )
            reconciliation_ready = await self._run_phase_isolated(
                context,
                current,
                "MISSED_SLOT_RECONCILIATION_FAILED",
                asyncio.wait_for(
                    self._reconcile_missed_slots(current, calendar),
                    timeout=20.0,
                ),
            )

            # Reconcile every older D0 reference against the original D1 09:30
            # receipt cutoff before consuming any mature shadow batch.  This is
            # what makes a restart on D1/D3 equivalent to an uninterrupted worker:
            # already-persisted D0 09:41 evidence is recovered, while a bar first
            # received after the cutoff can never be promoted retroactively.
            reference_ready = await self._run_phase_isolated(
                context,
                current,
                "REFERENCE_EXPIRY_FAILED",
                self._expire_reference_gaps(context, current),
            )

            # Mature shadow facts are consumed before today's state transition.
            maturity_ready = reference_ready and reconciliation_ready
            if (
                reference_ready
                and reconciliation_ready
                and current.timetz().replace(tzinfo=None) >= self.config.clock.prewarm
            ):
                maturity_timeout = (
                    5.0 if current.timetz().replace(tzinfo=None) >= time(9, 35) else 60.0
                )
                maturity_phase_ready = await self._run_phase_isolated(
                    context,
                    current,
                    "SHADOW_MATURITY_FAILED",
                    asyncio.wait_for(
                        self._process_mature_shadow(context, current),
                        timeout=maturity_timeout,
                    ),
                )
                maturity_ready = maturity_phase_ready and context.maturity_done

            # The daily slot has the only hard 09:40/09:45 deadlines.  It is never
            # placed behind a stale exit leg, reminder, or reference-data error.
            if reconciliation_ready and (
                maturity_ready
                or current.timetz().replace(tzinfo=None) >= self.config.clock.publish_deadline
            ):
                await self._run_phase_isolated(
                    context,
                    current,
                    "ENTRY_CYCLE_FAILED",
                    self._run_entry_cycle(context, current),
                )
            else:
                context.last_phase = (
                    "ENTRY_BLOCKED_BY_MATURITY"
                    if reconciliation_ready
                    else "ENTRY_BLOCKED_BY_STATE_RECONCILIATION"
                )
            await self._run_phase_isolated(
                context,
                current,
                "REFERENCE_LOCK_FAILED",
                self._run_reference_cycle(context, current),
            )
            # A terminal entry slot is final for the production scheduler.
            # Late 09:39 replay remains an explicit operator diagnostic only;
            # automatically scheduling it here would run a second selection
            # after cutoff and publish a misleading "expired review" message.
            await asyncio.gather(*exit_tasks)
        finally:
            for task in exit_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*exit_tasks, return_exceptions=True)
        await self._run_phase_isolated(
            context,
            current,
            "REMINDER_CYCLE_FAILED",
            self._run_reminders(context, current),
        )

    async def _run_phase_isolated(
        self,
        context: _DayContext,
        now: datetime,
        alert_code: str,
        operation: Awaitable[None],
        *,
        lane_name: str = "decision",
    ) -> bool:
        """Keep an auxiliary phase failure from starving the daily slot."""

        try:
            await operation
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"
            self._record_lane_error(lane_name, f"{alert_code}: {detail}", now)
            context.last_phase = alert_code
            logger.exception("V20 isolated phase failed: %s", alert_code)
            # A live-exit phase can already have settled a narrower durable
            # incident (stage timeout, market outage, or one failed leg).  Only
            # an explicit ``True`` suppresses the umbrella; False/None means
            # persistence was not proven and the generic alert is the fallback.
            if getattr(exc, "diagnostic_alert_emitted", False) is not True:
                await self._safe_alert(
                    code=alert_code,
                    entity_id=context.trade_date.isoformat(),
                    message=detail,
                    now=now,
                )
            return False

    def _exit_context_for(self, now: datetime, *, stale: bool) -> _DayContext:
        attribute = "_stale_exit_context" if stale else "_exit_context"
        context = getattr(self, attribute)
        calendar = self._calendar_cache if self._calendar_loaded_for == now.date() else ()
        if context is None or context.trade_date != now.date():
            context = _DayContext(trade_date=now.date(), calendar=calendar)
            setattr(self, attribute, context)
        elif calendar:
            # Tuple assignment is atomic; the decision lane remains the sole
            # owner of calendar refresh while exit lanes consume a read-only view.
            context.calendar = calendar
        return context

    def _live_exit_tick_budget(self) -> float:
        cadence = float(self.config.market.exit_poll_seconds)
        return min(LIVE_EXIT_MAX_TICK_SECONDS, cadence * 12.0 / 15.0)

    def _live_exit_scheduler_budget(self) -> float:
        cadence = float(self.config.market.exit_poll_seconds)
        return min(LIVE_EXIT_SCHEDULER_WATCHDOG_SECONDS, cadence * 14.0 / 15.0)

    async def _record_live_exit_stage_incident(
        self,
        context: _DayContext,
        now: datetime,
        exc: V20LiveExitStageTimeout,
    ) -> None:
        # Sort defensively: the semantic identity of an incident must not
        # depend on the caller's symbol ordering.
        sorted_symbols = sorted(set(exc.symbols))
        symbols = ",".join(sorted_symbols)
        incident_id = named_hash(
            "V20_LIVE_EXIT_STAGE_INCIDENT_ID_V2",
            {
                "route_id": self.config.route_id,
                "official_stream_id": self.config.official_stream_id,
                "state_lineage_id": self.config.state_lineage_id,
                "trade_date": context.trade_date.isoformat(),
                "stage": exc.stage,
                "symbols": tuple(sorted_symbols),
                "provider": exc.provider,
            },
        )
        # The absolute monotonic deadline and the per-tick elapsed/remaining
        # values change every tick, so they stay in the log only; the persisted
        # semantic must be replay-stable for the fixed
        # (date, stage, provider, symbols) incident id.
        logger.warning(
            "V20 live-exit stage timeout: stage=%s provider=%s symbols=%s "
            "deadline=%.3f elapsed=%.3f remaining=%.3f",
            exc.stage,
            exc.provider,
            symbols or "all",
            exc.deadline,
            exc.elapsed_seconds,
            exc.remaining_seconds,
        )
        alert_result = await self._safe_alert(
            code="LIVE_EXIT_STAGE_TIMEOUT",
            entity_id=f"{context.trade_date.isoformat()}:{symbols or 'all'}:{exc.stage}",
            message=str(exc),
            now=now,
            event_id=incident_id,
            semantic_extras={
                "incident_id": incident_id,
                "error": type(exc).__name__,
                "route_id": self.config.route_id,
                "official_stream_id": self.config.official_stream_id,
                "state_lineage_id": self.config.state_lineage_id,
                "stage": exc.stage,
                "symbol": symbols,
                "symbols": sorted_symbols,
                "provider": exc.provider,
            },
        )
        exc.diagnostic_alert_emitted = alert_result

    async def _run_live_exit_stage(
        self,
        operation_factory: Callable[[], Awaitable[Any]],
        *,
        stage: str,
        stage_cap: float,
        deadline: float,
        tick_started_at: float,
        symbols: Sequence[str],
        provider: str,
    ) -> Any:
        """Run one budgeted stage; the operation is created only after the budget check.

        Callers pass a factory, never an already-created coroutine: when the
        tick deadline is already exhausted the stage raises without invoking
        the factory, so no DB/provider call is made and no coroutine or orphan
        task is created.
        """
        loop = asyncio.get_running_loop()
        remaining = deadline - loop.time()
        if remaining <= 0:
            observed = loop.time()
            raise V20LiveExitStageTimeout(
                stage=stage,
                elapsed_seconds=max(0.0, observed - tick_started_at),
                remaining_seconds=max(0.0, deadline - observed),
                deadline=deadline,
                symbols=tuple(sorted(set(symbols))),
                provider=provider,
            )
        budget = min(stage_cap, remaining)
        if budget == remaining:
            budget -= LIVE_EXIT_MIN_DEADLINE_RESERVE_SECONDS
        if budget <= 0:
            observed = loop.time()
            raise V20LiveExitStageTimeout(
                stage=stage,
                elapsed_seconds=max(0.0, observed - tick_started_at),
                remaining_seconds=max(0.0, deadline - observed),
                deadline=deadline,
                symbols=tuple(sorted(set(symbols))),
                provider=provider,
            )
        operation = operation_factory()
        operation_task: asyncio.Future[Any] = asyncio.ensure_future(operation)
        try:
            return await asyncio.wait_for(asyncio.shield(operation_task), timeout=budget)
        except asyncio.TimeoutError as exc:
            observed = loop.time()
            raise V20LiveExitStageTimeout(
                stage=stage,
                elapsed_seconds=max(0.0, observed - tick_started_at),
                remaining_seconds=max(0.0, deadline - observed),
                deadline=deadline,
                symbols=tuple(sorted(set(symbols))),
                provider=provider,
            ) from exc
        finally:
            if not operation_task.done():
                operation_task.cancel()
            await asyncio.gather(operation_task, return_exceptions=True)

    async def _run_live_exit_tick(self, context: _DayContext, now: datetime) -> None:
        """Run today's D1/D2 protection under a budget shorter than its cadence."""
        loop = asyncio.get_running_loop()
        budget = self._live_exit_tick_budget()
        tick_started_at = loop.time()
        deadline = tick_started_at + budget

        def lock_timeout() -> V20LiveExitStageTimeout:
            return V20LiveExitStageTimeout(
                stage="lock",
                elapsed_seconds=loop.time() - tick_started_at,
                remaining_seconds=max(0.0, deadline - loop.time()),
                deadline=deadline,
                symbols=(),
                provider="internal",
            )

        try:
            # The shared monotonic deadline is the only budget: no extra
            # wait_for around the locked cycle.  Only the lock acquisition
            # itself is bounded, and its timeout becomes a structured
            # stage="lock" incident like every other stage.
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise lock_timeout()
            try:
                await asyncio.wait_for(self._live_exit_lock.acquire(), timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise lock_timeout() from exc
            try:
                await self._run_exit_cycle(
                    context,
                    now,
                    include_stale=False,
                    deadline=deadline,
                    tick_started_at=tick_started_at,
                )
            finally:
                self._live_exit_lock.release()
        except V20LiveExitStageTimeout as exc:
            # latest/db/rules/history stage timeouts all land here; record the
            # structured incident once, then let the scheduler lane observe it.
            if not exc.diagnostic_alert_emitted:
                await self._record_live_exit_stage_incident(context, now, exc)
            raise

    async def _wait_for_runtime_tick(self, started_at: float, cadence: float) -> None:
        remaining = max(0.0, cadence - (asyncio.get_running_loop().time() - started_at))
        if remaining == 0:
            await asyncio.sleep(0)
            return
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=remaining)
        except TimeoutError:
            pass

    async def _run_live_exit_scheduler(self) -> None:
        """Keep stop evaluation independent from slow entry/prewarm work."""

        cadence = float(self.config.market.exit_poll_seconds)
        while not self._stop_event.is_set():
            started_at = asyncio.get_running_loop().time()
            now = self._aware_now()
            try:
                await self._repository.assert_runtime_leader()
                context = self._exit_context_for(now, stale=False)
                # Leave one second for diagnostic persistence while keeping the
                # entire lane below the next fixed cadence boundary.
                succeeded = await asyncio.wait_for(
                    self._run_phase_isolated(
                        context,
                        now,
                        "LIVE_EXIT_CYCLE_FAILED",
                        self._run_live_exit_tick(context, now),
                        lane_name="live_exit",
                    ),
                    timeout=self._live_exit_scheduler_budget(),
                )
                if succeeded:
                    self._record_lane_success("live_exit", now)
            except asyncio.CancelledError:
                raise
            except V20LeadershipLost as exc:
                self._record_lane_error("live_exit", f"LEADERSHIP_LOST: {exc}", now)
                self._stop_event.set()
                raise
            except TimeoutError:
                self._record_lane_error("live_exit", "LIVE_EXIT_CYCLE_TIMEOUT", now)
                logger.error("V20 live exit cycle exceeded its fixed cadence budget")
            except Exception as exc:
                self._record_lane_error(
                    "live_exit",
                    f"LIVE_EXIT_SCHEDULER_FAILED: {type(exc).__name__}: {exc}",
                    now,
                )
                logger.exception("V20 live exit scheduler iteration failed")
            await self._wait_for_runtime_tick(started_at, cadence)

    async def _run_stale_exit_scheduler(self) -> None:
        """Recover old model legs in a bounded lane that cannot delay live stops."""

        while not self._stop_event.is_set():
            started_at = asyncio.get_running_loop().time()
            now = self._aware_now()
            try:
                await self._repository.assert_runtime_leader()
                context = self._exit_context_for(now, stale=True)
                succeeded = await asyncio.wait_for(
                    self._run_phase_isolated(
                        context,
                        now,
                        "STALE_EXIT_CYCLE_FAILED",
                        self._run_closed_and_stale_exit_cycle(context, now),
                        lane_name="stale_exit",
                    ),
                    timeout=STALE_EXIT_TICK_TIMEOUT_SECONDS,
                )
                if succeeded:
                    self._record_lane_success("stale_exit", now)
            except asyncio.CancelledError:
                raise
            except V20LeadershipLost as exc:
                self._record_lane_error("stale_exit", f"LEADERSHIP_LOST: {exc}", now)
                self._stop_event.set()
                raise
            except TimeoutError:
                self._record_lane_error("stale_exit", "STALE_EXIT_CYCLE_TIMEOUT", now)
                logger.error("V20 stale exit recovery exceeded its bounded tick")
            except Exception as exc:
                self._record_lane_error(
                    "stale_exit",
                    f"STALE_EXIT_SCHEDULER_FAILED: {type(exc).__name__}: {exc}",
                    now,
                )
                logger.exception("V20 stale exit scheduler iteration failed")
            await self._wait_for_runtime_tick(started_at, STALE_EXIT_TICK_SECONDS)

    async def _run_outbox_recovery_scheduler(self) -> None:
        """Seal committed events independently of decision and exit computation."""

        while not self._stop_event.is_set():
            started_at = asyncio.get_running_loop().time()
            now = self._aware_now()
            try:
                await self._repository.assert_runtime_leader()
                await asyncio.wait_for(
                    self._run_outbox_recovery_tick(),
                    timeout=OUTBOX_RECOVERY_LANE_TIMEOUT_SECONDS,
                )
                self._record_lane_success("outbox_recovery", now)
            except asyncio.CancelledError:
                raise
            except V20LeadershipLost as exc:
                self._record_lane_error("outbox_recovery", f"LEADERSHIP_LOST: {exc}", now)
                self._stop_event.set()
                raise
            except TimeoutError:
                self._record_lane_error("outbox_recovery", "OUTBOX_RECOVERY_TIMEOUT", now)
                logger.error("V20 outbox recovery exceeded its bounded tick")
            except Exception as exc:
                self._record_lane_error(
                    "outbox_recovery",
                    f"OUTBOX_RECOVERY_SCHEDULER_FAILED: {type(exc).__name__}: {exc}",
                    now,
                )
                logger.exception("V20 outbox recovery scheduler iteration failed")
            await self._wait_for_runtime_tick(started_at, OUTBOX_RECOVERY_TICK_SECONDS)

    async def _run_outbox_recovery_tick(self) -> None:
        """Seal outbox rows, then refresh the request-free status snapshot."""

        await self._seal_pending_outbox(fail_on_error=True)
        await self._refresh_status_snapshot()

    async def _run_publisher_scheduler(self) -> None:
        """Publish durable events while exposing relay/lease health separately."""

        def cycle_success() -> None:
            self._record_lane_success("publisher", self._aware_now())

        def cycle_error(detail: str) -> None:
            self._record_lane_error("publisher", f"PUBLISH_FAILED: {detail}", self._aware_now())

        try:
            await self._publisher.run(
                self._stop_event,
                before_cycle=self._repository.assert_runtime_leader,
                on_cycle_success=cycle_success,
                on_cycle_error=cycle_error,
            )
        except V20LeadershipLost as exc:
            self._record_lane_error("publisher", f"LEADERSHIP_LOST: {exc}", self._aware_now())
            self._stop_event.set()
            raise

    async def _refresh_mews_cache_once(
        self,
        now: datetime,
        calendar: Sequence[date],
    ) -> bool:
        """Run the scheduled 09:10 MEWS attempt through the shared singleflight.

        The scheduler and any missing-cache selection trigger join one per-date
        task — including its failure — so an overlapping window never starts a
        second raw attempt.
        """

        current = self._aware_now(now)
        wall = current.timetz().replace(tzinfo=None)
        if self._mews_cached_for == current.date():
            return False
        if current.date() not in calendar:
            return False
        if wall < MEWS_PUBLISH_TIME or wall >= MEWS_CACHE_CUTOFF:
            return False
        task = await self._mews_singleflight_join(current, stage="SCHEDULED_0910")
        return await self._await_mews_singleflight(task)

    async def _calculate_mews_once(
        self,
        now: datetime,
        calendar: Sequence[date],
        *,
        source_trade_date: date | None = None,
    ) -> bool:
        """Calculate a missing daily value; time affects eligibility, never calculation."""

        current = self._aware_now(now)
        if source_trade_date is None and current.date() not in calendar:
            return False
        if source_trade_date is None:
            predecessors = [day for day in calendar if day < current.date()]
            if not predecessors:
                raise V20RepositoryError("MEWS calendar has no preceding trading day")
            source_trade_date = predecessors[-1]
        elif source_trade_date >= current.date():
            raise V20RepositoryError("MEWS source trade date must precede availability date")
        if self._mews_cache_matches(current.date(), source_trade_date):
            return False
        source = self._mews_source
        if source is None:
            raise V20ConfigError("V20 local MEWS calculator is not configured")

        async with self._mews_refresh_lock:
            if self._mews_cache_matches(current.date(), source_trade_date):
                return False
            published = await source.fetch_snapshot(
                source_trade_date=source_trade_date,
                availability_date=current.date(),
            )
            payload = dict(published)
            generated_at = datetime.fromisoformat(str(payload.get("generated_at")))
            if generated_at.tzinfo is None or generated_at.utcoffset() is None:
                raise MewsSnapshotSourceError("calculated MEWS generated_at is timezone-naive")
            await self._repository.record_mews_snapshot(payload)
            cutoff = _local(current.date(), MEWS_CACHE_CUTOFF)
            if self._mews_guard_store is not None:
                selected_snapshot_id = await self._mews_guard_store.find_eligible_snapshot(
                    source_trade_date=source_trade_date,
                    cutoff=cutoff,
                    availability_date=current.date(),
                )
                eligible = selected_snapshot_id == str(payload["snapshot_id"])
                if not eligible:
                    raise MewsSnapshotSourceError(
                        "locally calculated MEWS did not pass the sealed receipt guard"
                    )
            else:
                # Compatibility-only repositories retain their historical
                # test surface.  Connected PostgreSQL services always own the
                # strict guard store and never call these repository methods.
                eligible = await self._repository.mews_snapshot_is_eligible(
                    str(payload["snapshot_id"]),
                    source_trade_date=source_trade_date,
                    cutoff=cutoff,
                )
                if not eligible and self._has_mews_receipt_guard():
                    try:
                        eligible = await V20MewsReceiptGuard(self._repository).is_eligible(
                            str(payload["snapshot_id"]),
                            source_trade_date=source_trade_date,
                            cutoff=cutoff,
                            availability_date=current.date(),
                        )
                    except Exception as exc:
                        raise V20RepositoryError("MEWS same-day receipt guard failed") from exc
                    if not eligible:
                        raise MewsSnapshotSourceError(
                            "MEWS same-day repair receipt is not sealed for the availability date"
                        )
            self._mews_cached_for = current.date()
            self._mews_source_trade_date = source_trade_date
            self._mews_snapshot_id = str(payload["snapshot_id"])
            self._mews_last_failure = None
            self._mews_failed_for = None
            logger.info(
                "V20 cached locally calculated MEWS %s for %s (cutoff_eligible=%s)",
                self._mews_snapshot_id,
                current.date(),
                eligible,
            )
            return True

    def _mews_cache_matches(
        self,
        availability_date: date,
        source_trade_date: date,
    ) -> bool:
        return (
            self._mews_cached_for == availability_date
            and self._mews_source_trade_date == source_trade_date
        )

    def kick_mews_for_selection_trigger(
        self,
        now: datetime | None = None,
    ) -> asyncio.Task[bool]:
        """Start/join the managed MEWS task without blocking D0 selection."""

        self._require_running()
        current = self._aware_now(now)

        async def _kick() -> bool:
            return await self.ensure_mews_for_selection_trigger(current)

        task = asyncio.create_task(
            _kick(),
            name=f"v20-mews-trigger-{current.date().isoformat()}",
        )
        self._mews_trigger_tasks.add(task)

        def _finished(finished: asyncio.Task[bool]) -> None:
            self._mews_trigger_tasks.discard(finished)
            try:
                finished.exception()
            except asyncio.CancelledError:
                pass

        task.add_done_callback(_finished)
        return task

    async def ensure_mews_for_selection_trigger(
        self,
        now: datetime | None = None,
    ) -> bool:
        """Join the per-date MEWS singleflight and await it before continuing.

        The trigger blocks until the shared attempt settles: success means the
        snapshot is already persisted, while a genuine failure settles one
        daily idempotent ``MEWS_CALCULATION_FAILED`` before the independent
        entry path continues.  A finished task is cleared, so a later distinct
        trigger retries while the cache is still missing — there is no
        permanent daily failure skip and no orphan background task.
        """

        self._require_running()
        current = self._aware_now(now)
        if self._mews_cached_for == current.date():
            return True
        task = await self._mews_singleflight_join(current, stage="SELECTION_TRIGGER")
        return await self._await_mews_singleflight(task)

    async def _ensure_mews_for_exit_date(
        self,
        target_date: date,
        *,
        source_trade_date: date,
        now: datetime,
    ) -> bool:
        """Calculate the exact daily D2 value without using process time."""

        self._require_running()
        target = _local(target_date, MEWS_PUBLISH_TIME)
        task = await self._mews_singleflight_join(
            target,
            stage="D2_EXIT",
            source_trade_date=source_trade_date,
        )
        await self._await_mews_singleflight(task)
        return self._mews_cache_matches(target_date, source_trade_date)

    async def _mews_singleflight_join(
        self,
        current: datetime,
        *,
        stage: str,
        source_trade_date: date | None = None,
    ) -> asyncio.Task[bool]:
        """Return the live per-date shared attempt, creating it exactly once."""

        async with self._mews_singleflight_lock:
            if self._stop_event.is_set():
                raise V20RepositoryError("V20 MEWS singleflight is stopped")
            self._require_running()
            task = self._mews_singleflight_task
            if task is not None and not task.done():
                if self._mews_singleflight_date == current.date() and (
                    source_trade_date is None
                    or self._mews_singleflight_source_trade_date == source_trade_date
                ):
                    return task
                task.cancel()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            if self._mews_singleflight_task is task:
                self._mews_singleflight_task = None
                self._mews_singleflight_date = None
                self._mews_singleflight_source_trade_date = None
            if self._stop_event.is_set():
                raise V20RepositoryError("V20 MEWS singleflight is stopped")
            self._require_running()
            task = asyncio.create_task(
                self._mews_singleflight_attempt(
                    current,
                    stage=stage,
                    source_trade_date=source_trade_date,
                ),
                name=f"v20-mews-singleflight-{current.date().isoformat()}",
            )
            self._mews_singleflight_task = task
            self._mews_singleflight_date = current.date()
            self._mews_singleflight_source_trade_date = source_trade_date
            return task

    async def _await_mews_singleflight(
        self,
        task: asyncio.Task[bool],
    ) -> bool:
        """Await the managed per-date attempt; there is no artificial outer budget.

        The attempt is finitely bounded by its own per-call provider and
        PostgreSQL timeouts, so callers simply await the shared task: an outer
        wall-clock cap would be a structural budget inversion that could abort
        a legitimate multi-call catch-up.  The shield keeps the managed task
        alive for peer joiners when this caller is cancelled; ``stop`` still
        cancels and awaits the managed task, so no orphan survives.
        """

        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            # Our own cancellation must propagate; a stop/peer cancellation of
            # the managed task surfaces through the shield as attempt failure.
            current_task = asyncio.current_task()
            if current_task is not None and current_task.cancelling():
                raise
            return False
        except V20LeadershipLost:
            raise
        except Exception:
            # The attempt body already settled the daily idempotent alert
            # before raising; every joiner shares that same failure.
            return False
        finally:
            async with self._mews_singleflight_lock:
                if self._mews_singleflight_task is task and task.done():
                    # Clear the finished task so a later distinct trigger retries
                    # the attempt while the cache is still missing.
                    self._mews_singleflight_task = None
                    self._mews_singleflight_date = None
                    self._mews_singleflight_source_trade_date = None

    async def _mews_singleflight_attempt(
        self,
        current: datetime,
        *,
        stage: str,
        source_trade_date: date | None = None,
    ) -> bool:
        """Fill a missing daily MEWS value as the shared per-date attempt."""

        self._require_running()
        await self._repository.assert_runtime_leader()
        calendar: Sequence[date] = ()
        try:
            if source_trade_date is None:
                calendar = await self._load_trade_calendar(current.date())
                if current.date() not in calendar:
                    self._record_lane_success("mews_cache", current)
                    return False
            # The long-running scheduler performs its own restart restore
            # before the scheduled 09:10 calculation.  A post-cutoff recovery
            # may be invoked directly, so it owns the same restore here.  A
            # selection-trigger miss deliberately calculates locally instead
            # of depending on a stale external/computed source.
            if (stage == "SCHEDULED_AFTER_CUTOFF_RECOVERY" or self._mews_guard_store is None) and (
                self._mews_cached_for != current.date()
                or (
                    source_trade_date is not None
                    and self._mews_source_trade_date != source_trade_date
                )
            ):
                await self._restore_mews_cache_once(
                    current,
                    calendar,
                    source_trade_date=source_trade_date,
                )
            await self._calculate_mews_once(
                current,
                calendar,
                source_trade_date=source_trade_date,
            )
            self._record_lane_success("mews_cache", self._aware_now())
            if source_trade_date is not None:
                return self._mews_cache_matches(current.date(), source_trade_date)
            return self._mews_cached_for == current.date()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._alert_mews_calculation_failure(
                exc,
                current,
                stage=stage,
                calendar=calendar,
                source_trade_date=source_trade_date,
            )
            raise

    async def _alert_mews_calculation_failure(
        self,
        exc: Exception,
        now: datetime,
        *,
        stage: str,
        calendar: Sequence[date],
        source_trade_date: date | None = None,
    ) -> bool:
        """Report one stable, idempotent daily failure with full context.

        A genuine local calculation failure is the only condition that may
        raise a MEWS alarm; success must never be followed by a missed-cache
        alarm or a permanent fallback declaration.
        """

        current = self._aware_now(now)
        if source_trade_date is None:
            predecessors = [day for day in calendar if day < current.date()]
            source_label = predecessors[-1].isoformat() if predecessors else "UNKNOWN"
        else:
            source_label = source_trade_date.isoformat()
        self._mews_last_failure = f"{type(exc).__name__}: {exc}"
        self._record_lane_error(
            "mews_cache",
            f"MEWS_CALCULATION_FAILED: {self._mews_last_failure}",
            current,
        )
        if self._mews_alerted_for == current.date():
            return True
        alert_persisted = await self._safe_alert(
            code="MEWS_CALCULATION_FAILED",
            entity_id=current.date().isoformat(),
            message=(
                f"阶段={stage}; 异常={type(exc).__name__}; "
                f"availability_date={current.date().isoformat()}; "
                f"source_trade_date={source_label}; "
                f"详情: {exc}"
            ),
            now=current,
            event_id=named_hash(
                "V20_MEWS_CALCULATION_FAILED_EVENT_ID_V1",
                {
                    "alert_code": "MEWS_CALCULATION_FAILED",
                    "entity_id": current.date().isoformat(),
                    "route_id": self.config.route_id,
                    "official_stream_id": self.config.official_stream_id,
                    "state_lineage_id": self.config.state_lineage_id,
                    "trade_date": current.date().isoformat(),
                },
            ),
        )
        if alert_persisted is True:
            self._mews_alerted_for = current.date()
            return True
        return False

    async def _recover_mews_after_cutoff_once(
        self,
        now: datetime,
        calendar: Sequence[date],
    ) -> bool:
        """Repair a missing daily value after 09:40 through the shared singleflight.

        The daily MEWS value is a pure function of the source trade date's
        market facts, so a late calculation is never invalid.  The scheduler
        tick joins and awaits the exact same per-date singleflight task as the
        09:10 refresh and every missing-cache selection trigger — including
        its failure — so overlapping callers never start a second raw attempt.
        Only a genuine local failure raises the daily idempotent alert (inside
        the shared attempt) and latches ``_mews_failed_for`` purely to prevent
        a tight scheduler retry loop; a later manual trigger ignores that
        latch and joins a fresh attempt.  No fallback snapshot is ever written.
        """

        current = self._aware_now(now)
        if self._mews_cached_for == current.date() or current.date() not in calendar:
            return True
        if self._mews_failed_for == current.date():
            return False
        task = await self._mews_singleflight_join(
            current,
            stage="SCHEDULED_AFTER_CUTOFF_RECOVERY",
        )
        recovered = await self._await_mews_singleflight(task)
        if recovered or self._mews_cached_for == current.date():
            return True
        if self._mews_alerted_for == current.date():
            self._mews_failed_for = current.date()
        return False

    async def _restore_mews_cache_once(
        self,
        now: datetime,
        calendar: Sequence[date],
        *,
        source_trade_date: date | None = None,
    ) -> bool:
        """Reattach to today's already-sealed cache after a process restart."""

        current = self._aware_now(now)
        if source_trade_date is None and current.date() not in calendar:
            return False
        if source_trade_date is None:
            predecessors = [day for day in calendar if day < current.date()]
            if not predecessors:
                raise V20RepositoryError("MEWS calendar has no preceding trading day")
            source_trade_date = predecessors[-1]
        if self._mews_cache_matches(current.date(), source_trade_date):
            return False
        cutoff = _local(current.date(), MEWS_CACHE_CUTOFF)
        if self._mews_guard_store is not None:
            snapshot_id = await self._mews_guard_store.find_eligible_snapshot(
                source_trade_date=source_trade_date,
                cutoff=cutoff,
                availability_date=current.date(),
            )
            if snapshot_id is None:
                return False
        else:
            snapshot_id = await self._repository.find_eligible_mews_snapshot(
                source_trade_date=source_trade_date,
                cutoff=cutoff,
                availability_date=current.date(),
            )
            if snapshot_id is None:
                return False
            eligible = await self._repository.mews_snapshot_is_eligible(
                snapshot_id,
                source_trade_date=source_trade_date,
                cutoff=cutoff,
            )
            if not eligible and self._has_mews_receipt_guard():
                try:
                    eligible = await V20MewsReceiptGuard(self._repository).is_eligible(
                        snapshot_id,
                        source_trade_date=source_trade_date,
                        cutoff=cutoff,
                        availability_date=current.date(),
                    )
                except Exception as exc:
                    raise V20RepositoryError("MEWS same-day receipt guard failed") from exc
            if not eligible:
                return False
        self._mews_cached_for = current.date()
        self._mews_source_trade_date = source_trade_date
        self._mews_snapshot_id = snapshot_id
        self._mews_last_failure = None
        logger.info("V20 restored cached MEWS %s after restart", snapshot_id)
        return True

    def _has_mews_receipt_guard(self) -> bool:
        return getattr(self._repository, "pool", None) is not None and isinstance(
            getattr(self._repository, "schema", None), str
        )

    async def _run_mews_cache_scheduler(self) -> None:
        """Cache at 09:10, retry until 09:40, then repair locally exactly once."""

        while not self._stop_event.is_set():
            now = self._aware_now()
            try:
                await self._repository.assert_runtime_leader()
                wall = now.timetz().replace(tzinfo=None)
                calendar: Sequence[date] = ()
                if wall >= MEWS_PUBLISH_TIME:
                    calendar = await self._load_trade_calendar(now.date())
                is_trading_day = now.date() in calendar
                lane_ok = True
                if is_trading_day and self._mews_cached_for != now.date():
                    await self._restore_mews_cache_once(now, calendar)
                if is_trading_day and MEWS_PUBLISH_TIME <= wall < MEWS_CACHE_CUTOFF:
                    await self._refresh_mews_cache_once(now, calendar)
                    # A failed attempt already recorded its lane error through
                    # the daily idempotent alert; only a filled cache is a
                    # lane success — never let an empty cache wash it green.
                    lane_ok = self._mews_cached_for == now.date()
                elif (
                    is_trading_day
                    and wall >= MEWS_CACHE_CUTOFF
                    and self._mews_cached_for != now.date()
                ):
                    lane_ok = await self._recover_mews_after_cutoff_once(now, calendar)
                if lane_ok:
                    self._record_lane_success("mews_cache", now)
            except asyncio.CancelledError:
                raise
            except V20LeadershipLost as exc:
                self._record_lane_error(
                    "mews_cache",
                    f"LEADERSHIP_LOST: {exc}",
                    now,
                )
                self._stop_event.set()
                raise
            except Exception as exc:
                self._mews_last_failure = f"{type(exc).__name__}: {exc}"
                self._record_lane_error(
                    "mews_cache",
                    f"MEWS_CACHE_FAILED: {self._mews_last_failure}",
                    now,
                )
                logger.warning("V20 MEWS cache refresh failed: %s", self._mews_last_failure)
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=MEWS_CACHE_POLL_SECONDS,
                )
            except TimeoutError:
                pass

    async def _run_scheduler(self) -> None:
        while not self._stop_event.is_set():
            now = self._aware_now()
            try:
                error_revision_before = self._lane_health["decision"].error_revision
                async with self._decision_cycle_lock:
                    await self._run_decision_iteration_with_cutoff(now)
                if self._lane_health["decision"].error_revision == error_revision_before:
                    self._record_lane_success("decision", now)
            except asyncio.CancelledError:
                raise
            except V20LeadershipLost as exc:
                self._record_lane_error("decision", f"LEADERSHIP_LOST: {exc}", now)
                self._stop_event.set()
                logger.critical("V20 runtime leadership was lost; scheduler is terminating")
                raise
            except Exception as exc:
                self._record_lane_error("decision", f"{type(exc).__name__}: {exc}", now)
                logger.exception("V20 scheduler iteration failed")
                await self._safe_alert(
                    code="SCHEDULER_ITERATION_FAILED",
                    entity_id=now.date().isoformat(),
                    message=f"{type(exc).__name__}: {exc}",
                    now=now,
                )
            interval = self._scheduler_interval(now)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except TimeoutError:
                pass

    def _known_entry_trade_date(self, trade_date: date) -> bool:
        """Return true only when exchange-calendar evidence covers the date."""

        if self._calendar_loaded_for == trade_date and trade_date in self._calendar_cache:
            return True
        return bool(
            self._context is not None
            and self._context.trade_date == trade_date
            and trade_date in self._context.calendar
        )

    async def _run_decision_iteration_with_cutoff(
        self,
        _sampled_at: datetime,
    ) -> None:
        """Let a started calculation finish while retaining the write fence.

        The 09:40 boundary controls whether the computed proposal may commit;
        it never cancels the data/validation/policy calculation itself.  The
        commit path re-samples the wall clock after calculation, while the
        database remains authoritative for the irreversible write boundary.
        """

        # This watchdog is allowed to bypass the normal ``run_once`` ordering at
        # the hard deadline, so it must carry its own leader fence.  In
        # particular, an old worker whose advisory-lock session has disappeared
        # must not create a terminal slot or public outbox row before it notices
        # that a replacement worker is now authoritative.
        await self._repository.assert_runtime_leader()
        current = self._aware_now()
        deadline = _local(current.date(), self.config.clock.publish_deadline)
        if current >= deadline:
            cutoff_reached = await self._enforce_or_alert_entry_cutoff(
                current.date(),
                now=current,
            )
            if not cutoff_reached:
                # The application clock may be ahead of PostgreSQL.  Do not run
                # any locally-post-cutoff phase until the authoritative database
                # clock reaches the irreversible boundary.
                return
            await self.run_once(
                current,
                include_exit_cycles=False,
                include_outbox_recovery=False,
            )
            return

        await self.run_once(
            current,
            include_exit_cycles=False,
            include_outbox_recovery=False,
        )
        cutoff_now = self._aware_now()
        if cutoff_now >= deadline:
            await self._enforce_or_alert_entry_cutoff(
                current.date(),
                now=cutoff_now,
            )

    async def _enforce_or_alert_entry_cutoff(
        self,
        trade_date: date,
        *,
        now: datetime,
    ) -> bool:
        """Fail closed only after leader and database-clock cutoff confirmation.

        ``True`` means PostgreSQL has authoritatively reached the boundary, even
        when the date is a confirmed exchange closure and no public event is
        required.  ``False`` means a fast application clock reached 09:40 first
        and the caller must wait for a later scheduler iteration.
        """

        # Re-probe after any pre-cutoff operation was cancelled.  A leader
        # session can be lost while that operation is blocked, and all deadline
        # writes/alerts must stop before a new leader can race this runtime.
        await self._repository.assert_runtime_leader()
        deadline = _local(trade_date, self.config.clock.publish_deadline)
        if now < deadline:
            return False
        if not await self._repository.database_cutoff_reached(deadline):
            return False

        # A date absent from a successfully loaded calendar is a confirmed
        # exchange closure and must stay quiet.
        calendar_known = self._calendar_loaded_for == trade_date
        if trade_date.weekday() >= 5 or (
            calendar_known and not self._known_entry_trade_date(trade_date)
        ):
            return True
        terminal_status = await self._get_verified_cutoff_entry_status(trade_date)
        if terminal_status is not None:
            return True
        if calendar_known:
            await self._enforce_entry_cutoff(trade_date, now=now)
            return True
        # Cold start: a weekday with no calendar evidence at the authoritative
        # cutoff gets exactly one bounded load attempt before failing closed.
        try:
            await asyncio.wait_for(
                self._load_trade_calendar(trade_date),
                timeout=_CALENDAR_CUTOFF_LOAD_BUDGET_SECONDS,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_lane_error(
                "decision",
                (
                    "ENTRY_CALENDAR_UNKNOWN_AT_0940: "
                    f"phase=calendar_load "
                    f"budget_seconds={_CALENDAR_CUTOFF_LOAD_BUDGET_SECONDS} "
                    f"{type(exc).__name__}: {exc}"
                ),
                now,
            )
            terminal_status = await self._get_verified_cutoff_entry_status(trade_date)
            if terminal_status is not None:
                return True
            await self._alert_entry_cutoff_no_buy(trade_date, now=now)
            return True
        # Re-judge with the freshly loaded evidence: a trading day takes the
        # normal cutoff path while a confirmed closure stays quiet.
        if self._known_entry_trade_date(trade_date):
            await self._enforce_entry_cutoff(trade_date, now=now)
        return True

    async def _enforce_entry_cutoff(
        self,
        trade_date: date,
        *,
        now: datetime | None = None,
    ) -> None:
        """Durably fail closed at 09:40, or emit an idempotent no-buy alert."""

        current = self._aware_now(now)
        deadline = _local(trade_date, self.config.clock.publish_deadline)
        if current < deadline or not self._known_entry_trade_date(trade_date):
            return
        context = (
            self._context
            if self._context is not None and self._context.trade_date == trade_date
            else None
        )
        if context is not None:
            try:
                if await self._refresh_entry_status(context) is not None:
                    return
                await self._finalize_invalid_entry(
                    context,
                    current,
                    reason="ENTRY_INPUT_UNAVAILABLE_BY_0940",
                    detail=(
                        "no durable normal V16 decision existed at the strict "
                        "09:40 boundary; "
                        + (context.last_entry_failure_detail or f"last_phase={context.last_phase}")
                    ),
                    invalid_commit_not_before_ts=deadline,
                )
                return
            except asyncio.CancelledError:
                raise
            except V20LeadershipLost:
                raise
            except Exception as exc:
                # Missing predecessor state can make the ordered slot impossible
                # to commit at this instant.  The scheduler will retry and the
                # normal reconciliation path will backfill it later; meanwhile
                # the operator still receives one stable, durable no-buy fact.
                detail = f"{type(exc).__name__}: {exc}"
                self._record_lane_error(
                    "decision",
                    f"ENTRY_CUTOFF_FINALIZATION_FAILED: {detail}",
                    current,
                )
                logger.exception("V20 could not finalize the 09:40 entry slot")

        await self._alert_entry_cutoff_no_buy(trade_date, now=current)

    async def _get_verified_cutoff_entry_status(
        self,
        trade_date: date,
    ) -> EntryStatus | None:
        status = await self._repository.get_entry_status(
            self.config.official_stream_id,
            trade_date,
        )
        if status is None:
            return None
        self._verify_entry_binding(status)
        if status.action not in _LATE_0939_REPLAY_ACTIONS:
            raise V20SemanticConflict("cutoff terminal entry action is invalid")
        return status

    async def _alert_entry_cutoff_no_buy(
        self,
        trade_date: date,
        *,
        now: datetime,
    ) -> None:
        await self._safe_alert(
            code="ENTRY_CUTOFF_NO_BUY",
            entity_id=trade_date.isoformat(),
            message="09:40 截止仍没有 durable 正常入场决定；今天不买，不要追买。",
            now=now,
            event_id=named_hash(
                "V20_ENTRY_CUTOFF_NO_BUY_EVENT_ID_V1",
                {
                    "alert_code": "ENTRY_CUTOFF_NO_BUY",
                    "entity_id": trade_date.isoformat(),
                    "route_id": self.config.route_id,
                    "official_stream_id": self.config.official_stream_id,
                    "state_lineage_id": self.config.state_lineage_id,
                    "trade_date": trade_date.isoformat(),
                },
            ),
        )

    async def _ensure_context(self, now: datetime, calendar: tuple[date, ...]) -> _DayContext:
        if self._context is not None and self._context.trade_date == now.date():
            await self._refresh_entry_status(self._context)
            return self._context
        status = await self._repository.get_entry_status(
            self.config.official_stream_id,
            now.date(),
        )
        if status is not None:
            self._verify_entry_binding(status)
        self._context = _DayContext(
            trade_date=now.date(),
            calendar=calendar,
            entry_status=status,
            last_phase="DECISION_COMMITTED" if status else "PREWARM_PENDING",
        )
        return self._context

    async def _refresh_entry_status(self, context: _DayContext) -> EntryStatus | None:
        """Reattach a same-day context to an already committed durable slot."""

        if context.entry_status is not None:
            return context.entry_status
        status = await self._repository.get_entry_status(
            self.config.official_stream_id,
            context.trade_date,
        )
        if status is not None:
            self._verify_entry_binding(status)
            context.entry_status = status
            context.last_phase = "DECISION_COMMITTED"
        return status

    async def _load_trade_calendar(self, current_date: date) -> tuple[date, ...]:
        """Load one bounded, refreshable and validated exchange calendar."""

        if self._calendar_loaded_for == current_date and self._calendar_cache:
            return self._calendar_cache
        async with self._calendar_tasks_lock:
            if self._stop_event.is_set():
                raise V20RepositoryError("V20 trade-calendar task lane is stopped")
            stale_tasks = [
                task
                for task_date, task in self._calendar_tasks.items()
                if task_date != current_date and not task.done()
            ]
            for task_date, task in list(self._calendar_tasks.items()):
                if task_date != current_date and self._calendar_tasks.get(task_date) is task:
                    self._calendar_tasks.pop(task_date, None)
            for task in stale_tasks:
                task.cancel()
            if stale_tasks:
                await asyncio.gather(*stale_tasks, return_exceptions=True)
            if self._stop_event.is_set():
                raise V20RepositoryError("V20 trade-calendar task lane is stopped")
            current_task = self._calendar_tasks.get(current_date)
            if current_task is None:
                current_task = asyncio.create_task(
                    self._load_trade_calendar_once(current_date),
                    name=f"v20-calendar-{current_date.isoformat()}",
                )
                self._calendar_tasks[current_date] = current_task

                def _remove(task: asyncio.Task[tuple[date, ...]]) -> None:
                    if self._calendar_tasks.get(current_date) is task:
                        self._calendar_tasks.pop(current_date, None)

                current_task.add_done_callback(_remove)
        return await asyncio.shield(current_task)

    async def _load_trade_calendar_once(self, current_date: date) -> tuple[date, ...]:
        try:
            if self._calendar_provider is not None:
                raw = await asyncio.wait_for(
                    self._calendar_provider(),
                    timeout=TRADE_CALENDAR_TIMEOUT_SECONDS,
                )
            else:
                client = self._scan_state.realtime_client
                if client is None or not hasattr(client, "fetch_trade_calendar"):
                    raise V20RepositoryError("Tushare trade-calendar adapter is unavailable")
                raw = await asyncio.wait_for(
                    client.fetch_trade_calendar(
                        current_date - timedelta(days=TRADE_CALENDAR_PAST_DAYS),
                        current_date + timedelta(days=TRADE_CALENDAR_FUTURE_DAYS),
                    ),
                    timeout=TRADE_CALENDAR_TIMEOUT_SECONDS,
                )
            calendar = tuple(raw)
            if (
                not calendar
                or any(type(item) is not date for item in calendar)
                or tuple(sorted(set(calendar))) != calendar
            ):
                raise V20RepositoryError("trade calendar is empty, unsorted, or duplicated")
            if not any(day < current_date for day in calendar):
                raise V20RepositoryError("trade calendar has no historical predecessor")
            # A production entry needs D1 and D2 successors.  Requiring at
            # least two future open dates also catches an exhausted vendor
            # horizon before a last-of-year decision can be formed.
            if sum(day > current_date for day in calendar) < 2:
                raise V20RepositoryError("trade calendar has fewer than two future open dates")
        except asyncio.CancelledError:
            raise
        except Exception:
            # A cache loaded earlier in this same process remains usable only
            # while it still proves the predecessor and two successors.
            cached = self._calendar_cache
            if (
                cached
                and any(day < current_date for day in cached)
                and sum(day > current_date for day in cached) >= 2
            ):
                logger.exception("V20 trade-calendar refresh failed; retaining safe cache")
                self._calendar_loaded_for = current_date
                return cached
            raise
        self._calendar_cache = calendar
        self._calendar_loaded_for = current_date
        return calendar

    async def _bootstrap_anchor_covers(self, trade_date: date) -> bool:
        state = await self._repository.load_state(self.config.state_lineage_id)
        if state.revision != 0 or state.payload.get("last_terminal_trade_date") is not None:
            return False
        predecessor = await self._repository.load_bootstrap_predecessor_trade_date(
            **self._ledger_scope
        )
        if predecessor > trade_date:
            raise V20RepositoryError("bootstrap predecessor is later than the runtime date")
        return predecessor == trade_date

    async def _reconcile_missed_slots(
        self,
        now: datetime,
        calendar: tuple[date, ...],
    ) -> None:
        """Finalize every skipped trading day before today's state may advance."""

        state = await self._repository.load_state(self.config.state_lineage_id)
        raw_last = state.payload.get("last_terminal_trade_date")
        has_terminal_predecessor = raw_last is not None
        if raw_last is None:
            if state.revision != 0:
                raise V20RepositoryError("non-genesis V20 state has no last_terminal_trade_date")
            last_terminal = await self._repository.load_bootstrap_predecessor_trade_date(
                **self._ledger_scope
            )
        else:
            try:
                last_terminal = date.fromisoformat(str(raw_last))
            except ValueError as exc:
                raise V20RepositoryError("V20 state has an invalid terminal trade date") from exc
            if last_terminal not in calendar:
                raise V20RepositoryError(
                    "last V20 terminal date is absent from the exchange calendar"
                )
        if last_terminal > now.date():
            raise V20RepositoryError("V20 state terminal date is in the future")
        if last_terminal == now.date():
            return

        missed_dates = [
            trade_date for trade_date in calendar if last_terminal < trade_date < now.date()
        ]
        if not missed_dates:
            return
        if has_terminal_predecessor:
            predecessor = await self._repository.get_entry_status(
                self.config.official_stream_id,
                last_terminal,
            )
            if predecessor is None:
                raise V20RepositoryError("V20 state predecessor slot is missing")
            self._verify_entry_binding(predecessor)

        for missed_date in missed_dates:
            existing = await self._repository.get_entry_status(
                self.config.official_stream_id,
                missed_date,
            )
            if existing is not None:
                self._verify_entry_binding(existing)
                refreshed = await self._repository.load_state(self.config.state_lineage_id)
                refreshed_raw = refreshed.payload.get("last_terminal_trade_date")
                try:
                    refreshed_last = date.fromisoformat(str(refreshed_raw))
                except ValueError as exc:
                    raise V20RepositoryError(
                        "official state has invalid terminal date during reconciliation"
                    ) from exc
                if refreshed_last not in calendar or refreshed_last < missed_date:
                    raise V20RepositoryError(
                        "terminal missed-day slot is not reflected in official state"
                    )
                continue

            recovery_context = _DayContext(
                trade_date=missed_date,
                calendar=calendar,
                last_phase="MISSED_SLOT_RECONCILIATION",
            )
            # Preserve the same daily ordering as a live run: reference
            # arbitration, mature-shadow consumption, then the terminal slot.
            await self._expire_reference_gaps(recovery_context, now)
            await self._process_mature_shadow(recovery_context, now)
            if not recovery_context.maturity_done:
                raise V20RepositoryError(
                    f"cannot reconcile {missed_date}: mature shadow state is incomplete"
                )
            health, rolling, gaps = await self._policy_inputs(missed_date)
            current_state = await self._repository.load_state(self.config.state_lineage_id)
            prepared = prepare_invalid_entry(
                config=self.config,
                state=current_state,
                trade_date=missed_date,
                calendar=calendar,
                reason_code="MISSED_TRADING_DAY_DOWNTIME",
                detail=(
                    "service recovered after the fixed daily finalization deadline; "
                    "the skipped trading day is terminally blocked"
                ),
                invalid_commit_not_before_ts=_local(
                    missed_date,
                    self.config.clock.decision_finalization_deadline,
                ),
                completed_health=health,
                completed_rolling=rolling,
                maturity_gaps=gaps,
                scheduled_exits_today=await self._scheduled_exits_today(missed_date),
            )
            await self._repository.commit_entry(prepared.commit)
            await self._repository.seal_event(prepared.commit.event_id, seal_v20_payload)
            await self._safe_alert(
                code="MISSED_TRADING_DAY_DOWNTIME",
                entity_id=missed_date.isoformat(),
                message="停机交易日已按失败终态补齐；后续日期现在可以按顺序继续",
                now=now,
            )

    async def _seal_pending_outbox(self, *, fail_on_error: bool = False) -> None:
        failed: list[tuple[str, str]] = []
        event_ids = await self._repository.list_unsealed_outbox_event_ids(
            route_id=self.config.route_id,
            **self._ledger_scope,
            limit=20,
            after_event_id=None,
        )
        for pending_event_id in event_ids:
            try:
                await self._repository.seal_event(pending_event_id, seal_v20_payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                detail = f"{type(exc).__name__}: {exc}"
                failed.append((pending_event_id, detail))
                logger.exception("V20 outbox event could not be sealed: %s", pending_event_id)

                async def persist_seal_diagnostic() -> None:
                    await self._repository.record_outbox_seal_error(
                        pending_event_id,
                        detail,
                        route_id=self.config.route_id,
                        **self._ledger_scope,
                    )

                diagnostic_result = (
                    await asyncio.gather(
                        persist_seal_diagnostic(),
                        return_exceptions=True,
                    )
                )[0]
                if isinstance(diagnostic_result, asyncio.CancelledError):
                    raise diagnostic_result
                if isinstance(diagnostic_result, BaseException):
                    logger.error(
                        "V20 outbox seal diagnostic could not be persisted: %s",
                        pending_event_id,
                        exc_info=(
                            type(diagnostic_result),
                            diagnostic_result,
                            diagnostic_result.__traceback__,
                        ),
                    )
        for event_id_value, detail in failed:
            await self._safe_alert(
                code="OUTBOX_SEAL_FAILED",
                entity_id=event_id_value,
                message=detail,
                now=self._aware_now(),
            )
        if failed and fail_on_error:
            raise V20RepositoryError(
                f"{len(failed)} outbox event(s) remained unsealed after this recovery tick"
            )

    async def _process_mature_shadow(self, context: _DayContext, now: datetime) -> None:
        context.maturity_done = False
        pending = await self._repository.list_pending_shadow_batches(
            context.trade_date, **self._ledger_scope
        )
        # Legacy ROLLING7 rows are retired, inert schema history.  Market
        # health rolling facts mature through the independent fact store.
        pending = [item for item in pending if item.kind == "HEALTH"]
        if not pending:
            context.maturity_done = True
            return

        # Reference arbitration is owned by _expire_reference_gaps and must
        # happen before this method.  A PENDING row is therefore a real state
        # gap, not permission to discard possibly eligible persisted evidence.
        unresolved_health = any(item.reference_status == "PENDING" for item in pending)
        pending = [item for item in pending if item.reference_status != "PENDING"]

        daily_candidates: dict[tuple[date, datetime | None], list[Any]] = {}
        daily_corrupt_ids: dict[tuple[date, datetime | None], tuple[str, ...]] = {}
        # Poll each T+2 daily response at a bounded cadence and persist the raw
        # candidate before it can affect policy.  HEALTH later reads only the
        # candidate whose conservative post-commit receipt is no later than its
        # fixed D3 09:39 cutoff.
        daily_dates = {item.t2_date for item in pending if item.reference_status == "LOCKED"}
        for t2_date in sorted(daily_dates, reverse=True):
            last_attempt = context.maturity_daily_last_attempt.get(t2_date)
            if last_attempt is not None and (now - last_attempt).total_seconds() < 30:
                continue
            context.maturity_daily_last_attempt[t2_date] = now
            try:
                daily = await self._scan_state.realtime_client.fetch_daily_bars(
                    t2_date.strftime("%Y%m%d")
                )
                await self._repository.record_daily_bar_snapshot(
                    t2_date,
                    _daily_snapshot_payload(t2_date, daily),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # A fresh source poll is only a staging attempt.  It must not
                # hide a snapshot that was already durably eligible at the
                # HEALTH cutoff.
                await self._safe_alert(
                    code="DAILY_MATURITY_SOURCE_UNAVAILABLE",
                    entity_id=t2_date.isoformat(),
                    message=f"{type(exc).__name__}: {exc}",
                    now=now,
                )

        for batch in pending:
            health_cutoff = _local(
                _next_trade_date(context.calendar, batch.t2_date),
                time.fromisoformat(self.config.clock.decision_bar_label),
            )
            if now < health_cutoff or not await self._repository.database_cutoff_reached(
                health_cutoff
            ):
                unresolved_health = True
                continue
            snapshots: list[Any] = []
            candidate_key = (batch.t2_date, health_cutoff)
            if batch.reference_status == "LOCKED":
                if candidate_key not in daily_candidates:
                    loaded, corrupt_ids = await self._repository.list_daily_bar_snapshots(
                        batch.t2_date,
                        received_before=health_cutoff,
                    )
                    daily_candidates[candidate_key] = list(loaded)
                    daily_corrupt_ids[candidate_key] = corrupt_ids
                snapshots = daily_candidates[candidate_key]

            evaluated: list[tuple[Any, Any]] = []
            for candidate in snapshots:
                try:
                    daily = _daily_rows_from_snapshot(candidate.payload)
                except V20SemanticConflict:
                    continue
                candidate_result = evaluate_shadow_batch(
                    kind=batch.kind,
                    payload=batch.payload,
                    reference_status=batch.reference_status,
                    reference_prices=batch.reference_prices,
                    daily_bars=daily,
                )
                evaluated.append((candidate, candidate_result))
            selected = next(
                (item for item in evaluated if item[1].status == "COMPLETE_VALID"),
                evaluated[0] if evaluated else None,
            )
            if selected is None:
                snapshot = None
                result = evaluate_shadow_batch(
                    kind=batch.kind,
                    payload=batch.payload,
                    reference_status=batch.reference_status,
                    reference_prices=batch.reference_prices,
                    daily_bars={},
                )
            else:
                snapshot, result = selected
            if result.status == "INCOMPLETE":
                continue
            evidence = {
                **dict(result.payload_update),
                "daily_snapshot_id": snapshot.snapshot_id if snapshot else None,
                "daily_snapshot_hash": snapshot.source_hash if snapshot else None,
                "daily_snapshot_receipt": (
                    snapshot.first_received_at.isoformat() if snapshot else None
                ),
                "health_maturity_cutoff_ts": (
                    health_cutoff.isoformat() if health_cutoff is not None else None
                ),
                "daily_snapshot_corrupt_ids": list(daily_corrupt_ids.get(candidate_key, ())),
            }
            await self._repository.complete_shadow_batch(
                batch.batch_id,
                batch_return=result.batch_return,
                status=result.status,
                payload_update=evidence,
                not_before_ts=health_cutoff,
                official_stream_id=self.config.official_stream_id,
                lineage_id=self.config.state_lineage_id,
            )
        context.maturity_done = not unresolved_health

    async def _policy_inputs(
        self, trade_date: date
    ) -> tuple[list[CompletedHealth], list[CompletedRolling], list[ActiveRollingGap]]:
        health_rows = await self._repository.load_recent_completed(
            "HEALTH", before_t2=trade_date, limit=1_000, **self._ledger_scope
        )
        rolling_rows = await self._repository.load_rolling7_market_health(
            before_t2=trade_date,
            limit=1_000,
        )

        health = [
            CompletedHealth(
                batch_id=row.batch_id,
                signal_date=row.signal_date,
                t2_date=row.t2_date,
                relative_return=row.batch_return,
                valid=row.status == "COMPLETE_VALID",
                invalid_reason=(
                    None
                    if row.status == "COMPLETE_VALID"
                    else str(row.payload.get("invalid_reason", "HEALTH_BATCH_INVALID"))
                ),
            )
            for row in health_rows
        ]
        rolling = [
            CompletedRolling(
                batch_id=(
                    row.canonical_snapshot_id
                    if row.canonical_snapshot_id
                    else f"rolling7:{row.signal_date.isoformat()}"
                ),
                signal_date=row.signal_date,
                t2_date=row.t2_date or row.signal_date,
                batch_return=float(row.batch_return or 0.0),
            )
            for row in rolling_rows
            if row.signal_kind is SignalKind.SIGNAL
            and row.status.value == "COMPLETE"
            and row.batch_return is not None
        ]
        gaps = [
            ActiveRollingGap(
                gap_id=f"rolling7:{row.signal_date.isoformat()}",
                signal_date=row.signal_date,
                maturity_date=row.t2_date or row.signal_date,
            )
            for row in rolling_rows
            if row.status.value == "DATA_GAP"
        ]
        return health, rolling, gaps

    def _late_0939_replay_event_id(
        self,
        trade_date: date,
        *,
        official_entry_event_id: str,
    ) -> str:
        return named_hash(
            "V20_LATE_0939_REPLAY_EVENT_ID_V1",
            {
                "route_id": self.config.route_id,
                "official_stream_id": self.config.official_stream_id,
                "lineage_id": self.config.state_lineage_id,
                "state_semantics_hash": self.config.state_semantics_hash,
                "trade_date": trade_date.isoformat(),
                "official_entry_event_id": official_entry_event_id,
            },
        )

    def _verify_late_0939_replay_record(
        self,
        record: OutboxRecord,
        *,
        trade_date: date,
        official_entry_event_id: str,
        official_entry_action: str,
    ) -> None:
        semantic = record.semantic
        expected = {
            "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
            "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
            "event_id": record.event_id,
            "strategy_version": self.config.strategy_version,
            "state_semantics_hash": self.config.state_semantics_hash,
            "deployment_mode": self.config.deployment_mode,
            "official_stream_id": self.config.official_stream_id,
            "state_lineage_id": self.config.state_lineage_id,
            "alert_code": "LATE_0939_REPLAY_RESULT",
            "delivery_priority_class": "OPERATOR_NOTIFICATION",
            "event_trade_date": trade_date.isoformat(),
            "replay_kind": "RETROSPECTIVE_POST_CUTOFF",
            "non_actionable": True,
        }
        if (
            record.event_type != "DATA_ALERT"
            or record.event_id
            != self._late_0939_replay_event_id(
                trade_date,
                official_entry_event_id=official_entry_event_id,
            )
            or (
                record.route_id,
                record.official_stream_id,
                record.lineage_id,
            )
            != (
                self.config.route_id,
                self.config.official_stream_id,
                self.config.state_lineage_id,
            )
            or any(semantic.get(key) != value for key, value in expected.items())
            or not isinstance(semantic.get("config_hash"), str)
            or re.fullmatch(r"[0-9a-f]{64}", str(semantic.get("config_hash"))) is None
        ):
            raise V20SemanticConflict("late 09:39 replay event has incompatible semantics")
        if semantic.get("official_entry_event_id") != official_entry_event_id:
            raise V20SemanticConflict("late 09:39 replay official event binding is invalid")
        if official_entry_action not in _LATE_0939_REPLAY_ACTIONS:
            raise V20SemanticConflict("late 09:39 replay official entry action is invalid")
        if semantic.get("official_entry_action") != official_entry_action:
            raise V20SemanticConflict("late 09:39 replay official entry action is mismatched")
        action = semantic.get("replay_action")
        multiplier = semantic.get("final_multiplier")
        if action not in {"ENTER", "BLOCK", "NO_SIGNAL"}:
            raise V20SemanticConflict("late 09:39 replay action is invalid")
        if (
            isinstance(multiplier, bool)
            or not isinstance(multiplier, (int, float))
            or not math.isfinite(float(multiplier))
            or not 0 <= float(multiplier) <= 1
            or (action == "ENTER") != (float(multiplier) > 0)
        ):
            raise V20SemanticConflict("late 09:39 replay multiplier is invalid")
        if not isinstance(semantic.get("symbols"), list):
            raise V20SemanticConflict("late 09:39 replay symbols are invalid")

    @staticmethod
    def _policy_inputs_from_terminal_status(
        status: EntryStatus,
    ) -> tuple[list[CompletedHealth], list[CompletedRolling], list[ActiveRollingGap]]:
        raw = status.snapshot.get("policy_inputs")
        if not isinstance(raw, Mapping) or raw.get("schema_version") != (
            "v20-policy-input-snapshot/v1"
        ):
            raise V20SemanticConflict("terminal slot lacks frozen V20 policy inputs")
        if set(raw) != {
            "schema_version",
            "completed_health",
            "completed_rolling",
            "maturity_gaps",
        } or any(
            not isinstance(raw.get(field), list)
            for field in ("completed_health", "completed_rolling", "maturity_gaps")
        ):
            raise V20SemanticConflict("terminal slot policy input field set is malformed")
        frozen_policy_hash = status.snapshot.get("policy_input_hash")
        if (
            not isinstance(frozen_policy_hash, str)
            or sha256_json(raw) != frozen_policy_hash
            or frozen_policy_hash != status.semantic.get("policy_input_hash")
        ):
            raise V20SemanticConflict("terminal slot policy input hash mismatch")
        if status.snapshot.get(
            "trade_date"
        ) != status.trade_date.isoformat() or status.snapshot.get(
            "state_before_hash"
        ) != status.semantic.get("state_before_hash"):
            raise V20SemanticConflict("terminal slot snapshot/semantic binding mismatch")
        try:
            health: list[CompletedHealth] = []
            for item in raw["completed_health"]:
                if not isinstance(item, Mapping) or set(item) != {
                    "batch_id",
                    "signal_date",
                    "t2_date",
                    "relative_return",
                    "valid",
                    "invalid_reason",
                }:
                    raise ValueError("health field set")
                if type(item["valid"]) is not bool:
                    raise ValueError("health valid flag")
                signal_date = date.fromisoformat(str(item["signal_date"]))
                t2_date = date.fromisoformat(str(item["t2_date"]))
                relative_return = item["relative_return"]
                if relative_return is not None:
                    if isinstance(relative_return, bool):
                        raise ValueError("health return")
                    relative_return = float(relative_return)
                    if not math.isfinite(relative_return):
                        raise ValueError("health return")
                if bool(item["valid"]) != (relative_return is not None):
                    raise ValueError("health validity/return")
                invalid_reason = item["invalid_reason"]
                if invalid_reason is not None and (
                    not isinstance(invalid_reason, str) or not invalid_reason
                ):
                    raise ValueError("health invalid reason")
                if not isinstance(item["batch_id"], str) or not item["batch_id"]:
                    raise ValueError("health batch id")
                if not signal_date < t2_date < status.trade_date:
                    raise ValueError("health date order")
                health.append(
                    CompletedHealth(
                        batch_id=item["batch_id"],
                        signal_date=signal_date,
                        t2_date=t2_date,
                        relative_return=relative_return,
                        valid=item["valid"],
                        invalid_reason=invalid_reason,
                    )
                )

            rolling: list[CompletedRolling] = []
            for item in raw["completed_rolling"]:
                if not isinstance(item, Mapping) or set(item) != {
                    "batch_id",
                    "signal_date",
                    "t2_date",
                    "batch_return",
                }:
                    raise ValueError("rolling field set")
                if not isinstance(item["batch_id"], str) or not item["batch_id"]:
                    raise ValueError("rolling batch id")
                if isinstance(item["batch_return"], bool):
                    raise ValueError("rolling return")
                signal_date = date.fromisoformat(str(item["signal_date"]))
                t2_date = date.fromisoformat(str(item["t2_date"]))
                batch_return = float(item["batch_return"])
                if not math.isfinite(batch_return) or not signal_date < t2_date < status.trade_date:
                    raise ValueError("rolling value/date")
                rolling.append(
                    CompletedRolling(
                        batch_id=item["batch_id"],
                        signal_date=signal_date,
                        t2_date=t2_date,
                        batch_return=batch_return,
                    )
                )

            gaps: list[ActiveRollingGap] = []
            for item in raw["maturity_gaps"]:
                if not isinstance(item, Mapping) or set(item) != {
                    "gap_id",
                    "signal_date",
                    "maturity_date",
                    "closed",
                    "aged_out",
                }:
                    raise ValueError("gap field set")
                if (
                    not isinstance(item["gap_id"], str)
                    or not item["gap_id"]
                    or type(item["closed"]) is not bool
                    or type(item["aged_out"]) is not bool
                ):
                    raise ValueError("gap identity/flags")
                signal_date = date.fromisoformat(str(item["signal_date"]))
                maturity_date = date.fromisoformat(str(item["maturity_date"]))
                if not signal_date < maturity_date <= status.trade_date:
                    raise ValueError("gap date order")
                gaps.append(
                    ActiveRollingGap(
                        gap_id=item["gap_id"],
                        signal_date=signal_date,
                        maturity_date=maturity_date,
                        closed=item["closed"],
                        aged_out=item["aged_out"],
                    )
                )
            if len({item.batch_id for item in health}) != len(health):
                raise ValueError("duplicate health batch")
            if len({item.batch_id for item in rolling}) != len(rolling):
                raise ValueError("duplicate rolling batch")
            if len({item.gap_id for item in gaps}) != len(gaps):
                raise ValueError("duplicate gap")
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise V20SemanticConflict("terminal slot policy inputs are malformed") from exc
        return health, rolling, gaps

    @staticmethod
    def _state_before_from_terminal_status(status: EntryStatus) -> StateRecord:
        """Restore the immutable state input consumed by the terminal slot."""

        expected_hash = status.semantic.get("state_before_hash")
        if not isinstance(expected_hash, str):
            raise V20SemanticConflict("terminal slot lacks a valid canonical state_before payload")
        try:
            return restore_state_before(
                status.snapshot,
                expected_lineage_id=status.lineage_id,
                expected_state_before_hash=expected_hash,
            )
        except (TypeError, ValueError) as exc:
            raise V20SemanticConflict(
                "terminal slot lacks a valid canonical state_before payload"
            ) from exc

    @staticmethod
    def _terminal_lacks_canonical_state_before(status: EntryStatus) -> bool:
        """Recognize only the deployed legacy terminal contract.

        A present-but-invalid value is corruption, not legacy compatibility, and
        remains fail-closed in ``_state_before_from_terminal_status``.
        """

        return "state_before" not in status.snapshot

    def _validate_legacy_terminal_without_prestate(
        self,
        status: EntryStatus,
        trade_date: date,
    ) -> None:
        """Validate every surviving legacy terminal input before compatibility."""

        trade_date_text = trade_date.isoformat()
        state_before_hash = status.semantic.get("state_before_hash")
        state_after_hash = status.semantic.get("state_after_hash")
        if (
            status.action not in {"ENTER", "BLOCK", "NO_SIGNAL", "INPUT_INVALID"}
            or status.trade_date != trade_date
            or status.slot_id != official_slot_id(self.config.official_stream_id, trade_date_text)
            or status.semantic.get("trade_date") != trade_date_text
            or status.snapshot.get("trade_date") != trade_date_text
            or not isinstance(state_before_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", state_before_hash) is None
            or status.snapshot.get("state_before_hash") != state_before_hash
            or not isinstance(state_after_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", state_after_hash) is None
        ):
            raise V20SemanticConflict(
                "legacy terminal slot without state_before has invalid surviving identity"
            )
        # Compatibility changes which policy inputs are used for the fresh
        # theoretical result; it does not waive validation of the old terminal.
        self._policy_inputs_from_terminal_status(status)

    def _validate_current_state_for_legacy_terminal(
        self,
        state: StateRecord,
        status: EntryStatus,
        trade_date: date,
    ) -> None:
        """Prove the current state is valid and descends from today's terminal."""

        payload = dict(state.payload)
        if (
            state.lineage_id != self.config.state_lineage_id
            or type(state.revision) is not int
            or state.revision < 0
            or payload.get("state_revision") != state.revision
            or sha256_json(payload) != state.state_hash
            or payload.get("last_terminal_slot_id") != status.slot_id
            or payload.get("last_terminal_trade_date") != trade_date.isoformat()
        ):
            raise V20SemanticConflict(
                "current V20 state is not a valid descendant of the legacy terminal slot"
            )

    @staticmethod
    def _verify_terminal_replay_transition(
        status: EntryStatus,
        prepared: PreparedEntry,
    ) -> None:
        """Bind replay inputs to the immutable terminal slot and prestate.

        The replay deliberately does not require its newly calculated
        ``next_state_hash`` to equal the old terminal ``state_after_hash``.
        The former is current-code output: a legitimate strategy change must
        be rendered as a successful calculation whose official comparison is
        ``DIFFERENT``, rather than being rejected as corrupt input.
        """

        commit = prepared.commit
        frozen_prestate = status.snapshot.get("state_before")
        frozen_revision = (
            frozen_prestate.get("revision") if isinstance(frozen_prestate, Mapping) else None
        )
        frozen_hash = (
            frozen_prestate.get("state_hash") if isinstance(frozen_prestate, Mapping) else None
        )
        if (
            commit.official_stream_id != status.official_stream_id
            or commit.trade_date != status.trade_date
            or commit.slot_id != status.slot_id
            or commit.lineage_id != status.lineage_id
            or commit.expected_state_revision != frozen_revision
            or commit.expected_state_hash != frozen_hash
            or frozen_hash != status.semantic.get("state_before_hash")
        ):
            raise V20SemanticConflict(
                "terminal slot state transition does not bind to canonical state_before"
            )

    @staticmethod
    def _canonical_raw_top10_codes(canonical: CanonicalV16ScanBundle) -> tuple[str, ...]:
        """Return the theoretical Top10 codes (by rank) of the canonical scan.

        The post-cutoff replay cross-checks its persisted raw evidence against
        exactly these ten codes.  ``canonical.early_bars`` covers the whole
        ~3k ready universe and is persisted in full by the shared helper; the
        Top10 remains the subset the replay semantic binds to.  Fail closed
        unless ``scan_result.recommended`` is itself exactly ten stocks with
        unique integer ranks covering 1..10 and unique six-digit codes; the
        result is returned in rank order.
        """

        try:
            recommended = list(getattr(canonical.scan_result, "recommended", None) or ())
            if len(recommended) != 10:
                raise ValueError("recommended count")
            ranks = []
            for stock in recommended:
                if type(stock.rank) is not int:
                    raise ValueError("rank type")
                ranks.append(stock.rank)
            if sorted(ranks) != list(range(1, 11)):
                raise ValueError("rank set")
            codes = tuple(stock.code for stock in sorted(recommended, key=lambda stock: stock.rank))
            if any(
                not isinstance(code, str) or len(code) != 6 or not code.isdigit() for code in codes
            ):
                raise ValueError("code format")
            if len(set(codes)) != 10:
                raise ValueError("code uniqueness")
        except (AttributeError, TypeError, ValueError) as exc:
            raise V20SemanticConflict(
                "canonical V16 scan does not yield an exact unique Top10 for raw evidence"
            ) from exc
        return codes

    @staticmethod
    def _canonical_recommendation_codes(
        canonical: CanonicalV16ScanBundle,
    ) -> tuple[str, ...]:
        """Validate the legal V16 recommendation cardinality (zero through ten)."""

        try:
            recommended = list(getattr(canonical.scan_result, "recommended", None) or ())
            if not 0 <= len(recommended) <= 10:
                raise ValueError("recommended count")
            ranks = [stock.rank for stock in recommended]
            if any(type(rank) is not int for rank in ranks):
                raise ValueError("rank type")
            if sorted(ranks) != list(range(1, len(recommended) + 1)):
                raise ValueError("rank set")
            codes = tuple(stock.code for stock in sorted(recommended, key=lambda stock: stock.rank))
            if any(
                not isinstance(code, str) or len(code) != 6 or not code.isdigit() for code in codes
            ):
                raise ValueError("code format")
            if len(set(codes)) != len(codes):
                raise ValueError("code uniqueness")
        except (AttributeError, TypeError, ValueError) as exc:
            raise V20SemanticConflict(
                "canonical V16 scan has an invalid zero-to-ten recommendation list"
            ) from exc
        return codes

    async def _persist_canonical_raw_minute_bars(
        self,
        canonical: CanonicalV16ScanBundle,
    ) -> None:
        """Durably persist every ready universe code's actual early raw bars.

        Every entry path (automatic, manual, post-cutoff replay, restart) runs
        this single helper against the shared canonical bundle, scoped to every
        code in ``canonical.early_bars`` — the full ready universe, never just
        the Top10 — so a later restart can rehydrate the canonical scan from
        persisted evidence alone.  Every actual normalized early bar (any end
        label at or before 09:39, including the 09:25/09:30 strategy inputs)
        is persisted as-is; a row whose end label is outside the early label
        set (00:00..09:39) never enters the canonical raw evidence or its
        hashes.  There is no fixed-label checklist: each row must simply be
        valid, bound to its code and the trade date, carry a known early end
        label, and be unique per code/label, flattened through the real
        ``_bar_payload``; each ready code must contribute a valid target-date
        09:39 bar.  The repository's returned sealed
        hashes must cover every payload exactly; any mismatch fails closed.
        Same-process cache reuse does not re-run the persistence.
        """

        trade_date = canonical.trade_date
        if trade_date in self._canonical_raw_persisted_dates:
            return
        # Selection may legally yield NO_SIGNAL or fewer than ten candidates.
        # Raw durability belongs to the full ready universe and must not be
        # conditional on the recommendation count.  Historical replay paths
        # that explicitly require an exact Top10 retain their stricter helper.
        self._canonical_recommendation_codes(canonical)
        if not canonical.early_bars:
            raise V20RepositoryError("canonical V16 has no early raw bars to persist")
        payloads: list[dict[str, Any]] = []
        for code in sorted(canonical.early_bars):
            seen_labels: set[str] = set()
            for bar in canonical.early_bars.get(code, ()):
                if bar.end_label in seen_labels:
                    raise V20SemanticConflict(
                        f"canonical V16 has duplicate early raw bars for {code}"
                    )
                if (
                    not bar.is_valid
                    or bar.stock_code != code
                    or bar.bar_end.astimezone(SHANGHAI).date() != trade_date
                    or bar.end_label not in EARLY_RAW_BAR_LABELS
                ):
                    raise V20SemanticConflict(f"canonical V16 early raw bar is invalid for {code}")
                seen_labels.add(bar.end_label)
                payloads.append(_bar_payload(bar))
            if EARLY_RAW_LAST_LABEL not in seen_labels:
                raise V20SemanticConflict(
                    f"canonical V16 early raw bars lack a valid 09:39 bar for {code}"
                )
        expected_hashes = frozenset(sha256_json(payload) for payload in payloads)
        if len(expected_hashes) != len(payloads):
            raise V20SemanticConflict("canonical V16 early raw payloads are not unique")
        sealed_hashes = await self._repository.record_minute_bars(payloads)
        if frozenset(sealed_hashes) != expected_hashes:
            raise V20RepositoryError(
                "canonical V16 early raw persistence is incomplete: "
                f"{len(set(sealed_hashes) & expected_hashes)}/{len(payloads)} sealed"
            )
        self._canonical_raw_persisted_dates.add(trade_date)

    async def _persist_canonical_artifact_barrier(
        self,
        canonical: CanonicalV16ScanBundle,
    ) -> None:
        """Settle raw durability, portable artifact, readback, and hydration.

        Returning from the shared V16 sink is the durable completion boundary.
        The portable row is deliberately written only after every canonical raw
        minute row has been sealed; it is then read and hydrated through the
        same path used by a cold restart.  Consequently an artifact ticket can
        never be interpreted as a replacement for its raw evidence.
        """

        if not self._canonical_callbacks_open:
            raise V20StateConflict("canonical V16 durable sink is stopped")
        store = self._canonical_artifact_store
        if store is None:
            raise V20RepositoryError("canonical V16 artifact store is unavailable")
        if not isinstance(canonical, CanonicalV16ScanBundle):
            raise V20SemanticConflict("canonical V16 sink received an invalid master")

        async with self._canonical_artifact_lock:
            if not self._canonical_callbacks_open:
                raise V20StateConflict("canonical V16 durable sink is stopped")
            await self._persist_canonical_raw_minute_bars(canonical)

            # Acceptance fakes expose a hydrate method so they can model the
            # raw barrier without implementing PostgreSQL.  The production
            # store persists only the strict portable projection.
            store_hydrate = getattr(store, "hydrate", None)
            if callable(store_hydrate):
                durable_payload: Any = canonical
            else:
                existing = await store.load(
                    official_stream_id=self.config.official_stream_id,
                    trade_date=canonical.trade_date,
                    event=V16_CANONICAL_ARTIFACT_EVENT,
                )
                if existing is not None:
                    hydrated_existing = await self._hydrate_canonical_artifact_record(existing)
                    existing_integrity = existing.payload.get("canonical_integrity_hash")
                    if existing_integrity == canonical._integrity_hash:
                        if hydrated_existing.trade_date != canonical.trade_date:
                            raise V20SemanticConflict("canonical V16 artifact reuse date differs")
                        # ``computed_at`` is deliberately outside canonical
                        # semantic identity.  Preserve the first durable
                        # artifact/receipt instead of colliding merely because
                        # an equivalent retry finished one second later.
                        if callable(getattr(self._repository, "save_rolling7_market_health", None)):
                            await self._record_rolling7_intent_from_artifact(existing)
                        return
                canonical_calendar = tuple(canonical.computation_calendar)
                projected = self._project_canonical_v16(
                    canonical,
                    calendar=canonical_calendar,
                )
                durable_payload = encode_v16_canonical_artifact(
                    projected,
                    calendar=canonical_calendar,
                    canonical_integrity_hash=canonical._integrity_hash,
                )
            await store.save_once(
                durable_payload,
                official_stream_id=self.config.official_stream_id,
                trade_date=canonical.trade_date,
                event=V16_CANONICAL_ARTIFACT_EVENT,
            )
            record = await store.load(
                official_stream_id=self.config.official_stream_id,
                trade_date=canonical.trade_date,
                event=V16_CANONICAL_ARTIFACT_EVENT,
            )
            if record is None:
                raise V20RepositoryError("canonical V16 artifact readback is missing")
            hydrated = await self._hydrate_canonical_artifact_record(record)
            hydrated_trade_date = getattr(hydrated, "trade_date", None)
            if hydrated_trade_date != canonical.trade_date:
                raise V20SemanticConflict("canonical V16 artifact readback date differs")
            if callable(getattr(self._repository, "save_rolling7_market_health", None)):
                await self._record_rolling7_intent_from_artifact(record)
            completed_at = self._aware_now()
            if completed_at.date() == canonical.trade_date:
                self._canonical_barrier_completed_at[canonical.trade_date] = completed_at

    async def _record_rolling7_intent_from_artifact(self, record: Any) -> Rolling7Batch:
        try:
            batch = self._rolling7_batch_from_artifact(record)
        except (KeyError, TypeError, ValueError, V20SemanticConflict):
            expected_t2 = await self._rolling7_expected_t2(record.trade_date)
            batch = make_missing_canonical_batch(
                signal_date=record.trade_date,
                t2_date=expected_t2,
            )
        current = await self._repository.get_rolling7_market_health_for_date(record.trade_date)
        if current is not None and current.canonical_available:
            current_identity = (
                current.canonical_snapshot_id,
                current.canonical_snapshot_hash,
                current.signal_kind,
                current.recommendations,
            )
            incoming_identity = (
                batch.canonical_snapshot_id,
                batch.canonical_snapshot_hash,
                batch.signal_kind,
                batch.recommendations,
            )
            if not batch.canonical_available or current_identity != incoming_identity:
                raise V20SemanticConflict(
                    "rolling7 canonical artifact differs from its durable fact"
                )
            if current.t2_date not in (None, batch.t2_date):
                raise V20SemanticConflict("rolling7 canonical artifact changed its established T2")
            if current.t2_date == batch.t2_date:
                return current
            batch = make_batch(
                signal_date=current.signal_date,
                canonical_snapshot_id=current.canonical_snapshot_id,
                canonical_snapshot_hash=current.canonical_snapshot_hash,
                recommendations=current.recommendations,
                t2_date=batch.t2_date,
                d0_references={
                    leg.code: leg.d0_reference
                    for leg in current.legs
                    if leg.d0_reference is not None
                },
                d2_closes={
                    leg.code: leg.d2_close for leg in current.legs if leg.d2_close is not None
                },
            )
        return (
            await self._repository.save_rolling7_market_health(
                batch,
                updated_at=self._aware_now(),
            )
        ).batch

    def _rolling7_batch_from_artifact(self, record: Any) -> Rolling7Batch:
        payload = record.payload
        if not isinstance(payload, Mapping):
            raise V20SemanticConflict("canonical V16 artifact payload is invalid")
        snapshot = payload.get("v20_snapshot", payload)
        if not isinstance(snapshot, Mapping):
            raise V20SemanticConflict("canonical V16 snapshot is invalid")
        symbols = snapshot.get("symbols", ())
        if not isinstance(symbols, Sequence) or isinstance(symbols, (str, bytes)):
            raise V20SemanticConflict("canonical V16 recommendations are invalid")
        recommendations = tuple(
            CanonicalRecommendation(rank=int(item["rank"]), code=str(item["code"]))
            for item in symbols
        )
        calendar_raw = payload.get("calendar", ())
        if not isinstance(calendar_raw, Sequence) or isinstance(calendar_raw, (str, bytes)):
            raise V20SemanticConflict("canonical V16 calendar is invalid")
        try:
            calendar = tuple(date.fromisoformat(str(item)) for item in calendar_raw)
        except ValueError as exc:
            raise V20SemanticConflict("canonical V16 calendar is invalid") from exc
        if any(left >= right for left, right in zip(calendar, calendar[1:])):
            raise V20SemanticConflict("canonical V16 calendar is not strictly ordered")
        try:
            signal_index = calendar.index(record.trade_date)
        except ValueError as exc:
            raise V20SemanticConflict("canonical V16 calendar lacks its signal date") from exc
        if signal_index > len(calendar) - 3:
            raise V20SemanticConflict("canonical V16 calendar lacks D1/D2 successors")
        return make_batch(
            signal_date=record.trade_date,
            canonical_snapshot_id=str(record.snapshot_id),
            canonical_snapshot_hash=str(record.snapshot_hash),
            recommendations=recommendations,
            t2_date=calendar[signal_index + 2],
        )

    async def _rolling7_expected_t2(self, signal_date: date) -> date | None:
        calendar = await self._load_trade_calendar(self._aware_now().date())
        try:
            index = calendar.index(signal_date)
        except ValueError:
            return None
        if index > len(calendar) - 3:
            return None
        return calendar[index + 2]

    def _rolling7_d0_references(
        self,
        records: Sequence[Any],
        signal_date: date,
        recommendations: Sequence[CanonicalRecommendation],
    ) -> dict[str, float]:
        candidates: dict[str, tuple[datetime, float]] = {}
        for record in records:
            bar = _tushare_minute_from_record(record.payload)
            received_at = record.first_received_at
            if (
                bar.end_label != self.config.clock.reference_bar_label
                or bar.bar_end.astimezone(SHANGHAI).date() != signal_date
                or not bar.is_valid
                or bar.open_price <= 0
                or received_at is None
                or received_at.tzinfo is None
                or received_at.utcoffset() is None
                or received_at <= bar.bar_end
            ):
                continue
            previous = candidates.get(bar.stock_code)
            if previous is not None and previous[1] != bar.open_price:
                raise V20SemanticConflict("conflicting D0 09:41 open evidence")
            if previous is None:
                candidates[bar.stock_code] = (received_at, bar.open_price)
        return {
            code: value[1]
            for code, value in candidates.items()
            if code in set(item.code for item in recommendations)
        }

    async def _acquire_rolling7_d0_evidence(
        self,
        context: _DayContext,
        now: datetime,
    ) -> None:
        signal_date = context.trade_date
        if now.timetz().replace(tzinfo=None) < time.fromisoformat(
            self.config.clock.reference_bar_label
        ):
            return
        store = self._canonical_artifact_store
        if store is None:
            raise V20RepositoryError("canonical V16 artifact store is unavailable")
        current = await self._repository.get_rolling7_market_health_for_date(signal_date)
        if current is not None and current.status is BatchStatus.COMPLETE:
            return
        record = await store.load(
            official_stream_id=self.config.official_stream_id,
            trade_date=signal_date,
            event=V16_CANONICAL_ARTIFACT_EVENT,
        )
        if record is None:
            if current is not None:
                return
            await self._repository.save_rolling7_market_health(
                make_missing_canonical_batch(
                    signal_date=signal_date,
                    t2_date=await self._rolling7_expected_t2(signal_date),
                ),
                updated_at=now,
            )
            return
        intent = await self._record_rolling7_intent_from_artifact(record)
        if not intent.recommendations:
            return
        codes = tuple(item.code for item in intent.recommendations)
        records = await self._repository.list_raw_minute_bar_records(
            codes,
            trade_date=signal_date,
            end_labels=(self.config.clock.reference_bar_label,),
        )
        references = self._rolling7_d0_references(
            records,
            signal_date,
            intent.recommendations,
        )
        if intent.status is BatchStatus.DATA_GAP:
            references.update(
                {leg.code: leg.d0_reference for leg in intent.legs if leg.d0_reference is not None}
            )
        missing = [code for code in codes if code not in references]
        if (
            missing
            and (
                context.last_rolling7_d0_history_at is None
                or (now - context.last_rolling7_d0_history_at).total_seconds() >= 30.0
            )
            and self._scan_state.realtime_client is not None
        ):
            context.last_rolling7_d0_history_at = now
            latest = await asyncio.wait_for(
                self._scan_state.realtime_client.batch_get_latest_minute_bars(list(missing)),
                timeout=LIVE_EXIT_LIVE_HISTORY_TIMEOUT_SECONDS,
            )
            payloads = [
                _bar_payload(bar)
                for code in missing
                for bar in (latest.get(code),)
                if bar is not None
                and bar.end_label == self.config.clock.reference_bar_label
                and bar.bar_end.astimezone(SHANGHAI).date() == signal_date
                and bar.is_valid
                and bar.open_price > 0
                and now > bar.bar_end
            ]
            if payloads:
                sealed = await asyncio.wait_for(
                    self._repository.record_minute_bars(payloads),
                    timeout=LIVE_EXIT_DB_STAGE_TIMEOUT_SECONDS,
                )
                if len(sealed) != len({sha256_json(payload) for payload in payloads}):
                    raise V20RepositoryError("rolling7 D0 raw persistence was incomplete")
            records = await asyncio.wait_for(
                self._repository.list_raw_minute_bar_records(
                    codes,
                    trade_date=signal_date,
                    end_labels=(self.config.clock.reference_bar_label,),
                ),
                timeout=LIVE_EXIT_DB_STAGE_TIMEOUT_SECONDS,
            )
            references = self._rolling7_d0_references(
                records,
                signal_date,
                intent.recommendations,
            )
        await self._repository.save_rolling7_market_health(
            make_batch(
                signal_date=signal_date,
                canonical_snapshot_id=intent.canonical_snapshot_id,
                canonical_snapshot_hash=intent.canonical_snapshot_hash,
                recommendations=intent.recommendations,
                t2_date=intent.t2_date,
                d0_references=references,
            ),
            updated_at=self._aware_now(now),
        )

    async def _rolling7_d2_closes(
        self,
        t2_date: date,
        recommendations: Sequence[CanonicalRecommendation],
        now: datetime,
    ) -> dict[str, float]:
        official_close = _local(t2_date, time(15, 0))
        if now < official_close:
            return {}
        snapshots, _corrupt = await self._repository.list_daily_bar_snapshots(
            t2_date,
            received_before=None,
        )
        required = {item.code for item in recommendations}
        selected_close: dict[str, float] = {}
        for snapshot in snapshots:
            if snapshot.first_received_at < official_close:
                continue
            rows = _daily_rows_from_snapshot(snapshot.payload)
            for code in required:
                if code not in rows:
                    continue
                close = rows[code].close_price
                previous = selected_close.get(code)
                if previous is not None and previous != close:
                    raise V20SemanticConflict("conflicting D2 official close evidence")
                if previous is None:
                    selected_close[code] = close
            if required.issubset(selected_close):
                return {code: selected_close[code] for code in required}
        client = self._scan_state.realtime_client
        if client is None:
            return {}
        daily = await asyncio.wait_for(
            client.fetch_daily_bars(t2_date.strftime("%Y%m%d")),
            timeout=LIVE_EXIT_LIVE_HISTORY_TIMEOUT_SECONDS,
        )
        snapshot = await asyncio.wait_for(
            self._repository.record_daily_bar_snapshot(
                t2_date,
                _daily_snapshot_payload(t2_date, daily),
            ),
            timeout=LIVE_EXIT_DB_STAGE_TIMEOUT_SECONDS,
        )
        if snapshot.first_received_at < official_close:
            return {}
        rows = _daily_rows_from_snapshot(snapshot.payload)
        return {code: rows[code].close_price for code in required if code in rows}

    async def _finalize_rolling7_market_health(
        self,
        signal_date: date,
        t2_date: date,
        now: datetime | None = None,
        *,
        calendar: Sequence[date] | None = None,
    ) -> Rolling7Batch:
        now = self._aware_now(now)
        current = await self._repository.get_rolling7_market_health_for_date(signal_date)
        if current is not None and current.status.value == "COMPLETE":
            return current
        store = self._canonical_artifact_store
        if store is None:
            raise V20RepositoryError("canonical V16 artifact store is unavailable")
        record = await store.load(
            official_stream_id=self.config.official_stream_id,
            trade_date=signal_date,
            event=V16_CANONICAL_ARTIFACT_EVENT,
        )
        if record is None and (current is None or not current.canonical_available):
            try:
                bootstrap_calendar = (
                    tuple(calendar)
                    if calendar is not None
                    else tuple(await self._load_trade_calendar(now.date()))
                )
                record = await self._bootstrap_historical_canonical_artifact(
                    signal_date,
                    bootstrap_calendar,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "V20 Rolling7 bootstrap %s stage=CANONICAL_COMPUTE pending: %s: %s",
                    signal_date.isoformat(),
                    type(exc).__name__,
                    exc,
                )
                return (
                    await self._repository.save_rolling7_market_health(
                        make_missing_canonical_batch(
                            signal_date=signal_date,
                            t2_date=t2_date,
                        ),
                        updated_at=now,
                    )
                ).batch
        intent = (
            current if record is None else await self._record_rolling7_intent_from_artifact(record)
        )
        if intent is None:
            raise V20RepositoryError("rolling7 canonical intent is unavailable")
        if not intent.recommendations:
            return intent
        records = await self._repository.list_raw_minute_bar_records(
            tuple(item.code for item in intent.recommendations),
            trade_date=signal_date,
            end_labels=(self.config.clock.reference_bar_label,),
        )
        d0_references = self._rolling7_d0_references(
            records,
            signal_date,
            intent.recommendations,
        )
        persisted_d0 = {
            leg.code: leg.d0_reference for leg in intent.legs if leg.d0_reference is not None
        }
        persisted_d0.update(d0_references)
        d0_references = persisted_d0
        missing_d0 = [
            item.code for item in intent.recommendations if item.code not in d0_references
        ]
        if missing_d0 and self._scan_state.realtime_client is not None:
            histories = await asyncio.wait_for(
                self._scan_state.realtime_client.batch_get_minute_history_for_date(
                    missing_d0,
                    signal_date,
                ),
                timeout=ENTRY_HISTORY_RECOVERY_TIMEOUT_SECONDS,
            )
            payloads = [
                _bar_payload(bar)
                for code in missing_d0
                for bar in histories.get(code, ())
                if bar.end_label == self.config.clock.reference_bar_label
                and bar.bar_end.astimezone(SHANGHAI).date() == signal_date
                and bar.is_valid
                and bar.open_price > 0
                and now > bar.bar_end
            ]
            if payloads:
                await asyncio.wait_for(
                    self._repository.record_minute_bars(payloads),
                    timeout=LIVE_EXIT_DB_STAGE_TIMEOUT_SECONDS,
                )
            records = await self._repository.list_raw_minute_bar_records(
                tuple(item.code for item in intent.recommendations),
                trade_date=signal_date,
                end_labels=(self.config.clock.reference_bar_label,),
            )
            d0_references = self._rolling7_d0_references(
                records,
                signal_date,
                intent.recommendations,
            )
            recovered_d0 = dict(persisted_d0)
            recovered_d0.update(d0_references)
            d0_references = recovered_d0
        d2_closes = await self._rolling7_d2_closes(t2_date, intent.recommendations, now)
        persisted_d2 = {leg.code: leg.d2_close for leg in intent.legs if leg.d2_close is not None}
        persisted_d2.update(d2_closes)
        d2_closes = persisted_d2
        return (
            await self._repository.save_rolling7_market_health(
                make_batch(
                    signal_date=signal_date,
                    canonical_snapshot_id=intent.canonical_snapshot_id,
                    canonical_snapshot_hash=intent.canonical_snapshot_hash,
                    recommendations=intent.recommendations,
                    t2_date=t2_date,
                    d0_references=d0_references,
                    d2_closes=d2_closes,
                ),
                updated_at=now,
            )
        ).batch

    def _evaluate_rolling7_recovery_state(
        self,
        rows: Sequence[Rolling7Batch],
        decision_date: date,
    ):
        complete = [
            RollingBatch(
                row.canonical_snapshot_id or f"rolling7:{row.signal_date.isoformat()}",
                row.signal_date,
                row.t2_date,
                float(row.batch_return or 0.0),
            )
            for row in rows
            if row.signal_kind is SignalKind.SIGNAL
            and row.status is BatchStatus.COMPLETE
            and row.batch_return is not None
            and row.t2_date is not None
        ]
        gaps = [
            RollingGap(
                f"rolling7:{row.signal_date.isoformat()}",
                row.signal_date,
                row.t2_date,
            )
            for row in rows
            if row.status is BatchStatus.DATA_GAP and row.t2_date is not None
        ]
        return evaluate_rolling7(
            decision_date=decision_date,
            complete_batches=complete,
            gaps=gaps,
        )

    async def backfill_rolling7_market_health(
        self,
        *,
        signal_dates: Sequence[date] | None = None,
        limit: int = ROLLING7_RECOVERY_SLICE,
        overall_cap: int = ROLLING7_RECOVERY_OVERALL_CAP,
    ) -> tuple[Rolling7Batch, ...]:
        if limit < 1 or overall_cap < 1:
            raise ValueError("Rolling7 recovery bounds must be positive")
        now = self._aware_now()
        calendar = await self._load_trade_calendar(now.date())
        known_t2 = {session: calendar[index + 2] for index, session in enumerate(calendar[:-2])}
        rows = await self._repository.load_rolling7_market_health(
            before_t2=now.date(),
            limit=1_000,
        )
        facts = {row.signal_date: row for row in rows}
        recovery_state = self._evaluate_rolling7_recovery_state(rows, now.date())
        if recovery_state.status.value in {"NON_BAD", "BAD"}:
            self._rolling7_recovery_cursor = None
            return ()
        if signal_dates is not None:
            candidates = tuple(session for session in signal_dates if session in known_t2)
            cursor_index = len(candidates) - 1
        else:
            matured = [
                session
                for session, t2 in known_t2.items()
                if t2 < now.date() or (t2 == now.date() and now >= _local(t2, time(15, 0)))
            ]
            candidates = tuple(calendar)
            cursor_index = (
                calendar.index(self._rolling7_recovery_cursor)
                if self._rolling7_recovery_cursor in calendar
                else len(matured) - 1
            )
        processed: list[Rolling7Batch] = []
        scanned = 0
        while cursor_index >= 0 and scanned < overall_cap and len(processed) < limit:
            signal_date = candidates[cursor_index]
            previous_cursor = self._rolling7_recovery_cursor
            self._rolling7_recovery_cursor = signal_date
            scanned += 1
            cursor_index -= 1
            fact = facts.get(signal_date)
            if fact is not None and fact.status.value == "COMPLETE":
                continue
            if fact is not None and fact.signal_kind is SignalKind.NO_SIGNAL:
                continue
            if fact is None:
                store = self._canonical_artifact_store
                if store is None:
                    raise V20RepositoryError("canonical V16 artifact store is unavailable")
                record = await store.load(
                    official_stream_id=self.config.official_stream_id,
                    trade_date=signal_date,
                    event=V16_CANONICAL_ARTIFACT_EVENT,
                )
                if record is not None:
                    try:
                        artifact_batch = self._rolling7_batch_from_artifact(record)
                    except (KeyError, TypeError, ValueError, V20SemanticConflict):
                        artifact_batch = None
                    if artifact_batch is not None and not artifact_batch.recommendations:
                        no_signal = await self._record_rolling7_intent_from_artifact(record)
                        facts[signal_date] = no_signal
                        continue
            t2_date = known_t2.get(signal_date)
            if (
                t2_date is None
                or t2_date > now.date()
                or (t2_date == now.date() and now < _local(t2_date, time(15, 0)))
            ):
                continue
            try:
                batch = await self._finalize_rolling7_market_health(
                    signal_date,
                    t2_date,
                    now=now,
                    calendar=calendar,
                )
            except asyncio.CancelledError:
                self._rolling7_recovery_cursor = previous_cursor
                raise
            except Exception as exc:
                logger.warning(
                    "V20 Rolling7 recovery left %s pending: %s",
                    signal_date.isoformat(),
                    exc,
                )
                continue
            processed.append(batch)
            facts[signal_date] = batch
            recovery_state = self._evaluate_rolling7_recovery_state(
                tuple(facts.values()),
                now.date(),
            )
            if recovery_state.status.value in {"NON_BAD", "BAD"}:
                break
        if cursor_index < 0:
            self._rolling7_recovery_cursor = None
        elif candidates:
            self._rolling7_recovery_cursor = candidates[max(cursor_index, 0)]
        return tuple(processed)

    def _rolling7_recovery_due(self, now: datetime) -> bool:
        return (
            self._rolling7_recovery_last_at is None
            or now - self._rolling7_recovery_last_at
            >= timedelta(seconds=ROLLING7_RECOVERY_TICK_SECONDS)
        )

    async def _rolling7_automatic_recovery_allowed(self, now: datetime) -> bool:
        """Keep heavyweight historical reconstruction off live trading lanes."""

        local_now = now.astimezone(SHANGHAI)
        calendar = await self._load_trade_calendar(local_now.date())
        wall = local_now.timetz().replace(tzinfo=None)
        return not (
            local_now.date() in calendar
            and ROLLING7_AUTOMATIC_BLACKOUT_START <= wall < ROLLING7_AUTOMATIC_BLACKOUT_END
        )

    async def _run_rolling7_recovery_scheduler(self) -> None:
        while not self._stop_event.is_set():
            started_at = asyncio.get_running_loop().time()
            now = self._aware_now()
            try:
                if self._rolling7_recovery_due(
                    now
                ) and await self._rolling7_automatic_recovery_allowed(now):
                    async with self._rolling7_recovery_lock:
                        self._rolling7_recovery_last_at = now
                        await self.backfill_rolling7_market_health()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("V20 Rolling7 recovery tick failed: %s", exc)
            await self._wait_for_runtime_tick(started_at, 1.0)

    async def _hydrate_canonical_artifact_record(self, record: Any) -> Any:
        """Hydrate one durable ticket and prove its raw barrier still exists."""

        store = self._canonical_artifact_store
        if store is None:
            raise V20RepositoryError("canonical V16 artifact store is unavailable")
        custom_hydrate = getattr(store, "hydrate", None)
        if callable(custom_hydrate):
            # Test stores use this hook to enforce the same raw-before-artifact
            # invariant without pretending to be asyncpg.
            return await custom_hydrate(record)

        payload = record.payload
        hashable_payload = dict(payload) if isinstance(payload, Mapping) else payload
        actual_snapshot_hash = sha256_json(hashable_payload)
        stored_snapshot_hash = getattr(record, "snapshot_hash", None)
        if stored_snapshot_hash is None:
            stored_snapshot_hash = actual_snapshot_hash
        hydrated = hydrate_v16_canonical_artifact(payload)
        bundle = hydrated.bundle
        if getattr(record, "trade_date", bundle.trade_date) != bundle.trade_date:
            raise V20SemanticConflict("canonical V16 artifact row date differs")
        if stored_snapshot_hash != actual_snapshot_hash:
            raise V20SemanticConflict("canonical V16 artifact row hash differs")

        required_codes = tuple(bundle.snapshot["raw_evidence_codes"])
        if not required_codes:
            raise V20SemanticConflict("canonical V16 artifact has no raw-evidence universe")
        scan_input_codes = tuple(bundle.snapshot["scan_input_codes"])
        if required_codes != tuple(sorted(set(required_codes))) or not set(
            scan_input_codes
        ).issubset(required_codes):
            raise V20SemanticConflict("canonical V16 artifact raw-evidence universe is invalid")
        artifact_received_at = getattr(record, "first_received_at", None)
        if (
            not isinstance(artifact_received_at, datetime)
            or artifact_received_at.tzinfo is None
            or artifact_received_at.utcoffset() is None
        ):
            raise V20SemanticConflict("canonical V16 artifact receipt timestamp is invalid")
        try:
            raw_loader = self._repository.list_raw_minute_bar_records
            raw_kwargs: dict[str, Any] = {
                "trade_date": bundle.trade_date,
                "end_labels": EARLY_RAW_BAR_LABELS,
            }
            supports_received_before = False
            try:
                parameters = inspect.signature(raw_loader).parameters.values()
                supports_received_before = "received_before" in {
                    parameter.name for parameter in parameters
                } or any(
                    parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters
                )
            except (TypeError, ValueError):
                pass
            if supports_received_before:
                raw_kwargs["received_before"] = artifact_received_at
            all_records = await raw_loader(required_codes, **raw_kwargs)
        except V20MinuteBarIntegrityConflict as exc:
            raise V20SemanticConflict("canonical V16 raw barrier is corrupt") from exc
        records: list[MinuteBarRecord] = []
        for raw_record in all_records:
            received_at = getattr(raw_record, "first_received_at", None)
            if (
                not isinstance(received_at, datetime)
                or received_at.tzinfo is None
                or received_at.utcoffset() is None
            ):
                raise V20SemanticConflict(
                    "canonical V16 artifact has raw evidence with an invalid receipt"
                )
            if received_at < artifact_received_at:
                records.append(raw_record)
        _usable, missing, conflicted = self._fold_universe_raw_records(
            records,
            required_codes,
            bundle.trade_date,
        )
        if missing or conflicted:
            raise V20SemanticConflict(
                "canonical V16 artifact exists without its complete durable raw barrier"
            )
        return bundle

    async def _load_canonical_artifact(
        self,
        trade_date: date,
    ) -> tuple[Any, datetime] | None:
        """Load and hydrate the only canonical input legal for V20 consumers."""

        store = self._canonical_artifact_store
        if store is None:
            raise V20RepositoryError("canonical V16 artifact store is unavailable")
        record = await store.load(
            official_stream_id=self.config.official_stream_id,
            trade_date=trade_date,
            event=V16_CANONICAL_ARTIFACT_EVENT,
        )
        if record is None:
            return None
        first_received_at = getattr(record, "first_received_at", None)
        if (
            not isinstance(first_received_at, datetime)
            or first_received_at.tzinfo is None
            or first_received_at.utcoffset() is None
        ):
            raise V20SemanticConflict("canonical V16 artifact receipt timestamp is invalid")
        # Capture the immutable database receipt before invoking a compatibility
        # hydrator.  Production records are frozen; a test adapter may annotate
        # its mutable stand-in while proving the raw barrier.
        first_received_at = first_received_at.astimezone(SHANGHAI)
        hydrated = await self._hydrate_canonical_artifact_record(record)
        # This database-owned receipt is the immutable visibility cutoff for a
        # retrospective recomputation.  Hydration/readback may finish later,
        # but that process timing must never admit raw rows received after the
        # artifact itself was durably visible.
        return hydrated, first_received_at

    @staticmethod
    def _record_matches_payload(record: MinuteBarRecord) -> bool:
        """Check the durable row key is exactly bound to its payload."""
        payload = record.payload
        try:
            if str(payload["stock_code"]) != str(record.code):
                return False
            if str(payload["end_label"]) != str(record.end_label):
                return False
            bar_end = datetime.fromisoformat(str(payload["bar_end"]))
            if bar_end.tzinfo is None or bar_end.utcoffset() is None:
                return False
            if bar_end != record.bar_end:
                return False
            # The row label must be the bar_end's own Shanghai HH:MM.
            return str(record.end_label) == record.bar_end.astimezone(SHANGHAI).strftime("%H:%M")
        except (KeyError, TypeError, ValueError, OverflowError):
            return False

    @staticmethod
    def _bar_from_raw_record(record: MinuteBarRecord) -> TushareMinuteBar:
        """Rebuild one minute bar from a persisted raw record, failing closed."""
        payload = record.payload
        if not V20Service._record_matches_payload(record):
            raise V20SemanticConflict(
                "persisted canonical raw minute-bar row is not bound to its payload"
            )
        try:
            bar = TushareMinuteBar(
                stock_code=str(payload["stock_code"]),
                bar_end=record.bar_end,
                end_label=str(record.end_label),
                open_price=float(payload["open"]),
                close_price=float(payload["close"]),
                high_price=float(payload["high"]),
                low_price=float(payload["low"]),
                volume=float(payload["volume"]),
                amount=float(payload["amount"]),
            )
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise V20SemanticConflict(
                "persisted canonical raw minute-bar payload is malformed"
            ) from exc
        return bar

    @staticmethod
    def _fold_universe_raw_records(
        records: Sequence[MinuteBarRecord],
        universe: Sequence[str],
        trade_date: date,
    ) -> tuple[dict[str, tuple[TushareMinuteBar, ...]], frozenset[str], frozenset[str]]:
        """Fold persisted raw revisions into per-code early (<=09:39) evidence.

        Returns ``(usable, missing, conflicted)``.  A code's first durable
        09:39 candidate establishes the receipt anchor and must itself be legal.
        A label already present by that anchor takes its earliest legal
        revision and ignores later corrections.  A label absent at the anchor
        may be filled once from its earliest later receipt (with an exact-time
        conflict rejected), so a delayed complete-minute response can supply
        the path needed by V16 without allowing subsequent revisions to rewrite
        it.
        There is no fixed-label continuity requirement: a legal 09:39 remains
        the canonical readiness boundary. Every revision at or before the
        anchor must still be exactly bound to its row identity, durably
        received after its bar end, numerically legal, and date-bound.  A
        malformed, misbound, or illegal selected revision makes the whole code
        conflicted.  Two different payloads at the selected first receipt are
        ambiguous and conflict.  Conflicted codes are never reported as
        missing, so they are excluded from backfill fetches.
        """
        grouped: dict[str, dict[str, list[MinuteBarRecord]]] = {}
        for record in records:
            label = str(record.end_label)
            if label not in EARLY_RAW_BAR_LABELS:
                continue
            grouped.setdefault(str(record.code), {}).setdefault(label, []).append(record)
        usable: dict[str, tuple[TushareMinuteBar, ...]] = {}
        conflicted: set[str] = set()
        for code, by_label in grouped.items():
            terminal_revisions = by_label.get(EARLY_RAW_LAST_LABEL, [])
            anchor_received_at: datetime | None = None
            if terminal_revisions:
                terminal_receipts: list[datetime] = []
                for revision in terminal_revisions:
                    received_at = getattr(revision, "first_received_at", None)
                    if (
                        not isinstance(received_at, datetime)
                        or received_at.tzinfo is None
                        or received_at.utcoffset() is None
                    ):
                        conflicted.add(code)
                        break
                    terminal_receipts.append(received_at)
                if code in conflicted:
                    continue
                anchor_received_at = min(terminal_receipts)

            if anchor_received_at is None:
                for revisions in by_label.values():
                    for revision in revisions:
                        received_at = getattr(revision, "first_received_at", None)
                        try:
                            bar = V20Service._bar_from_raw_record(revision)
                        except V20SemanticConflict:
                            conflicted.add(code)
                            break
                        if (
                            not isinstance(received_at, datetime)
                            or received_at.tzinfo is None
                            or received_at.utcoffset() is None
                            or received_at <= revision.bar_end
                            or not bar.is_valid
                            or bar.stock_code != code
                            or bar.bar_end.astimezone(SHANGHAI).date() != trade_date
                        ):
                            conflicted.add(code)
                            break
                    if code in conflicted:
                        break
                continue

            selected_bars: dict[str, TushareMinuteBar] = {}
            for label, revisions in by_label.items():
                stamped: list[tuple[datetime, MinuteBarRecord]] = []
                for revision in revisions:
                    received_at = getattr(revision, "first_received_at", None)
                    if (
                        not isinstance(received_at, datetime)
                        or received_at.tzinfo is None
                        or received_at.utcoffset() is None
                    ):
                        conflicted.add(code)
                        break
                    stamped.append((received_at, revision))
                if code in conflicted:
                    break
                if anchor_received_at is None:
                    continue
                at_anchor = [item for item in stamped if item[0] <= anchor_received_at]
                selected_receipt = (
                    min(item[0] for item in at_anchor)
                    if at_anchor
                    else min(item[0] for item in stamped)
                )
                selected = [item for item in stamped if item[0] == selected_receipt]
                selected_payload: Mapping[str, Any] | None = None
                selected_source_hash: str | None = None
                candidates: list[tuple[str, TushareMinuteBar]] = []
                for received_at, revision in selected:
                    if received_at <= revision.bar_end or not V20Service._record_matches_payload(
                        revision
                    ):
                        conflicted.add(code)
                        break
                    try:
                        bar = V20Service._bar_from_raw_record(revision)
                    except V20SemanticConflict:
                        conflicted.add(code)
                        break
                    if (
                        not bar.is_valid
                        or bar.stock_code != code
                        or bar.bar_end.astimezone(SHANGHAI).date() != trade_date
                    ):
                        conflicted.add(code)
                        break
                    if selected_payload is None:
                        selected_payload = revision.payload
                        selected_source_hash = str(revision.source_hash)
                    elif (
                        revision.payload != selected_payload
                        or str(revision.source_hash) != selected_source_hash
                    ):
                        conflicted.add(code)
                        break
                    candidates.append((str(revision.source_hash), bar))
                if code in conflicted:
                    break
                selected_bars[label] = min(candidates, key=lambda candidate: candidate[0])[1]
            if code in conflicted or anchor_received_at is None:
                continue
            usable[code] = tuple(selected_bars[label] for label in sorted(selected_bars))
        missing = frozenset(
            code for code in universe if code not in usable and code not in conflicted
        )
        return usable, missing, frozenset(conflicted)

    @staticmethod
    def _raise_historical_seed_conflict(conflicted: frozenset[str], *, phase: str) -> None:
        """Fail closed on any conflicted universe code, with a bounded sample."""
        sample = ", ".join(sorted(conflicted)[:HISTORICAL_SEED_CONFLICT_SAMPLE])
        raise V20SemanticConflict(
            f"canonical V16 historical seed {phase} fold has "
            f"{len(conflicted)} conflicted universe codes (sample: {sample})"
        )

    async def _acquire_current_day_early_market_data_once(
        self,
        trade_date: date,
        targets: Sequence[str],
        loader: Callable[
            [Sequence[str], date],
            Awaitable[Mapping[str, TushareEarlyMarketData]],
        ],
    ) -> Mapping[str, TushareEarlyMarketData]:
        """Join the sole V20 full-market acquisition for this provider minute.

        A task remains cached after both success and failure.  Retrying a failed
        full fan-out in the same minute would duplicate roughly 3,000 physical
        ``rt_min_daily`` requests even though the client has already exhausted
        its bounded per-symbol transport retries.  A later provider minute gets
        a new key and may make one new, independently fenced attempt.
        """

        current = self._aware_now().astimezone(SHANGHAI)
        decision_at = _local(
            trade_date,
            time.fromisoformat(self.config.clock.decision_bar_label),
        )
        if current < decision_at:
            raise V20StateConflict(
                "current-day canonical early acquisition is unavailable before 09:39"
            )
        provider_minute = current.replace(second=0, microsecond=0)
        key = (trade_date, provider_minute)
        requested = frozenset(targets)
        async with self._current_day_early_attempt_lock:
            if self._stop_event.is_set():
                raise V20StateConflict(
                    "current-day V20 acquisition is unavailable while the service is stopping"
                )
            active_prior_attempt: (
                tuple[
                    date,
                    frozenset[str],
                    asyncio.Task[Mapping[str, TushareEarlyMarketData]],
                ]
                | None
            ) = None
            for old_key, (_old_targets, old_task) in tuple(
                self._current_day_early_attempts.items()
            ):
                if old_key != key and old_task.done():
                    self._current_day_early_attempts.pop(old_key, None)
                elif old_key != key and active_prior_attempt is None:
                    active_prior_attempt = (old_key[0], _old_targets, old_task)
            shared = self._current_day_early_attempts.get(key)
            if shared is None and active_prior_attempt is not None:
                # Never overlap two V20 full-market rounds.  A request arriving
                # in a new minute first joins the still-running prior-minute
                # attempt; it may start the new minute's one attempt only after
                # the earlier round has reached a terminal state.
                active_trade_date, active_targets, active_task = active_prior_attempt
                if active_trade_date != trade_date:
                    raise V20StateConflict(
                        "a prior-date V20 current-day acquisition is still running"
                    )
                shared = (active_targets, active_task)
                # Occupy the new provider-minute key as well.  Otherwise a
                # later caller in this same minute could delete the completed
                # old key and accidentally launch a second full-market round.
                self._current_day_early_attempts[key] = shared
            if shared is None:
                launched_targets = requested

                async def _load() -> Mapping[str, TushareEarlyMarketData]:
                    return await loader(sorted(launched_targets), trade_date)

                attempt = asyncio.create_task(
                    _load(),
                    name=(
                        "v20-current-day-early-"
                        f"{trade_date.isoformat()}-{provider_minute.strftime('%H%M')}"
                    ),
                )
                self._current_day_early_attempts[key] = (launched_targets, attempt)
            else:
                launched_targets, attempt = shared
                uncovered = requested - launched_targets
                if uncovered:
                    sample = ",".join(sorted(uncovered)[:5])
                    raise V20StateConflict(
                        "current-day V20 acquisition already started with a smaller "
                        f"target set in this provider minute; uncovered={len(uncovered)} "
                        f"(sample: {sample})"
                    )
        response = await asyncio.shield(attempt)
        return {code: response[code] for code in sorted(requested) if code in response}

    async def _historical_early_evidence_seed(
        self,
        trade_date: date,
        *,
        universe_override: tuple[str, ...] | None = None,
        clean_boards_override: Mapping[str, Sequence[tuple[str, str]]] | None = None,
        evidence_codes: Sequence[str] = (),
        exact_evidence_codes: Sequence[str] | None = None,
        received_before: datetime | None = None,
        allow_backfill: bool = True,
    ) -> tuple[
        dict[str, TushareEarlyMarketData],
        tuple[str, ...],
        Mapping[str, tuple[tuple[str, str], ...]],
    ]:
        """Rebuild the canonical early-data seed for a non-future trade date.

        The exact canonical universe is resolved through the shared V16 universe
        semantics.  Every persisted raw revision for that universe is read with
        the full set of possible early end labels (00:00..09:39).  Each code is
        frozen at its first unambiguous durable 09:39 receipt; later revisions
        cannot rewrite that point-in-time input, while malformed or misbound
        evidence visible at the anchor makes the code conflicted/unusable.
        Backfill targets are exactly the
        missing nonconflicted codes.  An already-persisted legal target-date
        09:39 bar is sufficient under the same canonical V16 readiness rule
        and is never fetched again; the canonical 80% readiness gate only
        decides scan readiness and never stops fetching.
        A present response key is a successful per-code answer — an empty
        tuple explicitly confirms no bars — while a missing key is a failure.
        Current-day gaps use one bounded-concurrency ``rt_min_daily``
        acquisition for the complete missing set; past-day gaps use chunked
        ``stk_mins`` requests.  Successful bars are persisted and the database
        is always read back so the seed is hydrated exclusively from persisted
        evidence.  Any conflicted universe code raises
        ``V20SemanticConflict``.  Missing current-day keys remain visible to
        the canonical 80% readiness gate; unresolved historical targets fail.

        With ``exact_evidence_codes`` and ``received_before`` from an existing
        artifact, only those codes and rows received before the artifact are
        visible.  Every declared evidence code must be reconstructable, and a
        caller can disable backfill so a retrospective run cannot change which
        symbols were present in the formal run.
        """
        _scanner, _scorer, clean_boards, universe = derive_canonical_v16_universe(
            self._scan_state,
            universe_override=universe_override,
            clean_boards_override=clean_boards_override,
        )
        if received_before is not None and (
            received_before.tzinfo is None or received_before.utcoffset() is None
        ):
            raise V20SemanticConflict("canonical V16 raw receipt cutoff is invalid")
        if exact_evidence_codes is None:
            evidence_universe = tuple(sorted(set(universe).union(evidence_codes)))
        else:
            evidence_universe = tuple(exact_evidence_codes)
            if (
                not evidence_universe
                or tuple(sorted(set(evidence_universe))) != evidence_universe
                or any(
                    not isinstance(code, str) or len(code) != 6 or not code.isdigit()
                    for code in evidence_universe
                )
            ):
                raise V20SemanticConflict("canonical V16 frozen raw-evidence universe is invalid")
        raw_loader = self._repository.list_raw_minute_bar_records
        raw_kwargs: dict[str, Any] = {
            "trade_date": trade_date,
            "end_labels": EARLY_RAW_BAR_LABELS,
        }
        if received_before is not None:
            supports_received_before = False
            try:
                parameters = inspect.signature(raw_loader).parameters.values()
                supports_received_before = "received_before" in {
                    parameter.name for parameter in parameters
                } or any(
                    parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters
                )
            except (TypeError, ValueError):
                pass
            if supports_received_before:
                raw_kwargs["received_before"] = received_before
        records = await raw_loader(evidence_universe, **raw_kwargs)
        if received_before is not None:
            visible_records: list[MinuteBarRecord] = []
            for record in records:
                received_at = getattr(record, "first_received_at", None)
                if (
                    not isinstance(received_at, datetime)
                    or received_at.tzinfo is None
                    or received_at.utcoffset() is None
                ):
                    raise V20SemanticConflict(
                        "canonical V16 frozen raw evidence has an invalid receipt"
                    )
                if received_at < received_before:
                    visible_records.append(record)
            records = visible_records
        usable, missing, conflicted = self._fold_universe_raw_records(
            records,
            evidence_universe,
            trade_date,
        )
        if conflicted:
            self._raise_historical_seed_conflict(conflicted, phase="initial")
        if exact_evidence_codes is not None and missing:
            sample = ", ".join(sorted(missing)[:HISTORICAL_SEED_CONFLICT_SAMPLE])
            raise V20SemanticConflict(
                "canonical V16 frozen raw barrier is incomplete at artifact receipt: "
                f"{len(missing)} code(s) missing (sample: {sample})"
            )
        # A legal target-date 09:39 bar is the one canonical V16 per-symbol
        # readiness boundary.  Earlier labels are preserved when present but
        # never become a second, historical-only admission rule.
        today = self._aware_now().astimezone(SHANGHAI).date()
        targets = sorted(missing)
        if targets and not allow_backfill:
            sample = ", ".join(targets[:HISTORICAL_SEED_CONFLICT_SAMPLE])
            raise V20SemanticConflict(
                "canonical V16 frozen raw boundary forbids backfill: "
                f"{len(targets)} code(s) missing (sample: {sample})"
            )
        if targets:
            client = self._scan_state.realtime_client
            if trade_date > today:
                raise V20StateConflict("canonical V16 replay cannot target a future trade date")
            confirmed_empty: set[str] = set()
            if trade_date == today:
                current_loader = getattr(client, "batch_get_early_market_data", None)
                if client is None or not callable(current_loader):
                    raise V20RepositoryError(
                        "canonical V16 current-day early-minute adapter is unavailable"
                    )
                early_response = await self._acquire_current_day_early_market_data_once(
                    trade_date,
                    targets,
                    current_loader,
                )
                payloads = [
                    _bar_payload(bar)
                    for code in targets
                    if (early := early_response.get(code)) is not None
                    for bar in early.early_bars
                ]
                if payloads:
                    sealed_hashes = await self._repository.record_minute_bars(payloads)
                    expected_hashes = frozenset(sha256_json(payload) for payload in payloads)
                    if frozenset(sealed_hashes) != expected_hashes:
                        raise V20RepositoryError(
                            "canonical V16 current-day realtime persistence is incomplete: "
                            f"{len(set(sealed_hashes) & expected_hashes)}/{len(payloads)} sealed"
                        )
            else:
                historical_loader = getattr(client, "batch_get_early_minute_history_for_date", None)
                if client is None or not callable(historical_loader):
                    raise V20RepositoryError(
                        "canonical V16 historical replay batched early-minute adapter "
                        "is unavailable"
                    )
                for start in range(0, len(targets), HISTORICAL_SEED_BACKFILL_CHUNK):
                    chunk = targets[start : start + HISTORICAL_SEED_BACKFILL_CHUNK]
                    history = await historical_loader(chunk, trade_date)
                    payloads = []
                    for code in chunk:
                        if code not in history:
                            continue
                        rows = history[code]
                        if not rows:
                            confirmed_empty.add(code)
                            continue
                        truncated = tuple(
                            bar
                            for bar in rows
                            if bar.bar_end.astimezone(SHANGHAI).date() == trade_date
                            and bar.end_label in EARLY_RAW_BAR_LABELS
                        )
                        early = tushare_minute_bars_to_early_market_data(
                            code, truncated, trade_date
                        )
                        if early is not None:
                            payloads.extend(_bar_payload(bar) for bar in early.early_bars)
                    if payloads:
                        sealed_hashes = await self._repository.record_minute_bars(payloads)
                        expected_hashes = frozenset(sha256_json(payload) for payload in payloads)
                        if frozenset(sealed_hashes) != expected_hashes:
                            raise V20RepositoryError(
                                "canonical V16 replay minute-history persistence is incomplete: "
                                f"{len(set(sealed_hashes) & expected_hashes)}/{len(payloads)} "
                                "sealed"
                            )
                    if not any(code in history for code in chunk):
                        break
            # The seed must be hydrated from persisted evidence, never from the
            # vendor response that was just written; current-day misses remain
            # visible to the canonical readiness gate.
            records = await self._repository.list_raw_minute_bar_records(
                evidence_universe,
                trade_date=trade_date,
                end_labels=EARLY_RAW_BAR_LABELS,
            )
            usable, missing, readback_conflicted = self._fold_universe_raw_records(
                records,
                evidence_universe,
                trade_date,
            )
            if readback_conflicted:
                self._raise_historical_seed_conflict(readback_conflicted, phase="readback")
            unrecovered = [
                code for code in targets if code not in usable and code not in confirmed_empty
            ]
            if trade_date < today and unrecovered:
                raise V20RepositoryError(
                    "canonical V16 historical backfill is incomplete: "
                    f"{len(unrecovered)}/{len(targets)} targets lack qualified "
                    "persisted evidence or an explicitly empty vendor response"
                )
        seed: dict[str, TushareEarlyMarketData] = {}
        for code, bars in usable.items():
            early = tushare_minute_bars_to_early_market_data(code, bars, trade_date)
            if early is not None:
                seed[code] = early
        return seed, universe, clean_boards

    async def _historical_canonical_inputs(
        self,
        context: _DayContext,
    ) -> _HistoricalCanonicalInputs:
        """Acquire date-bound V16 inputs under an optional artifact boundary.

        Minute and D1 daily facts are read from the durable receipt boundary.
        The portable artifact stores identities rather than full OHLCV-history
        or name payloads, so those production dependencies are reacquired.  An
        outage fails the calculation; per-code OHLCV history or full consumed
        name-map drift is rejected by artifact identity before success is
        reported.
        """

        trade_date = context.trade_date
        frozen_calendar = getattr(context, "canonical_fact_calendar", None)
        context_calendar = tuple(frozen_calendar or context.calendar)
        if context_calendar and (
            any(type(item) is not date for item in context_calendar)
            or tuple(sorted(set(context_calendar))) != context_calendar
        ):
            raise V20SemanticConflict("Rolling7 canonical bootstrap context calendar is invalid")
        context_predecessors = [day for day in context_calendar if day < trade_date]
        context_successors = [day for day in context_calendar if day > trade_date]
        if trade_date in context_calendar and context_predecessors and len(context_successors) >= 2:
            calendar = context_calendar
        else:
            calendar = tuple(await self._load_trade_calendar(trade_date))
        if trade_date not in calendar:
            raise V20RepositoryError(
                f"Rolling7 canonical bootstrap calendar lacks {trade_date.isoformat()}"
            )
        predecessors = [day for day in calendar if day < trade_date]
        successors = [day for day in calendar if day > trade_date]
        if not predecessors or len(successors) < 2:
            raise V20RepositoryError(
                "Rolling7 canonical bootstrap calendar lacks predecessor or D1/D2"
            )
        prior_trade_date = predecessors[-1]

        _scanner, _scorer, clean_boards, derived_universe = derive_canonical_v16_universe(
            self._scan_state
        )
        universe = derived_universe
        fact_universe = getattr(context, "canonical_fact_universe", None)
        if fact_universe is not None:
            universe_override = fact_universe
            if (
                not universe_override
                or tuple(sorted(set(universe_override))) != universe_override
                or any(
                    not isinstance(code, str) or len(code) != 6 or not code.isdigit()
                    for code in universe_override
                )
            ):
                raise V20SemanticConflict("canonical V16 frozen comparison universe is invalid")
            universe = universe_override
        client = self._scan_state.realtime_client
        fact_received_before = getattr(context, "canonical_fact_received_before", None)
        if fact_received_before is None and (
            client is None or not callable(getattr(client, "fetch_daily_bars", None))
        ):
            raise V20RepositoryError(
                "Rolling7 canonical bootstrap daily-history adapter is unavailable"
            )
        historical_adapter = self._scan_state.historical_adapter
        if historical_adapter is None:
            raise V20RepositoryError(
                "Rolling7 canonical bootstrap OHLCV-history adapter is unavailable"
            )

        logger.info(
            "V20 Rolling7 bootstrap %s stage=D1_DAILY source=tushare.daily date=%s start",
            trade_date.isoformat(),
            prior_trade_date.isoformat(),
        )
        if fact_received_before is None:
            daily = await client.fetch_daily_bars(prior_trade_date.strftime("%Y%m%d"))
            if not daily:
                raise V20RepositoryError(
                    f"Rolling7 canonical bootstrap D1 daily is empty for {prior_trade_date}"
                )
            daily_record = await self._repository.record_daily_bar_snapshot(
                prior_trade_date,
                _daily_snapshot_payload(prior_trade_date, daily),
            )
        else:
            daily_loader = getattr(self._repository, "list_daily_bar_snapshots", None)
            if not callable(daily_loader):
                raise V20SemanticConflict(
                    "canonical V16 artifact cannot reload its frozen D1 daily fact"
                )
            daily_candidates, _corrupt_ids = await daily_loader(
                prior_trade_date,
                received_before=fact_received_before,
            )
            visible_daily_candidates = []
            for candidate in daily_candidates:
                received_at = getattr(candidate, "first_received_at", None)
                if (
                    not isinstance(received_at, datetime)
                    or received_at.tzinfo is None
                    or received_at.utcoffset() is None
                ):
                    raise V20SemanticConflict(
                        "canonical V16 frozen D1 daily fact has an invalid receipt"
                    )
                if received_at < fact_received_before:
                    visible_daily_candidates.append(candidate)
            if not visible_daily_candidates:
                raise V20SemanticConflict(
                    "canonical V16 artifact lacks a D1 daily fact at its receipt boundary"
                )
            # Repository order is newest durable candidate first within the
            # artifact receipt fence.  The selected candidate's full close and
            # amount map is bound by the external-market identity comparison.
            daily_record = visible_daily_candidates[0]
        prior_daily = _daily_rows_from_snapshot(daily_record.payload)
        expected_prior_text = prior_trade_date.strftime("%Y%m%d")
        if not prior_daily or any(
            row.stock_code != code or row.trade_date != expected_prior_text
            for code, row in prior_daily.items()
        ):
            raise V20SemanticConflict("Rolling7 canonical bootstrap D1 daily readback is invalid")
        prev_closes = {code: float(row.close_price) for code, row in prior_daily.items()}
        logger.info(
            "V20 Rolling7 bootstrap %s stage=D1_DAILY complete rows=%d",
            trade_date.isoformat(),
            len(prior_daily),
        )

        breadth_codes = tuple(
            sorted(
                code
                for code, previous_close in prev_closes.items()
                if len(code) == 6 and code.startswith(("00", "60")) and previous_close > 0
            )
        )
        today = self._aware_now().astimezone(SHANGHAI).date()
        minute_source = "rt_min_daily" if trade_date == today else "stk_mins"
        logger.info(
            "V20 Rolling7 bootstrap %s stage=EARLY_MINUTE_BACKFILL source=tushare.%s "
            "universe=%d breadth=%d start",
            trade_date.isoformat(),
            minute_source,
            len(universe),
            len(breadth_codes),
        )
        seed, frozen_universe, frozen_boards = await self._historical_early_evidence_seed(
            trade_date,
            universe_override=universe,
            clean_boards_override=clean_boards,
            evidence_codes=breadth_codes,
            exact_evidence_codes=getattr(context, "canonical_fact_evidence_codes", None),
            received_before=fact_received_before,
            allow_backfill=getattr(context, "canonical_fact_allow_backfill", True),
        )
        logger.info(
            "V20 Rolling7 bootstrap %s stage=EARLY_MINUTE_BACKFILL source=tushare.%s "
            "complete ready=%d",
            trade_date.isoformat(),
            minute_source,
            len(seed),
        )
        if trade_date == today:
            fundamentals = self._scan_state.fundamentals_db
            if fundamentals is None or not callable(
                getattr(fundamentals, "batch_current_names", None)
            ):
                raise V20RepositoryError(
                    "Rolling7 same-day canonical bootstrap stock_basic adapter is unavailable"
                )
            logger.info(
                "V20 Rolling7 bootstrap %s stage=ST_NAMES source=tushare.stock_basic start",
                trade_date.isoformat(),
            )
            names_raw = await fundamentals.batch_current_names(list(frozen_universe))
            frozen_codes = set(frozen_universe)
            if not isinstance(names_raw, Mapping) or any(
                not isinstance(code, str)
                or code not in frozen_codes
                or not isinstance(name, str)
                or not name.strip()
                for code, name in names_raw.items()
            ):
                raise V20SemanticConflict("Rolling7 same-day stock_basic name snapshot is invalid")
            names = {code: name.strip() for code, name in names_raw.items()}
            st_eligible_codes = tuple(
                sorted(code for code, name in names.items() if not name.startswith(("ST", "*ST")))
            )
        else:
            if not callable(getattr(client, "fetch_stock_names_for_date", None)):
                raise V20RepositoryError(
                    "Rolling7 canonical bootstrap historical-name adapter is unavailable"
                )
            logger.info(
                "V20 Rolling7 bootstrap %s stage=ST_NAMES source=tushare.bak_basic start",
                trade_date.isoformat(),
            )
            historical_names = await client.fetch_stock_names_for_date(
                trade_date.strftime("%Y%m%d")
            )
            if not historical_names:
                raise V20RepositoryError(
                    f"Rolling7 canonical bootstrap bak_basic is empty for {trade_date}"
                )
            names = {
                code: historical_names[code] for code in frozen_universe if code in historical_names
            }
            st_eligible_codes = tuple(
                sorted(code for code, name in names.items() if not name.startswith(("ST", "*ST")))
            )
        if not names:
            raise V20RepositoryError(
                "Rolling7 canonical bootstrap name snapshot has no universe overlap"
            )
        if len(st_eligible_codes) != len(set(st_eligible_codes)) or any(
            code not in frozen_universe for code in st_eligible_codes
        ):
            raise V20SemanticConflict("Rolling7 canonical bootstrap ST eligibility is invalid")
        logger.info(
            "V20 Rolling7 bootstrap %s stage=ST_NAMES complete names=%d non_st=%d",
            trade_date.isoformat(),
            len(names),
            len(st_eligible_codes),
        )

        trading_codes = [
            code for code in frozen_universe if code in seed and seed[code].quote.is_trading
        ]
        logger.info(
            "V20 Rolling7 bootstrap %s stage=DAILY_HISTORY source=iquant.history_quotes "
            "codes=%d start",
            trade_date.isoformat(),
            len(trading_codes),
        )
        history_raw = await _fetch_history_ohlcv(
            historical_adapter,
            trading_codes,
            trade_date,
        )
        if trading_codes and not history_raw:
            raise V20RepositoryError(
                f"Rolling7 canonical bootstrap history is empty for {trade_date}"
            )
        logger.info(
            "V20 Rolling7 bootstrap %s stage=DAILY_HISTORY complete rows=%d",
            trade_date.isoformat(),
            len(history_raw),
        )

        return _HistoricalCanonicalInputs(
            early_data_seed=seed,
            universe=frozen_universe,
            clean_boards=frozen_boards,
            prev_closes=prev_closes,
            history_raw=history_raw,
            names=names,
            calendar=calendar,
            prior_daily=prior_daily,
            st_eligible_codes=st_eligible_codes,
        )

    async def _compute_canonical_v16_from_persisted_raw(
        self,
        context: _DayContext,
    ) -> CanonicalV16ScanBundle:
        """Recompute canonical V16 for every automatic and manual calculation path.

        There is no second algorithm rebuilt from persisted rows: the same
        ``compute_canonical_v16_scan`` serves the live slot and post-cutoff
        checks.  Same-day or past, the seed is rebuilt
        from persisted early (<=09:39) raw evidence with a date-bound vendor
        backfill for missing nonconflicted codes (today through rt_min_daily,
        T-1 and earlier through stk_mins), and
        ``compute_canonical_v16_scan`` is called directly with realtime early
        fetches forbidden, so the canonical 80% readiness gate stays
        authoritative.  The canonical coordinator and its cache are never
        inspected or mutated here, and the date-bound vendor path never crosses
        the current-day/historical boundary.  A future trade date is
        rejected outright.
        """

        trade_date = context.trade_date
        today = self._aware_now().date()
        if trade_date > today:
            raise V20StateConflict("canonical V16 replay cannot target a future trade date")
        frozen = await self._historical_canonical_inputs(context)
        canonical = await compute_canonical_v16_scan(
            self._scan_state,
            trade_date,
            early_data_seed=frozen.early_data_seed,
            universe_override=frozen.universe,
            clean_boards_override=frozen.clean_boards,
            prev_closes_override=frozen.prev_closes,
            history_raw_override=frozen.history_raw,
            names_override=frozen.names,
            calendar_override=frozen.calendar,
            prior_daily_override=frozen.prior_daily,
            st_eligible_codes_override=frozen.st_eligible_codes,
            allow_realtime_fetch=False,
        )
        if getattr(context, "canonical_fact_persist_raw", True):
            await self._persist_canonical_raw_minute_bars(canonical)
        return canonical

    async def _bootstrap_historical_canonical_artifact(
        self,
        signal_date: date,
        calendar: Sequence[date],
    ) -> Any:
        """Rebuild and durably publish one absent canonical V16 artifact."""

        store = self._canonical_artifact_store
        if store is None:
            raise V20RepositoryError("canonical V16 artifact store is unavailable")
        async with self._rolling7_canonical_bootstrap_lock:
            existing = await store.load(
                official_stream_id=self.config.official_stream_id,
                trade_date=signal_date,
                event=V16_CANONICAL_ARTIFACT_EVENT,
            )
            if existing is not None:
                return existing
            logger.info(
                "V20 Rolling7 bootstrap %s stage=CANONICAL_COMPUTE start",
                signal_date.isoformat(),
            )
            canonical = await self._compute_canonical_v16_from_persisted_raw(
                _DayContext(trade_date=signal_date, calendar=tuple(calendar))
            )
            await self._persist_canonical_artifact_barrier(canonical)
            record = await store.load(
                official_stream_id=self.config.official_stream_id,
                trade_date=signal_date,
                event=V16_CANONICAL_ARTIFACT_EVENT,
            )
            if record is None:
                raise V20RepositoryError(
                    "Rolling7 canonical bootstrap completed without durable artifact"
                )
            logger.info(
                "V20 Rolling7 bootstrap %s stage=CANONICAL_COMPUTE complete",
                signal_date.isoformat(),
            )
            return record

    async def _build_late_0939_replay_semantic(
        self,
        context: _DayContext,
        now: datetime,
        *,
        replay_event_id: str,
    ) -> Mapping[str, Any]:
        """Build the legacy post-cutoff envelope around the shared calculation.

        This method no longer owns a second V16/DayGate/policy implementation.
        It only validates the retrospective boundary, invokes the exact same
        morning computation used by the automatic entry lane, and packages the
        already-computed proposal as a non-actionable operator notification.
        """

        status = context.entry_status
        if status is None:
            status = await self._repository.get_entry_status(
                self.config.official_stream_id,
                context.trade_date,
            )
        if status is None or status.action not in {"ENTER", "BLOCK", "NO_SIGNAL", "INPUT_INVALID"}:
            raise V20StateConflict("late 09:39 replay requires a terminal V20 slot")
        self._verify_entry_binding(status)
        if now < _local(context.trade_date, self.config.clock.publish_deadline):
            raise V20StateConflict("late 09:39 replay cannot run before the live cutoff")
        state_at_start = await self._repository.load_state(self.config.state_lineage_id)
        state_payload_hash = sha256_json(dict(state_at_start.payload))
        if (
            state_at_start.lineage_id != self.config.state_lineage_id
            or state_payload_hash != state_at_start.state_hash
        ):
            raise V20SemanticConflict("official state head is malformed before terminal replay")
        calculation = await self._orchestrate_morning_selection(context.trade_date)
        state_at_end = await self._repository.load_state(self.config.state_lineage_id)
        if (
            state_at_end.lineage_id != state_at_start.lineage_id
            or state_at_end.revision != state_at_start.revision
            or state_at_end.state_hash != state_at_start.state_hash
            or sha256_json(dict(state_at_end.payload)) != state_payload_hash
        ):
            raise V20StateConflict("official state changed during terminal replay")
        prepared = calculation.prepared
        bundle = calculation.bundle
        pure = dict(prepared.commit.semantic)
        if pure.get("action") not in {"ENTER", "BLOCK", "NO_SIGNAL"}:
            raise V20SemanticConflict("late replay unexpectedly produced an illegal action")
        context.canonical_bundle = bundle
        context.canonical_first_received_at = calculation.canonical_first_received_at

        raw_codes = tuple(bundle.snapshot.get("scan_input_codes") or ())
        records = list(
            await self._repository.list_raw_minute_bar_records(
                raw_codes,
                trade_date=context.trade_date,
                end_labels=EARLY_RAW_BAR_LABELS,
            )
        )
        computed_at = self._aware_now(now)
        receipt_times = [
            item.first_received_at.astimezone(SHANGHAI)
            for item in records
            if isinstance(getattr(item, "first_received_at", None), datetime)
            and item.first_received_at.tzinfo is not None
            and item.first_received_at.utcoffset() is not None
        ]
        if len(receipt_times) != len(records):
            raise V20SemanticConflict("late replay raw facts lack durable receipt clocks")
        live_cutoff = _local(context.trade_date, self.config.clock.publish_deadline)
        raw_fact_hash = sha256_json(
            [
                {
                    "code": item.code,
                    "label": item.end_label,
                    "payload_hash": sha256_json(dict(item.payload)),
                }
                for item in records
            ]
        )
        breadth_required = pure["health_state"] not in {"WARMUP", "HEALTHY"}
        pit_limitations = [
            "RAW_MINUTE_ROWS_RECEIVED_OR_REUSED_AFTER_LIVE_CUTOFF",
            "CURRENT_FUNDAMENTAL_NAME_AND_ST_METADATA_MAY_REFLECT_LATER_SAME_DAY_STATE",
            "MEWS_IS_NOT_A_09:39_ENTRY_INPUT",
            "CANONICAL_SELECTION_MAY_HAVE_BEEN_MATERIALIZED_BY_AN_EARLIER_PROCESS",
        ]
        if status.action == "INPUT_INVALID":
            pit_limitations.append(
                "OFFICIAL_INPUT_INVALID_SLOT_HAS_NO_FROZEN_MORNING_CANONICAL_IDENTITY"
            )
        semantic: dict[str, Any] = {
            "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
            "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
            "event_id": replay_event_id,
            "strategy_version": self.config.strategy_version,
            "config_hash": self.config.config_hash,
            "state_semantics_hash": self.config.state_semantics_hash,
            "deployment_mode": self.config.deployment_mode,
            "official_stream_id": self.config.official_stream_id,
            "state_lineage_id": self.config.state_lineage_id,
            "alert_code": "LATE_0939_REPLAY_RESULT",
            "delivery_priority_class": "OPERATOR_NOTIFICATION",
            "event_trade_date": context.trade_date.isoformat(),
            "replay_kind": "RETROSPECTIVE_POST_CUTOFF",
            "non_actionable": True,
            "official_entry_action": status.action,
            "official_entry_event_id": status.event_id,
            "replay_action": pure["action"],
            "final_multiplier": pure["final_multiplier"],
            "base_multiplier": pure["base_multiplier"],
            "defense_multiplier": pure["defense_multiplier"],
            "health_state": pure["health_state"],
            "rolling7_state": pure["rolling7_state"],
            "rolling7_r7": pure["rolling7_r7"],
            "rolling7_l7": pure["rolling7_l7"],
            "g_state": pure["g_state"],
            "g_max_component_size": pure["g_max_component_size"],
            "g_amount_below_q25_count": pure["g_amount_below_q25_count"],
            "reason_codes": list(pure["reason_codes"]),
            "symbols": list(pure["symbols"]),
            "last_complete_bar": pure["last_complete_bar"],
            # Preserve the shared PreparedEntry semantic exactly.  The outer
            # alert owns the manual event identity and actionability boundary.
            "entry_render_semantic": dict(pure),
            "v16_snapshot_hash": bundle.snapshot_hash,
            "early_market_source_hash": bundle.snapshot["early_market_source_hash"],
            "policy_input_hash": prepared.commit.snapshot["policy_input_hash"],
            "canonical_raw_hash": raw_fact_hash,
            "persisted_raw_hash": raw_fact_hash,
            "requested_at": now.isoformat(),
            "retrieved_at": computed_at.isoformat(),
            "computed_at": computed_at.isoformat(),
            "data_cutoff": "09:39",
            "data_receipt_timeliness": "POST_CUTOFF",
            "canonical_source": calculation.canonical_source,
            "canonical_selection_recomputed": True,
            "canonical_artifact_compared": calculation.canonical_artifact_compared,
            "canonical_artifact_matches": calculation.canonical_artifact_matches,
            "policy_recomputed": True,
            "raw_fact_n": len(records),
            "raw_first_received_at": (min(receipt_times).isoformat() if receipt_times else None),
            "raw_last_received_at": (max(receipt_times).isoformat() if receipt_times else None),
            "raw_pre_cutoff_n": sum(item < live_cutoff for item in receipt_times),
            "raw_post_cutoff_n": sum(item >= live_cutoff for item in receipt_times),
            "breadth_replay_mode": (
                "EXACT_09:39_FULL_MAIN_BOARD"
                if breadth_required
                else "SKIPPED_NOT_USED_BY_BASE_WARMUP_OR_HEALTHY"
            ),
            "breadth_valid_n": bundle.breadth_valid_n if breadth_required else None,
            "breadth_down_n": bundle.breadth_down_n if breadth_required else None,
            "state_replay_profile": "CURRENT_CODE_CANONICAL_V16_CHECK_ONLY",
            "bootstrap_mode": self.config.bootstrap_mode,
            "quote_coverage": bundle.snapshot.get("scan_input_coverage"),
            "pit_limitations": pit_limitations,
        }
        semantic["message"] = _late_0939_replay_body(
            official_status=status,
            replay_semantic=semantic,
        )
        return semantic

    async def _ensure_late_0939_replay(
        self,
        context: _DayContext,
        now: datetime,
    ) -> OutboxRecord:
        status = context.entry_status
        if status is None:
            status = await self._repository.get_entry_status(
                self.config.official_stream_id,
                context.trade_date,
            )
        if status is None or status.action not in _LATE_0939_REPLAY_ACTIONS:
            raise V20StateConflict("late 09:39 replay requires today's terminal official slot")
        self._verify_entry_binding(status)
        replay_event_id = self._late_0939_replay_event_id(
            context.trade_date,
            official_entry_event_id=status.event_id,
        )
        existing = await self._repository.get_outbox_event(
            replay_event_id,
            route_id=self.config.route_id,
            **self._ledger_scope,
        )
        if existing is not None:
            self._verify_late_0939_replay_record(
                existing,
                trade_date=context.trade_date,
                official_entry_event_id=status.event_id,
                official_entry_action=status.action,
            )
            if existing.payload is None:
                await self._repository.assert_runtime_leader()
                existing = await self._repository.seal_event(
                    replay_event_id,
                    seal_v20_payload,
                )
            context.late_0939_replay_completed = True
            return existing

        try:
            await asyncio.wait_for(
                self._late_0939_replay_lock.acquire(),
                timeout=LATE_0939_REPLAY_TOTAL_TIMEOUT_SECONDS,
            )
        except TimeoutError as exc:
            raise V20StateConflict("late 09:39 replay lane is busy") from exc
        try:
            existing = await self._repository.get_outbox_event(
                replay_event_id,
                route_id=self.config.route_id,
                **self._ledger_scope,
            )
            if existing is not None:
                self._verify_late_0939_replay_record(
                    existing,
                    trade_date=context.trade_date,
                    official_entry_event_id=status.event_id,
                    official_entry_action=status.action,
                )
                if existing.payload is None:
                    await self._repository.assert_runtime_leader()
                    existing = await self._repository.seal_event(
                        replay_event_id,
                        seal_v20_payload,
                    )
                context.late_0939_replay_completed = True
                return existing

            semantic = await asyncio.wait_for(
                self._build_late_0939_replay_semantic(
                    context,
                    now,
                    replay_event_id=replay_event_id,
                ),
                timeout=LATE_0939_REPLAY_TOTAL_TIMEOUT_SECONDS,
            )
            semantic_hash = sha256_json(semantic)
            await self._repository.assert_runtime_leader()
            await self._repository.enqueue_alert(
                replay_event_id,
                self.config.route_id,
                dict(semantic),
                semantic_hash,
                **self._ledger_scope,
            )
            await self._repository.assert_runtime_leader()
            sealed = await self._repository.seal_event(
                replay_event_id,
                seal_v20_payload,
            )
            self._verify_late_0939_replay_record(
                sealed,
                trade_date=context.trade_date,
                official_entry_event_id=status.event_id,
                official_entry_action=status.action,
            )
            context.late_0939_replay_completed = True
            return sealed
        finally:
            self._late_0939_replay_lock.release()

    async def _maybe_run_late_0939_replay(
        self,
        context: _DayContext,
        now: datetime,
    ) -> None:
        status = context.entry_status
        if (
            now.timetz().replace(tzinfo=None) < self.config.clock.publish_deadline
            or status is None
            or status.action not in _LATE_0939_REPLAY_ACTIONS
            or context.late_0939_replay_completed
        ):
            return
        previous = context.late_0939_replay_last_attempt_at
        if (
            context.late_0939_replay_automatic_attempts >= LATE_0939_REPLAY_MAX_AUTOMATIC_ATTEMPTS
            or (
                previous is not None
                and (now - previous).total_seconds() < LATE_0939_REPLAY_RETRY_SECONDS
            )
        ):
            return
        context.late_0939_replay_last_attempt_at = now
        context.late_0939_replay_automatic_attempts += 1
        try:
            await self._ensure_late_0939_replay(context, now)
        except asyncio.CancelledError:
            raise
        except Exception:
            # This lane is retrospective and must never degrade live entry/exit
            # health.  It retries with a bounded cadence and a manual trigger
            # can request an immediate retry.
            logger.exception("V20 automatic late 09:39 replay failed; will retry")

    def _schedule_late_0939_replay(
        self,
        context: _DayContext,
        now: datetime,
    ) -> None:
        """Start the bounded replay off the five production scheduler lanes."""

        if (
            now.timetz().replace(tzinfo=None) < self.config.clock.publish_deadline
            or context.entry_status is None
            or context.entry_status.action not in _LATE_0939_REPLAY_ACTIONS
            or context.late_0939_replay_completed
            or context.late_0939_replay_automatic_attempts
            >= LATE_0939_REPLAY_MAX_AUTOMATIC_ATTEMPTS
            or (
                context.late_0939_replay_last_attempt_at is not None
                and (now - context.late_0939_replay_last_attempt_at).total_seconds()
                < LATE_0939_REPLAY_RETRY_SECONDS
            )
        ):
            return
        current = self._late_0939_replay_task
        if current is not None and not current.done():
            return
        self._late_0939_replay_task = asyncio.create_task(
            self._maybe_run_late_0939_replay(context, now),
            name=f"v20-late-0939-replay-{context.trade_date.isoformat()}",
        )

    async def _resolve_canonical_morning_bundle(
        self,
        trade_date: date,
        *,
        terminal_status: EntryStatus | None = None,
    ) -> tuple[FrozenV16ScanBundle, datetime, tuple[date, ...], str, bool, bool | None]:
        """Rerun V16 and bind any replay to its durable artifact fact boundary.

        A first calculation may acquire missing <=09:39 evidence and publish
        the artifact.  Once that artifact exists, every caller uses its exact
        comparison universe, exact raw-evidence code set, and database receipt
        as an immutable visibility cutoff; it may not backfill formal misses.
        The scanner still executes current strategy code, and the frozen input
        identities must match before output differences can be reported.
        """

        loaded = (
            await self._load_canonical_artifact(trade_date)
            if getattr(self, "_canonical_artifact_store", None) is not None
            else None
        )
        expected_bundle: FrozenV16ScanBundle | None = None
        # INPUT_INVALID proves that no usable formal V16 decision fact existed.
        # An artifact found later (or written by the legacy mixed-hash build)
        # is not an official comparison boundary: compute the current theory,
        # report NOT_AVAILABLE upstream, and leave that artifact untouched.
        ignore_existing_artifact = (
            loaded is not None
            and terminal_status is not None
            and getattr(terminal_status, "action", None) == "INPUT_INVALID"
        )
        calendar_hint: tuple[date, ...] = (
            tuple(self._context.calendar)
            if self._context is not None and self._context.trade_date == trade_date
            else tuple(self._calendar_cache)
            if self._calendar_loaded_for == trade_date
            else ()
        )
        artifact_received_at: datetime | None = None
        if loaded is not None and not ignore_existing_artifact:
            hydrated, artifact_received_at = loaded
            if isinstance(hydrated, CanonicalV16ScanBundle):
                expected_bundle = self._project_canonical_v16(
                    hydrated,
                    calendar=tuple(hydrated.computation_calendar),
                )
            elif isinstance(hydrated, FrozenV16ScanBundle):
                expected_bundle = hydrated
            else:
                raise V20SemanticConflict("canonical V16 artifact hydration is invalid")
        elif terminal_status is not None and terminal_status.action in {
            "ENTER",
            "BLOCK",
            "NO_SIGNAL",
        }:
            raise V20SemanticConflict(
                "terminal V20 slot lacks its canonical V16 artifact fact boundary"
            )

        exact_universe: tuple[str, ...] | None = None
        exact_evidence_codes: tuple[str, ...] | None = None
        exact_calendar: tuple[date, ...] | None = None
        if expected_bundle is not None:
            if artifact_received_at is None:
                raise V20SemanticConflict("canonical V16 artifact receipt boundary is missing")
            try:
                snapshot = expected_bundle.snapshot
                exact_universe = tuple(snapshot["comparison_pool_codes"])
                exact_evidence_codes = tuple(snapshot["raw_evidence_codes"])
                history_dates = tuple(
                    date.fromisoformat(str(value))
                    for value in snapshot["history_date_valid_counts"]
                )
                exact_calendar = tuple(
                    sorted(set((*history_dates, *expected_bundle.computation_calendar)))
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise V20SemanticConflict(
                    "canonical V16 artifact lacks its frozen raw fact boundary"
                ) from exc

        canonical = await self._compute_canonical_v16_from_persisted_raw(
            _DayContext(
                trade_date=trade_date,
                calendar=calendar_hint,
                canonical_fact_received_before=artifact_received_at,
                canonical_fact_universe=exact_universe,
                canonical_fact_evidence_codes=exact_evidence_codes,
                canonical_fact_calendar=exact_calendar,
                canonical_fact_allow_backfill=expected_bundle is None,
                canonical_fact_persist_raw=expected_bundle is None,
            )
        )
        calendar = tuple(canonical.computation_calendar)
        bundle = self._project_canonical_v16(canonical, calendar=calendar)

        artifact_compared = expected_bundle is not None
        artifact_matches: bool | None = None
        if expected_bundle is not None:
            self._verify_frozen_canonical_input_identity(expected_bundle, bundle)
            artifact_matches = expected_bundle.snapshot_hash == bundle.snapshot_hash and dict(
                expected_bundle.snapshot
            ) == dict(bundle.snapshot)
        elif (
            getattr(self, "_canonical_artifact_store", None) is not None
            and not ignore_existing_artifact
        ):
            await self._persist_canonical_artifact_barrier(canonical)
            persisted = await self._load_canonical_artifact(trade_date)
            if persisted is None:
                raise V20RepositoryError(
                    "recomputed canonical V16 completed without a durable artifact"
                )
            hydrated, _artifact_received_at = persisted
            if isinstance(hydrated, CanonicalV16ScanBundle):
                persisted_bundle = self._project_canonical_v16(
                    hydrated,
                    calendar=tuple(hydrated.computation_calendar),
                )
            elif isinstance(hydrated, FrozenV16ScanBundle):
                persisted_bundle = hydrated
            else:
                raise V20SemanticConflict("canonical V16 artifact hydration is invalid")
            if persisted_bundle.snapshot_hash != bundle.snapshot_hash or dict(
                persisted_bundle.snapshot
            ) != dict(bundle.snapshot):
                raise V20SemanticConflict(
                    "persisted canonical V16 differs from its scanner recomputation"
                )
            artifact_compared = True
            artifact_matches = True

        # The artifact receipt belongs only to comparison evidence.  It must
        # not influence actionability or masquerade as the fresh scan's
        # formation/receipt time.
        first_received_at = canonical.computed_at
        canonical_source = "PERSISTED_RAW_SCANNER_RECOMPUTATION"

        if bundle.trade_date != trade_date:
            raise V20SemanticConflict("canonical V16 artifact belongs to another trade date")
        expected_dependencies = self.config.strategy_dependency_hashes
        for logical_path, snapshot_field in (
            ("models/v20/lgbrank_latest.txt", "scorer_model_sha256"),
            ("models/v20/feature_list.json", "scorer_feature_sha256"),
        ):
            if bundle.snapshot.get(snapshot_field) != expected_dependencies.get(logical_path):
                raise V20SemanticConflict(
                    f"canonical V16 artifact uses a different frozen dependency: {logical_path}"
                )
        if first_received_at.tzinfo is None or first_received_at.utcoffset() is None:
            raise V20SemanticConflict("canonical V16 receipt clock must be timezone-aware")
        if trade_date not in calendar:
            raise V20SemanticConflict("canonical V16 artifact lacks its trade calendar")
        return (
            bundle,
            first_received_at.astimezone(SHANGHAI),
            tuple(calendar),
            canonical_source,
            artifact_compared,
            artifact_matches,
        )

    @staticmethod
    def _verify_frozen_canonical_input_identity(
        expected: FrozenV16ScanBundle,
        actual: FrozenV16ScanBundle,
    ) -> None:
        """Verify only external facts the existing artifact can prove exactly.

        Candidate minute and D1 rows are first fenced by the artifact's database
        receipt.  ``early_market_source_hash`` then binds the exact selected
        minute facts plus full D1, name, board, universe, and calendar facts;
        per-code OHLCV history hashes are checked separately.  Model/feature
        identities and scanner-derived fields are deliberately excluded: they
        are current-code outputs whose changes must be reported as ``DIFFERENT``,
        not rejected as input corruption.
        """

        immutable_input_fields = (
            "early_market_source_hash",
            "history_input_hashes",
            "comparison_pool_codes",
            "prior_trade_date",
        )
        expected_snapshot = expected.snapshot
        actual_snapshot = actual.snapshot
        differing = tuple(
            field
            for field in immutable_input_fields
            if expected_snapshot.get(field) != actual_snapshot.get(field)
        )
        if differing:
            raise V20SemanticConflict(
                "canonical V16 replay cannot prove the artifact fact boundary; "
                f"input identity differs: {', '.join(differing)}"
            )

    async def _compute_morning_selection(
        self,
        trade_date: date,
        *,
        allow_legacy_terminal_fresh_theoretical: bool = False,
    ) -> _MorningSelectionComputation:
        """Run the complete canonical V16 -> V20 policy calculation.

        This is the sole production call site for ``prepare_entry``.  It has no
        clock or actionability parameter, so a post-cutoff operator request and
        the automatic morning lane cannot select different algorithms.  A
        caller may commit or render the returned proposal only after this
        method completes.
        """

        status = await self._repository.get_entry_status(
            self.config.official_stream_id,
            trade_date,
        )
        if status is not None:
            self._verify_entry_binding(status)
        legacy_terminal_fresh_theoretical = (
            status is not None
            and allow_legacy_terminal_fresh_theoretical
            and self._terminal_lacks_canonical_state_before(status)
        )
        if legacy_terminal_fresh_theoretical:
            assert status is not None
            self._validate_legacy_terminal_without_prestate(status, trade_date)
        (
            bundle,
            first_received_at,
            calendar,
            canonical_source,
            artifact_compared,
            artifact_matches,
        ) = await self._resolve_canonical_morning_bundle(
            trade_date,
            terminal_status=status,
        )
        if status is not None and not legacy_terminal_fresh_theoretical:
            scheduled_source = status.semantic.get("scheduled_exits_today") or ()
            completed_health, completed_rolling, maturity_gaps = (
                self._policy_inputs_from_terminal_status(status)
            )
            calculation_state = self._state_before_from_terminal_status(status)
        else:
            scheduled_source = await self._scheduled_exits_today(trade_date)
            completed_health, completed_rolling, maturity_gaps = await self._policy_inputs(
                trade_date
            )
            calculation_state = await self._repository.load_state(self.config.state_lineage_id)
            if legacy_terminal_fresh_theoretical:
                assert status is not None
                self._validate_current_state_for_legacy_terminal(
                    calculation_state,
                    status,
                    trade_date,
                )
        scheduled = tuple(dict(item) for item in scheduled_source)
        prepared = prepare_entry(
            config=self.config,
            state=calculation_state,
            bundle=bundle,
            completed_health=completed_health,
            completed_rolling=completed_rolling,
            maturity_gaps=maturity_gaps,
            artifacts=self._artifacts,
            calendar=calendar,
            scheduled_exits_today=scheduled,
        )
        if status is not None and not legacy_terminal_fresh_theoretical:
            self._verify_terminal_replay_transition(status, prepared)
        if prepared.commit.semantic.get("action") not in {"ENTER", "BLOCK", "NO_SIGNAL"}:
            raise V20SemanticConflict(
                "canonical V20 morning calculation produced an illegal action"
            )
        return _MorningSelectionComputation(
            prepared=prepared,
            bundle=bundle,
            canonical_first_received_at=first_received_at,
            calendar=calendar,
            scheduled_exits_today=scheduled,
            canonical_source=canonical_source,
            canonical_artifact_compared=artifact_compared,
            canonical_artifact_matches=artifact_matches,
            legacy_terminal_fresh_theoretical=legacy_terminal_fresh_theoretical,
        )

    async def _orchestrate_morning_selection(
        self,
        trade_date: date,
        *,
        allow_legacy_terminal_fresh_theoretical: bool = False,
    ) -> _MorningSelectionComputation:
        """Single high-level entry point for every V20 morning calculation.

        This layer deliberately contains no actionability or official-write
        decision.  Scheduled, pre-cutoff manual, retrospective check-only, and
        legacy late-replay callers all receive the same prepared proposal; only
        their final fence decides whether it may be committed or merely shown.
        """

        if allow_legacy_terminal_fresh_theoretical:
            return await self._compute_morning_selection(
                trade_date,
                allow_legacy_terminal_fresh_theoretical=True,
            )
        return await self._compute_morning_selection(trade_date)

    async def _run_entry_collection_cycle(
        self,
        context: _DayContext,
        now: datetime,
    ) -> None:
        """Schedule the independent MEWS repair without precomputing selection."""

        if callable(getattr(self._repository, "get_entry_status", None)):
            await self._refresh_entry_status(context)
        if context.entry_status is not None:
            return
        wall = now.timetz().replace(tzinfo=None)
        if (
            wall < time.fromisoformat(self.config.clock.decision_bar_label)
            or wall >= self.config.clock.decision_finalization_deadline
        ):
            return
        # MEWS is deliberately not an entry input.  Kick its independent
        # singleflight, then leave the complete V16 -> V20 calculation to
        # ``_attempt_entry``.  Keeping scanner execution out of collection is
        # what guarantees one scanner and one ``prepare_entry`` call per live
        # slot instead of a pre-compute followed by a second commit compute.
        if (
            wall < self.config.clock.decision_finalization_deadline
            and self._mews_cached_for != context.trade_date
        ):
            self.kick_mews_for_selection_trigger(now)

    def _project_canonical_v16(
        self,
        canonical: Any,
        *,
        calendar: tuple[date, ...] | None = None,
    ) -> FrozenV16ScanBundle:
        result = canonical.scan_result
        try:
            raw_evidence_codes = sorted(canonical.early_bars)
            early_source_codes = sorted(canonical.early_source_hashes)
            scan_input_codes = sorted(canonical.stock_data)
        except (AttributeError, TypeError) as exc:
            raise V20SemanticConflict("canonical V16 raw evidence universe is invalid") from exc
        if (
            not raw_evidence_codes
            or raw_evidence_codes != early_source_codes
            or any(
                not isinstance(code, str) or len(code) != 6 or not code.isdigit()
                for code in raw_evidence_codes
            )
            or not set(scan_input_codes).issubset(raw_evidence_codes)
        ):
            raise V20SemanticConflict("canonical V16 raw evidence universe is invalid")
        external_market_fact_hash = getattr(canonical, "external_market_fact_hash", None)
        if (
            not isinstance(external_market_fact_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", external_market_fact_hash) is None
            or external_market_fact_hash == "0" * 64
        ):
            raise V20SemanticConflict("canonical V16 external market fact identity is unavailable")
        symbols = [
            {
                "rank": stock.rank,
                "code": stock.code,
                "name": stock.name,
                "score": float(stock.score),
                "snapshot_price": float(stock.buy_price),
                "boards": result.stock_all_boards.get(stock.code),
                "best_board": result.stock_best_board.get(stock.code),
                "is_driver": result.stock_is_driver.get(stock.code),
                "cci": result.stock_cci.get(stock.code),
                "volume_937": result.stock_early_vol.get(stock.code),
                "history_hash": sha256_json(dict(canonical.history_raw.get(stock.code, {}))),
                "early_source_hash": canonical.early_source_hashes.get(stock.code),
            }
            for stock in result.recommended
        ]
        resolved_calendar = (
            calendar
            if calendar is not None
            else self._calendar_cache
            if self._calendar_loaded_for == canonical.trade_date
            else ()
        )
        if canonical.trade_date not in resolved_calendar:
            raise V20RepositoryError("canonical V16 trade date lacks calendar evidence")
        canonical_calendar = tuple(getattr(canonical, "computation_calendar", ()))
        canonical_predecessors = [day for day in canonical_calendar if day < canonical.trade_date]
        canonical_successors = [day for day in canonical_calendar if day > canonical.trade_date]
        if (
            not canonical_predecessors
            or len(canonical_successors) < 2
            or canonical.prior_trade_date != canonical_predecessors[-1]
            or any(
                day not in resolved_calendar
                for day in (
                    canonical.prior_trade_date,
                    canonical.trade_date,
                    *canonical_successors[:2],
                )
            )
        ):
            raise V20RepositoryError("canonical V16 computation calendar is incompatible")
        snapshot = {
            "schema_version": V20_V16_SNAPSHOT_SCHEMA,
            "trade_date": canonical.trade_date.isoformat(),
            "last_complete_bar": self.config.clock.decision_bar_label,
            # This existing artifact field is a pure external-fact envelope.
            # ``canonical.input_hash`` is intentionally excluded because its
            # legacy contract mixes model and scanner-derived structures.
            "early_market_source_hash": external_market_fact_hash,
            "early_market_conflict_codes": [],
            "breadth_market_source_hash": canonical.breadth_market_source_hash,
            "breadth_market_missing_codes": list(canonical.breadth_market_missing_codes),
            "breadth_market_conflict_codes": list(canonical.breadth_market_conflict_codes),
            "scorer_model_sha256": canonical.model_sha256,
            "scorer_feature_sha256": canonical.feature_list_sha256,
            "list_complete": True,
            "list_n": len(symbols),
            "symbols": symbols,
            "scan_input_codes": scan_input_codes,
            "raw_evidence_codes": raw_evidence_codes,
            "scan_input_failure_codes": [
                code for code in canonical.universe if code not in canonical.stock_data
            ],
            "scan_input_coverage": len(canonical.stock_data) / len(canonical.universe),
            "history_profile_id": "CANONICAL_V16_V1",
            "history_input_hashes": {
                code: sha256_json(dict(history))
                for code, history in sorted(canonical.history_raw.items())
            },
            "history_date_valid_counts": dict(canonical.history_date_valid_counts),
            "history_min_date_coverage": canonical.history_min_date_coverage,
            "comparison_pool_codes": sorted(canonical.universe),
            "comparison_pool_hash": sha256_json(sorted(canonical.universe)),
            "breadth_valid_n": canonical.breadth_valid_n,
            "breadth_down_n": canonical.breadth_down_n,
            "prior_trade_date": (
                canonical.prior_trade_date.isoformat()
                if canonical.prior_trade_date is not None
                else None
            ),
            "prior_amount_yuan": dict(canonical.prior_amount_yuan),
            "funnel": {
                "step0_universe_count": result.step0_universe_count,
                "step2_hot_board_count": result.step2_hot_board_count,
                "step2_filtered_by_avg_gain": result.step2_filtered_by_avg_gain,
                "step3_count": result.step3_count,
                "step4_count": result.step4_count,
                "step5_count": result.step5_count,
                "step6_count": result.step6_count,
                "step6_5_count": result.step6_5_count,
                "step6_6_count": result.step6_6_count,
                "final_candidates": result.final_candidates,
            },
            "stages": {
                "step0_codes": sorted(result.step0_codes),
                "step2_boards_detail": {
                    board: sorted(codes)
                    for board, codes in sorted(result.step2_boards_detail.items())
                },
                "step2_codes": sorted(result.step2_codes),
                "st_eligible_codes": sorted(result.st_eligible_codes),
                "step3_codes": sorted(result.step3_codes),
                "step4_codes": sorted(result.step4_codes),
                "step5_codes": sorted(result.step5_codes),
                "step6_codes": sorted(result.step6_codes),
                "step6_5_codes": sorted(result.step6_5_codes),
                "step6_6_codes": sorted(result.step6_6_codes),
            },
            "board_avg_gains": dict(sorted(result.step2_board_avg_gains.items())),
        }
        return FrozenV16ScanBundle(
            trade_date=canonical.trade_date,
            frozen_at=canonical.computed_at,
            scan_result=result,
            stock_data=canonical.stock_data,
            comparison_pool_codes=tuple(sorted(canonical.universe)),
            breadth_valid_n=canonical.breadth_valid_n,
            breadth_down_n=canonical.breadth_down_n,
            prior_trade_date=(
                canonical.prior_trade_date
                if canonical.prior_trade_date is not None
                else canonical.trade_date
            ),
            prior_amount_yuan=dict(canonical.prior_amount_yuan),
            snapshot=snapshot,
            snapshot_hash=sha256_json(snapshot),
            computation_calendar=canonical_calendar,
        )

    def _verify_prewarm_dependencies(self, prewarmed: V20PrewarmedScan) -> None:
        """Reject in-process model or concept-data drift under a frozen lineage."""

        expected = self.config.strategy_dependency_hashes
        actual = {
            "models/v20/lgbrank_latest.txt": prewarmed.scorer_model_sha256,
            "models/v20/feature_list.json": prewarmed.scorer_feature_sha256,
        }
        for filename in ("sectors.json", "board_constituents.json"):
            path = resolve_concept_data_path(self.config.project_root, filename)
            try:
                actual[f"data/{filename}"] = hashlib.sha256(path.read_bytes()).hexdigest()
            except OSError as exc:
                raise V20SemanticConflict(
                    f"frozen strategy dependency is unreadable: data/{filename}"
                ) from exc
        for logical_path, actual_hash in actual.items():
            expected_hash = expected.get(logical_path)
            if expected_hash is None or actual_hash != expected_hash:
                raise V20SemanticConflict(
                    f"frozen strategy dependency drifted during runtime: {logical_path}"
                )

    async def _run_entry_cycle(self, context: _DayContext, now: datetime) -> None:
        await self._refresh_entry_status(context)
        if context.entry_status is not None:
            context.last_phase = "DECISION_COMMITTED"
            return
        wall = now.timetz().replace(tzinfo=None)
        if wall < self.config.clock.prewarm:
            return
        if wall >= self.config.clock.decision_finalization_deadline:
            await self._finalize_invalid_entry(
                context,
                now,
                reason="SLOT_FINALIZED_FAILED",
                detail="no durable normal entry decision existed before the 09:45 deadline",
                invalid_commit_not_before_ts=_local(
                    context.trade_date,
                    self.config.clock.decision_finalization_deadline,
                ),
            )
            return
        if wall >= self.config.clock.publish_deadline:
            # Once the strict buy boundary has arrived there is no legitimate
            # reason to keep the user waiting for the 09:45 ledger backstop.
            # The repository still gates this transition on its database clock,
            # so a fast/skewed application clock cannot finalize early.
            await self._finalize_invalid_entry(
                context,
                now,
                reason="ENTRY_INPUT_UNAVAILABLE_BY_0940",
                detail=(
                    "no durable normal V16 decision existed at the strict 09:40 "
                    "boundary; "
                    + (context.last_entry_failure_detail or f"last_phase={context.last_phase}")
                ),
                invalid_commit_not_before_ts=_local(
                    context.trade_date,
                    self.config.clock.publish_deadline,
                ),
            )
            return

        # Trigger-side MEWS repair is also invoked independently before
        # maturity and predecessor reconciliation.  Repeating the idempotent
        # check here covers direct unit/manual invocations of the decision lane.
        await self._run_entry_collection_cycle(context, now)

        if (
            wall >= time.fromisoformat(self.config.clock.decision_bar_label)
            and wall < self.config.clock.decision_finalization_deadline
        ):
            try:
                await self._attempt_entry(context, now)
            except Exception as exc:
                context.last_phase = "ENTRY_RETRY"
                context.last_entry_failure_detail = f"ENTRY_RETRY: {type(exc).__name__}: {exc}"
                self._record_lane_error(
                    "decision",
                    f"ENTRY_RETRY: {type(exc).__name__}: {exc}",
                    now,
                )
                if wall >= self.config.clock.decision_finalization_deadline:
                    await self._finalize_invalid_entry(
                        context,
                        now,
                        reason="SLOT_FINALIZED_FAILED",
                        detail=f"{type(exc).__name__}: {exc}",
                        invalid_commit_not_before_ts=_local(
                            context.trade_date,
                            self.config.clock.decision_finalization_deadline,
                        ),
                    )
                else:
                    logger.warning("V20 entry attempt will retry: %s", exc)

        if (
            context.entry_status is None
            and wall >= self.config.clock.decision_finalization_deadline
        ):
            await self._finalize_invalid_entry(
                context,
                now,
                reason="SLOT_FINALIZED_FAILED",
                detail="exact raw 09:39 V16 decision was not durably committed by 09:45",
                invalid_commit_not_before_ts=_local(
                    context.trade_date,
                    self.config.clock.decision_finalization_deadline,
                ),
            )

    async def _poll_entry_market(self, context: _DayContext, now: datetime) -> None:
        if context.prewarmed is None or context.collector is None:
            return
        target_label = min(
            now.strftime("%H:%M"),
            self.config.clock.decision_bar_label,
        )
        # Quote completeness is a property of the V16 scan universe.  The
        # much wider breadth sample is fetched in the same request only for
        # latency efficiency and must never fill a missing V16 quote slot.
        captured_n = sum(
            (context.trade_date, code, target_label) in context.minute_rows
            for code in context.prewarmed.universe_codes
        )
        if (
            target_label < self.config.clock.decision_bar_label
            and captured_n / len(context.prewarmed.universe_codes)
            >= self.config.market.minimum_quote_coverage
        ):
            return
        if (
            context.last_early_poll_at is not None
            and (now - context.last_early_poll_at).total_seconds()
            < self.config.market.minute_poll_seconds
        ):
            return
        rows = await self._poll_latest(
            context,
            context.prewarmed.required_minute_codes,
            observed_at=now,
        )
        context.collector.ingest(rows.values())
        if context.breadth_collector is not None:
            context.breadth_collector.ingest(rows.values())
        context.last_early_poll_at = now

    async def _attempt_entry(self, context: _DayContext, now: datetime) -> None:
        await self._commit_entry_from_bundle(context, now)

    async def _commit_entry_from_bundle(
        self,
        context: _DayContext,
        now: datetime,
    ) -> None:
        calculation = await self._orchestrate_morning_selection(context.trade_date)
        resolved_bundle = calculation.bundle
        prepared = calculation.prepared
        context.canonical_bundle = resolved_bundle
        context.canonical_first_received_at = calculation.canonical_first_received_at

        if (
            resolved_bundle.frozen_at.tzinfo is None
            or resolved_bundle.frozen_at.utcoffset() is None
        ):
            raise V20SemanticConflict("V16 decision formation clock must be timezone-aware")
        formed_at = resolved_bundle.frozen_at.astimezone(SHANGHAI)
        if formed_at.date() != context.trade_date:
            raise V20SemanticConflict("V16 decision formation date does not match its slot")
        # ``now`` is the scheduler's start sample.  A slow but valid strategy
        # calculation may finish after 09:40, so actionability must be decided
        # from a fresh clock sample only after the shared calculation returns.
        observed_at = self._aware_now()
        normal_deadline = _local(context.trade_date, self.config.clock.publish_deadline)
        received_at = calculation.canonical_first_received_at
        context.canonical_entry_mode = (
            "ACTIONABLE" if received_at < normal_deadline else "CHECK_ONLY"
        )
        context.canonical_entry_action = (
            "NO_SIGNAL" if len(resolved_bundle.scan_result.recommended) == 0 else "CANDIDATES_READY"
        )
        if (
            formed_at >= normal_deadline
            or received_at >= normal_deadline
            or observed_at >= normal_deadline
        ):
            await self._finalize_invalid_entry(
                context,
                observed_at,
                reason="INPUT_TIME_BOUNDARY_VIOLATION",
                detail=(
                    "normal V16 ENTER/BLOCK/NO_SIGNAL missed the strict 09:40 "
                    f"formation/submission boundary: formed_at={formed_at.isoformat()}, "
                    f"received_at={received_at.isoformat()}, "
                    f"observed_at={observed_at.isoformat()}"
                ),
                invalid_commit_not_before_ts=normal_deadline,
            )
            return
        try:
            await self._repository.commit_entry(prepared.commit)
        except V20EntryDeadlineExceeded:
            rejected_at = self._aware_now()
            await self._finalize_invalid_entry(
                context,
                rejected_at,
                reason="INPUT_TIME_BOUNDARY_VIOLATION",
                detail=(
                    "database clock rejected normal V16 submission at the strict "
                    f"09:40 boundary: formed_at={formed_at.isoformat()}, "
                    f"rejected_at={rejected_at.isoformat()}"
                ),
                invalid_commit_not_before_ts=normal_deadline,
            )
            return
        status = await self._repository.get_entry_status(
            self.config.official_stream_id,
            context.trade_date,
        )
        if status is None:
            raise V20RepositoryError("committed V20 entry is not readable")
        self._verify_entry_binding(status)
        context.entry_status = status
        context.last_phase = "DECISION_COMMITTED"
        await self._repository.seal_event(prepared.commit.event_id, seal_v20_payload)

    async def _finalize_invalid_entry(
        self,
        context: _DayContext,
        now: datetime,
        *,
        reason: str,
        detail: str,
        invalid_commit_not_before_ts: datetime,
    ) -> None:
        existing = await self._repository.get_entry_status(
            self.config.official_stream_id,
            context.trade_date,
        )
        if existing is not None:
            self._verify_entry_binding(existing)
            context.entry_status = existing
            return
        health, rolling, gaps = await self._policy_inputs(context.trade_date)
        state = await self._repository.load_state(self.config.state_lineage_id)
        prepared = prepare_invalid_entry(
            config=self.config,
            state=state,
            trade_date=context.trade_date,
            calendar=context.calendar,
            reason_code=reason,
            detail=detail,
            invalid_commit_not_before_ts=invalid_commit_not_before_ts,
            completed_health=health,
            completed_rolling=rolling,
            maturity_gaps=gaps,
            scheduled_exits_today=await self._scheduled_exits_today(context.trade_date),
        )
        await self._repository.commit_entry(prepared.commit)
        await self._repository.seal_event(prepared.commit.event_id, seal_v20_payload)
        context.entry_status = await self._repository.get_entry_status(
            self.config.official_stream_id,
            context.trade_date,
        )
        context.last_phase = "INPUT_INVALID"
        await self._safe_alert(
            code=reason,
            entity_id=context.trade_date.isoformat(),
            message=detail,
            now=now,
        )

    async def _run_reference_cycle(self, context: _DayContext, now: datetime) -> None:
        try:
            await self._acquire_rolling7_d0_evidence(context, now)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "V20 Rolling7 D0 evidence acquisition left %s pending: %s",
                context.trade_date.isoformat(),
                exc,
            )
        status = await self._refresh_entry_status(context)
        if status is None or context.reference_finalized:
            return
        if status.action in {"NO_SIGNAL", "INPUT_INVALID"}:
            context.reference_finalized = True
            return
        if now.timetz().replace(tzinfo=None) < time.fromisoformat(
            self.config.clock.reference_bar_label
        ):
            return
        comparison_codes, symbol_codes = self._reference_code_sets(status)
        required = tuple(sorted(set(comparison_codes).union(symbol_codes)))
        if not required:
            context.reference_finalized = True
            return
        try:
            stored = await self._repository.list_raw_minute_bar_records(
                required,
                trade_date=context.trade_date,
                end_labels=(self.config.clock.reference_bar_label,),
            )
        except V20MinuteBarIntegrityConflict as exc:
            stored = list(exc.partial_records)
            await self._safe_alert(
                code="REFERENCE_RAW_FACT_CORRUPT",
                entity_id=context.trade_date.isoformat(),
                message=f"ignored {len(exc.corrupt_labels)} corrupt 09:41 row(s)",
                now=now,
            )
        for item in stored:
            self._remember_bar(context, _tushare_minute_from_record(item.payload))
        reference_poll_due = (
            context.last_reference_poll_at is None
            or (now - context.last_reference_poll_at).total_seconds()
            >= self.config.market.minute_poll_seconds
        )
        if reference_poll_due:
            await self._poll_latest(context, required, observed_at=now)
            context.last_reference_poll_at = now

        def _staged_reference_rows() -> dict[str, TushareMinuteBar]:
            # D0 is an inbox collection phase, not the reference arbiter.
            # Different observations of the same 09:41 fact are legitimate
            # revision candidates and must all remain persisted.  The D1 fixed-
            # cutoff path below selects the latest eligible receipt and treats
            # only an equal-receipt/different-hash tie as a true conflict.
            rows = {
                code: bar
                for (bar_date, code, label), bar in context.minute_rows.items()
                if bar_date == context.trade_date
                and label == self.config.clock.reference_bar_label
                and code in required
            }
            return rows

        # The shared ``rt_min`` poll above is the only current-day reference
        # acquisition path.  A missing 09:41 row stays recoverable on the next
        # batch poll; never fan out Top-N ``rt_min_daily`` calls here.
        exact = _staged_reference_rows()
        _prices, missing_codes, _source_hash = exact_reference_prices(
            exact,
            required,
            trade_date=context.trade_date,
            expected_label=self.config.clock.reference_bar_label,
        )

        # D0 only stages immutable raw evidence.  PENDING is deliberately not
        # finalized here: the frozen contract arbitrates *all* revisions whose
        # first receipt is before D1 09:30.  Locking the first D0 observation
        # would make worker timing decide which revision wins.
        wall = now.timetz().replace(tzinfo=None)
        snapshot_complete = not missing_codes
        shadow_cutoff = _local(
            context.trade_date,
            self.config.clock.decision_finalization_deadline,
        )
        freeze_partial = (
            wall >= self.config.clock.decision_finalization_deadline
            and await self._repository.database_cutoff_reached(shadow_cutoff)
        )
        if freeze_partial:
            context.reference_finalized = True
            context.last_phase = (
                "REFERENCE_EVIDENCE_STAGED" if snapshot_complete else "REFERENCE_EVIDENCE_PARTIAL"
            )
        elif snapshot_complete:
            # Keep polling through the bounded D0 collection window.  The D1
            # arbiter intentionally chooses the latest eligible revision, so
            # stopping on the first complete response would make worker timing
            # decide which source revision ever reaches the durable inbox.
            context.last_phase = "REFERENCE_EVIDENCE_STAGED"
        else:
            context.last_phase = "REFERENCE_EVIDENCE_PENDING"

        if missing_codes and freeze_partial:
            await self._safe_alert(
                code="REFERENCE_PARTIAL_SNAPSHOT",
                entity_id=context.trade_date.isoformat(),
                message=f"raw 09:41 missing {len(missing_codes)} codes; valid siblings retained",
                now=now,
            )

    async def _expire_reference_gaps(self, context: _DayContext, now: datetime) -> None:
        active = await self._repository.list_active_legs(context.trade_date, **self._ledger_scope)
        pending_legs = [leg for leg in active if leg.reference_status == "PENDING"]
        pending_shadows = await self._repository.list_pending_shadow_reference_batches(
            context.trade_date, **self._ledger_scope
        )
        pending_shadows = [batch for batch in pending_shadows if batch.kind == "HEALTH"]

        signal_dates = {leg.signal_date for leg in pending_legs}
        signal_dates.update(batch.signal_date for batch in pending_shadows)
        for signal_date in sorted(signal_dates):
            active_legs = [leg for leg in pending_legs if leg.signal_date == signal_date]
            legs = await self._repository.list_pending_reference_legs(
                signal_date, **self._ledger_scope
            )
            shadows = [batch for batch in pending_shadows if batch.signal_date == signal_date]
            d1_candidates = {leg.d1 for leg in active_legs}
            for batch in shadows:
                raw_d1 = batch.payload.get("d1")
                try:
                    d1_candidates.add(date.fromisoformat(str(raw_d1)))
                except ValueError as exc:
                    raise V20RepositoryError(
                        f"shadow batch {batch.batch_id!r} has invalid D1"
                    ) from exc
            if len(d1_candidates) != 1:
                raise V20RepositoryError(
                    f"reference D1 is missing or inconsistent for {signal_date.isoformat()}"
                )
            d1 = next(iter(d1_candidates))
            model_cutoff = _local(d1, self.config.clock.reference_lock_deadline_next_day)
            model_cutoff_reached = await self._repository.database_cutoff_reached(model_cutoff)
            if not model_cutoff_reached:
                # A restart on D1 must still be able to stage a selected leg's
                # raw D0 09:41 row before the immutable 09:30 receipt cutoff.
                # This bounded historical query deliberately targets model legs
                # only; the HEALTH comparison snapshot is collected on D0 and
                # has its own explicit 09:45 cutoff.
                history_targets = tuple(sorted({leg.code for leg in legs}))
                last_attempt = context.reference_gap_history_last_at.get(signal_date)
                history_due = last_attempt is None or (now - last_attempt).total_seconds() >= 60
                if history_targets and now.date() == d1 and history_due:
                    context.reference_gap_history_last_at[signal_date] = now
                    try:
                        history = await asyncio.wait_for(
                            self._scan_state.realtime_client.batch_get_minute_history_for_date(
                                list(history_targets), signal_date
                            ),
                            timeout=20.0,
                        )
                        await self._persist_history(context, history, observed_at=now)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        await self._safe_alert(
                            code="REFERENCE_HISTORY_RECOVERY_FAILED",
                            entity_id=signal_date.isoformat(),
                            message=f"{type(exc).__name__}: {exc}",
                            now=now,
                        )
                continue

            model_codes = tuple(sorted({leg.code for leg in legs}))
            shadow_codes: set[str] = set()
            for batch in shadows:
                for payload_field in ("top3", "symbols"):
                    values = batch.payload.get(payload_field) or []
                    shadow_codes.update(
                        str(item.get("code"))
                        for item in values
                        if isinstance(item, Mapping) and item.get("code")
                    )
                shadow_codes.update(
                    str(code) for code in (batch.payload.get("comparison_pool_codes") or [])
                )
            if any(
                len(code) != 6 or not code.isdigit()
                for code in set(model_codes).union(shadow_codes)
            ):
                raise V20RepositoryError(
                    f"reference code set is invalid for {signal_date.isoformat()}"
                )

            model_corrupt_labels: tuple[tuple[str, date, str], ...] = ()
            try:
                model_records = (
                    await self._repository.list_raw_minute_bar_records(
                        model_codes,
                        trade_date=signal_date,
                        end_labels=(self.config.clock.reference_bar_label,),
                        received_before=model_cutoff,
                    )
                    if model_codes
                    else []
                )
            except V20MinuteBarIntegrityConflict as exc:
                model_records = list(exc.partial_records)
                model_corrupt_labels = exc.corrupt_labels
            model_result = _arbitrate_reference_records(
                model_records,
                model_codes,
                trade_date=signal_date,
                expected_label=self.config.clock.reference_bar_label,
            )

            # HEALTH compares a market-wide D0 snapshot.  Rebuilding
            # thousands of names on D1 would make the result depend on a later
            # bulk history job, so their immutable receipt cutoff is D0 09:45.
            # Model legs remain independently recoverable until D1 09:30.
            shadow_cutoff = _local(
                signal_date,
                self.config.clock.decision_finalization_deadline,
            )
            normalized_shadow_codes = tuple(sorted(shadow_codes))
            shadow_corrupt_labels: tuple[tuple[str, date, str], ...] = ()
            try:
                shadow_records = (
                    await self._repository.list_raw_minute_bar_records(
                        normalized_shadow_codes,
                        trade_date=signal_date,
                        end_labels=(self.config.clock.reference_bar_label,),
                        received_before=shadow_cutoff,
                    )
                    if normalized_shadow_codes
                    else []
                )
            except V20MinuteBarIntegrityConflict as exc:
                shadow_records = list(exc.partial_records)
                shadow_corrupt_labels = exc.corrupt_labels
            shadow_result = _arbitrate_reference_records(
                shadow_records,
                normalized_shadow_codes,
                trade_date=signal_date,
                expected_label=self.config.clock.reference_bar_label,
            )

            for leg in legs:
                if leg.code not in model_result.prices:
                    continue
                leg_prices, _missing, leg_hash = exact_reference_prices(
                    {leg.code: model_result.exact_bars[leg.code]},
                    (leg.code,),
                    trade_date=signal_date,
                    expected_label=self.config.clock.reference_bar_label,
                )
                await self._repository.lock_reference_price(
                    leg.model_leg_id,
                    official_stream_id=self.config.official_stream_id,
                    lineage_id=self.config.state_lineage_id,
                    reference_profile_id=self.config.reference_profile_id,
                    price=leg_prices[leg.code],
                    snapshot_hash=leg_hash,
                    not_before_ts=model_cutoff,
                )

            if shadows and shadow_result.prices:
                await self._repository.update_shadow_references(
                    signal_date,
                    reference_prices=shadow_result.prices,
                    snapshot_hash=shadow_result.source_hash,
                    not_before_ts=shadow_cutoff,
                    **self._ledger_scope,
                )

            model_evidence_hash = sha256_json(
                {
                    "profile": self.config.reference_profile_id,
                    "signal_date": signal_date.isoformat(),
                    "status": "REFERENCE_UNAVAILABLE_AT_DEADLINE",
                    "scope": "MODEL_LEGS",
                    "deadline": model_cutoff.isoformat(),
                    "missing_codes": list(model_result.missing_codes),
                    "conflict_codes": list(model_result.conflict_codes),
                    "corrupt_labels": [
                        [code, day.isoformat(), label] for code, day, label in model_corrupt_labels
                    ],
                }
            )
            await self._repository.finalize_pending_references_unavailable(
                signal_date,
                reference_profile_id=self.config.reference_profile_id,
                snapshot_hash=model_evidence_hash,
                not_before_ts=model_cutoff,
                **self._ledger_scope,
            )
            shadow_evidence_hash = sha256_json(
                {
                    "profile": self.config.reference_profile_id,
                    "signal_date": signal_date.isoformat(),
                    "status": "REFERENCE_UNAVAILABLE_AT_DEADLINE",
                    "scope": "SHADOW_BATCHES",
                    "deadline": shadow_cutoff.isoformat(),
                    "missing_codes": list(shadow_result.missing_codes),
                    "conflict_codes": list(shadow_result.conflict_codes),
                    "corrupt_labels": [
                        [code, day.isoformat(), label] for code, day, label in shadow_corrupt_labels
                    ],
                }
            )
            if shadows and not shadow_result.prices:
                await self._repository.finalize_shadow_references_unavailable(
                    signal_date,
                    snapshot_hash=shadow_evidence_hash,
                    not_before_ts=shadow_cutoff,
                    **self._ledger_scope,
                )
            if (
                model_result.missing_codes
                or model_result.conflict_codes
                or shadow_result.missing_codes
                or shadow_result.conflict_codes
                or model_corrupt_labels
                or shadow_corrupt_labels
            ):
                await self._safe_alert(
                    code="REFERENCE_UNAVAILABLE",
                    entity_id=signal_date.isoformat(),
                    message=(
                        "raw 09:41参考价在固定截止后存在缺口："
                        f"model_missing={len(model_result.missing_codes)}, "
                        f"model_conflict={len(model_result.conflict_codes)}, "
                        f"shadow_missing={len(shadow_result.missing_codes)}, "
                        f"shadow_conflict={len(shadow_result.conflict_codes)}；"
                        "模型腿截止=D1 09:30，影子批次截止=D0 09:45；"
                        "有效模型腿已独立锁定，缺失腿仅保留D2计划退出"
                    ),
                    now=now,
                )

    async def _run_exit_cycle(
        self,
        context: _DayContext,
        now: datetime,
        *,
        include_stale: bool = True,
        deadline: float | None = None,
        tick_started_at: float | None = None,
    ) -> None:
        loop = asyncio.get_running_loop()
        tick_budget = self._live_exit_tick_budget()
        if deadline is None:
            tick_started_at = loop.time()
            deadline = tick_started_at + tick_budget
        elif tick_started_at is None:
            tick_started_at = deadline - tick_budget

        async def stage(
            operation_factory: Callable[[], Awaitable[Any]],
            *,
            name: str,
            cap: float,
            symbols: Sequence[str],
            provider: str = "postgres",
        ) -> Any:
            return await self._run_live_exit_stage(
                operation_factory,
                stage=name,
                stage_cap=cap,
                deadline=deadline,
                tick_started_at=tick_started_at or loop.time(),
                symbols=symbols,
                provider=provider,
            )

        active = await stage(
            lambda: self._repository.list_active_legs(context.trade_date, **self._ledger_scope),
            name="db_list_active_legs",
            cap=LIVE_EXIT_DB_STAGE_TIMEOUT_SECONDS,
            symbols=(),
        )
        if not active:
            return

        today_legs = self._today_exit_legs(active, context.trade_date)
        all_codes = tuple(leg.code for leg in today_legs)
        unresolved_rule_timeout = await stage(
            lambda: self._evaluate_active_exits(
                today_legs,
                now,
                context.calendar,
                deadline=deadline,
                symbol_timeout_seconds=LIVE_EXIT_RULE_SYMBOL_TIMEOUT_SECONDS,
            ),
            name="rules_initial",
            cap=LIVE_EXIT_RULE_BATCH_TIMEOUT_SECONDS,
            symbols=all_codes,
            provider="rules",
        )
        active = await stage(
            lambda: self._repository.list_active_legs(context.trade_date, **self._ledger_scope),
            name="db_list_after_initial_rules",
            cap=LIVE_EXIT_DB_STAGE_TIMEOUT_SECONDS,
            symbols=(),
        )
        if not active:
            return

        wall = now.timetz().replace(tzinfo=None)
        session = time(9, 31) < wall < time(11, 31) or time(13, 1) < wall < time(14, 58)
        today_legs = self._today_exit_legs(active, context.trade_date)
        tick_target_codes = frozenset(leg.code for leg in today_legs)
        expected_labels = frozenset(_expected_exit_labels(context.trade_date, now))
        freshest_expected_labels = _live_exit_health_labels(
            context.trade_date,
            now,
            expected_labels,
        )

        def missing_labels(code: str) -> frozenset[str]:
            observed_labels = {
                label
                for (bar_date, stored_code, label), bar in context.minute_rows.items()
                if bar_date == context.trade_date
                and stored_code == code
                and label in expected_labels
                and bar.is_valid
                and bar.volume > 0
                and bar.amount > 0
            }
            return expected_labels - observed_labels

        initial_missing = {code: missing_labels(code) for code in sorted(tick_target_codes)}
        cold_context = bool(tick_target_codes) and all(
            len(missing) >= 2 for missing in initial_missing.values()
        )

        latest_attempted = cold_context
        latest_failed = False
        latest_evidence_codes: set[str] = set()
        if (
            session
            and today_legs
            and not cold_context
            and (
                context.last_exit_poll_at is None
                or (now - context.last_exit_poll_at).total_seconds()
                >= self.config.market.exit_poll_seconds
            )
        ):
            latest_attempted = True
            try:
                latest = await self._poll_latest(
                    context,
                    sorted(tick_target_codes),
                    observed_at=now,
                    deadline=deadline,
                    tick_started_at=tick_started_at,
                )
                latest_evidence_codes.update(
                    _legal_exit_evidence_codes(
                        latest.values(),
                        trade_date=context.trade_date,
                        expected_labels=freshest_expected_labels,
                    )
                )
            except asyncio.CancelledError:
                raise
            except V20LiveExitStageTimeout:
                raise
            except Exception:
                latest_failed = True
                logger.exception("V20 latest-minute exit poll failed; trying daily history")
            finally:
                context.last_exit_poll_at = now

        active = await stage(
            lambda: self._repository.list_active_legs(context.trade_date, **self._ledger_scope),
            name="db_list_after_latest",
            cap=LIVE_EXIT_DB_STAGE_TIMEOUT_SECONDS,
            symbols=(),
        )
        today_legs = self._today_exit_legs(active, context.trade_date)
        unresolved_rule_timeout = await stage(
            lambda: self._evaluate_active_exits(
                today_legs,
                now,
                context.calendar,
                deadline=deadline,
                symbol_timeout_seconds=LIVE_EXIT_RULE_SYMBOL_TIMEOUT_SECONDS,
            ),
            name="rules_after_latest",
            cap=LIVE_EXIT_RULE_BATCH_TIMEOUT_SECONDS,
            symbols=tuple(leg.code for leg in today_legs),
            provider="rules",
        )
        active = await stage(
            lambda: self._repository.list_active_legs(context.trade_date, **self._ledger_scope),
            name="db_list_before_history",
            cap=LIVE_EXIT_DB_STAGE_TIMEOUT_SECONDS,
            symbols=(),
        )
        today_legs = self._today_exit_legs(active, context.trade_date)

        recovery_codes: list[str] = []
        for code in sorted({leg.code for leg in today_legs}):
            missing = missing_labels(code)
            if len(missing) < 2:
                continue
            key = (code, context.trade_date)
            last_attempt = context.exit_history_last_attempt.get(key)
            recovery_due = last_attempt is None or (now - last_attempt).total_seconds() >= 60
            if recovery_due:
                recovery_codes.append(code)

        history_attempted = False
        history_failed = False
        history_evidence_codes: set[str] = set()
        if recovery_codes:
            history_attempted = True
            requested_keys = {(code, context.trade_date) for code in recovery_codes}
            try:
                history = await stage(
                    lambda: self._scan_state.realtime_client.batch_get_minute_history(
                        recovery_codes
                    ),
                    name="history",
                    cap=LIVE_EXIT_LIVE_HISTORY_TIMEOUT_SECONDS,
                    symbols=recovery_codes,
                    provider="tushare_rt",
                )
                persisted_history = await self._persist_history(
                    context,
                    history,
                    observed_at=now,
                    deadline=deadline,
                    tick_started_at=tick_started_at,
                )
                history_evidence_codes.update(
                    _legal_exit_evidence_codes(
                        persisted_history,
                        trade_date=context.trade_date,
                        expected_labels=freshest_expected_labels,
                    )
                )
                scanned_through = max(expected_labels)
                for code in recovery_codes:
                    complete, scan_hash = _complete_history_evidence(
                        code,
                        context.trade_date,
                        tuple(history.get(code, ())),
                        scanned_through_label=scanned_through,
                        profile="TUSHARE_RT_MIN_DAILY_SCAN_WATERMARK_V2",
                    )
                    if not complete:
                        continue
                    for leg in today_legs:
                        if leg.code == code:

                            async def record_scan_watermark(
                                *,
                                model_leg_id: str = leg.model_leg_id,
                                source_hash: str = scan_hash,
                            ) -> bool:
                                return await self._repository.record_exit_scan_watermark(
                                    model_leg_id,
                                    trade_date=context.trade_date,
                                    scanned_through_label=scanned_through,
                                    source_hash=source_hash,
                                    **self._ledger_scope,
                                )

                            await stage(
                                record_scan_watermark,
                                name="db_exit_scan_watermark",
                                cap=LIVE_EXIT_DB_STAGE_TIMEOUT_SECONDS,
                                symbols=(code,),
                            )
                for key in requested_keys:
                    context.exit_history_last_attempt[key] = now
            except asyncio.CancelledError:
                raise
            except V20LiveExitStageTimeout:
                history_failed = True
                for key in requested_keys:
                    context.exit_history_last_attempt[key] = now
                # The tick boundary records the structured incident uniformly.
                raise
            except Exception as exc:
                history_failed = True
                for key in requested_keys:
                    context.exit_history_last_attempt[key] = now
                await self._safe_alert(
                    code="EXIT_HISTORY_RECOVERY_FAILED",
                    entity_id=(f"{','.join(recovery_codes)}:{context.trade_date.isoformat()}"),
                    message=f"{type(exc).__name__}: {exc}",
                    now=now,
                )

        context_evidence_codes: set[str] = set()
        if freshest_expected_labels:
            context_evidence_codes.update(
                _legal_exit_evidence_codes(
                    (
                        bar
                        for (bar_date, code, label), bar in context.minute_rows.items()
                        if bar_date == context.trade_date
                        and code in tick_target_codes
                        and label in freshest_expected_labels
                    ),
                    trade_date=context.trade_date,
                    expected_labels=freshest_expected_labels,
                )
            )
        evidence_codes = (
            latest_evidence_codes | history_evidence_codes | context_evidence_codes
        ) & set(tick_target_codes)
        checked_both_sources = latest_attempted and history_attempted
        lunch_history_authoritative = bool(
            history_attempted and time(11, 31) <= wall <= time(13, 1)
        )
        required_sources_checked = checked_both_sources or lunch_history_authoritative
        required_sources_failed = bool(
            len(tick_target_codes) > 1
            or (checked_both_sources and latest_failed and history_failed)
            or (lunch_history_authoritative and history_failed)
        )
        global_outage = bool(
            tick_target_codes
            and expected_labels
            and not evidence_codes
            and (
                context.live_exit_market_data_outage
                or (required_sources_checked and required_sources_failed)
            )
        )
        if global_outage:
            context.live_exit_market_data_outage = True
            target_text = ",".join(sorted(tick_target_codes))
            checked_source_text = (
                "current-day history"
                if lunch_history_authoritative
                else "latest-minute and current-day history"
            )
            diagnostic_alert_emitted = await self._safe_alert(
                code="LIVE_EXIT_MARKET_DATA_UNAVAILABLE",
                entity_id=f"{context.trade_date.isoformat()}:{target_text}",
                message=(
                    "no legal current-day exit evidence could be persisted for any "
                    f"live target from {checked_source_text}: {target_text}"
                ),
                now=now,
            )
            raise V20LiveExitIncidentError(
                "all live exit targets lack persisted legal current-day market evidence",
                diagnostic_alert_emitted=diagnostic_alert_emitted,
            )
        if evidence_codes:
            context.live_exit_market_data_outage = False

        unavailable_codes = tick_target_codes - evidence_codes
        if required_sources_checked and unavailable_codes and not global_outage:
            missing_symbols = sorted(unavailable_codes)
            healthy_siblings = sorted(tick_target_codes & evidence_codes)
            missing_text = ",".join(missing_symbols)
            healthy_text = ",".join(healthy_siblings) or "none"
            await self._safe_alert(
                code="LIVE_EXIT_SYMBOL_DATA_GAP",
                entity_id=f"{context.trade_date.isoformat()}:{missing_text}",
                message=(
                    "individual live-exit symbols returned no persisted legal evidence: "
                    f"missing symbols={missing_text}; healthy siblings={healthy_text}"
                ),
                now=now,
                semantic_extras={
                    "missing_symbols": missing_symbols,
                    "healthy_siblings": healthy_siblings,
                },
            )

        active = await stage(
            lambda: self._repository.list_active_legs(context.trade_date, **self._ledger_scope),
            name="db_list_final",
            cap=LIVE_EXIT_DB_STAGE_TIMEOUT_SECONDS,
            symbols=(),
        )
        today_legs = self._today_exit_legs(active, context.trade_date)
        unresolved_rule_timeout = await stage(
            lambda: self._evaluate_active_exits(
                today_legs,
                now,
                context.calendar,
                deadline=deadline,
                symbol_timeout_seconds=LIVE_EXIT_RULE_SYMBOL_TIMEOUT_SECONDS,
            ),
            name="rules_final",
            cap=LIVE_EXIT_RULE_BATCH_TIMEOUT_SECONDS,
            symbols=tuple(leg.code for leg in today_legs),
            provider="rules",
        )
        if isinstance(unresolved_rule_timeout, V20LiveExitStageTimeout):
            raise unresolved_rule_timeout
        if include_stale:
            await self._run_stale_exit_cycle(context, now)

    async def _run_stale_exit_cycle(
        self,
        context: _DayContext,
        now: datetime,
    ) -> None:
        """Recover a bounded restart backlog without delaying today's legs."""

        active = await self._repository.list_active_legs(context.trade_date, **self._ledger_scope)
        stale_page = self._stale_exit_page(active, context.trade_date)
        if not stale_page:
            return
        await self._evaluate_active_exits(stale_page, now, context.calendar)
        active = await self._repository.list_active_legs(context.trade_date, **self._ledger_scope)
        stale_page = self._stale_exit_page(active, context.trade_date)
        if not stale_page:
            return
        await self._recover_closed_exit_windows(context, stale_page, now)
        active = await self._repository.list_active_legs(context.trade_date, **self._ledger_scope)
        await self._evaluate_active_exits(
            self._stale_exit_page(active, context.trade_date),
            now,
            context.calendar,
        )

    async def _run_closed_and_stale_exit_cycle(
        self,
        context: _DayContext,
        now: datetime,
    ) -> None:
        active = await self._repository.list_active_legs(context.trade_date, **self._ledger_scope)
        today_legs = self._today_exit_legs(active, context.trade_date)
        await asyncio.gather(
            self._recover_closed_exit_windows(context, today_legs, now),
            self._run_stale_exit_cycle(context, now),
        )

    @staticmethod
    def _today_exit_legs(
        active: Sequence[ActiveModelLeg],
        trade_date: date,
    ) -> list[ActiveModelLeg]:
        return [leg for leg in active if trade_date in {leg.d1, leg.d2}]

    @staticmethod
    def _stale_exit_page(
        active: Sequence[ActiveModelLeg],
        trade_date: date,
    ) -> list[ActiveModelLeg]:
        stale = [leg for leg in active if leg.d2 < trade_date]
        stale.sort(key=lambda leg: (leg.d2, leg.signal_date, leg.rank, leg.model_leg_id))
        return stale[:MAX_STALE_EXIT_LEGS_PER_TICK]

    async def _evaluate_active_exits(
        self,
        active: Sequence[ActiveModelLeg],
        now: datetime,
        calendar: Sequence[date] = (),
        *,
        deadline: float | None = None,
        symbol_timeout_seconds: float = STALE_EXIT_RULE_SYMBOL_TIMEOUT_SECONDS,
    ) -> V20LiveExitStageTimeout | None:
        """Evaluate each leg in an independently cancellable timeout boundary.

        A stuck rule for one symbol must not serialize or cancel its healthy
        siblings.  Timeout children are explicitly cancelled and drained; a
        structured timeout is returned to the tick only after every sibling has
        had its chance to commit.  Ordinary leg failures settle their stable
        symbol alert and do not fail the batch when that alert is durable.
        """

        if not active:
            return None
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        symbol_deadline = started_at + max(0.0, symbol_timeout_seconds)
        if deadline is not None:
            tick_remaining = max(0.0, deadline - started_at)
            reserve = min(
                LIVE_EXIT_RULE_DRAIN_RESERVE_SECONDS,
                tick_remaining * 0.25,
            )
            reserve = max(LIVE_EXIT_MIN_DEADLINE_RESERVE_SECONDS, reserve)
            symbol_deadline = min(
                symbol_deadline,
                max(started_at, deadline - reserve),
            )
        symbol_deadline = max(started_at, symbol_deadline)

        if symbol_deadline <= started_at:
            timed_out_at = started_at
            return V20LiveExitStageTimeout(
                stage="rules_symbol",
                elapsed_seconds=0.0,
                remaining_seconds=(
                    max(0.0, deadline - timed_out_at) if deadline is not None else 0.0
                ),
                deadline=symbol_deadline,
                symbols=tuple(sorted({record.code for record in active})),
                provider="rules",
            )

        async def evaluate_one(
            record: ActiveModelLeg,
        ) -> tuple[ActiveModelLeg, BaseException | None]:
            child = asyncio.create_task(
                self._evaluate_one_exit(
                    record,
                    now,
                    detection_is_trading_day=(
                        now.date() in calendar or now.date() in {record.d1, record.d2}
                    ),
                    detection_calendar_status=(
                        "CONFIRMED_TRADING"
                        if now.date() in calendar or now.date() in {record.d1, record.d2}
                        else "CONFIRMED_NON_TRADING"
                        if calendar
                        else "UNKNOWN"
                    ),
                    next_trade_date=next(
                        (item for item in calendar if item > now.date()),
                        None,
                    ),
                    calendar=calendar,
                ),
                name=f"v20-exit-leg-{record.model_leg_id}",
            )
            timeout_scope = asyncio.timeout_at(symbol_deadline)
            try:
                async with timeout_scope:
                    await asyncio.shield(child)
                return record, None
            except TimeoutError as exc:
                if timeout_scope.expired():
                    timed_out_at = loop.time()
                    return record, V20LiveExitStageTimeout(
                        stage="rules_symbol",
                        elapsed_seconds=max(0.0, timed_out_at - started_at),
                        remaining_seconds=(
                            max(0.0, deadline - timed_out_at) if deadline is not None else 0.0
                        ),
                        deadline=symbol_deadline,
                        symbols=(record.code,),
                        provider="rules",
                    )
                return record, exc
            except asyncio.CancelledError:
                current_task = asyncio.current_task()
                if current_task is not None and current_task.cancelling():
                    raise
                return record, RuntimeError("exit leg evaluation task was cancelled")
            except Exception as exc:
                return record, exc
            finally:
                if not child.done():
                    child.cancel()
                await asyncio.gather(child, return_exceptions=True)

        tasks = [
            asyncio.create_task(
                evaluate_one(record),
                name=f"v20-exit-leg-boundary-{record.model_leg_id}",
            )
            for record in active
        ]
        try:
            results = await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        timeout_codes: set[str] = set()
        undurable_failures: list[str] = []
        for record, failure in results:
            if failure is None:
                continue
            if isinstance(failure, V20LiveExitStageTimeout):
                timeout_codes.add(record.code)
                continue
            diagnostic_alert_emitted = await self._safe_alert(
                code="EXIT_LEG_EVALUATION_FAILED",
                entity_id=record.model_leg_id,
                message=f"{record.code}: {type(failure).__name__}: {failure}",
                now=now,
            )
            if diagnostic_alert_emitted is not True:
                undurable_failures.append(f"{record.model_leg_id}:{type(failure).__name__}")

        if undurable_failures:
            raise V20LiveExitIncidentError(
                "one or more active exit legs could not be evaluated and their "
                "diagnostics were not durable: " + ",".join(undurable_failures[:20]),
                diagnostic_alert_emitted=False,
            )
        if timeout_codes:
            timed_out_at = loop.time()
            return V20LiveExitStageTimeout(
                stage="rules_symbol",
                elapsed_seconds=max(0.0, timed_out_at - started_at),
                remaining_seconds=(
                    max(0.0, deadline - timed_out_at) if deadline is not None else 0.0
                ),
                deadline=symbol_deadline,
                symbols=tuple(sorted(timeout_codes)),
                provider="rules",
            )
        return None

    async def _recover_closed_exit_windows(
        self,
        context: _DayContext,
        active_legs: Sequence[ActiveModelLeg],
        now: datetime,
    ) -> None:
        """Recover every closed D1/D2 scan before choosing a late or plan exit."""

        due_targets = {
            (leg.code, closed_date)
            for leg in active_legs
            for closed_date in (leg.d1, leg.d2)
            if closed_date < context.trade_date
            and (leg.code, closed_date) not in context.exit_history_completed
            and (
                (leg.code, closed_date) not in context.exit_history_last_attempt
                or (
                    now - context.exit_history_last_attempt[(leg.code, closed_date)]
                ).total_seconds()
                >= CLOSED_EXIT_RECOVERY_RETRY_SECONDS
            )
        }
        targets = tuple(sorted(due_targets))[:MAX_CLOSED_EXIT_RECOVERY_TARGETS_PER_TICK]
        if not targets:
            return

        recovery_slots = asyncio.Semaphore(4)

        async def _fetch_one(
            code: str,
            trade_date: date,
        ) -> tuple[str, date, Mapping[str, Sequence[TushareMinuteBar]]]:
            async with recovery_slots:
                result = await asyncio.wait_for(
                    self._scan_state.realtime_client.batch_get_minute_history_for_date(
                        [code], trade_date
                    ),
                    timeout=CLOSED_EXIT_RECOVERY_TIMEOUT_SECONDS,
                )
            return code, trade_date, result

        results = await asyncio.gather(
            *(_fetch_one(code, trade_date) for code, trade_date in targets),
            return_exceptions=True,
        )
        for target, result in zip(targets, results, strict=True):
            code, trade_date = target
            terminal_key = (code, trade_date)
            if isinstance(result, BaseException):
                if isinstance(result, asyncio.CancelledError):
                    raise result
                context.exit_history_last_attempt[terminal_key] = now
                await self._safe_alert(
                    code="D1_EXIT_HISTORY_RECOVERY_FAILED",
                    entity_id=f"{code}:{trade_date.isoformat()}",
                    message=f"{type(result).__name__}: {result}",
                    now=now,
                )
                continue
            _code, _trade_date, history = result
            context.exit_history_last_attempt[terminal_key] = now
            await self._persist_history(context, history, observed_at=now)
            rows = tuple(history.get(code, ()))
            is_d1 = any(leg.code == code and leg.d1 == trade_date for leg in active_legs)
            scanned_through = "14:57" if is_d1 else "14:56"
            complete, scan_hash = _complete_history_evidence(
                code,
                trade_date,
                rows,
                scanned_through_label=scanned_through,
                profile="TUSHARE_STK_MINS_CLOSED_DAY_SCAN_V2",
            )
            if not complete:
                await self._safe_alert(
                    code="CLOSED_EXIT_HISTORY_INCOMPLETE",
                    entity_id=f"{code}:{trade_date.isoformat()}",
                    message=(
                        f"{code} {trade_date.isoformat()} 历史分钟窗口不完整；"
                        "不推进扫描水位，不虚构保护性触发，D2 14:57计划退出仍保留"
                    ),
                    now=now,
                )
                continue
            for leg in active_legs:
                if leg.code == code and trade_date in {leg.d1, leg.d2}:
                    await self._repository.record_exit_scan_watermark(
                        leg.model_leg_id,
                        trade_date=trade_date,
                        scanned_through_label=scanned_through,
                        source_hash=scan_hash,
                        **self._ledger_scope,
                    )
            context.exit_history_completed.add((code, trade_date))

    async def _evaluate_one_exit(
        self,
        record: ActiveModelLeg,
        now: datetime,
        *,
        detection_is_trading_day: bool = False,
        detection_calendar_status: str = "UNKNOWN",
        next_trade_date: date | None = None,
        calendar: Sequence[date] = (),
    ) -> None:
        if detection_calendar_status not in {
            "UNKNOWN",
            "CONFIRMED_TRADING",
            "CONFIRMED_NON_TRADING",
        }:
            raise ValueError("invalid exit detection calendar status")
        if detection_calendar_status == "CONFIRMED_TRADING" and not detection_is_trading_day:
            raise ValueError("confirmed trading status requires a trading-day flag")
        if detection_calendar_status == "CONFIRMED_NON_TRADING" and detection_is_trading_day:
            raise ValueError("confirmed non-trading status conflicts with trading-day flag")
        plan_due = now >= _local(record.d2, self.config.clock.plan_exit)
        auxiliary_reasons: list[str] = []
        try:
            reference_status = ReferenceStatus(record.reference_status)
        except ValueError:
            if not plan_due:
                raise
            reference_status = ReferenceStatus.UNAVAILABLE
            auxiliary_reasons.append("REFERENCE_STATE_INVALID")
        leg = ModelLeg(
            model_leg_id=record.model_leg_id,
            model_batch_id=record.model_batch_id,
            code=record.code,
            d0=record.signal_date,
            d1=record.d1,
            d2=record.d2,
            origin_final_relative_weight=record.relative_weight,
            evaluation_only=record.evaluation_only,
            reference_status=reference_status,
            reference_entry_price=record.reference_price,
        )
        selected: SelectedMewsRecord | None = None
        if now.date() >= record.d2 and record.exit_intent_id is None:
            try:
                cutoff = _local(record.d2, self.config.clock.mews_cutoff_d1)
                late_source_trade_date = record.d1
                if self._mews_guard_store is not None:
                    selected = await self._mews_guard_store.load_frozen_for_leg(
                        record.model_leg_id,
                        d1=record.d1,
                        cutoff=cutoff,
                        late_source_trade_date=late_source_trade_date,
                        evaluation_date=record.d2,
                    )
                    if selected is None:
                        existing_snapshot_id = await self._mews_guard_store.find_eligible_snapshot(
                            source_trade_date=late_source_trade_date,
                            cutoff=cutoff,
                            availability_date=record.d2,
                        )
                        if existing_snapshot_id is None:
                            try:
                                await self._ensure_mews_for_exit_date(
                                    record.d2,
                                    source_trade_date=record.d1,
                                    now=now,
                                )
                                existing_snapshot_id = (
                                    await self._mews_guard_store.find_eligible_snapshot(
                                        source_trade_date=late_source_trade_date,
                                        cutoff=cutoff,
                                        availability_date=record.d2,
                                    )
                                )
                            except asyncio.CancelledError:
                                raise
                            except Exception:
                                pass
                        selected = await self._mews_guard_store.select_freeze_and_load(
                            record.model_leg_id,
                            d1=record.d1,
                            cutoff=cutoff,
                            late_source_trade_date=late_source_trade_date,
                            late_availability_date=record.d2,
                            evaluation_date=record.d2,
                        )
                else:
                    # Compatibility-only repositories.  A connected production
                    # PostgreSQL service always takes the atomic strict-store
                    # transaction above and never splits selection from load.
                    await self._repository.select_mews_for_leg(
                        record.model_leg_id,
                        d1=record.d1,
                        cutoff=cutoff,
                        late_source_trade_date=late_source_trade_date,
                        late_availability_date=record.d2,
                    )
                    selected = await self._repository.load_selected_mews_for_leg(
                        record.model_leg_id
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                auxiliary_reasons.append("MEWS_INPUT_UNAVAILABLE")
                if self._mews_guard_store is None:
                    await self._safe_alert(
                        code="MEWS_INPUT_UNAVAILABLE",
                        entity_id=record.model_leg_id,
                        message=f"{record.code}: {type(exc).__name__}: {exc}",
                        now=now,
                    )

        stored: list[Any] = []
        try:
            stored = await self._load_exit_bar_records(record, now)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            auxiliary_reasons.append("EXIT_BAR_INPUT_UNAVAILABLE")
            await self._safe_alert(
                code="EXIT_BAR_INPUT_UNAVAILABLE",
                entity_id=record.model_leg_id,
                message=f"{record.code}: {type(exc).__name__}: {exc}",
                now=now,
            )
        bars: list[MinuteBar] = []
        malformed_bar_count = 0
        for item in stored:
            try:
                bars.append(_minute_from_record(item.payload))
            except (KeyError, TypeError, ValueError, OverflowError):
                malformed_bar_count += 1
        if malformed_bar_count:
            auxiliary_reasons.append("MALFORMED_MINUTE_PAYLOAD_IGNORED")
            await self._safe_alert(
                code="MALFORMED_MINUTE_PAYLOAD",
                entity_id=record.model_leg_id,
                message=f"{record.code}: ignored {malformed_bar_count} malformed persisted bars",
                now=now,
            )
        try:
            watermarks = await self._repository.get_exit_scan_watermarks(
                record.model_leg_id,
                **self._ledger_scope,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            watermarks = {}
            auxiliary_reasons.append("EXIT_WATERMARK_UNAVAILABLE")
            await self._safe_alert(
                code="EXIT_WATERMARK_UNAVAILABLE",
                entity_id=record.model_leg_id,
                message=f"{record.code}: {type(exc).__name__}: {exc}",
                now=now,
            )
        d1_complete = (
            _exit_window_complete(
                bars,
                record.d1,
                as_of=now,
            )
            or watermarks.get(record.d1, "") >= "14:57"
        )
        d2_complete = (
            _exit_window_complete(
                bars,
                record.d2,
                as_of=now,
            )
            or watermarks.get(record.d2, "") >= "14:56"
        )
        try:
            mews_snapshots = _mews_snapshot(selected)
        except (TypeError, ValueError):
            mews_snapshots = ()
            auxiliary_reasons.append("MEWS_PAYLOAD_INVALID")
        evaluation = evaluate_exit(
            leg=leg,
            # A missing/illegal minute remains diagnostic evidence but must
            # never disable a later independently valid protection trigger.
            bars=bars,
            as_of=now,
            mews_snapshots=mews_snapshots,
            d1_window_complete=d1_complete,
            d2_pre1457_window_complete=d2_complete,
        )
        if now.date() >= record.d2 and (selected is None or selected.snapshot_id is None):
            await self._safe_alert(
                code="MEWS_UNAVAILABLE_FALLBACK_12",
                entity_id=record.model_leg_id,
                message=f"{record.code} 的D2阈值使用常驻-12%，未取得合格MEWS快照",
                now=now,
            )
        intent = evaluation.intent
        if intent is None:
            if evaluation.suppressed_reason == "D1_WINDOW_INCOMPLETE":
                await self._safe_alert(
                    code="D1_EXIT_WINDOW_INCOMPLETE",
                    entity_id=record.model_leg_id,
                    message=(
                        f"{record.code} 的D1分钟窗口尚无法证明已完整扫描；"
                        "D2盘中保护暂不形成，D2 14:57计划退出不受影响"
                    ),
                    now=now,
                )
            return
        reasons = list(intent.reason_codes)
        reasons.extend(reason for reason in auxiliary_reasons if reason not in reasons)
        trigger_record = next(
            (
                item
                for item in stored
                if intent.trigger_bar_end_ts is not None
                and getattr(item, "bar_end", None) == intent.trigger_bar_end_ts
            ),
            None,
        )
        trigger_received_at = (
            getattr(trigger_record, "first_received_at", None)
            if trigger_record is not None
            else None
        )
        exit_event_id = event_id("EXIT_SIGNAL", intent.exit_intent_id)
        semantic: dict[str, Any] = {
            "schema_version": V20_EXIT_SEMANTIC_SCHEMA,
            "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
            "event_id": exit_event_id,
            "event_type": "EXIT_SIGNAL",
            "strategy_version": self.config.strategy_version,
            "config_hash": self.config.config_hash,
            "deployment_mode": self.config.deployment_mode,
            "model_batch_id": record.model_batch_id,
            "model_leg_id": record.model_leg_id,
            "origin_decision_id": record.decision_id,
            "origin_kind": record.origin_kind,
            "origin_source_event_id": record.source_event_id,
            "code": record.code,
            "stock_name": record.stock_name,
            "rank": record.rank,
            "signal_date": record.signal_date.isoformat(),
            "d1": record.d1.isoformat(),
            "d2": record.d2.isoformat(),
            "exit_signal_type": intent.signal_type.value,
            "exit_scope": "FULL_MODEL_LEG",
            "recommended_exit_fraction": 1.0,
            "origin_final_relative_weight": intent.origin_final_relative_weight,
            "target_model_leg_relative_weight": 0.0,
            "trigger_ts": intent.trigger_ts.isoformat(),
            "trigger_bar_end_ts": (
                intent.trigger_bar_end_ts.isoformat() if intent.trigger_bar_end_ts else None
            ),
            "exit_input_arbitration_profile_id": "FIRST_DURABLE_MINUTE_REVISION_V1",
            "trigger_bar_source_hash": (
                getattr(trigger_record, "source_hash", None) if trigger_record else None
            ),
            "trigger_bar_received_at": (
                trigger_received_at.isoformat() if trigger_received_at is not None else None
            ),
            "reference_entry_price": intent.reference_entry_price,
            "observed_close": (
                intent.trigger_wealth_factor * intent.reference_entry_price
                if intent.trigger_wealth_factor is not None
                and intent.reference_entry_price is not None
                else None
            ),
            "wealth_factor": intent.trigger_wealth_factor,
            "threshold_wealth_factor": intent.threshold_wealth_factor,
            "rule_actionable_from": intent.rule_actionable_from.isoformat(),
            "detection_trade_date": now.date().isoformat(),
            "delivery_priority_class": (
                "LIVE_EXIT" if now.date() in {record.d1, record.d2} else "STALE_RECOVERY_EXIT"
            ),
            "detection_is_trading_day": detection_is_trading_day,
            "detection_calendar_status": detection_calendar_status,
            "next_confirmed_trade_date": (
                next_trade_date.isoformat() if next_trade_date is not None else None
            ),
            "mews_snapshot_id": selected.snapshot_id if selected else None,
            "mews_source_trade_date": (
                selected.source_trade_date.isoformat()
                if selected and selected.source_trade_date
                else None
            ),
            "mews_fast_state": selected.fast_state if selected else None,
            "market_restriction": "UNKNOWN",
            "market_restriction_as_of_ts": now.isoformat(),
            "reason_codes": reasons,
            "ignored_invalid_bar_count": (
                evaluation.ignored_invalid_bar_count + malformed_bar_count
            ),
            "return_profile_id": self.config.return_profile_id,
            "reference_profile_id": self.config.reference_profile_id,
        }
        commit = ExitCommit(
            exit_intent_id=intent.exit_intent_id,
            event_id=exit_event_id,
            model_leg_id=record.model_leg_id,
            signal_type=intent.signal_type.value,
            trigger_ts=intent.trigger_ts,
            rule_actionable_from=intent.rule_actionable_from,
            semantic=semantic,
            semantic_content_hash=sha256_json(semantic),
            route_id=self.config.route_id,
            official_stream_id=self.config.official_stream_id,
            lineage_id=self.config.state_lineage_id,
        )
        await self._repository.commit_exit(commit)
        await self._repository.seal_event(exit_event_id, seal_v20_payload)

    async def _load_exit_bar_records(
        self,
        record: ActiveModelLeg,
        now: datetime,
    ) -> list[Any]:
        """Load an exit window under deterministic receipt-time arbitration.

        The normal repository path freezes the first durably received legal
        revision of each raw minute label. Corrupt candidates remain diagnostic
        evidence but do not erase a legal observation of the same label. A
        conflict cannot manufacture a protective trigger or suppress the
        unconditional frozen D2 14:57 plan.
        """

        try:
            return await self._repository.list_minute_bars(
                record.code,
                trade_dates=(record.d1, record.d2),
                end_cutoff=now,
            )
        except V20MinuteBarIntegrityConflict as exc:
            await self._safe_alert(
                code="SOURCE_FACT_CORRUPT",
                entity_id=record.model_leg_id,
                message=(
                    f"{record.code} has corrupt candidates at "
                    f"{len(exc.corrupt_labels)} minute label(s); corrupt candidates are ignored, "
                    "first durable legal candidates remain usable, and the D2 14:57 plan "
                    "remains active"
                ),
                now=now,
            )
            return list(exc.partial_records)
        except V20SemanticConflict:
            raw_records = []
            try:
                for trade_date in (record.d1, record.d2):
                    raw_records.extend(
                        await self._repository.list_raw_minute_bar_records(
                            (record.code,),
                            trade_date=trade_date,
                            end_labels=FULL_EXIT_LABELS,
                        )
                    )
            except V20SemanticConflict:
                await self._safe_alert(
                    code="SOURCE_FACT_CORRUPT",
                    entity_id=record.model_leg_id,
                    message=(
                        f"{record.code} 分钟行情哈希损坏；保护bar全部停用，D2 14:57计划退出仍保留"
                    ),
                    now=now,
                )
                return []
            grouped: dict[tuple[date, str], list[Any]] = {}
            for item in raw_records:
                if item.bar_end.astimezone(SHANGHAI) > now:
                    continue
                key = (item.bar_end.astimezone(SHANGHAI).date(), item.end_label)
                grouped.setdefault(key, []).append(item)
            conflicts = sorted(key for key, revisions in grouped.items() if len(revisions) != 1)
            selected = [
                revisions[0] for _key, revisions in sorted(grouped.items()) if len(revisions) == 1
            ]
            await self._safe_alert(
                code="SOURCE_FACT_CONFLICT",
                entity_id=record.model_leg_id,
                message=(
                    f"{record.code} 分钟行情存在 {len(conflicts)} 个冲突标签；"
                    "冲突bar不触发止损，D2 14:57计划退出仍保留"
                ),
                now=now,
            )
            return selected

    async def _run_reminders(self, context: _DayContext, now: datetime) -> None:
        if context.reminders_done:
            return
        wall = now.timetz().replace(tzinfo=None)
        if wall < self.config.clock.reminder_check:
            return
        cutoff = _local(context.trade_date, self.config.clock.reminder_check)
        event_ids = await self._repository.enqueue_due_exit_reminders(
            context.trade_date,
            cutoff=cutoff,
            route_id=self.config.route_id,
            **self._ledger_scope,
        )
        for reminder_event_id in event_ids:
            await self._repository.seal_event(reminder_event_id, seal_v20_payload)
        context.reminders_done = True

    async def _scheduled_exits_today(self, trade_date: date) -> list[dict[str, Any]]:
        active = await self._repository.list_active_legs(trade_date, **self._ledger_scope)
        return [
            {
                "model_leg_id": leg.model_leg_id,
                "code": leg.code,
                "stock_name": leg.stock_name,
                "rank": leg.rank,
                "signal_date": leg.signal_date.isoformat(),
                "relative_weight": leg.relative_weight,
                "plan_time": self.config.clock.plan_exit.strftime("%H:%M"),
            }
            for leg in active
            if leg.d2 == trade_date
        ]

    async def _poll_latest(
        self,
        context: _DayContext,
        codes: Sequence[str],
        *,
        observed_at: datetime | None = None,
        deadline: float | None = None,
        tick_started_at: float | None = None,
    ) -> Mapping[str, TushareMinuteBar]:
        if deadline is None or tick_started_at is None:
            loop = asyncio.get_running_loop()
            tick_started_at = loop.time()
            deadline = tick_started_at + LIVE_EXIT_MAX_TICK_SECONDS
        rows = await self._run_live_exit_stage(
            lambda: self._scan_state.realtime_client.batch_get_latest_minute_bars(list(codes)),
            stage="latest",
            stage_cap=LATEST_MINUTE_POLL_TIMEOUT_SECONDS,
            deadline=deadline,
            tick_started_at=tick_started_at,
            symbols=tuple(codes),
            provider="tushare_rt",
        )
        observation = self._aware_now(observed_at)
        complete = {
            code: bar
            for code, bar in rows.items()
            if bar.bar_end.astimezone(SHANGHAI) < observation
        }
        if complete:
            payloads = {code: _bar_payload(bar) for code, bar in complete.items()}
            sealed_hashes = await self._run_live_exit_stage(
                lambda: self._repository.record_minute_bars(list(payloads.values())),
                stage="db_persist_latest",
                stage_cap=LIVE_EXIT_DB_STAGE_TIMEOUT_SECONDS,
                deadline=deadline,
                tick_started_at=tick_started_at,
                symbols=tuple(complete),
                provider="postgres",
            )
            complete = {
                code: bar
                for code, bar in complete.items()
                if sha256_json(payloads[code]) in sealed_hashes
            }
            for bar in complete.values():
                self._remember_bar(context, bar)
        return complete

    async def _persist_history(
        self,
        context: _DayContext,
        history: Mapping[str, Sequence[TushareMinuteBar]],
        *,
        observed_at: datetime | None = None,
        deadline: float | None = None,
        tick_started_at: float | None = None,
    ) -> tuple[TushareMinuteBar, ...]:
        if deadline is None or tick_started_at is None:
            loop = asyncio.get_running_loop()
            tick_started_at = loop.time()
            deadline = tick_started_at + LIVE_EXIT_MAX_TICK_SECONDS
        observation = self._aware_now(observed_at)
        bars = [
            bar
            for rows in history.values()
            for bar in rows
            if bar.bar_end.astimezone(SHANGHAI) < observation
        ]
        persisted: list[TushareMinuteBar] = []
        if bars:
            payloads = [(_bar_payload(bar), bar) for bar in bars]
            sealed_hashes = await self._run_live_exit_stage(
                lambda: self._repository.record_minute_bars(
                    [payload for payload, _bar in payloads]
                ),
                stage="db_persist_history",
                stage_cap=LIVE_EXIT_DB_STAGE_TIMEOUT_SECONDS,
                deadline=deadline,
                tick_started_at=tick_started_at,
                symbols=tuple(sorted({bar.stock_code for bar in bars})),
                provider="postgres",
            )
            for payload, bar in payloads:
                if sha256_json(payload) not in sealed_hashes:
                    continue
                self._remember_bar(context, bar)
                persisted.append(bar)
        return tuple(persisted)

    @staticmethod
    def _remember_bar(context: _DayContext, bar: TushareMinuteBar) -> None:
        key = (
            bar.bar_end.astimezone(SHANGHAI).date(),
            bar.stock_code,
            bar.end_label,
        )
        previous = context.minute_rows.get(key)
        if previous is not None and previous != bar:
            context.minute_conflicts.add(key)
            return
        context.minute_rows[key] = bar

    async def _safe_alert(
        self,
        *,
        code: str,
        entity_id: str,
        message: str,
        now: datetime,
        event_id: str | None = None,
        semantic_extras: Mapping[str, Any] | None = None,
    ) -> bool:
        if not self._repository_started:
            return False
        semantic: dict[str, Any] = {
            "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
            "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
            "strategy_version": self.config.strategy_version,
            "config_hash": self.config.config_hash,
            "deployment_mode": self.config.deployment_mode,
            "official_stream_id": self.config.official_stream_id,
            "state_lineage_id": self.config.state_lineage_id,
            "alert_code": code,
            "delivery_priority_class": "RUNTIME_CRITICAL_ALERT",
            "entity_id": entity_id,
            "event_trade_date": now.date().isoformat(),
            "reason": message,
            "message": message,
        }
        if semantic_extras is not None:
            semantic.update(semantic_extras)
        semantic_hash = sha256_json(semantic)
        alert_id = event_id or named_hash(
            "V20_DATA_ALERT_EVENT_ID_V1",
            {
                "alert_code": code,
                "entity_id": entity_id,
                "event_trade_date": now.date().isoformat(),
                "semantic_hash": semantic_hash,
            },
        )

        async def persist_alert() -> bool:
            if event_id is not None:
                get_event = getattr(self._repository, "get_outbox_event", None)
                if get_event is not None:
                    existing = await get_event(
                        alert_id,
                        route_id=self.config.route_id,
                        official_stream_id=self.config.official_stream_id,
                        lineage_id=self.config.state_lineage_id,
                    )
                    existing_semantic = existing.semantic if existing is not None else {}
                    if existing is not None and (
                        existing_semantic.get("alert_code") != code
                        or existing_semantic.get("entity_id") != entity_id
                        or existing_semantic.get("event_trade_date") != now.date().isoformat()
                    ):
                        raise V20SemanticConflict("stable alert event has incompatible semantics")
                    if existing is not None and existing.payload is not None:
                        return True
            try:
                await self._repository.enqueue_alert(
                    alert_id,
                    self.config.route_id,
                    semantic,
                    semantic_hash,
                    **self._ledger_scope,
                )
            except V20SemanticConflict:
                if event_id is None:
                    raise
                get_event = getattr(self._repository, "get_outbox_event", None)
                if get_event is None:
                    raise
                existing = await get_event(
                    alert_id,
                    route_id=self.config.route_id,
                    official_stream_id=self.config.official_stream_id,
                    lineage_id=self.config.state_lineage_id,
                )
                existing_semantic = existing.semantic if existing is not None else {}
                if (
                    existing is None
                    or existing_semantic.get("alert_code") != code
                    or existing_semantic.get("entity_id") != entity_id
                    or existing_semantic.get("event_trade_date") != now.date().isoformat()
                ):
                    raise
            await self._repository.seal_event(alert_id, seal_v20_payload)
            return True

        persist_result = (await asyncio.gather(persist_alert(), return_exceptions=True))[0]
        if isinstance(persist_result, asyncio.CancelledError):
            raise persist_result
        if isinstance(persist_result, BaseException):
            detail = f"DATA_ALERT_PERSIST_FAILED:{code}:{type(persist_result).__name__}"
            self._record_lane_error("outbox_recovery", detail, now)
            logger.error(
                "V20 could not persist DATA_ALERT %s",
                code,
                exc_info=(
                    type(persist_result),
                    persist_result,
                    persist_result.__traceback__,
                ),
            )
            return False
        return True

    def _reference_code_sets(self, status: EntryStatus) -> tuple[tuple[str, ...], tuple[str, ...]]:
        comparison = status.snapshot.get("comparison_pool_codes") or []
        symbols = status.snapshot.get("symbols") or []
        comparison_codes = tuple(sorted(set(str(code) for code in comparison)))
        symbol_codes = tuple(
            sorted(set(str(item.get("code")) for item in symbols if isinstance(item, Mapping)))
        )
        normalized = (*comparison_codes, *symbol_codes)
        if any(len(code) != 6 or not code.isdigit() for code in normalized):
            raise ValueError("persisted V20 reference code set is invalid")
        return comparison_codes, symbol_codes

    def _verify_entry_binding(self, status: EntryStatus) -> None:
        semantic_state_hash = status.semantic.get("state_semantics_hash")
        snapshot_state_hash = status.snapshot.get("state_semantics_hash")
        if (
            status.official_stream_id != self.config.official_stream_id
            or status.lineage_id != self.config.state_lineage_id
            or status.config_id != status.config_hash[:24]
            or re.fullmatch(r"[0-9a-f]{64}", status.config_hash) is None
            or not isinstance(status.strategy_version, str)
            or not status.strategy_version
            or status.semantic.get("strategy_version") != status.strategy_version
            or status.semantic.get("config_hash") != status.config_hash
            or not isinstance(semantic_state_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", semantic_state_hash) is None
            or semantic_state_hash != snapshot_state_hash
            or status.slot_status not in {"COMPLETED", "FAILED"}
        ):
            raise V20ConfigError("today's terminal V20 slot belongs to another config/lineage")
        if (
            status.semantic.get("schema_version") != V20_ENTRY_SEMANTIC_SCHEMA
            or status.semantic.get("feishu_formatter_profile") != V20_FEISHU_FORMATTER_PROFILE
            or status.semantic.get("action") != status.action
            or sha256_json(dict(status.semantic)) != status.semantic_content_hash
            or sha256_json(dict(status.snapshot)) != status.snapshot_hash
        ):
            raise V20ConfigError("persisted V20 entry semantic contract is incompatible")
        if status.action == "INPUT_INVALID":
            compatible_snapshot = (
                status.snapshot.get("schema_version") == V20_INVALID_INPUT_SNAPSHOT_SCHEMA
            )
        else:
            compatible_snapshot = (
                status.snapshot.get("schema_version") == V20_DECISION_INPUT_SNAPSHOT_SCHEMA
                and status.snapshot.get("v16_snapshot_schema_version") == V20_V16_SNAPSHOT_SCHEMA
            )
        if not compatible_snapshot:
            raise V20ConfigError("persisted V20 entry snapshot contract is incompatible")

    @staticmethod
    def _entry_status_readonly_fingerprint(status: EntryStatus | None) -> tuple[Any, ...] | None:
        """Freeze every terminal field used by a check-only operator probe."""

        if status is None:
            return None
        expiry = status.action_expiry_ts
        semantic_hash = sha256_json(dict(status.semantic))
        snapshot_hash = sha256_json(dict(status.snapshot))
        if semantic_hash != status.semantic_content_hash or snapshot_hash != status.snapshot_hash:
            raise V20SemanticConflict("official entry status content hash differs")
        return (
            status.official_stream_id,
            status.trade_date,
            status.slot_id,
            status.slot_status,
            status.slot_revision,
            status.strategy_version,
            status.config_id,
            status.config_hash,
            status.lineage_id,
            status.decision_id,
            status.event_id,
            status.action,
            status.final_multiplier,
            status.semantic_content_hash,
            semantic_hash,
            status.snapshot_id,
            status.snapshot_hash,
            snapshot_hash,
            expiry.isoformat() if expiry is not None else None,
        )

    def _aware_now(self, supplied: datetime | None = None) -> datetime:
        value = supplied if supplied is not None else self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("V20 service clock must be timezone-aware")
        return value.astimezone(SHANGHAI)

    def _require_running(self) -> None:
        if not self.config.enabled or not self._repository_started:
            raise V20RepositoryError("V20 service is not running")

    async def _require_manual_trigger_ready(self) -> None:
        """Require the entry-trigger readiness profile, not global health."""

        self._require_running()
        task_names = tuple(task.get_name() for task in self._tasks)
        if (
            not self._started
            or self._stop_event.is_set()
            or len(task_names) != len(V20_RUNTIME_TASK_NAMES)
            or frozenset(task_names) != V20_RUNTIME_TASK_NAMES
            or any(task.done() for task in self._tasks)
        ):
            raise V20RepositoryError("V20 runtime is not healthy enough for manual trigger")
        runtime_status = await self.status()
        decision_lane = runtime_status.get("runtime_lanes", {}).get("decision", {})
        if decision_lane.get("last_error") is not None:
            raise V20RepositoryError("V20 decision lane is unavailable for manual trigger")
        if runtime_status.get("status_snapshot", {}).get("stale") is not False:
            raise V20RepositoryError("V20 database status evidence is unavailable")

    @property
    def _ledger_scope(self) -> dict[str, str]:
        return {
            "official_stream_id": self.config.official_stream_id,
            "lineage_id": self.config.state_lineage_id,
        }

    def _record_error(self, detail: str) -> None:
        self._last_error = detail
        try:
            self._last_error_at = self._aware_now()
        except Exception:
            self._last_error_at = None

    def _refresh_aggregate_lane_health(self) -> None:
        active_errors = [
            (lane.last_error_at, lane.last_error)
            for lane in self._lane_health.values()
            if lane.last_error is not None and lane.last_error_at is not None
        ]
        if active_errors:
            error_at, detail = max(active_errors, key=lambda item: item[0])
            self._last_error_at = error_at
            self._last_error = detail
        else:
            self._last_error = None
            self._last_error_at = None
        successes = [
            lane.last_success_at
            for lane in self._lane_health.values()
            if lane.last_success_at is not None
        ]
        self._last_success_at = max(successes) if successes else None

    def _record_lane_success(self, lane_name: str, at: datetime) -> None:
        lane = self._lane_health[lane_name]
        lane.last_success_at = self._aware_now(at)
        lane.last_error = None
        lane.last_error_at = None
        self._refresh_aggregate_lane_health()

    def _record_lane_error(self, lane_name: str, detail: str, at: datetime) -> None:
        lane = self._lane_health[lane_name]
        lane.last_error = detail
        lane.last_error_at = self._aware_now(at)
        lane.error_revision += 1
        self._refresh_aggregate_lane_health()

    def _scheduler_interval(self, now: datetime) -> float:
        wall = now.timetz().replace(tzinfo=None)
        if time(9, 30) <= wall <= time(9, 46):
            return 1.0
        if time(9, 31) <= wall <= time(11, 30) or time(13, 1) <= wall <= time(14, 57):
            return float(min(self.config.market.exit_poll_seconds, 15))
        return 30.0


__all__ = ["V20Service"]
