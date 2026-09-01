"""Production orchestration for V20 decisions and Feishu notifications.

The service owns a model ledger, not a brokerage account.  It never reads
positions and never creates orders or fills.  Its only externally visible
effects are immutable decision evidence and durable Feishu outbox events.
"""

from __future__ import annotations

import asyncio
import hashlib
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
from src.data.clients.tushare_realtime import TushareDailyBar, TushareMinuteBar
from src.data.clients.v20_market_data import (
    V20EarlyBarCollector,
    exact_reference_prices,
)
from src.data.database.v20_repository import (
    ActiveModelLeg,
    CompatibleEntryBinding,
    EntryStatus,
    ExitCommit,
    ManualMonitorEnrollmentCommit,
    ManualMonitorEnrollmentRecord,
    ModelBatchWrite,
    ModelLegWrite,
    OutboxRecord,
    SelectedMewsRecord,
    ShadowBatchRecord,
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
    genesis_state,
    prepare_entry,
    prepare_invalid_entry,
)
from src.strategy.v20.exit_policy import (
    derive_model_leg_id,
    evaluate_exit,
    is_valid_complete_minute_bar,
)
from src.strategy.v20.identity import event_id, named_hash
from src.strategy.v20.models import (
    V20_DATA_ALERT_SEMANTIC_SCHEMA,
    V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
    V20_ENTRY_SEMANTIC_SCHEMA,
    V20_EXIT_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
    V20_INVALID_INPUT_SNAPSHOT_SCHEMA,
    V20_V16_SNAPSHOT_SCHEMA,
    HealthStatus,
    MewsSnapshot,
    MinuteBar,
    ModelLeg,
    ReferenceStatus,
    deserialize_health_snapshot,
)
from src.strategy.v20.runtime_config import (
    V20ConfigError,
    V20RouteBinding,
    V20RuntimeConfig,
    is_audited_legacy_state_semantics_hash,
    load_v20_runtime_config,
    validate_v20_api_keys,
    validate_v20_database_consumers,
    validated_v20_tushare_token,
)
from src.strategy.v20.shadow_evaluator import evaluate_shadow_batch
from src.web.v15_scan_service import V15ScanState
from src.web.v20_scan_pipeline import V20PrewarmedScan, V20ScanPipeline

logger = logging.getLogger(__name__)
SHANGHAI = ZoneInfo("Asia/Shanghai")
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
MAX_CLOSED_EXIT_RECOVERY_TARGETS_PER_TICK = 4
CLOSED_EXIT_RECOVERY_TIMEOUT_SECONDS = 3.0
CLOSED_EXIT_RECOVERY_RETRY_SECONDS = 30.0
MAX_STALE_EXIT_LEGS_PER_TICK = 20
OUTBOX_RECOVERY_TICK_TIMEOUT_SECONDS = 3.0
LIVE_EXIT_MAX_TICK_SECONDS = 12.0
LATEST_MINUTE_POLL_TIMEOUT_SECONDS = 8.0
LIVE_EXIT_MORNING_CLOSE_PUBLICATION_GRACE_SECONDS = 60.0
STALE_EXIT_TICK_SECONDS = 30.0
STALE_EXIT_TICK_TIMEOUT_SECONDS = 3.0
OUTBOX_RECOVERY_TICK_SECONDS = 2.0
OUTBOX_RECOVERY_LANE_TIMEOUT_SECONDS = 1.5
STATUS_SNAPSHOT_MAX_AGE_SECONDS = OUTBOX_RECOVERY_TICK_SECONDS * 3.0 + 1.0
MANUAL_TRIGGER_DECISION_LOCK_TIMEOUT_SECONDS = 15.0
LATE_0939_REPLAY_TOTAL_TIMEOUT_SECONDS = 180.0
LATE_0939_REPLAY_RETRY_SECONDS = 900.0
LATE_0939_REPLAY_MAX_AUTOMATIC_ATTEMPTS = 2
MANUAL_MONITOR_HISTORY_TIMEOUT_SECONDS = 30.0
V20_RUNTIME_LANE_COUNT = 6
MEWS_CACHE_CUTOFF = time(9, 40)
MEWS_CACHE_POLL_SECONDS = 30.0
_MANUAL_REQUEST_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}")

Clock = Callable[[], datetime]
ResourceInitializer = Callable[[V15ScanState], Awaitable[None]]
ResourceCleanup = Callable[[V15ScanState], Awaitable[None]]
CalendarProvider = Callable[[], Awaitable[list[date]]]


async def _init_v20_scan_resources(scan_state: V15ScanState) -> None:
    """Initialize the V16 scanner inputs without legacy scheduler side effects."""
    await _init_v20_scan_resources_with_token(
        scan_state,
        validated_v20_tushare_token(),
        manage_fundamentals=True,
    )


async def _init_embedded_v20_scan_resources(scan_state: V15ScanState) -> None:
    """Initialize embedded V20 while borrowing main's connected DB pool."""
    from src.common.config import get_tushare_token

    await _init_v20_scan_resources_with_token(
        scan_state,
        get_tushare_token(),
        manage_fundamentals=False,
    )


async def _init_owned_embedded_v20_scan_resources(scan_state: V15ScanState) -> None:
    """Initialize embedded V20 when main has no shared fundamentals pool."""
    from src.common.config import get_tushare_token

    await _init_v20_scan_resources_with_token(
        scan_state,
        get_tushare_token(),
        manage_fundamentals=True,
    )


async def _init_v20_scan_resources_with_token(
    scan_state: V15ScanState,
    token: str,
    *,
    manage_fundamentals: bool,
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
        if manage_fundamentals:
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
        scan_state.v15_scan_db = None
        scan_state.historical_adapter = historical_adapter
        scan_state.concept_mapper = concept_mapper
        scan_state.stock_filter = stock_filter
        scan_state.initialized = True
    except BaseException as initialization_error:
        cleanup_labels: list[str] = ["Tushare"]
        cleanup_operations: list[Awaitable[None]] = [tushare.stop()]
        if manage_fundamentals:
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


async def _cleanup_v20_scan_resources(scan_state: V15ScanState) -> None:
    """Close only resources owned by V20; never touch legacy global workers."""
    try:
        if scan_state.realtime_client is not None:
            await scan_state.realtime_client.stop()
    finally:
        try:
            if scan_state.fundamentals_db is not None:
                await scan_state.fundamentals_db.close()
        finally:
            scan_state.initialized = False


async def _cleanup_embedded_v20_scan_resources(scan_state: V15ScanState) -> None:
    """Close V20's market client while preserving main's shared DB pool."""
    try:
        if scan_state.realtime_client is not None:
            await scan_state.realtime_client.stop()
    finally:
        scan_state.initialized = False


@dataclass
class _DayContext:
    trade_date: date
    calendar: tuple[date, ...]
    entry_status: EntryStatus | None = None
    prewarmed: V20PrewarmedScan | None = None
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
    return (
        MewsSnapshot(
            source_trade_date=record.source_trade_date,
            generated_at=record.generated_at,
            received_at=record.received_at,
            fast_state=record.fast_state,
            model_version=record.model_version,
            data_version=record.data_version,
            snapshot_id=record.snapshot_id,
        ),
    )


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
    if checkpoint.get("schema_version") != "v20-bootstrap-checkpoint/v2":
        raise V20ConfigError("unsupported V20 checkpoint schema")
    if checkpoint.get("target_official_stream_id") != config.official_stream_id:
        raise V20ConfigError("V20 checkpoint target stream does not match active config")
    if checkpoint.get("state_lineage_id") != config.state_lineage_id:
        raise V20ConfigError("V20 checkpoint lineage does not match active config")
    source_state_semantics_hash = checkpoint.get("source_state_semantics_hash")
    resolved_state_semantics_hash = checkpoint.get("resolved_state_semantics_hash")
    if resolved_state_semantics_hash is None:
        resolved_state_semantics_hash = source_state_semantics_hash
    source_is_current = source_state_semantics_hash == resolved_state_semantics_hash
    source_is_resolved_legacy = (
        source_state_semantics_hash != resolved_state_semantics_hash
        and is_audited_legacy_state_semantics_hash(source_state_semantics_hash)
    )
    if (
        not source_is_current
        and not source_is_resolved_legacy
        or resolved_state_semantics_hash != config.state_semantics_hash
    ):
        raise V20ConfigError(
            "V20 checkpoint state semantics do not match the active strategy bytes/config"
        )
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
    valid_rolling_dates = {
        str(item.get("signal_date"))
        for item in shadow_batches
        if isinstance(item, Mapping)
        and item.get("kind") == "ROLLING7"
        and item.get("status") == "COMPLETE_VALID"
    }
    if len(valid_rolling_dates) < 7:
        raise V20ConfigError("production checkpoint requires seven completed rolling7 facts")
    return _BootstrapBundle(
        dict(state),
        predecessor_trade_date,
        tuple(dict(item) for item in shadow_batches),
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
        "用途: 截止后重拉并截取当日 raw 09:31..09:39，回答策略在该快照上会如何判断。",
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
        scan_state: V15ScanState,
        scan_pipeline: V20ScanPipeline,
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
        self._scan_pipeline = scan_pipeline
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
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[Any]] = []
        self._decision_cycle_lock = asyncio.Lock()
        self._manual_trigger_lock = asyncio.Lock()
        self._manual_monitor_lock = asyncio.Lock()
        self._late_0939_replay_lock = asyncio.Lock()
        self._late_0939_replay_task: asyncio.Task[Any] | None = None
        self._live_exit_lock = asyncio.Lock()
        self._mews_refresh_lock = asyncio.Lock()
        self._mews_cached_for: date | None = None
        self._mews_source_trade_date: date | None = None
        self._mews_snapshot_id: str | None = None
        self._mews_last_failure: str | None = None
        self._mews_alerted_for: date | None = None
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
        self._compatible_entry_bindings: set[tuple[str, str, str]] = {
            (
                self.config.config_hash[:24],
                self.config.config_hash,
                self.config.state_semantics_hash,
            )
        }
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
        fundamentals = None
        if config.enabled:
            token = validated_v20_tushare_token()
            fundamentals = create_fundamentals_db_from_config(
                database_config_path,
                tushare_token=token,
            )
            # Factories resolve YAML literals and ${ENV} expressions. Validate
            # the resulting objects—not merely the environment—before either
            # asyncpg consumer can open a socket.
            validate_v20_database_consumers(repository.config, fundamentals.config)
        scan_state = V15ScanState(fundamentals_db=fundamentals)
        pipeline = V20ScanPipeline(scan_state, project_root)
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
            scan_state=scan_state,
            scan_pipeline=pipeline,
            artifacts=artifacts,
            publisher=publisher,
            routes=routes,
            mews_source=mews_source,
        )

    @classmethod
    def from_legacy_runtime(cls, *, fundamentals_db: Any | None = None) -> V20Service:
        """Embed forward-shadow V20 in the existing main/V16 container.

        Strategy semantics, the ledger, outbox, 09:39 input boundary, and
        notification rules remain V20.  Only operational credentials and the
        final relay protocol are adapted to infrastructure already deployed by
        main.  The dedicated V20 host continues to use ``from_default_config``.
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
        owns_fundamentals = fundamentals_db is None
        fundamentals = fundamentals_db
        if fundamentals is None:
            fundamentals = create_fundamentals_db_from_config(
                database_config_path,
                tushare_token=token,
            )
        shared_ledger_pool = None
        if not owns_fundamentals:
            try:
                shared_ledger_pool = fundamentals.connection_pool
            except (AttributeError, RuntimeError) as exc:
                raise V20ConfigError(
                    "embedded V20 requires a connected shared fundamentals pool"
                ) from exc
        repository = create_embedded_v20_repository_from_config(
            database_config_path,
            shared_pool=shared_ledger_pool,
        )
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
        scan_state = V15ScanState(fundamentals_db=fundamentals)
        pipeline = V20ScanPipeline(scan_state, project_root)
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
            scan_state=scan_state,
            scan_pipeline=pipeline,
            artifacts=artifacts,
            publisher=publisher,
            routes=routes,
            mews_source=mews_source,
            initialize_resources=(
                _init_owned_embedded_v20_scan_resources
                if owns_fundamentals
                else _init_embedded_v20_scan_resources
            ),
            cleanup_resources=(
                _cleanup_v20_scan_resources
                if owns_fundamentals
                else _cleanup_embedded_v20_scan_resources
            ),
            embedded_legacy=True,
        )

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._stop_event.clear()
        if not self.config.enabled:
            self._startup_stage = "DISABLED"
            logger.info("V20 is disabled by configuration")
            return
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
                current_config_id=self.config.config_hash[:24],
                current_config_hash=self.config.config_hash,
                current_config_payload=self.config.frozen_payload,
                bootstrap_checkpoint_hash=self.config.bootstrap_checkpoint_sha256,
                bootstrap_predecessor_trade_date=bootstrap.predecessor_trade_date,
                bootstrap_shadow_batches=bootstrap.shadow_batches,
            )
            repository_bindings: Iterable[object] = getattr(
                self._repository,
                "compatible_entry_bindings",
                frozenset(),
            )
            for binding in repository_bindings:
                if not isinstance(binding, CompatibleEntryBinding):
                    raise V20ConfigError("repository returned an invalid config compatibility")
                self._compatible_entry_bindings.add(
                    (
                        binding.config_id,
                        binding.config_hash,
                        binding.state_semantics_hash,
                    )
                )
            self._startup_stage = "REFRESHING_STATUS"
            await self._refresh_status_snapshot()
            self._startup_stage = "INITIALIZING_MARKET_RESOURCES"
            await self._initialize_resources(self._scan_state)
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
            tasks, self._tasks = self._tasks, []
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            cleanup_labels: list[str] = []
            cleanup_operations: list[Awaitable[None]] = []
            if self._resources_started:
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

    async def stop(self) -> None:
        self._stop_event.set()
        tasks, self._tasks = self._tasks, []
        late_replay_task, self._late_0939_replay_task = self._late_0939_replay_task, None
        if late_replay_task is not None:
            tasks.append(late_replay_task)
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
            healthy = fresh and lane.last_error is None and durable_delivery_failures == 0
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
                or semantic.get("replay_action") != "ENTER"
                or semantic.get("v20_action") != "ENTER"
                or semantic.get("official_entry_action") != "INPUT_INVALID"
                or semantic.get("official_state_changed") is not False
                or semantic.get("orders_changed") is not False
                or semantic.get("non_actionable") is not True
                or semantic.get("retrospective_expired") is not True
                or semantic.get("visible_message_mode") != "AUTOMATIC_ENTRY_RENDER"
                or semantic.get("state_semantics_hash") != self.config.state_semantics_hash
                or semantic.get("strategy_version") != self.config.strategy_version
            ):
                raise V20SemanticConflict(
                    "manual monitor source is not a sealed compatible PASS/ENTER chain probe"
                )
            source_config_hash = semantic.get("config_hash")
            if not isinstance(source_config_hash, str) or len(source_config_hash) != 64:
                raise V20SemanticConflict("manual monitor source config hash is invalid")
            source_binding = (
                source_config_hash[:24],
                source_config_hash,
                self.config.state_semantics_hash,
            )
            if source_config_hash != self.config.config_hash and (
                source_binding not in self._compatible_entry_bindings
                and not await self._repository.is_registered_source_config_compatible(
                    source_config_hash,
                    strategy_version=self.config.strategy_version,
                    state_semantics_hash=self.config.state_semantics_hash,
                    official_stream_id=self.config.official_stream_id,
                    lineage_id=self.config.state_lineage_id,
                    route_id=self.config.route_id,
                )
            ):
                raise V20SemanticConflict(
                    "manual monitor source belongs to an unaudited historical config"
                )

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
            if (
                official.action != "INPUT_INVALID"
                or official.event_id != semantic.get("official_entry_event_id")
                or semantic.get("official_entry_event_id_before") != official.event_id
                or semantic.get("official_entry_event_id_after") != official.event_id
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
        if not self.config.clock.prewarm <= wall < self.config.clock.publish_deadline:
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

            if status_before is None:
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
                return {
                    "accepted": True,
                    "created": False,
                    "manual_request_id": request_id,
                    "trade_date": trade_date.isoformat(),
                    "cycle_result": (
                        context.last_phase if context is not None else "DECISION_PENDING"
                    ),
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
            # Raw market collection is independent of the previous state and
            # mature-shadow gates.  It must run from 09:15/09:31 even while a
            # D3 HEALTH label or a missed-slot predecessor is still pending.
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
            self._schedule_late_0939_replay(context, current)
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
        return max(1.0, min(LIVE_EXIT_MAX_TICK_SECONDS, cadence - 2.0))

    async def _run_live_exit_tick(self, context: _DayContext, now: datetime) -> None:
        """Run today's D1/D2 protection under a budget shorter than its cadence."""

        async def locked_cycle() -> None:
            async with self._live_exit_lock:
                await self._run_exit_cycle(context, now, include_stale=False)

        await asyncio.wait_for(locked_cycle(), timeout=self._live_exit_tick_budget())

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
                    timeout=min(cadence - 0.5, self._live_exit_tick_budget() + 1.0),
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
                        self._run_stale_exit_cycle(context, now),
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
        """Run the scheduled 09:10 MEWS attempt inside its retry window."""

        current = self._aware_now(now)
        wall = current.timetz().replace(tzinfo=None)
        if self._mews_cached_for == current.date():
            return False
        if current.date() not in calendar:
            return False
        if wall < MEWS_PUBLISH_TIME or wall >= MEWS_CACHE_CUTOFF:
            return False
        return await self._calculate_mews_once(
            current,
            calendar,
            require_cutoff_eligible=True,
        )

    async def _calculate_mews_once(
        self,
        now: datetime,
        calendar: Sequence[date],
        *,
        require_cutoff_eligible: bool,
    ) -> bool:
        """Calculate a missing daily value; time affects eligibility, never calculation."""

        current = self._aware_now(now)
        if self._mews_cached_for == current.date():
            return False
        if current.date() not in calendar:
            return False
        predecessors = [day for day in calendar if day < current.date()]
        if not predecessors:
            raise V20RepositoryError("MEWS calendar has no preceding trading day")
        source_trade_date = predecessors[-1]
        source = self._mews_source
        if source is None:
            raise V20ConfigError("V20 local MEWS calculator is not configured")

        async with self._mews_refresh_lock:
            if self._mews_cached_for == current.date():
                return False
            published = await source.fetch_snapshot(
                source_trade_date=source_trade_date,
                availability_date=current.date(),
            )
            payload = dict(published)
            generated_at = datetime.fromisoformat(str(payload.get("generated_at")))
            if generated_at.tzinfo is None or generated_at.utcoffset() is None:
                raise MewsSnapshotSourceError("calculated MEWS generated_at is timezone-naive")
            cutoff = _local(current.date(), MEWS_CACHE_CUTOFF)
            if require_cutoff_eligible and generated_at.astimezone(SHANGHAI) >= cutoff:
                raise MewsSnapshotSourceError("calculated MEWS missed the 09:40 cutoff")
            await self._repository.record_mews_snapshot(payload)
            eligible = await self._repository.mews_snapshot_is_eligible(
                str(payload["snapshot_id"]),
                source_trade_date=source_trade_date,
                cutoff=cutoff,
            )
            if require_cutoff_eligible and not eligible:
                raise MewsSnapshotSourceError(
                    "MEWS PostgreSQL receipt was not sealed before the 09:40 cutoff"
                )
            self._mews_cached_for = current.date()
            self._mews_source_trade_date = source_trade_date
            self._mews_snapshot_id = str(payload["snapshot_id"])
            self._mews_last_failure = None
            logger.info(
                "V20 cached locally calculated MEWS %s for %s (cutoff_eligible=%s)",
                self._mews_snapshot_id,
                current.date(),
                eligible,
            )
            return True

    async def ensure_mews_for_selection_trigger(
        self,
        now: datetime | None = None,
    ) -> bool:
        """Fill a missing daily MEWS value before handling a selection trigger."""

        self._require_running()
        await self._repository.assert_runtime_leader()
        current = self._aware_now(now)
        try:
            calendar = await self._load_trade_calendar(current.date())
            if current.date() not in calendar:
                self._record_lane_success("mews_cache", current)
                return False
            if self._mews_cached_for != current.date():
                await self._restore_mews_cache_once(current, calendar)
            calculated = await self._calculate_mews_once(
                current,
                calendar,
                require_cutoff_eligible=False,
            )
            self._record_lane_success("mews_cache", self._aware_now())
            return calculated
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._mews_last_failure = f"{type(exc).__name__}: {exc}"
            self._record_lane_error(
                "mews_cache",
                f"MEWS_CACHE_FAILED: {self._mews_last_failure}",
                self._aware_now(),
            )
            raise

    async def _restore_mews_cache_once(
        self,
        now: datetime,
        calendar: Sequence[date],
    ) -> bool:
        """Reattach to today's already-sealed cache after a process restart."""

        current = self._aware_now(now)
        if self._mews_cached_for == current.date() or current.date() not in calendar:
            return False
        predecessors = [day for day in calendar if day < current.date()]
        if not predecessors:
            raise V20RepositoryError("MEWS calendar has no preceding trading day")
        source_trade_date = predecessors[-1]
        snapshot_id = await self._repository.find_eligible_mews_snapshot(
            source_trade_date=source_trade_date,
            cutoff=_local(current.date(), MEWS_CACHE_CUTOFF),
        )
        if snapshot_id is None:
            return False
        self._mews_cached_for = current.date()
        self._mews_source_trade_date = source_trade_date
        self._mews_snapshot_id = snapshot_id
        self._mews_last_failure = None
        logger.info("V20 restored cached MEWS %s after restart", snapshot_id)
        return True

    async def _run_mews_cache_scheduler(self) -> None:
        """Pull after 09:10, retry until 09:40, then consume only PostgreSQL."""

        while not self._stop_event.is_set():
            now = self._aware_now()
            try:
                await self._repository.assert_runtime_leader()
                wall = now.timetz().replace(tzinfo=None)
                calendar: Sequence[date] = ()
                if wall >= MEWS_PUBLISH_TIME:
                    calendar = await self._load_trade_calendar(now.date())
                is_trading_day = now.date() in calendar
                if is_trading_day and self._mews_cached_for != now.date():
                    await self._restore_mews_cache_once(now, calendar)
                if is_trading_day and MEWS_PUBLISH_TIME <= wall < MEWS_CACHE_CUTOFF:
                    await self._refresh_mews_cache_once(now, calendar)
                elif (
                    is_trading_day
                    and wall >= MEWS_CACHE_CUTOFF
                    and self._mews_cached_for != now.date()
                ):
                    if self._mews_alerted_for != now.date():
                        self._mews_alerted_for = now.date()
                        await self._safe_alert(
                            code="MEWS_0910_CACHE_MISSED",
                            entity_id=now.date().isoformat(),
                            message=(
                                "09:10发布后的MEWS未在09:40前写入V20快照；"
                                "当日D2保护将按常驻-12%兜底"
                            ),
                            now=now,
                        )
                    raise MewsSnapshotSourceError("MEWS was not cached before today's 09:40 cutoff")
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
        """Preempt any decision phase that is still running at the 09:40 cutoff.

        ``asyncio.wait`` measures its timeout with the event loop's monotonic
        clock.  The wall clock is sampled again after the timeout, and the
        database remains authoritative for the actual terminal commit.  This
        prevents a 09:39 scheduler timestamp from remaining valid while a slow
        reconciliation or vendor request runs across 09:40.
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

        operation = asyncio.create_task(
            self.run_once(
                current,
                include_exit_cycles=False,
                include_outbox_recovery=False,
            ),
            name=f"v20-decision-before-cutoff-{current.date().isoformat()}",
        )
        remaining = max(0.0, (deadline - current).total_seconds())
        completed, _pending = await asyncio.wait({operation}, timeout=remaining)
        if operation in completed:
            await operation
            return

        operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)
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

        if self._known_entry_trade_date(trade_date):
            await self._enforce_entry_cutoff(trade_date, now=now)
            return True
        # A date absent from a successfully loaded calendar is a confirmed
        # exchange closure and must stay quiet.  If no calendar was obtained at
        # all, a weekday is conservatively treated as an unresolved session.
        calendar_known = self._calendar_loaded_for == trade_date
        if not calendar_known and trade_date.weekday() < 5:
            self._record_lane_error(
                "decision",
                "ENTRY_CALENDAR_UNKNOWN_AT_0940",
                now,
            )
            await self._safe_alert(
                code="ENTRY_CALENDAR_UNKNOWN_NO_BUY",
                entity_id=trade_date.isoformat(),
                message="09:40 仍无法确认交易日历；今天不买，不要追买。",
                now=now,
            )
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

        await self._safe_alert(
            code="ENTRY_CUTOFF_NO_BUY",
            entity_id=trade_date.isoformat(),
            message=("09:40 截止仍没有 durable 正常入场决定；今天不买，不要追买。"),
            now=current,
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
        if not pending:
            context.maturity_done = True
            return

        # Reference arbitration is owned by _expire_reference_gaps and must
        # happen before this method.  A PENDING row is therefore a real state
        # gap, not permission to discard possibly eligible persisted evidence.
        unresolved_health = any(
            item.kind == "HEALTH" and item.reference_status == "PENDING" for item in pending
        )
        pending = [item for item in pending if item.reference_status != "PENDING"]

        # A rolling gap is no longer recoverable once seven later complete
        # batches have displaced it.  Terminalize it before any daily-source
        # calls so old suspended/missing legs cannot accumulate as an
        # unbounded oldest-first retry backlog.
        completed_for_aging = await self._repository.load_recent_completed(
            "ROLLING7",
            before_t2=context.trade_date,
            limit=1_000,
            **self._ledger_scope,
        )
        rolling_complete_dates = [
            item.signal_date for item in completed_for_aging if item.status == "COMPLETE_VALID"
        ]
        retained: list[ShadowBatchRecord] = []
        daily_candidates: dict[tuple[date, datetime | None], list[Any]] = {}
        daily_corrupt_ids: dict[tuple[date, datetime | None], tuple[str, ...]] = {}
        for batch in pending:
            later_complete_n = sum(
                signal_date > batch.signal_date for signal_date in rolling_complete_dates
            )
            if batch.kind == "ROLLING7" and later_complete_n >= 7:
                await self._repository.complete_shadow_batch(
                    batch.batch_id,
                    batch_return=None,
                    status="COMPLETE_INVALID",
                    payload_update={
                        "evaluation_profile_id": "ZERO_COST_GROSS_PRICE_RETURN_V1",
                        "evaluation_status": "INVALID",
                        "invalid_reason": "ROLLING_GAP_AGED_OUT",
                        "later_complete_n": later_complete_n,
                    },
                    official_stream_id=self.config.official_stream_id,
                    lineage_id=self.config.state_lineage_id,
                )
                continue
            retained.append(batch)
        pending = retained

        # Poll each T+2 daily response at a bounded cadence and persist the raw
        # candidate before it can affect policy.  HEALTH later reads only the
        # candidate whose conservative post-commit receipt is no later than its
        # fixed D3 09:39 cutoff; ROLLING7 may legally accept a later completion.
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
                # HEALTH cutoff (or an earlier ROLLING7 candidate).
                await self._safe_alert(
                    code="DAILY_MATURITY_SOURCE_UNAVAILABLE",
                    entity_id=t2_date.isoformat(),
                    message=f"{type(exc).__name__}: {exc}",
                    now=now,
                )

        for batch in pending:
            health_cutoff: datetime | None = None
            if batch.kind == "HEALTH":
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
        # A rolling batch may intentionally remain PENDING/INCOMPLETE and is
        # represented as an active maturity gap in today's policy input.  That
        # is a completed maturity pass, not a reason to starve the 09:40 slot.
        context.maturity_done = not unresolved_health

    async def _policy_inputs(
        self, trade_date: date
    ) -> tuple[list[CompletedHealth], list[CompletedRolling], list[ActiveRollingGap]]:
        health_rows = await self._repository.load_recent_completed(
            "HEALTH", before_t2=trade_date, limit=1_000, **self._ledger_scope
        )
        rolling_rows = await self._repository.load_recent_completed(
            "ROLLING7", before_t2=trade_date, limit=1_000, **self._ledger_scope
        )
        pending_rows = await self._repository.list_pending_shadow_batches(
            trade_date, **self._ledger_scope
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
                batch_id=row.batch_id,
                signal_date=row.signal_date,
                t2_date=row.t2_date,
                batch_return=float(row.batch_return),
            )
            for row in rolling_rows
            if row.status == "COMPLETE_VALID" and row.batch_return is not None
        ]
        invalid_or_pending: list[ShadowBatchRecord] = [
            row for row in rolling_rows if row.status == "COMPLETE_INVALID"
        ]
        invalid_or_pending.extend(row for row in pending_rows if row.kind == "ROLLING7")
        gaps = [
            ActiveRollingGap(
                gap_id=row.batch_id,
                signal_date=row.signal_date,
                maturity_date=row.t2_date,
            )
            for row in invalid_or_pending
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
        if semantic.get("official_entry_action") != "INPUT_INVALID":
            raise V20SemanticConflict("late 09:39 replay is not bound to a failed live slot")
        if semantic.get("official_entry_event_id") != official_entry_event_id:
            raise V20SemanticConflict("late 09:39 replay official event binding is invalid")
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
    def _policy_inputs_from_failed_status(
        status: EntryStatus,
    ) -> tuple[list[CompletedHealth], list[CompletedRolling], list[ActiveRollingGap]]:
        raw = status.snapshot.get("policy_inputs")
        if not isinstance(raw, Mapping) or raw.get("schema_version") != (
            "v20-policy-input-snapshot/v1"
        ):
            raise V20SemanticConflict("failed slot lacks frozen V20 policy inputs")
        if set(raw) != {
            "schema_version",
            "completed_health",
            "completed_rolling",
            "maturity_gaps",
        } or any(
            not isinstance(raw.get(field), list)
            for field in ("completed_health", "completed_rolling", "maturity_gaps")
        ):
            raise V20SemanticConflict("failed slot policy input field set is malformed")
        frozen_policy_hash = status.snapshot.get("policy_input_hash")
        if (
            not isinstance(frozen_policy_hash, str)
            or sha256_json(raw) != frozen_policy_hash
            or frozen_policy_hash != status.semantic.get("policy_input_hash")
        ):
            raise V20SemanticConflict("failed slot policy input hash mismatch")
        if status.snapshot.get(
            "trade_date"
        ) != status.trade_date.isoformat() or status.snapshot.get(
            "state_before_hash"
        ) != status.semantic.get("state_before_hash"):
            raise V20SemanticConflict("failed slot snapshot/semantic binding mismatch")
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
            raise V20SemanticConflict("failed slot policy inputs are malformed") from exc
        return health, rolling, gaps

    async def _build_late_0939_replay_semantic(
        self,
        context: _DayContext,
        now: datetime,
        *,
        replay_event_id: str,
    ) -> Mapping[str, Any]:
        """Recompute the strategy in memory from a post-cutoff exact-09:39 pull.

        The official failed slot has already advanced the state ledger.  Health
        observations are watermark-idempotent, and the failed slot's own gap is
        not active on D0, but this remains a retrospective calculation.  The
        result deliberately exposes no synthetic decision/model identifiers.
        """

        status = context.entry_status
        if status is None:
            status = await self._repository.get_entry_status(
                self.config.official_stream_id,
                context.trade_date,
            )
        if status is None or status.action != "INPUT_INVALID":
            raise V20StateConflict("late 09:39 replay requires today's INPUT_INVALID slot")
        self._verify_entry_binding(status)
        if now < _local(context.trade_date, self.config.clock.publish_deadline):
            raise V20StateConflict("late 09:39 replay cannot run before the live cutoff")
        state = await self._repository.load_state(self.config.state_lineage_id)
        if state.state_hash != status.semantic.get("state_after_hash"):
            raise V20StateConflict("official state has moved beyond the failed replay slot")
        health_snapshot = deserialize_health_snapshot(state.payload["health"])

        prewarmed = context.prewarmed
        if prewarmed is None:
            prewarmed = await self._scan_pipeline.prewarm(
                context.trade_date,
                calendar=context.calendar,
            )
        if prewarmed.trade_date != context.trade_date:
            raise V20SemanticConflict("late replay prewarm belongs to another trade date")
        self._verify_prewarm_dependencies(prewarmed)

        breadth_required = health_snapshot.status not in {
            HealthStatus.WARMUP,
            HealthStatus.HEALTHY,
        }
        desired_codes = (
            prewarmed.required_minute_codes if breadth_required else prewarmed.universe_codes
        )
        required_labels = tuple(f"09:{minute:02d}" for minute in range(31, 40))

        async def load_persisted_early() -> tuple[
            list[Any], V20EarlyBarCollector, V20EarlyBarCollector
        ]:
            records = list(
                await self._repository.list_raw_minute_bar_records(
                    desired_codes,
                    trade_date=context.trade_date,
                    end_labels=required_labels,
                )
            )
            universe = V20EarlyBarCollector(
                context.trade_date,
                prewarmed.universe_codes,
            )
            breadth = V20EarlyBarCollector(
                context.trade_date,
                prewarmed.breadth_codes,
            )
            for record in records:
                bar = _tushare_minute_from_record(record.payload)
                if bar.end_label not in required_labels:
                    raise V20SemanticConflict("persisted late replay row escaped 09:31..09:39")
                universe.ingest((bar,))
                if breadth_required and bar.end_label == "09:39":
                    breadth.ingest((bar,))
            return records, universe, breadth

        records, universe_collector, breadth_collector = await load_persisted_early()
        missing_codes = set(universe_collector.incomplete_codes())
        if breadth_required:
            missing_codes.update(
                set(prewarmed.breadth_codes) - set(breadth_collector.codes_with_label("09:39"))
            )
        if missing_codes:
            client = self._scan_state.realtime_client
            if client is None or not hasattr(client, "batch_get_minute_history"):
                raise V20RepositoryError("late replay minute-history adapter is unavailable")
            histories = await client.batch_get_minute_history(sorted(missing_codes))
            filtered_history = {
                code: tuple(bar for bar in rows if bar.end_label in required_labels)
                for code, rows in histories.items()
            }
            await self._persist_history(
                context,
                filtered_history,
                observed_at=self._aware_now(),
            )
            # Form the replay only from durable rows.  Their database receipt
            # clock proves they arrived after cutoff instead of pretending the
            # afternoon vendor response existed at 09:40.
            records, universe_collector, breadth_collector = await load_persisted_early()

        early = universe_collector.freeze()
        breadth_early = breadth_collector.freeze_terminal()
        bundle = await self._scan_pipeline.scan(
            prewarmed,
            early,
            breadth_early=breadth_early,
            minimum_quote_coverage=self.config.market.minimum_quote_coverage,
        )

        completed_health, completed_rolling, maturity_gaps = self._policy_inputs_from_failed_status(
            status
        )
        failure_gap_id = named_hash(
            "V20_OFFICIAL_SHADOW_GAP_ID_V1",
            {
                "official_stream_id": self.config.official_stream_id,
                "trade_date": context.trade_date.isoformat(),
            },
        )
        raw_gaps = state.payload.get("official_rolling_gaps")
        if not isinstance(raw_gaps, list) or any(
            not isinstance(item, Mapping) for item in raw_gaps
        ):
            raise V20SemanticConflict("official state rolling gaps are malformed")
        if sum(item.get("gap_id") == failure_gap_id for item in raw_gaps) != 1:
            raise V20SemanticConflict("failed slot replay gap is missing or duplicated")
        replay_gaps = [dict(item) for item in raw_gaps if item.get("gap_id") != failure_gap_id]
        replay_state_payload = {
            **dict(state.payload),
            "official_rolling_gaps": replay_gaps,
        }
        replay_state = StateRecord(
            lineage_id=state.lineage_id,
            revision=state.revision,
            state_hash=sha256_json(replay_state_payload),
            payload=replay_state_payload,
        )
        prepared = prepare_entry(
            config=self.config,
            state=replay_state,
            bundle=bundle,
            completed_health=completed_health,
            completed_rolling=completed_rolling,
            maturity_gaps=maturity_gaps,
            artifacts=self._artifacts,
            calendar=context.calendar,
            scheduled_exits_today=tuple(status.semantic.get("scheduled_exits_today") or ()),
        )
        pure = prepared.commit.semantic
        if pure.get("action") == "INPUT_INVALID":
            raise V20SemanticConflict("late replay unexpectedly produced INPUT_INVALID")
        computed_at = self._aware_now()
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
            # Formatter-complete, frozen evidence for the manual endpoint's
            # byte-identical automatic-message rendering.  It remains nested
            # inside a non-actionable alert and is never committed as an
            # official decision, model batch, leg, holding, or order.
            "entry_render_semantic": dict(pure),
            "v16_snapshot_hash": bundle.snapshot_hash,
            "early_market_source_hash": early.source_hash,
            "policy_input_hash": status.semantic["policy_input_hash"],
            "requested_at": now.isoformat(),
            "retrieved_at": computed_at.isoformat(),
            "computed_at": computed_at.isoformat(),
            "data_cutoff": "09:39",
            "data_receipt_timeliness": "POST_CUTOFF",
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
            "state_reconstruction_profile": (
                "POST_TERMINAL_STATE_MINUS_D0_FAILURE_GAP_WATERMARK_IDEMPOTENT_V1"
            ),
            "state_replay_profile": "DEPLOYED_RUNTIME_LINEAGE",
            "bootstrap_mode": self.config.bootstrap_mode,
            "pit_limitations": [
                "RAW_MINUTE_ROWS_RECEIVED_OR_REUSED_AFTER_LIVE_CUTOFF",
                "CURRENT_FUNDAMENTAL_NAME_AND_ST_METADATA_MAY_REFLECT_LATER_SAME_DAY_STATE",
                "MEWS_IS_NOT_A_09:39_ENTRY_INPUT",
            ],
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
        if status is None or status.action != "INPUT_INVALID":
            raise V20StateConflict("late 09:39 replay requires today's INPUT_INVALID slot")
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
            or status.action != "INPUT_INVALID"
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
            or context.entry_status.action != "INPUT_INVALID"
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

    async def _run_entry_collection_cycle(
        self,
        context: _DayContext,
        now: datetime,
    ) -> None:
        """Prewarm and persist 09:31..09:39 facts without consuming state."""

        await self._refresh_entry_status(context)
        if context.entry_status is not None:
            return
        wall = now.timetz().replace(tzinfo=None)
        if (
            wall < self.config.clock.prewarm
            or wall >= self.config.clock.decision_finalization_deadline
        ):
            return
        if context.prewarmed is None:
            try:
                context.last_phase = "PREWARMING"
                budget_clock = self._aware_now()
                seconds_to_cutoff = (
                    _local(context.trade_date, self.config.clock.publish_deadline) - budget_clock
                ).total_seconds()
                prewarm_budget = min(
                    PREWARM_ATTEMPT_TIMEOUT_SECONDS,
                    seconds_to_cutoff - ENTRY_CUTOFF_RESERVE_SECONDS,
                )
                if prewarm_budget <= 0:
                    raise TimeoutError(
                        "prewarm cannot start without crossing the reserved 09:40 cutoff window"
                    )
                prewarmed = await asyncio.wait_for(
                    self._scan_pipeline.prewarm(
                        context.trade_date,
                        calendar=context.calendar,
                    ),
                    timeout=prewarm_budget,
                )
                self._verify_prewarm_dependencies(prewarmed)
                context.prewarmed = prewarmed
                context.collector = V20EarlyBarCollector(
                    context.trade_date,
                    context.prewarmed.universe_codes,
                )
                context.breadth_collector = V20EarlyBarCollector(
                    context.trade_date,
                    context.prewarmed.breadth_codes,
                )
                # Prewarm can take minutes.  Record the actual collector creation
                # clock, not the stale scheduler timestamp sampled before prewarm.
                context.collector_created_at = self._aware_now()
                context.last_phase = "COLLECTING_0939"
            except Exception as exc:
                context.last_phase = "PREWARM_RETRY"
                context.last_entry_failure_detail = f"PREWARM_RETRY: {type(exc).__name__}: {exc}"
                self._record_lane_error(
                    "decision",
                    f"PREWARM_RETRY: {type(exc).__name__}: {exc}",
                    now,
                )
                logger.warning("V20 prewarm will retry: %s", exc)

        if (
            self.config.clock.minute_collection_start
            <= wall
            < self.config.clock.decision_finalization_deadline
        ):
            try:
                await self._poll_entry_market(context, now)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                context.last_phase = "ENTRY_MARKET_RETRY"
                context.last_entry_failure_detail = (
                    f"ENTRY_MARKET_RETRY: {type(exc).__name__}: {exc}"
                )
                self._record_lane_error(
                    "decision",
                    f"ENTRY_MARKET_RETRY: {type(exc).__name__}: {exc}",
                    now,
                )
                logger.warning("V20 entry market collection will retry: %s", exc)

    def _verify_prewarm_dependencies(self, prewarmed: V20PrewarmedScan) -> None:
        """Reject in-process model or concept-data drift under a frozen lineage."""

        expected = self.config.strategy_dependency_hashes
        actual = {
            "models/lgbrank_latest.txt": prewarmed.scorer_model_sha256,
            "models/feature_list.json": prewarmed.scorer_feature_sha256,
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

        # The collection phase is also invoked independently before maturity
        # and predecessor reconciliation.  Repeating it here is idempotent and
        # covers direct unit/manual invocations of the decision phase.
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
        if (
            context.prewarmed is None
            or context.collector is None
            or context.breadth_collector is None
        ):
            raise RuntimeError("V20 entry prewarm is unavailable")
        if not context.early_stored_history_loaded:
            try:
                stored = await self._repository.list_raw_minute_bar_records(
                    context.prewarmed.required_minute_codes,
                    trade_date=context.trade_date,
                    end_labels=tuple(f"09:{minute:02d}" for minute in range(31, 40)),
                )
            except V20MinuteBarIntegrityConflict as exc:
                stored = list(exc.partial_records)
                await self._safe_alert(
                    code="ENTRY_RAW_FACT_CORRUPT",
                    entity_id=context.trade_date.isoformat(),
                    message=f"ignored {len(exc.corrupt_labels)} corrupt early-minute row(s)",
                    now=now,
                )
            stored_bars: list[TushareMinuteBar] = []
            for item in stored:
                try:
                    bar = _tushare_minute_from_record(item.payload)
                except (KeyError, TypeError, ValueError, OverflowError):
                    continue
                if bar.is_valid:
                    stored_bars.append(bar)
            context.collector.ingest(stored_bars)
            if context.breadth_collector is not None:
                context.breadth_collector.ingest(stored_bars)
            for bar in stored_bars:
                self._remember_bar(context, bar)
            context.early_stored_history_loaded = True
        # The 09:39 price alone is insufficient: V16 volume features require
        # the complete raw 09:31..09:39 path.  Recover only incomplete codes,
        # once, before a slot is allowed to commit.
        # The 80% gate belongs only to the actual V16 scan universe.  The
        # wider main-board breadth sample is best-effort and has its own
        # frozen <1000 => BASE 0 rule when the health state is paused.
        required_n = len(context.prewarmed.universe_codes)
        minimum_complete_n = math.ceil(required_n * self.config.market.minimum_quote_coverage)
        complete_n = len(context.collector.complete_codes())
        terminal_codes = frozenset(
            context.collector.codes_with_label(self.config.clock.decision_bar_label)
        )
        if len(terminal_codes) < minimum_complete_n:
            raise RuntimeError(
                f"raw 09:39 terminal-bar coverage is not ready: {len(terminal_codes)}/{required_n}"
            )
        incomplete_with_terminal = tuple(
            code for code in context.collector.incomplete_codes() if code in terminal_codes
        )
        recovery_needed = max(0, minimum_complete_n - complete_n)
        if not context.early_history_attempted and recovery_needed and incomplete_with_terminal:
            if recovery_needed > MAX_ENTRY_HISTORY_RECOVERY_CODES:
                raise RuntimeError(
                    "early-minute path recovery exceeds the hard 09:40 budget: "
                    f"need={recovery_needed}, cap={MAX_ENTRY_HISTORY_RECOVERY_CODES}"
                )
            context.early_history_attempted = True
            recovery_codes = list(incomplete_with_terminal[:recovery_needed])
            history = await asyncio.wait_for(
                self._scan_state.realtime_client.batch_get_minute_history(recovery_codes),
                timeout=ENTRY_HISTORY_RECOVERY_TIMEOUT_SECONDS,
            )
            context.collector.ingest_by_code(history)
            context.breadth_collector.ingest(bar for rows in history.values() for bar in rows)
            await self._persist_history(context, history, observed_at=now)

        early = context.collector.freeze()
        breadth_early = context.breadth_collector.freeze_terminal()
        bundle = await self._scan_pipeline.scan(
            context.prewarmed,
            early,
            breadth_early=breadth_early,
            minimum_quote_coverage=self.config.market.minimum_quote_coverage,
        )
        if bundle.frozen_at.tzinfo is None or bundle.frozen_at.utcoffset() is None:
            raise V20SemanticConflict("V16 decision formation clock must be timezone-aware")
        formed_at = bundle.frozen_at.astimezone(SHANGHAI)
        if formed_at.date() != context.trade_date:
            raise V20SemanticConflict("V16 decision formation date does not match its slot")
        observed_at = self._aware_now()
        normal_deadline = _local(context.trade_date, self.config.clock.publish_deadline)
        if formed_at >= normal_deadline or observed_at >= normal_deadline:
            await self._finalize_invalid_entry(
                context,
                observed_at,
                reason="INPUT_TIME_BOUNDARY_VIOLATION",
                detail=(
                    "normal V16 ENTER/BLOCK/NO_SIGNAL missed the strict 09:40 "
                    f"formation/submission boundary: formed_at={formed_at.isoformat()}, "
                    f"observed_at={observed_at.isoformat()}"
                ),
                invalid_commit_not_before_ts=normal_deadline,
            )
            return
        health, rolling, gaps = await self._policy_inputs(context.trade_date)
        state = await self._repository.load_state(self.config.state_lineage_id)
        scheduled = await self._scheduled_exits_today(context.trade_date)
        prepared = prepare_entry(
            config=self.config,
            state=state,
            bundle=bundle,
            completed_health=health,
            completed_rolling=rolling,
            maturity_gaps=gaps,
            artifacts=self._artifacts,
            calendar=context.calendar,
            scheduled_exits_today=scheduled,
        )
        try:
            await self._repository.commit_entry(prepared.commit)
        except V20EntryDeadlineExceeded:
            # The database clock is authoritative for the final boundary race.
            # The rejected normal transaction rolled back its state, shadows,
            # and model batch; replace it with the explicit failed-slot fact.
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

        exact = _staged_reference_rows()
        missing = [code for code in required if code not in exact]
        # Historical recovery is bounded to recommended symbols.  Pulling
        # rt_min_daily for the entire comparison universe would miss the hard
        # clock budget and is unnecessary for model-leg protection.
        history_targets = [code for code in missing if code in symbol_codes]
        history_due = (
            context.last_reference_history_at is None
            or (now - context.last_reference_history_at).total_seconds() >= 60
        )
        if history_targets and history_due:
            context.reference_history_attempted = True
            context.last_reference_history_at = now
            history = await self._scan_state.realtime_client.batch_get_minute_history(
                history_targets
            )
            await self._persist_history(context, history, observed_at=now)
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
                # only; the market-wide shadow comparison snapshot is collected
                # on D0 and has its own explicit 09:45 cutoff.
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

            # HEALTH/ROLLING7 compare a market-wide D0 snapshot.  Rebuilding
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
    ) -> None:
        active = await self._repository.list_active_legs(context.trade_date, **self._ledger_scope)
        if not active:
            return

        # Today's D1/D2 legs always run before an arbitrary restart backlog.
        # Persisted evidence and the unconditional D2 14:57 plan are evaluated
        # before any vendor recovery call.
        today_legs = self._today_exit_legs(active, context.trade_date)
        await self._evaluate_active_exits(today_legs, now, context.calendar)
        active = await self._repository.list_active_legs(context.trade_date, **self._ledger_scope)
        if not active:
            return
        wall = now.timetz().replace(tzinfo=None)
        # Keep one full wall-clock minute after each terminal raw label so a
        # 15-second polling phase cannot jump over 11:30 or 14:57.
        session = time(9, 31) < wall < time(11, 31) or time(13, 1) < wall < time(14, 58)
        today_legs = self._today_exit_legs(active, context.trade_date)
        tick_target_codes = frozenset(leg.code for leg in today_legs)
        expected_labels = frozenset(_expected_exit_labels(context.trade_date, now))
        # Feed health is about the bar that should be current at this tick, not
        # merely any legal bar seen earlier today.  During lunch the expected
        # frontier naturally remains 11:30; before the open it is empty.
        freshest_expected_labels = _live_exit_health_labels(
            context.trade_date,
            now,
            expected_labels,
        )
        latest_attempted = False
        latest_failed = False
        latest_evidence_codes: set[str] = set()
        if (
            session
            and today_legs
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
            except Exception:
                # Current-day history remains an independent fallback.  Only
                # after both sources have produced no legal evidence do we
                # classify a global live-exit data outage below.
                latest_failed = True
                logger.exception("V20 latest-minute exit poll failed; trying daily history")
            finally:
                context.last_exit_poll_at = now

        # Evaluate the just-persisted latest bar before attempting a potentially
        # slow full-day history recovery.
        active = await self._repository.list_active_legs(context.trade_date, **self._ledger_scope)
        today_legs = self._today_exit_legs(active, context.trade_date)
        await self._evaluate_active_exits(today_legs, now, context.calendar)
        active = await self._repository.list_active_legs(context.trade_date, **self._ledger_scope)
        today_legs = self._today_exit_legs(active, context.trade_date)

        recovery_codes: list[str] = []
        for code in sorted({leg.code for leg in today_legs}):
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
            missing = expected_labels - observed_labels
            key = (code, context.trade_date)
            last_attempt = context.exit_history_last_attempt.get(key)
            recovery_due = last_attempt is None or (now - last_attempt).total_seconds() >= 60
            if missing and recovery_due:
                context.exit_history_last_attempt[key] = now
                recovery_codes.append(code)
        history_attempted = False
        history_failed = False
        history_evidence_codes: set[str] = set()
        if recovery_codes:
            history_attempted = True
            try:
                history = await asyncio.wait_for(
                    self._scan_state.realtime_client.batch_get_minute_history(recovery_codes),
                    timeout=ENTRY_HISTORY_RECOVERY_TIMEOUT_SECONDS,
                )
                persisted_history = await self._persist_history(
                    context,
                    history,
                    observed_at=now,
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
                            await self._repository.record_exit_scan_watermark(
                                leg.model_leg_id,
                                trade_date=context.trade_date,
                                scanned_through_label=scanned_through,
                                source_hash=scan_hash,
                                **self._ledger_scope,
                            )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                history_failed = True
                await self._safe_alert(
                    code="EXIT_HISTORY_RECOVERY_FAILED",
                    entity_id=(f"{','.join(recovery_codes)}:{context.trade_date.isoformat()}"),
                    message=f"{type(exc).__name__}: {exc}",
                    now=now,
                )

        # A stale bar already present for one symbol must not disguise a total
        # current-feed outage, while one suspended/empty symbol must not turn a
        # healthy sibling response into a global failure. Only the bounded,
        # session-aware health labels are admitted as pre-existing evidence.
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
        # Once the bounded 11:30 publication grace has elapsed, the live
        # latest-minute endpoint is intentionally idle for lunch. Current-day
        # history is then the complete required source set until the first
        # afternoon bar is due, so it must be able to originate (not merely
        # preserve) a multi-symbol outage.
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
            await self._safe_alert(
                code="LIVE_EXIT_MARKET_DATA_UNAVAILABLE",
                entity_id=f"{context.trade_date.isoformat()}:{target_text}",
                message=(
                    f"{checked_source_text} produced no persisted legal "
                    f"exit evidence for any live target: {target_text}"
                ),
                now=now,
            )
            raise V20RepositoryError(
                "all live exit targets lack persisted legal current-day market evidence"
            )
        if evidence_codes:
            context.live_exit_market_data_outage = False

        unavailable_codes = tick_target_codes - evidence_codes
        if required_sources_checked and unavailable_codes and not global_outage:
            # This is deliberately diagnostic only: a single suspended or
            # empty-response name cannot suppress protection for valid siblings.
            missing_text = ",".join(sorted(unavailable_codes))
            await self._safe_alert(
                code="LIVE_EXIT_SYMBOL_DATA_GAP",
                entity_id=f"{context.trade_date.isoformat()}:{missing_text}",
                message=(
                    "individual live-exit symbols returned no persisted legal evidence; "
                    f"healthy siblings remain active: {missing_text}"
                ),
                now=now,
            )

        # Re-evaluate after today's bounded history staging and recover the
        # closed D1 side needed by a live D2 leg.
        active = await self._repository.list_active_legs(context.trade_date, **self._ledger_scope)
        today_legs = self._today_exit_legs(active, context.trade_date)
        await self._evaluate_active_exits(today_legs, now, context.calendar)
        active = await self._repository.list_active_legs(context.trade_date, **self._ledger_scope)
        recovery_scope = self._today_exit_legs(active, context.trade_date)
        if recovery_scope:
            await self._recover_closed_exit_windows(context, recovery_scope, now)
            active = await self._repository.list_active_legs(
                context.trade_date, **self._ledger_scope
            )
            final_workset = self._today_exit_legs(active, context.trade_date)
            await self._evaluate_active_exits(final_workset, now, context.calendar)
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
    ) -> None:
        failures: list[str] = []
        for record in active:
            try:
                detection_is_trading_day = now.date() in calendar or now.date() in {
                    record.d1,
                    record.d2,
                }
                detection_calendar_status = (
                    "CONFIRMED_TRADING"
                    if detection_is_trading_day
                    else "CONFIRMED_NON_TRADING"
                    if calendar
                    else "UNKNOWN"
                )
                next_trade_date = next(
                    (item for item in calendar if item > now.date()),
                    None,
                )
                await self._evaluate_one_exit(
                    record,
                    now,
                    detection_is_trading_day=detection_is_trading_day,
                    detection_calendar_status=detection_calendar_status,
                    next_trade_date=next_trade_date,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures.append(f"{record.model_leg_id}:{type(exc).__name__}")
                await self._safe_alert(
                    code="EXIT_LEG_EVALUATION_FAILED",
                    entity_id=record.model_leg_id,
                    message=f"{record.code}: {type(exc).__name__}: {exc}",
                    now=now,
                )
        if failures:
            raise V20RepositoryError(
                "one or more active exit legs could not be evaluated: " + ",".join(failures[:20])
            )

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
        for key in targets:
            context.exit_history_last_attempt[key] = now

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
            if isinstance(result, BaseException):
                if isinstance(result, asyncio.CancelledError):
                    raise result
                await self._safe_alert(
                    code="D1_EXIT_HISTORY_RECOVERY_FAILED",
                    entity_id=f"{code}:{trade_date.isoformat()}",
                    message=f"{type(result).__name__}: {result}",
                    now=now,
                )
                continue
            _code, _trade_date, history = result
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
        if now.date() >= record.d2:
            try:
                cutoff = _local(record.d1, self.config.clock.mews_cutoff_d1)
                await self._repository.select_mews_for_leg(
                    record.model_leg_id,
                    d1=record.d1,
                    cutoff=cutoff,
                )
                selected = await self._repository.load_selected_mews_for_leg(record.model_leg_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                auxiliary_reasons.append("MEWS_INPUT_UNAVAILABLE")
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
    ) -> Mapping[str, TushareMinuteBar]:
        rows = await asyncio.wait_for(
            self._scan_state.realtime_client.batch_get_latest_minute_bars(list(codes)),
            timeout=LATEST_MINUTE_POLL_TIMEOUT_SECONDS,
        )
        observation = self._aware_now(observed_at)
        complete = {
            code: bar
            for code, bar in rows.items()
            if bar.bar_end.astimezone(SHANGHAI) < observation
        }
        if complete:
            payloads = {code: _bar_payload(bar) for code, bar in complete.items()}
            sealed_hashes = await self._repository.record_minute_bars(list(payloads.values()))
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
    ) -> tuple[TushareMinuteBar, ...]:
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
            sealed_hashes = await self._repository.record_minute_bars(
                [payload for payload, _bar in payloads]
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
    ) -> None:
        if not self._repository_started:
            return
        semantic = {
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
        alert_id = named_hash(
            "V20_DATA_ALERT_EVENT_ID_V1",
            {
                "alert_code": code,
                "entity_id": entity_id,
                "event_trade_date": now.date().isoformat(),
                "semantic_hash": sha256_json(semantic),
            },
        )

        async def persist_alert() -> None:
            await self._repository.enqueue_alert(
                alert_id,
                self.config.route_id,
                semantic,
                sha256_json(semantic),
                **self._ledger_scope,
            )
            await self._repository.seal_event(alert_id, seal_v20_payload)

        persist_result = (await asyncio.gather(persist_alert(), return_exceptions=True))[0]
        if isinstance(persist_result, asyncio.CancelledError):
            raise persist_result
        if isinstance(persist_result, BaseException):
            detail = f"DATA_ALERT_PERSIST_FAILED:{code}:{type(persist_result).__name__}"
            self._record_lane_error("outbox_recovery", detail, now)
            logger.error(
                "V20 could not persist DATA_ALERT %s",
                code,
                exc_info=(type(persist_result), persist_result, persist_result.__traceback__),
            )

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
        if (
            status.official_stream_id != self.config.official_stream_id
            or status.strategy_version != self.config.strategy_version
            or status.lineage_id != self.config.state_lineage_id
            or status.config_id != status.config_hash[:24]
            or status.semantic.get("strategy_version") != status.strategy_version
            or status.semantic.get("config_hash") != status.config_hash
        ):
            raise V20ConfigError("today's terminal V20 slot belongs to another config/lineage")
        exact_current = (
            status.config_id == self.config.config_hash[:24]
            and status.config_hash == self.config.config_hash
        )
        semantic_state_hash = status.semantic.get("state_semantics_hash")
        snapshot_state_hash = status.snapshot.get("state_semantics_hash")
        if exact_current:
            if (
                semantic_state_hash != self.config.state_semantics_hash
                or snapshot_state_hash != self.config.state_semantics_hash
            ):
                raise V20ConfigError("persisted V20 slot state semantics are incompatible")
        else:
            compatible_binding = (
                status.config_id,
                status.config_hash,
                semantic_state_hash,
            )
            if (
                status.slot_status not in {"COMPLETED", "FAILED"}
                or semantic_state_hash != snapshot_state_hash
                or compatible_binding not in self._compatible_entry_bindings
            ):
                raise V20ConfigError(
                    "today's terminal V20 slot belongs to an unproven historical config"
                )
        if (
            status.semantic.get("schema_version") != V20_ENTRY_SEMANTIC_SCHEMA
            or status.semantic.get("feishu_formatter_profile") != V20_FEISHU_FORMATTER_PROFILE
            or status.semantic.get("action") != status.action
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

    def _aware_now(self, supplied: datetime | None = None) -> datetime:
        value = supplied if supplied is not None else self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("V20 service clock must be timezone-aware")
        return value.astimezone(SHANGHAI)

    def _require_running(self) -> None:
        if not self.config.enabled or not self._repository_started:
            raise V20RepositoryError("V20 service is not running")

    async def _require_manual_trigger_ready(self) -> None:
        """Require the same healthy runtime state exposed by the status API."""

        self._require_running()
        if (
            not self._started
            or self._stop_event.is_set()
            or len(self._tasks) != V20_RUNTIME_LANE_COUNT
            or any(task.done() for task in self._tasks)
        ):
            raise V20RepositoryError("V20 runtime is not healthy enough for manual trigger")
        runtime_status = await self.status()
        if runtime_status.get("healthy") is not True:
            raise V20RepositoryError("V20 runtime is not healthy enough for manual trigger")

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
