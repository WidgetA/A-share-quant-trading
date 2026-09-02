"""Typed value objects and shared durable contract identifiers for V20.

The objects remain free of adapter behavior.  The version constants give the
producer, persistence recovery path, and notification formatter one immutable
contract vocabulary without importing an adapter into deterministic policy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from math import isfinite
from typing import Mapping

# Durable notification contracts.  These versions are intentionally separate
# from the strategy version: a persisted semantic object must only be rendered
# by the formatter whose input contract it was created for.  In particular,
# the v2 entry contract freezes the full V16 display evidence (funnel, board
# gains, and per-symbol diagnostics) instead of letting a newer formatter fill
# missing legacy fields with plausible defaults.
V20_V16_SNAPSHOT_SCHEMA = "v20-v16-snapshot/v2"
V20_DECISION_INPUT_SNAPSHOT_SCHEMA = "v20-decision-input-snapshot/v2"
V20_INVALID_INPUT_SNAPSHOT_SCHEMA = "v20-invalid-input-snapshot/v1"
V20_ENTRY_SEMANTIC_SCHEMA = "v20-entry-semantic/v2"
V20_EXIT_SEMANTIC_SCHEMA = "v20-exit-semantic/v2"
V20_DATA_ALERT_SEMANTIC_SCHEMA = "v20-data-alert-semantic/v2"
V20_FEISHU_FORMATTER_PROFILE = "V20_FULL_V16_FEISHU_V1"
V20_FEISHU_PAYLOAD_SCHEMA = "v20-feishu-payload/v2"


class HealthStatus(StrEnum):
    WARMUP = "WARMUP"
    HEALTHY = "HEALTHY"
    PAUSED_R0 = "PAUSED_R0"
    PAUSED_R1 = "PAUSED_R1"
    PAUSED_R2 = "PAUSED_R2"


class Rolling7Status(StrEnum):
    BAD = "BAD"
    NON_BAD = "NON_BAD"
    WARMUP = "WARMUP"
    DATA_GAP = "DATA_GAP"
    UNKNOWN = "UNKNOWN"


class GStatus(StrEnum):
    NOT_EVALUATED = "NOT_EVALUATED"
    TRIGGERED = "TRIGGERED"
    CLEAR = "CLEAR"
    UNKNOWN = "UNKNOWN"


class EntryAction(StrEnum):
    ENTER = "ENTER"
    # The machine field is calculated_action=BLOCK.  BLOCKED is the separate
    # terminal event status assigned by the orchestration/persistence layer.
    BLOCK = "BLOCK"
    NO_SIGNAL = "NO_SIGNAL"
    INPUT_INVALID = "INPUT_INVALID"


class ReferenceStatus(StrEnum):
    PENDING = "PENDING"
    LOCKED = "LOCKED"
    UNAVAILABLE = "UNAVAILABLE"


class ExitSignalType(StrEnum):
    D1_CLOSE_CONFIRM_08 = "D1_CLOSE_CONFIRM_08"
    D2_ENTRY_12 = "D2_ENTRY_12"
    D2_MEWS_DANGER_ENTRY_05 = "D2_MEWS_DANGER_ENTRY_05"
    PLAN_1457 = "PLAN_1457"


@dataclass(frozen=True, slots=True)
class HealthObservation:
    """One terminal BASE-health shadow result.

    Invalid observations advance the health watermark but never occupy one of
    the three valid health-window positions.
    """

    batch_id: str
    signal_date: date
    t2_exit_date: date
    relative_return: float | None
    valid: bool = True
    invalid_reason: str | None = None

    def __post_init__(self) -> None:
        if not self.batch_id:
            raise ValueError("batch_id must be non-empty")
        if self.t2_exit_date <= self.signal_date:
            raise ValueError("t2_exit_date must be later than signal_date")
        if self.valid:
            if self.relative_return is None or not isfinite(self.relative_return):
                raise ValueError("valid health observation requires a finite return")
            if self.invalid_reason is not None:
                raise ValueError("valid health observation cannot have invalid_reason")
        else:
            if self.relative_return is not None and not isfinite(self.relative_return):
                raise ValueError("relative_return must be finite when provided")
            if not self.invalid_reason:
                raise ValueError("invalid health observation requires invalid_reason")

    @property
    def order_key(self) -> tuple[date, date, str]:
        return (self.t2_exit_date, self.signal_date, self.batch_id)


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    status: HealthStatus = HealthStatus.WARMUP
    recovery_count: int = 0
    recent_valid: tuple[HealthObservation, ...] = ()
    last_processed_key: tuple[date, date, str] | None = None

    def __post_init__(self) -> None:
        if len(self.recent_valid) > 3:
            raise ValueError("recent_valid contains at most three observations")
        if any(not item.valid for item in self.recent_valid):
            raise ValueError("recent_valid cannot contain invalid observations")
        if tuple(sorted(self.recent_valid, key=lambda item: item.order_key)) != self.recent_valid:
            raise ValueError("recent_valid must be ordered")
        expected_counts = {
            HealthStatus.WARMUP: {0},
            HealthStatus.HEALTHY: {0, 3},
            HealthStatus.PAUSED_R0: {0},
            HealthStatus.PAUSED_R1: {1},
            HealthStatus.PAUSED_R2: {2},
        }
        if self.recovery_count not in expected_counts[self.status]:
            raise ValueError("recovery_count is inconsistent with health status")
        if self.status is HealthStatus.WARMUP and len(self.recent_valid) >= 3:
            raise ValueError("WARMUP cannot contain a complete three-label window")
        if self.status is not HealthStatus.WARMUP and len(self.recent_valid) != 3:
            raise ValueError("a terminal non-WARMUP state requires exactly three valid labels")
        if self.recent_valid and self.last_processed_key is None:
            raise ValueError("a non-empty health window requires a processed watermark")
        if self.last_processed_key is not None:
            if any(item.order_key > self.last_processed_key for item in self.recent_valid):
                raise ValueError("recent health observation exceeds watermark")


@dataclass(frozen=True, slots=True)
class BreadthSnapshot:
    valid_n: int
    declining_n: int

    def __post_init__(self) -> None:
        if self.valid_n < 0:
            raise ValueError("valid_n cannot be negative")
        if self.declining_n < 0 or self.declining_n > self.valid_n:
            raise ValueError("declining_n must be between zero and valid_n")


@dataclass(frozen=True, slots=True)
class BaseDecision:
    multiplier: float
    breadth_evaluated: bool
    wilson_lower_bound: float | None
    reason: str


@dataclass(frozen=True, slots=True)
class RollingBatch:
    batch_id: str
    signal_date: date
    t2_exit_date: date
    gross_price_return: float

    def __post_init__(self) -> None:
        if not self.batch_id:
            raise ValueError("batch_id must be non-empty")
        if self.t2_exit_date <= self.signal_date:
            raise ValueError("t2_exit_date must be later than signal_date")
        if not isfinite(self.gross_price_return):
            raise ValueError("gross_price_return must be finite")


@dataclass(frozen=True, slots=True)
class RollingGap:
    gap_id: str
    signal_date: date
    gap_maturity_date: date
    closed: bool = False
    aged_out: bool = False

    def __post_init__(self) -> None:
        if not self.gap_id:
            raise ValueError("gap_id must be non-empty")
        if self.gap_maturity_date <= self.signal_date:
            raise ValueError("gap_maturity_date must be later than signal_date")
        if self.closed and self.aged_out:
            raise ValueError("a gap cannot be both closed and aged out")


@dataclass(frozen=True, slots=True)
class Rolling7Decision:
    status: Rolling7Status
    r7: float | None
    l7: int | None
    window: tuple[RollingBatch, ...]
    active_gap_ids: tuple[str, ...] = ()
    unknown_reason: str | None = None


@dataclass(frozen=True, slots=True)
class ThemeMapping:
    raw_label: str
    canonical_theme_id: str
    canonical_theme_name_cn: str
    label_role: str
    cluster_allowed: bool

    def __post_init__(self) -> None:
        if not all(
            (self.raw_label, self.canonical_theme_id, self.canonical_theme_name_cn, self.label_role)
        ):
            raise ValueError("theme mapping text fields must be non-empty")


@dataclass(frozen=True, slots=True)
class StockThemeInput:
    code: str
    best_board: tuple[str, ...] = ()
    hot_route: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.code:
            raise ValueError("code must be non-empty")

    @property
    def labels(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys((*self.best_board, *self.hot_route)))


@dataclass(frozen=True, slots=True)
class Q25Threshold:
    decision_half: str
    prior_amount_total: float
    prior_amount_median: float
    prior_amount_bottom3_sum: float
    sample_n: int
    calibration_as_of_ts: str = ""
    threshold_source_hash: str = ""

    def __post_init__(self) -> None:
        values = (
            self.prior_amount_total,
            self.prior_amount_median,
            self.prior_amount_bottom3_sum,
        )
        if any(not isfinite(value) or value <= 0 for value in values):
            raise ValueError("Q25 thresholds must be finite and positive")
        if self.sample_n <= 0:
            raise ValueError("sample_n must be positive")


@dataclass(frozen=True, slots=True)
class GDecision:
    status: GStatus
    max_cluster_size: int | None
    amount_valid_n: int
    prior_amount_total: float | None
    prior_amount_median: float | None
    prior_amount_bottom3_sum: float | None
    weak_metric_count: int | None
    reason: str


@dataclass(frozen=True, slots=True)
class EntryDecision:
    action: EntryAction
    final_multiplier: float
    base_multiplier: float
    defense_multiplier: float
    rolling7_status: Rolling7Status | None
    g_status: GStatus
    per_stock_relative_weight: float | None
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MinuteBar:
    code: str
    end_ts: datetime
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    amount: float | None
    source_confirms_complete: bool = True


@dataclass(frozen=True, slots=True)
class MewsSnapshot:
    source_trade_date: date | None
    generated_at: datetime | None
    received_at: datetime | None
    fast_state: str | None
    model_version: str | None
    data_version: str | None
    snapshot_id: str | None
    # The trade date the daily value belongs to (evidence
    # ``signal_available_date``).  A late same-day repair is a legal D2 input
    # only when this matches the leg's D1.
    availability_date: date | None = None


@dataclass(frozen=True, slots=True)
class MewsSelection:
    snapshot: MewsSnapshot | None
    danger: bool
    available: bool
    reason: str


@dataclass(frozen=True, slots=True)
class ModelLeg:
    model_leg_id: str
    model_batch_id: str
    code: str
    d0: date
    d1: date
    d2: date
    origin_final_relative_weight: float
    evaluation_only: bool
    reference_status: ReferenceStatus
    reference_entry_price: float | None

    def __post_init__(self) -> None:
        if not self.model_leg_id or not self.model_batch_id or not self.code:
            raise ValueError("model leg identity fields must be non-empty")
        if not (self.d0 < self.d1 < self.d2):
            raise ValueError("model leg dates must satisfy D0 < D1 < D2")
        if (
            not isfinite(self.origin_final_relative_weight)
            or self.origin_final_relative_weight <= 0
        ):
            raise ValueError("origin_final_relative_weight must be finite and positive")
        if self.reference_status is ReferenceStatus.LOCKED:
            if self.reference_entry_price is None:
                raise ValueError("LOCKED reference requires a price")
            if not isfinite(self.reference_entry_price) or self.reference_entry_price <= 0:
                raise ValueError("reference price must be finite and positive")
        elif self.reference_entry_price is not None:
            raise ValueError("only LOCKED reference may carry a price")


@dataclass(frozen=True, slots=True)
class ExitIntent:
    exit_intent_id: str
    model_leg_id: str
    model_batch_id: str
    code: str
    signal_type: ExitSignalType
    trigger_ts: datetime
    trigger_bar_end_ts: datetime | None
    trigger_wealth_factor: float | None
    threshold_wealth_factor: float | None
    rule_actionable_from: datetime
    reference_entry_price: float | None
    recommended_exit_fraction: float = 1.0
    target_model_leg_relative_weight: float = 0.0
    origin_final_relative_weight: float = 0.0
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not all((self.exit_intent_id, self.model_leg_id, self.model_batch_id, self.code)):
            raise ValueError("exit intent identity fields must be non-empty")
        if self.trigger_ts.tzinfo is None or self.trigger_ts.utcoffset() is None:
            raise ValueError("trigger_ts must be timezone-aware")
        if (
            self.rule_actionable_from.tzinfo is None
            or self.rule_actionable_from.utcoffset() is None
        ):
            raise ValueError("rule_actionable_from must be timezone-aware")
        if self.rule_actionable_from < self.trigger_ts:
            raise ValueError("rule_actionable_from cannot precede trigger_ts")
        if self.recommended_exit_fraction != 1.0:
            raise ValueError("V20 exits the full model leg")
        if self.target_model_leg_relative_weight != 0.0:
            raise ValueError("V20 exit target weight is zero")
        if (
            not isfinite(self.origin_final_relative_weight)
            or self.origin_final_relative_weight <= 0
        ):
            raise ValueError("origin weight must be finite and positive")
        expected_thresholds = {
            ExitSignalType.D1_CLOSE_CONFIRM_08: 0.92,
            ExitSignalType.D2_ENTRY_12: 0.88,
            ExitSignalType.D2_MEWS_DANGER_ENTRY_05: 0.95,
        }
        if self.signal_type is ExitSignalType.PLAN_1457:
            if any(
                value is not None
                for value in (
                    self.trigger_bar_end_ts,
                    self.trigger_wealth_factor,
                    self.threshold_wealth_factor,
                )
            ):
                raise ValueError("non-price exit cannot carry protection-trigger fields")
        else:
            if self.trigger_bar_end_ts != self.trigger_ts:
                raise ValueError("protection trigger_ts must equal trigger_bar_end_ts")
            if (
                self.trigger_wealth_factor is None
                or not isfinite(self.trigger_wealth_factor)
                or self.trigger_wealth_factor <= 0
            ):
                raise ValueError("protection intent requires a positive wealth factor")
            if self.threshold_wealth_factor != expected_thresholds[self.signal_type]:
                raise ValueError("protection threshold does not match signal type")
            if self.reference_entry_price is None or self.reference_entry_price <= 0:
                raise ValueError("protection intent requires a locked reference price")


@dataclass(frozen=True, slots=True)
class ExitEvaluation:
    intent: ExitIntent | None
    mews_selection: MewsSelection | None
    ignored_invalid_bar_count: int = 0
    suppressed_reason: str | None = None
    diagnostics: Mapping[str, object] = field(default_factory=dict)


def serialize_health_snapshot(snapshot: HealthSnapshot) -> dict[str, object]:
    """Return the versioned JSON-compatible BASE policy state.

    Input hashes, lineage, revisions, and pending batches belong to the outer
    official-state schema; this is only the pure BASE state fragment.
    """

    return {
        "schema_version": "v20-health-snapshot/v1",
        "status": snapshot.status.value,
        "recovery_count": snapshot.recovery_count,
        "recent_valid": [
            {
                "batch_id": item.batch_id,
                "signal_date": item.signal_date.isoformat(),
                "t2_exit_date": item.t2_exit_date.isoformat(),
                "relative_return": item.relative_return,
            }
            for item in snapshot.recent_valid
        ],
        "last_processed_key": (
            None
            if snapshot.last_processed_key is None
            else [
                snapshot.last_processed_key[0].isoformat(),
                snapshot.last_processed_key[1].isoformat(),
                snapshot.last_processed_key[2],
            ]
        ),
    }


def deserialize_health_snapshot(payload: Mapping[str, object]) -> HealthSnapshot:
    """Strict inverse of :func:`serialize_health_snapshot`; extras fail closed."""

    expected_keys = {
        "schema_version",
        "status",
        "recovery_count",
        "recent_valid",
        "last_processed_key",
    }
    if set(payload) != expected_keys:
        raise ValueError("health snapshot field set mismatch")
    if payload["schema_version"] != "v20-health-snapshot/v1":
        raise ValueError("unsupported health snapshot schema_version")
    status_raw = payload["status"]
    recovery_raw = payload["recovery_count"]
    recent_raw = payload["recent_valid"]
    watermark_raw = payload["last_processed_key"]
    if not isinstance(status_raw, str):
        raise ValueError("health status must be a string")
    if isinstance(recovery_raw, bool) or not isinstance(recovery_raw, int):
        raise ValueError("health recovery_count must be an integer")
    if not isinstance(recent_raw, list):
        raise ValueError("health recent_valid must be an array")
    recent: list[HealthObservation] = []
    item_keys = {"batch_id", "signal_date", "t2_exit_date", "relative_return"}
    for raw in recent_raw:
        if not isinstance(raw, dict) or set(raw) != item_keys:
            raise ValueError("health observation field set mismatch")
        if not isinstance(raw["batch_id"], str) or not raw["batch_id"]:
            raise ValueError("health batch_id must be a non-empty string")
        if not isinstance(raw["signal_date"], str) or not isinstance(raw["t2_exit_date"], str):
            raise ValueError("health observation dates must be strings")
        relative_return = raw["relative_return"]
        if isinstance(relative_return, bool) or not isinstance(relative_return, (int, float)):
            raise ValueError("health relative_return must be numeric")
        try:
            observation = HealthObservation(
                batch_id=raw["batch_id"],
                signal_date=date.fromisoformat(raw["signal_date"]),
                t2_exit_date=date.fromisoformat(raw["t2_exit_date"]),
                relative_return=float(relative_return),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid health observation") from exc
        recent.append(observation)
    watermark: tuple[date, date, str] | None
    if watermark_raw is None:
        watermark = None
    elif (
        isinstance(watermark_raw, list)
        and len(watermark_raw) == 3
        and all(isinstance(value, str) for value in watermark_raw)
    ):
        try:
            watermark = (
                date.fromisoformat(watermark_raw[0]),
                date.fromisoformat(watermark_raw[1]),
                watermark_raw[2],
            )
        except ValueError as exc:
            raise ValueError("invalid health watermark") from exc
        if not watermark[2]:
            raise ValueError("health watermark batch id must be non-empty")
    else:
        raise ValueError("health last_processed_key must be null or a three-item array")
    try:
        status = HealthStatus(status_raw)
    except ValueError as exc:
        raise ValueError("invalid health status") from exc
    return HealthSnapshot(status, recovery_raw, tuple(recent), watermark)
