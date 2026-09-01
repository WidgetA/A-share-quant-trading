"""Build one deterministic V20 entry transaction from a frozen V16 bundle."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from src.data.database.v20_repository import (
    EntryCommit,
    ModelBatchWrite,
    ModelLegWrite,
    ShadowBatchWrite,
    StateRecord,
    sha256_json,
)
from src.web.v20_scan_pipeline import FrozenV16ScanBundle

from .artifacts import GArtifactBundle
from .exit_policy import derive_model_leg_id
from .identity import batch_id, decision_id, event_id, model_batch_id, named_hash, official_slot_id
from .models import (
    V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
    V20_ENTRY_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
    V20_INVALID_INPUT_SNAPSHOT_SCHEMA,
    V20_V16_SNAPSHOT_SCHEMA,
    BreadthSnapshot,
    GDecision,
    HealthObservation,
    HealthSnapshot,
    RollingBatch,
    RollingGap,
    StockThemeInput,
    deserialize_health_snapshot,
    serialize_health_snapshot,
)
from .policy import (
    advance_health_state,
    combine_entry_decision,
    decide_base,
    decision_half_for_date,
    evaluate_g,
    evaluate_rolling7,
)
from .runtime_config import V20RuntimeConfig

SHANGHAI = ZoneInfo("Asia/Shanghai")
STATE_SCHEMA = "v20-official-state/v1"


@dataclass(frozen=True)
class CompletedHealth:
    batch_id: str
    signal_date: date
    t2_date: date
    relative_return: float | None
    valid: bool
    invalid_reason: str | None = None


@dataclass(frozen=True)
class CompletedRolling:
    batch_id: str
    signal_date: date
    t2_date: date
    batch_return: float


@dataclass(frozen=True)
class ActiveRollingGap:
    gap_id: str
    signal_date: date
    maturity_date: date
    closed: bool = False
    aged_out: bool = False


@dataclass(frozen=True)
class PreparedEntry:
    commit: EntryCommit
    action: str
    final_multiplier: float


def genesis_state() -> dict[str, Any]:
    return {
        "schema_version": STATE_SCHEMA,
        "state_revision": 0,
        "health": serialize_health_snapshot(HealthSnapshot()),
        "official_rolling_gaps": [],
        "last_terminal_slot_id": None,
        "last_terminal_trade_date": None,
    }


def _validate_state(
    record: StateRecord,
    *,
    expected_lineage_id: str | None = None,
) -> HealthSnapshot:
    if expected_lineage_id is not None and record.lineage_id != expected_lineage_id:
        raise ValueError("official V20 state lineage does not match runtime config")
    payload = record.payload
    required = {
        "schema_version",
        "state_revision",
        "health",
        "official_rolling_gaps",
        "last_terminal_slot_id",
        "last_terminal_trade_date",
    }
    if set(payload) != required or payload.get("schema_version") != STATE_SCHEMA:
        raise ValueError("official V20 state schema/field set mismatch")
    if payload.get("state_revision") != record.revision:
        raise ValueError("official V20 state revision mismatch")
    health = payload.get("health")
    if not isinstance(health, Mapping):
        raise ValueError("official V20 health state is invalid")
    return deserialize_health_snapshot(health)


def _t_plus_two(trade_date: date, calendar: Sequence[date]) -> tuple[date, date]:
    future = [item for item in calendar if item > trade_date]
    if len(future) < 2:
        raise ValueError("trade calendar does not contain D1/D2")
    return future[0], future[1]


def _state_gaps(payload: Mapping[str, Any]) -> list[ActiveRollingGap]:
    raw = payload.get("official_rolling_gaps")
    if not isinstance(raw, list):
        raise ValueError("official_rolling_gaps must be an array")
    result: list[ActiveRollingGap] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise ValueError("official rolling gap must be an object")
        result.append(
            ActiveRollingGap(
                gap_id=str(item["gap_id"]),
                signal_date=date.fromisoformat(str(item["signal_date"])),
                maturity_date=date.fromisoformat(str(item["maturity_date"])),
                closed=bool(item.get("closed", False)),
                aged_out=bool(item.get("aged_out", False)),
            )
        )
    return result


def _policy_input_snapshot(
    *,
    completed_health: Sequence[CompletedHealth],
    completed_rolling: Sequence[CompletedRolling],
    maturity_gaps: Sequence[ActiveRollingGap],
) -> dict[str, Any]:
    """Canonicalize every external ledger fact consulted by one entry slot.

    Mature shadow rows live in their immutable relational ledger rather than
    being copied wholesale into official state.  Binding the exact consumed
    rows into the decision snapshot prevents a retry from silently producing
    different BASE/rolling7 output under the same input identity.
    """

    return {
        "schema_version": "v20-policy-input-snapshot/v1",
        "completed_health": [
            {
                "batch_id": item.batch_id,
                "signal_date": item.signal_date.isoformat(),
                "t2_date": item.t2_date.isoformat(),
                "relative_return": item.relative_return,
                "valid": item.valid,
                "invalid_reason": item.invalid_reason,
            }
            for item in sorted(
                completed_health,
                key=lambda value: (value.t2_date, value.signal_date, value.batch_id),
            )
        ],
        "completed_rolling": [
            {
                "batch_id": item.batch_id,
                "signal_date": item.signal_date.isoformat(),
                "t2_date": item.t2_date.isoformat(),
                "batch_return": item.batch_return,
            }
            for item in sorted(
                completed_rolling,
                key=lambda value: (value.t2_date, value.signal_date, value.batch_id),
            )
        ],
        "maturity_gaps": [
            {
                "gap_id": item.gap_id,
                "signal_date": item.signal_date.isoformat(),
                "maturity_date": item.maturity_date.isoformat(),
                "closed": item.closed,
                "aged_out": item.aged_out,
            }
            for item in sorted(
                maturity_gaps,
                key=lambda value: (value.maturity_date, value.signal_date, value.gap_id),
            )
        ],
    }


def _bind_decision_snapshot(
    *,
    v16_snapshot: Mapping[str, Any],
    v16_snapshot_hash: str,
    state_before_hash: str,
    state_semantics_hash: str,
    policy_inputs: Mapping[str, Any],
) -> tuple[dict[str, Any], str, str]:
    _validate_v16_snapshot_formatter_evidence(v16_snapshot)
    if sha256_json(v16_snapshot) != v16_snapshot_hash:
        raise ValueError("V16 snapshot hash mismatch before decision binding")
    policy_input_hash = sha256_json(policy_inputs)
    snapshot = {
        **dict(v16_snapshot),
        "schema_version": V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
        "v16_snapshot_schema_version": v16_snapshot.get("schema_version"),
        "v16_snapshot_hash": v16_snapshot_hash,
        "state_before_hash": state_before_hash,
        "state_semantics_hash": state_semantics_hash,
        "policy_input_hash": policy_input_hash,
        "policy_inputs": dict(policy_inputs),
    }
    return snapshot, sha256_json(snapshot), policy_input_hash


def _finite_number(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    )


def _validate_v16_snapshot_formatter_evidence(snapshot: Mapping[str, Any]) -> None:
    """Reject legacy/partial snapshots before committing an unrenderable event."""

    if snapshot.get("schema_version") != V20_V16_SNAPSHOT_SCHEMA:
        raise ValueError("unsupported V16 snapshot schema_version")
    funnel = snapshot.get("funnel")
    if not isinstance(funnel, Mapping):
        raise ValueError("V16 snapshot funnel is missing or invalid")
    for field in ("step0_universe_count", "step2_hot_board_count", "final_candidates"):
        value = funnel.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"V16 snapshot funnel.{field} must be a non-negative integer")

    board_gains = snapshot.get("board_avg_gains")
    if not isinstance(board_gains, Mapping):
        raise ValueError("V16 snapshot board_avg_gains is missing or invalid")
    if any(
        not isinstance(board, str) or not board or not _finite_number(gain)
        for board, gain in board_gains.items()
    ):
        raise ValueError("V16 snapshot board_avg_gains contains invalid evidence")

    symbols = snapshot.get("symbols")
    if not isinstance(symbols, list):
        raise ValueError("V16 snapshot symbols must be an array")
    required = {
        "rank",
        "code",
        "name",
        "score",
        "snapshot_price",
        "boards",
        "best_board",
        "is_driver",
        "cci",
        "volume_937",
        "history_hash",
    }
    for item in symbols:
        if not isinstance(item, Mapping) or not required.issubset(item):
            raise ValueError("V16 snapshot symbol formatter evidence is incomplete")
        rank = item["rank"]
        code = item["code"]
        name = item["name"]
        boards = item["boards"]
        best_board = item["best_board"]
        history_hash = item["history_hash"]
        if isinstance(rank, bool) or not isinstance(rank, int) or rank <= 0:
            raise ValueError("V16 snapshot symbol rank is invalid")
        if not isinstance(code, str) or len(code) != 6 or not code.isdigit():
            raise ValueError("V16 snapshot symbol code is invalid")
        if not isinstance(name, str):
            raise ValueError("V16 snapshot symbol name is invalid")
        if (
            not isinstance(boards, list)
            or not boards
            or any(not isinstance(board, str) or not board for board in boards)
            or len(set(boards)) != len(boards)
        ):
            raise ValueError("V16 snapshot symbol boards are invalid")
        if not isinstance(best_board, str) or best_board not in boards:
            raise ValueError("V16 snapshot symbol best_board is invalid")
        if any(board not in board_gains for board in boards):
            raise ValueError("V16 snapshot symbol board lacks frozen average gain")
        if not isinstance(item["is_driver"], bool):
            raise ValueError("V16 snapshot symbol is_driver is invalid")
        for field in ("score", "snapshot_price", "cci", "volume_937"):
            if not _finite_number(item[field]):
                raise ValueError(f"V16 snapshot symbol {field} is invalid")
        if float(item["snapshot_price"]) <= 0 or float(item["volume_937"]) <= 0:
            raise ValueError("V16 snapshot symbol price/volume must be positive")
        if (
            not isinstance(history_hash, str)
            or len(history_hash) != 64
            or any(character not in "0123456789abcdef" for character in history_hash)
        ):
            raise ValueError("V16 snapshot symbol history_hash is invalid")


def _coalesce_gaps(
    persisted: Sequence[ActiveRollingGap],
    additions: Sequence[ActiveRollingGap],
) -> list[ActiveRollingGap]:
    """Merge one immutable gap identity across durable and newly mature input."""

    by_id: dict[str, ActiveRollingGap] = {}
    for item in (*persisted, *additions):
        previous = by_id.get(item.gap_id)
        if previous is not None:
            if (
                previous.signal_date != item.signal_date
                or previous.maturity_date != item.maturity_date
            ):
                raise ValueError(f"rolling gap {item.gap_id!r} has conflicting semantics")
            item = ActiveRollingGap(
                gap_id=item.gap_id,
                signal_date=item.signal_date,
                maturity_date=item.maturity_date,
                closed=previous.closed or item.closed,
                aged_out=previous.aged_out or item.aged_out,
            )
        by_id[item.gap_id] = item
    return sorted(by_id.values(), key=lambda gap: (gap.signal_date, gap.gap_id))


def _merge_and_age_gaps(
    *,
    persisted: Sequence[ActiveRollingGap],
    additions: Sequence[ActiveRollingGap],
    completed_rolling: Sequence[CompletedRolling],
) -> list[dict[str, Any]]:
    """Return the deterministic next rolling-gap ledger.

    A mature missing/invalid shadow observation blocks rolling7 until seven
    later complete signal batches have displaced it.  Keeping that fact in the
    official state is essential: otherwise a process restart would silently
    turn ``UNKNOWN`` back into an apparently complete seven-batch window.
    """

    coalesced = _coalesce_gaps(persisted, additions)

    ordered_batches = sorted(
        completed_rolling,
        key=lambda item: (item.signal_date, item.t2_date, item.batch_id),
    )
    completed_ids = {item.batch_id for item in ordered_batches}
    result: list[dict[str, Any]] = []
    for item in coalesced:
        closed = item.closed or item.gap_id in completed_ids
        later_complete_n = sum(batch.signal_date > item.signal_date for batch in ordered_batches)
        aged_out = item.aged_out or (not closed and later_complete_n >= 7)
        result.append(
            {
                "gap_id": item.gap_id,
                "signal_date": item.signal_date.isoformat(),
                "maturity_date": item.maturity_date.isoformat(),
                "closed": closed,
                "aged_out": aged_out,
            }
        )
    return result


def prepare_entry(
    *,
    config: V20RuntimeConfig,
    state: StateRecord,
    bundle: FrozenV16ScanBundle,
    completed_health: Sequence[CompletedHealth],
    completed_rolling: Sequence[CompletedRolling],
    maturity_gaps: Sequence[ActiveRollingGap],
    artifacts: GArtifactBundle,
    calendar: Sequence[date],
    scheduled_exits_today: Sequence[Mapping[str, Any]] = (),
) -> PreparedEntry:
    health_before = _validate_state(
        state,
        expected_lineage_id=config.state_lineage_id,
    )
    observations = [
        HealthObservation(
            batch_id=item.batch_id,
            signal_date=item.signal_date,
            t2_exit_date=item.t2_date,
            relative_return=item.relative_return,
            valid=item.valid,
            invalid_reason=item.invalid_reason,
        )
        for item in completed_health
    ]
    health_after = advance_health_state(health_before, observations)
    breadth = BreadthSnapshot(bundle.breadth_valid_n, bundle.breadth_down_n)
    base = decide_base(health_after, breadth)

    rolling_batches = [
        RollingBatch(item.batch_id, item.signal_date, item.t2_date, item.batch_return)
        for item in completed_rolling
    ]
    persisted_gaps = _state_gaps(state.payload)
    resolved_gap_payloads = _merge_and_age_gaps(
        persisted=persisted_gaps,
        additions=maturity_gaps,
        completed_rolling=completed_rolling,
    )
    all_gaps = [
        RollingGap(
            str(item["gap_id"]),
            date.fromisoformat(str(item["signal_date"])),
            date.fromisoformat(str(item["maturity_date"])),
            bool(item["closed"]),
            bool(item["aged_out"]),
        )
        for item in resolved_gap_payloads
    ]
    rolling = evaluate_rolling7(
        decision_date=bundle.trade_date,
        complete_batches=rolling_batches,
        gaps=all_gaps,
    )

    g: GDecision | None = None
    if rolling.status.value == "BAD":
        theme_inputs = []
        for stock in bundle.scan_result.recommended:
            best = bundle.scan_result.stock_best_board.get(stock.code)
            theme_inputs.append(
                StockThemeInput(
                    code=stock.code,
                    best_board=(best,) if best else (),
                    hot_route=tuple(bundle.scan_result.stock_all_boards.get(stock.code, ())),
                )
            )
        g = evaluate_g(
            decision_date=bundle.trade_date,
            recommendations=theme_inputs,
            mapping=artifacts.mapping,
            prior_trade_amounts=bundle.prior_amount_yuan,
            threshold=artifacts.thresholds.get(decision_half_for_date(bundle.trade_date)),
        )
    entry = combine_entry_decision(
        scan_valid=True,
        recommendation_count=len(bundle.scan_result.recommended),
        base=base,
        rolling7=rolling,
        g=g,
    )

    policy_inputs = _policy_input_snapshot(
        completed_health=completed_health,
        completed_rolling=completed_rolling,
        maturity_gaps=maturity_gaps,
    )
    decision_snapshot, decision_snapshot_hash, policy_input_hash = _bind_decision_snapshot(
        v16_snapshot=bundle.snapshot,
        v16_snapshot_hash=bundle.snapshot_hash,
        state_before_hash=state.state_hash,
        state_semantics_hash=config.state_semantics_hash,
        policy_inputs=policy_inputs,
    )
    trade_date_text = bundle.trade_date.isoformat()
    slot = official_slot_id(config.official_stream_id, trade_date_text)
    decision = decision_id(slot, config.config_hash, decision_snapshot_hash, state.state_hash)
    event = event_id("ENTRY_DECISION", decision)
    d1, d2 = _t_plus_two(bundle.trade_date, calendar)
    next_state = {
        **dict(state.payload),
        "state_revision": state.revision + 1,
        "health": serialize_health_snapshot(health_after),
        "official_rolling_gaps": resolved_gap_payloads,
        "last_terminal_slot_id": slot,
        "last_terminal_trade_date": trade_date_text,
    }
    next_state_hash = sha256_json(next_state)

    g_state = g.status.value if g is not None else "NOT_EVALUATED"
    semantic: dict[str, Any] = {
        "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": event,
        "decision_id": decision,
        "strategy_version": config.strategy_version,
        "config_hash": config.config_hash,
        "state_semantics_hash": config.state_semantics_hash,
        "deployment_mode": config.deployment_mode,
        "trade_date": trade_date_text,
        "action": entry.action.value,
        "final_multiplier": entry.final_multiplier,
        "base_multiplier": entry.base_multiplier,
        "defense_multiplier": entry.defense_multiplier,
        "health_state": health_after.status.value,
        "health_recovery_count": health_after.recovery_count,
        "health_trailing_mean": (
            sum(item.relative_return or 0.0 for item in health_after.recent_valid) / 3
            if len(health_after.recent_valid) == 3
            else None
        ),
        "breadth_valid_n": bundle.breadth_valid_n,
        "breadth_down_n": bundle.breadth_down_n,
        "breadth_wilson_lower": base.wilson_lower_bound,
        "rolling7_state": rolling.status.value,
        "rolling7_r7": rolling.r7,
        "rolling7_l7": rolling.l7,
        "rolling7_window_ids": [item.batch_id for item in rolling.window],
        "policy_input_hash": policy_input_hash,
        "v16_snapshot_hash": bundle.snapshot_hash,
        "g_state": g_state,
        "g_max_component_size": g.max_cluster_size if g else None,
        "g_amount_below_q25_count": g.weak_metric_count if g else None,
        "reason_codes": list(entry.reasons),
        "last_complete_bar": str(bundle.snapshot["last_complete_bar"]),
        "reference_profile_id": config.reference_profile_id,
        "return_profile_id": config.return_profile_id,
        "v16_funnel": dict(bundle.snapshot["funnel"]),
        "v16_board_avg_gains": dict(bundle.snapshot["board_avg_gains"]),
        "symbols": list(bundle.snapshot["symbols"]),
        "scheduled_exits_today": [dict(item) for item in scheduled_exits_today],
        "state_before_hash": state.state_hash,
        "state_after_hash": next_state_hash,
    }
    semantic_hash = sha256_json(semantic)
    snapshot_id = named_hash(
        "V20_V16_SNAPSHOT_ID_V1",
        {"trade_date": trade_date_text, "snapshot_hash": decision_snapshot_hash},
    )

    shadow_writes: tuple[ShadowBatchWrite, ...] = ()
    if bundle.scan_result.recommended:
        common: dict[str, Any] = {
            "signal_date": trade_date_text,
            "d1": d1.isoformat(),
            "d2": d2.isoformat(),
            "reference_profile_id": config.reference_profile_id,
            "reference_status": "PENDING",
            "reference_prices": {},
            "reference_snapshot_hash": None,
        }
        symbols = [
            {"rank": item["rank"], "code": item["code"]} for item in bundle.snapshot["symbols"]
        ]
        health_payload = {
            **common,
            "top3": symbols[:3],
            "comparison_pool_codes": list(bundle.comparison_pool_codes),
        }
        rolling_payload = {**common, "symbols": symbols}
        shadow_writes = (
            ShadowBatchWrite(
                batch_id=batch_id(decision, "HEALTH"),
                kind="HEALTH",
                signal_date=bundle.trade_date,
                t2_date=d2,
                payload=health_payload,
            ),
            ShadowBatchWrite(
                batch_id=batch_id(decision, "ROLLING7"),
                kind="ROLLING7",
                signal_date=bundle.trade_date,
                t2_date=d2,
                payload=rolling_payload,
            ),
        )

    model_write = None
    if entry.final_multiplier > 0 and bundle.scan_result.recommended:
        batch_identity = model_batch_id(decision)
        per_leg = entry.final_multiplier / len(bundle.scan_result.recommended)
        legs = tuple(
            ModelLegWrite(
                model_leg_id=derive_model_leg_id(
                    model_batch_id=batch_identity,
                    code=stock.code,
                ),
                code=stock.code,
                stock_name=stock.name,
                rank=stock.rank,
                relative_weight=per_leg,
                d1=d1,
                d2=d2,
            )
            for stock in bundle.scan_result.recommended
        )
        model_write = ModelBatchWrite(
            model_batch_id=batch_identity,
            multiplier=entry.final_multiplier,
            evaluation_only=False,
            reference_profile_id=config.reference_profile_id,
            legs=legs,
        )

    expiry = datetime.combine(
        bundle.trade_date,
        config.clock.publish_deadline,
        tzinfo=SHANGHAI,
    )
    return PreparedEntry(
        commit=EntryCommit(
            official_stream_id=config.official_stream_id,
            slot_id=slot,
            trade_date=bundle.trade_date,
            strategy_version=config.strategy_version,
            config_id=config.config_hash[:24],
            config_hash=config.config_hash,
            lineage_id=config.state_lineage_id,
            expected_state_revision=state.revision,
            expected_state_hash=state.state_hash,
            next_state=next_state,
            next_state_hash=next_state_hash,
            snapshot_id=snapshot_id,
            snapshot_hash=decision_snapshot_hash,
            snapshot=decision_snapshot,
            decision_id=decision,
            event_id=event,
            action=entry.action.value,
            final_multiplier=entry.final_multiplier,
            semantic=semantic,
            semantic_content_hash=semantic_hash,
            action_expiry_ts=expiry,
            route_id=config.route_id,
            shadow_batches=shadow_writes,
            model_batch=model_write,
        ),
        action=entry.action.value,
        final_multiplier=entry.final_multiplier,
    )


def prepare_invalid_entry(
    *,
    config: V20RuntimeConfig,
    state: StateRecord,
    trade_date: date,
    calendar: Sequence[date],
    reason_code: str,
    detail: str,
    invalid_commit_not_before_ts: datetime,
    completed_health: Sequence[CompletedHealth] = (),
    completed_rolling: Sequence[CompletedRolling] = (),
    maturity_gaps: Sequence[ActiveRollingGap] = (),
    scheduled_exits_today: Sequence[Mapping[str, Any]] = (),
) -> PreparedEntry:
    """Finalize a failed daily slot without manufacturing an empty V16 scan."""
    if (
        invalid_commit_not_before_ts.tzinfo is None
        or invalid_commit_not_before_ts.utcoffset() is None
    ):
        raise ValueError("invalid commit-not-before timestamp must be timezone-aware")
    if invalid_commit_not_before_ts.astimezone(SHANGHAI).date() != trade_date:
        raise ValueError("invalid commit-not-before timestamp must match the trade date")
    health_before = _validate_state(
        state,
        expected_lineage_id=config.state_lineage_id,
    )
    observations = [
        HealthObservation(
            batch_id=item.batch_id,
            signal_date=item.signal_date,
            t2_exit_date=item.t2_date,
            relative_return=item.relative_return,
            valid=item.valid,
            invalid_reason=item.invalid_reason,
        )
        for item in completed_health
    ]
    health_after = advance_health_state(health_before, observations)
    persisted_gaps = _state_gaps(state.payload)
    trade_date_text = trade_date.isoformat()
    slot = official_slot_id(config.official_stream_id, trade_date_text)
    policy_inputs = _policy_input_snapshot(
        completed_health=completed_health,
        completed_rolling=completed_rolling,
        maturity_gaps=maturity_gaps,
    )
    policy_input_hash = sha256_json(policy_inputs)
    snapshot = {
        "schema_version": V20_INVALID_INPUT_SNAPSHOT_SCHEMA,
        "trade_date": trade_date_text,
        "reason_code": reason_code,
        "detail": detail,
        "state_before_hash": state.state_hash,
        "state_semantics_hash": config.state_semantics_hash,
        "policy_input_hash": policy_input_hash,
        "policy_inputs": policy_inputs,
    }
    snapshot_hash = sha256_json(snapshot)
    decision = decision_id(slot, config.config_hash, snapshot_hash, state.state_hash)
    event = event_id("ENTRY_DECISION", decision)
    gaps = _merge_and_age_gaps(
        persisted=persisted_gaps,
        additions=maturity_gaps,
        completed_rolling=completed_rolling,
    )
    next_state = {
        **dict(state.payload),
        "state_revision": state.revision + 1,
        "health": serialize_health_snapshot(health_after),
        "official_rolling_gaps": gaps,
        "last_terminal_slot_id": slot,
        "last_terminal_trade_date": trade_date_text,
    }
    next_state_hash = sha256_json(next_state)
    semantic = {
        "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": event,
        "decision_id": decision,
        "strategy_version": config.strategy_version,
        "config_hash": config.config_hash,
        "state_semantics_hash": config.state_semantics_hash,
        "deployment_mode": config.deployment_mode,
        "trade_date": trade_date_text,
        "action": "INPUT_INVALID",
        "final_multiplier": 0.0,
        "base_multiplier": 0.0,
        "defense_multiplier": 0.0,
        "health_state": None,
        "health_recovery_count": None,
        "health_trailing_mean": None,
        "breadth_valid_n": None,
        "breadth_down_n": None,
        "breadth_wilson_lower": None,
        "rolling7_state": None,
        "rolling7_r7": None,
        "rolling7_l7": None,
        "rolling7_window_ids": [],
        "policy_input_hash": policy_input_hash,
        "g_state": "NOT_EVALUATED",
        "g_max_component_size": None,
        "g_amount_below_q25_count": None,
        "reason_codes": [reason_code],
        "failure_detail": detail,
        "slot_commit_not_before_ts": invalid_commit_not_before_ts.isoformat(),
        "last_complete_bar": None,
        "reference_profile_id": config.reference_profile_id,
        "return_profile_id": config.return_profile_id,
        "symbols": [],
        "scheduled_exits_today": [dict(item) for item in scheduled_exits_today],
        "state_before_hash": state.state_hash,
        "state_after_hash": next_state_hash,
    }
    expiry = datetime.combine(trade_date, config.clock.publish_deadline, tzinfo=SHANGHAI)
    return PreparedEntry(
        commit=EntryCommit(
            official_stream_id=config.official_stream_id,
            slot_id=slot,
            trade_date=trade_date,
            strategy_version=config.strategy_version,
            config_id=config.config_hash[:24],
            config_hash=config.config_hash,
            lineage_id=config.state_lineage_id,
            expected_state_revision=state.revision,
            expected_state_hash=state.state_hash,
            next_state=next_state,
            next_state_hash=next_state_hash,
            snapshot_id=named_hash(
                "V20_V16_SNAPSHOT_ID_V1",
                {"trade_date": trade_date_text, "snapshot_hash": snapshot_hash},
            ),
            snapshot_hash=snapshot_hash,
            snapshot=snapshot,
            decision_id=decision,
            event_id=event,
            action="INPUT_INVALID",
            final_multiplier=0.0,
            semantic=semantic,
            semantic_content_hash=sha256_json(semantic),
            action_expiry_ts=expiry,
            route_id=config.route_id,
            invalid_commit_not_before_ts=invalid_commit_not_before_ts,
        ),
        action="INPUT_INVALID",
        final_multiplier=0.0,
    )


__all__ = [
    "ActiveRollingGap",
    "CompletedHealth",
    "CompletedRolling",
    "PreparedEntry",
    "genesis_state",
    "prepare_entry",
    "prepare_invalid_entry",
]
