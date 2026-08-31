"""Pure V20 model-leg exit policy.

This module produces strategy intents, never orders.  It deliberately has no
position, fill, account, fee, or notification APIs.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import date, datetime, time, timedelta
from math import isclose, isfinite
from typing import Iterable
from zoneinfo import ZoneInfo

from .models import (
    ExitEvaluation,
    ExitIntent,
    ExitSignalType,
    MewsSelection,
    MewsSnapshot,
    MinuteBar,
    ModelLeg,
    ReferenceStatus,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
D1_THRESHOLD = 0.92
D2_THRESHOLD = 0.88
D2_MEWS_DANGER_THRESHOLD = 0.95


def _at_or_below(value: float, threshold: float) -> bool:
    """Treat an economically exact threshold touch as a trigger.

    Prices arrive as decimal market values but are represented as binary
    floats inside the policy model.  For example, ``8.8 / 10`` can evaluate
    to ``0.8800000000000001``.  The rule is explicitly "touch or below", so a
    machine-representation residue must not leave a protection gap.
    """

    return value < threshold or isclose(value, threshold, rel_tol=1e-12, abs_tol=1e-12)


def _local_datetime(day: date, wall_time: time) -> datetime:
    return datetime.combine(day, wall_time, tzinfo=SHANGHAI)


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _canonical_ts(value: datetime) -> str:
    _require_aware(value, "timestamp")
    return value.astimezone(SHANGHAI).isoformat(timespec="microseconds")


def _named_hash(domain: str, payload: dict[str, str]) -> str:
    preimage = [domain, payload]
    encoded = json.dumps(
        preimage, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def derive_model_leg_id(*, model_batch_id: str, code: str) -> str:
    """Derive a leg identity; the same code in another batch is another leg."""

    if not model_batch_id or not code:
        raise ValueError("model_batch_id and code must be non-empty")
    return _named_hash("V20_MODEL_LEG_ID_V1", {"model_batch_id": model_batch_id, "code": code})


def derive_exit_intent_id(
    *, model_leg_id: str, signal_type: ExitSignalType, trigger_ts: datetime
) -> str:
    if not model_leg_id:
        raise ValueError("model_leg_id must be non-empty")
    return _named_hash(
        "V20_EXIT_INTENT_ID_V1",
        {
            "model_leg_id": model_leg_id,
            "exit_signal_type": signal_type.value,
            "trigger_ts": _canonical_ts(trigger_ts),
        },
    )


def is_valid_complete_minute_bar(bar: MinuteBar) -> bool:
    """Apply the exact completeness/positive-value rule from the V20 spec."""

    if not bar.code or not bar.source_confirms_complete:
        return False
    if bar.end_ts.tzinfo is None or bar.end_ts.utcoffset() is None:
        return False
    values = (bar.open, bar.high, bar.low, bar.close, bar.volume, bar.amount)
    return all(value is not None and isfinite(value) and value > 0 for value in values)


def _is_supported_bar_label(value: datetime) -> bool:
    local = value.astimezone(SHANGHAI).timetz().replace(tzinfo=None)
    return time(9, 31) <= local <= time(11, 30) or time(13, 1) <= local <= time(14, 57)


def next_continuous_minute(bar_end_ts: datetime) -> datetime | None:
    """Return the next standard A-share continuous-minute label.

    ``None`` means there is no further continuous minute before the 14:57
    closing-auction action point.
    """

    _require_aware(bar_end_ts, "bar_end_ts")
    local = bar_end_ts.astimezone(SHANGHAI)
    wall = local.timetz().replace(tzinfo=None)
    day = local.date()
    if time(9, 31) <= wall < time(11, 30):
        return local + timedelta(minutes=1)
    if wall == time(11, 30):
        return _local_datetime(day, time(13, 1))
    if time(13, 1) <= wall < time(14, 57):
        return local + timedelta(minutes=1)
    if wall == time(14, 57):
        return None
    raise ValueError("bar_end_ts is not a supported complete A-share minute label")


def select_mews_snapshot(
    *, leg: ModelLeg, snapshots: Iterable[MewsSnapshot], as_of: datetime
) -> MewsSelection:
    """Select the latest fully qualified MEWS snapshot at the frozen cutoff."""

    _require_aware(as_of, "as_of")
    cutoff = _local_datetime(leg.d1, time(9, 40))
    visible_as_of = as_of.astimezone(SHANGHAI)
    valid: list[MewsSnapshot] = []
    invalid_seen = False
    for snapshot in snapshots:
        required = (
            snapshot.source_trade_date,
            snapshot.generated_at,
            snapshot.received_at,
            snapshot.fast_state,
            snapshot.model_version,
            snapshot.data_version,
            snapshot.snapshot_id,
        )
        if any(value is None or value == "" for value in required):
            invalid_seen = True
            continue
        if snapshot.fast_state not in {"NORMAL", "DANGER"}:
            invalid_seen = True
            continue
        assert snapshot.source_trade_date is not None
        assert snapshot.generated_at is not None
        assert snapshot.received_at is not None
        if snapshot.generated_at.tzinfo is None or snapshot.received_at.tzinfo is None:
            invalid_seen = True
            continue
        if snapshot.source_trade_date >= leg.d1:
            invalid_seen = True
            continue
        # "Before D1 09:40" is deliberately strict.  A record timestamped
        # exactly at the cutoff was not available to the frozen 09:40 choice.
        if snapshot.generated_at.astimezone(SHANGHAI) >= cutoff:
            continue
        if snapshot.received_at.astimezone(SHANGHAI) >= cutoff:
            continue
        if snapshot.generated_at.astimezone(SHANGHAI) > visible_as_of:
            continue
        if snapshot.received_at.astimezone(SHANGHAI) > visible_as_of:
            continue
        valid.append(snapshot)
    if not valid:
        reason = "MEWS_INVALID" if invalid_seen else "MEWS_UNAVAILABLE"
        return MewsSelection(None, False, False, reason)
    selected = max(
        valid,
        key=lambda item: (
            item.source_trade_date,
            item.generated_at,
            item.received_at,
            item.snapshot_id,
        ),
    )
    return MewsSelection(
        selected,
        selected.fast_state == "DANGER",
        True,
        "MEWS_DANGER" if selected.fast_state == "DANGER" else "MEWS_NON_DANGER",
    )


def _make_protection_intent(
    *,
    leg: ModelLeg,
    bar: MinuteBar,
    signal_type: ExitSignalType,
    threshold: float,
    rule_actionable_from: datetime,
    extra_reasons: tuple[str, ...] = (),
) -> ExitIntent:
    assert leg.reference_entry_price is not None
    assert bar.close is not None
    wealth_factor = bar.close / leg.reference_entry_price
    return ExitIntent(
        exit_intent_id=derive_exit_intent_id(
            model_leg_id=leg.model_leg_id,
            signal_type=signal_type,
            trigger_ts=bar.end_ts,
        ),
        model_leg_id=leg.model_leg_id,
        model_batch_id=leg.model_batch_id,
        code=leg.code,
        signal_type=signal_type,
        trigger_ts=bar.end_ts,
        trigger_bar_end_ts=bar.end_ts,
        trigger_wealth_factor=wealth_factor,
        threshold_wealth_factor=threshold,
        rule_actionable_from=rule_actionable_from,
        reference_entry_price=leg.reference_entry_price,
        origin_final_relative_weight=leg.origin_final_relative_weight,
        reason_codes=(signal_type.value, *extra_reasons),
    )


def _make_plan_intent(
    leg: ModelLeg,
    *,
    extra_reasons: tuple[str, ...] = (),
) -> ExitIntent:
    trigger_ts = _local_datetime(leg.d2, time(14, 57))
    reasons = [ExitSignalType.PLAN_1457.value]
    if leg.reference_status is not ReferenceStatus.LOCKED:
        reasons.append("REFERENCE_UNAVAILABLE")
    reasons.extend(extra_reasons)
    return ExitIntent(
        exit_intent_id=derive_exit_intent_id(
            model_leg_id=leg.model_leg_id,
            signal_type=ExitSignalType.PLAN_1457,
            trigger_ts=trigger_ts,
        ),
        model_leg_id=leg.model_leg_id,
        model_batch_id=leg.model_batch_id,
        code=leg.code,
        signal_type=ExitSignalType.PLAN_1457,
        trigger_ts=trigger_ts,
        trigger_bar_end_ts=None,
        trigger_wealth_factor=None,
        threshold_wealth_factor=None,
        rule_actionable_from=trigger_ts,
        reference_entry_price=(
            leg.reference_entry_price if leg.reference_status is ReferenceStatus.LOCKED else None
        ),
        origin_final_relative_weight=leg.origin_final_relative_weight,
        reason_codes=tuple(reasons),
    )


def evaluate_exit(
    *,
    leg: ModelLeg,
    bars: Iterable[MinuteBar],
    as_of: datetime,
    mews_snapshots: Iterable[MewsSnapshot] = (),
    existing_intent: ExitIntent | None = None,
    d1_window_complete: bool = False,
    d2_pre1457_window_complete: bool = False,
) -> ExitEvaluation:
    """Return the first legal immutable exit intent for a model leg.

    The two ``*_window_complete`` flags are causal watermarks supplied by the
    market-data adapter.  They are retained as diagnostics and on the 14:57
    plan, but a gap can never disable a later independently valid stop bar.
    """

    _require_aware(as_of, "as_of")
    as_of_local = as_of.astimezone(SHANGHAI)
    if existing_intent is not None:
        if existing_intent.model_leg_id != leg.model_leg_id:
            raise ValueError("existing intent belongs to a different model leg")
        if existing_intent.model_batch_id != leg.model_batch_id or existing_intent.code != leg.code:
            raise ValueError("existing intent identity does not match the model leg")
        return ExitEvaluation(
            existing_intent, None, suppressed_reason="FIRST_INTENT_ALREADY_FROZEN"
        )
    if leg.evaluation_only:
        return ExitEvaluation(None, None, suppressed_reason="EVALUATION_ONLY")
    if as_of_local.date() <= leg.d0:
        return ExitEvaluation(None, None, suppressed_reason="D0_EXIT_FORBIDDEN")

    bar_list = list(bars)
    if any(bar.code != leg.code for bar in bar_list):
        raise ValueError("all minute bars must belong to the evaluated model leg code")
    label_counts = Counter(bar.end_ts for bar in bar_list)
    invalid_count = 0
    valid_bars: list[MinuteBar] = []
    for bar in bar_list:
        if bar.end_ts > as_of:
            continue
        if label_counts[bar.end_ts] != 1:
            invalid_count += 1
            continue
        if not is_valid_complete_minute_bar(bar) or not _is_supported_bar_label(bar.end_ts):
            invalid_count += 1
            continue
        local = bar.end_ts.astimezone(SHANGHAI)
        if local.date() not in (leg.d1, leg.d2):
            continue
        valid_bars.append(bar)
    valid_bars.sort(key=lambda bar: bar.end_ts)

    if leg.reference_status is ReferenceStatus.LOCKED:
        assert leg.reference_entry_price is not None
        d1_cutoff = _local_datetime(leg.d1, time(14, 57))
        for bar in valid_bars:
            local = bar.end_ts.astimezone(SHANGHAI)
            if local.date() != leg.d1 or local > d1_cutoff:
                continue
            assert bar.close is not None
            wealth_factor = bar.close / leg.reference_entry_price
            if _at_or_below(wealth_factor, D1_THRESHOLD):
                next_minute = next_continuous_minute(bar.end_ts)
                # The raw 14:57 close is only known after that label has
                # completed.  Its next executable continuous-minute boundary
                # is D2 09:31, never the already elapsed D1 14:57 instant.
                actionable = (
                    next_minute if next_minute is not None else _local_datetime(leg.d2, time(9, 31))
                )
                late_reason = (
                    ("EXIT_SIGNAL_LATE_FORMATION",) if as_of_local.date() > local.date() else ()
                )
                return ExitEvaluation(
                    _make_protection_intent(
                        leg=leg,
                        bar=bar,
                        signal_type=ExitSignalType.D1_CLOSE_CONFIRM_08,
                        threshold=D1_THRESHOLD,
                        rule_actionable_from=actionable,
                        extra_reasons=late_reason,
                    ),
                    None,
                    invalid_count,
                )

    if as_of_local.date() < leg.d2:
        d1_close = _local_datetime(leg.d1, time(14, 57))
        if as_of_local > d1_close and not d1_window_complete:
            return ExitEvaluation(
                None,
                None,
                invalid_count,
                suppressed_reason="D1_WINDOW_INCOMPLETE",
                diagnostics={"d1_window_complete": False},
            )
        return ExitEvaluation(None, None, invalid_count)

    mews = select_mews_snapshot(leg=leg, snapshots=mews_snapshots, as_of=as_of)
    if leg.reference_status is ReferenceStatus.LOCKED:
        assert leg.reference_entry_price is not None
        threshold = D2_MEWS_DANGER_THRESHOLD if mews.danger else D2_THRESHOLD
        signal_type = (
            ExitSignalType.D2_MEWS_DANGER_ENTRY_05 if mews.danger else ExitSignalType.D2_ENTRY_12
        )
        d2_plan_ts = _local_datetime(leg.d2, time(14, 57))
        for bar in valid_bars:
            local = bar.end_ts.astimezone(SHANGHAI)
            if local.date() != leg.d2 or local >= d2_plan_ts:
                continue
            assert bar.close is not None
            wealth_factor = bar.close / leg.reference_entry_price
            if not _at_or_below(wealth_factor, threshold):
                continue
            next_minute = next_continuous_minute(bar.end_ts)
            if next_minute is None or next_minute >= d2_plan_ts:
                continue
            extra = () if mews.available else (mews.reason,)
            if not d1_window_complete:
                extra = (*extra, "D1_WINDOW_INCOMPLETE")
            if not d2_pre1457_window_complete:
                extra = (*extra, "D2_WINDOW_INCOMPLETE")
            if as_of_local.date() > local.date():
                extra = (*extra, "EXIT_SIGNAL_LATE_FORMATION")
            return ExitEvaluation(
                _make_protection_intent(
                    leg=leg,
                    bar=bar,
                    signal_type=signal_type,
                    threshold=threshold,
                    rule_actionable_from=next_minute,
                    extra_reasons=extra,
                ),
                mews,
                invalid_count,
            )

    plan_ts = _local_datetime(leg.d2, time(14, 57))
    if as_of_local >= plan_ts:
        incomplete_reasons = (
            *(("D1_WINDOW_INCOMPLETE",) if not d1_window_complete else ()),
            *(("D2_WINDOW_INCOMPLETE",) if not d2_pre1457_window_complete else ()),
        )
        return ExitEvaluation(
            _make_plan_intent(leg, extra_reasons=incomplete_reasons),
            mews,
            invalid_count,
        )
    return ExitEvaluation(None, mews, invalid_count)
