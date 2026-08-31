from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pytest

from src.strategy.v20.exit_policy import (
    derive_model_leg_id,
    evaluate_exit,
    is_valid_complete_minute_bar,
    select_mews_snapshot,
)
from src.strategy.v20.models import (
    ExitSignalType,
    MewsSnapshot,
    MinuteBar,
    ModelLeg,
    ReferenceStatus,
)

TZ = ZoneInfo("Asia/Shanghai")
D0 = date(2026, 8, 27)
D1 = date(2026, 8, 28)
D2 = date(2026, 8, 31)


def _dt(day: date, hour: int, minute: int) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=TZ)


def _leg(
    *,
    model_batch_id: str = "batch-a",
    code: str = "000001",
    reference_status: ReferenceStatus = ReferenceStatus.LOCKED,
    evaluation_only: bool = False,
) -> ModelLeg:
    leg_id = derive_model_leg_id(model_batch_id=model_batch_id, code=code)
    return ModelLeg(
        model_leg_id=leg_id,
        model_batch_id=model_batch_id,
        code=code,
        d0=D0,
        d1=D1,
        d2=D2,
        origin_final_relative_weight=0.1,
        evaluation_only=evaluation_only,
        reference_status=reference_status,
        reference_entry_price=100.0 if reference_status is ReferenceStatus.LOCKED else None,
    )


def _bar(
    day: date,
    hour: int,
    minute: int,
    close: float,
    *,
    code: str = "000001",
    complete: bool = True,
    volume: float = 1.0,
    amount: float = 1.0,
) -> MinuteBar:
    return MinuteBar(
        code=code,
        end_ts=_dt(day, hour, minute),
        open=close,
        high=close,
        low=close,
        close=close,
        volume=volume,
        amount=amount,
        source_confirms_complete=complete,
    )


def _mews(
    *,
    fast_state: str = "DANGER",
    source_trade_date: date | None = D0,
    generated_at: datetime | None = None,
    received_at: datetime | None = None,
    snapshot_id: str | None = "m1",
) -> MewsSnapshot:
    return MewsSnapshot(
        source_trade_date=source_trade_date,
        generated_at=generated_at or _dt(D1, 9, 39),
        received_at=received_at or _dt(D1, 9, 39),
        fast_state=fast_state,
        model_version="model-v1",
        data_version="data-v1",
        snapshot_id=snapshot_id,
    )


def test_d0_never_exits_and_evaluation_only_never_emits_official_intent() -> None:
    d0 = evaluate_exit(leg=_leg(), bars=[_bar(D0, 9, 31, 1)], as_of=_dt(D0, 15, 0))
    assert d0.intent is None
    assert d0.suppressed_reason == "D0_EXIT_FORBIDDEN"

    shadow = evaluate_exit(
        leg=_leg(evaluation_only=True),
        bars=[_bar(D1, 9, 31, 1)],
        as_of=_dt(D2, 15, 0),
        d1_window_complete=True,
        d2_pre1457_window_complete=True,
    )
    assert shadow.intent is None
    assert shadow.suppressed_reason == "EVALUATION_ONLY"


def test_d1_threshold_is_inclusive_and_uses_next_minute() -> None:
    result = evaluate_exit(
        leg=_leg(),
        bars=[_bar(D1, 9, 31, 92.000001), _bar(D1, 9, 32, 92.0)],
        as_of=_dt(D1, 9, 32),
    )
    assert result.intent is not None
    assert result.intent.signal_type is ExitSignalType.D1_CLOSE_CONFIRM_08
    assert result.intent.trigger_ts == _dt(D1, 9, 32)
    assert result.intent.rule_actionable_from == _dt(D1, 9, 33)
    assert result.intent.trigger_wealth_factor == pytest.approx(0.92)
    assert result.intent.threshold_wealth_factor == 0.92
    assert result.intent.recommended_exit_fraction == 1.0
    assert result.intent.target_model_leg_relative_weight == 0.0


def test_d1_midday_and_closing_boundaries_have_no_hole() -> None:
    midday = evaluate_exit(leg=_leg(), bars=[_bar(D1, 11, 30, 90)], as_of=_dt(D1, 11, 30))
    assert midday.intent is not None
    assert midday.intent.rule_actionable_from == _dt(D1, 13, 1)

    last_continuous = evaluate_exit(leg=_leg(), bars=[_bar(D1, 14, 56, 90)], as_of=_dt(D1, 14, 56))
    assert last_continuous.intent is not None
    assert last_continuous.intent.rule_actionable_from == _dt(D1, 14, 57)

    auction = evaluate_exit(leg=_leg(), bars=[_bar(D1, 14, 57, 90)], as_of=_dt(D1, 14, 57))
    assert auction.intent is not None
    assert auction.intent.rule_actionable_from == _dt(D2, 9, 31)


def test_invalid_or_duplicate_minute_cannot_trigger() -> None:
    duplicate = _bar(D1, 9, 31, 80)
    result = evaluate_exit(
        leg=_leg(),
        bars=[duplicate, duplicate, _bar(D1, 9, 32, 80, volume=0)],
        as_of=_dt(D1, 9, 33),
    )
    assert result.intent is None
    assert result.ignored_invalid_bar_count == 3
    assert not is_valid_complete_minute_bar(_bar(D1, 9, 31, 80, amount=0))


def test_earliest_d1_trigger_wins_even_when_input_is_unsorted() -> None:
    result = evaluate_exit(
        leg=_leg(),
        bars=[_bar(D1, 10, 0, 70), _bar(D1, 9, 31, 91), _bar(D1, 9, 32, 80)],
        as_of=_dt(D1, 10, 0),
    )
    assert result.intent is not None
    assert result.intent.trigger_ts == _dt(D1, 9, 31)


def test_historical_trigger_formed_on_later_day_is_explicitly_late() -> None:
    result = evaluate_exit(
        leg=_leg(),
        bars=[_bar(D1, 9, 31, 80)],
        as_of=_dt(D2, 9, 31),
        d1_window_complete=True,
    )
    assert result.intent is not None
    assert "EXIT_SIGNAL_LATE_FORMATION" in result.intent.reason_codes


def test_missing_closed_d1_window_cannot_disable_a_valid_d2_stop() -> None:
    result = evaluate_exit(leg=_leg(), bars=[_bar(D2, 9, 31, 80)], as_of=_dt(D2, 9, 31))
    assert result.intent is not None
    assert result.intent.signal_type is ExitSignalType.D2_ENTRY_12
    assert "D1_WINDOW_INCOMPLETE" in result.intent.reason_codes


def test_missing_early_d1_minute_cannot_disable_a_later_valid_stop() -> None:
    result = evaluate_exit(
        leg=_leg(),
        bars=[_bar(D1, 9, 32, 80)],
        as_of=_dt(D1, 9, 32),
    )

    assert result.intent is not None
    assert result.intent.signal_type is ExitSignalType.D1_CLOSE_CONFIRM_08
    assert result.intent.trigger_ts == _dt(D1, 9, 32)


def test_d2_default_threshold_is_inclusive_when_mews_unavailable() -> None:
    result = evaluate_exit(
        leg=_leg(),
        bars=[_bar(D2, 9, 31, 88.000001), _bar(D2, 9, 32, 88.0)],
        as_of=_dt(D2, 9, 32),
        d1_window_complete=True,
    )
    assert result.intent is not None
    assert result.intent.signal_type is ExitSignalType.D2_ENTRY_12
    assert result.intent.trigger_ts == _dt(D2, 9, 32)
    assert result.intent.rule_actionable_from == _dt(D2, 9, 33)
    assert "MEWS_UNAVAILABLE" in result.intent.reason_codes


def test_mews_danger_tightens_d2_to_five_percent() -> None:
    result = evaluate_exit(
        leg=_leg(),
        bars=[_bar(D2, 9, 31, 95.0)],
        as_of=_dt(D2, 9, 31),
        mews_snapshots=[_mews()],
        d1_window_complete=True,
    )
    assert result.intent is not None
    assert result.intent.signal_type is ExitSignalType.D2_MEWS_DANGER_ENTRY_05
    assert result.intent.threshold_wealth_factor == 0.95
    assert result.mews_selection is not None and result.mews_selection.danger


def test_mews_cutoff_is_strict_and_latest_qualified_selection() -> None:
    eligible_old = _mews(generated_at=_dt(D1, 9, 38), received_at=_dt(D1, 9, 38), snapshot_id="a")
    eligible_latest = _mews(
        generated_at=_dt(D1, 9, 39),
        received_at=_dt(D1, 9, 39),
        snapshot_id="b",
    )
    exactly_at_cutoff = _mews(
        generated_at=_dt(D1, 9, 40), received_at=_dt(D1, 9, 40), snapshot_id="cutoff"
    )
    future = _mews(generated_at=_dt(D1, 9, 41), received_at=_dt(D1, 9, 41), snapshot_id="c")
    wrong_source = _mews(source_trade_date=D1, snapshot_id="d")
    selected = select_mews_snapshot(
        leg=_leg(),
        snapshots=[eligible_old, future, wrong_source, exactly_at_cutoff, eligible_latest],
        as_of=_dt(D2, 9, 31),
    )
    assert selected.available
    assert selected.snapshot is eligible_latest

    not_yet_visible = select_mews_snapshot(
        leg=_leg(), snapshots=[eligible_latest], as_of=_dt(D1, 9, 38)
    )
    assert not not_yet_visible.available

    cutoff_only = select_mews_snapshot(
        leg=_leg(), snapshots=[exactly_at_cutoff], as_of=_dt(D2, 9, 31)
    )
    assert not cutoff_only.available


def test_invalid_mews_uses_default_threshold_and_records_alert_reason() -> None:
    invalid = _mews(snapshot_id=None)
    result = evaluate_exit(
        leg=_leg(),
        bars=[_bar(D2, 9, 31, 94), _bar(D2, 9, 32, 88)],
        as_of=_dt(D2, 9, 32),
        mews_snapshots=[invalid],
        d1_window_complete=True,
    )
    assert result.intent is not None
    assert result.intent.signal_type is ExitSignalType.D2_ENTRY_12
    assert "MEWS_INVALID" in result.intent.reason_codes

    malformed_state = _mews(fast_state="DANGER ")
    malformed_selection = select_mews_snapshot(
        leg=_leg(), snapshots=[malformed_state], as_of=_dt(D2, 9, 31)
    )
    assert not malformed_selection.available
    assert malformed_selection.reason == "MEWS_INVALID"


def test_d2_last_minute_protection_yields_to_1457_plan() -> None:
    result = evaluate_exit(
        leg=_leg(),
        bars=[_bar(D2, 14, 56, 1)],
        as_of=_dt(D2, 14, 57),
        d1_window_complete=True,
        d2_pre1457_window_complete=True,
    )
    assert result.intent is not None
    assert result.intent.signal_type is ExitSignalType.PLAN_1457
    assert result.intent.trigger_bar_end_ts is None
    assert result.intent.threshold_wealth_factor is None


def test_d2_1455_can_still_form_protection_before_plan() -> None:
    result = evaluate_exit(
        leg=_leg(),
        bars=[_bar(D2, 14, 55, 1)],
        as_of=_dt(D2, 14, 57),
        d1_window_complete=True,
        d2_pre1457_window_complete=True,
    )
    assert result.intent is not None
    assert result.intent.signal_type is ExitSignalType.D2_ENTRY_12
    assert result.intent.rule_actionable_from == _dt(D2, 14, 56)


def test_plan_survives_pre1457_data_gap_and_does_not_require_reference() -> None:
    unavailable_leg = _leg(reference_status=ReferenceStatus.UNAVAILABLE)
    waiting = evaluate_exit(
        leg=unavailable_leg,
        bars=[],
        as_of=_dt(D2, 14, 57),
        d1_window_complete=True,
        d2_pre1457_window_complete=False,
    )
    assert waiting.intent is not None
    assert waiting.intent.signal_type is ExitSignalType.PLAN_1457
    assert "D2_WINDOW_INCOMPLETE" in waiting.intent.reason_codes

    planned = evaluate_exit(
        leg=unavailable_leg,
        bars=[],
        as_of=_dt(D2, 14, 57),
        d1_window_complete=True,
        d2_pre1457_window_complete=True,
    )
    assert planned.intent is not None
    assert planned.intent.signal_type is ExitSignalType.PLAN_1457
    assert planned.intent.reference_entry_price is None
    assert "REFERENCE_UNAVAILABLE" in planned.intent.reason_codes


def test_plan_survives_both_d1_and_d2_window_gaps() -> None:
    result = evaluate_exit(
        leg=_leg(reference_status=ReferenceStatus.UNAVAILABLE),
        bars=[],
        as_of=_dt(D2, 14, 57),
        d1_window_complete=False,
        d2_pre1457_window_complete=False,
    )
    assert result.intent is not None
    assert result.intent.signal_type is ExitSignalType.PLAN_1457
    assert "D1_WINDOW_INCOMPLETE" in result.intent.reason_codes
    assert "D2_WINDOW_INCOMPLETE" in result.intent.reason_codes


def test_first_intent_is_immutable_and_must_match_same_leg() -> None:
    first = evaluate_exit(leg=_leg(), bars=[_bar(D1, 9, 31, 80)], as_of=_dt(D1, 9, 31)).intent
    assert first is not None
    repeated = evaluate_exit(
        leg=_leg(),
        bars=[_bar(D2, 9, 31, 1)],
        as_of=_dt(D2, 10, 0),
        existing_intent=first,
        d1_window_complete=True,
    )
    assert repeated.intent is first
    assert repeated.suppressed_reason == "FIRST_INTENT_ALREADY_FROZEN"

    with pytest.raises(ValueError, match="different model leg"):
        evaluate_exit(
            leg=_leg(model_batch_id="batch-b"),
            bars=[],
            as_of=_dt(D2, 10, 0),
            existing_intent=first,
        )


def test_same_stock_on_different_days_has_distinct_leg_and_intent_identity() -> None:
    first_leg = _leg(model_batch_id="decision-20260827")
    second_leg = _leg(model_batch_id="decision-20260828")
    assert first_leg.code == second_leg.code
    assert first_leg.model_leg_id != second_leg.model_leg_id

    first = evaluate_exit(leg=first_leg, bars=[_bar(D1, 9, 31, 80)], as_of=_dt(D1, 9, 31)).intent
    second = evaluate_exit(leg=second_leg, bars=[_bar(D1, 9, 31, 80)], as_of=_dt(D1, 9, 31)).intent
    assert first is not None and second is not None
    assert first.exit_intent_id != second.exit_intent_id


def test_cross_code_bar_is_rejected_instead_of_silently_mixed() -> None:
    with pytest.raises(ValueError, match="evaluated model leg code"):
        evaluate_exit(leg=_leg(), bars=[_bar(D1, 9, 31, 80, code="000002")], as_of=_dt(D1, 9, 31))
