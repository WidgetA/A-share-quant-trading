from datetime import date

import pytest

from src.strategy.v20.models import Rolling7Status, RollingBatch, RollingGap
from src.strategy.v20.policy import evaluate_rolling7
from src.strategy.v20.rolling7_market_health import (
    BatchStatus,
    CanonicalRecommendation,
    SignalKind,
    make_batch,
    make_missing_canonical_batch,
)


def _signal(day: int, batch_return: float = 0.01):
    signal_date = date(2026, 8, day)
    reference = 100.0
    return make_batch(
        signal_date=signal_date,
        canonical_snapshot_id=f"snapshot-{day:02d}",
        canonical_snapshot_hash=f"{day:064d}",
        recommendations=(CanonicalRecommendation(rank=1, code="000001"),),
        t2_date=date(2026, 8, day + 3),
        d0_references={"000001": reference},
        d2_closes={"000001": reference * (1.0 + batch_return)},
    )


def _evidence_gap(day: int, t2_day: int | None = None):
    signal_date = date(2026, 8, day)
    return make_batch(
        signal_date=signal_date,
        canonical_snapshot_id=f"snapshot-{day:02d}",
        canonical_snapshot_hash=f"{day:064d}",
        recommendations=(CanonicalRecommendation(rank=1, code="000001"),),
        t2_date=date(2026, 8, t2_day) if t2_day is not None else None,
    )


def _evaluate(*batches):
    complete = [
        RollingBatch(
            batch_id=batch.canonical_snapshot_id,
            signal_date=batch.signal_date,
            t2_exit_date=batch.t2_date,
            gross_price_return=batch.batch_return,
        )
        for batch in batches
        if batch.signal_kind is SignalKind.SIGNAL
        and batch.status is BatchStatus.COMPLETE
        and batch.t2_date is not None
        and batch.batch_return is not None
    ]
    gaps = [
        RollingGap(
            gap_id=f"rolling7:{batch.signal_date.isoformat()}",
            signal_date=batch.signal_date,
            gap_maturity_date=batch.t2_date,
        )
        for batch in batches
        if batch.status is BatchStatus.DATA_GAP and batch.t2_date is not None
    ]
    return evaluate_rolling7(
        decision_date=date(2026, 9, 1),
        complete_batches=complete,
        gaps=gaps,
    )


def test_equal_weight_batch_return_is_summed_not_averaged_across_batches():
    batches = [_signal(day, 0.01) for day in range(1, 8)]
    result = _evaluate(*batches)

    assert result.status is Rolling7Status.NON_BAD
    assert batches[0].batch_return == pytest.approx(0.01)
    assert result.r7 == pytest.approx(0.07)
    assert result.l7 == 0


def test_fewer_than_seven_batches_are_explicit_warmup_not_gap():
    result = _evaluate(*[_signal(day) for day in range(1, 4)])

    assert result.status is Rolling7Status.WARMUP
    assert result.unknown_reason == "WARMUP:3/7"
    assert result.r7 is None
    assert result.l7 is None


def test_mature_gap_blocks_until_seven_later_complete_signal_batches():
    gap = _evidence_gap(1, t2_day=4)
    six_later = [_signal(day) for day in range(2, 8)]
    blocked = _evaluate(gap, *six_later)
    displaced = _evaluate(gap, *six_later, _signal(8))

    assert blocked.status is Rolling7Status.DATA_GAP
    assert blocked.active_gap_ids == ("rolling7:2026-08-01",)
    assert displaced.status is Rolling7Status.NON_BAD
    assert [batch.signal_date.day for batch in displaced.window] == list(range(2, 9))


def test_bad_status_at_exact_five_loss_batch_boundary():
    losses = [_signal(day, -0.02) for day in range(1, 6)]
    gains = [_signal(day, 0.01) for day in range(6, 8)]
    result = _evaluate(*losses, *gains)

    assert result.status is Rolling7Status.BAD
    assert result.l7 == 5
    assert result.r7 == pytest.approx(-0.08)


def test_missing_placeholder_blocks_until_displaced_by_seven_complete_signals():
    placeholder = make_missing_canonical_batch(
        signal_date=date(2026, 8, 1), t2_date=date(2026, 8, 4)
    )
    six_later = [_signal(day) for day in range(2, 8)]
    blocked = _evaluate(placeholder, *six_later)
    displaced = _evaluate(placeholder, *six_later, _signal(8))

    assert blocked.status is Rolling7Status.DATA_GAP
    assert blocked.unknown_reason == "DATA_GAP:rolling7:2026-08-01"
    assert displaced.status is Rolling7Status.NON_BAD


def test_missing_canonical_can_be_replaced_by_same_date_complete_fact():
    placeholder = make_missing_canonical_batch(signal_date=date(2026, 8, 1), t2_date=None)
    assert placeholder.status is BatchStatus.DATA_GAP
    assert placeholder.signal_kind is SignalKind.MISSING_CANONICAL

    completed = _signal(1)
    result = _evaluate(completed, *(_signal(day) for day in range(2, 8)))
    assert result.status is Rolling7Status.NON_BAD


def test_invalid_evidence_gap_and_complete_keep_canonical_identity():
    gap = _evidence_gap(1, t2_day=4)
    completed = _signal(1)

    assert gap.status is BatchStatus.DATA_GAP
    assert gap.reason.startswith("MISSING_MARKET_EVIDENCE")
    assert completed.canonical_snapshot_id == gap.canonical_snapshot_id
    assert completed.canonical_snapshot_hash == gap.canonical_snapshot_hash
    assert completed.status is BatchStatus.COMPLETE


def test_empty_recommendations_are_durable_no_signal_and_excluded():
    no_signal = make_batch(
        signal_date=date(2026, 8, 1),
        canonical_snapshot_id="snapshot-01",
        canonical_snapshot_hash="1" * 64,
        recommendations=(),
        t2_date=date(2026, 8, 4),
    )

    assert no_signal.signal_kind is SignalKind.NO_SIGNAL
    assert no_signal.status is BatchStatus.COMPLETE
    assert no_signal.t2_date is None
    assert no_signal.batch_return is None
    assert _evaluate(no_signal).status is Rolling7Status.WARMUP
