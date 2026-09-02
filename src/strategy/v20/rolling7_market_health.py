from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import Enum
from math import isfinite

WINDOW_SIZE = 7
BAD_LOSS_BATCH_THRESHOLD = 5


class BatchStatus(str, Enum):
    COMPLETE = "COMPLETE"
    DATA_GAP = "DATA_GAP"


class SignalKind(str, Enum):
    SIGNAL = "SIGNAL"
    NO_SIGNAL = "NO_SIGNAL"
    MISSING_CANONICAL = "MISSING_CANONICAL"


@dataclass(frozen=True, slots=True)
class CanonicalRecommendation:
    rank: int
    code: str

    def __post_init__(self) -> None:
        code = self.code.strip()
        if len(code) != 6 or not code.isdigit():
            raise ValueError("canonical recommendation code is invalid")
        if type(self.rank) is not int or self.rank < 1:
            raise ValueError("canonical recommendation rank is invalid")
        object.__setattr__(self, "code", code)


@dataclass(frozen=True, slots=True)
class Rolling7Leg:
    rank: int
    code: str
    d0_reference: float | None
    d2_close: float | None


@dataclass(frozen=True, slots=True)
class Rolling7Batch:
    signal_date: date
    canonical_snapshot_id: str
    canonical_snapshot_hash: str
    canonical_available: bool
    signal_kind: SignalKind
    recommendations: tuple[CanonicalRecommendation, ...]
    t2_date: date | None
    legs: tuple[Rolling7Leg, ...]
    status: BatchStatus
    reason: str
    batch_return: float | None = None

    @property
    def signal(self) -> bool:
        return self.signal_kind is SignalKind.SIGNAL


def make_batch(
    *,
    signal_date: date,
    canonical_snapshot_id: str,
    canonical_snapshot_hash: str,
    recommendations: Sequence[CanonicalRecommendation],
    t2_date: date | None,
    d0_references: Mapping[str, float] | None = None,
    d2_closes: Mapping[str, float] | None = None,
) -> Rolling7Batch:
    ordered = tuple(sorted(recommendations, key=lambda item: (item.rank, item.code)))
    _validate_recommendations(ordered)
    if not ordered:
        return Rolling7Batch(
            signal_date=signal_date,
            canonical_snapshot_id=canonical_snapshot_id,
            canonical_snapshot_hash=canonical_snapshot_hash,
            canonical_available=True,
            signal_kind=SignalKind.NO_SIGNAL,
            recommendations=(),
            t2_date=None,
            legs=(),
            status=BatchStatus.COMPLETE,
            reason="NO_SIGNAL",
        )
    if t2_date is None or t2_date <= signal_date:
        return _gap_batch(
            signal_date=signal_date,
            canonical_snapshot_id=canonical_snapshot_id,
            canonical_snapshot_hash=canonical_snapshot_hash,
            canonical_available=True,
            signal_kind=SignalKind.SIGNAL,
            recommendations=ordered,
            t2_date=t2_date,
            reason="INVALID_T2_SESSION",
        )

    references = d0_references or {}
    closes = d2_closes or {}
    legs = tuple(
        Rolling7Leg(
            rank=item.rank,
            code=item.code,
            d0_reference=_positive_finite(references.get(item.code)),
            d2_close=_positive_finite(closes.get(item.code)),
        )
        for item in ordered
    )
    missing = [
        f"{item.code}:{'D0_REFERENCE' if item.d0_reference is None else 'D2_CLOSE'}"
        for item in legs
        if item.d0_reference is None or item.d2_close is None
    ]
    if missing:
        return _gap_batch(
            signal_date=signal_date,
            canonical_snapshot_id=canonical_snapshot_id,
            canonical_snapshot_hash=canonical_snapshot_hash,
            canonical_available=True,
            signal_kind=SignalKind.SIGNAL,
            recommendations=ordered,
            t2_date=t2_date,
            reason="MISSING_MARKET_EVIDENCE:" + ",".join(missing),
            legs=legs,
        )
    leg_returns: list[float] = []
    for leg in legs:
        if leg.d0_reference is None or leg.d2_close is None:
            raise AssertionError("complete Rolling7 evidence unexpectedly became incomplete")
        leg_returns.append(leg.d2_close / leg.d0_reference - 1.0)
    batch_return = sum(leg_returns) / len(leg_returns)
    if not isfinite(batch_return):
        return _gap_batch(
            signal_date=signal_date,
            canonical_snapshot_id=canonical_snapshot_id,
            canonical_snapshot_hash=canonical_snapshot_hash,
            canonical_available=True,
            signal_kind=SignalKind.SIGNAL,
            recommendations=ordered,
            t2_date=t2_date,
            reason="INVALID_BATCH_RETURN",
            legs=legs,
        )
    return Rolling7Batch(
        signal_date=signal_date,
        canonical_snapshot_id=canonical_snapshot_id,
        canonical_snapshot_hash=canonical_snapshot_hash,
        canonical_available=True,
        signal_kind=SignalKind.SIGNAL,
        recommendations=ordered,
        t2_date=t2_date,
        legs=legs,
        status=BatchStatus.COMPLETE,
        reason="COMPLETE",
        batch_return=batch_return,
    )


def _gap_batch(
    *,
    signal_date: date,
    canonical_snapshot_id: str,
    canonical_snapshot_hash: str,
    canonical_available: bool,
    signal_kind: SignalKind,
    recommendations: tuple[CanonicalRecommendation, ...],
    t2_date: date | None,
    reason: str,
    legs: tuple[Rolling7Leg, ...] = (),
) -> Rolling7Batch:
    return Rolling7Batch(
        signal_date=signal_date,
        canonical_snapshot_id=canonical_snapshot_id,
        canonical_snapshot_hash=canonical_snapshot_hash,
        canonical_available=canonical_available,
        signal_kind=signal_kind,
        recommendations=recommendations,
        t2_date=t2_date,
        legs=legs,
        status=BatchStatus.DATA_GAP,
        reason=reason,
    )


def _validate_recommendations(
    recommendations: tuple[CanonicalRecommendation, ...],
) -> None:
    ranks = [item.rank for item in recommendations]
    if ranks != list(range(1, len(recommendations) + 1)):
        raise ValueError("canonical recommendation ranks must be ordered and contiguous")
    codes = [item.code for item in recommendations]
    if len(set(codes)) != len(codes):
        raise ValueError("canonical recommendation codes must be unique")


def _positive_finite(value: float | None) -> float | None:
    if value is None or not isfinite(value) or value <= 0:
        return None
    return float(value)


__all__ = [
    "BAD_LOSS_BATCH_THRESHOLD",
    "BatchStatus",
    "CanonicalRecommendation",
    "Rolling7Batch",
    "Rolling7Leg",
    "SignalKind",
    "WINDOW_SIZE",
    "make_batch",
    "make_missing_canonical_batch",
]


def make_missing_canonical_batch(*, signal_date: date, t2_date: date | None) -> Rolling7Batch:
    return Rolling7Batch(
        signal_date=signal_date,
        canonical_snapshot_id="",
        canonical_snapshot_hash="",
        canonical_available=False,
        signal_kind=SignalKind.MISSING_CANONICAL,
        recommendations=(),
        t2_date=t2_date,
        legs=(),
        status=BatchStatus.DATA_GAP,
        reason="MISSING_CANONICAL",
    )
