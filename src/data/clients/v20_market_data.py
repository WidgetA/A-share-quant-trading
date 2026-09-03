"""Point-in-time market-data primitives for the V20 decision service.

The legacy V16 adapter aggregates whatever minute rows happen to be present.
V20 instead retains every raw end label, rejects conflicting revisions, and
will only produce an entry snapshot when the exact 09:39 row exists.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from math import isfinite
from typing import Iterable, Mapping

from src.data.clients.tushare_realtime import BEIJING_TZ, TushareMinuteBar, TushareQuote

ENTRY_END_LABEL = "09:39"
REFERENCE_END_LABEL = "09:41"
REFERENCE_PROFILE_ID = "CALENDAR_0940_OPEN_END_LABEL_0941_V1"
# V16's live ``rt_min_daily`` aggregation includes both call-auction rows
# before continuous trading when the provider returns them. V20 owns this
# frozen copy of the selection window so a later V20 change cannot alter V16.
ENTRY_AUCTION_LABELS = ("09:25", "09:30")
ENTRY_CONTINUOUS_LABELS = tuple(f"09:{minute:02d}" for minute in range(31, 40))
ENTRY_SELECTION_LABELS = ENTRY_AUCTION_LABELS + ENTRY_CONTINUOUS_LABELS
# Exact readiness remains aligned with V16: auction rows are consumed when
# present, while 09:39 is the terminal gate. This collector additionally
# requires the continuous path because it calculates cumulative minute volume.
ENTRY_REQUIRED_LABELS = ENTRY_CONTINUOUS_LABELS


class V20MarketDataError(RuntimeError):
    """A point-in-time market snapshot cannot be proven safe to use."""


@dataclass(frozen=True)
class ExactEarlySnapshot:
    trade_date: date
    last_complete_label: str
    quotes: Mapping[str, TushareQuote]
    missing_codes: tuple[str, ...]
    conflict_codes: tuple[str, ...]
    source_hash: str

    @property
    def coverage(self) -> float:
        total = len(self.quotes) + len(self.missing_codes)
        return len(self.quotes) / total if total else 0.0


def _canonical_hash(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _bar_semantics(bar: TushareMinuteBar) -> dict[str, object]:
    return {
        "stock_code": bar.stock_code,
        "bar_end": bar.bar_end.isoformat(),
        "end_label": bar.end_label,
        "open": bar.open_price,
        "close": bar.close_price,
        "high": bar.high_price,
        "low": bar.low_price,
        "volume": bar.volume,
        "amount": bar.amount,
    }


def _bar_semantics_sort_key(row: Mapping[str, object]) -> tuple[str, str]:
    return str(row["stock_code"]), str(row["bar_end"])


class V20EarlyBarCollector:
    """Collect raw morning bars and freeze an exact-label V16 input snapshot.

    The service may seed this collector with per-stock ``rt_min_daily`` rows
    during prewarm and then append the market-wide batch ``rt_min`` rows for
    the remaining minutes.  A second, different row for the same code/label
    invalidates that code's path; it cannot poison otherwise valid siblings.
    """

    def __init__(self, trade_date: date, required_codes: Iterable[str]) -> None:
        normalized = tuple(sorted(set(required_codes)))
        if not normalized:
            raise ValueError("required_codes cannot be empty")
        invalid = [code for code in normalized if len(code) != 6 or not code.isdigit()]
        if invalid:
            raise ValueError(f"required_codes contain invalid bare codes: {invalid[:5]}")
        self.trade_date = trade_date
        self.required_codes = normalized
        self._required = frozenset(normalized)
        self._bars: dict[tuple[str, str], TushareMinuteBar] = {}
        self._conflicts: set[tuple[str, str]] = set()

    def ingest(self, bars: Iterable[TushareMinuteBar]) -> None:
        for bar in bars:
            if bar.stock_code not in self._required:
                continue
            if not bar.is_valid:
                raise V20MarketDataError(
                    f"invalid minute bar for {bar.stock_code} at {bar.end_label}"
                )
            local_date = bar.bar_end.astimezone(BEIJING_TZ).date()
            if local_date != self.trade_date:
                raise V20MarketDataError(f"bar date mismatch for {bar.stock_code}: {local_date}")
            key = (bar.stock_code, bar.end_label)
            previous = self._bars.get(key)
            if previous is not None and previous != bar:
                self._conflicts.add(key)
                continue
            self._bars[key] = bar

    def ingest_by_code(self, rows: Mapping[str, Iterable[TushareMinuteBar]]) -> None:
        for code, bars in rows.items():
            for bar in bars:
                if bar.stock_code != code:
                    raise V20MarketDataError(
                        f"minute-history identity mismatch: key={code}, row={bar.stock_code}"
                    )
                self.ingest((bar,))

    def incomplete_codes(
        self,
        *,
        required_labels: Iterable[str] = ENTRY_REQUIRED_LABELS,
    ) -> tuple[str, ...]:
        """Return codes whose exact early-minute path is not fully proven.

        V16 consumes call-auction rows when present and uses the 09:39 price
        plus accumulated early volume. Merely having the terminal bar would
        make a dropped continuous-trading minute silently change those volume
        features, so this collector requires 09:31 through 09:39 and preserves
        09:25/09:30 in the frozen selection input.
        """

        labels = tuple(required_labels)
        if labels != ENTRY_REQUIRED_LABELS:
            raise ValueError("V20 entry path requires raw labels 09:31 through 09:39")
        return tuple(
            code
            for code in self.required_codes
            if any((code, label) not in self._bars for label in labels)
            or any((code, label) in self._conflicts for label in labels)
        )

    def codes_with_label(self, label: str) -> tuple[str, ...]:
        """Return non-conflicting required codes observed at one exact label."""

        return tuple(
            code
            for code in self.required_codes
            if (code, label) in self._bars and (code, label) not in self._conflicts
        )

    def complete_codes(self) -> tuple[str, ...]:
        incomplete = frozenset(self.incomplete_codes())
        return tuple(code for code in self.required_codes if code not in incomplete)

    def freeze(self, *, expected_label: str = ENTRY_END_LABEL) -> ExactEarlySnapshot:
        if expected_label != ENTRY_END_LABEL:
            raise ValueError(f"V20 entry snapshot requires raw label {ENTRY_END_LABEL}")
        quotes: dict[str, TushareQuote] = {}
        missing: list[str] = []
        selected_semantics: list[dict[str, object]] = []
        incomplete = frozenset(self.incomplete_codes())
        for code in self.required_codes:
            exact = self._bars.get((code, expected_label))
            if exact is None or code in incomplete:
                missing.append(code)
                continue
            morning = sorted(
                (
                    bar
                    for (bar_code, label), bar in self._bars.items()
                    if bar_code == code and label in ENTRY_SELECTION_LABELS
                ),
                key=lambda bar: bar.bar_end,
            )
            if not morning or morning[-1].end_label != expected_label:
                missing.append(code)
                continue
            first = morning[0]
            total_volume = sum(bar.volume for bar in morning)
            total_amount = sum(bar.amount for bar in morning)
            volume_937 = sum(bar.volume for bar in morning if bar.end_label <= "09:37")
            values = (
                first.open_price,
                exact.close_price,
                total_volume,
                total_amount,
                volume_937,
            )
            if not all(isfinite(value) and value > 0 for value in values):
                missing.append(code)
                continue
            quotes[code] = TushareQuote(
                stock_code=code,
                open_price=first.open_price,
                latest_price=exact.close_price,
                high_price=max(bar.high_price for bar in morning),
                low_price=min(bar.low_price for bar in morning),
                volume=total_volume,
                amount=total_amount,
                early_close=exact.close_price,
                early_high=max(bar.high_price for bar in morning),
                early_low=min(bar.low_price for bar in morning),
                early_volume=total_volume,
                volume_937=volume_937,
            )
            selected_semantics.extend(_bar_semantics(bar) for bar in morning)

        sorted_semantics: list[dict[str, object]] = sorted(
            selected_semantics,
            key=_bar_semantics_sort_key,
        )
        source_payload: dict[str, object] = {
            "profile": "V20_EXACT_END_LABEL_EARLY_SNAPSHOT_V1",
            "trade_date": self.trade_date.isoformat(),
            "expected_label": expected_label,
            "required_codes": self.required_codes,
            "missing_codes": sorted(missing),
            "conflict_codes": sorted({code for code, _label in self._conflicts}),
            "bars": sorted_semantics,
        }
        source_hash = _canonical_hash(source_payload)
        return ExactEarlySnapshot(
            trade_date=self.trade_date,
            last_complete_label=expected_label,
            quotes=quotes,
            missing_codes=tuple(sorted(missing)),
            conflict_codes=tuple(sorted({code for code, _label in self._conflicts})),
            source_hash=source_hash,
        )

    def freeze_terminal(self, *, expected_label: str = ENTRY_END_LABEL) -> ExactEarlySnapshot:
        """Freeze an exact terminal-price snapshot without V16 volume-path rules.

        This profile is intentionally limited to market breadth, whose frozen
        definition is the raw 09:39 close versus the previous close.  It must
        not inherit V16's separate 09:31..09:39 accumulated-volume requirement.
        """

        if expected_label != ENTRY_END_LABEL:
            raise ValueError(f"V20 terminal snapshot requires raw label {ENTRY_END_LABEL}")
        quotes: dict[str, TushareQuote] = {}
        missing: list[str] = []
        selected_semantics: list[dict[str, object]] = []
        terminal_conflicts = {code for code, label in self._conflicts if label == expected_label}
        for code in self.required_codes:
            exact = self._bars.get((code, expected_label))
            if exact is None or code in terminal_conflicts:
                missing.append(code)
                continue
            quotes[code] = TushareQuote(
                stock_code=code,
                open_price=exact.open_price,
                latest_price=exact.close_price,
                high_price=exact.high_price,
                low_price=exact.low_price,
                volume=exact.volume,
                amount=exact.amount,
                early_close=exact.close_price,
                early_high=exact.high_price,
                early_low=exact.low_price,
                early_volume=exact.volume,
                volume_937=0.0,
            )
            selected_semantics.append(_bar_semantics(exact))

        source_payload: dict[str, object] = {
            "profile": "V20_EXACT_END_LABEL_BREADTH_SNAPSHOT_V1",
            "trade_date": self.trade_date.isoformat(),
            "expected_label": expected_label,
            "required_codes": self.required_codes,
            "missing_codes": sorted(missing),
            "conflict_codes": sorted(terminal_conflicts),
            "bars": sorted(selected_semantics, key=_bar_semantics_sort_key),
        }
        return ExactEarlySnapshot(
            trade_date=self.trade_date,
            last_complete_label=expected_label,
            quotes=quotes,
            missing_codes=tuple(sorted(missing)),
            conflict_codes=tuple(sorted(terminal_conflicts)),
            source_hash=_canonical_hash(source_payload),
        )


def exact_reference_prices(
    bars: Mapping[str, TushareMinuteBar],
    required_codes: Iterable[str],
    *,
    trade_date: date,
    expected_label: str = REFERENCE_END_LABEL,
) -> tuple[dict[str, float], tuple[str, ...], str]:
    """Validate the calendar-09:40 reference profile (raw 09:41 open)."""
    if expected_label != REFERENCE_END_LABEL:
        raise ValueError(f"V20 reference profile requires raw label {REFERENCE_END_LABEL}")
    prices: dict[str, float] = {}
    missing: list[str] = []
    semantics: list[dict[str, object]] = []
    for code in sorted(set(required_codes)):
        bar = bars.get(code)
        if (
            bar is None
            or bar.stock_code != code
            or not bar.is_valid
            or bar.bar_end.astimezone(BEIJING_TZ).date() != trade_date
            or bar.end_label != expected_label
            or not isfinite(bar.open_price)
            or bar.open_price <= 0
            or not isfinite(bar.volume)
            or bar.volume <= 0
            or not isfinite(bar.amount)
            or bar.amount <= 0
        ):
            missing.append(code)
            continue
        prices[code] = bar.open_price
        semantics.append(_bar_semantics(bar))
    source_hash = _canonical_hash(
        {
            "profile": REFERENCE_PROFILE_ID,
            "trade_date": trade_date.isoformat(),
            "expected_label": expected_label,
            "missing_codes": missing,
            "bars": semantics,
        }
    )
    return prices, tuple(missing), source_hash


__all__ = [
    "ExactEarlySnapshot",
    "ENTRY_END_LABEL",
    "ENTRY_REQUIRED_LABELS",
    "REFERENCE_END_LABEL",
    "REFERENCE_PROFILE_ID",
    "V20EarlyBarCollector",
    "V20MarketDataError",
    "exact_reference_prices",
]
