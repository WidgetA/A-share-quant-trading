"""Validated market-history inputs for the V20 rolling-seven indicator.

These rows are derived exclusively from each day's complete V16 recommendation
batch and subsequent market prices. They are not positions, fills, orders, or
runtime-ledger bootstrap state.
"""

from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path
from typing import Any, Mapping

from src.strategy.v20.decision_engine import CompletedRolling

ROLLING7_HISTORY_RELATIVE_PATH = Path(
    "docs/strategy-v20-artifacts/rolling7-v16-market-history-v1.json"
)
ROLLING7_HISTORY_SCHEMA = "v20-rolling7-v16-market-history/v1"


class Rolling7HistoryError(ValueError):
    """The frozen V16/market-derived rolling history is malformed."""


def _exact_keys(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    if set(value) != expected:
        raise Rolling7HistoryError(
            f"{field} field set mismatch; "
            f"missing={sorted(expected - set(value))}, extra={sorted(set(value) - expected)}"
        )


def load_rolling7_market_history(
    project_root: Path,
    *,
    expected_return_profile_id: str,
    expected_reference_profile_id: str,
) -> tuple[CompletedRolling, ...]:
    """Load the checked-in V16 signal/market facts used across runtime restarts."""

    path = project_root / ROLLING7_HISTORY_RELATIVE_PATH
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Rolling7HistoryError(f"cannot load rolling7 market history: {path}") from exc
    if not isinstance(payload, Mapping):
        raise Rolling7HistoryError("rolling7 market history root must be an object")
    _exact_keys(
        payload,
        {
            "schema_version",
            "definition",
            "return_profile_id",
            "reference_profile_id",
            "source_evidence",
            "batches",
        },
        "rolling7 market history",
    )
    if payload["schema_version"] != ROLLING7_HISTORY_SCHEMA:
        raise Rolling7HistoryError("unsupported rolling7 market history schema")
    if payload["return_profile_id"] != expected_return_profile_id:
        raise Rolling7HistoryError("rolling7 market history return profile mismatch")
    if payload["reference_profile_id"] != expected_reference_profile_id:
        raise Rolling7HistoryError("rolling7 market history reference profile mismatch")
    rows = payload["batches"]
    if not isinstance(rows, list):
        raise Rolling7HistoryError("rolling7 market history batches must be a list")

    result: list[CompletedRolling] = []
    signal_dates: set[date] = set()
    batch_ids: set[str] = set()
    for index, raw in enumerate(rows):
        if not isinstance(raw, Mapping):
            raise Rolling7HistoryError(f"rolling7 batch {index} must be an object")
        _exact_keys(
            raw,
            {"batch_id", "signal_date", "t2_date", "batch_return"},
            f"batch {index}",
        )
        try:
            batch_id = str(raw["batch_id"])
            signal_date = date.fromisoformat(str(raw["signal_date"]))
            t2_date = date.fromisoformat(str(raw["t2_date"]))
            batch_return = float(raw["batch_return"])
        except (TypeError, ValueError) as exc:
            raise Rolling7HistoryError(f"rolling7 batch {index} has invalid values") from exc
        if not batch_id or batch_id in batch_ids:
            raise Rolling7HistoryError("rolling7 market history has duplicate/empty batch_id")
        if signal_date in signal_dates:
            raise Rolling7HistoryError("rolling7 market history has duplicate signal_date")
        if t2_date <= signal_date:
            raise Rolling7HistoryError("rolling7 market history t2_date must follow signal_date")
        if not math.isfinite(batch_return):
            raise Rolling7HistoryError("rolling7 market history return must be finite")
        batch_ids.add(batch_id)
        signal_dates.add(signal_date)
        result.append(CompletedRolling(batch_id, signal_date, t2_date, batch_return))

    if result != sorted(result, key=lambda row: (row.signal_date, row.batch_id)):
        raise Rolling7HistoryError("rolling7 market history batches must be sorted")
    return tuple(result)
