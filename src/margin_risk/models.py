"""Small typed values shared by ingestion, calculation, API and backtest."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from enum import StrEnum
from typing import Any


class RiskState(StrEnum):
    NORMAL = "NORMAL"
    WATCH = "WATCH"
    WARNING = "WARNING"
    DANGER = "DANGER"


class DataStatus(StrEnum):
    OK = "OK"
    PARTIAL = "PARTIAL"
    STALE = "STALE"
    FAILED = "FAILED"


@dataclass(slots=True)
class RiskObservation:
    trade_date: date
    mews_percentile: float | None
    confirmation_percentile: float | None
    dlb: float | None
    rpp: float | None
    data_status: DataStatus = DataStatus.OK


@dataclass(slots=True)
class QualityIssue:
    trade_date: date
    issue_key: str
    rule_name: str
    severity: str
    detail: str
    ts_code: str | None = None
    expected_value: float | None = None
    actual_value: float | None = None
    error_value: float | None = None
    tolerance: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
