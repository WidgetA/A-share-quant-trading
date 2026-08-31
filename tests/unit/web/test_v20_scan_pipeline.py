from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.data.clients.tushare_realtime import TushareQuote
from src.data.clients.v20_market_data import ExactEarlySnapshot
from src.strategy.strategies.v16_scanner import V16ScanResult
from src.strategy.v20.models import V20_V16_SNAPSHOT_SCHEMA
from src.web.v15_scan_service import LOOKBACK_DAYS, V15ScanState
from src.web.v20_scan_pipeline import (
    V20ScanPipeline,
    V20ScanPipelineError,
    _history_date_coverage,
    _normalize_history,
)


def _trade_days(end: date, count: int) -> list[date]:
    result: list[date] = []
    cursor = end - timedelta(days=1)
    while len(result) < count:
        if cursor.weekday() < 5:
            result.append(cursor)
        cursor -= timedelta(days=1)
    return sorted(result)


def _history(days: list[date]) -> dict[str, list[object]]:
    size = len(days)
    return {
        "time": [item.isoformat() for item in days],
        "open": [10.0] * size,
        "high": [10.5] * size,
        "low": [9.5] * size,
        "close": [10.2] * size,
        "volume": [100_000.0] * size,
    }


def test_history_requires_37_rows_and_freezes_only_the_latest_37() -> None:
    trade_date = date(2026, 8, 31)
    days = _trade_days(trade_date, LOOKBACK_DAYS + 1)
    allowed = frozenset(days)

    assert (
        _normalize_history(
            _history(days[:-2]),
            trade_date=trade_date,
            allowed_dates=allowed,
            required_dates=days[-LOOKBACK_DAYS:],
        )
        is None
    )
    normalized = _normalize_history(
        _history(days),
        trade_date=trade_date,
        allowed_dates=allowed,
        required_dates=days[-LOOKBACK_DAYS:],
    )

    assert normalized is not None
    assert len(normalized["time"]) == LOOKBACK_DAYS
    assert normalized["time"] == [item.isoformat() for item in days[-LOOKBACK_DAYS:]]


@pytest.mark.parametrize(
    "mutation",
    [
        "wrong_length",
        "duplicate_date",
        "out_of_order",
        "non_exchange_date",
        "d0_date",
        "nan",
        "infinite",
        "negative_volume",
        "zero_volume",
        "low_above_open",
        "high_below_close",
        "boolean_price",
        "boolean_volume",
    ],
)
def test_malformed_history_is_never_model_eligible(mutation: str) -> None:
    trade_date = date(2026, 8, 31)
    days = _trade_days(trade_date, LOOKBACK_DAYS)
    raw = _history(days)
    allowed = frozenset(days)

    if mutation == "wrong_length":
        raw["close"].pop()
    elif mutation == "duplicate_date":
        raw["time"][-1] = raw["time"][-2]
    elif mutation == "out_of_order":
        raw["time"][-2], raw["time"][-1] = raw["time"][-1], raw["time"][-2]
    elif mutation == "non_exchange_date":
        raw["time"][-1] = (trade_date - timedelta(days=1)).isoformat()
    elif mutation == "d0_date":
        raw["time"][-1] = trade_date.isoformat()
    elif mutation == "nan":
        raw["close"][-1] = float("nan")
    elif mutation == "infinite":
        raw["high"][-1] = float("inf")
    elif mutation == "negative_volume":
        raw["volume"][-1] = -1.0
    elif mutation == "zero_volume":
        raw["volume"][-1] = 0.0
    elif mutation == "low_above_open":
        raw["low"][-1] = 10.1
    elif mutation == "high_below_close":
        raw["high"][-1] = 10.1
    elif mutation == "boolean_price":
        raw["close"][-1] = True
    elif mutation == "boolean_volume":
        raw["volume"][-1] = False

    assert _normalize_history(raw, trade_date=trade_date, allowed_dates=allowed) is None


def test_missing_exchange_day_cannot_be_replaced_by_an_older_bar() -> None:
    trade_date = date(2026, 8, 31)
    exchange_days = _trade_days(trade_date, LOOKBACK_DAYS + 1)
    suspended_day = exchange_days[-5]
    actual_days = [item for item in exchange_days if item != suspended_day]

    normalized = _normalize_history(
        _history(actual_days),
        trade_date=trade_date,
        allowed_dates=frozenset(exchange_days),
        required_dates=exchange_days[-LOOKBACK_DAYS:],
    )

    assert normalized is None


def test_daily_history_source_coverage_uses_the_full_universe_denominator() -> None:
    trade_date = date(2026, 8, 31)
    days = _trade_days(trade_date, LOOKBACK_DAYS)
    normalized = _normalize_history(
        _history(days), trade_date=trade_date, allowed_dates=frozenset(days)
    )
    assert normalized is not None
    four_of_five = {f"00000{index}": deepcopy(normalized) for index in range(4)}
    three_of_five = dict(list(four_of_five.items())[:3])

    counts, coverage = _history_date_coverage(four_of_five, days, universe_size=5)
    _, below = _history_date_coverage(three_of_five, days, universe_size=5)

    assert set(counts.values()) == {4}
    assert coverage == 0.8
    assert below == 0.6


async def test_incomplete_history_never_reaches_the_v16_scanner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.web.v20_scan_pipeline as module

    trade_date = date(2026, 8, 31)
    expected_days = _trade_days(trade_date, LOOKBACK_DAYS)
    universe = tuple(f"00000{index}" for index in range(1, 6))
    incomplete_code = universe[-1]
    raw_history = {code: _history(expected_days) for code in universe[:-1]}
    raw_history[incomplete_code] = _history(expected_days[-5:])

    class _Scorer:
        model_sha256 = "a" * 64
        feature_list_sha256 = "b" * 64

        def __init__(self, *_args) -> None:
            pass

    class _Scanner:
        instances: list["_Scanner"] = []

        def __init__(self, **_kwargs) -> None:
            self.seen_codes: set[str] | None = None
            self.instances.append(self)

        def get_universe(self):
            return {}, set(universe)

        async def scan(self, stock_data, _clean_boards):
            self.seen_codes = set(stock_data)
            return V16ScanResult(step0_universe_count=len(universe))

    async def fetch_history(_adapter, _codes, _trade_date):
        return raw_history

    async def fetch_prev(_state, _trade_date, _calendar):
        return {code: 10.0 for code in universe}

    class _Realtime:
        async def fetch_daily_bars(self, _trade_date):
            return {}

    monkeypatch.setattr(module, "LGBRankScorer", _Scorer)
    monkeypatch.setattr(module, "V16Scanner", _Scanner)
    monkeypatch.setattr(module, "_fetch_history_ohlcv", fetch_history)
    monkeypatch.setattr(module, "_fetch_prev_closes", fetch_prev)
    state = V15ScanState(
        initialized=True,
        realtime_client=_Realtime(),
        historical_adapter=SimpleNamespace(),
    )
    pipeline = V20ScanPipeline(state, Path("."))
    calendar = tuple([*expected_days, trade_date, trade_date + timedelta(days=1)])
    prewarmed = await pipeline.prewarm(trade_date, calendar=calendar)
    quotes = {
        code: TushareQuote(
            stock_code=code,
            open_price=10.0,
            latest_price=10.1,
            high_price=10.2,
            low_price=9.9,
            volume=100_000.0,
            amount=1_000_000.0,
            early_close=10.1,
            early_high=10.2,
            early_low=9.9,
            early_volume=10_000.0,
            volume_937=8_000.0,
        )
        for code in universe
    }
    early = ExactEarlySnapshot(
        trade_date=trade_date,
        last_complete_label="09:39",
        quotes=quotes,
        missing_codes=(),
        conflict_codes=(),
        source_hash="c" * 64,
    )

    bundle = await pipeline.scan(
        prewarmed,
        early,
        breadth_early=early,
        minimum_quote_coverage=0.8,
    )

    assert _Scanner.instances[-1].seen_codes == set(universe[:-1])
    assert bundle.snapshot["schema_version"] == V20_V16_SNAPSHOT_SCHEMA
    assert incomplete_code not in bundle.snapshot["scan_input_codes"]
    assert incomplete_code in bundle.snapshot["scan_input_failure_codes"]
    assert bundle.snapshot["history_profile_id"] == "STRICT_LAST_37_EXCHANGE_SESSIONS_V1"
    assert set(bundle.snapshot["history_input_hashes"]) == set(universe[:-1])
    assert len(bundle.snapshot["history_date_valid_counts"]) == LOOKBACK_DAYS
    assert set(bundle.snapshot["history_date_valid_counts"].values()) == {4}
    assert bundle.snapshot["history_min_date_coverage"] == 0.8
    assert bundle.snapshot["funnel"] == {
        "step0_universe_count": len(universe),
        "step2_hot_board_count": 0,
        "final_candidates": 0,
    }
    assert bundle.snapshot["board_avg_gains"] == {}

    original_snapshot_hash = bundle.snapshot_hash
    raw_history[universe[0]]["close"][-1] = 10.3
    changed = await pipeline.prewarm(trade_date, calendar=calendar)
    changed_bundle = await pipeline.scan(
        changed,
        early,
        breadth_early=early,
        minimum_quote_coverage=0.8,
    )
    assert changed_bundle.snapshot_hash != original_snapshot_hash

    del raw_history[universe[1]]
    with pytest.raises(V20ScanPipelineError, match="below 80%"):
        await pipeline.prewarm(trade_date, calendar=calendar)
