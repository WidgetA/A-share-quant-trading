"""Malformed price data is an error; existing eligibility skips stay unchanged."""

from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from src.strategy.v22_slim.selection import build_stock


def _inputs():
    calendar = tuple(date(2026, 9, 1) + timedelta(days=index) for index in range(40))
    history = {
        "time": [str(day) for day in calendar[:-1]],
        "open": [20.0] * 39,
        "high": [21.0] * 39,
        "low": [19.0] * 39,
        "close": [20.0] * 39,
        "volume": [1000.0] * 39,
    }
    quote = SimpleNamespace(
        early_volume=100.0,
        amount=2000.0,
        open_price=20.0,
        early_close=20.2,
        early_high=20.3,
        early_low=20.0,
        volume_937=70.0,
    )
    return calendar, history, quote


@pytest.mark.parametrize(
    "field,value",
    [
        ("close", float("nan")),
        ("high", float("inf")),
        ("close", 0.0),
        ("open", -1.0),
        ("volume", -1.0),
        ("high", 10.0),
        ("low", 30.0),
    ],
)
def test_invalid_history_prices_are_errors_not_eligibility_skips(field, value):
    calendar, history, quote = _inputs()
    history[field][-1] = value
    with pytest.raises(RuntimeError, match="invalid historical OHLCV"):
        build_stock("600000", "sample", quote, history, calendar, calendar[-1])


@pytest.mark.parametrize("kind", ["short_history", "missing_latest_day", "zero_volume"])
def test_existing_history_eligibility_skips_are_preserved(kind):
    calendar, history, quote = _inputs()
    if kind == "short_history":
        history = {field: values[-4:] for field, values in history.items()}
    elif kind == "missing_latest_day":
        history = {field: values[:-1] for field, values in history.items()}
    else:
        history["volume"] = [0.0] * len(history["volume"])
    assert build_stock("600000", "sample", quote, history, calendar, calendar[-1]) is None


def test_valid_history_still_builds_stock():
    calendar, history, quote = _inputs()
    assert build_stock("600000", "sample", quote, history, calendar, calendar[-1]) is not None
