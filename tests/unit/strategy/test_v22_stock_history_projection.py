"""Contracts for the historical values consumed by V22 feature calculation."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from src.strategy.v22_slim.selection import build_stock

DAY = date(2026, 11, 2)
CALENDAR = tuple(DAY - timedelta(days=45 - index) for index in range(45))
FIELDS = ("open", "high", "low", "close", "volume")


def history():
    return {
        "time": [day.isoformat() for day in CALENDAR],
        "open": [10.0 + i / 10 for i in range(45)],
        "high": [11.0 + i / 10 for i in range(45)],
        "low": [9.0 + i / 10 for i in range(45)],
        "close": [10.5 + i / 10 for i in range(45)],
        "volume": [1000 + i for i in range(45)],
    }


def quote():
    return SimpleNamespace(
        open_price=15.0,
        early_close=15.1,
        early_high=15.2,
        early_low=14.9,
        early_volume=600.0,
        volume_937=350.0,
        amount=9000.0,
    )


def build(values=None, calendar=CALENDAR, day=DAY, current=None):
    return build_stock("600000", "test", current or quote(), values or history(), calendar, day)


def assert_projection(stock, expected):
    assert stock is not None
    pd.testing.assert_frame_equal(stock.history_df, expected, check_exact=True)
    values = expected.to_numpy(dtype=float)
    closes = values[:, 3]
    returns = (np.diff(closes) / closes[:-1])[-20:]
    assert stock.prev_close == float(closes[-1])
    assert stock.avg_daily_volume == float(values[:, 4].mean())
    assert stock.trend_5d == (float(closes[-1] / closes[-6] - 1) if len(closes) >= 6 else 0.0)
    assert stock.trend_10d == (float(closes[-1] / closes[-11] - 1) if len(closes) >= 11 else 0.0)
    assert stock.avg_daily_return_20d == float(returns.mean())
    assert stock.volatility_20d == (float(returns.std()) if len(returns) >= 2 else 0.0)
    consecutive = 0
    for previous, latest in zip(closes[-2::-1], closes[:0:-1], strict=True):
        if latest <= previous:
            break
        consecutive += 1
    assert stock.consecutive_up_days == consecutive
    assert stock.open_price == 15.0
    assert stock.price_940 == 15.1
    assert stock.high_940 == 15.2
    assert stock.low_940 == 14.9
    assert stock.volume_940 == 600.0
    assert stock.volume_937 == 350.0


def test_calendar_window_is_reused_across_stocks():
    class CountedDate(date):
        calls = 0

        def isoformat(self):
            CountedDate.calls += 1
            return super().isoformat()

    day = date(2033, 4, 8)
    calendar = tuple(CountedDate(2033, 2, 1) + timedelta(days=i) for i in range(45))
    values = history()
    values["time"] = [d.isoformat() for d in calendar]
    CountedDate.calls = 0
    first = build(values, calendar=calendar, day=day)
    second = build(values, calendar=calendar, day=day)
    assert first is not None and second is not None
    pd.testing.assert_frame_equal(first.history_df, second.history_df, check_exact=True)
    assert CountedDate.calls <= len(calendar)


def test_scrambled_history_preserves_exact_values_dtypes_and_metric_inputs():
    values = history()
    # Values outside the 37-session window influence the source column dtype,
    # but must not enter any feature or selected history row.
    values["volume"][0] = None
    expected = (
        pd.DataFrame({field: values[field] for field in FIELDS}).iloc[8:].reset_index(drop=True)
    )
    order = np.random.default_rng(22).permutation(45).tolist()
    scrambled = {key: [column[index] for index in order] for key, column in values.items()}
    assert_projection(build(scrambled), expected)


def test_series_columns_keep_the_existing_index_alignment_before_date_selection():
    values = history()
    indexed = {
        key: pd.Series(column, index=range(45) if key == "time" else range(44, -1, -1))
        for key, column in values.items()
    }
    # The aligned data remain valid OHLCV, but the prices now descend.
    expected = (
        pd.DataFrame({field: indexed[field] for field in FIELDS})
        .reindex(range(45))
        .iloc[8:]
        .reset_index(drop=True)
    )
    assert_projection(build(indexed), expected)


@pytest.mark.parametrize("time_kind", ["date", "datetime", "timestamp", "numpy"])
def test_history_date_representations_keep_the_existing_date_prefix_rule(time_kind):
    values = history()
    if time_kind == "date":
        values["time"] = list(CALENDAR)
    elif time_kind == "datetime":
        values["time"] = [datetime.combine(day, datetime.min.time()) for day in CALENDAR]
    elif time_kind == "timestamp":
        values["time"] = [pd.Timestamp(day, tz="Asia/Shanghai") for day in CALENDAR]
    else:
        values["time"] = [np.datetime64(day) for day in CALENDAR]
    expected = (
        pd.DataFrame({field: values[field] for field in FIELDS}).iloc[8:].reset_index(drop=True)
    )
    assert_projection(build(values), expected)


@pytest.mark.parametrize("value", [pd.NaT, "invalid", None])
def test_unselected_or_unparseable_date_rows_do_not_enter_history(value):
    values = history()
    values["time"][0] = value
    expected = (
        pd.DataFrame({field: values[field] for field in FIELDS}).iloc[8:].reset_index(drop=True)
    )
    assert_projection(build(values), expected)


def test_selected_duplicate_date_is_rejected_and_unselected_duplicates_are_ignored():
    values = history()
    values["time"][0] = values["time"][1]
    expected = (
        pd.DataFrame({field: values[field] for field in FIELDS}).iloc[8:].reset_index(drop=True)
    )
    assert_projection(build(values), expected)
    values["time"][-2] = values["time"][-1]
    with pytest.raises(ValueError, match="duplicate historical daily bar"):
        build(values)


def test_last_completed_session_and_at_least_five_rows_are_required():
    values = history()
    values["time"][-1] = "unavailable"
    assert build(values) is None
    values = {key: column[-4:] for key, column in history().items()}
    assert build(values) is None
    values = {key: column[-5:] for key, column in history().items()}
    expected = pd.DataFrame({field: values[field] for field in FIELDS})
    assert_projection(build(values), expected)


@pytest.mark.parametrize("field", FIELDS)
@pytest.mark.parametrize("value", [None, float("nan"), float("inf"), float("-inf")])
def test_nonfinite_selected_ohlcv_is_unusable(field, value):
    values = history()
    values[field][-1] = value
    assert build(values) is None


@pytest.mark.parametrize(
    "field,value",
    [("open", 0), ("close", -1), ("high", 1), ("low", 20), ("volume", -1)],
)
def test_illegal_ohlcv_is_unusable(field, value):
    values = history()
    values[field][-1] = value
    assert build(values) is None


def test_zero_volume_row_is_retained_but_zero_average_or_current_flow_is_unusable():
    values = history()
    values["volume"][-2] = 0
    expected = (
        pd.DataFrame({field: values[field] for field in FIELDS}).iloc[8:].reset_index(drop=True)
    )
    assert_projection(build(values), expected)
    values["volume"] = [0] * 45
    assert build(values) is None
    current = quote()
    current.early_volume = 0
    assert build(current=current) is None
    current = quote()
    current.amount = 0
    assert build(current=current) is None


def test_missing_empty_columns_and_mismatched_columns_keep_the_existing_errors():
    with pytest.raises(KeyError, match="open"):
        build_stock("600000", "test", quote(), {"time": []}, CALENDAR, DAY)
    values = history()
    values["open"].pop()
    with pytest.raises(ValueError, match="unequal history columns"):
        build(values)
    with pytest.raises(ValueError, match="requires 37 previous exchange sessions"):
        build(calendar=CALENDAR[-36:])
