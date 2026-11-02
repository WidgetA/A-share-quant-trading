"""Preserve selection inputs while preparing the entire history pool once."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from src.web.v20_canonical_selection import (
    _canonical_json_value,
    _fetch_history_ohlcv,
    _normalize_history_inputs,
)


async def test_history_prepares_all_codes_in_one_adapter_call_without_losing_rows() -> None:
    day = date(2026, 8, 31)
    codes = [f"{value:06}" for value in range(1, 132)] + ["600000"]
    calls: list[dict] = []

    class Adapter:
        async def history_quotes(self, **kwargs):
            calls.append(kwargs)
            return {
                "tables": [
                    {
                        "thscode": code,
                        "table": {
                            "time": ["2026-08-27", "2026-08-28"],
                            "open": [base, base + 1],
                            "high": [base + 2, base + 3],
                            "low": [base - 1, base],
                            "close": [base + 1, base + 2],
                            "volume": [base * 100, base * 200],
                        },
                    }
                    for code in kwargs["codes"].split(",")
                    for base in [float(int(code[:6])) + 1]
                ]
            }

    result = await _fetch_history_ohlcv(Adapter(), codes, day)

    assert len(calls) == 1, "the full history pool must not restart date acquisition per 50 codes"
    call = calls[0]
    assert call == {
        "codes": ",".join(code + (".SH" if code.startswith("6") else ".SZ") for code in codes),
        "indicators": "open,high,low,close,volume",
        "start_date": (day - timedelta(days=89)).isoformat(),
        "end_date": (day - timedelta(days=1)).isoformat(),
    }
    assert list(result) == codes
    for code, history in result.items():
        base = float(int(code)) + 1
        assert history == {
            "time": ["2026-08-27", "2026-08-28"],
            "open": [base, base + 1],
            "high": [base + 2, base + 3],
            "low": [base - 1, base],
            "close": [base + 1, base + 2],
            "volume": [base * 100, base * 200],
        }


def _history(times: list) -> dict:
    return {
        "time": times,
        "open": [10] * len(times),
        "high": [12.0] * len(times),
        "low": [9.0] * len(times),
        "close": [11.0] * len(times),
        "volume": [1000.0] * len(times),
    }


def test_history_date_preparation_preserves_local_days_for_all_supported_types() -> None:
    times = [
        "2026-08-31",
        "2026-08-30T18:30:00Z",
        pd.Timestamp("2026-08-31 03:00"),
        pd.Timestamp("2026-08-30T18:30:00Z"),
        datetime(2026, 8, 30, 17, tzinfo=timezone.utc),
        datetime(2026, 8, 30, 23, 30),
        date(2026, 8, 31),
        np.datetime64("2026-08-30T18:30"),
    ]
    raw = {"600000": _history(times), "000001": _history(times)}
    before = list(times)

    first = _normalize_history_inputs(raw)
    second = _normalize_history_inputs(raw)

    assert list(first) == ["000001", "600000"]
    assert first == second
    for result in first.values():
        assert result["time"] == [
            "2026-08-31",
            "2026-08-31",
            "2026-08-31",
            "2026-08-31",
            "2026-08-31",
            "2026-08-30",
            "2026-08-31",
            "2026-08-30",
        ]
        assert result["open"] == [10.0] * len(times)
        assert all(type(value) is float for value in result["open"])
    assert times == before
    assert raw["600000"]["time"] is times


@pytest.mark.parametrize("bad", [pd.NaT, np.datetime64("NaT"), "NaT"])
def test_history_nat_is_rejected_even_after_successful_date_preparation(bad) -> None:
    _normalize_history_inputs({"600000": _history(["2026-08-31"])})
    with pytest.raises(ValueError, match="600001 history input normalization failed: NaT time"):
        _normalize_history_inputs({"600001": _history([bad])})


@pytest.mark.parametrize(
    ("bad", "reason"),
    [
        ("not-a-date", "invalid time"),
        ("2026-02-30", "invalid time"),
        (123, "invalid time type"),
        (None, "invalid time type"),
    ],
)
def test_history_bad_date_reports_the_current_stock_code(bad, reason) -> None:
    for code in ["000001", "600000"]:
        with pytest.raises(
            ValueError, match=f"{code} history input normalization failed: {reason}"
        ):
            _normalize_history_inputs({code: _history([bad])})


def test_history_preparation_keeps_length_and_number_validation() -> None:
    bad_lengths = _history(["not-a-date", "2026-08-31"])
    bad_lengths["close"] = [11.0]
    with pytest.raises(ValueError, match="mismatched lengths"):
        _normalize_history_inputs({"000001": bad_lengths})
    for bad in [True, float("nan"), float("inf"), "bad-number"]:
        raw = _history(["2026-08-31"])
        raw["volume"] = [bad]
        with pytest.raises(ValueError, match="000001 history input normalization failed"):
            _normalize_history_inputs({"000001": raw})
    raw = _history(["2026-08-31"])
    raw["volume"] = [None]
    assert _normalize_history_inputs({"000001": raw})["000001"]["volume"] == [None]
    assert _normalize_history_inputs({"000001": None})["000001"] == {
        name: [] for name in ["time", "open", "high", "low", "close", "volume"]
    }


class _IntChild(int):
    pass


class _FloatChild(float):
    pass


class _ListChild(list):
    pass


class _DictChild(dict):
    pass


def test_json_fast_types_and_special_types_preserve_existing_values_and_order() -> None:
    raw = {
        "z": [None, True, 3, 2.5, "text", b"text"],
        2: (
            np.int64(4),
            np.float32(1.5),
            np.bool_(False),
            _IntChild(7),
            _FloatChild(0.25),
            _ListChild([8]),
            _DictChild(x=9),
        ),
        "a": date(2026, 8, 31),
    }

    actual = _canonical_json_value(raw)

    assert list(actual) == ["z", "2", "a"]
    assert actual == {
        "z": [None, True, 3, 2.5, "text", "text"],
        "2": [4, 1.5, False, 7, 0.25, [8], {"x": 9}],
        "a": "2026-08-31",
    }
    assert type(actual["2"][0]) is int
    assert type(actual["2"][1]) is float
    assert type(actual["2"][2]) is bool
    assert type(actual["2"][3]) is int
    assert type(actual["2"][4]) is float
    assert _canonical_json_value({1: "first", "1": "second"}) == {"1": "second"}


def test_json_dates_dataframe_and_timezone_conversion_keep_existing_meaning() -> None:
    values = [
        pd.Timestamp("2026-08-30T18:30:00Z"),
        datetime(2026, 8, 30, 18, 30, tzinfo=timezone.utc),
        pd.Timestamp("2026-08-30 18:30"),
        datetime(2026, 8, 30, 18, 30),
        np.datetime64("2026-08-30T18:30"),
    ]
    assert _canonical_json_value(values) == [
        "2026-08-31T02:30:00+08:00",
        "2026-08-31T02:30:00+08:00",
        "2026-08-30T18:30:00+08:00",
        "2026-08-30T18:30:00+08:00",
        "2026-08-30T18:30:00+08:00",
    ]
    frame = pd.DataFrame({"volume": [np.int64(3)], "date": [values[0]]})
    assert _canonical_json_value(frame) == [{"volume": 3, "date": "2026-08-31T02:30:00+08:00"}]


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), np.float32("inf")])
def test_json_fast_path_still_rejects_nonfinite_numbers(bad) -> None:
    with pytest.raises(ValueError, match="non-finite float cannot be canonicalized"):
        _canonical_json_value({"nested": [bad]})


@pytest.mark.parametrize("bad", [pd.NaT, np.datetime64("NaT")])
def test_json_still_rejects_nat(bad) -> None:
    with pytest.raises(ValueError, match="cannot canonicalize NaT"):
        _canonical_json_value(bad)


def test_json_still_rejects_unsupported_types_and_bad_utf8() -> None:
    with pytest.raises(ValueError, match="cannot canonicalize value of type set"):
        _canonical_json_value({1, 2})
    with pytest.raises(UnicodeDecodeError):
        _canonical_json_value(b"\xff")
