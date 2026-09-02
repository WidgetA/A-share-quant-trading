import asyncio
from datetime import date
from zoneinfo import ZoneInfo

import pytest

from src.data.clients.tushare_realtime import (
    TushareRealtimeClient,
    TushareRealtimeError,
)


def _minute_payload(*rows: list[object]) -> dict[str, object]:
    return {
        "data": {
            "fields": ["ts_code", "time", "open", "close", "high", "low", "vol", "amount"],
            "items": list(rows),
        }
    }


def test_minute_parser_attaches_beijing_timezone_and_keeps_raw_label() -> None:
    parsed = TushareRealtimeClient._parse_minute_bars(
        _minute_payload(["000001.SZ", "2026-08-31 09:39:00", 10, 10.1, 10.2, 9.9, 0, 0])
    )

    bar = parsed["000001"]
    assert bar.end_label == "09:39"
    assert bar.bar_end.tzinfo == ZoneInfo("Asia/Shanghai")
    assert bar.bar_end.date() == date(2026, 8, 31)
    assert bar.is_valid


@pytest.mark.asyncio
async def test_all_latest_and_current_history_transport_failures_raise(monkeypatch) -> None:
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]

    async def failed_call(*_args, **_kwargs):
        raise RuntimeError("vendor transport unavailable")

    monkeypatch.setattr(client, "_api_call", failed_call)

    with pytest.raises(TushareRealtimeError, match="all rt_min minute-bar batches failed"):
        await client.batch_get_latest_minute_bars(["000001"])
    with pytest.raises(
        TushareRealtimeError,
        match="all rt_min_daily minute-history requests failed",
    ):
        await client.batch_get_minute_history(["000001"])


@pytest.mark.asyncio
async def test_successful_empty_symbol_response_is_not_a_transport_failure(monkeypatch) -> None:
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]

    async def empty_call(*_args, **_kwargs):
        return {
            "data": {
                "fields": ["ts_code", "time", "open", "close", "high", "low", "vol", "amount"],
                "items": [],
            }
        }

    monkeypatch.setattr(client, "_api_call", empty_call)

    assert await client.batch_get_latest_minute_bars(["000001"]) == {}
    assert await client.batch_get_minute_history(["000001"]) == {"000001": ()}


@pytest.mark.asyncio
async def test_minute_history_deadline_retains_healthy_sibling_and_recycles_hang(
    monkeypatch,
) -> None:
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]
    client.TIMEOUT = 0.01
    entered_hang = asyncio.Event()

    async def hanging_call(endpoint: str, params: dict[str, str], **_kwargs):
        code = params["ts_code"].split(".")[0]
        if code == "000002":
            entered_hang.set()
            await asyncio.Event().wait()
        return {
            "data": {
                "fields": ["time", "open", "close", "high", "low", "vol", "amount"],
                "items": [["2026-08-31 09:39:00", 10, 10.1, 10.2, 9.9, 100, 1010]],
            }
        }

    monkeypatch.setattr(client, "_api_call", hanging_call)
    result = await asyncio.wait_for(
        client.batch_get_minute_history(["000001", "000002"]), timeout=1
    )

    assert set(result) == {"000001"}
    assert result["000001"][0].end_label == "09:39"
    await asyncio.sleep(0)
    assert not any(
        task.get_name().startswith("rt-minute-history-")
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
    )


@pytest.mark.asyncio
async def test_minute_history_partial_failure_retains_successful_symbol(
    monkeypatch,
) -> None:
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]

    async def partial_call(endpoint: str, params: dict[str, str], **_kwargs):
        code = params["ts_code"].split(".")[0]
        if code == "000002":
            raise RuntimeError("single-symbol transport failure")
        return {
            "data": {
                "fields": ["time", "open", "close", "high", "low", "vol", "amount"],
                "items": [["2026-08-31 09:39:00", 10, 10.1, 10.2, 9.9, 100, 1010]],
            }
        }

    monkeypatch.setattr(client, "_api_call", partial_call)
    result = await asyncio.wait_for(
        client.batch_get_minute_history(["000001", "000002"]), timeout=1
    )

    assert set(result) == {"000001"}
    assert result["000001"][0].is_valid


@pytest.mark.asyncio
async def test_minute_history_caller_cancellation_recycles_all_symbol_tasks(
    monkeypatch,
) -> None:
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]
    entered = asyncio.Event()

    async def hanging_call(*_args, **_kwargs):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(client, "_api_call", hanging_call)
    request = asyncio.create_task(client.batch_get_minute_history(["000001"]))
    await asyncio.wait_for(entered.wait(), timeout=1)
    request.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(request, timeout=1)

    await asyncio.sleep(0)
    assert not any(
        task.get_name().startswith("rt-minute-history-")
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
    )


def test_minute_parser_localizes_non_minute_timestamp_and_invalid_ohlc() -> None:
    assert (
        TushareRealtimeClient._parse_minute_bars(
            _minute_payload(["000001.SZ", "2026-08-31 09:39:01", 10, 10.1, 10.2, 9.9, 1, 1])
        )
        == {}
    )

    assert (
        TushareRealtimeClient._parse_minute_bars(
            _minute_payload(["000001.SZ", "2026-08-31 09:39:00", 10, 10.3, 10.2, 9.9, 1, 1])
        )
        == {}
    )


def test_minute_history_is_sorted_and_rejects_conflicting_revision() -> None:
    data = {
        "data": {
            "fields": ["time", "open", "close", "high", "low", "vol", "amount"],
            "items": [
                ["2026-08-31 09:39:00", 10, 10.1, 10.2, 9.9, 1, 1],
                ["2026-08-31 09:38:00", 10, 10.0, 10.1, 9.9, 1, 1],
            ],
        }
    }
    bars = TushareRealtimeClient._parse_minute_history("000001", data)
    assert [bar.end_label for bar in bars] == ["09:38", "09:39"]

    data["data"]["items"].append(["2026-08-31 09:39:00", 10, 10.2, 10.3, 9.9, 1, 1])
    bars = TushareRealtimeClient._parse_minute_history("000001", data)
    assert [bar.end_label for bar in bars] == ["09:38"]


def test_historical_minute_parser_uses_trade_time_and_enforces_requested_date() -> None:
    data = {
        "data": {
            "fields": [
                "ts_code",
                "trade_time",
                "open",
                "close",
                "high",
                "low",
                "vol",
                "amount",
            ],
            "items": [["000001.SZ", "2026-08-28 14:57:00", 10, 10, 10.1, 9.9, 1, 1]],
        }
    }

    bars = TushareRealtimeClient._parse_historical_minute_history("000001", date(2026, 8, 28), data)
    assert [bar.end_label for bar in bars] == ["14:57"]

    assert (
        TushareRealtimeClient._parse_historical_minute_history("000001", date(2026, 8, 27), data)
        == ()
    )


def test_minute_parsers_keep_valid_siblings_after_local_row_failures() -> None:
    latest = TushareRealtimeClient._parse_minute_bars(
        _minute_payload(
            ["000001.SZ", "2026-08-31 09:39:00", 10, 10, 10.1, 9.9, -1, 1],
            ["000002.SZ", "2026-08-31 09:39:00", 10, 9, 10.1, 8.9, 1, 1],
        )
    )
    assert set(latest) == {"000002"}

    history = TushareRealtimeClient._parse_minute_history(
        "000001",
        {
            "data": {
                "fields": ["time", "open", "close", "high", "low", "vol", "amount"],
                "items": [
                    ["2026-08-31 09:31:00", 10, 10, 10.1, 9.9, -1, 1],
                    ["2026-08-31 09:32:00", 10, 8, 10.1, 8, 1, 1],
                ],
            }
        },
    )
    assert [bar.end_label for bar in history] == ["09:32"]


def test_boolean_minute_values_are_rejected_locally() -> None:
    latest = TushareRealtimeClient._parse_minute_bars(
        _minute_payload(
            ["000001.SZ", "2026-08-31 09:39:00", True, True, True, True, True, True],
            ["000002.SZ", "2026-08-31 09:39:00", 10, 10, 10.1, 9.9, 1, 1],
        )
    )
    assert set(latest) == {"000002"}

    history = TushareRealtimeClient._parse_minute_history(
        "000001",
        {
            "data": {
                "fields": ["time", "open", "close", "high", "low", "vol", "amount"],
                "items": [
                    ["2026-08-31 09:31:00", 10, True, 10.1, 9.9, 1, 1],
                    ["2026-08-31 09:32:00", 10, 10, 10.1, 9.9, 1, 1],
                ],
            }
        },
    )
    assert [bar.end_label for bar in history] == ["09:32"]

    quotes = TushareRealtimeClient._parse_rt_min(
        _minute_payload(
            ["000001.SZ", "2026-08-31 09:39:00", True, True, True, True, True, True],
            ["000002.SZ", "2026-08-31 09:39:00", 10, 10, 10.1, 9.9, 1, 1],
        )
    )
    assert set(quotes) == {"000002"}


def test_boolean_early_history_invalidates_only_that_stock() -> None:
    payload = {
        "data": {
            "fields": ["time", "open", "close", "high", "low", "vol", "amount"],
            "items": [["2026-08-31 09:39:00", 10, 10, 10.1, 9.9, True, 1]],
        }
    }
    assert TushareRealtimeClient._parse_rt_min_daily("000001", payload) is None


def test_historical_parser_filters_wrong_date_but_keeps_requested_rows() -> None:
    bars = TushareRealtimeClient._parse_historical_minute_history(
        "000001",
        date(2026, 8, 28),
        {
            "data": {
                "fields": [
                    "ts_code",
                    "trade_time",
                    "open",
                    "close",
                    "high",
                    "low",
                    "vol",
                    "amount",
                ],
                "items": [
                    ["000001.SZ", "2026-08-27 14:57:00", 10, 10, 10.1, 9.9, 1, 1],
                    ["000001.SZ", "2026-08-28 14:57:00", 10, 8, 10.1, 8, 1, 1],
                ],
            }
        },
    )
    assert [bar.bar_end.date() for bar in bars] == [date(2026, 8, 28)]


def test_exchange_suffix_includes_beijing_exchange_and_validates_code() -> None:
    assert TushareRealtimeClient._to_ts_code("600000") == "600000.SH"
    assert TushareRealtimeClient._to_ts_code("000001") == "000001.SZ"
    assert TushareRealtimeClient._to_ts_code("830799") == "830799.BJ"
    assert TushareRealtimeClient._to_ts_code("920001") == "920001.BJ"
    with pytest.raises(ValueError, match="invalid bare A-share code"):
        TushareRealtimeClient._to_ts_code("1")


@pytest.mark.asyncio
async def test_daily_bars_normalize_amount_and_reject_wrong_trade_date(monkeypatch) -> None:
    client = TushareRealtimeClient("token")

    async def valid_call(*_args, **_kwargs):
        return {
            "data": {
                "fields": ["ts_code", "trade_date", "close", "amount"],
                "items": [["000001.SZ", "20260828", 10, 123.5]],
            }
        }

    monkeypatch.setattr(client, "_api_call", valid_call)
    rows = await client.fetch_daily_bars("20260828")
    assert rows["000001"].amount_yuan == 123_500

    async def wrong_date_call(*_args, **_kwargs):
        return {
            "data": {
                "fields": ["ts_code", "trade_date", "close", "amount"],
                "items": [["000001.SZ", "20260827", 10, 123.5]],
            }
        }

    monkeypatch.setattr(client, "_api_call", wrong_date_call)
    with pytest.raises(TushareRealtimeError, match="does not match request"):
        await client.fetch_daily_bars("20260828")


@pytest.mark.asyncio
async def test_boolean_daily_values_are_rejected_per_code(monkeypatch) -> None:
    client = TushareRealtimeClient("token")

    async def daily_call(*_args, **_kwargs):
        return {
            "data": {
                "fields": ["ts_code", "trade_date", "close", "amount"],
                "items": [
                    ["000001.SZ", "20260828", True, 123.5],
                    ["000002.SZ", "20260828", 10.0, 123.5],
                ],
            }
        }

    monkeypatch.setattr(client, "_api_call", daily_call)
    rows = await client.fetch_daily_bars("20260828")
    assert set(rows) == {"000002"}


@pytest.mark.asyncio
async def test_boolean_previous_close_is_rejected_per_code(monkeypatch) -> None:
    client = TushareRealtimeClient("token")

    async def daily_call(*_args, **_kwargs):
        return {
            "data": {
                "fields": ["ts_code", "close"],
                "items": [["000001.SZ", True], ["000002.SZ", 10.0]],
            }
        }

    monkeypatch.setattr(client, "_api_call", daily_call)
    rows = await client.fetch_prev_closes("20260828")
    assert rows == {"000002": 10.0}


@pytest.mark.asyncio
async def test_trade_calendar_uses_open_rows_and_rejects_duplicates(monkeypatch) -> None:
    client = TushareRealtimeClient("token")

    async def valid_call(*_args, **_kwargs):
        return {
            "data": {
                "fields": ["exchange", "cal_date", "is_open"],
                "items": [
                    ["SSE", "20260831", 1],
                    ["SSE", "20260901", 1],
                    ["SSE", "20260902", 0],
                ],
            }
        }

    monkeypatch.setattr(client, "_api_call", valid_call)
    result = await client.fetch_trade_calendar(date(2026, 8, 31), date(2026, 9, 2))
    assert result == (date(2026, 8, 31), date(2026, 9, 1))

    async def duplicate_call(*_args, **_kwargs):
        return {
            "data": {
                "fields": ["cal_date", "is_open"],
                "items": [["20260831", 1], ["20260831", 1]],
            }
        }

    monkeypatch.setattr(client, "_api_call", duplicate_call)
    with pytest.raises(TushareRealtimeError, match="duplicate trade_cal"):
        await client.fetch_trade_calendar(date(2026, 8, 31), date(2026, 9, 2))

    async def missing_middle_day_call(*_args, **_kwargs):
        return {
            "data": {
                "fields": ["cal_date", "is_open"],
                "items": [["20260831", 1], ["20260902", 1]],
            }
        }

    monkeypatch.setattr(client, "_api_call", missing_middle_day_call)
    with pytest.raises(TushareRealtimeError, match="does not cover every requested"):
        await client.fetch_trade_calendar(date(2026, 8, 31), date(2026, 9, 2))

    async def boolean_open_call(*_args, **_kwargs):
        return {
            "data": {
                "fields": ["cal_date", "is_open"],
                "items": [["20260831", True]],
            }
        }

    monkeypatch.setattr(client, "_api_call", boolean_open_call)
    with pytest.raises(TushareRealtimeError, match="boolean is_open"):
        await client.fetch_trade_calendar(date(2026, 8, 31), date(2026, 8, 31))


@pytest.mark.asyncio
async def test_closed_history_api_keeps_successful_code_when_a_sibling_fails(
    monkeypatch,
) -> None:
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]

    async def api_call(_api_name, params, **_kwargs):
        if params["ts_code"] == "000001.SZ":
            raise RuntimeError("one symbol unavailable")
        return {
            "data": {
                "fields": [
                    "ts_code",
                    "trade_time",
                    "open",
                    "close",
                    "high",
                    "low",
                    "vol",
                    "amount",
                ],
                "items": [["000002.SZ", "2026-08-28 14:57:00", 10, 8, 10.1, 8, 1, 1]],
            }
        }

    monkeypatch.setattr(client, "_api_call", api_call)

    result = await client.batch_get_minute_history_for_date(
        ["000001", "000002"],
        date(2026, 8, 28),
    )

    assert set(result) == {"000002"}
    assert result["000002"][0].end_label == "14:57"


@pytest.mark.asyncio
async def test_historical_early_api_batches_symbols_into_narrow_stk_mins_requests(
    monkeypatch,
) -> None:
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]
    client.HISTORICAL_EARLY_BATCH_SIZE = 400
    calls: list[tuple[str, dict[str, str], str]] = []

    async def api_call(api_name, params, fields=""):
        calls.append((api_name, dict(params), fields))
        return {
            "data": {
                "fields": [
                    "ts_code",
                    "trade_time",
                    "open",
                    "close",
                    "high",
                    "low",
                    "vol",
                    "amount",
                ],
                "items": [],
            }
        }

    monkeypatch.setattr(client, "_api_call", api_call)
    codes = [f"{index:06d}" for index in range(805)]

    result = await client.batch_get_early_minute_history_for_date(
        codes,
        date(2026, 8, 28),
    )

    assert result == {code: () for code in codes}
    assert len(calls) == 3
    assert [len(params["ts_code"].split(",")) for _api, params, _fields in calls] == [
        400,
        400,
        5,
    ]
    assert {api for api, _params, _fields in calls} == {"stk_mins"}
    assert {params["start_date"] for _api, params, _fields in calls} == {"2026-08-28 09:24:00"}
    assert {params["end_date"] for _api, params, _fields in calls} == {"2026-08-28 09:40:00"}


@pytest.mark.asyncio
async def test_historical_early_api_splits_batched_rows_and_keeps_failed_batch_pending(
    monkeypatch,
) -> None:
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]
    client.HISTORICAL_EARLY_BATCH_SIZE = 2

    async def api_call(_api_name, params, **_kwargs):
        requested = params["ts_code"].split(",")
        if requested[0].startswith("000003"):
            raise RuntimeError("one physical batch failed")
        return {
            "data": {
                "fields": [
                    "ts_code",
                    "trade_time",
                    "open",
                    "close",
                    "high",
                    "low",
                    "vol",
                    "amount",
                ],
                "items": [[requested[0], "2026-08-28 09:39:00", 10, 10.1, 10.2, 9.9, 100, 1000]],
            }
        }

    monkeypatch.setattr(client, "_api_call", api_call)

    result = await client.batch_get_early_minute_history_for_date(
        ["000001", "000002", "000003"],
        date(2026, 8, 28),
    )

    assert set(result) == {"000001", "000002"}
    assert result["000001"][0].end_label == "09:39"
    assert result["000002"] == ()
    assert "000003" not in result


def test_historical_early_batch_parser_rejects_unrequested_code_and_row_cap(
    monkeypatch,
) -> None:
    fields = ["ts_code", "trade_time", "open", "close", "high", "low", "vol", "amount"]
    row = ["000002.SZ", "2026-08-28 09:39:00", 10, 10.1, 10.2, 9.9, 100, 1000]
    parse = TushareRealtimeClient._parse_historical_minute_history_batch

    with pytest.raises(TushareRealtimeError, match="was not requested"):
        parse(
            ["000001"],
            date(2026, 8, 28),
            {"data": {"fields": fields, "items": [row]}},
        )

    monkeypatch.setattr(TushareRealtimeClient, "STK_MINS_MAX_ROWS", 2)
    with pytest.raises(TushareRealtimeError, match="may be truncated"):
        parse(
            ["000002"],
            date(2026, 8, 28),
            {"data": {"fields": fields, "items": [row, row]}},
        )


def test_historical_early_batch_parser_requires_schema_before_confirming_empty() -> None:
    parse = TushareRealtimeClient._parse_historical_minute_history_batch
    requested = ["000001", "000002"]

    with pytest.raises(TushareRealtimeError, match="missing fields"):
        parse(
            requested,
            date(2026, 8, 28),
            {"data": {"items": []}},
        )

    with pytest.raises(TushareRealtimeError, match="amount"):
        parse(
            requested,
            date(2026, 8, 28),
            {
                "data": {
                    "fields": [
                        "ts_code",
                        "trade_time",
                        "open",
                        "close",
                        "high",
                        "low",
                        "vol",
                    ],
                    "items": [],
                }
            },
        )

    fields = ["ts_code", "trade_time", "open", "close", "high", "low", "vol", "amount"]
    assert parse(
        requested,
        date(2026, 8, 28),
        {"data": {"fields": fields, "items": []}},
    ) == {"000001": (), "000002": ()}


def test_historical_early_batch_parser_rejects_any_wrong_trade_date_row() -> None:
    fields = ["ts_code", "trade_time", "open", "close", "high", "low", "vol", "amount"]
    parse = TushareRealtimeClient._parse_historical_minute_history_batch

    with pytest.raises(TushareRealtimeError, match="outside the requested trade date"):
        parse(
            ["000001", "000002"],
            date(2026, 8, 28),
            {
                "data": {
                    "fields": fields,
                    "items": [
                        [
                            "000001.SZ",
                            "2026-08-28 09:39:00",
                            10,
                            10.1,
                            10.2,
                            9.9,
                            100,
                            1000,
                        ],
                        [
                            "000002.SZ",
                            "2026-08-27 09:39:00",
                            20,
                            20.1,
                            20.2,
                            19.9,
                            200,
                            2000,
                        ],
                    ],
                }
            },
        )


@pytest.mark.parametrize(
    "items",
    [
        [["000001.SZ", "2026-08-28 09:39:00", True, 10.1, 10.2, 9.9, 100, 1000]],
        [
            ["000001.SZ", "2026-08-28 09:39:00", 10, 10.1, 10.2, 9.9, 100, 1000],
            ["000001.SZ", "2026-08-28 09:39:00", 10, 10.2, 10.3, 9.9, 100, 1000],
        ],
    ],
    ids=["invalid-boolean-ohlcv", "conflicting-0939-bars"],
)
def test_historical_early_batch_parser_never_confirms_raw_rows_as_empty(items) -> None:
    fields = ["ts_code", "trade_time", "open", "close", "high", "low", "vol", "amount"]

    with pytest.raises(TushareRealtimeError, match="produced no valid canonical bars"):
        TushareRealtimeClient._parse_historical_minute_history_batch(
            ["000001"],
            date(2026, 8, 28),
            {"data": {"fields": fields, "items": items}},
        )


def test_stk_mins_parser_requires_ts_code_and_trade_time_columns() -> None:
    parse = TushareRealtimeClient._parse_historical_minute_history
    row = ["000002.SZ", "2026-08-28 09:39:00", 10, 10.1, 10.2, 9.9, 100, 1000]

    with pytest.raises(TushareRealtimeError, match="missing ts_code"):
        parse(
            "000002",
            date(2026, 8, 28),
            {
                "data": {
                    "fields": ["trade_time", "open", "close", "high", "low", "vol", "amount"],
                    "items": [row[1:]],
                }
            },
        )
    with pytest.raises(TushareRealtimeError, match="missing trade_time"):
        parse(
            "000002",
            date(2026, 8, 28),
            {
                "data": {
                    "fields": ["ts_code", "open", "close", "high", "low", "vol", "amount"],
                    "items": [[row[0], *row[2:]]],
                }
            },
        )


def test_stk_mins_parser_fails_closed_on_wrong_or_mixed_instrument() -> None:
    parse = TushareRealtimeClient._parse_historical_minute_history
    fields = ["ts_code", "trade_time", "open", "close", "high", "low", "vol", "amount"]
    good = ["000002.SZ", "2026-08-28 09:39:00", 10, 10.1, 10.2, 9.9, 100, 1000]

    with pytest.raises(TushareRealtimeError, match="does not match requested"):
        parse(
            "000002",
            date(2026, 8, 28),
            {"data": {"fields": fields, "items": [["000001.SZ", *good[1:]]]}},
        )

    mixed = [good, ["600000.SH", "2026-08-28 09:38:00", 10, 10.1, 10.2, 9.9, 100, 1000]]
    with pytest.raises(TushareRealtimeError, match="does not match requested"):
        parse("000002", date(2026, 8, 28), {"data": {"fields": fields, "items": mixed}})


def test_stk_mins_parser_normalizes_ts_code_case_and_suffix() -> None:
    parse = TushareRealtimeClient._parse_historical_minute_history
    fields = ["ts_code", "trade_time", "open", "close", "high", "low", "vol", "amount"]
    rows = [
        [" 000002.sz ", "2026-08-28 09:38:00", 10, 10.1, 10.2, 9.9, 100, 1000],
        ["000002.SZ", "2026-08-28 09:39:00", 10, 10.2, 10.3, 9.9, 100, 1000],
    ]

    bars = parse("000002", date(2026, 8, 28), {"data": {"fields": fields, "items": rows}})

    assert [bar.end_label for bar in bars] == ["09:38", "09:39"]
    assert all(bar.stock_code == "000002" for bar in bars)


def test_stk_mins_parser_empty_response_is_confirmed_empty_not_an_error() -> None:
    parse = TushareRealtimeClient._parse_historical_minute_history

    assert parse("000002", date(2026, 8, 28), {"data": {"fields": [], "items": []}}) == ()
    assert (
        parse(
            "000002",
            date(2026, 8, 28),
            {"data": {"fields": ["ts_code", "trade_time"], "items": []}},
        )
        == ()
    )


@pytest.mark.asyncio
async def test_historical_stock_names_parse_one_market_wide_bak_basic_snapshot(
    monkeypatch,
) -> None:
    client = TushareRealtimeClient("token")
    calls = []

    async def api_call(api_name, params, fields=""):
        calls.append((api_name, params, fields))
        return {
            "data": {
                "fields": ["trade_date", "ts_code", "name"],
                "items": [
                    ["20260828", "000001.SZ", "平安银行"],
                    ["20260828", "600000.SH", "浦发银行"],
                    ["20260828", "830799.BJ", "艾融软件"],
                ],
            }
        }

    monkeypatch.setattr(client, "_api_call", api_call)

    result = await client.fetch_stock_names_for_date("20260828")

    assert result == {
        "000001": "平安银行",
        "600000": "浦发银行",
        "830799": "艾融软件",
    }
    assert calls == [
        (
            "bak_basic",
            {"trade_date": "20260828"},
            "trade_date,ts_code,name",
        )
    ]


@pytest.mark.asyncio
async def test_historical_stock_names_empty_snapshot_is_explicitly_empty(
    monkeypatch,
) -> None:
    client = TushareRealtimeClient("token")

    async def api_call(*_args, **_kwargs):
        return {
            "data": {
                "fields": ["trade_date", "ts_code", "name"],
                "items": [],
            }
        }

    monkeypatch.setattr(client, "_api_call", api_call)

    assert await client.fetch_stock_names_for_date("20260828") == {}


@pytest.mark.asyncio
async def test_historical_stock_names_reject_bad_request_and_wrong_row_date(
    monkeypatch,
) -> None:
    client = TushareRealtimeClient("token")
    calls = 0

    async def api_call(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return {
            "data": {
                "fields": ["trade_date", "ts_code", "name"],
                "items": [["20260827", "000001.SZ", "平安银行"]],
            }
        }

    monkeypatch.setattr(client, "_api_call", api_call)

    with pytest.raises(ValueError, match="trade_date must be YYYYMMDD"):
        await client.fetch_stock_names_for_date("2026-08-28")
    assert calls == 0

    with pytest.raises(TushareRealtimeError, match="does not match request"):
        await client.fetch_stock_names_for_date("20260828")
    assert calls == 1


@pytest.mark.asyncio
async def test_historical_stock_names_require_all_bound_fields(monkeypatch) -> None:
    client = TushareRealtimeClient("token")

    async def api_call(*_args, **_kwargs):
        return {
            "data": {
                "fields": ["trade_date", "ts_code"],
                "items": [["20260828", "000001.SZ"]],
            }
        }

    monkeypatch.setattr(client, "_api_call", api_call)

    with pytest.raises(TushareRealtimeError, match="missing fields: name"):
        await client.fetch_stock_names_for_date("20260828")


@pytest.mark.asyncio
async def test_historical_stock_names_reject_conflicting_duplicates(
    monkeypatch,
) -> None:
    client = TushareRealtimeClient("token")

    async def api_call(*_args, **_kwargs):
        return {
            "data": {
                "fields": ["trade_date", "ts_code", "name"],
                "items": [
                    ["20260828", "000001.SZ", "平安银行"],
                    ["20260828", "000001.SZ", "ST平安"],
                ],
            }
        }

    monkeypatch.setattr(client, "_api_call", api_call)

    with pytest.raises(TushareRealtimeError, match="conflicting duplicate"):
        await client.fetch_stock_names_for_date("20260828")


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_name", [None, 123, True])
async def test_historical_stock_names_reject_null_and_non_string_names(
    monkeypatch,
    invalid_name,
) -> None:
    client = TushareRealtimeClient("token")

    async def api_call(*_args, **_kwargs):
        return {
            "data": {
                "fields": ["trade_date", "ts_code", "name"],
                "items": [["20260828", "000001.SZ", invalid_name]],
            }
        }

    monkeypatch.setattr(client, "_api_call", api_call)

    with pytest.raises(TushareRealtimeError, match="invalid bak_basic row"):
        await client.fetch_stock_names_for_date("20260828")
