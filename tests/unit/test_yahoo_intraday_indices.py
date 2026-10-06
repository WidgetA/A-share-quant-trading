"""Minute-source preservation, bar/quote distinction and shared HTTP behavior."""

import asyncio
import copy
import hashlib
import json
import struct

import httpx
import pytest

from src.data import yahoo_indices as daily_module
from src.data.yahoo_indices import YahooIndexClient, YahooIndexError
from src.data.yahoo_intraday_indices import YahooIntradayIndexClient, parse_intraday_chart


def chart(symbol="KOSPI-25.KS", market="KR", interval="1m"):
    step = 60 if interval == "1m" else 300
    start = 1791207000
    stamps = [start, start + step, start + 2 * step]
    return {
        "chart": {
            "error": None,
            "result": [
                {
                    "meta": {
                        "symbol": symbol,
                        "instrumentType": "INDEX",
                        "shortName": symbol,
                        "currency": "KRW" if market == "KR" else "USD",
                        "exchangeTimezoneName": "Asia/Seoul"
                        if market == "KR"
                        else "America/New_York",
                        "dataGranularity": interval,
                        "regularMarketTime": stamps[-1] + 20,
                        "currentTradingPeriod": {"regular": {"start": start, "end": start + 23400}},
                    },
                    "timestamp": stamps,
                    "indicators": {
                        "quote": [
                            {
                                "open": [100, 101, 102],
                                "high": [105, 106, 107],
                                "low": [99, 100, 101],
                                "close": [104, 105, 106],
                                "volume": [0, 0, 0],
                            }
                        ]
                    },
                }
            ],
        }
    }


def parse(payload, *, symbol="KOSPI-25.KS", market="KR", interval="1m", fetched_at=1791230400999):
    return parse_intraday_chart(
        payload, symbol=symbol, market=market, interval=interval, fetched_at=fetched_at
    )


def test_real_minute_times_fields_and_source_finality_are_preserved():
    _, points = parse(chart())
    assert [point["ts"] for point in points] == [1791207000000, 1791207060000, 1791207120000]
    assert [point["is_final"] for point in points] == [True, True, False]
    assert all(point["data_kind"] == "minute_bar" and point["interval"] == "1m" for point in points)
    assert points[-1]["fetched_at"] == 1791230400999
    assert points[-1]["trade_date"] == "2026-10-05"
    assert points[-1]["close"] == 106 and points[-1]["volume"] == 0
    assert points[-1]["adjusted_close"] is None


def test_missing_ohl_and_all_zero_ohl_preserve_actual_close_and_volume():
    payload = chart()
    values = payload["chart"]["result"][0]["indicators"]["quote"][0]
    for key in ("open", "high", "low"):
        values[key] = [None, 0, None]
    values["volume"] = [None, 321, 0]
    _, points = parse(payload)
    assert [point["close"] for point in points] == [104, 105, 106]
    assert all(point[field] is None for point in points for field in ("open", "high", "low"))
    assert [point["volume"] for point in points] == [None, 321, 0]


def test_explicit_source_nulls_keep_every_timestamp_and_remain_unknown():
    payload = chart()
    values = payload["chart"]["result"][0]["indicators"]["quote"][0]
    for key in values:
        values[key] = [None] * 3
    _, points = parse(payload)
    assert len(points) == 3
    assert all(point["close"] is None and point["is_final"] is None for point in points)


def test_latest_bar_unknown_without_source_progress_despite_wall_clock_close():
    payload = chart()
    del payload["chart"]["result"][0]["meta"]["regularMarketTime"]
    _, points = parse(payload, fetched_at=1792000000000)
    assert points[-1]["is_final"] is None
    assert points[0]["is_final"] is True


def test_unaligned_appended_quote_is_separate_and_cannot_finalize_last_real_bar():
    payload = chart("^GSPC", "US")
    source = payload["chart"]["result"][0]
    source["timestamp"][-1] = source["timestamp"][-2] + 84
    source["meta"]["regularMarketTime"] = source["timestamp"][-1]
    _, points = parse(payload, symbol="^GSPC", market="US")
    assert points[-1]["ts"] == source["timestamp"][-1] * 1000
    assert points[-1]["ts"] % 60000 != 0
    assert points[-1]["interval"] == "1m" and points[-1]["data_kind"] == "minute_quote_snapshot"
    assert points[-1]["is_final"] is None and points[-1]["close"] == 106
    assert points[-2]["is_final"] is None
    assert points[0]["is_final"] is True


def old_window_with_aligned_latest_quote(interval="5m"):
    """Minimum distinguishing data from repaired KOSPI10/KQ51 archive RAW."""
    payload = chart(interval=interval)
    source = payload["chart"]["result"][0]
    step = 60 if interval == "1m" else 300
    source["timestamp"] = [1786320000, 1786320000 + step, 1791266400]
    source["meta"].update(
        regularMarketTime=1791284740,
        regularMarketPrice=3466.52,
        tradingPeriods=[
            [{"start": 1786320000, "end": 1786341600}],
            [{"start": 1791244800, "end": 1791266400}],
        ],
    )
    latest = struct.unpack("!f", struct.pack("!f", 3466.52))[0]
    for field in ("open", "high", "low", "close"):
        source["indicators"]["quote"][0][field][-1] = latest
    return payload


@pytest.mark.parametrize("interval", ["1m", "5m"])
def test_archive_grid_aligned_latest_quote_is_not_a_current_minute_bar(interval):
    _, points = parse_intraday_chart(
        old_window_with_aligned_latest_quote(interval),
        symbol="KOSPI-25.KS",
        market="KR",
        fetched_at=1791331200000,
        interval=interval,
        start=1786214209,
        end=1786819009,
    )
    assert [point["data_kind"] for point in points] == [
        "minute_bar",
        "minute_bar",
        "minute_quote_snapshot",
    ]
    assert points[-1]["ts"] == 1791266400000
    assert points[-1]["is_final"] is None
    assert points[-2]["is_final"] is None


@pytest.mark.parametrize("interval", ["1m", "5m"])
def test_korean_published_session_close_quote_stays_snapshot_inside_current_window(interval):
    payload = old_window_with_aligned_latest_quote(interval)
    source = payload["chart"]["result"][0]
    source["timestamp"][:2] = [1791244800, 1791244800 + (60 if interval == "1m" else 300)]
    _, points = parse_intraday_chart(
        payload,
        symbol="KOSPI-25.KS",
        market="KR",
        fetched_at=1791331200000,
        interval=interval,
        start=1791200000,
        end=1791310000,
    )
    assert points[-1]["data_kind"] == "minute_quote_snapshot"
    assert points[-1]["ts"] == 1791266400000 and points[-1]["is_final"] is None
    assert points[-2]["is_final"] is None


def test_unobserved_us_aligned_close_pattern_keeps_existing_inside_bar_semantics():
    payload = old_window_with_aligned_latest_quote()
    source = payload["chart"]["result"][0]
    source["meta"].update(symbol="^SOX", currency="USD", exchangeTimezoneName="America/New_York")
    source["timestamp"][:2] = [1791244800, 1791245100]
    _, points = parse_intraday_chart(
        payload,
        symbol="^SOX",
        market="US",
        fetched_at=1791331200000,
        interval="5m",
        start=1791200000,
        end=1791310000,
    )
    assert points[-1]["data_kind"] == "minute_bar"


def test_real_flat_bar_and_boundary_overlap_are_preserved_as_bars():
    payload = chart()
    source = payload["chart"]["result"][0]
    for field in ("open", "high", "low", "close"):
        source["indicators"]["quote"][0][field][-1] = 106
    source["meta"]["regularMarketPrice"] = 106
    source["meta"]["tradingPeriods"] = [[{"start": 1791207000, "end": 1791207180}]]
    _, points = parse_intraday_chart(
        payload,
        symbol="KOSPI-25.KS",
        market="KR",
        fetched_at=1791331200000,
        start=1791207001,
        end=1791207119,
    )
    assert len(points) == 3
    assert all(point["data_kind"] == "minute_bar" for point in points)


@pytest.mark.parametrize(
    "change", ["wrong_price", "wrong_latest_session", "positive_volume", "many_outside"]
)
def test_unproven_wrong_window_points_cannot_be_relabelled_as_snapshots(change):
    payload = old_window_with_aligned_latest_quote()
    source = payload["chart"]["result"][0]
    if change == "wrong_price":
        source["meta"]["regularMarketPrice"] = 999
    elif change == "wrong_latest_session":
        source["meta"]["tradingPeriods"][-1][0]["end"] -= 300
    elif change == "positive_volume":
        source["indicators"]["quote"][0]["volume"][-1] = 123
    else:
        source["timestamp"][1] = 1791244800
    with pytest.raises(YahooIndexError):
        parse_intraday_chart(
            payload,
            symbol="KOSPI-25.KS",
            market="KR",
            fetched_at=1791331200000,
            interval="5m",
            start=1786214209,
            end=1786819009,
        )


@pytest.mark.parametrize("change", ["history_unaligned", "unmatched_tail", "duplicate", "unsorted"])
def test_unproven_or_invalid_source_times_are_rejected(change):
    payload = chart()
    source = payload["chart"]["result"][0]
    if change == "history_unaligned":
        source["timestamp"][0] += 1
    elif change == "unmatched_tail":
        source["timestamp"][-1] += 1
    elif change == "duplicate":
        source["timestamp"][1] = source["timestamp"][0]
    else:
        source["timestamp"].reverse()
    with pytest.raises(YahooIndexError):
        parse(payload)


@pytest.mark.parametrize(
    "field,value",
    [
        ("symbol", "^GSPC"),
        ("instrumentType", "ETF"),
        ("currency", "USD"),
        ("exchangeTimezoneName", "Asia/Seoul_other"),
        ("dataGranularity", "1d"),
    ],
)
def test_wrong_identity_or_granularity_cannot_become_minutes(field, value):
    payload = chart()
    payload["chart"]["result"][0]["meta"][field] = value
    with pytest.raises(YahooIndexError):
        parse(payload)


def test_metadata_quote_without_source_timestamps_is_not_fabricated_into_a_minute():
    payload = chart()
    source = payload["chart"]["result"][0]
    source["timestamp"] = []
    source["meta"]["regularMarketPrice"] = 999
    with pytest.raises(YahooIndexError):
        parse(payload)


def empty_chart():
    """The observed SOX weekend HTTP200 shape, retaining its identity metadata."""
    payload = chart("^SOX", "US")
    source = payload["chart"]["result"][0]
    del source["timestamp"]
    source["indicators"] = {"quote": [{}]}
    source["meta"]["regularMarketPrice"] = 999
    return payload


def test_identity_valid_empty_source_is_preserved_without_a_metadata_price():
    metadata, points = parse(empty_chart(), symbol="^SOX", market="US")
    assert metadata["symbol"] == "^SOX" and metadata["regularMarketPrice"] == 999
    assert points == []


@pytest.mark.parametrize(
    "field,value",
    [("symbol", "^BKX"), ("instrumentType", "ETF"), ("currency", "KRW"), ("dataGranularity", "1d")],
)
def test_empty_source_still_requires_exact_identity_and_granularity(field, value):
    payload = empty_chart()
    payload["chart"]["result"][0]["meta"][field] = value
    with pytest.raises(YahooIndexError):
        parse(payload, symbol="^SOX", market="US")


@pytest.mark.parametrize("field", ["quote", "adjclose"])
def test_price_arrays_without_timestamps_are_not_an_empty_window(field):
    payload = empty_chart()
    payload["chart"]["result"][0]["indicators"][field] = [
        {"close" if field == "quote" else "adjclose": [123]}
    ]
    with pytest.raises(YahooIndexError):
        parse(payload, symbol="^SOX", market="US")


@pytest.mark.asyncio
async def test_empty_window_receipt_preserves_exact_requested_window_and_raw_source():
    raw = json.dumps(empty_chart()).encode()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=raw))
    ) as http:
        result = await YahooIntradayIndexClient(client=http).fetch(
            "^SOX", "US", start=1791000000, end=1791172800
        )
    assert result["source_empty"] is True
    assert result["requested_range"] == {"start": 1791000000, "end": 1791172800, "interval": "1m"}
    assert result["points"] == [] and result["missing_close_timestamps"] == []
    assert result["raw_json"].encode() == raw
    assert result["payload_sha256"] == hashlib.sha256(raw).hexdigest()


@pytest.mark.asyncio
async def test_unknown_symbol_404_cannot_be_acknowledged_as_an_empty_source():
    payload = {
        "chart": {
            "result": None,
            "error": {"code": "Not Found", "description": "No data found, symbol may be delisted"},
        }
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(404, json=payload))
    ) as http:
        with pytest.raises(YahooIndexError):
            await YahooIntradayIndexClient(client=http).fetch("^SOX", "US", start=1, end=2)


@pytest.mark.parametrize(
    "field,value",
    [
        ("close", float("nan")),
        ("high", float("inf")),
        ("volume", -1),
        ("open", False),
        ("low", 200),
        ("high", 1),
        ("close", 0),
    ],
)
def test_invalid_numeric_observations_do_not_enter_storage(field, value):
    payload = chart()
    payload["chart"]["result"][0]["indicators"]["quote"][0][field][0] = value
    with pytest.raises(YahooIndexError):
        parse(payload)


def test_misaligned_arrays_are_not_truncated_or_zipped():
    payload = chart()
    payload["chart"]["result"][0]["indicators"]["quote"][0]["close"] = [104]
    with pytest.raises(YahooIndexError):
        parse(payload)


def test_actual_five_minute_chart_retains_its_granularity():
    payload = chart(interval="5m")
    _, points = parse(payload, interval="5m")
    assert [point["ts"] for point in points] == [1791207000000, 1791207300000, 1791207600000]
    assert all(point["interval"] == "5m" for point in points)
    with pytest.raises(YahooIndexError):
        parse(payload, interval="1m")


@pytest.mark.asyncio
async def test_explicit_window_and_all_source_points_survive_transport_and_parsing():
    calls = []
    raw = json.dumps(chart(), ensure_ascii=False).encode()

    def respond(request):
        calls.append(request)
        return httpx.Response(200, content=raw, headers={"content-type": "application/json"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        result = await YahooIntradayIndexClient(client=http).fetch(
            "KOSPI-25.KS", "KR", start=1791207001, end=1791208000
        )
    assert calls[0].url.params == httpx.QueryParams(
        {"interval": "1m", "period1": "1791207001", "period2": "1791208000"}
    )
    assert result["payload_sha256"] == hashlib.sha256(raw).hexdigest()
    assert result["raw_json"].encode() == raw and len(result["points"]) == 3


@pytest.mark.asyncio
async def test_minute_transient_error_fails_over_to_other_host(monkeypatch):
    calls = []

    async def sleep(_):
        pass

    monkeypatch.setattr(daily_module.asyncio, "sleep", sleep)

    def respond(request):
        calls.append(request.url.host)
        return httpx.Response(502) if len(calls) == 1 else httpx.Response(200, json=chart())

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        result = await YahooIntradayIndexClient(client=http).fetch(
            "KOSPI-25.KS", "KR", start=1791207000, end=1791208000
        )
    assert len(result["points"]) == 3
    assert calls == ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]


@pytest.mark.asyncio
async def test_daily_and_minute_share_one_rate_limit_cooldown_and_borrowed_lifetime():
    calls = []
    loop = asyncio.get_running_loop()

    def respond(request):
        calls.append((request.url.params["interval"], loop.time()))
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "1"})
        if request.url.params["interval"] == "1d":
            daily = copy.deepcopy(chart("^SOX", "US"))
            daily["chart"]["result"][0]["meta"]["dataGranularity"] = "1d"
            return httpx.Response(200, json=daily)
        return httpx.Response(200, json=chart())

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        daily = YahooIndexClient(proxy=None, client=http)
        minute = YahooIntradayIndexClient(transport=daily)
        first = asyncio.create_task(daily.fetch("^SOX", "US", capability="daily_history"))
        await asyncio.sleep(0)
        second = asyncio.create_task(
            minute.fetch("KOSPI-25.KS", "KR", start=1791207000, end=1791208000)
        )
        results = await asyncio.gather(first, second)
        await minute.aclose()
        assert not http.is_closed
    assert len(calls) == 3
    assert all(stamp - calls[0][1] >= 0.9 for _, stamp in calls[1:])
    assert {result["points"][0]["interval"] for result in results} == {"1d", "1m"}


@pytest.mark.asyncio
async def test_permanent_minute_error_does_not_retry_the_other_host():
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(422, json={"chart": {"error": {"code": "Unprocessable Entity"}}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with pytest.raises(YahooIndexError):
            await YahooIntradayIndexClient(client=http).fetch("KOSPI-25.KS", "KR", start=0, end=1)
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("start,end", [(True, 2), (-1, 2), (1, 1), (1, True)])
async def test_invalid_window_is_rejected_before_http(start, end):
    calls = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: calls.append(request))
    ) as http:
        with pytest.raises(ValueError):
            await YahooIntradayIndexClient(client=http).fetch(
                "KOSPI-25.KS", "KR", start=start, end=end
            )
    assert calls == []
