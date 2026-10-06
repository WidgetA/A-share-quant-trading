"""Behavior checks for actual Yahoo quote-only and provisional-bar responses."""

import asyncio
import copy

import httpx
import pytest

from src.data.yahoo_indices import YahooIndexClient, YahooIndexError, parse_chart


def chart(symbol="^SOX", currency="USD", timezone="America/New_York"):
    return {
        "chart": {
            "error": None,
            "result": [
                {
                    "meta": {
                        "symbol": symbol,
                        "instrumentType": "INDEX",
                        "currency": currency,
                        "exchangeTimezoneName": timezone,
                        "firstTradeDate": 700000000,
                        "dataGranularity": "1d",
                        "regularMarketTime": 1791210000,
                        "currentTradingPeriod": {
                            "regular": {"start": 1791207000, "end": 1791230400}
                        },
                    },
                    "timestamp": [1790947800, 1791207000],
                    "indicators": {
                        "quote": [
                            {
                                "open": [100, 103],
                                "high": [105, 106],
                                "low": [99, 102],
                                "close": [104, 105],
                                "volume": [0, 0],
                            }
                        ]
                    },
                }
            ],
        }
    }


def test_real_bar_source_clock_and_active_bar_are_preserved():
    _, points = parse_chart(chart(), symbol="^SOX", market="US", fetched_at=1791210000000)
    assert [p["ts"] for p in points] == [1790947800000, 1791207000000]
    assert [p["is_final"] for p in points] == [True, False]
    assert points[1]["trade_date"] == "2026-10-05"
    assert points[1]["volume"] == 0


def test_korean_zero_ohl_placeholder_is_a_quote_not_a_bar():
    payload = chart("KOSPI-25.KS", "KRW", "Asia/Seoul")
    result = payload["chart"]["result"][0]
    result["meta"]["firstTradeDate"] = None
    result["timestamp"] = [1790939140]
    result["indicators"]["quote"] = [
        {
            "open": [0],
            "high": [0],
            "low": [0],
            "volume": [0],
            "close": [46539.140625],
        }
    ]
    _, points = parse_chart(payload, symbol="KOSPI-25.KS", market="KR", fetched_at=1791210000000)
    assert len(points) == 1
    point = points[0]
    assert (point["data_kind"], point["interval"]) == ("quote_snapshot", "quote")
    assert point["ts"] == 1790939140000
    assert point["trade_date"] == "2026-10-02"
    assert point["close"] == 46539.140625
    assert all(point[key] is None for key in ("open", "high", "low", "volume", "is_final"))


@pytest.mark.parametrize(
    "field,value", [("instrumentType", "ETF"), ("symbol", "^KS11"), ("currency", "KRW")]
)
def test_wrong_instrument_or_market_cannot_be_inserted(field, value):
    payload = chart()
    payload["chart"]["result"][0]["meta"][field] = value
    with pytest.raises(YahooIndexError):
        parse_chart(payload, symbol="^SOX", market="US", fetched_at=1791210000000)


def test_corrupt_parallel_array_is_rejected():
    payload = chart()
    payload["chart"]["result"][0]["indicators"]["quote"][0]["close"] = [104]
    with pytest.raises(YahooIndexError, match="Misaligned"):
        parse_chart(payload, symbol="^SOX", market="US", fetched_at=1791210000000)


def test_multiple_zero_ohl_rows_do_not_become_fake_history():
    payload = chart()
    series = payload["chart"]["result"][0]["indicators"]["quote"][0]
    for key in ("open", "high", "low"):
        series[key] = [0, 0]
    with pytest.raises(YahooIndexError, match="placeholders"):
        parse_chart(payload, symbol="^SOX", market="US", fetched_at=1791210000000)


def test_silent_monthly_response_is_rejected_even_when_request_was_daily():
    payload = chart()
    payload["chart"]["result"][0]["meta"]["dataGranularity"] = "1mo"
    with pytest.raises(YahooIndexError, match="granularity"):
        parse_chart(payload, symbol="^SOX", market="US", fetched_at=1791210000000)


def test_null_close_observation_is_preserved_for_gap_recovery():
    payload = chart()
    payload["chart"]["result"][0]["indicators"]["quote"][0]["close"][0] = None
    _, points = parse_chart(payload, symbol="^SOX", market="US", fetched_at=1791210000000)
    assert len(points) == 2
    assert points[0]["close"] is None
    assert points[0]["ts"] == 1790947800000
    assert points[0]["is_final"] is None


def test_delayed_latest_bar_is_not_final_merely_because_clock_passed_close():
    _, points = parse_chart(chart(), symbol="^SOX", market="US", fetched_at=1791231000000)
    assert points[-1]["is_final"] is None


def test_unsorted_arrays_are_rejected_before_checkpoint_can_move():
    payload = chart()
    result = payload["chart"]["result"][0]
    result["timestamp"].reverse()
    for values in result["indicators"]["quote"][0].values():
        values.reverse()
    with pytest.raises(YahooIndexError, match="chronological"):
        parse_chart(payload, symbol="^SOX", market="US", fetched_at=1791210000000)


def test_quote_only_index_with_real_ohl_remains_a_snapshot():
    payload = chart()
    result = payload["chart"]["result"][0]
    result["timestamp"] = result["timestamp"][-1:]
    for key in result["indicators"]["quote"][0]:
        result["indicators"]["quote"][0][key] = result["indicators"]["quote"][0][key][-1:]
    _, points = parse_chart(
        payload, symbol="^SOX", market="US", fetched_at=1791210000000, capability="snapshot_only"
    )
    assert points[0]["data_kind"] == "quote_snapshot"
    assert points[0]["open"] == 103
    assert points[0]["is_final"] is None


def test_single_incremental_daily_bar_with_missing_ohl_keeps_its_series():
    payload = chart()
    result = payload["chart"]["result"][0]
    result["timestamp"] = result["timestamp"][-1:]
    for key in result["indicators"]["quote"][0]:
        result["indicators"]["quote"][0][key] = result["indicators"]["quote"][0][key][-1:]
    for key in ("open", "high", "low"):
        result["indicators"]["quote"][0][key] = [None]
    _, points = parse_chart(
        payload, symbol="^SOX", market="US", fetched_at=1791210000000, capability="daily_history"
    )
    assert points[0]["data_kind"] == "daily_bar"
    assert points[0]["interval"] == "1d"
    assert points[0]["open"] is None
    assert points[0]["close"] == 105


def test_single_missing_daily_close_is_retained_as_an_explicit_gap():
    payload = chart()
    result = payload["chart"]["result"][0]
    result["timestamp"] = result["timestamp"][-1:]
    for key in result["indicators"]["quote"][0]:
        result["indicators"]["quote"][0][key] = result["indicators"]["quote"][0][key][-1:]
    result["indicators"]["quote"][0]["close"] = [None]
    _, points = parse_chart(
        payload, symbol="^SOX", market="US", fetched_at=1791210000000, capability="daily_history"
    )
    assert points[0]["data_kind"] == "daily_bar"
    assert points[0]["close"] is None
    assert points[0]["ts"] == 1791207000000


@pytest.mark.asyncio
async def test_transient_host_failure_retries_another_host():
    hosts = []

    def respond(request):
        hosts.append(request.url.host)
        if len(hosts) == 1:
            return httpx.Response(502)
        return httpx.Response(200, json=copy.deepcopy(chart()))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = YahooIndexClient(proxy=None, client=http)
        result = await client.fetch("^SOX", "US")
    assert hosts == ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]
    assert len(result["points"]) == 2


@pytest.mark.asyncio
async def test_permanent_rejection_does_not_flood_other_host():
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(403, text="service unavailable in this region")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with pytest.raises(YahooIndexError, match="403"):
            await YahooIndexClient(proxy=None, client=http).fetch("^SOX", "US")
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_rate_limit_cooldown_is_shared_with_other_index_requests():
    calls = []
    loop = asyncio.get_running_loop()

    def respond(request):
        calls.append((request.url.host, loop.time()))
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "1"})
        return httpx.Response(200, json=copy.deepcopy(chart()))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = YahooIndexClient(proxy=None, client=http)
        first = asyncio.create_task(client.fetch("^SOX", "US"))
        await asyncio.sleep(0)
        second = asyncio.create_task(client.fetch("^SOX", "US"))
        results = await asyncio.gather(first, second)
    assert len(calls) == 3
    assert all(stamp - calls[0][1] >= 0.9 for _, stamp in calls[1:])
    assert all(len(result["points"]) == 2 for result in results)


@pytest.mark.asyncio
async def test_initial_history_request_uses_explicit_full_daily_period():
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, json=copy.deepcopy(chart()))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        await YahooIndexClient(proxy=None, client=http).fetch("^SOX", "US")
    assert requests[0].url.params["period1"] == "0"
    assert int(requests[0].url.params["period2"]) > 1790000000
    assert requests[0].url.params["interval"] == "1d"
    assert "range" not in requests[0].url.params
