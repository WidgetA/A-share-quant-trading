"""FC source transport, complete stream validation and retry behavior."""

import copy
import hashlib
import io
import json
from types import SimpleNamespace

import httpx
import pytest

from src.data import fc_yahoo_indices as fc_module
from src.data.fc_yahoo_indices import FCYahooIndexClient, FCYahooIndexError


def source_chart(symbol="^SOX", market="US", rows=2):
    timestamps = [900000000 + number * 86400 for number in range(rows)]
    return {
        "chart": {
            "error": None,
            "result": [
                {
                    "meta": {
                        "symbol": symbol,
                        "shortName": "한글 source",
                        "instrumentType": "INDEX",
                        "currency": "USD" if market == "US" else "KRW",
                        "exchangeTimezoneName": "America/New_York"
                        if market == "US"
                        else "Asia/Seoul",
                        "firstTradeDate": timestamps[0],
                        "dataGranularity": "1d",
                        "regularMarketTime": timestamps[-1],
                    },
                    "timestamp": timestamps,
                    "indicators": {
                        "quote": [
                            {
                                "open": [100] * rows,
                                "high": [103] * rows,
                                "low": [98] * rows,
                                "close": [102] * rows,
                                "volume": [0] * rows,
                            }
                        ]
                    },
                }
            ],
        }
    }


def envelope(payload, source=None, **changes):
    raw = json.dumps(source if source is not None else source_chart(), ensure_ascii=False)
    value = {
        **payload,
        "fetched_at": 1791210000123,
        "raw_json": raw,
        "payload_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        "request_url": "https://query1.finance.yahoo.com/v8/finance/chart/%5ESOX",
        "runtime": {"region": "us-west-1", "fc_request_id": "native-execution-id"},
    }
    value.update(changes)
    return value


def response(value=None, *, status=200, headers=None, body=None):
    if body is None:
        body = io.BytesIO(json.dumps(value, ensure_ascii=False).encode("utf-8"))
    return SimpleNamespace(status_code=status, headers=headers or {}, body=body)


def client(invoke, **kwargs):
    return FCYahooIndexClient("https://account.us-west-1.fc.aliyuncs.com", invoke=invoke, **kwargs)


@pytest.mark.asyncio
async def test_reads_every_short_stream_chunk_and_keeps_full_history_and_source_clocks():
    seen = []

    class ShortReads(io.BytesIO):
        def read(self, size=-1):
            return super().read(min(97, size))

    def invoke(payload):
        source = source_chart(rows=9000)
        result = source["chart"]["result"][0]
        result["indicators"]["quote"][0]["close"][7] = None
        value = envelope(payload, source)
        # Remote parsed fields cannot replace independent local parsing.
        value["points"] = [{"close": 1}]
        stream = ShortReads(json.dumps(value, ensure_ascii=False).encode("utf-8"))
        seen.append((copy.deepcopy(payload), value, stream))
        return response(body=stream)

    result = await client(invoke).fetch("^SOX", "US", start=0, capability="daily_history")
    sent, original, stream = seen[0]
    assert len(result["points"]) == 9000
    assert result["points"][0]["ts"] == 900000000000
    assert result["points"][-1]["ts"] == (900000000 + 8999 * 86400) * 1000
    assert result["missing_close_timestamps"] == [(900000000 + 7 * 86400) * 1000]
    assert result["points"][7]["close"] is None
    assert result["fetched_at"] == result["points"][-1]["fetched_at"] == 1791210000123
    assert result["raw_json"] == original["raw_json"]
    assert result["payload_sha256"] == hashlib.sha256(result["raw_json"].encode()).hexdigest()
    assert result["runtime"]["region"] == "us-west-1" and stream.closed
    assert sent["start"] == 0 and sent["request_id"] == result["request_id"]


@pytest.mark.asyncio
async def test_korean_published_snapshot_preserves_unknown_fields():
    def invoke(payload):
        source = source_chart("KOSPI-25.KS", "KR", 1)
        result = source["chart"]["result"][0]
        for key in ("open", "high", "low", "volume"):
            result["indicators"]["quote"][0][key] = [0]
        return response(envelope(payload, source))

    result = await client(invoke).fetch("KOSPI-25.KS", "KR", capability="snapshot_only")
    point = result["points"][0]
    assert point["data_kind"] == "quote_snapshot" and point["interval"] == "quote"
    assert point["close"] == 102
    assert all(point[field] is None for field in ("open", "high", "low", "volume", "is_final"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": True},
        {"request_id": "other"},
        {"symbol": "^BKX"},
        {"market": "KR"},
        {"start": 1},
        {"capability": "snapshot_only"},
        {"fetched_at": True},
        {"runtime": {"region": "cn-shanghai", "fc_request_id": "id"}},
        {"runtime": {"region": "us-west-1"}},
        {"payload_sha256": "0" * 64},
    ],
)
async def test_mismatched_or_incomplete_success_envelope_is_terminal(change):
    calls = []

    def invoke(payload):
        calls.append(payload)
        return response(envelope(payload, **change))

    with pytest.raises(FCYahooIndexError):
        await client(invoke).fetch("^SOX", "US", capability="daily_history")
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("symbol", "^BKX"),
        ("instrumentType", "ETF"),
        ("currency", "KRW"),
        ("exchangeTimezoneName", "Asia/Seoul"),
        ("dataGranularity", "1mo"),
    ],
)
async def test_raw_source_with_matching_sha_must_pass_local_parser(field, value):
    def invoke(payload):
        source = source_chart()
        source["chart"]["result"][0]["meta"][field] = value
        return response(envelope(payload, source))

    with pytest.raises(FCYahooIndexError, match="local validation"):
        await client(invoke).fetch("^SOX", "US", capability="daily_history")


@pytest.mark.asyncio
@pytest.mark.parametrize("error_header", ["X-Fc-Error", "X-Fc-Error-Type"])
async def test_http_200_function_error_is_not_a_success_or_a_retry(error_header):
    calls = []
    streams = []

    def invoke(payload):
        calls.append(payload)
        # Function error headers must win even if the body resembles a success.
        value = envelope(payload)
        value["private_error_text"] = "private-user:secret-password"
        body = io.BytesIO(json.dumps(value).encode())
        streams.append(body)
        return response(body=body, headers={error_header: "UnhandledInvocationError"})

    with pytest.raises(FCYahooIndexError) as failure:
        await client(invoke).fetch("^SOX", "US", capability="daily_history")
    assert len(calls) == 1 and streams[0].closed
    assert failure.value.status_code == 200
    assert "private-user" not in str(failure.value) and "secret-password" not in str(failure.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["http429", "http503", "sdk503", "transport", "stream"])
async def test_transient_retry_reuses_entire_request_with_same_id(monkeypatch, failure_kind):
    calls, sleeps, clock = [], [], [10.0]
    monkeypatch.setattr(fc_module.time, "monotonic", lambda: clock[0])

    async def sleep(delay):
        sleeps.append(delay)
        clock[0] += delay

    monkeypatch.setattr(fc_module.asyncio, "sleep", sleep)

    class SDKFailure(Exception):
        data = {"statusCode": 503}

    class FailedStream(io.BytesIO):
        def read(self, size=-1):
            raise ConnectionResetError("http://private-user:secret-password@proxy")

    def invoke(payload):
        calls.append(copy.deepcopy(payload))
        if len(calls) == 1:
            if failure_kind == "http429":
                return response(
                    status=429, headers={"Retry-After": "3"}, body=io.BytesIO(b"private")
                )
            if failure_kind == "http503":
                return response(status=503, body=io.BytesIO(b"private"))
            if failure_kind == "sdk503":
                raise SDKFailure("http://private-user:secret-password@proxy")
            if failure_kind == "stream":
                return response(body=FailedStream())
            raise httpx.ReadTimeout("http://private-user:secret-password@proxy")
        return response(envelope(payload))

    result = await client(invoke).fetch("^SOX", "US", start=900000000, capability="daily_history")
    assert len(calls) == 2 and calls[0] == calls[1]
    assert result["request_id"] == calls[0]["request_id"]
    assert sleeps == ([3.0] if failure_kind == "http429" else [1])


@pytest.mark.asyncio
async def test_transport_retries_are_finite_and_errors_do_not_expose_original_text(monkeypatch):
    calls = []

    async def sleep(_):
        pass

    monkeypatch.setattr(fc_module.asyncio, "sleep", sleep)

    def invoke(payload):
        calls.append(copy.deepcopy(payload))
        raise httpx.ReadTimeout("http://private-user:secret-password@proxy")

    with pytest.raises(FCYahooIndexError) as failure:
        await client(invoke, max_attempts=3).fetch("^SOX", "US", capability="daily_history")
    assert len(calls) == 3 and all(payload == calls[0] for payload in calls)
    assert "secret-password" not in str(failure.value) and "http://" not in str(failure.value)
    assert failure.value.__suppress_context__


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [202, 400, 401, 403, 404])
async def test_permanent_http_error_never_retries(status):
    calls = []

    def invoke(payload):
        calls.append(payload)
        return response(status=status, body=io.BytesIO(b"private"))

    with pytest.raises(FCYahooIndexError):
        await client(invoke).fetch("^SOX", "US", capability="daily_history")
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [b"not JSON", b'{"incomplete":', b"\xff"])
async def test_invalid_or_truncated_stream_never_becomes_source_points(body):
    with pytest.raises(FCYahooIndexError):
        await client(lambda _: response(body=io.BytesIO(body))).fetch(
            "^SOX", "US", capability="daily_history"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("start", [True, -1, 1.5, "900000000"])
async def test_invalid_start_seconds_is_rejected_before_invocation(start):
    calls = []
    with pytest.raises(ValueError):
        await client(lambda payload: calls.append(payload)).fetch(
            "^SOX", "US", start=start, capability="daily_history"
        )
    assert calls == []


def test_training_function_or_wrong_region_cannot_be_invoked():
    with pytest.raises(ValueError):
        FCYahooIndexClient("fcv3.us-west-1.aliyuncs.com", "ashare_mltrain", invoke=lambda _: None)
    with pytest.raises(ValueError):
        FCYahooIndexClient(
            "fcv3.us-west-1.aliyuncs.com", region="cn-shanghai", invoke=lambda _: None
        )


def test_credentialed_endpoint_is_rejected_without_printing_its_secret():
    with pytest.raises(ValueError) as failure:
        FCYahooIndexClient("https://private-user:secret-password@fc.example", invoke=lambda _: None)
    assert "private-user" not in str(failure.value) and "secret-password" not in str(failure.value)
