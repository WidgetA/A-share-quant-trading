import hashlib
import io
import json

import pytest

from src.data.fc_intraday_indices import FCYahooIntradayIndexClient
from src.data.fc_yahoo_indices import FCYahooIndexError
from src.data.fc_yahoo_worker import FCYahooRequestError, fetch_envelope, validate_request


def source(empty=False):
    result = {
        "meta": {
            "symbol": "KOSPI-10.KS",
            "instrumentType": "INDEX",
            "currency": "KRW",
            "exchangeTimezoneName": "Asia/Seoul",
            "dataGranularity": "1m",
            "shortName": "KOSPI Non-metallic Mineral Prod",
            "regularMarketTime": 1791244920,
        },
        "indicators": {"quote": [{}]},
    }
    if not empty:
        result["timestamp"] = [1791244800, 1791244860]
        result["indicators"]["quote"] = [
            {
                "open": [10.0, 10.5],
                "high": [11.0, 11.5],
                "low": [9.0, 10.0],
                "close": [10.5, 11.0],
                "volume": [0, 4],
            }
        ]
    return json.dumps({"chart": {"result": [result], "error": None}}, separators=(",", ":"))


def response(payload, raw, **changes):
    envelope = {
        **payload,
        "raw_json": raw,
        "payload_sha256": hashlib.sha256(raw.encode()).hexdigest(),
        "fetched_at": 1791244920000,
        "request_url": "https://query1.finance.yahoo.com/v8/finance/chart/KOSPI-10.KS",
        "runtime": {"region": "us-west-1", "fc_request_id": "actual-native-request"},
        **changes,
    }
    return {"status_code": 200, "headers": {}, "body": io.BytesIO(json.dumps(envelope).encode())}


@pytest.mark.asyncio
@pytest.mark.parametrize("empty", [False, True])
async def test_domestic_minute_parser_receives_native_raw_and_source_empty(empty):
    raw = source(empty)
    client = FCYahooIntradayIndexClient("fc.example", invoke=lambda p: response(p, raw))
    result = await client.fetch("KOSPI-10.KS", "KR", start=1791244800, end=1791245000)
    assert result["raw_json"] == raw
    assert result["source_empty"] is empty
    assert len(result["points"]) == (0 if empty else 2)
    assert result["requested_range"] == {"start": 1791244800, "end": 1791245000, "interval": "1m"}
    if not empty:
        assert {p["data_kind"] for p in result["points"]} == {"minute_bar"}
        assert {p["interval"] for p in result["points"]} == {"1m"}


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("end", 1791246000), ("interval", "5m"), ("start", True)])
async def test_another_window_or_interval_cannot_be_accepted(field, value):
    client = FCYahooIntradayIndexClient(
        "fc.example",
        invoke=lambda p: response(p, source(), **{field: value}),
    )
    with pytest.raises(FCYahooIndexError):
        await client.fetch("KOSPI-10.KS", "KR", start=1791244800, end=1791245000)


@pytest.mark.asyncio
async def test_worker_minute_request_explicit_window_is_forwarded_and_raw_returned():
    request = {
        "schema_version": 1,
        "request_id": "same-id",
        "symbol": "KOSPI-10.KS",
        "market": "KR",
        "capability": "minute_history",
        "start": 1791244800,
        "end": 1791245000,
        "interval": "1m",
    }
    calls = []

    class Minute:
        async def fetch(self, *args, **kwargs):
            calls.append((args, kwargs))
            raw = source()
            return {
                "raw_json": raw,
                "payload_sha256": hashlib.sha256(raw.encode()).hexdigest(),
                "fetched_at": 1791244920000,
                "url": "https://query1.finance.yahoo.com/chart",
            }

    result = await fetch_envelope(
        request, yahoo=Minute(), fc_request_id="fc-id", region="us-west-1"
    )
    assert calls == [
        (("KOSPI-10.KS", "KR"), {"start": 1791244800, "end": 1791245000, "interval": "1m"})
    ]
    assert result["end"] == request["end"] and result["interval"] == "1m"


@pytest.mark.parametrize(
    "field,value", [("end", None), ("end", True), ("interval", "1d"), ("start", None)]
)
def test_worker_rejects_invalid_minute_window(field, value):
    request = {
        "schema_version": 1,
        "request_id": "same-id",
        "symbol": "^SOX",
        "market": "US",
        "capability": "minute_history",
        "start": 1791244800,
        "end": 1791245000,
        "interval": "1m",
    }
    request[field] = value
    with pytest.raises(FCYahooRequestError):
        validate_request(request)
