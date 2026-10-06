"""Native FC behavior using exact captured Yahoo bytes, without live requests.

SOX: captured 2026-10-05, sha d705b71be6a3f44dd9c776c028d9b1e43a4e6c59a98e026383d0291ecc1f3241.
KR KOSPI-25: captured 2026-10-05,
sha 99be50a26758412b1610b50c864c781b251c89fca8377c4a92b1f1791db12e03.
The source sample period is a fixture, not a production history-range limit.
"""

import asyncio
import hashlib
import importlib
import json
from types import SimpleNamespace

import httpx
import pytest

from src.data.fc_yahoo_worker import (
    FCYahooRequestError,
    FCYahooWorker,
    fetch_envelope,
    validate_request,
)
from src.data.yahoo_indices import YahooIndexClient, YahooIndexError

SOX_RAW = (
    '{"chart":{"result":[{"meta":{"currency":"USD","symbol":"^SOX","exchangeName":"NIM","full'
    'ExchangeName":"Nasdaq GIDS","instrumentType":"INDEX","firstTradeDate":768058200,"regular'
    'MarketTime":1791215934,"hasPrePostMarketData":false,"gmtoffset":-14400,"timezone":"EDT",'
    '"exchangeTimezoneName":"America/New_York","regularMarketPrice":13128.23,"regularMarketCh'
    'angePercent":-0.064,"fulldayPrice":13128.23,"fulldayChange":-8.444,"fulldayChangePercent'
    '":-0.064,"fiftyTwoWeekHigh":14655.29,"fiftyTwoWeekLow":6160.05,"regularMarketDayHigh":13'
    '152.674,"regularMarketDayLow":13004.668,"regularMarketVolume":0,"longName":"PHLX Semicon'
    'ductor","shortName":"PHLX Semiconductor","chartPreviousClose":11735.26,"priceHint":2,"cu'
    'rrentTradingPeriod":{"pre":{"timezone":"EDT","start":1791187200,"end":1791207000,"gmtoff'
    'set":-14400},"regular":{"timezone":"EDT","start":1791207000,"end":1791230400,"gmtoffset"'
    ':-14400},"post":{"timezone":"EDT","start":1791230400,"end":1791244800,"gmtoffset":-14400'
    '}},"dataGranularity":"1d","range":"1mo","validRanges":["1d","5d","1mo","3mo","6mo","1y",'
    '"2y","5y","10y","ytd","max"]},"timestamp":[1788874200,1788960600,1789047000,1789133400,1'
    '789392600,1789479000,1789565400,1789651800,1789738200,1789997400,1790083800,1790170200,1'
    '790256600,1790343000,1790602200,1790688600,1790775000,1790861400,1790947800,1791207000],'
    '"indicators":{"quote":[{"low":[11843.259765625,11854.2197265625,11561.0400390625,11711.3'
    '30078125,11110.1103515625,11128.8798828125,11132.259765625,11533.9501953125,11686.089843'
    '75,12059.2998046875,12338.51953125,12371.3701171875,12258.3896484375,12549.58984375,1227'
    '7.41015625,12588.8603515625,12555.650390625,12587.33984375,13082.009765625,13004.6679687'
    '5],"high":[12023.8603515625,12016.08984375,11733.6796875,11910.2001953125,11285.29980468'
    '75,11304.240234375,11413.4599609375,11643.8798828125,11922.9501953125,12491.6103515625,1'
    '2708.33984375,12660.91015625,12517.919921875,12732.7802734375,12665.400390625,12773.8896'
    '484375,12731.099609375,12913.400390625,13269.330078125,13152.673828125],"volume":[0,0,0,'
    '0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"open":[11987.2900390625,11866.5,11663.0,11753.870117'
    '1875,11191.8203125,11233.0400390625,11347.6796875,11568.4697265625,11691.0,12145.9199218'
    '75,12338.8798828125,12659.080078125,12309.08984375,12565.83984375,12595.1396484375,12667'
    '.3701171875,12665.669921875,12651.5595703125,13130.759765625,13148.8427734375],"close":['
    '11887.8701171875,11931.3203125,11614.169921875,11824.0,11131.2802734375,11175.5498046875'
    ',11246.1103515625,11599.490234375,11921.6904296875,12433.169921875,12689.8203125,12534.2'
    '802734375,12492.5400390625,12668.9296875,12465.240234375,12629.16015625,12628.6201171875'
    ',12829.0,13136.669921875,13128.23046875]}],"adjclose":[{"adjclose":[11887.8701171875,119'
    '31.3203125,11614.169921875,11824.0,11131.2802734375,11175.5498046875,11246.1103515625,11'
    '599.490234375,11921.6904296875,12433.169921875,12689.8203125,12534.2802734375,12492.5400'
    '390625,12668.9296875,12465.240234375,12629.16015625,12628.6201171875,12829.0,13136.66992'
    '1875,13128.23046875]}]}}],"error":null}}'
)
SOX_SHA = "d705b71be6a3f44dd9c776c028d9b1e43a4e6c59a98e026383d0291ecc1f3241"
KR_RAW = (
    '{"chart":{"result":[{"meta":{"currency":"KRW","symbol":"KOSPI-25.KS","exchangeName":"KSC'
    '","fullExchangeName":"KSE","instrumentType":"INDEX","firstTradeDate":null,"regularMarket'
    'Time":1790939140,"hasPrePostMarketData":false,"gmtoffset":32400,"timezone":"KST","exchan'
    'geTimezoneName":"Asia/Seoul","regularMarketPrice":46539.14,"regularMarketChangePercent":'
    '0.93,"fulldayPrice":46539.14,"fulldayChange":428.742,"fulldayChangePercent":0.93,"fiftyT'
    'woWeekHigh":0.0,"fiftyTwoWeekLow":0.0,"regularMarketDayHigh":0.0,"regularMarketDayLow":0'
    '.0,"regularMarketVolume":0,"longName":"KOSPI Financial Companies - Ins","shortName":"KOS'
    'PI Financial Companies - Ins","chartPreviousClose":46110.4,"priceHint":2,"currentTrading'
    'Period":{"pre":{"timezone":"KST","start":1791239400,"end":1791244800,"gmtoffset":32400},'
    '"regular":{"timezone":"KST","start":1791244800,"end":1791266400,"gmtoffset":32400},"post'
    '":{"timezone":"KST","start":1791266400,"end":1791277200,"gmtoffset":32400}},"dataGranula'
    'rity":"1d","range":"1y","validRanges":["1d","5d"]},"timestamp":[1790939140],"indicators"'
    ':{"quote":[{"volume":[0],"low":[0.0],"open":[0.0],"close":[46539.140625],"high":[0.0]}],'
    '"adjclose":[{"adjclose":[46539.140625]}]}}],"error":null}}'
)
KR_SHA = "99be50a26758412b1610b50c864c781b251c89fca8377c4a92b1f1791db12e03"


def request(symbol="^SOX", market="US", capability="daily_history", start=None):
    return {
        "schema_version": 1,
        "request_id": "invoke-行业-index",
        "symbol": symbol,
        "market": market,
        "start": start,
        "capability": capability,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_raw,digest,event",
    [
        (SOX_RAW, SOX_SHA, request()),
        (KR_RAW, KR_SHA, request("KOSPI-25.KS", "KR", "snapshot_only", 1790930000)),
    ],
)
async def test_saved_yahoo_responses_are_complete_raw_envelopes(source_raw, digest, event):
    calls = []

    def respond(req):
        calls.append(req)
        return httpx.Response(
            200, content=source_raw.encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        yahoo = YahooIndexClient(proxy=None, client=http)
        result = await fetch_envelope(
            event, yahoo=yahoo, fc_request_id="fc-physical-invoke", region="us-west-1",
        )
    assert len(calls) == 1
    assert dict(calls[0].url.params)["period1"] == str(event["start"] or 0)
    assert "range" not in calls[0].url.params
    assert calls[0].url.params["interval"] == "1d"
    assert result["raw_json"].encode("utf-8") == source_raw.encode("utf-8")
    assert result["payload_sha256"] == digest == hashlib.sha256(source_raw.encode()).hexdigest()
    assert result["request_url"] == str(calls[0].url)
    assert type(result["fetched_at"]) is int and result["fetched_at"] > 0
    for key, value in event.items():
        assert result[key] == value
    assert result["runtime"] == {"region": "us-west-1", "fc_request_id": "fc-physical-invoke"}
    assert set(result) == set(event) | {
        "fetched_at", "raw_json", "payload_sha256", "request_url", "runtime",
    }
    # Source history/quote shape survives remote transport, including the Korean
    # source placeholders. Only the domestic parser decides storage observations.
    original = json.loads(source_raw)["chart"]["result"][0]
    returned = json.loads(result["raw_json"])["chart"]["result"][0]
    assert returned["timestamp"] == original["timestamp"]
    assert returned["indicators"] == original["indicators"]


@pytest.mark.parametrize(
    "field", ["schema_version", "request_id", "symbol", "market", "start", "capability"],
)
def test_missing_contract_fields_fail_before_a_fetch(field):
    event = request()
    del event[field]
    with pytest.raises(FCYahooRequestError):
        validate_request(json.dumps(event).encode())


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", True), ("schema_version", 2), ("request_id", ""),
        ("symbol", None), ("market", "CN"), ("start", True), ("start", -1),
        ("start", 123.5), ("capability", "monthly_history"),
    ],
)
def test_invalid_request_types_and_units_are_rejected(field, value):
    event = request()
    event[field] = value
    with pytest.raises(FCYahooRequestError):
        validate_request(event)


@pytest.mark.parametrize("event", [b"not JSON", b"[]", "null", 123])
def test_event_must_be_one_json_object(event):
    with pytest.raises(FCYahooRequestError):
        validate_request(event)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value", [("symbol", "^BKX"), ("instrumentType", "ETF"), ("currency", "KRW")],
)
async def test_source_identity_failure_is_an_exception_not_success(field, value):
    payload = json.loads(SOX_RAW)
    payload["chart"]["result"][0]["meta"][field] = value
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json=payload))
    ) as http:
        with pytest.raises(YahooIndexError):
            await fetch_envelope(
                request(), yahoo=YahooIndexClient(proxy=None, client=http),
                fc_request_id="fc-rejected", region="us-west-1",
            )


class SavedSourceClient:
    def __init__(self, raw=SOX_RAW):
        self.raw = raw
        self.calls = []
        self.closed = False
        self.created_loop = asyncio.get_running_loop()
        self.cooldown_marker = object()

    async def fetch(self, symbol, market, *, start, capability):
        self.calls.append((symbol, market, start, capability, asyncio.get_running_loop()))
        return {
            "raw_json": self.raw,
            "payload_sha256": hashlib.sha256(self.raw.encode()).hexdigest(),
            "fetched_at": 1791215939940,
            "url": "https://query1.finance.yahoo.com/v8/finance/chart/%5ESOX?interval=1d&period1=0&period2=1791215940",
        }

    async def aclose(self):
        self.closed = True


@pytest.mark.asyncio
async def test_envelope_rejects_raw_digest_mismatch():
    client = SavedSourceClient()
    original = client.fetch

    async def corrupt(*args, **kwargs):
        result = await original(*args, **kwargs)
        result["payload_sha256"] = "0" * 64
        return result

    client.fetch = corrupt
    with pytest.raises(YahooIndexError, match="SHA256"):
        await fetch_envelope(request(), yahoo=client, fc_request_id="fc-hash", region="us-west-1")


def test_warm_invocations_reuse_the_same_direct_client_and_own_loop(monkeypatch):
    monkeypatch.setenv("FC_REGION", "us-west-1")
    monkeypatch.setenv("HTTPS_PROXY", "http://must-not-use:9999")
    created = []
    factory_args = []

    def factory(**kwargs):
        factory_args.append(kwargs)
        client = SavedSourceClient()
        created.append(client)
        return client

    worker = FCYahooWorker(client_factory=factory)
    try:
        first = json.loads(worker.invoke(
            json.dumps(request(), ensure_ascii=False).encode(), SimpleNamespace(request_id="fc-1"),
        ))
        second_event = request(start=1790000000)
        second = json.loads(worker.invoke(second_event, SimpleNamespace(request_id="fc-2")))
        assert factory_args == [{"proxy": None}]
        assert len(created) == 1
        client = created[0]
        assert not client.closed
        assert len(client.calls) == 2
        assert all(call[-1] is client.created_loop for call in client.calls)
        assert second["start"] == 1790000000
        assert first["runtime"]["fc_request_id"] == "fc-1"
        assert second["runtime"]["fc_request_id"] == "fc-2"
        assert first["raw_json"] == second["raw_json"] == SOX_RAW
    finally:
        worker.close()
    assert created[0].closed


def test_native_handler_returns_utf8_json_and_propagates_failures(monkeypatch, capsys):
    module = importlib.import_module("serverless.yahoo_indices.handler")
    created = []

    def factory(**kwargs):
        client = SavedSourceClient()
        created.append(client)
        return client

    worker = FCYahooWorker(client_factory=factory)
    monkeypatch.setattr(module, "_worker", worker)
    monkeypatch.setenv("FC_REGION", "us-west-1")
    try:
        response = module.handler(
            json.dumps(request(), ensure_ascii=False).encode("utf-8"),
            SimpleNamespace(request_id="fc-handler"),
        )
        assert isinstance(response, str)
        assert "行业" in response
        assert json.loads(response)["payload_sha256"] == SOX_SHA
        malformed = request()
        malformed["schema_version"] = 99
        with pytest.raises(FCYahooRequestError):
            module.handler(json.dumps(malformed).encode(), SimpleNamespace(request_id="fc-error"))

        async def fail(*args, **kwargs):
            raise YahooIndexError("Yahoo HTTP 429")

        created[0].fetch = fail
        with pytest.raises(YahooIndexError, match="429"):
            module.handler(
                json.dumps(request()).encode(), SimpleNamespace(request_id="fc-source-error"),
            )
        assert capsys.readouterr().out == ""
    finally:
        worker.close()


def test_default_direct_client_ignores_environment_proxy(monkeypatch):
    import src.data.yahoo_indices as yahoo_module
    actual_client = httpx.AsyncClient
    observed = []

    def factory(**kwargs):
        observed.append(kwargs)
        kwargs["transport"] = httpx.MockTransport(
            lambda req: httpx.Response(200, content=SOX_RAW.encode())
        )
        return actual_client(**kwargs)

    monkeypatch.setattr(yahoo_module.httpx, "AsyncClient", factory)
    monkeypatch.setenv("FC_REGION", "us-west-1")
    monkeypatch.setenv("HTTPS_PROXY", "http://must-not-use:9999")
    worker = FCYahooWorker()
    try:
        result = json.loads(worker.invoke(request(), SimpleNamespace(request_id="fc-egress")))
        assert result["payload_sha256"] == SOX_SHA
        assert observed[0]["proxy"] is None
        assert observed[0]["trust_env"] is False
    finally:
        worker.close()
