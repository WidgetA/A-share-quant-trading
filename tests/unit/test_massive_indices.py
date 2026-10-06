"""Source identity, sparse real bars, and restart-safe API allowance."""

import hashlib
import json
from pathlib import Path

import httpx
import pytest

from src.data.cross_market_massive_ingest import load_massive_reference
from src.data.cross_market_store import PRICE_COLUMNS
from src.data.massive_indices import MassiveIndexClient, MassiveIndexError, parse_aggregates


@pytest.fixture
def index():
    return {
        "index_id": "US:YAHOO:^BKX",
        "market": "US",
        "symbol": "^BKX",
        "name": "KBW Nasdaq Bank Index",
        "native_symbol": "BKX",
        "instrument_type": "INDEX",
        "currency": "USD",
        "exchange_timezone": "America/New_York",
        "vendor_ticker": "I:BKX",
        "official_name": "KBW Nasdaq Bank Index",
        "vendor_record": {
            "ticker": "I:BKX",
            "name": "KBW Nasdaq Bank Index",
            "market": "indices",
            "locale": "us",
            "source_feed": "NasdaqGIDS",
        },
        "same_index_status": "verified",
        "return_basis_status": "verified",
        "fullname_match": True,
        "return_basis": "price_return",
    }


@pytest.fixture
def payload():
    # First two actual bars of the independently read-back BKX 2023Q1 source.
    return {
        "ticker": "I:BKX",
        "status": "OK",
        "queryCount": 10,
        "count": 2,
        "results": [
            {
                "o": 112.37022641684,
                "c": 112.61957278797,
                "h": 112.61957278797,
                "l": 112.32627516828,
                "t": 1676471400000,
            },
            {
                "o": 112.61673069147,
                "c": 112.45758417453,
                "h": 112.67478095053,
                "l": 112.44963892981,
                "t": 1676471700000,
            },
        ],
    }


def parse(payload, index):
    return parse_aggregates(
        payload, index, start_date="2023-01-01", end_date="2023-03-31", fetched_at=1791310000000
    )


def test_real_ohlc_exact_18_fields_and_unavailable_fields_null(index, payload):
    result = parse(payload, index)
    assert result["source_first_date"] == "2023-02-15"
    assert not result["truncated"]
    row = result["points"][0]
    assert set(row) == set(PRICE_COLUMNS)
    assert row["provider"] == "massive" and row["symbol"] == "^BKX"
    assert row["close"] == payload["results"][0]["c"]
    assert row["volume"] is row["adjusted_close"] is row["is_final"] is None


def test_single_and_sparse_five_minute_bars_are_preserved(index, payload):
    payload["results"] = payload["results"][:1]
    payload["count"] = 1
    assert len(parse(payload, index)["points"]) == 1
    second = dict(payload["results"][0], t=payload["results"][0]["t"] + 600000)
    payload["results"].append(second)
    payload["count"] = 2
    assert len(parse(payload, index)["points"]) == 2


def test_delayed_success_keeps_actual_bars_and_actual_last_date(index):
    # Three unmodified source bars from the actual 237-bar BKX response;
    # only count/results are subsetted. Full RAW SHA c3622924e5c17efc...
    delayed = {
        "ticker": "I:BKX",
        "queryCount": 1181,
        "status": "DELAYED",
        "error": None,
        "count": 3,
        "results": [
            {
                "o": 169.0206873393,
                "c": 169.64038380546,
                "h": 169.74318420296,
                "l": 169.01193617496,
                "t": 1790861400000,
            },
            {
                "o": 169.63288159028,
                "c": 168.98731664542,
                "h": 169.64107184358,
                "l": 168.98731664542,
                "t": 1790861700000,
            },
            {
                "o": 170.65120601089,
                "c": 170.60743756048,
                "h": 170.65120601089,
                "l": 170.60743756048,
                "t": 1791230400000,
            },
        ],
    }
    kwargs = {"start_date": "2026-10-01", "end_date": "2026-10-06", "fetched_at": 1791327018377}
    result = parse_aggregates(delayed, index, **kwargs)
    assert (
        result["points"] == parse_aggregates(delayed | {"status": "OK"}, index, **kwargs)["points"]
    )
    assert result["source_first_date"] == "2026-10-01"
    assert result["source_last_date"] == "2026-10-05"
    assert result["source_empty"] is False


@pytest.mark.parametrize(
    "mutation",
    [
        {"error": "NOT_AUTHORIZED"},
        {"ticker": "I:BKXTR"},
    ],
)
def test_delayed_does_not_bypass_failure_or_identity_checks(index, payload, mutation):
    with pytest.raises(MassiveIndexError):
        parse(payload | {"status": "DELAYED"} | mutation, index)


@pytest.mark.parametrize(
    "mutation",
    [
        {"return_basis_status": "pending_value_crosscheck"},
        {"fullname_match": 1},
        {"return_basis": "gross_total_return"},
        {"native_symbol": "BKXTR"},
        {"symbol": "BKX_ETF"},
        {"vendor_ticker": "I:SOX"},
    ],
)
def test_no_pending_return_etf_or_guessed_vendor_identity(index, payload, mutation):
    with pytest.raises(MassiveIndexError, match="unverified_original_index_identity"):
        parse(payload, index | mutation)


def test_source_identity_empty_and_base_limit_are_different(index, payload):
    empty = {"ticker": "I:BKX", "status": "OK", "queryCount": 0, "request_id": "source"}
    assert parse(empty, index)["source_empty"] is True
    assert parse(empty, index)["points"] == []
    with pytest.raises(MassiveIndexError, match="source_identity_mismatch"):
        parse(empty | {"ticker": "I:BKXTR"}, index)
    with pytest.raises(MassiveIndexError, match="source_error"):
        parse(empty | {"status": "NOT_AUTHORIZED"}, index)
    assert parse(payload | {"queryCount": 50000}, index)["truncated"] is True
    assert parse(payload | {"next_url": "https://api.massive.com/next"}, index)["truncated"]


@pytest.mark.parametrize(
    "change",
    [
        {"t": 1676471400001},
        {"t": 1673793000000},
        {"h": 1},
        {"c": float("nan")},
    ],
)
def test_bad_ohlc_timestamp_or_requested_date_fails(index, payload, change):
    if change.get("t") == 1673793000000:  # date is inside Q1: explicitly use an outside year
        change = {"t": 1644935400000}
    payload["results"][0].update(change)
    with pytest.raises(MassiveIndexError):
        parse(payload, index)


def test_formal_124_are_original_indices_and_return_verified():
    root = Path(__file__).resolve().parents[2]
    ref = load_massive_reference(
        root / "src/data/reference/cross_market/massive_indices.json",
        root / "src/data/reference/cross_market/industry_indices.json",
    )
    assert len(ref["indices"]) == 124
    assert not {x["symbol"] for x in ref["indices"]} & {"^DJUSAL", "^DRG"}


class Clock:
    def __init__(self):
        self.now = 1000.0
        self.waits = []

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        self.waits.append(seconds)
        self.now += seconds


@pytest.mark.asyncio
async def test_rate_checkpoint_precedes_send_and_survives_restart(tmp_path, index, payload):
    key_file = tmp_path / "external.env"
    key_file.write_text('export MASSIVE_API_KEY="fake-unit-key"\n', encoding="utf8")
    clock, sends = Clock(), []
    rate = tmp_path / "rate.json"

    def handler(request):
        assert json.loads(rate.read_text())["next_allowed_at"] == clock.now + 13
        assert request.headers["Authorization"] == "Bearer fake-unit-key"
        assert "fake-unit-key" not in str(request.url)
        sends.append(clock.now)
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        for _ in range(6):
            # Six separate instances model process restarts sharing the same marker.
            client = MassiveIndexClient(
                key_file=key_file, rate_state_path=rate, client=http, clock=clock, sleep=clock.sleep
            )
            await client.fetch(index, start_date="2023-01-01", end_date="2023-03-31")
    assert sends == [1000, 1013, 1026, 1039, 1052, 1065]
    assert max(sum(t <= x < t + 60 for x in sends) for t in sends) == 5


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,classification",
    [
        (401, "authentication_denied"),
        (403, "permission_denied"),
        (429, "rate_limited"),
    ],
)
async def test_http_error_is_not_empty_and_key_echo_is_redacted(
    tmp_path,
    index,
    status,
    classification,
):
    key_file = tmp_path / "external.env"
    key_file.write_text("MASSIVE_API_KEY=fake-unit-key\n", encoding="utf8")
    clock = Clock()
    source = json.dumps(
        {"status": "ERROR", "message": "fake-unit-key", "request_id": "prefix-fake-unit-key"}
    ).encode()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                status,
                content=source,
                headers={"Retry-After": "120"},
            )
        )
    ) as http:
        client = MassiveIndexClient(
            key_file=key_file,
            rate_state_path=tmp_path / "rate.json",
            client=http,
            clock=clock,
            sleep=clock.sleep,
        )
        with pytest.raises(MassiveIndexError) as error:
            await client.fetch(index, start_date="2023-01-01", end_date="2023-03-31")
    assert error.value.classification == classification
    evidence = error.value.evidence
    assert "fake-unit-key" not in json.dumps(evidence)
    assert evidence["source_payload_sha256"] == hashlib.sha256(source).hexdigest()
    assert evidence["credential_redacted"]
    if status == 429:
        assert json.loads((tmp_path / "rate.json").read_text())["next_allowed_at"] == 1120
