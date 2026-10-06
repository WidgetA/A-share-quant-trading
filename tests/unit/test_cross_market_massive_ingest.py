"""Requested coverage advances only after durable source and exact read-back."""

import hashlib
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.data.cross_market_massive_ingest import CrossMarketMassiveIngestor, quarter_windows
from src.data.massive_indices import API_ROOT, BASE_LIMIT, MassiveIndexError, parse_aggregates
from tests.unit import test_massive_indices as source_cases


@pytest.fixture
def index():
    return source_cases.index.__wrapped__()


class Source:
    def __init__(self, *, empty=False, truncate=False, failure=None):
        self.calls = []
        self.empty, self.truncate, self.failure = empty, truncate, failure

    async def fetch(self, item, *, start_date, end_date):
        self.calls.append((start_date, end_date))
        if self.failure:
            raise MassiveIndexError(self.failure)
        from urllib.parse import quote

        ts = int(
            datetime.fromisoformat(start_date + "T09:30:00")
            .replace(tzinfo=ZoneInfo("America/New_York"))
            .timestamp()
            * 1000
        )
        bars = (
            []
            if self.empty
            else [
                {"t": ts, "o": 10, "h": 12, "l": 9, "c": 11},
                {"t": ts + 300000, "o": 11, "h": 13, "l": 10, "c": 12},
            ]
        )
        source = {
            "ticker": item["vendor_ticker"],
            "status": "OK",
            "queryCount": (50000 if self.truncate and len(self.calls) == 1 else len(bars) * 5),
            "results": bars,
        }
        raw = json.dumps(source)
        sha = hashlib.sha256(raw.encode()).hexdigest()
        fetched = {
            "schema_version": 1,
            "index_id": item["index_id"],
            "market": item["market"],
            "symbol": item["symbol"],
            "vendor_ticker": item["vendor_ticker"],
            "interval": "5m",
            "start_date": start_date,
            "end_date": end_date,
            "http_status": 200,
            "fetched_at": 1791310000000,
            "raw_json": raw,
            "payload_sha256": sha,
            "source_payload_sha256": sha,
            "credential_redacted": False,
            "request_url": (
                f"{API_ROOT}/v2/aggs/ticker/{quote(item['vendor_ticker'], safe='')}/"
                f"range/5/minute/{start_date}/{end_date}"
            ),
            "request_params": {"sort": "asc", "limit": BASE_LIMIT},
        }
        return fetched | parse_aggregates(
            source, item, start_date=start_date, end_date=end_date, fetched_at=fetched["fetched_at"]
        )


class Store:
    def __init__(self, *, fail_write=False, fail_verify=False):
        self.writes, self.verifies = [], []
        self.fail_write, self.fail_verify = fail_write, fail_verify
        self.before_write = None

    async def ensure_schema(self):
        pass

    async def upsert_prices(self, points):
        if self.before_write:
            self.before_write(points)
        self.writes.append(points)
        if self.fail_write:
            raise RuntimeError("write interrupted")
        return len(points)

    async def verify_prices(self, points):
        self.verifies.append(points)
        if self.fail_verify:
            raise RuntimeError("readback mismatch")
        return len(points)


def producer(tmp_path, index, source, store):
    industry = tmp_path / "industry.json"
    industry.write_text(json.dumps({"indices": [index]}), encoding="utf8")
    reference = tmp_path / "massive.json"
    reference.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "provider": "massive",
                "interval": "5m",
                "industry_reference_sha256": hashlib.sha256(industry.read_bytes()).hexdigest(),
                "indices": [index],
            }
        ),
        encoding="utf8",
    )
    return CrossMarketMassiveIngestor(
        reference_path=reference,
        industry_reference_path=industry,
        state_dir=tmp_path / "state",
        massive=source,
        store=store,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["write", "verify"])
async def test_raw_pending_before_write_and_failure_no_advance_then_raw_replay(
    tmp_path,
    index,
    failure,
):
    source = Source()
    store = Store(fail_write=failure == "write", fail_verify=failure == "verify")
    collector = producer(tmp_path, index, source, store)
    directory = collector.series_dir(index)

    def durable(points):
        pending = json.loads((directory / "pending.json").read_text())
        assert pending["fetched"]["points"] == points
        assert pending["fetched"]["raw_json"]

    store.before_write = durable
    result = await collector.run_once(end_date="2023-01-02")
    assert result["status"] == "partial_failure"
    state = json.loads((directory / "state.json").read_text())
    assert state["covered_until_exclusive"] == "2023-01-01"
    assert state["verified_rows"] == 0 and (directory / "pending.json").is_file()
    original_pending = (directory / "pending.json").read_bytes()
    no_network = Source(failure="network_must_not_be_called")
    restarted = producer(tmp_path, index, no_network, Store())
    result = await restarted.run_once(end_date="2023-01-02")
    assert result["status"] == "verified" and result["results"][0]["replayed"]
    assert not no_network.calls and not (directory / "pending.json").exists()
    receipt = json.loads(next((directory / "receipts").glob("*.json")).read_text())
    assert receipt["verified_rows"] == 2
    assert receipt["fetched"] == json.loads(original_pending)["fetched"]
    assert (
        json.loads((directory / "state.json").read_text())["covered_until_exclusive"]
        == "2023-01-03"
    )


@pytest.mark.asyncio
async def test_true_empty_advances_requested_receipt_without_prices_or_source_first_date(
    tmp_path, index
):
    store = Store()
    collector = producer(tmp_path, index, Source(empty=True), store)
    result = await collector.run_once(end_date="2023-03-31")
    assert result["status"] == "verified" and result["verified_rows"] == 0
    state = json.loads((collector.series_dir(index) / "state.json").read_text())
    assert state["source_first_date"] is state["source_last_date"] is None
    assert state["covered_until_exclusive"] == "2023-04-01"
    assert state["verified_windows"][0]["source_empty"] is True
    assert store.writes == [[]] and store.verifies == [[]]


@pytest.mark.asyncio
async def test_base_aggregate_limit_splits_without_writing_truncated_response(tmp_path, index):
    source, store = Source(truncate=True), Store()
    collector = producer(tmp_path, index, source, store)
    result = await collector.run_once(end_date="2023-01-04")
    assert result["status"] == "verified"
    assert source.calls == [
        ("2023-01-01", "2023-01-04"),
        ("2023-01-01", "2023-01-02"),
        ("2023-01-03", "2023-01-04"),
    ]
    assert len(store.writes) == 2
    state = json.loads((collector.series_dir(index) / "state.json").read_text())
    assert state["covered_until_exclusive"] == "2023-01-05"
    assert len(state["verified_windows"]) == 2
    assert len(list((collector.series_dir(index) / "truncated").glob("*.json"))) == 1


@pytest.mark.asyncio
async def test_wrong_request_window_never_writes_or_checkpoints(tmp_path, index):
    class WrongWindow(Source):
        async def fetch(self, item, **window):
            fetched = await super().fetch(item, **window)
            fetched["request_url"] = fetched["request_url"].replace("range/5/minute", "range/1/day")
            return fetched

    source, store = WrongWindow(), Store()
    collector = producer(tmp_path, index, source, store)
    result = await collector.run_once(end_date="2023-01-02")
    assert result["failed"][0]["classification"] == "pending_request_mismatch"
    assert not store.writes
    state = json.loads((collector.series_dir(index) / "state.json").read_text())
    assert state["covered_until_exclusive"] == "2023-01-01"


@pytest.mark.asyncio
async def test_http_permission_failure_not_empty_or_coverage(tmp_path, index):
    source, store = Source(failure="permission_denied"), Store()
    collector = producer(tmp_path, index, source, store)
    result = await collector.run_once(end_date="2023-03-31")
    assert result["failed"][0]["classification"] == "permission_denied"
    state = json.loads((collector.series_dir(index) / "state.json").read_text())
    assert state["covered_until_exclusive"] == "2023-01-01" and not store.writes


def test_full_dates_quarters_without_historical_business_cap():
    from datetime import date

    windows = quarter_windows(date(2023, 1, 1), date(2026, 10, 8))
    assert windows[0] == {"start_date": "2023-01-01", "end_date": "2023-03-31"}
    assert windows[-1] == {"start_date": "2026-10-01", "end_date": "2026-10-07"}
    assert len(windows) == 16
    for before, after in zip(windows, windows[1:]):
        assert (
            date.fromisoformat(after["start_date"]) - date.fromisoformat(before["end_date"])
        ).days == 1


@pytest.mark.asyncio
async def test_same_identity_reference_metadata_change_is_explicit_not_a_new_fetch(tmp_path, index):
    collector = producer(tmp_path, index, Source(), Store())
    old_sha = collector.reference["reference_sha256"]
    assert (await collector.run_once(end_date="2023-01-02"))["status"] == "verified"
    path = tmp_path / "massive.json"
    ref = json.loads(path.read_text())
    ref["evidence_note"] = "Additional evidence; original identity and price basis unchanged"
    path.write_text(json.dumps(ref), encoding="utf8")
    no_fetch = Source(failure="network_must_not_be_called")
    reused = CrossMarketMassiveIngestor(
        reference_path=path,
        industry_reference_path=tmp_path / "industry.json",
        state_dir=tmp_path / "state",
        massive=no_fetch,
        store=Store(),
    )
    result = (await reused.run_once(end_date="2023-01-02"))["results"][0]
    assert not no_fetch.calls and result["verified_rows"] == 0
    assert result["state_reference_sha256"] == old_sha
    assert result["current_reference_sha256"] != old_sha
    assert result["reference_changed_same_identity"] is True
    assert result["coverage_kind"] == "verified_requested_windows_not_calendar_bar_completeness"
