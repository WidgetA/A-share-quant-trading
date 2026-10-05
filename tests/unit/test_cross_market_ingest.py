"""Durable collection behavior, including uncertain writes and process restarts."""

import asyncio
import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from scripts import collect_cross_market_indices as cli
from src.data import cross_market_ingest as ingest_module
from src.data.cross_market_ingest import DAY_MS, CrossMarketIngestor, load_reference
from src.data.cross_market_store import GreptimeReadbackError, GreptimeWriteError


def source_point(symbol="^SOX", market="US", **changes):
    row = {
        "provider": "yahoo",
        "market": market,
        "symbol": symbol,
        "interval": "1d" if market == "US" else "quote",
        "data_kind": "daily_bar" if market == "US" else "quote_snapshot",
        "ts": 1728000000123,
        "name": symbol,
        "currency": "USD" if market == "US" else "KRW",
        "exchange_timezone": "America/New_York" if market == "US" else "Asia/Seoul",
        "trade_date": "2024-10-04",
        "open": None,
        "high": None,
        "low": None,
        "close": 100.25,
        "adjusted_close": None,
        "volume": None,
        "is_final": None,
        "fetched_at": 1728100000456,
    }
    row.update(changes)
    return row


def source_window(points):
    raw = json.dumps({"fixture_source_observations": points}, ensure_ascii=False)
    return {
        "raw_json": raw,
        "points": copy.deepcopy(points),
        "metadata": {
            "symbol": points[0]["symbol"],
            "instrumentType": "INDEX",
            "shortName": points[0]["name"],
            "currency": points[0]["currency"],
            "exchangeTimezoneName": points[0]["exchange_timezone"],
        },
        "payload_sha256": hashlib.sha256(raw.encode()).hexdigest(),
        "missing_close_timestamps": [point["ts"] for point in points if point["close"] is None],
        "fetched_at": points[0]["fetched_at"],
    }


@pytest.fixture
def references(tmp_path):
    def write_json(path, value):
        path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")

    base = {
        "taxonomy": {"source_id": "base_source"},
        "sources": [{"id": "base_source", "url": "https://example.invalid/industry-evidence"}],
        "boards": [
            {"id": "US:board", "name_original": "Test board", "source_id": "base_source"},
            {"id": "KR:board", "name_original": "원본 업종", "source_id": "base_source"},
        ],
        "industries": [],
    }
    document = {
        "schema_version": 1,
        "checked_at": "2026-10-06T00:00:00Z",
        "indices": [
            {
                "index_id": "us-semiconductors",
                "market": "US",
                "symbol": "^SOX",
                "name": "SOX",
                "capability": "daily_history",
            },
            {
                "index_id": "kr-semiconductors",
                "market": "KR",
                "symbol": "KOSPI-25.KS",
                "name": "KR semiconductor",
                "capability": "snapshot_only",
            },
            {
                "index_id": "unused-broad-market",
                "market": "US",
                "symbol": "^GSPC",
                "name": "Unreferenced index",
                "capability": "daily_history",
            },
        ],
        "industries": [],
    }
    for number in range(134):
        code, name = str(100000 + number), f"行业{number}"
        base_industry = {"sw_code": code, "sw_name": name}
        mapped = {"sw_code": code, "sw_name": name, "markets": {}}
        for market in ("US", "KR"):
            base_industry[market] = {
                "status": "mapped",
                "matches": [
                    {
                        "board_id": f"{market}:board",
                        "relation": "partial_overlap",
                        "scope_note": "原板块范围差异",
                        "source_ids": ["base_source"],
                    }
                ],
            }
            index_id = "us-semiconductors" if market == "US" else "kr-semiconductors"
            mapped["markets"][market] = {
                "status": "mapped" if number < 3 else "no_index_verified",
                "scope_note": "原映射差异保留",
                "matches": [
                    {
                        "index_id": index_id,
                        "board_ids": [f"{market}:board"],
                        "relation": "partial_overlap",
                        "scope_note": "实际指数口径差异",
                        "sources": ["https://example.invalid/index"],
                    }
                ]
                if number < 3
                else [],
            }
        base["industries"].append(base_industry)
        document["industries"].append(mapped)
    base_path, reference_path = tmp_path / "boards.json", tmp_path / "indices.json"
    for index in document["indices"]:
        index["yahoo_names"] = [index["symbol"]]
        index["currency"] = "USD" if index["market"] == "US" else "KRW"
        index["exchange_timezone"] = "America/New_York" if index["market"] == "US" else "Asia/Seoul"
    write_json(base_path, base)
    document["base_reference_sha256"] = hashlib.sha256(base_path.read_bytes()).hexdigest()
    write_json(reference_path, document)
    return SimpleNamespace(
        base=base_path, reference=reference_path, document=document, write=write_json
    )


class FakeYahoo:
    def __init__(self, windows=None, events=None, failures=None):
        self.windows = windows or {}
        self.events = events if events is not None else []
        self.failures = failures or {}
        self.calls = []
        self.active = 0
        self.max_active = 0

    async def fetch(self, symbol, market, *, start=None, capability=None):
        self.calls.append((symbol, market, start, capability))
        self.events.append(("fetch", symbol, start))
        self.active += 1
        self.max_active = max(self.active, self.max_active)
        try:
            await asyncio.sleep(0)
            if symbol in self.failures:
                raise self.failures[symbol]
            queued = self.windows.get(symbol)
            if queued:
                return queued.pop(0)
            return source_window([source_point(symbol, market)])
        finally:
            self.active -= 1


class FakeStore:
    def __init__(self, state_dir, events=None):
        self.state_dir = state_dir
        self.events = events if events is not None else []
        self.prices = {}
        self.mappings = {}
        self.writes = []
        self.mapping_writes = []
        self.mapping_verifications = []
        self.fail_symbol_once = None
        self.verify_fail_symbol = None
        self.mapping_failure = False

    @staticmethod
    def key(row):
        return tuple(
            row[name] for name in ("provider", "market", "symbol", "interval", "data_kind", "ts")
        )

    async def ensure_schema(self):
        assert (self.state_dir / "mappings.pending.json").exists() or (
            self.state_dir / "mappings.state.json"
        ).exists()

    async def upsert_industry_indices(self, rows):
        assert (self.state_dir / "mappings.pending.json").exists()
        self.mapping_writes.append(copy.deepcopy(rows))
        if self.mapping_failure:
            raise GreptimeWriteError("mapping write failed", confirmed_rows=0)
        for row in rows:
            key = tuple(row[name] for name in ("provider", "market", "sw_code", "reference_at"))
            self.mappings[key] = copy.deepcopy(row)
        return len(rows)

    async def verify_industry_indices(self, rows):
        self.mapping_verifications.append(copy.deepcopy(rows))
        for row in rows:
            key = tuple(row[name] for name in ("provider", "market", "sw_code", "reference_at"))
            assert self.mappings[key] == row
        return len(rows)

    async def upsert_prices(self, points):
        assert list(self.state_dir.glob("*.pending.json"))
        self.writes.append(copy.deepcopy(points))
        self.events.append(("write", points[0]["symbol"], [row["ts"] for row in points]))
        if self.fail_symbol_once == points[0]["symbol"]:
            self.fail_symbol_once = None
            # A later row survives, demonstrating why MAX(ts) cannot skip earlier holes.
            self.prices[self.key(points[-1])] = copy.deepcopy(points[-1])
            raise GreptimeWriteError("uncertain partial batch", confirmed_rows=1)
        for point in points:
            self.prices[self.key(point)] = copy.deepcopy(point)
        return len(points)

    async def verify_prices(self, points):
        self.events.append(("verify", points[0]["symbol"], [row["ts"] for row in points]))
        if self.verify_fail_symbol == points[0]["symbol"]:
            raise GreptimeReadbackError("different persisted source value")
        for row in points:
            assert self.prices[self.key(row)] == row
        return len(points)


def producer(refs, state_dir, yahoo, store):
    return CrossMarketIngestor(
        reference_path=refs.reference,
        base_reference_path=refs.base,
        state_dir=state_dir,
        yahoo=yahoo,
        store=store,
    )


def selected_index(refs, index_id="us-semiconductors"):
    return next(row for row in refs.document["indices"] if row["index_id"] == index_id)


def test_atomic_checkpoint_syncs_file_then_replace_then_parent_directory(monkeypatch, tmp_path):
    events = []
    directory_flag = 0x10000
    directory_fd = 987654
    original_open = ingest_module.os.open
    original_replace = ingest_module.os.replace
    original_fsync = ingest_module.os.fsync
    original_close = ingest_module.os.close
    monkeypatch.setattr(ingest_module.os, "O_DIRECTORY", directory_flag, raising=False)

    def open_path(path, flags, *args, **kwargs):
        if flags & directory_flag:
            assert Path(path) == tmp_path
            events.append("open_directory")
            return directory_fd
        return original_open(path, flags, *args, **kwargs)

    def sync_fd(fd):
        if fd == directory_fd:
            events.append("fsync_directory")
        else:
            events.append("fsync_file")
            original_fsync(fd)

    def replace_path(source, destination):
        events.append("replace")
        return original_replace(source, destination)

    def close_fd(fd):
        if fd == directory_fd:
            events.append("close_directory")
        else:
            original_close(fd)

    monkeypatch.setattr(ingest_module.os, "open", open_path)
    monkeypatch.setattr(ingest_module.os, "fsync", sync_fd)
    monkeypatch.setattr(ingest_module.os, "replace", replace_path)
    monkeypatch.setattr(ingest_module.os, "close", close_fd)
    destination = tmp_path / "pending.json"
    ingest_module._atomic_json(destination, {"original_window": [1, 2, 3]})
    assert events == [
        "fsync_file",
        "replace",
        "open_directory",
        "fsync_directory",
        "close_directory",
    ]
    assert json.loads(destination.read_text()) == {"original_window": [1, 2, 3]}


@pytest.mark.asyncio
async def test_complete_268_mappings_and_only_referenced_deduplicated_indices(references, tmp_path):
    state_dir = tmp_path / "state"
    yahoo, store = FakeYahoo(), FakeStore(state_dir)
    result = await producer(references, state_dir, yahoo, store).run_once()
    assert result["status"] == "verified"
    assert result["mapping"]["rows"] == 268
    assert len(store.mappings) == 268
    assert result["referenced_indices"] == 2
    assert {call[0] for call in yahoo.calls} == {"^SOX", "KOSPI-25.KS"}
    assert len(yahoo.calls) == 2 and yahoo.max_active == 2
    assert all(call[2] is None for call in yahoo.calls)
    assert {call[3] for call in yahoo.calls} == {"daily_history", "snapshot_only"}
    saved = store.mapping_writes[0][0]
    assert saved["reference_at"] == 1791244800000
    assert saved["mapping_json"]["mapping"]["scope_note"] == "原映射差异保留"
    assert saved["mapping_json"]["base_market"]["matches"][0]["scope_note"] == "原板块范围差异"
    assert saved["mapping_json"]["base_sources"][0]["url"].endswith("industry-evidence")
    assert len(saved["mapping_json"]["indices"]) == 1
    assert not (state_dir / "mappings.pending.json").exists()


@pytest.mark.asyncio
async def test_verified_static_mapping_is_cached_but_quotes_continue_and_missing_state_reverifies(
    references, tmp_path
):
    state_dir = tmp_path / "state"
    yahoo, store = FakeYahoo(), FakeStore(state_dir)
    first = await producer(references, state_dir, yahoo, store).run_once()
    second = await producer(references, state_dir, yahoo, store).run_once()
    assert first["mapping"] == {"status": "verified", "rows": 268}
    assert second["mapping"] == {"status": "previously_verified", "rows": 268}
    assert len(store.mapping_writes) == len(store.mapping_verifications) == 1
    assert len(yahoo.calls) == len(store.writes) == 4
    assert not (state_dir / "mappings.pending.json").exists()

    (state_dir / "mappings.state.json").unlink()
    third = await producer(references, state_dir, yahoo, store).run_once()
    assert third["mapping"] == {"status": "verified", "rows": 268}
    assert len(store.mapping_writes) == len(store.mapping_verifications) == 2
    assert len(yahoo.calls) == len(store.writes) == 6

    changed = copy.deepcopy(references.document)
    changed["checked_at"] = "2026-10-06T00:00:01Z"
    changed["industries"][0]["markets"]["US"]["scope_note"] = "Revised sourced scope"
    references.write(references.reference, changed)
    fourth = await producer(references, state_dir, yahoo, store).run_once()
    assert fourth["mapping"] == {"status": "verified", "rows": 268}
    assert len(store.mapping_writes) == len(store.mapping_verifications) == 3
    assert store.mapping_writes[-1][0]["reference_at"] == 1791244801000
    assert len(yahoo.calls) == len(store.writes) == 8


@pytest.mark.asyncio
async def test_partial_write_restart_replays_original_whole_window_before_new_fetch(
    references, tmp_path
):
    state_dir, events = tmp_path / "state", []
    original = [source_point(ts=1728000000123 + index * DAY_MS) for index in range(3)]
    store = FakeStore(state_dir, events)
    store.fail_symbol_once = "^SOX"
    first_yahoo = FakeYahoo({"^SOX": [source_window(original)]}, events)
    first = producer(references, state_dir, first_yahoo, store)
    state_path, pending_path = first._paths(selected_index(references))
    result = await first.run_once()
    assert result["status"] == "partial_failure"
    assert not state_path.exists() and pending_path.exists()
    pending = json.loads(pending_path.read_text(encoding="utf-8"))
    assert pending["source"]["points"] == original
    assert json.loads(pending["source"]["raw_json"])["fixture_source_observations"] == original
    assert max(key[-1] for key in store.prices if key[2] == "^SOX") == original[-1]["ts"]

    events.clear()
    latest = source_point(ts=original[-1]["ts"], close=101.5, fetched_at=1728300000000)
    next_yahoo = FakeYahoo({"^SOX": [source_window([latest])]}, events)
    restarted = producer(references, state_dir, next_yahoo, store)
    result = await restarted.run_once()
    assert result["status"] == "verified"
    sox_events = [event for event in events if event[1] == "^SOX"]
    assert sox_events[0] == ("write", "^SOX", [row["ts"] for row in original])
    assert sox_events[1][0] == "verify" and sox_events[2][0] == "fetch"
    sox_fetch = next(call for call in next_yahoo.calls if call[0] == "^SOX")
    assert sox_fetch[2] == (original[-1]["ts"] - DAY_MS) // 1000
    assert store.prices[store.key(latest)]["close"] == 101.5
    saved = json.loads(state_path.read_text())
    assert saved["last_verified_ms"] == original[-1]["ts"]
    assert saved["source_sha256"] == source_window([latest])["payload_sha256"]
    assert not pending_path.exists()


@pytest.mark.asyncio
async def test_verification_failure_retains_pending_and_does_not_pollute_other_success(
    references, tmp_path
):
    state_dir = tmp_path / "state"
    yahoo, store = FakeYahoo(), FakeStore(state_dir)
    store.verify_fail_symbol = "^SOX"
    job = producer(references, state_dir, yahoo, store)
    result = await job.run_once()
    sox_state, sox_pending = job._paths(selected_index(references))
    kr_state, kr_pending = job._paths(selected_index(references, "kr-semiconductors"))
    assert result["status"] == "partial_failure"
    assert not sox_state.exists() and sox_pending.exists()
    assert kr_state.exists() and not kr_pending.exists()
    assert json.loads(kr_state.read_text())["last_verified_ms"] == 1728000000123


@pytest.mark.asyncio
async def test_gap_is_retained_and_earliest_gap_is_requested_until_actual_price_arrives(
    references, tmp_path
):
    state_dir = tmp_path / "state"
    gap_time = 1728000000123
    latest_time = gap_time + 5 * DAY_MS
    missing = source_point(ts=gap_time, close=None)
    latest = source_point(ts=latest_time)
    yahoo = FakeYahoo({"^SOX": [source_window([missing, latest])]})
    store = FakeStore(state_dir)
    job = producer(references, state_dir, yahoo, store)
    state_path, _ = job._paths(selected_index(references))
    result = await job.run_once()
    sox_result = next(row for row in result["indices"] if row["symbol"] == "^SOX")
    assert sox_result["status"] == "verified_with_gaps"
    assert store.prices[store.key(missing)]["close"] is None
    assert json.loads(state_path.read_text())["gaps"] == [gap_time]
    recovered = source_point(ts=gap_time, close=99.5)
    new_yahoo = FakeYahoo({"^SOX": [source_window([recovered, latest])]})
    result = await producer(references, state_dir, new_yahoo, store).run_once()
    assert next(call for call in new_yahoo.calls if call[0] == "^SOX")[2] == gap_time // 1000
    assert json.loads(state_path.read_text())["gaps"] == []
    assert store.prices[store.key(missing)]["close"] == 99.5


@pytest.mark.asyncio
async def test_failed_second_window_cannot_advance_previous_success_state(references, tmp_path):
    state_dir = tmp_path / "state"
    store = FakeStore(state_dir)
    job = producer(references, state_dir, FakeYahoo(), store)
    await job.run_once()
    state_path, pending_path = job._paths(selected_index(references))
    before = state_path.read_bytes()
    newer = source_point(ts=1728000000123 + DAY_MS, close=102.0)
    store.fail_symbol_once = "^SOX"
    result = await producer(
        references, state_dir, FakeYahoo({"^SOX": [source_window([newer])]}), store
    ).run_once()
    assert result["status"] == "partial_failure"
    assert state_path.read_bytes() == before and pending_path.exists()


@pytest.mark.asyncio
async def test_mapping_failure_is_replayed_and_independent_symbols_still_run(references, tmp_path):
    state_dir = tmp_path / "state"
    store = FakeStore(state_dir)
    store.mapping_failure = True
    yahoo = FakeYahoo()
    first = await producer(references, state_dir, yahoo, store).run_once()
    assert first["status"] == "partial_failure" and len(yahoo.calls) == 2
    assert first["mapping"]["confirmed_rows"] == 0
    assert first["mapping"]["cause_type"] is None
    pending = state_dir / "mappings.pending.json"
    cached = json.loads(pending.read_text(encoding="utf-8"))
    assert len(cached["rows"]) == 268
    store.mapping_failure = False
    second = await producer(references, state_dir, FakeYahoo(), store).run_once()
    assert second["status"] == "verified"
    assert store.mapping_writes[-1] == cached["rows"]
    assert not pending.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("replay", [False, True])
async def test_uncertain_write_reports_confirmed_rows_and_cause_without_exception_text(
    references, tmp_path, replay
):
    state_dir = tmp_path / "state"
    original = [source_point(ts=1728000000123 + number * DAY_MS) for number in range(3)]

    class TimeoutStore(FakeStore):
        async def upsert_prices(self, points):
            if points[0]["symbol"] == "^SOX":
                self.prices[self.key(points[-1])] = copy.deepcopy(points[-1])
                try:
                    raise httpx.ReadTimeout(
                        "http://private-user:secret-password@proxy",
                        request=httpx.Request("POST", "http://private-user:secret-password@db"),
                    )
                except httpx.ReadTimeout as cause:
                    raise GreptimeWriteError(
                        "private-user:secret-password uncertain write", confirmed_rows=2
                    ) from cause
            return await super().upsert_prices(points)

    yahoo = FakeYahoo({"^SOX": [source_window(original)]})
    job = producer(references, state_dir, yahoo, TimeoutStore(state_dir))
    state_path, pending_path = job._paths(selected_index(references))
    result = await job.run_once()
    original_pending = pending_path.read_bytes()
    if replay:
        result = await job.run_once()
    failed = next(item for item in result["indices"] if item["symbol"] == "^SOX")
    assert failed["stage"] == ("pending_replay" if replay else "write_and_verify")
    assert failed["error_type"] == "GreptimeWriteError"
    assert failed["confirmed_rows"] == 2
    assert failed["cause_type"] == "ReadTimeout"
    assert failed["pending_retained"] is True
    assert not state_path.exists() and pending_path.read_bytes() == original_pending
    assert len([call for call in yahoo.calls if call[0] == "^SOX"]) == 1
    assert next(item for item in result["indices"] if item["market"] == "KR")[
        "status"
    ] == "verified"
    encoded = json.dumps(result)
    assert "private-user" not in encoded and "secret-password" not in encoded
    assert "http://" not in encoded and "uncertain write" not in encoded


@pytest.mark.asyncio
async def test_fetch_failure_does_not_leak_credentials_or_block_other_index(references, tmp_path):
    state_dir = tmp_path / "state"
    yahoo = FakeYahoo(failures={"^SOX": RuntimeError("http://private-user:secret-password@proxy")})
    result = await producer(references, state_dir, yahoo, FakeStore(state_dir)).run_once()
    assert result["status"] == "partial_failure"
    encoded = json.dumps(result)
    assert "secret-password" not in encoded and "private-user" not in encoded
    assert next(item for item in result["indices"] if item["symbol"] == "^SOX")["stage"] == "fetch"
    assert (
        next(item for item in result["indices"] if item["market"] == "KR")["status"] == "verified"
    )


@pytest.mark.parametrize("change", ["hash", "missing_industry", "unknown_index", "wrong_market"])
def test_invalid_reference_fails_before_fetch_or_write(references, change):
    document = copy.deepcopy(references.document)
    if change == "hash":
        document["base_reference_sha256"] = "0" * 64
    elif change == "missing_industry":
        document["industries"].pop()
    elif change == "unknown_index":
        document["industries"][0]["markets"]["US"]["matches"][0]["index_id"] = "absent"
    else:
        document["indices"][0]["market"] = "KR"
    references.write(references.reference, document)
    with pytest.raises(ValueError):
        load_reference(references.reference, references.base)


@pytest.mark.asyncio
async def test_changed_source_sha_never_becomes_a_pending_success(references, tmp_path):
    state_dir = tmp_path / "state"
    source = source_window([source_point()])
    source["payload_sha256"] = "0" * 64
    store = FakeStore(state_dir)
    job = producer(references, state_dir, FakeYahoo({"^SOX": [source]}), store)
    result = await job.run_once()
    state, pending = job._paths(selected_index(references))
    assert result["status"] == "partial_failure"
    assert not state.exists() and not pending.exists()
    assert not any(rows[0]["symbol"] == "^SOX" for rows in store.writes)


@pytest.mark.asyncio
async def test_cli_initialization_error_reports_type_without_secret(monkeypatch, capsys, tmp_path):
    def rejected_proxy(*, proxy):
        raise ValueError(f"Bad proxy: {proxy}")

    monkeypatch.setattr(cli, "YahooIndexClient", rejected_proxy)
    args = SimpleNamespace(
        proxy="http://user:secret-password@proxy",
        greptime_url="http://db",
        batch_size=100,
        reference=Path("unused"),
        base_reference=Path("unused"),
        state_dir=tmp_path,
        concurrency=1,
        loop_seconds=0,
    )
    assert await cli.run(args) == 1
    text = capsys.readouterr().out
    assert json.loads(text)["error_type"] == "ValueError"
    assert "secret-password" not in text and "http://user" not in text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("shortName", "HORIZON FDS"),
        ("instrumentType", "ETF"),
        ("currency", "KRW"),
        ("exchangeTimezoneName", "Europe/London"),
    ],
)
async def test_metadata_identity_conflict_does_not_write_or_advance(
    references, tmp_path, field, value
):
    state_dir = tmp_path / "state"
    source = source_window([source_point()])
    source["metadata"][field] = value
    store = FakeStore(state_dir)
    job = producer(references, state_dir, FakeYahoo({"^SOX": [source]}), store)
    result = await job.run_once()
    state, pending = job._paths(selected_index(references))
    assert result["status"] == "partial_failure"
    assert not state.exists() and not pending.exists()
    assert not any(rows[0]["symbol"] == "^SOX" for rows in store.writes)
    assert next(row for row in result["indices"] if row["market"] == "KR")["status"] == "verified"


@pytest.mark.asyncio
async def test_new_history_capability_replays_pending_then_fetches_full_without_deleting_state(
    references, tmp_path
):
    state_dir, events = tmp_path / "state", []
    document = copy.deepcopy(references.document)
    document["indices"][0]["capability"] = "snapshot_only"
    references.write(references.reference, document)
    # Establish an older snapshot checkpoint before a later failed snapshot window.
    old = source_point(interval="quote", data_kind="quote_snapshot")
    store = FakeStore(state_dir, events)
    first = producer(
        references, state_dir, FakeYahoo({"^SOX": [source_window([old])]}, events), store
    )
    assert (await first.run_once())["status"] == "verified"
    state_path, pending_path = first._paths(selected_index(references))
    next_quote = source_point(ts=old["ts"] + DAY_MS, interval="quote", data_kind="quote_snapshot")
    store.fail_symbol_once = "^SOX"
    second = producer(
        references, state_dir, FakeYahoo({"^SOX": [source_window([next_quote])]}, events), store
    )
    assert (await second.run_once())["status"] == "partial_failure"
    assert state_path.exists() and pending_path.exists()

    # A verified reference revision now advertises history for the same entity.
    references.write(references.reference, references.document)
    full_history = [source_point(ts=old["ts"] - 10 * DAY_MS), source_point(ts=old["ts"] + DAY_MS)]
    events.clear()
    yahoo = FakeYahoo({"^SOX": [source_window(full_history)]}, events)
    restarted = producer(references, state_dir, yahoo, store)
    assert (await restarted.run_once())["status"] == "verified"
    sox_events = [event for event in events if event[1] == "^SOX"]
    assert sox_events[0] == ("write", "^SOX", [next_quote["ts"]])
    assert sox_events[1][0] == "verify" and sox_events[2] == ("fetch", "^SOX", None)
    assert next(call for call in yahoo.calls if call[0] == "^SOX")[3] == "daily_history"
    assert json.loads(state_path.read_text())["capability"] == "daily_history"
    assert "capability" not in json.loads(state_path.read_text())["identity"]
    assert not pending_path.exists()


@pytest.mark.asyncio
async def test_atomic_checkpoint_failure_keeps_original_pending_for_restart(
    references, tmp_path, monkeypatch
):
    from src.data import cross_market_ingest as implementation

    state_dir = tmp_path / "state"
    store = FakeStore(state_dir)
    job = producer(references, state_dir, FakeYahoo(), store)
    state_path, pending_path = job._paths(selected_index(references))
    original = implementation._atomic_json
    failed = False

    def interrupted_replace(path, value):
        nonlocal failed
        if path == state_path and not failed:
            failed = True
            raise OSError("checkpoint replace interrupted")
        original(path, value)

    monkeypatch.setattr(implementation, "_atomic_json", interrupted_replace)
    assert (await job.run_once())["status"] == "partial_failure"
    assert not state_path.exists() and pending_path.exists()
    assert (await producer(references, state_dir, FakeYahoo(), store).run_once())[
        "status"
    ] == "verified"
    assert state_path.exists() and not pending_path.exists()
