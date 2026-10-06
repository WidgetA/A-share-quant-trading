"""Minute coverage/replay behavior, independent from live Yahoo or production."""

import asyncio
import copy
import hashlib
import json
from types import SimpleNamespace

import pytest

from scripts import collect_cross_market_intraday as cli
from src.data.cross_market_ingest import CrossMarketIngestor, _atomic_json
from src.data.cross_market_intraday_ingest import (
    DAY_SECONDS,
    CrossMarketIntradayIngestor,
    load_intraday_reference,
)
from src.data.cross_market_store import GreptimeReadbackError, GreptimeWriteError

NOW = 1791311400


@pytest.fixture
def refs(tmp_path):
    base = {"industries": [{"sw_code": "110100", "sw_name": "种植业", "US": {}, "KR": {}}]}
    base_path = tmp_path / "boards.json"
    base_path.write_text(json.dumps(base), encoding="utf-8")
    indices = []
    for market, symbol in (("US", "^SOX"), ("KR", "KOSPI-10.KS")):
        indices.append(
            {
                "index_id": market + ":industry",
                "market": market,
                "symbol": symbol,
                "name": symbol,
                "yahoo_names": [symbol],
                "capability": "snapshot_only",
                "currency": "USD" if market == "US" else "KRW",
                "exchange_timezone": "America/New_York" if market == "US" else "Asia/Seoul",
            }
        )
    document = {
        "schema_version": 1,
        "checked_at": "2026-10-06T00:00:00Z",
        "base_reference_sha256": hashlib.sha256(base_path.read_bytes()).hexdigest(),
        "indices": indices,
        "industries": [
            {
                "sw_code": "110100",
                "sw_name": "种植业",
                "markets": {
                    market: {"matches": [{"index_id": market + ":industry"}]}
                    for market in ("US", "KR")
                },
            }
        ],
    }
    reference = tmp_path / "indices.json"
    reference.write_text(json.dumps(document), encoding="utf-8")
    capability = {
        "schema_version": 1,
        "base_reference_sha256": hashlib.sha256(reference.read_bytes()).hexdigest(),
        "indices": [
            {**{k: index[k] for k in ("index_id", "market", "symbol")}, "intervals": ["1m", "5m"]}
            for index in indices
        ],
        "limits": {
            "1m": {"retention_seconds": 30 * DAY_SECONDS, "max_window_seconds": 8 * DAY_SECONDS},
            "5m": {"retention_seconds": 60 * DAY_SECONDS, "max_window_seconds": 60 * DAY_SECONDS},
        },
    }
    capabilities = tmp_path / "minutes.json"
    capabilities.write_text(json.dumps(capability), encoding="utf-8")
    return SimpleNamespace(
        base=base_path,
        reference=reference,
        capabilities=capabilities,
        indices=indices,
        capability=capability,
    )


def point(index, interval, ts, *, kind="minute_bar", close=10.0):
    return {
        "provider": "yahoo",
        "market": index["market"],
        "symbol": index["symbol"],
        "interval": interval,
        "data_kind": kind,
        "ts": ts * 1000,
        "name": index["symbol"],
        "currency": index["currency"],
        "exchange_timezone": index["exchange_timezone"],
        "trade_date": "2026-10-06",
        "open": 9.5,
        "high": 11.0,
        "low": 9.0,
        "close": close,
        "adjusted_close": None,
        "volume": 0.0,
        "is_final": None,
        "fetched_at": NOW * 1000,
    }


def source(index, interval, points, *, empty=False, start=None, end=None):
    metadata = {
        "symbol": index["symbol"],
        "instrumentType": "INDEX",
        "shortName": index["symbol"],
        "currency": index["currency"],
        "exchangeTimezoneName": index["exchange_timezone"],
        "dataGranularity": interval,
    }
    result = {"meta": metadata, "indicators": {"quote": [{}]}}
    if points:
        result["timestamp"] = [p["ts"] // 1000 for p in points]
        result["indicators"]["quote"] = [
            {key: [p[key] for p in points] for key in ("open", "high", "low", "close", "volume")}
        ]
    raw = json.dumps({"chart": {"result": [result], "error": None}})
    return {
        "metadata": metadata,
        "points": copy.deepcopy(points),
        "raw_json": raw,
        "payload_sha256": hashlib.sha256(raw.encode()).hexdigest(),
        "fetched_at": NOW * 1000,
        "url": "https://query1.finance.yahoo.com/v8/finance/chart/index?interval=" + interval,
        "source_empty": empty,
        "requested_range": {"start": start, "end": end, "interval": interval},
        "missing_close_timestamps": [p["ts"] for p in points if p["close"] is None],
    }


class Yahoo:
    def __init__(self, indices, events=None):
        self.indices = {i["symbol"]: i for i in indices}
        self.calls, self.active, self.max_active = [], 0, 0
        self.events = events if events is not None else []
        self.quote_only = False

    async def fetch(self, symbol, market, *, start, end, interval):
        self.calls.append((symbol, market, start, end, interval))
        self.events.append(("fetch", symbol, interval, start, end))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0)
            index = self.indices[symbol]
            if self.quote_only:
                rows = [point(index, interval, NOW + 1, kind="minute_quote_snapshot")]
            else:
                step = 60 if interval == "1m" else 300
                first = ((start + step - 1) // step) * step
                last = (end // step - 1) * step
                rows = [
                    point(index, interval, first),
                    point(index, interval, last),
                    point(index, interval, NOW + 1, kind="minute_quote_snapshot"),
                ]
            return source(index, interval, rows, start=start, end=end)
        finally:
            self.active -= 1


class Store:
    def __init__(self, state_dir, events=None):
        self.state_dir = state_dir
        self.events = events if events is not None else []
        self.rows, self.writes = {}, []
        self.fail_ts = None
        self.mismatch_ts = None

    async def ensure_schema(self):
        return None

    @staticmethod
    def key(row):
        return tuple(
            row[k] for k in ("provider", "market", "symbol", "interval", "data_kind", "ts")
        )

    async def upsert_prices(self, rows):
        assert any(
            json.loads(path.read_text())["source"]["points"] == rows
            for path in self.state_dir.glob("*.pending.json")
        )
        self.events.append(("write", rows[0]["symbol"] if rows else "empty"))
        self.writes.append(copy.deepcopy(rows))
        if any(row["ts"] == self.fail_ts for row in rows):
            self.rows[self.key(rows[-1])] = copy.deepcopy(rows[-1])
            self.fail_ts = None
            raise GreptimeWriteError("uncertain partial write", confirmed_rows=1)
        for row in rows:
            self.rows[self.key(row)] = copy.deepcopy(row)
        return len(rows)

    async def verify_prices(self, rows):
        self.events.append(("verify", rows[0]["symbol"] if rows else "empty"))
        if any(row["ts"] == self.mismatch_ts for row in rows):
            raise GreptimeReadbackError("different stored field")
        assert all(self.rows[self.key(row)] == row for row in rows)
        return len(rows)


def producer(refs, state_dir, yahoo, store, *, now=NOW):
    return CrossMarketIntradayIngestor(
        reference_path=refs.reference,
        base_reference_path=refs.base,
        capability_path=refs.capabilities,
        state_dir=state_dir,
        yahoo=yahoo,
        store=store,
        clock=lambda: now,
        concurrency=2,
    )


def entity(refs, interval="1m"):
    return {**refs.indices[0], "interval": interval}


def test_reference_requires_actual_index_hash_and_identity(refs):
    loaded = load_intraday_reference(refs.reference, refs.base, refs.capabilities)
    assert len(loaded["indices"]) == 4
    bad = copy.deepcopy(refs.capability)
    bad["base_reference_sha256"] = "0" * 64
    refs.capabilities.write_text(json.dumps(bad))
    with pytest.raises(ValueError, match="bound"):
        load_intraday_reference(refs.reference, refs.base, refs.capabilities)


@pytest.mark.asyncio
async def test_full_retention_partition_parallel_intervals_and_quote_cursor(refs, tmp_path):
    state = tmp_path / "state-intraday"
    yahoo, store = Yahoo(refs.indices), Store(state)
    job = producer(refs, state, yahoo, store)
    result = await job.run_once()
    assert result["status"] == "verified" and result["series_count"] == 4
    assert yahoo.max_active == 2
    for interval, retention in (("1m", 30 * DAY_SECONDS), ("5m", 60 * DAY_SECONDS)):
        calls = [c for c in yahoo.calls if c[0] == "^SOX" and c[4] == interval]
        assert calls[0][2] == NOW - retention + (60 if interval == "1m" else 300)
        assert calls[-1][3] == NOW
        assert all(left[3] == right[2] for left, right in zip(calls, calls[1:]))
        assert all(
            c[3] - c[2] <= refs.capability["limits"][interval]["max_window_seconds"] for c in calls
        )
        path, pending = job._paths(entity(refs, interval))
        saved = json.loads(path.read_text())
        assert saved["covered_until_s"] == saved["backfill_complete_until_s"] == NOW
        assert saved["last_source_bar_ms"] == (NOW - (60 if interval == "1m" else 300)) * 1000
        assert not pending.exists()
    assert job._paths(entity(refs, "1m")) != job._paths(entity(refs, "5m"))
    daily = CrossMarketIngestor(
        reference_path=refs.reference,
        base_reference_path=refs.base,
        state_dir=state,
        yahoo=yahoo,
        store=store,
    )
    assert daily._paths(refs.indices[0]) != job._paths(entity(refs))
    assert any(row["data_kind"] == "minute_quote_snapshot" for row in store.rows.values())


@pytest.mark.asyncio
async def test_partial_ack_pending_replays_before_network_and_preserves_history_cursor(
    refs, tmp_path
):
    state = tmp_path / "state-intraday"
    events = []
    yahoo, store = Yahoo(refs.indices, events), Store(state, events)
    start = NOW - 30 * DAY_SECONDS + 60
    store.fail_ts = (start + 14 * DAY_SECONDS - 60) * 1000
    job = producer(refs, state, yahoo, store)
    result = await job._collect(entity(refs), refs.capability["limits"]["1m"], NOW)
    assert result["status"] == "failed" and result["pending_retained"]
    path, pending_path = job._paths(entity(refs))
    saved = json.loads(path.read_text())
    assert saved["covered_until_s"] == start + 7 * DAY_SECONDS
    assert saved["backfill_complete_until_s"] is None
    assert saved["last_source_bar_ms"] == (start + 7 * DAY_SECONDS - 60) * 1000
    pending = json.loads(pending_path.read_text())
    assert pending["start_s"] == start + 7 * DAY_SECONDS
    assert (
        hashlib.sha256(pending["source"]["raw_json"].encode()).hexdigest()
        == pending["source"]["payload_sha256"]
    )
    events.clear()
    restarted = producer(refs, state, yahoo, store)
    result = await restarted._collect(entity(refs), refs.capability["limits"]["1m"], NOW)
    assert result["status"] == "verified" and result["replayed_rows"] == 3
    assert [e[0] for e in events[:3]] == ["write", "verify", "fetch"]
    assert events[2][3] == start + 14 * DAY_SECONDS


@pytest.mark.asyncio
async def test_readback_failure_does_not_advance_and_quote_only_does_not_complete(refs, tmp_path):
    state = tmp_path / "state-intraday"
    yahoo, store = Yahoo(refs.indices), Store(state)
    start = NOW - 30 * DAY_SECONDS + 60
    store.mismatch_ts = (start + 7 * DAY_SECONDS - 60) * 1000
    job = producer(refs, state, yahoo, store)
    result = await job._collect(entity(refs), refs.capability["limits"]["1m"], NOW)
    path, pending = job._paths(entity(refs))
    assert result["status"] == "failed" and pending.exists()
    assert json.loads(path.read_text())["covered_until_s"] is None
    other_state = tmp_path / "only-quote"
    yahoo.quote_only = True
    other = producer(refs, other_state, yahoo, Store(other_state))
    result = await other._collect(entity(refs), refs.capability["limits"]["1m"], NOW)
    assert result["status"] == "failed" and result["stage"] == "fetch"
    assert json.loads(other._paths(entity(refs))[0].read_text())["covered_until_s"] is None


@pytest.mark.asyncio
async def test_true_empty_receipt_advances_request_end_without_fabricating_bar(refs, tmp_path):
    state = tmp_path / "state-intraday"
    yahoo, store = Yahoo(refs.indices), Store(state)
    job, index = producer(refs, state, yahoo, store), entity(refs)
    path, pending_path = job._paths(index)
    start, end = NOW - 7200, NOW
    raw = source(index, "1m", [], empty=True, start=start, end=end)
    pending = {
        "identity": job._identity(index),
        "start_s": start,
        "end_s": end,
        "run_end_s": end,
        "source": raw,
    }
    _atomic_json(pending_path, pending)
    saved = await job._apply(index, pending, path)
    assert saved["covered_until_s"] == end and saved["last_source_bar_ms"] is None
    assert saved["gaps"] == [] and saved["last_verified_window"]["verified_rows"] == 0
    assert store.rows == {}
    broken = copy.deepcopy(raw)
    broken["requested_range"]["end"] += 1
    with pytest.raises(ValueError, match="requested window"):
        job._source(index, broken, start, end)
    broken = copy.deepcopy(raw)
    broken["source_empty"] = False
    with pytest.raises(ValueError, match="Unconfirmed"):
        job._source(index, broken, start, end)


@pytest.mark.asyncio
async def test_null_gap_retained_recovered_and_long_stop_records_unavailable_range(refs, tmp_path):
    state = tmp_path / "state-intraday"
    yahoo, store = Yahoo(refs.indices), Store(state)
    job, index = producer(refs, state, yahoo, store), entity(refs)
    path, pending_path = job._paths(index)
    stamp = NOW - 120

    def window(close):
        rows = [point(index, "1m", stamp, close=close), point(index, "1m", NOW - 60)]
        return {
            "identity": job._identity(index),
            "start_s": NOW - 600,
            "end_s": NOW,
            "run_end_s": NOW,
            "source": source(index, "1m", rows, start=NOW - 600, end=NOW),
        }

    _atomic_json(pending_path, window(None))
    saved = await job._apply(index, window(None), path)
    assert saved["gaps"] == [stamp * 1000]
    _atomic_json(pending_path, window(10.0))
    saved = await job._apply(index, window(10.0), path)
    assert saved["gaps"] == []
    pending_path.unlink()
    later = NOW + 45 * DAY_SECONDS
    restarted = producer(refs, state, yahoo, store, now=later)
    result = await restarted._collect(index, refs.capability["limits"]["1m"], later)
    assert result["retention_unavailable_ranges"]
    assert yahoo.calls[0][2] == later - 30 * DAY_SECONDS + 60
    assert result["covered_until_s"] == later


def test_cli_requires_fc_endpoint_and_accepts_one_cycle_without_proxy(monkeypatch):
    monkeypatch.delenv("CROSS_MARKET_FC_ENDPOINT", raising=False)
    monkeypatch.setattr("sys.argv", ["collector"])
    with pytest.raises(SystemExit):
        cli.arguments()
    monkeypatch.setattr(
        "sys.argv",
        [
            "collector",
            "--fc-endpoint",
            "https://account.us-west-1.fc.aliyuncs.com",
            "--loop-seconds",
            "0",
        ],
    )
    args = cli.arguments()
    assert args.batch_size == 1000 and args.concurrency == 2 and args.loop_seconds == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("interval,retention", [("1m", 30 * DAY_SECONDS), ("5m", 60 * DAY_SECONDS)])
async def test_queue_delay_rechecks_rolling_retention_before_fetch(
    refs, tmp_path, interval, retention
):
    state = tmp_path / "state-intraday"
    yahoo, store = Yahoo(refs.indices), Store(state)
    job = producer(refs, state, yahoo, store, now=NOW + 600)
    result = await job._collect(entity(refs, interval), refs.capability["limits"][interval], NOW)
    assert yahoo.calls[0][2] == NOW + 600 - retention + (60 if interval == "1m" else 300)
    assert all(
        call[3] - call[2] <= refs.capability["limits"][interval]["max_window_seconds"]
        for call in yahoo.calls
    )
    assert result["retention_unavailable_ranges"]
    assert result["request_boundary_guard_ranges"]
    assert result["covered_until_s"] == NOW


def test_nonempty_source_range_mismatch_is_rejected(refs):
    index = entity(refs)
    start, end = NOW - 600, NOW
    fetched = source(index, "1m", [point(index, "1m", NOW - 60)], start=start, end=end)
    fetched["requested_range"]["end"] = end - 60
    with pytest.raises(ValueError, match="requested window"):
        CrossMarketIntradayIngestor._source(index, fetched, start, end)


@pytest.mark.asyncio
async def test_next_cycle_rewinds_real_bar_one_hour_instead_of_quote_or_request_end(refs, tmp_path):
    state = tmp_path / "state-intraday"
    yahoo, store = Yahoo(refs.indices), Store(state)
    index = entity(refs)
    first = producer(refs, state, yahoo, store)
    await first._collect(index, refs.capability["limits"]["1m"], NOW)
    yahoo.calls.clear()
    next_cycle = producer(refs, state, yahoo, store, now=NOW + 3600)
    result = await next_cycle._collect(index, refs.capability["limits"]["1m"], NOW + 3600)
    assert yahoo.calls[0][2] == NOW - 60 - 3600
    assert yahoo.calls[-1][3] == NOW + 3600
    assert result["covered_until_s"] == NOW + 3600


@pytest.mark.asyncio
async def test_five_minute_non_grid_cutoff_keeps_next_available_bar(refs, tmp_path):
    state = tmp_path / "state-intraday"
    yahoo, store = Yahoo(refs.indices), Store(state)
    now = NOW + 61
    job = producer(refs, state, yahoo, store, now=now)
    index = entity(refs, "5m")
    result = await job._collect(index, refs.capability["limits"]["5m"], now)
    cutoff = now - 60 * DAY_SECONDS
    next_grid = NOW - 60 * DAY_SECONDS + 300
    assert yahoo.calls[0][2] == next_grid
    assert yahoo.calls[0][2] < cutoff + 300
    assert any(
        row["data_kind"] == "minute_bar" and row["ts"] == next_grid * 1000
        for row in store.rows.values()
    )
    initial_guard = result["request_boundary_guard_ranges"][0]
    assert initial_guard["source_grid_bar_count"] == 0
    assert initial_guard["unguaranteed_boundary_bar_timestamps_s"] == []


def test_exact_grid_boundary_records_one_unassured_bar():
    start = CrossMarketIntradayIngestor._safe_start(NOW, "5m")
    guard = CrossMarketIntradayIngestor._guard_record(NOW, start, "5m", "boundary")
    assert start == NOW + 300
    assert guard["unguaranteed_boundary_bar_timestamps_s"] == [NOW]
    assert guard["source_grid_bar_count"] == 1
    assert guard["source_bar_loss"] == "boundary_bar_availability_unverified"


@pytest.mark.asyncio
async def test_progress_emits_each_finished_series_without_raw_or_exception_text(refs, tmp_path):
    state = tmp_path / "state-intraday"
    yahoo, store = Yahoo(refs.indices), Store(state)
    job = producer(refs, state, yahoo, store)
    progress = []
    job.progress = progress.append
    result = await job.run_once()
    assert len(progress) == result["series_count"] == 4
    assert all(item["event"] == "intraday_series_finished" for item in progress)
    assert all(item["status"] == "verified" for item in progress)
    assert all("raw_json" not in item and "url" not in item for item in progress)
