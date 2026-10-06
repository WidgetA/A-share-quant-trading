"""Replayable Massive history backfill into the existing 18-field price table."""

import hashlib
import json
import os
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

from src.data.cross_market_ingest import _atomic_json, _failure_details, _read_json
from src.data.massive_indices import MassiveIndexError, parse_aggregates, validate_index


def load_massive_reference(reference_path: Path, industry_reference_path: Path):
    raw = Path(reference_path).read_bytes()
    reference = json.loads(raw)
    industry_raw = Path(industry_reference_path).read_bytes()
    industry = json.loads(industry_raw)
    if (
        reference.get("schema_version") != 1
        or reference.get("provider") != "massive"
        or reference.get("interval") != "5m"
        or reference.get("industry_reference_sha256") != hashlib.sha256(industry_raw).hexdigest()
        or not isinstance(reference.get("indices"), list)
        or not reference["indices"]
    ):
        raise ValueError("Massive reference does not bind the industry reference")
    originals = {item["index_id"]: item for item in industry["indices"]}
    seen, vendors = set(), set()
    for item in reference["indices"]:
        validate_index(item)
        original = originals.get(item["index_id"])
        if original is None or any(
            item.get(k) != original.get(k)
            for k in (
                "market",
                "symbol",
                "name",
                "native_symbol",
                "instrument_type",
                "currency",
                "exchange_timezone",
            )
        ):
            raise ValueError("Massive index differs from the original index")
        if item["index_id"] in seen or item["vendor_ticker"] in vendors:
            raise ValueError("Duplicate Massive index")
        seen.add(item["index_id"])
        vendors.add(item["vendor_ticker"])
    reference["reference_sha256"] = hashlib.sha256(raw).hexdigest()
    return reference


def quarter_windows(start: date, end_exclusive: date):
    """Cover every requested calendar date, including source-empty quarters."""
    windows = []
    while start < end_exclusive:
        month = ((start.month - 1) // 3 + 1) * 3 + 1
        boundary = date(start.year + 1, 1, 1) if month == 13 else date(start.year, month, 1)
        stop = min(boundary, end_exclusive)
        windows.append(
            {"start_date": start.isoformat(), "end_date": (stop - timedelta(days=1)).isoformat()}
        )
        start = stop
    return windows


@contextmanager
def queue_lock(state_dir: Path):
    """An OS lock prevents two producers from spending the same API allowance."""
    state_dir.mkdir(parents=True, exist_ok=True)
    with (state_dir / "queue.lock").open("a+b") as stream:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise MassiveIndexError("queue_already_running") from None
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


class CrossMarketMassiveIngestor:
    def __init__(
        self,
        *,
        reference_path: Path,
        industry_reference_path: Path,
        state_dir: Path,
        massive,
        store,
        progress=None,
    ):
        self.reference = load_massive_reference(reference_path, industry_reference_path)
        self.state_dir = Path(state_dir)
        self.massive, self.store, self.progress = massive, store, progress

    def _identity(self, index):
        return {
            k: index[k]
            for k in (
                "index_id",
                "market",
                "symbol",
                "vendor_ticker",
                "name",
                "currency",
                "exchange_timezone",
                "native_symbol",
                "official_name",
                "return_basis",
            )
        } | {"provider": "massive", "interval": "5m"}

    def series_dir(self, index):
        serialized = json.dumps(self._identity(index), sort_keys=True).encode()
        return self.state_dir / hashlib.sha256(serialized).hexdigest()

    def _emit(self, index, **details):
        if self.progress is not None:
            self.progress(
                {
                    "provider": "massive",
                    "index_id": index["index_id"],
                    "symbol": index["symbol"],
                    **details,
                }
            )

    def _validate_fetched(self, index, window, fetched):
        identity = self._identity(index)
        if any(
            fetched.get(k) != identity[k]
            for k in ("index_id", "market", "symbol", "vendor_ticker", "interval")
        ):
            raise MassiveIndexError("pending_identity_mismatch")
        if any(fetched.get(k) != window[k] for k in ("start_date", "end_date")):
            raise MassiveIndexError("pending_window_mismatch")
        raw = fetched.get("raw_json")
        if (
            not isinstance(raw, str)
            or fetched.get("http_status") != 200
            or hashlib.sha256(raw.encode()).hexdigest() != fetched.get("payload_sha256")
        ):
            raise MassiveIndexError("pending_raw_mismatch")
        from urllib.parse import quote

        from src.data.massive_indices import API_ROOT, BASE_LIMIT

        expected_url = (
            f"{API_ROOT}/v2/aggs/ticker/{quote(index['vendor_ticker'], safe='')}/range/5/minute/"
            f"{window['start_date']}/{window['end_date']}"
        )
        if fetched.get("request_url") != expected_url or fetched.get("request_params") != {
            "sort": "asc",
            "limit": BASE_LIMIT,
        }:
            raise MassiveIndexError("pending_request_mismatch")
        parsed = parse_aggregates(
            json.loads(raw),
            index,
            **window,
            fetched_at=fetched["fetched_at"],
        )
        if any(parsed[k] != fetched.get(k) for k in parsed):
            raise MassiveIndexError("pending_source_values_mismatch")
        return parsed

    async def _commit(self, directory, index, state, pending):
        window, fetched = pending["window"], pending["fetched"]
        if pending.get("identity") != self._identity(index):
            raise MassiveIndexError("pending_identity_mismatch")
        parsed = self._validate_fetched(index, window, fetched)
        if parsed["truncated"]:
            raise MassiveIndexError("truncated_pending")
        written = await self.store.upsert_prices(parsed["points"])
        verified = await self.store.verify_prices(parsed["points"])
        if written != len(parsed["points"]) or verified != len(parsed["points"]):
            raise MassiveIndexError("row_acknowledgement_mismatch")
        receipt = {
            **pending,
            "status": "verified",
            "written_rows": written,
            "verified_rows": verified,
        }
        receipt_name = f"{window['start_date']}_{window['end_date']}.verified.json"
        _atomic_json(directory / "receipts" / receipt_name, receipt)
        stop = (date.fromisoformat(window["end_date"]) + timedelta(days=1)).isoformat()
        covered = state["covered_until_exclusive"]
        if stop > covered:
            if window["start_date"] != covered or not state["todo"] or state["todo"][0] != window:
                raise MassiveIndexError("noncontiguous_checkpoint")
            state["todo"].pop(0)
            state["covered_until_exclusive"] = stop
            state["verified_rows"] += verified
            state["verified_windows"].append(
                {
                    **window,
                    "source_empty": parsed["source_empty"],
                    "verified_rows": verified,
                    "payload_sha256": fetched["payload_sha256"],
                    "source_first_date": parsed["source_first_date"],
                    "source_last_date": parsed["source_last_date"],
                }
            )
            for key, choose in (("source_first_date", min), ("source_last_date", max)):
                value = parsed[key]
                if value is not None:
                    state[key] = value if state[key] is None else choose(value, state[key])
            state["last_failure"] = None
            _atomic_json(directory / "state.json", state)
        # A crash after the state commit but before this unlink replays the same
        # source rows. The stop<=covered branch keeps counters idempotent.
        (directory / "pending.json").unlink()
        self._emit(
            index,
            status="window_verified",
            **window,
            rows=verified,
            source_empty=parsed["source_empty"],
            replayed=pending.get("replayed", False),
        )
        return verified

    async def _collect(self, index, start: date, stop: date):
        directory = self.series_dir(index)
        directory.mkdir(parents=True, exist_ok=True)
        state_path, pending_path = directory / "state.json", directory / "pending.json"
        if state_path.exists():
            state = _read_json(state_path)
            if (
                state.get("identity") != self._identity(index)
                or state.get("start_date") != start.isoformat()
            ):
                raise MassiveIndexError("state_identity_or_start_mismatch")
        else:
            state = {
                "schema_version": 1,
                "identity": self._identity(index),
                "start_date": start.isoformat(),
                "covered_until_exclusive": start.isoformat(),
                "todo": [],
                "verified_rows": 0,
                "verified_windows": [],
                "source_first_date": None,
                "source_last_date": None,
                "last_failure": None,
                "reference_sha256": self.reference["reference_sha256"],
            }
            _atomic_json(state_path, state)
        rows, replayed = 0, False
        try:
            if pending_path.exists():
                pending = _read_json(pending_path)
                pending["replayed"] = True
                rows += await self._commit(directory, index, state, pending)
                replayed = True
            tail = (
                date.fromisoformat(state["todo"][-1]["end_date"]) + timedelta(days=1)
                if state["todo"]
                else date.fromisoformat(state["covered_until_exclusive"])
            )
            state["todo"].extend(quarter_windows(tail, stop))
            _atomic_json(state_path, state)
            while state["todo"]:
                window = state["todo"][0]
                fetched = await self.massive.fetch(index, **window)
                parsed = self._validate_fetched(index, window, fetched)
                pending = {
                    "schema_version": 1,
                    "identity": self._identity(index),
                    "reference_sha256": self.reference["reference_sha256"],
                    "window": window,
                    "fetched": fetched,
                }
                if parsed["truncated"]:
                    _atomic_json(
                        directory
                        / "truncated"
                        / f"{window['start_date']}_{window['end_date']}.json",
                        pending,
                    )
                    first = date.fromisoformat(window["start_date"])
                    end = date.fromisoformat(window["end_date"]) + timedelta(days=1)
                    if (end - first).days <= 1:
                        raise MassiveIndexError("single_day_base_limit_reached")
                    middle = first + timedelta(days=(end - first).days // 2)
                    state["todo"][:1] = [
                        {
                            "start_date": first.isoformat(),
                            "end_date": (middle - timedelta(days=1)).isoformat(),
                        },
                        {
                            "start_date": middle.isoformat(),
                            "end_date": (end - timedelta(days=1)).isoformat(),
                        },
                    ]
                    _atomic_json(state_path, state)
                    self._emit(
                        index,
                        status="window_split_base_limit",
                        **window,
                        query_count=parsed["query_count"],
                    )
                    continue
                # Complete source bytes and all expected fields reach stable
                # storage before the first possibly partial database write.
                _atomic_json(pending_path, pending)
                rows += await self._commit(directory, index, state, pending)
            state["requested_complete_until"] = (stop - timedelta(days=1)).isoformat()
            _atomic_json(state_path, state)
            return {
                "index_id": index["index_id"],
                "status": "verified",
                "requested_complete_until": state["requested_complete_until"],
                "source_first_date": state["source_first_date"],
                "source_last_date": state["source_last_date"],
                "verified_rows": rows,
                "replayed": replayed,
                "source_empty_windows": sum(w["source_empty"] for w in state["verified_windows"]),
                "state_reference_sha256": state["reference_sha256"],
                "current_reference_sha256": self.reference["reference_sha256"],
                "reference_changed_same_identity": (
                    state["reference_sha256"] != self.reference["reference_sha256"]
                ),
                "coverage_kind": "verified_requested_windows_not_calendar_bar_completeness",
            }
        except Exception as exc:
            details = _failure_details(exc)
            if isinstance(exc, MassiveIndexError):
                details["classification"] = exc.classification
                if exc.evidence is not None:
                    _atomic_json(
                        directory / "failures" / f"{exc.evidence['fetched_at']}.json", exc.evidence
                    )
            state["last_failure"] = details
            _atomic_json(state_path, state)
            self._emit(index, status="failed", **details)
            return {"index_id": index["index_id"], "status": "failed", **details}

    async def run_once(self, *, start_date="2023-01-01", end_date: str):
        start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
        if end < start:
            raise ValueError("History end precedes start")
        stop = end + timedelta(days=1)
        with queue_lock(self.state_dir):
            await self.store.ensure_schema()
            results = []
            for index in self.reference["indices"]:
                result = await self._collect(index, start, stop)
                results.append(result)
                if result.get("classification") == "authentication_denied":
                    break
        failed = [x for x in results if x["status"] == "failed"]
        complete = len(results) == len(self.reference["indices"]) and not failed
        return {
            "provider": "massive",
            "status": "verified" if complete else "partial_failure",
            "start_date": start_date,
            "end_date": end_date,
            "reference_sha256": self.reference["reference_sha256"],
            "verified_rows": sum(x.get("verified_rows", 0) for x in results),
            "results": results,
            "failed": failed,
            "unattempted": len(self.reference["indices"]) - len(results),
        }
