"""Durable request-window coverage for referenced US/KR minute indices."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from src.data.cross_market_ingest import (
    CrossMarketIngestor,
    _atomic_json,
    _failure_details,
    _read_json,
    load_reference,
)

INTERVAL_SECONDS = {"1m": 60, "5m": 300}
DAY_SECONDS = 86400


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def load_intraday_reference(reference_path: Path, base_path: Path, capability_path: Path) -> dict:
    reference = load_reference(reference_path, base_path)
    raw = capability_path.read_bytes()
    capability = json.loads(raw)
    if type(capability.get("schema_version")) is not int or capability["schema_version"] != 1:
        raise ValueError("Unsupported intraday reference schema")
    if capability.get("base_reference_sha256") != reference["reference_sha256"]:
        raise ValueError("Intraday capability reference is not bound to the index reference")
    limits = capability.get("limits")
    if not isinstance(limits, dict):
        raise ValueError("Missing verified provider limits")
    for interval, policy in limits.items():
        if interval not in INTERVAL_SECONDS or not isinstance(policy, dict):
            raise ValueError("Unsupported minute interval policy")
        for field in ("retention_seconds", "max_window_seconds"):
            _integer(policy.get(field), field, minimum=INTERVAL_SECONDS[interval])
    adopted = {index["index_id"]: index for index in reference["indices"]}
    selected, seen = [], set()
    for entity in capability["indices"]:
        index = adopted.get(entity["index_id"])
        if index is None or entity["index_id"] in seen:
            raise ValueError("Unknown or duplicate minute index")
        seen.add(entity["index_id"])
        if any(entity.get(field) != index[field] for field in ("market", "symbol")):
            raise ValueError("Minute identity differs from the adopted index")
        intervals = entity.get("intervals")
        if not isinstance(intervals, list) or len(intervals) != len(set(intervals)):
            raise ValueError("Invalid supported interval list")
        for interval in intervals:
            if interval not in limits:
                raise ValueError("Supported interval has no verified provider limits")
            selected.append({**index, "interval": interval})
    return {
        "reference_sha256": reference["reference_sha256"],
        "capability_sha256": hashlib.sha256(raw).hexdigest(),
        "indices": selected,
        "limits": limits,
    }


class CrossMarketIntradayIngestor:
    """Persist/replay original source before advancing verified request coverage."""

    def __init__(
        self,
        *,
        reference_path: str | Path,
        base_reference_path: str | Path,
        capability_path: str | Path,
        state_dir: str | Path,
        yahoo: Any,
        store: Any,
        concurrency: int = 2,
        clock: Callable[[], float] = time.time,
        progress: Callable[[dict], None] | None = None,
    ):
        _integer(concurrency, "concurrency", minimum=1)
        self.reference_path = Path(reference_path)
        self.base_reference_path = Path(base_reference_path)
        self.capability_path = Path(capability_path)
        self.state_dir = Path(state_dir)
        self.yahoo, self.store = yahoo, store
        self.concurrency, self.clock = concurrency, clock
        self.progress = progress

    @staticmethod
    def _identity(index: Mapping[str, Any]) -> dict:
        return {field: index[field] for field in ("index_id", "market", "symbol", "interval")}

    def _paths(self, index: Mapping[str, Any]) -> tuple[Path, Path]:
        raw = json.dumps(self._identity(index), sort_keys=True).encode()
        key = hashlib.sha256(raw).hexdigest()
        return self.state_dir / f"{key}.state.json", self.state_dir / f"{key}.pending.json"

    @staticmethod
    def _safe_start(cutoff: int, interval: str) -> int:
        # A non-grid prefix contains no interval bar. At an exact grid boundary,
        # the boundary bar can expire during request transit, so use the next.
        step = INTERVAL_SECONDS[interval]
        return (cutoff // step + 1) * step

    @staticmethod
    def _guard_record(start: int, end: int, interval: str, reason: str) -> dict:
        step = INTERVAL_SECONDS[interval]
        first_grid = ((start + step - 1) // step) * step
        grids = list(range(first_grid, end, step))
        return {
            "start_s": start,
            "end_s": end,
            "reason": reason,
            "source_grid_bar_count": len(grids),
            "unguaranteed_boundary_bar_timestamps_s": grids,
            "source_bar_loss": "boundary_bar_availability_unverified"
            if grids
            else ("prefix_has_no_interval_grid_timestamp"),
        }

    def _state(self, index: Mapping[str, Any], path: Path) -> dict:
        if not path.exists():
            return {
                "identity": self._identity(index),
                "covered_until_s": None,
                "backfill_complete_until_s": None,
                "last_source_bar_ms": None,
                "gaps": [],
                "retention_unavailable_ranges": [],
                "request_boundary_guard_ranges": [],
            }
        state = _read_json(path)
        if state.get("identity") != self._identity(index):
            raise ValueError("Persisted minute identity differs from the reference")
        for field in ("covered_until_s", "backfill_complete_until_s", "last_source_bar_ms"):
            if state.get(field) is not None:
                _integer(state[field], field)
        if not isinstance(state.get("gaps"), list):
            raise ValueError("Invalid persisted minute gaps")
        for stamp in state["gaps"]:
            _integer(stamp, "gap timestamp")
        if not isinstance(state.get("retention_unavailable_ranges"), list):
            raise ValueError("Invalid persisted retention ranges")
        state.setdefault("request_boundary_guard_ranges", [])
        return state

    @staticmethod
    def _source(index: Mapping[str, Any], fetched: Mapping[str, Any], start: int, end: int) -> dict:
        if fetched.get("requested_range") != {
            "start": start,
            "end": end,
            "interval": index["interval"],
        }:
            raise ValueError("Source receipt differs from the requested window")
        if fetched.get("points"):
            source = CrossMarketIngestor._source_window(index, fetched)
            step = INTERVAL_SECONDS[index["interval"]]
            for point in source["points"]:
                if point.get("interval") != index["interval"] or point.get("data_kind") not in {
                    "minute_bar",
                    "minute_quote_snapshot",
                }:
                    raise ValueError("Point does not match the minute series")
                if point["data_kind"] == "minute_bar" and point["ts"] % (step * 1000):
                    raise ValueError("Minute bar is not on the requested interval grid")
            bars = [
                point
                for point in source["points"]
                if point["data_kind"] == "minute_bar"
                and point.get("close") is not None
                and (start - step) * 1000 <= point["ts"] < (end + step) * 1000
            ]
            if not bars:
                raise ValueError("Window has no inside or boundary-overlap minute bars")
            return source
        # A provider-confirmed empty calendar window is a zero-row receipt,
        # never a generic empty list, a 404, or a quote-only response.
        if fetched.get("source_empty") is not True or fetched.get("points") != []:
            raise ValueError("Unconfirmed empty minute response")
        raw = fetched.get("raw_json")
        if not isinstance(raw, str) or hashlib.sha256(raw.encode()).hexdigest() != fetched.get(
            "payload_sha256"
        ):
            raise ValueError("Empty source SHA256 does not match")
        chart = json.loads(raw).get("chart", {})
        results = chart.get("result")
        if chart.get("error") or not isinstance(results, list) or len(results) != 1:
            raise ValueError("Empty source has no unique successful index result")
        result = results[0]
        metadata = result.get("meta", {})
        if (
            metadata != fetched.get("metadata")
            or result.get("timestamp") not in (None, [])
            or result.get("indicators", {}).get("quote") != [{}]
            or metadata.get("symbol") != index["symbol"]
            or metadata.get("instrumentType") != "INDEX"
            or metadata.get("currency") != index["currency"]
            or metadata.get("exchangeTimezoneName") != index["exchange_timezone"]
            or metadata.get("dataGranularity") != index["interval"]
        ):
            raise ValueError("Empty source shape or index identity was not established")
        names = [metadata[field] for field in ("shortName", "longName") if metadata.get(field)]
        if not names or any(name not in index.get("yahoo_names", []) for name in names):
            raise ValueError("Empty source name differs from verified Yahoo aliases")
        return dict(fetched)

    async def _apply(self, index: dict, window: dict, state_path: Path) -> dict:
        if window.get("identity") != self._identity(index):
            raise ValueError("Pending minute identity differs from the reference")
        start = _integer(window.get("start_s"), "window start")
        end = _integer(window.get("end_s"), "window end", minimum=start + 1)
        _integer(window.get("run_end_s"), "cycle end", minimum=end)
        source = self._source(index, window["source"], start, end)
        points = source["points"]
        await self.store.upsert_prices(points)
        verified = await self.store.verify_prices(points)
        state = self._state(index, state_path)
        gaps = set(state["gaps"])
        step = INTERVAL_SECONDS[index["interval"]]
        bars = []
        for point in points:
            if point["data_kind"] != "minute_bar":
                continue
            if point.get("close") is None:
                gaps.add(point["ts"])
            else:
                gaps.discard(point["ts"])
                if (start - step) * 1000 <= point["ts"] < (end + step) * 1000:
                    bars.append(point["ts"])
        previous_bar = state["last_source_bar_ms"]
        if bars:
            state["last_source_bar_ms"] = max(bars + ([previous_bar] if previous_bar else []))
        state["covered_until_s"] = max(state["covered_until_s"] or 0, end)
        if end == window["run_end_s"]:
            state["backfill_complete_until_s"] = max(state["backfill_complete_until_s"] or 0, end)
        state.update(
            gaps=sorted(gaps),
            source_sha256=source["payload_sha256"],
            source_fetched_at=source.get("fetched_at"),
            verified_at_ms=int(self.clock() * 1000),
            last_verified_window={
                "start_s": start,
                "end_s": end,
                "rows": len(points),
                "verified_rows": verified,
                "source_empty": source.get("source_empty") is True,
                "source_sha256": source["payload_sha256"],
            },
        )
        _atomic_json(state_path, state)
        return state

    async def _collect(self, index: dict, policy: dict, run_end: int) -> dict:
        state_path, pending_path = self._paths(index)
        stage, replayed, rows, windows = "pending_replay", 0, 0, []
        try:
            if pending_path.exists():
                pending = _read_json(pending_path)
                pending_index = pending.get("index", index)
                if self._identity(pending_index) != self._identity(index):
                    raise ValueError("Pending entity differs from the current minute index")
                await self._apply(pending_index, pending, state_path)
                replayed = len(pending["source"]["points"])
                pending_path.unlink()
            state = self._state(index, state_path)
            retention = policy["retention_seconds"]
            earliest = max(0, int(self.clock()) - retention)
            if state["backfill_complete_until_s"] is None:
                start = state["covered_until_s"]
                if start is None:
                    initial_cutoff = max(0, run_end - retention)
                    start = self._safe_start(initial_cutoff, index["interval"])
                    state["request_boundary_guard_ranges"].append(
                        self._guard_record(
                            initial_cutoff,
                            start,
                            index["interval"],
                            "initial_request_boundary_guard",
                        )
                    )
                    _atomic_json(state_path, state)
            else:
                latest = state["last_source_bar_ms"]
                start = max(0, (latest // 1000 if latest else state["covered_until_s"]) - 3600)
                live_gaps = [
                    stamp // 1000 for stamp in state["gaps"] if earliest <= stamp // 1000 < run_end
                ]
                if live_gaps:
                    start = min(start, min(live_gaps))
            span = policy["max_window_seconds"]
            if index["interval"] == "1m":
                span = min(span, 7 * DAY_SECONDS)
            while start < run_end:
                # Recheck immediately before each fetch: queuing/cooldown can
                # outlive the initial guard while the provider's horizon slides.
                now = int(self.clock())
                cutoff = max(0, now - retention)
                safe_start = self._safe_start(cutoff, index["interval"])
                if start < safe_start:
                    expired_end = min(cutoff, run_end)
                    if start < expired_end:
                        state["retention_unavailable_ranges"].append(
                            {
                                "start_s": start,
                                "end_s": expired_end,
                                "retention_seconds": retention,
                                "recorded_at_s": now,
                            }
                        )
                    guard_start, guard_end = max(start, cutoff), min(safe_start, run_end)
                    if guard_start < guard_end:
                        state["request_boundary_guard_ranges"].append(
                            self._guard_record(
                                guard_start,
                                guard_end,
                                index["interval"],
                                "rolling_request_boundary_guard",
                            )
                        )
                    _atomic_json(state_path, state)
                    start = safe_start
                    if start >= run_end:
                        break
                end = min(start + span, run_end)
                stage = "fetch"
                fetched = await self.yahoo.fetch(
                    index["symbol"],
                    index["market"],
                    start=start,
                    end=end,
                    interval=index["interval"],
                )
                source = self._source(index, fetched, start, end)
                pending = {
                    "identity": self._identity(index),
                    "index": index,
                    "start_s": start,
                    "end_s": end,
                    "run_end_s": run_end,
                    "source": source,
                }
                stage = "persist_pending"
                _atomic_json(pending_path, pending)
                stage = "write_and_verify"
                state = await self._apply(index, pending, state_path)
                pending_path.unlink()
                windows.append(state["last_verified_window"])
                rows += len(source["points"])
                start = end
            return {
                **self._identity(index),
                "status": "verified_with_gaps"
                if (
                    state["gaps"]
                    or state["retention_unavailable_ranges"]
                    or (state["covered_until_s"] or 0) < run_end
                )
                else "verified",
                "rows": rows,
                "replayed_rows": replayed,
                "windows": windows,
                "covered_until_s": state["covered_until_s"],
                "backfill_complete_until_s": state["backfill_complete_until_s"],
                "last_source_bar_ms": state["last_source_bar_ms"],
                "gap_timestamps": state["gaps"],
                "retention_unavailable_ranges": state["retention_unavailable_ranges"],
                "request_boundary_guard_ranges": state["request_boundary_guard_ranges"],
            }
        except Exception as exc:
            return {
                **self._identity(index),
                "status": "failed",
                "stage": stage,
                **_failure_details(exc),
                "pending_retained": pending_path.exists(),
                "replayed_rows": replayed,
                "rows": rows,
                "windows": windows,
            }

    async def run_once(self) -> dict:
        reference = load_intraday_reference(
            self.reference_path,
            self.base_reference_path,
            self.capability_path,
        )
        await self.store.ensure_schema()
        run_end = int(self.clock())
        semaphore = asyncio.Semaphore(self.concurrency)

        async def collect(index):
            async with semaphore:
                result = await self._collect(index, reference["limits"][index["interval"]], run_end)
                if self.progress is not None:
                    self.progress(
                        {
                            "event": "intraday_series_finished",
                            **self._identity(index),
                            **{
                                field: result.get(field)
                                for field in (
                                    "status",
                                    "stage",
                                    "error_type",
                                    "rows",
                                    "replayed_rows",
                                    "covered_until_s",
                                    "backfill_complete_until_s",
                                    "last_source_bar_ms",
                                )
                            },
                            "gap_count": len(result.get("gap_timestamps", [])),
                            "pending_retained": result.get("pending_retained", False),
                        }
                    )
                return result

        results = await asyncio.gather(*(collect(index) for index in reference["indices"]))
        status = (
            "partial_failure"
            if any(r["status"] == "failed" for r in results)
            else (
                "verified_with_gaps"
                if any(r["status"] == "verified_with_gaps" for r in results)
                else "verified"
            )
        )
        return {
            "status": status,
            "reference_sha256": reference["reference_sha256"],
            "capability_sha256": reference["capability_sha256"],
            "run_end_s": run_end,
            "series_count": len(results),
            "results": results,
        }
