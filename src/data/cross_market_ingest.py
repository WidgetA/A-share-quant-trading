"""Collect only referenced Yahoo indices with durable, replayable Greptime windows."""

import asyncio
import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.data.cross_market_store import GreptimeWriteError

DAY_MS = 86_400_000


def _failure_details(exc: Exception) -> dict[str, Any]:
    """Expose acknowledgement progress and exception types, without their text or URLs."""
    details: dict[str, Any] = {"error_type": type(exc).__name__}
    if isinstance(exc, GreptimeWriteError):
        details["confirmed_rows"] = exc.confirmed_rows
        details["cause_type"] = type(exc.__cause__).__name__ if exc.__cause__ else None
    return details


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path.name}")
    return value


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    """Persist the complete new file before atomically replacing its predecessor."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="\n", dir=path.parent, delete=False
        ) as stream:
            temporary = Path(stream.name)
            json.dump(value, stream, ensure_ascii=False, sort_keys=True, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
        # Linux needs the containing directory synced too: fsyncing the file
        # does not persist the rename itself. Windows has no O_DIRECTORY.
        if hasattr(os, "O_DIRECTORY"):
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _milliseconds(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be integer milliseconds")
    return value


def _version_ms(value: Any) -> int:
    if not isinstance(value, str):
        raise ValueError("checked_at must be an ISO UTC reference time")
    when = datetime.fromisoformat(value.replace("Z", "+00:00"))
    offset = when.utcoffset()
    if when.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError("checked_at must be an ISO UTC reference time")
    return int(when.timestamp() * 1000)


def _linked_ids(value: Any, fields: set[str]) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if key in fields:
                if isinstance(item, str):
                    found.add(item)
                elif isinstance(item, list):
                    found.update(part for part in item if isinstance(part, str))
            found.update(_linked_ids(item, fields))
    elif isinstance(value, list):
        for item in value:
            found.update(_linked_ids(item, fields))
    return found


def load_reference(reference_path: Path, base_path: Path) -> dict[str, Any]:
    """Validate the entire industry scope before any fetch or database mutation."""
    raw = reference_path.read_bytes()
    document = json.loads(raw)
    base_raw = base_path.read_bytes()
    base = json.loads(base_raw)
    if document.get("schema_version") != 1:
        raise ValueError("Unsupported industry-index reference schema")
    base_sha = hashlib.sha256(base_raw).hexdigest()
    if document.get("base_reference_sha256") != base_sha:
        raise ValueError("Base industry reference SHA256 does not match")
    reference_at = _version_ms(document.get("checked_at"))
    base_industries = {item["sw_code"]: item for item in base["industries"]}
    base_sources = {item["id"]: item for item in base.get("sources", [])}
    base_boards = {item["id"]: item for item in base.get("boards", [])}
    industries = document["industries"]
    codes = [item["sw_code"] for item in industries]
    if len(codes) != len(set(codes)) or set(codes) != set(base_industries):
        raise ValueError("Industry-index reference does not cover the complete base scope")
    catalogue: dict[str, dict[str, Any]] = {}
    for index in document["indices"]:
        index_id = index["index_id"]
        if not isinstance(index_id, str) or not index_id or index_id in catalogue:
            raise ValueError("Invalid or duplicate index identity")
        if index.get("market") not in {"US", "KR"} or not index.get("symbol"):
            raise ValueError("Index has no supported market/symbol identity")
        if index.get("capability") not in {"daily_history", "snapshot_only"}:
            raise ValueError("Index has no verified acquisition capability")
        catalogue[index_id] = index
    selected: dict[str, dict[str, Any]] = {}
    fetched_at = int(datetime.now(UTC).timestamp() * 1000)
    mappings = []
    for industry in industries:
        code = industry["sw_code"]
        if industry.get("sw_name") != base_industries[code]["sw_name"]:
            raise ValueError("Industry name differs from the sourced base")
        for market in ("US", "KR"):
            market_reference = industry["markets"][market]
            base_market = base_industries[code][market]
            board_ids = _linked_ids(base_market, {"board_id", "board_ids"})
            boards = [base_boards[item] for item in sorted(board_ids) if item in base_boards]
            source_ids = _linked_ids(
                [base_market, boards], {"source_id", "source_ids", "reviewed_catalog_source_id"}
            )
            taxonomy_source = base.get("taxonomy", {}).get("source_id")
            if taxonomy_source:
                source_ids.add(taxonomy_source)
            matched = []
            for match in market_reference.get("matches", []):
                index = catalogue.get(match["index_id"])
                if index is None or index["market"] != market:
                    raise ValueError("Industry match has no index in the same market")
                selected[index["index_id"]] = index
                matched.append(index)
            mappings.append(
                {
                    "provider": "yahoo",
                    "market": market,
                    "sw_code": code,
                    "reference_at": reference_at,
                    "mapping_json": {
                        "schema_version": 1,
                        "checked_at": document["checked_at"],
                        "base_reference_sha256": base_sha,
                        "sw_code": code,
                        "sw_name": industry["sw_name"],
                        "market": market,
                        "mapping": market_reference,
                        "indices": matched,
                        "base_market": base_market,
                        "base_boards": boards,
                        "base_sources": [
                            base_sources[item]
                            for item in sorted(source_ids)
                            if item in base_sources
                        ],
                    },
                    "fetched_at": fetched_at,
                }
            )
    return {
        "reference_sha256": hashlib.sha256(raw).hexdigest(),
        "base_reference_sha256": base_sha,
        "reference_at": reference_at,
        "indices": list(selected.values()),
        "mappings": mappings,
    }


class CrossMarketIngestor:
    """Independent index jobs; a failed window remains pending across process restarts."""

    def __init__(
        self,
        *,
        reference_path: str | Path,
        base_reference_path: str | Path,
        state_dir: str | Path,
        yahoo: Any,
        store: Any,
        concurrency: int = 4,
    ):
        if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency <= 0:
            raise ValueError("concurrency must be a positive integer")
        self.reference_path = Path(reference_path)
        self.base_reference_path = Path(base_reference_path)
        self.state_dir = Path(state_dir)
        self.yahoo = yahoo
        self.store = store
        self.concurrency = concurrency

    def _paths(self, index: Mapping[str, Any]) -> tuple[Path, Path]:
        identity = json.dumps(
            [index["index_id"], index["market"], index["symbol"]], ensure_ascii=False
        )
        key = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        return self.state_dir / f"{key}.state.json", self.state_dir / f"{key}.pending.json"

    @staticmethod
    def _identity(index: Mapping[str, Any]) -> dict[str, str]:
        return {field: index[field] for field in ("index_id", "market", "symbol")}

    def _state(self, index: Mapping[str, Any], path: Path) -> dict[str, Any]:
        if not path.exists():
            return {
                "identity": self._identity(index),
                "capability": index["capability"],
                "last_verified_ms": None,
                "gaps": [],
            }
        state = _read_json(path)
        identity = state.get("identity", {})
        if {field: identity.get(field) for field in self._identity(index)} != self._identity(index):
            raise ValueError("Persisted index identity differs from the reference")
        state["capability"] = state.get(
            "capability", identity.get("capability", index["capability"])
        )
        if state.get("last_verified_ms") is not None:
            _milliseconds(state["last_verified_ms"], "last_verified_ms")
        if not isinstance(state.get("gaps"), list):
            raise ValueError("Invalid persisted gap list")
        for stamp in state["gaps"]:
            _milliseconds(stamp, "gap timestamp")
        return state

    @staticmethod
    def _source_window(index: Mapping[str, Any], fetched: Mapping[str, Any]) -> dict[str, Any]:
        points = fetched.get("points")
        if not isinstance(points, list) or not points:
            raise ValueError("Yahoo returned no source points")
        raw = fetched.get("raw_json")
        if not isinstance(raw, str):
            raise ValueError("Yahoo result has no original raw JSON to persist")
        source_sha = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        if fetched.get("payload_sha256") != source_sha:
            raise ValueError("Original Yahoo payload SHA256 does not match")
        json.loads(raw)
        metadata = fetched.get("metadata")
        if not isinstance(metadata, dict):
            raise ValueError("Source window has no index metadata")
        if metadata.get("symbol") != index["symbol"] or metadata.get("instrumentType") != "INDEX":
            raise ValueError("Source instrument identity differs from the mapped index")
        aliases = index.get("yahoo_names")
        if (
            not isinstance(aliases, list)
            or not aliases
            or not all(isinstance(v, str) for v in aliases)
        ):
            raise ValueError("Index reference has no verified Yahoo names")
        names = [metadata[field] for field in ("shortName", "longName") if metadata.get(field)]
        if not names or any(name not in aliases for name in names):
            raise ValueError("Source index name differs from verified Yahoo aliases")
        for field, source_field in (
            ("currency", "currency"),
            ("exchange_timezone", "exchangeTimezoneName"),
        ):
            expected = index.get(field)
            if (
                not isinstance(expected, str)
                or not expected
                or metadata.get(source_field) != expected
            ):
                raise ValueError("Source currency/timezone differs from the mapped index")
        for point in points:
            if (
                point.get("provider") != "yahoo"
                or point.get("market") != index["market"]
                or point.get("symbol") != index["symbol"]
                or point.get("currency") != index["currency"]
                or point.get("exchange_timezone") != index["exchange_timezone"]
            ):
                raise ValueError("Source point differs from the mapped index")
            _milliseconds(point.get("ts"), "source timestamp")
        return dict(fetched)

    async def _apply_window(
        self, index: dict[str, Any], window: dict[str, Any], state_path: Path
    ) -> dict[str, Any]:
        source = self._source_window(index, window["source"])
        points = source["points"]
        await self.store.upsert_prices(points)
        await self.store.verify_prices(points)
        previous = self._state(index, state_path)
        gaps = set(previous["gaps"])
        for point in points:
            if point.get("close") is None:
                gaps.add(point["ts"])
            else:
                gaps.discard(point["ts"])
        missing = source.get("missing_close_timestamps", [])
        for stamp in missing:
            gaps.add(_milliseconds(stamp, "missing close timestamp"))
        latest = max(point["ts"] for point in points)
        last = (
            previous.get("last_verified_ms")
            if previous["capability"] == index["capability"]
            else None
        )
        state = {
            "identity": self._identity(index),
            "capability": index["capability"],
            "last_verified_ms": latest if last is None else max(last, latest),
            "gaps": sorted(gaps),
            "source_sha256": source["payload_sha256"],
            "source_fetched_at": source.get("fetched_at"),
            "verified_at": int(datetime.now(UTC).timestamp() * 1000),
        }
        _atomic_json(state_path, state)
        return state

    async def _collect_index(self, index: dict[str, Any]) -> dict[str, Any]:
        state_path, pending_path = self._paths(index)
        stage = "pending_replay"
        replayed = 0
        try:
            if pending_path.exists():
                pending = _read_json(pending_path)
                identity = pending.get("identity", {})
                if {
                    field: identity.get(field) for field in self._identity(index)
                } != self._identity(index):
                    raise ValueError("Pending window identity differs from the reference")
                pending_index = pending.get("index") or {
                    **index,
                    "capability": identity.get("capability", index["capability"]),
                }
                if self._identity(pending_index) != self._identity(index):
                    raise ValueError("Pending reference entity differs from the current index")
                await self._apply_window(pending_index, pending, state_path)
                replayed = len(pending["source"]["points"])
                pending_path.unlink()
            previous = self._state(index, state_path)
            last = (
                previous.get("last_verified_ms")
                if previous["capability"] == index["capability"]
                else None
            )
            start_ms = None if last is None else last - DAY_MS
            if previous["gaps"] and previous["capability"] == index["capability"]:
                earliest_gap = min(previous["gaps"])
                start_ms = earliest_gap if start_ms is None else min(start_ms, earliest_gap)
            # Source endpoints accept seconds; storage and durable checkpoints use ms.
            start = None if start_ms is None else max(0, start_ms // 1000)
            stage = "fetch"
            fetched = await self.yahoo.fetch(
                index["symbol"], index["market"], start=start, capability=index["capability"]
            )
            source = self._source_window(index, fetched)
            pending = {
                "identity": self._identity(index),
                "index": index,
                "start_seconds": start,
                "source": source,
            }
            stage = "persist_pending"
            _atomic_json(pending_path, pending)
            stage = "write_and_verify"
            state = await self._apply_window(index, pending, state_path)
            pending_path.unlink()
            return {
                "index_id": index["index_id"],
                "symbol": index["symbol"],
                "market": index["market"],
                "status": "verified_with_gaps" if state["gaps"] else "verified",
                "rows": len(source["points"]),
                "replayed_rows": replayed,
                "last_verified_ms": state["last_verified_ms"],
                "gap_timestamps": state["gaps"],
                "source_sha256": state["source_sha256"],
            }
        except Exception as exc:
            # Transport exceptions can contain proxy credentials. Emit type/stage,
            # never the proxy URL, arbitrary response text, or exception repr.
            return {
                "index_id": index["index_id"],
                "symbol": index["symbol"],
                "market": index["market"],
                "status": "failed",
                "stage": stage,
                **_failure_details(exc),
                "pending_retained": pending_path.exists(),
                "replayed_rows": replayed,
            }

    def _mapping_state_matches(self, reference: dict[str, Any]) -> bool:
        state_path = self.state_dir / "mappings.state.json"
        try:
            state = _read_json(state_path)
        except (OSError, ValueError):
            return False
        return state.get("reference_sha256") == reference["reference_sha256"] and state.get(
            "rows"
        ) == len(reference["mappings"])

    async def _collect_mappings(self, reference: dict[str, Any]) -> dict[str, Any]:
        pending_path = self.state_dir / "mappings.pending.json"
        state_path = self.state_dir / "mappings.state.json"
        if pending_path.exists():
            pending = _read_json(pending_path)
            await self.store.upsert_industry_indices(pending["rows"])
            await self.store.verify_industry_indices(pending["rows"])
            _atomic_json(
                state_path,
                {"reference_sha256": pending["reference_sha256"], "rows": len(pending["rows"])},
            )
            pending_path.unlink()
            if pending["reference_sha256"] == reference["reference_sha256"]:
                return {"status": "verified", "rows": len(pending["rows"])}
        if self._mapping_state_matches(reference):
            # Static mappings already have exact readback evidence. Do not
            # rewrite their complete JSON rows merely to change fetched_at.
            return {"status": "previously_verified", "rows": len(reference["mappings"])}
        pending = {"reference_sha256": reference["reference_sha256"], "rows": reference["mappings"]}
        _atomic_json(pending_path, pending)
        await self.store.upsert_industry_indices(pending["rows"])
        await self.store.verify_industry_indices(pending["rows"])
        _atomic_json(
            state_path,
            {"reference_sha256": reference["reference_sha256"], "rows": len(pending["rows"])},
        )
        pending_path.unlink()
        return {"status": "verified", "rows": len(pending["rows"])}

    async def run_once(self) -> dict[str, Any]:
        reference = load_reference(self.reference_path, self.base_reference_path)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        mapping_pending = self.state_dir / "mappings.pending.json"
        if not mapping_pending.exists() and not self._mapping_state_matches(reference):
            _atomic_json(
                mapping_pending,
                {"reference_sha256": reference["reference_sha256"], "rows": reference["mappings"]},
            )
        await self.store.ensure_schema()
        mappings_task = asyncio.create_task(self._collect_mappings(reference))
        semaphore = asyncio.Semaphore(self.concurrency)

        async def collect(index: dict[str, Any]) -> dict[str, Any]:
            async with semaphore:
                return await self._collect_index(index)

        # References and separate index series can persist independently.
        results = await asyncio.gather(*(collect(index) for index in reference["indices"]))
        try:
            mappings = await mappings_task
        except Exception as exc:
            mappings = {"status": "failed", **_failure_details(exc)}
        failed = mappings["status"] == "failed" or any(r["status"] == "failed" for r in results)
        return {
            "status": "partial_failure" if failed else "verified",
            "reference_sha256": reference["reference_sha256"],
            "mapping": mappings,
            "referenced_indices": len(reference["indices"]),
            "indices": results,
        }
