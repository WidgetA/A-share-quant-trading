"""Single-index Yahoo fetches for a native Function Compute event handler."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import threading
from collections.abc import Callable, Mapping
from typing import Any

from src.data.yahoo_indices import YahooIndexClient, YahooIndexError
from src.data.yahoo_intraday_indices import YahooIntradayIndexClient

SCHEMA_VERSION = 1
REQUEST_FIELDS = ("schema_version", "request_id", "symbol", "market", "start", "capability")


class FCYahooRequestError(ValueError):
    """The invocation does not contain one supported index request."""


def validate_request(event: bytes | bytearray | str | Mapping[str, Any]) -> dict[str, Any]:
    """Read the native Python event bytes without adding an HTTP-trigger wrapper."""
    if isinstance(event, Mapping):
        request = dict(event)
    elif isinstance(event, (bytes, bytearray, str)):
        try:
            request = json.loads(event)
        except (ValueError, UnicodeDecodeError) as exc:
            raise FCYahooRequestError("Event must contain a JSON request object") from exc
    else:
        raise FCYahooRequestError("Event must contain a JSON request object")
    if not isinstance(request, dict) or any(field not in request for field in REQUEST_FIELDS):
        raise FCYahooRequestError("Event is missing required request fields")
    if type(request["schema_version"]) is not int or request["schema_version"] != SCHEMA_VERSION:
        raise FCYahooRequestError("Unsupported request schema_version")
    for field in ("request_id", "symbol"):
        if not isinstance(request[field], str) or not request[field]:
            raise FCYahooRequestError(f"{field} must be nonempty text")
    if request["market"] not in ("US", "KR"):
        raise FCYahooRequestError("market must be US or KR")
    start = request["start"]
    if start is not None and (type(start) is not int or start < 0):
        raise FCYahooRequestError("start must be null or nonnegative integer epoch seconds")
    if request["capability"] not in (
        "daily_history",
        "snapshot_only",
        "minute_history",
        "hour_history",
    ):
        raise FCYahooRequestError("Unsupported acquisition capability")
    fields = list(REQUEST_FIELDS)
    if request["capability"] in ("minute_history", "hour_history"):
        intervals = ("1h",) if request["capability"] == "hour_history" else ("1m", "5m")
        if request.get("interval") not in intervals:
            raise FCYahooRequestError("Unsupported minute interval")
        end = request.get("end")
        if start is None or type(end) is not int or end <= start:
            raise FCYahooRequestError("Minute request needs an explicit increasing window")
        fields.extend(("interval", "end"))
    return {field: request[field] for field in fields}


async def fetch_envelope(
    request: Mapping[str, Any],
    *,
    yahoo: YahooIndexClient,
    fc_request_id: str,
    region: str | None,
) -> dict[str, Any]:
    """Fetch one requested period and return original source bytes as UTF-8 JSON."""
    request = validate_request(request)
    if request["capability"] in ("minute_history", "hour_history"):
        source = await yahoo.fetch(
            request["symbol"],
            request["market"],
            start=request["start"],
            end=request["end"],
            interval=request["interval"],
        )
    else:
        source = await yahoo.fetch(
            request["symbol"],
            request["market"],
            start=request["start"],
            capability=request["capability"],
        )
    raw = source.get("raw_json")
    digest = source.get("payload_sha256")
    if not isinstance(raw, str) or hashlib.sha256(raw.encode("utf-8")).hexdigest() != digest:
        raise YahooIndexError("Original Yahoo response SHA256 does not match")
    # The Yahoo client validates the chart and derives observations. Bind the
    # small remote response to its original instrument, without returning points.
    try:
        payload = json.loads(raw)
        chart = payload["chart"]
        results = chart["result"]
        if chart.get("error") or not isinstance(results, list) or len(results) != 1:
            raise YahooIndexError("No unique Yahoo index chart result")
        metadata = results[0]["meta"]
    except (ValueError, TypeError, KeyError) as exc:
        raise YahooIndexError("Original response has no index chart metadata") from exc
    if (
        not isinstance(metadata, dict)
        or metadata.get("symbol") != request["symbol"]
        or metadata.get("instrumentType") != "INDEX"
    ):
        raise YahooIndexError("Original response is not the requested index")
    fetched_at = source.get("fetched_at")
    if type(fetched_at) is not int or fetched_at <= 0:
        raise YahooIndexError("Source fetch time must be integer epoch milliseconds")
    request_url = source.get("url")
    if not isinstance(request_url, str) or not request_url:
        raise YahooIndexError("Source fetch has no request URL")
    return {
        **request,
        "fetched_at": fetched_at,
        "raw_json": raw,
        "payload_sha256": digest,
        "request_url": request_url,
        "runtime": {"region": region, "fc_request_id": fc_request_id},
    }


class FCYahooWorker:
    """One warm instance keeps its own event loop, HTTP pool and 429 cooldown."""

    def __init__(
        self,
        *,
        client_factory: Callable[..., YahooIndexClient] = YahooIndexClient,
        intraday_client_factory: Callable[..., YahooIntradayIndexClient] = YahooIntradayIndexClient,
    ):
        self._loop = asyncio.new_event_loop()
        self._client_factory = client_factory
        self._yahoo: YahooIndexClient | None = None
        self._intraday: YahooIntradayIndexClient | None = None
        self._intraday_client_factory = intraday_client_factory
        self._invoke_lock = threading.Lock()

    async def _fetch(self, request: Mapping[str, Any], context: Any) -> dict[str, Any]:
        if self._yahoo is None:
            # No configured or environment proxy is used for US FC egress.
            self._yahoo = self._client_factory(proxy=None)
        source_client = self._yahoo
        if request["capability"] in ("minute_history", "hour_history"):
            if self._intraday is None:
                self._intraday = self._intraday_client_factory(transport=self._yahoo)
            source_client = self._intraday
        return await fetch_envelope(
            request,
            yahoo=source_client,
            fc_request_id=context.request_id,
            region=os.environ["FC_REGION"],
        )

    def invoke(self, event: bytes | bytearray | str | Mapping[str, Any], context: Any) -> str:
        request = validate_request(event)
        with self._invoke_lock:
            envelope = self._loop.run_until_complete(self._fetch(request, context))
        return json.dumps(envelope, ensure_ascii=False, allow_nan=False, separators=(",", ":"))

    def close(self) -> None:
        """Release this instance at teardown, never between invocations."""
        with self._invoke_lock:
            if not self._loop.is_closed():
                if self._yahoo is not None:
                    self._loop.run_until_complete(self._yahoo.aclose())
                self._loop.close()
