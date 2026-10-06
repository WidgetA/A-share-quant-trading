"""Exact-identity Massive index aggregates; one durable, rate-limited queue."""

import asyncio
import hashlib
import json
import math
import time
from collections.abc import Mapping
from datetime import date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx

from src.data.cross_market_ingest import _atomic_json, _read_json

API_ROOT = "https://api.massive.com"
BASE_LIMIT = 50_000
STEP_MS = 300_000
RATE_SECONDS = 13


class MassiveIndexError(RuntimeError):
    """A classified failure with a credential-free source receipt."""

    def __init__(self, classification: str, *, evidence=None):
        super().__init__(classification)
        self.classification = classification
        self.evidence = evidence


def _date(value: str) -> date:
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("Expected an ISO date")
    return parsed


def validate_index(index: Mapping[str, Any]) -> None:
    """Only the reviewed original price index can supply its existing symbol."""
    record = index.get("vendor_record")
    if (
        index.get("market") != "US"
        or index.get("instrument_type") != "INDEX"
        or index.get("same_index_status") != "verified"
        or index.get("return_basis_status") != "verified"
        or index.get("return_basis") != "price_return"
        or index.get("fullname_match") is not True
        or not isinstance(record, dict)
        or record.get("market") != "indices"
        or record.get("locale") != "us"
        or record.get("source_feed") != "NasdaqGIDS"
        or record.get("ticker") != index.get("vendor_ticker")
        or record.get("name") != index.get("official_name")
        or not isinstance(record.get("ticker"), str)
        or not record["ticker"].startswith("I:")
        or record["ticker"][2:] != index.get("native_symbol")
        or index.get("currency") != "USD"
        or index.get("exchange_timezone") != "America/New_York"
        or not isinstance(index.get("symbol"), str)
        or index.get("index_id") != "US:YAHOO:" + index["symbol"]
    ):
        raise MassiveIndexError("unverified_original_index_identity")


def _integer(value, label):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MassiveIndexError("invalid_" + label)
    return value


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MassiveIndexError("invalid_ohlc")
    result = float(value)
    if not math.isfinite(result):
        raise MassiveIndexError("invalid_ohlc")
    return result


def parse_aggregates(
    payload: Mapping[str, Any],
    index: Mapping[str, Any],
    *,
    start_date: str,
    end_date: str,
    fetched_at: int,
) -> dict[str, Any]:
    """Parse source values, preserving unavailable volume/finality as NULL."""
    validate_index(index)
    first, last = _date(start_date), _date(end_date)
    if last < first:
        raise MassiveIndexError("invalid_window")
    if not isinstance(payload, dict) or payload.get("ticker") != index["vendor_ticker"]:
        raise MassiveIndexError("source_identity_mismatch")
    if payload.get("status") != "OK" or payload.get("error"):
        raise MassiveIndexError("source_error")
    count = _integer(payload.get("queryCount"), "query_count")
    bars = payload.get("results", [])
    if not isinstance(bars, list):
        raise MassiveIndexError("invalid_results")
    reported = payload.get("resultsCount", payload.get("count", len(bars)))
    if _integer(reported, "result_count") != len(bars):
        raise MassiveIndexError("result_count_mismatch")
    truncated = count >= BASE_LIMIT or bool(payload.get("next_url"))
    if not bars:
        if count or truncated:
            raise MassiveIndexError("inconsistent_empty_source")
        return {
            "points": [],
            "query_count": count,
            "truncated": False,
            "source_empty": True,
            "source_first_date": None,
            "source_last_date": None,
        }
    if not count:
        raise MassiveIndexError("inconsistent_query_count")
    points = []
    previous = None
    timezone = ZoneInfo(index["exchange_timezone"])
    for bar in bars:
        if not isinstance(bar, dict):
            raise MassiveIndexError("invalid_bar")
        ts = _integer(bar.get("t"), "timestamp")
        when = datetime.fromtimestamp(ts / 1000, timezone)
        if ts % STEP_MS or not first <= when.date() <= last:
            raise MassiveIndexError("timestamp_outside_5min_window")
        if previous is not None:
            if ts <= previous[0]:
                raise MassiveIndexError("nonascending_timestamp")
        previous = ts, when.date()
        o, h, low, c = (_number(bar.get(k)) for k in ("o", "h", "l", "c"))
        if not low <= min(o, c) <= max(o, c) <= h:
            raise MassiveIndexError("inconsistent_ohlc")
        volume = None if bar.get("v") is None else _number(bar["v"])
        if volume is not None and volume < 0:
            raise MassiveIndexError("invalid_volume")
        points.append(
            {
                "provider": "massive",
                "market": index["market"],
                "symbol": index["symbol"],
                "interval": "5m",
                "data_kind": "minute_bar",
                "ts": ts,
                "name": index["name"],
                "currency": index["currency"],
                "exchange_timezone": index["exchange_timezone"],
                "trade_date": when.date().isoformat(),
                "open": o,
                "high": h,
                "low": low,
                "close": c,
                "adjusted_close": None,
                "volume": volume,
                "is_final": None,
                "fetched_at": _integer(fetched_at, "fetched_at"),
            }
        )
    return {
        "points": points,
        "query_count": count,
        "truncated": truncated,
        "source_empty": False,
        "source_first_date": points[0]["trade_date"],
        "source_last_date": points[-1]["trade_date"],
    }


def read_key_file(path: Path, variable: str = "MASSIVE_API_KEY") -> str:
    """Read only the supplied dotenv, without importing keys into the environment."""
    from dotenv import dotenv_values

    key = dotenv_values(path, interpolate=False).get(variable)
    if not isinstance(key, str) or not key.strip() or "\n" in key or "\r" in key:
        raise MassiveIndexError("missing_api_key")
    return key.strip()


def _redact(value, key):
    if isinstance(value, str):
        return value.replace(key, "[REDACTED]")
    if isinstance(value, list):
        return [_redact(x, key) for x in value]
    if isinstance(value, dict):
        return {_redact(k, key): _redact(v, key) for k, v in value.items()}
    return value


class MassiveIndexClient:
    """One in-process queue; the producer's process lock makes it one writer."""

    def __init__(
        self,
        *,
        key_file: Path,
        rate_state_path: Path,
        key_variable="MASSIVE_API_KEY",
        proxy=None,
        client=None,
        clock=time.time,
        sleep=asyncio.sleep,
    ):
        self._key = read_key_file(Path(key_file), key_variable)
        self.rate_state_path = Path(rate_state_path)
        self._client = client or httpx.AsyncClient(
            proxy=proxy,
            trust_env=False,
            timeout=60,
            follow_redirects=False,
        )
        self._owns_client = client is None
        self._clock, self._sleep = clock, sleep
        self._lock = asyncio.Lock()

    async def aclose(self):
        if self._owns_client:
            await self._client.aclose()

    async def _reserve(self):
        previous = _read_json(self.rate_state_path) if self.rate_state_path.exists() else {}
        due = previous.get("next_allowed_at", 0)
        if not isinstance(due, (int, float)) or not math.isfinite(due):
            raise MassiveIndexError("invalid_rate_checkpoint")
        while self._clock() < due:
            await self._sleep(due - self._clock())
        # Persist before sending, including for attempts that crash or fail.
        _atomic_json(self.rate_state_path, {"next_allowed_at": self._clock() + RATE_SECONDS})

    def _cooldown(self, header):
        delay = 60.0
        if header:
            try:
                parsed = float(header)
                if math.isfinite(parsed):
                    delay = max(0, parsed)
            except (TypeError, ValueError):
                try:
                    delay = max(0, parsedate_to_datetime(header).timestamp() - self._clock())
                except (ValueError, TypeError, OverflowError):
                    pass
        previous = _read_json(self.rate_state_path)
        _atomic_json(
            self.rate_state_path,
            {
                "next_allowed_at": max(previous["next_allowed_at"], self._clock() + delay),
            },
        )

    async def fetch(self, index, *, start_date: str, end_date: str):
        validate_index(index)
        if _date(start_date) > _date(end_date):
            raise MassiveIndexError("invalid_window")
        url = (
            f"{API_ROOT}/v2/aggs/ticker/{quote(index['vendor_ticker'], safe='')}/"
            f"range/5/minute/{start_date}/{end_date}"
        )
        params = {"sort": "asc", "limit": BASE_LIMIT}
        async with self._lock:
            await self._reserve()
            try:
                response = await self._client.get(
                    url,
                    params=params,
                    headers={"Authorization": "Bearer " + self._key},
                )
            except httpx.HTTPError:
                raise MassiveIndexError("transport_failure") from None
            raw = bytes(response.content)
            raw_text = raw.decode("utf-8", errors="replace")
            # Preserve original bytes through UTF-8 only when valid. Invalid source
            # encoding is a failure, rather than a replacement-character success.
            valid_utf8 = raw_text.encode("utf-8") == raw
            safe_raw = raw_text.replace(self._key, "[REDACTED]")
            evidence = {
                "schema_version": 1,
                "index_id": index["index_id"],
                "symbol": index["symbol"],
                "market": index["market"],
                "vendor_ticker": index["vendor_ticker"],
                "interval": "5m",
                "start_date": start_date,
                "end_date": end_date,
                "fetched_at": int(self._clock() * 1000),
                "http_status": response.status_code,
                "request_url": url,
                "request_params": params,
                "raw_json": safe_raw,
                "payload_sha256": hashlib.sha256(safe_raw.encode("utf-8")).hexdigest(),
                "source_payload_sha256": hashlib.sha256(raw).hexdigest(),
                "credential_redacted": safe_raw != raw_text,
                "retry_after": _redact(response.headers.get("Retry-After"), self._key),
            }
            classifications = {
                401: "authentication_denied",
                403: "permission_denied",
                404: "source_not_found",
                429: "rate_limited",
            }
            if response.status_code == 429:
                self._cooldown(response.headers.get("Retry-After"))
            if response.status_code != 200:
                raise MassiveIndexError(
                    classifications.get(response.status_code, "http_failure"),
                    evidence=evidence,
                )
            if not valid_utf8:
                raise MassiveIndexError("invalid_source_encoding", evidence=evidence)
            try:
                payload = json.loads(safe_raw)
            except ValueError:
                raise MassiveIndexError("invalid_source_json", evidence=evidence) from None
            try:
                parsed = parse_aggregates(
                    payload,
                    index,
                    start_date=start_date,
                    end_date=end_date,
                    fetched_at=evidence["fetched_at"],
                )
            except MassiveIndexError as exc:
                exc.evidence = evidence
                raise
            return {**evidence, **parsed}
