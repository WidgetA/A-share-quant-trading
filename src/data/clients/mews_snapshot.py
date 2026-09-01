"""Read the published MEWS result without importing its storage backend.

The MEWS calculation service owns its historical materialization.  V20 only
reads the already-published result after 09:10, validates its availability
boundary, and persists a small immutable snapshot in its own PostgreSQL
ledger.  Nothing in this module knows which storage backend the upstream
service uses.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from collections.abc import Mapping
from datetime import date, datetime, time
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx

SHANGHAI = ZoneInfo("Asia/Shanghai")
MEWS_MODEL_VERSION = "mews_v2"
MEWS_PUBLISH_TIME = time(9, 10)
MEWS_FETCH_TIMEOUT_SECONDS = 8.0
MEWS_REFRESH_TIMEOUT_SECONDS = 120.0


class MewsSnapshotSourceError(RuntimeError):
    """The published MEWS result is missing, stale, or malformed."""


class MewsSnapshotNotReady(MewsSnapshotSourceError):
    """The source is healthy enough to request its production backfill."""


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _parse_day(value: Any, field_name: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise MewsSnapshotSourceError(f"MEWS {field_name} is invalid") from exc


def _generated_at(value: Any) -> datetime:
    if isinstance(value, bool):
        raise MewsSnapshotSourceError("MEWS updated_at is invalid")
    try:
        # The production curve endpoint exposes a millisecond update receipt.
        # Converting that receipt, rather than using V20's pull time,
        # keeps retries and process restarts idempotent.
        parsed = datetime.fromtimestamp(float(value) / 1000.0, tz=SHANGHAI)
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise MewsSnapshotSourceError("MEWS updated_at is invalid") from exc
    return parsed


def _finite(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise MewsSnapshotSourceError(f"MEWS {field_name} is invalid")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise MewsSnapshotSourceError(f"MEWS {field_name} is invalid") from exc
    if not math.isfinite(parsed):
        raise MewsSnapshotSourceError(f"MEWS {field_name} is invalid")
    return parsed


class PublishedMewsSnapshotClient:
    """Fetch and normalize one published MEWS v2 observation."""

    def __init__(
        self,
        url: str,
        *,
        api_key: str,
        refresh_url: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("V20_MEWS_SOURCE_URL must be an absolute HTTP(S) URL")
        if not api_key.strip():
            raise ValueError("V20_MEWS_API_KEY (or TRADING_API_KEY) is required")
        if refresh_url is None:
            if not parsed.path.endswith("/margin-risk-curve"):
                raise ValueError(
                    "V20_MEWS_REFRESH_URL is required when V20_MEWS_SOURCE_URL "
                    "does not end with /margin-risk-curve"
                )
            refresh_url = parsed._replace(
                path=f"{parsed.path.removesuffix('/margin-risk-curve')}/margin-risk-refresh",
                query="",
                fragment="",
            ).geturl()
        refresh_parsed = urlparse(refresh_url)
        if refresh_parsed.scheme not in {"http", "https"} or not refresh_parsed.netloc:
            raise ValueError("V20_MEWS_REFRESH_URL must be an absolute HTTP(S) URL")
        self._url = url
        self._refresh_url = refresh_url
        self._headers = {"X-API-Key": api_key.strip()}
        self._transport = transport

    @classmethod
    def from_environment(cls) -> "PublishedMewsSnapshotClient":
        url = os.getenv("V20_MEWS_SOURCE_URL", "").strip()
        if not url:
            raise ValueError(
                "V20_MEWS_SOURCE_URL is required; V20 must pull the published "
                "MEWS result before the 09:40 cutoff"
            )
        api_key = (
            os.getenv("V20_MEWS_API_KEY", "").strip() or os.getenv("TRADING_API_KEY", "").strip()
        )
        refresh_url = os.getenv("V20_MEWS_REFRESH_URL", "").strip() or None
        return cls(url, api_key=api_key, refresh_url=refresh_url)

    async def refresh_missing_snapshot(self) -> None:
        """Ask the owning MEWS service to backfill through its publication boundary."""

        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(MEWS_REFRESH_TIMEOUT_SECONDS),
                transport=self._transport,
            ) as client:
                response = await client.post(self._refresh_url, headers=self._headers)
                if response.status_code in {409, 503}:
                    raise MewsSnapshotNotReady(
                        f"MEWS production refresh unavailable: HTTP {response.status_code}"
                    )
                response.raise_for_status()
                document = response.json()
        except MewsSnapshotNotReady:
            raise
        except (httpx.HTTPError, ValueError) as exc:
            raise MewsSnapshotSourceError(
                f"MEWS production refresh failed: {type(exc).__name__}"
            ) from None
        if not isinstance(document, Mapping):
            raise MewsSnapshotSourceError("MEWS refresh response must be an object")
        result = document.get("result")
        if (
            document.get("success") is not True
            or not isinstance(result, Mapping)
            or result.get("status") != "OK"
        ):
            raise MewsSnapshotSourceError("MEWS refresh response is invalid")

    async def fetch_snapshot(
        self,
        *,
        source_trade_date: date,
        availability_date: date,
    ) -> Mapping[str, Any]:
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(MEWS_FETCH_TIMEOUT_SECONDS),
                transport=self._transport,
            ) as client:
                response = await client.get(self._url, headers=self._headers)
                response.raise_for_status()
                document = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise MewsSnapshotSourceError(
                f"published MEWS request failed: {type(exc).__name__}"
            ) from None
        if not isinstance(document, Mapping):
            raise MewsSnapshotSourceError("published MEWS response must be an object")
        if document.get("version") != MEWS_MODEL_VERSION:
            raise MewsSnapshotSourceError("published MEWS version is not mews_v2")
        point = document.get("latest_valid")
        if not isinstance(point, Mapping):
            raise MewsSnapshotNotReady("published MEWS latest_valid is unavailable")

        actual_source_date = _parse_day(point.get("date"), "source trade date")
        actual_availability_date = _parse_day(
            point.get("signal_available_date"),
            "signal availability date",
        )
        if actual_source_date != source_trade_date:
            raise MewsSnapshotNotReady(
                "published MEWS is stale: "
                f"expected source {source_trade_date}, got {actual_source_date}"
            )
        if actual_availability_date != availability_date:
            raise MewsSnapshotNotReady("published MEWS is not available for today's session")
        if point.get("data_status") != "OK":
            raise MewsSnapshotNotReady("published MEWS data_status is not OK")

        risk_state = str(point.get("risk_state") or "")
        if risk_state not in {"NORMAL", "WATCH", "WARNING", "DANGER"}:
            raise MewsSnapshotSourceError("published MEWS risk_state is invalid")
        generated_at = _generated_at(point.get("updated_at"))
        publish_boundary = datetime.combine(
            availability_date,
            MEWS_PUBLISH_TIME,
            tzinfo=SHANGHAI,
        )
        if generated_at < publish_boundary or generated_at.date() != availability_date:
            raise MewsSnapshotNotReady(
                "published MEWS was not generated after today's 09:10 release"
            )

        evidence = {
            "profile": "PUBLISHED_MEWS_V2_0910_CACHE_V1",
            "source_trade_date": actual_source_date.isoformat(),
            "signal_available_date": actual_availability_date.isoformat(),
            "upstream_updated_at": int(point["updated_at"]),
            "risk_state": risk_state,
            "data_status": point["data_status"],
            "mews": _finite(point.get("mews"), "score"),
            "exhaustion_path": _finite(
                point.get("exhaustion_path"),
                "exhaustion_path",
            ),
            "persistent_deleveraging_path": _finite(
                point.get("persistent_deleveraging_path"),
                "persistent_deleveraging_path",
            ),
        }
        data_version = hashlib.sha256(_canonical_json(evidence).encode("utf-8")).hexdigest()
        return {
            "snapshot_id": f"mews-v2-{source_trade_date.isoformat()}-{data_version[:16]}",
            "source_trade_date": source_trade_date.isoformat(),
            "generated_at": generated_at.isoformat(),
            "fast_state": "DANGER" if risk_state == "DANGER" else "NORMAL",
            "model_version": MEWS_MODEL_VERSION,
            "data_version": data_version,
            "evidence": evidence,
        }


__all__ = [
    "MEWS_FETCH_TIMEOUT_SECONDS",
    "MEWS_MODEL_VERSION",
    "MEWS_PUBLISH_TIME",
    "MEWS_REFRESH_TIMEOUT_SECONDS",
    "MewsSnapshotNotReady",
    "MewsSnapshotSourceError",
    "PublishedMewsSnapshotClient",
]
