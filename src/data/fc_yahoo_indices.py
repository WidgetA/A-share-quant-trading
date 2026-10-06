"""Fetch complete Yahoo source windows through the dedicated, signed US FC function."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import math
import os
import time
import uuid
from collections.abc import Callable, Mapping
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit

import httpx

from src.data.yahoo_indices import YahooIndexError, parse_chart

FC_FUNCTION_NAME = "ashare_yahoo_indices_v15"
FC_REGION = "us-west-1"


class FCYahooIndexError(YahooIndexError):
    """An invocation or its source envelope did not establish a usable source window."""

    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class _InvocationFailure(FCYahooIndexError):
    def __init__(self, status_code: int, headers: Mapping[str, str], *, function_error=False):
        super().__init__(
            "FC function failed" if function_error else "FC HTTP failed", status_code=status_code
        )
        self.headers = headers


def _endpoint(value: str) -> str:
    if not isinstance(value, str) or not value or any(char.isspace() for char in value):
        raise ValueError("FC endpoint must be a hostname or HTTPS origin")
    parsed = urlsplit(value if "://" in value else "https://" + value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("FC endpoint must be a hostname or HTTPS origin")
    return parsed.netloc


def _sdk_invoke(endpoint: str, function_name: str, region: str, client: Any = None):
    # These imports stay off the local Yahoo path. SDK 4.8.2 returns BinaryIO.
    from alibabacloud_fc20230330 import models
    from alibabacloud_fc20230330.client import Client
    from alibabacloud_tea_openapi.utils_models import Config as DarabonbaConfig
    from darabonba.policy.retry import RetryOptions
    from darabonba.runtime import RuntimeOptions

    if client is None:
        key_id = os.environ.get("ALIYUN_ACCESS_KEY_ID")
        secret = os.environ.get("ALIYUN_ACCESS_KEY_SECRET")
        if not key_id or not secret:
            raise ValueError("FC access credentials are not configured")
        client = Client(
            DarabonbaConfig(
                access_key_id=key_id,
                access_key_secret=secret,
                security_token=os.environ.get("ALIYUN_SECURITY_TOKEN") or None,
                endpoint=endpoint,
                region_id=region,
                protocol="HTTPS",
                retry_options=RetryOptions(retryable=False, max_attempts=1),
            )
        )

    def invoke(payload: dict[str, Any]):
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        with io.BytesIO(body) as stream:
            return client.invoke_function_with_options(
                function_name,
                models.InvokeFunctionRequest(body=stream),
                models.InvokeFunctionHeaders(x_fc_invocation_type="Sync"),
                RuntimeOptions(autoretry=False, connect_timeout=10_000, read_timeout=600_000),
            )

    return invoke


def _field(response: Any, name: str, alias: str | None = None) -> Any:
    if isinstance(response, Mapping):
        return response.get(name, response.get(alias) if alias else None)
    return getattr(response, name, None)


def _read_response(response: Any) -> bytes:
    body = _field(response, "body")
    try:
        status = _field(response, "status_code", "statusCode")
        headers = _field(response, "headers") or {}
        if type(status) is not int or not isinstance(headers, Mapping):
            raise FCYahooIndexError("FC response has no valid HTTP status/headers")
        lowered = {str(key).lower(): value for key, value in headers.items()}
        function_error = bool(lowered.get("x-fc-error") or lowered.get("x-fc-error-type"))
        if status != 200 or function_error:
            raise _InvocationFailure(status, lowered, function_error=function_error)
        if isinstance(body, str):
            return body.encode("utf-8")
        if isinstance(body, bytes):
            return body
        if not callable(getattr(body, "read", None)):
            raise FCYahooIndexError("FC response has no readable body")
        chunks = []
        while chunk := body.read(65_536):
            if not isinstance(chunk, bytes):
                raise FCYahooIndexError("FC stream returned non-byte content")
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        if callable(getattr(body, "close", None)):
            body.close()


def _root_error(exc: Exception) -> Exception:
    seen = set()
    while id(exc) not in seen:
        seen.add(id(exc))
        if _status(exc) is not None:
            break
        inner = getattr(exc, "inner_exception", None)
        if (
            not isinstance(inner, Exception)
            and type(exc).__module__ == "darabonba.exceptions"
            and type(exc).__name__ == "RetryError"
        ):
            context = exc.__context__
            # SDK 1.0.9 wraps requests IOError in RetryError but retains its
            # real exception here. Only that proven transport type is usable.
            if isinstance(context, Exception) and _transient(context):
                inner = context
        if not isinstance(inner, Exception):
            break
        exc = inner
    return exc


def _status(exc: Exception) -> int | None:
    for name in ("status_code", "statusCode"):
        value = getattr(exc, name, None)
        if type(value) is int:
            return value
    data = getattr(exc, "data", None)
    value = data.get("statusCode") if isinstance(data, Mapping) else None
    return value if type(value) is int else None


def _transient(exc: Exception) -> bool:
    status = _status(exc)
    if status is not None:
        return status == 429 or 500 <= status < 600
    if isinstance(exc, (TimeoutError, ConnectionError, httpx.TransportError)):
        return True
    # Official SDK's synchronous transport uses requests. Load only if required.
    try:
        from requests.exceptions import ChunkedEncodingError
        from requests.exceptions import ConnectionError as RequestsConnectionError
        from requests.exceptions import Timeout as RequestsTimeout
    except ImportError:
        return False
    return isinstance(exc, (RequestsConnectionError, RequestsTimeout, ChunkedEncodingError))


def _retry_after(exc: Exception, default: float) -> float:
    value = getattr(exc, "retry_after", None)
    headers = getattr(exc, "headers", {})
    if value is None and isinstance(headers, Mapping):
        value = next((v for k, v in headers.items() if str(k).lower() == "retry-after"), None)
    try:
        delay = float(value)
        return max(1, delay) if math.isfinite(delay) else default
    except (TypeError, ValueError):
        try:
            return max(1, parsedate_to_datetime(value).timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return default


class FCYahooIndexClient:
    """A fetch-compatible signed FC client; ingestion/checkpoints remain domestic.

    ``invoke`` may inject a synchronous invocation returning the official response
    shape. The SDK call and entire response stream are read in a worker thread.
    """

    def __init__(
        self,
        endpoint: str,
        function_name: str = FC_FUNCTION_NAME,
        *,
        region: str = FC_REGION,
        invoke: Callable[[dict[str, Any]], Any] | None = None,
        client: Any = None,
        max_attempts: int = 4,
    ):
        endpoint = _endpoint(endpoint)
        if function_name != FC_FUNCTION_NAME or region != FC_REGION:
            raise ValueError("FC target does not match the dedicated US collection function")
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("FC max_attempts must be a positive integer")
        if invoke is not None and client is not None:
            raise ValueError("Supply an invocation or SDK client, not both")
        self._invoke = (
            invoke if invoke is not None else _sdk_invoke(endpoint, function_name, region, client)
        )
        self._region = region
        self._max_attempts = max_attempts
        self._cooldown_until = 0.0

    async def aclose(self) -> None:
        # Official generated client has no close API; per-call streams are closed.
        return None

    async def _wait_cooldown(self) -> None:
        while (remaining := self._cooldown_until - time.monotonic()) > 0:
            await asyncio.sleep(min(remaining, 30))

    def _decode(self, raw_response: bytes, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            response = json.loads(raw_response)
        except (ValueError, UnicodeDecodeError):
            raise FCYahooIndexError("FC returned invalid envelope JSON") from None
        if not isinstance(response, dict) or any(
            type(response.get(key)) is not type(value) or response.get(key) != value
            for key, value in payload.items()
        ):
            raise FCYahooIndexError("FC response identity differs from the requested window")
        fetched_at = response.get("fetched_at")
        raw = response.get("raw_json")
        runtime = response.get("runtime")
        if (
            type(fetched_at) is not int
            or not 0 < fetched_at < 2**63
            or not isinstance(raw, str)
            or not isinstance(runtime, dict)
            or runtime.get("region") != self._region
            or not isinstance(runtime.get("fc_request_id"), str)
            or not runtime["fc_request_id"]
            or not isinstance(response.get("request_url"), str)
            or not response["request_url"]
        ):
            raise FCYahooIndexError("FC source envelope lacks valid clocks or provenance")
        if hashlib.sha256(raw.encode("utf-8")).hexdigest() != response.get("payload_sha256"):
            raise FCYahooIndexError("FC raw source SHA256 does not match")
        try:
            source = json.loads(raw)
            if not isinstance(source, dict):
                raise ValueError("Source is not a JSON object")
            metadata, points = parse_chart(
                source,
                symbol=payload["symbol"],
                market=payload["market"],
                fetched_at=fetched_at,
                capability=payload["capability"],
            )
        except (ValueError, TypeError, AttributeError, KeyError, YahooIndexError):
            raise FCYahooIndexError("FC raw Yahoo source failed local validation") from None
        return {
            "metadata": metadata,
            "points": points,
            "missing_close_timestamps": [point["ts"] for point in points if point["close"] is None],
            "raw_json": raw,
            "payload_sha256": response["payload_sha256"],
            "url": response["request_url"],
            "fetched_at": fetched_at,
            "request_id": payload["request_id"],
            "runtime": runtime,
        }

    async def fetch(
        self,
        symbol: str,
        market: str,
        *,
        start: int | None = None,
        capability: str | None = None,
    ) -> dict[str, Any]:
        if (
            not isinstance(symbol, str)
            or not symbol
            or market not in {"US", "KR"}
            or capability not in {"daily_history", "snapshot_only"}
            or (start is not None and (type(start) is not int or start < 0))
        ):
            raise ValueError("Invalid FC source window request")
        payload = {
            "schema_version": 1,
            "request_id": uuid.uuid4().hex,
            "symbol": symbol,
            "market": market,
            "start": start,
            "capability": capability,
        }
        for attempt in range(self._max_attempts):
            await self._wait_cooldown()
            try:
                raw_response = await asyncio.to_thread(
                    lambda: _read_response(self._invoke(dict(payload)))
                )
            except Exception as exc:
                error = _root_error(exc)
                if not _transient(error) or attempt + 1 == self._max_attempts:
                    raise FCYahooIndexError(
                        "FC invocation failed", status_code=_status(error)
                    ) from None
                if _status(error) == 429:
                    delay = _retry_after(error, 30 * (attempt + 1))
                    self._cooldown_until = max(self._cooldown_until, time.monotonic() + delay)
                else:
                    await asyncio.sleep(min(2**attempt, 8))
                continue
            return self._decode(raw_response, payload)
        raise FCYahooIndexError("FC invocation did not return a source window")
