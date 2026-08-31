"""Narrow HTTP boundary for the V20 decision-notification service.

The router deliberately exposes no account, order, holding, fill, or execution
API.  It reports service health, accepts the two external evidence records
required by the documented V20 state machine, and exposes one non-bypassable
manual scan trigger for deployment verification.
"""

from __future__ import annotations

import hmac
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from datetime import date, datetime
from typing import Any, Literal, Protocol
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.data.database.v20_repository import (
    V20LeadershipLost,
    V20RepositoryError,
    V20SemanticConflict,
    V20StateConflict,
)

logger = logging.getLogger(__name__)

_INGEST_API_KEY_ENV = "V20_INGEST_API_KEY"
_INGEST_API_KEY_HEADER = APIKeyHeader(name="X-V20-API-Key", auto_error=False)
_STATUS_API_KEY_ENV = "V20_STATUS_API_KEY"
_STATUS_API_KEY_HEADER = APIKeyHeader(name="X-V20-Status-Key", auto_error=False)


class V20RouteService(Protocol):
    """The small service surface intentionally visible to HTTP routes."""

    async def status(self) -> Any: ...

    async def ingest_mews_snapshot(self, payload: Mapping[str, Any]) -> Any: ...

    async def record_reminder_stop_ack(self, payload: Mapping[str, Any]) -> Any: ...

    async def trigger_manual_scan(self, request_id: str) -> Any: ...


class MewsSnapshotRequest(BaseModel):
    """Immutable MEWS evidence accepted from the external research process."""

    model_config = ConfigDict(extra="allow")

    snapshot_id: str = Field(min_length=1)
    source_trade_date: date
    generated_at: datetime
    fast_state: Literal["NORMAL", "DANGER"]
    model_version: str = Field(min_length=1)
    data_version: str = Field(min_length=1)

    @field_validator("generated_at")
    @classmethod
    def generated_at_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("generated_at must be timezone-aware")
        return value


class ReminderStopAckRequest(BaseModel):
    """Consumer acknowledgement that suppresses future exit reminders only."""

    # received_at and authentication evidence are server-owned fields.  Reject
    # unknown keys so a caller cannot make either value appear client-authored.
    model_config = ConfigDict(extra="forbid")

    ack_id: str = Field(min_length=1)
    original_exit_event_id: str = Field(min_length=1)
    consumer_id: str = Field(min_length=1)
    ack_ts: datetime

    @field_validator("ack_ts")
    @classmethod
    def ack_ts_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("ack_ts must be timezone-aware")
        return value


def _require_ingest_api_key(
    provided_key: str | None = Depends(_INGEST_API_KEY_HEADER),
) -> None:
    """Fail closed and compare the configured ingest secret in constant time."""

    expected_key = os.environ.get(_INGEST_API_KEY_ENV)
    if not expected_key:
        raise HTTPException(
            status_code=503,
            detail=f"{_INGEST_API_KEY_ENV} is not configured",
        )

    candidate = provided_key or ""
    if not candidate or not hmac.compare_digest(candidate, expected_key):
        raise HTTPException(status_code=401, detail="Invalid V20 API key")


def _require_status_api_key(
    provided_key: str | None = Depends(_STATUS_API_KEY_HEADER),
) -> None:
    """Keep status credentials independent from evidence-write credentials."""

    expected_key = os.environ.get(_STATUS_API_KEY_ENV)
    if not expected_key:
        raise HTTPException(
            status_code=503,
            detail=f"{_STATUS_API_KEY_ENV} is not configured",
        )
    candidate = provided_key or ""
    if not candidate or not hmac.compare_digest(candidate, expected_key):
        raise HTTPException(status_code=401, detail="Invalid V20 status API key")


def _get_service(request: Request) -> V20RouteService:
    service = getattr(request.app.state, "v20_service", None)
    if service is None:
        raise HTTPException(status_code=503, detail="V20 service is not available")
    return service


async def _call_service(operation: Callable[[], Awaitable[Any]]) -> Any:
    """Translate domain/storage failures without exposing internal details."""

    try:
        return await operation()
    except V20LeadershipLost as exc:
        logger.error("V20 manual/runtime operation lost leadership: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="V20 runtime leadership is unavailable",
        ) from exc
    except (V20SemanticConflict, V20StateConflict) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except V20RepositoryError as exc:
        logger.warning("V20 repository unavailable: %s", exc)
        raise HTTPException(status_code=503, detail="V20 persistence is unavailable") from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("V20 service operation failed")
        raise HTTPException(status_code=503, detail="V20 service operation failed") from exc


def create_v20_router() -> APIRouter:
    """Build the V20 status/evidence router."""

    router = APIRouter(prefix="/api/v20", tags=["v20"])

    @router.get("/status", dependencies=[Depends(_require_status_api_key)])
    async def status(request: Request) -> Any:
        service = _get_service(request)
        return await _call_service(service.status)

    @router.post("/mews-snapshots", dependencies=[Depends(_require_ingest_api_key)])
    async def ingest_mews_snapshot(request: Request, body: MewsSnapshotRequest) -> Any:
        service = _get_service(request)
        payload = body.model_dump(mode="json")
        return await _call_service(lambda: service.ingest_mews_snapshot(payload))

    @router.post("/reminder-stop-acks", dependencies=[Depends(_require_ingest_api_key)])
    async def record_reminder_stop_ack(request: Request, body: ReminderStopAckRequest) -> Any:
        service = _get_service(request)
        payload = body.model_dump(mode="json")
        return await _call_service(lambda: service.record_reminder_stop_ack(payload))

    @router.post(
        "/trigger-scan",
        status_code=202,
    )
    async def trigger_scan(
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> Any:
        """Accelerate the legal decision lane and queue a non-actionable receipt."""

        async for chunk in request.stream():
            if chunk:
                raise HTTPException(
                    status_code=400,
                    detail="V20 manual trigger does not accept a request body",
                )
        service = _get_service(request)
        request_id = idempotency_key or f"manual-{uuid4()}"
        return await _call_service(lambda: service.trigger_manual_scan(request_id))

    return router
