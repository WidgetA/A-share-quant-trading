"""Narrow HTTP boundary for the V20 decision-notification service.

The router deliberately exposes no account, order, holding, fill, or execution
API.  It reports service health, accepts the two external evidence records
required by the documented V20 state machine, and exposes one non-bypassable
manual trigger for the production morning-selection path.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import re
from collections.abc import Awaitable, Callable, Mapping
from datetime import date, datetime
from typing import Any, Literal, Protocol
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.common.v20_feishu import seal_v20_payload
from src.data.database.v20_repository import (
    V20LeadershipLost,
    V20RepositoryError,
    V20SemanticConflict,
    V20StateConflict,
    sha256_json,
)
from src.strategy.v20.identity import named_hash
from src.strategy.v20.models import (
    V20_DATA_ALERT_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
)

logger = logging.getLogger(__name__)

_INGEST_API_KEY_ENV = "V20_INGEST_API_KEY"
_INGEST_API_KEY_HEADER = APIKeyHeader(name="X-V20-API-Key", auto_error=False)
_STATUS_API_KEY_ENV = "V20_STATUS_API_KEY"
_STATUS_API_KEY_HEADER = APIKeyHeader(name="X-V20-Status-Key", auto_error=False)
_MANUAL_REQUEST_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}")
_ENTRY_LOOKBACK_SESSIONS = 10
_MANUAL_ENTRY_LOCK_TIMEOUT_SECONDS = 180.0
_FROZEN_ENTRY_REPLAY_ALERT_CODE = "MANUAL_MORNING_ENTRY_MESSAGE_REPLAY"
_FROZEN_ENTRY_REPLAY_PROFILE = "FROZEN_OFFICIAL_ENTRY_MESSAGE_V1"
_POST_CUTOFF_SERIALIZATION_RETRY_LIMIT = 4
_POST_CUTOFF_SERIALIZATION_RETRY_BASE_SECONDS = 0.01


_POST_CUTOFF_TERMINAL_ACTIONS = frozenset({"ENTER", "BLOCK", "NO_SIGNAL", "INPUT_INVALID"})


class V20RouteService(Protocol):
    """The small service surface intentionally visible to HTTP routes."""

    async def status(self) -> Any: ...

    async def ingest_mews_snapshot(self, payload: Mapping[str, Any]) -> Any: ...

    async def record_reminder_stop_ack(self, payload: Mapping[str, Any]) -> Any: ...

    async def trigger_morning_selection(self, request_id: str) -> Any: ...

    async def trigger_canonical_selection_check_only(
        self,
        request_id: str,
        now: datetime,
    ) -> Any: ...

    def kick_mews_for_selection_trigger(self, now: datetime) -> Any: ...

    async def enroll_manual_monitor(self, source_event_id: str, request_id: str) -> Any: ...


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


class ManualMonitorRequest(BaseModel):
    """Name one immutable replay event; all tickets and dates remain server-owned."""

    model_config = ConfigDict(extra="forbid")

    source_event_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")


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


async def _latest_terminal_entry(service: Any, now: datetime) -> Any | None:
    """Return the latest durable morning slot at or before the service date."""

    calendar = tuple(await service._load_trade_calendar(now.date()))
    sessions = [session for session in calendar if session <= now.date()]
    wall = now.timetz().replace(tzinfo=None)
    if now.date() in sessions and wall >= service.config.clock.publish_deadline:
        # At/after today's hard cutoff, serialize behind the production decision
        # lane before deciding which slot to replay.  Otherwise an HTTP request
        # can observe today's row just before the scheduler commits it and
        # incorrectly fall back to yesterday's perfectly valid message.
        decision_lock = getattr(service, "_decision_cycle_lock", None)
        if decision_lock is None:
            raise V20StateConflict("V20 decision lane lock is unavailable")
        try:
            await asyncio.wait_for(
                decision_lock.acquire(),
                timeout=_MANUAL_ENTRY_LOCK_TIMEOUT_SECONDS,
            )
        except TimeoutError as exc:
            raise V20StateConflict("V20 decision lane is busy") from exc
        try:
            status = await service._repository.get_entry_status(
                service.config.official_stream_id,
                now.date(),
            )
        finally:
            decision_lock.release()
        if status is None:
            raise V20StateConflict(
                "current trading day's V20 morning slot is not terminal; refusing prior-day replay"
            )
        service._verify_entry_binding(status)
        return status

    for trade_date in reversed(sessions[-_ENTRY_LOOKBACK_SESSIONS:]):
        status = await service._repository.get_entry_status(
            service.config.official_stream_id,
            trade_date,
        )
        if status is not None:
            service._verify_entry_binding(status)
            return status
    return None


def _frozen_entry_replay_event_id(service: Any, request_id: str, source: Any) -> str:
    config = service.config
    return named_hash(
        "V20_MANUAL_MORNING_ENTRY_MESSAGE_REPLAY_EVENT_ID_V1",
        {
            "route_id": config.route_id,
            "official_stream_id": config.official_stream_id,
            "lineage_id": config.state_lineage_id,
            "config_hash": config.config_hash,
            "manual_request_id": request_id,
            "source_entry_event_id": source.event_id,
            "source_payload_hash": source.payload_hash,
        },
    )


def _ticket_summary(semantic: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "rank": item.get("rank"),
            "code": item.get("code"),
            "name": item.get("name"),
            "snapshot_price": item.get("snapshot_price"),
        }
        for item in (semantic.get("symbols") or [])
        if isinstance(item, Mapping)
    ]


def _frozen_entry_replay_response(
    service: Any,
    record: Any,
    *,
    request_id: str,
    created: bool,
) -> Mapping[str, Any]:
    semantic = record.semantic
    expected = {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": record.event_id,
        "strategy_version": service.config.strategy_version,
        "config_hash": service.config.config_hash,
        "state_semantics_hash": service.config.state_semantics_hash,
        "deployment_mode": service.config.deployment_mode,
        "official_stream_id": service.config.official_stream_id,
        "state_lineage_id": service.config.state_lineage_id,
        "alert_code": _FROZEN_ENTRY_REPLAY_ALERT_CODE,
        "delivery_priority_class": "OPERATOR_NOTIFICATION",
        "manual_request_id": request_id,
        "replay_profile": _FROZEN_ENTRY_REPLAY_PROFILE,
        "visible_message_mode": "FROZEN_OFFICIAL_PAYLOAD",
        "exact_automatic_message": True,
        "retrospective_expired": True,
        "official_state_changed": False,
        "orders_changed": False,
        "non_actionable": True,
    }
    payload_message = record.payload.get("message") if record.payload is not None else None
    if (
        record.event_type != "DATA_ALERT"
        or any(semantic.get(key) != value for key, value in expected.items())
        or not isinstance(payload_message, str)
        or "手工触发结果｜仅核查" not in payload_message
        or str(semantic.get("message")) not in payload_message
    ):
        raise V20SemanticConflict("manual morning message replay has incompatible semantics")
    return {
        "accepted": True,
        "created": created,
        "manual_request_id": request_id,
        "event_trade_date": semantic["event_trade_date"],
        "entry_action": semantic["source_entry_action"],
        "final_multiplier": semantic["source_final_multiplier"],
        "symbols": _ticket_summary(semantic),
        "source_entry_event_id": semantic["source_entry_event_id"],
        "replay_event_id": record.event_id,
        "visible_message_mode": "FROZEN_OFFICIAL_PAYLOAD",
        "exact_automatic_message": True,
        "retrospective_expired": True,
        "official_state_changed": False,
        "orders_changed": False,
        "non_actionable": True,
        "manual_notice_actionable": False,
        "feishu_delivery_confirmed": record.delivery_status == "SENT",
    }


async def _replay_frozen_entry_message(
    service: Any,
    request_id: str,
    status: Any,
) -> Mapping[str, Any]:
    """Queue a check-only replay embedding the sealed morning message byte-for-byte."""

    if _MANUAL_REQUEST_ID.fullmatch(request_id) is None:
        raise ValueError(
            "Idempotency-Key must be 8-128 characters using letters, digits, . _ : or -"
        )
    await service._require_manual_trigger_ready()
    await service._repository.assert_runtime_leader()
    config = service.config
    scope = {
        "official_stream_id": config.official_stream_id,
        "lineage_id": config.state_lineage_id,
    }
    source = await service._repository.get_outbox_event(
        status.event_id,
        route_id=config.route_id,
        **scope,
    )
    if source is None or source.event_type != "ENTRY_DECISION":
        raise V20RepositoryError("latest official morning entry message is unavailable")
    if source.payload is None:
        await service._repository.assert_runtime_leader()
        source = await service._repository.seal_event(source.event_id, seal_v20_payload)
    source_message = source.payload.get("message") if source.payload is not None else None
    if not isinstance(source_message, str) or not source_message or not source.payload_hash:
        raise V20SemanticConflict("latest official morning entry message is not sealed")
    event_id = _frozen_entry_replay_event_id(service, request_id, source)

    existing = await service._repository.get_outbox_event(
        event_id,
        route_id=config.route_id,
        **scope,
    )
    if existing is not None:
        if existing.payload is None:
            await service._repository.assert_runtime_leader()
            existing = await service._repository.seal_event(event_id, seal_v20_payload)
        return _frozen_entry_replay_response(
            service,
            existing,
            request_id=request_id,
            created=False,
        )

    async with service._manual_trigger_lock:
        await service._require_manual_trigger_ready()
        await service._repository.assert_runtime_leader()
        # The first lookup is only a fast path.  A same-key request may have
        # completed while this request waited for the process-local lock, so
        # idempotency requires a second durable lookup inside the lock.
        existing = await service._repository.get_outbox_event(
            event_id,
            route_id=config.route_id,
            **scope,
        )
        if existing is not None:
            if existing.payload is None:
                existing = await service._repository.seal_event(event_id, seal_v20_payload)
            return _frozen_entry_replay_response(
                service,
                existing,
                request_id=request_id,
                created=False,
            )

        symbols = _ticket_summary(status.semantic)
        semantic = {
            "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
            "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
            "event_id": event_id,
            "strategy_version": config.strategy_version,
            "config_hash": config.config_hash,
            "state_semantics_hash": config.state_semantics_hash,
            "deployment_mode": config.deployment_mode,
            "official_stream_id": config.official_stream_id,
            "state_lineage_id": config.state_lineage_id,
            "alert_code": _FROZEN_ENTRY_REPLAY_ALERT_CODE,
            "delivery_priority_class": "OPERATOR_NOTIFICATION",
            "manual_request_id": request_id,
            "event_trade_date": status.trade_date.isoformat(),
            "replay_profile": _FROZEN_ENTRY_REPLAY_PROFILE,
            "visible_message_mode": "FROZEN_OFFICIAL_PAYLOAD",
            "exact_automatic_message": True,
            "retrospective_expired": True,
            "source_entry_event_id": source.event_id,
            "source_entry_action": status.action,
            "source_final_multiplier": status.final_multiplier,
            "source_semantic_content_hash": source.semantic_content_hash,
            "source_payload_hash": source.payload_hash,
            "message_sha256": hashlib.sha256(source_message.encode("utf-8")).hexdigest(),
            "symbols": symbols,
            "official_state_changed": False,
            "orders_changed": False,
            "non_actionable": True,
            # The durable semantic keeps the verbatim official bytes so the
            # message_sha256 binding still authenticates the source.  At seal
            # time the Feishu formatter embeds this string unchanged inside a
            # clearly labeled sealed-source region under the manual check-only
            # banner; the visible payload is therefore banner + verbatim
            # source, never a bare copy that could read as a new instruction.
            "message": source_message,
        }
        created = await service._repository.enqueue_alert(
            event_id,
            config.route_id,
            semantic,
            sha256_json(semantic),
            **scope,
        )
        await service._repository.assert_runtime_leader()
        sealed = await service._repository.seal_event(event_id, seal_v20_payload)
        return _frozen_entry_replay_response(
            service,
            sealed,
            request_id=request_id,
            created=created,
        )


async def _today_terminal_entry(service: Any, now: datetime) -> Any | None:
    repository = service._repository
    config = service.config
    status = await repository.get_entry_status(
        config.official_stream_id,
        now.date(),
    )
    if status is not None:
        if status.action not in _POST_CUTOFF_TERMINAL_ACTIONS:
            raise V20StateConflict("current V20 morning slot is not terminal")
        service._verify_entry_binding(status)
        return status

    decision_lock = getattr(service, "_decision_cycle_lock", None)
    if decision_lock is None:
        raise V20StateConflict("V20 decision lane lock is unavailable")
    try:
        await asyncio.wait_for(
            decision_lock.acquire(),
            timeout=_MANUAL_ENTRY_LOCK_TIMEOUT_SECONDS,
        )
    except TimeoutError as exc:
        raise V20StateConflict("V20 decision lane is busy") from exc
    try:
        status = await repository.get_entry_status(
            config.official_stream_id,
            now.date(),
        )
    finally:
        decision_lock.release()
    if status is not None:
        if status.action not in _POST_CUTOFF_TERMINAL_ACTIONS:
            raise V20StateConflict("current V20 morning slot is not terminal")
        service._verify_entry_binding(status)
    return status


def _kick_mews_for_selection_trigger(service: Any, now: datetime) -> Any:
    kick = getattr(service, "kick_mews_for_selection_trigger", None)
    if kick is None:
        raise V20StateConflict("V20 MEWS trigger kick is unavailable")
    return kick(now)


def _is_postgres_serialization_conflict(exc: BaseException) -> bool:
    """Recognize only PostgreSQL serialization failures (SQLSTATE 40001)."""

    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        code = getattr(current, "pgcode", None) or getattr(current, "sqlstate", None)
        if code == "40001":
            return True
        current = current.__cause__ or current.__context__
    return False


async def _run_post_cutoff_idempotent_check(
    canonical_trigger: Callable[[str, datetime], Awaitable[Any]],
    request_id: str,
    now: datetime,
) -> Any:
    """Converge concurrent same-key operator probes after a serializable race.

    This adapter is intentionally restricted to the post-cutoff canonical
    check-only hook.  That hook has a deterministic event id and may only
    persist its idempotent operator notification; official state, orders, and
    model batches are read-only.  The live morning/ordering lanes never call
    this retry boundary.
    """

    for attempt in range(_POST_CUTOFF_SERIALIZATION_RETRY_LIMIT):
        try:
            return await canonical_trigger(request_id, now)
        except Exception as exc:
            if (
                not _is_postgres_serialization_conflict(exc)
                or attempt + 1 >= _POST_CUTOFF_SERIALIZATION_RETRY_LIMIT
            ):
                raise
            logger.warning(
                "V20 post-cutoff same-key probe hit PostgreSQL serialization conflict; "
                "retrying idempotent durable read (attempt %s/%s)",
                attempt + 2,
                _POST_CUTOFF_SERIALIZATION_RETRY_LIMIT,
            )
            await asyncio.sleep(_POST_CUTOFF_SERIALIZATION_RETRY_BASE_SECONDS * (2**attempt))
    raise AssertionError("unreachable post-cutoff serialization retry state")


async def _dispatch_manual_trigger(service: Any, request_id: str) -> Any:
    """Run the live lane or a post-cutoff durable-artifact check-only probe."""

    if _MANUAL_REQUEST_ID.fullmatch(request_id) is None:
        raise ValueError(
            "Idempotency-Key must be 8-128 characters using letters, digits, . _ : or -"
        )
    now = service._aware_now()
    wall = now.timetz().replace(tzinfo=None)
    clock = service.config.clock
    if wall < clock.publish_deadline:
        _kick_mews_for_selection_trigger(service, now)
        return await service.trigger_morning_selection(request_id)
    canonical_trigger = getattr(
        service,
        "trigger_canonical_selection_check_only",
        None,
    )
    if canonical_trigger is None:
        raise V20StateConflict("canonical V20 check-only selection adapter is unavailable")
    mews_attempt = _kick_mews_for_selection_trigger(service, now)
    try:
        return await _run_post_cutoff_idempotent_check(
            canonical_trigger,
            request_id,
            now,
        )
    finally:
        # A post-cutoff operator probe is allowed to wait for the independently
        # managed MEWS singleflight to settle.  Its success/failure never changes
        # the canonical selection result, while awaiting it here guarantees that
        # a genuine failure has finished its idempotent alert before the request
        # returns.  Narrow route doubles may expose a synchronous no-op kick.
        if isinstance(mews_attempt, Awaitable):
            await asyncio.gather(mews_attempt, return_exceptions=True)


def create_v20_router() -> APIRouter:
    """Build the V20 status/evidence router."""

    router = APIRouter(prefix="/api/v20", tags=["v20"])

    @router.get("/status", dependencies=[Depends(_require_status_api_key)])
    async def status(request: Request) -> Any:
        service = _get_service(request)
        return await _call_service(service.status)

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
        """Run the morning calculation and expose its result with the proper actionability.

        Before cutoff this can commit the ordinary automatic entry message.
        After cutoff it reuses the same strategy-output renderer inside a
        clearly non-actionable, read-only operator wrapper.
        """

        async for chunk in request.stream():
            if chunk:
                raise HTTPException(
                    status_code=400,
                    detail="V20 manual trigger does not accept a request body",
                )
        service = _get_service(request)
        request_id = idempotency_key or f"manual-{uuid4()}"
        return await _call_service(lambda: _dispatch_manual_trigger(service, request_id))

    @router.post(
        "/manual-monitor",
        status_code=202,
    )
    async def enroll_manual_monitor(
        request: Request,
        body: ManualMonitorRequest,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> Any:
        """Arm the ordinary D1/D2 exit lane for a sealed retrospective ticket list."""

        service = _get_service(request)
        request_id = idempotency_key or f"manual-monitor-{uuid4()}"
        return await _call_service(
            lambda: service.enroll_manual_monitor(body.source_event_id, request_id)
        )

    return router
