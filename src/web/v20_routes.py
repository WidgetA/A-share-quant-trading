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
_FRESH_PROBE_LOOKBACK_SESSIONS = 10
_FRESH_PROBE_LOCK_TIMEOUT_SECONDS = 180.0
_FRESH_PROBE_ALERT_CODE = "MANUAL_0939_CHAIN_PROBE_RESULT"
_FRESH_PROBE_PROFILE = "CURRENT_DEPLOYED_CODE_EXACT_0939_ENTRY_RENDER_V2"
_FROZEN_ENTRY_REPLAY_ALERT_CODE = "MANUAL_MORNING_ENTRY_MESSAGE_REPLAY"
_FROZEN_ENTRY_REPLAY_PROFILE = "FROZEN_OFFICIAL_ENTRY_MESSAGE_V1"


class V20RouteService(Protocol):
    """The small service surface intentionally visible to HTTP routes."""

    async def status(self) -> Any: ...

    async def ingest_mews_snapshot(self, payload: Mapping[str, Any]) -> Any: ...

    async def record_reminder_stop_ack(self, payload: Mapping[str, Any]) -> Any: ...

    async def trigger_manual_scan(self, request_id: str) -> Any: ...

    async def trigger_morning_selection(self, request_id: str) -> Any: ...


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


def _fresh_probe_event_id(service: Any, request_id: str) -> str:
    """Bind one idempotent deployment probe to the exact deployed runtime."""

    config = service.config
    return named_hash(
        "V20_MANUAL_0939_CHAIN_PROBE_EVENT_ID_V2",
        {
            "route_id": config.route_id,
            "official_stream_id": config.official_stream_id,
            "lineage_id": config.state_lineage_id,
            "state_semantics_hash": config.state_semantics_hash,
            "config_hash": config.config_hash,
            "manual_request_id": request_id,
        },
    )


def _fresh_probe_response(
    service: Any,
    record: Any,
    *,
    request_id: str,
    created: bool,
) -> Mapping[str, Any]:
    """Validate and expose a compact response without upgrading old evidence."""

    config = service.config
    semantic = record.semantic
    expected = {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": record.event_id,
        "strategy_version": config.strategy_version,
        "config_hash": config.config_hash,
        "state_semantics_hash": config.state_semantics_hash,
        "deployment_mode": config.deployment_mode,
        "official_stream_id": config.official_stream_id,
        "state_lineage_id": config.state_lineage_id,
        "alert_code": _FRESH_PROBE_ALERT_CODE,
        "delivery_priority_class": "OPERATOR_NOTIFICATION",
        "manual_request_id": request_id,
        "probe_profile": _FRESH_PROBE_PROFILE,
        "replay_reused": False,
        "official_state_changed": False,
        "orders_changed": False,
        "non_actionable": True,
        "retrospective_expired": True,
    }
    if (
        record.event_type != "DATA_ALERT"
        or record.event_id != _fresh_probe_event_id(service, request_id)
        or (record.route_id, record.official_stream_id, record.lineage_id)
        != (config.route_id, config.official_stream_id, config.state_lineage_id)
        or any(semantic.get(key) != value for key, value in expected.items())
        or semantic.get("probe_result") not in {"PASS", "FAIL"}
        or not isinstance(semantic.get("current_version_recomputed"), bool)
    ):
        raise V20SemanticConflict("manual 09:39 chain probe has incompatible persisted semantics")
    passed = semantic["probe_result"] == "PASS"
    if passed != bool(semantic["current_version_recomputed"]):
        raise V20SemanticConflict("manual 09:39 chain probe result is inconsistent")
    expected_message_mode = "AUTOMATIC_ENTRY_RENDER" if passed else "FAILURE_ALERT"
    if semantic.get("visible_message_mode") != expected_message_mode:
        raise V20SemanticConflict("manual 09:39 chain probe message mode is inconsistent")
    if record.payload is None:
        raise V20SemanticConflict("manual 09:39 chain probe payload is not sealed")
    return {
        "accepted": True,
        "created": created,
        "chain_probe_available": passed,
        "chain_probe_passed": passed,
        "chain_probe_event_id": record.event_id,
        "chain_probe_result": semantic["probe_result"],
        "probe_event_id": record.event_id,
        "probe_result": semantic["probe_result"],
        "current_version_recomputed": semantic["current_version_recomputed"],
        "replay_reused": False,
        "event_trade_date": semantic["event_trade_date"],
        "v16_count": semantic.get("v16_count", 0),
        "v20_action": semantic.get("v20_action"),
        "final_multiplier": semantic.get("final_multiplier"),
        "symbols": [
            {
                "rank": item.get("rank"),
                "code": item.get("code"),
                "name": item.get("name"),
                "snapshot_price": item.get("snapshot_price"),
            }
            for item in (semantic.get("symbols") or [])
            if isinstance(item, Mapping)
        ],
        "failure_stage": semantic.get("failure_stage"),
        "failure_reason": semantic.get("failure_reason"),
        "official_state_changed": False,
        "orders_changed": False,
        "retrospective_expired": True,
        "exact_automatic_message": passed,
        "visible_message_mode": expected_message_mode,
        "manual_notice_actionable": False,
        "feishu_delivery_confirmed": record.delivery_status == "SENT",
    }


async def _select_fresh_probe_context(service: Any, now: datetime) -> tuple[Any, Any, Any]:
    """Select the newest failed slot that is still the current official state."""

    config = service.config
    calendar = tuple(await service._load_trade_calendar(now.date()))
    current_state = await service._repository.load_state(config.state_lineage_id)
    sessions = [session for session in calendar if session <= now.date()]
    for trade_date in reversed(sessions[-_FRESH_PROBE_LOOKBACK_SESSIONS:]):
        status = await service._repository.get_entry_status(
            config.official_stream_id,
            trade_date,
        )
        if (
            status is None
            or status.action != "INPUT_INVALID"
            or status.semantic.get("state_after_hash") != current_state.state_hash
        ):
            continue
        service._verify_entry_binding(status)
        live_context = service._context
        if (
            live_context is not None
            and live_context.trade_date == trade_date
            and live_context.entry_status is not None
            and live_context.entry_status.event_id == status.event_id
        ):
            return live_context, status, current_state

        # The state-sensitive helper remains in v20_service.py.  This route
        # supplies its ordinary read-only context without changing that file.
        from src.web.v20_service import _DayContext

        context = _DayContext(
            trade_date=trade_date,
            calendar=calendar,
            entry_status=status,
            last_phase="DECISION_COMMITTED",
        )
        return context, status, current_state
    raise V20StateConflict("no recent INPUT_INVALID slot matches the current official V20 state")


def _fresh_probe_failure_semantic(
    service: Any,
    *,
    event_id: str,
    request_id: str,
    trade_date: date,
    now: datetime,
    failure_stage: str,
    failure: BaseException,
    official_status: Any | None,
) -> dict[str, Any]:
    config = service.config
    reason = f"{type(failure).__name__}: {failure}"[:1_000]
    return {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": event_id,
        "strategy_version": config.strategy_version,
        "config_hash": config.config_hash,
        "state_semantics_hash": config.state_semantics_hash,
        "deployment_mode": config.deployment_mode,
        "official_stream_id": config.official_stream_id,
        "state_lineage_id": config.state_lineage_id,
        "alert_code": _FRESH_PROBE_ALERT_CODE,
        "delivery_priority_class": "OPERATOR_NOTIFICATION",
        "manual_request_id": request_id,
        "event_trade_date": trade_date.isoformat(),
        "probe_profile": _FRESH_PROBE_PROFILE,
        "probe_result": "FAIL",
        "current_version_recomputed": False,
        "replay_reused": False,
        "data_source": "PERSISTED_09:31_09:39",
        "data_window_start": "09:31",
        "data_window_end": "09:39",
        "quote_coverage": None,
        "raw_fact_n": 0,
        "v16_count": 0,
        "v20_action": None,
        "final_multiplier": None,
        "symbols": [],
        "computed_at": now.isoformat(),
        "official_entry_action": (official_status.action if official_status is not None else None),
        "official_entry_event_id": (
            official_status.event_id if official_status is not None else None
        ),
        "official_state_changed": False,
        "orders_changed": False,
        "non_actionable": True,
        "retrospective_expired": True,
        "visible_message_mode": "FAILURE_ALERT",
        "failure_stage": failure_stage,
        "failure_reason": reason,
        "message": (
            "当前部署版本未能完成09:31-09:39原始数据到V16、V20的重新计算；"
            f"失败阶段={failure_stage}；原因={reason}。"
            "本次没有复用旧结果，也没有修改正式决策、持仓或订单。"
        ),
    }


def _fresh_probe_pass_semantic(
    service: Any,
    *,
    event_id: str,
    request_id: str,
    replay_semantic: Mapping[str, Any],
) -> dict[str, Any]:
    entry_render_semantic = replay_semantic.get("entry_render_semantic")
    if not isinstance(entry_render_semantic, Mapping):
        raise V20SemanticConflict("fresh 09:39 chain probe lacks entry formatter evidence")
    symbols = replay_semantic.get("symbols")
    if not isinstance(symbols, list):
        raise V20SemanticConflict("fresh 09:39 chain probe produced an invalid symbol list")
    raw_fact_n = replay_semantic.get("raw_fact_n")
    if isinstance(raw_fact_n, bool) or not isinstance(raw_fact_n, int) or raw_fact_n <= 0:
        raise V20SemanticConflict("fresh 09:39 chain probe lacks durable raw facts")
    action = replay_semantic.get("replay_action")
    if action not in {"ENTER", "BLOCK", "NO_SIGNAL"}:
        raise V20SemanticConflict("fresh 09:39 chain probe produced an invalid V20 action")
    if (
        entry_render_semantic.get("action") != action
        or entry_render_semantic.get("final_multiplier") != replay_semantic.get("final_multiplier")
        or entry_render_semantic.get("symbols") != symbols
    ):
        raise V20SemanticConflict("fresh 09:39 formatter evidence differs from its result")
    quote_coverage = replay_semantic.get("quote_coverage")
    if (
        isinstance(quote_coverage, bool)
        or not isinstance(quote_coverage, (int, float))
        or not 0 <= float(quote_coverage) <= 1
    ):
        quote_coverage = None
    return {
        **dict(replay_semantic),
        "event_id": event_id,
        "alert_code": _FRESH_PROBE_ALERT_CODE,
        "delivery_priority_class": "OPERATOR_NOTIFICATION",
        "manual_request_id": request_id,
        "probe_profile": _FRESH_PROBE_PROFILE,
        "probe_result": "PASS",
        "current_version_recomputed": True,
        "replay_reused": False,
        "data_source": "PERSISTED_09:31_09:39",
        "data_window_start": "09:31",
        "data_window_end": "09:39",
        # The existing replay helper returns the decision but not the bundle's
        # measured coverage.  Keep this unknown rather than inventing 100%.
        "quote_coverage": quote_coverage,
        "quote_coverage_note": (
            "MEASURED_BY_REPLAY_HELPER"
            if quote_coverage is not None
            else "NOT_EXPOSED_BY_EXISTING_REPLAY_HELPER"
        ),
        "v16_count": len(symbols),
        "v20_action": action,
        "official_state_changed": False,
        "orders_changed": False,
        "non_actionable": True,
        "retrospective_expired": True,
        "visible_message_mode": "AUTOMATIC_ENTRY_RENDER",
        "entry_render_semantic": dict(entry_render_semantic),
        "stage_results": {
            "persisted_raw_0931_0939": "PASS",
            "v16_scan": "PASS",
            "v20_prepare": "PASS",
        },
        "message": (
            "当前部署版本已从持久化的09:31-09:39原始数据重新完成V16到V20计算；"
            "没有复用旧复盘，也没有修改正式决策、持仓或订单。"
        ),
    }


async def _run_fresh_0939_probe(
    service: Any,
    request_id: str,
    now: datetime,
) -> Mapping[str, Any]:
    """Run and persist one fresh, post-cutoff, officially read-only probe."""

    if _MANUAL_REQUEST_ID.fullmatch(request_id) is None:
        raise ValueError(
            "Idempotency-Key must be 8-128 characters using letters, digits, . _ : or -"
        )
    await service._require_manual_trigger_ready()
    await service._repository.assert_runtime_leader()
    event_id = _fresh_probe_event_id(service, request_id)
    config = service.config
    ledger_scope = {
        "official_stream_id": config.official_stream_id,
        "lineage_id": config.state_lineage_id,
    }

    existing = await service._repository.get_outbox_event(
        event_id,
        route_id=config.route_id,
        **ledger_scope,
    )
    if existing is not None:
        if existing.payload is None:
            await service._require_manual_trigger_ready()
            await service._repository.assert_runtime_leader()
            existing = await service._repository.seal_event(event_id, seal_v20_payload)
        return _fresh_probe_response(
            service,
            existing,
            request_id=request_id,
            created=False,
        )

    if service._manual_trigger_lock.locked():
        raise V20StateConflict("another V20 manual trigger is already running")
    async with service._manual_trigger_lock:
        await service._require_manual_trigger_ready()
        await service._repository.assert_runtime_leader()
        existing = await service._repository.get_outbox_event(
            event_id,
            route_id=config.route_id,
            **ledger_scope,
        )
        if existing is not None:
            if existing.payload is None:
                existing = await service._repository.seal_event(event_id, seal_v20_payload)
            return _fresh_probe_response(
                service,
                existing,
                request_id=request_id,
                created=False,
            )

        probe_trade_date = now.date()
        official_status: Any | None = None
        failure_stage = "TARGET_SELECTION"
        try:
            decision_lock = getattr(service, "_decision_cycle_lock", None)
            if decision_lock is None:
                # Real V20Service always exposes this lock.  The local fallback
                # keeps narrow route doubles usable without weakening runtime.
                decision_lock = asyncio.Lock()
            try:
                await asyncio.wait_for(
                    decision_lock.acquire(),
                    timeout=_FRESH_PROBE_LOCK_TIMEOUT_SECONDS,
                )
            except TimeoutError as exc:
                raise V20StateConflict("V20 decision lane is busy") from exc
            try:
                context, official_status, state_before = await _select_fresh_probe_context(
                    service,
                    now,
                )
                probe_trade_date = context.trade_date
                status_before = (
                    official_status.event_id,
                    official_status.action,
                    official_status.semantic.get("state_after_hash"),
                    getattr(official_status, "semantic_content_hash", None),
                )
                failure_stage = "RAW_V16_V20_RECOMPUTE"
                try:
                    await asyncio.wait_for(
                        service._late_0939_replay_lock.acquire(),
                        timeout=_FRESH_PROBE_LOCK_TIMEOUT_SECONDS,
                    )
                except TimeoutError as exc:
                    raise V20StateConflict("fresh 09:39 probe lane is busy") from exc
                try:
                    replay_semantic = await asyncio.wait_for(
                        service._build_late_0939_replay_semantic(
                            context,
                            now,
                            replay_event_id=event_id,
                        ),
                        timeout=_FRESH_PROBE_LOCK_TIMEOUT_SECONDS,
                    )
                finally:
                    service._late_0939_replay_lock.release()

                failure_stage = "OFFICIAL_READ_ONLY_POSTCHECK"
                state_after = await service._repository.load_state(config.state_lineage_id)
                status_after = await service._repository.get_entry_status(
                    config.official_stream_id,
                    probe_trade_date,
                )
                if status_after is None:
                    raise V20StateConflict("probe target entry disappeared during recomputation")
                after_fingerprint = (
                    status_after.event_id,
                    status_after.action,
                    status_after.semantic.get("state_after_hash"),
                    getattr(status_after, "semantic_content_hash", None),
                )
                if (state_after.revision, state_after.state_hash) != (
                    state_before.revision,
                    state_before.state_hash,
                ) or after_fingerprint != status_before:
                    raise V20StateConflict(
                        "official V20 state changed while the read-only probe was running"
                    )
            finally:
                decision_lock.release()
            semantic = _fresh_probe_pass_semantic(
                service,
                event_id=event_id,
                request_id=request_id,
                replay_semantic=replay_semantic,
            )
            semantic.update(
                {
                    "official_state_revision_before": state_before.revision,
                    "official_state_hash_before": state_before.state_hash,
                    "official_state_revision_after": state_after.revision,
                    "official_state_hash_after": state_after.state_hash,
                    "official_entry_event_id_before": status_before[0],
                    "official_entry_event_id_after": after_fingerprint[0],
                }
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("V20 fresh 09:39 deployment probe failed")
            semantic = _fresh_probe_failure_semantic(
                service,
                event_id=event_id,
                request_id=request_id,
                trade_date=probe_trade_date,
                now=now,
                failure_stage=failure_stage,
                failure=exc,
                official_status=official_status,
            )

        await service._require_manual_trigger_ready()
        await service._repository.assert_runtime_leader()
        created = await service._repository.enqueue_alert(
            event_id,
            config.route_id,
            semantic,
            sha256_json(semantic),
            **ledger_scope,
        )
        await service._require_manual_trigger_ready()
        await service._repository.assert_runtime_leader()
        sealed = await service._repository.seal_event(event_id, seal_v20_payload)
        return _fresh_probe_response(
            service,
            sealed,
            request_id=request_id,
            created=created,
        )


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
                timeout=_FRESH_PROBE_LOCK_TIMEOUT_SECONDS,
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

    for trade_date in reversed(sessions[-_FRESH_PROBE_LOOKBACK_SESSIONS:]):
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
    if (
        record.event_type != "DATA_ALERT"
        or any(semantic.get(key) != value for key, value in expected.items())
        or record.payload is None
        or record.payload.get("message") != semantic.get("message")
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
        "manual_notice_actionable": False,
        "feishu_delivery_confirmed": record.delivery_status == "SENT",
    }


async def _replay_frozen_entry_message(
    service: Any,
    request_id: str,
    status: Any,
) -> Mapping[str, Any]:
    """Queue the already sealed automatic message without changing one byte."""

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
            # The Feishu sealer copies this exact string.  It must not prepend or
            # append a manual-trigger banner, timestamp, newline, or warning.
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


async def _dispatch_manual_trigger(service: Any, request_id: str) -> Any:
    """Run the official live lane or replay its exact visible morning output."""

    now = service._aware_now()
    wall = now.timetz().replace(tzinfo=None)
    clock = service.config.clock
    if clock.prewarm <= wall < clock.publish_deadline:
        return await service.trigger_morning_selection(request_id)
    latest = await _latest_terminal_entry(service, now)
    if latest is not None and latest.action != "INPUT_INVALID":
        return await _replay_frozen_entry_message(service, request_id, latest)
    return await _run_fresh_0939_probe(service, request_id, now)


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
        """Run/replay morning selection with the exact automatic Feishu text."""

        async for chunk in request.stream():
            if chunk:
                raise HTTPException(
                    status_code=400,
                    detail="V20 manual trigger does not accept a request body",
                )
        service = _get_service(request)
        request_id = idempotency_key or f"manual-{uuid4()}"
        return await _call_service(lambda: _dispatch_manual_trigger(service, request_id))

    return router
