"""Contract tests for the deliberately narrow V20 HTTP boundary."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import date, datetime, time
from types import SimpleNamespace
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.web.app as web_app
from src.data.database.v20_repository import (
    OutboxRecord,
    V20LeadershipLost,
    V20RepositoryError,
    V20SemanticConflict,
    V20StateConflict,
    sha256_json,
)
from src.strategy.v20.models import (
    V20_DATA_ALERT_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
)
from src.web.app import create_app
from src.web.v20_routes import (
    _dispatch_manual_trigger,
    _replay_frozen_entry_message,
    _wait_for_manual_trigger_ready,
    create_v20_router,
)


class StubV20Service:
    def __init__(self) -> None:
        self.config = SimpleNamespace(
            clock=SimpleNamespace(prewarm=time(9, 15), publish_deadline=time(9, 40)),
            config_hash="a" * 64,
            state_semantics_hash="b" * 64,
            strategy_version="V20",
            deployment_mode="forward_shadow",
            route_id="V20_SHADOW_FEISHU",
            official_stream_id="formal-stream",
            state_lineage_id="formal-lineage",
        )
        self.now = datetime(2026, 8, 31, 9, 39, tzinfo=ZoneInfo("Asia/Shanghai"))
        self._context = SimpleNamespace(
            trade_date=self.now.date(),
            calendar=(self.now.date(),),
        )
        self.mews_payload: dict[str, Any] | None = None
        self.ack_payload: dict[str, Any] | None = None
        self.trigger_request_id: str | None = None
        self.manual_monitor_source_event_id: str | None = None
        self.manual_monitor_request_id: str | None = None
        self.mews_trigger_times: list[datetime] = []
        self.error: Exception | None = None

    def _aware_now(self) -> datetime:
        return self.now

    async def _load_trade_calendar(self, _current_date: date) -> tuple[date, ...]:
        return (self.now.date(),)

    async def _result(self, value: dict[str, Any]) -> dict[str, Any]:
        if self.error is not None:
            raise self.error
        return value

    async def status(self) -> dict[str, Any]:
        return await self._result({"enabled": False, "mode": "disabled"})

    async def ingest_mews_snapshot(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.mews_payload = payload
        return await self._result({"snapshot_id": payload["snapshot_id"], "accepted": True})

    async def record_reminder_stop_ack(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.ack_payload = payload
        return await self._result({"ack_id": payload["ack_id"], "accepted": True})

    async def ensure_mews_for_selection_trigger(self, now: datetime) -> bool:
        self.mews_trigger_times.append(now)
        return False

    async def trigger_manual_scan(self, request_id: str) -> dict[str, Any]:
        self.trigger_request_id = request_id
        allowed = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-")
        if not 8 <= len(request_id) <= 128 or any(char not in allowed for char in request_id):
            raise ValueError("invalid Idempotency-Key")
        return await self._result(
            {
                "accepted": True,
                "created": True,
                "manual_request_id": request_id,
                "manual_event_id": "manual-event-1",
                "trade_date": "2026-08-31",
                "cycle_result": "ALREADY_TERMINAL",
                "formal_decision_available": True,
                "entry_action": "BLOCK",
                "entry_event_id": "entry-event-1",
                "official_state_changed": False,
                "manual_notice_actionable": False,
                "sealed": True,
                "delivery_status": "PENDING",
                "feishu_delivery_confirmed": False,
            }
        )

    async def trigger_morning_selection(self, request_id: str) -> dict[str, Any]:
        return await self.trigger_manual_scan(request_id)

    async def enroll_manual_monitor(
        self,
        source_event_id: str,
        request_id: str,
    ) -> dict[str, Any]:
        self.manual_monitor_source_event_id = source_event_id
        self.manual_monitor_request_id = request_id
        return await self._result(
            {
                "accepted": True,
                "created": True,
                "armed": True,
                "manual_request_id": request_id,
                "source_event_id": source_event_id,
                "armed_leg_count": 10,
                "official_state_changed": False,
                "orders_changed": False,
            }
        )


class _FreshProbeRepository:
    def __init__(self, service: "FreshProbeV20Service") -> None:
        self.service = service
        self.state = SimpleNamespace(
            lineage_id=service.config.state_lineage_id,
            revision=7,
            state_hash="c" * 64,
            payload={"state_revision": 7},
        )
        self.entry_status = SimpleNamespace(
            action="INPUT_INVALID",
            trade_date=date(2026, 8, 31),
            event_id="failed-entry-event",
            semantic={"state_after_hash": self.state.state_hash},
        )
        self.events: dict[str, OutboxRecord] = {
            "old-late-replay-event": OutboxRecord(
                event_id="old-late-replay-event",
                event_type="DATA_ALERT",
                route_id=service.config.route_id,
                official_stream_id=service.config.official_stream_id,
                lineage_id=service.config.state_lineage_id,
                semantic={"alert_code": "LATE_0939_REPLAY_RESULT"},
                semantic_content_hash="d" * 64,
                payload={"message": "old replay"},
                payload_hash=sha256_json({"message": "old replay"}),
                generated_at=service.now,
                commit_marker=1,
                action_expiry_ts=None,
                delivery_status="SENT",
                attempt_count=1,
            )
        }
        self.leader_calls = 0
        self.enqueue_calls = 0
        self.seal_calls = 0
        self.official_write_calls = 0

    async def assert_runtime_leader(self) -> None:
        self.leader_calls += 1

    async def load_state(self, lineage_id: str):
        assert lineage_id == self.service.config.state_lineage_id
        return self.state

    async def get_entry_status(self, official_stream_id: str, trade_date: date):
        assert official_stream_id == self.service.config.official_stream_id
        return self.entry_status if trade_date == self.entry_status.trade_date else None

    async def get_outbox_event(self, event_id: str, **_kwargs):
        return self.events.get(event_id)

    async def enqueue_alert(
        self,
        event_id: str,
        route_id: str,
        semantic: dict[str, Any],
        semantic_hash: str,
        **scope,
    ) -> bool:
        self.enqueue_calls += 1
        assert route_id == self.service.config.route_id
        assert scope == {
            "official_stream_id": self.service.config.official_stream_id,
            "lineage_id": self.service.config.state_lineage_id,
        }
        assert semantic_hash == sha256_json(semantic)
        if event_id in self.events:
            return False
        self.events[event_id] = OutboxRecord(
            event_id=event_id,
            event_type="DATA_ALERT",
            route_id=route_id,
            official_stream_id=scope["official_stream_id"],
            lineage_id=scope["lineage_id"],
            semantic=dict(semantic),
            semantic_content_hash=semantic_hash,
            payload=None,
            payload_hash=None,
            generated_at=None,
            commit_marker=None,
            action_expiry_ts=None,
            delivery_status="PENDING",
            attempt_count=0,
        )
        return True

    async def seal_event(self, event_id: str, builder) -> OutboxRecord:
        self.seal_calls += 1
        current = self.events[event_id]
        if current.payload is not None:
            return current
        payload = dict(builder(current, self.service.now, 100 + self.seal_calls, True))
        sealed = replace(
            current,
            payload=payload,
            payload_hash=sha256_json(payload),
            generated_at=self.service.now,
            commit_marker=100 + self.seal_calls,
        )
        self.events[event_id] = sealed
        return sealed

    async def commit_entry(self, *_args, **_kwargs) -> None:
        self.official_write_calls += 1
        raise AssertionError("manual chain probe must not commit an official entry")

    async def commit_exit(self, *_args, **_kwargs) -> None:
        self.official_write_calls += 1
        raise AssertionError("manual chain probe must not commit an exit")


class FreshProbeV20Service(StubV20Service):
    def __init__(self) -> None:
        super().__init__()
        self.now = datetime(2026, 9, 1, 0, 5, tzinfo=ZoneInfo("Asia/Shanghai"))
        self._context = None
        self._repository = _FreshProbeRepository(self)
        self._manual_trigger_lock = asyncio.Lock()
        self._decision_cycle_lock = asyncio.Lock()
        self._late_0939_replay_lock = asyncio.Lock()
        self.build_calls: list[tuple[date, str]] = []
        self.ensure_replay_calls = 0
        self.old_manual_calls = 0
        self.probe_error: Exception | None = None
        self.probe_symbols: list[dict[str, Any]] | None = None

    @property
    def _ledger_scope(self) -> dict[str, str]:
        return {
            "official_stream_id": self.config.official_stream_id,
            "lineage_id": self.config.state_lineage_id,
        }

    async def _require_manual_trigger_ready(self) -> None:
        return None

    async def _load_trade_calendar(self, _current_date: date) -> tuple[date, ...]:
        return (
            date(2026, 8, 28),
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
            date(2026, 9, 3),
        )

    def _verify_entry_binding(self, status: Any) -> None:
        assert status is self._repository.entry_status

    async def _build_late_0939_replay_semantic(
        self,
        context: Any,
        now: datetime,
        *,
        replay_event_id: str,
    ) -> dict[str, Any]:
        self.build_calls.append((context.trade_date, replay_event_id))
        if self.probe_error is not None:
            raise self.probe_error
        symbols = self.probe_symbols or [
            {
                "rank": 1,
                "code": "000001",
                "name": "平安银行",
                "score": 0.81234,
                "snapshot_price": 10.26,
                "boards": ["银行"],
                "best_board": "银行",
                "is_driver": True,
                "cci": 88.0,
                "volume_937": 120000.0,
                "history_hash": "f" * 64,
                "early_source_hash": "e" * 64,
            }
        ]
        return {
            "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
            "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
            "event_id": replay_event_id,
            "strategy_version": self.config.strategy_version,
            "config_hash": self.config.config_hash,
            "state_semantics_hash": self.config.state_semantics_hash,
            "deployment_mode": self.config.deployment_mode,
            "official_stream_id": self.config.official_stream_id,
            "state_lineage_id": self.config.state_lineage_id,
            "event_trade_date": context.trade_date.isoformat(),
            "official_entry_action": context.entry_status.action,
            "official_entry_event_id": context.entry_status.event_id,
            "replay_action": "ENTER",
            "final_multiplier": 1.0,
            "symbols": symbols,
            "entry_render_semantic": {
                "schema_version": "v20-entry-semantic/v2",
                "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
                "event_id": "hypothetical-entry-event",
                "deployment_mode": self.config.deployment_mode,
                "trade_date": context.trade_date.isoformat(),
                "action": "ENTER",
                "final_multiplier": 1.0,
                "base_multiplier": 1.0,
                "defense_multiplier": 1.0,
                "health_state": "HEALTHY",
                "rolling7_state": "NON_BAD",
                "rolling7_r7": 0.1,
                "rolling7_l7": 1,
                "g_state": "NOT_EVALUATED",
                "reason_codes": [],
                "last_complete_bar": "09:39",
                "v16_funnel": {
                    "step0_universe_count": 100,
                    "step2_hot_board_count": 1,
                    "final_candidates": 1,
                },
                "v16_board_avg_gains": {"银行": 1.23},
                "symbols": symbols,
                "scheduled_exits_today": [],
            },
            "raw_fact_n": 9,
            "quote_coverage": 1.0,
            "computed_at": now.isoformat(),
        }

    async def _ensure_late_0939_replay(self, *_args, **_kwargs):
        self.ensure_replay_calls += 1
        raise AssertionError("fresh chain probe must not reuse the old replay event")

    async def trigger_manual_scan(self, _request_id: str) -> dict[str, Any]:
        self.old_manual_calls += 1
        raise AssertionError("post-cutoff route must run the fresh chain probe")


class LifecycleV20Service:
    def __init__(
        self,
        *,
        enabled: bool,
        deployment_mode: str,
        start_error: Exception | None = None,
        stop_error: Exception | None = None,
    ) -> None:
        self.config = SimpleNamespace(enabled=enabled, deployment_mode=deployment_mode)
        self.start_error = start_error
        self.stop_error = stop_error
        self.start_calls = 0
        self.stop_calls = 0

    async def start(self) -> None:
        self.start_calls += 1
        if self.start_error is not None:
            raise self.start_error

    async def stop(self) -> None:
        self.stop_calls += 1
        if self.stop_error is not None:
            raise self.stop_error


class IquantMonitorStub:
    def __init__(self) -> None:
        self.start_calls = 0

    def _start_monitoring(self) -> None:
        self.start_calls += 1


def _lifecycle_app(service: object | None) -> FastAPI:
    app = FastAPI()
    app.state.v20_service = service
    app.state.v15_scan_state = object()
    app.state.iquant_router = IquantMonitorStub()
    return app


def _client(service: StubV20Service | None = None) -> TestClient:
    app = FastAPI()
    app.state.v20_service = service
    app.include_router(create_v20_router())
    return TestClient(app)


def _mews_payload() -> dict[str, Any]:
    return {
        "snapshot_id": "mews-20260831",
        "source_trade_date": "2026-08-30",
        "generated_at": "2026-08-31T09:38:00+08:00",
        "fast_state": "DANGER",
        "model_version": "mews-v1",
        "data_version": "snapshot-v7",
        "evidence_hash": "abc123",
    }


def _ack_payload() -> dict[str, Any]:
    return {
        "ack_id": "ack-1",
        "original_exit_event_id": "exit-event-1",
        "consumer_id": "manual-console",
        "ack_ts": "2026-08-31T10:15:00+08:00",
    }


def test_external_mews_ingest_route_is_closed_in_production() -> None:
    response = _client(StubV20Service()).post(
        "/api/v20/mews-snapshots",
        json=_mews_payload(),
        headers={"X-V20-API-Key": "test-v20-key"},
    )

    assert response.status_code == 404


def test_router_exposes_only_status_evidence_and_manual_trigger_endpoints() -> None:
    router = create_v20_router()

    routes = {(route.path, frozenset(route.methods or ())) for route in router.routes}

    assert routes == {
        ("/api/v20/status", frozenset({"GET"})),
        ("/api/v20/reminder-stop-acks", frozenset({"POST"})),
        ("/api/v20/trigger-scan", frozenset({"POST"})),
        ("/api/v20/manual-monitor", frozenset({"POST"})),
    }
    assert not any(
        word in route.path for route in router.routes for word in ("order", "holding", "fill")
    )


def test_app_factory_reserves_service_state_and_mounts_router() -> None:
    app = create_app()

    assert app.state.v20_service is None
    assert "/api/v20/status" in {route.path for route in app.routes}


def test_app_factory_binds_one_scan_state_instance_to_v20() -> None:
    class Service:
        def __init__(self) -> None:
            self.scan_state = None

        def bind_shared_v15_scan_state(self, scan_state) -> None:
            self.scan_state = scan_state

    service = Service()
    app = create_app(v20_service=service)

    assert service.scan_state is app.state.v15_scan_state


def test_legacy_main_factory_selects_embedded_v20_without_dedicated_activation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.web.v20_service import V20Service

    for name in (
        "V20_ENABLED",
        "V20_MODE",
        "V20_ALLOW_PRODUCTION_PUSH",
        "V20_EMBEDDED_ENABLED",
    ):
        monkeypatch.delenv(name, raising=False)
    embedded = object()
    strict = object()
    monkeypatch.setattr(
        V20Service,
        "from_legacy_runtime",
        classmethod(lambda _cls, **_kwargs: embedded),
    )
    monkeypatch.setattr(
        V20Service,
        "from_default_config",
        classmethod(lambda _cls, **_kwargs: strict),
    )

    assert web_app._create_default_v20_service() is embedded

    monkeypatch.setenv("V20_EMBEDDED_ENABLED", "false")
    assert web_app._create_default_v20_service() is strict


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (RuntimeError("legacy main Feishu route is not configured"), "FEISHU_CONFIGURATION"),
        (ValueError("Tushare Pro token not configured"), "TUSHARE_CONFIGURATION"),
        (ConnectionError("Cannot connect to fundamentals database"), "DATABASE_CONNECTION"),
        (RuntimeError("permission denied for schema v20"), "DATABASE_PERMISSION"),
    ],
)
def test_v20_startup_diagnostics_are_sanitized(error: Exception, code: str) -> None:
    assert web_app._v20_start_error_code(error) == code


def test_authorized_status_returns_disabled_service_structure(monkeypatch) -> None:
    monkeypatch.setenv("V20_STATUS_API_KEY", "status-secret")
    response = _client(StubV20Service()).get(
        "/api/v20/status",
        headers={"X-V20-Status-Key": "status-secret"},
    )

    assert response.status_code == 200
    assert response.json() == {"enabled": False, "mode": "disabled"}


def test_all_routes_fail_gracefully_when_service_is_unavailable(monkeypatch) -> None:
    monkeypatch.setenv("V20_INGEST_API_KEY", "secret")
    monkeypatch.setenv("V20_STATUS_API_KEY", "status-secret")
    client = _client(None)

    assert (
        client.get(
            "/api/v20/status",
            headers={"X-V20-Status-Key": "status-secret"},
        ).status_code
        == 503
    )
    assert (
        client.post(
            "/api/v20/mews-snapshots",
            headers={"X-V20-API-Key": "secret"},
            json=_mews_payload(),
        ).status_code
        == 404
    )
    assert (
        client.post(
            "/api/v20/reminder-stop-acks",
            headers={"X-V20-API-Key": "secret"},
            json=_ack_payload(),
        ).status_code
        == 503
    )
    assert (
        client.post(
            "/api/v20/trigger-scan",
            headers={"Idempotency-Key": "deploy-check-1"},
        ).status_code
        == 503
    )
    assert (
        client.post(
            "/api/v20/manual-monitor",
            headers={"Idempotency-Key": "manual-monitor-check-1"},
            json={"source_event_id": "a" * 64},
        ).status_code
        == 503
    )


def test_writes_fail_closed_when_ingest_key_is_not_configured(monkeypatch) -> None:
    monkeypatch.delenv("V20_INGEST_API_KEY", raising=False)
    service = StubV20Service()

    response = _client(service).post(
        "/api/v20/mews-snapshots",
        headers={"X-V20-API-Key": "anything"},
        json=_mews_payload(),
    )

    assert response.status_code == 404
    assert service.mews_payload is None


def test_status_fails_closed_when_api_key_is_not_configured(monkeypatch) -> None:
    monkeypatch.delenv("V20_STATUS_API_KEY", raising=False)

    response = _client(StubV20Service()).get("/api/v20/status")

    assert response.status_code == 503


@pytest.mark.parametrize("provided", [None, "wrong"])
def test_status_rejects_missing_or_invalid_key(monkeypatch, provided: str | None) -> None:
    monkeypatch.setenv("V20_STATUS_API_KEY", "correct-status-secret")
    headers = {"X-V20-Status-Key": provided} if provided is not None else {}

    response = _client(StubV20Service()).get("/api/v20/status", headers=headers)

    assert response.status_code == 401


def test_status_and_ingest_keys_cannot_cross_authorize(monkeypatch) -> None:
    monkeypatch.setenv("V20_STATUS_API_KEY", "status-only-secret")
    monkeypatch.setenv("V20_INGEST_API_KEY", "ingest-only-secret")
    client = _client(StubV20Service())

    assert (
        client.get(
            "/api/v20/status",
            headers={"X-V20-Status-Key": "ingest-only-secret"},
        ).status_code
        == 401
    )
    assert (
        client.post(
            "/api/v20/mews-snapshots",
            headers={"X-V20-API-Key": "status-only-secret"},
            json=_mews_payload(),
        ).status_code
        == 404
    )


@pytest.mark.parametrize("provided", [None, "wrong"])
def test_writes_reject_missing_or_invalid_key(monkeypatch, provided: str | None) -> None:
    monkeypatch.setenv("V20_INGEST_API_KEY", "correct-secret")
    headers = {"X-V20-API-Key": provided} if provided is not None else {}

    response = _client(StubV20Service()).post(
        "/api/v20/mews-snapshots",
        headers=headers,
        json=_mews_payload(),
    )

    assert response.status_code == 404


def test_authorized_mews_snapshot_cannot_reach_production_service(monkeypatch) -> None:
    monkeypatch.setenv("V20_INGEST_API_KEY", "correct-secret")
    service = StubV20Service()

    response = _client(service).post(
        "/api/v20/mews-snapshots",
        headers={"X-V20-API-Key": "correct-secret"},
        json=_mews_payload(),
    )

    assert response.status_code == 404
    assert service.mews_payload is None


def test_authorized_reminder_ack_passes_only_client_owned_fields(monkeypatch) -> None:
    monkeypatch.setenv("V20_INGEST_API_KEY", "correct-secret")
    service = StubV20Service()

    response = _client(service).post(
        "/api/v20/reminder-stop-acks",
        headers={"X-V20-API-Key": "correct-secret"},
        json=_ack_payload(),
    )

    assert response.status_code == 200
    assert response.json() == {"ack_id": "ack-1", "accepted": True}
    assert service.ack_payload == _ack_payload()


def test_reminder_ack_rejects_server_owned_received_at(monkeypatch) -> None:
    monkeypatch.setenv("V20_INGEST_API_KEY", "correct-secret")
    payload = {**_ack_payload(), "received_at": "2020-01-01T00:00:00+08:00"}

    response = _client(StubV20Service()).post(
        "/api/v20/reminder-stop-acks",
        headers={"X-V20-API-Key": "correct-secret"},
        json=payload,
    )

    assert response.status_code == 422


@pytest.mark.parametrize(
    "request_id",
    ["short", "contains space", "x" * 129],
)
def test_trigger_rejects_invalid_optional_idempotency_key(request_id: str) -> None:
    response = _client(StubV20Service()).post(
        "/api/v20/trigger-scan",
        headers={"Idempotency-Key": request_id},
    )

    assert response.status_code == 400


def test_trigger_without_headers_gets_a_server_owned_request_id() -> None:
    service = StubV20Service()

    response = _client(service).post("/api/v20/trigger-scan")

    assert response.status_code == 202
    assert service.trigger_request_id is not None
    assert service.trigger_request_id.startswith("manual-")
    UUID(service.trigger_request_id.removeprefix("manual-"))
    assert response.json()["manual_request_id"] == service.trigger_request_id


def test_trigger_returns_202_and_passes_idempotency_key_without_api_key() -> None:
    service = StubV20Service()

    response = _client(service).post(
        "/api/v20/trigger-scan",
        headers={"Idempotency-Key": "deploy-check-20260831"},
    )

    assert response.status_code == 202
    assert service.trigger_request_id == "deploy-check-20260831"
    assert service.mews_trigger_times == [service.now]
    assert response.json() == {
        "accepted": True,
        "created": True,
        "manual_request_id": "deploy-check-20260831",
        "manual_event_id": "manual-event-1",
        "trade_date": "2026-08-31",
        "cycle_result": "ALREADY_TERMINAL",
        "formal_decision_available": True,
        "entry_action": "BLOCK",
        "entry_event_id": "entry-event-1",
        "official_state_changed": False,
        "manual_notice_actionable": False,
        "sealed": True,
        "delivery_status": "PENDING",
        "feishu_delivery_confirmed": False,
    }


@pytest.mark.asyncio
async def test_manual_trigger_final_fence_waits_for_transient_lane_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    class _Service:
        async def _require_manual_trigger_ready(self) -> None:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise V20RepositoryError("runtime lane is briefly stale")

    monkeypatch.setattr(
        "src.web.v20_routes._FRESH_PROBE_HEALTH_RECOVERY_TIMEOUT_SECONDS",
        1.0,
    )

    await _wait_for_manual_trigger_ready(_Service())

    assert attempts == 3


@pytest.mark.asyncio
async def test_manual_trigger_final_fence_rejects_persistent_runtime_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Service:
        async def _require_manual_trigger_ready(self) -> None:
            raise V20RepositoryError("runtime remains unhealthy")

    monkeypatch.setattr(
        "src.web.v20_routes._FRESH_PROBE_HEALTH_RECOVERY_TIMEOUT_SECONDS",
        0.01,
    )

    with pytest.raises(V20RepositoryError, match="runtime remains unhealthy"):
        await _wait_for_manual_trigger_ready(_Service())


def test_manual_monitor_returns_202_and_passes_only_source_and_idempotency_key() -> None:
    service = StubV20Service()
    source_event_id = "a" * 64

    response = _client(service).post(
        "/api/v20/manual-monitor",
        headers={"Idempotency-Key": "manual-monitor-20260831"},
        json={"source_event_id": source_event_id},
    )

    assert response.status_code == 202
    assert service.manual_monitor_source_event_id == source_event_id
    assert service.manual_monitor_request_id == "manual-monitor-20260831"
    assert response.json() == {
        "accepted": True,
        "created": True,
        "armed": True,
        "manual_request_id": "manual-monitor-20260831",
        "source_event_id": source_event_id,
        "armed_leg_count": 10,
        "official_state_changed": False,
        "orders_changed": False,
    }


def test_manual_monitor_without_key_gets_a_server_owned_request_id() -> None:
    service = StubV20Service()

    response = _client(service).post(
        "/api/v20/manual-monitor",
        json={"source_event_id": "b" * 64},
    )

    assert response.status_code == 202
    assert service.manual_monitor_request_id is not None
    assert service.manual_monitor_request_id.startswith("manual-monitor-")
    UUID(service.manual_monitor_request_id.removeprefix("manual-monitor-"))
    assert response.json()["manual_request_id"] == service.manual_monitor_request_id


@pytest.mark.parametrize(
    "body",
    [
        {"source_event_id": "a" * 64, "trade_date": "2026-08-31"},
        {"source_event_id": "a" * 63},
        {"source_event_id": "A" * 64},
        {"source_event_id": "g" * 64},
        {"source_event_id": ""},
        {},
    ],
)
def test_manual_monitor_rejects_extra_fields_and_invalid_source(body: dict[str, Any]) -> None:
    service = StubV20Service()

    response = _client(service).post("/api/v20/manual-monitor", json=body)

    assert response.status_code == 422
    assert service.manual_monitor_source_event_id is None
    assert service.manual_monitor_request_id is None


@pytest.mark.parametrize(
    ("error", "expected_status"),
    [
        (ValueError("bad request identity"), 400),
        (V20SemanticConflict("source mismatch"), 409),
        (V20StateConflict("attachment expired"), 409),
        (V20LeadershipLost("leader replaced"), 503),
        (V20RepositoryError("database unavailable"), 503),
        (RuntimeError("unexpected"), 503),
    ],
)
def test_manual_monitor_service_failures_use_the_v20_error_boundary(
    error: Exception,
    expected_status: int,
) -> None:
    service = StubV20Service()
    service.error = error

    response = _client(service).post(
        "/api/v20/manual-monitor",
        headers={"Idempotency-Key": "manual-monitor-error-1"},
        json={"source_event_id": "c" * 64},
    )

    assert response.status_code == expected_status


def test_post_cutoff_trigger_recomputes_current_chain_without_reusing_old_replay() -> None:
    service = FreshProbeV20Service()
    repository = service._repository
    state_before = (repository.state.revision, repository.state.state_hash)
    entry_before = repository.entry_status
    client = _client(service)

    first = client.post(
        "/api/v20/trigger-scan",
        headers={"Idempotency-Key": "deploy-current-chain-001"},
    )
    repeated = client.post(
        "/api/v20/trigger-scan",
        headers={"Idempotency-Key": "deploy-current-chain-001"},
    )

    assert first.status_code == 202
    assert repeated.status_code == 202
    result = first.json()
    assert result["accepted"] is True
    assert result["created"] is True
    assert result["chain_probe_available"] is True
    assert result["chain_probe_result"] == "PASS"
    assert result["current_version_recomputed"] is True
    assert result["replay_reused"] is False
    assert result["v16_count"] == 1
    assert result["v20_action"] == "ENTER"
    assert result["final_multiplier"] == 1.0
    assert result["official_state_changed"] is False
    assert result["orders_changed"] is False
    assert result["manual_notice_actionable"] is False
    assert repeated.json() == {**result, "created": False}

    assert len(service.build_calls) == 1
    probe_event_id = result["chain_probe_event_id"]
    assert service.build_calls[0] == (date(2026, 8, 31), probe_event_id)
    assert probe_event_id != "old-late-replay-event"
    assert service.ensure_replay_calls == 0
    assert service.old_manual_calls == 0
    assert repository.enqueue_calls == 1
    assert repository.official_write_calls == 0
    assert repository.entry_status is entry_before
    assert (repository.state.revision, repository.state.state_hash) == state_before

    probe = repository.events[probe_event_id]
    assert probe.semantic["alert_code"] == "MANUAL_0939_CHAIN_PROBE_RESULT"
    assert probe.semantic["manual_request_id"] == "deploy-current-chain-001"
    assert probe.semantic["config_hash"] == service.config.config_hash
    assert probe.semantic["probe_result"] == "PASS"
    assert probe.semantic["current_version_recomputed"] is True
    assert probe.semantic["replay_reused"] is False
    assert probe.semantic["data_source"] == "PERSISTED_09:31_09:39"
    assert probe.semantic["data_window_start"] == "09:31"
    assert probe.semantic["data_window_end"] == "09:39"
    assert probe.semantic["official_state_changed"] is False
    assert probe.semantic["orders_changed"] is False
    assert probe.semantic["non_actionable"] is True
    assert probe.semantic["visible_message_mode"] == "MANUAL_OPERATOR_RENDER"
    assert probe.payload is not None
    message = str(probe.payload["message"])
    message_lines = message.splitlines()
    assert message_lines[0] == "[V20][SHADOW] 手工触发结果｜仅核查"
    assert "每日决策" not in message_lines[0]
    assert message_lines[2] == "当前操作：不生成新的入场指令，不补买，不追买"
    assert message_lines[3] == "原因：手工触发时间已过当日入场时点。"
    assert message.index("当前操作") < message.index("策略计算结果")
    assert "最终倍率 100%" in message
    assert "滚动7" in message
    assert "极端门G" in message
    assert "000001 平安银行" in message
    assert "正常建立" not in message
    assert f"事件：{probe_event_id[:16]}" in message_lines[-1]
    for banned in ("理论复盘", "当时本应", "复盘已过期"):
        assert banned not in message
    assert result["symbols"] == [
        {
            "rank": 1,
            "code": "000001",
            "name": "平安银行",
            "snapshot_price": 10.26,
        }
    ]
    assert result["non_actionable"] is True
    assert result["retrospective_expired"] is True
    assert result["exact_automatic_message"] is False
    assert repository.events["old-late-replay-event"].payload == {"message": "old replay"}


def test_post_cutoff_existing_enter_recomputes_fresh_check_only_chain() -> None:
    service = FreshProbeV20Service()
    repository = service._repository
    source_message = "[V20][SHADOW] 每日决策 (2026-08-31 09:40)\n原始票单：一字不改 ✅"
    repository.entry_status.action = "ENTER"
    repository.entry_status.trade_date = date(2026, 9, 1)
    service.now = datetime(2026, 9, 1, 14, 5, tzinfo=ZoneInfo("Asia/Shanghai"))
    expected_codes = [
        "603068",
        "605299",
        "603990",
        "603232",
        "605098",
        "603193",
        "001238",
        "002368",
        "600486",
        "600557",
    ]
    service.probe_symbols = [
        {
            "rank": rank,
            "code": code,
            "name": code,
            "score": 0.5,
            "snapshot_price": 10.0,
            "boards": ["银行"],
            "best_board": "银行",
            "is_driver": True,
            "cci": 50.0,
            "volume_937": 1000.0,
            "history_hash": "f" * 64,
            "early_source_hash": "e" * 64,
        }
        for rank, code in enumerate(expected_codes, start=1)
    ]
    repository.entry_status.final_multiplier = 1.0
    repository.entry_status.semantic = {
        "symbols": [
            {
                "rank": 1,
                "code": "000001",
                "name": "平安银行",
                "snapshot_price": 10.26,
            }
        ]
    }
    repository.events[repository.entry_status.event_id] = OutboxRecord(
        event_id=repository.entry_status.event_id,
        event_type="ENTRY_DECISION",
        route_id=service.config.route_id,
        official_stream_id=service.config.official_stream_id,
        lineage_id=service.config.state_lineage_id,
        semantic=repository.entry_status.semantic,
        semantic_content_hash="1" * 64,
        payload={"message": source_message},
        payload_hash="2" * 64,
        generated_at=service.now,
        commit_marker=9,
        action_expiry_ts=None,
        delivery_status="SENT",
        attempt_count=1,
    )

    response = _client(service).post(
        "/api/v20/trigger-scan",
        headers={"Idempotency-Key": "replay-frozen-entry-001"},
    )

    assert response.status_code == 202
    result = response.json()
    assert result["chain_probe_available"] is True
    assert result["chain_probe_passed"] is True
    assert result["current_version_recomputed"] is True
    assert result["replay_reused"] is False
    assert [item["code"] for item in result["symbols"]] == expected_codes
    assert result["exact_automatic_message"] is False
    assert result["non_actionable"] is True
    assert result["retrospective_expired"] is True
    assert result["official_state_changed"] is False
    assert result["orders_changed"] is False
    assert len(service.build_calls) == 1
    assert repository.official_write_calls == 0
    source = repository.events[repository.entry_status.event_id]
    assert source.payload == {"message": source_message}
    return
    replay = repository.events[result["replay_event_id"]]
    assert replay.payload is not None
    message = str(replay.payload["message"])
    message_lines = message.splitlines()
    assert message_lines[0] == "[V20][SHADOW] 手工触发结果｜仅核查"
    assert message_lines[2] == "当前操作：不生成新的入场指令，不补买，不追买"
    assert message_lines[3] == "原因：手工触发时间已过当日入场时点。"
    assert "以下为早盘封存原文" in message
    begin = "----- 早盘封存原文开始（逐字节未改动，仅供核查） -----\n"
    end = "\n----- 早盘封存原文结束 -----"
    extracted = message.split(begin, 1)[1].split(end, 1)[0]
    assert extracted == source_message
    assert extracted.encode("utf-8") == source_message.encode("utf-8")
    assert f"事件：{result['replay_event_id'][:16]}" in message_lines[-1]
    for banned in ("理论复盘", "当时本应", "复盘已过期"):
        assert banned not in message
    assert result["symbols"] == [
        {
            "rank": 1,
            "code": "000001",
            "name": "平安银行",
            "snapshot_price": 10.26,
        }
    ]
    assert result["exact_automatic_message"] is True
    assert result["non_actionable"] is True
    assert result["retrospective_expired"] is True
    assert result["official_state_changed"] is False
    assert result["orders_changed"] is False
    assert service.build_calls == []
    assert repository.official_write_calls == 0


def _install_normal_entry(
    service: FreshProbeV20Service,
    *,
    trade_date: date,
    event_id: str,
    message: str,
) -> Any:
    semantic = {
        "symbols": [
            {
                "rank": 1,
                "code": "000001",
                "name": "平安银行",
                "snapshot_price": 10.26,
            }
        ]
    }
    status = SimpleNamespace(
        action="ENTER",
        trade_date=trade_date,
        event_id=event_id,
        final_multiplier=1.0,
        semantic=semantic,
    )
    payload = {"message": message}
    service._repository.events[event_id] = OutboxRecord(
        event_id=event_id,
        event_type="ENTRY_DECISION",
        route_id=service.config.route_id,
        official_stream_id=service.config.official_stream_id,
        lineage_id=service.config.state_lineage_id,
        semantic=semantic,
        semantic_content_hash="1" * 64,
        payload=payload,
        payload_hash=sha256_json(payload),
        generated_at=service.now,
        commit_marker=9,
        action_expiry_ts=None,
        delivery_status="SENT",
        attempt_count=1,
    )
    return status


@pytest.mark.asyncio
async def test_concurrent_frozen_replay_same_key_enqueues_once_inside_manual_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = FreshProbeV20Service()
    repository = service._repository
    status = _install_normal_entry(
        service,
        trade_date=date(2026, 8, 31),
        event_id="concurrent-source-entry",
        message="[V20][SHADOW] 每日决策\n并发时也必须逐字一致",
    )
    repository.entry_status = status
    original_enqueue = repository.enqueue_alert
    active_enqueues = 0
    maximum_active_enqueues = 0

    async def slow_enqueue(*args: Any, **kwargs: Any) -> bool:
        nonlocal active_enqueues, maximum_active_enqueues
        active_enqueues += 1
        maximum_active_enqueues = max(maximum_active_enqueues, active_enqueues)
        try:
            await asyncio.sleep(0.01)
            return await original_enqueue(*args, **kwargs)
        finally:
            active_enqueues -= 1

    monkeypatch.setattr(repository, "enqueue_alert", slow_enqueue)

    first, second = await asyncio.gather(
        _replay_frozen_entry_message(service, "same-frozen-request-001", status),
        _replay_frozen_entry_message(service, "same-frozen-request-001", status),
    )

    assert sorted((first["created"], second["created"])) == [False, True]
    assert first["replay_event_id"] == second["replay_event_id"]
    assert maximum_active_enqueues == 1
    assert repository.enqueue_calls == 1


@pytest.mark.asyncio
async def test_post_cutoff_current_session_without_terminal_never_replays_prior_day() -> None:
    service = FreshProbeV20Service()
    service.now = datetime(2026, 9, 1, 9, 40, tzinfo=ZoneInfo("Asia/Shanghai"))
    prior = _install_normal_entry(
        service,
        trade_date=date(2026, 8, 31),
        event_id="prior-day-entry",
        message="prior day must never be replayed at today's cutoff",
    )
    service._repository.entry_status = prior

    result = await _dispatch_manual_trigger(service, "cutoff-no-prior-fallback")

    assert result["chain_probe_passed"] is False
    assert result["current_version_recomputed"] is False
    assert result["replay_reused"] is False
    assert result["official_state_changed"] is False
    assert result["orders_changed"] is False
    assert result["symbols"] == []
    assert "prior-day-entry" not in {
        value.semantic.get("source_entry_event_id") for value in service._repository.events.values()
    }


@pytest.mark.asyncio
async def test_post_cutoff_waits_for_inflight_current_terminal_before_selecting_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = FreshProbeV20Service()
    service.now = datetime(2026, 9, 1, 9, 40, tzinfo=ZoneInfo("Asia/Shanghai"))
    repository = service._repository
    prior = _install_normal_entry(
        service,
        trade_date=date(2026, 8, 31),
        event_id="race-prior-entry",
        message="stale prior-day message",
    )
    current = _install_normal_entry(
        service,
        trade_date=date(2026, 9, 1),
        event_id="race-current-entry",
        message="current-day message",
    )
    repository.entry_status = current
    current_visible = False

    async def racing_status(_stream_id: str, trade_date: date) -> Any | None:
        if trade_date == current.trade_date:
            return current if current_visible else None
        return prior if trade_date == prior.trade_date else None

    monkeypatch.setattr(repository, "get_entry_status", racing_status)
    monkeypatch.setattr(service, "_verify_entry_binding", lambda _status: None)
    await service._decision_cycle_lock.acquire()
    pending = asyncio.create_task(_dispatch_manual_trigger(service, "cutoff-inflight-current-001"))
    await asyncio.sleep(0)
    current_visible = True
    service._decision_cycle_lock.release()

    result = await pending

    assert result["event_trade_date"] == current.trade_date.isoformat()
    assert result["chain_probe_passed"] is True
    assert result["current_version_recomputed"] is True
    assert result["replay_reused"] is False
    assert result["official_state_changed"] is False
    assert result["orders_changed"] is False
    assert result["non_actionable"] is True
    assert result["retrospective_expired"] is True
    return
    replay = repository.events[result["replay_event_id"]]
    assert replay.payload is not None
    replay_message = str(replay.payload["message"])
    assert replay_message.startswith("[V20][SHADOW] 手工触发结果｜仅核查")
    begin = "----- 早盘封存原文开始（逐字节未改动，仅供核查） -----\n"
    end = "\n----- 早盘封存原文结束 -----"
    assert replay_message.split(begin, 1)[1].split(end, 1)[0] == "current-day message"


def test_same_request_recomputes_again_after_full_config_hash_changes() -> None:
    service = FreshProbeV20Service()
    client = _client(service)
    headers = {"Idempotency-Key": "deploy-current-chain-config"}

    first = client.post("/api/v20/trigger-scan", headers=headers)
    old_event_id = first.json()["chain_probe_event_id"]
    service.config.config_hash = "e" * 64
    second = client.post("/api/v20/trigger-scan", headers=headers)

    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json()["created"] is True
    assert second.json()["current_version_recomputed"] is True
    assert second.json()["replay_reused"] is False
    assert second.json()["chain_probe_event_id"] != old_event_id
    assert len(service.build_calls) == 2
    assert (
        service._repository.events[second.json()["chain_probe_event_id"]].semantic["config_hash"]
        == "e" * 64
    )


def test_failed_current_chain_probe_is_durable_and_never_falls_back_to_old_replay() -> None:
    service = FreshProbeV20Service()
    service.probe_error = RuntimeError("exact 09:39 persisted input is incomplete")

    response = _client(service).post(
        "/api/v20/trigger-scan",
        headers={"Idempotency-Key": "deploy-current-chain-fail"},
    )

    assert response.status_code == 202
    result = response.json()
    assert result["created"] is True
    assert result["chain_probe_available"] is False
    assert result["chain_probe_result"] == "FAIL"
    assert result["current_version_recomputed"] is False
    assert result["replay_reused"] is False
    assert result["official_state_changed"] is False
    assert result["orders_changed"] is False
    assert service.ensure_replay_calls == 0
    assert service.old_manual_calls == 0
    assert service._repository.official_write_calls == 0
    failed = service._repository.events[result["chain_probe_event_id"]]
    assert failed.semantic["v20_action"] is None
    assert failed.semantic["final_multiplier"] is None
    assert failed.semantic["failure_stage"]
    assert "exact 09:39 persisted input is incomplete" in failed.semantic["failure_reason"]
    assert failed.payload is not None
    assert "当前版本早盘链路重算｜❌ 失败" in str(failed.payload["message"])


def test_trigger_rejects_body_instead_of_accepting_force_or_time_overrides() -> None:
    service = StubV20Service()

    response = _client(service).post(
        "/api/v20/trigger-scan",
        headers={"Idempotency-Key": "deploy-check-no-bypass"},
        json={"force": True, "now": "2026-08-31T09:39:00+08:00"},
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "V20 manual trigger does not accept a request body"}
    assert service.trigger_request_id is None


def test_trigger_leadership_loss_is_503_not_inherited_state_conflict_409() -> None:
    service = StubV20Service()
    service.error = V20LeadershipLost("leader session was replaced")

    response = _client(service).post(
        "/api/v20/trigger-scan",
        headers={"Idempotency-Key": "deploy-check-leader-loss"},
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "V20 runtime leadership is unavailable"}


@pytest.mark.parametrize(
    ("error", "expected_status"),
    [
        (ValueError("bad evidence"), 400),
        (V20SemanticConflict("collision"), 409),
        (V20StateConflict("stale state"), 409),
        (V20RepositoryError("database offline"), 503),
        (RuntimeError("unexpected"), 503),
    ],
)
def test_service_failures_are_mapped_without_a_500(
    error: Exception,
    expected_status: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("V20_STATUS_API_KEY", "status-secret")
    service = StubV20Service()
    service.error = error

    response = _client(service).get(
        "/api/v20/status",
        headers={"X-V20-Status-Key": "status-secret"},
    )

    assert response.status_code == expected_status


@pytest.mark.asyncio
async def test_forward_shadow_starts_beside_legacy_and_keeps_iquant(monkeypatch) -> None:
    service = LifecycleV20Service(enabled=True, deployment_mode="forward_shadow")
    app = _lifecycle_app(service)
    legacy_starts: list[object] = []
    monkeypatch.setattr(web_app, "start_scan_scheduler", legacy_starts.append)

    await web_app._start_strategy_services(app)

    assert service.start_calls == 1
    assert app.state.v20_service_started is True
    assert app.state.legacy_v15_scan_allowed is True
    assert legacy_starts == [app.state.v15_scan_state]
    assert app.state.iquant_router.start_calls == 1


@pytest.mark.asyncio
async def test_legacy_app_refuses_production_v20_but_keeps_platform_iquant(monkeypatch) -> None:
    service = LifecycleV20Service(enabled=True, deployment_mode="production_push")
    app = _lifecycle_app(service)
    legacy_starts: list[object] = []
    monkeypatch.setattr(web_app, "start_scan_scheduler", legacy_starts.append)

    await web_app._start_strategy_services(app)

    assert service.start_calls == 0
    assert app.state.v20_service_started is False
    assert app.state.legacy_v15_scan_allowed is False
    assert "dedicated" in app.state.v20_start_error
    assert legacy_starts == []
    assert app.state.iquant_router.start_calls == 1


@pytest.mark.asyncio
async def test_legacy_app_never_attempts_a_failing_production_service(monkeypatch) -> None:
    service = LifecycleV20Service(
        enabled=True,
        deployment_mode="production_push",
        start_error=RuntimeError("database unavailable"),
    )
    app = _lifecycle_app(service)
    legacy_starts: list[object] = []
    monkeypatch.setattr(web_app, "start_scan_scheduler", legacy_starts.append)

    await web_app._start_strategy_services(app)

    assert service.start_calls == 0
    assert app.state.v20_service_started is False
    assert "dedicated" in app.state.v20_start_error
    assert app.state.legacy_v15_scan_allowed is False
    assert legacy_starts == []
    assert app.state.iquant_router.start_calls == 1

    await web_app._stop_v20_lifecycle(app)
    assert service.stop_calls == 0


@pytest.mark.asyncio
async def test_shadow_start_failure_retains_legacy_scan(monkeypatch) -> None:
    service = LifecycleV20Service(
        enabled=True,
        deployment_mode="forward_shadow",
        start_error=RuntimeError("shadow database unavailable"),
    )
    app = _lifecycle_app(service)
    legacy_starts: list[object] = []
    monkeypatch.setattr(web_app, "start_scan_scheduler", legacy_starts.append)

    await web_app._start_strategy_services(app)

    assert service.start_calls == 1
    assert app.state.v20_service_started is False
    assert app.state.legacy_v15_scan_allowed is True
    assert legacy_starts == [app.state.v15_scan_state]
    assert app.state.iquant_router.start_calls == 1
    assert app.state.v20_retry_task is not None

    await web_app._stop_v20_lifecycle(app)
    assert app.state.v20_retry_task is None


@pytest.mark.asyncio
async def test_shadow_startup_retry_recovers_without_restarting_v16(monkeypatch) -> None:
    class _FlakyShadowService(LifecycleV20Service):
        async def start(self) -> None:
            self.start_calls += 1
            if self.start_calls == 1:
                raise ConnectionError("database warming up")

    service = _FlakyShadowService(enabled=True, deployment_mode="forward_shadow")
    app = _lifecycle_app(service)
    legacy_starts: list[object] = []
    monkeypatch.setattr(web_app, "_V20_START_RETRY_SECONDS", 0.0)
    monkeypatch.setattr(web_app, "start_scan_scheduler", legacy_starts.append)

    await web_app._start_strategy_services(app)
    for _ in range(20):
        if app.state.v20_service_started:
            break
        await asyncio.sleep(0)

    assert app.state.v20_service_started is True
    assert app.state.v20_start_error is None
    assert app.state.v20_start_error_code is None
    assert service.start_calls == 2
    assert legacy_starts == [app.state.v15_scan_state]

    await web_app._stop_v20_lifecycle(app)
    assert service.stop_calls == 1


@pytest.mark.asyncio
async def test_repeated_lifecycle_keeps_retry_owner_and_shutdown_cannot_revive_v20(
    monkeypatch,
) -> None:
    retry_entered = asyncio.Event()
    retry_release = asyncio.Event()
    retry_cancelled = asyncio.Event()

    class _BlockingRetryService(LifecycleV20Service):
        async def start(self) -> None:
            self.start_calls += 1
            if self.start_calls == 1:
                raise ConnectionError("database warming up")
            if self.start_calls == 2:
                retry_entered.set()
                try:
                    await retry_release.wait()
                except asyncio.CancelledError:
                    retry_cancelled.set()
                    raise
                return
            # Before the lifecycle guard existed, the repeated invocation
            # reached this branch, discarded the blocked retry task reference,
            # and allowed that old task to revive V20 after shutdown.
            return

    service = _BlockingRetryService(enabled=True, deployment_mode="forward_shadow")
    app = _lifecycle_app(service)
    monkeypatch.setattr(web_app, "_V20_START_RETRY_SECONDS", 0.0)

    assert await web_app._start_v20_lifecycle(app) is True
    await asyncio.wait_for(retry_entered.wait(), timeout=1.0)
    retry_task = app.state.v20_retry_task
    assert retry_task is not None

    assert await web_app._start_v20_lifecycle(app) is True
    assert app.state.v20_retry_task is retry_task
    assert service.start_calls == 2

    await web_app._stop_v20_lifecycle(app)
    retry_release.set()
    for _ in range(3):
        await asyncio.sleep(0)

    assert retry_cancelled.is_set()
    assert retry_task.done()
    assert service.start_calls == 2
    assert service.stop_calls == 1
    assert app.state.v20_retry_task is None
    assert app.state.v20_service_started is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "legacy_allowed"),
    [("forward_shadow", True), ("production_push", False)],
)
async def test_disabled_service_does_not_start_and_mode_still_controls_ownership(
    monkeypatch,
    mode: str,
    legacy_allowed: bool,
) -> None:
    service = LifecycleV20Service(enabled=False, deployment_mode=mode)
    app = _lifecycle_app(service)
    legacy_starts: list[object] = []
    monkeypatch.setattr(web_app, "start_scan_scheduler", legacy_starts.append)

    await web_app._start_strategy_services(app)
    await web_app._stop_v20_lifecycle(app)

    assert service.start_calls == 0
    assert service.stop_calls == 0
    assert app.state.legacy_v15_scan_allowed is legacy_allowed
    assert len(legacy_starts) == int(legacy_allowed)
    assert app.state.iquant_router.start_calls == 1


@pytest.mark.asyncio
async def test_default_factory_result_is_injected_and_lifecycle_managed(monkeypatch) -> None:
    service = LifecycleV20Service(enabled=True, deployment_mode="forward_shadow")
    app = _lifecycle_app(None)
    shared_fundamentals = object()
    app.state.fundamentals_db = shared_fundamentals
    captured: dict[str, object] = {}

    def factory(**kwargs):
        captured.update(kwargs)
        return service

    monkeypatch.setattr(
        web_app,
        "_create_default_v20_service",
        factory,
    )

    assert await web_app._start_v20_lifecycle(app) is True
    assert app.state.v20_service is service
    assert service.start_calls == 1
    assert captured["fundamentals_db"] is shared_fundamentals
    assert captured["scan_state"] is app.state.v15_scan_state

    await web_app._stop_v20_lifecycle(app)
    assert service.stop_calls == 1
    assert app.state.v20_stop_error is None


@pytest.mark.asyncio
async def test_legacy_host_exposes_v20_shutdown_failure_without_skipping_state_cleanup() -> None:
    service = LifecycleV20Service(
        enabled=True,
        deployment_mode="forward_shadow",
        stop_error=RuntimeError("shutdown failed"),
    )
    app = _lifecycle_app(service)

    assert await web_app._start_v20_lifecycle(app) is True
    await web_app._stop_v20_lifecycle(app)

    assert service.stop_calls == 1
    assert app.state.v20_service_started is False
    assert app.state.v20_stop_error == "RuntimeError: shutdown failed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode_hint", "legacy_allowed"),
    [("forward_shadow", True), ("production_push", False), (None, False)],
)
async def test_default_factory_failure_uses_fail_closed_mode_hint(
    monkeypatch,
    mode_hint: str | None,
    legacy_allowed: bool,
) -> None:
    app = _lifecycle_app(None)

    def fail_factory(**_kwargs) -> object:
        raise RuntimeError("invalid V20 configuration")

    monkeypatch.setattr(web_app, "_create_default_v20_service", fail_factory)
    monkeypatch.setattr(web_app, "_default_v20_mode_hint", lambda: mode_hint)

    assert await web_app._start_v20_lifecycle(app) is legacy_allowed
    assert "invalid V20 configuration" in app.state.v20_start_error


@pytest.mark.asyncio
async def test_invalid_injected_service_config_fails_closed() -> None:
    service = LifecycleV20Service(enabled=True, deployment_mode="unexpected")
    app = _lifecycle_app(service)

    assert await web_app._start_v20_lifecycle(app) is False
    assert service.start_calls == 0
    assert app.state.v20_deployment_mode is None
    assert "deployment_mode is invalid" in app.state.v20_start_error
