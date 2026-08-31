"""Contract tests for the deliberately narrow V20 HTTP boundary."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.web.app as web_app
from src.data.database.v20_repository import (
    V20LeadershipLost,
    V20RepositoryError,
    V20SemanticConflict,
    V20StateConflict,
)
from src.web.app import create_app
from src.web.v20_routes import create_v20_router


class StubV20Service:
    def __init__(self) -> None:
        self.mews_payload: dict[str, Any] | None = None
        self.ack_payload: dict[str, Any] | None = None
        self.trigger_request_id: str | None = None
        self.error: Exception | None = None

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


def test_mews_fast_state_is_a_frozen_enum(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("V20_INGEST_API_KEY", "test-v20-key")
    client = _client(StubV20Service())
    payload = _mews_payload()
    payload["fast_state"] = "DANGER "

    response = client.post(
        "/api/v20/mews-snapshots",
        json=payload,
        headers={"X-V20-API-Key": "test-v20-key"},
    )

    assert response.status_code == 422


def test_router_exposes_only_status_evidence_and_manual_trigger_endpoints() -> None:
    router = create_v20_router()

    routes = {(route.path, frozenset(route.methods or ())) for route in router.routes}

    assert routes == {
        ("/api/v20/status", frozenset({"GET"})),
        ("/api/v20/mews-snapshots", frozenset({"POST"})),
        ("/api/v20/reminder-stop-acks", frozenset({"POST"})),
        ("/api/v20/trigger-scan", frozenset({"POST"})),
    }
    assert not any(
        word in route.path for route in router.routes for word in ("order", "holding", "fill")
    )


def test_app_factory_reserves_service_state_and_mounts_router() -> None:
    app = create_app()

    assert app.state.v20_service is None
    assert "/api/v20/status" in {route.path for route in app.routes}


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
        classmethod(lambda _cls: embedded),
    )
    monkeypatch.setattr(
        V20Service,
        "from_default_config",
        classmethod(lambda _cls: strict),
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
        == 503
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


def test_writes_fail_closed_when_ingest_key_is_not_configured(monkeypatch) -> None:
    monkeypatch.delenv("V20_INGEST_API_KEY", raising=False)
    service = StubV20Service()

    response = _client(service).post(
        "/api/v20/mews-snapshots",
        headers={"X-V20-API-Key": "anything"},
        json=_mews_payload(),
    )

    assert response.status_code == 503
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
        == 401
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

    assert response.status_code == 401


def test_authorized_mews_snapshot_preserves_extra_evidence(monkeypatch) -> None:
    monkeypatch.setenv("V20_INGEST_API_KEY", "correct-secret")
    service = StubV20Service()

    response = _client(service).post(
        "/api/v20/mews-snapshots",
        headers={"X-V20-API-Key": "correct-secret"},
        json=_mews_payload(),
    )

    assert response.status_code == 200
    assert response.json() == {"snapshot_id": "mews-20260831", "accepted": True}
    assert service.mews_payload == _mews_payload()


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
    monkeypatch.setattr(web_app, "_create_default_v20_service", lambda: service)

    assert await web_app._start_v20_lifecycle(app) is True
    assert app.state.v20_service is service
    assert service.start_calls == 1

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

    def fail_factory() -> object:
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
