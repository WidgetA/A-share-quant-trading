"""Public host status must expose V20 health without leaking diagnostics."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.web.routes import create_router


class _StatusService:
    def __init__(self, result: Any = None, error: Exception | None = None) -> None:
        self.config = SimpleNamespace(enabled=True)
        self.startup_stage = "RUNNING"
        self._result = result
        self._error = error

    async def status(self) -> Any:
        if self._error is not None:
            raise self._error
        return self._result


def _client(service: object | None) -> TestClient:
    app = FastAPI()
    app.state.pending_store = []
    app.state.v20_service = service
    app.state.v20_deployment_mode = "forward_shadow"
    app.state.v20_service_started = service is not None
    app.state.v20_retry_task = None
    app.state.v20_start_error = None
    app.state.v20_start_error_code = None
    app.include_router(create_router())
    return TestClient(app)


def test_public_status_exposes_only_sanitized_v20_lane_and_outbox_health() -> None:
    service = _StatusService(
        {
            "healthy": False,
            "running": True,
            "config_hash": "do-not-publish-config-hash",
            "runtime_lanes": {
                "live_exit": {
                    "healthy": False,
                    "last_success_at": "2026-09-01T09:39:45+08:00",
                    "success_age_seconds": 16.25,
                    "last_error": "LIVE_EXIT_CYCLE_TIMEOUT: password=never-publish-this",
                },
                "publisher": {
                    "healthy": True,
                    "last_success_at": "2026-09-01T09:39:59+08:00",
                    "success_age_seconds": 1,
                    "last_error": None,
                },
                "mews_cache": {
                    "healthy": True,
                    "last_success_at": "2026-09-01T09:39:50+08:00",
                    "success_age_seconds": 10,
                    "last_error": None,
                },
                "decision": {"last_error": "secret decision detail"},
            },
            "outbox": {
                "pending_delivery_n": 2,
                "leased_n": 1,
                "delivery_error_n": 3,
                "oldest_unsent_at": "sensitive-internal-detail",
            },
        }
    )

    response = _client(service).get("/api/status")

    assert response.status_code == 200
    v20 = response.json()["v20"]
    assert v20["health_summary_available"] is True
    assert v20["healthy"] is False
    assert v20["running"] is True
    assert v20["health_error_code"] is None
    assert v20["runtime_lanes"] == {
        "live_exit": {
            "healthy": False,
            "last_success_at": "2026-09-01T09:39:45+08:00",
            "success_age_seconds": 16.25,
            "last_error_code": "LIVE_EXIT_CYCLE_TIMEOUT",
        },
        "publisher": {
            "healthy": True,
            "last_success_at": "2026-09-01T09:39:59+08:00",
            "success_age_seconds": 1.0,
            "last_error_code": None,
        },
        "mews_cache": {
            "healthy": True,
            "last_success_at": "2026-09-01T09:39:50+08:00",
            "success_age_seconds": 10.0,
            "last_error_code": None,
        },
    }
    assert v20["outbox"] == {
        "pending_delivery_n": 2,
        "leased_n": 1,
        "delivery_error_n": 3,
    }
    rendered = response.text
    assert "never-publish-this" not in rendered
    assert "do-not-publish-config-hash" not in rendered
    assert "sensitive-internal-detail" not in rendered
    assert "secret decision detail" not in rendered


def test_public_status_fails_closed_without_failing_host_health_or_leaking_error() -> None:
    service = _StatusService(error=RuntimeError("postgres://user:secret@internal/v20"))

    response = _client(service).get("/api/status")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    v20 = response.json()["v20"]
    assert v20["health_summary_available"] is False
    assert v20["healthy"] is False
    assert v20["running"] is False
    assert v20["health_error_code"] == "STATUS_UNAVAILABLE"
    assert v20["runtime_lanes"]["live_exit"]["last_error_code"] is None
    assert v20["outbox"] == {
        "pending_delivery_n": None,
        "leased_n": None,
        "delivery_error_n": None,
    }
    assert "secret" not in response.text


def test_public_status_rejects_malformed_cached_snapshot_fields() -> None:
    service = _StatusService(
        {
            "healthy": True,
            "running": True,
            "runtime_lanes": {"live_exit": {}, "publisher": "not-a-lane"},
            "outbox": {},
        }
    )

    response = _client(service).get("/api/status")

    assert response.status_code == 200
    v20 = response.json()["v20"]
    assert v20["health_summary_available"] is False
    assert v20["healthy"] is False
    assert v20["running"] is False
    assert v20["health_error_code"] == "STATUS_SNAPSHOT_INVALID"


def test_public_status_does_not_publish_unknown_uppercase_error_tokens() -> None:
    service = _StatusService(
        {
            "healthy": False,
            "running": True,
            "runtime_lanes": {
                "live_exit": {"last_error": "SUPER_SECRET_TOKEN: credential detail"},
                "publisher": {"last_error": "LIVE_EXIT_CYCLE_TIMEOUT: wrong lane"},
                "mews_cache": {},
            },
            "outbox": {},
        }
    )

    response = _client(service).get("/api/status")

    assert response.status_code == 200
    lanes = response.json()["v20"]["runtime_lanes"]
    assert lanes["live_exit"]["last_error_code"] == "UNCLASSIFIED_ERROR"
    assert lanes["publisher"]["last_error_code"] == "UNCLASSIFIED_ERROR"
    assert "SUPER_SECRET_TOKEN" not in response.text
    assert "credential detail" not in response.text
    assert "LIVE_EXIT_CYCLE_TIMEOUT" not in response.text
