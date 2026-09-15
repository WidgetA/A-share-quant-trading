from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from src.web.v20_routes import create_v20_router


async def test_user_can_report_legacy_stock_sold_without_an_exit_signal(monkeypatch):
    monkeypatch.setenv("V20_INGEST_API_KEY", "test-key")
    service = SimpleNamespace(
        list_legacy_positions=AsyncMock(return_value=[{"position_id": "old-1", "revision": 0}]),
        calibrate_legacy_position=AsyncMock(return_value={"status": "CLOSED", "quantity": 0}),
    )
    app = FastAPI()
    app.state.v20_service = service
    app.include_router(create_v20_router())
    headers = {"X-V20-API-Key": "test-key", "Idempotency-Key": "sold-20260915-001"}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        result = await client.post(
            "/api/v20/legacy-positions/old-1/calibrate",
            headers=headers,
            json={"expected_revision": 0, "quantity": 0},
        )
        assert result.status_code == 200, result.text
        assert result.json() == {"status": "CLOSED", "quantity": 0}
        assert (await client.get("/api/v20/legacy-positions")).status_code == 401
        assert (await client.get("/api/v20/legacy-positions", headers=headers)).json() == [
            {"position_id": "old-1", "revision": 0}
        ]
    service.calibrate_legacy_position.assert_awaited_once_with(
        "old-1", "sold-20260915-001", {"expected_revision": 0, "quantity": 0}
    )


@pytest.mark.parametrize(
    "change",
    [
        {},
        {"quantity": -1},
        {"quantity": 1.5},
        {"status": "SOLD"},
        {"sold_price": 1},
        {"quantity": None},
        {"quantity": 3, "status": "CLOSED"},
        {"quantity": 0, "status": "MONITORING"},
    ],
)
async def test_legacy_sync_rejects_invalid_or_inconsistent_corrections(monkeypatch, change):
    monkeypatch.setenv("V20_INGEST_API_KEY", "test-key")
    app = FastAPI()
    service = SimpleNamespace(calibrate_legacy_position=AsyncMock())
    app.state.v20_service = service
    app.include_router(create_v20_router())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v20/legacy-positions/old-1/calibrate",
            headers={"X-V20-API-Key": "test-key", "Idempotency-Key": "invalid-001"},
            json={"expected_revision": 0, **change},
        )
    assert response.status_code == 422
    service.calibrate_legacy_position.assert_not_awaited()
