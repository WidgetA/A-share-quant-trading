from __future__ import annotations

from datetime import date

import httpx
import pytest

from src.data.clients.mews_snapshot import (
    MewsSnapshotNotReady,
    MewsSnapshotSourceError,
    PublishedMewsSnapshotClient,
)


def _document(**point_overrides):
    point = {
        "date": "2026-08-31",
        "signal_available_date": "2026-09-01",
        "updated_at": 1_788_226_200_000,  # 2026-09-01 09:30 Asia/Shanghai
        "risk_state": "DANGER",
        "data_status": "OK",
        "mews": 72.5,
        "exhaustion_path": 60.0,
        "persistent_deleveraging_path": 72.5,
    }
    point.update(point_overrides)
    return {
        "version": "mews_v2",
        "latest_valid": point,
        "storage": {
            "metric_end": "2026-08-31",
            "latest_ingestion_status": "OK",
        },
    }


def _client(document) -> PublishedMewsSnapshotClient:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=document)

    return PublishedMewsSnapshotClient(
        "http://mews.internal/api/trading/margin-risk-curve?days=5",
        api_key="test-key",
        transport=httpx.MockTransport(handler),
    )


def test_mews_source_configuration_is_mandatory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("V20_MEWS_SOURCE_URL", raising=False)

    with pytest.raises(ValueError, match="V20_MEWS_SOURCE_URL is required"):
        PublishedMewsSnapshotClient.from_environment()


def test_mews_api_key_is_mandatory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "V20_MEWS_SOURCE_URL",
        "http://mews.internal/api/trading/margin-risk-curve?days=5",
    )
    monkeypatch.delenv("V20_MEWS_API_KEY", raising=False)
    monkeypatch.delenv("TRADING_API_KEY", raising=False)

    with pytest.raises(ValueError, match="V20_MEWS_API_KEY"):
        PublishedMewsSnapshotClient.from_environment()


async def test_published_mews_is_normalized_to_an_idempotent_v20_snapshot() -> None:
    client = _client(_document())

    first = await client.fetch_snapshot(
        source_trade_date=date(2026, 8, 31),
        availability_date=date(2026, 9, 1),
    )
    second = await client.fetch_snapshot(
        source_trade_date=date(2026, 8, 31),
        availability_date=date(2026, 9, 1),
    )

    assert first == second
    assert first["fast_state"] == "DANGER"
    assert first["model_version"] == "mews_v2"
    assert first["source_trade_date"] == "2026-08-31"
    assert first["generated_at"].startswith("2026-09-01T09:30:00")
    assert len(first["data_version"]) == 64
    assert first["evidence"]["signal_available_date"] == "2026-09-01"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"date": "2026-08-28"}, "stale"),
        ({"signal_available_date": "2026-09-02"}, "today's session"),
        ({"data_status": "PARTIAL"}, "data_status"),
        ({"risk_state": "UNKNOWN"}, "risk_state"),
        ({"updated_at": 1_788_224_399_000}, "09:10 release"),
    ],
)
async def test_published_mews_rejects_stale_or_unqualified_material(
    overrides,
    message,
) -> None:
    client = _client(_document(**overrides))

    with pytest.raises(MewsSnapshotSourceError, match=message):
        await client.fetch_snapshot(
            source_trade_date=date(2026, 8, 31),
            availability_date=date(2026, 9, 1),
        )


async def test_published_mews_rejects_wrong_model_version() -> None:
    document = _document()
    document["version"] = "mews_v1"

    with pytest.raises(MewsSnapshotSourceError, match="version"):
        await _client(document).fetch_snapshot(
            source_trade_date=date(2026, 8, 31),
            availability_date=date(2026, 9, 1),
        )


async def test_refresh_calls_derived_production_endpoint_with_trading_key() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"success": True, "result": {"status": "OK"}})

    client = PublishedMewsSnapshotClient(
        "http://mews.internal/api/trading/margin-risk-curve?days=5",
        api_key="refresh-secret",
        transport=httpx.MockTransport(handler),
    )

    await client.refresh_missing_snapshot()

    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert requests[0].url.path == "/api/trading/margin-risk-refresh"
    assert requests[0].url.query == b""
    assert requests[0].headers["X-API-Key"] == "refresh-secret"


@pytest.mark.parametrize("status_code", [409, 503])
async def test_refresh_busy_or_incomplete_remains_retryable(status_code: int) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={"detail": "not ready"})

    client = PublishedMewsSnapshotClient(
        "http://mews.internal/api/trading/margin-risk-curve?days=5",
        api_key="test-key",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(MewsSnapshotNotReady, match=f"HTTP {status_code}"):
        await client.refresh_missing_snapshot()
