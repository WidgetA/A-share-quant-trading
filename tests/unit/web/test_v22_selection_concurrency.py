"""Selection fan-out must be local to V22's early-minute acquisition."""

import asyncio
from datetime import date
from unittest.mock import AsyncMock

import pytest

from src.data.clients.tushare_realtime import TushareRealtimeClient
from src.web.v20_canonical_selection import V20CanonicalSelectionState
from src.web.v20_service import (
    _cleanup_v20_scan_resources,
    _init_v20_scan_resources_with_token,
)


async def _initialize(monkeypatch: pytest.MonkeyPatch, version: str):
    async def start(client: TushareRealtimeClient) -> None:
        client._client = AsyncMock()

    monkeypatch.setattr(TushareRealtimeClient, "start", start)
    monkeypatch.setattr(
        "src.data.sources.local_concept_mapper.LocalConceptMapper", lambda: object()
    )
    state = V20CanonicalSelectionState(
        selection_version=version,
        fundamentals_db=AsyncMock(),
    )
    await _init_v20_scan_resources_with_token(state, "test-token")
    return state


async def _assert_rolling_limit(
    monkeypatch: pytest.MonkeyPatch,
    client: TushareRealtimeClient,
    *,
    early: bool,
    expected: int,
) -> None:
    entered: asyncio.Queue[asyncio.Event] = asyncio.Queue()
    releases: list[asyncio.Event] = []
    active = 0
    peak = 0

    async def api(api_name, params, **kwargs):
        nonlocal active, peak
        assert api_name == ("rt_min_daily" if early else "rt_min")
        release = asyncio.Event()
        releases.append(release)
        active += 1
        peak = max(peak, active)
        entered.put_nowait(release)
        try:
            await release.wait()
            return {"data": {"fields": [], "items": []}}
        finally:
            active -= 1

    monkeypatch.setattr(client, "_api_call", api)
    monkeypatch.setattr(client, "BATCH_SIZE", 1)
    codes = [f"{index:06d}" for index in range(1, expected + 4)]
    call = (
        client.batch_get_early_market_data(codes, date(2026, 11, 2))
        if early
        else client.batch_get_quotes(codes)
    )
    task = asyncio.create_task(call)
    try:
        first = [await asyncio.wait_for(entered.get(), 1) for _ in range(expected)]
        await asyncio.sleep(0)
        assert entered.empty(), "fan-out exceeded the requested limit"
        assert active == expected
        first[0].set()
        # One free slot starts the next request while all other workers remain blocked.
        await asyncio.wait_for(entered.get(), 1)
        assert not first[1].is_set()
        assert peak == expected
    finally:
        task.cancel()
        for release in releases:
            release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("version, expected", [("V22-slim", 10), ("V20", 5)])
async def test_initialized_selection_uses_its_own_rolling_limit(
    monkeypatch: pytest.MonkeyPatch, version: str, expected: int
) -> None:
    state = await _initialize(monkeypatch, version)
    try:
        await _assert_rolling_limit(
            monkeypatch, state.realtime_client, early=True, expected=expected
        )
        assert TushareRealtimeClient.MAX_CONCURRENCY == 5
    finally:
        await _cleanup_v20_scan_resources(state)


@pytest.mark.asyncio
async def test_v22_snapshot_calls_stay_at_five_and_shutdown_is_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    other = await _initialize(monkeypatch, "V20")
    v22 = await _initialize(monkeypatch, "V22-slim")
    other_client = other.realtime_client
    other_fundamentals = other.fundamentals_db
    try:
        await _assert_rolling_limit(monkeypatch, v22.realtime_client, early=False, expected=5)
        await _cleanup_v20_scan_resources(v22)
        assert other.realtime_client is other_client
        assert other_client._client is not None
        other_fundamentals.close.assert_not_awaited()
        await _assert_rolling_limit(monkeypatch, other_client, early=True, expected=5)
    finally:
        if v22.initialized:
            await _cleanup_v20_scan_resources(v22)
        await _cleanup_v20_scan_resources(other)


@pytest.mark.asyncio
async def test_v22_initialization_failure_preserves_other_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    other = await _initialize(monkeypatch, "V20")
    other_client = other.realtime_client
    other_fundamentals = other.fundamentals_db

    def fail_adapter(*args, **kwargs):
        raise RuntimeError("synthetic historical-adapter construction failure")

    monkeypatch.setattr(
        "src.data.clients.iquant_historical_adapter.IQuantHistoricalAdapter", fail_adapter
    )
    failed_fundamentals = AsyncMock()
    failed = V20CanonicalSelectionState(
        selection_version="V22-slim", fundamentals_db=failed_fundamentals
    )
    try:
        with pytest.raises(RuntimeError, match="synthetic historical-adapter"):
            await _init_v20_scan_resources_with_token(failed, "test-token")
        assert failed.initialized is False
        assert failed.realtime_client is None
        failed_fundamentals.close.assert_awaited_once()
        assert other.realtime_client is other_client
        assert other_client._client is not None
        other_fundamentals.close.assert_not_awaited()
        await _assert_rolling_limit(monkeypatch, other_client, early=True, expected=5)
    finally:
        await _cleanup_v20_scan_resources(other)
