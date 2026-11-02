"""Default minute requests use five rolling slots without reducing the code set."""

from __future__ import annotations

import asyncio
from collections import Counter
from datetime import date
from typing import Any

import pytest

from src.data.clients.tushare_realtime import TushareRealtimeClient

DAY = date(2026, 8, 31)


class _ControlledApi:
    def __init__(self) -> None:
        self.started: list[str] = []
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.gates: dict[str, asyncio.Event] = {}
        self.changed = asyncio.Event()
        self.active = 0
        self.maximum = 0
        self.released = False

    async def wait_count(self, count: int) -> None:
        async def wait() -> None:
            while len(self.started) < count:
                self.changed.clear()
                await self.changed.wait()

        await asyncio.wait_for(wait(), timeout=2)

    def release_all(self) -> None:
        self.released = True
        for gate in self.gates.values():
            gate.set()

    async def __call__(self, api: str, params: dict[str, Any], fields: str = "") -> dict:
        del fields
        key = params["ts_code"]
        self.requests.append((api, dict(params)))
        self.started.append(key)
        gate = self.gates.setdefault(key, asyncio.Event())
        if self.released:
            gate.set()
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        self.changed.set()
        try:
            await gate.wait()
            time_column = "trade_time" if api == "stk_mins" else "time"
            labels = ["09:39"] if api == "rt_min" else [f"09:{i:02}" for i in range(30, 40)]
            return {
                "data": {
                    "fields": [
                        "ts_code",
                        time_column,
                        "open",
                        "close",
                        "high",
                        "low",
                        "vol",
                        "amount",
                    ],
                    "items": [
                        [code, f"{DAY} {label}:00", 10.0, 10.1, 10.2, 9.9, 100.0, 1000.0]
                        for code in key.split(",")
                        for label in labels
                    ],
                }
            }
        finally:
            self.active -= 1
            self.changed.set()


@pytest.mark.parametrize("entry", ["early", "latest", "history", "historical"])
async def test_default_minute_requests_have_five_rolling_slots_and_keep_all_codes(
    monkeypatch: pytest.MonkeyPatch, entry: str
) -> None:
    client = TushareRealtimeClient("test-token")
    client._client = object()  # type: ignore[assignment]
    api = _ControlledApi()
    monkeypatch.setattr(client, "_api_call", api)
    # Exercise the real default 500-code rt_min batching, not a test-only batch size.
    codes = [f"{i:06}" for i in range(1, 2502 if entry == "latest" else 9)]
    if entry == "early":
        operation = client.batch_get_early_market_data(codes, expected_trade_date=DAY)
    elif entry == "latest":
        operation = client.batch_get_latest_minute_bars(codes)
    elif entry == "history":
        operation = client.batch_get_minute_history(codes)
    else:
        operation = client.batch_get_minute_history_for_date(codes, DAY)
    task = asyncio.create_task(operation)
    try:
        await api.wait_count(5)
        assert api.active == api.maximum == 5
        assert len(api.started) == 5, "a sixth default request must wait for a free slot"
        slow_first = api.started[0]
        api.gates[api.started[1]].set()
        await api.wait_count(6)
        assert not api.gates[slow_first].is_set(), "the next request must not wait for a batch"
        assert api.maximum == 5
        api.release_all()
        result = await task
    finally:
        api.release_all()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert set(result) == set(codes)
    assert api.active == 0
    assert api.maximum == 5
    expected_api = (
        "stk_mins" if entry == "historical" else ("rt_min" if entry == "latest" else "rt_min_daily")
    )
    assert {name for name, _ in api.requests} == {expected_api}
    requested_codes = [
        code.split(".")[0] for _, params in api.requests for code in params["ts_code"].split(",")
    ]
    assert Counter(requested_codes) == Counter(codes)
    if entry == "early":
        assert len(api.requests) == len(codes)
        assert all(
            "," not in params["ts_code"] and params["freq"] == "1MIN" for _, params in api.requests
        )
