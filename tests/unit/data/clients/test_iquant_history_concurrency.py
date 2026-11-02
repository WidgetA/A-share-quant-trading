"""Behavior checks for rolling, market-wide daily-history acquisition."""

from __future__ import annotations

import asyncio
from collections import Counter
from datetime import date
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from src.data.clients.iquant_historical_adapter import (
    IQuantHistoricalAdapter,
    IQuantHistoricalAdapterError,
)

DATES = (
    "20261019",
    "20261020",
    "20261021",
    "20261022",
    "20261023",
    "20261026",
    "20261027",
    "20261028",
)
FIELDS = ["ts_code", "open", "high", "low", "close", "pre_close", "vol"]


def _iso(day: str) -> str:
    return f"{day[:4]}-{day[4:6]}-{day[6:]}"


def _market_row(day: str, symbol_index: int) -> list[Any]:
    opening = 10.0 + int(day[-2:]) + symbol_index / 10000
    symbol = "600000.SH" if symbol_index == 0 else f"{symbol_index:06d}.SZ"
    return [
        symbol,
        opening,
        opening + 2,
        opening - 1,
        opening + 1,
        opening - 0.5,
        100 + symbol_index,
    ]


class _Response:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self.payload


class _ControlledClient:
    """Gate each requested date, so rolling progress needs no timing guesses."""

    def __init__(
        self,
        days: tuple[str, ...],
        *,
        market_size: int = 2,
        blocked: bool = True,
        failed_day: str | None = None,
        failure_kind: str = "api",
    ) -> None:
        self.market_size = market_size
        self.gates = {day: asyncio.Event() for day in days}
        if not blocked:
            self.release_all()
        self.failed_day = failed_day
        self.failure_kind = failure_kind
        self.started: list[str] = []
        self.completed: list[str] = []
        self.cancelled: list[str] = []
        self.requests: list[dict[str, Any]] = []
        self.active = 0
        self.maximum = 0
        self.changed = asyncio.Event()
        self.active_on_close: list[int] = []

    async def __aenter__(self) -> _ControlledClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        self.active_on_close.append(self.active)

    def release_all(self, *, except_day: str | None = None) -> None:
        for day, gate in self.gates.items():
            if day != except_day:
                gate.set()

    async def wait_count(self, attribute: str, count: int) -> None:
        async def wait() -> None:
            while len(getattr(self, attribute)) < count:
                self.changed.clear()
                await self.changed.wait()

        try:
            await asyncio.wait_for(wait(), timeout=2)
        except TimeoutError as exc:
            raise AssertionError(
                f"expected {count} {attribute} date requests, observed "
                f"{len(getattr(self, attribute))}; started={self.started}"
            ) from exc

    async def post(self, url: str, *, json: dict[str, Any]) -> _Response:
        day = json["params"]["trade_date"]
        self.requests.append({"url": url, "body": json})
        self.started.append(day)
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        self.changed.set()
        try:
            await self.gates[day].wait()
            if day == self.failed_day:
                if self.failure_kind == "transport":
                    raise httpx.ConnectError(
                        "test transport failure", request=httpx.Request("POST", url)
                    )
                return _Response({"code": 10001, "msg": "test API failure"})
            return _Response(
                {
                    "code": 0,
                    "data": {
                        "fields": FIELDS,
                        "items": [_market_row(day, index) for index in range(self.market_size)],
                    },
                }
            )
        except asyncio.CancelledError:
            self.cancelled.append(day)
            raise
        finally:
            self.active -= 1
            self.completed.append(day)
            self.changed.set()


def _adapter(monkeypatch: pytest.MonkeyPatch, client: _ControlledClient) -> IQuantHistoricalAdapter:
    monkeypatch.setattr(
        "src.data.clients.iquant_historical_adapter.httpx.AsyncClient", lambda **_kwargs: client
    )
    adapter = IQuantHistoricalAdapter(
        SimpleNamespace(as_ifind_format=lambda *_args: None), tushare_token="test-only-token"
    )
    adapter.set_exchange_trade_calendar(
        tuple(date.fromisoformat(_iso(day)) for day in sorted(client.gates))
    )
    return adapter


async def _drain(task: asyncio.Task[Any], client: _ControlledClient) -> None:
    client.release_all()
    if not task.done():
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_five_rolling_dates_preserve_full_market_and_chronological_output(
    monkeypatch,
) -> None:
    # More than 3,700 returned securities catches an accidental market-size cap.
    client = _ControlledClient(DATES, market_size=3802)
    adapter = _adapter(monkeypatch, client)
    task = asyncio.create_task(
        adapter.history_quotes(
            "000001.SZ,600000.SH", "open,high,low,close,volume", _iso(DATES[0]), _iso(DATES[-1])
        )
    )
    try:
        await client.wait_count("started", 5)
        assert client.maximum == 5
        slow_day = client.started[0]
        released_day = client.started[1]
        client.gates[released_day].set()

        await client.wait_count("started", 6)
        assert slow_day not in client.completed, "the sixth date must replace a completed slot"
        assert client.completed == [released_day]
        assert client.active == 5

        client.release_all(except_day=slow_day)
        await client.wait_count("completed", len(DATES) - 1)
        assert not task.done(), "the slow date still contributes required history"
        client.gates[slow_day].set()
        result = await task
    finally:
        await _drain(task, client)

    assert client.maximum == 5
    assert Counter(client.started) == Counter(DATES)
    assert client.active_on_close == [0]
    for request in client.requests:
        body = request["body"]
        assert body["api_name"] == "daily"
        assert body["params"] == {"trade_date": body["params"]["trade_date"]}
        assert "limit" not in body and "offset" not in body

    for day in DATES:
        cached = adapter._daily_data[_iso(day)]
        assert len(cached) == client.market_size
        for symbol_index in range(client.market_size):
            row = _market_row(day, symbol_index)
            assert cached[row[0][:6]] == {
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                "volume": float(row[6]),
            }

    assert [table["thscode"] for table in result["tables"]] == ["000001.SZ", "600000.SH"]
    for table, symbol_index in zip(result["tables"], (1, 0), strict=True):
        expected = [_market_row(day, symbol_index) for day in DATES]
        assert table["table"] == {
            "time": [_iso(day) for day in DATES],
            "open": [row[1] for row in expected],
            "high": [row[2] for row in expected],
            "low": [row[3] for row in expected],
            "close": [row[4] for row in expected],
            "volume": [float(row[6]) * 100 for row in expected],
        }
    requests_before = len(client.requests)
    again = await adapter.history_quotes(
        "000001.SZ,600000.SH", "open,high,low,close,volume", _iso(DATES[0]), _iso(DATES[-1])
    )
    assert again == result
    assert len(client.requests) == requests_before, "cached dates must not be re-downloaded"


@pytest.mark.parametrize("failure_kind", ["api", "transport"])
async def test_failed_date_is_uncached_siblings_complete_and_only_failure_is_retried(
    monkeypatch, failure_kind
) -> None:
    failed_day = DATES[2]
    client = _ControlledClient(
        DATES, blocked=False, failed_day=failed_day, failure_kind=failure_kind
    )
    adapter = _adapter(monkeypatch, client)
    with pytest.raises(IQuantHistoricalAdapterError, match="1 requested dates"):
        await adapter.history_quotes("000001.SZ", "close,volume", _iso(DATES[0]), _iso(DATES[-1]))

    assert Counter(client.started) == Counter(DATES)
    assert _iso(failed_day) not in adapter._daily_data
    good_days = {day: adapter._daily_data[_iso(day)] for day in DATES if day != failed_day}
    assert len(good_days) == len(DATES) - 1
    assert client.active == 0

    client.failed_day = None
    before_retry = len(client.started)
    result = await adapter.history_quotes(
        "000001.SZ", "close,volume", _iso(DATES[0]), _iso(DATES[-1])
    )

    assert client.started[before_retry:] == [failed_day]
    assert result["tables"][0]["table"]["time"] == [_iso(day) for day in DATES]
    assert all(adapter._daily_data[_iso(day)] is cache for day, cache in good_days.items())
    assert client.active_on_close == [0, 0]


async def test_cancel_drains_inflight_dates_before_client_closes_and_allows_retry(
    monkeypatch,
) -> None:
    client = _ControlledClient(DATES)
    adapter = _adapter(monkeypatch, client)
    task = asyncio.create_task(adapter._ensure_daily_range(_iso(DATES[0]), _iso(DATES[-1])))
    try:
        await client.wait_count("started", 5)
        started_before_cancel = tuple(client.started)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.active == 0
        assert Counter(client.cancelled) == Counter(started_before_cancel)
        assert client.active_on_close == [0]
        assert all(_iso(day) not in adapter._daily_data for day in DATES)
    finally:
        await _drain(task, client)

    client.release_all()
    before_retry = len(client.started)
    await adapter._ensure_daily_range(_iso(DATES[0]), _iso(DATES[-1]))
    assert Counter(client.started[before_retry:]) == Counter(DATES)
    assert all(_iso(day) in adapter._daily_data for day in DATES)
    assert client.maximum <= 5


async def test_simultaneous_date_range_calls_share_the_five_request_limit(monkeypatch) -> None:
    left = DATES[:4]
    right = DATES[4:]
    client = _ControlledClient(DATES)
    adapter = _adapter(monkeypatch, client)
    tasks = [
        asyncio.create_task(adapter._ensure_daily_range(_iso(days[0]), _iso(days[-1])))
        for days in (left, right)
    ]
    try:
        await client.wait_count("started", 5)
        assert client.active == client.maximum == 5
        client.release_all()
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            await _drain(task, client)
    assert client.maximum == 5
    assert Counter(client.started) == Counter(DATES)
