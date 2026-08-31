from __future__ import annotations

from collections import deque
from datetime import date

import httpx
import pytest

from src.data.clients.iquant_historical_adapter import (
    IQuantHistoricalAdapter,
    IQuantHistoricalAdapterError,
)


class _Realtime:
    async def as_ifind_format(self, *_args, **_kwargs):
        return {}


class _Response:
    def __init__(self, payload=None, *, error: Exception | None = None) -> None:
        self.payload = payload
        self.error = error

    def raise_for_status(self) -> None:
        if self.error is not None:
            raise self.error

    def json(self):
        return self.payload


class _Client:
    responses: deque[_Response] = deque()
    posts: list[str] = []
    tokens: list[str] = []

    def __init__(self, **_kwargs) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args) -> None:
        return None

    async def post(self, _url, *, json):
        self.posts.append(json["params"]["trade_date"])
        self.tokens.append(json["token"])
        return self.responses.popleft()


def _success(trade_date: str, close: float = 10.0) -> _Response:
    return _Response(
        {
            "code": 0,
            "data": {
                "fields": ["ts_code", "open", "high", "low", "close", "vol"],
                "items": [["000001.SZ", 9.8, 10.2, 9.7, close, 1000.0]],
            },
        }
    )


async def test_explicit_token_bypasses_legacy_resolver_and_reaches_daily_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.data.clients.iquant_historical_adapter.httpx.AsyncClient", _Client)
    monkeypatch.setattr(
        "src.common.config.get_tushare_token",
        lambda: pytest.fail("legacy token resolver must not be called"),
    )
    _Client.posts = []
    _Client.tokens = []
    _Client.responses = deque([_success("20260828")])
    adapter = IQuantHistoricalAdapter(_Realtime(), tushare_token="environment-token")

    await adapter._ensure_daily_range("2026-08-28", "2026-08-28")

    assert _Client.tokens == ["environment-token"]


async def test_failed_daily_dates_are_not_cached_and_are_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.data.clients.iquant_historical_adapter.httpx.AsyncClient", _Client)
    monkeypatch.setattr("src.common.config.get_tushare_token", lambda: "token")
    _Client.posts = []
    _Client.responses = deque(
        [
            _success("20260827"),
            _Response(
                error=httpx.ConnectError(
                    "offline", request=httpx.Request("POST", "https://api.tushare.pro")
                )
            ),
        ]
    )
    adapter = IQuantHistoricalAdapter(_Realtime())

    with pytest.raises(IQuantHistoricalAdapterError, match="1 requested dates"):
        await adapter._ensure_daily_range("2026-08-27", "2026-08-28")

    assert "2026-08-27" in adapter._daily_data
    assert "2026-08-28" not in adapter._daily_data

    _Client.responses = deque([_success("20260828", close=10.1)])
    await adapter._ensure_daily_range("2026-08-27", "2026-08-28")

    assert _Client.posts == ["20260827", "20260828", "20260828"]
    assert adapter._daily_data["2026-08-28"]["000001"]["close"] == 10.1


@pytest.mark.parametrize(
    "payload",
    [
        {"code": 10001, "msg": "permission denied"},
        [],
        {"code": 0, "data": []},
        {"code": 0, "data": {"fields": ["ts_code"], "items": []}},
    ],
)
async def test_api_and_schema_failures_never_become_empty_cached_days(
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
) -> None:
    monkeypatch.setattr("src.data.clients.iquant_historical_adapter.httpx.AsyncClient", _Client)
    monkeypatch.setattr("src.common.config.get_tushare_token", lambda: "token")
    _Client.posts = []
    _Client.responses = deque([_Response(payload)])
    adapter = IQuantHistoricalAdapter(_Realtime())

    with pytest.raises(IQuantHistoricalAdapterError):
        await adapter._ensure_daily_range("2026-08-28", "2026-08-28")

    assert "2026-08-28" not in adapter._daily_data


async def test_empty_open_date_is_not_cached_and_is_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.data.clients.iquant_historical_adapter.httpx.AsyncClient", _Client)
    monkeypatch.setattr("src.common.config.get_tushare_token", lambda: "token")
    _Client.posts = []
    empty = _Response(
        {
            "code": 0,
            "data": {
                "fields": ["ts_code", "open", "high", "low", "close", "vol"],
                "items": [],
            },
        }
    )
    _Client.responses = deque([empty])
    adapter = IQuantHistoricalAdapter(_Realtime())
    adapter.set_exchange_trade_calendar([date(2026, 8, 28)])

    with pytest.raises(IQuantHistoricalAdapterError, match="EMPTY_OPEN_DATE"):
        await adapter._ensure_daily_range("2026-08-28", "2026-08-28")
    assert "2026-08-28" not in adapter._daily_data

    _Client.responses = deque([_success("20260828", close=10.1)])
    await adapter._ensure_daily_range("2026-08-28", "2026-08-28")

    assert _Client.posts == ["20260828", "20260828"]
    assert adapter._daily_data["2026-08-28"]["000001"]["close"] == 10.1


async def test_empty_confirmed_closed_weekday_is_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.data.clients.iquant_historical_adapter.httpx.AsyncClient", _Client)
    monkeypatch.setattr("src.common.config.get_tushare_token", lambda: "token")
    _Client.posts = []
    _Client.responses = deque(
        [
            _Response(
                {
                    "code": 0,
                    "data": {
                        "fields": ["ts_code", "open", "high", "low", "close", "vol"],
                        "items": [],
                    },
                }
            )
        ]
    )
    adapter = IQuantHistoricalAdapter(_Realtime())
    adapter.set_exchange_trade_calendar([date(2026, 8, 27)])

    await adapter._ensure_daily_range("2026-08-28", "2026-08-28")
    await adapter._ensure_daily_range("2026-08-28", "2026-08-28")

    assert _Client.posts == ["20260828"]
    assert adapter._daily_data["2026-08-28"] == {}


async def test_boolean_vendor_row_is_dropped_without_erasing_valid_sibling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.data.clients.iquant_historical_adapter.httpx.AsyncClient", _Client)
    monkeypatch.setattr("src.common.config.get_tushare_token", lambda: "token")
    _Client.responses = deque(
        [
            _Response(
                {
                    "code": 0,
                    "data": {
                        "fields": ["ts_code", "open", "high", "low", "close", "vol"],
                        "items": [
                            ["000001.SZ", 9.8, 10.2, 9.7, True, 1000.0],
                            ["000002.SZ", 9.8, 10.2, 9.7, 10.1, 2000.0],
                        ],
                    },
                }
            )
        ]
    )
    adapter = IQuantHistoricalAdapter(_Realtime())
    adapter.set_exchange_trade_calendar([date(2026, 8, 28)])

    await adapter._ensure_daily_range("2026-08-28", "2026-08-28")

    assert "000001" not in adapter._daily_data["2026-08-28"]
    assert adapter._daily_data["2026-08-28"]["000002"]["close"] == 10.1


@pytest.mark.parametrize("reverse", [False, True])
async def test_conflicting_duplicate_vendor_rows_are_order_independent_and_local(
    monkeypatch: pytest.MonkeyPatch,
    reverse: bool,
) -> None:
    monkeypatch.setattr("src.data.clients.iquant_historical_adapter.httpx.AsyncClient", _Client)
    monkeypatch.setattr("src.common.config.get_tushare_token", lambda: "token")
    conflicting = [
        ["000001.SZ", 9.8, 10.2, 9.7, 10.0, 1000.0],
        ["000001.SZ", 9.8, 10.3, 9.7, 10.1, 1000.0],
    ]
    if reverse:
        conflicting.reverse()
    _Client.responses = deque(
        [
            _Response(
                {
                    "code": 0,
                    "data": {
                        "fields": ["ts_code", "open", "high", "low", "close", "vol"],
                        "items": [
                            *conflicting,
                            ["000002.SZ", 9.8, 10.2, 9.7, 10.1, 2000.0],
                        ],
                    },
                }
            )
        ]
    )
    adapter = IQuantHistoricalAdapter(_Realtime())
    adapter.set_exchange_trade_calendar([date(2026, 8, 28)])

    await adapter._ensure_daily_range("2026-08-28", "2026-08-28")

    assert set(adapter._daily_data["2026-08-28"]) == {"000002"}


async def test_identical_duplicate_vendor_rows_are_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.data.clients.iquant_historical_adapter.httpx.AsyncClient", _Client)
    monkeypatch.setattr("src.common.config.get_tushare_token", lambda: "token")
    row = ["000001.SZ", 9.8, 10.2, 9.7, 10.0, 1000.0]
    _Client.responses = deque(
        [
            _Response(
                {
                    "code": 0,
                    "data": {
                        "fields": ["ts_code", "open", "high", "low", "close", "vol"],
                        "items": [row, list(row)],
                    },
                }
            )
        ]
    )
    adapter = IQuantHistoricalAdapter(_Realtime())
    adapter.set_exchange_trade_calendar([date(2026, 8, 28)])

    await adapter._ensure_daily_range("2026-08-28", "2026-08-28")

    assert adapter._daily_data["2026-08-28"]["000001"]["close"] == 10.0
