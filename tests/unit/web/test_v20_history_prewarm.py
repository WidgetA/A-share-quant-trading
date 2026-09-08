"""Regression coverage for historical work deferred until the Sep 8 live scan."""

import asyncio
from datetime import date, datetime, time, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from zoneinfo import ZoneInfo

import httpx
import pytest

from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter
from src.data.database.v20_repository import V20RepositoryError
from src.web import v20_service as module
from src.web.v20_canonical_selection import _fetch_history_ohlcv
from src.web.v20_service import V20Service, _DayContext

TZ = ZoneInfo("Asia/Shanghai")
DAY = date(2026, 9, 8)


def make_service(monkeypatch, wall=time(9, 15)):
    service = object.__new__(V20Service)
    service.config = SimpleNamespace(clock=SimpleNamespace(prewarm=time(9, 15)))
    service._clock = lambda: datetime.combine(DAY, wall, TZ)
    adapter = SimpleNamespace(set_exchange_trade_calendar=Mock())
    service._scan_state = SimpleNamespace(historical_adapter=adapter)
    monkeypatch.setattr(
        module,
        "derive_canonical_v16_universe",
        lambda state: (None, None, {}, ("000001", "600001")),
    )
    history = AsyncMock(return_value={"000001": {"close": [10.0]}})
    monkeypatch.setattr(module, "_fetch_history_ohlcv", history)
    calendar = (DAY - timedelta(days=100), DAY - timedelta(days=1), DAY)
    return service, _DayContext(DAY, calendar), history


async def test_prewarm_is_date_scoped_and_uses_only_v20_owned_history(monkeypatch):
    service, context, history = make_service(monkeypatch)
    legacy_adapter = SimpleNamespace(set_exchange_trade_calendar=Mock(), cache={})
    # Shared credentials do not imply shared mutable adapters or state.
    legacy_state = SimpleNamespace(historical_adapter=legacy_adapter)
    await service._prewarm_canonical_history(context)
    await service._prewarm_canonical_history(context)
    history.assert_awaited_once_with(
        service._scan_state.historical_adapter, ["000001", "600001"], DAY
    )
    assert context.canonical_history_warmed
    service._scan_state.historical_adapter.set_exchange_trade_calendar.assert_called_once_with(
        context.calendar
    )
    assert legacy_state.historical_adapter is legacy_adapter
    legacy_adapter.set_exchange_trade_calendar.assert_not_called()
    assert legacy_adapter.cache == {}
    # A flag on yesterday's context cannot suppress today's preparation.
    next_context = _DayContext(DAY + timedelta(days=1), context.calendar)
    service._clock = lambda: datetime(2026, 9, 9, 9, 15, tzinfo=TZ)
    await service._prewarm_canonical_history(next_context)
    assert history.await_count == 2


@pytest.mark.parametrize("wall", [time(9, 14, 59), time(9, 38), time(9, 39), time(10)])
async def test_prewarm_never_enters_realtime_acquisition_window(monkeypatch, wall):
    service, context, history = make_service(monkeypatch, wall)
    await service._prewarm_canonical_history(context)
    history.assert_not_awaited()
    assert not context.canonical_history_warmed


async def test_prewarm_failure_can_retry_and_never_claims_ready(monkeypatch):
    service, context, history = make_service(monkeypatch)
    history.side_effect = [RuntimeError("provider failure"), {}]
    with pytest.raises(RuntimeError, match="provider failure"):
        await service._prewarm_canonical_history(context)
    assert not context.canonical_history_warmed
    with pytest.raises(V20RepositoryError, match="empty history"):
        await service._prewarm_canonical_history(context)
    assert not context.canonical_history_warmed


async def test_prewarm_is_cancelled_at_absolute_0938_boundary(monkeypatch):
    service, context, history = make_service(monkeypatch)
    service._clock = lambda: datetime(2026, 9, 8, 9, 37, 59, 999000, tzinfo=TZ)
    settled = asyncio.Event()

    async def blocked(*args):
        try:
            await asyncio.Event().wait()
        finally:
            settled.set()

    history.side_effect = blocked
    with pytest.raises(TimeoutError):
        await service._prewarm_canonical_history(context)
    assert settled.is_set()
    assert not context.canonical_history_warmed


def test_partial_artifact_calendar_cannot_classify_older_weekdays_as_closed():
    adapter = SimpleNamespace(set_exchange_trade_calendar=Mock())
    calendar = (DAY - timedelta(days=60), DAY - timedelta(days=1), DAY)
    V20Service._configure_canonical_history_calendar(adapter, calendar, DAY)
    adapter.set_exchange_trade_calendar.assert_not_called()


async def test_collection_prepares_history_without_scanning_or_early_quotes(monkeypatch):
    service, context, history = make_service(monkeypatch)
    service.config.clock.decision_bar_label = "09:39"
    service.config.clock.decision_finalization_deadline = time(9, 45)
    service._repository = SimpleNamespace()
    service.kick_mews_for_selection_trigger = Mock(side_effect=AssertionError("too early"))
    service._scan_state.realtime_client = SimpleNamespace(
        batch_get_early_market_data=AsyncMock(side_effect=AssertionError("too early"))
    )
    scanner = AsyncMock(side_effect=AssertionError("must not scan in prewarm"))
    monkeypatch.setattr(module, "compute_canonical_v16_scan", scanner)
    await service._run_entry_collection_cycle(context, service._clock())
    history.assert_awaited_once()
    scanner.assert_not_awaited()
    service._scan_state.realtime_client.batch_get_early_market_data.assert_not_awaited()
    assert context.canonical_bundle is None


async def test_warm_and_cold_history_match_without_repeated_holiday_requests(monkeypatch):
    """Exercise the real adapter and batch loop that repeated June 19 in production."""

    codes = [f"{number:06d}" for number in range(1, 56)]  # two history batches
    calls = []

    class DailyClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def post(self, url, *, json):
            trade_date = json["params"]["trade_date"]
            assert json["api_name"] == "daily"
            assert trade_date < DAY.strftime("%Y%m%d")
            calls.append(trade_date)
            items = (
                []
                if trade_date == "20260619"
                else [[f"{code}.SZ", 10, 11, 9, 10.5, 1000] for code in codes]
            )
            return httpx.Response(
                200,
                request=httpx.Request("POST", url),
                json={
                    "code": 0,
                    "data": {
                        "fields": ["ts_code", "open", "high", "low", "close", "vol"],
                        "items": items,
                    },
                },
            )

    monkeypatch.setattr("src.data.clients.iquant_historical_adapter.httpx.AsyncClient", DailyClient)
    realtime = SimpleNamespace(as_ifind_format=AsyncMock())
    cold_adapter = IQuantHistoricalAdapter(realtime, tushare_token="test")
    cold = await _fetch_history_ohlcv(cold_adapter, codes, DAY)
    cold_holiday_calls = calls.count("20260619")
    assert cold_holiday_calls > 1

    calls.clear()
    service, context, _history = make_service(monkeypatch)
    adapter = IQuantHistoricalAdapter(realtime, tushare_token="test")
    service._scan_state.historical_adapter = adapter
    calendar = tuple(
        day
        for offset in range(100, -1, -1)
        if (day := DAY - timedelta(days=offset)).weekday() < 5 and day != date(2026, 6, 19)
    )
    context.calendar = calendar
    monkeypatch.setattr(module, "_fetch_history_ohlcv", _fetch_history_ohlcv)
    monkeypatch.setattr(
        module, "derive_canonical_v16_universe", lambda state: (None, None, {}, tuple(codes))
    )
    await service._prewarm_canonical_history(context)
    assert calls.count("20260619") == 1
    request_count = len(calls)
    warm = await _fetch_history_ohlcv(adapter, codes, DAY)
    assert warm == cold
    assert len(calls) == request_count  # no historical network I/O on the live path
