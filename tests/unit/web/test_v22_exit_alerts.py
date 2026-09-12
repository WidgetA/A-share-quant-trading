from dataclasses import replace
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import httpx
import pytest
from fastapi import FastAPI

from src.strategy.v22_slim.exits import Bar, evaluate_day, recovery_failure, strong_at_1456
from src.web.v20_routes import create_v20_router
from src.web.v22_exit_monitor import V22ExitMonitor

TZ = ZoneInfo("Asia/Shanghai")
DAY = date(2026, 9, 8)


def bar(clock="09:31", price=100, *, day=DAY):
    at = datetime.fromisoformat(f"{day}T{clock}:00").replace(tzinfo=TZ)
    return Bar(at, price, price, price, price, 100, price * 100)


def evaluate(bars, **changes):
    args = dict(
        phase=1,
        now=bar("15:00").at,
        entry=100,
        prior=[],
        pre_close=100,
        before_factor=1,
        up_limit=110,
    )
    return evaluate_day(bars, **(args | changes))


@pytest.mark.parametrize(
    "phase,price,danger,reason",
    [
        (0, 80, False, None),
        (1, 92, False, "D1_STOP"),
        (1, 92.01, False, None),
        (2, 88, False, "D2_STOP"),
        (2, 95, True, "D2_RISK_STOP"),
        (2, 95, False, None),
    ],
)
def test_frozen_stop_boundaries(phase, price, danger, reason):
    signal, _ = evaluate([bar(price=price)], phase=phase, danger=danger, now=bar("10:00").at)
    assert (signal.reason if signal else None) == reason


def test_stop_uses_completed_close_and_valid_volume_not_low():
    quote = replace(bar(), low=80)
    assert evaluate([quote])[0] is None
    assert evaluate([bar("10:01", 80)], now=bar("10:00").at)[0] is None
    assert evaluate([replace(bar(price=80), volume=0, amount=0)])[0] is None
    assert evaluate([bar("09:30", 90)])[0].reason == "D1_STOP"
    assert evaluate([bar("14:57", 80)], phase=2)[0].reason == "D2_PLAN"


def rebound_bars():
    prices = [100, 99.5, 99, 98.5, 98, 97.5, 97, 97, 97, 97] + [105] * 12 + [101, 101]
    start = bar().at
    return [
        replace(bar(price=price), at=start + timedelta(minutes=i)) for i, price in enumerate(prices)
    ]


def recovery(bars, **changes):
    return recovery_failure(
        bars, **(dict(before_factor=1, pre_close=100, entry=100, up_limit=110, atr=0.02) | changes)
    )


def test_rebound_failure_can_trigger_while_still_profitable():
    bars = rebound_bars()
    signal = recovery(bars)
    assert signal is not None and signal.reason == "RECOVERY_FAILED"
    assert signal.at == bars[-1].at and signal.price == 101
    assert recovery(bars[:-1]) is None
    assert recovery(bars, up_limit=101) is None
    assert recovery(bars, pre_close=99) is None
    assert recovery(bars, atr=None) is None


@pytest.mark.parametrize("kind", ["gap", "lunch", "bad_volume"])
def test_rebound_state_does_not_survive_unknown_or_discontinuity(kind):
    bars = rebound_bars()
    if kind == "gap":
        bars[-2:] = [replace(b, at=b.at + timedelta(minutes=1)) for b in bars[-2:]]
    elif kind == "lunch":
        bars[-2:] = [
            replace(b, at=bar("13:01").at + timedelta(minutes=i)) for i, b in enumerate(bars[-2:])
        ]
    else:
        bars[1] = replace(bars[1], amount=1)
    assert recovery(bars) is None


def strong_inputs():
    prior = [dict(open=10, high=10.1, low=9.9, close=10, pre_close=10)] * 3
    start = bar("09:30", 10.2).at
    times = [start + timedelta(minutes=i) for i in range(121)]
    times += [bar("13:01").at + timedelta(minutes=i) for i in range(116)]
    bars = [replace(bar(price=10.2 + 0.8 * i / 236), at=at) for i, at in enumerate(times)]
    return bars, prior


def test_strong_requires_exact_prefix_and_never_uses_full_day_candle():
    bars, prior = strong_inputs()
    assert strong_at_1456(bars, prior, 10)
    assert not strong_at_1456(bars[1:], prior, 10)
    assert not strong_at_1456([*bars, bars[-1]], prior, 10)
    assert strong_at_1456([*bars, bar("15:00", 1)], prior, 10)
    signal, extended = evaluate(
        [*bars, bar("15:00", 1)], phase=2, entry=10, prior=prior, pre_close=10
    )
    assert signal is None and extended
    signal, _ = evaluate([bar()], phase=3, extended=True)
    assert signal.reason == "D3_PLAN"
    assert evaluate([], phase=3, extended=True)[0] is None
    assert evaluate([bar("09:30")], phase=3, extended=True)[0].at == bar("09:30").at
    assert evaluate([bar("12:00")], phase=3, extended=True)[0] is None


def test_earlier_stop_cannot_be_cancelled_by_strong_and_plan_has_no_fake_price():
    bars, prior = strong_inputs()
    bars[2] = replace(bars[2], open=8, high=8, low=8, close=8, amount=800)
    assert evaluate(bars, phase=2, entry=10, prior=prior, pre_close=10)[0].reason == "D2_STOP"
    signal, _ = evaluate([bar("15:00", 101)], phase=2)
    assert signal.reason == "D2_PLAN" and signal.price is None


def monitor(client):
    service = SimpleNamespace(
        _repository=SimpleNamespace(schema="v20"),
        config=SimpleNamespace(),
        _scan_state=SimpleNamespace(realtime_client=client),
    )
    return V22ExitMonitor(service)


@pytest.mark.parametrize("historical", [False, True])
async def test_current_and_historical_api_boundary_without_fallback(historical):
    client = SimpleNamespace(
        _api_call=AsyncMock(return_value={"data": {"fields": [], "items": []}})
    )
    subject = monitor(client)
    now = bar().at
    await subject.bars("000001", DAY - timedelta(days=1) if historical else DAY, now)
    assert [c.args[0] for c in client._api_call.call_args_list] == [
        "stk_mins" if historical else "rt_min_daily"
    ]


async def test_empty_reference_does_not_invent_cost_or_alert():
    subject = monitor(None)
    subject.bars = AsyncMock(return_value=[])
    subject.store.apply = AsyncMock()
    position = dict(entry_date="2026-09-07", code="000001", entry_price=None)
    await subject.position(position, tuple(date(2026, 9, d) for d in range(7, 12)), bar().at)
    subject.store.apply.assert_not_awaited()


async def test_reference_is_0941_open_and_no_same_day_exit():
    subject = monitor(None)
    subject.bars = AsyncMock(
        return_value=[bar("09:40", 110), replace(bar("09:41", 100), close=99, low=99)]
    )
    subject.store.apply = AsyncMock()
    position = dict(entry_date=str(DAY), code="000001", entry_price=None)
    await subject.position(
        position, tuple(DAY + timedelta(days=i) for i in range(5)), bar("10:00").at
    )
    subject.store.apply.assert_awaited_once_with(position, reference=100)


async def test_persisted_strong_extension_is_not_rewritten_after_restart():
    subject = monitor(None)
    subject.bars = AsyncMock(return_value=[bar(day=date(2026, 9, 10))])
    subject.daily = AsyncMock(return_value=[])
    subject.danger = AsyncMock(return_value=False)
    subject.limits = AsyncMock(return_value=None)
    subject.store.apply = AsyncMock(return_value=None)
    position = dict(
        entry_date="2026-09-07", code="000001", entry_price=100, extended=True, revision=1
    )
    await subject.position(
        position, tuple(date(2026, 9, d) for d in range(7, 12)), bar(day=date(2026, 9, 10)).at
    )
    assert [call.args[1] for call in subject.bars.call_args_list] == [date(2026, 9, 10)]
    assert subject.store.apply.call_args.kwargs["signal"].reason == "D3_PLAN"


async def test_calibration_api_auth_validation_and_preserves_omitted_fields(monkeypatch):
    monkeypatch.setenv("V20_INGEST_API_KEY", "test-key")
    service = SimpleNamespace(
        list_v22_positions=AsyncMock(return_value=[]),
        calibrate_v22_position=AsyncMock(return_value={"quantity": 300}),
    )
    app = FastAPI()
    app.state.v20_service = service
    app.include_router(create_v20_router())
    headers = {"X-V20-API-Key": "test-key", "Idempotency-Key": "calibrate-001"}
    path = "/api/v20/v22-positions/p1/calibrate"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get("/api/v20/v22-positions")).status_code == 401
        assert (await client.get("/api/v20/v22-positions", headers=headers)).json() == []
        for change in (
            {},
            {"quantity": -1},
            {"quantity": 1.5},
            {"entry_price": 0},
            {"entry_price": None},
            {"status": "SOLD"},
        ):
            result = await client.post(
                path, headers=headers, json={"expected_revision": 0, **change}
            )
            assert result.status_code == 422
        result = await client.post(
            path, headers=headers, json={"expected_revision": 0, "quantity": 300}
        )
        assert result.status_code == 200
    service.calibrate_v22_position.assert_awaited_once_with(
        "p1", "calibrate-001", {"expected_revision": 0, "quantity": 300}
    )
