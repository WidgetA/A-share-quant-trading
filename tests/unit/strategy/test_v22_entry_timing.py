import asyncio
from dataclasses import replace
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

from src.data.clients.tushare_realtime import TushareMinuteBar
from src.strategy.v22_slim.entry_timing import (
    _fetch_tencent_index as fetch_public_index,
)
from src.strategy.v22_slim.entry_timing import (
    evaluate_entry_timing,
    matches_wait_rule,
    project_features,
)

TZ = ZoneInfo("Asia/Shanghai")
DAY = date(2026, 9, 23)
PRIOR = date(2026, 9, 22)
NOW = datetime(2026, 9, 23, 10, 0, tzinfo=TZ)
CODE = "600001"
SYMBOLS = [{"code": CODE, "name": "股票甲"}]


@pytest.fixture(autouse=True)
def no_real_index_network(monkeypatch):
    monkeypatch.setattr(
        "src.strategy.v22_slim.entry_timing._fetch_tencent_index", AsyncMock(return_value={})
    )


def tencent_payload(*, day="20260923", minutes=None, code="000001", prior="3952.13"):
    qt = ["1", "上证指数", code, "3936.52", prior, "3951.37"] + [""] * 24 + [day + "161401"]
    return {
        "code": 0,
        "msg": "",
        "data": {
            "sh000001": {
                "data": {
                    "date": day,
                    "data": ["0939 3955.00 10 20", "0940 3947.59 10 20", "1500 3936.52 10 20"]
                    if minutes is None
                    else minutes,
                },
                "qt": {"sh000001": qt},
            }
        },
    }


def prefix(closes=None, amounts=None):
    closes = closes or [100.0, 102.0, 101.0, 99.0, 98.0, 100.0, 101.0, 100.0, 101.0, 102.0]
    amounts = amounts or [10_000_000.0] * 10
    rows = []
    for minute, (close, amount) in enumerate(zip(closes, amounts, strict=True), 30):
        stamp = datetime(2026, 9, 23, 9, minute, tzinfo=TZ)
        rows.append(
            TushareMinuteBar(
                CODE,
                stamp,
                stamp.strftime("%H:%M"),
                close,
                close,
                close,
                close,
                amount / close,
                amount,
            )
        )
    return rows


def payload(rows):
    fields = list(rows[0]) if rows else ["ts_code", "time", "close"]
    return {"data": {"fields": fields, "items": [[row.get(key) for key in fields] for row in rows]}}


def minute(clock="09:40", close=2990.0, *, day=DAY, code="000001.SH"):
    return dict(
        ts_code=code,
        time=f"{day} {clock}:00",
        open=3000.0,
        high=3010.0,
        low=2980.0,
        close=close,
        vol=1000.0,
        amount=3_000_000.0,
    )


def feature(drop=2.0, amount=100_000_000.0, share=0.3):
    return {CODE: dict(max_drop_3m_pct=drop, prefix_amount_yuan=amount, last3_amount_share=share)}


def client_with(rows=None, daily=None):
    return SimpleNamespace(
        _api_call=AsyncMock(
            side_effect=[
                payload(rows if rows is not None else [minute()]),
                payload(
                    daily
                    if daily is not None
                    else [dict(ts_code="000001.SH", trade_date="20260922", close=3000.0)]
                ),
            ]
        )
    )


async def evaluate(client, *, features=None, **kwargs):
    return await evaluate_entry_timing(
        client,
        trade_date=DAY,
        prior_trade_date=PRIOR,
        symbols=SYMBOLS,
        features=feature() if features is None else features,
        now=NOW,
        **kwargs,
    )


def test_projection_matches_original_close_pair_formula_and_yuan_amount():
    bars = prefix()
    result = project_features({CODE: bars}, DAY, [CODE])[CODE]
    assert result == pytest.approx(
        dict(
            max_drop_3m_pct=100 * (1 - 98 / 102),
            prefix_amount_yuan=100_000_000.0,
            last3_amount_share=0.3,
        )
    )
    # The research compares timed closes only; the opening spike is excluded.
    bars[0] = replace(bars[0], open_price=200.0, high_price=200.0)
    assert project_features({CODE: bars}, DAY, [CODE])[CODE] == pytest.approx(result)


def test_projection_excludes_0940_0941_and_unrequested_stocks():
    bars = prefix()
    baseline = project_features({CODE: bars}, DAY, [CODE])
    for clock in ("09:40", "09:41", "15:00"):
        stamp = datetime.fromisoformat(f"{DAY}T{clock}:00").replace(tzinfo=TZ)
        bars.append(replace(bars[0], bar_end=stamp, end_label=clock, close_price=0.01, amount=9e15))
    assert project_features({CODE: list(reversed(bars)), "600002": bars}, DAY, [CODE]) == baseline


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "duplicate",
        "wrong_day",
        "wrong_code",
        "negative_amount",
        "bad_pair",
        "nan",
        "naive",
    ],
)
def test_projection_does_not_turn_invalid_prefix_into_available_features(damage):
    bars = prefix()
    if damage == "missing":
        bars.pop()
    elif damage == "duplicate":
        bars.append(bars[0])
    elif damage == "wrong_day":
        bars[0] = replace(bars[0], bar_end=bars[0].bar_end - timedelta(days=1))
    elif damage == "wrong_code":
        bars[0] = replace(bars[0], stock_code="600002")
    elif damage == "negative_amount":
        bars[0] = replace(bars[0], amount=-1.0)
    elif damage == "bad_pair":
        bars[0] = replace(bars[0], volume=1.0)
    elif damage == "nan":
        bars[0] = replace(bars[0], close_price=float("nan"))
    else:
        bars[0] = replace(bars[0], bar_end=bars[0].bar_end.replace(tzinfo=None))
    assert CODE not in project_features({CODE: bars}, DAY, [CODE])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drop,amount,share,wait",
    [
        (1.61, 418_000_000.0, 0.3647, False),
        (1.61, 900_000_000.0, 0.3647001, True),
        (1.610001, 418_000_000.0, 0.1, True),
        (1.610001, 418_000_001.0, 0.9, False),
        (0.0, 20_000_000.0, 0.8, True),
    ],
)
async def test_fixed_thresholds_have_exact_boundaries(drop, amount, share, wait):
    assert matches_wait_rule(feature(drop, amount, share)[CODE]) is wait
    result = await evaluate(client_with(), features=feature(drop, amount, share))
    assert result["schema"] == "v22-entry-timing/v1"
    assert result["status"] == ("WAIT" if wait else "NO_WAIT")
    assert [s["code"] for s in result["symbols"]] == ([CODE] if wait else [])


@pytest.mark.asyncio
async def test_only_exact_0940_index_close_controls_green_and_sources_are_retained():
    client = client_with(
        [minute("09:39", 3100.0), minute("09:40", 2990.0), minute("09:59", 3200.0)]
    )
    result = await evaluate(client)
    assert result["status"] == "WAIT"
    assert result["symbols"][0]["name"] == "股票甲"
    assert result["symbols"][0]["features"] == feature()[CODE]
    assert result["index"]["close_0940"] == 2990.0
    assert result["index"]["prior_close"] == 3000.0
    assert result["index"]["minute_row"]["time"] == f"{DAY} 09:40:00"
    assert result["index"]["prior_daily_row"]["trade_date"] == "20260922"
    calls = client._api_call.await_args_list
    assert [c.args[0] for c in calls] == ["rt_idx_min_daily", "index_daily"]
    assert calls[0].args[1] == {"ts_code": "000001.SH", "freq": "1MIN"}
    assert calls[1].args[1] == {"ts_code": "000001.SH", "trade_date": "20260922"}


@pytest.mark.asyncio
@pytest.mark.parametrize("close", [3000.0, 3001.0])
async def test_flat_or_red_index_does_not_alert(close):
    assert (await evaluate(client_with([minute(close=close)])))["status"] == "NO_WAIT"


@pytest.mark.asyncio
async def test_missing_0940_is_pending_without_substituting_current_quote():
    result = await evaluate(client_with([minute("09:39", 2990.0), minute("09:59", 2980.0)]))
    assert result["status"] == "UNAVAILABLE"
    assert result["reason"] == "INDEX_0940_PENDING"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        [minute(), minute()],
        [minute(code="000001.SZ")],
        [minute(day=PRIOR)],
        [minute(close=float("nan"))],
        [minute(close=0.0)],
    ],
)
async def test_invalid_index_evidence_never_becomes_a_green_signal(rows):
    assert (await evaluate(client_with(rows)))["status"] == "UNAVAILABLE"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "daily",
    [
        [],
        [dict(ts_code="000001.SH", trade_date="20260921", close=3000.0)],
        [dict(ts_code="000001.SZ", trade_date="20260922", close=3000.0)],
        [dict(ts_code="000001.SH", trade_date="20260922", close=0.0)],
    ],
)
async def test_prior_index_daily_must_be_the_exact_requested_session(daily):
    assert (await evaluate(client_with(daily=daily)))["status"] == "UNAVAILABLE"


@pytest.mark.asyncio
async def test_historical_target_routes_only_to_historical_index_minutes():
    row = minute()
    row["trade_time"] = row.pop("time")
    client = client_with([row])
    result = await evaluate_entry_timing(
        client,
        trade_date=DAY,
        prior_trade_date=PRIOR,
        symbols=SYMBOLS,
        features=feature(),
        now=NOW + timedelta(days=1),
    )
    assert result["status"] == "WAIT"
    calls = client._api_call.await_args_list
    assert calls[0].args[0] == "idx_mins"
    assert calls[0].args[1] == dict(
        ts_code="000001.SH",
        freq="1min",
        start_date="2026-09-23 09:40:00",
        end_date="2026-09-23 09:40:00",
    )
    assert all(not c.args[0].startswith("rt_") for c in calls)


@pytest.mark.asyncio
async def test_empty_allowed_symbols_needs_no_market_api():
    client = SimpleNamespace(_api_call=AsyncMock())
    result = await evaluate_entry_timing(
        client, trade_date=DAY, prior_trade_date=PRIOR, symbols=[], features={}, now=NOW
    )
    assert result["status"] == "NO_WAIT"
    client._api_call.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_stock_match_needs_no_index_or_0940_wait():
    client = SimpleNamespace(_api_call=AsyncMock())
    result = await evaluate_entry_timing(
        client,
        trade_date=DAY,
        prior_trade_date=PRIOR,
        symbols=SYMBOLS,
        features=feature(drop=0.0, share=0.1),
        now=NOW.replace(hour=9, minute=39),
    )
    assert (result["status"], result["reason"]) == ("NO_WAIT", "NO_STOCK_RULE_MATCH")
    client._api_call.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_stock_features_is_unavailable_instead_of_silent_no_wait():
    result = await evaluate(client_with(), features={})
    assert result["status"] == "UNAVAILABLE"
    assert result["reason"] == "STOCK_FEATURES_UNAVAILABLE"


@pytest.mark.asyncio
async def test_early_clock_cannot_use_a_future_0940_row():
    client = client_with()
    result = await evaluate_entry_timing(
        client,
        trade_date=DAY,
        prior_trade_date=PRIOR,
        symbols=SYMBOLS,
        features=feature(),
        now=NOW.replace(hour=9, minute=39),
    )
    assert (result["status"], result["reason"]) == ("UNAVAILABLE", "INDEX_0940_PENDING")
    client._api_call.assert_not_awaited()


@pytest.mark.asyncio
async def test_transport_failure_is_unavailable_but_cancellation_propagates():
    client = SimpleNamespace(_api_call=AsyncMock(side_effect=RuntimeError("provider unavailable")))
    assert (await evaluate(client))["status"] == "UNAVAILABLE"
    client._api_call.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await evaluate(client)


@pytest.mark.asyncio
async def test_current_tushare_permission_failure_uses_valid_current_tencent_facts():
    client = SimpleNamespace(_api_call=AsyncMock(side_effect=RuntimeError("40203")))
    fetch = AsyncMock(return_value=tencent_payload())
    result = await evaluate(client, tencent_fetch=fetch)
    assert result["status"] == "WAIT"
    assert result["index"]["source"] == "TENCENT_CURRENT_MINUTE"
    assert result["index"]["close_0940"] == 3947.59
    assert result["index"]["prior_close"] == 3952.13
    assert result["index"]["minute_row"] == "0940 3947.59 10 20"
    assert result["index"]["fallback_from"]["reason"] == "INDEX_MINUTE_SOURCE_ERROR"
    fetch.assert_awaited_once_with()
    assert client._api_call.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "data",
    [
        {},
        tencent_payload(day="20260922"),
        tencent_payload(minutes=[]),
        tencent_payload(minutes=["0940 3947.59 10 20", "0940 3947.59 10 20"]),
        tencent_payload(code="000001.SZ"),
        tencent_payload(prior="0"),
        tencent_payload(minutes=["0940 nan 10 20"]),
        tencent_payload(minutes=["0939 3900 10 20", "1500 3900 10 20"]),
    ],
)
async def test_invalid_tencent_current_evidence_never_substitutes_old_or_latest_price(data):
    client = SimpleNamespace(_api_call=AsyncMock(side_effect=RuntimeError("40203")))
    assert (await evaluate(client, tencent_fetch=AsyncMock(return_value=data)))[
        "status"
    ] == "UNAVAILABLE"


@pytest.mark.asyncio
async def test_historical_failure_never_falls_back_to_current_tencent():
    client = SimpleNamespace(_api_call=AsyncMock(side_effect=RuntimeError("40203")))
    fetch = AsyncMock(return_value=tencent_payload())
    result = await evaluate_entry_timing(
        client,
        trade_date=DAY,
        prior_trade_date=PRIOR,
        symbols=SYMBOLS,
        features=feature(),
        now=NOW + timedelta(days=1),
        tencent_fetch=fetch,
    )
    assert result["status"] == "UNAVAILABLE"
    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_tencent_cancellation_is_not_swallowed():
    client = SimpleNamespace(_api_call=AsyncMock(side_effect=RuntimeError("40203")))
    with pytest.raises(asyncio.CancelledError):
        await evaluate(client, tencent_fetch=AsyncMock(side_effect=asyncio.CancelledError()))


@pytest.mark.asyncio
async def test_tencent_missing_0940_remains_retryable_after_provider_permission_failure():
    client = SimpleNamespace(_api_call=AsyncMock(side_effect=RuntimeError("40203")))
    pending = tencent_payload(minutes=["0939 3955.00 10 20"])
    result = await evaluate(client, tencent_fetch=AsyncMock(return_value=pending))
    assert (result["status"], result["reason"]) == ("UNAVAILABLE", "INDEX_0940_PENDING")
    duplicate = tencent_payload(minutes=["0940 3947.59 10 20", "0940 3947.59 10 20"])
    result = await evaluate(client, tencent_fetch=AsyncMock(return_value=duplicate))
    assert result["status"] == "UNAVAILABLE" and result["reason"] != "INDEX_0940_PENDING"


@pytest.mark.asyncio
async def test_public_fetch_uses_a_separate_fixed_get_without_tushare_credentials(monkeypatch):
    response = SimpleNamespace(raise_for_status=lambda: None, json=lambda: tencent_payload())
    get = AsyncMock(return_value=response)
    settings = []

    class PublicClient:
        def __init__(self, **kwargs):
            settings.append(kwargs)

        async def __aenter__(self):
            return SimpleNamespace(get=get)

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr("src.strategy.v22_slim.entry_timing.httpx.AsyncClient", PublicClient)
    assert await fetch_public_index() == tencent_payload()
    assert settings == [{"timeout": 8.0, "follow_redirects": False, "trust_env": False}]
    get.assert_awaited_once_with(
        "https://ifzq.gtimg.cn/appstock/app/minute/query", params={"code": "sh000001"}
    )
