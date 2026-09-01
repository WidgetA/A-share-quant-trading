"""Single rt_min_daily pull must yield quote + expected-date early bars together."""

from __future__ import annotations

import asyncio
import dataclasses
from collections import Counter
from datetime import date, datetime, timezone

import pytest

from src.data.clients.tushare_realtime import (
    BEIJING_TZ,
    TushareEarlyMarketData,
    TushareMinuteBar,
    TushareQuote,
    TushareRealtimeClient,
    TushareRealtimeError,
)

_FIELDS = ["time", "open", "close", "high", "low", "vol", "amount"]
TRADE_DATE = date(2026, 8, 31)


def _row(label: str, close: float, *, vol: float = 100.0) -> list[object]:
    return [
        f"{TRADE_DATE.isoformat()} {label}:00",
        round(close - 0.05, 4),  # open
        close,
        round(close + 0.1, 4),  # high
        round(close - 0.1, 4),  # low
        vol,
        1000.0,
    ]


def _daily_payload(closes: dict[str, float]) -> dict[str, object]:
    return {
        "data": {
            "fields": list(_FIELDS),
            "items": [_row(label, close) for label, close in closes.items()],
        }
    }


def _full_day_payload(base: float = 10.0) -> dict[str, object]:
    labels = [f"09:{minute:02d}" for minute in range(31, 41)]  # 09:31..09:40
    return _daily_payload({label: base + i * 0.1 for i, label in enumerate(labels)})


def _payload(
    closes: dict[str, float],
    *,
    fields: list[str],
    **envelope: object,
) -> dict[str, object]:
    rows = {label: dict(zip(_FIELDS, _row(label, close))) for label, close in closes.items()}
    return {
        **envelope,
        "data": {
            "fields": list(fields),
            "items": [[row[field] for field in fields] for row in rows.values()],
        },
    }


def _make_client(monkeypatch: pytest.MonkeyPatch, responses: dict[str, dict]):
    """Fake transport keyed by ts_code; returns (client, recorded_calls)."""
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]
    calls: list[tuple[str, str]] = []

    async def fake_api_call(api_name, params, fields=""):
        ts_code = params["ts_code"]
        calls.append((api_name, ts_code))
        return responses[ts_code.split(".")[0]]

    monkeypatch.setattr(client, "_api_call", fake_api_call)
    return client, calls


@pytest.mark.asyncio
async def test_one_api_call_per_code_yields_quote_and_bars_from_same_response(
    monkeypatch,
) -> None:
    responses = {
        "000001": _full_day_payload(10.0),
        "600000": _full_day_payload(20.0),
    }
    client, calls = _make_client(monkeypatch, responses)

    result = await client.batch_get_early_market_data(
        ["000001", "600000"], expected_trade_date=TRADE_DATE
    )

    # Exactly one rt_min_daily call per stock code — no double pull.
    assert sorted(calls) == [("rt_min_daily", "000001.SZ"), ("rt_min_daily", "600000.SH")]

    data = result["000001"]
    assert isinstance(data, TushareEarlyMarketData)
    # Aggregated quote semantics (same as _parse_rt_min_daily):
    assert data.quote.stock_code == "000001"
    assert data.quote.open_price == pytest.approx(9.95)  # first bar open
    assert data.quote.latest_price == pytest.approx(10.9)  # 09:40 close
    assert data.quote.early_close == pytest.approx(10.8)  # 09:39 close
    # Minute-parse semantics: exactly the 09:31..09:39 labels, in order.
    assert [bar.end_label for bar in data.early_bars] == [
        f"09:{minute:02d}" for minute in range(31, 40)
    ]
    assert all(bar.stock_code == "000001" for bar in data.early_bars)
    assert len(data.source_hash) == 64
    int(data.source_hash, 16)  # hex

    assert result["600000"].quote.open_price == pytest.approx(19.95)
    assert len(result["600000"].early_bars) == 9


@pytest.mark.asyncio
async def test_duplicates_and_reverse_completion_preserve_unique_input_order(
    monkeypatch,
) -> None:
    codes = ["000001", "600000", "000001", "300750"]
    unique_order = ["000001", "600000", "300750"]
    responses = {
        "000001": _full_day_payload(10.0),
        "600000": _full_day_payload(20.0),
        "300750": _full_day_payload(30.0),
    }
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]
    calls: list[tuple[str, str]] = []
    completions: list[str] = []

    async def fake_api_call(api_name, params, fields=""):
        bare_code = params["ts_code"].split(".")[0]
        # Reverse completion: 300750 first, then 600000, then 000001.
        delay = (len(unique_order) - 1 - unique_order.index(bare_code)) * 0.02
        await asyncio.sleep(delay)
        completions.append(bare_code)
        calls.append((api_name, params["ts_code"]))
        return responses[bare_code]

    monkeypatch.setattr(client, "_api_call", fake_api_call)
    result = await client.batch_get_early_market_data(codes, expected_trade_date=TRADE_DATE)

    assert completions == ["300750", "600000", "000001"]
    assert Counter(calls) == Counter(
        [
            ("rt_min_daily", "000001.SZ"),
            ("rt_min_daily", "600000.SH"),
            ("rt_min_daily", "300750.SZ"),
        ]
    )
    assert len(calls) == 3
    assert list(result) == ["000001", "600000", "300750"]


@pytest.mark.asyncio
async def test_source_hash_is_stable_for_identical_raw_response(monkeypatch) -> None:
    responses = {"000001": _full_day_payload(10.0)}
    client, _ = _make_client(monkeypatch, responses)

    first = await client.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    second = await client.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)

    assert first["000001"].source_hash == second["000001"].source_hash

    changed = {"000001": _full_day_payload(11.0)}
    client2, _ = _make_client(monkeypatch, changed)
    third = await client2.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    assert third["000001"].source_hash != first["000001"].source_hash


@pytest.mark.asyncio
async def test_source_hash_ignores_response_field_order(monkeypatch) -> None:
    closes = {f"09:{minute:02d}": 10.0 + minute * 0.01 for minute in range(31, 41)}
    rows = [dict(zip(_FIELDS, _row(label, close))) for label, close in closes.items()]

    def build(fields_order: list[str]) -> dict[str, object]:
        return {
            "request_id": "abc",
            "data": {
                "fields": fields_order,
                "items": [[row[field] for field in fields_order] for row in rows],
            },
        }

    order_a = ["time", "open", "close", "high", "low", "vol", "amount"]
    order_b = ["amount", "vol", "low", "high", "close", "open", "time"]

    client_a, _ = _make_client(monkeypatch, {"000001": build(order_a)})
    client_b, _ = _make_client(monkeypatch, {"000001": build(order_b)})

    hash_a = (
        await client_a.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash
    hash_b = (
        await client_b.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash

    assert hash_a == hash_b


@pytest.mark.asyncio
async def test_source_hash_changes_when_any_early_row_changes(monkeypatch) -> None:
    labels = [f"09:{minute:02d}" for minute in range(31, 41)]

    def build(close_map: dict[str, float]) -> dict[str, object]:
        return _daily_payload({label: close_map[label] for label in labels})

    base = {label: 10.0 for label in labels}
    changed = dict(base)
    changed["09:35"] = 99.0

    client_a, _ = _make_client(monkeypatch, {"000001": build(base)})
    client_b, _ = _make_client(monkeypatch, {"000001": build(changed)})

    hash_a = (
        await client_a.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash
    hash_b = (
        await client_b.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash

    assert hash_a != hash_b


@pytest.mark.asyncio
async def test_source_hash_includes_call_auction_and_0931_before(monkeypatch) -> None:
    """Rows before 09:31 (e.g., 09:25 call auction) are selection-relevant."""
    labels = ["09:25", "09:31", "09:32"]
    base = _daily_payload({label: 10.0 for label in labels})
    changed = _daily_payload({label: 99.0 if label == "09:25" else 10.0 for label in labels})

    client_a, _ = _make_client(monkeypatch, {"000001": base})
    client_b, _ = _make_client(monkeypatch, {"000001": changed})

    hash_a = (
        await client_a.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash
    hash_b = (
        await client_b.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash

    assert hash_a != hash_b


@pytest.mark.asyncio
async def test_source_hash_ignores_0940_and_later_rows(monkeypatch) -> None:
    early_labels = [f"09:{minute:02d}" for minute in range(31, 40)]
    base = _daily_payload({label: 10.0 for label in early_labels})
    extra = _daily_payload({label: 10.0 for label in early_labels + ["09:40", "09:41"]})

    client_a, _ = _make_client(monkeypatch, {"000001": base})
    client_b, _ = _make_client(monkeypatch, {"000001": extra})

    hash_a = (
        await client_a.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash
    hash_b = (
        await client_b.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash

    assert hash_a == hash_b


@pytest.mark.asyncio
async def test_wrong_trade_date_is_dropped(monkeypatch) -> None:
    """A response whose bars are for a different date must not yield ready evidence."""
    wrong_date = date(2026, 8, 30)

    def wrong_day_payload() -> dict[str, object]:
        labels = [f"09:{minute:02d}" for minute in range(31, 40)]
        items = [
            [f"{wrong_date.isoformat()} {label}:00", 10.0, 10.0, 10.1, 9.9, 100.0, 1000.0]
            for label in labels
        ]
        return {"data": {"fields": list(_FIELDS), "items": items}}

    client, _ = _make_client(monkeypatch, {"000001": wrong_day_payload()})
    result = await client.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)

    assert "000001" not in result


@pytest.mark.asyncio
async def test_early_market_data_is_frozen(monkeypatch) -> None:
    responses = {"000001": _full_day_payload(10.0)}
    client, _ = _make_client(monkeypatch, responses)

    data = (await client.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE))[
        "000001"
    ]

    with pytest.raises(dataclasses.FrozenInstanceError):
        data.source_hash = "tampered"  # type: ignore[misc]


@pytest.mark.asyncio
async def test_tushare_quote_is_not_globally_frozen(monkeypatch) -> None:
    responses = {"000001": _full_day_payload(10.0)}
    client, _ = _make_client(monkeypatch, responses)

    data = (await client.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE))[
        "000001"
    ]

    # Mutation of a TushareQuote instance must be allowed (it is not globally frozen).
    data.quote.early_close = 99.0
    assert data.quote.early_close == 99.0


@pytest.mark.asyncio
async def test_early_quotes_wrapper_returns_same_quotes(monkeypatch) -> None:
    responses = {"000001": _full_day_payload(10.0), "600000": _full_day_payload(20.0)}
    client, calls = _make_client(monkeypatch, responses)

    quotes = await client.batch_get_early_quotes(
        ["000001", "600000"], expected_trade_date=TRADE_DATE
    )

    assert set(quotes) == {"000001", "600000"}
    assert quotes["000001"].early_close == pytest.approx(10.8)
    assert quotes["600000"].latest_price == pytest.approx(20.9)
    # Wrapper still performs exactly one API call per code.
    assert sorted(calls) == [("rt_min_daily", "000001.SZ"), ("rt_min_daily", "600000.SH")]


@pytest.mark.asyncio
async def test_early_quotes_wrapper_keeps_failure_semantics(monkeypatch) -> None:
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]

    async def failed_call(*_args, **_kwargs):
        raise TushareRealtimeError("vendor down")

    monkeypatch.setattr(client, "_api_call", failed_call)

    with pytest.raises(TushareRealtimeError, match="vendor down"):
        await client.batch_get_early_quotes(["000001"])
    with pytest.raises(TushareRealtimeError, match="vendor down"):
        await client.batch_get_early_market_data(["000001"])


@pytest.mark.asyncio
async def test_empty_response_code_is_dropped_with_warning(monkeypatch, caplog) -> None:
    empty = {"data": {"fields": list(_FIELDS), "items": []}}
    responses = {"000001": _full_day_payload(10.0), "600000": empty}
    client, _ = _make_client(monkeypatch, responses)

    with caplog.at_level("WARNING"):
        quotes = await client.batch_get_early_quotes(
            ["000001", "600000"], expected_trade_date=TRADE_DATE
        )

    assert set(quotes) == {"000001"}
    assert "600000" in caplog.text


@pytest.mark.asyncio
async def test_bars_come_from_same_response_not_a_second_pull(monkeypatch) -> None:
    """A code whose 09:40 bar is absent must not trigger any extra fetch."""
    labels = [f"09:{minute:02d}" for minute in range(31, 40)]  # no 09:40 bar
    responses = {"000001": _daily_payload({label: 10.0 for label in labels})}
    client, calls = _make_client(monkeypatch, responses)

    result = await client.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)

    assert calls == [("rt_min_daily", "000001.SZ")]
    assert [bar.end_label for bar in result["000001"].early_bars] == labels
    assert result["000001"].quote.latest_price == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_legacy_response_without_time_returns_quote_and_empty_bars(
    monkeypatch,
) -> None:
    """Origin/main compatibility: valid OHLCV without time still yields a quote."""
    items = [[9.95, 10.0, 10.1, 9.9, 1000.0, 10000.0]]
    no_time = {
        "data": {
            "fields": ["open", "close", "high", "low", "vol", "amount"],
            "items": items,
        }
    }
    client, calls = _make_client(monkeypatch, {"000001": no_time})

    result = await client.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)

    assert calls == [("rt_min_daily", "000001.SZ")]
    data = result["000001"]
    assert data.quote.stock_code == "000001"
    assert data.quote.open_price == pytest.approx(9.95)
    assert data.quote.latest_price == pytest.approx(10.0)
    assert data.early_bars == ()
    assert len(data.source_hash) == 64


@pytest.mark.asyncio
async def test_wrong_date_fixture_does_not_produce_ready_bars(monkeypatch) -> None:
    """A bar stamped yesterday with a valid 09:39 label must not count as ready."""
    wrong_date = date(2026, 8, 30)
    items = [[f"{wrong_date.isoformat()} 09:39:00", 10.0, 10.0, 10.1, 9.9, 100.0, 1000.0]]
    response = {"data": {"fields": list(_FIELDS), "items": items}}
    client, _ = _make_client(monkeypatch, {"000001": response})

    result = await client.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)

    assert "000001" not in result


# --- T1 regression tests: hardened canonical minute-fact pipeline -------------


def _valid_early_closes() -> dict[str, float]:
    return {f"09:{minute:02d}": 10.0 for minute in range(31, 40)}


def _quote_dict(quote: TushareQuote) -> dict[str, object]:
    return dataclasses.asdict(quote)


def _bars_fields(bars: tuple[TushareMinuteBar, ...]) -> list[dict[str, object]]:
    return [dataclasses.asdict(bar) for bar in bars]


def _assert_same_three_piece(
    a: TushareEarlyMarketData,
    b: TushareEarlyMarketData,
) -> None:
    """Assert two TushareEarlyMarketData are identical in selection-relevant output."""
    assert _quote_dict(a.quote) == _quote_dict(b.quote)
    assert _bars_fields(a.early_bars) == _bars_fields(b.early_bars)
    assert a.source_hash == b.source_hash


@pytest.mark.asyncio
async def test_mixed_code_tushare_error_keeps_healthy_siblings(monkeypatch) -> None:
    """One failed rt_min_daily code must not discard successful siblings."""
    responses = {
        "000001": _full_day_payload(10.0),
        "600000": _full_day_payload(20.0),
    }
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]
    calls: list[str] = []

    async def fake_api_call(api_name, params, fields=""):
        bare = params["ts_code"].split(".")[0]
        calls.append(bare)
        if bare == "600000":
            raise TushareRealtimeError("vendor 600000 down")
        return responses[bare]

    monkeypatch.setattr(client, "_api_call", fake_api_call)
    result = await client.batch_get_early_market_data(
        ["000001", "600000"], expected_trade_date=TRADE_DATE
    )

    assert "000001" in result
    assert "600000" not in result
    assert calls == ["000001", "600000"]


@pytest.mark.asyncio
async def test_all_codes_failed_raises_original_exception_object(monkeypatch) -> None:
    """When every rt_min_daily code fails, the original exception object is raised."""
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]
    original = TushareRealtimeError("vendor down")

    async def fake_api_call(api_name, params, fields=""):  # noqa: ARG001
        raise original

    monkeypatch.setattr(client, "_api_call", fake_api_call)

    with pytest.raises(TushareRealtimeError) as exc_info:
        await client.batch_get_early_market_data(
            ["000001", "600000"], expected_trade_date=TRADE_DATE
        )
    assert exc_info.value is original


@pytest.mark.asyncio
async def test_cancelled_error_is_propagated_not_swallowed(monkeypatch) -> None:
    """asyncio.CancelledError must not be treated as a per-symbol vendor error."""
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]

    async def fake_api_call(api_name, params, fields=""):  # noqa: ARG001
        raise asyncio.CancelledError("cancelled")

    monkeypatch.setattr(client, "_api_call", fake_api_call)

    with pytest.raises(asyncio.CancelledError):
        await client.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)


@pytest.mark.asyncio
async def test_raw_items_reverse_order_same_quote_bars_and_hash(monkeypatch) -> None:
    """Vendor item order must not change the canonical quote, early bars, or hash."""
    labels = [f"09:{minute:02d}" for minute in range(31, 41)]
    rows = [dict(zip(_FIELDS, _row(label, 10.0 + i * 0.1))) for i, label in enumerate(labels)]

    def build(reverse: bool) -> dict[str, object]:
        ordered = list(reversed(rows)) if reverse else rows
        return {
            "data": {
                "fields": list(_FIELDS),
                "items": [[row[field] for field in _FIELDS] for row in ordered],
            }
        }

    client_a, _ = _make_client(monkeypatch, {"000001": build(False)})
    client_b, _ = _make_client(monkeypatch, {"000001": build(True)})

    a = (await client_a.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE))[
        "000001"
    ]
    b = (await client_b.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE))[
        "000001"
    ]

    _assert_same_three_piece(a, b)


@pytest.mark.asyncio
async def test_identical_duplicate_does_not_change_three_piece(monkeypatch) -> None:
    """A duplicated identical row must be folded once."""
    labels = [f"09:{minute:02d}" for minute in range(31, 40)]
    unique_rows = [dict(zip(_FIELDS, _row(label, 10.0))) for label in labels]
    duplicated_rows = [*unique_rows, dict(unique_rows[-1])]

    baseline = {"data": {"fields": list(_FIELDS), "items": [list(r.values()) for r in unique_rows]}}
    duplicated = {
        "data": {
            "fields": list(_FIELDS),
            "items": [list(r.values()) for r in duplicated_rows],
        }
    }

    client_base, _ = _make_client(monkeypatch, {"000001": baseline})
    client_dup, _ = _make_client(monkeypatch, {"000001": duplicated})

    base_data = (
        await client_base.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"]
    dup_data = (
        await client_dup.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"]

    _assert_same_three_piece(base_data, dup_data)


@pytest.mark.asyncio
async def test_conflicting_duplicate_result_independent_of_item_order(monkeypatch) -> None:
    """Same-timestamp conflicting rows must be dropped regardless of arrival order."""
    base_labels = [f"09:{minute:02d}" for minute in range(31, 39)]
    base_rows = [dict(zip(_FIELDS, _row(label, 10.0))) for label in base_labels]
    conflict_a = dict(zip(_FIELDS, _row("09:39", 10.9)))
    conflict_b = dict(zip(_FIELDS, _row("09:39", 99.0)))

    def build(first: dict, second: dict) -> dict[str, object]:
        rows = [*base_rows, first, second]
        return {
            "data": {
                "fields": list(_FIELDS),
                "items": [[row[field] for field in _FIELDS] for row in rows],
            }
        }

    client_a, _ = _make_client(monkeypatch, {"000001": build(conflict_a, conflict_b)})
    client_b, _ = _make_client(monkeypatch, {"000001": build(conflict_b, conflict_a)})

    a = (await client_a.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE))[
        "000001"
    ]
    b = (await client_b.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE))[
        "000001"
    ]

    _assert_same_three_piece(a, b)
    assert all(bar.end_label != "09:39" for bar in a.early_bars)
    assert all(bar.end_label != "09:39" for bar in b.early_bars)


@pytest.mark.asyncio
async def test_timezone_equivalence_full_three_piece(monkeypatch) -> None:
    """ISO-T, +08:00, and UTC timestamps for the same instant produce identical output."""
    labels = [f"09:{minute:02d}" for minute in range(31, 40)]

    def ts_for(label: str, fmt: str) -> str:
        base = f"{TRADE_DATE.isoformat()} {label}:00"
        if fmt == "naive":
            return base
        dt = datetime.fromisoformat(base).replace(tzinfo=BEIJING_TZ)
        if fmt == "utc":
            return dt.astimezone(timezone.utc).isoformat()
        if fmt == "offset":
            return dt.isoformat()
        return base

    def build(fmt: str) -> dict[str, object]:
        rows = []
        for i, label in enumerate(labels):
            row = dict(zip(_FIELDS, _row(label, 10.0 + i * 0.1)))
            row["time"] = ts_for(label, fmt)
            rows.append(row)
        return {
            "data": {
                "fields": list(_FIELDS),
                "items": [[row[field] for field in _FIELDS] for row in rows],
            }
        }

    results = [
        (
            await _make_client(monkeypatch, {"000001": build(fmt)})[0].batch_get_early_market_data(
                ["000001"], expected_trade_date=TRADE_DATE
            )
        )["000001"]
        for fmt in ("naive", "offset", "utc")
    ]

    _assert_same_three_piece(results[0], results[1])
    _assert_same_three_piece(results[1], results[2])


@pytest.mark.asyncio
async def test_wrong_date_mixed_with_valid_equals_baseline(monkeypatch) -> None:
    """Wrong-date malformed/boolean rows must not affect the valid-baseline output."""
    wrong_date = date(2026, 8, 30)
    baseline = _daily_payload(_valid_early_closes())

    bad_rows = [
        # wrong-date boolean volume
        [f"{wrong_date.isoformat()} 09:39:00", 10.0, 10.0, 10.1, 9.9, True, 1000.0],
        # wrong-date malformed (invalid OHLCV)
        [f"{wrong_date.isoformat()} 09:38:00", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    ]
    mixed = {
        "data": {
            "fields": list(_FIELDS),
            "items": baseline["data"]["items"] + bad_rows,
        }
    }

    client_base, _ = _make_client(monkeypatch, {"000001": baseline})
    client_mixed, _ = _make_client(monkeypatch, {"000001": mixed})

    base_data = (
        await client_base.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"]
    mixed_data = (
        await client_mixed.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"]

    _assert_same_three_piece(base_data, mixed_data)


@pytest.mark.asyncio
async def test_same_day_0940_malformed_does_not_affect_early_baseline(monkeypatch) -> None:
    """09:40+ malformed/boolean rows must be ignored; early evidence stays baseline."""
    baseline = _daily_payload(_valid_early_closes())

    bad_rows = [
        # same-day 09:40 boolean volume
        [f"{TRADE_DATE.isoformat()} 09:40:00", 11.0, 11.0, 11.1, 10.9, True, 1100.0],
        # same-day 09:41 malformed
        [f"{TRADE_DATE.isoformat()} 09:41:00", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    ]
    mixed = {
        "data": {
            "fields": list(_FIELDS),
            "items": baseline["data"]["items"] + bad_rows,
        }
    }

    client_base, _ = _make_client(monkeypatch, {"000001": baseline})
    client_mixed, _ = _make_client(monkeypatch, {"000001": mixed})

    base_data = (
        await client_base.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"]
    mixed_data = (
        await client_mixed.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"]

    # Early evidence must be identical.
    assert _bars_fields(base_data.early_bars) == _bars_fields(mixed_data.early_bars)
    assert base_data.source_hash == mixed_data.source_hash
    # Full-day quote unaffected because 09:40+ rows were invalid.
    assert _quote_dict(base_data.quote) == _quote_dict(mixed_data.quote)


@pytest.mark.asyncio
async def test_date_only_timestamp_is_rejected(monkeypatch) -> None:
    """A bare date string 'YYYY-MM-DD' must not be accepted as a 00:00 bar."""
    items = [[TRADE_DATE.isoformat(), 10.0, 10.0, 10.1, 9.9, 100.0, 1000.0]]
    payload = {"data": {"fields": list(_FIELDS), "items": items}}
    client, _ = _make_client(monkeypatch, {"000001": payload})

    result = await client.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    assert "000001" not in result


@pytest.mark.asyncio
async def test_valid_plus_date_only_equals_baseline(monkeypatch) -> None:
    """A valid 09:39 bar plus a bare date-only row must equal the valid-only baseline."""
    baseline = _daily_payload({"09:39": 10.0})
    mixed = {
        "data": {
            "fields": list(_FIELDS),
            "items": [
                *baseline["data"]["items"],
                [TRADE_DATE.isoformat(), 10.0, 10.0, 10.1, 9.9, 100.0, 1000.0],
            ],
        }
    }

    client_base, _ = _make_client(monkeypatch, {"000001": baseline})
    client_mixed, _ = _make_client(monkeypatch, {"000001": mixed})

    base_data = (
        await client_base.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"]
    mixed_data = (
        await client_mixed.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"]

    _assert_same_three_piece(base_data, mixed_data)


@pytest.mark.asyncio
async def test_no_time_legacy_quote_has_all_early_fields(monkeypatch) -> None:
    """Legacy no-time payload must set early_* fields from full-day aggregates."""
    items = [[9.95, 10.0, 10.1, 9.9, 1000.0, 10000.0]]
    no_time = {
        "data": {
            "fields": ["open", "close", "high", "low", "vol", "amount"],
            "items": items,
        }
    }
    client, calls = _make_client(monkeypatch, {"000001": no_time})

    data = (await client.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE))[
        "000001"
    ]

    assert calls == [("rt_min_daily", "000001.SZ")]
    q = data.quote
    assert q.stock_code == "000001"
    assert q.open_price == pytest.approx(9.95)
    assert q.latest_price == pytest.approx(10.0)
    assert q.high_price == pytest.approx(10.1)
    assert q.low_price == pytest.approx(9.9)
    assert q.volume == pytest.approx(1000.0)
    assert q.amount == pytest.approx(10000.0)
    assert q.early_close == pytest.approx(q.latest_price)
    assert q.early_high == pytest.approx(q.high_price)
    assert q.early_low == pytest.approx(q.low_price)
    assert q.early_volume == pytest.approx(q.volume)
    assert q.volume_937 == pytest.approx(q.volume)
    assert data.early_bars == ()
    assert len(data.source_hash) == 64

    # _parse_rt_min_daily backward-compatible alias must return the same quote.
    direct_quote = TushareRealtimeClient._parse_rt_min_daily(
        "000001", no_time, expected_trade_date=TRADE_DATE
    )
    assert direct_quote == q


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "vol", "amount"])
@pytest.mark.asyncio
async def test_source_hash_changes_for_each_ohlcv_field(field: str, monkeypatch) -> None:
    """Changing exactly one OHLCV field in a valid early bar must change source_hash."""
    base_time = f"{TRADE_DATE.isoformat()} 09:39:00"
    # Wide enough OHLC range that a single-field tweak keeps the bar valid.
    base_row = [base_time, 10.0, 10.0, 10.5, 9.5, 100.0, 1000.0]
    field_index = {f: i for i, f in enumerate(_FIELDS)}

    def row_with_field_changed(field_name: str) -> list[object]:
        row = list(base_row)
        if field_name == "open":
            row[field_index["open"]] = 10.2
        elif field_name == "close":
            row[field_index["close"]] = 10.3
        elif field_name == "high":
            row[field_index["high"]] = 10.6
        elif field_name == "low":
            row[field_index["low"]] = 9.4
        elif field_name == "vol":
            row[field_index["vol"]] = 200.0
        elif field_name == "amount":
            row[field_index["amount"]] = 2000.0
        return row

    base_payload = {"data": {"fields": list(_FIELDS), "items": [base_row]}}
    changed_payload = {"data": {"fields": list(_FIELDS), "items": [row_with_field_changed(field)]}}

    client_base, _ = _make_client(monkeypatch, {"000001": base_payload})
    client_changed, _ = _make_client(monkeypatch, {"000001": changed_payload})

    base_result = (
        await client_base.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"]
    changed_result = (
        await client_changed.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"]

    assert base_result.source_hash != changed_result.source_hash, (
        f"source_hash did not change when {field} changed"
    )

    # Only the intended field changed in the canonical early bar.
    base_bar = base_result.early_bars[0]
    changed_bar = changed_result.early_bars[0]
    for attr in ("stock_code", "bar_end", "end_label"):
        assert getattr(base_bar, attr) == getattr(changed_bar, attr)
    for price_field in ("open_price", "close_price", "high_price", "low_price"):
        if field == price_field.replace("_price", ""):
            assert getattr(base_bar, price_field) != getattr(changed_bar, price_field)
        else:
            assert getattr(base_bar, price_field) == getattr(changed_bar, price_field)
    for flow_field, flow_name in (("volume", "vol"), ("amount", "amount")):
        if field == flow_name:
            assert getattr(base_bar, flow_field) != getattr(changed_bar, flow_field)
        else:
            assert getattr(base_bar, flow_field) == getattr(changed_bar, flow_field)


@pytest.mark.asyncio
async def test_source_hash_includes_code_and_bar_end(monkeypatch) -> None:
    """source_hash must differ when code or bar timestamp differs."""
    labels = [f"09:{minute:02d}" for minute in range(31, 40)]
    rows = [dict(zip(_FIELDS, _row(label, 10.0))) for label in labels]
    payload = {
        "data": {
            "fields": list(_FIELDS),
            "items": [[row[field] for field in _FIELDS] for row in rows],
        }
    }

    client_a, _ = _make_client(monkeypatch, {"000001": payload})
    client_b, _ = _make_client(monkeypatch, {"000002": payload})

    hash_a = (
        await client_a.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash
    hash_b = (
        await client_b.batch_get_early_market_data(["000002"], expected_trade_date=TRADE_DATE)
    )["000002"].source_hash

    assert hash_a != hash_b

    shifted_rows = [dict(row) for row in rows]
    shifted_rows[0]["time"] = f"{TRADE_DATE.isoformat()} 09:30:00"
    shifted_payload = {
        "data": {
            "fields": list(_FIELDS),
            "items": [[row[field] for field in _FIELDS] for row in shifted_rows],
        }
    }
    client_c, _ = _make_client(monkeypatch, {"000001": shifted_payload})
    hash_c = (
        await client_c.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash
    assert hash_c != hash_a


_NO_TIME_FIELDS = ["open", "close", "high", "low", "vol", "amount"]


def _no_time_row(
    open_price: float = 9.95,
    close_price: float = 10.0,
    high_price: float = 10.1,
    low_price: float = 9.9,
    volume: float = 1000.0,
    amount: float = 10000.0,
) -> list[object]:
    return [open_price, close_price, high_price, low_price, volume, amount]


def _no_time_payload(rows: list[list[object]] | None = None) -> dict[str, object]:
    if rows is None:
        rows = [_no_time_row()]
    return {"data": {"fields": list(_NO_TIME_FIELDS), "items": rows}}


_INVALID_NO_TIME_VALUES: list[tuple[object, str]] = [
    (True, "bool True"),
    (False, "bool False"),
    (float("nan"), "NaN"),
    (float("inf"), "+Inf"),
    (float("-inf"), "-Inf"),
    ("not-a-number", "non-numeric string"),
]


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "vol", "amount"])
@pytest.mark.parametrize("bad_value, label", _INVALID_NO_TIME_VALUES)
@pytest.mark.asyncio
async def test_no_time_invalid_value_rejects_whole_symbol(
    field: str, bad_value: object, label: str, monkeypatch
) -> None:
    """Any invalid OHLCV value in any no-time row rejects that symbol, not siblings."""
    rows = [
        _no_time_row(),
        _no_time_row(close_price=10.1),
        _no_time_row(close_price=10.2),
    ]
    field_pos = {f: i for i, f in enumerate(_NO_TIME_FIELDS)}
    rows[1][field_pos[field]] = bad_value  # type: ignore[index]
    bad_payload = _no_time_payload(rows)

    responses = {"000001": bad_payload, "600000": _no_time_payload()}
    client, _ = _make_client(monkeypatch, responses)

    result = await client.batch_get_early_market_data(
        ["000001", "600000"], expected_trade_date=TRADE_DATE
    )

    assert "000001" not in result, f"{label} in {field} should reject the symbol"
    assert "600000" in result

    # Direct alias must return None for both two-argument and three-argument forms.
    assert TushareRealtimeClient._parse_rt_min_daily("000001", bad_payload) is None
    assert (
        TushareRealtimeClient._parse_rt_min_daily(
            "000001", bad_payload, expected_trade_date=TRADE_DATE
        )
        is None
    )


@pytest.mark.asyncio
async def test_no_time_short_row_rejects_whole_symbol(monkeypatch) -> None:
    """A no-time row missing columns must reject the whole symbol."""
    bad_rows = [_no_time_row(), [9.95, 10.0, 10.1, 9.9, 1000.0]]  # missing amount
    bad_payload = _no_time_payload(bad_rows)

    responses = {"000001": bad_payload, "600000": _no_time_payload()}
    client, _ = _make_client(monkeypatch, responses)

    result = await client.batch_get_early_market_data(
        ["000001", "600000"], expected_trade_date=TRADE_DATE
    )

    assert "000001" not in result
    assert "600000" in result
    assert TushareRealtimeClient._parse_rt_min_daily("000001", bad_payload) is None
    assert (
        TushareRealtimeClient._parse_rt_min_daily(
            "000001", bad_payload, expected_trade_date=TRADE_DATE
        )
        is None
    )


@pytest.mark.parametrize(
    "row_index, field, bad_value",
    [
        (0, "open", True),
        (0, "open", False),
        (2, "close", True),
        (2, "close", False),
    ],
)
@pytest.mark.asyncio
async def test_no_time_invalid_value_at_row_ends_rejects_symbol(
    row_index: int, field: str, bad_value: object, monkeypatch
) -> None:
    """Invalid values at the first/last row endpoints must still reject the symbol."""
    rows = [
        _no_time_row(),
        _no_time_row(close_price=10.1),
        _no_time_row(close_price=10.2),
    ]
    field_pos = {f: i for i, f in enumerate(_NO_TIME_FIELDS)}
    rows[row_index][field_pos[field]] = bad_value  # type: ignore[index]
    bad_payload = _no_time_payload(rows)

    responses = {"000001": bad_payload, "600000": _no_time_payload()}
    client, _ = _make_client(monkeypatch, responses)

    result = await client.batch_get_early_market_data(
        ["000001", "600000"], expected_trade_date=TRADE_DATE
    )

    assert "000001" not in result
    assert "600000" in result
    assert TushareRealtimeClient._parse_rt_min_daily("000001", bad_payload) is None
    assert (
        TushareRealtimeClient._parse_rt_min_daily(
            "000001", bad_payload, expected_trade_date=TRADE_DATE
        )
        is None
    )


@pytest.mark.asyncio
async def test_source_hash_includes_code_when_early_bars_empty(monkeypatch) -> None:
    """Only 09:40+ rows => empty early_bars, but source_hash must still differ per code."""
    payload_a = _daily_payload({"09:40": 10.0})
    payload_b = _daily_payload({"09:40": 10.0})

    client_a, _ = _make_client(monkeypatch, {"000001": payload_a})
    client_b, _ = _make_client(monkeypatch, {"000002": payload_b})

    hash_a = (
        await client_a.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash
    hash_b = (
        await client_b.batch_get_early_market_data(["000002"], expected_trade_date=TRADE_DATE)
    )["000002"].source_hash

    assert hash_a != hash_b


@pytest.mark.asyncio
async def test_source_hash_empty_ignores_field_order_and_metadata(monkeypatch) -> None:
    """Empty-early hash must be stable under response field order and transport envelope."""
    rows = [dict(zip(_FIELDS, _row("09:40", 10.0)))]

    def build(fields_order: list[str], **envelope: object) -> dict[str, object]:
        return {
            **envelope,
            "data": {
                "fields": fields_order,
                "items": [[row[field] for field in fields_order] for row in rows],
            },
        }

    order_a = list(_FIELDS)
    order_b = list(reversed(_FIELDS))

    client_a, _ = _make_client(monkeypatch, {"000001": build(order_a, request_id="a")})
    client_b, _ = _make_client(monkeypatch, {"000001": build(order_b, request_id="b")})

    hash_a = (
        await client_a.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash
    hash_b = (
        await client_b.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash

    assert hash_a == hash_b


@pytest.mark.asyncio
async def test_legacy_source_hash_differs_per_code(monkeypatch) -> None:
    """No-time legacy hash with identical facts must still encode the stock code."""
    payload = _no_time_payload([_no_time_row()])

    client_a, _ = _make_client(monkeypatch, {"000001": payload})
    client_b, _ = _make_client(monkeypatch, {"000002": payload})

    hash_a = (
        await client_a.batch_get_early_market_data(["000001"], expected_trade_date=TRADE_DATE)
    )["000001"].source_hash
    hash_b = (
        await client_b.batch_get_early_market_data(["000002"], expected_trade_date=TRADE_DATE)
    )["000002"].source_hash

    assert hash_a != hash_b
