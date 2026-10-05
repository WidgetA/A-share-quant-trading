"""HTTP contract tests; live Greptime persistence must be verified independently."""

import json
from urllib.parse import parse_qs

import httpx
import pytest

from src.data.cross_market_store import (
    PRICE_COLUMNS,
    PRICE_DDL,
    PRICE_KEYS,
    REFERENCE_COLUMNS,
    REFERENCE_DDL,
    CrossMarketStore,
    GreptimeError,
    GreptimeReadbackError,
    GreptimeWriteError,
)


def point(**overrides):
    value = {
        "provider": "yahoo",
        "market": "US",
        "symbol": "^SOX",
        "interval": "1d",
        "data_kind": "daily_bar",
        "ts": 1728000000123,
        "name": "Semiconductor Index",
        "currency": "USD",
        "exchange_timezone": "America/New_York",
        "trade_date": "2024-10-04",
        "open": 100.25,
        "high": 105.5,
        "low": 98.125,
        "close": 103.75,
        "adjusted_close": 103.75,
        "volume": 0,
        "is_final": True,
        "fetched_at": 1728100000456,
    }
    value.update(overrides)
    return value


def reference(**overrides):
    value = {
        "provider": "yahoo",
        "market": "US",
        "sw_code": "270100",
        "reference_at": 1728000000000,
        "mapping_json": {
            "board_id": "US:test:semiconductors",
            "status": "verified",
            "index": {"symbol": "^SOX", "quote_type": "INDEX"},
            "evidence": ["Source's 原始说明"],
        },
        "fetched_at": 1728100000456,
    }
    value.update(overrides)
    return value


def sql_from(request):
    assert request.method == "POST"
    assert request.url.path == "/v1/sql"
    assert request.url.params["db"] == "public"
    assert request.headers["X-Greptime-Timezone"] == "+00:00"
    assert request.headers["Content-Type"] == "application/x-www-form-urlencoded"
    return parse_qs(request.content.decode())["sql"][0]


def affected(count):
    return httpx.Response(200, json={"output": [{"affectedrows": count}]})


def records(rows, columns=PRICE_COLUMNS):
    return httpx.Response(
        200,
        json={
            "output": [
                {
                    "records": {
                        "schema": {"column_schemas": [{"name": name} for name in columns]},
                        "rows": [[row[name] for name in columns] for row in rows],
                        "total_rows": len(rows),
                    }
                }
            ]
        },
    )


@pytest.mark.asyncio
async def test_schema_has_deduplicating_series_keys_and_source_time():
    calls = []

    def handler(request):
        calls.append(sql_from(request))
        return affected(0)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = CrossMarketStore("http://greptime.test/", client=client)
        await store.ensure_schema()
    assert calls == [PRICE_DDL, REFERENCE_DDL]
    assert 'TIME INDEX ("ts")' in PRICE_DDL
    assert 'PRIMARY KEY ("provider", "market", "symbol", "interval", "data_kind")' in PRICE_DDL
    assert 'TIME INDEX ("reference_at")' in REFERENCE_DDL
    assert 'PRIMARY KEY ("provider", "market", "sw_code")' in REFERENCE_DDL
    assert "'merge_mode'='last_row'" in PRICE_DDL
    assert "'merge_mode'='last_row'" in REFERENCE_DDL
    assert all("CREATE TABLE IF NOT EXISTS cross_market_" in sql for sql in calls)


@pytest.mark.asyncio
async def test_sql_escaping_source_milliseconds_and_snapshot_nulls_are_preserved():
    calls = []

    def handler(request):
        calls.append(sql_from(request))
        return affected(1)

    snapshot = point(
        market="KR",
        symbol="^KS'11",
        name="O'Reilly 한국",
        interval="quote",
        data_kind="quote_snapshot",
        ts=1728001234567,
        currency="KRW",
        exchange_timezone="Asia/Seoul",
        open=None,
        high=None,
        low=None,
        adjusted_close=None,
        volume=None,
        is_final=None,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = CrossMarketStore("http://greptime.test", client=client)
        assert await store.upsert_prices([snapshot]) == 1
    sql = calls[0]
    assert "'^KS''11'" in sql and "'O''Reilly 한국'" in sql
    assert "1728001234567" in sql and "1728100000456" in sql
    assert '"interval"' in sql and '"open"' in sql and '"volume"' in sql
    assert sql.count("NULL") == 6
    assert ", NULL, NULL, NULL, 103.75, NULL, NULL, NULL," in sql


@pytest.mark.asyncio
async def test_full_reference_json_and_version_timestamp_survive_storage():
    calls = []

    def handler(request):
        calls.append(sql_from(request))
        return affected(1)

    original = reference()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = CrossMarketStore("http://greptime.test", client=client)
        assert await store.upsert_industry_indices([original]) == 1
    expected_json = json.dumps(
        original["mapping_json"], ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    assert "'" + expected_json.replace("'", "''") + "'" in calls[0]
    assert str(original["reference_at"]) in calls[0]
    assert str(original["fetched_at"]) in calls[0]
    assert original["mapping_json"]["evidence"] == ["Source's 原始说明"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, json={"error": "invalid SQL", "code": 1004}),
        httpx.Response(200, json={"code": 1004, "output": [{"affectedrows": 1}]}),
        httpx.Response(200, json={"output": [{"error": "region failure"}]}),
        httpx.Response(200, json={"output": [{"code": 1004, "affectedrows": 1}]}),
        httpx.Response(503, json={"error": "temporarily unavailable"}),
        httpx.Response(200, text="<html>upstream login</html>"),
        httpx.Response(200, json={"output": []}),
        httpx.Response(200, json={"output": [{"affectedrows": True}]}),
        httpx.Response(200, json={"output": [{"affectedrows": 0}]}),
    ],
)
async def test_failure_is_never_reported_as_success(response):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response)) as client:
        store = CrossMarketStore("http://greptime.test", client=client)
        with pytest.raises(GreptimeWriteError) as failure:
            await store.upsert_prices([point()])
    assert failure.value.confirmed_rows == 0


@pytest.mark.asyncio
async def test_partial_batches_raise_and_the_whole_window_can_be_replayed():
    calls = []
    failed_once = False

    def handler(request):
        nonlocal failed_once
        sql = sql_from(request)
        calls.append(sql)
        if len(calls) == 2 and not failed_once:
            failed_once = True
            raise httpx.ReadTimeout("response lost", request=request)
        return affected(1)

    source = [point(ts=1728000000000 + index) for index in range(3)]
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = CrossMarketStore("http://greptime.test", client=client, batch_size=1)
        with pytest.raises(GreptimeWriteError) as failure:
            await store.upsert_prices(source)
        assert failure.value.confirmed_rows == 1
        assert len(calls) == 2  # Later batches never run after uncertainty.
        assert await store.upsert_prices(source) == 3
    assert calls[0] == calls[2]
    assert calls[1] == calls[3]


@pytest.mark.asyncio
async def test_duplicate_input_identity_uses_last_row_and_replay_keeps_same_source_key():
    calls = []

    def handler(request):
        calls.append(sql_from(request))
        return affected(1)

    first = point(close=100.0)
    updated = point(close=101.0, fetched_at=1728100000457)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = CrossMarketStore("http://greptime.test", client=client)
        assert await store.upsert_prices([first, updated]) == 1
        assert await store.upsert_prices([updated]) == 1
    assert calls[0] == calls[1]
    assert calls[0].count(str(updated["ts"])) == 1
    assert ", 101.0," in calls[0]
    assert ", 100.0," not in calls[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"ts": 1728000000.123},
        {"ts": True},
        {"fetched_at": "1728100000456"},
        {"fetched_at": 2**63},
        {"close": float("nan")},
        {"high": float("inf")},
        {"volume": False},
        {"is_final": 1},
        {"symbol": ""},
    ],
)
async def test_all_rows_are_validated_before_any_mutation(overrides):
    calls = []

    def handler(request):
        calls.append(sql_from(request))
        return affected(1)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = CrossMarketStore("http://greptime.test", client=client, batch_size=1)
        with pytest.raises(ValueError):
            await store.upsert_prices([point(), point(**{"ts": 1728000000124, **overrides})])
    assert calls == []


@pytest.mark.asyncio
async def test_readback_checks_values_nulls_and_source_keys():
    expected = point()
    requests = []

    def handler(request):
        requests.append(sql_from(request))
        return records([expected])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = CrossMarketStore("http://greptime.test", client=client)
        assert await store.verify_prices([expected]) == 1
    sql = requests[0]
    assert 'CAST("ts" AS BIGINT) AS "ts"' in sql
    assert '"ts" IN (CAST(1728000000123 AS TIMESTAMP(3)))' in sql
    assert all(f'"{field}" =' in sql for field in PRICE_KEYS[:-1])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["missing", "wrong_time", "changed_price", "fabricated_zero", "duplicate", "wrong_bool"]
)
async def test_readback_rejects_missing_wrong_or_duplicate_facts(case):
    expected = point(volume=None)
    rows = [expected.copy()]
    if case == "missing":
        rows = []
    elif case == "wrong_time":
        rows[0]["ts"] += 1
    elif case == "changed_price":
        rows[0]["close"] += 1
    elif case == "fabricated_zero":
        rows[0]["volume"] = 0
    elif case == "duplicate":
        rows.append(expected.copy())
    elif case == "wrong_bool":
        rows[0]["is_final"] = 1
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: records(rows))
    ) as client:
        store = CrossMarketStore("http://greptime.test", client=client)
        with pytest.raises(GreptimeReadbackError):
            await store.verify_prices([expected])


@pytest.mark.asyncio
async def test_mapping_readback_checks_complete_json_and_version_identity():
    expected = reference()
    persisted = expected.copy()
    persisted["mapping_json"] = json.dumps(
        expected["mapping_json"], ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    calls = []

    def handler(request):
        calls.append(sql_from(request))
        return records([persisted], REFERENCE_COLUMNS)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = CrossMarketStore("http://greptime.test", client=client)
        assert await store.verify_industry_indices([expected]) == 1
    assert '"reference_at" IN (CAST(1728000000000 AS TIMESTAMP(3)))' in calls[0]
    assert "\"sw_code\" = '270100'" in calls[0]


@pytest.mark.asyncio
async def test_count_filters_are_escaped_and_match_real_series_columns():
    calls = []

    def handler(request):
        calls.append(sql_from(request))
        return records([{"row_count": 7}], ("row_count",))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = CrossMarketStore("http://greptime.test", client=client)
        assert await store.count_prices(market="KR", data_kind="quote_snapshot", symbol="X'Y") == 7
        assert await store.count_industry_indices(provider="yahoo", sw_code="270100") == 7
        with pytest.raises(ValueError):
            await store.count_prices(unsafe_column="value")
    assert "\"symbol\" = 'X''Y'" in calls[0]
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_supplied_client_is_not_closed_and_empty_inputs_issue_no_sql():
    calls = []

    def handler(request):
        calls.append(request)
        return affected(0)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        async with CrossMarketStore("http://greptime.test", client=client) as store:
            assert await store.upsert_prices([]) == 0
            assert await store.upsert_industry_indices([]) == 0
            assert await store.verify_prices([]) == 0
            assert await store.verify_industry_indices([]) == 0
        assert not client.is_closed
    assert calls == []


@pytest.mark.asyncio
async def test_schema_sql_error_does_not_continue_to_second_table():
    calls = []

    def handler(request):
        calls.append(sql_from(request))
        return httpx.Response(200, json={"error": "invalid DDL", "code": 1004})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = CrossMarketStore("http://greptime.test", client=client)
        with pytest.raises(GreptimeError):
            await store.ensure_schema()
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mapping", ['{"bad":NaN}', '["missing object"]', "{broken", {"bad": float("inf")}]
)
async def test_invalid_mapping_json_never_reaches_database(mapping):
    calls = []

    def handler(request):
        calls.append(request)
        return affected(1)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = CrossMarketStore("http://greptime.test", client=client)
        with pytest.raises(ValueError):
            await store.upsert_industry_indices([reference(), reference(mapping_json=mapping)])
    assert calls == []


@pytest.mark.asyncio
async def test_readback_compiles_series_tags_once_and_preserves_exact_timestamp_set():
    requested = [point(ts=1728000000123), point(ts=1728000000456), point(ts=1728000000789)]
    calls = []

    def handler(request):
        calls.append(sql_from(request))
        return records(requested)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = CrossMarketStore("http://greptime.test", client=client)
        assert await store.verify_prices(requested) == 3
    assert len(calls) == 1
    sql = calls[0]
    for field in PRICE_KEYS[:-1]:
        assert sql.count(f'"{field}" =') == 1
    assert (
        '"ts" IN (CAST(1728000000123 AS TIMESTAMP(3)), '
        "CAST(1728000000456 AS TIMESTAMP(3)), CAST(1728000000789 AS TIMESTAMP(3)))" in sql
    )
    assert '"ts" >=' not in sql and '"ts" <=' not in sql


@pytest.mark.asyncio
async def test_distinct_series_keep_separate_exact_timestamp_groups():
    requested = [
        point(ts=1728000000123),
        point(market="KR", symbol="KOSPI-25.KS", ts=1728000000456),
    ]
    calls = []

    def handler(request):
        calls.append(sql_from(request))
        return records(requested)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = CrossMarketStore("http://greptime.test", client=client)
        assert await store.verify_prices(requested) == 2
    clauses = calls[0].split(" WHERE ", 1)[1].split(" OR ")
    assert len(clauses) == 2
    assert "\"market\" = 'US'" in clauses[0] and "\"symbol\" = '^SOX'" in clauses[0]
    assert "IN (CAST(1728000000123 AS TIMESTAMP(3)))" in clauses[0]
    assert "\"market\" = 'KR'" in clauses[1] and "\"symbol\" = 'KOSPI-25.KS'" in clauses[1]
    assert "IN (CAST(1728000000456 AS TIMESTAMP(3)))" in clauses[1]
