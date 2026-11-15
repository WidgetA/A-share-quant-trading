from contextlib import asynccontextmanager
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

from src.data.database.v22_positions import V22PositionStore, register_selection

DAY = date(2026, 11, 16)
CALENDAR = (date(2026, 11, 13), DAY, date(2026, 11, 17))


def position(**changes):
    return dict(
        position_id="old-stock",
        code="000001",
        stock_name="Example",
        entry_date="2026-11-13",
        quantity=None,
        status="MONITORING",
        asset_kind="STOCK",
        alert_event_id="already-notified",
        **changes,
    )


def commit(*, permit=False, status="KNOWN", action="BLOCK", version="V24.5"):
    return SimpleNamespace(
        strategy_version=version,
        action=action,
        official_stream_id="live",
        lineage_id="existing",
        trade_date=DAY,
        semantic={
            "symbols": [{"code": "000001", "name": "Example"}],
            "v245_transition": {
                "signal_status": status,
                "permit": permit,
                "raw_open": permit if status == "KNOWN" else None,
                "trade_date": str(DAY),
                "state_after": {"mode": "NORMAL"},
            },
        },
    )


def store_with(connection):
    @asynccontextmanager
    async def acquire():
        yield connection

    return V22PositionStore(
        SimpleNamespace(schema="v20", pool=SimpleNamespace(acquire=acquire)),
        SimpleNamespace(
            official_stream_id="live", state_lineage_id="existing", strategy_version="V24.5"
        ),
    )


async def test_v245_stock_registration_is_not_silently_skipped_or_given_fake_cost():
    con = SimpleNamespace(fetchval=AsyncMock(return_value="batch"), execute=AsyncMock())
    await register_selection(con, "v20", commit(permit=True, action="ENTER"), "event")
    assert con.execute.await_count == 1
    sql, *args = con.execute.call_args.args
    assert "STOCK" in args and "V24.5" in args
    assert "entry_price" not in sql and "quantity" not in sql
    assert None in args  # no fictional 09:41 cost on an unconfirmed bank restoration


async def test_all_unclosed_includes_already_alerted_but_r33_excludes_bank():
    con = SimpleNamespace(fetch=AsyncMock(return_value=[]))
    store = store_with(con)
    await store.list(unclosed=True)
    sql = con.fetch.call_args.args[0]
    assert "alert_event_id IS NULL" not in sql
    assert "MONITORING" in sql and "quantity" in sql
    await store.list(active=True)
    sql = con.fetch.call_args.args[0]
    assert "alert_event_id IS NULL" in sql and "asset_kind='STOCK'" in sql


def test_close_plan_preserves_t1_and_unknown_quantity_without_fabricated_sale():
    from src.data.database.v245_switch_positions import build_switch_plan

    old = position()
    today = dict(old, position_id="new", entry_date=str(DAY))
    closed = dict(old, position_id="closed", status="CLOSED", quantity=0)
    plan = build_switch_plan([old, today, closed], day=DAY, permit=False, calendar=CALENDAR)
    assert plan["target"] == "BANK"
    assert len(plan["sell_positions"]) == 2
    assert plan["sell_positions"][0]["t1_eligible"] is True
    assert plan["sell_positions"][1]["t1_eligible"] is False
    assert plan["sell_positions"][1]["earliest_sell_date"] == "2026-11-17"
    assert plan["sell_positions"][0]["quantity"] is None
    assert plan["notification_only"] is True
    assert plan["buy_instruction"] == "ICBC_WITH_ACTUALLY_AVAILABLE_CASH"
    assert "fill_price" not in str(plan) and "cash_proceeds" not in str(plan)
    assert old["status"] == "MONITORING"


def test_restoration_plan_uses_bank_first_and_never_invents_0942_fill():
    from src.data.database.v245_switch_positions import build_switch_plan

    bank = dict(position(), position_id="bank", code="601398", asset_kind="BANK")
    plan = build_switch_plan([position(), bank], day=DAY, permit=True, calendar=CALENDAR)
    assert plan["target"] == "STOCKS"
    assert [p["code"] for p in plan["sell_positions"]] == ["601398"]
    assert plan["buy_instruction"] == "ORIGINAL_STOCK_ALLOCATION_AFTER_BANK_PROCEEDS"
    assert plan["reference_clock"] is None
    assert plan["requires_actual_proceeds"] is True


def test_unknown_input_has_no_action_plan():
    from src.data.database.v245_switch_positions import build_switch_plan

    assert build_switch_plan([position()], day=DAY, permit=None, calendar=CALENDAR) is None


@pytest.mark.parametrize("bad_calendar", [(), (date(2026, 11, 13),)])
def test_missing_calendar_cannot_turn_t1_into_sellable(bad_calendar):
    from src.data.database.v245_switch_positions import build_switch_plan

    with pytest.raises(ValueError, match="calendar"):
        build_switch_plan([position()], day=DAY, permit=False, calendar=bad_calendar)


async def test_unknown_transition_never_creates_bank_record_or_intent():
    from src.data.database.v245_switch_positions import register_v245_selection

    con = SimpleNamespace(
        execute=AsyncMock(), fetch=AsyncMock(), fetchrow=AsyncMock(), fetchval=AsyncMock()
    )
    assert await register_v245_selection(con, "v20", commit(status="UNKNOWN"), "e") is None
    con.execute.assert_not_awaited()
    con.fetchval.assert_not_awaited()


async def test_known_bank_intent_is_once_daily_and_creates_only_unknown_reference():
    from src.data.database.v245_switch_positions import register_v245_selection

    con = SimpleNamespace(
        execute=AsyncMock(),
        fetch=AsyncMock(return_value=[position()]),
        fetchrow=AsyncMock(return_value=None),
        fetchval=AsyncMock(side_effect=["intent", "batch"]),
    )
    result = await register_v245_selection(con, "v20", commit(), "e")
    assert result["target"] == "BANK"
    sqls = [c.args[0] for c in con.execute.call_args_list]
    assert not any("UPDATE" in sql and "v22_alert_positions" in sql for sql in sqls)
    bank = next(
        c for c in con.execute.call_args_list if "INSERT INTO v20.v22_alert_positions" in c.args[0]
    )
    assert "BANK" in bank.args and "601398" in bank.args
    assert "entry_price" not in bank.args[0] and "quantity" not in bank.args[0]
    assert all("outbox_events" not in sql for sql in sqls)
    retry = SimpleNamespace(
        execute=AsyncMock(),
        fetchval=AsyncMock(return_value=None),
        fetch=AsyncMock(),
        fetchrow=AsyncMock(return_value={"target": "BANK"}),
    )
    await register_v245_selection(retry, "v20", commit(), "new-run")
    retry.execute.assert_not_awaited()


async def test_continuing_bank_phase_does_not_register_another_assumed_purchase():
    from src.data.database.v245_switch_positions import register_v245_selection

    con = SimpleNamespace(
        execute=AsyncMock(),
        fetch=AsyncMock(return_value=[]),
        fetchrow=AsyncMock(return_value={"target": "BANK"}),
        fetchval=AsyncMock(return_value="new-day"),
    )
    await register_v245_selection(con, "v20", commit(), "e")
    assert not any("v22_alert_positions" in c.args[0] for c in con.execute.call_args_list)


def test_migration_is_additive_and_preserves_old_records():
    from src.data.database.v245_switch_positions import migration_sql

    sql = migration_sql("scope")
    assert "scope.v245_switch_intents" in sql
    assert "scope.v245_position_switches" in sql
    assert "DEFAULT 'STOCK'" in sql
    assert "DROP TABLE" not in sql and "DELETE " not in sql
    with pytest.raises(ValueError):
        migration_sql("bad;schema")


async def test_original_v22_registration_is_unchanged():
    con = SimpleNamespace(fetchval=AsyncMock(return_value="batch"), execute=AsyncMock())
    await register_selection(con, "v20", commit(action="ENTER", version="V22-slim"), "e")
    assert con.execute.await_count == 1
    assert len(con.execute.call_args.args) == 6
    assert "asset_kind" not in con.execute.call_args.args[0]


@pytest.mark.parametrize(
    "version,status,permit",
    [("V20", "KNOWN", True), ("V24.5", "UNKNOWN", True), ("V24.5", "KNOWN", False)],
)
async def test_non_permitted_or_other_strategy_never_gets_stock_references(version, status, permit):
    con = SimpleNamespace(fetchval=AsyncMock(), execute=AsyncMock())
    await register_selection(
        con, "v20", commit(action="ENTER", version=version, status=status, permit=permit), "e"
    )
    con.execute.assert_not_awaited()


async def test_bank_record_cannot_be_given_r33_reference_or_signal_even_when_called_directly():
    @asynccontextmanager
    async def transaction():
        yield

    con = SimpleNamespace(transaction=transaction, execute=AsyncMock())
    store = store_with(con)
    store._locked = AsyncMock(return_value=dict(position(), asset_kind="BANK"))
    assert await store.apply(position(), reference=10, signal=object()) is None
    con.execute.assert_not_awaited()


async def test_pending_switches_do_not_disappear_after_r33_alert_or_target_changes():
    old = dict(position(), requested_date="2026-11-13", switch_target="BANK")
    con = SimpleNamespace(fetch=AsyncMock(return_value=[old]))
    rows = await store_with(con).pending_switches()
    assert rows[0]["alert_event_id"] == "already-notified"
    assert rows[0]["switch_target"] == "BANK"
    sql = con.fetch.call_args.args[0]
    assert "alert_event_id IS NULL" not in sql
    assert "status='MONITORING'" in sql


def test_same_day_bank_reference_is_t1_pending_and_uncorrected_remains_unknown():
    from src.data.database.v245_switch_positions import build_switch_plan

    bank = dict(position(), code="601398", asset_kind="BANK", entry_date=str(DAY))
    plan = build_switch_plan([bank], day=DAY, permit=True, calendar=CALENDAR)
    assert not plan["sell_positions"][0]["t1_eligible"]
    assert not plan["sell_positions"][0]["quantity_confirmed"]
    assert not plan["dividend_receivable_is_available_cash"]
    assert plan["cash_scope"] == "ORIGINAL_STOCK_ALLOCATION"


def test_bank_and_stock_daily_batches_have_separate_unique_identity():
    from src.data.database.v245_switch_positions import migration_sql

    sql = migration_sql()
    assert "reference_kind TEXT NOT NULL DEFAULT 'STOCK'" in sql
    assert "(official_stream_id,lineage_id,trade_date,reference_kind)" in sql


async def test_same_day_new_target_is_a_new_intent_without_advancing_policy_here():
    from src.data.database.v245_switch_positions import register_v245_selection

    con = SimpleNamespace(
        execute=AsyncMock(),
        fetch=AsyncMock(return_value=[]),
        fetchrow=AsyncMock(return_value={"target": "BANK"}),
        fetchval=AsyncMock(return_value="intent"),
    )
    await register_v245_selection(con, "v20", commit(permit=True, action="ENTER"), "new-e")
    sql = con.fetchval.call_args.args[0]
    assert "trade_date,target) DO UPDATE" in sql
    assert "last_event_id<>EXCLUDED.last_event_id" in sql
    assert "trade_date<=$3" in con.fetchrow.call_args.args[0]
    assert not any("policy_state" in call.args[0] for call in con.execute.call_args_list)


def switch_store(*, target="BANK", existing=None, current=None):
    @asynccontextmanager
    async def transaction():
        yield

    pending = dict(
        intent_id="intent",
        switch_target="BANK",
        current_target=target,
        current_target_event_id="latest",
        switch_alert_event_id=existing,
    )
    con = SimpleNamespace(
        transaction=transaction,
        execute=AsyncMock(),
        fetchrow=AsyncMock(side_effect=[{"revision": 0}, pending]),
    )
    store = store_with(con)
    record = dict(position(), revision=0)
    store._locked = AsyncMock(return_value=current or record)
    return store, con, record


def switch_quote(**changes):
    return dict(
        trigger_at=datetime(2026, 11, 16, 9, 41, tzinfo=ZoneInfo("Asia/Shanghai")),
        trigger_price=10.0,
        quote_volume=1000.0,
        down_limit=9.0,
        quote_source="rt_min_daily",
        **changes,
    )


async def test_switch_alert_is_separate_from_r33_and_never_closes_the_holding():
    store, con, record = switch_store()
    event_id = await store.apply_switch(record, **switch_quote())
    assert event_id and con.execute.await_count == 2
    first, second = con.execute.call_args_list
    assert "outbox_events" in first.args[0]
    payload = __import__("json").loads(first.args[-1])
    assert payload["alert_code"] == "V245_SWITCH_EXIT_ALERT"
    assert payload["quantity"] is None and payload["notification_only"]
    assert payload["target_event_id"] == "latest"
    assert "switch_alert_event_id" in second.args[0]
    assert "v22_alert_positions" not in second.args[0]


async def test_pending_bank_sale_is_suppressed_when_latest_target_has_changed_back():
    store, con, record = switch_store(target="STOCKS")
    assert await store.apply_switch(record, **switch_quote()) is None
    con.execute.assert_not_awaited()


async def test_switch_alert_retry_keeps_same_event_and_does_not_repeat_writes():
    store, con, record = switch_store(existing="old-switch-alert")
    assert await store.apply_switch(record, **switch_quote()) == "old-switch-alert"
    con.execute.assert_not_awaited()


@pytest.mark.parametrize("case", ["t1", "closed", "zero", "stale"])
async def test_switch_alert_respects_actual_holding_changes(case):
    current = dict(position(), revision=0)
    current.update(
        {
            "t1": {"entry_date": str(DAY)},
            "closed": {"status": "CLOSED"},
            "zero": {"quantity": 0},
            "stale": {"revision": 1},
        }[case]
    )
    store, con, record = switch_store(current=current)
    assert await store.apply_switch(record, **switch_quote()) is None
    con.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "change",
    [
        {"quote_volume": 0},
        {"trigger_price": 9},
        {"down_limit": float("nan")},
        {"trigger_at": datetime(2026, 11, 16, 9, 41)},
    ],
)
async def test_invalid_switch_quote_never_becomes_a_sell_alert(change):
    store, con, record = switch_store()
    with pytest.raises(ValueError):
        await store.apply_switch(record, **(switch_quote() | change))
    con.execute.assert_not_awaited()


async def test_locked_current_target_time_blocks_stale_quote_even_if_monitor_raced():
    store, con, record = switch_store()
    con.fetchrow.side_effect = [
        {"revision": 0},
        dict(
            intent_id="intent",
            switch_target="BANK",
            current_target="BANK",
            current_target_event_id="latest",
            switch_alert_event_id=None,
            current_target_available_at=switch_quote()["trigger_at"].replace(second=30),
        ),
    ]
    assert await store.apply_switch(record, **switch_quote()) is None
    con.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "changed_phase,sent,reason,creates",
    [
        (True, False, "V245_TARGET_SUPERSEDED", True),
        (False, False, "V245_TARGET_SUPERSEDED", False),
        (True, True, "V245_TARGET_SUPERSEDED", False),
        (True, False, "V245_POSITION_NO_LONGER_SWITCHABLE", False),
    ],
)
async def test_only_new_phase_can_replace_a_permanently_suppressed_unsent_alert(
    changed_phase, sent, reason, creates
):
    store, con, record = switch_store(existing="old-alert")
    pending = dict(
        intent_id="old-intent",
        switch_target="BANK",
        current_target="BANK",
        current_target_event_id="latest",
        switch_alert_event_id="old-alert",
        phase_event_id="old-phase",
        current_phase_event_id="new-phase" if changed_phase else "old-phase",
        current_intent_id="new-intent",
        switch_suppression_reason=reason,
        switch_delivery_status="SENT" if sent else "PENDING",
    )
    con.fetchrow.side_effect = [{"revision": 0}, pending]
    result = await store.apply_switch(record, **switch_quote())
    if creates:
        assert result and result != "old-alert"
        assert con.execute.await_count == 2
        semantic = __import__("json").loads(con.execute.call_args_list[0].args[-1])
        assert semantic["switch_phase_event_id"] == "new-phase"
        assert semantic["replaces_suppressed_alert_id"] == "old-alert"
        assert all(
            "UPDATE" not in c.args[0] or "outbox_events" not in c.args[0]
            for c in con.execute.call_args_list
        )
    else:
        con.execute.assert_not_awaited()


async def test_repeated_same_target_keeps_phase_but_opposite_target_starts_another():
    from src.data.database.v245_switch_positions import register_v245_selection

    for previous_target, expected in [("BANK", "old-phase"), ("STOCKS", "new-event")]:
        con = SimpleNamespace(
            execute=AsyncMock(),
            fetch=AsyncMock(return_value=[]),
            fetchrow=AsyncMock(
                return_value={"target": previous_target, "phase_event_id": "old-phase"}
            ),
            fetchval=AsyncMock(return_value="id"),
        )
        await register_v245_selection(con, "v20", commit(), "new-event")
        assert expected in con.fetchval.call_args_list[0].args[1:]
