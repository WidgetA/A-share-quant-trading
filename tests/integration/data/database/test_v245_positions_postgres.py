"""Real SQL coverage of references/intent identity; no broker execution is modeled."""

from datetime import date
from types import SimpleNamespace

import pytest

from src.data.database.v20_repository import sha256_json
from src.data.database.v22_positions import V22PositionStore, register_selection
from src.data.database.v245_switch_positions import migration_sql, register_v245_selection
from tests.integration.data.database.test_v20_outbox_postgres import (
    repository as _repository_fixture,
)

pytestmark = pytest.mark.postgres
repository = _repository_fixture
DAY = date(2026, 11, 16)
CONFIG = SimpleNamespace(
    official_stream_id="switch-test", state_lineage_id="existing", strategy_version="V24.5"
)


def decision(permit):
    return SimpleNamespace(
        strategy_version="V24.5",
        action="ENTER" if permit else "BLOCK",
        official_stream_id=CONFIG.official_stream_id,
        lineage_id=CONFIG.state_lineage_id,
        trade_date=DAY,
        semantic={
            "symbols": [
                {"code": "000001", "name": "Stock A"},
                {"code": "600000", "name": "Stock B"},
            ],
            "v245_transition": {"signal_status": "KNOWN", "permit": permit, "trade_date": str(DAY)},
        },
    )


async def event(con, schema, key):
    event_id = sha256_json(["switch-test", key])
    await con.execute(
        f"INSERT INTO {schema}.outbox_events "
        "(event_id,event_type,route_id,official_stream_id,lineage_id,semantic_content_hash,"
        "semantic_json,action_expiry_ts) VALUES ($1,'ENTRY_DECISION','test',$2,$3,$4,'{}',"
        "clock_timestamp()+interval '1 day')",
        event_id,
        CONFIG.official_stream_id,
        CONFIG.state_lineage_id,
        sha256_json({}),
    )
    return event_id


@pytest.mark.parametrize("sequence", [(False, True, False), (True, False, True)])
async def test_same_day_targets_share_neither_batch_slot_nor_fake_fills(repository, sequence):
    instance, pool, schema = repository
    # Idempotent migration cannot delete existing reference records.
    await pool.execute(migration_sql(schema))
    store = V22PositionStore(instance, CONFIG)
    for n, permit in enumerate(sequence):
        async with pool.acquire() as con, con.transaction():
            event_id = await event(con, schema, n)
            await register_selection(con, schema, decision(permit), event_id)
            await register_v245_selection(con, schema, decision(permit), event_id)
            # Transport retry does not duplicate either references or obligations.
            await register_selection(con, schema, decision(permit), event_id)
            await register_v245_selection(con, schema, decision(permit), event_id)
    positions = await store.list(unclosed=True)
    assert len(positions) == 3
    assert sorted(p["asset_kind"] for p in positions) == ["BANK", "STOCK", "STOCK"]
    assert all(p["entry_price"] is None and p["quantity"] is None for p in positions)
    assert all(p["status"] == "MONITORING" for p in positions)
    assert len(await store.list(active=True)) == 2  # BANK never reaches R33.
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v22_alert_batches") == 2
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v245_switch_intents") == 2
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.outbox_events") == 3
    latest = await pool.fetchrow(
        f"SELECT target,last_event_id FROM {schema}.v245_switch_intents "
        "ORDER BY updated_at DESC LIMIT 1"
    )
    assert latest["target"] == ("STOCKS" if sequence[-1] else "BANK")
    assert latest["last_event_id"] == sha256_json(["switch-test", 2])


async def test_notified_stock_remains_switchable_and_calibration_closes_actual_record(repository):
    instance, pool, schema = repository
    store = V22PositionStore(instance, CONFIG)
    async with pool.acquire() as con, con.transaction():
        first = await event(con, schema, "stocks")
        old = decision(True)
        old.strategy_version = "V22-slim"
        await register_selection(con, schema, old, first)
        await con.execute(f"UPDATE {schema}.v22_alert_positions SET alert_event_id=$1", first)
        next_event = await event(con, schema, "bank")
        await register_v245_selection(con, schema, decision(False), next_event)
    pending = await store.pending_switches()
    assert len(pending) == 2
    assert all(p["alert_event_id"] == first for p in pending)
    assert all(p["switch_target"] == "BANK" for p in pending)
    corrected = await store.calibrate(
        pending[0]["position_id"],
        "actual-sale",
        {
            "expected_revision": 0,
            "quantity": 0,
        },
    )
    assert corrected["status"] == "CLOSED"
    assert len(await store.pending_switches()) == 1
    assert (
        await store.calibrate(
            pending[0]["position_id"],
            "actual-sale",
            {
                "expected_revision": 0,
                "quantity": 0,
            },
        )
    ) == corrected
    # Recommending BANK again cannot undo that correction or create a second bank reference.
    async with pool.acquire() as con, con.transaction():
        third = await event(con, schema, "bank-repeat")
        await register_v245_selection(con, schema, decision(False), third)
    assert len(await store.pending_switches()) == 1
    bank = [p for p in await store.list() if p["asset_kind"] == "BANK"]
    assert len(bank) == 1 and bank[0]["quantity"] is None


async def test_switch_persistence_rolls_back_with_selection_transaction(repository):
    _instance, pool, schema = repository
    with pytest.raises(RuntimeError, match="rollback"):
        async with pool.acquire() as con, con.transaction():
            event_id = await event(con, schema, "rollback")
            await register_v245_selection(con, schema, decision(False), event_id)
            raise RuntimeError("rollback")
    for table in (
        "v245_switch_intents",
        "v245_position_switches",
        "v22_alert_batches",
        "v22_alert_positions",
        "outbox_events",
    ):
        assert await pool.fetchval(f"SELECT count(*) FROM {schema}.{table}") == 0
