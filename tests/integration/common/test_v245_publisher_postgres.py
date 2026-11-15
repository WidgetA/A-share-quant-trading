"""Real dispatch transactions: obsolete instructions never cross mock HTTP.

Only the outward relay is replaced. State, target, holding, lease, suppression,
delivery attempts and receipts use a random isolated PostgreSQL schema.
"""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.common.v20_feishu import V20OutboxPublisher
from src.data.database.v20_repository import sha256_json
from src.data.database.v22_positions import V22PositionStore
from src.strategy.v20.decision_engine import genesis_state
from tests.integration.data.database.test_v20_outbox_postgres import (
    repository as _repository_fixture,
)
from tests.unit.common.test_v20_feishu import _CapturingRelay, _route

pytestmark = pytest.mark.postgres
repository = _repository_fixture
SCOPE = dict(route_id="test", official_stream_id="switch-test", lineage_id="existing")


def publisher(repo, relay):
    return V20OutboxPublisher(repo, {"test": _route(relay)}, worker_id="worker", **SCOPE)


async def insert_event(pool, schema, event_id, kind, semantic, *, marker=None):
    payload = {"message": f"sealed {event_id}", "expired_delivery_message": "expired"}
    await pool.execute(
        f"INSERT INTO {schema}.outbox_events "
        "(event_id,event_type,route_id,official_stream_id,lineage_id,semantic_content_hash,"
        "semantic_json,action_expiry_ts) "
        "VALUES ($1,$2,'test','switch-test','existing',$3,$4::jsonb,"
        "CASE WHEN $2='ENTRY_DECISION' THEN clock_timestamp()+interval '1 day' ELSE NULL END)",
        event_id,
        kind,
        sha256_json(semantic),
        json.dumps(semantic),
    )
    if marker is not None:
        await pool.execute(
            f"UPDATE {schema}.outbox_events SET seal_status='SEALED',payload_json=$2::jsonb,"
            "payload_hash=$3,generated_at=clock_timestamp(),commit_marker=$4 WHERE event_id=$1",
            event_id,
            json.dumps(payload),
            sha256_json(payload),
            marker,
        )


async def change_target(pool, schema, target, *, event_id="new-target-event"):
    semantic = {
        "strategy_version": "V24.5",
        "action": "BLOCK",
        "final_multiplier": 0.0,
        "v245_switch_plan": {"target": target},
    }
    await insert_event(pool, schema, event_id, "ENTRY_DECISION", semantic)
    await pool.execute(
        f"INSERT INTO {schema}.v245_switch_intents "
        "(intent_id,official_stream_id,lineage_id,trade_date,target,source_event_id,last_event_id) "
        "VALUES ($1,'switch-test','existing','2026-11-16',$2,$3,$3) "
        "ON CONFLICT(official_stream_id,lineage_id,trade_date,target) DO UPDATE "
        "SET last_event_id=EXCLUDED.last_event_id,updated_at=clock_timestamp()",
        f"intent-{target}",
        target,
        event_id,
    )


async def seed(repository, *, kind="DATA_ALERT", deliver_source=True):
    repo, pool, schema = repository
    state = genesis_state()
    await pool.execute(
        f"INSERT INTO {schema}.official_state(lineage_id,revision,state_hash,state_json) "
        "VALUES ('existing',0,$1,$2::jsonb)",
        sha256_json(state),
        json.dumps(state),
    )
    entry = {
        "strategy_version": "V24.5",
        "action": "BLOCK",
        "final_multiplier": 0.0,
        "v245_switch_plan": {"target": "BANK"},
    }
    await insert_event(pool, schema, "old-entry", "ENTRY_DECISION", entry, marker=1)
    await pool.execute(
        f"INSERT INTO {schema}.v245_switch_intents "
        "(intent_id,official_stream_id,lineage_id,trade_date,target,source_event_id,last_event_id) "
        "VALUES ('intent-BANK','switch-test','existing','2026-11-16',"
        "'BANK','old-entry','old-entry')"
    )
    await pool.execute(
        f"INSERT INTO {schema}.v22_alert_batches "
        "(batch_id,official_stream_id,lineage_id,trade_date,source_event_id) "
        "VALUES ('batch','switch-test','existing','2026-11-13','old-entry')"
    )
    await pool.execute(
        f"INSERT INTO {schema}.v22_alert_positions "
        "(position_id,batch_id,code,stock_name,entry_date,quantity,asset_kind) "
        "VALUES ('position','batch','000001','Stock','2026-11-13',100,'STOCK')"
    )
    relay = _CapturingRelay()
    event_id = "old-entry"
    if kind == "DATA_ALERT":
        if deliver_source:
            assert await publisher(repo, relay).publish_once() == 1
        semantic = {
            "strategy_version": "V24.5",
            "alert_code": "V245_SWITCH_EXIT_ALERT",
            "switch_target": "BANK",
            "target_event_id": "old-entry",
            "position_id": "position",
            "asset_kind": "STOCK",
            "quantity": 100,
            "entry_date": "2026-11-13",
            "event_trade_date": "2026-11-16",
        }
        event_id = "old-switch"
        await insert_event(pool, schema, event_id, kind, semantic, marker=2)
    relay.envelopes.clear()
    return event_id, relay


async def frozen_row(pool, schema, event_id):
    return await pool.fetchrow(
        f"SELECT payload_json,payload_hash,semantic_json,semantic_content_hash "
        f"FROM {schema}.outbox_events WHERE event_id=$1",
        event_id,
    )


@pytest.mark.parametrize("kind", ["ENTRY_DECISION", "DATA_ALERT"])
async def test_change_after_lease_suppresses_without_attempt_or_message_rewrite(repository, kind):
    repo, pool, schema = repository
    event_id, relay = await seed(repository, kind=kind)
    before = await frozen_row(pool, schema, event_id)
    leased = await repo.lease_outbox(worker_id="worker", limit=1, **SCOPE)
    assert [row.event_id for row in leased] == [event_id]
    await change_target(pool, schema, "STOCKS")
    real_lease = repo.lease_outbox
    repo.lease_outbox = AsyncMock(return_value=leased)
    assert await publisher(repo, relay).publish_once() == 0
    repo.lease_outbox = real_lease
    assert relay.envelopes == []
    assert await frozen_row(pool, schema, event_id) == before
    assert (
        await pool.fetchval(
            f"SELECT count(*) FROM {schema}.delivery_attempts WHERE event_id=$1", event_id
        )
        == 0
    )
    row = await repo.get_outbox_event(event_id, **SCOPE)
    assert row.delivery_status == "PENDING" and row.last_error == "V245_TARGET_SUPERSEDED"
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v245_delivery_suppressions") == 1
    assert await repo.lease_outbox(worker_id="worker", limit=10, **SCOPE) == []
    health = await repo.get_outbox_health(**SCOPE)
    assert health["suppressed_count"] == 1
    assert health["delivery_error_n"] == 0


@pytest.mark.parametrize("kind", ["ENTRY_DECISION", "DATA_ALERT"])
async def test_same_target_new_run_keeps_valid_notification(repository, kind):
    repo, pool, schema = repository
    event_id, relay = await seed(repository, kind=kind)
    await change_target(pool, schema, "BANK")
    assert await publisher(repo, relay).publish_once() == 1
    assert [e["event_id"] for e in relay.envelopes] == [event_id]
    assert (await repo.get_outbox_event(event_id, **SCOPE)).delivery_status == "SENT"
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v245_delivery_suppressions") == 0


@pytest.mark.parametrize("status", ["CLOSED", "NOT_BOUGHT"])
async def test_real_calibration_after_lease_prevents_sale_notice(repository, status):
    repo, pool, schema = repository
    event_id, relay = await seed(repository)
    leased = await repo.lease_outbox(worker_id="worker", limit=1, **SCOPE)
    store = V22PositionStore(
        repo, SimpleNamespace(official_stream_id="switch-test", state_lineage_id="existing")
    )
    await store.calibrate(
        "position", "actual-correction", {"expected_revision": 0, "status": status, "quantity": 0}
    )
    repo.lease_outbox = AsyncMock(return_value=leased)
    assert await publisher(repo, relay).publish_once() == 0
    assert relay.envelopes == []
    assert (
        await pool.fetchval(
            f"SELECT count(*) FROM {schema}.delivery_attempts WHERE event_id=$1", event_id
        )
        == 0
    )


async def test_dispatch_waits_for_committing_target_change_on_official_state_lock(repository):
    repo, pool, schema = repository
    event_id, relay = await seed(repository)
    leased = await repo.lease_outbox(worker_id="worker", limit=1, **SCOPE)
    repo.lease_outbox = AsyncMock(return_value=leased)
    async with pool.acquire() as con, con.transaction():
        await con.fetchrow(
            f"SELECT * FROM {schema}.official_state WHERE lineage_id='existing' FOR UPDATE"
        )
        task = asyncio.create_task(publisher(repo, relay).publish_once())
        try:
            await asyncio.sleep(0.05)
            assert not task.done() and relay.envelopes == []
            # The same connection holds the target's commit serialization lock.
            await change_target(con, schema, "STOCKS")
        except BaseException:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise
    assert await asyncio.wait_for(task, timeout=5) == 0
    assert relay.envelopes == []
    assert (await repo.get_outbox_event(event_id, **SCOPE)).last_error == "V245_TARGET_SUPERSEDED"


async def test_switch_waits_for_visible_direction_but_obsolete_unknown_source_can_be_suppressed(
    repository,
):
    repo, pool, schema = repository
    event_id, relay = await seed(repository, deliver_source=False)
    leased = await repo.lease_outbox(worker_id="worker", limit=10, **SCOPE)
    assert [r.event_id for r in leased] == ["old-entry"]
    attempt = await repo.begin_delivery_attempt("old-entry", worker_id="worker", **SCOPE)
    await repo.complete_delivery(
        "old-entry",
        attempt_number=attempt.attempt_number,
        worker_id="worker",
        outcome="UNKNOWN",
        error="fixture: uncertain external receipt",
        **SCOPE,
    )
    assert await repo.lease_outbox(worker_id="worker", limit=10, **SCOPE) == []
    await change_target(pool, schema, "STOCKS")
    # The old switch may now be leased solely for the atomic suppression path.
    assert await publisher(repo, relay).publish_once() == 0
    assert relay.envelopes == []
    old_switch = await repo.get_outbox_event(event_id, **SCOPE)
    assert old_switch.last_error == "V245_TARGET_SUPERSEDED"
    assert (await repo.get_outbox_event("old-entry", **SCOPE)).delivery_status == "DELIVERY_UNKNOWN"
