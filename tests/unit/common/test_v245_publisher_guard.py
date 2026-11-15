"""Exercise the actual publisher and dispatch transaction with isolated SQL I/O.

The corresponding PostgreSQL tests execute the same boundary against real rows.
"""

from datetime import date, datetime, timedelta

import pytest

from src.common.v20_feishu import V20OutboxPublisher
from src.data.database.v20_repository import OutboxRecord, sha256_json
from tests.unit.common.test_v20_feishu import TZ, _CapturingRelay, _route
from tests.unit.data.database.test_v20_repository_contract import _repository


class GuardConnection:
    def __init__(self, semantic, event_type, *, target="BANK", status="MONITORING", quantity=100):
        self.semantic = semantic
        self.event_type = event_type
        self.target = target
        self.status = status
        self.quantity = quantity
        self.calls = []

    def transaction(self, **kwargs):
        from tests.unit.data.database.test_v20_repository_contract import _AsyncContext

        return _AsyncContext(self)

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        if "v245_switch_intents" in sql:
            return {
                "intent_id": "latest-intent",
                "target": self.target,
                "last_event_id": "new-same-direction-run",
                "trade_date": date(2026, 11, 16),
            }
        if "official_state" in sql:
            return {"revision": 3}
        if "v22_alert_positions" in sql:
            return {
                "position_id": "position",
                "status": self.status,
                "quantity": self.quantity,
                "asset_kind": "STOCK",
                "entry_date": date(2026, 11, 13),
                "revision": 0,
            }
        if "SELECT attempt_count" in sql:
            return {
                "attempt_count": 0,
                "action_expiry_ts": None,
                "db_now": datetime.now(TZ),
                "event_type": self.event_type,
                "semantic_json": self.semantic,
            }
        if "INSERT INTO" in sql and "delivery_attempts" in sql:
            return {"attempt_number": 1, "delivery_variant": "PRIMARY"}
        if "UPDATE" in sql and "outbox_events" in sql:
            return {"event_id": "old-event"}
        raise AssertionError(f"unmodeled SQL boundary: {sql}")

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        if "SELECT EXISTS" in sql:
            return True
        return "SENT" if "outbox_events" in sql else self.status

    async def execute(self, sql, *args):
        self.calls.append((sql, args))
        if sql.lstrip().startswith("SELECT"):
            return "SELECT 1"
        return "UPDATE 1" if sql.lstrip().startswith("UPDATE") else "INSERT 0 1"


async def _run(*, event_type="DATA_ALERT", target="BANK", status="MONITORING", quantity=100):
    semantic = {
        "strategy_version": "V24.5",
        "action": "BLOCK",
        "final_multiplier": 0.0,
        "alert_code": "V245_SWITCH_EXIT_ALERT",
        "position_id": "position",
        "asset_kind": "STOCK",
        "quantity": 100,
        "entry_date": "2026-11-13",
        "event_trade_date": "2026-11-16",
        "switch_target": "BANK",
        "target_event_id": "original-target-event",
        "v245_switch_plan": {"target": "BANK"},
    }
    if event_type == "ENTRY_DECISION":
        semantic.pop("alert_code")
    con = GuardConnection(semantic, event_type, target=target, status=status, quantity=quantity)
    repo = _repository(con)
    payload = {"message": "sealed original direction", "expired_delivery_message": "expired"}
    record = OutboxRecord(
        event_id="old-event",
        event_type=event_type,
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
        payload=payload,
        payload_hash=sha256_json(payload),
        generated_at=datetime.now(TZ),
        commit_marker=1,
        action_expiry_ts=(datetime.now(TZ) + timedelta(days=1))
        if event_type == "ENTRY_DECISION"
        else None,
        delivery_status="LEASED",
        attempt_count=0,
    )

    async def lease(**kwargs):
        return [record]

    completed = []

    async def complete(*args, **kwargs):
        completed.append((args, kwargs))

    repo.lease_outbox = lease
    repo.complete_delivery = complete
    relay = _CapturingRelay()
    publisher = V20OutboxPublisher(
        repo,
        {"route": _route(relay)},
        worker_id="worker",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
    )
    before = dict(payload)
    count = await publisher.publish_once()
    assert payload == before
    return count, relay, con, completed


@pytest.mark.parametrize("event_type", ["DATA_ALERT", "ENTRY_DECISION"])
async def test_old_direction_is_not_dispatched_or_falsely_marked_delivered(event_type):
    count, relay, con, completed = await _run(event_type=event_type, target="STOCKS")
    assert count == 0
    assert relay.envelopes == [] and completed == []
    assert not any("INSERT INTO" in sql and "delivery_attempts" in sql for sql, _ in con.calls)
    assert any("v245_delivery_suppressions" in sql for sql, _ in con.calls)


@pytest.mark.parametrize(
    "status,quantity", [("CLOSED", 100), ("NOT_BOUGHT", 100), ("MONITORING", 0)]
)
async def test_calibrated_holding_is_checked_again_at_dispatch(status, quantity):
    count, relay, _, completed = await _run(status=status, quantity=quantity)
    assert count == 0 and relay.envelopes == [] and completed == []


@pytest.mark.parametrize("event_type", ["DATA_ALERT", "ENTRY_DECISION"])
async def test_new_same_direction_run_does_not_cancel_valid_old_notification(event_type):
    count, relay, _, completed = await _run(event_type=event_type)
    assert count == 1 and len(relay.envelopes) == 1
    assert completed[0][1]["outcome"] == "DELIVERED"
    assert relay.envelopes[0]["message"] == "sealed original direction"
