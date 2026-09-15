from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import datetime, time, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import httpx
import pytest
from fastapi import FastAPI

from src.common.v20_feishu import _render_entry_strategy_body, seal_v20_payload
from src.data.database.v20_positions import LegacyPositionStore
from src.data.database.v20_repository import ExitCommit, V20StateConflict, sha256_json
from src.strategy.v20.models import V20_EXIT_SEMANTIC_SCHEMA, V20_FEISHU_FORMATTER_PROFILE
from src.web.v20_routes import create_v20_router
from src.web.v20_service import V20Service
from tests.integration.data.database.test_v20_outbox_postgres import (
    repository as _repository_fixture,
)

pytestmark = pytest.mark.postgres
repository = _repository_fixture


async def _seed_legacy(repository):
    """Create real sealed recommendations through the production repository."""
    from src.common.v20_feishu import seal_v20_payload
    from src.strategy.v20.models import V20_ENTRY_SEMANTIC_SCHEMA, V20_FEISHU_FORMATTER_PROFILE
    from tests.unit.data.database.test_v20_repository_contract import _enter, _official_state

    instance, pool, schema = repository
    tz = ZoneInfo("Asia/Shanghai")
    now = (await pool.fetchval("SELECT clock_timestamp()")).astimezone(tz)
    today = now.date()
    base = _enter(1.0, 2)
    before = _official_state(0)
    after = dict(base.next_state)
    after["last_terminal_trade_date"] = today.isoformat()
    snapshot = {**base.snapshot, "trade_date": today.isoformat()}
    semantic = {
        **base.semantic,
        "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "deployment_mode": "forward_shadow",
        "base_multiplier": 1.0,
        "defense_multiplier": 1.0,
        "health_state": "WARMUP",
        "rolling7_state": "WARMUP",
        "rolling7_r7": None,
        "rolling7_l7": None,
        "g_state": "NOT_EVALUATED",
        "reason_codes": [],
        "scheduled_exits_today": [],
        "v16_funnel": {
            "step0_universe_count": 2,
            "step2_hot_board_count": 1,
            "final_candidates": 2,
        },
        "v16_board_avg_gains": {"test board": 1.0},
        "event_id": base.event_id,
        "trade_date": today.isoformat(),
        "symbols": [
            {
                "rank": rank,
                "code": f"{rank:06d}",
                "name": f"test {rank}",
                "score": 1.0,
                "snapshot_price": 10.0,
                "boards": ["test board"],
                "best_board": "test board",
                "is_driver": False,
                "cci": 0.0,
                "volume_937": 1000.0,
                "history_hash": "a" * 64,
                "early_source_hash": "b" * 64,
            }
            for rank in (1, 2)
        ],
        "last_complete_bar": "09:39",
    }
    commit = replace(
        base,
        trade_date=today,
        next_state=after,
        next_state_hash=sha256_json(after),
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
        action_expiry_ts=datetime.combine(today + timedelta(days=1), time.min, tz),
        model_batch=replace(
            base.model_batch,
            legs=tuple(
                replace(leg, d1=today + timedelta(days=1), d2=today + timedelta(days=2))
                for leg in base.model_batch.legs
            ),
        ),
    )
    await pool.execute(
        f"INSERT INTO {schema}.runtime_configs "
        "(config_id,config_hash,strategy_version,deployment_mode,effective_trade_date,config_json) "
        "VALUES ($1,$2,$3,'forward_shadow',$4,'{}')",
        commit.config_id,
        commit.config_hash,
        commit.strategy_version,
        today,
    )
    await pool.execute(
        f"INSERT INTO {schema}.official_state (lineage_id,revision,state_hash,state_json) "
        "VALUES ($1,0,$2,$3::jsonb)",
        commit.lineage_id,
        sha256_json(before),
        json.dumps(before),
    )
    await instance.commit_entry(commit)
    await instance.commit_entry(commit)  # identical retry cannot duplicate the batch
    status = await instance.get_entry_status(commit.official_stream_id, today)
    assert status.action == "ENTER"
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.model_legs") == 2
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.outbox_events") == 1
    sealed = await instance.seal_event(commit.event_id, seal_v20_payload)
    assert sealed.action_expiry_ts == commit.action_expiry_ts
    assert sealed.payload["timeliness_status"] == "ON_TIME"
    assert "000001" in sealed.payload["message"]
    assert "000002" in sealed.payload["message"]
    assert "已过09:40" not in sealed.payload["message"]

    return commit


def _service(instance, entry):
    service = V20Service.__new__(V20Service)
    service.config = SimpleNamespace(
        official_stream_id=entry.official_stream_id,
        state_lineage_id=entry.lineage_id,
        clock=SimpleNamespace(plan_exit=time(14, 57)),
    )
    service._repository = instance
    service._require_running = lambda: None
    return service


async def test_sold_sync_filters_daily_list_survives_restart_and_is_retry_safe(
    repository, monkeypatch
):
    instance, pool, schema = repository
    entry = await _seed_legacy(repository)
    service = _service(instance, entry)
    monkeypatch.setattr(instance, "assert_runtime_leader", AsyncMock())
    monkeypatch.setenv("V20_INGEST_API_KEY", "test-key")
    app = FastAPI()
    app.state.v20_service = service
    app.include_router(create_v20_router())
    path = "/api/v20/legacy-positions/leg-1/calibrate"
    headers = {"X-V20-API-Key": "test-key", "Idempotency-Key": "sold-integration-001"}
    check_day = entry.model_batch.legs[0].d2
    assert len(await service._scheduled_exits_today(check_day)) == 2
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        before = (await client.get("/api/v20/legacy-positions", headers=headers)).json()
        assert all(p["status"] == "UNCONFIRMED" and p["quantity"] is None for p in before)
        responses = await asyncio.gather(
            *[
                client.post(path, headers=headers, json={"expected_revision": 0, "quantity": 0})
                for _ in range(2)
            ]
        )
        assert [r.status_code for r in responses] == [200, 200]
        assert responses[0].json() == responses[1].json()
        assert responses[0].json()["status"] == "CLOSED"
        assert responses[0].json()["revision"] == 1
        assert (
            await client.post(
                path,
                headers={**headers, "Idempotency-Key": "stale-002"},
                json={"expected_revision": 0, "quantity": 200},
            )
        ).status_code == 409
        assert (
            await client.post(path, headers=headers, json={"expected_revision": 0, "quantity": 200})
        ).status_code == 409
    restarted = _service(instance, entry)
    scheduled = await restarted._scheduled_exits_today(check_day)
    assert [p["code"] for p in scheduled] == ["000002"]
    message = _render_entry_strategy_body({"action": "BLOCK", "scheduled_exits_today": scheduled})
    assert "000001" not in message and "000002" in message
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.legacy_position_calibrations") == 1
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.model_legs") == 2
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.exit_intents") == 0
    assert (
        await instance.get_entry_status(entry.official_stream_id, entry.trade_date)
    ).semantic == entry.semantic
    other = LegacyPositionStore(
        instance, SimpleNamespace(official_stream_id="other", state_lineage_id=entry.lineage_id)
    )
    assert await other.list() == []
    with pytest.raises(V20StateConflict):
        await other.calibrate("leg-2", "cross-scope-001", {"expected_revision": 0, "quantity": 0})


async def _exit(instance, entry, leg, now):
    semantic = {
        "schema_version": V20_EXIT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": "exit-" + leg.model_leg_id,
        "event_type": "EXIT_SIGNAL",
        "deployment_mode": "forward_shadow",
        "exit_signal_type": "D2_PLAN",
        "code": leg.code,
        "stock_name": leg.stock_name,
        "signal_date": entry.trade_date.isoformat(),
        "rank": leg.rank,
        "model_leg_id": leg.model_leg_id,
        "reference_entry_price": 10.0,
        "origin_final_relative_weight": leg.relative_weight,
        "rule_actionable_from": now.isoformat(),
        "reason_codes": [],
        "detection_trade_date": now.date().isoformat(),
        "detection_is_trading_day": True,
    }
    commit = ExitCommit(
        exit_intent_id="intent-" + leg.model_leg_id,
        event_id=semantic["event_id"],
        model_leg_id=leg.model_leg_id,
        signal_type="D2_PLAN",
        trigger_ts=now,
        rule_actionable_from=now,
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
        route_id=entry.route_id,
        official_stream_id=entry.official_stream_id,
        lineage_id=entry.lineage_id,
    )
    await instance.commit_exit(commit)
    await instance.seal_event(commit.event_id, seal_v20_payload)
    return commit.event_id


@pytest.mark.parametrize("leased_before_close", [False, True])
async def test_sold_sync_blocks_queued_exit_and_reminder_at_dispatch(
    repository, leased_before_close
):
    instance, pool, schema = repository
    entry = await _seed_legacy(repository)
    now = (await pool.fetchval("SELECT clock_timestamp()")).astimezone(ZoneInfo("Asia/Shanghai"))
    scope = dict(official_stream_id=entry.official_stream_id, lineage_id=entry.lineage_id)
    store = LegacyPositionStore(instance, _service(instance, entry).config)
    event = await _exit(instance, entry, entry.model_batch.legs[0], now)
    tomorrow = now.date() + timedelta(days=1)
    reminders = await instance.enqueue_due_exit_reminders(
        tomorrow,
        cutoff=datetime.combine(tomorrow, time(9, 40), now.tzinfo),
        route_id=entry.route_id,
        **scope,
    )
    assert len(reminders) == 1
    await instance.seal_event(reminders[0], seal_v20_payload)
    if leased_before_close:
        leased = await instance.lease_outbox(
            worker_id="test-worker", route_id=entry.route_id, **scope
        )
        assert {event, reminders[0]} <= {record.event_id for record in leased}
    await store.calibrate("leg-1", "sold-before-send-001", {"expected_revision": 0, "quantity": 0})
    if leased_before_close:
        for event_id in (event, reminders[0]):
            assert (
                await instance.begin_delivery_attempt(
                    event_id, worker_id="test-worker", route_id=entry.route_id, **scope
                )
                is None
            )
    new_leases = await instance.lease_outbox(
        worker_id="next-worker", route_id=entry.route_id, **scope
    )
    assert not ({event, reminders[0]} & {record.event_id for record in new_leases})
    assert (
        await instance.enqueue_due_exit_reminders(
            tomorrow + timedelta(days=1),
            cutoff=datetime.combine(tomorrow + timedelta(days=1), time(9, 40), now.tzinfo),
            route_id=entry.route_id,
            **scope,
        )
        == ()
    )
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.delivery_attempts") == 0
    assert (
        await pool.fetchval(
            f"SELECT delivery_status FROM {schema}.outbox_events WHERE event_id=$1", event
        )
        == "PENDING"
    )


async def test_partial_sale_and_not_bought_are_scoped_and_audited(repository):
    instance, pool, schema = repository
    entry = await _seed_legacy(repository)
    service = _service(instance, entry)
    store = LegacyPositionStore(instance, service.config)
    updated = await store.calibrate(
        "leg-1", "partial-001", {"expected_revision": 0, "quantity": 100}
    )
    assert updated["quantity"] == 100 and updated["status"] == "MONITORING"
    assert len(await service._scheduled_exits_today(entry.model_batch.legs[0].d2)) == 2
    await store.calibrate(
        "leg-1", "not-bought-001", {"expected_revision": 1, "status": "NOT_BOUGHT"}
    )
    assert [
        p["code"] for p in await service._scheduled_exits_today(entry.model_batch.legs[0].d2)
    ] == ["000002"]
    audit = await pool.fetchrow(
        f"SELECT before_json,after_json FROM {schema}.legacy_position_calibrations "
        "WHERE request_id='not-bought-001'"
    )
    assert json.loads(audit["before_json"])["quantity"] == 100
    assert json.loads(audit["after_json"])["quantity"] == 0
