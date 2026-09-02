from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, time
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import asyncpg
import httpx
import pytest
from fastapi import FastAPI, Request

from src.common import feishu_bot
from src.common.feishu_bot import FeishuBot
from src.common.v20_feishu import (
    V20FeishuRoute,
    V20OutboxPublisher,
    seal_v20_payload,
)
from src.data.database.v20_repository import V20DatabaseConfig, V20Repository, sha256_json
from src.strategy.v20.models import (
    V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
    V20_ENTRY_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
    V20_V16_SNAPSHOT_SCHEMA,
)
from src.web.v20_routes import create_v20_router
from src.web.v20_service import V20Service

DSN = os.environ.get("V20_TEST_POSTGRES_DSN", "")
SAFE_PREFIX = "v20_feishu_test_"
if DSN:
    pytestmark = pytest.mark.postgres
else:
    pytestmark = [
        pytest.mark.postgres,
        pytest.mark.skip(
            reason=(
                "V20_TEST_POSTGRES_DSN is not set; real PostgreSQL V20 Feishu "
                "tests were skipped rather than faked"
            )
        ),
    ]


def _schema() -> str:
    value = SAFE_PREFIX + uuid.uuid4().hex
    assert value.startswith(SAFE_PREFIX)
    return value


def _config(schema: str) -> V20DatabaseConfig:
    return V20DatabaseConfig(
        schema=schema,
        pool_min_size=1,
        pool_max_size=12,
        ssl_mode="disable",
        connection_profile="legacy_embedded",
    )


@pytest.fixture
async def repository():
    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=1, max_size=12)
    instance = V20Repository(_config(schema), shared_pool=pool)
    try:
        await instance.connect(migrate=True)
        yield instance, pool, schema
    finally:
        await instance.close()
        assert schema.startswith(SAFE_PREFIX)
        async with pool.acquire() as connection:
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await pool.close()


def _publisher(repository: V20Repository, route: V20FeishuRoute, worker_id: str = "owner"):
    return V20OutboxPublisher(
        repository,
        {"formal-route": route},
        worker_id=worker_id,
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )


def _route(bot_url: str = "https://relay.invalid") -> V20FeishuRoute:
    return V20FeishuRoute(
        route_id="formal-route",
        bot_url=bot_url,
        app_id="app",
        app_secret="secret",
        chat_id="chat",
        transport="legacy_send",
    )


async def _seed_read_timeout_event(pool: asyncpg.Pool, schema: str) -> None:
    semantic = {"event_id": "read-timeout-event", "kind": "DATA_ALERT"}
    payload = {"event_id": "read-timeout-event", "message": "one"}
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {schema}.outbox_events
                (event_id,event_type,route_id,official_stream_id,lineage_id,
                 semantic_content_hash,semantic_json,payload_json,payload_hash,
                 seal_status,generated_at,commit_marker,delivery_status)
            VALUES
                ('read-timeout-event','DATA_ALERT','formal-route','official','lineage-1',
                 $1,$2::jsonb,$3::jsonb,$4,'SEALED',clock_timestamp(),
                 nextval('{schema}.commit_marker_seq'),'PENDING')
            """,
            sha256_json(semantic),
            json.dumps(semantic, separators=(",", ":"), ensure_ascii=False),
            json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
            sha256_json(payload),
        )


async def _state(pool: asyncpg.Pool, schema: str):
    async with pool.acquire() as connection:
        outbox = await connection.fetchrow(
            f"SELECT delivery_status,attempt_count,last_error,lease_owner,lease_until "
            f"FROM {schema}.outbox_events WHERE event_id='read-timeout-event'"
        )
        attempt = await connection.fetchrow(
            f"SELECT phase,succeeded,completed_at,error_text "
            f"FROM {schema}.delivery_attempts WHERE event_id='read-timeout-event' "
            "ORDER BY attempt_number DESC LIMIT 1"
        )
    return outbox, attempt


async def _attempts(pool: asyncpg.Pool, schema: str) -> list[asyncpg.Record]:
    async with pool.acquire() as connection:
        return await connection.fetch(
            f"SELECT * FROM {schema}.delivery_attempts "
            "WHERE event_id='read-timeout-event' ORDER BY attempt_number"
        )


class _PostCutoffRouteService:
    def __init__(self, config, repository):
        self.config = config
        self._repository = repository
        self._verify_entry_binding = V20Service._verify_entry_binding.__get__(self)
        self.canonical_check_only_calls: list[str] = []
        self._manual_trigger_lock = asyncio.Lock()
        self._decision_cycle_lock = asyncio.Lock()
        self._late_0939_replay_lock = asyncio.Lock()

    def _aware_now(self):
        return datetime(2026, 9, 1, 14, 4, tzinfo=ZoneInfo("Asia/Shanghai"))

    async def _require_manual_trigger_ready(self):
        return None

    async def ensure_mews_for_selection_trigger(self, now):
        return False

    def kick_mews_for_selection_trigger(self, now):
        return None

    async def trigger_canonical_selection_check_only(self, request_id, now):
        self.canonical_check_only_calls.append(request_id)
        event_id = sha256_json(["V20_PG_TEST_CANONICAL_CHECK_ONLY_EVENT_ID_V1", request_id])
        semantic = {
            "schema_version": "v20-data-alert-semantic/v2",
            "feishu_formatter_profile": "V20_FULL_V16_FEISHU_V1",
            "event_id": event_id,
            "strategy_version": self.config.strategy_version,
            "config_hash": self.config.config_hash,
            "state_semantics_hash": self.config.state_semantics_hash,
            "deployment_mode": self.config.deployment_mode,
            "official_stream_id": self.config.official_stream_id,
            "state_lineage_id": self.config.state_lineage_id,
            "alert_code": "V20_PG_TEST_CANONICAL_CHECK_ONLY",
            "delivery_priority_class": "OPERATOR_NOTIFICATION",
            "entity_id": request_id,
            "event_trade_date": now.date().isoformat(),
            "reason": "canonical check-only fallback",
            "message": "canonical check-only fallback",
        }
        created = await self._repository.enqueue_alert(
            event_id,
            self.config.route_id,
            semantic,
            sha256_json(semantic),
            official_stream_id=self.config.official_stream_id,
            lineage_id=self.config.state_lineage_id,
        )
        sealed = await self._repository.seal_event(event_id, seal_v20_payload)
        return {
            "accepted": True,
            "created": created,
            "event_id": sealed.event_id,
            "fallback": True,
        }

    async def _load_trade_calendar(self, current_date):
        raise RuntimeError("integration-test fixed calendar failure")


async def _seed_production_terminal_official_entry(
    pool: asyncpg.Pool,
    schema: str,
    config,
) -> None:
    source_event_id = "official-entry-event"
    slot_id = "integration-official-slot"
    decision_id = "integration-official-decision"
    state_before = {
        "state_revision": 0,
        "last_terminal_slot_id": None,
        "last_terminal_trade_date": None,
    }
    state_after = {
        "state_revision": 1,
        "last_terminal_slot_id": slot_id,
        "last_terminal_trade_date": "2026-09-01",
    }
    state_before_hash = sha256_json(state_before)
    state_after_hash = sha256_json(state_after)
    policy_input_hash = sha256_json(
        {"completed_health": [], "completed_rolling": [], "maturity_gaps": []}
    )
    source_semantic = {
        "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": source_event_id,
        "strategy_version": config.strategy_version,
        "config_hash": config.config_hash,
        "config_id": config.config_hash[:24],
        "state_semantics_hash": config.state_semantics_hash,
        "deployment_mode": config.deployment_mode,
        "official_stream_id": config.official_stream_id,
        "state_lineage_id": config.state_lineage_id,
        "state_before_hash": state_before_hash,
        "state_after_hash": state_after_hash,
        "policy_input_hash": policy_input_hash,
        "trade_date": "2026-09-01",
        "action": "NO_SIGNAL",
        "final_multiplier": 0.0,
        "base_multiplier": 0.0,
        "defense_multiplier": 0.0,
        "health_state": "WARMUP",
        "rolling7_state": "UNKNOWN",
        "rolling7_r7": None,
        "rolling7_l7": None,
        "g_state": "UNKNOWN",
        "reason_codes": ["NO_CANDIDATES"],
        "last_complete_bar": "09:39",
        "symbols": [],
        "scheduled_exits_today": [],
        "v16_funnel": {
            "step0_universe_count": 1,
            "step2_hot_board_count": 0,
            "final_candidates": 0,
        },
        "v16_board_avg_gains": {},
    }
    snapshot = {
        "schema_version": V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
        "v16_snapshot_schema_version": V20_V16_SNAPSHOT_SCHEMA,
        "state_before_hash": state_before_hash,
        "state_semantics_hash": config.state_semantics_hash,
        "policy_input_hash": policy_input_hash,
        "config_hash": config.config_hash,
        "config_id": config.config_hash[:24],
        "trade_date": "2026-09-01",
        "early_market_source_hash": "1" * 64,
        "scorer_model_sha256": "3" * 64,
        "scorer_feature_sha256": "4" * 64,
        "v16_snapshot_hash": "5" * 64,
        "comparison_pool_codes": [],
        "symbols": [],
    }
    config_payload = {
        "schema_version": "v20-runtime-config/v1",
        "strategy_version": config.strategy_version,
        "config_hash": config.config_hash,
        "state_lineage_id": config.state_lineage_id,
        "official_stream_id": config.official_stream_id,
        "route_id": config.route_id,
        "state_semantics_hash": config.state_semantics_hash,
    }
    encoded_semantic = json.dumps(source_semantic, separators=(",", ":"), ensure_ascii=False)
    encoded_snapshot = json.dumps(snapshot, separators=(",", ":"), ensure_ascii=False)
    encoded_config = json.dumps(config_payload, separators=(",", ":"), ensure_ascii=False)
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {schema}.runtime_configs
                (config_id,config_hash,strategy_version,deployment_mode,
                 effective_trade_date,config_json)
            VALUES ($1,$2,$3,$4,'2026-08-31',$5::jsonb)
            ON CONFLICT (config_id) DO NOTHING
            """,
            config.config_hash[:24],
            config.config_hash,
            config.strategy_version,
            config.deployment_mode,
            encoded_config,
        )
        await connection.execute(
            f"""
            INSERT INTO {schema}.official_state
                (lineage_id,revision,state_hash,state_json)
            VALUES ($1,1,$2,$3::jsonb)
            """,
            config.state_lineage_id,
            state_after_hash,
            json.dumps(state_after, separators=(",", ":"), ensure_ascii=False),
        )
        await connection.execute(
            f"""
            INSERT INTO {schema}.input_snapshots
                (snapshot_id,snapshot_type,trade_date,snapshot_hash,snapshot_json)
            VALUES ($4,$1,'2026-09-01',$2,$3::jsonb)
            """,
            "V16",
            sha256_json(snapshot),
            encoded_snapshot,
            "integration-official-snapshot",
        )
        await connection.execute(
            f"""
            INSERT INTO {schema}.decision_slots
                (official_stream_id,trade_date,slot_id,strategy_version,config_id,
                 config_hash,lineage_id,slot_status,slot_revision,
                 terminal_event_id,terminal_decision_id,completed_at)
            VALUES ($1,'2026-09-01',$2,$3,$4,$5,$6,'COMPLETED',1,$7,$8,
                    '2026-09-01 09:40+08')
            """,
            config.official_stream_id,
            slot_id,
            config.strategy_version,
            config.config_hash[:24],
            config.config_hash,
            config.state_lineage_id,
            source_event_id,
            decision_id,
        )
        await connection.execute(
            f"""
            INSERT INTO {schema}.outbox_events
                (event_id,event_type,route_id,official_stream_id,lineage_id,
                 semantic_content_hash,semantic_json,delivery_status,action_expiry_ts)
            VALUES ($1,'ENTRY_DECISION',$2,$3,$4,$5,$6::jsonb,'PENDING',
                    '2026-09-01 09:40+08')
            """,
            source_event_id,
            config.route_id,
            config.official_stream_id,
            config.state_lineage_id,
            sha256_json(source_semantic),
            encoded_semantic,
        )
        await connection.execute(
            f"""
            INSERT INTO {schema}.entry_decisions
                (decision_id,slot_id,event_id,snapshot_id,action,final_multiplier,
                 semantic_content_hash,commit_fingerprint,semantic_json)
            VALUES ($1,$2,$3,$4,'NO_SIGNAL',0.0,$5,$6,$7::jsonb)
            """,
            decision_id,
            slot_id,
            source_event_id,
            "integration-official-snapshot",
            sha256_json(source_semantic),
            "d" * 64,
            encoded_semantic,
        )


async def _official_runtime_snapshot(pool: asyncpg.Pool, schema: str) -> dict[str, Any]:
    async with pool.acquire() as connection:
        state = await connection.fetchrow(
            f"SELECT revision,state_hash,state_json::text AS state_json "
            f"FROM {schema}.official_state WHERE lineage_id='lineage-1'"
        )
        slot = await connection.fetchrow(f"SELECT * FROM {schema}.decision_slots")
        decision = await connection.fetchrow(f"SELECT * FROM {schema}.entry_decisions")
        counts = await connection.fetchrow(
            f"SELECT"
            f" (SELECT count(*) FROM {schema}.shadow_batches) AS shadow_batches,"
            f" (SELECT count(*) FROM {schema}.model_batches) AS model_batches,"
            f" (SELECT count(*) FROM {schema}.model_legs) AS model_legs,"
            f" (SELECT count(*) FROM {schema}.exit_intents) AS exit_intents,"
            f" (SELECT count(*) FROM {schema}.manual_monitor_enrollments) "
            f"AS manual_monitor_enrollments"
        )
    return {
        "state": dict(state) if state is not None else None,
        "slot": dict(slot) if slot is not None else None,
        "decision": dict(decision) if decision is not None else None,
        "counts": dict(counts) if counts is not None else None,
    }


class _ResponseLostTransport(httpx.ASGITransport):
    def __init__(self, inner: httpx.ASGITransport):
        self.inner = inner
        self.posts = 0

    async def handle_async_request(self, request):
        if request.method == "POST" and self.posts == 0:
            self.posts += 1
            response = await self.inner.handle_async_request(request)
            await response.aread()
            await response.aclose()
            raise httpx.ReadTimeout(
                "relay accepted first post but response was lost", request=request
            )
        return await self.inner.handle_async_request(request)


async def test_duplicate_feishu_trigger_full_chain_posts_once_after_response_loss(
    repository, monkeypatch
) -> None:
    """Prove one dispatch attempt after response loss, not exact delivery."""
    instance, pool, schema = repository

    config = SimpleNamespace(
        route_id="formal-route",
        official_stream_id="official",
        state_lineage_id="lineage-1",
        strategy_version="V20",
        deployment_mode="production_push",
        config_hash="b" * 64,
        state_semantics_hash="c" * 64,
        clock=SimpleNamespace(prewarm=time(9, 15), publish_deadline=time(9, 40)),
    )
    await _seed_production_terminal_official_entry(pool, schema, config)
    sealed_source = await instance.seal_event("official-entry-event", seal_v20_payload)
    assert sealed_source.action_expiry_ts is not None
    terminal = await instance.get_entry_status(
        config.official_stream_id,
        sealed_source.action_expiry_ts.astimezone(ZoneInfo("Asia/Shanghai")).date(),
    )
    assert terminal is not None
    assert terminal.slot_status == "COMPLETED"
    assert terminal.action == "NO_SIGNAL"
    assert terminal.event_id == sealed_source.event_id
    assert terminal.official_stream_id == config.official_stream_id
    assert terminal.lineage_id == config.state_lineage_id
    assert terminal.strategy_version == config.strategy_version
    assert terminal.config_id == config.config_hash[:24]
    assert terminal.config_hash == config.config_hash
    assert terminal.semantic["state_after_hash"] == sha256_json(
        {
            "state_revision": 1,
            "last_terminal_slot_id": "integration-official-slot",
            "last_terminal_trade_date": "2026-09-01",
        }
    )
    assert sealed_source.event_type == "ENTRY_DECISION"
    assert sealed_source.payload is not None
    assert sealed_source.payload_hash is not None
    async with pool.acquire() as connection:
        source_row = await connection.fetchrow(
            f"SELECT seal_status,payload_json,payload_hash FROM {schema}.outbox_events "
            "WHERE event_id=$1",
            sealed_source.event_id,
        )
        await connection.execute(
            f"UPDATE {schema}.outbox_events "
            "SET delivery_status='SENT',delivered_at=clock_timestamp() WHERE event_id=$1",
            sealed_source.event_id,
        )
    assert source_row["seal_status"] == "SEALED"
    assert json.loads(source_row["payload_json"]) == sealed_source.payload
    assert source_row["payload_hash"] == sealed_source.payload_hash
    await instance.acquire_runtime_leader(
        route_id=config.route_id,
        official_stream_id=config.official_stream_id,
        lineage_id=config.state_lineage_id,
    )
    service = _PostCutoffRouteService(config, instance)
    service._verify_entry_binding(terminal)
    official_before = await _official_runtime_snapshot(pool, schema)
    api_app = FastAPI()
    api_app.include_router(create_v20_router())
    api_app.state.v20_service = service

    relay_posts: list[dict] = []
    relay_app = FastAPI()

    @relay_app.post("/api/send")
    async def relay_send(request: Request):
        relay_posts.append(await request.json())
        return {"code": 0, "msg": "success"}

    original_async_client = httpx.AsyncClient
    lossy_transport = _ResponseLostTransport(httpx.ASGITransport(app=relay_app))

    class LossyAsyncClient(original_async_client):
        def __init__(self, **kwargs):
            kwargs["transport"] = lossy_transport
            super().__init__(**kwargs)

    monkeypatch.setattr(feishu_bot, "httpx", SimpleNamespace(AsyncClient=LossyAsyncClient))
    request_headers = {"Idempotency-Key": "duplicate-chain-20260901"}
    async with original_async_client(
        transport=httpx.ASGITransport(app=api_app), base_url="http://testserver"
    ) as client:
        responses = await asyncio.gather(
            *(client.post("/api/v20/trigger-scan", headers=request_headers) for _ in range(3))
        )

    assert [response.status_code for response in responses] == [202, 202, 202]
    response_bodies = [response.json() for response in responses]
    assert service.canonical_check_only_calls == []
    expected_response_keys = {
        "accepted",
        "created",
        "manual_request_id",
        "event_trade_date",
        "entry_action",
        "final_multiplier",
        "symbols",
        "source_entry_event_id",
        "replay_event_id",
        "visible_message_mode",
        "official_state_changed",
        "orders_changed",
        "non_actionable",
        "retrospective_expired",
        "exact_automatic_message",
        "manual_notice_actionable",
        "feishu_delivery_confirmed",
    }
    assert all(set(body) == expected_response_keys for body in response_bodies)
    event_ids = {body["replay_event_id"] for body in response_bodies}
    assert len(event_ids) == 1
    event_id = next(iter(event_ids))
    assert [body["created"] for body in response_bodies].count(True) == 1
    assert [body["created"] for body in response_bodies].count(False) == 2
    assert all(body["entry_action"] == "NO_SIGNAL" for body in response_bodies)
    assert all(body["source_entry_event_id"] == "official-entry-event" for body in response_bodies)
    assert all(body["exact_automatic_message"] is True for body in response_bodies)
    assert all(body["official_state_changed"] is False for body in response_bodies)
    assert all(body["orders_changed"] is False for body in response_bodies)
    assert all(body["feishu_delivery_confirmed"] is False for body in response_bodies)

    async with pool.acquire() as connection:
        outbox_count = await connection.fetchval(
            f"SELECT count(*) FROM {schema}.outbox_events WHERE event_id=$1", event_id
        )
        outbox_rows = await connection.fetch(
            f"SELECT event_id,semantic_json,payload_json,payload_hash,seal_status "
            f"FROM {schema}.outbox_events "
            "WHERE event_id=$1",
            event_id,
        )
    assert outbox_count == 1
    outbox = outbox_rows[0]
    replay_semantic = json.loads(outbox["semantic_json"])
    sealed_payload = json.loads(outbox["payload_json"])
    assert outbox["event_id"] == event_id
    assert outbox["seal_status"] == "SEALED"
    assert outbox["payload_hash"] == sha256_json(sealed_payload)
    assert sealed_payload["event_id"] == event_id
    source_message = str(sealed_source.payload["message"])
    assert replay_semantic["message"] == source_message
    assert replay_semantic["source_payload_hash"] == sealed_source.payload_hash
    assert replay_semantic["source_semantic_content_hash"] == (sealed_source.semantic_content_hash)
    assert sealed_payload["message"].count(source_message) == 1

    publisher = _publisher(
        instance,
        _route("https://relay.test"),
        worker_id="initial-publisher",
    )
    try:
        assert await publisher.publish_once() == 0
        assert await instance.close() is None
        for restart_number in range(3):
            restart = V20Repository(_config(schema), shared_pool=pool)
            try:
                await restart.connect(migrate=False)
                await restart.acquire_runtime_leader(
                    route_id=config.route_id,
                    official_stream_id=config.official_stream_id,
                    lineage_id=config.state_lineage_id,
                )
                async with pool.acquire() as connection:
                    await connection.execute(
                        f"UPDATE {schema}.outbox_events "
                        "SET available_at=clock_timestamp()-interval '1 second' "
                        "WHERE event_id=$1",
                        event_id,
                    )
                restart_publisher = _publisher(
                    restart,
                    _route("https://relay.test"),
                    worker_id=f"restart-{restart_number}",
                )
                assert await restart_publisher.publish_once() == 0
            finally:
                await restart.close()
    finally:
        await instance.close()

    async with pool.acquire() as connection:
        final_outbox = await connection.fetchrow(
            f"SELECT delivery_status,attempt_count FROM {schema}.outbox_events WHERE event_id=$1",
            event_id,
        )
        attempts = await connection.fetch(
            f"SELECT event_id,phase FROM {schema}.delivery_attempts "
            "WHERE event_id=$1 ORDER BY attempt_number",
            event_id,
        )
    assert final_outbox["delivery_status"] == "DELIVERY_UNKNOWN"
    assert final_outbox["attempt_count"] == 1
    assert len(attempts) == 1
    assert attempts[0]["event_id"] == event_id
    assert attempts[0]["phase"] == "UNKNOWN"
    assert len(relay_posts) == 1
    assert relay_posts[0]["message"] == sealed_payload["message"]
    assert set(relay_posts[0]) == {"app_id", "app_secret", "chat_id", "message"}
    assert await _official_runtime_snapshot(pool, schema) == official_before


@pytest.mark.parametrize(
    "exception_factory",
    [
        lambda: httpx.ReadTimeout("relay accepted request but response timed out"),
        lambda: httpx.WriteTimeout("request write timed out"),
        lambda: httpx.RemoteProtocolError("connection broke after acceptance"),
        lambda: (
            lambda request: httpx.HTTPStatusError(
                "server error",
                request=request,
                response=httpx.Response(500, request=request),
            )
        )(httpx.Request("POST", "https://relay.invalid/api/send")),
        lambda: ValueError("legacy relay returned invalid JSON"),
    ],
    ids=["read-timeout", "write-timeout", "remote-protocol", "http-500", "bad-json"],
)
async def test_fake_http_ambiguous_failure_posts_once_and_is_unknown(
    repository, monkeypatch, exception_factory
) -> None:
    instance, pool, schema = repository
    await _seed_read_timeout_event(pool, schema)
    calls: list[dict] = []
    exception = exception_factory()

    async def accepted_then_failure(**kwargs):
        calls.append(dict(kwargs))
        raise exception

    monkeypatch.setattr("src.common.v20_feishu.post_message_once", accepted_then_failure)
    publisher = _publisher(instance, _route())

    assert await publisher.publish_once() == 0
    outbox, attempt = await _state(pool, schema)
    assert len(calls) == 1
    assert outbox["delivery_status"] == "DELIVERY_UNKNOWN"
    assert outbox["attempt_count"] == 1
    assert outbox["lease_owner"] is None
    assert outbox["lease_until"] is None
    assert attempt["phase"] == "UNKNOWN"
    assert attempt["completed_at"] is not None
    assert attempt["error_text"] is not None

    assert await publisher.publish_once() == 0
    assert len(calls) == 1


async def test_fake_http_cancel_posts_once_propagates_and_is_unknown(
    repository, monkeypatch
) -> None:
    instance, pool, schema = repository
    await _seed_read_timeout_event(pool, schema)
    calls: list[dict] = []

    async def accepted_then_cancel(**kwargs):
        calls.append(dict(kwargs))
        raise asyncio.CancelledError

    monkeypatch.setattr("src.common.v20_feishu.post_message_once", accepted_then_cancel)
    publisher = _publisher(instance, _route())

    with pytest.raises(asyncio.CancelledError):
        await publisher.publish_once()

    outbox, attempt = await _state(pool, schema)
    assert len(calls) == 1
    assert outbox["delivery_status"] == "DELIVERY_UNKNOWN"
    assert outbox["attempt_count"] == 1
    assert outbox["lease_owner"] is None
    assert outbox["lease_until"] is None
    assert attempt["phase"] == "UNKNOWN"
    assert attempt["completed_at"] is not None

    restart = _publisher(instance, _route(), worker_id="restart")
    assert await restart.publish_once() == 0
    assert len(calls) == 1


async def test_relay_false_is_terminal_unknown(repository) -> None:
    instance, pool, schema = repository
    await _seed_read_timeout_event(pool, schema)
    calls: list[dict] = []

    class FalseRelay:
        def is_configured(self):
            return True

        async def send_delivery(self, envelope, *, delivery_variant="PRIMARY"):
            calls.append({"envelope": dict(envelope), "variant": delivery_variant})
            return False

    false_route = type(
        "Route",
        (),
        {
            "is_configured": lambda self: True,
            "destination_fingerprint": "d" * 64,
            "transport": "legacy_send",
            "relay": lambda self: FalseRelay(),
        },
    )()
    assert await _publisher(instance, false_route).publish_once() == 0
    outbox, attempt = await _state(pool, schema)
    assert len(calls) == 1
    assert outbox["delivery_status"] == "DELIVERY_UNKNOWN"
    assert outbox["attempt_count"] == 1
    assert outbox["lease_owner"] is None
    assert outbox["lease_until"] is None
    assert attempt["phase"] == "UNKNOWN"
    assert await _publisher(instance, _route(), worker_id="restart").publish_once() == 0
    assert len(calls) == 1


async def test_closed_port_safe_retry_then_fake_server_receives_once(
    repository, monkeypatch
) -> None:
    instance, pool, schema = repository
    await _seed_read_timeout_event(pool, schema)
    closed_port = _publisher(instance, _route("https://127.0.0.1:1"))
    assert await closed_port.publish_once() == 0
    outbox, attempt = await _state(pool, schema)
    assert outbox["delivery_status"] == "PENDING"
    assert attempt["phase"] == "SAFE_RETRY"
    async with pool.acquire() as connection:
        await connection.execute(
            f"UPDATE {schema}.outbox_events "
            "SET available_at=clock_timestamp()-interval '1 second' "
            "WHERE event_id='read-timeout-event'"
        )

    calls: list[dict] = []

    async def fake_server_success(**kwargs):
        calls.append(dict(kwargs))
        return True

    monkeypatch.setattr("src.common.v20_feishu.post_message_once", fake_server_success)
    retried = _publisher(instance, _route(), worker_id="retry-owner")
    assert await retried.publish_once() == 1
    outbox, attempt = await _state(pool, schema)
    assert len(calls) == 1
    assert outbox["delivery_status"] == "SENT"
    assert attempt["phase"] == "DELIVERED"
    assert [row["phase"] for row in await _attempts(pool, schema)] == [
        "SAFE_RETRY",
        "DELIVERED",
    ]


@pytest.mark.parametrize(
    "safe_exception",
    [httpx.ConnectTimeout, httpx.PoolTimeout],
)
async def test_proven_pre_request_timeout_safe_retries_once(
    repository, monkeypatch, safe_exception
) -> None:
    instance, pool, schema = repository
    await _seed_read_timeout_event(pool, schema)
    calls: list[dict] = []

    async def timeout_then_success(**kwargs):
        calls.append(dict(kwargs))
        if len(calls) == 1:
            raise safe_exception("connection was never established")
        return True

    monkeypatch.setattr("src.common.v20_feishu.post_message_once", timeout_then_success)
    assert await _publisher(instance, _route()).publish_once() == 0
    async with pool.acquire() as connection:
        await connection.execute(
            f"UPDATE {schema}.outbox_events "
            "SET available_at=clock_timestamp()-interval '1 second' "
            "WHERE event_id='read-timeout-event'"
        )
    assert await _publisher(instance, _route(), worker_id="retry-owner").publish_once() == 1

    outbox, attempt = await _state(pool, schema)
    assert len(calls) == 2
    assert outbox["delivery_status"] == "SENT"
    assert outbox["attempt_count"] == 2
    assert attempt["phase"] == "DELIVERED"
    assert [row["phase"] for row in await _attempts(pool, schema)] == [
        "SAFE_RETRY",
        "DELIVERED",
    ]


async def test_finalize_before_commit_failure_leaves_started_unknown(
    repository, monkeypatch
) -> None:
    instance, pool, schema = repository
    await _seed_read_timeout_event(pool, schema)
    calls: list[dict] = []

    async def success_once(**kwargs):
        calls.append(dict(kwargs))
        return True

    class CompleteBeforeCommitFails:
        async def lease_outbox(self, **kwargs):
            return await instance.lease_outbox(**kwargs)

        async def begin_delivery_attempt(self, *args, **kwargs):
            return await instance.begin_delivery_attempt(*args, **kwargs)

        async def complete_delivery(self, *args, **kwargs):
            raise RuntimeError("simulate DB commit-before failure")

    monkeypatch.setattr("src.common.v20_feishu.post_message_once", success_once)
    publisher = _publisher(CompleteBeforeCommitFails(), _route())  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="DB commit-before failure"):
        await publisher.publish_once()

    outbox, attempt = await _state(pool, schema)
    assert len(calls) == 1
    assert outbox["delivery_status"] == "DELIVERY_UNKNOWN"
    assert outbox["attempt_count"] == 1
    assert outbox["lease_owner"] == "owner"
    assert outbox["lease_until"] is not None
    assert attempt["phase"] == "STARTED"
    assert await _publisher(instance, _route(), worker_id="restart").publish_once() == 0
    assert len(calls) == 1


async def test_finalize_after_commit_response_loss_is_sent(repository, monkeypatch) -> None:
    instance, pool, schema = repository
    await _seed_read_timeout_event(pool, schema)
    calls: list[dict] = []

    async def success_once(**kwargs):
        calls.append(dict(kwargs))
        return True

    class FirstCompleteFails:
        def __init__(self, inner: V20Repository):
            self.inner = inner
            self.calls = 0

        async def lease_outbox(self, **kwargs):
            return await self.inner.lease_outbox(**kwargs)

        async def begin_delivery_attempt(self, *args, **kwargs):
            return await self.inner.begin_delivery_attempt(*args, **kwargs)

        async def complete_delivery(self, *args, **kwargs):
            self.calls += 1
            await self.inner.complete_delivery(*args, **kwargs)
            raise RuntimeError("simulate finalize response loss")

    monkeypatch.setattr("src.common.v20_feishu.post_message_once", success_once)
    wrapper = FirstCompleteFails(instance)
    publisher = _publisher(wrapper, _route())  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="finalize response loss"):
        await publisher.publish_once()

    outbox, attempt = await _state(pool, schema)
    assert len(calls) == 1
    assert outbox["delivery_status"] == "SENT"
    assert attempt["phase"] == "DELIVERED"
    assert await _publisher(instance, _route(), worker_id="restart").publish_once() == 0
    assert len(calls) == 1


async def test_v20_uses_single_shared_primitive_and_v16_keeps_outer_retry(
    repository, monkeypatch
) -> None:
    instance, pool, schema = repository
    await _seed_read_timeout_event(pool, schema)
    primitive_calls: list[dict] = []

    async def primitive_success(**kwargs):
        primitive_calls.append(dict(kwargs))
        return True

    monkeypatch.setattr("src.common.v20_feishu.post_message_once", primitive_success)
    assert await _publisher(instance, _route()).publish_once() == 1
    assert len(primitive_calls) == 1
    assert primitive_calls[0]["app_id"] == "app"
    assert primitive_calls[0]["app_secret"] == "secret"
    assert primitive_calls[0]["chat_id"] == "chat"
    assert primitive_calls[0]["message"] == "one"


async def test_v16_send_message_preserves_original_outer_retry(monkeypatch) -> None:
    primitive_calls: list[dict] = []

    async def fail_once_then_succeed(**kwargs):
        primitive_calls.append(dict(kwargs))
        if len(primitive_calls) == 1:
            raise httpx.ConnectError("first V16 connection attempt failed")
        return True

    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr("src.common.feishu_bot.post_message_once", fail_once_then_succeed)
    monkeypatch.setattr("src.common.feishu_bot.asyncio.sleep", no_sleep)
    bot = FeishuBot(
        bot_url="https://relay.invalid",
        app_id="v16-app",
        app_secret="v16-secret",
        chat_id="v16-chat",
    )
    assert await bot.send_message("v16 retry compatibility", max_retries=1) is True
    assert len(primitive_calls) == 2
