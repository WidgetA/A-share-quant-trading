from __future__ import annotations

import asyncio
import json
import os
import uuid

import asyncpg
import httpx
import pytest

from src.common.feishu_bot import FeishuBot
from src.common.v20_feishu import V20FeishuRoute, V20OutboxPublisher
from src.data.database.v20_repository import V20DatabaseConfig, V20Repository, sha256_json

DSN = os.environ.get("V20_TEST_POSTGRES_DSN", "")
if not DSN:
    pytest.fail(
        "V20_TEST_POSTGRES_DSN is required for real PostgreSQL V20 Feishu tests",
        pytrace=False,
    )

SAFE_PREFIX = "v20_feishu_test_"
pytestmark = pytest.mark.postgres


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
        yield instance, pool, schema
    finally:
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
    instance, _, _ = repository
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
