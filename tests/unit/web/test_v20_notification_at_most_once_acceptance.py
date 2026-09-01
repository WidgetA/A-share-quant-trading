"""Independent acceptance coverage for V20 notification at-most-once delivery."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest

from src.common.feishu_bot import FeishuBot
from src.common.v20_feishu import V20FeishuRoute, V20OutboxPublisher
from src.data.database.v20_repository import DeliveryAttempt, OutboxRecord

UTC = timezone.utc
SCOPE = {
    "route_id": "formal-route",
    "official_stream_id": "formal-stream",
    "lineage_id": "formal-lineage",
}


@dataclass
class _AttemptRow:
    attempt_number: int
    worker_id: str
    phase: str
    delivery_variant: str


class _RealCasOutboxRepository:
    """Small, thread-safe executor for the production outbox state contract.

    This is intentionally not a permissive mock: leasing, dispatch-boundary CAS,
    attempt ownership, and finalization all enforce the same conditions as the
    PostgreSQL SQL contract. The publisher and relay under test are production
    objects; PostgreSQL concurrency is separately covered by the F1 CI harness.
    """

    def __init__(self, event_id: str) -> None:
        self.event_id = event_id
        self.trigger_requests = 0
        self.delivery_status = "PENDING"
        self.attempt_count = 0
        self.available_at = datetime.now(UTC) - timedelta(seconds=1)
        self.lease_owner: str | None = None
        self.lease_until: datetime | None = None
        self.attempts: dict[int, _AttemptRow] = {}
        self._lock = asyncio.Lock()

    async def register_trigger(self) -> None:
        async with self._lock:
            self.trigger_requests += 1

    async def lease_outbox(self, **kwargs):
        worker_id = kwargs["worker_id"]
        async with self._lock:
            reclaim_expired_lease = (
                self.delivery_status == "LEASED"
                and self.lease_until is not None
                and self.lease_until < datetime.now(UTC)
                and not any(attempt.phase == "STARTED" for attempt in self.attempts.values())
            )
            if self.delivery_status != "PENDING" and not reclaim_expired_lease:
                return []
            if self.available_at > datetime.now(UTC):
                return []

            self.delivery_status = "LEASED"
            self.lease_owner = worker_id
            self.lease_until = datetime.now(UTC) + timedelta(
                seconds=kwargs.get("lease_seconds", 60)
            )
            return [self._record(worker_id)]

    async def begin_delivery_attempt(self, event_id, **kwargs):
        assert event_id == self.event_id
        async with self._lock:
            if (
                self.delivery_status != "LEASED"
                or self.lease_owner != kwargs["worker_id"]
                or kwargs["route_id"] != SCOPE["route_id"]
                or kwargs["official_stream_id"] != SCOPE["official_stream_id"]
                or kwargs["lineage_id"] != SCOPE["lineage_id"]
            ):
                raise RuntimeError("outbox dispatch lease is missing or not owned")
            attempt_number = self.attempt_count + 1
            self.attempts[attempt_number] = _AttemptRow(
                attempt_number=attempt_number,
                worker_id=kwargs["worker_id"],
                phase="STARTED",
                delivery_variant="PRIMARY",
            )
            self.delivery_status = "DELIVERY_UNKNOWN"
            self.attempt_count = attempt_number
            return DeliveryAttempt(
                attempt_number=attempt_number,
                delivery_variant="PRIMARY",
            )

    async def defer_before_dispatch(self, event_id, **kwargs):
        assert event_id == self.event_id
        async with self._lock:
            if self.delivery_status != "LEASED" or self.lease_owner != kwargs["worker_id"]:
                raise RuntimeError("outbox pre-dispatch release CAS lost")
            self.delivery_status = "PENDING"
            self.lease_owner = None
            self.lease_until = None
            self.available_at = datetime.now(UTC) + timedelta(
                seconds=kwargs.get("retry_after_seconds", 30)
            )

    async def complete_delivery(self, event_id, **kwargs):
        assert event_id == self.event_id
        outcome = kwargs["outcome"]
        async with self._lock:
            attempt = self.attempts.get(kwargs["attempt_number"])
            if (
                self.delivery_status != "DELIVERY_UNKNOWN"
                or attempt is None
                or attempt.phase != "STARTED"
                or attempt.worker_id != kwargs["worker_id"]
                or self.lease_owner != kwargs["worker_id"]
            ):
                raise RuntimeError("outbox delivery finalize CAS lost")
            if outcome == "DELIVERED":
                attempt.phase = "DELIVERED"
                self.delivery_status = "SENT"
                self.lease_owner = None
                self.lease_until = None
            elif outcome == "SAFE_RETRY":
                attempt.phase = "SAFE_RETRY"
                self.delivery_status = "PENDING"
                self.lease_owner = None
                self.lease_until = None
                self.available_at = datetime.now(UTC) + timedelta(
                    seconds=kwargs.get("retry_after_seconds", 30)
                )
            else:
                assert outcome == "UNKNOWN"
                attempt.phase = "UNKNOWN"
                self.delivery_status = "DELIVERY_UNKNOWN"
                self.lease_owner = None
                self.lease_until = None

    def _record(self, worker_id: str) -> OutboxRecord:
        return OutboxRecord(
            event_id=self.event_id,
            event_type="DATA_ALERT",
            route_id=SCOPE["route_id"],
            official_stream_id=SCOPE["official_stream_id"],
            lineage_id=SCOPE["lineage_id"],
            semantic={"alert_code": "AT_MOST_ONCE_ACCEPTANCE"},
            semantic_content_hash="a" * 64,
            payload={"message": "at-most-once acceptance"},
            payload_hash="b" * 64,
            generated_at=datetime.now(UTC),
            commit_marker=1,
            action_expiry_ts=None,
            delivery_status=self.delivery_status,
            attempt_count=self.attempt_count,
            lease_db_ts=datetime.now(UTC),
        )


class _RelayResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            request = httpx.Request("POST", "https://relay.example/api/send")
            response = httpx.Response(self.status_code, request=request)
            raise httpx.HTTPStatusError(
                f"HTTP {self.status_code}",
                request=request,
                response=response,
            )

    def json(self) -> dict[str, Any]:
        return self._payload


class _SinglePostTransport:
    posts: list[dict[str, Any]] = []
    responses: list[_RelayResponse] = []
    exceptions: list[Exception] = []

    def __init__(self, **_kwargs) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args) -> None:
        return None

    async def post(self, url: str, *, json: dict[str, Any]) -> _RelayResponse:
        self.posts.append({"url": url, "json": dict(json)})
        if self.exceptions:
            raise self.exceptions.pop(0)
        if not self.responses:
            return _RelayResponse(200, {"code": 0, "msg": "success"})
        return self.responses.pop(0)


def _publisher(repository: _RealCasOutboxRepository, worker_id: str) -> V20OutboxPublisher:
    route = V20FeishuRoute(
        route_id=SCOPE["route_id"],
        bot_url="https://relay.example",
        app_id="app",
        app_secret="secret",
        chat_id="chat",
        transport="legacy_send",
    )
    return V20OutboxPublisher(
        repository,
        {SCOPE["route_id"]: route},
        worker_id=worker_id,
        **SCOPE,
    )


def _reset_transport(
    responses: list[_RelayResponse] | None = None,
    exceptions: list[Exception] | None = None,
) -> list[dict[str, Any]]:
    _SinglePostTransport.posts = []
    _SinglePostTransport.responses = list(responses or [])
    _SinglePostTransport.exceptions = list(exceptions or [])
    return _SinglePostTransport.posts


@pytest.mark.asyncio
async def test_three_repeated_triggers_and_concurrent_workers_post_once(monkeypatch):
    monkeypatch.setattr("src.common.feishu_bot.httpx.AsyncClient", _SinglePostTransport)
    posts = _reset_transport()
    repository = _RealCasOutboxRepository("same-user-visible-event")

    await asyncio.gather(*(repository.register_trigger() for _ in range(3)))
    publishers = [_publisher(repository, f"worker-{index}") for index in range(3)]

    sent = await asyncio.gather(*(publisher.publish_once() for publisher in publishers))

    assert sent == [1, 0, 0]
    assert repository.trigger_requests == 3
    assert len(posts) == 1
    assert posts[0]["url"] == "https://relay.example/api/send"
    assert posts[0]["json"] == {
        "app_id": "app",
        "app_secret": "secret",
        "chat_id": "chat",
        "message": "at-most-once acceptance",
    }
    assert repository.delivery_status == "SENT"
    assert repository.attempt_count == 1
    assert repository.attempts[1].phase == "DELIVERED"


@pytest.mark.asyncio
async def test_ambiguous_timeout_after_acceptance_survives_restarts_without_repost(
    monkeypatch,
):
    monkeypatch.setattr("src.common.feishu_bot.httpx.AsyncClient", _SinglePostTransport)
    posts = _reset_transport(
        exceptions=[httpx.ReadTimeout("relay accepted request but response timed out")]
    )
    repository = _RealCasOutboxRepository("accepted-then-timeout-event")
    first_worker = _publisher(repository, "first-worker")

    assert await first_worker.publish_once() == 0

    for restart_index in range(3):
        restarted = _publisher(repository, f"restart-{restart_index}")
        assert await restarted.publish_once() == 0

    assert len(posts) == 1
    assert repository.delivery_status == "DELIVERY_UNKNOWN"
    assert repository.attempt_count == 1
    assert repository.attempts[1].phase == "UNKNOWN"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        _RelayResponse(200, {"code": False, "msg": "json false is not integer zero"}),
        _RelayResponse(200, {"code": 0.0, "msg": "float zero is not integer zero"}),
        _RelayResponse(200, {"code": "0", "msg": "string zero is not integer zero"}),
        _RelayResponse(200, {"code": None, "msg": "missing code"}),
        _RelayResponse(500, {"code": 0, "msg": "HTTP error cannot be success"}),
    ],
    ids=["false", "float-zero", "string-zero", "null-code", "http-500"],
)
async def test_invalid_success_evidence_is_unknown_and_never_reposts(monkeypatch, response):
    monkeypatch.setattr("src.common.feishu_bot.httpx.AsyncClient", _SinglePostTransport)
    posts = _reset_transport(responses=[response])
    repository = _RealCasOutboxRepository("invalid-receipt-event")
    publisher = _publisher(repository, "worker")

    assert await publisher.publish_once() == 0
    assert await _publisher(repository, "restart").publish_once() == 0

    assert len(posts) == 1
    assert repository.delivery_status == "DELIVERY_UNKNOWN"
    assert repository.attempt_count == 1
    assert repository.attempts[1].phase == "UNKNOWN"


@pytest.mark.asyncio
async def test_v16_and_v20_use_the_same_single_post_api_contract(monkeypatch):
    monkeypatch.setattr("src.common.feishu_bot.httpx.AsyncClient", _SinglePostTransport)
    posts = _reset_transport()
    repository = _RealCasOutboxRepository("shared-infrastructure-event")

    assert await _publisher(repository, "v20-worker").publish_once() == 1
    bot = FeishuBot(
        bot_url="https://relay.example",
        app_id="app",
        app_secret="secret",
        chat_id="chat",
    )
    assert await bot.send_message("V16 shared primitive", max_retries=0) is True

    assert len(posts) == 2
    assert [post["url"] for post in posts] == [
        "https://relay.example/api/send",
        "https://relay.example/api/send",
    ]
    assert all(
        set(post["json"]) == {"app_id", "app_secret", "chat_id", "message"} for post in posts
    )
