from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.data.database.v20_repository import OutboxRecord, V20SemanticConflict, sha256_json
from src.web import v20_routes as routes
from src.web.v20_routes import _dispatch_manual_trigger

TZ = ZoneInfo("Asia/Shanghai")
TODAY = date(2026, 9, 1)
PRIOR = date(2026, 8, 31)


def _config() -> Any:
    return type(
        "Config",
        (),
        {
            "clock": type(
                "Clock",
                (),
                {
                    "prewarm": time(9, 15),
                    "publish_deadline": time(9, 40),
                },
            )(),
            "config_hash": "a" * 64,
            "state_semantics_hash": "b" * 64,
            "strategy_version": "V20",
            "deployment_mode": "forward_shadow",
            "route_id": "route-test",
            "official_stream_id": "stream-test",
            "state_lineage_id": "lineage-test",
        },
    )()


class Repository:
    def __init__(self, status: Any, source: OutboxRecord) -> None:
        self.status = status
        self.status_by_date = {status.trade_date: status}
        self.source = source
        self.events: dict[str, OutboxRecord] = {source.event_id: source}
        self.status_calls: list[date] = []
        self.enqueue_calls = 0
        self.seal_calls = 0
        self.official_writes = 0

    async def assert_runtime_leader(self) -> None:
        return None

    async def get_entry_status(self, _stream: str, trade_date: date) -> Any | None:
        self.status_calls.append(trade_date)
        return self.status_by_date.get(trade_date)

    async def get_outbox_event(self, event_id: str, **_scope: Any) -> Any | None:
        return self.events.get(event_id)

    async def enqueue_alert(
        self,
        event_id: str,
        _route_id: str,
        semantic: dict[str, Any],
        semantic_hash: str,
        **_scope: Any,
    ) -> bool:
        self.enqueue_calls += 1
        current = self.events.get(event_id)
        if current is not None:
            if current.semantic != semantic or current.semantic_content_hash != semantic_hash:
                raise V20SemanticConflict("same replay event has different semantics")
            return False
        self.events[event_id] = OutboxRecord(
            event_id=event_id,
            event_type="DATA_ALERT",
            route_id=self.source.route_id,
            official_stream_id=self.source.official_stream_id,
            lineage_id=self.source.lineage_id,
            semantic=dict(semantic),
            semantic_content_hash=semantic_hash,
            payload=None,
            payload_hash=None,
            generated_at=None,
            commit_marker=None,
            action_expiry_ts=None,
            delivery_status="PENDING",
            attempt_count=0,
        )
        return True

    async def seal_event(self, event_id: str, builder: Any) -> OutboxRecord:
        self.seal_calls += 1
        current = self.events[event_id]
        if current.payload is not None:
            return current
        sealed = replace(
            current,
            payload=builder(current, datetime(2026, 9, 1, 14, 6, tzinfo=TZ), 99, True),
            payload_hash=None,
            generated_at=datetime(2026, 9, 1, 14, 6, tzinfo=TZ),
            commit_marker=99,
        )
        sealed = replace(sealed, payload_hash=sha256_json(sealed.payload))
        self.events[event_id] = sealed
        return sealed

    async def commit_entry(self, *_args: Any, **_kwargs: Any) -> None:
        self.official_writes += 1
        raise AssertionError("terminal replay must not commit an entry")

    async def commit_exit(self, *_args: Any, **_kwargs: Any) -> None:
        self.official_writes += 1
        raise AssertionError("terminal replay must not commit an exit")


class Service:
    def __init__(self, repository: Repository) -> None:
        self.config = _config()
        self._repository = repository
        self._manual_trigger_lock = asyncio.Lock()
        self._decision_cycle_lock = asyncio.Lock()
        self.now = datetime(2026, 9, 1, 14, 5, tzinfo=TZ)
        self.mews_calls = 0
        self.mews_kick_calls = 0
        self.mews_calculation_calls = 0
        self.calendar_calls = 0
        self.morning_calls = 0
        self.canonical_calls: list[tuple[str, datetime, Any | None]] = []
        self.mews_kick_tasks: list[asyncio.Task[bool]] = []

    def _aware_now(self) -> datetime:
        return self.now

    async def _require_manual_trigger_ready(self) -> None:
        return None

    async def ensure_mews_for_selection_trigger(self, _now: datetime) -> bool:
        self.mews_calls += 1
        self.mews_calculation_calls += 1
        return False

    def kick_mews_for_selection_trigger(self, now: datetime) -> asyncio.Task[bool]:
        self.mews_kick_calls += 1
        task = asyncio.create_task(self.ensure_mews_for_selection_trigger(now))
        task.add_done_callback(lambda finished: finished.exception())
        self.mews_kick_tasks.append(task)
        return task

    async def _load_trade_calendar(self, _current_date: date) -> tuple[date, ...]:
        self.calendar_calls += 1
        raise AssertionError("sealed today terminal must be replayed before calendar")

    async def trigger_morning_selection(self, _request_id: str) -> Any:
        self.morning_calls += 1
        raise AssertionError("post-cutoff must not run morning selection")

    async def trigger_canonical_selection_check_only(
        self,
        request_id: str,
        now: datetime,
    ) -> dict[str, Any]:
        async with self._decision_cycle_lock:
            status = await self._repository.get_entry_status(
                self.config.official_stream_id,
                now.date(),
            )
            self.canonical_calls.append((request_id, now, status))
        return {
            "accepted": True,
            "created": True,
            "manual_request_id": request_id,
            "event_trade_date": now.date().isoformat(),
            "official_entry_action": status.action if status is not None else "MISSING",
            "official_entry_event_id": status.event_id if status is not None else None,
            "v20_action": "ENTER",
            "official_state_changed": False,
            "orders_changed": False,
            "non_actionable": True,
            "retrospective_expired": True,
        }

    def _verify_entry_binding(self, status: Any) -> None:
        assert status is self._repository.status_by_date.get(status.trade_date)


def _source(action: str) -> tuple[Any, OutboxRecord]:
    status = type(
        "Status",
        (),
        {
            "action": action,
            "trade_date": TODAY,
            "event_id": "entry-" + action.lower().replace("_", ""),
            "final_multiplier": 0.0 if action == "INPUT_INVALID" else 1.0,
            "semantic": {
                "symbols": [
                    {
                        "rank": 1,
                        "code": "000001",
                        "name": "平安银行",
                        "snapshot_price": 10.26,
                    }
                ]
            },
        },
    )()
    message = "[V20][SHADOW] 每日决策\n原始票单：一成不改 ✅\n换行 / Unicode / Δ bytes"
    payload = {"message": message}
    source = OutboxRecord(
        event_id=status.event_id,
        event_type="ENTRY_DECISION",
        route_id="route-test",
        official_stream_id="stream-test",
        lineage_id="lineage-test",
        semantic=dict(status.semantic),
        semantic_content_hash="1" * 64,
        payload=payload,
        payload_hash=sha256_json(payload),
        generated_at=datetime(2026, 9, 1, 9, 40, tzinfo=TZ),
        commit_marker=12,
        action_expiry_ts=None,
        delivery_status="SENT",
        attempt_count=1,
    )
    return status, source


def _forbid_fresh_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fresh_bomb(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("sealed today terminal must not enter fresh probe")

    async def select_bomb(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("sealed today terminal must not select a fresh context")

    monkeypatch.setattr(routes, "_run_fresh_0939_probe", fresh_bomb)
    monkeypatch.setattr(routes, "_select_fresh_probe_context", select_bomb)


@pytest.mark.parametrize("action", ["ENTER", "BLOCK", "NO_SIGNAL", "INPUT_INVALID"])
@pytest.mark.asyncio
async def test_post_cutoff_today_terminal_runs_current_check_only_and_settles_mews(
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    status, source = _source(action)
    repository = Repository(status, source)
    service = Service(repository)
    _forbid_fresh_probe(monkeypatch)

    result = await asyncio.wait_for(
        _dispatch_manual_trigger(service, "terminal-replay-001"),
        timeout=2.0,
    )

    assert result["official_entry_action"] == action
    assert result["official_entry_event_id"] == status.event_id
    assert result["official_state_changed"] is False
    assert result["orders_changed"] is False
    assert result["non_actionable"] is True
    assert len(service.canonical_calls) == 1
    assert service.canonical_calls[0][2] is status
    assert repository.official_writes == 0
    assert service.mews_calls == 1
    assert service.mews_kick_calls == 1
    assert service.calendar_calls == 0
    assert service.morning_calls == 0


@pytest.mark.asyncio
async def test_post_cutoff_terminal_race_waits_for_today_and_never_uses_prior_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current, current_source = _source("ENTER")
    repository = Repository(current, current_source)
    prior = type(
        "Status",
        (),
        {
            "action": "ENTER",
            "trade_date": PRIOR,
            "event_id": "prior-entry",
            "final_multiplier": 1.0,
            "semantic": {"symbols": [{"rank": 1, "code": "600000", "snapshot_price": 1.0}]},
        },
    )()
    prior_payload = {"message": "prior day must never win"}
    prior_source = OutboxRecord(
        event_id=prior.event_id,
        event_type="ENTRY_DECISION",
        route_id="route-test",
        official_stream_id="stream-test",
        lineage_id="lineage-test",
        semantic=dict(prior.semantic),
        semantic_content_hash="2" * 64,
        payload=prior_payload,
        payload_hash=sha256_json(prior_payload),
        generated_at=datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
        commit_marker=8,
        action_expiry_ts=None,
        delivery_status="SENT",
        attempt_count=1,
    )
    repository.events[prior_source.event_id] = prior_source
    current_visible = False

    async def racing_status(_stream: str, trade_date: date) -> Any | None:
        repository.status_calls.append(trade_date)
        if trade_date == TODAY:
            return current if current_visible else None
        if trade_date == PRIOR:
            return prior
        return None

    repository.status_by_date.clear()
    repository.status_by_date[PRIOR] = prior
    repository.get_entry_status = racing_status
    service = Service(repository)
    _forbid_fresh_probe(monkeypatch)
    await service._decision_cycle_lock.acquire()
    pending = asyncio.create_task(_dispatch_manual_trigger(service, "terminal-race-001"))
    try:
        await asyncio.sleep(0)
        current_visible = True
        repository.status_by_date[TODAY] = current
        service._decision_cycle_lock.release()
        result = await asyncio.wait_for(pending, timeout=2.0)
    except BaseException:
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        raise

    assert result["event_trade_date"] == TODAY.isoformat()
    assert repository.status_calls[0] == TODAY
    assert PRIOR not in repository.status_calls
    assert result["official_entry_event_id"] == current.event_id
    assert result["official_entry_action"] == current.action
    assert service.canonical_calls[0][2] is current
    assert service.mews_calls == 1
    assert service.calendar_calls == 0
    assert service.morning_calls == 0
