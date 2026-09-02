from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest

import src.web.v15_scan_service as v15_scan_service
import src.web.v20_service as service_module
from src.data.database.v20_repository import EntryStatus, V20SemanticConflict, sha256_json
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.models import (
    V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
    V20_ENTRY_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
    V20_INVALID_INPUT_SNAPSHOT_SCHEMA,
    V20_V16_SNAPSHOT_SCHEMA,
)
from src.strategy.v20.runtime_config import V20RouteBinding, load_v20_runtime_config
from src.web.v20_service import V20Service

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TZ = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 8, 31)
VALID_CALENDAR = (
    date(2026, 8, 28),
    TRADE_DATE,
    date(2026, 9, 1),
    date(2026, 9, 2),
)
CUTOFF = datetime(2026, 8, 31, 9, 40, tzinfo=TZ)


@pytest.fixture(autouse=True)
def _runtime_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.setenv("V20_MODE", "forward_shadow")
    monkeypatch.setenv("DB_SSLROOTCERT_SHA256", "c" * 64)
    monkeypatch.setenv("V20_INGEST_API_KEY", "i" * 32)
    monkeypatch.setenv("V20_STATUS_API_KEY", "s" * 32)
    monkeypatch.delenv("V20_ALLOW_PRODUCTION_PUSH", raising=False)


class _Repository:
    def __init__(self) -> None:
        self.events: dict[str, dict[str, Any]] = {}
        self.semantic_hashes: dict[str, str] = {}
        self.status: EntryStatus | None = None
        self.get_entry_status_calls = 0
        self.transition_after_first_get = False

    async def assert_runtime_leader(self) -> None:
        return None

    async def database_cutoff_reached(self, deadline: datetime) -> bool:
        assert deadline == CUTOFF
        return True

    async def get_entry_status(self, _stream_id: str, trade_date: date) -> EntryStatus | None:
        assert trade_date == TRADE_DATE
        self.get_entry_status_calls += 1
        current = self.status
        if self.transition_after_first_get and self.get_entry_status_calls == 1:
            self.status = _terminal_status(self._config, "ENTER")
            return None
        return current

    async def enqueue_alert(
        self,
        event_id: str,
        _route_id: str,
        semantic: dict[str, Any],
        _semantic_hash: str,
        **_kwargs: Any,
    ) -> bool:
        existing = self.events.get(event_id)
        if existing is not None:
            if existing != semantic:
                raise V20SemanticConflict("duplicate event id has different semantics")
            return False
        self.events[event_id] = dict(semantic)
        self.semantic_hashes[event_id] = _semantic_hash
        return True

    async def seal_event(self, _event_id: str, _payload: Any) -> None:
        return None


def _service(repository: _Repository) -> V20Service:
    config = load_v20_runtime_config(PROJECT_ROOT)
    binding = V20RouteBinding(
        route_id=config.route_id,
        expected_bot_origin="https://relay.internal",
        expected_app_id_sha256=hashlib.sha256(b"shadow-app").hexdigest(),
        expected_chat_id_sha256=hashlib.sha256(b"shadow-chat").hexdigest(),
    )
    config = replace(
        config,
        enabled=True,
        route_binding=binding,
        route_bindings={**config.route_bindings, "forward_shadow": binding},
        v20_db_ca_sha256="d" * 64,
        fundamentals_db_ca_sha256="c" * 64,
    )

    async def unused_mews(**_kwargs: Any) -> None:
        return None

    service = V20Service(
        config=config,
        repository=repository,
        scan_state=v15_scan_service.V15ScanState(initialized=True),
        artifacts=load_g_artifacts(
            config.artifact_manifest_path.parent,
            expected_manifest_sha256=config.artifact_manifest_sha256,
        ),
        publisher=SimpleNamespace(),
        routes={},
        mews_source=SimpleNamespace(fetch_snapshot=unused_mews),
    )
    repository._config = config
    return service


def _terminal_status(config: Any, action: str) -> EntryStatus:
    snapshot = {
        "schema_version": (
            V20_INVALID_INPUT_SNAPSHOT_SCHEMA
            if action == "INPUT_INVALID"
            else V20_DECISION_INPUT_SNAPSHOT_SCHEMA
        ),
        "state_semantics_hash": config.state_semantics_hash,
    }
    if action != "INPUT_INVALID":
        snapshot["v16_snapshot_schema_version"] = V20_V16_SNAPSHOT_SCHEMA
    semantic = {
        "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "strategy_version": config.strategy_version,
        "config_hash": config.config_hash,
        "state_semantics_hash": config.state_semantics_hash,
        "action": action,
    }
    return EntryStatus(
        official_stream_id=config.official_stream_id,
        trade_date=TRADE_DATE,
        slot_id=f"slot-{action.lower()}",
        slot_status="FAILED" if action == "INPUT_INVALID" else "COMPLETED",
        slot_revision=1,
        strategy_version=config.strategy_version,
        config_id=config.config_hash[:24],
        config_hash=config.config_hash,
        lineage_id=config.state_lineage_id,
        decision_id=f"decision-{action.lower()}",
        event_id=f"event-{action.lower()}",
        action=action,
        final_multiplier=0.0,
        semantic_content_hash=sha256_json(semantic),
        semantic=semantic,
        snapshot_id=f"snapshot-{action.lower()}",
        snapshot_hash=sha256_json(snapshot),
        snapshot=snapshot,
        action_expiry_ts=CUTOFF,
    )


async def test_cutoff_outer_budget_covers_preexisting_calendar_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _Repository()
    service = _service(repository)
    service._repository_started = True
    provider_started = asyncio.Event()

    async def provider() -> list[date]:
        provider_started.set()
        await asyncio.sleep(0.05)
        return list(VALID_CALENDAR)

    service._calendar_provider = provider
    assert (
        service_module._CALENDAR_CUTOFF_LOAD_BUDGET_SECONDS
        >= service_module.TRADE_CALENDAR_TIMEOUT_SECONDS
        + service_module.ENTRY_CUTOFF_RESERVE_SECONDS
    )

    # This master already exists before the cutoff caller joins it. Shrink the
    # provider timeout for a deterministic test while retaining the required
    # reserve relationship and using the production outer cutoff budget.
    monkeypatch.setattr(service_module, "TRADE_CALENDAR_TIMEOUT_SECONDS", 0.08)
    monkeypatch.setattr(service_module, "ENTRY_CUTOFF_RESERVE_SECONDS", 0.01)
    master = asyncio.create_task(service._load_trade_calendar(TRADE_DATE))
    await provider_started.wait()
    caller = asyncio.create_task(service._enforce_or_alert_entry_cutoff(TRADE_DATE, now=CUTOFF))

    assert await master == VALID_CALENDAR
    assert await caller is True
    assert service._calendar_cache == VALID_CALENDAR
    assert len(repository.events) == 1
    assert repository.events[next(iter(repository.events))]["alert_code"] == ("ENTRY_CUTOFF_NO_BUY")


async def test_calendar_failure_then_success_emits_one_stable_no_buy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _Repository()
    service = _service(repository)
    service._repository_started = True
    attempts = 0

    async def provider() -> list[date]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TimeoutError("provider bounded timeout")
        return list(VALID_CALENDAR)

    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", provider)
    service._calendar_provider = v15_scan_service.get_trade_calendar

    assert await service._enforce_or_alert_entry_cutoff(TRADE_DATE, now=CUTOFF) is True
    assert attempts == 1
    assert len(repository.events) == 1
    first_alert_id = next(iter(repository.events))
    decision_error = service._lane_health["decision"].last_error
    assert decision_error is not None
    assert decision_error.startswith("ENTRY_CALENDAR_UNKNOWN_AT_0940:")
    assert repository.events[first_alert_id]["alert_code"] == "ENTRY_CUTOFF_NO_BUY"
    first_semantic = dict(repository.events[first_alert_id])
    first_semantic_hash = repository.semantic_hashes[first_alert_id]

    await asyncio.sleep(0)
    later = datetime(2026, 8, 31, 9, 41, tzinfo=TZ)
    assert await service._enforce_or_alert_entry_cutoff(TRADE_DATE, now=later) is True
    assert attempts == 2
    assert [semantic["alert_code"] for semantic in repository.events.values()] == [
        "ENTRY_CUTOFF_NO_BUY"
    ]
    assert list(repository.events) == [first_alert_id]
    assert repository.events[first_alert_id] == first_semantic
    assert repository.semantic_hashes[first_alert_id] == first_semantic_hash


@pytest.mark.parametrize("action", ["ENTER", "BLOCK", "NO_SIGNAL", "INPUT_INVALID"])
async def test_cold_start_with_durable_terminal_entry_never_falls_back_to_no_buy(
    action: str,
) -> None:
    repository = _Repository()
    service = _service(repository)
    repository.status = _terminal_status(service.config, action)
    service._repository_started = True
    service._calendar_cache = VALID_CALENDAR
    service._calendar_loaded_for = TRADE_DATE
    assert service._context is None

    assert await service._enforce_or_alert_entry_cutoff(TRADE_DATE, now=CUTOFF) is True

    assert repository.events == {}
    assert repository.get_entry_status_calls >= 1


async def test_terminal_entry_wins_calendar_failure_race_before_public_alert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _Repository()
    service = _service(repository)
    service._repository_started = True
    repository.transition_after_first_get = True

    async def provider() -> list[date]:
        raise TimeoutError("provider bounded timeout")

    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", provider)
    service._calendar_provider = v15_scan_service.get_trade_calendar

    assert await service._enforce_or_alert_entry_cutoff(TRADE_DATE, now=CUTOFF) is True

    assert repository.events == {}
    assert repository.get_entry_status_calls >= 2


async def test_two_service_restarts_keep_one_stable_no_buy_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _Repository()
    first = _service(repository)
    second = _service(repository)
    first._repository_started = True
    second._repository_started = True

    async def provider() -> list[date]:
        raise TimeoutError("provider bounded timeout")

    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", provider)
    first._calendar_provider = v15_scan_service.get_trade_calendar
    second._calendar_provider = v15_scan_service.get_trade_calendar

    assert await first._enforce_or_alert_entry_cutoff(TRADE_DATE, now=CUTOFF) is True
    assert (
        await second._enforce_or_alert_entry_cutoff(
            TRADE_DATE,
            now=datetime(2026, 8, 31, 9, 41, tzinfo=TZ),
        )
        is True
    )

    assert list(repository.events) == [
        next(iter(repository.semantic_hashes)),
    ]
    assert list(repository.semantic_hashes) == list(repository.events)
    assert next(iter(repository.events.values()))["alert_code"] == "ENTRY_CUTOFF_NO_BUY"


@pytest.mark.parametrize(("closed_market", "expected"), [(False, False), (True, True)])
async def test_before_cutoff_and_confirmed_closure_have_no_side_effects(
    closed_market: bool,
    expected: bool,
) -> None:
    repository = _Repository()
    service = _service(repository)
    service._repository_started = True
    if closed_market:
        service._calendar_cache = (date(2026, 8, 28),)
        service._calendar_loaded_for = TRADE_DATE

    async def provider() -> list[date]:
        raise AssertionError("calendar provider must not run")

    service._calendar_provider = provider
    result = await service._enforce_or_alert_entry_cutoff(
        TRADE_DATE,
        now=CUTOFF if closed_market else CUTOFF - timedelta(milliseconds=1),
    )

    assert result is expected
    assert repository.get_entry_status_calls == 0
    assert repository.events == {}
