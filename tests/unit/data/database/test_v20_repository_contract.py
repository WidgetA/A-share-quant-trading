import asyncio
import hashlib
import inspect
import json
import re
import ssl
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import certifi
import pytest

import src.strategy.v20.runtime_config as runtime_config_module
from src.data.database.v20_repository import (
    EntryCommit,
    ExitCommit,
    ModelBatchWrite,
    ModelLegWrite,
    V20DatabaseConfig,
    V20EntryDeadlineExceeded,
    V20MinuteBarIntegrityConflict,
    V20Repository,
    V20SemanticConflict,
    V20StateConflict,
    canonical_json,
    create_embedded_v20_repository_from_config,
    create_v20_repository_from_config,
    migration_sql,
    sha256_json,
)
from src.strategy.v20.decision_engine import genesis_state
from src.strategy.v20.models import HealthObservation, deserialize_health_snapshot
from src.strategy.v20.policy import advance_health_state
from src.strategy.v20.runtime_config import (
    load_v20_runtime_config,
    state_semantics_payload_from_frozen_payload,
)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
SCOPE = {
    "official_stream_id": "official",
    "lineage_id": "lineage-1",
}


class _AsyncContext:
    def __init__(self, value=None) -> None:
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args) -> None:
        return None


class _FakeConnection:
    def __init__(self, *, fetchrows=(), fetches=(), fetchvals=(), executes=()) -> None:
        self.fetchrow_results = list(fetchrows)
        self.fetch_results = list(fetches)
        self.fetchval_results = list(fetchvals)
        self.execute_results = list(executes)
        self.calls: list[tuple[str, str, tuple[object, ...]]] = []

    def transaction(self, **kwargs):
        self.calls.append(("transaction", canonical_json(kwargs), ()))
        return _AsyncContext()

    async def fetchrow(self, sql, *args):
        self.calls.append(("fetchrow", sql, args))
        return self.fetchrow_results.pop(0)

    async def fetch(self, sql, *args):
        self.calls.append(("fetch", sql, args))
        return self.fetch_results.pop(0)

    async def fetchval(self, sql, *args):
        self.calls.append(("fetchval", sql, args))
        return self.fetchval_results.pop(0)

    async def execute(self, sql, *args):
        self.calls.append(("execute", sql, args))
        return self.execute_results.pop(0) if self.execute_results else "OK"


class _FakePool:
    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection

    def acquire(self):
        return _AsyncContext(self.connection)


def _repository(connection: _FakeConnection) -> V20Repository:
    repository = V20Repository(V20DatabaseConfig())
    repository._pool = _FakePool(connection)  # type: ignore[assignment]
    return repository


def _checkpoint_shadow_row(
    batch_id: str,
    kind: str,
    signal_date: date,
    *,
    status: str = "COMPLETE_VALID",
) -> dict[str, object]:
    reference_status = "LOCKED" if status == "COMPLETE_VALID" else "UNAVAILABLE"
    return {
        "batch_id": batch_id,
        "decision_id": f"source-decision:{batch_id}",
        "official_stream_id": "shadow-stream",
        "lineage_id": "shadow-lineage",
        "source_batch_id": None,
        "kind": kind,
        "signal_date": signal_date,
        "t2_date": signal_date + timedelta(days=2),
        "status": status,
        "batch_json": canonical_json({"symbols": ["000001"]}),
        "batch_return": 0.01 if status == "COMPLETE_VALID" else None,
        "reference_status": reference_status,
        "reference_prices_json": (
            canonical_json({"000001": 10.0}) if reference_status == "LOCKED" else None
        ),
        "reference_snapshot_hash": "b" * 64,
    }


def _checkpoint_source_state() -> dict[str, object]:
    health_rows = []
    for index in range(3):
        signal = date(2026, 7, 1) + timedelta(days=index)
        health_rows.append(
            {
                "batch_id": f"health-{index}",
                "signal_date": signal.isoformat(),
                "t2_exit_date": (signal + timedelta(days=2)).isoformat(),
                "relative_return": 0.01,
            }
        )
    return {
        "schema_version": "v20-official-state/v1",
        "state_revision": 42,
        "health": {
            "schema_version": "v20-health-snapshot/v1",
            "status": "HEALTHY",
            "recovery_count": 0,
            "recent_valid": health_rows,
            "last_processed_key": ["2026-07-05", "2026-07-03", "health-2"],
        },
        "official_rolling_gaps": [
            {
                "gap_id": "active-gap",
                "signal_date": "2026-07-10",
                "maturity_date": "2026-07-12",
                "closed": False,
                "aged_out": False,
            }
        ],
        "last_terminal_slot_id": "shadow-slot-20260831",
        "last_terminal_trade_date": "2026-08-31",
    }


def _checkpoint_source_config() -> dict[str, object]:
    project_root = Path(__file__).resolve().parents[4]
    return dict(load_v20_runtime_config(project_root).frozen_payload)


def _legacy_runtime_payload(current: dict[str, object]) -> tuple[dict[str, object], str]:
    legacy = json.loads(canonical_json(current))
    legacy.pop("state_semantics_payload", None)
    legacy["route_id"] = "legacy-reviewed-route"
    legacy_semantics = {
        "schema_version": "v20-state-semantics/v1",
        "strategy_version": legacy["strategy_version"],
        "timezone": legacy["timezone"],
        "return_profile_id": legacy["return_profile_id"],
        "reference_profile_id": legacy["reference_profile_id"],
        "clock": legacy["clock"],
        "market_data": legacy["market_data"],
        "policy": legacy["policy"],
        "g_manifest_sha256": legacy["g_manifest_sha256"],
        "strategy_dependency_hashes": legacy["strategy_dependency_hashes"],
    }
    legacy_hash = sha256_json(legacy_semantics)
    legacy["state_semantics_hash"] = legacy_hash
    return legacy, legacy_hash


def _config_slot_row(
    payload: dict[str, object],
    *,
    slot_status: str = "FAILED",
) -> dict[str, object]:
    config_hash = sha256_json(payload)
    return {
        "slot_config_id": config_hash[:24],
        "slot_config_hash": config_hash,
        "slot_strategy_version": payload["strategy_version"],
        "slot_status": slot_status,
        "runtime_config_id": config_hash[:24],
        "runtime_config_hash": config_hash,
        "runtime_strategy_version": payload["strategy_version"],
        "config_json": canonical_json(payload),
    }


def _runtime_config_row(payload: dict[str, object]) -> dict[str, object]:
    config_hash = sha256_json(payload)
    return {
        "config_id": config_hash[:24],
        "config_hash": config_hash,
        "strategy_version": payload["strategy_version"],
        "config_json": canonical_json(payload),
    }


@pytest.mark.asyncio
async def test_genesis_authenticates_legacy_to_core_without_rewriting_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = Path(__file__).resolve().parents[4]
    config = load_v20_runtime_config(project_root)
    current = json.loads(canonical_json(config.frozen_payload))
    legacy, legacy_hash = _legacy_runtime_payload(current)
    monkeypatch.setattr(
        runtime_config_module,
        "_AUDITED_LEGACY_STATE_SEMANTICS_HASHES",
        frozenset({legacy_hash}),
    )
    state = genesis_state()
    state_hash = sha256_json(state)
    registry = {
        "official_stream_id": config.official_stream_id,
        "genesis_state_hash": state_hash,
        "state_semantics_hash": legacy_hash,
        "bootstrap_mode": "EMPTY_FORWARD_SHADOW",
        "bootstrap_checkpoint_hash": None,
        "bootstrap_predecessor_trade_date": date(2026, 8, 30),
    }
    current_hash = sha256_json(current)
    legacy_config_hash = sha256_json(legacy)
    evidence = {
        "schema_version": "v20-state-semantics-compatibility/v1",
        "lineage_id": config.state_lineage_id,
        "official_stream_id": config.official_stream_id,
        "legacy_state_semantics_hash": legacy_hash,
        "core_state_semantics_hash": config.state_semantics_hash,
        "evidence_config_id": legacy_config_hash[:24],
        "evidence_config_hash": legacy_config_hash,
        "accepted_config_id": current_hash[:24],
        "accepted_config_hash": current_hash,
        "dependency_diff": [],
    }
    persisted_evidence = {
        "official_stream_id": config.official_stream_id,
        "evidence_config_id": legacy_config_hash[:24],
        "evidence_config_hash": legacy_config_hash,
        "accepted_config_id": current_hash[:24],
        "accepted_config_hash": current_hash,
        "evidence_json": canonical_json(evidence),
        "evidence_hash": sha256_json(evidence),
    }
    advanced_state = {
        **state,
        "state_revision": 1,
        "last_terminal_slot_id": "legacy-terminal-slot",
        "last_terminal_trade_date": "2026-08-31",
    }
    advanced_hash = sha256_json(advanced_state)
    state_row = {
        "revision": 1,
        "state_hash": advanced_hash,
        "state_json": canonical_json(advanced_state),
    }
    connection = _FakeConnection(
        fetchrows=[
            registry,
            _runtime_config_row(current),
            persisted_evidence,
            state_row,
        ],
        fetches=[[_config_slot_row(legacy)]],
    )
    repository = _repository(connection)

    stored = await repository.ensure_genesis_state(
        config.state_lineage_id,
        state,
        state_hash,
        official_stream_id=config.official_stream_id,
        state_semantics_hash=config.state_semantics_hash,
        current_config_id=current_hash[:24],
        current_config_hash=current_hash,
        current_config_payload=current,
        bootstrap_mode="EMPTY_FORWARD_SHADOW",
        bootstrap_checkpoint_hash=None,
        bootstrap_predecessor_trade_date=date(2026, 8, 30),
    )

    assert (stored.revision, stored.state_hash, stored.payload) == (
        1,
        advanced_hash,
        advanced_state,
    )
    assert len(repository.compatible_entry_bindings) == 1
    binding = next(iter(repository.compatible_entry_bindings))
    assert (binding.config_id, binding.config_hash, binding.state_semantics_hash) == (
        legacy_config_hash[:24],
        legacy_config_hash,
        legacy_hash,
    )
    assert any(
        call[0] == "execute" and "INSERT INTO v20.state_semantics_compatibility" in call[1]
        for call in connection.calls
    )
    assert not any(
        call[0] == "execute" and "SET state_semantics_hash" in call[1] for call in connection.calls
    )
    assert not any(
        call[0] == "execute" and "UPDATE v20.official_state" in call[1] for call in connection.calls
    )


@pytest.mark.asyncio
async def test_genesis_loads_prior_v2_terminal_binding_when_core_is_unchanged() -> None:
    project_root = Path(__file__).resolve().parents[4]
    config = load_v20_runtime_config(project_root)
    current = json.loads(canonical_json(config.frozen_payload))
    historical = json.loads(canonical_json(current))
    historical["route_id"] = "historical-notification-route"
    historical_hash = sha256_json(historical)
    state = genesis_state()
    state_hash = sha256_json(state)
    registry = {
        "official_stream_id": config.official_stream_id,
        "genesis_state_hash": state_hash,
        "state_semantics_hash": config.state_semantics_hash,
        "bootstrap_mode": "EMPTY_FORWARD_SHADOW",
        "bootstrap_checkpoint_hash": None,
        "bootstrap_predecessor_trade_date": date(2026, 8, 30),
    }
    state_row = {
        "revision": 0,
        "state_hash": state_hash,
        "state_json": canonical_json(state),
    }
    connection = _FakeConnection(
        fetchrows=[registry, _runtime_config_row(current), state_row],
        fetches=[[_config_slot_row(historical)]],
    )
    repository = _repository(connection)
    current_hash = sha256_json(current)

    await repository.ensure_genesis_state(
        config.state_lineage_id,
        state,
        state_hash,
        official_stream_id=config.official_stream_id,
        state_semantics_hash=config.state_semantics_hash,
        current_config_id=current_hash[:24],
        current_config_hash=current_hash,
        current_config_payload=current,
        bootstrap_mode="EMPTY_FORWARD_SHADOW",
        bootstrap_checkpoint_hash=None,
        bootstrap_predecessor_trade_date=date(2026, 8, 30),
    )

    binding = next(iter(repository.compatible_entry_bindings))
    assert (binding.config_id, binding.config_hash) == (
        historical_hash[:24],
        historical_hash,
    )
    assert not any(
        call[0] == "execute" and "state_semantics_compatibility" in call[1]
        for call in connection.calls
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("slot_status", ["OPEN", "COMPLETED"])
async def test_genesis_rejects_open_or_core_changed_historical_config(
    monkeypatch: pytest.MonkeyPatch,
    slot_status: str,
) -> None:
    project_root = Path(__file__).resolve().parents[4]
    config = load_v20_runtime_config(project_root)
    current = json.loads(canonical_json(config.frozen_payload))
    legacy, legacy_hash = _legacy_runtime_payload(current)
    monkeypatch.setattr(
        runtime_config_module,
        "_AUDITED_LEGACY_STATE_SEMANTICS_HASHES",
        frozenset({legacy_hash}),
    )
    if slot_status == "COMPLETED":
        dependencies = dict(current["strategy_dependency_hashes"])
        dependencies["src/strategy/v20/policy.py"] = "f" * 64
        current["strategy_dependency_hashes"] = dependencies
        state_payload = state_semantics_payload_from_frozen_payload(current)
        current["state_semantics_payload"] = state_payload
        current["state_semantics_hash"] = sha256_json(state_payload)
    state = genesis_state()
    state_hash = sha256_json(state)
    registry = {
        "official_stream_id": config.official_stream_id,
        "genesis_state_hash": state_hash,
        "state_semantics_hash": legacy_hash,
        "bootstrap_mode": "EMPTY_FORWARD_SHADOW",
        "bootstrap_checkpoint_hash": None,
        "bootstrap_predecessor_trade_date": date(2026, 8, 30),
    }
    current_hash = sha256_json(current)
    connection = _FakeConnection(
        fetchrows=[registry, _runtime_config_row(current)],
        fetches=[[_config_slot_row(legacy, slot_status=slot_status)]],
    )

    with pytest.raises(V20SemanticConflict):
        await _repository(connection).ensure_genesis_state(
            config.state_lineage_id,
            state,
            state_hash,
            official_stream_id=config.official_stream_id,
            state_semantics_hash=str(current["state_semantics_hash"]),
            current_config_id=current_hash[:24],
            current_config_hash=current_hash,
            current_config_payload=current,
            bootstrap_mode="EMPTY_FORWARD_SHADOW",
            bootstrap_checkpoint_hash=None,
            bootstrap_predecessor_trade_date=date(2026, 8, 30),
        )


def _compact_sql(value: str) -> str:
    value = "\n".join(line for line in value.splitlines() if not line.startswith("--"))
    return re.sub(r"\s+", " ", value).strip()


def _assert_call_is_scoped(call: tuple[str, str, tuple[object, ...]]) -> None:
    sql = _compact_sql(call[1])
    assert re.search(r"(?:slot|shadow)\.official_stream_id=", sql)
    assert re.search(r"(?:slot|shadow)\.lineage_id=", sql)
    assert SCOPE["official_stream_id"] in call[2]
    assert SCOPE["lineage_id"] in call[2]


def _entry(action: str) -> EntryCommit:
    state_before = {"revision": 0}
    state_after = {"revision": 1, "terminal": action}
    snapshot = {"trade_date": "2026-08-31", "codes": []}
    semantic = {"action": action, "final_multiplier": 0.0}
    return EntryCommit(
        official_stream_id="official",
        slot_id="slot-20260831",
        trade_date=date(2026, 8, 31),
        strategy_version="V20",
        config_id="config-1",
        config_hash="a" * 64,
        lineage_id="lineage-1",
        expected_state_revision=0,
        expected_state_hash=sha256_json(state_before),
        next_state=state_after,
        next_state_hash=sha256_json(state_after),
        snapshot_id="snapshot-1",
        snapshot_hash=sha256_json(snapshot),
        snapshot=snapshot,
        decision_id="decision-1",
        event_id="event-1",
        action=action,
        final_multiplier=0.0,
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
        action_expiry_ts=datetime(2026, 8, 31, 9, 40, tzinfo=BEIJING_TZ),
        route_id="route-1",
        invalid_commit_not_before_ts=(
            datetime(2026, 8, 31, 9, 45, tzinfo=BEIJING_TZ) if action == "INPUT_INVALID" else None
        ),
    )


def _outbox_event_row(
    *,
    route_id: str = "alert-route",
    official_stream_id: str = "official",
    lineage_id: str = "lineage-1",
) -> dict[str, object]:
    semantic = {
        "schema_version": "v20-data-alert-semantic/v2",
        "alert_code": "MANUAL_TRIGGER_RECEIPT",
        "non_actionable": True,
    }
    payload = {
        "schema_version": "v20-feishu-payload/v2",
        "event_id": "manual-event",
        "message": "[V20] 人工触发验证（非交易指令）",
    }
    return {
        "event_id": "manual-event",
        "event_type": "DATA_ALERT",
        "route_id": route_id,
        "official_stream_id": official_stream_id,
        "lineage_id": lineage_id,
        "semantic_content_hash": sha256_json(semantic),
        "semantic_json": canonical_json(semantic),
        "payload_json": canonical_json(payload),
        "payload_hash": sha256_json(payload),
        "generated_at": datetime(2026, 8, 31, 15, 30, tzinfo=BEIJING_TZ),
        "commit_marker": 19,
        "action_expiry_ts": None,
        "delivery_status": "PENDING",
        "attempt_count": 0,
    }


def _enter(multiplier: float, leg_count: int) -> EntryCommit:
    base = _entry("NO_SIGNAL")
    semantic = {"action": "ENTER", "final_multiplier": multiplier}
    legs = tuple(
        ModelLegWrite(
            model_leg_id=f"leg-{rank}",
            code=f"{rank:06d}",
            stock_name=f"stock-{rank}",
            rank=rank,
            relative_weight=multiplier / leg_count,
            d1=date(2026, 9, 1),
            d2=date(2026, 9, 2),
        )
        for rank in range(1, leg_count + 1)
    )
    return EntryCommit(
        **{
            **base.__dict__,
            "action": "ENTER",
            "final_multiplier": multiplier,
            "semantic": semantic,
            "semantic_content_hash": sha256_json(semantic),
            "model_batch": ModelBatchWrite(
                model_batch_id="batch-1",
                multiplier=multiplier,
                evaluation_only=False,
                reference_profile_id="profile-1",
                legs=legs,
            ),
        }
    )


def _entry_connection(commit: EntryCommit, *, extra_execute_count: int = 0) -> _FakeConnection:
    return _FakeConnection(
        fetchrows=[
            None,
            {
                "config_hash": commit.config_hash,
                "strategy_version": commit.strategy_version,
                "effective_trade_date": commit.trade_date,
            },
            {"revision": 0, "state_hash": commit.expected_state_hash},
            {
                "slot_id": commit.slot_id,
                "strategy_version": commit.strategy_version,
                "config_id": commit.config_id,
                "config_hash": commit.config_hash,
                "lineage_id": commit.lineage_id,
                "slot_status": "OPEN",
                "slot_revision": 0,
            },
            {
                "snapshot_hash": commit.snapshot_hash,
                "snapshot_json": canonical_json(commit.snapshot),
            },
        ],
        executes=["OK"] * (3 + extra_execute_count) + ["UPDATE 1", "UPDATE 1"],
    )


def test_standalone_migration_is_identical_to_runtime_default_schema() -> None:
    root = Path(__file__).resolve().parents[4]
    standalone = (root / "migrations" / "v20" / "001_v20.sql").read_text(encoding="utf-8")

    assert _compact_sql(standalone) == _compact_sql(migration_sql("v20"))
    assert "commit_fingerprint CHAR(64) NOT NULL" in standalone
    assert "reference_status='UNAVAILABLE'" in standalone
    assert "uq_v20_shadow_source_mapping" in standalone
    assert "bootstrap_predecessor_trade_date DATE NOT NULL" in standalone
    assert "official_stream_id TEXT NOT NULL" in standalone
    assert "lineage_id TEXT NOT NULL" in standalone
    assert "LEGACY_UNSCOPED" in standalone
    assert "idx_v20_outbox_scope_ready" in standalone
    assert "idx_v20_outbox_scope_unsealed" in standalone
    assert "idx_v20_minute_bar_time_code_label" in standalone
    assert standalone.index("CREATE TABLE IF NOT EXISTS v20.exit_reminders") < standalone.index(
        "FROM v20.exit_reminders AS reminder"
    )


@pytest.mark.asyncio
async def test_unsealed_scan_is_bound_to_route_stream_and_lineage() -> None:
    connection = _FakeConnection(fetches=[[]])

    assert (
        await _repository(connection).list_unsealed_outbox_event_ids(
            route_id="formal-route",
            **SCOPE,
            limit=20,
        )
        == ()
    )

    call = [item for item in connection.calls if item[0] == "fetch"][0]
    sql = _compact_sql(call[1])
    assert "route_id=$1 AND official_stream_id=$2 AND lineage_id=$3" in sql
    assert "action_expiry_ts > clock_timestamp()" in sql
    assert "seal_attempt_count" in sql
    assert "created_at DESC" in sql
    assert "FROM updated JOIN candidates USING (event_id)" in sql
    assert "ORDER BY candidates.delivery_priority" in sql
    assert (
        sql.index("semantic_json->>'delivery_priority_class'='LIVE_EXIT' THEN 1")
        < sql.index("semantic_json->>'delivery_priority_class'= 'RUNTIME_CRITICAL_ALERT' THEN 2")
        < sql.index("WHEN event_type='EXIT_SIGNAL' THEN 4")
    )
    assert call[2][:3] == ("formal-route", "official", "lineage-1")


@pytest.mark.asyncio
async def test_minute_bar_batch_uses_two_set_based_round_trips_and_returns_new_seals() -> None:
    connection = _FakeConnection(fetches=[[{"source_hash": "a" * 64}]])
    rows = [
        {
            "stock_code": f"{index:06d}",
            "bar_end": datetime(2026, 8, 31, 9, 39, tzinfo=BEIJING_TZ),
            "end_label": "09:39",
            "open": 10.0,
            "high": 10.1,
            "low": 9.9,
            "close": 10.0,
            "volume": 100.0,
            "amount": 1_000.0,
            "source_confirms_complete": True,
        }
        for index in range(3_000)
    ]

    sealed = await _repository(connection).record_minute_bars(rows)

    assert sealed == frozenset({"a" * 64})
    execute_calls = [call for call in connection.calls if call[0] == "execute"]
    fetch_calls = [call for call in connection.calls if call[0] == "fetch"]
    assert len(execute_calls) == 1
    assert len(fetch_calls) == 1
    assert "jsonb_to_recordset" in execute_calls[0][1]
    seal_sql = _compact_sql(fetch_calls[0][1])
    assert "receipt AS MATERIALIZED" in seal_sql
    assert "receipt.received_at > bar.bar_end" in seal_sql
    assert "SELECT source_hash FROM attempted UNION" in seal_sql
    assert len(json.loads(execute_calls[0][2][0])) == 3_000


@pytest.mark.asyncio
async def test_exit_commit_waits_for_authoritative_database_trigger_time() -> None:
    semantic = {"event_type": "EXIT_SIGNAL", "code": "000001"}
    commit = ExitCommit(
        exit_intent_id="exit-1",
        event_id="exit-event-1",
        model_leg_id="leg-1",
        signal_type="PLAN_1457",
        trigger_ts=datetime(2026, 8, 31, 14, 57, tzinfo=BEIJING_TZ),
        rule_actionable_from=datetime(2026, 8, 31, 14, 57, tzinfo=BEIJING_TZ),
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    connection = _FakeConnection(fetchrows=[None], fetchvals=[False])

    with pytest.raises(V20StateConflict, match="trigger_ts"):
        await _repository(connection).commit_exit(commit)

    assert not [call for call in connection.calls if call[0] == "execute"]


@pytest.mark.asyncio
async def test_exit_can_publish_after_trigger_but_before_future_actionable_time() -> None:
    semantic = {"event_type": "EXIT_SIGNAL", "code": "000001"}
    commit = ExitCommit(
        exit_intent_id="exit-d1",
        event_id="exit-event-d1",
        model_leg_id="leg-d1",
        signal_type="D1_CLOSE_CONFIRM_08",
        trigger_ts=datetime(2026, 8, 31, 14, 57, tzinfo=BEIJING_TZ),
        rule_actionable_from=datetime(2026, 9, 1, 9, 31, tzinfo=BEIJING_TZ),
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    connection = _FakeConnection(
        fetchrows=[
            None,
            {"evaluation_only": False, "seal_status": "SEALED"},
        ],
        fetchvals=[True],
    )

    assert await _repository(connection).commit_exit(commit) is True

    guard = [call for call in connection.calls if call[0] == "fetchval"][0]
    assert guard[2] == (commit.trigger_ts,)
    assert len([call for call in connection.calls if call[0] == "execute"]) == 2


@pytest.mark.asyncio
async def test_connect_closes_new_pool_when_migration_fails(monkeypatch) -> None:
    class _ClosablePool:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    pool = _ClosablePool()
    pool_kwargs = {}

    async def create_pool(**kwargs):
        pool_kwargs.update(kwargs)
        return pool

    async def fail_migration():
        raise RuntimeError("migration failed")

    ca_path = Path(certifi.where())
    repository = V20Repository(
        V20DatabaseConfig(
            ssl_root_cert=str(ca_path),
            ssl_root_cert_sha256=hashlib.sha256(ca_path.read_bytes()).hexdigest(),
        )
    )
    monkeypatch.setattr("src.data.database.v20_repository.asyncpg.create_pool", create_pool)
    monkeypatch.setattr(repository, "migrate", fail_migration)

    with pytest.raises(RuntimeError, match="migration failed"):
        await repository.connect()

    assert pool.closed is True
    assert repository._pool is None
    assert isinstance(pool_kwargs["ssl"], ssl.SSLContext)
    assert pool_kwargs["ssl"].verify_mode == ssl.CERT_REQUIRED
    assert pool_kwargs["ssl"].check_hostname is True
    assert pool_kwargs["timeout"] == 5.0
    assert pool_kwargs["command_timeout"] == 15.0
    assert pool_kwargs["server_settings"]["lock_timeout"] == "3000"


@pytest.mark.asyncio
async def test_connect_preserves_migration_failure_when_pool_cleanup_also_fails(
    monkeypatch,
) -> None:
    class _FailingClosePool:
        async def close(self) -> None:
            raise OSError("pool close failed")

    async def create_pool(**_kwargs):
        return _FailingClosePool()

    async def fail_migration() -> None:
        raise RuntimeError("migration failed")

    ca_path = Path(certifi.where())
    repository = V20Repository(
        V20DatabaseConfig(
            ssl_root_cert=str(ca_path),
            ssl_root_cert_sha256=hashlib.sha256(ca_path.read_bytes()).hexdigest(),
        )
    )
    monkeypatch.setattr("src.data.database.v20_repository.asyncpg.create_pool", create_pool)
    monkeypatch.setattr(repository, "migrate", fail_migration)

    with pytest.raises(RuntimeError, match="migration failed") as caught:
        await repository.connect()

    assert isinstance(caught.value.__cause__, OSError)
    assert str(caught.value.__cause__) == "pool close failed"
    assert repository._pool is None


def test_repository_pool_reserves_connections_for_independent_runtime_lanes() -> None:
    with pytest.raises(ValueError, match="max >= 7"):
        V20DatabaseConfig(pool_min_size=1, pool_max_size=6)


def test_v20_repository_rejects_tls_without_hostname_verification() -> None:
    with pytest.raises(ValueError, match="verify-full"):
        V20DatabaseConfig(ssl_mode="require")


@pytest.mark.parametrize("ssl_mode", ["allow", "prefer"])
def test_embedded_v20_repository_rejects_opportunistic_tls(ssl_mode: str) -> None:
    with pytest.raises(ValueError, match="unsupported"):
        V20DatabaseConfig(
            ssl_mode=ssl_mode,
            connection_profile="legacy_embedded",
        )


def test_dedicated_repository_cannot_borrow_another_components_pool() -> None:
    with pytest.raises(ValueError, match="only embedded V20"):
        V20Repository(V20DatabaseConfig(), shared_pool=object())


@pytest.mark.asyncio
async def test_borrowed_pool_migration_failure_preserves_pool_for_retry(
    monkeypatch,
) -> None:
    class _BorrowedPool:
        def __init__(self) -> None:
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    pool = _BorrowedPool()
    repository = V20Repository(
        V20DatabaseConfig(
            ssl_mode="require",
            connection_profile="legacy_embedded",
        ),
        shared_pool=pool,
    )
    attempts = 0

    async def migrate() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("migration failed once")

    async def forbidden_create_pool(**_kwargs):
        raise AssertionError("borrowed repository must not create another pool")

    monkeypatch.setattr(repository, "migrate", migrate)
    monkeypatch.setattr(
        "src.data.database.v20_repository.asyncpg.create_pool",
        forbidden_create_pool,
    )

    with pytest.raises(RuntimeError, match="failed once"):
        await repository.connect()

    assert repository.uses_shared_pool is True
    assert repository._pool is pool
    assert pool.close_calls == 0

    await repository.connect()

    assert attempts == 2
    assert repository._connection_ready is True


@pytest.mark.asyncio
async def test_borrowed_pool_close_releases_leader_without_closing_owner_pool() -> None:
    class _Leader:
        def __init__(self) -> None:
            self.unlock_keys: list[int] = []

        def is_closed(self) -> bool:
            return False

        async def fetchval(self, _sql: str, key: int) -> bool:
            self.unlock_keys.append(key)
            return True

    class _BorrowedPool:
        def __init__(self) -> None:
            self.released: list[object] = []
            self.close_calls = 0

        async def release(self, connection: object) -> None:
            self.released.append(connection)

        async def close(self) -> None:
            self.close_calls += 1

    leader = _Leader()
    pool = _BorrowedPool()
    repository = V20Repository(
        V20DatabaseConfig(
            ssl_mode="require",
            connection_profile="legacy_embedded",
        ),
        shared_pool=pool,
    )
    repository._connection_ready = True
    repository._leader_connection = leader
    repository._leader_key = 42
    repository._leader_scope = ("route", "stream", "lineage")

    await repository.close()

    assert leader.unlock_keys == [42]
    assert pool.released == [leader]
    assert pool.close_calls == 0
    assert repository._pool is pool
    assert repository._connection_ready is False


@pytest.mark.asyncio
async def test_concurrent_runtime_leader_probes_are_serialized_on_held_connection() -> None:
    class _LeaderConnection:
        inflight = 0
        max_inflight = 0

        def is_closed(self) -> bool:
            return False

        async def fetchval(self, sql: str) -> int:
            assert sql == "SELECT 1"
            self.inflight += 1
            self.max_inflight = max(self.max_inflight, self.inflight)
            await asyncio.sleep(0)
            self.inflight -= 1
            return 1

    connection = _LeaderConnection()
    repository = V20Repository(V20DatabaseConfig())
    repository._leader_connection = connection
    repository._leader_scope = ("formal-route", "official", "lineage-1")

    await asyncio.gather(
        repository.assert_runtime_leader(),
        repository.assert_runtime_leader(),
        repository.assert_runtime_leader(),
    )

    assert connection.max_inflight == 1


@pytest.mark.asyncio
async def test_runtime_leader_lock_excludes_new_lineage_on_same_public_route() -> None:
    held_keys: set[int] = set()

    class _LeaderConnection:
        def __init__(self) -> None:
            self.requested_key: int | None = None

        async def fetchval(self, _sql: str, key: int) -> bool:
            self.requested_key = key
            if key in held_keys:
                return False
            held_keys.add(key)
            return True

    class _LeaderPool:
        def __init__(self) -> None:
            self.connection = _LeaderConnection()

        async def acquire(self):
            return self.connection

        async def release(self, _connection) -> None:
            return None

    def repository() -> tuple[V20Repository, _LeaderPool]:
        instance = V20Repository(V20DatabaseConfig())
        pool = _LeaderPool()
        instance._pool = pool  # type: ignore[assignment]
        return instance, pool

    first, first_pool = repository()
    replacement, replacement_pool = repository()
    shadow, shadow_pool = repository()

    await first.acquire_runtime_leader(
        route_id="V20_FORMAL_FEISHU",
        official_stream_id="V20_FORMAL",
        lineage_id="LINEAGE_OLD",
    )
    with pytest.raises(V20StateConflict, match="another V20 worker"):
        await replacement.acquire_runtime_leader(
            route_id="V20_FORMAL_FEISHU",
            official_stream_id="V20_FORMAL_REPLACEMENT",
            lineage_id="LINEAGE_NEW",
        )
    await shadow.acquire_runtime_leader(
        route_id="V20_FORWARD_SHADOW_FEISHU",
        official_stream_id="V20_FORWARD_SHADOW",
        lineage_id="LINEAGE_SHADOW",
    )

    assert first_pool.connection.requested_key == replacement_pool.connection.requested_key
    assert shadow_pool.connection.requested_key != first_pool.connection.requested_key


@pytest.mark.asyncio
async def test_outbox_lease_cannot_consume_another_route_or_lineage_backlog() -> None:
    connection = _FakeConnection(fetches=[[]])

    assert (
        await _repository(connection).lease_outbox(
            worker_id="formal-worker",
            route_id="formal-route",
            **SCOPE,
        )
        == []
    )

    call = [item for item in connection.calls if item[0] == "fetch"][0]
    sql = _compact_sql(call[1])
    assert "route_id=$1 AND official_stream_id=$2 AND lineage_id=$3" in sql
    assert "semantic_json->>'delivery_priority_class'='LIVE_EXIT' THEN 1" in sql
    assert "semantic_json->>'delivery_priority_class'= 'RUNTIME_CRITICAL_ALERT' THEN 2" in sql
    assert "WHEN event_type='EXIT_SIGNAL' THEN 4" in sql
    assert "action_expiry_ts NULLS LAST,created_at,event_id" in sql
    assert "AS lease_db_ts" in sql
    assert call[2][:3] == ("formal-route", "official", "lineage-1")


@pytest.mark.asyncio
async def test_outbox_health_reports_only_the_current_delivery_scope() -> None:
    row = {
        "unsealed_n": 0,
        "pending_delivery_n": 1,
        "leased_n": 0,
        "seal_error_n": 0,
        "delivery_error_n": 1,
        "max_seal_attempt_count": 0,
        "max_delivery_attempt_count": 3,
        "last_seal_attempt_at": None,
        "oldest_unsent_at": None,
        "last_delivered_at": None,
    }
    connection = _FakeConnection(fetchrows=[row])

    health = await _repository(connection).get_outbox_health(
        route_id="formal-route",
        **SCOPE,
    )

    assert health["pending_delivery_n"] == 1
    assert health["seal_error_n"] == 0
    assert health["delivery_error_n"] == 1
    assert health["max_delivery_attempt_count"] == 3
    call = [item for item in connection.calls if item[0] == "fetchrow"][0]
    sql = _compact_sql(call[1])
    assert "WHERE route_id=$1 AND official_stream_id=$2 AND lineage_id=$3" in sql
    assert "delivery_status <> 'SENT' AND last_error IS NOT NULL" in sql


@pytest.mark.asyncio
async def test_outbox_seal_error_update_is_scoped_and_bounded() -> None:
    connection = _FakeConnection(executes=["UPDATE 1"])

    updated = await _repository(connection).record_outbox_seal_error(
        "event-1",
        "x" * 5_000,
        route_id="formal-route",
        **SCOPE,
    )

    assert updated is True
    call = [item for item in connection.calls if item[0] == "execute"][0]
    sql = _compact_sql(call[1])
    assert "event_id=$2 AND seal_status='PENDING'" in sql
    assert "route_id=$3 AND official_stream_id=$4 AND lineage_id=$5" in sql
    assert len(call[2][0]) == 4_000


@pytest.mark.asyncio
async def test_checkpoint_export_resets_target_predecessor_and_rewrites_state_batch_ids() -> None:
    source_state = _checkpoint_source_state()
    source_config = _checkpoint_source_config()
    source_row = {
        "bootstrap_mode": "EMPTY_FORWARD_SHADOW",
        "bootstrap_checkpoint_hash": None,
        "revision": 42,
        "state_hash": sha256_json(source_state),
        "state_json": canonical_json(source_state),
        "source_terminal_slot_id": "shadow-slot-20260831",
        "source_terminal_trade_date": date(2026, 8, 31),
        "source_terminal_slot_status": "COMPLETED",
        "source_deployment_mode": "forward_shadow",
        "source_config_hash": sha256_json(source_config),
        "source_config_json": canonical_json(source_config),
        "target_lineage_count": 0,
    }
    shadow_rows = [
        _checkpoint_shadow_row(
            f"rolling-{index}",
            "ROLLING7",
            date(2026, 8, 1) + timedelta(days=index),
        )
        for index in range(7)
    ]
    shadow_rows.extend(
        _checkpoint_shadow_row(
            f"health-{index}",
            "HEALTH",
            date(2026, 7, 1) + timedelta(days=index),
        )
        for index in range(3)
    )
    shadow_rows.append(
        _checkpoint_shadow_row(
            "active-gap",
            "ROLLING7",
            date(2026, 7, 10),
            status="COMPLETE_INVALID",
        )
    )
    pending = _checkpoint_shadow_row(
        "pending-health",
        "HEALTH",
        date(2026, 8, 31),
        status="COMPLETE_INVALID",
    )
    pending.update(
        status="PENDING",
        batch_return=None,
        reference_status="PENDING",
        reference_prices_json=None,
        reference_snapshot_hash=None,
    )
    shadow_rows.append(pending)
    unconsumed_health = _checkpoint_shadow_row(
        "health-after-watermark",
        "HEALTH",
        date(2026, 8, 20),
    )
    unconsumed_health["batch_return"] = -0.10
    shadow_rows.append(unconsumed_health)
    shadow_rows.append(
        _checkpoint_shadow_row(
            "rolling-invalid-after-cut",
            "ROLLING7",
            date(2026, 8, 21),
            status="COMPLETE_INVALID",
        )
    )
    connection = _FakeConnection(fetchrows=[source_row], fetches=[shadow_rows])

    checkpoint = await _repository(connection).export_bootstrap_checkpoint(
        source_official_stream_id="shadow-stream",
        source_lineage_id="shadow-lineage",
        target_official_stream_id="production-stream",
        target_lineage_id="production-lineage",
        as_of_trade_date=date(2026, 8, 31),
    )

    assert checkpoint["schema_version"] == "v20-bootstrap-checkpoint/v2"
    assert checkpoint["source_config_hash"] == sha256_json(source_config)
    assert checkpoint["source_state_semantics_hash"] == source_config["state_semantics_hash"]
    assert checkpoint["resolved_state_semantics_hash"] == source_config["state_semantics_hash"]
    assert checkpoint["official_state_hash"] == sha256_json(checkpoint["official_state"])
    target_state = checkpoint["official_state"]
    assert target_state["state_revision"] == 0
    assert target_state["last_terminal_slot_id"] is None
    assert target_state["last_terminal_trade_date"] is None
    mapping = checkpoint["batch_id_migration"]
    assert target_state["health"]["recent_valid"][0]["batch_id"] == mapping["health-0"]
    assert target_state["health"]["last_processed_key"][2] == mapping["health-2"]
    assert target_state["official_rolling_gaps"][0]["gap_id"] == mapping["active-gap"]
    assert (
        sum(
            row["kind"] == "ROLLING7" and row["status"] == "COMPLETE_VALID"
            for row in checkpoint["state_shadow_batches"]
        )
        == 7
    )
    assert any(
        row["source_batch_id"] == "pending-health" and row["status"] == "PENDING"
        for row in checkpoint["state_shadow_batches"]
    )
    assert any(
        row["source_batch_id"] == "health-after-watermark" and row["status"] == "COMPLETE_VALID"
        for row in checkpoint["state_shadow_batches"]
    )
    assert any(
        row["source_batch_id"] == "rolling-invalid-after-cut"
        and row["status"] == "COMPLETE_INVALID"
        for row in checkpoint["state_shadow_batches"]
    )
    exported_health = next(
        row
        for row in checkpoint["state_shadow_batches"]
        if row["source_batch_id"] == "health-after-watermark"
    )
    advanced = advance_health_state(
        deserialize_health_snapshot(target_state["health"]),
        [
            HealthObservation(
                batch_id=exported_health["batch_id"],
                signal_date=date.fromisoformat(exported_health["signal_date"]),
                t2_exit_date=date.fromisoformat(exported_health["t2_date"]),
                relative_return=float(exported_health["batch_return"]),
            )
        ],
    )
    assert advanced.status.value == "PAUSED_R0"
    shadow_fetch = [call for call in connection.calls if call[0] == "fetch"][0]
    compact_shadow_sql = _compact_sql(shadow_fetch[1])
    assert "shadow.kind='HEALTH'" in compact_shadow_sql
    assert "(shadow.t2_date,shadow.signal_date,shadow.batch_id) >" in compact_shadow_sql
    assert "shadow.kind='ROLLING7'" in compact_shadow_sql
    assert "NOT (shadow.batch_id=ANY($8::text[]))" in compact_shadow_sql
    assert shadow_fetch[2][-4:-1] == (
        date(2026, 7, 5),
        date(2026, 7, 3),
        "health-2",
    )
    assert shadow_fetch[2][-1] == ["active-gap"]
    assert connection.calls[0] == (
        "transaction",
        canonical_json({"isolation": "serializable", "readonly": True}),
        (),
    )


@pytest.mark.asyncio
async def test_checkpoint_export_resolves_audited_legacy_terminal_to_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = Path(__file__).resolve().parents[4]
    config = load_v20_runtime_config(project_root)
    current = json.loads(canonical_json(config.frozen_payload))
    legacy, legacy_hash = _legacy_runtime_payload(current)
    monkeypatch.setattr(
        runtime_config_module,
        "_AUDITED_LEGACY_STATE_SEMANTICS_HASHES",
        frozenset({legacy_hash}),
    )
    as_of = date(2026, 8, 31)
    source_state = genesis_state()
    source_state.update(
        state_revision=1,
        last_terminal_slot_id="legacy-terminal-slot",
        last_terminal_trade_date=as_of.isoformat(),
    )
    source_row = {
        "bootstrap_mode": "EMPTY_FORWARD_SHADOW",
        "bootstrap_checkpoint_hash": None,
        "revision": 1,
        "state_hash": sha256_json(source_state),
        "state_json": canonical_json(source_state),
        "source_terminal_slot_id": "legacy-terminal-slot",
        "source_terminal_trade_date": as_of,
        "source_terminal_slot_status": "FAILED",
        "source_deployment_mode": "forward_shadow",
        "source_config_id": sha256_json(legacy)[:24],
        "source_config_hash": sha256_json(legacy),
        "source_config_json": canonical_json(legacy),
        "target_lineage_count": 0,
    }
    rolling_rows = [
        _checkpoint_shadow_row(
            f"rolling-{index}",
            "ROLLING7",
            date(2026, 8, 1) + timedelta(days=index),
        )
        for index in range(7)
    ]
    legacy_config_hash = sha256_json(legacy)
    current_config_hash = sha256_json(current)
    evidence = {
        "schema_version": "v20-state-semantics-compatibility/v1",
        "lineage_id": "shadow-lineage",
        "official_stream_id": "shadow-stream",
        "legacy_state_semantics_hash": legacy_hash,
        "core_state_semantics_hash": config.state_semantics_hash,
        "evidence_config_id": legacy_config_hash[:24],
        "evidence_config_hash": legacy_config_hash,
        "accepted_config_id": current_config_hash[:24],
        "accepted_config_hash": current_config_hash,
        "dependency_diff": [],
    }
    compatibility_row = {
        "official_stream_id": "shadow-stream",
        "legacy_state_semantics_hash": legacy_hash,
        "core_state_semantics_hash": config.state_semantics_hash,
        "evidence_config_id": legacy_config_hash[:24],
        "evidence_config_hash": legacy_config_hash,
        "accepted_config_id": current_config_hash[:24],
        "accepted_config_hash": current_config_hash,
        "evidence_json": canonical_json(evidence),
        "evidence_hash": sha256_json(evidence),
    }
    connection = _FakeConnection(
        fetchrows=[source_row],
        fetches=[
            [compatibility_row],
            rolling_rows,
        ],
    )

    checkpoint = await _repository(connection).export_bootstrap_checkpoint(
        source_official_stream_id="shadow-stream",
        source_lineage_id="shadow-lineage",
        target_official_stream_id="production-stream",
        target_lineage_id="production-lineage",
        as_of_trade_date=as_of,
    )

    assert checkpoint["source_state_semantics_hash"] == legacy_hash
    assert checkpoint["resolved_state_semantics_hash"] == config.state_semantics_hash

    tampered = {**compatibility_row, "evidence_hash": "0" * 64}
    with pytest.raises(V20SemanticConflict, match="resolution evidence is invalid"):
        await _repository(
            _FakeConnection(fetchrows=[source_row], fetches=[[tampered]])
        ).export_bootstrap_checkpoint(
            source_official_stream_id="shadow-stream",
            source_lineage_id="shadow-lineage",
            target_official_stream_id="production-stream",
            target_lineage_id="production-lineage",
            as_of_trade_date=as_of,
        )


@pytest.mark.asyncio
async def test_checkpoint_export_rejects_incomplete_rolling_window() -> None:
    source_state = _checkpoint_source_state()
    source_config = _checkpoint_source_config()
    source_state["health"] = {
        "schema_version": "v20-health-snapshot/v1",
        "status": "WARMUP",
        "recovery_count": 0,
        "recent_valid": [],
        "last_processed_key": None,
    }
    source_state["official_rolling_gaps"] = []
    source_row = {
        "bootstrap_mode": "EMPTY_FORWARD_SHADOW",
        "bootstrap_checkpoint_hash": None,
        "revision": 42,
        "state_hash": sha256_json(source_state),
        "state_json": canonical_json(source_state),
        "source_terminal_slot_id": "shadow-slot-20260831",
        "source_terminal_trade_date": date(2026, 8, 31),
        "source_terminal_slot_status": "COMPLETED",
        "source_deployment_mode": "forward_shadow",
        "source_config_hash": sha256_json(source_config),
        "source_config_json": canonical_json(source_config),
        "target_lineage_count": 0,
    }
    only_six = [
        _checkpoint_shadow_row(
            f"rolling-{index}",
            "ROLLING7",
            date(2026, 8, 1) + timedelta(days=index),
        )
        for index in range(6)
    ]

    with pytest.raises(V20StateConflict, match="seven distinct valid rolling"):
        await _repository(
            _FakeConnection(fetchrows=[source_row], fetches=[only_six])
        ).export_bootstrap_checkpoint(
            source_official_stream_id="shadow-stream",
            source_lineage_id="shadow-lineage",
            target_official_stream_id="production-stream",
            target_lineage_id="production-lineage",
            as_of_trade_date=date(2026, 8, 31),
        )


@pytest.mark.asyncio
async def test_checkpoint_genesis_import_is_atomic_and_repeatable_by_source_mapping() -> None:
    source_state = _checkpoint_source_state()
    source_state["health"] = {
        "schema_version": "v20-health-snapshot/v1",
        "status": "WARMUP",
        "recovery_count": 0,
        "recent_valid": [],
        "last_processed_key": None,
    }
    source_state["official_rolling_gaps"] = []
    source_state["state_revision"] = 0
    source_state["last_terminal_slot_id"] = None
    source_state["last_terminal_trade_date"] = None
    state_hash = sha256_json(source_state)
    batches = []
    for index in range(7):
        source_batch_id = f"rolling-{index}"
        batches.append(
            {
                "batch_id": f"target-{index}",
                "source_batch_id": source_batch_id,
                "kind": "ROLLING7",
                "signal_date": (date(2026, 8, 1) + timedelta(days=index)).isoformat(),
                "t2_date": (date(2026, 8, 3) + timedelta(days=index)).isoformat(),
                "status": "COMPLETE_VALID",
                "payload": {"symbols": ["000001"]},
                "batch_return": 0.01,
                "reference_status": "LOCKED",
                "reference_prices": {"000001": 10.0},
                "reference_snapshot_hash": "b" * 64,
            }
        )
    registry = {
        "official_stream_id": "production-stream",
        "genesis_state_hash": state_hash,
        "state_semantics_hash": "e" * 64,
        "bootstrap_mode": "CHECKPOINT",
        "bootstrap_checkpoint_hash": "d" * 64,
        "bootstrap_predecessor_trade_date": date(2026, 8, 31),
    }
    state_row = {
        "revision": 0,
        "state_hash": state_hash,
        "state_json": canonical_json(source_state),
    }
    connection = _FakeConnection(
        fetchrows=[registry, state_row],
        fetches=[[] for _ in batches],
    )

    stored = await _repository(connection).ensure_genesis_state(
        "production-lineage",
        source_state,
        state_hash,
        official_stream_id="production-stream",
        state_semantics_hash="e" * 64,
        bootstrap_mode="CHECKPOINT",
        bootstrap_checkpoint_hash="d" * 64,
        bootstrap_predecessor_trade_date=date(2026, 8, 31),
        bootstrap_shadow_batches=batches,
    )

    assert stored.revision == 0
    shadow_inserts = [
        call
        for call in connection.calls
        if call[0] == "execute" and "INSERT INTO v20.shadow_batches" in call[1]
    ]
    assert len(shadow_inserts) == 7
    assert all(
        call[2][1:4]
        == ("production-stream", "production-lineage", batches[index]["source_batch_id"])
        for index, call in enumerate(shadow_inserts)
    )

    existing_rows = []
    for batch in batches:
        existing_rows.append(
            {
                "batch_id": batch["batch_id"],
                "decision_id": None,
                "official_stream_id": "production-stream",
                "lineage_id": "production-lineage",
                "source_batch_id": batch["source_batch_id"],
                "kind": batch["kind"],
                "signal_date": date.fromisoformat(batch["signal_date"]),
                "t2_date": date.fromisoformat(batch["t2_date"]),
                "status": batch["status"],
                "batch_json": canonical_json(batch["payload"]),
                "batch_return": batch["batch_return"],
                "reference_status": batch["reference_status"],
                "reference_prices_json": canonical_json(batch["reference_prices"]),
                "reference_snapshot_hash": batch["reference_snapshot_hash"],
            }
        )
    retry_connection = _FakeConnection(
        fetchrows=[registry, state_row],
        fetches=[[row] for row in existing_rows],
    )
    retried = await _repository(retry_connection).ensure_genesis_state(
        "production-lineage",
        source_state,
        state_hash,
        official_stream_id="production-stream",
        state_semantics_hash="e" * 64,
        bootstrap_mode="CHECKPOINT",
        bootstrap_checkpoint_hash="d" * 64,
        bootstrap_predecessor_trade_date=date(2026, 8, 31),
        bootstrap_shadow_batches=batches,
    )
    assert retried == stored
    assert not any(
        call[0] == "execute" and "INSERT INTO v20.shadow_batches" in call[1]
        for call in retry_connection.calls
    )


@pytest.mark.asyncio
async def test_checkpoint_genesis_rejects_duplicate_shadow_facts_before_db() -> None:
    state = {
        "schema_version": "v20-official-state/v1",
        "state_revision": 0,
        "health": {},
        "official_rolling_gaps": [],
        "last_terminal_slot_id": None,
        "last_terminal_trade_date": None,
    }
    batch = {
        "batch_id": "target-1",
        "source_batch_id": "source-1",
        "kind": "ROLLING7",
        "signal_date": "2026-08-01",
        "t2_date": "2026-08-03",
        "status": "COMPLETE_VALID",
        "payload": {},
        "batch_return": 0.01,
        "reference_status": "LOCKED",
        "reference_prices": {"000001": 10.0},
        "reference_snapshot_hash": "b" * 64,
    }
    connection = _FakeConnection()

    with pytest.raises(ValueError, match="batch_id values must be unique"):
        await _repository(connection).ensure_genesis_state(
            "production-lineage",
            state,
            sha256_json(state),
            official_stream_id="production-stream",
            state_semantics_hash="e" * 64,
            bootstrap_mode="CHECKPOINT",
            bootstrap_checkpoint_hash="d" * 64,
            bootstrap_predecessor_trade_date=date(2026, 8, 31),
            bootstrap_shadow_batches=[batch] * 7,
        )
    assert connection.calls == []


@pytest.mark.asyncio
async def test_empty_genesis_restart_keeps_first_persisted_predecessor_anchor() -> None:
    state = {
        "schema_version": "v20-official-state/v1",
        "state_revision": 0,
        "health": {
            "schema_version": "v20-health-snapshot/v1",
            "status": "WARMUP",
            "recovery_count": 0,
            "recent_valid": [],
            "last_processed_key": None,
        },
        "official_rolling_gaps": [],
        "last_terminal_slot_id": None,
        "last_terminal_trade_date": None,
    }
    state_hash = sha256_json(state)
    persisted_anchor = date(2026, 8, 30)
    registry = {
        "official_stream_id": "shadow-stream",
        "genesis_state_hash": state_hash,
        "state_semantics_hash": "e" * 64,
        "bootstrap_mode": "EMPTY_FORWARD_SHADOW",
        "bootstrap_checkpoint_hash": None,
        "bootstrap_predecessor_trade_date": persisted_anchor,
    }
    state_row = {
        "revision": 0,
        "state_hash": state_hash,
        "state_json": canonical_json(state),
    }
    connection = _FakeConnection(fetchrows=[registry, state_row])

    stored = await _repository(connection).ensure_genesis_state(
        "shadow-lineage",
        state,
        state_hash,
        official_stream_id="shadow-stream",
        state_semantics_hash="e" * 64,
        bootstrap_mode="EMPTY_FORWARD_SHADOW",
        bootstrap_checkpoint_hash=None,
        # A later process restart proposes a new inception candidate.  The
        # original persisted boundary remains authoritative.
        bootstrap_predecessor_trade_date=date(2026, 9, 1),
    )

    assert stored.revision == 0
    registry_insert = [
        call
        for call in connection.calls
        if call[0] == "execute" and "INSERT INTO v20.state_lineage_registry" in call[1]
    ][0]
    assert registry_insert[2][-1] == date(2026, 9, 1)
    assert not any(
        call[0] == "execute" and "SET bootstrap_predecessor_trade_date" in call[1]
        for call in connection.calls
    )

    incompatible_registry = {**registry, "state_semantics_hash": "f" * 64}
    with pytest.raises(V20SemanticConflict, match="different state semantics"):
        await _repository(_FakeConnection(fetchrows=[incompatible_registry])).ensure_genesis_state(
            "shadow-lineage",
            state,
            state_hash,
            official_stream_id="shadow-stream",
            state_semantics_hash="e" * 64,
            bootstrap_mode="EMPTY_FORWARD_SHADOW",
            bootstrap_checkpoint_hash=None,
            bootstrap_predecessor_trade_date=persisted_anchor,
        )


def test_schema_identifier_is_never_interpolated_unvalidated() -> None:
    with pytest.raises(ValueError, match="invalid PostgreSQL schema"):
        V20DatabaseConfig(schema="v20; DROP SCHEMA public")
    with pytest.raises(ValueError, match="invalid PostgreSQL schema"):
        migration_sql('v20".evil')


def test_all_point_in_time_receipt_seals_use_the_postgresql_clock() -> None:
    for method_name in (
        "record_mews_snapshot",
        "record_reminder_stop_ack",
        "record_daily_bar_snapshot",
        "record_minute_bars",
    ):
        source = inspect.getsource(getattr(V20Repository, method_name))
        assert "receipt_sealed_at=clock_timestamp()" in source or (
            "SELECT clock_timestamp() AS received_at" in source
            and "receipt_sealed_at=receipt.received_at" in source
        )
        assert "datetime.now" not in source


def test_operational_ledger_scope_is_explicit_and_keyword_required() -> None:
    methods = (
        "list_pending_shadow_batches",
        "list_pending_shadow_reference_batches",
        "update_shadow_references",
        "get_shadow_reference_status",
        "finalize_shadow_references_unavailable",
        "complete_shadow_batch",
        "load_recent_completed",
        "lock_reference_price",
        "list_pending_reference_legs",
        "finalize_pending_references_unavailable",
        "list_active_legs",
        "record_reminder_stop_ack",
        "enqueue_due_exit_reminders",
    )

    for method_name in methods:
        parameters = inspect.signature(getattr(V20Repository, method_name)).parameters
        for scope_name in SCOPE:
            parameter = parameters[scope_name]
            assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
            assert parameter.default is inspect.Parameter.empty


@pytest.mark.asyncio
async def test_operational_history_reads_filter_stream_and_lineage() -> None:
    connection = _FakeConnection(fetches=[[], [], [], [], [], []])
    repository = _repository(connection)

    await repository.list_pending_shadow_batches(date(2026, 9, 1), **SCOPE)
    await repository.list_pending_shadow_reference_batches(date(2026, 9, 1), **SCOPE)
    assert await repository.get_shadow_reference_status(date(2026, 9, 1), **SCOPE) is None
    await repository.load_recent_completed("HEALTH", date(2026, 9, 1), 7, **SCOPE)
    await repository.list_pending_reference_legs(date(2026, 9, 1), **SCOPE)
    await repository.list_active_legs(date(2026, 9, 1), **SCOPE)

    scoped_reads = [call for call in connection.calls if call[0] == "fetch"]
    assert len(scoped_reads) == 6
    for call in scoped_reads:
        _assert_call_is_scoped(call)


@pytest.mark.asyncio
async def test_operational_scope_cannot_be_empty() -> None:
    repository = _repository(_FakeConnection())

    with pytest.raises(ValueError, match="official_stream_id"):
        await repository.list_active_legs(
            date(2026, 9, 1), official_stream_id="", lineage_id="lineage-1"
        )
    with pytest.raises(ValueError, match="lineage_id"):
        await repository.list_active_legs(
            date(2026, 9, 1), official_stream_id="official", lineage_id=""
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "terminal_status"),
    [("NO_SIGNAL", "COMPLETED"), ("INPUT_INVALID", "FAILED")],
)
async def test_entry_action_drives_terminal_slot_status(action, terminal_status) -> None:
    commit = _entry(action)
    connection = _entry_connection(commit)

    await _repository(connection).commit_entry(commit)

    slot_update = [
        call for call in connection.calls if call[0] == "execute" and "decision_slots" in call[1]
    ][-1]
    assert slot_update[2][0] == terminal_status
    assert "terminal_receipt.terminal_at >= $8" in slot_update[1]
    assert "terminal_receipt.terminal_at < $7" in slot_update[1]
    assert "completed_at=terminal_receipt.terminal_at" in slot_update[1]
    assert slot_update[2][5] == action
    assert slot_update[2][6] == datetime(2026, 8, 31, 9, 40, tzinfo=BEIJING_TZ)
    assert slot_update[2][7] == commit.invalid_commit_not_before_ts


@pytest.mark.asyncio
async def test_database_clock_rejects_normal_entry_at_strict_0940_boundary() -> None:
    commit = _enter(1.0, 1)
    connection = _entry_connection(commit, extra_execute_count=2)
    connection.execute_results[-1] = "UPDATE 0"

    with pytest.raises(V20EntryDeadlineExceeded, match="normal-entry deadline"):
        await _repository(connection).commit_entry(commit)

    slot_update = [
        call for call in connection.calls if call[0] == "execute" and "decision_slots" in call[1]
    ][-1]
    assert "terminal_receipt.terminal_at < $7" in slot_update[1]
    assert slot_update[2][6] == datetime(2026, 8, 31, 9, 40, tzinfo=BEIJING_TZ)


@pytest.mark.asyncio
async def test_genesis_predecessor_reader_is_stream_and_lineage_scoped() -> None:
    predecessor = date(2026, 8, 28)
    connection = _FakeConnection(fetchrows=[{"bootstrap_predecessor_trade_date": predecessor}])

    loaded = await _repository(connection).load_bootstrap_predecessor_trade_date(**SCOPE)

    assert loaded == predecessor
    call = connection.calls[-1]
    assert "official_stream_id=$1 AND lineage_id=$2" in _compact_sql(call[1])
    assert call[2] == (SCOPE["official_stream_id"], SCOPE["lineage_id"])


@pytest.mark.asyncio
async def test_config_registration_is_idempotent_across_process_restart_days() -> None:
    payload = {"strategy": "V20", "mode": "forward_shadow"}
    connection = _FakeConnection(
        fetchrows=[
            {
                "config_hash": "a" * 64,
                "strategy_version": "V20",
                "deployment_mode": "forward_shadow",
                # The first process registered this immutable config yesterday.
                "effective_trade_date": date(2026, 8, 30),
                "config_json": canonical_json(payload),
            }
        ],
        executes=["OK"],
    )

    await _repository(connection).register_config(
        config_id="config-1",
        config_hash="a" * 64,
        strategy_version="V20",
        deployment_mode="forward_shadow",
        effective_trade_date=date(2026, 8, 31),
        payload=payload,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(("multiplier", "leg_count"), [(0.5, 2), (0.25, 4)])
async def test_entry_leg_weights_sum_to_final_multiplier(multiplier, leg_count) -> None:
    commit = _enter(multiplier, leg_count)
    connection = _entry_connection(commit, extra_execute_count=leg_count + 1)

    await _repository(connection).commit_entry(commit)

    leg_inserts = [
        call
        for call in connection.calls
        if call[0] == "execute" and "INSERT INTO v20.model_legs" in call[1]
    ]
    assert len(leg_inserts) == leg_count
    assert sum(call[2][5] for call in leg_inserts) == pytest.approx(multiplier)


@pytest.mark.asyncio
async def test_post_commit_late_seal_cannot_mutate_the_committed_model_batch() -> None:
    semantic = {"action": "ENTER", "final_multiplier": 1.0}
    expiry = datetime(2026, 8, 31, 9, 40, tzinfo=BEIJING_TZ)
    outbox_row = {
        "event_id": "event-1",
        "event_type": "ENTRY_DECISION",
        "route_id": "route-1",
        "official_stream_id": "official",
        "lineage_id": "lineage-1",
        "semantic_json": canonical_json(semantic),
        "semantic_content_hash": sha256_json(semantic),
        "payload_json": None,
        "payload_hash": None,
        "generated_at": None,
        "commit_marker": None,
        "action_expiry_ts": expiry,
        "delivery_status": "PENDING",
        "attempt_count": 0,
        "seal_status": "PENDING",
    }
    connection = _FakeConnection(
        fetchrows=[outbox_row, {"durable_at": expiry, "marker": 7}],
        executes=["UPDATE 1"],
    )

    record = await _repository(connection).seal_event(
        "event-1",
        lambda _record, durable_at, marker, on_time: {
            "generated_at": durable_at.isoformat(),
            "durable_commit_marker": marker,
            "timeliness_status": "ON_TIME" if on_time else "LATE",
        },
    )

    assert record.payload["timeliness_status"] == "LATE"
    assert not any(call[0] == "execute" and "model_batches" in call[1] for call in connection.calls)
    assert connection.calls[0][0] == "transaction"


@pytest.mark.asyncio
async def test_reference_lock_is_exactly_idempotent_and_profile_bound() -> None:
    snapshot_hash = "b" * 64
    pending = {
        "reference_status": "PENDING",
        "reference_price": None,
        "reference_snapshot_hash": None,
        "reference_profile_id": "profile-1",
    }
    connection = _FakeConnection(fetchrows=[pending], executes=["UPDATE 1"])
    repository = _repository(connection)

    assert await repository.lock_reference_price(
        "leg-1",
        **SCOPE,
        reference_profile_id="profile-1",
        price=10.5,
        snapshot_hash=snapshot_hash,
    )
    for call in connection.calls:
        if call[0] in {"fetchrow", "execute"}:
            _assert_call_is_scoped(call)

    same = dict(pending)
    same.update(
        reference_status="LOCKED",
        reference_price=10.5,
        reference_snapshot_hash=snapshot_hash,
    )
    assert not await _repository(_FakeConnection(fetchrows=[same])).lock_reference_price(
        "leg-1",
        **SCOPE,
        reference_profile_id="profile-1",
        price=10.5,
        snapshot_hash=snapshot_hash,
    )

    with pytest.raises(V20SemanticConflict, match="profile"):
        await _repository(_FakeConnection(fetchrows=[pending])).lock_reference_price(
            "leg-1",
            **SCOPE,
            reference_profile_id="wrong-profile",
            price=10.5,
            snapshot_hash=snapshot_hash,
        )


@pytest.mark.asyncio
async def test_shadow_reference_lock_updates_health_and_rolling_together() -> None:
    rows = [
        {
            "batch_id": "health-1",
            "kind": "HEALTH",
            "reference_status": "PENDING",
            "reference_prices_json": None,
            "reference_snapshot_hash": None,
        },
        {
            "batch_id": "rolling-1",
            "kind": "ROLLING7",
            "reference_status": "PENDING",
            "reference_prices_json": None,
            "reference_snapshot_hash": None,
        },
    ]
    connection = _FakeConnection(fetches=[rows], executes=["UPDATE 2"])

    updated = await _repository(connection).update_shadow_references(
        date(2026, 8, 31),
        **SCOPE,
        reference_prices={"000001": 10.0},
        snapshot_hash="c" * 64,
    )

    assert updated == ("health-1", "rolling-1")
    for call in connection.calls:
        if call[0] in {"fetch", "execute"}:
            _assert_call_is_scoped(call)
    update_call = [call for call in connection.calls if call[0] == "execute"][-1]
    assert update_call[2][2] == ["health-1", "rolling-1"]


@pytest.mark.asyncio
async def test_shadow_finalization_and_completion_updates_are_scoped() -> None:
    reference_rows = [
        {
            "batch_id": "health-1",
            "kind": "HEALTH",
            "reference_status": "PENDING",
            "reference_snapshot_hash": None,
        },
        {
            "batch_id": "rolling-1",
            "kind": "ROLLING7",
            "reference_status": "PENDING",
            "reference_snapshot_hash": None,
        },
    ]
    reference_connection = _FakeConnection(fetches=[reference_rows], executes=["UPDATE 2"])
    await _repository(reference_connection).finalize_shadow_references_unavailable(
        date(2026, 8, 31), **SCOPE, snapshot_hash="e" * 64
    )
    for call in reference_connection.calls:
        if call[0] in {"fetch", "execute"}:
            _assert_call_is_scoped(call)

    pending_batch = {
        "batch_id": "health-1",
        "decision_id": "decision-1",
        "kind": "HEALTH",
        "signal_date": date(2026, 8, 31),
        "t2_date": date(2026, 9, 2),
        "status": "PENDING",
        "batch_json": canonical_json({"codes": ["000001"]}),
        "batch_return": None,
        "reference_status": "LOCKED",
        "reference_prices_json": canonical_json({"000001": 10.0}),
        "reference_snapshot_hash": "e" * 64,
    }
    completion_connection = _FakeConnection(fetchrows=[pending_batch], executes=["UPDATE 1"])
    assert await _repository(completion_connection).complete_shadow_batch(
        "health-1",
        0.05,
        "COMPLETE_VALID",
        {"close_prices": {"000001": 10.5}},
        **SCOPE,
    )
    for call in completion_connection.calls:
        if call[0] in {"fetchrow", "execute"}:
            _assert_call_is_scoped(call)


@pytest.mark.asyncio
async def test_bulk_pending_reference_finalization_is_scoped() -> None:
    rows = [
        {
            "model_leg_id": "leg-1",
            "reference_status": "PENDING",
            "reference_snapshot_hash": None,
            "reference_profile_id": "profile-1",
        }
    ]
    connection = _FakeConnection(fetches=[rows], executes=["UPDATE 1"])

    updated = await _repository(connection).finalize_pending_references_unavailable(
        date(2026, 8, 31),
        **SCOPE,
        reference_profile_id="profile-1",
        snapshot_hash="f" * 64,
    )

    assert updated == ("leg-1",)
    for call in connection.calls:
        if call[0] in {"fetch", "execute"}:
            _assert_call_is_scoped(call)


@pytest.mark.asyncio
async def test_minute_reader_freezes_first_durable_revision_deterministically() -> None:
    bar_end = datetime(2026, 8, 31, 9, 39, tzinfo=BEIJING_TZ)
    payload_1 = {
        "stock_code": "000001",
        "bar_end": bar_end.isoformat(),
        "end_label": "09:39",
        "open": 10.0,
        "high": 10.1,
        "low": 9.9,
        "close": 10.0,
        "volume": 100.0,
        "amount": 1_000.0,
        "source_confirms_complete": True,
    }
    payload_2 = dict(payload_1, close=10.1)
    common = {
        "code": "000001",
        "bar_end": bar_end,
        "end_label": "09:39",
        "first_received_at": bar_end,
    }
    rows = [
        dict(common, source_hash=sha256_json(payload_1), bar_json=canonical_json(payload_1)),
        dict(common, source_hash=sha256_json(payload_2), bar_json=canonical_json(payload_2)),
    ]

    connection = _FakeConnection(fetches=[list(reversed(rows))])
    selected = await _repository(connection).list_minute_bars(
        "000001",
        trade_dates=[date(2026, 8, 31)],
        end_cutoff=bar_end,
    )

    assert len(selected) == 1
    assert selected[0].source_hash == min(
        sha256_json(payload_1),
        sha256_json(payload_2),
    )
    minute_fetch = [call for call in connection.calls if call[0] == "fetch"][0]
    minute_sql = _compact_sql(minute_fetch[1])
    assert "bar_end >= $4" in minute_sql
    assert "bar_end < $5" in minute_sql
    assert minute_fetch[2][3].isoformat() == "2026-08-31T00:00:00+08:00"
    assert minute_fetch[2][4].isoformat() == "2026-09-01T00:00:00+08:00"


@pytest.mark.asyncio
async def test_bulk_raw_minute_reader_uses_an_indexable_local_day_range() -> None:
    connection = _FakeConnection(fetches=[[]])

    records = await _repository(connection).list_raw_minute_bar_records(
        ["000001", "600000"],
        trade_date=date(2026, 8, 31),
        end_labels=["09:31", "09:39"],
    )

    assert records == []
    call = [item for item in connection.calls if item[0] == "fetch"][0]
    sql = _compact_sql(call[1])
    assert "bar_end >= $2" in sql
    assert "bar_end < $3" in sql
    assert call[2][1].isoformat() == "2026-08-31T00:00:00+08:00"
    assert call[2][2].isoformat() == "2026-09-01T00:00:00+08:00"


@pytest.mark.asyncio
async def test_minute_reader_skips_illegal_first_revision_then_freezes_first_legal() -> None:
    bar_end = datetime(2026, 8, 31, 9, 39, tzinfo=BEIJING_TZ)
    legal = {
        "stock_code": "000001",
        "bar_end": bar_end.isoformat(),
        "end_label": "09:39",
        "open": 10.0,
        "high": 10.1,
        "low": 9.9,
        "close": 10.0,
        "volume": 100.0,
        "amount": 1_000.0,
        "source_confirms_complete": True,
    }
    illegal = dict(legal, volume=0.0)
    corrected = dict(legal, close=10.05)
    rows = []
    for offset, payload in enumerate((illegal, legal, corrected)):
        rows.append(
            {
                "code": "000001",
                "bar_end": bar_end,
                "end_label": "09:39",
                "source_hash": sha256_json(payload),
                "bar_json": canonical_json(payload),
                "first_received_at": bar_end + timedelta(seconds=offset),
            }
        )

    selected = await _repository(_FakeConnection(fetches=[rows])).list_minute_bars(
        "000001",
        trade_dates=[bar_end.date()],
        end_cutoff=bar_end + timedelta(minutes=1),
    )

    assert len(selected) == 1
    assert selected[0].source_hash == sha256_json(legal)


@pytest.mark.parametrize("corrupt_first", [False, True])
@pytest.mark.asyncio
async def test_minute_reader_reports_corruption_but_preserves_legal_same_label_candidate(
    corrupt_first: bool,
) -> None:
    bar_end = datetime(2026, 8, 31, 9, 39, tzinfo=BEIJING_TZ)
    legal = {
        "stock_code": "000001",
        "bar_end": bar_end.isoformat(),
        "end_label": "09:39",
        "open": 10.0,
        "high": 10.1,
        "low": 8.7,
        "close": 8.8,
        "volume": 100.0,
        "amount": 1_000.0,
        "source_confirms_complete": True,
    }
    legal_row = {
        "code": "000001",
        "bar_end": bar_end,
        "end_label": "09:39",
        "source_hash": sha256_json(legal),
        "bar_json": canonical_json(legal),
        "first_received_at": bar_end + timedelta(seconds=2),
    }
    corrupt_row = {
        **legal_row,
        "source_hash": "f" * 64,
        "first_received_at": bar_end + timedelta(seconds=1),
    }
    rows = [corrupt_row, legal_row] if corrupt_first else [legal_row, corrupt_row]

    with pytest.raises(V20MinuteBarIntegrityConflict) as captured:
        await _repository(_FakeConnection(fetches=[rows])).list_minute_bars(
            "000001",
            trade_dates=[bar_end.date()],
            end_cutoff=bar_end + timedelta(minutes=1),
        )

    assert len(captured.value.partial_records) == 1
    assert captured.value.partial_records[0].source_hash == sha256_json(legal)
    assert captured.value.corrupt_labels == (("000001", bar_end.date(), "09:39"),)


@pytest.mark.asyncio
async def test_exit_scan_watermark_is_scoped_and_persisted() -> None:
    connection = _FakeConnection(
        fetchrows=[
            {"d1": date(2026, 8, 31), "d2": date(2026, 9, 1)},
            None,
        ],
        executes=["OK"],
    )

    created = await _repository(connection).record_exit_scan_watermark(
        "leg-1",
        trade_date=date(2026, 8, 31),
        scanned_through_label="14:57",
        source_hash="e" * 64,
        **SCOPE,
    )

    assert created
    scoped_lookup = [
        call
        for call in connection.calls
        if call[0] == "fetchrow" and "FROM v20.model_legs" in call[1]
    ][0]
    assert scoped_lookup[2][1:] == (
        SCOPE["official_stream_id"],
        SCOPE["lineage_id"],
    )
    assert any(
        call[0] == "execute" and "INSERT INTO v20.exit_scan_watermarks" in call[1]
        for call in connection.calls
    )


@pytest.mark.asyncio
async def test_exit_scan_watermark_reader_is_scoped() -> None:
    connection = _FakeConnection(
        fetches=[
            [
                {
                    "trade_date": date(2026, 8, 31),
                    "scanned_through_label": "14:57",
                }
            ]
        ]
    )

    result = await _repository(connection).get_exit_scan_watermarks("leg-1", **SCOPE)

    assert result == {date(2026, 8, 31): "14:57"}
    query = connection.calls[0]
    assert query[2] == (
        "leg-1",
        SCOPE["official_stream_id"],
        SCOPE["lineage_id"],
    )


def test_repository_factory_resolves_v20_environment_placeholders(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TEST_V20_HOST", "db.internal")
    monkeypatch.setenv("TEST_V20_PORT", "5544")
    config_path = tmp_path / "database-config.yaml"
    config_path.write_text(
        """
database:
  v20:
    host: "${TEST_V20_HOST:localhost}"
    port: "${TEST_V20_PORT:5432}"
    database: "${TEST_V20_NAME:ledger}"
    user: "${TEST_V20_USER:writer}"
    password: "${TEST_V20_PASSWORD}"
    schema: "v20_test"
    pool_min_size: 2
    pool_max_size: 7
""".strip(),
        encoding="utf-8",
    )

    repository = create_v20_repository_from_config(config_path)

    assert repository.config.host == "db.internal"
    assert repository.config.port == 5544
    assert repository.config.database == "ledger"
    assert repository.config.user == "writer"
    assert repository.config.password == ""
    assert repository.config.schema == "v20_test"
    assert repository.config.pool_min_size == 2
    assert repository.config.pool_max_size == 7
    assert repository.config.ssl_mode == "verify-full"
    assert repository.config.connection_profile == "dedicated"


def test_embedded_repository_reuses_db_identity_but_keeps_v20_schema_and_pool(
    tmp_path,
    monkeypatch,
) -> None:
    for name, value in {
        "TEST_DB_HOST": "legacy-db.internal",
        "TEST_DB_PORT": "5544",
        "TEST_DB_NAME": "strategy",
        "TEST_DB_USER": "main-writer",
        "TEST_DB_PASSWORD": "main-secret",
    }.items():
        monkeypatch.setenv(name, value)
    config_path = tmp_path / "database-config.yaml"
    config_path.write_text(
        """
database:
  trading:
    host: "${TEST_DB_HOST}"
    port: "${TEST_DB_PORT}"
    database: "${TEST_DB_NAME}"
    user: "${TEST_DB_USER}"
    password: "${TEST_DB_PASSWORD}"
    schema: "trading"
    pool_min_size: 2
    pool_max_size: 5
  fundamentals:
    host: "${TEST_DB_HOST}"
    port: "${TEST_DB_PORT}"
    database: "${TEST_DB_NAME}"
    user: "${TEST_DB_USER}"
    password: "${TEST_DB_PASSWORD}"
    schema: "public"
    ssl_mode: "require"
    ssl_root_cert: ""
    ssl_root_cert_sha256: ""
    connect_timeout_seconds: 6
    command_timeout_seconds: 16
""".strip(),
        encoding="utf-8",
    )

    repository = create_embedded_v20_repository_from_config(config_path)

    assert repository.config.host == "legacy-db.internal"
    assert repository.config.port == 5544
    assert repository.config.database == "strategy"
    assert repository.config.user == "main-writer"
    assert repository.config.password == "main-secret"
    assert repository.config.schema == "v20"
    assert (repository.config.pool_min_size, repository.config.pool_max_size) == (1, 8)
    # The legacy trading/state pools do not pass ``ssl`` to asyncpg.  The
    # fundamentals section has an independent TLS policy and must not affect
    # the embedded ledger connection.
    assert repository.config.ssl_mode == "disable"
    assert repository.config.connect_timeout_seconds == 5
    assert repository.config.command_timeout_seconds == 15
    assert repository.config.connection_profile == "legacy_embedded"


def test_embedded_repository_prefers_connected_shared_fundamentals_pool(
    tmp_path,
    monkeypatch,
) -> None:
    for name, value in {
        "TEST_DB_HOST": "legacy-db.internal",
        "TEST_DB_PORT": "5544",
        "TEST_DB_NAME": "strategy",
        "TEST_DB_USER": "main-writer",
        "TEST_DB_PASSWORD": "main-secret",
    }.items():
        monkeypatch.setenv(name, value)
    config_path = tmp_path / "database-config.yaml"
    config_path.write_text(
        """
database:
  trading:
    host: "${TEST_DB_HOST}"
    port: "${TEST_DB_PORT}"
    database: "${TEST_DB_NAME}"
    user: "${TEST_DB_USER}"
    password: "${TEST_DB_PASSWORD}"
    schema: "trading"
  fundamentals:
    host: "${TEST_DB_HOST}"
    port: "${TEST_DB_PORT}"
    database: "${TEST_DB_NAME}"
    user: "${TEST_DB_USER}"
    password: "${TEST_DB_PASSWORD}"
    schema: "public"
    connect_timeout_seconds: 6
    command_timeout_seconds: 16
""".strip(),
        encoding="utf-8",
    )
    shared_pool = object()

    repository = create_embedded_v20_repository_from_config(
        config_path,
        shared_pool=shared_pool,
    )

    assert repository.uses_shared_pool is True
    assert repository._pool is shared_pool
    assert repository.config.schema == "v20"
    assert repository.config.ssl_mode == "disable"
    assert repository.config.connect_timeout_seconds == 6
    assert repository.config.command_timeout_seconds == 16


@pytest.mark.asyncio
async def test_embedded_repository_passes_disabled_ssl_to_asyncpg(monkeypatch) -> None:
    captured: dict = {}

    async def create_pool(**kwargs):
        captured.update(kwargs)
        return object()

    repository = V20Repository(
        V20DatabaseConfig(
            ssl_mode="disable",
            connection_profile="legacy_embedded",
        )
    )
    monkeypatch.setattr("src.data.database.v20_repository.asyncpg.create_pool", create_pool)

    await repository.connect(migrate=False)

    assert captured["ssl"] is False


@pytest.mark.asyncio
async def test_stop_ack_is_idempotency_keyed_by_exit_event_and_consumer() -> None:
    ack_ts = datetime(2026, 9, 3, 10, 0, tzinfo=BEIJING_TZ)
    connection = _FakeConnection(
        fetchrows=[{"exit_intent_id": "exit-1"}, None],
        executes=["OK"],
    )

    created = await _repository(connection).record_reminder_stop_ack(
        "exit-event-1",
        "manual-desk",
        **SCOPE,
        ack_ts=ack_ts,
        auth_evidence_hash="d" * 64,
    )

    assert created
    scoped_queries = [call for call in connection.calls if call[0] == "fetchrow"]
    assert len(scoped_queries) == 2
    for call in scoped_queries:
        _assert_call_is_scoped(call)
    insert = [
        call
        for call in connection.calls
        if call[0] == "execute" and "reminder_stop_acks" in call[1]
    ][0]
    assert insert[2][1:4] == ("exit-event-1", "manual-desk", ack_ts)
    assert insert[2][0].startswith("ack:")


@pytest.mark.asyncio
async def test_due_reminder_id_is_deterministic_and_copies_exit_semantics() -> None:
    cutoff = datetime(2026, 9, 3, 9, 35, tzinfo=BEIJING_TZ)
    original = {"code": "000001", "stock_name": "平安银行", "event_type": "EXIT_SIGNAL"}
    due = {
        "exit_intent_id": "exit-1",
        "original_exit_event_id": "exit-event-1",
        "model_leg_id": "leg-1",
        "signal_type": "D2_PLAN_1457",
        "original_semantic_content_hash": sha256_json(original),
        "original_semantic_json": canonical_json(original),
        "original_payload_json": canonical_json({"actionable_from": "2026-09-02T14:57:01+08:00"}),
    }

    async def run_once():
        connection = _FakeConnection(fetches=[[due]], executes=["OK", "OK"])
        event_ids = await _repository(connection).enqueue_due_exit_reminders(
            date(2026, 9, 3),
            **SCOPE,
            cutoff=cutoff,
            route_id="exit-route",
        )
        return event_ids, connection

    first, connection = await run_once()
    second, _ = await run_once()

    assert first == second
    assert first[0].startswith("exit-reminder:")
    _assert_call_is_scoped([call for call in connection.calls if call[0] == "fetch"][0])
    reminder_insert = [
        call for call in connection.calls if call[0] == "execute" and "exit_reminders" in call[1]
    ][0]
    semantic = json.loads(reminder_insert[2][6])
    assert semantic["code"] == "000001"
    assert semantic["event_type"] == "EXIT_REMINDER"
    assert semantic["original_exit_event_id"] == "exit-event-1"
    assert semantic["actionable_from"] == "2026-09-02T14:57:01+08:00"


@pytest.mark.asyncio
async def test_load_selected_mews_verifies_complete_pit_snapshot() -> None:
    cutoff = datetime(2026, 9, 1, 9, 40, tzinfo=BEIJING_TZ)
    generated = datetime(2026, 8, 31, 16, 0, tzinfo=BEIJING_TZ)
    received = datetime(2026, 8, 31, 16, 1, tzinfo=BEIJING_TZ)
    payload = {
        "snapshot_id": "mews-1",
        "source_trade_date": "2026-08-31",
        "generated_at": generated.isoformat(),
        "fast_state": "DANGER",
        "model_version": "m1",
        "data_version": "d1",
    }
    row = {
        "model_leg_id": "leg-1",
        "d1": date(2026, 9, 1),
        "cutoff_ts": cutoff,
        "selection_reason": "ELIGIBLE",
        "selected_at": cutoff,
        "snapshot_id": "mews-1",
        "selected_fast_state": "DANGER",
        "source_trade_date": date(2026, 8, 31),
        "generated_at": generated,
        "received_at": received,
        "fast_state": "DANGER",
        "model_version": "m1",
        "data_version": "d1",
        "content_hash": sha256_json(payload),
        "snapshot_json": canonical_json(payload),
    }

    selected = await _repository(_FakeConnection(fetchrows=[row])).load_selected_mews_for_leg(
        "leg-1"
    )

    assert selected is not None
    assert selected.fast_state == "DANGER"
    assert selected.source_trade_date == date(2026, 8, 31)
    assert selected.payload == payload


@pytest.mark.asyncio
async def test_selected_mews_rejects_timestamp_equal_to_cutoff() -> None:
    cutoff = datetime(2026, 9, 1, 9, 40, tzinfo=BEIJING_TZ)
    payload = {
        "snapshot_id": "mews-boundary",
        "source_trade_date": "2026-08-31",
        "generated_at": cutoff.isoformat(),
        "fast_state": "DANGER",
        "model_version": "m1",
        "data_version": "d1",
    }
    row = {
        "model_leg_id": "leg-1",
        "d1": date(2026, 9, 1),
        "cutoff_ts": cutoff,
        "selection_reason": "ELIGIBLE",
        "selected_at": cutoff,
        "snapshot_id": "mews-boundary",
        "selected_fast_state": "DANGER",
        "source_trade_date": date(2026, 8, 31),
        "generated_at": cutoff,
        "received_at": cutoff,
        "fast_state": "DANGER",
        "model_version": "m1",
        "data_version": "d1",
        "content_hash": sha256_json(payload),
        "snapshot_json": canonical_json(payload),
    }

    with pytest.raises(V20SemanticConflict, match="violates PIT cutoff"):
        await _repository(_FakeConnection(fetchrows=[row])).load_selected_mews_for_leg("leg-1")


@pytest.mark.asyncio
async def test_mews_selection_sql_uses_strict_cutoff_boundary() -> None:
    cutoff = datetime(2026, 9, 1, 9, 40, tzinfo=BEIJING_TZ)
    connection = _FakeConnection(
        fetchrows=[{"d1": date(2026, 9, 1)}, None, None],
        executes=["OK"],
    )

    selected = await _repository(connection).select_mews_for_leg(
        "leg-1",
        d1=date(2026, 9, 1),
        cutoff=cutoff,
    )

    assert selected == (None, None, "MEWS_UNAVAILABLE_FALLBACK_12")
    candidate_sql = [
        call[1]
        for call in connection.calls
        if call[0] == "fetchrow" and "FROM v20.mews_snapshots" in call[1]
    ][0]
    assert "generated_at < $2" in candidate_sql
    assert "receipt_sealed_at < $2" in candidate_sql


@pytest.mark.asyncio
async def test_alert_outbox_is_exactly_idempotent() -> None:
    semantic = {"alert_code": "REFERENCE_UNAVAILABLE", "entity": "leg-1"}
    semantic_hash = sha256_json(semantic)
    created_connection = _FakeConnection(fetchrows=[None], executes=["OK"])
    assert await _repository(created_connection).enqueue_alert(
        "alert-1", "alert-route", semantic, semantic_hash, **SCOPE
    )

    existing = {
        "event_type": "DATA_ALERT",
        "route_id": "alert-route",
        "official_stream_id": SCOPE["official_stream_id"],
        "lineage_id": SCOPE["lineage_id"],
        "semantic_content_hash": semantic_hash,
        "semantic_json": canonical_json(semantic),
    }
    assert not await _repository(_FakeConnection(fetchrows=[existing])).enqueue_alert(
        "alert-1", "alert-route", semantic, semantic_hash, **SCOPE
    )

    conflicting = dict(existing, route_id="other-route")
    with pytest.raises(V20SemanticConflict, match="different semantics"):
        await _repository(_FakeConnection(fetchrows=[conflicting])).enqueue_alert(
            "alert-1", "alert-route", semantic, semantic_hash, **SCOPE
        )


@pytest.mark.asyncio
async def test_get_outbox_event_returns_complete_integrity_checked_record_in_exact_scope() -> None:
    row = _outbox_event_row()
    connection = _FakeConnection(fetchrows=[row])

    record = await _repository(connection).get_outbox_event(
        "manual-event",
        route_id="alert-route",
        **SCOPE,
    )

    assert record is not None
    assert record.event_id == "manual-event"
    assert record.event_type == "DATA_ALERT"
    assert record.route_id == "alert-route"
    assert record.official_stream_id == SCOPE["official_stream_id"]
    assert record.lineage_id == SCOPE["lineage_id"]
    assert record.semantic["alert_code"] == "MANUAL_TRIGGER_RECEIPT"
    assert record.payload is not None
    assert record.payload["message"] == "[V20] 人工触发验证（非交易指令）"
    assert record.commit_marker == 19
    assert record.action_expiry_ts is None

    call = [item for item in connection.calls if item[0] == "fetchrow"][0]
    assert "FROM v20.outbox_events WHERE event_id=$1" in _compact_sql(call[1])
    assert call[2] == ("manual-event",)


@pytest.mark.asyncio
async def test_get_outbox_event_rejects_an_event_owned_by_another_scope() -> None:
    connection = _FakeConnection(
        fetchrows=[_outbox_event_row(route_id="another-route")],
    )

    with pytest.raises(V20SemanticConflict, match="belongs to another V20 scope"):
        await _repository(connection).get_outbox_event(
            "manual-event",
            route_id="alert-route",
            **SCOPE,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("corrupt_field", "error"),
    [
        ("semantic_content_hash", "semantic hash mismatch"),
        ("payload_hash", "payload hash mismatch"),
    ],
)
async def test_get_outbox_event_fails_closed_on_corrupt_content(
    corrupt_field: str,
    error: str,
) -> None:
    row = _outbox_event_row()
    row[corrupt_field] = "f" * 64

    with pytest.raises(V20SemanticConflict, match=error):
        await _repository(_FakeConnection(fetchrows=[row])).get_outbox_event(
            "manual-event",
            route_id="alert-route",
            **SCOPE,
        )


@pytest.mark.asyncio
async def test_get_outbox_event_returns_none_only_when_event_id_is_absent() -> None:
    connection = _FakeConnection(fetchrows=[None])

    assert (
        await _repository(connection).get_outbox_event(
            "missing-event",
            route_id="alert-route",
            **SCOPE,
        )
        is None
    )


@pytest.mark.asyncio
async def test_entry_status_restores_semantic_and_snapshot_for_reference_lock() -> None:
    semantic = {"action": "ENTER", "required_codes": ["000001"]}
    snapshot = {"comparison_pool": ["000001", "600000"]}
    expiry = datetime(2026, 8, 31, 9, 40, tzinfo=BEIJING_TZ)
    row = {
        "official_stream_id": "official",
        "trade_date": date(2026, 8, 31),
        "slot_id": "slot-1",
        "slot_status": "COMPLETED",
        "slot_revision": 1,
        "strategy_version": "V20",
        "config_id": "config-1",
        "config_hash": "a" * 64,
        "lineage_id": "lineage-1",
        "decision_id": "decision-1",
        "event_id": "event-1",
        "action": "ENTER",
        "final_multiplier": 0.5,
        "semantic_content_hash": sha256_json(semantic),
        "semantic_json": canonical_json(semantic),
        "snapshot_id": "snapshot-1",
        "snapshot_hash": sha256_json(snapshot),
        "snapshot_json": canonical_json(snapshot),
        "action_expiry_ts": expiry,
    }

    status = await _repository(_FakeConnection(fetchrows=[row])).get_entry_status(
        "official", date(2026, 8, 31)
    )

    assert status is not None
    assert status.event_id == "event-1"
    assert status.slot_status == "COMPLETED"
    assert status.action == "ENTER"
    assert status.semantic == semantic
    assert status.snapshot == snapshot
