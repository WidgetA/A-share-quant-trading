import asyncio
import hashlib
import inspect
import json
import re
import ssl
from collections.abc import Sequence
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import certifi
import pytest

import src.data.database.v20_repository as v20_repository_module
import src.strategy.v20.runtime_config as runtime_config_module
from src.data.database.v20_repository import (
    EntryCommit,
    ExitCommit,
    ManualMonitorEnrollmentCommit,
    ModelBatchWrite,
    ModelLegWrite,
    V20DatabaseConfig,
    V20EntryDeadlineExceeded,
    V20MinuteBarIntegrityConflict,
    V20Repository,
    V20RepositoryError,
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
    declared_state_semantics_is_authentic,
    load_v20_runtime_config,
    state_semantics_payload_from_frozen_payload,
)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
SCOPE = {
    "official_stream_id": "official",
    "lineage_id": "lineage-1",
}
_PRE_SELECTION_V2_CORE = "ca8670343e13251287e7016ed2af1d26101f567b40f70705020733350e56dbbc"
_SELECTION_V3_CORE = "94464f2a2c4a9c33c5041aeb640f0510947a438f4d5ddd305cdfc0e5f1cfba4b"
_SELECTION_V3_CONFIG_HASH = "3659caae539d63ac0cf03d6d8d0ed20c9458a9401bca4df965efc96c363f5140"
_TYPE_CLEAN_CORE = "d402b32262be3f922a218c3fcd87c67c3943460b61103bdb9fae0e27104b8c41"
_PRE_SELECTION_V2_DEPENDENCIES = {
    "pyproject.toml": "b98d44b91a0509ff84f8bda06fdfaf5e7ed5d764465bf56fcd7920b438555ee0",
    "src/data/clients/tushare_realtime.py": (
        "03906a2b31f536335b82a6ed69fb13ac1febf8acc5494017b33e402b8760a97e"
    ),
    "src/strategy/strategies/v16_scanner.py": (
        "898fc16de390065419d0c62869de402176ec2ec0ad4aa340b24fbd22634d2b15"
    ),
    "src/strategy/v20/decision_engine.py": (
        "1105368da348c68b95cd9524d5e8236ab8a12a1a901ecf92053eea7d8eb32747"
    ),
    "src/strategy/v20/exit_policy.py": (
        "44919b2878d24b46708387229bf2810d314937d70cb94596bac2500c1c58b43e"
    ),
    "src/strategy/v20/models.py": (
        "f1a3fb0916b9ad56e99cf003951b845d8e8d26eec3bc96c982a581b08d3fe662"
    ),
    "src/web/v15_scan_service.py": (
        "73bd5ace0935ba235aff4b8a09e61b9ad355dc309378b555dbb3978e3ff508a8"
    ),
    "src/web/v20_service.py": ("8980fac4479611337dbac117b8265829ba20e1ed6c882b2f3f1718d3a9624051"),
    "src/data/database/v20_repository.py": (
        "ef6f26eec1a3ea40ae2fb9937d097307290c558b331f8633d4dde4b10e8f8dd7"
    ),
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
    payload = json.loads(canonical_json(load_v20_runtime_config(project_root).frozen_payload))
    payload["official_stream_id"] = "shadow-stream"
    payload["state_lineage_id"] = "shadow-lineage"
    return payload


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


def _pre_selection_v2_runtime_payload(current: dict[str, object]) -> dict[str, object]:
    historical = json.loads(canonical_json(current))
    dependencies = historical["strategy_dependency_hashes"]
    assert isinstance(dependencies, dict)
    dependencies.update(_PRE_SELECTION_V2_DEPENDENCIES)
    state_payload = state_semantics_payload_from_frozen_payload(historical)
    assert sha256_json(state_payload) == _PRE_SELECTION_V2_CORE
    historical["state_semantics_payload"] = state_payload
    historical["state_semantics_hash"] = _PRE_SELECTION_V2_CORE
    return historical


def _selection_v3_runtime_payload(_current: dict[str, object]) -> dict[str, object]:
    fixture_path = (
        Path(__file__).resolve().parents[4]
        / "tests/fixtures/v20/runtime_config_498f868_94464f2.json"
    )
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert fixture["source_commit"] == "498f868faa6b89b8c6639c3e506c6401e854410b"
    assert fixture["expected_config_hash"] == _SELECTION_V3_CONFIG_HASH
    assert fixture["expected_state_semantics_hash"] == _SELECTION_V3_CORE
    historical = fixture["payload"]
    assert isinstance(historical, dict)
    assert sha256_json(historical) == _SELECTION_V3_CONFIG_HASH
    assert historical["state_semantics_hash"] == _SELECTION_V3_CORE
    assert declared_state_semantics_is_authentic(historical)
    return historical


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
        "runtime_deployment_mode": payload["deployment_mode"],
        "config_json": canonical_json(payload),
    }


def _runtime_config_row(payload: dict[str, object]) -> dict[str, object]:
    config_hash = sha256_json(payload)
    return {
        "config_id": config_hash[:24],
        "config_hash": config_hash,
        "strategy_version": payload["strategy_version"],
        "deployment_mode": payload["deployment_mode"],
        "config_json": canonical_json(payload),
    }


def _compatibility_receipt_row(
    source: dict[str, object],
    target: dict[str, object],
    *,
    lineage_id: str,
    official_stream_id: str,
) -> dict[str, object]:
    source_hash = str(source["state_semantics_hash"])
    target_hash = str(target["state_semantics_hash"])
    source_hash_value = sha256_json(source)
    target_hash_value = sha256_json(target)
    source_dependencies = source["strategy_dependency_hashes"]
    target_dependencies = target["strategy_dependency_hashes"]
    assert isinstance(source_dependencies, dict)
    assert isinstance(target_dependencies, dict)
    dependency_diff = sorted(
        relative
        for relative in set(source_dependencies) | set(target_dependencies)
        if source_dependencies.get(relative) != target_dependencies.get(relative)
    )
    evidence = {
        "schema_version": "v20-state-semantics-compatibility/v1",
        "lineage_id": lineage_id,
        "official_stream_id": official_stream_id,
        "legacy_state_semantics_hash": source_hash,
        "core_state_semantics_hash": target_hash,
        "evidence_config_id": source_hash_value[:24],
        "evidence_config_hash": source_hash_value,
        "accepted_config_id": target_hash_value[:24],
        "accepted_config_hash": target_hash_value,
        "dependency_diff": dependency_diff,
    }
    return {
        "lineage_id": lineage_id,
        "official_stream_id": official_stream_id,
        "legacy_state_semantics_hash": source_hash,
        "core_state_semantics_hash": target_hash,
        "evidence_config_id": source_hash_value[:24],
        "evidence_config_hash": source_hash_value,
        "accepted_config_id": target_hash_value[:24],
        "accepted_config_hash": target_hash_value,
        "evidence_json": canonical_json(evidence),
        "evidence_hash": sha256_json(evidence),
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
    receipt = _compatibility_receipt_row(
        legacy,
        current,
        lineage_id=config.state_lineage_id,
        official_stream_id=config.official_stream_id,
    )
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
            _runtime_config_row(legacy),
            _runtime_config_row(legacy),
            _runtime_config_row(current),
            state_row,
        ],
        fetches=[[_config_slot_row(legacy)], [], [receipt]],
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
    insert_call = next(
        call
        for call in connection.calls
        if call[0] == "execute" and "INSERT INTO v20.state_semantics_compatibility" in call[1]
    )
    assert insert_call[2] == (
        config.state_lineage_id,
        config.official_stream_id,
        legacy_hash,
        config.state_semantics_hash,
        legacy_config_hash[:24],
        legacy_config_hash,
        current_hash[:24],
        current_hash,
        receipt["evidence_json"],
        receipt["evidence_hash"],
    )
    assert not any(
        call[0] == "execute" and "SET state_semantics_hash" in call[1] for call in connection.calls
    )
    assert not any(
        call[0] == "execute" and "UPDATE v20.official_state" in call[1] for call in connection.calls
    )


@pytest.mark.asyncio
async def test_compatibility_receipt_authentication_rejects_wrong_stream_row(
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
    receipt = _compatibility_receipt_row(
        legacy,
        current,
        lineage_id=config.state_lineage_id,
        official_stream_id=config.official_stream_id,
    )
    wrong_stream_receipt = {
        **receipt,
        "official_stream_id": "wrong-official-stream",
    }
    connection = _FakeConnection(
        fetchrows=[_runtime_config_row(legacy), _runtime_config_row(current)],
        fetches=[[receipt, wrong_stream_receipt]],
    )
    legacy_config_hash = sha256_json(legacy)

    with pytest.raises(V20SemanticConflict, match="row binding is invalid"):
        await _repository(connection)._authenticate_compatibility_receipts(
            connection,
            config.state_lineage_id,
            config.official_stream_id,
            current["strategy_version"],
            current["deployment_mode"],
            {(legacy_config_hash[:24], legacy_config_hash)},
        )

    receipt_fetch = connection.calls[0]
    assert receipt_fetch[2] == (config.state_lineage_id,)
    assert not any(call[0] == "execute" for call in connection.calls)


def _duplicate_semantic_edge_fixture() -> tuple[
    object,
    dict[str, object],
    dict[str, object],
    dict[str, object],
    str,
    list[dict[str, object]],
    list[dict[str, object]],
]:
    project_root = Path(__file__).resolve().parents[4]
    config = load_v20_runtime_config(project_root)
    current = json.loads(canonical_json(config.frozen_payload))
    fixture = json.loads(
        (project_root / "tests/fixtures/v20/runtime_config_4211cd0_b2ba54f.json").read_text(
            encoding="utf-8"
        )
    )
    assert fixture["source_commit"] == "4211cd0f6fa0da8afd7557d2cff8b0821df1dcc5"
    expected_config_hash = "06c00866a37aef72e0bda48a58a1a27efec79a38ec241c10312fb0c90a15cfc7"
    legacy_hash = "b2ba54f990cfe6b0e4b8f38c97e096a72205d78e34e484593eacaf5243ac2ce0"
    assert fixture["expected_config_hash"] == expected_config_hash
    assert fixture["expected_state_semantics_hash"] == legacy_hash
    legacy = fixture["payload"]
    assert isinstance(legacy, dict)
    assert sha256_json(legacy) == expected_config_hash
    assert legacy["state_semantics_hash"] == legacy_hash
    intermediate = _pre_selection_v2_runtime_payload(legacy)

    intermediates = []
    for index in range(9):
        duplicate = json.loads(canonical_json(intermediate))
        duplicate["route_id"] = f"duplicate-intermediate-route-{index}"
        assert duplicate["state_semantics_hash"] == _PRE_SELECTION_V2_CORE
        intermediates.append(duplicate)

    receipts = [
        _compatibility_receipt_row(
            legacy,
            duplicate,
            lineage_id=config.state_lineage_id,
            official_stream_id=config.official_stream_id,
        )
        for duplicate in intermediates
    ]
    return config, current, legacy, intermediate, legacy_hash, intermediates, receipts


def _runtime_config_rows_for_receipts(
    receipts: Sequence[dict[str, object]],
    payloads: Sequence[dict[str, object]],
) -> list[dict[str, object]]:
    rows_by_hash = {sha256_json(payload): _runtime_config_row(payload) for payload in payloads}
    rows = []
    for receipt in receipts:
        source_hash = receipt["evidence_config_hash"]
        target_hash = receipt["accepted_config_hash"]
        assert source_hash in rows_by_hash and target_hash in rows_by_hash
        rows.extend((rows_by_hash[source_hash], rows_by_hash[target_hash]))
    return rows


@pytest.mark.asyncio
async def test_genesis_deduplicates_real_multi_generation_semantic_edges() -> None:
    (
        config,
        current,
        legacy,
        _intermediate,
        legacy_hash,
        intermediates,
        duplicate_receipts,
    ) = _duplicate_semantic_edge_fixture()

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
    advanced_state = {
        **state,
        "state_revision": 1,
        "last_terminal_slot_id": "legacy-terminal-slot",
        "last_terminal_trade_date": "2026-08-31",
    }
    state_row = {
        "revision": 1,
        "state_hash": sha256_json(advanced_state),
        "state_json": canonical_json(advanced_state),
    }
    current_hash = sha256_json(current)
    terminal_intermediate = intermediates[-1]
    selection_v3 = _selection_v3_runtime_payload(current)
    terminal_slots = [
        _config_slot_row(legacy, slot_status="FAILED"),
        _config_slot_row(terminal_intermediate, slot_status="FAILED"),
        _config_slot_row(selection_v3, slot_status="FAILED"),
    ]
    selection_v3_receipt = _compatibility_receipt_row(
        terminal_intermediate,
        selection_v3,
        lineage_id=config.state_lineage_id,
        official_stream_id=config.official_stream_id,
    )
    terminal_receipt = _compatibility_receipt_row(
        selection_v3,
        current,
        lineage_id=config.state_lineage_id,
        official_stream_id=config.official_stream_id,
    )
    all_payloads = [legacy, *intermediates, selection_v3, current]
    persisted_receipts = [*duplicate_receipts, selection_v3_receipt]
    all_receipts = [*persisted_receipts, terminal_receipt]
    connection = _FakeConnection(
        fetchrows=[
            registry,
            _runtime_config_row(current),
            _runtime_config_row(legacy),
            _runtime_config_row(terminal_intermediate),
            _runtime_config_row(selection_v3),
            *_runtime_config_rows_for_receipts(persisted_receipts, all_payloads),
            *_runtime_config_rows_for_receipts(all_receipts, all_payloads),
            state_row,
        ],
        fetches=[terminal_slots, persisted_receipts, all_receipts],
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
        state_row["state_hash"],
        advanced_state,
    )
    compatibility_inserts = [
        call
        for call in connection.calls
        if call[0] == "execute" and "INSERT INTO v20.state_semantics_compatibility" in call[1]
    ]
    assert len(compatibility_inserts) == 1
    assert compatibility_inserts[0][2] == (
        config.state_lineage_id,
        config.official_stream_id,
        _SELECTION_V3_CORE,
        _TYPE_CLEAN_CORE,
        sha256_json(selection_v3)[:24],
        sha256_json(selection_v3),
        current_hash[:24],
        current_hash,
        terminal_receipt["evidence_json"],
        terminal_receipt["evidence_hash"],
    )
    assert (compatibility_inserts[0][2][2], compatibility_inserts[0][2][3]) == (
        _SELECTION_V3_CORE,
        _TYPE_CLEAN_CORE,
    )
    legacy_config_hash = sha256_json(legacy)
    expected_bindings = {
        (legacy_config_hash[:24], legacy_config_hash, legacy_hash),
        (
            sha256_json(terminal_intermediate)[:24],
            sha256_json(terminal_intermediate),
            _PRE_SELECTION_V2_CORE,
        ),
        (
            sha256_json(selection_v3)[:24],
            sha256_json(selection_v3),
            _SELECTION_V3_CORE,
        ),
    }
    assert {
        (binding.config_id, binding.config_hash, binding.state_semantics_hash)
        for binding in repository.compatible_entry_bindings
    } == expected_bindings
    assert not any(
        call[0] == "execute" and "SET state_semantics_hash" in call[1] for call in connection.calls
    )
    assert not any(
        call[0] == "execute" and "UPDATE v20.official_state" in call[1] for call in connection.calls
    )

    retry_connection = _FakeConnection(
        fetchrows=[
            registry,
            _runtime_config_row(current),
            _runtime_config_row(legacy),
            _runtime_config_row(terminal_intermediate),
            _runtime_config_row(selection_v3),
            *_runtime_config_rows_for_receipts(all_receipts, all_payloads),
            state_row,
        ],
        fetches=[terminal_slots, all_receipts],
    )
    retry_repository = _repository(retry_connection)
    retried = await retry_repository.ensure_genesis_state(
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
    assert retried == stored
    assert not any(
        call[0] == "execute" and "INSERT INTO v20.state_semantics_compatibility" in call[1]
        for call in retry_connection.calls
    )


@pytest.mark.asyncio
async def test_genesis_rejects_tampered_duplicate_semantic_edge_before_mutation() -> None:
    (
        config,
        current,
        legacy,
        _intermediate,
        legacy_hash,
        intermediates,
        duplicate_receipts,
    ) = _duplicate_semantic_edge_fixture()
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
    tampered_receipts = [dict(row) for row in duplicate_receipts]
    tampered_receipts[-1]["evidence_hash"] = "0" * 64
    terminal_intermediate = intermediates[-1]
    connection = _FakeConnection(
        fetchrows=[
            registry,
            _runtime_config_row(current),
            _runtime_config_row(legacy),
            _runtime_config_row(terminal_intermediate),
            *_runtime_config_rows_for_receipts(
                duplicate_receipts[:-1],
                [legacy, *intermediates, current],
            ),
        ],
        fetches=[
            [
                _config_slot_row(legacy, slot_status="FAILED"),
                _config_slot_row(terminal_intermediate, slot_status="FAILED"),
            ],
            tampered_receipts,
        ],
    )

    with pytest.raises(
        V20SemanticConflict,
        match="V20 compatibility receipt IDs or evidence are invalid",
    ):
        await _repository(connection).ensure_genesis_state(
            config.state_lineage_id,
            state,
            state_hash,
            official_stream_id=config.official_stream_id,
            state_semantics_hash=config.state_semantics_hash,
            current_config_id=sha256_json(current)[:24],
            current_config_hash=sha256_json(current),
            current_config_payload=current,
            bootstrap_mode="EMPTY_FORWARD_SHADOW",
            bootstrap_checkpoint_hash=None,
            bootstrap_predecessor_trade_date=date(2026, 8, 30),
        )

    assert not any(
        call[0] == "execute" and "INSERT INTO v20.state_semantics_compatibility" in call[1]
        for call in connection.calls
    )
    assert not any(
        call[0] == "execute"
        and ("SET state_semantics_hash" in call[1] or "UPDATE v20.official_state" in call[1])
        for call in connection.calls
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
        fetchrows=[
            registry,
            _runtime_config_row(current),
            _runtime_config_row(historical),
            state_row,
        ],
        fetches=[[_config_slot_row(historical)], []],
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
@pytest.mark.parametrize("operational_route_variant", [False, True])
async def test_genesis_upgrades_deployed_selection_v3_directly_without_v4_evidence(
    operational_route_variant: bool,
) -> None:
    project_root = Path(__file__).resolve().parents[4]
    config = load_v20_runtime_config(project_root)
    assert config.state_semantics_hash == _TYPE_CLEAN_CORE
    current = json.loads(canonical_json(config.frozen_payload))
    historical = _selection_v3_runtime_payload(current)
    if operational_route_variant:
        historical["route_id"] = "V20_SHADOW_FEISHU_OPERATIONAL_VARIANT"
        assert historical["state_semantics_hash"] == _SELECTION_V3_CORE
        assert declared_state_semantics_is_authentic(historical)
    state = {
        **genesis_state(),
        "state_revision": 7,
        "last_terminal_slot_id": "selection-v3-terminal",
        "last_terminal_trade_date": "2026-09-01",
    }
    state_hash = sha256_json(state)
    registry = {
        "official_stream_id": config.official_stream_id,
        "genesis_state_hash": sha256_json(genesis_state()),
        "state_semantics_hash": _SELECTION_V3_CORE,
        "bootstrap_mode": "EMPTY_FORWARD_SHADOW",
        "bootstrap_checkpoint_hash": None,
        "bootstrap_predecessor_trade_date": date(2026, 8, 30),
    }
    current_hash = sha256_json(current)
    historical_hash = sha256_json(historical)
    dependency_diff = sorted(
        relative
        for relative in set(historical["strategy_dependency_hashes"])
        | set(current["strategy_dependency_hashes"])
        if historical["strategy_dependency_hashes"].get(relative)
        != current["strategy_dependency_hashes"].get(relative)
    )
    assert dependency_diff
    receipt = _compatibility_receipt_row(
        historical,
        current,
        lineage_id=config.state_lineage_id,
        official_stream_id=config.official_stream_id,
    )
    connection = _FakeConnection(
        fetchrows=[
            registry,
            _runtime_config_row(current),
            _runtime_config_row(historical),
            _runtime_config_row(historical),
            _runtime_config_row(current),
            {"revision": 7, "state_hash": state_hash, "state_json": canonical_json(state)},
        ],
        fetches=[
            [_config_slot_row(historical, slot_status="COMPLETED")],
            [],
            [receipt],
        ],
    )
    repository = _repository(connection)

    stored = await repository.ensure_genesis_state(
        config.state_lineage_id,
        genesis_state(),
        sha256_json(genesis_state()),
        official_stream_id=config.official_stream_id,
        state_semantics_hash=config.state_semantics_hash,
        current_config_id=current_hash[:24],
        current_config_hash=current_hash,
        current_config_payload=current,
        bootstrap_mode="EMPTY_FORWARD_SHADOW",
        bootstrap_checkpoint_hash=None,
        bootstrap_predecessor_trade_date=date(2026, 8, 30),
    )

    assert (stored.revision, stored.state_hash, stored.payload) == (7, state_hash, state)
    insert_call = next(
        call
        for call in connection.calls
        if call[0] == "execute" and "INSERT INTO v20.state_semantics_compatibility" in call[1]
    )
    assert insert_call[2] == (
        config.state_lineage_id,
        config.official_stream_id,
        _SELECTION_V3_CORE,
        _TYPE_CLEAN_CORE,
        historical_hash[:24],
        historical_hash,
        current_hash[:24],
        current_hash,
        receipt["evidence_json"],
        receipt["evidence_hash"],
    )
    assert len(repository.compatible_entry_bindings) == 1
    assert next(iter(repository.compatible_entry_bindings)).state_semantics_hash == (
        _SELECTION_V3_CORE
    )
    assert not any(
        call[0] == "execute" and "UPDATE v20.official_state" in call[1] for call in connection.calls
    )
    assert not any(
        call[0] == "execute" and "SET state_semantics_hash" in call[1] for call in connection.calls
    )


@pytest.mark.asyncio
async def test_genesis_rejects_pre_selection_v2_direct_bypass_to_type_clean_current() -> None:
    project_root = Path(__file__).resolve().parents[4]
    config = load_v20_runtime_config(project_root)
    assert config.state_semantics_hash == _TYPE_CLEAN_CORE
    current = json.loads(canonical_json(config.frozen_payload))
    fixture = json.loads(
        (project_root / "tests/fixtures/v20/runtime_config_4211cd0_b2ba54f.json").read_text(
            encoding="utf-8"
        )
    )
    historical = _pre_selection_v2_runtime_payload(fixture["payload"])
    state = genesis_state()
    state_hash = sha256_json(state)
    registry = {
        "official_stream_id": config.official_stream_id,
        "genesis_state_hash": state_hash,
        "state_semantics_hash": historical["state_semantics_hash"],
        "bootstrap_mode": "EMPTY_FORWARD_SHADOW",
        "bootstrap_checkpoint_hash": None,
        "bootstrap_predecessor_trade_date": date(2026, 8, 30),
    }
    current_hash = sha256_json(current)
    connection = _FakeConnection(
        fetchrows=[
            registry,
            _runtime_config_row(current),
            _runtime_config_row(historical),
        ],
        fetches=[[_config_slot_row(historical, slot_status="COMPLETED")], []],
    )

    with pytest.raises(V20SemanticConflict, match="tail-to-current transition is unsupported"):
        await _repository(connection).ensure_genesis_state(
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

    assert not any(
        call[0] == "execute" and "INSERT INTO v20.state_semantics_compatibility" in call[1]
        for call in connection.calls
    )
    assert not any(
        call[0] == "execute"
        and ("SET state_semantics_hash" in call[1] or "UPDATE v20.official_state" in call[1])
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
        fetchrows=[
            registry,
            _runtime_config_row(current),
            _runtime_config_row(legacy),
        ],
        fetches=[
            [_config_slot_row(legacy, slot_status=slot_status)],
            [],
        ],
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
    assert re.search(r"(?:slot|shadow|batch|b)\.official_stream_id=", sql)
    assert re.search(r"(?:slot|shadow|batch|b)\.lineage_id=", sql)
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

    assert _compact_sql(standalone) in _compact_sql(migration_sql("v20"))
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
async def test_exit_rejects_a_sealed_official_source_not_bound_to_batch_decision() -> None:
    semantic = {"event_type": "EXIT_SIGNAL", "code": "000001"}
    commit = ExitCommit(
        exit_intent_id="exit-mismatched-source",
        event_id="exit-event-mismatched-source",
        model_leg_id="leg-mismatched-source",
        signal_type="PLAN_1457",
        trigger_ts=datetime(2026, 8, 31, 14, 57, tzinfo=BEIJING_TZ),
        rule_actionable_from=datetime(2026, 8, 31, 14, 57, tzinfo=BEIJING_TZ),
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
        route_id="formal-route",
        official_stream_id="official",
        lineage_id="lineage-1",
    )
    # PostgreSQL returns no leg when the sealed source is in scope but the
    # decision_id -> entry_decisions.event_id binding fails the SQL predicate.
    connection = _FakeConnection(fetchrows=[None, None], fetchvals=[True])

    with pytest.raises(V20RepositoryError, match="unknown model leg"):
        await _repository(connection).commit_exit(commit)

    authorization_sql = _compact_sql(
        next(
            call[1]
            for call in connection.calls
            if call[0] == "fetchrow" and "model_batches" in call[1]
        )
    )
    assert "origin_decision.decision_id=batch.decision_id" in authorization_sql
    assert "origin_decision.event_id=batch.source_event_id" in authorization_sql
    assert not any(call[0] == "execute" for call in connection.calls)


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
    assert "delivery_status='PENDING'" in sql
    assert "delivery_status='LEASED'" in sql
    assert "lease_until < clock_timestamp()" in sql
    assert "attempt.phase='STARTED'" in sql
    assert "delivery_status='DELIVERY_UNKNOWN'" not in sql.split("UPDATE")[0]
    assert "WHEN event_type='EXIT_SIGNAL' THEN 4" in sql
    assert "action_expiry_ts NULLS LAST,created_at,event_id" in sql
    assert "AS lease_db_ts" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "SET delivery_status='LEASED',lease_owner=$5" in sql
    assert call[2][:3] == ("formal-route", "official", "lineage-1")


def test_runtime_migration_is_mechanically_identical_to_001_plus_002() -> None:
    root = Path(__file__).resolve().parents[4]
    standalone_002 = (root / "migrations" / "v20" / "002_outbox_at_most_once.sql").read_text(
        encoding="utf-8"
    )
    assert migration_sql("v20").endswith("\n\n" + standalone_002 + "\n")
    declaration_pattern = re.compile(r"migration_checksum text := '([^']*)';")
    match = declaration_pattern.search(standalone_002)
    assert match is not None
    assert match.group(1) == v20_repository_module._outbox_002_contract_checksum(standalone_002)
    sql = migration_sql("v20")
    compact = _compact_sql(sql)
    assert "delivery_status IN ('PENDING','LEASED','DELIVERY_UNKNOWN','SENT')" in _compact_sql(sql)
    assert "pg_advisory_xact_lock" in sql
    assert compact.index("PERFORM pg_advisory_xact_lock") < compact.index(
        "CREATE TABLE IF NOT EXISTS v20.migration_receipts"
    )
    assert "CONSTRAINT v20_001_expected_status" in compact
    assert "CONSTRAINT v20_001_expected_lease" in compact
    assert "v20_001_expected_attempt_count" in compact
    assert "clean_status_name IS NULL OR clean_lease_name IS NULL" in compact
    assert "rejected_status_name IS NULL OR rejected_lease_name IS NULL" in compact
    assert "DROP CONSTRAINT %I, DROP CONSTRAINT %I" in compact
    assert "DROP CONSTRAINT clean_status_name" not in compact
    assert "DROP CONSTRAINT rejected_status_name" not in compact
    assert "v20_test.outbox_events" in migration_sql("v20_test")
    assert "_v20_index_reference" in compact
    assert "pg_my_temp_schema()" in compact
    assert "reference_keys.key_columns = actual_keys.key_columns" in compact
    assert "reference_index.indoption = actual_index.indoption" in compact
    assert "reference_index.indnkeyatts = actual_index.indnkeyatts" in compact
    assert "reference_class.relam = actual_class.relam" in compact
    assert "pg_get_expr(reference_index.indexprs" in compact
    assert "(reference_name,actual_name,actual_table)" in compact
    assert "nspname = 'pg_temp'" not in compact
    assert "ck_v20_delivery_attempt_variant_required_v2" in compact
    assert "ALTER COLUMN delivery_variant SET NOT NULL" not in compact
    assert compact.count("ON v20.delivery_attempts(event_id)") == 1
    assert "_v20_reference_started ON _v20_index_reference" in compact
    assert "trg_v20_delivery_attempt_identity_v2" in compact
    assert "trg_v20_outbox_attempt_count_v2" in compact
    assert "_v20_expected_index_pairs" in compact
    assert "_v20_reference_started" in compact
    assert "_v20_reference_unknown" in compact
    assert "reference_index.indclass = actual_index.indclass" in compact
    assert "reference_index.indcollation = actual_index.indcollation" in compact
    assert "must contain exactly four entries" in compact
    assert "uq_v20_delivery_attempt_started" in compact
    assert "idx_v20_outbox_unknown_v2" in compact
    assert "LIKE '\\_v20\\_expected\\_%'" not in compact
    assert "002_outbox_at_most_once" in sql
    assert "delivery_quarantine" in sql
    assert "uq_v20_delivery_attempt_started" in sql
    assert "ILIKE '%delivery_status%'" not in sql
    assert "DROP INDEX IF EXISTS idx_v20_outbox_ready;" not in sql
    assert "DROP INDEX IF EXISTS idx_v20_outbox_scope_ready;" not in sql


def test_runtime_rejects_drifted_002_checksum(monkeypatch, tmp_path) -> None:
    root = Path(__file__).resolve().parents[4]
    target = tmp_path / "migrations" / "v20"
    target.mkdir(parents=True)
    source = root / "migrations" / "v20" / "002_outbox_at_most_once.sql"
    drifted = source.read_text(encoding="utf-8").replace(
        "migration_checksum text := '",
        "migration_checksum text := '0",
        1,
    )
    (target / "002_outbox_at_most_once.sql").write_text(drifted, encoding="utf-8")
    monkeypatch.setattr(v20_repository_module, "_PROJECT_ROOT", tmp_path)

    with pytest.raises(RuntimeError, match="checksum does not match"):
        migration_sql("v20")


@pytest.mark.asyncio
async def test_migration_takes_schema_lock_before_any_migration_sql() -> None:
    connection = _FakeConnection()

    await _repository(connection).migrate()

    assert [item[0] for item in connection.calls[:4]] == [
        "transaction",
        "execute",
        "execute",
        "execute",
    ]
    assert "SET LOCAL lock_timeout" in connection.calls[1][1]
    assert "pg_advisory_xact_lock" in connection.calls[2][1]
    assert "CREATE SCHEMA IF NOT EXISTS" in connection.calls[3][1]


@pytest.mark.asyncio
async def test_begin_delivery_attempt_is_atomic_owned_and_audited() -> None:
    connection = _FakeConnection(
        fetchrows=[
            {
                "attempt_count": 2,
                "action_expiry_ts": None,
                "db_now": datetime(2026, 8, 31, 9, 0),
            },
            {"attempt_number": 3, "delivery_variant": "PRIMARY"},
            {"event_id": "event-1"},
        ]
    )

    attempt = await _repository(connection).begin_delivery_attempt(
        "event-1",
        worker_id="worker-1",
        route_id="formal-route",
        **SCOPE,
    )

    assert (attempt.attempt_number, attempt.delivery_variant) == (3, "PRIMARY")
    lock_sql = _compact_sql(connection.calls[1][1])
    assert "delivery_status='LEASED' AND lease_owner=$2" in lock_sql
    assert "FOR UPDATE" in lock_sql
    insert_call = connection.calls[2]
    insert_sql = _compact_sql(insert_call[1])
    assert "phase,worker_id,delivery_variant" in insert_sql
    assert "'STARTED'" in insert_sql
    assert insert_call[2][-2:] == ("worker-1", "PRIMARY")
    update_sql = _compact_sql(connection.calls[3][1])
    assert "delivery_status='DELIVERY_UNKNOWN',attempt_count=$1" in update_sql
    assert "lease_owner=$3" in update_sql
    assert "RETURNING event_id" in update_sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("db_now", "expiry", "relay_enforced", "expected_variant"),
    [
        (
            datetime(2026, 8, 31, 9, 39, 57),
            datetime(2026, 8, 31, 9, 40),
            False,
            "ACTIONABLE",
        ),
        (
            datetime(2026, 8, 31, 9, 39, 58),
            datetime(2026, 8, 31, 9, 40),
            False,
            "EXPIRED_NOTICE",
        ),
        (
            datetime(2026, 8, 31, 9, 40),
            datetime(2026, 8, 31, 9, 40),
            False,
            "EXPIRED_NOTICE",
        ),
        (
            datetime(2026, 8, 31, 9, 39, 59),
            datetime(2026, 8, 31, 9, 40),
            True,
            "RELAY_ENFORCED",
        ),
    ],
)
async def test_begin_delivery_variant_uses_database_clock_and_reserve(
    db_now, expiry, relay_enforced, expected_variant
) -> None:
    connection = _FakeConnection(
        fetchrows=[
            {"attempt_count": 0, "action_expiry_ts": expiry, "db_now": db_now},
            {"attempt_number": 1, "delivery_variant": expected_variant},
            {"event_id": "event-1"},
        ]
    )

    attempt = await _repository(connection).begin_delivery_attempt(
        "event-1",
        worker_id="worker-1",
        route_id="formal-route",
        action_reserve_seconds=2.0,
        relay_enforced=relay_enforced,
        **SCOPE,
    )

    assert attempt.delivery_variant == expected_variant


@pytest.mark.asyncio
async def test_defer_before_dispatch_does_not_create_attempt_or_increment() -> None:
    connection = _FakeConnection(fetchrows=[{"event_id": "event-1"}])

    await _repository(connection).defer_before_dispatch(
        "event-1",
        worker_id="worker-1",
        route_id="formal-route",
        error="pre-dispatch validation failed",
        retry_after_seconds=17,
        **SCOPE,
    )

    sql = _compact_sql(connection.calls[1][1])
    assert "delivery_status='PENDING'" in sql
    assert "delivery_attempts" not in sql
    assert "attempt_count" not in sql
    assert "delivery_status='LEASED' AND lease_owner=$4" in sql
    assert connection.calls[1][2][1] == 17


@pytest.mark.asyncio
async def test_complete_delivery_cas_requires_exact_attempt_and_owner() -> None:
    connection = _FakeConnection(fetchrows=[{"event_id": "event-1"}, {"event_id": "event-1"}])

    await _repository(connection).complete_delivery(
        "event-1",
        attempt_number=3,
        worker_id="worker-1",
        route_id="formal-route",
        outcome="DELIVERED",
        **SCOPE,
    )

    attempt_sql = _compact_sql(connection.calls[1][1])
    outbox_sql = _compact_sql(connection.calls[2][1])
    assert "attempt_number=$6 AND worker_id=$2" in attempt_sql
    assert "phase='STARTED'" in attempt_sql
    assert "completed_at=clock_timestamp()" in attempt_sql
    assert "delivery_status='DELIVERY_UNKNOWN'" in outbox_sql
    assert "attempt_count=$2 AND lease_owner=$3" in outbox_sql
    assert "delivery_status='SENT'" in outbox_sql
    assert "lease_owner=NULL,lease_until=NULL" in outbox_sql


@pytest.mark.asyncio
async def test_complete_delivery_safe_retry_and_unknown_outcomes() -> None:
    for outcome in ("SAFE_RETRY", "UNKNOWN"):
        connection = _FakeConnection(fetchrows=[{"event_id": "event-1"}, {"event_id": "event-1"}])
        await _repository(connection).complete_delivery(
            "event-1",
            attempt_number=1,
            worker_id="worker-1",
            route_id="formal-route",
            outcome=outcome,
            error=f"{outcome} reason",
            **SCOPE,
        )
        assert connection.calls[1][2][4] == outcome
        assert connection.calls[2][1].count("RETURNING event_id") == 1


@pytest.mark.asyncio
async def test_wrong_worker_or_stale_attempt_conflicts_before_outbox_update() -> None:
    connection = _FakeConnection(fetchrows=[None])

    with pytest.raises(V20StateConflict, match="missing, stale, or owned"):
        await _repository(connection).complete_delivery(
            "event-1",
            attempt_number=2,
            worker_id="other-worker",
            route_id="formal-route",
            outcome="UNKNOWN",
            error="wrong worker",
            **SCOPE,
        )
    assert [item[0] for item in connection.calls].count("execute") == 0
    assert [item[0] for item in connection.calls].count("fetchrow") == 1


@pytest.mark.asyncio
async def test_outbox_health_separates_unknown_and_dispatching_states() -> None:
    row = {
        "unsealed_n": 0,
        "pending_delivery_n": 1,
        "leased_n": 1,
        "dispatching_n": 1,
        "stale_started_n": 2,
        "terminal_unknown_n": 3,
        "unknown_n": 6,
        "seal_error_n": 0,
        "pending_delivery_error_n": 1,
        "unknown_error_n": 6,
        "max_seal_attempt_count": 0,
        "max_delivery_attempt_count": 3,
        "last_seal_attempt_at": None,
        "oldest_unsent_at": None,
        "oldest_unknown_at": datetime(2026, 8, 31, 9, 0),
        "last_delivered_at": None,
    }
    connection = _FakeConnection(fetchrows=[row])

    health = await _repository(connection).get_outbox_health(
        route_id="formal-route",
        **SCOPE,
    )

    assert health["pending_delivery_n"] == 1
    assert health["leased_n"] == 1
    assert health["dispatching_n"] == 1
    assert health["stale_started_n"] == 2
    assert health["terminal_unknown_n"] == 3
    assert health["unknown_n"] == 6
    assert health["delivery_error_n"] == 7
    assert health["oldest_unknown_at"] == "2026-08-31T09:00:00"
    sql = _compact_sql(connection.calls[0][1])
    assert "delivery_status='DELIVERY_UNKNOWN'" in sql
    assert "phase='STARTED'" in sql


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
        "source_registry_semantics_hash": source_config["state_semantics_hash"],
        "source_strategy_version": source_config["strategy_version"],
        "source_config_id": sha256_json(source_config)[:24],
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
    connection = _FakeConnection(
        fetchrows=[
            source_row,
            _runtime_config_row(source_config),
            _runtime_config_row(source_config),
        ],
        fetches=[
            [_config_slot_row(source_config, slot_status="COMPLETED")],
            [],
            shadow_rows,
        ],
    )

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
    shadow_fetch = next(
        call for call in connection.calls if call[0] == "fetch" and "WITH latest_rolling" in call[1]
    )
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
    current = _checkpoint_source_config()
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
        "source_registry_semantics_hash": legacy_hash,
        "source_strategy_version": legacy["strategy_version"],
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
    compatibility_row = _compatibility_receipt_row(
        legacy,
        current,
        lineage_id="shadow-lineage",
        official_stream_id="shadow-stream",
    )
    connection = _FakeConnection(
        fetchrows=[
            source_row,
            _runtime_config_row(legacy),
            _runtime_config_row(legacy),
            _runtime_config_row(legacy),
            _runtime_config_row(current),
        ],
        fetches=[
            [_config_slot_row(legacy, slot_status="FAILED")],
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
    with pytest.raises(
        V20SemanticConflict,
        match="V20 compatibility receipt IDs or evidence are invalid",
    ):
        await _repository(
            _FakeConnection(
                fetchrows=[
                    source_row,
                    _runtime_config_row(legacy),
                    _runtime_config_row(legacy),
                ],
                fetches=[[_config_slot_row(legacy, slot_status="FAILED")], [tampered]],
            )
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
        "source_registry_semantics_hash": source_config["state_semantics_hash"],
        "source_strategy_version": source_config["strategy_version"],
        "source_config_id": sha256_json(source_config)[:24],
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
            _FakeConnection(
                fetchrows=[
                    source_row,
                    _runtime_config_row(source_config),
                    _runtime_config_row(source_config),
                ],
                fetches=[
                    [_config_slot_row(source_config, slot_status="COMPLETED")],
                    [],
                    only_six,
                ],
            )
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
        "list_manual_monitor_batch_legs",
        "record_reminder_stop_ack",
        "enqueue_due_exit_reminders",
    )

    for method_name in methods:
        parameters = inspect.signature(getattr(V20Repository, method_name)).parameters
        for scope_name in SCOPE:
            parameter = parameters[scope_name]
            assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
            assert parameter.default is inspect.Parameter.empty


@pytest.mark.parametrize(
    ("method_name", "authorization_count"),
    [
        ("lock_reference_price", 2),
        ("list_pending_reference_legs", 1),
        ("finalize_pending_references_unavailable", 2),
        ("list_active_legs", 1),
        ("select_mews_for_leg", 1),
        ("load_selected_mews_for_leg", 1),
        ("commit_exit", 1),
        ("record_reminder_stop_ack", 2),
        ("enqueue_due_exit_reminders", 1),
        ("record_exit_scan_watermark", 1),
        ("get_exit_scan_watermarks", 1),
    ],
)
def test_every_model_leg_downstream_path_uses_dual_origin_authorization(
    method_name: str,
    authorization_count: int,
) -> None:
    source = inspect.getsource(getattr(V20Repository, method_name))

    assert source.count("_model_batch_authorization_sql") == authorization_count


@pytest.mark.asyncio
async def test_operational_history_reads_filter_stream_and_lineage() -> None:
    connection = _FakeConnection(fetches=[[], [], [], [], [], [], []])
    repository = _repository(connection)

    await repository.list_pending_shadow_batches(date(2026, 9, 1), **SCOPE)
    await repository.list_pending_shadow_reference_batches(date(2026, 9, 1), **SCOPE)
    assert await repository.get_shadow_reference_status(date(2026, 9, 1), **SCOPE) is None
    await repository.load_recent_completed("HEALTH", date(2026, 9, 1), 7, **SCOPE)
    await repository.list_pending_reference_legs(date(2026, 9, 1), **SCOPE)
    await repository.list_active_legs(date(2026, 9, 1), **SCOPE)
    await repository.list_manual_monitor_batch_legs("manual-batch", **SCOPE)

    scoped_reads = [call for call in connection.calls if call[0] == "fetch"]
    assert len(scoped_reads) == 7
    for call in scoped_reads:
        _assert_call_is_scoped(call)
    active_sql = _compact_sql(scoped_reads[-2][1])
    assert "origin_decision.decision_id=b.decision_id" in active_sql
    assert "origin_decision.event_id=b.source_event_id" in active_sql
    assert "origin_slot.official_stream_id=b.official_stream_id" in active_sql
    assert "enrollment.source_event_id=b.source_event_id" in active_sql


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
async def test_0910_mews_cache_verifies_database_receipt_before_cutoff() -> None:
    cutoff = datetime(2026, 9, 1, 9, 40, tzinfo=BEIJING_TZ)
    connection = _FakeConnection(fetchvals=[True])

    assert await _repository(connection).mews_snapshot_is_eligible(
        "mews-v2-2026-08-31-deadbeef",
        source_trade_date=date(2026, 8, 31),
        cutoff=cutoff,
    )

    call = connection.calls[0]
    assert call[0] == "fetchval"
    assert "generated_at < $3" in call[1]
    assert "receipt_sealed_at < $3" in call[1]
    assert call[2] == (
        "mews-v2-2026-08-31-deadbeef",
        date(2026, 8, 31),
        cutoff,
    )


@pytest.mark.asyncio
async def test_0910_mews_cache_can_be_restored_after_process_restart() -> None:
    cutoff = datetime(2026, 9, 1, 9, 40, tzinfo=BEIJING_TZ)
    connection = _FakeConnection(fetchvals=["mews-v2-2026-08-31-restored"])

    snapshot_id = await _repository(connection).find_eligible_mews_snapshot(
        source_trade_date=date(2026, 8, 31),
        cutoff=cutoff,
        availability_date=date(2026, 9, 1),
    )

    assert snapshot_id == "mews-v2-2026-08-31-restored"
    call = connection.calls[0]
    assert "source_trade_date=$1" in call[1]
    assert "generated_at < $2" in call[1]
    assert "receipt_sealed_at < $2" in call[1]
    assert "signal_available_date" in call[1]
    assert call[2] == (date(2026, 8, 31), cutoff, "2026-09-01")


@pytest.mark.asyncio
async def test_late_same_day_daily_snapshot_is_restorable_after_restart() -> None:
    cutoff = datetime(2026, 9, 1, 9, 40, tzinfo=BEIJING_TZ)
    connection = _FakeConnection(fetchvals=["mews-v2-2026-08-31-late"])

    snapshot_id = await _repository(connection).find_eligible_mews_snapshot(
        source_trade_date=date(2026, 8, 31),
        cutoff=cutoff,
        availability_date=date(2026, 9, 1),
    )

    assert snapshot_id == "mews-v2-2026-08-31-late"
    call = connection.calls[0]
    assert "snapshot_json->'evidence'->>'signal_available_date' = $3" in call[1]


@pytest.mark.asyncio
async def test_mews_selection_accepts_late_same_day_daily_snapshot() -> None:
    cutoff = datetime(2026, 9, 1, 9, 40, tzinfo=BEIJING_TZ)
    connection = _FakeConnection(
        fetchrows=[
            {"d1": date(2026, 9, 1)},
            None,
            {"snapshot_id": "mews-v2-2026-08-31-late", "fast_state": "DANGER", "on_time": False},
        ],
        executes=["OK"],
    )

    selected = await _repository(connection).select_mews_for_leg(
        "leg-1",
        d1=date(2026, 9, 1),
        cutoff=cutoff,
        late_source_trade_date=date(2026, 8, 31),
        late_availability_date=date(2026, 9, 1),
    )

    assert selected == (
        "mews-v2-2026-08-31-late",
        "DANGER",
        "ELIGIBLE_LATE_SAME_DAY",
    )
    candidate = [
        call
        for call in connection.calls
        if call[0] == "fetchrow" and "FROM v20.mews_snapshots" in call[1]
    ][0]
    assert "snapshot_json->'evidence'->>'signal_available_date' = $4" in candidate[1]
    assert candidate[2] == (
        date(2026, 9, 1),
        cutoff,
        date(2026, 8, 31),
        "2026-09-01",
    )
    insert = [call for call in connection.calls if call[0] == "execute"][0]
    assert insert[2] == (
        "leg-1",
        "mews-v2-2026-08-31-late",
        "DANGER",
        cutoff,
        "ELIGIBLE_LATE_SAME_DAY",
    )


@pytest.mark.asyncio
async def test_frozen_mews_selection_is_never_rewritten_by_the_late_window() -> None:
    cutoff = datetime(2026, 9, 1, 9, 40, tzinfo=BEIJING_TZ)
    connection = _FakeConnection(
        fetchrows=[
            {"d1": date(2026, 9, 1)},
            {
                "snapshot_id": None,
                "fast_state": None,
                "selection_reason": "MEWS_UNAVAILABLE_FALLBACK_12",
                "cutoff_ts": cutoff,
            },
        ],
    )

    selected = await _repository(connection).select_mews_for_leg(
        "leg-1",
        d1=date(2026, 9, 1),
        cutoff=cutoff,
        late_source_trade_date=date(2026, 8, 31),
        late_availability_date=date(2026, 9, 1),
    )

    assert selected == (None, None, "MEWS_UNAVAILABLE_FALLBACK_12")
    assert not [
        call
        for call in connection.calls
        if call[0] == "execute" and "leg_mews_selection" in call[1]
    ]


@pytest.mark.asyncio
async def test_load_selected_mews_accepts_late_same_day_daily_snapshot() -> None:
    cutoff = datetime(2026, 9, 1, 9, 40, tzinfo=BEIJING_TZ)
    generated = datetime(2026, 9, 1, 14, 4, tzinfo=BEIJING_TZ)
    received = datetime(2026, 9, 1, 14, 5, tzinfo=BEIJING_TZ)
    payload = {
        "snapshot_id": "mews-late",
        "source_trade_date": "2026-08-31",
        "generated_at": generated.isoformat(),
        "fast_state": "DANGER",
        "model_version": "mews_v2",
        "data_version": "d1",
        "evidence": {"signal_available_date": "2026-09-01"},
    }
    row = {
        "model_leg_id": "leg-1",
        "d1": date(2026, 9, 1),
        "cutoff_ts": cutoff,
        "selection_reason": "ELIGIBLE_LATE_SAME_DAY",
        "selected_at": received,
        "snapshot_id": "mews-late",
        "selected_fast_state": "DANGER",
        "source_trade_date": date(2026, 8, 31),
        "generated_at": generated,
        "received_at": received,
        "fast_state": "DANGER",
        "model_version": "mews_v2",
        "data_version": "d1",
        "content_hash": sha256_json(payload),
        "snapshot_json": canonical_json(payload),
    }

    selected = await _repository(_FakeConnection(fetchrows=[row])).load_selected_mews_for_leg(
        "leg-1"
    )

    assert selected is not None
    assert selected.snapshot_id == "mews-late"
    assert selected.fast_state == "DANGER"


@pytest.mark.asyncio
async def test_load_selected_mews_rejects_late_snapshot_with_wrong_availability() -> None:
    cutoff = datetime(2026, 9, 1, 9, 40, tzinfo=BEIJING_TZ)
    generated = datetime(2026, 9, 1, 14, 4, tzinfo=BEIJING_TZ)
    received = datetime(2026, 9, 1, 14, 5, tzinfo=BEIJING_TZ)
    payload = {
        "snapshot_id": "mews-stale",
        "source_trade_date": "2026-08-31",
        "generated_at": generated.isoformat(),
        "fast_state": "DANGER",
        "model_version": "mews_v2",
        "data_version": "d1",
        "evidence": {"signal_available_date": "2026-08-31"},
    }
    row = {
        "model_leg_id": "leg-1",
        "d1": date(2026, 9, 1),
        "cutoff_ts": cutoff,
        "selection_reason": "ELIGIBLE",
        "selected_at": received,
        "snapshot_id": "mews-stale",
        "selected_fast_state": "DANGER",
        "source_trade_date": date(2026, 8, 31),
        "generated_at": generated,
        "received_at": received,
        "fast_state": "DANGER",
        "model_version": "mews_v2",
        "data_version": "d1",
        "content_hash": sha256_json(payload),
        "snapshot_json": canonical_json(payload),
    }

    with pytest.raises(V20SemanticConflict, match="violates PIT cutoff"):
        await _repository(_FakeConnection(fetchrows=[row])).load_selected_mews_for_leg("leg-1")


async def test_local_mews_calculation_state_round_trips_with_integrity_check() -> None:
    state = {
        "schema": "v20-mews-incremental-state/v1",
        "model_version": "mews_v2",
        "state_date": "2026-08-31",
        "market_history": [],
        "security_states": {},
        "risk_state": "NORMAL",
        "clear_streak": 0,
    }
    row = {
        "state_date": date(2026, 8, 31),
        "model_version": "mews_v2",
        "content_hash": sha256_json(state),
        "state_json": canonical_json(state),
    }

    loaded = await _repository(_FakeConnection(fetchrows=[row])).load_mews_calculation_state()

    assert loaded == state


async def test_local_mews_calculation_state_is_monotonic_and_idempotent() -> None:
    state = {
        "schema": "v20-mews-incremental-state/v1",
        "model_version": "mews_v2",
        "state_date": "2026-08-31",
        "market_history": [],
        "security_states": {},
        "risk_state": "NORMAL",
        "clear_streak": 0,
    }
    digest = sha256_json(state)
    insert_connection = _FakeConnection(fetchrows=[None])

    assert await _repository(insert_connection).save_mews_calculation_state(state) == digest
    assert any(
        call[0] == "execute" and "INSERT INTO v20.mews_calculation_state" in call[1]
        for call in insert_connection.calls
    )

    idempotent_connection = _FakeConnection(
        fetchrows=[{"state_date": date(2026, 8, 31), "content_hash": digest}]
    )
    assert await _repository(idempotent_connection).save_mews_calculation_state(state) == digest
    assert not any(call[0] == "execute" for call in idempotent_connection.calls)

    regression_connection = _FakeConnection(
        fetchrows=[{"state_date": date(2026, 9, 1), "content_hash": "b" * 64}]
    )
    with pytest.raises(V20SemanticConflict, match="cannot regress"):
        await _repository(regression_connection).save_mews_calculation_state(state)


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


def _registered_same_core_config() -> tuple[dict[str, object], dict[str, object]]:
    dependencies = {
        relative: "1" * 64 for relative in runtime_config_module._STATE_SEMANTICS_DEPENDENCY_FILES
    }
    dependencies.update(
        {
            relative: next(iter(reviewed))
            for relative, reviewed in runtime_config_module._MIXED_STATE_SOURCE_CLASSES.items()
        }
    )
    payload: dict[str, object] = {
        "strategy_version": "V20",
        "official_stream_id": "official",
        "state_lineage_id": "lineage-1",
        "route_id": "alert-route",
        "deployment_mode": "forward_shadow",
        "timezone": "Asia/Shanghai",
        "return_profile_id": "ZERO_COST_GROSS_PRICE_RETURN_V1",
        "reference_profile_id": "CALENDAR_0940_OPEN_END_LABEL_0941_V1",
        "clock": {},
        "market_data": {},
        "policy": {},
        "g_manifest_sha256": "2" * 64,
        "strategy_dependency_hashes": dependencies,
    }
    state_payload = state_semantics_payload_from_frozen_payload(payload)
    payload["state_semantics_payload"] = state_payload
    payload["state_semantics_hash"] = sha256_json(state_payload)
    config_hash = sha256_json(payload)
    row: dict[str, object] = {
        "config_id": config_hash[:24],
        "config_hash": config_hash,
        "strategy_version": payload["strategy_version"],
        "deployment_mode": payload["deployment_mode"],
        "config_json": canonical_json(payload),
    }
    return payload, row


def _manual_monitor_fixture() -> tuple[
    ManualMonitorEnrollmentCommit,
    dict[str, object],
    dict[str, object],
]:
    signal_date = date(2026, 8, 31)
    d1 = date(2026, 9, 1)
    d2 = date(2026, 9, 2)
    source_event_id = "probe-event-1"
    official_entry_event_id = "official-entry-event-1"
    registered_payload, registered_row = _registered_same_core_config()
    source_config_hash = str(registered_row["config_hash"])
    state_semantics_hash = str(registered_payload["state_semantics_hash"])
    symbols = [
        {"rank": 1, "code": "000001", "name": "平安银行", "snapshot_price": 10.2},
        {"rank": 2, "code": "600000", "name": "浦发银行", "snapshot_price": 12.3},
    ]
    entry_render = {
        "strategy_version": "V20",
        "config_hash": source_config_hash,
        "state_semantics_hash": state_semantics_hash,
        "trade_date": signal_date.isoformat(),
        "action": "ENTER",
        "final_multiplier": 1.0,
        "reference_profile_id": "CALENDAR_0940_OPEN_END_LABEL_0941_V1",
        "symbols": symbols,
    }
    source_semantic = {
        "event_id": source_event_id,
        "alert_code": "MANUAL_0939_CHAIN_PROBE_RESULT",
        "probe_profile": "CURRENT_DEPLOYED_CODE_EXACT_0939_ENTRY_RENDER_V2",
        "probe_result": "PASS",
        "current_version_recomputed": True,
        "replay_reused": False,
        "visible_message_mode": "MANUAL_OPERATOR_RENDER",
        "strategy_version": "V20",
        "config_hash": source_config_hash,
        "state_semantics_hash": state_semantics_hash,
        "official_stream_id": "official",
        "state_lineage_id": "lineage-1",
        "official_entry_action": "INPUT_INVALID",
        "official_entry_event_id": official_entry_event_id,
        "official_entry_event_id_before": official_entry_event_id,
        "official_entry_event_id_after": official_entry_event_id,
        "v20_action": "ENTER",
        "replay_action": "ENTER",
        "official_state_changed": False,
        "orders_changed": False,
        "non_actionable": True,
        "retrospective_expired": True,
        "event_trade_date": signal_date.isoformat(),
        "final_multiplier": 1.0,
        "symbols": symbols,
        "entry_render_semantic": entry_render,
    }
    source_payload = {"message": "frozen automatic morning message"}
    enrollment_semantic = {
        "profile": "V20_MANUAL_MONITOR_ENROLLMENT_V1",
        "source_event_id": source_event_id,
        "official_entry_event_id": official_entry_event_id,
        "signal_date": signal_date.isoformat(),
        "d1": d1.isoformat(),
        "d2": d2.isoformat(),
    }
    batch = ModelBatchWrite(
        model_batch_id="manual-batch-1",
        multiplier=1.0,
        evaluation_only=False,
        reference_profile_id="CALENDAR_0940_OPEN_END_LABEL_0941_V1",
        legs=(
            ModelLegWrite(
                model_leg_id="manual-leg-1",
                code="000001",
                stock_name="平安银行",
                rank=1,
                relative_weight=0.5,
                d1=d1,
                d2=d2,
            ),
            ModelLegWrite(
                model_leg_id="manual-leg-2",
                code="600000",
                stock_name="浦发银行",
                rank=2,
                relative_weight=0.5,
                d1=d1,
                d2=d2,
            ),
        ),
    )
    commit = ManualMonitorEnrollmentCommit(
        enrollment_id="manual-enrollment-1",
        source_event_id=source_event_id,
        official_entry_event_id=official_entry_event_id,
        request_id="manual-request-1",
        route_id="alert-route",
        official_stream_id="official",
        lineage_id="lineage-1",
        strategy_version="V20",
        source_config_hash=source_config_hash,
        state_semantics_hash=state_semantics_hash,
        signal_date=signal_date,
        d1=d1,
        d2=d2,
        activation_cutoff_ts=datetime(2026, 9, 1, 9, 30, tzinfo=BEIJING_TZ),
        source_semantic_content_hash=sha256_json(source_semantic),
        source_payload_hash=sha256_json(source_payload),
        calendar_evidence_hash="c" * 64,
        enrollment_semantic=enrollment_semantic,
        enrollment_semantic_hash=sha256_json(enrollment_semantic),
        model_batch=batch,
    )
    source_row: dict[str, object] = {
        "event_id": source_event_id,
        "event_type": "DATA_ALERT",
        "route_id": "alert-route",
        "official_stream_id": "official",
        "lineage_id": "lineage-1",
        "semantic_content_hash": commit.source_semantic_content_hash,
        "semantic_json": canonical_json(source_semantic),
        "payload_hash": commit.source_payload_hash,
        "payload_json": canonical_json(source_payload),
        "seal_status": "SEALED",
    }
    return commit, source_row, registered_row


def _failed_official_entry_row(commit: ManualMonitorEnrollmentCommit) -> dict[str, object]:
    return {
        "event_id": commit.official_entry_event_id,
        "event_type": "ENTRY_DECISION",
        "route_id": commit.route_id,
        "official_stream_id": commit.official_stream_id,
        "lineage_id": commit.lineage_id,
        "seal_status": "SEALED",
        "action": "INPUT_INVALID",
        "trade_date": commit.signal_date,
        "slot_status": "FAILED",
        "slot_official_stream_id": commit.official_stream_id,
        "slot_lineage_id": commit.lineage_id,
    }


@pytest.mark.asyncio
async def test_registered_same_core_source_config_needs_no_terminal_slot() -> None:
    commit, _source, registered = _manual_monitor_fixture()
    connection = _FakeConnection(fetchrows=[registered])

    assert (
        await _repository(connection).is_registered_source_config_compatible(
            commit.source_config_hash,
            strategy_version=commit.strategy_version,
            state_semantics_hash=commit.state_semantics_hash,
            official_stream_id=commit.official_stream_id,
            lineage_id=commit.lineage_id,
            route_id=commit.route_id,
        )
        is True
    )
    sql = _compact_sql(connection.calls[0][1])
    assert "FROM v20.runtime_configs" in sql
    assert "decision_slots" not in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["unregistered", "different_core"])
async def test_registered_source_config_rejects_missing_or_different_core(
    failure: str,
) -> None:
    commit, _source, registered = _manual_monitor_fixture()
    row = None if failure == "unregistered" else registered
    state_semantics_hash = "f" * 64 if failure == "different_core" else commit.state_semantics_hash

    assert (
        await _repository(_FakeConnection(fetchrows=[row])).is_registered_source_config_compatible(
            commit.source_config_hash,
            strategy_version=commit.strategy_version,
            state_semantics_hash=state_semantics_hash,
            official_stream_id=commit.official_stream_id,
            lineage_id=commit.lineage_id,
            route_id=commit.route_id,
        )
        is False
    )


@pytest.mark.asyncio
async def test_manual_monitor_enrollment_is_atomic_explicit_and_non_official() -> None:
    commit, source, registered = _manual_monitor_fixture()
    connection = _FakeConnection(
        fetchrows=[source, _failed_official_entry_row(commit), registered, None],
        fetchvals=[True],
        executes=["OK", "OK", "OK", "INSERT 0 1"],
    )

    assert await _repository(connection).enroll_manual_monitor(commit) is True

    writes = [call for call in connection.calls if call[0] == "execute"]
    assert len(writes) == 4
    assert "'MANUAL_MONITOR'" in writes[0][1]
    assert writes[0][2][1] == commit.source_event_id
    assert sum("model_legs" in sql for _kind, sql, _args in writes) == 2
    assert "manual_monitor_enrollments" in writes[-1][1]
    forbidden = ("official_state", "decision_slots", "entry_decisions", "shadow_batches")
    assert not any(any(name in sql for name in forbidden) for _kind, sql, _args in writes)


@pytest.mark.asyncio
async def test_manual_monitor_same_source_retry_is_idempotent_even_after_cutoff() -> None:
    commit, source, registered = _manual_monitor_fixture()
    first_connection = _FakeConnection(
        fetchrows=[source, _failed_official_entry_row(commit), registered, None],
        fetchvals=[True],
        executes=["OK", "OK", "OK", "INSERT 0 1"],
    )
    assert await _repository(first_connection).enroll_manual_monitor(commit) is True
    enrollment_insert = next(
        call
        for call in first_connection.calls
        if call[0] == "execute" and "manual_monitor_enrollments" in call[1]
    )
    fingerprint = enrollment_insert[2][15]
    existing = {
        "source_event_id": commit.source_event_id,
        "enrollment_id": commit.enrollment_id,
        "model_batch_id": commit.model_batch.model_batch_id,
        "enrollment_fingerprint": fingerprint,
    }
    retry_connection = _FakeConnection(
        fetchrows=[source, _failed_official_entry_row(commit), registered, existing]
    )

    assert (
        await _repository(retry_connection).enroll_manual_monitor(
            replace(commit, request_id="a-different-retry-key")
        )
        is False
    )
    assert not any(call[0] == "fetchval" for call in retry_connection.calls)
    assert not any(call[0] == "execute" for call in retry_connection.calls)


@pytest.mark.asyncio
async def test_manual_monitor_same_request_cannot_select_a_different_source() -> None:
    commit, source, registered = _manual_monitor_fixture()
    request_collision = {
        "source_event_id": "another-source",
        "enrollment_id": "another-enrollment",
        "model_batch_id": "another-batch",
        "enrollment_fingerprint": "f" * 64,
    }
    connection = _FakeConnection(
        fetchrows=[source, _failed_official_entry_row(commit), registered, request_collision]
    )

    with pytest.raises(V20SemanticConflict, match="ID collision"):
        await _repository(connection).enroll_manual_monitor(commit)

    assert not any(call[0] == "execute" for call in connection.calls)


@pytest.mark.asyncio
async def test_different_probe_for_same_failed_slot_cannot_create_a_second_batch() -> None:
    commit, source, registered = _manual_monitor_fixture()
    semantic = json.loads(str(source["semantic_json"]))
    semantic["event_id"] = "probe-event-2"
    source = {
        **source,
        "event_id": "probe-event-2",
        "semantic_json": canonical_json(semantic),
        "semantic_content_hash": sha256_json(semantic),
    }
    enrollment_semantic = {
        **commit.enrollment_semantic,
        "source_event_id": "probe-event-2",
    }
    second_batch = replace(
        commit.model_batch,
        model_batch_id="manual-batch-2",
        legs=tuple(
            replace(leg, model_leg_id=f"manual-leg-2-{leg.rank}") for leg in commit.model_batch.legs
        ),
    )
    second = replace(
        commit,
        enrollment_id="manual-enrollment-2",
        source_event_id="probe-event-2",
        request_id="manual-request-2",
        source_semantic_content_hash=sha256_json(semantic),
        enrollment_semantic=enrollment_semantic,
        enrollment_semantic_hash=sha256_json(enrollment_semantic),
        model_batch=second_batch,
    )
    existing = {
        "source_event_id": commit.source_event_id,
        "official_entry_event_id": commit.official_entry_event_id,
        "enrollment_id": commit.enrollment_id,
        "model_batch_id": commit.model_batch.model_batch_id,
        "enrollment_fingerprint": "f" * 64,
    }
    connection = _FakeConnection(
        fetchrows=[source, _failed_official_entry_row(second), registered, existing]
    )

    with pytest.raises(V20SemanticConflict, match="ID collision"):
        await _repository(connection).enroll_manual_monitor(second)

    lock_call = next(
        call for call in connection.calls if call[0] == "fetchrow" and "entry_decisions" in call[1]
    )
    assert "FOR UPDATE OF official,decision,slot" in _compact_sql(lock_call[1])
    collision_call = connection.calls[-1]
    assert "official_entry_event_id=$7" in _compact_sql(collision_call[1])
    assert not any(call[0] == "execute" for call in connection.calls)


@pytest.mark.asyncio
async def test_manual_monitor_rejects_ineligible_probe_without_writes() -> None:
    commit, source, _registered = _manual_monitor_fixture()
    semantic = json.loads(str(source["semantic_json"]))
    semantic["v20_action"] = "BLOCK"
    source["semantic_json"] = canonical_json(semantic)
    source["semantic_content_hash"] = sha256_json(semantic)
    commit = replace(commit, source_semantic_content_hash=sha256_json(semantic))
    connection = _FakeConnection(fetchrows=[source, None])

    with pytest.raises(V20SemanticConflict, match="eligible current ENTER probe"):
        await _repository(connection).enroll_manual_monitor(commit)

    assert not any(call[0] == "execute" for call in connection.calls)


@pytest.mark.asyncio
async def test_manual_monitor_transaction_rejects_unregistered_source_config() -> None:
    commit, source, _registered = _manual_monitor_fixture()
    connection = _FakeConnection(fetchrows=[source, _failed_official_entry_row(commit), None])

    with pytest.raises(V20SemanticConflict, match="registered same-core binding"):
        await _repository(connection).enroll_manual_monitor(commit)

    assert any(call[0] == "fetchrow" and "runtime_configs" in call[1] for call in connection.calls)
    assert not any(call[0] == "execute" for call in connection.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("probe_profile", "OLDER_PROFILE"),
        ("config_hash", "d" * 64),
        ("state_semantics_hash", "e" * 64),
    ],
)
async def test_manual_monitor_revalidates_probe_profile_and_frozen_bindings(
    field_name: str,
    replacement: str,
) -> None:
    commit, source, _registered = _manual_monitor_fixture()
    semantic = json.loads(str(source["semantic_json"]))
    semantic[field_name] = replacement
    source["semantic_json"] = canonical_json(semantic)
    source["semantic_content_hash"] = sha256_json(semantic)
    commit = replace(commit, source_semantic_content_hash=sha256_json(semantic))
    connection = _FakeConnection(fetchrows=[source, None])

    with pytest.raises(V20SemanticConflict, match="eligible current ENTER probe"):
        await _repository(connection).enroll_manual_monitor(commit)

    assert not any(call[0] == "execute" for call in connection.calls)


@pytest.mark.asyncio
async def test_manual_monitor_first_enrollment_fails_closed_at_d1_cutoff() -> None:
    commit, source, registered = _manual_monitor_fixture()
    connection = _FakeConnection(
        fetchrows=[source, _failed_official_entry_row(commit), registered, None],
        fetchvals=[False],
    )

    with pytest.raises(V20StateConflict, match="closed at D1 09:30"):
        await _repository(connection).enroll_manual_monitor(commit)

    assert not any(call[0] == "execute" for call in connection.calls)


@pytest.mark.asyncio
async def test_manual_monitor_final_insert_closes_a_cutoff_race() -> None:
    commit, source, registered = _manual_monitor_fixture()
    connection = _FakeConnection(
        fetchrows=[source, _failed_official_entry_row(commit), registered, None],
        fetchvals=[True],
        executes=["OK", "OK", "OK", "INSERT 0 0"],
    )

    with pytest.raises(V20StateConflict, match="crossed the D1 09:30"):
        await _repository(connection).enroll_manual_monitor(commit)

    final_insert = [call for call in connection.calls if call[0] == "execute"][-1]
    assert "WHERE clock_timestamp() < $11::timestamptz" in _compact_sql(final_insert[1])


@pytest.mark.asyncio
async def test_get_manual_monitor_enrollment_validates_scope_and_hash() -> None:
    commit, _source, _registered = _manual_monitor_fixture()
    created_at = datetime(2026, 9, 1, 0, 20, tzinfo=BEIJING_TZ)
    row = {
        "enrollment_id": commit.enrollment_id,
        "source_event_id": commit.source_event_id,
        "official_entry_event_id": commit.official_entry_event_id,
        "model_batch_id": commit.model_batch.model_batch_id,
        "request_id": commit.request_id,
        "signal_date": commit.signal_date,
        "d1": commit.d1,
        "d2": commit.d2,
        "activation_cutoff_ts": commit.activation_cutoff_ts,
        "source_semantic_content_hash": commit.source_semantic_content_hash,
        "source_payload_hash": commit.source_payload_hash,
        "calendar_evidence_hash": commit.calendar_evidence_hash,
        "enrollment_semantic_hash": commit.enrollment_semantic_hash,
        "enrollment_json": canonical_json(commit.enrollment_semantic),
        "created_at": created_at,
    }

    record = await _repository(_FakeConnection(fetchrows=[row])).get_manual_monitor_enrollment(
        commit.source_event_id,
        **SCOPE,
    )

    assert record is not None
    assert record.model_batch_id == commit.model_batch.model_batch_id
    assert record.official_entry_event_id == commit.official_entry_event_id
    assert record.semantic == commit.enrollment_semantic
    assert record.created_at == created_at


@pytest.mark.asyncio
async def test_manual_monitor_batch_read_includes_exited_legs_and_is_strictly_bound() -> None:
    commit, _source, _registered = _manual_monitor_fixture()
    leg = commit.model_batch.legs[0]
    row = {
        "model_leg_id": leg.model_leg_id,
        "model_batch_id": commit.model_batch.model_batch_id,
        "decision_id": None,
        "origin_kind": "MANUAL_MONITOR",
        "source_event_id": commit.source_event_id,
        "signal_date": commit.signal_date,
        "code": leg.code,
        "stock_name": leg.stock_name,
        "rank": leg.rank,
        "relative_weight": leg.relative_weight,
        "d1": leg.d1,
        "d2": leg.d2,
        "reference_status": "LOCKED",
        "reference_price": 10.0,
        "reference_snapshot_hash": "d" * 64,
        "evaluation_only": False,
        "mews_snapshot_id": None,
        "mews_fast_state": None,
        "exit_intent_id": "already-exited-intent",
    }
    connection = _FakeConnection(fetches=[[row]])

    records = await _repository(connection).list_manual_monitor_batch_legs(
        commit.model_batch.model_batch_id,
        **SCOPE,
    )

    assert len(records) == 1
    assert records[0].exit_intent_id == "already-exited-intent"
    sql = _compact_sql(connection.calls[0][1])
    assert "JOIN v20.manual_monitor_enrollments AS enrollment" in sql
    assert "source.event_type='DATA_ALERT' AND source.seal_status='SEALED'" in sql
    assert "exit_intent_id IS NULL" not in sql
