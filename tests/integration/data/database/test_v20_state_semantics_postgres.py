from __future__ import annotations

import json
import os
import uuid
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import asyncpg
import pytest

from src.data.database.v20_repository import (
    V20DatabaseConfig,
    V20Repository,
    V20SemanticConflict,
    canonical_json,
    sha256_json,
)
from src.strategy.v20.decision_engine import genesis_state
from src.strategy.v20.runtime_config import (
    declared_state_semantics_is_authentic,
    load_v20_runtime_config,
    state_semantics_payload_from_frozen_payload,
)

DSN = os.environ.get("V20_TEST_POSTGRES_DSN", "")

SAFE_PREFIX = "v20_test_state_"
pytestmark = pytest.mark.postgres

B2_CORE = "b2ba54f990cfe6b0e4b8f38c97e096a72205d78e34e484593eacaf5243ac2ce0"
B2_CONFIG_HASH = "06c00866a37aef72e0bda48a58a1a27efec79a38ec241c10312fb0c90a15cfc7"
B2_SOURCE_COMMIT = "4211cd0f6fa0da8afd7557d2cff8b0821df1dcc5"
CA867_CORE = "ca8670343e13251287e7016ed2af1d26101f567b40f70705020733350e56dbbc"
SELECTION_V3_CORE = "94464f2a2c4a9c33c5041aeb640f0510947a438f4d5ddd305cdfc0e5f1cfba4b"
SELECTION_V3_CONFIG_HASH = "3659caae539d63ac0cf03d6d8d0ed20c9458a9401bca4df965efc96c363f5140"
SELECTION_V4_CORE = "0f5fbbd1e6cce372217373023f3681cf09100b870e7c4d187e2ebc7ebd1a8290"
TYPE_CLEAN_CORE = "d402b32262be3f922a218c3fcd87c67c3943460b61103bdb9fae0e27104b8c41"

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


async def _drop_schema(pool: asyncpg.Pool, schema: str) -> None:
    assert schema.startswith(SAFE_PREFIX)
    async with pool.acquire() as connection:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture
async def repository():
    if not DSN:
        pytest.fail(
            "V20_TEST_POSTGRES_DSN is required for real PostgreSQL V20 state tests",
            pytrace=False,
        )

    schema = _schema()
    pool = await asyncpg.create_pool(dsn=DSN, min_size=1, max_size=12)
    instance = V20Repository(_config(schema), shared_pool=pool)
    try:
        await instance.connect(migrate=True)
        await _seed(pool, schema)
        yield instance, pool, schema
    finally:
        await _drop_schema(pool, schema)
        await pool.close()


def _project_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _fixture() -> dict[str, object]:
    path = _project_root() / "tests/fixtures/v20/runtime_config_4211cd0_b2ba54f.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _current_payload() -> dict[str, object]:
    config = load_v20_runtime_config(_project_root())
    payload = json.loads(canonical_json(config.frozen_payload))
    assert config.state_semantics_hash == TYPE_CLEAN_CORE
    assert payload["state_semantics_hash"] == TYPE_CLEAN_CORE
    semantics = payload["state_semantics_payload"]
    assert isinstance(semantics, dict)
    assert semantics["state_input_orchestration_profile"] == ("V20_STATE_INPUT_ORCHESTRATION_V3")
    assert declared_state_semantics_is_authentic(payload)
    return payload


def _selection_v3_payload(_current: Mapping[str, object]) -> dict[str, object]:
    fixture_path = _project_root() / "tests/fixtures/v20/runtime_config_498f868_94464f2.json"
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert fixture["source_commit"] == "498f868faa6b89b8c6639c3e506c6401e854410b"
    assert fixture["expected_config_hash"] == SELECTION_V3_CONFIG_HASH
    assert fixture["expected_state_semantics_hash"] == SELECTION_V3_CORE
    payload = fixture["payload"]
    assert isinstance(payload, dict)
    assert sha256_json(payload) == SELECTION_V3_CONFIG_HASH
    assert payload["state_semantics_hash"] == SELECTION_V3_CORE
    semantics = payload["state_semantics_payload"]
    assert isinstance(semantics, dict)
    assert semantics["state_input_orchestration_profile"] == ("V20_STATE_INPUT_ORCHESTRATION_V1")
    assert declared_state_semantics_is_authentic(payload)
    return payload


def _pre_selection_payload(selection_v3: Mapping[str, object], index: int) -> dict[str, object]:
    payload = json.loads(canonical_json(selection_v3))
    dependencies = payload["strategy_dependency_hashes"]
    assert isinstance(dependencies, dict)
    dependencies.update(_PRE_SELECTION_V2_DEPENDENCIES)
    semantics = state_semantics_payload_from_frozen_payload(payload)
    assert sha256_json(semantics) == CA867_CORE
    payload["state_semantics_payload"] = semantics
    payload["state_semantics_hash"] = CA867_CORE
    payload["database_config_sha256"] = f"{'0' * 63}{index}"
    assert declared_state_semantics_is_authentic(payload)
    return payload


def _config_id(payload: Mapping[str, object]) -> str:
    return sha256_json(payload)[:24]


def _compatibility_receipt(
    source: Mapping[str, object],
    target: Mapping[str, object],
    lineage_id: str,
    official_stream_id: str,
) -> dict[str, object]:
    source_config_hash = sha256_json(source)
    target_config_hash = sha256_json(target)
    source_dependencies = source["strategy_dependency_hashes"]
    target_dependencies = target["strategy_dependency_hashes"]
    assert isinstance(source_dependencies, Mapping)
    assert isinstance(target_dependencies, Mapping)
    dependency_diff = sorted(
        relative
        for relative in set(source_dependencies) | set(target_dependencies)
        if source_dependencies.get(relative) != target_dependencies.get(relative)
    )
    evidence = {
        "schema_version": "v20-state-semantics-compatibility/v1",
        "lineage_id": lineage_id,
        "official_stream_id": official_stream_id,
        "legacy_state_semantics_hash": source["state_semantics_hash"],
        "core_state_semantics_hash": target["state_semantics_hash"],
        "evidence_config_id": source_config_hash[:24],
        "evidence_config_hash": source_config_hash,
        "accepted_config_id": target_config_hash[:24],
        "accepted_config_hash": target_config_hash,
        "dependency_diff": dependency_diff,
    }
    return {
        "lineage_id": lineage_id,
        "official_stream_id": official_stream_id,
        "legacy_state_semantics_hash": source["state_semantics_hash"],
        "core_state_semantics_hash": target["state_semantics_hash"],
        "evidence_config_id": source_config_hash[:24],
        "evidence_config_hash": source_config_hash,
        "accepted_config_id": target_config_hash[:24],
        "accepted_config_hash": target_config_hash,
        "evidence_json": canonical_json(evidence),
        "evidence_hash": sha256_json(evidence),
    }


def _revision_2_state() -> dict[str, object]:
    return {
        **genesis_state(),
        "state_revision": 2,
        "last_terminal_slot_id": "slot-ca9",
        "last_terminal_trade_date": "2026-09-01",
    }


async def _seed(pool: asyncpg.Pool, schema: str) -> None:
    fixture = _fixture()
    assert fixture["source_commit"] == B2_SOURCE_COMMIT
    assert fixture["expected_config_hash"] == B2_CONFIG_HASH
    assert fixture["expected_state_semantics_hash"] == B2_CORE
    b2 = fixture["payload"]
    assert isinstance(b2, dict)
    assert sha256_json(b2) == B2_CONFIG_HASH
    assert b2["state_semantics_hash"] == B2_CORE
    assert declared_state_semantics_is_authentic(b2)

    current = _current_payload()
    selection_v3 = _selection_v3_payload(current)
    ca_configs = [_pre_selection_payload(selection_v3, index) for index in range(1, 10)]
    terminal_ca = ca_configs[-1]
    current_config = load_v20_runtime_config(_project_root())
    registry_state = _revision_2_state()
    configs = [b2, *ca_configs, selection_v3, current]
    effective_dates = [
        date(2026, 8, 31),
        *[date(2026, 9, 1) for _ in ca_configs],
        date(2026, 9, 2),
        date(2026, 9, 2),
    ]

    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.executemany(
                f"""
                INSERT INTO {schema}.runtime_configs
                    (config_id,config_hash,strategy_version,deployment_mode,
                     effective_trade_date,config_json,created_at)
                VALUES ($1,$2,$3,$4,$5,$6::jsonb,
                        TIMESTAMPTZ '2026-09-01 01:00:00+00')
                """,
                [
                    (
                        _config_id(payload),
                        sha256_json(payload),
                        payload["strategy_version"],
                        payload["deployment_mode"],
                        effective_trade_date,
                        canonical_json(payload),
                    )
                    for payload, effective_trade_date in zip(configs, effective_dates, strict=True)
                ],
            )

            genesis = genesis_state()
            await connection.execute(
                f"""
                INSERT INTO {schema}.state_lineage_registry
                    (lineage_id,official_stream_id,genesis_state_hash,state_semantics_hash,
                     bootstrap_mode,bootstrap_checkpoint_hash,
                     bootstrap_predecessor_trade_date,created_at)
                VALUES ($1,$2,$3,$4,'EMPTY_FORWARD_SHADOW',NULL,$5,
                        TIMESTAMPTZ '2026-08-30 01:00:00+00')
                """,
                current_config.state_lineage_id,
                current_config.official_stream_id,
                sha256_json(genesis),
                B2_CORE,
                date(2026, 8, 30),
            )
            await connection.execute(
                f"""
                INSERT INTO {schema}.official_state
                    (lineage_id,revision,state_hash,state_json,updated_at)
                VALUES ($1,2,$2,$3::jsonb,TIMESTAMPTZ '2026-09-01 01:00:00+00')
                """,
                current_config.state_lineage_id,
                sha256_json(registry_state),
                canonical_json(registry_state),
            )

            terminals = [
                (b2, date(2026, 8, 31), "slot-b2", "event-b2", "decision-b2"),
                (terminal_ca, date(2026, 9, 1), "slot-ca9", "event-ca9", "decision-ca9"),
                (
                    selection_v3,
                    date(2026, 9, 2),
                    "slot-v3",
                    "event-v3",
                    "decision-v3",
                ),
            ]
            await connection.executemany(
                f"""
                INSERT INTO {schema}.decision_slots
                    (official_stream_id,trade_date,slot_id,strategy_version,config_id,
                     config_hash,lineage_id,slot_status,slot_revision,terminal_event_id,
                     terminal_decision_id,created_at,completed_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,'FAILED',1,$8,$9,
                        TIMESTAMPTZ '2026-09-01 01:00:00+00',
                        TIMESTAMPTZ '2026-09-01 01:00:00+00')
                """,
                [
                    (
                        current_config.official_stream_id,
                        trade_date,
                        slot_id,
                        payload["strategy_version"],
                        _config_id(payload),
                        sha256_json(payload),
                        current_config.state_lineage_id,
                        event_id,
                        decision_id,
                    )
                    for payload, trade_date, slot_id, event_id, decision_id in terminals
                ],
            )

            snapshots = [
                ("snapshot-b2", date(2026, 8, 31), "slot-b2"),
                ("snapshot-ca9", date(2026, 9, 1), "slot-ca9"),
                ("snapshot-v3", date(2026, 9, 2), "slot-v3"),
            ]
            await connection.executemany(
                f"""
                INSERT INTO {schema}.input_snapshots
                    (snapshot_id,snapshot_type,trade_date,snapshot_hash,snapshot_json,
                     first_received_at)
                VALUES ($1,'ENTRY_INPUT',$2,$3,$4::jsonb,
                        TIMESTAMPTZ '2026-09-01 01:00:00+00')
                """,
                [
                    (
                        snapshot_id,
                        trade_date,
                        sha256_json(
                            {
                                "schema_version": "v20-entry-input/v1",
                                "slot_id": slot_id,
                            }
                        ),
                        canonical_json(
                            {
                                "schema_version": "v20-entry-input/v1",
                                "slot_id": slot_id,
                            }
                        ),
                    )
                    for snapshot_id, trade_date, slot_id in snapshots
                ],
            )

            decisions = [
                ("decision-b2", "slot-b2", "event-b2", "snapshot-b2"),
                ("decision-ca9", "slot-ca9", "event-ca9", "snapshot-ca9"),
                ("decision-v3", "slot-v3", "event-v3", "snapshot-v3"),
            ]
            await connection.executemany(
                f"""
                INSERT INTO {schema}.entry_decisions
                    (decision_id,slot_id,event_id,snapshot_id,action,final_multiplier,
                     semantic_content_hash,commit_fingerprint,semantic_json,created_at)
                VALUES ($1,$2,$3,$4,'INPUT_INVALID',0,$5,$6,$7::jsonb,
                        TIMESTAMPTZ '2026-09-01 01:00:00+00')
                """,
                [
                    (
                        decision_id,
                        slot_id,
                        event_id,
                        snapshot_id,
                        sha256_json(semantic),
                        sha256_json({"decision_id": decision_id}),
                        canonical_json(semantic),
                    )
                    for decision_id, slot_id, event_id, snapshot_id in decisions
                    for semantic in (
                        {
                            "event_type": "ENTRY_DECISION",
                            "event_id": event_id,
                            "action": "INPUT_INVALID",
                        },
                    )
                ],
            )

            data_alert_semantic = {
                "event_type": "DATA_ALERT",
                "event_id": "event-data-alert",
                "alert_code": "MANUAL_0939_CHAIN_PROBE_RESULT",
                "probe_result": "PASS",
                "visible_message_mode": "MANUAL_OPERATOR_RENDER",
                "strategy_version": current["strategy_version"],
                "official_entry_event_id": "event-ca9",
                "symbols": [],
                "final_multiplier": 1.0,
            }
            await connection.execute(
                f"""
                INSERT INTO {schema}.outbox_events
                    (event_id,event_type,route_id,official_stream_id,lineage_id,
                     semantic_content_hash,semantic_json,payload_json,payload_hash,
                     seal_status,seal_attempt_count,delivery_status,
                     generated_at,commit_marker,available_at,created_at)
                VALUES ('event-data-alert','DATA_ALERT',$1,$2,$3,$4,$5::jsonb,
                        $6::jsonb,$7,'SEALED',0,'PENDING',
                        TIMESTAMPTZ '2026-09-01 01:00:00+00',
                        nextval($8::regclass),
                        TIMESTAMPTZ '2026-09-01 01:00:00+00',
                        TIMESTAMPTZ '2026-09-01 01:00:00+00')
                """,
                current["route_id"],
                current_config.official_stream_id,
                current_config.state_lineage_id,
                sha256_json(data_alert_semantic),
                canonical_json(data_alert_semantic),
                canonical_json({"event_id": "event-data-alert"}),
                sha256_json({"event_id": "event-data-alert"}),
                f"{schema}.commit_marker_seq",
            )

            await connection.execute(
                f"""
                INSERT INTO {schema}.model_batches
                    (model_batch_id,origin_kind,source_event_id,official_stream_id,
                     lineage_id,signal_date,multiplier,evaluation_only,
                     reference_profile_id,created_at)
                VALUES ('manual-batch','MANUAL_MONITOR','event-data-alert',$1,$2,$3,1.0,
                        FALSE,$4,TIMESTAMPTZ '2026-09-01 01:00:00+00')
                """,
                current_config.official_stream_id,
                current_config.state_lineage_id,
                date(2026, 9, 1),
                current["reference_profile_id"],
            )
            await connection.executemany(
                f"""
                INSERT INTO {schema}.model_legs
                    (model_leg_id,model_batch_id,code,stock_name,rank,relative_weight,d1,d2,
                     created_at)
                VALUES ($1,'manual-batch',$2,$3,$4,0.1,$5,$6,
                        TIMESTAMPTZ '2026-09-01 01:00:00+00')
                """,
                [
                    (
                        f"manual-leg-{index}",
                        f"{index:06d}",
                        f"stock-{index}",
                        index,
                        date(2026, 9, 2),
                        date(2026, 9, 3),
                    )
                    for index in range(1, 11)
                ],
            )

            parallel_receipts = [
                _compatibility_receipt(
                    b2,
                    payload,
                    current_config.state_lineage_id,
                    current_config.official_stream_id,
                )
                for payload in ca_configs
            ]
            parallel_receipts.append(
                _compatibility_receipt(
                    terminal_ca,
                    selection_v3,
                    current_config.state_lineage_id,
                    current_config.official_stream_id,
                )
            )
            await connection.executemany(
                f"""
                INSERT INTO {schema}.state_semantics_compatibility
                    (lineage_id,official_stream_id,legacy_state_semantics_hash,
                     core_state_semantics_hash,evidence_config_id,evidence_config_hash,
                     accepted_config_id,accepted_config_hash,evidence_json,evidence_hash,
                     created_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,
                        TIMESTAMPTZ '2026-09-01 01:00:00+00')
                """,
                [
                    (
                        receipt["lineage_id"],
                        receipt["official_stream_id"],
                        receipt["legacy_state_semantics_hash"],
                        receipt["core_state_semantics_hash"],
                        receipt["evidence_config_id"],
                        receipt["evidence_config_hash"],
                        receipt["accepted_config_id"],
                        receipt["accepted_config_hash"],
                        receipt["evidence_json"],
                        receipt["evidence_hash"],
                    )
                    for receipt in parallel_receipts
                ],
            )


async def _call(
    repository: V20Repository,
    payload: Mapping[str, object],
) -> object:
    config = load_v20_runtime_config(_project_root())
    return await repository.ensure_genesis_state(
        config.state_lineage_id,
        genesis_state(),
        sha256_json(genesis_state()),
        official_stream_id=config.official_stream_id,
        state_semantics_hash=config.state_semantics_hash,
        current_config_id=sha256_json(payload)[:24],
        current_config_hash=sha256_json(payload),
        current_config_payload=payload,
        bootstrap_mode="EMPTY_FORWARD_SHADOW",
        bootstrap_checkpoint_hash=None,
        bootstrap_predecessor_trade_date=date(2026, 8, 30),
    )


async def _snapshot(pool: asyncpg.Pool, schema: str) -> dict[str, tuple[str, ...]]:
    async with pool.acquire() as connection:
        tables = await connection.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema=$1 AND table_type='BASE TABLE'
              AND table_name <> 'state_semantics_compatibility'
            ORDER BY table_name
            """,
            schema,
        )
        result: dict[str, tuple[str, ...]] = {}
        for table in tables:
            name = table["table_name"]
            rows = await connection.fetch(
                f"""
                SELECT to_jsonb(t)::text AS data
                FROM "{schema}"."{name}" AS t
                ORDER BY to_jsonb(t)::text
                """
            )
            result[name] = tuple(row["data"] for row in rows)
        return result


async def _compatibility_snapshot(
    pool: asyncpg.Pool,
    schema: str,
) -> tuple[str, ...]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            f"""
            SELECT to_jsonb(receipt)::text AS data
            FROM {schema}.state_semantics_compatibility AS receipt
            ORDER BY to_jsonb(receipt)::text
            """
        )
        return tuple(row["data"] for row in rows)


def _normalized_receipt(receipt: Mapping[str, object]) -> dict[str, object]:
    normalized = dict(receipt)
    evidence_json = normalized.get("evidence_json")
    if isinstance(evidence_json, str):
        normalized["evidence_json"] = json.loads(evidence_json)
    elif not isinstance(evidence_json, Mapping):
        raise AssertionError("compatibility receipt evidence_json is malformed")
    normalized.pop("created_at", None)
    return normalized


def _normalized_sql_receipts(snapshot: tuple[str, ...]) -> set[str]:
    normalized = set()
    for receipt_json in snapshot:
        receipt = json.loads(receipt_json)
        normalized.add(canonical_json(_normalized_receipt(receipt)))
    return normalized


@pytest.mark.asyncio
async def test_deployed_v3_tail_upgrades_directly_without_unreleased_v4_evidence(
    repository,
) -> None:
    instance, pool, schema = repository
    before = await _snapshot(pool, schema)
    original = await _compatibility_snapshot(pool, schema)
    async with pool.acquire() as connection:
        unreleased_config_count = await connection.fetchval(
            f"""
            SELECT count(*)
            FROM {schema}.runtime_configs
            WHERE config_json->>'state_semantics_hash'=$1
            """,
            SELECTION_V4_CORE,
        )
        unreleased_receipt_count = await connection.fetchval(
            f"""
            SELECT count(*)
            FROM {schema}.state_semantics_compatibility
            WHERE legacy_state_semantics_hash=$1 OR core_state_semantics_hash=$1
            """,
            SELECTION_V4_CORE,
        )
    assert unreleased_config_count == 0
    assert unreleased_receipt_count == 0
    assert len(original) == 10
    assert _normalized_sql_receipts(original) == _expected_original_receipts()
    assert {
        (
            receipt["legacy_state_semantics_hash"],
            receipt["core_state_semantics_hash"],
        )
        for receipt in map(json.loads, original)
    } == {
        (B2_CORE, CA867_CORE),
        (CA867_CORE, SELECTION_V3_CORE),
    }

    current = _current_payload()
    selection_v3 = _selection_v3_payload(current)
    current_config = load_v20_runtime_config(_project_root())
    current_hash = sha256_json(current)
    selection_v3_hash = sha256_json(selection_v3)
    terminal_ca = _pre_selection_payload(selection_v3, 9)
    terminal_ca_hash = sha256_json(terminal_ca)
    expected_tail = _compatibility_receipt(
        selection_v3,
        current,
        current_config.state_lineage_id,
        current_config.official_stream_id,
    )
    first = await _call(instance, current)
    registry_state = _revision_2_state()

    assert first.lineage_id == current_config.state_lineage_id
    assert first.revision == 2
    assert first.state_hash == sha256_json(registry_state)
    assert canonical_json(first.payload) == canonical_json(registry_state)
    assert await _snapshot(pool, schema) == before

    after_first = await _compatibility_snapshot(pool, schema)
    assert len(after_first) == 11
    assert _normalized_sql_receipts(after_first) == _expected_receipts_with_tail(expected_tail)
    assert {
        (
            receipt["legacy_state_semantics_hash"],
            receipt["core_state_semantics_hash"],
        )
        for receipt in map(json.loads, after_first)
    } == {
        (B2_CORE, CA867_CORE),
        (CA867_CORE, SELECTION_V3_CORE),
        (SELECTION_V3_CORE, TYPE_CLEAN_CORE),
    }

    appended = None
    for receipt_json in after_first:
        receipt = json.loads(receipt_json)
        if receipt["accepted_config_hash"] == current_hash:
            appended = receipt
            break
    assert appended is not None
    assert appended["evidence_config_hash"] == selection_v3_hash
    assert "created_at" in appended
    assert set(appended) == set(expected_tail) | {"created_at"}
    assert _normalized_receipt(appended) == _normalized_receipt(expected_tail)
    assert not any(
        receipt["legacy_state_semantics_hash"]
        in {
            B2_CORE,
            CA867_CORE,
            SELECTION_V4_CORE,
        }
        and receipt["core_state_semantics_hash"] == TYPE_CLEAN_CORE
        for receipt in map(json.loads, after_first)
    )
    actual_bindings = {
        (binding.config_id, binding.config_hash, binding.state_semantics_hash)
        for binding in instance.compatible_entry_bindings
    }
    expected_bindings = {
        (
            _config_id(_fixture()["payload"]),
            B2_CONFIG_HASH,
            B2_CORE,
        ),
        (
            terminal_ca_hash[:24],
            terminal_ca_hash,
            CA867_CORE,
        ),
        (selection_v3_hash[:24], selection_v3_hash, SELECTION_V3_CORE),
    }
    assert len(actual_bindings) == 3
    assert actual_bindings == expected_bindings

    retry = await _call(instance, current)
    assert retry == first
    assert await _snapshot(pool, schema) == before
    assert await _compatibility_snapshot(pool, schema) == after_first
    assert {
        (binding.config_id, binding.config_hash, binding.state_semantics_hash)
        for binding in instance.compatible_entry_bindings
    } == expected_bindings


def _expected_original_receipts() -> set[str]:
    fixture = _fixture()
    b2 = fixture["payload"]
    assert isinstance(b2, dict)
    current = _current_payload()
    selection_v3 = _selection_v3_payload(current)
    current_config = load_v20_runtime_config(_project_root())
    ca_configs = [_pre_selection_payload(selection_v3, index) for index in range(1, 10)]

    expected = set()
    for ca_config in ca_configs:
        receipt = _compatibility_receipt(
            b2,
            ca_config,
            current_config.state_lineage_id,
            current_config.official_stream_id,
        )
        expected.add(canonical_json(_normalized_receipt(receipt)))
    expected.add(
        canonical_json(
            _normalized_receipt(
                _compatibility_receipt(
                    ca_configs[-1],
                    selection_v3,
                    current_config.state_lineage_id,
                    current_config.official_stream_id,
                )
            )
        )
    )
    return expected


def _expected_receipts_with_tail(tail: Mapping[str, object]) -> set[str]:
    expected = _expected_original_receipts()
    expected.add(canonical_json(_normalized_receipt(tail)))
    return expected


@pytest.mark.asyncio
async def test_tampered_deployed_v3_to_type_clean_receipt_rolls_back(repository) -> None:
    instance, pool, schema = repository
    current = _current_payload()
    selection_v3 = _selection_v3_payload(current)
    current_config = load_v20_runtime_config(_project_root())
    receipt = _compatibility_receipt(
        selection_v3,
        current,
        current_config.state_lineage_id,
        current_config.official_stream_id,
    )
    async with pool.acquire() as connection:
        await connection.execute(
            f"""
            INSERT INTO {schema}.state_semantics_compatibility
                (lineage_id,official_stream_id,legacy_state_semantics_hash,
                 core_state_semantics_hash,evidence_config_id,evidence_config_hash,
                 accepted_config_id,accepted_config_hash,evidence_json,evidence_hash)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)
            """,
            receipt["lineage_id"],
            receipt["official_stream_id"],
            receipt["legacy_state_semantics_hash"],
            receipt["core_state_semantics_hash"],
            receipt["evidence_config_id"],
            receipt["evidence_config_hash"],
            receipt["accepted_config_id"],
            receipt["accepted_config_hash"],
            receipt["evidence_json"],
            receipt["evidence_hash"],
        )
        updated = await connection.execute(
            f"""
            UPDATE {schema}.state_semantics_compatibility
            SET evidence_hash=repeat('0',64)
            WHERE legacy_state_semantics_hash=$1
              AND core_state_semantics_hash=$2
            """,
            SELECTION_V3_CORE,
            TYPE_CLEAN_CORE,
        )
    assert updated == "UPDATE 1"

    noncompat_before = await _snapshot(pool, schema)
    receipts_before = await _compatibility_snapshot(pool, schema)
    with pytest.raises(
        V20SemanticConflict,
        match="V20 compatibility receipt IDs or evidence are invalid",
    ):
        await _call(instance, current)

    assert await _snapshot(pool, schema) == noncompat_before
    assert await _compatibility_snapshot(pool, schema) == receipts_before


@pytest.mark.asyncio
async def test_pre_selection_v2_cannot_bypass_deployed_v3(repository) -> None:
    instance, pool, schema = repository
    async with pool.acquire() as connection:
        deleted = await connection.execute(
            f"""
            DELETE FROM {schema}.state_semantics_compatibility
            WHERE legacy_state_semantics_hash=$1
              AND core_state_semantics_hash=$2
            """,
            CA867_CORE,
            SELECTION_V3_CORE,
        )
        opened = await connection.execute(
            f"""
            UPDATE {schema}.decision_slots
            SET slot_status='OPEN', completed_at=NULL
            WHERE slot_id='slot-v3'
            """
        )
    assert deleted == "DELETE 1"
    assert opened == "UPDATE 1"

    tables_before = await _snapshot(pool, schema)
    receipts_before = await _compatibility_snapshot(pool, schema)
    with pytest.raises(
        V20SemanticConflict,
        match="V20 tail-to-current transition is unsupported",
    ):
        await _call(instance, _current_payload())

    assert await _snapshot(pool, schema) == tables_before
    assert await _compatibility_snapshot(pool, schema) == receipts_before


@pytest.mark.asyncio
async def test_late_official_state_failure_rolls_back_tail_insert(repository) -> None:
    instance, pool, schema = repository
    async with pool.acquire() as connection:
        updated = await connection.execute(
            f"""
            UPDATE {schema}.official_state
            SET state_hash=repeat('0',64)
            """
        )
    assert updated == "UPDATE 1"

    tables_before = await _snapshot(pool, schema)
    receipts_before = await _compatibility_snapshot(pool, schema)
    with pytest.raises(
        V20SemanticConflict,
        match="persisted official state hash mismatch",
    ):
        await _call(instance, _current_payload())

    assert await _snapshot(pool, schema) == tables_before
    assert await _compatibility_snapshot(pool, schema) == receipts_before


@pytest.mark.asyncio
async def test_same_lineage_wrong_stream_receipt_is_rejected(repository) -> None:
    instance, pool, schema = repository
    current = _current_payload()
    selection_v3 = _selection_v3_payload(current)
    current_config = load_v20_runtime_config(_project_root())
    receipt = _compatibility_receipt(
        selection_v3,
        current,
        current_config.state_lineage_id,
        current_config.official_stream_id,
    )
    async with pool.acquire() as connection:
        inserted = await connection.execute(
            f"""
            INSERT INTO {schema}.state_semantics_compatibility
                (lineage_id,official_stream_id,legacy_state_semantics_hash,
                 core_state_semantics_hash,evidence_config_id,evidence_config_hash,
                 accepted_config_id,accepted_config_hash,evidence_json,evidence_hash)
            VALUES ($1,'wrong-official-stream',$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
            """,
            receipt["lineage_id"],
            receipt["legacy_state_semantics_hash"],
            receipt["core_state_semantics_hash"],
            receipt["evidence_config_id"],
            receipt["evidence_config_hash"],
            receipt["accepted_config_id"],
            receipt["accepted_config_hash"],
            receipt["evidence_json"],
            receipt["evidence_hash"],
        )
    assert inserted == "INSERT 0 1"

    noncompat_before = await _snapshot(pool, schema)
    receipts_before = await _compatibility_snapshot(pool, schema)
    with pytest.raises(V20SemanticConflict, match="row binding is invalid"):
        await _call(instance, current)

    assert await _snapshot(pool, schema) == noncompat_before
    assert await _compatibility_snapshot(pool, schema) == receipts_before
