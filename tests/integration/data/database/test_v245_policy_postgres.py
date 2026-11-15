"""Real selection transactions for the independent V24.5 daily policy.

The shared fixture requires an explicit test DSN and creates/drops only a random
v20_test_* schema. No production schema, broker, or external data is used.
"""

import asyncio
import json
from dataclasses import replace
from datetime import datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import asyncpg
import pytest

from src.data.clients.tushare_realtime import TushareDailyBar
from src.data.database.v20_repository import V20SemanticConflict, sha256_json
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.decision_engine import prepare_entry, prepare_invalid_entry
from src.strategy.v20.runtime_config import load_v20_runtime_config
from src.strategy.v20.shadow_evaluator import evaluate_shadow_batch
from src.strategy.v24_5.entry import apply_turtle
from src.strategy.v24_5.policy import genesis_state
from tests.integration.data.database.test_v20_outbox_postgres import (
    repository as _repository_fixture,
)
from tests.unit.strategy.test_v22_slim_entry import fixture

pytestmark = pytest.mark.postgres
repository = _repository_fixture
ROOT = Path(__file__).resolve().parents[4]
TZ = ZoneInfo("Asia/Shanghai")


async def case(repository, *, day_offset=0):
    instance, pool, schema = repository
    config = load_v20_runtime_config(ROOT, ROOT / "config/v24-5.yaml")
    _, state, bundle = fixture()
    now = (await pool.fetchval("SELECT clock_timestamp()")).astimezone(TZ)
    day = now.date() + timedelta(days=day_offset)
    calendar = tuple(day + timedelta(days=n) for n in range(-3, 4))
    state = replace(state, lineage_id=config.state_lineage_id)
    snapshot = {
        **bundle.snapshot,
        "trade_date": str(day),
        "prior_trade_date": str(calendar[2]),
    }
    bundle = replace(
        bundle,
        trade_date=day,
        frozen_at=now,
        prior_trade_date=calendar[2],
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
        computation_calendar=calendar,
    )
    artifacts = load_g_artifacts(
        config.artifact_manifest_path.parent,
        expected_manifest_sha256=config.artifact_manifest_sha256,
    )
    await instance.register_config(
        config_id=config.config_hash[:24],
        config_hash=config.config_hash,
        strategy_version=config.strategy_version,
        deployment_mode=config.deployment_mode,
        effective_trade_date=day,
        payload=config.frozen_payload,
    )
    await pool.execute(
        f"INSERT INTO {schema}.official_state (lineage_id,revision,state_hash,state_json) "
        "VALUES ($1,0,$2,$3::jsonb)",
        state.lineage_id,
        state.state_hash,
        json.dumps(state.payload),
    )

    def original(raw_open=True):
        snapshot = {
            **bundle.snapshot,
            "v22_slim_inputs": {
                **bundle.snapshot["v22_slim_inputs"],
                "h90": {"known": True, "block": not raw_open},
            },
        }
        actual = replace(bundle, snapshot=snapshot, snapshot_hash=sha256_json(snapshot))
        prepared = prepare_entry(
            config=config,
            state=state,
            bundle=actual,
            completed_health=[],
            completed_rolling=[],
            maturity_gaps=[],
            calendar=calendar,
            artifacts=artifacts,
        )
        assert (prepared.action == "ENTER") is raw_open
        return prepared

    async def decision(raw_open=True):
        context = await instance.load_v245_state_before(
            official_stream_id=config.official_stream_id,
            lineage_id=config.state_lineage_id,
            trade_date=day,
        )
        return apply_turtle(original(raw_open), context, calendar[2]).commit

    return config, state, day, calendar, original, decision


async def test_first_selection_atomic_state_reference_and_retry(repository):
    instance, pool, schema = repository
    config, _, day, _, original, decision = await case(repository)
    prepared = original()
    commit = await decision()
    assert commit.semantic["reference_symbols"] == prepared.commit.semantic["reference_symbols"]
    assert commit.next_state == prepared.commit.next_state
    event = await instance.commit_selection_run(commit, run_id="a" * 64)
    assert await instance.commit_selection_run(commit, run_id="a" * 64) == event
    second = await decision()
    second_event = await instance.commit_selection_run(second, run_id="b" * 64)
    assert second_event != event
    assert await pool.fetchval(f"SELECT revision FROM {schema}.official_state") == 1
    assert await pool.fetchval(f"SELECT revision FROM {schema}.v245_policy_state") == 1
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.selection_runs") == 2
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v245_policy_days") == 1
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v245_switch_intents") == 1
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v22_alert_positions") == 1
    bank = await pool.fetchrow(f"SELECT * FROM {schema}.v22_alert_positions")
    assert bank["code"] == "601398" and bank["quantity"] is None and bank["entry_price"] is None
    restored = await instance.load_v245_state_before(
        official_stream_id=config.official_stream_id,
        lineage_id=config.state_lineage_id,
        trade_date=day,
    )
    assert restored["state_before"] == genesis_state() and restored["day_status"] == "KNOWN"
    # Re-running schema migration retains both policy and references.
    await instance.migrate()
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.selection_runs") == 2


@pytest.mark.parametrize("sequence", [(False, True, False), (True, False, True)])
async def test_same_day_fresh_targets_change_without_counting_twice(repository, sequence):
    instance, pool, schema = repository
    config, _, day, _, _, decision = await case(repository)
    before = {**genesis_state(), "open_streak": 2, "last_trade_date": str(day - timedelta(days=1))}
    await pool.execute(
        f"INSERT INTO {schema}.v245_policy_state "
        "(official_stream_id,lineage_id,revision,state_hash,state_json) "
        "VALUES ($1,$2,2,$3,$4::jsonb)",
        config.official_stream_id,
        config.state_lineage_id,
        sha256_json(before),
        json.dumps(before),
    )
    for n, raw in enumerate(sequence):
        commit = await decision(raw)
        assert commit.semantic["v245_transition"]["permit"] is raw
        await instance.commit_selection_run(commit, run_id=str(n + 1) * 64)
    assert await pool.fetchval(f"SELECT revision FROM {schema}.v245_policy_state") == 3
    assert await pool.fetchval(f"SELECT raw_open FROM {schema}.v245_policy_days") is sequence[0]
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v245_switch_intents") == 2
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v22_alert_batches") == 2
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v22_alert_positions") == 4
    latest = await pool.fetchval(
        f"SELECT target FROM {schema}.v245_switch_intents ORDER BY updated_at DESC LIMIT 1"
    )
    assert latest == ("STOCKS" if sequence[-1] else "BANK")


@pytest.mark.parametrize("reference_available", [True, False])
async def test_failed_official_slot_then_valid_run_recovers_reference_once(
    repository, reference_available
):
    instance, pool, schema = repository
    config, state, day, calendar, original, decision = await case(repository, day_offset=-5)
    invalid = prepare_invalid_entry(
        config=config,
        state=state,
        trade_date=day,
        calendar=calendar,
        reason_code="CURRENT_DATA_UNAVAILABLE",
        detail="test transport failure",
        invalid_commit_not_before_ts=datetime.combine(day, time(9, 45), tzinfo=TZ),
    )
    await instance.commit_entry(invalid.commit)
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v245_policy_days") == 0
    assert (
        await instance.get_valid_selection_reference(
            config.official_stream_id, day, lineage_id=config.state_lineage_id
        )
        is None
    )
    first = await decision()
    event = await instance.commit_selection_run(first, run_id="d" * 64)
    reference = await instance.get_valid_selection_reference(
        config.official_stream_id, day, lineage_id=config.state_lineage_id
    )
    assert reference.event_id == event and reference.action == "BLOCK"
    assert (
        reference.semantic["reference_symbols"] == original().commit.semantic["reference_symbols"]
    )
    assert reference.snapshot["v22_market"] == original().commit.snapshot["v22_market"]
    assert await pool.fetchval(f"SELECT slot_status FROM {schema}.decision_slots") == "FAILED"
    assert await pool.fetchval(f"SELECT revision FROM {schema}.official_state") == 1
    assert await pool.fetchval(f"SELECT revision FROM {schema}.v245_policy_state") == 1
    shadows = await pool.fetch(f"SELECT * FROM {schema}.shadow_batches")
    assert len(shadows) == len(first.shadow_batches) == 1
    assert json.loads(shadows[0]["batch_json"]) == dict(first.shadow_batches[0].payload)
    assert (
        await pool.fetchval(f"SELECT source_event_id FROM {schema}.v245_reference_repairs") == event
    )
    await instance.commit_selection_run(await decision(False), run_id="e" * 64)
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.shadow_batches") == 1
    recovered = await instance.get_valid_selection_reference(
        config.official_stream_id, day, lineage_id=config.state_lineage_id
    )
    assert recovered.event_id == event
    # The recovered nullable-decision basket is consumed by the existing price
    # lock, maturity evaluator, and next-session completed-reference loader.
    scope = {"official_stream_id": config.official_stream_id, "lineage_id": config.state_lineage_id}
    pending = await instance.list_pending_shadow_reference_batches(day + timedelta(days=1), **scope)
    assert len(pending) == 1 and pending[0].decision_id is None
    cutoff = datetime.combine(day, time(9, 45), tzinfo=TZ)
    references = {code: 20.0 for code in pending[0].payload["comparison_pool_codes"]}
    if reference_available:
        await instance.update_shadow_references(
            day,
            reference_prices=references,
            snapshot_hash=sha256_json(references),
            not_before_ts=cutoff,
            **scope,
        )
    else:
        await instance.finalize_shadow_references_unavailable(
            day,
            snapshot_hash=sha256_json({"no_eligible_receipts": True}),
            not_before_ts=cutoff,
            **scope,
        )
    mature = await instance.list_pending_shadow_batches(day + timedelta(days=3), **scope)
    assert len(mature) == 1
    result = evaluate_shadow_batch(
        kind=mature[0].kind,
        payload=mature[0].payload,
        reference_status=mature[0].reference_status,
        reference_prices=mature[0].reference_prices,
        daily_bars={
            code: TushareDailyBar(code, str(day + timedelta(days=2)), 21.0, 10000)
            for code in references
        },
    )
    assert result.status == ("COMPLETE_VALID" if reference_available else "COMPLETE_INVALID")
    assert await instance.complete_shadow_batch(
        mature[0].batch_id,
        result.batch_return,
        result.status,
        result.payload_update,
        not_before_ts=datetime.combine(day + timedelta(days=3), time(9, 39), tzinfo=TZ),
        **scope,
    )
    completed = await instance.load_recent_completed(
        "HEALTH", before_t2=day + timedelta(days=3), limit=10, **scope
    )
    assert len(completed) == 1 and completed[0].status == result.status
    assert completed[0].batch_return == result.batch_return


async def test_downstream_registration_failure_rolls_back_whole_selection(repository, monkeypatch):
    instance, pool, schema = repository
    _, _, _, _, _, decision = await case(repository)
    commit = await decision()

    async def fail(*args):
        raise RuntimeError("failure after turtle-state and stock-reference writes")

    monkeypatch.setattr("src.data.database.v245_switch_positions.register_v245_selection", fail)
    with pytest.raises(RuntimeError, match="failure after"):
        await instance.commit_selection_run(commit, run_id="f" * 64)
    for table in (
        "selection_runs",
        "outbox_events",
        "decision_slots",
        "shadow_batches",
        "v245_policy_days",
        "v245_policy_state",
        "v22_alert_positions",
    ):
        assert await pool.fetchval(f"SELECT count(*) FROM {schema}.{table}") == 0
    assert await pool.fetchval(f"SELECT revision FROM {schema}.official_state") == 0


async def test_forged_transition_rolls_back_even_if_original_entry_was_valid(repository):
    instance, pool, schema = repository
    _, _, _, _, _, decision = await case(repository)
    valid = await decision()
    semantic = {
        **valid.semantic,
        "v245_transition": {**valid.semantic["v245_transition"], "permit": True},
    }
    forged = replace(valid, semantic=semantic, semantic_content_hash=sha256_json(semantic))
    with pytest.raises(V20SemanticConflict):
        await instance.commit_selection_run(forged, run_id="c" * 64)
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.selection_runs") == 0
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.outbox_events") == 0
    assert await pool.fetchval(f"SELECT revision FROM {schema}.official_state") == 0


async def test_concurrent_same_request_has_one_policy_and_one_outbox_result(repository):
    instance, pool, schema = repository
    _, _, _, _, _, decision = await case(repository)
    commit = await decision()

    async def submit():
        # The service's existing retry boundary retries serialization conflicts.
        for attempt in range(5):
            try:
                return await instance.commit_selection_run(commit, run_id="9" * 64)
            except asyncpg.SerializationError:
                if attempt == 4:
                    raise
                await asyncio.sleep(0)

    results = await asyncio.gather(submit(), submit())
    assert results[0] == results[1]
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.selection_runs") == 1
    assert await pool.fetchval(f"SELECT revision FROM {schema}.v245_policy_state") == 1
