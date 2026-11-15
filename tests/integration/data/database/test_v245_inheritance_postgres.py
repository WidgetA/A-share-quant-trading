"""A mid-stream upgrade inherits original gates without replaying old orders.

Uses the existing isolated PostgreSQL fixture and real V22 preparation/commit
path. A schema-local database clock advances through the historical sessions;
the original same-date and expiry predicates remain in force. No V24.5 policy
state is seeded and no production database is involved.
"""

import json
from dataclasses import replace
from datetime import date, datetime, time

import asyncpg
import pytest

from src.data.database.v20_repository import V20EntryDeadlineExceeded, sha256_json
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.decision_engine import prepare_entry, prepare_invalid_entry
from src.strategy.v20.runtime_config import load_v20_runtime_config
from src.strategy.v24_5.entry import apply_turtle
from tests.integration.data.database.test_v20_outbox_postgres import (
    DSN,
)
from tests.integration.data.database.test_v20_outbox_postgres import (
    repository as _repository_fixture,
)
from tests.unit.strategy.test_v22_slim_entry import ROOT, TZ, fixture

pytestmark = pytest.mark.postgres
_base_repository = _repository_fixture

UPGRADE_DAY = date(2026, 11, 12)
CALENDAR = (
    date(2026, 10, 30),
    date(2026, 11, 2),
    date(2026, 11, 3),
    date(2026, 11, 4),
    date(2026, 11, 5),
    date(2026, 11, 6),
    date(2026, 11, 9),
    date(2026, 11, 10),
    date(2026, 11, 11),
    UPGRADE_DAY,
    date(2026, 11, 13),
    date(2026, 11, 16),
    date(2026, 11, 17),
)
LEGACY_TABLES = (
    "official_state",
    "decision_slots",
    "entry_decisions",
    "selection_runs",
    "input_snapshots",
    "shadow_batches",
    "outbox_events",
    "delivery_attempts",
    "v22_alert_batches",
    "v22_alert_positions",
)


@pytest.fixture
async def repository(_base_repository):
    """Use real PostgreSQL transactions while replaying a test-only DB clock.

    Explicit pg_catalog placement makes unqualified clock_timestamp() resolve
    to this disposable schema's function. Production SQL and its cutoff/date
    guards are unchanged, and the other integration fixtures use their normal
    pools. Schema/function/table disappear with the existing isolated fixture.
    """
    instance, original_pool, schema = _base_repository
    await original_pool.execute(
        f"CREATE TABLE {schema}.inheritance_clock_control (as_of TIMESTAMPTZ NOT NULL);"
        f"CREATE FUNCTION {schema}.clock_timestamp() RETURNS TIMESTAMPTZ "
        "LANGUAGE SQL VOLATILE AS $$ "
        f"SELECT as_of FROM {schema}.inheritance_clock_control $$"
    )
    await original_pool.execute(
        f"INSERT INTO {schema}.inheritance_clock_control (as_of) VALUES ($1)",
        datetime.combine(UPGRADE_DAY, time(9, 46), tzinfo=TZ),
    )
    pool = await asyncpg.create_pool(
        dsn=DSN,
        min_size=1,
        max_size=4,
        server_settings={"search_path": f"{schema},pg_catalog"},
    )
    instance._pool = pool
    try:
        assert await pool.fetchval("SELECT clock_timestamp()") == datetime.combine(
            UPGRADE_DAY, time(9, 46), tzinfo=TZ
        )
        yield instance, pool, schema
    finally:
        instance._pool = original_pool
        await pool.close()


async def set_database_day(pool, schema, day):
    expected = datetime.combine(day, time(9, 46), tzinfo=TZ)
    await pool.execute(
        f"UPDATE {schema}.inheritance_clock_control SET as_of=$1",
        expected,
    )
    assert await pool.fetchval("SELECT clock_timestamp()") == expected


async def legacy_snapshot(pool, schema):
    """Compare original ledger contents, not just table counts."""
    snapshot = {}
    for table in LEGACY_TABLES:
        rows = await pool.fetch(
            f"SELECT to_jsonb(t)::text AS payload FROM {schema}.{table} AS t "
            "ORDER BY to_jsonb(t)::text"
        )
        snapshot[table] = tuple(row["payload"] for row in rows)
    return snapshot


def prepared_original(config, current, day, raw_open):
    _, _, template = fixture()
    prior = CALENDAR[CALENDAR.index(day) - 1]
    snapshot = {
        **template.snapshot,
        "trade_date": day.isoformat(),
        "prior_trade_date": prior.isoformat(),
        "v22_slim_inputs": {
            **template.snapshot["v22_slim_inputs"],
            "health_before": current.payload["health"],
            "h90": {"known": True, "block": not raw_open},
        },
    }
    bundle = replace(
        template,
        trade_date=day,
        frozen_at=datetime.combine(day, time(9, 40), tzinfo=TZ),
        prior_trade_date=prior,
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
        computation_calendar=CALENDAR,
    )
    prepared = prepare_entry(
        config=config,
        state=current,
        bundle=bundle,
        completed_health=(),
        completed_rolling=(),
        maturity_gaps=(),
        calendar=CALENDAR,
        artifacts=load_g_artifacts(
            config.artifact_manifest_path.parent,
            expected_manifest_sha256=config.artifact_manifest_sha256,
        ),
    )
    assert (prepared.action == "ENTER") is raw_open
    return prepared


async def seed_original_history(
    repository, sequence=(True, True, True), *, invalid_then_valid=False, canonical_v20=False
):
    instance, pool, schema = repository
    config, initial_state, _ = fixture()
    assert config.strategy_version == "V22-slim"
    target_index = CALENDAR.index(UPGRADE_DAY)
    history_days = CALENDAR[target_index - len(sequence) : target_index]
    await instance.register_config(
        config_id=config.config_hash[:24],
        config_hash=config.config_hash,
        strategy_version=config.strategy_version,
        deployment_mode=config.deployment_mode,
        effective_trade_date=history_days[0],
        payload=config.frozen_payload,
    )
    await pool.execute(
        f"INSERT INTO {schema}.official_state (lineage_id,revision,state_hash,state_json) "
        "VALUES ($1,0,$2,$3::jsonb)",
        initial_state.lineage_id,
        initial_state.state_hash,
        json.dumps(initial_state.payload),
    )
    v20_config = None
    if canonical_v20:
        v20_config = load_v20_runtime_config(ROOT, ROOT / "config/v20.yaml")
        await instance.register_config(
            config_id=v20_config.config_hash[:24],
            config_hash=v20_config.config_hash,
            strategy_version=v20_config.strategy_version,
            deployment_mode=v20_config.deployment_mode,
            effective_trade_date=history_days[0],
            payload=v20_config.frozen_payload,
        )
    for index, (day, raw_open) in enumerate(zip(history_days, sequence, strict=True)):
        await set_database_day(pool, schema, day)
        current = await instance.load_state(config.state_lineage_id)
        if (invalid_then_valid and index == 1) or (canonical_v20 and index == 0):
            invalid = prepare_invalid_entry(
                config=v20_config if canonical_v20 and index == 0 else config,
                state=current,
                trade_date=day,
                calendar=CALENDAR,
                reason_code="NO_CURRENT_DATA",
                detail="The first original slot had no usable current data.",
                invalid_commit_not_before_ts=datetime.combine(day, time(9, 45), tzinfo=TZ),
            )
            await instance.commit_entry(invalid.commit)
        prepared = prepared_original(config, current, day, raw_open)
        assert "v245_transition" not in prepared.commit.semantic
        await instance.commit_selection_run(
            prepared.commit, run_id=sha256_json(["original-v22-history", day.isoformat()])
        )
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.selection_runs") == len(sequence)
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v245_policy_state") == 0
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v245_policy_days") == 0
    await set_database_day(pool, schema, UPGRADE_DAY)
    return config


async def test_first_v245_day_inherits_three_original_opens_without_replaying_history(repository):
    instance, pool, schema = repository
    config = await seed_original_history(repository)
    before = await legacy_snapshot(pool, schema)

    context = await instance.load_v245_state_before(
        official_stream_id=config.official_stream_id,
        lineage_id=config.state_lineage_id,
        trade_date=UPGRADE_DAY,
    )
    state_row = await pool.fetchrow(f"SELECT * FROM {schema}.v245_policy_state")
    audit_row = await pool.fetchrow(f"SELECT * FROM {schema}.v245_policy_bootstraps")
    again = await instance.load_v245_state_before(
        official_stream_id=config.official_stream_id,
        lineage_id=config.state_lineage_id,
        trade_date=UPGRADE_DAY,
    )

    # History is state input, not permission to issue past recommendations or
    # register past holdings again. The original gate/risk ledger is immutable.
    assert await legacy_snapshot(pool, schema) == before
    assert context["state_before"] == again["state_before"]
    assert dict(await pool.fetchrow(f"SELECT * FROM {schema}.v245_policy_state")) == dict(state_row)
    assert dict(await pool.fetchrow(f"SELECT * FROM {schema}.v245_policy_bootstraps")) == dict(
        audit_row
    )
    assert state_row["revision"] == 0
    assert audit_row["target_trade_date"] == UPGRADE_DAY
    assert audit_row["history_until"] == date(2026, 11, 11)
    audit = json.loads(audit_row["audit_json"])
    assert sha256_json(audit) == audit_row["audit_hash"]
    assert len(audit["source_events"]) == 3
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v245_policy_days") == 0
    assert context["state_before"] == {
        "schema": "v24.5-policy-state/v1",
        "mode": "NORMAL",
        "open_streak": 3,
        "closed_streak": 0,
        "last_trade_date": "2026-11-11",
    }


@pytest.mark.parametrize(
    "sequence,mode,opens,closed,today_permit",
    [
        ((True, True), "TURTLE", 2, 0, True),
        ((True, True, True), "NORMAL", 3, 0, True),
        *((((True,) * 3 + (False,) * n), "NORMAL", 0, n, True) for n in range(1, 5)),
        ((True, True, True, False, False, False, False, False), "TURTLE", 0, 5, False),
    ],
)
async def test_first_today_permission_uses_inherited_mode_and_streaks(
    repository, sequence, mode, opens, closed, today_permit
):
    instance, pool, schema = repository
    original = await seed_original_history(repository, sequence)
    prior = await legacy_snapshot(pool, schema)
    context = await instance.load_v245_state_before(
        official_stream_id=original.official_stream_id,
        lineage_id=original.state_lineage_id,
        trade_date=UPGRADE_DAY,
    )
    assert context["state_before"]["mode"] == mode
    assert context["state_before"]["open_streak"] == opens
    assert context["state_before"]["closed_streak"] == closed
    upgraded = load_v20_runtime_config(ROOT, ROOT / "config/v24-5.yaml")
    assert upgraded.official_stream_id == original.official_stream_id
    assert upgraded.state_lineage_id == original.state_lineage_id
    prepared = prepared_original(
        upgraded, await instance.load_state(original.state_lineage_id), UPGRADE_DAY, True
    )
    result = apply_turtle(prepared, context, date(2026, 11, 11))
    assert result.commit.semantic["v245_transition"]["permit"] is today_permit
    assert (result.action == "ENTER") is today_permit
    assert await legacy_snapshot(pool, schema) == prior
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v245_policy_days") == 0


async def test_invalid_original_slot_uses_first_valid_same_day_signal(repository):
    instance, pool, schema = repository
    config = await seed_original_history(repository, invalid_then_valid=True)
    assert (
        await pool.fetchval(
            f"SELECT slot_status FROM {schema}.decision_slots WHERE trade_date=$1",
            date(2026, 11, 10),
        )
        == "FAILED"
    )
    prior = await legacy_snapshot(pool, schema)
    context = await instance.load_v245_state_before(
        official_stream_id=config.official_stream_id,
        lineage_id=config.state_lineage_id,
        trade_date=UPGRADE_DAY,
    )
    assert context["state_before"]["mode"] == "NORMAL"
    assert context["state_before"]["open_streak"] == 3
    assert context["state_before"]["last_trade_date"] == "2026-11-11"
    assert await legacy_snapshot(pool, schema) == prior


async def test_inherited_before_is_frozen_for_same_day_fresh_rerun(repository):
    instance, pool, schema = repository
    original = await seed_original_history(repository, (True, True))
    config = load_v20_runtime_config(ROOT, ROOT / "config/v24-5.yaml")
    await instance.register_config(
        config_id=config.config_hash[:24],
        config_hash=config.config_hash,
        strategy_version=config.strategy_version,
        deployment_mode=config.deployment_mode,
        effective_trade_date=UPGRADE_DAY,
        payload=config.frozen_payload,
    )
    original_before = await instance.load_state(original.state_lineage_id)
    frozen = None
    for raw_open, run_id in ((True, "a" * 64), (False, "b" * 64)):
        context = await instance.load_v245_state_before(
            official_stream_id=config.official_stream_id,
            lineage_id=config.state_lineage_id,
            trade_date=UPGRADE_DAY,
        )
        if frozen is None:
            frozen = context["state_before"]
        assert context["state_before"] == frozen
        prepared = prepared_original(config, original_before, UPGRADE_DAY, raw_open)
        commit = apply_turtle(prepared, context, date(2026, 11, 11)).commit
        assert commit.semantic["v245_transition"]["permit"] is raw_open
        await instance.commit_selection_run(commit, run_id=run_id)
    assert await pool.fetchval(f"SELECT revision FROM {schema}.v245_policy_state") == 1
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v245_policy_days") == 1
    assert await pool.fetchval(f"SELECT raw_open FROM {schema}.v245_policy_days") is True


async def test_other_stream_original_history_is_not_used(repository):
    instance, pool, schema = repository
    config = await seed_original_history(repository)
    prior = await legacy_snapshot(pool, schema)
    context = await instance.load_v245_state_before(
        official_stream_id=config.official_stream_id + "-different",
        lineage_id=config.state_lineage_id,
        trade_date=UPGRADE_DAY,
    )
    assert context["state_before"]["mode"] == "TURTLE"
    assert context["state_before"]["last_trade_date"] is None
    assert context["state_before"]["open_streak"] == 0
    assert await legacy_snapshot(pool, schema) == prior


async def test_current_and_future_original_days_are_not_counted_into_before(repository):
    instance, pool, schema = repository
    config = await seed_original_history(repository)
    prior = await legacy_snapshot(pool, schema)
    context = await instance.load_v245_state_before(
        official_stream_id=config.official_stream_id,
        lineage_id=config.state_lineage_id,
        trade_date=date(2026, 11, 11),
    )
    assert context["state_before"]["mode"] == "TURTLE"
    assert context["state_before"]["open_streak"] == 2
    assert context["state_before"]["last_trade_date"] == "2026-11-10"
    assert await legacy_snapshot(pool, schema) == prior


async def test_v20_daily_slot_is_not_reclassified_by_later_v22_manual_rerun(repository):
    instance, pool, schema = repository
    config = await seed_original_history(repository, canonical_v20=True)
    prior = await legacy_snapshot(pool, schema)
    slot = await pool.fetchrow(
        f"SELECT strategy_version,slot_status FROM {schema}.decision_slots WHERE trade_date=$1",
        date(2026, 11, 9),
    )
    assert slot["strategy_version"] == "V20_BAD_E50_G_BASE_V1"
    assert slot["slot_status"] == "FAILED"
    context = await instance.load_v245_state_before(
        official_stream_id=config.official_stream_id,
        lineage_id=config.state_lineage_id,
        trade_date=UPGRADE_DAY,
    )
    assert context["state_before"]["mode"] == "TURTLE"
    assert context["state_before"]["open_streak"] == 2
    audit = json.loads(
        await pool.fetchval(f"SELECT audit_json FROM {schema}.v245_policy_bootstraps")
    )
    assert len(audit["source_events"]) == 2
    assert await legacy_snapshot(pool, schema) == prior


@pytest.mark.parametrize("database_day", [date(2026, 11, 11), date(2026, 11, 13)])
async def test_historical_clock_fixture_still_rejects_nonmatching_commit_date(
    repository, database_day
):
    instance, pool, schema = repository
    original = await seed_original_history(repository, (True, True))
    config = load_v20_runtime_config(ROOT, ROOT / "config/v24-5.yaml")
    await instance.register_config(
        config_id=config.config_hash[:24],
        config_hash=config.config_hash,
        strategy_version=config.strategy_version,
        deployment_mode=config.deployment_mode,
        effective_trade_date=UPGRADE_DAY,
        payload=config.frozen_payload,
    )
    context = await instance.load_v245_state_before(
        official_stream_id=config.official_stream_id,
        lineage_id=config.state_lineage_id,
        trade_date=UPGRADE_DAY,
    )
    prepared = prepared_original(
        config, await instance.load_state(original.state_lineage_id), UPGRADE_DAY, True
    )
    commit = apply_turtle(prepared, context, date(2026, 11, 11)).commit
    prior = await legacy_snapshot(pool, schema)
    await set_database_day(pool, schema, database_day)
    clock = await pool.fetchval("SELECT clock_timestamp()")
    assert clock.astimezone(TZ).date() != UPGRADE_DAY
    # The earlier date is unexpired; the later date is also past expiry. This
    # verifies nonmatching-date rejection, not an independently isolated expiry.
    assert (clock < commit.action_expiry_ts) is (database_day < UPGRADE_DAY)
    with pytest.raises(V20EntryDeadlineExceeded):
        await instance.commit_selection_run(commit, run_id="f" * 64)
    assert await legacy_snapshot(pool, schema) == prior
    assert await pool.fetchval(f"SELECT count(*) FROM {schema}.v245_policy_days") == 0
