"""An upgrade must retain populated original state, not reset to turtle history."""

from datetime import date

import pytest

from src.data.database.v20_repository import sha256_json
from src.strategy.v20.decision_engine import genesis_state
from src.strategy.v20.runtime_config import load_v20_runtime_config
from tests.integration.data.database.test_v20_state_semantics_postgres import (
    _project_root,
    _snapshot,
    repository,  # noqa: F401 - real PostgreSQL, isolated schema fixture
)

pytestmark = pytest.mark.postgres


async def test_populated_original_ledger_is_identical_across_version_startup(repository):  # noqa: F811
    instance, pool, schema = repository
    root = _project_root()
    original = load_v20_runtime_config(root, root / "config/v22-slim.yaml")
    upgraded = load_v20_runtime_config(root, root / "config/v24-5.yaml")
    before = await _snapshot(pool, schema)
    original_state = await instance.load_state(original.state_lineage_id)
    assert original_state.revision > 0  # Exercise a populated ledger, not an empty genesis.
    genesis = genesis_state()
    for config in (original, upgraded, upgraded):
        restored = await instance.ensure_genesis_state(
            config.state_lineage_id,
            genesis,
            sha256_json(genesis),
            official_stream_id=config.official_stream_id,
            state_semantics_hash=config.state_semantics_hash,
            bootstrap_mode="EMPTY_FORWARD_SHADOW",
            bootstrap_checkpoint_hash=None,
            bootstrap_predecessor_trade_date=date(2026, 11, 15),
        )
        assert restored == original_state
    assert await _snapshot(pool, schema) == before
    policy = await instance.load_v245_state_before(
        official_stream_id=upgraded.official_stream_id,
        lineage_id=upgraded.state_lineage_id,
        trade_date=date(2026, 11, 16),
    )
    assert policy["state_before"]["mode"] == "TURTLE"
    assert policy["expected_revision"] == 0
    assert await instance.load_state(original.state_lineage_id) == original_state
