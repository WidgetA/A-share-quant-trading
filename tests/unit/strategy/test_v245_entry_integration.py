"""V24.5 must keep the original selector/gate before applying turtle policy."""

from dataclasses import replace

from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.decision_engine import prepare_entry
from tests.unit.strategy.test_v22_slim_entry import CALENDAR, fixture


def test_v245_retains_v22_gate_reference_and_entry_only_contract():
    config, state, bundle = fixture()
    artifacts = load_g_artifacts(
        config.artifact_manifest_path.parent,
        expected_manifest_sha256=config.artifact_manifest_sha256,
    )
    kwargs = dict(
        state=state,
        bundle=bundle,
        artifacts=artifacts,
        calendar=CALENDAR,
        completed_health=(),
        completed_rolling=(),
        maturity_gaps=(),
    )
    original = prepare_entry(config=config, **kwargs)
    upgraded = prepare_entry(config=replace(config, strategy_version="V24.5"), **kwargs)
    assert upgraded.commit.semantic["entry_only"] is True
    assert upgraded.commit.model_batch is None
    assert upgraded.action == original.action
    for key in (
        "symbols",
        "reference_symbols",
        "normal_open",
        "risk_streak_before",
        "health_after",
    ):
        assert upgraded.commit.semantic[key] == original.commit.semantic[key]
    assert upgraded.commit.semantic["selection_version"] == "V24.5"
    assert upgraded.commit.next_state == original.commit.next_state
