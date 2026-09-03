from __future__ import annotations

from dataclasses import replace
from datetime import datetime

import pytest

from src.data.database.v20_repository import sha256_json
from src.strategy.v20.models import (
    V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
    V20_V16_SNAPSHOT_SCHEMA,
)
from tests.unit.web.test_v20_service import TZ, _late_replay_service

_ACTIONS = ("ENTER", "BLOCK", "NO_SIGNAL", "INPUT_INVALID")


@pytest.mark.parametrize("action", _ACTIONS)
async def test_late_replay_persists_shared_recomputation_without_daygate_dependency(
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    service, repository, _client, compute_calls, _observed, context = _late_replay_service(
        monkeypatch
    )
    status = context.entry_status
    assert status is not None
    if action != "INPUT_INVALID":
        canonical = await service._compute_canonical_v16_from_persisted_raw(context)
        bundle = service._project_canonical_v16(canonical, calendar=context.calendar)
        compute_calls.clear()
        semantic = {
            **status.semantic,
            "action": action,
            "v16_snapshot_hash": bundle.snapshot_hash,
        }
        snapshot = {
            **status.snapshot,
            "schema_version": V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
            "v16_snapshot_schema_version": V20_V16_SNAPSHOT_SCHEMA,
            "early_market_source_hash": canonical.input_hash,
            "scorer_model_sha256": canonical.model_sha256,
            "scorer_feature_sha256": canonical.feature_list_sha256,
            "v16_snapshot_hash": bundle.snapshot_hash,
        }
        status = replace(
            status,
            action=action,
            semantic=semantic,
            semantic_content_hash=sha256_json(semantic),
            snapshot=snapshot,
            snapshot_hash=sha256_json(snapshot),
        )
        context.entry_status = status
    sealed = await service._ensure_late_0939_replay(
        context, datetime(2026, 8, 31, 15, 30, tzinfo=TZ)
    )

    assert compute_calls == [context.trade_date]
    assert "v16_day_gate_attestation" not in sealed.semantic
    assert sealed.semantic["canonical_selection_recomputed"] is True
    assert sealed.semantic["canonical_source"] == "PERSISTED_RAW_SCANNER_RECOMPUTATION"
    assert sealed.semantic["official_entry_action"] == action


async def test_pre_cutoff_replay_never_invokes_shared_recomputation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, _client, compute_calls, _observed, context = _late_replay_service(
        monkeypatch
    )
    await service._maybe_run_late_0939_replay(context, datetime(2026, 8, 31, 9, 39, tzinfo=TZ))

    assert compute_calls == []
    assert repository.enqueue_calls == 0
    assert repository.seal_calls == 0
    assert repository.events == {}
