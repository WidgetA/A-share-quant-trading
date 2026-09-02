from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime
from typing import Any

import pytest

import src.web.v20_service as service_module
from src.data.database.v20_repository import V20SemanticConflict, sha256_json
from src.strategy.v20.models import (
    V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
    V20_V16_SNAPSHOT_SCHEMA,
)
from src.web.v20_v16_daygate_attestation import V16DayGateAttestationError
from tests.unit.web.test_v20_service import TZ, _late_replay_service

_ACTIONS = ("ENTER", "BLOCK", "NO_SIGNAL", "INPUT_INVALID")


def _pass_metadata() -> dict[str, Any]:
    return {
        "status": "PASS",
        "schema_version": "v16-day-gate-attestation/v1",
        "trade_date": "2026-08-31",
        "evidence_content_sha256": "e" * 64,
        "frozen_at": "2026-08-31T09:39:00+08:00",
        "evaluated_at": "2026-08-31T09:39:00+08:00",
        "evidence_relative_path": "daygate/2026-08-31.json",
        "limitation": {
            "code": "V16_DAY_GATE_EVIDENCE_ATTESTS_ORDERED_OUTPUT_NOT_FULL_READY_UNIVERSE",
            "text": (
                "V16 DayGate evidence attests the frozen ordered Top-10 output and "
                "its gate inputs; it does not contain or attest the full ready or "
                "missing stock universe."
            ),
        },
    }


def _install_attestation(
    monkeypatch: pytest.MonkeyPatch,
    result: dict[str, Any] | Exception,
    *,
    before_call: Any = None,
) -> list[dict[str, Any]]:
    observed: list[dict[str, Any]] = []

    def fake_attestation(*args: Any, **kwargs: Any) -> dict[str, Any]:
        if before_call is not None:
            before_call()
        if isinstance(result, Exception):
            raise result
        return result

    real_to_thread = asyncio.to_thread

    async def spy_to_thread(func, *args, **kwargs):
        assert func is fake_attestation
        call = {"args": args, "kwargs": kwargs, "function": func}
        observed.append(call)
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(
        service_module,
        "attest_post_cutoff_v16_day_gate",
        fake_attestation,
        raising=False,
    )
    monkeypatch.setattr(asyncio, "to_thread", spy_to_thread)
    return observed


@pytest.mark.parametrize("action", _ACTIONS)
async def test_pass_metadata_is_persisted_after_recompute_before_publish(
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
    metadata = _pass_metadata()

    def assert_recompute_already_finished() -> None:
        assert compute_calls == [context.trade_date]
        assert repository.enqueue_calls == 0
        assert repository.seal_calls == 0
        assert repository.events == {}

    calls = _install_attestation(
        monkeypatch,
        metadata,
        before_call=assert_recompute_already_finished,
    )
    sealed = await service._ensure_late_0939_replay(
        context, datetime(2026, 8, 31, 15, 30, tzinfo=TZ)
    )

    assert len(calls) == 1
    assert calls[0]["args"][0] == service.config.project_root
    assert calls[0]["args"][1].trade_date == context.trade_date
    assert calls[0]["args"][2:] == (context.trade_date, context.trade_date)
    assert calls[0]["function"] is service_module.attest_post_cutoff_v16_day_gate
    assert sealed.semantic["v16_day_gate_attestation"] == metadata
    assert sealed.semantic["official_entry_action"] == action

    attestation = sealed.semantic["v16_day_gate_attestation"]
    if action == "INPUT_INVALID":
        limitation = attestation["limitation"]
        assert "ordered Top-10 output" in limitation["text"]
        assert "does not contain or attest" in limitation["text"]
        assert "full ready or missing stock universe" in limitation["text"]
        assert "full universe" not in attestation
        assert "official frozen morning" not in str(attestation)


@pytest.mark.parametrize(
    ("reason", "detail"),
    [
        ("V16_DAY_GATE_EVIDENCE_MISSING", "no evidence exists"),
        ("V16_DAY_GATE_EVIDENCE_INVALID", "candidate is malformed"),
        ("V16_DAY_GATE_EVIDENCE_MISMATCH", "ordered output differs"),
    ],
)
async def test_attestation_failure_becomes_exact_semantic_conflict_and_never_publishes(
    monkeypatch: pytest.MonkeyPatch,
    reason: str,
    detail: str,
) -> None:
    service, repository, _client, _compute_calls, _observed, context = _late_replay_service(
        monkeypatch
    )
    calls = _install_attestation(
        monkeypatch,
        V16DayGateAttestationError(reason, detail),
    )

    with pytest.raises(V20SemanticConflict, match=f"^{reason}:{detail}$"):
        await service._ensure_late_0939_replay(context, datetime(2026, 8, 31, 15, 30, tzinfo=TZ))

    assert len(calls) == 1
    assert repository.enqueue_calls == 0
    assert repository.seal_calls == 0
    assert repository.events == {}
    assert context.late_0939_replay_completed is not True


async def test_pre_cutoff_replay_never_invokes_daygate_attestation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, _client, compute_calls, _observed, context = _late_replay_service(
        monkeypatch
    )
    calls = _install_attestation(monkeypatch, _pass_metadata())

    await service._maybe_run_late_0939_replay(context, datetime(2026, 8, 31, 9, 39, tzinfo=TZ))

    assert calls == []
    assert compute_calls == []
    assert repository.enqueue_calls == 0
    assert repository.seal_calls == 0
    assert repository.events == {}
