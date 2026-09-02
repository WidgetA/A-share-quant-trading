from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime
from typing import Any

import pytest

import src.web.v20_service as service_module
from src.strategy.v20.identity import named_hash
from src.web.v20_service import (
    LIVE_EXIT_MAX_TICK_SECONDS,
    LIVE_EXIT_SCHEDULER_WATCHDOG_SECONDS,
    V20LiveExitStageTimeout,
)
from tests.unit.web.test_v20_live_exit_deadline_acceptance import (
    TRADE_DATE,
    TZ,
    _bar,
    _DeadlineClient,
    _DeadlineRepository,
    _leg,
    _prepare,
    _seed_warm_live_history,
)


async def test_hanging_symbol_evaluation_cannot_starve_healthy_sibling_in_same_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bad = _leg("001306")
    healthy = _leg("000001")
    repository = _DeadlineRepository([healthy, bad])
    client = _DeadlineClient(
        latest={
            leg.code: _bar(leg.code, "10:00", close=8.0, trade_date=TRADE_DATE)
            for leg in (bad, healthy)
        },
        history={},
    )
    service, context = _prepare(monkeypatch, repository, client)
    _seed_warm_live_history(context, bad.code)
    _seed_warm_live_history(context, healthy.code)
    evaluated: list[str] = []
    healthy_rule_calls = 0
    never_finish = asyncio.Event()
    hanging_tasks: list[asyncio.Task[None]] = []
    original_evaluate_one = service._evaluate_one_exit
    original_commit_exit = repository.commit_exit

    async def evaluate_one(record: Any, *_args: Any, **_kwargs: Any) -> None:
        nonlocal healthy_rule_calls
        if record.code == bad.code:
            hanging_task = asyncio.create_task(never_finish.wait())
            hanging_tasks.append(hanging_task)
            await hanging_task
            return
        evaluated.append(record.code)
        healthy_rule_calls += 1
        await original_evaluate_one(record, *_args, **_kwargs)

    async def commit_and_retire_healthy_leg(commit: Any) -> bool:
        committed = await original_commit_exit(commit)
        repository.legs = [
            leg for leg in repository.legs if leg.model_leg_id != commit.model_leg_id
        ]
        return committed

    async def quiet_alert(**_kwargs: Any) -> bool:
        return True

    service.config = replace(
        service.config,
        market=replace(service.config.market, exit_poll_seconds=1),
    )
    monkeypatch.setattr(service_module, "LIVE_EXIT_RULE_STAGE_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr(service, "_evaluate_one_exit", evaluate_one)
    monkeypatch.setattr(repository, "commit_exit", commit_and_retire_healthy_leg)
    monkeypatch.setattr(service, "_safe_alert", quiet_alert)
    task = asyncio.create_task(
        service._run_live_exit_tick(
            context,
            datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ),
        )
    )
    with pytest.raises(V20LiveExitStageTimeout):
        await asyncio.wait_for(task, timeout=2.0)
    assert task.done()
    assert hanging_tasks
    hanging_results = await asyncio.gather(
        *hanging_tasks,
        return_exceptions=True,
    )
    assert len(hanging_results) == len(hanging_tasks)
    assert all(isinstance(result, asyncio.CancelledError) for result in hanging_results)

    expected_rows = {
        (bad.code, "10:00"),
        (healthy.code, "10:00"),
    }
    persisted_rows = {
        (str(row["stock_code"]), str(row["end_label"])) for row in repository.persisted_rows
    }
    assert healthy.code in evaluated
    assert healthy_rule_calls == 2
    assert len(repository.exit_commits) == 1
    commit = repository.exit_commits[0]
    assert commit.model_leg_id == healthy.model_leg_id
    assert commit.signal_type == "D2_ENTRY_12"
    assert commit.trigger_ts == datetime(2026, 9, 1, 10, 0, tzinfo=TZ)
    assert commit.rule_actionable_from == datetime(2026, 9, 1, 10, 1, tzinfo=TZ)
    assert commit.semantic["code"] == healthy.code
    assert commit.semantic["observed_close"] == pytest.approx(8.0)
    assert commit.semantic["trigger_bar_end_ts"] == commit.trigger_ts.isoformat()
    assert repository.sealed_event_ids == [commit.event_id]
    assert persisted_rows == expected_rows
    assert repository.record_calls == 1


async def test_stage_alert_identity_is_bound_to_every_runtime_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = ("route-a", "stream-a", "lineage-a")
    scopes = [
        baseline,
        ("route-b", "stream-a", "lineage-a"),
        ("route-a", "stream-b", "lineage-a"),
        ("route-a", "stream-a", "lineage-b"),
    ]
    incident_ids: list[str] = []
    semantics_by_id: dict[str, dict[str, Any]] = {}
    now = datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ)

    for scope in scopes:
        route_id, official_stream_id, state_lineage_id = scope
        repository = _DeadlineRepository([_leg()])
        client = _DeadlineClient(latest={}, history={})
        service, context = _prepare(monkeypatch, repository, client)
        service._repository_started = True
        service.config = replace(
            service.config,
            route_id=route_id,
            official_stream_id=official_stream_id,
            state_lineage_id=state_lineage_id,
        )
        exc = V20LiveExitStageTimeout(
            stage="latest",
            elapsed_seconds=1.0,
            remaining_seconds=2.0,
            deadline=3.0,
            symbols=("000001",),
            provider="tushare_rt",
        )
        await service._record_live_exit_stage_incident(context, now, exc)
        assert repository.alerts
        incident_id, semantic = repository.alerts[0]
        incident_ids.append(incident_id)
        semantics_by_id[incident_id] = semantic

        if scope == baseline:
            retry = V20LiveExitStageTimeout(
                stage="latest",
                elapsed_seconds=8.0,
                remaining_seconds=0.25,
                deadline=99.0,
                symbols=("000001",),
                provider="tushare_rt",
            )
            await service._record_live_exit_stage_incident(context, now, retry)
            assert len(repository.alerts) == 1
            assert len(repository.alert_attempts) == 2
            assert repository.alert_attempts[0][1] == repository.alert_attempts[1][1]

    assert len(set(incident_ids)) == len(scopes)
    for (route_id, official_stream_id, state_lineage_id), incident_id in zip(
        scopes, incident_ids, strict=True
    ):
        expected = named_hash(
            "V20_LIVE_EXIT_STAGE_INCIDENT_ID_V2",
            {
                "route_id": route_id,
                "official_stream_id": official_stream_id,
                "state_lineage_id": state_lineage_id,
                "trade_date": TRADE_DATE.isoformat(),
                "stage": "latest",
                "symbols": ("000001",),
                "provider": "tushare_rt",
            },
        )
        assert incident_id == expected
        semantic = semantics_by_id[incident_id]
        assert semantic["route_id"] == route_id
        assert semantic["official_stream_id"] == official_stream_id
        assert semantic["state_lineage_id"] == state_lineage_id


async def test_stage_alert_symbols_are_canonical_across_reordered_duplicate_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _DeadlineRepository([_leg()])
    client = _DeadlineClient(latest={}, history={})
    service, context = _prepare(monkeypatch, repository, client)
    service._repository_started = True
    now = datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ)
    first = V20LiveExitStageTimeout(
        stage="latest",
        elapsed_seconds=1.0,
        remaining_seconds=2.0,
        deadline=3.0,
        symbols=("000001", "001306"),
        provider="tushare_rt",
    )
    reordered_duplicate = V20LiveExitStageTimeout(
        stage="latest",
        elapsed_seconds=7.5,
        remaining_seconds=0.25,
        deadline=99.0,
        symbols=("001306", "000001", "001306"),
        provider="tushare_rt",
    )

    await service._record_live_exit_stage_incident(context, now, first)
    await service._record_live_exit_stage_incident(context, now, reordered_duplicate)

    assert len(repository.alerts) == 1
    incident_id, semantic = repository.alerts[0]
    assert incident_id == named_hash(
        "V20_LIVE_EXIT_STAGE_INCIDENT_ID_V2",
        {
            "route_id": service.config.route_id,
            "official_stream_id": service.config.official_stream_id,
            "state_lineage_id": service.config.state_lineage_id,
            "trade_date": TRADE_DATE.isoformat(),
            "stage": "latest",
            "symbols": ("000001", "001306"),
            "provider": "tushare_rt",
        },
    )
    assert semantic["symbol"] == "000001,001306"
    assert semantic["symbols"] == ["000001", "001306"]
    attempts = [attempt for attempt in repository.alert_attempts if attempt[0] == incident_id]
    assert len(attempts) == 2
    assert attempts[0][1] == attempts[1][1] == semantic


@pytest.mark.parametrize("diagnostic_result", [True, False, None])
async def test_stage_diagnostic_alert_result_determines_exactly_one_alert(
    monkeypatch: pytest.MonkeyPatch,
    diagnostic_result: bool | None,
) -> None:
    repository = _DeadlineRepository([_leg()])
    client = _DeadlineClient(latest={}, history={})
    service, context = _prepare(monkeypatch, repository, client)
    service._repository_started = True
    now = datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ)
    exc = V20LiveExitStageTimeout(
        stage="latest",
        elapsed_seconds=1.0,
        remaining_seconds=2.0,
        deadline=3.0,
        symbols=("000001",),
        provider="tushare_rt",
    )

    attempts: list[str] = []
    emitted: list[str] = []

    async def safe_alert(**kwargs: Any) -> bool | None:
        code = str(kwargs["code"])
        attempts.append(code)
        if code == "LIVE_EXIT_STAGE_TIMEOUT":
            if diagnostic_result is True:
                emitted.append(code)
            return diagnostic_result
        emitted.append(code)
        return True

    monkeypatch.setattr(service, "_safe_alert", safe_alert)
    await service._record_live_exit_stage_incident(context, now, exc)

    async def raise_stage_timeout() -> None:
        raise exc

    await service._run_phase_isolated(
        context,
        now,
        "LIVE_EXIT_CYCLE_FAILED",
        raise_stage_timeout(),
        lane_name="live_exit",
    )
    if diagnostic_result is True:
        assert attempts == ["LIVE_EXIT_STAGE_TIMEOUT"]
        assert emitted == ["LIVE_EXIT_STAGE_TIMEOUT"]
    else:
        assert attempts == [
            "LIVE_EXIT_STAGE_TIMEOUT",
            "LIVE_EXIT_CYCLE_FAILED",
        ]
        assert emitted == ["LIVE_EXIT_CYCLE_FAILED"]
    assert exc.diagnostic_alert_emitted is diagnostic_result


@pytest.mark.parametrize(
    "failure_kind",
    ["global_market_outage", "leg_evaluation_failure"],
)
async def test_specific_live_exit_incident_suppresses_duplicate_umbrella(
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
) -> None:
    legs = [_leg("000001"), _leg("001306")] if failure_kind == "global_market_outage" else [_leg()]
    repository = _DeadlineRepository(legs)
    client = _DeadlineClient(latest={}, history={})
    service, context = _prepare(monkeypatch, repository, client)
    service._repository_started = True
    now = datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ)

    if failure_kind == "global_market_outage":
        provider_failure = RuntimeError("provider unavailable")
        client.latest = provider_failure
        client.history = provider_failure
        operation = service._run_exit_cycle(context, now, include_stale=False)
    else:

        async def failing_leg(*_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError("symbol-specific diagnostic")

        monkeypatch.setattr(service, "_evaluate_one_exit", failing_leg)
        operation = service._run_exit_cycle(context, now, include_stale=False)

    await service._run_phase_isolated(
        context,
        now,
        "LIVE_EXIT_CYCLE_FAILED",
        operation,
        lane_name="live_exit",
    )
    alert_codes = [semantic["alert_code"] for _event_id, semantic in repository.alerts]
    assert (
        "EXIT_LEG_EVALUATION_FAILED" in alert_codes
        or "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" in alert_codes
    )
    assert "LIVE_EXIT_CYCLE_FAILED" not in alert_codes


@pytest.mark.parametrize("cadence", [1, 2, 15])
async def test_shared_production_budgets_prevent_outer_inner_inversion(
    monkeypatch: pytest.MonkeyPatch,
    cadence: int,
) -> None:
    repository = _DeadlineRepository([])
    client = _DeadlineClient(latest={}, history={})
    service, _context = _prepare(monkeypatch, repository, client)
    service.config = replace(
        service.config,
        market=replace(service.config.market, exit_poll_seconds=cadence),
    )

    tick_budget = service._live_exit_tick_budget()
    watchdog_budget = service._live_exit_scheduler_budget()
    assert 0 < tick_budget < watchdog_budget < float(cadence)
    if cadence == 15:
        assert tick_budget == LIVE_EXIT_MAX_TICK_SECONDS
        assert watchdog_budget == LIVE_EXIT_SCHEDULER_WATCHDOG_SECONDS
