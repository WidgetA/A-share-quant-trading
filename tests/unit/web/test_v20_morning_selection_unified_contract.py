from __future__ import annotations

import asyncio
import inspect
from dataclasses import replace
from datetime import date, datetime, time
from types import SimpleNamespace
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

import pytest

import src.web.v20_service as service_module
from src.common.v20_feishu import _render_entry_strategy_body
from src.data.clients.tushare_realtime import (
    TushareEarlyMarketData,
    TushareMinuteBar,
    TushareRealtimeClient,
    tushare_minute_bars_to_early_market_data,
)
from src.data.database.v20_repository import (
    ActiveModelLeg,
    ManualMonitorEnrollmentRecord,
    MinuteBarRecord,
    V20SemanticConflict,
    V20StateConflict,
    sha256_json,
)
from src.strategy.v20.decision_engine import prepare_invalid_entry
from src.web.v15_scan_service import V15ScanState
from src.web.v20_routes import _dispatch_manual_trigger
from src.web.v20_service import V20Service, _bar_payload, _DayContext
from tests.unit.web.test_v20_auto_manual_exact_parity_acceptance import (
    _canonical_master,
    _raw_bars,
    _service_and_artifact,
)
from tests.unit.web.test_v20_canonical_projection_acceptance import (
    BREADTH_ONLY_CODE,
    FULL_EXCHANGE_CALENDAR,
    RAW_EVIDENCE_CODES,
    TRADE_DATE,
    _rehash,
)

TZ = ZoneInfo("Asia/Shanghai")
BEFORE_DECISION_BAR = datetime.combine(TRADE_DATE, time(9, 38, 59), TZ)
AT_DECISION_BAR = datetime.combine(TRADE_DATE, time(9, 39, 0), TZ)
JUST_BEFORE_CUTOFF = datetime.combine(TRADE_DATE, time(9, 39, 59), TZ)
AT_CUTOFF = datetime.combine(TRADE_DATE, time(9, 40, 0), TZ)


class _CurrentDayRtMinDailyProbe:
    """Stand-in for one V20 service-level rt_min_daily acquisition.

    The concrete client's one-physical-call-per-unique-code contract is covered
    in ``tests/unit/data/clients/test_tushare_early_market_data.py``.  This test
    file owns only orchestration timing and service-level singleflight.
    """

    def __init__(self, *, failures: int = 0) -> None:
        self.failures = failures
        self.calls: list[tuple[tuple[str, ...], date]] = []
        self.historical_calls: list[tuple[tuple[str, ...], date]] = []
        self.started: asyncio.Event | None = None
        self.release: asyncio.Event | None = None

    async def batch_get_early_market_data(
        self,
        codes: list[str],
        expected_trade_date: date,
    ) -> dict[str, TushareEarlyMarketData]:
        self.calls.append((tuple(codes), expected_trade_date))
        if self.started is not None:
            self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.failures:
            self.failures -= 1
            raise RuntimeError("synthetic current-day rt_min_daily outage")
        bars = _raw_bars()
        result: dict[str, TushareEarlyMarketData] = {}
        for code in codes:
            early = tushare_minute_bars_to_early_market_data(
                code,
                bars[code],
                expected_trade_date,
            )
            assert early is not None
            result[code] = early
        return result

    async def batch_get_early_minute_history_for_date(
        self,
        codes: list[str],
        trade_date: date,
    ) -> dict[str, tuple[Any, ...]]:
        self.historical_calls.append((tuple(codes), trade_date))
        raise AssertionError("a current-day selection must never call stk_mins")


def _canonical_for(service: V20Service) -> Any:
    return _rehash(
        replace(
            _canonical_master(),
            model_sha256=service.config.strategy_dependency_hashes["models/lgbrank_latest.txt"],
            feature_list_sha256=service.config.strategy_dependency_hashes[
                "models/feature_list.json"
            ],
        )
    )


def _install_real_current_day_acquisition_boundary(
    monkeypatch: pytest.MonkeyPatch,
    service: V20Service,
    repository: Any,
    *,
    failures: int = 0,
) -> _CurrentDayRtMinDailyProbe:
    """Exercise the real persisted-raw -> current-day adapter boundary.

    The expensive scanner itself is replaced by a deterministic canonical
    result.  Date routing, the complete missing-code set, rt_min_daily adapter
    call, persistence/readback, and the production scheduling gates remain real.
    """

    canonical = _canonical_for(service)
    repository.raw_by_key.clear()
    repository.raw_read_calls.clear()
    client = _CurrentDayRtMinDailyProbe(failures=failures)
    service._scan_state.realtime_client = client

    async def frozen_inputs(context: _DayContext) -> Any:
        seed, universe, clean_boards = await service._historical_early_evidence_seed(
            context.trade_date,
            universe_override=tuple(canonical.universe),
            clean_boards_override=canonical.clean_boards,
            evidence_codes=(BREADTH_ONLY_CODE,),
        )
        return SimpleNamespace(
            early_data_seed=seed,
            universe=universe,
            clean_boards=clean_boards,
            prev_closes=canonical.prev_closes,
            history_raw=canonical.history_raw,
            names={code: f"name-{code}" for code in canonical.universe},
            calendar=FULL_EXCHANGE_CALENDAR,
            prior_daily={},
            st_eligible_codes=tuple(canonical.universe),
        )

    async def deterministic_scanner(
        _state: Any,
        requested_trade_date: date,
        **kwargs: Any,
    ) -> Any:
        assert requested_trade_date == TRADE_DATE
        assert kwargs["allow_realtime_fetch"] is False
        assert set(kwargs["early_data_seed"]) == set(RAW_EVIDENCE_CODES)
        return canonical

    monkeypatch.setattr(service, "_historical_canonical_inputs", frozen_inputs)
    monkeypatch.setattr(
        service,
        "_compute_canonical_v16_from_persisted_raw",
        V20Service._compute_canonical_v16_from_persisted_raw.__get__(service),
    )
    monkeypatch.setattr(service_module, "compute_canonical_v16_scan", deterministic_scanner)
    return client


async def _scheduled(service: V20Service, _request_id: str) -> Any:
    return await service._run_decision_iteration_with_cutoff(service._aware_now())


async def _manual(service: V20Service, request_id: str) -> Any:
    return await _dispatch_manual_trigger(service, request_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "invoke", "cold_start"),
    (
        ("scheduled-warm", _scheduled, False),
        ("scheduled-cold-start", _scheduled, True),
        ("manual-warm", _manual, False),
        ("manual-cold-start", _manual, True),
    ),
)
async def test_complete_current_day_acquisition_starts_only_at_093900(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    invoke: Callable[[V20Service, str], Awaitable[Any]],
    cold_start: bool,
) -> None:
    now = [BEFORE_DECISION_BAR]
    service, repository, _artifact = _service_and_artifact(
        monkeypatch,
        now=BEFORE_DECISION_BAR,
        artifact_hit=False,
    )
    service._clock = lambda: now[0]
    if not cold_start:
        service._context = _DayContext(
            trade_date=TRADE_DATE,
            calendar=FULL_EXCHANGE_CALENDAR,
        )
    client = _install_real_current_day_acquisition_boundary(
        monkeypatch,
        service,
        repository,
    )

    # A separately-owned, long-running V16 state is a sentinel: exercising any
    # V20 timing path must not swap or mutate its client/runtime fields.
    v16_client = object()
    v16_state = V15ScanState(initialized=True, realtime_client=v16_client)

    await invoke(service, f"{path}-before-0939")
    assert client.calls == []
    assert client.historical_calls == []
    assert v16_state.initialized is True
    assert v16_state.realtime_client is v16_client

    now[0] = AT_DECISION_BAR
    await invoke(service, f"{path}-at-0939")

    assert client.calls == [(RAW_EVIDENCE_CODES, TRADE_DATE)]
    assert client.historical_calls == []
    assert repository.commit_entry_calls == 1
    assert v16_state.initialized is True
    assert v16_state.realtime_client is v16_client


@pytest.mark.asyncio
async def test_failed_0939_full_acquisition_is_not_restarted_until_0940(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [BEFORE_DECISION_BAR]
    service, repository, _artifact = _service_and_artifact(
        monkeypatch,
        now=BEFORE_DECISION_BAR,
        artifact_hit=False,
    )
    service._clock = lambda: now[0]
    client = _install_real_current_day_acquisition_boundary(
        monkeypatch,
        service,
        repository,
        failures=1,
    )

    await _scheduled(service, "retry-before-0939")
    assert client.calls == []

    now[0] = AT_DECISION_BAR
    await _scheduled(service, "retry-first-attempt")
    assert client.calls == [(RAW_EVIDENCE_CODES, TRADE_DATE)]
    assert repository.commit_entry_calls == 0

    # The client already owns bounded per-symbol transport retries.  A second
    # service-level fan-out inside the same provider minute would issue the
    # whole market again and can consume the 6,000/minute allowance.
    now[0] = AT_DECISION_BAR.replace(second=1)
    await _scheduled(service, "retry-second-attempt")
    await _manual(service, "retry-manual-same-minute")
    assert client.calls == [(RAW_EVIDENCE_CODES, TRADE_DATE)]
    assert repository.commit_entry_calls == 0

    # A post-cutoff check in the next provider minute may make one new
    # non-actionable attempt, but still cannot write the official entry slot.
    now[0] = AT_CUTOFF
    result = await _manual(service, "retry-post-cutoff-next-minute")
    await _drain_mews_kicks(service)
    assert client.calls == [
        (RAW_EVIDENCE_CODES, TRADE_DATE),
        (RAW_EVIDENCE_CODES, TRADE_DATE),
    ]
    assert client.historical_calls == []
    assert result["non_actionable"] is True
    assert repository.commit_entry_calls == 0


@pytest.mark.asyncio
async def test_0939_scheduled_and_manual_contenders_share_one_full_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert TushareRealtimeClient.MAX_CONCURRENCY == 40
    service, repository, _artifact = _service_and_artifact(
        monkeypatch,
        now=AT_DECISION_BAR,
        artifact_hit=False,
    )
    client = _install_real_current_day_acquisition_boundary(
        monkeypatch,
        service,
        repository,
    )
    client.started = asyncio.Event()
    client.release = asyncio.Event()

    scheduled = asyncio.create_task(service._run_decision_iteration_with_cutoff(AT_DECISION_BAR))
    await asyncio.wait_for(client.started.wait(), timeout=1.0)
    manual = asyncio.create_task(
        _dispatch_manual_trigger(service, "manual-093900-singleflight-contender")
    )
    await asyncio.sleep(0)

    # The second legal V20 contender must wait on the same decision lane; it
    # must not start a second 40-worker service-level acquisition and turn the
    # approved V20 fan-out into an 80-worker burst.
    assert client.calls == [(RAW_EVIDENCE_CODES, TRADE_DATE)]
    client.release.set()
    results = await asyncio.gather(scheduled, manual, return_exceptions=True)
    assert not [item for item in results if isinstance(item, BaseException)]
    assert client.calls == [(RAW_EVIDENCE_CODES, TRADE_DATE)]
    assert client.historical_calls == []
    assert repository.commit_entry_calls == 1
    await _drain_mews_kicks(service)


@pytest.mark.asyncio
async def test_same_minute_subset_joins_first_v20_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _repository, _artifact = _service_and_artifact(
        monkeypatch,
        now=AT_DECISION_BAR,
    )
    client = _CurrentDayRtMinDailyProbe()
    client.started = asyncio.Event()
    client.release = asyncio.Event()

    complete = asyncio.create_task(
        service._acquire_current_day_early_market_data_once(
            TRADE_DATE,
            RAW_EVIDENCE_CODES,
            client.batch_get_early_market_data,
        )
    )
    await asyncio.wait_for(client.started.wait(), timeout=1.0)
    subset_codes = RAW_EVIDENCE_CODES[:2]
    subset = asyncio.create_task(
        service._acquire_current_day_early_market_data_once(
            TRADE_DATE,
            subset_codes,
            client.batch_get_early_market_data,
        )
    )
    await asyncio.sleep(0)

    assert client.calls == [(RAW_EVIDENCE_CODES, TRADE_DATE)]
    client.release.set()
    complete_result, subset_result = await asyncio.gather(complete, subset)
    assert set(complete_result) == set(RAW_EVIDENCE_CODES)
    assert set(subset_result) == set(subset_codes)


@pytest.mark.asyncio
async def test_same_minute_superset_is_rejected_without_second_v20_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _repository, _artifact = _service_and_artifact(
        monkeypatch,
        now=AT_DECISION_BAR,
    )
    client = _CurrentDayRtMinDailyProbe()
    client.started = asyncio.Event()
    client.release = asyncio.Event()
    subset_codes = RAW_EVIDENCE_CODES[:2]

    first = asyncio.create_task(
        service._acquire_current_day_early_market_data_once(
            TRADE_DATE,
            subset_codes,
            client.batch_get_early_market_data,
        )
    )
    await asyncio.wait_for(client.started.wait(), timeout=1.0)
    with pytest.raises(V20StateConflict, match="smaller target set"):
        await service._acquire_current_day_early_market_data_once(
            TRADE_DATE,
            RAW_EVIDENCE_CODES,
            client.batch_get_early_market_data,
        )
    assert client.calls == [(subset_codes, TRADE_DATE)]

    client.release.set()
    await first


@pytest.mark.asyncio
async def test_new_minute_never_overlaps_a_still_running_v20_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [AT_DECISION_BAR]
    service, _repository, _artifact = _service_and_artifact(
        monkeypatch,
        now=AT_DECISION_BAR,
    )
    service._clock = lambda: now[0]
    client = _CurrentDayRtMinDailyProbe()
    client.started = asyncio.Event()
    client.release = asyncio.Event()

    first = asyncio.create_task(
        service._acquire_current_day_early_market_data_once(
            TRADE_DATE,
            RAW_EVIDENCE_CODES,
            client.batch_get_early_market_data,
        )
    )
    await asyncio.wait_for(client.started.wait(), timeout=1.0)
    now[0] = AT_CUTOFF
    next_minute = asyncio.create_task(
        service._acquire_current_day_early_market_data_once(
            TRADE_DATE,
            RAW_EVIDENCE_CODES,
            client.batch_get_early_market_data,
        )
    )
    await asyncio.sleep(0)

    assert client.calls == [(RAW_EVIDENCE_CODES, TRADE_DATE)]
    client.release.set()
    first_result, next_minute_result = await asyncio.gather(first, next_minute)
    assert first_result == next_minute_result
    repeated_next_minute = await service._acquire_current_day_early_market_data_once(
        TRADE_DATE,
        RAW_EVIDENCE_CODES,
        client.batch_get_early_market_data,
    )
    assert repeated_next_minute == first_result
    assert client.calls == [(RAW_EVIDENCE_CODES, TRADE_DATE)]
    detached = await service._take_current_day_early_attempt_tasks()
    assert len(detached) == 1


@pytest.mark.asyncio
async def test_stop_cancels_and_clears_v20_current_day_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _repository, _artifact = _service_and_artifact(
        monkeypatch,
        now=AT_DECISION_BAR,
    )
    client = _CurrentDayRtMinDailyProbe()
    client.started = asyncio.Event()
    client.release = asyncio.Event()
    waiter = asyncio.create_task(
        service._acquire_current_day_early_market_data_once(
            TRADE_DATE,
            RAW_EVIDENCE_CODES,
            client.batch_get_early_market_data,
        )
    )
    await asyncio.wait_for(client.started.wait(), timeout=1.0)
    assert service._current_day_early_attempts

    # Keep the lifecycle assertion scoped to the V20-owned acquisition task.
    service._repository_started = False
    await service.stop()
    result = await asyncio.gather(waiter, return_exceptions=True)

    assert isinstance(result[0], asyncio.CancelledError)
    assert service._current_day_early_attempts == {}


@pytest.mark.asyncio
async def test_stop_fence_prevents_waiter_from_starting_acquisition_after_detach(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _repository, _artifact = _service_and_artifact(
        monkeypatch,
        now=AT_DECISION_BAR,
    )
    client = _CurrentDayRtMinDailyProbe()
    service._repository_started = False
    await service._current_day_early_attempt_lock.acquire()
    stop_task = asyncio.create_task(service.stop())
    await asyncio.sleep(0)
    assert service._stop_event.is_set()
    waiter = asyncio.create_task(
        service._acquire_current_day_early_market_data_once(
            TRADE_DATE,
            RAW_EVIDENCE_CODES,
            client.batch_get_early_market_data,
        )
    )
    service._current_day_early_attempt_lock.release()

    await stop_task
    result = await asyncio.gather(waiter, return_exceptions=True)
    assert isinstance(result[0], V20StateConflict)
    assert client.calls == []
    assert service._current_day_early_attempts == {}


def _install_compute_caller_spy(
    monkeypatch: pytest.MonkeyPatch,
    service: V20Service,
    callers: list[str],
) -> None:
    original = service._compute_morning_selection

    async def spy(trade_date: date, **kwargs: Any) -> Any:
        frame = inspect.currentframe()
        assert frame is not None and frame.f_back is not None
        callers.append(frame.f_back.f_code.co_name)
        return await original(trade_date, **kwargs)

    monkeypatch.setattr(service, "_compute_morning_selection", spy)


async def _drain_mews_kicks(service: V20Service) -> None:
    tasks = tuple(service._mews_trigger_tasks)
    if tasks:
        await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_scheduled_pre_cutoff_manual_and_post_cutoff_manual_share_one_orchestrator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduled, _scheduled_repo, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    manual, _manual_repo, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    post_cutoff, _post_repo, _ = _service_and_artifact(
        monkeypatch,
        now=AT_CUTOFF,
    )
    callers: list[str] = []
    for service in (scheduled, manual, post_cutoff):
        _install_compute_caller_spy(monkeypatch, service, callers)

    await scheduled._run_decision_iteration_with_cutoff(JUST_BEFORE_CUTOFF)
    await _dispatch_manual_trigger(manual, "manual-at-093959-shared-path")
    await _dispatch_manual_trigger(post_cutoff, "manual-at-094000-shared-path")
    await _drain_mews_kicks(manual)
    await _drain_mews_kicks(post_cutoff)

    assert len(callers) == 3
    assert len(set(callers)) == 1, (
        "all three paths must call _compute_morning_selection through one shared "
        f"high-level orchestrator; observed immediate callers={callers}"
    )


@pytest.mark.asyncio
async def test_same_input_has_identical_prepared_entry_and_formal_strategy_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    automatic, automatic_repo, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    post_cutoff, post_repo, _ = _service_and_artifact(
        monkeypatch,
        now=AT_CUTOFF,
    )

    automatic_calculation = await automatic._compute_morning_selection(TRADE_DATE)
    post_calculation = await post_cutoff._compute_morning_selection(TRADE_DATE)
    assert automatic_calculation.prepared == post_calculation.prepared

    await automatic._run_decision_iteration_with_cutoff(JUST_BEFORE_CUTOFF)
    result = await _dispatch_manual_trigger(
        post_cutoff,
        "manual-at-094000-body-parity",
    )
    await _drain_mews_kicks(post_cutoff)

    assert automatic_repo.commit is not None
    alert = post_repo.alerts[result["operator_event_id"]]
    formal_semantic = dict(automatic_repo.commit.semantic)
    check_semantic = dict(alert.semantic["entry_render_semantic"])
    assert formal_semantic == check_semantic

    formal_body = _render_entry_strategy_body(formal_semantic)
    check_body = _render_entry_strategy_body(check_semantic)
    assert formal_body == check_body
    assert automatic_repo.outbox is not None and automatic_repo.outbox.payload is not None
    assert alert.payload is not None
    assert formal_body in automatic_repo.outbox.payload["message"]
    assert formal_body in alert.payload["message"]


@pytest.mark.asyncio
async def test_committed_morning_slot_replays_exact_prepared_entry_and_body_after_cutoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [JUST_BEFORE_CUTOFF]
    service, repository, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    service._clock = lambda: now[0]

    await service._run_decision_iteration_with_cutoff(JUST_BEFORE_CUTOFF)
    assert repository.commit is not None
    assert repository.status is not None
    assert "state_before" in repository.status.snapshot
    formal_commit = repository.commit
    formal_status = repository.status
    formal_state = repository.state
    formal_body = _render_entry_strategy_body(dict(formal_status.semantic))

    now[0] = AT_CUTOFF
    repository.seal_at = AT_CUTOFF
    replay = await service._compute_morning_selection(TRADE_DATE)
    assert replay.prepared.commit == formal_commit

    result = await _dispatch_manual_trigger(
        service,
        "manual-at-094000-real-terminal-parity",
    )
    await _drain_mews_kicks(service)
    alert = repository.alerts[result["operator_event_id"]]
    replay_semantic = dict(alert.semantic["entry_render_semantic"])

    assert replay_semantic == dict(formal_status.semantic)
    assert _render_entry_strategy_body(replay_semantic) == formal_body
    assert result["calculation_result"] == "SUCCESS"
    assert result["official_comparison_result"] == "MATCH"
    assert result["official_mismatch_fields"] == []
    assert repository.status == formal_status
    assert repository.state == formal_state
    assert repository.commit == formal_commit
    assert repository.commit_entry_calls == 1


@pytest.mark.asyncio
async def test_committed_morning_slot_replays_after_official_state_head_advances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [JUST_BEFORE_CUTOFF]
    service, repository, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    service._clock = lambda: now[0]
    await service._run_decision_iteration_with_cutoff(JUST_BEFORE_CUTOFF)
    assert repository.commit is not None
    assert repository.status is not None
    formal_commit = repository.commit
    formal_status = repository.status

    # Model a later, independently committed state transition.  The immutable
    # morning terminal still carries its own exact prestate and must remain
    # replayable; the current head is only fenced against concurrent mutation.
    advanced_payload = {
        **dict(repository.state.payload),
        "state_revision": repository.state.revision + 1,
    }
    repository.state = replace(
        repository.state,
        revision=repository.state.revision + 1,
        state_hash=sha256_json(advanced_payload),
        payload=advanced_payload,
    )
    advanced_state = repository.state
    now[0] = AT_CUTOFF
    repository.seal_at = AT_CUTOFF

    replay = await service._compute_morning_selection(TRADE_DATE)
    assert replay.prepared.commit == formal_commit
    result = await _dispatch_manual_trigger(
        service,
        "manual-at-094000-after-state-advance",
    )
    await _drain_mews_kicks(service)
    alert = repository.alerts[result["operator_event_id"]]

    assert alert.semantic["entry_render_semantic"] == formal_status.semantic
    assert result["official_comparison_result"] == "MATCH"
    assert repository.state == advanced_state
    assert repository.status == formal_status
    assert repository.commit_entry_calls == 1


@pytest.mark.asyncio
async def test_input_invalid_terminal_replays_from_its_exact_prestate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [JUST_BEFORE_CUTOFF]
    service, repository, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    service._clock = lambda: now[0]

    # Compute the independent legal proposal while the repository still holds
    # the exact pre-terminal state.  The failed terminal then advances state,
    # just as a real INPUT_INVALID commit does.
    expected = await service._compute_morning_selection(TRADE_DATE)
    prestate = repository.state
    invalid = prepare_invalid_entry(
        config=service.config,
        state=prestate,
        trade_date=TRADE_DATE,
        calendar=FULL_EXCHANGE_CALENDAR,
        reason_code="INPUT_TIME_BOUNDARY_VIOLATION",
        detail="synthetic strict-cutoff rejection",
        invalid_commit_not_before_ts=AT_CUTOFF,
        scheduled_exits_today=expected.scheduled_exits_today,
    )
    await repository.commit_entry(invalid.commit)
    assert repository.status is not None
    assert repository.status.action == "INPUT_INVALID"
    assert repository.status.snapshot["state_before"]["payload"] == dict(prestate.payload)
    invalid_status = repository.status
    invalid_state = repository.state

    now[0] = AT_CUTOFF
    repository.seal_at = AT_CUTOFF
    replay = await service._compute_morning_selection(TRADE_DATE)
    assert replay.prepared == expected.prepared

    result = await _dispatch_manual_trigger(
        service,
        "manual-at-094000-invalid-terminal-replay",
    )
    await _drain_mews_kicks(service)
    alert = repository.alerts[result["operator_event_id"]]
    assert alert.semantic["entry_render_semantic"] == expected.prepared.commit.semantic
    assert result["calculation_result"] == "SUCCESS"
    assert result["official_comparison_result"] == "NOT_AVAILABLE"
    assert repository.status == invalid_status
    assert repository.state == invalid_state
    assert repository.commit_entry_calls == 1


@pytest.mark.asyncio
async def test_current_input_invalid_probe_can_feed_manual_monitor_without_legacy_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [JUST_BEFORE_CUTOFF]
    service, repository, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    service._clock = lambda: now[0]

    expected = await service._compute_morning_selection(TRADE_DATE)
    invalid = prepare_invalid_entry(
        config=service.config,
        state=repository.state,
        trade_date=TRADE_DATE,
        calendar=FULL_EXCHANGE_CALENDAR,
        reason_code="INPUT_TIME_BOUNDARY_VIOLATION",
        detail="synthetic strict-cutoff rejection",
        invalid_commit_not_before_ts=AT_CUTOFF,
        scheduled_exits_today=expected.scheduled_exits_today,
    )
    await repository.commit_entry(invalid.commit)
    assert repository.status is not None
    assert repository.status.action == "INPUT_INVALID"

    now[0] = AT_CUTOFF
    repository.seal_at = AT_CUTOFF
    probe_result = await service.trigger_canonical_selection_check_only(
        "manual-at-094000-monitor-source",
        AT_CUTOFF,
    )
    source_event_id = str(probe_result["operator_event_id"])
    source = repository.alerts[source_event_id]
    assert source.semantic["v20_action"] == "ENTER"
    assert source.semantic["official_entry_event_id"] == repository.status.event_id
    assert "replay_action" not in source.semantic
    assert "official_entry_event_id_before" not in source.semantic
    assert "official_entry_event_id_after" not in source.semantic

    monitor_at = datetime.combine(TRADE_DATE, time(10, 0), TZ)
    reference_received_at = datetime.combine(TRADE_DATE, time(9, 42), TZ)
    reference_records: list[MinuteBarRecord] = []
    for offset, item in enumerate(source.semantic["symbols"]):
        price = 20.0 + offset
        bar = TushareMinuteBar(
            stock_code=str(item["code"]),
            bar_end=datetime.combine(TRADE_DATE, time(9, 41), TZ),
            end_label="09:41",
            open_price=price,
            high_price=price + 0.2,
            low_price=price - 0.2,
            close_price=price + 0.1,
            volume=100_000.0,
            amount=2_000_000.0,
        )
        payload = _bar_payload(bar)
        reference_records.append(
            MinuteBarRecord(
                code=bar.stock_code,
                bar_end=bar.bar_end,
                end_label=bar.end_label,
                source_hash=sha256_json(payload),
                payload=payload,
                first_received_at=reference_received_at,
            )
        )

    original_raw_loader = repository.list_raw_minute_bar_records

    async def raw_loader(codes, *, trade_date, end_labels, received_before=None):
        if tuple(end_labels) == ("09:41",):
            return tuple(
                record
                for record in reference_records
                if record.code in set(codes)
                and (received_before is None or record.first_received_at < received_before)
            )
        return await original_raw_loader(
            codes,
            trade_date=trade_date,
            end_labels=end_labels,
        )

    enrollment: ManualMonitorEnrollmentRecord | None = None
    enrolled_legs: tuple[ActiveModelLeg, ...] = ()

    async def get_enrollment(_source_event_id: str, **_scope: Any):
        return enrollment

    async def enroll(commit):
        nonlocal enrollment, enrolled_legs
        enrollment = ManualMonitorEnrollmentRecord(
            enrollment_id=commit.enrollment_id,
            source_event_id=commit.source_event_id,
            official_entry_event_id=commit.official_entry_event_id,
            model_batch_id=commit.model_batch.model_batch_id,
            request_id=commit.request_id,
            signal_date=commit.signal_date,
            d1=commit.d1,
            d2=commit.d2,
            activation_cutoff_ts=commit.activation_cutoff_ts,
            source_semantic_content_hash=commit.source_semantic_content_hash,
            source_payload_hash=commit.source_payload_hash,
            calendar_evidence_hash=commit.calendar_evidence_hash,
            semantic=commit.enrollment_semantic,
            created_at=monitor_at,
        )
        enrolled_legs = tuple(
            ActiveModelLeg(
                model_leg_id=leg.model_leg_id,
                model_batch_id=commit.model_batch.model_batch_id,
                decision_id=None,
                signal_date=commit.signal_date,
                code=leg.code,
                stock_name=leg.stock_name,
                rank=leg.rank,
                relative_weight=leg.relative_weight,
                d1=leg.d1,
                d2=leg.d2,
                reference_status="PENDING",
                reference_price=None,
                reference_snapshot_hash=None,
                evaluation_only=False,
                mews_snapshot_id=None,
                mews_fast_state=None,
                exit_intent_id=None,
                origin_kind="MANUAL_MONITOR",
                source_event_id=commit.source_event_id,
            )
            for leg in commit.model_batch.legs
        )
        return True

    async def list_enrolled_legs(model_batch_id: str, **_scope: Any):
        return [leg for leg in enrolled_legs if leg.model_batch_id == model_batch_id]

    monkeypatch.setattr(repository, "list_raw_minute_bar_records", raw_loader)
    monkeypatch.setattr(repository, "get_manual_monitor_enrollment", get_enrollment, raising=False)
    monkeypatch.setattr(repository, "enroll_manual_monitor", enroll, raising=False)
    monkeypatch.setattr(
        repository,
        "list_manual_monitor_batch_legs",
        list_enrolled_legs,
        raising=False,
    )
    now[0] = monitor_at
    repository.seal_at = monitor_at

    monitor_result = await service.enroll_manual_monitor(
        source_event_id,
        "manual-monitor-current-probe-source",
    )

    assert monitor_result["created"] is True
    assert monitor_result["armed"] is True
    assert monitor_result["armed_leg_count"] == len(source.semantic["symbols"])
    assert enrollment is not None
    assert enrollment.source_event_id == source_event_id


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ("present_none", "extra_field", "payload_hash"))
async def test_terminal_without_valid_canonical_prestate_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    service, repository, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    await service._run_decision_iteration_with_cutoff(JUST_BEFORE_CUTOFF)
    assert repository.status is not None

    snapshot = dict(repository.status.snapshot)
    if corruption == "present_none":
        snapshot["state_before"] = None
    else:
        state_before = dict(snapshot["state_before"])
        if corruption == "extra_field":
            state_before["unexpected"] = "must-not-be-accepted"
        else:
            state_before["payload"] = {
                **dict(state_before["payload"]),
                "state_revision": 99,
            }
        snapshot["state_before"] = state_before
    repository.status = replace(
        repository.status,
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
    )
    repository.seal_at = AT_CUTOFF

    with pytest.raises(V20SemanticConflict, match="canonical state_before"):
        await service.trigger_canonical_selection_check_only(
            f"manual-at-094000-bad-prestate-{corruption}",
            AT_CUTOFF,
        )

    assert repository.alert_write_calls == 0
    assert repository.commit_entry_calls == 1


@pytest.mark.asyncio
async def test_explicit_post_cutoff_manual_accepts_only_legacy_missing_prestate_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [JUST_BEFORE_CUTOFF]
    service, repository, artifact = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    service._clock = lambda: now[0]
    await service._run_decision_iteration_with_cutoff(JUST_BEFORE_CUTOFF)
    assert repository.status is not None

    snapshot = dict(repository.status.snapshot)
    snapshot.pop("state_before")
    repository.status = replace(
        repository.status,
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
    )
    # A later legal state transition may advance the head while retaining the
    # durable identity of today's last terminal slot.
    advanced_payload = {
        **dict(repository.state.payload),
        "state_revision": repository.state.revision + 1,
    }
    repository.state = replace(
        repository.state,
        revision=repository.state.revision + 1,
        state_hash=sha256_json(advanced_payload),
        payload=advanced_payload,
    )
    legacy_status = repository.status
    official_state = repository.state
    official_commit = repository.commit
    commit_calls = repository.commit_entry_calls
    raw_writes = repository.raw_write_calls
    artifact_saves = tuple(artifact.save_calls)
    alert_writes = repository.alert_write_calls
    seal_calls = repository.seal_calls

    current_input_calls: list[str] = []
    original_scheduled = service._scheduled_exits_today
    original_policy = service._policy_inputs

    async def current_scheduled(trade_date: date) -> Any:
        current_input_calls.append("scheduled")
        return await original_scheduled(trade_date)

    async def current_policy(trade_date: date) -> Any:
        current_input_calls.append("policy")
        return await original_policy(trade_date)

    monkeypatch.setattr(service, "_scheduled_exits_today", current_scheduled)
    monkeypatch.setattr(service, "_policy_inputs", current_policy)
    now[0] = AT_CUTOFF
    repository.seal_at = AT_CUTOFF

    first = await _dispatch_manual_trigger(
        service,
        "manual-at-094000-legacy-missing-prestate",
    )
    second = await _dispatch_manual_trigger(
        service,
        "manual-at-094000-legacy-missing-prestate",
    )
    await _drain_mews_kicks(service)
    alert = repository.alerts[first["operator_event_id"]]
    recomputed = alert.semantic["entry_render_semantic"]

    assert first["created"] is True
    assert second == {**first, "created": False}
    assert first["calculation_result"] == "SUCCESS"
    assert first["official_comparison_result"] == "NOT_AVAILABLE"
    assert first["official_comparison_unavailable_reason"] == "LEGACY_TERMINAL_PRESTATE_UNAVAILABLE"
    assert first["official_mismatch_fields"] == []
    assert first["official_v16_snapshot_hash"] is None
    assert first["canonical_artifact_compared"] is True
    assert first["canonical_artifact_matches"] is True
    assert alert.semantic["official_comparison_unavailable_reason"] == (
        "LEGACY_TERMINAL_PRESTATE_UNAVAILABLE"
    )
    assert alert.payload is not None
    assert "LEGACY_TERMINAL_PRESTATE_UNAVAILABLE" in alert.payload["message"]
    assert recomputed["state_before_hash"] == official_state.state_hash
    assert current_input_calls == ["scheduled", "policy"]

    assert repository.status == legacy_status
    assert repository.state == official_state
    assert repository.commit == official_commit
    assert repository.commit_entry_calls == commit_calls
    assert repository.raw_write_calls == raw_writes
    assert tuple(artifact.save_calls) == artifact_saves
    assert repository.forbidden_write_calls == []
    assert repository.alert_write_calls == alert_writes + 1
    assert repository.seal_calls == seal_calls + 1


@pytest.mark.asyncio
async def test_explicit_manual_legacy_input_invalid_uses_existing_fresh_theory_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [JUST_BEFORE_CUTOFF]
    service, repository, artifact = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    service._clock = lambda: now[0]
    baseline = await service._compute_morning_selection(TRADE_DATE)
    invalid = prepare_invalid_entry(
        config=service.config,
        state=repository.state,
        trade_date=TRADE_DATE,
        calendar=FULL_EXCHANGE_CALENDAR,
        reason_code="INPUT_TIME_BOUNDARY_VIOLATION",
        detail="synthetic legacy INPUT_INVALID terminal",
        invalid_commit_not_before_ts=AT_CUTOFF,
        scheduled_exits_today=baseline.scheduled_exits_today,
    )
    await repository.commit_entry(invalid.commit)
    assert repository.status is not None
    snapshot = dict(repository.status.snapshot)
    snapshot.pop("state_before")
    repository.status = replace(
        repository.status,
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
    )
    legacy_status = repository.status
    official_state = repository.state
    raw_writes = repository.raw_write_calls
    artifact_saves = tuple(artifact.save_calls)
    now[0] = AT_CUTOFF
    repository.seal_at = AT_CUTOFF

    result = await _dispatch_manual_trigger(
        service,
        "manual-at-094000-legacy-input-invalid",
    )
    alert = repository.alerts[result["operator_event_id"]]

    assert result["calculation_result"] == "SUCCESS"
    assert result["official_comparison_result"] == "NOT_AVAILABLE"
    assert result["official_comparison_unavailable_reason"] == (
        "LEGACY_TERMINAL_PRESTATE_UNAVAILABLE"
    )
    assert result["official_mismatch_fields"] == []
    assert result["official_v16_snapshot_hash"] is None
    assert result["canonical_artifact_compared"] is False
    assert result["canonical_artifact_matches"] is None
    assert alert.semantic["entry_render_semantic"]["state_before_hash"] == official_state.state_hash
    assert repository.status == legacy_status
    assert repository.state == official_state
    assert repository.commit_entry_calls == 1
    assert repository.raw_write_calls == raw_writes
    assert tuple(artifact.save_calls) == artifact_saves
    assert repository.forbidden_write_calls == []
    assert repository.alert_write_calls == 1


@pytest.mark.asyncio
async def test_legacy_missing_prestate_remains_strict_outside_explicit_manual_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    await service._run_decision_iteration_with_cutoff(JUST_BEFORE_CUTOFF)
    assert repository.status is not None
    snapshot = dict(repository.status.snapshot)
    snapshot.pop("state_before")
    repository.status = replace(
        repository.status,
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
    )
    context = _DayContext(
        trade_date=TRADE_DATE,
        calendar=FULL_EXCHANGE_CALENDAR,
        entry_status=repository.status,
    )
    alerts_before = dict(repository.alerts)

    with pytest.raises(V20SemanticConflict, match="canonical state_before"):
        await service._compute_morning_selection(TRADE_DATE)
    with pytest.raises(V20SemanticConflict, match="canonical state_before"):
        await service._build_late_0939_replay_semantic(
            context,
            AT_CUTOFF,
            replay_event_id="legacy-background-must-not-publish",
        )

    assert repository.alerts == alerts_before
    assert repository.alert_write_calls == 0
    assert repository.commit_entry_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption",
    ("wrong_trade_date", "wrong_slot", "malformed_policy", "unbound_current_state"),
)
async def test_legacy_missing_prestate_requires_every_surviving_binding(
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    service, repository, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    await service._run_decision_iteration_with_cutoff(JUST_BEFORE_CUTOFF)
    assert repository.status is not None
    snapshot = dict(repository.status.snapshot)
    snapshot.pop("state_before")
    if corruption == "malformed_policy":
        snapshot["policy_inputs"] = {"not": "the frozen policy contract"}
    repository.status = replace(
        repository.status,
        trade_date=(date(2026, 9, 2) if corruption == "wrong_trade_date" else TRADE_DATE),
        slot_id=("wrong-slot" if corruption == "wrong_slot" else repository.status.slot_id),
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
    )
    if corruption == "unbound_current_state":
        payload = {
            **dict(repository.state.payload),
            "last_terminal_slot_id": "unrelated-terminal-slot",
        }
        repository.state = replace(
            repository.state,
            state_hash=sha256_json(payload),
            payload=payload,
        )

    # The production query is keyed by date.  This permissive adapter lets the
    # service itself prove that a corrupt row cannot cross that repository seam.
    async def get_corrupt_status(_stream_id: str, _trade_date: date) -> Any:
        return repository.status

    monkeypatch.setattr(repository, "get_entry_status", get_corrupt_status)
    repository.seal_at = AT_CUTOFF

    with pytest.raises(V20SemanticConflict):
        await service.trigger_canonical_selection_check_only(
            f"manual-at-094000-legacy-binding-{corruption}",
            AT_CUTOFF,
        )

    assert repository.alert_write_calls == 0
    assert repository.commit_entry_calls == 1


@pytest.mark.asyncio
async def test_terminal_slot_identity_tampering_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    await service._run_decision_iteration_with_cutoff(JUST_BEFORE_CUTOFF)
    assert repository.status is not None

    repository.status = replace(
        repository.status,
        slot_id="tampered-terminal-slot",
    )
    repository.seal_at = AT_CUTOFF

    with pytest.raises(V20SemanticConflict, match="state transition"):
        await service.trigger_canonical_selection_check_only(
            "manual-at-094000-bad-terminal-slot",
            AT_CUTOFF,
        )

    assert repository.alert_write_calls == 0
    assert repository.commit_entry_calls == 1


@pytest.mark.asyncio
async def test_current_state_head_is_only_a_readonly_concurrency_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    await service._run_decision_iteration_with_cutoff(JUST_BEFORE_CUTOFF)
    assert repository.status is not None
    repository.seal_at = AT_CUTOFF
    original = service._orchestrate_morning_selection

    async def compute_then_advance(trade_date: date, **kwargs: Any) -> Any:
        calculation = await original(trade_date, **kwargs)
        advanced_payload = {
            **dict(repository.state.payload),
            "state_revision": repository.state.revision + 1,
        }
        repository.state = replace(
            repository.state,
            revision=repository.state.revision + 1,
            state_hash=sha256_json(advanced_payload),
            payload=advanced_payload,
        )
        return calculation

    monkeypatch.setattr(service, "_orchestrate_morning_selection", compute_then_advance)
    with pytest.raises(V20StateConflict, match="changed during"):
        await service.trigger_canonical_selection_check_only(
            "manual-at-094000-concurrent-state-advance",
            AT_CUTOFF,
        )

    assert repository.alert_write_calls == 0
    assert repository.commit_entry_calls == 1


@pytest.mark.asyncio
async def test_094000_manual_is_read_only_for_every_official_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, _ = _service_and_artifact(monkeypatch, now=AT_CUTOFF)
    state_before = repository.state
    status_before = repository.status

    result = await _dispatch_manual_trigger(service, "manual-at-094000-read-only")
    await _drain_mews_kicks(service)

    assert result["non_actionable"] is True
    assert result["official_state_changed"] is False
    assert result["orders_changed"] is False
    assert repository.state == state_before
    assert repository.status == status_before
    assert repository.commit is None
    assert repository.commit_entry_calls == 0
    assert repository.forbidden_write_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("contract", ("result_fields", "visible_message"))
async def test_successful_current_calculation_is_not_fail_when_old_official_differs(
    monkeypatch: pytest.MonkeyPatch,
    contract: str,
) -> None:
    automatic, automatic_repo, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    await automatic._run_decision_iteration_with_cutoff(JUST_BEFORE_CUTOFF)
    assert automatic_repo.status is not None

    check_only, check_repo, _ = _service_and_artifact(monkeypatch, now=AT_CUTOFF)
    old_v16_hash = "0" * 64
    old_semantic = {
        **dict(automatic_repo.status.semantic),
        "v16_snapshot_hash": old_v16_hash,
    }
    old_snapshot = {
        **dict(automatic_repo.status.snapshot),
        "v16_snapshot_hash": old_v16_hash,
    }
    check_repo.status = replace(
        automatic_repo.status,
        semantic=old_semantic,
        semantic_content_hash=sha256_json(old_semantic),
        snapshot=old_snapshot,
        snapshot_hash=sha256_json(old_snapshot),
    )
    check_repo.state = automatic_repo.state

    state_before = check_repo.state
    status_before = check_repo.status
    result = await _dispatch_manual_trigger(
        check_only,
        "manual-at-094000-old-official-diff",
    )
    await _drain_mews_kicks(check_only)
    alert = check_repo.alerts[result["operator_event_id"]]
    assert alert.payload is not None
    assert alert.semantic["v20_action"] in {"ENTER", "BLOCK", "NO_SIGNAL"}
    assert isinstance(alert.semantic["entry_render_semantic"], dict)

    if contract == "result_fields":
        assert result.get("calculation_result") == "SUCCESS"
        assert result.get("official_comparison_result") == "DIFFERENT"
        assert result.get("probe_result") != "FAIL"
        assert result.get("probe_mismatch_fields") == []
    else:
        assert "核查结论：FAIL" not in alert.payload["message"]
        assert "本次计算：成功" in alert.payload["message"]
        assert "与早盘正式结果对比：不一致" in alert.payload["message"]
        assert "failure_stage" not in alert.semantic
    assert check_repo.state == state_before
    assert check_repo.status == status_before
    assert check_repo.commit_entry_calls == 0


@pytest.mark.asyncio
async def test_current_code_output_change_is_successful_different_and_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [JUST_BEFORE_CUTOFF]
    service, repository, _ = _service_and_artifact(
        monkeypatch,
        now=JUST_BEFORE_CUTOFF,
    )
    service._clock = lambda: now[0]
    await service._run_decision_iteration_with_cutoff(JUST_BEFORE_CUTOFF)
    assert repository.status is not None
    assert repository.commit is not None

    official_status = repository.status
    official_commit = repository.commit
    official_state = repository.state
    original_prepare_entry = service_module.prepare_entry
    recomputed_next_hashes: list[str] = []

    def prepare_entry_with_revised_health_policy(*args: Any, **kwargs: Any) -> Any:
        """Model a deployed policy revision while consuming identical inputs."""

        assert kwargs["state"].revision == official_status.snapshot["state_before"]["revision"]
        assert kwargs["state"].state_hash == official_status.semantic["state_before_hash"]
        prepared = original_prepare_entry(*args, **kwargs)
        revised_health = {
            "schema_version": "v20-health-snapshot/v1",
            "status": "HEALTHY",
            "recovery_count": 0,
            "recent_valid": [
                {
                    "batch_id": "current-code-health-1",
                    "signal_date": "2026-08-20",
                    "t2_exit_date": "2026-08-24",
                    "relative_return": 0.01,
                },
                {
                    "batch_id": "current-code-health-2",
                    "signal_date": "2026-08-21",
                    "t2_exit_date": "2026-08-25",
                    "relative_return": 0.02,
                },
                {
                    "batch_id": "current-code-health-3",
                    "signal_date": "2026-08-24",
                    "t2_exit_date": "2026-08-26",
                    "relative_return": 0.03,
                },
            ],
            "last_processed_key": [
                "2026-08-26",
                "2026-08-24",
                "current-code-health-3",
            ],
        }
        next_state = {
            **dict(prepared.commit.next_state),
            "health": revised_health,
        }
        next_state_hash = sha256_json(next_state)
        semantic = {
            **dict(prepared.commit.semantic),
            "health_state": "HEALTHY",
            "health_recovery_count": 0,
            "health_trailing_mean": 0.02,
            "state_after_hash": next_state_hash,
        }
        recomputed_next_hashes.append(next_state_hash)
        return replace(
            prepared,
            commit=replace(
                prepared.commit,
                next_state=next_state,
                next_state_hash=next_state_hash,
                semantic=semantic,
                semantic_content_hash=sha256_json(semantic),
            ),
        )

    monkeypatch.setattr(
        service_module,
        "prepare_entry",
        prepare_entry_with_revised_health_policy,
    )
    now[0] = AT_CUTOFF
    repository.seal_at = AT_CUTOFF

    result = await _dispatch_manual_trigger(
        service,
        "manual-at-094000-current-code-health-change",
    )
    await _drain_mews_kicks(service)
    alert = repository.alerts[result["operator_event_id"]]
    recomputed = alert.semantic["entry_render_semantic"]

    assert recomputed_next_hashes
    assert recomputed_next_hashes[-1] != official_status.semantic["state_after_hash"]
    assert recomputed["state_after_hash"] == recomputed_next_hashes[-1]
    assert recomputed["health_state"] == "HEALTHY"
    assert recomputed["health_state"] != official_status.semantic["health_state"]
    assert result["calculation_result"] == "SUCCESS"
    assert result["official_comparison_result"] == "DIFFERENT"
    assert "health_state" in result["official_mismatch_fields"]
    assert result["probe_result"] == "PASS"
    assert result["probe_mismatch_fields"] == []
    assert repository.state == official_state
    assert repository.status == official_status
    assert repository.commit == official_commit
    assert repository.commit_entry_calls == 1
