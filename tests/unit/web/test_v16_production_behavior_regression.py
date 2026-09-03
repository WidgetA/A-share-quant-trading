"""Regression contract for the standalone V16 production runtime.

The reference behaviour is main commit ``4b88fd6``: V16 owns a simple,
independent scan loop.  A manual request is a real new scan, not a read from a
V20/canonical cache, and V20 lifecycle state is outside the V16 boundary.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, time, timedelta
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from src.data.clients.tushare_realtime import TushareQuote
from src.web import app as web_app
from src.web import iquant_routes, v15_scan_service
from src.web.iquant_routes import create_iquant_router


def _route_app(scan_state: v15_scan_service.V15ScanState) -> FastAPI:
    router = create_iquant_router()
    router._inject_scan_state(scan_state)
    app = FastAPI()
    app.include_router(router)
    return app


async def _post_trigger(app: FastAPI) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        return await client.post("/api/iquant/trigger-scan")


@pytest.mark.asyncio
async def test_each_v16_run_reloads_providers_rescores_and_republishes_top10(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two scans rerun V16 while sharing their same-minute realtime acquisition."""

    today = datetime.now(v15_scan_service.BEIJING_TZ).date()
    counters = {
        "scorer": 0,
        "scanner": 0,
        "calendar": 0,
        "early": 0,
        "prev_close": 0,
        "history": 0,
        "top10": 0,
    }
    early_requests: list[tuple[tuple[str, ...], Any]] = []

    class FakeScorer:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            counters["scorer"] += 1
            self.model_sha256 = "m" * 64
            self.feature_list_sha256 = "f" * 64

    class FakeScanner:
        def __init__(self, **_kwargs: Any) -> None:
            counters["scanner"] += 1

        def get_universe(self):
            return {"board-a": [("600000", "浦发银行")]}, {"600000"}

        async def scan(self, stock_data, clean_boards):
            assert tuple(stock_data) == ("600000",)
            assert clean_boards == {"board-a": [("600000", "浦发银行")]}
            top1 = SimpleNamespace(
                code="600000",
                name="浦发银行",
                buy_price=10.20,
                score=0.12,
                rank=1,
            )
            return SimpleNamespace(
                recommended=[top1],
                all_scored=[top1],
                stock_best_board={"600000": "board-a"},
                stock_all_boards={"600000": ["board-a"]},
                step2_hot_board_count=1,
                final_candidates=1,
            )

    class FakeRealtime:
        async def batch_get_early_quotes(self, codes, expected_trade_date=None):
            counters["early"] += 1
            early_requests.append((tuple(codes), expected_trade_date))
            return {
                "600000": TushareQuote(
                    stock_code="600000",
                    open_price=10.0,
                    latest_price=10.2,
                    high_price=10.3,
                    low_price=9.9,
                    volume=10_000.0,
                    amount=102_000.0,
                    early_close=10.2,
                    early_high=10.3,
                    early_low=9.9,
                    early_volume=10_000.0,
                    volume_937=8_000.0,
                )
            }

        async def fetch_prev_closes(self, trade_date: str):
            counters["prev_close"] += 1
            assert len(trade_date) == 8
            # An out-of-universe row must not expand the V16 early-data request.
            return {"600000": 9.8, "000001": 11.0}

        async def batch_get_early_market_data(self, *_args: Any, **_kwargs: Any):
            pytest.fail("V16 production run must not enter the canonical/breadth provider")

        async def fetch_daily_bars(self, *_args: Any, **_kwargs: Any):
            pytest.fail("V16 must not require prior_amount/daily-bar canonical evidence")

        async def batch_get_early_minute_history_for_date(self, *_args: Any, **_kwargs: Any):
            pytest.fail("V16 current-day scan must not call stk_mins")

        async def batch_get_minute_history_for_date(self, *_args: Any, **_kwargs: Any):
            pytest.fail("V16 current-day scan must not call stk_mins")

    class FakeHistory:
        async def history_quotes(
            self,
            *,
            codes: str,
            indicators: str,
            start_date: str,
            end_date: str,
        ):
            counters["history"] += 1
            assert codes == "600000.SH"
            assert indicators == "open,high,low,close,volume"
            assert start_date < end_date
            dates = [(today - timedelta(days=40 - index)).isoformat() for index in range(40)]
            return {
                "tables": [
                    {
                        "thscode": "600000.SH",
                        "table": {
                            "time": dates,
                            "open": [10.0] * 40,
                            "high": [10.3] * 40,
                            "low": [9.7] * 40,
                            "close": [10.0] * 40,
                            "volume": [100_000.0] * 40,
                        },
                    }
                ]
            }

    class FakeFundamentals:
        async def batch_get_fundamentals(self, codes):
            return {code: SimpleNamespace(company_name="浦发银行") for code in codes}

        async def batch_current_names(self, codes):
            return {code: "浦发银行" for code in codes}

    async def fake_calendar():
        counters["calendar"] += 1
        return [today - timedelta(days=1), today]

    async def record_top10(_scan_result):
        counters["top10"] += 1

    async def no_refresh(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr("src.strategy.lgbrank_scorer.LGBRankScorer", FakeScorer)
    monkeypatch.setattr("src.strategy.strategies.v16_scanner.V16Scanner", FakeScanner)
    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", fake_calendar)
    monkeypatch.setattr(v15_scan_service, "_notify_feishu_v16_top10", record_top10)
    monkeypatch.setattr(v15_scan_service, "_refresh_top10_names", no_refresh)
    monkeypatch.setattr(
        "src.strategy.v16_day_gate_shadow.freeze_v16_day_gate_runtime",
        lambda *_args, **_kwargs: None,
    )

    state = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=FakeRealtime(),
        fundamentals_db=FakeFundamentals(),
        historical_adapter=FakeHistory(),
        concept_mapper=object(),
        stock_filter=object(),
    )

    first = await v15_scan_service.run_v16_scan(state)
    second = await v15_scan_service.run_v16_scan(state)

    assert first == second
    assert first == {
        "stock_code": "600000",
        "stock_name": "娴﹀彂閾惰",
        "board_name": "board-a",
        "open_price": 10.0,
        "prev_close": 9.8,
        "latest_price": 10.2,
        "lgb_score": 0.12,
        "hot_board_count": 1,
        "final_candidates": 1,
    } | {"stock_name": first["stock_name"]}
    assert first["stock_name"]
    assert counters == {
        "scorer": 2,
        "scanner": 2,
        "calendar": 2,
        "early": 1,
        "prev_close": 2,
        "history": 2,
        "top10": 2,
    }
    assert early_requests == [(("600000",), None)]
    assert not hasattr(state, "canonical_coordinator")
    assert not hasattr(state, "canonical_sink")
    assert not hasattr(state, "canonical_artifact_probe")


@pytest.mark.asyncio
async def test_trigger_scan_returns_503_until_v16_resources_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenSchedulerDatetime.value = datetime(2026, 9, 3, 15, 1, tzinfo=v15_scan_service.BEIJING_TZ)
    monkeypatch.setattr(iquant_routes, "datetime", _FrozenSchedulerDatetime)

    async def must_not_run(_state):
        pytest.fail("uninitialized trigger must not call the scanner")

    monkeypatch.setattr(v15_scan_service, "run_v16_scan", must_not_run)
    response = await _post_trigger(_route_app(v15_scan_service.V15ScanState()))

    assert response.status_code == 503
    assert response.json() == {"detail": "Scan resources not initialized yet"}


@pytest.mark.asyncio
async def test_trigger_scan_maps_each_scan_failure_to_500_without_overwriting_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenSchedulerDatetime.value = datetime(2026, 9, 3, 15, 1, tzinfo=v15_scan_service.BEIJING_TZ)
    monkeypatch.setattr(iquant_routes, "datetime", _FrozenSchedulerDatetime)

    old = {"stock_code": "old"}
    state = v15_scan_service.V15ScanState(
        initialized=True,
        today_recommendation=old,
        scan_done_date="2026-01-01",
        scan_error="old-error",
    )

    async def fail(_state):
        raise RuntimeError("provider exploded")

    monkeypatch.setattr(v15_scan_service, "run_v16_scan", fail)
    response = await _post_trigger(_route_app(state))

    assert response.status_code == 500
    assert response.json() == {"detail": "RuntimeError: provider exploded"}
    assert state.today_recommendation is old
    assert state.scan_done_date == "2026-01-01"
    assert state.scan_error == "old-error"


@pytest.mark.asyncio
async def test_after_hours_trigger_scan_runs_fresh_on_each_sequential_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenSchedulerDatetime.value = datetime(2026, 9, 3, 15, 1, tzinfo=v15_scan_service.BEIJING_TZ)
    monkeypatch.setattr(iquant_routes, "datetime", _FrozenSchedulerDatetime)

    state = v15_scan_service.V15ScanState(
        initialized=True,
        scan_done_date="unchanged",
        auto_scan_missed_date="2026-09-03",
        scan_error="automatic slot missed",
    )
    calls = 0

    async def rescan(received_state):
        nonlocal calls
        assert received_state is state
        calls += 1
        return {"stock_code": f"run-{calls}"}

    monkeypatch.setattr(v15_scan_service, "run_v16_scan", rescan)
    app = _route_app(state)

    first = await _post_trigger(app)
    second = await _post_trigger(app)

    assert first.status_code == second.status_code == 200
    assert first.json() == {"success": True, "recommendation": {"stock_code": "run-1"}}
    assert second.json() == {"success": True, "recommendation": {"stock_code": "run-2"}}
    assert calls == 2
    assert state.today_recommendation == {"stock_code": "run-2"}
    assert state.scan_done_date == "2026-09-03"
    assert state.scan_published_date == "2026-09-03"
    assert state.auto_scan_missed_date == ""
    assert state.scan_error is None


class _FrozenSchedulerDatetime(datetime):
    value: datetime

    @classmethod
    def now(cls, tz=None):
        value = cls.value
        if tz is None:
            return value.replace(tzinfo=None)
        return value.astimezone(tz)


@pytest.mark.asyncio
async def test_v16_early_quote_deadline_cancels_and_settles_blocked_fanout() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    class BlockingRealtime:
        async def batch_get_early_quotes(self, _codes):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    deadline = datetime.now(v15_scan_service.BEIJING_TZ) + timedelta(milliseconds=50)
    with pytest.raises(TimeoutError, match="did not settle before the 09:39 cutoff"):
        await v15_scan_service._load_v16_early_quotes(
            v15_scan_service.V15ScanState(),
            BlockingRealtime(),
            ["600000"],
            realtime_deadline=deadline,
        )

    assert started.is_set()
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_v16_early_quote_deadline_preserves_provider_timeout_detail() -> None:
    class FailingRealtime:
        async def batch_get_early_quotes(self, _codes):
            raise TimeoutError("provider-native-timeout")

    deadline = datetime.now(v15_scan_service.BEIJING_TZ) + timedelta(seconds=1)
    with pytest.raises(TimeoutError, match="provider-native-timeout"):
        await v15_scan_service._load_v16_early_quotes(
            v15_scan_service.V15ScanState(),
            FailingRealtime(),
            ["600000"],
            realtime_deadline=deadline,
        )


@pytest.mark.asyncio
async def test_v16_early_quotes_are_reused_only_within_provider_minute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenSchedulerDatetime.value = datetime(
        2026, 9, 3, 15, 1, 10, tzinfo=v15_scan_service.BEIJING_TZ
    )
    monkeypatch.setattr(v15_scan_service, "datetime", _FrozenSchedulerDatetime)
    calls = 0

    class Realtime:
        async def batch_get_early_quotes(self, _codes):
            nonlocal calls
            calls += 1
            return {"call": calls}

    state = v15_scan_service.V15ScanState()
    realtime = Realtime()
    first = await v15_scan_service._load_v16_early_quotes(
        state,
        realtime,
        ["600000"],
        realtime_deadline=None,
    )
    second = await v15_scan_service._load_v16_early_quotes(
        state,
        realtime,
        ["600000"],
        realtime_deadline=None,
    )
    _FrozenSchedulerDatetime.value = datetime(2026, 9, 3, 15, 2, tzinfo=v15_scan_service.BEIJING_TZ)
    third = await v15_scan_service._load_v16_early_quotes(
        state,
        realtime,
        ["600000"],
        realtime_deadline=None,
    )

    assert first == second == {"call": 1}
    assert third == {"call": 2}
    assert calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", ("targets", "client"))
async def test_v16_early_quotes_reject_same_minute_context_conflict_without_refetch(
    monkeypatch: pytest.MonkeyPatch,
    conflict: str,
) -> None:
    _FrozenSchedulerDatetime.value = datetime(
        2026, 9, 3, 15, 1, 10, tzinfo=v15_scan_service.BEIJING_TZ
    )
    monkeypatch.setattr(v15_scan_service, "datetime", _FrozenSchedulerDatetime)
    calls = {"first": 0, "second": 0}

    class Realtime:
        def __init__(self, label: str) -> None:
            self.label = label

        async def batch_get_early_quotes(self, _codes):
            calls[self.label] += 1
            return {"source": self.label}

    state = v15_scan_service.V15ScanState()
    first_client = Realtime("first")
    second_client = Realtime("second")
    assert await v15_scan_service._load_v16_early_quotes(
        state,
        first_client,
        ["000001", "600000"],
        realtime_deadline=None,
    ) == {"source": "first"}

    conflicting_client = second_client if conflict == "client" else first_client
    conflicting_targets = ["000001", "600001"] if conflict == "targets" else ["600000", "000001"]
    with pytest.raises(RuntimeError, match="same-minute V16 early acquisition conflicts"):
        await v15_scan_service._load_v16_early_quotes(
            state,
            conflicting_client,
            conflicting_targets,
            realtime_deadline=None,
        )

    assert calls == {"first": 1, "second": 0}


@pytest.mark.asyncio
async def test_v16_early_quote_failure_is_not_retried_in_same_provider_minute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FrozenSchedulerDatetime.value = datetime(
        2026, 9, 3, 9, 38, 10, tzinfo=v15_scan_service.BEIJING_TZ
    )
    monkeypatch.setattr(v15_scan_service, "datetime", _FrozenSchedulerDatetime)
    calls = 0

    class FailingRealtime:
        async def batch_get_early_quotes(self, _codes):
            nonlocal calls
            calls += 1
            raise RuntimeError("same-attempt-failure")

    state = v15_scan_service.V15ScanState()
    realtime = FailingRealtime()
    for _ in range(2):
        with pytest.raises(RuntimeError, match="same-attempt-failure"):
            await v15_scan_service._load_v16_early_quotes(
                state,
                realtime,
                ["600000"],
                realtime_deadline=None,
            )

    assert calls == 1


@pytest.mark.asyncio
async def test_v16_cleanup_cancels_and_clears_early_quote_attempt() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    class BlockingRealtime:
        async def batch_get_early_quotes(self, _codes):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    state = v15_scan_service.V15ScanState()
    waiter = asyncio.create_task(
        v15_scan_service._load_v16_early_quotes(
            state,
            BlockingRealtime(),
            ["600000"],
            realtime_deadline=None,
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1)

    await v15_scan_service.cleanup_scan_resources(state)
    await asyncio.gather(waiter, return_exceptions=True)

    assert cancelled.is_set()
    assert state.early_quotes_key is None
    assert state.early_quotes_targets is None
    assert state.early_quotes_client is None
    assert state.early_quotes_task is None


@pytest.mark.asyncio
@pytest.mark.parametrize("blocked_wall", (time(9, 39), time(9, 44, 59)))
async def test_v16_manual_trigger_rejects_v20_critical_window_without_scan(
    monkeypatch: pytest.MonkeyPatch,
    blocked_wall: time,
) -> None:
    _FrozenSchedulerDatetime.value = datetime.combine(
        datetime(2026, 9, 3).date(),
        blocked_wall,
        tzinfo=v15_scan_service.BEIJING_TZ,
    )
    monkeypatch.setattr(iquant_routes, "datetime", _FrozenSchedulerDatetime)
    calls = 0

    async def must_not_run(_state, **_kwargs):
        nonlocal calls
        calls += 1
        return None

    monkeypatch.setattr(v15_scan_service, "run_v16_scan", must_not_run)
    state = v15_scan_service.V15ScanState(initialized=True)
    response = await _post_trigger(_route_app(state))

    assert response.status_code == 409
    assert response.json() == {
        "detail": "V16 manual scan is unavailable during the V20 09:39-09:45 window"
    }
    assert calls == 0
    assert state.scan_flight_task is None


@pytest.mark.asyncio
async def test_v16_manual_before_0939_passes_absolute_realtime_cutoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 9, 3, 9, 38, 30, tzinfo=v15_scan_service.BEIJING_TZ)
    _FrozenSchedulerDatetime.value = now
    monkeypatch.setattr(iquant_routes, "datetime", _FrozenSchedulerDatetime)
    deadlines: list[datetime | None] = []

    async def run(_state, *, realtime_deadline=None):
        deadlines.append(realtime_deadline)
        return None

    monkeypatch.setattr(v15_scan_service, "run_v16_scan", run)
    response = await _post_trigger(_route_app(v15_scan_service.V15ScanState(initialized=True)))

    assert response.status_code == 200
    assert deadlines == [
        datetime.combine(
            now.date(),
            time(9, 39),
            tzinfo=v15_scan_service.BEIJING_TZ,
        )
    ]


@pytest.mark.asyncio
async def test_v16_automatic_and_manual_overlap_join_one_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frozen = datetime(2026, 9, 3, 9, 38, 10, tzinfo=v15_scan_service.BEIJING_TZ)
    _FrozenSchedulerDatetime.value = frozen
    monkeypatch.setattr(v15_scan_service, "datetime", _FrozenSchedulerDatetime)
    monkeypatch.setattr(iquant_routes, "datetime", _FrozenSchedulerDatetime)
    real_sleep = asyncio.sleep
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0
    deadlines: list[datetime | None] = []

    async def calendar():
        return [frozen.date()]

    async def run(state, *, realtime_deadline=None):
        nonlocal calls
        calls += 1
        deadlines.append(realtime_deadline)
        assert state.scan_done_date == ""
        assert state.today_recommendation is None
        started.set()
        await release.wait()
        return None

    async def end_scheduler_after_iteration(seconds):
        if seconds in (15, 30):
            raise asyncio.CancelledError
        await real_sleep(seconds)

    async def ignore_notification(*_args: Any, **_kwargs: Any):
        return None

    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", calendar)
    monkeypatch.setattr(v15_scan_service, "run_v16_scan", run)
    monkeypatch.setattr(v15_scan_service, "_notify_feishu_error", ignore_notification)
    monkeypatch.setattr(v15_scan_service, "_notify_feishu_signal", ignore_notification)
    monkeypatch.setattr(v15_scan_service.asyncio, "sleep", end_scheduler_after_iteration)
    state = v15_scan_service.V15ScanState(initialized=True)
    scheduler = asyncio.create_task(v15_scan_service._scan_scheduler(state))
    await asyncio.wait_for(started.wait(), timeout=1)

    manual = asyncio.create_task(_post_trigger(_route_app(state)))
    for _ in range(5):
        await real_sleep(0)
    assert not manual.done()
    assert calls == 1

    release.set()
    response, _ = await asyncio.gather(manual, scheduler)

    assert response.status_code == 200
    assert response.json() == {"success": True, "recommendation": None}
    assert calls == 1
    assert deadlines == [
        datetime.combine(
            frozen.date(),
            time(9, 39),
            tzinfo=v15_scan_service.BEIJING_TZ,
        )
    ]
    assert state.scan_flight_task is None


@pytest.mark.asyncio
async def test_v16_singleflight_waiter_cancellation_does_not_cancel_shared_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    underlying_cancelled = False

    async def run(_state, **_kwargs):
        nonlocal underlying_cancelled
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            underlying_cancelled = True
            raise
        return {"stock_code": "600000"}

    monkeypatch.setattr(v15_scan_service, "run_v16_scan", run)
    state = v15_scan_service.V15ScanState(initialized=True)
    cancelled_waiter = asyncio.create_task(v15_scan_service.run_v16_scan_singleflight(state))
    surviving_waiter = asyncio.create_task(v15_scan_service.run_v16_scan_singleflight(state))
    await asyncio.wait_for(started.wait(), timeout=1)

    cancelled_waiter.cancel()
    cancelled_result = (await asyncio.gather(cancelled_waiter, return_exceptions=True))[0]
    assert isinstance(cancelled_result, asyncio.CancelledError)
    assert not underlying_cancelled
    assert state.scan_flight_task is not None

    release.set()
    assert await surviving_waiter == {"stock_code": "600000"}
    assert state.scan_flight_task is None


@pytest.mark.asyncio
async def test_v16_cleanup_settles_scan_before_closing_realtime_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    events: list[str] = []

    async def run(_state, **_kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            events.append("scan-settled")

    class Realtime:
        async def stop(self):
            events.append("client-stopped")

    monkeypatch.setattr(v15_scan_service, "run_v16_scan", run)
    state = v15_scan_service.V15ScanState(initialized=True, realtime_client=Realtime())
    waiter = asyncio.create_task(v15_scan_service.run_v16_scan_singleflight(state))
    await asyncio.wait_for(started.wait(), timeout=1)

    await v15_scan_service.cleanup_scan_resources(state)
    waiter_result = (await asyncio.gather(waiter, return_exceptions=True))[0]

    assert isinstance(waiter_result, asyncio.CancelledError)
    assert events == ["scan-settled", "client-stopped"]
    assert state.scan_flight_task is None
    assert state.realtime_client is None
    assert state.initialized is False


@pytest.mark.asyncio
async def test_v16_cold_start_ready_at_0939_marks_missed_without_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trade_date = datetime(2026, 9, 3).date()
    _FrozenSchedulerDatetime.value = datetime(
        2026, 9, 3, 9, 38, 59, tzinfo=v15_scan_service.BEIJING_TZ
    )
    calls = 0
    init_calls = 0

    async def initialize(state):
        nonlocal init_calls
        init_calls += 1
        state.initialized = True
        _FrozenSchedulerDatetime.value = datetime(
            2026, 9, 3, 9, 39, tzinfo=v15_scan_service.BEIJING_TZ
        )

    async def calendar():
        return [trade_date]

    async def must_not_run(_state, **_kwargs):
        nonlocal calls
        calls += 1
        return None

    async def cancel_after_iteration(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(v15_scan_service, "datetime", _FrozenSchedulerDatetime)
    monkeypatch.setattr(v15_scan_service, "init_scan_resources", initialize)
    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", calendar)
    monkeypatch.setattr(v15_scan_service, "run_v16_scan", must_not_run)
    monkeypatch.setattr(v15_scan_service.asyncio, "sleep", cancel_after_iteration)
    stale_recommendation = {"stock_code": "previous-day"}
    state = v15_scan_service.V15ScanState(
        today_recommendation=stale_recommendation,
        scan_done_date="2026-09-02",
        scan_error="previous-error",
    )

    await v15_scan_service._scan_scheduler(state)

    assert init_calls == 1
    assert calls == 0
    assert state.scan_done_date == "2026-09-02"
    assert state.auto_scan_missed_date == trade_date.isoformat()
    assert state.today_recommendation is None
    assert state.scan_error == "V16 automatic 09:38 scan window missed for 2026-09-03"


@pytest.mark.asyncio
@pytest.mark.parametrize("guard", ("missed", "in_flight", "unpublished"))
async def test_v16_trade_consumer_rejects_stale_recommendation(
    monkeypatch: pytest.MonkeyPatch,
    guard: str,
) -> None:
    """Missed, in-flight, and legacy-unpublished state cannot release a stale BUY."""

    frozen = datetime(2026, 9, 3, 9, 40, tzinfo=iquant_routes.BEIJING_TZ)
    _FrozenSchedulerDatetime.value = frozen
    monkeypatch.setattr(iquant_routes, "datetime", _FrozenSchedulerDatetime)

    async def stop_after_one_iteration(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(iquant_routes.asyncio, "sleep", stop_after_one_iteration)
    stale_recommendation = {
        "stock_code": "previous-day",
        "stock_name": "stale",
        "board_name": "stale",
        "latest_price": 1.0,
        "lgb_score": 1.0,
    }
    state = v15_scan_service.V15ScanState(
        initialized=True,
        today_recommendation=stale_recommendation,
        # Deliberately simulate the dangerous marker left by an older process.
        scan_done_date=frozen.date().isoformat(),
    )
    blocked_flight: asyncio.Task[None] | None = None
    if guard == "missed":
        state.auto_scan_missed_date = frozen.date().isoformat()
    elif guard == "in_flight":
        blocked_flight = asyncio.create_task(asyncio.Event().wait())
        state.scan_flight_task = blocked_flight

    router = create_iquant_router()
    router._inject_scan_state(state)
    try:
        await router._trading_scheduler()
        pending_endpoint = next(
            route.endpoint
            for route in router.routes
            if getattr(route, "path", "") == "/api/iquant/pending-signals"
        )
        assert await pending_endpoint() == {"signals": [], "count": 0}
    finally:
        if blocked_flight is not None:
            blocked_flight.cancel()
            await asyncio.gather(blocked_flight, return_exceptions=True)


@pytest.mark.asyncio
async def test_v16_miss_then_0945_manual_scan_publishes_only_fresh_buy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trade_date = datetime(2026, 9, 3).date()
    _FrozenSchedulerDatetime.value = datetime(2026, 9, 3, 9, 40, tzinfo=v15_scan_service.BEIJING_TZ)
    monkeypatch.setattr(v15_scan_service, "datetime", _FrozenSchedulerDatetime)
    monkeypatch.setattr(iquant_routes, "datetime", _FrozenSchedulerDatetime)

    async def calendar():
        return [trade_date]

    async def stop_scheduler(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", calendar)
    monkeypatch.setattr(v15_scan_service.asyncio, "sleep", stop_scheduler)
    stale = {"stock_code": "previous-day"}
    state = v15_scan_service.V15ScanState(
        initialized=True,
        today_recommendation=stale,
        scan_done_date="2026-09-02",
        scan_error="previous-error",
    )
    await v15_scan_service._scan_scheduler(state)
    assert state.today_recommendation is None
    assert state.auto_scan_missed_date == trade_date.isoformat()

    fresh = {
        "stock_code": "600000",
        "stock_name": "fresh",
        "board_name": "fresh-board",
        "latest_price": 10.0,
        "lgb_score": 0.2,
    }

    async def manual_scan(_state, **_kwargs):
        return fresh

    _FrozenSchedulerDatetime.value = datetime(2026, 9, 3, 9, 45, tzinfo=v15_scan_service.BEIJING_TZ)
    monkeypatch.setattr(v15_scan_service, "run_v16_scan", manual_scan)
    router = create_iquant_router()
    router._inject_scan_state(state)
    app = FastAPI()
    app.include_router(router)
    response = await _post_trigger(app)

    assert response.status_code == 200
    assert response.json() == {"success": True, "recommendation": fresh}
    assert state.today_recommendation == fresh
    assert state.scan_error is None
    assert state.scan_done_date == trade_date.isoformat()
    assert state.scan_published_date == trade_date.isoformat()
    assert state.auto_scan_missed_date == ""

    iterations = 0

    async def stop_after_two_iterations(_seconds):
        nonlocal iterations
        iterations += 1
        if iterations >= 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(iquant_routes.asyncio, "sleep", stop_after_two_iterations)
    await router._trading_scheduler()
    pending_endpoint = next(
        route.endpoint
        for route in router.routes
        if getattr(route, "path", "") == "/api/iquant/pending-signals"
    )
    pending = await pending_endpoint()

    assert pending["count"] == 1
    assert [signal["stock_code"] for signal in pending["signals"]] == ["600000"]


@pytest.mark.asyncio
async def test_v16_scheduler_starts_scan_at_0938(monkeypatch: pytest.MonkeyPatch) -> None:
    frozen = datetime(2026, 9, 3, 9, 38, tzinfo=v15_scan_service.BEIJING_TZ)
    _FrozenSchedulerDatetime.value = frozen
    calls = 0

    async def calendar():
        return [frozen.date()]

    async def run(state, *, realtime_deadline=None):
        nonlocal calls
        calls += 1
        assert state.scan_done_date == ""
        assert realtime_deadline == datetime.combine(
            frozen.date(),
            time(9, 39),
            tzinfo=v15_scan_service.BEIJING_TZ,
        )
        return None

    async def cancel_after_iteration(_seconds):
        raise asyncio.CancelledError

    async def ignore_notification(*_args: Any, **_kwargs: Any):
        return None

    monkeypatch.setattr(v15_scan_service, "datetime", _FrozenSchedulerDatetime)
    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", calendar)
    monkeypatch.setattr(v15_scan_service, "run_v16_scan", run)
    monkeypatch.setattr(v15_scan_service, "_notify_feishu_error", ignore_notification)
    monkeypatch.setattr(v15_scan_service.asyncio, "sleep", cancel_after_iteration)
    state = v15_scan_service.V15ScanState(initialized=True)

    await v15_scan_service._scan_scheduler(state)

    assert calls == 1
    assert state.scan_done_date == frozen.date().isoformat()
    assert state.scan_published_date == frozen.date().isoformat()
    assert state.today_recommendation is None
    assert state.scan_error is None
    assert state.auto_scan_missed_date == ""


@pytest.mark.asyncio
async def test_v16_late_scheduler_preserves_already_published_manual_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Calendar/init lag must not erase a completed 09:45 manual recovery."""

    frozen = datetime(2026, 9, 3, 9, 45, tzinfo=v15_scan_service.BEIJING_TZ)
    _FrozenSchedulerDatetime.value = frozen

    async def calendar():
        return [frozen.date()]

    async def must_not_run(_state, **_kwargs):
        pytest.fail("late scheduler must not start an automatic V16 scan")

    async def stop_after_iteration(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(v15_scan_service, "datetime", _FrozenSchedulerDatetime)
    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", calendar)
    monkeypatch.setattr(v15_scan_service, "run_v16_scan", must_not_run)
    monkeypatch.setattr(v15_scan_service.asyncio, "sleep", stop_after_iteration)
    fresh = {"stock_code": "600000"}
    state = v15_scan_service.V15ScanState(
        initialized=True,
        today_recommendation=fresh,
        scan_done_date=frozen.date().isoformat(),
        scan_published_date=frozen.date().isoformat(),
        scan_error=None,
    )

    await v15_scan_service._scan_scheduler(state)

    assert state.today_recommendation is fresh
    assert state.scan_done_date == frozen.date().isoformat()
    assert state.scan_published_date == frozen.date().isoformat()
    assert state.auto_scan_missed_date == ""
    assert state.scan_error is None


@pytest.mark.asyncio
async def test_v16_late_scheduler_rejects_legacy_today_marker_without_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An old pre-completion date marker cannot masquerade as a fresh result."""

    frozen = datetime(2026, 9, 3, 9, 45, tzinfo=v15_scan_service.BEIJING_TZ)
    _FrozenSchedulerDatetime.value = frozen

    async def calendar():
        return [frozen.date()]

    async def stop_after_iteration(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(v15_scan_service, "datetime", _FrozenSchedulerDatetime)
    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", calendar)
    monkeypatch.setattr(v15_scan_service.asyncio, "sleep", stop_after_iteration)
    stale = {"stock_code": "previous-day"}
    state = v15_scan_service.V15ScanState(
        initialized=True,
        today_recommendation=stale,
        # The former scheduler wrote this before computation completed.
        scan_done_date=frozen.date().isoformat(),
        scan_published_date="",
        scan_error=None,
    )

    await v15_scan_service._scan_scheduler(state)

    assert state.today_recommendation is None
    assert state.scan_done_date == frozen.date().isoformat()
    assert state.scan_published_date == ""
    assert state.auto_scan_missed_date == frozen.date().isoformat()
    assert state.scan_error == "V16 automatic 09:38 scan window missed for 2026-09-03"


@pytest.mark.asyncio
async def test_v16_scheduler_never_probes_v20_artifacts_after_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frozen = datetime(2026, 9, 3, 10, 1, tzinfo=v15_scan_service.BEIJING_TZ)
    _FrozenSchedulerDatetime.value = frozen
    probe_calls = 0

    async def calendar():
        return [frozen.date()]

    async def probe(_trade_date):
        nonlocal probe_calls
        probe_calls += 1
        return None

    async def cancel_after_iteration(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(v15_scan_service, "datetime", _FrozenSchedulerDatetime)
    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", calendar)
    monkeypatch.setattr(v15_scan_service.asyncio, "sleep", cancel_after_iteration)
    old_recommendation = {"stock_code": "previous-day"}
    state = v15_scan_service.V15ScanState(
        initialized=True,
        today_recommendation=old_recommendation,
        scan_done_date="2026-09-02",
        scan_error="previous-error",
    )

    await v15_scan_service._scan_scheduler(state)

    assert probe_calls == 0
    assert not hasattr(state, "canonical_artifact_probe")
    assert not hasattr(state, "canonical_coordinator")
    assert state.scan_done_date == "2026-09-02"
    assert state.auto_scan_missed_date == frozen.date().isoformat()
    assert state.today_recommendation is None
    assert state.scan_error == "V16 automatic 09:38 scan window missed for 2026-09-03"


@pytest.mark.asyncio
async def test_v20_failure_cannot_bind_or_mutate_v16_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The V20 lifecycle must not receive the process-owned V16 state object."""

    class FailingV20:
        config = SimpleNamespace(enabled=True, deployment_mode="forward_shadow")

        def __init__(self) -> None:
            self.bound_state = None
            self.own_state = v15_scan_service.V15ScanState(
                today_recommendation={"stock_code": "v20-only"},
                scan_error="v20-error",
            )

        def bind_shared_v15_scan_state(self, state) -> None:
            self.bound_state = state

        async def start(self) -> None:
            if self.bound_state is not None:
                self.bound_state.today_recommendation = {"stock_code": "corrupted-by-v20"}
                self.bound_state.scan_done_date = "2099-01-01"
                self.bound_state.scan_error = "corrupted-by-v20"
            raise RuntimeError("v20 failed")

        async def stop(self) -> None:
            return None

    service = FailingV20()
    app = web_app.create_app(v20_service=service)
    state = app.state.v15_scan_state
    original_recommendation = {"stock_code": "v16"}
    state.today_recommendation = original_recommendation
    state.scan_done_date = "2026-09-03"
    state.scan_error = None
    app.state.iquant_router._start_monitoring = lambda: None
    started_states: list[Any] = []

    monkeypatch.setattr(web_app, "_schedule_v20_shadow_retry", lambda *_args: None)
    monkeypatch.setattr(web_app, "start_scan_scheduler", started_states.append)

    await web_app._start_strategy_services(app)

    assert service.bound_state is None
    assert state.today_recommendation is original_recommendation
    assert state.scan_done_date == "2026-09-03"
    assert state.scan_error is None
    assert started_states == [state]
    assert service.own_state.today_recommendation == {"stock_code": "v20-only"}
    assert service.own_state.scan_error == "v20-error"
