"""Regression contract for the standalone V16 production runtime.

The reference behaviour is main commit ``4b88fd6``: V16 owns a simple,
independent scan loop.  A manual request is a real new scan, not a read from a
V20/canonical cache, and V20 lifecycle state is outside the V16 boundary.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from src.data.clients.tushare_realtime import TushareQuote
from src.web import app as web_app
from src.web import v15_scan_service
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
    """Two sequential production calls are two scans, as they were at 4b88fd6."""

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

    async def forbidden_canonical(*_args: Any, **_kwargs: Any):
        pytest.fail("standalone V16 must not enter a canonical coordinator")

    monkeypatch.setattr("src.strategy.lgbrank_scorer.LGBRankScorer", FakeScorer)
    monkeypatch.setattr("src.strategy.strategies.v16_scanner.V16Scanner", FakeScanner)
    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", fake_calendar)
    monkeypatch.setattr(v15_scan_service, "_notify_feishu_v16_top10", record_top10)
    monkeypatch.setattr(v15_scan_service, "_refresh_top10_names", no_refresh)
    monkeypatch.setattr(v15_scan_service, "get_or_compute_canonical_v16", forbidden_canonical)
    monkeypatch.setattr(v15_scan_service, "compute_canonical_v16_scan", forbidden_canonical)
    monkeypatch.setattr(v15_scan_service, "_fetch_prior_daily_once", forbidden_canonical)
    monkeypatch.setattr(
        "src.strategy.v16_day_gate_shadow.freeze_v16_day_gate_runtime",
        lambda *_args, **_kwargs: None,
    )

    forbidden_callback_calls: list[str] = []

    async def forbidden_callback(*_args: Any, **_kwargs: Any):
        forbidden_callback_calls.append("called")
        raise AssertionError("V16 must not call V20 artifact callbacks")

    state = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=FakeRealtime(),
        fundamentals_db=FakeFundamentals(),
        historical_adapter=FakeHistory(),
        concept_mapper=object(),
        stock_filter=object(),
        canonical_sink=forbidden_callback,
        canonical_artifact_probe=forbidden_callback,
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
        "early": 2,
        "prev_close": 2,
        "history": 2,
        "top10": 2,
    }
    assert early_requests == [(("600000",), None), (("600000",), None)]
    assert forbidden_callback_calls == []
    assert state.canonical_coordinator is None


@pytest.mark.asyncio
async def test_trigger_scan_returns_503_until_v16_resources_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
async def test_trigger_scan_calls_real_v16_entry_on_every_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = v15_scan_service.V15ScanState(initialized=True, scan_done_date="unchanged")
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
    assert state.scan_done_date == "unchanged"


class _FrozenSchedulerDatetime(datetime):
    value: datetime

    @classmethod
    def now(cls, tz=None):
        value = cls.value
        if tz is None:
            return value.replace(tzinfo=None)
        return value.astimezone(tz)


@pytest.mark.asyncio
async def test_v16_scheduler_starts_scan_at_0938(monkeypatch: pytest.MonkeyPatch) -> None:
    frozen = datetime(2026, 9, 3, 9, 38, tzinfo=v15_scan_service.BEIJING_TZ)
    _FrozenSchedulerDatetime.value = frozen
    calls = 0

    async def calendar():
        return [frozen.date()]

    async def run(state):
        nonlocal calls
        calls += 1
        assert state.scan_done_date == frozen.date().isoformat()
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
    assert state.today_recommendation is None
    assert state.scan_error is None


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
        scan_error="previous-error",
        canonical_artifact_probe=probe,
    )

    await v15_scan_service._scan_scheduler(state)

    assert probe_calls == 0
    assert state.scan_done_date == frozen.date().isoformat()
    assert state.today_recommendation is old_recommendation
    assert state.scan_error == "previous-error"


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
