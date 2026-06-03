# === MODULE PURPOSE ===
# Tests for the unified daily data-maintenance pipeline in CacheScheduler
# (docs/data-integrity-pipeline.md §4.1): ① load-tushare → ② kimi → ③ rebuild
# (全历史查漏) → ④ index-fill (补缺) → ⑤ rebuild touched dates (确认). Covers the
# orchestration order, failure isolation (③ fail ⇒ skip ④⑤), the ⑤-only-if-touched
# rule, the _bounded step wrapper, and the busy-guard. External steps are faked at
# their source modules (the pipeline re-imports them at call time).

from __future__ import annotations

import asyncio
from datetime import date

import pytest

from src.data.services import cache_scheduler as mod
from src.data.services.cache_scheduler import CacheScheduler


class _FakeStorage:
    is_ready = True

    def __init__(self):
        self.logged: list = []

    async def get_trading_calendar_summary(self):
        return {"by_daily_state": {"ok": 100, "source_none": 5}}

    async def get_calendar_problem_codes(self, states):
        return set()

    async def get_last_scheduler_run(self, name):
        return None

    async def log_scheduler_run(self, name, trigger, result, message):
        self.logged.append((trigger, result, message))


class _FakePipeline:
    def __init__(self, fill_result, calls):
        self._fill = fill_result
        self._calls = calls

    async def fill_daily_from_calendar(self, quiet=False):
        self._calls.append("fill")
        if isinstance(self._fill, Exception):
            raise self._fill
        return self._fill


class _FakeClient:
    def __init__(self, *a, **k):
        self.started = self.stopped = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True


class _FakeLV:
    """Stand-in ListingVerifyScheduler with just verify_unverified."""

    def __init__(self, result, calls):
        self._result = result
        self._calls = calls

    async def verify_unverified(self, quiet=False):
        self._calls.append("kimi")
        return self._result


class _AppState:
    def __init__(self, storage, pipeline, lv=None):
        self.storage = storage
        self.pipeline = pipeline
        self.listing_verify_scheduler = lv
        self.active_download = None


def _patch(monkeypatch, *, rebuild_results, kimi_ok=True):
    """Patch the external steps at their source modules; return the order list.

    kimi runs as step ② whenever a listing_verify_scheduler is present on app.state
    AND kimi_available() (kimi_ok) — there is no on/off toggle anymore.
    """
    calls: list[str] = []

    async def fake_load(storage, *, feishu, client=None, stop_storage=False):
        calls.append("load")
        return {
            "listed": 10,
            "delisted": 2,
            "total_entries": 12,
            "written": 12,
            "preserved_kimi": 1,
        }

    rb_iter = iter(rebuild_results)

    async def fake_rebuild(storage, *, trading_days=None, start=None, end=None, client=None):
        calls.append("rebuild_full" if trading_days is None else "rebuild_days")
        r = next(rb_iter)
        if isinstance(r, Exception):
            raise r
        return r

    async def fake_notify(msg):
        calls.append("notify")

    monkeypatch.setattr("scripts.load_listing_from_tushare.run_load_listing", fake_load)
    monkeypatch.setattr("src.data.services.trading_calendar.rebuild_calendar", fake_rebuild)
    monkeypatch.setattr(mod, "_notify_feishu", fake_notify)
    monkeypatch.setattr("src.common.config.get_tushare_token", lambda: "tok")
    monkeypatch.setattr("src.data.clients.tushare_realtime.TushareRealtimeClient", _FakeClient)
    monkeypatch.setattr("src.data.services.kimi_listing_verifier.kimi_available", lambda: kimi_ok)
    return calls


_RB_CLEAN = {"days": 800, "rows": 4_000_000, "problem_rows": 0, "by_state": {"ok": 4_000_000}}
_NO_GAPS = {"dates": 0, "filled": 0, "processed_dates": []}


@pytest.mark.asyncio
async def test_pipeline_happy_path_no_gaps_runs_in_order(monkeypatch):
    calls = _patch(monkeypatch, rebuild_results=[_RB_CLEAN])
    storage = _FakeStorage()
    lv = _FakeLV({"checked": 0, "verified": 0, "failed": 0, "remaining": 0, "findings": []}, calls)
    sched = CacheScheduler(_AppState(storage, _FakePipeline(_NO_GAPS, calls), lv=lv))
    result, message = await sched._run_pipeline("scheduled")
    assert result == "success"
    # order: ① load → ② kimi → ③ rebuild(full) → ④ fill; ⑤ skipped (no gaps to confirm)
    assert calls[:4] == ["load", "kimi", "rebuild_full", "fill"]
    assert "rebuild_days" not in calls
    assert "gap_report" not in calls  # legacy per-day diagnosis no longer auto-sent


@pytest.mark.asyncio
async def test_pipeline_fill_gaps_triggers_confirm_rebuild(monkeypatch):
    # ③ then ⑤ both rebuild → supply two results.
    calls = _patch(monkeypatch, rebuild_results=[_RB_CLEAN, _RB_CLEAN])
    fill = {"dates": 2, "filled": 50, "processed_dates": [date(2024, 1, 2), date(2024, 1, 3)]}
    storage = _FakeStorage()
    lv = _FakeLV({"checked": 0, "verified": 0, "failed": 0, "remaining": 0, "findings": []}, calls)
    sched = CacheScheduler(_AppState(storage, _FakePipeline(fill, calls), lv=lv))
    result, _ = await sched._run_pipeline("scheduled")
    assert result == "success"
    # ⑤ runs because ④ touched dates → a second rebuild over those days. No gap-report spam.
    assert calls == ["load", "kimi", "rebuild_full", "fill", "rebuild_days", "notify"]


@pytest.mark.asyncio
async def test_rebuild_failure_skips_fill_and_confirm(monkeypatch):
    calls = _patch(monkeypatch, rebuild_results=[RuntimeError("tushare down")])
    storage = _FakeStorage()
    sched = CacheScheduler(_AppState(storage, _FakePipeline(_NO_GAPS, calls)))
    result, message = await sched._run_pipeline("scheduled")
    assert result == "failed"
    assert "③" in message  # the rebuild step is flagged
    # ④ fill + ⑤ confirm must NOT run on a stale index.
    assert "fill" not in calls
    assert "rebuild_days" not in calls


@pytest.mark.asyncio
async def test_kimi_runs_when_enabled_and_available(monkeypatch):
    calls = _patch(monkeypatch, rebuild_results=[_RB_CLEAN])
    kimi_result = {
        "checked": 1,
        "verified": 1,
        "failed": 0,
        "remaining": 0,
        "findings": [
            {"code": "300114", "name": "中航成飞", "status": "更名换号", "note": "迁 302132"}
        ],
    }
    storage = _FakeStorage()
    lv = _FakeLV(kimi_result, calls)
    sched = CacheScheduler(_AppState(storage, _FakePipeline(_NO_GAPS, calls), lv=lv))
    result, _ = await sched._run_pipeline("scheduled")
    assert result == "success"
    # kimi (②) runs after load (①), before rebuild (③).
    assert calls.index("kimi") == 1
    assert calls.index("kimi") < calls.index("rebuild_full")


@pytest.mark.asyncio
async def test_kimi_tool_errors_surface_as_warning(monkeypatch):
    # Sub-threshold kimi tool/auth errors must NOT be hidden as plain 成功.
    calls = _patch(monkeypatch, rebuild_results=[_RB_CLEAN])
    kimi_result = {
        "checked": 3,
        "verified": 1,
        "failed": 0,
        "remaining": 0,
        "tool_errors": 2,
        "findings": [],
    }
    storage = _FakeStorage()
    lv = _FakeLV(kimi_result, calls)
    sched = CacheScheduler(_AppState(storage, _FakePipeline(_NO_GAPS, calls), lv=lv))
    result, message = await sched._run_pipeline("scheduled")
    # Pipeline still functionally succeeded, but the message flags the kimi warning
    # so the operator sees a partially-broken kimi night (not reported as clean).
    assert result == "success"
    assert "告警" in message and "② 核身份" in message


@pytest.mark.asyncio
async def test_bounded_isolates_timeout_and_errors():
    async def ok():
        return 42

    async def boom():
        raise ValueError("nope")

    async def slow():
        await asyncio.sleep(10)

    assert await CacheScheduler._bounded(ok(), 5) == (True, 42)
    good, msg = await CacheScheduler._bounded(boom(), 5)
    assert good is False and "nope" in msg
    good, msg = await CacheScheduler._bounded(slow(), 1)  # 1s timeout used as int → wait_for
    # 1//60 == 0 minutes in the message, but it must report a timeout (not crash).
    assert good is False and "超时" in msg


@pytest.mark.asyncio
async def test_run_once_defers_when_a_data_flag_is_set(monkeypatch):
    monkeypatch.setattr(mod, "_notify_feishu", _noop_notify)
    monkeypatch.setattr("src.common.config.get_cache_scheduler_enabled", lambda: True)
    storage = _FakeStorage()
    state = _AppState(storage, _FakePipeline(_NO_GAPS, []))
    state.calendar_rebuild_running = True  # a manual rebuild is in flight
    sched = CacheScheduler(state)
    await sched._run_once("scheduled")
    assert sched.last_run_result == "skipped"


async def _noop_notify(msg):
    return None
