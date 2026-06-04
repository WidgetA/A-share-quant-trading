# === MODULE PURPOSE ===
# Tests for the unified daily data-maintenance pipeline in CacheScheduler
# (docs/data-integrity-pipeline.md §4.1): ① load-tushare → ② kimi → ③ rebuild
# (增量查漏·含分钟状态) → ④ 补日线 → ⑤ 补分钟(带上限) → ⑥ rebuild touched dates (确认日线+分钟).
# Covers the orchestration order, failure isolation (③ fail ⇒ skip ④⑤⑥), the
# ⑥-only-if-touched rule, the truncated-minute warning, the _bounded step wrapper, and
# the busy-guard. External steps are faked at their source modules (the pipeline
# re-imports them at call time).

from __future__ import annotations

import asyncio
from datetime import date

import pytest

from src.data.services import cache_scheduler as mod
from src.data.services.cache_scheduler import CacheScheduler


class _FakeStorage:
    is_ready = True

    def __init__(self, max_date="2023-01-01"):
        self.logged: list = []
        self._max_date = max_date

    async def get_trading_calendar_summary(self):
        return {
            "by_daily_state": {"ok": 100, "source_none": 5},
            "max_date": self._max_date,
        }

    async def get_calendar_problem_codes(self, states):
        return set()

    async def get_last_scheduler_run(self, name):
        return None

    async def log_scheduler_run(self, name, trigger, result, message):
        self.logged.append((trigger, result, message))


_NO_MINUTE_GAPS = {
    "dates": 0,
    "filled": 0,
    "processed_dates": [],
    "source_short": {},
    "processed": 0,
    "truncated": False,
    "remaining": 0,
}


class _FakePipeline:
    def __init__(self, fill_result, calls, minute_result=None):
        self._fill = fill_result
        self._minute = minute_result if minute_result is not None else _NO_MINUTE_GAPS
        self._calls = calls

    async def fill_daily_from_calendar(self, quiet=False):
        self._calls.append("fill")
        if isinstance(self._fill, Exception):
            raise self._fill
        return self._fill

    async def fill_minute_from_calendar(self, quiet=False, max_codes=None):
        self._calls.append("fill_minute")
        if isinstance(self._minute, Exception):
            raise self._minute
        return self._minute


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

    async def fake_rebuild(
        storage,
        *,
        trading_days=None,
        start=None,
        end=None,
        client=None,
        with_minute=False,
        extra_minute_source_short=None,
    ):
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
    # ① load → ② kimi → ③ rebuild → ④ 补日线 → ⑤ 补分钟; ⑥ confirm skipped (nothing touched).
    assert calls[:5] == ["load", "kimi", "rebuild_full", "fill", "fill_minute"]
    assert "rebuild_days" not in calls  # ⑥ confirm skipped when nothing was filled


@pytest.mark.asyncio
async def test_pipeline_fill_gaps_triggers_confirm_rebuild(monkeypatch):
    # ③ then ⑥ both rebuild → supply two results.
    calls = _patch(monkeypatch, rebuild_results=[_RB_CLEAN, _RB_CLEAN])
    fill = {"dates": 2, "filled": 50, "processed_dates": [date(2024, 1, 2), date(2024, 1, 3)]}
    storage = _FakeStorage()
    lv = _FakeLV({"checked": 0, "verified": 0, "failed": 0, "remaining": 0, "findings": []}, calls)
    sched = CacheScheduler(_AppState(storage, _FakePipeline(fill, calls), lv=lv))
    result, _ = await sched._run_pipeline("scheduled")
    assert result == "success"
    # ④ touched dates (minute had none) → ⑥ confirm rebuilds over those days.
    assert calls == [
        "load",
        "kimi",
        "rebuild_full",
        "fill",
        "fill_minute",
        "rebuild_days",
        "notify",
    ]


@pytest.mark.asyncio
async def test_minute_fill_truncated_warns_and_confirms(monkeypatch):
    # ⑤ minute fill hit the nightly cap → reported as 警告 (visible, NOT silently
    # swallowed — CLAUDE.md §12) and ⑥ confirm still re-reconciles the touched days
    # (here minute days only; daily had no gaps). ③ + ⑥ → two rebuild results.
    calls = _patch(monkeypatch, rebuild_results=[_RB_CLEAN, _RB_CLEAN])
    minute = {
        "dates": 2,
        "filled": 9000,
        "processed_dates": [date(2024, 1, 2), date(2024, 1, 3)],
        "source_short": {date(2024, 1, 2): {"600000"}},
        "processed": 12000,
        "truncated": True,
        "remaining": 3000,
    }
    storage = _FakeStorage()
    lv = _FakeLV({"checked": 0, "verified": 0, "failed": 0, "remaining": 0, "findings": []}, calls)
    sched = CacheScheduler(
        _AppState(storage, _FakePipeline(_NO_GAPS, calls, minute_result=minute), lv=lv)
    )
    result, message = await sched._run_pipeline("scheduled")
    assert calls == [
        "load",
        "kimi",
        "rebuild_full",
        "fill",
        "fill_minute",
        "rebuild_days",
        "notify",
    ]
    # Truncated backlog surfaces as a warning; the pass still functionally succeeds.
    assert result == "success"
    assert "告警" in message and "⑤ 补分钟" in message


@pytest.mark.asyncio
async def test_confirm_runs_with_minute_only_over_minute_touched_days(monkeypatch):
    # ⑥ must NOT feed the (uncapped) daily-gap set into a with_minute=True confirm — each
    # such day costs a ~15-30s minute GROUP BY, so an unbounded daily backlog would blow
    # _STEP_TIMEOUT_CONFIRM and wedge the nightly. Only ⑤'s minute-touched days (bounded by
    # the cap) get with_minute=True; daily-only days confirm cheaply (with_minute=False).
    rebuild_calls: list[dict] = []

    async def fake_load(storage, *, feishu, client=None, stop_storage=False):
        return {"listed": 1, "delisted": 0, "total_entries": 1, "written": 1, "preserved_kimi": 0}

    rb_iter = iter([_RB_CLEAN, _RB_CLEAN, _RB_CLEAN])  # ③ + ⑥a(minute) + ⑥b(daily-only)

    async def fake_rebuild(
        storage,
        *,
        trading_days=None,
        start=None,
        end=None,
        client=None,
        with_minute=False,
        extra_minute_source_short=None,
    ):
        rebuild_calls.append(
            {
                "kind": "full" if trading_days is None else "days",
                "trading_days": trading_days,
                "with_minute": with_minute,
            }
        )
        return next(rb_iter)

    async def fake_notify(msg):
        pass

    monkeypatch.setattr("scripts.load_listing_from_tushare.run_load_listing", fake_load)
    monkeypatch.setattr("src.data.services.trading_calendar.rebuild_calendar", fake_rebuild)
    monkeypatch.setattr(mod, "_notify_feishu", fake_notify)
    monkeypatch.setattr("src.common.config.get_tushare_token", lambda: "tok")
    monkeypatch.setattr("src.data.clients.tushare_realtime.TushareRealtimeClient", _FakeClient)
    monkeypatch.setattr("src.data.services.kimi_listing_verifier.kimi_available", lambda: False)

    day_a, day_b = date(2024, 2, 1), date(2024, 2, 2)  # daily touches A, minute touches B
    fill = {"dates": 1, "filled": 10, "processed_dates": [day_a]}
    minute = {
        "dates": 1,
        "filled": 20,
        "processed_dates": [day_b],
        "source_short": {},
        "processed": 20,
        "truncated": False,
        "remaining": 0,
    }
    sched = CacheScheduler(_AppState(_FakeStorage(), _FakePipeline(fill, [], minute_result=minute)))
    result, _ = await sched._run_pipeline("scheduled")
    assert result == "success"
    confirms = [c for c in rebuild_calls if c["kind"] == "days"]
    assert len(confirms) == 2  # one minute-confirm + one daily-only-confirm (disjoint days)
    by_minute = {c["with_minute"]: c["trading_days"] for c in confirms}
    assert by_minute[True] == [day_b]  # minute-touched day → with_minute=True
    assert by_minute[False] == [day_a]  # daily-only day → cheap, with_minute=False


@pytest.mark.asyncio
async def test_rebuild_failure_skips_fill_and_confirm(monkeypatch):
    calls = _patch(monkeypatch, rebuild_results=[RuntimeError("tushare down")])
    storage = _FakeStorage()
    sched = CacheScheduler(_AppState(storage, _FakePipeline(_NO_GAPS, calls)))
    result, message = await sched._run_pipeline("scheduled")
    assert result == "failed"
    assert "③" in message  # the rebuild step is flagged
    # ④ 补日线 + ⑤ 补分钟 + ⑥ confirm must NOT run on a stale index.
    assert "fill" not in calls
    assert "fill_minute" not in calls
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
async def test_incremental_rebuild_starts_after_max_date(monkeypatch):
    # ③ must rebuild ONLY new days (max_date+1 .. T-1), trusting the materialized
    # historical index — never re-scan all history nightly.
    captured: dict = {}

    async def fake_load(storage, *, feishu, client=None, stop_storage=False):
        return {"listed": 1, "delisted": 0, "total_entries": 1, "written": 1, "preserved_kimi": 0}

    async def fake_rebuild(
        storage,
        *,
        trading_days=None,
        start=None,
        end=None,
        client=None,
        with_minute=False,
        extra_minute_source_short=None,
    ):
        if trading_days is None:
            captured["start"] = start
        return _RB_CLEAN

    async def fake_notify(msg):
        pass

    monkeypatch.setattr("scripts.load_listing_from_tushare.run_load_listing", fake_load)
    monkeypatch.setattr("src.data.services.trading_calendar.rebuild_calendar", fake_rebuild)
    monkeypatch.setattr(mod, "_notify_feishu", fake_notify)
    monkeypatch.setattr("src.common.config.get_tushare_token", lambda: "tok")
    monkeypatch.setattr("src.data.clients.tushare_realtime.TushareRealtimeClient", _FakeClient)
    monkeypatch.setattr("src.data.services.kimi_listing_verifier.kimi_available", lambda: False)

    storage = _FakeStorage(max_date="2024-01-10")
    sched = CacheScheduler(_AppState(storage, _FakePipeline(_NO_GAPS, [])))
    await sched._run_pipeline("scheduled")
    assert captured["start"] == date(2024, 1, 11)  # max_date + 1, NOT CACHE_START_DATE


@pytest.mark.asyncio
async def test_empty_truth_table_skips_rebuild_and_fill(monkeypatch):
    # No materialized index yet → do NOT auto-full-rebuild nightly (manual bootstrap).
    calls = _patch(monkeypatch, rebuild_results=[])
    storage = _FakeStorage(max_date=None)
    sched = CacheScheduler(_AppState(storage, _FakePipeline(_NO_GAPS, calls)))
    result, _ = await sched._run_pipeline("scheduled")
    assert "rebuild_full" not in calls  # no rebuild
    assert "fill" not in calls  # ④ skipped (no fresh index to fill against)
    assert result == "success"  # a warning, not a hard failure


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
