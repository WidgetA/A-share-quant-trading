"""2026-11-09: exchange closures must never start market-data work."""

from dataclasses import replace
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from src.strategy.v20.runtime_config import load_v20_runtime_config
from src.web.v20_service import _DayContext
from tests.unit.web.test_v20_auto_manual_exact_parity_acceptance import (
    PROJECT_ROOT,
    TZ,
    _service_and_artifact,
)


def _service(monkeypatch, day, *, open_day=False):
    now = datetime(day.year, day.month, day.day, 10, tzinfo=TZ)
    service, repository, _ = _service_and_artifact(monkeypatch, now=now)
    service.config = replace(
        load_v20_runtime_config(PROJECT_ROOT, PROJECT_ROOT / "config/v22-slim.yaml"),
        enabled=True,
    )
    repository.bind_lineage(service.config.state_lineage_id)
    calendar = [day - timedelta(days=10), day + timedelta(days=10), day + timedelta(days=11)]
    if open_day:
        calendar.insert(1, day)
    service._calendar_provider = AsyncMock(return_value=calendar)
    return service, repository, now


@pytest.mark.parametrize("day", [date(2026, 11, 7), date(2026, 11, 8), date(2026, 10, 1)])
async def test_closed_day_repeated_timer_and_manual_never_start_market_work(monkeypatch, day):
    service, repository, now = _service(monkeypatch, day)
    service._run_entry_collection_cycle = AsyncMock()
    service._run_entry_cycle = AsyncMock()
    service._run_live_exit_tick = AsyncMock()
    service._execute_selection_task = AsyncMock(return_value=(None, False))
    service._seal_pending_outbox = AsyncMock()
    for offset in (0, 1, 90, 600):
        current = now + timedelta(minutes=offset)
        service._clock = lambda: current
        await service.run_once(current)
        result = await service.trigger_morning_selection(f"closed-{offset}")
        assert result["cycle_result"] == "NON_TRADING_DAY"
        assert result["symbols"] == []
        assert result["trade_date"] == day.isoformat()
        assert result["delivery_status"] == "NOT_REQUIRED"
    service._run_entry_collection_cycle.assert_not_awaited()
    service._run_entry_cycle.assert_not_awaited()
    service._run_live_exit_tick.assert_not_awaited()
    service._execute_selection_task.assert_not_awaited()
    assert repository.commit_entry_calls == 0
    assert service._calendar_provider.await_count == 1
    assert service._seal_pending_outbox.await_count == 4


async def test_closed_day_shared_task_stops_before_history_and_realtime(monkeypatch):
    service, _, now = _service(monkeypatch, date(2026, 10, 1))
    service._orchestrate_morning_selection = AsyncMock()
    service.kick_mews_for_selection_trigger = lambda *_: pytest.fail("closed day kicked MEWS")
    record, created = await service._execute_selection_task(
        _DayContext(trade_date=now.date(), calendar=()), now, request_id="closed-direct"
    )
    assert record is None and not created
    service._orchestrate_morning_selection.assert_not_awaited()


async def test_closed_day_independent_live_exit_lane_never_fetches(monkeypatch):
    service, _, now = _service(monkeypatch, date(2026, 10, 1))
    service._run_exit_cycle = AsyncMock()
    service._run_v22_exit_tick = AsyncMock()
    await service._run_live_exit_tick(_DayContext(trade_date=now.date(), calendar=()), now)
    service._run_exit_cycle.assert_not_awaited()
    service._run_v22_exit_tick.assert_not_awaited()


async def test_unknown_calendar_is_failure_not_closed_day(monkeypatch):
    service, _, now = _service(monkeypatch, date(2026, 10, 1))
    service._calendar_provider = AsyncMock(side_effect=RuntimeError("calendar unavailable"))
    service._run_live_exit_tick = AsyncMock()
    for run in (
        lambda: service.run_once(now),
        lambda: service.trigger_morning_selection("unknown-calendar"),
    ):
        with pytest.raises(RuntimeError, match="calendar unavailable"):
            await run()
    service._run_live_exit_tick.assert_not_awaited()


async def test_closed_day_stale_exit_and_rolling_recovery_skip_data(monkeypatch):
    service, repository, now = _service(monkeypatch, date(2026, 10, 1))
    repository.list_active_legs = AsyncMock(return_value=[])
    service._recover_closed_exit_windows = AsyncMock()
    service._run_stale_exit_cycle = AsyncMock()
    await service._run_closed_and_stale_exit_cycle(
        _DayContext(trade_date=now.date(), calendar=()), now
    )
    service._recover_closed_exit_windows.assert_not_awaited()
    service._run_stale_exit_cycle.assert_not_awaited()
    assert not await service._rolling7_automatic_recovery_allowed(now)


async def test_closed_wall_day_mews_cannot_bypass_with_historical_source(monkeypatch):
    service, _, now = _service(monkeypatch, date(2026, 10, 1))
    service._restore_mews_cache_once = AsyncMock()
    service._calculate_mews_once = AsyncMock()
    assert not await service._mews_singleflight_attempt(
        now - timedelta(days=1), stage="D2_EXIT", source_trade_date=now.date() - timedelta(days=2)
    )
    service._restore_mews_cache_once.assert_not_awaited()
    service._calculate_mews_once.assert_not_awaited()


async def test_closed_day_cache_does_not_skip_next_open_day(monkeypatch):
    service, _, now = _service(monkeypatch, date(2026, 11, 8))
    monday = date(2026, 11, 9)
    service._calendar_provider = AsyncMock(
        return_value=[date(2026, 11, 6), monday, date(2026, 11, 10), date(2026, 11, 11)]
    )
    assert await service._is_non_trading_market_day(now)
    assert not await service._is_non_trading_market_day(now + timedelta(days=1))
    assert service._calendar_provider.await_count == 2


async def test_open_day_keeps_timer_and_manual_complete_task(monkeypatch):
    service, _, now = _service(monkeypatch, date(2026, 11, 9), open_day=True)
    service._run_entry_collection_cycle = AsyncMock()
    service._execute_selection_task = AsyncMock(return_value=(None, False))
    await service.run_once(now, include_exit_cycles=False, include_outbox_recovery=False)
    await service.trigger_morning_selection("open-day")
    assert service._execute_selection_task.await_count == 2
    assert [c.kwargs["request_id"] for c in service._execute_selection_task.await_args_list] == [
        "scheduled",
        "open-day",
    ]
