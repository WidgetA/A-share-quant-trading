"""Independent release checks for the replacement, not a strategy backtest.

Real service entry points run against the existing isolated fixture. These
checks cannot replace real PostgreSQL or a current trading-day delivery.
"""

from dataclasses import replace
from datetime import date, timedelta
from unittest.mock import AsyncMock

import pytest

from src.web.v20_service import _DayContext
from tests.unit.web.test_v22_non_trading_day import _service


def _v245_service(monkeypatch, day, *, open_day=False):
    service, repository, now = _service(monkeypatch, day, open_day=open_day)
    service.config = replace(service.config, strategy_version="V24.5")
    return service, repository, now


@pytest.mark.parametrize("day", [date(2026, 11, 15), date(2026, 10, 1)])
async def test_new_version_calendar_stops_both_entry_points_before_market(monkeypatch, day):
    service, repository, now = _v245_service(monkeypatch, day)
    service._run_entry_collection_cycle = AsyncMock()
    service._run_entry_cycle = AsyncMock()
    service._run_live_exit_tick = AsyncMock()
    service._execute_selection_task = AsyncMock(return_value=(None, False))
    service._seal_pending_outbox = AsyncMock()
    await service.run_once(now)
    result = await service.trigger_morning_selection("v245-release-closed")
    assert result["selection_version"] == "V24.5"
    assert result["trade_date"] == day.isoformat()
    assert result["cycle_result"] == "NON_TRADING_DAY"
    assert result["calculation_result"] == "NOT_RUN"
    assert result["delivery_status"] == "NOT_REQUIRED"
    assert result["symbols"] == []
    assert result["task_success"] is False
    assert result["official_state_changed"] is False
    service._execute_selection_task.assert_not_awaited()
    service._run_entry_collection_cycle.assert_not_awaited()
    service._run_entry_cycle.assert_not_awaited()
    service._run_live_exit_tick.assert_not_awaited()
    assert repository.commit_entry_calls == 0


async def test_new_version_direct_shared_task_cannot_bypass_closure(monkeypatch):
    service, _, now = _v245_service(monkeypatch, date(2026, 11, 15))
    service._orchestrate_morning_selection = AsyncMock()
    service.kick_mews_for_selection_trigger = lambda *_: pytest.fail("closed day fetched MEWS")
    result = await service._execute_selection_task(
        _DayContext(trade_date=now.date(), calendar=()), now, request_id="v245-direct-closed"
    )
    assert result == (None, False)
    service._orchestrate_morning_selection.assert_not_awaited()


async def test_unknown_calendar_does_not_invent_closed_day_or_enter(monkeypatch):
    service, _, now = _v245_service(monkeypatch, date(2026, 11, 16), open_day=True)
    service._calendar_provider = AsyncMock(side_effect=RuntimeError("calendar unavailable"))
    service._execute_selection_task = AsyncMock()
    with pytest.raises(RuntimeError, match="calendar unavailable"):
        await service.trigger_morning_selection("v245-calendar-missing")
    service._execute_selection_task.assert_not_awaited()


async def test_open_day_timer_and_new_button_use_same_complete_task(monkeypatch):
    service, _, now = _v245_service(monkeypatch, date(2026, 11, 16), open_day=True)
    service._run_entry_collection_cycle = AsyncMock()
    service._execute_selection_task = AsyncMock(return_value=(None, False))
    await service.run_once(now, include_exit_cycles=False, include_outbox_recovery=False)
    await service.trigger_morning_selection("v245-fresh-button")
    assert [c.kwargs["request_id"] for c in service._execute_selection_task.await_args_list] == [
        "scheduled",
        "v245-fresh-button",
    ]


async def test_nontrading_cache_does_not_hide_next_current_session(monkeypatch):
    service, _, sunday = _v245_service(monkeypatch, date(2026, 11, 15))
    monday = date(2026, 11, 16)
    service._calendar_provider = AsyncMock(
        return_value=[date(2026, 11, 13), monday, date(2026, 11, 17), date(2026, 11, 18)]
    )
    assert await service._is_non_trading_market_day(sunday)
    assert not await service._is_non_trading_market_day(sunday + timedelta(days=1))
    assert service._calendar_provider.await_count == 2


async def test_closed_day_live_exit_lane_is_also_current_date_guarded(monkeypatch):
    service, _, now = _v245_service(monkeypatch, date(2026, 11, 15))
    service._run_exit_cycle = AsyncMock()
    service._run_v22_exit_tick = AsyncMock()
    await service._run_live_exit_tick(_DayContext(trade_date=now.date(), calendar=()), now)
    service._run_exit_cycle.assert_not_awaited()
    service._run_v22_exit_tick.assert_not_awaited()


def test_v245_config_keeps_original_signal_state_identity(monkeypatch):
    from src.strategy.v20.runtime_config import load_v20_runtime_config
    from tests.unit.web.test_v20_auto_manual_exact_parity_acceptance import PROJECT_ROOT

    for key in ("V20_ENABLED", "V20_MODE", "V20_ALLOW_PRODUCTION_PUSH"):
        monkeypatch.delenv(key, raising=False)
    original = load_v20_runtime_config(PROJECT_ROOT, PROJECT_ROOT / "config/v22-slim.yaml")
    current = load_v20_runtime_config(PROJECT_ROOT, PROJECT_ROOT / "config/v24-5.yaml")
    assert current.strategy_version == "V24.5"
    for field in (
        "official_stream_id",
        "state_lineage_id",
        "return_profile_id",
        "reference_profile_id",
        "deployment_mode",
    ):
        assert getattr(current, field) == getattr(original, field)


def test_new_runtime_reads_authentic_old_terminal_without_rewriting_hash(monkeypatch):
    from tests.unit.web.test_v20_service import _entry_status

    service, _, _ = _v245_service(monkeypatch, date(2026, 11, 16), open_day=True)
    original = replace(service.config, strategy_version="V22-slim")
    terminal = _entry_status(original)
    old_hash = terminal.semantic_content_hash
    service._verify_entry_binding(terminal)
    assert terminal.strategy_version == "V22-slim"
    assert terminal.semantic_content_hash == old_hash
