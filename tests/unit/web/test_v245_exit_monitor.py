from contextlib import asynccontextmanager
from datetime import date, time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.unit.web.test_v22_exit_alerts import DAY, bar, monitor


def subject(delivery=None):
    result = monitor(None)

    @asynccontextmanager
    async def acquire():
        yield SimpleNamespace(fetchrow=AsyncMock(return_value=delivery))

    result.service._repository.pool = SimpleNamespace(acquire=acquire)
    result.service._repository.seal_event = AsyncMock()
    result.service.config.strategy_version = "V24.5"
    result.store.apply = AsyncMock()
    result.store.apply_switch = AsyncMock(return_value="switch-event")
    return result


def holding(**changes):
    return (
        dict(
            position_id="p1",
            code="000001",
            entry_date=str(DAY),
            entry_price=None,
            strategy_version="V24.5",
            asset_kind="STOCK",
            source_event_id="recommendation",
            reference_clock=None,
            revision=0,
            quantity=None,
            status="MONITORING",
        )
        | changes
    )


def calendar():
    return tuple(date(2026, 9, d) for d in range(7, 12))


async def test_reference_automatically_uses_first_complete_open_after_confirmed_delivery():
    result = subject(dict(delivery_status="SENT", delivered_at=bar("09:41").at.replace(second=30)))
    result.bars = AsyncMock(return_value=[bar("09:41", 10), bar("09:42", 11)])
    p = holding()
    await result.position(p, calendar(), bar("10:00").at)
    result.store.apply.assert_awaited_once_with(p, reference=11, reference_clock=time(9, 42))


@pytest.mark.parametrize(
    "delivery",
    [
        None,
        dict(delivery_status="DELIVERY_UNKNOWN", delivered_at=None),
        dict(delivery_status="SENT", delivered_at=bar("15:01").at),
    ],
)
async def test_no_delivery_or_postclose_delivery_never_backfills_0941_reference(delivery):
    result = subject(delivery)
    result.bars = AsyncMock(return_value=[bar("09:41", 10), bar("15:00", 11)])
    await result.position(holding(), calendar(), bar("15:10").at)
    result.store.apply.assert_not_awaited()


async def test_switch_still_checks_old_stock_even_if_r33_alert_was_already_sent():
    result = subject()
    p = holding(
        entry_date="2026-09-07",
        alert_event_id="old-r33",
        switch_target="BANK",
        current_target="BANK",
        switch_alert_event_id=None,
    )
    result.bars = AsyncMock(return_value=[bar("09:41", 10)])
    result.limits = AsyncMock(return_value={"down_limit": 9})
    await result.switch_position(p, calendar(), bar("09:42").at)
    call = result.store.apply_switch.call_args
    assert call.args == (p,)
    assert call.kwargs["trigger_price"] == 10
    assert call.kwargs["quote_source"] == "rt_min_daily"
    result.service._repository.seal_event.assert_awaited_once()


async def test_switch_quote_at_upper_limit_can_sell_but_lower_limit_waits():
    result = subject()
    p = holding(
        entry_date="2026-09-07",
        switch_target="BANK",
        current_target="BANK",
        switch_alert_event_id=None,
    )
    result.bars = AsyncMock(return_value=[bar("09:41", 9)])
    result.limits = AsyncMock(return_value={"down_limit": 9, "up_limit": 11})
    await result.switch_position(p, calendar(), bar("09:42").at)
    result.store.apply_switch.assert_not_awaited()
    result.bars.return_value = [bar("09:42", 11)]
    await result.switch_position(p, calendar(), bar("09:43").at)
    assert result.store.apply_switch.call_args.kwargs["trigger_price"] == 11


async def test_t1_and_opposite_target_do_not_generate_switch_alert():
    result = subject()
    result.bars = AsyncMock()
    for p in [
        holding(switch_target="BANK", current_target="BANK"),
        holding(
            entry_date="2026-09-07",
            asset_kind="BANK",
            code="601398",
            switch_target="STOCKS",
            current_target="BANK",
        ),
    ]:
        await result.switch_position(p, calendar(), bar("09:42").at)
    result.bars.assert_not_awaited()
    result.store.apply_switch.assert_not_awaited()


async def test_bank_never_reaches_r33_but_pending_switch_runs_without_any_active_stock():
    result = subject()
    result.store.list = AsyncMock(return_value=[])
    bank = holding(
        code="601398",
        asset_kind="BANK",
        entry_date="2026-09-07",
        switch_target="STOCKS",
        current_target="STOCKS",
    )
    result.store.pending_switches = AsyncMock(return_value=[bank])
    result.service._load_trade_calendar = AsyncMock(return_value=calendar())
    result.switch_position = AsyncMock()
    result.position = AsyncMock()
    await result.run(bar("09:42").at)
    result.switch_position.assert_awaited_once()
    result.position.assert_not_awaited()


async def test_current_switch_has_precedence_over_ordinary_r33_for_same_stock():
    result = subject()
    stock = holding(entry_date="2026-09-07", switch_target="BANK", current_target="BANK")
    result.store.list = AsyncMock(return_value=[stock])
    result.store.pending_switches = AsyncMock(return_value=[stock])
    result.service._load_trade_calendar = AsyncMock(return_value=calendar())
    result.switch_position = AsyncMock()
    result.position = AsyncMock()
    await result.run(bar("09:42").at)
    result.switch_position.assert_awaited_once()
    result.position.assert_not_awaited()


async def test_switch_cannot_use_quote_from_before_target_was_available():
    result = subject()
    p = holding(
        entry_date="2026-09-07",
        switch_target="BANK",
        current_target="BANK",
        current_target_available_at=bar("09:41").at.replace(second=30).isoformat(),
    )
    result.bars = AsyncMock(return_value=[bar("09:41", 10)])
    result.limits = AsyncMock(return_value={"down_limit": 9})
    await result.switch_position(p, calendar(), bar("09:42").at)
    result.store.apply_switch.assert_not_awaited()
    result.bars.return_value = [bar("09:42", 10)]
    await result.switch_position(p, calendar(), bar("09:43").at)
    result.store.apply_switch.assert_awaited_once()


async def test_new_phase_rechecks_quote_after_old_alert_was_permanently_suppressed():
    result = subject()
    p = holding(
        entry_date="2026-09-07",
        switch_target="BANK",
        current_target="BANK",
        switch_alert_event_id="old-alert",
        phase_event_id="old-phase",
        current_phase_event_id="new-phase",
        switch_suppression_reason="V245_TARGET_SUPERSEDED",
        switch_delivery_status="PENDING",
    )
    result.bars = AsyncMock(return_value=[bar("09:42", 10)])
    result.limits = AsyncMock(return_value={"down_limit": 9})
    await result.switch_position(p, calendar(), bar("09:43").at)
    result.store.apply_switch.assert_awaited_once()
    assert result.service._repository.seal_event.call_args.args[0] == "switch-event"
