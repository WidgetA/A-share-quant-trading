from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from src.data.clients.tushare_realtime import TushareEarlyMarketData, TushareMinuteBar, TushareQuote
from src.strategy.v20.models import HealthObservation, HealthSnapshot, HealthStatus
from src.strategy.v22_slim.policy import advance_health, close_risk_count, entry_gate, h90
from src.strategy.v22_slim.selection import exact_early


def test_partial_v20_weight_is_whole_assigned_sleeve_when_v22_opens():
    result = entry_gate(count=10, v20_weight=0.25, h90_block=False, risk_before=4, strong=False)
    assert result.final_open and result.multiplier == 1


def test_extra_block_does_not_manufacture_a_risk_loss():
    result = entry_gate(count=10, v20_weight=1, h90_block=False, risk_before=4, strong=True)
    assert result.normal_open and not result.final_open
    assert (
        close_risk_count(4, has_signal=True, normal_open=result.normal_open, reference_return=0.01)
        == 0
    )
    assert close_risk_count(4, has_signal=False, normal_open=False, reference_return=None) == 4


def test_h90_uses_same_stocks_for_each_window():
    # A disappearing large constituent cannot create a false market contraction.
    history = [{"600001": 100.0, "600002": 10000.0} for _ in range(20)]
    result = h90({"600001": 100.0}, history)
    assert not result["block"] and result["h20_ratio"] == 1
    assert h90({"600001": 90.0}, history)["block"]
    with pytest.raises(ValueError, match="twenty"):
        h90({"600001": 90.0}, history[:-1])


def observation(index, value):
    day = date(2026, 8, 1) + timedelta(days=index)
    return HealthObservation(str(index), day, day + timedelta(days=2), value)


def test_late_health_backfill_cannot_supply_three_recovery_confirmations_in_one_day():
    negative = tuple(observation(index, -0.1) for index in range(3))
    paused = HealthSnapshot(HealthStatus.PAUSED_R0, 0, negative, negative[-1].order_key)
    positive = [observation(index, 0.1) for index in range(3, 8)]
    result = advance_health(paused, positive, today=date(2026, 9, 1))
    assert result.status == HealthStatus.PAUSED_R1
    assert advance_health(result, positive, today=date(2026, 9, 2)) == result


def test_today_t2_close_cannot_enter_morning_health():
    future = observation(3, 0.1)
    with pytest.raises(ValueError, match="matured"):
        advance_health(HealthSnapshot(), [future], today=future.t2_exit_date)


def early_data(*, omit=None, add_auction=False, bad_amount=False):
    day = date(2026, 9, 7)
    tz = ZoneInfo("Asia/Shanghai")
    labels = list(range(30, 40)) + ([25] if add_auction else [])
    bars = tuple(
        TushareMinuteBar(
            "600001",
            datetime(2026, 9, 7, 9, minute, tzinfo=tz),
            f"09:{minute:02}",
            20.0,
            20.0,
            20.0,
            20.0,
            100.0,
            9000.0 if bad_amount and minute == 35 else 2000.0,
        )
        for minute in labels
        if minute != omit
    )
    return day, TushareEarlyMarketData(
        TushareQuote("600001", 20.0, 20.0, 20.0, 20.0, 100.0, 2000.0), bars, "a" * 64
    )


def test_exact_prefix_includes_0930_and_excludes_optional_0925():
    day, data = early_data(add_auction=True)
    output = exact_early(data, day)
    assert output is not None
    assert output.quote.early_volume == 1000
    assert output.quote.volume_937 == 800
    day, missing = early_data(omit=30)
    assert exact_early(missing, day) is None


def test_bad_volume_amount_pair_is_excluded_before_board_average():
    day, data = early_data(bad_amount=True)
    assert exact_early(data, day) is None
