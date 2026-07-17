"""Tests for V16Scanner Step 2 hot-board detection and multi-board attribution.

A stock can belong to several hot boards at once (board averages use ALL
constituents, so one board's movers can drag in a stock that barely moved
itself). `stock_all_boards` must retain every hot board a stock belongs to,
not just the single "best" one — this is what the daily report annotates.
"""

import pandas as pd
import pytest

from src.strategy.strategies.v16_scanner import V16Scanner, V16StockData


def _sd(code: str, open_price: float, price_940: float) -> V16StockData:
    return V16StockData(
        code=code,
        name=code,
        open_price=open_price,
        prev_close=open_price,
        price_940=price_940,
        high_940=price_940,
        low_940=open_price,
        volume_940=0,
        volume_937=0,
        avg_daily_volume=0,
        trend_5d=0,
        trend_10d=0,
        avg_daily_return_20d=0,
        volatility_20d=0,
        consecutive_up_days=0,
        history_df=pd.DataFrame(),
    )


class _StubFDB:
    async def batch_filter_st(self, codes):
        return codes

    async def batch_get_fundamentals(self, codes):
        return {}


def _scanner() -> V16Scanner:
    return V16Scanner(_StubFDB())


def test_hot_board_admits_all_constituents_not_just_gainers():
    scanner = _scanner()
    # avg = (3% + 3% + 0.3%) / 3 = 2.1% >= 0.8% threshold -> hot, ALL 3 admitted
    clean_boards = {"board_A": [("s1", "S1"), ("s2", "S2"), ("s3", "S3")]}
    stock_data = {
        "s1": _sd("s1", 10.0, 10.30),  # +3.0%, drove the average
        "s2": _sd("s2", 10.0, 10.30),  # +3.0%, drove the average
        "s3": _sd("s3", 10.0, 10.03),  # +0.3%, only admitted via board expansion
    }
    hot_boards, _stock_all_boards, _filtered, board_avg_gains, _all_gains = (
        scanner._step2_hot_boards(clean_boards, stock_data)
    )
    assert set(hot_boards["board_A"]) == {"s1", "s2", "s3"}
    assert board_avg_gains["board_A"] == pytest.approx(2.1, abs=1e-6)


def test_stock_all_boards_keeps_every_hot_board_a_stock_belongs_to():
    scanner = _scanner()
    clean_boards = {
        "board_A": [("s1", "S1"), ("s2", "S2"), ("s3", "S3")],
        "board_B": [("s1", "S1"), ("s4", "S4")],
        "board_C": [("s3", "S3"), ("s5", "S5")],  # avg (0.3%+0.2%)/2 -> not hot
    }
    stock_data = {
        "s1": _sd("s1", 10.0, 10.30),  # +3.0%
        "s2": _sd("s2", 10.0, 10.30),  # +3.0%
        "s3": _sd("s3", 10.0, 10.03),  # +0.3%
        "s4": _sd("s4", 10.0, 10.30),  # +3.0%
        "s5": _sd("s5", 10.0, 10.02),  # +0.2%
    }
    hot_boards, stock_all_boards, _filtered, _board_avg_gains, _all_gains = (
        scanner._step2_hot_boards(clean_boards, stock_data)
    )
    assert set(hot_boards.keys()) == {"board_A", "board_B"}  # board_C stays below threshold
    # s1 is a member of BOTH hot boards -> must retain both, not collapse to one
    assert set(stock_all_boards["s1"]) == {"board_A", "board_B"}
    # s3 is only in board_A (its board_C membership never qualifies as hot)
    assert stock_all_boards["s3"] == ["board_A"]
