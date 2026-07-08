# === MODULE PURPOSE ===
# Tests for TRD-001 equity history ledger reconstruction (equity_ledger.py):
# - reconstruct_equity(): 持仓倒推 / 逐日盈亏(手算数字校验)/ 负持仓截断 /
#   缺价近似 / 股息+逆回购收益 / 费用
# - build_reconstructed_history(): trade_notes + backtest_daily 假库端到端
#
# All expected numbers are hand-computed; comments show the arithmetic.

from __future__ import annotations

import asyncio
import calendar
from datetime import datetime, timedelta

from src.trading.equity_ledger import (
    build_reconstructed_history,
    is_non_stock_position_code,
    reconstruct_equity,
)

D1, D2, D3 = "2026-07-01", "2026-07-02", "2026-07-03"


def test_hold_through_no_trades():
    # 1000 股拿满三天:pnl(D3)=1000*(11-10.5)=500, pnl(D2)=1000*(10.5-10)=500
    r = reconstruct_equity(
        anchor_equity=110000.0,
        positions_at_axis_end={"000001": 1000},
        fills_by_date={},
        cash_events_by_date={},
        closes_by_code={"000001": [(D1, 10.0), (D2, 10.5), (D3, 11.0)]},
        axis=[D1, D2, D3],
    )
    assert r.truncated_at is None
    assert r.points == [
        {"date": D1, "total_asset": 109000.0},
        {"date": D2, "total_asset": 109500.0},
    ]


def test_buy_mid_window_with_fee():
    # D2 买 600 @10.2,佣金 5;之前持 400。
    # pnl(D3) = 1000*(11-10.5) = 500
    # pnl(D2) = [1000*10.5 - 400*10] - 600*10.2 - 5 = 6500 - 6120 - 5 = 375
    r = reconstruct_equity(
        anchor_equity=100000.0,
        positions_at_axis_end={"000001": 1000},
        fills_by_date={D2: [{"code": "000001", "side": "buy", "qty": 600, "price": 10.2}]},
        cash_events_by_date={D2: {"fees": 5.0, "dividend": 0.0, "repo_income": 0.0}},
        closes_by_code={"000001": [(D1, 10.0), (D2, 10.5), (D3, 11.0)]},
        axis=[D1, D2, D3],
    )
    assert r.points == [
        {"date": D1, "total_asset": 99125.0},  # 99500 - 375
        {"date": D2, "total_asset": 99500.0},  # 100000 - 500
    ]


def test_sell_all_with_tax_and_dividend():
    # D2 清仓 1000 @10.4,费 12,同日登记股息 50;D3 起空仓。
    # pnl(D3) = 0
    # pnl(D2) = [0 - 1000*10] + 10400 - 12 + 50 = 438
    r = reconstruct_equity(
        anchor_equity=100000.0,
        positions_at_axis_end={},
        fills_by_date={D2: [{"code": "000001", "side": "sell", "qty": 1000, "price": 10.4}]},
        cash_events_by_date={D2: {"fees": 12.0, "dividend": 50.0, "repo_income": 0.0}},
        closes_by_code={"000001": [(D1, 10.0), (D2, 10.4), (D3, 10.6)]},
        axis=[D1, D2, D3],
    )
    assert r.points == [
        {"date": D1, "total_asset": 99562.0},  # 100000 - 438
        {"date": D2, "total_asset": 100000.0},
    ]


def test_repo_income_day_without_fills():
    # D2 逆回购利息 30、手续费 1 → pnl(D2) += 29(空仓,无股票价差)
    r = reconstruct_equity(
        anchor_equity=50000.0,
        positions_at_axis_end={},
        fills_by_date={},
        cash_events_by_date={D2: {"fees": 1.0, "dividend": 0.0, "repo_income": 30.0}},
        closes_by_code={"000001": [(D1, 10.0), (D2, 10.0), (D3, 10.0)]},
        axis=[D1, D2, D3],
    )
    assert r.points == [
        {"date": D1, "total_asset": 49971.0},  # 50000 - 29
        {"date": D2, "total_asset": 50000.0},
    ]


def test_negative_position_truncates_with_warning():
    # 现在空仓,但 D2 有一笔买入 → 倒推 D1 收盘 = -1000,账本有洞 → 截断到 D2
    r = reconstruct_equity(
        anchor_equity=100000.0,
        positions_at_axis_end={},
        fills_by_date={D2: [{"code": "000001", "side": "buy", "qty": 1000, "price": 10.0}]},
        cash_events_by_date={},
        closes_by_code={"000001": [(D1, 10.0), (D2, 10.0), (D3, 10.0)]},
        axis=[D1, D2, D3],
    )
    assert r.truncated_at == D2
    assert r.points == [{"date": D2, "total_asset": 100000.0}]  # pnl(D3)=0
    assert any("000001" in w and "负" in w for w in r.warnings)


def test_missing_qty_truncates():
    r = reconstruct_equity(
        anchor_equity=100000.0,
        positions_at_axis_end={"000001": 1000},
        fills_by_date={D2: [{"code": "000001", "side": "buy", "qty": None, "price": 10.0}]},
        cash_events_by_date={},
        closes_by_code={"000001": [(D1, 10.0), (D2, 10.0), (D3, 10.0)]},
        axis=[D1, D2, D3],
    )
    assert r.truncated_at == D2
    assert any("缺股数" in w for w in r.warnings)


def test_missing_price_uses_close_and_warns():
    # D2 买入缺价格 → 按 D2 收盘 10.5 近似 → 该笔买卖价差计 0
    # pnl(D2) = [1000*10.5 - 400*10] - 600*10.5 = 200
    r = reconstruct_equity(
        anchor_equity=100000.0,
        positions_at_axis_end={"000001": 1000},
        fills_by_date={D2: [{"code": "000001", "side": "buy", "qty": 600, "price": None}]},
        cash_events_by_date={},
        closes_by_code={"000001": [(D1, 10.0), (D2, 10.5), (D3, 10.5)]},
        axis=[D1, D2, D3],
    )
    assert r.points[0]["total_asset"] == 99800.0  # 100000 - 0(pnl D3) - 200
    assert any("按当日收盘价近似" in w for w in r.warnings)


def test_suspended_day_forward_fills_close():
    # 000001 D2 停牌(无行)→ D2 用 D1 收盘补 → pnl(D2)=0, pnl(D3)=1000*(11-10)
    r = reconstruct_equity(
        anchor_equity=100000.0,
        positions_at_axis_end={"000001": 1000},
        fills_by_date={},
        cash_events_by_date={},
        closes_by_code={"000001": [(D1, 10.0), (D3, 11.0)]},
        axis=[D1, D2, D3],
    )
    assert r.points == [
        {"date": D1, "total_asset": 99000.0},
        {"date": D2, "total_asset": 99000.0},
    ]
    assert r.warnings == []  # 前向补价是正常口径,不告警


def test_non_stock_position_codes():
    assert is_non_stock_position_code("888880")
    assert is_non_stock_position_code("888880.SH")
    assert is_non_stock_position_code("204001")
    assert is_non_stock_position_code("131810.SZ")
    assert not is_non_stock_position_code("600000.SH")
    assert not is_non_stock_position_code("000001")


# ---------- orchestrator (fake storage) ----------


def _bj_ts_ms(date_str: str, hour_bj: int = 10) -> int:
    """北京时间某日 hour 点 → UTC epoch ms。"""
    y, m, d = (int(p) for p in date_str.split("-"))
    naive_utc = datetime(y, m, d, hour_bj) - timedelta(hours=8)
    return calendar.timegm(naive_utc.timetuple()) * 1000


def _day_ts_ms(date_str: str) -> int:
    """backtest_daily 的 ts:交易日 00:00 UTC。"""
    y, m, d = (int(p) for p in date_str.split("-"))
    return calendar.timegm(datetime(y, m, d).timetuple()) * 1000


def _note_row(date_str: str, code: str, side: str, qty: int, price: float, commission: float):
    return {
        "ts": _bj_ts_ms(date_str),
        "code": code,
        "event_id": f"broker_{code}_{date_str}",
        "event_type": "买入" if side == "buy" else "卖出",
        "event_source": "broker",
        "title": "",
        "price": price,
        "qty": qty,
        "side": side,
        "content": "",
        "content_external": "",
        "author": "system",
        "deleted": False,
        "commission": commission,
        "transfer_fee": None,
        "stamp_tax": None,
        "dividend": None,
        "realized_pnl": None,
        "repo_income": None,
    }


class _FakeStorage:
    """storage.db.fetch 供 TradeNoteStore 用;get_daily_for_code 供收盘价用。"""

    def __init__(self, note_rows: list[dict], daily: dict[str, list[tuple[str, float]]]):
        self._note_rows = note_rows
        self._daily = daily
        self.db = self

    async def fetch(self, sql: str):
        assert "trade_notes" in sql
        return self._note_rows

    async def get_daily_for_code(self, code: str, start: str, end: str):
        return [
            {"ts": _day_ts_ms(d), "close_price": c}
            for d, c in self._daily.get(code, [])
            if start <= d <= end
        ]


def test_build_reconstructed_history_end_to_end():
    # 07-04(周五)买 1000 @10.0 佣金 5;07-05/06 周末;锚点 07-07 快照 100000。
    # 收盘: 07-02 9.8 / 07-03 9.9 / 07-04 10.3 / 07-07 10.1
    # pnl(07-07) = 1000*(10.1-10.3) = -200          → E[07-04] = 100200
    # pnl(07-04) = [1000*10.3 - 0] - 10000 - 5 = 295 → E[07-03] = 99905
    # pnl(07-03) = 0(空仓)                          → E[07-02] = 99905
    storage = _FakeStorage(
        note_rows=[_note_row("2026-07-04", "000001", "buy", 1000, 10.0, 5.0)],
        daily={
            "000001": [
                ("2026-07-02", 9.8),
                ("2026-07-03", 9.9),
                ("2026-07-04", 10.3),
                ("2026-07-07", 10.1),
            ]
        },
    )
    r = asyncio.run(
        build_reconstructed_history(
            storage,
            positions_now={"000001": 1000},
            anchor_date="2026-07-07",
            anchor_equity=100000.0,
            today_bj="2026-07-08",
            window_start="2026-07-01",
        )
    )
    assert r.truncated_at is None
    assert r.points == [
        {"date": "2026-07-02", "total_asset": 99905.0},
        {"date": "2026-07-03", "total_asset": 99905.0},
        {"date": "2026-07-04", "total_asset": 100200.0},
    ]


def test_build_history_fills_after_anchor_rolled_back():
    # 锚点 07-07,今天 07-08 又买了 500 → 锚点日持仓 = 现在 1500 - 500 = 1000
    storage = _FakeStorage(
        note_rows=[
            _note_row("2026-07-04", "000001", "buy", 1000, 10.0, 5.0),
            _note_row("2026-07-08", "000001", "buy", 500, 10.2, 3.0),
        ],
        daily={
            "000001": [
                ("2026-07-03", 9.9),
                ("2026-07-04", 10.3),
                ("2026-07-07", 10.1),
            ]
        },
    )
    r = asyncio.run(
        build_reconstructed_history(
            storage,
            positions_now={"000001": 1500},
            anchor_date="2026-07-07",
            anchor_equity=100000.0,
            today_bj="2026-07-08",
            window_start="2026-07-01",
        )
    )
    # 与上例同构:E[07-04] = 100000 - 1000*(10.1-10.3) = 100200
    assert r.points[-1] == {"date": "2026-07-04", "total_asset": 100200.0}


def test_build_history_anchor_before_window_returns_empty():
    storage = _FakeStorage(note_rows=[], daily={})
    r = asyncio.run(
        build_reconstructed_history(
            storage,
            positions_now={},
            anchor_date="2026-06-01",
            anchor_equity=1.0,
            today_bj="2026-07-08",
            window_start="2026-07-01",
        )
    )
    assert r.points == [] and r.warnings == []
