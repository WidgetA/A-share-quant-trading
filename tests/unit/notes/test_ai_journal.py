# Tests for NOTE-002 AI 交易日志代写 — 纯函数部分:
#   build_fifo_ledger  — FIFO 买卖腿配对(成本/买入日/费用分摊)
#   classify_event     — 策略票判定(在榜单才代写;不在榜单/无数据 → 手动清单)
#   build_exemplars    — 手写范文抽取(排除 AI 写的)
# kimi spawn / storage IO 不在此覆盖(线上跑批自身有守卫 + trace)。

from datetime import datetime, timezone

from src.notes.ai_journal import (
    TOP_LIST_SIZE,
    build_exemplars,
    build_fifo_ledger,
    classify_event,
)
from src.notes.note_store import NoteEvent


def _ev(
    event_id: str,
    event_type: str,
    code: str = "002407",
    ts: datetime | None = None,
    price: float | None = 10.0,
    qty: int | None = 100,
    commission: float | None = 1.0,
    content: str = "",
    content_external: str = "",
    author: str = "system",
) -> NoteEvent:
    return NoteEvent(
        ts=ts or datetime(2026, 6, 3, 1, 39, 0, tzinfo=timezone.utc),
        code=code,
        event_id=event_id,
        event_type=event_type,
        source="broker",
        title="",
        price=price,
        qty=qty,
        side="buy" if event_type == "买入" else "sell",
        content=content,
        content_external=content_external,
        author=author,
        deleted=False,
        commission=commission,
        transfer_fee=None,
        stamp_tax=None,
        dividend=None,
        realized_pnl=None,
        repo_income=None,
    )


class TestFifoLedger:
    def test_single_buy_full_sell(self):
        buy = _ev(
            "b1",
            "买入",
            ts=datetime(2026, 6, 3, 1, 39, tzinfo=timezone.utc),
            price=20.0,
            qty=1000,
            commission=5.0,
        )
        sell = _ev(
            "s1", "卖出", ts=datetime(2026, 6, 5, 3, 0, tzinfo=timezone.utc), price=21.0, qty=1000
        )
        ledger = build_fifo_ledger([buy, sell])
        leg = ledger["s1"]
        assert leg["cost_avg"] == 20.0
        assert leg["matched_qty"] == 1000
        assert leg["unmatched_qty"] == 0
        assert leg["buy_dates"] == ["2026-06-03"]  # 北京时区日历日
        assert leg["buy_fee_alloc"] == 5.0

    def test_partial_sell_allocates_fees_proportionally(self):
        buy = _ev("b1", "买入", price=20.0, qty=1000, commission=10.0)
        sell = _ev(
            "s1", "卖出", ts=datetime(2026, 6, 4, 3, 0, tzinfo=timezone.utc), price=21.0, qty=400
        )
        ledger = build_fifo_ledger([buy, sell])
        assert ledger["s1"]["buy_fee_alloc"] == 4.0  # 10 * 400/1000

    def test_fifo_across_two_lots(self):
        b1 = _ev(
            "b1",
            "买入",
            ts=datetime(2026, 6, 1, 2, 0, tzinfo=timezone.utc),
            price=10.0,
            qty=100,
            commission=1.0,
        )
        b2 = _ev(
            "b2",
            "买入",
            ts=datetime(2026, 6, 2, 2, 0, tzinfo=timezone.utc),
            price=20.0,
            qty=100,
            commission=1.0,
        )
        sell = _ev(
            "s1", "卖出", ts=datetime(2026, 6, 3, 3, 0, tzinfo=timezone.utc), price=30.0, qty=150
        )
        ledger = build_fifo_ledger([b1, b2, sell])
        leg = ledger["s1"]
        # 100@10 + 50@20 = 2000 / 150
        assert leg["cost_avg"] == round(2000 / 150, 4)
        assert leg["buy_dates"] == ["2026-06-01", "2026-06-02"]

    def test_sell_without_buy_history(self):
        sell = _ev("s1", "卖出", price=30.0, qty=100)
        ledger = build_fifo_ledger([sell])
        leg = ledger["s1"]
        assert leg["cost_avg"] is None
        assert leg["unmatched_qty"] == 100
        assert leg["buy_dates"] == []


class TestClassifyEvent:
    SCAN = {
        "2026-06-03": [
            {"stock_code": "002407", "rank": 3},
            {"stock_code": "600246", "rank": 2},
            {"stock_code": "000062", "rank": 15},
        ],
        "2026-06-04": None,  # 该日无推票数据
    }

    def test_buy_in_top_list(self):
        ok, why, row = classify_event("买入", "002407", "2026-06-03", None, self.SCAN)
        assert ok and row["rank"] == 3

    def test_buy_rank_below_cutoff_is_manual(self):
        ok, why, row = classify_event("买入", "000062", "2026-06-03", None, self.SCAN)
        assert not ok
        assert f"Top-{TOP_LIST_SIZE}" in why

    def test_buy_absent_from_list_is_manual(self):
        ok, why, _ = classify_event("买入", "003009", "2026-06-03", None, self.SCAN)
        assert not ok

    def test_buy_date_without_scan_data_is_manual(self):
        ok, why, _ = classify_event("买入", "002407", "2026-06-04", None, self.SCAN)
        assert not ok
        assert "无推票数据" in why

    def test_sell_follows_buy_leg_date(self):
        leg = {"buy_dates": ["2026-06-03"]}
        ok, _, row = classify_event("卖出", "002407", "2026-06-05", leg, self.SCAN)
        assert ok and row["rank"] == 3

    def test_sell_without_buy_leg_is_manual(self):
        ok, why, _ = classify_event("卖出", "002407", "2026-06-05", {"buy_dates": []}, self.SCAN)
        assert not ok
        assert "买入腿" in why


class TestBuildExemplars:
    def test_picks_handwritten_and_skips_ai(self):
        hand_buy = _ev("b1", "买入", content_external="手写买入范文")
        ai_buy = _ev("b2", "买入", content_external="AI 写的", author="ai_journal")
        hand_sell = _ev("s1", "卖出", content="手写卖出复盘")
        empty = _ev("b3", "买入")
        text = build_exemplars([hand_buy, ai_buy, hand_sell, empty])
        assert "手写买入范文" in text
        assert "手写卖出复盘" in text
        assert "AI 写的" not in text
