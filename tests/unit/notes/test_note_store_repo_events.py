# === MODULE PURPOSE ===
# Tests for 逆回购 (reverse repo) event handling (0.19.0). QMT delivers repo
# fills as ordinary sell trades; the note-writing layer must recognize repo
# codes (SH 204xxx / SZ 1318xx) and record event_type='逆回购' with side NULL —
# repo is not a buy/sell, only its income (realized_pnl) gets registered.

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from src.notes.note_store import TradeNoteStore, is_reverse_repo_code


class _FakeDB:
    def __init__(self, row: dict | None = None):
        self._row = row
        self.executed: list[str] = []

    async def fetchrow(self, sql: str):
        return self._row

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)


def _store(row: dict | None = None) -> TradeNoteStore:
    return TradeNoteStore(SimpleNamespace(db=_FakeDB(row)))


def test_is_reverse_repo_code():
    assert is_reverse_repo_code("204001")  # 沪 GC001
    assert is_reverse_repo_code("204007")
    assert is_reverse_repo_code("131810")  # 深 R-001
    assert is_reverse_repo_code("204001.SH")
    assert not is_reverse_repo_code("002407")
    assert not is_reverse_repo_code("600460")
    assert not is_reverse_repo_code("131")  # too short
    assert not is_reverse_repo_code("120401")  # 可转债段，非回购


def test_broker_upsert_repo_code_records_repo_type_without_side():
    st = _store(row=None)  # no existing row → fresh INSERT
    asyncio.run(
        st.upsert_broker_event_by_order_id(
            order_id=42, code="204001", side="sell", qty=10020, price=1.46, ts_ms=1782956409000
        )
    )
    (insert_sql,) = [s for s in st._db.executed if s.startswith("INSERT INTO trade_notes")]
    assert "'逆回购'" in insert_sql
    assert "逆回购 @1.46 x 10020" in insert_sql
    assert "'sell'" not in insert_sql  # side must be NULL, not the broker's sell


def test_broker_upsert_normal_code_still_records_sell():
    st = _store(row=None)
    asyncio.run(
        st.upsert_broker_event_by_order_id(
            order_id=43, code="002407", side="sell", qty=800, price=33.04, ts_ms=1749024300000
        )
    )
    (insert_sql,) = [s for s in st._db.executed if s.startswith("INSERT INTO trade_notes")]
    assert "'卖出'" in insert_sql
    assert "'sell'" in insert_sql


_SELL_ROW = {
    "ts": 1780368533000,
    "code": "204001",
    "event_id": "broker_manual_204001_sell_3900_1780368533000",
    "event_type": "卖出",
    "event_source": "broker",
    "title": "卖出 @100.00 x 3900",
    "price": 100.0,
    "qty": 3900,
    "side": "sell",
    "content": "",
    "content_external": "",
    "author": "system",
    "deleted": False,
    "commission": None,
    "transfer_fee": None,
    "stamp_tax": None,
    "dividend": None,
    "realized_pnl": None,
}


def test_update_event_to_repo_type_clears_side():
    st = _store(row=_SELL_ROW)
    ok = asyncio.run(
        st.update_event(
            "204001",
            _SELL_ROW["event_id"],
            event_type="逆回购",
            title="逆回购 @100.00 x 3900",
        )
    )
    assert ok is True
    (insert_sql,) = [s for s in st._db.executed if s.startswith("INSERT INTO trade_notes")]
    assert "'逆回购'" in insert_sql
    assert "'sell'" not in insert_sql  # side cleared to NULL on conversion


def test_create_manual_repo_event_no_side_auto_title():
    st = _store(row=None)
    asyncio.run(
        st.create_event(
            code="204001",
            event_type="逆回购",
            title="",
            content="",
            price=1.26,
            qty=5510,
            repo_income=3.9,
            ts_ms=1780624772000,
        )
    )
    (insert_sql,) = [s for s in st._db.executed if s.startswith("INSERT INTO trade_notes")]
    assert "'逆回购'" in insert_sql
    assert "逆回购 @1.26 x 5510" in insert_sql
    assert "'sell'" not in insert_sql
    # 收益进独立列 repo_income（INSERT 末位），realized_pnl 保持 NULL。
    assert insert_sql.rstrip(")").endswith("3.9")


# ---- 收益隔离硬守卫：平仓收益(realized_pnl)与逆回购利息(repo_income)物理隔离，
# ---- 任何情况都不允许混——塞错字段拒收，类型切换自动清空、绝不搬家。


def test_create_repo_event_rejects_realized_pnl():
    st = _store(row=None)
    with pytest.raises(ValueError, match="repo_income"):
        asyncio.run(
            st.create_event(
                code="204001",
                event_type="逆回购",
                title="",
                content="",
                realized_pnl=3.9,  # 平仓收益字段不属于逆回购 → 拒收
            )
        )
    assert st._db.executed == []


def test_create_sell_event_rejects_repo_income():
    st = _store(row=None)
    with pytest.raises(ValueError, match="逆回购"):
        asyncio.run(
            st.create_event(
                code="002407",
                event_type="卖出",
                title="",
                content="",
                repo_income=3.9,  # 利息字段不属于卖出 → 拒收
            )
        )
    assert st._db.executed == []


def test_convert_sell_with_pnl_to_repo_does_not_carry_pnl_over():
    # 卖出行已有平仓收益 -8000，转成逆回购：数值绝不搬进 repo_income，
    # realized_pnl/过户费/印花税/股息清空；佣金(手续费)两类通用，保留。
    row = dict(_SELL_ROW, realized_pnl=-8000.0, commission=8.11, stamp_tax=47.49)
    st = _store(row=row)
    ok = asyncio.run(st.update_event("204001", row["event_id"], event_type="逆回购"))
    assert ok is True
    (insert_sql,) = [s for s in st._db.executed if s.startswith("INSERT INTO trade_notes")]
    assert "'逆回购'" in insert_sql
    assert "-8000" not in insert_sql  # 平仓收益没有跟过来
    assert "8.11" in insert_sql  # 佣金通用，保留
    assert "47.49" not in insert_sql  # 印花税清了


def test_repo_event_accepts_commission():
    st = _store(row=None)
    asyncio.run(
        st.create_event(
            code="204001",
            event_type="逆回购",
            title="",
            content="",
            price=1.46,
            qty=10020,
            commission=1.0,  # 逆回购手续费——合法
            repo_income=4.1,
        )
    )
    (insert_sql,) = [s for s in st._db.executed if s.startswith("INSERT INTO trade_notes")]
    assert "'逆回购'" in insert_sql
    assert "1.0" in insert_sql
    assert "4.1" in insert_sql


def test_repo_event_rejects_transfer_fee():
    st = _store(row=None)
    with pytest.raises(ValueError, match="transfer_fee"):
        asyncio.run(
            st.create_event(
                code="204001",
                event_type="逆回购",
                title="",
                content="",
                transfer_fee=0.09,  # 过户费不属于逆回购 → 拒收
            )
        )
    assert st._db.executed == []


def test_convert_repo_with_income_to_sell_does_not_carry_income_over():
    row = dict(
        _SELL_ROW,
        event_type="逆回购",
        side=None,
        title="逆回购 @100.00 x 3900",
        repo_income=5.2,
    )
    st = _store(row=row)
    ok = asyncio.run(st.update_event("204001", row["event_id"], event_type="卖出"))
    assert ok is True
    (insert_sql,) = [s for s in st._db.executed if s.startswith("INSERT INTO trade_notes")]
    assert "'卖出'" in insert_sql
    assert "5.2" not in insert_sql  # 利息没有跟过来变成平仓收益


def test_update_repo_event_rejects_realized_pnl():
    row = dict(_SELL_ROW, event_type="逆回购", side=None)
    st = _store(row=row)
    with pytest.raises(ValueError, match="repo_income"):
        asyncio.run(st.update_event("204001", row["event_id"], realized_pnl=9.9))
    assert st._db.executed == []


def test_update_sell_event_rejects_repo_income():
    st = _store(row=dict(_SELL_ROW, code="002407", event_id="broker_1"))
    with pytest.raises(ValueError, match="逆回购"):
        asyncio.run(st.update_event("002407", "broker_1", repo_income=9.9))
    assert st._db.executed == []
