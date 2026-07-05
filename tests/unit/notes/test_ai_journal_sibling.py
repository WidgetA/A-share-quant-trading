# NOTE-002: 「同天同票同方向已有手写腿 → 整组不代写」规则 (has_handwritten_sibling)

from datetime import datetime, timezone

from src.notes.ai_journal import has_handwritten_sibling
from src.notes.note_store import NoteEvent


def _ev(event_id, event_type, code="002929", hour=2, content="", author="system"):
    return NoteEvent(
        ts=datetime(2026, 5, 18, hour, 44, 0, tzinfo=timezone.utc),
        code=code,
        event_id=event_id,
        event_type=event_type,
        source="broker",
        title="",
        price=93.0,
        qty=100,
        side="sell" if event_type == "卖出" else "buy",
        content=content,
        content_external="",
        author=author,
        deleted=False,
        commission=None,
        transfer_fee=None,
        stamp_tax=None,
        dividend=None,
        realized_pnl=None,
        repo_income=None,
    )


def test_handwritten_sibling_blocks_group():
    empty = _ev("s2", "卖出")
    hand = _ev("s1", "卖出", content="先出一半锁定", author="user")
    assert has_handwritten_sibling(empty, [empty, hand]) is True


def test_ai_written_sibling_does_not_block():
    empty = _ev("s2", "卖出")
    ai = _ev("s1", "卖出", content="AI 写的", author="ai")
    assert has_handwritten_sibling(empty, [empty, ai]) is False


def test_other_code_does_not_block():
    empty_sell = _ev("s2", "卖出")
    hand_other_code = _ev("s9", "卖出", code="600000", content="别的票", author="user")
    assert has_handwritten_sibling(empty_sell, [empty_sell, hand_other_code]) is False


def test_handwritten_buy_blocks_its_sells():
    # 「买入我写了的你不要多嘴」——买入是用户手写的,这条链的卖出也不代写
    from datetime import datetime, timezone

    hand_buy = _ev("b1", "买入", content="进场理由", author="user")
    hand_buy.ts = datetime(2026, 5, 15, 1, 39, tzinfo=timezone.utc)
    empty_sell = _ev("s2", "卖出")
    leg = {"buy_dates": ["2026-05-15"]}
    assert has_handwritten_sibling(empty_sell, [hand_buy, empty_sell], leg) is True
    # 买入是 AI 写的则不拦
    ai_buy = _ev("b1", "买入", content="AI 写的进场", author="ai")
    ai_buy.ts = datetime(2026, 5, 15, 1, 39, tzinfo=timezone.utc)
    assert has_handwritten_sibling(empty_sell, [ai_buy, empty_sell], leg) is False
