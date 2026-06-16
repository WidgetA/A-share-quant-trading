"""Tests for board junk-filtering.

Regression guard: 专精特新 is a policy label (1160 constituents), not a tradable
theme. It must be in JUNK_BOARDS so is_junk_board() actually excludes it — it
previously leaked into the universe because it sat in the un-filtered
BROAD_CONCEPT_BOARDS list.
"""

from src.strategy.filters.board_filter import (
    BROAD_CONCEPT_BOARDS,
    JUNK_BOARDS,
    is_junk_board,
)


def test_zhuanjingtexin_is_junk():
    """专精特新 must be filtered out of the universe."""
    assert is_junk_board("专精特新") is True
    assert "专精特新" in JUNK_BOARDS
    assert "专精特新" not in BROAD_CONCEPT_BOARDS


def test_guojia_dajijin_is_junk():
    """国家大基金持股 is a fund-holding label, not a tradable theme."""
    assert is_junk_board("国家大基金持股") is True
    assert "国家大基金持股" in JUNK_BOARDS


def test_real_themes_not_junk():
    """Narrow, tradable themes must survive filtering."""
    for board in ("减速器", "无人驾驶", "汽车热管理"):
        assert is_junk_board(board) is False


def test_known_junk_boards_filtered():
    """Index/channel boards stay filtered."""
    for board in ("融资融券", "沪股通", "昨日涨停"):
        assert is_junk_board(board) is True


def test_junk_and_broad_sets_disjoint():
    """A board must not live in both lists — that ambiguity is what caused the leak."""
    assert JUNK_BOARDS.isdisjoint(BROAD_CONCEPT_BOARDS)
