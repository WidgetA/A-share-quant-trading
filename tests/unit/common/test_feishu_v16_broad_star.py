"""Tests for the ⭐ broad-board marker in the V16 Feishu top-10 report.

A pick whose best-board is a BROAD_CONCEPT_BOARD (≥400 constituents, vague
theme) must be flagged with ⭐ in both the Top-1 line and the top-10 list, and
a legend must appear. Narrow/real-theme boards stay unmarked.
"""

from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from src.common.feishu_bot import FeishuBot
from src.strategy.lgbrank_scorer import ScoredStock
from src.strategy.strategies.v16_scanner import V16ScanResult


def _make_result() -> V16ScanResult:
    result = V16ScanResult()
    result.step0_universe_count = 100
    result.step2_hot_board_count = 5
    result.final_candidates = 2
    result.recommended = [
        # Top-1: best-board is a broad concept board -> starred
        ScoredStock(code="603119", name="浙江荣泰", score=0.18, rank=1, buy_price=68.77),
        # #2: real narrow theme -> NOT starred
        ScoredStock(code="002297", name="博云新材", score=0.17, rank=2, buy_price=20.58),
    ]
    result.stock_best_board = {"603119": "锂电池概念", "002297": "碳纤维"}
    result.step2_board_avg_gains = {"锂电池概念": 0.88, "碳纤维": 0.92}
    return result


def _bot() -> FeishuBot:
    return FeishuBot(
        bot_url="http://test.local",
        app_id="a",
        app_secret="s",
        chat_id="c",
    )


@pytest.mark.asyncio
async def test_broad_board_starred_narrow_not():
    bot = _bot()
    bot.send_message = AsyncMock(return_value=True)

    await bot.send_v16_top10_report(_make_result(), scan_time=datetime(2026, 6, 1, 9, 40))

    msg = bot.send_message.call_args.args[0]
    # broad board flagged in both Top-1 line and the list row
    assert "板块: ⭐锂电池概念" in msg
    assert "⭐锂电池概念(+0.88%)" in msg
    # narrow real-theme board left unmarked
    assert "碳纤维(+0.92%)" in msg
    assert "⭐碳纤维" not in msg
    # legend present
    assert "⭐=宽泛板块" in msg


@pytest.mark.asyncio
async def test_no_legend_when_no_broad_board():
    result = _make_result()
    # both picks on narrow boards -> no star anywhere
    result.stock_best_board = {"603119": "减速器", "002297": "碳纤维"}
    result.step2_board_avg_gains = {"减速器": 0.7, "碳纤维": 0.92}

    bot = _bot()
    bot.send_message = AsyncMock(return_value=True)
    await bot.send_v16_top10_report(result, scan_time=datetime(2026, 6, 1, 9, 40))

    msg = bot.send_message.call_args.args[0]
    assert "⭐" not in msg
    assert "宽泛板块" not in msg


@pytest.mark.asyncio
async def test_multi_board_stock_lists_all_hot_boards():
    """A stock in >1 hot board must show every board, not just stock_best_board's pick."""
    result = _make_result()
    # "减速器" is a narrow real-theme board (unstarred), unlike broad "锂电池概念"
    result.stock_all_boards = {"603119": ["锂电池概念", "减速器"], "002297": ["碳纤维"]}
    result.step2_board_avg_gains = {"锂电池概念": 0.88, "减速器": 1.20, "碳纤维": 0.92}

    bot = _bot()
    bot.send_message = AsyncMock(return_value=True)
    await bot.send_v16_top10_report(result, scan_time=datetime(2026, 6, 1, 9, 40))

    msg = bot.send_message.call_args.args[0]
    assert "⭐锂电池概念(+0.88%)、减速器(+1.20%)" in msg
    assert "碳纤维(+0.92%)" in msg


@pytest.mark.asyncio
async def test_driver_vs_expanded_tag_and_legend():
    result = _make_result()
    result.stock_all_boards = {"603119": ["锂电池概念"], "002297": ["碳纤维"]}
    # 603119 own gain cleared the hot-board bar -> driver; 002297 only rode the board -> expanded
    result.stock_is_driver = {"603119": True, "002297": False}

    bot = _bot()
    bot.send_message = AsyncMock(return_value=True)
    await bot.send_v16_top10_report(result, scan_time=datetime(2026, 6, 1, 9, 40))

    msg = bot.send_message.call_args.args[0]
    assert "[带动]⭐锂电池概念" in msg
    assert "[扩增]碳纤维" in msg
    assert "[带动]=个股自身涨幅已达热门板块门槛" in msg


@pytest.mark.asyncio
async def test_no_driver_legend_when_field_unset():
    """Older/unset stock_is_driver (all None) must not print the driver/expanded legend."""
    bot = _bot()
    bot.send_message = AsyncMock(return_value=True)
    await bot.send_v16_top10_report(_make_result(), scan_time=datetime(2026, 6, 1, 9, 40))

    msg = bot.send_message.call_args.args[0]
    assert "[带动]" not in msg
    assert "[扩增]" not in msg
    assert "个股自身涨幅已达热门板块门槛" not in msg
