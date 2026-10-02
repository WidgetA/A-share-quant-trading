import copy
import json
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.strategy.filters.board_filter import is_junk_board
from src.strategy.v22_slim import selection
from src.web.v20_canonical_selection import _stable_external_market_fact_hash

EXCLUDED_BOARDS = [
    "中韩自贸区",
    "上海自贸区",
    "同花顺果指数",
    "同花顺新质50",
    "中国AI 50",
    "同花顺漂亮100",
    "同花顺中特估100",
    "同花顺出海50",
]
FUTURE_PREFIX_BOARDS = ["同花顺未来主题99", "同花顺", "同花顺出海500"]


def test_v22_records_all_eight_exact_names_and_current_five_prefix_boards():
    assert selection.V22_BOARD_BLACKLIST == frozenset(EXCLUDED_BOARDS)
    raw = json.loads(selection.read_asset("board_constituents.json"))
    assert {name for name in raw if name.startswith("同花顺")} == {
        "同花顺漂亮100",
        "同花顺中特估100",
        "同花顺新质50",
        "同花顺果指数",
        "同花顺出海50",
    }


@pytest.mark.parametrize("excluded_board", EXCLUDED_BOARDS)
def test_production_blacklist_preserves_raw_reference_facts_and_their_identity(excluded_board):
    raw = json.loads(selection.read_asset("board_constituents.json"))
    assert raw[excluded_board]  # Exercise each exact name in the production asset.
    # These two were already generic junk boards; do not mutate that shared policy.
    assert is_junk_board(excluded_board) == (excluded_board in {"同花顺漂亮100", "同花顺中特估100"})
    scanner, _, boards = selection.make_scanner()
    clean_boards, universe = scanner.get_universe()
    expected = {
        board: [
            (str(row[0])[:6], str(row[1])) for row in rows if scanner._filter.is_allowed(row[0])
        ]
        for board, rows in raw.items()
        if not is_junk_board(board)
    }
    expected = {board: rows for board, rows in expected.items() if rows}
    assert clean_boards == expected
    assert boards._board_stocks[excluded_board] == [tuple(row) for row in raw[excluded_board]]
    assert universe == {code for rows in expected.values() for code, _ in rows}
    args = (date(2026, 9, 30), sorted(universe))
    calendar = tuple(date(2026, 9, 30) + timedelta(days=i) for i in range(-37, 3))
    facts = ({}, {}, boards.names, calendar)
    assert _stable_external_market_fact_hash(*args, clean_boards, *facts) == (
        _stable_external_market_fact_hash(*args, expected, *facts)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("excluded_board", EXCLUDED_BOARDS + FUTURE_PREFIX_BOARDS)
async def test_blacklist_blocks_candidate_routes_and_labels_without_mutating_raw_facts(
    monkeypatch, excluded_board
):
    read_asset = selection.read_asset
    raw = {
        excluded_board: [["600001", "Only excluded route"], ["600002", "Two routes"]],
        "半导体": [["600002", "Two routes"], ["600003", "Valid route"]],
        "非同花顺概念": [
            ["600004", "合法甲"],
            ["600005", "合法乙"],
        ],
    }
    monkeypatch.setattr(
        selection,
        "read_asset",
        lambda name: json.dumps(raw).encode()
        if name == "board_constituents.json"
        else read_asset(name),
    )
    scanner, _, boards = selection.make_scanner()
    # Exercise the route boundary for every name, including the two already-junk
    # names, without deleting or changing any raw reference member facts.
    clean_boards = copy.deepcopy(boards._board_stocks)
    original = copy.deepcopy(clean_boards)
    stocks = {
        code: SimpleNamespace(name=code, open_price=10.0, price_940=10.3)
        for code in ("600001", "600002", "600003", "600004", "600005")
    }
    # The V20 base algorithm still accepts the route; only V22 overrides it.
    base_hot, *_ = selection.V16Scanner._step2_hot_boards(scanner, clean_boards, stocks)
    assert excluded_board in base_hot
    assert "非同花顺概念" in base_hot
    # Inspect real candidates reaching the next stage, before unrelated filters.
    gain_filter = AsyncMock(return_value=[])
    monkeypatch.setattr(scanner, "_step3_gain_filter", gain_filter)
    result = await scanner.scan(stocks, clean_boards)
    assert clean_boards == original
    assert result.step2_codes == ["600002", "600003", "600004", "600005"]
    assert result.step2_boards_detail == {
        "半导体": ["600002", "600003"],
        "非同花顺概念": ["600004", "600005"],
    }
    assert (
        set(result.step2_board_avg_gains)
        == set(result.step2_all_board_avg_gains)
        == {"半导体", "非同花顺概念"}
    )
    assert result.stock_best_board == {
        "600002": "半导体",
        "600003": "半导体",
        "600004": "非同花顺概念",
        "600005": "非同花顺概念",
    }
    assert result.stock_all_boards == {
        "600002": ["半导体"],
        "600003": ["半导体"],
        "600004": ["非同花顺概念"],
        "600005": ["非同花顺概念"],
    }
    candidates = gain_filter.await_args.args[0]
    assert [(s.code, s.board_name) for s in candidates] == [
        ("600002", "半导体"),
        ("600003", "半导体"),
        ("600004", "非同花顺概念"),
        ("600005", "非同花顺概念"),
    ]


@pytest.mark.parametrize("allowed_board", ["新同花顺概念", "非同花顺概念", "中国AI 500", "半导体"])
def test_v22_prefix_and_exact_name_boundaries_keep_allowed_boards(monkeypatch, allowed_board):
    read_asset = selection.read_asset
    raw = {allowed_board: [["600001", "Allowed stock"], ["600002", "Another stock"]]}
    monkeypatch.setattr(
        selection,
        "read_asset",
        lambda name: json.dumps(raw).encode()
        if name == "board_constituents.json"
        else read_asset(name),
    )
    scanner, _, boards = selection.make_scanner()
    stocks = {
        code: SimpleNamespace(name=code, open_price=10.0, price_940=10.3)
        for code in ("600001", "600002")
    }
    hot, *_ = scanner._step2_hot_boards(boards._board_stocks, stocks)
    assert hot == {allowed_board: ["600001", "600002"]}
