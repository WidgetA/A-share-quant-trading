import copy
import json
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.strategy.filters.board_filter import is_junk_board
from src.strategy.v22_slim import selection
from src.web.v20_canonical_selection import _stable_external_market_fact_hash


def test_production_blacklist_preserves_raw_reference_facts_and_their_identity():
    raw = json.loads(selection.read_asset("board_constituents.json"))
    assert raw["中韩自贸区"]  # Exercise the board that exists in the production asset.
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
    assert boards._board_stocks["中韩自贸区"] == [tuple(row) for row in raw["中韩自贸区"]]
    assert universe == {code for rows in expected.values() for code, _ in rows}
    args = (date(2026, 9, 30), sorted(universe))
    calendar = tuple(date(2026, 9, 30) + timedelta(days=i) for i in range(-37, 3))
    facts = ({}, {}, boards.names, calendar)
    assert _stable_external_market_fact_hash(*args, clean_boards, *facts) == (
        _stable_external_market_fact_hash(*args, expected, *facts)
    )


@pytest.mark.asyncio
async def test_blacklist_blocks_candidate_routes_and_labels_without_mutating_raw_facts(monkeypatch):
    read_asset = selection.read_asset
    raw = {
        "中韩自贸区": [["600001", "Only excluded route"], ["600002", "Two routes"]],
        "半导体": [["600002", "Two routes"], ["600003", "Valid route"]],
    }
    monkeypatch.setattr(
        selection,
        "read_asset",
        lambda name: json.dumps(raw).encode()
        if name == "board_constituents.json"
        else read_asset(name),
    )
    scanner, _, _ = selection.make_scanner()
    clean_boards, _ = scanner.get_universe()
    original = copy.deepcopy(clean_boards)
    stocks = {
        code: SimpleNamespace(name=code, open_price=10.0, price_940=10.3)
        for code in ("600001", "600002", "600003")
    }
    # Inspect real candidates reaching the next stage, before unrelated filters.
    gain_filter = AsyncMock(return_value=[])
    monkeypatch.setattr(scanner, "_step3_gain_filter", gain_filter)
    result = await scanner.scan(stocks, clean_boards)
    assert clean_boards == original
    assert result.step2_codes == ["600002", "600003"]
    assert result.step2_boards_detail == {"半导体": ["600002", "600003"]}
    assert set(result.step2_board_avg_gains) == set(result.step2_all_board_avg_gains) == {"半导体"}
    assert result.stock_best_board == {"600002": "半导体", "600003": "半导体"}
    assert result.stock_all_boards == {"600002": ["半导体"], "600003": ["半导体"]}
    candidates = gain_filter.await_args.args[0]
    assert [(s.code, s.board_name) for s in candidates] == [
        ("600002", "半导体"),
        ("600003", "半导体"),
    ]
