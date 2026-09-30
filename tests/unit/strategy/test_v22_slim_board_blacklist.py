import json

from src.strategy.filters.board_filter import is_junk_board
from src.strategy.v22_slim import selection


def test_production_scanner_excludes_zhonghan_board_and_preserves_other_routes():
    raw = json.loads(selection.read_asset("board_constituents.json"))
    assert raw["中韩自贸区"]  # Exercise the board that exists in the production asset.
    scanner, _, _ = selection.make_scanner()
    clean_boards, universe = scanner.get_universe()
    assert "中韩自贸区" not in clean_boards
    expected = {
        board: [
            (str(row[0])[:6], str(row[1])) for row in rows if scanner._filter.is_allowed(row[0])
        ]
        for board, rows in raw.items()
        if board != "中韩自贸区" and not is_junk_board(board)
    }
    expected = {board: rows for board, rows in expected.items() if rows}
    assert clean_boards == expected
    assert universe == {code for rows in expected.values() for code, _ in rows}


def test_blacklist_excludes_a_board_not_all_its_member_stocks(monkeypatch):
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
    clean_boards, universe = scanner.get_universe()
    assert clean_boards == {"半导体": [("600002", "Two routes"), ("600003", "Valid route")]}
    assert universe == {"600002", "600003"}
