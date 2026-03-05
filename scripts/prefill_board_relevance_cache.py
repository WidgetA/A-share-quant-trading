"""Pre-fill board relevance cache by calling LLM for all board-stock pairs.

Usage:
    uv run python scripts/prefill_board_relevance_cache.py

Skips junk boards and already-cached pairs. Safe to re-run (resume).
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.strategy.filters.board_filter import is_junk_board
from src.strategy.filters.board_relevance_filter import (
    BoardRelevanceFilter,
    DashScopeConfig,
)


async def main() -> None:
    from src.common.config import get_aliyun_api_key

    api_key = get_aliyun_api_key()
    config = DashScopeConfig(api_key=api_key)
    cache_path = "data/board_relevance_cache.json"
    fltr = BoardRelevanceFilter(config, cache_path=cache_path)

    # Load board constituents
    bc_path = Path("data/board_constituents.json")
    board_constituents: dict[str, list[list[str]]] = json.loads(bc_path.read_text(encoding="utf-8"))

    # Filter out junk boards
    boards = {
        name: stocks for name, stocks in board_constituents.items() if not is_junk_board(name)
    }

    # Find uncached pairs
    class FakeStock:
        def __init__(self, code: str, name: str, board: str):
            self.stock_code = code
            self.stock_name = name
            self.board_name = board
            self.open_gain_pct = 0.0
            self.pe_ttm = 0.0
            self.board_avg_pe = 0.0

    uncached_by_board: dict[str, list[FakeStock]] = {}
    total_cached = 0
    total_uncached = 0

    for board_name, stocks in boards.items():
        uncached = []
        for code, name in stocks:
            key = fltr._cache_key(board_name, code)
            if key in fltr._cache:
                total_cached += 1
            else:
                uncached.append(FakeStock(code, name, board_name))
                total_uncached += 1
        if uncached:
            uncached_by_board[board_name] = uncached

    total_boards = len(boards)
    uncached_boards = len(uncached_by_board)
    print(
        f"Boards: {total_boards} (excl junk), "
        f"cached: {total_cached}, uncached: {total_uncached} "
        f"across {uncached_boards} boards"
    )

    if not uncached_by_board:
        print("All pairs cached. Nothing to do.")
        return

    # Chunk large boards into groups of 30
    tasks: list[tuple[str, list[FakeStock]]] = []
    for board_name, stocks in uncached_by_board.items():
        for i in range(0, len(stocks), 30):
            tasks.append((board_name, stocks[i : i + 30]))

    print(f"LLM calls needed: {len(tasks)}")

    # Process with concurrency
    sem = asyncio.Semaphore(10)
    completed = 0
    failed = 0
    start_time = time.time()

    async def process(board_name: str, stocks: list[FakeStock]) -> None:
        nonlocal completed, failed
        async with sem:
            try:
                results = await fltr._call_llm(board_name, stocks)
                for r in results:
                    key = fltr._cache_key(r.board_name, r.stock_code)
                    fltr._cache[key] = {"level": r.level, "reason": r.reason}
            except Exception as e:
                failed += 1
                print(f"  FAIL: {board_name} ({len(stocks)} stocks): {e}")
                return

            completed += 1
            if completed % 10 == 0 or completed == len(tasks):
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                remaining = (len(tasks) - completed) / rate if rate > 0 else 0
                print(
                    f"  [{completed}/{len(tasks)}] "
                    f"{elapsed:.0f}s elapsed, ~{remaining:.0f}s remaining"
                )

    # Save cache periodically
    save_interval = 50
    batch_size = save_interval
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i : i + batch_size]
        await asyncio.gather(*[process(bn, stks) for bn, stks in batch])
        fltr._save_cache()
        print(f"  Cache saved ({len(fltr._cache)} entries)")

    elapsed = time.time() - start_time
    print(
        f"\nDone! {completed} calls succeeded, {failed} failed. "
        f"Total cache: {len(fltr._cache)} entries. "
        f"Time: {elapsed:.0f}s"
    )


if __name__ == "__main__":
    asyncio.run(main())
