"""Compare 9:40 gain distribution between today and a past date."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.basicConfig(level=logging.WARNING)

TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "").strip()


async def get_day_stats(tushare, clean_boards, universe, date_str=None):
    """Get per-stock gains. If date_str is None, use today's live data."""
    quotes = await tushare.batch_get_early_quotes(sorted(universe))

    all_gains = []
    for code, q in quotes.items():
        if q.is_trading and q.open_price > 0:
            g = (q.early_close - q.open_price) / q.open_price * 100
            all_gains.append((code, g))

    # Per-board stats
    board_stats = []
    for board_name, members in clean_boards.items():
        gains = []
        for code, _ in members:
            q = quotes.get(code)
            if q and q.is_trading and q.open_price > 0:
                g = (q.early_close - q.open_price) / q.open_price * 100
                gains.append(g)
        if gains:
            avg = sum(gains) / len(gains)
            pos = sum(1 for g in gains if g > 0)
            board_stats.append((board_name, avg, len(gains), pos))

    return all_gains, board_stats


async def run():
    from src.data.clients.tushare_realtime import TushareRealtimeClient
    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig
    from src.strategy.strategies.v16_scanner import V16Scanner

    tushare = TushareRealtimeClient(token=TUSHARE_TOKEN)
    await tushare.start()
    mapper = LocalConceptMapper()
    sf = StockFilter(
        StockFilterConfig(
            exclude_bse=True, exclude_chinext=True, exclude_star=True, exclude_sme=False
        )
    )

    class Stub:
        async def batch_filter_st(self, c):
            return c

        async def batch_get_fundamentals(self, c):
            return {}

    scanner = V16Scanner(Stub(), mapper, sf)
    clean_boards, universe = scanner.get_universe()

    # Today's data (rt_min_daily returns today's bars)
    all_gains, board_stats = await get_day_stats(tushare, clean_boards, universe)

    total = len(all_gains)
    pos_count = sum(1 for _, g in all_gains if g > 0)
    neg_count = sum(1 for _, g in all_gains if g < 0)
    flat_count = sum(1 for _, g in all_gains if g == 0)
    avg_gain = sum(g for _, g in all_gains) / total if total else 0
    median_gain = sorted(g for _, g in all_gains)[total // 2] if total else 0

    # Gain > 0.26% (V16 threshold)
    above_threshold = sum(1 for _, g in all_gains if g > 0.26)

    # Board distribution
    hot_boards = [(n, a, c, p) for n, a, c, p in board_stats if a >= 0.80]
    warm_boards = [(n, a, c, p) for n, a, c, p in board_stats if 0.30 <= a < 0.80]
    cool_boards = [(n, a, c, p) for n, a, c, p in board_stats if 0 <= a < 0.30]
    cold_boards = [(n, a, c, p) for n, a, c, p in board_stats if a < 0]

    # Per-board: what % of stocks are up?
    board_pos_rates = [(n, p / c * 100, a, c) for n, a, c, p in board_stats if c > 0]
    board_pos_rates.sort(key=lambda x: -x[1])

    out = _ROOT / "data" / "day_comparison.txt"
    with open(out, "w", encoding="utf-8") as f:
        f.write("=== Today's 9:40 Gain Distribution ===\n\n")
        f.write(f"Total stocks: {total}\n")
        f.write(f"  Up:   {pos_count} ({pos_count / total * 100:.1f}%)\n")
        f.write(f"  Flat: {flat_count} ({flat_count / total * 100:.1f}%)\n")
        f.write(f"  Down: {neg_count} ({neg_count / total * 100:.1f}%)\n")
        f.write(f"  Avg gain: {avg_gain:+.4f}%\n")
        f.write(f"  Median gain: {median_gain:+.4f}%\n")
        f.write(
            f"  Above 0.26% threshold: {above_threshold} ({above_threshold / total * 100:.1f}%)\n\n"
        )

        f.write("=== Board Distribution ===\n")
        f.write(f"  Hot (avg >= 0.80%):   {len(hot_boards)}\n")
        f.write(f"  Warm (0.30-0.80%):    {len(warm_boards)}\n")
        f.write(f"  Cool (0-0.30%):       {len(cool_boards)}\n")
        f.write(f"  Cold (< 0%):          {len(cold_boards)}\n")
        f.write(f"  Total boards:         {len(board_stats)}\n\n")

        # Gain percentile distribution
        sorted_gains = sorted(g for _, g in all_gains)
        pcts = [10, 25, 50, 75, 90, 95, 99]
        f.write("=== Gain Percentiles ===\n")
        for p in pcts:
            idx = int(len(sorted_gains) * p / 100)
            f.write(f"  P{p:>2}: {sorted_gains[idx]:+.4f}%\n")
        f.write(f"  Max: {sorted_gains[-1]:+.4f}%\n")
        f.write(f"  Min: {sorted_gains[0]:+.4f}%\n\n")

        # Top 30 boards by % of stocks up
        f.write("=== Top 30 Boards by % Stocks Up ===\n")
        f.write(f"{'Board':<28} {'%Up':>6} {'AvgGain%':>10} {'Cnt':>5}\n")
        f.write("-" * 55 + "\n")
        for name, prate, avg, cnt in board_pos_rates[:30]:
            f.write(f"{name:<28} {prate:>5.1f}% {avg:>+10.4f} {cnt:>5}\n")

        f.write("\n=== Bottom 10 Boards by % Stocks Up ===\n")
        f.write(f"{'Board':<28} {'%Up':>6} {'AvgGain%':>10} {'Cnt':>5}\n")
        f.write("-" * 55 + "\n")
        for name, prate, avg, cnt in board_pos_rates[-10:]:
            f.write(f"{name:<28} {prate:>5.1f}% {avg:>+10.4f} {cnt:>5}\n")

    print(f"Written to {out}")
    await tushare.stop()


if __name__ == "__main__":
    asyncio.run(run())
