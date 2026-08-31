"""Print all board avg gains at 9:40 and 9:50."""

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

    sem = asyncio.Semaphore(40)
    quotes: dict[str, tuple[float, float | None, float | None]] = {}

    async def fetch(code: str):
        ts_code = code + ".SH" if code.startswith(("6", "5")) else code + ".SZ"
        async with sem:
            data = await tushare._api_call(
                "rt_min_daily",
                {"ts_code": ts_code, "freq": "1MIN"},
                fields="time,open,close,high,low,vol,amount",
            )
        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])
        if not fields or not items:
            return
        idx = {f: i for i, f in enumerate(fields)}
        if "open" not in idx or "close" not in idx or "time" not in idx:
            return
        first_open = items[0][idx["open"]]
        if not first_open:
            return
        bars_40 = []
        bars_50 = []
        for r in items:
            t = str(r[idx["time"]])
            if " " in t:
                t = t.split(" ")[-1]
            hhmm = t.replace(":", "")[:4]
            if hhmm <= "0940":
                bars_40.append(r)
            if hhmm <= "0950":
                bars_50.append(r)
        e40 = float(bars_40[-1][idx["close"]]) if bars_40 else None
        e50 = float(bars_50[-1][idx["close"]]) if bars_50 else None
        quotes[code] = (float(first_open), e40, e50)

    codes = sorted(universe)
    await asyncio.gather(*[fetch(c) for c in codes], return_exceptions=True)

    rows = []
    for board_name, members in clean_boards.items():
        g40_list = []
        g50_list = []
        for code, _ in members:
            q = quotes.get(code)
            if not q or q[0] <= 0:
                continue
            op, e40, e50 = q
            if e40:
                g40_list.append((e40 - op) / op * 100)
            if e50:
                g50_list.append((e50 - op) / op * 100)
        if not g40_list:
            continue
        avg40 = sum(g40_list) / len(g40_list)
        avg50 = sum(g50_list) / len(g50_list) if g50_list else 0
        rows.append((board_name, avg40, avg50, len(g40_list)))

    rows.sort(key=lambda x: -x[2])

    out = _ROOT / "data" / "board_gains_today.txt"
    with open(out, "w", encoding="utf-8") as f:
        header = f"{'Board':<28} {'9:40 avg%':>10} {'9:50 avg%':>10} {'hot40':>6} {'hot50':>6} {'cnt':>6}"
        f.write(header + "\n")
        f.write("-" * len(header) + "\n")
        for name, g40, g50, cnt in rows:
            h40 = "YES" if g40 >= 0.80 else ""
            h50 = "YES" if g50 >= 0.80 else ""
            f.write(f"{name:<28} {g40:>+10.4f} {g50:>+10.4f} {h40:>6} {h50:>6} {cnt:>6}\n")
        f.write("-" * len(header) + "\n")
        hot40 = sum(1 for _, g40, _, _ in rows if g40 >= 0.80)
        hot50 = sum(1 for _, _, g50, _ in rows if g50 >= 0.80)
        f.write(f"Total boards: {len(rows)}, hot@9:40={hot40}, hot@9:50={hot50}\n")

    print(f"Written to {out}")
    await tushare.stop()


if __name__ == "__main__":
    asyncio.run(run())
