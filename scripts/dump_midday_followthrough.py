"""一次性脚本: 看某天午盘 13:01-13:10 涨幅榜个股, 后续走势(当天收盘 + 次日盘中现价)。

只对给定的一小撮代码跑(不是全市场), 所以比 dump_midday_top_gainers.py 快得多:
  1. stk_mins 重新拉这几只票 trade_date 的 13:00-13:11 (午盘开盘10分钟)
  2. Tushare daily 拉 trade_date 全天收盘价 (fetch_prev_closes, 一次性拉全市场再筛)
  3. Tushare rt_min 拉这几只票"现在"(次日盘中)的最新价

用法:
    uv run python scripts/dump_midday_followthrough.py 2026-07-13 600629,603201,603378,...
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# 导入这个模块会把 sys.stdout/stderr 包成 UTF-8 TextIOWrapper (win32), 不要在这里重复包
from scripts.reproduce_v16_midday_offline import TUSHARE_TOKEN, _fetch_stk_mins_midday_quotes


async def main(trade_date: str, codes_str: str) -> None:
    import os

    os.environ.setdefault("TUSHARE_TOKEN", TUSHARE_TOKEN)

    from src.data.clients.tushare_realtime import TushareRealtimeClient
    from src.data.sources.local_concept_mapper import LocalConceptMapper

    codes = [c.strip() for c in codes_str.split(",") if c.strip()]

    tushare = TushareRealtimeClient(token=TUSHARE_TOKEN)
    await tushare.start()
    mapper = LocalConceptMapper()
    mapper._ensure_loaded()

    try:
        name_map: dict[str, str] = {}
        board_map: dict[str, list[str]] = {c: [] for c in codes}
        for board, members in mapper._board_stocks.items():
            for code, nm in members:
                if code not in name_map and nm:
                    name_map[code] = nm
                if code in board_map:
                    board_map[code].append(board)

        midday_quotes = await _fetch_stk_mins_midday_quotes(tushare, codes, trade_date)
        print(f"Midday quotes: {len(midday_quotes)}/{len(codes)}")

        day_closes = await tushare.fetch_prev_closes(trade_date.replace("-", ""))
        print(f"Day closes ({trade_date}): {len(day_closes)} total stocks")

        now_quotes = await tushare.batch_get_quotes(codes)
        print(f"Now quotes (rt_min): {len(now_quotes)}/{len(codes)}\n")

        print(
            f"{'代码':<8}{'名称':<10}{'13:00开':>9}{'13:10收':>9}{'午盘10min':>10}"
            f"{'当日收盘':>9}{'收盘vs13:10':>12}{'现价(次日)':>11}{'现vs昨收':>10}"
        )
        for code in codes:
            mq = midday_quotes.get(code)
            dc = day_closes.get(code)
            nq = now_quotes.get(code)
            name = name_map.get(code, "")[:8]
            if not mq:
                print(f"{code:<8}{name:<10}  (无午盘数据)")
                continue
            midday_gain = (mq.early_close - mq.open_price) / mq.open_price * 100
            row = f"{code:<8}{name:<10}{mq.open_price:>9.2f}{mq.early_close:>9.2f}{midday_gain:>+9.2f}%"
            if dc and dc > 0:
                close_vs_1310 = (dc - mq.early_close) / mq.early_close * 100
                row += f"{dc:>9.2f}{close_vs_1310:>+11.2f}%"
            else:
                row += f"{'--':>9}{'--':>12}"
            if nq and dc and dc > 0:
                now_vs_close = (nq.latest_price - dc) / dc * 100
                row += f"{nq.latest_price:>11.2f}{now_vs_close:>+9.2f}%"
            else:
                row += f"{'--':>11}{'--':>10}"
            print(row)
            boards = board_map.get(code, [])
            if boards:
                print(f"    板块: {', '.join(boards[:6])}")
    finally:
        await tushare.stop()


if __name__ == "__main__":
    ds = sys.argv[1] if len(sys.argv) > 1 else "2026-07-13"
    codes_arg = sys.argv[2] if len(sys.argv) > 2 else ""
    asyncio.run(main(ds, codes_arg))
