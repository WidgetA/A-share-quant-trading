"""一次性脚本: 拉某交易日午盘 13:01-13:10 的个股涨幅排行(不走 V16 板块/评分漏斗)。

复用 reproduce_v16_midday_offline.py 里的 stk_mins 午盘拉取函数, 只看
gain = (13:10收盘 - 13:00开盘) / 13:00开盘, 按跌涨排个股, 不需要 prev_close/37天历史。

用法:
    uv run python scripts/dump_midday_top_gainers.py 2026-07-13 [top_n]
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

# 导入 reproduce_v16_midday_offline 时它自己的模块顶层代码已经把 sys.stdout/stderr
# 重新包成 UTF-8 TextIOWrapper (win32下解决中文输出编码问题); 这里不能重复再包一次——
# 两层 TextIOWrapper 共享同一个底层 buffer, 第一层被 GC 时会把 buffer 关掉,
# 导致第二层 (当前 sys.stdout) 后续 print() 报 "I/O operation on closed file"。
from scripts.reproduce_v16_midday_offline import TUSHARE_TOKEN, _fetch_stk_mins_midday_quotes


async def main(trade_date: str, top_n: int) -> None:
    import os

    os.environ.setdefault("TUSHARE_TOKEN", TUSHARE_TOKEN)

    from src.data.clients.tushare_realtime import TushareRealtimeClient
    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig
    from src.strategy.lgbrank_scorer import LGBRankScorer
    from src.strategy.strategies.v16_scanner import V16Scanner

    tushare = TushareRealtimeClient(token=TUSHARE_TOKEN)
    await tushare.start()
    mapper = LocalConceptMapper()
    stock_filter = StockFilter(
        StockFilterConfig(
            exclude_bse=True, exclude_chinext=True, exclude_star=True, exclude_sme=False
        )
    )
    scorer = LGBRankScorer(
        _ROOT / "models" / "lgbrank_latest.txt", _ROOT / "models" / "feature_list.json"
    )

    class _StubFDB:
        async def batch_filter_st(self, codes):
            return codes

        async def batch_get_fundamentals(self, codes):
            return {}

    scanner = V16Scanner(_StubFDB(), mapper, stock_filter, scorer)

    try:
        _clean_boards, universe_codes = scanner.get_universe()
        universe_list = sorted(universe_codes)
        print(f"Universe: {len(universe_list)} 股\n")

        quotes = await _fetch_stk_mins_midday_quotes(tushare, universe_list, trade_date)
        print(f"Quotes (stk_mins→13:01-13:10): {len(quotes)}/{len(universe_list)}\n")

        mapper._ensure_loaded()
        name_map: dict[str, str] = {}
        for _b, members in mapper._board_stocks.items():
            for code, nm in members:
                if code not in name_map and nm:
                    name_map[code] = nm

        ranked = []
        for code, q in quotes.items():
            if not q.is_trading or q.open_price <= 0:
                continue
            gain = (q.early_close - q.open_price) / q.open_price * 100
            ranked.append((code, name_map.get(code, ""), gain, q.early_volume, q.volume_937))
        ranked.sort(key=lambda x: -x[2])

        print(f"=== {trade_date} 午盘 13:01-13:10 涨幅排行 TOP {top_n} (猛烈上攻) ===")
        for code, name, gain, vol, v937 in ranked[:top_n]:
            print(
                f"  {gain:+.2f}%  {code} {name:<8}  10min量={vol / 1e4:.0f}万  7min量={v937 / 1e4:.0f}万"
            )

        print(f"\n=== {trade_date} 午盘 13:01-13:10 跌幅排行 BOTTOM {top_n} (猛烈下探) ===")
        for code, name, gain, vol, v937 in ranked[-top_n:][::-1]:
            print(
                f"  {gain:+.2f}%  {code} {name:<8}  10min量={vol / 1e4:.0f}万  7min量={v937 / 1e4:.0f}万"
            )
    finally:
        await tushare.stop()


if __name__ == "__main__":
    ds = sys.argv[1] if len(sys.argv) > 1 else "2026-07-13"
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 25
    asyncio.run(main(ds, n))
