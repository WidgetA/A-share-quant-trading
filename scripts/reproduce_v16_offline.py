"""离线复刻某历史交易日的 V16 实时扫描结果。

为什么这样写 (CRITICAL):
    线上 09:39 实时扫描的"早盘快照"是 batch_get_early_quotes → _parse_rt_min_daily
    按 **≤09:39** 聚合 (price/high/low/vol), "7min量"按 **≤09:37** 聚合
    (见 tushare_realtime.py:285-330)。它喂的实时源 rt_min_daily 事后拉不回来。

    本脚本用历史源 stk_mins 拉同一天 09:30-09:41 的 1 分钟 bar, **重映射成
    rt_min_daily 的行格式后喂给同一个 _parse_rt_min_daily**, 因此聚合口径
    (≤09:39 / ≤09:37、含 09:30 集合竞价 bar) 与线上逐字段一致 —— 不是回测
    缓存那种写死 09:40 + volume_937=0 的口径。

    其余环节全部复用线上代码: _build_stock_data 组装 V16StockData、
    LGBRankScorer 打分、V16Scanner.scan、真实 Tushare stock_basic 做 ST 过滤、
    StockFilter 同配置。只换两处: ① 板块文件 (--boards old/new) ② quote 源
    (stk_mins 历史 vs rt_min_daily 实时)。

用法:
    export TUSHARE_TOKEN=...
    uv run python scripts/reproduce_v16_offline.py 2026-06-17 --boards old
    uv run python scripts/reproduce_v16_offline.py 2026-06-17 --boards new
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "").strip()

# 旧板块备份 (部署新板块前的 2026-03-19 版, 即线上 6/17 当时用的文件)
OLD_CONSTITUENTS = Path("e:/tmp/board_constituents.OLD-2026-03-19.json")
OLD_SECTORS = Path("e:/tmp/sectors.OLD-2026-03-19.json")


def _point_mapper_at(boards: str) -> None:
    """让 LocalConceptMapper 读旧/新板块文件 (覆盖模块级路径)。"""
    import src.data.sources.local_concept_mapper as lcm

    if boards == "old":
        if not OLD_CONSTITUENTS.exists() or not OLD_SECTORS.exists():
            raise SystemExit(f"旧板块备份不存在: {OLD_CONSTITUENTS} / {OLD_SECTORS}")
        lcm._CONSTITUENTS_PATH = OLD_CONSTITUENTS
        lcm._SECTORS_PATH = OLD_SECTORS
    else:  # new = 当前仓库 data/
        lcm._CONSTITUENTS_PATH = _ROOT / "data" / "board_constituents.json"
        lcm._SECTORS_PATH = _ROOT / "data" / "sectors.json"
    logger.info("板块文件: %s", lcm._CONSTITUENTS_PATH)


async def _fetch_stk_mins_quotes(tushare, codes: list[str], trade_date: str) -> dict:
    """用 stk_mins 拉 trade_date 的 09:30-09:41 分钟 bar, 重映射后喂 _parse_rt_min_daily。

    返回 {bare_code: TushareQuote}, 聚合口径与线上 rt_min_daily 完全一致。
    """
    import time as _time

    from src.data.clients.tushare_realtime import TushareRealtimeClient, TushareRealtimeError

    sem = asyncio.Semaphore(12)
    out: dict = {}
    s_dt = f"{trade_date} 09:30:00"
    e_dt = f"{trade_date} 09:41:00"

    # stk_mins 限频 500 次/分钟 → 滑动窗口令牌桶限到 460/min, 撞限频再退避重试
    _call_times: list[float] = []
    _rate_lock = asyncio.Lock()
    _LIMIT_PER_MIN = 460

    async def _throttle() -> None:
        while True:
            async with _rate_lock:
                now = _time.monotonic()
                while _call_times and now - _call_times[0] > 60:
                    _call_times.pop(0)
                if len(_call_times) < _LIMIT_PER_MIN:
                    _call_times.append(now)
                    return
                wait = 60 - (now - _call_times[0]) + 0.05
            await asyncio.sleep(min(wait, 5))

    async def _one(bare: str):
        ts_code = bare + ".SH" if bare.startswith(("6", "5")) else bare + ".SZ"
        data = None
        for attempt in range(6):
            await _throttle()
            async with sem:
                try:
                    data = await tushare._api_call(
                        "stk_mins",
                        {"ts_code": ts_code, "freq": "1min", "start_date": s_dt, "end_date": e_dt},
                        fields="ts_code,trade_time,open,close,high,low,vol,amount",
                    )
                    break
                except TushareRealtimeError as e:
                    if "40203" in str(e) or "频率超限" in str(e):
                        await asyncio.sleep(2.0 + attempt)
                        continue
                    return bare, None
        if data is None:
            return bare, None
        d = data.get("data", {})
        fields = d.get("fields", [])
        items = d.get("items", [])
        if not fields or not items:
            return bare, None
        fi = {f: i for i, f in enumerate(fields)}
        if "trade_time" not in fi:
            return bare, None
        has_amount = "amount" in fi
        # 重映射成 rt_min_daily 行格式: time/open/close/high/low/vol/amount
        remap_fields = ["time", "open", "close", "high", "low", "vol", "amount"]
        remap_items = []
        for r in items:
            try:
                o = r[fi["open"]]
                c = r[fi["close"]]
                h = r[fi["high"]]
                lo = r[fi["low"]]
                v = r[fi["vol"]]
            except (IndexError, KeyError):
                continue
            amt = (
                r[fi["amount"]]
                if has_amount
                else ((float(v) * float(c)) if v is not None and c is not None else None)
            )
            remap_items.append([r[fi["trade_time"]], o, c, h, lo, v, amt])
        # stk_mins 返回 DESC, _parse 内部不排序 → 这里按 time 升序
        remap_items.sort(key=lambda x: str(x[0]))
        fake = {"data": {"fields": remap_fields, "items": remap_items}}
        quote = TushareRealtimeClient._parse_rt_min_daily(bare, fake)
        return bare, quote

    results = await asyncio.gather(*[_one(c) for c in codes], return_exceptions=True)
    failed = 0
    for res in results:
        if isinstance(res, BaseException):
            failed += 1
            continue
        bare, q = res
        if q is not None:
            out[bare] = q
        else:
            failed += 1
    logger.info("stk_mins quotes: %d/%d (failed %d)", len(out), len(codes), failed)
    return out


class _STFilterFDB:
    """复刻 fundamentals_db.batch_filter_st: 查实时 Tushare stock_basic, 过滤 ST/*ST。"""

    async def batch_filter_st(self, codes: list[str]) -> list[str]:
        from src.data.database.fundamentals_db import _ST_PREFIXES, _fetch_tushare_names

        if not codes:
            return []
        names = await _fetch_tushare_names(codes)
        return [c for c in codes if c in names and not names[c].startswith(_ST_PREFIXES)]

    async def batch_get_fundamentals(self, codes):
        return {}


async def main(trade_date: str, boards: str) -> None:
    os.environ.setdefault("TUSHARE_TOKEN", TUSHARE_TOKEN)
    _point_mapper_at(boards)

    from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter
    from src.data.clients.tushare_realtime import TushareRealtimeClient
    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig
    from src.strategy.lgbrank_scorer import LGBRankScorer
    from src.strategy.strategies.v16_scanner import V16Scanner
    from src.web.v15_scan_service import _build_stock_data, _fetch_history_ohlcv, get_trade_calendar

    ref_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
    print(f"=== 离线复刻 V16  日期={trade_date}  板块={boards}  截点=09:39(同线上) ===\n")

    tushare = TushareRealtimeClient(token=TUSHARE_TOKEN)
    await tushare.start()
    hist_adapter = IQuantHistoricalAdapter(tushare, cache=None)
    mapper = LocalConceptMapper()
    stock_filter = StockFilter(
        StockFilterConfig(
            exclude_bse=True, exclude_chinext=True, exclude_star=True, exclude_sme=False
        )
    )
    scorer = LGBRankScorer(
        _ROOT / "models" / "lgbrank_latest.txt", _ROOT / "models" / "feature_list.json"
    )
    scanner = V16Scanner(_STFilterFDB(), mapper, stock_filter, scorer)

    try:
        clean_boards, universe_codes = scanner.get_universe()
        universe_list = sorted(universe_codes)
        print(f"Step0 universe: {len(universe_codes)} 股, {len(clean_boards)} 板块")

        # quotes via stk_mins(历史) → _parse_rt_min_daily(≤09:39)
        quotes = await _fetch_stk_mins_quotes(tushare, universe_list, trade_date)
        print(f"Quotes (stk_mins→≤09:39): {len(quotes)}/{len(universe_list)}")

        # prev_close: 上一交易日 Tushare daily
        calendar = await get_trade_calendar()
        prev_dates = [d for d in calendar if d < ref_date]
        prev_trade_date = prev_dates[-1]
        prev_closes = await tushare.fetch_prev_closes(prev_trade_date.strftime("%Y%m%d"))
        print(f"Prev closes ({prev_trade_date}): {len(prev_closes)}")

        # 37d history
        trading_codes = [c for c, q in quotes.items() if q.is_trading]
        hist_raw = await _fetch_history_ohlcv(hist_adapter, trading_codes, ref_date)
        print(f"History: {len(hist_raw)}/{len(trading_codes)}")

        # names from mapper
        name_map: dict[str, str] = {}
        mapper._ensure_loaded()
        for _b, members in mapper._board_stocks.items():
            for code, nm in members:
                if code not in name_map and nm:
                    name_map[code] = nm

        stock_data = {}
        errs = 0
        for code in trading_codes:
            q = quotes.get(code)
            pc = prev_closes.get(code)
            hr = hist_raw.get(code)
            if not q or not pc or pc <= 0 or not hr:
                errs += 1
                continue
            try:
                sd = _build_stock_data(code, name_map.get(code, ""), q, pc, hr, ref_date)
            except RuntimeError:
                errs += 1
                continue
            if sd is not None:
                stock_data[code] = sd
        print(f"Stock data: {len(stock_data)} built, {errs} skipped\n")

        result = await scanner.scan(stock_data, clean_boards)

        print("--- 漏斗 ---")
        print(f"  热门板块: {result.step2_hot_board_count}")
        print(f"  涨幅过滤: {result.step3_count}")
        print(f"  价格过滤: {result.step4_count}")
        print(f"  量能过滤: {result.step5_count}")
        print(f"  反转过滤: {result.step6_count}")
        print(f"  涨停过滤: {result.step6_5_count}")
        print(f"  上影线过滤: {result.step6_6_count}")
        print(f"  最终: {result.final_candidates}")

        print("\n--- Top-10 ---")
        for s in result.recommended:
            board = result.stock_best_board.get(s.code, "-")
            bg = result.step2_board_avg_gains.get(board, 0.0)
            v937 = result.stock_early_vol.get(s.code, 0.0)
            print(
                f"  {s.rank:>2}. {s.code} {s.name:<8} LGB={s.score:.4f} "
                f"买入:{s.buy_price:.2f}  {board}({bg:+.2f}%)  7min={v937 / 1e4:.0f}万"
            )
    finally:
        await tushare.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("trade_date", help="YYYY-MM-DD")
    ap.add_argument("--boards", choices=["old", "new"], default="old")
    args = ap.parse_args()
    asyncio.run(main(args.trade_date, args.boards))
