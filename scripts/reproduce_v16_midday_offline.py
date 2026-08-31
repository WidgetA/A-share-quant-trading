"""V16 早盘策略的"午盘"变种实验: 跑 2026-07-10 至今每个交易日, 把早盘 09:30 开盘后前10分钟
(09:31-09:40) 换成午盘 13:00 开盘后前10分钟(13:01-13:10), 其余环节与
scripts/reproduce_v16_offline.py 完全一致(当前仓库板块文件、旧版 LGBRankScorer、
StockFilter 同配置、ST 走实时 Tushare stock_basic)。

数据源: 历史 stk_mins(逐分钟), 不依赖 iFinD token(与 reproduce_v16_offline.py 早盘
拉法一致), 只是把 09:30-09:41 窗口换成 13:00-13:11:
  - "open_price" = 13:01 那根 bar 的 open (午盘开盘价)
  - "price_940"  = ≤13:10 窗口最后一根 bar 的 close (午盘开盘后10分钟收盘)
  - "7min量"      = ≤13:07 (对标早盘 ≤09:37, 开盘+7分钟)
V16 的涨幅是 gain_from_open = (price - open)/open, 换成午盘开盘后自动变成
"午盘开盘10分钟涨幅", 不用改 scanner。限价/涨跌停过滤仍用真实 prev_close(昨日收盘)。

用法:
    export TUSHARE_TOKEN=...
    uv run python scripts/reproduce_v16_midday_offline.py                        # 2026-07-10 → 今天
    uv run python scripts/reproduce_v16_midday_offline.py --start 2026-07-10 --end 2026-07-13
    uv run python scripts/reproduce_v16_midday_offline.py 2026-07-10             # 单日
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
import os
import sys
from datetime import datetime
from datetime import time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "").strip()

OPEN_CUT = "1300"  # bars with hhmm > 1300 ...
CLOSE_CUT = "1310"  # ... and hhmm <= this (午盘开盘后10分钟, 对标早盘 ≤09:39/09:40)
VOL937_CUT = "1307"  # 午盘"7分钟量" (对标早盘 ≤09:37)


MORNING_CLOSE_CUT = "1130"  # 早盘收盘最后一根bar (11:29-11:30, 标签11:30, 实测确认)


async def _fetch_stk_mins_midday_quotes(
    tushare, codes: list[str], trade_date: str, batch_size: int = 500, lunch_gap: bool = False
) -> dict:
    """用 stk_mins 批量拉 trade_date 的分钟bar, 聚合出午盘开盘10分钟快照。

    不复用 TushareRealtimeClient._parse_rt_min_daily (它的早盘快照硬编码 ≤09:39/≤09:37,
    套不到午盘), 直接按 OPEN_CUT/CLOSE_CUT/VOL937_CUT 窗口聚合。

    stk_mins 的 ts_code 参数其实支持逗号分隔批量代码 (实测 2026-07-14: 上限 1000 个代码,
    单次返回上限 8000 行; 10分钟窗口每只约10行, 所以 batch_size=500 → 每批5000行,
    留出安全余量)。之前逐只调用 + 460/min 节流是误判了这个接口不支持批量, 3067 只要跑
    6-7分钟; 批量后只需 ~7 次调用, 几秒钟就能拉完, 不再需要节流。

    lunch_gap=True: "open_price" 不用13:01那根bar的open, 改用早盘最后一根bar(≤11:30)的
    close, 这样 gain_from_open 就变成"早盘收盘→午盘10分钟"的完整跨午休缺口涨幅
    (而不是"午盘重新开盘10分钟"), 拉取窗口相应从11:20开始(留buffer找最后一根早盘bar)。
    """
    from src.data.clients.tushare_realtime import TushareQuote, TushareRealtimeError

    out: dict = {}
    s_dt = f"{trade_date} 11:20:00" if lunch_gap else f"{trade_date} 13:00:00"
    e_dt = f"{trade_date} 13:11:00"

    def _ts_code(bare: str) -> str:
        return bare + ".SH" if bare.startswith(("6", "5")) else bare + ".SZ"

    sem = asyncio.Semaphore(4)

    async def _one_batch(batch: list[str]) -> dict:
        ts_codes = ",".join(_ts_code(c) for c in batch)
        data = None
        for attempt in range(4):
            async with sem:
                try:
                    data = await tushare._api_call(
                        "stk_mins",
                        {"ts_code": ts_codes, "freq": "1min", "start_date": s_dt, "end_date": e_dt},
                        fields="ts_code,trade_time,open,close,high,low,vol,amount",
                    )
                    break
                except TushareRealtimeError as e:
                    if "40203" in str(e) or "频率超限" in str(e):
                        await asyncio.sleep(2.0 + attempt)
                        continue
                    raise
        if data is None:
            return {}
        d = data.get("data", {})
        fields = d.get("fields", [])
        items = d.get("items", [])
        if not fields or not items:
            return {}
        fi = {f: i for i, f in enumerate(fields)}
        if not {"ts_code", "trade_time", "open", "close", "high", "low", "vol"}.issubset(fi):
            return {}
        has_amount = "amount" in fi

        by_code: dict[str, list] = {}
        for r in items:
            bare = str(r[fi["ts_code"]]).split(".")[0]
            by_code.setdefault(bare, []).append(r)

        batch_out: dict[str, TushareQuote] = {}
        for bare, rows in by_code.items():
            rows_sorted = sorted(rows, key=lambda r: str(r[fi["trade_time"]]))
            win = []  # (hhmm, open, high, low, close, vol, amount) — 13:01-13:10窗口
            morning_close = None  # 早盘最后一根bar(≤11:30)的close, 仅 lunch_gap 用
            for r in rows_sorted:
                t = str(r[fi["trade_time"]])
                hhmm = t.split(" ")[-1].replace(":", "")[:4] if " " in t else t.replace(":", "")[:4]
                if lunch_gap and hhmm <= MORNING_CLOSE_CUT:
                    morning_close = r[fi["close"]]  # 排过序, 保留最后一根覆盖前面的
                if OPEN_CUT < hhmm <= CLOSE_CUT:
                    win.append(
                        (
                            hhmm,
                            r[fi["open"]],
                            r[fi["high"]],
                            r[fi["low"]],
                            r[fi["close"]],
                            r[fi["vol"]],
                            r[fi["amount"]] if has_amount else None,
                        )
                    )
            if not win:
                continue
            if lunch_gap and morning_close is None:
                continue
            try:
                open_price = float(morning_close) if lunch_gap else float(win[0][1])
                e_close = float(win[-1][4])
                e_high = float(max(x[2] for x in win if x[2] is not None))
                e_low = float(min(x[3] for x in win if x[3] is not None))
                e_vol = float(sum(x[5] for x in win if x[5] is not None))
                e_amt = float(sum(x[6] for x in win if x[6] is not None))
                v937 = float(sum(x[5] for x in win if x[5] is not None and x[0] <= VOL937_CUT))
            except (ValueError, TypeError):
                continue
            if open_price <= 0 or e_close <= 0:
                continue
            batch_out[bare] = TushareQuote(
                stock_code=bare,
                open_price=open_price,
                latest_price=e_close,
                high_price=e_high,
                low_price=e_low,
                volume=e_vol,
                amount=e_amt,
                early_close=e_close,
                early_high=e_high,
                early_low=e_low,
                early_volume=e_vol,
                volume_937=v937,
            )
        return batch_out

    batches = [codes[i : i + batch_size] for i in range(0, len(codes), batch_size)]
    results = await asyncio.gather(*[_one_batch(b) for b in batches], return_exceptions=True)
    failed_batches = 0
    for res in results:
        if isinstance(res, BaseException):
            failed_batches += 1
            logger.warning("stk_mins batch failed: %s", res)
            continue
        out.update(res)
    logger.info(
        "stk_mins midday quotes (batched, %d batches, lunch_gap=%s): %d/%d (failed batches: %d)",
        len(batches),
        lunch_gap,
        len(out),
        len(codes),
        failed_batches,
    )
    return out


async def _fetch_rtmin_midday_quotes_today(
    tushare, codes: list[str], lunch_gap: bool = False
) -> dict:
    """当天用 rt_min_daily 拉午盘 13:01-13:10 快照 (stk_mins 对"今天"有数据滞后, 收盘后仍查不到,
    2026-07-14 实测确认; rt_min_daily 才是当天可查的实时接口)。

    rt_min_daily 经实测 (2026-07-14) 不支持批量 ts_code (逗号分隔报 50101 参数校验失败),
    只能单只单只查 —— 但这正是生产 V15/V16 每天 09:39 实盘用的
    TushareRealtimeClient.batch_get_early_quotes 走的同一个接口/并发方式 (MAX_CONCURRENCY=40,
    无需额外节流), 这里复用同样的并发度。

    lunch_gap=True: open_price 改用早盘最后一根bar(≤11:30)的close (见
    _fetch_stk_mins_midday_quotes 同名参数); rt_min_daily 本来就返回全天bar, 不需要额外
    加宽拉取窗口, 直接从已有数据里顺手取。
    """
    from src.data.clients.tushare_realtime import TushareQuote

    sem = asyncio.Semaphore(40)

    async def _one(bare: str):
        ts_code = bare + ".SH" if bare.startswith(("6", "5")) else bare + ".SZ"
        async with sem:
            data = await tushare._api_call(
                "rt_min_daily",
                {"ts_code": ts_code, "freq": "1MIN"},
                fields="time,open,close,high,low,vol,amount",
            )
        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])
        if not fields or not items:
            return bare, None
        fi = {f: i for i, f in enumerate(fields)}
        if not {"time", "open", "close", "high", "low", "vol"}.issubset(fi):
            return bare, None
        has_amount = "amount" in fi

        items_sorted = sorted(items, key=lambda r: str(r[fi["time"]]))
        win = []  # (hhmm, open, high, low, close, vol, amount)
        morning_close = None
        for r in items_sorted:
            t = str(r[fi["time"]])
            hhmm = t.split(" ")[-1].replace(":", "")[:4] if " " in t else t.replace(":", "")[:4]
            if lunch_gap and hhmm <= MORNING_CLOSE_CUT:
                morning_close = r[fi["close"]]
            if OPEN_CUT < hhmm <= CLOSE_CUT:
                win.append(
                    (
                        hhmm,
                        r[fi["open"]],
                        r[fi["high"]],
                        r[fi["low"]],
                        r[fi["close"]],
                        r[fi["vol"]],
                        r[fi["amount"]] if has_amount else None,
                    )
                )
        if not win:
            return bare, None
        if lunch_gap and morning_close is None:
            return bare, None
        try:
            open_price = float(morning_close) if lunch_gap else float(win[0][1])
            e_close = float(win[-1][4])
            e_high = float(max(x[2] for x in win if x[2] is not None))
            e_low = float(min(x[3] for x in win if x[3] is not None))
            e_vol = float(sum(x[5] for x in win if x[5] is not None))
            e_amt = float(sum(x[6] for x in win if x[6] is not None))
            v937 = float(sum(x[5] for x in win if x[5] is not None and x[0] <= VOL937_CUT))
        except (ValueError, TypeError):
            return bare, None
        if open_price <= 0 or e_close <= 0:
            return bare, None
        return bare, TushareQuote(
            stock_code=bare,
            open_price=open_price,
            latest_price=e_close,
            high_price=e_high,
            low_price=e_low,
            volume=e_vol,
            amount=e_amt,
            early_close=e_close,
            early_high=e_high,
            early_low=e_low,
            early_volume=e_vol,
            volume_937=v937,
        )

    results = await asyncio.gather(*[_one(c) for c in codes], return_exceptions=True)
    out: dict = {}
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
    logger.info(
        "rt_min_daily midday quotes (today): %d/%d (failed %d)", len(out), len(codes), failed
    )
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


async def main(start_str: str, end_str: str | None, lunch_gap: bool = False) -> None:
    os.environ.setdefault("TUSHARE_TOKEN", TUSHARE_TOKEN)

    from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter
    from src.data.clients.tushare_realtime import TushareRealtimeClient
    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig
    from src.strategy.lgbrank_scorer import LGBRankScorer
    from src.strategy.strategies.v16_scanner import V16Scanner
    from src.web.v15_scan_service import _build_stock_data, _fetch_history_ohlcv, get_trade_calendar

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
        calendar = await get_trade_calendar()
        start_d = datetime.strptime(start_str, "%Y-%m-%d").date()
        now_bj = datetime.now(BEIJING_TZ)
        end_d = datetime.strptime(end_str, "%Y-%m-%d").date() if end_str else now_bj.date()

        dates = [d for d in calendar if start_d <= d <= end_d]
        if dates and dates[-1] == now_bj.date() and now_bj.time() < dtime(13, 10):
            print(f"[跳过 {dates[-1]}: 今天午盘 13:10 尚未收盘, 数据还不存在]")
            dates = dates[:-1]
        if not dates:
            print("没有可跑的交易日(区间内无交易日, 或今天午盘还没收盘)")
            return
        print(f"将跑 {len(dates)} 个交易日: {', '.join(d.strftime('%Y-%m-%d') for d in dates)}\n")

        clean_boards, universe_codes = scanner.get_universe()
        universe_list = sorted(universe_codes)
        print(f"Universe: {len(universe_list)} 股, {len(clean_boards)} 板块")

        open_label = "早盘收盘(≤11:30)→午盘13:10" if lunch_gap else "13:01-13:10"
        summary_blocks: list[str] = []
        for ref_date in dates:
            ds = ref_date.strftime("%Y-%m-%d")
            print(
                f"\n{'=' * 70}\n=== 午盘变种复刻 V16  日期={ds}  窗口={open_label} ===\n{'=' * 70}"
            )

            if ref_date == now_bj.date():
                # stk_mins 对"今天"有数据滞后, 收盘后仍查不到 (2026-07-14 实测), 改走
                # rt_min_daily (今天可查的实时接口, 与生产 batch_get_early_quotes 同源)
                quotes = await _fetch_rtmin_midday_quotes_today(
                    tushare, universe_list, lunch_gap=lunch_gap
                )
                print(
                    f"Quotes (rt_min_daily→{open_label}, 今天): {len(quotes)}/{len(universe_list)}"
                )
            else:
                quotes = await _fetch_stk_mins_midday_quotes(
                    tushare, universe_list, ds, lunch_gap=lunch_gap
                )
                print(f"Quotes (stk_mins→{open_label}): {len(quotes)}/{len(universe_list)}")
            if not quotes:
                print("  无数据, 跳过该日")
                continue

            prev_dates = [d for d in calendar if d < ref_date]
            prev_trade_date = prev_dates[-1]
            prev_closes = await tushare.fetch_prev_closes(prev_trade_date.strftime("%Y%m%d"))
            print(f"Prev closes ({prev_trade_date}): {len(prev_closes)}")

            trading_codes = [c for c, q in quotes.items() if q.is_trading]
            hist_raw = await _fetch_history_ohlcv(hist_adapter, trading_codes, ref_date)
            print(f"History: {len(hist_raw)}/{len(trading_codes)}")

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
            print(f"Stock data: {len(stock_data)} built, {errs} skipped")

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

            print("--- Top-10 (午盘变种) ---")
            day_lines = [f"=== {ds} (午盘 13:01-13:10) ==="]
            if result.recommended:
                for s in result.recommended:
                    board = result.stock_best_board.get(s.code, "-")
                    bg = result.step2_board_avg_gains.get(board, 0.0)
                    v937 = result.stock_early_vol.get(s.code, 0.0)
                    line = (
                        f"  {s.rank:>2}. {s.code} {s.name:<8} LGB={s.score:.4f} "
                        f"买入:{s.buy_price:.2f}  {board}({bg:+.2f}%)  7min={v937 / 1e4:.0f}万"
                    )
                    print(line)
                    day_lines.append(line)
            else:
                print("  无推荐")
                day_lines.append("  无推荐")
            summary_blocks.append("\n".join(day_lines))

        fname = (
            "v16_lunchgap_variant_summary.txt" if lunch_gap else "v16_midday_variant_summary.txt"
        )
        out_path = Path("e:/tmp") / fname
        out_path.write_text("\n\n".join(summary_blocks), encoding="utf-8")
        print(f"\n\n[汇总已写入 {out_path}]")
    finally:
        await tushare.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "date", nargs="?", help="单日 YYYY-MM-DD (给了就只跑这一天, 忽略 --start/--end)"
    )
    ap.add_argument("--start", default="2026-07-10")
    ap.add_argument("--end", default=None, help="默认到今天(北京时间); 今天午盘未收盘则自动跳过")
    ap.add_argument(
        "--lunch-gap",
        action="store_true",
        help="open_price 改用早盘最后一根bar(≤11:30)的close, 而不是13:01那根bar的open"
        " (捕捉跨午休缺口+午盘10分钟的完整涨幅, 而不只是午盘重新开盘10分钟)",
    )
    args = ap.parse_args()
    if args.date:
        asyncio.run(main(args.date, args.date, lunch_gap=args.lunch_gap))
    else:
        asyncio.run(main(args.start, args.end, lunch_gap=args.lunch_gap))
