"""Run today's V16 scan locally with configurable cutoff time.

Usage:
    python scripts/run_v16_today.py          # default 9:40
    python scripts/run_v16_today.py 0950     # use 9:50 cutoff
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# Project root
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "").strip()


async def main(cutoff: str = "0940") -> None:
    os.environ.setdefault("TUSHARE_TOKEN", TUSHARE_TOKEN)

    from scripts.lgbrank_new_model_scorer import NewModelLGBRankScorer
    from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter
    from src.data.clients.tushare_realtime import TushareRealtimeClient
    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig
    from src.strategy.strategies.v16_scanner import V16Scanner, V16StockData
    from src.web.v15_scan_service import (
        _build_stock_data,
        _fetch_history_ohlcv,
        get_trade_calendar,
    )

    today = datetime.now(BEIJING_TZ).date()
    cutoff_display = f"{cutoff[:2]}:{cutoff[2:]}"
    print(f"=== V16 Scan for {today} (cutoff {cutoff_display}) ===\n")

    # --- Init ---
    tushare = TushareRealtimeClient(token=TUSHARE_TOKEN)
    await tushare.start()
    hist_adapter = IQuantHistoricalAdapter(tushare, cache=None)
    mapper = LocalConceptMapper()
    stock_filter = StockFilter(
        StockFilterConfig(
            exclude_bse=True, exclude_chinext=True, exclude_star=True, exclude_sme=False
        )
    )
    scorer = NewModelLGBRankScorer(
        _ROOT / "models" / "lgbrank_latest.txt", _ROOT / "models" / "feature_list.json"
    )

    class _StubFDB:
        async def batch_filter_st(self, codes):
            return codes

        async def batch_get_fundamentals(self, codes):
            return {}

    scanner = V16Scanner(_StubFDB(), mapper, stock_filter, scorer)

    try:
        # Step 0
        clean_boards, universe_codes = scanner.get_universe()
        universe_list = sorted(universe_codes)
        print(f"Universe: {len(universe_codes)} stocks, {len(clean_boards)} boards")

        # --- Fetch quotes with custom cutoff ---
        # batch_get_early_quotes() internally cuts at <=09:39 (fixed early snapshot,
        # what production currently uses). Route to it only for "0939"; any other
        # value (incl. "0940") goes through the inclusive custom path (bars <= cutoff,
        # i.e. "0940" really includes the 09:40:00 bar).
        if cutoff == "0939":
            quotes = await tushare.batch_get_early_quotes(universe_list)
        else:
            # Custom cutoff: fetch raw rt_min_daily and aggregate manually (bars <= cutoff)
            quotes = await _fetch_quotes_custom_cutoff(tushare, universe_list, cutoff)

        coverage = len(quotes) / len(universe_list) * 100
        print(f"Quotes: {len(quotes)}/{len(universe_list)} ({coverage:.1f}%)")
        if coverage < 80:
            print(f"\n!!! COVERAGE TOO LOW ({coverage:.1f}%) — API is broken !!!")
            return

        # Prev close
        calendar = await get_trade_calendar()
        prev_dates = [d for d in calendar if d < today]
        prev_trade_date = prev_dates[-1]
        prev_closes = await tushare.fetch_prev_closes(prev_trade_date.strftime("%Y%m%d"))
        print(f"Prev closes: {len(prev_closes)}")

        # History
        trading_codes = [c for c, q in quotes.items() if q.is_trading]
        print(f"Trading: {len(trading_codes)}, fetching history...")
        hist_raw = await _fetch_history_ohlcv(hist_adapter, trading_codes, today)
        hist_cov = len(hist_raw) / len(trading_codes) * 100 if trading_codes else 0
        print(f"History: {len(hist_raw)}/{len(trading_codes)} ({hist_cov:.1f}%)")
        if hist_cov < 80:
            print(f"\n!!! HISTORY COVERAGE TOO LOW ({hist_cov:.1f}%) !!!")
            return

        # Names
        name_map: dict[str, str] = {}
        mapper._ensure_loaded()
        for _b, members in mapper._board_stocks.items():
            for code, name in members:
                if code not in name_map and name:
                    name_map[code] = name

        # Build stock data
        stock_data: dict[str, V16StockData] = {}
        errs = 0
        for code in trading_codes:
            quote = quotes.get(code)
            if not quote or not quote.is_trading:
                continue
            pc = prev_closes.get(code)
            if not pc or pc <= 0:
                errs += 1
                continue
            hr = hist_raw.get(code)
            if not hr:
                errs += 1
                continue
            try:
                sd = _build_stock_data(code, name_map.get(code, ""), quote, pc, hr, today)
            except RuntimeError:
                errs += 1
                continue
            if sd is None:
                continue
            stock_data[code] = sd

        print(f"Stock data: {len(stock_data)} built, {errs} errors")

        # Scan
        result = await scanner.scan(stock_data, clean_boards)

        # Print results
        print("\n--- Results ---")
        print(f"Hot boards: {result.step2_hot_board_count}")
        print(f"Gain filter: {result.step3_count}")
        print(f"Price filter: {result.step4_count}")
        print(f"Volume filter: {result.step5_count}")
        print(f"Reversal: {result.step6_count}")
        print(f"Limit-up: {result.step6_5_count}")
        print(f"Shadow: {result.step6_6_count}")
        print(f"Final: {result.final_candidates}")

        if result.recommended:
            print("\n--- Top 10 ---")
            for s in result.recommended:
                board = result.stock_best_board.get(s.code, "-")
                bg = result.step2_board_avg_gains.get(board, 0)
                print(
                    f"  {s.rank:>2}. {s.code} {s.name:<8} "
                    f"LGB={s.score:.4f}  buy:{s.buy_price:.2f}  "
                    f"{board}({bg:+.2f}%)"
                )
        else:
            print("\nNo recommendation.")

    finally:
        await tushare.stop()


async def _fetch_quotes_custom_cutoff(tushare, codes: list[str], cutoff: str) -> dict:
    """Fetch quotes with custom time cutoff (e.g. '0950' for 9:50)."""
    from src.data.clients.tushare_realtime import TushareQuote

    sem = asyncio.Semaphore(40)
    all_quotes: dict[str, TushareQuote] = {}

    async def _fetch_one(bare_code: str):
        ts_code = bare_code + ".SH" if bare_code.startswith(("6", "5")) else bare_code + ".SZ"
        async with sem:
            data = await tushare._api_call(
                "rt_min_daily",
                {"ts_code": ts_code, "freq": "1MIN"},
                fields="time,open,close,high,low,vol,amount",
            )
        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])
        if not fields or not items:
            return bare_code, None

        idx = {f: i for i, f in enumerate(fields)}
        required = {"open", "close", "high", "low", "vol", "amount"}
        if not required.issubset(idx.keys()):
            return bare_code, None

        has_time = "time" in idx
        first_open = items[0][idx["open"]]
        if not first_open:
            return bare_code, None

        # Aggregate bars up to cutoff
        early_bars = []
        if has_time:
            for r in items:
                t = str(r[idx["time"]])
                if " " in t:
                    t = t.split(" ")[-1]
                hhmm = t.replace(":", "")[:4]
                if hhmm <= cutoff:
                    early_bars.append(r)

        if not early_bars:
            early_bars = items  # fallback

        try:
            e_close = float(early_bars[-1][idx["close"]])
            e_high = float(max(r[idx["high"]] for r in early_bars if r[idx["high"]] is not None))
            e_low = float(min(r[idx["low"]] for r in early_bars if r[idx["low"]] is not None))
            e_vol = float(sum(r[idx["vol"]] for r in early_bars if r[idx["vol"]] is not None))
            total_amount = float(
                sum(r[idx["amount"]] for r in early_bars if r[idx["amount"]] is not None)
            )
        except (ValueError, TypeError):
            return bare_code, None

        quote = TushareQuote(
            stock_code=bare_code,
            open_price=float(first_open),
            latest_price=e_close,
            high_price=e_high,
            low_price=e_low,
            volume=e_vol,
            amount=total_amount,
            early_close=e_close,
            early_high=e_high,
            early_low=e_low,
            early_volume=e_vol,
        )
        return bare_code, quote

    results = await asyncio.gather(*[_fetch_one(c) for c in codes], return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            continue
        bare_code, quote = result
        if quote is not None:
            all_quotes[bare_code] = quote

    print(f"  rt_min_daily (cutoff {cutoff}): {len(all_quotes)}/{len(codes)}")
    return all_quotes


if __name__ == "__main__":
    cutoff = sys.argv[1] if len(sys.argv) > 1 else "0940"
    asyncio.run(main(cutoff))
