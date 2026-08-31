"""Run V16 scan on the MIDDAY open window (13:00 open → 13:10), for a given date.

Mirrors the morning 09:30→09:40 logic onto the afternoon session:
  - "open_price"  = 13:00 midday open (first 13:01 bar's open)
  - "price_940"   = 13:10 close (early window close)
  - early window  = 13:01..13:10 (10 bars, end-time labels, like 09:31..09:40)

Data sources (decoupled to dodge the stk_mins 300k/day cap):
  - Midday minute bars : iFinD high_frequency (needs IFIND_REFRESH_TOKEN)
  - Daily history (37d) + prev_close : Tushare `daily` (separate, un-throttled quota)

V16's gain is gain_from_open = (price - open)/open, so swapping open→13:00 makes the
gain become "midday-open 10min surge" automatically — no scanner change needed.
Limit-up / price filters still use the REAL prev_close (yesterday's daily close).

Usage:
    IFIND_REFRESH_TOKEN=... python scripts/run_v16_midday.py 20260617
    IFIND_REFRESH_TOKEN=... python scripts/run_v16_midday.py 20260617 60   # limit 60 codes (smoke test)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "").strip()

OPEN_CUT = "1300"  # bars with hhmm > 1300 ...
CLOSE_CUT = os.environ.get("MIDDAY_CLOSE_CUT", "1310")  # ... and hhmm <= this (e.g. 1330 = 30min)
VOL937_CUT = "1307"  # midday first 7 minutes (analogue of ≤09:37)


def _ts_code(bare: str) -> str:
    return bare + ".SH" if bare.startswith(("6", "5")) else bare + ".SZ"


async def _fetch_midday_quotes_ifind(
    client, codes: list[str], trade_date: str, batch_size: int = 50
) -> dict:
    """Fetch 13:01..13:10 aggregated quotes for all codes via iFinD high_frequency.

    trade_date: 'YYYY-MM-DD'. Returns bare_code -> TushareQuote.
    """
    from datetime import datetime as _dt
    from datetime import timedelta as _td

    from src.data.clients.tushare_realtime import TushareQuote

    start_t = f"{trade_date} 13:00:00"
    _end_dt = _dt(2000, 1, 1, int(CLOSE_CUT[:2]), int(CLOSE_CUT[2:])) + _td(minutes=1)
    end_t = f"{trade_date} {_end_dt.strftime('%H:%M')}:00"
    quotes: dict[str, TushareQuote] = {}

    sem = asyncio.Semaphore(5)

    async def _one_batch(batch: list[str]):
        codes_str = ",".join(_ts_code(c) for c in batch)
        async with sem:
            resp = await client.high_frequency(
                codes_str,
                "open,high,low,close,volume,amount",
                start_t,
                end_t,
                {"Interval": "1"},
            )
        if resp.get("errorcode") not in (0, None):
            raise RuntimeError(
                f"iFinD high_frequency error: {resp.get('errorcode')} {resp.get('errmsg')}"
            )
        out: dict[str, TushareQuote] = {}
        for entry in resp.get("tables", []):
            thscode = entry.get("thscode", "")
            bare = thscode.split(".")[0] if thscode else ""
            if not bare:
                continue
            times = entry.get("time", [])
            tbl = entry.get("table", {})
            if not times:
                continue
            o = tbl.get("open", [])
            h = tbl.get("high", [])
            lo = tbl.get("low", [])
            cl = tbl.get("close", [])
            vol = tbl.get("volume", [])
            amt = tbl.get("amount", [])

            win = []  # (hhmm, open, high, low, close, vol, amount)
            for i, t in enumerate(times):
                ts = str(t)
                hhmm = (
                    ts.split(" ")[-1].replace(":", "")[:4] if " " in ts else ts.replace(":", "")[:4]
                )
                if OPEN_CUT < hhmm <= CLOSE_CUT:
                    win.append(
                        (
                            hhmm,
                            o[i] if i < len(o) else None,
                            h[i] if i < len(h) else None,
                            lo[i] if i < len(lo) else None,
                            cl[i] if i < len(cl) else None,
                            vol[i] if i < len(vol) else None,
                            amt[i] if i < len(amt) else None,
                        )
                    )
            if not win:
                continue
            try:
                open_price = float(win[0][1])
                e_close = float(win[-1][4])
                e_high = float(max(r[2] for r in win if r[2] is not None))
                e_low = float(min(r[3] for r in win if r[3] is not None))
                e_vol = float(sum(r[5] for r in win if r[5] is not None))
                e_amt = float(sum(r[6] for r in win if r[6] is not None))
                v937 = float(sum(r[5] for r in win if r[5] is not None and r[0] <= VOL937_CUT))
            except (ValueError, TypeError):
                continue
            if open_price <= 0 or e_close <= 0:
                continue
            out[bare] = TushareQuote(
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
        return out

    batches = [codes[i : i + batch_size] for i in range(0, len(codes), batch_size)]
    results = await asyncio.gather(*[_one_batch(b) for b in batches], return_exceptions=True)
    for r in results:
        if isinstance(r, BaseException):
            logging.warning(f"iFinD batch failed: {r}")
            continue
        quotes.update(r)
    return quotes


async def _fetch_midday_quotes_rtmin(tushare, codes: list[str]) -> dict:
    """Fetch 13:01..CLOSE_CUT aggregated quotes via Tushare rt_min_daily (TODAY only).

    rt_min_daily returns ALL of today's 1-min bars per stock; we keep the midday
    window (OPEN_CUT < hhmm <= CLOSE_CUT) and aggregate like the iFinD path.
    """
    from src.data.clients.tushare_realtime import TushareQuote

    sem = asyncio.Semaphore(40)
    quotes: dict[str, TushareQuote] = {}

    async def _one(bare: str):
        ts = _ts_code(bare)
        async with sem:
            data = await tushare._api_call(
                "rt_min_daily",
                {"ts_code": ts, "freq": "1MIN"},
                fields="time,open,close,high,low,vol,amount",
            )
        fields = data.get("data", {}).get("fields", [])
        items = data.get("data", {}).get("items", [])
        if not fields or not items:
            return bare, None
        idx = {f: i for i, f in enumerate(fields)}
        if not {"open", "close", "high", "low", "vol"}.issubset(idx):
            return bare, None
        win = []  # (hhmm, row)
        for r in items:
            t = str(r[idx["time"]])
            hhmm = t.split(" ")[-1].replace(":", "")[:4] if " " in t else t.replace(":", "")[:4]
            if OPEN_CUT < hhmm <= CLOSE_CUT:
                win.append((hhmm, r))
        if not win:
            return bare, None
        try:
            open_price = float(win[0][1][idx["open"]])
            e_close = float(win[-1][1][idx["close"]])
            e_high = float(max(r[idx["high"]] for _, r in win if r[idx["high"]] is not None))
            e_low = float(min(r[idx["low"]] for _, r in win if r[idx["low"]] is not None))
            e_vol = float(sum(r[idx["vol"]] for _, r in win if r[idx["vol"]] is not None))
            e_amt = float(
                sum(
                    r[idx["amount"]]
                    for _, r in win
                    if "amount" in idx and r[idx["amount"]] is not None
                )
            )
            v937 = float(
                sum(
                    r[idx["vol"]] for hh, r in win if r[idx["vol"]] is not None and hh <= VOL937_CUT
                )
            )
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
    for r in results:
        if isinstance(r, BaseException):
            continue
        bare, q = r
        if q is not None:
            quotes[bare] = q
    return quotes


async def main(as_of_str: str, limit: int | None = None) -> None:
    os.environ.setdefault("TUSHARE_TOKEN", TUSHARE_TOKEN)
    source = os.environ.get("MIDDAY_SOURCE", "ifind")  # "ifind" (any date) or "rtmin" (TODAY only)
    refresh = os.environ.get("IFIND_REFRESH_TOKEN")
    if source == "ifind" and not refresh:
        print("!!! IFIND_REFRESH_TOKEN not set — required for iFinD midday data")
        return

    from scripts.lgbrank_new_model_scorer import NewModelLGBRankScorer
    from src.data.clients.ifind_http_client import IFinDHttpClient
    from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter
    from src.data.clients.tushare_realtime import TushareRealtimeClient
    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig
    from src.strategy.strategies.v16_scanner import V16Scanner, V16StockData
    from src.web.v15_scan_service import _build_stock_data, _fetch_history_ohlcv, get_trade_calendar

    as_of = datetime.strptime(as_of_str, "%Y%m%d").date()
    trade_date = as_of.strftime("%Y-%m-%d")
    print(f"=== V16 MIDDAY Scan for {as_of} (window 13:01–13:10, iFinD) ===\n")

    tushare = TushareRealtimeClient(token=TUSHARE_TOKEN)
    await tushare.start()
    ifind = None
    if source == "ifind":
        ifind = IFinDHttpClient(refresh_token=refresh)
        await ifind.start()
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
        clean_boards, universe_codes = scanner.get_universe()
        universe_list = sorted(universe_codes)
        if limit:
            universe_list = universe_list[:limit]
        print(f"Universe: {len(universe_list)} stocks, {len(clean_boards)} boards")

        # Midday quotes via iFinD
        quotes = await _fetch_midday_quotes_ifind(ifind, universe_list, trade_date)
        coverage = len(quotes) / len(universe_list) * 100
        print(
            f"Midday quotes (iFinD 13:01–13:10): {len(quotes)}/{len(universe_list)} ({coverage:.1f}%)"
        )
        if coverage < 80:
            print(f"\n!!! COVERAGE TOO LOW ({coverage:.1f}%) !!!")
            return

        # prev_close = real close of the trading day BEFORE as_of (Tushare daily)
        calendar = await get_trade_calendar()
        prev_dates = [d for d in calendar if d < as_of]
        prev_trade_date = prev_dates[-1]
        prev_closes = await tushare.fetch_prev_closes(prev_trade_date.strftime("%Y%m%d"))
        print(f"Prev closes ({prev_trade_date}): {len(prev_closes)}")

        # Daily history 37d, ending the day before as_of (Tushare daily, un-throttled)
        trading_codes = [c for c, q in quotes.items() if q.is_trading]
        print(f"Trading: {len(trading_codes)}, fetching daily history...")
        hist_raw = await _fetch_history_ohlcv(hist_adapter, trading_codes, as_of)
        hist_cov = len(hist_raw) / len(trading_codes) * 100 if trading_codes else 0
        print(f"History: {len(hist_raw)}/{len(trading_codes)} ({hist_cov:.1f}%)")
        if hist_cov < 80:
            print(f"\n!!! HISTORY COVERAGE TOO LOW ({hist_cov:.1f}%) !!!")
            return

        name_map: dict[str, str] = {}
        mapper._ensure_loaded()
        for _b, members in mapper._board_stocks.items():
            for code, name in members:
                if code not in name_map and name:
                    name_map[code] = name

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
                sd = _build_stock_data(code, name_map.get(code, ""), quote, pc, hr, as_of)
            except RuntimeError:
                errs += 1
                continue
            if sd is None:
                continue
            stock_data[code] = sd

        print(f"Stock data: {len(stock_data)} built, {errs} errors")

        result = await scanner.scan(stock_data, clean_boards)

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
                    f"  {s.rank:>2}. {s.code} {s.name}  "
                    f"LGB={s.score:.4f}  buy:{s.buy_price:.2f}  {board}({bg:+.2f}%)"
                )
        else:
            print("\nNo recommendation.")

        # Dump midday strength ranking (UTF-8) regardless of recommendation
        board_rank = sorted(result.step2_all_board_avg_gains.items(), key=lambda x: -x[1])[:20]
        stock_rank = sorted(
            (
                (c, sd.name, (sd.price_940 - sd.open_price) / sd.open_price * 100)
                for c, sd in stock_data.items()
                if sd.open_price > 0
            ),
            key=lambda x: -x[2],
        )[:25]
        win_lbl = f"13:01..{CLOSE_CUT[:2]}:{CLOSE_CUT[2:]}"
        lines = [
            f"=== Midday strength {as_of} window {win_lbl} ===",
            "",
            "TOP BOARDS (avg gain_from_open %):",
        ]
        lines += [f"  {g:+.3f}%  {b}" for b, g in board_rank]
        lines += ["", "TOP STOCKS (gain_from_open %):"]
        lines += [f"  {g:+.2f}%  {c} {n}" for c, n, g in stock_rank]
        Path("e:/tmp/midday_rank.txt").write_text("\n".join(lines), encoding="utf-8")
        print(f"\n[ranking dumped to e:/tmp/midday_rank.txt, window {win_lbl}]")

    finally:
        await tushare.stop()
        await ifind.stop()


if __name__ == "__main__":
    as_of = sys.argv[1] if len(sys.argv) > 1 else "20260617"
    lim = int(sys.argv[2]) if len(sys.argv) > 2 else None
    asyncio.run(main(as_of, lim))
