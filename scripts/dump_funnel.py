"""Dump V16 funnel data for a given date in research-compatible JSON format.

Usage:
    uv run python scripts/dump_funnel.py 2026-03-31
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# Project root on sys.path
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from scripts.lgbrank_new_model_scorer import NewModelLGBRankScorer  # noqa: E402
from src.common.config import get_tushare_token  # noqa: E402
from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter  # noqa: E402
from src.data.clients.tushare_realtime import TushareRealtimeClient  # noqa: E402
from src.data.sources.local_concept_mapper import LocalConceptMapper  # noqa: E402
from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig  # noqa: E402
from src.strategy.strategies.v16_scanner import V16Scanner, V16StockData  # noqa: E402
from src.web.v15_scan_service import (  # noqa: E402
    LOOKBACK_DAYS,
    _build_stock_data,
    _fetch_history_ohlcv,
    get_trade_calendar,
)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


async def main(trade_date_str: str) -> None:
    trade_date = datetime.strptime(trade_date_str, "%Y-%m-%d").date()

    # --- Init clients ---
    tushare_token = get_tushare_token()
    tushare = TushareRealtimeClient(token=tushare_token)
    await tushare.start()

    hist_adapter = IQuantHistoricalAdapter(tushare, cache=None)
    concept_mapper = LocalConceptMapper()
    stock_filter = StockFilter(
        StockFilterConfig(
            exclude_bse=True, exclude_chinext=True, exclude_star=True, exclude_sme=False
        )
    )
    scorer = NewModelLGBRankScorer(
        _ROOT / "models" / "lgbrank_latest.txt",
        _ROOT / "models" / "feature_list.json",
    )

    # Use a stub for fundamentals_db (no PG needed locally).
    # ST filter will be skipped — negligible impact on funnel comparison.
    class _StubFDB:
        async def batch_filter_st(self, codes):
            return codes  # pass all through

        async def batch_get_fundamentals(self, codes):
            return {}

    scanner = V16Scanner(_StubFDB(), concept_mapper, stock_filter, scorer)

    try:
        # --- Step 0: Universe ---
        clean_boards, universe_codes = scanner.get_universe()
        logger.info(f"Universe: {len(clean_boards)} boards, {len(universe_codes)} stocks")

        # --- Fetch 9:40 quotes ---
        universe_list = sorted(universe_codes)
        quotes = await tushare.batch_get_early_quotes(universe_list)
        logger.info(f"Tushare returned {len(quotes)} quotes")

        # --- Fetch prev_close via Tushare daily ---
        calendar = await get_trade_calendar()
        prev_dates = [d for d in calendar if d < trade_date]
        prev_trade_date = prev_dates[-1]
        prev_closes = await tushare.fetch_prev_closes(prev_trade_date.strftime("%Y%m%d"))
        logger.info(f"Prev closes: {len(prev_closes)} stocks")

        # --- Fetch 37d history ---
        trading_codes = [c for c, q in quotes.items() if q.is_trading]
        logger.info(f"{len(trading_codes)} stocks trading, fetching history...")
        hist_raw = await _fetch_history_ohlcv(hist_adapter, trading_codes, trade_date)
        logger.info(f"History fetched for {len(hist_raw)} stocks")

        # --- Company names (from concept mapper board_stocks tuples) ---
        name_map: dict[str, str] = {}
        concept_mapper._ensure_loaded()
        for _board, members in concept_mapper._board_stocks.items():
            for code, name in members:
                if code not in name_map and name:
                    name_map[code] = name

        # --- Build V16StockData ---
        stock_data: dict[str, V16StockData] = {}
        for code in trading_codes:
            quote = quotes.get(code)
            if not quote or not quote.is_trading:
                continue
            pc = prev_closes.get(code)
            if not pc or pc <= 0:
                continue
            hr = hist_raw.get(code)
            if not hr:
                continue
            try:
                sd = _build_stock_data(code, name_map.get(code, ""), quote, pc, hr, trade_date)
            except RuntimeError:
                continue
            if sd is None:
                continue
            stock_data[code] = sd

        logger.info(f"Built {len(stock_data)} V16StockData")

        # --- Run V16 scan ---
        result = await scanner.scan(stock_data, clean_boards)

        # --- Build research-format JSON ---
        # Compute avg_market_open_gain
        gains = []
        for sd in stock_data.values():
            if sd.prev_close > 0 and sd.open_price > 0:
                gains.append((sd.open_price - sd.prev_close) / sd.prev_close)
        avg_market_open_gain = sum(gains) / len(gains) if gains else 0.0

        # Config section (mirrors V16Scanner params)
        config = {
            "gain_threshold": scanner.GAIN_FROM_OPEN_THRESHOLD / 100,
            "min_price": scanner.MIN_PRICE,
            "min_hot_count": scanner.MIN_STOCKS_PER_BOARD,
            "min_avg_gain": scanner.MIN_BOARD_AVG_GAIN / 100,
            "board_blacklist": [],
            "vol_max": scanner.MAX_TURNOVER_AMP,
            "vol_min": scanner.MIN_TURNOVER_AMP,
            "lookback": LOOKBACK_DAYS,
            "rev_percentile": 95.0,
            "rev_floor": 0.15,
            "shadow_max": scanner.MAX_UPPER_SHADOW,
            "shadow_exemption": scanner.SHADOW_EXEMPT_GAIN / 100,
        }

        # Funnel layers
        all_hot_board_codes = set()
        for codes in result.step2_boards_detail.values():
            all_hot_board_codes.update(codes)

        # Compute filtered codes per layer
        step2_all_boards = set(clean_boards.keys())
        step2_hot_boards = set(result.step2_boards_detail.keys())
        step2_filtered_boards = sorted(step2_all_boards - step2_hot_boards)

        step3_set = set(result.step3_codes)
        step2_codes_set = set(result.step2_codes)
        step3_filtered = sorted(step2_codes_set - step3_set)

        # step4 filtered = step3 passed - step4 passed (since step4 is subset of step3)
        step4_set = set(result.step4_codes)
        # In the research format, L1_gain_filter combines gain+price
        # Let's keep individual layers for accuracy

        step5_set = set(result.step5_codes)

        step6_set = set(result.step6_codes)

        step6_5_set = set(result.step6_5_codes)

        step6_6_set = set(result.step6_6_codes)

        funnel_layers = {
            "L0_board_clean": {
                "passed_count": 1,
                "filtered_count": 1,
                "passed_codes": [f"{len(clean_boards)} boards, {len(universe_codes)} stocks"],
                "filtered_codes": [f"{len(universe_codes)} universe stocks"],
            },
            "L3_hot_boards": {
                "passed_count": len(step2_hot_boards),
                "filtered_count": len(step2_filtered_boards),
                "passed_codes": sorted(step2_hot_boards),
                "filtered_codes": step2_filtered_boards,
            },
            "L1_gain_filter": {
                "passed_count": len(result.step3_codes),
                "filtered_count": len(step3_filtered),
                "passed_codes": result.step3_codes,
                "filtered_codes": step3_filtered,
            },
            "L4_price_filter": {
                "passed_count": len(result.step4_codes),
                "filtered_count": len(step3_set - step4_set),
                "passed_codes": result.step4_codes,
                "filtered_codes": sorted(step3_set - step4_set),
            },
            "L5_volume_filter": {
                "passed_count": len(result.step5_codes),
                "filtered_count": len(step4_set - step5_set),
                "passed_codes": result.step5_codes,
                "filtered_codes": sorted(step4_set - step5_set),
            },
            "L6_reversal_filter": {
                "passed_count": len(result.step6_codes),
                "filtered_count": len(step5_set - step6_set),
                "passed_codes": result.step6_codes,
                "filtered_codes": sorted(step5_set - step6_set),
            },
            "L6.5_limit_up": {
                "passed_count": len(result.step6_5_codes),
                "filtered_count": len(step6_set - step6_5_set),
                "passed_codes": result.step6_5_codes,
                "filtered_codes": sorted(step6_set - step6_5_set),
            },
            "L6.6_upper_shadow": {
                "passed_count": len(result.step6_6_codes),
                "filtered_count": len(step6_5_set - step6_6_set),
                "passed_codes": result.step6_6_codes,
                "filtered_codes": sorted(step6_5_set - step6_6_set),
            },
            "L_candidates": {
                "passed_count": result.final_candidates,
                "filtered_count": 0,
                "passed_codes": result.step6_6_codes,
                "filtered_codes": [],
            },
        }

        # hot_boards detail
        hot_boards = result.step2_boards_detail

        # candidates_detail
        candidates_detail = []
        for code in result.step6_6_codes:
            sd = stock_data[code]
            gain_from_open = (
                (sd.price_940 - sd.open_price) / sd.open_price if sd.open_price > 0 else 0
            )
            open_gap = (sd.open_price - sd.prev_close) / sd.prev_close if sd.prev_close > 0 else 0
            expected_early = sd.avg_daily_volume * scanner.TURNOVER_FRACTION
            turnover_amp = sd.volume_940 / expected_early if expected_early > 0 else 0
            body_top = max(sd.open_price, sd.price_940)
            upper_shadow = (sd.high_940 - body_top) / sd.open_price if sd.open_price > 0 else 0

            candidates_detail.append(
                {
                    "code": code,
                    "name": sd.name,
                    "board": result.stock_best_board.get(code, ""),
                    "open": round(sd.open_price, 2),
                    "preclose": round(sd.prev_close, 2),
                    "price_940": round(sd.price_940, 2),
                    "high_940": round(sd.high_940, 2),
                    "low_940": round(sd.low_940, 2),
                    "volume_940": round(sd.volume_940, 1),
                    "gain_from_open_pct": round(gain_from_open, 6),
                    "open_gap_pct": round(open_gap, 6),
                    "turnover_amp": round(turnover_amp, 4),
                    "upper_shadow": round(upper_shadow, 6),
                    "avg_daily_volume": round(sd.avg_daily_volume, 1),
                    "trend_5d": round(sd.trend_5d, 6),
                    "trend_10d": round(sd.trend_10d, 6),
                    "avg_daily_return_20d": round(sd.avg_daily_return_20d, 6),
                    "volatility_20d": round(sd.volatility_20d, 6),
                    "consecutive_up_days": sd.consecutive_up_days,
                }
            )

        # candidates_lgbrank
        candidates_lgbrank = [
            {
                "rank": s.rank,
                "code": s.code,
                "name": s.name,
                "lgb_score": round(s.score, 6),
                "buy_price": round(s.buy_price, 2),
            }
            for s in result.all_scored
        ]

        # Summary
        summary = {
            "total_stocks": len(universe_codes),
            "main_board_stocks": len(stock_data),
            "snapshot_coverage": len(quotes),
            "hot_boards": len(step2_hot_boards),
            "candidates_after_funnel": result.final_candidates,
            "avg_market_open_gain": round(avg_market_open_gain, 6),
        }

        dump = {
            "date": trade_date_str,
            "config": config,
            "summary": summary,
            "funnel_layers": funnel_layers,
            "hot_boards": hot_boards,
            "candidates_detail": candidates_detail,
            "candidates_lgbrank": candidates_lgbrank,
        }

        # Write output
        out_path = _ROOT / "data" / f"funnel_dump_{trade_date_str.replace('-', '')}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(dump, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info(f"Funnel dump written to {out_path}")

        # Print summary
        print(f"\n=== Funnel Dump {trade_date_str} ===")
        print(f"Universe: {len(universe_codes)} stocks, {len(clean_boards)} boards")
        print(f"Stock data built: {len(stock_data)}")
        print(f"Hot boards: {len(step2_hot_boards)}")
        print(f"Step 3 (gain): {result.step3_count}")
        print(f"Step 4 (price): {result.step4_count}")
        print(f"Step 5 (volume): {result.step5_count}")
        print(f"Step 6 (reversal): {result.step6_count}")
        print(f"Step 6.5 (limit-up): {result.step6_5_count}")
        print(f"Step 6.6 (shadow): {result.step6_6_count}")
        print(f"Final candidates: {result.final_candidates}")
        if result.recommended:
            top = result.recommended[0]
            print(f"Top-1: {top.code} {top.name} LGB={top.score:.4f} price={top.buy_price:.2f}")

    finally:
        await tushare.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: uv run python scripts/dump_funnel.py YYYY-MM-DD")
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))
