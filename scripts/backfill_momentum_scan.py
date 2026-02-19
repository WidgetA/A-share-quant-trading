#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Backfill momentum scan selected stocks into PostgreSQL.

Iterates over trading days in a date range, runs the full scan pipeline
(Steps 1-5.6), fetches T+1 open prices, calculates return, and saves to DB.

Supports resume: skips dates already in the database.

Usage:
    uv run python scripts/backfill_momentum_scan.py --start 2023-02-19 --end 2026-02-19
    uv run python scripts/backfill_momentum_scan.py --start 2025-12-01 --end 2026-02-19 --debug
"""

import argparse
import asyncio
import io
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

# Fix Windows console encoding for Chinese characters
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.backtest_momentum import fetch_main_board_prices_for_date
from src.data.clients.ifind_http_client import IFinDHttpClient, IFinDHttpError
from src.data.database.fundamentals_db import create_fundamentals_db_from_config
from src.data.database.momentum_scan_db import create_momentum_scan_db_from_config
from src.data.sources.concept_mapper import ConceptMapper
from src.strategy.strategies.momentum_sector_scanner import MomentumSectorScanner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _calc_net_return_pct(buy_price: float, sell_price: float) -> float:
    """Calculate net return % after transaction costs (1 lot = 100 shares).

    Cost model (same as interval backtest):
    - Buy commission: max(0.3%, 5 yuan)
    - Sell commission: max(0.3%, 5 yuan)
    - Stamp tax (sell only): 0.05%
    - Transfer fee: 0.001% each way
    """
    shares = 100
    buy_amount = shares * buy_price
    buy_commission = max(buy_amount * 0.003, 5.0)
    buy_transfer = buy_amount * 0.00001
    total_buy_cost = buy_amount + buy_commission + buy_transfer

    sell_amount = shares * sell_price
    sell_commission = max(sell_amount * 0.003, 5.0)
    sell_transfer = sell_amount * 0.00001
    sell_stamp = sell_amount * 0.0005
    net_sell = sell_amount - sell_commission - sell_transfer - sell_stamp

    return (net_sell - total_buy_cost) / total_buy_cost * 100 if total_buy_cost > 0 else 0.0


def get_trading_calendar(start_date: date, end_date: date) -> list[date]:
    """Get trading days via AKShare."""
    try:
        import akshare as ak

        df = ak.tool_trade_date_hist_sina()
        all_dates = set(df["trade_date"].dt.date)
        days = sorted(d for d in all_dates if start_date <= d <= end_date)
        if days:
            logger.info(f"Trading calendar: {len(days)} days in [{start_date}, {end_date}]")
            return days
    except Exception as e:
        logger.warning(f"AKShare trading calendar failed: {e}")

    # Fallback: weekdays
    logger.warning("Falling back to weekday generation")
    days = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


async def fetch_batch_open_prices(
    ifind_client: IFinDHttpClient,
    stock_codes: list[str],
    target_date: date,
) -> dict[str, float]:
    """Fetch open prices for multiple stocks on a single date.

    Returns: stock_code → open_price
    """
    if not stock_codes:
        return {}

    result: dict[str, float] = {}
    batch_size = 50

    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i : i + batch_size]
        codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)
        date_str = target_date.strftime("%Y-%m-%d")

        try:
            data = await ifind_client.history_quotes(
                codes=codes_str,
                indicators="open,preClose",
                start_date=date_str,
                end_date=date_str,
            )
            for table_entry in data.get("tables", []):
                thscode = table_entry.get("thscode", "")
                bare = thscode.split(".")[0] if thscode else ""
                if not bare:
                    continue
                tbl = table_entry.get("table", {})
                opens = tbl.get("open", [])
                if opens and opens[0] is not None:
                    result[bare] = float(opens[0])
        except IFinDHttpError as e:
            logger.warning(f"Batch open price fetch failed for {target_date}: {e}")

    return result


async def fetch_growth_rates(
    ifind_client: IFinDHttpClient,
    stock_codes: list[str],
) -> dict[str, float]:
    """Fetch YoY quarterly revenue growth rates via iwencai.

    Same query as scanner Step 6: "{codes} 同比季度收入增长率".
    Returns: stock_code → growth_rate (%)
    """
    if not stock_codes:
        return {}

    result: dict[str, float] = {}
    # iwencai can handle ~30 codes at a time
    batch_size = 30

    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i : i + batch_size]
        codes_str = ";".join(batch)
        query = f"{codes_str} 同比季度收入增长率"

        try:
            data = await ifind_client.smart_stock_picking(query, "stock")
            tables = data.get("tables", [])
            if not tables:
                continue

            table = tables[0].get("table", {})
            code_col = table.get("股票代码", [])

            # Find the growth column (dynamic name from iwencai)
            growth_col_values = []
            for col_name, col_values in table.items():
                if "收入" in col_name and ("增长率" in col_name or "同比" in col_name):
                    growth_col_values = col_values
                    break

            if code_col and growth_col_values:
                for j, code in enumerate(code_col):
                    bare = code.split(".")[0] if isinstance(code, str) else str(code)
                    if j < len(growth_col_values):
                        val = growth_col_values[j]
                        if val is not None and val != "--":
                            try:
                                result[bare] = float(val)
                            except (ValueError, TypeError):
                                pass

        except IFinDHttpError as e:
            logger.warning(f"Growth rate fetch failed: {e}")

    return result


async def backfill(start_date: date, end_date: date, delay: float = 1.0) -> None:
    """Run backfill for date range."""
    # Get full calendar (extend beyond end_date for T+1 lookups)
    full_calendar = get_trading_calendar(start_date, end_date + timedelta(days=10))
    trading_days = [d for d in full_calendar if start_date <= d <= end_date]

    if not trading_days:
        logger.error("No trading days in range")
        return

    # Build T+1 map
    next_day_map: dict[date, date] = {}
    for idx, d in enumerate(full_calendar):
        if idx + 1 < len(full_calendar):
            next_day_map[d] = full_calendar[idx + 1]

    # Initialize DB and check existing data
    scan_db = create_momentum_scan_db_from_config()
    await scan_db.connect()

    existing_dates = set(
        await scan_db.get_dates_with_data(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )
    )
    remaining = [d for d in trading_days if d not in existing_dates]
    logger.info(
        f"Total trading days: {len(trading_days)}, "
        f"already backfilled: {len(existing_dates)}, "
        f"remaining: {len(remaining)}"
    )

    if not remaining:
        logger.info("All dates already backfilled, nothing to do")
        await scan_db.close()
        return

    # Initialize iFinD client and scanner
    ifind_client = IFinDHttpClient()
    fundamentals_db = create_fundamentals_db_from_config()

    try:
        await ifind_client.start()
        await fundamentals_db.connect()

        concept_mapper = ConceptMapper(ifind_client)
        scanner = MomentumSectorScanner(
            ifind_client=ifind_client,
            fundamentals_db=fundamentals_db,
            concept_mapper=concept_mapper,
        )

        success_count = 0
        error_count = 0
        total_stocks = 0
        t_start = time.time()

        for i, day in enumerate(remaining):
            progress = f"[{i + 1}/{len(remaining)}]"
            try:
                # Fetch prices (reuse backtest_momentum logic)
                price_snapshots = await fetch_main_board_prices_for_date(ifind_client, day)

                if not price_snapshots:
                    logger.info(f"{progress} {day}: no price data (non-trading day?)")
                    success_count += 1
                    continue

                # Run scan pipeline (Steps 1-5.6)
                result = await scanner.scan(price_snapshots, trade_date=day)
                selected = result.selected_stocks
                all_snapshots = result.all_snapshots

                if not selected:
                    logger.info(f"{progress} {day}: 0 stocks selected")
                    success_count += 1
                    continue

                # Collect unique stock codes for T+1 fetch
                unique_codes = list({s.stock_code for s in selected})

                # Fetch T+1 open prices
                next_day = next_day_map.get(day)
                next_day_opens: dict[str, float] = {}
                if next_day:
                    next_day_opens = await fetch_batch_open_prices(
                        ifind_client, unique_codes, next_day
                    )

                # Fetch growth rates for all selected stocks
                growth_rates = await fetch_growth_rates(ifind_client, unique_codes)

                # Build rows with price data, return, and growth rate
                db_rows: list[dict] = []
                for s in selected:
                    snap = all_snapshots.get(s.stock_code)
                    buy_price = snap.latest_price if snap else 0.0
                    open_price = snap.open_price if snap else 0.0
                    prev_close = snap.prev_close if snap else 0.0

                    ndo = next_day_opens.get(s.stock_code)
                    ret = None
                    if ndo and buy_price > 0:
                        ret = round(_calc_net_return_pct(buy_price, ndo), 4)

                    db_rows.append(
                        {
                            "stock_code": s.stock_code,
                            "stock_name": s.stock_name,
                            "board_name": s.board_name,
                            "open_gain_pct": s.open_gain_pct,
                            "pe_ttm": s.pe_ttm,
                            "board_avg_pe": s.board_avg_pe,
                            "open_price": open_price,
                            "prev_close": prev_close,
                            "buy_price": buy_price,
                            "next_day_open": ndo,
                            "return_pct": ret,
                            "growth_rate": growth_rates.get(s.stock_code),
                        }
                    )

                saved = await scan_db.save_day(day, db_rows)
                total_stocks += saved
                success_count += 1

                elapsed = time.time() - t_start
                avg = elapsed / (i + 1)
                eta = avg * (len(remaining) - i - 1)
                logger.info(
                    f"{progress} {day}: {saved} stocks saved "
                    f"(elapsed: {elapsed:.0f}s, ETA: {eta:.0f}s)"
                )

            except Exception as e:
                error_count += 1
                logger.error(f"{progress} {day}: ERROR — {e}")

            # Rate limit
            if delay > 0 and i < len(remaining) - 1:
                await asyncio.sleep(delay)

        elapsed_total = time.time() - t_start
        logger.info(
            f"Backfill complete in {elapsed_total:.0f}s: "
            f"{success_count} days OK, {error_count} errors, "
            f"{total_stocks} total stocks saved"
        )

    finally:
        await fundamentals_db.close()
        await ifind_client.stop()
        await scan_db.close()


def main():
    parser = argparse.ArgumentParser(description="Backfill momentum scan selected stocks")
    parser.add_argument("--start", "-s", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", "-e", type=str, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--delay", type=float, default=1.0, help="Delay between days in seconds (default: 1.0)"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    if end < start:
        logger.error("End date must be >= start date")
        sys.exit(1)

    asyncio.run(backfill(start, end, delay=args.delay))


if __name__ == "__main__":
    main()
