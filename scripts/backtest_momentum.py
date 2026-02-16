#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
回测动量板块策略。

策略逻辑：
1. 预筛：开盘涨幅 > -0.5% 的沪深主板非ST股票（宽进）
2. 9:40 筛选：(9:40价 - 开盘价) / 开盘价 > 0.56% 的股票
3. 反查概念板块，找到 ≥2 只入选股的"热门板块"
4. 拉取热门板块全部成分股
5. 成分股筛选：9:40 vs 开盘 > 0.56% 且 PE(TTM) 在板块中位数 ±30%

用法：
    uv run python scripts/backtest_momentum.py --date 2026-02-10
    uv run python scripts/backtest_momentum.py --date 2026-02-10 --notify
"""

import argparse
import asyncio
import io
import logging
import sys
from datetime import date
from pathlib import Path

# Fix Windows console encoding for Chinese characters
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.common.feishu_bot import FeishuBot
from src.data.clients.ifind_http_client import IFinDHttpClient, IFinDHttpError
from src.data.database.fundamentals_db import create_fundamentals_db_from_config
from src.data.sources.concept_mapper import ConceptMapper
from src.strategy.strategies.momentum_sector_scanner import (
    MomentumSectorScanner,
    PriceSnapshot,
    ScanResult,
)

# Re-use the scanner's 9:40 price fetcher via a standalone helper
# (The scanner's _fetch_940_prices is an instance method, but the logic is reusable here.)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def fetch_main_board_prices_for_date(
    client: IFinDHttpClient,
    trade_date: date,
) -> dict[str, PriceSnapshot]:
    """
    Fetch open + prev_close for main board stocks on a given date.

    Uses iwencai to get stocks with opening gain > -0.5% (relaxed pre-filter),
    then the scanner's Step 1 applies the 9:40 gain-from-open filter.

    Args:
        client: iFinD HTTP client.
        trade_date: Target date for backtest.

    Returns:
        Dict of stock_code → PriceSnapshot for stocks meeting initial gain criteria.
    """
    date_str = trade_date.strftime("%Y%m%d")
    query = f"{date_str}开盘涨幅大于-0.5%的沪深主板非ST股票"
    logger.info(f"iwencai query: {query}")

    try:
        result = await client.smart_stock_picking(query, "stock")
    except IFinDHttpError as e:
        logger.error(f"iwencai query failed: {e}")
        return {}

    tables = result.get("tables", [])
    if not tables:
        logger.warning("iwencai returned empty results")
        return {}

    snapshots: dict[str, PriceSnapshot] = {}

    for table_wrapper in tables:
        if not isinstance(table_wrapper, dict):
            continue

        table = table_wrapper.get("table", table_wrapper)
        if not isinstance(table, dict):
            continue

        # Parse columns from iwencai response
        codes = None
        names = None

        for col_name, col_data in table.items():
            if "代码" in col_name:
                codes = col_data
            elif "简称" in col_name or "名称" in col_name:
                names = col_data

        if not codes:
            continue

        # For each stock from iwencai, we still need open + prev_close
        # for the full pipeline. Fetch via history_quotes.
        stock_codes_raw = []
        stock_names_map: dict[str, str] = {}
        for i in range(len(codes)):
            raw = str(codes[i]).strip()
            # Normalize code
            bare = raw
            for suffix in (".SZ", ".SH", ".BJ", ".sz", ".sh", ".bj"):
                if bare.endswith(suffix):
                    bare = bare[: -len(suffix)]
                    break
            if len(bare) == 6 and bare.isdigit():
                stock_codes_raw.append(raw)
                name = str(names[i]).strip() if names and i < len(names) else ""
                stock_names_map[bare] = name

    if not stock_codes_raw:
        logger.warning("No valid stock codes from iwencai")
        return {}

    logger.info(f"iwencai found {len(stock_codes_raw)} stocks, fetching prices...")

    # Batch fetch open + prev_close via history_quotes
    batch_size = 50
    for i in range(0, len(stock_codes_raw), batch_size):
        batch = stock_codes_raw[i : i + batch_size]
        # Ensure proper suffix format
        formatted_codes = []
        for raw in batch:
            if "." in raw:
                formatted_codes.append(raw)
            else:
                suffix = ".SH" if raw.startswith("6") else ".SZ"
                formatted_codes.append(f"{raw}{suffix}")

        codes_str = ",".join(formatted_codes)
        date_fmt = trade_date.strftime("%Y-%m-%d")

        try:
            data = await client.history_quotes(
                codes=codes_str,
                indicators="open,preClose",
                start_date=date_fmt,
                end_date=date_fmt,
            )

            for table_entry in data.get("tables", []):
                thscode = table_entry.get("thscode", "")
                bare = thscode.split(".")[0] if thscode else ""
                if not bare:
                    continue

                tbl = table_entry.get("table", {})
                open_vals = tbl.get("open", [])
                prev_vals = tbl.get("preClose", [])

                if open_vals and prev_vals:
                    open_price = float(open_vals[0])
                    prev_close = float(prev_vals[0])

                    if prev_close > 0:
                        snapshots[bare] = PriceSnapshot(
                            stock_code=bare,
                            stock_name=stock_names_map.get(bare, ""),
                            open_price=open_price,
                            prev_close=prev_close,
                            latest_price=open_price,  # Backtest: use open as latest
                        )

        except IFinDHttpError as e:
            logger.error(f"history_quotes batch failed: {e}")

    logger.info(f"Fetched prices for {len(snapshots)} stocks")

    # Fetch 9:40 price to use as latest_price (for gain check and buy price)
    if snapshots:
        prices_940 = await _fetch_940_prices(client, list(snapshots.keys()), trade_date)
        updated = 0
        for code, price in prices_940.items():
            if code in snapshots and price > 0:
                snapshots[code].latest_price = price
                updated += 1
        logger.info(f"Updated {updated}/{len(snapshots)} stocks with 9:40 price")

    return snapshots


async def _fetch_940_prices(
    client: IFinDHttpClient,
    stock_codes: list[str],
    trade_date: date,
) -> dict[str, float]:
    """Fetch the 9:40 price for stocks via high_frequency API (1-min bars)."""
    result: dict[str, float] = {}
    batch_size = 50
    start_time = f"{trade_date} 09:30:00"
    end_time = f"{trade_date} 09:40:00"

    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i : i + batch_size]
        codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)

        try:
            data = await client.high_frequency(
                codes=codes_str,
                indicators="close",
                start_time=start_time,
                end_time=end_time,
                function_para={"Interval": "1"},  # 1-minute bars
            )

            for table_entry in data.get("tables", []):
                thscode = table_entry.get("thscode", "")
                bare_code = thscode.split(".")[0] if thscode else ""
                if not bare_code:
                    continue

                tbl = table_entry.get("table", {})
                close_vals = tbl.get("close", [])
                if close_vals:
                    last_close = close_vals[-1]
                    if last_close is not None:
                        result[bare_code] = float(last_close)

        except IFinDHttpError as e:
            logger.warning(f"high_frequency 9:40 fetch failed for batch: {e}")

    return result


def print_scan_result(result: ScanResult, trade_date: date) -> None:
    """Pretty-print the scan result to console."""
    print(f"\n{'=' * 70}")
    print(f"  动量板块策略回测结果 — {trade_date}")
    print(f"{'=' * 70}")
    print(f"  初筛: {len(result.initial_gainers)} 只(9:40 vs 开盘 >0.56%)")
    print(f"  热门板块: {len(result.hot_boards)} 个")
    print(f"  最终选股: {len(result.selected_stocks)} 只")
    print(f"{'=' * 70}")

    if not result.selected_stocks:
        print("  (无符合条件的股票)")
        return

    # Group by board
    board_stocks: dict[str, list] = {}
    for stock in result.selected_stocks:
        if stock.board_name not in board_stocks:
            board_stocks[stock.board_name] = []
        board_stocks[stock.board_name].append(stock)

    for board_name, stocks in board_stocks.items():
        gainer_codes = result.hot_boards.get(board_name, [])
        print(f"\n  🔥 {board_name} (触发: {', '.join(gainer_codes)})")
        print(f"  {'─' * 60}")
        print(f"  {'代码':>8}  {'名称':<10}  {'开盘涨幅':>8}  {'PE(TTM)':>8}  {'板块均值':>8}")
        print(f"  {'─' * 60}")

        for s in stocks:
            print(
                f"  {s.stock_code:>8}  {s.stock_name:<10}  "
                f"{s.open_gain_pct:>+7.2f}%  {s.pe_ttm:>8.1f}  {s.board_avg_pe:>8.1f}"
            )

    print()


async def run_backtest(trade_date: date, notify: bool = False) -> ScanResult:
    """Run backtest for a single date."""
    logger.info(f"Running backtest for {trade_date}")

    # Initialize components
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

        # Fetch price data for the date
        price_snapshots = await fetch_main_board_prices_for_date(ifind_client, trade_date)

        if not price_snapshots:
            logger.warning(f"No price data for {trade_date}")
            return ScanResult()

        # Run the scan (pass trade_date so constituent prices use history_quotes)
        result = await scanner.scan(price_snapshots, trade_date=trade_date)

        # Print results
        print_scan_result(result, trade_date)

        # Send Feishu notification if requested
        if notify and result.has_results:
            bot = FeishuBot()
            if bot.is_configured():
                await bot.send_momentum_scan_result(
                    selected_stocks=result.selected_stocks,
                    hot_boards=result.hot_boards,
                    initial_gainer_count=len(result.initial_gainers),
                    scan_time=result.scan_time,
                    recommended_stock=result.recommended_stock,
                )
                logger.info("Feishu notification sent")
            else:
                logger.warning("Feishu bot not configured, skipping notification")

        return result

    finally:
        await fundamentals_db.close()
        await ifind_client.stop()


def main():
    parser = argparse.ArgumentParser(description="回测动量板块策略")
    parser.add_argument(
        "--date",
        "-d",
        type=str,
        required=True,
        help="回测日期 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--notify",
        "-n",
        action="store_true",
        help="发送飞书通知",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="显示调试信息",
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    trade_date = date.fromisoformat(args.date)
    asyncio.run(run_backtest(trade_date, notify=args.notify))


if __name__ == "__main__":
    main()
