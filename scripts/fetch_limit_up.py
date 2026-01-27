#!/usr/bin/env python
"""
Fetch limit-up stocks data using iFinD API.

Usage:
    # Fetch today's limit-up stocks
    uv run python scripts/fetch_limit_up.py

    # Fetch for a specific date
    uv run python scripts/fetch_limit_up.py --date 2026-01-27

    # Backfill historical data
    uv run python scripts/fetch_limit_up.py --backfill --days 30

    # Show statistics
    uv run python scripts/fetch_limit_up.py --stats --start 2026-01-01 --end 2026-01-27
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.common.config import Config
from src.data.sources.ifind_limit_up import IFinDLimitUpSource


def setup_logging(level: str = "INFO") -> None:
    """Configure logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


async def fetch_today(source: IFinDLimitUpSource, date: str | None = None) -> None:
    """Fetch limit-up stocks for today or specified date."""
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    print(f"Fetching limit-up stocks for {date}...")

    try:
        count = await source.fetch_and_save(date)
        print(f"Successfully saved {count} limit-up stocks")

        # Show some results
        stocks = await source.query_by_date(date)
        if stocks:
            print("\nTop 10 limit-up stocks by time:")
            print("-" * 80)
            for stock in stocks[:10]:
                sealed = "封板" if stock.open_count == 0 else f"开板{stock.open_count}次"
                print(
                    f"{stock.limit_up_time} | {stock.stock_code} | "
                    f"{stock.stock_name:8} | {sealed:8} | {stock.reason or '无'}"
                )
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise


async def backfill(source: IFinDLimitUpSource, days: int) -> None:
    """Backfill historical limit-up data."""
    print(f"Backfilling {days} trading days of limit-up data...")

    results = await source.backfill(days=days)

    total = sum(results.values())
    print(f"\nBackfill complete. Total stocks: {total}")
    print("\nDaily breakdown:")
    for date, count in sorted(results.items(), reverse=True):
        if count > 0:
            print(f"  {date}: {count} stocks")


async def show_statistics(
    source: IFinDLimitUpSource,
    start_date: str,
    end_date: str,
) -> None:
    """Show statistics for a date range."""
    stats = await source.get_statistics(start_date, end_date)

    print(f"\nLimit-up Statistics ({start_date} to {end_date})")
    print("-" * 50)
    print(f"Trading days with data: {stats['trading_days']}")
    print(f"Total limit-up stocks:  {stats['total_stocks']}")
    print(f"Average per day:        {stats['avg_per_day']:.1f}")
    print(f"Maximum per day:        {stats['max_per_day']}")
    print(f"Minimum per day:        {stats['min_per_day']}")


async def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Fetch limit-up stocks data using iFinD API"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Trading date (YYYY-MM-DD format, default: today)",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Backfill historical data",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to backfill (default: 30)",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show statistics for a date range",
    )
    parser.add_argument(
        "--start",
        type=str,
        help="Start date for statistics (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date for statistics (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/market-data-config.yaml",
        help="Path to configuration file",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )

    args = parser.parse_args()

    setup_logging(args.log_level)

    # Load configuration
    config_path = project_root / args.config
    if config_path.exists():
        config = Config.load(config_path)
        db_path = config.get_str(
            "market_data.limit_up.database.path",
            default="data/limit_up.db"
        )
    else:
        db_path = "data/limit_up.db"

    # Initialize source
    source = IFinDLimitUpSource(db_path=project_root / db_path)

    try:
        await source.start()

        if args.backfill:
            await backfill(source, args.days)
        elif args.stats:
            if not args.start or not args.end:
                print("Error: --stats requires --start and --end dates")
                sys.exit(1)
            await show_statistics(source, args.start, args.end)
        else:
            await fetch_today(source, args.date)

    finally:
        await source.stop()


if __name__ == "__main__":
    asyncio.run(main())
