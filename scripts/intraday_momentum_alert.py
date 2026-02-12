#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
盘中动量板块策略实时监控。

9:30 开始每30秒轮询，追踪涨幅超过5%的沪深主板非ST股票。
9:40 时运行完整策略流程，选股后飞书通知。

用法：
    uv run python scripts/intraday_momentum_alert.py
    uv run python scripts/intraday_momentum_alert.py --no-wait    # 不等9:30，立即运行
    uv run python scripts/intraday_momentum_alert.py --end 09:45  # 自定义结束时间
"""

import argparse
import asyncio
import io
import logging
import sys
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

# Fix Windows console encoding for Chinese characters
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

from pathlib import Path

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
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

# Monitoring parameters
POLL_INTERVAL_SECONDS = 30
DEFAULT_START_TIME = time(9, 30)
DEFAULT_END_TIME = time(9, 40)


async def poll_realtime_gainers(
    client: IFinDHttpClient,
) -> dict[str, PriceSnapshot]:
    """
    Poll real-time quotes to find stocks with >5% gain.

    Uses iwencai: "涨幅大于5%的沪深主板非ST股票"
    Then fetches open + preClose via real_time_quotation for full data.

    Returns:
        Dict of stock_code → PriceSnapshot.
    """
    try:
        result = await client.smart_stock_picking(
            "涨幅大于5%的沪深主板非ST股票", "stock"
        )
    except IFinDHttpError as e:
        logger.error(f"iwencai realtime query failed: {e}")
        return {}

    tables = result.get("tables", [])
    if not tables:
        return {}

    # Extract stock codes and names
    raw_codes: list[str] = []
    names_map: dict[str, str] = {}

    for table_wrapper in tables:
        if not isinstance(table_wrapper, dict):
            continue
        table = table_wrapper.get("table", table_wrapper)
        if not isinstance(table, dict):
            continue

        codes = None
        names = None
        for col_name, col_data in table.items():
            if "代码" in col_name:
                codes = col_data
            elif "简称" in col_name or "名称" in col_name:
                names = col_data

        if not codes:
            continue

        for i in range(len(codes)):
            raw = str(codes[i]).strip()
            bare = raw
            for suffix in (".SZ", ".SH", ".BJ"):
                if bare.upper().endswith(suffix):
                    bare = bare[: -len(suffix)]
                    break
            if len(bare) == 6 and bare.isdigit():
                raw_codes.append(raw)
                name = str(names[i]).strip() if names and i < len(names) else ""
                names_map[bare] = name

    if not raw_codes:
        return {}

    # Fetch real-time prices (open, preClose, latest) in batches
    snapshots: dict[str, PriceSnapshot] = {}
    batch_size = 50

    for i in range(0, len(raw_codes), batch_size):
        batch = raw_codes[i : i + batch_size]
        formatted = []
        for raw in batch:
            if "." in raw:
                formatted.append(raw)
            else:
                suffix = ".SH" if raw.startswith("6") else ".SZ"
                formatted.append(f"{raw}{suffix}")

        codes_str = ",".join(formatted)

        try:
            data = await client.real_time_quotation(
                codes=codes_str,
                indicators="open,preClose,latest",
            )

            for table_entry in data.get("tables", []):
                thscode = table_entry.get("thscode", "")
                bare = thscode.split(".")[0] if thscode else ""
                if not bare:
                    continue

                tbl = table_entry.get("table", {})
                open_vals = tbl.get("open", [])
                prev_vals = tbl.get("preClose", [])
                latest_vals = tbl.get("latest", [])

                if open_vals and prev_vals and latest_vals:
                    open_price = float(open_vals[0]) if open_vals[0] else 0.0
                    prev_close = float(prev_vals[0]) if prev_vals[0] else 0.0
                    latest = float(latest_vals[0]) if latest_vals[0] else 0.0

                    if prev_close > 0:
                        snapshots[bare] = PriceSnapshot(
                            stock_code=bare,
                            stock_name=names_map.get(bare, ""),
                            open_price=open_price,
                            prev_close=prev_close,
                            latest_price=latest,
                        )

        except IFinDHttpError as e:
            logger.error(f"real_time_quotation batch failed: {e}")

    return snapshots


async def wait_until(target: time) -> None:
    """Wait until a specific Beijing time."""
    while True:
        now = datetime.now(BEIJING_TZ).time()
        if now >= target:
            return

        # Calculate seconds to wait
        now_dt = datetime.now(BEIJING_TZ)
        target_dt = now_dt.replace(
            hour=target.hour, minute=target.minute, second=target.second
        )
        delta = (target_dt - now_dt).total_seconds()

        if delta <= 0:
            return

        wait = min(delta, 10)  # Check every 10s
        logger.info(f"Waiting for {target.strftime('%H:%M')}... ({delta:.0f}s remaining)")
        await asyncio.sleep(wait)


async def monitor(
    start_time: time = DEFAULT_START_TIME,
    end_time: time = DEFAULT_END_TIME,
    no_wait: bool = False,
) -> None:
    """
    Main monitoring loop.

    1. Wait for start_time (or skip if no_wait)
    2. Poll every POLL_INTERVAL_SECONDS until end_time
    3. Accumulate all >5% gainers seen during the window
    4. At end_time, run full strategy scan
    5. Send Feishu notification
    """
    ifind_client = IFinDHttpClient()
    fundamentals_db = create_fundamentals_db_from_config()

    try:
        await ifind_client.start()
        await fundamentals_db.connect()
        logger.info("Components initialized")

        # Wait for market open
        if not no_wait:
            logger.info(f"Waiting for {start_time.strftime('%H:%M')} to start monitoring...")
            await wait_until(start_time)

        logger.info("Starting monitoring loop")

        # Accumulate all snapshots across polls (latest snapshot wins)
        accumulated: dict[str, PriceSnapshot] = {}
        poll_count = 0

        while True:
            now = datetime.now(BEIJING_TZ).time()

            # Check if we've passed end time
            if now >= end_time and not no_wait:
                logger.info(f"Reached {end_time.strftime('%H:%M')}, stopping polls")
                break

            # Poll
            poll_count += 1
            logger.info(f"Poll #{poll_count} at {now.strftime('%H:%M:%S')}")

            snapshots = await poll_realtime_gainers(ifind_client)
            logger.info(f"  Found {len(snapshots)} stocks with >5% gain")

            # Accumulate (update with latest prices)
            accumulated.update(snapshots)

            # In no_wait mode, run just once
            if no_wait:
                break

            # Wait for next poll
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

        # Run full strategy scan on accumulated data
        logger.info(
            f"Running strategy scan on {len(accumulated)} accumulated stocks..."
        )

        concept_mapper = ConceptMapper(ifind_client)
        scanner = MomentumSectorScanner(
            ifind_client=ifind_client,
            fundamentals_db=fundamentals_db,
            concept_mapper=concept_mapper,
        )

        result = await scanner.scan(accumulated)

        # Print results
        scan_time = datetime.now(BEIJING_TZ)
        print(f"\n{'=' * 70}")
        print(f"  动量板块策略选股结果 — {scan_time.strftime('%Y-%m-%d %H:%M')}")
        print(f"{'=' * 70}")
        print(f"  轮询次数: {poll_count}")
        print(f"  累计涨幅>5%: {len(accumulated)} 只")
        print(f"  初筛通过: {len(result.initial_gainers)} 只")
        print(f"  热门板块: {len(result.hot_boards)} 个")
        print(f"  最终选股: {len(result.selected_stocks)} 只")
        print(f"{'=' * 70}")

        if result.selected_stocks:
            for s in result.selected_stocks:
                print(
                    f"  {s.stock_code} {s.stock_name:<10}  "
                    f"板块:{s.board_name}  涨幅{s.open_gain_pct:+.2f}%  "
                    f"PE {s.pe_ttm:.1f} (均值{s.board_avg_pe:.1f})"
                )
        else:
            print("  (无符合条件的股票)")

        # Send Feishu notification
        bot = FeishuBot()
        if bot.is_configured():
            logger.info("Sending Feishu notification...")
            await bot.send_momentum_scan_result(
                selected_stocks=result.selected_stocks,
                hot_boards=result.hot_boards,
                initial_gainer_count=len(result.initial_gainers),
                scan_time=scan_time,
            )
            logger.info("Feishu notification sent")
        else:
            logger.warning("Feishu bot not configured, skipping notification")

    finally:
        await fundamentals_db.close()
        await ifind_client.stop()
        logger.info("Monitoring stopped")


def main():
    parser = argparse.ArgumentParser(description="盘中动量板块策略实时监控")
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="不等待9:30，立即运行一次",
    )
    parser.add_argument(
        "--start",
        type=str,
        default="09:30",
        help="监控开始时间 (HH:MM, 默认 09:30)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default="09:40",
        help="监控结束时间 (HH:MM, 默认 09:40)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="显示调试信息",
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    start_time = time(*map(int, args.start.split(":")))
    end_time = time(*map(int, args.end.split(":")))

    asyncio.run(monitor(start_time, end_time, no_wait=args.no_wait))


if __name__ == "__main__":
    main()
