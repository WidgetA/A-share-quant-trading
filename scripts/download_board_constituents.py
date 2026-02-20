#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载同花顺概念板块成分股数据，保存到 data/board_constituents.json。

读取 data/sectors.json 中的概念板块名称，通过 iFinD iwencai 查询每个板块的成分股。
运行一次后本地即有全量板块→成分股映射，运行时不再需要 API。

用法：
    uv run python scripts/download_board_constituents.py
    uv run python scripts/download_board_constituents.py --dry-run   # 只打印板块列表
"""

import argparse
import asyncio
import io
import json
import logging
import sys
from pathlib import Path

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.data.clients.ifind_http_client import IFinDHttpClient, IFinDHttpError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SECTORS_PATH = PROJECT_ROOT / "data" / "sectors.json"
OUTPUT_PATH = PROJECT_ROOT / "data" / "board_constituents.json"

# Max concurrent iwencai requests
_CONCURRENCY = 8


def _load_concept_board_names() -> list[str]:
    """Load concept board names from sectors.json."""
    with open(SECTORS_PATH, encoding="utf-8") as f:
        sectors = json.load(f)

    names = [item["name"] for item in sectors.get("concept", [])]
    logger.info(f"Loaded {len(names)} concept boards from sectors.json")
    return names


def _parse_stock_list(result: dict) -> list[list[str]]:
    """Parse iwencai response into [[code, name], ...] list."""
    tables = result.get("tables", [])
    if not tables:
        return []

    stocks: list[list[str]] = []
    for table_wrapper in tables:
        if not isinstance(table_wrapper, dict):
            continue
        table = table_wrapper.get("table", table_wrapper)
        if not isinstance(table, dict):
            continue

        codes = None
        names = None
        for col_name, col_data in table.items():
            if "代码" in col_name or "thscode" in col_name.lower():
                codes = col_data
            if "简称" in col_name or "名称" in col_name:
                names = col_data

        if codes:
            for i in range(len(codes)):
                raw = str(codes[i]).strip()
                bare = raw
                for suffix in (".SZ", ".SH", ".BJ", ".sz", ".sh", ".bj"):
                    if bare.endswith(suffix):
                        bare = bare[: -len(suffix)]
                        break
                if len(bare) == 6 and bare.isdigit():
                    name = str(names[i]).strip() if names and i < len(names) else ""
                    stocks.append([bare, name])

    return stocks


async def download_all(dry_run: bool = False) -> None:
    board_names = _load_concept_board_names()
    if not board_names:
        logger.error("No concept boards found in sectors.json")
        return

    if dry_run:
        for i, name in enumerate(board_names, 1):
            print(f"  {i:3d}. {name}")
        print(f"\nTotal: {len(board_names)} boards (dry run, no download)")
        return

    client = IFinDHttpClient()
    await client.start()

    result: dict[str, list[list[str]]] = {}
    failed: list[str] = []
    sem = asyncio.Semaphore(_CONCURRENCY)
    done = 0

    async def _fetch(board_name: str) -> None:
        nonlocal done
        async with sem:
            # Query pattern matters:
            # - Boards ending with "概念": use "{name}股" (safest)
            #   "{name}板块成分股" causes iwencai to return full market for some
            #   boards (e.g. 净水概念板块成分股 → 5488 = all stocks)
            # - Other boards: use "{name}概念板块成分股"
            if board_name.endswith("概念"):
                query = f"{board_name}股"
            else:
                query = f"{board_name}概念板块成分股"
            try:
                resp = await client.smart_stock_picking(query, "stock")
                stocks = _parse_stock_list(resp)
                # Sanity check: >3000 stocks means iwencai returned full market
                if len(stocks) > 3000:
                    logger.warning(
                        f"SUSPECT: {board_name} → {len(stocks)} stocks "
                        f'(query="{query}"). Likely full-market dump — skipping.'
                    )
                    failed.append(board_name)
                    done += 1
                    return
                result[board_name] = stocks
                done += 1
                if done % 20 == 0 or done == len(board_names):
                    logger.info(f"Progress: {done}/{len(board_names)}")
            except IFinDHttpError as e:
                logger.warning(f"Failed: {board_name}: {e}")
                failed.append(board_name)
                done += 1
            except Exception as e:
                logger.error(f"Unexpected error for {board_name}: {e}")
                failed.append(board_name)
                done += 1

    tasks = [_fetch(name) for name in board_names]
    await asyncio.gather(*tasks)

    await client.stop()

    # Save
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    total_stocks = sum(len(v) for v in result.values())
    logger.info(
        f"Done: {len(result)} boards, {total_stocks} total stock entries, "
        f"{len(failed)} failed. Saved to {OUTPUT_PATH}"
    )
    if failed:
        logger.warning(f"Failed boards: {failed}")


def main() -> None:
    parser = argparse.ArgumentParser(description="下载同花顺概念板块成分股")
    parser.add_argument("--dry-run", action="store_true", help="只打印板块列表，不下载")
    args = parser.parse_args()
    asyncio.run(download_all(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
