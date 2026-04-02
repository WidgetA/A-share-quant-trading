"""Clean dirty minute data: DELETE rows with close_940 <= 0.

These are suspended-day bars where tsanghi returned close=0.
Must run inside docker container or connect to GreptimeDB pgwire port.

Usage (on server):
  docker-compose exec trading-service python scripts/clean_minute_dirty_data.py
"""

from __future__ import annotations

import asyncio
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 100  # stay well under ~200 row DELETE limit


async def main() -> None:
    import asyncpg
    from src.data.clients.greptime_db import _no_reset_connection_class

    pool = await asyncpg.create_pool(
        host="greptimedb",
        port=4003,
        database="public",
        user="greptime",
        min_size=1,
        max_size=1,
        connection_class=_no_reset_connection_class(),
    )

    async with pool.acquire() as conn:
        # Count dirty rows
        row = await conn.fetchrow(
            "SELECT count(*) as cnt FROM backtest_minute WHERE close_940 <= 0"
        )
        total = row["cnt"]
        logger.info(f"Found {total} dirty rows with close_940 <= 0")

        if total == 0:
            logger.info("Nothing to clean")
            return

        # Fetch all dirty (stock_code, ts) pairs
        dirty_rows = await conn.fetch(
            "SELECT stock_code, ts FROM backtest_minute WHERE close_940 <= 0"
        )
        logger.info(f"Fetched {len(dirty_rows)} dirty row keys")

        # Delete in small batches
        deleted = 0
        for i in range(0, len(dirty_rows), BATCH_SIZE):
            batch = dirty_rows[i : i + BATCH_SIZE]
            for r in batch:
                await conn.execute(
                    f"DELETE FROM backtest_minute "
                    f"WHERE stock_code = '{r['stock_code']}' "
                    f"AND ts = '{r['ts']}'"
                )
            deleted += len(batch)
            logger.info(f"Deleted {deleted}/{len(dirty_rows)}")

        # Flush tombstones to disk
        await conn.execute("ADMIN FLUSH_TABLE('backtest_minute')")
        logger.info("Flushed tombstones to disk")

        # Verify
        row = await conn.fetchrow(
            "SELECT count(*) as cnt FROM backtest_minute WHERE close_940 <= 0"
        )
        remaining = row["cnt"]
        logger.info(f"Verification: {remaining} dirty rows remaining")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
