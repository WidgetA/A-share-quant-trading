# === MODULE PURPOSE ===
# SQLite database layer for limit-up stock data persistence.
# Handles all database operations for storing and querying limit-up stocks.

# === DEPENDENCIES ===
# - aiosqlite: Async SQLite operations
# - LimitUpStock model: Data structure for limit-up stocks

# === KEY CONCEPTS ===
# - Async operations: All DB operations are async for non-blocking I/O
# - Auto-migration: Table is created automatically on first use
# - Idempotent: Re-saving the same date updates existing records (INSERT OR REPLACE)

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator

import aiosqlite

from src.data.models.limit_up import LimitUpStock

logger = logging.getLogger(__name__)


class LimitUpDatabase:
    """
    Async SQLite database for limit-up stock storage.

    Data Flow:
        LimitUpStock -> save() -> SQLite
        SQLite -> query() -> LimitUpStock

    Usage:
        async with LimitUpDatabase("data/limit_up.db") as db:
            await db.save(stock)
            stocks = await db.query_by_date("2026-01-27")
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "LimitUpDatabase":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def connect(self) -> None:
        """Establish database connection and ensure schema exists."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = await aiosqlite.connect(self.db_path)
        self._connection.row_factory = aiosqlite.Row
        await self._create_tables()
        logger.info(f"Connected to limit-up database: {self.db_path}")

    async def close(self) -> None:
        """Close database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None
            logger.info("Limit-up database connection closed")

    async def _create_tables(self) -> None:
        """Create limit_up_stocks table if not exists."""
        if not self._connection:
            raise RuntimeError("Database not connected")

        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS limit_up_stocks (
                id TEXT PRIMARY KEY,
                trade_date TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                limit_up_price REAL NOT NULL,
                limit_up_time TEXT NOT NULL,
                open_count INTEGER DEFAULT 0,
                last_limit_up_time TEXT,
                turnover_rate REAL,
                amount REAL,
                circulation_mv REAL,
                reason TEXT,
                industry TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create indexes for common queries
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_limit_up_trade_date
            ON limit_up_stocks(trade_date DESC)
        """)
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_limit_up_stock_code
            ON limit_up_stocks(stock_code)
        """)
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_limit_up_time
            ON limit_up_stocks(limit_up_time)
        """)

        await self._connection.commit()

    async def save(self, stock: LimitUpStock) -> None:
        """Save a single limit-up stock to database."""
        if not self._connection:
            raise RuntimeError("Database not connected")

        async with self._lock:
            data = stock.to_dict()
            await self._connection.execute(
                """
                INSERT OR REPLACE INTO limit_up_stocks
                (id, trade_date, stock_code, stock_name, limit_up_price, limit_up_time,
                 open_count, last_limit_up_time, turnover_rate, amount, circulation_mv,
                 reason, industry, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data["id"],
                    data["trade_date"],
                    data["stock_code"],
                    data["stock_name"],
                    data["limit_up_price"],
                    data["limit_up_time"],
                    data["open_count"],
                    data["last_limit_up_time"],
                    data["turnover_rate"],
                    data["amount"],
                    data["circulation_mv"],
                    data["reason"],
                    data["industry"],
                    data["created_at"],
                    data["updated_at"],
                ),
            )
            await self._connection.commit()
            logger.debug(f"Saved limit-up stock: {stock.stock_code} - {stock.stock_name}")

    async def save_batch(self, stocks: list[LimitUpStock]) -> int:
        """Save multiple limit-up stocks in a single transaction."""
        if not self._connection:
            raise RuntimeError("Database not connected")

        if not stocks:
            return 0

        async with self._lock:
            data_list = [s.to_dict() for s in stocks]
            await self._connection.executemany(
                """
                INSERT OR REPLACE INTO limit_up_stocks
                (id, trade_date, stock_code, stock_name, limit_up_price, limit_up_time,
                 open_count, last_limit_up_time, turnover_rate, amount, circulation_mv,
                 reason, industry, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        d["id"],
                        d["trade_date"],
                        d["stock_code"],
                        d["stock_name"],
                        d["limit_up_price"],
                        d["limit_up_time"],
                        d["open_count"],
                        d["last_limit_up_time"],
                        d["turnover_rate"],
                        d["amount"],
                        d["circulation_mv"],
                        d["reason"],
                        d["industry"],
                        d["created_at"],
                        d["updated_at"],
                    )
                    for d in data_list
                ],
            )
            await self._connection.commit()
            logger.info(f"Saved {len(stocks)} limit-up stocks in batch")
            return len(stocks)

    async def query_by_date(
        self,
        trade_date: str,
        order_by: str = "limit_up_time",
    ) -> list[LimitUpStock]:
        """
        Query all limit-up stocks for a specific date.

        Args:
            trade_date: Trading date in YYYY-MM-DD format
            order_by: Column to order by (default: limit_up_time)

        Returns:
            List of LimitUpStock objects for that date
        """
        if not self._connection:
            raise RuntimeError("Database not connected")

        query = f"""
            SELECT * FROM limit_up_stocks
            WHERE trade_date = ?
            ORDER BY {order_by}
        """

        async with self._connection.execute(query, (trade_date,)) as cursor:
            rows = await cursor.fetchall()
            return [LimitUpStock.from_dict(dict(row)) for row in rows]

    async def query(
        self,
        stock_code: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        reason_contains: str | None = None,
        industry: str | None = None,
        min_open_count: int | None = None,
        max_open_count: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[LimitUpStock]:
        """
        Query limit-up stocks with various filters.

        Args:
            stock_code: Filter by specific stock code
            start_date: Filter by start date (inclusive)
            end_date: Filter by end date (inclusive)
            reason_contains: Filter by reason containing this text
            industry: Filter by industry
            min_open_count: Minimum open count
            max_open_count: Maximum open count (0 = sealed all day)
            limit: Maximum number of results
            offset: Skip first N results

        Returns:
            List of LimitUpStock objects matching the filters
        """
        if not self._connection:
            raise RuntimeError("Database not connected")

        conditions: list[str] = []
        params: list[str | int] = []

        if stock_code:
            conditions.append("stock_code = ?")
            params.append(stock_code)
        if start_date:
            conditions.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= ?")
            params.append(end_date)
        if reason_contains:
            conditions.append("reason LIKE ?")
            params.append(f"%{reason_contains}%")
        if industry:
            conditions.append("industry = ?")
            params.append(industry)
        if min_open_count is not None:
            conditions.append("open_count >= ?")
            params.append(min_open_count)
        if max_open_count is not None:
            conditions.append("open_count <= ?")
            params.append(max_open_count)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params.extend([limit, offset])

        query = f"""
            SELECT * FROM limit_up_stocks
            WHERE {where_clause}
            ORDER BY trade_date DESC, limit_up_time
            LIMIT ? OFFSET ?
        """

        async with self._connection.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [LimitUpStock.from_dict(dict(row)) for row in rows]

    async def stream_by_date_range(
        self,
        start_date: str,
        end_date: str,
        batch_size: int = 100,
    ) -> AsyncIterator[LimitUpStock]:
        """
        Stream limit-up stocks for a date range.

        Yields stocks one by one, fetching in batches for efficiency.
        """
        offset = 0
        while True:
            stocks = await self.query(
                start_date=start_date,
                end_date=end_date,
                limit=batch_size,
                offset=offset,
            )
            if not stocks:
                break
            for stock in stocks:
                yield stock
            offset += batch_size

    async def count_by_date(self, trade_date: str) -> int:
        """Count limit-up stocks for a specific date."""
        if not self._connection:
            raise RuntimeError("Database not connected")

        query = "SELECT COUNT(*) FROM limit_up_stocks WHERE trade_date = ?"
        async with self._connection.execute(query, (trade_date,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_dates_with_data(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[str]:
        """Get list of dates that have limit-up data."""
        if not self._connection:
            raise RuntimeError("Database not connected")

        conditions: list[str] = []
        params: list[str] = []

        if start_date:
            conditions.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= ?")
            params.append(end_date)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT DISTINCT trade_date FROM limit_up_stocks
            WHERE {where_clause}
            ORDER BY trade_date DESC
        """

        async with self._connection.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    async def delete_by_date(self, trade_date: str) -> int:
        """Delete all limit-up stocks for a specific date."""
        if not self._connection:
            raise RuntimeError("Database not connected")

        async with self._lock:
            cursor = await self._connection.execute(
                "DELETE FROM limit_up_stocks WHERE trade_date = ?",
                (trade_date,),
            )
            await self._connection.commit()
            deleted = cursor.rowcount
            logger.info(f"Deleted {deleted} limit-up stocks for {trade_date}")
            return deleted

    async def exists(self, stock_id: str) -> bool:
        """Check if a limit-up stock record with given ID exists."""
        if not self._connection:
            raise RuntimeError("Database not connected")

        async with self._connection.execute(
            "SELECT 1 FROM limit_up_stocks WHERE id = ?", (stock_id,)
        ) as cursor:
            return await cursor.fetchone() is not None
