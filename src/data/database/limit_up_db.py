# === MODULE PURPOSE ===
# PostgreSQL database layer for limit-up stock data persistence.
# Handles all database operations for storing and querying limit-up stocks.

# === DEPENDENCIES ===
# - asyncpg: Async PostgreSQL operations
# - LimitUpStock model: Data structure for limit-up stocks

# === KEY CONCEPTS ===
# - Async operations: All DB operations are async for non-blocking I/O
# - Auto-migration: Table is created automatically on first use
# - Idempotent: Re-saving the same date updates existing records (ON CONFLICT)

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Any, AsyncIterator

import asyncpg

from src.data.models.limit_up import LimitUpStock

logger = logging.getLogger(__name__)


@dataclass
class LimitUpDatabaseConfig:
    """Configuration for PostgreSQL limit-up database."""

    host: str = "localhost"
    port: int = 5432
    database: str = "messages"
    user: str = "reader"
    password: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 5
    schema: str = "trading"


# SQL for schema and table creation
SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS {schema};
"""

TABLES_SQL = """
-- Limit-up stocks table
CREATE TABLE IF NOT EXISTS {schema}.limit_up_stocks (
    id VARCHAR(50) PRIMARY KEY,
    trade_date DATE NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(50) NOT NULL,
    limit_up_price DECIMAL(12, 4) NOT NULL,
    limit_up_time VARCHAR(20) NOT NULL,
    open_count INTEGER DEFAULT 0,
    last_limit_up_time VARCHAR(20),
    turnover_rate DECIMAL(10, 4),
    amount DECIMAL(20, 2),
    circulation_mv DECIMAL(20, 2),
    reason TEXT,
    industry VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_limit_up_trade_date
ON {schema}.limit_up_stocks(trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_limit_up_stock_code
ON {schema}.limit_up_stocks(stock_code);

CREATE INDEX IF NOT EXISTS idx_limit_up_time
ON {schema}.limit_up_stocks(limit_up_time);
"""


class LimitUpDatabase:
    """
    Async PostgreSQL database for limit-up stock storage.

    Data Flow:
        LimitUpStock -> save() -> PostgreSQL
        PostgreSQL -> query() -> LimitUpStock

    Usage:
        config = LimitUpDatabaseConfig(host="localhost", ...)
        db = LimitUpDatabase(config)
        await db.connect()
        await db.save(stock)
        stocks = await db.query_by_date("2026-01-27")
        await db.close()
    """

    def __init__(self, config: LimitUpDatabaseConfig):
        self._config = config
        self._pool: asyncpg.Pool | None = None
        self._is_connected = False
        self._schema = config.schema
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "LimitUpDatabase":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def connect(self) -> None:
        """Establish database connection and ensure schema exists."""
        if self._is_connected:
            return

        try:
            logger.info(
                f"Connecting LimitUpDatabase to PostgreSQL: {self._config.host}:{self._config.port}"
                f"/{self._config.database} (schema: {self._schema})"
            )

            self._pool = await asyncpg.create_pool(
                host=self._config.host,
                port=self._config.port,
                database=self._config.database,
                user=self._config.user,
                password=self._config.password,
                min_size=self._config.pool_min_size,
                max_size=self._config.pool_max_size,
            )

            await self._create_tables()
            self._is_connected = True
            logger.info("LimitUpDatabase connected to PostgreSQL")

        except Exception as e:
            logger.error(f"Failed to connect LimitUpDatabase to PostgreSQL: {e}")
            raise ConnectionError(f"Cannot connect to limit-up database: {e}") from e

    async def close(self) -> None:
        """Close database connection."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            self._is_connected = False
            logger.info("LimitUpDatabase connection closed")

    async def _create_tables(self) -> None:
        """Create limit_up_stocks table if not exists."""
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA_SQL.format(schema=self._schema))
            await conn.execute(TABLES_SQL.format(schema=self._schema))
            logger.info(f"Initialized limit-up stocks schema: {self._schema}")

    def _ensure_connected(self) -> None:
        """Ensure database is connected."""
        if not self._is_connected or not self._pool:
            raise RuntimeError("LimitUpDatabase is not connected. Call connect() first.")

    @property
    def _db_pool(self) -> asyncpg.Pool:
        """Get the database pool, raising if not connected."""
        self._ensure_connected()
        assert self._pool is not None
        return self._pool

    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._is_connected

    async def save(self, stock: LimitUpStock) -> None:
        """Save a single limit-up stock to database."""
        async with self._lock:
            data = stock.to_dict()
            trade_date = data["trade_date"]
            if isinstance(trade_date, str):
                trade_date = date.fromisoformat(trade_date)

            async with self._db_pool.acquire() as conn:
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.limit_up_stocks
                    (id, trade_date, stock_code, stock_name, limit_up_price, limit_up_time,
                     open_count, last_limit_up_time, turnover_rate, amount, circulation_mv,
                     reason, industry, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (id) DO UPDATE SET
                        trade_date = EXCLUDED.trade_date,
                        stock_code = EXCLUDED.stock_code,
                        stock_name = EXCLUDED.stock_name,
                        limit_up_price = EXCLUDED.limit_up_price,
                        limit_up_time = EXCLUDED.limit_up_time,
                        open_count = EXCLUDED.open_count,
                        last_limit_up_time = EXCLUDED.last_limit_up_time,
                        turnover_rate = EXCLUDED.turnover_rate,
                        amount = EXCLUDED.amount,
                        circulation_mv = EXCLUDED.circulation_mv,
                        reason = EXCLUDED.reason,
                        industry = EXCLUDED.industry,
                        updated_at = EXCLUDED.updated_at
                    """,
                    data["id"],
                    trade_date,
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
                )
            logger.debug(f"Saved limit-up stock: {stock.stock_code} - {stock.stock_name}")

    async def save_batch(self, stocks: list[LimitUpStock]) -> int:
        """Save multiple limit-up stocks in a single transaction."""
        if not stocks:
            return 0

        async with self._lock:
            async with self._db_pool.acquire() as conn:
                async with conn.transaction():
                    for stock in stocks:
                        data = stock.to_dict()
                        trade_date = data["trade_date"]
                        if isinstance(trade_date, str):
                            trade_date = date.fromisoformat(trade_date)

                        await conn.execute(
                            f"""
                            INSERT INTO {self._schema}.limit_up_stocks
                            (id, trade_date, stock_code, stock_name, limit_up_price, limit_up_time,
                             open_count, last_limit_up_time, turnover_rate, amount, circulation_mv,
                             reason, industry, created_at, updated_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                                    $15)
                            ON CONFLICT (id) DO UPDATE SET
                                trade_date = EXCLUDED.trade_date,
                                stock_code = EXCLUDED.stock_code,
                                stock_name = EXCLUDED.stock_name,
                                limit_up_price = EXCLUDED.limit_up_price,
                                limit_up_time = EXCLUDED.limit_up_time,
                                open_count = EXCLUDED.open_count,
                                last_limit_up_time = EXCLUDED.last_limit_up_time,
                                turnover_rate = EXCLUDED.turnover_rate,
                                amount = EXCLUDED.amount,
                                circulation_mv = EXCLUDED.circulation_mv,
                                reason = EXCLUDED.reason,
                                industry = EXCLUDED.industry,
                                updated_at = EXCLUDED.updated_at
                            """,
                            data["id"],
                            trade_date,
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
                        )

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
        # Validate order_by to prevent SQL injection
        valid_columns = {
            "limit_up_time", "stock_code", "stock_name", "amount",
            "turnover_rate", "open_count", "created_at"
        }
        if order_by not in valid_columns:
            order_by = "limit_up_time"

        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM {self._schema}.limit_up_stocks
                WHERE trade_date = $1
                ORDER BY {order_by}
                """,
                date.fromisoformat(trade_date),
            )

        return [self._row_to_model(row) for row in rows]

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
        conditions: list[str] = []
        params: list[Any] = []
        param_idx = 1

        if stock_code:
            conditions.append(f"stock_code = ${param_idx}")
            params.append(stock_code)
            param_idx += 1
        if start_date:
            conditions.append(f"trade_date >= ${param_idx}")
            params.append(date.fromisoformat(start_date))
            param_idx += 1
        if end_date:
            conditions.append(f"trade_date <= ${param_idx}")
            params.append(date.fromisoformat(end_date))
            param_idx += 1
        if reason_contains:
            conditions.append(f"reason LIKE ${param_idx}")
            params.append(f"%{reason_contains}%")
            param_idx += 1
        if industry:
            conditions.append(f"industry = ${param_idx}")
            params.append(industry)
            param_idx += 1
        if min_open_count is not None:
            conditions.append(f"open_count >= ${param_idx}")
            params.append(min_open_count)
            param_idx += 1
        if max_open_count is not None:
            conditions.append(f"open_count <= ${param_idx}")
            params.append(max_open_count)
            param_idx += 1

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params.extend([limit, offset])

        query = f"""
            SELECT * FROM {self._schema}.limit_up_stocks
            WHERE {where_clause}
            ORDER BY trade_date DESC, limit_up_time
            LIMIT ${param_idx} OFFSET ${param_idx + 1}
        """

        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [self._row_to_model(row) for row in rows]

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
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt FROM {self._schema}.limit_up_stocks WHERE trade_date = $1",
                date.fromisoformat(trade_date),
            )
            return row["cnt"] if row else 0

    async def get_dates_with_data(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[str]:
        """Get list of dates that have limit-up data."""
        conditions: list[str] = []
        params: list[Any] = []
        param_idx = 1

        if start_date:
            conditions.append(f"trade_date >= ${param_idx}")
            params.append(date.fromisoformat(start_date))
            param_idx += 1
        if end_date:
            conditions.append(f"trade_date <= ${param_idx}")
            params.append(date.fromisoformat(end_date))
            param_idx += 1

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT DISTINCT trade_date FROM {self._schema}.limit_up_stocks
            WHERE {where_clause}
            ORDER BY trade_date DESC
        """

        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [row["trade_date"].isoformat() for row in rows]

    async def delete_by_date(self, trade_date: str) -> int:
        """Delete all limit-up stocks for a specific date."""
        async with self._lock:
            async with self._db_pool.acquire() as conn:
                result = await conn.execute(
                    f"DELETE FROM {self._schema}.limit_up_stocks WHERE trade_date = $1",
                    date.fromisoformat(trade_date),
                )
                # Parse "DELETE N" result
                deleted = int(result.split()[-1]) if result else 0
                logger.info(f"Deleted {deleted} limit-up stocks for {trade_date}")
                return deleted

    async def exists(self, stock_id: str) -> bool:
        """Check if a limit-up stock record with given ID exists."""
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT 1 FROM {self._schema}.limit_up_stocks WHERE id = $1",
                stock_id,
            )
            return row is not None

    def _row_to_model(self, row: asyncpg.Record) -> LimitUpStock:
        """Convert database row to LimitUpStock model."""
        trade_date_val = row["trade_date"]
        trade_date_str = (
            trade_date_val.isoformat() if isinstance(trade_date_val, date) else trade_date_val
        )
        return LimitUpStock.from_dict({
            "id": row["id"],
            "trade_date": trade_date_str,
            "stock_code": row["stock_code"],
            "stock_name": row["stock_name"],
            "limit_up_price": float(row["limit_up_price"]) if row["limit_up_price"] else 0,
            "limit_up_time": row["limit_up_time"],
            "open_count": row["open_count"],
            "last_limit_up_time": row["last_limit_up_time"],
            "turnover_rate": float(row["turnover_rate"]) if row["turnover_rate"] else None,
            "amount": float(row["amount"]) if row["amount"] else None,
            "circulation_mv": float(row["circulation_mv"]) if row["circulation_mv"] else None,
            "reason": row["reason"],
            "industry": row["industry"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        })


def create_limit_up_db_from_config() -> LimitUpDatabase:
    """
    Create LimitUpDatabase from configuration file.

    Returns:
        Configured LimitUpDatabase instance.
    """
    from src.common.config import load_config

    config = load_config("config/database-config.yaml")
    db_config = config.get_dict("database.trading", {})

    if not db_config:
        raise ValueError("Trading database configuration not found")

    # Support environment variable substitution
    def resolve_env(value: Any) -> Any:
        if isinstance(value, str) and value.startswith("${"):
            # Parse ${VAR:default} or ${VAR}
            inner = value[2:-1]
            if ":" in inner:
                var_name, default = inner.split(":", 1)
            else:
                var_name, default = inner, ""
            return os.environ.get(var_name, default)
        return value

    limit_up_config = LimitUpDatabaseConfig(
        host=resolve_env(db_config.get("host", "localhost")),
        port=int(resolve_env(db_config.get("port", 5432))),
        database=resolve_env(db_config.get("database", "messages")),
        user=resolve_env(db_config.get("user", "reader")),
        password=resolve_env(db_config.get("password", "")),
        pool_min_size=db_config.get("pool_min_size", 2),
        pool_max_size=db_config.get("pool_max_size", 5),
        schema=db_config.get("schema", "trading"),
    )

    return LimitUpDatabase(limit_up_config)
