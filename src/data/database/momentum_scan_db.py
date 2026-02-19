# === MODULE PURPOSE ===
# PostgreSQL database layer for momentum scan selected stocks.
# Stores the candidate pool (after all filters, before final recommendation)
# for historical analysis and CSV export.

# === DEPENDENCIES ===
# - asyncpg: Async PostgreSQL operations
# - SelectedStock: Data structure from momentum_sector_scanner

# === KEY CONCEPTS ===
# - One stock can appear in multiple boards on the same day
# - Primary key: (trade_date, stock_code, board_name)
# - save_day replaces all rows for a date (clean upsert)
# - Stores price data (open, prev_close, 9:40 price) and T+1 return

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)


@dataclass
class MomentumScanDBConfig:
    """Configuration for momentum scan database."""

    host: str = "localhost"
    port: int = 5432
    database: str = "messages"
    user: str = "reader"
    password: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 5
    schema: str = "trading"


SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS {schema};
"""

TABLES_SQL = """
CREATE TABLE IF NOT EXISTS {schema}.momentum_scan_stocks (
    trade_date      DATE NOT NULL,
    stock_code      VARCHAR(20) NOT NULL,
    stock_name      VARCHAR(50) NOT NULL,
    board_name      VARCHAR(100) NOT NULL,
    open_gain_pct   DECIMAL(8, 4),
    pe_ttm          DECIMAL(12, 4),
    board_avg_pe    DECIMAL(12, 4),
    open_price      DECIMAL(12, 4),
    prev_close      DECIMAL(12, 4),
    buy_price       DECIMAL(12, 4),
    next_day_open   DECIMAL(12, 4),
    return_pct      DECIMAL(8, 4),
    growth_rate     DECIMAL(10, 4),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, stock_code, board_name)
);

CREATE INDEX IF NOT EXISTS idx_scan_stocks_date
ON {schema}.momentum_scan_stocks(trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_scan_stocks_code
ON {schema}.momentum_scan_stocks(stock_code);
"""

# Migration: add columns if table was created with old schema
MIGRATION_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = 'momentum_scan_stocks'
          AND column_name = 'open_price'
    ) THEN
        ALTER TABLE {schema}.momentum_scan_stocks
            ADD COLUMN open_price    DECIMAL(12, 4),
            ADD COLUMN prev_close    DECIMAL(12, 4),
            ADD COLUMN buy_price     DECIMAL(12, 4),
            ADD COLUMN next_day_open DECIMAL(12, 4),
            ADD COLUMN return_pct    DECIMAL(8, 4),
            ADD COLUMN growth_rate   DECIMAL(10, 4);
    END IF;
END $$;
"""


class MomentumScanDB:
    """
    Async PostgreSQL database for momentum scan selected stocks.

    Data Flow:
        scanner.scan() → selected_stocks + snapshots → save_day() → PostgreSQL
        PostgreSQL → query() → list[dict] → CSV export

    Usage:
        db = MomentumScanDB(config)
        await db.connect()
        await db.save_day(trade_date, rows)
        rows = await db.query("2023-01-01", "2026-02-19")
        await db.close()
    """

    def __init__(self, config: MomentumScanDBConfig):
        self._config = config
        self._pool: asyncpg.Pool | None = None
        self._is_connected = False
        self._schema = config.schema
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "MomentumScanDB":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def connect(self) -> None:
        """Establish database connection and ensure schema/table exist."""
        if self._is_connected:
            return

        try:
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
            logger.info("MomentumScanDB connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Failed to connect MomentumScanDB: {e}")
            raise ConnectionError(f"Cannot connect to momentum scan database: {e}") from e

    async def close(self) -> None:
        """Close database connection."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            self._is_connected = False
            logger.info("MomentumScanDB connection closed")

    async def _create_tables(self) -> None:
        """Create table if not exists, run migrations for schema updates."""
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA_SQL.format(schema=self._schema))
            await conn.execute(TABLES_SQL.format(schema=self._schema))
            await conn.execute(MIGRATION_SQL.format(schema=self._schema))

    @property
    def _db_pool(self) -> asyncpg.Pool:
        if not self._is_connected or not self._pool:
            raise RuntimeError("MomentumScanDB is not connected. Call connect() first.")
        return self._pool

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    async def save_day(self, trade_date: date, rows: list[dict]) -> int:
        """Save one day's selected stocks. Replaces existing data for that date.

        Args:
            trade_date: The trading date.
            rows: List of dicts with keys: stock_code, stock_name, board_name,
                  open_gain_pct, pe_ttm, board_avg_pe, open_price, prev_close,
                  buy_price, next_day_open, return_pct.

        Returns:
            Number of rows saved.
        """
        if not rows:
            return 0

        async with self._lock:
            async with self._db_pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        f"DELETE FROM {self._schema}.momentum_scan_stocks WHERE trade_date = $1",
                        trade_date,
                    )
                    for r in rows:
                        await conn.execute(
                            f"""
                            INSERT INTO {self._schema}.momentum_scan_stocks
                            (trade_date, stock_code, stock_name, board_name,
                             open_gain_pct, pe_ttm, board_avg_pe,
                             open_price, prev_close, buy_price,
                             next_day_open, return_pct, growth_rate)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                            """,
                            trade_date,
                            r["stock_code"],
                            r["stock_name"],
                            r["board_name"],
                            r.get("open_gain_pct"),
                            r.get("pe_ttm"),
                            r.get("board_avg_pe"),
                            r.get("open_price"),
                            r.get("prev_close"),
                            r.get("buy_price"),
                            r.get("next_day_open"),
                            r.get("return_pct"),
                            r.get("growth_rate"),
                        )

        logger.debug(f"Saved {len(rows)} scan stocks for {trade_date}")
        return len(rows)

    async def query(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        stock_code: str | None = None,
        board_name: str | None = None,
        limit: int = 100000,
    ) -> list[dict]:
        """Query selected stocks with filters.

        Returns list of dicts suitable for CSV export.
        """
        conditions: list[str] = []
        params: list[Any] = []
        idx = 1

        if start_date:
            conditions.append(f"trade_date >= ${idx}")
            params.append(date.fromisoformat(start_date))
            idx += 1
        if end_date:
            conditions.append(f"trade_date <= ${idx}")
            params.append(date.fromisoformat(end_date))
            idx += 1
        if stock_code:
            conditions.append(f"stock_code = ${idx}")
            params.append(stock_code)
            idx += 1
        if board_name:
            conditions.append(f"board_name = ${idx}")
            params.append(board_name)
            idx += 1

        where = " AND ".join(conditions) if conditions else "1=1"
        params.append(limit)

        sql = f"""
            SELECT trade_date, stock_code, stock_name, board_name,
                   open_gain_pct, pe_ttm, board_avg_pe,
                   open_price, prev_close, buy_price,
                   next_day_open, return_pct, growth_rate
            FROM {self._schema}.momentum_scan_stocks
            WHERE {where}
            ORDER BY trade_date DESC, stock_code
            LIMIT ${idx}
        """

        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        def _f(val):
            return round(float(val), 4) if val is not None else None

        return [
            {
                "trade_date": row["trade_date"].isoformat(),
                "stock_code": row["stock_code"],
                "stock_name": row["stock_name"],
                "board_name": row["board_name"],
                "open_gain_pct": _f(row["open_gain_pct"]),
                "pe_ttm": _f(row["pe_ttm"]),
                "board_avg_pe": _f(row["board_avg_pe"]),
                "open_price": _f(row["open_price"]),
                "prev_close": _f(row["prev_close"]),
                "buy_price": _f(row["buy_price"]),
                "next_day_open": _f(row["next_day_open"]),
                "return_pct": _f(row["return_pct"]),
                "growth_rate": _f(row["growth_rate"]),
            }
            for row in rows
        ]

    async def get_dates_with_data(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[date]:
        """Get list of dates that already have scan data (for resume support)."""
        conditions: list[str] = []
        params: list[Any] = []
        idx = 1

        if start_date:
            conditions.append(f"trade_date >= ${idx}")
            params.append(date.fromisoformat(start_date))
            idx += 1
        if end_date:
            conditions.append(f"trade_date <= ${idx}")
            params.append(date.fromisoformat(end_date))
            idx += 1

        where = " AND ".join(conditions) if conditions else "1=1"

        sql = f"""
            SELECT DISTINCT trade_date
            FROM {self._schema}.momentum_scan_stocks
            WHERE {where}
            ORDER BY trade_date
        """

        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        return [row["trade_date"] for row in rows]

    async def count_by_date(self, trade_date: str) -> int:
        """Count selected stocks for a specific date."""
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt FROM {self._schema}.momentum_scan_stocks WHERE trade_date = $1",
                date.fromisoformat(trade_date),
            )
            return row["cnt"] if row else 0


def create_momentum_scan_db_from_config() -> MomentumScanDB:
    """Create MomentumScanDB from database-config.yaml."""
    from src.common.config import load_config

    config = load_config("config/database-config.yaml")
    db_config = config.get_dict("database.trading", {})

    if not db_config:
        raise ValueError("Trading database configuration not found")

    def resolve_env(value: Any) -> Any:
        if isinstance(value, str) and value.startswith("${"):
            inner = value[2:-1]
            if ":" in inner:
                var_name, default = inner.split(":", 1)
            else:
                var_name, default = inner, ""
            return os.environ.get(var_name, default)
        return value

    return MomentumScanDB(
        MomentumScanDBConfig(
            host=resolve_env(db_config.get("host", "localhost")),
            port=int(resolve_env(db_config.get("port", 5432))),
            database=resolve_env(db_config.get("database", "messages")),
            user=resolve_env(db_config.get("user", "reader")),
            password=resolve_env(db_config.get("password", "")),
            pool_min_size=db_config.get("pool_min_size", 2),
            pool_max_size=db_config.get("pool_max_size", 5),
            schema=db_config.get("schema", "trading"),
        )
    )
