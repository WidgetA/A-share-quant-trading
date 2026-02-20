# === MODULE PURPOSE ===
# Read-only database layer for stock_fundamentals table.
# Provides PE(TTM) and other fundamental data for strategy filtering.

# === DEPENDENCIES ===
# - asyncpg: Async PostgreSQL operations
# - stock_fundamentals table: Populated by external process

# === KEY CONCEPTS ===
# - Read-only: This project only reads from stock_fundamentals, never writes
# - ST detection: company_name containing "ST" indicates ST stock
# - PE filtering: Used by momentum sector strategy for board-level PE comparison

import logging
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)


@dataclass
class FundamentalsDBConfig:
    """Configuration for stock_fundamentals database connection."""

    host: str = "localhost"
    port: int = 5432
    database: str = "messages"
    user: str = "reader"
    password: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 5
    schema: str = "public"


@dataclass
class StockFundamentals:
    """Stock fundamental data from stock_fundamentals table."""

    stock_code: str
    company_name: str
    pe_ttm: float | None
    ps_ttm: float | None
    pb: float | None
    total_market_cap: float | None
    roe: float | None
    annual_revenue_yoy: float | None  # 年度营收同比增长率 (%)
    quarterly_revenue_yoy: float | None  # 季度营收同比增长率 (%)
    annual_net_profit_yoy: float | None  # 年度净利润同比增长率 (%)
    quarterly_net_profit_yoy: float | None  # 季度净利润同比增长率 (%)
    report_date_annual: str | None  # 年报报告期
    report_date_quarterly: str | None  # 季报报告期

    @property
    def is_st(self) -> bool:
        """Check if this is an ST stock based on company name."""
        return "ST" in self.company_name.upper()


class FundamentalsDB:
    """
    Read-only async database layer for stock_fundamentals table.

    Data Flow:
        External process -> stock_fundamentals table -> this reader -> strategy

    Follows the same asyncpg pool pattern as LimitUpDatabase.

    Usage:
        db = FundamentalsDB(config)
        await db.connect()
        pe = await db.get_pe("000001")
        pe_map = await db.batch_get_pe(["000001", "600519"])
        non_st = await db.batch_filter_st(["000001", "600519", "000002"])
        await db.close()
    """

    def __init__(self, config: FundamentalsDBConfig):
        self._config = config
        self._pool: asyncpg.Pool | None = None
        self._is_connected = False
        self._schema = config.schema

    async def __aenter__(self) -> "FundamentalsDB":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def connect(self) -> None:
        """Establish database connection."""
        if self._is_connected:
            return

        try:
            logger.info(
                f"Connecting FundamentalsDB to PostgreSQL: {self._config.host}:{self._config.port}"
                f"/{self._config.database}"
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

            self._is_connected = True
            logger.info("FundamentalsDB connected to PostgreSQL")

        except Exception as e:
            logger.error(f"Failed to connect FundamentalsDB: {e}")
            raise ConnectionError(f"Cannot connect to fundamentals database: {e}") from e

    async def close(self) -> None:
        """Close database connection."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            self._is_connected = False
            logger.info("FundamentalsDB connection closed")

    def _ensure_connected(self) -> None:
        """Ensure database is connected."""
        if not self._is_connected or not self._pool:
            raise RuntimeError("FundamentalsDB is not connected. Call connect() first.")

    @property
    def _db_pool(self) -> asyncpg.Pool:
        """Get the database pool, raising if not connected."""
        self._ensure_connected()
        assert self._pool is not None
        return self._pool

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    async def get_fundamentals(self, stock_code: str) -> StockFundamentals | None:
        """
        Get fundamental data for a single stock.

        Args:
            stock_code: Stock code without suffix (e.g., "000001")

        Returns:
            StockFundamentals or None if not found.
        """
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT * FROM {self._schema}.stock_fundamentals WHERE stock_code = $1",
                stock_code,
            )

        if not row:
            return None

        return self._row_to_model(row)

    async def batch_get_fundamentals(self, stock_codes: list[str]) -> dict[str, StockFundamentals]:
        """
        Get fundamental data for multiple stocks.

        Args:
            stock_codes: List of stock codes.

        Returns:
            Dict mapping stock_code to StockFundamentals (only for found stocks).
        """
        if not stock_codes:
            return {}

        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {self._schema}.stock_fundamentals WHERE stock_code = ANY($1)",
                stock_codes,
            )

        return {row["stock_code"]: self._row_to_model(row) for row in rows}

    async def get_pe(self, stock_code: str) -> float | None:
        """Get PE(TTM) for a single stock."""
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT pe_ttm FROM {self._schema}.stock_fundamentals
                WHERE stock_code = $1
                """,
                stock_code,
            )

        if not row or row["pe_ttm"] is None:
            return None

        return float(row["pe_ttm"])

    async def batch_get_pe(self, stock_codes: list[str]) -> dict[str, float]:
        """
        Get PE(TTM) for multiple stocks.

        Args:
            stock_codes: List of stock codes.

        Returns:
            Dict mapping stock_code to PE value (only for stocks with valid PE).
        """
        if not stock_codes:
            return {}

        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT stock_code, pe_ttm FROM {self._schema}.stock_fundamentals
                WHERE stock_code = ANY($1) AND pe_ttm IS NOT NULL
                """,
                stock_codes,
            )

        return {row["stock_code"]: float(row["pe_ttm"]) for row in rows}

    async def batch_get_revenue_growth(self, stock_codes: list[str]) -> dict[str, float]:
        """
        Get quarterly revenue YoY growth (季度营收同比增长率) for multiple stocks.

        Returns:
            Dict mapping stock_code to quarterly_revenue_yoy % (only for stocks with valid data).
        """
        if not stock_codes:
            return {}

        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT stock_code, quarterly_revenue_yoy
                FROM {self._schema}.stock_fundamentals
                WHERE stock_code = ANY($1) AND quarterly_revenue_yoy IS NOT NULL
                """,
                stock_codes,
            )

        return {row["stock_code"]: float(row["quarterly_revenue_yoy"]) for row in rows}

    async def is_st(self, stock_code: str) -> bool:
        """
        Check if a stock is ST based on company_name.

        Returns True if ST or if stock not found (fail-safe: exclude unknown stocks).
        """
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT company_name FROM {self._schema}.stock_fundamentals
                WHERE stock_code = $1
                """,
                stock_code,
            )

        if not row:
            # Fail-safe: unknown stock treated as excluded
            return True

        return "ST" in row["company_name"].upper()

    async def batch_filter_st(self, stock_codes: list[str]) -> list[str]:
        """
        Filter out ST stocks from a list.

        Args:
            stock_codes: List of stock codes.

        Returns:
            List of non-ST stock codes (stocks not found in DB are excluded).
        """
        if not stock_codes:
            return []

        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT stock_code FROM {self._schema}.stock_fundamentals
                WHERE stock_code = ANY($1)
                  AND company_name NOT LIKE '%ST%'
                  AND company_name NOT LIKE '%st%'
                """,
                stock_codes,
            )

        return [row["stock_code"] for row in rows]

    def _row_to_model(self, row: asyncpg.Record) -> StockFundamentals:
        """Convert database row to StockFundamentals."""

        def to_float(val: Any) -> float | None:
            if val is None:
                return None
            if isinstance(val, Decimal):
                return float(val)
            return float(val)

        return StockFundamentals(
            stock_code=row["stock_code"],
            company_name=row["company_name"],
            pe_ttm=to_float(row["pe_ttm"]),
            ps_ttm=to_float(row["ps_ttm"]),
            pb=to_float(row["pb"]),
            total_market_cap=to_float(row["total_market_cap"]),
            roe=to_float(row["roe"]),
            annual_revenue_yoy=to_float(row["annual_revenue_yoy"]),
            quarterly_revenue_yoy=to_float(row["quarterly_revenue_yoy"]),
            annual_net_profit_yoy=to_float(row["annual_net_profit_yoy"]),
            quarterly_net_profit_yoy=to_float(row["quarterly_net_profit_yoy"]),
            report_date_annual=row["report_date_annual"],
            report_date_quarterly=row["report_date_quarterly"],
        )


def create_fundamentals_db_from_config() -> FundamentalsDB:
    """
    Create FundamentalsDB from configuration file.

    Returns:
        Configured FundamentalsDB instance.
    """
    from src.common.config import load_config

    config = load_config("config/database-config.yaml")
    db_config = config.get_dict("database.fundamentals", {})

    if not db_config:
        raise ValueError("Fundamentals database configuration not found")

    def resolve_env(value: Any) -> Any:
        if isinstance(value, str) and value.startswith("${"):
            inner = value[2:-1]
            if ":" in inner:
                var_name, default = inner.split(":", 1)
            else:
                var_name, default = inner, ""
            return os.environ.get(var_name, default)
        return value

    fundamentals_config = FundamentalsDBConfig(
        host=resolve_env(db_config.get("host", "localhost")),
        port=int(resolve_env(db_config.get("port", 5432))),
        database=resolve_env(db_config.get("database", "messages")),
        user=resolve_env(db_config.get("user", "reader")),
        password=resolve_env(db_config.get("password", "")),
        pool_min_size=db_config.get("pool_min_size", 2),
        pool_max_size=db_config.get("pool_max_size", 5),
        schema=db_config.get("schema", "public"),
    )

    return FundamentalsDB(fundamentals_config)
