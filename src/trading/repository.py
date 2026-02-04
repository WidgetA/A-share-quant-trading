# === MODULE PURPOSE ===
# PostgreSQL repository for trading data persistence.
# Uses separate 'trading' schema for isolation from messages.

# === DEPENDENCIES ===
# - asyncpg: Async PostgreSQL client
# - position_manager: Data models for positions

# === KEY CONCEPTS ===
# - Schema isolation: All tables in 'trading' schema
# - Tables: position_slots, stock_holdings, orders, transactions
# - Auto-migration: Creates schema and tables if not exist

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)


class OrderType(Enum):
    """Order direction."""

    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    """Order execution status."""

    PENDING = "pending"  # Created, not submitted
    SUBMITTED = "submitted"  # Sent to broker
    PARTIAL = "partial"  # Partially filled
    FILLED = "filled"  # Fully filled
    CANCELLED = "cancelled"  # Cancelled by user
    FAILED = "failed"  # Submission failed


@dataclass
class TradingRepositoryConfig:
    """Configuration for trading repository."""

    host: str = "localhost"
    port: int = 5432
    database: str = "messages"
    user: str = "reader"
    password: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 5
    schema: str = "trading"
    auto_create_schema: bool = True


# SQL for schema and table creation
SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS {schema};
"""

TABLES_SQL = """
-- Position slots table
CREATE TABLE IF NOT EXISTS {schema}.position_slots (
    id SERIAL PRIMARY KEY,
    slot_id INTEGER NOT NULL UNIQUE,
    slot_type VARCHAR(20) NOT NULL,  -- premarket/intraday
    state VARCHAR(20) NOT NULL DEFAULT 'empty',  -- empty/pending_buy/filled/pending_sell
    entry_time TIMESTAMP,
    entry_reason TEXT,
    sector_name VARCHAR(100),
    pending_order_id VARCHAR(50),
    exit_price DECIMAL(12, 4),
    exit_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Stock holdings table (supports multiple stocks per slot for sector buying)
CREATE TABLE IF NOT EXISTS {schema}.stock_holdings (
    id SERIAL PRIMARY KEY,
    slot_id INTEGER NOT NULL REFERENCES {schema}.position_slots(slot_id) ON DELETE CASCADE,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(50),
    quantity INTEGER NOT NULL DEFAULT 0,
    entry_price DECIMAL(12, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders table
CREATE TABLE IF NOT EXISTS {schema}.orders (
    id VARCHAR(50) PRIMARY KEY,
    slot_id INTEGER REFERENCES {schema}.position_slots(slot_id),
    order_type VARCHAR(10) NOT NULL,  -- buy/sell
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(50),
    quantity INTEGER NOT NULL,
    price DECIMAL(12, 4),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    reason TEXT,
    broker_order_id VARCHAR(100),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transactions table (actual fills)
CREATE TABLE IF NOT EXISTS {schema}.transactions (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES {schema}.orders(id),
    stock_code VARCHAR(20) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(12, 4) NOT NULL,
    amount DECIMAL(16, 2) NOT NULL,
    commission DECIMAL(12, 4) DEFAULT 0,
    transaction_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Overnight holdings tracking (for morning review)
CREATE TABLE IF NOT EXISTS {schema}.overnight_holdings (
    id SERIAL PRIMARY KEY,
    slot_id INTEGER NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(50),
    quantity INTEGER NOT NULL,
    entry_price DECIMAL(12, 4),
    entry_time TIMESTAMP,
    entry_reason TEXT,
    slot_type VARCHAR(20),
    record_date DATE NOT NULL,  -- The date holdings were recorded
    current_price DECIMAL(12, 4),
    pnl_amount DECIMAL(16, 2),
    pnl_percent DECIMAL(8, 4),
    reviewed BOOLEAN DEFAULT FALSE,
    review_action VARCHAR(20),  -- sell/hold
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_holdings_slot_id ON {schema}.stock_holdings(slot_id);
CREATE INDEX IF NOT EXISTS idx_orders_slot_id ON {schema}.orders(slot_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON {schema}.orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON {schema}.orders(created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_order_id ON {schema}.transactions(order_id);
CREATE INDEX IF NOT EXISTS idx_transactions_time ON {schema}.transactions(transaction_time);
CREATE INDEX IF NOT EXISTS idx_overnight_record_date ON {schema}.overnight_holdings(record_date);
"""


class TradingRepository:
    """
    PostgreSQL repository for trading data.

    Handles persistence of:
    - Position slots and their state
    - Stock holdings within slots
    - Orders and their status
    - Transaction records

    Usage:
        repo = TradingRepository(config)
        await repo.connect()

        # Save position slot
        await repo.save_slot(slot_data)

        # Get all slots
        slots = await repo.get_all_slots()

        # Create order
        order_id = await repo.create_order(order_data)

        await repo.close()
    """

    def __init__(self, config: TradingRepositoryConfig):
        self._config = config
        self._pool: asyncpg.Pool | None = None
        self._is_connected = False
        self._schema = config.schema

    async def connect(self) -> None:
        """Establish connection pool and initialize schema."""
        if self._is_connected:
            return

        try:
            logger.info(
                f"Connecting to PostgreSQL: {self._config.host}:{self._config.port}"
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

            # Initialize schema and tables
            if self._config.auto_create_schema:
                await self._init_schema()

            self._is_connected = True
            logger.info("TradingRepository connected to PostgreSQL")

        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise ConnectionError(f"Cannot connect to trading database: {e}") from e

    async def close(self) -> None:
        """Close connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            self._is_connected = False
            logger.info("TradingRepository disconnected")

    async def _init_schema(self) -> None:
        """Create schema and tables if not exist."""
        # Note: Called during connect() before _is_connected is set, so use _pool directly
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            # Create schema
            await conn.execute(SCHEMA_SQL.format(schema=self._schema))

            # Create tables
            await conn.execute(TABLES_SQL.format(schema=self._schema))

            logger.info(f"Initialized trading schema: {self._schema}")

    # ==================== Position Slots ====================

    async def save_slot(self, slot_data: dict[str, Any]) -> None:
        """
        Save or update a position slot.

        Args:
            slot_data: Slot data dictionary with keys:
                - slot_id, slot_type, state, entry_time, entry_reason,
                - sector_name, pending_order_id, exit_price, exit_time
        """
        async with self._db_pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.position_slots
                    (slot_id, slot_type, state, entry_time, entry_reason,
                     sector_name, pending_order_id, exit_price, exit_time, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP)
                ON CONFLICT (slot_id) DO UPDATE SET
                    slot_type = EXCLUDED.slot_type,
                    state = EXCLUDED.state,
                    entry_time = EXCLUDED.entry_time,
                    entry_reason = EXCLUDED.entry_reason,
                    sector_name = EXCLUDED.sector_name,
                    pending_order_id = EXCLUDED.pending_order_id,
                    exit_price = EXCLUDED.exit_price,
                    exit_time = EXCLUDED.exit_time,
                    updated_at = CURRENT_TIMESTAMP
                """,
                slot_data["slot_id"],
                slot_data["slot_type"],
                slot_data["state"],
                slot_data.get("entry_time"),
                slot_data.get("entry_reason"),
                slot_data.get("sector_name"),
                slot_data.get("pending_order_id"),
                slot_data.get("exit_price"),
                slot_data.get("exit_time"),
            )

    async def get_slot(self, slot_id: int) -> dict[str, Any] | None:
        """Get a single slot by ID."""
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT slot_id, slot_type, state, entry_time, entry_reason,
                       sector_name, pending_order_id, exit_price, exit_time
                FROM {self._schema}.position_slots
                WHERE slot_id = $1
                """,
                slot_id,
            )

        if not row:
            return None

        return dict(row)

    async def get_all_slots(self) -> list[dict[str, Any]]:
        """Get all position slots."""
        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT slot_id, slot_type, state, entry_time, entry_reason,
                       sector_name, pending_order_id, exit_price, exit_time
                FROM {self._schema}.position_slots
                ORDER BY slot_id
                """
            )

        return [dict(row) for row in rows]

    async def reset_slot(self, slot_id: int) -> None:
        """Reset a slot to empty state and delete its holdings."""
        async with self._db_pool.acquire() as conn:
            async with conn.transaction():
                # Delete holdings first (foreign key)
                await conn.execute(
                    f"DELETE FROM {self._schema}.stock_holdings WHERE slot_id = $1",
                    slot_id,
                )

                # Reset slot
                await conn.execute(
                    f"""
                    UPDATE {self._schema}.position_slots SET
                        state = 'empty',
                        entry_time = NULL,
                        entry_reason = NULL,
                        sector_name = NULL,
                        pending_order_id = NULL,
                        exit_price = NULL,
                        exit_time = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE slot_id = $1
                    """,
                    slot_id,
                )

    # ==================== Stock Holdings ====================

    async def save_holdings(self, slot_id: int, holdings: list[dict[str, Any]]) -> None:
        """
        Save holdings for a slot (replaces existing).

        Args:
            slot_id: The slot ID
            holdings: List of holding dicts with stock_code, stock_name, quantity, entry_price
        """
        async with self._db_pool.acquire() as conn:
            async with conn.transaction():
                # Delete existing holdings
                await conn.execute(
                    f"DELETE FROM {self._schema}.stock_holdings WHERE slot_id = $1",
                    slot_id,
                )

                # Insert new holdings
                for h in holdings:
                    await conn.execute(
                        f"""
                        INSERT INTO {self._schema}.stock_holdings
                            (slot_id, stock_code, stock_name, quantity, entry_price)
                        VALUES ($1, $2, $3, $4, $5)
                        """,
                        slot_id,
                        h["stock_code"],
                        h.get("stock_name"),
                        h.get("quantity", 0),
                        h.get("entry_price"),
                    )

    async def get_holdings(self, slot_id: int) -> list[dict[str, Any]]:
        """Get all holdings for a slot."""
        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT stock_code, stock_name, quantity, entry_price
                FROM {self._schema}.stock_holdings
                WHERE slot_id = $1
                """,
                slot_id,
            )

        return [dict(row) for row in rows]

    async def get_all_holdings(self) -> dict[int, list[dict[str, Any]]]:
        """Get all holdings grouped by slot_id."""
        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT slot_id, stock_code, stock_name, quantity, entry_price
                FROM {self._schema}.stock_holdings
                ORDER BY slot_id, id
                """
            )

        result: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            slot_id = row["slot_id"]
            if slot_id not in result:
                result[slot_id] = []
            result[slot_id].append(
                {
                    "stock_code": row["stock_code"],
                    "stock_name": row["stock_name"],
                    "quantity": row["quantity"],
                    "entry_price": float(row["entry_price"]) if row["entry_price"] else None,
                }
            )

        return result

    async def get_account_state(self) -> dict[str, Any] | None:
        """
        Get account state including cash balance.

        Returns:
            Dict with total_capital, total_assets, cash_balance, realized_pnl
            or None if no account state exists.
        """
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT total_capital, total_assets, cash_balance, realized_pnl
                FROM {self._schema}.account_state
                ORDER BY updated_at DESC
                LIMIT 1
                """
            )

        if row:
            return {
                "total_capital": float(row["total_capital"]) if row["total_capital"] else 0.0,
                "total_assets": float(row["total_assets"]) if row["total_assets"] else 0.0,
                "cash_balance": float(row["cash_balance"]) if row["cash_balance"] else 0.0,
                "realized_pnl": float(row["realized_pnl"]) if row["realized_pnl"] else 0.0,
            }
        return None

    # ==================== Orders ====================

    async def create_order(
        self,
        slot_id: int | None,
        order_type: OrderType | str,
        stock_code: str,
        quantity: int,
        price: float | None = None,
        stock_name: str | None = None,
        reason: str | None = None,
    ) -> str:
        """
        Create a new order.

        Returns:
            Order ID (UUID)
        """
        self._ensure_connected()

        order_id = str(uuid.uuid4())
        order_type_str = order_type.value if isinstance(order_type, OrderType) else order_type

        async with self._db_pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._schema}.orders
                    (id, slot_id, order_type, stock_code, stock_name,
                     quantity, price, status, reason)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8)
                """,
                order_id,
                slot_id,
                order_type_str,
                stock_code,
                stock_name,
                quantity,
                price,
                reason,
            )

        logger.info(f"Created order {order_id}: {order_type_str} {stock_code} x{quantity}")
        return order_id

    async def update_order_status(
        self,
        order_id: str,
        status: OrderStatus | str,
        broker_order_id: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Update order status."""
        self._ensure_connected()

        status_str = status.value if isinstance(status, OrderStatus) else status

        async with self._db_pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.orders SET
                    status = $2,
                    broker_order_id = COALESCE($3, broker_order_id),
                    error_message = $4,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = $1
                """,
                order_id,
                status_str,
                broker_order_id,
                error_message,
            )

    async def get_order(self, order_id: str) -> dict[str, Any] | None:
        """Get order by ID."""
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT * FROM {self._schema}.orders WHERE id = $1",
                order_id,
            )

        return dict(row) if row else None

    async def get_orders_by_slot(self, slot_id: int) -> list[dict[str, Any]]:
        """Get all orders for a slot."""
        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM {self._schema}.orders
                WHERE slot_id = $1
                ORDER BY created_at DESC
                """,
                slot_id,
            )

        return [dict(row) for row in rows]

    async def get_pending_orders(self) -> list[dict[str, Any]]:
        """Get all pending orders."""
        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM {self._schema}.orders
                WHERE status IN ('pending', 'submitted')
                ORDER BY created_at
                """
            )

        return [dict(row) for row in rows]

    # ==================== Transactions ====================

    async def create_transaction(
        self,
        order_id: str,
        stock_code: str,
        quantity: int,
        price: float,
        commission: float = 0,
        transaction_time: datetime | None = None,
    ) -> int:
        """
        Record a transaction (fill).

        Returns:
            Transaction ID
        """
        self._ensure_connected()

        amount = quantity * price
        tx_time = transaction_time or datetime.now()

        async with self._db_pool.acquire() as conn:
            tx_id = await conn.fetchval(
                f"""
                INSERT INTO {self._schema}.transactions
                    (order_id, stock_code, quantity, price, amount, commission, transaction_time)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id
                """,
                order_id,
                stock_code,
                quantity,
                price,
                amount,
                commission,
                tx_time,
            )

        logger.info(f"Created transaction {tx_id}: {stock_code} x{quantity} @ {price}")
        return tx_id

    async def get_transactions_by_order(self, order_id: str) -> list[dict[str, Any]]:
        """Get all transactions for an order."""
        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT * FROM {self._schema}.transactions
                WHERE order_id = $1
                ORDER BY transaction_time
                """,
                order_id,
            )

        return [dict(row) for row in rows]

    async def get_transactions_in_range(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, Any]]:
        """Get transactions within a time range."""
        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT t.*, o.order_type, o.slot_id
                FROM {self._schema}.transactions t
                JOIN {self._schema}.orders o ON t.order_id = o.id
                WHERE t.transaction_time >= $1 AND t.transaction_time < $2
                ORDER BY t.transaction_time
                """,
                start_time,
                end_time,
            )

        return [dict(row) for row in rows]

    # ==================== Overnight Holdings ====================

    async def save_overnight_holdings(
        self,
        record_date: datetime,
        holdings: list[dict[str, Any]],
    ) -> None:
        """
        Save overnight holdings for morning review.

        Args:
            record_date: The date holdings were recorded (market close date).
            holdings: List of holding dicts with slot_id, stock_code, etc.
        """
        self._ensure_connected()

        date_only = record_date.date() if isinstance(record_date, datetime) else record_date

        async with self._db_pool.acquire() as conn:
            async with conn.transaction():
                # Delete existing records for this date
                await conn.execute(
                    f"DELETE FROM {self._schema}.overnight_holdings WHERE record_date = $1",
                    date_only,
                )

                # Insert new records
                for h in holdings:
                    await conn.execute(
                        f"""
                        INSERT INTO {self._schema}.overnight_holdings
                            (slot_id, stock_code, stock_name, quantity, entry_price,
                             entry_time, entry_reason, slot_type, record_date)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        """,
                        h["slot_id"],
                        h["stock_code"],
                        h.get("stock_name"),
                        h["quantity"],
                        h.get("entry_price"),
                        h.get("entry_time"),
                        h.get("entry_reason"),
                        h.get("slot_type"),
                        date_only,
                    )

        logger.info(f"Saved {len(holdings)} overnight holdings for {date_only}")

    async def get_overnight_holdings(
        self,
        record_date: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get overnight holdings for review.

        Args:
            record_date: Date to query. If None, gets latest.

        Returns:
            List of overnight holding records.
        """
        async with self._db_pool.acquire() as conn:
            if record_date:
                date_only = record_date.date() if isinstance(record_date, datetime) else record_date
                rows = await conn.fetch(
                    f"""
                    SELECT * FROM {self._schema}.overnight_holdings
                    WHERE record_date = $1
                    ORDER BY slot_id
                    """,
                    date_only,
                )
            else:
                # Get latest date's holdings
                rows = await conn.fetch(
                    f"""
                    SELECT * FROM {self._schema}.overnight_holdings
                    WHERE record_date = (
                        SELECT MAX(record_date) FROM {self._schema}.overnight_holdings
                    )
                    ORDER BY slot_id
                    """
                )

        return [dict(row) for row in rows]

    async def update_overnight_holding_price(
        self,
        holding_id: int,
        current_price: float,
        pnl_amount: float,
        pnl_percent: float,
    ) -> None:
        """Update overnight holding with current price and P&L."""
        async with self._db_pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.overnight_holdings SET
                    current_price = $2,
                    pnl_amount = $3,
                    pnl_percent = $4
                WHERE id = $1
                """,
                holding_id,
                current_price,
                pnl_amount,
                pnl_percent,
            )

    async def mark_overnight_holding_reviewed(
        self,
        holding_id: int,
        action: str,  # "sell" or "hold"
    ) -> None:
        """Mark overnight holding as reviewed with action."""
        async with self._db_pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._schema}.overnight_holdings SET
                    reviewed = TRUE,
                    review_action = $2
                WHERE id = $1
                """,
                holding_id,
                action,
            )

    # ==================== Utilities ====================

    def _ensure_connected(self) -> None:
        """Ensure repository is connected."""
        if not self._is_connected or not self._pool:
            raise RuntimeError("TradingRepository is not connected. Call connect() first.")

    @property
    def _db_pool(self) -> asyncpg.Pool:
        """Get the database pool, raising if not connected."""
        self._ensure_connected()
        assert self._pool is not None  # For type checker
        return self._pool

    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._is_connected


def create_trading_repository_from_config() -> TradingRepository:
    """
    Create TradingRepository from configuration file.

    Returns:
        Configured TradingRepository instance.
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

    repo_config = TradingRepositoryConfig(
        host=resolve_env(db_config.get("host", "localhost")),
        port=int(resolve_env(db_config.get("port", 5432))),
        database=resolve_env(db_config.get("database", "messages")),
        user=resolve_env(db_config.get("user", "reader")),
        password=resolve_env(db_config.get("password", "")),
        pool_min_size=db_config.get("pool_min_size", 2),
        pool_max_size=db_config.get("pool_max_size", 5),
        schema=db_config.get("schema", "trading"),
        auto_create_schema=db_config.get("auto_create_schema", True),
    )

    return TradingRepository(repo_config)
