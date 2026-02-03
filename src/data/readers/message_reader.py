# === MODULE PURPOSE ===
# Platform-level component for reading messages from external PostgreSQL database.
# Message collection is handled by a separate project that streams data into PostgreSQL.
# This reader provides a unified interface for all strategies to access messages.

# === DEPENDENCIES ===
# - asyncpg: Async PostgreSQL client
# - External PostgreSQL: Message data source (read-only)

# === KEY CONCEPTS ===
# - MessageReader: Platform layer component, initialized once by SystemManager
# - Strategies access messages via StrategyContext, not directly through this reader
# - Polling-based: Uses incremental queries with fetch_time for efficiency

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

from src.data.models.message import Message

logger = logging.getLogger(__name__)


@dataclass
class MessageReaderConfig:
    """Configuration for PostgreSQL message reader."""

    host: str = "localhost"
    port: int = 5432
    database: str = "messages"
    user: str = "reader"
    password: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 10
    table_name: str = "messages"


class MessageReader:
    """
    Reads messages from external PostgreSQL database.

    Architecture:
        External Collector → PostgreSQL → MessageReader → StrategyContext → Strategies

    Usage:
        # Platform layer initialization (in SystemManager)
        reader = MessageReader(config)
        await reader.connect()

        # Query messages
        messages = await reader.get_messages_since(last_check_time)

        # Cleanup
        await reader.close()

    Thread Safety:
        This class uses asyncpg connection pool which is thread-safe.
        Multiple concurrent queries are supported.
    """

    def __init__(self, config: MessageReaderConfig):
        """
        Initialize message reader.

        Args:
            config: PostgreSQL connection configuration.
        """
        self._config = config
        self._pool: asyncpg.Pool | None = None
        self._is_connected = False

    async def connect(self) -> None:
        """
        Establish connection pool to PostgreSQL.

        Raises:
            ConnectionError: If connection fails.
        """
        if self._is_connected:
            logger.debug("MessageReader already connected")
            return

        try:
            logger.info(
                f"Connecting to PostgreSQL: {self._config.host}:{self._config.port}"
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

            # Verify connection
            async with self._pool.acquire() as conn:
                await conn.fetchval("SELECT 1")

            self._is_connected = True
            logger.info("MessageReader connected to PostgreSQL")

        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise ConnectionError(f"Cannot connect to message database: {e}") from e

    async def close(self) -> None:
        """Close connection pool."""
        if not self._is_connected or not self._pool:
            return

        await self._pool.close()
        self._pool = None
        self._is_connected = False
        logger.info("MessageReader disconnected from PostgreSQL")

    async def get_messages_since(
        self,
        since: datetime,
        source_type: str | None = None,
        limit: int = 1000,
    ) -> list[Message]:
        """
        Get messages fetched since a given time.

        Args:
            since: Fetch messages with fetch_time > since.
            source_type: Optional filter by source type (announcement/news/social).
            limit: Maximum number of messages to return.

        Returns:
            List of Message objects, ordered by fetch_time ascending.

        Raises:
            RuntimeError: If not connected.
        """
        self._ensure_connected()

        query = f"""
            SELECT id, source_type, source_name, title, content, url,
                   stock_codes, publish_time, fetch_time, raw_data
            FROM {self._config.table_name}
            WHERE fetch_time > $1
        """
        params: list[Any] = [since]

        if source_type:
            query += " AND source_type = $2"
            params.append(source_type)

        query += " ORDER BY fetch_time ASC LIMIT $" + str(len(params) + 1)
        params.append(limit)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [self._row_to_message(row) for row in rows]

    async def get_messages_in_range(
        self,
        start_time: datetime,
        end_time: datetime,
        source_type: str | None = None,
        limit: int = 1000,
    ) -> list[Message]:
        """
        Get messages published within a time range.

        Args:
            start_time: Start of publish_time range (inclusive).
            end_time: End of publish_time range (exclusive).
            source_type: Optional filter by source type.
            limit: Maximum number of messages to return.

        Returns:
            List of Message objects, ordered by publish_time ascending.
        """
        self._ensure_connected()

        query = f"""
            SELECT id, source_type, source_name, title, content, url,
                   stock_codes, publish_time, fetch_time, raw_data
            FROM {self._config.table_name}
            WHERE publish_time >= $1 AND publish_time < $2
        """
        params: list[Any] = [start_time, end_time]

        if source_type:
            query += " AND source_type = $3"
            params.append(source_type)

        query += " ORDER BY publish_time ASC LIMIT $" + str(len(params) + 1)
        params.append(limit)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [self._row_to_message(row) for row in rows]

    async def get_messages_by_stock(
        self,
        stock_code: str,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[Message]:
        """
        Get messages related to a specific stock.

        Args:
            stock_code: Stock code to search for.
            since: Optional filter by fetch_time.
            limit: Maximum number of messages to return.

        Returns:
            List of Message objects containing the stock code.
        """
        self._ensure_connected()

        # Normalize stock code (remove suffix)
        code = stock_code.split(".")[0] if "." in stock_code else stock_code

        query = f"""
            SELECT id, source_type, source_name, title, content, url,
                   stock_codes, publish_time, fetch_time, raw_data
            FROM {self._config.table_name}
            WHERE stock_codes LIKE $1
        """
        params: list[Any] = [f"%{code}%"]

        if since:
            query += " AND fetch_time > $2"
            params.append(since)

        query += " ORDER BY fetch_time DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [self._row_to_message(row) for row in rows]

    async def get_latest_fetch_time(self) -> datetime | None:
        """
        Get the latest fetch_time in the database.

        Returns:
            Latest fetch_time or None if table is empty.
        """
        self._ensure_connected()

        query = f"SELECT MAX(fetch_time) FROM {self._config.table_name}"

        async with self._pool.acquire() as conn:
            result = await conn.fetchval(query)

        return result

    async def get_message_count(
        self,
        since: datetime | None = None,
        source_type: str | None = None,
    ) -> int:
        """
        Get count of messages.

        Args:
            since: Optional filter by fetch_time.
            source_type: Optional filter by source type.

        Returns:
            Number of matching messages.
        """
        self._ensure_connected()

        query = f"SELECT COUNT(*) FROM {self._config.table_name} WHERE 1=1"
        params: list[Any] = []

        if since:
            query += f" AND fetch_time > ${len(params) + 1}"
            params.append(since)

        if source_type:
            query += f" AND source_type = ${len(params) + 1}"
            params.append(source_type)

        async with self._pool.acquire() as conn:
            count = await conn.fetchval(query, *params)

        return count or 0

    def _row_to_message(self, row: asyncpg.Record) -> Message:
        """Convert database row to Message object."""
        # Parse stock_codes (could be JSON array or comma-separated string)
        stock_codes_raw = row["stock_codes"]
        if stock_codes_raw:
            if isinstance(stock_codes_raw, str):
                if stock_codes_raw.startswith("["):
                    stock_codes = json.loads(stock_codes_raw)
                else:
                    stock_codes = [c.strip() for c in stock_codes_raw.split(",") if c.strip()]
            else:
                stock_codes = list(stock_codes_raw)
        else:
            stock_codes = []

        # Parse raw_data (could be JSONB or string)
        raw_data = row["raw_data"]
        if isinstance(raw_data, str):
            try:
                raw_data = json.loads(raw_data)
            except (json.JSONDecodeError, TypeError):
                raw_data = None

        return Message(
            id=str(row["id"]),
            source_type=row["source_type"],
            source_name=row["source_name"],
            title=row["title"],
            content=row["content"] or "",
            url=row["url"],
            stock_codes=stock_codes,
            publish_time=row["publish_time"],
            fetch_time=row["fetch_time"],
            raw_data=raw_data,
        )

    def _ensure_connected(self) -> None:
        """Ensure reader is connected."""
        if not self._is_connected or not self._pool:
            raise RuntimeError("MessageReader is not connected. Call connect() first.")

    @property
    def is_connected(self) -> bool:
        """Check if reader is connected."""
        return self._is_connected


def create_message_reader_from_config() -> MessageReader:
    """
    Create MessageReader from configuration file.

    Returns:
        Configured MessageReader instance.

    Raises:
        ValueError: If configuration is missing.
    """
    from src.common.config import load_config

    config = load_config("config/database-config.yaml")
    db_config = config.get_dict("database.messages", {})

    if not db_config:
        raise ValueError("Database configuration not found in config/database-config.yaml")

    # Support environment variable substitution for password
    import os

    password = db_config.get("password", "")
    if password.startswith("${") and password.endswith("}"):
        env_var = password[2:-1]
        password = os.environ.get(env_var, "")

    reader_config = MessageReaderConfig(
        host=db_config.get("host", "localhost"),
        port=db_config.get("port", 5432),
        database=db_config.get("database", "messages"),
        user=db_config.get("user", "reader"),
        password=password,
        pool_min_size=db_config.get("pool_min_size", 2),
        pool_max_size=db_config.get("pool_max_size", 10),
        table_name=db_config.get("table_name", "messages"),
    )

    return MessageReader(reader_config)
