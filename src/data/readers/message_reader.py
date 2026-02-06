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
# - JOIN message_analysis: Analysis results are joined from message_analysis table

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

from src.data.models.message import Message, MessageAnalysis, Sentiment

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
        sentiment: str | None = None,
        only_analyzed: bool = False,
        limit: int = 1000,
    ) -> list[Message]:
        """
        Get messages fetched since a given time with analysis results.

        Args:
            since: Fetch messages with fetch_time > since.
            source_type: Optional filter by source type (announcement/news/social).
            sentiment: Optional filter by sentiment ('strong_bullish', 'bullish', etc.).
            only_analyzed: If True, only return messages that have been analyzed.
            limit: Maximum number of messages to return.

        Returns:
            List of Message objects with analysis, ordered by fetch_time ascending.

        Raises:
            RuntimeError: If not connected.
        """
        self._ensure_connected()

        # JOIN with message_analysis to get analysis results
        join_type = "INNER JOIN" if only_analyzed else "LEFT JOIN"
        query = f"""
            SELECT m.id, m.source_type, m.source_name, m.title, m.content, m.url,
                   m.stock_codes, m.publish_time, m.fetch_time, m.raw_data,
                   a.sentiment, a.confidence, a.reasoning, a.extracted_entities,
                   a.matched_sector_ids, a.affected_stocks, a.analyzed_at, a.analysis_source
            FROM {self._config.table_name} m
            {join_type} message_analysis a ON m.id = a.message_id
            WHERE m.fetch_time > $1
        """
        params: list[Any] = [since]

        if source_type:
            query += f" AND m.source_type = ${len(params) + 1}"
            params.append(source_type)

        if sentiment:
            query += f" AND a.sentiment = ${len(params) + 1}"
            params.append(sentiment)

        query += f" ORDER BY m.fetch_time ASC LIMIT ${len(params) + 1}"
        params.append(limit)

        pool = self._ensure_connected()
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [self._row_to_message(row) for row in rows]

    async def get_positive_messages_since(
        self,
        since: datetime,
        source_type: str | None = None,
        limit: int = 500,
    ) -> list[Message]:
        """
        Get messages with positive sentiment (bullish or strong_bullish) since a given time.

        This is a convenience method for strategies that only care about positive signals.

        Args:
            since: Fetch messages with fetch_time > since.
            source_type: Optional filter by source type.
            limit: Maximum number of messages to return.

        Returns:
            List of Message objects with positive sentiment.
        """
        pool = self._ensure_connected()

        query = f"""
            SELECT m.id, m.source_type, m.source_name, m.title, m.content, m.url,
                   m.stock_codes, m.publish_time, m.fetch_time, m.raw_data,
                   a.sentiment, a.confidence, a.reasoning, a.extracted_entities,
                   a.matched_sector_ids, a.affected_stocks, a.analyzed_at, a.analysis_source
            FROM {self._config.table_name} m
            INNER JOIN message_analysis a ON m.id = a.message_id
            WHERE m.fetch_time > $1
              AND a.sentiment IN ('strong_bullish', 'bullish')
        """
        params: list[Any] = [since]

        if source_type:
            query += f" AND m.source_type = ${len(params) + 1}"
            params.append(source_type)

        query += f" ORDER BY m.fetch_time ASC LIMIT ${len(params) + 1}"
        params.append(limit)

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [self._row_to_message(row) for row in rows]

    async def get_messages_in_range(
        self,
        start_time: datetime,
        end_time: datetime,
        source_type: str | None = None,
        only_positive: bool = False,
        limit: int = 1000,
        offset: int = 0,
    ) -> list[Message]:
        """
        Get messages published within a time range with analysis results.

        Args:
            start_time: Start of publish_time range (inclusive).
            end_time: End of publish_time range (exclusive).
            source_type: Optional filter by source type.
            only_positive: If True, only return messages with positive sentiment.
            limit: Maximum number of messages to return.
            offset: Number of messages to skip (for pagination).

        Returns:
            List of Message objects with analysis, ordered by publish_time ascending.
        """
        pool = self._ensure_connected()

        join_type = "INNER JOIN" if only_positive else "LEFT JOIN"
        query = f"""
            SELECT m.id, m.source_type, m.source_name, m.title, m.content, m.url,
                   m.stock_codes, m.publish_time, m.fetch_time, m.raw_data,
                   a.sentiment, a.confidence, a.reasoning, a.extracted_entities,
                   a.matched_sector_ids, a.affected_stocks, a.analyzed_at, a.analysis_source
            FROM {self._config.table_name} m
            {join_type} message_analysis a ON m.id = a.message_id
            WHERE m.publish_time >= $1 AND m.publish_time < $2
        """
        params: list[Any] = [start_time, end_time]

        if source_type:
            query += f" AND m.source_type = ${len(params) + 1}"
            params.append(source_type)

        if only_positive:
            query += " AND a.sentiment IN ('strong_bullish', 'bullish')"

        query += f" ORDER BY m.publish_time ASC LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}"
        params.append(limit)
        params.append(offset)

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [self._row_to_message(row) for row in rows]

    async def count_messages_in_range(
        self,
        start_time: datetime,
        end_time: datetime,
        source_type: str | None = None,
        only_positive: bool = False,
    ) -> int:
        """
        Count messages published within a time range.

        Args:
            start_time: Start of publish_time range (inclusive).
            end_time: End of publish_time range (exclusive).
            source_type: Optional filter by source type.
            only_positive: If True, only count messages with positive sentiment.

        Returns:
            Number of matching messages.
        """
        pool = self._ensure_connected()

        join_type = "INNER JOIN" if only_positive else "LEFT JOIN"
        query = f"""
            SELECT COUNT(*)
            FROM {self._config.table_name} m
            {join_type} message_analysis a ON m.id = a.message_id
            WHERE m.publish_time >= $1 AND m.publish_time < $2
        """
        params: list[Any] = [start_time, end_time]

        if source_type:
            query += f" AND m.source_type = ${len(params) + 1}"
            params.append(source_type)

        if only_positive:
            query += " AND a.sentiment IN ('strong_bullish', 'bullish')"

        async with pool.acquire() as conn:
            count = await conn.fetchval(query, *params)

        return count or 0

    async def get_messages_by_stock(
        self,
        stock_code: str,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[Message]:
        """
        Get messages related to a specific stock with analysis results.

        Args:
            stock_code: Stock code to search for.
            since: Optional filter by fetch_time.
            limit: Maximum number of messages to return.

        Returns:
            List of Message objects with analysis containing the stock code.
        """
        pool = self._ensure_connected()

        # Normalize stock code (remove suffix)
        code = stock_code.split(".")[0] if "." in stock_code else stock_code

        query = f"""
            SELECT m.id, m.source_type, m.source_name, m.title, m.content, m.url,
                   m.stock_codes, m.publish_time, m.fetch_time, m.raw_data,
                   a.sentiment, a.confidence, a.reasoning, a.extracted_entities,
                   a.matched_sector_ids, a.affected_stocks, a.analyzed_at, a.analysis_source
            FROM {self._config.table_name} m
            LEFT JOIN message_analysis a ON m.id = a.message_id
            WHERE $1 = ANY(m.stock_codes)
        """
        params: list[Any] = [code]

        if since:
            query += f" AND m.fetch_time > ${len(params) + 1}"
            params.append(since)

        query += f" ORDER BY m.fetch_time DESC LIMIT ${len(params) + 1}"
        params.append(limit)

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        return [self._row_to_message(row) for row in rows]

    async def get_latest_fetch_time(self) -> datetime | None:
        """
        Get the latest fetch_time in the database.

        Returns:
            Latest fetch_time or None if table is empty.
        """
        pool = self._ensure_connected()

        query = f"SELECT MAX(fetch_time) FROM {self._config.table_name}"

        async with pool.acquire() as conn:
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
        pool = self._ensure_connected()

        query = f"SELECT COUNT(*) FROM {self._config.table_name} WHERE 1=1"
        params: list[Any] = []

        if since:
            query += f" AND fetch_time > ${len(params) + 1}"
            params.append(since)

        if source_type:
            query += f" AND source_type = ${len(params) + 1}"
            params.append(source_type)

        async with pool.acquire() as conn:
            count = await conn.fetchval(query, *params)

        return count or 0

    def _row_to_message(self, row: asyncpg.Record) -> Message:
        """Convert database row to Message object with analysis."""
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

        # Parse analysis result if present
        analysis = None
        sentiment_value = row.get("sentiment")
        if sentiment_value:
            try:
                sentiment = Sentiment(sentiment_value)

                # Parse array fields
                extracted_entities = row.get("extracted_entities") or []
                if isinstance(extracted_entities, str):
                    extracted_entities = json.loads(extracted_entities)

                matched_sector_ids = row.get("matched_sector_ids") or []
                if isinstance(matched_sector_ids, str):
                    matched_sector_ids = json.loads(matched_sector_ids)

                affected_stocks = row.get("affected_stocks") or []
                if isinstance(affected_stocks, str):
                    affected_stocks = json.loads(affected_stocks)

                analysis = MessageAnalysis(
                    sentiment=sentiment,
                    confidence=float(row.get("confidence") or 0.0),
                    reasoning=row.get("reasoning") or "",
                    extracted_entities=list(extracted_entities),
                    matched_sector_ids=list(matched_sector_ids),
                    affected_stocks=list(affected_stocks),
                    analyzed_at=row.get("analyzed_at"),
                    analysis_source=row.get("analysis_source") or "text",
                )
            except (ValueError, KeyError) as e:
                logger.warning(f"Failed to parse analysis for message {row['id']}: {e}")
                analysis = None

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
            analysis=analysis,
        )

    def _ensure_connected(self) -> asyncpg.Pool:
        """Ensure reader is connected and return pool."""
        if not self._is_connected or not self._pool:
            raise RuntimeError("MessageReader is not connected. Call connect() first.")
        return self._pool

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
    import os
    from typing import Any

    from src.common.config import load_config

    config = load_config("config/database-config.yaml")
    db_config = config.get_dict("database.messages", {})

    if not db_config:
        raise ValueError("Database configuration not found in config/database-config.yaml")

    # Support environment variable substitution: ${VAR:default} or ${VAR}
    def resolve_env(value: Any) -> Any:
        if isinstance(value, str) and value.startswith("${"):
            inner = value[2:-1]
            if ":" in inner:
                var_name, default = inner.split(":", 1)
            else:
                var_name, default = inner, ""
            return os.environ.get(var_name, default)
        return value

    reader_config = MessageReaderConfig(
        host=resolve_env(db_config.get("host", "localhost")),
        port=int(resolve_env(db_config.get("port", 5432))),
        database=resolve_env(db_config.get("database", "messages")),
        user=resolve_env(db_config.get("user", "reader")),
        password=resolve_env(db_config.get("password", "")),
        pool_min_size=db_config.get("pool_min_size", 2),
        pool_max_size=db_config.get("pool_max_size", 10),
        table_name=db_config.get("table_name", "messages"),
    )

    return MessageReader(reader_config)
