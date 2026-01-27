# === MODULE PURPOSE ===
# SQLite database layer for message persistence.
# Handles all database operations for storing and querying messages.

# === DEPENDENCIES ===
# - aiosqlite: Async SQLite operations
# - Message model: Data structure for messages

# === KEY CONCEPTS ===
# - Async operations: All DB operations are async for non-blocking I/O
# - Auto-migration: Table is created automatically on first use
# - Connection pooling: Single connection reused across operations

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator

import aiosqlite

from src.data.models.message import Message

logger = logging.getLogger(__name__)


class MessageDatabase:
    """
    Async SQLite database for message storage.

    Data Flow:
        Message -> save() -> SQLite
        SQLite -> query() -> Message

    Usage:
        async with MessageDatabase("messages.db") as db:
            await db.save(message)
            messages = await db.query(source_type="news")
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "MessageDatabase":
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
        logger.info(f"Connected to database: {self.db_path}")

    async def close(self) -> None:
        """Close database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None
            logger.info("Database connection closed")

    async def _create_tables(self) -> None:
        """Create messages table if not exists."""
        if not self._connection:
            raise RuntimeError("Database not connected")

        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                source_name TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                url TEXT,
                stock_codes TEXT,
                publish_time TEXT NOT NULL,
                fetch_time TEXT NOT NULL,
                raw_data TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create indexes for common queries
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_source_type
            ON messages(source_type)
        """)
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_publish_time
            ON messages(publish_time DESC)
        """)
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_source_name
            ON messages(source_name)
        """)

        await self._connection.commit()

    async def save(self, message: Message) -> bool:
        """
        Save a single message to database if not exists.

        Args:
            message: The message to save

        Returns:
            True if message was saved, False if it already existed
        """
        if not self._connection:
            raise RuntimeError("Database not connected")

        async with self._lock:
            # Check if already exists
            if await self._exists_unlocked(message.id):
                logger.debug(f"Message already exists, skipping: {message.id}")
                return False

            data = message.to_dict()
            await self._connection.execute(
                """
                INSERT INTO messages
                (id, source_type, source_name, title, content, url,
                 stock_codes, publish_time, fetch_time, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data["id"],
                    data["source_type"],
                    data["source_name"],
                    data["title"],
                    data["content"],
                    data["url"],
                    data["stock_codes"],
                    data["publish_time"],
                    data["fetch_time"],
                    data["raw_data"],
                ),
            )
            await self._connection.commit()
            logger.debug(f"Saved message: {message.id} - {message.title[:50]}")
            return True

    async def _exists_unlocked(self, message_id: str) -> bool:
        """Check if message exists (must be called with lock held)."""
        async with self._connection.execute(
            "SELECT 1 FROM messages WHERE id = ?", (message_id,)
        ) as cursor:
            return await cursor.fetchone() is not None

    async def save_batch(self, messages: list[Message]) -> int:
        """
        Save multiple messages, skipping those that already exist.

        Args:
            messages: List of messages to save

        Returns:
            Number of new messages actually saved
        """
        if not self._connection:
            raise RuntimeError("Database not connected")

        if not messages:
            return 0

        async with self._lock:
            # Get existing IDs in one query
            message_ids = [m.id for m in messages]
            placeholders = ",".join("?" * len(message_ids))
            async with self._connection.execute(
                f"SELECT id FROM messages WHERE id IN ({placeholders})",
                message_ids,
            ) as cursor:
                existing_ids = {row[0] async for row in cursor}

            # Filter out existing messages
            new_messages = [m for m in messages if m.id not in existing_ids]

            if not new_messages:
                logger.info(f"All {len(messages)} messages already exist, skipping")
                return 0

            # Insert only new messages
            data_list = [m.to_dict() for m in new_messages]
            await self._connection.executemany(
                """
                INSERT INTO messages
                (id, source_type, source_name, title, content, url,
                 stock_codes, publish_time, fetch_time, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        d["id"],
                        d["source_type"],
                        d["source_name"],
                        d["title"],
                        d["content"],
                        d["url"],
                        d["stock_codes"],
                        d["publish_time"],
                        d["fetch_time"],
                        d["raw_data"],
                    )
                    for d in data_list
                ],
            )
            await self._connection.commit()
            skipped = len(messages) - len(new_messages)
            logger.info(f"Saved {len(new_messages)} new messages, skipped {skipped} existing")
            return len(new_messages)

    async def query(
        self,
        source_type: str | None = None,
        source_name: str | None = None,
        stock_code: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Message]:
        """
        Query messages with filters.

        Args:
            source_type: Filter by source type (announcement/news/social)
            source_name: Filter by specific source
            stock_code: Filter by stock code (searches in stock_codes field)
            start_time: Filter messages published after this time
            end_time: Filter messages published before this time
            limit: Maximum number of results
            offset: Skip first N results

        Returns:
            List of Message objects matching the filters
        """
        if not self._connection:
            raise RuntimeError("Database not connected")

        conditions: list[str] = []
        params: list[str | int] = []

        if source_type:
            conditions.append("source_type = ?")
            params.append(source_type)
        if source_name:
            conditions.append("source_name = ?")
            params.append(source_name)
        if stock_code:
            conditions.append("stock_codes LIKE ?")
            params.append(f"%{stock_code}%")
        if start_time:
            conditions.append("publish_time >= ?")
            params.append(start_time.isoformat())
        if end_time:
            conditions.append("publish_time <= ?")
            params.append(end_time.isoformat())

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params.extend([limit, offset])

        query = f"""
            SELECT * FROM messages
            WHERE {where_clause}
            ORDER BY publish_time DESC
            LIMIT ? OFFSET ?
        """

        async with self._connection.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [Message.from_dict(dict(row)) for row in rows]

    async def stream_query(
        self,
        source_type: str | None = None,
        batch_size: int = 100,
    ) -> AsyncIterator[Message]:
        """
        Stream messages from database.

        Yields messages one by one, fetching in batches for efficiency.
        """
        offset = 0
        while True:
            messages = await self.query(
                source_type=source_type,
                limit=batch_size,
                offset=offset,
            )
            if not messages:
                break
            for message in messages:
                yield message
            offset += batch_size

    async def count(
        self,
        source_type: str | None = None,
        source_name: str | None = None,
    ) -> int:
        """Count messages matching filters."""
        if not self._connection:
            raise RuntimeError("Database not connected")

        conditions: list[str] = []
        params: list[str] = []

        if source_type:
            conditions.append("source_type = ?")
            params.append(source_type)
        if source_name:
            conditions.append("source_name = ?")
            params.append(source_name)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"SELECT COUNT(*) FROM messages WHERE {where_clause}"
        async with self._connection.execute(query, params) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def exists(self, message_id: str) -> bool:
        """Check if a message with given ID exists."""
        if not self._connection:
            raise RuntimeError("Database not connected")

        async with self._connection.execute(
            "SELECT 1 FROM messages WHERE id = ?", (message_id,)
        ) as cursor:
            return await cursor.fetchone() is not None

    async def get_existing_ids(
        self,
        source_name: str | None = None,
        limit: int = 10000,
    ) -> set[str]:
        """
        Get existing message IDs for cache initialization.

        Args:
            source_name: Filter by source name (optional)
            limit: Maximum number of IDs to return (for memory safety)

        Returns:
            Set of existing message IDs
        """
        if not self._connection:
            raise RuntimeError("Database not connected")

        if source_name:
            query = "SELECT id FROM messages WHERE source_name = ? ORDER BY publish_time DESC LIMIT ?"
            params = (source_name, limit)
        else:
            query = "SELECT id FROM messages ORDER BY publish_time DESC LIMIT ?"
            params = (limit,)

        async with self._connection.execute(query, params) as cursor:
            return {row[0] async for row in cursor}
