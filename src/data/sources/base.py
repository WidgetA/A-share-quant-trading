# === MODULE PURPOSE ===
# Base class for all message sources.
# Defines the interface that all concrete message sources must implement.

# === KEY CONCEPTS ===
# - Plugin architecture: Each source implements this interface
# - Async iterator: Sources yield messages as they are fetched
# - Lifecycle management: start/stop methods for resource management
# - Deduplication: Content-based ID generation prevents duplicate messages
# - Historical fetch: Support for batch fetching historical data

import asyncio
import hashlib
import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import datetime
from typing import AsyncIterator

from src.data.models.message import Message

logger = logging.getLogger(__name__)


class BaseMessageSource(ABC):
    """
    Abstract base class for message sources.

    All message sources (announcements, news, social media) must inherit
    from this class and implement the required methods.

    Lifecycle:
        1. __init__(): Configure the source
        2. start(): Initialize resources (connections, sessions)
        3. fetch_messages(): Yield messages as they are discovered
        4. stop(): Clean up resources

    Deduplication:
        Messages are deduplicated using content-based IDs generated from
        (source_name, title, publish_time). The _seen_ids cache prevents
        yielding the same message twice within a session.

    Historical Fetch:
        Override fetch_historical() to support batch fetching of historical
        data on first startup.

    Hot-Reload Support:
        Sources can be added/removed at runtime via SourceRegistry.
        The MessageService will handle starting/stopping sources dynamically.
    """

    # Maximum number of message IDs to cache for deduplication
    MAX_SEEN_IDS = 10000

    def __init__(self, interval: float = 60.0):
        """
        Initialize the message source.

        Args:
            interval: Polling interval in seconds between fetch cycles
        """
        self.interval = interval
        self._running = False
        self._task: asyncio.Task | None = None
        # LRU cache for deduplication (OrderedDict maintains insertion order)
        self._seen_ids: OrderedDict[str, bool] = OrderedDict()

    @property
    @abstractmethod
    def source_type(self) -> str:
        """
        Category of this source.

        Returns one of:
            - "announcement": Stock announcements and filings
            - "news": Financial news articles
            - "social": Social media posts and discussions
        """
        ...

    @property
    @abstractmethod
    def source_name(self) -> str:
        """
        Unique identifier for this specific source.

        Examples: "eastmoney", "sina_finance", "xueqiu", "cls", "akshare_eastmoney"
        """
        ...

    @abstractmethod
    async def fetch_messages(self) -> AsyncIterator[Message]:
        """
        Fetch messages from the source.

        This method should:
            1. Connect to the data source (API, website, etc.)
            2. Fetch new messages since last fetch
            3. Use is_duplicate() to filter already-seen messages
            4. Yield Message objects one by one

        Yields:
            Message objects as they are fetched (deduplicated)

        Note:
            This is an async generator. Use 'async for' to iterate:
                async for msg in source.fetch_messages():
                    process(msg)
        """
        # Make this an async generator
        if False:
            yield  # pragma: no cover

    async def fetch_historical(self, days: int = 7) -> AsyncIterator[Message]:
        """
        Fetch historical messages for initial data population.

        Override this method to implement batch historical data fetching.
        By default, this just calls fetch_messages() once.

        Args:
            days: Number of days of historical data to fetch

        Yields:
            Historical Message objects
        """
        logger.info(f"{self.source_name}: fetch_historical not implemented, using fetch_messages")
        async for message in self.fetch_messages():
            yield message

    def generate_message_id(self, title: str, publish_time: datetime) -> str:
        """
        Generate a unique message ID based on content.

        This creates a deterministic ID from the message content,
        allowing deduplication across sessions.

        Args:
            title: Message title
            publish_time: Message publish time

        Returns:
            A unique hash string for this message
        """
        # Create a unique string from source, title, and time
        content = f"{self.source_name}|{title}|{publish_time.isoformat()}"
        # Use SHA256 for consistent hashing
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:32]

    def is_duplicate(self, message_id: str) -> bool:
        """
        Check if a message has already been seen in this session.

        This uses an LRU cache to track recently seen message IDs.

        Args:
            message_id: The message ID to check

        Returns:
            True if the message was already seen, False otherwise
        """
        if message_id in self._seen_ids:
            return True

        # Add to cache
        self._seen_ids[message_id] = True

        # Evict oldest entries if cache is full
        while len(self._seen_ids) > self.MAX_SEEN_IDS:
            self._seen_ids.popitem(last=False)

        return False

    def mark_seen(self, message_id: str) -> None:
        """
        Mark a message ID as seen without checking for duplicates.

        Useful when loading existing messages from database on startup.

        Args:
            message_id: The message ID to mark as seen
        """
        self._seen_ids[message_id] = True
        while len(self._seen_ids) > self.MAX_SEEN_IDS:
            self._seen_ids.popitem(last=False)

    def clear_seen_cache(self) -> None:
        """Clear the seen message ID cache."""
        self._seen_ids.clear()

    async def start(self) -> None:
        """
        Initialize the source and prepare for fetching.

        Override this method to set up:
            - HTTP sessions
            - Database connections
            - Authentication
        """
        self._running = True
        logger.info(f"Started source: {self.source_name} ({self.source_type})")

    async def stop(self) -> None:
        """
        Stop the source and clean up resources.

        Override this method to close:
            - HTTP sessions
            - Database connections
            - Cancel pending requests
        """
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(f"Stopped source: {self.source_name}")

    @property
    def is_running(self) -> bool:
        """Check if the source is currently active."""
        return self._running

    @property
    def seen_count(self) -> int:
        """Number of message IDs in the deduplication cache."""
        return len(self._seen_ids)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} type={self.source_type} name={self.source_name}>"
