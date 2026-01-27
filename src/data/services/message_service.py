# === MODULE PURPOSE ===
# Main service for managing message collection from all sources.
# Orchestrates source lifecycle, message fetching, and database storage.

# === DEPENDENCIES ===
# - SourceRegistry: Manages registered message sources
# - MessageDatabase: Stores fetched messages
# - BaseMessageSource: Interface for all message sources

# === KEY CONCEPTS ===
# - Continuous polling: Each source runs in its own async task
# - Dynamic source management: Sources can be added/removed at runtime
# - Graceful shutdown: Proper cleanup on stop

import asyncio
import logging
from pathlib import Path
from typing import Callable

from src.data.database.message_db import MessageDatabase
from src.data.models.message import Message
from src.data.sources.base import BaseMessageSource
from src.data.sources.registry import SourceRegistry

logger = logging.getLogger(__name__)

# Type alias for message callbacks
MessageCallback = Callable[[Message], None]


class MessageService:
    """
    Main service for continuous message collection.

    Manages the lifecycle of all message sources and coordinates
    message fetching and storage.

    Data Flow:
        Sources -> fetch_messages() -> MessageService -> Database
                                                      -> Callbacks (optional)

    Lifecycle:
        1. Create service with database path
        2. Add sources via add_source()
        3. Call start() to begin collection
        4. Service runs until stop() is called

    Dynamic Source Management:
        Sources can be added or removed while the service is running.
        New sources start fetching immediately after registration.

    Usage:
        service = MessageService("data/messages.db")
        service.add_source(AnnouncementSource())
        service.add_source(NewsSource())

        await service.start()  # Runs until stopped
        # ... later ...
        await service.stop()
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self._registry = SourceRegistry()
        self._database: MessageDatabase | None = None
        self._running = False
        self._tasks: dict[str, asyncio.Task] = {}
        self._callbacks: list[MessageCallback] = []

        # Register callback for dynamic source changes
        self._registry.add_callback(self._on_source_change)

    @property
    def registry(self) -> SourceRegistry:
        """Access the source registry for advanced operations."""
        return self._registry

    @property
    def is_running(self) -> bool:
        """Check if the service is currently running."""
        return self._running

    def add_callback(self, callback: MessageCallback) -> None:
        """
        Add a callback to be invoked when new messages are received.

        Callbacks receive each message as it is fetched, before database storage.
        Useful for real-time processing, alerting, or forwarding.
        """
        self._callbacks.append(callback)

    def remove_callback(self, callback: MessageCallback) -> None:
        """Remove a previously added callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    async def add_source(self, source: BaseMessageSource) -> None:
        """
        Add a message source to the service.

        If the service is running, the source starts fetching immediately.

        Args:
            source: The message source to add
        """
        await self._registry.register(source)
        logger.info(f"Added source: {source.source_name}")

        # If service is running, start the source immediately
        if self._running:
            await self._start_source(source)

    async def remove_source(self, source_name: str) -> None:
        """
        Remove a message source from the service.

        Stops the source's fetch task and unregisters it.

        Args:
            source_name: Name of the source to remove
        """
        # Stop the task if running
        if source_name in self._tasks:
            self._tasks[source_name].cancel()
            try:
                await self._tasks[source_name]
            except asyncio.CancelledError:
                pass
            del self._tasks[source_name]

        # Unregister and stop the source
        source = await self._registry.unregister(source_name)
        if source:
            await source.stop()
            logger.info(f"Removed source: {source_name}")

    async def start(self) -> None:
        """
        Start the message service.

        This method:
            1. Connects to the database
            2. Starts all registered sources
            3. Runs until stop() is called

        Note: This is a blocking call. Use asyncio.create_task() to run
        in the background.
        """
        if self._running:
            logger.warning("Service is already running")
            return

        logger.info("Starting message service...")

        # Connect to database
        self._database = MessageDatabase(self.db_path)
        await self._database.connect()

        self._running = True

        # Start all registered sources
        sources = await self._registry.get_all()
        for source in sources:
            await self._start_source(source)

        logger.info(f"Message service started with {len(sources)} sources")

        # Keep running until stopped
        try:
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self._cleanup()

    async def stop(self) -> None:
        """
        Stop the message service gracefully.

        Stops all sources, cancels tasks, and closes database connection.
        """
        if not self._running:
            return

        logger.info("Stopping message service...")
        self._running = False

        # Stop all tasks
        for task in self._tasks.values():
            task.cancel()

        # Wait for all tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)

        self._tasks.clear()

    async def _cleanup(self) -> None:
        """Clean up resources on shutdown."""
        # Stop all sources
        sources = await self._registry.get_all()
        for source in sources:
            await source.stop()

        # Close database
        if self._database:
            await self._database.close()
            self._database = None

        logger.info("Message service stopped")

    async def _start_source(self, source: BaseMessageSource) -> None:
        """Start fetching from a single source."""
        # Load existing message IDs into source's deduplication cache
        if self._database:
            existing_ids = await self._database.get_existing_ids(
                source_name=source.source_name,
                limit=source.MAX_SEEN_IDS,
            )
            for msg_id in existing_ids:
                source.mark_seen(msg_id)
            logger.info(
                f"Loaded {len(existing_ids)} existing IDs for {source.source_name}"
            )

        await source.start()
        task = asyncio.create_task(
            self._fetch_loop(source),
            name=f"fetch_{source.source_name}",
        )
        self._tasks[source.source_name] = task

    async def _fetch_loop(self, source: BaseMessageSource) -> None:
        """
        Continuous fetch loop for a single source.

        Runs until cancelled, fetching messages at the source's interval.
        """
        logger.info(f"Started fetch loop for {source.source_name}")

        while self._running and source.is_running:
            try:
                # Fetch messages from source
                async for message in source.fetch_messages():
                    await self._process_message(message)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in fetch loop for {source.source_name}: {e}")

            # Wait before next fetch
            try:
                await asyncio.sleep(source.interval)
            except asyncio.CancelledError:
                break

        logger.info(f"Stopped fetch loop for {source.source_name}")

    async def _process_message(self, message: Message) -> bool:
        """
        Process a single message: save to database and invoke callbacks.

        Returns:
            True if message was new and saved, False if it already existed
        """
        # Save to database first (check if new)
        is_new = False
        if self._database:
            is_new = await self._database.save(message)

        # Only invoke callbacks for new messages
        if is_new:
            for callback in self._callbacks:
                try:
                    callback(message)
                except Exception as e:
                    logger.error(f"Callback error: {e}")

        return is_new

    def _on_source_change(self, source: BaseMessageSource, event: str) -> None:
        """Handle source registry changes."""
        logger.debug(f"Source {event}: {source.source_name}")

    async def get_stats(self) -> dict:
        """
        Get service statistics.

        Returns:
            Dictionary with service status, source count, and message counts.
        """
        stats = {
            "running": self._running,
            "sources": [],
            "total_messages": 0,
        }

        sources = await self._registry.get_all()
        for source in sources:
            source_stats = {
                "name": source.source_name,
                "type": source.source_type,
                "running": source.is_running,
                "interval": source.interval,
            }

            if self._database:
                count = await self._database.count(source_name=source.source_name)
                source_stats["message_count"] = count
                stats["total_messages"] += count

            stats["sources"].append(source_stats)

        return stats
