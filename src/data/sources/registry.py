# === MODULE PURPOSE ===
# Registry for managing message sources.
# Allows dynamic registration and removal of sources at runtime.

# === KEY CONCEPTS ===
# - Dynamic source management: Add/remove sources without restart
# - Thread-safe operations: Uses locks for concurrent access
# - Observer pattern: Notifies listeners when sources change

import asyncio
import logging
from typing import Callable

from src.data.sources.base import BaseMessageSource

logger = logging.getLogger(__name__)

# Type alias for source change callbacks
SourceCallback = Callable[[BaseMessageSource, str], None]  # (source, event_type)


class SourceRegistry:
    """
    Registry for managing message sources.

    Provides dynamic source management with the ability to add/remove
    sources at runtime. Other components can register callbacks to be
    notified when sources are added or removed.

    Usage:
        registry = SourceRegistry()

        # Register a source
        source = AnnouncementSource()
        registry.register(source)

        # Get all sources
        for source in registry.get_all():
            async for msg in source.fetch_messages():
                process(msg)

        # Remove a source
        registry.unregister("announcement_source")

    Thread Safety:
        All operations use asyncio.Lock for safe concurrent access.
    """

    def __init__(self):
        self._sources: dict[str, BaseMessageSource] = {}
        self._lock = asyncio.Lock()
        self._callbacks: list[SourceCallback] = []

    async def register(self, source: BaseMessageSource) -> None:
        """
        Register a new message source.

        Args:
            source: The message source to register

        Raises:
            ValueError: If a source with the same name is already registered
        """
        async with self._lock:
            if source.source_name in self._sources:
                raise ValueError(f"Source already registered: {source.source_name}")

            self._sources[source.source_name] = source
            logger.info(f"Registered source: {source.source_name} ({source.source_type})")

            # Notify callbacks
            for callback in self._callbacks:
                try:
                    callback(source, "registered")
                except Exception as e:
                    logger.error(f"Callback error on register: {e}")

    async def unregister(self, source_name: str) -> BaseMessageSource | None:
        """
        Unregister a message source by name.

        Args:
            source_name: Name of the source to remove

        Returns:
            The removed source, or None if not found
        """
        async with self._lock:
            source = self._sources.pop(source_name, None)
            if source:
                logger.info(f"Unregistered source: {source_name}")

                # Notify callbacks
                for callback in self._callbacks:
                    try:
                        callback(source, "unregistered")
                    except Exception as e:
                        logger.error(f"Callback error on unregister: {e}")

            return source

    async def get(self, source_name: str) -> BaseMessageSource | None:
        """Get a source by name."""
        async with self._lock:
            return self._sources.get(source_name)

    async def get_all(self) -> list[BaseMessageSource]:
        """Get all registered sources."""
        async with self._lock:
            return list(self._sources.values())

    async def get_by_type(self, source_type: str) -> list[BaseMessageSource]:
        """Get all sources of a specific type."""
        async with self._lock:
            return [s for s in self._sources.values() if s.source_type == source_type]

    def add_callback(self, callback: SourceCallback) -> None:
        """
        Add a callback to be notified of source changes.

        Callback signature: (source: BaseMessageSource, event: str) -> None
        Events: "registered", "unregistered"
        """
        self._callbacks.append(callback)

    def remove_callback(self, callback: SourceCallback) -> None:
        """Remove a previously added callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    async def clear(self) -> None:
        """Remove all registered sources."""
        async with self._lock:
            sources = list(self._sources.values())
            self._sources.clear()

            # Notify callbacks for each removed source
            for source in sources:
                for callback in self._callbacks:
                    try:
                        callback(source, "unregistered")
                    except Exception as e:
                        logger.error(f"Callback error on clear: {e}")

            logger.info("Cleared all sources from registry")

    @property
    def count(self) -> int:
        """Number of registered sources."""
        return len(self._sources)

    def __len__(self) -> int:
        return len(self._sources)

    def __contains__(self, source_name: str) -> bool:
        return source_name in self._sources
