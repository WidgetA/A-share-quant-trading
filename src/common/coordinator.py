# === MODULE PURPOSE ===
# Module coordinator for inter-module communication.
# Provides event-driven architecture for loose coupling.

# === DEPENDENCIES ===
# - Modules publish events through coordinator
# - Handlers subscribe to event types

# === KEY CONCEPTS ===
# - Event: Message passed between modules
# - EventType: Predefined event categories
# - Pub/Sub: Publishers emit, subscribers receive

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Coroutine

logger = logging.getLogger(__name__)


class EventType(Enum):
    """
    System event types for inter-module communication.

    Events flow between modules to coordinate actions without
    direct coupling between module implementations.
    """

    # Data module events
    NEWS_RECEIVED = "news_received"  # New message fetched
    MARKET_DATA_UPDATE = "market_data_update"  # Quote update
    LIMIT_UP_DETECTED = "limit_up_detected"  # Stock hit limit

    # Strategy module events
    SIGNAL_GENERATED = "signal_generated"  # Trading signal
    STRATEGY_LOADED = "strategy_loaded"  # Strategy activated
    STRATEGY_UNLOADED = "strategy_unloaded"  # Strategy deactivated
    STRATEGY_RELOADED = "strategy_reloaded"  # Strategy hot-reloaded

    # Trading module events
    ORDER_SUBMITTED = "order_submitted"  # Order sent to broker
    ORDER_FILLED = "order_filled"  # Order executed
    ORDER_CANCELLED = "order_cancelled"  # Order cancelled
    POSITION_CHANGED = "position_changed"  # Holdings updated

    # System events
    SESSION_CHANGED = "session_changed"  # Trading session changed
    SYSTEM_STARTING = "system_starting"  # System starting up
    SYSTEM_RUNNING = "system_running"  # System fully operational
    SYSTEM_STOPPING = "system_stopping"  # System shutting down
    CHECKPOINT_SAVED = "checkpoint_saved"  # State checkpoint saved
    ERROR_OCCURRED = "error_occurred"  # Module error


@dataclass
class Event:
    """
    Inter-module event message.

    Events carry information between modules through the
    coordinator. They are processed asynchronously.

    Fields:
        event_type: Category of event
        source_module: Module that published the event
        payload: Event-specific data
        timestamp: When event was created
        event_id: Unique identifier for tracing
    """

    event_type: EventType
    source_module: str
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    event_id: str = field(default_factory=lambda: "")

    def __post_init__(self):
        """Generate event ID if not provided."""
        if not self.event_id:
            ts = self.timestamp.strftime("%Y%m%d%H%M%S%f")
            self.event_id = f"{self.event_type.value}_{ts}"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "event_type": self.event_type.value,
            "source_module": self.source_module,
            "payload": self.payload,
            "timestamp": self.timestamp.isoformat(),
            "event_id": self.event_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Event":
        """Create event from dictionary."""
        return cls(
            event_type=EventType(data["event_type"]),
            source_module=data["source_module"],
            payload=data.get("payload", {}),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            event_id=data.get("event_id", ""),
        )


# Type aliases for handlers
SyncHandler = Callable[[Event], None]
AsyncHandler = Callable[[Event], Coroutine[Any, Any, None]]
EventHandler = SyncHandler | AsyncHandler


class ModuleCoordinator:
    """
    Coordinates communication between system modules.

    Provides a publish-subscribe event system for loose coupling
    between modules. Events are processed asynchronously.

    Architecture:
        ┌─────────────┐     publish()     ┌─────────────────┐
        │ Data Module │ ───────────────▶ │                  │
        └─────────────┘                   │                  │
                                          │  ModuleCoordinator  │
        ┌─────────────┐     publish()     │                  │
        │Strategy Mod │ ───────────────▶ │                  │
        └─────────────┘                   │   Event Queue    │
                                          │                  │
        ┌─────────────┐     subscribe()   │                  │
        │Trading Mod  │ ◀─────────────── │                  │
        └─────────────┘                   └─────────────────┘

    Features:
        - Async event processing
        - Multiple handlers per event type
        - Priority-based handler ordering
        - Event history for debugging
        - Error isolation (handler errors don't affect others)

    Usage:
        coordinator = ModuleCoordinator()

        # Subscribe to events
        async def on_news(event: Event):
            message = event.payload["message"]
            # Process news...

        coordinator.subscribe(EventType.NEWS_RECEIVED, on_news)

        # Publish events
        await coordinator.publish(Event(
            event_type=EventType.NEWS_RECEIVED,
            source_module="data",
            payload={"message": message}
        ))

        # Start processing
        await coordinator.start()

    Thread Safety:
        All operations are async-safe. The event queue ensures
        ordered processing while allowing concurrent publishing.
    """

    # Maximum events to keep in history
    MAX_HISTORY_SIZE = 1000

    def __init__(self, max_queue_size: int = 10000):
        """
        Initialize coordinator.

        Args:
            max_queue_size: Maximum events in queue before blocking.
        """
        self._handlers: dict[EventType, list[EventHandler]] = {}
        self._event_queue: asyncio.Queue[Event] = asyncio.Queue(
            maxsize=max_queue_size
        )
        self._running = False
        self._processor_task: asyncio.Task | None = None
        self._history: list[Event] = []
        self._stats: dict[str, int] = {
            "published": 0,
            "processed": 0,
            "errors": 0,
        }

    @property
    def is_running(self) -> bool:
        """Check if coordinator is running."""
        return self._running

    @property
    def queue_size(self) -> int:
        """Get current event queue size."""
        return self._event_queue.qsize()

    def subscribe(
        self,
        event_type: EventType,
        handler: EventHandler,
    ) -> None:
        """
        Subscribe to an event type.

        Args:
            event_type: Type of events to receive.
            handler: Function to call when event occurs.
                    Can be sync or async.
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []

        if handler not in self._handlers[event_type]:
            self._handlers[event_type].append(handler)
            logger.debug(
                f"Subscribed handler to {event_type.value}: {handler.__name__}"
            )

    def unsubscribe(
        self,
        event_type: EventType,
        handler: EventHandler,
    ) -> None:
        """
        Unsubscribe from an event type.

        Args:
            event_type: Type of events.
            handler: Handler to remove.
        """
        if event_type in self._handlers:
            if handler in self._handlers[event_type]:
                self._handlers[event_type].remove(handler)
                logger.debug(
                    f"Unsubscribed handler from {event_type.value}"
                )

    async def publish(self, event: Event) -> None:
        """
        Publish an event to all subscribers.

        The event is added to the queue and processed
        asynchronously by the event processor.

        Args:
            event: Event to publish.
        """
        await self._event_queue.put(event)
        self._stats["published"] += 1

        logger.debug(
            f"Published {event.event_type.value} from {event.source_module}"
        )

    def publish_sync(self, event: Event) -> None:
        """
        Publish event synchronously (non-blocking).

        Attempts to add event to queue without waiting.
        Drops event if queue is full.

        Args:
            event: Event to publish.
        """
        try:
            self._event_queue.put_nowait(event)
            self._stats["published"] += 1
        except asyncio.QueueFull:
            logger.warning(
                f"Event queue full, dropping {event.event_type.value}"
            )

    async def start(self) -> None:
        """
        Start the event processor.

        Begins processing events from the queue and
        dispatching to handlers.
        """
        if self._running:
            logger.warning("Coordinator already running")
            return

        self._running = True
        self._processor_task = asyncio.create_task(
            self._process_events(),
            name="event_processor",
        )
        logger.info("ModuleCoordinator started")

    async def stop(self) -> None:
        """
        Stop the event processor.

        Processes remaining events before stopping.
        """
        if not self._running:
            return

        logger.info("Stopping ModuleCoordinator...")
        self._running = False

        # Process remaining events
        while not self._event_queue.empty():
            try:
                event = self._event_queue.get_nowait()
                await self._dispatch_event(event)
            except asyncio.QueueEmpty:
                break

        # Cancel processor task
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
            self._processor_task = None

        logger.info("ModuleCoordinator stopped")

    async def _process_events(self) -> None:
        """Event processing loop."""
        while self._running:
            try:
                # Wait for event with timeout
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0,
                )
                await self._dispatch_event(event)

            except asyncio.TimeoutError:
                # Normal timeout, continue loop
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in event processor: {e}")
                self._stats["errors"] += 1

    async def _dispatch_event(self, event: Event) -> None:
        """Dispatch event to all handlers."""
        handlers = self._handlers.get(event.event_type, [])

        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                logger.error(
                    f"Handler error for {event.event_type.value}: {e}"
                )
                self._stats["errors"] += 1

        self._stats["processed"] += 1

        # Add to history
        self._history.append(event)
        if len(self._history) > self.MAX_HISTORY_SIZE:
            self._history = self._history[-self.MAX_HISTORY_SIZE:]

    def get_history(
        self,
        event_type: EventType | None = None,
        limit: int = 100,
    ) -> list[Event]:
        """
        Get recent event history.

        Args:
            event_type: Filter by event type (optional).
            limit: Maximum events to return.

        Returns:
            List of recent events.
        """
        events = self._history
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        return events[-limit:]

    def get_stats(self) -> dict[str, Any]:
        """Get coordinator statistics."""
        return {
            "running": self._running,
            "queue_size": self._event_queue.qsize(),
            "handler_count": sum(
                len(handlers) for handlers in self._handlers.values()
            ),
            "subscriptions": {
                et.value: len(handlers)
                for et, handlers in self._handlers.items()
            },
            **self._stats,
        }

    def clear_history(self) -> None:
        """Clear event history."""
        self._history.clear()

    async def wait_for_event(
        self,
        event_type: EventType,
        timeout: float | None = None,
    ) -> Event | None:
        """
        Wait for a specific event type.

        Useful for synchronization between modules.

        Args:
            event_type: Type of event to wait for.
            timeout: Maximum wait time in seconds.

        Returns:
            The event if received, None if timeout.
        """
        received_event: Event | None = None
        event_received = asyncio.Event()

        def handler(event: Event) -> None:
            nonlocal received_event
            received_event = event
            event_received.set()

        self.subscribe(event_type, handler)
        try:
            await asyncio.wait_for(event_received.wait(), timeout=timeout)
            return received_event
        except asyncio.TimeoutError:
            return None
        finally:
            self.unsubscribe(event_type, handler)
