# === MODULE PURPOSE ===
# Base strategy interface for all trading strategies.
# Defines the contract that strategies must implement.

# === DEPENDENCIES ===
# - signals: TradingSignal data model
# - MessageReader: Platform layer for reading messages from PostgreSQL
# - Strategies receive market data and messages, produce signals

# === KEY CONCEPTS ===
# - BaseStrategy: Abstract base class for strategies
# - StrategyContext: Provides unified access to market data and messages
# - Hot-reload: Strategies can be updated at runtime
# - generate_signals(): Main entry point for signal generation

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, AsyncIterator

from src.strategy.signals import TradingSignal

if TYPE_CHECKING:
    from src.data.models.message import Message
    from src.data.readers.message_reader import MessageReader


@dataclass
class StrategyContext:
    """
    Context data provided to strategies for signal generation.

    Contains all information a strategy might need to make decisions.
    Messages are accessed via methods that query the platform-layer MessageReader.

    Architecture:
        MessageReader (platform) -> StrategyContext -> Strategy
        Strategies should NOT directly access MessageReader.

    Fields:
        timestamp: Current evaluation time
        market_data: Real-time quotes keyed by stock code
        positions: Current portfolio positions
        account: Account information (cash, equity, etc.)
        metadata: Additional context from coordinator
        _message_reader: Internal reference to platform MessageReader (do not use directly)

    Usage:
        # In strategy's generate_signals():
        messages = await context.get_messages_since(last_check_time)
        news = await context.get_messages(source_type="news", limit=100)
    """

    timestamp: datetime
    market_data: dict[str, dict[str, Any]] = field(default_factory=dict)
    positions: dict[str, dict[str, Any]] = field(default_factory=dict)
    account: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    _message_reader: "MessageReader | None" = field(default=None, repr=False)

    async def get_messages_since(
        self,
        since: datetime,
        source_type: str | None = None,
        limit: int = 1000,
    ) -> list["Message"]:
        """
        Get messages fetched since a given time with analysis results.

        Args:
            since: Fetch messages with fetch_time > since.
            source_type: Optional filter by source type (announcement/news/social).
            limit: Maximum number of messages to return.

        Returns:
            List of Message objects with analysis, ordered by fetch_time ascending.

        Raises:
            RuntimeError: If MessageReader is not available.
        """
        if not self._message_reader:
            raise RuntimeError("MessageReader not available in context")
        return await self._message_reader.get_messages_since(since, source_type, limit=limit)

    async def get_positive_messages_since(
        self,
        since: datetime,
        source_type: str | None = None,
        limit: int = 500,
    ) -> list["Message"]:
        """
        Get messages with positive sentiment (bullish/strong_bullish) since a given time.

        This is a convenience method for strategies that only care about positive signals.
        Analysis results come from external message_analysis table.

        Args:
            since: Fetch messages with fetch_time > since.
            source_type: Optional filter by source type.
            limit: Maximum number of messages to return.

        Returns:
            List of Message objects with positive sentiment.

        Raises:
            RuntimeError: If MessageReader is not available.
        """
        if not self._message_reader:
            raise RuntimeError("MessageReader not available in context")
        return await self._message_reader.get_positive_messages_since(since, source_type, limit)

    async def get_messages_in_range(
        self,
        start_time: datetime,
        end_time: datetime,
        source_type: str | None = None,
        limit: int = 1000,
    ) -> list["Message"]:
        """
        Get messages published within a time range with analysis results.

        Args:
            start_time: Start of publish_time range (inclusive).
            end_time: End of publish_time range (exclusive).
            source_type: Optional filter by source type.
            limit: Maximum number of messages to return.

        Returns:
            List of Message objects with analysis, ordered by publish_time ascending.
        """
        if not self._message_reader:
            raise RuntimeError("MessageReader not available in context")
        return await self._message_reader.get_messages_in_range(
            start_time, end_time, source_type, limit=limit
        )

    async def get_messages_by_stock(
        self,
        stock_code: str,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list["Message"]:
        """
        Get messages related to a specific stock.

        Args:
            stock_code: Stock code to search for.
            since: Optional filter by fetch_time.
            limit: Maximum number of messages to return.

        Returns:
            List of Message objects containing the stock code.
        """
        if not self._message_reader:
            raise RuntimeError("MessageReader not available in context")
        return await self._message_reader.get_messages_by_stock(stock_code, since, limit)

    @property
    def has_message_reader(self) -> bool:
        """Check if MessageReader is available."""
        return self._message_reader is not None and self._message_reader.is_connected


@dataclass
class StrategyConfig:
    """
    Strategy configuration loaded from YAML.

    Strategies can define their own parameters that are
    loaded from configuration files.
    """

    name: str
    enabled: bool = True
    parameters: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StrategyConfig":
        """Create from dictionary."""
        return cls(
            name=data.get("name", "unnamed"),
            enabled=data.get("enabled", True),
            parameters=data.get("parameters", {}),
        )


class BaseStrategy(ABC):
    """
    Abstract base class for all trading strategies.

    All strategies must inherit from this class and implement
    the required abstract methods. The strategy engine uses
    this interface to manage and execute strategies.

    Hot-Reload Support:
        Strategies can be reloaded at runtime without stopping
        the system. When reloaded:
        1. on_unload() is called on the old instance
        2. New strategy is instantiated with same config
        3. on_load() is called on the new instance
        4. New strategy starts receiving signals

    Lifecycle:
        1. __init__(config) - Initialize with configuration
        2. on_load() - Called when strategy becomes active
        3. generate_signals() - Called repeatedly during trading
        4. on_unload() - Called before strategy is replaced/stopped

    Example Implementation:
        class MyStrategy(BaseStrategy):
            @property
            def strategy_name(self) -> str:
                return "my_strategy"

            async def generate_signals(
                self,
                context: StrategyContext
            ) -> AsyncIterator[TradingSignal]:
                # Analyze context and yield signals
                if some_condition:
                    yield TradingSignal(
                        signal_type=SignalType.BUY,
                        stock_code="000001.SZ",
                        quantity=100,
                        strategy_name=self.strategy_name,
                        reason="Buy condition met"
                    )

    Thread Safety:
        Strategies should be stateless or use proper locking
        if maintaining internal state. The engine may call
        methods from different async contexts.
    """

    def __init__(self, config: StrategyConfig | None = None):
        """
        Initialize strategy with configuration.

        Args:
            config: Strategy configuration. If None, uses defaults.
        """
        self._config = config or StrategyConfig(name=self.strategy_name)
        self._is_loaded = False
        self._load_time: datetime | None = None

    @property
    @abstractmethod
    def strategy_name(self) -> str:
        """
        Unique identifier for this strategy.

        Returns:
            String name that uniquely identifies this strategy.
            Used for logging, configuration, and management.
        """
        ...

    @property
    def config(self) -> StrategyConfig:
        """Get strategy configuration."""
        return self._config

    @property
    def is_loaded(self) -> bool:
        """Check if strategy is currently loaded and active."""
        return self._is_loaded

    @property
    def load_time(self) -> datetime | None:
        """Get time when strategy was loaded."""
        return self._load_time

    def get_parameter(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration parameter.

        Args:
            key: Parameter name.
            default: Default value if parameter not found.

        Returns:
            Parameter value or default.
        """
        return self._config.parameters.get(key, default)

    @abstractmethod
    async def generate_signals(
        self,
        context: StrategyContext,
    ) -> AsyncIterator[TradingSignal]:
        """
        Generate trading signals based on current context.

        This is the main entry point for strategy logic. Called
        repeatedly during trading hours (and optionally outside).

        Args:
            context: Current market context with data and positions.

        Yields:
            TradingSignal objects for each trading decision.

        Note:
            - Should be idempotent for the same context
            - Should handle missing/incomplete data gracefully
            - Should not perform I/O directly (use context data)
            - Should complete quickly (< 1 second typically)
        """
        ...
        # Make this a generator (required for AsyncIterator)
        if False:
            yield  # pragma: no cover

    async def on_load(self) -> None:
        """
        Called when strategy becomes active.

        Override to perform initialization that requires async,
        such as loading models or connecting to services.

        Default implementation marks strategy as loaded.
        """
        self._is_loaded = True
        self._load_time = datetime.now()

    async def on_unload(self) -> None:
        """
        Called before strategy is replaced or stopped.

        Override to perform cleanup, such as saving state
        or releasing resources.

        Default implementation marks strategy as unloaded.
        """
        self._is_loaded = False

    async def on_reload(self) -> None:
        """
        Called when strategy file is modified and reloaded.

        Override to handle hot-reload scenarios, such as
        re-reading parameters or resetting state.

        Default implementation does nothing.
        """
        pass

    def validate(self) -> list[str]:
        """
        Validate strategy configuration.

        Override to add custom validation logic. Called before
        the strategy is activated.

        Returns:
            List of validation error messages. Empty if valid.
        """
        errors = []

        if not self.strategy_name:
            errors.append("strategy_name cannot be empty")

        return errors

    def get_state(self) -> dict[str, Any]:
        """
        Get strategy state for checkpointing.

        Override to include strategy-specific state that should
        be persisted for recovery.

        Returns:
            Dictionary of state data.
        """
        return {
            "strategy_name": self.strategy_name,
            "is_loaded": self._is_loaded,
            "load_time": self._load_time.isoformat() if self._load_time else None,
            "config": {
                "name": self._config.name,
                "enabled": self._config.enabled,
                "parameters": self._config.parameters,
            },
        }

    def __repr__(self) -> str:
        """String representation."""
        status = "loaded" if self._is_loaded else "unloaded"
        return f"<{self.__class__.__name__} '{self.strategy_name}' ({status})>"
