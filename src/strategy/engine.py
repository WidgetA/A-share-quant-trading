# === MODULE PURPOSE ===
# Strategy execution engine with hot-reload support.
# Manages strategy lifecycle, loading, and execution.

# === DEPENDENCIES ===
# - base: BaseStrategy interface
# - signals: TradingSignal data model
# - File system watching for hot-reload

# === KEY CONCEPTS ===
# - StrategyEngine: Central manager for all strategies
# - Hot-reload: Detect file changes and reload strategies
# - Validation: Verify strategies before activation

import asyncio
import hashlib
import importlib.util
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator, Callable

from src.strategy.base import BaseStrategy, StrategyConfig, StrategyContext
from src.strategy.signals import TradingSignal

logger = logging.getLogger(__name__)

# Type alias for strategy change callbacks
StrategyCallback = Callable[[str, str], None]  # (strategy_name, event)


@dataclass
class StrategyInfo:
    """Information about a loaded strategy."""

    strategy: BaseStrategy
    file_path: Path | None
    file_hash: str | None
    loaded_at: datetime
    reload_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "strategy_name": self.strategy.strategy_name,
            "file_path": str(self.file_path) if self.file_path else None,
            "file_hash": self.file_hash,
            "loaded_at": self.loaded_at.isoformat(),
            "reload_count": self.reload_count,
            "is_loaded": self.strategy.is_loaded,
        }


class StrategyEngine:
    """
    Strategy execution engine with hot-reload support.

    Manages the lifecycle of trading strategies including loading,
    unloading, and hot-reloading when files change.

    Features:
        - Load strategies from Python files
        - Hot-reload on file changes
        - Strategy validation before activation
        - Graceful strategy replacement
        - Rollback on reload failure

    Architecture:
        ┌─────────────────────────────────────────┐
        │            StrategyEngine               │
        ├─────────────────────────────────────────┤
        │  strategies: {name: StrategyInfo}       │
        │  file_watcher: asyncio.Task             │
        │  callbacks: list[StrategyCallback]      │
        └─────────────────────────────────────────┘
                         │
                         ▼
        ┌─────────────────────────────────────────┐
        │      BaseStrategy implementations       │
        │  (loaded from strategy_dir/*.py)        │
        └─────────────────────────────────────────┘

    Usage:
        engine = StrategyEngine(
            strategy_dir=Path("src/strategy/strategies"),
            hot_reload=True
        )
        await engine.start()

        # Get signals from all strategies
        async for signal in engine.generate_all_signals(context):
            await trading_module.process_signal(signal)

        await engine.stop()

    Hot-Reload Flow:
        1. File watcher detects change
        2. Calculate new file hash
        3. If hash changed, attempt reload:
           a. Load new strategy class
           b. Validate new strategy
           c. Call on_unload() on old strategy
           d. Swap strategy instance
           e. Call on_load() on new strategy
        4. If reload fails, keep old strategy
    """

    # File check interval for hot-reload
    FILE_CHECK_INTERVAL = 5.0  # seconds

    def __init__(
        self,
        strategy_dir: Path | str | None = None,
        hot_reload: bool = True,
        config: dict[str, StrategyConfig] | None = None,
    ):
        """
        Initialize strategy engine.

        Args:
            strategy_dir: Directory containing strategy files.
            hot_reload: Enable file watching for hot-reload.
            config: Strategy configurations keyed by strategy name.
        """
        self.strategy_dir = Path(strategy_dir) if strategy_dir else None
        self.hot_reload = hot_reload
        self._configs = config or {}

        self._strategies: dict[str, StrategyInfo] = {}
        self._callbacks: list[StrategyCallback] = []
        self._running = False
        self._watcher_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        """Check if engine is running."""
        return self._running

    @property
    def strategy_count(self) -> int:
        """Get number of loaded strategies."""
        return len(self._strategies)

    def add_callback(self, callback: StrategyCallback) -> None:
        """
        Add callback for strategy events.

        Callbacks receive (strategy_name, event) where event is:
        - "loaded": Strategy was loaded
        - "unloaded": Strategy was unloaded
        - "reloaded": Strategy was hot-reloaded

        Args:
            callback: Function to call on events.
        """
        self._callbacks.append(callback)

    def remove_callback(self, callback: StrategyCallback) -> None:
        """Remove a callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    async def start(self) -> None:
        """
        Start the strategy engine.

        Loads all strategies from the strategy directory and
        starts the file watcher if hot-reload is enabled.
        """
        if self._running:
            logger.warning("StrategyEngine already running")
            return

        logger.info("Starting StrategyEngine...")
        self._running = True

        # Load strategies from directory
        if self.strategy_dir and self.strategy_dir.exists():
            await self._load_all_strategies()

        # Start file watcher for hot-reload
        if self.hot_reload and self.strategy_dir:
            self._watcher_task = asyncio.create_task(
                self._file_watcher_loop(),
                name="strategy_file_watcher",
            )

        logger.info(
            f"StrategyEngine started with {len(self._strategies)} strategies"
        )

    async def stop(self) -> None:
        """
        Stop the strategy engine.

        Unloads all strategies and stops the file watcher.
        """
        if not self._running:
            return

        logger.info("Stopping StrategyEngine...")
        self._running = False

        # Stop file watcher
        if self._watcher_task:
            self._watcher_task.cancel()
            try:
                await self._watcher_task
            except asyncio.CancelledError:
                pass
            self._watcher_task = None

        # Unload all strategies
        for name in list(self._strategies.keys()):
            await self._unload_strategy(name)

        logger.info("StrategyEngine stopped")

    async def load_strategy_file(self, file_path: Path) -> str | None:
        """
        Load a strategy from a Python file.

        Args:
            file_path: Path to Python file containing strategy class.

        Returns:
            Strategy name if loaded successfully, None otherwise.
        """
        try:
            strategy_class = self._load_class_from_file(file_path)
            if strategy_class is None:
                return None

            # Get config if available
            temp_instance = strategy_class()
            strategy_name = temp_instance.strategy_name
            config = self._configs.get(strategy_name)

            # Create final instance with config
            strategy = strategy_class(config)

            # Validate
            errors = strategy.validate()
            if errors:
                logger.error(
                    f"Strategy {strategy_name} validation failed: {errors}"
                )
                return None

            # Calculate file hash
            file_hash = self._calculate_file_hash(file_path)

            # Load the strategy
            async with self._lock:
                # Unload existing if present
                if strategy_name in self._strategies:
                    await self._unload_strategy(strategy_name)

                await strategy.on_load()

                self._strategies[strategy_name] = StrategyInfo(
                    strategy=strategy,
                    file_path=file_path,
                    file_hash=file_hash,
                    loaded_at=datetime.now(),
                )

            self._notify_callbacks(strategy_name, "loaded")
            logger.info(f"Loaded strategy: {strategy_name} from {file_path}")
            return strategy_name

        except Exception as e:
            logger.error(f"Failed to load strategy from {file_path}: {e}")
            return None

    async def load_strategy_instance(
        self,
        strategy: BaseStrategy,
    ) -> str | None:
        """
        Load a strategy instance directly.

        Args:
            strategy: Strategy instance to load.

        Returns:
            Strategy name if loaded successfully.
        """
        try:
            strategy_name = strategy.strategy_name

            # Validate
            errors = strategy.validate()
            if errors:
                logger.error(
                    f"Strategy {strategy_name} validation failed: {errors}"
                )
                return None

            async with self._lock:
                # Unload existing if present
                if strategy_name in self._strategies:
                    await self._unload_strategy(strategy_name)

                await strategy.on_load()

                self._strategies[strategy_name] = StrategyInfo(
                    strategy=strategy,
                    file_path=None,
                    file_hash=None,
                    loaded_at=datetime.now(),
                )

            self._notify_callbacks(strategy_name, "loaded")
            logger.info(f"Loaded strategy instance: {strategy_name}")
            return strategy_name

        except Exception as e:
            logger.error(f"Failed to load strategy {strategy.strategy_name}: {e}")
            return None

    async def unload_strategy(self, strategy_name: str) -> bool:
        """
        Unload a strategy.

        Args:
            strategy_name: Name of strategy to unload.

        Returns:
            True if strategy was unloaded.
        """
        async with self._lock:
            return await self._unload_strategy(strategy_name)

    async def _unload_strategy(self, strategy_name: str) -> bool:
        """Internal unload without lock."""
        if strategy_name not in self._strategies:
            return False

        info = self._strategies[strategy_name]
        try:
            await info.strategy.on_unload()
        except Exception as e:
            logger.error(f"Error in on_unload for {strategy_name}: {e}")

        del self._strategies[strategy_name]
        self._notify_callbacks(strategy_name, "unloaded")
        logger.info(f"Unloaded strategy: {strategy_name}")
        return True

    async def reload_strategy(self, strategy_name: str) -> bool:
        """
        Force reload a strategy from its file.

        Args:
            strategy_name: Name of strategy to reload.

        Returns:
            True if reload was successful.
        """
        if strategy_name not in self._strategies:
            logger.warning(f"Strategy {strategy_name} not found for reload")
            return False

        info = self._strategies[strategy_name]
        if not info.file_path:
            logger.warning(f"Strategy {strategy_name} has no file path")
            return False

        return await self._reload_strategy_from_file(info)

    async def generate_all_signals(
        self,
        context: StrategyContext,
    ) -> AsyncIterator[TradingSignal]:
        """
        Generate signals from all loaded strategies.

        Args:
            context: Current market context.

        Yields:
            TradingSignal from each strategy.
        """
        for name, info in list(self._strategies.items()):
            if not info.strategy.is_loaded:
                continue

            if not info.strategy.config.enabled:
                continue

            try:
                async for signal in info.strategy.generate_signals(context):
                    yield signal
            except Exception as e:
                logger.error(f"Error generating signals from {name}: {e}")

    async def generate_signals(
        self,
        strategy_name: str,
        context: StrategyContext,
    ) -> AsyncIterator[TradingSignal]:
        """
        Generate signals from a specific strategy.

        Args:
            strategy_name: Name of strategy.
            context: Current market context.

        Yields:
            TradingSignal from the strategy.
        """
        if strategy_name not in self._strategies:
            return

        info = self._strategies[strategy_name]
        if not info.strategy.is_loaded or not info.strategy.config.enabled:
            return

        async for signal in info.strategy.generate_signals(context):
            yield signal

    def get_strategy(self, strategy_name: str) -> BaseStrategy | None:
        """Get a loaded strategy by name."""
        if strategy_name in self._strategies:
            return self._strategies[strategy_name].strategy
        return None

    def get_all_strategies(self) -> list[BaseStrategy]:
        """Get all loaded strategies."""
        return [info.strategy for info in self._strategies.values()]

    def get_stats(self) -> dict[str, Any]:
        """Get engine statistics."""
        return {
            "running": self._running,
            "strategy_count": len(self._strategies),
            "hot_reload_enabled": self.hot_reload,
            "strategy_dir": str(self.strategy_dir) if self.strategy_dir else None,
            "strategies": [
                info.to_dict() for info in self._strategies.values()
            ],
        }

    def get_state(self) -> dict[str, Any]:
        """Get state for checkpointing."""
        return {
            "strategies": {
                name: info.strategy.get_state()
                for name, info in self._strategies.items()
            },
        }

    async def _load_all_strategies(self) -> None:
        """Load all strategies from strategy directory."""
        if not self.strategy_dir:
            return

        for file_path in self.strategy_dir.glob("*.py"):
            if file_path.name.startswith("_"):
                continue
            await self.load_strategy_file(file_path)

    async def _file_watcher_loop(self) -> None:
        """File watcher loop for hot-reload."""
        logger.info("Started strategy file watcher")

        while self._running:
            try:
                await asyncio.sleep(self.FILE_CHECK_INTERVAL)

                if not self._running:
                    break

                await self._check_for_file_changes()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in file watcher: {e}")

        logger.info("Stopped strategy file watcher")

    async def _check_for_file_changes(self) -> None:
        """Check for strategy file changes and reload if needed."""
        for name, info in list(self._strategies.items()):
            if not info.file_path or not info.file_path.exists():
                continue

            current_hash = self._calculate_file_hash(info.file_path)
            if current_hash != info.file_hash:
                logger.info(f"Detected change in {info.file_path}")
                await self._reload_strategy_from_file(info)

    async def _reload_strategy_from_file(self, info: StrategyInfo) -> bool:
        """Reload a strategy from its file."""
        if not info.file_path:
            return False

        strategy_name = info.strategy.strategy_name
        old_strategy = info.strategy

        try:
            # Load new class
            strategy_class = self._load_class_from_file(info.file_path)
            if strategy_class is None:
                logger.error(f"Failed to reload {strategy_name}: no class found")
                return False

            # Create new instance with existing config
            config = self._configs.get(strategy_name) or old_strategy.config
            new_strategy = strategy_class(config)

            # Validate
            errors = new_strategy.validate()
            if errors:
                logger.error(f"Reload validation failed for {strategy_name}: {errors}")
                return False

            # Swap strategies
            async with self._lock:
                # Unload old
                await old_strategy.on_unload()

                # Load new
                await new_strategy.on_load()
                await new_strategy.on_reload()

                # Update info
                info.strategy = new_strategy
                info.file_hash = self._calculate_file_hash(info.file_path)
                info.loaded_at = datetime.now()
                info.reload_count += 1

            self._notify_callbacks(strategy_name, "reloaded")
            logger.info(
                f"Reloaded strategy {strategy_name} "
                f"(reload #{info.reload_count})"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to reload {strategy_name}: {e}")
            # Keep old strategy
            return False

    def _load_class_from_file(self, file_path: Path) -> type | None:
        """Load strategy class from Python file."""
        try:
            # Generate unique module name
            module_name = f"strategy_{file_path.stem}_{id(file_path)}"

            spec = importlib.util.spec_from_file_location(
                module_name, file_path
            )
            if spec is None or spec.loader is None:
                return None

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            # Find BaseStrategy subclass
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, BaseStrategy)
                    and attr is not BaseStrategy
                ):
                    return attr

            logger.warning(f"No strategy class found in {file_path}")
            return None

        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return None

    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file content."""
        content = file_path.read_bytes()
        return hashlib.sha256(content).hexdigest()[:16]

    def _notify_callbacks(self, strategy_name: str, event: str) -> None:
        """Notify all callbacks of a strategy event."""
        for callback in self._callbacks:
            try:
                callback(strategy_name, event)
            except Exception as e:
                logger.error(f"Callback error: {e}")
