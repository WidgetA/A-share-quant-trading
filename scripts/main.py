#!/usr/bin/env python3
# === MODULE PURPOSE ===
# Main entry point for the A-Share Quantitative Trading System.
# Orchestrates all modules and provides 24/7 operation with recovery support.

# === USAGE ===
# uv run python scripts/main.py
# uv run python scripts/main.py --config config/main-config.yaml

# === KEY CONCEPTS ===
# - SystemManager: Central coordinator for all modules
# - 24/7 operation with state persistence
# - Automatic recovery from interruptions
# - Trading session-aware scheduling

import argparse
import asyncio
import logging
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.common.config import Config
from src.common.coordinator import Event, EventType, ModuleCoordinator
from src.common.scheduler import MarketSession, TradingScheduler
from src.common.state_manager import StateManager, SystemState
from src.data.models.message import Message
from src.data.services.message_service import MessageService
from src.data.sources.cls_news import CLSNewsSource
from src.data.sources.eastmoney_news import EastmoneyNewsSource
from src.data.sources.sina_news import SinaNewsSource
from src.strategy.base import StrategyContext
from src.strategy.engine import StrategyEngine

logger = logging.getLogger(__name__)


def setup_logging(config: Config) -> None:
    """Configure logging based on config."""
    level = config.get_str("logging.level", "INFO")
    format_str = config.get_str(
        "logging.format",
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Create logs directory if needed
    log_file = config.get_str("logging.file")
    if log_file:
        log_path = project_root / log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=getattr(logging, level.upper()),
            format=format_str,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(log_path, encoding="utf-8"),
            ],
        )
    else:
        logging.basicConfig(
            level=getattr(logging, level.upper()),
            format=format_str,
        )


class SystemManager:
    """
    Main system manager for the A-Share Quantitative Trading System.

    Responsibilities:
        1. Initialize and coordinate all modules
        2. Manage system lifecycle (start, stop, recovery)
        3. Handle trading session transitions
        4. Persist state for crash recovery
        5. Route events between modules

    Lifecycle:
        1. Load configuration
        2. Initialize StateManager, check for recovery
        3. Initialize Scheduler, Coordinator
        4. Start modules: MessageService -> StrategyEngine
        5. Main loop: schedule tasks based on session
        6. Handle shutdown gracefully

    Recovery Flow:
        1. On startup, StateManager checks last state
        2. If was RUNNING, system crashed - load checkpoints
        3. Resume modules from checkpoint data
        4. Clear checkpoints after successful recovery
    """

    # Checkpoint save interval (seconds)
    CHECKPOINT_INTERVAL = 60

    def __init__(self, config: Config):
        """
        Initialize system manager.

        Args:
            config: Loaded configuration.
        """
        self.config = config
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Core components
        self.state_manager: StateManager | None = None
        self.scheduler: TradingScheduler | None = None
        self.coordinator: ModuleCoordinator | None = None

        # Modules
        self.message_service: MessageService | None = None
        self.strategy_engine: StrategyEngine | None = None

        # Tasks
        self._tasks: list[asyncio.Task] = []

        # Statistics
        self._start_time: datetime | None = None
        self._last_checkpoint: datetime | None = None

    async def initialize(self) -> None:
        """Initialize all components and check for recovery."""
        logger.info("Initializing system...")

        # 1. Initialize state manager
        state_db_path = project_root / self.config.get_str(
            "system.state_database", "data/system_state.db"
        )
        checkpoint_max_age = self.config.get_int(
            "system.recovery.max_age_hours", 24
        )
        self.state_manager = StateManager(
            state_db_path,
            checkpoint_max_age_hours=checkpoint_max_age,
        )
        await self.state_manager.connect()

        # 2. Check for recovery
        needs_recovery = await self.state_manager.needs_recovery()
        if needs_recovery:
            logger.warning("System needs recovery from previous crash")
            await self._perform_recovery()

        # 3. Initialize scheduler
        self.scheduler = TradingScheduler()
        self.scheduler.add_session_callback(self._on_session_change)

        # 4. Initialize coordinator
        self.coordinator = ModuleCoordinator()
        await self.coordinator.start()

        # 5. Initialize modules
        await self._initialize_modules()

        logger.info("System initialization complete")

    async def _initialize_modules(self) -> None:
        """Initialize all system modules."""
        modules_config = self.config.get_dict("modules", {})

        # Data module (MessageService for news)
        data_config = modules_config.get("data", {})
        if data_config.get("enabled", True):
            await self._initialize_data_module(data_config)

        # Strategy module
        strategy_config = modules_config.get("strategy", {})
        if strategy_config.get("enabled", True):
            await self._initialize_strategy_module(strategy_config)

    async def _initialize_data_module(self, config: dict) -> None:
        """Initialize the data/message collection module."""
        # Load message service config
        msg_config_path = config.get("config_path", "config/message-config.yaml")
        msg_config = Config.load(project_root / msg_config_path)

        # Get database path
        db_path = project_root / msg_config.get_str(
            "message.database.path", "data/messages.db"
        )

        self.message_service = MessageService(db_path)

        # Add news sources (not announcements - only news as per user request)
        sources_config = msg_config.get_dict("message.sources", {})

        # CLS news
        cls_config = sources_config.get("cls", {})
        if cls_config.get("enabled", True):
            interval = cls_config.get("interval", 30)
            symbol = cls_config.get("symbol", "全部")
            await self.message_service.add_source(
                CLSNewsSource(interval=float(interval), symbol=symbol)
            )
            logger.info(f"Added CLS news source (interval={interval}s)")

        # East Money news
        em_config = sources_config.get("eastmoney", {})
        if em_config.get("enabled", True):
            interval = em_config.get("interval", 60)
            await self.message_service.add_source(
                EastmoneyNewsSource(interval=float(interval))
            )
            logger.info(f"Added East Money news source (interval={interval}s)")

        # Sina news
        sina_config = sources_config.get("sina", {})
        if sina_config.get("enabled", True):
            interval = sina_config.get("interval", 60)
            await self.message_service.add_source(
                SinaNewsSource(interval=float(interval))
            )
            logger.info(f"Added Sina news source (interval={interval}s)")

        # Add callback to publish events
        def on_message(message: Message) -> None:
            if self.coordinator:
                self.coordinator.publish_sync(
                    Event(
                        event_type=EventType.NEWS_RECEIVED,
                        source_module="data",
                        payload={"message": message.to_dict()},
                    )
                )

        self.message_service.add_callback(on_message)
        logger.info("Data module initialized")

    async def _initialize_strategy_module(self, config: dict) -> None:
        """Initialize the strategy execution module."""
        strategy_dir = config.get("strategy_dir", "src/strategy/strategies")
        hot_reload = config.get("hot_reload", True)

        self.strategy_engine = StrategyEngine(
            strategy_dir=project_root / strategy_dir,
            hot_reload=hot_reload,
        )

        # Add callback to publish events
        def on_strategy_change(name: str, event: str) -> None:
            if self.coordinator:
                event_type = {
                    "loaded": EventType.STRATEGY_LOADED,
                    "unloaded": EventType.STRATEGY_UNLOADED,
                    "reloaded": EventType.STRATEGY_RELOADED,
                }.get(event, EventType.STRATEGY_LOADED)

                self.coordinator.publish_sync(
                    Event(
                        event_type=event_type,
                        source_module="strategy",
                        payload={"strategy_name": name, "event": event},
                    )
                )

        self.strategy_engine.add_callback(on_strategy_change)
        logger.info("Strategy module initialized")

    async def _perform_recovery(self) -> None:
        """Perform system recovery from last state."""
        if not self.state_manager:
            return

        recovery_info = await self.state_manager.get_recovery_info()
        logger.info(f"Recovery info: {recovery_info}")

        # Get checkpoints for each module
        checkpoints = await self.state_manager.get_all_checkpoints()
        for checkpoint in checkpoints:
            logger.info(
                f"Restoring checkpoint for {checkpoint.module_name}: "
                f"{checkpoint.checkpoint_id}"
            )
            # Module-specific recovery logic would go here

        # Clear checkpoints after successful recovery
        await self.state_manager.clear_all_checkpoints()
        logger.info("Recovery complete, checkpoints cleared")

    async def run(self) -> None:
        """
        Main run loop.

        Runs until shutdown signal is received or error occurs.
        """
        self._running = True
        self._start_time = datetime.now()

        # Update state to RUNNING
        if self.state_manager:
            await self.state_manager.save_state(
                SystemState.RUNNING,
                {"start_time": self._start_time.isoformat()},
            )

        # Publish system starting event
        if self.coordinator:
            await self.coordinator.publish(
                Event(
                    event_type=EventType.SYSTEM_RUNNING,
                    source_module="system",
                    payload={"start_time": self._start_time.isoformat()},
                )
            )

        logger.info("System is running")

        # Start message service
        if self.message_service:
            self._tasks.append(
                asyncio.create_task(
                    self.message_service.start(),
                    name="message_service",
                )
            )

        # Start strategy engine
        if self.strategy_engine:
            await self.strategy_engine.start()

        # Start background tasks
        self._tasks.append(
            asyncio.create_task(
                self._checkpoint_loop(),
                name="checkpoint_loop",
            )
        )
        self._tasks.append(
            asyncio.create_task(
                self._session_monitor_loop(),
                name="session_monitor",
            )
        )
        self._tasks.append(
            asyncio.create_task(
                self._stats_loop(),
                name="stats_loop",
            )
        )

        # Wait for shutdown signal
        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            pass

    async def shutdown(self) -> None:
        """Graceful shutdown."""
        if not self._running:
            return

        logger.info("Initiating shutdown...")
        self._running = False

        # Update state to STOPPING
        if self.state_manager:
            await self.state_manager.save_state(SystemState.STOPPING)

        # Publish shutdown event
        if self.coordinator:
            await self.coordinator.publish(
                Event(
                    event_type=EventType.SYSTEM_STOPPING,
                    source_module="system",
                )
            )

        # Cancel all tasks
        for task in self._tasks:
            task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        # Stop modules in reverse order
        if self.strategy_engine:
            await self.strategy_engine.stop()

        if self.message_service:
            await self.message_service.stop()

        # Stop coordinator
        if self.coordinator:
            await self.coordinator.stop()

        # Update state to STOPPED
        if self.state_manager:
            await self.state_manager.save_state(SystemState.STOPPED)
            await self.state_manager.close()

        logger.info("Shutdown complete")

    async def _checkpoint_loop(self) -> None:
        """Periodic checkpoint saving."""
        interval = self.config.get_int("system.checkpoint_interval", 60)

        while self._running:
            try:
                await asyncio.sleep(interval)

                if not self._running:
                    break

                await self._save_checkpoint()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Checkpoint error: {e}")

    async def _save_checkpoint(self) -> None:
        """Save checkpoint for all modules."""
        if not self.state_manager:
            return

        # Save message service state
        if self.message_service:
            stats = await self.message_service.get_stats()
            await self.state_manager.save_checkpoint(
                "message_service",
                {
                    "total_messages": stats.get("total_messages", 0),
                    "sources": stats.get("sources", []),
                },
            )

        # Save strategy engine state
        if self.strategy_engine:
            state = self.strategy_engine.get_state()
            await self.state_manager.save_checkpoint("strategy_engine", state)

        self._last_checkpoint = datetime.now()

        if self.coordinator:
            self.coordinator.publish_sync(
                Event(
                    event_type=EventType.CHECKPOINT_SAVED,
                    source_module="system",
                    payload={"timestamp": self._last_checkpoint.isoformat()},
                )
            )

        logger.debug("Checkpoint saved")

    async def _session_monitor_loop(self) -> None:
        """Monitor trading session changes."""
        while self._running:
            try:
                await asyncio.sleep(60)  # Check every minute

                if not self._running:
                    break

                if self.scheduler:
                    self.scheduler.check_session_change()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Session monitor error: {e}")

    def _on_session_change(
        self,
        old_session: MarketSession,
        new_session: MarketSession,
    ) -> None:
        """Handle trading session change."""
        logger.info(f"Session changed: {old_session.value} -> {new_session.value}")

        if self.coordinator:
            self.coordinator.publish_sync(
                Event(
                    event_type=EventType.SESSION_CHANGED,
                    source_module="system",
                    payload={
                        "old_session": old_session.value,
                        "new_session": new_session.value,
                    },
                )
            )

    async def _stats_loop(self) -> None:
        """Periodic statistics logging."""
        while self._running:
            try:
                await asyncio.sleep(300)  # Every 5 minutes

                if not self._running:
                    break

                await self._log_stats()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Stats loop error: {e}")

    async def _log_stats(self) -> None:
        """Log system statistics."""
        stats = await self.get_stats()

        logger.info(
            f"System stats - "
            f"Uptime: {stats['uptime']}, "
            f"Session: {stats['session']['current_session']}, "
            f"Trading: {stats['session']['is_trading_hours']}"
        )

        if self.message_service:
            msg_stats = await self.message_service.get_stats()
            logger.info(
                f"Message stats - "
                f"Sources: {len(msg_stats['sources'])}, "
                f"Total messages: {msg_stats['total_messages']}"
            )

        if self.strategy_engine:
            engine_stats = self.strategy_engine.get_stats()
            logger.info(
                f"Strategy stats - "
                f"Loaded: {engine_stats['strategy_count']}"
            )

    async def get_stats(self) -> dict[str, Any]:
        """Get comprehensive system statistics."""
        now = datetime.now()
        uptime = str(now - self._start_time).split(".")[0] if self._start_time else "0:00:00"

        stats = {
            "running": self._running,
            "start_time": self._start_time.isoformat() if self._start_time else None,
            "uptime": uptime,
            "last_checkpoint": (
                self._last_checkpoint.isoformat()
                if self._last_checkpoint
                else None
            ),
            "session": {},
            "modules": {},
        }

        if self.scheduler:
            stats["session"] = self.scheduler.get_session_info()

        if self.message_service:
            stats["modules"]["message_service"] = await self.message_service.get_stats()

        if self.strategy_engine:
            stats["modules"]["strategy_engine"] = self.strategy_engine.get_stats()

        if self.coordinator:
            stats["modules"]["coordinator"] = self.coordinator.get_stats()

        return stats

    def request_shutdown(self) -> None:
        """Request system shutdown."""
        self._shutdown_event.set()


async def main(config_path: str) -> None:
    """Main entry point."""
    # Load configuration
    config = Config.load(config_path)
    setup_logging(config)

    logger.info("=" * 60)
    logger.info("A-Share Quantitative Trading System")
    logger.info("=" * 60)

    # Create system manager
    manager = SystemManager(config)

    # Setup signal handlers
    def signal_handler():
        logger.info("Shutdown signal received")
        manager.request_shutdown()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            signal.signal(sig, lambda s, f: signal_handler())

    try:
        # Initialize
        await manager.initialize()

        # Run
        await manager.run()

    except Exception as e:
        logger.error(f"System error: {e}", exc_info=True)
        raise
    finally:
        # Shutdown
        await manager.shutdown()

    logger.info("System terminated")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A-Share Quantitative Trading System"
    )
    parser.add_argument(
        "--config",
        "-c",
        default="config/main-config.yaml",
        help="Path to main configuration file",
    )
    args = parser.parse_args()

    asyncio.run(main(args.config))
