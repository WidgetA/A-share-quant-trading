#!/usr/bin/env python3
# === MODULE PURPOSE ===
# Main entry point for the A-Share Quantitative Trading System.
# Orchestrates all modules and provides 24/7 operation with recovery support.

# === USAGE ===
# uv run python scripts/main.py
# uv run python scripts/main.py --config config/main-config.yaml

# === KEY CONCEPTS ===
# - SystemManager: Central coordinator for all modules
# - MessageReader: Platform layer for reading messages from PostgreSQL
# - 24/7 operation with state persistence
# - Trading session-aware scheduling

# === ARCHITECTURE NOTE ===
# Message collection is handled by an external project that streams data into PostgreSQL.
# This system only READS messages from PostgreSQL via MessageReader.

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
from src.common.feishu_bot import FeishuBot
from src.common.scheduler import MarketSession, TradingScheduler
from src.common.state_manager import StateManager, SystemState, create_state_manager_from_config
from src.data.readers.message_reader import MessageReader, MessageReaderConfig
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

    Architecture:
        - Message collection is handled by an EXTERNAL project
        - This system reads messages from PostgreSQL via MessageReader
        - MessageReader is passed to StrategyContext for strategy access

    Lifecycle:
        1. Load configuration
        2. Initialize StateManager, check for recovery
        3. Initialize Scheduler, Coordinator
        4. Initialize MessageReader (connects to PostgreSQL)
        5. Start modules: StrategyEngine
        6. Main loop: schedule tasks based on session
        7. Handle shutdown gracefully

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

        # Platform layer components
        self.message_reader: MessageReader | None = None

        # Modules
        self.strategy_engine: StrategyEngine | None = None

        # Feishu alert bot
        self.feishu_bot: FeishuBot = FeishuBot()

        # Tasks
        self._tasks: list[asyncio.Task] = []

        # Statistics
        self._start_time: datetime | None = None
        self._last_checkpoint: datetime | None = None

    async def initialize(self) -> None:
        """Initialize all components and check for recovery."""
        logger.info("Initializing system...")

        # 1. Initialize state manager (uses PostgreSQL via database-config.yaml)
        self.state_manager = create_state_manager_from_config()
        await self.state_manager.connect()

        # 2. Check if recovery is needed
        needs_recovery = await self.state_manager.needs_recovery()
        if needs_recovery:
            logger.warning("System needs recovery from previous crash")

        # 3. Initialize scheduler
        self.scheduler = TradingScheduler()
        self.scheduler.add_session_callback(self._on_session_change)

        # 4. Initialize coordinator
        self.coordinator = ModuleCoordinator()
        await self.coordinator.start()

        # 5. Initialize MessageReader (platform layer)
        await self._initialize_message_reader()

        # 6. Initialize modules
        await self._initialize_modules()

        logger.info("System initialization complete")

    async def _initialize_message_reader(self) -> None:
        """Initialize the MessageReader for reading from external PostgreSQL."""
        import os

        # Load database config
        db_config_path = self.config.get_str("database.config_path", "config/database-config.yaml")
        try:
            db_config = Config.load(project_root / db_config_path)
            msg_db_config = db_config.get_dict("database.messages", {})
        except FileNotFoundError:
            logger.warning(f"Database config not found: {db_config_path}, using defaults")
            msg_db_config = {}

        # Build MessageReaderConfig
        password = msg_db_config.get("password", "")
        if password.startswith("${") and password.endswith("}"):
            env_var = password[2:-1]
            password = os.environ.get(env_var, "")

        reader_config = MessageReaderConfig(
            host=msg_db_config.get("host", "localhost"),
            port=msg_db_config.get("port", 5432),
            database=msg_db_config.get("database", "messages"),
            user=msg_db_config.get("user", "reader"),
            password=password,
            pool_min_size=msg_db_config.get("pool_min_size", 2),
            pool_max_size=msg_db_config.get("pool_max_size", 10),
            table_name=msg_db_config.get("table_name", "messages"),
        )

        self.message_reader = MessageReader(reader_config)

        try:
            await self.message_reader.connect()
            logger.info(
                f"MessageReader connected to PostgreSQL: "
                f"{reader_config.host}:{reader_config.port}/{reader_config.database}"
            )
        except Exception as e:
            logger.warning(
                f"Failed to connect MessageReader to PostgreSQL: {e}. "
                f"Strategies will not have access to messages."
            )
            self.message_reader = None

    async def _initialize_modules(self) -> None:
        """Initialize all system modules."""
        modules_config = self.config.get_dict("modules", {})

        # Strategy module
        strategy_config = modules_config.get("strategy", {})
        if strategy_config.get("enabled", True):
            await self._initialize_strategy_module(strategy_config)

    async def _initialize_strategy_module(self, config: dict) -> None:
        """Initialize the strategy execution module."""
        strategy_dir = config.get("strategy_dir", "src/strategy/strategies")
        hot_reload = config.get("hot_reload", True)

        # Load strategy configurations
        strategy_configs = {}
        strategy_config_path = config.get("config_path", "config/news-strategy-config.yaml")
        try:
            from src.strategy.base import StrategyConfig

            strategy_file = Config.load(project_root / strategy_config_path)
            news_cfg = strategy_file.get_dict("strategy.news_analysis", {})
            if news_cfg:
                pos = news_cfg.get("position", {})
                ana = news_cfg.get("analysis", {})
                flt = news_cfg.get("filter", {})
                itx = news_cfg.get("interaction", {})
                params = {
                    "total_capital": pos.get("total_capital", 10_000_000),
                    "premarket_slots": pos.get("premarket_slots", 3),
                    "intraday_slots": pos.get("intraday_slots", 2),
                    "min_confidence": ana.get("min_confidence", 0.7),
                    "signal_types": ana.get(
                        "signal_types", ["dividend", "earnings", "restructure"]
                    ),
                    "max_stocks_per_sector": ana.get("max_stocks_per_sector", 5),
                    "exclude_bse": flt.get("exclude_bse", True),
                    "exclude_chinext": flt.get("exclude_chinext", True),
                    "exclude_star": flt.get("exclude_star", False),
                    "premarket_timeout": itx.get("premarket_timeout", 300),
                    "intraday_timeout": itx.get("intraday_timeout", 60),
                    "morning_timeout": itx.get("morning_timeout", 300),
                }
                strategy_configs["news_analysis"] = StrategyConfig(
                    name="news_analysis",
                    enabled=news_cfg.get("enabled", True),
                    parameters=params,
                )
                logger.info(f"Loaded news_analysis config: total_capital={params['total_capital']}")
        except Exception as e:
            logger.warning(f"Failed to load strategy config: {e}")

        self.strategy_engine = StrategyEngine(
            strategy_dir=project_root / strategy_dir,
            hot_reload=hot_reload,
            config=strategy_configs,
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
        """
        Perform system recovery from last state.

        Logs recovery info from checkpoints.
        """
        if not self.state_manager:
            return

        recovery_info = await self.state_manager.get_recovery_info()
        logger.info(f"Recovery info: {recovery_info}")

        # Log available checkpoints for reference
        checkpoints = await self.state_manager.get_all_checkpoints()
        for checkpoint in checkpoints:
            logger.info(
                f"Found checkpoint for {checkpoint.module_name}: "
                f"{checkpoint.checkpoint_id} at {checkpoint.created_at}"
            )

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

        # Send Feishu startup notification
        if self.feishu_bot.is_configured():
            asyncio.create_task(self.feishu_bot.send_startup_notification())

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
        self._tasks.append(
            asyncio.create_task(
                self._strategy_loop(),
                name="strategy_loop",
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

        # Close MessageReader
        if self.message_reader:
            await self.message_reader.close()

        # Stop coordinator
        if self.coordinator:
            await self.coordinator.stop()

        # Update state to STOPPED
        if self.state_manager:
            await self.state_manager.save_state(SystemState.STOPPED)
            await self.state_manager.close()

        # Send Feishu shutdown notification
        if self.feishu_bot.is_configured():
            await self.feishu_bot.send_shutdown_notification()

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

    async def _strategy_loop(self) -> None:
        """
        Periodically run strategy signal generation.

        Creates StrategyContext with MessageReader access and passes
        it to the strategy engine for signal generation.
        """
        interval = self.config.get_int("strategy.loop_interval", 60)

        while self._running:
            try:
                await asyncio.sleep(interval)

                if not self._running:
                    break

                if not self.strategy_engine:
                    continue

                # Build strategy context with MessageReader
                context = self._build_strategy_context()

                # Generate signals from all strategies
                async for signal in self.strategy_engine.generate_all_signals(context):
                    logger.info(
                        f"Signal generated: {signal.signal_type.value} "
                        f"{signal.stock_code} qty={signal.quantity} "
                        f"reason={signal.reason[:50]}..."
                    )

                    # Publish signal event
                    if self.coordinator:
                        await self.coordinator.publish(
                            Event(
                                event_type=EventType.SIGNAL_GENERATED,
                                source_module="strategy",
                                payload={
                                    "signal_type": signal.signal_type.value,
                                    "stock_code": signal.stock_code,
                                    "quantity": signal.quantity,
                                    "price": signal.price,
                                    "strategy_name": signal.strategy_name,
                                    "reason": signal.reason,
                                },
                            )
                        )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Strategy loop error: {e}", exc_info=True)

    def _build_strategy_context(self) -> StrategyContext:
        """
        Build StrategyContext with all required data.

        Returns:
            StrategyContext with MessageReader, market data, and session info.
        """
        now = datetime.now()

        # Get current session
        session = MarketSession.CLOSED
        if self.scheduler:
            session = self.scheduler.get_current_session()

        # Build context with MessageReader
        context = StrategyContext(
            timestamp=now,
            market_data={},  # TODO: Add market data from iFinD
            positions={},  # TODO: Add from position manager
            account={},  # TODO: Add account info
            metadata={
                "session": session,
                "is_trading_hours": self.scheduler.is_trading_hours() if self.scheduler else False,
            },
            _message_reader=self.message_reader,
        )

        return context

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

        if self.message_reader and self.message_reader.is_connected:
            try:
                msg_count = await self.message_reader.get_message_count()
                logger.info(f"Message reader connected, total messages: {msg_count}")
            except Exception as e:
                logger.warning(f"Failed to get message count: {e}")

        if self.strategy_engine:
            engine_stats = self.strategy_engine.get_stats()
            logger.info(f"Strategy stats - Loaded: {engine_stats['strategy_count']}")

    async def get_stats(self) -> dict[str, Any]:
        """Get comprehensive system statistics."""
        now = datetime.now()
        uptime = str(now - self._start_time).split(".")[0] if self._start_time else "0:00:00"

        stats = {
            "running": self._running,
            "start_time": self._start_time.isoformat() if self._start_time else None,
            "uptime": uptime,
            "last_checkpoint": (
                self._last_checkpoint.isoformat() if self._last_checkpoint else None
            ),
            "session": {},
            "modules": {},
        }

        if self.scheduler:
            stats["session"] = self.scheduler.get_session_info()

        if self.message_reader:
            stats["modules"]["message_reader"] = {
                "connected": self.message_reader.is_connected,
            }

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
        # Send Feishu error alert
        if manager.feishu_bot.is_configured():
            await manager.feishu_bot.send_alert(
                "系统错误",
                f"A股交易系统发生严重错误:\n\n{type(e).__name__}: {e}",
            )
        raise
    finally:
        # Shutdown
        await manager.shutdown()

    logger.info("System terminated")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A-Share Quantitative Trading System")
    parser.add_argument(
        "--config",
        "-c",
        default="config/main-config.yaml",
        help="Path to main configuration file",
    )
    args = parser.parse_args()

    asyncio.run(main(args.config))
