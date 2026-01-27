#!/usr/bin/env python3
# === MODULE PURPOSE ===
# Startup script for the message collection service.
# Loads configuration and starts continuous message fetching.

# === USAGE ===
# uv run python scripts/run_message_service.py
# uv run python scripts/run_message_service.py --config config/message-config.yaml

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.common.config import Config
from src.data.services.message_service import MessageService
from src.data.sources.akshare_announcement import AkshareAnnouncementSource
from src.data.sources.cls_news import CLSNewsSource
from src.data.sources.eastmoney_news import EastmoneyNewsSource
from src.data.sources.sina_news import SinaNewsSource


def setup_logging(config: Config) -> None:
    """Configure logging based on config."""
    level = config.get_str("logging.level", "INFO")
    format_str = config.get_str(
        "logging.format",
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_str,
    )


async def main(config_path: str) -> None:
    """Main entry point for the message service."""
    # Load configuration
    config = Config.load(config_path)
    setup_logging(config)

    logger = logging.getLogger(__name__)
    logger.info("Starting message service...")

    # Get database path
    db_path = project_root / config.get_str("message.database.path", "data/messages.db")

    # Create service
    service = MessageService(db_path)

    # Add sources based on configuration
    sources_config = config.get_dict("message.sources", {})

    # Akshare announcements (East Money data source)
    akshare_config = sources_config.get("akshare", sources_config.get("baostock", {}))
    if akshare_config.get("enabled", True):
        interval = akshare_config.get("interval", 300)
        category = akshare_config.get("category", "all")
        await service.add_source(AkshareAnnouncementSource(interval=float(interval), category=category))
        logger.info(f"Added Akshare announcement source (interval={interval}s, category={category})")

    # CLS news
    cls_config = sources_config.get("cls", {})
    if cls_config.get("enabled", True):
        interval = cls_config.get("interval", 30)
        symbol = cls_config.get("symbol", "全部")
        await service.add_source(CLSNewsSource(interval=float(interval), symbol=symbol))
        logger.info(f"Added CLS source (interval={interval}s, symbol={symbol})")

    # East Money news
    eastmoney_config = sources_config.get("eastmoney", {})
    if eastmoney_config.get("enabled", True):
        interval = eastmoney_config.get("interval", 60)
        await service.add_source(EastmoneyNewsSource(interval=float(interval)))
        logger.info(f"Added East Money source (interval={interval}s)")

    # Sina news
    sina_config = sources_config.get("sina", {})
    if sina_config.get("enabled", True):
        interval = sina_config.get("interval", 60)
        await service.add_source(SinaNewsSource(interval=float(interval)))
        logger.info(f"Added Sina source (interval={interval}s)")

    # Setup graceful shutdown
    shutdown_event = asyncio.Event()

    def signal_handler():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    # Register signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            signal.signal(sig, lambda s, f: signal_handler())

    # Start service in background
    service_task = asyncio.create_task(service.start())

    # Print status periodically
    async def print_stats():
        while not shutdown_event.is_set():
            await asyncio.sleep(60)
            if service.is_running:
                stats = await service.get_stats()
                logger.info(
                    f"Stats: {len(stats['sources'])} sources, "
                    f"{stats['total_messages']} total messages"
                )
                # Print per-source stats
                for source_stat in stats["sources"]:
                    logger.info(
                        f"  - {source_stat['name']}: {source_stat.get('message_count', 0)} messages"
                    )

    stats_task = asyncio.create_task(print_stats())

    # Wait for shutdown signal
    await shutdown_event.wait()

    # Cleanup
    logger.info("Shutting down...")
    stats_task.cancel()
    await service.stop()
    service_task.cancel()

    try:
        await service_task
    except asyncio.CancelledError:
        pass

    logger.info("Service stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the message collection service")
    parser.add_argument(
        "--config",
        "-c",
        default="config/message-config.yaml",
        help="Path to configuration file",
    )
    args = parser.parse_args()

    asyncio.run(main(args.config))
