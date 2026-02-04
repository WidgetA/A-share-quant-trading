#!/usr/bin/env python3
"""
Migrate position data from local JSON file to PostgreSQL database.

Usage:
    # Set environment variables first (same as docker-compose.prod.yml)
    export DB_HOST=pc-uf691yl22372t36ej.pg.polardb.rds.aliyuncs.com
    export DB_PORT=5432
    export DB_USER=widgeta
    export DB_PASSWORD='!a9HtYsI-C!1PkCt!Q'
    export DB_NAME=_supabase

    # Run migration
    uv run python scripts/migrate_positions.py
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.trading.position_manager import PositionConfig, PositionManager
from src.trading.repository import create_trading_repository_from_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def migrate_positions(json_file: str = "data/position_state.json") -> None:
    """
    Migrate positions from JSON file to PostgreSQL.

    Args:
        json_file: Path to the JSON state file.
    """
    json_path = project_root / json_file

    if not json_path.exists():
        logger.error(f"JSON file not found: {json_path}")
        return

    # Load JSON data
    with open(json_path) as f:
        data = json.load(f)

    logger.info(f"Loaded position data from {json_path}")
    logger.info(f"Config: {data.get('config', {})}")

    # Count filled slots
    filled_slots = [s for s in data.get("slots", []) if s.get("state") == "filled"]
    logger.info(f"Found {len(filled_slots)} filled positions to migrate")

    if not filled_slots:
        logger.info("No positions to migrate")
        return

    # Create PositionManager with same config
    config_data = data.get("config", {})
    position_config = PositionConfig(
        total_capital=config_data.get("total_capital", 10_000_000),
        premarket_slots=config_data.get("premarket_slots", 3),
        intraday_slots=config_data.get("intraday_slots", 2),
    )
    manager = PositionManager(position_config)

    # Load from JSON file first
    manager.load_from_file(json_path)
    logger.info("Loaded positions into PositionManager")

    # Show current holdings
    holdings = manager.get_holdings()
    for slot in holdings:
        logger.info(f"  Slot {slot.slot_id} ({slot.slot_type.value}): {slot.sector_name or 'single'}")
        for h in slot.holdings:
            logger.info(f"    - {h.stock_code} {h.stock_name}: {h.quantity} @ {h.entry_price}")

    # Connect to database
    logger.info("Connecting to PostgreSQL...")
    try:
        repo = create_trading_repository_from_config()
        await repo.connect()
        logger.info("Connected to database")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return

    # Set repository and save
    manager.set_repository(repo)

    try:
        await manager.save_to_db()
        logger.info("Successfully migrated positions to database!")
    except Exception as e:
        logger.error(f"Failed to save to database: {e}")
        return
    finally:
        await repo.close()

    # Verify by loading back
    logger.info("Verifying migration...")
    manager2 = PositionManager(position_config)
    repo2 = create_trading_repository_from_config()
    await repo2.connect()
    manager2.set_repository(repo2)
    await manager2.load_from_db()

    holdings2 = manager2.get_holdings()
    logger.info(f"Verified: {len(holdings2)} positions in database")
    for slot in holdings2:
        logger.info(f"  Slot {slot.slot_id}: {len(slot.holdings)} holdings")

    await repo2.close()
    logger.info("Migration complete!")


if __name__ == "__main__":
    asyncio.run(migrate_positions())
