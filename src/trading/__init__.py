# === MODULE PURPOSE ===
# Trading module for order execution, position management, and P&L tracking.
# Handles both paper trading and live trading with broker APIs.

# === KEY CONCEPTS ===
# - PositionManager: Manages capital allocation across slots
# - HoldingTracker: Tracks overnight holdings for next-day confirmation
# - TradingRepository: PostgreSQL persistence for trading data
# - OrderExecutor: Executes trading signals (planned)

# === PERSISTENCE ===
# Uses PostgreSQL with 'trading' schema for data isolation from messages.
# Tables: position_slots, stock_holdings, orders, transactions, overnight_holdings

from src.trading.holding_tracker import HoldingRecord, HoldingTracker
from src.trading.position_manager import (
    PositionConfig,
    PositionManager,
    PositionSlot,
    SlotState,
    SlotType,
    StockHolding,
    create_position_manager_with_db,
    create_position_manager_with_state,
)
from src.trading.repository import (
    OrderStatus,
    OrderType,
    TradingRepository,
    TradingRepositoryConfig,
    create_trading_repository_from_config,
)

__all__ = [
    # Position Management
    "PositionManager",
    "PositionConfig",
    "PositionSlot",
    "SlotState",
    "SlotType",
    "StockHolding",
    "create_position_manager_with_state",
    "create_position_manager_with_db",
    # Holding Tracker
    "HoldingTracker",
    "HoldingRecord",
    # Repository
    "TradingRepository",
    "TradingRepositoryConfig",
    "create_trading_repository_from_config",
    "OrderType",
    "OrderStatus",
]
