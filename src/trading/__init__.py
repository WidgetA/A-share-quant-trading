# === MODULE PURPOSE ===
# Trading module for order execution, position management, and P&L tracking.
# Handles both paper trading and live trading with broker APIs.

# === KEY CONCEPTS ===
# - PositionManager: Manages capital allocation across slots
# - HoldingTracker: Tracks overnight holdings for next-day confirmation
# - OrderExecutor: Executes trading signals (planned)

from src.trading.holding_tracker import HoldingRecord, HoldingTracker
from src.trading.position_manager import (
    PositionConfig,
    PositionManager,
    PositionSlot,
    SlotState,
    SlotType,
    StockHolding,
)

__all__ = [
    "PositionManager",
    "PositionConfig",
    "PositionSlot",
    "SlotState",
    "SlotType",
    "StockHolding",
    "HoldingTracker",
    "HoldingRecord",
]
