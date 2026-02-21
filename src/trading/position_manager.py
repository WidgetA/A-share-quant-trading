# === MODULE PURPOSE ===
# Position and capital management for the news-driven strategy.
# Implements slot-based position sizing with premarket/intraday allocation.

# === KEY CONCEPTS ===
# - Slot: A position allocation unit (20% of total capital)
# - 5 slots total: 3 for premarket signals, 2 for intraday signals
# - Each slot can hold one position at a time
# - State machine: EMPTY -> PENDING_BUY -> FILLED -> PENDING_SELL -> EMPTY

# === PERSISTENCE ===
# - Primary: PostgreSQL (trading schema) for production
# - Fallback: JSON file for local development without DB

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.trading.repository import TradingRepository

logger = logging.getLogger(__name__)

# Default state file path (fallback when no DB)
DEFAULT_STATE_FILE = Path("data/position_state.json")


class SlotState(Enum):
    """State of a position slot."""

    EMPTY = "empty"  # No position
    PENDING_BUY = "pending_buy"  # Waiting for buy order to fill
    FILLED = "filled"  # Position held
    PENDING_SELL = "pending_sell"  # Waiting for sell order to fill


class SlotType(Enum):
    """Type of position slot."""

    PREMARKET = "premarket"  # Reserved for premarket signals
    INTRADAY = "intraday"  # Reserved for intraday signals


@dataclass
class StockHolding:
    """
    Individual stock holding within a position slot.

    For single stock positions, there's one holding.
    For sector buying, there are multiple holdings sharing the slot capital.
    """

    stock_code: str
    stock_name: str | None = None
    quantity: int = 0
    entry_price: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for persistence."""
        return {
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "quantity": self.quantity,
            "entry_price": self.entry_price,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StockHolding":
        """Create from dictionary."""
        return cls(
            stock_code=data["stock_code"],
            stock_name=data.get("stock_name"),
            quantity=data.get("quantity", 0),
            entry_price=data.get("entry_price"),
        )


@dataclass
class PositionSlot:
    """
    A position slot representing a capital allocation unit.

    Each slot can hold one or more stocks (for sector buying).
    - Single stock: One holding with full slot capital
    - Sector buying: Multiple holdings sharing slot capital equally

    State Machine:
        EMPTY -> PENDING_BUY (allocate) -> FILLED (fill) ->
        PENDING_SELL (mark_pending_sell) -> EMPTY (release)
    """

    slot_id: int
    slot_type: SlotType
    state: SlotState = SlotState.EMPTY

    # Holdings: supports single stock or sector (multiple stocks)
    holdings: list[StockHolding] = field(default_factory=list)

    # Entry metadata
    entry_time: datetime | None = None
    entry_reason: str = ""
    sector_name: str | None = None  # Set when sector buying

    # Order tracking
    pending_order_id: str | None = None

    # For sell tracking
    exit_price: float | None = None
    exit_time: datetime | None = None

    # Backward compatibility properties
    @property
    def stock_code(self) -> str | None:
        """Get first stock code (for single stock positions)."""
        return self.holdings[0].stock_code if self.holdings else None

    @property
    def stock_name(self) -> str | None:
        """Get first stock name (for single stock positions)."""
        return self.holdings[0].stock_name if self.holdings else None

    @property
    def quantity(self) -> int:
        """Get total quantity across all holdings."""
        return sum(h.quantity for h in self.holdings)

    @property
    def entry_price(self) -> float | None:
        """Get average entry price across all holdings."""
        if not self.holdings:
            return None
        total_value = sum((h.entry_price or 0) * h.quantity for h in self.holdings)
        total_qty = sum(h.quantity for h in self.holdings)
        return total_value / total_qty if total_qty > 0 else None

    def is_available(self) -> bool:
        """Check if slot is available for new position."""
        return self.state == SlotState.EMPTY

    def is_filled(self) -> bool:
        """Check if slot has a filled position."""
        return self.state == SlotState.FILLED

    def is_sector_position(self) -> bool:
        """Check if this is a sector position (multiple stocks)."""
        return len(self.holdings) > 1

    def reset(self) -> None:
        """Reset slot to empty state."""
        self.state = SlotState.EMPTY
        self.holdings = []
        self.entry_time = None
        self.entry_reason = ""
        self.sector_name = None
        self.pending_order_id = None
        self.exit_price = None
        self.exit_time = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for persistence."""
        # Include both new holdings format and legacy fields for compatibility
        result = {
            "slot_id": self.slot_id,
            "slot_type": self.slot_type.value,
            "state": self.state.value,
            "holdings": [h.to_dict() for h in self.holdings],
            "entry_time": self.entry_time.isoformat() if self.entry_time else None,
            "entry_reason": self.entry_reason,
            "sector_name": self.sector_name,
            "pending_order_id": self.pending_order_id,
            "exit_price": self.exit_price,
            "exit_time": self.exit_time.isoformat() if self.exit_time else None,
            # Legacy fields for backward compatibility
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "quantity": self.quantity,
            "entry_price": self.entry_price,
        }
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PositionSlot":
        """Create from dictionary."""
        slot = cls(
            slot_id=data["slot_id"],
            slot_type=SlotType(data["slot_type"]),
            state=SlotState(data.get("state", "empty")),
        )

        # Load holdings (new format) or legacy single stock format
        if "holdings" in data and data["holdings"]:
            slot.holdings = [StockHolding.from_dict(h) for h in data["holdings"]]
        elif data.get("stock_code"):
            # Legacy format: convert to single holding
            slot.holdings = [
                StockHolding(
                    stock_code=data["stock_code"],
                    stock_name=data.get("stock_name"),
                    quantity=data.get("quantity", 0),
                    entry_price=data.get("entry_price"),
                )
            ]

        slot.entry_time = (
            datetime.fromisoformat(data["entry_time"]) if data.get("entry_time") else None
        )
        slot.entry_reason = data.get("entry_reason", "")
        slot.sector_name = data.get("sector_name")
        slot.pending_order_id = data.get("pending_order_id")
        slot.exit_price = data.get("exit_price")
        slot.exit_time = (
            datetime.fromisoformat(data["exit_time"]) if data.get("exit_time") else None
        )
        return slot


@dataclass
class PositionConfig:
    """Configuration for position management."""

    total_capital: float = 10_000_000.0  # 1000万初始资金
    num_slots: int = 5  # 5个仓位
    premarket_slots: int = 3  # 3个盘前仓位
    intraday_slots: int = 2  # 2个盘中仓位
    min_order_amount: float = 10000.0  # 最小下单金额
    lot_size: int = 100  # A股最小交易单位

    @property
    def slot_capital(self) -> float:
        """Capital per slot (20% of total)."""
        return self.total_capital / self.num_slots


class PositionManager:
    """
    Manages capital allocation and position slots.

    Rules:
        - 5 slots total, each 20% of capital (2,000,000 per slot)
        - 3 slots reserved for premarket signals
        - 2 slots reserved for intraday signals
        - Hold overnight only

    Persistence:
        - PostgreSQL (trading schema): set repository via set_repository()
        - JSON file fallback: use save_to_file() / load_from_file()

    Usage:
        manager = PositionManager(PositionConfig(total_capital=10_000_000))

        # With PostgreSQL (recommended)
        repo = TradingRepository(config)
        await repo.connect()
        manager.set_repository(repo)
        await manager.load_from_db()

        # Allocate a premarket slot
        slot = manager.get_available_slot(SlotType.PREMARKET)
        if slot:
            quantity = manager.allocate_slot(
                slot=slot,
                stock_code="000001",
                price=15.50,
                reason="分红利好"
            )
            print(f"Buy {quantity} shares")

        # Get all filled positions
        holdings = manager.get_holdings()

        # Release a slot after selling
        manager.release_slot(slot.slot_id)
    """

    def __init__(self, config: PositionConfig | None = None):
        """
        Initialize position manager.

        Args:
            config: Position configuration. Uses defaults if None.
        """
        self._config = config or PositionConfig()
        self._slots: list[PositionSlot] = []
        self._repository: TradingRepository | None = None
        self._initialize_slots()

    def set_repository(self, repository: TradingRepository) -> None:
        """
        Set the database repository for persistence.

        Args:
            repository: Connected TradingRepository instance.
        """
        self._repository = repository
        logger.info("PositionManager: Using PostgreSQL for persistence")

    def _initialize_slots(self) -> None:
        """Initialize position slots based on configuration."""
        self._slots = []

        # Create premarket slots
        for i in range(self._config.premarket_slots):
            self._slots.append(PositionSlot(slot_id=i, slot_type=SlotType.PREMARKET))

        # Create intraday slots
        for i in range(self._config.intraday_slots):
            self._slots.append(
                PositionSlot(
                    slot_id=self._config.premarket_slots + i,
                    slot_type=SlotType.INTRADAY,
                )
            )

    def get_available_slot(self, slot_type: str | SlotType) -> PositionSlot | None:
        """
        Get an available slot for the given type.

        Args:
            slot_type: "premarket" or "intraday" (or SlotType enum).

        Returns:
            Available PositionSlot or None if no slots available.
        """
        if isinstance(slot_type, str):
            slot_type = SlotType(slot_type)

        for slot in self._slots:
            if slot.slot_type == slot_type and slot.is_available():
                return slot

        return None

    def get_available_slots(self, slot_type: str | SlotType) -> list[PositionSlot]:
        """
        Get all available slots for the given type.

        Args:
            slot_type: "premarket" or "intraday" (or SlotType enum).

        Returns:
            List of available PositionSlots.
        """
        if isinstance(slot_type, str):
            slot_type = SlotType(slot_type)

        return [slot for slot in self._slots if slot.slot_type == slot_type and slot.is_available()]

    def get_slot(self, slot_id: int) -> PositionSlot | None:
        """
        Get a slot by its ID.

        Args:
            slot_id: The slot ID.

        Returns:
            PositionSlot or None if not found.
        """
        for slot in self._slots:
            if slot.slot_id == slot_id:
                return slot
        return None

    def reserve_slot(self, slot_id: int) -> None:
        """
        Reserve a slot for pending order.

        This marks a slot as "reserved" so it won't be allocated
        to other orders while waiting for execution. Used during
        premarket analysis when actual execution happens later
        at morning auction.

        Args:
            slot_id: The slot ID to reserve.

        Raises:
            ValueError: If slot not found or already occupied.
        """
        slot = self.get_slot(slot_id)
        if not slot:
            raise ValueError(f"Slot {slot_id} not found")

        if not slot.is_available():
            raise ValueError(f"Slot {slot_id} is not available (state: {slot.state})")

        # Mark as pending buy to prevent other allocations
        slot.state = SlotState.PENDING_BUY

    def allocate_slot(
        self,
        slot: PositionSlot,
        stock_code: str,
        price: float,
        reason: str,
        stock_name: str | None = None,
        order_id: str | None = None,
    ) -> int:
        """
        Allocate a slot for a single stock position.

        Args:
            slot: The slot to allocate.
            stock_code: Stock code to buy.
            price: Expected buy price.
            reason: Reason for the trade.
            stock_name: Stock name (optional).
            order_id: Order ID for tracking (optional).

        Returns:
            Number of shares to buy based on slot capital.

        Raises:
            ValueError: If slot is not available.
        """
        if not slot.is_available():
            raise ValueError(f"Slot {slot.slot_id} is not available")

        # Calculate quantity
        quantity = self.get_quantity_for_price(price)

        if quantity <= 0:
            raise ValueError(f"Cannot calculate valid quantity for price {price}")

        # Update slot with single holding
        slot.state = SlotState.PENDING_BUY
        slot.holdings = [
            StockHolding(
                stock_code=stock_code,
                stock_name=stock_name,
                quantity=quantity,
                entry_price=price,
            )
        ]
        slot.entry_time = datetime.now()
        slot.entry_reason = reason
        slot.sector_name = None
        slot.pending_order_id = order_id

        logger.info(
            f"Allocated slot {slot.slot_id} for {stock_code}: {quantity} shares @ {price:.2f}"
        )

        return quantity

    def allocate_slot_sector(
        self,
        slot: PositionSlot,
        stocks: list[tuple[str, str | None, float]],
        reason: str,
        sector_name: str,
        order_id: str | None = None,
    ) -> list[StockHolding]:
        """
        Allocate a slot for sector buying (multiple stocks).

        Slot capital is divided equally among all stocks in the sector.

        Args:
            slot: The slot to allocate.
            stocks: List of (stock_code, stock_name, price) tuples.
            reason: Reason for the trade.
            sector_name: Name of the sector.
            order_id: Order ID for tracking (optional).

        Returns:
            List of StockHolding with calculated quantities.

        Raises:
            ValueError: If slot is not available or stocks list is empty.
        """
        if not slot.is_available():
            raise ValueError(f"Slot {slot.slot_id} is not available")

        if not stocks:
            raise ValueError("Stocks list cannot be empty for sector buying")

        # Calculate capital per stock
        capital_per_stock = self._config.slot_capital / len(stocks)

        holdings = []
        for stock_code, stock_name, price in stocks:
            if price <= 0:
                logger.warning(f"Skipping {stock_code}: invalid price {price}")
                continue

            # Calculate quantity for this stock's share of capital
            raw_qty = capital_per_stock / price
            quantity = int(raw_qty / self._config.lot_size) * self._config.lot_size

            if quantity * price < self._config.min_order_amount:
                logger.warning(f"Skipping {stock_code}: quantity {quantity} below min order amount")
                continue

            holdings.append(
                StockHolding(
                    stock_code=stock_code,
                    stock_name=stock_name,
                    quantity=quantity,
                    entry_price=price,
                )
            )

        if not holdings:
            raise ValueError("No valid stocks for sector buying after filtering")

        # Update slot
        slot.state = SlotState.PENDING_BUY
        slot.holdings = holdings
        slot.entry_time = datetime.now()
        slot.entry_reason = reason
        slot.sector_name = sector_name
        slot.pending_order_id = order_id

        logger.info(
            f"Allocated slot {slot.slot_id} for sector '{sector_name}': {len(holdings)} stocks"
        )
        for h in holdings:
            logger.info(f"  {h.stock_code} {h.stock_name}: {h.quantity} @ {h.entry_price:.2f}")

        return holdings

    def fill_slot(
        self,
        slot_id: int,
        fill_prices: dict[str, float] | float | None = None,
    ) -> None:
        """
        Mark a slot's buy order as filled.

        Args:
            slot_id: Slot ID to update.
            fill_prices: Actual fill prices. Can be:
                - None: uses entry_price for all holdings
                - float: applies to single stock position
                - dict[stock_code, price]: per-stock fill prices for sector buying
        """
        slot = self.get_slot(slot_id)
        if not slot:
            raise ValueError(f"Slot {slot_id} not found")

        if slot.state != SlotState.PENDING_BUY:
            raise ValueError(f"Slot {slot_id} is not pending buy")

        slot.state = SlotState.FILLED

        # Update fill prices if provided
        if fill_prices is not None:
            if isinstance(fill_prices, dict):
                for holding in slot.holdings:
                    if holding.stock_code in fill_prices:
                        holding.entry_price = fill_prices[holding.stock_code]
            elif len(slot.holdings) == 1:
                slot.holdings[0].entry_price = fill_prices

        slot.pending_order_id = None

        if slot.is_sector_position():
            logger.info(
                f"Slot {slot_id} filled: {slot.sector_name} "
                f"({len(slot.holdings)} stocks, total {slot.quantity} shares)"
            )
        else:
            logger.info(f"Slot {slot_id} filled: {slot.stock_code} x{slot.quantity}")

    def mark_pending_sell(self, slot_id: int, order_id: str | None = None) -> None:
        """
        Mark a slot as pending sell.

        Args:
            slot_id: Slot ID to update.
            order_id: Sell order ID for tracking.
        """
        slot = self.get_slot(slot_id)
        if not slot:
            raise ValueError(f"Slot {slot_id} not found")

        if slot.state != SlotState.FILLED:
            raise ValueError(f"Slot {slot_id} is not filled")

        slot.state = SlotState.PENDING_SELL
        slot.pending_order_id = order_id

    def release_slot(
        self,
        slot_id: int,
        exit_prices: dict[str, float] | float | None = None,
    ) -> None:
        """
        Release a slot after selling.

        Args:
            slot_id: Slot ID to release.
            exit_prices: Exit price(s) for P&L calculation. Can be:
                - None: no P&L calculation
                - float: applies to single stock or average for sector
                - dict[stock_code, price]: per-stock exit prices for sector
        """
        slot = self.get_slot(slot_id)
        if not slot:
            raise ValueError(f"Slot {slot_id} not found")

        # Calculate and log P&L
        if exit_prices is not None and slot.holdings:
            total_pnl = 0.0
            total_entry_value = 0.0
            total_exit_value = 0.0

            for holding in slot.holdings:
                if holding.entry_price is None:
                    continue

                # Get exit price for this holding
                if isinstance(exit_prices, dict):
                    if holding.stock_code not in exit_prices:
                        raise ValueError(
                            f"Missing exit price for {holding.stock_code} in slot {slot_id} "
                            f"— cannot release slot without complete P&L data"
                        )
                    exit_price = exit_prices[holding.stock_code]
                else:
                    exit_price = exit_prices

                if exit_price <= 0:
                    raise ValueError(
                        f"Invalid exit price {exit_price} for {holding.stock_code} "
                        f"in slot {slot_id} — refusing to calculate P&L with zero/negative price"
                    )

                if exit_price > 0:
                    pnl = (exit_price - holding.entry_price) * holding.quantity
                    total_pnl += pnl
                    total_entry_value += holding.entry_price * holding.quantity
                    total_exit_value += exit_price * holding.quantity

            if total_entry_value > 0:
                pnl_pct = (total_exit_value / total_entry_value - 1) * 100
                if slot.is_sector_position():
                    logger.info(
                        f"Released slot {slot_id} ({slot.sector_name}): "
                        f"P&L = {total_pnl:,.0f} ({pnl_pct:+.2f}%)"
                    )
                else:
                    logger.info(
                        f"Released slot {slot_id} ({slot.stock_code}): "
                        f"P&L = {total_pnl:,.0f} ({pnl_pct:+.2f}%)"
                    )

        # Store exit info before reset
        if isinstance(exit_prices, dict):
            # Use average exit price
            total_exit = sum(exit_prices.get(h.stock_code, 0) * h.quantity for h in slot.holdings)
            total_qty = sum(h.quantity for h in slot.holdings)
            slot.exit_price = total_exit / total_qty if total_qty > 0 else None
        else:
            slot.exit_price = exit_prices

        slot.exit_time = datetime.now()
        slot.reset()

    def get_holdings(self) -> list[PositionSlot]:
        """Get all filled positions."""
        return [slot for slot in self._slots if slot.is_filled()]

    def get_all_allocated(self) -> list[PositionSlot]:
        """Get all allocated slots (pending or filled)."""
        return [slot for slot in self._slots if not slot.is_available()]

    def get_quantity_for_price(self, price: float) -> int:
        """
        Calculate share quantity for slot capital and price.

        Args:
            price: Stock price.

        Returns:
            Number of shares (rounded to lot size).
        """
        if price <= 0:
            return 0

        # Calculate raw quantity
        raw_qty = self._config.slot_capital / price

        # Round down to lot size
        quantity = int(raw_qty / self._config.lot_size) * self._config.lot_size

        # Check minimum order amount
        if quantity * price < self._config.min_order_amount:
            return 0

        return quantity

    def get_available_capital(self, slot_type: str | SlotType) -> float:
        """
        Get total available capital for a slot type.

        Args:
            slot_type: "premarket" or "intraday".

        Returns:
            Total available capital across available slots.
        """
        available_slots = self.get_available_slots(slot_type)
        return len(available_slots) * self._config.slot_capital

    def get_state(self) -> dict[str, Any]:
        """
        Get manager state for persistence.

        Returns:
            State dictionary for checkpointing.
        """
        return {
            "config": {
                "total_capital": self._config.total_capital,
                "num_slots": self._config.num_slots,
                "premarket_slots": self._config.premarket_slots,
                "intraday_slots": self._config.intraday_slots,
                "min_order_amount": self._config.min_order_amount,
                "lot_size": self._config.lot_size,
            },
            "slots": [slot.to_dict() for slot in self._slots],
        }

    def load_state(self, state: dict[str, Any]) -> None:
        """
        Load manager state from persistence.

        Args:
            state: State dictionary from checkpointing.
        """
        # Load config
        config_data = state.get("config", {})
        self._config = PositionConfig(
            total_capital=config_data.get("total_capital", 10_000_000),
            num_slots=config_data.get("num_slots", 5),
            premarket_slots=config_data.get("premarket_slots", 3),
            intraday_slots=config_data.get("intraday_slots", 2),
            min_order_amount=config_data.get("min_order_amount", 10000),
            lot_size=config_data.get("lot_size", 100),
        )

        # Load slots
        slots_data = state.get("slots", [])
        if slots_data:
            self._slots = [PositionSlot.from_dict(s) for s in slots_data]
        else:
            self._initialize_slots()

    def get_summary(self) -> dict[str, Any]:
        """
        Get summary of current positions.

        Returns:
            Summary dictionary with stats.
        """
        filled_slots = self.get_holdings()
        premarket_available = len(self.get_available_slots(SlotType.PREMARKET))
        intraday_available = len(self.get_available_slots(SlotType.INTRADAY))

        # Calculate total value across all holdings in all slots
        total_value = 0.0
        total_stocks = 0
        for slot in filled_slots:
            for holding in slot.holdings:
                total_value += (holding.entry_price or 0) * holding.quantity
                total_stocks += 1

        return {
            "total_capital": self._config.total_capital,
            "slot_capital": self._config.slot_capital,
            "total_slots": len(self._slots),
            "premarket_available": premarket_available,
            "intraday_available": intraday_available,
            "filled_slots_count": len(filled_slots),
            "total_stocks_count": total_stocks,
            "holdings_value": total_value,
        }

    @property
    def config(self) -> PositionConfig:
        """Get current configuration."""
        return self._config

    def save_to_file(self, file_path: Path | str | None = None) -> None:
        """
        Save position state to a JSON file.

        Args:
            file_path: Path to save file. Uses DEFAULT_STATE_FILE if None.
        """
        path = Path(file_path) if file_path else DEFAULT_STATE_FILE
        path.parent.mkdir(parents=True, exist_ok=True)

        state = self.get_state()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved position state to {path}")

    def load_from_file(self, file_path: Path | str | None = None) -> bool:
        """
        Load position state from a JSON file.

        Args:
            file_path: Path to load file. Uses DEFAULT_STATE_FILE if None.

        Returns:
            True if state was loaded, False if file doesn't exist.
        """
        path = Path(file_path) if file_path else DEFAULT_STATE_FILE

        if not path.exists():
            logger.info(f"No position state file found at {path}")
            return False

        with open(path, "r", encoding="utf-8") as f:
            state = json.load(f)

        self.load_state(state)
        logger.info(f"Loaded position state from {path}")

        # Log loaded positions
        filled_slots = self.get_holdings()
        if filled_slots:
            for slot in filled_slots:
                if slot.is_sector_position():
                    logger.info(
                        f"  Slot {slot.slot_id} ({slot.sector_name}): {len(slot.holdings)} stocks"
                    )
                    for h in slot.holdings:
                        logger.info(
                            f"    {h.stock_code} {h.stock_name}: {h.quantity} @ {h.entry_price}"
                        )
                else:
                    logger.info(
                        f"  Slot {slot.slot_id}: {slot.stock_code} {slot.stock_name} "
                        f"x{slot.quantity} @ {slot.entry_price}"
                    )

        return True

    # ==================== PostgreSQL Persistence ====================

    async def save_to_db(self) -> None:
        """
        Save all slots to PostgreSQL database.

        Raises:
            RuntimeError: If repository not set.
        """
        if not self._repository:
            raise RuntimeError("Repository not set. Call set_repository() first.")

        for slot in self._slots:
            # Save slot
            slot_data = {
                "slot_id": slot.slot_id,
                "slot_type": slot.slot_type.value,
                "state": slot.state.value,
                "entry_time": slot.entry_time,
                "entry_reason": slot.entry_reason,
                "sector_name": slot.sector_name,
                "pending_order_id": slot.pending_order_id,
                "exit_price": slot.exit_price,
                "exit_time": slot.exit_time,
            }
            await self._repository.save_slot(slot_data)

            # Save holdings if any
            if slot.holdings:
                holdings_data = [h.to_dict() for h in slot.holdings]
                await self._repository.save_holdings(slot.slot_id, holdings_data)

        logger.info(f"Saved {len(self._slots)} slots to database")

    async def save_slot_to_db(self, slot_id: int) -> None:
        """
        Save a single slot to database.

        Args:
            slot_id: The slot ID to save.
        """
        if not self._repository:
            raise RuntimeError("Repository not set. Call set_repository() first.")

        slot = self.get_slot(slot_id)
        if not slot:
            raise ValueError(f"Slot {slot_id} not found")

        slot_data = {
            "slot_id": slot.slot_id,
            "slot_type": slot.slot_type.value,
            "state": slot.state.value,
            "entry_time": slot.entry_time,
            "entry_reason": slot.entry_reason,
            "sector_name": slot.sector_name,
            "pending_order_id": slot.pending_order_id,
            "exit_price": slot.exit_price,
            "exit_time": slot.exit_time,
        }
        await self._repository.save_slot(slot_data)

        if slot.holdings:
            holdings_data = [h.to_dict() for h in slot.holdings]
            await self._repository.save_holdings(slot.slot_id, holdings_data)

        logger.debug(f"Saved slot {slot_id} to database")

    async def load_from_db(self) -> bool:
        """
        Load slots from PostgreSQL database.

        Returns:
            True if state was loaded, False if no data exists.

        Raises:
            RuntimeError: If repository not set.
        """
        if not self._repository:
            raise RuntimeError("Repository not set. Call set_repository() first.")

        slots_data = await self._repository.get_all_slots()

        if not slots_data:
            # No data in DB, initialize fresh and save
            logger.info("No position data in database, initializing fresh slots")
            await self.save_to_db()
            return False

        # Load holdings for all slots
        all_holdings = await self._repository.get_all_holdings()

        # Reconstruct slots
        self._slots = []
        for data in slots_data:
            slot = PositionSlot(
                slot_id=data["slot_id"],
                slot_type=SlotType(data["slot_type"]),
                state=SlotState(data.get("state", "empty")),
            )

            # Load holdings
            slot_holdings = all_holdings.get(data["slot_id"], [])
            slot.holdings = [StockHolding.from_dict(h) for h in slot_holdings]

            slot.entry_time = data.get("entry_time")
            slot.entry_reason = data.get("entry_reason") or ""
            slot.sector_name = data.get("sector_name")
            slot.pending_order_id = data.get("pending_order_id")
            slot.exit_price = float(data["exit_price"]) if data.get("exit_price") else None
            slot.exit_time = data.get("exit_time")

            self._slots.append(slot)

        # Ensure we have all slots (in case DB is incomplete)
        existing_ids = {s.slot_id for s in self._slots}
        for i in range(self._config.premarket_slots):
            if i not in existing_ids:
                self._slots.append(PositionSlot(slot_id=i, slot_type=SlotType.PREMARKET))
        for i in range(self._config.intraday_slots):
            slot_id = self._config.premarket_slots + i
            if slot_id not in existing_ids:
                self._slots.append(PositionSlot(slot_id=slot_id, slot_type=SlotType.INTRADAY))

        # Sort by slot_id
        self._slots.sort(key=lambda s: s.slot_id)

        logger.info(f"Loaded {len(self._slots)} slots from database")

        # Log filled positions
        filled_slots = self.get_holdings()
        if filled_slots:
            for slot in filled_slots:
                if slot.is_sector_position():
                    logger.info(
                        f"  Slot {slot.slot_id} ({slot.sector_name}): {len(slot.holdings)} stocks"
                    )
                    for h in slot.holdings:
                        logger.info(
                            f"    {h.stock_code} {h.stock_name}: {h.quantity} @ {h.entry_price}"
                        )
                else:
                    logger.info(
                        f"  Slot {slot.slot_id}: {slot.stock_code} {slot.stock_name} "
                        f"x{slot.quantity} @ {slot.entry_price}"
                    )

        return True

    @property
    def has_repository(self) -> bool:
        """Check if repository is set."""
        return self._repository is not None


def create_position_manager_with_state(
    config: PositionConfig | None = None,
    state_file: Path | str | None = None,
) -> PositionManager:
    """
    Create a PositionManager and load existing state from JSON file.

    Note: For PostgreSQL persistence, use create_position_manager_with_db() instead.

    Args:
        config: Position configuration (used if no state file exists).
        state_file: Path to state file. Uses DEFAULT_STATE_FILE if None.

    Returns:
        PositionManager with loaded state or fresh initialization.
    """
    manager = PositionManager(config)
    manager.load_from_file(state_file)
    return manager


async def create_position_manager_with_db(
    config: PositionConfig | None = None,
    repository: TradingRepository | None = None,
) -> PositionManager:
    """
    Create a PositionManager with PostgreSQL persistence.

    Args:
        config: Position configuration.
        repository: Connected TradingRepository. If None, creates one from config.

    Returns:
        PositionManager with database persistence enabled.

    Example:
        # Option 1: Auto-create repository
        manager = await create_position_manager_with_db(config)

        # Option 2: Use existing repository
        repo = TradingRepository(repo_config)
        await repo.connect()
        manager = await create_position_manager_with_db(config, repo)
    """
    from src.trading.repository import create_trading_repository_from_config

    manager = PositionManager(config)

    if repository is None:
        repository = create_trading_repository_from_config()
        await repository.connect()

    manager.set_repository(repository)
    await manager.load_from_db()

    return manager
