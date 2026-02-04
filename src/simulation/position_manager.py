# === MODULE PURPOSE ===
# Simulation-only position manager that doesn't persist to database.
# Tracks positions and transactions for P&L calculation.

# === KEY CONCEPTS ===
# - Isolated state: No database persistence
# - Transaction tracking: Records all simulated trades
# - P&L calculation: Computes final simulation results

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from src.simulation.models import (
    SimulatedTransaction,
    SimulationConfig,
    SimulationHolding,
    SimulationResult,
)

logger = logging.getLogger(__name__)


class SlotState:
    """State of a position slot."""

    EMPTY = "empty"
    PENDING_BUY = "pending_buy"
    FILLED = "filled"
    PENDING_SELL = "pending_sell"


class SlotType:
    """Type of position slot."""

    PREMARKET = "premarket"
    INTRADAY = "intraday"


@dataclass
class SimulationSlot:
    """A position slot for simulation."""

    slot_id: int
    slot_type: str  # "premarket" or "intraday"
    state: str = SlotState.EMPTY
    holdings: list[SimulationHolding] = field(default_factory=list)
    entry_reason: str = ""
    sector_name: str | None = None

    def is_available(self) -> bool:
        """Check if slot is available."""
        return self.state == SlotState.EMPTY

    def is_filled(self) -> bool:
        """Check if slot has filled position."""
        return self.state == SlotState.FILLED

    def reset(self) -> None:
        """Reset slot to empty."""
        self.state = SlotState.EMPTY
        self.holdings = []
        self.entry_reason = ""
        self.sector_name = None


class SimulationPositionManager:
    """
    Position manager for simulation that doesn't persist to database.

    Tracks all simulated positions and transactions for P&L calculation.
    Uses the same slot-based capital allocation as the real PositionManager.

    Usage:
        config = SimulationConfig(total_capital=10_000_000)
        manager = SimulationPositionManager(config)

        # Allocate and buy
        slot = manager.get_available_slot("premarket")
        manager.allocate_slot(slot, "600519.SH", 1800.0, "Test reason")
        manager.fill_slot(slot.slot_id, {"600519.SH": 1800.0})

        # Calculate P&L
        result = manager.calculate_result(
            start_date, end_date,
            current_prices={"600519.SH": 1850.0}
        )
    """

    def __init__(self, config: SimulationConfig | None = None):
        """Initialize simulation position manager."""
        self._config = config or SimulationConfig()
        self._slots: list[SimulationSlot] = []
        self._transactions: list[SimulatedTransaction] = []
        self._available_cash: float = self._config.total_capital

        # Initialize slots
        self._init_slots()

    def _init_slots(self) -> None:
        """Initialize position slots."""
        self._slots = []

        # Premarket slots
        for i in range(self._config.premarket_slots):
            self._slots.append(SimulationSlot(slot_id=i, slot_type=SlotType.PREMARKET))

        # Intraday slots
        for i in range(self._config.intraday_slots):
            slot_id = self._config.premarket_slots + i
            self._slots.append(SimulationSlot(slot_id=slot_id, slot_type=SlotType.INTRADAY))

    @property
    def total_capital(self) -> float:
        """Get total capital."""
        return self._config.total_capital

    @property
    def slot_capital(self) -> float:
        """Get capital per slot."""
        return self._config.slot_capital

    @property
    def available_cash(self) -> float:
        """Get available cash."""
        return self._available_cash

    @property
    def transactions(self) -> list[SimulatedTransaction]:
        """Get all transactions."""
        return self._transactions

    def get_available_slot(self, slot_type: str) -> SimulationSlot | None:
        """
        Get an available slot of the specified type.

        Args:
            slot_type: "premarket" or "intraday"

        Returns:
            Available slot or None if no slots available.
        """
        for slot in self._slots:
            if slot.slot_type == slot_type and slot.is_available():
                return slot
        return None

    def get_slot(self, slot_id: int) -> SimulationSlot | None:
        """Get slot by ID."""
        for slot in self._slots:
            if slot.slot_id == slot_id:
                return slot
        return None

    def allocate_slot(
        self,
        slot: SimulationSlot,
        stock_code: str,
        price: float,
        reason: str,
        stock_name: str = "",
        timestamp: datetime | None = None,
    ) -> int:
        """
        Allocate a slot for a single stock.

        Args:
            slot: Slot to allocate
            stock_code: Stock code to buy
            price: Price to buy at
            reason: Reason for trade
            stock_name: Stock name
            timestamp: Transaction time

        Returns:
            Quantity to buy.
        """
        # Calculate quantity
        quantity = self._calculate_quantity(price)

        holding = SimulationHolding(
            slot_id=slot.slot_id,
            stock_code=stock_code,
            stock_name=stock_name,
            quantity=quantity,
            entry_price=price,
            entry_time=timestamp or datetime.now(),
            entry_reason=reason,
        )

        slot.holdings = [holding]
        slot.state = SlotState.PENDING_BUY
        slot.entry_reason = reason

        logger.info(f"Allocated slot {slot.slot_id}: {stock_code} x{quantity} @ {price}")

        return quantity

    def allocate_slot_sector(
        self,
        slot: SimulationSlot,
        stocks: list[tuple[str, str, float]],  # [(code, name, price), ...]
        sector_name: str,
        reason: str,
        timestamp: datetime | None = None,
    ) -> list[tuple[str, int]]:
        """
        Allocate a slot for multiple stocks (sector buying).

        Args:
            slot: Slot to allocate
            stocks: List of (code, name, price) tuples
            sector_name: Sector name
            reason: Reason for trade
            timestamp: Transaction time

        Returns:
            List of (code, quantity) tuples.
        """
        if not stocks:
            return []

        # Split capital among stocks
        capital_per_stock = self.slot_capital / len(stocks)
        ts = timestamp or datetime.now()

        holdings = []
        result = []

        for code, name, price in stocks:
            quantity = self._calculate_quantity_with_capital(price, capital_per_stock)
            if quantity > 0:
                holding = SimulationHolding(
                    slot_id=slot.slot_id,
                    stock_code=code,
                    stock_name=name,
                    quantity=quantity,
                    entry_price=price,
                    entry_time=ts,
                    entry_reason=reason,
                )
                holdings.append(holding)
                result.append((code, quantity))

        slot.holdings = holdings
        slot.state = SlotState.PENDING_BUY
        slot.sector_name = sector_name
        slot.entry_reason = reason

        logger.info(f"Allocated slot {slot.slot_id} for sector {sector_name}")

        return result

    def fill_slot(
        self,
        slot_id: int,
        fill_prices: dict[str, float] | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        """
        Mark slot as filled and record transactions.

        Args:
            slot_id: Slot ID to fill
            fill_prices: Optional dict of code -> actual fill price
            timestamp: Transaction time
        """
        slot = self.get_slot(slot_id)
        if not slot or slot.state != SlotState.PENDING_BUY:
            return

        ts = timestamp or datetime.now()

        for holding in slot.holdings:
            # Use fill price if provided, otherwise use entry price
            price = (
                fill_prices.get(holding.stock_code, holding.entry_price)
                if fill_prices
                else holding.entry_price
            )

            if price:
                holding.entry_price = price

            # Record transaction
            tx = SimulatedTransaction(
                action="BUY",
                stock_code=holding.stock_code,
                stock_name=holding.stock_name,
                quantity=holding.quantity,
                price=holding.entry_price,
                timestamp=ts,
                slot_id=slot_id,
                reason=slot.entry_reason,
            )
            self._transactions.append(tx)

            # Reduce available cash
            self._available_cash -= tx.amount

        slot.state = SlotState.FILLED
        logger.info(f"Filled slot {slot_id}")

    def mark_pending_sell(self, slot_id: int) -> None:
        """Mark slot as pending sell."""
        slot = self.get_slot(slot_id)
        if slot and slot.state == SlotState.FILLED:
            slot.state = SlotState.PENDING_SELL

    def release_slot(
        self,
        slot_id: int,
        sell_prices: dict[str, float],
        timestamp: datetime | None = None,
    ) -> float:
        """
        Release slot (sell all holdings).

        Args:
            slot_id: Slot ID to release
            sell_prices: Dict of code -> sell price
            timestamp: Transaction time

        Returns:
            Total P&L for this slot.
        """
        slot = self.get_slot(slot_id)
        if not slot or slot.state not in (SlotState.FILLED, SlotState.PENDING_SELL):
            return 0.0

        ts = timestamp or datetime.now()
        total_pnl = 0.0

        for holding in slot.holdings:
            sell_price = sell_prices.get(holding.stock_code, 0.0)

            # Record sell transaction
            tx = SimulatedTransaction(
                action="SELL",
                stock_code=holding.stock_code,
                stock_name=holding.stock_name,
                quantity=holding.quantity,
                price=sell_price,
                timestamp=ts,
                slot_id=slot_id,
                reason=f"Sell from slot {slot_id}",
            )
            self._transactions.append(tx)

            # Calculate P&L
            cost = holding.quantity * holding.entry_price
            revenue = holding.quantity * sell_price
            pnl = revenue - cost
            total_pnl += pnl

            # Add to available cash
            self._available_cash += revenue

        slot.reset()
        logger.info(f"Released slot {slot_id}, P&L: {total_pnl:+.2f}")

        return total_pnl

    def get_filled_slots(self) -> list[SimulationSlot]:
        """Get all filled slots."""
        return [s for s in self._slots if s.is_filled()]

    def get_all_holdings(self) -> list[SimulationHolding]:
        """Get all holdings from filled slots."""
        holdings = []
        for slot in self._slots:
            if slot.state in (SlotState.FILLED, SlotState.PENDING_SELL):
                holdings.extend(slot.holdings)
        return holdings

    def update_holding_prices(self, prices: dict[str, float]) -> None:
        """Update current prices for all holdings."""
        for slot in self._slots:
            for holding in slot.holdings:
                if holding.stock_code in prices:
                    holding.current_price = prices[holding.stock_code]

    def calculate_holdings_value(self, prices: dict[str, float]) -> float:
        """Calculate total value of all holdings."""
        total = 0.0
        for holding in self.get_all_holdings():
            price = prices.get(holding.stock_code, holding.entry_price)
            total += holding.quantity * price
        return total

    def calculate_result(
        self,
        start_date,
        end_date,
        current_prices: dict[str, float],
    ) -> SimulationResult:
        """
        Calculate final simulation result.

        Args:
            start_date: Simulation start date
            end_date: Simulation end date
            current_prices: Current prices for held stocks

        Returns:
            SimulationResult with P&L summary.
        """
        # Update prices
        self.update_holding_prices(current_prices)

        # Calculate final capital
        holdings_value = self.calculate_holdings_value(current_prices)
        final_capital = self._available_cash + holdings_value

        # Total P&L
        total_pnl = final_capital - self._config.total_capital
        pnl_percent = (total_pnl / self._config.total_capital) * 100

        # Get final holdings
        final_holdings = self.get_all_holdings()

        return SimulationResult(
            start_date=start_date,
            end_date=end_date,
            total_days=(end_date - start_date).days + 1,
            initial_capital=self._config.total_capital,
            final_capital=final_capital,
            total_pnl=total_pnl,
            pnl_percent=pnl_percent,
            transactions=self._transactions,
            final_holdings=final_holdings,
        )

    def get_summary(self) -> dict[str, Any]:
        """Get position summary."""
        filled = self.get_filled_slots()
        premarket_available = sum(
            1 for s in self._slots if s.slot_type == SlotType.PREMARKET and s.is_available()
        )
        intraday_available = sum(
            1 for s in self._slots if s.slot_type == SlotType.INTRADAY and s.is_available()
        )

        return {
            "total_capital": self._config.total_capital,
            "slot_capital": self._config.slot_capital,
            "available_cash": self._available_cash,
            "total_slots": len(self._slots),
            "filled_slots": len(filled),
            "premarket_available": premarket_available,
            "intraday_available": intraday_available,
            "total_holdings": len(self.get_all_holdings()),
            "transaction_count": len(self._transactions),
        }

    def _calculate_quantity(self, price: float) -> int:
        """Calculate quantity based on slot capital and price."""
        return self._calculate_quantity_with_capital(price, self.slot_capital)

    def _calculate_quantity_with_capital(self, price: float, capital: float) -> int:
        """Calculate quantity for given capital."""
        if price <= 0:
            return 0

        # Calculate shares, round down to lot size
        shares = int(capital / price)
        lot_size = self._config.lot_size
        quantity = (shares // lot_size) * lot_size

        # Check minimum order amount
        if quantity * price < self._config.min_order_amount:
            return 0

        return quantity

    def reset(self) -> None:
        """Reset all state."""
        self._init_slots()
        self._transactions = []
        self._available_cash = self._config.total_capital

    def load_holdings(
        self,
        slots_data: list[dict],
        holdings_data: dict[int, list[dict]],
    ) -> tuple[int, float]:
        """
        Load existing holdings into simulation slots.

        This is used to initialize simulation with historical positions
        loaded from the database. When loading holdings, the initial capital
        is automatically calculated based on the loaded positions.

        Args:
            slots_data: List of slot dicts from database with keys:
                - slot_id, slot_type, state, entry_time, entry_reason, sector_name
            holdings_data: Dict mapping slot_id to list of holding dicts:
                - stock_code, stock_name, quantity, entry_price

        Returns:
            Tuple of (filled_count, total_holdings_value)
        """
        filled_count = 0
        total_holdings_value = 0.0

        for slot_dict in slots_data:
            slot_id = slot_dict["slot_id"]
            state = slot_dict.get("state", "empty")

            # Only load filled slots
            if state != "filled":
                continue

            # Find matching simulation slot
            slot = self.get_slot(slot_id)
            if not slot:
                logger.warning(f"Slot {slot_id} not found in simulation slots")
                continue

            # Get holdings for this slot
            slot_holdings = holdings_data.get(slot_id, [])
            if not slot_holdings:
                logger.warning(f"Slot {slot_id} marked as filled but has no holdings")
                continue

            # Load holdings into slot
            holdings = []
            slot_value = 0.0
            entry_time = slot_dict.get("entry_time") or datetime.now()
            for h in slot_holdings:
                entry_price = float(h["entry_price"]) if h.get("entry_price") else 0.0
                quantity = h.get("quantity", 0)
                holding = SimulationHolding(
                    slot_id=slot_id,
                    stock_code=h["stock_code"],
                    stock_name=h.get("stock_name", ""),
                    quantity=quantity,
                    entry_price=entry_price,
                    entry_time=entry_time,
                    entry_reason=slot_dict.get("entry_reason", ""),
                )
                holdings.append(holding)
                slot_value += entry_price * quantity

            # Update slot state
            slot.state = SlotState.FILLED
            slot.holdings = holdings
            slot.entry_reason = slot_dict.get("entry_reason", "")
            slot.sector_name = slot_dict.get("sector_name")

            total_holdings_value += slot_value
            filled_count += 1
            logger.info(f"Loaded slot {slot_id}: {len(holdings)} holdings, value={slot_value:,.0f}")

        # When loading holdings, set initial capital = holdings value
        # This means available_cash = 0 (all capital is in positions)
        if filled_count > 0:
            self._config = SimulationConfig(
                total_capital=total_holdings_value,
                num_slots=self._config.num_slots,
                premarket_slots=self._config.premarket_slots,
                intraday_slots=self._config.intraday_slots,
            )
            self._available_cash = 0.0
            logger.info(f"Set initial capital to holdings value: {total_holdings_value:,.0f}")

        return filled_count, total_holdings_value

    # No-op methods for compatibility with real PositionManager
    async def save_to_db(self) -> None:
        """No-op: simulation doesn't persist to database."""
        pass

    async def load_from_db(self) -> bool:
        """No-op: simulation doesn't load from database."""
        return False
