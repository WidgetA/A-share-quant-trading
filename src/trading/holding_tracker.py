# === MODULE PURPOSE ===
# Tracks overnight holdings for next-morning confirmation.
# Supports user confirmation before market open.

# === KEY CONCEPTS ===
# - HoldingRecord: Snapshot of a holding for review
# - Morning confirmation: User decides to sell or hold
# - Generate sell signals for confirmed sales

# === PERSISTENCE ===
# - PostgreSQL (trading.overnight_holdings table) via TradingRepository
# - Supports system restart between market close and morning review

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from src.strategy.signals import SignalType, TradingSignal

if TYPE_CHECKING:
    from src.trading.position_manager import PositionManager, PositionSlot
    from src.trading.repository import TradingRepository

logger = logging.getLogger(__name__)


@dataclass
class HoldingRecord:
    """
    Record of an overnight holding for morning review.

    Contains all information needed for user to decide
    whether to sell or continue holding.
    """

    slot_id: int
    stock_code: str
    stock_name: str
    quantity: int
    entry_price: float
    entry_time: datetime
    entry_reason: str
    slot_type: str  # "premarket" or "intraday"

    # Updated at morning review
    current_price: float | None = None
    pnl_amount: float | None = None
    pnl_percent: float | None = None

    # Database ID (set when loaded from DB)
    db_id: int | None = None

    def calculate_pnl(self, current_price: float) -> None:
        """Calculate P&L based on current price."""
        self.current_price = current_price
        if self.entry_price and self.entry_price > 0:
            self.pnl_amount = (current_price - self.entry_price) * self.quantity
            self.pnl_percent = (current_price / self.entry_price - 1) * 100

    def get_display_string(self) -> str:
        """Get formatted string for display."""
        pnl_str = ""
        if self.pnl_percent is not None:
            sign = "+" if self.pnl_percent >= 0 else ""
            pnl_str = f" ({sign}{self.pnl_percent:.2f}%)"

        return (
            f"{self.stock_code} {self.stock_name or ''} "
            f"x{self.quantity} @ {self.entry_price:.2f}{pnl_str}"
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "slot_id": self.slot_id,
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "quantity": self.quantity,
            "entry_price": self.entry_price,
            "entry_time": self.entry_time.isoformat(),
            "entry_reason": self.entry_reason,
            "slot_type": self.slot_type,
            "current_price": self.current_price,
            "pnl_amount": self.pnl_amount,
            "pnl_percent": self.pnl_percent,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HoldingRecord":
        """Create from dictionary."""
        record = cls(
            slot_id=data["slot_id"],
            stock_code=data["stock_code"],
            stock_name=data.get("stock_name", ""),
            quantity=data["quantity"],
            entry_price=data["entry_price"],
            entry_time=datetime.fromisoformat(data["entry_time"]),
            entry_reason=data.get("entry_reason", ""),
            slot_type=data.get("slot_type", "premarket"),
        )
        record.current_price = data.get("current_price")
        record.pnl_amount = data.get("pnl_amount")
        record.pnl_percent = data.get("pnl_percent")
        return record


class HoldingTracker:
    """
    Tracks holdings for next-morning confirmation.

    Workflow:
        1. At market close, record all holdings
        2. Before next open, present holdings to user
        3. User confirms: sell or continue holding
        4. Generate sell signals for confirmed sales

    Persistence:
        - PostgreSQL (trading.overnight_holdings) via set_repository()
        - Survives system restart between close and morning review

    Usage:
        tracker = HoldingTracker()
        tracker.set_repository(repo)  # Optional: for DB persistence

        # At market close
        holdings = await tracker.record_holdings(position_manager)

        # Before next open
        holdings = await tracker.get_morning_review(price_fetcher)

        # After user selects which to sell
        sell_signals = await tracker.generate_sell_signals(
            slots_to_sell=[0, 2],
            strategy_name="news_analysis"
        )
    """

    def __init__(self):
        """Initialize holding tracker."""
        self._holdings: list[HoldingRecord] = []
        self._record_time: datetime | None = None
        self._repository: TradingRepository | None = None

    def set_repository(self, repository: TradingRepository) -> None:
        """Set the database repository for persistence."""
        self._repository = repository
        logger.info("HoldingTracker: Using PostgreSQL for persistence")

    async def record_holdings(
        self,
        position_manager: "PositionManager",
    ) -> list[HoldingRecord]:
        """
        Record all current holdings at market close.

        Args:
            position_manager: Position manager with current holdings.

        Returns:
            List of HoldingRecords for overnight tracking.
        """

        self._holdings = []
        holdings = position_manager.get_holdings()

        for slot in holdings:
            record = self._slot_to_record(slot)
            self._holdings.append(record)

        self._record_time = datetime.now()

        # Save to database if repository is set
        if self._repository:
            holdings_data = [h.to_dict() for h in self._holdings]
            await self._repository.save_overnight_holdings(self._record_time, holdings_data)

        logger.info(f"Recorded {len(self._holdings)} holdings for overnight tracking")
        return self._holdings

    def _slot_to_record(self, slot: "PositionSlot") -> HoldingRecord:
        """Convert a PositionSlot to HoldingRecord."""
        return HoldingRecord(
            slot_id=slot.slot_id,
            stock_code=slot.stock_code or "",
            stock_name=slot.stock_name or "",
            quantity=slot.quantity,
            entry_price=slot.entry_price or 0.0,
            entry_time=slot.entry_time or datetime.now(),
            entry_reason=slot.entry_reason,
            slot_type=slot.slot_type.value,
        )

    async def get_morning_review(
        self,
        price_fetcher: Any | None = None,
    ) -> list[HoldingRecord]:
        """
        Get holdings for morning review with current prices.

        Args:
            price_fetcher: Optional callable to fetch current prices.
                Should be async: price_fetcher(stock_code) -> float

        Returns:
            List of HoldingRecords with P&L calculations.
        """
        if price_fetcher:
            for holding in self._holdings:
                try:
                    current_price = await price_fetcher(holding.stock_code)
                    if current_price and current_price > 0:
                        holding.calculate_pnl(current_price)
                except Exception as e:
                    logger.warning(f"Failed to fetch price for {holding.stock_code}: {e}")

        return self._holdings

    async def generate_sell_signals(
        self,
        slots_to_sell: list[int],
        strategy_name: str,
        sell_price: float | None = None,
    ) -> list[TradingSignal]:
        """
        Generate sell signals for selected holdings.

        Args:
            slots_to_sell: List of slot IDs to sell.
            strategy_name: Strategy name for signals.
            sell_price: Optional target sell price.

        Returns:
            List of SELL TradingSignals.
        """
        signals = []

        for holding in self._holdings:
            if holding.slot_id in slots_to_sell:
                signal = TradingSignal(
                    signal_type=SignalType.SELL,
                    stock_code=holding.stock_code,
                    quantity=holding.quantity,
                    strategy_name=strategy_name,
                    price=sell_price or holding.current_price,
                    reason=f"隔夜持仓卖出 (入场原因: {holding.entry_reason})",
                    metadata={
                        "slot_id": holding.slot_id,
                        "entry_price": holding.entry_price,
                        "entry_time": holding.entry_time.isoformat(),
                        "holding_type": "overnight",
                    },
                )
                signals.append(signal)

                logger.info(
                    f"Generated sell signal for slot {holding.slot_id}: "
                    f"{holding.stock_code} x{holding.quantity}"
                )

        return signals

    def get_holdings(self) -> list[HoldingRecord]:
        """Get all tracked holdings."""
        return self._holdings

    def get_holding_by_slot(self, slot_id: int) -> HoldingRecord | None:
        """Get holding by slot ID."""
        for holding in self._holdings:
            if holding.slot_id == slot_id:
                return holding
        return None

    def clear(self) -> None:
        """Clear all tracked holdings."""
        self._holdings = []
        self._record_time = None

    def get_state(self) -> dict[str, Any]:
        """Get state for persistence."""
        return {
            "holdings": [h.to_dict() for h in self._holdings],
            "record_time": (self._record_time.isoformat() if self._record_time else None),
        }

    def load_state(self, state: dict[str, Any]) -> None:
        """Load state from persistence."""
        holdings_data = state.get("holdings", [])
        self._holdings = [HoldingRecord.from_dict(h) for h in holdings_data]

        record_time_str = state.get("record_time")
        self._record_time = datetime.fromisoformat(record_time_str) if record_time_str else None

    @property
    def record_time(self) -> datetime | None:
        """Get time when holdings were recorded."""
        return self._record_time

    @property
    def holdings_count(self) -> int:
        """Get number of tracked holdings."""
        return len(self._holdings)

    # ==================== Database Persistence ====================

    async def load_from_db(self, record_date: datetime | None = None) -> bool:
        """
        Load overnight holdings from database.

        Args:
            record_date: Date to load. If None, loads latest.

        Returns:
            True if holdings were loaded, False if no data.
        """
        if not self._repository:
            logger.warning("Repository not set, cannot load from database")
            return False

        rows = await self._repository.get_overnight_holdings(record_date)

        if not rows:
            logger.info("No overnight holdings found in database")
            return False

        self._holdings = []
        for row in rows:
            record = HoldingRecord(
                slot_id=row["slot_id"],
                stock_code=row["stock_code"],
                stock_name=row.get("stock_name") or "",
                quantity=row["quantity"],
                entry_price=float(row["entry_price"]) if row.get("entry_price") else 0.0,
                entry_time=row.get("entry_time") or datetime.now(),
                entry_reason=row.get("entry_reason") or "",
                slot_type=row.get("slot_type") or "premarket",
            )
            record.current_price = float(row["current_price"]) if row.get("current_price") else None
            record.pnl_amount = float(row["pnl_amount"]) if row.get("pnl_amount") else None
            record.pnl_percent = float(row["pnl_percent"]) if row.get("pnl_percent") else None
            # Store database ID for later updates
            record.db_id = row["id"]
            self._holdings.append(record)

        # Get record time from first row
        if rows and rows[0].get("record_date"):
            self._record_time = datetime.combine(rows[0]["record_date"], datetime.min.time())

        logger.info(f"Loaded {len(self._holdings)} overnight holdings from database")
        return True

    async def save_price_updates_to_db(self) -> None:
        """Save current price and P&L updates to database."""
        if not self._repository:
            return

        for holding in self._holdings:
            if holding.db_id is not None and holding.current_price is not None:
                await self._repository.update_overnight_holding_price(
                    holding.db_id,
                    holding.current_price,
                    holding.pnl_amount or 0,
                    holding.pnl_percent or 0,
                )

    async def mark_reviewed_in_db(self, slot_id: int, action: str) -> None:
        """Mark a holding as reviewed in database."""
        if not self._repository:
            return

        for holding in self._holdings:
            if holding.slot_id == slot_id and holding.db_id is not None:
                await self._repository.mark_overnight_holding_reviewed(holding.db_id, action)
                break

    @property
    def has_repository(self) -> bool:
        """Check if repository is set."""
        return self._repository is not None
