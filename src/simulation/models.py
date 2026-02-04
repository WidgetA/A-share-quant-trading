# === MODULE PURPOSE ===
# Data models for the historical simulation system.
# Defines states, phases, transactions, and results for simulation.

# === KEY CONCEPTS ===
# - SimulationPhase: Trading day phases (premarket, auction, trading, close)
# - SimulationState: Current snapshot of the simulation
# - SimulatedTransaction: Record of a simulated trade
# - SimulationResult: Final P&L and summary

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any


class SimulationPhase(Enum):
    """Phases of the simulation trading day."""

    NOT_STARTED = "not_started"
    PREMARKET_ANALYSIS = "premarket_analysis"  # 8:30 - User selects stocks
    MORNING_AUCTION = "morning_auction"  # 9:25 - Execute buys
    TRADING_HOURS = "trading_hours"  # 9:30-15:00 - Monitor
    MARKET_CLOSE = "market_close"  # 15:00 - Day summary
    MORNING_CONFIRMATION = "morning_confirmation"  # Next day 9:00 - Sell/hold
    COMPLETED = "completed"  # Simulation finished


@dataclass
class SimulatedTransaction:
    """Record of a simulated trade."""

    action: str  # 'BUY' or 'SELL'
    stock_code: str
    stock_name: str
    quantity: int
    price: float
    timestamp: datetime
    slot_id: int
    reason: str

    @property
    def amount(self) -> float:
        """Total transaction amount."""
        return self.quantity * self.price

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "action": self.action,
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "quantity": self.quantity,
            "price": self.price,
            "amount": self.amount,
            "timestamp": self.timestamp.isoformat(),
            "slot_id": self.slot_id,
            "reason": self.reason,
        }


@dataclass
class SimulationHolding:
    """A holding position in the simulation."""

    slot_id: int
    stock_code: str
    stock_name: str
    quantity: int
    entry_price: float
    entry_time: datetime
    entry_reason: str
    current_price: float | None = None

    @property
    def cost_basis(self) -> float:
        """Total cost of the position."""
        return self.quantity * self.entry_price

    @property
    def current_value(self) -> float | None:
        """Current value if price is available."""
        if self.current_price is None:
            return None
        return self.quantity * self.current_price

    @property
    def pnl_amount(self) -> float | None:
        """Profit/loss amount."""
        current_value = self.current_value
        if current_value is None:
            return None
        return current_value - self.cost_basis

    @property
    def pnl_percent(self) -> float | None:
        """Profit/loss percentage."""
        if self.current_price is None or self.entry_price == 0:
            return None
        return (self.current_price - self.entry_price) / self.entry_price * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "slot_id": self.slot_id,
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "quantity": self.quantity,
            "entry_price": self.entry_price,
            "entry_time": self.entry_time.isoformat(),
            "entry_reason": self.entry_reason,
            "current_price": self.current_price,
            "cost_basis": self.cost_basis,
            "current_value": self.current_value,
            "pnl_amount": self.pnl_amount,
            "pnl_percent": self.pnl_percent,
        }


@dataclass
class PendingSignal:
    """A signal pending user selection."""

    index: int  # 1-based index for user selection
    signal_type: str  # 'buy_stock' or 'buy_sector'
    sentiment: str  # 'strong_bullish', 'bullish', etc.
    confidence: float  # 0.0-1.0
    target_stocks: list[str]  # Stock codes
    target_sectors: list[str]  # Sector names
    title: str
    reasoning: str
    message_id: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "index": self.index,
            "signal_type": self.signal_type,
            "sentiment": self.sentiment,
            "confidence": self.confidence,
            "target_stocks": self.target_stocks,
            "target_sectors": self.target_sectors,
            "title": self.title,
            "reasoning": self.reasoning,
        }


@dataclass
class SimulationState:
    """Current state of the simulation."""

    phase: SimulationPhase
    sim_date: date
    sim_time: datetime
    day_number: int  # Which trading day (1, 2, ...)
    total_days: int  # Total days to simulate

    # Pending user actions
    pending_signals: list[PendingSignal] = field(default_factory=list)
    pending_holdings: list[SimulationHolding] = field(default_factory=list)

    # Current positions
    holdings: list[SimulationHolding] = field(default_factory=list)

    # Completed transactions
    transactions: list[SimulatedTransaction] = field(default_factory=list)

    # Capital tracking
    initial_capital: float = 10_000_000.0
    available_cash: float = 10_000_000.0

    # Day summary (updated at close)
    day_pnl_amount: float = 0.0
    day_pnl_percent: float = 0.0

    # Messages for display
    messages: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "phase": self.phase.value,
            "sim_date": self.sim_date.isoformat(),
            "sim_time": self.sim_time.isoformat(),
            "day_number": self.day_number,
            "total_days": self.total_days,
            "pending_signals": [s.to_dict() for s in self.pending_signals],
            "pending_holdings": [h.to_dict() for h in self.pending_holdings],
            "holdings": [h.to_dict() for h in self.holdings],
            "transactions": [t.to_dict() for t in self.transactions],
            "initial_capital": self.initial_capital,
            "available_cash": self.available_cash,
            "day_pnl_amount": self.day_pnl_amount,
            "day_pnl_percent": self.day_pnl_percent,
            "messages": self.messages,
        }


@dataclass
class SimulationResult:
    """Final result of the simulation."""

    start_date: date
    end_date: date
    total_days: int

    initial_capital: float
    final_capital: float
    total_pnl: float
    pnl_percent: float

    transactions: list[SimulatedTransaction]
    final_holdings: list[SimulationHolding]

    # Per-day breakdown
    daily_pnl: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "total_days": self.total_days,
            "initial_capital": self.initial_capital,
            "final_capital": self.final_capital,
            "total_pnl": self.total_pnl,
            "pnl_percent": self.pnl_percent,
            "transactions": [t.to_dict() for t in self.transactions],
            "final_holdings": [h.to_dict() for h in self.final_holdings],
            "daily_pnl": self.daily_pnl,
        }


@dataclass
class SimulationConfig:
    """Configuration for simulation."""

    total_capital: float = 10_000_000.0
    num_slots: int = 5
    premarket_slots: int = 3
    intraday_slots: int = 2
    min_order_amount: float = 10_000.0
    lot_size: int = 100

    @property
    def slot_capital(self) -> float:
        """Capital allocated per slot."""
        return self.total_capital / self.num_slots
