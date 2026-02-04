# === MODULE PURPOSE ===
# Main orchestrator for historical simulation.
# Manages the simulation lifecycle and coordinates all components.

# === KEY CONCEPTS ===
# - Phase-based execution: Move through trading day phases
# - Web-driven: State is exposed via API, user actions come from web
# - Isolated: Doesn't affect real trading data

import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from src.data.readers.message_reader import MessageReader, create_message_reader_from_config
from src.simulation.clock import SimulationClock
from src.simulation.context import SimulationContext
from src.simulation.historical_message_reader import HistoricalMessageReader
from src.simulation.historical_price_service import HistoricalPriceService
from src.simulation.models import (
    PendingSignal,
    SimulationConfig,
    SimulationPhase,
    SimulationResult,
    SimulationState,
)
from src.simulation.position_manager import SimulationPositionManager, SlotType

logger = logging.getLogger(__name__)


@dataclass
class SimulationSettings:
    """Settings for a simulation run."""

    start_date: date
    num_days: int = 1
    initial_capital: float = 10_000_000.0
    load_holdings_from_date: date | None = None


class SimulationManager:
    """
    Main orchestrator for historical trading simulation.

    Manages the complete lifecycle of a simulation:
    1. Initialize components (clock, price service, message reader)
    2. Progress through trading day phases
    3. Generate signals from historical messages
    4. Process user selections
    5. Calculate and report P&L

    Usage (via Web API):
        manager = SimulationManager()
        await manager.initialize(settings)

        # Get current state
        state = manager.get_state()

        # Advance through phases
        await manager.advance_to_next_phase()

        # Process user selection
        await manager.process_selection([1, 2])  # Select signals 1 and 2

        # Get final result
        result = manager.get_result()

    The simulation is driven by Web API calls, not automated execution.
    This allows the user to control the pace and make decisions.
    """

    def __init__(self):
        """Initialize simulation manager (not started yet)."""
        self._settings: SimulationSettings | None = None
        self._clock: SimulationClock | None = None
        self._message_reader: MessageReader | None = None
        self._hist_message_reader: HistoricalMessageReader | None = None
        self._price_service: HistoricalPriceService | None = None
        self._position_manager: SimulationPositionManager | None = None
        self._context: SimulationContext | None = None

        # Simulation state
        self._phase = SimulationPhase.NOT_STARTED
        self._current_day = 0
        self._pending_signals: list[PendingSignal] = []
        self._selected_signals: list[PendingSignal] = []
        self._messages: list[str] = []  # Log messages for UI

        # Track what we've processed
        self._last_message_time: datetime | None = None

    @property
    def is_initialized(self) -> bool:
        """Check if simulation is initialized."""
        return self._clock is not None

    @property
    def is_running(self) -> bool:
        """Check if simulation is in progress."""
        return self._phase not in (
            SimulationPhase.NOT_STARTED,
            SimulationPhase.COMPLETED,
        )

    async def initialize(self, settings: SimulationSettings) -> None:
        """
        Initialize simulation with given settings.

        Args:
            settings: Simulation configuration.
        """
        self._settings = settings
        self._current_day = 1

        # Initialize clock
        self._clock = SimulationClock(start_date=settings.start_date)

        # Initialize price service
        self._price_service = HistoricalPriceService()
        await self._price_service.start()

        # Initialize message reader
        self._message_reader = create_message_reader_from_config()
        await self._message_reader.connect()
        self._hist_message_reader = HistoricalMessageReader(self._message_reader, self._clock)

        # Initialize position manager
        config = SimulationConfig(total_capital=settings.initial_capital)
        self._position_manager = SimulationPositionManager(config)

        # Initialize context
        self._context = SimulationContext(
            clock=self._clock,
            message_reader=self._hist_message_reader,
            price_service=self._price_service,
        )

        # TODO: Load initial holdings if specified
        # if settings.load_holdings_from_date:
        #     await self._load_initial_holdings(settings.load_holdings_from_date)

        # Set phase to premarket
        self._phase = SimulationPhase.PREMARKET_ANALYSIS
        self._last_message_time = datetime.combine(
            settings.start_date - timedelta(days=1),
            time(15, 0),
        )

        self._add_message(
            f"Simulation initialized: {settings.start_date} ({settings.num_days} day(s))"
        )

        logger.info(f"Simulation initialized for {settings.start_date}")

    async def cleanup(self) -> None:
        """Cleanup resources."""
        if self._price_service:
            await self._price_service.stop()
        if self._message_reader:
            await self._message_reader.close()

        self._phase = SimulationPhase.NOT_STARTED
        logger.info("Simulation cleaned up")

    def get_state(self) -> SimulationState:
        """
        Get current simulation state.

        Returns:
            SimulationState with all current data.
        """
        if not self._clock or not self._position_manager:
            return SimulationState(
                phase=SimulationPhase.NOT_STARTED,
                sim_date=date.today(),
                sim_time=datetime.now(),
                day_number=0,
                total_days=0,
            )

        # Get holdings with current prices
        holdings = []
        for h in self._position_manager.get_all_holdings():
            holdings.append(h)

        # Get pending holdings for morning confirmation
        pending_holdings = []
        if self._phase == SimulationPhase.MORNING_CONFIRMATION:
            pending_holdings = holdings.copy()

        return SimulationState(
            phase=self._phase,
            sim_date=self._clock.current_date,
            sim_time=self._clock.current_time,
            day_number=self._current_day,
            total_days=self._settings.num_days if self._settings else 0,
            pending_signals=self._pending_signals,
            pending_holdings=pending_holdings,
            holdings=holdings,
            transactions=self._position_manager.transactions,
            initial_capital=self._position_manager.total_capital,
            available_cash=self._position_manager.available_cash,
            messages=self._messages[-10:],  # Last 10 messages
        )

    async def advance_to_next_phase(self) -> SimulationPhase:
        """
        Advance to the next simulation phase.

        Returns:
            New phase after advancing.
        """
        if not self._clock or not self._settings or not self._position_manager:
            return SimulationPhase.NOT_STARTED

        current_phase = self._phase

        if current_phase == SimulationPhase.PREMARKET_ANALYSIS:
            # Generate signals for premarket
            await self._generate_premarket_signals()
            # Stay in premarket until user makes selection
            # (Phase advances via process_selection)

        elif current_phase == SimulationPhase.MORNING_AUCTION:
            # Execute pending buys
            await self._execute_morning_auction()
            self._phase = SimulationPhase.TRADING_HOURS
            self._clock.advance_to_time(time(9, 30))

        elif current_phase == SimulationPhase.TRADING_HOURS:
            # Advance to market close
            self._clock.advance_to_time(time(15, 0))
            await self._update_closing_prices()
            self._phase = SimulationPhase.MARKET_CLOSE

        elif current_phase == SimulationPhase.MARKET_CLOSE:
            # Check if more days to simulate
            if self._current_day < self._settings.num_days:
                # Move to next day
                self._clock.advance_to_next_day()
                self._current_day += 1

                # Check if there are holdings to confirm
                if self._position_manager.get_filled_slots():
                    self._phase = SimulationPhase.MORNING_CONFIRMATION
                    self._clock.advance_to_time(time(9, 0))
                else:
                    self._phase = SimulationPhase.PREMARKET_ANALYSIS
            else:
                # Simulation complete
                self._phase = SimulationPhase.COMPLETED

        elif current_phase == SimulationPhase.MORNING_CONFIRMATION:
            # After confirmation, go to premarket for new signals
            self._phase = SimulationPhase.PREMARKET_ANALYSIS
            self._clock.advance_to_time(time(8, 30))
            await self._generate_premarket_signals()

        self._add_message(f"Phase: {self._phase.value}")
        return self._phase

    async def process_selection(self, selected_indices: list[int]) -> None:
        """
        Process user's selection of signals.

        Args:
            selected_indices: 1-based indices of selected signals.
        """
        if self._phase != SimulationPhase.PREMARKET_ANALYSIS or not self._clock:
            return

        # Filter selected signals
        self._selected_signals = [s for s in self._pending_signals if s.index in selected_indices]

        if not self._selected_signals:
            self._add_message("No signals selected, skipping to next phase")
            self._pending_signals = []
            self._phase = SimulationPhase.TRADING_HOURS
            self._clock.advance_to_time(time(9, 30))
            return

        self._add_message(f"Selected {len(self._selected_signals)} signal(s)")

        # Allocate slots for selected signals
        await self._allocate_selected_signals()

        # Move to morning auction
        self._pending_signals = []
        self._phase = SimulationPhase.MORNING_AUCTION
        self._clock.advance_to_time(time(9, 25))

    async def process_sell_decision(self, slots_to_sell: list[int]) -> None:
        """
        Process user's sell decision for morning confirmation.

        Args:
            slots_to_sell: List of slot IDs to sell.
        """
        if self._phase != SimulationPhase.MORNING_CONFIRMATION:
            return

        if not self._position_manager or not self._price_service or not self._clock:
            return

        if not slots_to_sell:
            self._add_message("Holding all positions")
            return

        # Get current prices and sell
        for slot_id in slots_to_sell:
            slot = self._position_manager.get_slot(slot_id)
            if slot and slot.holdings:
                prices = {}
                for h in slot.holdings:
                    price = await self._price_service.get_price_at_time(
                        h.stock_code, self._clock.current_time
                    )
                    if price:
                        prices[h.stock_code] = price

                pnl = self._position_manager.release_slot(slot_id, prices, self._clock.current_time)
                self._add_message(f"Sold slot {slot_id}, P&L: {pnl:+,.0f}")

    def get_result(self) -> SimulationResult | None:
        """
        Get final simulation result.

        Returns:
            SimulationResult if completed, None otherwise.
        """
        if not self._position_manager or not self._settings:
            return None

        # Get final prices for open positions
        prices = {}
        for h in self._position_manager.get_all_holdings():
            if h.current_price:
                prices[h.stock_code] = h.current_price

        return self._position_manager.calculate_result(
            start_date=self._settings.start_date,
            end_date=self._clock.current_date if self._clock else self._settings.start_date,
            current_prices=prices,
        )

    # === Private methods ===

    async def _generate_premarket_signals(self) -> None:
        """Generate signals from premarket messages."""
        if not self._hist_message_reader or not self._clock:
            return

        self._pending_signals = []

        # Get positive messages for premarket
        messages = await self._hist_message_reader.get_premarket_messages(
            trade_date=self._clock.current_time,
            only_positive=True,
            limit=50,
        )

        if not messages:
            self._add_message("No positive messages found for premarket")
            return

        # Convert messages to pending signals
        for i, msg in enumerate(messages, 1):
            if not msg.analysis:
                continue

            signal = PendingSignal(
                index=i,
                signal_type="buy_sector" if msg.analysis.affected_stocks else "buy_stock",
                sentiment=msg.analysis.sentiment.value,
                confidence=msg.analysis.confidence,
                target_stocks=msg.analysis.affected_stocks or msg.stock_codes,
                target_sectors=[],  # Could extract from analysis
                title=msg.title[:100] if msg.title else "",
                reasoning=msg.analysis.reasoning[:200] if msg.analysis.reasoning else "",
                message_id=msg.id,
            )
            self._pending_signals.append(signal)

            # Limit to reasonable number
            if len(self._pending_signals) >= 10:
                break

        self._add_message(f"Found {len(self._pending_signals)} premarket signals")

    async def _allocate_selected_signals(self) -> None:
        """Allocate slots for selected signals."""
        if not self._position_manager or not self._price_service or not self._clock:
            return

        for signal in self._selected_signals:
            slot = self._position_manager.get_available_slot(SlotType.PREMARKET)
            if not slot:
                self._add_message("No more slots available")
                break

            # Get stocks and prices
            stocks_to_buy = []
            for code in signal.target_stocks[:3]:  # Max 3 stocks per signal
                # Normalize code if needed
                if "." not in code:
                    code = f"{code}.SH" if code.startswith("6") else f"{code}.SZ"

                price = await self._price_service.get_price_at_time(code, self._clock.current_time)

                if price:
                    stocks_to_buy.append((code, "", price))

            if not stocks_to_buy:
                continue

            # Allocate slot
            if len(stocks_to_buy) == 1:
                code, name, price = stocks_to_buy[0]
                self._position_manager.allocate_slot(
                    slot,
                    code,
                    price,
                    signal.reasoning,
                    timestamp=self._clock.current_time,
                )
            else:
                self._position_manager.allocate_slot_sector(
                    slot,
                    stocks_to_buy,
                    sector_name=signal.title[:30],
                    reason=signal.reasoning,
                    timestamp=self._clock.current_time,
                )

    async def _execute_morning_auction(self) -> None:
        """Execute buy orders at morning auction."""
        if not self._position_manager or not self._price_service or not self._clock:
            return

        for slot in self._position_manager._slots:
            if slot.state != "pending_buy":
                continue

            # Check for limit-up and get fill prices
            fill_prices = {}
            skip_slot = False

            for holding in slot.holdings:
                is_limit_up = await self._price_service.is_limit_up_at_open(
                    holding.stock_code, self._clock.current_date
                )

                if is_limit_up:
                    self._add_message(f"Skipped {holding.stock_code} - opened at limit-up")
                    skip_slot = True
                    break

                # Get open price
                daily = await self._price_service.get_daily_data(
                    holding.stock_code, self._clock.current_date
                )
                if daily:
                    fill_prices[holding.stock_code] = daily.open

            if skip_slot:
                slot.reset()
                continue

            # Fill the slot
            self._position_manager.fill_slot(slot.slot_id, fill_prices, self._clock.current_time)

            for h in slot.holdings:
                price = fill_prices.get(h.stock_code, h.entry_price)
                self._add_message(f"Bought {h.stock_code} x{h.quantity} @ {price:.2f}")

    async def _update_closing_prices(self) -> None:
        """Update holdings with closing prices."""
        if not self._position_manager or not self._price_service or not self._clock:
            return

        prices = {}
        for holding in self._position_manager.get_all_holdings():
            daily = await self._price_service.get_daily_data(
                holding.stock_code, self._clock.current_date
            )
            if daily:
                prices[holding.stock_code] = daily.close

        self._position_manager.update_holding_prices(prices)

        # Calculate day P&L
        holdings = self._position_manager.get_all_holdings()
        day_pnl = 0.0
        for h in holdings:
            if h.current_price and h.entry_price:
                day_pnl += h.quantity * (h.current_price - h.entry_price)

        self._add_message(f"Day {self._current_day} closed. P&L: {day_pnl:+,.0f}")

    def _add_message(self, msg: str) -> None:
        """Add a message to the log."""
        timestamp = self._clock.get_time_string() if self._clock else ""
        full_msg = f"[{timestamp}] {msg}" if timestamp else msg
        self._messages.append(full_msg)
        logger.info(msg)


# Singleton instance for web API
_simulation_manager: SimulationManager | None = None


def get_simulation_manager() -> SimulationManager:
    """Get or create the simulation manager singleton."""
    global _simulation_manager
    if _simulation_manager is None:
        _simulation_manager = SimulationManager()
    return _simulation_manager


async def reset_simulation_manager() -> None:
    """Reset the simulation manager."""
    global _simulation_manager
    if _simulation_manager:
        await _simulation_manager.cleanup()
    _simulation_manager = SimulationManager()
