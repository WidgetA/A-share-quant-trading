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
from src.data.sources.sector_mapper import SectorMapper
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
from src.trading.repository import TradingRepository, create_trading_repository_from_config

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
        self._trading_repo: TradingRepository | None = None
        self._sector_mapper: SectorMapper | None = None

        # Simulation state
        self._phase = SimulationPhase.NOT_STARTED
        self._current_day = 0
        self._pending_signals: list[PendingSignal] = []
        self._selected_signals: list[PendingSignal] = []
        self._intraday_signals: list[PendingSignal] = []
        self._messages: list[str] = []  # Log messages for UI
        self._is_synced: bool = False
        self._pending_close_sell_done: bool = False  # Track if sell decision made at close

        # Track what we've processed
        self._last_message_time: datetime | None = None
        self._last_intraday_check: datetime | None = None

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

        # Initialize sector mapper for stock names
        self._sector_mapper = SectorMapper()
        try:
            await self._sector_mapper.load_sector_data()
            logger.info("Sector mapper loaded successfully")
        except Exception as e:
            logger.warning(f"Failed to load sector mapper: {e}")
            # Continue without stock names

        # Load initial holdings if specified
        if settings.load_holdings_from_date:
            await self._load_initial_holdings()

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
        if self._trading_repo:
            await self._trading_repo.close()
            self._trading_repo = None
        if self._sector_mapper:
            await self._sector_mapper.close()
            self._sector_mapper = None

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

        # Get pending holdings for sell decision (at close or morning confirmation)
        pending_holdings = []
        if self._phase in (
            SimulationPhase.MARKET_CLOSE,
            SimulationPhase.MORNING_CONFIRMATION,
        ):
            pending_holdings = holdings.copy()

        return SimulationState(
            phase=self._phase,
            sim_date=self._clock.current_date,
            sim_time=self._clock.current_time,
            day_number=self._current_day,
            total_days=self._settings.num_days if self._settings else 0,
            pending_signals=self._pending_signals,
            pending_holdings=pending_holdings,
            intraday_signals=self._intraday_signals,
            holdings=holdings,
            transactions=self._position_manager.transactions,
            initial_capital=self._position_manager.total_capital,
            available_cash=self._position_manager.available_cash,
            messages=self._messages[-10:],  # Last 10 messages
            can_sync_to_db=(self._phase == SimulationPhase.COMPLETED),
            is_synced=self._is_synced,
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
            # Check for intraday messages first
            if not self._intraday_signals:
                await self._check_intraday_messages()
                if self._intraday_signals:
                    self._add_message(f"发现 {len(self._intraday_signals)} 条盘中消息")
                    return self._phase  # Stay in trading hours, let user review

            # No intraday messages or user skipped, advance to market close
            self._intraday_signals = []
            self._clock.advance_to_time(time(15, 0))
            await self._update_closing_prices()
            self._phase = SimulationPhase.MARKET_CLOSE

        elif current_phase == SimulationPhase.MARKET_CLOSE:
            # At market close, check if there are holdings to sell
            # Strategy: 持仓不过夜 (don't hold overnight)
            filled_slots = self._position_manager.get_filled_slots()
            if filled_slots and not self._pending_close_sell_done:
                # Stay at market close, let user decide on selling
                # pending_holdings will be populated in get_state()
                self._add_message("收盘，请决定是否卖出当前持仓")
                return self._phase

            # After sell decision or no holdings, proceed
            self._pending_close_sell_done = False  # Reset for next day

            if self._current_day < self._settings.num_days:
                # Move to next day
                self._clock.advance_to_next_day()
                self._current_day += 1
                self._phase = SimulationPhase.PREMARKET_ANALYSIS
                self._clock.advance_to_time(time(8, 30))
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
        Process user's sell decision at market close or morning confirmation.

        Args:
            slots_to_sell: List of slot IDs to sell.
        """
        # Allow sell at both MARKET_CLOSE and MORNING_CONFIRMATION
        if self._phase not in (
            SimulationPhase.MARKET_CLOSE,
            SimulationPhase.MORNING_CONFIRMATION,
        ):
            return

        if not self._position_manager or not self._price_service or not self._clock:
            return

        # Mark that sell decision is done at close
        if self._phase == SimulationPhase.MARKET_CLOSE:
            self._pending_close_sell_done = True

        if not slots_to_sell:
            self._add_message("保留所有持仓")
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
                self._add_message(f"卖出仓位 {slot_id}, 盈亏: {pnl:+,.0f}")

    async def process_intraday_selection(self, selected_indices: list[int]) -> None:
        """
        Process user's intraday signal selection.

        Args:
            selected_indices: 1-based indices of selected intraday signals.
        """
        if self._phase != SimulationPhase.TRADING_HOURS:
            return

        if not self._position_manager or not self._price_service or not self._clock:
            return

        # Filter selected signals
        selected = [s for s in self._intraday_signals if s.index in selected_indices]

        if not selected:
            self._add_message("未选择盘中信号")
            self._intraday_signals = []
            return

        self._add_message(f"选择了 {len(selected)} 个盘中信号")

        # Allocate intraday slots for selected signals
        for signal in selected:
            slot = self._position_manager.get_available_slot(SlotType.INTRADAY)
            if not slot:
                self._add_message("无可用盘中仓位")
                break

            # Get stocks and prices
            stocks_to_buy = []
            for code in signal.target_stocks[:3]:
                clean_code = code.split(".")[0] if "." in code else code
                if "." not in code:
                    code = f"{code}.SH" if code.startswith("6") else f"{code}.SZ"

                price = await self._price_service.get_price_at_time(code, self._clock.current_time)

                if price:
                    name = signal.target_stock_names.get(clean_code, "")
                    name = name or self._get_stock_name(code)
                    stocks_to_buy.append((code, name, price))

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
                    stock_name=name,
                    timestamp=self._clock.current_time,
                )
                # Fill immediately for intraday
                self._position_manager.fill_slot(
                    slot.slot_id, {code: price}, self._clock.current_time
                )
                self._add_message(f"盘中买入 {code} {name} @ {price:.2f}")
            else:
                self._position_manager.allocate_slot_sector(
                    slot,
                    stocks_to_buy,
                    sector_name=signal.title[:30],
                    reason=signal.reasoning,
                    timestamp=self._clock.current_time,
                )
                fill_prices = {s[0]: s[2] for s in stocks_to_buy}
                self._position_manager.fill_slot(
                    slot.slot_id, fill_prices, self._clock.current_time
                )
                self._add_message(f"盘中买入 {signal.title[:20]}")

        self._intraday_signals = []

    async def skip_intraday(self) -> None:
        """Skip intraday messages and continue to market close."""
        if self._phase != SimulationPhase.TRADING_HOURS:
            return

        self._add_message("跳过盘中消息")
        self._intraday_signals = []

    async def buy_from_messages(self, messages: list[dict]) -> dict[str, int]:
        """
        Buy stocks from selected messages (from messages viewer modal).

        Args:
            messages: List of message dicts with id, stocks, title, sentiment.

        Returns:
            Dict with processed count.
        """
        if not self._position_manager or not self._price_service or not self._clock:
            raise RuntimeError("Simulation not properly initialized")

        # Only allow buying in premarket or trading hours
        if self._phase not in (
            SimulationPhase.PREMARKET_ANALYSIS,
            SimulationPhase.TRADING_HOURS,
        ):
            raise RuntimeError("只能在盘前分析或交易时段买入")

        processed = 0
        slot_type = (
            SlotType.PREMARKET
            if self._phase == SimulationPhase.PREMARKET_ANALYSIS
            else SlotType.INTRADAY
        )

        for msg_data in messages:
            stock_codes = msg_data.get("stocks", [])
            title = msg_data.get("title", "")[:50]

            if not stock_codes:
                continue

            # Get available slot
            slot = self._position_manager.get_available_slot(slot_type)
            if not slot:
                self._add_message("无可用仓位，无法继续买入")
                break

            # Get prices for stocks
            stocks_to_buy = []
            for code in stock_codes[:3]:  # Max 3 stocks per message
                if "." not in code:
                    code = f"{code}.SH" if code.startswith("6") else f"{code}.SZ"

                price = await self._price_service.get_price_at_time(code, self._clock.current_time)
                if price:
                    name = self._get_stock_name(code)
                    stocks_to_buy.append((code, name, price))

            if not stocks_to_buy:
                continue

            # Allocate and fill slot
            if len(stocks_to_buy) == 1:
                code, name, price = stocks_to_buy[0]
                self._position_manager.allocate_slot(
                    slot,
                    code,
                    price,
                    f"消息: {title}",
                    stock_name=name,
                    timestamp=self._clock.current_time,
                )
                self._position_manager.fill_slot(
                    slot.slot_id, {code: price}, self._clock.current_time
                )
                self._add_message(f"买入 {code} {name} @ {price:.2f}")
            else:
                self._position_manager.allocate_slot_sector(
                    slot,
                    stocks_to_buy,
                    sector_name=title,
                    reason=f"消息选择: {title}",
                    timestamp=self._clock.current_time,
                )
                fill_prices = {s[0]: s[2] for s in stocks_to_buy}
                self._position_manager.fill_slot(
                    slot.slot_id, fill_prices, self._clock.current_time
                )
                self._add_message(f"买入 {title}")

            processed += 1

        # After buying from messages, clear pending signals and advance phase
        if processed > 0:
            self._pending_signals = []
            if self._phase == SimulationPhase.PREMARKET_ANALYSIS:
                # Skip to trading hours since buys are already executed
                self._phase = SimulationPhase.TRADING_HOURS
                self._clock.advance_to_time(time(9, 30))
                self._add_message("已完成买入，进入交易时段")

        return {"processed": processed}

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

    async def sync_to_database(self) -> dict[str, int]:
        """
        Sync simulation results to trading database.

        Returns:
            Dict with synced_slots and synced_holdings counts.

        Raises:
            RuntimeError: If simulation not completed or sync fails.
        """
        if self._phase != SimulationPhase.COMPLETED:
            raise RuntimeError("只能在模拟完成后同步到数据库")

        if not self._position_manager:
            raise RuntimeError("Position manager not available")

        # Connect to trading repository if not already connected
        if not self._trading_repo:
            self._trading_repo = create_trading_repository_from_config()
            await self._trading_repo.connect()

        try:
            synced_slots = 0
            synced_holdings = 0

            # Sync each slot
            for slot in self._position_manager._slots:
                # Save slot data
                slot_data = {
                    "slot_id": slot.slot_id,
                    "slot_type": slot.slot_type,
                    "state": slot.state,
                    "entry_time": slot.holdings[0].entry_time if slot.holdings else None,
                    "entry_reason": slot.entry_reason,
                    "sector_name": slot.sector_name,
                }
                await self._trading_repo.save_slot(slot_data)

                # Save holdings if slot is filled
                if slot.state == "filled" and slot.holdings:
                    holdings_data = []
                    for h in slot.holdings:
                        holdings_data.append(
                            {
                                "stock_code": h.stock_code,
                                "stock_name": h.stock_name,
                                "quantity": h.quantity,
                                "entry_price": h.entry_price,
                            }
                        )
                    await self._trading_repo.save_holdings(slot.slot_id, holdings_data)
                    synced_holdings += len(holdings_data)
                    synced_slots += 1
                else:
                    # Clear holdings for empty slots
                    await self._trading_repo.save_holdings(slot.slot_id, [])

            self._is_synced = True
            self._add_message(f"已同步到数据库：{synced_slots} 个仓位，{synced_holdings} 只股票")

            logger.info(f"Synced to database: {synced_slots} slots, {synced_holdings} holdings")

            return {
                "synced_slots": synced_slots,
                "synced_holdings": synced_holdings,
            }

        except Exception as e:
            logger.error(f"Failed to sync to database: {e}")
            raise RuntimeError(f"同步失败: {e}")

    # === Private methods ===

    async def _load_initial_holdings(self) -> None:
        """
        Load initial holdings from the trading database.

        Loads the current positions from trading.position_slots and
        trading.stock_holdings tables into the simulation position manager.
        """
        if not self._position_manager:
            return

        try:
            # Connect to trading database
            self._trading_repo = create_trading_repository_from_config()
            await self._trading_repo.connect()

            # Load slots and holdings
            slots_data = await self._trading_repo.get_all_slots()
            holdings_data = await self._trading_repo.get_all_holdings()

            if not slots_data:
                self._add_message("No position data found in database")
                return

            # Load into simulation position manager
            filled_count, holdings_value = self._position_manager.load_holdings(
                slots_data, holdings_data
            )

            if filled_count > 0:
                self._add_message(
                    f"Loaded {filled_count} position(s), total value: {holdings_value:,.0f}"
                )

                # Log details of loaded positions
                for slot in self._position_manager._slots:
                    if slot.state == "filled" and slot.holdings:
                        if slot.sector_name:
                            self._add_message(
                                f"  Slot {slot.slot_id} ({slot.sector_name}): "
                                f"{len(slot.holdings)} stocks"
                            )
                        else:
                            h = slot.holdings[0]
                            self._add_message(
                                f"  Slot {slot.slot_id}: {h.stock_code} {h.stock_name} "
                                f"x{h.quantity} @ {h.entry_price:.2f}"
                            )
            else:
                self._add_message("No filled positions to load")

            logger.info(f"Loaded {filled_count} holdings from database")

        except Exception as e:
            logger.error(f"Failed to load initial holdings: {e}")
            self._add_message(f"Warning: Failed to load holdings - {e}")

    def _get_stock_name(self, code: str) -> str:
        """Get stock name for a code, or return empty string if not found."""
        if not self._sector_mapper:
            return ""
        # Remove suffix if present
        clean_code = code.split(".")[0] if "." in code else code
        name = self._sector_mapper.get_stock_name(clean_code)
        return name or ""

    def _get_stock_names_dict(self, codes: list[str]) -> dict[str, str]:
        """Get stock names for a list of codes."""
        result = {}
        for code in codes:
            clean_code = code.split(".")[0] if "." in code else code
            result[clean_code] = self._get_stock_name(code)
        return result

    async def _generate_premarket_signals(self) -> None:
        """Generate signals from premarket messages (all messages, not just positive)."""
        if not self._hist_message_reader or not self._clock:
            return

        self._pending_signals = []

        # Get ALL messages for premarket (not just positive), let user decide
        messages = await self._hist_message_reader.get_premarket_messages(
            trade_date=self._clock.current_time,
            only_positive=False,  # Get all messages for human review
            limit=100,
        )

        if not messages:
            self._add_message("当日无消息")
            return

        # Convert messages to pending signals
        positive_count = 0
        for i, msg in enumerate(messages, 1):
            # Include messages with or without analysis
            sentiment = "unknown"
            confidence = 0.0
            reasoning = ""
            target_stocks = msg.stock_codes or []

            if msg.analysis:
                sentiment = msg.analysis.sentiment.value
                confidence = msg.analysis.confidence
                reasoning = msg.analysis.reasoning[:200] if msg.analysis.reasoning else ""
                target_stocks = msg.analysis.affected_stocks or msg.stock_codes

            # Track positive count
            if sentiment in ("strong_bullish", "bullish"):
                positive_count += 1

            stock_names = self._get_stock_names_dict(target_stocks)

            signal = PendingSignal(
                index=i,
                signal_type="buy_sector" if len(target_stocks) > 1 else "buy_stock",
                sentiment=sentiment,
                confidence=confidence,
                target_stocks=target_stocks,
                target_stock_names=stock_names,
                target_sectors=[],
                title=msg.title[:100] if msg.title else "",
                reasoning=reasoning,
                message_id=msg.id,
            )
            self._pending_signals.append(signal)

            # Limit to reasonable number
            if len(self._pending_signals) >= 20:
                break

        msg_count = len(self._pending_signals)
        self._add_message(f"找到 {msg_count} 条消息 (其中 {positive_count} 条正面)")

    async def _check_intraday_messages(self) -> None:
        """Check for intraday messages during trading hours."""
        if not self._hist_message_reader or not self._clock:
            return

        self._intraday_signals = []

        # Set the time range for intraday messages
        since = self._last_intraday_check or datetime.combine(self._clock.current_date, time(9, 30))

        # Get intraday messages
        messages = await self._hist_message_reader.get_intraday_messages(
            since=since,
            only_positive=False,
            limit=50,
        )

        self._last_intraday_check = self._clock.current_time

        if not messages:
            return

        # Convert messages to pending signals
        positive_count = 0
        for i, msg in enumerate(messages, 1):
            sentiment = "unknown"
            confidence = 0.0
            reasoning = ""
            target_stocks = msg.stock_codes or []

            if msg.analysis:
                sentiment = msg.analysis.sentiment.value
                confidence = msg.analysis.confidence
                reasoning = msg.analysis.reasoning[:200] if msg.analysis.reasoning else ""
                target_stocks = msg.analysis.affected_stocks or msg.stock_codes

            if sentiment in ("strong_bullish", "bullish"):
                positive_count += 1

            stock_names = self._get_stock_names_dict(target_stocks)

            signal = PendingSignal(
                index=i,
                signal_type="buy_sector" if len(target_stocks) > 1 else "buy_stock",
                sentiment=sentiment,
                confidence=confidence,
                target_stocks=target_stocks,
                target_stock_names=stock_names,
                target_sectors=[],
                title=msg.title[:100] if msg.title else "",
                reasoning=reasoning,
                message_id=msg.id,
            )
            self._intraday_signals.append(signal)

            if len(self._intraday_signals) >= 10:
                break

    async def _allocate_selected_signals(self) -> None:
        """Allocate slots for selected signals."""
        if not self._position_manager or not self._price_service or not self._clock:
            return

        for signal in self._selected_signals:
            slot = self._position_manager.get_available_slot(SlotType.PREMARKET)
            if not slot:
                self._add_message("无可用仓位")
                break

            # Get stocks and prices
            stocks_to_buy = []
            for code in signal.target_stocks[:3]:  # Max 3 stocks per signal
                # Normalize code if needed
                clean_code = code.split(".")[0] if "." in code else code
                if "." not in code:
                    code = f"{code}.SH" if code.startswith("6") else f"{code}.SZ"

                price = await self._price_service.get_price_at_time(code, self._clock.current_time)

                if price:
                    # Get stock name from signal or lookup
                    name = signal.target_stock_names.get(clean_code, "")
                    name = name or self._get_stock_name(code)
                    stocks_to_buy.append((code, name, price))

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
                    stock_name=name,
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
