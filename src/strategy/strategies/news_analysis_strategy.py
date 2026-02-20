# === MODULE PURPOSE ===
# News-driven trading strategy using pre-analyzed messages from database.
# Analysis is done by external message collection project - this strategy
# only filters and acts on the results.

# === WORKFLOW ===
# 1. PRE_MARKET (8:30): Filter positive messages, present to user, save pending orders
# 2. MORNING_AUCTION (9:25-9:30): Check real-time prices, skip limit-up stocks, execute buys
# 3. MORNING/AFTERNOON: Monitor new positive messages, confirm intraday buys
# 4. AFTER_HOURS (15:00): Record holdings for next day
# 5. NEXT PRE_MARKET (9:00): Confirm sell/hold decisions

# === DEPENDENCIES ===
# - stock_filter: For exchange filtering
# - sector_mapper: For sector resolution
# - news_analyzer: Filters pre-analyzed messages
# - position_manager: Manages capital slots
# - holding_tracker: Tracks overnight positions
# - user_interaction: Gets user confirmations

# === KEY CHANGE ===
# No LLM calls needed - analysis results come from message_analysis table
# via MessageReader JOIN

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, AsyncIterator

from src.common.scheduler import MarketSession
from src.common.user_interaction import InteractionConfig, UserInteraction
from src.data.sources.sector_mapper import SectorMapper
from src.strategy.analyzers.news_analyzer import NewsAnalyzer, NewsAnalyzerConfig
from src.strategy.base import BaseStrategy, StrategyContext
from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig
from src.strategy.signals import SignalType, TradingSignal
from src.trading.holding_tracker import HoldingTracker
from src.trading.position_manager import PositionConfig, PositionManager, SlotType

logger = logging.getLogger(__name__)


@dataclass
class PendingPremarketOrder:
    """
    Represents a pending order from premarket analysis.

    These orders are created during premarket analysis (8:30) and
    executed during morning auction (9:25-9:30) after checking
    real-time prices for limit-up conditions.
    """

    slot_id: int
    stock_codes: list[str]  # Candidate stocks for this order
    stock_names: dict[str, str]  # code -> name mapping
    reason: str
    news_id: str
    signal_type: str
    confidence: float
    sector_name: str = ""


class NewsAnalysisStrategy(BaseStrategy):
    """
    News-driven trading strategy using pre-analyzed messages.

    This strategy uses analysis results from external message collection
    project (stored in message_analysis table). It supports:
    - Premarket filtering of positive messages (8:30 AM)
    - Intraday real-time monitoring
    - Overnight position tracking
    - Morning sell confirmations

    Key difference from previous version:
    - No LLM calls - analysis is already done by external project
    - Messages come with sentiment, confidence, reasoning pre-populated
    - Much faster execution (no API calls needed)

    Configuration (YAML):
        strategies:
          news_analysis:
            enabled: true
            parameters:
              min_confidence: 0.7
              total_capital: 10000000
              premarket_slots: 3
              intraday_slots: 2
              premarket_analysis_time: "08:30"
              morning_confirmation_time: "09:00"
              exclude_bse: true
              exclude_chinext: true
              exclude_star: false
    """

    @property
    def strategy_name(self) -> str:
        return "news_analysis"

    async def on_load(self) -> None:
        """Initialize all components."""
        await super().on_load()

        # Initialize stock filter
        filter_config = StockFilterConfig(
            exclude_bse=self.get_parameter("exclude_bse", True),
            exclude_chinext=self.get_parameter("exclude_chinext", True),
            exclude_star=self.get_parameter("exclude_star", False),
        )
        self._stock_filter = StockFilter(filter_config)

        # Initialize sector mapper
        self._sector_mapper = SectorMapper()
        # Note: Sector data will be loaded lazily on first use

        # Initialize news analyzer (no LLM needed - uses pre-analyzed data)
        analyzer_config = NewsAnalyzerConfig(
            min_confidence=self.get_parameter("min_confidence", 0.7),
            max_stocks_per_sector=self.get_parameter("max_stocks_per_sector", 5),
            actionable_sentiments=self.get_parameter(
                "actionable_sentiments", ["strong_bullish", "bullish"]
            ),
        )
        self._news_analyzer = NewsAnalyzer(
            stock_filter=self._stock_filter,
            sector_mapper=self._sector_mapper,
            config=analyzer_config,
        )

        # Initialize position manager with state persistence
        position_config = PositionConfig(
            total_capital=self.get_parameter("total_capital", 10_000_000.0),
            premarket_slots=self.get_parameter("premarket_slots", 3),
            intraday_slots=self.get_parameter("intraday_slots", 2),
        )
        self._position_manager = PositionManager(position_config)

        # Note: Database connection will be set up in on_load() (async)
        # For now, try to load from file as fallback
        state_file = self.get_parameter("position_state_file", "data/position_state.json")
        if self._position_manager.load_from_file(state_file):
            holdings = self._position_manager.get_holdings()
            if holdings:
                logger.info(f"Loaded {len(holdings)} existing positions from state file")

        # Initialize holding tracker
        self._holding_tracker = HoldingTracker()

        # Initialize user interaction
        interaction_config = InteractionConfig(
            premarket_timeout=self.get_parameter("premarket_timeout", 300.0),
            intraday_timeout=self.get_parameter("intraday_timeout", 60.0),
            morning_timeout=self.get_parameter("morning_timeout", 300.0),
        )
        self._user_interaction = UserInteraction(interaction_config)

        # State tracking
        self._last_premarket_analysis: datetime | None = None
        self._last_morning_confirmation: datetime | None = None
        self._last_morning_auction: datetime | None = None
        self._last_message_check: datetime | None = None
        self._processed_message_ids: set[str] = set()

        # Pending orders from premarket analysis (to be executed at morning auction)
        self._pending_premarket_orders: list[PendingPremarketOrder] = []

        # Try to connect to trading database for position persistence
        try:
            from src.trading.repository import create_trading_repository_from_config

            trading_repo = create_trading_repository_from_config()
            await trading_repo.connect()
            self._position_manager.set_repository(trading_repo)
            await self._position_manager.load_from_db()
            logger.info("PositionManager connected to trading database")
        except Exception:
            logger.error("Failed to connect to trading database")
            raise

        logger.info(f"NewsAnalysisStrategy loaded with config: {position_config}")

    async def on_unload(self) -> None:
        """Clean up resources."""
        if hasattr(self, "_sector_mapper") and self._sector_mapper:
            await self._sector_mapper.close()

        await super().on_unload()
        logger.info("NewsAnalysisStrategy unloaded")

    async def _save_position_state(self) -> None:
        """
        Save position state to database or file.

        Uses database if repository is connected, otherwise falls back to file.
        """
        if self._position_manager.has_repository:
            try:
                await self._position_manager.save_to_db()
                logger.debug("Position state saved to database")
            except Exception as e:
                logger.error(f"Failed to save to database: {e}, falling back to file")
                state_file = self.get_parameter("position_state_file", "data/position_state.json")
                self._position_manager.save_to_file(state_file)
        else:
            state_file = self.get_parameter("position_state_file", "data/position_state.json")
            self._position_manager.save_to_file(state_file)

    async def generate_signals(
        self,
        context: StrategyContext,
    ) -> AsyncIterator[TradingSignal]:
        """
        Generate trading signals based on session and news.

        Behavior varies by market session:
            PRE_MARKET: Analyze overnight news, user selects stocks
            MORNING/AFTERNOON: Monitor new news, confirm intraday buys
            AFTER_HOURS: Record holdings
        """
        session = context.metadata.get("session", MarketSession.CLOSED)

        # Handle different sessions
        if session == MarketSession.PRE_MARKET:
            # Morning confirmation for overnight holdings
            async for signal in self._handle_morning_confirmation(context):
                yield signal

            # Premarket analysis (only saves pending orders, does not execute)
            await self._handle_premarket(context)

        elif session == MarketSession.MORNING_AUCTION:
            # Execute pending premarket orders after checking real-time prices
            async for signal in self._handle_morning_auction(context):
                yield signal

        elif session in (MarketSession.MORNING, MarketSession.AFTERNOON):
            # Also check for pending orders at start of morning session
            # (in case morning auction was missed)
            if self._pending_premarket_orders:
                async for signal in self._handle_morning_auction(context):
                    yield signal

            # Intraday monitoring
            async for signal in self._handle_trading_hours(context):
                yield signal

        elif session == MarketSession.AFTER_HOURS:
            # Record holdings for next day
            await self._handle_after_hours(context)

    async def _handle_morning_confirmation(
        self,
        context: StrategyContext,
    ) -> AsyncIterator[TradingSignal]:
        """Handle morning sell confirmation for overnight holdings."""
        # Check if it's confirmation time (around 9:00)
        confirmation_time = self.get_parameter("morning_confirmation_time", "09:00")

        # Only run once per day
        if self._last_morning_confirmation:
            if self._last_morning_confirmation.date() == context.timestamp.date():
                return

        # Check time window (confirmation_time - 5min to confirmation_time + 10min)
        conf_hour, conf_minute = map(int, confirmation_time.split(":"))
        current_minutes = context.timestamp.hour * 60 + context.timestamp.minute
        conf_minutes = conf_hour * 60 + conf_minute

        if not (conf_minutes - 5 <= current_minutes <= conf_minutes + 10):
            return

        # Get overnight holdings
        holdings = self._holding_tracker.get_holdings()
        if not holdings:
            self._last_morning_confirmation = context.timestamp
            return

        logger.info(f"Starting morning confirmation for {len(holdings)} holdings")

        # Get user confirmation
        holdings_to_review = await self._holding_tracker.get_morning_review()
        slots_to_sell = await self._user_interaction.morning_confirmation(holdings_to_review)

        # Generate sell signals
        if slots_to_sell:
            signals = await self._holding_tracker.generate_sell_signals(
                slots_to_sell=slots_to_sell,
                strategy_name=self.strategy_name,
            )

            for signal in signals:
                yield signal

                # Release the slot
                self._position_manager.release_slot(signal.metadata["slot_id"])

        self._last_morning_confirmation = context.timestamp
        self._holding_tracker.clear()

    async def _handle_premarket(
        self,
        context: StrategyContext,
    ) -> None:
        """
        Handle premarket analysis at configured time (default 8:30 AM).

        This function only analyzes news and saves pending orders.
        Actual execution happens in _handle_morning_auction after
        checking real-time prices for limit-up conditions.

        IMPORTANT: Must wait for morning_confirmation to complete first
        if there are overnight holdings. This ensures we know how many
        slots are available before selecting new stocks.
        """
        # Only run once per day
        if self._last_premarket_analysis:
            if self._last_premarket_analysis.date() == context.timestamp.date():
                return

        # CRITICAL: If there are overnight holdings, wait for morning_confirmation first
        # This ensures we know how many slots will be freed before selecting new stocks
        holdings = self._holding_tracker.get_holdings()
        if holdings:
            # Check if morning confirmation has been done today
            if not self._last_morning_confirmation:
                logger.debug(
                    f"Waiting for morning confirmation: {len(holdings)} overnight holdings"
                )
                return
            if self._last_morning_confirmation.date() != context.timestamp.date():
                logger.debug(
                    f"Waiting for morning confirmation: {len(holdings)} overnight holdings"
                )
                return

        # Check if it's analysis time (extended window to 09:15 to run after confirmation)
        analysis_time = self.get_parameter("premarket_analysis_time", "08:30")
        analysis_hour, analysis_minute = map(int, analysis_time.split(":"))
        current_minutes = context.timestamp.hour * 60 + context.timestamp.minute
        analysis_minutes = analysis_hour * 60 + analysis_minute

        # Allow premarket analysis from 08:30 to 09:15 (after morning confirmation at 09:00)
        if not (analysis_minutes <= current_minutes <= analysis_minutes + 45):
            return

        logger.info("Starting premarket news analysis")

        # Check if MessageReader is available
        if not context.has_message_reader:
            logger.warning("MessageReader not available, skipping premarket analysis")
            self._last_premarket_analysis = context.timestamp
            return

        # Ensure sector data is loaded
        if not self._sector_mapper.is_loaded:
            try:
                await self._sector_mapper.load_sector_data()
            except Exception:
                logger.error("Failed to load sector data")
                raise

        # Get POSITIVE messages since last close (already analyzed by external project)
        # Using get_positive_messages_since to only fetch bullish/strong_bullish
        last_close = self._get_last_close_time(context.timestamp)
        messages = await context.get_positive_messages_since(last_close)

        if not messages:
            logger.info("No positive messages for premarket analysis")
            self._last_premarket_analysis = context.timestamp
            return

        logger.info(f"Found {len(messages)} positive messages for premarket")

        # Filter and convert to signals (no LLM needed - already analyzed)
        signals = self._news_analyzer.filter_positive_messages(messages, slot_type="premarket")

        if not signals:
            logger.info("No positive signals found in premarket analysis")
            self._last_premarket_analysis = context.timestamp
            return

        # Present to user for selection
        selected = await self._user_interaction.premarket_review(signals)

        # Save pending orders (do NOT execute yet)
        self._pending_premarket_orders.clear()

        for news_signal in selected:
            slot = self._position_manager.get_available_slot(SlotType.PREMARKET)
            if not slot:
                logger.warning("No more premarket slots available")
                break

            # Reserve the slot
            self._position_manager.reserve_slot(slot.slot_id)

            # Build stock name mapping
            stock_names = {}
            for stock_code in news_signal.target_stocks:
                stock_names[stock_code] = (
                    self._sector_mapper.get_stock_name(stock_code) or stock_code
                )

            # Determine sector name
            sector_name = (
                news_signal.target_sectors[0] if news_signal.target_sectors else "相关板块"
            )

            # Create pending order
            pending_order = PendingPremarketOrder(
                slot_id=slot.slot_id,
                stock_codes=news_signal.target_stocks,
                stock_names=stock_names,
                reason=news_signal.analysis.reason,
                news_id=news_signal.message.id,
                signal_type=news_signal.analysis.signal_type,
                confidence=news_signal.analysis.confidence,
                sector_name=sector_name,
            )
            self._pending_premarket_orders.append(pending_order)

            logger.info(
                f"Pending order created for slot {slot.slot_id}: "
                f"{len(news_signal.target_stocks)} candidate stocks, "
                f"reason: {news_signal.analysis.reason[:50]}..."
            )

        if self._pending_premarket_orders:
            logger.info(
                f"Premarket analysis complete: {len(self._pending_premarket_orders)} "
                f"pending orders waiting for morning auction execution"
            )
        else:
            logger.info("No pending orders created from premarket analysis")

        self._last_premarket_analysis = context.timestamp

    async def _handle_morning_auction(
        self,
        context: StrategyContext,
    ) -> AsyncIterator[TradingSignal]:
        """
        Execute pending premarket orders during morning auction (9:25-9:30).

        This is where we check real-time prices and skip stocks that
        opened at limit-up. Users are notified when stocks are skipped.

        Trading Safety: This function ensures we don't buy stocks that
        opened at limit-up, as they have no upside potential and high
        risk of next-day drop.
        """
        # Only run once per day
        if self._last_morning_auction:
            if self._last_morning_auction.date() == context.timestamp.date():
                return

        if not self._pending_premarket_orders:
            self._last_morning_auction = context.timestamp
            return

        logger.info(
            f"Morning auction: executing {len(self._pending_premarket_orders)} "
            f"pending premarket orders"
        )

        # Process each pending order
        for pending_order in self._pending_premarket_orders:
            # Classify stocks based on current real-time prices
            limit_up_stocks: list[tuple[str, str]] = []  # (code, name)
            available_stocks: list[tuple[str, str, float, float | None]] = []

            for stock_code in pending_order.stock_codes:
                price = self._get_stock_price(stock_code, context)
                if not price or price <= 0:
                    logger.warning(f"Cannot get price for {stock_code}, skipping")
                    continue

                stock_name = pending_order.stock_names.get(stock_code, stock_code)
                change_pct = self._get_change_pct(stock_code, price, context)

                if self._is_at_limit_up(stock_code, price, context):
                    limit_up_stocks.append((stock_code, stock_name))
                    logger.info(
                        f"Skip {stock_code} ({stock_name}): opened at limit-up (price={price:.2f})"
                    )
                else:
                    available_stocks.append((stock_code, stock_name, price, change_pct))

            # Notify user about limit-up stocks
            if limit_up_stocks:
                await self._user_interaction.notify_limit_up_skip(
                    sector_name=pending_order.sector_name,
                    limit_up_stocks=limit_up_stocks,
                    available_stocks=available_stocks,
                    reason=pending_order.reason,
                )

            # If all stocks are at limit-up, release the slot and skip
            if not available_stocks:
                logger.warning(
                    f"All candidate stocks for slot {pending_order.slot_id} "
                    f"are at limit-up, releasing slot"
                )
                self._position_manager.release_slot(pending_order.slot_id)
                continue

            # Execute buy for available stocks
            slot = self._position_manager.get_slot(pending_order.slot_id)
            if not slot:
                logger.error(f"Slot {pending_order.slot_id} not found")
                continue

            # Buy the first available stock
            for stock_code, stock_name, price, _change_pct in available_stocks:
                try:
                    quantity = self._position_manager.allocate_slot(
                        slot=slot,
                        stock_code=stock_code,
                        price=price,
                        reason=pending_order.reason,
                        stock_name=stock_name,
                    )

                    # Mark as filled
                    self._position_manager.fill_slot(slot.slot_id, price)

                    yield TradingSignal(
                        signal_type=SignalType.BUY,
                        stock_code=stock_code,
                        quantity=quantity,
                        strategy_name=self.strategy_name,
                        price=price,
                        confidence=pending_order.confidence,
                        reason=pending_order.reason,
                        metadata={
                            "slot_id": slot.slot_id,
                            "slot_type": "premarket",
                            "news_id": pending_order.news_id,
                            "signal_type": pending_order.signal_type,
                            "skipped_limit_up": [s[0] for s in limit_up_stocks],
                        },
                    )

                    logger.info(
                        f"Executed premarket buy: {stock_code} ({stock_name}) "
                        f"@ {price:.2f}, qty={quantity}"
                    )
                    # Successfully bought one stock, stop looking for more
                    break

                except Exception:
                    logger.error(f"Failed to execute order for {stock_code}")
                    raise

        # Clear pending orders after processing
        self._pending_premarket_orders.clear()
        self._last_morning_auction = context.timestamp

        # Save position state
        await self._save_position_state()
        logger.info("Morning auction execution complete, position state saved")

    async def _handle_trading_hours(
        self,
        context: StrategyContext,
    ) -> AsyncIterator[TradingSignal]:
        """Handle intraday monitoring and buying."""
        # Check if MessageReader is available
        if not context.has_message_reader:
            return

        # Get new POSITIVE messages since last check (already analyzed)
        new_messages = await self._get_new_positive_messages(context)

        if not new_messages:
            return

        logger.debug(f"Found {len(new_messages)} new positive messages for intraday")

        # Filter and convert to signals (no LLM needed)
        signals = self._news_analyzer.filter_positive_messages(new_messages, slot_type="intraday")

        # For each positive signal, ask user for confirmation
        for news_signal in signals:
            if not news_signal.is_actionable():
                continue

            # Check if we have available intraday slots
            slot = self._position_manager.get_available_slot(SlotType.INTRADAY)
            if not slot:
                logger.debug("No intraday slots available")
                continue

            # Classify stocks: limit-up vs available
            limit_up_stocks: list[tuple[str, str]] = []  # (code, name)
            # (code, name, price, change_pct)
            available_stocks: list[tuple[str, str, float, float | None]] = []

            for stock_code in news_signal.target_stocks:
                price = self._get_stock_price(stock_code, context)
                if not price or price <= 0:
                    continue

                stock_name = self._sector_mapper.get_stock_name(stock_code) or stock_code
                change_pct = self._get_change_pct(stock_code, price, context)

                if self._is_at_limit_up(stock_code, price, context):
                    limit_up_stocks.append((stock_code, stock_name))
                else:
                    available_stocks.append((stock_code, stock_name, price, change_pct))

            # If some stocks are at limit-up, ask user for confirmation
            if limit_up_stocks:
                sector_name = (
                    news_signal.target_sectors[0] if news_signal.target_sectors else "相关板块"
                )
                selected = await self._user_interaction.confirm_limit_up_situation(
                    sector_name=sector_name,
                    total_stocks=len(news_signal.target_stocks),
                    limit_up_stocks=limit_up_stocks,
                    available_stocks=available_stocks,
                )

                if not selected:
                    continue  # User chose to skip

                # Use user's selection
                stocks_to_buy = selected
            else:
                # No limit-up stocks, ask for normal confirmation
                confirmed = await self._user_interaction.intraday_confirm(news_signal)
                if not confirmed:
                    continue
                stocks_to_buy = available_stocks

            # Buy the selected stock(s)
            for stock_code, stock_name, price, _change_pct in stocks_to_buy:
                try:
                    quantity = self._position_manager.allocate_slot(
                        slot=slot,
                        stock_code=stock_code,
                        price=price,
                        reason=news_signal.analysis.reason,
                        stock_name=stock_name,
                    )

                    # Mark as filled
                    self._position_manager.fill_slot(slot.slot_id, price)

                    yield TradingSignal(
                        signal_type=SignalType.BUY,
                        stock_code=stock_code,
                        quantity=quantity,
                        strategy_name=self.strategy_name,
                        price=price,
                        confidence=news_signal.analysis.confidence,
                        reason=news_signal.analysis.reason,
                        metadata={
                            "slot_id": slot.slot_id,
                            "slot_type": "intraday",
                            "news_id": news_signal.message.id,
                            "signal_type": news_signal.analysis.signal_type,
                        },
                    )
                    # Successfully bought one stock, stop looking for more
                    break

                except Exception:
                    logger.error("Failed to allocate intraday slot")
                    raise

    async def _handle_after_hours(self, context: StrategyContext) -> None:
        """Handle after-hours: record holdings for next day and save state."""
        holdings = self._position_manager.get_holdings()

        if holdings:
            await self._holding_tracker.record_holdings(self._position_manager)
            logger.info(f"Recorded {len(holdings)} holdings for overnight tracking")

        # Save position state
        await self._save_position_state()
        logger.info("Saved position state")

    def _get_last_close_time(self, current_time: datetime) -> datetime:
        """Get the time of last market close."""
        # If before 15:00 today, use yesterday's close
        close_hour = 15
        close_minute = 0

        if current_time.hour < close_hour:
            # Use yesterday's close
            yesterday = current_time - timedelta(days=1)
            return yesterday.replace(hour=close_hour, minute=close_minute, second=0, microsecond=0)
        else:
            # Use today's close
            return current_time.replace(
                hour=close_hour, minute=close_minute, second=0, microsecond=0
            )

    async def _get_new_positive_messages(
        self,
        context: StrategyContext,
    ) -> list[Any]:
        """
        Get new positive messages not yet processed.

        Uses incremental query via MessageReader to only fetch new positive
        messages (bullish/strong_bullish) since the last check.
        """
        # Determine since time: use last fetch time or 5 minutes ago
        if self._last_message_check is not None:
            since = self._last_message_check
        else:
            since = context.timestamp - timedelta(minutes=5)

        # Fetch new POSITIVE messages from database (already filtered by sentiment)
        all_messages = await context.get_positive_messages_since(since, limit=100)

        # Filter out already processed messages
        new_messages = []
        for msg in all_messages:
            if msg.id not in self._processed_message_ids:
                self._processed_message_ids.add(msg.id)
                new_messages.append(msg)

        # Update last check time
        self._last_message_check = context.timestamp

        # Limit cache size
        if len(self._processed_message_ids) > 10000:
            self._processed_message_ids = set(list(self._processed_message_ids)[-5000:])

        return new_messages

    def _get_stock_price(
        self,
        stock_code: str,
        context: StrategyContext,
    ) -> float | None:
        """Get current price for a stock from context."""
        # Try to get from market_data in context
        market_data = context.market_data.get(stock_code, {})
        price = market_data.get("price") or market_data.get("last_price")

        if price:
            return float(price)

        # Fallback: use a placeholder price for testing
        # In production, this should fetch real-time price
        return 10.0  # Placeholder

    def _get_change_pct(
        self,
        stock_code: str,
        current_price: float,
        context: StrategyContext,
    ) -> float | None:
        """
        Get the percentage change from previous close.

        Args:
            stock_code: Stock code with exchange suffix
            current_price: Current trading price
            context: Strategy context with market data

        Returns:
            Percentage change (e.g., 5.5 for +5.5%), or None if unavailable
        """
        market_data = context.market_data.get(stock_code, {})

        # Method 1: Direct change ratio
        change_ratio = market_data.get("change_ratio") or market_data.get("pct_change")
        if change_ratio is not None:
            return float(change_ratio)

        # Method 2: Calculate from previous close
        prev_close = market_data.get("prev_close") or market_data.get("pre_close")
        if prev_close:
            prev_close = float(prev_close)
            if prev_close > 0:
                return (current_price / prev_close - 1) * 100

        return None

    def _get_limit_up_ratio(self, stock_code: str) -> float:
        """
        Get the limit-up ratio based on stock exchange and board.

        Returns:
            0.10 for main board (SH/SZ), 0.20 for ChiNext (300xxx) and STAR (688xxx)
        """
        # ChiNext (创业板): 300xxx, 301xxx
        if stock_code.startswith("30"):
            return 0.20
        # STAR Market (科创板): 688xxx
        if stock_code.startswith("688"):
            return 0.20
        # Main board: 10%
        return 0.10

    def _is_at_limit_up(
        self,
        stock_code: str,
        current_price: float,
        context: StrategyContext,
    ) -> bool:
        """
        Check if a stock is at or near its limit-up price.

        Trading Safety: Do NOT buy stocks that have already hit limit-up,
        as there's no upside left for the day and high risk of next-day drop.

        Args:
            stock_code: Stock code with exchange suffix
            current_price: Current trading price
            context: Strategy context with market data

        Returns:
            True if stock is at limit-up (should skip buying)
        """
        market_data = context.market_data.get(stock_code, {})

        # Method 1: Check if limit_up_price is directly available
        limit_up_price = market_data.get("limit_up_price") or market_data.get("upper_limit")
        if limit_up_price:
            limit_up_price = float(limit_up_price)
            # Consider at limit-up if price >= 99.5% of limit price (allow small tolerance)
            if current_price >= limit_up_price * 0.995:
                logger.info(
                    f"Skip {stock_code}: at limit-up (price={current_price:.2f}, "
                    f"limit={limit_up_price:.2f})"
                )
                return True
            return False

        # Method 2: Calculate from previous close
        prev_close = market_data.get("prev_close") or market_data.get("pre_close")
        if prev_close:
            prev_close = float(prev_close)
            limit_ratio = self._get_limit_up_ratio(stock_code)
            calculated_limit = round(prev_close * (1 + limit_ratio), 2)

            if current_price >= calculated_limit * 0.995:
                logger.info(
                    f"Skip {stock_code}: at limit-up (price={current_price:.2f}, "
                    f"calculated_limit={calculated_limit:.2f}, prev_close={prev_close:.2f})"
                )
                return True
            return False

        # Method 3: Check change ratio if available
        change_ratio = market_data.get("change_ratio") or market_data.get("pct_change")
        if change_ratio:
            change_ratio = float(change_ratio)
            limit_ratio = self._get_limit_up_ratio(stock_code) * 100  # Convert to percentage
            # If change ratio >= 9.9% (main board) or 19.8% (ChiNext/STAR), likely at limit
            if change_ratio >= limit_ratio * 0.99:
                logger.info(f"Skip {stock_code}: at limit-up (change_ratio={change_ratio:.2f}%)")
                return True
            return False

        # Trading Safety: If we can't determine, assume it's at limit-up
        # (better to miss a trade than to buy at ceiling price)
        # This follows the principle: Trading Safety > Program Robustness
        logger.warning(
            f"Cannot determine limit-up status for {stock_code}: "
            f"no limit_up_price, prev_close, or change_ratio in market_data. "
            f"Assuming limit-up for safety."
        )
        return True

    def get_state(self) -> dict[str, Any]:
        """Get strategy state for checkpointing."""
        state = super().get_state()
        state.update(
            {
                "position_manager": self._position_manager.get_state()
                if hasattr(self, "_position_manager")
                else None,
                "holding_tracker": self._holding_tracker.get_state()
                if hasattr(self, "_holding_tracker")
                else None,
                "last_premarket_analysis": self._last_premarket_analysis.isoformat()
                if self._last_premarket_analysis
                else None,
                "last_morning_confirmation": self._last_morning_confirmation.isoformat()
                if self._last_morning_confirmation
                else None,
                "last_morning_auction": self._last_morning_auction.isoformat()
                if self._last_morning_auction
                else None,
                "pending_premarket_orders": len(self._pending_premarket_orders)
                if hasattr(self, "_pending_premarket_orders")
                else 0,
            }
        )
        return state

    def validate(self) -> list[str]:
        """Validate strategy configuration."""
        errors = super().validate()

        # Validate capital
        total_capital = self.get_parameter("total_capital", 0)
        if total_capital <= 0:
            errors.append("total_capital must be positive")

        # Validate confidence
        min_confidence = self.get_parameter("min_confidence", 0.7)
        if not 0 <= min_confidence <= 1:
            errors.append("min_confidence must be between 0 and 1")

        return errors
