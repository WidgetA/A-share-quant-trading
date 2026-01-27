# === MODULE PURPOSE ===
# News-driven trading strategy with LLM analysis.
# Implements premarket batch analysis and intraday monitoring.
# Supports full PDF content reading via Aliyun multimodal model.

# === WORKFLOW ===
# 1. PRE_MARKET (8:30): Analyze overnight news, present to user
# 2. MORNING_AUCTION (9:15-9:30): Execute confirmed premarket buys
# 3. MORNING/AFTERNOON: Monitor new news, confirm intraday buys
# 4. AFTER_HOURS (15:00): Record holdings for next day
# 5. NEXT PRE_MARKET (9:00): Confirm sell/hold decisions

# === DEPENDENCIES ===
# - llm_service: For news analysis
# - stock_filter: For exchange filtering
# - sector_mapper: For sector resolution
# - news_analyzer: Coordinates analysis (with optional PDF reading)
# - content_fetcher: For reading full PDF content (Aliyun Qwen-VL)
# - position_manager: Manages capital slots
# - holding_tracker: Tracks overnight positions
# - user_interaction: Gets user confirmations

import logging
from datetime import datetime, timedelta
from typing import Any, AsyncIterator

from src.common.config import Config
from src.common.llm_service import LLMConfig, LLMService, create_llm_service_from_config
from src.common.scheduler import MarketSession
from src.common.user_interaction import InteractionConfig, UserInteraction
from src.data.sources.announcement_content import (
    AnnouncementContentFetcher,
    create_content_fetcher_from_config,
)
from src.data.sources.sector_mapper import SectorMapper
from src.strategy.analyzers.news_analyzer import NewsAnalyzer, NewsAnalyzerConfig
from src.strategy.base import BaseStrategy, StrategyConfig, StrategyContext
from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig
from src.strategy.signals import SignalType, TradingSignal
from src.trading.holding_tracker import HoldingTracker
from src.trading.position_manager import PositionConfig, PositionManager, SlotType

logger = logging.getLogger(__name__)


class NewsAnalysisStrategy(BaseStrategy):
    """
    News-driven trading strategy with LLM analysis.

    This strategy analyzes news and announcements using LLM to identify
    trading signals. It supports:
    - Premarket batch analysis (8:30 AM)
    - Intraday real-time monitoring
    - Overnight position tracking
    - Morning sell confirmations

    Configuration (YAML):
        strategies:
          news_analysis:
            enabled: true
            parameters:
              llm_model: "Qwen/Qwen2.5-72B-Instruct"
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

        # Initialize LLM service
        try:
            self._llm_service = create_llm_service_from_config()
        except (ValueError, FileNotFoundError) as e:
            logger.error(f"Failed to create LLM service: {e}")
            # Create with placeholder config - will fail on actual use
            self._llm_service = LLMService(LLMConfig(api_key="NOT_CONFIGURED"))

        await self._llm_service.start()

        # Initialize content fetcher for PDF reading (optional)
        self._content_fetcher: AnnouncementContentFetcher | None = None
        fetch_full_content = self.get_parameter("fetch_full_content", True)

        if fetch_full_content:
            try:
                self._content_fetcher = create_content_fetcher_from_config()
                await self._content_fetcher.start()
                logger.info("Content fetcher initialized for PDF reading")
            except (ValueError, FileNotFoundError) as e:
                logger.warning(f"Content fetcher not available (PDF reading disabled): {e}")
                self._content_fetcher = None

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

        # Initialize news analyzer with content fetcher
        analyzer_config = NewsAnalyzerConfig(
            min_confidence=self.get_parameter("min_confidence", 0.7),
            max_stocks_per_sector=self.get_parameter("max_stocks_per_sector", 5),
            signal_types=self.get_parameter(
                "signal_types", ["dividend", "earnings", "restructure"]
            ),
            fetch_full_content=fetch_full_content,
            content_fetch_timeout=self.get_parameter("content_fetch_timeout", 30.0),
            content_fetch_concurrency=self.get_parameter("content_fetch_concurrency", 5),
        )
        self._news_analyzer = NewsAnalyzer(
            llm_service=self._llm_service,
            stock_filter=self._stock_filter,
            sector_mapper=self._sector_mapper,
            content_fetcher=self._content_fetcher,
            config=analyzer_config,
        )

        # Initialize position manager with state persistence
        position_config = PositionConfig(
            total_capital=self.get_parameter("total_capital", 10_000_000.0),
            premarket_slots=self.get_parameter("premarket_slots", 3),
            intraday_slots=self.get_parameter("intraday_slots", 2),
        )
        self._position_manager = PositionManager(position_config)

        # Load existing position state from file
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
        self._processed_message_ids: set[str] = set()

        logger.info(f"NewsAnalysisStrategy loaded with config: {position_config}")

    async def on_unload(self) -> None:
        """Clean up resources."""
        if hasattr(self, "_content_fetcher") and self._content_fetcher:
            await self._content_fetcher.stop()

        if hasattr(self, "_llm_service") and self._llm_service:
            await self._llm_service.stop()

        if hasattr(self, "_sector_mapper") and self._sector_mapper:
            await self._sector_mapper.close()

        await super().on_unload()
        logger.info("NewsAnalysisStrategy unloaded")

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

            # Premarket analysis
            async for signal in self._handle_premarket(context):
                yield signal

        elif session in (MarketSession.MORNING, MarketSession.AFTERNOON):
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
        current_time = context.timestamp.strftime("%H:%M")

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
        slots_to_sell = await self._user_interaction.morning_confirmation(
            holdings_to_review
        )

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
    ) -> AsyncIterator[TradingSignal]:
        """Handle premarket analysis at configured time (default 8:30 AM)."""
        # Check if it's analysis time
        analysis_time = self.get_parameter("premarket_analysis_time", "08:30")
        current_time = context.timestamp.strftime("%H:%M")

        # Only run once per day
        if self._last_premarket_analysis:
            if self._last_premarket_analysis.date() == context.timestamp.date():
                return

        # Check time window (analysis_time to analysis_time + 30min)
        analysis_hour, analysis_minute = map(int, analysis_time.split(":"))
        current_minutes = context.timestamp.hour * 60 + context.timestamp.minute
        analysis_minutes = analysis_hour * 60 + analysis_minute

        if not (analysis_minutes <= current_minutes <= analysis_minutes + 30):
            return

        logger.info("Starting premarket news analysis")

        # Ensure sector data is loaded
        if not self._sector_mapper.is_loaded:
            try:
                await self._sector_mapper.load_sector_data()
            except Exception as e:
                logger.warning(f"Failed to load sector data: {e}")

        # Get messages since last close (yesterday 15:00)
        last_close = self._get_last_close_time(context.timestamp)
        messages = self._get_messages_since(last_close, context)

        if not messages:
            logger.info("No new messages for premarket analysis")
            self._last_premarket_analysis = context.timestamp
            return

        logger.info(f"Analyzing {len(messages)} messages for premarket")

        # Analyze messages
        signals = await self._news_analyzer.analyze_messages(
            messages, slot_type="premarket"
        )

        if not signals:
            logger.info("No positive signals found in premarket analysis")
            self._last_premarket_analysis = context.timestamp
            return

        # Present to user for selection
        selected = await self._user_interaction.premarket_review(signals)

        # Generate buy signals for selected stocks
        for news_signal in selected:
            slot = self._position_manager.get_available_slot(SlotType.PREMARKET)
            if not slot:
                logger.warning("No more premarket slots available")
                break

            # Get first target stock that is NOT at limit-up
            for stock_code in news_signal.target_stocks:
                # Get price from context or use a placeholder
                price = self._get_stock_price(stock_code, context)
                if not price or price <= 0:
                    logger.warning(f"Cannot get price for {stock_code}")
                    continue

                # Skip stocks already at limit-up - no upside left
                if self._is_at_limit_up(stock_code, price, context):
                    continue

                try:
                    quantity = self._position_manager.allocate_slot(
                        slot=slot,
                        stock_code=stock_code,
                        price=price,
                        reason=news_signal.analysis.reason,
                        stock_name=self._sector_mapper.get_stock_name(stock_code),
                    )

                    # Mark as filled immediately (paper trading)
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
                            "slot_type": "premarket",
                            "news_id": news_signal.message.id,
                            "signal_type": news_signal.analysis.signal_type,
                        },
                    )
                    # Successfully bought one stock, stop looking for more
                    break

                except Exception as e:
                    logger.error(f"Failed to allocate slot for {stock_code}: {e}")

        self._last_premarket_analysis = context.timestamp

    async def _handle_trading_hours(
        self,
        context: StrategyContext,
    ) -> AsyncIterator[TradingSignal]:
        """Handle intraday monitoring and buying."""
        # Get new messages from context
        new_messages = self._get_new_messages(context)

        if not new_messages:
            return

        logger.debug(f"Checking {len(new_messages)} new messages for intraday signals")

        # Analyze new messages
        signals = await self._news_analyzer.analyze_messages(
            new_messages, slot_type="intraday"
        )

        # For each positive signal, ask user for confirmation
        for news_signal in signals:
            if not news_signal.is_actionable():
                continue

            # Check if we have available intraday slots
            slot = self._position_manager.get_available_slot(SlotType.INTRADAY)
            if not slot:
                logger.debug("No intraday slots available")
                continue

            # Ask user for confirmation
            confirmed = await self._user_interaction.intraday_confirm(news_signal)

            if not confirmed:
                continue

            # Get first target stock that is NOT at limit-up
            for stock_code in news_signal.target_stocks:
                price = self._get_stock_price(stock_code, context)
                if not price or price <= 0:
                    continue

                # Skip stocks already at limit-up - no upside left
                if self._is_at_limit_up(stock_code, price, context):
                    continue

                try:
                    quantity = self._position_manager.allocate_slot(
                        slot=slot,
                        stock_code=stock_code,
                        price=price,
                        reason=news_signal.analysis.reason,
                        stock_name=self._sector_mapper.get_stock_name(stock_code),
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

                except Exception as e:
                    logger.error(f"Failed to allocate intraday slot: {e}")

    async def _handle_after_hours(self, context: StrategyContext) -> None:
        """Handle after-hours: record holdings for next day and save state."""
        holdings = self._position_manager.get_holdings()

        if holdings:
            await self._holding_tracker.record_holdings(self._position_manager)
            logger.info(f"Recorded {len(holdings)} holdings for overnight tracking")

        # Save position state to file
        state_file = self.get_parameter("position_state_file", "data/position_state.json")
        self._position_manager.save_to_file(state_file)
        logger.info("Saved position state to file")

    def _get_last_close_time(self, current_time: datetime) -> datetime:
        """Get the time of last market close."""
        # If before 15:00 today, use yesterday's close
        close_hour = 15
        close_minute = 0

        if current_time.hour < close_hour:
            # Use yesterday's close
            yesterday = current_time - timedelta(days=1)
            return yesterday.replace(
                hour=close_hour, minute=close_minute, second=0, microsecond=0
            )
        else:
            # Use today's close
            return current_time.replace(
                hour=close_hour, minute=close_minute, second=0, microsecond=0
            )

    def _get_messages_since(
        self,
        since: datetime,
        context: StrategyContext,
    ) -> list[Any]:
        """Get messages from context since the given time."""
        messages = []

        for news in context.news:
            # Convert to Message if dict
            publish_time = news.get("publish_time")
            if isinstance(publish_time, str):
                try:
                    publish_time = datetime.fromisoformat(publish_time)
                except ValueError:
                    continue

            if publish_time and publish_time >= since:
                messages.append(self._dict_to_message(news))

        return messages

    def _get_new_messages(self, context: StrategyContext) -> list[Any]:
        """Get messages not yet processed."""
        messages = []

        for news in context.news:
            msg_id = news.get("id")
            if msg_id and msg_id not in self._processed_message_ids:
                self._processed_message_ids.add(msg_id)
                messages.append(self._dict_to_message(news))

        # Limit cache size
        if len(self._processed_message_ids) > 10000:
            self._processed_message_ids = set(
                list(self._processed_message_ids)[-5000:]
            )

        return messages

    def _dict_to_message(self, data: dict[str, Any]) -> Any:
        """Convert dict to Message object."""
        from src.data.models.message import Message

        return Message(
            id=data.get("id", ""),
            source_type=data.get("source_type", "news"),
            source_name=data.get("source_name", "unknown"),
            title=data.get("title", ""),
            content=data.get("content", ""),
            url=data.get("url"),
            stock_codes=data.get("stock_codes", []),
            publish_time=self._parse_datetime(data.get("publish_time")),
            fetch_time=self._parse_datetime(data.get("fetch_time")),
        )

    def _parse_datetime(self, value: Any) -> datetime:
        """Parse datetime from various formats."""
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                pass
        return datetime.now()

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
                logger.info(
                    f"Skip {stock_code}: at limit-up (change_ratio={change_ratio:.2f}%)"
                )
                return True
            return False

        # If we can't determine, log warning but allow the trade
        # (better to miss than to crash, but log for investigation)
        logger.warning(
            f"Cannot determine limit-up status for {stock_code}: "
            f"no limit_up_price, prev_close, or change_ratio in market_data"
        )
        return False

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
