# === MODULE PURPOSE ===
# Simulation context that extends StrategyContext for historical simulation.
# Uses simulated time and historical data instead of real-time.

# === DEPENDENCIES ===
# - StrategyContext: Base context interface
# - SimulationClock: Virtual time
# - HistoricalMessageReader: Time-filtered messages
# - HistoricalPriceService: Historical prices

# === KEY CONCEPTS ===
# - Drop-in replacement: Works with existing strategies unchanged
# - Virtual time: timestamp and session from SimulationClock
# - Historical data: Messages and prices from history

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from src.common.scheduler import MarketSession
from src.simulation.clock import SimulationClock
from src.simulation.historical_message_reader import HistoricalMessageReader
from src.simulation.historical_price_service import HistoricalPriceService

if TYPE_CHECKING:
    from src.data.models.message import Message

logger = logging.getLogger(__name__)


@dataclass
class SimulationContext:
    """
    Context for historical simulation, compatible with StrategyContext.

    Provides the same interface as StrategyContext but uses:
    - SimulationClock for timestamp
    - HistoricalMessageReader for time-filtered messages
    - HistoricalPriceService for historical prices

    Usage:
        clock = SimulationClock(start_date=date(2026, 1, 29))
        context = SimulationContext(
            clock=clock,
            message_reader=hist_reader,
            price_service=price_service,
        )

        # Use like StrategyContext
        messages = await context.get_positive_messages_since(since)
    """

    clock: SimulationClock
    message_reader: HistoricalMessageReader
    price_service: HistoricalPriceService

    market_data: dict[str, dict[str, Any]] = field(default_factory=dict)
    positions: dict[str, dict[str, Any]] = field(default_factory=dict)
    account: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def timestamp(self) -> datetime:
        """Get current simulated timestamp."""
        return self.clock.current_time

    @property
    def _message_reader(self) -> HistoricalMessageReader:
        """Alias for compatibility with StrategyContext."""
        return self.message_reader

    @property
    def has_message_reader(self) -> bool:
        """Check if MessageReader is available."""
        return self.message_reader is not None and self.message_reader.is_connected

    def get_session(self) -> MarketSession:
        """Get current market session from simulation clock."""
        return self.clock.get_current_session()

    async def get_messages_since(
        self,
        since: datetime,
        source_type: str | None = None,
        limit: int = 1000,
    ) -> list["Message"]:
        """
        Get messages that have "arrived" by simulation time.

        Args:
            since: Fetch messages with fetch_time > since.
            source_type: Optional filter by source type.
            limit: Maximum number of messages.

        Returns:
            List of messages that have "arrived".
        """
        return await self.message_reader.get_messages_since(
            since=since,
            source_type=source_type,
            limit=limit,
        )

    async def get_positive_messages_since(
        self,
        since: datetime,
        source_type: str | None = None,
        limit: int = 500,
    ) -> list["Message"]:
        """
        Get positive sentiment messages that have "arrived".

        Args:
            since: Fetch messages with fetch_time > since.
            source_type: Optional filter by source type.
            limit: Maximum number of messages.

        Returns:
            List of positive messages.
        """
        return await self.message_reader.get_positive_messages_since(
            since=since,
            source_type=source_type,
            limit=limit,
        )

    async def get_messages_in_range(
        self,
        start_time: datetime,
        end_time: datetime,
        source_type: str | None = None,
        limit: int = 1000,
    ) -> list["Message"]:
        """
        Get messages in a time range.

        Args:
            start_time: Start of publish_time range.
            end_time: End of publish_time range (capped at sim time).
            source_type: Optional filter by source type.
            limit: Maximum number of messages.

        Returns:
            List of messages in range.
        """
        return await self.message_reader.get_messages_in_range(
            start_time=start_time,
            end_time=end_time,
            source_type=source_type,
            limit=limit,
        )

    async def get_messages_by_stock(
        self,
        stock_code: str,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list["Message"]:
        """
        Get messages for a specific stock.

        Args:
            stock_code: Stock code to search for.
            since: Optional filter by fetch_time.
            limit: Maximum number of messages.

        Returns:
            List of messages for the stock.
        """
        return await self.message_reader.get_messages_by_stock(
            stock_code=stock_code,
            since=since,
            limit=limit,
        )

    async def refresh_market_data(self, stock_codes: list[str]) -> None:
        """
        Refresh market data for given stock codes.

        Fetches historical prices and populates market_data dict.

        Args:
            stock_codes: List of stock codes to refresh.
        """
        trade_date = self.clock.current_date

        for code in stock_codes:
            daily_data = await self.price_service.get_daily_data(code, trade_date)
            price = await self.price_service.get_price_at_time(code, self.clock.current_time)

            if daily_data:
                self.market_data[code] = {
                    "price": price,
                    "open": daily_data.open,
                    "high": daily_data.high,
                    "low": daily_data.low,
                    "close": daily_data.close,
                    "prev_close": daily_data.prev_close,
                    "limit_up_price": daily_data.limit_up_price,
                    "limit_down_price": daily_data.limit_down_price,
                    "change_percent": daily_data.change_percent,
                    "is_limit_up": daily_data.is_limit_up(),
                }

    async def is_limit_up_at_open(self, stock_code: str) -> bool:
        """
        Check if stock opened at limit-up.

        Args:
            stock_code: Stock code to check.

        Returns:
            True if stock opened at limit-up price.
        """
        return await self.price_service.is_limit_up_at_open(stock_code, self.clock.current_date)

    async def get_current_price(self, stock_code: str) -> float | None:
        """
        Get current price for a stock at simulated time.

        Args:
            stock_code: Stock code.

        Returns:
            Price or None if unavailable.
        """
        return await self.price_service.get_price_at_time(stock_code, self.clock.current_time)

    def update_positions(self, positions: dict[str, dict[str, Any]]) -> None:
        """Update positions data."""
        self.positions = positions

    def update_account(self, account: dict[str, Any]) -> None:
        """Update account data."""
        self.account = account

    def update_metadata(self, key: str, value: Any) -> None:
        """Update a metadata field."""
        self.metadata[key] = value

    def set_session_metadata(self) -> None:
        """Set session info in metadata."""
        self.metadata["session"] = self.get_session().value
        self.metadata["is_trading_hours"] = self.clock.is_trading_hours()
        self.metadata["sim_time"] = self.clock.get_time_string()
