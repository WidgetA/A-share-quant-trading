# === MODULE PURPOSE ===
# Historical message reader that filters messages by simulation time.
# Wraps MessageReader to simulate "message arrival" as time progresses.

# === DEPENDENCIES ===
# - MessageReader: Underlying database reader
# - SimulationClock: Virtual time reference

# === KEY CONCEPTS ===
# - Time-filtered access: Messages only "arrive" when sim time passes their publish_time
# - Wrapper pattern: Delegates to MessageReader, adds time filtering

import logging
from datetime import datetime

from src.data.models.message import Message
from src.data.readers.message_reader import MessageReader
from src.simulation.clock import SimulationClock

logger = logging.getLogger(__name__)


class HistoricalMessageReader:
    """
    Wraps MessageReader to filter messages by simulated time.

    This reader simulates the experience of receiving messages in real-time
    by only returning messages that have "arrived" (publish_time <= sim_time).

    Usage:
        reader = MessageReader(config)
        clock = SimulationClock(start_date=date(2026, 1, 29))

        hist_reader = HistoricalMessageReader(reader, clock)

        # Only returns messages published before sim time
        messages = await hist_reader.get_positive_messages_since(
            since=datetime(2026, 1, 29, 0, 0)
        )

    Architecture:
        HistoricalMessageReader
            └─► MessageReader (actual DB queries)
                └─► PostgreSQL (messages table)
    """

    def __init__(
        self,
        reader: MessageReader,
        clock: SimulationClock,
    ):
        """
        Initialize historical message reader.

        Args:
            reader: Underlying MessageReader for database access.
            clock: SimulationClock for virtual time reference.
        """
        self._reader = reader
        self._clock = clock

    @property
    def clock(self) -> SimulationClock:
        """Get the simulation clock."""
        return self._clock

    @property
    def is_connected(self) -> bool:
        """Check if underlying reader is connected."""
        return self._reader.is_connected

    async def get_messages_since(
        self,
        since: datetime,
        source_type: str | None = None,
        sentiment: str | None = None,
        only_analyzed: bool = False,
        limit: int = 1000,
    ) -> list[Message]:
        """
        Get messages that have "arrived" by simulation time.

        Only returns messages where:
        - fetch_time > since
        - publish_time <= clock.current_time

        Args:
            since: Fetch messages with fetch_time > since.
            source_type: Optional filter by source type.
            sentiment: Optional filter by sentiment.
            only_analyzed: If True, only return analyzed messages.
            limit: Maximum number of messages.

        Returns:
            List of messages that have "arrived" in simulation.
        """
        # Get messages from DB
        messages = await self._reader.get_messages_since(
            since=since,
            source_type=source_type,
            sentiment=sentiment,
            only_analyzed=only_analyzed,
            limit=limit,
        )

        # Filter by simulation time - only return messages that have "arrived"
        sim_time = self._clock.current_time
        filtered = [m for m in messages if m.publish_time <= sim_time]

        logger.debug(
            f"Historical filter: {len(messages)} messages -> "
            f"{len(filtered)} after sim time filter (sim_time={sim_time})"
        )

        return filtered

    async def get_positive_messages_since(
        self,
        since: datetime,
        source_type: str | None = None,
        limit: int = 500,
    ) -> list[Message]:
        """
        Get positive sentiment messages that have "arrived".

        Args:
            since: Fetch messages with fetch_time > since.
            source_type: Optional filter by source type.
            limit: Maximum number of messages.

        Returns:
            List of positive messages that have "arrived".
        """
        messages = await self._reader.get_positive_messages_since(
            since=since,
            source_type=source_type,
            limit=limit,
        )

        # Filter by simulation time
        sim_time = self._clock.current_time
        filtered = [m for m in messages if m.publish_time <= sim_time]

        logger.debug(f"Historical positive filter: {len(messages)} -> {len(filtered)}")

        return filtered

    async def get_messages_in_range(
        self,
        start_time: datetime,
        end_time: datetime,
        source_type: str | None = None,
        only_positive: bool = False,
        limit: int = 1000,
        offset: int = 0,
    ) -> list[Message]:
        """
        Get messages in a time range, capped by simulation time.

        The end_time is automatically capped at the current simulation time.

        Args:
            start_time: Start of publish_time range.
            end_time: End of publish_time range (capped at sim time).
            source_type: Optional filter by source type.
            only_positive: If True, only return positive sentiment.
            limit: Maximum number of messages.
            offset: Number of messages to skip (for pagination).

        Returns:
            List of messages in the effective range.
        """
        # Cap end_time at simulation time
        sim_time = self._clock.current_time
        effective_end = min(end_time, sim_time)

        if start_time >= effective_end:
            # No valid range
            return []

        return await self._reader.get_messages_in_range(
            start_time=start_time,
            end_time=effective_end,
            source_type=source_type,
            only_positive=only_positive,
            limit=limit,
            offset=offset,
        )

    async def count_messages_in_range(
        self,
        start_time: datetime,
        end_time: datetime,
        source_type: str | None = None,
        only_positive: bool = False,
    ) -> int:
        """
        Count messages in a time range, capped by simulation time.

        Args:
            start_time: Start of publish_time range.
            end_time: End of publish_time range (capped at sim time).
            source_type: Optional filter by source type.
            only_positive: If True, only count positive sentiment.

        Returns:
            Number of matching messages.
        """
        sim_time = self._clock.current_time
        effective_end = min(end_time, sim_time)

        if start_time >= effective_end:
            return 0

        return await self._reader.count_messages_in_range(
            start_time=start_time,
            end_time=effective_end,
            source_type=source_type,
            only_positive=only_positive,
        )

    async def get_messages_by_stock(
        self,
        stock_code: str,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[Message]:
        """
        Get messages for a specific stock that have "arrived".

        Args:
            stock_code: Stock code to search for.
            since: Optional filter by fetch_time.
            limit: Maximum number of messages.

        Returns:
            List of messages for the stock that have "arrived".
        """
        messages = await self._reader.get_messages_by_stock(
            stock_code=stock_code,
            since=since,
            limit=limit,
        )

        # Filter by simulation time
        sim_time = self._clock.current_time
        filtered = [m for m in messages if m.publish_time <= sim_time]

        return filtered

    async def get_premarket_messages(
        self,
        trade_date: datetime,
        only_positive: bool = True,
        limit: int = 500,
        offset: int = 0,
    ) -> list[Message]:
        """
        Get messages for premarket analysis.

        Returns messages published between:
        - Previous day's market close (15:00)
        - Current day's premarket time (08:30)

        Args:
            trade_date: The trading date to get premarket messages for.
            only_positive: If True, only return positive sentiment messages.
            limit: Maximum number of messages.
            offset: Number of messages to skip (for pagination).

        Returns:
            List of overnight messages for premarket analysis.
        """
        from datetime import time, timedelta

        # Previous trading day close
        prev_date = trade_date.date() - timedelta(days=1)
        # Skip weekends
        while prev_date.weekday() >= 5:
            prev_date -= timedelta(days=1)

        start_time = datetime.combine(prev_date, time(15, 0))
        end_time = datetime.combine(trade_date.date(), time(8, 30))

        return await self.get_messages_in_range(
            start_time=start_time,
            end_time=end_time,
            only_positive=only_positive,
            limit=limit,
            offset=offset,
        )

    async def count_premarket_messages(
        self,
        trade_date: datetime,
        only_positive: bool = True,
    ) -> int:
        """
        Count messages for premarket analysis.

        Returns count of messages published between:
        - Previous day's market close (15:00)
        - Current day's premarket time (08:30)

        Args:
            trade_date: The trading date to count premarket messages for.
            only_positive: If True, only count positive sentiment messages.

        Returns:
            Number of overnight messages for premarket analysis.
        """
        from datetime import time, timedelta

        # Previous trading day close
        prev_date = trade_date.date() - timedelta(days=1)
        # Skip weekends
        while prev_date.weekday() >= 5:
            prev_date -= timedelta(days=1)

        start_time = datetime.combine(prev_date, time(15, 0))
        end_time = datetime.combine(trade_date.date(), time(8, 30))

        return await self.count_messages_in_range(
            start_time=start_time,
            end_time=end_time,
            only_positive=only_positive,
        )

    async def get_intraday_messages(
        self,
        since: datetime,
        only_positive: bool = True,
        limit: int = 100,
    ) -> list[Message]:
        """
        Get new intraday messages since last check.

        Args:
            since: Get messages with publish_time > since.
            only_positive: If True, only return positive sentiment.
            limit: Maximum number of messages.

        Returns:
            List of new intraday messages.
        """
        sim_time = self._clock.current_time

        return await self.get_messages_in_range(
            start_time=since,
            end_time=sim_time,
            only_positive=only_positive,
            limit=limit,
        )
