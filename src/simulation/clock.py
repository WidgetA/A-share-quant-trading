# === MODULE PURPOSE ===
# Virtual time controller for historical simulation.
# Provides time manipulation for replaying past trading days.

# === KEY CONCEPTS ===
# - Virtual time: Simulated clock independent of real time
# - Key time points: Important moments in the trading day
# - Fast-forward: Jump to next key time point

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta

from src.common.scheduler import MarketSession

logger = logging.getLogger(__name__)


@dataclass
class SimulationClock:
    """
    Virtual time controller for historical simulation.

    Manages simulated time that can be advanced through key trading
    day time points without real-time waiting.

    Usage:
        clock = SimulationClock(start_date=date(2026, 1, 29))

        # Get current simulated time
        now = clock.current_time

        # Advance to next key point
        clock.advance_to_next_key_point()

        # Jump to specific time
        clock.advance_to_time(time(9, 30))

        # Move to next trading day
        clock.advance_to_next_day()
    """

    start_date: date
    current_time: datetime = field(init=False)
    _day_count: int = field(default=1, init=False)

    # Key time points for simulation (Beijing time)
    KEY_TIME_POINTS: list[time] = field(
        default_factory=lambda: [
            time(8, 30),  # Premarket analysis
            time(9, 15),  # Auction start
            time(9, 25),  # Auction critical
            time(9, 30),  # Market open
            time(10, 0),  # Intraday check 1
            time(11, 0),  # Intraday check 2
            time(11, 30),  # Lunch break
            time(13, 0),  # Afternoon open
            time(14, 0),  # Intraday check 3
            time(14, 30),  # Near close
            time(15, 0),  # Market close
            time(15, 30),  # After hours
        ],
        init=False,
    )

    def __post_init__(self) -> None:
        """Initialize current time to start of first trading day."""
        # Start at 8:30 on the first day (premarket)
        self.current_time = datetime.combine(
            self.start_date,
            time(8, 30),
        )
        self._day_count = 1

    @property
    def current_date(self) -> date:
        """Get current simulation date."""
        return self.current_time.date()

    @property
    def day_number(self) -> int:
        """Get current trading day number (1-based)."""
        return self._day_count

    def get_current_session(self) -> MarketSession:
        """
        Determine market session based on simulated time.

        Returns:
            MarketSession enum for current simulated time.
        """
        # Check weekend
        if self.current_time.weekday() >= 5:
            return MarketSession.CLOSED

        current = self.current_time.time()

        if current < time(9, 15):
            return MarketSession.PRE_MARKET
        elif current < time(9, 30):
            return MarketSession.MORNING_AUCTION
        elif current < time(11, 30):
            return MarketSession.MORNING
        elif current < time(13, 0):
            return MarketSession.LUNCH_BREAK
        elif current < time(15, 0):
            return MarketSession.AFTERNOON
        else:
            return MarketSession.AFTER_HOURS

    def advance_to_time(self, target: time) -> datetime:
        """
        Advance to specific time on current day.

        Args:
            target: Target time to advance to.

        Returns:
            New current time.

        Raises:
            ValueError: If target is before current time.
        """
        new_time = datetime.combine(self.current_date, target)

        if new_time < self.current_time:
            raise ValueError(f"Cannot go back in time: {target} < {self.current_time.time()}")

        old_time = self.current_time
        self.current_time = new_time

        logger.debug(
            f"Clock advanced: {old_time.strftime('%H:%M')} -> {new_time.strftime('%H:%M')}"
        )

        return self.current_time

    def advance_to_next_key_point(self) -> datetime:
        """
        Advance to the next key time point.

        If at end of day, stays at current time (use advance_to_next_day).

        Returns:
            New current time after advancing.
        """
        current = self.current_time.time()

        # Find next key point after current time
        for key_point in self.KEY_TIME_POINTS:
            if key_point > current:
                return self.advance_to_time(key_point)

        # No more key points today - stay at current
        logger.debug("No more key points today")
        return self.current_time

    def advance_to_next_day(self) -> datetime:
        """
        Advance to premarket of next trading day.

        Skips weekends automatically.

        Returns:
            New current time (8:30 of next trading day).
        """
        next_date = self.current_date + timedelta(days=1)

        # Skip weekends
        while next_date.weekday() >= 5:
            next_date += timedelta(days=1)

        self._day_count += 1
        self.current_time = datetime.combine(next_date, time(8, 30))

        logger.info(
            f"Advanced to day {self._day_count}: {self.current_time.strftime('%Y-%m-%d %H:%M')}"
        )

        return self.current_time

    def is_trading_hours(self) -> bool:
        """Check if simulated time is during trading hours."""
        session = self.get_current_session()
        return session in (MarketSession.MORNING, MarketSession.AFTERNOON)

    def is_before_open(self) -> bool:
        """Check if simulated time is before market open."""
        session = self.get_current_session()
        return session in (MarketSession.PRE_MARKET, MarketSession.MORNING_AUCTION)

    def is_after_close(self) -> bool:
        """Check if simulated time is after market close."""
        return self.get_current_session() == MarketSession.AFTER_HOURS

    def get_time_string(self) -> str:
        """Get formatted time string for display."""
        return self.current_time.strftime("%Y-%m-%d %H:%M")

    def set_time(self, new_time: datetime) -> None:
        """
        Set the current simulation time directly.

        Use with caution - mainly for testing.

        Args:
            new_time: New simulation time.
        """
        self.current_time = new_time

    def get_remaining_key_points(self) -> list[time]:
        """Get list of remaining key time points for today."""
        current = self.current_time.time()
        return [kp for kp in self.KEY_TIME_POINTS if kp > current]

    def is_end_of_day(self) -> bool:
        """Check if all key time points for today have passed."""
        return len(self.get_remaining_key_points()) == 0
