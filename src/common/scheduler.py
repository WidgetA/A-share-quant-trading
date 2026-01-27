# === MODULE PURPOSE ===
# Trading session scheduler for A-share market.
# Provides real-time detection of market trading sessions.

# === DEPENDENCIES ===
# - None (pure Python, no external dependencies)

# === KEY CONCEPTS ===
# - MarketSession: Enum representing different trading periods
# - TradingScheduler: Main class for session detection
# - Trading Hours: 9:30-11:30 (morning), 13:00-15:00 (afternoon)

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from enum import Enum
from typing import Callable
import logging

logger = logging.getLogger(__name__)


class MarketSession(Enum):
    """
    A-share market trading sessions.

    Timeline (Beijing Time):
        00:00-09:15  PRE_MARKET
        09:15-09:30  MORNING_AUCTION (call auction)
        09:30-11:30  MORNING (continuous trading)
        11:30-13:00  LUNCH_BREAK
        13:00-15:00  AFTERNOON (continuous trading)
        15:00-24:00  AFTER_HOURS
        Weekend      CLOSED
    """

    PRE_MARKET = "pre_market"
    MORNING_AUCTION = "morning_auction"
    MORNING = "morning"
    LUNCH_BREAK = "lunch_break"
    AFTERNOON = "afternoon"
    AFTER_HOURS = "after_hours"
    CLOSED = "closed"


@dataclass
class SessionTimes:
    """Time boundaries for trading sessions."""

    morning_auction_start: time = time(9, 15)
    morning_open: time = time(9, 30)
    morning_close: time = time(11, 30)
    afternoon_open: time = time(13, 0)
    afternoon_close: time = time(15, 0)


# Type alias for session change callbacks
SessionCallback = Callable[[MarketSession, MarketSession], None]


class TradingScheduler:
    """
    A-share market trading session scheduler.

    Provides real-time detection of current trading session and
    utilities for trading time management.

    Features:
        - Real-time session detection
        - Trading day detection (excludes weekends)
        - Time until next session calculation
        - Session change callbacks

    Usage:
        scheduler = TradingScheduler()

        # Check current session
        session = scheduler.get_current_session()
        if scheduler.is_trading_hours():
            # Execute trading logic
            pass

        # Register callback for session changes
        def on_session_change(old: MarketSession, new: MarketSession):
            print(f"Session changed: {old} -> {new}")
        scheduler.add_session_callback(on_session_change)

    Note:
        Holiday calendar is not included. For production use,
        integrate with a holiday data source.
    """

    def __init__(self, session_times: SessionTimes | None = None):
        """
        Initialize scheduler with optional custom session times.

        Args:
            session_times: Custom trading session boundaries.
                          Defaults to standard A-share times.
        """
        self.times = session_times or SessionTimes()
        self._callbacks: list[SessionCallback] = []
        self._last_session: MarketSession | None = None

    def get_current_session(self, now: datetime | None = None) -> MarketSession:
        """
        Determine the current trading session.

        Args:
            now: Optional datetime for testing. Uses current time if None.

        Returns:
            MarketSession enum indicating current session.
        """
        if now is None:
            now = datetime.now()

        # Check if it's a weekend (closed)
        if not self._is_weekday(now):
            return MarketSession.CLOSED

        current_time = now.time()

        # Pre-market: before morning auction
        if current_time < self.times.morning_auction_start:
            return MarketSession.PRE_MARKET

        # Morning auction: 9:15-9:30
        if current_time < self.times.morning_open:
            return MarketSession.MORNING_AUCTION

        # Morning session: 9:30-11:30
        if current_time < self.times.morning_close:
            return MarketSession.MORNING

        # Lunch break: 11:30-13:00
        if current_time < self.times.afternoon_open:
            return MarketSession.LUNCH_BREAK

        # Afternoon session: 13:00-15:00
        if current_time < self.times.afternoon_close:
            return MarketSession.AFTERNOON

        # After hours: after 15:00
        return MarketSession.AFTER_HOURS

    def is_trading_hours(self, now: datetime | None = None) -> bool:
        """
        Check if the market is currently in trading hours.

        Trading hours are MORNING and AFTERNOON sessions only.
        Does not include auction or break times.

        Args:
            now: Optional datetime for testing.

        Returns:
            True if currently in trading session.
        """
        session = self.get_current_session(now)
        return session in (MarketSession.MORNING, MarketSession.AFTERNOON)

    def is_trading_day(self, date: datetime | None = None) -> bool:
        """
        Check if the given date is a trading day.

        Currently only checks for weekdays. For production use,
        integrate with a holiday calendar.

        Args:
            date: Optional datetime for testing.

        Returns:
            True if the date is a potential trading day (weekday).
        """
        if date is None:
            date = datetime.now()
        return self._is_weekday(date)

    def time_until_next_session(
        self, now: datetime | None = None
    ) -> tuple[MarketSession, timedelta]:
        """
        Calculate time until the next trading session.

        Args:
            now: Optional datetime for testing.

        Returns:
            Tuple of (next_session, time_delta).
        """
        if now is None:
            now = datetime.now()

        current_session = self.get_current_session(now)
        current_time = now.time()
        today = now.date()

        # Determine next session based on current session
        if current_session == MarketSession.PRE_MARKET:
            next_session = MarketSession.MORNING_AUCTION
            next_time = datetime.combine(today, self.times.morning_auction_start)

        elif current_session == MarketSession.MORNING_AUCTION:
            next_session = MarketSession.MORNING
            next_time = datetime.combine(today, self.times.morning_open)

        elif current_session == MarketSession.MORNING:
            next_session = MarketSession.LUNCH_BREAK
            next_time = datetime.combine(today, self.times.morning_close)

        elif current_session == MarketSession.LUNCH_BREAK:
            next_session = MarketSession.AFTERNOON
            next_time = datetime.combine(today, self.times.afternoon_open)

        elif current_session == MarketSession.AFTERNOON:
            next_session = MarketSession.AFTER_HOURS
            next_time = datetime.combine(today, self.times.afternoon_close)

        elif current_session == MarketSession.AFTER_HOURS:
            # Next session is tomorrow's pre-market/morning
            next_day = self._next_trading_day(now)
            next_session = MarketSession.MORNING_AUCTION
            next_time = datetime.combine(next_day, self.times.morning_auction_start)

        else:  # CLOSED (weekend)
            next_day = self._next_trading_day(now)
            next_session = MarketSession.MORNING_AUCTION
            next_time = datetime.combine(next_day, self.times.morning_auction_start)

        return next_session, next_time - now

    def time_until_market_open(self, now: datetime | None = None) -> timedelta | None:
        """
        Calculate time until market opens (morning session starts).

        Args:
            now: Optional datetime for testing.

        Returns:
            Time delta until market open, or None if already in trading hours.
        """
        if now is None:
            now = datetime.now()

        if self.is_trading_hours(now):
            return None

        current_session = self.get_current_session(now)
        today = now.date()

        if current_session in (
            MarketSession.PRE_MARKET,
            MarketSession.MORNING_AUCTION,
        ):
            # Today's morning open
            open_time = datetime.combine(today, self.times.morning_open)
            return open_time - now

        elif current_session == MarketSession.LUNCH_BREAK:
            # Today's afternoon open
            open_time = datetime.combine(today, self.times.afternoon_open)
            return open_time - now

        else:
            # After hours or weekend - next trading day
            next_day = self._next_trading_day(now)
            open_time = datetime.combine(next_day, self.times.morning_open)
            return open_time - now

    def add_session_callback(self, callback: SessionCallback) -> None:
        """
        Register a callback for session changes.

        The callback receives (old_session, new_session) when
        the trading session changes.

        Args:
            callback: Function to call on session change.
        """
        self._callbacks.append(callback)

    def remove_session_callback(self, callback: SessionCallback) -> None:
        """Remove a previously registered callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    def check_session_change(self, now: datetime | None = None) -> bool:
        """
        Check if session has changed and notify callbacks.

        Call this periodically (e.g., every minute) to detect
        session transitions and trigger callbacks.

        Args:
            now: Optional datetime for testing.

        Returns:
            True if session changed since last check.
        """
        current_session = self.get_current_session(now)

        if self._last_session is None:
            self._last_session = current_session
            return False

        if current_session != self._last_session:
            old_session = self._last_session
            self._last_session = current_session

            # Notify callbacks
            for callback in self._callbacks:
                try:
                    callback(old_session, current_session)
                except Exception as e:
                    logger.error(f"Session callback error: {e}")

            logger.info(f"Session changed: {old_session.value} -> {current_session.value}")
            return True

        return False

    def _is_weekday(self, dt: datetime) -> bool:
        """Check if datetime is a weekday (Monday=0 to Friday=4)."""
        return dt.weekday() < 5

    def _next_trading_day(self, from_date: datetime) -> datetime:
        """
        Get the next trading day after the given date.

        Skips weekends. Does not account for holidays.

        Args:
            from_date: Starting datetime.

        Returns:
            Date of next trading day.
        """
        next_day = from_date.date() + timedelta(days=1)

        # Skip to Monday if weekend
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)

        return next_day

    def get_session_info(self, now: datetime | None = None) -> dict:
        """
        Get comprehensive session information.

        Returns:
            Dictionary with session details for logging/display.
        """
        if now is None:
            now = datetime.now()

        session = self.get_current_session(now)
        next_session, time_until = self.time_until_next_session(now)

        return {
            "current_session": session.value,
            "is_trading_hours": self.is_trading_hours(now),
            "is_trading_day": self.is_trading_day(now),
            "next_session": next_session.value,
            "time_until_next": str(time_until).split(".")[0],  # Remove microseconds
            "timestamp": now.isoformat(),
        }
