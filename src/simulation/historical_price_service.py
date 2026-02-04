# === MODULE PURPOSE ===
# Historical price data service using iFinD API.
# Provides historical OHLCV data for simulation P&L calculation.

# === DEPENDENCIES ===
# - iFinDPy: THS iFinD SDK for A-share market data
# - SimulationClock: Virtual time reference

# === KEY CONCEPTS ===
# - Historical quotes: Past OHLCV data via THS_HistoryQuotes
# - Price interpolation: Estimate price at specific time within trading day
# - Caching: Reduce API calls by caching daily data

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any

from src.common.config import get_ifind_credentials

logger = logging.getLogger(__name__)


@dataclass
class DailyPriceData:
    """Daily OHLCV data for a stock."""

    stock_code: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    prev_close: float
    volume: float | None = None
    amount: float | None = None

    @property
    def limit_up_price(self) -> float:
        """Calculate limit-up price (10% for main board)."""
        # Simplified: assumes 10% limit for all stocks
        # TODO: Handle 20% for ChiNext/STAR, 5% for ST stocks
        return round(self.prev_close * 1.1, 2)

    @property
    def limit_down_price(self) -> float:
        """Calculate limit-down price."""
        return round(self.prev_close * 0.9, 2)

    @property
    def change_percent(self) -> float:
        """Calculate change percentage from previous close."""
        if self.prev_close == 0:
            return 0.0
        return (self.close - self.prev_close) / self.prev_close * 100

    def is_limit_up(self) -> bool:
        """Check if stock closed at limit-up."""
        return abs(self.close - self.limit_up_price) < 0.01

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "stock_code": self.stock_code,
            "trade_date": self.trade_date.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "prev_close": self.prev_close,
            "volume": self.volume,
            "amount": self.amount,
            "limit_up_price": self.limit_up_price,
            "limit_down_price": self.limit_down_price,
            "change_percent": self.change_percent,
        }


class HistoricalPriceService:
    """
    Historical price data service using iFinD API.

    Fetches and caches historical OHLCV data for simulation.
    All trading-related price data uses iFinD per CLAUDE.md Section 14.

    Usage:
        service = HistoricalPriceService()
        await service.start()

        # Get daily data
        data = await service.get_daily_data("600519.SH", date(2026, 1, 29))

        # Get price at specific time
        price = await service.get_price_at_time("600519.SH", datetime(2026, 1, 29, 10, 30))

        await service.stop()
    """

    def __init__(self) -> None:
        """Initialize the price service."""
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ifind_hist")
        self._logged_in = False

        # Cache: (stock_code, date) -> DailyPriceData
        self._cache: dict[tuple[str, date], DailyPriceData] = {}

    async def start(self) -> None:
        """Start the service and login to iFinD."""
        if not await self._ensure_login():
            logger.warning("iFinD login failed - price service may not work")

    async def stop(self) -> None:
        """Stop the service and cleanup."""
        if self._logged_in:
            await self._logout()
        self._executor.shutdown(wait=False)
        self._cache.clear()

    async def _ensure_login(self) -> bool:
        """Ensure iFinD is logged in."""
        if self._logged_in:
            return True

        loop = asyncio.get_event_loop()
        self._logged_in = await loop.run_in_executor(self._executor, self._do_login)
        return self._logged_in

    def _do_login(self) -> bool:
        """Login to iFinD (runs in thread pool)."""
        try:
            from iFinDPy import THS_iFinDLogin

            username, password = get_ifind_credentials()
            result = THS_iFinDLogin(username, password)

            if result == 0:
                logger.info("iFinD login successful (price service)")
                return True
            else:
                logger.error(f"iFinD login failed with code: {result}")
                return False
        except ImportError as e:
            logger.error(f"iFinDPy module not available: {e}")
            return False
        except Exception as e:
            logger.error(f"iFinD login error: {e}")
            return False

    async def _logout(self) -> None:
        """Logout from iFinD."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self._executor, self._do_logout)

    def _do_logout(self) -> None:
        """Perform logout (runs in thread pool)."""
        try:
            from iFinDPy import THS_iFinDLogout

            THS_iFinDLogout()
            logger.info("iFinD logout (price service)")
        except Exception as e:
            logger.warning(f"iFinD logout error: {e}")

    async def get_daily_data(
        self,
        stock_code: str,
        trade_date: date,
    ) -> DailyPriceData | None:
        """
        Get daily OHLCV data for a stock.

        Args:
            stock_code: Stock code (e.g., "600519.SH")
            trade_date: Trading date

        Returns:
            DailyPriceData or None if not available.
        """
        # Check cache first
        cache_key = (stock_code, trade_date)
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Ensure logged in
        if not await self._ensure_login():
            logger.error("Cannot fetch price data - not logged in")
            return None

        # Fetch from iFinD
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(
            self._executor,
            self._fetch_daily_data,
            stock_code,
            trade_date,
        )

        if data:
            self._cache[cache_key] = data

        return data

    def _fetch_daily_data(
        self,
        stock_code: str,
        trade_date: date,
    ) -> DailyPriceData | None:
        """Fetch daily data from iFinD (runs in thread pool)."""
        try:
            from iFinDPy import THS_HistoryQuotes

            date_str = trade_date.strftime("%Y-%m-%d")

            # Fetch OHLCV data
            result = THS_HistoryQuotes(
                stock_code,
                "open;high;low;close;preClose;vol;amount",
                "",  # No params
                date_str,
                date_str,
            )

            # Parse result
            if isinstance(result, dict):
                if result.get("errorcode", 0) != 0:
                    err_msg = result.get("errmsg", "Unknown error")
                    logger.error(f"THS_HistoryQuotes error: {err_msg}")
                    return None

                tables = result.get("tables", [])
                if not tables:
                    logger.warning(f"No data for {stock_code} on {date_str}")
                    return None

                # Parse first table
                table = tables[0].get("table", {})

                # Extract values (arrays with single element for single day)
                open_vals = table.get("open", [])
                high_vals = table.get("high", [])
                low_vals = table.get("low", [])
                close_vals = table.get("close", [])
                prev_vals = table.get("preClose", [])
                vol_vals = table.get("vol", [])
                amt_vals = table.get("amount", [])

                if not all([open_vals, high_vals, low_vals, close_vals, prev_vals]):
                    logger.warning(f"Incomplete data for {stock_code} on {date_str}")
                    return None

                return DailyPriceData(
                    stock_code=stock_code,
                    trade_date=trade_date,
                    open=float(open_vals[0]) if open_vals else 0.0,
                    high=float(high_vals[0]) if high_vals else 0.0,
                    low=float(low_vals[0]) if low_vals else 0.0,
                    close=float(close_vals[0]) if close_vals else 0.0,
                    prev_close=float(prev_vals[0]) if prev_vals else 0.0,
                    volume=float(vol_vals[0]) if vol_vals else None,
                    amount=float(amt_vals[0]) if amt_vals else None,
                )

            logger.warning(f"Unexpected result type: {type(result)}")
            return None

        except ImportError as e:
            logger.error(f"iFinDPy not available: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching daily data: {e}")
            return None

    async def get_price_at_time(
        self,
        stock_code: str,
        target_time: datetime,
    ) -> float | None:
        """
        Get estimated price at a specific time.

        Uses daily OHLCV to interpolate/estimate price based on time:
        - Before 9:30: previous close
        - 9:30-9:35: open price
        - 9:35-14:57: interpolated between open and close
        - After 14:57: close price

        Args:
            stock_code: Stock code
            target_time: Target datetime

        Returns:
            Estimated price or None if data unavailable.
        """
        daily_data = await self.get_daily_data(stock_code, target_time.date())

        if not daily_data:
            return None

        current_time = target_time.time()

        # Before market open - use previous close
        if current_time < time(9, 30):
            return daily_data.prev_close

        # First 5 minutes - use open price
        if current_time < time(9, 35):
            return daily_data.open

        # Near close - use close price
        if current_time >= time(14, 57):
            return daily_data.close

        # During trading hours - interpolate
        # Simple linear interpolation between open and close
        # This is a simplification - real intraday prices vary
        return self._interpolate_price(daily_data, current_time)

    def _interpolate_price(
        self,
        data: DailyPriceData,
        current_time: time,
    ) -> float:
        """
        Interpolate price based on time of day.

        Uses a simple model that moves from open toward close,
        potentially touching high/low during the day.
        """
        # Trading minutes since open (9:35)
        open_minutes = 9 * 60 + 35
        close_minutes = 15 * 60
        current_minutes = current_time.hour * 60 + current_time.minute

        # Handle lunch break (11:30-13:00)
        if time(11, 30) <= current_time < time(13, 0):
            # During lunch, use morning end price
            current_minutes = 11 * 60 + 30

        # Adjust for afternoon (skip lunch break minutes)
        if current_time >= time(13, 0):
            current_minutes -= 90  # 1.5 hour lunch break

        total_minutes = close_minutes - open_minutes - 90  # Exclude lunch
        elapsed = min(current_minutes - open_minutes, total_minutes)

        if elapsed <= 0:
            return data.open
        if elapsed >= total_minutes:
            return data.close

        # Linear interpolation from open to close
        progress = elapsed / total_minutes
        return data.open + (data.close - data.open) * progress

    async def batch_get_daily_data(
        self,
        stock_codes: list[str],
        trade_date: date,
    ) -> dict[str, DailyPriceData]:
        """
        Get daily data for multiple stocks.

        Args:
            stock_codes: List of stock codes
            trade_date: Trading date

        Returns:
            Dict mapping stock code to DailyPriceData.
        """
        results: dict[str, DailyPriceData] = {}

        # Fetch in parallel using asyncio.gather
        tasks = [self.get_daily_data(code, trade_date) for code in stock_codes]
        data_list = await asyncio.gather(*tasks)

        for code, data in zip(stock_codes, data_list):
            if data:
                results[code] = data

        return results

    async def is_limit_up_at_open(
        self,
        stock_code: str,
        trade_date: date,
    ) -> bool:
        """
        Check if stock opened at limit-up price.

        Args:
            stock_code: Stock code
            trade_date: Trading date

        Returns:
            True if stock opened at limit-up.
        """
        data = await self.get_daily_data(stock_code, trade_date)

        if not data:
            # Conservative: assume limit-up if we can't verify
            # Per Trading Safety Priority Principle (CLAUDE.md Section 12)
            logger.warning(f"Cannot verify limit-up for {stock_code} - assuming limit-up")
            return True

        # Check if open price equals limit-up price
        return abs(data.open - data.limit_up_price) < 0.01

    def clear_cache(self) -> None:
        """Clear the price data cache."""
        self._cache.clear()

    def get_cached_count(self) -> int:
        """Get number of cached entries."""
        return len(self._cache)
