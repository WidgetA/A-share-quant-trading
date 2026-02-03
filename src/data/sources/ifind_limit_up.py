# === MODULE PURPOSE ===
# iFinD data source for fetching daily limit-up (涨停) stock information.
# Retrieves all stocks that hit the daily price limit after market close.

# === DEPENDENCIES ===
# - iFinDPy: THS iFinD SDK for A-share market data
# - LimitUpStock model: Data structure for limit-up stocks
# - LimitUpDatabase: SQLite storage layer

# === KEY CONCEPTS ===
# - Limit-up (涨停): Stock reaches maximum daily price increase limit
#   - Main board: +10% (or +5% for ST stocks)
#   - ChiNext/STAR: +20%
# - Best called after market close (15:00 Beijing time)
# - Uses THS_iwencai API (问财) for natural language query

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.common.config import Config, get_ifind_credentials
from src.data.database.limit_up_db import LimitUpDatabase
from src.data.models.limit_up import LimitUpStock

logger = logging.getLogger(__name__)


class IFinDLimitUpSource:
    """
    Data source for fetching limit-up stocks via iFinD API.

    Data Flow:
        iFinD THS_DataPool API -> DataFrame -> LimitUpStock -> LimitUpDatabase

    Usage:
        source = IFinDLimitUpSource()
        await source.start()

        # Fetch today's limit-up stocks
        stocks = await source.fetch_limit_up_stocks()

        # Fetch and save to database
        count = await source.fetch_and_save()

        # Backfill historical data
        await source.backfill(days=30)

        await source.stop()
    """

    def __init__(
        self,
        db_path: str | Path = "data/limit_up.db",
        config: Config | None = None,
    ):
        """
        Initialize the iFinD limit-up data source.

        Args:
            db_path: Path to SQLite database
            config: Optional configuration (for future extensibility)
        """
        self.db_path = Path(db_path)
        self.config = config
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ifind")
        self._database: LimitUpDatabase | None = None
        self._logged_in = False

    async def start(self) -> None:
        """Initialize resources and connect to database."""
        self._database = LimitUpDatabase(self.db_path)
        await self._database.connect()
        logger.info("IFinD limit-up source started")

    async def stop(self) -> None:
        """Cleanup resources."""
        if self._database:
            await self._database.close()
            self._database = None

        # Logout before shutting down executor (logout uses executor)
        if self._logged_in:
            await self._ifind_logout()

        self._executor.shutdown(wait=False)

        logger.info("IFinD limit-up source stopped")

    async def _ensure_login(self) -> bool:
        """Ensure iFinD is logged in."""
        if self._logged_in:
            return True

        loop = asyncio.get_event_loop()
        self._logged_in = await loop.run_in_executor(self._executor, self._ifind_login)
        return self._logged_in

    def _ifind_login(self) -> bool:
        """Login to iFinD API (runs in thread pool)."""
        try:
            from iFinDPy import THS_iFinDLogin

            username, password = get_ifind_credentials()
            result = THS_iFinDLogin(username, password)

            if result == 0:
                logger.info("iFinD login successful")
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

    async def _ifind_logout(self) -> None:
        """Logout from iFinD API."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self._executor, self._do_logout)

    def _do_logout(self) -> None:
        """Perform logout (runs in thread pool)."""
        try:
            from iFinDPy import THS_iFinDLogout

            THS_iFinDLogout()
            logger.info("iFinD logout successful")
        except Exception as e:
            logger.warning(f"iFinD logout error: {e}")

    async def fetch_limit_up_stocks(
        self,
        trade_date: str | None = None,
    ) -> list[LimitUpStock]:
        """
        Fetch limit-up stocks for a specific date.

        Args:
            trade_date: Trading date in YYYY-MM-DD format (default: today)

        Returns:
            List of LimitUpStock objects

        Raises:
            RuntimeError: If iFinD login fails
        """
        if not await self._ensure_login():
            raise RuntimeError("Failed to login to iFinD")

        if trade_date is None:
            trade_date = datetime.now().strftime("%Y-%m-%d")

        loop = asyncio.get_event_loop()
        raw_data = await loop.run_in_executor(
            self._executor,
            self._fetch_limit_up_data,
            trade_date,
        )

        if raw_data is None:
            logger.warning(f"No limit-up data returned for {trade_date}")
            return []

        stocks = self._process_raw_data(raw_data, trade_date)
        logger.info(f"Fetched {len(stocks)} limit-up stocks for {trade_date}")
        return stocks

    def _fetch_limit_up_data(self, trade_date: str) -> dict[str, Any] | None:
        """
        Fetch limit-up data from iFinD API (runs in thread pool).

        Uses THS_iwencai (问财) for natural language query.
        """
        try:
            from iFinDPy import THS_iwencai

            # Build query string with date and required fields
            # Format date for query: YYYY-MM-DD -> YYYYMMDD or use relative date
            today = datetime.now().strftime("%Y-%m-%d")
            query_fields = "首次涨停时间 涨停原因类型 涨停开板次数 成交额 换手率 所属行业"
            if trade_date == today:
                query = f"今日涨停 {query_fields}"
            else:
                # For historical dates, specify the date in query
                date_str = trade_date.replace("-", "")
                query = f"{date_str}涨停 {query_fields}"

            result = THS_iwencai(query, "stock")

            # THS_iwencai returns OrderedDict with errorcode
            if isinstance(result, dict):
                if result.get("errorcode", 0) != 0:
                    err_msg = result.get("errmsg")
                    err_code = result.get("errorcode")
                    logger.error(f"THS_iwencai error: {err_msg} (code: {err_code})")
                    return None
                logger.debug(f"THS_iwencai returned {len(result.get('tables', []))} tables")
                return result
            else:
                logger.warning(f"Unexpected THS_iwencai return type: {type(result)}")
                return None

        except ImportError as e:
            logger.error(f"iFinDPy module not available: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching limit-up data: {e}")
            return None

    def _process_raw_data(
        self,
        raw_data: dict[str, Any],
        trade_date: str,
    ) -> list[LimitUpStock]:
        """
        Process raw iFinD data into LimitUpStock objects.

        Args:
            raw_data: Raw data from THS_iwencai
            trade_date: Trading date

        Returns:
            List of LimitUpStock objects
        """
        stocks = []

        # THS_iwencai returns data as:
        # {'tables': [{'table': {'股票代码': [...], '股票简称': [...], ...}}]}
        # Column names are in Chinese and may include date suffixes like [20260127]
        try:
            if not raw_data:
                return stocks

            tables = raw_data.get("tables", [])
            if not tables:
                logger.warning("No tables in response")
                return stocks

            for table_wrapper in tables:
                if not isinstance(table_wrapper, dict):
                    continue

                # iwencai wraps data in 'table' key
                table = table_wrapper.get("table", table_wrapper)
                if not isinstance(table, dict):
                    continue

                # Find columns by matching partial Chinese names
                # Column names may have date suffix like "首次涨停时间[20260127]"
                thscodes = self._find_column(table, ["股票代码", "thscode"])
                names = self._find_column(table, ["股票简称", "股票名称", "security_name"])
                first_times = self._find_column(table, ["首次涨停时间", "涨停时间"])
                open_counts = self._find_column(table, ["涨停开板次数", "开板次数"])
                turnovers = self._find_column(table, ["换手率"])
                amounts = self._find_column(table, ["成交额"])
                reasons = self._find_column(table, ["涨停原因", "涨停原因类型"])
                industries = self._find_column(table, ["所属行业", "行业"])

                if not thscodes:
                    logger.warning("No stock codes found in table")
                    continue

                logger.debug(f"Found {len(thscodes)} stocks in table")

                for i in range(len(thscodes)):
                    try:
                        # Parse limit-up time - may be full datetime like "2026-01-27 13:01:37"
                        time_str = self._safe_get(first_times, i, "")
                        limit_up_time = self._parse_limit_up_time(time_str)

                        stock = LimitUpStock(
                            trade_date=trade_date,
                            stock_code=self._safe_get(thscodes, i, ""),
                            stock_name=self._safe_get(names, i, ""),
                            limit_up_price=0.0,  # Not available from iwencai query
                            limit_up_time=limit_up_time,
                            open_count=self._safe_int(open_counts, i, 0),
                            turnover_rate=self._safe_float(turnovers, i),
                            amount=self._safe_float(amounts, i),
                            reason=self._safe_get(reasons, i),
                            industry=self._safe_get(industries, i),
                        )
                        stocks.append(stock)
                    except Exception as e:
                        logger.error(f"Error processing row {i}: {e}")
                        continue

        except Exception as e:
            logger.error(f"Error processing raw data: {e}")

        return stocks

    def _find_column(self, table: dict, names: list[str]) -> list:
        """Find column by matching partial Chinese names."""
        for col_name, col_data in table.items():
            for name in names:
                if name in col_name:
                    return col_data if isinstance(col_data, list) else []
        return []

    def _parse_limit_up_time(self, time_str: str | None) -> str:
        """Parse limit-up time from various formats."""
        if not time_str or time_str in ("--", ""):
            return ""

        time_str = str(time_str).strip()

        # Format: "2026-01-27 13:01:37" -> "13:01:37"
        if " " in time_str:
            parts = time_str.split(" ")
            if len(parts) >= 2:
                return parts[1]

        # Already in HH:MM:SS format
        if ":" in time_str:
            return time_str

        # Format time if numeric
        return self._format_time(time_str)

    def _safe_get(self, lst: list, index: int, default: Any = None) -> Any:
        """Safely get item from list."""
        try:
            if lst and index < len(lst):
                value = lst[index]
                return value if value not in (None, "", "--") else default
            return default
        except (IndexError, TypeError):
            return default

    def _safe_float(self, lst: list, index: int, default: float | None = None) -> float | None:
        """Safely get float from list."""
        value = self._safe_get(lst, index)
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _safe_int(self, lst: list, index: int, default: int = 0) -> int:
        """Safely get int from list."""
        value = self._safe_get(lst, index)
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def _format_time(self, time_str: str | None) -> str:
        """Format time string to HH:MM:SS."""
        if not time_str or time_str in ("--", ""):
            return ""

        # Handle various time formats
        time_str = str(time_str).strip()

        # Already in HH:MM:SS format
        if len(time_str) == 8 and ":" in time_str:
            return time_str

        # Format: HHMMSS or HHMM
        if time_str.isdigit():
            if len(time_str) == 6:
                return f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
            elif len(time_str) == 4:
                return f"{time_str[:2]}:{time_str[2:4]}:00"

        return time_str

    async def fetch_and_save(
        self,
        trade_date: str | None = None,
    ) -> int:
        """
        Fetch limit-up stocks and save to database.

        Args:
            trade_date: Trading date (default: today)

        Returns:
            Number of stocks saved
        """
        if self._database is None:
            raise RuntimeError("Database not initialized. Call start() first.")

        stocks = await self.fetch_limit_up_stocks(trade_date)
        if stocks:
            count = await self._database.save_batch(stocks)
            logger.info(f"Saved {count} limit-up stocks to database")
            return count
        return 0

    async def backfill(
        self,
        days: int = 30,
        end_date: str | None = None,
    ) -> dict[str, int]:
        """
        Backfill historical limit-up data.

        Args:
            days: Number of trading days to backfill
            end_date: End date (default: today)

        Returns:
            Dict mapping dates to number of stocks fetched
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        results: dict[str, int] = {}
        current_date = datetime.strptime(end_date, "%Y-%m-%d")

        for i in range(days):
            date_str = current_date.strftime("%Y-%m-%d")

            # Skip weekends
            if current_date.weekday() >= 5:
                current_date -= timedelta(days=1)
                continue

            try:
                count = await self.fetch_and_save(date_str)
                results[date_str] = count
                logger.info(f"Backfilled {date_str}: {count} stocks")
            except Exception as e:
                logger.error(f"Error backfilling {date_str}: {e}")
                results[date_str] = 0

            current_date -= timedelta(days=1)

            # Small delay to avoid overwhelming the API
            await asyncio.sleep(0.5)

        return results

    async def query_by_date(self, trade_date: str) -> list[LimitUpStock]:
        """Query stored limit-up stocks by date."""
        if self._database is None:
            raise RuntimeError("Database not initialized. Call start() first.")
        return await self._database.query_by_date(trade_date)

    async def get_statistics(
        self,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        """
        Get statistics for limit-up stocks over a date range.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Dict with statistics (total_count, avg_per_day, etc.)
        """
        if self._database is None:
            raise RuntimeError("Database not initialized. Call start() first.")

        dates = await self._database.get_dates_with_data(start_date, end_date)
        total_count = 0
        daily_counts = []

        for date in dates:
            count = await self._database.count_by_date(date)
            total_count += count
            daily_counts.append(count)

        return {
            "start_date": start_date,
            "end_date": end_date,
            "trading_days": len(dates),
            "total_stocks": total_count,
            "avg_per_day": total_count / len(dates) if dates else 0,
            "max_per_day": max(daily_counts) if daily_counts else 0,
            "min_per_day": min(daily_counts) if daily_counts else 0,
        }
