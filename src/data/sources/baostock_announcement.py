# === MODULE PURPOSE ===
# Baostock announcement source for fetching performance reports and forecasts.
# Uses baostock library to fetch official company announcements.

# === DEPENDENCIES ===
# - baostock: Free stock data API for China A-share market
# - BaseMessageSource: Base class for all message sources

# === KEY CONCEPTS ===
# - Performance Express Report (业绩快报): Quick financial summary after quarter end
# - Forecast Report (业绩预告): Earnings guidance before official report

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import AsyncIterator

import baostock as bs
import pandas as pd

from src.data.models.message import Message
from src.data.sources.base import BaseMessageSource

logger = logging.getLogger(__name__)


class BaostockAnnouncementSource(BaseMessageSource):
    """
    Message source for stock announcements via Baostock.

    Fetches:
        - Performance express reports (业绩快报)
        - Forecast reports (业绩预告)

    Data Flow:
        Baostock API -> DataFrame -> Message objects

    Note:
        Baostock is a synchronous library, so we run it in a thread pool
        to avoid blocking the async event loop.
    """

    def __init__(self, interval: float = 300.0):
        """
        Initialize the Baostock announcement source.

        Args:
            interval: Polling interval in seconds (default 5 minutes)
        """
        super().__init__(interval=interval)
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="baostock")
        self._logged_in = False

    @property
    def source_type(self) -> str:
        return "announcement"

    @property
    def source_name(self) -> str:
        return "baostock"

    async def start(self) -> None:
        """Initialize Baostock connection."""
        await super().start()
        # Login in thread pool
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self._executor, self._login)

    async def stop(self) -> None:
        """Logout and cleanup."""
        if self._logged_in:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(self._executor, self._logout)
        self._executor.shutdown(wait=False)
        await super().stop()

    def _login(self) -> None:
        """Login to Baostock (sync, runs in thread pool)."""
        try:
            lg = bs.login()
            if lg.error_code == "0":
                self._logged_in = True
                logger.info("Baostock login successful")
            else:
                logger.error(f"Baostock login failed: {lg.error_msg}")
        except Exception as e:
            logger.error(f"Baostock login error: {e}")

    def _logout(self) -> None:
        """Logout from Baostock (sync, runs in thread pool)."""
        try:
            bs.logout()
            self._logged_in = False
            logger.info("Baostock logout successful")
        except Exception as e:
            logger.error(f"Baostock logout error: {e}")

    async def fetch_messages(self) -> AsyncIterator[Message]:
        """
        Fetch latest announcements from Baostock.

        Yields deduplicated Message objects for:
            - Performance express reports
            - Forecast reports
        """
        if not self._logged_in:
            logger.warning("Baostock not logged in, skipping fetch")
            return

        loop = asyncio.get_event_loop()

        # Fetch performance express reports
        try:
            df = await loop.run_in_executor(
                self._executor, self._fetch_performance_express
            )
            async for msg in self._process_dataframe(df, "performance_express"):
                yield msg
        except Exception as e:
            logger.error(f"Error fetching performance express: {e}")

        # Fetch forecast reports
        try:
            df = await loop.run_in_executor(
                self._executor, self._fetch_forecast
            )
            async for msg in self._process_dataframe(df, "forecast"):
                yield msg
        except Exception as e:
            logger.error(f"Error fetching forecast: {e}")

    async def fetch_historical(self, days: int = 30) -> AsyncIterator[Message]:
        """
        Fetch historical announcements.

        Args:
            days: Number of days of historical data

        Yields:
            Historical Message objects
        """
        if not self._logged_in:
            logger.warning("Baostock not logged in, skipping historical fetch")
            return

        loop = asyncio.get_event_loop()
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Fetching historical announcements from {start_date} to {end_date}")

        # Fetch performance express reports
        try:
            df = await loop.run_in_executor(
                self._executor,
                lambda: self._fetch_performance_express(start_date, end_date),
            )
            async for msg in self._process_dataframe(df, "performance_express"):
                yield msg
        except Exception as e:
            logger.error(f"Error fetching historical performance express: {e}")

        # Fetch forecast reports
        try:
            df = await loop.run_in_executor(
                self._executor,
                lambda: self._fetch_forecast(start_date, end_date),
            )
            async for msg in self._process_dataframe(df, "forecast"):
                yield msg
        except Exception as e:
            logger.error(f"Error fetching historical forecast: {e}")

    def _fetch_performance_express(
        self, start_date: str | None = None, end_date: str | None = None
    ) -> pd.DataFrame:
        """
        Fetch performance express reports (业绩快报).

        Runs in thread pool.
        """
        if start_date is None:
            # Default to last 7 days
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        rs = bs.query_performance_express_report(start_date=start_date, end_date=end_date)

        if rs.error_code != "0":
            logger.error(f"Baostock performance express error: {rs.error_msg}")
            return pd.DataFrame()

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            return pd.DataFrame()

        return pd.DataFrame(data_list, columns=rs.fields)

    def _fetch_forecast(
        self, start_date: str | None = None, end_date: str | None = None
    ) -> pd.DataFrame:
        """
        Fetch forecast reports (业绩预告).

        Runs in thread pool.
        """
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        rs = bs.query_forecast_report(start_date=start_date, end_date=end_date)

        if rs.error_code != "0":
            logger.error(f"Baostock forecast error: {rs.error_msg}")
            return pd.DataFrame()

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            return pd.DataFrame()

        return pd.DataFrame(data_list, columns=rs.fields)

    async def _process_dataframe(
        self, df: pd.DataFrame, report_type: str
    ) -> AsyncIterator[Message]:
        """
        Convert DataFrame rows to Message objects.

        Args:
            df: DataFrame with announcement data
            report_type: Type of report (performance_express/forecast)

        Yields:
            Deduplicated Message objects
        """
        if df.empty:
            return

        for _, row in df.iterrows():
            try:
                # Extract stock code
                stock_code = row.get("code", "")
                if stock_code.startswith("sh.") or stock_code.startswith("sz."):
                    stock_code = stock_code[3:]

                # Parse publish date
                pub_date_str = row.get("publishDate", row.get("update_date", ""))
                if pub_date_str:
                    try:
                        publish_time = datetime.strptime(pub_date_str, "%Y-%m-%d")
                    except ValueError:
                        publish_time = datetime.now()
                else:
                    publish_time = datetime.now()

                # Build title based on report type
                if report_type == "performance_express":
                    title = f"[{stock_code}] 业绩快报 - {row.get('performanceExpStatDate', '')}"
                    content = self._format_performance_express(row)
                else:
                    title = f"[{stock_code}] 业绩预告 - {row.get('reportDate', '')}"
                    content = self._format_forecast(row)

                # Generate unique ID and check for duplicates
                msg_id = self.generate_message_id(title, publish_time)
                if self.is_duplicate(msg_id):
                    continue

                message = Message(
                    id=msg_id,
                    source_type=self.source_type,
                    source_name=self.source_name,
                    title=title,
                    content=content,
                    stock_codes=[stock_code] if stock_code else [],
                    publish_time=publish_time,
                    raw_data=row.to_dict(),
                )

                yield message

            except Exception as e:
                logger.error(f"Error processing row: {e}")
                continue

    def _format_performance_express(self, row: pd.Series) -> str:
        """Format performance express report content."""
        parts = [
            f"股票代码: {row.get('code', 'N/A')}",
            f"统计截止日: {row.get('performanceExpStatDate', 'N/A')}",
            f"发布日期: {row.get('publishDate', 'N/A')}",
            f"每股收益: {row.get('performanceExpEPS', 'N/A')}",
            f"净资产收益率: {row.get('performanceExpROE', 'N/A')}",
        ]
        return "\n".join(parts)

    def _format_forecast(self, row: pd.Series) -> str:
        """Format forecast report content."""
        parts = [
            f"股票代码: {row.get('code', 'N/A')}",
            f"报告期: {row.get('reportDate', 'N/A')}",
            f"业绩类型: {row.get('type', 'N/A')}",
            f"业绩变动原因: {row.get('changeReason', 'N/A')}",
        ]
        return "\n".join(parts)
