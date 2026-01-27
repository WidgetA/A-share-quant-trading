# === MODULE PURPOSE ===
# East Money (东方财富) news source for fetching financial news.
# Uses akshare library to fetch news from eastmoney.com.

# === DEPENDENCIES ===
# - akshare: Financial data interface library
# - BaseMessageSource: Base class for all message sources

# === KEY CONCEPTS ===
# - Global news (全球财经快讯): Real-time global financial news
# - Financial breakfast (财经早餐): Daily morning news digest

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import AsyncIterator

import pandas as pd

from src.data.models.message import Message
from src.data.sources.base import BaseMessageSource

logger = logging.getLogger(__name__)


class EastmoneyNewsSource(BaseMessageSource):
    """
    Message source for East Money (东方财富) financial news.

    Uses akshare's stock_info_global_em interface to fetch
    real-time global financial news.

    Data Flow:
        akshare.stock_info_global_em() -> DataFrame -> Message objects
    """

    def __init__(self, interval: float = 60.0):
        """
        Initialize the East Money news source.

        Args:
            interval: Polling interval in seconds (default 60s)
        """
        super().__init__(interval=interval)
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="eastmoney")

    @property
    def source_type(self) -> str:
        return "news"

    @property
    def source_name(self) -> str:
        return "eastmoney"

    async def start(self) -> None:
        """Start the East Money news source."""
        await super().start()
        logger.info("East Money news source started")

    async def stop(self) -> None:
        """Stop and cleanup."""
        self._executor.shutdown(wait=False)
        await super().stop()

    async def fetch_messages(self) -> AsyncIterator[Message]:
        """
        Fetch latest news from East Money.

        Yields deduplicated Message objects.
        """
        loop = asyncio.get_event_loop()

        try:
            df = await loop.run_in_executor(self._executor, self._fetch_news)
            async for msg in self._process_dataframe(df):
                yield msg
        except Exception as e:
            logger.error(f"Error fetching East Money news: {e}")

    async def fetch_historical(self, days: int = 7) -> AsyncIterator[Message]:
        """
        Fetch historical news.

        Note: East Money API may only return recent news.

        Args:
            days: Number of days (limited by API)

        Yields:
            Message objects
        """
        logger.info("Fetching historical East Money news (limited by API availability)")
        async for msg in self.fetch_messages():
            yield msg

    def _fetch_news(self) -> pd.DataFrame:
        """
        Fetch news from East Money via akshare.

        Runs in thread pool to avoid blocking.
        """
        try:
            import akshare as ak

            df = ak.stock_info_global_em()
            logger.debug(f"Fetched {len(df)} East Money news items")
            return df
        except Exception as e:
            logger.error(f"akshare East Money fetch error: {e}")
            return pd.DataFrame()

    async def _process_dataframe(self, df: pd.DataFrame) -> AsyncIterator[Message]:
        """
        Convert DataFrame rows to Message objects.

        Expected columns from akshare:
            - 时间: Time
            - 内容: Content
            - Or other column names depending on API version

        Yields:
            Deduplicated Message objects
        """
        if df.empty:
            return

        for _, row in df.iterrows():
            try:
                # Try different column names for content
                content = str(
                    row.get("内容", row.get("content", row.get("摘要", "")))
                )
                if not content:
                    continue

                # Try different column names for time
                time_str = str(
                    row.get("时间", row.get("time", row.get("发布时间", "")))
                )

                publish_time = self._parse_datetime(time_str)

                # Use first 50 chars of content as title
                title = content[:50] + "..." if len(content) > 50 else content

                # Extract stock codes from content
                stock_codes = self._extract_stock_codes(content)

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
                    url="https://www.eastmoney.com/",
                    stock_codes=stock_codes,
                    publish_time=publish_time,
                    raw_data=row.to_dict(),
                )

                yield message

            except Exception as e:
                logger.error(f"Error processing East Money row: {e}")
                continue

    def _parse_datetime(self, time_str: str) -> datetime:
        """Parse datetime from East Money format."""
        now = datetime.now()

        if not time_str or time_str == "nan":
            return now

        # Common formats from East Money
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%m-%d %H:%M",
            "%H:%M:%S",
            "%H:%M",
        ]

        for fmt in formats:
            try:
                if fmt in ["%H:%M:%S", "%H:%M"]:
                    time_part = datetime.strptime(time_str, fmt).time()
                    return datetime.combine(now.date(), time_part)
                elif fmt == "%m-%d %H:%M":
                    parsed = datetime.strptime(time_str, fmt)
                    return parsed.replace(year=now.year)
                else:
                    return datetime.strptime(time_str, fmt)
            except ValueError:
                continue

        return now

    def _extract_stock_codes(self, content: str) -> list[str]:
        """Extract stock codes from content."""
        import re

        codes = []
        # Match 6-digit codes starting with 0, 3, or 6
        matches = re.findall(r"\b([036]\d{5})\b", content)
        codes.extend(matches)

        return list(set(codes))
