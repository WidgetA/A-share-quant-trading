# === MODULE PURPOSE ===
# CLS (Cailian Press / 财联社) news source for fetching financial news.
# Uses akshare library to fetch news from cls.cn telegraph service.

# === DEPENDENCIES ===
# - akshare: Financial data interface library
# - BaseMessageSource: Base class for all message sources

# === KEY CONCEPTS ===
# - Telegraph (电报): Real-time financial news updates from CLS
# - Supports both "全部" (all) and "重点" (key) news categories

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import AsyncIterator

import pandas as pd

from src.data.models.message import Message
from src.data.sources.base import BaseMessageSource

logger = logging.getLogger(__name__)


class CLSNewsSource(BaseMessageSource):
    """
    Message source for CLS (财联社) financial news.

    Uses akshare's stock_telegraph_cls interface to fetch
    real-time financial news from cls.cn.

    Data Flow:
        akshare.stock_telegraph_cls() -> DataFrame -> Message objects
    """

    def __init__(self, interval: float = 30.0, symbol: str = "全部"):
        """
        Initialize the CLS news source.

        Args:
            interval: Polling interval in seconds (default 30s)
            symbol: News category - "全部" (all) or "重点" (key)
        """
        super().__init__(interval=interval)
        self.symbol = symbol
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="cls")

    @property
    def source_type(self) -> str:
        return "news"

    @property
    def source_name(self) -> str:
        return "cls"

    async def start(self) -> None:
        """Start the CLS news source."""
        await super().start()
        logger.info(f"CLS news source started with symbol={self.symbol}")

    async def stop(self) -> None:
        """Stop and cleanup."""
        self._executor.shutdown(wait=False)
        await super().stop()

    async def fetch_messages(self) -> AsyncIterator[Message]:
        """
        Fetch latest news from CLS.

        Yields deduplicated Message objects.
        """
        loop = asyncio.get_event_loop()

        try:
            df = await loop.run_in_executor(self._executor, self._fetch_news)
            async for msg in self._process_dataframe(df):
                yield msg
        except Exception as e:
            logger.error(f"Error fetching CLS news: {e}")

    async def fetch_historical(self, days: int = 7) -> AsyncIterator[Message]:
        """
        Fetch historical news.

        Note: CLS telegraph API only returns recent news,
        historical data may be limited.

        Args:
            days: Number of days (limited by API)

        Yields:
            Message objects
        """
        logger.info(f"Fetching historical CLS news (limited by API availability)")
        async for msg in self.fetch_messages():
            yield msg

    def _fetch_news(self) -> pd.DataFrame:
        """
        Fetch news from CLS via akshare.

        Runs in thread pool to avoid blocking.
        """
        try:
            import akshare as ak

            df = ak.stock_telegraph_cls(symbol=self.symbol)
            logger.debug(f"Fetched {len(df)} CLS news items")
            return df
        except Exception as e:
            logger.error(f"akshare CLS fetch error: {e}")
            return pd.DataFrame()

    async def _process_dataframe(self, df: pd.DataFrame) -> AsyncIterator[Message]:
        """
        Convert DataFrame rows to Message objects.

        Expected columns from akshare:
            - 发布时间: Publish time
            - 内容: Content
            - 发布日期: Publish date (sometimes present)

        Yields:
            Deduplicated Message objects
        """
        if df.empty:
            return

        for _, row in df.iterrows():
            try:
                # Extract content
                content = str(row.get("内容", row.get("content", "")))
                if not content:
                    continue

                # Parse publish time
                time_str = str(row.get("发布时间", row.get("time", "")))
                date_str = str(row.get("发布日期", ""))

                publish_time = self._parse_datetime(time_str, date_str)

                # Use first 50 chars of content as title
                title = content[:50] + "..." if len(content) > 50 else content

                # Extract stock codes from content (format: $000001$)
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
                    url="https://www.cls.cn/telegraph",
                    stock_codes=stock_codes,
                    publish_time=publish_time,
                    raw_data=row.to_dict(),
                )

                yield message

            except Exception as e:
                logger.error(f"Error processing CLS row: {e}")
                continue

    def _parse_datetime(self, time_str: str, date_str: str = "") -> datetime:
        """Parse datetime from CLS format."""
        now = datetime.now()

        # Try parsing full datetime
        if date_str and time_str:
            try:
                return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass

        # Try parsing time only (assume today)
        if time_str:
            try:
                time_part = datetime.strptime(time_str, "%H:%M:%S").time()
                return datetime.combine(now.date(), time_part)
            except ValueError:
                pass

            try:
                time_part = datetime.strptime(time_str, "%H:%M").time()
                return datetime.combine(now.date(), time_part)
            except ValueError:
                pass

        return now

    def _extract_stock_codes(self, content: str) -> list[str]:
        """Extract stock codes from content (format: $000001$ or $600519$)."""
        import re

        codes = []
        # Match patterns like $000001$ or $600519$
        matches = re.findall(r"\$(\d{6})\$", content)
        codes.extend(matches)

        # Also match plain 6-digit codes that look like stock codes
        # (starting with 0, 3, or 6)
        matches = re.findall(r"\b([036]\d{5})\b", content)
        codes.extend(matches)

        return list(set(codes))  # Remove duplicates
