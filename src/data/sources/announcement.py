# === MODULE PURPOSE ===
# Stock announcement source for fetching company announcements and filings.
# This is a template implementation that can be extended for real data sources.

# === KEY CONCEPTS ===
# - Announcements: Official company disclosures (annual reports, earnings, etc.)
# - Example implementation: Override _fetch_from_api() for real data source

import asyncio
import logging
import random
from datetime import datetime
from typing import AsyncIterator

from src.data.models.message import Message
from src.data.sources.base import BaseMessageSource

logger = logging.getLogger(__name__)


class AnnouncementSource(BaseMessageSource):
    """
    Message source for stock announcements.

    This is a template implementation that generates sample data.
    To use with a real data source:
        1. Subclass this class
        2. Override _fetch_from_api() to fetch real announcements
        3. Parse the response into Message objects

    Example real sources:
        - cninfo.com.cn (巨潮资讯)
        - sse.com.cn (上海证券交易所)
        - szse.cn (深圳证券交易所)
    """

    def __init__(self, interval: float = 60.0):
        super().__init__(interval=interval)
        self._last_fetch_time: datetime | None = None

    @property
    def source_type(self) -> str:
        return "announcement"

    @property
    def source_name(self) -> str:
        return "sample_announcement"

    async def fetch_messages(self) -> AsyncIterator[Message]:
        """
        Fetch announcements from the source.

        This sample implementation generates mock data.
        Override _fetch_from_api() for real implementation.
        """
        try:
            announcements = await self._fetch_from_api()
            for announcement in announcements:
                yield announcement
            self._last_fetch_time = datetime.now()
        except Exception as e:
            logger.error(f"Error fetching announcements: {e}")
            raise

    async def _fetch_from_api(self) -> list[Message]:
        """
        Fetch announcements from API.

        Override this method to implement real data fetching.
        This sample implementation returns mock data.
        """
        # Simulate API latency
        await asyncio.sleep(0.1)

        # Sample announcements (replace with real API call)
        sample_stocks = ["000001", "600519", "000858", "601318", "000333"]
        sample_titles = [
            "关于召开年度股东大会的通知",
            "第三季度报告",
            "关于重大合同的公告",
            "董事会决议公告",
            "关于回购股份的进展公告",
        ]

        messages = []
        num_messages = random.randint(0, 3)

        for _ in range(num_messages):
            stock_code = random.choice(sample_stocks)
            title = random.choice(sample_titles)

            message = Message(
                source_type=self.source_type,
                source_name=self.source_name,
                title=f"[{stock_code}] {title}",
                content=f"这是一条示例公告内容。股票代码：{stock_code}，公告类型：{title}",
                url=f"https://example.com/announcement/{stock_code}",
                stock_codes=[stock_code],
                publish_time=datetime.now(),
            )
            messages.append(message)

        if messages:
            logger.debug(f"Fetched {len(messages)} announcements")

        return messages
