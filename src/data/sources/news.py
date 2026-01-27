# === MODULE PURPOSE ===
# Financial news source for fetching market news and articles.
# This is a template implementation that can be extended for real data sources.

# === KEY CONCEPTS ===
# - News: Financial news, market analysis, economic reports
# - Example implementation: Override _fetch_from_api() for real data source

import asyncio
import logging
import random
from datetime import datetime
from typing import AsyncIterator

from src.data.models.message import Message
from src.data.sources.base import BaseMessageSource

logger = logging.getLogger(__name__)


class NewsSource(BaseMessageSource):
    """
    Message source for financial news.

    This is a template implementation that generates sample data.
    To use with a real data source:
        1. Subclass this class
        2. Override _fetch_from_api() to fetch real news
        3. Parse the response into Message objects

    Example real sources:
        - eastmoney.com (东方财富)
        - sina.com.cn/finance (新浪财经)
        - wallstreetcn.com (华尔街见闻)
    """

    def __init__(self, interval: float = 30.0):
        super().__init__(interval=interval)
        self._last_fetch_time: datetime | None = None

    @property
    def source_type(self) -> str:
        return "news"

    @property
    def source_name(self) -> str:
        return "sample_news"

    async def fetch_messages(self) -> AsyncIterator[Message]:
        """
        Fetch news from the source.

        This sample implementation generates mock data.
        Override _fetch_from_api() for real implementation.
        """
        try:
            news_items = await self._fetch_from_api()
            for news in news_items:
                yield news
            self._last_fetch_time = datetime.now()
        except Exception as e:
            logger.error(f"Error fetching news: {e}")
            raise

    async def _fetch_from_api(self) -> list[Message]:
        """
        Fetch news from API.

        Override this method to implement real data fetching.
        This sample implementation returns mock data.
        """
        # Simulate API latency
        await asyncio.sleep(0.1)

        # Sample news topics
        sample_titles = [
            "央行今日开展逆回购操作",
            "A股三大指数集体高开",
            "北向资金净流入超百亿",
            "新能源板块持续走强",
            "消费电子行业迎来复苏",
            "半导体设备国产替代加速",
            "医药生物板块震荡整理",
        ]

        sample_stocks = ["000001", "600519", "300750", "002475", "601012"]

        messages = []
        num_messages = random.randint(0, 5)

        for _ in range(num_messages):
            title = random.choice(sample_titles)
            # Some news may be related to specific stocks
            stock_codes = (
                [random.choice(sample_stocks)] if random.random() > 0.5 else []
            )

            message = Message(
                source_type=self.source_type,
                source_name=self.source_name,
                title=title,
                content=f"这是一条示例新闻内容。标题：{title}。详细报道...",
                url=f"https://example.com/news/{random.randint(10000, 99999)}",
                stock_codes=stock_codes,
                publish_time=datetime.now(),
            )
            messages.append(message)

        if messages:
            logger.debug(f"Fetched {len(messages)} news items")

        return messages
