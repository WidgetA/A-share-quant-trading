# === MODULE PURPOSE ===
# Social media source for fetching discussions and posts about stocks.
# This is a template implementation that can be extended for real data sources.

# === KEY CONCEPTS ===
# - Social: User discussions, opinions, sentiment from social platforms
# - Example implementation: Override _fetch_from_api() for real data source

import asyncio
import logging
import random
from datetime import datetime
from typing import AsyncIterator

from src.data.models.message import Message
from src.data.sources.base import BaseMessageSource

logger = logging.getLogger(__name__)


class SocialMediaSource(BaseMessageSource):
    """
    Message source for social media discussions.

    This is a template implementation that generates sample data.
    To use with a real data source:
        1. Subclass this class
        2. Override _fetch_from_api() to fetch real posts
        3. Parse the response into Message objects

    Example real sources:
        - xueqiu.com (雪球)
        - guba.eastmoney.com (东方财富股吧)
        - weibo.com (微博财经)
    """

    def __init__(self, interval: float = 120.0):
        super().__init__(interval=interval)
        self._last_fetch_time: datetime | None = None

    @property
    def source_type(self) -> str:
        return "social"

    @property
    def source_name(self) -> str:
        return "sample_social"

    async def fetch_messages(self) -> AsyncIterator[Message]:
        """
        Fetch social media posts from the source.

        This sample implementation generates mock data.
        Override _fetch_from_api() for real implementation.
        """
        try:
            posts = await self._fetch_from_api()
            for post in posts:
                yield post
            self._last_fetch_time = datetime.now()
        except Exception as e:
            logger.error(f"Error fetching social posts: {e}")
            raise

    async def _fetch_from_api(self) -> list[Message]:
        """
        Fetch posts from API.

        Override this method to implement real data fetching.
        This sample implementation returns mock data.
        """
        # Simulate API latency
        await asyncio.sleep(0.1)

        # Sample social media content
        sample_contents = [
            "看好这只股票的长期发展",
            "今天大盘走势不错，继续持有",
            "技术面分析：短期有回调压力",
            "基本面良好，业绩预期向上",
            "注意风险，谨慎操作",
            "板块轮动明显，关注调仓机会",
        ]

        sample_stocks = ["000001", "600519", "300750", "002475", "601012", "000858"]

        messages = []
        num_messages = random.randint(0, 4)

        for _ in range(num_messages):
            content = random.choice(sample_contents)
            stock_code = random.choice(sample_stocks)

            message = Message(
                source_type=self.source_type,
                source_name=self.source_name,
                title=f"关于 {stock_code} 的讨论",
                content=f"${stock_code}$ {content}",
                url=f"https://example.com/post/{random.randint(100000, 999999)}",
                stock_codes=[stock_code],
                publish_time=datetime.now(),
            )
            messages.append(message)

        if messages:
            logger.debug(f"Fetched {len(messages)} social posts")

        return messages
