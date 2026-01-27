# === MODULE PURPOSE ===
# Announcement source for fetching A-share stock announcements via akshare.
# Uses East Money (东方财富) data source through akshare library.

# === DEPENDENCIES ===
# - akshare: Free financial data API for China market
# - BaseMessageSource: Base class for all message sources

# === KEY CONCEPTS ===
# - Announcement categories: 全部, 重大事项, 财务报告, 融资公告, 风险提示, 资产重组, 信息变更, 持股变动
# - Data source: https://data.eastmoney.com/notices/hsa/5.html

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import AsyncIterator

import akshare as ak
import pandas as pd

from src.data.models.message import Message
from src.data.sources.base import BaseMessageSource

logger = logging.getLogger(__name__)

# Announcement category mapping
ANNOUNCEMENT_CATEGORIES = {
    "all": "全部",
    "major": "重大事项",
    "financial": "财务报告",
    "financing": "融资公告",
    "risk": "风险提示",
    "restructuring": "资产重组",
    "info_change": "信息变更",
    "shareholding": "持股变动",
}


class AkshareAnnouncementSource(BaseMessageSource):
    """
    Message source for A-share stock announcements via akshare.

    Fetches announcements from East Money (东方财富) data source.

    Data Flow:
        akshare API -> DataFrame -> Message objects

    Categories:
        - 全部 (all): All announcements
        - 重大事项 (major): Major events
        - 财务报告 (financial): Financial reports
        - 融资公告 (financing): Financing announcements
        - 风险提示 (risk): Risk warnings
        - 资产重组 (restructuring): Asset restructuring
        - 信息变更 (info_change): Information changes
        - 持股变动 (shareholding): Shareholding changes

    Note:
        akshare is a synchronous library, so we run it in a thread pool
        to avoid blocking the async event loop.
    """

    def __init__(self, interval: float = 300.0, category: str = "all"):
        """
        Initialize the akshare announcement source.

        Args:
            interval: Polling interval in seconds (default 5 minutes)
            category: Announcement category key (default "all")
        """
        super().__init__(interval=interval)
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="akshare_ann")
        self._category = ANNOUNCEMENT_CATEGORIES.get(category, "全部")

    @property
    def source_type(self) -> str:
        return "announcement"

    @property
    def source_name(self) -> str:
        return "akshare_eastmoney"

    async def start(self) -> None:
        """Initialize the source."""
        await super().start()
        logger.info(f"AkshareAnnouncementSource started with category: {self._category}")

    async def stop(self) -> None:
        """Cleanup resources."""
        self._executor.shutdown(wait=False)
        await super().stop()
        logger.info("AkshareAnnouncementSource stopped")

    async def fetch_messages(self) -> AsyncIterator[Message]:
        """
        Fetch latest announcements from akshare.

        Yields deduplicated Message objects for today's announcements.
        """
        loop = asyncio.get_event_loop()
        today = datetime.now().strftime("%Y%m%d")

        try:
            df = await loop.run_in_executor(
                self._executor,
                lambda: self._fetch_announcements(today),
            )
            async for msg in self._process_dataframe(df):
                yield msg
        except Exception as e:
            logger.error(f"Error fetching announcements: {e}")

    async def fetch_historical(self, days: int = 7) -> AsyncIterator[Message]:
        """
        Fetch historical announcements.

        Args:
            days: Number of days of historical data (default 7)

        Yields:
            Historical Message objects
        """
        loop = asyncio.get_event_loop()

        for i in range(days):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            logger.info(f"Fetching announcements for {date}")

            try:
                df = await loop.run_in_executor(
                    self._executor,
                    lambda d=date: self._fetch_announcements(d),
                )
                async for msg in self._process_dataframe(df):
                    yield msg
            except Exception as e:
                logger.error(f"Error fetching announcements for {date}: {e}")

    async def fetch_by_date_range(
        self, start_date: str, end_date: str
    ) -> AsyncIterator[Message]:
        """
        Fetch announcements for a specific date range.

        Args:
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Yields:
            Message objects for the date range
        """
        loop = asyncio.get_event_loop()
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
        current = start

        while current <= end:
            date_str = current.strftime("%Y%m%d")
            logger.info(f"Fetching announcements for {date_str}")

            try:
                df = await loop.run_in_executor(
                    self._executor,
                    lambda d=date_str: self._fetch_announcements(d),
                )
                async for msg in self._process_dataframe(df):
                    yield msg
            except Exception as e:
                logger.error(f"Error fetching announcements for {date_str}: {e}")

            current += timedelta(days=1)

    def _fetch_announcements(self, date: str) -> pd.DataFrame:
        """
        Fetch announcements for a specific date (sync, runs in thread pool).

        Args:
            date: Date in YYYYMMDD format

        Returns:
            DataFrame with announcement data
        """
        try:
            df = ak.stock_notice_report(symbol=self._category, date=date)
            logger.info(f"Fetched {len(df)} announcements for {date}")
            return df
        except Exception as e:
            logger.error(f"akshare announcement fetch error for {date}: {e}")
            return pd.DataFrame()

    async def _process_dataframe(self, df: pd.DataFrame) -> AsyncIterator[Message]:
        """
        Convert DataFrame rows to Message objects.

        Args:
            df: DataFrame with announcement data

        Yields:
            Deduplicated Message objects
        """
        if df.empty:
            return

        # Column names from akshare (may have encoding issues, use positional)
        # Columns: 代码, 名称, 公告标题, 公告类型, 公告日期, 网址
        columns = df.columns.tolist()

        for _, row in df.iterrows():
            try:
                # Extract data by position (more reliable than column names)
                stock_code = str(row.iloc[0]) if len(row) > 0 else ""
                stock_name = str(row.iloc[1]) if len(row) > 1 else ""
                title = str(row.iloc[2]) if len(row) > 2 else ""
                ann_type = str(row.iloc[3]) if len(row) > 3 else ""
                pub_date_str = str(row.iloc[4]) if len(row) > 4 else ""
                url = str(row.iloc[5]) if len(row) > 5 else ""

                # Parse publish date
                try:
                    if "-" in pub_date_str:
                        publish_time = datetime.strptime(pub_date_str, "%Y-%m-%d")
                    else:
                        publish_time = datetime.strptime(pub_date_str, "%Y%m%d")
                except ValueError:
                    publish_time = datetime.now()

                # Build full title with stock info
                full_title = f"[{stock_code}] {stock_name}: {title}"

                # Generate unique ID and check for duplicates
                msg_id = self.generate_message_id(full_title, publish_time)
                if self.is_duplicate(msg_id):
                    continue

                # Build content
                content = f"股票代码: {stock_code}\n"
                content += f"股票名称: {stock_name}\n"
                content += f"公告类型: {ann_type}\n"
                content += f"公告日期: {pub_date_str}\n"
                content += f"公告标题: {title}\n"
                content += f"链接: {url}"

                message = Message(
                    id=msg_id,
                    source_type=self.source_type,
                    source_name=self.source_name,
                    title=full_title,
                    content=content,
                    url=url,
                    stock_codes=[stock_code] if stock_code else [],
                    publish_time=publish_time,
                    raw_data=row.to_dict(),
                )

                yield message

            except Exception as e:
                logger.error(f"Error processing announcement row: {e}")
                continue


# Backward compatibility alias
BaostockAnnouncementSource = AkshareAnnouncementSource


# === CLI ENTRY POINT ===
# Allows running as: python -m src.data.sources.akshare_announcement
# Or directly: python src/data/sources/akshare_announcement.py

async def _cli_main(
    start_date: str | None = None,
    end_date: str | None = None,
    category: str = "all",
    db_path: str = "data/messages.db",
) -> None:
    """CLI entry point for fetching and saving announcements."""
    from pathlib import Path
    from src.data.database.message_db import MessageDatabase

    source = AkshareAnnouncementSource(category=category)
    db = MessageDatabase(Path(db_path))

    try:
        await db.connect()
        await source.start()

        # Load existing IDs to avoid re-processing
        existing_ids = await db.get_existing_ids(
            source_name=source.source_name,
            limit=source.MAX_SEEN_IDS,
        )
        for msg_id in existing_ids:
            source.mark_seen(msg_id)
        logger.info(f"Loaded {len(existing_ids)} existing IDs from database")

        # Determine date range
        if start_date and end_date:
            messages = source.fetch_by_date_range(start_date, end_date)
        elif start_date:
            messages = source.fetch_by_date_range(start_date, start_date)
        else:
            messages = source.fetch_messages()

        # Fetch and save
        total = 0
        saved = 0
        async for msg in messages:
            total += 1
            if await db.save(msg):
                saved += 1

        logger.info(f"完成: 获取 {total} 条, 新增 {saved} 条, 跳过 {total - saved} 条已存在")

    finally:
        await source.stop()
        await db.close()


def main() -> None:
    """Main entry point with argument parsing."""
    import argparse

    parser = argparse.ArgumentParser(
        description="获取A股公告数据并入库",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python -m src.data.sources.akshare_announcement                     # 获取今日公告
  python -m src.data.sources.akshare_announcement -d 20260124         # 获取指定日期
  python -m src.data.sources.akshare_announcement -s 20260124 -e 20260126  # 日期范围
  python -m src.data.sources.akshare_announcement -c major            # 只获取重大事项

公告类型:
  all          全部公告
  major        重大事项
  financial    财务报告
  financing    融资公告
  risk         风险提示
  restructuring 资产重组
  info_change  信息变更
  shareholding 持股变动
        """,
    )
    parser.add_argument("-d", "--date", help="指定日期 (YYYYMMDD)")
    parser.add_argument("-s", "--start", help="开始日期 (YYYYMMDD)")
    parser.add_argument("-e", "--end", help="结束日期 (YYYYMMDD)")
    parser.add_argument(
        "-c", "--category",
        default="all",
        choices=list(ANNOUNCEMENT_CATEGORIES.keys()),
        help="公告类型 (默认: all)",
    )
    parser.add_argument(
        "--db",
        default="data/messages.db",
        help="数据库路径 (默认: data/messages.db)",
    )
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Determine dates
    start_date = args.date or args.start
    end_date = args.date or args.end

    asyncio.run(_cli_main(start_date, end_date, args.category, args.db))


if __name__ == "__main__":
    main()
