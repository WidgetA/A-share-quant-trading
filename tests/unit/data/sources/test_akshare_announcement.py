# === MODULE PURPOSE ===
# Tests for AkshareAnnouncementSource (and backward compatible BaostockAnnouncementSource).
# Verifies connectivity, data format, deduplication, and error handling.

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, AsyncMock
import pandas as pd

from src.data.sources.akshare_announcement import (
    AkshareAnnouncementSource,
    BaostockAnnouncementSource,
    ANNOUNCEMENT_CATEGORIES,
)


class TestAkshareAnnouncementSource:
    """Tests for AkshareAnnouncementSource."""

    def test_source_properties(self):
        """Test source type and name are correct."""
        source = AkshareAnnouncementSource()
        assert source.source_type == "announcement"
        assert source.source_name == "akshare_eastmoney"

    def test_backward_compatibility_alias(self):
        """Test BaostockAnnouncementSource is an alias to AkshareAnnouncementSource."""
        assert BaostockAnnouncementSource is AkshareAnnouncementSource

    def test_default_interval(self):
        """Test default polling interval."""
        source = AkshareAnnouncementSource()
        assert source.interval == 300.0  # 5 minutes

    def test_custom_interval(self):
        """Test custom polling interval."""
        source = AkshareAnnouncementSource(interval=600.0)
        assert source.interval == 600.0

    def test_default_category(self):
        """Test default category is all."""
        source = AkshareAnnouncementSource()
        assert source._category == "全部"

    def test_custom_category(self):
        """Test custom category setting."""
        source = AkshareAnnouncementSource(category="major")
        assert source._category == "重大事项"

    def test_invalid_category_fallback(self):
        """Test invalid category falls back to all."""
        source = AkshareAnnouncementSource(category="invalid")
        assert source._category == "全部"

    def test_category_mapping(self):
        """Test all category mappings are correct."""
        assert ANNOUNCEMENT_CATEGORIES["all"] == "全部"
        assert ANNOUNCEMENT_CATEGORIES["major"] == "重大事项"
        assert ANNOUNCEMENT_CATEGORIES["financial"] == "财务报告"
        assert ANNOUNCEMENT_CATEGORIES["financing"] == "融资公告"
        assert ANNOUNCEMENT_CATEGORIES["risk"] == "风险提示"
        assert ANNOUNCEMENT_CATEGORIES["restructuring"] == "资产重组"
        assert ANNOUNCEMENT_CATEGORIES["info_change"] == "信息变更"
        assert ANNOUNCEMENT_CATEGORIES["shareholding"] == "持股变动"

    def test_generate_message_id(self):
        """Test message ID generation is deterministic."""
        source = AkshareAnnouncementSource()
        dt = datetime(2024, 1, 15, 10, 30, 0)

        id1 = source.generate_message_id("Test Title", dt)
        id2 = source.generate_message_id("Test Title", dt)
        id3 = source.generate_message_id("Different Title", dt)

        assert id1 == id2  # Same input = same ID
        assert id1 != id3  # Different input = different ID
        assert len(id1) == 32  # SHA256 truncated to 32 chars

    def test_deduplication(self):
        """Test that duplicate messages are filtered."""
        source = AkshareAnnouncementSource()
        dt = datetime(2024, 1, 15, 10, 30, 0)
        msg_id = source.generate_message_id("Test", dt)

        # First call should return False (not a duplicate)
        assert source.is_duplicate(msg_id) is False
        # Second call should return True (is a duplicate)
        assert source.is_duplicate(msg_id) is True

    def test_seen_cache_limit(self):
        """Test that seen cache respects MAX_SEEN_IDS limit."""
        source = AkshareAnnouncementSource()
        source.MAX_SEEN_IDS = 10  # Set low limit for testing

        # Add more IDs than the limit
        for i in range(15):
            source.is_duplicate(f"id_{i}")

        # Only 10 should be cached
        assert source.seen_count == 10
        # Oldest IDs should be evicted
        assert "id_0" not in source._seen_ids
        assert "id_14" in source._seen_ids

    @pytest.mark.asyncio
    async def test_process_dataframe_empty(self):
        """Test processing empty DataFrame returns nothing."""
        source = AkshareAnnouncementSource()
        df = pd.DataFrame()

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        assert messages == []

    @pytest.mark.asyncio
    async def test_process_dataframe_valid_data(self):
        """Test processing valid DataFrame returns messages."""
        source = AkshareAnnouncementSource()

        # Simulate akshare response (columns: 代码, 名称, 公告标题, 公告类型, 公告日期, 网址)
        df = pd.DataFrame([
            ["600519", "贵州茅台", "关于重大事项的公告", "重大事项", "2024-01-15", "https://example.com/1"],
            ["000001", "平安银行", "2023年年度报告", "财务报告", "2024-01-15", "https://example.com/2"],
        ])

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        assert len(messages) == 2
        assert messages[0].stock_codes == ["600519"]
        assert "贵州茅台" in messages[0].title
        assert messages[0].source_name == "akshare_eastmoney"
        assert messages[0].url == "https://example.com/1"

    @pytest.mark.asyncio
    async def test_process_dataframe_deduplication(self):
        """Test that duplicate rows are filtered."""
        source = AkshareAnnouncementSource()

        # Same announcement twice
        df = pd.DataFrame([
            ["600519", "贵州茅台", "公告标题", "重大事项", "2024-01-15", "https://example.com"],
            ["600519", "贵州茅台", "公告标题", "重大事项", "2024-01-15", "https://example.com"],
        ])

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        # Should only get one message due to deduplication
        assert len(messages) == 1

    @pytest.mark.asyncio
    async def test_fetch_announcements_error_handling(self):
        """Test that fetch handles API errors gracefully."""
        source = AkshareAnnouncementSource()

        with patch("akshare.stock_notice_report", side_effect=Exception("API Error")):
            df = source._fetch_announcements("20240115")
            assert df.empty

    @pytest.mark.live
    @pytest.mark.asyncio
    async def test_live_connectivity(self):
        """
        Test actual API connectivity.

        This test requires network access and may be slow.
        Mark with @pytest.mark.live to skip in CI.
        """
        source = AkshareAnnouncementSource()

        try:
            await source.start()

            messages = []
            async for msg in source.fetch_messages():
                messages.append(msg)
                if len(messages) >= 5:  # Limit for test
                    break

            # Should have some messages (unless holiday/weekend)
            assert isinstance(messages, list)
            if messages:
                assert messages[0].source_type == "announcement"
                assert messages[0].source_name == "akshare_eastmoney"

        finally:
            await source.stop()

    @pytest.mark.live
    @pytest.mark.asyncio
    async def test_live_fetch_by_date_range(self):
        """
        Test fetching announcements by date range.

        Mark with @pytest.mark.live to skip in CI.
        """
        source = AkshareAnnouncementSource()

        try:
            await source.start()

            messages = []
            async for msg in source.fetch_by_date_range("20240115", "20240115"):
                messages.append(msg)
                if len(messages) >= 10:
                    break

            assert isinstance(messages, list)

        finally:
            await source.stop()
