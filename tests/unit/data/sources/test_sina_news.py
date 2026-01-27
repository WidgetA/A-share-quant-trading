# === MODULE PURPOSE ===
# Tests for SinaNewsSource.
# Verifies connectivity, data format, deduplication, and error handling.

import pytest
from datetime import datetime
from unittest.mock import patch
import pandas as pd

from src.data.sources.sina_news import SinaNewsSource


class TestSinaNewsSource:
    """Tests for SinaNewsSource."""

    def test_source_properties(self):
        """Test source type and name are correct."""
        source = SinaNewsSource()
        assert source.source_type == "news"
        assert source.source_name == "sina"

    def test_default_interval(self):
        """Test default polling interval."""
        source = SinaNewsSource()
        assert source.interval == 60.0

    def test_custom_interval(self):
        """Test custom polling interval."""
        source = SinaNewsSource(interval=90.0)
        assert source.interval == 90.0

    def test_generate_message_id(self):
        """Test message ID generation is deterministic."""
        source = SinaNewsSource()
        dt = datetime(2024, 1, 15, 10, 30, 0)

        id1 = source.generate_message_id("新浪财经新闻标题", dt)
        id2 = source.generate_message_id("新浪财经新闻标题", dt)
        id3 = source.generate_message_id("另一条新闻", dt)

        assert id1 == id2
        assert id1 != id3
        assert len(id1) == 32

    def test_deduplication(self):
        """Test that duplicate messages are filtered."""
        source = SinaNewsSource()
        msg_id = "sina_test_id"

        assert source.is_duplicate(msg_id) is False
        assert source.is_duplicate(msg_id) is True

    def test_extract_stock_codes(self):
        """Test stock code extraction from content."""
        source = SinaNewsSource()

        content = "五粮液000858和中信证券600030走势分化"
        codes = source._extract_stock_codes(content)

        assert "000858" in codes
        assert "600030" in codes

    def test_parse_datetime_full(self):
        """Test datetime parsing with full format."""
        source = SinaNewsSource()

        result = source._parse_datetime("2024-01-15 14:30:00")
        assert result.year == 2024
        assert result.hour == 14

    def test_parse_datetime_chinese_format(self):
        """Test datetime parsing with Chinese format."""
        source = SinaNewsSource()

        result = source._parse_datetime("2024年01月15日 14:30")
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_datetime_time_only(self):
        """Test datetime parsing with time only."""
        source = SinaNewsSource()
        now = datetime.now()

        result = source._parse_datetime("14:30")
        assert result.hour == 14
        assert result.minute == 30
        assert result.date() == now.date()

    def test_parse_datetime_invalid(self):
        """Test datetime parsing with invalid input."""
        source = SinaNewsSource()
        now = datetime.now()

        result = source._parse_datetime("invalid_format")
        assert abs((result - now).total_seconds()) < 5

    @pytest.mark.asyncio
    async def test_process_dataframe_empty(self):
        """Test processing empty DataFrame."""
        source = SinaNewsSource()
        df = pd.DataFrame()

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        assert messages == []

    @pytest.mark.asyncio
    async def test_process_dataframe_with_data(self):
        """Test processing DataFrame with valid data."""
        source = SinaNewsSource()
        df = pd.DataFrame([
            {
                "时间": "2024-01-15 10:30:00",
                "标题": "新浪财经新闻标题",
                "内容": "新浪财经测试新闻详细内容",
            },
            {
                "时间": "2024-01-15 11:00:00",
                "标题": "另一条新浪新闻",
                "内容": "新闻内容描述",
            },
        ])

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        assert len(messages) == 2
        assert messages[0].source_name == "sina"
        assert "新浪财经新闻标题" == messages[0].title

    @pytest.mark.asyncio
    async def test_process_dataframe_deduplication(self):
        """Test that duplicate rows are filtered."""
        source = SinaNewsSource()
        df = pd.DataFrame([
            {"时间": "2024-01-15 10:30:00", "标题": "重复标题", "内容": "重复内容"},
            {"时间": "2024-01-15 10:30:00", "标题": "重复标题", "内容": "重复内容"},
        ])

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        assert len(messages) == 1

    @pytest.mark.asyncio
    async def test_process_dataframe_title_fallback(self):
        """Test title extraction falls back to content."""
        source = SinaNewsSource()
        df = pd.DataFrame([
            {
                "时间": "2024-01-15 10:30:00",
                "内容": "这是一条没有标题的新闻内容，应该截取前50个字符作为标题",
            },
        ])

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        assert len(messages) == 1
        assert "这是一条没有标题的新闻内容" in messages[0].title

    @pytest.mark.live
    @pytest.mark.asyncio
    async def test_live_connectivity(self):
        """
        Test actual API connectivity.

        This test requires network access and may be slow.
        """
        source = SinaNewsSource()

        try:
            await source.start()

            messages = []
            async for msg in source.fetch_messages():
                messages.append(msg)
                if len(messages) >= 5:
                    break

            assert isinstance(messages, list)

            if messages:
                msg = messages[0]
                assert msg.source_type == "news"
                assert msg.source_name == "sina"
                assert msg.title
                assert msg.content

        finally:
            await source.stop()

    @pytest.mark.asyncio
    async def test_error_handling_api_failure(self):
        """Test graceful handling of API errors."""
        source = SinaNewsSource()

        with patch.object(source, "_fetch_news", side_effect=Exception("Connection Error")):
            messages = []
            async for msg in source.fetch_messages():
                messages.append(msg)

            assert messages == []

    def test_clear_seen_cache(self):
        """Test clearing the deduplication cache."""
        source = SinaNewsSource()

        # Add some IDs
        source.is_duplicate("id1")
        source.is_duplicate("id2")
        assert source.seen_count == 2

        # Clear cache
        source.clear_seen_cache()
        assert source.seen_count == 0

        # Same IDs should no longer be duplicates
        assert source.is_duplicate("id1") is False

    def test_mark_seen(self):
        """Test marking IDs as seen without duplicate check."""
        source = SinaNewsSource()

        # Mark as seen
        source.mark_seen("preloaded_id")
        assert source.seen_count == 1

        # Now it should be considered a duplicate
        assert source.is_duplicate("preloaded_id") is True
