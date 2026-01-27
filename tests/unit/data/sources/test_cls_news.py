# === MODULE PURPOSE ===
# Tests for CLSNewsSource.
# Verifies connectivity, data format, deduplication, and error handling.

import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock
import pandas as pd

from src.data.sources.cls_news import CLSNewsSource


class TestCLSNewsSource:
    """Tests for CLSNewsSource."""

    def test_source_properties(self):
        """Test source type and name are correct."""
        source = CLSNewsSource()
        assert source.source_type == "news"
        assert source.source_name == "cls"

    def test_default_interval(self):
        """Test default polling interval."""
        source = CLSNewsSource()
        assert source.interval == 30.0

    def test_custom_symbol(self):
        """Test custom news symbol."""
        source = CLSNewsSource(symbol="重点")
        assert source.symbol == "重点"

    def test_generate_message_id(self):
        """Test message ID generation is deterministic."""
        source = CLSNewsSource()
        dt = datetime(2024, 1, 15, 10, 30, 0)

        id1 = source.generate_message_id("财联社新闻标题", dt)
        id2 = source.generate_message_id("财联社新闻标题", dt)

        assert id1 == id2
        assert len(id1) == 32

    def test_deduplication(self):
        """Test that duplicate messages are filtered."""
        source = CLSNewsSource()
        msg_id = "test_id_12345"

        assert source.is_duplicate(msg_id) is False
        assert source.is_duplicate(msg_id) is True

    def test_extract_stock_codes(self):
        """Test stock code extraction from content."""
        source = CLSNewsSource()

        # Test $code$ format
        content1 = "关注$000001$平安银行和$600519$贵州茅台"
        codes1 = source._extract_stock_codes(content1)
        assert "000001" in codes1
        assert "600519" in codes1

        # Test plain code format
        content2 = "关注600519和000858的走势"
        codes2 = source._extract_stock_codes(content2)
        assert "600519" in codes2
        assert "000858" in codes2

        # Test no codes
        content3 = "今日市场整体表现平稳"
        codes3 = source._extract_stock_codes(content3)
        assert codes3 == []

    def test_parse_datetime_full(self):
        """Test datetime parsing with full datetime string."""
        source = CLSNewsSource()

        result = source._parse_datetime("10:30:00", "2024-01-15")
        assert result.hour == 10
        assert result.minute == 30
        assert result.year == 2024

    def test_parse_datetime_time_only(self):
        """Test datetime parsing with time only."""
        source = CLSNewsSource()
        now = datetime.now()

        result = source._parse_datetime("14:30:00", "")
        assert result.hour == 14
        assert result.minute == 30
        assert result.date() == now.date()

    def test_parse_datetime_invalid(self):
        """Test datetime parsing with invalid input returns now."""
        source = CLSNewsSource()
        now = datetime.now()

        result = source._parse_datetime("invalid", "")
        assert abs((result - now).total_seconds()) < 5

    @pytest.mark.asyncio
    async def test_process_dataframe_empty(self):
        """Test processing empty DataFrame."""
        source = CLSNewsSource()
        df = pd.DataFrame()

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        assert messages == []

    @pytest.mark.asyncio
    async def test_process_dataframe_with_data(self):
        """Test processing DataFrame with valid data."""
        source = CLSNewsSource()
        df = pd.DataFrame([
            {
                "发布时间": "10:30:00",
                "发布日期": "2024-01-15",
                "内容": "测试财联社新闻内容$600519$",
            },
            {
                "发布时间": "11:00:00",
                "发布日期": "2024-01-15",
                "内容": "另一条新闻内容",
            },
        ])

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        assert len(messages) == 2
        assert messages[0].source_name == "cls"
        assert "600519" in messages[0].stock_codes

    @pytest.mark.asyncio
    async def test_process_dataframe_deduplication(self):
        """Test that duplicate rows are filtered."""
        source = CLSNewsSource()
        # Same content twice
        df = pd.DataFrame([
            {"发布时间": "10:30:00", "发布日期": "2024-01-15", "内容": "重复内容"},
            {"发布时间": "10:30:00", "发布日期": "2024-01-15", "内容": "重复内容"},
        ])

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        # Should only yield one message due to deduplication
        assert len(messages) == 1

    @pytest.mark.live
    @pytest.mark.asyncio
    async def test_live_connectivity(self):
        """
        Test actual API connectivity.

        This test requires network access and may be slow.
        """
        source = CLSNewsSource()

        try:
            await source.start()

            messages = []
            async for msg in source.fetch_messages():
                messages.append(msg)
                if len(messages) >= 5:
                    break

            # Should get some messages from CLS
            # Note: May be empty during non-trading hours
            assert isinstance(messages, list)

            if messages:
                # Verify message structure
                msg = messages[0]
                assert msg.source_type == "news"
                assert msg.source_name == "cls"
                assert msg.title
                assert msg.content

        finally:
            await source.stop()

    @pytest.mark.asyncio
    async def test_error_handling_api_failure(self):
        """Test graceful handling of API errors."""
        source = CLSNewsSource()

        # Mock akshare to raise an error
        with patch.object(source, "_fetch_news", side_effect=Exception("API Error")):
            messages = []
            async for msg in source.fetch_messages():
                messages.append(msg)

            # Should return empty, not raise
            assert messages == []
