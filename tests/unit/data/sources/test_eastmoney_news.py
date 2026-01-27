# === MODULE PURPOSE ===
# Tests for EastmoneyNewsSource.
# Verifies connectivity, data format, deduplication, and error handling.

import pytest
from datetime import datetime
from unittest.mock import patch
import pandas as pd

from src.data.sources.eastmoney_news import EastmoneyNewsSource


class TestEastmoneyNewsSource:
    """Tests for EastmoneyNewsSource."""

    def test_source_properties(self):
        """Test source type and name are correct."""
        source = EastmoneyNewsSource()
        assert source.source_type == "news"
        assert source.source_name == "eastmoney"

    def test_default_interval(self):
        """Test default polling interval."""
        source = EastmoneyNewsSource()
        assert source.interval == 60.0

    def test_custom_interval(self):
        """Test custom polling interval."""
        source = EastmoneyNewsSource(interval=120.0)
        assert source.interval == 120.0

    def test_generate_message_id(self):
        """Test message ID generation is deterministic."""
        source = EastmoneyNewsSource()
        dt = datetime(2024, 1, 15, 10, 30, 0)

        id1 = source.generate_message_id("东方财富新闻", dt)
        id2 = source.generate_message_id("东方财富新闻", dt)
        id3 = source.generate_message_id("不同的新闻", dt)

        assert id1 == id2
        assert id1 != id3

    def test_deduplication(self):
        """Test that duplicate messages are filtered."""
        source = EastmoneyNewsSource()
        msg_id = "eastmoney_test_id"

        assert source.is_duplicate(msg_id) is False
        assert source.is_duplicate(msg_id) is True

    def test_extract_stock_codes(self):
        """Test stock code extraction from content."""
        source = EastmoneyNewsSource()

        content = "贵州茅台600519今日涨停，平安银行000001表现活跃"
        codes = source._extract_stock_codes(content)

        assert "600519" in codes
        assert "000001" in codes

    def test_parse_datetime_full(self):
        """Test datetime parsing with full format."""
        source = EastmoneyNewsSource()

        result = source._parse_datetime("2024-01-15 14:30:00")
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 14
        assert result.minute == 30

    def test_parse_datetime_time_only(self):
        """Test datetime parsing with time only."""
        source = EastmoneyNewsSource()
        now = datetime.now()

        result = source._parse_datetime("14:30")
        assert result.hour == 14
        assert result.minute == 30
        assert result.date() == now.date()

    def test_parse_datetime_invalid(self):
        """Test datetime parsing with invalid input."""
        source = EastmoneyNewsSource()
        now = datetime.now()

        result = source._parse_datetime("nan")
        assert abs((result - now).total_seconds()) < 5

    @pytest.mark.asyncio
    async def test_process_dataframe_empty(self):
        """Test processing empty DataFrame."""
        source = EastmoneyNewsSource()
        df = pd.DataFrame()

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        assert messages == []

    @pytest.mark.asyncio
    async def test_process_dataframe_with_data(self):
        """Test processing DataFrame with valid data."""
        source = EastmoneyNewsSource()
        df = pd.DataFrame([
            {
                "时间": "2024-01-15 10:30:00",
                "内容": "东方财富测试新闻内容600519",
            },
            {
                "时间": "2024-01-15 11:00:00",
                "内容": "另一条东方财富新闻",
            },
        ])

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        assert len(messages) == 2
        assert messages[0].source_name == "eastmoney"

    @pytest.mark.asyncio
    async def test_process_dataframe_deduplication(self):
        """Test that duplicate rows are filtered."""
        source = EastmoneyNewsSource()
        df = pd.DataFrame([
            {"时间": "2024-01-15 10:30:00", "内容": "重复的东方财富新闻"},
            {"时间": "2024-01-15 10:30:00", "内容": "重复的东方财富新闻"},
        ])

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        assert len(messages) == 1

    @pytest.mark.asyncio
    async def test_process_dataframe_alternative_columns(self):
        """Test processing with alternative column names."""
        source = EastmoneyNewsSource()
        df = pd.DataFrame([
            {
                "time": "2024-01-15 10:30:00",
                "content": "Alternative column format",
            },
        ])

        messages = []
        async for msg in source._process_dataframe(df):
            messages.append(msg)

        assert len(messages) == 1
        assert "Alternative" in messages[0].content

    @pytest.mark.live
    @pytest.mark.asyncio
    async def test_live_connectivity(self):
        """
        Test actual API connectivity.

        This test requires network access and may be slow.
        """
        source = EastmoneyNewsSource()

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
                assert msg.source_name == "eastmoney"
                assert msg.title
                assert msg.content

        finally:
            await source.stop()

    @pytest.mark.asyncio
    async def test_error_handling_api_failure(self):
        """Test graceful handling of API errors."""
        source = EastmoneyNewsSource()

        with patch.object(source, "_fetch_news", side_effect=Exception("Network Error")):
            messages = []
            async for msg in source.fetch_messages():
                messages.append(msg)

            assert messages == []
