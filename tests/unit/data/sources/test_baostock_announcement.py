# === MODULE PURPOSE ===
# Tests for BaostockAnnouncementSource.
# Verifies connectivity, data format, deduplication, and error handling.

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
import pandas as pd

from src.data.sources.baostock_announcement import BaostockAnnouncementSource


class TestBaostockAnnouncementSource:
    """Tests for BaostockAnnouncementSource."""

    def test_source_properties(self):
        """Test source type and name are correct."""
        source = BaostockAnnouncementSource()
        assert source.source_type == "announcement"
        assert source.source_name == "baostock"

    def test_default_interval(self):
        """Test default polling interval."""
        source = BaostockAnnouncementSource()
        assert source.interval == 300.0  # 5 minutes

    def test_custom_interval(self):
        """Test custom polling interval."""
        source = BaostockAnnouncementSource(interval=600.0)
        assert source.interval == 600.0

    def test_generate_message_id(self):
        """Test message ID generation is deterministic."""
        source = BaostockAnnouncementSource()
        dt = datetime(2024, 1, 15, 10, 30, 0)

        id1 = source.generate_message_id("Test Title", dt)
        id2 = source.generate_message_id("Test Title", dt)
        id3 = source.generate_message_id("Different Title", dt)

        assert id1 == id2  # Same input = same ID
        assert id1 != id3  # Different input = different ID
        assert len(id1) == 32  # SHA256 truncated to 32 chars

    def test_deduplication(self):
        """Test that duplicate messages are filtered."""
        source = BaostockAnnouncementSource()
        dt = datetime(2024, 1, 15, 10, 30, 0)
        msg_id = source.generate_message_id("Test", dt)

        # First call should return False (not a duplicate)
        assert source.is_duplicate(msg_id) is False
        # Second call should return True (is a duplicate)
        assert source.is_duplicate(msg_id) is True

    def test_seen_cache_limit(self):
        """Test that seen cache respects MAX_SEEN_IDS limit."""
        source = BaostockAnnouncementSource()
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
    async def test_format_performance_express(self):
        """Test performance express report formatting."""
        source = BaostockAnnouncementSource()
        row = pd.Series({
            "code": "sh.600519",
            "performanceExpStatDate": "2024-03-31",
            "publishDate": "2024-04-15",
            "performanceExpEPS": "1.23",
            "performanceExpROE": "15.5%",
        })

        content = source._format_performance_express(row)

        assert "600519" in content
        assert "2024-03-31" in content
        assert "1.23" in content

    @pytest.mark.asyncio
    async def test_format_forecast(self):
        """Test forecast report formatting."""
        source = BaostockAnnouncementSource()
        row = pd.Series({
            "code": "sh.000001",
            "reportDate": "2024-06-30",
            "type": "预增",
            "changeReason": "业务增长",
        })

        content = source._format_forecast(row)

        assert "000001" in content
        assert "预增" in content
        assert "业务增长" in content

    @pytest.mark.live
    @pytest.mark.asyncio
    async def test_live_connectivity(self):
        """
        Test actual API connectivity.

        This test requires network access and may be slow.
        Mark with @pytest.mark.live to skip in CI.
        """
        source = BaostockAnnouncementSource()

        try:
            await source.start()
            assert source._logged_in is True

            messages = []
            async for msg in source.fetch_messages():
                messages.append(msg)
                if len(messages) >= 5:  # Limit for test
                    break

            # May or may not have messages, but should not error
            assert isinstance(messages, list)

        finally:
            await source.stop()

    @pytest.mark.asyncio
    async def test_error_handling_not_logged_in(self):
        """Test that fetch handles not-logged-in state gracefully."""
        source = BaostockAnnouncementSource()
        # Don't call start(), so _logged_in is False

        messages = []
        async for msg in source.fetch_messages():
            messages.append(msg)

        # Should return empty, not raise error
        assert messages == []
