# === TEST MODULE ===
# Tests for announcement content fetcher with PDF support.

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.data.sources.announcement_content import (
    AliyunConfig,
    AnnouncementContent,
    AnnouncementContentFetcher,
)


class TestAnnouncementContent:
    """Tests for AnnouncementContent dataclass."""

    def test_to_dict(self):
        """Test conversion to dictionary."""
        content = AnnouncementContent(
            url="http://example.com/announcement.pdf",
            title="测试公告",
            content="公告内容",
            content_type="pdf",
            stock_codes=["000001"],
            fetch_time=datetime(2026, 1, 28, 10, 0, 0),
            success=True,
        )

        result = content.to_dict()

        assert result["url"] == "http://example.com/announcement.pdf"
        assert result["title"] == "测试公告"
        assert result["content"] == "公告内容"
        assert result["content_type"] == "pdf"
        assert result["stock_codes"] == ["000001"]
        assert result["success"] is True


class TestAliyunConfig:
    """Tests for AliyunConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = AliyunConfig(api_key="test-key")

        assert config.api_key == "test-key"
        assert config.base_url == "https://dashscope.aliyuncs.com/compatible-mode/v1"
        assert config.model == "qwen-vl-max-latest"
        assert config.max_tokens == 4000
        assert config.timeout == 120.0


class TestAnnouncementContentFetcher:
    """Tests for AnnouncementContentFetcher."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return AliyunConfig(api_key="test-api-key")

    @pytest.fixture
    def fetcher(self, config, tmp_path):
        """Create fetcher instance."""
        return AnnouncementContentFetcher(config, cache_dir=tmp_path)

    def test_is_pdf_url(self, fetcher):
        """Test PDF URL detection."""
        # Should be detected as PDF
        assert fetcher._is_pdf_url("http://example.com/doc.pdf") is True
        assert fetcher._is_pdf_url("http://example.com/pdf/doc") is True
        assert fetcher._is_pdf_url("http://example.com/doc?filetype=pdf") is True

        # Should NOT be detected as PDF
        assert fetcher._is_pdf_url("http://example.com/doc.html") is False
        assert fetcher._is_pdf_url("http://example.com/news/123") is False

    def test_cache_key_generation(self, fetcher):
        """Test cache key is consistent."""
        url = "http://example.com/test.pdf"
        key1 = fetcher._get_cache_key(url)
        key2 = fetcher._get_cache_key(url)

        assert key1 == key2
        assert len(key1) == 32  # MD5 hex length

    @pytest.mark.asyncio
    async def test_start_stop(self, fetcher):
        """Test fetcher lifecycle."""
        assert fetcher.is_running is False

        await fetcher.start()
        assert fetcher.is_running is True

        await fetcher.stop()
        assert fetcher.is_running is False

    @pytest.mark.asyncio
    async def test_fetch_without_start_raises(self, fetcher):
        """Test that fetching without start raises error."""
        with pytest.raises(RuntimeError, match="not running"):
            await fetcher.fetch_content("http://example.com/test.pdf")

    @pytest.mark.asyncio
    async def test_cache_content(self, fetcher, tmp_path):
        """Test content caching."""
        cache_key = "test_cache_key"
        content = "Cached content for testing"

        # Cache the content
        fetcher._cache_content(cache_key, content)

        # Verify it's cached
        cached = fetcher._get_cached_content(cache_key)
        assert cached == content

    @pytest.mark.asyncio
    async def test_fetch_html_content(self, fetcher):
        """Test HTML content fetching with mocked response."""
        await fetcher.start()

        html_content = """
        <html>
            <head><title>Test</title></head>
            <body>
                <script>console.log('test');</script>
                <p>测试公告内容</p>
            </body>
        </html>
        """

        # Mock the HTTP client
        mock_response = MagicMock()
        mock_response.text = html_content
        mock_response.raise_for_status = MagicMock()

        with patch.object(fetcher._client, "get", new=AsyncMock(return_value=mock_response)):
            result = await fetcher.fetch_content(
                url="http://example.com/notice.html",
                title="测试公告",
            )

        assert result.success is True
        assert result.content_type == "html"
        assert "测试公告内容" in result.content
        assert "console.log" not in result.content  # Script should be removed

        await fetcher.stop()

    @pytest.mark.asyncio
    async def test_fetch_pdf_content(self, fetcher):
        """Test PDF content fetching with mocked API response."""
        await fetcher.start()

        # Mock PDF download
        mock_pdf_response = MagicMock()
        mock_pdf_response.content = b"fake pdf content"
        mock_pdf_response.raise_for_status = MagicMock()

        # Mock API response
        mock_api_response = MagicMock()
        mock_api_response.json.return_value = {
            "choices": [{"message": {"content": "公告摘要：该公司拟每10股派发现金红利5元。"}}]
        }
        mock_api_response.raise_for_status = MagicMock()

        async def mock_get(url, **kwargs):
            return mock_pdf_response

        async def mock_post(url, **kwargs):
            return mock_api_response

        with patch.object(fetcher._client, "get", side_effect=mock_get):
            with patch.object(fetcher._client, "post", side_effect=mock_post):
                result = await fetcher.fetch_content(
                    url="http://example.com/notice.pdf",
                    title="分红公告",
                    stock_codes=["000001"],
                )

        assert result.success is True
        assert result.content_type == "pdf"
        assert "每10股派发现金红利5元" in result.content

        await fetcher.stop()

    @pytest.mark.asyncio
    async def test_error_handling(self, fetcher):
        """Test error handling for failed requests."""
        await fetcher.start()

        # Mock a failed request
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("Network error")

        with patch.object(fetcher._client, "get", new=AsyncMock(return_value=mock_response)):
            result = await fetcher.fetch_content(
                url="http://example.com/notice.pdf",
                title="测试公告",
            )

        assert result.success is False
        assert "Network error" in result.error

        await fetcher.stop()


# Live test - only run manually for integration testing
@pytest.mark.skip(reason="Live test - run manually")
class TestLiveContentFetcher:
    """Live integration tests - requires real API key."""

    @pytest.mark.asyncio
    async def test_live_pdf_fetch(self):
        """Test actual PDF fetching with real API."""
        from src.data.sources.announcement_content import create_content_fetcher_from_config

        fetcher = create_content_fetcher_from_config()
        await fetcher.start()

        try:
            # Use a real announcement URL for testing
            # Replace with actual URL when testing
            result = await fetcher.fetch_content(
                url="http://static.cninfo.com.cn/finalpage/2026-01-28/xxxxx.PDF",
                title="测试公告",
            )
            print(f"Success: {result.success}")
            print(f"Content type: {result.content_type}")
            print(f"Content: {result.content[:500]}...")
        finally:
            await fetcher.stop()
