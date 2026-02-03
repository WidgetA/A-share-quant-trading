# Tests for LLM Service
# Tests Silicon Flow Qwen API integration

import json
from unittest.mock import MagicMock, patch

import pytest

from src.common.llm_service import (
    AnalysisResult,
    LLMConfig,
    LLMService,
)


class TestAnalysisResult:
    """Tests for AnalysisResult dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        result = AnalysisResult()
        assert result.sentiment == "neutral"
        assert result.signal_type == "other"
        assert result.confidence == 0.0
        assert result.stock_codes == []
        assert result.sectors == []
        assert result.reason == ""
        assert result.success is True

    def test_is_positive_true(self):
        """Test is_positive returns True for positive high-confidence signals."""
        result = AnalysisResult(sentiment="positive", confidence=0.8)
        assert result.is_positive() is True

    def test_is_positive_false_low_confidence(self):
        """Test is_positive returns False for low confidence."""
        result = AnalysisResult(sentiment="positive", confidence=0.5)
        assert result.is_positive() is False

    def test_is_positive_false_negative_sentiment(self):
        """Test is_positive returns False for negative sentiment."""
        result = AnalysisResult(sentiment="negative", confidence=0.9)
        assert result.is_positive() is False

    def test_is_strong_signal_dividend(self):
        """Test is_strong_signal for dividend signal."""
        result = AnalysisResult(
            sentiment="positive",
            signal_type="dividend",
            confidence=0.85,
        )
        assert result.is_strong_signal() is True

    def test_is_strong_signal_earnings(self):
        """Test is_strong_signal for earnings signal."""
        result = AnalysisResult(
            sentiment="positive",
            signal_type="earnings",
            confidence=0.9,
        )
        assert result.is_strong_signal() is True

    def test_is_strong_signal_false_other_type(self):
        """Test is_strong_signal returns False for other signal types."""
        result = AnalysisResult(
            sentiment="positive",
            signal_type="other",
            confidence=0.9,
        )
        assert result.is_strong_signal() is False

    def test_to_dict(self):
        """Test serialization to dictionary."""
        result = AnalysisResult(
            sentiment="positive",
            signal_type="dividend",
            confidence=0.85,
            stock_codes=["000001", "600036"],
            sectors=["银行"],
            reason="高分红",
        )
        data = result.to_dict()

        assert data["sentiment"] == "positive"
        assert data["signal_type"] == "dividend"
        assert data["confidence"] == 0.85
        assert data["stock_codes"] == ["000001", "600036"]
        assert data["sectors"] == ["银行"]
        assert data["reason"] == "高分红"

    def test_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            "sentiment": "positive",
            "signal_type": "earnings",
            "confidence": 0.9,
            "stock_codes": ["600519"],
            "sectors": ["白酒"],
            "reason": "业绩超预期",
        }
        result = AnalysisResult.from_dict(data)

        assert result.sentiment == "positive"
        assert result.signal_type == "earnings"
        assert result.confidence == 0.9
        assert result.stock_codes == ["600519"]
        assert result.sectors == ["白酒"]


class TestLLMService:
    """Tests for LLMService class."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return LLMConfig(
            api_key="test-api-key",
            model="test-model",
            max_retries=1,
        )

    @pytest.fixture
    def service(self, config):
        """Create LLM service instance."""
        return LLMService(config)

    def test_init(self, service, config):
        """Test service initialization."""
        assert service._config == config
        assert service._client is None
        assert service.is_running is False

    @pytest.mark.asyncio
    async def test_start(self, service):
        """Test service start."""
        await service.start()
        assert service.is_running is True
        assert service._client is not None
        await service.stop()

    @pytest.mark.asyncio
    async def test_stop(self, service):
        """Test service stop."""
        await service.start()
        await service.stop()
        assert service.is_running is False
        assert service._client is None

    @pytest.mark.asyncio
    async def test_analyze_news_not_running(self, service):
        """Test analyze_news raises error when service not running."""
        with pytest.raises(RuntimeError, match="not running"):
            await service.analyze_news("标题", "内容")

    @pytest.mark.asyncio
    async def test_analyze_news_success(self, service):
        """Test successful news analysis."""
        mock_response = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "sentiment": "positive",
                                "signal_type": "dividend",
                                "confidence": 0.85,
                                "stock_codes": ["000001"],
                                "sectors": ["银行"],
                                "reason": "拟每10股派5元",
                            }
                        )
                    }
                }
            ]
        }

        await service.start()

        with patch.object(service._client, "post") as mock_post:
            mock_response_obj = MagicMock()
            mock_response_obj.json.return_value = mock_response
            mock_response_obj.raise_for_status = MagicMock()
            mock_post.return_value = mock_response_obj

            result = await service.analyze_news(
                "平安银行分红公告",
                "公司拟向全体股东每10股派发现金红利5元",
            )

            assert result.success is True
            assert result.sentiment == "positive"
            assert result.signal_type == "dividend"
            assert result.confidence == 0.85
            assert "000001" in result.stock_codes

        await service.stop()

    @pytest.mark.asyncio
    async def test_analyze_news_api_error(self, service):
        """Test handling of API errors."""
        import httpx

        await service.start()

        with patch.object(service._client, "post") as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_response.text = "Internal Server Error"
            mock_post.return_value = mock_response
            mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
                "Server Error",
                request=MagicMock(),
                response=mock_response,
            )

            result = await service.analyze_news("标题", "内容")

            assert result.success is False
            assert "500" in result.error

        await service.stop()

    @pytest.mark.asyncio
    async def test_batch_analyze(self, service):
        """Test batch analysis of multiple items."""
        mock_response = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "sentiment": "neutral",
                                "signal_type": "other",
                                "confidence": 0.5,
                                "stock_codes": [],
                                "sectors": [],
                                "reason": "一般新闻",
                            }
                        )
                    }
                }
            ]
        }

        await service.start()

        with patch.object(service._client, "post") as mock_post:
            mock_response_obj = MagicMock()
            mock_response_obj.json.return_value = mock_response
            mock_response_obj.raise_for_status = MagicMock()
            mock_post.return_value = mock_response_obj

            items = [
                ("标题1", "内容1"),
                ("标题2", "内容2"),
                ("标题3", "内容3"),
            ]
            results = await service.batch_analyze(items, max_concurrent=2)

            assert len(results) == 3
            assert all(r.success for r in results)

        await service.stop()

    def test_parse_analysis_response_valid_json(self, service):
        """Test parsing valid JSON response."""
        response = json.dumps(
            {
                "sentiment": "positive",
                "signal_type": "earnings",
                "confidence": 0.9,
                "stock_codes": ["600519"],
                "sectors": ["白酒"],
                "reason": "净利润增长20%",
            }
        )

        result = service._parse_analysis_response(response)

        assert result.success is True
        assert result.sentiment == "positive"
        assert result.signal_type == "earnings"
        assert result.confidence == 0.9

    def test_parse_analysis_response_with_extra_text(self, service):
        """Test parsing response with extra text around JSON."""
        json_content = (
            '{"sentiment": "positive", "signal_type": "dividend", '
            '"confidence": 0.8, "stock_codes": ["000001"], '
            '"sectors": ["银行"], "reason": "高分红"}'
        )
        response = f"""
        根据分析，这是一条利好新闻：
        {json_content}
        以上是我的分析结果。
        """

        result = service._parse_analysis_response(response)

        assert result.success is True
        assert result.sentiment == "positive"

    def test_parse_analysis_response_no_json(self, service):
        """Test parsing response without JSON."""
        response = "这条新闻是利好的，建议买入。"

        result = service._parse_analysis_response(response)

        assert result.success is False
        assert "No JSON" in result.error

    def test_parse_analysis_response_invalid_sentiment(self, service):
        """Test parsing response with invalid sentiment value."""
        response = json.dumps(
            {
                "sentiment": "very_positive",  # Invalid value
                "signal_type": "dividend",
                "confidence": 0.8,
                "stock_codes": [],
                "sectors": [],
                "reason": "test",
            }
        )

        result = service._parse_analysis_response(response)

        # Should normalize to neutral
        assert result.sentiment == "neutral"

    def test_parse_analysis_response_confidence_clamping(self, service):
        """Test confidence value is clamped to 0-1 range."""
        response = json.dumps(
            {
                "sentiment": "positive",
                "signal_type": "earnings",
                "confidence": 1.5,  # Out of range
                "stock_codes": [],
                "sectors": [],
                "reason": "test",
            }
        )

        result = service._parse_analysis_response(response)

        assert result.confidence == 1.0

    def test_parse_analysis_response_stock_code_validation(self, service):
        """Test stock codes are validated (6 digits only)."""
        response = json.dumps(
            {
                "sentiment": "positive",
                "signal_type": "earnings",
                "confidence": 0.8,
                "stock_codes": ["000001", "invalid", "12345", "600036"],
                "sectors": [],
                "reason": "test",
            }
        )

        result = service._parse_analysis_response(response)

        # Only valid 6-digit codes should be kept
        assert result.stock_codes == ["000001", "600036"]


@pytest.mark.live
class TestLLMServiceLive:
    """Live tests for LLM service (require API key)."""

    @pytest.fixture
    def live_service(self):
        """Create service with real API key from config."""
        try:
            from src.common.llm_service import create_llm_service_from_config

            return create_llm_service_from_config()
        except (ValueError, FileNotFoundError):
            pytest.skip("API key not configured")

    @pytest.mark.asyncio
    async def test_live_analyze_dividend_news(self, live_service):
        """Test live analysis of dividend news."""
        await live_service.start()

        result = await live_service.analyze_news(
            title="平安银行2025年度利润分配方案公告",
            content="公司董事会审议通过2025年度利润分配预案，拟向全体股东每10股派发现金红利5.00元（含税）。",
        )

        await live_service.stop()

        assert result.success is True
        # Should identify as positive dividend signal
        assert result.sentiment in ("positive", "neutral")
        if result.sentiment == "positive":
            assert result.signal_type == "dividend"
