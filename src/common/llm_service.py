# === MODULE PURPOSE ===
# LLM service for news analysis using Silicon Flow's Qwen model.
# Provides async interface to Silicon Flow API for financial news analysis.

# === DEPENDENCIES ===
# - httpx: Async HTTP client for API calls
# - config: For API credentials from secrets.yaml

# === KEY CONCEPTS ===
# - Silicon Flow provides OpenAI-compatible chat completions API
# - Uses Qwen model for Chinese financial text analysis
# - Structured JSON output for consistent parsing
# - Rate limiting and retry logic for reliability

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

logger = logging.getLogger(__name__)


@dataclass
class LLMConfig:
    """Configuration for LLM service."""

    api_key: str
    base_url: str = "https://api.siliconflow.cn/v1/chat/completions"
    model: str = "Qwen/Qwen2.5-72B-Instruct"
    max_tokens: int = 2000
    temperature: float = 0.1  # Low temperature for consistent analysis
    timeout: float = 60.0
    max_retries: int = 3
    retry_delay: float = 1.0


@dataclass
class AnalysisResult:
    """
    Result from LLM analysis of news/announcements.

    Fields:
        sentiment: Overall sentiment (positive/negative/neutral)
        signal_type: Type of signal identified (dividend/earnings/restructure/other)
        confidence: Confidence score (0.0-1.0)
        stock_codes: Related stock codes extracted from analysis
        sectors: Related industry sectors
        reason: Brief explanation of the analysis
        raw_response: Original LLM response text
        success: Whether analysis completed successfully
        error: Error message if analysis failed
    """

    sentiment: str = "neutral"
    signal_type: str = "other"
    confidence: float = 0.0
    stock_codes: list[str] = field(default_factory=list)
    sectors: list[str] = field(default_factory=list)
    reason: str = ""
    raw_response: str = ""
    success: bool = True
    error: str = ""

    def is_positive(self) -> bool:
        """Check if this is a positive signal worth acting on."""
        return self.sentiment == "positive" and self.confidence >= 0.7

    def is_strong_signal(self) -> bool:
        """Check if this is a strong trading signal."""
        return (
            self.sentiment == "positive"
            and self.signal_type in ("dividend", "earnings", "restructure")
            and self.confidence >= 0.8
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "sentiment": self.sentiment,
            "signal_type": self.signal_type,
            "confidence": self.confidence,
            "stock_codes": self.stock_codes,
            "sectors": self.sectors,
            "reason": self.reason,
            "raw_response": self.raw_response,
            "success": self.success,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AnalysisResult":
        """Create from dictionary."""
        return cls(
            sentiment=data.get("sentiment", "neutral"),
            signal_type=data.get("signal_type", "other"),
            confidence=data.get("confidence", 0.0),
            stock_codes=data.get("stock_codes", []),
            sectors=data.get("sectors", []),
            reason=data.get("reason", ""),
            raw_response=data.get("raw_response", ""),
            success=data.get("success", True),
            error=data.get("error", ""),
        )


# System prompt for news analysis
ANALYSIS_SYSTEM_PROMPT = """你是一个专业的A股财经新闻分析师。
请分析以下新闻/公告，判断对相关股票价格的影响。

重点识别以下利好信号：
1. 分红信息（dividend）- 现金分红、高送转、股份回购
2. 好业绩（earnings）- 净利润增长、营收增长、业绩预增、扭亏为盈、超预期
3. 重组并购（restructure）- 资产注入、借壳上市、战略合作、重大合同

利空信号：
1. 业绩下滑、亏损、业绩预减
2. 股东减持、高管离职
3. 监管处罚、诉讼风险
4. 重大资产减值

请严格以JSON格式返回分析结果，不要包含其他内容：
{
  "sentiment": "positive/negative/neutral",
  "signal_type": "dividend/earnings/restructure/other",
  "confidence": 0.0-1.0,
  "stock_codes": ["000001"],
  "sectors": ["银行", "金融"],
  "reason": "简要说明判断理由（不超过50字）"
}

注意：
- confidence 表示你对这个判断的把握程度
- stock_codes 是新闻中涉及的股票代码（6位数字），如果没有明确股票则留空
- sectors 是涉及的行业板块
- 只有明确的利好/利空才给出 positive/negative，不确定就返回 neutral"""


class LLMService:
    """
    Async LLM service for financial news analysis.

    Uses Silicon Flow's Qwen model to analyze news and announcements
    for trading signals.

    Usage:
        config = LLMConfig(api_key="sk-xxx")
        llm = LLMService(config)
        await llm.start()

        result = await llm.analyze_news("标题", "内容")
        if result.is_positive():
            print(f"发现利好: {result.reason}")

        await llm.stop()

    Thread Safety:
        This service is async-safe and can be called from multiple
        coroutines concurrently. Uses httpx.AsyncClient for connection
        pooling.
    """

    def __init__(self, config: LLMConfig):
        """
        Initialize LLM service.

        Args:
            config: LLM configuration with API key and model settings.
        """
        self._config = config
        self._client: httpx.AsyncClient | None = None
        self._is_running = False

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._is_running

    async def start(self) -> None:
        """
        Start the LLM service.

        Initializes the HTTP client for API calls.
        """
        if self._is_running:
            return

        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._config.timeout),
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )
        self._is_running = True
        logger.info(f"LLM service started with model: {self._config.model}")

    async def stop(self) -> None:
        """
        Stop the LLM service.

        Closes the HTTP client and releases resources.
        """
        if not self._is_running:
            return

        if self._client:
            await self._client.aclose()
            self._client = None

        self._is_running = False
        logger.info("LLM service stopped")

    async def analyze_news(
        self,
        title: str,
        content: str,
        system_prompt: str | None = None,
    ) -> AnalysisResult:
        """
        Analyze a single news item for trading signals.

        Args:
            title: News title/headline.
            content: Full news content.
            system_prompt: Custom system prompt (uses default if None).

        Returns:
            AnalysisResult with sentiment and signal information.

        Raises:
            RuntimeError: If service is not running.
        """
        if not self._is_running or not self._client:
            raise RuntimeError("LLM service is not running. Call start() first.")

        # Prepare the user message
        user_message = f"标题：{title}\n\n内容：{content}"

        # Use default or custom system prompt
        sys_prompt = system_prompt or ANALYSIS_SYSTEM_PROMPT

        # Build request payload
        payload = {
            "model": self._config.model,
            "messages": [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_message},
            ],
            "temperature": self._config.temperature,
            "max_tokens": self._config.max_tokens,
        }

        # Make API call with retries
        last_error = None
        for attempt in range(self._config.max_retries):
            try:
                response = await self._client.post(
                    self._config.base_url,
                    json=payload,
                    headers={
                        "Authorization": f"Bearer {self._config.api_key}",
                        "Content-Type": "application/json",
                    },
                )
                response.raise_for_status()

                # Parse response
                data = response.json()
                raw_content = data["choices"][0]["message"]["content"]

                # Parse the JSON from response
                result = self._parse_analysis_response(raw_content)
                result.raw_response = raw_content
                return result

            except httpx.HTTPStatusError as e:
                last_error = f"HTTP error {e.response.status_code}: {e.response.text}"
                retries = self._config.max_retries
                logger.warning(f"LLM API error (attempt {attempt + 1}/{retries}): {last_error}")
            except httpx.RequestError as e:
                last_error = f"Request error: {str(e)}"
                retries = self._config.max_retries
                logger.warning(f"LLM request error ({attempt + 1}/{retries}): {last_error}")
            except (KeyError, json.JSONDecodeError) as e:
                last_error = f"Response parse error: {str(e)}"
                retries = self._config.max_retries
                logger.warning(f"LLM parse error ({attempt + 1}/{retries}): {last_error}")

            # Wait before retry
            if attempt < self._config.max_retries - 1:
                await asyncio.sleep(self._config.retry_delay * (attempt + 1))

        # All retries failed
        return AnalysisResult(
            success=False,
            error=last_error or "Unknown error",
        )

    async def batch_analyze(
        self,
        items: list[tuple[str, str]],
        max_concurrent: int = 5,
    ) -> list[AnalysisResult]:
        """
        Analyze multiple news items concurrently.

        Args:
            items: List of (title, content) tuples.
            max_concurrent: Maximum concurrent API calls.

        Returns:
            List of AnalysisResult in the same order as input.
        """
        if not items:
            return []

        # Use semaphore to limit concurrency
        semaphore = asyncio.Semaphore(max_concurrent)

        async def analyze_with_limit(title: str, content: str) -> AnalysisResult:
            async with semaphore:
                return await self.analyze_news(title, content)

        # Run all analyses concurrently
        tasks = [analyze_with_limit(title, content) for title, content in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to failed results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append(
                    AnalysisResult(
                        success=False,
                        error=str(result),
                    )
                )
            else:
                processed_results.append(result)

        return processed_results

    def _parse_analysis_response(self, response_text: str) -> AnalysisResult:
        """
        Parse LLM response text into AnalysisResult.

        Handles various response formats and extracts JSON.
        """
        # Try to extract JSON from response
        json_match = re.search(r"\{[^{}]*\}", response_text, re.DOTALL)
        if not json_match:
            logger.warning(f"No JSON found in LLM response: {response_text[:200]}")
            return AnalysisResult(
                success=False,
                error="No JSON found in response",
                raw_response=response_text,
            )

        try:
            data = json.loads(json_match.group())

            # Validate and normalize sentiment
            sentiment = data.get("sentiment", "neutral").lower()
            if sentiment not in ("positive", "negative", "neutral"):
                sentiment = "neutral"

            # Validate and normalize signal_type
            signal_type = data.get("signal_type", "other").lower()
            valid_types = ("dividend", "earnings", "restructure", "other")
            if signal_type not in valid_types:
                signal_type = "other"

            # Validate confidence
            confidence = float(data.get("confidence", 0.0))
            confidence = max(0.0, min(1.0, confidence))

            # Extract stock codes (ensure they are strings)
            stock_codes = [str(code) for code in data.get("stock_codes", [])]
            # Filter to valid 6-digit codes
            stock_codes = [c for c in stock_codes if re.match(r"^\d{6}$", c)]

            # Extract sectors
            sectors = data.get("sectors", [])
            if isinstance(sectors, str):
                sectors = [sectors]

            return AnalysisResult(
                sentiment=sentiment,
                signal_type=signal_type,
                confidence=confidence,
                stock_codes=stock_codes,
                sectors=sectors if isinstance(sectors, list) else [],
                reason=str(data.get("reason", "")),
                success=True,
            )

        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to parse LLM response JSON: {e}")
            return AnalysisResult(
                success=False,
                error=f"JSON parse error: {str(e)}",
                raw_response=response_text,
            )


def create_llm_service_from_config() -> LLMService:
    """
    Create LLM service from configuration file.

    Loads API credentials from config/secrets.yaml.

    Returns:
        Configured LLMService instance.

    Raises:
        ValueError: If API key is not found in config.
        FileNotFoundError: If secrets.yaml doesn't exist.
    """
    from src.common.config import load_secrets

    secrets = load_secrets()

    siliconflow_config = secrets.get_dict("siliconflow", {})
    api_key = siliconflow_config.get("api_key")

    if not api_key:
        raise ValueError("Silicon Flow API key not found in secrets.yaml")

    config = LLMConfig(
        api_key=api_key,
        base_url=siliconflow_config.get(
            "base_url", "https://api.siliconflow.cn/v1/chat/completions"
        ),
        model=siliconflow_config.get("model", "Qwen/Qwen2.5-72B-Instruct"),
        max_tokens=siliconflow_config.get("max_tokens", 2000),
        temperature=siliconflow_config.get("temperature", 0.1),
    )

    return LLMService(config)
