# === MODULE PURPOSE ===
# Fetches full announcement content from URLs.
# Supports HTML pages and PDF documents.
# Uses Aliyun Qwen-VL model to read PDF content directly.

# === DEPENDENCIES ===
# - httpx: For downloading announcement files
# - Aliyun DashScope API: For PDF content extraction

# === KEY CONCEPTS ===
# - PDF announcements: Use Aliyun multimodal model to read
# - HTML announcements: Parse with regex/basic extraction
# - Caching: Avoid re-downloading same announcements

import base64
import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)


@dataclass
class AnnouncementContent:
    """Full content of an announcement."""

    url: str
    title: str
    content: str  # Extracted text content
    content_type: str  # "pdf", "html", "unknown"
    stock_codes: list[str]
    fetch_time: datetime
    success: bool
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "title": self.title,
            "content": self.content,
            "content_type": self.content_type,
            "stock_codes": self.stock_codes,
            "fetch_time": self.fetch_time.isoformat(),
            "success": self.success,
            "error": self.error,
        }


@dataclass
class AliyunConfig:
    """Configuration for Aliyun DashScope API."""

    api_key: str
    base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    model: str = "qwen-vl-max-latest"
    max_tokens: int = 4000
    timeout: float = 120.0


class AnnouncementContentFetcher:
    """
    Fetches full announcement content from URLs.

    Supports:
        - PDF documents: Uses Aliyun Qwen-VL to extract text
        - HTML pages: Basic text extraction

    Usage:
        config = AliyunConfig(api_key="sk-xxx")
        fetcher = AnnouncementContentFetcher(config)
        await fetcher.start()

        content = await fetcher.fetch_content(
            url="http://...",
            title="公告标题"
        )
        if content.success:
            print(content.content)

        await fetcher.stop()
    """

    # Common announcement URL patterns
    PDF_PATTERNS = [
        r"\.pdf",
        r"/pdf/",
        r"filetype=pdf",
    ]

    def __init__(
        self,
        config: AliyunConfig,
        cache_dir: Path | None = None,
    ):
        """
        Initialize content fetcher.

        Args:
            config: Aliyun API configuration.
            cache_dir: Directory for caching downloaded files.
        """
        self._config = config
        self._cache_dir = cache_dir or Path("data/announcement_cache")
        self._client: httpx.AsyncClient | None = None
        self._is_running = False

    async def start(self) -> None:
        """Start the fetcher."""
        if self._is_running:
            return

        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._config.timeout),
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            },
        )
        self._is_running = True
        logger.info("AnnouncementContentFetcher started")

    async def stop(self) -> None:
        """Stop the fetcher."""
        if not self._is_running:
            return

        if self._client:
            await self._client.aclose()
            self._client = None

        self._is_running = False
        logger.info("AnnouncementContentFetcher stopped")

    async def fetch_content(
        self,
        url: str,
        title: str = "",
        stock_codes: list[str] | None = None,
    ) -> AnnouncementContent:
        """
        Fetch full content from an announcement URL.

        Args:
            url: Announcement URL.
            title: Announcement title (for context).
            stock_codes: Related stock codes.

        Returns:
            AnnouncementContent with extracted text.
        """
        if not self._is_running or not self._client:
            raise RuntimeError("Fetcher is not running")

        stock_codes = stock_codes or []

        # Determine content type
        is_pdf = self._is_pdf_url(url)

        try:
            if is_pdf:
                # Download PDF and use Aliyun to read it
                content = await self._fetch_pdf_content(url, title)
                return AnnouncementContent(
                    url=url,
                    title=title,
                    content=content,
                    content_type="pdf",
                    stock_codes=stock_codes,
                    fetch_time=datetime.now(),
                    success=True,
                )
            else:
                # Try to fetch as HTML
                content = await self._fetch_html_content(url)
                return AnnouncementContent(
                    url=url,
                    title=title,
                    content=content,
                    content_type="html",
                    stock_codes=stock_codes,
                    fetch_time=datetime.now(),
                    success=True,
                )

        except Exception as e:
            logger.error(f"Failed to fetch content from {url}: {e}")
            return AnnouncementContent(
                url=url,
                title=title,
                content="",
                content_type="unknown",
                stock_codes=stock_codes,
                fetch_time=datetime.now(),
                success=False,
                error=str(e),
            )

    async def _fetch_pdf_content(self, url: str, title: str) -> str:
        """
        Fetch PDF and extract content using Aliyun Qwen-VL.

        Args:
            url: PDF URL.
            title: Document title for context.

        Returns:
            Extracted text content.
        """
        # Check cache first
        cache_key = self._get_cache_key(url)
        cached = self._get_cached_content(cache_key)
        if cached:
            logger.debug(f"Using cached content for {url}")
            return cached

        # Download PDF
        logger.info(f"Downloading PDF: {url}")
        response = await self._client.get(url)
        response.raise_for_status()
        pdf_data = response.content

        # Convert to base64 for API
        pdf_base64 = base64.standard_b64encode(pdf_data).decode("utf-8")

        # Call Aliyun API to extract content
        content = await self._call_aliyun_pdf_api(pdf_base64, title)

        # Cache the result
        self._cache_content(cache_key, content)

        return content

    async def _call_aliyun_pdf_api(self, pdf_base64: str, title: str) -> str:
        """
        Call Aliyun Qwen-VL API to extract PDF content.

        Args:
            pdf_base64: Base64 encoded PDF data.
            title: Document title for context.

        Returns:
            Extracted text content.
        """
        # Build the request
        messages = [
            {
                "role": "user",
                "content": [
                    {
                        "type": "file",
                        "file": {
                            "file_data": f"data:application/pdf;base64,{pdf_base64}"
                        },
                    },
                    {
                        "type": "text",
                        "text": f"""请仔细阅读这份公告文件《{title}》，提取以下关键信息：

1. 公告的核心内容是什么？（如：分红方案、业绩预告、重大合同、股权变动等）
2. 涉及的具体数字（金额、比例、日期等）
3. 对股价可能的影响（利好/利空/中性）
4. 关键时间节点

请用简洁的中文总结，突出重点信息。如果是分红公告，请明确每股分红金额；如果是业绩公告，请明确营收和利润变化。""",
                    },
                ],
            }
        ]

        payload = {
            "model": self._config.model,
            "messages": messages,
            "max_tokens": self._config.max_tokens,
        }

        # Make API call
        api_url = f"{self._config.base_url}/chat/completions"
        response = await self._client.post(
            api_url,
            json=payload,
            headers={
                "Authorization": f"Bearer {self._config.api_key}",
                "Content-Type": "application/json",
            },
            timeout=self._config.timeout,
        )
        response.raise_for_status()

        data = response.json()
        content = data["choices"][0]["message"]["content"]

        logger.info(f"Successfully extracted PDF content ({len(content)} chars)")
        return content

    async def _fetch_html_content(self, url: str) -> str:
        """
        Fetch HTML page and extract text content.

        Args:
            url: HTML page URL.

        Returns:
            Extracted text content.
        """
        response = await self._client.get(url)
        response.raise_for_status()

        html = response.text

        # Basic HTML text extraction
        # Remove scripts and styles
        html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.I)
        html = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL | re.I)

        # Remove HTML tags
        text = re.sub(r"<[^>]+>", " ", html)

        # Clean up whitespace
        text = re.sub(r"\s+", " ", text).strip()

        # Decode HTML entities
        text = text.replace("&nbsp;", " ")
        text = text.replace("&lt;", "<")
        text = text.replace("&gt;", ">")
        text = text.replace("&amp;", "&")
        text = text.replace("&quot;", '"')

        return text[:10000]  # Limit length

    def _is_pdf_url(self, url: str) -> bool:
        """Check if URL points to a PDF document."""
        url_lower = url.lower()
        for pattern in self.PDF_PATTERNS:
            if re.search(pattern, url_lower):
                return True
        return False

    def _get_cache_key(self, url: str) -> str:
        """Generate cache key for URL."""
        return hashlib.md5(url.encode()).hexdigest()

    def _get_cached_content(self, cache_key: str) -> str | None:
        """Get cached content if exists."""
        cache_file = self._cache_dir / f"{cache_key}.txt"
        if cache_file.exists():
            return cache_file.read_text(encoding="utf-8")
        return None

    def _cache_content(self, cache_key: str, content: str) -> None:
        """Cache content to file."""
        cache_file = self._cache_dir / f"{cache_key}.txt"
        cache_file.write_text(content, encoding="utf-8")

    @property
    def is_running(self) -> bool:
        return self._is_running


def create_content_fetcher_from_config() -> AnnouncementContentFetcher:
    """
    Create content fetcher from configuration.

    Returns:
        Configured AnnouncementContentFetcher instance.
    """
    from src.common.config import load_secrets

    secrets = load_secrets()
    aliyun_config = secrets.get_dict("aliyun", {})

    api_key = aliyun_config.get("api_key")
    if not api_key:
        raise ValueError("Aliyun API key not found in secrets.yaml")

    config = AliyunConfig(
        api_key=api_key,
        base_url=aliyun_config.get(
            "base_url", "https://dashscope.aliyuncs.com/compatible-mode/v1"
        ),
        model=aliyun_config.get("model", "qwen-vl-max-latest"),
    )

    return AnnouncementContentFetcher(config)
