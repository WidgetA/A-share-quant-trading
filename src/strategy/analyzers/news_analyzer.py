# === MODULE PURPOSE ===
# News analyzer that uses LLM to identify trading signals from news.
# Coordinates between LLM service, stock filter, and sector mapper.
# Supports fetching full PDF content for deeper analysis.

# === DEPENDENCIES ===
# - llm_service: For sentiment/signal analysis
# - stock_filter: To filter out excluded exchanges
# - sector_mapper: To resolve sector-level signals to stocks
# - announcement_content: For fetching full PDF/HTML content (optional)

# === KEY CONCEPTS ===
# - NewsSignal: Trading signal derived from news analysis
# - Batch processing for efficiency
# - Caching to avoid re-analyzing same news
# - PDF content fetching: Use Aliyun multimodal model to read PDFs

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from src.common.llm_service import AnalysisResult, LLMService
from src.data.models.message import Message
from src.data.sources.announcement_content import AnnouncementContentFetcher
from src.data.sources.sector_mapper import SectorMapper
from src.strategy.filters.stock_filter import StockFilter

logger = logging.getLogger(__name__)


@dataclass
class NewsAnalyzerConfig:
    """Configuration for news analyzer."""

    min_confidence: float = 0.7  # Minimum confidence to consider signal
    max_stocks_per_sector: int = 5  # Max stocks when buying sector
    signal_types: list[str] = field(default_factory=lambda: ["dividend", "earnings", "restructure"])
    batch_size: int = 20  # Max items per batch analysis
    cache_ttl_minutes: int = 60  # Cache TTL
    fetch_full_content: bool = True  # Whether to fetch full PDF content
    content_fetch_timeout: float = 30.0  # Timeout for content fetching
    content_fetch_concurrency: int = 5  # Max concurrent content fetches


@dataclass
class NewsSignal:
    """
    Trading signal derived from news analysis.

    Represents a potential trading opportunity identified from
    news or announcement analysis.

    Fields:
        message: Original message that generated this signal
        analysis: LLM analysis result
        recommended_action: buy_stock/buy_sector/ignore
        target_stocks: Filtered stock codes to potentially buy
        target_sectors: Related sectors
        slot_type: premarket/intraday
        created_at: When signal was created
    """

    message: Message
    analysis: AnalysisResult
    recommended_action: str  # "buy_stock", "buy_sector", "ignore"
    target_stocks: list[str] = field(default_factory=list)
    target_sectors: list[str] = field(default_factory=list)
    slot_type: str = "intraday"  # "premarket" or "intraday"
    created_at: datetime = field(default_factory=datetime.now)

    def is_actionable(self) -> bool:
        """Check if signal should trigger user interaction."""
        return self.recommended_action in ("buy_stock", "buy_sector")

    def get_display_title(self) -> str:
        """Get a display-friendly title for this signal."""
        if self.target_stocks:
            stocks_str = ", ".join(self.target_stocks[:3])
            if len(self.target_stocks) > 3:
                stocks_str += f" +{len(self.target_stocks) - 3}只"
            return f"{stocks_str} - {self.analysis.signal_type}"
        elif self.target_sectors:
            return f"{self.target_sectors[0]}板块 - {self.analysis.signal_type}"
        else:
            return self.message.title[:30]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "message_id": self.message.id,
            "message_title": self.message.title,
            "analysis": self.analysis.to_dict(),
            "recommended_action": self.recommended_action,
            "target_stocks": self.target_stocks,
            "target_sectors": self.target_sectors,
            "slot_type": self.slot_type,
            "created_at": self.created_at.isoformat(),
        }


class NewsAnalyzer:
    """
    Analyzes news for trading signals using LLM.

    Workflow:
        1. Receive messages from MessageService
        2. Filter by stock exchange (exclude BSE/ChiNext)
        3. Fetch full content if URL points to PDF (optional)
        4. Call LLM for sentiment analysis
        5. Generate NewsSignal if positive
        6. Cache results to avoid re-analysis

    Usage:
        analyzer = NewsAnalyzer(
            llm_service=llm,
            stock_filter=filter,
            sector_mapper=mapper,
            content_fetcher=fetcher,  # Optional: for PDF reading
            config=NewsAnalyzerConfig(),
        )

        signals = await analyzer.analyze_messages(messages, slot_type="premarket")
        for signal in signals:
            if signal.is_actionable():
                print(f"利好信号: {signal.get_display_title()}")
    """

    def __init__(
        self,
        llm_service: LLMService,
        stock_filter: StockFilter,
        sector_mapper: SectorMapper,
        content_fetcher: AnnouncementContentFetcher | None = None,
        config: NewsAnalyzerConfig | None = None,
    ):
        """
        Initialize news analyzer.

        Args:
            llm_service: LLM service for analysis.
            stock_filter: Stock filter for exchange filtering.
            sector_mapper: Sector mapper for sector resolution.
            content_fetcher: Optional content fetcher for PDF reading.
            config: Analyzer configuration.
        """
        self._llm = llm_service
        self._filter = stock_filter
        self._mapper = sector_mapper
        self._content_fetcher = content_fetcher
        self._config = config or NewsAnalyzerConfig()

        # Cache: message_id -> NewsSignal
        self._cache: dict[str, NewsSignal] = {}
        self._cache_times: dict[str, datetime] = {}

        # Content cache: url -> fetched content
        self._content_cache: dict[str, str] = {}

    async def analyze_messages(
        self,
        messages: list[Message],
        slot_type: str = "intraday",
    ) -> list[NewsSignal]:
        """
        Analyze a batch of messages for trading signals.

        Args:
            messages: News/announcements to analyze.
            slot_type: "premarket" or "intraday".

        Returns:
            List of NewsSignals (only positive/actionable ones).
        """
        if not messages:
            return []

        signals = []

        # Check cache and prepare items for analysis
        to_analyze: list[tuple[Message, int]] = []  # (message, result_index)

        for msg in messages:
            # Check cache
            cached = self._get_cached(msg.id)
            if cached:
                cached.slot_type = slot_type
                signals.append(cached)
                continue

            to_analyze.append((msg, len(signals)))
            signals.append(None)  # Placeholder

        # Batch analyze uncached messages
        if to_analyze:
            # Fetch full content for messages with URLs (PDFs)
            messages_to_analyze = [msg for msg, _ in to_analyze]
            enhanced_contents = await self._fetch_contents_batch(messages_to_analyze)

            # Prepare items for batch analysis with enhanced content
            items = []
            for msg, enhanced in zip(messages_to_analyze, enhanced_contents):
                content = enhanced if enhanced else msg.content
                items.append((msg.title, content))

            # Analyze in batches
            all_results = []
            for i in range(0, len(items), self._config.batch_size):
                batch = items[i : i + self._config.batch_size]
                results = await self._llm.batch_analyze(batch)
                all_results.extend(results)

            # Process results
            for (msg, result_idx), analysis in zip(to_analyze, all_results):
                signal = self._create_signal(msg, analysis, slot_type)
                signals[result_idx] = signal

                # Cache the result
                self._cache_signal(msg.id, signal)

        # Filter out None placeholders and non-actionable signals
        return [s for s in signals if s is not None and s.is_actionable()]

    async def _fetch_contents_batch(self, messages: list[Message]) -> list[str | None]:
        """
        Fetch full content for a batch of messages concurrently.

        Args:
            messages: Messages to fetch content for.

        Returns:
            List of fetched contents (None if fetch failed or not applicable).
        """
        if not self._content_fetcher or not self._config.fetch_full_content:
            return [None] * len(messages)

        if not self._content_fetcher.is_running:
            logger.warning("Content fetcher not running, skipping content fetch")
            return [None] * len(messages)

        # Create fetch tasks with concurrency limit
        semaphore = asyncio.Semaphore(self._config.content_fetch_concurrency)

        async def fetch_with_limit(msg: Message) -> str | None:
            async with semaphore:
                return await self._fetch_single_content(msg)

        tasks = [fetch_with_limit(msg) for msg in messages]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to None
        return [r if isinstance(r, str) else None for r in results]

    async def _fetch_single_content(self, message: Message) -> str | None:
        """
        Fetch full content for a single message.

        Args:
            message: Message to fetch content for.

        Returns:
            Fetched content or None if not applicable/failed.
        """
        if not message.url:
            return None

        # Check content cache
        if message.url in self._content_cache:
            return self._content_cache[message.url]

        try:
            result = await asyncio.wait_for(
                self._content_fetcher.fetch_content(
                    url=message.url,
                    title=message.title,
                    stock_codes=message.stock_codes,
                ),
                timeout=self._config.content_fetch_timeout,
            )

            if result.success and result.content:
                # Cache the content
                self._content_cache[message.url] = result.content
                logger.info(
                    f"Fetched full content for {message.title[:30]}... "
                    f"({len(result.content)} chars, type={result.content_type})"
                )
                return result.content
            else:
                logger.warning(f"Failed to fetch content for {message.url}: {result.error}")
                return None

        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching content for {message.url}")
            return None
        except Exception as e:
            logger.error(f"Error fetching content for {message.url}: {e}")
            return None

    async def analyze_single(
        self,
        message: Message,
        slot_type: str = "intraday",
    ) -> NewsSignal:
        """
        Analyze a single message.

        Args:
            message: News/announcement to analyze.
            slot_type: "premarket" or "intraday".

        Returns:
            NewsSignal with analysis result.
        """
        # Check cache
        cached = self._get_cached(message.id)
        if cached:
            cached.slot_type = slot_type
            return cached

        # Fetch full content if available
        content = message.content
        fetched_content = await self._fetch_single_content(message)
        if fetched_content:
            content = fetched_content

        # Analyze with enhanced content
        analysis = await self._llm.analyze_news(message.title, content)
        signal = self._create_signal(message, analysis, slot_type)

        # Cache
        self._cache_signal(message.id, signal)

        return signal

    def _create_signal(
        self,
        message: Message,
        analysis: AnalysisResult,
        slot_type: str,
    ) -> NewsSignal:
        """
        Create a NewsSignal from analysis result.

        Determines recommended action and resolves target stocks.
        """
        # Default: ignore
        recommended_action = "ignore"
        target_stocks: list[str] = []
        target_sectors: list[str] = []

        # Check if this is an actionable signal
        if (
            analysis.success
            and analysis.sentiment == "positive"
            and analysis.confidence >= self._config.min_confidence
            and analysis.signal_type in self._config.signal_types
        ):
            # Get stock codes from message and analysis
            all_codes = set(message.stock_codes or [])
            all_codes.update(analysis.stock_codes)

            # Filter stocks by exchange
            filtered_codes = self._filter.filter_stocks(list(all_codes))

            if filtered_codes:
                # Direct stock signal
                recommended_action = "buy_stock"
                target_stocks = filtered_codes
                target_sectors = analysis.sectors

            elif analysis.sectors:
                # Sector-level signal - resolve to stocks
                target_sectors = analysis.sectors

                # Get stocks from sectors
                for sector in analysis.sectors:
                    sector_stocks = self._mapper.get_sector_stocks(
                        sector, limit=self._config.max_stocks_per_sector * 2
                    )
                    # Filter these stocks too
                    allowed = self._filter.filter_stocks(sector_stocks)
                    target_stocks.extend(allowed[: self._config.max_stocks_per_sector])

                if target_stocks:
                    recommended_action = "buy_sector"
                    # Remove duplicates while preserving order
                    seen = set()
                    unique_stocks = []
                    for code in target_stocks:
                        if code not in seen:
                            seen.add(code)
                            unique_stocks.append(code)
                    target_stocks = unique_stocks

        return NewsSignal(
            message=message,
            analysis=analysis,
            recommended_action=recommended_action,
            target_stocks=target_stocks,
            target_sectors=target_sectors,
            slot_type=slot_type,
        )

    def _get_cached(self, message_id: str) -> NewsSignal | None:
        """Get cached signal if still valid."""
        if message_id not in self._cache:
            return None

        cache_time = self._cache_times.get(message_id)
        if not cache_time:
            return None

        # Check TTL
        age = datetime.now() - cache_time
        if age.total_seconds() > self._config.cache_ttl_minutes * 60:
            # Expired
            del self._cache[message_id]
            del self._cache_times[message_id]
            return None

        return self._cache[message_id]

    def _cache_signal(self, message_id: str, signal: NewsSignal) -> None:
        """Cache a signal."""
        self._cache[message_id] = signal
        self._cache_times[message_id] = datetime.now()

        # Clean old entries if cache is too large
        if len(self._cache) > 1000:
            self._clean_cache()

    def _clean_cache(self) -> None:
        """Remove expired entries from cache."""
        now = datetime.now()
        ttl_seconds = self._config.cache_ttl_minutes * 60

        expired = [
            msg_id
            for msg_id, cache_time in self._cache_times.items()
            if (now - cache_time).total_seconds() > ttl_seconds
        ]

        for msg_id in expired:
            self._cache.pop(msg_id, None)
            self._cache_times.pop(msg_id, None)

    def clear_cache(self) -> None:
        """Clear all cached signals and content."""
        self._cache.clear()
        self._cache_times.clear()
        self._content_cache.clear()

    def clear_content_cache(self) -> None:
        """Clear only the content cache."""
        self._content_cache.clear()

    @property
    def config(self) -> NewsAnalyzerConfig:
        """Get current configuration."""
        return self._config

    @property
    def content_fetcher(self) -> AnnouncementContentFetcher | None:
        """Get the content fetcher."""
        return self._content_fetcher

    def set_content_fetcher(self, fetcher: AnnouncementContentFetcher | None) -> None:
        """Set or update the content fetcher."""
        self._content_fetcher = fetcher

    def update_config(self, config: NewsAnalyzerConfig) -> None:
        """Update analyzer configuration."""
        self._config = config


def create_news_analyzer_with_content_fetcher(
    llm_service: LLMService,
    stock_filter: StockFilter,
    sector_mapper: SectorMapper,
    config: NewsAnalyzerConfig | None = None,
) -> tuple[NewsAnalyzer, AnnouncementContentFetcher]:
    """
    Create NewsAnalyzer with content fetcher configured from secrets.

    Returns:
        Tuple of (NewsAnalyzer, AnnouncementContentFetcher).
        Caller is responsible for calling content_fetcher.start()/stop().

    Usage:
        analyzer, fetcher = create_news_analyzer_with_content_fetcher(...)
        await fetcher.start()
        try:
            signals = await analyzer.analyze_messages(messages)
        finally:
            await fetcher.stop()
    """
    from src.data.sources.announcement_content import create_content_fetcher_from_config

    content_fetcher = create_content_fetcher_from_config()
    analyzer = NewsAnalyzer(
        llm_service=llm_service,
        stock_filter=stock_filter,
        sector_mapper=sector_mapper,
        content_fetcher=content_fetcher,
        config=config,
    )
    return analyzer, content_fetcher
