# === MODULE PURPOSE ===
# Analyzers for strategy signal generation.
# Contains components that analyze data to identify trading opportunities.
#
# === PDF Content Support ===
# NewsAnalyzer can be configured to fetch full PDF content before analysis.
# Use create_news_analyzer_with_content_fetcher() for automatic setup.

from src.strategy.analyzers.news_analyzer import (
    NewsAnalyzer,
    NewsAnalyzerConfig,
    NewsSignal,
    create_news_analyzer_with_content_fetcher,
)

__all__ = [
    "NewsAnalyzer",
    "NewsAnalyzerConfig",
    "NewsSignal",
    "create_news_analyzer_with_content_fetcher",
]
