# === MODULE PURPOSE ===
# News analyzer that converts pre-analyzed messages to trading signals.
# Analysis is now done by external message collection project and stored in DB.
# This module only filters and converts the analysis results.

# === DEPENDENCIES ===
# - stock_filter: To filter out excluded exchanges
# - sector_mapper: To resolve sector-level signals to stocks

# === KEY CONCEPTS ===
# - NewsSignal: Trading signal derived from message analysis
# - Analysis results come from message_analysis table (already populated)
# - No LLM calls needed - just filtering and conversion

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from src.data.models.message import Message
from src.data.sources.sector_mapper import SectorMapper
from src.strategy.filters.stock_filter import StockFilter

logger = logging.getLogger(__name__)


@dataclass
class NewsAnalyzerConfig:
    """Configuration for news analyzer."""

    min_confidence: float = 0.7  # Minimum confidence to consider signal
    max_stocks_per_sector: int = 5  # Max stocks when buying sector
    # Which sentiments to treat as actionable
    actionable_sentiments: list[str] = field(
        default_factory=lambda: ["strong_bullish", "bullish"]
    )


@dataclass
class NewsSignal:
    """
    Trading signal derived from message analysis.

    Represents a potential trading opportunity identified from
    pre-analyzed news or announcements.

    Fields:
        message: Original message with analysis results
        recommended_action: buy_stock/buy_sector/ignore
        target_stocks: Filtered stock codes to potentially buy
        target_sectors: Related sectors
        slot_type: premarket/intraday
        created_at: When signal was created
    """

    message: Message
    recommended_action: str  # "buy_stock", "buy_sector", "ignore"
    target_stocks: list[str] = field(default_factory=list)
    target_sectors: list[str] = field(default_factory=list)
    slot_type: str = "intraday"  # "premarket" or "intraday"
    created_at: datetime = field(default_factory=datetime.now)

    @property
    def analysis(self):
        """Get analysis from message for backward compatibility."""
        return self.message.analysis

    def is_actionable(self) -> bool:
        """Check if signal should trigger user interaction."""
        return self.recommended_action in ("buy_stock", "buy_sector")

    def get_display_title(self) -> str:
        """Get a display-friendly title for this signal."""
        sentiment = "unknown"
        if self.message.analysis:
            sentiment = self.message.analysis.sentiment.value

        if self.target_stocks:
            stocks_str = ", ".join(self.target_stocks[:3])
            if len(self.target_stocks) > 3:
                stocks_str += f" +{len(self.target_stocks) - 3}只"
            return f"{stocks_str} - {sentiment}"
        elif self.target_sectors:
            return f"{self.target_sectors[0]}板块 - {sentiment}"
        else:
            return self.message.title[:30]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        analysis_dict = {}
        if self.message.analysis:
            analysis_dict = {
                "sentiment": self.message.analysis.sentiment.value,
                "confidence": self.message.analysis.confidence,
                "reasoning": self.message.analysis.reasoning,
            }

        return {
            "message_id": self.message.id,
            "message_title": self.message.title,
            "analysis": analysis_dict,
            "recommended_action": self.recommended_action,
            "target_stocks": self.target_stocks,
            "target_sectors": self.target_sectors,
            "slot_type": self.slot_type,
            "created_at": self.created_at.isoformat(),
        }


class NewsAnalyzer:
    """
    Converts pre-analyzed messages to trading signals.

    The actual LLM analysis is done by external message collection project.
    This class only:
    1. Filters messages by sentiment and confidence
    2. Filters stocks by exchange
    3. Resolves sector signals to individual stocks
    4. Creates NewsSignal objects

    Workflow:
        MessageReader (with analysis) -> NewsAnalyzer -> NewsSignal

    Usage:
        analyzer = NewsAnalyzer(
            stock_filter=filter,
            sector_mapper=mapper,
            config=NewsAnalyzerConfig(),
        )

        # Messages already have analysis from database
        signals = analyzer.filter_positive_messages(messages, slot_type="premarket")
        for signal in signals:
            if signal.is_actionable():
                print(f"利好信号: {signal.get_display_title()}")
    """

    def __init__(
        self,
        stock_filter: StockFilter,
        sector_mapper: SectorMapper,
        config: NewsAnalyzerConfig | None = None,
    ):
        """
        Initialize news analyzer.

        Args:
            stock_filter: Stock filter for exchange filtering.
            sector_mapper: Sector mapper for sector resolution.
            config: Analyzer configuration.
        """
        self._filter = stock_filter
        self._mapper = sector_mapper
        self._config = config or NewsAnalyzerConfig()

    def filter_positive_messages(
        self,
        messages: list[Message],
        slot_type: str = "intraday",
    ) -> list[NewsSignal]:
        """
        Filter messages with positive sentiment and create signals.

        Args:
            messages: Messages with analysis results from database.
            slot_type: "premarket" or "intraday".

        Returns:
            List of actionable NewsSignals.
        """
        if not messages:
            return []

        signals = []

        for msg in messages:
            signal = self._create_signal(msg, slot_type)
            if signal.is_actionable():
                signals.append(signal)

        logger.info(
            f"Filtered {len(messages)} messages -> {len(signals)} actionable signals"
        )
        return signals

    def _create_signal(
        self,
        message: Message,
        slot_type: str,
    ) -> NewsSignal:
        """
        Create a NewsSignal from a pre-analyzed message.

        Determines recommended action and resolves target stocks.
        """
        # Default: ignore
        recommended_action = "ignore"
        target_stocks: list[str] = []
        target_sectors: list[str] = []

        # Check if message has valid analysis
        if not message.analysis:
            return NewsSignal(
                message=message,
                recommended_action="ignore",
                slot_type=slot_type,
            )

        analysis = message.analysis

        # Check if this is an actionable signal
        is_positive = analysis.sentiment.value in self._config.actionable_sentiments
        meets_confidence = analysis.confidence >= self._config.min_confidence

        if is_positive and meets_confidence:
            # Get stock codes from message and analysis
            all_codes = set(message.stock_codes or [])
            all_codes.update(analysis.affected_stocks or [])

            # Filter stocks by exchange
            filtered_codes = self._filter.filter_stocks(list(all_codes))

            if filtered_codes:
                # Direct stock signal
                recommended_action = "buy_stock"
                target_stocks = filtered_codes

            # Try to resolve sectors from extracted entities
            # Note: matched_sector_ids are from external DB's sectors table,
            # which may not match our SectorMapper. Use extracted_entities instead.
            if analysis.extracted_entities and not target_stocks:
                # Try to find sectors matching extracted entities
                for entity in analysis.extracted_entities:
                    related = self._mapper.get_related_sectors([entity])
                    for sector_name in related:
                        if sector_name not in target_sectors:
                            target_sectors.append(sector_name)

                # Get stocks from matching sectors
                if target_sectors:
                    for sector in target_sectors[:3]:  # Limit to top 3 sectors
                        sector_stocks = self._mapper.get_sector_stocks(
                            sector, limit=self._config.max_stocks_per_sector * 2
                        )
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
            recommended_action=recommended_action,
            target_stocks=target_stocks,
            target_sectors=target_sectors,
            slot_type=slot_type,
        )

    @property
    def config(self) -> NewsAnalyzerConfig:
        """Get current configuration."""
        return self._config

    def update_config(self, config: NewsAnalyzerConfig) -> None:
        """Update analyzer configuration."""
        self._config = config
