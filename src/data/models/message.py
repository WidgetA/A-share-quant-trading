# === MODULE PURPOSE ===
# Defines the Message data model for storing news, announcements, and social media content.
# This is the core data structure used across the message module.

# === KEY CONCEPTS ===
# - Message: A piece of information from various sources (announcements, news, social media)
# - source_type: Category of the source (announcement/news/social)
# - source_name: Specific source identifier (e.g., "eastmoney", "sina", "xueqiu")
# - Analysis results are populated from external message_analysis table

# === SENTIMENT VALUES ===
# - strong_bullish: 强利好 (政策重大支持、业绩大幅增长)
# - bullish: 利好 (中标项目、战略合作)
# - neutral: 中性 (日常公告、人事变动)
# - bearish: 利空 (业绩下滑、监管处罚)
# - strong_bearish: 强利空 (重大亏损、退市风险)

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4


class Sentiment(Enum):
    """Message sentiment from AI analysis."""

    STRONG_BULLISH = "strong_bullish"  # 强利好
    BULLISH = "bullish"  # 利好
    NEUTRAL = "neutral"  # 中性
    BEARISH = "bearish"  # 利空
    STRONG_BEARISH = "strong_bearish"  # 强利空

    def is_positive(self) -> bool:
        """Check if sentiment is positive (bullish or strong_bullish)."""
        return self in (Sentiment.STRONG_BULLISH, Sentiment.BULLISH)

    def is_negative(self) -> bool:
        """Check if sentiment is negative (bearish or strong_bearish)."""
        return self in (Sentiment.STRONG_BEARISH, Sentiment.BEARISH)


@dataclass
class MessageAnalysis:
    """
    AI analysis result for a message.

    Populated from external message_analysis table in PostgreSQL.
    """

    sentiment: Sentiment
    confidence: float  # 0.0 - 1.0
    reasoning: str  # LLM analysis reason
    extracted_entities: list[str] = field(default_factory=list)  # Extracted entities
    matched_sector_ids: list[int] = field(default_factory=list)  # Matched sector IDs
    affected_stocks: list[str] = field(default_factory=list)  # Affected stock codes
    analyzed_at: datetime | None = None
    analysis_source: str = "text"  # 'text', 'pdf', 'text_pdf_oversized', 'text_pdf_timeout'


@dataclass
class Message:
    """
    Represents a message from any data source.

    Data Flow:
        External Collector -> PostgreSQL (messages + message_analysis)
        -> MessageReader -> Message (with analysis) -> Strategy

    Fields:
        - id: Unique identifier (SHA256 hash)
        - source_type: Category of source (announcement/news/social)
        - source_name: Specific source name
        - title: Message title/headline
        - content: Full message content
        - url: Original URL (if available)
        - stock_codes: List of related stock codes (e.g., ["000001", "600519"])
        - publish_time: When the message was originally published
        - fetch_time: When we fetched this message
        - raw_data: Original data from source for debugging/reprocessing
        - analysis: AI analysis result (from message_analysis table)
    """

    source_type: str
    source_name: str
    title: str
    content: str
    publish_time: datetime
    id: str = field(default_factory=lambda: str(uuid4()))
    url: str | None = None
    stock_codes: list[str] = field(default_factory=list)
    fetch_time: datetime = field(default_factory=datetime.now)
    raw_data: dict[str, Any] | None = None
    # AI analysis result (populated from message_analysis table)
    analysis: MessageAnalysis | None = None

    def is_positive(self) -> bool:
        """Check if message has positive sentiment (bullish/strong_bullish)."""
        if self.analysis is None:
            return False
        return self.analysis.sentiment.is_positive()

    def is_strong_positive(self) -> bool:
        """Check if message has strong positive sentiment."""
        if self.analysis is None:
            return False
        return self.analysis.sentiment == Sentiment.STRONG_BULLISH

    def has_analysis(self) -> bool:
        """Check if message has been analyzed."""
        return self.analysis is not None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for database storage."""
        result: dict[str, Any] = {
            "id": self.id,
            "source_type": self.source_type,
            "source_name": self.source_name,
            "title": self.title,
            "content": self.content,
            "url": self.url,
            "stock_codes": ",".join(self.stock_codes) if self.stock_codes else "",
            "publish_time": self.publish_time.isoformat(),
            "fetch_time": self.fetch_time.isoformat(),
            "raw_data": str(self.raw_data) if self.raw_data else None,
        }

        if self.analysis:
            result["sentiment"] = self.analysis.sentiment.value
            result["confidence"] = self.analysis.confidence
            result["reasoning"] = self.analysis.reasoning
            result["extracted_entities"] = self.analysis.extracted_entities
            result["matched_sector_ids"] = self.analysis.matched_sector_ids
            result["affected_stocks"] = self.analysis.affected_stocks
            result["analyzed_at"] = (
                self.analysis.analyzed_at.isoformat() if self.analysis.analyzed_at else None
            )
            result["analysis_source"] = self.analysis.analysis_source

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Message":
        """Create Message from dictionary (database row)."""
        stock_codes_str = data.get("stock_codes", "")
        stock_codes = stock_codes_str.split(",") if stock_codes_str else []

        # Parse analysis if present
        analysis = None
        if data.get("sentiment"):
            try:
                sentiment = Sentiment(data["sentiment"])
                analyzed_at = None
                if data.get("analyzed_at"):
                    if isinstance(data["analyzed_at"], datetime):
                        analyzed_at = data["analyzed_at"]
                    else:
                        analyzed_at = datetime.fromisoformat(data["analyzed_at"])

                analysis = MessageAnalysis(
                    sentiment=sentiment,
                    confidence=float(data.get("confidence", 0.0)),
                    reasoning=data.get("reasoning", ""),
                    extracted_entities=data.get("extracted_entities", []),
                    matched_sector_ids=data.get("matched_sector_ids", []),
                    affected_stocks=data.get("affected_stocks", []),
                    analyzed_at=analyzed_at,
                    analysis_source=data.get("analysis_source", "text"),
                )
            except (ValueError, KeyError):
                analysis = None

        publish_time = data["publish_time"]
        if isinstance(publish_time, str):
            publish_time = datetime.fromisoformat(publish_time)

        fetch_time = data["fetch_time"]
        if isinstance(fetch_time, str):
            fetch_time = datetime.fromisoformat(fetch_time)

        return cls(
            id=data["id"],
            source_type=data["source_type"],
            source_name=data["source_name"],
            title=data["title"],
            content=data["content"],
            url=data.get("url"),
            stock_codes=stock_codes,
            publish_time=publish_time,
            fetch_time=fetch_time,
            raw_data=eval(data["raw_data"]) if data.get("raw_data") else None,
            analysis=analysis,
        )
