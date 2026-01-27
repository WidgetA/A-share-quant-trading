# === MODULE PURPOSE ===
# Defines the Message data model for storing news, announcements, and social media content.
# This is the core data structure used across the message module.

# === KEY CONCEPTS ===
# - Message: A piece of information from various sources (announcements, news, social media)
# - source_type: Category of the source (announcement/news/social)
# - source_name: Specific source identifier (e.g., "eastmoney", "sina", "xueqiu")

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import uuid4


@dataclass
class Message:
    """
    Represents a message from any data source.

    Data Flow:
        MessageSource.fetch_messages() -> Message -> MessageDatabase.save()

    Fields:
        - id: Unique identifier (UUID)
        - source_type: Category of source (announcement/news/social)
        - source_name: Specific source name
        - title: Message title/headline
        - content: Full message content
        - url: Original URL (if available)
        - stock_codes: List of related stock codes (e.g., ["000001", "600519"])
        - publish_time: When the message was originally published
        - fetch_time: When we fetched this message
        - raw_data: Original data from source for debugging/reprocessing
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

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
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

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Message":
        """Create Message from dictionary (database row)."""
        stock_codes_str = data.get("stock_codes", "")
        stock_codes = stock_codes_str.split(",") if stock_codes_str else []

        return cls(
            id=data["id"],
            source_type=data["source_type"],
            source_name=data["source_name"],
            title=data["title"],
            content=data["content"],
            url=data.get("url"),
            stock_codes=stock_codes,
            publish_time=datetime.fromisoformat(data["publish_time"]),
            fetch_time=datetime.fromisoformat(data["fetch_time"]),
            raw_data=eval(data["raw_data"]) if data.get("raw_data") else None,
        )
