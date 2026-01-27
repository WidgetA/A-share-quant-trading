# === MODULE PURPOSE ===
# Message sources: pluggable data sources for fetching messages.
#
# === USAGE ===
# As library:
#   from src.data.sources import AkshareAnnouncementSource, ANNOUNCEMENT_CATEGORIES
#   source = AkshareAnnouncementSource(category="major")
#
# As CLI:
#   python -m src.data.sources.akshare_announcement -d 20260124
#   python -m src.data.sources.akshare_announcement -s 20260124 -e 20260126 -c major

from .base import BaseMessageSource
from .akshare_announcement import (
    AkshareAnnouncementSource,
    ANNOUNCEMENT_CATEGORIES,
)
from .cls_news import CLSNewsSource
from .eastmoney_news import EastmoneyNewsSource
from .registry import SourceRegistry
from .sina_news import SinaNewsSource

__all__ = [
    # Base
    "BaseMessageSource",
    "SourceRegistry",
    # Sources
    "AkshareAnnouncementSource",
    "CLSNewsSource",
    "EastmoneyNewsSource",
    "SinaNewsSource",
    # Constants
    "ANNOUNCEMENT_CATEGORIES",
]
