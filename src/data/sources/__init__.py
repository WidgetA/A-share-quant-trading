# === MODULE PURPOSE ===
# Message sources: pluggable data sources for fetching messages.

from .base import BaseMessageSource
from .baostock_announcement import BaostockAnnouncementSource
from .cls_news import CLSNewsSource
from .eastmoney_news import EastmoneyNewsSource
from .registry import SourceRegistry
from .sina_news import SinaNewsSource

__all__ = [
    "BaseMessageSource",
    "SourceRegistry",
    "BaostockAnnouncementSource",
    "CLSNewsSource",
    "EastmoneyNewsSource",
    "SinaNewsSource",
]
