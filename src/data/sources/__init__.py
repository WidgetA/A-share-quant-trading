# === MODULE PURPOSE ===
# Data sources for strategy platform.
# Note: Message collection has been moved to external project.
# This module only contains utility sources used by strategies.

from .announcement_content import (
    AliyunConfig,
    AnnouncementContent,
    AnnouncementContentFetcher,
    create_content_fetcher_from_config,
)
from .ifind_limit_up import IFinDLimitUpSource
from .sector_mapper import SectorData, SectorInfo, SectorMapper

__all__ = [
    # Sector mapping
    "SectorMapper",
    "SectorInfo",
    "SectorData",
    # Content fetcher
    "AliyunConfig",
    "AnnouncementContent",
    "AnnouncementContentFetcher",
    "create_content_fetcher_from_config",
    # iFinD
    "IFinDLimitUpSource",
]
