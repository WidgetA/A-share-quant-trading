# === MODULE PURPOSE ===
# Data sources for strategy platform.
# Note: Message collection has been moved to external project.
# This module only contains utility sources used by strategies.

# Sector mapping (stock code <-> industry sector)
from .sector_mapper import SectorMapper, SectorInfo, SectorData

# Announcement content fetcher (PDF/HTML reading via Aliyun)
from .announcement_content import (
    AliyunConfig,
    AnnouncementContent,
    AnnouncementContentFetcher,
    create_content_fetcher_from_config,
)

# iFinD limit-up stocks
from .ifind_limit_up import IFinDLimitUpSource

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
