"""Frozen V22 entry selection, replacing V20's morning-selection policy."""

VERSION = "V22-slim"


def is_v22_family(version: object) -> bool:
    """Versions that share the frozen V22 selector and original entry policy."""
    return version in (VERSION, "V24.5")
