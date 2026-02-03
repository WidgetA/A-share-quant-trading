# === MODULE PURPOSE ===
# Data readers for the strategy platform.
# Reads data from external sources (PostgreSQL, etc.) for use by strategies.

from .message_reader import (
    MessageReader,
    MessageReaderConfig,
    create_message_reader_from_config,
)

__all__ = [
    "MessageReader",
    "MessageReaderConfig",
    "create_message_reader_from_config",
]
