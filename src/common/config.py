# === MODULE PURPOSE ===
# Configuration management for the trading system.
# Loads YAML configuration files and provides typed access to settings.

# === KEY CONCEPTS ===
# - YAML-based: Human-readable configuration format
# - Environment-aware: Support for dev/staging/prod configs
# - Type-safe: Provides typed accessors for settings
# - Secrets separation: Sensitive credentials stored in secrets.yaml

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent
SECRETS_PATH = PROJECT_ROOT / "config" / "secrets.yaml"


class Config:
    """
    Configuration loader and accessor.

    Loads configuration from YAML files and provides typed access
    to configuration values.

    Usage:
        config = Config.load("config/message-config.yaml")

        # Access nested values
        db_path = config.get("message.database.path", default="data/messages.db")

        # Access with type checking
        interval = config.get_int("message.sources.news.interval", default=30)
    """

    def __init__(self, data: dict[str, Any]):
        self._data = data

    @classmethod
    def load(cls, config_path: str | Path) -> "Config":
        """
        Load configuration from a YAML file.

        Args:
            config_path: Path to the YAML configuration file

        Returns:
            Config instance with loaded data

        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If YAML parsing fails
        """
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        logger.info(f"Loaded configuration from {path}")
        return cls(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Config":
        """Create Config from a dictionary."""
        return cls(data)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by dot-separated key.

        Args:
            key: Dot-separated path (e.g., "message.database.path")
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        keys = key.split(".")
        value: Any = self._data

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default

        return value

    def get_str(self, key: str, default: str = "") -> str:
        """Get a string configuration value."""
        value = self.get(key, default)
        return str(value) if value is not None else default

    def get_int(self, key: str, default: int = 0) -> int:
        """Get an integer configuration value."""
        value = self.get(key, default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def get_float(self, key: str, default: float = 0.0) -> float:
        """Get a float configuration value."""
        value = self.get(key, default)
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get a boolean configuration value."""
        value = self.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "yes", "1", "on")
        return bool(value) if value is not None else default

    def get_list(self, key: str, default: list | None = None) -> list:
        """Get a list configuration value."""
        value = self.get(key, default)
        if isinstance(value, list):
            return value
        return default if default is not None else []

    def get_dict(self, key: str, default: dict | None = None) -> dict:
        """Get a dictionary configuration value."""
        value = self.get(key, default)
        if isinstance(value, dict):
            return value
        return default if default is not None else {}

    @property
    def raw(self) -> dict[str, Any]:
        """Access raw configuration data."""
        return self._data

    def __repr__(self) -> str:
        return f"Config({list(self._data.keys())})"


# === SECRETS MANAGEMENT ===

_secrets_cache: Config | None = None


def load_secrets() -> Config:
    """
    Load secrets from config/secrets.yaml.

    Returns:
        Config instance with secrets data

    Raises:
        FileNotFoundError: If secrets.yaml doesn't exist

    Usage:
        from src.common.config import load_secrets

        secrets = load_secrets()
        ifind_user = secrets.get_str("ifind.username")
        ifind_pass = secrets.get_str("ifind.password")
    """
    global _secrets_cache
    if _secrets_cache is not None:
        return _secrets_cache

    if not SECRETS_PATH.exists():
        raise FileNotFoundError(
            f"Secrets file not found: {SECRETS_PATH}\n"
            "Please copy config/secrets.yaml.example to config/secrets.yaml "
            "and fill in your credentials."
        )

    _secrets_cache = Config.load(SECRETS_PATH)
    logger.info("Loaded secrets configuration")
    return _secrets_cache


def get_ifind_credentials() -> tuple[str, str]:
    """
    Get iFinD API credentials.

    Returns:
        Tuple of (username, password)

    Raises:
        FileNotFoundError: If secrets.yaml doesn't exist
        ValueError: If credentials are missing

    Usage:
        from src.common.config import get_ifind_credentials

        username, password = get_ifind_credentials()
        iFinDPy.THS_iFinDLogin(username, password)
    """
    secrets = load_secrets()
    username = secrets.get_str("ifind.username")
    password = secrets.get_str("ifind.password")

    if not username or not password:
        raise ValueError("iFinD credentials not configured in secrets.yaml")

    return username, password


def load_config(config_path: str | Path) -> Config:
    """
    Load configuration from a YAML file.

    This is a convenience function that wraps Config.load().

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        Config instance
    """
    return Config.load(config_path)
