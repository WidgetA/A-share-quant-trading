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


def get_feishu_config() -> dict[str, str]:
    """
    Get Feishu bot configuration from environment variables.

    Environment variables:
        FEISHU_BOT_URL: Bot relay service URL (optional)
        FEISHU_APP_ID: Feishu app ID (required for sending)
        FEISHU_APP_SECRET: Feishu app secret (required for sending)
        FEISHU_CHAT_ID: Target chat ID (required for sending)

    Returns:
        Dictionary with bot configuration:
            - bot_url: Bot relay service URL
            - app_id: Feishu app ID
            - app_secret: Feishu app secret
            - chat_id: Target chat ID

    Usage:
        from src.common.config import get_feishu_config

        config = get_feishu_config()
        if config["app_id"] and config["app_secret"] and config["chat_id"]:
            # Feishu is configured, can send messages
            pass
    """
    import os

    return {
        "bot_url": os.getenv(
            "FEISHU_BOT_URL",
            "https://feishugroupbot-widgetinp950-g352rogo.leapcell.dev",
        ),
        "app_id": os.getenv("FEISHU_APP_ID", ""),
        "app_secret": os.getenv("FEISHU_APP_SECRET", ""),
        "chat_id": os.getenv("FEISHU_CHAT_ID", ""),
    }


def get_ifind_credentials() -> tuple[str, str]:
    """
    Get iFinD API credentials.

    Credentials are read in the following order:
    1. Environment variables: IFIND_USERNAME, IFIND_PASSWORD
    2. secrets.yaml file: ifind.username, ifind.password

    Returns:
        Tuple of (username, password)

    Raises:
        ValueError: If credentials are missing from both env and secrets.yaml

    Usage:
        from src.common.config import get_ifind_credentials

        username, password = get_ifind_credentials()
        iFinDPy.THS_iFinDLogin(username, password)
    """
    import os

    # Priority 1: Environment variables (for Docker deployment)
    username = os.environ.get("IFIND_USERNAME", "")
    password = os.environ.get("IFIND_PASSWORD", "")

    if username and password:
        logger.debug("Using iFinD credentials from environment variables")
        return username, password

    # Priority 2: secrets.yaml (for local development)
    try:
        secrets = load_secrets()
        username = secrets.get_str("ifind.username")
        password = secrets.get_str("ifind.password")

        if username and password:
            logger.debug("Using iFinD credentials from secrets.yaml")
            return username, password
    except FileNotFoundError:
        pass  # secrets.yaml not found, will raise ValueError below

    raise ValueError(
        "iFinD credentials not configured. "
        "Set IFIND_USERNAME and IFIND_PASSWORD environment variables, "
        "or configure in config/secrets.yaml"
    )


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


def get_web_config() -> dict[str, Any]:
    """
    Get Web UI configuration from environment variables.

    Environment variables:
        WEB_ENABLED: Whether to enable web UI (default: false)
        WEB_HOST: Host to bind to (default: 0.0.0.0)
        WEB_PORT: Port to listen on (default: 8000)
        WEB_BASE_URL: Base URL for the web UI (optional)
        INTERACTION_MODE: Interaction mode - 'cli' or 'web' (default: cli)

    Returns:
        Dictionary with web configuration:
            - enabled: Whether web UI is enabled
            - host: Host to bind to
            - port: Port to listen on
            - base_url: Base URL for the web UI
            - interaction_mode: Interaction mode

    Usage:
        from src.common.config import get_web_config

        config = get_web_config()
        if config["enabled"]:
            # Start web server
            pass
    """
    import os

    enabled_str = os.getenv("WEB_ENABLED", "false").lower()
    enabled = enabled_str in ("true", "yes", "1", "on")

    return {
        "enabled": enabled,
        "host": os.getenv("WEB_HOST", "0.0.0.0"),
        "port": int(os.getenv("WEB_PORT", "8000")),
        "base_url": os.getenv("WEB_BASE_URL", ""),
        "interaction_mode": os.getenv("INTERACTION_MODE", "cli"),
    }
