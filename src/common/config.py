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

# Runtime override for iFinD refresh_token (set via web UI)
_ifind_token_override: str | None = None
# Persistence file for iFinD refresh_token (survives container restarts)
IFIND_TOKEN_FILE = PROJECT_ROOT / "data" / "ifind_token.txt"

# Runtime override for Tavily API key (set via web UI)
_tavily_key_override: str | None = None
# Persistence file for Tavily API key (survives container restarts)
TAVILY_KEY_FILE = PROJECT_ROOT / "data" / "tavily_key.txt"

# Runtime override for iQuant API key (set via web UI)
_iquant_key_override: str | None = None
# Persistence file for iQuant API key (survives container restarts)
IQUANT_KEY_FILE = PROJECT_ROOT / "data" / "iquant_api_key.txt"

# Runtime override for Tsanghi (沧海数据) token (set via web UI)
_tsanghi_token_override: str | None = None
# Persistence file for Tsanghi token (survives container restarts)
TSANGHI_TOKEN_FILE = PROJECT_ROOT / "data" / "tsanghi_token.txt"


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
    Get iFinD API credentials (legacy SDK method).

    DEPRECATED: Use get_ifind_refresh_token() for HTTP API instead.

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


def get_ifind_refresh_token() -> str:
    """
    Get iFinD HTTP API refresh_token.

    The refresh_token is used to obtain access_token for HTTP API calls.
    It can be obtained from the iFinD Windows client:
    Tools -> refresh_token Query/Update

    Credentials are read in the following order:
    1. Runtime override (set via web UI, in-memory)
    2. Persisted file (data/ifind_token.txt, survives restarts)
    3. Environment variable: IFIND_REFRESH_TOKEN
    4. secrets.yaml file: ifind.refresh_token

    Returns:
        refresh_token string

    Raises:
        ValueError: If refresh_token is not configured

    Usage:
        from src.common.config import get_ifind_refresh_token

        refresh_token = get_ifind_refresh_token()
        # Use with IFinDHttpClient
    """
    import os

    # Priority 1: Runtime override (set via web UI)
    if _ifind_token_override:
        logger.debug("Using iFinD refresh_token from runtime override")
        return _ifind_token_override

    # Priority 2: Persisted file (survives container restarts)
    if IFIND_TOKEN_FILE.exists():
        token = IFIND_TOKEN_FILE.read_text(encoding="utf-8").strip()
        if token:
            logger.debug("Using iFinD refresh_token from persisted file")
            return token

    # Priority 3: Environment variable (for Docker deployment)
    refresh_token = os.environ.get("IFIND_REFRESH_TOKEN", "")

    if refresh_token:
        logger.debug("Using iFinD refresh_token from environment variable")
        return refresh_token

    # Priority 4: secrets.yaml (for local development)
    try:
        secrets = load_secrets()
        refresh_token = secrets.get_str("ifind.refresh_token")

        if refresh_token:
            logger.debug("Using iFinD refresh_token from secrets.yaml")
            return refresh_token
    except FileNotFoundError:
        pass  # secrets.yaml not found, will raise ValueError below

    raise ValueError(
        "iFinD refresh_token not configured. "
        "Set IFIND_REFRESH_TOKEN environment variable, "
        "or configure ifind.refresh_token in config/secrets.yaml. "
        "You can obtain refresh_token from iFinD Windows client: "
        "Tools -> refresh_token Query/Update"
    )


def set_ifind_refresh_token(token: str) -> None:
    """
    Set iFinD refresh_token at runtime and persist to disk.

    Called from the web UI settings page. The token takes effect
    immediately for all new IFinDHttpClient instances.

    Args:
        token: The new refresh_token string
    """
    global _ifind_token_override
    _ifind_token_override = token

    # Persist to file so it survives container restarts
    IFIND_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    IFIND_TOKEN_FILE.write_text(token, encoding="utf-8")
    logger.info("iFinD refresh_token updated via web UI and persisted to disk")


def get_ifind_token_source() -> str:
    """
    Return which source the current iFinD refresh_token comes from.

    Returns:
        One of: "web_ui", "persisted_file", "env_var", "secrets_yaml", "not_configured"
    """
    import os

    if _ifind_token_override:
        return "web_ui"
    if IFIND_TOKEN_FILE.exists() and IFIND_TOKEN_FILE.read_text(encoding="utf-8").strip():
        return "persisted_file"
    if os.environ.get("IFIND_REFRESH_TOKEN", ""):
        return "env_var"
    try:
        secrets = load_secrets()
        if secrets.get_str("ifind.refresh_token"):
            return "secrets_yaml"
    except FileNotFoundError:
        pass
    return "not_configured"


def get_tavily_api_key() -> str:
    """
    Get Tavily API key for web search.

    Credentials are read in the following order:
    1. Runtime override (set via web UI, in-memory)
    2. Persisted file (data/tavily_key.txt, survives restarts)
    3. Environment variable: TAVILY_API_KEY
    4. secrets.yaml file: tavily.api_key

    Returns:
        Tavily API key string

    Raises:
        ValueError: If API key is not configured
    """
    import os

    if _tavily_key_override:
        return _tavily_key_override

    if TAVILY_KEY_FILE.exists():
        key = TAVILY_KEY_FILE.read_text(encoding="utf-8").strip()
        if key:
            return key

    env_key = os.environ.get("TAVILY_API_KEY", "")
    if env_key:
        return env_key

    try:
        secrets = load_secrets()
        key = secrets.get_str("tavily.api_key")
        if key:
            return key
    except FileNotFoundError:
        pass

    raise ValueError(
        "Tavily API key not configured. "
        "Set via web UI Settings page, TAVILY_API_KEY environment variable, "
        "or configure tavily.api_key in config/secrets.yaml. "
        "Get a free key at https://tavily.com"
    )


def set_tavily_api_key(key: str) -> None:
    """Set Tavily API key at runtime and persist to disk."""
    global _tavily_key_override
    _tavily_key_override = key

    TAVILY_KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
    TAVILY_KEY_FILE.write_text(key, encoding="utf-8")
    logger.info("Tavily API key updated via web UI and persisted to disk")


def get_tavily_key_source() -> str:
    """
    Return which source the current Tavily API key comes from.

    Returns:
        One of: "web_ui", "persisted_file", "env_var", "secrets_yaml", "not_configured"
    """
    import os

    if _tavily_key_override:
        return "web_ui"
    if TAVILY_KEY_FILE.exists() and TAVILY_KEY_FILE.read_text(encoding="utf-8").strip():
        return "persisted_file"
    if os.environ.get("TAVILY_API_KEY", ""):
        return "env_var"
    try:
        secrets = load_secrets()
        if secrets.get_str("tavily.api_key"):
            return "secrets_yaml"
    except FileNotFoundError:
        pass
    return "not_configured"


# === Tushare Token ===

# Runtime override for Tushare Pro token (set via web UI)
_tushare_token_override: str | None = None
# Persistence file for Tushare Pro token (survives container restarts)
TUSHARE_TOKEN_FILE = PROJECT_ROOT / "data" / "tushare_token.txt"


def get_tushare_token() -> str:
    """Get Tushare Pro API token.

    Priority: runtime override > persisted file > env var > secrets.yaml.

    Returns:
        Tushare Pro token string

    Raises:
        ValueError: If token is not configured
    """
    import os

    if _tushare_token_override:
        return _tushare_token_override

    if TUSHARE_TOKEN_FILE.exists():
        token = TUSHARE_TOKEN_FILE.read_text(encoding="utf-8").strip()
        if token:
            return token

    env_token = os.environ.get("TUSHARE_TOKEN", "")
    if env_token:
        return env_token

    try:
        secrets = load_secrets()
        token = secrets.get_str("tushare.token")
        if token:
            return token
    except FileNotFoundError:
        pass

    raise ValueError(
        "Tushare Pro token not configured. "
        "Set via web UI Settings page, TUSHARE_TOKEN environment variable, "
        "or configure tushare.token in config/secrets.yaml. "
        "Get your token at https://tushare.pro"
    )


def set_tushare_token(token: str) -> None:
    """Set Tushare Pro token at runtime and persist to disk."""
    global _tushare_token_override
    _tushare_token_override = token

    TUSHARE_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TUSHARE_TOKEN_FILE.write_text(token, encoding="utf-8")
    logger.info("Tushare Pro token updated via web UI and persisted to disk")


def get_tushare_token_source() -> str:
    """Return which source the current Tushare Pro token comes from."""
    import os

    if _tushare_token_override:
        return "web_ui"
    if TUSHARE_TOKEN_FILE.exists() and TUSHARE_TOKEN_FILE.read_text(encoding="utf-8").strip():
        return "persisted_file"
    if os.environ.get("TUSHARE_TOKEN", ""):
        return "env_var"
    try:
        secrets = load_secrets()
        if secrets.get_str("tushare.token"):
            return "secrets_yaml"
    except FileNotFoundError:
        pass
    return "not_configured"


# === iQuant API Key ===


def get_iquant_api_key() -> str:
    """Get iQuant API key for authenticating iQuant script requests.

    Priority: runtime override > persisted file > env var > not configured.
    """
    import os

    if _iquant_key_override:
        return _iquant_key_override

    if IQUANT_KEY_FILE.exists():
        key = IQUANT_KEY_FILE.read_text(encoding="utf-8").strip()
        if key:
            return key

    env_key = os.environ.get("IQUANT_API_KEY", "")
    if env_key:
        return env_key

    raise ValueError(
        "iQuant API key not configured. "
        "Set via web UI Settings page or IQUANT_API_KEY environment variable."
    )


def set_iquant_api_key(key: str) -> None:
    """Set iQuant API key at runtime and persist to disk."""
    global _iquant_key_override
    _iquant_key_override = key

    IQUANT_KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
    IQUANT_KEY_FILE.write_text(key, encoding="utf-8")
    logger.info("iQuant API key updated via web UI and persisted to disk")


def get_iquant_key_source() -> str:
    """Return which source the current iQuant API key comes from."""
    import os

    if _iquant_key_override:
        return "web_ui"
    if IQUANT_KEY_FILE.exists() and IQUANT_KEY_FILE.read_text(encoding="utf-8").strip():
        return "persisted_file"
    if os.environ.get("IQUANT_API_KEY", ""):
        return "env_var"
    return "not_configured"


# === Tsanghi (沧海数据) Token ===


def get_tsanghi_token() -> str:
    """Get Tsanghi (沧海数据) API token for free backtest daily data.

    Priority: runtime override > persisted file > env var > secrets.yaml.

    Returns:
        Tsanghi token string

    Raises:
        ValueError: If token is not configured
    """
    import os

    if _tsanghi_token_override:
        return _tsanghi_token_override

    if TSANGHI_TOKEN_FILE.exists():
        token = TSANGHI_TOKEN_FILE.read_text(encoding="utf-8").strip()
        if token:
            return token

    env_token = os.environ.get("TSANGHI_TOKEN", "")
    if env_token:
        return env_token

    try:
        secrets = load_secrets()
        token = secrets.get_str("tsanghi.token")
        if token:
            return token
    except FileNotFoundError:
        pass

    raise ValueError(
        "Tsanghi token not configured. "
        "Set via web UI Settings page, TSANGHI_TOKEN environment variable, "
        "or configure tsanghi.token in config/secrets.yaml. "
        "Register at https://tsanghi.com to get a free token."
    )


def set_tsanghi_token(token: str) -> None:
    """Set Tsanghi token at runtime and persist to disk."""
    global _tsanghi_token_override
    _tsanghi_token_override = token

    TSANGHI_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TSANGHI_TOKEN_FILE.write_text(token, encoding="utf-8")
    logger.info("Tsanghi token updated via web UI and persisted to disk")


def get_tsanghi_token_source() -> str:
    """Return which source the current Tsanghi token comes from."""
    import os

    if _tsanghi_token_override:
        return "web_ui"
    if TSANGHI_TOKEN_FILE.exists() and TSANGHI_TOKEN_FILE.read_text(encoding="utf-8").strip():
        return "persisted_file"
    if os.environ.get("TSANGHI_TOKEN", ""):
        return "env_var"
    try:
        secrets = load_secrets()
        if secrets.get_str("tsanghi.token"):
            return "secrets_yaml"
    except FileNotFoundError:
        pass
    return "not_configured"


# === Aliyun DashScope API Key ===

_aliyun_key_override: str | None = None
ALIYUN_KEY_FILE = PROJECT_ROOT / "data" / "aliyun_api_key.txt"


def get_aliyun_api_key() -> str:
    """Get Aliyun DashScope API key for LLM calls.

    Priority: runtime override > persisted file > env var > secrets.yaml.

    Returns:
        Aliyun API key string

    Raises:
        ValueError: If key is not configured
    """
    import os

    if _aliyun_key_override:
        return _aliyun_key_override

    if ALIYUN_KEY_FILE.exists():
        key = ALIYUN_KEY_FILE.read_text(encoding="utf-8").strip()
        if key:
            return key

    env_key = os.environ.get("ALIYUN_API_KEY", "")
    if env_key:
        return env_key

    try:
        secrets = load_secrets()
        key = secrets.get_str("aliyun.api_key", "")
        if key:
            return key
    except FileNotFoundError:
        pass

    raise ValueError(
        "Aliyun DashScope API key not configured. "
        "Set via web UI Settings page, ALIYUN_API_KEY environment variable, "
        "or configure aliyun.api_key in config/secrets.yaml."
    )


def set_aliyun_api_key(key: str) -> None:
    """Set Aliyun API key at runtime and persist to disk."""
    global _aliyun_key_override
    _aliyun_key_override = key

    ALIYUN_KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
    ALIYUN_KEY_FILE.write_text(key, encoding="utf-8")
    logger.info("Aliyun API key updated via web UI and persisted to disk")


def get_aliyun_api_key_source() -> str:
    """Return which source the current Aliyun API key comes from."""
    import os

    if _aliyun_key_override:
        return "web_ui"
    if ALIYUN_KEY_FILE.exists():
        if ALIYUN_KEY_FILE.read_text(encoding="utf-8").strip():
            return "persisted_file"
    if os.environ.get("ALIYUN_API_KEY", ""):
        return "env_var"
    try:
        secrets = load_secrets()
        if secrets.get_str("aliyun.api_key", ""):
            return "secrets_yaml"
    except FileNotFoundError:
        pass
    return "not_configured"


# === Monitor Data Source ===

_monitor_ds_override: str | None = None
MONITOR_DS_FILE = PROJECT_ROOT / "data" / "monitor_data_source.txt"


def get_monitor_data_source() -> str:
    """Get intraday monitor data source: 'ifind' or 'tushare'.

    Priority: runtime override > persisted file > default ('tushare').
    Legacy value 'sina' is treated as 'tushare' (Sina API defunct).
    """
    if _monitor_ds_override:
        val = _monitor_ds_override
    elif MONITOR_DS_FILE.exists():
        val = MONITOR_DS_FILE.read_text(encoding="utf-8").strip()
    else:
        val = "tushare"

    # Backward compat: "sina" → "tushare" (Sina free API removed)
    if val == "sina":
        return "tushare"
    if val in ("ifind", "tushare"):
        return val
    return "tushare"


def set_monitor_data_source(source: str) -> None:
    """Set monitor data source and persist to disk."""
    global _monitor_ds_override
    if source not in ("ifind", "tushare"):
        raise ValueError(f"Invalid data source: {source}. Must be 'ifind' or 'tushare'.")
    _monitor_ds_override = source

    MONITOR_DS_FILE.parent.mkdir(parents=True, exist_ok=True)
    MONITOR_DS_FILE.write_text(source, encoding="utf-8")
    logger.info(f"Monitor data source set to '{source}' and persisted")


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
