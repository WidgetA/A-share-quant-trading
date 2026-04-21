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

# Global stock blacklist — excluded from ALL data pipelines and trading signals.
# Tushare stk_mins returns empty for these codes on ALL dates.
STOCK_BLACKLIST: frozenset[str] = frozenset(
    {
        "302132",  # Tushare无分钟线数据，决策信息不足
    }
)


def get_stock_blacklist() -> frozenset[str]:
    """Return the global stock blacklist."""
    return STOCK_BLACKLIST


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
        token = secrets.get_str("tushare.token")
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


# --- Cache Scheduler Toggle ---

_cache_scheduler_enabled_override: bool | None = None
CACHE_SCHEDULER_FILE = PROJECT_ROOT / "data" / "cache_scheduler_enabled.txt"


def get_cache_scheduler_enabled() -> bool:
    """Return whether the cache scheduler is enabled. Default: True."""
    global _cache_scheduler_enabled_override
    if _cache_scheduler_enabled_override is not None:
        return _cache_scheduler_enabled_override
    if CACHE_SCHEDULER_FILE.exists():
        val = CACHE_SCHEDULER_FILE.read_text(encoding="utf-8").strip().lower()
        if val in ("false", "0", "off", "no"):
            return False
        return True
    return True


def set_cache_scheduler_enabled(enabled: bool) -> None:
    """Set cache scheduler enabled state and persist to disk."""
    global _cache_scheduler_enabled_override
    _cache_scheduler_enabled_override = enabled
    CACHE_SCHEDULER_FILE.parent.mkdir(parents=True, exist_ok=True)
    CACHE_SCHEDULER_FILE.write_text(str(enabled).lower(), encoding="utf-8")
    logger.info(f"Cache scheduler {'enabled' if enabled else 'disabled'} via web UI")


# --- Daily Scan Toggle ---

_daily_scan_enabled_override: bool | None = None
DAILY_SCAN_FILE = PROJECT_ROOT / "data" / "daily_scan_enabled.txt"


def get_daily_scan_enabled() -> bool:
    """Return whether the daily scan is enabled. Default: True."""
    global _daily_scan_enabled_override
    if _daily_scan_enabled_override is not None:
        return _daily_scan_enabled_override
    if DAILY_SCAN_FILE.exists():
        val = DAILY_SCAN_FILE.read_text(encoding="utf-8").strip().lower()
        if val in ("false", "0", "off", "no"):
            return False
        return True
    return True


def set_daily_scan_enabled(enabled: bool) -> None:
    """Set daily scan enabled state and persist to disk."""
    global _daily_scan_enabled_override
    _daily_scan_enabled_override = enabled
    DAILY_SCAN_FILE.parent.mkdir(parents=True, exist_ok=True)
    DAILY_SCAN_FILE.write_text(str(enabled).lower(), encoding="utf-8")
    logger.info(f"Daily scan {'enabled' if enabled else 'disabled'} via web UI")


# --- Recommendations Toggle ---

_recommendations_enabled_override: bool | None = None
RECOMMENDATIONS_FILE = PROJECT_ROOT / "data" / "recommendations_enabled.txt"


def get_recommendations_enabled() -> bool:
    """Return whether the recommendations panel is enabled. Default: True."""
    global _recommendations_enabled_override
    if _recommendations_enabled_override is not None:
        return _recommendations_enabled_override
    if RECOMMENDATIONS_FILE.exists():
        val = RECOMMENDATIONS_FILE.read_text(encoding="utf-8").strip().lower()
        if val in ("false", "0", "off", "no"):
            return False
        return True
    return True


def set_recommendations_enabled(enabled: bool) -> None:
    """Set recommendations enabled state and persist to disk."""
    global _recommendations_enabled_override
    _recommendations_enabled_override = enabled
    RECOMMENDATIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    RECOMMENDATIONS_FILE.write_text(str(enabled).lower(), encoding="utf-8")
    logger.info(f"Recommendations {'enabled' if enabled else 'disabled'} via web UI")


# --- S3 Config ---

S3_CONFIG_FILE = PROJECT_ROOT / "data" / "s3_config.json"


GREPTIMEDB_TOML = PROJECT_ROOT / "config" / "greptimedb.toml"


def _read_s3_from_greptimedb_toml() -> dict[str, str]:
    """Read OSS credentials from config/greptimedb.toml [storage] section.

    The endpoint may use internal VPC address (oss-cn-xxx-internal.aliyuncs.com).
    For boto3 S3-compatible access, strip '-internal' and ensure https://.
    """
    if not GREPTIMEDB_TOML.exists():
        return {}
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[no-redef]

    try:
        with open(GREPTIMEDB_TOML, "rb") as f:
            data = tomllib.load(f)
    except Exception:
        return {}

    storage = data.get("storage", {})
    if storage.get("type", "").lower() != "oss":
        return {}

    endpoint = storage.get("endpoint", "")
    endpoint = endpoint.replace("-internal.", ".")
    if endpoint and not endpoint.startswith("https://"):
        endpoint = f"https://{endpoint}"

    bucket = storage.get("bucket", "")
    access_key = storage.get("access_key_id", "")
    secret_key = storage.get("access_key_secret", "")
    if endpoint and bucket:
        return {
            "endpoint_url": endpoint,
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket": bucket,
        }
    return {}


def get_s3_config() -> dict[str, str]:
    """Get S3 configuration.

    Priority: persisted file > greptimedb.toml > env vars > secrets.yaml.

    Returns:
        Dict with keys: endpoint_url, access_key, secret_key, bucket.
        Empty dict if not configured.
    """
    import json
    import os

    # 1. Persisted file (web UI override)
    if S3_CONFIG_FILE.exists():
        try:
            config = json.loads(S3_CONFIG_FILE.read_text(encoding="utf-8"))
            if config.get("endpoint_url") and config.get("bucket"):
                return config
        except (json.JSONDecodeError, KeyError):
            pass

    # 2. GreptimeDB TOML — same OSS bucket, already has credentials
    config = _read_s3_from_greptimedb_toml()
    if config:
        return config

    # 3. Environment variables
    endpoint = os.environ.get("S3_ENDPOINT_URL", "")
    access_key = os.environ.get("S3_ACCESS_KEY", "")
    secret_key = os.environ.get("S3_SECRET_KEY", "")
    bucket = os.environ.get("S3_BUCKET", "")
    if endpoint and bucket:
        return {
            "endpoint_url": endpoint,
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket": bucket,
        }

    # 4. secrets.yaml
    try:
        secrets = load_secrets()
        endpoint = secrets.get_str("s3.endpoint_url") or ""
        if endpoint:
            return {
                "endpoint_url": endpoint,
                "access_key": secrets.get_str("s3.access_key") or "",
                "secret_key": secrets.get_str("s3.secret_key") or "",
                "bucket": secrets.get_str("s3.bucket") or "",
            }
    except FileNotFoundError:
        pass

    return {}


def set_s3_config(config: dict[str, str]) -> None:
    """Persist S3 configuration to disk."""
    import json

    S3_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    S3_CONFIG_FILE.write_text(json.dumps(config, indent=2), encoding="utf-8")
    logger.info("S3 config updated via web UI")


# --- FC (Serverless Training) URL ---

FC_URL_FILE = PROJECT_ROOT / "data" / "fc_url.txt"


def get_fc_url() -> str | None:
    """Get FC serverless training endpoint URL.

    Priority: persisted file > env var FC_ENDPOINT_URL.
    Returns None if not configured.
    """
    import os

    if FC_URL_FILE.exists():
        try:
            url = FC_URL_FILE.read_text(encoding="utf-8").strip()
            if url:
                return url
        except OSError:
            pass

    return os.environ.get("FC_ENDPOINT_URL") or None


def set_fc_url(url: str) -> None:
    """Persist FC endpoint URL to disk."""
    FC_URL_FILE.parent.mkdir(parents=True, exist_ok=True)
    FC_URL_FILE.write_text(url.strip(), encoding="utf-8")
    logger.info("FC endpoint URL updated via web UI")


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
