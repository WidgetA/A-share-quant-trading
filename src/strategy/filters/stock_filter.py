# === MODULE PURPOSE ===
# Stock filter to exclude specific exchanges from trading.
# Filters Beijing Stock Exchange (BSE) and ChiNext (GEM) stocks.

# === KEY CONCEPTS ===
# Stock code patterns by exchange:
# - 上证主板 (Shanghai Main): 600xxx, 601xxx, 603xxx, 605xxx
# - 深证主板 (Shenzhen Main): 000xxx, 001xxx
# - 中小板 (SME): 002xxx
# - 创业板 (ChiNext/GEM): 300xxx - EXCLUDED
# - 科创板 (STAR): 688xxx - Allowed by default
# - 北交所 (BSE): 8xxxxx, 4xxxxx (e.g., 830xxx, 430xxx) - EXCLUDED

import logging
import re
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class Exchange(Enum):
    """A-share stock exchanges."""

    SHANGHAI_MAIN = "上证主板"
    SHENZHEN_MAIN = "深证主板"
    SME = "中小板"
    CHINEXT = "创业板"
    STAR = "科创板"
    BSE = "北交所"
    UNKNOWN = "未知"


@dataclass
class StockFilterConfig:
    """Configuration for stock filtering."""

    exclude_bse: bool = True  # Exclude Beijing Stock Exchange
    exclude_chinext: bool = True  # Exclude ChiNext (创业板)
    exclude_star: bool = False  # Exclude STAR Market (科创板)


class StockFilter:
    """
    Filters stocks by exchange rules.

    Default filtering rules:
        - Exclude 北交所 (BSE): Codes starting with 8 or 4
        - Exclude 创业板 (ChiNext): Codes starting with 300
        - Allow 科创板 (STAR): Codes starting with 688 (configurable)

    Allowed exchanges by default:
        - 上证主板: 600xxx, 601xxx, 603xxx, 605xxx
        - 深证主板: 000xxx, 001xxx
        - 中小板: 002xxx
        - 科创板: 688xxx

    Usage:
        filter = StockFilter()

        # Check single stock
        if filter.is_allowed("000001"):
            print("平安银行 is allowed")

        # Filter list of stocks
        stocks = ["000001", "300001", "830001"]
        allowed = filter.filter_stocks(stocks)
        # Returns: ["000001"]

        # Get exchange name
        exchange = filter.get_exchange("688001")
        # Returns: Exchange.STAR
    """

    # Code patterns for each exchange
    # Shanghai Main Board: 600xxx, 601xxx, 603xxx, 605xxx
    _SHANGHAI_MAIN_PATTERNS = [r"^60[0135]\d{3}$"]

    # Shenzhen Main Board: 000xxx, 001xxx
    _SHENZHEN_MAIN_PATTERNS = [r"^00[01]\d{3}$"]

    # SME Board (中小板): 002xxx
    _SME_PATTERNS = [r"^002\d{3}$"]

    # ChiNext (创业板): 300xxx, 301xxx
    _CHINEXT_PATTERNS = [r"^30[01]\d{3}$"]

    # STAR Market (科创板): 688xxx, 689xxx
    _STAR_PATTERNS = [r"^68[89]\d{3}$"]

    # BSE (北交所): 8xxxxx, 4xxxxx
    _BSE_PATTERNS = [r"^[84]\d{5}$"]

    def __init__(self, config: StockFilterConfig | None = None):
        """
        Initialize stock filter.

        Args:
            config: Filter configuration. Uses defaults if None.
        """
        self._config = config or StockFilterConfig()
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        """Compile regex patterns for each exchange."""
        self._patterns = {
            Exchange.SHANGHAI_MAIN: [re.compile(p) for p in self._SHANGHAI_MAIN_PATTERNS],
            Exchange.SHENZHEN_MAIN: [re.compile(p) for p in self._SHENZHEN_MAIN_PATTERNS],
            Exchange.SME: [re.compile(p) for p in self._SME_PATTERNS],
            Exchange.CHINEXT: [re.compile(p) for p in self._CHINEXT_PATTERNS],
            Exchange.STAR: [re.compile(p) for p in self._STAR_PATTERNS],
            Exchange.BSE: [re.compile(p) for p in self._BSE_PATTERNS],
        }

    def get_exchange(self, stock_code: str) -> Exchange:
        """
        Determine the exchange for a stock code.

        Args:
            stock_code: 6-digit stock code (without suffix like .SZ/.SH).

        Returns:
            Exchange enum indicating the stock's exchange.
        """
        # Normalize code (remove suffix if present)
        code = self._normalize_code(stock_code)

        if not code:
            return Exchange.UNKNOWN

        for exchange, patterns in self._patterns.items():
            for pattern in patterns:
                if pattern.match(code):
                    return exchange

        return Exchange.UNKNOWN

    def is_allowed(self, stock_code: str) -> bool:
        """
        Check if a stock code passes the filter.

        Args:
            stock_code: 6-digit stock code (without suffix like .SZ/.SH).

        Returns:
            True if the stock is allowed, False if filtered out.
        """
        exchange = self.get_exchange(stock_code)

        # Unknown exchanges are not allowed
        if exchange == Exchange.UNKNOWN:
            return False

        # Check exclusion rules
        if self._config.exclude_bse and exchange == Exchange.BSE:
            return False

        if self._config.exclude_chinext and exchange == Exchange.CHINEXT:
            return False

        if self._config.exclude_star and exchange == Exchange.STAR:
            return False

        return True

    def filter_stocks(self, stock_codes: list[str]) -> list[str]:
        """
        Filter a list of stock codes.

        Args:
            stock_codes: List of stock codes to filter.

        Returns:
            List of stock codes that pass the filter.
        """
        return [code for code in stock_codes if self.is_allowed(code)]

    def filter_with_reason(self, stock_codes: list[str]) -> tuple[list[str], dict[str, str]]:
        """
        Filter stocks and provide reasons for exclusion.

        Args:
            stock_codes: List of stock codes to filter.

        Returns:
            Tuple of (allowed_codes, excluded_reasons).
            excluded_reasons is a dict mapping code to exclusion reason.
        """
        allowed = []
        excluded = {}

        for code in stock_codes:
            exchange = self.get_exchange(code)

            if exchange == Exchange.UNKNOWN:
                excluded[code] = "未知股票代码"
            elif self._config.exclude_bse and exchange == Exchange.BSE:
                excluded[code] = "北交所股票"
            elif self._config.exclude_chinext and exchange == Exchange.CHINEXT:
                excluded[code] = "创业板股票"
            elif self._config.exclude_star and exchange == Exchange.STAR:
                excluded[code] = "科创板股票"
            else:
                allowed.append(code)

        return allowed, excluded

    def get_exchange_name(self, stock_code: str) -> str:
        """
        Get human-readable exchange name for a stock code.

        Args:
            stock_code: 6-digit stock code.

        Returns:
            Chinese exchange name string.
        """
        return self.get_exchange(stock_code).value

    def _normalize_code(self, stock_code: str) -> str:
        """
        Normalize stock code to 6-digit format.

        Removes suffixes like .SZ, .SH, etc.

        Args:
            stock_code: Stock code in various formats.

        Returns:
            6-digit stock code or empty string if invalid.
        """
        if not stock_code:
            return ""

        # Convert to string and strip whitespace
        code = str(stock_code).strip()

        # Remove common suffixes
        for suffix in (".SZ", ".SH", ".BJ", ".sz", ".sh", ".bj"):
            if code.endswith(suffix):
                code = code[: -len(suffix)]
                break

        # Validate: should be 6 digits
        if len(code) == 6 and code.isdigit():
            return code

        return ""

    @property
    def config(self) -> StockFilterConfig:
        """Get current filter configuration."""
        return self._config

    def update_config(self, config: StockFilterConfig) -> None:
        """
        Update filter configuration.

        Args:
            config: New filter configuration.
        """
        self._config = config


def create_default_filter() -> StockFilter:
    """
    Create a stock filter with default settings.

    Excludes BSE and ChiNext, allows STAR market.

    Returns:
        Configured StockFilter instance.
    """
    return StockFilter(
        StockFilterConfig(
            exclude_bse=True,
            exclude_chinext=True,
            exclude_star=False,
        )
    )
