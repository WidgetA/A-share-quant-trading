# Tests for Stock Filter
# Tests exchange filtering logic

import pytest

from src.strategy.filters.stock_filter import (
    Exchange,
    StockFilter,
    StockFilterConfig,
    create_default_filter,
)


class TestExchange:
    """Tests for Exchange enum."""

    def test_exchange_values(self):
        """Test exchange enum has correct Chinese names."""
        assert Exchange.SHANGHAI_MAIN.value == "上证主板"
        assert Exchange.SHENZHEN_MAIN.value == "深证主板"
        assert Exchange.SME.value == "中小板"
        assert Exchange.CHINEXT.value == "创业板"
        assert Exchange.STAR.value == "科创板"
        assert Exchange.BSE.value == "北交所"


class TestStockFilterConfig:
    """Tests for StockFilterConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = StockFilterConfig()
        assert config.exclude_bse is True
        assert config.exclude_chinext is True
        assert config.exclude_star is False


class TestStockFilter:
    """Tests for StockFilter class."""

    @pytest.fixture
    def default_filter(self):
        """Create filter with default settings."""
        return create_default_filter()

    # === Exchange Detection Tests ===

    def test_get_exchange_shanghai_main(self, default_filter):
        """Test Shanghai main board detection."""
        assert default_filter.get_exchange("600000") == Exchange.SHANGHAI_MAIN
        assert default_filter.get_exchange("600519") == Exchange.SHANGHAI_MAIN
        assert default_filter.get_exchange("601398") == Exchange.SHANGHAI_MAIN
        assert default_filter.get_exchange("603288") == Exchange.SHANGHAI_MAIN
        assert default_filter.get_exchange("605001") == Exchange.SHANGHAI_MAIN

    def test_get_exchange_shenzhen_main(self, default_filter):
        """Test Shenzhen main board detection."""
        assert default_filter.get_exchange("000001") == Exchange.SHENZHEN_MAIN
        assert default_filter.get_exchange("000002") == Exchange.SHENZHEN_MAIN
        assert default_filter.get_exchange("001289") == Exchange.SHENZHEN_MAIN

    def test_get_exchange_sme(self, default_filter):
        """Test SME board detection."""
        assert default_filter.get_exchange("002001") == Exchange.SME
        assert default_filter.get_exchange("002415") == Exchange.SME
        assert default_filter.get_exchange("002594") == Exchange.SME

    def test_get_exchange_chinext(self, default_filter):
        """Test ChiNext board detection."""
        assert default_filter.get_exchange("300001") == Exchange.CHINEXT
        assert default_filter.get_exchange("300750") == Exchange.CHINEXT
        assert default_filter.get_exchange("301001") == Exchange.CHINEXT

    def test_get_exchange_star(self, default_filter):
        """Test STAR market detection."""
        assert default_filter.get_exchange("688001") == Exchange.STAR
        assert default_filter.get_exchange("688981") == Exchange.STAR
        assert default_filter.get_exchange("689009") == Exchange.STAR

    def test_get_exchange_bse(self, default_filter):
        """Test BSE detection."""
        assert default_filter.get_exchange("830001") == Exchange.BSE
        assert default_filter.get_exchange("430001") == Exchange.BSE
        assert default_filter.get_exchange("871001") == Exchange.BSE
        assert default_filter.get_exchange("430047") == Exchange.BSE

    def test_get_exchange_unknown(self, default_filter):
        """Test unknown stock codes."""
        assert default_filter.get_exchange("999999") == Exchange.UNKNOWN
        assert default_filter.get_exchange("123456") == Exchange.UNKNOWN
        assert default_filter.get_exchange("") == Exchange.UNKNOWN
        assert default_filter.get_exchange("invalid") == Exchange.UNKNOWN

    # === Code Normalization Tests ===

    def test_normalize_code_with_suffix(self, default_filter):
        """Test code normalization removes suffixes."""
        assert default_filter.is_allowed("000001.SZ")
        assert default_filter.is_allowed("600000.SH")
        assert default_filter.is_allowed("688001.sh")

    def test_normalize_code_with_whitespace(self, default_filter):
        """Test code normalization strips whitespace."""
        assert default_filter.is_allowed(" 000001 ")
        assert default_filter.is_allowed("600000  ")

    # === Filtering Tests (Default Config) ===

    def test_is_allowed_shanghai_main(self, default_filter):
        """Test Shanghai main board stocks are allowed."""
        assert default_filter.is_allowed("600519") is True
        assert default_filter.is_allowed("601398") is True

    def test_is_allowed_shenzhen_main(self, default_filter):
        """Test Shenzhen main board stocks are allowed."""
        assert default_filter.is_allowed("000001") is True
        assert default_filter.is_allowed("000002") is True

    def test_is_allowed_sme(self, default_filter):
        """Test SME board stocks are allowed."""
        assert default_filter.is_allowed("002001") is True
        assert default_filter.is_allowed("002415") is True

    def test_is_allowed_star(self, default_filter):
        """Test STAR market stocks are allowed by default."""
        assert default_filter.is_allowed("688001") is True
        assert default_filter.is_allowed("688981") is True

    def test_not_allowed_chinext(self, default_filter):
        """Test ChiNext stocks are excluded by default."""
        assert default_filter.is_allowed("300001") is False
        assert default_filter.is_allowed("300750") is False
        assert default_filter.is_allowed("301001") is False

    def test_not_allowed_bse(self, default_filter):
        """Test BSE stocks are excluded by default."""
        assert default_filter.is_allowed("830001") is False
        assert default_filter.is_allowed("430001") is False
        assert default_filter.is_allowed("871001") is False

    def test_not_allowed_unknown(self, default_filter):
        """Test unknown codes are not allowed."""
        assert default_filter.is_allowed("999999") is False
        assert default_filter.is_allowed("") is False

    # === Filter Stocks List Tests ===

    def test_filter_stocks_mixed_list(self, default_filter):
        """Test filtering a mixed list of stocks."""
        stocks = [
            "000001",  # Shenzhen main - allowed
            "600519",  # Shanghai main - allowed
            "300001",  # ChiNext - excluded
            "688001",  # STAR - allowed
            "830001",  # BSE - excluded
            "002001",  # SME - allowed
        ]

        result = default_filter.filter_stocks(stocks)

        assert "000001" in result
        assert "600519" in result
        assert "688001" in result
        assert "002001" in result
        assert "300001" not in result
        assert "830001" not in result
        assert len(result) == 4

    def test_filter_stocks_empty_list(self, default_filter):
        """Test filtering empty list."""
        result = default_filter.filter_stocks([])
        assert result == []

    def test_filter_stocks_all_excluded(self, default_filter):
        """Test filtering when all stocks are excluded."""
        stocks = ["300001", "300002", "830001"]
        result = default_filter.filter_stocks(stocks)
        assert result == []

    # === Filter With Reason Tests ===

    def test_filter_with_reason(self, default_filter):
        """Test filter_with_reason provides exclusion reasons."""
        stocks = ["000001", "300001", "830001", "999999"]

        allowed, excluded = default_filter.filter_with_reason(stocks)

        assert allowed == ["000001"]
        assert "300001" in excluded
        assert excluded["300001"] == "创业板股票"
        assert excluded["830001"] == "北交所股票"
        assert excluded["999999"] == "未知股票代码"

    # === Custom Configuration Tests ===

    def test_custom_config_allow_chinext(self):
        """Test allowing ChiNext when configured."""
        config = StockFilterConfig(
            exclude_bse=True,
            exclude_chinext=False,  # Allow ChiNext
            exclude_star=False,
        )
        filter = StockFilter(config)

        assert filter.is_allowed("300001") is True
        assert filter.is_allowed("830001") is False  # Still exclude BSE

    def test_custom_config_exclude_star(self):
        """Test excluding STAR market when configured."""
        config = StockFilterConfig(
            exclude_bse=True,
            exclude_chinext=True,
            exclude_star=True,  # Also exclude STAR
        )
        filter = StockFilter(config)

        assert filter.is_allowed("688001") is False
        assert filter.is_allowed("600519") is True

    def test_custom_config_allow_all(self):
        """Test allowing all exchanges."""
        config = StockFilterConfig(
            exclude_bse=False,
            exclude_chinext=False,
            exclude_star=False,
        )
        filter = StockFilter(config)

        assert filter.is_allowed("000001") is True
        assert filter.is_allowed("300001") is True
        assert filter.is_allowed("688001") is True
        assert filter.is_allowed("830001") is True

    # === Update Config Tests ===

    def test_update_config(self, default_filter):
        """Test updating filter configuration."""
        # Initially ChiNext is excluded
        assert default_filter.is_allowed("300001") is False

        # Update to allow ChiNext
        new_config = StockFilterConfig(
            exclude_bse=True,
            exclude_chinext=False,
            exclude_star=False,
        )
        default_filter.update_config(new_config)

        # Now ChiNext should be allowed
        assert default_filter.is_allowed("300001") is True

    # === Get Exchange Name Tests ===

    def test_get_exchange_name(self, default_filter):
        """Test get_exchange_name returns Chinese names."""
        assert default_filter.get_exchange_name("000001") == "深证主板"
        assert default_filter.get_exchange_name("600519") == "上证主板"
        assert default_filter.get_exchange_name("300001") == "创业板"
        assert default_filter.get_exchange_name("688001") == "科创板"
        assert default_filter.get_exchange_name("830001") == "北交所"


class TestCreateDefaultFilter:
    """Tests for create_default_filter function."""

    def test_create_default_filter(self):
        """Test default filter is created correctly."""
        filter = create_default_filter()

        # Should exclude BSE and ChiNext, allow STAR
        assert filter.config.exclude_bse is True
        assert filter.config.exclude_chinext is True
        assert filter.config.exclude_star is False
