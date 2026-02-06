# === MODULE PURPOSE ===
# Tests for IFinDLimitUpSource.
# Verifies connectivity, data format, database operations, and error handling.

import pytest

from src.data.database.limit_up_db import LimitUpDatabase, LimitUpDatabaseConfig
from src.data.models.limit_up import LimitUpStock
from src.data.sources.ifind_limit_up import IFinDLimitUpSource


class TestLimitUpStockModel:
    """Tests for LimitUpStock data model."""

    def test_model_creation(self):
        """Test basic model creation."""
        stock = LimitUpStock(
            trade_date="2026-01-27",
            stock_code="000001.SZ",
            stock_name="平安银行",
            limit_up_price=15.50,
            limit_up_time="09:35:00",
        )
        assert stock.trade_date == "2026-01-27"
        assert stock.stock_code == "000001.SZ"
        assert stock.stock_name == "平安银行"
        assert stock.limit_up_price == 15.50
        assert stock.limit_up_time == "09:35:00"

    def test_composite_id_generation(self):
        """Test that composite ID is auto-generated."""
        stock = LimitUpStock(
            trade_date="2026-01-27",
            stock_code="000001.SZ",
            stock_name="平安银行",
            limit_up_price=15.50,
            limit_up_time="09:35:00",
        )
        assert stock.id == "2026-01-27_000001.SZ"

    def test_to_dict(self):
        """Test conversion to dictionary."""
        stock = LimitUpStock(
            trade_date="2026-01-27",
            stock_code="000001.SZ",
            stock_name="平安银行",
            limit_up_price=15.50,
            limit_up_time="09:35:00",
            open_count=2,
            reason="AI概念",
            industry="银行",
        )
        data = stock.to_dict()

        assert data["id"] == "2026-01-27_000001.SZ"
        assert data["trade_date"] == "2026-01-27"
        assert data["stock_code"] == "000001.SZ"
        assert data["stock_name"] == "平安银行"
        assert data["limit_up_price"] == 15.50
        assert data["open_count"] == 2
        assert data["reason"] == "AI概念"

    def test_from_dict(self):
        """Test creation from dictionary."""
        data = {
            "id": "2026-01-27_000001.SZ",
            "trade_date": "2026-01-27",
            "stock_code": "000001.SZ",
            "stock_name": "平安银行",
            "limit_up_price": 15.50,
            "limit_up_time": "09:35:00",
            "open_count": 0,
            "turnover_rate": 5.5,
            "amount": 1000000000,
            "created_at": "2026-01-27T16:00:00",
            "updated_at": "2026-01-27T16:00:00",
        }
        stock = LimitUpStock.from_dict(data)

        assert stock.stock_code == "000001.SZ"
        assert stock.turnover_rate == 5.5
        assert stock.amount == 1000000000


class TestLimitUpDatabaseConfig:
    """Tests for LimitUpDatabaseConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = LimitUpDatabaseConfig()
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "messages"
        assert config.schema == "trading"

    def test_custom_config(self):
        """Test custom configuration values."""
        config = LimitUpDatabaseConfig(
            host="db.example.com",
            port=5433,
            database="custom_db",
            schema="custom_schema",
        )
        assert config.host == "db.example.com"
        assert config.port == 5433
        assert config.database == "custom_db"
        assert config.schema == "custom_schema"


class TestLimitUpDatabase:
    """Tests for LimitUpDatabase.

    Note: Database connection tests are marked as 'live' and require
    a real PostgreSQL database to run.
    """

    @pytest.fixture
    def mock_config(self):
        """Create a test database configuration."""
        return LimitUpDatabaseConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            schema="test_schema",
        )

    def test_database_initialization(self, mock_config):
        """Test database object initialization."""
        db = LimitUpDatabase(mock_config)
        assert db._config == mock_config
        assert db._pool is None
        assert db._is_connected is False
        assert db._schema == "test_schema"

    @pytest.mark.live
    @pytest.mark.asyncio
    async def test_database_connect_and_close(self, mock_config):
        """Test database connection lifecycle (requires live database)."""
        db = LimitUpDatabase(mock_config)
        await db.connect()
        assert db.is_connected
        await db.close()
        assert not db.is_connected


class TestIFinDLimitUpSource:
    """Tests for IFinDLimitUpSource."""

    @pytest.fixture
    def mock_db_config(self):
        """Create a test database configuration."""
        return LimitUpDatabaseConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
            schema="test_schema",
        )

    def test_source_initialization(self, mock_db_config):
        """Test source initialization."""
        source = IFinDLimitUpSource(db_config=mock_db_config)
        assert source._database is None
        assert source._http_client is None

    def test_format_time_hhmmss(self, mock_db_config):
        """Test time formatting from HHMMSS."""
        source = IFinDLimitUpSource(db_config=mock_db_config)
        assert source._format_time("093500") == "09:35:00"

    def test_format_time_hhmm(self, mock_db_config):
        """Test time formatting from HHMM."""
        source = IFinDLimitUpSource(db_config=mock_db_config)
        assert source._format_time("0935") == "09:35:00"

    def test_format_time_already_formatted(self, mock_db_config):
        """Test already formatted time."""
        source = IFinDLimitUpSource(db_config=mock_db_config)
        assert source._format_time("09:35:00") == "09:35:00"

    def test_format_time_empty(self, mock_db_config):
        """Test empty time value."""
        source = IFinDLimitUpSource(db_config=mock_db_config)
        assert source._format_time("") == ""
        assert source._format_time("--") == ""
        assert source._format_time(None) == ""

    def test_safe_get(self, mock_db_config):
        """Test safe_get helper."""
        source = IFinDLimitUpSource(db_config=mock_db_config)
        test_list = ["a", "b", None, "--", ""]

        assert source._safe_get(test_list, 0) == "a"
        assert source._safe_get(test_list, 1) == "b"
        assert source._safe_get(test_list, 2, "default") == "default"
        assert source._safe_get(test_list, 3, "default") == "default"
        assert source._safe_get(test_list, 4, "default") == "default"
        assert source._safe_get(test_list, 10, "default") == "default"
        assert source._safe_get([], 0, "default") == "default"

    def test_safe_float(self, mock_db_config):
        """Test safe_float helper."""
        source = IFinDLimitUpSource(db_config=mock_db_config)
        test_list = [1.5, "2.5", "invalid", None]

        assert source._safe_float(test_list, 0) == 1.5
        assert source._safe_float(test_list, 1) == 2.5
        assert source._safe_float(test_list, 2) is None
        assert source._safe_float(test_list, 3) is None

    def test_safe_int(self, mock_db_config):
        """Test safe_int helper."""
        source = IFinDLimitUpSource(db_config=mock_db_config)
        test_list = [1, "2", "invalid", None]

        assert source._safe_int(test_list, 0) == 1
        assert source._safe_int(test_list, 1) == 2
        assert source._safe_int(test_list, 2, 0) == 0
        assert source._safe_int(test_list, 3, 0) == 0

    def test_process_raw_data_empty(self, mock_db_config):
        """Test processing empty data."""
        source = IFinDLimitUpSource(db_config=mock_db_config)

        assert source._process_raw_data(None, "2026-01-27") == []
        assert source._process_raw_data({}, "2026-01-27") == []
        assert source._process_raw_data({"tables": []}, "2026-01-27") == []

    def test_process_raw_data_with_tables(self, mock_db_config):
        """Test processing data with tables."""
        source = IFinDLimitUpSource(db_config=mock_db_config)

        raw_data = {
            "tables": [
                {
                    "table": {
                        "股票代码": ["000001.SZ", "000002.SZ"],
                        "股票简称": ["平安银行", "万科A"],
                        "首次涨停时间": ["09:35:00", "10:15:00"],
                        "涨停开板次数": [0, 1],
                        "换手率": [5.5, 3.2],
                        "成交额": [1000000000, 500000000],
                        "涨停原因类型": ["AI概念", "地产复苏"],
                        "所属行业": ["银行", "房地产"],
                    }
                }
            ]
        }

        stocks = source._process_raw_data(raw_data, "2026-01-27")

        assert len(stocks) == 2
        assert stocks[0].stock_code == "000001.SZ"
        assert stocks[0].stock_name == "平安银行"
        assert stocks[0].open_count == 0
        assert stocks[1].stock_code == "000002.SZ"
        assert stocks[1].open_count == 1

    @pytest.mark.live
    @pytest.mark.asyncio
    async def test_start_and_stop(self, mock_db_config):
        """Test source start and stop lifecycle (requires live database)."""
        source = IFinDLimitUpSource(db_config=mock_db_config)
        await source.start()
        assert source._database is not None
        await source.stop()
        assert source._database is None
