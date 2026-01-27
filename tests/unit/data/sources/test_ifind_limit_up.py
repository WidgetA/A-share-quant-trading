# === MODULE PURPOSE ===
# Tests for IFinDLimitUpSource.
# Verifies connectivity, data format, database operations, and error handling.

import pytest
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from src.data.models.limit_up import LimitUpStock
from src.data.sources.ifind_limit_up import IFinDLimitUpSource
from src.data.database.limit_up_db import LimitUpDatabase


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


class TestLimitUpDatabase:
    """Tests for LimitUpDatabase."""

    @pytest.fixture
    def temp_db_path(self, tmp_path):
        """Create a temporary database path."""
        return tmp_path / "test_limit_up.db"

    @pytest.mark.asyncio
    async def test_database_connect_and_close(self, temp_db_path):
        """Test database connection lifecycle."""
        db = LimitUpDatabase(temp_db_path)

        await db.connect()
        assert db._connection is not None
        assert temp_db_path.exists()

        await db.close()
        assert db._connection is None

    @pytest.mark.asyncio
    async def test_save_and_query(self, temp_db_path):
        """Test saving and querying stocks."""
        async with LimitUpDatabase(temp_db_path) as db:
            stock = LimitUpStock(
                trade_date="2026-01-27",
                stock_code="000001.SZ",
                stock_name="平安银行",
                limit_up_price=15.50,
                limit_up_time="09:35:00",
            )

            await db.save(stock)

            results = await db.query_by_date("2026-01-27")
            assert len(results) == 1
            assert results[0].stock_code == "000001.SZ"

    @pytest.mark.asyncio
    async def test_save_batch(self, temp_db_path):
        """Test batch saving."""
        async with LimitUpDatabase(temp_db_path) as db:
            stocks = [
                LimitUpStock(
                    trade_date="2026-01-27",
                    stock_code=f"00000{i}.SZ",
                    stock_name=f"测试股票{i}",
                    limit_up_price=10.0 + i,
                    limit_up_time=f"09:3{i}:00",
                )
                for i in range(5)
            ]

            count = await db.save_batch(stocks)
            assert count == 5

            results = await db.query_by_date("2026-01-27")
            assert len(results) == 5

    @pytest.mark.asyncio
    async def test_upsert_behavior(self, temp_db_path):
        """Test that re-saving updates existing record."""
        async with LimitUpDatabase(temp_db_path) as db:
            stock1 = LimitUpStock(
                trade_date="2026-01-27",
                stock_code="000001.SZ",
                stock_name="平安银行",
                limit_up_price=15.50,
                limit_up_time="09:35:00",
                open_count=0,
            )
            await db.save(stock1)

            # Save again with updated open_count
            stock2 = LimitUpStock(
                trade_date="2026-01-27",
                stock_code="000001.SZ",
                stock_name="平安银行",
                limit_up_price=15.50,
                limit_up_time="09:35:00",
                open_count=2,
            )
            await db.save(stock2)

            results = await db.query_by_date("2026-01-27")
            assert len(results) == 1
            assert results[0].open_count == 2

    @pytest.mark.asyncio
    async def test_query_with_filters(self, temp_db_path):
        """Test query with various filters."""
        async with LimitUpDatabase(temp_db_path) as db:
            stocks = [
                LimitUpStock(
                    trade_date="2026-01-27",
                    stock_code="000001.SZ",
                    stock_name="平安银行",
                    limit_up_price=15.50,
                    limit_up_time="09:35:00",
                    industry="银行",
                    reason="AI概念",
                    open_count=0,
                ),
                LimitUpStock(
                    trade_date="2026-01-27",
                    stock_code="600519.SH",
                    stock_name="贵州茅台",
                    limit_up_price=2000.0,
                    limit_up_time="10:00:00",
                    industry="白酒",
                    reason="消费升级",
                    open_count=3,
                ),
            ]
            await db.save_batch(stocks)

            # Query by industry
            results = await db.query(industry="银行")
            assert len(results) == 1
            assert results[0].stock_code == "000001.SZ"

            # Query by max_open_count (sealed stocks)
            sealed = await db.query(max_open_count=0)
            assert len(sealed) == 1

    @pytest.mark.asyncio
    async def test_count_by_date(self, temp_db_path):
        """Test counting stocks by date."""
        async with LimitUpDatabase(temp_db_path) as db:
            stocks = [
                LimitUpStock(
                    trade_date="2026-01-27",
                    stock_code=f"00000{i}.SZ",
                    stock_name=f"测试股票{i}",
                    limit_up_price=10.0,
                    limit_up_time="09:30:00",
                )
                for i in range(10)
            ]
            await db.save_batch(stocks)

            count = await db.count_by_date("2026-01-27")
            assert count == 10

    @pytest.mark.asyncio
    async def test_exists(self, temp_db_path):
        """Test checking existence of a record."""
        async with LimitUpDatabase(temp_db_path) as db:
            stock = LimitUpStock(
                trade_date="2026-01-27",
                stock_code="000001.SZ",
                stock_name="平安银行",
                limit_up_price=15.50,
                limit_up_time="09:35:00",
            )
            await db.save(stock)

            assert await db.exists("2026-01-27_000001.SZ") is True
            assert await db.exists("2026-01-27_999999.SZ") is False


class TestIFinDLimitUpSource:
    """Tests for IFinDLimitUpSource."""

    @pytest.fixture
    def temp_db_path(self, tmp_path):
        """Create a temporary database path."""
        return tmp_path / "test_limit_up.db"

    def test_source_initialization(self, temp_db_path):
        """Test source initialization."""
        source = IFinDLimitUpSource(db_path=temp_db_path)
        assert source.db_path == temp_db_path
        assert source._logged_in is False

    def test_format_time_hhmmss(self):
        """Test time formatting from HHMMSS."""
        source = IFinDLimitUpSource()
        assert source._format_time("093500") == "09:35:00"
        assert source._format_time("143000") == "14:30:00"

    def test_format_time_hhmm(self):
        """Test time formatting from HHMM."""
        source = IFinDLimitUpSource()
        assert source._format_time("0935") == "09:35:00"

    def test_format_time_already_formatted(self):
        """Test time that's already in HH:MM:SS format."""
        source = IFinDLimitUpSource()
        assert source._format_time("09:35:00") == "09:35:00"

    def test_format_time_empty(self):
        """Test empty time values."""
        source = IFinDLimitUpSource()
        assert source._format_time("") == ""
        assert source._format_time("--") == ""
        assert source._format_time(None) == ""

    def test_safe_get(self):
        """Test safe list access."""
        source = IFinDLimitUpSource()
        lst = ["a", "b", "c"]

        assert source._safe_get(lst, 0) == "a"
        assert source._safe_get(lst, 10) is None
        assert source._safe_get(lst, 10, "default") == "default"
        assert source._safe_get([], 0) is None
        assert source._safe_get(None, 0) is None

    def test_safe_float(self):
        """Test safe float conversion."""
        source = IFinDLimitUpSource()
        lst = [10.5, "20.5", "--", None]

        assert source._safe_float(lst, 0) == 10.5
        assert source._safe_float(lst, 1) == 20.5
        assert source._safe_float(lst, 2) is None
        assert source._safe_float(lst, 3) is None
        assert source._safe_float(lst, 10) is None

    def test_safe_int(self):
        """Test safe int conversion."""
        source = IFinDLimitUpSource()
        lst = [10, "20", "--", None]

        assert source._safe_int(lst, 0) == 10
        assert source._safe_int(lst, 1) == 20
        assert source._safe_int(lst, 2) == 0
        assert source._safe_int(lst, 3) == 0

    def test_process_raw_data_empty(self):
        """Test processing empty raw data."""
        source = IFinDLimitUpSource()
        stocks = source._process_raw_data({}, "2026-01-27")
        assert stocks == []

    def test_process_raw_data_with_tables(self):
        """Test processing raw data with iwencai tables format."""
        source = IFinDLimitUpSource()
        # iwencai returns data in 'table' wrapper with Chinese column names
        raw_data = {
            "tables": [
                {
                    "table": {
                        "股票代码": ["000001.SZ", "600519.SH"],
                        "股票简称": ["平安银行", "贵州茅台"],
                        "首次涨停时间[20260127]": ["2026-01-27 09:35:00", "2026-01-27 10:00:00"],
                        "涨停开板次数[20260127]": ["0", "2"],
                        "换手率[20260127]": ["5.5", "3.2"],
                        "成交额[20260127]": ["1000000000", "500000000"],
                        "涨停原因类型[20260127]": ["AI概念", "消费升级"],
                        "所属行业": ["银行", "白酒"],
                    }
                }
            ]
        }

        stocks = source._process_raw_data(raw_data, "2026-01-27")

        assert len(stocks) == 2
        assert stocks[0].stock_code == "000001.SZ"
        assert stocks[0].stock_name == "平安银行"
        assert stocks[0].limit_up_time == "09:35:00"
        assert stocks[0].open_count == 0
        assert stocks[0].reason == "AI概念"

        assert stocks[1].stock_code == "600519.SH"
        assert stocks[1].open_count == 2
        assert stocks[1].limit_up_time == "10:00:00"

    @pytest.mark.asyncio
    async def test_start_and_stop(self, temp_db_path):
        """Test source lifecycle."""
        source = IFinDLimitUpSource(db_path=temp_db_path)

        await source.start()
        assert source._database is not None

        await source.stop()
        assert source._database is None

    @pytest.mark.asyncio
    async def test_fetch_and_save_with_mock(self, temp_db_path):
        """Test fetch and save with mocked iFinD."""
        source = IFinDLimitUpSource(db_path=temp_db_path)

        mock_stocks = [
            LimitUpStock(
                trade_date="2026-01-27",
                stock_code="000001.SZ",
                stock_name="平安银行",
                limit_up_price=15.50,
                limit_up_time="09:35:00",
            ),
            LimitUpStock(
                trade_date="2026-01-27",
                stock_code="600519.SH",
                stock_name="贵州茅台",
                limit_up_price=2000.0,
                limit_up_time="10:00:00",
            ),
        ]

        await source.start()
        try:
            # Mock the fetch method
            source.fetch_limit_up_stocks = AsyncMock(return_value=mock_stocks)

            count = await source.fetch_and_save("2026-01-27")
            assert count == 2

            # Verify saved to database
            results = await source.query_by_date("2026-01-27")
            assert len(results) == 2
        finally:
            await source.stop()

    @pytest.mark.asyncio
    async def test_get_statistics(self, temp_db_path):
        """Test statistics calculation."""
        source = IFinDLimitUpSource(db_path=temp_db_path)

        await source.start()
        try:
            # Add test data for multiple days
            for day in ["2026-01-24", "2026-01-27"]:
                stocks = [
                    LimitUpStock(
                        trade_date=day,
                        stock_code=f"00000{i}.SZ",
                        stock_name=f"测试股票{i}",
                        limit_up_price=10.0,
                        limit_up_time="09:30:00",
                    )
                    for i in range(5 if day == "2026-01-24" else 10)
                ]
                await source._database.save_batch(stocks)

            stats = await source.get_statistics("2026-01-24", "2026-01-27")

            assert stats["trading_days"] == 2
            assert stats["total_stocks"] == 15
            assert stats["avg_per_day"] == 7.5
            assert stats["max_per_day"] == 10
            assert stats["min_per_day"] == 5
        finally:
            await source.stop()

    @pytest.mark.asyncio
    async def test_error_handling_login_failure(self, temp_db_path):
        """Test error handling when iFinD login fails."""
        source = IFinDLimitUpSource(db_path=temp_db_path)

        await source.start()
        try:
            # Mock login to fail
            with patch.object(source, "_ifind_login", return_value=False):
                with pytest.raises(RuntimeError, match="Failed to login"):
                    await source.fetch_limit_up_stocks("2026-01-27")
        finally:
            await source.stop()

    @pytest.mark.live
    @pytest.mark.asyncio
    async def test_live_connectivity(self, temp_db_path):
        """
        Test actual iFinD API connectivity.

        This test requires:
        - iFinD SDK installed
        - Valid credentials in config/secrets.yaml
        - Network access to iFinD servers

        Run with: pytest -m live
        """
        source = IFinDLimitUpSource(db_path=temp_db_path)

        try:
            await source.start()

            # Fetch today's limit-up stocks
            stocks = await source.fetch_limit_up_stocks()

            # Should return a list (may be empty if no limit-up stocks or non-trading day)
            assert isinstance(stocks, list)

            if stocks:
                # Verify data structure
                stock = stocks[0]
                assert stock.trade_date
                assert stock.stock_code
                assert stock.stock_name
                assert stock.limit_up_price > 0
                assert stock.limit_up_time

                # Verify stock code format
                assert "." in stock.stock_code  # e.g., "000001.SZ"

                print(f"Fetched {len(stocks)} limit-up stocks")
                print(f"First stock: {stock.stock_name} ({stock.stock_code})")
        finally:
            await source.stop()

    @pytest.mark.live
    @pytest.mark.asyncio
    async def test_live_fetch_and_save(self, temp_db_path):
        """
        Test actual fetch and save to database.

        Run with: pytest -m live
        """
        source = IFinDLimitUpSource(db_path=temp_db_path)

        try:
            await source.start()

            count = await source.fetch_and_save()
            print(f"Saved {count} limit-up stocks to database")

            # Query back from database
            today = datetime.now().strftime("%Y-%m-%d")
            results = await source.query_by_date(today)

            assert len(results) == count

            if results:
                print(f"First result: {results[0].stock_name}")
        finally:
            await source.stop()
