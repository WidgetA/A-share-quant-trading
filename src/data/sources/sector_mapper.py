# === MODULE PURPOSE ===
# Maps stock codes to industry sectors using akshare data.
# Provides bidirectional mapping: stock -> sector and sector -> stocks.

# === DEPENDENCIES ===
# - akshare: For fetching industry board data from East Money

# === KEY CONCEPTS ===
# - Sector: Industry classification (e.g., 银行, 白酒, 新能源)
# - Uses East Money industry board data via akshare
# - Caches data to avoid repeated API calls

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import akshare as ak

logger = logging.getLogger(__name__)


@dataclass
class SectorInfo:
    """Information about a stock's sector."""

    stock_code: str
    stock_name: str
    sector_name: str
    sector_code: str | None = None


@dataclass
class SectorData:
    """Cached sector mapping data."""

    # Stock code -> sector name
    stock_to_sector: dict[str, str] = field(default_factory=dict)

    # Stock code -> stock name
    stock_names: dict[str, str] = field(default_factory=dict)

    # Sector name -> list of stock codes
    sector_to_stocks: dict[str, list[str]] = field(default_factory=dict)

    # Last update time
    last_updated: datetime | None = None


class SectorMapper:
    """
    Maps stocks to industry sectors using akshare data.

    Data Source:
        East Money industry boards via akshare:
        - ak.stock_board_industry_name_em() - Get all industry board names
        - ak.stock_board_industry_cons_em() - Get stocks in each board

    Usage:
        mapper = SectorMapper()
        await mapper.load_sector_data()

        # Get sector for a stock
        sector = mapper.get_sector("000001")  # -> "银行"

        # Get stocks in a sector
        stocks = mapper.get_sector_stocks("银行", limit=10)

        # Find sectors by keywords
        sectors = mapper.get_related_sectors(["新能源", "电池"])

    Caching:
        Data is cached after loading. Use refresh_interval to control
        how often data is refreshed (default: 24 hours).
    """

    def __init__(
        self,
        refresh_interval: timedelta = timedelta(hours=24),
        max_workers: int = 4,
    ):
        """
        Initialize sector mapper.

        Args:
            refresh_interval: How often to refresh sector data.
            max_workers: Max threads for parallel data fetching.
        """
        self._refresh_interval = refresh_interval
        self._max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._data = SectorData()
        self._loading = False
        self._lock = asyncio.Lock()

    async def load_sector_data(self, force: bool = False) -> None:
        """
        Load sector mapping data from akshare.

        Args:
            force: Force reload even if data is fresh.
        """
        async with self._lock:
            # Check if we need to reload
            if not force and self._data.last_updated:
                age = datetime.now() - self._data.last_updated
                if age < self._refresh_interval:
                    logger.debug(f"Sector data is fresh (age: {age}), skipping reload")
                    return

            if self._loading:
                logger.debug("Sector data is already being loaded")
                return

            self._loading = True

        try:
            logger.info("Loading sector mapping data from akshare...")

            # Fetch industry board list
            loop = asyncio.get_event_loop()
            boards = await loop.run_in_executor(
                self._executor,
                self._fetch_industry_boards,
            )

            if not boards:
                logger.warning("No industry boards fetched")
                return

            # Fetch stocks for each board (parallel)
            await self._fetch_all_board_stocks(boards)

            self._data.last_updated = datetime.now()
            logger.info(
                f"Sector data loaded: {len(self._data.stock_to_sector)} stocks, "
                f"{len(self._data.sector_to_stocks)} sectors"
            )

        except Exception as e:
            logger.error(f"Failed to load sector data: {e}")
            raise

        finally:
            self._loading = False

    def _fetch_industry_boards(self) -> list[dict[str, Any]]:
        """Fetch list of industry boards (sync, runs in thread pool)."""
        try:
            df = ak.stock_board_industry_name_em()
            if df is None or df.empty:
                return []

            boards = []
            for _, row in df.iterrows():
                boards.append(
                    {
                        "name": str(row.get("板块名称", "")),
                        "code": str(row.get("板块代码", "")),
                    }
                )
            return boards

        except Exception as e:
            logger.error(f"Failed to fetch industry boards: {e}")
            return []

    async def _fetch_all_board_stocks(self, boards: list[dict[str, Any]]) -> None:
        """Fetch stocks for all boards in parallel."""
        # Reset data
        self._data.stock_to_sector = {}
        self._data.stock_names = {}
        self._data.sector_to_stocks = {}

        # Create tasks for parallel fetching
        semaphore = asyncio.Semaphore(self._max_workers)

        async def fetch_board_stocks(board: dict[str, Any]) -> None:
            async with semaphore:
                await self._fetch_board_stocks(board)

        tasks = [fetch_board_stocks(board) for board in boards]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_board_stocks(self, board: dict[str, Any]) -> None:
        """Fetch stocks for a single board."""
        board_name = board["name"]

        try:
            loop = asyncio.get_event_loop()
            stocks = await loop.run_in_executor(
                self._executor,
                lambda: self._fetch_board_stocks_sync(board_name),
            )

            if stocks:
                self._data.sector_to_stocks[board_name] = []
                for stock in stocks:
                    code = stock["code"]
                    name = stock["name"]

                    # Store mappings
                    self._data.stock_to_sector[code] = board_name
                    self._data.stock_names[code] = name
                    self._data.sector_to_stocks[board_name].append(code)

        except Exception as e:
            logger.warning(f"Failed to fetch stocks for board {board_name}: {e}")

    def _fetch_board_stocks_sync(self, board_name: str) -> list[dict[str, str]]:
        """Fetch stocks for a board (sync, runs in thread pool)."""
        try:
            df = ak.stock_board_industry_cons_em(symbol=board_name)
            if df is None or df.empty:
                return []

            stocks = []
            for _, row in df.iterrows():
                code = str(row.get("代码", ""))
                name = str(row.get("名称", ""))
                if code and len(code) == 6:
                    stocks.append({"code": code, "name": name})

            return stocks

        except Exception as e:
            logger.debug(f"Failed to fetch stocks for {board_name}: {e}")
            return []

    def get_sector(self, stock_code: str) -> str | None:
        """
        Get the sector name for a stock.

        Args:
            stock_code: 6-digit stock code.

        Returns:
            Sector name or None if not found.
        """
        # Normalize code
        code = self._normalize_code(stock_code)
        return self._data.stock_to_sector.get(code)

    def get_stock_name(self, stock_code: str) -> str | None:
        """
        Get the stock name for a code.

        Args:
            stock_code: 6-digit stock code.

        Returns:
            Stock name or None if not found.
        """
        code = self._normalize_code(stock_code)
        return self._data.stock_names.get(code)

    def get_sector_info(self, stock_code: str) -> SectorInfo | None:
        """
        Get complete sector information for a stock.

        Args:
            stock_code: 6-digit stock code.

        Returns:
            SectorInfo or None if not found.
        """
        code = self._normalize_code(stock_code)
        sector = self._data.stock_to_sector.get(code)
        name = self._data.stock_names.get(code)

        if not sector:
            return None

        return SectorInfo(
            stock_code=code,
            stock_name=name or "",
            sector_name=sector,
        )

    def get_sector_stocks(
        self,
        sector_name: str,
        limit: int = 10,
    ) -> list[str]:
        """
        Get stocks in a sector.

        Args:
            sector_name: Sector/industry name.
            limit: Maximum number of stocks to return.

        Returns:
            List of stock codes in the sector.
        """
        stocks = self._data.sector_to_stocks.get(sector_name, [])
        return stocks[:limit]

    def get_sector_stocks_with_names(
        self,
        sector_name: str,
        limit: int = 10,
    ) -> list[tuple[str, str]]:
        """
        Get stocks in a sector with their names.

        Args:
            sector_name: Sector/industry name.
            limit: Maximum number of stocks to return.

        Returns:
            List of (code, name) tuples.
        """
        stocks = self.get_sector_stocks(sector_name, limit)
        return [(code, self._data.stock_names.get(code, "")) for code in stocks]

    def get_related_sectors(self, keywords: list[str]) -> list[str]:
        """
        Find sectors matching keywords.

        Args:
            keywords: List of keywords to search for.

        Returns:
            List of matching sector names.
        """
        matching = []
        for sector_name in self._data.sector_to_stocks.keys():
            for keyword in keywords:
                if keyword in sector_name:
                    matching.append(sector_name)
                    break
        return matching

    def get_all_sectors(self) -> list[str]:
        """
        Get list of all sector names.

        Returns:
            List of all sector names.
        """
        return list(self._data.sector_to_stocks.keys())

    def get_stats(self) -> dict[str, Any]:
        """
        Get statistics about loaded data.

        Returns:
            Dictionary with stats.
        """
        return {
            "total_stocks": len(self._data.stock_to_sector),
            "total_sectors": len(self._data.sector_to_stocks),
            "last_updated": (
                self._data.last_updated.isoformat() if self._data.last_updated else None
            ),
        }

    def _normalize_code(self, stock_code: str) -> str:
        """Normalize stock code to 6-digit format."""
        if not stock_code:
            return ""

        code = str(stock_code).strip()

        # Remove common suffixes
        for suffix in (".SZ", ".SH", ".BJ", ".sz", ".sh", ".bj"):
            if code.endswith(suffix):
                code = code[: -len(suffix)]
                break

        return code

    @property
    def is_loaded(self) -> bool:
        """Check if sector data is loaded."""
        return self._data.last_updated is not None

    async def close(self) -> None:
        """Close resources."""
        self._executor.shutdown(wait=False)
