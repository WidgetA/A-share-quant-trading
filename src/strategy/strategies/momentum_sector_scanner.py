# === MODULE PURPOSE ===
# Core momentum sector scanning strategy.
# Identifies "hot" concept boards by finding stocks with >5% gain,
# then selects PE-reasonable stocks from those boards.

# === DEPENDENCIES ===
# - IFinDHttpClient: Price data (historical + real-time)
# - ConceptMapper: Stock ↔ concept board mapping via iwencai
# - FundamentalsDB: PE(TTM) data from stock_fundamentals table
# - StockFilter: Main board filtering
# - board_filter: Junk board filtering

# === DATA FLOW ===
# Step 1: iwencai "涨幅>5%主板非ST" → initial gainers
# Step 2: per-stock iwencai "所属同花顺概念" → concept boards (filtered)
# Step 3: boards with ≥2 gainers → "hot boards"
# Step 4: per-board iwencai "XX成分股" → all constituent stocks
# Step 5: constituents with open_gain>0 AND PE within board avg ±10%
# Step 6: → ScanResult → Feishu notification

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime

from src.data.clients.ifind_http_client import IFinDHttpClient
from src.data.database.fundamentals_db import FundamentalsDB
from src.data.sources.concept_mapper import ConceptMapper
from src.strategy.filters.stock_filter import StockFilter, create_main_board_only_filter

logger = logging.getLogger(__name__)


# === DATA MODELS ===


@dataclass
class PriceSnapshot:
    """Price data for a single stock at scan time."""

    stock_code: str
    stock_name: str
    open_price: float
    prev_close: float
    latest_price: float  # For live mode; equals open_price in backtest

    @property
    def open_gain_pct(self) -> float:
        """Opening gain percentage: (open - prev_close) / prev_close * 100."""
        if self.prev_close == 0:
            return 0.0
        return (self.open_price - self.prev_close) / self.prev_close * 100

    @property
    def current_gain_pct(self) -> float:
        """Current gain percentage: (latest - prev_close) / prev_close * 100."""
        if self.prev_close == 0:
            return 0.0
        return (self.latest_price - self.prev_close) / self.prev_close * 100


@dataclass
class SelectedStock:
    """A stock selected by the strategy."""

    stock_code: str
    stock_name: str
    board_name: str  # The hot board this stock was selected from
    open_gain_pct: float  # Opening gain %
    pe_ttm: float
    board_avg_pe: float  # Average PE of the board


@dataclass
class ScanResult:
    """Complete result of a momentum sector scan."""

    selected_stocks: list[SelectedStock] = field(default_factory=list)
    # Hot boards: board_name → list of initial gainer codes that triggered it
    hot_boards: dict[str, list[str]] = field(default_factory=dict)
    # Initial gainers that passed Step 1
    initial_gainers: list[str] = field(default_factory=list)
    scan_time: datetime = field(default_factory=datetime.now)

    @property
    def has_results(self) -> bool:
        return len(self.selected_stocks) > 0


# === CORE SCANNER ===


class MomentumSectorScanner:
    """
    Momentum sector scanning strategy.

    Identifies concept boards with multiple momentum stocks (>5% gain),
    then selects PE-reasonable constituents from those boards.

    This class contains the core strategy logic shared by both
    backtest and live intraday alert modes.

    Usage:
        scanner = MomentumSectorScanner(ifind_client, fundamentals_db)
        result = await scanner.scan(price_snapshots)
    """

    # Strategy parameters
    INITIAL_GAIN_THRESHOLD = 5.0  # Step 1: minimum gain % for initial scan
    MIN_STOCKS_PER_BOARD = 2  # Step 3: minimum gainers to qualify a hot board
    OPEN_GAIN_THRESHOLD = 0.0  # Step 5: minimum opening gain %
    PE_TOLERANCE = 0.10  # Step 5: ±10% of board average PE

    def __init__(
        self,
        ifind_client: IFinDHttpClient,
        fundamentals_db: FundamentalsDB,
        concept_mapper: ConceptMapper | None = None,
        stock_filter: StockFilter | None = None,
    ):
        self._ifind = ifind_client
        self._fundamentals_db = fundamentals_db
        self._concept_mapper = concept_mapper or ConceptMapper(ifind_client)
        self._stock_filter = stock_filter or create_main_board_only_filter()

    async def scan(
        self,
        price_snapshots: dict[str, PriceSnapshot],
        trade_date: date | None = None,
    ) -> ScanResult:
        """
        Run the full 5-step scan pipeline.

        Args:
            price_snapshots: Dict of stock_code → PriceSnapshot.
                For backtest: built from history_quotes.
                For live: built from real_time_quotation.
            trade_date: If provided, use history_quotes for constituent prices
                (backtest mode). If None, use real_time_quotation (live mode).

        Returns:
            ScanResult with selected stocks and metadata.
        """
        self._trade_date = trade_date
        result = ScanResult(scan_time=datetime.now())

        # Step 1: Filter initial gainers (>5%, main board, non-ST)
        gainers = await self._step1_filter_gainers(price_snapshots)
        result.initial_gainers = list(gainers.keys())
        logger.info(f"Step 1: {len(gainers)} stocks with >{self.INITIAL_GAIN_THRESHOLD}% gain")

        if not gainers:
            logger.info("No gainers found, scan complete")
            return result

        # Step 2: Reverse lookup concept boards for each gainer
        stock_boards = await self._step2_reverse_lookup(list(gainers.keys()))
        logger.info(f"Step 2: Found concept boards for {len(stock_boards)} stocks")

        # Step 3: Find hot boards (≥2 gainers in same board)
        hot_boards = self._step3_find_hot_boards(stock_boards)
        result.hot_boards = hot_boards
        logger.info(f"Step 3: {len(hot_boards)} hot boards found")

        if not hot_boards:
            logger.info("No hot boards found, scan complete")
            return result

        # Step 4: Get ALL constituent stocks of hot boards
        board_constituents = await self._step4_get_constituents(list(hot_boards.keys()))
        total_constituents = sum(len(v) for v in board_constituents.values())
        logger.info(f"Step 4: {total_constituents} total constituent stocks across hot boards")

        # Step 5: PE filter — open gain > 0 AND PE within board avg ±10%
        selected = await self._step5_pe_filter(board_constituents, price_snapshots)
        result.selected_stocks = selected
        logger.info(f"Step 5: {len(selected)} stocks selected after PE filter")

        return result

    # === STEP IMPLEMENTATIONS ===

    async def _step1_filter_gainers(
        self, price_snapshots: dict[str, PriceSnapshot]
    ) -> dict[str, PriceSnapshot]:
        """
        Step 1: Find stocks with gain > threshold, main board, non-ST.

        When price_snapshots is provided (from iwencai or pre-built),
        we just apply main board + ST filters on top.
        """
        # Filter by gain threshold
        candidates = {
            code: snap
            for code, snap in price_snapshots.items()
            if snap.current_gain_pct >= self.INITIAL_GAIN_THRESHOLD
        }

        if not candidates:
            return {}

        # Filter by main board
        main_board_codes = self._stock_filter.filter_stocks(list(candidates.keys()))
        candidates = {code: candidates[code] for code in main_board_codes}

        if not candidates:
            return {}

        # Filter out ST stocks via fundamentals DB
        non_st_codes = await self._fundamentals_db.batch_filter_st(list(candidates.keys()))
        candidates = {code: candidates[code] for code in non_st_codes}

        return candidates

    async def _step2_reverse_lookup(self, stock_codes: list[str]) -> dict[str, list[str]]:
        """
        Step 2: For each gainer, find its concept boards.

        Returns dict: stock_code → [board_name, ...]
        Junk boards already filtered by ConceptMapper.
        """
        return await self._concept_mapper.batch_get_stock_concepts(stock_codes)

    def _step3_find_hot_boards(self, stock_boards: dict[str, list[str]]) -> dict[str, list[str]]:
        """
        Step 3: Find boards that contain ≥ MIN_STOCKS_PER_BOARD gainers.

        Returns dict: board_name → [gainer_code, ...]
        """
        # Invert: board → stocks
        board_to_stocks: dict[str, list[str]] = defaultdict(list)
        for stock_code, boards in stock_boards.items():
            for board in boards:
                board_to_stocks[board].append(stock_code)

        # Keep only boards with enough gainers
        hot_boards = {
            board: stocks
            for board, stocks in board_to_stocks.items()
            if len(stocks) >= self.MIN_STOCKS_PER_BOARD
        }

        return hot_boards

    async def _step4_get_constituents(
        self, board_names: list[str]
    ) -> dict[str, list[tuple[str, str]]]:
        """
        Step 4: Get ALL constituent stocks of each hot board.

        Returns dict: board_name → [(stock_code, stock_name), ...]
        """
        return await self._concept_mapper.batch_get_board_stocks(board_names)

    async def _step5_pe_filter(
        self,
        board_constituents: dict[str, list[tuple[str, str]]],
        price_snapshots: dict[str, PriceSnapshot],
    ) -> list[SelectedStock]:
        """
        Step 5: From all constituent stocks, select those with:
        - Opening gain > 0
        - PE(TTM) within board average PE ± 10%

        For constituent stocks not in price_snapshots, we need to fetch
        their prices. This is done per-board.
        """
        # Collect all unique constituent stock codes
        all_constituent_codes: set[str] = set()
        for stocks in board_constituents.values():
            for code, _ in stocks:
                all_constituent_codes.add(code)

        # Get PE data for all constituents
        pe_data = await self._fundamentals_db.batch_get_pe(list(all_constituent_codes))

        # Get price data for constituents not already in price_snapshots
        missing_codes = [code for code in all_constituent_codes if code not in price_snapshots]
        if missing_codes:
            extra_prices = await self._fetch_constituent_prices(missing_codes)
            price_snapshots = {**price_snapshots, **extra_prices}

        # Process each board
        selected: list[SelectedStock] = []

        for board_name, stocks in board_constituents.items():
            # Collect valid PE values for board average calculation
            board_pe_values: list[float] = []
            for code, _ in stocks:
                pe = pe_data.get(code)
                if pe is not None and pe > 0:
                    board_pe_values.append(pe)

            if not board_pe_values:
                logger.debug(f"Board '{board_name}': no valid PE data, skipping")
                continue

            board_avg_pe = sum(board_pe_values) / len(board_pe_values)
            pe_lower = board_avg_pe * (1 - self.PE_TOLERANCE)
            pe_upper = board_avg_pe * (1 + self.PE_TOLERANCE)

            logger.debug(
                f"Board '{board_name}': avg PE={board_avg_pe:.2f}, "
                f"range=[{pe_lower:.2f}, {pe_upper:.2f}]"
            )

            for code, name in stocks:
                snap = price_snapshots.get(code)
                pe = pe_data.get(code)

                # Skip if no price data
                if not snap:
                    continue

                # Skip if no PE data
                if pe is None or pe <= 0:
                    continue

                # Filter: opening gain > 0
                if snap.open_gain_pct <= self.OPEN_GAIN_THRESHOLD:
                    continue

                # Filter: PE within board average ± tolerance
                if not (pe_lower <= pe <= pe_upper):
                    continue

                selected.append(
                    SelectedStock(
                        stock_code=code,
                        stock_name=name,
                        board_name=board_name,
                        open_gain_pct=snap.open_gain_pct,
                        pe_ttm=pe,
                        board_avg_pe=board_avg_pe,
                    )
                )

        # Deduplicate: a stock may appear in multiple hot boards.
        # Keep the entry with highest open_gain_pct.
        seen: dict[str, SelectedStock] = {}
        for stock in selected:
            existing = seen.get(stock.stock_code)
            if existing is None or stock.open_gain_pct > existing.open_gain_pct:
                seen[stock.stock_code] = stock

        return sorted(seen.values(), key=lambda s: s.open_gain_pct, reverse=True)

    async def _fetch_constituent_prices(self, stock_codes: list[str]) -> dict[str, PriceSnapshot]:
        """
        Fetch open and prev_close for stocks not yet in price_snapshots.

        Uses history_quotes (backtest) or real_time_quotation (live)
        depending on whether trade_date was set in scan().
        """
        if self._trade_date is not None:
            return await self._fetch_prices_historical(stock_codes, self._trade_date)
        return await self._fetch_prices_realtime(stock_codes)

    async def _fetch_prices_realtime(self, stock_codes: list[str]) -> dict[str, PriceSnapshot]:
        """Fetch prices via real_time_quotation (live mode)."""
        result: dict[str, PriceSnapshot] = {}
        batch_size = 50

        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i : i + batch_size]
            codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)

            try:
                data = await self._ifind.real_time_quotation(
                    codes=codes_str,
                    indicators="open,preClose,latest,name",
                )

                for table_wrapper in data.get("tables", []):
                    if not isinstance(table_wrapper, dict):
                        continue
                    table = table_wrapper.get("table", table_wrapper)
                    thscode = table_wrapper.get("thscode", "")
                    bare_code = thscode.split(".")[0] if thscode else ""
                    if not isinstance(table, dict) or not bare_code:
                        continue

                    open_vals = table.get("open", [])
                    prev_vals = table.get("preClose", [])
                    latest_vals = table.get("latest", [])
                    name_vals = table.get("name", [])

                    if open_vals and prev_vals and latest_vals:
                        open_price = float(open_vals[0]) if open_vals[0] else 0.0
                        prev_close = float(prev_vals[0]) if prev_vals[0] else 0.0
                        latest = float(latest_vals[0]) if latest_vals[0] else open_price
                        name = str(name_vals[0]) if name_vals else ""

                        if prev_close > 0:
                            result[bare_code] = PriceSnapshot(
                                stock_code=bare_code,
                                stock_name=name,
                                open_price=open_price,
                                prev_close=prev_close,
                                latest_price=latest,
                            )

            except Exception as e:
                logger.error(f"Error fetching realtime prices for batch: {e}")

        return result

    async def _fetch_prices_historical(
        self, stock_codes: list[str], trade_date: date
    ) -> dict[str, PriceSnapshot]:
        """Fetch prices via history_quotes (backtest mode)."""
        result: dict[str, PriceSnapshot] = {}
        batch_size = 50
        date_str = trade_date.strftime("%Y-%m-%d")

        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i : i + batch_size]
            codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)

            try:
                data = await self._ifind.history_quotes(
                    codes=codes_str,
                    indicators="open,preClose",
                    start_date=date_str,
                    end_date=date_str,
                )

                for table_entry in data.get("tables", []):
                    thscode = table_entry.get("thscode", "")
                    bare_code = thscode.split(".")[0] if thscode else ""
                    if not bare_code:
                        continue

                    tbl = table_entry.get("table", {})
                    open_vals = tbl.get("open", [])
                    prev_vals = tbl.get("preClose", [])

                    if open_vals and prev_vals:
                        open_price = float(open_vals[0])
                        prev_close = float(prev_vals[0])
                        if prev_close > 0:
                            result[bare_code] = PriceSnapshot(
                                stock_code=bare_code,
                                stock_name="",
                                open_price=open_price,
                                prev_close=prev_close,
                                latest_price=open_price,
                            )

            except Exception as e:
                logger.error(f"Error fetching historical prices for batch: {e}")

        return result
