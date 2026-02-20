# === MODULE PURPOSE ===
# Core momentum sector scanning strategy.
# Identifies "hot" concept boards by finding stocks with 9:40 gain from open >0.56%,
# then selects constituent gainers from those boards.

# === DEPENDENCIES ===
# - IFinDHttpClient: Price data (historical + real-time)
# - ConceptMapper: Stock ↔ concept board mapping via iwencai
# - FundamentalsDB: ST detection from stock_fundamentals table
# - StockFilter: Main board filtering
# - board_filter: Junk board filtering

# === DATA FLOW ===
# Pre-filter: iwencai "涨幅>-0.5%主板非ST" → broad candidate pool
# Step 1: 9:40 gain from open > 0.56% → initial gainers
# Step 2: per-stock iwencai "所属同花顺概念" → concept boards (filtered)
# Step 3: boards with ≥2 gainers → "hot boards"
# Step 4: per-board iwencai "XX成分股" → all constituent stocks
# Step 5: constituents with 9:40 gain from open > 0.56% (main board only)
# Step 5.5: momentum quality filter (declining trend + low turnover amp → fake breakout)
# Step 5.6: reversal factor filter (early fade from 9:40 high → 冲高回落 risk)
# Step 6: recommend — filter consecutive-up ≥2d, score by Z(开盘涨幅)-Z(营收增长率),
#          check #1 for negative news via Tavily+LLM, fall back to #2 if negative
# Step 7: → ScanResult → Feishu notification

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING

from src.data.clients.ifind_http_client import IFinDHttpClient
from src.data.database.fundamentals_db import FundamentalsDB
from src.data.sources.concept_mapper import ConceptMapper
from src.strategy.filters.momentum_quality_filter import (
    MomentumQualityConfig,
    MomentumQualityFilter,
    QualityAssessment,
)
from src.strategy.filters.reversal_factor_filter import (
    ReversalFactorConfig,
    ReversalFactorFilter,
)
from src.strategy.filters.stock_filter import StockFilter, create_main_board_only_filter

if TYPE_CHECKING:
    from src.strategy.analyzers.negative_news_checker import NegativeNewsChecker

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
    early_volume: float = 0.0  # Cumulative volume from 9:30 to scan time (~9:40)
    high_price: float = 0.0  # Intraday high up to scan time (for reversal filter)
    low_price: float = 0.0  # Intraday low up to scan time (for reversal filter)

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

    @property
    def gain_from_open_pct(self) -> float:
        """Gain from open price: (latest - open) / open * 100."""
        if self.open_price == 0:
            return 0.0
        return (self.latest_price - self.open_price) / self.open_price * 100


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
class RecommendedStock:
    """The top pick from largest board, scored by Z(开盘涨幅) - Z(营收增长率)."""

    stock_code: str
    stock_name: str
    board_name: str  # Which board it was recommended from
    board_stock_count: int  # How many selected stocks in that board
    growth_rate: float  # 同比季度收入增长率 (%)
    open_gain_pct: float
    pe_ttm: float
    board_avg_pe: float
    open_price: float = 0.0  # Raw open price
    prev_close: float = 0.0  # Previous close price
    latest_price: float = 0.0  # 9:40 price (buy price for range backtest)
    news_check_passed: bool | None = None  # None=not checked, True=clean, False=had negative
    news_check_detail: str = ""  # LLM reasoning if checked


@dataclass
class ScanResult:
    """Complete result of a momentum sector scan."""

    selected_stocks: list[SelectedStock] = field(default_factory=list)
    # Hot boards: board_name → list of initial gainer codes that triggered it
    hot_boards: dict[str, list[str]] = field(default_factory=dict)
    # Initial gainers that passed Step 1
    initial_gainers: list[str] = field(default_factory=list)
    scan_time: datetime = field(default_factory=datetime.now)
    # Step 6: recommended stock (scored by 开盘涨幅↑ + 营收增长率↓)
    recommended_stock: RecommendedStock | None = None
    # Price snapshots for selected stocks (for backfill/export use)
    all_snapshots: dict[str, PriceSnapshot] = field(default_factory=dict)

    @property
    def has_results(self) -> bool:
        return len(self.selected_stocks) > 0


# === CORE SCANNER ===


class MomentumSectorScanner:
    """
    Momentum sector scanning strategy.

    Pre-filter: stocks with opening gain > -0.5% (broad pool).
    Step 1: keep stocks where 9:40 gain from open > 0.56%.
    Then identifies concept boards with multiple such stocks,
    and selects constituent gainers from those boards.

    This class contains the core strategy logic shared by both
    backtest and live intraday alert modes.

    Usage:
        scanner = MomentumSectorScanner(ifind_client, fundamentals_db)
        result = await scanner.scan(price_snapshots)
    """

    # Strategy parameters
    GAIN_FROM_OPEN_THRESHOLD = 0.56  # Step 1 & 5: minimum (9:40 - open) / open %
    MIN_STOCKS_PER_BOARD = 2  # Step 3: minimum gainers to qualify a hot board

    def __init__(
        self,
        ifind_client: IFinDHttpClient,
        fundamentals_db: FundamentalsDB,
        concept_mapper: ConceptMapper | None = None,
        stock_filter: StockFilter | None = None,
        momentum_quality_config: MomentumQualityConfig | None = None,
        reversal_factor_config: ReversalFactorConfig | None = None,
        negative_news_checker: NegativeNewsChecker | None = None,
    ):
        self._ifind = ifind_client
        self._fundamentals_db = fundamentals_db
        self._concept_mapper = concept_mapper or ConceptMapper(ifind_client)
        self._stock_filter = stock_filter or create_main_board_only_filter()
        self._quality_filter = MomentumQualityFilter(ifind_client, momentum_quality_config)
        self._reversal_filter = ReversalFactorFilter(reversal_factor_config)
        self._news_checker = negative_news_checker

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

        # Step 1: Filter initial gainers (9:40 vs open > 0.56%, main board, non-ST)
        gainers = await self._step1_filter_gainers(price_snapshots)
        result.initial_gainers = list(gainers.keys())
        logger.info(
            f"Step 1: {len(gainers)} stocks with gain from open >{self.GAIN_FROM_OPEN_THRESHOLD}%"
        )

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

        # Step 5: Select constituents with gain from open > threshold
        selected, all_snapshots = await self._step5_select_constituents(
            board_constituents, price_snapshots
        )
        result.selected_stocks = selected
        result.all_snapshots = all_snapshots
        logger.info(f"Step 5: {len(selected)} stocks selected from constituents")

        # Step 5.5: Momentum quality filter — remove fake breakouts
        # (declining trend + low turnover amplification)
        quality_assessments: list[QualityAssessment] = []
        if selected:
            selected, quality_assessments = await self._quality_filter.filter_stocks(
                selected, all_snapshots, trade_date
            )
            result.selected_stocks = selected

        # Step 5.6: Reversal factor filter — remove stocks showing 冲高回落 at 9:40
        # (early fade from intraday high + weak price position)
        if selected:
            selected, reversal_assessments = await self._reversal_filter.filter_stocks(
                selected, all_snapshots, trade_date
            )
            result.selected_stocks = selected

        # Step 6: Recommend — high opening gap + negative growth
        # Also filter out consecutive-up stocks using data from Step 5.5
        consecutive_up_data = {
            a.stock_code: a.consecutive_up_days
            for a in quality_assessments
            if a.consecutive_up_days is not None
        }
        if selected:
            result.recommended_stock = await self._step6_recommend(
                selected, all_snapshots, consecutive_up_data
            )
            if result.recommended_stock:
                rec = result.recommended_stock
                news_str = ""
                if rec.news_check_passed is not None:
                    news_str = f", 舆情={'通过' if rec.news_check_passed else '有负面'}"
                logger.info(
                    f"Step 6: Recommended {rec.stock_code} "
                    f"{rec.stock_name} from board '{rec.board_name}' "
                    f"(OG={rec.open_gain_pct:+.1f}%, GR={rec.growth_rate:+.1f}%{news_str})"
                )
            else:
                logger.info("Step 6: No recommendation (scoring returned None)")

        return result

    # === STEP IMPLEMENTATIONS ===

    async def _step1_filter_gainers(
        self, price_snapshots: dict[str, PriceSnapshot]
    ) -> dict[str, PriceSnapshot]:
        """
        Step 1: Find stocks with 9:40 gain from open > threshold, main board, non-ST.

        When price_snapshots is provided (from iwencai or pre-built),
        we just apply main board + ST filters on top.
        """
        # Filter by gain-from-open threshold (9:40 price vs open price)
        candidates = {
            code: snap
            for code, snap in price_snapshots.items()
            if snap.gain_from_open_pct >= self.GAIN_FROM_OPEN_THRESHOLD
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

    async def _step5_select_constituents(
        self,
        board_constituents: dict[str, list[tuple[str, str]]],
        price_snapshots: dict[str, PriceSnapshot],
    ) -> tuple[list[SelectedStock], dict[str, PriceSnapshot]]:
        """
        Step 5: From all constituent stocks, select those with:
        - Main board only (same filter as Step 1)
        - 9:40 gain from open > 0.56% (uses latest_price, which is 9:40 price in backtest)

        For constituent stocks not in price_snapshots, we need to fetch
        their prices. This is done per-board.
        """
        # Filter constituent stocks by main board (exclude ChiNext, STAR, BSE, etc.)
        all_constituent_codes: set[str] = set()
        filtered_board_constituents: dict[str, list[tuple[str, str]]] = {}
        for board_name, stocks in board_constituents.items():
            allowed = [(code, name) for code, name in stocks if self._stock_filter.is_allowed(code)]
            filtered_board_constituents[board_name] = allowed
            for code, _ in allowed:
                all_constituent_codes.add(code)

        board_constituents = filtered_board_constituents

        # Filter out ST stocks (same as Step 1)
        if all_constituent_codes:
            non_st_codes = set(
                await self._fundamentals_db.batch_filter_st(list(all_constituent_codes))
            )
            board_constituents = {
                board: [(c, n) for c, n in stocks if c in non_st_codes]
                for board, stocks in board_constituents.items()
            }
            all_constituent_codes &= non_st_codes

        # Get PE data for display purposes (feishu notification, etc.)
        pe_data = await self._fundamentals_db.batch_get_pe(list(all_constituent_codes))

        # Get price data for constituents not already in price_snapshots
        missing_codes = [code for code in all_constituent_codes if code not in price_snapshots]
        if missing_codes:
            extra_prices = await self._fetch_constituent_prices(missing_codes)
            price_snapshots = {**price_snapshots, **extra_prices}

        # Process each board
        selected: list[SelectedStock] = []

        for board_name, stocks in board_constituents.items():
            for code, name in stocks:
                snap = price_snapshots.get(code)
                if not snap:
                    continue

                # Filter: 9:40 gain from open > threshold
                if snap.gain_from_open_pct < self.GAIN_FROM_OPEN_THRESHOLD:
                    continue

                pe = pe_data.get(code)
                selected.append(
                    SelectedStock(
                        stock_code=code,
                        stock_name=name,
                        board_name=board_name,
                        open_gain_pct=snap.open_gain_pct,
                        pe_ttm=pe if pe and pe > 0 else 0.0,
                        board_avg_pe=0.0,
                    )
                )

        # Deduplicate: a stock may appear in multiple hot boards.
        # Keep the entry with highest open_gain_pct.
        seen: dict[str, SelectedStock] = {}
        for stock in selected:
            existing = seen.get(stock.stock_code)
            if existing is None or stock.open_gain_pct > existing.open_gain_pct:
                seen[stock.stock_code] = stock

        return sorted(seen.values(), key=lambda s: s.open_gain_pct, reverse=True), price_snapshots

    async def _step6_recommend(
        self,
        selected_stocks: list[SelectedStock],
        price_snapshots: dict[str, PriceSnapshot] | None = None,
        consecutive_up_data: dict[str, int] | None = None,
    ) -> RecommendedStock | None:
        """
        Step 6: Score each stock and pick the highest-scoring one.

        Filters:
            - Limit-up at 9:40 → unbuyable, skip
            - Consecutive up days ≥ 2 → chasing momentum, skip

        Scoring formula (within the largest hot board):
            score = Z(开盘涨幅) - Z(营收增长率)

        Post-scoring: if news_checker is configured, check #1 for negative news.
        If negative → fall back to #2 (no recursive check on #2).
        """
        if not selected_stocks:
            return None

        # --- Find the largest hot board ---
        board_groups: dict[str, list[SelectedStock]] = defaultdict(list)
        for stock in selected_stocks:
            board_groups[stock.board_name].append(stock)

        top_board = max(
            board_groups.keys(),
            key=lambda b: (
                len(board_groups[b]),
                sum(s.open_gain_pct for s in board_groups[b]) / len(board_groups[b]),
            ),
        )
        top_board_stocks = board_groups[top_board]
        logger.info(f"Step 6: Top board '{top_board}' has {len(top_board_stocks)} selected stocks")

        # --- Query growth rates from fundamentals DB ---
        stock_codes = [s.stock_code for s in top_board_stocks]
        growth_data = await self._fundamentals_db.batch_get_revenue_growth(stock_codes)

        # --- Filter out stocks at limit-up at 9:40 ---
        LIMIT_UP_RATIO = 0.10  # Main board +10%
        non_limit_up: list[SelectedStock] = []
        for s in top_board_stocks:
            snap = price_snapshots.get(s.stock_code) if price_snapshots else None
            if snap and snap.prev_close > 0 and snap.latest_price > 0:
                limit_up_price = round(snap.prev_close * (1 + LIMIT_UP_RATIO), 2)
                if snap.latest_price >= limit_up_price:
                    logger.info(
                        f"Step 6: Skip {s.stock_code} ({s.stock_name}): "
                        f"9:40 price {snap.latest_price:.2f} at limit-up "
                        f"({limit_up_price:.2f})"
                    )
                    continue
            non_limit_up.append(s)

        if not non_limit_up:
            logger.info("Step 6: All candidates at limit-up, no recommendation")
            return None

        top_board_stocks = non_limit_up

        # --- Filter out stocks with ≥2 consecutive up days ---
        _cup_data = consecutive_up_data or {}
        non_consecutive: list[SelectedStock] = []
        for s in top_board_stocks:
            cup = _cup_data.get(s.stock_code)
            if cup is not None and cup >= 2:
                logger.info(
                    f"Step 6: Skip {s.stock_code} ({s.stock_name}): consecutive up {cup} days >= 2"
                )
                continue
            non_consecutive.append(s)

        if not non_consecutive:
            logger.info("Step 6: All candidates had >= 2 consecutive up days, no recommendation")
            return None

        top_board_stocks = non_consecutive

        # --- Score and pick ---
        # All candidates must have growth data. Missing → don't trade.
        missing = [s.stock_code for s in top_board_stocks if s.stock_code not in growth_data]
        if missing:
            logger.info(
                f"Step 6: Incomplete data for {missing}, no recommendation (数据不全不交易)"
            )
            return None

        candidates = top_board_stocks

        if len(candidates) == 1:
            ranked = candidates
        else:
            # Z-score standardization:
            #   开盘涨幅: higher = better (+)
            #   营收增长率: lower = better (-)
            og_values = [s.open_gain_pct for s in candidates]
            gr_values = [growth_data[s.stock_code] for s in candidates]

            og_mean = sum(og_values) / len(og_values)
            gr_mean = sum(gr_values) / len(gr_values)

            og_std = (sum((v - og_mean) ** 2 for v in og_values) / len(og_values)) ** 0.5
            gr_std = (sum((v - gr_mean) ** 2 for v in gr_values) / len(gr_values)) ** 0.5

            og_std = og_std if og_std > 0 else 1.0
            gr_std = gr_std if gr_std > 0 else 1.0

            def score(s: SelectedStock) -> float:
                og_z = (s.open_gain_pct - og_mean) / og_std
                gr_z = (growth_data[s.stock_code] - gr_mean) / gr_std
                return og_z - gr_z

            ranked = sorted(candidates, key=score, reverse=True)

        best = ranked[0]
        best_growth = growth_data[best.stock_code]

        # Log scoring details for top 3
        if len(ranked) > 1:
            top3_info = ", ".join(
                f"{s.stock_code}(OG={s.open_gain_pct:+.1f}%,"
                f"GR={growth_data[s.stock_code]:+.1f}%,分={score(s):.2f})"
                for s in ranked[:3]
            )
            logger.info(f"Step 6: Top 3 scores: {top3_info}")
        else:
            logger.info(
                f"Step 6: Single candidate {best.stock_code} "
                f"(OG={best.open_gain_pct:+.1f}%, GR={best_growth:+.1f}%)"
            )

        # --- Negative news check on #1 (if checker configured) ---
        news_check_passed: bool | None = None
        news_check_detail = ""

        if self._news_checker:
            news_result = await self._news_checker.check(best.stock_code, best.stock_name)
            if news_result.error:
                logger.warning(
                    f"Step 6: News check error for {best.stock_code}: {news_result.error}, "
                    f"proceeding with recommendation (fail-open)"
                )
            elif news_result.has_negative_news:
                news_check_passed = False
                news_check_detail = news_result.reason
                logger.info(f"Step 6: {best.stock_code} has negative news: {news_result.reason}")
                if len(ranked) >= 2:
                    best = ranked[1]
                    best_growth = growth_data[best.stock_code]
                    news_check_passed = None  # #2 not checked
                    news_check_detail = f"顺延: {ranked[0].stock_code} 有负面"
                    logger.info(f"Step 6: Falling back to #2: {best.stock_code} {best.stock_name}")
                else:
                    logger.info("Step 6: No fallback candidate, recommending #1 with warning")
            else:
                news_check_passed = True
                news_check_detail = news_result.reason

        snap = price_snapshots.get(best.stock_code) if price_snapshots else None

        return RecommendedStock(
            stock_code=best.stock_code,
            stock_name=best.stock_name,
            board_name=top_board,
            board_stock_count=len(top_board_stocks),
            growth_rate=best_growth,
            open_gain_pct=best.open_gain_pct,
            pe_ttm=best.pe_ttm,
            board_avg_pe=best.board_avg_pe,
            open_price=snap.open_price if snap else 0.0,
            prev_close=snap.prev_close if snap else 0.0,
            latest_price=snap.latest_price if snap else 0.0,
            news_check_passed=news_check_passed,
            news_check_detail=news_check_detail,
        )

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
                    indicators="open,preClose,latest,volume,high,low",
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
                    vol_vals = table.get("volume", [])
                    high_vals = table.get("high", [])
                    low_vals = table.get("low", [])

                    if open_vals and prev_vals and latest_vals:
                        open_price = float(open_vals[0]) if open_vals[0] else 0.0
                        prev_close = float(prev_vals[0]) if prev_vals[0] else 0.0
                        latest = float(latest_vals[0]) if latest_vals[0] else open_price
                        volume = float(vol_vals[0]) if vol_vals and vol_vals[0] else 0.0
                        high = float(high_vals[0]) if high_vals and high_vals[0] else 0.0
                        low = float(low_vals[0]) if low_vals and low_vals[0] else 0.0

                        if prev_close > 0:
                            result[bare_code] = PriceSnapshot(
                                stock_code=bare_code,
                                stock_name="",
                                open_price=open_price,
                                prev_close=prev_close,
                                latest_price=latest,
                                early_volume=volume,
                                high_price=high,
                                low_price=low,
                            )

            except Exception as e:
                logger.error(f"Error fetching realtime prices for batch: {e}")

        return result

    async def _fetch_prices_historical(
        self, stock_codes: list[str], trade_date: date
    ) -> dict[str, PriceSnapshot]:
        """Fetch prices via history_quotes + high_frequency 9:40 price (backtest mode)."""
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
                                latest_price=open_price,  # default; overwritten by 9:40 price below
                            )

            except Exception as e:
                logger.error(f"Error fetching historical prices for batch: {e}")

        # Fetch 9:40 price, volume, high, low for all stocks that got open/prevClose
        if result:
            data_940 = await self._fetch_940_data(list(result.keys()), trade_date)
            for code, (price, volume, high, low) in data_940.items():
                if code in result and price > 0:
                    result[code].latest_price = price
                    result[code].early_volume = volume
                    result[code].high_price = high
                    result[code].low_price = low

        return result

    async def _fetch_940_data(
        self, stock_codes: list[str], trade_date: date
    ) -> dict[str, tuple[float, float, float, float]]:
        """Fetch 9:40 price, volume, high, low via high_frequency API (1-min bars).

        Returns:
            dict: stock_code → (price_at_940, cumulative_volume, max_high, min_low)
        """
        result: dict[str, tuple[float, float, float, float]] = {}
        batch_size = 50
        start_time = f"{trade_date} 09:30:00"
        end_time = f"{trade_date} 09:40:00"

        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i : i + batch_size]
            codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)

            try:
                data = await self._ifind.high_frequency(
                    codes=codes_str,
                    indicators="close,volume,high,low",
                    start_time=start_time,
                    end_time=end_time,
                    function_para={"Interval": "1"},  # 1-minute bars
                )

                for table_entry in data.get("tables", []):
                    thscode = table_entry.get("thscode", "")
                    bare_code = thscode.split(".")[0] if thscode else ""
                    if not bare_code:
                        continue

                    tbl = table_entry.get("table", {})
                    close_vals = tbl.get("close", [])
                    vol_vals = tbl.get("volume", [])
                    high_vals = tbl.get("high", [])
                    low_vals = tbl.get("low", [])

                    price = 0.0
                    cum_volume = 0.0
                    max_high = 0.0
                    min_low = 0.0

                    if close_vals:
                        last_close = close_vals[-1]
                        if last_close is not None:
                            price = float(last_close)

                    if vol_vals:
                        cum_volume = sum(float(v) for v in vol_vals if v is not None)

                    if high_vals:
                        valid_highs = [float(v) for v in high_vals if v is not None]
                        if valid_highs:
                            max_high = max(valid_highs)

                    if low_vals:
                        valid_lows = [float(v) for v in low_vals if v is not None]
                        if valid_lows:
                            min_low = min(valid_lows)

                    if price > 0:
                        result[bare_code] = (price, cum_volume, max_high, min_low)

            except Exception as e:
                logger.warning(f"high_frequency 9:40 fetch failed for batch: {e}")

        return result
