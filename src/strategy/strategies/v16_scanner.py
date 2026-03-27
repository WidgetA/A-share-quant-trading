# === MODULE PURPOSE ===
# V16 live trading scanner: board-first funnel + LGBRank scoring.
# Replaces V15 — no L4 constituent expansion, no V3 regression.
#
# === FUNNEL OVERVIEW ===
# Step 0:  Board cleaning → ~200 boards, ~1500 main-board stocks
# Step 2:  Hot boards — all traded constituents avg gain ≥ 0.80%, ≥2 stocks
# Step 3:  Individual gain ≥ 0.26%, non-ST
# Step 4:  Price ≥ 12 元
# Step 5:  Volume — turnover_amp ∈ [0.498, 6.0]
# Step 6:  Reversal — surge/vol ratio, max(P95, 0.15)
# Step 6.5: Limit-up filter
# Step 6.6: Upper shadow filter (>3%, exempt ≥9.5%)
# Step 7:  LGBRank evaluation → top-10
#
# === KEY DESIGN DIFFERENCES FROM V15 ===
# - Board cleaning FIRST (Step 0), then filter stocks within clean boards
# - No L4 constituent expansion — stocks come from Step 0 universe
# - avg gain uses ALL traded constituents in the board (not just gainers)
# - Data pre-built by caller: scanner never fetches data
# - Missing data = RuntimeError (trading safety principle)
# - LGBRank replaces V3 regression
# - Returns top-10, not single stock
#
# === DATA CONTRACT ===
# Caller provides V16StockData with ALL fields populated:
#   - 9:40 snapshot (open, prev_close, price_940, high, low, volume)
#   - Historical metrics (trend_5d/10d, avg_daily_return_20d, volatility_20d, etc.)
#   - 37-day OHLCV DataFrame (for LGBRank advanced features)
# Missing any field → RuntimeError. No defaults. No approximations.

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field

import pandas as pd

from src.data.sources.local_concept_mapper import LocalConceptMapper
from src.strategy.filters.board_filter import BROAD_CONCEPT_BOARDS, is_junk_board
from src.strategy.filters.reversal_factor_filter import (
    ReversalFactorConfig,
    ReversalFactorFilter,
)
from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig
from src.strategy.lgbrank_scorer import CandidateSnapshot, LGBRankScorer, ScoredStock

logger = logging.getLogger(__name__)


# ── Data Models ──────────────────────────────────────────────


@dataclass
class V16StockData:
    """Complete data for one stock. Built by caller, validated by scanner.

    All prices in 元. All volumes in 股 (shares, NOT lots).
    """

    code: str
    name: str
    open_price: float
    prev_close: float
    price_940: float
    high_940: float
    low_940: float
    volume_940: float  # 9:30-9:40 cumulative volume in 股
    avg_daily_volume: float  # 37d average daily volume in 股
    trend_5d: float  # 5-day price change ratio (e.g. 0.05 = 5%)
    trend_10d: float  # 10-day price change ratio
    avg_daily_return_20d: float  # 20-day average daily return
    volatility_20d: float  # 20-day return std dev
    consecutive_up_days: int
    history_df: pd.DataFrame  # 37d OHLCV (open/high/low/close/volume), ascending


@dataclass
class V16ScanResult:
    """Complete result of a V16 scan."""

    recommended: list[ScoredStock] = field(default_factory=list)  # top-10
    all_scored: list[ScoredStock] = field(default_factory=list)

    # Per-layer counts
    step0_universe_count: int = 0
    step2_hot_board_count: int = 0
    step2_filtered_by_avg_gain: int = 0
    step3_count: int = 0
    step4_count: int = 0
    step5_count: int = 0
    step6_count: int = 0
    step6_5_count: int = 0
    step6_6_count: int = 0
    final_candidates: int = 0

    # Per-layer stock codes for export/debugging
    step0_codes: list[str] = field(default_factory=list)
    step2_boards_detail: dict[str, list[str]] = field(default_factory=dict)
    step2_codes: list[str] = field(default_factory=list)
    step3_codes: list[str] = field(default_factory=list)
    step4_codes: list[str] = field(default_factory=list)
    step5_codes: list[str] = field(default_factory=list)
    step6_codes: list[str] = field(default_factory=list)
    step6_5_codes: list[str] = field(default_factory=list)
    step6_6_codes: list[str] = field(default_factory=list)

    # Hot boards mapping: code → best board name
    stock_best_board: dict[str, str] = field(default_factory=dict)
    # Board avg gain (%) for hot boards
    step2_board_avg_gains: dict[str, float] = field(default_factory=dict)


@dataclass
class _FunnelStock:
    """Internal: a stock passing through the funnel."""

    code: str
    name: str
    board_name: str  # best board (assigned after Step 2)


# ── V16 Scanner ──────────────────────────────────────────────

# Minimum history rows for LGBRank advanced features
_MIN_HISTORY_ROWS = 5


class V16Scanner:
    """V16 board-first momentum scanner with LGBRank scoring.

    Usage:
        scanner = V16Scanner(fundamentals_db, concept_mapper, stock_filter, scorer)
        clean_boards, universe_codes = scanner.get_universe()
        # ... caller builds stock_data dict ...
        result = await scanner.scan(stock_data, clean_boards)
    """

    # ── Step 2 Parameters ──
    MIN_STOCKS_PER_BOARD = 2
    MIN_BOARD_AVG_GAIN = 0.80  # avg gain_from_open_pct (%) for hot board

    # ── Step 3 Parameters ──
    GAIN_FROM_OPEN_THRESHOLD = 0.26  # 0.26% (in pct points)

    # ── Step 4 Parameters ──
    MIN_PRICE = 12.0

    # ── Step 5 Parameters ──
    MIN_TURNOVER_AMP = 0.498
    MAX_TURNOVER_AMP = 6.0
    TURNOVER_FRACTION = 0.125  # 10min / 80min

    # ── Step 6.5 Parameters ──
    LIMIT_UP_RATIO = 0.10

    # ── Step 6.6 Parameters ──
    MAX_UPPER_SHADOW = 0.030  # 3.0%
    SHADOW_EXEMPT_GAIN = 9.5  # gain_from_open >= 9.5% exempt

    # ── V15 board blacklist (carried over) ──
    BOARD_BLACKLIST = frozenset(
        {"物联网", "医疗器械概念", "特高压", "冷链物流", "特钢概念", "三胎概念", "长三角一体化"}
    )

    # ── Top-N for recommendation ──
    TOP_N = 10

    def __init__(
        self,
        fundamentals_db,
        concept_mapper: LocalConceptMapper | None = None,
        stock_filter: StockFilter | None = None,
        scorer: LGBRankScorer | None = None,
    ):
        self._fdb = fundamentals_db
        self._mapper = concept_mapper or LocalConceptMapper()
        self._filter = stock_filter or StockFilter(
            StockFilterConfig(
                exclude_bse=True,
                exclude_chinext=True,
                exclude_star=True,
                exclude_sme=False,
            )
        )
        self._reversal = ReversalFactorFilter(
            ReversalFactorConfig(
                filter_percentile=95.0,
                min_ratio_floor=0.15,
                min_sample_for_percentile=10,
            )
        )
        self._scorer = scorer

    # ── Step 0: Universe Construction ──

    def get_universe(self) -> tuple[dict[str, list[tuple[str, str]]], set[str]]:
        """Step 0: Board cleaning → clean boards + universe stock codes.

        1. Load all boards from LocalConceptMapper (junk already filtered)
        2. Additionally filter BROAD_CONCEPT_BOARDS and BOARD_BLACKLIST
        3. Per board: keep only main board + SME stocks
        4. Aggregate all stock codes

        Returns:
            (clean_boards, universe_codes)
            clean_boards: board_name → [(code, name), ...] (main board only)
            universe_codes: set of all stock codes in clean boards
        """
        self._mapper._ensure_loaded()

        clean_boards: dict[str, list[tuple[str, str]]] = {}
        universe: set[str] = set()

        for board_name, members in self._mapper._board_stocks.items():
            # Skip broad concept boards and blacklisted boards
            if board_name in BROAD_CONCEPT_BOARDS:
                continue
            if board_name in self.BOARD_BLACKLIST:
                continue
            # is_junk_board already filtered in LocalConceptMapper, but double-check
            if is_junk_board(board_name):
                continue

            # Filter: main board + SME only
            filtered = [(c, n) for c, n in members if self._filter.is_allowed(c)]
            if not filtered:
                continue

            clean_boards[board_name] = filtered
            for c, _ in filtered:
                universe.add(c)

        logger.info(
            f"Step 0: {len(clean_boards)} clean boards, {len(universe)} unique stocks in universe"
        )
        return clean_boards, universe

    # ── Main Scan ──

    async def scan(
        self,
        stock_data: dict[str, V16StockData],
        clean_boards: dict[str, list[tuple[str, str]]],
    ) -> V16ScanResult:
        """Run Steps 2-7 on pre-built stock data.

        Args:
            stock_data: code → V16StockData for all universe stocks with valid data.
            clean_boards: from get_universe() Step 0.

        Returns:
            V16ScanResult with top-10 recommendations.
        """
        result = V16ScanResult()
        result.step0_universe_count = len(stock_data)
        result.step0_codes = sorted(stock_data.keys())

        # ── Step 2: Hot boards ──
        hot_boards, stock_all_boards, avg_gain_filtered, board_avg_gains = self._step2_hot_boards(
            clean_boards, stock_data
        )
        result.step2_hot_board_count = len(hot_boards)
        result.step2_filtered_by_avg_gain = avg_gain_filtered
        result.step2_boards_detail = {b: sorted(codes) for b, codes in hot_boards.items()}
        result.step2_board_avg_gains = board_avg_gains

        # Collect unique codes from hot boards
        hot_board_codes: set[str] = set()
        for codes in hot_boards.values():
            hot_board_codes.update(codes)
        result.step2_codes = sorted(hot_board_codes)

        logger.info(
            f"Step 2: {len(hot_boards)} hot boards, "
            f"{len(hot_board_codes)} unique stocks, "
            f"{avg_gain_filtered} boards filtered by avg gain"
        )
        if not hot_boards:
            return result

        # Assign best board per stock (board with most gainers)
        hot_board_sizes = {b: len(codes) for b, codes in hot_boards.items()}
        stock_best_board: dict[str, str] = {}
        for code, boards in stock_all_boards.items():
            stock_best_board[code] = max(boards, key=lambda b: hot_board_sizes.get(b, 0))
        result.stock_best_board = stock_best_board

        # Build funnel candidates from hot board stocks
        candidates = [
            _FunnelStock(code=c, name=stock_data[c].name, board_name=stock_best_board.get(c, ""))
            for c in hot_board_codes
            if c in stock_data
        ]

        # ── Step 3: Individual gain + ST filter ──
        candidates = await self._step3_gain_filter(candidates, stock_data)
        result.step3_count = len(candidates)
        result.step3_codes = sorted(s.code for s in candidates)
        logger.info(f"Step 3: {len(candidates)} passed gain + ST filter")
        if not candidates:
            return result

        # ── Step 4: Price floor ──
        candidates = self._step4_price_filter(candidates, stock_data)
        result.step4_count = len(candidates)
        result.step4_codes = sorted(s.code for s in candidates)
        logger.info(f"Step 4: {len(candidates)} passed price filter")
        if not candidates:
            return result

        # ── Step 5: Volume filter ──
        candidates = self._step5_volume_filter(candidates, stock_data)
        result.step5_count = len(candidates)
        result.step5_codes = sorted(s.code for s in candidates)
        logger.info(f"Step 5: {len(candidates)} passed volume filter")
        if not candidates:
            return result

        # ── Step 6: Reversal filter ──
        candidates = await self._step6_reversal_filter(candidates, stock_data)
        result.step6_count = len(candidates)
        result.step6_codes = sorted(s.code for s in candidates)
        logger.info(f"Step 6: {len(candidates)} passed reversal filter")
        if not candidates:
            return result

        # ── Step 6.5: Limit-up filter ──
        candidates = self._step6_5_limit_up_filter(candidates, stock_data)
        result.step6_5_count = len(candidates)
        result.step6_5_codes = sorted(s.code for s in candidates)
        logger.info(f"Step 6.5: {len(candidates)} passed limit-up filter")
        if not candidates:
            return result

        # ── Step 6.6: Upper shadow filter ──
        candidates = self._step6_6_upper_shadow_filter(candidates, stock_data)
        result.step6_6_count = len(candidates)
        result.step6_6_codes = sorted(s.code for s in candidates)
        logger.info(f"Step 6.6: {len(candidates)} passed upper shadow filter")
        if not candidates:
            return result

        result.final_candidates = len(candidates)

        # ── Step 7: LGBRank scoring ──
        scored = self._step7_lgbrank(candidates, stock_data)
        result.all_scored = scored
        result.recommended = scored[: self.TOP_N]

        if scored:
            top1 = scored[0]
            board = stock_best_board.get(top1.code, "")
            logger.info(
                f"Step 7: Top-1 = {top1.code} {top1.name} "
                f"board='{board}' LGB={top1.score:.4f} "
                f"price={top1.buy_price:.2f}"
            )
            top5_str = ", ".join(f"{s.code}(LGB={s.score:.4f})" for s in scored[:5])
            logger.info(f"Step 7: Top 5: {top5_str}")
        else:
            logger.info("Step 7: No scored candidates")

        return result

    # ── Step 2: Hot Boards ──

    def _step2_hot_boards(
        self,
        clean_boards: dict[str, list[tuple[str, str]]],
        stock_data: dict[str, V16StockData],
    ) -> tuple[dict[str, list[str]], dict[str, list[str]], int, dict[str, float]]:
        """Find boards with avg gain ≥ threshold using ALL traded constituents.

        Key difference from V15: avg gain uses ALL constituents that have trading
        data (not just gainers). This gives a true picture of board momentum.

        Returns:
            (hot_boards, stock_all_boards, filtered_by_avg_gain_count, board_avg_gains)
            hot_boards: board_name → [code, ...]
            stock_all_boards: code → [board_name, ...]
            board_avg_gains: board_name → avg gain %
        """
        hot_boards: dict[str, list[str]] = {}
        stock_all_boards: dict[str, list[str]] = defaultdict(list)
        board_avg_gains: dict[str, float] = {}
        filtered_by_avg = 0

        for board_name, members in clean_boards.items():
            # Collect gain_from_open for ALL constituents with data
            board_gains: list[float] = []
            board_codes: list[str] = []

            for code, _name in members:
                sd = stock_data.get(code)
                if sd is None:
                    continue  # no data for this stock (excluded in Step 0/1)
                if sd.open_price <= 0:
                    continue  # not trading today

                gain_from_open = (sd.price_940 - sd.open_price) / sd.open_price * 100
                board_gains.append(gain_from_open)
                board_codes.append(code)

            if len(board_codes) < self.MIN_STOCKS_PER_BOARD:
                continue

            avg_gain = sum(board_gains) / len(board_gains)
            if avg_gain < self.MIN_BOARD_AVG_GAIN:
                filtered_by_avg += 1
                continue

            hot_boards[board_name] = board_codes
            board_avg_gains[board_name] = avg_gain
            for code in board_codes:
                stock_all_boards[code].append(board_name)

        return hot_boards, dict(stock_all_boards), filtered_by_avg, board_avg_gains

    # ── Step 3: Gain + ST Filter ──

    async def _step3_gain_filter(
        self,
        candidates: list[_FunnelStock],
        stock_data: dict[str, V16StockData],
    ) -> list[_FunnelStock]:
        """Filter by gain_from_open ≥ 0.26% and non-ST."""
        kept: list[_FunnelStock] = []
        for s in candidates:
            sd = stock_data[s.code]
            if sd.open_price <= 0:
                continue
            gain = (sd.price_940 - sd.open_price) / sd.open_price * 100
            if gain < self.GAIN_FROM_OPEN_THRESHOLD:
                continue
            kept.append(s)

        if not kept:
            return []

        # ST filter
        non_st = set(await self._fdb.batch_filter_st([s.code for s in kept]))
        return [s for s in kept if s.code in non_st]

    # ── Step 4: Price Floor ──

    def _step4_price_filter(
        self,
        candidates: list[_FunnelStock],
        stock_data: dict[str, V16StockData],
    ) -> list[_FunnelStock]:
        """Filter by 9:40 price ≥ 12 元."""
        kept = []
        for s in candidates:
            sd = stock_data[s.code]
            if sd.price_940 < self.MIN_PRICE:
                logger.info(
                    f"Step 4: filtered {s.code} ({s.name}): "
                    f"price={sd.price_940:.2f} < {self.MIN_PRICE}"
                )
                continue
            kept.append(s)
        return kept

    # ── Step 5: Volume Filter ──

    def _step5_volume_filter(
        self,
        candidates: list[_FunnelStock],
        stock_data: dict[str, V16StockData],
    ) -> list[_FunnelStock]:
        """Filter by turnover amplification: keep [MIN, MAX]."""
        kept = []
        for s in candidates:
            sd = stock_data[s.code]

            if sd.volume_940 <= 0:
                raise RuntimeError(
                    f"Step 5: volume_940=0 for {s.code} ({s.name}). "
                    f"Missing 9:40 volume data — halting."
                )
            if sd.avg_daily_volume <= 0:
                raise RuntimeError(
                    f"Step 5: avg_daily_volume=0 for {s.code} ({s.name}). "
                    f"Missing historical volume data — halting."
                )

            expected_early = sd.avg_daily_volume * self.TURNOVER_FRACTION
            turnover_amp = sd.volume_940 / expected_early

            if turnover_amp > self.MAX_TURNOVER_AMP:
                logger.info(
                    f"Step 5: filtered {s.code} ({s.name}): "
                    f"amp={turnover_amp:.2f}x > {self.MAX_TURNOVER_AMP}"
                )
                continue
            if turnover_amp < self.MIN_TURNOVER_AMP:
                logger.info(
                    f"Step 5: filtered {s.code} ({s.name}): "
                    f"amp={turnover_amp:.2f}x < {self.MIN_TURNOVER_AMP}"
                )
                continue
            kept.append(s)

        return kept

    # ── Step 6: Reversal Filter ──

    async def _step6_reversal_filter(
        self,
        candidates: list[_FunnelStock],
        stock_data: dict[str, V16StockData],
    ) -> list[_FunnelStock]:
        """Reversal filter using ReversalFactorFilter (P95, floor=0.15)."""
        from src.strategy.strategies.momentum_sector_scanner import (
            PriceSnapshot,
            SelectedStock,
        )

        # Build PriceSnapshot + avg_vol dicts
        snapshots: dict[str, PriceSnapshot] = {}
        avg_vol_data: dict[str, float] = {}

        for s in candidates:
            sd = stock_data[s.code]
            snapshots[s.code] = PriceSnapshot(
                stock_code=s.code,
                stock_name=s.name,
                open_price=sd.open_price,
                prev_close=sd.prev_close,
                latest_price=sd.price_940,
                early_volume=sd.volume_940,
                high_price=sd.high_940,
                low_price=sd.low_940,
            )
            if sd.avg_daily_volume <= 0:
                raise RuntimeError(
                    f"Step 6: avg_daily_volume=0 for {s.code} ({s.name}). "
                    f"Cannot assess reversal risk — halting."
                )
            avg_vol_data[s.code] = sd.avg_daily_volume

        # Adapt to SelectedStock interface
        adapted = [
            SelectedStock(
                stock_code=s.code,
                stock_name=s.name,
                board_name=s.board_name,
                open_gain_pct=0.0,
                pe_ttm=0.0,
                board_avg_pe=0.0,
            )
            for s in candidates
        ]

        kept_adapted, _ = await self._reversal.filter_stocks(adapted, snapshots, avg_vol_data)
        kept_codes = {s.stock_code for s in kept_adapted}
        return [s for s in candidates if s.code in kept_codes]

    # ── Step 6.5: Limit-Up Filter ──

    def _step6_5_limit_up_filter(
        self,
        candidates: list[_FunnelStock],
        stock_data: dict[str, V16StockData],
    ) -> list[_FunnelStock]:
        """Remove stocks at limit-up (open or 9:40 price)."""
        kept = []
        for s in candidates:
            sd = stock_data[s.code]
            if sd.prev_close <= 0:
                raise RuntimeError(
                    f"Step 6.5: prev_close=0 for {s.code} ({s.name}). "
                    f"Cannot calculate limit price — halting."
                )
            limit_price = round(sd.prev_close * (1 + self.LIMIT_UP_RATIO), 2)
            if sd.open_price >= limit_price or sd.price_940 >= limit_price:
                logger.info(
                    f"Step 6.5: filtered {s.code} ({s.name}): at limit-up {limit_price:.2f}"
                )
                continue
            kept.append(s)
        return kept

    # ── Step 6.6: Upper Shadow Filter ──

    def _step6_6_upper_shadow_filter(
        self,
        candidates: list[_FunnelStock],
        stock_data: dict[str, V16StockData],
    ) -> list[_FunnelStock]:
        """Remove stocks with large upper shadow, unless near limit-up gain."""
        kept = []
        for s in candidates:
            sd = stock_data[s.code]
            if sd.open_price <= 0:
                raise RuntimeError(f"Step 6.6: open_price=0 for {s.code} ({s.name}). Halting.")

            gain_from_open = (sd.price_940 - sd.open_price) / sd.open_price * 100
            body_top = max(sd.open_price, sd.price_940)
            shadow = (sd.high_940 - body_top) / sd.open_price

            if shadow > self.MAX_UPPER_SHADOW:
                if gain_from_open >= self.SHADOW_EXEMPT_GAIN:
                    kept.append(s)
                else:
                    logger.info(
                        f"Step 6.6: filtered {s.code} ({s.name}): "
                        f"shadow={shadow:.3%} > {self.MAX_UPPER_SHADOW:.1%}"
                    )
                    continue
            else:
                kept.append(s)

        return kept

    # ── Step 7: LGBRank Scoring ──

    def _step7_lgbrank(
        self,
        candidates: list[_FunnelStock],
        stock_data: dict[str, V16StockData],
    ) -> list[ScoredStock]:
        """Score candidates with LGBRank model."""
        if not self._scorer:
            raise RuntimeError("Step 7: LGBRankScorer not provided. Cannot score candidates.")

        # Build CandidateSnapshot list + history_map
        snapshots: list[CandidateSnapshot] = []
        history_map: dict[str, pd.DataFrame] = {}

        for s in candidates:
            sd = stock_data[s.code]

            # Validate history
            if sd.history_df is None or len(sd.history_df) < _MIN_HISTORY_ROWS:
                rows = 0 if sd.history_df is None else len(sd.history_df)
                raise RuntimeError(
                    f"Step 7: {s.code} ({s.name}) has only {rows} history rows, "
                    f"need ≥{_MIN_HISTORY_ROWS}. Cannot compute LGBRank features — halting."
                )

            snapshots.append(
                CandidateSnapshot(
                    code=sd.code,
                    name=sd.name,
                    open_price=sd.open_price,
                    prev_close=sd.prev_close,
                    price_at_940=sd.price_940,
                    high_price=sd.high_940,
                    low_price=sd.low_940,
                    early_volume=sd.volume_940,
                    avg_daily_volume=sd.avg_daily_volume,
                    trend_pct=sd.trend_5d,
                    trend_10d=sd.trend_10d,
                    avg_daily_return_20d=sd.avg_daily_return_20d,
                    volatility_20d=sd.volatility_20d,
                    consecutive_up_days=sd.consecutive_up_days,
                )
            )
            history_map[sd.code] = sd.history_df

        # Compute avg_market_open_gain from ALL stock_data (not just candidates)
        avg_market_open_gain = self._compute_avg_market_open_gain(stock_data)

        return self._scorer.score_and_rank(snapshots, history_map, avg_market_open_gain)

    # ── Helpers ──

    @staticmethod
    def _compute_avg_market_open_gain(stock_data: dict[str, V16StockData]) -> float:
        """Average (open - prev_close) / prev_close across all stocks with valid data."""
        gains: list[float] = []
        for sd in stock_data.values():
            if sd.prev_close > 0 and sd.open_price > 0:
                gains.append((sd.open_price - sd.prev_close) / sd.prev_close)
        if not gains:
            return 0.0
        return sum(gains) / len(gains)
