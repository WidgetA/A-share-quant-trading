# === MODULE PURPOSE ===
# New stock selection strategy: 8-layer filter + ML ranking.
# Builds stock universe from local board data, then applies progressive filtering
# and machine learning scoring to output Top-10 recommendations.
#
# === PIPELINE ===
# Step 0: Build universe — boards → stocks → exchange filter → ST filter
# Data:   Fetch prev_close (8:00) + rt_min_daily + 37d history (9:39, parallel)
# Preprocess: Compute indicators (avg_vol, trends, volatility, cup) + IPO filter
# Step 2: Hot board filter — board avg early gain ≥ 0.80%, best board assignment
# Step 3: Individual early gain filter — early_gain_pct ≥ 0.26%, exclude limit-up
# Step 4: Price filter — latest_price ≥ 12 yuan
# Step 5: Volume amplification filter — 0.498 ≤ amp ≤ 6.0
# Step 6: Surge ratio filter — dynamic p95 cutoff (floor 0.15)
# Step 7: Upper shadow filter — shadow > 3% AND gain < 7% → remove
# ML scoring: LightGBM LambdaRank (76 features = 38 raw + 38 Z-scored)
#
# === DATA DEPENDENCIES ===
# - LocalConceptMapper: board ↔ stock mapping (local JSON)
# - StockFilter: exchange-based filtering
# - board_filter: junk board definitions (used internally by LocalConceptMapper)
# - ST detection: via stock name (contains "ST") from board_constituents.json
# - TushareRealtimeClient: rt_min_daily raw 1-min bars + prev_close + suspended
# - EarlyWindowAggregator: aggregates raw bars into 09:31~09:40 Snapshot
# - GreptimeBacktestStorage: 37-day historical OHLCV (backtest_daily table)

from __future__ import annotations

import logging
import math
import statistics
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from src.data.sources.local_concept_mapper import LocalConceptMapper
from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig

if TYPE_CHECKING:
    from src.strategy.aggregators import Snapshot

logger = logging.getLogger(__name__)


def _is_st(stock_name: str) -> bool:
    """Check if a stock is ST based on its name.

    ST stocks always have "ST" in their name (e.g. "*ST航通", "ST曙光").
    Uses the stock name from board_constituents.json — no database needed.
    """
    return "ST" in stock_name.upper()


# ── Data Models ──────────────────────────────────────────────


@dataclass
class UniverseResult:
    """Output of Step 0: the tradeable stock universe."""

    codes: set[str]  # valid stock codes after all filtering
    board_stocks: dict[str, list[tuple[str, str]]]  # board → [(code, name), ...]
    stock_boards: dict[str, list[str]]  # code → [board, ...]
    stats: dict[str, int] = field(default_factory=dict)  # diagnostic counts


class DailyBar(NamedTuple):
    """One day of OHLCV data."""

    date: date
    open: float
    high: float
    low: float
    close: float
    volume: float  # in 股 (shares)


@dataclass
class StockSnapshot:
    """Complete data snapshot for one stock at 9:40.

    Assembled from three sources:
    - prev_close: from fetch_prev_closes (8:00 pre-fetch)
    - 9:40 realtime: from rt_min_daily (open, latest, high/low, volume)
    - history: from GreptimeDB daily cache (37 trading days OHLCV)
    """

    stock_code: str
    prev_close: float
    open_price: float  # 9:30 daily open
    latest_price: float  # 9:40 price
    high_940: float  # 9:30-9:40 high
    low_940: float  # 9:30-9:40 low
    early_volume: float  # 9:30-9:40 cumulative volume (股)
    history: list[DailyBar]  # 37 trading days, oldest first

    @property
    def gain_pct(self) -> float:
        """(latest - prev_close) / prev_close as percentage."""
        if self.prev_close <= 0:
            return 0.0
        return (self.latest_price - self.prev_close) / self.prev_close * 100

    @property
    def open_gain_pct(self) -> float:
        """(open - prev_close) / prev_close as percentage."""
        if self.prev_close <= 0:
            return 0.0
        return (self.open_price - self.prev_close) / self.prev_close * 100

    @property
    def early_gain_pct(self) -> float:
        """(latest_price - open_price) / open_price as percentage.

        Intraday gain during 9:30-9:40 window.
        """
        if self.open_price <= 0:
            return 0.0
        return (self.latest_price - self.open_price) / self.open_price * 100


@dataclass
class StockIndicators:
    """Derived indicators from historical data for one stock."""

    avg_volume_37d: float  # 37-day average daily volume (股)
    trend_5d: float  # 5-day close-to-close change (%)
    trend_10d: float  # 10-day close-to-close change (%)
    avg_return_20d: float  # 20-day mean daily return (%)
    volatility_20d: float  # 20-day daily return std dev (%)
    consecutive_up_days: int  # consecutive days close > prev_close


@dataclass
class PreprocessResult:
    """Output of the data preprocessing step."""

    indicators: dict[str, StockIndicators]  # code → indicators (valid stocks only)
    skipped_ipo: list[str]  # stocks listed < 60 days (silently skipped)
    data_quality_alerts: list[str]  # non-new stocks with < 5 bars (→ Feishu alert)


@dataclass
class HotBoardResult:
    """Output of Step 2: hot board filtering."""

    hot_boards: dict[str, float]  # board → average early gain (%)
    qualified_codes: set[str]  # stocks in hot boards that passed preprocess
    best_board: dict[str, str]  # code → best board (most gainers)
    board_data_alerts: dict[str, list[str]]  # board → [missing non-suspended codes]


@dataclass
class SnapshotResult:
    """Output of build_snapshots()."""

    snapshots: dict[str, StockSnapshot]  # code → snapshot (complete data only)
    prev_close_alerts: list[str]  # codes with prev_close <= 0 → Feishu alert


@dataclass
class VolumeFilterResult:
    """Output of Step 5: volume amplification filter."""

    passed_codes: set[str]  # codes within [0.498, 6.0]
    volume_alerts: list[str]  # zero volume or zero avg_volume → Feishu


@dataclass
class FeatureVector:
    """76-dimensional feature vector for one stock (38 raw + 38 Z-scored)."""

    code: str
    raw: dict[str, float]  # 38 raw features
    z_scored: dict[str, float]  # 38 Z-score normalized features

    def to_array(self, feature_names: list[str]) -> list[float]:
        """Flatten to ordered array for model input."""
        return [self.raw[n] for n in feature_names] + [self.z_scored[n] for n in feature_names]


@dataclass
class ScoringResult:
    """Output of ML scoring step."""

    ranked: list[tuple[str, float]]  # [(code, score), ...] sorted descending
    feature_importance: dict[str, float]  # feature → gain importance


@dataclass
class TrainingRecord:
    """One stock on one day, for assembling training data."""

    trade_date: date
    stock_code: str
    features: list[float]  # 76-dimensional (38 raw + 38 Z-scored)
    forward_return_pct: float  # 2-day net return after fees (%)
    label: int  # 0-4 quintile (assigned per day)


@dataclass
class MLScoredStock:
    """One scored stock from ML inference."""

    stock_code: str
    stock_name: str  # from board_constituents.json
    board_name: str  # best hot board
    ml_score: float  # LightGBM LambdaRank prediction
    open_price: float
    prev_close: float
    latest_price: float
    gain_pct: float  # (latest - prev_close) / prev_close × 100
    early_gain_pct: float  # (latest - open) / open × 100
    turnover_amp: float


@dataclass
class FunnelStage:
    """One stage of the scan funnel. Scanner owns key + display label."""

    key: str  # stable machine id, e.g. "L2_hot_boards"
    label: str  # Chinese display label shown in UI / Feishu
    count: int
    codes: list[str] = field(default_factory=list)  # sorted stock codes at this layer


@dataclass
class MLScanResult:
    """Complete output of ML scan pipeline."""

    recommended: MLScoredStock | None = None  # top-1 ranked stock
    all_scored: list[MLScoredStock] = field(default_factory=list)  # top-10
    feature_importance: dict[str, float] = field(default_factory=dict)
    model_name: str = ""
    funnel: list[FunnelStage] = field(default_factory=list)
    hot_board_count: int = 0
    final_candidates: int = 0
    skip_reason: str = ""  # why empty: "no_daily_data" | "no_snapshots" | ""


# ── Feature Constants ───────────────────────────────────────

# 13 basic + 14 advanced + 11 cross = 38 raw features
FEATURE_NAMES_BASIC: list[str] = [
    "open_gain",  # (latest - open) / open × 100
    "volume_amp",  # early_vol / (avg_vol × early_ratio)
    "consecutive_up_days",
    "trend_5d",
    "trend_10d",
    "avg_return_20d",
    "volatility_20d",
    "early_price_range",  # (high_940 - low_940) / open × 100
    "market_open_gain",  # average open_gain of all candidates (same for all)
    "trend_consistency",  # fraction of last 5 days with positive returns
    "gap",  # (open - prev_close) / prev_close × 100
    "upper_shadow_ratio",  # (high - max(open, latest)) / open × 100
    "volume_ratio",  # early_vol / avg_vol (unnormalized)
]

FEATURE_NAMES_ADVANCED: list[str] = [
    "open_position_consistency",  # open vs yesterday's range
    "volume_price_divergence",  # sign(return) × sign(vol_change)
    "intraday_momentum_cont",  # early_gain × gap (continuation)
    "volume_concentration",  # early vol concentration
    "relative_strength",  # stock gain vs market gain
    "return_consistency",  # |mean(ret)| / std(ret)
    "amplitude_decay",  # 5d amplitude / 20d amplitude
    "volume_stability",  # CV of volume over 20d
    "close_vs_vwap",  # latest vs typical price proxy
    "volume_weighted_return",  # vol-weighted avg return 20d
    "price_channel_position",  # (latest - 20d_low) / (20d_high - 20d_low)
    "up_day_ratio_20d",  # fraction of up days in 20d
    "amplitude_20d",  # avg daily amplitude over 20d (%)
    "volume_ratio_5d_20d",  # avg vol 5d / avg vol 20d
]

FEATURE_NAMES_CROSS: list[str] = [
    "momentum_x_mean_reversion",  # trend_5d × (1 - ret/vol)
    "trend_acceleration",  # trend_5d - trend_10d
    "momentum_quality",  # trend_5d × volume_amp
    "volume_trend_interaction",  # vol_ratio_5d_20d × trend_5d
    "gap_volume_interaction",  # gap × volume_amp
    "strength_persistence",  # relative_strength × cup
    "volatility_adj_return",  # avg_return / volatility (Sharpe-like)
    "volume_price_momentum",  # volume_amp × open_gain
    "gap_reversion",  # gap × (open_gain - gap)
    "trend_volume_divergence",  # trend_5d / vol_ratio_5d_20d
    "momentum_stability",  # return_consistency × trend_5d
]

FEATURE_NAMES_RAW: list[str] = FEATURE_NAMES_BASIC + FEATURE_NAMES_ADVANCED + FEATURE_NAMES_CROSS
assert len(FEATURE_NAMES_RAW) == 38  # noqa: S101

# Full 76 feature names: raw + Z-scored versions
FEATURE_NAMES_ALL: list[str] = FEATURE_NAMES_RAW + [f"z_{name}" for name in FEATURE_NAMES_RAW]
assert len(FEATURE_NAMES_ALL) == 76  # noqa: S101

# ── ML Constants ────────────────────────────────────────────

_RETRAIN_INTERVAL_DAYS = 20  # retrain every 20 trading days

_MODEL_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "models"


# ── ML Scanner ──────────────────────────────────────────────


class MLScanner:
    """8-layer filter + ML ranking stock selection strategy.

    Usage:
        mapper = LocalConceptMapper()
        scanner = MLScanner(mapper)

        universe = await scanner.build_universe()
        print(f"Universe: {len(universe.codes)} stocks from {len(universe.board_stocks)} boards")
    """

    _IPO_PROTECTION_DAYS = 60  # skip stocks listed < 60 calendar days
    _MIN_HISTORY_BARS = 5  # non-new stocks with fewer bars → data quality alert
    _HISTORY_REQUEST_DAYS = 37  # trading days of history we request
    _HOT_BOARD_THRESHOLD = 0.80  # board avg early gain ≥ 0.80% → hot
    _MIN_EARLY_GAIN = 0.26  # individual stock early gain ≥ 0.26%
    _LIMIT_UP_PCT = 9.8  # gain_pct (vs prev_close) ≥ this → limit up, can't buy
    _MIN_PRICE = 12.0  # stock price ≥ 12 yuan
    _TURNOVER_AMP_MIN = 0.498  # volume amplification lower bound
    _TURNOVER_AMP_MAX = 6.0  # volume amplification upper bound
    _EARLY_SESSION_RATIO = 0.125  # empirical: first 10min ≈ 12.5% of daily volume
    _SURGE_RATIO_PERCENTILE = 0.95  # dynamic cutoff at 95th percentile
    _SURGE_RATIO_FLOOR = 0.15  # cutoff never below this
    _UPPER_SHADOW_MAX = 3.0  # upper shadow > 3% → remove
    _UPPER_SHADOW_EXEMPT = 9.5  # but exempt if early_gain_pct >= 9.5% (intraday)

    def __init__(
        self,
        concept_mapper: LocalConceptMapper,
    ) -> None:
        self._mapper = concept_mapper
        # Exclude BSE, ChiNext, STAR; keep Shanghai Main + Shenzhen Main + SME
        self._stock_filter = StockFilter(
            StockFilterConfig(
                exclude_bse=True,
                exclude_chinext=True,
                exclude_star=True,
                exclude_sme=False,
            )
        )

    async def build_universe(self) -> UniverseResult:
        """Step 0: Build the tradeable stock universe.

        Pipeline:
            1. Load all THS boards + constituent stocks (LocalConceptMapper)
            2. Remove junk boards (~200+)
            3. Collect unique stock codes from remaining boards
            4. Exchange filter: keep SH Main (600/601/603/605) + SZ Main (000/001/002/003)
            5. ST filter: exclude stocks with "ST" in name

        Returns:
            UniverseResult with filtered codes, board mappings, and stats.
        """
        # LocalConceptMapper already filters junk boards on load
        self._mapper._ensure_loaded()

        # Get non-junk board → stocks mapping (already filtered by LocalConceptMapper)
        board_stocks = dict(self._mapper._board_stocks)
        total_boards = len(board_stocks)

        # Collect all unique stocks: code → name (for ST check by name)
        code_name: dict[str, str] = {}
        for members in board_stocks.values():
            for code, name in members:
                if code not in code_name:
                    code_name[code] = name
        total_raw = len(code_name)

        # Exchange filter: keep only SH Main + SZ Main + SME
        exchange_filtered = set(self._stock_filter.filter_stocks(list(code_name)))
        excluded_exchange = total_raw - len(exchange_filtered)

        # ST filter: check stock name from board_constituents.json
        st_codes = {code for code in exchange_filtered if _is_st(code_name[code])}
        valid_codes = exchange_filtered - st_codes
        excluded_st = len(st_codes)

        # Rebuild board_stocks and stock_boards with only valid codes
        filtered_board_stocks: dict[str, list[tuple[str, str]]] = {}
        for board_name, members in board_stocks.items():
            valid_members = [(c, n) for c, n in members if c in valid_codes]
            if valid_members:
                filtered_board_stocks[board_name] = valid_members

        # Build reverse index: stock → boards
        stock_boards: dict[str, list[str]] = {}
        for board_name, members in filtered_board_stocks.items():
            for code, _name in members:
                if code not in stock_boards:
                    stock_boards[code] = []
                stock_boards[code].append(board_name)

        stats = {
            "total_boards": total_boards,
            "total_raw_stocks": total_raw,
            "excluded_exchange": excluded_exchange,
            "excluded_st": excluded_st,
            "final_boards": len(filtered_board_stocks),
            "final_stocks": len(valid_codes),
        }

        logger.info(
            "Step 0 universe: %d boards, %d raw stocks → "
            "-%d exchange -%d ST → %d final stocks in %d boards",
            total_boards,
            total_raw,
            excluded_exchange,
            excluded_st,
            len(valid_codes),
            len(filtered_board_stocks),
        )

        return UniverseResult(
            codes=valid_codes,
            board_stocks=filtered_board_stocks,
            stock_boards=stock_boards,
            stats=stats,
        )

    @staticmethod
    def build_snapshots(
        universe_codes: set[str],
        prev_closes: dict[str, float],
        early_data: dict[str, tuple[float, Snapshot]],
        historical_bars: dict[str, list[DailyBar]],
    ) -> SnapshotResult:
        """Assemble StockSnapshot for each stock from three data sources.

        Stocks missing any required data are skipped (logged).
        Stocks with prev_close <= 0 are collected as data quality alerts.

        Args:
            universe_codes: Valid stock codes from build_universe().
            prev_closes: {code: close_price} from fetch_prev_closes (8:00).
            early_data: {code: (day_open_price, Snapshot)} where Snapshot is the
                09:31~09:40 aggregation produced by ``EarlyWindowAggregator`` on
                the raw 1-min bars returned by ``batch_get_minute_bars()``.
            historical_bars: {code: [DailyBar, ...]} from GreptimeDB (9:39).

        Returns:
            SnapshotResult with snapshots and prev_close alerts.
        """
        snapshots: dict[str, StockSnapshot] = {}
        prev_close_alerts: list[str] = []
        skipped_no_prev = 0
        skipped_no_early = 0
        skipped_no_hist = 0

        for code in universe_codes:
            pc = prev_closes.get(code)
            if pc is None:
                skipped_no_prev += 1
                continue
            if pc <= 0:
                prev_close_alerts.append(code)
                continue
            ed = early_data.get(code)
            if ed is None:
                skipped_no_early += 1
                continue
            open_price, snap = ed
            if open_price <= 0 or snap.close <= 0:
                skipped_no_early += 1
                continue
            hist = historical_bars.get(code)
            if not hist:
                skipped_no_hist += 1
                continue

            snapshots[code] = StockSnapshot(
                stock_code=code,
                prev_close=pc,
                open_price=open_price,
                latest_price=snap.close,
                high_940=snap.max_high,
                low_940=snap.min_low,
                early_volume=snap.cum_volume,
                history=hist,
            )

        if prev_close_alerts:
            logger.warning(
                "build_snapshots: %d stocks have prev_close <= 0: %s",
                len(prev_close_alerts),
                prev_close_alerts[:20],
            )

        logger.info(
            "build_snapshots: %d/%d stocks have complete data "
            "(skipped: %d no prev_close, %d bad prev_close, %d no early data, %d no history)",
            len(snapshots),
            len(universe_codes),
            skipped_no_prev,
            len(prev_close_alerts),
            skipped_no_early,
            skipped_no_hist,
        )
        return SnapshotResult(
            snapshots=snapshots,
            prev_close_alerts=sorted(prev_close_alerts),
        )

    @staticmethod
    def preprocess(
        snapshots: dict[str, StockSnapshot],
        today: date,
    ) -> PreprocessResult:
        """Compute indicators and apply new-stock protection.

        For each stock in snapshots:
        1. Listed < 60 calendar days → skip (IPO volatility too high)
        2. Non-new but < 5 history bars → data quality alert (→ Feishu)
        3. Otherwise → compute all indicators

        Args:
            snapshots: {code: StockSnapshot} from build_snapshots().
            today: Current trading date.

        Returns:
            PreprocessResult with indicators, skipped IPOs, and data alerts.
        """
        indicators: dict[str, StockIndicators] = {}
        skipped_ipo: list[str] = []
        data_quality_alerts: list[str] = []

        for code, snap in snapshots.items():
            history = snap.history

            # ── New stock protection ──
            # Fewer bars than requested → stock was listed recently.
            # Use first bar's date to check calendar days since listing.
            if len(history) < MLScanner._HISTORY_REQUEST_DAYS:
                listing_days = (today - history[0].date).days
                if listing_days < MLScanner._IPO_PROTECTION_DAYS:
                    skipped_ipo.append(code)
                    continue

            # ── Data quality check ──
            if len(history) < MLScanner._MIN_HISTORY_BARS:
                data_quality_alerts.append(code)
                continue

            # ── Compute indicators ──
            closes = [bar.close for bar in history]
            volumes = [bar.volume for bar in history]

            # 37-day average volume
            avg_vol = statistics.mean(volumes)

            # 5-day price trend
            if len(closes) >= 6 and closes[-6] > 0:
                trend_5d = (closes[-1] - closes[-6]) / closes[-6] * 100
            else:
                trend_5d = 0.0

            # 10-day price trend
            if len(closes) >= 11 and closes[-11] > 0:
                trend_10d = (closes[-1] - closes[-11]) / closes[-11] * 100
            else:
                trend_10d = 0.0

            # 20-day average daily return and volatility
            recent = closes[-21:] if len(closes) >= 21 else closes
            returns_pct = [
                (recent[i + 1] - recent[i]) / recent[i] * 100
                for i in range(len(recent) - 1)
                if recent[i] > 0
            ]
            if returns_pct:
                avg_ret = statistics.mean(returns_pct)
                vol = statistics.stdev(returns_pct) if len(returns_pct) >= 2 else 0.0
            else:
                avg_ret = 0.0
                vol = 0.0

            # Consecutive up days (from latest bar backwards)
            cup = 0
            for i in range(len(history) - 1, 0, -1):
                if history[i].close > history[i - 1].close:
                    cup += 1
                else:
                    break

            indicators[code] = StockIndicators(
                avg_volume_37d=avg_vol,
                trend_5d=trend_5d,
                trend_10d=trend_10d,
                avg_return_20d=avg_ret,
                volatility_20d=vol,
                consecutive_up_days=cup,
            )

        logger.info(
            "preprocess: %d snapshots → %d with indicators, %d IPO skipped, %d data quality alerts",
            len(snapshots),
            len(indicators),
            len(skipped_ipo),
            len(data_quality_alerts),
        )

        return PreprocessResult(
            indicators=indicators,
            skipped_ipo=sorted(skipped_ipo),
            data_quality_alerts=sorted(data_quality_alerts),
        )

    @staticmethod
    def filter_hot_boards(
        board_stocks: dict[str, list[tuple[str, str]]],
        snapshots: dict[str, StockSnapshot],
        valid_codes: set[str],
        suspended: set[str],
    ) -> HotBoardResult:
        """Step 2: Filter hot boards by average early gain.

        For each board:
        1. Compute early_gain_pct for all constituent stocks with data
        2. Flag boards with non-suspended stocks missing data (→ Feishu)
        3. Board avg early gain ≥ 0.80% → hot board
        4. Each qualifying stock records its best board (most gainers)

        Args:
            board_stocks: board → [(code, name)] from UniverseResult.
            snapshots: {code: StockSnapshot} from build_snapshots().
            valid_codes: Codes that passed preprocess (indicators.keys()).
            suspended: Today's suspended stock codes.

        Returns:
            HotBoardResult with hot boards, qualified stocks, and alerts.
        """
        hot_boards: dict[str, float] = {}
        board_gainers: dict[str, int] = {}
        board_data_alerts: dict[str, list[str]] = {}

        for board_name, members in board_stocks.items():
            gains: list[float] = []
            n_gainers = 0
            missing: list[str] = []

            for code, _name in members:
                snap = snapshots.get(code)
                if snap is None:
                    if code not in suspended:
                        missing.append(code)
                    continue
                gain = snap.early_gain_pct
                gains.append(gain)
                if gain > 0:
                    n_gainers += 1

            if missing:
                board_data_alerts[board_name] = missing

            if not gains:
                continue

            avg_gain = statistics.mean(gains)
            if avg_gain >= MLScanner._HOT_BOARD_THRESHOLD:
                hot_boards[board_name] = round(avg_gain, 4)
                board_gainers[board_name] = n_gainers

        # Collect qualified stocks + determine best board per stock
        qualified_codes: set[str] = set()
        best_board: dict[str, str] = {}

        for board_name in hot_boards:
            for code, _name in board_stocks[board_name]:
                if code not in valid_codes:
                    continue
                qualified_codes.add(code)
                prev_best = best_board.get(code)
                if prev_best is None or board_gainers[board_name] > board_gainers[prev_best]:
                    best_board[code] = board_name

        logger.info(
            "filter_hot_boards: %d/%d boards hot (≥%.2f%%), "
            "%d qualified stocks, %d boards with data alerts",
            len(hot_boards),
            len(board_stocks),
            MLScanner._HOT_BOARD_THRESHOLD,
            len(qualified_codes),
            len(board_data_alerts),
        )

        return HotBoardResult(
            hot_boards=hot_boards,
            qualified_codes=qualified_codes,
            best_board=best_board,
            board_data_alerts=board_data_alerts,
        )

    @staticmethod
    def filter_by_early_gain(
        codes: set[str],
        snapshots: dict[str, StockSnapshot],
    ) -> set[str]:
        """Step 3: Keep stocks with 0.26% <= early gain, exclude limit-up.

        - early_gain_pct < 0.26% → not following board momentum, remove
        - gain_pct >= 9.8% (vs prev_close) → limit up, can't buy, remove

        Args:
            codes: Qualified codes from filter_hot_boards().
            snapshots: {code: StockSnapshot} from build_snapshots().

        Returns:
            Set of codes that pass the early gain threshold.
        """
        passed: set[str] = set()
        removed_low = 0
        removed_limit = 0

        for code in codes:
            snap = snapshots.get(code)
            if snap is None:
                continue
            if snap.gain_pct >= MLScanner._LIMIT_UP_PCT:
                removed_limit += 1
            elif snap.early_gain_pct < MLScanner._MIN_EARLY_GAIN:
                removed_low += 1
            else:
                passed.add(code)

        logger.info(
            "filter_by_early_gain: %d → %d (-%d low <%.2f%%, -%d limit-up ≥%.1f%%)",
            len(codes),
            len(passed),
            removed_low,
            MLScanner._MIN_EARLY_GAIN,
            removed_limit,
            MLScanner._LIMIT_UP_PCT,
        )
        return passed

    @staticmethod
    def filter_by_price(
        codes: set[str],
        snapshots: dict[str, StockSnapshot],
    ) -> set[str]:
        """Step 4: Keep only stocks with latest price >= 12 yuan."""
        passed = {
            code
            for code in codes
            if code in snapshots and snapshots[code].latest_price >= MLScanner._MIN_PRICE
        }
        logger.info(
            "filter_by_price: %d → %d (≥%.0f yuan)",
            len(codes),
            len(passed),
            MLScanner._MIN_PRICE,
        )
        return passed

    @staticmethod
    def filter_by_volume(
        codes: set[str],
        snapshots: dict[str, StockSnapshot],
        indicators: dict[str, StockIndicators],
    ) -> VolumeFilterResult:
        """Step 5: Filter by volume amplification (turnover ratio).

        amp = early_volume / (avg_volume_37d × 0.125)
        Keep stocks where 0.498 <= amp <= 6.0.

        Args:
            codes: Codes from filter_by_price().
            snapshots: {code: StockSnapshot} from build_snapshots().
            indicators: {code: StockIndicators} from preprocess().

        Returns:
            VolumeFilterResult with passed codes and volume alerts.
        """
        passed: set[str] = set()
        volume_alerts: list[str] = []
        removed_low = 0
        removed_high = 0

        for code in codes:
            snap = snapshots.get(code)
            ind = indicators.get(code)
            if snap is None or ind is None:
                continue

            if snap.early_volume <= 0 or ind.avg_volume_37d <= 0:
                volume_alerts.append(code)
                continue

            expected = ind.avg_volume_37d * MLScanner._EARLY_SESSION_RATIO
            amp = snap.early_volume / expected

            if amp < MLScanner._TURNOVER_AMP_MIN:
                removed_low += 1
            elif amp > MLScanner._TURNOVER_AMP_MAX:
                removed_high += 1
            else:
                passed.add(code)

        logger.info(
            "filter_by_volume: %d → %d (-%d low <%.3f, -%d high >%.1f, %d alerts)",
            len(codes),
            len(passed),
            removed_low,
            MLScanner._TURNOVER_AMP_MIN,
            removed_high,
            MLScanner._TURNOVER_AMP_MAX,
            len(volume_alerts),
        )

        return VolumeFilterResult(
            passed_codes=passed,
            volume_alerts=sorted(volume_alerts),
        )

    @staticmethod
    def filter_by_surge_ratio(
        codes: set[str],
        snapshots: dict[str, StockSnapshot],
        indicators: dict[str, StockIndicators],
    ) -> set[str]:
        """Step 6: Remove stocks with hollow surges (high price on low volume).

        surge_range  = (high_940 - open) / open
        rel_volume   = early_volume / avg_volume_37d
        surge_ratio  = surge_range / rel_volume

        Cutoff = max(95th percentile of surge_ratio, 0.15).
        Stocks above the cutoff are removed.

        Args:
            codes: Codes from filter_by_volume().
            snapshots: {code: StockSnapshot} from build_snapshots().
            indicators: {code: StockIndicators} from preprocess().

        Returns:
            Set of codes that pass the surge ratio filter.
        """
        # Phase 1: compute surge_ratio for each stock
        ratios: dict[str, float] = {}
        for code in codes:
            snap = snapshots.get(code)
            ind = indicators.get(code)
            if snap is None or ind is None:
                continue
            if snap.open_price <= 0 or ind.avg_volume_37d <= 0:
                continue

            surge_range = (snap.high_940 - snap.open_price) / snap.open_price
            rel_volume = snap.early_volume / ind.avg_volume_37d

            if rel_volume <= 0:
                continue
            ratios[code] = surge_range / rel_volume

        if not ratios:
            return set()

        # Phase 2: dynamic cutoff at 95th percentile (floor 0.15)
        sorted_vals = sorted(ratios.values())
        idx = int(len(sorted_vals) * MLScanner._SURGE_RATIO_PERCENTILE)
        idx = min(idx, len(sorted_vals) - 1)
        p95 = sorted_vals[idx]
        cutoff = max(p95, MLScanner._SURGE_RATIO_FLOOR)

        passed = {code for code, r in ratios.items() if r <= cutoff}
        removed = len(ratios) - len(passed)

        logger.info(
            "filter_by_surge_ratio: %d → %d (cutoff=%.4f, p95=%.4f, removed %d)",
            len(codes),
            len(passed),
            cutoff,
            p95,
            removed,
        )
        return passed

    @staticmethod
    def filter_by_upper_shadow(
        codes: set[str],
        snapshots: dict[str, StockSnapshot],
    ) -> set[str]:
        """Step 7: Remove stocks with heavy upper shadow (selling pressure).

        upper_shadow = (high_940 - max(open, latest)) / open
        Remove if upper_shadow > 3% AND early_gain_pct < 9.5%.
        Stocks with strong intraday gain (≥9.5%) are exempt — buyer dominance.

        Args:
            codes: Codes from filter_by_surge_ratio().
            snapshots: {code: StockSnapshot} from build_snapshots().

        Returns:
            Set of codes that pass the upper shadow filter.
        """
        passed: set[str] = set()
        removed = 0

        for code in codes:
            snap = snapshots.get(code)
            if snap is None:
                continue
            if snap.open_price <= 0:
                continue

            body_top = max(snap.open_price, snap.latest_price)
            shadow = (snap.high_940 - body_top) / snap.open_price * 100

            if (
                shadow > MLScanner._UPPER_SHADOW_MAX
                and snap.early_gain_pct < MLScanner._UPPER_SHADOW_EXEMPT
            ):
                removed += 1
            else:
                passed.add(code)

        logger.info(
            "filter_by_upper_shadow: %d → %d (removed %d with shadow >%.1f%%)",
            len(codes),
            len(passed),
            removed,
            MLScanner._UPPER_SHADOW_MAX,
        )
        return passed

    @staticmethod
    def validate_prev_closes(
        universe_codes: set[str],
        prev_closes: dict[str, float],
        suspended: set[str],
    ) -> list[str]:
        """8:00 pre-market check: find non-suspended stocks missing prev_close.

        Args:
            universe_codes: Stock codes from build_universe().
            prev_closes: {code: close_price} from fetch_prev_closes().
            suspended: Today's suspended stock codes from fetch_suspended_stocks().

        Returns:
            List of stock codes that are NOT suspended but have no prev_close.
            Empty list = all good.
        """
        expected = universe_codes - suspended
        missing = sorted(code for code in expected if code not in prev_closes)
        if missing:
            logger.warning(
                "validate_prev_closes: %d non-suspended stocks missing prev_close: %s",
                len(missing),
                missing[:20],  # log first 20
            )
        else:
            logger.info(
                "validate_prev_closes: OK — %d stocks all have prev_close (%d suspended excluded)",
                len(expected),
                len(universe_codes) - len(expected),
            )
        return missing

    # ── ML Feature Engineering ──────────────────────────────

    @staticmethod
    def compute_raw_features(
        snap: StockSnapshot,
        ind: StockIndicators,
        market_open_gain: float,
    ) -> dict[str, float]:
        """Compute 38 raw features for one stock.

        Args:
            snap: Stock snapshot at 9:40.
            ind: Pre-computed indicators from preprocess().
            market_open_gain: Average early_gain_pct of all candidates today.

        Returns:
            Dict of 38 feature_name → value.
        """
        f: dict[str, float] = {}
        history = snap.history
        closes = [bar.close for bar in history]

        # ── 13 Basic Features ──
        f["open_gain"] = snap.early_gain_pct

        expected_vol = ind.avg_volume_37d * MLScanner._EARLY_SESSION_RATIO
        f["volume_amp"] = snap.early_volume / expected_vol if expected_vol > 0 else 0.0

        f["consecutive_up_days"] = float(ind.consecutive_up_days)
        f["trend_5d"] = ind.trend_5d
        f["trend_10d"] = ind.trend_10d
        f["avg_return_20d"] = ind.avg_return_20d
        f["volatility_20d"] = ind.volatility_20d

        f["early_price_range"] = (
            (snap.high_940 - snap.low_940) / snap.open_price * 100 if snap.open_price > 0 else 0.0
        )

        f["market_open_gain"] = market_open_gain

        # Trend consistency: fraction of last 5 days with positive returns
        if len(closes) >= 6:
            recent_5 = closes[-6:]
            pos_days = sum(1 for i in range(1, len(recent_5)) if recent_5[i] > recent_5[i - 1])
            f["trend_consistency"] = pos_days / 5.0
        else:
            f["trend_consistency"] = 0.0

        f["gap"] = snap.open_gain_pct  # (open - prev_close) / prev_close

        body_top = max(snap.open_price, snap.latest_price)
        f["upper_shadow_ratio"] = (
            (snap.high_940 - body_top) / snap.open_price * 100 if snap.open_price > 0 else 0.0
        )

        f["volume_ratio"] = (
            snap.early_volume / ind.avg_volume_37d if ind.avg_volume_37d > 0 else 0.0
        )

        # ── 14 Advanced Features ──

        # Open position consistency: where today's open sits in yesterday's range
        if len(history) >= 1:
            last_bar = history[-1]
            day_range = last_bar.high - last_bar.low
            f["open_position_consistency"] = (
                (snap.open_price - last_bar.low) / day_range if day_range > 0 else 0.5
            )
        else:
            f["open_position_consistency"] = 0.5

        # Volume-price divergence: return × volume change (negative = divergence)
        if len(history) >= 2:
            prev_c = history[-2].close
            last_ret = (history[-1].close - prev_c) / prev_c if prev_c > 0 else 0.0
            prev_v = history[-2].volume
            last_vol_chg = (history[-1].volume - prev_v) / prev_v if prev_v > 0 else 0.0
            f["volume_price_divergence"] = last_ret * last_vol_chg
        else:
            f["volume_price_divergence"] = 0.0

        # Intraday momentum continuation: early gain × gap (positive = continuation)
        f["intraday_momentum_cont"] = snap.early_gain_pct * snap.open_gain_pct

        # Volume concentration: how much of avg daily vol is in early session
        f["volume_concentration"] = f["volume_amp"] * MLScanner._EARLY_SESSION_RATIO

        # Relative strength: stock gain vs market gain
        f["relative_strength"] = snap.early_gain_pct - market_open_gain

        # Return consistency: |mean(ret)| / std(ret) over 20d
        if len(closes) >= 21:
            rets = [
                (closes[i + 1] - closes[i]) / closes[i] * 100
                for i in range(len(closes) - 21, len(closes) - 1)
                if closes[i] > 0
            ]
            if len(rets) >= 2:
                mean_r = statistics.mean(rets)
                std_r = statistics.stdev(rets)
                f["return_consistency"] = abs(mean_r) / std_r if std_r > 0 else 0.0
            else:
                f["return_consistency"] = 0.0
        else:
            f["return_consistency"] = 0.0

        # Amplitude decay: avg amplitude 5d / avg amplitude 20d
        if len(history) >= 20:
            amp_5 = [(b.high - b.low) / b.open * 100 for b in history[-5:] if b.open > 0]
            amp_20 = [(b.high - b.low) / b.open * 100 for b in history[-20:] if b.open > 0]
            mean_5 = statistics.mean(amp_5) if amp_5 else 0.0
            mean_20 = statistics.mean(amp_20) if amp_20 else 0.0
            f["amplitude_decay"] = mean_5 / mean_20 if mean_20 > 0 else 1.0
        else:
            f["amplitude_decay"] = 1.0

        # Volume stability: CV of volumes over 20d
        if len(history) >= 20:
            vols_20 = [b.volume for b in history[-20:]]
            mean_v = statistics.mean(vols_20)
            std_v = statistics.stdev(vols_20) if len(vols_20) >= 2 else 0.0
            f["volume_stability"] = std_v / mean_v if mean_v > 0 else 0.0
        else:
            f["volume_stability"] = 0.0

        # Close vs VWAP position: latest vs typical price proxy of last bar
        if len(history) >= 1:
            last = history[-1]
            typical = (last.high + last.low + last.close) / 3
            f["close_vs_vwap"] = (
                (snap.latest_price - typical) / typical * 100 if typical > 0 else 0.0
            )
        else:
            f["close_vs_vwap"] = 0.0

        # Volume-weighted return over 20d
        if len(history) >= 21:
            total_vw = 0.0
            total_vol = 0.0
            for i in range(len(history) - 20, len(history)):
                if i > 0 and history[i - 1].close > 0:
                    ret = (history[i].close - history[i - 1].close) / history[i - 1].close * 100
                    total_vw += ret * history[i].volume
                    total_vol += history[i].volume
            f["volume_weighted_return"] = total_vw / total_vol if total_vol > 0 else 0.0
        else:
            f["volume_weighted_return"] = 0.0

        # Price channel position: (latest - 20d_low) / (20d_high - 20d_low)
        if len(history) >= 20:
            highs_20 = [b.high for b in history[-20:]]
            lows_20 = [b.low for b in history[-20:]]
            h20, l20 = max(highs_20), min(lows_20)
            channel = h20 - l20
            f["price_channel_position"] = (
                (snap.latest_price - l20) / channel if channel > 0 else 0.5
            )
        else:
            f["price_channel_position"] = 0.5

        # 20d up-day ratio
        if len(closes) >= 21:
            up_days = sum(
                1 for i in range(len(closes) - 20, len(closes)) if closes[i] > closes[i - 1]
            )
            f["up_day_ratio_20d"] = up_days / 20.0
        else:
            f["up_day_ratio_20d"] = 0.5

        # 20d average daily amplitude
        if len(history) >= 20:
            amps = [(b.high - b.low) / b.open * 100 for b in history[-20:] if b.open > 0]
            f["amplitude_20d"] = statistics.mean(amps) if amps else 0.0
        else:
            f["amplitude_20d"] = 0.0

        # 5d vs 20d volume ratio
        if len(history) >= 20:
            vol_5d = statistics.mean([b.volume for b in history[-5:]])
            vol_20d = statistics.mean([b.volume for b in history[-20:]])
            f["volume_ratio_5d_20d"] = vol_5d / vol_20d if vol_20d > 0 else 1.0
        else:
            f["volume_ratio_5d_20d"] = 1.0

        # ── 11 Cross Features ──
        vol_20 = f["volatility_20d"]
        f["momentum_x_mean_reversion"] = (
            f["trend_5d"] * (1.0 - f["avg_return_20d"] / vol_20) if vol_20 > 0 else 0.0
        )
        f["trend_acceleration"] = f["trend_5d"] - f["trend_10d"]
        f["momentum_quality"] = f["trend_5d"] * f["volume_amp"]
        f["volume_trend_interaction"] = f["volume_ratio_5d_20d"] * f["trend_5d"]
        f["gap_volume_interaction"] = f["gap"] * f["volume_amp"]
        f["strength_persistence"] = f["relative_strength"] * f["consecutive_up_days"]
        f["volatility_adj_return"] = f["avg_return_20d"] / vol_20 if vol_20 > 0 else 0.0
        f["volume_price_momentum"] = f["volume_amp"] * f["open_gain"]
        f["gap_reversion"] = f["gap"] * (f["open_gain"] - f["gap"])
        vr_5_20 = f["volume_ratio_5d_20d"]
        f["trend_volume_divergence"] = f["trend_5d"] / vr_5_20 if vr_5_20 > 0 else 0.0
        f["momentum_stability"] = f["return_consistency"] * f["trend_5d"]

        # Sanitize: replace NaN/Inf with 0
        for k, v in f.items():
            if math.isnan(v) or math.isinf(v):
                f[k] = 0.0

        return f

    @staticmethod
    def compute_all_features(
        codes: set[str],
        snapshots: dict[str, StockSnapshot],
        indicators: dict[str, StockIndicators],
    ) -> dict[str, FeatureVector]:
        """Compute 76 features (38 raw + 38 Z-scored) for all candidates.

        Z-score normalization is done within the day's candidate pool:
            z_i = (x_i - mean) / std

        Args:
            codes: Candidate stock codes after all filters.
            snapshots: {code: StockSnapshot}.
            indicators: {code: StockIndicators}.

        Returns:
            Dict of code → FeatureVector.
        """
        # Compute market-wide open gain (average across ALL snapshots, not just
        # filtered candidates — this represents the true market-level signal)
        gains = [s.early_gain_pct for s in snapshots.values() if s.open_price > 0]
        market_open_gain = statistics.mean(gains) if gains else 0.0

        # Compute raw features for each stock
        raw_features: dict[str, dict[str, float]] = {}
        for code in codes:
            snap = snapshots.get(code)
            ind = indicators.get(code)
            if snap is None or ind is None:
                continue
            raw_features[code] = MLScanner.compute_raw_features(snap, ind, market_open_gain)

        if not raw_features:
            return {}

        # Z-score normalize across candidates
        feature_names = FEATURE_NAMES_RAW
        feature_means: dict[str, float] = {}
        feature_stds: dict[str, float] = {}

        for fname in feature_names:
            vals = [raw_features[c][fname] for c in raw_features]
            feature_means[fname] = statistics.mean(vals)
            feature_stds[fname] = statistics.stdev(vals) if len(vals) >= 2 else 1.0

        result: dict[str, FeatureVector] = {}
        for code, raw in raw_features.items():
            z_scored: dict[str, float] = {}
            for fname in feature_names:
                std = feature_stds[fname]
                if std > 0:
                    z_scored[fname] = (raw[fname] - feature_means[fname]) / std
                else:
                    z_scored[fname] = 0.0
            result[code] = FeatureVector(code=code, raw=raw, z_scored=z_scored)

        logger.info(
            "compute_all_features: %d candidates → %d feature vectors (76 dims)",
            len(codes),
            len(result),
        )
        return result

    # ── ML Inference ────────────────────────────────────────

    @staticmethod
    def score_candidates(
        model: Any,
        features: dict[str, FeatureVector],
    ) -> ScoringResult:
        """Score candidates using a trained LambdaRank model.

        Args:
            model: Trained lgb.Booster from train_model().
            features: {code: FeatureVector} from compute_all_features().

        Returns:
            ScoringResult with candidates ranked by predicted relevance.
        """
        if not features:
            return ScoringResult(ranked=[], feature_importance={})

        codes = list(features.keys())
        feature_matrix = [features[code].to_array(FEATURE_NAMES_RAW) for code in codes]

        scores = model.predict(feature_matrix)

        ranked = sorted(zip(codes, scores), key=lambda x: x[1], reverse=True)

        # Feature importance
        importance_vals = model.feature_importance(importance_type="gain")
        importance = dict(zip(FEATURE_NAMES_ALL, importance_vals))

        logger.info(
            "score_candidates: scored %d candidates, top score=%.4f",
            len(ranked),
            ranked[0][1] if ranked else 0.0,
        )

        return ScoringResult(
            ranked=[(code, float(score)) for code, score in ranked],
            feature_importance=importance,
        )

    @staticmethod
    def load_model(name: str) -> Any:
        """Load a trained model from disk.

        Args:
            name: Model filename (without extension).

        Returns:
            lgb.Booster instance.

        Raises:
            FileNotFoundError: If model file doesn't exist.
            ImportError: If lightgbm is not installed.
        """
        import lightgbm as lgb

        path = _MODEL_DIR / f"{name}.lgb"
        if not path.exists():
            raise FileNotFoundError(f"Model not found: {path}")
        booster = lgb.Booster(model_file=str(path))
        logger.info("load_model: loaded from %s", path)
        return booster

    # ── Full Scan Pipeline ──────────────────────────────────

    async def scan(
        self,
        snapshots: dict[str, StockSnapshot],
        today: date,
        model_name: str = "full_latest",
        suspended: set[str] | None = None,
    ) -> MLScanResult:
        """Run full 8-layer filter + ML scoring pipeline.

        Args:
            snapshots: {code: StockSnapshot} from build_snapshots().
            today: Current trading date.
            model_name: Model file name (without .lgb extension).
            suspended: Today's suspended stock codes (for board data alerts).

        Returns:
            MLScanResult with ranked recommendations.

        Raises:
            FileNotFoundError: If model file doesn't exist.
            ImportError: If lightgbm is not installed.
        """
        funnel: list[FunnelStage] = []

        # Step 0: Build universe (for board mapping)
        universe = await self.build_universe()
        funnel.append(
            FunnelStage(
                "L0_universe",
                "L0 股票池",
                len(universe.codes),
                sorted(universe.codes),
            )
        )

        # Build code → name lookup from universe board_stocks
        code_name: dict[str, str] = {}
        for members in universe.board_stocks.values():
            for code, name in members:
                if code not in code_name:
                    code_name[code] = name

        # Preprocess: compute indicators + IPO filter
        prep = MLScanner.preprocess(snapshots, today)
        funnel.append(
            FunnelStage(
                "L1_preprocess",
                "L1 预处理",
                len(prep.indicators),
                sorted(prep.indicators.keys()),
            )
        )

        # Step 2: Hot board filter
        hot = MLScanner.filter_hot_boards(
            universe.board_stocks,
            snapshots,
            set(prep.indicators.keys()),
            suspended or set(),
        )
        funnel.append(
            FunnelStage(
                "L2_hot_boards",
                "L2 热门板块",
                len(hot.qualified_codes),
                sorted(hot.qualified_codes),
            )
        )

        # Step 3: Early gain filter
        gain_codes = MLScanner.filter_by_early_gain(hot.qualified_codes, snapshots)
        funnel.append(
            FunnelStage(
                "L3_early_gain",
                "L3 早盘涨幅",
                len(gain_codes),
                sorted(gain_codes),
            )
        )

        # Step 4: Price filter
        price_codes = MLScanner.filter_by_price(gain_codes, snapshots)
        funnel.append(
            FunnelStage(
                "L4_price",
                "L4 价格",
                len(price_codes),
                sorted(price_codes),
            )
        )

        # Step 5: Volume amplification filter
        vol_result = MLScanner.filter_by_volume(price_codes, snapshots, prep.indicators)
        funnel.append(
            FunnelStage(
                "L5_volume",
                "L5 量能",
                len(vol_result.passed_codes),
                sorted(vol_result.passed_codes),
            )
        )

        # Step 6: Surge ratio filter
        surge_codes = MLScanner.filter_by_surge_ratio(
            vol_result.passed_codes, snapshots, prep.indicators
        )
        funnel.append(
            FunnelStage(
                "L6_surge",
                "L6 脉冲",
                len(surge_codes),
                sorted(surge_codes),
            )
        )

        # Step 7: Upper shadow filter
        final_codes = MLScanner.filter_by_upper_shadow(surge_codes, snapshots)
        funnel.append(
            FunnelStage(
                "L7_final",
                "L7 上影线",
                len(final_codes),
                sorted(final_codes),
            )
        )

        if not final_codes:
            logger.info("scan: no candidates after filtering, returning empty result")
            return MLScanResult(
                funnel=funnel,
                hot_board_count=len(hot.hot_boards),
                final_candidates=0,
                model_name=model_name,
            )

        # ML scoring: features → model → rank
        features = MLScanner.compute_all_features(set(final_codes), snapshots, prep.indicators)
        model = MLScanner.load_model(model_name)
        scoring = MLScanner.score_candidates(model, features)

        # Build MLScoredStock list from ranked results
        all_scored: list[MLScoredStock] = []
        for code, score in scoring.ranked:
            snap = snapshots[code]
            ind = prep.indicators.get(code)
            amp = 0.0
            if ind and ind.avg_volume_37d > 0:
                expected = ind.avg_volume_37d * MLScanner._EARLY_SESSION_RATIO
                amp = snap.early_volume / expected if expected > 0 else 0.0

            all_scored.append(
                MLScoredStock(
                    stock_code=code,
                    stock_name=code_name.get(code, ""),
                    board_name=hot.best_board.get(code, ""),
                    ml_score=score,
                    open_price=snap.open_price,
                    prev_close=snap.prev_close,
                    latest_price=snap.latest_price,
                    gain_pct=snap.gain_pct,
                    early_gain_pct=snap.early_gain_pct,
                    turnover_amp=round(amp, 4),
                )
            )

        logger.info(
            "scan: %d final candidates, top=%s (%.4f), model=%s",
            len(all_scored),
            all_scored[0].stock_code if all_scored else "-",
            all_scored[0].ml_score if all_scored else 0.0,
            model_name,
        )

        return MLScanResult(
            recommended=all_scored[0] if all_scored else None,
            all_scored=all_scored[:10],
            feature_importance=scoring.feature_importance,
            model_name=model_name,
            funnel=funnel,
            hot_board_count=len(hot.hot_boards),
            final_candidates=len(final_codes),
        )
