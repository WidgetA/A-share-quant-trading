# === MODULE PURPOSE ===
# V15 live trading scanner: 7-layer funnel from raw universe to single best stock.
# Completely independent from MomentumSectorScanner (backtest code untouched).
# No LLM filter, no negative news checker. Pure parametric funnel + V3 regression scoring.
#
# === 7-LAYER FUNNEL ===
# L1:   gain_from_open >= 0.26%, main_board+SME, non-ST, price >= 12.0
# L2:   reverse lookup stock → concept boards (local JSON)
# L3:   hot boards (>=2 gainers, avg gain >= 0.80%), exclude blacklist
# L4:   board constituent expansion + L1 gain re-filter
# L5:   volume filter (turnover_amp ∈ [0.498, 6.0], 37d lookback)
# L6:   reversal filter (percentile=95, floor=0.15, min_sample=10)
# L6.5: limit-up filter (open or price_940 at limit)
# L6.6: upper shadow filter (shadow > 3.0%, exempt if gain >= 9.5%)
# L7:   V3 regression scoring → single best stock
#
# === DATA DEPENDENCIES ===
# - PriceSnapshot: from momentum_sector_scanner (reused data model)
# - IQuantHistoricalAdapter: history_quotes (close + volume, 37+ trading days)
# - LocalConceptMapper: board ↔ stock mapping
# - FundamentalsDB: ST detection
# - ReversalFactorFilter: reused with V15-specific config

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

from src.data.sources.local_concept_mapper import LocalConceptMapper
from src.strategy.filters.reversal_factor_filter import (
    ReversalFactorConfig,
    ReversalFactorFilter,
)
from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig

if TYPE_CHECKING:
    from src.data.clients.ifind_http_client import IFinDHttpClient
    from src.data.database.fundamentals_db import FundamentalsDB
    from src.strategy.strategies.momentum_sector_scanner import PriceSnapshot

logger = logging.getLogger(__name__)


# ── Data Models ──────────────────────────────────────────────


@dataclass
class V15ScoredStock:
    """A stock scored by V3 regression."""

    stock_code: str
    stock_name: str
    board_name: str
    latest_price: float
    open_price: float
    prev_close: float
    gain_from_open_pct: float
    turnover_amp: float
    v3_score: float
    # V3 feature values (for logging/debugging)
    trend_10d: float = 0.0
    avg_daily_return: float = 0.0
    intraday_range_940: float = 0.0
    consecutive_up_days: int = 0
    avg_market_open_gain: float = 0.0
    trend_consistency: float = 0.0


@dataclass
class V15ScanResult:
    """Complete result of a V15 scan."""

    recommended: V15ScoredStock | None = None
    all_scored: list[V15ScoredStock] = field(default_factory=list)
    initial_gainers_count: int = 0
    hot_board_count: int = 0
    l3_filtered_by_avg_gain: int = 0
    l4_count: int = 0
    l5_count: int = 0
    l6_count: int = 0
    final_candidates: int = 0
    scan_time: datetime = field(default_factory=datetime.now)


@dataclass
class _L4Stock:
    """Internal: a stock that passed L4 constituent expansion."""

    stock_code: str
    stock_name: str
    board_name: str  # will be corrected to best board later


# ── V15 Scanner ──────────────────────────────────────────────


class V15Scanner:
    """V15 momentum board scanning strategy — 7-layer funnel + V3 scoring.

    Usage:
        scanner = V15Scanner(historical_adapter, fundamentals_db)
        result = await scanner.scan(price_snapshots)
        if result.recommended:
            print(result.recommended.stock_code, result.recommended.v3_score)
    """

    # ── L1 Parameters ──
    GAIN_FROM_OPEN_THRESHOLD = 0.2578  # 0.26% gain from open (in pct, e.g. 0.26)
    MIN_PRICE = 12.0

    # ── L3 Parameters ──
    MIN_STOCKS_PER_BOARD = 2
    MIN_BOARD_AVG_GAIN = 0.80  # avg gain_from_open_pct (%) for hot board qualification
    BOARD_BLACKLIST = frozenset(
        {"物联网", "医疗器械概念", "特高压", "冷链物流", "特钢概念", "三胎概念", "长三角一体化"}
    )

    # ── L5 Parameters ──
    MIN_TURNOVER_AMP = 0.498
    MAX_TURNOVER_AMP = 6.0
    TURNOVER_LOOKBACK_DAYS = 37
    TURNOVER_FRACTION = 0.125  # 10min / 80min trading = 0.125

    # ── L6 Parameters ──
    # (passed to ReversalFactorFilter via config)

    # ── L6.5 Parameters ──
    LIMIT_UP_RATIO = 0.10  # main board ±10%

    # ── L6.6 Parameters ──
    MAX_UPPER_SHADOW = 0.030  # 3.0%
    SHADOW_EXEMPT_GAIN = 9.5  # gain_from_open >= 9.5% exempt

    # ── L7 V3 Coefficients ──
    V3_INTERCEPT = 0.0106
    V3_TREND_10D = -0.1034
    V3_AVG_DAILY_RETURN = 1.0699
    V3_INTRADAY_RANGE = -0.3293
    V3_CONSEC_UP = 0.0089
    V3_MARKET_GAIN = 0.4792
    V3_TREND_CONSISTENCY = 0.002

    def __init__(
        self,
        historical_adapter: IFinDHttpClient,
        fundamentals_db: FundamentalsDB,
        concept_mapper: LocalConceptMapper | None = None,
        stock_filter: StockFilter | None = None,
    ):
        self._hist = historical_adapter
        self._fdb = fundamentals_db
        self._mapper = concept_mapper or LocalConceptMapper()
        # V15: main board + SME, exclude ChiNext + STAR + BSE
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

    # ── Public API ──

    async def scan(
        self,
        price_snapshots: dict[str, PriceSnapshot],
        trade_date: date | None = None,
    ) -> V15ScanResult:
        """Run the 7-layer pipeline on 9:40 snapshots.

        Args:
            price_snapshots: Full universe snapshots (stock_code → PriceSnapshot).
                Used for L1 filtering AND avg_market_open_gain computation.
            trade_date: Reference date for historical data lookback (L5/L6/L7).
                Defaults to today if None. Must be set for backtesting.

        Returns:
            V15ScanResult with recommended stock (or None).
        """
        result = V15ScanResult()

        # L1: gain filter + main board + non-ST + price floor
        gainers = await self._l1_gain_filter(price_snapshots)
        result.initial_gainers_count = len(gainers)
        threshold = self.GAIN_FROM_OPEN_THRESHOLD
        logger.info(f"L1: {len(gainers)} gainers (>={threshold}%, price>={self.MIN_PRICE})")
        if not gainers:
            return result

        # L2: board reverse lookup
        stock_boards = await self._l2_board_lookup(list(gainers.keys()))
        logger.info(f"L2: boards found for {len(stock_boards)} stocks")

        # L3: hot boards (avg gain >= threshold)
        hot_boards, l3_avg_filtered = self._l3_hot_boards(stock_boards, gainers)
        result.hot_board_count = len(hot_boards)
        result.l3_filtered_by_avg_gain = l3_avg_filtered
        logger.info(f"L3: {len(hot_boards)} hot boards")
        if not hot_boards:
            return result

        # L4: constituent expansion + gain re-filter
        selected, snapshots, stock_all_boards = await self._l4_constituent_expansion(
            hot_boards, price_snapshots
        )
        result.l4_count = len(selected)
        logger.info(f"L4: {len(selected)} constituent gainers")
        if not selected:
            return result

        # Fetch historical data ONCE for all L4 survivors (used by L5, L6, L7)
        codes = list({s.stock_code for s in selected})
        hist = await self._fetch_historical(codes, trade_date=trade_date)

        # L5: volume filter
        selected = self._l5_volume_filter(selected, snapshots, hist)
        result.l5_count = len(selected)
        logger.info(f"L5: {len(selected)} passed volume filter")
        if not selected:
            return result

        # L6: reversal filter
        avg_vol_data = {
            c: h["avg_daily_volume"] for c, h in hist.items() if h.get("avg_daily_volume")
        }
        # ReversalFactorFilter expects SelectedStock, adapt _L4Stock
        from src.strategy.strategies.momentum_sector_scanner import SelectedStock

        adapted = [
            SelectedStock(
                stock_code=s.stock_code,
                stock_name=s.stock_name,
                board_name=s.board_name,
                open_gain_pct=0.0,
                pe_ttm=0.0,
                board_avg_pe=0.0,
            )
            for s in selected
        ]
        kept_adapted, _ = await self._reversal.filter_stocks(adapted, snapshots, avg_vol_data)
        kept_codes = {s.stock_code for s in kept_adapted}
        selected = [s for s in selected if s.stock_code in kept_codes]
        result.l6_count = len(selected)
        logger.info(f"L6: {len(selected)} passed reversal filter")
        if not selected:
            return result

        # L6.5: limit-up filter
        selected = self._l6_5_limit_up_filter(selected, snapshots)
        logger.info(f"L6.5: {len(selected)} passed limit-up filter")
        if not selected:
            return result

        # L6.6: upper shadow filter
        selected = self._l6_6_upper_shadow_filter(selected, snapshots)
        logger.info(f"L6.6: {len(selected)} passed upper shadow filter")
        if not selected:
            return result

        result.final_candidates = len(selected)

        # Pick best board per stock (hottest board)
        hot_board_sizes = {b: len(codes_list) for b, codes_list in hot_boards.items()}
        for s in selected:
            boards = stock_all_boards.get(s.stock_code, [s.board_name])
            s.board_name = max(boards, key=lambda b: hot_board_sizes.get(b, 0))

        # L7: V3 scoring
        avg_mkt_gain = self._compute_avg_market_open_gain(price_snapshots)
        scored = self._l7_v3_score(selected, snapshots, hist, avg_mkt_gain)
        result.all_scored = scored

        if scored:
            result.recommended = scored[0]
            rec = result.recommended
            logger.info(
                f"L7: Recommended {rec.stock_code} {rec.stock_name} "
                f"board='{rec.board_name}' score={rec.v3_score:.4f} "
                f"price={rec.latest_price:.2f} gfo={rec.gain_from_open_pct:+.2f}% "
                f"amp={rec.turnover_amp:.2f}x"
            )
            if len(scored) > 1:
                top3 = ", ".join(f"{s.stock_code}(score={s.v3_score:.4f})" for s in scored[:3])
                logger.info(f"L7: Top 3: {top3}")
        else:
            logger.info("L7: No recommendation")

        return result

    # ── L1: Gain Filter ──

    async def _l1_gain_filter(
        self, snapshots: dict[str, PriceSnapshot]
    ) -> dict[str, PriceSnapshot]:
        """L1 + L1.5: gain >= 0.26%, main board, non-ST, price >= 12.0."""
        candidates = {
            code: snap
            for code, snap in snapshots.items()
            if snap.gain_from_open_pct >= self.GAIN_FROM_OPEN_THRESHOLD
            and snap.latest_price >= self.MIN_PRICE
        }
        if not candidates:
            return {}

        # Main board filter
        allowed = self._filter.filter_stocks(list(candidates.keys()))
        candidates = {c: candidates[c] for c in allowed}
        if not candidates:
            return {}

        # ST filter
        non_st = await self._fdb.batch_filter_st(list(candidates.keys()))
        return {c: candidates[c] for c in non_st}

    # ── L2: Board Lookup ──

    async def _l2_board_lookup(self, codes: list[str]) -> dict[str, list[str]]:
        """Reverse lookup: stock → concept boards."""
        return await self._mapper.batch_get_stock_concepts(codes)

    # ── L3: Hot Boards ──

    def _l3_hot_boards(
        self,
        stock_boards: dict[str, list[str]],
        gainers: dict[str, PriceSnapshot],
    ) -> tuple[dict[str, list[str]], int]:
        """Find boards with >= 2 gainers AND avg gain >= threshold, excluding blacklist.

        Returns:
            (hot_boards, filtered_by_avg_gain_count)
        """
        board_to_stocks: dict[str, list[str]] = defaultdict(list)
        for code, boards in stock_boards.items():
            for board in boards:
                if board not in self.BOARD_BLACKLIST:
                    board_to_stocks[board].append(code)

        result: dict[str, list[str]] = {}
        filtered_by_avg = 0
        for b, codes in board_to_stocks.items():
            if len(codes) < self.MIN_STOCKS_PER_BOARD:
                continue
            gains = [gainers[c].gain_from_open_pct for c in codes if c in gainers]
            if not gains:
                continue
            avg_gain = sum(gains) / len(gains)
            if avg_gain < self.MIN_BOARD_AVG_GAIN:
                filtered_by_avg += 1
                logger.info(
                    f"L3: filtered '{b}' ({len(codes)} stocks, "
                    f"avg_gain={avg_gain:.2f}% < {self.MIN_BOARD_AVG_GAIN}%)"
                )
                continue
            result[b] = codes

        return result, filtered_by_avg

    # ── L4: Constituent Expansion ──

    async def _l4_constituent_expansion(
        self,
        hot_boards: dict[str, list[str]],
        price_snapshots: dict[str, PriceSnapshot],
    ) -> tuple[list[_L4Stock], dict[str, PriceSnapshot], dict[str, list[str]]]:
        """Expand hot boards → all constituents → re-filter by gain threshold.

        Returns:
            (selected_stocks, updated_snapshots, stock_all_boards)
        """
        # Get all constituents of hot boards
        board_constituents = await self._mapper.batch_get_board_stocks(list(hot_boards.keys()))

        # Filter by main board + non-ST
        all_codes: set[str] = set()
        filtered: dict[str, list[tuple[str, str]]] = {}
        for board, stocks in board_constituents.items():
            allowed = [(c, n) for c, n in stocks if self._filter.is_allowed(c)]
            filtered[board] = allowed
            for c, _ in allowed:
                all_codes.add(c)

        if all_codes:
            non_st = set(await self._fdb.batch_filter_st(list(all_codes)))
            filtered = {
                b: [(c, n) for c, n in stocks if c in non_st] for b, stocks in filtered.items()
            }
            all_codes &= non_st

        # Fetch prices for constituents not in snapshots
        missing = [c for c in all_codes if c not in price_snapshots]
        if missing:
            extra = await self._fetch_constituent_prices(missing)
            price_snapshots = {**price_snapshots, **extra}

        # Select constituents with gain >= threshold and price >= floor
        selected: list[_L4Stock] = []
        stock_all_boards: dict[str, list[str]] = defaultdict(list)

        for board, stocks in filtered.items():
            for code, name in stocks:
                snap = price_snapshots.get(code)
                if not snap:
                    continue
                if snap.gain_from_open_pct < self.GAIN_FROM_OPEN_THRESHOLD:
                    continue
                if snap.latest_price < self.MIN_PRICE:
                    continue
                selected.append(_L4Stock(stock_code=code, stock_name=name, board_name=board))
                if board not in stock_all_boards[code]:
                    stock_all_boards[code].append(board)

        # Deduplicate: keep one entry per stock
        seen: dict[str, _L4Stock] = {}
        for s in selected:
            if s.stock_code not in seen:
                seen[s.stock_code] = s
        deduped = list(seen.values())

        return deduped, price_snapshots, dict(stock_all_boards)

    # ── L5: Volume Filter ──

    def _l5_volume_filter(
        self,
        selected: list[_L4Stock],
        snapshots: dict[str, PriceSnapshot],
        hist: dict[str, dict],
    ) -> list[_L4Stock]:
        """Filter by turnover amplification: keep [MIN, MAX]."""
        kept: list[_L4Stock] = []
        for s in selected:
            snap = snapshots.get(s.stock_code)
            h = hist.get(s.stock_code)

            if not snap or snap.early_volume <= 0:
                raise RuntimeError(
                    f"L5: missing early_volume for {s.stock_code} ({s.stock_name}). Halting."
                )
            if not h or not h.get("avg_daily_volume") or h["avg_daily_volume"] <= 0:
                raise RuntimeError(
                    f"L5: missing avg_daily_volume for {s.stock_code} ({s.stock_name}). Halting."
                )

            avg_vol = h["avg_daily_volume"]
            expected_early = avg_vol * self.TURNOVER_FRACTION
            turnover_amp = snap.early_volume / expected_early

            if turnover_amp > self.MAX_TURNOVER_AMP:
                logger.info(
                    f"L5: filtered {s.stock_code} ({s.stock_name}): "
                    f"amp={turnover_amp:.2f}x > {self.MAX_TURNOVER_AMP}"
                )
                continue
            if turnover_amp < self.MIN_TURNOVER_AMP:
                logger.info(
                    f"L5: filtered {s.stock_code} ({s.stock_name}): "
                    f"amp={turnover_amp:.2f}x < {self.MIN_TURNOVER_AMP}"
                )
                continue

            kept.append(s)

        return kept

    # ── L6.5: Limit-Up Filter ──

    def _l6_5_limit_up_filter(
        self,
        selected: list[_L4Stock],
        snapshots: dict[str, PriceSnapshot],
    ) -> list[_L4Stock]:
        """Remove stocks at limit-up (open or 9:40 price)."""
        kept: list[_L4Stock] = []
        for s in selected:
            snap = snapshots.get(s.stock_code)
            if not snap or snap.prev_close <= 0:
                raise RuntimeError(f"L6.5: missing snapshot for {s.stock_code}. Halting.")
            limit_price = round(snap.prev_close * (1 + self.LIMIT_UP_RATIO), 2)
            if snap.open_price >= limit_price or snap.latest_price >= limit_price:
                logger.info(
                    f"L6.5: filtered {s.stock_code} ({s.stock_name}): at limit-up {limit_price:.2f}"
                )
                continue
            kept.append(s)
        return kept

    # ── L6.6: Upper Shadow Filter ──

    def _l6_6_upper_shadow_filter(
        self,
        selected: list[_L4Stock],
        snapshots: dict[str, PriceSnapshot],
    ) -> list[_L4Stock]:
        """Remove stocks with large upper shadow, unless near limit-up gain."""
        kept: list[_L4Stock] = []
        for s in selected:
            snap = snapshots.get(s.stock_code)
            if not snap or snap.open_price <= 0:
                raise RuntimeError(f"L6.6: missing snapshot for {s.stock_code}. Halting.")

            body_top = max(snap.open_price, snap.latest_price)
            shadow = (snap.high_price - body_top) / snap.open_price

            if shadow > self.MAX_UPPER_SHADOW:
                if snap.gain_from_open_pct >= self.SHADOW_EXEMPT_GAIN:
                    kept.append(s)  # exempt: strong gainer
                else:
                    logger.info(
                        f"L6.6: filtered {s.stock_code} ({s.stock_name}): "
                        f"shadow={shadow:.3%} > {self.MAX_UPPER_SHADOW:.1%}"
                    )
                    continue
            else:
                kept.append(s)

        return kept

    # ── L7: V3 Regression Scoring ──

    def _l7_v3_score(
        self,
        selected: list[_L4Stock],
        snapshots: dict[str, PriceSnapshot],
        hist: dict[str, dict],
        avg_market_open_gain: float,
    ) -> list[V15ScoredStock]:
        """Score all candidates with V3 regression, return sorted descending."""
        scored: list[V15ScoredStock] = []

        for s in selected:
            snap = snapshots.get(s.stock_code)
            h = hist.get(s.stock_code)
            if not snap or not h:
                raise RuntimeError(f"L7: missing data for {s.stock_code}. Halting.")

            trend_10d = h.get("trend_10d", 0.0)
            avg_ret = h.get("avg_daily_return", 0.0)
            cup = h.get("consecutive_up_days", 0)
            consistency = h.get("trend_consistency", 0.0)

            # intraday_range_940 = (high - low) / open
            intraday_range = 0.0
            if snap.open_price > 0 and snap.high_price > 0 and snap.low_price > 0:
                intraday_range = (snap.high_price - snap.low_price) / snap.open_price

            # turnover_amp (for display, not used in V3 formula)
            avg_vol = h.get("avg_daily_volume", 1.0)
            turnover_amp = (
                snap.early_volume / (avg_vol * self.TURNOVER_FRACTION) if avg_vol > 0 else 0.0
            )

            # V3 regression
            v3 = (
                self.V3_INTERCEPT
                + self.V3_TREND_10D * trend_10d
                + self.V3_AVG_DAILY_RETURN * avg_ret
                + self.V3_INTRADAY_RANGE * intraday_range
                + self.V3_CONSEC_UP * cup
                + self.V3_MARKET_GAIN * avg_market_open_gain
                + self.V3_TREND_CONSISTENCY * consistency
            )

            scored.append(
                V15ScoredStock(
                    stock_code=s.stock_code,
                    stock_name=s.stock_name,
                    board_name=s.board_name,
                    latest_price=snap.latest_price,
                    open_price=snap.open_price,
                    prev_close=snap.prev_close,
                    gain_from_open_pct=snap.gain_from_open_pct,
                    turnover_amp=turnover_amp,
                    v3_score=v3,
                    trend_10d=trend_10d,
                    avg_daily_return=avg_ret,
                    intraday_range_940=intraday_range,
                    consecutive_up_days=cup,
                    avg_market_open_gain=avg_market_open_gain,
                    trend_consistency=consistency,
                )
            )

        scored.sort(key=lambda x: x.v3_score, reverse=True)
        return scored

    # ── Helpers ──

    def _compute_avg_market_open_gain(self, snapshots: dict[str, PriceSnapshot]) -> float:
        """Average (open - prev_close) / prev_close across all universe stocks."""
        gains: list[float] = []
        for snap in snapshots.values():
            if snap.prev_close > 0 and snap.open_price > 0:
                gains.append((snap.open_price - snap.prev_close) / snap.prev_close)
        if not gains:
            return 0.0
        return sum(gains) / len(gains)

    async def _fetch_historical(
        self,
        codes: list[str],
        trade_date: date | None = None,
    ) -> dict[str, dict]:
        """Fetch historical close + volume for L5/L6/L7.

        Returns:
            dict: code → {
                "avg_daily_volume": float,  # mean volume over lookback
                "trend_10d": float,         # 10-trading-day price change (decimal)
                "avg_daily_return": float,  # mean of daily returns (decimal)
                "volatility": float,        # std of daily returns
                "consecutive_up_days": int,
                "trend_consistency": float, # avg_ret / (vol + 0.001)
                "is_new_listing": bool,
            }
        """
        if not codes:
            return {}

        ref_date = trade_date or date.today()
        # 37 trading days ≈ 55 calendar days; add buffer
        calendar_buffer = self.TURNOVER_LOOKBACK_DAYS * 2 + 15
        start = ref_date - timedelta(days=calendar_buffer)
        end = ref_date - timedelta(days=1)  # up to previous day only

        result: dict[str, dict] = {}
        batch_size = 50

        for i in range(0, len(codes), batch_size):
            batch = codes[i : i + batch_size]
            codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)

            data = await self._hist.history_quotes(
                codes=codes_str,
                indicators="close,volume",
                start_date=start.strftime("%Y-%m-%d"),
                end_date=end.strftime("%Y-%m-%d"),
            )

            for table_entry in data.get("tables", []):
                thscode = table_entry.get("thscode", "")
                bare_code = thscode.split(".")[0] if thscode else ""
                if not bare_code:
                    continue

                tbl = table_entry.get("table", {})
                time_vals = tbl.get("time", [])
                close_vals = tbl.get("close", [])
                vol_vals = tbl.get("volume", [])

                entry: dict = {"is_new_listing": False}

                # Detect new listing
                if time_vals:
                    from datetime import datetime as _dt

                    first_date = _dt.strptime(time_vals[0], "%Y-%m-%d").date()
                    entry["is_new_listing"] = (ref_date - first_date).days < 30

                # Parse closes
                closes = [float(c) for c in close_vals if c is not None]

                # avg_daily_volume (37d lookback)
                volumes = [float(v) for v in vol_vals if v is not None and float(v) > 0]
                if volumes:
                    recent_vol = volumes[-self.TURNOVER_LOOKBACK_DAYS :]
                    entry["avg_daily_volume"] = sum(recent_vol) / len(recent_vol)

                if len(closes) >= 2:
                    # Daily returns
                    returns = [
                        (closes[j] - closes[j - 1]) / closes[j - 1]
                        for j in range(1, len(closes))
                        if closes[j - 1] > 0
                    ]

                    if returns:
                        entry["avg_daily_return"] = sum(returns) / len(returns)
                        if len(returns) >= 2:
                            mean_r = entry["avg_daily_return"]
                            var = sum((r - mean_r) ** 2 for r in returns) / len(returns)
                            entry["volatility"] = var**0.5
                        else:
                            entry["volatility"] = 0.0
                        entry["trend_consistency"] = entry["avg_daily_return"] / (
                            entry["volatility"] + 0.001
                        )

                    # trend_10d: close[-1] vs close[-11]
                    if len(closes) >= 11:
                        c_now = closes[-1]
                        c_10ago = closes[-11]
                        if c_10ago > 0:
                            entry["trend_10d"] = (c_now - c_10ago) / c_10ago

                    # consecutive_up_days
                    cup = 0
                    for j in range(len(closes) - 1, 0, -1):
                        if closes[j] > closes[j - 1]:
                            cup += 1
                        else:
                            break
                    entry["consecutive_up_days"] = cup

                result[bare_code] = entry

        return result

    async def _fetch_constituent_prices(self, codes: list[str]) -> dict[str, PriceSnapshot]:
        """Fetch realtime prices for constituents not in initial snapshots."""
        from src.strategy.strategies.momentum_sector_scanner import PriceSnapshot

        result: dict[str, PriceSnapshot] = {}
        batch_size = 50

        for i in range(0, len(codes), batch_size):
            batch = codes[i : i + batch_size]
            codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)

            data = await self._hist.real_time_quotation(
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
                    if not open_vals[0] or not prev_vals[0] or not latest_vals[0]:
                        continue
                    open_price = float(open_vals[0])
                    prev_close = float(prev_vals[0])
                    latest = float(latest_vals[0])
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

        return result
