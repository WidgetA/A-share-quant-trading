# === MODULE PURPOSE ===
# Shared data models used across strategy, filters, and web layers.
# Extracted from momentum_sector_scanner.py to decouple model definitions
# from strategy implementation.

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol, runtime_checkable


# === Protocol for historical data providers ===


@runtime_checkable
class HistoricalDataProvider(Protocol):
    """Duck-typed interface for history_quotes + real_time_quotation.

    Both IQuantHistoricalAdapter and TsanghiHistoricalAdapter implement this.
    """

    async def history_quotes(
        self,
        codes: str,
        indicators: str,
        start_date: str,
        end_date: str,
    ) -> dict: ...

    async def real_time_quotation(
        self,
        codes: str,
        indicators: str,
    ) -> dict: ...


# === Data Models ===


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
    """The top pick across all candidates, ranked by composite score.

    Composite = Z(amp) - cup_days*0.3 - max(0, trend_pct)*0.05
    Higher turnover amplitude ranks higher; consecutive up days penalized.
    - amp: early_volume / avg_daily_volume (9:40 data, no hindsight)
    - cup_days: consecutive up days — soft penalty (each day deducts 0.3 std)
    - trend_pct: 5d cumulative gain — excess trend penalty (×0.05)
    """

    stock_code: str
    stock_name: str
    board_name: str  # Which board it was recommended from
    board_stock_count: int  # How many selected stocks in that board
    open_gain_pct: float  # (open - prev_close) / prev_close %, kept for display
    pe_ttm: float
    board_avg_pe: float
    growth_rate: float = 0.0  # Deprecated: kept for DB compat, no longer used in scoring
    open_price: float = 0.0  # Raw open price
    prev_close: float = 0.0  # Previous close price
    latest_price: float = 0.0  # 9:40 price (buy price for range backtest)
    gain_from_open_pct: float = 0.0  # (9:40 - open) / open %, intraday momentum
    turnover_amp: float = 0.0  # early_volume / avg_daily_volume, 9:40 capital intensity
    composite_score: float = 0.0  # Z(amp) - cup_penalty - trend_penalty
    news_check_passed: bool | None = None  # None=not checked, True=clean, False=had negative
    news_check_detail: str = ""  # LLM reasoning if checked


@dataclass
class ScoredCandidate:
    """A candidate with its composite score from Step 6 ranking."""

    stock_code: str
    stock_name: str
    board_name: str
    composite_score: float
    gain_from_open_pct: float
    turnover_amp: float
    consecutive_up_days: int
    trend_pct: float
    latest_price: float


@dataclass
class ScanResult:
    """Complete result of a momentum sector scan."""

    selected_stocks: list[SelectedStock] = field(default_factory=list)
    # Hot boards: board_name → list of initial gainer codes that triggered it
    hot_boards: dict[str, list[str]] = field(default_factory=dict)
    # Initial gainers that passed Step 1
    initial_gainers: list[str] = field(default_factory=list)
    scan_time: datetime = field(default_factory=datetime.now)
    # Step 6: recommended stock (ranked by composite score)
    recommended_stock: RecommendedStock | None = None
    # Price snapshots for selected stocks (for backfill/export use)
    all_snapshots: dict[str, PriceSnapshot] = field(default_factory=dict)
    # Step 6: all scored candidates sorted by composite score descending
    scored_candidates: list[ScoredCandidate] = field(default_factory=list)

    @property
    def has_results(self) -> bool:
        return len(self.selected_stocks) > 0
