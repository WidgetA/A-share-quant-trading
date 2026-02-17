# === MODULE PURPOSE ===
# Post-selection filter to remove stocks with high "高开低走" (gap-up fade) risk.
# Applied after momentum selection to filter out stocks most likely to
# gap up then drop, causing losses when holding to next-day open.

# === KEY SIGNALS ===
# 1. Intraday strength ratio: fraction of total gain from real buying vs gap
# 2. Open gap ceiling: overly large gaps fade more often
# 3. Multi-day extension: stocks already up significantly tend to mean-revert
# 4. Previous day big gain: follow-through after a huge day is unreliable

# === DATA FLOW ===
# MomentumSectorScanner Step 5 → GapFadeFilter → Step 6 (recommend)

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

from src.data.clients.ifind_http_client import IFinDHttpClient
from src.strategy.strategies.momentum_sector_scanner import (
    PriceSnapshot,
    SelectedStock,
)

logger = logging.getLogger(__name__)


@dataclass
class GapFadeConfig:
    """Configuration for gap-fade filter thresholds."""

    enabled: bool = True

    # Signal 1: Minimum ratio of intraday momentum to total gain.
    # gain_from_open / current_gain_pct — if too low, the stock's apparent
    # strength is mostly the opening gap, not real buying pressure.
    # Example: open +4%, 9:40 +4.6% → ratio=0.56/4.6=12% (mostly gap, risky)
    #          open +0.5%, 9:40 +1.1% → ratio=0.56/1.1=51% (real momentum)
    min_intraday_strength: float = 0.12

    # Signal 2: Maximum open gap percentage.
    # Gaps > this threshold have statistically higher fade rates.
    max_open_gap_pct: float = 5.0

    # Signal 3: Maximum cumulative gain over past N trading days.
    # Extended runs are exhaustion-prone; gap-up is often the final push.
    max_n_day_gain_pct: float = 12.0
    lookback_days: int = 5  # Number of trading days to look back

    # Signal 4: Maximum previous day gain.
    # A huge previous-day move (near limit-up) followed by gap-up often fades.
    max_prev_day_gain_pct: float = 7.0


@dataclass
class FadeRiskResult:
    """Assessment result for a single stock's fade risk."""

    stock_code: str
    filtered_out: bool
    reasons: list[str] = field(default_factory=list)
    # Signal values for logging/debugging
    open_gap_pct: float = 0.0
    intraday_strength: float = 0.0
    prev_day_gain_pct: float | None = None
    n_day_gain_pct: float | None = None


class GapFadeFilter:
    """
    Filters out stocks with high gap-fade risk from momentum selections.

    Uses multiple signals to score each stock's likelihood of 高开低走:
    - Intraday strength ratio (most important): distinguishes real momentum from gap illusion
    - Open gap size: large gaps fill more frequently
    - Multi-day extension: exhaustion detection
    - Previous day gain: post-big-move follow-through is unreliable

    Usage:
        filter = GapFadeFilter(ifind_client)
        kept, assessments = await filter.filter_stocks(selected, snapshots, trade_date)
    """

    def __init__(
        self,
        ifind_client: IFinDHttpClient,
        config: GapFadeConfig | None = None,
    ):
        self._ifind = ifind_client
        self._config = config or GapFadeConfig()

    async def filter_stocks(
        self,
        selected_stocks: list[SelectedStock],
        price_snapshots: dict[str, PriceSnapshot],
        trade_date: date | None = None,
    ) -> tuple[list[SelectedStock], list[FadeRiskResult]]:
        """
        Filter out stocks with high gap-fade risk.

        Args:
            selected_stocks: Stocks selected by momentum scanner (after PE filter).
            price_snapshots: Price data for all stocks.
            trade_date: Trade date (for historical data fetch). None = live mode.

        Returns:
            (kept_stocks, all_assessments) — kept stocks and full risk assessments.
        """
        if not self._config.enabled or not selected_stocks:
            return selected_stocks, []

        # Fetch historical context for multi-day and prev-day signals
        codes = [s.stock_code for s in selected_stocks]
        historical = await self._fetch_historical_context(codes, trade_date)

        kept: list[SelectedStock] = []
        assessments: list[FadeRiskResult] = []

        for stock in selected_stocks:
            snap = price_snapshots.get(stock.stock_code)
            hist = historical.get(stock.stock_code)
            result = self._assess(stock, snap, hist)
            assessments.append(result)

            if result.filtered_out:
                logger.info(
                    f"GapFadeFilter: FILTERED {stock.stock_code} {stock.stock_name} "
                    f"— {'; '.join(result.reasons)}"
                )
            else:
                kept.append(stock)

        logger.info(
            f"GapFadeFilter: {len(kept)}/{len(selected_stocks)} stocks passed "
            f"({len(selected_stocks) - len(kept)} filtered out)"
        )
        return kept, assessments

    def _assess(
        self,
        stock: SelectedStock,
        snap: PriceSnapshot | None,
        hist: dict | None,
    ) -> FadeRiskResult:
        """Assess a single stock's fade risk. Any triggered signal → filtered out."""
        reasons: list[str] = []
        open_gap = snap.open_gain_pct if snap else 0.0
        intraday_strength = 0.0
        prev_day_gain: float | None = None
        n_day_gain: float | None = None

        # Signal 1: Intraday strength ratio
        if snap and snap.current_gain_pct > 0:
            intraday_strength = snap.gain_from_open_pct / snap.current_gain_pct
            if intraday_strength < self._config.min_intraday_strength:
                reasons.append(
                    f"日内动量占比{intraday_strength:.0%}<{self._config.min_intraday_strength:.0%}"
                )

        # Signal 2: Open gap ceiling
        if open_gap > self._config.max_open_gap_pct:
            reasons.append(f"跳空{open_gap:.1f}%>{self._config.max_open_gap_pct:.0f}%")

        # Signal 3: Multi-day extension
        if hist and hist.get("n_day_gain") is not None:
            n_day_gain = hist["n_day_gain"]
            if n_day_gain > self._config.max_n_day_gain_pct:
                reasons.append(
                    f"{self._config.lookback_days}日涨{n_day_gain:.1f}%"
                    f">{self._config.max_n_day_gain_pct:.0f}%"
                )

        # Signal 4: Previous day big gain
        if hist and hist.get("prev_day_gain") is not None:
            prev_day_gain = hist["prev_day_gain"]
            if prev_day_gain > self._config.max_prev_day_gain_pct:
                reasons.append(
                    f"前日涨{prev_day_gain:.1f}%>{self._config.max_prev_day_gain_pct:.0f}%"
                )

        return FadeRiskResult(
            stock_code=stock.stock_code,
            filtered_out=len(reasons) > 0,
            reasons=reasons,
            open_gap_pct=open_gap,
            intraday_strength=intraday_strength,
            prev_day_gain_pct=prev_day_gain,
            n_day_gain_pct=n_day_gain,
        )

    async def _fetch_historical_context(
        self,
        stock_codes: list[str],
        trade_date: date | None = None,
    ) -> dict[str, dict]:
        """
        Fetch past N trading days of close prices for multi-day and prev-day signals.

        Returns dict: stock_code → {
            "prev_day_gain": float | None,  # previous day's gain %
            "n_day_gain": float | None,     # cumulative N-day gain %
        }
        """
        if not stock_codes:
            return {}

        # Determine date range: go back enough calendar days to cover N trading days
        ref_date = trade_date or date.today()
        # lookback_days trading days ≈ lookback_days * 1.5 calendar days + buffer
        calendar_buffer = self._config.lookback_days * 2 + 5
        start = ref_date - timedelta(days=calendar_buffer)
        end = ref_date - timedelta(days=1)  # Up to previous trading day

        result: dict[str, dict] = {}
        batch_size = 50

        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i : i + batch_size]
            codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)

            try:
                data = await self._ifind.history_quotes(
                    codes=codes_str,
                    indicators="close",
                    start_date=start.strftime("%Y-%m-%d"),
                    end_date=end.strftime("%Y-%m-%d"),
                )

                for table_entry in data.get("tables", []):
                    thscode = table_entry.get("thscode", "")
                    bare_code = thscode.split(".")[0] if thscode else ""
                    if not bare_code:
                        continue

                    tbl = table_entry.get("table", {})
                    close_vals = tbl.get("close", [])

                    if not close_vals:
                        continue

                    # close_vals is ordered by date, last entry = most recent (prev day)
                    # Filter out None values
                    closes = [float(v) for v in close_vals if v is not None]
                    if len(closes) < 2:
                        continue

                    context: dict = {}

                    # Previous day gain: last close vs second-to-last close
                    context["prev_day_gain"] = (closes[-1] - closes[-2]) / closes[-2] * 100

                    # N-day cumulative gain: last close vs close N days back
                    lookback = self._config.lookback_days
                    if len(closes) > lookback:
                        context["n_day_gain"] = (
                            (closes[-1] - closes[-(lookback + 1)]) / closes[-(lookback + 1)] * 100
                        )

                    result[bare_code] = context

            except Exception as e:
                logger.warning(f"GapFadeFilter: historical fetch failed for batch: {e}")

        return result
