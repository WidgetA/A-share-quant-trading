# === MODULE PURPOSE ===
# Aggregate raw 1-min bars into the per-day snapshot used by current strategies.
#
# This is BUSINESS LOGIC. Strategy services call this directly at query time
# against raw bars read from ``GreptimeBacktestStorage``. The data layer has
# no knowledge of windows or reductions — if a future strategy needs a
# different window (e.g. 09:30~09:45) or a different reduction (e.g. VWAP),
# add a new aggregator here without touching the data layer.

from __future__ import annotations

from typing import Any, NamedTuple


class Snapshot(NamedTuple):
    """Per-day aggregated minute snapshot for the early window."""

    close: float
    cum_volume: float
    cum_amount: float  # cumulative turnover in 元 (yuan)
    max_high: float
    min_low: float


class EarlyWindowAggregator:
    """Aggregate the 09:31~09:38 window into one snapshot per day.

    Uses bars 09:31-09:38 (8 bars). The live scan triggers at 09:39 but
    the 09:39 bar is still forming, so the latest complete bar is 09:38.
    Backtest must match live: use the same 8-bar window.

    Consumes raw 1-min bar dicts in Tushare ``stk_mins`` format
    (``trade_time``, ``open``, ``high``, ``low``, ``close``, ``vol``, ``amount``).
    """

    WINDOW_START = "09:31"
    WINDOW_END = "09:38"

    def aggregate(self, bars: list[dict[str, Any]]) -> dict[str, Snapshot]:
        """Return ``{YYYY-MM-DD: Snapshot}`` for each day present in ``bars``."""
        out: dict[str, Snapshot] = {}
        for bar in bars:
            trade_time = str(bar.get("trade_time", ""))
            if len(trade_time) < 16:
                continue
            bar_date = trade_time[:10]
            bar_time = trade_time[11:16]
            if bar_time < self.WINDOW_START or bar_time > self.WINDOW_END:
                continue
            try:
                h = float(bar["high"])
                lo = float(bar["low"])
                c = float(bar["close"])
                v = float(bar.get("vol", 0))
                a = float(bar.get("amount", 0))
            except (ValueError, TypeError, KeyError):
                continue

            existing = out.get(bar_date)
            if existing is None:
                out[bar_date] = Snapshot(
                    close=c, cum_volume=v, cum_amount=a, max_high=h, min_low=lo
                )
            else:
                out[bar_date] = Snapshot(
                    close=c,
                    cum_volume=existing.cum_volume + v,
                    cum_amount=existing.cum_amount + a,
                    max_high=max(existing.max_high, h),
                    min_low=min(existing.min_low, lo),
                )
        return out
