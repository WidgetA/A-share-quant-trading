# === MODULE PURPOSE ===
# Aggregate raw 1-min bars into the per-day snapshot used by current strategies.
#
# This is BUSINESS LOGIC. The data pipeline only depends on
# ``MinuteAggregatorProtocol``. If a future strategy needs a different window
# (e.g. 09:30~09:45) or a different reduction (e.g. VWAP instead of cum_volume),
# add a new aggregator implementation here — do NOT touch the data layer.

from __future__ import annotations

from typing import Any, NamedTuple, Protocol


class Snapshot(NamedTuple):
    """Per-day aggregated minute snapshot.

    Field names mirror the legacy ``backtest_minute`` columns:
        close      → close_940
        cum_volume → cum_volume
        max_high   → max_high
        min_low    → min_low
    """

    close: float
    cum_volume: float
    max_high: float
    min_low: float


class MinuteAggregatorProtocol(Protocol):
    """Contract the data pipeline depends on.

    Implementations consume a list of raw 1-min bar dicts (Tushare stk_mins
    format: ``trade_time``, ``open``, ``high``, ``low``, ``close``, ``vol``)
    and return one snapshot per trading day.
    """

    def aggregate(self, bars: list[dict[str, Any]]) -> dict[str, Snapshot]:
        """Return ``{date_str: Snapshot}`` keyed by ``YYYY-MM-DD``."""
        ...


class EarlyWindowAggregator:
    """Aggregate the 09:31~09:40 window into one snapshot per day.

    Used by current momentum / ML strategies as the early-day signal source.
    """

    WINDOW_START = "09:31"
    WINDOW_END = "09:40"

    def aggregate(self, bars: list[dict[str, Any]]) -> dict[str, Snapshot]:
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
            except (ValueError, TypeError, KeyError):
                continue

            existing = out.get(bar_date)
            if existing is None:
                out[bar_date] = Snapshot(close=c, cum_volume=v, max_high=h, min_low=lo)
            else:
                out[bar_date] = Snapshot(
                    close=c,
                    cum_volume=existing.cum_volume + v,
                    max_high=max(existing.max_high, h),
                    min_low=min(existing.min_low, lo),
                )
        return out
