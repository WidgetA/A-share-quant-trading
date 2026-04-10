# Strategy-layer aggregators that turn raw 1-min bars into per-day snapshots
# at query time. Strategies pull raw bars from ``GreptimeBacktestStorage`` and
# pass them to one of these aggregators — the data layer has no knowledge of
# windows or reductions.

from src.strategy.aggregators.early_window_aggregator import (
    EarlyWindowAggregator,
    Snapshot,
)

__all__ = ["EarlyWindowAggregator", "Snapshot"]
