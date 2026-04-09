# Business-layer aggregators that turn raw 1-min bars into the snapshot the
# data pipeline persists to backtest_minute.
#
# Each aggregator implements ``MinuteAggregatorProtocol``. The data pipeline
# only depends on the protocol — concrete implementations live here so the
# rules (window start/end, OHLCV reduction) can change without touching the
# data layer.

from src.strategy.aggregators.early_window_aggregator import (
    EarlyWindowAggregator,
    MinuteAggregatorProtocol,
    Snapshot,
)

__all__ = ["EarlyWindowAggregator", "MinuteAggregatorProtocol", "Snapshot"]
