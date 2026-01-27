# === MODULE PURPOSE ===
# Strategy module for trading signal generation.
# Provides base classes and engine for strategy management.

# === EXPORTS ===
# - BaseStrategy: Abstract base class for all strategies
# - StrategyEngine: Hot-reloadable strategy execution engine
# - TradingSignal, SignalType: Signal data models

from src.strategy.base import BaseStrategy
from src.strategy.signals import SignalType, TradingSignal

__all__ = [
    "BaseStrategy",
    "TradingSignal",
    "SignalType",
]
