# === MODULE PURPOSE ===
# Example strategy implementation for reference.
# Demonstrates how to create a trading strategy.

# === USAGE ===
# This is a template. Copy and modify for your own strategies.
# The strategy is automatically loaded by StrategyEngine.

from typing import AsyncIterator

from src.strategy.base import BaseStrategy, StrategyContext
from src.strategy.signals import SignalType, TradingSignal


class ExampleStrategy(BaseStrategy):
    """
    Example trading strategy.

    This is a simple demonstration strategy that:
    - Shows the basic structure of a strategy
    - Demonstrates signal generation
    - Illustrates configuration usage

    This strategy does NOT generate real trading signals.
    It's meant as a template for developing actual strategies.

    Configuration (in YAML):
        strategies:
          example_strategy:
            enabled: true
            parameters:
              min_confidence: 0.7
              max_position_size: 1000
    """

    @property
    def strategy_name(self) -> str:
        """Unique strategy identifier."""
        return "example_strategy"

    async def generate_signals(
        self,
        context: StrategyContext,
    ) -> AsyncIterator[TradingSignal]:
        """
        Generate trading signals based on context.

        This example implementation:
        1. Checks if we have market data
        2. Analyzes news for relevant keywords
        3. Generates signals based on simple rules

        In a real strategy, you would implement your
        actual trading logic here.
        """
        # Get configuration parameters
        min_confidence = self.get_parameter("min_confidence", 0.5)
        max_position_size = self.get_parameter("max_position_size", 100)

        # Example: Check news for keywords
        for news_item in context.news:
            title = news_item.get("title", "")
            stock_codes = news_item.get("stock_codes", [])

            # Simple keyword-based signal (for demonstration)
            if "利好" in title or "增长" in title:
                for stock_code in stock_codes[:1]:  # Limit to first stock
                    yield TradingSignal(
                        signal_type=SignalType.BUY,
                        stock_code=stock_code,
                        quantity=min(100, max_position_size),
                        strategy_name=self.strategy_name,
                        confidence=min_confidence,
                        reason=f"Positive news: {title[:50]}...",
                        metadata={"news_id": news_item.get("id")},
                    )

            elif "利空" in title or "下降" in title:
                for stock_code in stock_codes[:1]:
                    yield TradingSignal(
                        signal_type=SignalType.SELL,
                        stock_code=stock_code,
                        quantity=min(100, max_position_size),
                        strategy_name=self.strategy_name,
                        confidence=min_confidence,
                        reason=f"Negative news: {title[:50]}...",
                        metadata={"news_id": news_item.get("id")},
                    )

    async def on_load(self) -> None:
        """Called when strategy becomes active."""
        await super().on_load()
        # Initialize any resources here
        # e.g., load ML models, connect to data sources

    async def on_unload(self) -> None:
        """Called before strategy is stopped."""
        await super().on_unload()
        # Clean up resources here

    async def on_reload(self) -> None:
        """Called when strategy file is modified."""
        await super().on_reload()
        # Re-initialize if needed (e.g., reload parameters)

    def validate(self) -> list[str]:
        """Validate strategy configuration."""
        errors = super().validate()

        # Add custom validation
        min_conf = self.get_parameter("min_confidence", 0.5)
        if not 0.0 <= min_conf <= 1.0:
            errors.append("min_confidence must be between 0.0 and 1.0")

        max_pos = self.get_parameter("max_position_size", 100)
        if max_pos <= 0:
            errors.append("max_position_size must be positive")

        return errors
