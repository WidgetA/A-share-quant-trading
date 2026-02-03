# === MODULE PURPOSE ===
# User interaction for trading confirmations via command line.
# Supports premarket review, intraday confirmation, and morning sell decisions.

# === KEY CONCEPTS ===
# - Async input: Uses asyncio for non-blocking input
# - Timeout: Defaults to safe action if user doesn't respond
# - Formatted display: Clear presentation of trading signals

import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.strategy.analyzers.news_analyzer import NewsSignal
    from src.trading.holding_tracker import HoldingRecord

logger = logging.getLogger(__name__)


@dataclass
class InteractionConfig:
    """Configuration for user interaction."""

    # Timeouts (seconds)
    premarket_timeout: float = 300.0  # 5 minutes for premarket review
    intraday_timeout: float = 60.0  # 1 minute for intraday confirmation
    morning_timeout: float = 300.0  # 5 minutes for morning sell review

    # Default actions on timeout
    premarket_default: str = "skip"  # Skip all if timeout
    intraday_default: bool = False  # Don't buy if timeout
    morning_default: str = "sell_all"  # Sell all if timeout


class UserInteraction:
    """
    Command line interaction for trading decisions.

    Provides formatted display of signals and holdings,
    and collects user input for buy/sell decisions.

    Usage:
        ui = UserInteraction()

        # Premarket review
        selected = await ui.premarket_review(signals)

        # Intraday confirmation
        confirmed = await ui.intraday_confirm(signal)

        # Morning sell confirmation
        slots_to_sell = await ui.morning_confirmation(holdings)
    """

    def __init__(self, config: InteractionConfig | None = None):
        """
        Initialize user interaction.

        Args:
            config: Interaction configuration.
        """
        self._config = config or InteractionConfig()

    async def premarket_review(
        self,
        signals: list["NewsSignal"],
    ) -> list["NewsSignal"]:
        """
        Present premarket signals for user selection.

        Displays a formatted list of signals and allows user
        to select which stocks to buy.

        Args:
            signals: List of NewsSignals to review.

        Returns:
            List of selected NewsSignals for buying.
        """
        if not signals:
            print("\n📊 盘前分析结果: 无利好信号")
            return []

        # Display header
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        print(f"\n{'=' * 60}")
        print(f"📊 盘前分析结果 ({now})")
        print(f"{'=' * 60}")

        # Display signals
        for i, signal in enumerate(signals, 1):
            self._display_signal(i, signal)

        print(f"{'=' * 60}")
        print(f"共 {len(signals)} 个利好信号")
        print()

        # Get user selection
        try:
            selection = await self._get_input_with_timeout(
                "请选择要买入的项目 (逗号分隔，如 1,2,3; 输入 'all' 全选; 输入 'skip' 跳过): ",
                timeout=self._config.premarket_timeout,
            )

            if selection is None or selection.lower() == "skip":
                print("⏭️ 跳过盘前买入")
                return []

            if selection.lower() == "all":
                print(f"✅ 选择全部 {len(signals)} 个信号")
                return signals

            # Parse selection
            selected_indices = self._parse_selection(selection, len(signals))
            selected = [signals[i - 1] for i in selected_indices]

            if selected:
                print(f"✅ 选择了 {len(selected)} 个信号")
            else:
                print("⏭️ 未选择任何信号")

            return selected

        except asyncio.TimeoutError:
            print(f"\n⏱️ 超时 ({self._config.premarket_timeout}秒)，跳过盘前买入")
            return []

    async def intraday_confirm(
        self,
        signal: "NewsSignal",
    ) -> bool:
        """
        Ask user to confirm an intraday buy signal.

        Args:
            signal: NewsSignal to confirm.

        Returns:
            True if user confirms buy.
        """
        print(f"\n{'=' * 50}")
        print("🔔 盘中利好信号!")
        print(f"{'=' * 50}")

        self._display_signal(1, signal)

        print(f"{'=' * 50}")

        try:
            response = await self._get_input_with_timeout(
                "是否买入? [Y/n]: ",
                timeout=self._config.intraday_timeout,
            )

            if response is None:
                print(f"⏱️ 超时，使用默认: {'买入' if self._config.intraday_default else '跳过'}")
                return self._config.intraday_default

            # Empty or 'y'/'Y' means yes
            confirmed = response.strip().lower() in ("", "y", "yes", "是")

            if confirmed:
                print("✅ 确认买入")
            else:
                print("⏭️ 跳过")

            return confirmed

        except asyncio.TimeoutError:
            print(f"\n⏱️ 超时 ({self._config.intraday_timeout}秒)，跳过买入")
            return self._config.intraday_default

    async def morning_confirmation(
        self,
        holdings: list["HoldingRecord"],
    ) -> list[int]:
        """
        Ask user which holdings to sell at market open.

        Args:
            holdings: List of HoldingRecords to review.

        Returns:
            List of slot_ids to sell.
        """
        if not holdings:
            print("\n📋 无隔夜持仓需要处理")
            return []

        # Display header
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        print(f"\n{'=' * 60}")
        print(f"📋 隔夜持仓确认 ({now})")
        print(f"{'=' * 60}")

        # Display holdings
        for i, holding in enumerate(holdings, 1):
            self._display_holding(i, holding)

        print(f"{'=' * 60}")
        print(f"共 {len(holdings)} 个持仓")
        print()

        try:
            selection = await self._get_input_with_timeout(
                "请选择要卖出的持仓 (逗号分隔; 输入 'all' 全部卖出; 输入 'hold' 全部继续持有): ",
                timeout=self._config.morning_timeout,
            )

            if selection is None:
                # Timeout - use default action
                if self._config.morning_default == "sell_all":
                    print("⏱️ 超时，执行默认操作: 全部卖出")
                    return [h.slot_id for h in holdings]
                else:
                    print("⏱️ 超时，执行默认操作: 继续持有")
                    return []

            if selection.lower() == "all":
                print("📤 卖出全部持仓")
                return [h.slot_id for h in holdings]

            if selection.lower() == "hold":
                print("📥 继续持有全部")
                return []

            # Parse selection
            selected_indices = self._parse_selection(selection, len(holdings))
            slots_to_sell = [holdings[i - 1].slot_id for i in selected_indices]

            if slots_to_sell:
                print(f"📤 卖出 {len(slots_to_sell)} 个持仓")
            else:
                print("📥 继续持有全部")

            return slots_to_sell

        except asyncio.TimeoutError:
            print(f"\n⏱️ 超时 ({self._config.morning_timeout}秒)，执行全部卖出")
            return [h.slot_id for h in holdings]

    def _display_signal(self, index: int, signal: "NewsSignal") -> None:
        """Display a single signal with formatting."""
        analysis = signal.analysis
        if not analysis:
            print(f"\n[{index}] 无分析结果")
            print(f"    标题: {signal.message.title[:50]}...")
            return

        confidence_pct = int(analysis.confidence * 100)

        # Sentiment display
        sentiment_map = {
            "strong_bullish": "🚀 强利好",
            "bullish": "📈 利好",
            "neutral": "➖ 中性",
            "bearish": "📉 利空",
            "strong_bearish": "💥 强利空",
        }
        sentiment_display = sentiment_map.get(analysis.sentiment.value, "📰 未知")

        # Action display
        action_map = {
            "buy_stock": "个股",
            "buy_sector": "板块",
        }
        action = action_map.get(signal.recommended_action, "")

        # Targets
        if signal.target_stocks:
            targets = ", ".join(signal.target_stocks[:3])
            if len(signal.target_stocks) > 3:
                targets += f" +{len(signal.target_stocks) - 3}"
        elif signal.target_sectors:
            targets = ", ".join(signal.target_sectors)
        else:
            targets = "无"

        print(f"\n[{index}] {sentiment_display} | 置信度: {confidence_pct}% | {action}")
        print(f"    目标: {targets}")
        print(f"    标题: {signal.message.title[:50]}...")
        print(f"    理由: {analysis.reasoning[:80]}..." if analysis.reasoning else "    理由: 无")

    def _display_holding(self, index: int, holding: "HoldingRecord") -> None:
        """Display a single holding with formatting."""
        # P&L display
        pnl_str = ""
        if holding.pnl_percent is not None:
            if holding.pnl_percent >= 0:
                pnl_str = f"📈 +{holding.pnl_percent:.2f}%"
            else:
                pnl_str = f"📉 {holding.pnl_percent:.2f}%"

        # Price display
        price_str = f"成本: ¥{holding.entry_price:.2f}"
        if holding.current_price:
            price_str += f" → 现价: ¥{holding.current_price:.2f}"

        # Entry time
        entry_time_str = holding.entry_time.strftime("%m-%d %H:%M")

        print(f"\n[{index}] {holding.stock_code} {holding.stock_name or ''}")
        print(f"    数量: {holding.quantity}股 | {price_str} {pnl_str}")
        print(f"    入场: {entry_time_str} | 原因: {holding.entry_reason[:30]}")

    async def _get_input_with_timeout(
        self,
        prompt: str,
        timeout: float,
    ) -> str | None:
        """
        Get user input with timeout.

        Args:
            prompt: Input prompt to display.
            timeout: Timeout in seconds.

        Returns:
            User input string or None if timeout.
        """
        print(prompt, end="", flush=True)

        try:
            # Use asyncio to read input with timeout
            loop = asyncio.get_event_loop()

            # Create a future for input
            future = loop.run_in_executor(None, sys.stdin.readline)

            # Wait with timeout
            result = await asyncio.wait_for(future, timeout=timeout)
            return result.strip()

        except asyncio.TimeoutError:
            print()  # New line after timeout
            return None

    def _format_change_pct(self, change_pct: float | None) -> str:
        """Format change percentage for display."""
        if change_pct is None:
            return ""
        if change_pct >= 0:
            return f"📈 +{change_pct:.2f}%"
        else:
            return f"📉 {change_pct:.2f}%"

    def _parse_selection(self, selection: str, max_index: int) -> list[int]:
        """
        Parse user selection string into indices.

        Args:
            selection: User input (e.g., "1,2,3" or "1-3" or "1,3-5")
            max_index: Maximum valid index.

        Returns:
            List of valid indices (1-based).
        """
        indices = set()

        parts = selection.replace(" ", "").split(",")
        for part in parts:
            if not part:
                continue

            if "-" in part:
                # Range: "1-3"
                try:
                    start, end = part.split("-", 1)
                    start_idx = int(start)
                    end_idx = int(end)
                    for i in range(start_idx, end_idx + 1):
                        if 1 <= i <= max_index:
                            indices.add(i)
                except ValueError:
                    continue
            else:
                # Single number
                try:
                    idx = int(part)
                    if 1 <= idx <= max_index:
                        indices.add(idx)
                except ValueError:
                    continue

        return sorted(indices)

    def display_message(self, message: str, level: str = "info") -> None:
        """
        Display a message to the user.

        Args:
            message: Message to display.
            level: Message level (info, warning, error).
        """
        prefix_map = {
            "info": "ℹ️",
            "warning": "⚠️",
            "error": "❌",
            "success": "✅",
        }
        prefix = prefix_map.get(level, "")
        print(f"{prefix} {message}")

    async def confirm_limit_up_situation(
        self,
        sector_name: str,
        total_stocks: int,
        limit_up_stocks: list[tuple[str, str]],  # [(code, name), ...]
        # [(code, name, price, change_pct), ...]
        available_stocks: list[tuple[str, str, float, float | None]],
    ) -> list[tuple[str, str, float, float | None]] | None:
        """
        Ask user to confirm buying when many stocks in sector are at limit-up.

        Args:
            sector_name: Name of the sector/board.
            total_stocks: Total number of stocks in sector.
            limit_up_stocks: List of (code, name) tuples for stocks at limit-up.
            available_stocks: List of (code, name, price, change_pct) tuples for buyable stocks.
                change_pct is the percentage change from previous close (can be None).

        Returns:
            List of (code, name, price, change_pct) tuples user selected to buy,
            or None if user chooses to skip entirely.
        """
        print(f"\n{'=' * 60}")
        print(f"⚠️  板块涨停提醒: {sector_name}")
        print(f"{'=' * 60}")

        # Show limit-up stocks
        limit_up_count = len(limit_up_stocks)
        limit_up_pct = limit_up_count / total_stocks * 100 if total_stocks > 0 else 0

        print(f"\n🔒 已涨停 ({limit_up_count}/{total_stocks}, {limit_up_pct:.0f}%):")
        for code, name in limit_up_stocks[:5]:
            print(f"    {code} {name} [涨停]")
        if len(limit_up_stocks) > 5:
            print(f"    ... 及其他 {len(limit_up_stocks) - 5} 只")

        # Show available stocks
        available_count = len(available_stocks)
        if available_count == 0:
            print("\n❌ 板块内所有股票均已涨停，无法买入")
            print("=" * 60)
            await self._get_input_with_timeout("按回车继续...", timeout=10.0)
            return None

        print(f"\n✅ 可买入 ({available_count} 只):")
        for i, (code, name, price, change_pct) in enumerate(available_stocks, 1):
            change_str = self._format_change_pct(change_pct)
            print(f"    [{i}] {code} {name} ¥{price:.2f} {change_str}")

        print(f"\n{'=' * 60}")

        try:
            selection = await self._get_input_with_timeout(
                "选择要买入的股票 (逗号分隔; 输入 'skip' 放弃本板块): ",
                timeout=self._config.intraday_timeout,
            )

            if selection is None or selection.lower() in ("skip", "n", "no", ""):
                print("⏭️ 跳过本板块")
                return None

            if selection.lower() == "all":
                print(f"✅ 买入全部 {available_count} 只可用股票")
                return available_stocks

            # Parse selection
            selected_indices = self._parse_selection(selection, available_count)
            if not selected_indices:
                print("⏭️ 未选择，跳过本板块")
                return None

            selected = [available_stocks[i - 1] for i in selected_indices]
            print(f"✅ 选择了 {len(selected)} 只股票")
            return selected

        except asyncio.TimeoutError:
            print("\n⏱️ 超时，跳过本板块")
            return None

    async def notify_limit_up_skip(
        self,
        sector_name: str,
        limit_up_stocks: list[tuple[str, str]],  # [(code, name), ...]
        # [(code, name, price, change_pct), ...]
        available_stocks: list[tuple[str, str, float, float | None]],
        reason: str,
    ) -> None:
        """
        Notify user that some stocks were skipped because they opened at limit-up.

        This is an informational notification during morning auction execution.
        It lets users know which stocks from their premarket selections were
        not bought due to opening at limit-up prices.

        Args:
            sector_name: Name of the sector/board.
            limit_up_stocks: List of (code, name) tuples for stocks at limit-up.
            available_stocks: List of (code, name, price, change_pct) tuples still buyable.
            reason: Original reason for the trade signal.
        """
        print(f"\n{'=' * 60}")
        print(f"⚠️  开盘涨停提醒: {sector_name}")
        print(f"{'=' * 60}")

        # Show why we were trying to buy
        print(f"\n📰 原因: {reason[:60]}...")

        # Show limit-up stocks that were skipped
        print("\n🔒 以下股票开盘涨停，已跳过买入:")
        for code, name in limit_up_stocks:
            print(f"    ❌ {code} {name} [开盘涨停]")

        # Show what we're buying instead (if any)
        if available_stocks:
            print("\n✅ 改为买入:")
            for code, name, price, change_pct in available_stocks[:3]:
                change_str = self._format_change_pct(change_pct)
                print(f"    ✓ {code} {name} ¥{price:.2f} {change_str}")
        else:
            print("\n❌ 该板块所有候选股票均涨停，放弃买入")

        print(f"{'=' * 60}\n")

        # Brief pause to let user see the notification
        await asyncio.sleep(0.5)

    @property
    def config(self) -> InteractionConfig:
        """Get current configuration."""
        return self._config
