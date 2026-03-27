# === MODULE PURPOSE ===
# Feishu (Lark) bot client for sending alert notifications.
# Used for error alerts and system startup/shutdown notifications.

# === DEPENDENCIES ===
# - httpx: Async HTTP client
# - asyncio: Async sleep for retry delays
# - config: get_feishu_config for environment variable management

# === KEY CONCEPTS ===
# - External Bot Service: Messages sent via intermediary relay service
# - Retry Mechanism: Exponential backoff for reliability
# - Graceful Degradation: Skip if not configured, don't crash

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import httpx

if TYPE_CHECKING:
    from src.strategy.strategies.momentum_sector_scanner import (
        RecommendedStock,
        ScanResult,
        SelectedStock,
    )
    from src.strategy.strategies.v15_scanner import V15ScanResult
    from src.strategy.strategies.v16_scanner import V16ScanResult

from src.common.config import get_feishu_config

logger = logging.getLogger(__name__)

# Beijing timezone for timestamps
BEIJING_TZ = ZoneInfo("Asia/Shanghai")


class FeishuBot:
    """
    Feishu bot client for sending alert messages via external bot service.

    The bot sends messages through an intermediary relay service that handles
    Feishu API authentication. This avoids embedding Feishu SDK dependencies.

    API Protocol:
        POST /api/send
        Payload: {app_id, app_secret, chat_id, message}
        Response: {code: 0, msg: "success"} or {code: non-zero, msg: "error"}

    Usage:
        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_alert("Error Title", "Error details...")
            await bot.send_startup_notification()
    """

    def __init__(
        self,
        bot_url: str | None = None,
        app_id: str | None = None,
        app_secret: str | None = None,
        chat_id: str | None = None,
    ):
        """
        Initialize Feishu bot client.

        Args:
            bot_url: Bot relay service URL (default from FEISHU_BOT_URL env)
            app_id: Feishu app ID (default from FEISHU_APP_ID env)
            app_secret: Feishu app secret (default from FEISHU_APP_SECRET env)
            chat_id: Target chat ID (default from FEISHU_CHAT_ID env)
        """
        config = get_feishu_config()
        self.bot_url = bot_url or config["bot_url"]
        self.app_id = app_id or config["app_id"]
        self.app_secret = app_secret or config["app_secret"]
        self.chat_id = chat_id or config["chat_id"]

    def is_configured(self) -> bool:
        """
        Check if all required credentials are configured.

        Returns:
            True if app_id, app_secret, and chat_id are all set
        """
        return bool(self.app_id and self.app_secret and self.chat_id)

    async def send_message(self, message: str, max_retries: int = 20) -> bool:
        """
        Send a text message to the configured Feishu group with retry mechanism.

        Args:
            message: The message text to send
            max_retries: Maximum number of retry attempts (default: 20)

        Returns:
            True if sent successfully, False otherwise

        Note:
            Uses exponential backoff: 1s, 2s, 4s, 8s, ... capped at 60s
        """
        if not self.is_configured():
            logger.warning("Feishu bot not configured, skipping message")
            return False

        last_error: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.post(
                        f"{self.bot_url}/api/send",
                        json={
                            "app_id": self.app_id,
                            "app_secret": self.app_secret,
                            "chat_id": self.chat_id,
                            "message": message,
                        },
                    )
                    response.raise_for_status()
                    data = response.json()

                    if data.get("code") == 0:
                        if attempt > 0:
                            logger.info(
                                f"Message sent successfully after {attempt} retries: "
                                f"{message[:50]}..."
                            )
                        else:
                            logger.debug(f"Message sent successfully: {message[:50]}...")
                        return True
                    else:
                        # API returned error code, treat as failure
                        last_error = Exception(f"Feishu API error: {data}")
                        logger.warning(
                            f"Feishu API error (attempt {attempt + 1}/{max_retries + 1}): {data}"
                        )

            except httpx.HTTPStatusError as e:
                last_error = e
                logger.warning(
                    f"Feishu HTTP error (attempt {attempt + 1}/{max_retries + 1}): "
                    f"{e.response.status_code} - {e.response.text}"
                )
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Failed to send Feishu message (attempt {attempt + 1}/{max_retries + 1}): {e}"
                )

            # If not the last attempt, wait before retrying with exponential backoff
            if attempt < max_retries:
                # Exponential backoff: 1s, 2s, 4s, 8s, ... capped at 60s
                delay = min(2**attempt, 60)
                logger.debug(f"Retrying in {delay} seconds...")
                await asyncio.sleep(delay)

        # All retries exhausted
        logger.error(
            f"Failed to send Feishu message after {max_retries + 1} attempts. "
            f"Last error: {last_error}"
        )
        return False

    async def send_alert(self, title: str, content: str) -> bool:
        """
        Send an error alert message.

        Args:
            title: Alert title (e.g., "Database Error", "Trading Execution Failed")
            content: Alert content/details

        Returns:
            True if sent successfully, False otherwise
        """
        message = f"🚨 {title}\n\n{content}"
        return await self.send_message(message)

    async def send_startup_notification(
        self,
        git_commit: str | None = None,
        git_branch: str | None = None,
        build_time: str | None = None,
    ) -> bool:
        """
        Send system startup notification with optional version info.

        Args:
            git_commit: Git commit hash (short or full)
            git_branch: Git branch name
            build_time: Build timestamp

        Returns:
            True if sent successfully, False otherwise
        """
        now = datetime.now(BEIJING_TZ)
        lines = [
            "✅ A股交易系统已启动",
            "",
            f"⏰ 启动时间: {now.strftime('%Y-%m-%d %H:%M:%S')}",
        ]

        # Add version info if available
        if git_commit and git_commit != "unknown":
            lines.append(f"📦 版本: {git_commit[:8]}")
        if git_branch and git_branch != "unknown":
            lines.append(f"🌿 分支: {git_branch}")
        if build_time and build_time != "unknown":
            lines.append(f"🔨 构建: {build_time}")

        message = "\n".join(lines)
        return await self.send_message(message)

    async def send_shutdown_notification(self) -> bool:
        """
        Send system shutdown notification.

        Returns:
            True if sent successfully, False otherwise
        """
        now = datetime.now(BEIJING_TZ)
        message = f"⚠️ A股交易系统已停止\n\n⏰ 停止时间: {now.strftime('%Y-%m-%d %H:%M:%S')}"
        return await self.send_message(message)

    async def send_limit_up_skip_notification(
        self,
        sector_name: str,
        limit_up_stocks: list[tuple[str, str]],
        available_stocks: list[tuple[str, str, float, float | None]],
        reason: str,
    ) -> bool:
        """
        Send notification about stocks skipped due to limit-up.

        Args:
            sector_name: Name of the sector/board.
            limit_up_stocks: List of (code, name) tuples for stocks at limit-up.
            available_stocks: List of (code, name, price, change_pct) tuples still buyable.
            reason: Original reason for the trade signal.

        Returns:
            True if sent successfully, False otherwise
        """
        # Format limit-up stocks
        limit_up_str = "\n".join([f"  {code} {name}" for code, name in limit_up_stocks[:5]])
        if len(limit_up_stocks) > 5:
            limit_up_str += f"\n  ... +{len(limit_up_stocks) - 5}"

        # Format available stocks
        if available_stocks:
            available_str = "\n".join(
                [f"  {code} {name} ¥{price:.2f}" for code, name, price, _ in available_stocks[:3]]
            )
            buying_section = f"Buying instead:\n{available_str}"
        else:
            buying_section = "No available stocks, skipping sector."

        message = f"""Limit-Up Skip: {sector_name}

Reason: {reason[:60]}...

Limit-up (skipped):
{limit_up_str}

{buying_section}"""

        return await self.send_message(message)

    async def send_momentum_scan_result(
        self,
        selected_stocks: list[SelectedStock],
        hot_boards: dict[str, list[str]],
        initial_gainer_count: int,
        scan_time: datetime | None = None,
        recommended_stock: RecommendedStock | None = None,
    ) -> bool:
        """
        Send momentum sector strategy scan result.

        Args:
            selected_stocks: List of SelectedStock (from momentum_sector_scanner).
            hot_boards: Dict of board_name → list of initial gainer codes.
            initial_gainer_count: Number of stocks that passed initial >5% filter.
            scan_time: When the scan was performed.
            recommended_stock: RecommendedStock (the top pick), or None.

        Returns:
            True if sent successfully, False otherwise.
        """
        now = scan_time or datetime.now(BEIJING_TZ)
        time_str = now.strftime("%Y-%m-%d %H:%M")

        lines = [
            f"📊 动量板块策略选股 ({time_str})",
            f"初筛: {initial_gainer_count}只(9:40 vs 开盘>0.56%) | 热门板块: {len(hot_boards)}个",
            "",
        ]

        if not selected_stocks:
            lines.append("未筛选到符合条件的股票")
            return await self.send_message("\n".join(lines))

        # Group selected stocks by board
        board_stocks: dict[str, list] = {}
        for stock in selected_stocks:
            board = stock.board_name
            if board not in board_stocks:
                board_stocks[board] = []
            board_stocks[board].append(stock)

        for board_name, stocks in board_stocks.items():
            gainer_count = len(hot_boards.get(board_name, []))
            lines.append(f"🔥 {board_name} ({gainer_count}只触发)")
            for s in stocks:
                pe_diff = s.pe_ttm - s.board_avg_pe
                pe_sign = "+" if pe_diff >= 0 else ""
                lines.append(
                    f"  ✅ {s.stock_code} {s.stock_name}  "
                    f"涨幅{s.open_gain_pct:+.1f}%  "
                    f"PE {s.pe_ttm:.1f} (均值{s.board_avg_pe:.1f} {pe_sign}{pe_diff:.1f})"
                )
            lines.append("")

        lines.append(f"共选出 {len(selected_stocks)} 只标的")

        # Recommendation section
        if recommended_stock:
            rec = recommended_stock
            lines.append("")
            lines.append(f"⭐ 推荐: {rec.stock_code} {rec.stock_name}")
            lines.append(f"  板块: {rec.board_name} (选出{rec.board_stock_count}只，为最多板块)")
            lines.append(
                f"  盘中涨幅: {rec.gain_from_open_pct:+.2f}%  "
                f"换手放大: {rec.turnover_amp:.1f}x  "
                f"综合得分: {rec.composite_score:+.2f}"
            )
            lines.append(f"  开盘涨幅: {rec.open_gain_pct:+.1f}%  PE: {rec.pe_ttm:.1f}")
        else:
            lines.append("")
            lines.append("⭐ 推荐: 无 (今日无符合条件的推荐标的)")

        return await self.send_message("\n".join(lines))

    async def send_v15_top5_report(
        self,
        scan_result: V15ScanResult,
        scan_time: datetime | None = None,
    ) -> bool:
        """Send V15 scanner top-5 scored candidates to Feishu."""
        now = scan_time or datetime.now(BEIJING_TZ)
        time_str = now.strftime("%Y-%m-%d %H:%M")

        lines = [
            f"[V16] 每日扫描报告 ({time_str})",
            (
                f"初筛: {scan_result.initial_gainers_count}只 | "
                f"热门板块: {scan_result.hot_board_count}个"
                + (
                    f"(弱板块过滤: {scan_result.l3_filtered_by_avg_gain}个)"
                    if getattr(scan_result, "l3_filtered_by_avg_gain", 0)
                    else ""
                )
                + f" | L5: {scan_result.l5_count} | L6: {scan_result.l6_count} | "
                f"最终: {scan_result.final_candidates}只"
            ),
        ]

        rec = scan_result.recommended
        if rec:
            lines.append("")
            lines.append(f"推荐: {rec.stock_code} {rec.stock_name}")
            lines.append(
                f"  板块: {rec.board_name} | V3: {rec.v3_score:+.4f} | "
                f"盘中涨: {rec.gain_from_open_pct:+.2f}% | "
                f"换手放大: {rec.turnover_amp:.2f}x"
            )
        else:
            lines.append("")
            lines.append("推荐: 无")

        scored = scan_result.all_scored
        if scored:
            lines.append("")
            lines.append("评分前5:")
            for i, s in enumerate(scored[:5]):
                lines.append(
                    f"{i + 1}. {s.stock_code} {s.stock_name}  "
                    f"V3={s.v3_score:+.4f}  {s.board_name}  "
                    f"涨{s.gain_from_open_pct:+.2f}%  "
                    f"换手{s.turnover_amp:.2f}x  "
                    f"连涨{s.consecutive_up_days}天"
                )

        return await self.send_message("\n".join(lines))

    async def send_v16_top10_report(
        self,
        scan_result: V16ScanResult,
        scan_time: datetime | None = None,
    ) -> bool:
        """Send V16 scanner top-10 scored candidates to Feishu."""
        now = scan_time or datetime.now(BEIJING_TZ)
        time_str = now.strftime("%Y-%m-%d %H:%M")

        lines = [
            f"[V16] 每日扫描报告 ({time_str})",
            (
                f"股票池: {scan_result.step0_universe_count}只 | "
                f"热门板块: {scan_result.step2_hot_board_count}个 | "
                f"最终: {scan_result.final_candidates}只"
            ),
        ]

        recommended = scan_result.recommended
        if recommended:
            top1 = recommended[0]
            board = scan_result.stock_best_board.get(top1.code, "-")
            lines.append("")
            lines.append(f"推荐 Top-1: {top1.code} {top1.name}")
            lines.append(
                f"  板块: {board} | LGB: {top1.score:.4f} | "
                f"价格: {top1.buy_price:.2f}"
            )

            lines.append("")
            lines.append("评分前10:")
            for s in recommended:
                board = scan_result.stock_best_board.get(s.code, "-")
                lines.append(
                    f"{s.rank}. {s.code} {s.name}  "
                    f"LGB={s.score:.4f}  {board}"
                )
        else:
            lines.append("")
            lines.append("推荐: 无")

        return await self.send_message("\n".join(lines))

    async def send_daily_pick_report(
        self,
        scan_result: ScanResult,
        elapsed_seconds: float,
        scan_time: datetime | None = None,
    ) -> bool:
        """
        Send comprehensive daily momentum pick report.

        Includes: recommendation, top 5 by score, hot boards, timing.
        """
        now = scan_time or datetime.now(BEIJING_TZ)
        time_str = now.strftime("%Y-%m-%d %H:%M")

        lines = [
            f"📊 每日动量选股报告 ({time_str})",
            f"⏱️ 计算用时: {elapsed_seconds:.1f}秒",
            (
                f"初筛: {len(scan_result.initial_gainers)}只 | "
                f"入选: {len(scan_result.selected_stocks)}只 | "
                f"热门板块: {len(scan_result.hot_boards)}个"
            ),
        ]

        rec = scan_result.recommended_stock
        if rec:
            lines.append("")
            lines.append(f"⭐ 今日推荐买入: {rec.stock_code} {rec.stock_name}")
            lines.append(
                f"板块: {rec.board_name} | 得分: {rec.composite_score:+.2f} | "
                f"盘中涨: {rec.gain_from_open_pct:+.2f}%"
            )
            lines.append(
                f"换手放大: {rec.turnover_amp:.1f}x | "
                f"开盘涨: {rec.open_gain_pct:+.1f}% | "
                f"现价: {rec.latest_price:.2f}"
            )
        else:
            lines.append("")
            lines.append("⭐ 今日无推荐标的")

        # Top 5 scored candidates
        scored = scan_result.scored_candidates
        if scored:
            lines.append("")
            lines.append("📈 得分前5名:")
            for i, c in enumerate(scored[:5]):
                lines.append(
                    f"{i + 1}. {c.stock_code} {c.stock_name}  "
                    f"{c.composite_score:+.2f}  {c.board_name}  "
                    f"涨{c.gain_from_open_pct:+.2f}%"
                )

        # Hot boards summary
        if scan_result.hot_boards:
            lines.append("")
            sorted_boards = sorted(scan_result.hot_boards.items(), key=lambda x: -len(x[1]))
            board_parts = [f"{name}({len(codes)}只)" for name, codes in sorted_boards[:8]]
            lines.append(f"🔥 热门板块: {' | '.join(board_parts)}")

        return await self.send_message("\n".join(lines))
