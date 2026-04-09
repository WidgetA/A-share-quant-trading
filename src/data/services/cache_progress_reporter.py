# === MODULE PURPOSE ===
# Single point of contact for cache pipeline progress reporting:
#
#   - Phase enum (no more magic strings)
#   - Async progress callback wrapper (handles sync + coroutine callbacks)
#   - Feishu notifications (consolidated from 6 inlined sites in the old cache
#     file into one place)
#   - Test-friendly: pass ``progress_cb=None`` and ``feishu=None`` to silence
#     everything
#
# The pipeline depends on ``CacheProgressReporter``. Tests / scripts can pass
# ``NullCacheProgressReporter`` to suppress all output.

from __future__ import annotations

import asyncio
import logging
from datetime import date
from enum import Enum
from typing import Any, Callable, Iterable

from src.common.feishu_bot import FeishuBot

logger = logging.getLogger(__name__)


class Phase(str, Enum):
    """Pipeline phase identifiers (used by progress callbacks).

    The string values are the names emitted to the frontend through
    ``progress_cb(phase, current, total, detail)`` so the JS side can render
    the right step indicator.
    """

    INIT = "init"
    DAILY_RESUME = "daily_resume"
    DAILY = "daily"
    DAILY_BACKFILL = "daily_backfill"
    BACKFILL = "backfill"
    STOCK_LIST = "stock_list"
    MINUTE_RESUME = "minute_resume"
    MINUTE_ACTIVE = "minute_active"
    MINUTE = "minute"
    DOWNLOAD = "download"

    def __str__(self) -> str:  # ensure %s formatting yields the bare phase name
        return self.value


ProgressCallback = Callable[[str, int, int, str], Any]


async def _maybe_await(result: Any) -> None:
    """Await ``result`` if it's a coroutine, otherwise no-op."""
    if asyncio.iscoroutine(result):
        await result


class CacheProgressReporter:
    """Reports pipeline progress to a frontend callback and Feishu.

    Args:
        progress_cb: function called as ``cb(phase, current, total, detail)``
            after each step. Both sync and coroutine functions are supported.
        feishu: optional FeishuBot. Defaults to a new ``FeishuBot()`` if not
            provided. Pass ``None`` to disable Feishu entirely (tests).
    """

    def __init__(
        self,
        progress_cb: ProgressCallback | None = None,
        feishu: FeishuBot | None | object = ...,  # sentinel: default = new bot
    ) -> None:
        self._progress_cb = progress_cb
        if feishu is ...:
            self._feishu = FeishuBot()
        else:
            self._feishu = feishu  # type: ignore[assignment]

    # ------------------------------------------------------------------
    # Progress
    # ------------------------------------------------------------------

    async def progress(self, phase: Phase, current: int, total: int, detail: str = "") -> None:
        """Forward a progress update to the registered callback."""
        if self._progress_cb is None:
            return
        await _maybe_await(self._progress_cb(str(phase), current, total, detail))

    def with_progress_cb(self, progress_cb: ProgressCallback | None) -> "CacheProgressReporter":
        """Return a sibling reporter that shares the Feishu sink but uses a
        different progress callback.

        Used by ``CachePipeline.download_prices`` so each individual download
        request can wire up its own per-call progress channel without losing
        the shared notification configuration.
        """
        return CacheProgressReporter(progress_cb=progress_cb, feishu=self._feishu)

    # ------------------------------------------------------------------
    # Feishu
    # ------------------------------------------------------------------

    async def _send_feishu(self, message: str) -> None:
        if self._feishu is None:
            return
        try:
            if not self._feishu.is_configured():
                return
            await self._feishu.send_message(message)
        except Exception:  # safety: notification failure must not abort downloads
            logger.warning("Feishu send failed (%s)", message[:80], exc_info=True)

    async def notify_suspend_d_failure(self, day_str: str, exc: Exception) -> None:
        await self._send_feishu(
            f"[缓存下载] 严重错误\n"
            f"Tushare suspend_d API 失败 ({day_str})\n"
            f"错误: {str(exc)[:200]}\n"
            f"已中止日线下载，防止写入错误停牌数据"
        )

    async def notify_backfill_suspend_failure(self, day_str: str, exc: Exception) -> None:
        await self._send_feishu(
            f"[缓存回填] 严重错误\n"
            f"Tushare suspend_d API 失败 ({day_str})\n"
            f"错误: {str(exc)[:200]}\n"
            f"已中止回填"
        )

    async def notify_suspended_stocks(self, day_str: str, codes: Iterable[str]) -> None:
        codes_sorted = sorted(codes)
        n = len(codes_sorted)
        if n == 0:
            return
        sample = ", ".join(codes_sorted[:15])
        tail = f" 等{n}只" if n > 15 else ""
        await self._send_feishu(
            f"[缓存下载] 停牌记录\n日期: {day_str}\n停牌: {n} 只\n{sample}{tail}"
        )

    async def notify_null_data(self, day_str: str, codes: list[str]) -> None:
        if not codes:
            return
        sample = ", ".join(codes[:10])
        extra = f" 等{len(codes)}只" if len(codes) > 10 else ""
        await self._send_feishu(
            f"[缓存下载] 数据异常\n"
            f"日期: {day_str}\n"
            f"tsanghi 返回 {len(codes)} 只股票 open/close 为空，但 Tushare 未标记停牌\n"
            f"已跳过: {sample}{extra}"
        )

    async def notify_backfill_summary(self, fixed_dates: int, null_remaining: int) -> None:
        if null_remaining > 0:
            await self._send_feishu(
                f"[缓存回填] ❌ 验证失败\n"
                f"回填 {fixed_dates} 天后仍有 {null_remaining} 行 is_suspended=NULL"
            )
        else:
            await self._send_feishu(
                f"[缓存回填] ✅ 回填完成\n修复 {fixed_dates} 天, is_suspended NULL 剩余: 0"
            )

    async def notify_missing_minute_report(
        self,
        *,
        would_download: list[str],
        no_data_reasons: dict[str, str],
        missing_unknown: list[str],
        daily_count: int,
        minute_count: int,
        daily_dates: int,
        dl_start: date,
        end_date: date,
    ) -> None:
        from collections import Counter

        reason_counts = Counter(r.split(":")[0] for r in no_data_reasons.values() if r)

        lines = [
            "📊 分钟线缺失报告",
            f"日期范围: {dl_start} ~ {end_date}",
            f"日线: {daily_count}只/{daily_dates}天 | 分钟线: {minute_count}只",
            f"缺失: {len(would_download)}只",
            "",
        ]

        if reason_counts:
            lines.append("【缺失原因统计】")
            for reason, count in reason_counts.most_common():
                label = self._reason_label(reason)
                lines.append(f"  {label}: {count}只")
            lines.append("")

        if missing_unknown:
            lines.append(f"【数据丢失(下载成功但刷盘后消失)】{len(missing_unknown)}只:")
            lines.append(", ".join(sorted(missing_unknown)[:50]))
            if len(missing_unknown) > 50:
                lines.append(f"  ...及其他 {len(missing_unknown) - 50} 只")
            lines.append("")

        by_reason: dict[str, list[str]] = {}
        for code in would_download:
            reason = no_data_reasons.get(code, "unknown")
            key = reason.split(":")[0]
            by_reason.setdefault(key, []).append(code)

        for reason_key, codes in sorted(by_reason.items()):
            if reason_key == "unknown":
                continue
            label = self._reason_label(reason_key)
            lines.append(f"【{label}】{len(codes)}只:")
            lines.append(", ".join(sorted(codes)[:50]))
            if len(codes) > 50:
                lines.append(f"  ...及其他 {len(codes) - 50} 只")
            lines.append("")

        message = "\n".join(lines)
        logger.info("Missing minute report:\n%s", message)
        await self._send_feishu(message)

    @staticmethod
    def _reason_label(reason: str) -> str:
        return {
            "api_empty": "API返回空数据",
            "api_error": "API报错",
            "no_0931_0940": "有数据但无09:31~09:40",
            "no_window": "聚合后无窗口数据",
            "all_dates_suspended": "全部日期均为停牌",
            "unknown_exchange": "无法识别交易所",
            "cancelled": "用户取消",
        }.get(reason, reason)


class NullCacheProgressReporter(CacheProgressReporter):
    """No-op reporter for tests / scripts that don't want notifications."""

    def __init__(self) -> None:
        super().__init__(progress_cb=None, feishu=None)
