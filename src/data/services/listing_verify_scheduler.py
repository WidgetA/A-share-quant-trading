# === MODULE PURPOSE ===
# Path B: server-side auto-verification of stock_listing_info.
# Each day at 4am (after the 3am cache fill, so stock_snapshot is fresh)
# this scheduler feeds snapshot codes that have no verified list_date yet
# to the container's kimi-cli, and writes the confirmed IPO dates back
# into stock_listing_info. This is what lets audit_daily_gaps compute a
# clean effective universe (see CLAUDE.md §12).
#
# Trading-safety posture: this is a data-quality audit job, not a trading
# path — but the audit must stay reliable. kimi must SearchWeb-confirm the
# date; on any parse/timeout failure we write a verified=false placeholder
# rather than inventing a list_date, and alert via Feishu.

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.data.services.kimi_listing_verifier import (
    KimiToolError,
    kimi_available,
    run_kimi_for_code,
)

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
SCHEDULE_HOUR = 4  # 4am Beijing time — after the 3am cache fill
_STARTUP_DELAY_SECONDS = 90  # wait for storage to initialize
MAX_CODES_PER_RUN = 500  # bound runtime/cost on the 1.58G machine
# 1, NOT more: each kimi run is a full LLM-agent subprocess. The prod box has
# only 1.58G shared with GreptimeDB + trading-service; running several kimi
# processes concurrently risks OOM (and an OOM→restart→startup-run→OOM loop).
# Serial is slow but safe — a multi-hour 4am run is fine.
CONCURRENCY = 1
_UPSERT_BATCH = 200  # GreptimeDB drops rows silently above ~200 per INSERT
_FAILED_SOURCE = "kimi-not-found"
_FEISHU_FAILED_DISPLAY = 50  # cap failed-code list in the Feishu message
# If kimi itself fails (auth/tool error) this many times in a row with zero
# successes, abort the whole run and alert — don't grind through hundreds of
# stocks masking a broken/unauthenticated kimi as "查不到".
_TOOL_ERROR_ABORT = 5


async def _notify_feishu(message: str) -> None:
    """Best-effort Feishu notification, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_message(message)
    except Exception:
        logger.warning("Failed to send Feishu listing-verify notification", exc_info=True)


class ListingVerifyScheduler:
    """Background task that fills stock_listing_info via kimi-cli (path B).

    - On startup: after a short delay, verify a batch.
    - Then: sleep until 4am daily, verify a batch.
    - Per-run capped at MAX_CODES_PER_RUN so a multi-thousand backlog is
      chipped away over several nights rather than blocking one window.

    Usage:
        scheduler = ListingVerifyScheduler(app.state)
        task = asyncio.create_task(scheduler.run())
        # On shutdown:
        task.cancel()
    """

    _LOG_NAME = "listing_verify"

    def __init__(self, app_state) -> None:
        self._app_state = app_state
        self.next_run_time: str | None = None
        self.last_run_time: str | None = None
        self.last_run_result: str | None = None  # success|failed|skipped|no_codes
        self.last_run_message: str | None = None
        self.in_progress: bool = False
        # Last-run counters for the settings card.
        self.last_verified: int = 0
        self.last_failed: int = 0
        self.last_remaining: int = 0
        self.log_lines: list[str] = []

    def _get_storage(self):
        return getattr(self._app_state, "storage", None)

    def _append_log(self, msg: str) -> None:
        ts = datetime.now(BEIJING_TZ).strftime("%H:%M:%S")
        self.log_lines.append(f"[{ts}] {msg}")
        if len(self.log_lines) > 100:
            self.log_lines = self.log_lines[-100:]
        logger.info("ListingVerify: %s", msg)

    def _next_run_str(self) -> str:
        now = datetime.now(BEIJING_TZ)
        target = datetime.combine(now.date(), time(SCHEDULE_HOUR, 0), tzinfo=BEIJING_TZ)
        if now >= target:
            target += timedelta(days=1)
        return target.strftime("%m-%d %H:%M")

    def get_status(self) -> dict:
        """Return scheduler status for the settings page."""
        from src.common.config import get_listing_verify_enabled

        return {
            "enabled": get_listing_verify_enabled(),
            "next_run_time": self.next_run_time,
            "last_run_time": self.last_run_time,
            "last_run_result": self.last_run_result,
            "last_run_message": self.last_run_message,
            "in_progress": self.in_progress,
            "last_verified": self.last_verified,
            "last_failed": self.last_failed,
            "last_remaining": self.last_remaining,
            "log_lines": self.log_lines[-30:],
        }

    # ------------------------------------------------------------------
    # Persistent status (GreptimeDB scheduler_log)
    # ------------------------------------------------------------------

    async def _restore_status_from_db(self) -> None:
        storage = self._get_storage()
        if not storage or not storage.is_ready:
            return
        last = await storage.get_last_scheduler_run(self._LOG_NAME)
        if last:
            self.last_run_time = last["time"]
            self.last_run_result = last["result"]
            self.last_run_message = last["message"]
            logger.info(
                "ListingVerifyScheduler: restored last run from DB: %s %s",
                self.last_run_time,
                self.last_run_result,
            )

    async def _persist_status(self, trigger: str) -> None:
        storage = self._get_storage()
        if not storage or not storage.is_ready:
            return
        await storage.log_scheduler_run(
            self._LOG_NAME,
            trigger,
            self.last_run_result or "unknown",
            self.last_run_message or "",
        )

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        logger.info("ListingVerifyScheduler started, will run daily at 4am Beijing time")
        try:
            await asyncio.sleep(_STARTUP_DELAY_SECONDS)
            await self._restore_status_from_db()

            await self._run_once("startup")

            while True:
                now = datetime.now(BEIJING_TZ)
                target = datetime.combine(now.date(), time(SCHEDULE_HOUR, 0), tzinfo=BEIJING_TZ)
                if now >= target:
                    target += timedelta(days=1)
                self.next_run_time = target.strftime("%Y-%m-%d %H:%M")
                wait_secs = (target - now).total_seconds()
                logger.info(
                    "ListingVerifyScheduler: next run at %s (%.1fh from now)",
                    target.strftime("%Y-%m-%d %H:%M"),
                    wait_secs / 3600,
                )
                await asyncio.sleep(wait_secs)
                await self._run_once("scheduled")
        except asyncio.CancelledError:
            logger.info("ListingVerifyScheduler cancelled")

    async def trigger_manual(
        self,
        include_failed: bool = False,
        codes: set[str] | None = None,
        max_codes: int | None = None,
    ) -> None:
        """Manual one-shot run (settings-page button / API). Bypasses the
        enable toggle since the operator asked for it explicitly, but keeps
        the kimi-availability, cache-fill and in-progress guards.

        If ``codes`` is given, verify exactly those (e.g. the truth-table's
        source_none/orphan backstop set) instead of snapshot-unverified.
        """
        await self._run_once(
            "manual",
            force_enabled=True,
            include_failed=include_failed,
            codes=codes,
            max_codes=max_codes,
        )

    async def _run_once(
        self,
        trigger: str,
        force_enabled: bool = False,
        include_failed: bool = False,
        codes: set[str] | None = None,
        max_codes: int | None = None,
    ) -> None:
        """Execute one verify cycle, with guards. trigger: startup|scheduled|manual."""
        from src.common.config import get_listing_verify_enabled

        run_time = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

        if not force_enabled and not get_listing_verify_enabled():
            logger.info("ListingVerifyScheduler: disabled via settings, skipping (%s)", trigger)
            self.last_run_time = run_time
            self.last_run_result = "skipped"
            self.last_run_message = "已关闭，跳过本次执行"
            await self._persist_status(trigger)
            return

        if not kimi_available():
            self.last_run_time = run_time
            self.last_run_result = "skipped"
            self.last_run_message = "容器内未找到 kimi-cli，跳过"
            logger.warning("ListingVerifyScheduler: kimi-cli not on PATH, skipping (%s)", trigger)
            await self._persist_status(trigger)
            await _notify_feishu("[上市日验证·路径B] 容器内未找到 kimi-cli，本次跳过")
            return

        # Don't compete with a running cache fill for memory on the small box.
        if getattr(self._app_state, "cache_fill_running", False):
            self.last_run_time = run_time
            self.last_run_result = "skipped"
            self.last_run_message = "缓存补全正在运行，跳过本次执行"
            logger.info("ListingVerifyScheduler: cache fill in progress, skipping (%s)", trigger)
            await self._persist_status(trigger)
            return

        if self.in_progress:
            logger.info("ListingVerifyScheduler: a verify run is already active, skipping")
            return

        self.in_progress = True
        try:
            result = await self.verify_unverified(
                include_failed=include_failed, codes=codes, max_codes=max_codes
            )
            self.last_run_time = run_time
            if result.get("error"):
                self.last_run_result = "failed"
                self.last_run_message = result["error"][:120]
            elif result["checked"] == 0:
                self.last_run_result = "no_codes"
                self.last_run_message = "无未验证代码"
            else:
                self.last_run_result = "success"
                self.last_run_message = (
                    f"验证 {result['verified']} / 失败 {result['failed']} "
                    f"/ 剩余 {result['remaining']}"
                )
        except Exception as e:
            logger.error("ListingVerifyScheduler verify failed: %s", e, exc_info=True)
            self.last_run_time = run_time
            self.last_run_result = "failed"
            self.last_run_message = str(e)[:120]
            await _notify_feishu(f"[上市日验证·路径B] 执行异常 ({trigger})\n{e}")
        finally:
            self.in_progress = False
            await self._persist_status(trigger)

    # ------------------------------------------------------------------
    # Core verification
    # ------------------------------------------------------------------

    async def verify_unverified(
        self,
        progress_cb=None,
        include_failed: bool = False,
        codes: set[str] | None = None,
        max_codes: int | None = None,
    ) -> dict:
        """Verify codes via kimi-cli (the listing-date backstop).

        Args:
            progress_cb: optional async callable(msg) for live progress.
            include_failed: also re-verify existing verified=false placeholders.
            codes: explicit code set to verify (e.g. the truth-table's
                source_none/orphan backstop set). If None, defaults to
                snapshot codes lacking a listing_info row.
            max_codes: per-run cap (defaults to MAX_CODES_PER_RUN).

        Returns dict: {checked, verified, failed, remaining, error?}.
        """
        storage = self._get_storage()
        if storage is None or not storage.is_ready:
            return {
                "checked": 0,
                "verified": 0,
                "failed": 0,
                "remaining": 0,
                "error": "GreptimeDB storage 不可用",
            }

        async def _log(msg: str) -> None:
            self._append_log(msg)
            if progress_cb:
                await progress_cb(msg)

        if codes is None:
            codes = await storage.get_unverified_codes_in_snapshot()
            if include_failed:
                codes = codes | await storage.get_failed_verified_codes()
        all_codes = sorted(codes)
        total = len(all_codes)
        if total == 0:
            await _log("无待验证代码，跳过")
            return {"checked": 0, "verified": 0, "failed": 0, "remaining": 0}

        cap = max_codes if max_codes is not None else MAX_CODES_PER_RUN
        batch = all_codes[:cap]
        truncated = total - len(batch)
        remaining = truncated
        if truncated > 0:
            msg = (
                f"本次只验证 {len(batch)}/{total},剩余 {truncated} 下次继续 "
                f"(MAX_CODES_PER_RUN={MAX_CODES_PER_RUN})"
            )
            await _log(msg)
        else:
            await _log(f"待验证 {len(batch)} 只代码 (并发 {CONCURRENCY})")

        # Serial (CONCURRENCY=1 on the small box). Three outcomes per code:
        #   - real date          → verified=True row
        #   - kimi 'not found'   → verified=False placeholder (genuine, kimi searched)
        #   - KimiToolError      → tool/auth failure: DO NOT write a placeholder,
        #                          it's not "查不到". Abort early if it keeps failing.
        verified_entries: list[dict] = []  # rows to upsert (true + genuine not-found)
        not_found_codes: list[str] = []  # kimi searched, genuinely not found
        tool_errors: list[tuple[str, str]] = []  # (code, reason) — kimi itself failed
        verified_n = 0

        def _abort(reason: str, checked: int) -> dict:
            self.last_verified = verified_n
            self.last_failed = len(not_found_codes)
            self.last_remaining = remaining
            return {
                "checked": checked,
                "verified": verified_n,
                "failed": len(not_found_codes),
                "tool_errors": len(tool_errors),
                "remaining": remaining,
                "error": reason,
            }

        for idx, code in enumerate(batch, 1):
            try:
                result = await run_kimi_for_code(code)
            except KimiToolError as e:
                tool_errors.append((code, str(e)))
                # kimi is clearly broken/unauthenticated → stop, don't write
                # garbage placeholders for hundreds of stocks.
                broken = len(tool_errors) >= _TOOL_ERROR_ABORT
                if broken and verified_n == 0 and not not_found_codes:
                    reason = tool_errors[0][1]
                    await _log(f"kimi 连续 {len(tool_errors)} 次工具错误且零成功 → 中止: {reason}")
                    await _notify_feishu(
                        "[上市日验证·路径B] 已中止 — kimi 工具/凭证失败\n"
                        f"前 {len(tool_errors)} 只全部是 kimi 工具错误、0 成功,"
                        "未写任何占位(不是'查不到')。\n"
                        f"原因: {reason}\n"
                        "请检查容器内 kimi 是否已登录 / 凭证是否有效。"
                    )
                    return _abort(f"kimi 工具/凭证失败: {reason}", idx)
                continue
            except Exception as e:  # noqa: BLE001 — unexpected; treat as tool error
                logger.warning("kimi verify %s unexpected error: %s", code, e)
                tool_errors.append((code, f"未预期错误: {e}"))
                continue

            ld = result.get("list_date")
            if isinstance(ld, str) and len(ld) == 10 and ld[4] == "-":
                verified_n += 1
                verified_entries.append(
                    {
                        "code": code,
                        "name": result.get("name"),
                        "list_date": ld,
                        "delist_date": result.get("delist_date"),
                        "verified": True,
                        "source": result.get("source"),
                    }
                )
            else:
                not_found_codes.append(code)
                verified_entries.append(
                    {
                        "code": code,
                        "name": result.get("name"),
                        "list_date": None,
                        "delist_date": None,
                        "verified": False,
                        "source": _FAILED_SOURCE,
                    }
                )
            if idx % 50 == 0:
                await _log(f"进度 {idx}/{len(batch)}")

        # Write back in small batches (GreptimeDB silently drops big INSERTs).
        written = 0
        for i in range(0, len(verified_entries), _UPSERT_BATCH):
            chunk = verified_entries[i : i + _UPSERT_BATCH]
            written += await storage.upsert_listing_info(chunk)

        await _log(
            f"完成: 验证 {verified_n} / 查不到 {len(not_found_codes)} / "
            f"kimi工具错误 {len(tool_errors)} / 写入 {written} / 剩余 {remaining}"
        )

        summary = (
            "[上市日验证·路径B] 本次完成\n"
            f"验证成功: {verified_n}\n"
            f"查不到(已搜过,写占位): {len(not_found_codes)}\n"
            f"kimi工具错误(未写,需排查): {len(tool_errors)}\n"
            f"剩余待验证: {remaining}"
        )
        if not_found_codes:
            shown = not_found_codes[:_FEISHU_FAILED_DISPLAY]
            extra = len(not_found_codes) - len(shown)
            more = f" …(+{extra})" if extra > 0 else ""
            summary += "\n查不到上市日的代码: " + ", ".join(shown) + more
        if tool_errors:
            summary += f"\n⚠ kimi 工具错误样本: {tool_errors[0][1]}"
        await _notify_feishu(summary)

        self.last_verified = verified_n
        self.last_failed = len(not_found_codes)
        self.last_remaining = remaining
        return {
            "checked": len(batch),
            "verified": verified_n,
            "failed": len(not_found_codes),
            "tool_errors": len(tool_errors),
            "remaining": remaining,
        }
