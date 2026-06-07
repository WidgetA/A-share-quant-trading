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
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from src.data.services.kimi_listing_verifier import (
    KimiToolError,
    finding_from_result,
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
# A 迁号/换号 old code is dead: identity + history belong to the new code (which Tushare
# stock_basic carries). We write the old code as a list_date=None "已迁移" marker so
# roster_for_day excludes it (NOT a verified live listing row — that caused the source_none
# loop), and record old→new in code_alias. load_listing drops it entirely on the next reload.
_MIGRATED_SOURCE = "kimi-migrated"
_FEISHU_FAILED_DISPLAY = 50  # cap failed-code list in the Feishu message
# If kimi itself fails (auth/tool error) this many times in a row with zero
# successes, abort the whole run and alert — don't grind through hundreds of
# stocks masking a broken/unauthenticated kimi as "查不到".
_TOOL_ERROR_ABORT = 5
# Observability: keep each code's FULL raw kimi output (the tool trace —
# SearchWeb/FetchURL/Shell calls + results) so a "查不到" can be inspected
# (GET /api/audit/listing-info/kimi-raw?code=) instead of guessed at.
KIMI_RAW_DIR = Path("data/audit/kimi_raw")
# kimi's PARSED, human-readable verdict per code ("怎么回事" — name/status/note/
# dates). This is what flows into the report the user reads and what
# GET /api/audit/listing-info/findings returns. One JSON file per code,
# overwritten on re-verify (latest verdict wins).
KIMI_FINDINGS_DIR = Path("data/audit/kimi_findings")
# How many per-code "怎么回事" lines to inline in the Feishu report before
# pointing at the full list (keeps the message under Feishu's size limit).
_FEISHU_FINDINGS_DISPLAY = 30


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

    @staticmethod
    def _save_finding(finding: dict) -> None:
        """Persist kimi's plain-Chinese verdict for one code (best-effort).

        One JSON file per code under KIMI_FINDINGS_DIR, overwritten on
        re-verify. Read back by GET /api/audit/listing-info/findings so the
        user can see "怎么回事" per code without re-running kimi."""
        try:
            import json

            KIMI_FINDINGS_DIR.mkdir(parents=True, exist_ok=True)
            code = finding.get("code", "unknown")
            (KIMI_FINDINGS_DIR / f"{code}.json").write_text(
                json.dumps(finding, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            logger.warning("Failed to persist kimi finding", exc_info=True)

    def _next_run_str(self) -> str:
        now = datetime.now(BEIJING_TZ)
        target = datetime.combine(now.date(), time(SCHEDULE_HOUR, 0), tzinfo=BEIJING_TZ)
        if now >= target:
            target += timedelta(days=1)
        return target.strftime("%m-%d %H:%M")

    def get_status(self) -> dict:
        """Return verify status for the settings page (no on/off toggle anymore —
        kimi runs as step ② of the 3am pipeline; ``available`` reflects whether it CAN)."""
        return {
            "available": kimi_available(),
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
        """Manual one-shot run (settings-page button / API). Keeps the
        kimi-availability, cache-fill and in-progress guards.

        If ``codes`` is given, verify exactly those (e.g. the truth-table's
        source_none/orphan backstop set) instead of snapshot-unverified.
        """
        await self._run_once(
            "manual",
            include_failed=include_failed,
            codes=codes,
            max_codes=max_codes,
        )

    async def _run_once(
        self,
        trigger: str,
        include_failed: bool = False,
        codes: set[str] | None = None,
        max_codes: int | None = None,
    ) -> None:
        """Execute one verify cycle, with guards. trigger: scheduled|manual.

        No on/off toggle: kimi runs whenever available — if it can't, that's an
        alert, not a silent skip (see module docstring + the 3am pipeline step ②).
        """
        run_time = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

        if not kimi_available():
            self.last_run_time = run_time
            self.last_run_result = "skipped"
            self.last_run_message = "容器内未找到 kimi-cli，跳过"
            logger.warning("ListingVerifyScheduler: kimi-cli not on PATH, skipping (%s)", trigger)
            await self._persist_status(trigger)
            await _notify_feishu("【阶段一·索引建设｜上市日核验】 容器内未找到 kimi-cli，本次跳过")
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
            await _notify_feishu(f"【阶段一·索引建设｜上市日核验】 执行异常 ({trigger})\n{e}")
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
        quiet: bool = False,
    ) -> dict:
        """Verify codes via kimi-cli (the listing-date backstop).

        Args:
            progress_cb: optional async callable(msg) for live progress.
            include_failed: also re-verify existing verified=false placeholders.
            codes: explicit code set to verify (e.g. the truth-table's
                source_none/orphan backstop set). If None, defaults to
                snapshot codes lacking a listing_info row.
            max_codes: per-run cap (defaults to MAX_CODES_PER_RUN).
            quiet: suppress the standalone per-run Feishu success summary (the
                daily-maintenance pipeline folds kimi's result into its one
                unified message). Tool/auth-failure aborts still alert regardless.

        Returns dict: {checked, verified, failed, remaining, findings, error?}.
        ``findings`` = kimi's plain-Chinese "怎么回事" per code (for the caller's report).
        """
        storage = self._get_storage()
        if storage is None or not storage.is_ready:
            return {
                "checked": 0,
                "verified": 0,
                "failed": 0,
                "remaining": 0,
                "findings": [],
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
            return {"checked": 0, "verified": 0, "failed": 0, "remaining": 0, "findings": []}

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
        findings: list[dict] = []  # kimi's plain-Chinese "怎么回事" per code → report
        verified_n = 0
        migrated_n = 0  # 迁号/换号 old codes — written as excluded markers + code_alias

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
                "findings": findings,
                "error": reason,
            }

        for idx, code in enumerate(batch, 1):
            try:
                # raw_dir → keep the full tool trace per code for observability
                result = await run_kimi_for_code(code, raw_dir=KIMI_RAW_DIR)
            except KimiToolError as e:
                tool_errors.append((code, str(e)))
                # kimi is clearly broken/unauthenticated → stop, don't write
                # garbage placeholders for hundreds of stocks.
                broken = len(tool_errors) >= _TOOL_ERROR_ABORT
                if broken and verified_n == 0 and not not_found_codes:
                    reason = tool_errors[0][1]
                    await _log(f"kimi 连续 {len(tool_errors)} 次工具错误且零成功 → 中止: {reason}")
                    await _notify_feishu(
                        "【阶段一·索引建设｜上市日核验】 已中止 — kimi 工具/凭证失败\n"
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

            # kimi's plain-Chinese verdict ("怎么回事") — persist + collect for
            # the report so the user reads kimi's own explanation, not a guess.
            finding = finding_from_result(code, result)
            findings.append(finding)
            self._save_finding(finding)

            nc = result.get("new_code")
            ld = result.get("list_date")
            is_migration = isinstance(nc, str) and len(nc) == 6 and nc.isdigit() and nc != code
            if is_migration:
                # 迁号/换号:老码作废,身份+历史归新码(新码由 stock_basic 在册)。绝不把老码当
                # 正常上市码写"在册行"——那正是 source_none 死循环的根。写成 list_date=None 的
                # 已迁移占位 → roster_for_day 直接不纳入在册;old→new 由下面的 code_alias 记。
                migrated_n += 1
                verified_entries.append(
                    {
                        "code": code,
                        "name": result.get("name"),
                        "list_date": None,
                        "delist_date": None,
                        "verified": False,
                        "source": _MIGRATED_SOURCE,
                    }
                )
            elif isinstance(ld, str) and len(ld) == 10 and ld[4] == "-":
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

        # Auto-maintain the code-change map: when kimi reports a 迁号/更名换号 with a
        # concrete new code, record old→new so future ingestion re-keys it (the
        # "接住未来的重组换号" mechanism). Additive + safe — only real 6-digit pairs.
        aliases: list[dict] = []
        for f in findings:
            if not f.get("new_code"):
                continue
            dd = f.get("delist_date")  # use the migration date as the change date
            change_date = (
                date.fromisoformat(dd)
                if isinstance(dd, str) and len(dd) == 10 and dd[4] == "-"
                else None
            )
            aliases.append(
                {
                    "old_code": f["code"],
                    "new_code": f["new_code"],
                    "change_date": change_date,
                    "note": f.get("note"),
                    "source": "kimi",
                }
            )
        alias_written = await storage.upsert_code_alias(aliases) if aliases else 0

        await _log(
            f"完成: 验证 {verified_n} / 迁号 {migrated_n} / 查不到 {len(not_found_codes)} / "
            f"kimi工具错误 {len(tool_errors)} / 写入 {written} / 换号映射 {alias_written} / "
            f"剩余 {remaining}"
        )

        ok_emoji = "✅" if tool_errors == [] else "⚠️"
        summary = (
            f"【阶段一·索引建设·核对股票身份】{ok_emoji} 完成\n"
            "任务: 让大模型(kimi)逐只查清这些代码到底是什么、现在什么情况(在交易 / 已退市 / "
            "迁去新代码 / 更名),并写清上市日,以便把名单修准\n"
            f"结果: 查清 {len(findings)} 只 / 其中确认上市日并写入 {verified_n} 只 / "
            f"迁号换码(老码出名单、记对应表) {migrated_n} 只 / "
            f"搜过仍查不清 {len(not_found_codes)} 只 / "
            f"出错(超时或认证,未写) {len(tool_errors)} 只 / 还剩 {remaining} 只待核\n"
        )
        # kimi 自己查到的"怎么回事",逐条放进报告给用户看(这才是放 kimi 上去的目的)。
        if findings:
            summary += "\n逐只情况(kimi 查证):"
            for f in findings[:_FEISHU_FINDINGS_DISPLAY]:
                name = f.get("name") or "(名字未知)"
                line = f"\n· {f['code']} {name}｜{f['status']}｜{f['note']}"
                if f.get("list_date"):
                    line += f"(上市 {f['list_date']}"
                    if f.get("delist_date"):
                        line += f"、退市/迁移 {f['delist_date']}"
                    line += ")"
                summary += line
            extra = len(findings) - min(len(findings), _FEISHU_FINDINGS_DISPLAY)
            if extra > 0:
                summary += f"\n…另有 {extra} 只,完整清单见设置页 / 「逐只情况」接口"
        if tool_errors:
            summary += f"\n⚠ kimi 工具错误样本: {tool_errors[0][1]}"
        # quiet=True when called inside the daily-maintenance pipeline — it folds
        # kimi's result into its one unified Feishu, so don't double-send here.
        if not quiet:
            await _notify_feishu(summary)

        self.last_verified = verified_n
        self.last_failed = len(not_found_codes)
        self.last_remaining = remaining
        return {
            "checked": len(batch),
            "verified": verified_n,
            "migrated": migrated_n,
            "failed": len(not_found_codes),
            "tool_errors": len(tool_errors),
            "remaining": remaining,
            "findings": findings,
        }
