# === MODULE PURPOSE ===
# Audit-related endpoints:
#   1. POST /api/audit/kimi-credentials/upload
#      Receive the kimi-cli OAuth credentials JSON (~/.kimi/credentials/
#      kimi-code.json) so the container's kimi-cli can use it without a
#      browser. The ONLY way to refresh the server-side kimi auth — driven
#      by the PowerShell helper scripts/kimi_login_and_upload.ps1 (which
#      runs `kimi login` locally and POSTs the resulting credentials).
#   2. GET /api/audit/listing-info/status
#      Read-only summary of the stock_listing_info table — surfaced in
#      the settings page so the operator can see "how much of stock_snapshot
#      has been verified by server-side kimi". The actual writes happen
#      server-side once the kimi-cli auto-verification path is wired up
#      (no upload endpoint — verification is owned by the server).
#
# kimi-credentials upload is guarded by X-API-Key (verify_trading_api_key).

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse

from src.web.routes import verify_trading_api_key

logger = logging.getLogger(__name__)


# Container path where kimi-cli looks for OAuth credentials.
# Override with KIMI_CREDENTIALS_PATH for non-default deploys.
def _kimi_credentials_path() -> Path:
    import os

    override = os.environ.get("KIMI_CREDENTIALS_PATH")
    if override:
        return Path(override)
    return Path.home() / ".kimi" / "credentials" / "kimi-code.json"


def _validate_kimi_credentials(payload: dict[str, Any]) -> str | None:
    """Return a human-readable error message, or None if the credentials
    JSON looks structurally valid (we can't actually probe Moonshot
    without burning a request)."""
    required = ("access_token", "refresh_token", "expires_at", "scope", "token_type")
    missing = [k for k in required if k not in payload]
    if missing:
        return f"缺少必需字段: {', '.join(missing)}"
    if payload.get("token_type") != "Bearer":
        return f"token_type 应为 'Bearer'，收到 {payload.get('token_type')!r}"
    if payload.get("scope") != "kimi-code":
        return f"scope 应为 'kimi-code'，收到 {payload.get('scope')!r}"
    for tok in ("access_token", "refresh_token"):
        v = payload.get(tok)
        if not isinstance(v, str) or len(v) < 50:
            return f"{tok} 看起来不是合法的 OAuth token"
    return None


def create_audit_router() -> APIRouter:
    """Routes under /api/audit/* for uploading kimi credentials + listing info."""
    router = APIRouter(prefix="/api/audit", tags=["audit"])

    # ------------------------------------------------------------------
    # Kimi OAuth credentials upload
    # ------------------------------------------------------------------

    @router.post(
        "/kimi-credentials/upload",
        dependencies=[Depends(verify_trading_api_key)],
    )
    async def upload_kimi_credentials(file: UploadFile) -> JSONResponse:
        """Receive a kimi-code.json OAuth credentials file and place it at
        the location kimi-cli expects.

        kimi-cli will auto-refresh access_token via refresh_token; only when
        the refresh_token itself expires (typically 7-30d) does this need
        to be re-uploaded.
        """
        try:
            raw = await file.read()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"读取上传文件失败: {e}")

        if not raw:
            raise HTTPException(status_code=400, detail="文件为空")

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"非合法 JSON: {e}")

        err = _validate_kimi_credentials(payload)
        if err:
            raise HTTPException(status_code=400, detail=f"credentials 校验失败: {err}")

        dest = _kimi_credentials_path()
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(raw)
        except Exception as e:
            logger.error("写 kimi credentials 失败: %s", e, exc_info=True)
            raise HTTPException(status_code=500, detail=f"写入失败: {e}")

        import time

        expires_at = float(payload.get("expires_at", 0))
        access_remaining = max(0, int(expires_at - time.time()))
        return JSONResponse(
            {
                "success": True,
                "message": "Kimi 凭证已写入容器",
                "destination": str(dest),
                "access_token_seconds_remaining": access_remaining,
            }
        )

    # ------------------------------------------------------------------
    # Listing info upload (kimi verification result)
    # ------------------------------------------------------------------

    @router.get("/listing-info/status")
    async def listing_info_status(request: Request) -> JSONResponse:
        """Coverage stats + path-B scheduler status for the settings card."""
        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            return JSONResponse({"ready": False})
        try:
            info = await storage.get_listing_info_all()
        except Exception as e:
            logger.warning("listing_info_status 读取失败: %s", e, exc_info=True)
            return JSONResponse({"ready": False, "error": str(e)})

        verified = sum(1 for v in info.values() if v.get("verified"))
        payload: dict[str, Any] = {
            "ready": True,
            "total_codes": len(info),
            "verified_codes": verified,
            "unverified_codes": len(info) - verified,
        }
        scheduler = getattr(request.app.state, "listing_verify_scheduler", None)
        if scheduler is not None:
            payload["scheduler"] = scheduler.get_status()
        return JSONResponse(payload)

    @router.post(
        "/listing-info/verify",
        dependencies=[Depends(verify_trading_api_key)],
    )
    async def trigger_listing_verify(request: Request) -> JSONResponse:
        """Trigger an immediate path-B verify run in the background.

        Does NOT receive offline verification results — verification is owned
        by the server-side kimi-cli; this only kicks the scheduler. Pass
        ``?include_failed=1`` to also re-verify existing verified=false rows.
        """
        scheduler = getattr(request.app.state, "listing_verify_scheduler", None)
        if scheduler is None:
            raise HTTPException(status_code=503, detail="验证调度器未就绪")
        if scheduler.in_progress:
            return JSONResponse({"success": False, "message": "验证正在进行中，请稍后"})
        include_failed = request.query_params.get("include_failed") in ("1", "true", "yes")
        asyncio.create_task(scheduler.trigger_manual(include_failed=include_failed))
        return JSONResponse(
            {"success": True, "message": "已触发后台验证", "include_failed": include_failed}
        )

    # ------------------------------------------------------------------
    # TEMPORARY: 新旧索引对照验证 (POST /api/audit/index-compare)
    # 触发后台任务,把新索引(三合一-kimi)逐日跟旧索引(Tushare bak_basic)对照,
    # 结果发飞书。用来在「索引驱动修复」上线前确认新索引可信。确认完即可删此段
    # + scripts/compare_index_old_new.py。
    # ------------------------------------------------------------------

    @router.post("/index-compare")
    async def trigger_index_compare(request: Request) -> JSONResponse:
        """Kick a background old-vs-new index comparison; result → Feishu.

        TEMPORARY tool — intentionally un-gated (read-only compare + Feishu),
        no X-API-Key needed, so it can be triggered freely while we validate
        the new index. Delete with the rest of the index-compare code.

        Query params: start / end (YYYY-MM-DD, optional), max_days (default 30).
        """
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo

        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="GreptimeDB storage 未就绪")
        if getattr(request.app.state, "index_compare_running", False):
            return JSONResponse({"success": False, "message": "对照任务正在进行中，请稍后"})

        qp = request.query_params
        today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        start = qp.get("start") or (today - timedelta(days=45)).isoformat()
        end = qp.get("end") or today.isoformat()
        try:
            max_days = int(qp.get("max_days", "30"))
        except ValueError:
            max_days = 30

        async def _run() -> None:
            from scripts.compare_index_old_new import (
                _notify_feishu,
                compare_index_range,
                format_feishu_summary,
            )
            from src.common.config import get_tushare_token
            from src.data.clients.tushare_realtime import (
                TushareRealtimeClient,
                get_tushare_trade_calendar,
            )

            request.app.state.index_compare_running = True
            client = TushareRealtimeClient(token=get_tushare_token())
            try:
                cal = await get_tushare_trade_calendar(start, end)
                dates = sorted(datetime.strptime(s, "%Y-%m-%d").date() for s in cal)
                if len(dates) > max_days:
                    logger.warning(
                        "index-compare: %d 交易日超过 max_days=%d, 只对照最近 %d 天",
                        len(dates),
                        max_days,
                        max_days,
                    )
                    dates = dates[-max_days:]
                if not dates:
                    await _notify_feishu("[新旧索引对照] 无可对照的交易日")
                    return
                await client.start()
                result = await compare_index_range(storage, client, dates)
                await _notify_feishu(format_feishu_summary(result))
            except Exception as e:
                logger.error("index-compare 失败: %s", e, exc_info=True)
                await _notify_feishu(f"[新旧索引对照] 执行异常\n{e}")
            finally:
                await client.stop()
                request.app.state.index_compare_running = False

        asyncio.create_task(_run())
        return JSONResponse(
            {"success": True, "message": "已触发后台对照", "start": start, "end": end}
        )

    # ------------------------------------------------------------------
    # stock_snapshot 历史回填 (POST /api/audit/snapshot-backfill)
    # 把索引基表 stock_snapshot 回填到历史区间(默认 2023-01-01~昨天)。
    # 只跑 snapshot(B∪D∪S 三源并集),不碰分钟线;resume-safe(跳已存在日期);
    # 撞 Tushare 限频自动 sleep 后靠 resume 续跑。后台执行,起止发飞书。
    # ------------------------------------------------------------------

    @router.post("/snapshot-backfill")
    async def trigger_snapshot_backfill(request: Request) -> JSONResponse:
        """Backfill stock_snapshot over a historical range (snapshot-only)."""
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo

        storage = getattr(request.app.state, "storage", None)
        pipeline = getattr(request.app.state, "pipeline", None)
        if storage is None or pipeline is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="storage/pipeline 未就绪")
        if getattr(request.app.state, "snapshot_backfill_running", False):
            return JSONResponse({"success": False, "message": "snapshot 回填正在进行中，请稍后"})
        if getattr(request.app.state, "cache_fill_running", False):
            return JSONResponse(
                {"success": False, "message": "缓存补全正在运行，待其结束后再试(避免抢内存)"}
            )

        qp = request.query_params
        today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        start = qp.get("start") or "2023-01-01"
        end = qp.get("end") or (today - timedelta(days=1)).isoformat()

        async def _run() -> None:
            from scripts.compare_index_old_new import _notify_feishu
            from src.data.clients.tushare_realtime import get_tushare_trade_calendar

            request.app.state.snapshot_backfill_running = True
            try:
                cal = await get_tushare_trade_calendar(start, end)
                dates = sorted(datetime.strptime(s, "%Y-%m-%d").date() for s in cal)
                if not dates:
                    await _notify_feishu("[snapshot 回填] 无交易日,跳过")
                    return

                async def _missing() -> int:
                    existing = await storage.get_existing_snapshot_dates()
                    return sum(1 for d in dates if d not in existing)

                miss0 = await _missing()
                await _notify_feishu(
                    f"[snapshot 回填] 开始 {start}~{end}: 目标 {len(dates)} 天, "
                    f"待回填 {miss0} 天(只跑 snapshot,resume-safe)"
                )

                # daily_source + metadata_source must be entered (httpx clients);
                # minute_source is NOT needed for snapshot.
                stale = 0
                async with pipeline.daily_source, pipeline.metadata_source:
                    while True:
                        prev = await _missing()
                        if prev == 0:
                            break
                        try:
                            # resume-safe: re-running skips already-synced dates,
                            # so a rate-limit mid-run just continues next round.
                            await pipeline._sync_stock_snapshot(dates, None)
                        except Exception as e:  # noqa: BLE001 — backfill, keep going
                            logger.warning("snapshot-backfill 中断(resume 续跑): %s", e)
                            await asyncio.sleep(20)
                        now_missing = await _missing()
                        if now_missing >= prev:  # no progress this round
                            stale += 1
                            if stale >= 5:
                                logger.error(
                                    "snapshot-backfill 连续 5 轮无进展,停止 (还缺 %d 天)",
                                    now_missing,
                                )
                                break
                            await asyncio.sleep(30)
                        else:
                            stale = 0

                done = len(dates) - await _missing()
                await _notify_feishu(
                    f"[snapshot 回填] 完成 {start}~{end}: 目标 {len(dates)} 天, "
                    f"已入库 {done} 天, 仍缺 {len(dates) - done} 天"
                )
            except Exception as e:
                logger.error("snapshot-backfill 失败: %s", e, exc_info=True)
                await _notify_feishu(f"[snapshot 回填] 异常\n{e}")
            finally:
                request.app.state.snapshot_backfill_running = False

        asyncio.create_task(_run())
        return JSONResponse(
            {"success": True, "message": "已触发 snapshot 回填", "start": start, "end": end}
        )

    # ------------------------------------------------------------------
    # 数据缺口诊断报告 (POST /api/audit/diagnose-gaps)
    # 把每日补全报的三类问题逐条核查,产出"问题→根因→该多少→怎么修"报告,发飞书。
    # ------------------------------------------------------------------

    @router.post("/diagnose-gaps")
    async def trigger_diagnose_gaps(request: Request) -> JSONResponse:
        """Run the gap-diagnosis report in the background; result → Feishu."""
        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="GreptimeDB storage 未就绪")
        if getattr(request.app.state, "diagnose_gaps_running", False):
            return JSONResponse({"success": False, "message": "诊断正在进行中，请稍后"})

        # Bounded by default so a manual trigger can't blow Tushare's 500/min
        # limit (suspend_d per daily-gap day + stk_mins per missing minute stock).
        # Override via query params for a deliberate bigger run.
        def _qint(name: str, default: int) -> int:
            try:
                return int(request.query_params.get(name, str(default)))
            except ValueError:
                return default

        daily_detail_days = _qint("daily_detail_days", 30)
        minute_detail_days = _qint("minute_detail_days", 30)
        max_minute_source_checks = _qint("max_minute_source_checks", 60)

        async def _run() -> None:
            from scripts.diagnose_gaps import _notify_feishu, run_diagnosis_report

            request.app.state.diagnose_gaps_running = True
            try:
                await run_diagnosis_report(
                    storage,
                    feishu=True,
                    daily_detail_days=daily_detail_days,
                    minute_detail_days=minute_detail_days,
                    max_minute_source_checks=max_minute_source_checks,
                )
            except Exception as e:
                logger.error("diagnose-gaps 失败: %s", e, exc_info=True)
                await _notify_feishu(f"[数据诊断报告] 执行异常\n{e}")
            finally:
                request.app.state.diagnose_gaps_running = False

        asyncio.create_task(_run())
        return JSONResponse({"success": True, "message": "已触发数据诊断,结果发飞书"})

    # ------------------------------------------------------------------
    # 灌权威上市索引 (POST /api/audit/listing-info/load-tushare)
    # 用 Tushare stock_basic(在市+已退市 2 次调用)把每只票的上市/退市日灌进
    # stock_listing_info,建成「一查即知」的权威 list。无需 kimi。
    # ------------------------------------------------------------------

    @router.post("/listing-info/load-tushare")
    async def trigger_load_listing(request: Request) -> JSONResponse:
        """Build the queryable listing index from Tushare stock_basic (background)."""
        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="GreptimeDB storage 未就绪")
        if getattr(request.app.state, "load_listing_running", False):
            return JSONResponse({"success": False, "message": "正在灌入中,请稍后"})

        async def _run() -> None:
            from scripts.load_listing_from_tushare import run_load_listing

            request.app.state.load_listing_running = True
            try:
                await run_load_listing(storage, feishu=True)
            except Exception as e:
                logger.error("load-tushare listing 失败: %s", e, exc_info=True)
                from scripts.load_listing_from_tushare import _notify_feishu

                await _notify_feishu(f"[上市索引] 灌入失败\n{e}")
            finally:
                request.app.state.load_listing_running = False

        asyncio.create_task(_run())
        return JSONResponse({"success": True, "message": "已触发:从 Tushare 灌权威上市索引"})

    # ------------------------------------------------------------------
    # 只补日线的历史回填 (POST /api/audit/backfill-daily)
    # 日线按交易日整批拉(自带主板/创业板/科创板/北交所),所以重下即可补全
    # 那些"只存了主板"的历史日子。分钟线 NOT 触碰(单独跑,免得拖垮小机器)。
    # 范围 ?start=YYYY-MM-DD&end=YYYY-MM-DD,默认 2024-01-01 ~ 昨天。
    # ------------------------------------------------------------------

    @router.post("/backfill-daily")
    async def trigger_backfill_daily(request: Request) -> JSONResponse:
        """Daily-only historical backfill (all boards), minute untouched."""
        from datetime import date, datetime, timedelta
        from zoneinfo import ZoneInfo

        storage = getattr(request.app.state, "storage", None)
        pipeline = getattr(request.app.state, "pipeline", None)
        if storage is None or pipeline is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="GreptimeDB storage/pipeline 未就绪")
        if getattr(request.app.state, "backfill_daily_running", False):
            return JSONResponse({"success": False, "message": "日线补全正在进行中,请稍后"})

        def _qdate(name: str, default: date) -> date:
            v = request.query_params.get(name)
            if not v:
                return default
            try:
                return datetime.strptime(v, "%Y-%m-%d").date()
            except ValueError:
                return default

        today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        start = _qdate("start", date(2024, 1, 1))
        end = _qdate("end", today - timedelta(days=1))

        async def _run() -> None:
            request.app.state.backfill_daily_running = True
            try:
                result = await pipeline.download_prices(start, end, skip_minute=True)
                msg = f"[日线补全] 完成 {start}~{end}\n{result.get('verify_msg')}"
            except Exception as e:
                logger.error("backfill-daily 失败: %s", e, exc_info=True)
                msg = f"[日线补全] 失败 {start}~{end}\n{e}"
            finally:
                request.app.state.backfill_daily_running = False
            try:
                from src.common.feishu_bot import FeishuBot

                bot = FeishuBot()
                if bot.is_configured():
                    await bot.send_message(msg)
            except Exception as e:  # noqa: BLE001
                logger.warning("backfill-daily 飞书通知失败: %s", e)

        asyncio.create_task(_run())
        return JSONResponse(
            {"success": True, "message": f"已触发日线补全 {start}~{end}(只日线),结果发飞书"}
        )

    return router
