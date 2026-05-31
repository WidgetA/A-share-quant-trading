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

    return router
