# === MODULE PURPOSE ===
# Endpoints for uploading audit artifacts produced offline:
#   1. POST /api/audit/kimi-credentials/upload
#      Receive the kimi-cli OAuth credentials JSON (~/.kimi/credentials/
#      kimi-code.json) so the container's kimi-cli can use it without a
#      browser. The PowerShell helper script in scripts/ wraps `kimi login`
#      + this upload behind one click.
#   2. POST /api/audit/listing-info/upload
#      Receive the kimi-verified stock listing metadata JSON produced by
#      scripts/verify_list_date_kimi.py. Persists per-code list_date /
#      delist_date into the stock_listing_info table, which drives the
#      blocklist filter on the stock_snapshot universe.
#
# Both endpoints are guarded by X-API-Key (verify_trading_api_key).

from __future__ import annotations

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

    @router.post(
        "/listing-info/upload",
        dependencies=[Depends(verify_trading_api_key)],
    )
    async def upload_listing_info(request: Request, file: UploadFile) -> JSONResponse:
        """Receive the kimi-verified per-code list_date metadata.

        Expected JSON schema (this is the format we own; matches what
        scripts/verify_list_date_kimi.py + a small merge step produce):

          {
            "version": 1,
            "generated_at": "2026-05-12T08:00:00",
            "source": "kimi-cli",
            "entries": [
              {"code": "001220", "name": "世盟股份",
               "list_date": "2026-02-03", "delist_date": null,
               "source": "<URL>"},
              {"code": "001237", "name": null,
               "list_date": null, "delist_date": null,
               "_unverified": true},
              ...
            ]
          }
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

        entries_raw = payload.get("entries")
        if not isinstance(entries_raw, list):
            raise HTTPException(
                status_code=400,
                detail="JSON 缺少 'entries' 数组",
            )

        # Normalize: filter out entries with no code; mark unverified explicitly.
        norm: list[dict[str, Any]] = []
        for raw_e in entries_raw:
            if not isinstance(raw_e, dict):
                continue
            code = raw_e.get("code")
            if not isinstance(code, str) or len(code) != 6:
                continue
            list_date = raw_e.get("list_date")
            delist_date = raw_e.get("delist_date")
            unverified = bool(raw_e.get("_unverified", False))
            verified = (list_date is not None) and not unverified
            norm.append(
                {
                    "code": code,
                    "name": raw_e.get("name"),
                    "list_date": list_date,
                    "delist_date": delist_date,
                    "verified": verified,
                    "source": raw_e.get("source"),
                }
            )

        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="GreptimeDB storage 未连接")

        try:
            written = await storage.upsert_listing_info(norm)
        except Exception as e:
            logger.error("写 stock_listing_info 失败: %s", e, exc_info=True)
            raise HTTPException(status_code=500, detail=f"写入数据库失败: {e}")

        return JSONResponse(
            {
                "success": True,
                "message": f"已写入 {written} 条 listing_info",
                "input_entries": len(entries_raw),
                "written": written,
                "skipped": len(entries_raw) - written,
            }
        )

    @router.get("/listing-info/status")
    async def listing_info_status(request: Request) -> JSONResponse:
        """Quick read of how many listing_info entries are stored."""
        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            return JSONResponse({"ready": False})
        try:
            info = await storage.get_listing_info_all()
        except Exception as e:
            logger.warning("listing_info_status 读取失败: %s", e, exc_info=True)
            return JSONResponse({"ready": False, "error": str(e)})

        verified = sum(1 for v in info.values() if v.get("verified"))
        return JSONResponse(
            {
                "ready": True,
                "total_codes": len(info),
                "verified_codes": verified,
                "unverified_codes": len(info) - verified,
            }
        )

    return router
