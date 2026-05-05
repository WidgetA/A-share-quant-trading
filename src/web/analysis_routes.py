# === MODULE PURPOSE ===
# HTTP entry points for analysis features that depend on external services
# (overseas Lambda renderer + 柏拉图AI vision LLM).
#
# Currently exposes:
#   POST /api/analyze-kline           {code, days?, prompt?} → analysis text + image_url
#   POST /api/pre-market-report/run   → kick off ANA-002 holdings report (manual)
#   GET  /api/pre-market-report/status → ANA-002 status (last run, enabled, etc.)

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# Pydantic models MUST be defined at module level — defining them inside
# create_analysis_router() interacts badly with `from __future__ import
# annotations` and FastAPI can no longer resolve the body type, falling back
# to query-param parsing (loc=["query","body"] 422 errors).
class AnalyzeKlineRequest(BaseModel):
    code: str = Field(..., description="Stock code, e.g. 000001.SZ")
    days: int = Field(30, ge=5, le=180, description="Trading days to render")
    prompt: str | None = Field(None, description="Override default Chinese analyst prompt")


def create_analysis_router() -> APIRouter:
    router = APIRouter(tags=["analysis"])

    @router.post("/api/analyze-kline")
    async def analyze_kline_endpoint(req: AnalyzeKlineRequest, request: Request) -> dict:
        from src.analysis.kline_llm import analyze_kline

        # Reuse the long-lived asyncpg pool from app.state instead of building
        # one per request. If GreptimeDB never connected at startup, fail fast.
        storage = getattr(request.app.state, "storage", None)
        if storage is None:
            raise HTTPException(
                status_code=503,
                detail="GreptimeDB unavailable (app.state.storage not initialized)",
            )

        try:
            result = await analyze_kline(
                storage=storage,
                code=req.code,
                days=req.days,
                prompt=req.prompt,
            )
        except ValueError as e:
            # Bad input or missing config (e.g. LAMBDA_KLINE_URL not set)
            raise HTTPException(status_code=400, detail=str(e)) from e
        except RuntimeError as e:
            # Upstream (Lambda or bltcy) returned an error
            raise HTTPException(status_code=502, detail=str(e)) from e

        # Strip raw LLM response from the public payload — keep it server-side only.
        return {
            "code": result["code"],
            "days": result["days"],
            "bars": result["bars"],
            "image_url": result["image_url"],
            "model": result["model"],
            "analysis": result["analysis"],
        }

    @router.post("/api/pre-market-report/run")
    async def run_pre_market_report(request: Request) -> dict:
        """Manually fire one ANA-002 pre-market holdings report.

        Returns immediately; the actual report runs in the background and
        pushes results to Feishu. Use `/api/pre-market-report/status` to
        check progress.
        """
        scheduler = getattr(request.app.state, "pre_market_report_scheduler", None)
        if scheduler is None:
            raise HTTPException(
                status_code=503,
                detail="PreMarketReportScheduler not initialized",
            )
        if scheduler.is_running():
            raise HTTPException(status_code=409, detail="上一次执行还没结束")
        # Fire-and-forget: HTTP returns immediately, scheduler does the work.
        asyncio.create_task(scheduler.trigger_manual())
        return {"started": True, "trigger": "manual"}

    @router.get("/api/pre-market-report/status")
    async def get_pre_market_report_status(request: Request) -> dict:
        scheduler = getattr(request.app.state, "pre_market_report_scheduler", None)
        if scheduler is None:
            return {
                "enabled": False,
                "running": False,
                "next_run_time": None,
                "last_run_time": None,
                "last_run_result": None,
                "last_run_message": "scheduler not initialized",
            }
        return scheduler.get_status()

    return router
