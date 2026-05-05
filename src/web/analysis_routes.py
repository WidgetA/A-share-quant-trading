# === MODULE PURPOSE ===
# HTTP entry points for analysis features that depend on external services
# (overseas Lambda renderer + 柏拉图AI vision LLM).
#
# Currently exposes:
#   POST /api/analyze-kline   {code, days?, prompt?} → analysis text + image_url

from __future__ import annotations

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

    return router
