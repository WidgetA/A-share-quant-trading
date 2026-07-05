# === MODULE PURPOSE ===
# V16 每日推票榜单持久化 + 查询「选股接口」。
#
# 背景: V16 扫描结果此前只进飞书消息,从未落库(v15_scan_db 需要的 PostgreSQL
# 在线上并不存在,save_top_n 一直没有调用点)。笔记服务(另一分支部署)的
# 「AI 写日志」功能需要按日期回查当日 Top-10 —— 这里用**本地 JSON 文件**持久化
# (data/v16_scan_history/YYYY-MM-DD.json,与 kimi findings 同模式,零 DB 依赖)。
#
# - 每日 9:39 扫描完成后 run_v16_scan 调 persist_scan_history(source="live")
# - GET  /api/v16/scan-history?date=YYYY-MM-DD  → {date, source, rows}; 无数据 404
# - POST /api/v16/scan-history/backfill (X-API-Key=iquant key) → 补历史日期
#   (离线复刻结果上传);不覆盖已有 live 行,除非 force

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

SCAN_HISTORY_DIR = Path("data/v16_scan_history")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_API_KEY_HEADER = APIKeyHeader(name="X-API-Key")


def _verify_api_key(api_key: str = Depends(_API_KEY_HEADER)) -> str:
    from src.common.config import get_iquant_api_key

    expected = get_iquant_api_key()
    if not expected:
        raise HTTPException(status_code=500, detail="IQUANT_API_KEY not configured")
    if api_key != expected:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return api_key


class ScanHistoryBackfillRequest(BaseModel):
    """POST /api/v16/scan-history/backfill 请求体."""

    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    rows: list[dict] = Field(..., min_length=1, max_length=2000)
    force: bool = Field(False, description="已有数据时是否覆盖(live 数据默认拒绝覆盖)")


def persist_scan_history(trade_date: str, rows: list[dict], source: str) -> None:
    """把某日推票榜单写盘(原子写)。trade_date=YYYY-MM-DD。

    非交易路径,失败不许打断扫描——调用方负责 try/except + 响亮记日志。
    """
    SCAN_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"date": trade_date, "source": source, "rows": rows}
    fd, tmp_path = tempfile.mkstemp(dir=SCAN_HISTORY_DIR, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=1)
        os.replace(tmp_path, SCAN_HISTORY_DIR / f"{trade_date}.json")
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    logger.info(
        "v16 scan history persisted: %s (%d rows, source=%s)", trade_date, len(rows), source
    )


def load_scan_history(trade_date: str) -> dict | None:
    path = SCAN_HISTORY_DIR / f"{trade_date}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def build_history_rows_from_scan(scan_result, stock_data: dict) -> list[dict]:
    """从 V16ScanResult + stock_data 构造持久化行(top-10)。"""
    rows: list[dict] = []
    for rank, s in enumerate(scan_result.recommended, 1):
        sd = stock_data.get(s.code)
        open_price = getattr(sd, "open_price", None) if sd else None
        prev_close = getattr(sd, "prev_close", None) if sd else None
        gain_from_open = (
            round((s.buy_price - open_price) / open_price * 100, 4) if open_price else None
        )
        rows.append(
            {
                "rank": rank,
                "stock_code": s.code,
                "stock_name": s.name,
                "board_name": scan_result.stock_best_board.get(s.code, ""),
                "score": round(float(s.score), 6),
                "buy_price": round(float(s.buy_price), 4),
                "open_price": round(float(open_price), 4) if open_price else None,
                "prev_close": round(float(prev_close), 4) if prev_close else None,
                "gain_from_open_pct": gain_from_open,
                "cci14": scan_result.cci.get(s.code),
                "final_candidates": scan_result.final_candidates,
            }
        )
    return rows


def create_v16_scan_history_router() -> APIRouter:
    router = APIRouter(tags=["v16-scan-history"])

    @router.get("/api/v16/scan-history")
    async def get_scan_history(date: str) -> dict:
        """按日期查当日推票榜单(「选股接口」)。无当日数据 → 404。"""
        if not _DATE_RE.match(date):
            raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")
        payload = load_scan_history(date)
        if payload is None:
            raise HTTPException(status_code=404, detail=f"{date} 无推票榜单数据")
        return payload

    @router.post("/api/v16/scan-history/backfill")
    async def backfill_scan_history(
        body: ScanHistoryBackfillRequest, api_key: str = Depends(_verify_api_key)
    ) -> dict:
        """补写历史日期榜单(离线复刻结果上传)。live 数据默认拒绝覆盖。"""
        existing = load_scan_history(body.date)
        if existing is not None and not body.force:
            raise HTTPException(
                status_code=409,
                detail=f"{body.date} 已有数据(source={existing.get('source')}),覆盖需 force=true",
            )
        persist_scan_history(body.date, body.rows, source="backfill")
        return {"date": body.date, "rows_written": len(body.rows)}

    return router
