# === MODULE PURPOSE ===
# Orchestrate K-line technical analysis:
#   1. Pull last N trading days of OHLCV from GreptimeDB.
#   2. POST to overseas Lambda → Lambda renders PNG → uploads to public S3 →
#      returns image URL.
#   3. POST to 柏拉图AI vision endpoint (OpenAI-compatible) with the URL → get
#      Chinese technical analysis text.
#
# Why the Lambda hop: matplotlib + public-readable storage live in AWS us-east-1
# so we sidestep mainland China ICP filing rules and keep the trading-service
# image small. The trading-service only ships JSON over HTTPS.

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any

import httpx

from src.common.config import (
    get_bltcy_api_key,
    get_bltcy_base_url,
    get_bltcy_model,
    get_lambda_kline_token,
    get_lambda_kline_url,
)

if TYPE_CHECKING:
    from src.data.clients.greptime_storage import GreptimeBacktestStorage

logger = logging.getLogger(__name__)


DEFAULT_PROMPT = (
    "你是一名 A 股技术分析师。下面这张 K 线图包含日 K、MA5/MA10/MA20 三条均线，"
    "副图是成交量柱（红涨绿跌）和 VMA5/VMA20 量能均线。\n\n"
    "读者已持仓，只关心：接下来怎么走、该卖/持/加哪一档、触发条件、关键风险。"
    "不要展开趋势/均线/量价/形态四大章节，不要解释「为什么」——直接给结论和价位。\n\n"
    "按以下四段输出，每段独立，不要前言不要总结：\n\n"
    "【当前状态】一句话：最新价 ¥xx.xx，最接近【卖出 / 持有 / 增持】哪一档，"
    "距离最近卖出触发 ¥xx.xx（¥-x.xx，-x.x%） / 增持触发 ¥xx.xx（¥+x.xx，+x.x%）。\n\n"
    "【走势研判】≤4 行：\n"
    "  - 位置：例如「高位整理 / 突破后回踩 / 主升中段 / 顶部背离 / 下跌中继 / 筑底」\n"
    "  - 压力：¥xx.xx - ¥xx.xx（说明哪条均线/前高/缺口构成）\n"
    "  - 支撑：¥xx.xx - ¥xx.xx（同上）\n"
    "  - 短期倾向（1-2 周）：例如「整理后大概率上攻」「冲高乏力，走坏概率上升」"
    "「等突破才能定方向」，必须明确倾向，不要「震荡走势」这种废话。\n\n"
    "【触发条件】每档 ≤3 条，每条 1 行，形态 + 触发价位 + 量能阈值（适用时）：\n"
    "卖出（减仓/清仓）：\n"
    "  - …\n"
    "持有（不动作）：\n"
    "  - …\n"
    "增持（加仓）：\n"
    "  - …\n\n"
    "【风险】≤2 条最关键的，每条 1 行，必须带具体价位或量能数值（不要空泛地说「注意压力位」）：\n"
    "  - …"
)


# ── Data fetch ──────────────────────────────────────────────


async def _fetch_ohlcv(
    storage: GreptimeBacktestStorage,
    code: str,
    days: int,
) -> list[dict[str, Any]]:
    """Pull last `days` trading days of OHLCV for one code from GreptimeDB.

    `storage` must be an already-started `GreptimeBacktestStorage` (typically
    `request.app.state.storage`). This function does not start/stop it — that
    lifecycle belongs to the FastAPI app so the asyncpg pool is reused across
    requests.

    Returns a list of dicts with keys: date (YYYY-MM-DD), open, high, low, close,
    volume — already converted to 股 (shares). Sorted ascending by date.
    """
    from src.data.clients.greptime_storage import ts_to_date

    # GreptimeDB stores 6-digit codes without exchange suffix. Accept either
    # form from the caller and normalize.
    code_norm = code.split(".")[0].strip()

    today = date.today()
    # Pull a wider window then tail(days) to handle weekends/holidays/suspensions.
    start = today - timedelta(days=int(days * 2.2) + 60)

    rows = await storage.get_daily_for_code(code_norm, str(start), str(today))

    if not rows:
        raise ValueError(f"No daily data found for {code!r} in GreptimeDB")

    out: list[dict[str, Any]] = []
    for r in rows:
        if r["close_price"] is None or float(r["close_price"]) <= 0:
            continue  # skip suspended days
        out.append(
            {
                "date": ts_to_date(r["ts"]).isoformat(),
                "open": float(r["open_price"]),
                "high": float(r["high_price"]),
                "low": float(r["low_price"]),
                "close": float(r["close_price"]),
                # daily vol stored in 手, the Lambda doesn't care about units —
                # but keep the conversion consistent with the rest of the system.
                "volume": float(r["vol"] or 0) * 100.0,
            }
        )

    if not out:
        raise ValueError(f"All bars for {code!r} were filtered out (suspended?)")

    return out[-days:]


# ── Lambda call ─────────────────────────────────────────────


async def _render_via_lambda(
    code: str, days: int, ohlcv: list[dict[str, Any]], timeout: float
) -> str:
    """POST OHLCV to the K-line render Lambda. Returns the public PNG URL."""
    url = get_lambda_kline_url()
    token = get_lambda_kline_token()

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                url,
                headers={
                    "x-upload-token": token,
                    "content-type": "application/json",
                },
                json={"code": code, "days": days, "ohlcv": ohlcv},
            )
    except httpx.TimeoutException as e:
        raise RuntimeError(f"lambda-kline timeout after {timeout}s: {e}") from e
    except httpx.HTTPError as e:
        raise RuntimeError(f"lambda-kline transport error: {type(e).__name__}: {e}") from e

    if resp.status_code >= 400:
        raise RuntimeError(f"lambda-kline {resp.status_code}: {resp.text[:500]}")

    data = resp.json()
    if not data.get("ok") or not data.get("url"):
        raise RuntimeError(f"lambda-kline bad response: {data}")
    logger.info(
        "lambda-kline rendered %s in %sms -> %s",
        code,
        data.get("elapsed_ms"),
        data["url"],
    )
    return data["url"]


# ── LLM call ────────────────────────────────────────────────


async def _ask_vision_llm(
    code: str, image_url: str, prompt: str, timeout: float
) -> tuple[str, dict[str, Any]]:
    """Send the image URL + prompt to bltcy chat/completions. Returns (text, raw)."""
    api_key = get_bltcy_api_key()
    base_url = get_bltcy_base_url().rstrip("/")
    model = get_bltcy_model()

    payload = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": f"股票代码：{code}\n\n{prompt}"},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ],
            }
        ],
    }

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                f"{base_url}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}"},
                json=payload,
            )
    except httpx.TimeoutException as e:
        raise RuntimeError(f"bltcy timeout after {timeout}s: {e}") from e
    except httpx.HTTPError as e:
        raise RuntimeError(f"bltcy transport error: {type(e).__name__}: {e}") from e

    if resp.status_code >= 400:
        raise RuntimeError(f"bltcy {resp.status_code}: {resp.text[:500]}")

    data = resp.json()
    try:
        analysis = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as e:
        raise RuntimeError(f"unexpected bltcy response shape: {data}") from e

    return analysis, data


# ── Public entry point ──────────────────────────────────────


async def analyze_kline(
    storage: GreptimeBacktestStorage,
    code: str,
    days: int = 30,
    prompt: str | None = None,
    timeout_render: float = 60.0,
    timeout_llm: float = 600.0,
) -> dict[str, Any]:
    """Full pipeline: fetch OHLCV → render via Lambda → ask vision LLM.

    `storage` is the long-lived `GreptimeBacktestStorage` from
    `app.state.storage`. The caller owns its start/stop lifecycle.

    Returns:
        {
            "code": str,
            "days": int,
            "bars": int,
            "image_url": str,
            "model": str,
            "analysis": str,
            "raw": dict,    # raw LLM response
        }
    """
    if days < 5:
        raise ValueError("days must be >= 5")

    ohlcv = await _fetch_ohlcv(storage, code, days)
    image_url = await _render_via_lambda(code, days, ohlcv, timeout_render)
    user_prompt = prompt or DEFAULT_PROMPT
    analysis, raw = await _ask_vision_llm(code, image_url, user_prompt, timeout_llm)

    return {
        "code": code,
        "days": days,
        "bars": len(ohlcv),
        "image_url": image_url,
        "model": get_bltcy_model(),
        "prompt": user_prompt,
        "analysis": analysis,
        "raw": raw,
    }
