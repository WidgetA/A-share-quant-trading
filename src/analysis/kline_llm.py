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
from typing import Any

import httpx

from src.common.config import (
    get_bltcy_api_key,
    get_bltcy_base_url,
    get_bltcy_model,
    get_lambda_kline_token,
    get_lambda_kline_url,
)

logger = logging.getLogger(__name__)


DEFAULT_PROMPT = (
    "你是一名 A 股市场技术分析师。下面这张 K 线图包含日 K 线、"
    "MA5 / MA10 / MA20 三条均线，下方副图是成交量柱（红涨绿跌）"
    "和 VMA5 / VMA20 量能均线。请基于图像本身做技术面分析，覆盖以下要点：\n"
    "1. 大趋势判断（上升 / 下降 / 震荡），及当前所处阶段\n"
    "2. 均线系统（多/空头排列、纠缠、关键拐点）\n"
    "3. 量价关系（放量上涨 / 缩量回调 / 量价背离 等）\n"
    "4. 关键支撑位与压力位（结合近期高低点 + 均线位置给出价格区间）\n"
    "5. 近期形态（突破 / 假突破 / 头肩 / 双底 / 旗形 等，如不明显请说明）\n"
    "6. 短期（1-2 周）操作建议与主要风险点\n"
    "回答用中文，结论清晰、给出具体价格区间，不要套话。"
)


# ── Data fetch ──────────────────────────────────────────────


async def _fetch_ohlcv(code: str, days: int) -> list[dict[str, Any]]:
    """Pull last `days` trading days of OHLCV for one code from GreptimeDB.

    Returns a list of dicts with keys: date (YYYY-MM-DD), open, high, low, close,
    volume — already converted to 股 (shares). Sorted ascending by date.
    """
    from src.data.clients.greptime_storage import (
        create_storage_from_config,
        ts_to_date,
    )

    today = date.today()
    # Pull a wider window then tail(days) to handle weekends/holidays/suspensions.
    start = today - timedelta(days=int(days * 2.2) + 60)

    storage = create_storage_from_config()
    await storage.start()
    try:
        rows = await storage.get_daily_for_code(code, str(start), str(today))
    finally:
        await storage.stop()

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

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            url,
            headers={
                "x-upload-token": token,
                "content-type": "application/json",
            },
            json={"code": code, "days": days, "ohlcv": ohlcv},
        )

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

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            f"{base_url}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json=payload,
        )

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
    code: str,
    days: int = 30,
    prompt: str | None = None,
    timeout_render: float = 60.0,
    timeout_llm: float = 120.0,
) -> dict[str, Any]:
    """Full pipeline: fetch OHLCV → render via Lambda → ask vision LLM.

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

    ohlcv = await _fetch_ohlcv(code, days)
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
