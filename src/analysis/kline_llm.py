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
    "你是一名 A 股市场技术分析师。下面这张 K 线图包含日 K 线、"
    "MA5 / MA10 / MA20 三条均线，下方副图是成交量柱（红涨绿跌）"
    "和 VMA5 / VMA20 量能均线。请基于图像本身做技术面分析，覆盖以下要点：\n"
    "1. 大趋势判断（上升 / 下降 / 震荡），及当前所处阶段\n"
    "2. 均线系统（多/空头排列、纠缠、关键拐点）\n"
    "3. 量价关系（放量上涨 / 缩量回调 / 量价背离 等）\n"
    "4. 关键支撑位与压力位（结合近期高低点 + 均线位置给出价格区间）\n"
    "5. 近期形态（突破 / 假突破 / 头肩 / 双底 / 旗形 等，如不明显请说明）\n"
    "6. 当日持仓操作建议（假设用户当前已持仓）——必须按【卖出 / 持有 / 增持】"
    "三档明确给出信号，每一档都要写明【触发形态 + 具体触发价位】，"
    "禁止「如果跌破就卖」这种没有价位的空话：\n"
    "   ▸ 卖出信号：列出哪些形态/价位出现应当减仓或清仓\n"
    "     （例：跌破 MAxx 且收盘价 < ¥xx.xx；放量长阴吞没前 N 日涨幅；"
    "MA10 死叉 MA20；量价顶背离；头肩顶右肩跌破颈线 ¥xx.xx 等）\n"
    "   ▸ 持有信号：列出哪些形态说明可以继续拿住、不必动作\n"
    "     （例：均线多头排列未破；缩量回踩 MA10 不破 ¥xx.xx；K 线运行在通道内 ¥xx-¥xx 等）\n"
    "   ▸ 增持信号：列出哪些形态出现可以考虑加仓\n"
    "     （例：缩量回踩 ¥xx.xx 支撑后放量上攻；放量突破压力位 ¥xx.xx；"
    "双底/W 底颈线 ¥xx.xx 突破确认 等）\n"
    "   最后给出一句【当前状态结论】：现在最接近哪一档信号、距离触发价位还有多远。\n"
    "7. 短期（1-2 周）主要风险点\n"
    "回答用中文，结论清晰、所有触发条件必须落到具体价位或形态判定标准，不要套话。"
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
    timeout_llm: float = 360.0,
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
