"""K-line chart renderer Lambda."""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import secrets
import time
import uuid
from datetime import datetime, timezone

import boto3
import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["BUCKET_NAME"]
UPLOAD_TOKEN = os.environ["UPLOAD_TOKEN"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")


def _json(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json; charset=utf-8"},
        "body": json.dumps(body, ensure_ascii=False),
    }


def _get_header(event: dict, name: str) -> str | None:
    headers = event.get("headers") or {}
    target = name.lower()
    for k, v in headers.items():
        if k and k.lower() == target:
            return v
    return None


def _get_method(event: dict) -> str:
    method = event.get("requestContext", {}).get("http", {}).get("method")
    return (method or event.get("httpMethod") or "GET").upper()


def _read_body(event: dict) -> bytes:
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        return base64.b64decode(body)
    return body.encode("utf-8") if isinstance(body, str) else body


def _build_dataframe(ohlcv: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(ohlcv)
    df["Date"] = pd.to_datetime(df["date"])
    df = df.rename(
        columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    )
    df = df.set_index("Date").sort_index()
    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    df = df[df["Close"] > 0]

    for n in (5, 10, 20):
        df[f"MA{n}"] = df["Close"].rolling(n).mean()
    for n in (5, 20):
        df[f"VMA{n}"] = df["Volume"].rolling(n).mean()

    return df


def _draw_candles(ax, data, width=0.62, up="red", down="green") -> None:
    x = mdates.date2num(data.index.to_pydatetime())
    for xi, (o, h, lo, c) in zip(x, data[["Open", "High", "Low", "Close"]].to_numpy()):
        color = up if c >= o else down
        ax.vlines(xi, lo, h, color=color, linewidth=0.9)
        lower = min(o, c)
        height = abs(c - o)
        if height < 0.01 * c:
            ax.hlines(c, xi - width / 2, xi + width / 2, color=color, linewidth=1.1)
        else:
            ax.add_patch(
                plt.Rectangle(
                    (xi - width / 2, lower),
                    width,
                    height,
                    facecolor=color,
                    edgecolor=color,
                    linewidth=0.7,
                    alpha=0.85,
                )
            )
    ax.set_xlim(x[0] - 1, x[-1] + 1)
    ax.xaxis_date()


def _render_png(df: pd.DataFrame, code: str) -> bytes:
    start = df.index[0].strftime("%Y-%m-%d")
    end = df.index[-1].strftime("%Y-%m-%d")

    fig = plt.figure(figsize=(14, 9), dpi=140)
    gs = fig.add_gridspec(2, 1, height_ratios=[3, 1], hspace=0.03)
    ax = fig.add_subplot(gs[0])
    vax = fig.add_subplot(gs[1], sharex=ax)

    _draw_candles(ax, df)
    for n, lw in ((5, 1.2), (10, 1.2), (20, 1.4)):
        col = f"MA{n}"
        if col in df.columns and df[col].notna().any():
            ax.plot(df.index, df[col], linewidth=lw, label=f"MA{n}")

    ax.set_title(f"{code}  {start} ~ {end}", fontsize=13, pad=10)
    ax.set_ylabel("Price")
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.35)
    ax.legend(loc="upper left", ncol=3, frameon=True, fontsize=8)
    ax.tick_params(axis="x", which="both", labelbottom=False)

    x = mdates.date2num(df.index.to_pydatetime())
    colors = ["red" if c >= o else "green" for o, c in df[["Open", "Close"]].to_numpy()]
    vax.bar(x, df["Volume"] / 1e6, width=0.62, color=colors, alpha=0.35, align="center")
    vax.set_xlim(x[0] - 1, x[-1] + 1)
    vax.xaxis_date()
    for n, lw in ((5, 1.2), (20, 1.4)):
        col = f"VMA{n}"
        if col in df.columns and df[col].notna().any():
            vax.plot(df.index, df[col] / 1e6, linewidth=lw, label=f"Vol MA{n}")
    vax.set_ylabel("Volume / M")
    vax.grid(True, linestyle="--", linewidth=0.5, alpha=0.3)
    vax.legend(loc="upper left", ncol=2, frameon=True, fontsize=8)
    vax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=10))
    vax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    fig.autofmt_xdate(rotation=0)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def _upload(png: bytes, code: str) -> tuple[str, str]:
    safe_code = code.replace(".", "-")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    suffix = secrets.token_hex(4)
    key = f"kline/{safe_code}/{ts}-{suffix}.png"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=png,
        ContentType="image/png",
        ACL="public-read",
        CacheControl="public, max-age=86400",
    )
    url = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{key}"
    return key, url


def lambda_handler(event: dict, context) -> dict:
    method = _get_method(event)
    if method == "GET":
        return _json(200, {"ok": True, "service": "kline-render"})
    if method != "POST":
        return _json(405, {"ok": False, "message": f"Method {method} not allowed"})

    if _get_header(event, "x-upload-token") != UPLOAD_TOKEN:
        return _json(401, {"ok": False, "message": "Unauthorized"})

    try:
        payload = json.loads(_read_body(event).decode("utf-8"))
    except Exception as e:
        return _json(400, {"ok": False, "message": f"Bad JSON: {e}"})

    code = payload.get("code")
    days = int(payload.get("days") or 30)
    ohlcv = payload.get("ohlcv") or []
    if not code or not ohlcv:
        return _json(400, {"ok": False, "message": "code and ohlcv required"})

    t0 = time.time()
    try:
        df = _build_dataframe(ohlcv)
    except Exception as e:
        logger.exception("dataframe build failed")
        return _json(400, {"ok": False, "message": f"Bad ohlcv rows: {e}"})

    df = df.tail(days)
    if df.empty:
        return _json(400, {"ok": False, "message": "no valid bars after filtering"})

    try:
        png = _render_png(df, code)
        key, url = _upload(png, code)
    except Exception as e:
        logger.exception("render/upload failed")
        return _json(500, {"ok": False, "message": str(e)})

    elapsed_ms = int((time.time() - t0) * 1000)
    logger.info("rendered %s (%d bars) in %dms -> %s", code, len(df), elapsed_ms, url)
    return _json(
        200,
        {
            "ok": True,
            "code": code,
            "days": days,
            "bars": len(df),
            "key": key,
            "url": url,
            "elapsed_ms": elapsed_ms,
            "request_id": str(uuid.uuid4()),
        },
    )
