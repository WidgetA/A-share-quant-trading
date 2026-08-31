"""Dump the closes array for a stock as the online V16 scanner would see it.

Usage:
    uv run python scripts/dump_closes.py 002173 2026-03-31
"""

import asyncio
import sys
from datetime import date, datetime, timedelta

import httpx

# Reuse the exact same code path as the online system
LOOKBACK_DAYS = 37
_TUSHARE_API_URL = "http://api.tushare.pro"


async def main(stock_code: str, ref_date: date) -> None:
    from src.common.config import get_tushare_token

    # Same parameters as v15_scan_service._fetch_history_ohlcv
    calendar_buffer = LOOKBACK_DAYS * 2 + 15  # = 89
    start = ref_date - timedelta(days=calendar_buffer)
    end = ref_date - timedelta(days=1)

    suffix = "SH" if stock_code.startswith(("6", "9")) else "SZ"
    ts_code = f"{stock_code}.{suffix}"

    print(f"Stock: {stock_code} ({ts_code}), ref_date: {ref_date}")
    print(f"Querying Tushare daily: {ts_code}  {start} ~ {end}")
    print()

    token = get_tushare_token()
    body = {
        "api_name": "daily",
        "token": token,
        "params": {
            "ts_code": ts_code,
            "start_date": start.strftime("%Y%m%d"),
            "end_date": end.strftime("%Y%m%d"),
        },
        "fields": "ts_code,trade_date,open,high,low,close,vol",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(_TUSHARE_API_URL, json=body)
        resp.raise_for_status()
        data = resp.json()

    if data.get("code") != 0:
        print(f"Tushare error: {data.get('msg', 'unknown')}")
        return

    fields = data.get("data", {}).get("fields", [])
    items = data.get("data", {}).get("items", [])

    if not items:
        print("No records returned!")
        return

    idx = {f: i for i, f in enumerate(fields)}

    # Tushare returns rows DESC; sort ASC for time-series indexing.
    items_sorted = sorted(items, key=lambda r: r[idx["trade_date"]])

    # Same filtering as _build_stock_data in v15_scan_service.py
    rows = []
    for rec in items_sorted:
        td = rec[idx.get("trade_date", -1)]
        o = rec[idx.get("open", -1)]
        h = rec[idx.get("high", -1)]
        lo = rec[idx.get("low", -1)]
        c = rec[idx.get("close", -1)]
        v = rec[idx.get("vol", -1)]
        if any(x is None for x in (o, h, lo, c, v)):
            continue
        o, h, lo, c, v = float(o), float(h), float(lo), float(c), float(v)
        if o <= 0 or c <= 0:
            continue
        # Format trade_date YYYYMMDD -> YYYY-MM-DD for display parity.
        d = f"{td[:4]}-{td[4:6]}-{td[6:8]}" if td and len(td) == 8 else td
        rows.append({"date": d, "open": o, "high": h, "low": lo, "close": c, "volume": v})

    print(f"Total valid rows: {len(rows)}")
    print(f"{'idx':>4}  {'date':>12}  {'close':>10}")
    print("-" * 32)
    for i, r in enumerate(rows):
        marker = ""
        n = len(rows)
        if i == n - 1:
            marker = "  ← closes[-1]"
        elif i == n - 6:
            marker = "  ← closes[-6] (5d ago)"
        elif i == n - 11:
            marker = "  ← closes[-11] (10d ago)"
        print(f"{i:4d}  {r['date']:>12}  {r['close']:10.2f}{marker}")

    closes = [r["close"] for r in rows]
    print()
    if len(closes) >= 6:
        c_now, c_5ago = closes[-1], closes[-6]
        trend_5d = (c_now - c_5ago) / c_5ago if c_5ago > 0 else 0
        print(f"trend_5d  = ({c_now} - {c_5ago}) / {c_5ago} = {trend_5d:.6f}")
    if len(closes) >= 11:
        c_now, c_10ago = closes[-1], closes[-11]
        trend_10d = (c_now - c_10ago) / c_10ago if c_10ago > 0 else 0
        print(f"trend_10d = ({c_now} - {c_10ago}) / {c_10ago} = {trend_10d:.6f}")


if __name__ == "__main__":
    code = sys.argv[1] if len(sys.argv) > 1 else "002173"
    ref = (
        datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
        if len(sys.argv) > 2
        else date(2026, 3, 31)
    )
    asyncio.run(main(code, ref))
