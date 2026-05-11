# === MODULE PURPOSE ===
# One-off experiment: pull (bak_basic, daily, suspend_d) for every A-share
# trading day from 2023-01-01 onward and dump per-day JSON snapshots, so we
# can later independently verify whether `B - D - S` is *always* "stocks that
# weren't actually listed that day" — i.e. whether `bak_basic` can be safely
# dropped from the stock_list pipeline.
#
# Output: data/audit/sdb/YYYY-MM-DD.json (one file per trading day)
# Resume: existing files are skipped, so the script can be killed and rerun.
# This directory is .gitignored — it's experiment data, not a deliverable.

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

# Allow running this script directly from the repo root (e.g. `uv run python
# scripts/audit_sdb_diff.py`). Without this, `import src.*` fails because
# the project isn't an installed package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.common.config import get_tushare_token  # noqa: E402
from src.data.clients.tushare_realtime import (  # noqa: E402
    TushareRealtimeClient,
    get_tushare_trade_calendar,
)

OUT_DIR = Path("data/audit/sdb")
RETRY_DELAY_SEC = 30


async def _fetch_day(
    client: TushareRealtimeClient, td_compact: str
) -> tuple[set[str], set[str], set[str]]:
    """Fetch (B=bak_basic, D=daily, S=suspend_d) for one trade date.

    Raises on any underlying API failure.
    """
    bak = set(await client.fetch_bak_basic(td_compact))
    daily_records = await client.fetch_daily(td_compact)
    daily = {r["ticker"] for r in daily_records}
    suspended = await client.fetch_suspended_stocks(td_compact)
    return bak, daily, suspended


async def main(start: str, end: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cal_dates = await get_tushare_trade_calendar(start, end)
    print(
        f"交易日历: {start} ~ {end} 共 {len(cal_dates)} 个交易日",
        file=sys.stderr,
        flush=True,
    )

    client = TushareRealtimeClient(token=get_tushare_token())
    await client.start()

    completed = 0
    skipped = 0
    failed: list[str] = []
    try:
        for i, trade_date in enumerate(cal_dates, 1):
            out_file = OUT_DIR / f"{trade_date}.json"
            if out_file.exists():
                skipped += 1
                continue
            td_compact = trade_date.replace("-", "")

            try:
                bak, daily, suspended = await _fetch_day(client, td_compact)
            except Exception as e:
                # Likely rate-limit. Notify, sleep, retry once.
                print(
                    f"⚠ [{i}/{len(cal_dates)}] {trade_date} API 失败: {e}\n"
                    f"   sleep {RETRY_DELAY_SEC}s 后重试 1 次...",
                    file=sys.stderr,
                    flush=True,
                )
                await asyncio.sleep(RETRY_DELAY_SEC)
                try:
                    bak, daily, suspended = await _fetch_day(client, td_compact)
                    print(
                        f"   [{trade_date}] 重试成功 ✓",
                        file=sys.stderr,
                        flush=True,
                    )
                except Exception as e2:
                    print(
                        f"⚠⚠ [{i}/{len(cal_dates)}] {trade_date} 重试仍失败: {e2}, "
                        f"跳过 (下次运行会再试)",
                        file=sys.stderr,
                        flush=True,
                    )
                    failed.append(trade_date)
                    continue

            diff = sorted(bak - daily - suspended)
            payload = {
                "date": trade_date,
                "bak_basic": sorted(bak),
                "daily": sorted(daily),
                "suspend_d": sorted(suspended),
                "b_minus_d_minus_s": diff,
                "counts": {
                    "B": len(bak),
                    "D": len(daily),
                    "S": len(suspended),
                    "diff": len(diff),
                },
            }
            out_file.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            completed += 1
            print(
                f"[{i}/{len(cal_dates)}] {trade_date}: "
                f"B={len(bak)} D={len(daily)} S={len(suspended)} diff={len(diff)}",
                file=sys.stderr,
                flush=True,
            )
    finally:
        await client.stop()

    print(
        f"\n完成: {completed} 天新增, {skipped} 天已有 (跳过), {len(failed)} 天失败",
        file=sys.stderr,
        flush=True,
    )
    if failed:
        print(
            f"失败日期: {failed}\n下次运行会自动重试 (因为只跳过 .json 已存在的)",
            file=sys.stderr,
            flush=True,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Audit per-day (bak_basic ∪ daily ∪ suspend_d) for A-shares "
            "from 2023-01-01 onward. Per-day JSON in data/audit/sdb/."
        )
    )
    parser.add_argument("--start", default="2023-01-01", help="YYYY-MM-DD")
    parser.add_argument(
        "--end",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="YYYY-MM-DD (default: today)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.start, args.end))
