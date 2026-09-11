"""Read-only current-session evidence acquisition for V22-slim migration."""

import asyncio
import gzip
import json
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import get_tushare_token
from src.data.clients.tushare_realtime import TushareRealtimeClient
from src.strategy.v22_slim.runtime_inputs import api_rows
from src.strategy.v22_slim.selection import make_scanner


async def main():
    today = datetime.now(ZoneInfo("Asia/Shanghai"))
    if today.hour < 16:
        raise ValueError("cutover checkpoint requires a completed session")
    folder = Path(__file__).resolve().parents[1] / "reports/v22_slim/cutover"
    folder.mkdir(parents=True, exist_ok=True)
    client = TushareRealtimeClient(get_tushare_token())
    client.TIMEOUT = 600
    await client.start()
    try:
        day = today.strftime("%Y%m%d")

        async def fetch(name, params):
            path = folder / (name + ".json")
            if path.exists():
                saved = json.loads(path.read_text(encoding="utf-8"))
                if name != "stk_limit" or "pre_close" in saved[0]:
                    return saved
            rows = await api_rows(client, name, params)
            path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
            return rows

        daily = await fetch("daily", {"trade_date": day})
        await fetch("stk_limit", {"trade_date": day})
        await fetch(
            "index_daily", {"ts_code": "932000.CSI", "start_date": "20260801", "end_date": day}
        )
        await fetch(
            "trade_cal",
            {"exchange": "SSE", "start_date": "20260601", "end_date": "20261001", "is_open": "1"},
        )
        scanner, _, _ = make_scanner()
        _, universe = scanner.get_universe()
        codes = sorted(
            universe
            | {row["ts_code"][:6] for row in daily if row["ts_code"].startswith(("00", "60"))}
        )
        path = folder / "early_raw.json.gz"
        if not path.exists():
            raw = await client.batch_get_minute_history(codes)
            payload = {
                code: [
                    {**asdict(bar), "bar_end": bar.bar_end.isoformat()}
                    for bar in bars
                    if bar.end_label <= "09:41"
                ]
                for code, bars in raw.items()
            }
            path.write_bytes(
                gzip.compress(json.dumps(payload, ensure_ascii=False).encode(), mtime=0)
            )
            print(
                json.dumps(
                    {
                        "date": day,
                        "requested": len(codes),
                        "received": len(raw),
                        "source": "rt_min_daily",
                        "maximum_concurrency": client.MAX_CONCURRENCY,
                    }
                ),
                flush=True,
            )
    finally:
        await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
