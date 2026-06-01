# === MODULE PURPOSE ===
# 把全市场每只股票的权威「上市日 / 退市日 / 名称 / 板块」一次性灌进数据库的
# stock_listing_info 表,数据源 = Tushare stock_basic(在市 L + 已退市 D 两次调用,
# 覆盖含北交所)。建好后这张表就是「一查即知」的权威 list —— 不用 kimi 逐只查、
# 不用运行时复杂推断。
#
# 用法: uv run python scripts/load_listing_from_tushare.py [--feishu]
# 也被 endpoint POST /api/audit/listing-info/load-tushare 复用。

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.clients.greptime_storage import create_storage_from_config  # noqa: E402

_SOURCE = "tushare_stock_basic"
_UPSERT_BATCH = 200  # GreptimeDB drops rows silently above ~200 per INSERT


def build_entries(rows_listed: list[dict], rows_delisted: list[dict]) -> list[dict]:
    """Turn stock_basic rows into stock_listing_info upsert entries.

    Pure function (unit-testable). verified=True because stock_basic IS the
    authoritative exchange listing record.
    """
    entries: list[dict] = []
    for rows in (rows_listed, rows_delisted):
        for r in rows:
            code = r.get("code")
            if not code or len(code) != 6:
                continue
            entries.append(
                {
                    "code": code,
                    "name": r.get("name"),
                    "list_date": r.get("list_date"),
                    "delist_date": r.get("delist_date"),
                    "verified": True,
                    "source": _SOURCE,
                }
            )
    return entries


async def load_listing(storage, client) -> dict:
    """Fetch stock_basic (L + D) and upsert into stock_listing_info. Returns counts."""
    listed = await client.fetch_stock_basic_full("L")
    delisted = await client.fetch_stock_basic_full("D")
    entries = build_entries(listed, delisted)

    written = 0
    for i in range(0, len(entries), _UPSERT_BATCH):
        written += await storage.upsert_listing_info(entries[i : i + _UPSERT_BATCH])

    return {
        "listed": len(listed),
        "delisted": len(delisted),
        "total_entries": len(entries),
        "written": written,
    }


async def _notify_feishu(message: str) -> None:
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_message(message)
    except Exception as e:  # noqa: BLE001
        print(f"⚠ 飞书发送失败: {e}", file=sys.stderr, flush=True)


async def run_load_listing(
    storage, *, feishu: bool, client=None, stop_storage: bool = False
) -> dict:
    """Build the listing index from stock_basic. ``storage`` is usually the
    app's started storage; CLI passes a fresh one + stop_storage=True."""
    from src.common.config import get_tushare_token
    from src.data.clients.tushare_realtime import TushareRealtimeClient

    own_client = client is None
    if own_client:
        client = TushareRealtimeClient(token=get_tushare_token())
        await client.start()
    try:
        result = await load_listing(storage, client)
    finally:
        if own_client:
            await client.stop()
        if stop_storage:
            await storage.stop()

    msg = (
        f"[上市索引] 从 Tushare stock_basic 灌入完成:在市 {result['listed']} + "
        f"已退市 {result['delisted']} = {result['total_entries']} 只,写入 {result['written']} 行。"
        "现在 stock_listing_info 表可直接查。"
    )
    print(msg, file=sys.stderr, flush=True)
    if feishu:
        await _notify_feishu(msg)
    return result


async def main(*, feishu: bool) -> None:
    storage = create_storage_from_config()
    await storage.start()
    await run_load_listing(storage, feishu=feishu, stop_storage=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="从 Tushare stock_basic 灌权威上市索引")
    parser.add_argument("--feishu", action="store_true", help="完成后发飞书")
    args = parser.parse_args()
    asyncio.run(main(feishu=args.feishu))
