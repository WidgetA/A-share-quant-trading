"""Diagnose is_suspended NULL regression in GreptimeDB.

Run inside the trading-service container:
    python scripts/diag_is_suspended.py

Connects to GreptimeDB via asyncpg (pgwire port 4003).
"""
import asyncio
import os

import asyncpg


async def main():
    host = os.environ.get("GREPTIME_HOST", "localhost")
    port = int(os.environ.get("GREPTIME_PORT", "4003"))
    print(f"Connecting to GreptimeDB at {host}:{port} ...")
    conn = await asyncpg.connect(host=host, port=port, database="public", user="greptime")
    print("Connected.\n")

    # 1. Count NULL rows
    row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM backtest_daily WHERE is_suspended IS NULL")
    null_cnt = row["cnt"]
    print(f"[1] is_suspended IS NULL 总行数: {null_cnt}")

    if null_cnt == 0:
        print("    没有 NULL 行，无需回填。")
        await conn.close()
        return

    # 2. Which dates have NULL?
    rows = await conn.fetch("SELECT DISTINCT ts FROM backtest_daily WHERE is_suspended IS NULL ORDER BY ts")
    print(f"[2] 涉及 {len(rows)} 个日期:")
    for r in rows[:10]:
        ts = r["ts"]
        # GreptimeDB ts is epoch ms or timestamp
        print(f"    ts = {ts}")

    # 3. Total rows vs NULL rows per sample date
    if rows:
        sample_ts = rows[0]["ts"]
        total = await conn.fetchrow(f"SELECT COUNT(*) as cnt FROM backtest_daily WHERE ts = '{sample_ts}'")
        null = await conn.fetchrow(f"SELECT COUNT(*) as cnt FROM backtest_daily WHERE ts = '{sample_ts}' AND is_suspended IS NULL")
        not_null = await conn.fetchrow(f"SELECT COUNT(*) as cnt FROM backtest_daily WHERE ts = '{sample_ts}' AND is_suspended IS NOT NULL")
        print(f"\n[3] 抽样日期 ts={sample_ts}:")
        print(f"    总行数:        {total['cnt']}")
        print(f"    NULL 行数:     {null['cnt']}")
        print(f"    非 NULL 行数:  {not_null['cnt']}")

    # 4. Check if COMPACT_TABLE is available
    print("\n[4] 尝试 COMPACT_TABLE ...")
    try:
        await conn.execute("ADMIN COMPACT_TABLE('backtest_daily')")
        print("    COMPACT_TABLE 执行成功")
    except Exception as e:
        print(f"    COMPACT_TABLE 失败: {e}")

    # 5. Re-check after compact
    row2 = await conn.fetchrow("SELECT COUNT(*) as cnt FROM backtest_daily WHERE is_suspended IS NULL")
    print(f"\n[5] COMPACT 后 is_suspended IS NULL 总行数: {row2['cnt']}")
    if row2["cnt"] < null_cnt:
        print(f"    减少了 {null_cnt - row2['cnt']} 行 — COMPACT 有效!")
    elif row2["cnt"] == null_cnt:
        print("    没有变化 — COMPACT 没有解决问题")
    else:
        print(f"    反而增加了?! ({row2['cnt']})")

    await conn.close()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
