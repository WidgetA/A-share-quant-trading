"""诊断脚本：测试 DELETE + INSERT 能否扛住 FLUSH。

用法: python scripts/debug_backfill_upsert.py

已知：纯 INSERT upsert 在 FLUSH 后会被旧 SST 覆盖。
本次测试 DELETE 旧行 → INSERT 新行 → FLUSH → 验证。
所有写入值均通过 Tushare 查真实停牌状态，不会污染数据。
"""

import asyncio
import os

import asyncpg


class _Conn(asyncpg.Connection):
    async def reset(self, *, timeout=None):
        pass


async def main():
    host = os.environ.get("GREPTIME_HOST", "localhost")
    port = int(os.environ.get("GREPTIME_PORT", "4003"))

    pool = await asyncpg.create_pool(
        host=host,
        port=port,
        database="public",
        user="greptime",
        min_size=1,
        max_size=2,
        statement_cache_size=0,
        connection_class=_Conn,
    )

    async with pool.acquire() as conn:
        # 总数
        total_null = await conn.fetchrow(
            "SELECT COUNT(*) as cnt FROM backtest_daily WHERE is_suspended IS NULL"
        )
        print(f"当前 is_suspended IS NULL 总数: {total_null['cnt']}\n")

        # 找同一天的 NULL 行
        sample_date = await conn.fetchrow(
            "SELECT ts FROM backtest_daily WHERE is_suspended IS NULL LIMIT 1"
        )
        if not sample_date:
            print("没有 NULL 行")
            await pool.close()
            return

        ts = sample_date["ts"]
        if hasattr(ts, "timetuple"):
            import calendar

            ts_ms = int(calendar.timegm(ts.timetuple()) * 1000)
            date_str = ts.strftime("%Y-%m-%d")
        else:
            from datetime import datetime, timezone

            ts_ms = int(ts)
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            date_str = dt.strftime("%Y-%m-%d")

        day_null = await conn.fetchrow(
            f"SELECT COUNT(*) as cnt FROM backtest_daily "
            f"WHERE ts = {ts_ms} AND is_suspended IS NULL"
        )
        print(f"选定日期: {date_str} (ts_ms={ts_ms})")
        print(f"该天 IS NULL 行数: {day_null['cnt']}\n")

        # 取 50 条测试
        rows = await conn.fetch(
            f"SELECT stock_code, open_price, high_price, low_price, close_price, "
            f"pre_close, vol, amount, turnover_ratio "
            f"FROM backtest_daily WHERE ts = {ts_ms} AND is_suspended IS NULL LIMIT 50"
        )
        print(f"取到 {len(rows)} 条用于测试\n")

        # 查 Tushare 停牌状态
        from src.data.clients.tushare_realtime import get_tushare_suspended_stocks

        print(f"查询 Tushare suspend_d ({date_str})...")
        suspended_codes = await get_tushare_suspended_stocks(date_str)
        print(f"  该日停牌股: {len(suspended_codes)} 只\n")

        cols = (
            "(stock_code,ts,open_price,high_price,low_price,close_price,"
            "pre_close,vol,amount,turnover_ratio,is_suspended)"
        )

        # 准备每条的正确 INSERT 值
        test_codes = []
        insert_map: dict[str, str] = {}
        for r in rows:
            code = r["stock_code"]
            test_codes.append(code)
            is_susp = code in suspended_codes
            tr = r["turnover_ratio"]
            tr_str = str(tr) if tr is not None else "NULL"

            if is_susp:
                pre_close = float(r["pre_close"]) if r["pre_close"] else 0.0
                fill = pre_close if pre_close > 0 else 0.0
                insert_map[code] = (
                    f"('{code}',{ts_ms},{fill},{fill},{fill},{fill},{pre_close},0.0,0.0,NULL,true)"
                )
            else:
                insert_map[code] = (
                    f"('{code}',{ts_ms},"
                    f"{r['open_price']},{r['high_price']},"
                    f"{r['low_price']},{r['close_price']},"
                    f"{r['pre_close']},{r['vol']},{r['amount']},"
                    f"{tr_str},false)"
                )

        in_clause = ",".join(f"'{c}'" for c in test_codes)

        async def check_null() -> int:
            r = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt FROM backtest_daily "
                f"WHERE ts = {ts_ms} AND stock_code IN ({in_clause}) "
                f"AND is_suspended IS NULL"
            )
            return int(r["cnt"])

        # === 测试: DELETE → INSERT → FLUSH → 验证 ===
        print(f"--- 逐条 DELETE {len(test_codes)} 行 ---")
        for code in test_codes:
            await conn.execute(
                f"DELETE FROM backtest_daily WHERE stock_code = '{code}' AND ts = {ts_ms}"
            )
        deleted_null = await check_null()
        print(f"  DELETE 后 IS NULL: {deleted_null}")

        # flush DELETE
        print("  FLUSH after DELETE...")
        await conn.execute("ADMIN FLUSH_TABLE('backtest_daily')")
        deleted_flushed = await check_null()
        print(f"  FLUSH 后 IS NULL: {deleted_flushed}")

        # 检查行是否真的消失了
        exists = await conn.fetchrow(
            f"SELECT COUNT(*) as cnt FROM backtest_daily "
            f"WHERE ts = {ts_ms} AND stock_code IN ({in_clause})"
        )
        print(f"  FLUSH 后总行数 (含非 NULL): {exists['cnt']}\n")

        print(f"--- 逐条 INSERT {len(test_codes)} 行（正确值）---")
        for code in test_codes:
            await conn.execute(f"INSERT INTO backtest_daily{cols} VALUES {insert_map[code]}")

        after_insert = await check_null()
        print(f"  INSERT 后立即查 IS NULL: {after_insert}")

        # flush INSERT
        print("  FLUSH after INSERT...")
        await conn.execute("ADMIN FLUSH_TABLE('backtest_daily')")
        after_insert_flush = await check_null()
        print(f"  FLUSH 后 IS NULL: {after_insert_flush}")

        # 等 3 秒
        await asyncio.sleep(3)
        after_wait = await check_null()
        print(f"  等 3 秒后 IS NULL: {after_wait}\n")

        # compact 试试
        print("--- ADMIN COMPACT_TABLE ---")
        try:
            await conn.execute("ADMIN COMPACT_TABLE('backtest_daily')")
            await asyncio.sleep(3)
            after_compact = await check_null()
            print(f"  COMPACT 后 IS NULL: {after_compact}\n")
        except Exception as e:
            print(f"  COMPACT 失败: {e}\n")
            after_compact = after_wait

        # 汇总
        if after_compact == 0:
            print("✅ DELETE + INSERT + FLUSH 成功！回填用这个方案")
        elif after_compact < len(test_codes):
            print(
                f"⚠️ 部分成功: {len(test_codes) - after_compact}/{len(test_codes)}, 可能需要 COMPACT"
            )
        else:
            print("❌ DELETE + INSERT 也扛不住 FLUSH，需要 DROP TABLE 重建")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
