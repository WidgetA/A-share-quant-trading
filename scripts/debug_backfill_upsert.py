"""诊断脚本：测试 GreptimeDB 对 is_suspended NULL 行的各种写入方式。

用法: uv run python scripts/debug_backfill_upsert.py

找一条 is_suspended IS NULL 的真实行，通过 Tushare suspend_d 查出
该股票当天是否真正停牌，然后用正确的值依次测试：
  1. 单行 INSERT upsert（不 DELETE）
  2. DELETE 再 INSERT
  3. DELETE 后等 2 秒再 INSERT
每步都 SELECT 验证结果。
"""

import asyncio
import os

import asyncpg


class _Conn(asyncpg.Connection):
    async def reset(self, *, timeout=None):
        pass  # GreptimeDB 不支持 RESET ALL


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
        # 找一条 NULL 行
        row = await conn.fetchrow(
            "SELECT stock_code, ts FROM backtest_daily "
            "WHERE is_suspended IS NULL LIMIT 1"
        )
        if not row:
            print("没有 is_suspended IS NULL 的行，无需修复！")
            await pool.close()
            return

        code = row["stock_code"]
        ts = row["ts"]
        if hasattr(ts, "timetuple"):
            import calendar

            ts_ms = int(calendar.timegm(ts.timetuple()) * 1000)
            date_str = ts.strftime("%Y-%m-%d")
            date_yyyymmdd = ts.strftime("%Y%m%d")
        else:
            from datetime import datetime, timezone

            ts_ms = int(ts)
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            date_str = dt.strftime("%Y-%m-%d")
            date_yyyymmdd = dt.strftime("%Y%m%d")

        print(f"=== 测试目标: stock_code={code}, date={date_str} (ts_ms={ts_ms}) ===\n")

        # 读取当前完整行
        full = await conn.fetchrow(
            f"SELECT * FROM backtest_daily "
            f"WHERE stock_code = '{code}' AND ts = {ts_ms}"
        )
        print(f"[当前值] is_suspended = {full['is_suspended']}")
        print(
            f"         open={full['open_price']}, close={full['close_price']}, vol={full['vol']}\n"
        )

        # 通过 Tushare 查真正的停牌状态
        from src.data.clients.tushare_realtime import get_tushare_suspended_stocks

        print(f"查询 Tushare suspend_d ({date_str})...")
        suspended_codes = await get_tushare_suspended_stocks(date_str)
        is_susp = code in suspended_codes
        susp_str = "true" if is_susp else "false"
        print(f"  该日停牌股数量: {len(suspended_codes)}")
        print(f"  {code} 停牌? {is_susp} → 写入 is_suspended={susp_str}\n")

        cols = (
            "(stock_code,ts,open_price,high_price,low_price,close_price,"
            "pre_close,vol,amount,turnover_ratio,is_suspended)"
        )
        tr = full["turnover_ratio"]
        tr_str = str(tr) if tr is not None else "NULL"

        if is_susp:
            pre_close = float(full["pre_close"]) if full["pre_close"] else 0.0
            fill = pre_close if pre_close > 0 else 0.0
            val = (
                f"('{code}',{ts_ms},"
                f"{fill},{fill},{fill},{fill},"
                f"{pre_close},0.0,0.0,NULL,true)"
            )
        else:
            val = (
                f"('{code}',{ts_ms},"
                f"{full['open_price']},{full['high_price']},"
                f"{full['low_price']},{full['close_price']},"
                f"{full['pre_close']},{full['vol']},{full['amount']},"
                f"{tr_str},false)"
            )

        # === 测试 1: 纯 upsert ===
        print("--- 测试 1: 单行 INSERT upsert（不 DELETE）---")
        result = await conn.execute(f"INSERT INTO backtest_daily{cols} VALUES {val}")
        print(f"  INSERT result: {result}")
        check = await conn.fetchrow(
            f"SELECT is_suspended FROM backtest_daily "
            f"WHERE stock_code = '{code}' AND ts = {ts_ms}"
        )
        print(f"  SELECT is_suspended = {check['is_suspended']}")
        if check["is_suspended"] is not None:
            print("  ✅ 纯 upsert 成功！后续回填用这个方案即可")
            await pool.close()
            return
        print("  ❌ 纯 upsert 失败，is_suspended 仍为 NULL\n")

        # === 测试 2: DELETE + INSERT ===
        print("--- 测试 2: DELETE 后立即 INSERT ---")
        del_result = await conn.execute(
            f"DELETE FROM backtest_daily "
            f"WHERE stock_code = '{code}' AND ts = {ts_ms}"
        )
        print(f"  DELETE result: {del_result}")

        gone = await conn.fetchrow(
            f"SELECT COUNT(*) as cnt FROM backtest_daily "
            f"WHERE stock_code = '{code}' AND ts = {ts_ms}"
        )
        print(f"  DELETE 后 COUNT = {gone['cnt']}")

        ins_result = await conn.execute(f"INSERT INTO backtest_daily{cols} VALUES {val}")
        print(f"  INSERT result: {ins_result}")
        check2 = await conn.fetchrow(
            f"SELECT is_suspended FROM backtest_daily "
            f"WHERE stock_code = '{code}' AND ts = {ts_ms}"
        )
        print(f"  SELECT is_suspended = {check2['is_suspended']}")
        if check2["is_suspended"] is not None:
            print("  ✅ DELETE + INSERT 成功！回填应该用 DELETE 再单行 INSERT")
            await pool.close()
            return
        print("  ❌ DELETE + INSERT 失败\n")

        # === 测试 3: DELETE + 等 2 秒 + INSERT ===
        print("--- 测试 3: DELETE 后等 2 秒再 INSERT ---")
        await conn.execute(
            f"DELETE FROM backtest_daily "
            f"WHERE stock_code = '{code}' AND ts = {ts_ms}"
        )
        print("  等待 2 秒...")
        await asyncio.sleep(2)

        gone2 = await conn.fetchrow(
            f"SELECT COUNT(*) as cnt FROM backtest_daily "
            f"WHERE stock_code = '{code}' AND ts = {ts_ms}"
        )
        print(f"  2 秒后 COUNT = {gone2['cnt']}")

        await conn.execute(f"INSERT INTO backtest_daily{cols} VALUES {val}")
        check3 = await conn.fetchrow(
            f"SELECT is_suspended FROM backtest_daily "
            f"WHERE stock_code = '{code}' AND ts = {ts_ms}"
        )
        print(f"  SELECT is_suspended = {check3['is_suspended']}")
        if check3["is_suspended"] is not None:
            print("  ✅ DELETE + 等待 + INSERT 成功！回填需要在 DELETE 后加延迟")
        else:
            print("  ❌ 全部失败，可能需要 DROP TABLE 重建")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
