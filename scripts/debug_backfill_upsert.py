"""诊断脚本：测试 GreptimeDB 批量单行 INSERT 后 IS NULL 是否真正消除。

用法: python scripts/debug_backfill_upsert.py

测试：
  1. 找 100 条 is_suspended IS NULL 行
  2. 通过 Tushare 查真正停牌状态，逐条 INSERT 正确值
  3. 不 flush：立即 COUNT ... IS NULL
  4. flush：ADMIN FLUSH_TABLE 后再 COUNT ... IS NULL
  5. 等 3 秒后再 COUNT（排除延迟可能）
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

        # 找同一天的 100 条 NULL 行
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

        # 该天的 NULL 总数
        day_null = await conn.fetchrow(
            f"SELECT COUNT(*) as cnt FROM backtest_daily "
            f"WHERE ts = {ts_ms} AND is_suspended IS NULL"
        )
        print(f"选定日期: {date_str} (ts_ms={ts_ms})")
        print(f"该天 IS NULL 行数: {day_null['cnt']}\n")

        # 取 100 条
        rows = await conn.fetch(
            f"SELECT stock_code, open_price, high_price, low_price, close_price, "
            f"pre_close, vol, amount, turnover_ratio "
            f"FROM backtest_daily WHERE ts = {ts_ms} AND is_suspended IS NULL LIMIT 100"
        )
        print(f"取到 {len(rows)} 条用于测试\n")

        # 查 Tushare 停牌状态
        from src.data.clients.tushare_realtime import get_tushare_suspended_stocks

        print(f"查询 Tushare suspend_d ({date_str})...")
        suspended_codes = await get_tushare_suspended_stocks(date_str)
        print(f"  该日停牌股: {len(suspended_codes)} 只\n")

        # 逐条 INSERT
        cols = (
            "(stock_code,ts,open_price,high_price,low_price,close_price,"
            "pre_close,vol,amount,turnover_ratio,is_suspended)"
        )
        test_codes = []
        print(f"开始逐条 INSERT {len(rows)} 行...")
        for r in rows:
            code = r["stock_code"]
            test_codes.append(code)
            is_susp = code in suspended_codes
            tr = r["turnover_ratio"]
            tr_str = str(tr) if tr is not None else "NULL"

            if is_susp:
                pre_close = float(r["pre_close"]) if r["pre_close"] else 0.0
                fill = pre_close if pre_close > 0 else 0.0
                val = (
                    f"('{code}',{ts_ms},{fill},{fill},{fill},{fill},{pre_close},0.0,0.0,NULL,true)"
                )
            else:
                val = (
                    f"('{code}',{ts_ms},"
                    f"{r['open_price']},{r['high_price']},"
                    f"{r['low_price']},{r['close_price']},"
                    f"{r['pre_close']},{r['vol']},{r['amount']},"
                    f"{tr_str},false)"
                )
            await conn.execute(f"INSERT INTO backtest_daily{cols} VALUES {val}")

        print(f"INSERT 完成: {len(test_codes)} 条\n")

        # 验证 1: 立即查
        in_clause = ",".join(f"'{c}'" for c in test_codes)
        imm = await conn.fetchrow(
            f"SELECT COUNT(*) as cnt FROM backtest_daily "
            f"WHERE ts = {ts_ms} AND stock_code IN ({in_clause}) AND is_suspended IS NULL"
        )
        print("--- 验证 1: 立即查 ---")
        print(f"  这 {len(test_codes)} 条中仍 IS NULL: {imm['cnt']}\n")

        # 验证 2: flush 后查
        print("--- 验证 2: ADMIN FLUSH_TABLE 后查 ---")
        try:
            flush_result = await conn.execute("ADMIN FLUSH_TABLE('backtest_daily')")
            print(f"  FLUSH result: {flush_result}")
        except Exception as e:
            print(f"  FLUSH 失败: {e}")

        after_flush = await conn.fetchrow(
            f"SELECT COUNT(*) as cnt FROM backtest_daily "
            f"WHERE ts = {ts_ms} AND stock_code IN ({in_clause}) AND is_suspended IS NULL"
        )
        print(f"  这 {len(test_codes)} 条中仍 IS NULL: {after_flush['cnt']}\n")

        # 验证 3: 等 3 秒后查
        print("--- 验证 3: 等 3 秒后查 ---")
        await asyncio.sleep(3)
        after_wait = await conn.fetchrow(
            f"SELECT COUNT(*) as cnt FROM backtest_daily "
            f"WHERE ts = {ts_ms} AND stock_code IN ({in_clause}) AND is_suspended IS NULL"
        )
        print(f"  这 {len(test_codes)} 条中仍 IS NULL: {after_wait['cnt']}\n")

        # 汇总
        if after_wait["cnt"] == 0:
            print("✅ 全部修复成功（可能需要等待或 flush）")
        elif after_wait["cnt"] < len(test_codes):
            print(f"⚠️ 部分成功: {len(test_codes) - after_wait['cnt']}/{len(test_codes)}")
        else:
            print("❌ 全部失败")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
