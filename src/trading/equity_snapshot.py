# === MODULE PURPOSE ===
# 每日账户资产快照 (`account_equity_snapshot` 表) — TRD-001 账户概览的数据底座。
#
# 每账户每北京日一行:ts 固定为该北京日 00:00 对应的 UTC epoch ms,broker 轮询
# 期间反复 upsert(相同 PK+ts → mito 原地覆写),当日最后一笔写入即当日收盘值。
# 历史无法回填(broker 无历史资产接口),曲线从部署当天开始积累。
#
# 与 note_store 相同的存储约定:raw inlined SQL(GreptimeDB 不支持 PREPARE),
# 字符串经 `_q()` 转义,数值经 float()/int() 强转后内联。

from __future__ import annotations

import calendar
import logging
from datetime import date, datetime, timedelta, timezone

logger = logging.getLogger(__name__)

_CREATE_EQUITY_SNAPSHOT_SQL = """
CREATE TABLE IF NOT EXISTS account_equity_snapshot (
    ts           TIMESTAMP TIME INDEX,
    account_id   STRING,
    trade_date   STRING,
    total_asset  FLOAT64,
    cash         FLOAT64,
    market_value FLOAT64,
    updated_at   INT64,
    PRIMARY KEY (account_id, trade_date)
)
"""


def _q(s: str) -> str:
    """SQL-escape a string for inlining: wrap in single quotes, double internal quotes."""
    return "'" + s.replace("'", "''") + "'"


def _beijing_date_to_utc_ms(trade_date: str) -> int:
    """'YYYY-MM-DD' 北京日 00:00 → UTC epoch ms (CLAUDE.md §7: calendar.timegm 口径)。"""
    y, m, d = (int(p) for p in trade_date.split("-"))
    naive_utc = datetime(y, m, d) - timedelta(hours=8)
    return calendar.timegm(naive_utc.timetuple()) * 1000


class EquitySnapshotStore:
    """CRUD for `account_equity_snapshot`.

    Reuses the long-lived `GreptimeBacktestStorage.db` connection pool from
    `app.state.storage` — do NOT instantiate a separate pool.
    """

    def __init__(self, storage) -> None:
        # storage: GreptimeBacktestStorage (typed loosely to avoid import cycle)
        self._db = storage.db

    async def ensure_schema(self) -> None:
        await self._db.execute(_CREATE_EQUITY_SNAPSHOT_SQL)

    async def upsert_snapshot(
        self,
        *,
        account_id: str,
        trade_date: str,
        total_asset: float,
        cash: float,
        market_value: float,
    ) -> None:
        """写入/覆写 (account_id, trade_date) 当日快照。

        ts 固定为北京日 00:00 → 同一天反复写只保留最后一笔(mito PK+ts 去重)。
        """
        if not account_id:
            raise ValueError("account_id is required")
        # 校验日期格式顺带防注入(非法格式直接抛 ValueError)
        date.fromisoformat(trade_date)
        ts_ms = _beijing_date_to_utc_ms(trade_date)
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        sql = (
            "INSERT INTO account_equity_snapshot "
            "(ts, account_id, trade_date, total_asset, cash, market_value, updated_at) "
            f"VALUES ({ts_ms}, {_q(account_id)}, {_q(trade_date)}, "
            f"{float(total_asset)}, {float(cash)}, {float(market_value)}, {now_ms})"
        )
        await self._db.execute(sql)

    async def list_snapshots(self, account_id: str | None = None, days: int = 365) -> list[dict]:
        """近 `days` 天快照,按日期升序。每行一天,直接读 trade_date 字符串列。"""
        days = max(1, min(int(days), 3650))
        where = f"WHERE account_id = {_q(account_id)} " if account_id else ""
        sql = (
            "SELECT trade_date, total_asset, cash, market_value "
            f"FROM account_equity_snapshot {where}"
            f"ORDER BY trade_date DESC LIMIT {days}"
        )
        rows = await self._db.fetch(sql)
        out: list[dict] = []
        for r in rows:
            if r["total_asset"] is None:
                continue
            out.append(
                {
                    "date": r["trade_date"],
                    "total_asset": float(r["total_asset"]),
                    "cash": float(r["cash"]) if r["cash"] is not None else None,
                    "market_value": (
                        float(r["market_value"]) if r["market_value"] is not None else None
                    ),
                }
            )
        out.reverse()
        return out


def compute_weekly_returns(snapshots: list[dict], max_weeks: int = 12) -> list[dict]:
    """按 ISO 周(周一起始)聚合周收益率。纯函数,方便单测。

    snapshots: 按日期**升序**的 [{"date": "YYYY-MM-DD", "total_asset": float}, ...]
    (list_snapshots 的返回即此格式)。

    口径:
    - 周收益 = 本周最后一笔 total_asset ÷ 上周最后一笔 − 1
    - 最早一周没有上周基数 → 用该周第一笔做基数;该周只有一笔 → return_pct=None
    - 基数 ≤ 0 → return_pct=None(绝不产生除零/荒谬值)

    返回最近 max_weeks 周,升序,每项:
    {year, week, start_date, end_date, end_asset, return_pct(百分数,2位小数)|None}
    """
    weeks: list[dict] = []
    for snap in snapshots:
        iso = date.fromisoformat(snap["date"]).isocalendar()
        key = (iso[0], iso[1])
        asset = float(snap["total_asset"])
        if not weeks or weeks[-1]["_key"] != key:
            weeks.append(
                {
                    "_key": key,
                    "year": iso[0],
                    "week": iso[1],
                    "start_date": snap["date"],
                    "end_date": snap["date"],
                    "_first_asset": asset,
                    "end_asset": asset,
                }
            )
        else:
            weeks[-1]["end_date"] = snap["date"]
            weeks[-1]["end_asset"] = asset

    out: list[dict] = []
    prev_end: float | None = None
    for w in weeks:
        ret: float | None
        if prev_end is not None:
            ret = (w["end_asset"] / prev_end - 1.0) if prev_end > 0 else None
        elif w["end_date"] != w["start_date"] and w["_first_asset"] > 0:
            ret = w["end_asset"] / w["_first_asset"] - 1.0
        else:
            ret = None
        out.append(
            {
                "year": w["year"],
                "week": w["week"],
                "start_date": w["start_date"],
                "end_date": w["end_date"],
                "end_asset": w["end_asset"],
                "return_pct": None if ret is None else round(ret * 100, 2),
            }
        )
        prev_end = w["end_asset"]
    return out[-max_weeks:]
