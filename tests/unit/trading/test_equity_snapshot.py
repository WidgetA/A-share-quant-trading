# === MODULE PURPOSE ===
# Tests for TRD-001 account equity snapshot (src/trading/equity_snapshot.py):
# - compute_weekly_returns(): ISO-week aggregation + return math + edge cases
# - EquitySnapshotStore: upsert SQL (Beijing-day-00:00 ts, escaping, one row
#   per day via fixed ts) and list_snapshots row conversion
#
# The fake db records SQL strings; store SQL runs against a real GreptimeDB
# in deployment (same stubbing approach as tests/unit/notes/*).

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from src.trading.equity_snapshot import (
    EquitySnapshotStore,
    _beijing_date_to_utc_ms,
    compute_weekly_returns,
)


def _snap(d: str, asset: float) -> dict:
    return {"date": d, "total_asset": asset}


class TestComputeWeeklyReturns:
    def test_return_is_week_end_over_prev_week_end(self):
        # 2026-06-29(周一)~07-03(周五) 一周, 07-06 下一周
        snaps = [
            _snap("2026-06-29", 100000.0),
            _snap("2026-07-01", 103000.0),
            _snap("2026-07-03", 110000.0),  # 第一周收于 11万
            _snap("2026-07-06", 104500.0),  # 第二周收于 10.45万
        ]
        out = compute_weekly_returns(snaps)
        assert len(out) == 2
        # 第一周: 没有上周基数 → 不报数(0.22.1:拿周内点当基数会吞掉周初盈亏)
        assert out[0]["return_pct"] is None
        assert out[0]["pnl_amount"] is None
        assert out[0]["start_date"] == "2026-06-29"
        assert out[0]["end_date"] == "2026-07-03"
        # 第二周: 10.45/11 - 1 = -5%,金额 -5500
        assert out[1]["return_pct"] == -5.0
        assert out[1]["pnl_amount"] == -5500.0
        assert out[1]["end_asset"] == 104500.0

    def test_first_week_single_snapshot_has_no_return(self):
        out = compute_weekly_returns([_snap("2026-07-06", 100000.0)])
        assert len(out) == 1
        assert out[0]["return_pct"] is None

    def test_second_week_single_snapshot_still_computes(self):
        snaps = [_snap("2026-07-03", 100000.0), _snap("2026-07-06", 101000.0)]
        out = compute_weekly_returns(snaps)
        assert out[1]["return_pct"] == 1.0
        assert out[1]["pnl_amount"] == 1000.0

    def test_zero_base_yields_none_not_crash(self):
        snaps = [_snap("2026-07-03", 0.0), _snap("2026-07-06", 101000.0)]
        out = compute_weekly_returns(snaps)
        assert out[0]["return_pct"] is None  # 单笔首周
        assert out[1]["return_pct"] is None  # 上周基数为 0

    def test_max_weeks_keeps_most_recent(self):
        # 20 个连续周一,各一笔
        snaps = []
        from datetime import date, timedelta

        d = date(2026, 1, 5)  # 周一
        for i in range(20):
            snaps.append(_snap((d + timedelta(weeks=i)).isoformat(), 100000.0 + i))
        out = compute_weekly_returns(snaps, max_weeks=12)
        assert len(out) == 12
        assert out[-1]["end_asset"] == 100019.0

    def test_iso_year_boundary_weeks_stay_separate(self):
        # 2025-12-29(周一, ISO 2026-W01) 与 2026-01-05(ISO 2026-W02)
        snaps = [
            _snap("2025-12-26", 100000.0),  # ISO 2025-W52 (周五)
            _snap("2025-12-29", 102000.0),  # ISO 2026-W01
            _snap("2026-01-05", 103020.0),  # ISO 2026-W02
        ]
        out = compute_weekly_returns(snaps)
        assert len(out) == 3
        assert out[1]["return_pct"] == 2.0
        assert out[2]["return_pct"] == 1.0


class TestBeijingDateToUtcMs:
    def test_beijing_midnight_is_utc_minus_8h(self):
        ms = _beijing_date_to_utc_ms("2026-07-07")
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        assert dt == datetime(2026, 7, 6, 16, 0, 0, tzinfo=timezone.utc)

    def test_same_day_always_same_ts(self):
        assert _beijing_date_to_utc_ms("2026-07-07") == _beijing_date_to_utc_ms("2026-07-07")


class _FakeDB:
    def __init__(self, rows: list[dict] | None = None, row: dict | None = None):
        self._rows = rows or []
        self._row = row
        self.executed: list[str] = []

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)

    async def fetch(self, sql: str):
        self.executed.append(sql)
        return self._rows

    async def fetchrow(self, sql: str):
        self.executed.append(sql)
        return self._row


def _store(db: _FakeDB) -> EquitySnapshotStore:
    return EquitySnapshotStore(SimpleNamespace(db=db))


class TestEquitySnapshotStore:
    def test_upsert_writes_fixed_beijing_day_ts(self):
        db = _FakeDB()
        asyncio.run(
            _store(db).upsert_snapshot(
                account_id="123456",
                trade_date="2026-07-07",
                total_asset=250000.5,
                cash=50000.0,
                market_value=200000.5,
            )
        )
        assert len(db.executed) == 1
        sql = db.executed[0]
        expected_ts = _beijing_date_to_utc_ms("2026-07-07")
        assert f"VALUES ({expected_ts}, '123456', '2026-07-07'" in sql
        assert "250000.5" in sql and "50000.0" in sql and "200000.5" in sql
        assert "'broker'" in sql  # 默认 source

    def test_upsert_manual_source_and_rejects_bad_source(self):
        db = _FakeDB()
        asyncio.run(
            _store(db).upsert_snapshot(
                account_id="1",
                trade_date="2026-07-02",
                total_asset=1002872.83,
                cash=0.0,
                market_value=0.0,
                source="manual",
            )
        )
        assert "'manual'" in db.executed[0]
        with pytest.raises(ValueError):
            asyncio.run(
                _store(db).upsert_snapshot(
                    account_id="1",
                    trade_date="2026-07-02",
                    total_asset=1.0,
                    cash=0.0,
                    market_value=0.0,
                    source="hacker",
                )
            )

    def test_delete_manual_snapshot_only_deletes_manual(self):
        # broker 行:拒删
        db = _FakeDB(
            row={
                "trade_date": "2026-07-07",
                "total_asset": 100.0,
                "cash": 1.0,
                "market_value": 99.0,
                "source": "broker",
            }
        )
        with pytest.raises(ValueError):
            asyncio.run(_store(db).delete_manual_snapshot("1", "2026-07-07"))
        assert not any("DELETE" in s for s in db.executed)
        # manual 行:删除
        db2 = _FakeDB(
            row={
                "trade_date": "2026-07-02",
                "total_asset": 100.0,
                "cash": None,
                "market_value": None,
                "source": "manual",
            }
        )
        assert asyncio.run(_store(db2).delete_manual_snapshot("1", "2026-07-02")) is True
        assert any("DELETE FROM account_equity_snapshot" in s for s in db2.executed)
        # 不存在:False
        db3 = _FakeDB(row=None)
        assert asyncio.run(_store(db3).delete_manual_snapshot("1", "2026-07-02")) is False

    def test_upsert_rejects_bad_date_and_empty_account(self):
        db = _FakeDB()
        with pytest.raises(ValueError):
            asyncio.run(
                _store(db).upsert_snapshot(
                    account_id="1",
                    trade_date="07/07/2026",
                    total_asset=1.0,
                    cash=0.0,
                    market_value=0.0,
                )
            )
        with pytest.raises(ValueError):
            asyncio.run(
                _store(db).upsert_snapshot(
                    account_id="",
                    trade_date="2026-07-07",
                    total_asset=1.0,
                    cash=0.0,
                    market_value=0.0,
                )
            )
        assert db.executed == []

    def test_upsert_escapes_account_id(self):
        db = _FakeDB()
        asyncio.run(
            _store(db).upsert_snapshot(
                account_id="a'b",
                trade_date="2026-07-07",
                total_asset=1.0,
                cash=0.0,
                market_value=0.0,
            )
        )
        assert "'a''b'" in db.executed[0]

    def test_list_snapshots_ascending_and_skips_null_asset(self):
        # 查询 ORDER BY trade_date DESC — fake 按 DESC 给行
        def _row(d: str, asset, cash, mv, source=None) -> dict:
            return {
                "trade_date": d,
                "total_asset": asset,
                "cash": cash,
                "market_value": mv,
                "source": source,
            }

        db = _FakeDB(
            rows=[
                _row("2026-07-07", 102.0, 2.0, 100.0),
                _row("2026-07-06", None, 1.0, 1.0),
                _row("2026-07-03", 100.0, None, None, source="manual"),
            ]
        )
        out = asyncio.run(_store(db).list_snapshots(account_id="123456", days=30))
        assert [s["date"] for s in out] == ["2026-07-03", "2026-07-07"]
        assert out[0]["cash"] is None
        assert out[0]["source"] == "manual"
        assert out[1]["total_asset"] == 102.0
        assert out[1]["source"] == "broker"  # 旧行 NULL 视同 broker
        assert "WHERE account_id = '123456'" in db.executed[0]
        assert "LIMIT 30" in db.executed[0]

    def test_list_snapshots_clamps_days(self):
        db = _FakeDB()
        asyncio.run(_store(db).list_snapshots(days=999999))
        assert "LIMIT 3650" in db.executed[0]
        db2 = _FakeDB()
        asyncio.run(_store(db2).list_snapshots(days=0))
        assert "LIMIT 1" in db2.executed[0]
