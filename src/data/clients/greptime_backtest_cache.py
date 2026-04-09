# === MODULE PURPOSE ===
# Backtest data cache backed by GreptimeDB (replaces pickle+OSS TsanghiBacktestCache).
# Provides GreptimeBacktestCache for async read/write/download and
# GreptimeHistoricalAdapter implementing HistoricalDataProvider for MomentumScanner.
#
# === KEY CONCEPTS ===
# - All reads go through SQL via asyncpg (PostgreSQL wire protocol, port 4003)
# - Natural upsert: same (stock_code, ts) = last write wins
# - No transactions needed — each INSERT is independent (GreptimeDB handles flush internally)
# - Volume stored in 手 (lots) — adapter converts ×100 at read time
# - Data sources: tsanghi (daily OHLCV), Tushare Pro stk_mins (1-min bars for 9:40 snapshot)
#
# === TABLES ===
# backtest_daily:  stock_code(TAG), ts(TIME INDEX), open_price, high_price,
#                  low_price, close_price, pre_close, vol, amount, turnover_ratio,
#                  is_suspended
# backtest_minute: stock_code(TAG), ts(TIME INDEX), close_940, cum_volume,
#                  max_high, min_low

from __future__ import annotations

import asyncio
import calendar
import logging
import threading
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, NamedTuple

import asyncpg

logger = logging.getLogger(__name__)

# Cancel checker — compatible with both threading.Event and simple callables
_CancelChecker = threading.Event | None


# ---------------------------------------------------------------------------
# Data transfer object — same field names as old DailyBar for caller compat
# ---------------------------------------------------------------------------


class DailyBar(NamedTuple):
    """Daily OHLCV record. Field names match old TsanghiBacktestCache.DailyBar."""

    open: float
    high: float
    low: float
    close: float
    preClose: float
    volume: float  # in 手 (lots); adapter converts ×100 at read time
    amount: float
    turnoverRatio: float | None
    is_suspended: bool = False


# ---------------------------------------------------------------------------
# Low-level GreptimeDB client via asyncpg (PostgreSQL wire protocol, port 4003)
# ---------------------------------------------------------------------------

_CREATE_DAILY_SQL = """
CREATE TABLE IF NOT EXISTS backtest_daily (
    stock_code STRING,
    ts TIMESTAMP TIME INDEX,
    open_price FLOAT64,
    high_price FLOAT64,
    low_price FLOAT64,
    close_price FLOAT64,
    pre_close FLOAT64,
    vol FLOAT64,
    amount FLOAT64,
    turnover_ratio FLOAT64,
    is_suspended BOOLEAN,
    PRIMARY KEY (stock_code)
)
"""

_CREATE_MINUTE_SQL = """
CREATE TABLE IF NOT EXISTS backtest_minute (
    stock_code STRING,
    ts TIMESTAMP TIME INDEX,
    close_940 FLOAT64,
    cum_volume FLOAT64,
    max_high FLOAT64,
    min_low FLOAT64,
    PRIMARY KEY (stock_code)
)
"""

_CREATE_STOCK_LIST_SQL = """
CREATE TABLE IF NOT EXISTS stock_list (
    stock_code STRING,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (stock_code)
)
"""


def _date_to_epoch_ms(d: date) -> int:
    """Convert date to epoch milliseconds (midnight UTC)."""
    return calendar.timegm(d.timetuple()) * 1000


def _epoch_ms_to_date(ms: int | float) -> date:
    """Convert epoch milliseconds to date."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()


def _parse_date_str(date_str: str) -> date:
    """Parse YYYY-MM-DD string to date."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def _ts_to_date(val: Any) -> date:
    """Convert asyncpg timestamp result to date.

    GreptimeDB pgwire returns timestamps as datetime objects.
    """
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, (int, float)):
        return _epoch_ms_to_date(val)
    if isinstance(val, str):
        dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        return dt.date()
    raise TypeError(f"Cannot convert {type(val)} to date: {val}")


def _ts_to_epoch_ms(val: Any) -> int:
    """Convert asyncpg timestamp result to epoch ms."""
    if isinstance(val, datetime):
        return int(val.replace(tzinfo=timezone.utc).timestamp() * 1000)
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    raise TypeError(f"Cannot convert {type(val)} to epoch ms: {val}")


class _GreptimeConnection(asyncpg.Connection):
    """asyncpg connection subclass that disables reset for GreptimeDB.

    GreptimeDB doesn't support RESET ALL / DEALLOCATE ALL which asyncpg
    runs when returning connections to the pool. Override to no-op.
    """

    async def reset(self, *, timeout: float | None = None) -> None:  # type: ignore[override]
        pass


class GreptimeClient:
    """Low-level async client for GreptimeDB via PostgreSQL wire protocol (port 4003).

    Uses asyncpg with GreptimeDB-specific settings:
    - statement_cache_size=0 (GreptimeDB doesn't support DEALLOCATE/PREPARE)
    - Connection pool for safe concurrent access (download + status polling)
    - Custom connection class with no-op reset (GreptimeDB doesn't support RESET ALL)
    - All queries have a 120s timeout to prevent indefinite hangs
    """

    _QUERY_TIMEOUT: float = 120.0  # seconds
    _ACQUIRE_TIMEOUT: float = 30.0  # seconds

    def __init__(self, host: str, port: int, database: str = "public") -> None:
        self._host = host
        self._port = port
        self._database = database
        self._pool: asyncpg.Pool | None = None

    async def start(self) -> None:
        self._pool = await asyncpg.create_pool(
            host=self._host,
            port=self._port,
            database=self._database,
            user="greptime",
            min_size=0,
            max_size=3,
            statement_cache_size=0,
            connection_class=_GreptimeConnection,
        )

    async def stop(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    @property
    def is_connected(self) -> bool:
        return self._pool is not None and not self._pool._closed

    async def execute(self, sql: str) -> str:
        """Execute DDL/DML and return status string."""
        if not self._pool:
            raise RuntimeError("GreptimeClient not started")
        async with self._pool.acquire(timeout=self._ACQUIRE_TIMEOUT) as conn:
            return await asyncio.wait_for(conn.execute(sql), timeout=self._QUERY_TIMEOUT)

    async def fetch(self, sql: str) -> list[asyncpg.Record]:
        """Execute SELECT and return rows."""
        if not self._pool:
            raise RuntimeError("GreptimeClient not started")
        async with self._pool.acquire(timeout=self._ACQUIRE_TIMEOUT) as conn:
            return await asyncio.wait_for(conn.fetch(sql), timeout=self._QUERY_TIMEOUT)

    async def fetchrow(self, sql: str) -> asyncpg.Record | None:
        """Execute SELECT and return first row."""
        if not self._pool:
            raise RuntimeError("GreptimeClient not started")
        async with self._pool.acquire(timeout=self._ACQUIRE_TIMEOUT) as conn:
            return await asyncio.wait_for(conn.fetchrow(sql), timeout=self._QUERY_TIMEOUT)


# ---------------------------------------------------------------------------
# Main cache class
# ---------------------------------------------------------------------------

# Integrity thresholds
_MIN_EXPECTED_STOCKS = 2500
_MAX_EXPECTED_STOCKS = 5500
_MIN_STOCKS_PER_DAY = 1000
_MIN_MINUTE_COVERAGE = 0.5


class GreptimeBacktestCache:
    """Backtest data cache backed by GreptimeDB.

    Replaces TsanghiBacktestCache + OSS persistence.
    All reads go through SQL — no in-memory caching.

    Usage:
        cache = GreptimeBacktestCache("localhost", 4003)
        await cache.start()
        await cache.download_prices(start_date, end_date, progress_cb)
        bar = await cache.get_daily("600519", "2024-06-01")
        await cache.stop()
    """

    def __init__(self, host: str = "localhost", port: int = 4003, database: str = "public") -> None:
        self._db = GreptimeClient(host, port, database)

    async def start(self) -> None:
        """Connect to GreptimeDB and ensure tables exist."""
        await self._db.start()
        await self._db.execute(_CREATE_DAILY_SQL)
        await self._db.execute(_CREATE_MINUTE_SQL)
        await self._db.execute(_CREATE_STOCK_LIST_SQL)
        # Add is_suspended column if missing (CREATE IF NOT EXISTS won't alter)
        try:
            await self._db.execute("ALTER TABLE backtest_daily ADD COLUMN is_suspended BOOLEAN")
            logger.info("Added is_suspended column to backtest_daily")
        except Exception:  # safety: ignore — column already exists
            pass
        logger.info(f"GreptimeBacktestCache connected via pgwire {self._db._host}:{self._db._port}")

    async def stop(self) -> None:
        """Close connection."""
        await self._db.stop()

    @property
    def is_ready(self) -> bool:
        return self._db.is_connected

    # ==================== Read Methods ====================

    async def get_daily(self, code: str, date_str: str) -> DailyBar | None:
        """Get daily OHLCV for a stock on a date. date_str is YYYY-MM-DD."""
        ms = _date_to_epoch_ms(_parse_date_str(date_str))
        row = await self._db.fetchrow(
            f"SELECT open_price, high_price, low_price, close_price, pre_close, "
            f"vol, amount, turnover_ratio, is_suspended "
            f"FROM backtest_daily "
            f"WHERE stock_code = '{code}' AND ts = {ms}"
        )
        if not row:
            return None
        return DailyBar(
            open=float(row["open_price"]),
            high=float(row["high_price"]),
            low=float(row["low_price"]),
            close=float(row["close_price"]),
            preClose=float(row["pre_close"]),
            volume=float(row["vol"]),
            amount=float(row["amount"]),
            turnoverRatio=row["turnover_ratio"],
            is_suspended=bool(row["is_suspended"]) if row["is_suspended"] is not None else False,
        )

    async def get_940_price(
        self, code: str, date_str: str
    ) -> tuple[float, float, float, float] | None:
        """Get 9:40 price data: (close, cum_volume, max_high, min_low)."""
        ms = _date_to_epoch_ms(_parse_date_str(date_str))
        row = await self._db.fetchrow(
            f"SELECT close_940, cum_volume, max_high, min_low "
            f"FROM backtest_minute "
            f"WHERE stock_code = '{code}' AND ts = {ms}"
        )
        if not row:
            return None
        return (
            float(row["close_940"]),
            float(row["cum_volume"]),
            float(row["max_high"]),
            float(row["min_low"]),
        )

    async def get_all_codes_with_daily(self, date_str: str) -> dict[str, DailyBar]:
        """Get daily data for ALL stocks on a specific date."""
        ms = _date_to_epoch_ms(_parse_date_str(date_str))
        rows = await self._db.fetch(
            f"SELECT stock_code, open_price, high_price, low_price, close_price, "
            f"pre_close, vol, amount, turnover_ratio, is_suspended "
            f"FROM backtest_daily WHERE ts = {ms}"
        )
        result: dict[str, DailyBar] = {}
        for r in rows:
            result[r["stock_code"]] = DailyBar(
                open=float(r["open_price"]),
                high=float(r["high_price"]),
                low=float(r["low_price"]),
                close=float(r["close_price"]),
                preClose=float(r["pre_close"]),
                volume=float(r["vol"]),
                amount=float(r["amount"]),
                turnoverRatio=r["turnover_ratio"],
                is_suspended=bool(r["is_suspended"]) if r["is_suspended"] is not None else False,
            )
        return result

    async def get_multi_day_history(self, start_date: str, end_date: str) -> dict[str, list[tuple]]:
        """Get daily OHLCV for ALL non-suspended stocks across a date range.

        Single SQL query, no memory caching. Volume is converted from
        手 (lots) to 股 (shares) at read time (×100).

        Args:
            start_date: Start date (YYYY-MM-DD), inclusive.
            end_date: End date (YYYY-MM-DD), inclusive.

        Returns:
            {code: [(date, open, high, low, close, volume_in_shares), ...]}
            sorted by date ascending per stock.
        """
        start_ms = _date_to_epoch_ms(_parse_date_str(start_date))
        end_ms = _date_to_epoch_ms(_parse_date_str(end_date))
        rows = await self._db.fetch(
            f"SELECT stock_code, ts, open_price, high_price, low_price, "
            f"close_price, vol "
            f"FROM backtest_daily "
            f"WHERE ts >= {start_ms} AND ts <= {end_ms} "
            f"AND (is_suspended = false OR is_suspended IS NULL) "
            f"ORDER BY stock_code, ts"
        )

        result: dict[str, list[tuple]] = {}
        for r in rows:
            code = r["stock_code"]
            bar = (
                _ts_to_date(r["ts"]),
                float(r["open_price"]),
                float(r["high_price"]),
                float(r["low_price"]),
                float(r["close_price"]),
                float(r["vol"]) * 100,  # 手 → 股
            )
            if code not in result:
                result[code] = []
            result[code].append(bar)

        logger.info(
            "get_multi_day_history: %s..%s → %d stocks, %d total bars",
            start_date,
            end_date,
            len(result),
            len(rows),
        )
        return result

    async def get_stock_codes(self) -> list[str]:
        """Get all unique stock codes in daily table."""
        rows = await self._db.fetch(
            "SELECT DISTINCT stock_code FROM backtest_daily ORDER BY stock_code"
        )
        return [r["stock_code"] for r in rows]

    async def get_date_range(self) -> tuple[date | None, date | None]:
        """Get (min_date, max_date) of daily data."""
        row = await self._db.fetchrow(
            "SELECT MIN(ts) as min_ts, MAX(ts) as max_ts FROM backtest_daily"
        )
        if not row or row["min_ts"] is None:
            return (None, None)
        return (_ts_to_date(row["min_ts"]), _ts_to_date(row["max_ts"]))

    async def get_daily_stock_count(self) -> int:
        """Count distinct stock codes in daily table."""
        row = await self._db.fetchrow(
            "SELECT COUNT(DISTINCT stock_code) as cnt FROM backtest_daily"
        )
        return int(row["cnt"]) if row else 0

    async def get_minute_stock_count(self) -> int:
        """Count distinct stock codes in minute table."""
        row = await self._db.fetchrow(
            "SELECT COUNT(DISTINCT stock_code) as cnt FROM backtest_minute"
        )
        return int(row["cnt"]) if row else 0

    async def get_daily_date_count(self) -> int:
        """Count distinct trading dates in daily table."""
        row = await self._db.fetchrow("SELECT COUNT(DISTINCT ts) as cnt FROM backtest_daily")
        return int(row["cnt"]) if row else 0

    # ==================== Range / Gap Detection ====================

    async def covers_range(self, start_date: date, end_date: date) -> bool:
        """Check if cached data covers the requested date range."""
        db_start, db_end = await self.get_date_range()
        if db_start is None or db_end is None:
            return False
        return db_start <= start_date and db_end >= end_date

    async def find_minute_gaps(self) -> list[tuple[date, date]]:
        """Find date ranges where daily exists but minute data is sparse/missing.

        A date is a "gap" if daily_count > 100 and minute_count < 50% of daily_count.
        Returns sorted list of contiguous gap ranges.
        """
        daily_rows = await self._db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_daily GROUP BY ts"
        )
        minute_rows = await self._db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_minute GROUP BY ts"
        )

        daily_counts: dict[int, int] = {}
        for r in daily_rows:
            ts_ms = _ts_to_epoch_ms(r["ts"])
            daily_counts[ts_ms] = int(r["cnt"])

        minute_counts: dict[int, int] = {}
        for r in minute_rows:
            ts_ms = _ts_to_epoch_ms(r["ts"])
            minute_counts[ts_ms] = int(r["cnt"])

        gap_dates: list[date] = []
        for ts_ms, daily_count in daily_counts.items():
            if daily_count <= 100:
                continue
            minute_count = minute_counts.get(ts_ms, 0)
            if minute_count < daily_count * _MIN_MINUTE_COVERAGE:
                gap_dates.append(_epoch_ms_to_date(ts_ms))

        if not gap_dates:
            return []

        gap_dates.sort()
        return _group_contiguous_dates(gap_dates)

    async def missing_ranges(self, start_date: date, end_date: date) -> list[tuple[date, date]]:
        """Return date ranges not covered by this cache (boundary + internal gaps)."""
        db_start, db_end = await self.get_date_range()
        if db_start is None or db_end is None:
            return [(start_date, end_date)]

        gaps: list[tuple[date, date]] = []

        # Boundary gaps
        if start_date < db_start:
            gaps.append((start_date, db_start - timedelta(days=1)))
        if end_date > db_end:
            gaps.append((db_end + timedelta(days=1), end_date))

        # Internal daily-data gaps (dates with too few stocks = partial download)
        partial_dates = await self._find_partial_daily_dates()
        for d in partial_dates:
            if start_date <= d <= end_date:
                gaps.append((d, d))

        # Internal minute-data gaps
        for gap_start, gap_end in await self.find_minute_gaps():
            clipped_start = max(gap_start, start_date)
            clipped_end = min(gap_end, end_date)
            if clipped_start <= clipped_end:
                gaps.append((clipped_start, clipped_end))

        # Sort and merge
        if len(gaps) <= 1:
            return gaps
        gaps.sort()
        merged: list[tuple[date, date]] = [gaps[0]]
        for s, e in gaps[1:]:
            prev_s, prev_e = merged[-1]
            if s <= prev_e + timedelta(days=1):
                merged[-1] = (prev_s, max(prev_e, e))
            else:
                merged.append((s, e))
        return merged

    # ==================== Integrity Validation ====================

    async def validate_integrity(self) -> list[str]:
        """Run data integrity checks. Returns list of warning messages (empty = OK)."""
        warnings: list[str] = []

        # 1. Total stock count
        total_stocks = await self.get_daily_stock_count()
        if total_stocks < _MIN_EXPECTED_STOCKS:
            warnings.append(f"日线股票数偏少: {total_stocks} (预期 >={_MIN_EXPECTED_STOCKS})")
        elif total_stocks > _MAX_EXPECTED_STOCKS:
            warnings.append(f"日线股票数异常多: {total_stocks} (预期 <={_MAX_EXPECTED_STOCKS})")

        # 2. Per-day stock count consistency
        daily_rows = await self._db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_daily GROUP BY ts ORDER BY cnt"
        )
        if daily_rows:
            counts = [int(r["cnt"]) for r in daily_rows]
            median_count = counts[len(counts) // 2]

            anomaly_days = []
            for r in daily_rows:
                cnt = int(r["cnt"])
                if cnt < _MIN_STOCKS_PER_DAY and median_count > _MIN_STOCKS_PER_DAY:
                    d = _ts_to_date(r["ts"])
                    anomaly_days.append(f"{d}({cnt})")
            if anomaly_days:
                sample = anomaly_days[:5]
                suffix = f" ...+{len(anomaly_days) - 5}天" if len(anomaly_days) > 5 else ""
                warnings.append(
                    f"日线某些天股票数异常少 (中位数{median_count}): {', '.join(sample)}{suffix}"
                )

        # 3. Minute coverage
        minute_stocks = await self.get_minute_stock_count()
        if total_stocks > 0 and minute_stocks < total_stocks * _MIN_MINUTE_COVERAGE:
            pct = minute_stocks / total_stocks * 100
            warnings.append(f"分钟线覆盖率不足: {minute_stocks}/{total_stocks} ({pct:.0f}%)")

        # 4. Stock list vs daily gaps
        daily_gaps = await self._audit_daily_gaps()
        if daily_gaps:
            sample = [f"{d}({act}/{exp})" for d, exp, act in daily_gaps[:5]]
            suffix = f" ...+{len(daily_gaps) - 5}天" if len(daily_gaps) > 5 else ""
            warnings.append(f"日线缺失: {len(daily_gaps)}天 {', '.join(sample)}{suffix}")

        # 5. Daily vs minute gaps (non-suspended)
        minute_gaps = await self._audit_minute_gaps()
        if minute_gaps:
            sample = [f"{d}({act}/{exp})" for d, exp, act in minute_gaps[:5]]
            suffix = f" ...+{len(minute_gaps) - 5}天" if len(minute_gaps) > 5 else ""
            warnings.append(f"分钟线缺失: {len(minute_gaps)}天 {', '.join(sample)}{suffix}")

        return warnings

    async def check_data_integrity(self) -> list[dict[str, Any]]:
        """Run comprehensive data integrity checks on existing cached data.

        Checks for garbage values, NULL fields, OHLC relationship violations,
        and unreasonable values in both daily and minute tables.

        Returns list of issue dicts (empty = all checks passed), each with:
            level:   "error" | "warning"
            check:   short identifier
            message: human-readable description (Chinese)
            count:   number of affected rows
            samples: list of "stock_code @ date" strings (max 5)
        """
        issues: list[dict[str, Any]] = []

        total_stocks = await self.get_daily_stock_count()
        if total_stocks == 0:
            return issues

        # Column sets to SELECT for each table so error logs show actual values
        _detail_cols: dict[str, str] = {
            "backtest_daily": (
                "stock_code, ts, open_price, high_price, low_price, close_price, vol, is_suspended"
            ),
            "backtest_minute": ("stock_code, ts, close_940, cum_volume, max_high, min_low"),
        }

        async def _count_and_sample(
            table: str, where: str, level: str, check: str, message_tpl: str
        ) -> None:
            """Helper: COUNT + fetch ALL bad rows (up to 100) with full field values."""
            row = await self._db.fetchrow(f"SELECT COUNT(*) as cnt FROM {table} WHERE {where}")
            cnt = int(row["cnt"]) if row else 0
            if cnt == 0:
                return
            # Fetch all bad rows with full details (cap at 100)
            details: list[str] = []
            try:
                cols = _detail_cols.get(table, "stock_code, ts")
                detail_rows = await self._db.fetch(
                    f"SELECT {cols} FROM {table} WHERE {where} LIMIT 100"
                )
                for dr in detail_rows:
                    d = _ts_to_date(dr["ts"])
                    # Build "field=value" pairs for all columns except stock_code/ts
                    parts = []
                    for key in dr.keys():
                        if key in ("stock_code", "ts"):
                            continue
                        parts.append(f"{key}={dr[key]}")
                    details.append(f"  {dr['stock_code']}@{d}: {', '.join(parts)}")
            except Exception:
                pass  # detail fetch is best-effort
            msg = message_tpl.format(cnt=cnt)
            if details:
                msg += ":\n" + "\n".join(details)
                if cnt > 100:
                    msg += f"\n  ... 及其余 {cnt - 100} 条"
            issues.append(
                {
                    "level": level,
                    "check": check,
                    "message": msg,
                    "count": cnt,
                }
            )

        # --- Daily table checks ---

        # 1. NULL is_suspended (old data not backfilled)
        await _count_and_sample(
            "backtest_daily",
            "is_suspended IS NULL",
            "error",
            "null_is_suspended",
            "日线: {cnt} 条记录 is_suspended 为 NULL（需要回填）",
        )

        # 2. NULL price fields
        await _count_and_sample(
            "backtest_daily",
            "open_price IS NULL OR high_price IS NULL OR low_price IS NULL OR close_price IS NULL",
            "error",
            "null_prices",
            "日线: {cnt} 条记录价格字段为 NULL",
        )

        # 3. Non-suspended stocks with zero open/close
        await _count_and_sample(
            "backtest_daily",
            "is_suspended = false AND (open_price = 0 OR close_price = 0)",
            "error",
            "zero_price_active",
            "日线: {cnt} 条非停牌记录 open/close 为 0",
        )

        # 4. Negative prices
        await _count_and_sample(
            "backtest_daily",
            "open_price < 0 OR high_price < 0 OR low_price < 0 OR close_price < 0",
            "error",
            "negative_price",
            "日线: {cnt} 条记录价格为负数",
        )

        # 5. high < low (non-suspended)
        await _count_and_sample(
            "backtest_daily",
            "is_suspended = false AND high_price < low_price",
            "error",
            "high_lt_low",
            "日线: {cnt} 条非停牌记录 high < low",
        )

        # 6. open/close outside [low, high] range
        await _count_and_sample(
            "backtest_daily",
            "is_suspended = false AND ("
            "open_price > high_price OR open_price < low_price "
            "OR close_price > high_price OR close_price < low_price)",
            "warning",
            "ohlc_range_violation",
            "日线: {cnt} 条非停牌记录 open/close 超出 [low,high] 范围",
        )

        # 7. Negative volume
        await _count_and_sample(
            "backtest_daily",
            "vol < 0",
            "error",
            "negative_volume",
            "日线: {cnt} 条记录 vol 为负数",
        )

        # 8. Non-suspended with zero volume (warning if > 100, small numbers normal)
        row = await self._db.fetchrow(
            "SELECT COUNT(*) as cnt FROM backtest_daily WHERE is_suspended = false AND vol = 0"
        )
        zero_vol_cnt = int(row["cnt"]) if row else 0
        if zero_vol_cnt > 100:
            issues.append(
                {
                    "level": "warning",
                    "check": "zero_volume_active",
                    "message": f"日线: {zero_vol_cnt} 条非停牌记录 vol=0（涨跌停无成交属正常）",
                    "count": zero_vol_cnt,
                    "samples": [],
                }
            )

        # --- Minute table checks ---

        minute_count = await self.get_minute_stock_count()
        if minute_count > 0:
            # 9. close_940 <= 0
            await _count_and_sample(
                "backtest_minute",
                "close_940 <= 0",
                "error",
                "zero_close_940",
                "分钟线: {cnt} 条记录 close_940 <= 0",
            )

            # 10. max_high < min_low
            await _count_and_sample(
                "backtest_minute",
                "max_high < min_low",
                "error",
                "minute_high_lt_low",
                "分钟线: {cnt} 条记录 max_high < min_low",
            )

            # 11. NULL fields
            await _count_and_sample(
                "backtest_minute",
                "close_940 IS NULL OR cum_volume IS NULL OR max_high IS NULL OR min_low IS NULL",
                "error",
                "null_minute_fields",
                "分钟线: {cnt} 条记录有 NULL 字段",
            )

            # 12. Negative cum_volume
            await _count_and_sample(
                "backtest_minute",
                "cum_volume < 0",
                "error",
                "negative_cum_volume",
                "分钟线: {cnt} 条记录 cum_volume 为负数",
            )

        return issues

    # ==================== Status for UI ====================

    _cache_status_result: dict | None = None
    _cache_status_ts: float = 0.0
    _CACHE_STATUS_TTL = 60.0  # seconds

    async def get_cache_status(self) -> dict:
        """Return cache status dict for the frontend status endpoint.

        Results are cached in-memory for 60s to avoid hammering GreptimeDB.
        """
        import time as _time

        now = _time.monotonic()
        if self._cache_status_result and (now - self._cache_status_ts) < self._CACHE_STATUS_TTL:
            return self._cache_status_result

        result = await self.get_cache_status_streaming()
        self._cache_status_result = result
        self._cache_status_ts = now
        return result

    async def get_cache_status_streaming(
        self,
        on_step: Callable[[str], Any] | None = None,
    ) -> dict:
        """Query cache status with per-step progress callback.

        ``on_step`` is called with a human-readable description before each
        SQL query, e.g. ``on_step("查询日期范围...")``.  The caller can use
        this to stream progress to the frontend.
        """

        async def _step(msg: str) -> None:
            if on_step:
                await _maybe_await(on_step(msg))

        if not self.is_ready:
            return {"status": "disconnected"}

        await _step("查询日期范围...")
        db_start, db_end = await self.get_date_range()
        if db_start is None:
            return {"status": "empty"}

        await _step("统计日线股票数...")
        daily_stocks = await self.get_daily_stock_count()

        await _step("统计日线天数...")
        daily_days = await self.get_daily_date_count()

        await _step("统计分钟线股票数...")
        minute_stocks = await self.get_minute_stock_count()

        return {
            "status": "ready",
            "start_date": str(db_start),
            "end_date": str(db_end),
            "daily_stocks": daily_stocks,
            "daily_days": daily_days,
            "minute_stocks": minute_stocks,
            "minute_gaps": [],
            "has_gaps": False,
        }

    def invalidate_cache_status(self) -> None:
        """Force next get_cache_status() to re-query. Call after downloads."""
        self._cache_status_result = None

    # ==================== Download Methods ====================

    async def download_prices(
        self,
        start_date: date,
        end_date: date,
        progress_cb: Callable[[str, int, int, str], Any] | None = None,
        cancel_event: _CancelChecker | None = None,
    ) -> dict[str, int | bool | str]:
        """Download daily + minute data for all main-board stocks.

        Phase 1 — Daily OHLCV via tsanghi REST API (fast, batch per-date).
        Phase 2 — 1-min bars via Tushare Pro stk_mins API (per-stock, for 9:40 snapshot).

        Data is written to GreptimeDB (flush handled internally by the database).
        """
        # Extra history for lookback (preClose needs 1 day, QualityFilter needs ~30 days)
        dl_start = start_date - timedelta(days=60)

        # Compact existing SST files before writing new data
        for tbl in ("backtest_daily", "backtest_minute"):
            try:
                await self._db.execute(f"ADMIN COMPACT_TABLE('{tbl}')")
            except asyncpg.UndefinedTableError:
                logger.debug(f"COMPACT_TABLE('{tbl}') skipped (table not exist yet)")

        if progress_cb:
            await _maybe_await(progress_cb("init", 0, 0, ""))

        # Phase 1: Daily OHLCV from tsanghi
        stock_codes = await self._download_daily_tsanghi(
            dl_start,
            end_date,
            progress_cb,
            cancel_event,
        )

        # Phase 2: Minute data from Tushare Pro 1min
        # Always use full stock list from DB — daily download only returns codes
        # from newly downloaded dates, which can be a tiny subset when most dates
        # are already cached.
        stock_codes = await self.get_stock_codes()
        if not stock_codes:
            logger.warning("No stock codes in DB, skipping minute download")

        # Collect per-stock reasons for missing minute data across all phases
        all_no_data_reasons: dict[str, str] = {}
        if stock_codes:
            reasons = await self._download_minute(
                stock_codes,
                dl_start,
                end_date,
                progress_cb=progress_cb,
                cancel_event=cancel_event,
            )
            all_no_data_reasons.update(reasons)

        # Final verification: re-run resume check to confirm download won't re-trigger.
        if progress_cb:
            await _maybe_await(progress_cb("download", 0, 1, "最终验证中..."))

        daily_count = await self.get_daily_stock_count()
        minute_count = await self.get_minute_stock_count()
        daily_dates = await self.get_daily_date_count()

        # Re-run the same resume check that decides what to download.
        # If it says 0 stocks to download, verification passes.
        # Must use dl_start (not start_date) to match what _download_minute uses.
        active_codes = await self._get_active_daily_codes(dl_start, end_date)
        existing_minute = await self._get_existing_minute_codes(dl_start, end_date)
        would_download = [c for c in active_codes if c not in existing_minute]

        self.invalidate_cache_status()

        # Classify missing stocks by reason
        from collections import Counter

        missing_with_reason = []
        missing_unknown = []  # data was downloaded but didn't persist
        for code in would_download:
            reason = all_no_data_reasons.get(code)
            if reason:
                missing_with_reason.append((code, reason))
            else:
                missing_unknown.append(code)

        reason_counts = Counter(r.split(":")[0] for _, r in missing_with_reason)

        verified = len(would_download) == 0
        if verified:
            verify_msg = f"验证通过: 日线 {daily_count}只/{daily_dates}天, 分钟线 {minute_count}只"
        else:
            parts = [
                f"日线 {daily_count}只/{daily_dates}天",
                f"分钟线 {minute_count}只",
                f"缺失 {len(would_download)} 只",
            ]
            if reason_counts:
                reason_strs = [f"{k}={v}" for k, v in reason_counts.items()]
                parts.append(f"原因: {', '.join(reason_strs)}")
            if missing_unknown:
                parts.append(f"数据丢失(刷盘失败): {len(missing_unknown)}只")
            verify_msg = " | ".join(parts)

        logger.info(f"Final verify: {verify_msg}")
        if progress_cb:
            await _maybe_await(progress_cb("download", 1, 1, verify_msg))

        # Send detailed missing report to Feishu
        if would_download:
            await self._send_missing_minute_report(
                would_download,
                all_no_data_reasons,
                missing_unknown,
                daily_count,
                minute_count,
                daily_dates,
                dl_start,
                end_date,
            )

        return {
            "daily_count": daily_count,
            "minute_count": minute_count,
            "verified": verified,
            "verify_msg": verify_msg,
        }

        # Data integrity validation — send results via progress_cb so frontend sees them
        integrity_warnings = await self.validate_integrity()
        for w in integrity_warnings:
            logger.warning(f"Data integrity: {w}")
        if progress_cb:
            await _maybe_await(
                progress_cb(
                    "post_integrity",
                    len(integrity_warnings),
                    0,
                    "\n".join(integrity_warnings) if integrity_warnings else "",
                )
            )

    async def _send_missing_minute_report(
        self,
        would_download: list[str],
        no_data_reasons: dict[str, str],
        missing_unknown: list[str],
        daily_count: int,
        minute_count: int,
        daily_dates: int,
        dl_start: date,
        end_date: date,
    ) -> None:
        """Send a Feishu message listing all stocks missing minute data and why."""
        from collections import Counter

        from src.common.feishu_bot import FeishuBot

        reason_counts = Counter(r.split(":")[0] for r in no_data_reasons.values() if r)

        lines = [
            "📊 分钟线缺失报告",
            f"日期范围: {dl_start} ~ {end_date}",
            f"日线: {daily_count}只/{daily_dates}天 | 分钟线: {minute_count}只",
            f"缺失: {len(would_download)}只",
            "",
        ]

        # Group by reason
        if reason_counts:
            lines.append("【缺失原因统计】")
            for reason, count in reason_counts.most_common():
                label = {
                    "api_empty": "API返回空数据",
                    "api_error": "API报错",
                    "no_0931_0940": "有数据但无09:31~09:40",
                    "unknown_exchange": "无法识别交易所",
                    "cancelled": "用户取消",
                }.get(reason, reason)
                lines.append(f"  {label}: {count}只")
            lines.append("")

        if missing_unknown:
            lines.append(f"【数据丢失(下载成功但刷盘后消失)】{len(missing_unknown)}只:")
            lines.append(", ".join(sorted(missing_unknown)[:50]))
            if len(missing_unknown) > 50:
                lines.append(f"  ...及其他 {len(missing_unknown) - 50} 只")
            lines.append("")

        # List stocks by reason category (up to 50 per category)
        by_reason: dict[str, list[str]] = {}
        for code in would_download:
            reason = no_data_reasons.get(code, "unknown")
            key = reason.split(":")[0]
            by_reason.setdefault(key, []).append(code)

        for reason_key, codes in sorted(by_reason.items()):
            if reason_key == "unknown":
                continue  # already shown above as missing_unknown
            label = {
                "api_empty": "API返回空数据",
                "api_error": "API报错",
                "no_0931_0940": "有数据但无09:31~09:40",
                "unknown_exchange": "无法识别交易所",
                "cancelled": "用户取消",
            }.get(reason_key, reason_key)
            lines.append(f"【{label}】{len(codes)}只:")
            lines.append(", ".join(sorted(codes)[:50]))
            if len(codes) > 50:
                lines.append(f"  ...及其他 {len(codes) - 50} 只")

            # For no_0935_0940, show sample detail
            if reason_key == "no_0935_0940":
                samples = [
                    (c, no_data_reasons[c]) for c in sorted(codes)[:3] if c in no_data_reasons
                ]
                for c, detail in samples:
                    lines.append(f"  例: {c} → {detail}")
            lines.append("")

        message = "\n".join(lines)
        logger.info(f"Missing minute report:\n{message}")

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_message(message)
        else:
            logger.warning("Feishu bot not configured, missing minute report not sent")

    async def _download_daily_tsanghi(
        self,
        dl_start: date,
        end_date: date,
        progress_cb: Callable[[str, int, int, str], Any] | None = None,
        cancel_event: _CancelChecker = None,
    ) -> list[str]:
        """Download daily OHLCV from tsanghi and INSERT to GreptimeDB.

        Returns list of all stock codes found.
        """
        from src.common.config import get_tushare_token
        from src.data.clients.tsanghi_client import TsanghiClient
        from src.data.clients.tushare_realtime import TushareRealtimeClient

        client = TsanghiClient()
        await client.start()

        tushare_client = TushareRealtimeClient(token=get_tushare_token())
        await tushare_client.start()

        try:
            # Resume: check which dates already exist in DB.
            # NOTE: Do NOT jump dl_start past max(existing_dates) — that
            # skips over any gaps (missing trading days within the cached
            # range).  The per-date check below (line "if current in
            # existing_dates") already skips cached days efficiently.
            existing_dates = await self._get_existing_daily_dates()

            if existing_dates:
                logger.info(
                    "Daily resume: %d dates cached (%s ~ %s), will skip them individually",
                    len(existing_dates),
                    min(existing_dates),
                    max(existing_dates),
                )

            if progress_cb:
                skipped = len(existing_dates)
                await _maybe_await(progress_cb("daily_resume", skipped, skipped, ""))

            # Backfill: fix existing dates with is_suspended IS NULL
            await self._backfill_is_suspended(tushare_client, progress_cb, cancel_event)

            # Fetch trade calendar to skip weekends/holidays
            from src.data.clients.tushare_realtime import get_tushare_trade_calendar

            trading_dates: set[date] | None = None
            try:
                trade_cal_strs = await get_tushare_trade_calendar(
                    dl_start.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                )
                trading_dates = {_parse_date_str(d) for d in trade_cal_strs}
                logger.info(f"Trade calendar: {len(trading_dates)} trading days in range")
            except Exception as e:
                logger.warning(f"Trade calendar fetch failed: {e}, will check all dates")
                if progress_cb:
                    await _maybe_await(progress_cb("daily", 0, 0, f"⚠ 交易日历获取失败: {e}"))

            # Track preClose across days (for computing pre_close field)
            prev_close_map = await self._get_latest_closes()

            total_days = (end_date - dl_start).days + 1
            trading_days_found = 0
            all_stock_codes: set[str] = set()
            current = dl_start

            while current <= end_date:
                if cancel_event and cancel_event.is_set():
                    logger.info("Daily download cancelled by user")
                    raise asyncio.CancelledError()

                date_str = current.strftime("%Y-%m-%d")
                ts_ms = _date_to_epoch_ms(current)

                # Skip non-trading days (weekends/holidays) via trade calendar
                if trading_dates is not None and current not in trading_dates:
                    current += timedelta(days=1)
                    continue

                # Skip dates already in cache
                if current in existing_dates:
                    if progress_cb:
                        elapsed = (current - dl_start).days + 1
                        await _maybe_await(
                            progress_cb("daily", elapsed, total_days, f"{date_str} 已缓存")
                        )
                    current += timedelta(days=1)
                    continue

                day_records: list[tuple[str, dict]] = []  # (code, record_dict)
                _null_data_codes: list[str] = []  # 接口返回但数据为空的非停牌股

                # Fetch suspended stocks from Tushare suspend_d (authoritative)
                # If this fails, send Feishu alert and re-raise — do NOT write
                # wrong suspension data.
                try:
                    suspended_codes = await tushare_client.fetch_suspended_stocks(
                        current.strftime("%Y%m%d")
                    )
                except Exception as e:
                    logger.critical(
                        f"FATAL: Tushare suspend_d API failed for {date_str}: {e}. "
                        f"Aborting daily download to prevent wrong suspension data.",
                        exc_info=True,
                    )
                    try:
                        from src.common.feishu_bot import FeishuBot

                        bot = FeishuBot()
                        if bot.is_configured():
                            await bot.send_message(
                                f"[缓存下载] 严重错误\n"
                                f"Tushare suspend_d API 失败 ({date_str})\n"
                                f"错误: {str(e)[:200]}\n"
                                f"已中止日线下载，防止写入错误停牌数据"
                            )
                    except Exception:  # safety: ignore — 通知失败不阻断下载
                        logger.warning("Failed to send Feishu alert for suspend_d failure")
                    raise

                seen_codes: set[str] = set()  # track codes from tsanghi
                failed_exchanges: list[str] = []

                for exchange in ("XSHG", "XSHE"):
                    try:
                        records = await client.daily_latest(exchange, date_str)
                    except RuntimeError as e:
                        failed_exchanges.append(exchange)
                        logger.warning(f"tsanghi daily_latest({exchange}, {date_str}) FAILED: {e}")
                        continue

                    if not records:
                        continue

                    for rec in records:
                        ticker = str(rec.get("ticker", ""))
                        if not ticker or len(ticker) != 6:
                            continue

                        seen_codes.add(ticker)
                        o = rec.get("open")
                        c = rec.get("close")
                        pre_close = prev_close_map.get(ticker, 0.0)
                        is_susp = ticker in suspended_codes

                        if is_susp:
                            # Case 1: Tushare suspend_d 确认停牌
                            fill_price = pre_close if pre_close > 0 else 0.0
                            day_records.append(
                                (
                                    ticker,
                                    {
                                        "open": fill_price,
                                        "high": fill_price,
                                        "low": fill_price,
                                        "close": fill_price,
                                        "pre_close": pre_close,
                                        "volume": 0.0,
                                        "amount": 0.0,
                                        "turnover_ratio": None,
                                        "is_suspended": True,
                                    },
                                )
                            )
                        elif o is None or c is None:
                            # Case 2: 接口返回了记录但 open/close 为空，
                            # 且 Tushare 未标记停牌 — 数据异常，跳过不写入
                            _null_data_codes.append(ticker)
                            continue
                        else:
                            day_records.append(
                                (
                                    ticker,
                                    {
                                        "open": float(o),
                                        "high": float(rec.get("high", o)),
                                        "low": float(rec.get("low", o)),
                                        "close": float(c),
                                        "pre_close": pre_close,
                                        "volume": float(rec.get("volume", 0)),
                                        "amount": 0.0,
                                        "turnover_ratio": None,
                                        "is_suspended": False,
                                    },
                                )
                            )
                        all_stock_codes.add(ticker)

                # Report exchange failures
                if failed_exchanges:
                    if progress_cb:
                        elapsed = (current - dl_start).days + 1
                        await _maybe_await(
                            progress_cb(
                                "daily",
                                elapsed,
                                total_days,
                                f"{date_str} ⚠ API失败: {','.join(failed_exchanges)}",
                            )
                        )
                    if len(failed_exchanges) == 2:
                        logger.error(f"Daily {date_str}: BOTH exchanges failed, skipping")
                        current += timedelta(days=1)
                        continue

                # Stocks in Tushare suspend list but NOT in tsanghi response
                # (tsanghi may omit suspended stocks entirely)
                for susp_code in suspended_codes:
                    if susp_code in seen_codes:
                        continue
                    pre_close = prev_close_map.get(susp_code, 0.0)
                    fill_price = pre_close if pre_close > 0 else 0.0
                    if fill_price <= 0:
                        continue  # no price info at all, skip
                    day_records.append(
                        (
                            susp_code,
                            {
                                "open": fill_price,
                                "high": fill_price,
                                "low": fill_price,
                                "close": fill_price,
                                "pre_close": pre_close,
                                "volume": 0.0,
                                "amount": 0.0,
                                "turnover_ratio": None,
                                "is_suspended": True,
                            },
                        )
                    )
                    all_stock_codes.add(susp_code)

                # 停牌通报
                if suspended_codes:
                    logger.info(f"{date_str}: {len(suspended_codes)} stocks suspended")
                    try:
                        from src.common.feishu_bot import FeishuBot

                        bot = FeishuBot()
                        if bot.is_configured():
                            sample = ", ".join(sorted(suspended_codes)[:15])
                            n = len(suspended_codes)
                            tail = f" 等{n}只" if n > 15 else ""
                            await bot.send_message(
                                f"[缓存下载] 停牌记录\n"
                                f"日期: {date_str}\n"
                                f"停牌: {n} 只\n"
                                f"{sample}{tail}"
                            )
                    except Exception:  # safety: ignore — 通知失败不阻断下载
                        logger.warning("Failed to send Feishu suspension alert")

                # Case 2 聚合告警: 接口返回但数据为空的非停牌股
                if _null_data_codes:
                    codes_sample = ", ".join(_null_data_codes[:10])
                    extra = f" 等{len(_null_data_codes)}只" if len(_null_data_codes) > 10 else ""
                    logger.warning(
                        f"tsanghi {date_str}: {len(_null_data_codes)} stocks "
                        f"returned null open/close but NOT in suspend_d list, "
                        f"skipped: {codes_sample}{extra}"
                    )
                    try:
                        from src.common.feishu_bot import FeishuBot

                        bot = FeishuBot()
                        if bot.is_configured():
                            await bot.send_message(
                                f"[缓存下载] 数据异常\n"
                                f"日期: {date_str}\n"
                                f"tsanghi 返回 {len(_null_data_codes)} 只股票 "
                                f"open/close 为空，但 Tushare 未标记停牌\n"
                                f"已跳过: {codes_sample}{extra}"
                            )
                    except Exception:  # safety: ignore — 通知失败不阻断下载
                        logger.warning("Failed to send Feishu null-data alert")

                if day_records:
                    trading_days_found += 1
                    await self._write_daily(ts_ms, day_records)

                    if progress_cb:
                        elapsed = (current - dl_start).days + 1
                        stocks_today = len(day_records)
                        null_part = (
                            f", {len(_null_data_codes)}只数据为空" if _null_data_codes else ""
                        )
                        status = f"{date_str} ({stocks_today}只{null_part}) ✓"
                        await _maybe_await(progress_cb("daily", elapsed, total_days, status))

                    # Update prev_close_map for next day
                    for code, rec_data in day_records:
                        prev_close_map[code] = rec_data["close"]
                else:
                    # API returned no usable records for this trading day
                    if progress_cb:
                        elapsed = (current - dl_start).days + 1
                        null_part = (
                            f", {len(_null_data_codes)}只数据为空" if _null_data_codes else ""
                        )
                        status = f"{date_str} ⚠ API返回0条记录{null_part}"
                        await _maybe_await(progress_cb("daily", elapsed, total_days, status))
                    logger.warning(f"Daily {date_str}: 0 usable records from tsanghi API")

                current += timedelta(days=1)

            logger.info(
                f"tsanghi daily download: {len(all_stock_codes)} stocks, "
                f"{trading_days_found} trading days in [{dl_start} ~ {end_date}]"
            )

            # Sync stock_list from bak_basic (authoritative stock list per date)
            if trading_dates:
                await self._sync_stock_list(
                    tushare_client,
                    sorted(trading_dates),
                    progress_cb,
                    cancel_event,
                )

            # Audit & backfill: detect dates where daily data is incomplete
            await self._backfill_daily_gaps(client, tushare_client, progress_cb, cancel_event)

            return sorted(all_stock_codes)
        finally:
            await client.stop()
            await tushare_client.stop()

    async def _download_minute(
        self,
        codes: list[str],
        dl_start: date,
        end_date: date,
        progress_cb: Callable[[str, int, int, str], Any] | None = None,
        cancel_event: _CancelChecker = None,
    ) -> dict[str, str]:
        """Download 1-min bars from Tushare Pro and INSERT 9:40 snapshots to GreptimeDB.

        Uses stk_mins API (api.tushare.pro) with 1min granularity.
        Aggregates 09:31~09:40 bars into a single 9:40 snapshot per day.

        Returns dict of {stock_code: reason} for stocks with no minute data.
        """
        from src.common.config import get_tushare_token
        from src.data.clients.tushare_realtime import TushareRealtimeClient

        # Resume: check which codes already have minute data.
        # Use dl_start (not start_date) — must match the download range,
        # otherwise stocks with minute data only in the lookback period
        # would be re-downloaded every time.
        existing_codes = await self._get_existing_minute_codes(dl_start, end_date)
        # Filter out stocks that are fully suspended (no active trading day)
        # — they have no minute bars and would be re-downloaded forever.
        active_codes = await self._get_active_daily_codes(dl_start, end_date)

        total_before = len(codes)
        codes = [c for c in codes if c in active_codes]
        suspended_count = total_before - len(codes)
        codes_to_download = [c for c in codes if c not in existing_codes]
        if suspended_count > 0:
            logger.info(f"Minute: skipped {suspended_count} fully-suspended stocks")
        logger.info(
            f"Minute resume: {len(existing_codes)} cached / {len(codes)} active, "
            f"downloading {len(codes_to_download)}"
        )

        if progress_cb:
            await _maybe_await(progress_cb("minute_resume", len(existing_codes), len(codes), ""))

        if not codes_to_download:
            return {}

        # Pre-load suspended (stock_code, date_str) pairs so we can
        # skip suspended dates after aggregation instead of relying
        # on zero-value heuristics.
        start_ms = _date_to_epoch_ms(dl_start)
        end_ms = _date_to_epoch_ms(end_date)
        susp_rows = await self._db.fetch(
            f"SELECT stock_code, ts FROM backtest_daily "
            f"WHERE ts >= {start_ms} AND ts <= {end_ms} "
            f"AND is_suspended = true"
        )
        suspended_pairs: set[tuple[str, str]] = set()
        for sr in susp_rows:
            d = _ts_to_date(sr["ts"])
            suspended_pairs.add((sr["stock_code"], d))
        if suspended_pairs:
            logger.info(f"Minute: loaded {len(suspended_pairs)} suspended (stock,date) pairs")

        client = TushareRealtimeClient(token=get_tushare_token())
        await client.start()

        try:
            total = len(codes_to_download)
            done = 0
            start_str = dl_start.strftime("%Y-%m-%d") + " 09:00:00"
            end_str = end_date.strftime("%Y-%m-%d") + " 15:00:00"

            # Track why each stock has no minute data:
            # key=stock_code, value=reason string
            no_data_reasons: dict[str, str] = {}

            def _aggregate_bars(
                bars: list[dict],
            ) -> dict[str, tuple[float, float, float, float]]:
                """Aggregate 09:31~09:40 bars into per-day (close, vol, high, low)."""
                day_data: dict[str, tuple[float, float, float, float]] = {}
                for bar in bars:
                    trade_time = str(bar.get("trade_time", ""))
                    if len(trade_time) < 16:
                        continue
                    bar_date = trade_time[:10]
                    bar_time = trade_time[11:16]
                    if bar_time < "09:31" or bar_time > "09:40":
                        continue
                    try:
                        h = float(bar["high"])
                        lo = float(bar["low"])
                        c = float(bar["close"])
                        v = float(bar.get("vol", 0))
                    except (ValueError, TypeError, KeyError):
                        continue
                    if bar_date in day_data:
                        prev_c, prev_v, prev_h, prev_l = day_data[bar_date]
                        day_data[bar_date] = (c, prev_v + v, max(prev_h, h), min(prev_l, lo))
                    else:
                        day_data[bar_date] = (c, v, h, lo)
                return day_data

            # Batch API calls: comma-separated ts_codes, 20 stocks per request
            # to reduce 3000+ individual calls to ~160.
            # Rate limit: 200 req/min sustained, so 0.5s delay between requests.
            api_batch_size = 20
            for i in range(0, total, api_batch_size):
                if cancel_event and cancel_event.is_set():
                    logger.info("Minute download cancelled by user")
                    raise asyncio.CancelledError()

                batch_codes = codes_to_download[i : i + api_batch_size]

                if progress_cb:
                    await _maybe_await(progress_cb("minute_active", done, total, batch_codes[0]))

                # Convert bare codes to ts_codes, skip invalid ones
                ts_to_bare: dict[str, str] = {}
                for code in batch_codes:
                    try:
                        ts_to_bare[TushareRealtimeClient._to_ts_code(code)] = code
                    except ValueError:
                        no_data_reasons[code] = "unknown_exchange"
                        done += 1

                if not ts_to_bare:
                    continue

                # Single API call for multiple stocks
                ts_codes_str = ",".join(ts_to_bare.keys())
                try:
                    bars = await client.stk_mins(
                        ts_codes_str,
                        freq="1min",
                        start_date=start_str,
                        end_date=end_str,
                        limit=5000000,
                    )
                except Exception as e:  # TushareRealtimeError or network
                    for code in ts_to_bare.values():
                        no_data_reasons[code] = f"api_error: {e}"
                        done += 1
                    if progress_cb:
                        await _maybe_await(
                            progress_cb(
                                "minute",
                                done,
                                total,
                                f"API错误{len(ts_to_bare)}只: {e}",
                            )
                        )
                    await asyncio.sleep(0.5)
                    continue

                # Group response bars by ts_code
                grouped: dict[str, list[dict]] = {ts: [] for ts in ts_to_bare}
                for bar in bars:
                    ts = str(bar.get("ts_code", ""))
                    if ts in grouped:
                        grouped[ts].append(bar)

                # Aggregate and write each stock
                batch_ok = 0
                batch_empty = 0
                for ts_code, code in ts_to_bare.items():
                    code_bars = grouped.get(ts_code, [])
                    if not code_bars:
                        no_data_reasons[code] = "api_empty"
                        batch_empty += 1
                        done += 1
                        continue
                    day_data = _aggregate_bars(code_bars)
                    if not day_data:
                        no_data_reasons[code] = f"no_0931_0940: {len(code_bars)}bars"
                        batch_empty += 1
                        done += 1
                        continue
                    # Remove suspended dates using is_suspended from daily table
                    susp_dates = [d for d in day_data if (code, d) in suspended_pairs]
                    for d in susp_dates:
                        del day_data[d]
                    if not day_data:
                        no_data_reasons[code] = "all_dates_suspended"
                        batch_empty += 1
                        done += 1
                        continue
                    await self._write_minute(code, day_data)
                    batch_ok += 1
                    done += 1

                if progress_cb:
                    parts = []
                    if batch_ok:
                        parts.append(f"写入{batch_ok}只")
                    if batch_empty:
                        parts.append(f"无数据{batch_empty}只")
                    detail = ", ".join(parts) if parts else ""
                    await _maybe_await(progress_cb("minute", done, total, detail))

                # Rate limit: stay under 200 req/min
                await asyncio.sleep(0.5)

                if done % 200 == 0:
                    logger.info(f"minute download: {done}/{total} stocks processed")

            # Log summary of no-data reasons
            if no_data_reasons:
                from collections import Counter

                reason_counts = Counter(r.split(":")[0] for r in no_data_reasons.values())
                logger.info(
                    f"minute download: {len(no_data_reasons)} stocks have no data — "
                    f"{dict(reason_counts)}"
                )

            logger.info(f"minute download done: {done}/{total} stocks")
        finally:
            await client.stop()

        return no_data_reasons

    # ==================== Internal Write Helpers ====================

    async def _write_daily(self, ts_ms: int, records: list[tuple[str, dict]]) -> None:
        """INSERT daily records for a single date, one row at a time.

        GreptimeDB silently drops data when batch INSERT exceeds ~200 rows.
        """
        if not records:
            return
        cols = (
            "(stock_code,ts,open_price,high_price,low_price,close_price,"
            "pre_close,vol,amount,turnover_ratio,is_suspended)"
        )
        for code, rec in records:
            tr = rec["turnover_ratio"]
            tr_str = str(tr) if tr is not None else "NULL"
            suspended = "true" if rec.get("is_suspended") else "false"
            val = (
                f"('{code}',{ts_ms},"
                f"{rec['open']},{rec['high']},{rec['low']},{rec['close']},"
                f"{rec['pre_close']},{rec['volume']},{rec['amount']},{tr_str},{suspended})"
            )
            await self._db.execute(f"INSERT INTO backtest_daily{cols} VALUES {val}")

    async def _write_minute(
        self, code: str, min_data: dict[str, tuple[float, float, float, float]]
    ) -> None:
        """INSERT minute data for a single stock."""
        if not min_data:
            return
        # Validate: garbage data must halt the download, not be silently skipped
        garbage = [
            (ds, close_940, cum_vol, max_high, min_low)
            for ds, (close_940, cum_vol, max_high, min_low) in min_data.items()
            if close_940 <= 0 or max_high <= 0 or min_low <= 0
        ]
        if garbage:
            details = "; ".join(
                f"{code}@{ds} close_940={c940} high={h} low={lo}" for ds, c940, _, h, lo in garbage
            )
            raise RuntimeError(f"分钟线垃圾数据，已停止下载。请人工确认后处理:\n{details}")

        values = []
        for ds, (close_940, cum_vol, max_high, min_low) in min_data.items():
            ts_ms = _date_to_epoch_ms(_parse_date_str(ds))
            values.append(f"('{code}',{ts_ms},{close_940},{cum_vol},{max_high},{min_low})")
        cols = "(stock_code,ts,close_940,cum_volume,max_high,min_low)"
        batch_size = 200
        for bi in range(0, len(values), batch_size):
            batch = values[bi : bi + batch_size]
            sql = f"INSERT INTO backtest_minute{cols} VALUES " + ",".join(batch)
            await self._db.execute(sql)

    # ==================== Backfill ====================

    async def _backfill_is_suspended(
        self,
        tushare_client: Any,
        progress_cb: Callable[[str, int, int, str], Any] | None = None,
        cancel_event: _CancelChecker = None,
    ) -> None:
        """Fix existing daily records where is_suspended IS NULL.

        INSERT upsert works for single rows but silently fails for bulk
        INSERT with 3000+ rows. Solution: INSERT in batches of 200.
        """
        # Find dates that need backfill
        rows = await self._db.fetch(
            "SELECT DISTINCT ts FROM backtest_daily WHERE is_suspended IS NULL"
        )
        if not rows:
            return

        dates_to_fix = sorted(_ts_to_date(r["ts"]) for r in rows)
        if not dates_to_fix:
            return
        logger.info(
            f"Backfill is_suspended: {len(dates_to_fix)} dates need fixing "
            f"({dates_to_fix[0]} ~ {dates_to_fix[-1]})"
        )

        if progress_cb:
            await _maybe_await(progress_cb("backfill", 0, len(dates_to_fix), "回填停牌标记"))

        # Build prev_close map from the day before the earliest date to fix
        prev_close_map: dict[str, float] = {}
        earliest = dates_to_fix[0]
        earliest_ms = _date_to_epoch_ms(earliest)
        prev_rows = await self._db.fetch(
            f"SELECT stock_code, close_price FROM backtest_daily "
            f"WHERE ts < {earliest_ms} ORDER BY ts DESC LIMIT 10000"
        )
        for r in prev_rows:
            code = r["stock_code"]
            if code not in prev_close_map:
                prev_close_map[code] = float(r["close_price"])

        for idx, day in enumerate(dates_to_fix):
            if cancel_event and cancel_event.is_set():
                logger.info("Backfill cancelled by user")
                raise asyncio.CancelledError()

            date_str = day.strftime("%Y-%m-%d")
            ts_ms = _date_to_epoch_ms(day)

            if progress_cb:
                await _maybe_await(
                    progress_cb(
                        "backfill",
                        idx,
                        len(dates_to_fix),
                        f"{date_str} 查询停牌...",
                    )
                )

            # Fetch suspended codes (fail-fast)
            try:
                suspended_codes = await tushare_client.fetch_suspended_stocks(
                    day.strftime("%Y%m%d")
                )
            except Exception as e:
                logger.critical(
                    f"FATAL: Tushare suspend_d failed during backfill for {date_str}: {e}",
                    exc_info=True,
                )
                try:
                    from src.common.feishu_bot import FeishuBot

                    bot = FeishuBot()
                    if bot.is_configured():
                        await bot.send_message(
                            f"[缓存回填] 严重错误\n"
                            f"Tushare suspend_d API 失败 ({date_str})\n"
                            f"错误: {str(e)[:200]}\n"
                            f"已中止回填"
                        )
                except Exception:  # safety: ignore — 通知失败不阻断下载
                    pass
                raise

            # Read records with NULL is_suspended for this date
            db_rows = await self._db.fetch(
                f"SELECT stock_code, open_price, high_price, low_price, "
                f"close_price, pre_close, vol, amount, turnover_ratio "
                f"FROM backtest_daily WHERE ts = {ts_ms} AND is_suspended IS NULL"
            )

            db_codes: dict[str, dict] = {}
            for r in db_rows:
                code = r["stock_code"]
                db_codes[code] = {
                    "open": float(r["open_price"]),
                    "high": float(r["high_price"]),
                    "low": float(r["low_price"]),
                    "close": float(r["close_price"]),
                    "pre_close": float(r["pre_close"]) if r["pre_close"] else 0.0,
                    "vol": float(r["vol"]) if r["vol"] else 0.0,
                    "amount": float(r["amount"]) if r["amount"] else 0.0,
                    "tr": r["turnover_ratio"],
                }

            # DELETE old rows then INSERT with new columns
            cols = (
                "(stock_code,ts,open_price,high_price,low_price,close_price,"
                "pre_close,vol,amount,turnover_ratio,is_suspended)"
            )

            # Step 1: DELETE all rows for this date
            for code in db_codes:
                await self._db.execute(
                    f"DELETE FROM backtest_daily WHERE stock_code = '{code}' AND ts = {ts_ms}"
                )

            # Step 2: INSERT fresh rows with is_suspended set
            upserted = 0
            for code, rec in db_codes.items():
                if code in suspended_codes:
                    # Case A: in DB + suspended → fix OHLC
                    pre_close = rec["pre_close"]
                    fill = pre_close if pre_close > 0 else 0.0
                    val = (
                        f"('{code}',{ts_ms},"
                        f"{fill},{fill},{fill},{fill},"
                        f"{pre_close},0.0,0.0,NULL,true)"
                    )
                else:
                    # Case B: in DB + normal → keep data, set is_suspended=false
                    tr_str = str(rec["tr"]) if rec["tr"] is not None else "NULL"
                    val = (
                        f"('{code}',{ts_ms},"
                        f"{rec['open']},{rec['high']},{rec['low']},{rec['close']},"
                        f"{rec['pre_close']},{rec['vol']},{rec['amount']},"
                        f"{tr_str},false)"
                    )
                    prev_close_map[code] = rec["close"]

                await self._db.execute(f"INSERT INTO backtest_daily{cols} VALUES {val}")
                upserted += 1

            # Case C: suspended but not in DB → insert with pre_close fill
            for susp_code in suspended_codes:
                if susp_code in db_codes:
                    continue
                pre_close = prev_close_map.get(susp_code, 0.0)
                if pre_close <= 0:
                    continue
                val = (
                    f"('{susp_code}',{ts_ms},"
                    f"{pre_close},{pre_close},{pre_close},{pre_close},"
                    f"{pre_close},0.0,0.0,NULL,true)"
                )
                await self._db.execute(f"INSERT INTO backtest_daily{cols} VALUES {val}")
                upserted += 1

            logger.info(f"Backfill {date_str}: {upserted} rows (suspended={len(suspended_codes)})")
            if progress_cb:
                await _maybe_await(
                    progress_cb(
                        "backfill",
                        idx + 1,
                        len(dates_to_fix),
                        f"{date_str} ✓ ({upserted}行, 停牌{len(suspended_codes)}只)",
                    )
                )

        logger.info(f"Backfill is_suspended complete: {len(dates_to_fix)} dates fixed")

        # Final verification: count remaining NULL rows
        null_count_row = await self._db.fetchrow(
            "SELECT COUNT(*) as cnt FROM backtest_daily WHERE is_suspended IS NULL"
        )
        null_remaining = int(null_count_row["cnt"]) if null_count_row else -1

        notifier = None
        try:
            from src.common.feishu_bot import FeishuBot

            _bot = FeishuBot()
            if _bot.is_configured():
                notifier = _bot
        except Exception:
            pass

        if null_remaining > 0:
            remaining = await self._db.fetch(
                "SELECT DISTINCT ts FROM backtest_daily WHERE is_suspended IS NULL"
            )
            remaining_dates = sorted(_ts_to_date(r["ts"]) for r in remaining)
            msg = (
                f"[缓存回填] ❌ 验证失败\n"
                f"回填 {len(dates_to_fix)} 天后仍有 {null_remaining} 行 "
                f"is_suspended=NULL ({len(remaining_dates)} 天)\n"
                f"日期: {remaining_dates[:5]}"
            )
            logger.error(msg)
            if notifier:
                try:
                    await notifier.send_message(msg)
                except Exception:
                    pass
        else:
            msg = f"[缓存回填] ✅ 回填完成\n修复 {len(dates_to_fix)} 天, is_suspended NULL 剩余: 0"
            logger.info(msg)
            if notifier:
                try:
                    await notifier.send_message(msg)
                except Exception:
                    pass

    # ==================== Stock List Sync & Audit ====================

    async def _get_existing_stock_list_dates(self) -> set[date]:
        """Get dates that already have stock_list data in DB."""
        rows = await self._db.fetch("SELECT DISTINCT ts FROM stock_list")
        return {_ts_to_date(r["ts"]) for r in rows}

    async def _sync_stock_list(
        self,
        tushare_client: Any,
        trading_dates: list[date],
        progress_cb: Callable[[str, int, int, str], Any] | None = None,
        cancel_event: _CancelChecker = None,
    ) -> None:
        """Sync stock_list table from Tushare bak_basic for each trading date.

        Skips dates already in the table (idempotent).
        Each row = (stock_code, ts) meaning this stock was listed on this date.
        """
        existing = await self._get_existing_stock_list_dates()
        to_sync = [d for d in trading_dates if d not in existing]

        if not to_sync:
            logger.info("stock_list: all %d dates already synced", len(trading_dates))
            return

        logger.info(
            "stock_list: syncing %d dates (%d already cached)",
            len(to_sync),
            len(existing),
        )

        # bak_basic API limit: 2 calls/min. Sleep 31s between calls.
        for i, td in enumerate(to_sync):
            if cancel_event and cancel_event.is_set():
                logger.info("stock_list sync cancelled")
                return

            date_str = td.strftime("%Y%m%d")
            ts_ms = _date_to_epoch_ms(td)

            # Rate-limit aware fetch with retry on 40203
            codes: list[str] = []
            for attempt in range(3):
                try:
                    codes = await tushare_client.fetch_bak_basic(date_str)
                    break
                except Exception as e:
                    if "40203" in str(e):
                        logger.warning(
                            "stock_list: rate limited on %s, waiting 60s (attempt %d/3)",
                            date_str,
                            attempt + 1,
                        )
                        await asyncio.sleep(60)
                    else:
                        raise
            if not codes:
                logger.warning("stock_list: bak_basic returned 0 codes for %s", date_str)
                continue

            # Single-row INSERT (GreptimeDB upserts on (stock_code, ts))
            for code in codes:
                await self._db.execute(
                    f"INSERT INTO stock_list(stock_code,ts) VALUES ('{code}',{ts_ms})"
                )

            if progress_cb:
                await _maybe_await(
                    progress_cb("stock_list", i + 1, len(to_sync), f"{td} ({len(codes)}只)")
                )

            logger.info("stock_list: %s → %d codes", td, len(codes))

            # Respect bak_basic rate limit: 2 calls/min
            if i < len(to_sync) - 1:
                await asyncio.sleep(31)

        logger.info("stock_list: sync complete, %d dates added", len(to_sync))

    async def _audit_daily_gaps(self) -> list[tuple[date, int, int]]:
        """Compare stock_list vs backtest_daily per date. Return dates with gaps.

        Returns:
            [(date, expected_count, actual_count), ...] where actual < expected.
        """
        # stock_list counts per date
        sl_rows = await self._db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM stock_list GROUP BY ts ORDER BY ts"
        )
        if not sl_rows:
            return []

        expected: dict[date, int] = {_ts_to_date(r["ts"]): int(r["cnt"]) for r in sl_rows}

        # backtest_daily counts per date
        daily_rows = await self._db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_daily GROUP BY ts ORDER BY ts"
        )
        actual: dict[date, int] = {_ts_to_date(r["ts"]): int(r["cnt"]) for r in daily_rows}

        gaps = []
        for d, exp in expected.items():
            act = actual.get(d, 0)
            if act < exp:
                gaps.append((d, exp, act))

        if gaps:
            logger.info(
                "audit_daily_gaps: %d/%d dates have gaps (e.g. %s: %d/%d)",
                len(gaps),
                len(expected),
                gaps[0][0],
                gaps[0][2],
                gaps[0][1],
            )
        else:
            logger.info("audit_daily_gaps: all %d dates complete", len(expected))

        return gaps

    async def _audit_minute_gaps(self) -> list[tuple[date, int, int]]:
        """Compare non-suspended daily vs backtest_minute per date.

        Expected minute count = non-suspended stocks with volume in backtest_daily.
        Actual minute count = rows in backtest_minute.

        Returns:
            [(date, expected_count, actual_count), ...] where actual < expected.
        """
        # Non-suspended daily counts per date
        daily_rows = await self._db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_daily "
            "WHERE (is_suspended = false OR is_suspended IS NULL) AND vol > 0 "
            "GROUP BY ts ORDER BY ts"
        )
        if not daily_rows:
            return []

        expected: dict[date, int] = {_ts_to_date(r["ts"]): int(r["cnt"]) for r in daily_rows}

        # backtest_minute counts per date
        minute_rows = await self._db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_minute GROUP BY ts ORDER BY ts"
        )
        actual: dict[date, int] = {_ts_to_date(r["ts"]): int(r["cnt"]) for r in minute_rows}

        gaps = []
        for d, exp in expected.items():
            act = actual.get(d, 0)
            if act < exp:
                gaps.append((d, exp, act))

        if gaps:
            logger.info(
                "audit_minute_gaps: %d/%d dates have gaps (e.g. %s: %d/%d)",
                len(gaps),
                len(expected),
                gaps[0][0],
                gaps[0][2],
                gaps[0][1],
            )
        else:
            logger.info("audit_minute_gaps: all %d dates complete", len(expected))

        return gaps

    async def _backfill_daily_gaps(
        self,
        tsanghi_client: Any,
        tushare_client: Any,
        progress_cb: Callable[[str, int, int, str], Any] | None = None,
        cancel_event: _CancelChecker = None,
    ) -> int:
        """Backfill dates where backtest_daily has fewer stocks than stock_list.

        For each gap date:
        1. Query existing stock_codes in backtest_daily for that date
        2. Re-download from tsanghi daily_latest (XSHG + XSHE)
        3. INSERT only stocks not already present (upsert is safe)

        Returns:
            Number of dates backfilled.
        """
        gaps = await self._audit_daily_gaps()
        if not gaps:
            return 0

        logger.info("backfill_daily_gaps: %d dates to backfill", len(gaps))
        backfilled = 0

        # Need prev_close for suspended stock fill
        prev_close_map = await self._get_latest_closes()

        for i, (gap_date, expected, actual) in enumerate(gaps):
            if cancel_event and cancel_event.is_set():
                logger.info("daily backfill cancelled")
                break

            date_str = gap_date.strftime("%Y-%m-%d")
            ts_ms = _date_to_epoch_ms(gap_date)

            # Get existing codes for this date
            existing_rows = await self._db.fetch(
                f"SELECT stock_code FROM backtest_daily WHERE ts = {ts_ms}"
            )
            existing_codes = {r["stock_code"] for r in existing_rows}

            # Re-fetch from tsanghi
            new_records: list[tuple[str, dict]] = []

            # Fetch suspended stocks for this date
            suspended_codes: set[str] = set()
            try:
                suspended_codes = await tushare_client.fetch_suspended_stocks(
                    gap_date.strftime("%Y%m%d")
                )
            except Exception as e:
                logger.warning("backfill: suspend_d failed for %s: %s, skipping date", date_str, e)
                continue

            for exchange in ("XSHG", "XSHE"):
                try:
                    records = await tsanghi_client.daily_latest(exchange, date_str)
                except RuntimeError as e:
                    logger.warning("backfill: tsanghi %s %s failed: %s", exchange, date_str, e)
                    continue

                if not records:
                    continue

                for rec in records:
                    ticker = str(rec.get("ticker", ""))
                    if not ticker or len(ticker) != 6:
                        continue
                    if ticker in existing_codes:
                        continue  # already in DB

                    o = rec.get("open")
                    c = rec.get("close")
                    is_susp = ticker in suspended_codes

                    if is_susp:
                        fill_price = prev_close_map.get(ticker, 0.0)
                        new_records.append(
                            (
                                ticker,
                                {
                                    "open": fill_price,
                                    "high": fill_price,
                                    "low": fill_price,
                                    "close": fill_price,
                                    "pre_close": prev_close_map.get(ticker, 0.0),
                                    "volume": 0.0,
                                    "amount": 0.0,
                                    "turnover_ratio": None,
                                    "is_suspended": True,
                                },
                            )
                        )
                    elif o is None or c is None:
                        continue
                    else:
                        new_records.append(
                            (
                                ticker,
                                {
                                    "open": float(o),
                                    "high": float(rec.get("high", o)),
                                    "low": float(rec.get("low", o)),
                                    "close": float(c),
                                    "pre_close": prev_close_map.get(ticker, 0.0),
                                    "volume": float(rec.get("volume", 0)),
                                    "amount": 0.0,
                                    "turnover_ratio": None,
                                    "is_suspended": False,
                                },
                            )
                        )

            if new_records:
                await self._write_daily(ts_ms, new_records)
                logger.info(
                    "backfill: %s added %d stocks (was %d, expected %d)",
                    date_str,
                    len(new_records),
                    actual,
                    expected,
                )

            # Update prev_close_map with newly written closes
            for code, rec in new_records:
                if rec.get("close") and rec["close"] > 0:
                    prev_close_map[code] = rec["close"]

            backfilled += 1
            if progress_cb:
                await _maybe_await(
                    progress_cb(
                        "daily_backfill",
                        i + 1,
                        len(gaps),
                        f"{date_str} +{len(new_records)}只"
                        f" ({actual}→{actual + len(new_records)}/{expected})",
                    )
                )

        logger.info("backfill_daily_gaps: done, %d dates backfilled", backfilled)
        return backfilled

    # ==================== Resume Helpers ====================

    async def _find_partial_daily_dates(self) -> list[date]:
        """Find dates with > 0 but < _MIN_STOCKS_PER_DAY daily rows.

        These are partial downloads that should be re-downloaded.
        """
        rows = await self._db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_daily "
            f"GROUP BY ts HAVING COUNT(*) < {_MIN_STOCKS_PER_DAY}"
        )
        return [_ts_to_date(r["ts"]) for r in rows if int(r["cnt"]) > 0]

    async def _get_existing_daily_dates(self) -> set[date]:
        """Get dates that have *sufficient* daily data in DB.

        A date is considered cached only if it has >= _MIN_STOCKS_PER_DAY
        stocks.  Dates with fewer rows (partial downloads) are NOT
        returned so that the resume logic will re-download them.
        """
        rows = await self._db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_daily GROUP BY ts ORDER BY ts"
        )
        if not rows:
            return set()
        result: set[date] = set()
        for r in rows:
            cnt = int(r["cnt"])
            if cnt >= _MIN_STOCKS_PER_DAY:
                result.add(_ts_to_date(r["ts"]))
            else:
                d = _ts_to_date(r["ts"])
                logger.warning(
                    "Partial daily data for %s: %d stocks (need >=%d), will re-download",
                    d,
                    cnt,
                    _MIN_STOCKS_PER_DAY,
                )
        return result

    async def _get_existing_minute_codes(self, start_date: date, end_date: date) -> set[str]:
        """Get stock codes that have any minute data in the given range.

        A stock is considered cached if it has at least 1 row in backtest_minute.
        This avoids infinite re-download loops for stocks where the API has no
        minute data (returns empty) — they'd never reach coverage thresholds.
        """
        start_ms = _date_to_epoch_ms(start_date)
        end_ms = _date_to_epoch_ms(end_date)

        rows = await self._db.fetch(
            f"SELECT DISTINCT stock_code FROM backtest_minute "
            f"WHERE ts >= {start_ms} AND ts <= {end_ms}"
        )
        return {r["stock_code"] for r in rows}

    async def _get_active_daily_codes(self, start_date: date, end_date: date) -> set[str]:
        """Get stock codes that have at least one non-suspended trading day.

        Stocks fully suspended/delisted in the range have no minute bars and
        should be excluded from minute download.
        """
        start_ms = _date_to_epoch_ms(start_date)
        end_ms = _date_to_epoch_ms(end_date)
        rows = await self._db.fetch(
            f"SELECT DISTINCT stock_code FROM backtest_daily "
            f"WHERE ts >= {start_ms} AND ts <= {end_ms} "
            f"AND (is_suspended = false OR is_suspended IS NULL) AND vol > 0"
        )
        return {r["stock_code"] for r in rows}

    async def _get_latest_closes(self) -> dict[str, float]:
        """Get the latest close price per stock from existing daily data.

        Used to seed pre_close computation during incremental downloads.
        """
        # Get the most recent date in DB
        row = await self._db.fetchrow("SELECT MAX(ts) as max_ts FROM backtest_daily")
        if not row or row["max_ts"] is None:
            return {}
        max_ts = _ts_to_epoch_ms(row["max_ts"])
        # Get closes on that date
        rows = await self._db.fetch(
            f"SELECT stock_code, close_price FROM backtest_daily WHERE ts = {max_ts}"
        )
        return {r["stock_code"]: float(r["close_price"]) for r in rows}


# ---------------------------------------------------------------------------
# Historical adapter for MomentumScanner (backtest mode)
# ---------------------------------------------------------------------------


class GreptimeHistoricalAdapter:
    """Implements HistoricalDataProvider for backtest use.

    Reads from GreptimeDB via GreptimeBacktestCache and returns data in the
    expected history_quotes / high_frequency response format.
    """

    def __init__(self, cache: GreptimeBacktestCache) -> None:
        self._cache = cache

    @property
    def is_connected(self) -> bool:
        return self._cache.is_ready

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def history_quotes(
        self,
        codes: str,
        indicators: str,
        start_date: str,
        end_date: str,
        function_para: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Return cached daily data in history_quotes format."""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        indicator_list = [ind.strip() for ind in indicators.split(",")]
        tables: list[dict[str, Any]] = []

        start_ms = _date_to_epoch_ms(_parse_date_str(start_date))
        end_ms = _date_to_epoch_ms(_parse_date_str(end_date))

        for full_code in code_list:
            bare = full_code.split(".")[0]
            rows = await self._cache._db.fetch(
                f"SELECT ts, open_price, high_price, low_price, close_price, "
                f"pre_close, vol, amount, turnover_ratio, is_suspended "
                f"FROM backtest_daily "
                f"WHERE stock_code = '{bare}' AND ts >= {start_ms} AND ts <= {end_ms} "
                f"ORDER BY ts"
            )

            if not rows:
                continue

            time_vals: list[str] = []
            indicator_data: dict[str, list] = {ind: [] for ind in indicator_list}

            # Map indicator names to DB column names
            ind_to_col = {
                "open": "open_price",
                "high": "high_price",
                "low": "low_price",
                "close": "close_price",
                "preClose": "pre_close",
                "volume": "vol",
                "amount": "amount",
                "turnoverRatio": "turnover_ratio",
                "is_suspended": "is_suspended",
            }

            for r in rows:
                ts_date = _ts_to_date(r["ts"])
                time_vals.append(ts_date.strftime("%Y-%m-%d"))
                for ind in indicator_list:
                    col = ind_to_col.get(ind, ind)
                    val = r[col] if col in r.keys() else None
                    # tsanghi volume is in 手; convert to 股 at read time
                    if ind == "volume" and val is not None:
                        val = val * 100
                    indicator_data[ind].append(val)

            tables.append({"thscode": full_code, "table": {"time": time_vals, **indicator_data}})

        return {"errorcode": 0, "tables": tables}

    async def high_frequency(
        self,
        codes: str,
        indicators: str,
        start_time: str,
        end_time: str,
        function_para: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Return cached minute data in high_frequency format."""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        tables: list[dict[str, Any]] = []
        date_str = start_time.split(" ")[0]

        for full_code in code_list:
            bare = full_code.split(".")[0]
            data_940 = await self._cache.get_940_price(bare, date_str)
            if data_940:
                close_val, cum_vol, max_high, min_low = data_940
                table: dict[str, list] = {}
                for ind in indicators.split(","):
                    ind = ind.strip()
                    if ind == "close":
                        table["close"] = [close_val]
                    elif ind == "volume":
                        table["volume"] = [cum_vol]
                    elif ind == "high":
                        table["high"] = [max_high]
                    elif ind == "low":
                        table["low"] = [min_low]
                tables.append({"thscode": full_code, "table": table})

        return {"errorcode": 0, "tables": tables}

    async def smart_stock_picking(
        self,
        search_string: str,
        search_type: str = "stock",
    ) -> dict[str, Any]:
        """Return empty result — triggers fallback in scanner Step 6."""
        return {"errorcode": 0, "tables": []}

    async def real_time_quotation(
        self,
        codes: str,
        indicators: str,
    ) -> dict[str, Any]:
        """Not supported in backtest mode."""
        return {"errorcode": 0, "tables": []}

    async def get_trade_dates(
        self,
        market_code: str,
        start_date: str,
        end_date: str,
    ) -> list[str]:
        """Get trading dates via Tushare trade_cal.

        Falls back to inferring from existing daily data in GreptimeDB.
        """
        from src.data.clients.tushare_realtime import get_tushare_trade_calendar

        try:
            return await get_tushare_trade_calendar(start_date, end_date)
        except Exception as e:  # safety: ignore — 交易日历有 DB 兜底
            logger.warning(f"Tushare trade_cal failed: {e}, falling back to DB inference")

        # Fallback: infer from existing daily data
        rows = await self._cache._db.fetch("SELECT DISTINCT ts FROM backtest_daily ORDER BY ts")
        all_dates = [_ts_to_date(r["ts"]).strftime("%Y-%m-%d") for r in rows]
        sd = start_date if isinstance(start_date, str) else start_date.strftime("%Y-%m-%d")
        ed = end_date if isinstance(end_date, str) else end_date.strftime("%Y-%m-%d")
        return [d for d in all_dates if sd <= d <= ed]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _group_contiguous_dates(dates: list[date]) -> list[tuple[date, date]]:
    """Group sorted dates into contiguous ranges (allowing 3-day gaps for weekends)."""
    if not dates:
        return []
    sorted_dates = sorted(dates)
    ranges: list[tuple[date, date]] = []
    range_start = sorted_dates[0]
    prev = sorted_dates[0]
    for d in sorted_dates[1:]:
        if (d - prev).days <= 3:
            prev = d
        else:
            ranges.append((range_start, prev))
            range_start = d
            prev = d
    ranges.append((range_start, prev))
    return ranges


async def _maybe_await(result: Any) -> None:
    """Await result if it's a coroutine, otherwise do nothing."""
    if asyncio.iscoroutine(result):
        await result


def create_backtest_cache_from_config() -> GreptimeBacktestCache:
    """Create GreptimeBacktestCache from env var settings."""
    import os

    host = os.environ.get("GREPTIME_HOST", "localhost")
    port = int(os.environ.get("GREPTIME_PORT", "4003"))
    return GreptimeBacktestCache(host=host, port=port)
