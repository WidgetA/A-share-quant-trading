# === MODULE PURPOSE ===
# Pure storage layer for backtest data on top of GreptimeDB.
#
# This file contains ZERO business logic:
#   - No data source / API knowledge (download → src/data/sources/*)
#   - No aggregation rules (windows / business calendars → src/strategy/*)
#   - No notification / progress UI (→ src/data/services/cache_progress_reporter)
#   - No download orchestration (→ src/data/services/cache_pipeline)
#
# Provides:
#   - GreptimeClient — async asyncpg pool to GreptimeDB (PG wire protocol, port 4003)
#   - GreptimeBacktestStorage — schema, CRUD, range/gap detection, integrity audits
#   - DailyBar / Snapshot DTOs
#
# === TABLES ===
# backtest_daily:  stock_code(TAG), ts(TIME INDEX), open_price, high_price,
#                  low_price, close_price, pre_close, vol, amount, turnover_ratio,
#                  is_suspended
# backtest_minute: stock_code(TAG), ts(TIME INDEX), close_940, cum_volume,
#                  max_high, min_low
#                  NOTE: column names are historical — semantically these are
#                  "early-window aggregation" fields whose meaning is defined by
#                  the business-layer aggregator (src/strategy/aggregators/).
#                  The storage layer treats them as opaque float columns.
# stock_list:      stock_code(TAG), ts(TIME INDEX) — per-date authoritative
#                  listed stock universe (from upstream metadata source).
#
# === CONVENTIONS ===
# - All reads/writes via SQL through asyncpg
# - Natural upsert: same (stock_code, ts) = last write wins
# - One INSERT per row — GreptimeDB silently drops batch INSERTs >200 rows
# - Volume stored in 手 (lots), upper layers convert ×100 at read time

from __future__ import annotations

import asyncio
import calendar
import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, NamedTuple

import asyncpg

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Integrity audit thresholds (used by validate_integrity / check_data_integrity)
# These describe storage health, not business rules.
# ---------------------------------------------------------------------------

_MIN_EXPECTED_STOCKS = 2500
_MAX_EXPECTED_STOCKS = 5500
_MIN_MINUTE_COVERAGE = 0.5

# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------


class DailyBar(NamedTuple):
    """Daily OHLCV record returned by storage reads."""

    open: float
    high: float
    low: float
    close: float
    preClose: float
    volume: float  # in 手 (lots); adapter converts ×100 at read time
    amount: float
    turnoverRatio: float | None
    is_suspended: bool = False


class MinuteSnapshot(NamedTuple):
    """Minute-window aggregated snapshot returned by storage reads."""

    close: float
    cum_volume: float
    max_high: float
    min_low: float


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def date_to_epoch_ms(d: date) -> int:
    """Convert date to epoch milliseconds (midnight UTC)."""
    return calendar.timegm(d.timetuple()) * 1000


def epoch_ms_to_date(ms: int | float) -> date:
    """Convert epoch milliseconds to date."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()


def parse_date_str(date_str: str) -> date:
    """Parse YYYY-MM-DD string to date."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def ts_to_date(val: Any) -> date:
    """Convert asyncpg timestamp result to date."""
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, (int, float)):
        return epoch_ms_to_date(val)
    if isinstance(val, str):
        dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        return dt.date()
    raise TypeError(f"Cannot convert {type(val)} to date: {val}")


def ts_to_epoch_ms(val: Any) -> int:
    """Convert asyncpg timestamp result to epoch ms."""
    if isinstance(val, datetime):
        return int(val.replace(tzinfo=timezone.utc).timestamp() * 1000)
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    raise TypeError(f"Cannot convert {type(val)} to epoch ms: {val}")


def group_contiguous_dates(dates: list[date]) -> list[tuple[date, date]]:
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


# ---------------------------------------------------------------------------
# Schema DDL
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


# ---------------------------------------------------------------------------
# Low-level GreptimeDB connection pool
# ---------------------------------------------------------------------------


class _GreptimeConnection(asyncpg.Connection):
    """asyncpg connection subclass tailored for GreptimeDB.

    GreptimeDB's PostgreSQL wire protocol compatibility is incomplete in
    two places that the pool exercises on every release/recycle:

    1. `reset()` issues `RESET ALL` / `DEALLOCATE ALL`, which GreptimeDB
       rejects. Override to a no-op.
    2. `close()` sends a Terminate message and then `await`s the server
       to close the socket. GreptimeDB never closes the socket, so
       `close()` hangs forever. This is what asyncpg calls from
       `PoolConnectionHolder.release()` once `max_queries` (default
       50000) is exceeded — and since we do ~5000 INSERTs per date for
       stock_list, the pool trips that threshold after ~10 dates and
       silently wedges the whole download. Override `close()` to force
       a `terminate()` (TCP reset, no handshake) instead.
    """

    async def reset(self, *, timeout: float | None = None) -> None:  # type: ignore[override]
        pass

    async def close(self, *, timeout: float | None = None) -> None:  # type: ignore[override]
        # Do NOT call super().close() — it sends a PG Terminate message
        # and waits for the server socket to close, which hangs forever
        # against GreptimeDB. terminate() just does a TCP reset.
        if not self.is_closed():
            self.terminate()


class GreptimeClient:
    """Low-level async client for GreptimeDB via PostgreSQL wire protocol (port 4003).

    Settings:
    - statement_cache_size=0 (GreptimeDB does not support DEALLOCATE/PREPARE)
    - Connection pool for safe concurrent access (download + status polling)
    - Custom connection class with no-op reset
    - All queries have a 120s timeout to prevent indefinite hangs
    """

    _QUERY_TIMEOUT: float = 120.0
    _ACQUIRE_TIMEOUT: float = 30.0
    # Slow query watchdog: any SQL still running after this many seconds
    # gets a logger.warning printed (and again every interval). Used to
    # diagnose hangs where asyncpg's C extension swallows CancelledError.
    _SLOW_QUERY_WARN_SEC: float = 30.0

    def __init__(self, host: str, port: int, database: str = "public") -> None:
        self._host = host
        self._port = port
        self._database = database
        self._pool: asyncpg.Pool | None = None

    async def start(self) -> None:
        # Note: asyncpg's `max_queries` (default 50000) triggers connection
        # recycling via `close()`, which used to hang forever on GreptimeDB
        # because the server ignores the PG Terminate handshake. That is
        # fixed at the root in `_GreptimeConnection.close()` — we no longer
        # need to override `max_queries` here.
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

    async def _run(
        self, op_name: str, sql: str, runner: Callable[[asyncpg.Connection], Any]
    ) -> Any:
        """Run a DB op with explicit acquire/query timeout classification.

        On timeout, raises RuntimeError carrying which phase (pool acquire vs
        query execution) hung, the elapsed time, the configured limit, and the
        SQL prefix. This lets the caller see exactly what timed out instead of
        the bare `TimeoutError` produced by `asyncio.wait_for`.
        """
        if not self._pool:
            raise RuntimeError("GreptimeClient not started")

        sql_preview = sql.strip().replace("\n", " ")[:120]

        t_acquire = time.monotonic()
        try:
            conn = await asyncio.wait_for(self._pool.acquire(), timeout=self._ACQUIRE_TIMEOUT)
        except (asyncio.TimeoutError, TimeoutError) as e:
            elapsed = time.monotonic() - t_acquire
            raise RuntimeError(
                f"GreptimeDB pool.acquire timeout: "
                f"elapsed={elapsed:.2f}s limit={self._ACQUIRE_TIMEOUT}s "
                f"pool_size={self._pool.get_size()} pool_idle={self._pool.get_idle_size()} "
                f"op={op_name} sql={sql_preview!r}"
            ) from e

        acquire_elapsed = time.monotonic() - t_acquire
        t_query = time.monotonic()

        async def _slow_query_watchdog() -> None:
            """Periodically warn about a still-running SQL.

            Runs as a sibling task. Cancelled when the query finishes (or
            times out via wait_for). asyncpg's C extension can hang on
            socket recv and ignore CancelledError, so this is the only
            reliable way to surface "stuck on SQL X" to the operator.
            """
            try:
                while True:
                    await asyncio.sleep(self._SLOW_QUERY_WARN_SEC)
                    elapsed = time.monotonic() - t_query
                    logger.warning(
                        "SQL still running after %.0fs: op=%s sql=%r",
                        elapsed,
                        op_name,
                        sql_preview,
                    )
            except asyncio.CancelledError:
                return

        watchdog = asyncio.create_task(_slow_query_watchdog())
        try:
            try:
                return await asyncio.wait_for(runner(conn), timeout=self._QUERY_TIMEOUT)
            except (asyncio.TimeoutError, TimeoutError) as e:
                query_elapsed = time.monotonic() - t_query
                raise RuntimeError(
                    f"GreptimeDB query timeout: "
                    f"query_elapsed={query_elapsed:.2f}s limit={self._QUERY_TIMEOUT}s "
                    f"acquire_elapsed={acquire_elapsed:.2f}s "
                    f"op={op_name} sql={sql_preview!r}"
                ) from e
        finally:
            watchdog.cancel()
            try:
                await watchdog
            except (asyncio.CancelledError, Exception):
                pass
            # Belt-and-suspenders: `pool.release()` previously hung
            # indefinitely on GreptimeDB because asyncpg's default
            # `close()` (called during connection recycling) waits for
            # a Terminate handshake the server never completes. That is
            # now fixed in `_GreptimeConnection.close()`, but we still
            # wrap release() in a short timeout so any future regression
            # surfaces as a loud error instead of silent wedge.
            try:
                await asyncio.wait_for(self._pool.release(conn), timeout=10.0)
            except (asyncio.TimeoutError, TimeoutError):
                logger.error(
                    "GreptimeDB pool.release() timed out after 10s "
                    "(op=%s sql=%r) — terminating connection",
                    op_name,
                    sql_preview,
                )
                try:
                    conn.terminate()
                except Exception:
                    pass

    async def execute(self, sql: str) -> str:
        return await self._run("execute", sql, lambda c: c.execute(sql))

    async def fetch(self, sql: str) -> list[asyncpg.Record]:
        return await self._run("fetch", sql, lambda c: c.fetch(sql))

    async def fetchrow(self, sql: str) -> asyncpg.Record | None:
        return await self._run("fetchrow", sql, lambda c: c.fetchrow(sql))


# ---------------------------------------------------------------------------
# Storage layer
# ---------------------------------------------------------------------------


class GreptimeBacktestStorage:
    """Pure storage for backtest data on GreptimeDB.

    Responsibilities:
      - Schema management (DDL)
      - CRUD primitives for backtest_daily / backtest_minute / stock_list
      - Range / gap detection
      - Resume helpers (return what's already in DB)
      - Read-only integrity audits

    Non-responsibilities:
      - Calling upstream APIs
      - Aggregating raw data
      - Sending notifications
      - Knowing about business windows or trading calendars
    """

    def __init__(self, host: str = "localhost", port: int = 4003, database: str = "public") -> None:
        self.db = GreptimeClient(host, port, database)

    @property
    def is_ready(self) -> bool:
        return self.db.is_connected

    async def start(self) -> None:
        """Connect to GreptimeDB and ensure tables exist."""
        await self.db.start()
        await self.db.execute(_CREATE_DAILY_SQL)
        await self.db.execute(_CREATE_MINUTE_SQL)
        await self.db.execute(_CREATE_STOCK_LIST_SQL)
        # Add is_suspended column if missing (CREATE IF NOT EXISTS won't alter)
        try:
            await self.db.execute("ALTER TABLE backtest_daily ADD COLUMN is_suspended BOOLEAN")
            logger.info("Added is_suspended column to backtest_daily")
        except Exception:  # safety: column already exists, can be ignored
            pass
        logger.info(f"GreptimeBacktestStorage connected via pgwire {self.db._host}:{self.db._port}")

    async def stop(self) -> None:
        await self.db.stop()

    async def compact_tables(self) -> None:
        """Trigger COMPACT_TABLE on the backtest tables.

        Should be called before bulk writes to merge existing SST files.
        """
        for tbl in ("backtest_daily", "backtest_minute"):
            try:
                await self.db.execute(f"ADMIN COMPACT_TABLE('{tbl}')")
            except asyncpg.UndefinedTableError:
                logger.debug(f"COMPACT_TABLE('{tbl}') skipped (table not exist yet)")

    # ==================== Daily reads ====================

    async def get_daily(self, code: str, date_str: str) -> DailyBar | None:
        """Get daily OHLCV for a stock on a date. date_str is YYYY-MM-DD."""
        ms = date_to_epoch_ms(parse_date_str(date_str))
        row = await self.db.fetchrow(
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

    async def get_all_codes_with_daily(self, date_str: str) -> dict[str, DailyBar]:
        """Get daily data for ALL stocks on a specific date."""
        ms = date_to_epoch_ms(parse_date_str(date_str))
        rows = await self.db.fetch(
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

        Returns:
            {code: [(date, open, high, low, close, volume_in_shares), ...]}
            Volume is converted from 手 (lots) to 股 (shares) at read time (×100).
        """
        start_ms = date_to_epoch_ms(parse_date_str(start_date))
        end_ms = date_to_epoch_ms(parse_date_str(end_date))
        rows = await self.db.fetch(
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
                ts_to_date(r["ts"]),
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

    async def get_daily_for_code(
        self, code: str, start_date: str, end_date: str
    ) -> list[asyncpg.Record]:
        """Fetch all daily rows for a single stock in a date range.

        Returns raw asyncpg rows; caller chooses how to project the columns.
        Used by GreptimeHistoricalAdapter.history_quotes which needs flexible
        indicator selection.
        """
        start_ms = date_to_epoch_ms(parse_date_str(start_date))
        end_ms = date_to_epoch_ms(parse_date_str(end_date))
        return await self.db.fetch(
            f"SELECT ts, open_price, high_price, low_price, close_price, "
            f"pre_close, vol, amount, turnover_ratio, is_suspended "
            f"FROM backtest_daily "
            f"WHERE stock_code = '{code}' AND ts >= {start_ms} AND ts <= {end_ms} "
            f"ORDER BY ts"
        )

    async def list_distinct_daily_dates(self) -> list[date]:
        """Return all distinct dates that have daily data, sorted ascending."""
        rows = await self.db.fetch("SELECT DISTINCT ts FROM backtest_daily ORDER BY ts")
        return [ts_to_date(r["ts"]) for r in rows]

    async def get_stock_codes(self) -> list[str]:
        """Get all unique stock codes in daily table."""
        rows = await self.db.fetch(
            "SELECT DISTINCT stock_code FROM backtest_daily ORDER BY stock_code"
        )
        return [r["stock_code"] for r in rows]

    async def get_date_range(self) -> tuple[date | None, date | None]:
        """Get (min_date, max_date) of daily data."""
        row = await self.db.fetchrow(
            "SELECT MIN(ts) as min_ts, MAX(ts) as max_ts FROM backtest_daily"
        )
        if not row or row["min_ts"] is None:
            return (None, None)
        return (ts_to_date(row["min_ts"]), ts_to_date(row["max_ts"]))

    async def get_daily_stock_count(self) -> int:
        """Count distinct stock codes in daily table."""
        row = await self.db.fetchrow("SELECT COUNT(DISTINCT stock_code) as cnt FROM backtest_daily")
        return int(row["cnt"]) if row else 0

    async def get_minute_stock_count(self) -> int:
        """Count distinct stock codes in minute table."""
        row = await self.db.fetchrow(
            "SELECT COUNT(DISTINCT stock_code) as cnt FROM backtest_minute"
        )
        return int(row["cnt"]) if row else 0

    async def get_daily_date_count(self) -> int:
        """Count distinct trading dates in daily table."""
        row = await self.db.fetchrow("SELECT COUNT(DISTINCT ts) as cnt FROM backtest_daily")
        return int(row["cnt"]) if row else 0

    # ==================== Minute reads ====================

    async def get_minute_snapshot(self, code: str, date_str: str) -> MinuteSnapshot | None:
        """Get the minute aggregation snapshot for a stock on a date."""
        ms = date_to_epoch_ms(parse_date_str(date_str))
        row = await self.db.fetchrow(
            f"SELECT close_940, cum_volume, max_high, min_low "
            f"FROM backtest_minute "
            f"WHERE stock_code = '{code}' AND ts = {ms}"
        )
        if not row:
            return None
        return MinuteSnapshot(
            close=float(row["close_940"]),
            cum_volume=float(row["cum_volume"]),
            max_high=float(row["max_high"]),
            min_low=float(row["min_low"]),
        )

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
        """
        daily_rows = await self.db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_daily GROUP BY ts"
        )
        minute_rows = await self.db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_minute GROUP BY ts"
        )

        daily_counts: dict[int, int] = {}
        for r in daily_rows:
            daily_counts[ts_to_epoch_ms(r["ts"])] = int(r["cnt"])

        minute_counts: dict[int, int] = {}
        for r in minute_rows:
            minute_counts[ts_to_epoch_ms(r["ts"])] = int(r["cnt"])

        gap_dates: list[date] = []
        for ts_ms, daily_count in daily_counts.items():
            if daily_count <= 100:
                continue
            minute_count = minute_counts.get(ts_ms, 0)
            if minute_count < daily_count * _MIN_MINUTE_COVERAGE:
                gap_dates.append(epoch_ms_to_date(ts_ms))

        if not gap_dates:
            return []
        gap_dates.sort()
        return group_contiguous_dates(gap_dates)

    async def missing_ranges(self, start_date: date, end_date: date) -> list[tuple[date, date]]:
        """Return date ranges not covered by this storage (boundary + internal gaps)."""
        db_start, db_end = await self.get_date_range()
        if db_start is None or db_end is None:
            return [(start_date, end_date)]

        gaps: list[tuple[date, date]] = []

        # Boundary gaps
        if start_date < db_start:
            gaps.append((start_date, db_start - timedelta(days=1)))
        if end_date > db_end:
            gaps.append((db_end + timedelta(days=1), end_date))

        # Internal minute-data gaps
        for gap_start, gap_end in await self.find_minute_gaps():
            clipped_start = max(gap_start, start_date)
            clipped_end = min(gap_end, end_date)
            if clipped_start <= clipped_end:
                gaps.append((clipped_start, clipped_end))

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

    # ==================== Resume Helpers (public) ====================

    async def get_existing_daily_dates(self) -> set[date]:
        """Return all dates that have at least one row in backtest_daily.

        Used by the download pipeline to skip dates already cached. Completeness
        of each date is judged externally by comparing against the stock_list
        baseline (see audit_daily_gaps); this method does not apply any
        per-day-count threshold.
        """
        rows = await self.db.fetch("SELECT DISTINCT ts FROM backtest_daily")
        return {ts_to_date(r["ts"]) for r in rows}

    async def get_existing_minute_codes(self, start_date: date, end_date: date) -> set[str]:
        """Get stock codes that have any minute data in the given range."""
        start_ms = date_to_epoch_ms(start_date)
        end_ms = date_to_epoch_ms(end_date)
        rows = await self.db.fetch(
            f"SELECT DISTINCT stock_code FROM backtest_minute "
            f"WHERE ts >= {start_ms} AND ts <= {end_ms}"
        )
        return {r["stock_code"] for r in rows}

    async def get_active_daily_codes(self, start_date: date, end_date: date) -> set[str]:
        """Get stock codes that have at least one non-suspended trading day."""
        start_ms = date_to_epoch_ms(start_date)
        end_ms = date_to_epoch_ms(end_date)
        rows = await self.db.fetch(
            f"SELECT DISTINCT stock_code FROM backtest_daily "
            f"WHERE ts >= {start_ms} AND ts <= {end_ms} "
            f"AND (is_suspended = false OR is_suspended IS NULL) AND vol > 0"
        )
        return {r["stock_code"] for r in rows}

    async def get_latest_closes(self) -> dict[str, float]:
        """Get the latest close price per stock from existing daily data.

        Used by the pipeline to seed pre_close computation during incremental
        downloads.
        """
        row = await self.db.fetchrow("SELECT MAX(ts) as max_ts FROM backtest_daily")
        if not row or row["max_ts"] is None:
            return {}
        max_ts = ts_to_epoch_ms(row["max_ts"])
        rows = await self.db.fetch(
            f"SELECT stock_code, close_price FROM backtest_daily WHERE ts = {max_ts}"
        )
        return {r["stock_code"]: float(r["close_price"]) for r in rows}

    async def get_existing_stock_list_dates(self) -> set[date]:
        """Get dates that already have stock_list data in DB."""
        rows = await self.db.fetch("SELECT DISTINCT ts FROM stock_list")
        return {ts_to_date(r["ts"]) for r in rows}

    async def get_codes_for_daily_date(self, day: date) -> set[str]:
        """Return all stock codes present in backtest_daily for a given date."""
        ts_ms = date_to_epoch_ms(day)
        rows = await self.db.fetch(f"SELECT stock_code FROM backtest_daily WHERE ts = {ts_ms}")
        return {r["stock_code"] for r in rows}

    async def get_suspended_pairs(self, start_date: date, end_date: date) -> set[tuple[str, date]]:
        """Return (stock_code, date) pairs marked is_suspended=true in range."""
        start_ms = date_to_epoch_ms(start_date)
        end_ms = date_to_epoch_ms(end_date)
        rows = await self.db.fetch(
            f"SELECT stock_code, ts FROM backtest_daily "
            f"WHERE ts >= {start_ms} AND ts <= {end_ms} "
            f"AND is_suspended = true"
        )
        return {(r["stock_code"], ts_to_date(r["ts"])) for r in rows}

    async def get_null_is_suspended_dates(self) -> list[date]:
        """Return dates that have at least one row with is_suspended IS NULL."""
        rows = await self.db.fetch(
            "SELECT DISTINCT ts FROM backtest_daily WHERE is_suspended IS NULL"
        )
        return sorted(ts_to_date(r["ts"]) for r in rows)

    async def get_null_is_suspended_count(self) -> int:
        """Count rows with is_suspended IS NULL."""
        row = await self.db.fetchrow(
            "SELECT COUNT(*) as cnt FROM backtest_daily WHERE is_suspended IS NULL"
        )
        return int(row["cnt"]) if row else 0

    async def get_daily_rows_for_date(self, day: date) -> list[dict]:
        """Return all rows in backtest_daily for the given date as plain dicts.

        Used by repair routines that need to read existing data, mutate it, and
        re-insert (DELETE → INSERT) without leaking column types to callers.
        """
        ts_ms = date_to_epoch_ms(day)
        rows = await self.db.fetch(
            f"SELECT stock_code, open_price, high_price, low_price, close_price, "
            f"pre_close, vol, amount, turnover_ratio, is_suspended "
            f"FROM backtest_daily WHERE ts = {ts_ms}"
        )
        return [
            {
                "stock_code": r["stock_code"],
                "open": float(r["open_price"]),
                "high": float(r["high_price"]),
                "low": float(r["low_price"]),
                "close": float(r["close_price"]),
                "pre_close": float(r["pre_close"]) if r["pre_close"] else 0.0,
                "vol": float(r["vol"]) if r["vol"] else 0.0,
                "amount": float(r["amount"]) if r["amount"] else 0.0,
                "turnover_ratio": r["turnover_ratio"],
                "is_suspended": (
                    bool(r["is_suspended"]) if r["is_suspended"] is not None else None
                ),
            }
            for r in rows
        ]

    async def get_previous_closes_before(self, day: date, limit: int = 10000) -> dict[str, float]:
        """Return the most recent close per stock strictly before ``day``.

        Used to seed prev_close maps when repairing historical data.
        """
        ts_ms = date_to_epoch_ms(day)
        rows = await self.db.fetch(
            f"SELECT stock_code, close_price FROM backtest_daily "
            f"WHERE ts < {ts_ms} ORDER BY ts DESC LIMIT {limit}"
        )
        result: dict[str, float] = {}
        for r in rows:
            code = r["stock_code"]
            if code not in result:
                result[code] = float(r["close_price"])
        return result

    # ==================== Write Primitives ====================

    async def insert_daily_record(self, code: str, day: date, record: dict) -> None:
        """INSERT one daily row.

        ``record`` keys: open, high, low, close, pre_close, volume, amount,
        turnover_ratio (may be None), is_suspended.
        """
        ts_ms = date_to_epoch_ms(day)
        tr = record["turnover_ratio"]
        tr_str = str(tr) if tr is not None else "NULL"
        suspended = "true" if record.get("is_suspended") else "false"
        cols = (
            "(stock_code,ts,open_price,high_price,low_price,close_price,"
            "pre_close,vol,amount,turnover_ratio,is_suspended)"
        )
        val = (
            f"('{code}',{ts_ms},"
            f"{record['open']},{record['high']},{record['low']},{record['close']},"
            f"{record['pre_close']},{record['volume']},{record['amount']},{tr_str},{suspended})"
        )
        await self.db.execute(f"INSERT INTO backtest_daily{cols} VALUES {val}")

    async def delete_daily_row(self, code: str, day: date) -> None:
        """DELETE a single (stock_code, date) row from backtest_daily."""
        ts_ms = date_to_epoch_ms(day)
        await self.db.execute(
            f"DELETE FROM backtest_daily WHERE stock_code = '{code}' AND ts = {ts_ms}"
        )

    async def insert_minute_snapshot(self, code: str, day: date, snapshot: MinuteSnapshot) -> None:
        """INSERT one minute aggregation snapshot."""
        if snapshot.close <= 0 or snapshot.max_high <= 0 or snapshot.min_low <= 0:
            raise RuntimeError(
                f"分钟线垃圾数据，已停止下载。请人工确认后处理: {code}@{day} "
                f"close={snapshot.close} high={snapshot.max_high} low={snapshot.min_low}"
            )
        ts_ms = date_to_epoch_ms(day)
        cols = "(stock_code,ts,close_940,cum_volume,max_high,min_low)"
        val = (
            f"('{code}',{ts_ms},"
            f"{snapshot.close},{snapshot.cum_volume},{snapshot.max_high},{snapshot.min_low})"
        )
        await self.db.execute(f"INSERT INTO backtest_minute{cols} VALUES {val}")

    async def insert_stock_list_codes(
        self,
        day: date,
        codes: list[str],
        on_progress: Callable[[int, int], Awaitable[None]] | None = None,
    ) -> None:
        """INSERT stock_list rows for a date (one INSERT per code, sequential).

        Was briefly parallelized across 3 connections (ff5c102) but that
        introduced an unexplained hang where INSERTs stopped without any
        CPU/memory pressure on GreptimeDB. Reverted to single-connection
        sequential until the root cause is understood.

        ``on_progress(done, total)`` is awaited every 200 rows so the caller
        can surface intra-date progress to the UI (otherwise 5k+ INSERTs look
        like a hang).
        """
        ts_ms = date_to_epoch_ms(day)
        total = len(codes)
        for i, code in enumerate(codes, start=1):
            await self.db.execute(
                f"INSERT INTO stock_list(stock_code,ts) VALUES ('{code}',{ts_ms})"
            )
            if on_progress is not None and (i % 200 == 0 or i == total):
                await on_progress(i, total)

    # ==================== Audits ====================

    async def audit_daily_gaps(self) -> list[tuple[date, int, int]]:
        """Compare stock_list vs backtest_daily per date.

        Returns: [(date, expected_count, actual_count), ...] where actual < expected.
        """
        sl_rows = await self.db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM stock_list GROUP BY ts ORDER BY ts"
        )
        if not sl_rows:
            return []
        expected = {ts_to_date(r["ts"]): int(r["cnt"]) for r in sl_rows}
        daily_rows = await self.db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_daily GROUP BY ts ORDER BY ts"
        )
        actual = {ts_to_date(r["ts"]): int(r["cnt"]) for r in daily_rows}
        gaps: list[tuple[date, int, int]] = []
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

    async def audit_minute_gaps(self) -> list[tuple[date, int, int]]:
        """Compare non-suspended daily vs backtest_minute per date.

        Returns: [(date, expected_count, actual_count), ...] where actual < expected.
        """
        daily_rows = await self.db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_daily "
            "WHERE (is_suspended = false OR is_suspended IS NULL) AND vol > 0 "
            "GROUP BY ts ORDER BY ts"
        )
        if not daily_rows:
            return []
        expected = {ts_to_date(r["ts"]): int(r["cnt"]) for r in daily_rows}
        minute_rows = await self.db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_minute GROUP BY ts ORDER BY ts"
        )
        actual = {ts_to_date(r["ts"]): int(r["cnt"]) for r in minute_rows}
        gaps: list[tuple[date, int, int]] = []
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

    async def validate_integrity(self) -> list[str]:
        """Run high-level integrity checks. Returns list of warning messages."""
        warnings: list[str] = []

        total_stocks = await self.get_daily_stock_count()
        if total_stocks < _MIN_EXPECTED_STOCKS:
            warnings.append(f"日线股票数偏少: {total_stocks} (预期 >={_MIN_EXPECTED_STOCKS})")
        elif total_stocks > _MAX_EXPECTED_STOCKS:
            warnings.append(f"日线股票数异常多: {total_stocks} (预期 <={_MAX_EXPECTED_STOCKS})")

        # Per-day stock count consistency: anomaly = day count well below the median
        daily_rows = await self.db.fetch(
            "SELECT ts, COUNT(*) as cnt FROM backtest_daily GROUP BY ts ORDER BY cnt"
        )
        if daily_rows:
            counts = [int(r["cnt"]) for r in daily_rows]
            median_count = counts[len(counts) // 2]
            threshold = max(1, int(median_count * 0.5))
            anomaly_days: list[str] = []
            for r in daily_rows:
                cnt = int(r["cnt"])
                if cnt < threshold:
                    d = ts_to_date(r["ts"])
                    anomaly_days.append(f"{d}({cnt})")
            if anomaly_days:
                sample = anomaly_days[:5]
                suffix = f" ...+{len(anomaly_days) - 5}天" if len(anomaly_days) > 5 else ""
                warnings.append(
                    f"日线某些天股票数异常少 (中位数{median_count}): {', '.join(sample)}{suffix}"
                )

        minute_stocks = await self.get_minute_stock_count()
        if total_stocks > 0 and minute_stocks < total_stocks * _MIN_MINUTE_COVERAGE:
            pct = minute_stocks / total_stocks * 100
            warnings.append(f"分钟线覆盖率不足: {minute_stocks}/{total_stocks} ({pct:.0f}%)")

        daily_gaps = await self.audit_daily_gaps()
        if daily_gaps:
            sample = [f"{d}({act}/{exp})" for d, exp, act in daily_gaps[:5]]
            suffix = f" ...+{len(daily_gaps) - 5}天" if len(daily_gaps) > 5 else ""
            warnings.append(f"日线缺失: {len(daily_gaps)}天 {', '.join(sample)}{suffix}")

        minute_gaps = await self.audit_minute_gaps()
        if minute_gaps:
            sample = [f"{d}({act}/{exp})" for d, exp, act in minute_gaps[:5]]
            suffix = f" ...+{len(minute_gaps) - 5}天" if len(minute_gaps) > 5 else ""
            warnings.append(f"分钟线缺失: {len(minute_gaps)}天 {', '.join(sample)}{suffix}")

        return warnings

    async def check_data_integrity(self) -> list[dict[str, Any]]:
        """Run row-level integrity checks (NULL fields, OHLC violations, etc).

        Returns list of issue dicts (empty = all checks passed).
        """
        issues: list[dict[str, Any]] = []

        total_stocks = await self.get_daily_stock_count()
        if total_stocks == 0:
            return issues

        _detail_cols: dict[str, str] = {
            "backtest_daily": (
                "stock_code, ts, open_price, high_price, low_price, close_price, vol, is_suspended"
            ),
            "backtest_minute": "stock_code, ts, close_940, cum_volume, max_high, min_low",
        }

        async def _count_and_sample(
            table: str, where: str, level: str, check: str, message_tpl: str
        ) -> None:
            row = await self.db.fetchrow(f"SELECT COUNT(*) as cnt FROM {table} WHERE {where}")
            cnt = int(row["cnt"]) if row else 0
            if cnt == 0:
                return
            details: list[str] = []
            try:
                cols = _detail_cols.get(table, "stock_code, ts")
                detail_rows = await self.db.fetch(
                    f"SELECT {cols} FROM {table} WHERE {where} LIMIT 100"
                )
                for dr in detail_rows:
                    d = ts_to_date(dr["ts"])
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
            issues.append({"level": level, "check": check, "message": msg, "count": cnt})

        await _count_and_sample(
            "backtest_daily",
            "is_suspended IS NULL",
            "error",
            "null_is_suspended",
            "日线: {cnt} 条记录 is_suspended 为 NULL（需要回填）",
        )
        await _count_and_sample(
            "backtest_daily",
            "open_price IS NULL OR high_price IS NULL OR low_price IS NULL OR close_price IS NULL",
            "error",
            "null_prices",
            "日线: {cnt} 条记录价格字段为 NULL",
        )
        await _count_and_sample(
            "backtest_daily",
            "is_suspended = false AND (open_price = 0 OR close_price = 0)",
            "error",
            "zero_price_active",
            "日线: {cnt} 条非停牌记录 open/close 为 0",
        )
        await _count_and_sample(
            "backtest_daily",
            "open_price < 0 OR high_price < 0 OR low_price < 0 OR close_price < 0",
            "error",
            "negative_price",
            "日线: {cnt} 条记录价格为负数",
        )
        await _count_and_sample(
            "backtest_daily",
            "is_suspended = false AND high_price < low_price",
            "error",
            "high_lt_low",
            "日线: {cnt} 条非停牌记录 high < low",
        )
        await _count_and_sample(
            "backtest_daily",
            "is_suspended = false AND ("
            "open_price > high_price OR open_price < low_price "
            "OR close_price > high_price OR close_price < low_price)",
            "warning",
            "ohlc_range_violation",
            "日线: {cnt} 条非停牌记录 open/close 超出 [low,high] 范围",
        )
        await _count_and_sample(
            "backtest_daily",
            "vol < 0",
            "error",
            "negative_volume",
            "日线: {cnt} 条记录 vol 为负数",
        )

        row = await self.db.fetchrow(
            "SELECT COUNT(*) as cnt FROM backtest_daily WHERE is_suspended = false AND vol = 0"
        )
        zero_vol_cnt = int(row["cnt"]) if row else 0
        if zero_vol_cnt > 100:
            issues.append(
                {
                    "level": "warning",
                    "check": "zero_volume_active",
                    "message": (f"日线: {zero_vol_cnt} 条非停牌记录 vol=0（涨跌停无成交属正常）"),
                    "count": zero_vol_cnt,
                    "samples": [],
                }
            )

        minute_count = await self.get_minute_stock_count()
        if minute_count > 0:
            await _count_and_sample(
                "backtest_minute",
                "close_940 <= 0",
                "error",
                "zero_close_940",
                "分钟线: {cnt} 条记录 close_940 <= 0",
            )
            await _count_and_sample(
                "backtest_minute",
                "max_high < min_low",
                "error",
                "minute_high_lt_low",
                "分钟线: {cnt} 条记录 max_high < min_low",
            )
            await _count_and_sample(
                "backtest_minute",
                "close_940 IS NULL OR cum_volume IS NULL OR max_high IS NULL OR min_low IS NULL",
                "error",
                "null_minute_fields",
                "分钟线: {cnt} 条记录有 NULL 字段",
            )
            await _count_and_sample(
                "backtest_minute",
                "cum_volume < 0",
                "error",
                "negative_cum_volume",
                "分钟线: {cnt} 条记录 cum_volume 为负数",
            )

        return issues

    # ==================== Status (for UI) ====================

    _cache_status_result: dict | None = None
    _cache_status_ts: float = 0.0
    _CACHE_STATUS_TTL = 60.0  # seconds

    async def get_cache_status(self) -> dict:
        """Return cache status dict for the frontend status endpoint (60s TTL)."""
        import time as _time

        now = _time.monotonic()
        if self._cache_status_result and (now - self._cache_status_ts) < self._CACHE_STATUS_TTL:
            return self._cache_status_result

        result = await self.get_cache_status_streaming()
        self._cache_status_result = result
        self._cache_status_ts = now
        return result

    async def get_cache_status_streaming(self, on_step: Callable[[str], Any] | None = None) -> dict:
        """Query cache status with per-step progress callback."""

        async def _step(msg: str) -> None:
            if on_step:
                result = on_step(msg)
                if asyncio.iscoroutine(result):
                    await result

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


def create_storage_from_config() -> GreptimeBacktestStorage:
    """Create GreptimeBacktestStorage from env var settings."""
    import os

    host = os.environ.get("GREPTIME_HOST", "localhost")
    port = int(os.environ.get("GREPTIME_PORT", "4003"))
    return GreptimeBacktestStorage(host=host, port=port)
