# === MODULE PURPOSE ===
# Backtest data cache backed by GreptimeDB (replaces pickle+OSS TsanghiBacktestCache).
# Provides GreptimeBacktestCache for async read/write/download and
# GreptimeHistoricalAdapter implementing HistoricalDataProvider for V15Scanner.
#
# === KEY CONCEPTS ===
# - All reads go through SQL via asyncpg (PostgreSQL wire protocol, port 4003)
# - Natural upsert: same (stock_code, ts) = last write wins
# - No transactions needed — each INSERT is independently persisted via WAL
# - Volume stored in 手 (lots) — adapter converts ×100 at read time
# - Data sources: tsanghi (daily OHLCV) + baostock (5-min bars for 9:40 snapshot)
#
# === TABLES ===
# backtest_daily:  stock_code(TAG), ts(TIME INDEX), open_price, high_price,
#                  low_price, close_price, pre_close, vol, amount, turnover_ratio
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


class GreptimeClient:
    """Low-level async client for GreptimeDB via PostgreSQL wire protocol (port 4003).

    Uses asyncpg with GreptimeDB-specific settings:
    - statement_cache_size=0 (GreptimeDB doesn't support DEALLOCATE/PREPARE)
    - Single connection (no pool needed for this workload)
    """

    def __init__(self, host: str, port: int, database: str = "public") -> None:
        self._host = host
        self._port = port
        self._database = database
        self._conn: asyncpg.Connection | None = None

    async def start(self) -> None:
        self._conn = await asyncpg.connect(
            host=self._host,
            port=self._port,
            database=self._database,
            user="greptime",
            statement_cache_size=0,
        )

    async def stop(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    @property
    def is_connected(self) -> bool:
        return self._conn is not None and not self._conn.is_closed()

    async def execute(self, sql: str) -> str:
        """Execute DDL/DML and return status string."""
        if not self._conn:
            raise RuntimeError("GreptimeClient not started")
        return await self._conn.execute(sql)

    async def fetch(self, sql: str) -> list[asyncpg.Record]:
        """Execute SELECT and return rows."""
        if not self._conn:
            raise RuntimeError("GreptimeClient not started")
        return await self._conn.fetch(sql)

    async def fetchrow(self, sql: str) -> asyncpg.Record | None:
        """Execute SELECT and return first row."""
        if not self._conn:
            raise RuntimeError("GreptimeClient not started")
        return await self._conn.fetchrow(sql)


# ---------------------------------------------------------------------------
# Main cache class
# ---------------------------------------------------------------------------

# Integrity thresholds (same as old TsanghiBacktestCache)
_MIN_EXPECTED_STOCKS = 2500
_MAX_EXPECTED_STOCKS = 5000
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
            f"vol, amount, turnover_ratio "
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
            f"pre_close, vol, amount, turnover_ratio "
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

        return warnings

    # ==================== Status for UI ====================

    async def get_cache_status(self) -> dict:
        """Return cache status dict for the frontend status endpoint."""
        if not self.is_ready:
            return {"status": "disconnected"}

        db_start, db_end = await self.get_date_range()
        if db_start is None:
            return {"status": "empty"}

        daily_stocks = await self.get_daily_stock_count()
        daily_days = await self.get_daily_date_count()
        minute_stocks = await self.get_minute_stock_count()
        minute_gaps = await self.find_minute_gaps()
        gap_ranges = [[str(s), str(e)] for s, e in minute_gaps]

        return {
            "status": "ready",
            "start_date": str(db_start),
            "end_date": str(db_end),
            "daily_stocks": daily_stocks,
            "daily_days": daily_days,
            "minute_stocks": minute_stocks,
            "minute_gaps": gap_ranges,
            "has_gaps": len(gap_ranges) > 0,
        }

    # ==================== Download Methods ====================

    async def download_prices(
        self,
        start_date: date,
        end_date: date,
        progress_cb: Callable[[str, int, int], Any] | None = None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        """Download daily + minute data for all main-board stocks.

        Phase 1 — Daily OHLCV via tsanghi REST API (fast, batch per-date).
        Phase 2 — 5-min bars via baostock (per-stock, for 9:40 snapshot only).

        Data is written directly to GreptimeDB (each INSERT persisted independently).
        """
        # Extra history for lookback (preClose needs 1 day, QualityFilter needs ~30 days)
        dl_start = start_date - timedelta(days=60)

        if progress_cb:
            await _maybe_await(progress_cb("init", 0, 0))

        # Phase 1: Daily OHLCV from tsanghi
        stock_codes = await self._download_daily_tsanghi(
            dl_start,
            end_date,
            progress_cb,
            cancel_event,
        )

        # Phase 2: Minute data from baostock
        if stock_codes:
            await self._download_minute_baostock(
                stock_codes, dl_start, end_date, progress_cb, cancel_event
            )

        total = len(stock_codes)
        if progress_cb:
            await _maybe_await(progress_cb("download", total, total))

        daily_count = await self.get_daily_stock_count()
        minute_count = await self.get_minute_stock_count()
        logger.info(
            f"Download complete: {daily_count} daily stocks, "
            f"{minute_count} minute stocks out of {total} downloaded"
        )

        # Data integrity validation
        integrity_warnings = await self.validate_integrity()
        for w in integrity_warnings:
            logger.warning(f"Data integrity: {w}")

    async def _download_daily_tsanghi(
        self,
        dl_start: date,
        end_date: date,
        progress_cb: Callable[[str, int, int], Any] | None = None,
        cancel_event: threading.Event | None = None,
    ) -> list[str]:
        """Download daily OHLCV from tsanghi and INSERT to GreptimeDB.

        Returns list of all stock codes found.
        """
        from src.data.clients.tsanghi_client import TsanghiClient

        client = TsanghiClient()
        await client.start()

        try:
            # Resume: check which dates already exist in DB
            existing_dates = await self._get_existing_daily_dates()
            if existing_dates:
                latest_cached = max(existing_dates)
                if latest_cached >= dl_start:
                    skipped = (latest_cached - dl_start).days + 1
                    dl_start = latest_cached + timedelta(days=1)
                    logger.warning(
                        f"Daily resume: skipping {skipped} days "
                        f"(cached up to {latest_cached}), starting from {dl_start}"
                    )

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

                # Skip dates already in cache
                if current in existing_dates:
                    if progress_cb:
                        elapsed = (current - dl_start).days + 1
                        await _maybe_await(progress_cb("daily", elapsed, total_days))
                    current += timedelta(days=1)
                    continue

                day_records: list[tuple[str, dict]] = []  # (code, record_dict)

                for exchange in ("XSHG", "XSHE"):
                    try:
                        records = await client.daily_latest(exchange, date_str)
                    except RuntimeError as e:
                        logger.debug(f"tsanghi daily_latest({exchange}, {date_str}): {e}")
                        continue

                    if not records:
                        continue

                    for rec in records:
                        ticker = str(rec.get("ticker", ""))
                        if not ticker or len(ticker) != 6:
                            continue
                        # Main-board only: 60xxxx (SH) and 00xxxx (SZ)
                        if not (ticker.startswith("60") or ticker.startswith("00")):
                            continue

                        o = rec.get("open")
                        c = rec.get("close")
                        if o is None or c is None:
                            continue  # skip suspended stocks

                        pre_close = prev_close_map.get(ticker, 0.0)
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
                                },
                            )
                        )
                        all_stock_codes.add(ticker)

                if day_records:
                    trading_days_found += 1
                    # INSERT this day's data immediately
                    await self._write_daily(ts_ms, day_records)
                    # Update prev_close_map for next day
                    for code, rec_data in day_records:
                        prev_close_map[code] = rec_data["close"]

                if progress_cb:
                    elapsed = (current - dl_start).days + 1
                    await _maybe_await(progress_cb("daily", elapsed, total_days))

                current += timedelta(days=1)

            logger.info(
                f"tsanghi daily download: {len(all_stock_codes)} stocks, "
                f"{trading_days_found} trading days in [{dl_start} ~ {end_date}]"
            )
            return sorted(all_stock_codes)
        finally:
            await client.stop()

    async def _download_minute_baostock(
        self,
        codes: list[str],
        dl_start: date,
        end_date: date,
        progress_cb: Callable[[str, int, int], Any] | None = None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        """Download 5-min bars from baostock and INSERT to GreptimeDB."""
        # Resume: check which codes already have minute data
        existing_codes = await self._get_existing_minute_codes(dl_start, end_date)
        codes_to_download = [c for c in codes if c not in existing_codes]
        if existing_codes:
            logger.info(
                f"Minute resume: skipping {len(existing_codes)} stocks "
                f"with existing data, downloading {len(codes_to_download)}"
            )

        if not codes_to_download:
            return

        done = [0]
        total = len(codes_to_download)
        thread_exc: list[BaseException] = []
        # Collect minute data in thread, then INSERT from main async loop
        minute_queue: asyncio.Queue[tuple[str, dict[str, tuple]] | None] = asyncio.Queue()

        def _baostock_minute_download() -> None:
            import baostock as bs

            lg = bs.login()
            if lg.error_code != "0":
                thread_exc.append(RuntimeError(f"baostock login failed: {lg.error_msg}"))
                asyncio.get_event_loop().call_soon_threadsafe(minute_queue.put_nowait, None)
                return

            try:
                start_str = dl_start.strftime("%Y-%m-%d")
                end_str = end_date.strftime("%Y-%m-%d")

                for code in codes_to_download:
                    if cancel_event and cancel_event.is_set():
                        break

                    prefix = "sh" if code.startswith("6") else "sz"
                    bs_code = f"{prefix}.{code}"

                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        "date,time,open,high,low,close,volume",
                        start_date=start_str,
                        end_date=end_str,
                        frequency="5",
                        adjustflag="2",
                    )

                    # Aggregate: for each date, compute 9:40 snapshot
                    # 9:35 bar covers 9:30-9:35, 9:40 bar covers 9:35-9:40
                    day_data: dict[str, tuple[float, float, float, float]] = {}

                    while rs.error_code == "0" and rs.next():
                        row = rs.get_row_data()
                        bar_date = row[0]  # YYYY-MM-DD
                        bar_time = row[1]  # YYYYMMDDHHMMSSmmm
                        hhmm = bar_time[8:12]  # e.g. "0935", "0940"

                        if hhmm not in ("0935", "0940"):
                            continue

                        try:
                            float(row[2])  # open (unused, validate only)
                            h = float(row[3])
                            lo = float(row[4])
                            c = float(row[5])
                            v = float(row[6])
                        except (ValueError, IndexError):
                            continue

                        if bar_date in day_data:
                            prev_c, prev_v, prev_h, prev_l = day_data[bar_date]
                            day_data[bar_date] = (
                                c,  # close of latest bar (9:40)
                                prev_v + v,  # cumulative volume
                                max(prev_h, h),
                                min(prev_l, lo),
                            )
                        else:
                            day_data[bar_date] = (c, v, h, lo)

                    if day_data:
                        asyncio.get_event_loop().call_soon_threadsafe(
                            minute_queue.put_nowait, (code, day_data)
                        )
            except Exception as e:
                thread_exc.append(e)
            finally:
                bs.logout()
                asyncio.get_event_loop().call_soon_threadsafe(minute_queue.put_nowait, None)

        thread = threading.Thread(target=_baostock_minute_download, daemon=True)
        thread.start()

        # Consume from queue and INSERT
        while True:
            item = await minute_queue.get()
            if item is None:
                break

            code, min_data = item
            done[0] += 1
            await self._write_minute(code, min_data)

            if progress_cb and done[0] % 50 == 0:
                await _maybe_await(progress_cb("minute", done[0], total))

        thread.join(timeout=5)

        if progress_cb:
            await _maybe_await(progress_cb("minute", done[0], total))

        if thread_exc:
            raise thread_exc[0]

    # ==================== Internal Write Helpers ====================

    async def _write_daily(self, ts_ms: int, records: list[tuple[str, dict]]) -> None:
        """INSERT daily records for a single date (one INSERT per day)."""
        if not records:
            return
        values = []
        for code, rec in records:
            tr = rec["turnover_ratio"]
            tr_str = str(tr) if tr is not None else "NULL"
            values.append(
                f"('{code}',{ts_ms},"
                f"{rec['open']},{rec['high']},{rec['low']},{rec['close']},"
                f"{rec['pre_close']},{rec['volume']},{rec['amount']},{tr_str})"
            )
        sql = (
            "INSERT INTO backtest_daily"
            "(stock_code,ts,open_price,high_price,low_price,close_price,"
            "pre_close,vol,amount,turnover_ratio) VALUES " + ",".join(values)
        )
        await self._db.execute(sql)

    async def _write_minute(
        self, code: str, min_data: dict[str, tuple[float, float, float, float]]
    ) -> None:
        """INSERT minute data for a single stock (one INSERT per stock)."""
        if not min_data:
            return
        values = []
        for ds, (close_940, cum_vol, max_high, min_low) in min_data.items():
            ts_ms = _date_to_epoch_ms(_parse_date_str(ds))
            values.append(f"('{code}',{ts_ms},{close_940},{cum_vol},{max_high},{min_low})")
        sql = (
            "INSERT INTO backtest_minute"
            "(stock_code,ts,close_940,cum_volume,max_high,min_low) VALUES " + ",".join(values)
        )
        await self._db.execute(sql)

    # ==================== Resume Helpers ====================

    async def _get_existing_daily_dates(self) -> set[date]:
        """Get all dates that have daily data in DB."""
        rows = await self._db.fetch("SELECT DISTINCT ts FROM backtest_daily")
        return {_ts_to_date(r["ts"]) for r in rows}

    async def _get_existing_minute_codes(self, start_date: date, end_date: date) -> set[str]:
        """Get stock codes that have minute data in the given range."""
        start_ms = _date_to_epoch_ms(start_date)
        end_ms = _date_to_epoch_ms(end_date)
        rows = await self._db.fetch(
            f"SELECT DISTINCT stock_code FROM backtest_minute "
            f"WHERE ts >= {start_ms} AND ts <= {end_ms}"
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
# Historical adapter for V15Scanner (backtest mode)
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
        """Return cached daily data in iFinD history_quotes format."""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        indicator_list = [ind.strip() for ind in indicators.split(",")]
        tables: list[dict[str, Any]] = []

        start_ms = _date_to_epoch_ms(_parse_date_str(start_date))
        end_ms = _date_to_epoch_ms(_parse_date_str(end_date))

        for full_code in code_list:
            bare = full_code.split(".")[0]
            rows = await self._cache._db.fetch(
                f"SELECT ts, open_price, high_price, low_price, close_price, "
                f"pre_close, vol, amount, turnover_ratio "
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
        """Return cached minute data in iFinD high_frequency format."""
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
        """Get trading dates via baostock."""

        def _query() -> list[str]:
            import baostock as bs

            lg = bs.login()
            if lg.error_code != "0":
                raise RuntimeError(f"baostock login failed: {lg.error_msg}")
            try:
                rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
                dates: list[str] = []
                while rs.next():
                    row = rs.get_row_data()
                    if row[1] == "1":
                        dates.append(row[0])
                return dates
            finally:
                bs.logout()

        try:
            return await asyncio.to_thread(_query)
        except Exception:
            logger.error("Failed to get trade dates")
            raise


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
