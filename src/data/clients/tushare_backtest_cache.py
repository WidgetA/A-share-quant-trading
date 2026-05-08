# === MODULE PURPOSE ===
# Pre-downloads A-share daily + minute price data for backtesting from Tushare Pro.
# Provides an adapter that mimics IFinDHttpClient interface so
# MomentumSectorScanner can run without any code changes.

# === DEPENDENCIES ===
# - Tushare Pro REST API: daily OHLCV (batch per trade_date) + stk_mins 1-min bars
# - akshare: trading calendar (get_trade_dates)
# - IFinDHttpClient interface: Adapter returns data in iFinD response format

# === KEY CONCEPTS ===
# - TushareBacktestCache: Downloads and stores all price data in memory + OSS
# - TushareHistoricalAdapter: Duck-types IFinDHttpClient for the scanner
# - Daily OHLCV: Tushare `daily` API (batch per trade_date, fast)
# - Minute bars: Tushare `stk_mins` (per-stock, freq=1min), aggregated for 9:40 snapshot
# - OSS cache: Alibaba Cloud OSS — survives container redeployment
# - Data is NOT used for live trading (backtest only)
# - Daily volume from tushare `daily` is in 手 (lots) — adapter converts ×100 at read time
# - Minute volume from tushare `stk_mins` is in 股 (shares) — stored as-is

from __future__ import annotations

import asyncio
import gzip
import logging
import os
import pickle
import tempfile
from datetime import date, datetime, timedelta
from typing import Any, Callable

import httpx

logger = logging.getLogger(__name__)

# OSS cache key prefix — distinct from old tsanghi-era prefix so the cache schema
# change (different units, different fields) doesn't accidentally collide.
_OSS_PREFIX = "tushare-backtest-cache/"

# Tushare Pro endpoint
_TUSHARE_API_URL = "http://api.tushare.pro"

# stk_mins concurrency: paid plans support more, but conservative default avoids
# tripping the per-minute call limit. Retries on rate-limit errors will self-throttle,
# so this is a soft cap — bump it up if your Tushare tier allows it.
_STK_MINS_CONCURRENCY = 5
_DAILY_TIMEOUT = 30.0
_STK_MINS_TIMEOUT = 60.0
_MAX_RETRIES = 3
_RETRY_BACKOFF = 2.0  # seconds; doubles each attempt

# Tushare `stk_mins` caps each call at ~8000 rows (DESC-truncated). Each calendar
# day has ≤240 1-min bars across the 09:30-15:00 trading session, so we chunk
# the date range to keep each call comfortably under that cap. 20 calendar days
# ≈ 14 trading days × 240 ≈ 3360 bars — leaves headroom for half-days, etc.
_STK_MINS_CHUNK_DAYS = 20


class TushareBacktestError(Exception):
    """Error from Tushare Pro API during backtest data download."""


def _get_oss_bucket():
    """Get OSS bucket instance from environment variables. Returns None if not configured."""
    key_id = os.environ.get("OSS_ACCESS_KEY_ID")
    key_secret = os.environ.get("OSS_ACCESS_KEY_SECRET")
    endpoint = os.environ.get("OSS_ENDPOINT")
    bucket_name = os.environ.get("OSS_BUCKET_NAME")
    if not all([key_id, key_secret, endpoint, bucket_name]):
        logger.warning("OSS not configured — cache persistence disabled")
        return None
    import oss2

    auth = oss2.Auth(key_id, key_secret)
    return oss2.Bucket(auth, endpoint, bucket_name)


def check_oss_available() -> str | None:
    """Pre-flight check: verify OSS is configured and reachable.

    Returns None if OK, or an error message string.
    """
    bucket = _get_oss_bucket()
    if bucket is None:
        oss_vars = (
            "OSS_ACCESS_KEY_ID",
            "OSS_ACCESS_KEY_SECRET",
            "OSS_ENDPOINT",
            "OSS_BUCKET_NAME",
        )
        missing = [v for v in oss_vars if not os.environ.get(v)]
        return f"OSS 环境变量缺失: {', '.join(missing)}"
    try:
        bucket.get_bucket_info()
        return None
    except Exception as e:
        return f"OSS 连接失败: {e}"


def _bare_to_ts_code(code: str) -> str:
    """Convert bare 6-digit code to Tushare ts_code: 600519 -> 600519.SH."""
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"


async def _tushare_call(
    client: httpx.AsyncClient,
    token: str,
    api_name: str,
    params: dict[str, Any],
    fields: str = "",
) -> dict[str, Any]:
    """Single Tushare Pro POST call with retry. Returns the parsed response dict."""
    body: dict[str, Any] = {
        "api_name": api_name,
        "token": token,
        "params": params,
    }
    if fields:
        body["fields"] = fields

    last_err: Exception | None = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = await client.post(_TUSHARE_API_URL, json=body)
            resp.raise_for_status()
            data = resp.json()

            code = data.get("code")
            if code != 0:
                msg = data.get("msg", "unknown error")
                # Rate-limit messages are retryable; auth errors aren't.
                msg_lower = str(msg).lower()
                if "每分钟" in str(msg) or "rate" in msg_lower or "频率" in str(msg):
                    raise httpx.HTTPStatusError(
                        f"Tushare rate limit: {msg}",
                        request=resp.request,
                        response=resp,
                    )
                raise TushareBacktestError(
                    f"Tushare {api_name} error: code={code}, msg={msg}"
                )
            return data

        except TushareBacktestError:
            raise

        except (httpx.HTTPError, ConnectionError, OSError) as exc:
            last_err = exc
            if attempt < _MAX_RETRIES:
                wait = _RETRY_BACKOFF * (2 ** (attempt - 1))
                logger.warning(
                    f"Tushare {api_name} attempt {attempt}/{_MAX_RETRIES} failed: "
                    f"{exc}; retrying in {wait:.1f}s"
                )
                await asyncio.sleep(wait)
            else:
                break

    raise TushareBacktestError(
        f"Tushare {api_name} failed after {_MAX_RETRIES} attempts: {last_err}"
    )


class TushareBacktestCache:
    """
    Pre-downloads daily OHLCV and 09:30-09:40 minute bar data for all
    main-board A-share stocks, keyed by (stock_code, date).

    OSS persistence:
        After download, data is saved to Alibaba Cloud OSS as pickle files.
        On next request, if the cached range covers the requested range,
        data is loaded from OSS instead of re-downloading.

    Usage:
        cache = TushareBacktestCache()
        await cache.download_prices(start_date, end_date, progress_cb)
        snap = cache.get_daily(code, trade_date)
        p940 = cache.get_940_price(code, trade_date)
    """

    def __init__(self) -> None:
        # daily[code][date_str] = {open, high, low, close, preClose, volume, amount, turnoverRatio}
        # volume stored in 手 (Tushare native unit); adapter converts ×100 at read time.
        self._daily: dict[str, dict[str, dict[str, float | None]]] = {}
        # minute[code][date_str] = (close_at_940, cum_volume_股, max_high, min_low)
        self._minute: dict[str, dict[str, tuple[float, float, float, float]]] = {}
        self._stock_codes: list[str] = []
        self._is_ready = False
        self._start_date: date | None = None
        self._end_date: date | None = None

    @property
    def is_ready(self) -> bool:
        return self._is_ready

    @property
    def stock_codes(self) -> list[str]:
        return self._stock_codes

    def get_daily(self, code: str, date_str: str) -> dict[str, float | None] | None:
        """Get daily OHLCV for a stock on a date. date_str is YYYY-MM-DD."""
        return self._daily.get(code, {}).get(date_str)

    def get_940_price(self, code: str, date_str: str) -> tuple[float, float, float, float] | None:
        """Get 9:40 price data: (close, cum_volume_股, max_high, min_low)."""
        return self._minute.get(code, {}).get(date_str)

    def get_all_codes_with_daily(self, date_str: str) -> dict[str, dict[str, float | None]]:
        """Get daily data for ALL stocks on a specific date."""
        result: dict[str, dict[str, float | None]] = {}
        for code, dates in self._daily.items():
            day_data = dates.get(date_str)
            if day_data:
                result[code] = day_data
        return result

    def find_minute_gaps(self) -> list[tuple[date, date]]:
        """Find date ranges where daily data exists but minute data is sparse/missing."""
        daily_date_counts: dict[str, int] = {}
        minute_date_counts: dict[str, int] = {}

        for _code, dates in self._daily.items():
            for ds in dates:
                daily_date_counts[ds] = daily_date_counts.get(ds, 0) + 1
        for _code, min_dates in self._minute.items():
            for ds in min_dates:
                minute_date_counts[ds] = minute_date_counts.get(ds, 0) + 1

        gap_dates: list[date] = []
        for ds, daily_count in daily_date_counts.items():
            if daily_count <= 100:
                continue
            minute_count = minute_date_counts.get(ds, 0)
            if minute_count < daily_count * 0.5:
                try:
                    gap_dates.append(datetime.strptime(ds, "%Y-%m-%d").date())
                except ValueError:
                    continue

        if not gap_dates:
            return []

        gap_dates.sort()
        ranges: list[tuple[date, date]] = []
        range_start = gap_dates[0]
        prev = gap_dates[0]
        for d in gap_dates[1:]:
            if (d - prev).days <= 3:
                prev = d
            else:
                ranges.append((range_start, prev))
                range_start = d
                prev = d
        ranges.append((range_start, prev))
        return ranges

    def missing_ranges(self, start_date: date, end_date: date) -> list[tuple[date, date]]:
        """Return date ranges not covered by this cache (boundary + internal gaps)."""
        if not self._is_ready or not self._start_date or not self._end_date:
            return [(start_date, end_date)]

        gaps: list[tuple[date, date]] = []

        if start_date < self._start_date:
            gaps.append((start_date, self._start_date - timedelta(days=1)))
        if end_date > self._end_date:
            gaps.append((self._end_date + timedelta(days=1), end_date))

        for gap_start, gap_end in self.find_minute_gaps():
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

    def copy(self) -> TushareBacktestCache:
        """Create a copy safe for merge_from without mutating the original."""
        clone = TushareBacktestCache()
        clone._daily = {code: dict(dates) for code, dates in self._daily.items()}
        clone._minute = {code: dict(dates) for code, dates in self._minute.items()}
        clone._stock_codes = list(self._stock_codes)
        clone._start_date = self._start_date
        clone._end_date = self._end_date
        clone._is_ready = self._is_ready
        return clone

    def merge_from(self, other: TushareBacktestCache) -> None:
        """Merge data from another cache (e.g. a gap download) into this one."""
        for code, dates in other._daily.items():
            if code in self._daily:
                self._daily[code].update(dates)
            else:
                self._daily[code] = dates
        for code, min_dates in other._minute.items():
            if code in self._minute:
                self._minute[code].update(min_dates)
            else:
                self._minute[code] = min_dates
        existing_codes = set(self._stock_codes)
        for code in other._stock_codes:
            if code not in existing_codes:
                self._stock_codes.append(code)
        if other._start_date:
            if self._start_date is None or other._start_date < self._start_date:
                self._start_date = other._start_date
        if other._end_date:
            if self._end_date is None or other._end_date > self._end_date:
                self._end_date = other._end_date

    def covers_range(self, start_date: date, end_date: date) -> bool:
        """Check if cached data covers the requested date range."""
        if not self._is_ready or not self._start_date or not self._end_date:
            return False
        if not (self._start_date <= start_date and self._end_date >= end_date):
            return False
        for gap_start, gap_end in self.find_minute_gaps():
            if gap_start <= end_date and gap_end >= start_date:
                return False
        return True

    def _recalculate_date_range(self) -> None:
        """Recompute _start_date/_end_date from intersection of daily and minute ranges."""

        def _scan_range(data: dict[str, dict]) -> tuple[date | None, date | None]:
            lo: date | None = None
            hi: date | None = None
            sampled = 0
            for _code, dates in data.items():
                if not dates:
                    continue
                keys = sorted(dates.keys())
                try:
                    first = datetime.strptime(keys[0], "%Y-%m-%d").date()
                    last = datetime.strptime(keys[-1], "%Y-%m-%d").date()
                except (ValueError, IndexError):
                    continue
                if lo is None or first < lo:
                    lo = first
                if hi is None or last > hi:
                    hi = last
                sampled += 1
                if sampled >= 200:
                    break
            return lo, hi

        d_lo, d_hi = _scan_range(self._daily)
        m_lo, m_hi = _scan_range(self._minute)  # type: ignore[arg-type]

        if d_lo and d_hi:
            if m_lo and m_hi:
                new_start = max(d_lo, m_lo)
                new_end = min(d_hi, m_hi)
            else:
                new_start, new_end = d_lo, d_hi

            old_start, old_end = self._start_date, self._end_date
            self._start_date = new_start
            self._end_date = new_end
            if old_start != new_start or old_end != new_end:
                logger.warning(
                    f"Date range recalculated: "
                    f"[{old_start} ~ {old_end}] → [{new_start} ~ {new_end}]"
                    f" (daily=[{d_lo}~{d_hi}], minute=[{m_lo}~{m_hi}])"
                )

    def _has_valid_format(self) -> bool:
        """Check if daily data has required fields (close, volume)."""
        for _code, dates in self._daily.items():
            for _ds, day in dates.items():
                return "close" in day and "volume" in day
        return False

    def _save_to_oss(self) -> str | None:
        """Persist cache to Alibaba Cloud OSS as gzip-compressed pickle files."""
        bucket = _get_oss_bucket()
        if bucket is None:
            return "OSS 未配置（缺少环境变量）"

        meta = {
            "start_date": self._start_date,
            "end_date": self._end_date,
            "stock_codes": self._stock_codes,
        }
        try:
            for name, obj in [
                ("daily.pkl.gz", self._daily),
                ("minute.pkl.gz", self._minute),
                ("meta.pkl.gz", meta),
            ]:
                tmp_fd, tmp_path = tempfile.mkstemp(suffix=".pkl.gz")
                try:
                    with os.fdopen(tmp_fd, "wb") as raw_f:
                        with gzip.GzipFile(fileobj=raw_f, mode="wb", compresslevel=1) as gz_f:
                            pickle.dump(obj, gz_f, protocol=pickle.HIGHEST_PROTOCOL)
                    size_mb = os.path.getsize(tmp_path) / 1024 / 1024
                    logger.info(f"Uploading {name} to OSS ({size_mb:.1f} MB compressed)...")
                    bucket.put_object_from_file(f"{_OSS_PREFIX}{name}", tmp_path)
                    logger.info(f"Uploaded {name} to OSS OK")
                finally:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
            logger.info(
                f"Cache saved to OSS: {len(self._daily)} daily, {len(self._minute)} minute"
            )
            return None
        except Exception as e:
            msg = f"OSS 上传失败: {e}"
            logger.error(msg)
            return msg

    @classmethod
    def load_from_oss(cls) -> TushareBacktestCache | None:
        """Load cache from OSS if available. Returns None if not found."""
        bucket = _get_oss_bucket()
        if bucket is None:
            logger.warning("load_from_oss: _get_oss_bucket() returned None")
            return None

        try:
            files = {}
            base_names = ("meta", "daily", "minute")
            for base in base_names:
                gz_key = f"{_OSS_PREFIX}{base}.pkl.gz"
                if not bucket.object_exists(gz_key):
                    logger.warning(f"load_from_oss: key not found: {gz_key}")
                    return None
                logger.info(f"load_from_oss: downloading {gz_key}...")
                tmp_fd, tmp_path = tempfile.mkstemp(suffix=".pkl.gz")
                try:
                    os.close(tmp_fd)
                    bucket.get_object_to_file(gz_key, tmp_path)
                    size_mb = os.path.getsize(tmp_path) / 1024 / 1024
                    logger.info(f"load_from_oss: {gz_key} downloaded ({size_mb:.1f} MB)")
                    with gzip.open(tmp_path, "rb") as gz_f:
                        files[base] = pickle.load(gz_f)  # noqa: S301
                finally:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)

            meta = files["meta"]
            cache = cls()
            cache._start_date = meta["start_date"]
            cache._end_date = meta["end_date"]
            cache._stock_codes = meta["stock_codes"]
            cache._daily = files["daily"]
            cache._minute = files["minute"]

            if not cache._has_valid_format():
                logger.warning(
                    "OSS cache is STALE: daily data missing required fields. "
                    "Discarding — will re-download."
                )
                return None

            cache._recalculate_date_range()
            cache._is_ready = True
            logger.info(
                f"Cache loaded from OSS: {len(cache._daily)} daily, "
                f"{len(cache._minute)} minute, "
                f"range [{cache._start_date} ~ {cache._end_date}]"
            )
            return cache
        except Exception:
            logger.error("Failed to load cache from OSS", exc_info=True)
            raise

    async def download_prices(
        self,
        start_date: date,
        end_date: date,
        progress_cb: Callable[[str, int, int], Any] | None = None,
    ) -> None:
        """
        Download daily + minute data from Tushare for all main-board stocks.

        Phase 1 — Daily OHLCV via Tushare `daily` (one call per trade_date).
        Phase 2 — 9:30-9:40 1-min bars via Tushare `stk_mins` (per stock, concurrent).

        Args:
            start_date: First trading date (inclusive).
            end_date: Last trading date (inclusive).
            progress_cb: Optional callback(phase, current, total).
        """
        from src.common.config import get_tushare_token

        token = get_tushare_token()

        self._start_date = start_date
        self._end_date = end_date
        # Extra history before start_date for preClose + lookbacks (~30 trading days
        # for QualityFilter turnover, +5 for trend, plus weekend buffer).
        dl_start = start_date - timedelta(days=60)

        if progress_cb:
            await _maybe_await(progress_cb("init", 0, 0))

        async with httpx.AsyncClient(timeout=httpx.Timeout(_DAILY_TIMEOUT)) as client:
            # --- Phase 1: Daily OHLCV ---
            await self._download_daily(client, token, dl_start, end_date, progress_cb)

            # Tushare `daily` returns pre_close per row, but only for rows that
            # have data. Backfill anything missing from the previous trading day.
            self._compute_pre_close()

            # --- Phase 2: 1-min bars ---
            codes = list(self._daily.keys())
            self._stock_codes = codes
            if codes:
                await self._download_minute(
                    client, token, codes, dl_start, end_date, progress_cb
                )

        total = len(self._stock_codes)
        if progress_cb:
            await _maybe_await(progress_cb("download", total, total))
        logger.info(
            f"Download complete: {len(self._daily)} daily (tushare), "
            f"{len(self._minute)} minute (tushare stk_mins) out of {total} stocks"
        )

        self._recalculate_date_range()
        self._is_ready = True

    async def _download_daily(
        self,
        client: httpx.AsyncClient,
        token: str,
        dl_start: date,
        end_date: date,
        progress_cb: Callable[[str, int, int], Any] | None = None,
    ) -> None:
        """Download daily OHLCV via Tushare `daily` (one call per trade_date).

        Each call returns ALL stocks for that trade_date, so a 90-day backtest
        with 60-day lookback is ~150 calls total — well within rate limits.
        """
        total_days = (end_date - dl_start).days + 1
        trading_days_found = 0
        current = dl_start

        while current <= end_date:
            if current.weekday() >= 5:
                # Weekend — Tushare returns empty, skip the call entirely.
                current += timedelta(days=1)
                if progress_cb:
                    elapsed = (current - dl_start).days
                    await _maybe_await(progress_cb("daily", elapsed, total_days))
                continue

            ts_date = current.strftime("%Y%m%d")
            try:
                data = await _tushare_call(
                    client,
                    token,
                    "daily",
                    {"trade_date": ts_date},
                    fields="ts_code,trade_date,open,high,low,close,pre_close,vol,amount",
                )
            except TushareBacktestError as e:
                logger.warning(f"tushare daily {ts_date}: {e}")
                current += timedelta(days=1)
                continue

            fields = data.get("data", {}).get("fields", [])
            items = data.get("data", {}).get("items", [])

            if items:
                idx = {f: i for i, f in enumerate(fields)}
                date_str = current.strftime("%Y-%m-%d")
                day_has_data = False

                for row in items:
                    ts_code = row[idx.get("ts_code", -1)]
                    if not ts_code:
                        continue
                    bare = ts_code.split(".")[0]
                    if len(bare) != 6:
                        continue
                    # Filter to main-board only: 60xxxx (SH) and 00xxxx (SZ).
                    if not (bare.startswith("60") or bare.startswith("00")):
                        continue

                    o = row[idx.get("open", -1)] if "open" in idx else None
                    c = row[idx.get("close", -1)] if "close" in idx else None
                    if o is None or c is None:
                        continue  # suspended

                    pc = row[idx.get("pre_close", -1)] if "pre_close" in idx else None
                    h = row[idx.get("high", -1)] if "high" in idx else None
                    lo = row[idx.get("low", -1)] if "low" in idx else None
                    vol = row[idx.get("vol", -1)] if "vol" in idx else None
                    amt = row[idx.get("amount", -1)] if "amount" in idx else None

                    if bare not in self._daily:
                        self._daily[bare] = {}
                    self._daily[bare][date_str] = {
                        "open": float(o),
                        "high": float(h) if h is not None else float(o),
                        "low": float(lo) if lo is not None else float(o),
                        "close": float(c),
                        "preClose": float(pc) if pc is not None else 0.0,
                        # Tushare `daily` vol is in 手 (lots); stored as-is, adapter ×100.
                        "volume": float(vol) if vol is not None else 0.0,
                        # Tushare `daily` amount is in 千元; convert to 元 here.
                        "amount": float(amt) * 1000.0 if amt is not None else 0.0,
                        # Tushare `daily` doesn't return turnover_ratio.
                        "turnoverRatio": None,
                    }
                    day_has_data = True

                if day_has_data:
                    trading_days_found += 1

            if progress_cb:
                elapsed = (current - dl_start).days + 1
                await _maybe_await(progress_cb("daily", elapsed, total_days))

            current += timedelta(days=1)

        logger.info(
            f"tushare daily download: {len(self._daily)} stocks, "
            f"{trading_days_found} trading days in [{dl_start} ~ {end_date}]"
        )

    def _compute_pre_close(self) -> None:
        """Fill preClose for entries Tushare didn't supply (first trading day, etc.)."""
        for code, dates in self._daily.items():
            sorted_dates = sorted(dates.keys())
            for i, ds in enumerate(sorted_dates):
                day = dates[ds]
                if day.get("preClose") is None or day.get("preClose") == 0.0:
                    if i > 0:
                        prev_ds = sorted_dates[i - 1]
                        day["preClose"] = dates[prev_ds]["close"]

    async def _download_minute(
        self,
        client: httpx.AsyncClient,
        token: str,
        codes: list[str],
        dl_start: date,
        end_date: date,
        progress_cb: Callable[[str, int, int], Any] | None = None,
    ) -> None:
        """Download 1-min bars (09:30-09:40) via Tushare `stk_mins` per stock.

        For each stock, the [dl_start, end_date] range is split into chunks small
        enough to stay under Tushare's per-call row cap (see _STK_MINS_CHUNK_DAYS).
        Bars are filtered client-side to keep only those covering the 09:30-09:40
        continuous-trading window (the 09:30:00 call-auction bar is excluded so
        cum_vol matches the legacy tsanghi 5-min semantics).

        Volume is in 股 (Tushare `stk_mins` native unit) — no conversion.

        Per-stock atomicity: if any chunk for a stock fails, the whole stock is
        marked failed and no partial data is stored.
        """
        # Build chunk list once — reused across all stocks.
        chunks: list[tuple[date, date]] = []
        ck_start = dl_start
        while ck_start <= end_date:
            ck_end = min(ck_start + timedelta(days=_STK_MINS_CHUNK_DAYS - 1), end_date)
            chunks.append((ck_start, ck_end))
            ck_start = ck_end + timedelta(days=1)
        logger.info(
            f"tushare stk_mins: {len(codes)} stocks × {len(chunks)} chunks "
            f"= {len(codes) * len(chunks)} calls "
            f"(chunk size {_STK_MINS_CHUNK_DAYS}d, range {dl_start} ~ {end_date})"
        )

        sem = asyncio.Semaphore(_STK_MINS_CONCURRENCY)
        done_count = [0]
        fail_count = [0]
        total = len(codes)

        # Use a longer-timeout client for stk_mins (sometimes slow under load).
        async with httpx.AsyncClient(timeout=httpx.Timeout(_STK_MINS_TIMEOUT)) as mins_client:

            async def _fetch_one(
                code: str,
            ) -> tuple[str, dict[str, tuple[float, float, float, float]]]:
                ts_code = _bare_to_ts_code(code)
                min_data: dict[str, tuple[float, float, float, float]] = {}

                async with sem:
                    for ck_start_d, ck_end_d in chunks:
                        s_dt = f"{ck_start_d.strftime('%Y-%m-%d')} 09:30:00"
                        e_dt = f"{ck_end_d.strftime('%Y-%m-%d')} 09:41:00"
                        try:
                            data = await _tushare_call(
                                mins_client,
                                token,
                                "stk_mins",
                                {
                                    "ts_code": ts_code,
                                    "freq": "1min",
                                    "start_date": s_dt,
                                    "end_date": e_dt,
                                },
                                fields="ts_code,trade_time,open,close,high,low,vol",
                            )
                        except (TushareBacktestError, httpx.HTTPError) as exc:
                            # Per-stock atomicity: any chunk failure → drop the
                            # whole stock so we never store a partial timeline.
                            fail_count[0] += 1
                            if fail_count[0] <= 10:
                                logger.warning(
                                    f"tushare stk_mins({code}, "
                                    f"{ck_start_d}~{ck_end_d}) failed: {exc} "
                                    f"— dropping all minute data for {code}"
                                )
                            elif fail_count[0] == 11:
                                logger.warning(
                                    "tushare stk_mins: too many failures, "
                                    "suppressing further warnings"
                                )
                            done_count[0] += 1
                            return code, {}

                        fields = data.get("data", {}).get("fields", [])
                        items = data.get("data", {}).get("items", [])
                        if not fields or not items:
                            continue  # this chunk had no bars (e.g. all weekends)

                        idx = {f: i for i, f in enumerate(fields)}
                        if "trade_time" not in idx or "close" not in idx:
                            continue

                        # Tushare returns minute bars in DESC; sort ASC so the
                        # last bar processed per date is the 09:40 bar.
                        rows = sorted(items, key=lambda r: r[idx["trade_time"]])

                        for row in rows:
                            t_str = str(row[idx["trade_time"]])
                            if " " not in t_str:
                                continue
                            ds, hms = t_str.split(" ")
                            # Tushare returns "YYYY-MM-DD HH:MM:SS".
                            hhmm = hms.replace(":", "")[:4]
                            # Keep 09:31-09:40 (continuous trading 09:30-09:40).
                            # Excludes 09:30:00 (call-auction bar) to match the
                            # legacy tsanghi 5-min cum_vol semantics.
                            if hhmm <= "0930" or hhmm > "0940":
                                continue

                            c = row[idx["close"]]
                            h = row[idx["high"]] if "high" in idx else None
                            lo = row[idx["low"]] if "low" in idx else None
                            v = row[idx["vol"]] if "vol" in idx else None
                            if c is None or h is None or lo is None:
                                continue
                            try:
                                close_val = float(c)
                                high_val = float(h)
                                low_val = float(lo)
                                vol_val = float(v) if v is not None else 0.0
                            except (TypeError, ValueError):
                                continue

                            if close_val <= 0 or high_val <= 0 or low_val <= 0:
                                continue

                            if ds in min_data:
                                prev = min_data[ds]
                                min_data[ds] = (
                                    close_val,  # last bar's close = 09:40 close
                                    prev[1] + vol_val,
                                    max(prev[2], high_val),
                                    min(prev[3], low_val),
                                )
                            else:
                                min_data[ds] = (close_val, vol_val, high_val, low_val)

                    done_count[0] += 1
                    return code, min_data

            tasks = [asyncio.create_task(_fetch_one(code)) for code in codes]

            while not all(t.done() for t in tasks):
                await asyncio.sleep(2)
                if progress_cb:
                    await _maybe_await(progress_cb("minute", done_count[0], total))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = 0
            for result in results:
                if isinstance(result, BaseException):
                    raise result
                code, min_data = result
                if min_data:
                    self._minute[code] = min_data
                    success_count += 1

        if fail_count[0] > 0:
            logger.warning(
                f"tushare stk_mins download: {success_count} OK, "
                f"{fail_count[0]} failed out of {total} stocks"
            )
        else:
            logger.info(
                f"tushare stk_mins download: {success_count} OK out of {total} stocks"
            )

    async def save_to_oss(self) -> str | None:
        """Save cache to OSS. Returns None on success, error message on failure."""
        return await asyncio.to_thread(self._save_to_oss)


class TushareHistoricalAdapter:
    """
    Duck-types IFinDHttpClient for backtest use.

    Reads from TushareBacktestCache and returns data in iFinD response format
    so MomentumSectorScanner / V15Scanner / V16 work without modification.
    """

    def __init__(self, cache: TushareBacktestCache, trade_date: date | None = None) -> None:
        self._cache = cache
        self.trade_date: date | None = trade_date

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
        tables: list[dict[str, Any]] = []

        for full_code in code_list:
            bare = full_code.split(".")[0]
            daily_dates = self._cache._daily.get(bare, {})

            time_vals: list[str] = []
            indicator_data: dict[str, list] = {}
            for ind in indicators.split(","):
                indicator_data[ind.strip()] = []

            d = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_d = datetime.strptime(end_date, "%Y-%m-%d").date()
            while d <= end_d:
                ds = d.strftime("%Y-%m-%d")
                day = daily_dates.get(ds)
                if day:
                    time_vals.append(ds)
                    for ind in indicators.split(","):
                        ind = ind.strip()
                        val = day.get(ind)
                        # tushare daily volume is in 手 — convert ×100 to 股 here.
                        if ind == "volume" and val is not None:
                            val = val * 100
                        indicator_data[ind].append(val)
                d += timedelta(days=1)

            if time_vals:
                table = {"time": time_vals, **indicator_data}
                tables.append({"thscode": full_code, "table": table})

        return {"errorcode": 0, "tables": tables}

    async def high_frequency(
        self,
        codes: str,
        indicators: str,
        start_time: str,
        end_time: str,
        function_para: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Return cached minute data (single 9:40 aggregate bar) in iFinD format."""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        tables: list[dict[str, Any]] = []

        date_str = start_time.split(" ")[0]

        for full_code in code_list:
            bare = full_code.split(".")[0]
            data_940 = self._cache.get_940_price(bare, date_str)
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
        """Return empty result — backtest path supplies candidates directly."""
        return {"errorcode": 0, "tables": []}

    async def real_time_quotation(
        self,
        codes: str,
        indicators: str,
    ) -> dict[str, Any]:
        """Return 9:40 snapshot data from cache for the backtest date."""
        if self.trade_date is None:
            return {"errorcode": 0, "tables": []}

        date_str = self.trade_date.strftime("%Y-%m-%d")
        tables: list[dict[str, Any]] = []

        for full_code in (c.strip() for c in codes.split(",") if c.strip()):
            bare = full_code.split(".")[0]
            day = self._cache.get_daily(bare, date_str)
            data_940 = self._cache.get_940_price(bare, date_str)
            if not day or not data_940:
                continue

            close_940, vol_940, high_940, low_940 = data_940
            table: dict[str, list[float | None]] = {}
            for ind in (i.strip() for i in indicators.split(",")):
                if ind == "open":
                    table["open"] = [day.get("open", 0.0)]
                elif ind == "preClose":
                    # Match live behavior: TushareRealtimeClient returns None
                    # for preClose (rt_min endpoint doesn't provide it).
                    table["preClose"] = [None]
                elif ind == "latest":
                    table["latest"] = [close_940]
                elif ind == "volume":
                    table["volume"] = [vol_940]
                elif ind == "high":
                    table["high"] = [high_940]
                elif ind == "low":
                    table["low"] = [low_940]
            tables.append({"thscode": full_code, "table": table})

        return {"errorcode": 0, "tables": tables}

    async def get_trade_dates(
        self,
        market_code: str,
        start_date: str,
        end_date: str,
    ) -> list[str]:
        """Get trading dates via akshare (same source as live IQuantHistoricalAdapter)."""
        import akshare as ak

        df = await asyncio.to_thread(ak.tool_trade_date_hist_sina)
        all_dates = df["trade_date"].dt.date
        sd = datetime.strptime(start_date, "%Y-%m-%d").date()
        ed = datetime.strptime(end_date, "%Y-%m-%d").date()
        return [d.strftime("%Y-%m-%d") for d in sorted(all_dates) if sd <= d <= ed]


async def _maybe_await(result: Any) -> None:
    """Await result if it's a coroutine, otherwise do nothing."""
    if asyncio.iscoroutine(result):
        await result
