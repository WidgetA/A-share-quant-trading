"""V20-owned canonical V16 selection computation.

This module owns the state, resources, single-flight cache, persistence hook,
and reproducible selection bundle used by V20. It may reuse the stateless V16
scanner and ranking algorithm, but it never imports or mutates the V16 runtime,
scheduler, trade calendar, Feishu callbacks, or iQuant state.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from math import isfinite
from pathlib import Path
from types import MappingProxyType
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from src.data.clients.tushare_realtime import TushareDailyBar, TushareEarlyMarketData

logger = logging.getLogger(__name__)
BEIJING_TZ = ZoneInfo("Asia/Shanghai")
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_trade_calendar_cache: list[date] | None = None


async def get_v20_trade_calendar() -> list[date]:
    """Load V20's own cached exchange calendar without V16 runtime state."""

    global _trade_calendar_cache
    if _trade_calendar_cache is not None:
        return _trade_calendar_cache
    import akshare as ak

    frame = await asyncio.to_thread(ak.tool_trade_date_hist_sina)
    _trade_calendar_cache = sorted(
        datetime.strptime(str(value), "%Y-%m-%d").date() for value in frame["trade_date"]
    )
    return _trade_calendar_cache


@dataclass
class V20CanonicalSelectionState:
    """Resources and cache owned exclusively by one V20 service instance."""

    initialized: bool = False
    realtime_client: Any = None
    fundamentals_db: Any = None
    historical_adapter: Any = None
    concept_mapper: Any = None
    stock_filter: Any = None
    tushare_cache: Any = None
    resource_owner: str | None = None
    resource_init_task: asyncio.Task[None] | None = None
    resource_cleanup_task: asyncio.Task[None] | None = None
    resource_init_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    resource_cleanup_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    canonical_coordinator: "_CanonicalV16Coordinator | None" = None
    canonical_sink: Callable[["CanonicalV16ScanBundle"], Awaitable[None]] | None = None


async def _notify_canonical_error(title: str, detail: str) -> None:
    """Best-effort V20-owned alert for a canonical selection failure."""

    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_alert(f"[V20] {title}", detail)
    except Exception:
        logger.warning("Failed to send V20 canonical error notification", exc_info=True)


async def _initialize_v20_selection_resources_once(
    state: V20CanonicalSelectionState,
    initializer: Callable[[], Awaitable[None]],
) -> None:
    """Run one V20 resource initializer while concurrent callers share its task."""

    async def master() -> None:
        try:
            await initializer()
        except BaseException:
            if not state.initialized:
                state.resource_owner = None
            raise
        state.resource_owner = "V20"

    task: asyncio.Task[None] | None = None
    while task is None:
        async with state.resource_init_lock:
            cleanup = state.resource_cleanup_task
            if cleanup is None or cleanup.done():
                if state.initialized:
                    return
                task = state.resource_init_task
                if task is None or task.done():
                    task = asyncio.create_task(master())
                    state.resource_init_task = task
                    state.resource_owner = "V20"

                    def clear(finished: asyncio.Task[None]) -> None:
                        if state.resource_init_task is finished:
                            state.resource_init_task = None

                    task.add_done_callback(clear)
        if task is None and cleanup is not None:
            await asyncio.shield(cleanup)
    await asyncio.shield(task)


async def _cleanup_v20_selection_resources_once(
    state: V20CanonicalSelectionState,
) -> None:
    errors: list[BaseException] = []
    coordinator = state.canonical_coordinator
    try:
        if coordinator is not None:
            tasks = [
                *coordinator.inflight.values(),
                *coordinator.publish.values(),
                *coordinator.daily_tasks.values(),
            ]
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as exc:
        errors.append(exc)
    finally:
        if coordinator is not None:
            coordinator.pending_persist.clear()
            coordinator.daily_tasks.clear()
            coordinator.daily_bars.clear()
            coordinator.daily_owners.clear()
        state.canonical_coordinator = None
        state.canonical_sink = None

    operations: list[tuple[str, Awaitable[None]]] = []
    if state.realtime_client is not None:
        operations.append(("realtime client", state.realtime_client.stop()))
    if state.fundamentals_db is not None:
        operations.append(("fundamentals DB", state.fundamentals_db.close()))
    for label, operation in operations:
        try:
            await operation
        except Exception as exc:
            logger.error("V20 cleanup: failed to close %s", label, exc_info=True)
            errors.append(exc)

    state.realtime_client = None
    state.historical_adapter = None
    state.concept_mapper = None
    state.stock_filter = None
    state.fundamentals_db = None
    state.initialized = False
    state.resource_owner = None
    if errors:
        raise RuntimeError(
            "V20 selection resource cleanup failed: "
            + "; ".join(f"{type(error).__name__}: {error}" for error in errors)
        )


async def cleanup_v20_selection_resources(
    state: V20CanonicalSelectionState,
) -> None:
    """Close this V20 resource generation without racing a replacement init.

    The cleanup task is registered while holding the same lock that publishes
    initialization masters.  A caller arriving after cleanup begins therefore
    observes the barrier before the old master is cancelled and cannot publish
    a replacement resource set underneath the close operation.
    """

    async with state.resource_cleanup_lock:
        async with state.resource_init_lock:
            current = state.resource_cleanup_task
            if current is not None and not current.done():
                task = current
            else:
                init_task = state.resource_init_task

                async def cleanup_master() -> None:
                    if init_task is not None:
                        if not init_task.done():
                            init_task.cancel()
                        await asyncio.gather(init_task, return_exceptions=True)
                    state.initialized = False
                    state.resource_owner = None
                    await _cleanup_v20_selection_resources_once(state)

                task = asyncio.create_task(cleanup_master())
                state.resource_cleanup_task = task

                def clear(finished: asyncio.Task[None]) -> None:
                    if state.resource_cleanup_task is finished:
                        state.resource_cleanup_task = None

                task.add_done_callback(clear)

    await asyncio.shield(task)


@dataclass
class _CanonicalV16Coordinator:
    """In-memory single-flight coordinator keyed by trade_date only.

    State is bound to the current/most recent trade date and cleared on cleanup.
    Same-day resource replacement or manual wrapper calls reuse the already sealed
    official bundle.
    """

    cache: dict[date, "CanonicalV16ScanBundle"] = field(default_factory=dict)
    inflight: dict[date, asyncio.Task] = field(default_factory=dict)
    pending_persist: dict[date, "CanonicalV16ScanBundle"] = field(default_factory=dict)
    failures: dict[date, str] = field(default_factory=dict)
    daily_bars: dict[date, Mapping[str, TushareDailyBar]] = field(default_factory=dict)
    daily_tasks: dict[date, asyncio.Task[dict[str, TushareDailyBar]]] = field(default_factory=dict)
    daily_owners: dict[date, date] = field(default_factory=dict)
    publish: dict[date, asyncio.Task] = field(default_factory=dict)
    published: set[date] = field(default_factory=set)
    data_errors_sent: set[date] = field(default_factory=set)
    # Stable fatal identity: (trade_date, notify_title, detail_sha256). Different
    # incidents on the same date are not deduplicated against each other.
    fatal_errors_sent: set[tuple[date, str, str]] = field(default_factory=set)
    not_ready_alert_sent: set[date] = field(default_factory=set)
    # Partial 09:39-ready evidence for the current date. Only codes with a valid
    # 09:39 bar on the requested trade date are retained across NOT_READY retries.
    partial: dict[date, dict[str, "TushareEarlyMarketData"]] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(frozen=True)
class CanonicalV16ScanBundle:
    """Frozen, reproducible result of a canonical V16 scan run.

    Contains every intermediate input used by ``run_v16_scan`` so that V16 Top10,
    day-gate scheduling, and the legacy top-1 payload can be produced from the
    same canonical bundle without rerunning scanner.scan or the early market pull.

    Outer mappings are exposed as read-only views and an integrity hash is stored
    so accidental mutation of selection-relevant structures is detected before
    cached reuse or publication.
    """

    trade_date: date
    scan_result: Any
    stock_data: Mapping[str, Any]
    clean_boards: Mapping[str, Any]
    universe: tuple[str, ...]
    quotes: Mapping[str, Any]
    prev_closes: Mapping[str, float]
    history_raw: Mapping[str, Mapping[str, Any]]
    early_bars: Mapping[str, tuple[Any, ...]]
    early_source_hashes: Mapping[str, str]
    failed_no_prev_close: tuple[str, ...]
    failed_no_history: tuple[str, ...]
    failed_build: tuple[str, ...]
    skipped_new_listings: tuple[str, ...]
    model_sha256: str
    feature_list_sha256: str
    computed_at: datetime
    input_hash: str
    _integrity_hash: str
    computation_calendar: tuple[date, ...] = ()
    data_error_notification: tuple[str, str] | None = None
    prior_trade_date: date | None = None
    prior_amount_yuan: Mapping[str, float] = field(default_factory=lambda: MappingProxyType({}))
    breadth_valid_n: int = 0
    breadth_down_n: int = 0
    breadth_market_source_hash: str = ""
    breadth_market_missing_codes: tuple[str, ...] = ()
    breadth_market_conflict_codes: tuple[str, ...] = ()
    history_date_valid_counts: Mapping[str, int] = field(
        default_factory=lambda: MappingProxyType({})
    )
    history_min_date_coverage: float = 0.0


class CachedCanonicalV16Status(Enum):
    AVAILABLE = "available"
    NOT_CACHED = "not_cached"
    IN_FLIGHT = "in_flight"
    PERSISTENCE_PENDING = "persistence_pending"
    FAILED = "failed"
    TRADE_DATE_MISMATCH = "trade_date_mismatch"
    INTEGRITY_INVALID = "integrity_invalid"


@dataclass(frozen=True)
class CachedCanonicalV16Result:
    status: CachedCanonicalV16Status
    bundle: CanonicalV16ScanBundle | None = None
    detail: str | None = None

    @property
    def available(self) -> bool:
        return self.status is CachedCanonicalV16Status.AVAILABLE


class CanonicalV16ScanError(RuntimeError):
    """Structured failure from ``compute_canonical_v16_scan``.

    Carries the original Feishu notification title/detail so the compute done
    callback can emit the alert exactly once, even if every waiter is cancelled.
    It may also carry a separate data-error notification that must be emitted
    before the fatal alert (preserving old ordering).
    """

    def __init__(
        self,
        message: str,
        *,
        notify_title: str | None = None,
        notify_detail: str | None = None,
        data_error_notification: tuple[str, str] | None = None,
    ):
        super().__init__(message)
        self.notify_title = notify_title
        self.notify_detail = notify_detail
        self.data_error_notification = data_error_notification


class CanonicalV16PersistencePendingError(RuntimeError):
    """A verified canonical result is retained while its durable sink retries."""


class CanonicalV16ArtifactProbeError(RuntimeError):
    """Recovering a durable canonical artifact failed and may be retried."""


@dataclass(frozen=True)
class _CanonicalV16NotReadyEvidence:
    trade_date: date
    observed_at: datetime


class CanonicalV16NotReadyError(CanonicalV16ScanError):
    """09:39 early evidence is not yet available for enough returned quotes.

    The ``partial`` field carries the 09:39-ready evidence that has already been
    acquired so a later retry only fetches the unresolved subset.
    """

    def __init__(
        self,
        message: str,
        *,
        partial: dict[str, "TushareEarlyMarketData"] | None = None,
        notify_title: str | None = None,
        notify_detail: str | None = None,
    ):
        super().__init__(message, notify_title=notify_title, notify_detail=notify_detail)
        self.partial = partial


def _ready_codes(
    early_data: Mapping[str, TushareEarlyMarketData],
    trade_date: date,
) -> set[str]:
    """Return codes whose early evidence contains a valid 09:39 bar on ``trade_date``."""
    return {
        code
        for code, emd in early_data.items()
        if any(
            bar.end_label == "09:39" and bar.bar_end.astimezone(BEIJING_TZ).date() == trade_date
            for bar in emd.early_bars
        )
    }


def _evict_stale_dates(coord: _CanonicalV16Coordinator, keep_date: date) -> None:
    """Drop stale completed state while retaining every registered master.

    A newer request must not remove an older date's ``inflight`` registration:
    the task remains live (or at least has not yet run its done callback), so
    removing it would let a later caller start a duplicate computation.  Dates
    can therefore compute concurrently; only completed bookkeeping is bounded to
    the current request plus dates that still have registered masters.
    """
    retained_dates = {
        keep_date,
        *coord.inflight,
        *coord.pending_persist,
        *(
            prior_date
            for prior_date, owner_date in coord.daily_owners.items()
            if owner_date == keep_date
            or owner_date in coord.inflight
            or owner_date in coord.pending_persist
        ),
    }
    for container in (
        coord.cache,
        coord.failures,
        coord.publish,
        coord.partial,
        coord.daily_bars,
        coord.daily_owners,
    ):
        for stale in [d for d in container if d not in retained_dates]:
            container.pop(stale, None)
    coord.published.intersection_update(retained_dates)
    coord.data_errors_sent.intersection_update(retained_dates)
    coord.not_ready_alert_sent.intersection_update(retained_dates)
    coord.fatal_errors_sent = {
        item for item in coord.fatal_errors_sent if item[0] in retained_dates
    }


async def _refresh_top10_names(fdb: Any, recommended: list) -> None:
    """Override the ≤10 final picks' display names with live Tushare names.

    The board/concept JSON is refreshed offline in bulk, so its stock names can
    lag reality — e.g. 600360 摘帽 (de-ST) on 2026-05-20 yet still carried as
    "*ST华微", which then showed up on the 06-29 report even though the stock is
    no longer ST. This affects **display only**: ST *filtering* already queries
    live Tushare (``batch_filter_st``), so trading correctness never depended on
    these names. Only the final picks are looked up, so the extra Tushare call
    is negligible. Best-effort — on any failure the cached names are kept.
    """
    if not fdb or not recommended:
        return
    try:
        cur_names = await fdb.batch_current_names([s.code for s in recommended])
    except Exception:
        logger.warning(
            "Top-10 name refresh from Tushare failed; keeping cached names", exc_info=True
        )
        return
    for s in recommended:
        fresh = cur_names.get(s.code)
        if fresh:
            s.name = fresh


def _build_v16_recommendation_payload(
    scan_result: Any,
    stock_data: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Build the legacy top-1 payload consumed by iQuant, byte-for-byte."""
    recommended = scan_result.recommended
    if not recommended:
        return None
    top1 = recommended[0]
    board = scan_result.stock_best_board.get(top1.code, "")
    return {
        "stock_code": top1.code,
        "stock_name": top1.name,
        "board_name": board,
        "open_price": round(stock_data[top1.code].open_price, 4),
        "prev_close": round(stock_data[top1.code].prev_close, 4),
        "latest_price": round(top1.buy_price, 4),
        "lgb_score": round(top1.score, 6),
        "hot_board_count": scan_result.step2_hot_board_count,
        "final_candidates": scan_result.final_candidates,
    }


LOOKBACK_DAYS = 37  # trading days for historical data


def _validated_computation_calendar(
    calendar: Sequence[date],
    trade_date: date,
) -> tuple[date, ...]:
    """Validate the exact exchange calendar frozen into a canonical master."""

    frozen = tuple(calendar)
    if (
        not frozen
        or any(type(day) is not date for day in frozen)
        or tuple(sorted(set(frozen))) != frozen
        or trade_date not in frozen
    ):
        raise CanonicalV16ScanError(
            "V16 scan: canonical computation calendar is empty or malformed"
        )
    predecessors = [day for day in frozen if day < trade_date]
    successors = [day for day in frozen if day > trade_date]
    if len(predecessors) < LOOKBACK_DAYS or len(successors) < 2:
        raise CanonicalV16ScanError(
            "V16 scan: canonical computation calendar lacks 37 predecessors or D1/D2"
        )
    return frozen


async def _fetch_prev_closes(
    scan_state: V20CanonicalSelectionState,
    today: date,
    calendar: Sequence[date],
    *,
    owner_date: date | None = None,
) -> dict[str, float]:
    """Fetch canonical prev_close inputs used by the V20 pipeline."""
    prev_dates = [d for d in calendar if d < today]
    if not prev_dates:
        raise RuntimeError("V16 scan: no previous trading day found in calendar")
    prev_trade_date = prev_dates[-1].strftime("%Y-%m-%d")

    prev_closes: dict[str, float] = {}

    # Source 1: OSS cache (instant, no API call)
    cache = scan_state.tushare_cache
    if cache and cache.is_ready:
        all_daily = cache.get_all_codes_with_daily(prev_trade_date)
        for code, daily in all_daily.items():
            close_val = daily.get("close")
            if close_val and close_val > 0:
                prev_closes[code] = close_val

    # Source 2: Tushare `daily` API fallback
    if len(prev_closes) < 100:
        rt_client = scan_state.realtime_client
        if rt_client is None:
            raise RuntimeError(
                f"V16 scan: prev_close cache miss for {prev_trade_date} and no "
                f"Tushare client available to fall back to."
            )
        try:
            api_closes = {
                code: row.close_price
                for code, row in (
                    await _fetch_prior_daily_once(
                        scan_state,
                        prev_dates[-1],
                        owner_date=owner_date,
                    )
                ).items()
            }
        except Exception:
            if prev_closes:
                return prev_closes
            raise
        for bare, close_val in api_closes.items():
            if bare and len(bare) == 6 and close_val:
                prev_closes.setdefault(bare, float(close_val))

    if not prev_closes:
        raise RuntimeError(
            f"V16 scan: failed to get prev_close for {prev_trade_date} "
            f"from both OSS cache and Tushare API"
        )
    logger.info(f"V16: prev_close ({prev_trade_date}): {len(prev_closes)} stocks")
    return prev_closes


async def _fetch_prior_daily_once(
    scan_state: V20CanonicalSelectionState,
    prior_trade_date: date,
    *,
    owner_date: date | None = None,
) -> dict[str, TushareDailyBar]:
    """Load one exact D1 daily snapshot, cached across canonical retries."""

    if scan_state.canonical_coordinator is None:
        scan_state.canonical_coordinator = _CanonicalV16Coordinator()
    coord = scan_state.canonical_coordinator
    async with coord.lock:
        cached = coord.daily_bars.get(prior_trade_date)
        if cached is not None:
            return dict(cached)
        task = coord.daily_tasks.get(prior_trade_date)
        coord.daily_owners[prior_trade_date] = (
            owner_date if owner_date is not None else prior_trade_date
        )
        if task is None:

            async def _load() -> dict[str, TushareDailyBar]:
                date_text = prior_trade_date.isoformat()
                cached_rows: dict[str, TushareDailyBar] = {}
                cache = scan_state.tushare_cache
                if cache and cache.is_ready:
                    for code, daily in cache.get_all_codes_with_daily(date_text).items():
                        close = daily.get("close")
                        amount = daily.get("amount")
                        if (
                            isinstance(close, (int, float))
                            and isinstance(amount, (int, float))
                            and not isinstance(close, bool)
                            and not isinstance(amount, bool)
                            and isfinite(float(close))
                            and isfinite(float(amount))
                            and float(close) > 0
                            and float(amount) > 0
                        ):
                            cached_rows[code] = TushareDailyBar(
                                stock_code=code,
                                trade_date=date_text,
                                close_price=float(close),
                                amount_yuan=float(amount),
                            )
                # The one market-wide Tushare ``daily`` response is the
                # authoritative D1 close+amount snapshot.  A populated OSS
                # cache may be partial or from a schema that did not persist
                # amount, so it must never mask a successful API row.  Cache is
                # only a fail-closed fallback when no realtime client exists.
                if scan_state.realtime_client is not None:
                    requested_date = prior_trade_date.strftime("%Y%m%d")
                    api_rows = await scan_state.realtime_client.fetch_daily_bars(requested_date)
                    rows = {
                        code: row
                        for code, row in api_rows.items()
                        if row.stock_code == code
                        and row.trade_date in (date_text, requested_date)
                        and isfinite(float(row.close_price))
                        and float(row.close_price) > 0
                        and isfinite(float(row.amount_yuan))
                        and float(row.amount_yuan) > 0
                    }
                else:
                    rows = cached_rows
                if not rows:
                    raise RuntimeError("V16 scan: D1 daily snapshot is empty")
                return rows

            task = asyncio.create_task(_load())
            coord.daily_tasks[prior_trade_date] = task

            def _remove(finished: asyncio.Task[dict[str, TushareDailyBar]]) -> None:
                coord.daily_tasks.pop(prior_trade_date, None)
                try:
                    coord.daily_bars[prior_trade_date] = MappingProxyType(finished.result())
                except BaseException:
                    return

            task.add_done_callback(_remove)
    return dict(await asyncio.shield(task))


async def _fetch_history_ohlcv(
    hist_adapter: Any,
    codes: list[str],
    ref_date: date,
) -> dict[str, dict]:
    """Fetch 37d OHLCV history via historical adapter. Returns code → raw history dict.

    The returned dict per code has keys: time, open, high, low, close, volume
    """
    if not codes:
        return {}

    calendar_buffer = LOOKBACK_DAYS * 2 + 15
    start = ref_date - timedelta(days=calendar_buffer)
    end = ref_date - timedelta(days=1)

    result: dict[str, dict] = {}
    batch_size = 50

    for i in range(0, len(codes), batch_size):
        batch = codes[i : i + batch_size]
        codes_str = ",".join(f"{c}.SH" if c.startswith("6") else f"{c}.SZ" for c in batch)

        data = await hist_adapter.history_quotes(
            codes=codes_str,
            indicators="open,high,low,close,volume",
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )

        for table_entry in data.get("tables", []):
            thscode = table_entry.get("thscode", "")
            bare_code = thscode.split(".")[0] if thscode else ""
            if not bare_code:
                continue

            tbl = table_entry.get("table", {})
            result[bare_code] = {
                "time": tbl.get("time", []),
                "open": tbl.get("open", []),
                "high": tbl.get("high", []),
                "low": tbl.get("low", []),
                "close": tbl.get("close", []),
                "volume": tbl.get("volume", []),
            }

    return result


def _normalize_history_inputs(
    history_raw: Mapping[str, Mapping[str, Any] | None],
) -> dict[str, dict[str, Any]]:
    fields = ("time", "open", "high", "low", "close", "volume")

    def number(code: str, field: str, value: Any) -> float | None:
        if value is None:
            return None
        # bool must be rejected before float() so True/False are not accepted as 1/0.
        if isinstance(value, bool):
            raise ValueError(f"{code} history input normalization failed: invalid {field} type")
        try:
            result = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{code} history input normalization failed: invalid {field} type"
            ) from exc
        if not isfinite(result):
            raise ValueError(f"{code} history input normalization failed: non-finite {field}")
        return result

    def day(code: str, value: Any) -> str:
        # Date-only strings, naive datetimes/Timestamps and np.datetime64 are interpreted
        # as Asia/Shanghai local dates. Aware datetimes/Timestamps are converted to
        # Shanghai before extracting the date.
        if isinstance(value, str):
            try:
                normalized = pd.Timestamp(value)
            except ValueError as exc:
                raise ValueError(
                    f"{code} history input normalization failed: invalid time {value!r}"
                ) from exc
        elif isinstance(value, pd.Timestamp):
            normalized = value
        elif isinstance(value, np.datetime64):
            normalized = pd.Timestamp(value)
        elif isinstance(value, datetime):
            normalized = pd.Timestamp(value)
        elif isinstance(value, date):
            return value.isoformat()
        else:
            raise ValueError(f"{code} history input normalization failed: invalid time type")
        if pd.isna(normalized):
            raise ValueError(f"{code} history input normalization failed: NaT time")
        if normalized.tzinfo is None:
            normalized = normalized.tz_localize(BEIJING_TZ)
        else:
            normalized = normalized.tz_convert(BEIJING_TZ)
        return normalized.strftime("%Y-%m-%d")

    normalized: dict[str, dict[str, Any]] = {}
    for code in sorted(history_raw):
        hist = history_raw[code]
        if hist is None:
            normalized[code] = {field: [] for field in fields}
            continue
        arrays = {field: list(hist.get(field, [])) for field in fields}
        lengths = {len(values) for values in arrays.values()}
        if len(lengths) != 1:
            raise ValueError(f"{code} history input normalization failed: mismatched lengths")
        normalized[code] = {
            "time": [day(code, value) for value in arrays["time"]],
            **{
                field: [number(code, field, value) for value in arrays[field]]
                for field in fields[1:]
            },
        }
    return normalized


def _valid_history_dates(history: Mapping[str, Any]) -> frozenset[str]:
    """Return dates backed by exactly one legal OHLCV row for this symbol."""

    fields = ("time", "open", "high", "low", "close", "volume")
    arrays = {field: list(history.get(field, [])) for field in fields}
    if len({len(values) for values in arrays.values()}) != 1:
        return frozenset()
    validity: dict[str, bool] = {}
    for index, raw_day in enumerate(arrays["time"]):
        day = str(raw_day)
        if day in validity:
            validity[day] = False
            continue
        try:
            o, h, low, close, volume = (
                float(arrays[field][index]) for field in ("open", "high", "low", "close", "volume")
            )
            legal = (
                all(isfinite(value) for value in (o, h, low, close, volume))
                and min(o, h, low, close, volume) > 0
                and low <= min(o, close)
                and h >= max(o, close)
                and low <= h
            )
        except (TypeError, ValueError, OverflowError):
            legal = False
        validity[day] = legal
    return frozenset(day for day, legal in validity.items() if legal)


def _build_stock_data(
    code: str,
    name: str,
    quote: Any,
    prev_close: float,
    hist_raw: dict,
    ref_date: date,
) -> Any:
    """Build V16StockData from raw data. Returns None if data is insufficient.

    Raises RuntimeError for old stocks with missing data.
    """
    from src.strategy.strategies.v16_scanner import V16StockData

    time_vals = hist_raw.get("time", [])
    close_vals = hist_raw.get("close", [])
    open_vals = hist_raw.get("open", [])
    high_vals = hist_raw.get("high", [])
    low_vals = hist_raw.get("low", [])
    vol_vals = hist_raw.get("volume", [])

    # Build valid OHLCV rows
    rows = []

    def _history_date(value: Any) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return datetime.strptime(str(value), "%Y-%m-%d").date()

    for idx in range(len(time_vals)):
        try:
            o = float(open_vals[idx]) if open_vals[idx] is not None else None
            h = float(high_vals[idx]) if high_vals[idx] is not None else None
            lo = float(low_vals[idx]) if low_vals[idx] is not None else None
            c = float(close_vals[idx]) if close_vals[idx] is not None else None
            v = float(vol_vals[idx]) if vol_vals[idx] is not None else None
        except (ValueError, IndexError):
            continue
        if o is None or h is None or lo is None or c is None or v is None:
            continue
        if o <= 0 or c <= 0:
            continue
        rows.append({"open": o, "high": h, "low": lo, "close": c, "volume": v})

    if len(rows) < LOOKBACK_DAYS:
        # Check if new listing (first date < 37 trading days ago)
        if time_vals:
            first_date = _history_date(time_vals[0])
            if (ref_date - first_date).days < 60:  # ~37 trading days ≈ 55 calendar days
                logger.info(
                    f"V16: skipping new listing {code} ({name}): "
                    f"only {len(rows)} history days (IPO: {time_vals[0]})"
                )
                return None
        # Old stock with insufficient data — hard error, never silently skip
        if len(rows) < 5:
            raise RuntimeError(
                f"V16: {code} ({name}) has only {len(rows)} history rows "
                f"(need ≥5 for LGBRank). Old stock with missing data — halting."
            )

    hist_df = pd.DataFrame(rows)

    # Compute derived metrics from history
    closes = np.array([r["close"] for r in rows])
    volumes = np.array([r["volume"] for r in rows])

    # avg_daily_volume (37d)
    recent_vol = volumes[-LOOKBACK_DAYS:]
    avg_daily_volume = float(recent_vol.mean()) if len(recent_vol) > 0 else 0.0

    # trend_5d
    trend_5d = 0.0
    if len(closes) >= 6:
        c_now, c_5ago = closes[-1], closes[-6]
        if c_5ago > 0:
            trend_5d = (c_now - c_5ago) / c_5ago

    # trend_10d
    trend_10d = 0.0
    if len(closes) >= 11:
        c_now, c_10ago = closes[-1], closes[-11]
        if c_10ago > 0:
            trend_10d = (c_now - c_10ago) / c_10ago

    # avg_daily_return_20d and volatility_20d
    avg_daily_return_20d = 0.0
    volatility_20d = 0.0
    if len(closes) >= 2:
        returns = np.diff(closes) / closes[:-1]
        recent_returns = returns[-20:] if len(returns) >= 20 else returns
        avg_daily_return_20d = float(np.mean(recent_returns))
        if len(recent_returns) >= 2:
            volatility_20d = float(np.std(recent_returns))

    # consecutive_up_days
    consecutive_up_days = 0
    for j in range(len(closes) - 1, 0, -1):
        if closes[j] > closes[j - 1]:
            consecutive_up_days += 1
        else:
            break

    return V16StockData(
        code=code,
        name=name,
        open_price=quote.open_price,
        prev_close=prev_close,
        price_940=quote.early_close,
        high_940=quote.early_high,
        low_940=quote.early_low,
        volume_940=quote.early_volume,
        volume_937=quote.volume_937,
        avg_daily_volume=avg_daily_volume,
        trend_5d=trend_5d,
        trend_10d=trend_10d,
        avg_daily_return_20d=avg_daily_return_20d,
        volatility_20d=volatility_20d,
        consecutive_up_days=consecutive_up_days,
        history_df=hist_df,
    )


def _canonical_key(scan_state: V20CanonicalSelectionState, trade_date: date) -> date:
    """Identity key for single-flight deduplication.

    Keyed by trade_date only so same-day resource replacement reuses the sealed
    official bundle.
    """
    return trade_date


def _canonical_json_value(value: Any) -> Any:
    """Deterministic JSON-serializable form for hash/fingerprint inputs.

    Handles numpy/pandas scalar types, ``Timestamp``/``datetime``/``date``,
    ``DataFrame`` rows, lists, and dicts. Rejects non-finite floats and ``NaN``/``Inf``.
    Naive datetimes/Timestamps are localized to Asia/Shanghai; aware values are
    converted to Asia/Shanghai. ``pd.NaT`` and ``np.datetime64('NaT')`` are rejected.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if not isfinite(value):
            raise ValueError(f"non-finite float cannot be canonicalized: {value!r}")
        return float(value)
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if value is pd.NaT:
        raise ValueError("cannot canonicalize NaT")
    # pd.Timestamp is a datetime subclass; check it first so naive Timestamps are
    # explicitly localized to Asia/Shanghai instead of relying on system local time.
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            raise ValueError("cannot canonicalize NaT")
        if value.tzinfo is None:
            return value.tz_localize(BEIJING_TZ).isoformat()
        return value.tz_convert(BEIJING_TZ).isoformat()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=BEIJING_TZ).isoformat()
        return value.astimezone(BEIJING_TZ).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, np.datetime64):
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            raise ValueError("cannot canonicalize NaT")
        return ts.tz_localize(BEIJING_TZ).isoformat()
    if isinstance(value, np.generic):
        return _canonical_json_value(value.item())
    if isinstance(value, pd.DataFrame):
        try:
            rows = value.to_dict(orient="records")
        except Exception as exc:
            raise ValueError(f"cannot canonicalize DataFrame: {exc}") from exc
        return [_canonical_json_value(row) for row in rows]
    if isinstance(value, (list, tuple)):
        return [_canonical_json_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _canonical_json_value(v) for k, v in value.items()}
    raise ValueError(f"cannot canonicalize value of type {type(value).__name__}")


def _stable_input_hash(
    trade_date: date,
    universe: tuple[str, ...],
    clean_boards: Mapping[str, Any],
    model_sha256: str,
    feature_list_sha256: str,
    early_source_hashes: Mapping[str, str],
    prev_closes: Mapping[str, float],
    history_raw: Mapping[str, dict | None],
    stock_data: Mapping[str, Any],
    failed_no_prev_close: tuple[str, ...],
    failed_no_history: tuple[str, ...],
    failed_build: tuple[str, ...],
    skipped_new_listings: tuple[str, ...],
    recommended_names: Mapping[str, str],
    st_eligible_codes: Sequence[str],
) -> str:
    """Deterministic hash over every input that can affect selection/output.

    Mapping insertion order and response completion order must not affect the hash.
    ``history_raw`` values may be ``None``; they are normalized to empty history.
    ``stock_data`` is encoded with its derived scalars and history rows.
    """

    def _normalize_history(hist: dict | None) -> dict[str, Any]:
        if hist is None:
            return {"time": [], "open": [], "high": [], "low": [], "close": [], "volume": []}
        return {
            "time": list(hist.get("time", [])),
            "open": list(hist.get("open", [])),
            "high": list(hist.get("high", [])),
            "low": list(hist.get("low", [])),
            "close": list(hist.get("close", [])),
            "volume": list(hist.get("volume", [])),
        }

    def _normalize_stock_data(sd: Any) -> dict[str, Any]:
        hist_rows: list[dict[str, Any]] = []
        hist_df = getattr(sd, "history_df", None)
        if hist_df is not None:
            try:
                hist_rows = hist_df.to_dict(orient="records")
            except Exception as exc:
                raise ValueError(f"cannot encode history_df for {sd.code}: {exc}") from exc
        return {
            "code": sd.code,
            "name": sd.name,
            "open_price": sd.open_price,
            "prev_close": sd.prev_close,
            "price_940": sd.price_940,
            "high_940": sd.high_940,
            "low_940": sd.low_940,
            "volume_940": sd.volume_940,
            "volume_937": sd.volume_937,
            "avg_daily_volume": sd.avg_daily_volume,
            "trend_5d": sd.trend_5d,
            "trend_10d": sd.trend_10d,
            "avg_daily_return_20d": sd.avg_daily_return_20d,
            "volatility_20d": sd.volatility_20d,
            "consecutive_up_days": sd.consecutive_up_days,
            "history_df": hist_rows,
        }

    payload = {
        "trade_date": trade_date.isoformat(),
        "universe": list(universe),
        "clean_boards": {
            board: [list(member) for member in codes]
            for board, codes in sorted(clean_boards.items())
        },
        "model_sha256": model_sha256,
        "feature_list_sha256": feature_list_sha256,
        "early_source_hashes": {code: h for code, h in sorted(early_source_hashes.items())},
        "prev_closes": {code: pc for code, pc in sorted(prev_closes.items()) if code in universe},
        "history_raw": {
            code: _normalize_history(hist) for code, hist in sorted(history_raw.items())
        },
        "stock_data": {code: _normalize_stock_data(sd) for code, sd in sorted(stock_data.items())},
        "failed_no_prev_close": sorted(failed_no_prev_close),
        "failed_no_history": sorted(failed_no_history),
        "failed_build": sorted(failed_build),
        "skipped_new_listings": sorted(skipped_new_listings),
        "recommended_names": {code: name for code, name in sorted(recommended_names.items())},
    }
    payload["st_eligible_codes"] = sorted(st_eligible_codes)
    canonical = json.dumps(
        _canonical_json_value(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _bundle_fingerprint(bundle: CanonicalV16ScanBundle) -> str:
    """Integrity fingerprint over selection-relevant bundle contents.

    Uses an explicit V16ScanResult field schema (no ``dir()`` or ``str`` fallback).
    Covers stock_data scalars + history_df, the full scan_result, canonical clean
    boards, early evidence (including bar stock_code and bar_end), raw history,
    failure sets, and model/feature hashes. Mutation of any selection-relevant
    structure is therefore detected before cached reuse or publication.
    """

    def _quote_norm(quote: Any) -> dict[str, Any]:
        return {
            "open_price": float(quote.open_price),
            "latest_price": float(quote.latest_price),
            "high_price": float(quote.high_price),
            "low_price": float(quote.low_price),
            "volume": float(quote.volume),
            "amount": float(quote.amount),
            "early_close": float(quote.early_close),
            "early_high": float(quote.early_high),
            "early_low": float(quote.early_low),
            "early_volume": float(quote.early_volume),
            "volume_937": float(quote.volume_937),
        }

    def _bar_norm(bar: Any) -> dict[str, Any]:
        return {
            "code": bar.stock_code,
            "end": bar.bar_end.astimezone(BEIJING_TZ).replace(microsecond=0).isoformat(),
            "end_label": bar.end_label,
            "open_price": float(bar.open_price),
            "close_price": float(bar.close_price),
            "high_price": float(bar.high_price),
            "low_price": float(bar.low_price),
            "volume": float(bar.volume),
            "amount": float(bar.amount),
        }

    def _stock_data_norm(sd: Any) -> dict[str, Any]:
        hist_rows: list[dict[str, Any]] = []
        hist_df = getattr(sd, "history_df", None)
        if hist_df is not None:
            try:
                hist_rows = hist_df.to_dict(orient="records")
            except Exception as exc:
                raise ValueError(f"cannot encode history_df for {sd.code}: {exc}") from exc
        return {
            "code": sd.code,
            "name": sd.name,
            "open_price": float(sd.open_price),
            "prev_close": float(sd.prev_close),
            "price_940": float(sd.price_940),
            "high_940": float(sd.high_940),
            "low_940": float(sd.low_940),
            "volume_940": float(sd.volume_940),
            "volume_937": float(sd.volume_937),
            "avg_daily_volume": float(sd.avg_daily_volume),
            "trend_5d": float(sd.trend_5d),
            "trend_10d": float(sd.trend_10d),
            "avg_daily_return_20d": float(sd.avg_daily_return_20d),
            "volatility_20d": float(sd.volatility_20d),
            "consecutive_up_days": int(sd.consecutive_up_days),
            "history_df": hist_rows,
        }

    def _scan_result_norm(scan_result: Any) -> dict[str, Any]:
        def _scored_norm(stocks: list[Any]) -> list[dict[str, Any]]:
            return [
                {
                    "code": s.code,
                    "name": getattr(s, "name", None),
                    "buy_price": float(s.buy_price),
                    "score": float(s.score),
                    "rank": getattr(s, "rank", None),
                }
                for s in stocks
            ]

        primitive_fields = (
            "step0_universe_count",
            "step2_hot_board_count",
            "step2_filtered_by_avg_gain",
            "step3_count",
            "step4_count",
            "step5_count",
            "step6_count",
            "step6_5_count",
            "step6_6_count",
            "final_candidates",
        )
        code_list_fields = (
            "st_eligible_codes",
            "step0_codes",
            "step2_codes",
            "step3_codes",
            "step4_codes",
            "step5_codes",
            "step6_codes",
            "step6_5_codes",
            "step6_6_codes",
        )
        dict_fields = (
            "step2_board_avg_gains",
            "stock_best_board",
            "stock_all_boards",
            "stock_gain_from_open",
            "stock_is_driver",
            "stock_cci",
            "stock_early_vol",
            "step2_all_board_avg_gains",
        )

        normalized: dict[str, Any] = {}
        for attr in primitive_fields:
            normalized[attr] = getattr(scan_result, attr, 0)
        for attr in code_list_fields:
            normalized[attr] = list(getattr(scan_result, attr, []))
        normalized["recommended"] = _scored_norm(getattr(scan_result, "recommended", []))
        normalized["all_scored"] = _scored_norm(getattr(scan_result, "all_scored", []))
        normalized["step2_boards_detail"] = {
            board: sorted(codes)
            for board, codes in sorted(getattr(scan_result, "step2_boards_detail", {}).items())
        }
        for attr in dict_fields:
            value = getattr(scan_result, attr, {})
            normalized[attr] = {k: v for k, v in sorted(value.items())}
        return normalized

    payload: dict[str, Any] = {
        "trade_date": bundle.trade_date.isoformat(),
        "universe": list(bundle.universe),
        "clean_boards": {
            board: [list(member) for member in codes]
            for board, codes in sorted(bundle.clean_boards.items())
        },
        "quotes": {code: _quote_norm(q) for code, q in sorted(bundle.quotes.items())},
        "prev_closes": {
            code: float(pc)
            for code, pc in sorted(bundle.prev_closes.items())
            if code in bundle.universe
        },
        "history_raw": {
            code: {
                "time": list(hist.get("time", [])),
                "open": list(hist.get("open", [])),
                "high": list(hist.get("high", [])),
                "low": list(hist.get("low", [])),
                "close": list(hist.get("close", [])),
                "volume": list(hist.get("volume", [])),
            }
            for code, hist in sorted(bundle.history_raw.items())
        },
        "early_bars": {
            code: [_bar_norm(bar) for bar in bars]
            for code, bars in sorted(bundle.early_bars.items())
        },
        "early_source_hashes": {code: h for code, h in sorted(bundle.early_source_hashes.items())},
        "stock_data": {
            code: _stock_data_norm(sd) for code, sd in sorted(bundle.stock_data.items())
        },
        "scan_result": _scan_result_norm(bundle.scan_result),
        "failed_no_prev_close": sorted(bundle.failed_no_prev_close),
        "failed_no_history": sorted(bundle.failed_no_history),
        "failed_build": sorted(bundle.failed_build),
        "skipped_new_listings": sorted(bundle.skipped_new_listings),
        "model_sha256": bundle.model_sha256,
        "feature_list_sha256": bundle.feature_list_sha256,
        "input_hash": bundle.input_hash,
        "computation_calendar": [day.isoformat() for day in bundle.computation_calendar],
        "data_error_notification": bundle.data_error_notification,
        "prior_trade_date": (
            bundle.prior_trade_date.isoformat() if bundle.prior_trade_date is not None else None
        ),
        "prior_amount_yuan": {
            code: float(amount) for code, amount in sorted(bundle.prior_amount_yuan.items())
        },
        "breadth_valid_n": bundle.breadth_valid_n,
        "breadth_down_n": bundle.breadth_down_n,
        "breadth_market_source_hash": bundle.breadth_market_source_hash,
        "breadth_market_missing_codes": list(bundle.breadth_market_missing_codes),
        "breadth_market_conflict_codes": list(bundle.breadth_market_conflict_codes),
        "history_date_valid_counts": {
            day: count for day, count in sorted(bundle.history_date_valid_counts.items())
        },
        "history_min_date_coverage": bundle.history_min_date_coverage,
    }
    canonical = json.dumps(
        _canonical_json_value(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _verify_bundle_integrity(bundle: CanonicalV16ScanBundle) -> None:
    """Raise if selection-relevant bundle contents have been mutated."""
    expected = bundle._integrity_hash
    actual = _bundle_fingerprint(bundle)
    if actual != expected:
        raise RuntimeError(
            f"CanonicalV16ScanBundle integrity check failed (expected {expected}, got {actual})"
        )


def _send_fatal_once(
    coord: _CanonicalV16Coordinator,
    trade_date: date,
    error: CanonicalV16ScanError,
) -> None:
    """Schedule one fatal notification per stable incident identity.

    Different incidents on the same date have different identities and are not
    deduplicated against each other. The notification is scheduled as a separate
    task so a waiter cancellation cannot cancel it.
    """
    if not error.notify_title:
        return
    detail = error.notify_detail or ""
    detail_hash = hashlib.sha256(detail.encode("utf-8")).hexdigest()
    identity = (trade_date, error.notify_title, detail_hash)
    if identity in coord.fatal_errors_sent:
        return
    coord.fatal_errors_sent.add(identity)
    # Emit any required data-error alert first, preserving original order/text.
    if error.data_error_notification and trade_date not in coord.data_errors_sent:
        data_title, data_detail = error.data_error_notification
        asyncio.create_task(_notify_canonical_error(data_title, data_detail))
        coord.data_errors_sent.add(trade_date)
    asyncio.create_task(_notify_canonical_error(error.notify_title, detail))


def _isolate_bundle(bundle: CanonicalV16ScanBundle) -> CanonicalV16ScanBundle:
    """Return a deep-copied consumer artifact; the master bundle stays untouched.

    Outer mappings remain read-only and inner mutable objects (DataFrames,
    V16StockData, scan_result namespace) are deep-copied so that consumer mutation
    cannot affect the cached master or other consumers.
    """
    from dataclasses import replace

    return replace(
        bundle,
        scan_result=copy.deepcopy(bundle.scan_result),
        stock_data=MappingProxyType(copy.deepcopy(dict(bundle.stock_data))),
        clean_boards=MappingProxyType({k: tuple(v) for k, v in bundle.clean_boards.items()}),
        quotes=MappingProxyType(copy.deepcopy(dict(bundle.quotes))),
        prev_closes=MappingProxyType(copy.deepcopy(dict(bundle.prev_closes))),
        history_raw=MappingProxyType(
            {
                code: MappingProxyType(copy.deepcopy(dict(hist)))
                for code, hist in bundle.history_raw.items()
            }
        ),
        early_bars=MappingProxyType(copy.deepcopy(dict(bundle.early_bars))),
        early_source_hashes=MappingProxyType(copy.deepcopy(dict(bundle.early_source_hashes))),
        prior_amount_yuan=MappingProxyType(dict(bundle.prior_amount_yuan)),
        history_date_valid_counts=MappingProxyType(dict(bundle.history_date_valid_counts)),
    )


async def get_cached_canonical_v16(
    scan_state: V20CanonicalSelectionState,
    trade_date: date | None = None,
) -> CachedCanonicalV16Result:
    """Read the sealed canonical V16 master without computing or fetching.

    This accessor is cached-only: it never creates the coordinator, starts or
    joins an in-flight task, invokes the scanner, or contacts a vendor.  It is
    an ``async`` API so inspection shares the coordinator's ``asyncio.Lock``
    with scheduler-side state transitions.  As with the rest of V20CanonicalSelectionState,
    the coordinator is intended for the event loop that owns it and is not a
    cross-thread primitive.
    """
    if trade_date is None:
        trade_date = datetime.now(BEIJING_TZ).date()

    coord = scan_state.canonical_coordinator
    if coord is None:
        return CachedCanonicalV16Result(CachedCanonicalV16Status.NOT_CACHED)

    async with coord.lock:
        if coord.inflight.get(trade_date) is not None:
            if trade_date in coord.pending_persist:
                return CachedCanonicalV16Result(CachedCanonicalV16Status.PERSISTENCE_PENDING)
            return CachedCanonicalV16Result(CachedCanonicalV16Status.IN_FLIGHT)

        if trade_date in coord.pending_persist:
            return CachedCanonicalV16Result(CachedCanonicalV16Status.PERSISTENCE_PENDING)

        master = coord.cache.get(trade_date)
        if master is None:
            detail = coord.failures.get(trade_date)
            if detail is not None:
                return CachedCanonicalV16Result(
                    CachedCanonicalV16Status.FAILED,
                    detail=detail,
                )
            return CachedCanonicalV16Result(CachedCanonicalV16Status.NOT_CACHED)

        if master.trade_date != trade_date:
            return CachedCanonicalV16Result(CachedCanonicalV16Status.TRADE_DATE_MISMATCH)

        try:
            _verify_bundle_integrity(master)
            bundle = _isolate_bundle(master)
        except Exception as exc:
            return CachedCanonicalV16Result(
                CachedCanonicalV16Status.INTEGRITY_INVALID,
                detail=f"{type(exc).__name__}: {exc}",
            )
        return CachedCanonicalV16Result(
            CachedCanonicalV16Status.AVAILABLE,
            bundle=bundle,
        )


async def get_or_compute_canonical_v16(
    scan_state: V20CanonicalSelectionState,
    trade_date: date | None = None,
    *,
    early_data_seed: Mapping[str, TushareEarlyMarketData] | None = None,
    allow_realtime_fetch: bool = True,
    universe_override: tuple[str, ...] | None = None,
    clean_boards_override: Mapping[str, Sequence[tuple[str, str]]] | None = None,
    prev_closes_override: Mapping[str, float] | None = None,
    history_raw_override: Mapping[str, Mapping[str, Any] | None] | None = None,
    names_override: Mapping[str, str] | None = None,
    calendar_override: tuple[date, ...] | None = None,
    prior_daily_override: Mapping[str, TushareDailyBar] | None = None,
    st_eligible_codes_override: Sequence[str] | None = None,
) -> CanonicalV16ScanBundle:
    """Return an isolated copy of the canonical V16 bundle for ``trade_date``.

    Concurrent callers for the same key share exactly one ``compute_canonical_v16_scan``
    invocation (one early pull, one ``scanner.scan``). Successful results are frozen
    in the cache as a master bundle that is never exposed directly; each consumer
    receives its own deep-copied artifact. Failures are not cached and may be retried.
    Cancelling a waiting caller does not cancel the shared in-flight task, and a fatal
    alert is still sent from the compute done callback even if all waiters are gone.

    ``early_data_seed`` supplies pre-acquired early evidence for a cold computation
    (e.g. a historical replay hydrated from persisted raw bars); it is merged under
    any retained partial evidence. ``allow_realtime_fetch=False`` forbids the
    unresolved-codes realtime pull, leaving the 09:39 readiness gate authoritative
    over the seeded coverage. ``universe_override``/``clean_boards_override`` pin the
    exact universe the seed was gathered for. A cached master bundle or an in-flight
    task always wins — these arguments only shape a fresh computation.
    """
    if trade_date is None:
        trade_date = datetime.now(BEIJING_TZ).date()

    if scan_state.canonical_coordinator is None:
        scan_state.canonical_coordinator = _CanonicalV16Coordinator()
    coord = scan_state.canonical_coordinator
    key = _canonical_key(scan_state, trade_date)
    created_task = False

    async with coord.lock:
        _evict_stale_dates(coord, key)

        master = coord.cache.get(key)
        if master is not None:
            _verify_bundle_integrity(master)
            return _isolate_bundle(master)

        task = coord.inflight.get(key)
        if task is None:
            pending = coord.pending_persist.get(key)
            partial = coord.partial.get(key, {})
            coord.failures.pop(key, None)

            async def _runner() -> CanonicalV16ScanBundle:
                if pending is not None:
                    bundle = pending
                else:
                    bundle = await compute_canonical_v16_scan(
                        scan_state,
                        trade_date,
                        partial=partial,
                        universe_override=universe_override,
                        clean_boards_override=clean_boards_override,
                        prev_closes_override=prev_closes_override,
                        history_raw_override=history_raw_override,
                        names_override=names_override,
                        calendar_override=calendar_override,
                        prior_daily_override=prior_daily_override,
                        st_eligible_codes_override=st_eligible_codes_override,
                        early_data_seed=early_data_seed,
                        allow_realtime_fetch=allow_realtime_fetch,
                    )
                _verify_bundle_integrity(bundle)
                sink = scan_state.canonical_sink
                if sink is None:
                    return bundle

                coord.pending_persist[key] = bundle
                durable_input = _isolate_bundle(bundle)
                try:
                    await sink(durable_input)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    raise CanonicalV16PersistencePendingError(
                        f"canonical V16 durable persistence is pending: {exc}"
                    ) from exc
                try:
                    _verify_bundle_integrity(durable_input)
                    _verify_bundle_integrity(bundle)
                except Exception as exc:
                    raise CanonicalV16PersistencePendingError(
                        f"canonical V16 durable persistence output failed verification: {exc}"
                    ) from exc
                return bundle

            task = asyncio.create_task(_runner())
            coord.inflight[key] = task
            created_task = True

    if created_task:

        def _finalize(t: asyncio.Task) -> None:
            coord.inflight.pop(key, None)
            try:
                result = t.result()
                coord.cache[key] = result
                coord.pending_persist.pop(key, None)
                coord.partial.pop(key, None)
                coord.failures.pop(key, None)
            except asyncio.CancelledError:
                # CancelledError is a BaseException; it must not escape the callback.
                coord.failures[key] = "CanonicalV16 computation was cancelled"
            except CanonicalV16NotReadyError as e:
                # Preserve 09:39-ready partial evidence so retries only fetch
                # the unresolved subset.
                if e.partial:
                    coord.partial[key] = e.partial
                coord.failures[key] = f"CanonicalV16NotReadyError: {e}"
            except CanonicalV16PersistencePendingError:
                # The verified master remains in pending_persist for a sink-only retry.
                coord.failures.pop(key, None)
            except CanonicalV16ScanError as e:
                # Fatal alerts are emitted here so they survive waiter cancellation.
                _send_fatal_once(coord, key, e)
                reason = e.__cause__ or e
                coord.failures[key] = f"{type(reason).__name__}: {reason}"
            except BaseException as e:
                # Any other failure is left for the scheduler/consumer to handle.
                coord.failures[key] = f"{type(e).__name__}: {e}"

        task.add_done_callback(_finalize)

    result = await asyncio.shield(task)
    # The master bundle returned by the shared task is never handed to consumers.
    return _isolate_bundle(result)


def derive_canonical_v16_universe(
    scan_state: V20CanonicalSelectionState,
    *,
    universe_override: tuple[str, ...] | None = None,
    clean_boards_override: Mapping[str, Sequence[tuple[str, str]]] | None = None,
) -> tuple[Any, Any, Mapping[str, tuple[tuple[str, str], ...]], tuple[str, ...]]:
    """Construct the canonical V16 scanner and derive its deterministic universe.

    This is the single construction path used by normal scans and replay. It always
    creates the scorer and scanner from the same model paths and scan-state
    dependencies before obtaining or normalizing the universe.
    """
    from src.strategy.lgbrank_scorer import LGBRankScorer as DefaultLGBRankScorer
    from src.strategy.strategies.v16_scanner import V16Scanner as DefaultV16Scanner

    scorer = DefaultLGBRankScorer(
        _PROJECT_ROOT / "models" / "lgbrank_latest.txt",
        _PROJECT_ROOT / "models" / "feature_list.json",
    )
    scanner = DefaultV16Scanner(
        fundamentals_db=scan_state.fundamentals_db,
        concept_mapper=scan_state.concept_mapper,
        stock_filter=scan_state.stock_filter,
        scorer=scorer,
    )
    clean_boards_raw, universe_codes = (
        (dict(clean_boards_override), set(universe_override))
        if universe_override is not None and clean_boards_override is not None
        else scanner.get_universe()
    )
    if not universe_codes:
        raise RuntimeError("V16 scan: universe is empty after board cleaning")

    universe = tuple(sorted(universe_codes))
    clean_boards_for_scan = {
        board: sorted(codes) for board, codes in sorted(clean_boards_raw.items())
    }
    clean_boards = MappingProxyType(
        {board: tuple(codes) for board, codes in clean_boards_for_scan.items()}
    )
    return scanner, scorer, clean_boards, universe


async def compute_canonical_v16_scan(
    scan_state: V20CanonicalSelectionState,
    trade_date: date,
    partial: Mapping[str, TushareEarlyMarketData] | None = None,
    *,
    universe_override: tuple[str, ...] | None = None,
    clean_boards_override: Mapping[str, Sequence[tuple[str, str]]] | None = None,
    prev_closes_override: Mapping[str, float] | None = None,
    history_raw_override: Mapping[str, Mapping[str, Any] | None] | None = None,
    names_override: Mapping[str, str] | None = None,
    calendar_override: tuple[date, ...] | None = None,
    prior_daily_override: Mapping[str, TushareDailyBar] | None = None,
    st_eligible_codes_override: Sequence[str] | None = None,
    early_data_seed: Mapping[str, TushareEarlyMarketData] | None = None,
    allow_realtime_fetch: bool = True,
) -> CanonicalV16ScanBundle:
    """Canonical V16 scan computation; does not send V16 Top10 or day-gate messages.

    The order of operations, filters, and thresholds are copied exactly from the
    original ``run_v16_scan`` implementation. Only codes with a valid 09:39 bar on
    ``trade_date`` are admitted into ``quotes``, ``trading_codes``, ``stock_data``
    and ``scanner.scan``. Partial ready evidence can be supplied so retries only
    fetch the unresolved subset.

    ``early_data_seed`` is pre-acquired early evidence (e.g. hydrated from
    persisted raw bars for a historical replay); it is merged under any retained
    partial evidence. ``allow_realtime_fetch=False`` forbids the unresolved-codes
    realtime pull entirely, leaving the 09:39 readiness gate authoritative over
    the seeded coverage.
    """
    from dataclasses import replace

    validated_calendar_override = (
        _validated_computation_calendar(calendar_override, trade_date)
        if calendar_override is not None
        else None
    )
    if not allow_realtime_fetch:
        replay_inputs = (
            universe_override,
            clean_boards_override,
            prev_closes_override,
            history_raw_override,
            names_override,
            calendar_override,
            prior_daily_override,
            st_eligible_codes_override,
            early_data_seed,
        )
        if any(value is None for value in replay_inputs):
            raise CanonicalV16ScanError(
                "V16 scan: vendor-free replay requires every frozen input override"
            )

    scanner, scorer, clean_boards, universe_list = derive_canonical_v16_universe(
        scan_state,
        universe_override=universe_override,
        clean_boards_override=clean_boards_override,
    )
    logger.info(f"V16 scan: universe = {len(universe_list)} stocks")
    clean_boards_for_scan = {board: list(codes) for board, codes in clean_boards.items()}
    if st_eligible_codes_override is not None:
        eligible = frozenset(st_eligible_codes_override)
        if any(code not in universe_list for code in eligible):
            raise CanonicalV16ScanError("V16 scan: frozen ST eligibility has unknown codes")

        class _FrozenStEligibility:
            async def batch_filter_st(self, codes: list[str]) -> list[str]:
                return sorted(code for code in codes if code in eligible)

        scanner._fdb = _FrozenStEligibility()

    calendar_source = (
        validated_calendar_override
        if validated_calendar_override is not None
        else tuple(await get_v20_trade_calendar())
    )
    calendar = _validated_computation_calendar(calendar_source, trade_date)
    previous_dates = [day for day in calendar if day < trade_date]
    if not previous_dates:
        raise RuntimeError("V16 scan: no previous trading day found in calendar")
    following_dates = [day for day in calendar if day > trade_date]
    if len(following_dates) < 2:
        raise RuntimeError("V16 scan: canonical calendar lacks D1 and D2 evidence")
    prior_trade_date = previous_dates[-1]
    prior_daily = (
        dict(prior_daily_override)
        if prior_daily_override is not None
        else await _fetch_prior_daily_once(
            scan_state,
            prior_trade_date,
            owner_date=trade_date,
        )
    )
    requested_prior_dates = {prior_trade_date.isoformat(), prior_trade_date.strftime("%Y%m%d")}
    if not prior_daily:
        raise CanonicalV16ScanError("V16 scan: D1 daily snapshot is empty")
    for code, row in prior_daily.items():
        if (
            not isinstance(code, str)
            or len(code) != 6
            or not code.isdigit()
            or row.stock_code != code
            or row.trade_date not in requested_prior_dates
            or isinstance(row.close_price, bool)
            or not isinstance(row.close_price, (int, float))
            or not isfinite(float(row.close_price))
            or float(row.close_price) <= 0
            or isinstance(row.amount_yuan, bool)
            or not isinstance(row.amount_yuan, (int, float))
            or not isfinite(float(row.amount_yuan))
            or float(row.amount_yuan) <= 0
        ):
            raise CanonicalV16ScanError("V16 scan: D1 daily snapshot contains an invalid row")

    # A successful market-wide ``daily`` response is the live source of truth
    # for both D1 close and amount.  Deriving closes from that same response is
    # what keeps the 00/60 breadth union complete and prevents an older/partial
    # OSS cache from silently shrinking it.  Vendor-free replay supplies both
    # frozen projections and the equality check above binds them together.
    prev_closes = (
        dict(prev_closes_override)
        if prev_closes_override is not None
        else {code: float(row.close_price) for code, row in prior_daily.items()}
    )
    for code, previous_close in prev_closes.items():
        if (
            not isinstance(code, str)
            or len(code) != 6
            or not code.isdigit()
            or isinstance(previous_close, bool)
            or not isinstance(previous_close, (int, float))
            or not isfinite(float(previous_close))
            or float(previous_close) <= 0
        ):
            raise CanonicalV16ScanError("V16 scan: D1 close snapshot contains an invalid row")
        daily_row = prior_daily.get(code)
        if daily_row is not None and float(previous_close) != float(daily_row.close_price):
            raise CanonicalV16ScanError(
                f"V16 scan: D1 close differs across frozen sources for {code}"
            )

    breadth_universe = tuple(
        sorted(
            code
            for code in prev_closes
            if len(code) == 6 and code.startswith(("00", "60")) and prev_closes[code] > 0
        )
    )
    early_universe = tuple(sorted(set(universe_list).union(breadth_universe)))

    # Merge retained partial evidence and any caller-supplied seed with a fresh
    # pull of unresolved codes.  Conflicting retained/seeded provenance is a
    # semantic error; it is never resolved by arrival order.
    partial_data: dict[str, TushareEarlyMarketData] = dict(partial) if partial else {}
    if set(partial_data) - set(early_universe):
        raise CanonicalV16ScanError("V16 scan: retained early evidence has unknown codes")
    if early_data_seed is not None:
        if set(early_data_seed) - set(early_universe):
            raise CanonicalV16ScanError("V16 scan: seeded early evidence has unknown codes")
        for code, seeded in early_data_seed.items():
            retained = partial_data.get(code)
            if retained is not None and retained.source_hash != seeded.source_hash:
                raise CanonicalV16ScanError(
                    f"V16 scan: retained and seeded early provenance differs for {code}"
                )
            partial_data[code] = seeded
    ready_partial_codes = set(partial_data.keys())
    unresolved = [code for code in early_universe if code not in ready_partial_codes]

    rt_client = scan_state.realtime_client
    new_data: dict[str, TushareEarlyMarketData] = {}
    if unresolved and allow_realtime_fetch:
        new_data = await rt_client.batch_get_early_market_data(
            unresolved, expected_trade_date=trade_date
        )
    elif unresolved:
        logger.info(
            f"V16 scan: realtime early fetch forbidden; "
            f"{len(unresolved)}/{len(universe_list)} codes unresolved"
        )

    early_data: dict[str, TushareEarlyMarketData] = {}
    for code in early_universe:
        if code in partial_data:
            early_data[code] = partial_data[code]
        elif code in new_data:
            early_data[code] = new_data[code]

    ready_set = _ready_codes(early_data, trade_date)
    breadth_ready = [code for code in breadth_universe if code in ready_set]
    breadth_valid_n = 0
    breadth_down_n = 0
    for code in breadth_ready:
        breadth_quote = early_data[code].quote
        breadth_previous_close = prev_closes.get(code)
        if (
            breadth_quote.early_close is not None
            and breadth_quote.early_close > 0
            and breadth_previous_close is not None
            and breadth_previous_close > 0
        ):
            breadth_valid_n += 1
            if breadth_quote.early_close < float(breadth_previous_close):
                breadth_down_n += 1
    breadth_market_source_hash = hashlib.sha256(
        json.dumps(
            {code: early_data[code].source_hash for code in breadth_ready},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    breadth_missing = tuple(code for code in breadth_universe if code not in ready_set)
    ready_codes = [code for code in universe_list if code in ready_set]
    quotes = {code: early_data[code].quote for code in ready_codes}
    # Raw/provenance fields retain the whole one-request union.  Scanner inputs
    # below remain restricted to the legacy V16 universe, so breadth enrichment
    # cannot change ticket selection or its 80% readiness denominator.
    ready_union_codes = [code for code in early_universe if code in ready_set]
    early_bars_map = {code: early_data[code].early_bars for code in ready_union_codes}
    early_source_hashes = {code: early_data[code].source_hash for code in ready_union_codes}

    response_count = sum(code in early_data for code in universe_list)
    readiness = len(ready_codes) / len(universe_list) if universe_list else 0
    logger.info(
        f"V16 scan: Tushare returned {response_count}/{len(universe_list)} early responses, "
        f"{len(ready_codes)} ready (readiness={readiness:.1%})"
    )

    if response_count == 0:
        raise CanonicalV16ScanError(
            f"V16 scan: Tushare returned 0 quotes for {len(universe_list)} stocks",
            notify_title="9:40行情全空",
            notify_detail=(
                f"Tushare batch_get_early_quotes 返回空\n请求股票数: {len(universe_list)}\n扫描中止"
            ),
        )

    # 09:39 readiness bound: if <80% of the universe has a current-date 09:39 bar,
    # preserve the ready subset and raise NOT_READY so retries fetch only missing codes.
    if readiness < 0.8:
        ready_partial = {code: early_data[code] for code in early_universe if code in ready_set}
        detail = (
            f"有09:39分钟线: {len(ready_codes)} 只\n"
            f"universe: {len(universe_list)} 只\n"
            f"占比: {readiness:.1%} (阈值80%)\n"
            f"09:39数据未就绪，稍后重试"
        )
        raise CanonicalV16NotReadyError(
            f"V16 scan: 09:39 readiness {len(ready_codes)}/{len(universe_list)} "
            f"({readiness:.1%}) below 80% threshold — not ready",
            partial=ready_partial,
            notify_title="9:39数据未就绪",
            notify_detail=detail,
        )

    # Fetch 37d OHLCV history for ready stocks
    trading_codes = [code for code in ready_codes if quotes[code].is_trading]
    logger.info(f"V16 scan: {len(trading_codes)} stocks trading, fetching history...")

    if len(trading_codes) < len(quotes) * 0.5:
        detail = (
            f"09:39就绪: {len(quotes)} 只\n"
            f"标记交易中: {len(trading_codes)} 只\n"
            f"占比: {len(trading_codes) / len(quotes):.1%} (阈值50%)\n"
            f"数据可能异常，扫描中止"
        )
        raise CanonicalV16ScanError(
            f"V16 scan: only {len(trading_codes)}/{len(quotes)} stocks marked trading "
            f"({len(trading_codes) / len(quotes):.1%}) — halting",
            notify_title="交易中股票过少",
            notify_detail=detail,
        )

    hist_raw = (
        {
            code: dict(raw_history)
            for code in trading_codes
            if (raw_history := history_raw_override.get(code)) is not None
        }
        if history_raw_override is not None
        else await _fetch_history_ohlcv(
            scan_state.historical_adapter,
            trading_codes,
            trade_date,
        )
    )
    # Ensure deterministic insertion order matching the scanner input order.
    hist_raw = {code: hist_raw[code] for code in trading_codes if code in hist_raw}
    normalized_history: dict[str, dict[str, Any]] = {}
    history_normalization_errors: dict[str, str] = {}
    for code, history in hist_raw.items():
        try:
            normalized_history[code] = _normalize_history_inputs({code: history})[code]
        except (TypeError, ValueError) as exc:
            history_normalization_errors[code] = str(exc)
    hist_raw = normalized_history
    expected_history_dates = previous_dates[-LOOKBACK_DAYS:]
    valid_history_dates = {
        code: _valid_history_dates(history) for code, history in hist_raw.items()
    }
    history_date_valid_counts = {
        day.isoformat(): sum(
            day.isoformat() in valid_dates for valid_dates in valid_history_dates.values()
        )
        for day in expected_history_dates
    }
    history_min_date_coverage = (
        min(history_date_valid_counts.values()) / len(universe_list)
        if universe_list and history_date_valid_counts
        else 0.0
    )
    # Preserve the original V16 safety gate exactly.  Per-exchange-date minimum
    # coverage is frozen separately for V20 health evaluation; it must not
    # silently replace the legacy per-trading-symbol 80% denominator.
    hist_coverage = len(hist_raw) / len(trading_codes) if trading_codes else 0.0
    logger.info(
        f"V16 scan: history fetched for {len(hist_raw)}/{len(trading_codes)} stocks "
        f"(coverage={hist_coverage:.1%})"
    )

    if trading_codes and len(history_normalization_errors) == len(trading_codes):
        first_code = sorted(history_normalization_errors)[0]
        first_error = history_normalization_errors[first_code]
        raise CanonicalV16ScanError(
            first_error,
            notify_title="V16 history normalization failed",
            notify_detail=(
                f"trading stocks: {len(trading_codes)}\n"
                f"normalization failures: {len(history_normalization_errors)}\n"
                f"first error: {first_error}"
            ),
        )

    if hist_coverage < 0.8:
        detail = (
            f"请求: {len(trading_codes)} 只\n"
            f"返回: {len(hist_raw)} 只\n"
            f"覆盖率: {hist_coverage:.1%} (阈值80%)\n"
            f"历史数据源可能异常，扫描中止"
        )
        raise CanonicalV16ScanError(
            f"V16 scan: history coverage {len(hist_raw)}/{len(trading_codes)} "
            f"({hist_coverage:.1%}) below 80% threshold — halting",
            notify_title="历史数据覆盖率不足",
            notify_detail=detail,
        )

    # Batch fetch company names from fundamentals DB
    fdb = scan_state.fundamentals_db
    name_map: dict[str, str] = {}
    if names_override is not None:
        name_map = dict(names_override)
    elif fdb:
        try:
            fund_data = await fdb.batch_get_fundamentals(trading_codes)
            name_map = {code: f.company_name for code, f in fund_data.items()}
        except Exception as e:
            logger.warning(f"V16 scan: failed to fetch company names: {e}")

    # Build V16StockData for each stock
    from src.strategy.strategies.v16_scanner import V16StockData  # noqa: F811

    stock_data: dict[str, V16StockData] = {}
    errors_no_prev_close: list[str] = []
    errors_no_hist: list[str] = []
    errors_build: list[str] = []
    skipped_new_listings: list[str] = []

    for code in trading_codes:
        stock_quote = quotes.get(code)
        if not stock_quote or not stock_quote.is_trading:
            continue

        pc = prev_closes.get(code)
        if not pc or pc <= 0:
            errors_no_prev_close.append(code)
            continue

        hr = hist_raw.get(code)
        if code in history_normalization_errors:
            errors_no_hist.append(f"{code}: {history_normalization_errors[code]}")
            continue
        if not hr:
            errors_no_hist.append(code)
            continue

        try:
            sd = _build_stock_data(code, name_map.get(code, ""), stock_quote, pc, hr, trade_date)
        except RuntimeError as e:
            errors_build.append(f"{code}: {e}")
            continue

        if sd is None:
            skipped_new_listings.append(code)
            continue

        stock_data[code] = sd

    # --- Data failure reporting: collect structured notification, never silent ---
    total_errors = len(errors_no_prev_close) + len(errors_no_hist) + len(errors_build)
    data_error_notification: tuple[str, str] | None = None
    if total_errors > 0:
        detail_lines = []
        if errors_no_prev_close:
            detail_lines.append(
                f"缺昨收({len(errors_no_prev_close)}): " + ", ".join(errors_no_prev_close[:20])
            )
        if errors_no_hist:
            detail_lines.append(f"缺历史({len(errors_no_hist)}): " + ", ".join(errors_no_hist[:20]))
        if errors_build:
            detail_lines.append(f"构建失败({len(errors_build)}): " + "\n".join(errors_build[:10]))
        detail = "\n".join(detail_lines)
        logger.error(
            f"V16 scan: {total_errors} stocks with data errors "
            f"(no_prev_close={len(errors_no_prev_close)}, "
            f"no_hist={len(errors_no_hist)}, "
            f"build_fail={len(errors_build)})"
        )
        data_error_notification = (
            "数据缺失报警",
            f"交易中股票: {len(trading_codes)}\n"
            f"数据错误: {total_errors} 只\n"
            f"新股跳过: {len(skipped_new_listings)} 只\n"
            f"成功构建: {len(stock_data)} 只\n\n{detail}",
        )

    if total_errors > 0 and total_errors > len(trading_codes) * 0.2:
        assert data_error_notification is not None
        raise CanonicalV16ScanError(
            f"V16 scan: data error rate {total_errors}/{len(trading_codes)} "
            f"exceeds 20% threshold — data source likely broken, halting",
            notify_title=data_error_notification[0],
            notify_detail=data_error_notification[1],
            data_error_notification=data_error_notification,
        )

    logger.info(
        f"V16 scan: built {len(stock_data)} V16StockData "
        f"(errors={total_errors}, new_listing={len(skipped_new_listings)})"
    )

    if not stock_data:
        raise CanonicalV16ScanError(
            "V16 scan: no valid stock data after building",
            notify_title="无有效股票数据",
            notify_detail=(f"交易中股票: {len(trading_codes)}\n全部数据缺失或为新股, 无法执行扫描"),
            data_error_notification=data_error_notification,
        )

    # Run V16 scan
    try:
        scan_result = await scanner.scan(stock_data, clean_boards_for_scan)
    except Exception as e:
        # If there were nonfatal data errors, the original data-error alert must be
        # emitted unchanged; the scanner failure is reported as a separate fatal.
        raise CanonicalV16ScanError(
            f"V16 scanner failed: {e}",
            notify_title="V16扫描失败",
            notify_detail=f"扫描核心异常: {type(e).__name__}: {e}",
            data_error_notification=data_error_notification,
        ) from e

    # Final name refresh is part of the canonical bundle (display only).
    if names_override is None:
        await _refresh_top10_names(scan_state.fundamentals_db, scan_result.recommended)

    computed_at = datetime.now(BEIJING_TZ)
    recommended_names = {s.code: s.name for s in getattr(scan_result, "recommended", [])}
    try:
        input_hash = _stable_input_hash(
            trade_date,
            tuple(universe_list),
            clean_boards,
            scorer.model_sha256,
            scorer.feature_list_sha256,
            early_source_hashes,
            prev_closes,
            hist_raw,
            stock_data,
            tuple(errors_no_prev_close),
            tuple(errors_no_hist),
            tuple(errors_build),
            tuple(skipped_new_listings),
            recommended_names,
            getattr(scan_result, "st_eligible_codes", []),
        )
    except (ValueError, TypeError) as exc:
        raise CanonicalV16ScanError(
            f"V16 scan: failed to canonicalize selection inputs: {exc}",
            notify_title="V16扫描失败",
            notify_detail=f"输入规范化失败: {type(exc).__name__}: {exc}",
        ) from exc

    # Wrap outer containers in read-only views; inner objects are protected by the
    # integrity hash computed below. Consumers receive deep-copied artifacts.
    frozen_stock_data = MappingProxyType(dict(stock_data))
    frozen_prev_closes = MappingProxyType(dict(prev_closes))
    frozen_history_raw = MappingProxyType(
        {code: MappingProxyType(dict(hist)) for code, hist in hist_raw.items()}
    )
    frozen_early_bars = MappingProxyType(dict(early_bars_map))
    frozen_early_source_hashes = MappingProxyType(dict(early_source_hashes))
    frozen_quotes = MappingProxyType(dict(quotes))
    recommended_codes = {
        getattr(stock, "code", None) for stock in getattr(scan_result, "recommended", [])
    }
    prior_amount_yuan = {
        code: float(row.amount_yuan)
        for code, row in prior_daily.items()
        if code in recommended_codes and isfinite(row.amount_yuan) and row.amount_yuan > 0
    }
    missing_prior_amounts = sorted(
        code for code in recommended_codes if code and code not in prior_amount_yuan
    )
    if missing_prior_amounts:
        raise CanonicalV16ScanError(
            "V16 scan: authoritative D1 amount is missing for recommendations: "
            + ",".join(missing_prior_amounts)
        )
    frozen_history_counts = MappingProxyType(dict(history_date_valid_counts))

    pre_bundle = CanonicalV16ScanBundle(
        trade_date=trade_date,
        scan_result=scan_result,
        stock_data=frozen_stock_data,
        clean_boards=clean_boards,
        universe=tuple(universe_list),
        quotes=frozen_quotes,
        prev_closes=frozen_prev_closes,
        history_raw=frozen_history_raw,
        early_bars=frozen_early_bars,
        early_source_hashes=frozen_early_source_hashes,
        failed_no_prev_close=tuple(errors_no_prev_close),
        failed_no_history=tuple(errors_no_hist),
        failed_build=tuple(errors_build),
        skipped_new_listings=tuple(skipped_new_listings),
        model_sha256=scorer.model_sha256,
        feature_list_sha256=scorer.feature_list_sha256,
        computed_at=computed_at,
        input_hash=input_hash,
        _integrity_hash="",
        computation_calendar=tuple(calendar),
        data_error_notification=data_error_notification,
        prior_trade_date=prior_trade_date,
        prior_amount_yuan=MappingProxyType(prior_amount_yuan),
        breadth_valid_n=breadth_valid_n,
        breadth_down_n=breadth_down_n,
        breadth_market_source_hash=breadth_market_source_hash,
        breadth_market_missing_codes=breadth_missing,
        breadth_market_conflict_codes=(),
        history_date_valid_counts=frozen_history_counts,
        history_min_date_coverage=history_min_date_coverage,
    )
    return replace(pre_bundle, _integrity_hash=_bundle_fingerprint(pre_bundle))
