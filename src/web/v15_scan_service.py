# === MODULE PURPOSE ===
# Autonomous V16 scan scheduler — runs independently from app startup.
# Decoupled from trading: always scans + pushes Feishu, never checks holdings.
#
# === ARCHITECTURE ===
# Owns scan resources (Tushare, FundamentalsDB, HistAdapter, ConceptMapper, LGBRank).
# Writes today's recommendation to V15ScanState for trading module to read.
# Resource initialization retries on failure (resilient to transient errors).
#
# === DATA FLOW ===
# 09:38-10:00  → Run V16 scan → push Feishu top-10 + recommendation
#              → Write result to scan_state.today_recommendation (top-1 for trading)
# Trading scheduler (in iquant_routes.py) reads scan_state to decide BUY/SELL.

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import traceback
from collections.abc import Awaitable, Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from math import isfinite
from pathlib import Path
from types import MappingProxyType
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from src.data.clients.tushare_realtime import TushareEarlyMarketData

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Strong references for observation-only DayGate workers.  These tasks never
# feed a value back into scan_state or the iQuant signal path.
_DAY_GATE_SHADOW_TASKS: set[asyncio.Task[None]] = set()
_DAY_GATE_SHADOW_EXECUTOR: ThreadPoolExecutor | None = None


# --- Scan state container ---


@dataclass
class V15ScanState:
    """State owned by the scan service. Trading module reads today_recommendation."""

    initialized: bool = False
    today_recommendation: dict[str, Any] | None = None
    scan_done_date: str = ""  # "YYYY-MM-DD" of last completed scan
    scan_error: str | None = None

    # Resources (initialized by init_scan_resources)
    realtime_client: Any = None
    fundamentals_db: Any = None
    historical_adapter: Any = None
    concept_mapper: Any = None
    stock_filter: Any = None
    v15_scan_db: Any = None
    tushare_cache: Any = None
    universe_cache: list[str] | None = None

    # Scheduler task reference
    scheduler_task: Any = None
    resource_owner: str | None = None
    resource_init_task: asyncio.Task[None] | None = None
    resource_cleanup_task: asyncio.Task[None] | None = None
    resource_init_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    resource_cleanup_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # Lazy single-flight coordinator for canonical V16 scan results.
    canonical_coordinator: "_CanonicalV16Coordinator | None" = None


@dataclass
class _CanonicalV16Coordinator:
    """In-memory single-flight coordinator keyed by trade_date only.

    State is bound to the current/most recent trade date and cleared on cleanup.
    Same-day resource replacement or manual wrapper calls reuse the already sealed
    official bundle.
    """

    cache: dict[date, "CanonicalV16ScanBundle"] = field(default_factory=dict)
    inflight: dict[date, asyncio.Task] = field(default_factory=dict)
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
    data_error_notification: tuple[str, str] | None = None


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
    """Drop all coordinator state except the current/most recent trade date."""
    for container in (coord.cache, coord.inflight, coord.publish, coord.partial):
        for stale in [d for d in container if d != keep_date]:
            container.pop(stale, None)
    coord.published.intersection_update({keep_date})
    coord.data_errors_sent.intersection_update({keep_date})
    coord.not_ready_alert_sent.intersection_update({keep_date})
    coord.fatal_errors_sent = {item for item in coord.fatal_errors_sent if item[0] == keep_date}


async def _fail_not_ready_deadline(
    scan_state: V15ScanState,
    trade_date: date,
    now_bj: datetime,
) -> None:
    """Single audit alert when 09:39 evidence is still missing at the 10:00 deadline."""
    if scan_state.canonical_coordinator is None:
        scan_state.canonical_coordinator = _CanonicalV16Coordinator()
    coord = scan_state.canonical_coordinator
    if trade_date in coord.not_ready_alert_sent:
        return
    coord.not_ready_alert_sent.add(trade_date)
    detail = f"截至 {now_bj.strftime('%H:%M')} 09:39数据仍未就绪\n今日V16扫描终止，已清空推荐"
    await _notify_feishu_error("9:39数据未就绪截止", detail)
    scan_state.scan_error = f"CanonicalV16NotReadyError: {detail}"
    scan_state.today_recommendation = None


# --- Feishu notification helpers ---


async def _notify_feishu_error(title: str, detail: str) -> None:
    """Send error alert to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_alert(f"[V16] {title}", detail)
    except Exception:
        logger.warning("Failed to send Feishu error notification", exc_info=True)


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


async def _notify_feishu_v16_top10(scan_result: Any) -> None:
    """Send V16 top-10 scored report to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_v16_top10_report(scan_result)
    except Exception:
        logger.warning("Failed to send Feishu V16 top-10 report", exc_info=True)


async def _notify_feishu_v16_day_gate_shadow(message: str) -> None:
    """Send the separate DayGate shadow report. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_message(message, max_retries=3)
    except Exception:
        logger.warning("Failed to send Feishu V16 DayGate shadow report", exc_info=True)


def _day_gate_shadow_executor() -> ThreadPoolExecutor:
    """Return the dedicated, capacity-one executor for local shadow I/O."""

    global _DAY_GATE_SHADOW_EXECUTOR
    if _DAY_GATE_SHADOW_EXECUTOR is None:
        _DAY_GATE_SHADOW_EXECUTOR = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="v16-day-gate-shadow",
        )
    return _DAY_GATE_SHADOW_EXECUTOR


def _execute_v16_day_gate_shadow_sync(
    frozen_snapshot: dict[str, Any],
    frozen_runtime: Any,
) -> tuple[bool, str, str] | None:
    """Evaluate and append one shadow record in a worker thread.

    This function intentionally has no access to ``scan_state`` and returns no
    recommendation.  Any exception is handled by the async wrapper and cannot
    propagate into the scan/trading path.
    """
    from src.strategy.v16_day_gate_evidence import (
        append_v16_day_gate_evidence,
        build_v16_day_gate_evidence,
    )
    from src.strategy.v16_day_gate_shadow import (
        prepare_shadow_decision,
        prepared_to_metadata,
        shadow_message,
    )

    config = frozen_runtime.config
    prepared = prepare_shadow_decision(frozen_snapshot, frozen_runtime, _PROJECT_ROOT)
    evidence_snapshot = {
        **frozen_snapshot,
        "shadow_evaluation": prepared_to_metadata(prepared),
    }
    evaluated_at = datetime.now(BEIJING_TZ)
    scanner_version = prepared.provenance.get("scanner_runtime_version") or "unknown"
    record = build_v16_day_gate_evidence(
        gate_input=prepared.gate_input,
        decision=prepared.decision,
        frozen_snapshot=evidence_snapshot,
        evaluated_at=evaluated_at,
        scanner_version=scanner_version,
        model_version=prepared.gate_input.model_version,
        taxonomy_version=prepared.gate_input.taxonomy_version,
        policy_version=prepared.decision.policy_version,
    )
    evidence_path = append_v16_day_gate_evidence(config.evidence_dir, record)
    message = shadow_message(frozen_snapshot, prepared, evidence_path)
    return config.send_feishu, message, str(evidence_path)


async def _run_v16_day_gate_shadow(
    frozen_snapshot: dict[str, Any],
    frozen_runtime: Any,
) -> None:
    """Run observation-only evaluation without delaying or mutating a trade."""
    try:
        loop = asyncio.get_running_loop()
        outcome = await loop.run_in_executor(
            _day_gate_shadow_executor(),
            _execute_v16_day_gate_shadow_sync,
            frozen_snapshot,
            frozen_runtime,
        )
        if outcome is None:
            return
        send_feishu, message, evidence_path = outcome
        logger.info(
            "V16 DayGate shadow evidence appended: run_id=%s path=%s",
            frozen_snapshot.get("run_id"),
            evidence_path,
        )
        if send_feishu:
            await _notify_feishu_v16_day_gate_shadow(message)
    except asyncio.CancelledError:
        raise
    except Exception:
        # Phase 1 is structurally fail-open: errors are observable but can
        # neither clear today_recommendation nor set scan_error.
        logger.warning(
            "V16 DayGate shadow worker failed: run_id=%s",
            frozen_snapshot.get("run_id"),
            exc_info=True,
        )


def _schedule_v16_day_gate_shadow(
    frozen_snapshot: dict[str, Any],
    frozen_runtime: Any,
) -> None:
    """Schedule a managed shadow worker and return immediately."""
    task = asyncio.create_task(
        _run_v16_day_gate_shadow(frozen_snapshot, frozen_runtime),
        name=f"v16-day-gate-shadow-{frozen_snapshot.get('run_id', 'unknown')}",
    )
    _DAY_GATE_SHADOW_TASKS.add(task)
    task.add_done_callback(_DAY_GATE_SHADOW_TASKS.discard)


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


async def _notify_feishu_signal(signal: dict) -> None:
    """Send signal notification to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if not bot.is_configured():
            return

        direction = "买入" if signal["type"] == "buy" else "卖出"
        lines = [
            f"[V16] {direction}信号",
            f"股票: {signal['stock_code']} {signal.get('stock_name', '')}",
        ]
        if signal["type"] == "buy":
            lines.append(f"板块: {signal.get('board_name', '-')}")
            lines.append(f"买入参考价(09:40): {signal.get('latest_price', '-')}")
            lines.append(f"LGB评分: {signal.get('lgb_score', '-')}")
        if signal["type"] == "sell":
            lines.append(f"原因: {signal.get('reason', '-')}")
        lines.append(f"时间: {signal.get('created_at', '')}")

        await bot.send_message("\n".join(lines))
    except Exception:
        logger.warning("Failed to send Feishu signal notification", exc_info=True)


# --- Resource management ---


async def _initialize_scan_resources_once(
    scan_state: V15ScanState,
    owner: str,
    initializer: Callable[[], Awaitable[None]],
) -> None:
    """Initialize one resource set once, even across concurrent owners/borrowers.

    The master initialization task itself publishes the pending and final
    ``resource_owner`` atomically. A waiter cancelled while shielded behind the
    master therefore never prevents the surviving master from recording its
    owner, and a concurrent cleanup can always tell who owns an in-flight
    initialization.

    The lock is held only to wait-for/select/create the shared master task;
    every await happens outside it. All concurrent callers therefore capture
    the same master, and a borrower already queued behind it resolves on that
    original master's outcome even after a correct cleanup — it never
    re-initializes resources behind the cleanup's back.
    """

    async def _master() -> None:
        try:
            await initializer()
        except BaseException:
            # A failed/cancelled attempt must not leave its pending owner
            # claim behind; the next caller retries under its own owner.
            if not scan_state.initialized and scan_state.resource_owner == owner:
                scan_state.resource_owner = None
            raise
        # Ownership is published by the master task itself, not by whichever
        # caller happened to await it — that caller may have been cancelled.
        scan_state.resource_owner = owner

    master: asyncio.Task[None] | None = None
    while master is None:
        async with scan_state.resource_init_lock:
            cleanup_task = scan_state.resource_cleanup_task
            cleanup_pending = cleanup_task is not None and not cleanup_task.done()
            if not cleanup_pending:
                if scan_state.initialized:
                    return
                master = scan_state.resource_init_task
                if master is None or master.done():
                    master = asyncio.create_task(_master())
                    scan_state.resource_init_task = master
                    # Atomic pending claim, visible to cleanup before init completes.
                    scan_state.resource_owner = owner

                    def _remove(task: asyncio.Task[None]) -> None:
                        if scan_state.resource_init_task is task:
                            scan_state.resource_init_task = None

                    master.add_done_callback(_remove)
        if master is None:
            # A cleanup is in flight: wait for it outside the lock, then
            # re-check the state before selecting or creating a master.
            if cleanup_task is not None:
                await asyncio.shield(cleanup_task)
    await asyncio.shield(master)


async def _initialize_v16_scan_resources(scan_state: V15ScanState) -> None:
    """Initialize V16 resources with all-or-nothing rollback."""

    from src.common.config import get_tushare_token
    from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter
    from src.data.clients.tushare_realtime import TushareRealtimeClient
    from src.data.database.fundamentals_db import create_fundamentals_db_from_config
    from src.data.database.v15_scan_db import create_v15_scan_db_from_config
    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig

    tushare = TushareRealtimeClient(token=get_tushare_token())
    fundamentals = scan_state.fundamentals_db
    if fundamentals is not None and getattr(fundamentals, "closed", False) is True:
        # A closed pool must never be mistaken for a healthy one: drop the
        # stale reference so initialization reconnects with a fresh pool.
        logger.warning("V16 init: discarding closed fundamentals pool reference")
        scan_state.fundamentals_db = None
        fundamentals = None
    owns_fundamentals = fundamentals is None
    scan_db = scan_state.v15_scan_db
    owns_scan_db = scan_db is None
    try:
        await tushare.start()
        if owns_fundamentals:
            fundamentals = create_fundamentals_db_from_config()
            await fundamentals.connect()
        if owns_scan_db:
            # V15ScanDB only stores scan history; its failure is non-critical
            # and must not roll back RT/history/fundamentals.
            try:
                scan_db = create_v15_scan_db_from_config()
                await scan_db.connect()
            except Exception as scan_db_error:
                logger.warning(
                    "V15ScanDB init failed (scan history disabled): %s",
                    scan_db_error,
                )
                scan_state.scan_error = (
                    f"V15ScanDB init failed (non-critical): "
                    f"{type(scan_db_error).__name__}: {scan_db_error}"
                )
                if scan_db is not None:
                    # connect() may have partially opened the fresh object;
                    # close it best-effort so no half-open handle leaks. A
                    # close failure is only logged — the canonical chain must
                    # still initialize.
                    try:
                        await scan_db.close()
                    except Exception:
                        logger.warning(
                            "V15ScanDB close after failed connect also failed",
                            exc_info=True,
                        )
                scan_db = None
        historical_adapter = IQuantHistoricalAdapter(
            tushare,
            cache=scan_state.tushare_cache,
        )
        concept_mapper = LocalConceptMapper()
        stock_filter = StockFilter(
            StockFilterConfig(
                exclude_bse=True,
                exclude_chinext=True,
                exclude_star=True,
                exclude_sme=False,
            )
        )
    except BaseException as initialization_error:
        cleanup_tasks: list[Awaitable[None]] = [tushare.stop()]
        if owns_fundamentals and fundamentals is not None:
            cleanup_tasks.append(fundamentals.close())
        if owns_scan_db and scan_db is not None:
            cleanup_tasks.append(scan_db.close())
        cleanup_results = await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        for cleanup_result in cleanup_results:
            if isinstance(cleanup_result, BaseException):
                initialization_error.add_note(
                    f"V16 partial-resource cleanup failed: "
                    f"{type(cleanup_result).__name__}: {cleanup_result}"
                )
        if isinstance(initialization_error, Exception):
            await _notify_feishu_error(
                "V16 resource initialization failed",
                f"{type(initialization_error).__name__}: {initialization_error}",
            )
        raise

    scan_state.realtime_client = tushare
    scan_state.fundamentals_db = fundamentals
    scan_state.v15_scan_db = scan_db
    scan_state.historical_adapter = historical_adapter
    scan_state.concept_mapper = concept_mapper
    scan_state.stock_filter = stock_filter
    scan_state.initialized = True
    scan_state.resource_owner = "V16"
    logger.info("V16 scan resources initialized")


async def init_scan_resources(scan_state: V15ScanState) -> None:
    """Initialize the V16-owned resource set through the shared singleflight."""

    await _initialize_scan_resources_once(
        scan_state,
        "V16",
        lambda: _initialize_v16_scan_resources(scan_state),
    )


async def _cleanup_scan_resources_once(
    scan_state: V15ScanState,
    *,
    owner: str,
    close_fundamentals: bool,
) -> None:
    """Cleanup scan resources on shutdown.

    Cancels and awaits any in-flight canonical compute or publication tasks before
    stopping underlying resources, then clears coordinator state so a later retry
    after cancellation starts fresh.

    Every phase is isolated by its own try/finally: one failing close never
    skips the remaining resources, and shared state is always left consistent
    before any aggregated error is raised.
    """
    del owner  # ownership was already enforced by the public wrapper
    global _DAY_GATE_SHADOW_EXECUTOR
    cleanup_errors: list[BaseException] = []

    task = scan_state.scheduler_task
    try:
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    except Exception as exc:
        cleanup_errors.append(exc)
    finally:
        scan_state.scheduler_task = None

    coord = scan_state.canonical_coordinator
    try:
        if coord is not None:
            coord_tasks: list[asyncio.Task] = []
            for t in list(coord.inflight.values()) + list(coord.publish.values()):
                if not t.done():
                    t.cancel()
                    coord_tasks.append(t)
            if coord_tasks:
                await asyncio.gather(*coord_tasks, return_exceptions=True)
    except Exception as exc:
        cleanup_errors.append(exc)
    finally:
        scan_state.canonical_coordinator = None

    try:
        shadow_tasks = tuple(_DAY_GATE_SHADOW_TASKS)
        for shadow_task in shadow_tasks:
            shadow_task.cancel()
        if shadow_tasks:
            await asyncio.gather(*shadow_tasks, return_exceptions=True)
        shadow_executor = _DAY_GATE_SHADOW_EXECUTOR
        _DAY_GATE_SHADOW_EXECUTOR = None
        if shadow_executor is not None:
            # Cancelling an asyncio wrapper cannot stop an already-running thread.
            # A dedicated executor lets shutdown explicitly drain local evidence I/O,
            # so no write can continue after resource cleanup returns.
            shadow_executor.shutdown(wait=True, cancel_futures=False)
    except Exception as exc:
        cleanup_errors.append(exc)

    rt_client = scan_state.realtime_client
    fdb = scan_state.fundamentals_db if close_fundamentals else None
    v15db = scan_state.v15_scan_db
    close_operations: list[tuple[str, Awaitable[None]]] = []
    if rt_client is not None:
        close_operations.append(("realtime client", rt_client.stop()))
    if fdb is not None:
        close_operations.append(("fundamentals DB", fdb.close()))
    if v15db is not None:
        close_operations.append(("V15 scan DB", v15db.close()))
    for label, operation in close_operations:
        try:
            await operation
        except Exception as exc:
            logger.error("V16 cleanup: failed to close %s", label, exc_info=True)
            cleanup_errors.append(exc)

    scan_state.realtime_client = None
    scan_state.historical_adapter = None
    scan_state.concept_mapper = None
    scan_state.stock_filter = None
    if close_fundamentals and fdb is not None:
        # The pool is closed (or its close was attempted); drop the reference so
        # a later initialization reconnects instead of reusing a closed pool.
        scan_state.fundamentals_db = None
    scan_state.v15_scan_db = None
    scan_state.initialized = False
    scan_state.resource_owner = None
    if cleanup_errors:
        raise RuntimeError(
            "scan resource cleanup failed: "
            + "; ".join(f"{type(error).__name__}: {error}" for error in cleanup_errors)
        )
    logger.info("V16 scan resources cleaned up")


async def cleanup_scan_resources(
    scan_state: V15ScanState,
    *,
    owner: str = "V16",
    close_fundamentals: bool = True,
) -> None:
    """Clean the live resource set once; only its owner may close it."""

    async with scan_state.resource_cleanup_lock:
        # Owner gate first: a wrong-owner cleanup must never cancel another
        # owner's pending initialization, nor tear down its resources.
        init_task = scan_state.resource_init_task
        init_pending = init_task is not None and not init_task.done()
        if scan_state.resource_owner not in (None, owner) and (
            scan_state.initialized or init_pending
        ):
            return
        if init_task is not None:
            if init_pending:
                init_task.cancel()
            await asyncio.gather(init_task, return_exceptions=True)
        current = scan_state.resource_cleanup_task
        if current is not None and not current.done():
            await asyncio.shield(current)
            return
        scan_state.initialized = False
        scan_state.resource_owner = None
        task = asyncio.create_task(
            _cleanup_scan_resources_once(
                scan_state,
                owner=owner,
                close_fundamentals=close_fundamentals,
            )
        )
        scan_state.resource_cleanup_task = task

        def _remove(task: asyncio.Task[None]) -> None:
            if scan_state.resource_cleanup_task is task:
                scan_state.resource_cleanup_task = None

        task.add_done_callback(_remove)
        await asyncio.shield(task)


# --- Trade calendar (shared) ---

_trade_calendar_cache: list[date] | None = None


async def get_trade_calendar() -> list[date]:
    """Get A-share trade calendar (cached). Uses akshare."""
    global _trade_calendar_cache
    if _trade_calendar_cache is not None:
        return _trade_calendar_cache

    import akshare as ak

    df = await asyncio.to_thread(ak.tool_trade_date_hist_sina)
    _trade_calendar_cache = sorted(
        datetime.strptime(str(d), "%Y-%m-%d").date() for d in df["trade_date"]
    )
    logger.info(f"Trade calendar cached: {len(_trade_calendar_cache)} dates")
    return _trade_calendar_cache


# --- V16 data building helpers ---

LOOKBACK_DAYS = 37  # trading days for historical data


async def _fetch_prev_closes(
    scan_state: V15ScanState, today: date, calendar: Sequence[date]
) -> dict[str, float]:
    """Fetch prev_close for all stocks. Returns code → prev_close."""
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
        ts_date = prev_trade_date.replace("-", "")
        api_closes = await rt_client.fetch_prev_closes(ts_date)
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


# --- V16 canonical core / single-flight ---


def _canonical_key(scan_state: V15ScanState, trade_date: date) -> date:
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
        "data_error_notification": bundle.data_error_notification,
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
        asyncio.create_task(_notify_feishu_error(data_title, data_detail))
        coord.data_errors_sent.add(trade_date)
    asyncio.create_task(_notify_feishu_error(error.notify_title, detail))


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
    )


async def get_or_compute_canonical_v16(
    scan_state: V15ScanState,
    trade_date: date | None = None,
) -> CanonicalV16ScanBundle:
    """Return an isolated copy of the canonical V16 bundle for ``trade_date``.

    Concurrent callers for the same key share exactly one ``compute_canonical_v16_scan``
    invocation (one early pull, one ``scanner.scan``). Successful results are frozen
    in the cache as a master bundle that is never exposed directly; each consumer
    receives its own deep-copied artifact. Failures are not cached and may be retried.
    Cancelling a waiting caller does not cancel the shared in-flight task, and a fatal
    alert is still sent from the compute done callback even if all waiters are gone.
    """
    if trade_date is None:
        trade_date = datetime.now(BEIJING_TZ).date()

    if scan_state.canonical_coordinator is None:
        scan_state.canonical_coordinator = _CanonicalV16Coordinator()
    coord = scan_state.canonical_coordinator
    key = _canonical_key(scan_state, trade_date)

    _evict_stale_dates(coord, key)

    master = coord.cache.get(key)
    if master is not None:
        _verify_bundle_integrity(master)
        return _isolate_bundle(master)

    task = coord.inflight.get(key)
    if task is None:
        partial = coord.partial.get(key, {})

        async def _runner() -> CanonicalV16ScanBundle:
            return await compute_canonical_v16_scan(scan_state, trade_date, partial=partial)

        task = asyncio.create_task(_runner())
        coord.inflight[key] = task

        def _finalize(t: asyncio.Task) -> None:
            coord.inflight.pop(key, None)
            try:
                result = t.result()
                coord.cache[key] = result
                coord.partial.pop(key, None)
            except asyncio.CancelledError:
                # CancelledError is a BaseException; it must not escape the callback.
                pass
            except CanonicalV16NotReadyError as e:
                # Preserve 09:39-ready partial evidence so retries only fetch
                # the unresolved subset.
                if e.partial:
                    coord.partial[key] = e.partial
            except CanonicalV16ScanError as e:
                # Fatal alerts are emitted here so they survive waiter cancellation.
                _send_fatal_once(coord, key, e)
            except BaseException:
                # Any other failure is left for the scheduler/consumer to handle.
                pass

        task.add_done_callback(_finalize)

    result = await asyncio.shield(task)
    # The master bundle returned by the shared task is never handed to consumers.
    return _isolate_bundle(result)


async def compute_canonical_v16_scan(
    scan_state: V15ScanState,
    trade_date: date,
    partial: Mapping[str, TushareEarlyMarketData] | None = None,
    *,
    universe_override: tuple[str, ...] | None = None,
    clean_boards_override: Mapping[str, Sequence[tuple[str, str]]] | None = None,
    prev_closes_override: Mapping[str, float] | None = None,
    history_raw_override: Mapping[str, Mapping[str, Any] | None] | None = None,
    names_override: Mapping[str, str] | None = None,
    calendar_override: tuple[date, ...] | None = None,
) -> CanonicalV16ScanBundle:
    """Canonical V16 scan computation; does not send V16 Top10 or day-gate messages.

    The order of operations, filters, and thresholds are copied exactly from the
    original ``run_v16_scan`` implementation. Only codes with a valid 09:39 bar on
    ``trade_date`` are admitted into ``quotes``, ``trading_codes``, ``stock_data``
    and ``scanner.scan``. Partial ready evidence can be supplied so retries only
    fetch the unresolved subset.
    """
    from dataclasses import replace

    from src.strategy.lgbrank_scorer import LGBRankScorer
    from src.strategy.strategies.v16_scanner import V16Scanner

    model_path = _PROJECT_ROOT / "models" / "lgbrank_latest.txt"
    feature_path = _PROJECT_ROOT / "models" / "feature_list.json"
    scorer = LGBRankScorer(model_path, feature_path)

    scanner = V16Scanner(
        fundamentals_db=scan_state.fundamentals_db,
        concept_mapper=scan_state.concept_mapper,
        stock_filter=scan_state.stock_filter,
        scorer=scorer,
    )

    # Step 0: Get universe from board cleaning
    clean_boards_raw, universe_codes = (
        (dict(clean_boards_override), set(universe_override))
        if universe_override is not None and clean_boards_override is not None
        else scanner.get_universe()
    )
    if not universe_codes:
        raise RuntimeError("V16 scan: universe is empty after board cleaning")
    logger.info(f"V16 scan: universe = {len(universe_codes)} stocks")

    universe_list = sorted(universe_codes)
    # Canonical deterministic board order: board names ascending, members sorted by
    # (code, name). Both the bundle and the hash encode the same order the scanner
    # actually consumes, so identical inputs always produce identical scan semantics.
    clean_boards_for_scan: dict[str, list[tuple[str, str]]] = {
        board: sorted(codes) for board, codes in sorted(clean_boards_raw.items())
    }
    clean_boards = MappingProxyType(
        {board: tuple(codes) for board, codes in clean_boards_for_scan.items()}
    )

    # Merge previously ready partial evidence with a fresh pull of unresolved codes.
    partial_data: dict[str, TushareEarlyMarketData] = dict(partial) if partial else {}
    ready_partial_codes = set(partial_data.keys())
    unresolved = [c for c in universe_list if c not in ready_partial_codes]

    rt_client = scan_state.realtime_client
    new_data: dict[str, TushareEarlyMarketData] = {}
    if unresolved:
        new_data = await rt_client.batch_get_early_market_data(
            unresolved, expected_trade_date=trade_date
        )

    early_data: dict[str, TushareEarlyMarketData] = {}
    for code in universe_list:
        if code in partial_data:
            early_data[code] = partial_data[code]
        elif code in new_data:
            early_data[code] = new_data[code]

    ready_set = _ready_codes(early_data, trade_date)
    ready_codes = [code for code in universe_list if code in ready_set]
    quotes = {code: early_data[code].quote for code in ready_codes}
    early_bars_map = {code: early_data[code].early_bars for code in ready_codes}
    early_source_hashes = {code: early_data[code].source_hash for code in ready_codes}

    response_count = len(early_data)
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
        ready_partial = {code: early_data[code] for code in ready_codes}
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

    # Fetch prev_close
    calendar = calendar_override or await get_trade_calendar()
    prev_closes = (
        dict(prev_closes_override)
        if prev_closes_override is not None
        else await _fetch_prev_closes(scan_state, trade_date, calendar)
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
    hist_coverage = len(hist_raw) / len(trading_codes) if trading_codes else 0
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
        quote = quotes.get(code)
        if not quote or not quote.is_trading:
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
            sd = _build_stock_data(code, name_map.get(code, ""), quote, pc, hr, trade_date)
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
        data_error_notification=data_error_notification,
    )
    return replace(pre_bundle, _integrity_hash=_bundle_fingerprint(pre_bundle))


async def run_v16_scan(scan_state: V15ScanState) -> dict[str, Any] | None:
    """Compatibility wrapper: consume a canonical bundle and emit legacy artifacts.

    Returns the same legacy top-1 payload and preserves the original Top10/day-gate
    side effects. The canonical core is computed at most once per trade_date;
    repeated wrapper calls reuse the cached master bundle. Concurrent wrapper calls
    share exactly one publication task (data-error alert, Top10 send, DayGate schedule).
    Fatal notifications are handled by the shared compute done callback, not here.
    """
    trade_date = datetime.now(BEIJING_TZ).date()
    if scan_state.canonical_coordinator is None:
        scan_state.canonical_coordinator = _CanonicalV16Coordinator()
    coord = scan_state.canonical_coordinator
    key = _canonical_key(scan_state, trade_date)

    # get_or_compute returns an isolated consumer artifact; the master stays cached.
    bundle = await get_or_compute_canonical_v16(scan_state, trade_date)
    _verify_bundle_integrity(bundle)

    async def _publish() -> None:
        # Data-error notification (non-fatal) is sent once per key.
        if bundle.data_error_notification and key not in coord.data_errors_sent:
            title, detail = bundle.data_error_notification
            await _notify_feishu_error(title, detail)
            coord.data_errors_sent.add(key)

        # Top10 report and day-gate scheduling are sent exactly once per key.
        if key not in coord.published:
            await _notify_feishu_v16_top10(bundle.scan_result)

            recommendation_payload = _build_v16_recommendation_payload(
                bundle.scan_result, bundle.stock_data
            )

            try:
                from src.strategy.v16_day_gate_shadow import (
                    freeze_v16_day_gate_runtime,
                    freeze_v16_scan_snapshot,
                )

                shadow_cutoff = datetime.now(BEIJING_TZ)
                frozen_runtime = freeze_v16_day_gate_runtime(
                    _PROJECT_ROOT,
                    ranking_model_sha256=bundle.model_sha256,
                    ranking_feature_list_sha256=bundle.feature_list_sha256,
                    captured_at=shadow_cutoff,
                )
                if frozen_runtime is not None:
                    frozen_snapshot = freeze_v16_scan_snapshot(
                        bundle.scan_result,
                        bundle.stock_data,
                        recommendation_payload,
                        frozen_at=shadow_cutoff,
                    )
                    _schedule_v16_day_gate_shadow(frozen_snapshot, frozen_runtime)
            except Exception:
                logger.warning(
                    "V16 DayGate shadow snapshot/scheduling failed; recommendation unchanged",
                    exc_info=True,
                )

            coord.published.add(key)

    async with coord.lock:
        publish_task = coord.publish.get(key)
        if publish_task is None:
            publish_task = asyncio.create_task(_publish())
            coord.publish[key] = publish_task

            def _cleanup(t: asyncio.Task) -> None:
                coord.publish.pop(key, None)

            publish_task.add_done_callback(_cleanup)

    # Cancellation of a waiter must not cancel the shared publication work.
    await asyncio.shield(publish_task)
    return _build_v16_recommendation_payload(bundle.scan_result, bundle.stock_data)


# --- Scan scheduler ---


async def _scan_scheduler(scan_state: V15ScanState) -> None:
    """Autonomous scan scheduler. Runs from app startup, independent of iQuant.

    Time window: 09:39-10:00
    - Always runs V16 scan
    - Always pushes Feishu top-10 report + recommendation
    - Writes result to scan_state.today_recommendation
    - Does NOT check holdings or push trading signals
    """
    SCAN_WINDOW = (time(9, 39), time(10, 0))
    scan_done_date = ""

    logger.info("V16 scan scheduler started (autonomous)")

    # Initialize resources with retry
    while not scan_state.initialized:
        try:
            await init_scan_resources(scan_state)
        except Exception as e:
            logger.error(f"V16 scan resource init failed, retry in 60s: {e}")
            await asyncio.sleep(60)

    # Pre-load trade calendar
    try:
        await get_trade_calendar()
    except Exception as e:
        logger.error(f"Trade calendar load failed: {e}")
        await _notify_feishu_error(
            "交易日历加载失败",
            f"无法加载交易日历\n错误: {e}",
        )

    try:
        while True:
            now_bj = datetime.now(BEIJING_TZ)
            ex_date = now_bj.strftime("%Y-%m-%d")
            ex_time = now_bj.time().replace(second=0, microsecond=0)
            trade_date = now_bj.date()

            # --- SCAN: 09:39-10:00 ---
            if scan_done_date != ex_date and SCAN_WINDOW[0] <= ex_time <= SCAN_WINDOW[1]:
                try:
                    rec = await run_v16_scan(scan_state)
                    scan_state.today_recommendation = rec
                    scan_state.scan_error = None
                    scan_done_date = ex_date
                    scan_state.scan_done_date = ex_date

                    if rec:
                        now_str = datetime.now(BEIJING_TZ).strftime("%H:%M:%S")
                        rec_signal = {
                            "type": "buy",
                            "stock_code": rec["stock_code"],
                            "stock_name": rec["stock_name"],
                            "board_name": rec["board_name"],
                            "latest_price": rec["latest_price"],
                            "lgb_score": rec["lgb_score"],
                            "reason": f"V16推荐 (板块={rec['board_name']}, "
                            f"LGB={rec['lgb_score']:.4f})",
                            "created_at": now_str,
                        }
                        await _notify_feishu_signal(rec_signal)
                    else:
                        logger.info("V16 scan: no recommendation today")
                        await _notify_feishu_error(
                            "V16扫描结果",
                            "今日V16扫描完成，无符合条件的推荐股票",
                        )
                except CanonicalV16NotReadyError:
                    # 09:39 data not yet ready; retry within the window.
                    logger.info("V16 scan not ready at %s, will retry", ex_time)
                except CanonicalV16ScanError as e:
                    # Fatal was already emitted once by the compute done callback.
                    # Just record state so trading knows today's scan is done.
                    error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                    scan_state.scan_error = error_detail
                    scan_state.today_recommendation = None
                    scan_done_date = ex_date
                    scan_state.scan_done_date = ex_date
                    logger.error(f"V16 scan failed: {error_detail}")
                except Exception as e:
                    error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                    scan_state.scan_error = error_detail
                    scan_state.today_recommendation = None
                    scan_done_date = ex_date
                    scan_state.scan_done_date = ex_date
                    logger.error(f"V16 scan failed: {error_detail}")
                    await _notify_feishu_error("V16扫描失败", error_detail)

            # Scan deadline: after 10:00, NOT_READY becomes a fatal single audit alert
            # and the previous recommendation is cleared.
            if scan_done_date != ex_date and ex_time > SCAN_WINDOW[1]:
                scan_done_date = ex_date
                scan_state.scan_done_date = ex_date
                await _fail_not_ready_deadline(scan_state, trade_date, now_bj)

            # Adaptive sleep
            await asyncio.sleep(30 if scan_done_date == ex_date else 15)

    except asyncio.CancelledError:
        logger.info("V16 scan scheduler stopped")
    except Exception as e:
        error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        logger.critical(f"V16 scan scheduler CRASHED: {error_detail}")
        await _notify_feishu_error(
            "V16扫描调度器崩溃",
            f"扫描调度器意外退出!\n{error_detail}\n今日将无法推送V16扫描结果",
        )


def start_scan_scheduler(scan_state: V15ScanState) -> None:
    """Start the autonomous scan scheduler. Called from app.py startup."""
    if scan_state.scheduler_task and not scan_state.scheduler_task.done():
        return  # already running
    scan_state.scheduler_task = asyncio.create_task(_scan_scheduler(scan_state))


# --- Cache injection ---


def inject_cache(scan_state: V15ScanState, cache: Any) -> None:
    """Inject OSS cache and rebuild historical adapter if resources are ready."""
    from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter

    scan_state.tushare_cache = cache
    if scan_state.realtime_client:
        scan_state.historical_adapter = IQuantHistoricalAdapter(
            scan_state.realtime_client, cache=cache
        )
        logger.info("V16 scan: OSS cache injected, historical adapter rebuilt")
    else:
        logger.info("V16 scan: OSS cache stored (adapter will be built on init)")
