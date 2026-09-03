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
# 09:38 minute → Run V16 scan → push Feishu top-10 + recommendation
#              → Write result to scan_state.today_recommendation (top-1 for trading)
# Trading scheduler (in iquant_routes.py) reads scan_state to decide BUY/SELL.

from __future__ import annotations

import asyncio
import logging
import traceback
from collections.abc import Awaitable, Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

V16_AUTO_SCAN_START = time(9, 38)
V16_REALTIME_CUTOFF = time(9, 39)
V16_MANUAL_BLOCK_END = time(9, 45)

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
    scan_published_date: str = ""  # result fields were completely published by this runtime
    auto_scan_missed_date: str = ""  # automatic 09:38 slot missed without a scan
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
    scan_flight_task: asyncio.Task[dict[str, Any] | None] | None = None
    scan_flight_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    early_quotes_key: tuple[str, str] | None = None
    early_quotes_targets: tuple[str, ...] | None = None
    early_quotes_client: Any = None
    early_quotes_task: asyncio.Task[Mapping[str, Any]] | None = None
    early_quotes_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


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

    # A manual request can be awaiting the same V16-owned scan after the
    # scheduler task has been cancelled.  Detach and settle that flight before
    # closing its private realtime client or database resources.
    async with scan_state.scan_flight_lock:
        scan_flight = scan_state.scan_flight_task
        scan_state.scan_flight_task = None
    try:
        if scan_flight is not None and not scan_flight.done():
            scan_flight.cancel()
        if scan_flight is not None:
            await asyncio.gather(scan_flight, return_exceptions=True)
    except Exception as exc:
        cleanup_errors.append(exc)

    # The minute-scoped acquisition task is independent from the full scan
    # task.  Cancel and forget it before closing the V16-owned provider client.
    async with scan_state.early_quotes_lock:
        early_quotes_task = scan_state.early_quotes_task
        scan_state.early_quotes_task = None
        scan_state.early_quotes_key = None
        scan_state.early_quotes_targets = None
        scan_state.early_quotes_client = None
    try:
        if early_quotes_task is not None and not early_quotes_task.done():
            early_quotes_task.cancel()
        if early_quotes_task is not None:
            await asyncio.gather(early_quotes_task, return_exceptions=True)
    except Exception as exc:
        cleanup_errors.append(exc)

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


# --- Trade calendar ---

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


async def _fetch_v16_prev_closes(
    scan_state: V15ScanState,
    today: date,
    calendar: Sequence[date],
) -> dict[str, float]:
    """Load V16 previous closes without using V20's canonical coordinator."""

    prev_dates = [day for day in calendar if day < today]
    if not prev_dates:
        raise RuntimeError("V16 scan: no previous trading day found in calendar")
    prev_trade_date = prev_dates[-1].strftime("%Y-%m-%d")

    prev_closes: dict[str, float] = {}
    cache = scan_state.tushare_cache
    if cache and cache.is_ready:
        for code, daily in cache.get_all_codes_with_daily(prev_trade_date).items():
            close_value = daily.get("close")
            if close_value and close_value > 0:
                prev_closes[code] = float(close_value)

    if len(prev_closes) < 100:
        rt_client = scan_state.realtime_client
        if rt_client is None:
            raise RuntimeError(
                f"V16 scan: prev_close cache miss for {prev_trade_date} and no "
                "Tushare client available to fall back to."
            )
        api_closes = await rt_client.fetch_prev_closes(prev_trade_date.replace("-", ""))
        for code, close_value in api_closes.items():
            if code and len(code) == 6 and close_value:
                prev_closes.setdefault(code, float(close_value))

    if not prev_closes:
        raise RuntimeError(
            f"V16 scan: failed to get prev_close for {prev_trade_date} "
            "from both OSS cache and Tushare API"
        )
    logger.info("V16: prev_close (%s): %d stocks", prev_trade_date, len(prev_closes))
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


async def _fetch_v16_early_quotes(
    rt_client: Any,
    universe: list[str],
    *,
    realtime_deadline: datetime | None,
) -> Mapping[str, Any]:
    """Load the one V16 current-day fan-out and settle it by its deadline.

    Only the ``batch_get_early_quotes`` stage is bounded.  Once those current-day
    facts have arrived, V16's existing history, scoring, and report path may
    finish normally without competing with V20 for ``rt_min_daily`` capacity.
    """

    if realtime_deadline is None:
        return await rt_client.batch_get_early_quotes(universe)
    if realtime_deadline.tzinfo is None or realtime_deadline.utcoffset() is None:
        raise ValueError("V16 realtime deadline must be timezone-aware")
    deadline = realtime_deadline.astimezone(BEIJING_TZ)
    remaining = (deadline - datetime.now(BEIJING_TZ)).total_seconds()
    if remaining <= 0:
        raise TimeoutError("V16 rt_min_daily acquisition reached the 09:39 cutoff")

    task = asyncio.create_task(
        rt_client.batch_get_early_quotes(universe),
        name=f"v16-early-quotes-{deadline.date().isoformat()}",
    )
    deadline_timer = asyncio.create_task(
        asyncio.sleep(remaining),
        name=f"v16-early-quotes-cutoff-{deadline.date().isoformat()}",
    )
    try:
        done, _pending = await asyncio.wait(
            (task, deadline_timer),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            # ``result`` deliberately propagates the provider's own exception,
            # including a native TimeoutError, without relabelling it as our
            # wall-clock cutoff.
            return task.result()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise TimeoutError("V16 rt_min_daily acquisition did not settle before the 09:39 cutoff")
    finally:
        for pending_task in (task, deadline_timer):
            if not pending_task.done():
                pending_task.cancel()
        await asyncio.gather(task, deadline_timer, return_exceptions=True)


async def _load_v16_early_quotes(
    scan_state: V15ScanState,
    rt_client: Any,
    universe: list[str],
    *,
    realtime_deadline: datetime | None,
) -> Mapping[str, Any]:
    """Share one V16 provider acquisition within a Shanghai wall-clock minute.

    Successful and failed attempts remain cached for the provider minute, so
    sequential automatic/manual scans cannot start another full-market fan-out.
    The next minute starts a fresh acquisition; scoring and reporting are never
    cached here.
    """

    now_bj = datetime.now(BEIJING_TZ)
    acquisition_key = (now_bj.date().isoformat(), now_bj.strftime("%H:%M"))
    acquisition_targets = tuple(sorted(universe))
    async with scan_state.early_quotes_lock:
        task = scan_state.early_quotes_task
        if scan_state.early_quotes_key == acquisition_key and task is not None:
            if (
                scan_state.early_quotes_targets != acquisition_targets
                or scan_state.early_quotes_client is not rt_client
            ):
                raise RuntimeError(
                    "same-minute V16 early acquisition conflicts with the cached "
                    "target set or realtime client"
                )
        else:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            task = asyncio.create_task(
                _fetch_v16_early_quotes(
                    rt_client,
                    universe,
                    realtime_deadline=realtime_deadline,
                ),
                name=f"v16-early-acquisition-{acquisition_key[0]}-{acquisition_key[1]}",
            )
            scan_state.early_quotes_key = acquisition_key
            scan_state.early_quotes_targets = acquisition_targets
            scan_state.early_quotes_client = rt_client
            scan_state.early_quotes_task = task

    return await asyncio.shield(task)


async def run_v16_scan(
    scan_state: V15ScanState,
    *,
    realtime_deadline: datetime | None = None,
) -> dict[str, Any] | None:
    """Run one fresh, standalone V16 scan and publish its Top-10 report.

    Every invocation deliberately repeats the original V16 production path:
    build the V16 universe, fetch today's ``rt_min_daily`` evidence, load the
    previous close and history, run the scanner, and publish Top-10.

    The returned ``latest_price`` is the close of the latest current-day minute
    bar at or before 09:39, never the price at invocation time.
    """
    from src.strategy.lgbrank_scorer import LGBRankScorer
    from src.strategy.strategies.v16_scanner import V16Scanner, V16StockData

    today = datetime.now(BEIJING_TZ).date()

    scorer = LGBRankScorer(
        _PROJECT_ROOT / "models" / "lgbrank_latest.txt",
        _PROJECT_ROOT / "models" / "feature_list.json",
    )
    scanner = V16Scanner(
        fundamentals_db=scan_state.fundamentals_db,
        concept_mapper=scan_state.concept_mapper,
        stock_filter=scan_state.stock_filter,
        scorer=scorer,
    )

    clean_boards, universe_codes = scanner.get_universe()
    if not universe_codes:
        raise RuntimeError("V16 scan: universe is empty after board cleaning")
    logger.info("V16 scan: universe = %d stocks", len(universe_codes))

    rt_client = scan_state.realtime_client
    if rt_client is None:
        raise RuntimeError("V16 scan: Tushare realtime client is unavailable")
    universe_list = sorted(universe_codes)
    quotes = await _load_v16_early_quotes(
        scan_state,
        rt_client,
        universe_list,
        realtime_deadline=realtime_deadline,
    )
    quote_coverage = len(quotes) / len(universe_list) if universe_list else 0
    logger.info(
        "V16 scan: Tushare returned %d/%d quotes (coverage=%.1f%%)",
        len(quotes),
        len(universe_list),
        quote_coverage * 100,
    )

    if not quotes:
        await _notify_feishu_error(
            "9:40行情全空",
            f"Tushare batch_get_early_quotes 返回空\n请求股票数: {len(universe_list)}\n扫描中止",
        )
        raise RuntimeError(f"V16 scan: Tushare returned 0 quotes for {len(universe_list)} stocks")

    if quote_coverage < 0.8:
        await _notify_feishu_error(
            "行情覆盖率不足",
            f"请求: {len(universe_list)} 只\n"
            f"返回: {len(quotes)} 只\n"
            f"覆盖率: {quote_coverage:.1%} (阈值80%)\n"
            "Tushare API 可能异常，扫描中止",
        )
        raise RuntimeError(
            f"V16 scan: quote coverage {len(quotes)}/{len(universe_list)} "
            f"({quote_coverage:.1%}) below 80% threshold — halting"
        )

    calendar = await get_trade_calendar()
    prev_closes = await _fetch_v16_prev_closes(scan_state, today, calendar)

    trading_codes = [code for code, quote in quotes.items() if quote.is_trading]
    logger.info("V16 scan: %d stocks trading, fetching history...", len(trading_codes))

    if len(trading_codes) < len(quotes) * 0.5:
        await _notify_feishu_error(
            "交易中股票过少",
            f"行情返回: {len(quotes)} 只\n"
            f"标记交易中: {len(trading_codes)} 只\n"
            f"占比: {len(trading_codes) / len(quotes):.1%} (阈值50%)\n"
            "数据可能异常，扫描中止",
        )
        raise RuntimeError(
            f"V16 scan: only {len(trading_codes)}/{len(quotes)} stocks marked trading "
            f"({len(trading_codes) / len(quotes):.1%}) — halting"
        )

    hist_raw = await _fetch_history_ohlcv(
        scan_state.historical_adapter,
        trading_codes,
        today,
    )
    hist_coverage = len(hist_raw) / len(trading_codes) if trading_codes else 0
    logger.info(
        "V16 scan: history fetched for %d/%d stocks (coverage=%.1f%%)",
        len(hist_raw),
        len(trading_codes),
        hist_coverage * 100,
    )

    if hist_coverage < 0.8:
        await _notify_feishu_error(
            "历史数据覆盖率不足",
            f"请求: {len(trading_codes)} 只\n"
            f"返回: {len(hist_raw)} 只\n"
            f"覆盖率: {hist_coverage:.1%} (阈值80%)\n"
            "历史数据源可能异常，扫描中止",
        )
        raise RuntimeError(
            f"V16 scan: history coverage {len(hist_raw)}/{len(trading_codes)} "
            f"({hist_coverage:.1%}) below 80% threshold — halting"
        )

    name_map: dict[str, str] = {}
    if scan_state.fundamentals_db:
        try:
            fund_data = await scan_state.fundamentals_db.batch_get_fundamentals(trading_codes)
            name_map = {code: item.company_name for code, item in fund_data.items()}
        except Exception as exc:
            logger.warning("V16 scan: failed to fetch company names: %s", exc)

    stock_data: dict[str, V16StockData] = {}
    errors_no_prev_close: list[str] = []
    errors_no_hist: list[str] = []
    errors_build: list[str] = []
    skipped_new = 0

    for code in trading_codes:
        quote = quotes.get(code)
        if not quote or not quote.is_trading:
            continue

        prev_close = prev_closes.get(code)
        if not prev_close or prev_close <= 0:
            errors_no_prev_close.append(code)
            continue

        history = hist_raw.get(code)
        if not history:
            errors_no_hist.append(code)
            continue

        try:
            built = _build_stock_data(
                code,
                name_map.get(code, ""),
                quote,
                prev_close,
                history,
                today,
            )
        except RuntimeError as exc:
            errors_build.append(f"{code}: {exc}")
            continue

        if built is None:
            skipped_new += 1
            continue
        stock_data[code] = built

    total_errors = len(errors_no_prev_close) + len(errors_no_hist) + len(errors_build)
    if total_errors > 0:
        detail_lines: list[str] = []
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
            "V16 scan: %d stocks with data errors (no_prev_close=%d, no_hist=%d, build_fail=%d)",
            total_errors,
            len(errors_no_prev_close),
            len(errors_no_hist),
            len(errors_build),
        )
        await _notify_feishu_error(
            "数据缺失报警",
            f"交易中股票: {len(trading_codes)}\n"
            f"数据错误: {total_errors} 只\n"
            f"新股跳过: {skipped_new} 只\n"
            f"成功构建: {len(stock_data)} 只\n\n{detail}",
        )

    if total_errors > 0 and total_errors > len(trading_codes) * 0.2:
        raise RuntimeError(
            f"V16 scan: data error rate {total_errors}/{len(trading_codes)} "
            "exceeds 20% threshold — data source likely broken, halting"
        )

    logger.info(
        "V16 scan: built %d V16StockData (errors=%d, new_listing=%d)",
        len(stock_data),
        total_errors,
        skipped_new,
    )
    if not stock_data:
        await _notify_feishu_error(
            "无有效股票数据",
            f"交易中股票: {len(trading_codes)}\n全部数据缺失或为新股, 无法执行扫描",
        )
        raise RuntimeError("V16 scan: no valid stock data after building")

    scan_result = await scanner.scan(stock_data, clean_boards)
    await _refresh_top10_names(scan_state.fundamentals_db, scan_result.recommended)
    await _notify_feishu_v16_top10(scan_result)

    recommendation_payload = _build_v16_recommendation_payload(scan_result, stock_data)
    try:
        from src.strategy.v16_day_gate_shadow import (
            freeze_v16_day_gate_runtime,
            freeze_v16_scan_snapshot,
        )

        shadow_cutoff = datetime.now(BEIJING_TZ)
        frozen_runtime = freeze_v16_day_gate_runtime(
            _PROJECT_ROOT,
            ranking_model_sha256=scorer.model_sha256,
            ranking_feature_list_sha256=scorer.feature_list_sha256,
            captured_at=shadow_cutoff,
        )
        if frozen_runtime is not None:
            frozen_snapshot = freeze_v16_scan_snapshot(
                scan_result,
                stock_data,
                recommendation_payload,
                frozen_at=shadow_cutoff,
            )
            _schedule_v16_day_gate_shadow(frozen_snapshot, frozen_runtime)
    except Exception:
        logger.warning(
            "V16 DayGate shadow snapshot/scheduling failed; recommendation unchanged",
            exc_info=True,
        )

    return recommendation_payload


async def run_v16_scan_singleflight(
    scan_state: V15ScanState,
    *,
    realtime_deadline: datetime | None = None,
) -> dict[str, Any] | None:
    """Join one V16-owned scan while preserving fresh sequential reruns.

    The coordinator lives only on ``V15ScanState``.  It shares no task, lock,
    client, cache, or result with V20.  Overlapping automatic/manual callers
    await the same scan (and therefore publish one report); after that task is
    terminal, the next caller starts a fresh historical V16 run.
    """

    async with scan_state.scan_flight_lock:
        if not scan_state.initialized:
            raise RuntimeError("V16 scan resources are not initialized")
        task = scan_state.scan_flight_task
        if task is None or task.done():
            if realtime_deadline is None:
                operation = run_v16_scan(scan_state)
            else:
                operation = run_v16_scan(
                    scan_state,
                    realtime_deadline=realtime_deadline,
                )
            task = asyncio.create_task(operation, name="v16-scan-singleflight")
            scan_state.scan_flight_task = task

    try:
        return await asyncio.shield(task)
    finally:
        if task.done():
            async with scan_state.scan_flight_lock:
                if scan_state.scan_flight_task is task:
                    scan_state.scan_flight_task = None


# --- Scan scheduler ---


async def _scan_scheduler(scan_state: V15ScanState) -> None:
    """Autonomous scan scheduler. Runs from app startup, independent of iQuant.

    Start window: [09:38:00, 09:39:00)
    - Always runs V16 scan
    - Always pushes Feishu top-10 report + recommendation
    - Writes result to scan_state.today_recommendation
    - Does NOT check holdings or push trading signals
    """
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
            ex_time = now_bj.timetz().replace(tzinfo=None)

            # --- SCAN: only start during the dedicated 09:38 provider minute ---
            if scan_done_date != ex_date and V16_AUTO_SCAN_START <= ex_time < V16_REALTIME_CUTOFF:
                scan_done_date = ex_date
                # ``scan_done_date`` is consumed as a completion marker.  Clear
                # yesterday's payload before starting, then publish today's
                # date only after the complete result has formed.
                scan_state.today_recommendation = None
                scan_state.scan_error = None
                scan_state.scan_published_date = ""
                scan_state.auto_scan_missed_date = ""

                try:
                    rec = await run_v16_scan_singleflight(
                        scan_state,
                        realtime_deadline=datetime.combine(
                            now_bj.date(),
                            V16_REALTIME_CUTOFF,
                            tzinfo=BEIJING_TZ,
                        ),
                    )
                    scan_state.today_recommendation = rec
                    scan_state.scan_error = None
                    scan_state.scan_done_date = ex_date
                    # Publish readiness last.  Consumers must not trust the
                    # legacy date marker alone because older code wrote it
                    # before the scan had actually completed.
                    scan_state.scan_published_date = ex_date

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
                except Exception as e:
                    error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                    scan_state.scan_error = error_detail
                    scan_state.today_recommendation = None
                    logger.error(f"V16 scan failed: {error_detail}")
                    await _notify_feishu_error("V16扫描失败", error_detail)

            # A cold start or resource-init retry after 09:39 never backfills
            # the automatic scan into V20's provider window.
            if scan_done_date != ex_date and ex_time >= V16_REALTIME_CUTOFF:
                scan_done_date = ex_date
                if (
                    scan_state.scan_done_date == ex_date
                    and scan_state.scan_published_date == ex_date
                ):
                    # A fresh manual scan may have completed while this
                    # scheduler was still initializing or pre-loading the
                    # calendar.  Its last-written publication marker proves
                    # the payload transition completed; do not erase it.
                    logger.info(
                        "V16 automatic slot was missed, but a fresh manual result "
                        "was already published for %s",
                        ex_date,
                    )
                else:
                    scan_state.scan_published_date = ""
                    scan_state.auto_scan_missed_date = ex_date
                    scan_state.today_recommendation = None
                    scan_state.scan_error = f"V16 automatic 09:38 scan window missed for {ex_date}"
                    logger.warning("V16 automatic 09:38 scan window was missed for %s", ex_date)

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
