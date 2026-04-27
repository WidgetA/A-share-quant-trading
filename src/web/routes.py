# === MODULE PURPOSE ===
# API routes and page handlers for trading confirmations, backtesting, and settings.
#
# === ENDPOINTS ===
# GET  /                     - Main dashboard (HTML)
# GET  /confirm/{id}         - Confirmation page (HTML)
# GET  /api/pending          - List pending confirmations (JSON)
# GET  /api/pending/{id}     - Get confirmation details (JSON)
# POST /api/pending/{id}/submit - Submit user decision (JSON)
# GET  /api/status           - Health check (JSON)
# GET  /api/positions        - Get current positions (JSON)
#
# === MOMENTUM BACKTEST ENDPOINTS ===
# POST /api/momentum/backtest          - Run single-day momentum scan (JSON)
# POST /api/momentum/backtest-export   - Export single-day funnel as CSV download
# POST /api/momentum/combined-analysis - Run range backtest with SSE streaming
# GET  /api/momentum/monitor-status    - Get intraday monitor status (JSON)
# POST /api/momentum/tsanghi-prepare      - Start background download task (JSON)
# GET  /api/momentum/tsanghi-stream       - SSE stream for task progress (reconnectable)
# GET  /api/momentum/tsanghi-task-status  - Current task state (JSON)
# POST /api/momentum/tsanghi-cancel       - Cancel running task (JSON)
# POST /api/momentum/tsanghi-clear        - Clear FAILED state (JSON)
# GET  /api/momentum/tsanghi-cache-status - Cache data coverage status (SSE)

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from starlette.responses import StreamingResponse

from src.common.pending_store import PendingConfirmationStore
from src.data.services.download_task import DownloadState

logger = logging.getLogger(__name__)


class SubmitRequest(BaseModel):
    """Request body for submitting a confirmation result."""

    selection: Any  # Can be list[int], str ("all", "skip"), bool, etc.


class SubmitResponse(BaseModel):
    """Response for confirmation submission."""

    success: bool
    message: str


class TokenUpdateRequest(BaseModel):
    """Request body for updating API tokens."""

    token: str


def create_router() -> APIRouter:
    """Create API router with all endpoints."""
    router = APIRouter()

    def get_store(request: Request) -> PendingConfirmationStore:
        """Get pending store from app state."""
        return request.app.state.pending_store

    # ==================== HTML Pages ====================

    @router.get("/", response_class=HTMLResponse)
    async def index_page(request: Request):
        """Main dashboard with status cards + trading module."""
        from datetime import datetime
        from zoneinfo import ZoneInfo

        templates = request.app.state.templates

        # Get broker connection status
        ml_rtr = getattr(request.app.state, "ml_router", None)
        if ml_rtr and hasattr(ml_rtr, "_get_status"):
            iquant_status = ml_rtr._get_status(request.app.state)
        else:
            iquant_status = {
                "broker_configured": False,
                "holdings_count": 0,
                "available_cash": 0,
            }

        # Get cache scheduler status
        scheduler = getattr(request.app.state, "cache_scheduler", None)
        scheduler_status = (
            scheduler.get_status()
            if scheduler
            else {
                "enabled": False,
                "next_run_time": None,
                "last_run_time": None,
                "last_run_result": None,
                "last_run_message": None,
            }
        )

        # Get model training scheduler status
        model_sched = getattr(request.app.state, "model_scheduler", None)
        model_scheduler_status = (
            model_sched.get_status()
            if model_sched
            else {
                "next_run_time": None,
                "last_run_time": None,
                "last_run_result": None,
                "last_run_message": None,
                "has_full_model": False,
                "current_model": None,
                "model_training_date": None,
                "last_success_time": None,
                "last_success_label": None,
            }
        )

        today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")

        # Get daily scan status
        from src.common.config import get_daily_scan_enabled, get_recommendations_enabled

        monitor_state = getattr(request.app.state, "momentum_monitor_state", {})
        daily_scan_status = {
            "enabled": get_daily_scan_enabled(),
            "running": monitor_state.get("running", False),
            "last_scan_time": monitor_state.get("last_scan_time"),
            "last_scan_result": monitor_state.get("last_scan_result"),
            "last_scan_message": monitor_state.get("last_scan_message"),
        }

        resp = templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "iquant_status": iquant_status,
                "scheduler": scheduler_status,
                "model_scheduler": model_scheduler_status,
                "daily_scan": daily_scan_status,
                "recommendations_enabled": get_recommendations_enabled(),
                "today": today,
            },
        )
        resp.headers["Cache-Control"] = "no-cache"
        return resp

    @router.get("/confirm/{confirm_id}", response_class=HTMLResponse)
    async def confirm_page(request: Request, confirm_id: str):
        """Confirmation page for a specific pending item."""
        store = get_store(request)
        confirm = store.get_confirmation(confirm_id)
        templates = request.app.state.templates

        if not confirm:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error": "not_found",
                    "message": f"确认项 {confirm_id} 不存在或已过期",
                },
                status_code=404,
            )

        return templates.TemplateResponse(
            "confirm.html",
            {
                "request": request,
                "confirm": confirm.to_detail_dict(),
            },
        )

    @router.get("/momentum", response_class=HTMLResponse)
    async def backtest_page(request: Request):
        """Unified backtest page with 3 tabs."""
        templates = request.app.state.templates
        return templates.TemplateResponse("backtest.html", {"request": request})

    @router.get("/database", response_class=HTMLResponse)
    async def database_page(request: Request):
        """GreptimeDB dashboard embedded page."""
        templates = request.app.state.templates
        return templates.TemplateResponse("database.html", {"request": request})

    async def _greptimedb_proxy(request: Request, path: str):
        """Reverse proxy to GreptimeDB HTTP port (Docker internal network)."""
        import os

        import httpx

        host = os.environ.get("GREPTIME_HOST", "localhost")
        port = os.environ.get("GREPTIMEDB_HTTP_PORT", "4000")
        target = f"http://{host}:{port}/{path}"
        if request.url.query:
            target += f"?{request.url.query}"

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.request(
                method=request.method,
                url=target,
                headers={
                    k: v
                    for k, v in request.headers.items()
                    if k.lower() not in ("host", "connection")
                },
                content=await request.body(),
            )

        return StreamingResponse(
            iter([resp.content]),
            status_code=resp.status_code,
            headers={
                k: v
                for k, v in resp.headers.items()
                if k.lower() not in ("transfer-encoding", "connection")
            },
        )

    # Proxy GreptimeDB dashboard + all its sub-resources (JS/CSS/API)
    # Must use /dashboard path so the SPA's internal asset references work
    @router.api_route("/dashboard", methods=["GET", "POST", "PUT", "DELETE"])
    async def greptimedb_dashboard_root(request: Request):
        return await _greptimedb_proxy(request, "dashboard")

    @router.api_route("/dashboard/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
    async def greptimedb_dashboard(request: Request, path: str):
        return await _greptimedb_proxy(request, f"dashboard/{path}")

    # Proxy GreptimeDB API endpoints used by the dashboard
    @router.api_route("/health", methods=["GET"])
    async def greptimedb_health(request: Request):
        return await _greptimedb_proxy(request, "health")

    @router.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
    async def greptimedb_v1_api(request: Request, path: str):
        return await _greptimedb_proxy(request, f"v1/{path}")

    # ==================== API Endpoints ====================

    @router.get("/api/status")
    async def api_status(request: Request) -> dict:
        """Health check endpoint."""
        import os

        store = get_store(request)
        return {
            "status": "ok",
            "pending_count": len(store),
            "git_commit": os.environ.get("GIT_COMMIT", "unknown"),
            "git_branch": os.environ.get("GIT_BRANCH", "unknown"),
            "build_time": os.environ.get("BUILD_TIME", "unknown"),
        }

    @router.get("/api/debug/tasks")
    async def api_debug_tasks(request: Request) -> dict:
        """Dump all asyncio tasks with their current stack frames.

        Used to diagnose hangs where the download pipeline stops emitting
        progress events without any error, timeout, or watchdog warning.
        The response is ALSO written to the app logger so it shows up in
        docker logs — use whichever is easier to read.
        """
        import asyncio
        import io

        tasks = [t for t in asyncio.all_tasks() if not t.done()]
        out: list[dict] = []
        text_lines: list[str] = [f"=== asyncio task dump: {len(tasks)} tasks ==="]
        for t in tasks:
            name = t.get_name()
            coro = t.get_coro()
            coro_name = getattr(coro, "__qualname__", repr(coro))
            buf = io.StringIO()
            try:
                t.print_stack(file=buf)
            except Exception as e:
                buf.write(f"<print_stack failed: {e}>\n")
            stack = buf.getvalue()
            out.append({"name": name, "coro": coro_name, "stack": stack})
            text_lines.append(f"\n--- task name={name} coro={coro_name} ---\n{stack}")

        # Also log it so user can see via `docker-compose logs trading-service`
        logger.warning("\n".join(text_lines))

        return {"count": len(tasks), "tasks": out}

    @router.get("/api/pending")
    async def api_pending_list(request: Request) -> dict:
        """List all pending confirmations."""
        store = get_store(request)
        pending = store.get_pending_list()
        return {
            "pending": pending,
            "count": len(pending),
        }

    @router.get("/api/pending/{confirm_id}")
    async def api_pending_detail(request: Request, confirm_id: str) -> dict:
        """Get details of a specific confirmation."""
        store = get_store(request)
        confirm = store.get_confirmation(confirm_id)

        if not confirm:
            raise HTTPException(
                status_code=404,
                detail=f"Confirmation {confirm_id} not found or expired",
            )

        return confirm.to_detail_dict()

    @router.post("/api/pending/{confirm_id}/submit")
    async def api_submit(
        request: Request,
        confirm_id: str,
        body: SubmitRequest,
    ) -> SubmitResponse:
        """Submit user's decision for a confirmation."""
        store = get_store(request)
        confirm = store.get_confirmation(confirm_id)

        if not confirm:
            raise HTTPException(
                status_code=404,
                detail=f"Confirmation {confirm_id} not found or expired",
            )

        # Parse and validate selection based on confirmation type
        result = _parse_selection(confirm.confirm_type.value, body.selection, confirm.data)

        success = store.submit_result(confirm_id, result)

        if success:
            logger.info(f"User submitted selection for {confirm_id}: {result}")
            return SubmitResponse(success=True, message="选择已确认")
        else:
            raise HTTPException(
                status_code=400,
                detail="Failed to submit selection",
            )

    return router


def _parse_selection(confirm_type: str, selection: Any, data: dict) -> Any:
    """Parse and validate user selection based on confirmation type."""
    if confirm_type == "premarket":
        if selection == "all":
            return "all"
        if selection in ("skip", None, []):
            return []
        if isinstance(selection, list):
            max_idx = len(data.get("signals", []))
            return [i for i in selection if isinstance(i, int) and 1 <= i <= max_idx]
        return []

    elif confirm_type == "intraday":
        if isinstance(selection, bool):
            return selection
        if selection in ("yes", "y", "buy", True):
            return True
        return False

    elif confirm_type == "morning":
        if selection == "all":
            return "all"
        if selection in ("hold", None, []):
            return []
        if isinstance(selection, list):
            return selection
        return []

    elif confirm_type == "limit_up":
        if selection == "all":
            return "all"
        if selection in ("skip", None, []):
            return None
        if isinstance(selection, list):
            max_idx = len(data.get("available_stocks", []))
            return [i for i in selection if isinstance(i, int) and 1 <= i <= max_idx]
        return None

    else:
        return selection


# ==================== Momentum Backtest Pydantic Models ====================


class BacktestScanRequest(BaseModel):
    """Request body for single-day momentum scan."""

    trade_date: str  # YYYY-MM-DD format
    notify: bool = False  # Send Feishu notification


class RangeBacktestRequest(BaseModel):
    """Request body for range backtest with capital simulation."""

    start_date: str  # YYYY-MM-DD format
    end_date: str  # YYYY-MM-DD format
    initial_capital: float  # Starting capital in yuan


class TsanghiPrepareRequest(BaseModel):
    """Request body for tsanghi data pre-download."""

    start_date: str  # YYYY-MM-DD format
    end_date: str  # YYYY-MM-DD format
    force: bool = False  # Force full re-download (clears existing cache)


# ==================== Momentum Backtest Router ====================


def create_momentum_router() -> APIRouter:
    """Create router for momentum backtest and intraday monitor endpoints."""
    import asyncio
    import json
    from datetime import datetime

    router = APIRouter(tags=["momentum"])

    def _sse(data: dict) -> str:
        return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

    def _get_monitor_state(request: Request) -> dict[str, Any]:
        """Get monitor state from app.state (created at startup)."""
        return getattr(
            request.app.state,
            "momentum_monitor_state",
            {
                "running": False,
                "last_scan_time": None,
                "last_result": None,
                "today_results": [],
                "task": None,
            },
        )

    # === Tsanghi cache endpoints ===

    @router.get("/api/momentum/tsanghi-cache-status")
    async def tsanghi_cache_status(request: Request):
        """Return backtest cache state as SSE stream with per-query progress."""
        storage = getattr(request.app.state, "storage", None)
        if storage is None:

            async def _empty():
                yield f"data: {json.dumps({'type': 'result', 'status': 'empty'})}\n\n"

            return StreamingResponse(_empty(), media_type="text/event-stream")

        # Check in-memory cache first (60s TTL) — return instantly
        import time as _time

        if (
            storage._cache_status_result
            and (_time.monotonic() - storage._cache_status_ts) < storage._CACHE_STATUS_TTL
        ):

            async def _cached():
                payload = {"type": "result", **storage._cache_status_result}
                yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

            return StreamingResponse(_cached(), media_type="text/event-stream")

        progress_queue: asyncio.Queue[str | None] = asyncio.Queue()
        result_holder: list[dict] = []

        async def _on_step(msg: str) -> None:
            await progress_queue.put(msg)

        async def _run_queries() -> None:
            try:
                result = await storage.get_cache_status_streaming(on_step=_on_step)
                storage._cache_status_result = result
                storage._cache_status_ts = _time.monotonic()
                result_holder.append(result)
            except Exception as e:
                result_holder.append({"status": "error", "error": str(e)})
            finally:
                await progress_queue.put(None)  # sentinel

        async def _stream():
            task = asyncio.create_task(_run_queries())
            while True:
                msg = await progress_queue.get()
                if msg is None:
                    break
                evt = {"type": "step", "message": msg}
                yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
            await task
            r = result_holder[0] if result_holder else {"status": "error"}
            payload = {"type": "result", **r}
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        return StreamingResponse(_stream(), media_type="text/event-stream")

    # ── Download task helpers ────────────────────────────────────────────────

    def _fmt_progress(phase: str, current: int, total: int, detail: str = "") -> str:
        if phase == "init":
            return "正在初始化..."
        elif phase == "backfill":
            if detail:
                return f"回填 {current}/{total}: {detail}"
            return f"回填停牌标记: {current}/{total} 天"
        elif phase == "stock_list":
            if detail:
                return f"同步股票列表 {current}/{total}: {detail}"
            return f"同步股票列表: {current}/{total} 天"
        elif phase == "daily_check":
            if detail:
                return f"检查日线缺口: {detail}"
            return "检查日线缺口..."
        elif phase == "daily":
            if detail:
                return f"日线 {current}/{total}: {detail}"
            return f"下载日线数据: {current}/{total} 天"
        elif phase == "minute":
            if detail:
                return f"分钟线 {current}/{total}: {detail}"
            return f"分钟线 {current}/{total}"
        elif phase == "minute_backfill":
            if detail:
                return f"分钟线补全 {current}/{total}: {detail}"
            return f"分钟线补全: {current}/{total} 天"
        elif phase == "download":
            return f"下载完成: {detail}" if detail else "下载完成"
        elif phase == "post_integrity":
            if current == 0:
                return "下载后完整性检查通过"
            return f"下载后完整性检查: {current} 个警告"
        return f"{phase}: {current}/{total}"

    def _phase_to_overall(phase: str, current: int, total: int) -> float:
        if phase == "init":
            return 0.02
        elif phase == "backfill":
            return 0.02 + 0.02 * (current / total) if total > 0 else 0.02
        elif phase == "stock_list":
            return 0.04 + 0.06 * (current / total) if total > 0 else 0.04
        elif phase == "daily_check":
            return 0.10 + 0.01 * (current / total) if total > 0 else 0.10
        elif phase == "daily":
            return 0.11 + 0.09 * (current / total) if total > 0 else 0.11
        elif phase == "minute":
            return 0.20 + 0.65 * (current / total) if total > 0 else 0.20
        elif phase == "minute_backfill":
            return 0.85 + 0.10 * (current / total) if total > 0 else 0.85
        return 1.0

    async def _run_watchdog(active_dl, download_task: asyncio.Task) -> None:
        """Cancel the download task if no progress for 10 minutes."""
        NO_PROGRESS_TIMEOUT_SEC = 600
        import time as _time

        while not download_task.done():
            await asyncio.sleep(30)
            if download_task.done():
                break
            silent = _time.monotonic() - active_dl.last_event_at[0]
            if silent >= NO_PROGRESS_TIMEOUT_SEC:
                logger.error("Download watchdog: no progress for %.0fs, force-cancelling", silent)
                active_dl.cancel_event.set()
                download_task.cancel()
                active_dl.broadcast(
                    {
                        "type": "error",
                        "message": f"下载卡死: {silent:.0f}s 无进度，已强制终止",
                    }
                )
                break

    async def _run_download(active_dl, storage, pipeline, start_date, end_date) -> None:
        """Main download coroutine. Updates active_dl.state on exit."""

        def on_progress(phase: str, current: int, total: int, detail: str = "") -> None:
            if phase == "status":
                active_dl.broadcast({"type": "status", "message": detail})
                return
            if phase == "post_integrity":
                warnings = [w for w in detail.split("\n") if w] if detail else []
                active_dl.broadcast(
                    {
                        "type": "integrity_report",
                        "issues": [
                            {"level": "warning", "message": w, "count": 0, "samples": []}
                            for w in warnings
                        ],
                        "message": (
                            f"下载后完整性检查: {len(warnings)} 个警告"
                            if warnings
                            else "下载后完整性检查通过"
                        ),
                    }
                )
                return
            overall = _phase_to_overall(phase, current, total)
            active_dl.broadcast(
                {
                    "type": "progress",
                    "progress": overall,
                    "message": _fmt_progress(phase, current, total, detail),
                    "phase": phase,
                }
            )

        try:
            # Notify start (best effort)
            try:
                await pipeline.reporter.notify_download_lifecycle(
                    "started", f"日期: {active_dl.start_date} ~ {active_dl.end_date}"
                )
            except Exception:
                logger.warning("Failed to send start notice", exc_info=True)

            counts = await pipeline.download_prices(
                start_date,
                end_date,
                progress_cb=on_progress,
                cancel_event=active_dl.cancel_event,
            )
            # Success: broadcast complete event then clear active task
            active_dl.broadcast(
                {
                    "type": "complete",
                    "daily_count": counts.get("daily_count", 0),
                    "minute_count": counts.get("minute_count", 0),
                    "verified": counts.get("verified", False),
                    "verify_msg": counts.get("verify_msg", ""),
                    "cached": False,
                }
            )
            try:
                await pipeline.reporter.notify_download_lifecycle(
                    "completed",
                    f"日期: {active_dl.start_date} ~ {active_dl.end_date}",
                    f"日线: {counts.get('daily_count', 0)} 只 | "
                    f"分钟线: {counts.get('minute_count', 0)} 只",
                )
            except Exception:
                logger.warning("Failed to send completion notice", exc_info=True)

        except asyncio.CancelledError:
            active_dl.cancel_event.set()
            active_dl.broadcast({"type": "cancelled", "message": "下载已取消"})
            try:
                await pipeline.reporter.notify_download_lifecycle(
                    "cancelled",
                    f"日期: {active_dl.start_date} ~ {active_dl.end_date}",
                )
            except Exception:
                logger.warning("Failed to send cancel notice", exc_info=True)
            raise

        except Exception as e:
            err_msg = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            logger.error("Cache download failed: %s", err_msg, exc_info=True)
            active_dl.error_msg = err_msg[:300]
            active_dl.state = DownloadState.FAILED
            active_dl.broadcast({"type": "error", "message": err_msg[:300]})
            try:
                await pipeline.reporter.notify_download_lifecycle(
                    "error",
                    f"日期: {active_dl.start_date} ~ {active_dl.end_date}",
                    err_msg,
                )
            except Exception:
                logger.warning("Failed to send error notice", exc_info=True)

    # ── Routes ───────────────────────────────────────────────────────────────

    @router.post("/api/momentum/tsanghi-prepare")
    async def tsanghi_prepare(request: Request, body: TsanghiPrepareRequest):
        """Start a background download task (or report cached/already-running).

        Returns JSON immediately; progress is observed via GET tsanghi-stream.
        """
        import threading
        import time

        from src.common.config import get_tsanghi_token_source
        from src.data.services.download_task import ActiveDownload

        if get_tsanghi_token_source() == "not_configured":
            raise HTTPException(
                status_code=503,
                detail="沧海数据 token 未配置，请先在设置页面配置 tsanghi token",
            )

        try:
            start_date = datetime.strptime(body.start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(body.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误")

        storage = getattr(request.app.state, "storage", None)
        pipeline = getattr(request.app.state, "pipeline", None)
        if storage is None or pipeline is None:
            raise HTTPException(status_code=503, detail="GreptimeDB 缓存未连接")

        # Use a lock so check+start is atomic — prevents two concurrent requests
        # from both passing the "already running?" check and spawning two tasks.
        lock: asyncio.Lock = getattr(request.app.state, "download_lock", None) or asyncio.Lock()
        async with lock:
            # Already running? Reject — only one download at a time.
            existing = getattr(request.app.state, "active_download", None)
            if existing is not None and existing.state == DownloadState.RUNNING:
                raise HTTPException(
                    status_code=409,
                    detail=f"下载任务已在运行中 ({existing.start_date} ~ {existing.end_date})",
                )

            # Build the ActiveDownload object (no asyncio_task yet — set below).
            from datetime import datetime as _dt
            from zoneinfo import ZoneInfo as _ZI

            last_event_at = [time.monotonic()]
            cancel_event = threading.Event()
            active_dl = ActiveDownload(
                state=DownloadState.RUNNING,
                asyncio_task=None,  # filled in below
                watchdog_task=None,  # filled in below
                cancel_event=cancel_event,
                log_buffer=__import__("collections").deque(maxlen=300),
                last_event_at=last_event_at,
                start_date=body.start_date,
                end_date=body.end_date,
                started_at=_dt.now(_ZI("Asia/Shanghai")),
            )

            # Register immediately (while still holding the lock) so any
            # concurrent request that acquires the lock next will see it.
            request.app.state.active_download = active_dl

        # Lock released — create tasks outside the lock.
        # Feishu start notification is now inside _run_download (non-blocking).

        # Create the download and watchdog tasks — strong refs stored on active_dl.
        dl_task = asyncio.create_task(
            _run_download(active_dl, storage, pipeline, start_date, end_date)
        )
        active_dl.asyncio_task = dl_task

        wdog_task = asyncio.create_task(_run_watchdog(active_dl, dl_task))
        active_dl.watchdog_task = wdog_task

        # Cleanup when download finishes (success or cancel → clear active_download).
        def _on_done(task: asyncio.Task) -> None:
            # FAILED state was already set inside _run_download; leave it so the
            # user can see the error when they reconnect.
            if active_dl.state == DownloadState.RUNNING:
                # Completed successfully or was cancelled → no longer active.
                request.app.state.active_download = None
            else:
                # FAILED: keep active_dl for error display, but drop task refs
                # so the completed Task objects (and anything they still pin) can
                # be collected. error_msg is already copied onto active_dl.
                active_dl.asyncio_task = None
                active_dl.watchdog_task = None
            wdog_task.cancel()
            # After a bulk download Python's glibc malloc holds freed pages in
            # its internal arena and never returns them to the OS on its own.
            # gc.collect() frees Python objects; malloc_trim(0) returns empty
            # pages back to the kernel (Linux/glibc only — silently ignored
            # elsewhere).
            import gc as _gc

            _gc.collect()
            try:
                import ctypes as _ct

                _ct.CDLL("libc.so.6").malloc_trim(0)
                logger.info("malloc_trim(0): memory pages returned to OS after download")
            except Exception:
                pass  # non-Linux or non-glibc environment

        dl_task.add_done_callback(_on_done)

        return {"state": "running", "start_date": body.start_date, "end_date": body.end_date}

    @router.get("/api/momentum/tsanghi-stream")
    async def tsanghi_stream(request: Request):
        """SSE stream for download task progress.

        Safe to connect, disconnect and reconnect at any time.  On (re)connect
        the last 300 log messages are replayed so the browser catches up, then
        live events follow until the task ends or the browser disconnects.
        """

        def _sse_local(msg: dict) -> str:
            return f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"

        async def stream():
            active_dl = getattr(request.app.state, "active_download", None)
            if active_dl is None:
                yield _sse_local({"type": "state", "state": "not_started"})
                return

            # Replay buffered history so a reconnecting browser catches up.
            for msg in list(active_dl.log_buffer):
                yield _sse_local(msg)

            # Task already finished?
            if active_dl.asyncio_task is not None and active_dl.asyncio_task.done():
                yield _sse_local(
                    {
                        "type": "state",
                        "state": active_dl.state.value,
                        "error_msg": active_dl.error_msg,
                    }
                )
                return

            # Subscribe to live events.
            q = active_dl.subscribe()
            try:
                while True:
                    try:
                        msg = await asyncio.wait_for(q.get(), timeout=30)
                    except asyncio.TimeoutError:
                        yield _sse_local({"type": "heartbeat"})
                        continue
                    yield _sse_local(msg)
                    if msg.get("type") in ("complete", "error", "cancelled"):
                        break
            finally:
                active_dl.unsubscribe(q)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @router.get("/api/momentum/tsanghi-task-status")
    async def tsanghi_task_status(request: Request):
        """Return current download task state + cache scheduler result.

        All fields read from in-memory state — zero DB queries.
        """
        storage = getattr(request.app.state, "storage", None)
        db_connected = storage is not None and storage.is_ready

        # Cache scheduler last run result (in-memory)
        scheduler = getattr(request.app.state, "cache_scheduler", None)
        sched = scheduler.get_status() if scheduler else {}

        base = {
            "db_connected": db_connected,
            "scheduler_last_result": sched.get("last_run_result"),
            "scheduler_last_time": sched.get("last_run_time"),
            "scheduler_last_message": sched.get("last_run_message"),
            "scheduler_next_time": sched.get("next_run_time"),
        }

        active_dl = getattr(request.app.state, "active_download", None)
        if active_dl is None:
            return {"state": "not_started", **base}
        return {
            "state": active_dl.state.value,
            "start_date": active_dl.start_date,
            "end_date": active_dl.end_date,
            "error_msg": active_dl.error_msg,
            "started_at": active_dl.started_at.isoformat() if active_dl.started_at else None,
            **base,
        }

    @router.post("/api/momentum/tsanghi-cancel")
    async def tsanghi_cancel(request: Request):
        """Cancel the running download task."""
        active_dl = getattr(request.app.state, "active_download", None)
        if active_dl is not None and active_dl.state == DownloadState.RUNNING:
            active_dl.cancel_event.set()
            if active_dl.asyncio_task is not None:
                active_dl.asyncio_task.cancel()
            return {"success": True, "message": "取消请求已发送"}
        return {"success": False, "message": "没有正在进行的下载"}

    @router.post("/api/momentum/tsanghi-clear")
    async def tsanghi_clear(request: Request):
        """Clear a FAILED task state so a new download can be started."""
        active_dl = getattr(request.app.state, "active_download", None)
        if active_dl is not None and active_dl.state == DownloadState.FAILED:
            request.app.state.active_download = None
            return {"success": True}
        return {"success": False, "message": "没有失败的任务可清除"}

    # === Single-day momentum scan ===

    @router.post("/api/momentum/backtest")
    async def run_backtest(request: Request, body: BacktestScanRequest) -> dict:
        """Run ML strategy scan for a specific date using backtest cache."""
        from src.common.feishu_bot import FeishuBot
        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.ml_strategy_service import (
            MinuteDataMissingError,
            run_ml_backtest,
        )

        try:
            trade_date = datetime.strptime(body.trade_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"日期格式错误: {body.trade_date}，请使用 YYYY-MM-DD",
            )

        storage = getattr(request.app.state, "storage", None)
        if not storage or not storage.is_ready:
            raise HTTPException(status_code=400, detail="请先预下载回测数据")

        concept_mapper = LocalConceptMapper()

        try:
            try:
                result = await run_ml_backtest(
                    storage=storage,
                    concept_mapper=concept_mapper,
                    trade_date=trade_date,
                )
            except MinuteDataMissingError as e:
                return {
                    "success": False,
                    "trade_date": body.trade_date,
                    "error": str(e),
                }
            if not result.recommended and not result.all_scored:
                ds = trade_date.strftime("%Y-%m-%d")
                if result.skip_reason == "no_daily_data":
                    msg = f"沧海缓存中无 {ds} 的日线数据（非交易日或数据缺失，请尝试补充下载）"
                elif result.skip_reason:
                    msg = f"{ds} 日线有数据但无有效候选股 ({result.skip_reason})"
                else:
                    msg = f"{ds} 扫描完成但无推荐结果"
                return {
                    "success": False,
                    "trade_date": body.trade_date,
                    "error": msg,
                }

            rec = result.recommended
            response_data: dict[str, Any] = {
                "success": True,
                "trade_date": body.trade_date,
                "hot_boards": result.hot_board_count,
                "final_candidates": result.final_candidates,
                "funnel": [
                    {"key": s.key, "label": s.label, "count": s.count} for s in result.funnel
                ],
                "scored_stocks": [
                    {
                        "stock_code": s.stock_code,
                        "stock_name": s.stock_name,
                        "board_name": s.board_name,
                        "ml_score": round(s.ml_score, 4),
                        "early_gain_pct": round(s.early_gain_pct, 2),
                        "turnover_amp": round(s.turnover_amp, 2),
                        "latest_price": round(s.latest_price, 2),
                    }
                    for s in result.all_scored
                ],
                "recommended_stock": {
                    "stock_code": rec.stock_code,
                    "stock_name": rec.stock_name,
                    "board_name": rec.board_name,
                    "ml_score": round(rec.ml_score, 4),
                    "early_gain_pct": round(rec.early_gain_pct, 2),
                    "turnover_amp": round(rec.turnover_amp, 2),
                    "open_price": round(rec.open_price, 2),
                    "prev_close": round(rec.prev_close, 2),
                    "latest_price": round(rec.latest_price, 2),
                }
                if rec
                else None,
            }

            # Send Feishu if requested
            if body.notify and rec:
                bot = FeishuBot()
                if bot.is_configured():
                    await bot.send_ml_top5_report(result)
                    response_data["feishu_sent"] = True

            return response_data

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"ML backtest error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"回测出错: {str(e)}")

    # === Funnel export (CSV) ===

    @router.post("/api/momentum/backtest-export")
    async def export_backtest_funnel(request: Request, body: BacktestScanRequest):
        """Run single-day ML scan and return per-layer funnel as CSV download."""
        import csv
        import io
        from datetime import datetime

        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.ml_strategy_service import run_ml_backtest

        try:
            trade_date = datetime.strptime(body.trade_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误")

        storage = getattr(request.app.state, "storage", None)
        if not storage or not storage.is_ready:
            raise HTTPException(status_code=400, detail="请先预下载回测数据")

        result = await run_ml_backtest(
            storage=storage,
            concept_mapper=LocalConceptMapper(),
            trade_date=trade_date,
        )

        # Build code → name/board lookup from scored stocks
        code_name: dict[str, str] = {}
        code_board: dict[str, str] = {}
        for s in result.all_scored:
            code_name[s.stock_code] = s.stock_name
            code_board[s.stock_code] = s.board_name

        # Collect all codes and per-layer membership
        layer_keys = [s.key for s in result.funnel]
        layer_codes = {s.key: set(s.codes) for s in result.funnel}

        # ML scores for final candidates
        ml_scores: dict[str, float] = {s.stock_code: s.ml_score for s in result.all_scored}

        # Union of all codes across all layers
        all_codes: set[str] = set()
        for codes in layer_codes.values():
            all_codes.update(codes)

        # Build CSV
        buf = io.StringIO()
        writer = csv.writer(buf)
        header = ["stock_code", "stock_name", "board"] + layer_keys + ["ml_score"]
        writer.writerow(header)

        for code in sorted(all_codes):
            row = [
                code,
                code_name.get(code, ""),
                code_board.get(code, ""),
            ]
            for key in layer_keys:
                row.append("1" if code in layer_codes[key] else "")
            row.append(f"{ml_scores[code]:.4f}" if code in ml_scores else "")
            writer.writerow(row)

        csv_bytes = buf.getvalue().encode("utf-8-sig")  # BOM for Excel
        filename = f"funnel_{body.trade_date}.csv"
        return StreamingResponse(
            iter([csv_bytes]),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # === Range backtest with capital simulation ===

    @router.post("/api/momentum/combined-analysis")
    async def run_combined_analysis(request: Request, body: RangeBacktestRequest):
        """Run momentum range backtest with capital simulation (SSE streaming)."""
        import json
        import math
        from datetime import datetime

        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.ml_strategy_service import (
            MinuteDataMissingError,
            run_ml_backtest,
        )

        try:
            start_date = datetime.strptime(body.start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(body.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD")

        if end_date <= start_date:
            raise HTTPException(status_code=400, detail="结束日期必须晚于起始日期")

        if body.initial_capital < 1000:
            raise HTTPException(status_code=400, detail="起始资金不能低于 1000 元")

        storage = getattr(request.app.state, "storage", None)
        if not storage or not storage.is_ready:
            raise HTTPException(status_code=400, detail="请先预下载回测数据")

        concept_mapper = LocalConceptMapper()

        async def event_stream():
            def sse(data: dict) -> str:
                return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

            try:
                from datetime import timedelta

                cal_end = end_date + timedelta(days=10)
                trading_days_all = await _get_trading_calendar(start_date, cal_end)
                if not trading_days_all:
                    yield sse({"type": "error", "message": "所选日期范围内无交易日"})
                    return

                days_in_range = [d for d in trading_days_all if start_date <= d <= end_date]
                if not days_in_range:
                    yield sse({"type": "error", "message": "所选日期范围内无交易日"})
                    return

                # Build T+1 map
                next_day_map = {}
                for i, d in enumerate(trading_days_all):
                    if d > end_date:
                        break
                    if d >= start_date and i + 1 < len(trading_days_all):
                        next_day_map[d] = trading_days_all[i + 1]

                if len(days_in_range) > 250:
                    days_in_range = days_in_range[:250]
                    yield sse(
                        {
                            "type": "warning",
                            "message": f"已截断至前 250 个交易日"
                            f" ({days_in_range[0]} ~ {days_in_range[-1]})",
                        }
                    )

                yield sse(
                    {
                        "type": "init",
                        "total_days": len(days_in_range),
                        "start_date": str(days_in_range[0]),
                        "end_date": str(days_in_range[-1]),
                        "initial_capital": body.initial_capital,
                    }
                )

                capital = body.initial_capital
                day_results: list[dict] = []

                for day_idx, trade_date in enumerate(days_in_range):
                    next_trade_date = next_day_map.get(trade_date)
                    if not next_trade_date:
                        continue

                    yield sse(
                        {
                            "type": "progress",
                            "day": day_idx + 1,
                            "total": len(days_in_range),
                            "trade_date": str(trade_date),
                        }
                    )

                    try:
                        try:
                            scan_result = await run_ml_backtest(
                                storage=storage,
                                concept_mapper=concept_mapper,
                                trade_date=trade_date,
                            )
                        except MinuteDataMissingError as e:
                            day_results.append(
                                {
                                    "trade_date": str(trade_date),
                                    "has_trade": False,
                                    "skip_reason": str(e),
                                    "capital": round(capital, 2),
                                }
                            )
                            yield sse(
                                {
                                    "type": "day_skip",
                                    "trade_date": str(trade_date),
                                    "reason": str(e),
                                }
                            )
                            await asyncio.sleep(0.05)
                            continue

                        # Empty result means no data for that date
                        if not scan_result.recommended and not scan_result.all_scored:
                            date_key = trade_date.strftime("%Y-%m-%d")
                            reason = scan_result.skip_reason or "no_data"
                            skip_msg = (
                                f"沧海缓存中无 {date_key} 的日线数据"
                                if reason == "no_daily_data"
                                else f"{date_key} 无有效候选股 ({reason})"
                            )
                            day_results.append(
                                {
                                    "trade_date": str(trade_date),
                                    "has_trade": False,
                                    "skip_reason": skip_msg,
                                    "capital": round(capital, 2),
                                }
                            )
                            yield sse(
                                {
                                    "type": "day_skip",
                                    "trade_date": str(trade_date),
                                    "reason": skip_msg,
                                }
                            )
                            await asyncio.sleep(0.05)
                            continue

                        rec = scan_result.recommended

                        # Build day backtest entry
                        day_backtest: dict = {
                            "trade_date": str(trade_date),
                            "has_trade": False,
                            "capital": round(capital, 2),
                            "funnel": [
                                {"key": s.key, "label": s.label, "count": s.count}
                                for s in scan_result.funnel
                            ],
                        }

                        if rec:
                            buy_price = rec.latest_price
                            if buy_price <= 0:
                                buy_price = rec.open_price

                            # Get T+1 open from cache
                            next_date_key = next_trade_date.strftime("%Y-%m-%d")
                            next_day_data = await storage.get_daily(
                                rec.stock_code,
                                next_date_key,
                            )
                            sell_price_val = (
                                float(next_day_data.open)
                                if next_day_data and next_day_data.open
                                else 0.0
                            )

                            if buy_price > 0 and sell_price_val > 0:
                                lots = math.floor(capital / (buy_price * 100))
                                if lots > 0:
                                    buy_amount = lots * 100 * buy_price
                                    buy_commission = max(buy_amount * 0.003, 5.0)
                                    buy_transfer = buy_amount * 0.00001
                                    total_buy_cost = buy_amount + buy_commission + buy_transfer

                                    while total_buy_cost > capital and lots > 0:
                                        lots -= 1
                                        buy_amount = lots * 100 * buy_price
                                        buy_commission = max(buy_amount * 0.003, 5.0)
                                        buy_transfer = buy_amount * 0.00001
                                        total_buy_cost = buy_amount + buy_commission + buy_transfer

                                if lots > 0:
                                    sell_amount = lots * 100 * sell_price_val
                                    sell_commission = max(sell_amount * 0.003, 5.0)
                                    sell_transfer = sell_amount * 0.00001
                                    sell_stamp = sell_amount * 0.0005
                                    net_sell = (
                                        sell_amount - sell_commission - sell_transfer - sell_stamp
                                    )

                                    capital_before = capital
                                    capital = capital - total_buy_cost + net_sell
                                    trade_profit = net_sell - total_buy_cost
                                    trade_return_pct = (
                                        trade_profit / total_buy_cost * 100
                                        if total_buy_cost > 0
                                        else 0
                                    )

                                    day_backtest = {
                                        "trade_date": str(trade_date),
                                        "has_trade": True,
                                        "stock_code": rec.stock_code,
                                        "stock_name": rec.stock_name,
                                        "board_name": rec.board_name,
                                        "buy_price": round(buy_price, 2),
                                        "sell_price": round(sell_price_val, 2),
                                        "sell_date": str(next_trade_date),
                                        "lots": lots,
                                        "profit": round(trade_profit, 2),
                                        "return_pct": round(trade_return_pct, 2),
                                        "capital_before": round(capital_before, 2),
                                        "capital": round(capital, 2),
                                        "ml_score": round(rec.ml_score, 4),
                                        "funnel": [
                                            {"key": s.key, "label": s.label, "count": s.count}
                                            for s in scan_result.funnel
                                        ],
                                    }
                                else:
                                    day_backtest["skip_reason"] = (
                                        f"资金不足 (需 {buy_price * 100:.0f} 元/手)"
                                    )
                                    day_backtest["stock_code"] = rec.stock_code
                                    day_backtest["stock_name"] = rec.stock_name
                            else:
                                reason_parts = []
                                if buy_price <= 0:
                                    reason_parts.append("无买入价")
                                if sell_price_val <= 0:
                                    reason_parts.append("无次日开盘卖出价")
                                day_backtest["skip_reason"] = "、".join(reason_parts)
                                day_backtest["stock_code"] = rec.stock_code
                                day_backtest["stock_name"] = rec.stock_name
                        else:
                            day_backtest["skip_reason"] = "无推荐"

                        day_backtest["capital"] = round(capital, 2)
                        day_results.append(day_backtest)

                        yield sse(
                            {
                                "type": "day_result",
                                "trade_date": str(trade_date),
                                "backtest": day_backtest,
                            }
                        )
                        await asyncio.sleep(0.05)

                    except Exception as e:
                        logger.error(
                            f"Range backtest error on {trade_date}: {e}",
                            exc_info=True,
                        )
                        day_results.append(
                            {
                                "trade_date": str(trade_date),
                                "has_trade": False,
                                "skip_reason": f"出错: {str(e)[:60]}",
                                "capital": round(capital, 2),
                            }
                        )
                        yield sse(
                            {
                                "type": "day_skip",
                                "trade_date": str(trade_date),
                                "reason": f"出错: {str(e)[:60]}",
                            }
                        )
                        await asyncio.sleep(0.05)
                        continue

                # === Summary ===
                trade_results = [d for d in day_results if d.get("has_trade")]
                wins = [d for d in trade_results if d["profit"] > 0]
                losses = [d for d in trade_results if d["profit"] < 0]
                total_return_pct = (capital - body.initial_capital) / body.initial_capital * 100

                backtest_summary = {
                    "initial_capital": body.initial_capital,
                    "final_capital": round(capital, 2),
                    "total_return_pct": round(total_return_pct, 2),
                    "total_days": len(days_in_range),
                    "trade_days": len(trade_results),
                    "skip_days": len(days_in_range) - len(trade_results),
                    "win_days": len(wins),
                    "lose_days": len(losses),
                    "even_days": len(trade_results) - len(wins) - len(losses),
                    "win_rate": round(
                        len(wins) / len(trade_results) * 100 if trade_results else 0,
                        1,
                    ),
                    "max_win": round(max((d["profit"] for d in trade_results), default=0), 2),
                    "max_loss": round(min((d["profit"] for d in trade_results), default=0), 2),
                }

                yield sse(
                    {
                        "type": "complete",
                        "days_processed": len(days_in_range),
                        "backtest_summary": backtest_summary,
                    }
                )

            except Exception as e:
                logger.error(f"Range backtest error: {e}", exc_info=True)
                yield sse({"type": "error", "message": f"分析出错: {str(e)}"})

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # === Intraday monitor endpoints ===

    @router.get("/api/momentum/monitor-status")
    async def get_monitor_status(request: Request) -> dict:
        """Get intraday monitor status and latest results."""
        monitor_state = _get_monitor_state(request)
        return {
            "running": monitor_state["running"],
            "last_scan_time": monitor_state["last_scan_time"],
            "last_scan_result": monitor_state.get("last_scan_result"),
            "last_scan_message": monitor_state.get("last_scan_message"),
            "today_results": monitor_state["today_results"],
        }

    @router.post("/api/momentum/monitor/start")
    async def start_monitor(request: Request) -> dict:
        """Manually start the intraday monitor."""
        monitor_state = _get_monitor_state(request)
        if monitor_state["running"]:
            return {"success": True, "message": "监控已在运行中"}

        monitor_state["_app_state"] = request.app.state
        task = asyncio.create_task(_run_intraday_monitor(monitor_state))
        monitor_state["task"] = task
        return {"success": True, "message": "监控已启动"}

    @router.post("/api/momentum/monitor/stop")
    async def stop_monitor(request: Request) -> dict:
        """Manually stop the intraday monitor."""
        monitor_state = _get_monitor_state(request)
        task = monitor_state.get("task")
        if task and not task.done():
            task.cancel()
        monitor_state["running"] = False
        monitor_state["task"] = None
        return {"success": True, "message": "监控已停止"}

    @router.post("/api/momentum/monitor/trigger")
    async def trigger_monitor_scan(request: Request) -> dict:
        """Manually trigger a momentum scan right now (any time of day)."""
        from zoneinfo import ZoneInfo

        monitor_state = _get_monitor_state(request)
        storage = getattr(request.app.state, "storage", None)
        scan_time_str = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M")
        monitor_state["last_scan_time"] = scan_time_str
        try:
            result = await _execute_monitor_scan(monitor_state, storage=storage)
            if result is None:
                monitor_state["last_scan_result"] = "no_result"
                monitor_state["last_scan_message"] = "无合格股票"
                return {"success": False, "message": "扫描未产生结果"}
            monitor_state["last_scan_result"] = "success"
            candidates = result.get("final_candidates", 0)
            rec = result.get("recommended_stock")
            rec_name = rec["stock_name"] if rec else "无"
            monitor_state["last_scan_message"] = f"{candidates}只候选, 推荐: {rec_name}"
            await _persist_scan_status(monitor_state, "manual")
            return {"success": True, "result": result}
        except Exception as e:
            monitor_state["last_scan_result"] = "failed"
            monitor_state["last_scan_message"] = str(e)[:100]
            await _persist_scan_status(monitor_state, "manual")
            logger.error(f"Manual scan trigger error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"扫描失败: {str(e)}")

    # === Cache scheduler endpoints ===

    @router.get("/api/cache/status")
    async def cache_scheduler_status(request: Request) -> dict:
        """Return cache scheduler status and coverage info."""
        storage = getattr(request.app.state, "storage", None)
        scheduler_task = getattr(request.app.state, "cache_scheduler_task", None)
        scheduler = getattr(request.app.state, "cache_scheduler", None)

        scheduler_info = (
            scheduler.get_status()
            if scheduler
            else {
                "enabled": False,
                "next_run_time": None,
                "last_run_time": None,
                "last_run_result": None,
                "last_run_message": None,
            }
        )

        if not storage or not storage.is_ready:
            return {
                "cache_ready": False,
                "start_date": None,
                "end_date": None,
                "daily_stock_count": 0,
                "minute_stock_count": 0,
                "scheduler_running": scheduler_task is not None and not scheduler_task.done()
                if scheduler_task
                else False,
                **scheduler_info,
            }

        db_start, db_end = await storage.get_date_range()
        daily_count = await storage.get_daily_stock_count()
        minute_count = await storage.get_minute_stock_count()
        return {
            "cache_ready": True,
            "start_date": str(db_start) if db_start else None,
            "end_date": str(db_end) if db_end else None,
            "daily_stock_count": daily_count,
            "minute_stock_count": minute_count,
            "scheduler_running": scheduler_task is not None and not scheduler_task.done()
            if scheduler_task
            else False,
            **scheduler_info,
        }

    @router.post("/api/cache/trigger")
    async def trigger_cache_fill(request: Request) -> dict:
        """Manually trigger the cache gap-fill process."""
        from src.data.services.cache_scheduler import CacheScheduler

        storage = getattr(request.app.state, "storage", None)
        if not storage:
            raise HTTPException(503, "GreptimeDB 缓存未连接")

        if getattr(request.app.state, "cache_fill_running", False):
            raise HTTPException(409, "缓存补全正在运行中，请等待完成后再试")

        request.app.state.cache_fill_running = True
        try:
            scheduler = CacheScheduler(request.app.state)
            result = await scheduler.check_and_fill_gaps()
            return {"success": True, **result}
        finally:
            request.app.state.cache_fill_running = False

    return router


# ==================== Helper Functions ====================


def _scan_result_to_recs(date_str: str, scan_result, n: int = 10) -> list[dict]:
    """Convert MLScanResult.all_scored to recommendation dicts."""

    def _f(val):
        return round(float(val), 4) if val is not None else None

    top = scan_result.all_scored[:n]
    return [
        {
            "trade_date": date_str,
            "rank": i + 1,
            "stock_code": s.stock_code,
            "stock_name": s.stock_name,
            "board_name": s.board_name,
            "ml_score": _f(s.ml_score),
            "open_price": _f(s.open_price),
            "prev_close": _f(s.prev_close),
            "latest_price": _f(s.latest_price),
            "gain_pct": _f(s.gain_pct),
            "early_gain_pct": _f(s.early_gain_pct),
            "turnover_amp": _f(s.turnover_amp),
            "final_candidates": scan_result.final_candidates,
        }
        for i, s in enumerate(top)
    ]


async def _compute_ml_scan(date_str: str, storage):
    """Compute ML scan on-demand. Returns (recs_list, MLScanResult | None).

    Two paths:
    - Past dates: GreptimeDB cache via strategy service (fast, ~1-3s)
    - Today: Tushare rt_min_daily raw bars via strategy service (slow, ~30-60s)
    """
    from datetime import date, datetime
    from zoneinfo import ZoneInfo

    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.ml_strategy_service import run_ml_backtest, run_ml_live

    beijing_tz = ZoneInfo("Asia/Shanghai")
    now_bj = datetime.now(beijing_tz)
    today_str = now_bj.strftime("%Y-%m-%d")
    is_today = date_str == today_str
    concept_mapper = LocalConceptMapper()

    if not is_today:
        # --- Past date: use GreptimeDB cache ---
        if not storage or not getattr(storage, "is_ready", False):
            raise ValueError("GreptimeDB 缓存未就绪，无法计算历史推荐")

        trade_date = date.fromisoformat(date_str)
        result = await run_ml_backtest(
            storage=storage,
            concept_mapper=concept_mapper,
            trade_date=trade_date,
        )
        if not result.recommended and not result.all_scored:
            return [], None
        recs = _scan_result_to_recs(date_str, result, n=10)
        return recs, result

    # --- Today: live Tushare quotes ---
    from src.data.clients.tushare_realtime import get_tushare_trade_calendar

    today_s = now_bj.strftime("%Y-%m-%d")
    cal_dates = await get_tushare_trade_calendar(today_s, today_s)
    if today_s not in cal_dates:
        raise ValueError(f"今天非交易日（{now_bj.strftime('%m/%d')}），A股不开市")

    if now_bj.hour < 9 or (now_bj.hour == 9 and now_bj.minute < 39):
        raise ValueError("今日扫描需在 09:39 之后执行（早盘数据尚未就绪）")

    from src.common.config import get_tushare_token
    from src.data.clients.tushare_realtime import TushareRealtimeClient

    tushare_token = get_tushare_token()
    tushare = TushareRealtimeClient(token=tushare_token)
    await tushare.start()
    try:
        result = await run_ml_live(
            realtime_client=tushare,
            storage=storage,
            concept_mapper=concept_mapper,
        )
    finally:
        await tushare.stop()

    if not result.recommended and not result.all_scored:
        return [], None
    recs = _scan_result_to_recs(date_str, result, n=10)
    return recs, result


def _calc_net_return_pct(buy_price: float, sell_price: float) -> float:
    """Calculate net return percentage after transaction costs (assuming 1 lot = 100 shares)."""
    shares = 100
    buy_amount = shares * buy_price
    buy_commission = max(buy_amount * 0.003, 5.0)
    buy_transfer = buy_amount * 0.00001
    total_buy_cost = buy_amount + buy_commission + buy_transfer

    sell_amount = shares * sell_price
    sell_commission = max(sell_amount * 0.003, 5.0)
    sell_transfer = sell_amount * 0.00001
    sell_stamp = sell_amount * 0.0005
    net_sell = sell_amount - sell_commission - sell_transfer - sell_stamp

    return (net_sell - total_buy_cost) / total_buy_cost * 100 if total_buy_cost > 0 else 0.0


async def _get_trading_calendar(start_date, end_date) -> list:
    """Get trading days via Tushare trade_cal, fallback to weekdays."""
    from datetime import datetime as dt
    from datetime import timedelta

    try:
        from src.data.clients.tushare_realtime import get_tushare_trade_calendar

        sd = start_date.strftime("%Y-%m-%d")
        ed = end_date.strftime("%Y-%m-%d")
        date_strs = await get_tushare_trade_calendar(sd, ed)
        days = sorted(
            dt.strptime(d, "%Y-%m-%d").date()
            for d in date_strs
            if start_date <= dt.strptime(d, "%Y-%m-%d").date() < end_date
        )
        if days:
            logger.info(f"Tushare trading calendar: {len(days)} days in [{start_date}, {end_date})")
            return days
        logger.warning("Tushare trading calendar returned no dates in range")
    except Exception as e:
        logger.warning(f"Tushare trade_cal failed: {e}")

    # Fallback: weekdays
    logger.warning("Falling back to weekday generation for trading calendar")
    days = []
    current = start_date
    while current < end_date:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


async def _execute_monitor_scan(state: dict, storage: Any = None) -> dict | None:
    """Execute a single ML scan using Tushare + MLScanner.

    Returns result_entry dict on success, None on failure.
    """
    import time as time_module
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    from src.common.config import get_tushare_token
    from src.common.feishu_bot import FeishuBot
    from src.data.clients.tushare_realtime import TushareRealtimeClient
    from src.data.sources.local_concept_mapper import LocalConceptMapper
    from src.strategy.ml_strategy_service import run_ml_live

    beijing_tz = ZoneInfo("Asia/Shanghai")
    logger.info("Monitor scan starting (ML scanner)")

    start_time = time_module.monotonic()

    # Quick trading day check (weekends + holidays)
    now_bj = datetime.now(beijing_tz)
    if now_bj.weekday() >= 5:
        day_name = "周六" if now_bj.weekday() == 5 else "周日"
        logger.warning(f"Monitor: 今天是{day_name}，A股不开市")
        raise RuntimeError(f"今天是{day_name}，A股不开市，无法扫描")
    try:
        cal = await _get_trading_calendar(now_bj.date(), now_bj.date() + timedelta(days=1))
        if not cal:
            logger.warning(f"Monitor: {now_bj.date()} is a holiday, A股不开市")
            raise RuntimeError(f"今天是节假日({now_bj.date()})，A股不开市，无法扫描")
    except RuntimeError:
        raise
    except Exception as e:
        logger.warning(f"Trade calendar check failed, proceeding: {e}")

    if not (storage and getattr(storage, "is_ready", False)):
        raise RuntimeError("GreptimeDB 缓存未连接，无法进行扫描。请先下载回测数据。")

    tushare_token = get_tushare_token()
    tushare = TushareRealtimeClient(token=tushare_token)
    await tushare.start()
    try:
        concept_mapper = LocalConceptMapper()
        scan_result = await run_ml_live(
            realtime_client=tushare,
            storage=storage,
            concept_mapper=concept_mapper,
        )
    finally:
        await tushare.stop()

    elapsed = time_module.monotonic() - start_time
    scan_time = datetime.now(beijing_tz)

    rec = scan_result.recommended
    result_entry = {
        "scan_time": scan_time.strftime("%Y-%m-%d %H:%M"),
        "hot_boards": scan_result.hot_board_count,
        "final_candidates": scan_result.final_candidates,
        "funnel": [{"key": s.key, "label": s.label, "count": s.count} for s in scan_result.funnel],
        "elapsed_seconds": round(elapsed, 1),
        "scored_top5": [
            {
                "stock_code": s.stock_code,
                "stock_name": s.stock_name,
                "board_name": s.board_name,
                "ml_score": round(s.ml_score, 4),
                "early_gain_pct": round(s.early_gain_pct, 2),
            }
            for s in scan_result.all_scored[:5]
        ],
        "recommended_stock": {
            "stock_code": rec.stock_code,
            "stock_name": rec.stock_name,
            "board_name": rec.board_name,
            "ml_score": round(rec.ml_score, 4),
            "early_gain_pct": round(rec.early_gain_pct, 2),
            "turnover_amp": round(rec.turnover_amp, 2),
            "latest_price": round(rec.latest_price, 2),
        }
        if rec
        else None,
    }

    state["last_result"] = result_entry
    # last_scan_time is set by the caller (_run_intraday_monitor / trigger endpoint)
    # to ensure it's always set even on failure.
    # Manual trigger also sets it here as fallback:
    if not state.get("last_scan_time"):
        state["last_scan_time"] = scan_time.strftime("%Y-%m-%d %H:%M")
    state["today_results"].append(result_entry)

    # Send Feishu notification
    bot = FeishuBot()
    if bot.is_configured():
        await bot.send_ml_top5_report(scan_result, scan_time=scan_time)
        logger.info("Monitor: Feishu ML scan report sent")
    else:
        logger.warning(
            "Monitor: Feishu bot NOT configured — set FEISHU_APP_ID, "
            "FEISHU_APP_SECRET, FEISHU_CHAT_ID environment variables"
        )

    logger.info(
        f"Monitor scan complete: {scan_result.final_candidates} candidates, {elapsed:.1f}s elapsed"
    )
    return result_entry


async def _persist_scan_status(state: dict, trigger: str) -> None:
    """Write daily scan status to GreptimeDB scheduler_log."""
    try:
        app_state = state.get("_app_state")
        storage = getattr(app_state, "storage", None) if app_state else None
        if storage and getattr(storage, "is_ready", False):
            await storage.log_scheduler_run(
                "daily_scan",
                trigger,
                state.get("last_scan_result") or "unknown",
                state.get("last_scan_message") or "",
            )
    except Exception:
        logger.warning("Failed to persist scan status to DB", exc_info=True)


async def _restore_scan_status(state: dict) -> None:
    """Restore daily scan status from GreptimeDB on startup."""
    try:
        app_state = state.get("_app_state")
        storage = getattr(app_state, "storage", None) if app_state else None
        if storage and getattr(storage, "is_ready", False):
            last = await storage.get_last_scheduler_run("daily_scan")
            if last:
                state["last_scan_time"] = last["time"]
                state["last_scan_result"] = last["result"]
                state["last_scan_message"] = last["message"]
                logger.info(
                    "Monitor: restored last scan from DB: %s %s",
                    last["time"],
                    last["result"],
                )
    except Exception:
        logger.warning("Failed to restore scan status from DB", exc_info=True)


async def _run_intraday_monitor(state: dict) -> None:
    """Background task: intraday momentum monitor.

    Runs every trading day at ~9:40, executes momentum scan, sends Feishu notification.
    """
    import asyncio
    from datetime import datetime, time, timedelta
    from zoneinfo import ZoneInfo

    beijing_tz = ZoneInfo("Asia/Shanghai")
    SCAN_TIME = time(9, 39)

    state["running"] = True

    # Restore last scan status from DB so dashboard shows history after restart
    await asyncio.sleep(30)  # wait for storage to be ready
    await _restore_scan_status(state)
    logger.info("Intraday momentum monitor started")

    try:
        while state["running"]:
            now = datetime.now(beijing_tz)
            current_time = now.time()

            # Check if daily scan is enabled
            from src.common.config import get_daily_scan_enabled

            if not get_daily_scan_enabled():
                state["scan_enabled"] = False
                await asyncio.sleep(30)
                continue
            state["scan_enabled"] = True

            # Skip non-trading days (weekends + holidays)
            if now.weekday() >= 5:
                await asyncio.sleep(3600)
                continue
            try:
                cal = await _get_trading_calendar(now.date(), now.date() + timedelta(days=1))
                if not cal:
                    logger.info(f"Monitor: {now.date()} is not a trading day, skipping")
                    await asyncio.sleep(3600)
                    continue
            except Exception as e:
                logger.warning(f"Trade calendar check failed, proceeding: {e}")

            # Before scan time — wait
            if current_time < SCAN_TIME:
                delta = datetime.combine(now.date(), SCAN_TIME) - datetime.combine(
                    now.date(), current_time
                )
                wait_secs = max(delta.total_seconds(), 10)
                logger.debug(f"Monitor waiting {wait_secs:.0f}s until {SCAN_TIME}")
                await asyncio.sleep(min(wait_secs, 60))
                continue

            # After scan window — wait for tomorrow
            if current_time > time(9, 50):
                tomorrow = now + timedelta(days=1)
                target = datetime.combine(tomorrow.date(), time(9, 25), tzinfo=beijing_tz)
                wait_secs = (target - now).total_seconds()
                logger.debug(f"Monitor done for today, sleeping {wait_secs:.0f}s")
                await asyncio.sleep(min(wait_secs, 3600))
                continue

            # 9:39-9:50 window — run scan once
            logger.info("Monitor: entering scan window")
            scan_time_str = datetime.now(beijing_tz).strftime("%Y-%m-%d %H:%M")
            state["last_scan_time"] = scan_time_str  # Always set when we attempt
            try:
                app_state = state.get("_app_state")
                storage = getattr(app_state, "storage", None) if app_state else None
                result = await _execute_monitor_scan(state, storage=storage)
                if result is None:
                    state["last_scan_result"] = "no_result"
                    state["last_scan_message"] = "无合格股票"
                    logger.warning("Monitor scan returned None (no result)")
                    try:
                        from src.common.feishu_bot import FeishuBot

                        bot = FeishuBot()
                        if bot.is_configured():
                            await bot.send_alert(
                                "盘中监控扫描无结果",
                                "扫描完成但未产生结果（数据源不可用或无合格股票）",
                            )
                    except Exception:
                        pass
                else:
                    state["last_scan_result"] = "success"
                    candidates = result.get("final_candidates", 0)
                    rec = result.get("recommended_stock")
                    rec_name = rec["stock_name"] if rec else "无"
                    state["last_scan_message"] = f"{candidates}只候选, 推荐: {rec_name}"
            except Exception as e:
                state["last_scan_result"] = "failed"
                state["last_scan_message"] = str(e)[:100]
                logger.error(f"Monitor scan error: {e}", exc_info=True)
                try:
                    from src.common.feishu_bot import FeishuBot

                    bot = FeishuBot()
                    if bot.is_configured():
                        await bot.send_alert(
                            "盘中监控扫描失败",
                            f"{type(e).__name__}: {e}",
                        )
                except Exception:
                    pass

            # Persist scan status to GreptimeDB
            await _persist_scan_status(state, "scheduled")

            # After scan, wait until next day (keep today_results for status endpoint)
            tomorrow = now + timedelta(days=1)
            target = datetime.combine(tomorrow.date(), time(9, 25), tzinfo=beijing_tz)
            wait_secs = (target - datetime.now(beijing_tz)).total_seconds()
            await asyncio.sleep(min(max(wait_secs, 10), 3600 * 18))
            state["today_results"] = []

    except asyncio.CancelledError:
        logger.info("Intraday momentum monitor cancelled")
    except Exception as e:
        logger.error(f"Intraday momentum monitor error: {e}", exc_info=True)
    finally:
        state["running"] = False
        state["task"] = None
        logger.info("Intraday momentum monitor stopped")


# === SAFETY AUDIT ENDPOINT ===


# === SETTINGS ENDPOINTS ===


def create_settings_router() -> APIRouter:
    """Create router for settings page (API key management)."""
    router = APIRouter(tags=["settings"])

    @router.get("/settings", response_class=HTMLResponse)
    async def settings_page(request: Request):
        """Settings page."""
        templates = request.app.state.templates
        return templates.TemplateResponse("settings.html", {"request": request})

    # === TUSHARE TOKEN SETTINGS ===

    @router.get("/api/settings/tushare-token")
    async def get_tushare_token_status():
        """Get current Tushare Pro token status (masked)."""
        from src.common.config import get_tushare_token, get_tushare_token_source

        source = get_tushare_token_source()
        source_labels = {
            "web_ui": "Web UI (当前会话)",
            "persisted_file": "Web UI (已持久化)",
            "env_var": "环境变量",
            "secrets_yaml": "secrets.yaml",
            "not_configured": "未配置",
        }

        try:
            token = get_tushare_token()
            if len(token) > 20:
                masked = token[:8] + "..." + token[-8:]
            else:
                masked = "***"
            return {
                "configured": True,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_token": masked,
                "token_length": len(token),
            }
        except ValueError:
            return {
                "configured": False,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_token": "",
                "token_length": 0,
            }

    @router.post("/api/settings/tushare-token")
    async def update_tushare_token(body: TokenUpdateRequest):
        """Save a new Tushare Pro token."""
        from src.common.config import set_tushare_token

        token = body.token.strip()
        if not token:
            raise HTTPException(status_code=400, detail="Token 不能为空")

        set_tushare_token(token)
        return {"success": True, "message": "Token 已保存，新的 API 调用将使用此 token"}

    @router.post("/api/settings/tushare-token/test")
    async def test_tushare_token(body: TokenUpdateRequest):
        """Test a Tushare Pro token by calling a simple API."""
        import httpx

        token = body.token.strip()
        if not token:
            raise HTTPException(status_code=400, detail="Token 不能为空")

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    "http://api.tushare.pro",
                    json={
                        "api_name": "rt_min",
                        "token": token,
                        "params": {"ts_code": "000001.SZ", "freq": "1MIN"},
                        "fields": "ts_code,trade_time,close",
                    },
                )
                response.raise_for_status()
                data = response.json()

                code = data.get("code", -1)
                if code == 0:
                    items = data.get("data", {}).get("items", [])
                    return {
                        "success": True,
                        "message": f"Token 验证成功 (rt_min)，获取到 {len(items)} 条分钟数据",
                    }
                else:
                    msg = data.get("msg", "未知错误")
                    return {
                        "success": False,
                        "message": f"Token 验证失败: {msg} (错误码: {code})",
                    }
        except httpx.TimeoutException:
            return {"success": False, "message": "请求超时，请检查网络连接"}
        except httpx.HTTPError as e:
            return {"success": False, "message": f"HTTP 请求失败: {e}"}

    # === IQUANT API KEY SETTINGS ===

    @router.get("/api/settings/iquant-key")
    async def get_iquant_key_status():
        """Get current iQuant API key status (masked)."""
        from src.common.config import get_iquant_api_key, get_iquant_key_source

        source = get_iquant_key_source()
        source_labels = {
            "web_ui": "Web UI (当前会话)",
            "persisted_file": "Web UI (已持久化)",
            "env_var": "环境变量",
            "not_configured": "未配置",
        }

        try:
            key = get_iquant_api_key()
            if len(key) > 16:
                masked = key[:4] + "..." + key[-4:]
            else:
                masked = "***"
            return {
                "configured": True,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_key": masked,
                "key_length": len(key),
            }
        except ValueError:
            return {
                "configured": False,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_key": "",
                "key_length": 0,
            }

    @router.post("/api/settings/iquant-key")
    async def update_iquant_key(body: TokenUpdateRequest):
        """Save a new iQuant API key."""
        from src.common.config import set_iquant_api_key

        key = body.token.strip()
        if not key:
            raise HTTPException(status_code=400, detail="API Key 不能为空")

        set_iquant_api_key(key)
        return {"success": True, "message": "iQuant API Key 已保存"}

    # === TSANGHI TOKEN SETTINGS ===

    @router.get("/api/settings/tsanghi-token")
    async def get_tsanghi_token_status():
        """Get current Tsanghi token status (masked)."""
        from src.common.config import get_tsanghi_token, get_tsanghi_token_source

        source = get_tsanghi_token_source()
        source_labels = {
            "web_ui": "Web UI (当前会话)",
            "persisted_file": "Web UI (已持久化)",
            "env_var": "环境变量",
            "secrets_yaml": "secrets.yaml",
            "not_configured": "未配置",
        }

        try:
            token = get_tsanghi_token()
            if len(token) > 20:
                masked = token[:8] + "..." + token[-8:]
            else:
                masked = "***"
            return {
                "configured": True,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_token": masked,
                "token_length": len(token),
            }
        except ValueError:
            return {
                "configured": False,
                "source": source,
                "source_label": source_labels.get(source, source),
                "masked_token": "",
                "token_length": 0,
            }

    @router.post("/api/settings/tsanghi-token")
    async def update_tsanghi_token(body: TokenUpdateRequest):
        """Save a new Tsanghi token."""
        from src.common.config import set_tsanghi_token

        token = body.token.strip()
        if not token:
            raise HTTPException(status_code=400, detail="Token 不能为空")

        set_tsanghi_token(token)
        return {"success": True, "message": "Token 已保存，回测数据将使用此 token"}

    @router.post("/api/settings/tsanghi-token/test")
    async def test_tsanghi_token(body: TokenUpdateRequest):
        """Test a Tsanghi token by fetching one stock's daily data."""
        import httpx

        token = body.token.strip()
        if not token:
            raise HTTPException(status_code=400, detail="Token 不能为空")

        try:
            url = "https://tsanghi.com/api/fin/stock/XSHG/daily"
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    url,
                    params={"token": token, "ticker": "600519", "order": 2},
                )
                resp.raise_for_status()
                data = resp.json()

                code = data.get("code")
                if code == 200 and data.get("data"):
                    count = len(data["data"])
                    return {
                        "success": True,
                        "message": f"Token 验证成功，获取到 {count} 条日线数据",
                    }
                else:
                    msg = data.get("msg", "未知错误")
                    return {
                        "success": False,
                        "message": f"Token 验证失败: {msg} (code={code})",
                    }
        except httpx.TimeoutException:
            return {"success": False, "message": "请求超时，请检查网络连接"}
        except httpx.HTTPError as e:
            return {"success": False, "message": f"HTTP 请求失败: {e}"}

    # === CACHE SCHEDULER TOGGLE ===

    @router.get("/api/settings/cache-scheduler")
    async def get_cache_scheduler_status():
        from src.common.config import get_cache_scheduler_enabled

        return {"enabled": get_cache_scheduler_enabled()}

    @router.post("/api/settings/cache-scheduler")
    async def set_cache_scheduler_status(request: Request):
        from src.common.config import set_cache_scheduler_enabled

        body = await request.json()
        enabled = bool(body.get("enabled", True))
        set_cache_scheduler_enabled(enabled)
        return {
            "success": True,
            "enabled": enabled,
            "message": f"缓存定时更新已{'开启' if enabled else '关闭'}",
        }

    # === DAILY SCAN TOGGLE ===

    @router.get("/api/settings/daily-scan")
    async def get_daily_scan_status():
        from src.common.config import get_daily_scan_enabled

        return {"enabled": get_daily_scan_enabled()}

    @router.post("/api/settings/daily-scan")
    async def set_daily_scan_status(request: Request):
        from src.common.config import set_daily_scan_enabled

        body = await request.json()
        enabled = bool(body.get("enabled", True))
        set_daily_scan_enabled(enabled)
        return {
            "success": True,
            "enabled": enabled,
            "message": f"每日扫描已{'开启' if enabled else '关闭'}",
        }

    # === RECOMMENDATIONS TOGGLE ===

    @router.get("/api/settings/recommendations")
    async def get_recommendations_status():
        from src.common.config import get_recommendations_enabled

        return {"enabled": get_recommendations_enabled()}

    @router.post("/api/settings/recommendations")
    async def set_recommendations_status(request: Request):
        from src.common.config import set_recommendations_enabled

        body = await request.json()
        enabled = bool(body.get("enabled", True))
        set_recommendations_enabled(enabled)
        return {
            "success": True,
            "enabled": enabled,
            "message": f"今日推荐已{'开启' if enabled else '关闭'}",
        }

    # === FC TRAINING ENDPOINT URL ===

    @router.get("/api/settings/fc-url")
    async def get_fc_url_status():
        from src.common.config import get_fc_url

        url = get_fc_url()
        if url:
            # Mask middle of URL for display
            masked = url[:20] + "..." + url[-10:] if len(url) > 35 else url
            return {
                "configured": True,
                "masked_token": masked,
                "token_length": len(url),
                "source_label": "文件配置",
            }
        return {"configured": False}

    @router.post("/api/settings/fc-url")
    async def update_fc_url(request: Request):
        from src.common.config import set_fc_url

        body = await request.json()
        url = (body.get("token") or "").strip()
        if not url:
            raise HTTPException(status_code=400, detail="URL 不能为空")

        set_fc_url(url)
        return {"success": True, "message": "FC 训练端点已保存"}

    @router.post("/api/settings/fc-url/test")
    async def test_fc_url(request: Request):
        import httpx

        body = await request.json()
        url = (body.get("token") or "").strip()
        if not url:
            raise HTTPException(status_code=400, detail="URL 不能为空")

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url)
                data = resp.json()
                if data.get("status") == "ok":
                    return {"success": True, "message": "FC 端点连接成功"}
                return {"success": False, "message": f"意外响应: {data}"}
        except httpx.TimeoutException:
            return {"success": False, "message": "连接超时"}
        except Exception as e:
            return {"success": False, "message": f"连接失败: {e}"}

    # === S3 OBJECT STORAGE CONFIG ===

    @router.get("/api/settings/s3-config")
    async def get_s3_config_status():
        from src.common.config import get_s3_config

        config = get_s3_config()
        if config and config.get("endpoint_url") and config.get("bucket"):
            ep = config["endpoint_url"]
            masked_ep = ep[:20] + "..." + ep[-10:] if len(ep) > 35 else ep
            ak = config.get("access_key", "")
            masked_ak = ak[:4] + "***" + ak[-4:] if len(ak) > 8 else "***"
            return {
                "configured": True,
                "masked_token": f"{masked_ep} | {masked_ak}",
                "token_length": len(ep),
                "source_label": "文件配置",
                "bucket": config.get("bucket", ""),
            }
        return {"configured": False}

    @router.post("/api/settings/s3-config")
    async def update_s3_config(request: Request):
        from src.common.config import set_s3_config

        body = await request.json()
        config = {
            "endpoint_url": (body.get("endpoint_url") or "").strip(),
            "access_key": (body.get("access_key") or "").strip(),
            "secret_key": (body.get("secret_key") or "").strip(),
            "bucket": (body.get("bucket") or "").strip(),
        }
        if not config["endpoint_url"] or not config["bucket"]:
            raise HTTPException(status_code=400, detail="Endpoint URL 和 Bucket 不能为空")
        if not config["access_key"] or not config["secret_key"]:
            raise HTTPException(status_code=400, detail="Access Key 和 Secret Key 不能为空")

        set_s3_config(config)
        return {"success": True, "message": "S3 配置已保存"}

    @router.post("/api/settings/s3-config/test")
    async def test_s3_config(request: Request):
        body = await request.json()
        endpoint_url = (body.get("endpoint_url") or "").strip()
        access_key = (body.get("access_key") or "").strip()
        secret_key = (body.get("secret_key") or "").strip()
        bucket = (body.get("bucket") or "").strip()

        if not all([endpoint_url, access_key, secret_key, bucket]):
            return {"success": False, "message": "请填写全部 4 个字段"}

        try:
            from src.common.s3_client import S3Client

            s3 = S3Client(endpoint_url, access_key, secret_key, bucket)
            models = await s3.list_models(prefix="models/")
            return {
                "success": True,
                "message": f"连接成功，Bucket 中有 {len(models)} 个模型文件",
            }
        except Exception as e:
            msg = str(e)
            if "NoSuchBucket" in msg:
                return {"success": False, "message": f"Bucket '{bucket}' 不存在"}
            if "AccessDenied" in msg or "InvalidAccessKeyId" in msg:
                return {"success": False, "message": "Access Key 无效或无权限"}
            if "SignatureDoesNotMatch" in msg:
                return {"success": False, "message": "Secret Key 不匹配"}
            return {"success": False, "message": f"连接失败: {msg}"}

    # === ALL KEYS STATUS ===

    @router.get("/api/settings/keys-status")
    async def get_all_keys_status():
        """Get status of all API keys needed for live trading."""
        from src.common.config import (
            get_iquant_key_source,
            get_tsanghi_token_source,
        )

        iquant_ok = get_iquant_key_source() != "not_configured"
        tsanghi_ok = get_tsanghi_token_source() != "not_configured"

        return {
            "iquant": {"configured": iquant_ok, "source": get_iquant_key_source()},
            "tsanghi": {"configured": tsanghi_ok, "source": get_tsanghi_token_source()},
        }

    # === XTQUANT BROKER SETTINGS ===

    class XtquantConfigRequest(BaseModel):
        server_url: str
        api_key: str

    @router.get("/api/settings/xtquant")
    async def get_xtquant_status():
        """Get current xtquant-trade-server config status."""
        from src.common.config import get_xtquant_api_key, get_xtquant_server_url

        url_configured = False
        current_url = ""
        try:
            current_url = get_xtquant_server_url()
            url_configured = True
        except ValueError:
            pass

        key_configured = False
        masked_key = ""
        try:
            key = get_xtquant_api_key()
            masked_key = key[:4] + "..." + key[-4:] if len(key) > 8 else "***"
            key_configured = True
        except ValueError:
            pass

        return {
            "configured": url_configured and key_configured,
            "server_url": current_url,
            "key_configured": key_configured,
            "masked_key": masked_key,
        }

    @router.post("/api/settings/xtquant")
    async def update_xtquant_config(body: XtquantConfigRequest):
        """Save xtquant-trade-server URL and API key."""
        from src.common.config import set_xtquant_api_key, set_xtquant_server_url

        url = body.server_url.strip().rstrip("/")
        key = body.api_key.strip()
        if not url:
            raise HTTPException(status_code=400, detail="Server URL 不能为空")
        if not key:
            raise HTTPException(status_code=400, detail="API Key 不能为空")

        set_xtquant_server_url(url)
        set_xtquant_api_key(key)
        return {"success": True, "message": "Broker 配置已保存，重启服务后生效"}

    @router.post("/api/settings/xtquant/test")
    async def test_xtquant_connection(request: Request, body: XtquantConfigRequest):
        """Test xtquant-trade-server connection via /readyz."""
        import httpx

        url = body.server_url.strip().rstrip("/")
        key = body.api_key.strip()
        if not url or not key:
            raise HTTPException(status_code=400, detail="URL 和 Key 不能为空")

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{url}/readyz")
                if resp.status_code == 200:
                    return {"success": True, "message": "连接成功，Broker 已就绪"}
                elif resp.status_code == 503:
                    return {
                        "success": False,
                        "message": "服务器在线但 Broker 未就绪（QMT 可能未登录）",
                    }
                else:
                    return {"success": False, "message": f"服务器返回 {resp.status_code}"}
        except httpx.ConnectError:
            return {"success": False, "message": f"无法连接到 {url}，请检查 IP 和端口"}
        except httpx.TimeoutException:
            return {"success": False, "message": "连接超时，请检查内网是否可达"}

    return router


# ---------------------------------------------------------------------------
# Trade Backtest Router — CSV upload → aggregate stats + equity curve
# ---------------------------------------------------------------------------


def create_trade_backtest_router() -> APIRouter:
    """Router for CSV trade record analysis."""
    import csv
    import io
    import statistics

    router = APIRouter(tags=["trade-backtest"])

    @router.post("/api/trade-backtest/analyze")
    async def analyze_trades(request: Request):
        import traceback as _tb

        try:
            return await _analyze_trades_impl(request)
        except HTTPException:
            raise
        except Exception as exc:
            logger.error(f"trade-backtest analyze error: {exc}\n{_tb.format_exc()}")
            raise HTTPException(500, f"分析失败: {exc}")

    async def _analyze_trades_impl(request: Request) -> dict[str, Any]:
        content = await request.body()

        text = None
        for encoding in ("utf-8-sig", "gbk", "gb2312"):
            try:
                text = content.decode(encoding)
                break
            except (UnicodeDecodeError, LookupError):
                continue
        if text is None:
            raise HTTPException(400, "无法解码 CSV 文件，请使用 UTF-8 或 GBK 编码")

        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            raise HTTPException(400, "CSV 文件为空或格式错误")

        cols = set(reader.fieldnames)
        required = {"股票代码", "买入时间", "卖出时间"}
        missing = required - cols
        if missing:
            raise HTTPException(400, f"CSV 缺少必要列: {', '.join(missing)}")

        rows_raw: list[dict[str, str]] = []
        for row in reader:
            code = row["股票代码"].strip()
            if not code:
                continue
            rows_raw.append(row)

        if not rows_raw:
            raise HTTPException(400, "CSV 中没有交易记录")

        storage = getattr(request.app.state, "storage", None)
        if storage is None or not storage.is_ready:
            raise HTTPException(503, "GreptimeDB 缓存未连接，请稍后再试")

        all_dates: list[str] = []
        for row in rows_raw:
            all_dates.append(row["买入时间"].strip()[:10])
            all_dates.append(row["卖出时间"].strip()[:10])
        all_dates.sort()
        from datetime import date

        csv_start = date.fromisoformat(all_dates[0])
        csv_end = date.fromisoformat(all_dates[-1])

        if not await storage.covers_range(csv_start, csv_end):
            db_start, db_end = await storage.get_date_range()
            cache_start = str(db_start) if db_start else "无"
            cache_end = str(db_end) if db_end else "无"
            gaps = await storage.missing_ranges(csv_start, csv_end)
            gap_strs = [f"{s}~{e}" for s, e in gaps] if gaps else [f"{csv_start}~{csv_end}"]
            return {
                "needs_cache_update": True,
                "csv_start": str(csv_start),
                "csv_end": str(csv_end),
                "cache_start": cache_start,
                "cache_end": cache_end,
                "missing_ranges": gap_strs,
                "message": (
                    f"CSV 交易区间 {csv_start}~{csv_end}，"
                    f"缓存覆盖 {cache_start}~{cache_end}，"
                    f"需要增量下载: {', '.join(gap_strs)}"
                ),
            }

        import math

        initial_capital = 100000.0
        cap_param = request.query_params.get("initial_capital")
        if cap_param:
            try:
                initial_capital = float(cap_param)
            except ValueError:
                raise HTTPException(400, f"initial_capital 参数无效: {cap_param}")
            if initial_capital <= 0:
                raise HTTPException(400, "initial_capital 必须为正数")

        capital = initial_capital
        trades: list[dict[str, Any]] = []
        returns: list[float] = []
        buy_times: list[str] = []
        sell_times: list[str] = []
        hold_days_list: list[float] = []
        total_commission = 0.0
        total_stamp_tax = 0.0
        capital_history: list[float] = [capital]

        for i, row in enumerate(rows_raw, 1):
            try:
                code = row["股票代码"].strip()
                buy_time = row["买入时间"].strip()
                sell_time = row["卖出时间"].strip()
                buy_date = buy_time[:10]
                sell_date = sell_time[:10]

                buy_day = await storage.get_daily(code, buy_date)
                sell_day = await storage.get_daily(code, sell_date)
                if not buy_day or buy_day.open is None:
                    raise HTTPException(
                        400,
                        f"第 {i} 行: 缓存中无 {code} 在 {buy_date} 的价格数据",
                    )
                if not sell_day or sell_day.close is None:
                    raise HTTPException(
                        400,
                        f"第 {i} 行: 缓存中无 {code} 在 {sell_date} 的价格数据",
                    )
                buy_price = float(buy_day.open)
                sell_price = float(sell_day.close)
                if buy_price <= 0:
                    raise HTTPException(
                        400,
                        f"第 {i} 行: {code} 在 {buy_date} 的开盘价为 0",
                    )

                lots = math.floor(capital / (buy_price * 100))
                if lots <= 0:
                    trades.append(
                        {
                            "idx": i,
                            "stock_code": code,
                            "stock_name": row.get("股票名称", "").strip(),
                            "board": row.get("所属板块", "").strip(),
                            "buy_time": buy_time,
                            "buy_price": buy_price,
                            "sell_time": sell_time,
                            "sell_price": sell_price,
                            "hold_days": None,
                            "return_pct": 0.0,
                            "sell_reason": "资金不足",
                            "lots": 0,
                        }
                    )
                    returns.append(0.0)
                    buy_times.append(buy_time)
                    sell_times.append(sell_time)
                    capital_history.append(capital)
                    continue

                buy_amount = lots * 100 * buy_price
                buy_comm = max(buy_amount * 0.003, 5.0)
                buy_transfer = buy_amount * 0.00001
                total_buy_cost = buy_amount + buy_comm + buy_transfer

                while total_buy_cost > capital and lots > 0:
                    lots -= 1
                    buy_amount = lots * 100 * buy_price
                    buy_comm = max(buy_amount * 0.003, 5.0)
                    buy_transfer = buy_amount * 0.00001
                    total_buy_cost = buy_amount + buy_comm + buy_transfer

                if lots <= 0:
                    trades.append(
                        {
                            "idx": i,
                            "stock_code": code,
                            "stock_name": row.get("股票名称", "").strip(),
                            "board": row.get("所属板块", "").strip(),
                            "buy_time": buy_time,
                            "buy_price": buy_price,
                            "sell_time": sell_time,
                            "sell_price": sell_price,
                            "hold_days": None,
                            "return_pct": 0.0,
                            "sell_reason": "资金不足（含手续费）",
                            "lots": 0,
                        }
                    )
                    returns.append(0.0)
                    buy_times.append(buy_time)
                    sell_times.append(sell_time)
                    capital_history.append(capital)
                    continue

                sell_amount = lots * 100 * sell_price
                sell_comm = max(sell_amount * 0.003, 5.0)
                sell_transfer = sell_amount * 0.00001
                sell_stamp = sell_amount * 0.0005
                net_sell = sell_amount - sell_comm - sell_transfer - sell_stamp

                trade_profit = net_sell - total_buy_cost
                ret = round(trade_profit / total_buy_cost * 100, 2)
                capital = capital - total_buy_cost + net_sell
                total_commission += buy_comm + sell_comm
                total_stamp_tax += sell_stamp

                hd = int(row["持有天数"]) if row.get("持有天数") else None
                trades.append(
                    {
                        "idx": i,
                        "stock_code": code,
                        "stock_name": row.get("股票名称", "").strip(),
                        "board": row.get("所属板块", "").strip(),
                        "buy_time": buy_time,
                        "buy_price": buy_price,
                        "sell_time": sell_time,
                        "sell_price": sell_price,
                        "hold_days": hd,
                        "return_pct": ret,
                        "sell_reason": row.get("卖出原因", "").strip(),
                        "lots": lots,
                    }
                )
                returns.append(ret)
                buy_times.append(buy_time)
                sell_times.append(sell_time)
                capital_history.append(capital)
                if hd is not None:
                    hold_days_list.append(float(hd))
            except HTTPException:
                raise
            except (ValueError, KeyError) as exc:
                raise HTTPException(400, f"第 {i} 行数据格式错误: {exc}")

        if not trades:
            raise HTTPException(400, "CSV 中没有交易记录")

        total = len(trades)
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r < 0]

        equity = [c / initial_capital for c in capital_history]
        cumulative_return_pct = (capital - initial_capital) / initial_capital * 100

        peak = equity[0]
        max_dd = 0.0
        for eq in equity:
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak * 100
            if dd > max_dd:
                max_dd = dd

        max_cw = max_cl = 0
        streak = 0
        for r in returns:
            if r > 0:
                streak = streak + 1 if streak > 0 else 1
                max_cw = max(max_cw, streak)
            elif r < 0:
                streak = streak - 1 if streak < 0 else -1
                max_cl = max(max_cl, abs(streak))
            else:
                streak = 0

        profit_amounts = [r for r in returns if r > 0]
        loss_amounts = [abs(r) for r in returns if r < 0]
        sum_w = sum(profit_amounts) if profit_amounts else 0.0
        sum_l = sum(loss_amounts) if loss_amounts else 0.0
        profit_factor = round(sum_w / sum_l, 2) if sum_l > 0 else float("inf")

        avg_hold = round(statistics.mean(hold_days_list), 1) if hold_days_list else None

        summary = {
            "total_trades": total,
            "win_count": len(wins),
            "lose_count": len(losses),
            "flat_count": total - len(wins) - len(losses),
            "win_rate": round(len(wins) / total * 100, 2),
            "avg_return_pct": round(statistics.mean(returns), 2),
            "median_return_pct": round(statistics.median(returns), 2),
            "cumulative_return_pct": round(cumulative_return_pct, 2),
            "max_single_win_pct": round(max(returns), 2) if returns else 0,
            "max_single_loss_pct": round(min(returns), 2) if returns else 0,
            "max_consecutive_wins": max_cw,
            "max_consecutive_losses": max_cl,
            "max_drawdown_pct": round(max_dd, 2),
            "profit_factor": profit_factor,
            "avg_holding_days": avg_hold,
            "date_range": f"{buy_times[0][:10]} ~ {sell_times[-1][:10]}",
            "initial_capital": initial_capital,
            "final_capital": round(capital, 2),
            "total_commission": round(total_commission, 2),
            "total_stamp_tax": round(total_stamp_tax, 2),
        }

        monthly: dict[str, list[float]] = defaultdict(list)
        for idx, ret in enumerate(returns):
            month = buy_times[idx][:7]
            monthly[month].append(ret)

        monthly_returns = []
        for month in sorted(monthly.keys()):
            rets = monthly[month]
            compound = 1.0
            for r in rets:
                compound *= 1 + r / 100
            w = [r for r in rets if r > 0]
            monthly_returns.append(
                {
                    "month": month,
                    "trades": len(rets),
                    "return_pct": round((compound - 1) * 100, 2),
                    "win_rate": round(len(w) / len(rets) * 100, 1),
                }
            )

        equity_curve: list[dict[str, Any]] = [
            {"trade_idx": 0, "date": buy_times[0][:10], "equity": round(equity[0], 4)}
        ]
        for idx in range(total):
            equity_curve.append(
                {
                    "trade_idx": idx + 1,
                    "date": sell_times[idx][:10],
                    "equity": round(equity[idx + 1], 4),
                }
            )

        return {
            "success": True,
            "summary": summary,
            "monthly_returns": monthly_returns,
            "equity_curve": equity_curve,
            "trades": trades,
        }

    return router


def create_trading_router() -> APIRouter:
    """Router for the dashboard trading module (buy/sell + recommendations).

    API-only — the trading UI is embedded in the main dashboard (index.html).
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    router = APIRouter(tags=["trading"])
    BEIJING_TZ = ZoneInfo("Asia/Shanghai")

    @router.get("/api/trading/holdings")
    async def get_holdings(request: Request) -> dict:
        """Get current broker positions (cached from xtquant-trade-server poll)."""
        broker_pos = getattr(request.app.state, "broker_positions", [])

        from src.data.sources.local_concept_mapper import LocalConceptMapper

        mapper = LocalConceptMapper()
        holdings = [
            {
                "code": pos["code"],
                "name": mapper.get_stock_name(pos["code"]),
                "quantity": pos.get("volume", 0),
            }
            for pos in broker_pos
            if pos.get("volume", 0) > 0
        ]
        return {"holdings": holdings}

    @router.get("/api/trading/recommendations")
    async def get_recommendations(request: Request, date: str | None = None) -> dict:
        """Get top-10 recommendations by computing ML scan on-demand.

        Past dates use GreptimeDB cache (~1-3s).
        Today uses Tushare rt_min_daily (~30-60s, only after 09:39).
        No DB caching — strategy is actively being iterated.
        """
        from src.common.config import get_recommendations_enabled

        if date is None:
            date = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")

        if not get_recommendations_enabled():
            return {"date": date, "recommendations": [], "error": "推荐功能已关闭"}

        storage = getattr(request.app.state, "storage", None)

        try:
            recs, _scan_result = await _compute_ml_scan(
                date_str=date,
                storage=storage,
            )
        except ValueError as e:
            return {"date": date, "recommendations": [], "error": str(e)}
        except Exception as e:
            from src.strategy.ml_strategy_service import MinuteDataMissingError

            if isinstance(e, MinuteDataMissingError):
                return {"date": date, "recommendations": [], "error": str(e)}
            logger.error(f"On-demand ML scan failed for {date}: {e}", exc_info=True)
            return {"date": date, "recommendations": [], "error": str(e)}

        return {"date": date, "recommendations": recs}

    @router.post("/api/trading/buy")
    async def submit_buy(request: Request) -> dict:
        """Place a BUY order via xtquant-trade-server."""
        from src.trading.broker_client import BrokerClient, BrokerError

        body = await request.json()
        stock_code = body.get("stock_code", "")
        stock_name = body.get("stock_name", "")
        quantity = int(body.get("quantity", 0))
        price = body.get("price")

        if not stock_code or quantity <= 0 or quantity % 100 != 0:
            raise HTTPException(status_code=400, detail="stock_code 或 quantity 无效")

        broker: BrokerClient | None = getattr(request.app.state, "broker", None)
        if broker is None:
            raise HTTPException(status_code=503, detail="Broker 未配置，请设置 XTQUANT_SERVER_URL")

        try:
            result = await broker.place_order(
                code=stock_code,
                side="BUY",
                qty=quantity,
                price_type="LIMIT" if price else "MARKET",
                price=float(price) if price else None,
                remark=f"Dashboard买入 {stock_name}".strip(),
            )
        except BrokerError as e:
            raise HTTPException(status_code=400, detail=f"Broker拒单: {e.message}")

        logger.info(f"BUY order placed: {stock_code} qty={quantity} order_id={result.order_id}")
        return {
            "success": True,
            "order_id": result.order_id,
            "status": result.status,
            "quantity": quantity,
        }

    @router.post("/api/trading/sell")
    async def submit_sell(request: Request) -> dict:
        """Place a SELL order via xtquant-trade-server."""
        from src.trading.broker_client import BrokerClient, BrokerError

        body = await request.json()
        stock_code = body.get("stock_code", "")
        stock_name = body.get("stock_name", "")
        quantity = int(body.get("quantity", 0))

        if not stock_code or quantity <= 0 or quantity % 100 != 0:
            raise HTTPException(status_code=400, detail="stock_code 或 quantity 无效")

        broker: BrokerClient | None = getattr(request.app.state, "broker", None)
        if broker is None:
            raise HTTPException(status_code=503, detail="Broker 未配置，请设置 XTQUANT_SERVER_URL")

        try:
            result = await broker.place_order(
                code=stock_code,
                side="SELL",
                qty=quantity,
                price_type="MARKET",
                remark=f"Dashboard卖出 {stock_name}".strip(),
            )
        except BrokerError as e:
            raise HTTPException(status_code=400, detail=f"Broker拒单: {e.message}")

        logger.info(f"SELL order placed: {stock_code} qty={quantity} order_id={result.order_id}")
        return {
            "success": True,
            "order_id": result.order_id,
            "status": result.status,
            "quantity": quantity,
        }

    @router.get("/api/trading/orders")
    async def get_orders(request: Request) -> dict:
        """Return today's open orders from broker."""
        from src.trading.broker_client import BrokerClient

        broker: BrokerClient | None = getattr(request.app.state, "broker", None)
        if broker is None:
            return {"orders": []}
        try:
            orders = await broker.get_orders()
            return {"orders": orders}
        except Exception as e:
            logger.warning(f"get_orders failed: {e}")
            return {"orders": []}

    @router.delete("/api/trading/orders/{order_id}")
    async def cancel_order(request: Request, order_id: int) -> dict:
        """Cancel an open order by broker order_id."""
        from src.trading.broker_client import BrokerClient, BrokerError

        broker: BrokerClient | None = getattr(request.app.state, "broker", None)
        if broker is None:
            raise HTTPException(status_code=503, detail="Broker 未配置")

        try:
            result = await broker.cancel_order(order_id)
            return {"success": True, "result": result}
        except BrokerError as e:
            raise HTTPException(status_code=400, detail=e.message)

    return router


def create_model_router() -> APIRouter:
    """Router for ML model management (training, fine-tuning, status)."""
    import asyncio
    import json

    router = APIRouter(tags=["models"])

    def _sse(data: dict) -> str:
        return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

    def _get_scheduler(request: Request):
        scheduler = getattr(request.app.state, "model_scheduler", None)
        if scheduler is None:
            raise HTTPException(status_code=503, detail="模型调度器未就绪")
        return scheduler

    @router.get("/api/model/status")
    async def get_model_status(request: Request) -> dict:
        """Get model scheduler status for dashboard polling."""
        scheduler = _get_scheduler(request)
        return scheduler.get_status()

    @router.get("/api/model/logs")
    async def get_model_logs(request: Request) -> dict:
        """Get recent training log lines."""
        scheduler = _get_scheduler(request)
        return {"logs": scheduler.training_log[-100:]}

    @router.post("/api/model/full-train")
    async def full_train(request: Request):
        """Start full model training with SSE progress stream."""
        scheduler = _get_scheduler(request)

        if scheduler.training_in_progress:
            return StreamingResponse(
                iter([_sse({"type": "error", "message": "训练正在进行中"})]),
                media_type="text/event-stream",
            )

        queue: asyncio.Queue[dict | None] = asyncio.Queue()

        async def _run():
            try:

                async def _progress(msg: str):
                    queue.put_nowait({"type": "progress", "message": msg})
                    await asyncio.sleep(0)  # yield to event loop so SSE flushes each message

                result = await scheduler.run_full_training(progress_cb=_progress)
                if result.get("error"):
                    queue.put_nowait({"type": "error", "message": result["error"]})
                else:
                    queue.put_nowait({"type": "result", "data": result})
            except Exception as e:
                queue.put_nowait({"type": "error", "message": str(e)[:200]})
            finally:
                queue.put_nowait(None)

        async def event_stream():
            task = asyncio.create_task(_run())
            request.app.state.model_train_task = task
            had_error = False
            try:
                yield _sse({"type": "status", "message": "全量训练开始..."})
                while True:
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=30)
                    except asyncio.TimeoutError:
                        yield _sse({"type": "heartbeat"})
                        continue

                    if event is None:
                        break
                    if event.get("type") == "error":
                        had_error = True
                    yield _sse(event)

                if not had_error:
                    yield _sse({"type": "complete", "message": "全量训练完成"})
            except Exception as e:
                logger.error(f"Full train SSE error: {e}", exc_info=True)
                yield _sse({"type": "error", "message": str(e)[:200]})
            finally:
                request.app.state.model_train_task = None

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @router.post("/api/model/finetune")
    async def finetune(request: Request):
        """Start model fine-tuning with SSE progress stream."""
        scheduler = _get_scheduler(request)

        if scheduler.training_in_progress:
            return StreamingResponse(
                iter([_sse({"type": "error", "message": "训练正在进行中"})]),
                media_type="text/event-stream",
            )

        if not scheduler.s3_has_full_model:
            return StreamingResponse(
                iter([_sse({"type": "error", "message": "S3 无全量模型，请先执行全量训练"})]),
                media_type="text/event-stream",
            )

        queue: asyncio.Queue[dict | None] = asyncio.Queue()

        async def _run():
            try:

                async def _progress(msg: str):
                    queue.put_nowait({"type": "progress", "message": msg})
                    await asyncio.sleep(0)  # yield to event loop so SSE flushes each message

                result = await scheduler.run_finetune(progress_cb=_progress)
                if result.get("error"):
                    queue.put_nowait({"type": "error", "message": result["error"]})
                else:
                    queue.put_nowait({"type": "result", "data": result})
            except Exception as e:
                queue.put_nowait({"type": "error", "message": str(e)[:200]})
            finally:
                queue.put_nowait(None)

        async def event_stream():
            task = asyncio.create_task(_run())
            request.app.state.model_finetune_task = task
            had_error = False
            try:
                yield _sse({"type": "status", "message": "微调开始..."})
                while True:
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=30)
                    except asyncio.TimeoutError:
                        yield _sse({"type": "heartbeat"})
                        continue

                    if event is None:
                        break
                    if event.get("type") == "error":
                        had_error = True
                    yield _sse(event)

                if not had_error:
                    yield _sse({"type": "complete", "message": "微调完成"})
            except Exception as e:
                logger.error(f"Finetune SSE error: {e}", exc_info=True)
                yield _sse({"type": "error", "message": str(e)[:200]})
            finally:
                request.app.state.model_finetune_task = None

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @router.get("/api/model/training-data")
    async def stream_training_data(request: Request, token: str):
        """Stream daily OHLCV as NDJSON for FC callback. Auth via one-time token."""
        scheduler = _get_scheduler(request)
        mode = scheduler.validate_and_consume_token(token)
        if mode is None:
            debug = (
                f"[训练回调401] token={token[:8]}..., "
                f"stored={len(scheduler._training_tokens)}, "
                f"keys={[k[:8] for k in scheduler._training_tokens]}"
            )
            logger.warning(debug)
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        storage = getattr(request.app.state, "storage", None)
        if storage is None or not storage.is_ready:
            raise HTTPException(status_code=503, detail="Backtest cache not available")

        async def generate_ndjson():
            existing_dates = sorted(await storage.get_existing_daily_dates())
            if mode == "finetune":
                # 120 training days + 37 lookback buffer for history-based features
                existing_dates = existing_dates[-157:]

            yield (
                json.dumps(
                    {
                        "__meta__": True,
                        "total_days": len(existing_dates),
                        "date_range": [str(existing_dates[0]), str(existing_dates[-1])],
                    }
                )
                + "\n"
            )

            for trade_date in existing_dates:
                date_str = (
                    trade_date.strftime("%Y-%m-%d")
                    if hasattr(trade_date, "strftime")
                    else str(trade_date)
                )
                try:
                    bar_map = await storage.get_all_codes_with_daily(date_str)
                except (asyncio.TimeoutError, TimeoutError):
                    logger.warning("Stream export timeout %s, skip", date_str)
                    continue
                except Exception:
                    logger.warning("Stream export error %s, skip", date_str, exc_info=True)
                    continue

                if not bar_map:
                    continue

                day_dict: dict[str, dict] = {}
                for code, bar in bar_map.items():
                    day_dict[code] = {
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                        "amount": bar.amount,
                        "is_suspended": bar.is_suspended,
                    }
                yield json.dumps({"date": date_str, "stocks": day_dict}) + "\n"

        return StreamingResponse(
            generate_ndjson(),
            media_type="application/x-ndjson",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @router.post("/api/model/training-result")
    async def receive_training_result(request: Request, token: str):
        """Receive async training result from FC callback."""
        scheduler = _get_scheduler(request)
        body = await request.json()
        accepted = scheduler.receive_training_result(token, body)
        if not accepted:
            raise HTTPException(status_code=401, detail="Invalid or unknown token")
        return {"status": "accepted"}

    @router.get("/api/ml/model-info")
    async def get_model_info():
        """List available ML models and their metadata."""
        from datetime import datetime
        from pathlib import Path
        from zoneinfo import ZoneInfo

        model_dir = Path(__file__).resolve().parent.parent / "strategy" / "strategies"
        model_dir = model_dir.parent.parent / "data" / "models"

        models = []
        if model_dir.exists():
            for f in sorted(model_dir.glob("*.lgb"), key=lambda p: p.stat().st_mtime, reverse=True):
                stat = f.stat()
                models.append(
                    {
                        "name": f.stem,
                        "file": f.name,
                        "size_kb": round(stat.st_size / 1024, 1),
                        "modified": datetime.fromtimestamp(
                            stat.st_mtime, tz=ZoneInfo("Asia/Shanghai")
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )

        return {
            "model_dir": str(model_dir),
            "models": models,
            "default": "full_latest",
            "has_default": (
                (model_dir / "full_latest.lgb").exists() if model_dir.exists() else False
            ),
        }

    @router.get("/api/model/s3-models")
    async def list_s3_models():
        """List available model files on S3."""
        from src.common.s3_client import create_s3_client_from_config

        s3 = create_s3_client_from_config()
        if s3 is None:
            raise HTTPException(status_code=503, detail="S3 未配置")
        models = await s3.list_models(prefix="models/")
        return {"models": models}

    @router.post("/api/model/download-s3")
    async def download_s3_model(request: Request):
        """Download a model file from S3 and serve it to the browser."""
        import tempfile
        from pathlib import Path

        from starlette.responses import FileResponse

        from src.common.s3_client import create_s3_client_from_config

        s3 = create_s3_client_from_config()
        if s3 is None:
            raise HTTPException(status_code=503, detail="S3 未配置")

        body = await request.json()
        s3_key = body.get("s3_key", "")
        if not s3_key or not s3_key.endswith(".lgb"):
            raise HTTPException(status_code=400, detail="无效的 S3 key")

        filename = s3_key.rsplit("/", 1)[-1]

        # Download to temp file and serve to browser
        tmp = tempfile.NamedTemporaryFile(suffix=".lgb", delete=False)
        tmp.close()
        tmp_path = Path(tmp.name)
        await s3.download_file(s3_key, tmp_path)

        return FileResponse(
            tmp_path,
            filename=filename,
            media_type="application/octet-stream",
        )

    return router
