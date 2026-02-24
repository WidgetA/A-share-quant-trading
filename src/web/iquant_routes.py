# === MODULE PURPOSE ===
# API endpoints for iQuant scripts running on a Windows server.
# Fully isolated from the main online system — creates own DB pool,
# own Sina client, own historical adapter. No shared app.state resources.
#
# === ARCHITECTURE ===
# Push-based: server runs background tasks that produce signals at the right time.
# iQuant polls /pending-signals every bar, executes immediately, then acks.
#
# Signal flow:
#   09:30  → server pushes SELL signal (for yesterday's buy, T+1 sell at open)
#   09:40  → server runs momentum scan → pushes BUY signal
#   iQuant → polls /pending-signals → passorder() → POST /ack-signal
#
# === AUTHENTICATION ===
# All endpoints require X-API-Key header matching IQUANT_API_KEY env var.

from __future__ import annotations

import asyncio
import logging
import traceback
import uuid
from datetime import datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

# --- Authentication ---

_API_KEY_HEADER = APIKeyHeader(name="X-API-Key")


def _verify_api_key(api_key: str = Depends(_API_KEY_HEADER)) -> str:
    """Verify the API key from X-API-Key header."""
    from src.common.config import get_iquant_api_key

    try:
        expected = get_iquant_api_key()
    except ValueError:
        raise HTTPException(
            status_code=500, detail="IQUANT_API_KEY not configured — set via Settings page"
        )
    if api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key


# --- Request models ---


class AckRequest(BaseModel):
    """Request body for /api/iquant/ack-signal."""

    signal_id: str


class QuoteRequest(BaseModel):
    """Request body for /api/iquant/quote."""

    stock_codes: list[str]


# --- Feishu notification helpers ---


async def _notify_feishu_error(title: str, detail: str) -> None:
    """Send error alert to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_alert(f"[iQuant] {title}", detail)
    except Exception:
        logger.warning("Failed to send Feishu error notification", exc_info=True)


async def _notify_feishu_signal(signal: dict) -> None:
    """Send signal notification to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if not bot.is_configured():
            return

        direction = "买入" if signal["type"] == "buy" else "卖出"
        lines = [
            f"📊 [iQuant] {direction}信号",
            f"股票: {signal['stock_code']} {signal.get('stock_name', '')}",
        ]
        if signal["type"] == "buy":
            lines.append(f"板块: {signal.get('board_name', '-')}")
            lines.append(f"价格: {signal.get('latest_price', '-')}")
            lines.append(f"评分: {signal.get('composite_score', '-')}")
        lines.append(f"数量: {signal.get('volume', '-')}股")
        lines.append(f"时间: {signal.get('created_at', '')}")

        await bot.send_message("\n".join(lines))
    except Exception:
        logger.warning("Failed to send Feishu signal notification", exc_info=True)


# --- Router factory ---


def create_iquant_router() -> APIRouter:
    """Create the iQuant API router.

    Includes a background scheduler that auto-generates signals:
    - 09:30: SELL signal for yesterday's holdings (T+1 at open)
    - 09:40: BUY signal from momentum sector scan
    """
    router = APIRouter(prefix="/api/iquant", tags=["iquant"])

    # Isolated state (not shared with main app.state)
    _state: dict[str, Any] = {
        "initialized": False,
        "pending_signals": [],  # signals waiting for iQuant to execute
        "executed_signals": [],  # acked signals (history)
        "holdings": [],  # [{code, name, volume, buy_price, buy_date}]
        "scheduler_task": None,
        "universe_cache": None,
    }

    # --- Resource management ---

    async def _ensure_resources() -> dict[str, Any]:
        """Lazily initialize iQuant-specific resources and start scheduler."""
        if _state["initialized"]:
            return _state

        from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter
        from src.data.clients.sina_realtime import SinaRealtimeClient
        from src.data.database.fundamentals_db import create_fundamentals_db_from_config
        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.filters.stock_filter import create_main_board_only_filter

        sina = SinaRealtimeClient()
        await sina.start()
        _state["sina_client"] = sina

        fdb = create_fundamentals_db_from_config()
        await fdb.connect()
        _state["fundamentals_db"] = fdb

        _state["historical_adapter"] = IQuantHistoricalAdapter(sina)
        _state["concept_mapper"] = LocalConceptMapper()
        _state["stock_filter"] = create_main_board_only_filter()

        # Start background scheduler
        _state["scheduler_task"] = asyncio.create_task(_signal_scheduler())

        _state["initialized"] = True
        logger.info("iQuant resources initialized + scheduler started")
        return _state

    async def _cleanup_resources() -> None:
        """Cleanup on shutdown."""
        task = _state.get("scheduler_task")
        if task and not task.done():
            task.cancel()
        sina = _state.get("sina_client")
        if sina:
            await sina.stop()
        fdb = _state.get("fundamentals_db")
        if fdb:
            await fdb.close()
        _state["initialized"] = False
        logger.info("iQuant resources cleaned up")

    router._iquant_cleanup = _cleanup_resources  # type: ignore[attr-defined]

    # --- Signal helpers ---

    def _push_signal(signal: dict) -> None:
        """Add a signal to the pending queue."""
        signal.setdefault("id", str(uuid.uuid4())[:8])
        signal.setdefault("created_at", datetime.now(BEIJING_TZ).strftime("%H:%M:%S"))
        _state["pending_signals"].append(signal)
        logger.info(
            f"iQuant signal pushed: {signal['type']} {signal['stock_code']} (id={signal['id']})"
        )

    # --- Universe ---

    async def _get_universe() -> list[str]:
        """Get main-board stock codes (cached)."""
        if _state["universe_cache"]:
            return _state["universe_cache"]

        import akshare as ak

        df = await asyncio.to_thread(ak.stock_info_a_code_name)
        stock_filter = _state["stock_filter"]
        codes = [
            row["code"]
            for _, row in df.iterrows()
            if isinstance(row["code"], str)
            and len(row["code"]) == 6
            and stock_filter.is_allowed(row["code"])
        ]
        _state["universe_cache"] = codes
        logger.info(f"iQuant universe cached: {len(codes)} codes")
        return codes

    # --- Core scan logic ---

    async def _run_scan() -> dict | None:
        """Run momentum scan via Sina + scanner. Returns recommendation dict or None."""
        from src.strategy.strategies.momentum_sector_scanner import (
            MomentumSectorScanner,
            PriceSnapshot,
        )

        universe = await _get_universe()
        if not universe:
            raise RuntimeError("Universe is empty")

        sina_client = _state["sina_client"]
        quotes = await sina_client.batch_get_quotes(universe)
        logger.info(f"iQuant scan: Sina returned {len(quotes)} quotes")

        if not quotes:
            return None

        price_snapshots: dict[str, PriceSnapshot] = {}
        for code, quote in quotes.items():
            if not quote.is_trading:
                continue
            price_snapshots[code] = PriceSnapshot(
                stock_code=code,
                stock_name=quote.stock_name,
                open_price=quote.open_price,
                prev_close=quote.prev_close,
                latest_price=quote.latest_price,
                early_volume=quote.volume,
                high_price=quote.high_price,
                low_price=quote.low_price,
            )

        scanner = MomentumSectorScanner(
            ifind_client=_state["historical_adapter"],  # type: ignore[arg-type]
            fundamentals_db=_state["fundamentals_db"],
            concept_mapper=_state["concept_mapper"],
            stock_filter=_state["stock_filter"],
        )

        scan_result = await scanner.scan(price_snapshots, trade_date=None)
        rec = scan_result.recommended_stock
        if not rec:
            return None

        return {
            "stock_code": rec.stock_code,
            "stock_name": rec.stock_name,
            "board_name": rec.board_name,
            "open_price": round(rec.open_price, 4),
            "prev_close": round(rec.prev_close, 4),
            "latest_price": round(rec.latest_price, 4),
            "gain_from_open_pct": round(rec.gain_from_open_pct, 2),
            "turnover_amp": round(rec.turnover_amp, 4),
            "composite_score": round(rec.composite_score, 4),
            "selected_count": len(scan_result.selected_stocks),
            "hot_board_count": len(scan_result.hot_boards),
        }

    # --- Background scheduler ---

    async def _signal_scheduler() -> None:
        """Background task: produces signals at scheduled times.

        09:30 → SELL signals for yesterday's holdings (T+1 at open)
        09:40 → BUY signal from momentum scan
        """
        BUY_AMOUNT = 50000  # yuan per position, TODO: make configurable

        SELL_TIME = time(9, 30)
        SCAN_TIME = time(9, 40)

        logger.info("iQuant signal scheduler started")

        sell_done_date = ""
        scan_done_date = ""

        try:
            while True:
                now = datetime.now(BEIJING_TZ)
                today_str = now.strftime("%Y-%m-%d")
                current_time = now.time()

                # Skip weekends
                if now.weekday() >= 5:
                    await asyncio.sleep(60)
                    continue

                # --- 09:30: Push SELL signals for yesterday's holdings ---
                if current_time >= SELL_TIME and sell_done_date != today_str and _state["holdings"]:
                    sell_done_date = today_str
                    for holding in _state["holdings"]:
                        _push_signal(
                            {
                                "type": "sell",
                                "stock_code": holding["code"],
                                "stock_name": holding.get("name", ""),
                                "volume": holding["volume"],
                                "reason": "T+1 次日开盘卖出",
                            }
                        )
                        await _notify_feishu_signal(_state["pending_signals"][-1])
                    logger.info(f"iQuant: pushed {len(_state['holdings'])} SELL signals")

                # --- 09:40: Run scan, push BUY signal ---
                if current_time >= SCAN_TIME and scan_done_date != today_str:
                    scan_done_date = today_str

                    # Ensure resources are ready (universe, clients)
                    if not _state["initialized"]:
                        await asyncio.sleep(10)
                        continue

                    try:
                        rec = await _run_scan()
                        if rec:
                            price = rec["latest_price"]
                            volume = int(BUY_AMOUNT / price / 100) * 100
                            if volume >= 100:
                                _push_signal(
                                    {
                                        "type": "buy",
                                        "stock_code": rec["stock_code"],
                                        "stock_name": rec["stock_name"],
                                        "board_name": rec["board_name"],
                                        "latest_price": price,
                                        "volume": volume,
                                        "composite_score": rec["composite_score"],
                                        "reason": f"动量扫描推荐 (板块={rec['board_name']})",
                                    }
                                )
                                await _notify_feishu_signal(_state["pending_signals"][-1])
                            else:
                                logger.info(
                                    f"iQuant: price {price} too high for BUY_AMOUNT={BUY_AMOUNT}"
                                )
                        else:
                            logger.info("iQuant scan: no recommendation today")
                    except Exception as e:
                        error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                        logger.error(f"iQuant scan failed: {error_detail}")
                        await _notify_feishu_error("扫描失败", error_detail)

                # Sleep 30s between checks
                await asyncio.sleep(30)

        except asyncio.CancelledError:
            logger.info("iQuant signal scheduler stopped")

    # --- Endpoints ---

    @router.get("/ping")
    async def ping(api_key: str = Depends(_verify_api_key)) -> dict:
        """Health check + trigger lazy init."""
        await _ensure_resources()
        now = datetime.now(BEIJING_TZ)
        return {
            "status": "ok",
            "service": "iquant",
            "server_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "pending_count": len(_state["pending_signals"]),
            "holdings_count": len(_state["holdings"]),
        }

    @router.get("/pending-signals")
    async def pending_signals(api_key: str = Depends(_verify_api_key)) -> dict:
        """Return all pending (unacknowledged) signals.

        iQuant polls this every bar. When signals are present,
        it executes passorder() for each, then acks them.
        """
        return {
            "signals": _state["pending_signals"],
            "count": len(_state["pending_signals"]),
        }

    @router.post("/ack-signal")
    async def ack_signal(
        body: AckRequest,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Acknowledge a signal after iQuant has executed it.

        For BUY signals: the stock is added to holdings (for T+1 sell).
        For SELL signals: the stock is removed from holdings.
        """
        signal_id = body.signal_id
        found = None
        for i, sig in enumerate(_state["pending_signals"]):
            if sig["id"] == signal_id:
                found = _state["pending_signals"].pop(i)
                break

        if not found:
            raise HTTPException(status_code=404, detail=f"Signal {signal_id} not found")

        found["acked_at"] = datetime.now(BEIJING_TZ).strftime("%H:%M:%S")
        _state["executed_signals"].append(found)

        # Update holdings
        if found["type"] == "buy":
            _state["holdings"].append(
                {
                    "code": found["stock_code"],
                    "name": found.get("stock_name", ""),
                    "volume": found["volume"],
                    "buy_price": found.get("latest_price", 0),
                    "buy_date": datetime.now(BEIJING_TZ).strftime("%Y-%m-%d"),
                }
            )
            logger.info(
                f"iQuant: BUY acked {found['stock_code']}, "
                f"added to holdings ({len(_state['holdings'])} total)"
            )
        elif found["type"] == "sell":
            _state["holdings"] = [h for h in _state["holdings"] if h["code"] != found["stock_code"]]
            logger.info(
                f"iQuant: SELL acked {found['stock_code']}, "
                f"removed from holdings ({len(_state['holdings'])} remaining)"
            )

        return {"success": True, "signal": found}

    @router.get("/holdings")
    async def holdings(api_key: str = Depends(_verify_api_key)) -> dict:
        """Return current holdings (for monitoring)."""
        return {"holdings": _state["holdings"]}

    @router.get("/universe")
    async def universe(api_key: str = Depends(_verify_api_key)) -> dict:
        """Return all main-board stock codes (cached)."""
        await _ensure_resources()
        codes = await _get_universe()
        return {"codes": codes, "count": len(codes)}

    @router.post("/quote")
    async def quote(
        body: QuoteRequest,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Get Sina real-time quotes for specific stocks."""
        await _ensure_resources()

        if not body.stock_codes:
            raise HTTPException(status_code=400, detail="stock_codes is required")

        try:
            sina_client = _state["sina_client"]
            quotes = await sina_client.batch_get_quotes(body.stock_codes)
            return {
                "success": True,
                "quotes": {
                    code: {
                        "name": q.stock_name,
                        "open": q.open_price,
                        "prev_close": q.prev_close,
                        "latest": q.latest_price,
                        "high": q.high_price,
                        "low": q.low_price,
                        "volume": q.volume,
                        "amount": q.amount,
                    }
                    for code, q in quotes.items()
                },
            }
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}"
            logger.error(f"iQuant quote failed: {error_detail}")
            await _notify_feishu_error("行情获取失败", error_detail)
            raise HTTPException(status_code=500, detail=error_detail)

    return router
