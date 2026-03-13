# === MODULE PURPOSE ===
# API endpoints for iQuant scripts running on a Windows server.
# Fully isolated from the main online system — creates own DB pool,
# own Tushare client, own historical adapter. No shared app.state resources.
#
# === ARCHITECTURE ===
# Push-based: server runs background tasks that produce signals at the right time.
# iQuant polls /pending-signals every bar, executes immediately, then acks.
#
# === V15 STRATEGY ===
# Signal flow (T+2 adaptive sell):
#   09:25-09:35  → GAP CHECK: T+1 gap < -3% → mark early sell; T+2 → mark sell
#   09:38-10:00  → SCAN: if no holdings, run V15 7-layer funnel → BUY signal
#   14:50-14:58  → SELL: push SELL signals for marked holdings
#   iQuant       → polls /pending-signals → passorder() → POST /ack-signal
#
# === AUTHENTICATION ===
# All endpoints require X-API-Key header matching IQUANT_API_KEY env var.

from __future__ import annotations

import asyncio
import json
import logging
import traceback
import uuid
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

# Holdings persistence file (trading safety: must survive restart)
_HOLDINGS_PATH = Path("data/v15_holdings.json")

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


class ManualOrderRequest(BaseModel):
    """Request body for /api/iquant/manual-order (live testing)."""

    stock_code: str  # e.g. "601398"
    direction: str = "buy"  # "buy" or "sell"
    quantity: int = 100  # 股数 (1手=100股)
    price: float | None = None  # 指定价格; None=市价
    price_type: str = "market"  # "market" or "limit"
    reason: str = "手动测试单"


class BacktestScanRequest(BaseModel):
    """Request body for /api/iquant/backtest-scan."""

    trade_date: str  # YYYY-MM-DD
    data_source: str = "tsanghi"  # "tsanghi" or "ifind"


# --- Feishu notification helpers ---


async def _notify_feishu_error(title: str, detail: str) -> None:
    """Send error alert to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_alert(f"[V15] {title}", detail)
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
            f"[V15] {direction}信号",
            f"股票: {signal['stock_code']} {signal.get('stock_name', '')}",
        ]
        if signal["type"] == "buy":
            lines.append(f"板块: {signal.get('board_name', '-')}")
            lines.append(f"价格: {signal.get('latest_price', '-')}")
            lines.append(f"V3评分: {signal.get('v3_score', '-')}")
        if signal["type"] == "sell":
            lines.append(f"原因: {signal.get('reason', '-')}")
        lines.append(f"时间: {signal.get('created_at', '')}")

        await bot.send_message("\n".join(lines))
    except Exception:
        logger.warning("Failed to send Feishu signal notification", exc_info=True)


# --- Holdings persistence ---


def _save_holdings(holdings: list[dict]) -> None:
    """Persist holdings to disk. Trading safety: must not lose holdings on restart."""
    _HOLDINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    _HOLDINGS_PATH.write_text(json.dumps(holdings, ensure_ascii=False, indent=2))
    logger.info(f"V15 holdings saved ({len(holdings)} items)")


def _load_holdings() -> list[dict]:
    """Load holdings from disk on startup."""
    if not _HOLDINGS_PATH.exists():
        return []
    try:
        data = json.loads(_HOLDINGS_PATH.read_text())
        if not isinstance(data, list):
            raise ValueError("Holdings file is not a list")
        logger.info(f"V15 holdings loaded from disk ({len(data)} items)")
        return data
    except (json.JSONDecodeError, ValueError) as e:
        raise RuntimeError(
            f"V15 holdings file corrupt: {e}. "
            f"Cannot trade with unknown position state. Fix {_HOLDINGS_PATH} manually."
        )


# --- Trade calendar ---

_trade_calendar_cache: list[date] | None = None


async def _get_trade_calendar() -> list[date]:
    """Get A-share trade calendar (cached). Uses akshare."""
    global _trade_calendar_cache
    if _trade_calendar_cache is not None:
        return _trade_calendar_cache

    import akshare as ak

    df = await asyncio.to_thread(ak.tool_trade_date_hist_sina)
    _trade_calendar_cache = sorted(df["trade_date"].dt.date)
    logger.info(f"Trade calendar cached: {len(_trade_calendar_cache)} dates")
    return _trade_calendar_cache


def _count_trading_days(calendar: list[date], from_date: date, to_date: date) -> int:
    """Count trading days between two dates (exclusive from, inclusive to)."""
    return sum(1 for d in calendar if from_date < d <= to_date)


# --- Router factory ---


def create_iquant_router() -> APIRouter:
    """Create the iQuant API router with V15 strategy.

    V15 signal scheduler:
    - 09:25-09:35: GAP CHECK (T+1 gap <-3% → early sell, T+2 → sell)
    - 09:38-10:00: V15 SCAN (if no holdings → BUY signal)
    - 14:50-14:58: SELL (push sell signals for marked holdings)
    """
    router = APIRouter(prefix="/api/iquant", tags=["iquant"])

    # Isolated state (not shared with main app.state)
    _state: dict[str, Any] = {
        "initialized": False,
        "pending_signals": [],  # signals waiting for iQuant to execute
        "executed_signals": [],  # acked signals (history)
        "holdings": [],  # V15: [{code, name, buy_date, entry_price, marked_sell_today, early_exit}]
        "scheduler_task": None,
        "universe_cache": None,
        "tsanghi_cache": None,  # injected from app.py after OSS load
    }

    # --- Resource management ---

    async def _ensure_resources() -> dict[str, Any]:
        """Lazily initialize iQuant-specific resources and start scheduler."""
        if _state["initialized"]:
            return _state

        from src.common.config import get_tushare_token
        from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter
        from src.data.clients.tushare_realtime import TushareRealtimeClient
        from src.data.database.fundamentals_db import create_fundamentals_db_from_config
        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig

        tushare_token = get_tushare_token()
        tushare = TushareRealtimeClient(token=tushare_token)
        await tushare.start()
        _state["realtime_client"] = tushare

        fdb = create_fundamentals_db_from_config()
        await fdb.connect()
        _state["fundamentals_db"] = fdb

        # Build historical adapter with OSS cache if available
        cache = _state.get("tsanghi_cache")
        _state["historical_adapter"] = IQuantHistoricalAdapter(tushare, cache=cache)
        _state["concept_mapper"] = LocalConceptMapper()
        # V15 filter: main board + SME (002), exclude ChiNext (300) + STAR (688) + BSE
        _state["stock_filter"] = StockFilter(
            StockFilterConfig(
                exclude_bse=True,
                exclude_chinext=True,
                exclude_star=True,
                exclude_sme=False,
            )
        )

        # Load persisted holdings
        _state["holdings"] = _load_holdings()

        # Pre-load trade calendar
        await _get_trade_calendar()

        # Start V15 background scheduler
        _state["scheduler_task"] = asyncio.create_task(_signal_scheduler())

        _state["initialized"] = True
        logger.info("V15 iQuant resources initialized + scheduler started")
        return _state

    async def _cleanup_resources() -> None:
        """Cleanup on shutdown."""
        task = _state.get("scheduler_task")
        if task and not task.done():
            task.cancel()
        rt_client = _state.get("realtime_client")
        if rt_client:
            await rt_client.stop()
        fdb = _state.get("fundamentals_db")
        if fdb:
            await fdb.close()
        _state["initialized"] = False
        logger.info("V15 iQuant resources cleaned up")

    router._iquant_cleanup = _cleanup_resources  # type: ignore[attr-defined]

    # --- Cache injection (called from app.py after OSS load) ---

    def _inject_cache(cache: Any) -> None:
        """Inject OSS cache and rebuild historical adapter."""
        from src.data.clients.iquant_historical_adapter import IQuantHistoricalAdapter

        _state["tsanghi_cache"] = cache
        rt_client = _state.get("realtime_client")
        if rt_client:
            _state["historical_adapter"] = IQuantHistoricalAdapter(rt_client, cache=cache)
            logger.info("V15: OSS cache injected, historical adapter rebuilt")
        else:
            logger.info("V15: OSS cache stored (adapter will be built on init)")

    router._inject_cache = _inject_cache  # type: ignore[attr-defined]

    # --- Signal helpers ---

    def _push_signal(signal: dict) -> None:
        """Add a signal to the pending queue."""
        signal.setdefault("id", str(uuid.uuid4())[:8])
        signal.setdefault("created_at", datetime.now(BEIJING_TZ).strftime("%H:%M:%S"))
        _state["pending_signals"].append(signal)
        logger.info(
            f"V15 signal pushed: {signal['type']} {signal['stock_code']} (id={signal['id']})"
        )

    # --- Universe ---

    async def _get_universe() -> list[str]:
        """Get stock codes for V15 universe (main board + SME, cached)."""
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
        logger.info(f"V15 universe cached: {len(codes)} codes")
        return codes

    # --- V15 scan logic ---

    async def _run_v15_scan() -> dict | None:
        """Run V15 scan via Tushare + V15Scanner. Returns recommendation dict or None."""
        from src.strategy.strategies.momentum_sector_scanner import PriceSnapshot
        from src.strategy.strategies.v15_scanner import V15Scanner

        universe = await _get_universe()
        if not universe:
            raise RuntimeError("Universe is empty")

        rt_client = _state["realtime_client"]
        quotes = await rt_client.batch_get_quotes(universe)
        logger.info(f"V15 scan: Tushare returned {len(quotes)} quotes")

        if not quotes:
            return None

        # Fetch prev_close from Tushare daily API
        today = datetime.now(BEIJING_TZ).date()
        prev_closes: dict[str, float] = {}
        for days_back in range(1, 8):
            prev_date = today - timedelta(days=days_back)
            prev_date_str = prev_date.strftime("%Y%m%d")
            try:
                prev_closes = await rt_client.fetch_prev_closes(prev_date_str)
                if prev_closes:
                    logger.info(f"V15: preClose from Tushare daily date={prev_date_str}")
                    break
            except Exception as e:
                logger.warning(f"V15: failed to fetch daily for {prev_date_str}: {e}")
                continue

        # Build PriceSnapshot dict
        price_snapshots: dict[str, PriceSnapshot] = {}
        skipped = 0
        for code, quote in quotes.items():
            if not quote.is_trading:
                continue
            prev_close = prev_closes.get(code, 0.0)
            if prev_close <= 0:
                skipped += 1
                continue
            price_snapshots[code] = PriceSnapshot(
                stock_code=code,
                stock_name="",
                open_price=quote.open_price,
                prev_close=prev_close,
                latest_price=quote.latest_price,
                early_volume=quote.volume,
                high_price=quote.high_price,
                low_price=quote.low_price,
            )

        if skipped:
            logger.warning(f"V15 scan: skipped {skipped} stocks (no prev_close)")

        scanner = V15Scanner(
            historical_adapter=_state["historical_adapter"],
            fundamentals_db=_state["fundamentals_db"],
            concept_mapper=_state["concept_mapper"],
        )

        scan_result = await scanner.scan(price_snapshots)
        rec = scan_result.recommended
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
            "v3_score": round(rec.v3_score, 6),
            "hot_board_count": scan_result.hot_board_count,
            "final_candidates": scan_result.final_candidates,
        }

    # --- Gap check logic ---

    async def _gap_check_holdings(today_date: date) -> None:
        """Check each holding for T+1 gap-down or T+2 sell."""
        calendar = await _get_trade_calendar()
        rt_client = _state["realtime_client"]

        for holding in _state["holdings"]:
            buy_date = date.fromisoformat(holding["buy_date"])
            days_held = _count_trading_days(calendar, buy_date, today_date)
            entry_price = holding.get("entry_price", 0.0)

            if days_held <= 0:
                # Same day as buy (T+0) — cannot sell in A-share
                continue

            if days_held == 1:
                # T+1: check gap
                if entry_price <= 0:
                    # No entry price recorded — mark early sell for safety
                    holding["marked_sell_today"] = True
                    holding["early_exit"] = True
                    logger.warning(
                        f"V15 gap check: {holding['code']} no entry_price, marking early exit"
                    )
                    continue

                quotes = await rt_client.batch_get_quotes([holding["code"]])
                quote = quotes.get(holding["code"])
                if not quote or not quote.is_trading:
                    holding["marked_sell_today"] = True
                    holding["early_exit"] = True
                    logger.warning(
                        f"V15 gap check: {holding['code']} no quote available, marking early exit"
                    )
                    await _notify_feishu_error(
                        "V15跳空检测失败",
                        f"{holding['code']} {holding.get('name', '')} 无法获取开盘价，标记早卖",
                    )
                    continue

                gap_pct = (quote.open_price - entry_price) / entry_price
                if gap_pct < -0.03:
                    holding["marked_sell_today"] = True
                    holding["early_exit"] = True
                    logger.info(
                        f"V15 gap check: {holding['code']} gap={gap_pct:.2%} < -3%, "
                        f"marking early exit (entry={entry_price:.2f}, open={quote.open_price:.2f})"
                    )
                    await _notify_feishu_error(
                        "V15跳空止损",
                        f"{holding['code']} {holding.get('name', '')} "
                        f"开盘跳空 {gap_pct:.2%}\n"
                        f"买入价: {entry_price:.2f}, 今开: {quote.open_price:.2f}\n"
                        f"将于14:57卖出",
                    )
                else:
                    logger.info(
                        f"V15 gap check: {holding['code']} gap={gap_pct:.2%} >= -3%, "
                        f"holding through T+1"
                    )

            elif days_held >= 2:
                # T+2: sell today
                holding["marked_sell_today"] = True
                holding["early_exit"] = False
                logger.info(
                    f"V15: {holding['code']} T+2 reached (days_held={days_held}), "
                    f"marking for sell today"
                )

        _save_holdings(_state["holdings"])

    # --- V15 Background scheduler ---

    async def _signal_scheduler() -> None:
        """V15 background scheduler: T+2 adaptive sell.

        Three timing windows:
        1. GAP_CHECK (09:25-09:35): Check holdings for gap-down or T+2 sell
        2. SCAN (09:38-10:00): If no holdings, run V15 scan → BUY signal
        3. SELL (14:50-14:58): Push SELL signals for marked holdings

        Timing uses Beijing TZ (local clock).
        """
        GAP_CHECK_WINDOW = (time(9, 25), time(9, 35))
        SCAN_WINDOW = (time(9, 38), time(10, 0))
        SELL_WINDOW = (time(14, 50), time(14, 58))

        logger.info("V15 signal scheduler started")

        gap_done_date = ""
        scan_done_date = ""
        sell_done_date = ""

        try:
            while True:
                now_bj = datetime.now(BEIJING_TZ)
                ex_date = now_bj.strftime("%Y-%m-%d")
                ex_time = now_bj.time().replace(second=0, microsecond=0)
                today_date = now_bj.date()

                # --- GAP CHECK: 09:25-09:35 ---
                if (
                    gap_done_date != ex_date
                    and GAP_CHECK_WINDOW[0] <= ex_time <= GAP_CHECK_WINDOW[1]
                    and _state["holdings"]
                ):
                    if not _state["initialized"]:
                        await asyncio.sleep(10)
                        continue
                    gap_done_date = ex_date
                    try:
                        await _gap_check_holdings(today_date)
                    except Exception as e:
                        error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                        logger.error(f"V15 gap check failed: {error_detail}")
                        await _notify_feishu_error("V15跳空检测失败", error_detail)

                # Gap check deadline
                if gap_done_date != ex_date and ex_time > GAP_CHECK_WINDOW[1]:
                    gap_done_date = ex_date
                    if _state["holdings"]:
                        logger.info("V15: past GAP_CHECK window, skipping")

                # --- SCAN: 09:38-10:00 ---
                if scan_done_date != ex_date and SCAN_WINDOW[0] <= ex_time <= SCAN_WINDOW[1]:
                    if not _state["initialized"]:
                        await asyncio.sleep(10)
                        continue

                    scan_done_date = ex_date

                    if _state["holdings"]:
                        logger.info("V15: holdings exist, skipping scan")
                    else:
                        try:
                            rec = await _run_v15_scan()
                            if rec:
                                _push_signal(
                                    {
                                        "type": "buy",
                                        "stock_code": rec["stock_code"],
                                        "stock_name": rec["stock_name"],
                                        "board_name": rec["board_name"],
                                        "latest_price": rec["latest_price"],
                                        "v3_score": rec["v3_score"],
                                        "reason": f"V15推荐 (板块={rec['board_name']}, "
                                        f"score={rec['v3_score']:.4f})",
                                    }
                                )
                                await _notify_feishu_signal(_state["pending_signals"][-1])
                            else:
                                logger.info("V15 scan: no recommendation today")
                        except Exception as e:
                            error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                            logger.error(f"V15 scan failed: {error_detail}")
                            await _notify_feishu_error("V15扫描失败", error_detail)

                # Scan deadline
                if scan_done_date != ex_date and ex_time > SCAN_WINDOW[1]:
                    scan_done_date = ex_date

                # --- SELL: 14:50-14:58 ---
                if sell_done_date != ex_date and SELL_WINDOW[0] <= ex_time <= SELL_WINDOW[1]:
                    marked = [h for h in _state["holdings"] if h.get("marked_sell_today")]
                    if marked:
                        sell_done_date = ex_date
                        for h in marked:
                            reason = "V15尾盘卖出"
                            if h.get("early_exit"):
                                reason += " (T+1跳空止损)"
                            else:
                                reason += " (T+2到期)"
                            _push_signal(
                                {
                                    "type": "sell",
                                    "stock_code": h["code"],
                                    "stock_name": h.get("name", ""),
                                    "reason": reason,
                                }
                            )
                            await _notify_feishu_signal(_state["pending_signals"][-1])
                        logger.info(f"V15: pushed {len(marked)} SELL signals for marked holdings")

                # Sell deadline
                if sell_done_date != ex_date and ex_time > SELL_WINDOW[1]:
                    sell_done_date = ex_date

                # Adaptive sleep
                all_done = (
                    gap_done_date == ex_date
                    and scan_done_date == ex_date
                    and sell_done_date == ex_date
                )
                await asyncio.sleep(120 if all_done else 30)

        except asyncio.CancelledError:
            logger.info("V15 signal scheduler stopped")

    # --- Endpoints ---

    @router.get("/ping")
    async def ping(api_key: str = Depends(_verify_api_key)) -> dict:
        """Health check + trigger lazy init."""
        await _ensure_resources()
        now = datetime.now(BEIJING_TZ)
        return {
            "status": "ok",
            "service": "iquant-v15",
            "server_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "pending_count": len(_state["pending_signals"]),
            "holdings_count": len(_state["holdings"]),
            "holdings": _state["holdings"],
        }

    @router.get("/pending-signals")
    async def pending_signals(api_key: str = Depends(_verify_api_key)) -> dict:
        """Return all pending (unacknowledged) signals."""
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

        For BUY signals: stock is added to holdings with entry_price.
        For SELL signals: stock is removed from holdings.
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

        if found["type"] == "buy":
            _state["holdings"].append(
                {
                    "code": found["stock_code"],
                    "name": found.get("stock_name", ""),
                    "buy_date": datetime.now(BEIJING_TZ).strftime("%Y-%m-%d"),
                    "entry_price": found.get("latest_price", 0.0),
                    "marked_sell_today": False,
                    "early_exit": False,
                }
            )
            _save_holdings(_state["holdings"])
            logger.info(
                f"V15: BUY acked {found['stock_code']} @ {found.get('latest_price', '?')}, "
                f"added to holdings ({len(_state['holdings'])} total)"
            )
        elif found["type"] == "sell":
            _state["holdings"] = [
                h for h in _state["holdings"] if h["code"] != found["stock_code"]
            ]
            _save_holdings(_state["holdings"])
            logger.info(
                f"V15: SELL acked {found['stock_code']}, "
                f"removed from holdings ({len(_state['holdings'])} remaining)"
            )

        return {"success": True, "signal": found}

    @router.get("/holdings")
    async def holdings(api_key: str = Depends(_verify_api_key)) -> dict:
        """Return current holdings (for monitoring)."""
        return {"holdings": _state["holdings"]}

    @router.post("/manual-order")
    async def manual_order(
        body: ManualOrderRequest,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Push a manual BUY/SELL signal for live trading tests.

        The signal enters the same pending queue as V15 signals.
        iQuant polls /pending-signals and executes via passorder().

        Example: POST /api/iquant/manual-order
        {
            "stock_code": "601398",
            "direction": "buy",
            "quantity": 100,
            "price_type": "market",
            "reason": "手动测试单"
        }
        """
        if body.direction not in ("buy", "sell"):
            raise HTTPException(status_code=400, detail="direction must be 'buy' or 'sell'")
        if body.quantity <= 0 or body.quantity % 100 != 0:
            raise HTTPException(status_code=400, detail="quantity must be positive multiple of 100")
        if body.price_type == "limit" and (body.price is None or body.price <= 0):
            raise HTTPException(status_code=400, detail="limit order requires positive price")

        signal = {
            "type": body.direction,
            "stock_code": body.stock_code,
            "stock_name": "",
            "quantity": body.quantity,
            "price": body.price,
            "price_type": body.price_type,
            "reason": body.reason,
            "manual": True,
        }
        _push_signal(signal)

        logger.info(
            f"Manual order pushed: {body.direction} {body.stock_code} "
            f"qty={body.quantity} price_type={body.price_type} price={body.price}"
        )

        return {
            "success": True,
            "signal": _state["pending_signals"][-1],
            "message": "Signal pushed. iQuant will pick it up on next /pending-signals poll.",
        }

    @router.get("/universe")
    async def universe(api_key: str = Depends(_verify_api_key)) -> dict:
        """Return all stock codes in V15 universe (cached)."""
        await _ensure_resources()
        codes = await _get_universe()
        return {"codes": codes, "count": len(codes)}

    @router.post("/quote")
    async def quote(
        body: QuoteRequest,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Get Tushare real-time quotes for specific stocks."""
        await _ensure_resources()

        if not body.stock_codes:
            raise HTTPException(status_code=400, detail="stock_codes is required")

        try:
            rt_client = _state["realtime_client"]
            quotes = await rt_client.batch_get_quotes(body.stock_codes)
            return {
                "success": True,
                "quotes": {
                    code: {
                        "name": "",
                        "open": q.open_price,
                        "prev_close": 0.0,
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
            logger.error(f"V15 quote failed: {error_detail}")
            await _notify_feishu_error("行情获取失败", error_detail)
            raise HTTPException(status_code=500, detail=error_detail)

    @router.post("/backtest-scan")
    async def backtest_scan(
        request: Request,
        body: BacktestScanRequest,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Run momentum scan for a specific historical date (legacy backtest).

        Uses the old MomentumSectorScanner for backtest compatibility.
        """
        from src.data.clients.tsanghi_backtest_cache import TsanghiHistoricalAdapter
        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.filters.board_relevance_filter import create_board_relevance_filter
        from src.strategy.strategies.momentum_sector_scanner import MomentumSectorScanner
        from src.web.routes import MinuteDataMissingError, _build_snapshots_from_cache

        try:
            trade_date = datetime.strptime(body.trade_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid date: {body.trade_date}")

        await _ensure_resources()

        if body.data_source == "tsanghi":
            ak_cache = getattr(request.app.state, "tsanghi_cache", None)
            if not ak_cache and getattr(request.app.state, "tsanghi_cache_loading", False):
                logger.info("backtest-scan: tsanghi cache loading from OSS, waiting...")
                for _ in range(90):
                    await asyncio.sleep(1)
                    ak_cache = getattr(request.app.state, "tsanghi_cache", None)
                    if ak_cache:
                        break
                    if not getattr(request.app.state, "tsanghi_cache_loading", False):
                        break

            if not ak_cache:
                raise HTTPException(
                    status_code=503,
                    detail="沧海缓存未加载。请先在 web 页面的回测页下载数据。",
                )

            date_key = trade_date.strftime("%Y-%m-%d")
            try:
                price_snapshots = _build_snapshots_from_cache(ak_cache, date_key)
            except MinuteDataMissingError as e:
                raise HTTPException(status_code=400, detail=str(e))

            if not price_snapshots:
                return {"recommendation": None, "reason": f"No data for {date_key}"}

            adapter = TsanghiHistoricalAdapter(ak_cache)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported data_source: {body.data_source}. Use 'tsanghi'.",
            )

        concept_mapper = LocalConceptMapper()
        scanner = MomentumSectorScanner(
            ifind_client=adapter,  # type: ignore[arg-type]
            fundamentals_db=_state["fundamentals_db"],
            concept_mapper=concept_mapper,
            board_relevance_filter=create_board_relevance_filter(),
        )

        scan_result = await scanner.scan(price_snapshots, trade_date=trade_date)
        rec = scan_result.recommended_stock

        if not rec:
            return {"recommendation": None, "reason": "No recommendation for this date"}

        return {
            "recommendation": {
                "stock_code": rec.stock_code,
                "stock_name": rec.stock_name,
                "board_name": rec.board_name,
                "latest_price": round(rec.latest_price, 4),
                "open_price": round(rec.open_price, 4),
                "prev_close": round(rec.prev_close, 4),
                "composite_score": round(rec.composite_score, 4),
            }
        }

    return router
