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
# NOTE: Auto buy/sell signal pushing is DISABLED. Scheduler only scans + reports.
#       Manual orders via /manual-order endpoint still work.
# Signal flow (T+2 adaptive sell):
#   09:31-09:35  → GAP CHECK: T+1 gap < -3% → mark early sell; T+2 → mark sell
#   09:38-10:00  → SCAN: run V15 7-layer funnel → Feishu report (no auto BUY)
#   14:50-14:58  → SELL: log marked holdings (no auto SELL)
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
from datetime import date, datetime, time
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


async def _notify_feishu_ack(signal: dict) -> None:
    """Send execution confirmation to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if not bot.is_configured():
            return

        direction = "买入" if signal["type"] == "buy" else "卖出"
        pushed = signal.get("created_at", "?")
        acked = signal.get("acked_at", "?")
        lines = [
            f"[V15] {direction}已执行",
            f"股票: {signal['stock_code']} {signal.get('stock_name', '')}",
        ]
        if signal["type"] == "buy":
            lines.append(f"价格: {signal.get('latest_price', '-')}")
            lines.append(f"板块: {signal.get('board_name', '-')}")
        if signal["type"] == "sell":
            lines.append(f"原因: {signal.get('reason', '-')}")
        lines.append(f"推送→执行: {pushed} → {acked}")

        await bot.send_message("\n".join(lines))
    except Exception:
        logger.warning("Failed to send Feishu ack notification", exc_info=True)


async def _notify_feishu_v15_top5(scan_result) -> None:
    """Send V15 top-5 scored report to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_v15_top5_report(scan_result)
    except Exception:
        logger.warning("Failed to send Feishu V15 top-5 report", exc_info=True)


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
    """Get A-share trade calendar (cached). Uses Tushare trade_cal."""
    global _trade_calendar_cache
    if _trade_calendar_cache is not None:
        return _trade_calendar_cache

    from src.data.clients.tushare_realtime import get_tushare_trade_calendar

    # Fetch a wide range covering all needed dates
    date_strs = await get_tushare_trade_calendar("2020-01-01", "2030-12-31")
    _trade_calendar_cache = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in date_strs)
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
        "broker_positions": [],  # Actual broker positions: [{code, volume}]
        "scheduler_task": None,
        "universe_cache": None,
        "backtest_cache": None,  # injected from app.py after GreptimeDB connect
        # --- Monitoring ---
        "last_poll_time": None,  # datetime: last time iQuant polled /pending-signals
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
        from src.data.database.v15_scan_db import create_v15_scan_db_from_config
        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig

        tushare_token = get_tushare_token()
        tushare = TushareRealtimeClient(token=tushare_token)
        try:
            await tushare.start()
        except Exception as e:
            await _notify_feishu_error(
                "Tushare连接失败",
                f"Tushare实时行情客户端启动失败\n错误: {e}\n"
                f"V15交易功能不可用，请检查Tushare token和网络",
            )
            raise
        _state["realtime_client"] = tushare

        fdb = create_fundamentals_db_from_config()
        try:
            await fdb.connect()
        except Exception as e:
            await _notify_feishu_error(
                "数据库连接失败",
                f"PostgreSQL基本面数据库连接失败\n错误: {e}\nV15交易功能不可用，请检查数据库配置",
            )
            raise
        _state["fundamentals_db"] = fdb

        # V15 scan history DB (non-critical — log and continue on failure)
        try:
            v15db = create_v15_scan_db_from_config()
            await v15db.connect()
            _state["v15_scan_db"] = v15db
        except Exception as e:
            logger.warning(f"V15ScanDB init failed (scan history disabled): {e}")
            _state["v15_scan_db"] = None

        _state["historical_adapter"] = IQuantHistoricalAdapter(tushare)
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
        try:
            await _get_trade_calendar()
        except Exception as e:
            await _notify_feishu_error(
                "交易日历加载失败",
                f"无法加载交易日历(Tushare)\n错误: {e}\n跳空检测将无法判断交易日",
            )
            raise

        # Start V15 background scheduler
        _state["scheduler_task"] = asyncio.create_task(_signal_scheduler())

        _state["initialized"] = True
        logger.info("V15 iQuant resources initialized + scheduler started")
        return _state

    async def _cleanup_resources() -> None:
        """Cleanup on shutdown."""
        for key in ("scheduler_task", "monitoring_task"):
            task = _state.get(key)
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
    router._iquant_init = _ensure_resources  # type: ignore[attr-defined]

    # --- Cache injection (called from app.py after GreptimeDB connect) ---

    def _inject_cache(cache: Any) -> None:
        """Inject GreptimeDB backtest cache for preClose lookups."""
        _state["backtest_cache"] = cache
        logger.info("V15: GreptimeDB backtest cache injected")

    router._inject_cache = _inject_cache  # type: ignore[attr-defined]

    # --- Signal helpers ---

    def _push_signal(signal: dict) -> None:
        """Add a signal to the pending queue."""
        now = datetime.now(BEIJING_TZ)
        signal.setdefault("id", str(uuid.uuid4())[:8])
        signal.setdefault("created_at", now.strftime("%H:%M:%S"))
        signal["pushed_at"] = now  # datetime for timeout tracking (not serialized)
        _state["pending_signals"].append(signal)
        logger.info(
            f"V15 signal pushed: {signal['type']} {signal['stock_code']} (id={signal['id']})"
        )

    # --- Universe ---

    async def _get_universe() -> list[str]:
        """Get stock codes for V15 universe (main board + SME, cached)."""
        if _state["universe_cache"]:
            return _state["universe_cache"]

        rt_client = _state["realtime_client"]
        all_codes = await rt_client.fetch_stock_list()
        stock_filter = _state["stock_filter"]
        codes = [c for c in all_codes if stock_filter.is_allowed(c)]
        _state["universe_cache"] = codes
        logger.info(f"V15 universe cached: {len(codes)} codes")
        return codes

    # --- V15 scan logic ---

    async def _run_v15_scan() -> dict[str, Any] | None:
        """Run V15 scan via Tushare + V15Scanner. Returns recommendation dict or None."""
        from src.strategy.models import PriceSnapshot
        from src.strategy.strategies.v15_scanner import V15Scanner, V15ScoredStock

        universe = await _get_universe()
        if not universe:
            raise RuntimeError("Universe is empty")

        rt_client = _state["realtime_client"]
        quotes = await rt_client.batch_get_early_quotes(universe)
        logger.info(f"V15 scan: Tushare rt_min_daily returned {len(quotes)} quotes")

        if not quotes:
            return None

        # Get prev_close (previous trading day's close).
        # L6.5 limit-up check needs real prev_close — NEVER approximate with open_price
        # because gap-up stocks would get wrong limit price.
        prev_closes: dict[str, float] = {}
        today = datetime.now(BEIJING_TZ).date()
        calendar = await _get_trade_calendar()
        prev_dates = [d for d in calendar if d < today]
        if not prev_dates:
            raise RuntimeError("V15 scan: no previous trading day found in calendar")
        prev_trade_date = prev_dates[-1].strftime("%Y-%m-%d")

        # Source 1: GreptimeDB cache (instant SQL query)
        cache = _state.get("backtest_cache")
        if cache and cache.is_ready:
            all_daily = await cache.get_all_codes_with_daily(prev_trade_date)
            for code, daily in all_daily.items():
                close_val = daily.close
                if close_val and close_val > 0:
                    prev_closes[code] = close_val

        # Source 2: tsanghi API fallback (2 calls for XSHG+XSHE)
        # Use API if cache coverage < 80% of live quotes (partial cache is dangerous)
        if len(prev_closes) < len(quotes) * 0.8:
            from src.data.clients.tsanghi_client import TsanghiClient

            ts_client = TsanghiClient()
            await ts_client.start()
            try:
                for exchange in ("XSHG", "XSHE"):
                    records = await ts_client.daily_latest(exchange, prev_trade_date)
                    for row in records:
                        ticker = str(row.get("ticker", ""))
                        close_val = row.get("close")
                        if ticker and len(ticker) == 6 and close_val:
                            prev_closes[ticker] = float(close_val)
            finally:
                await ts_client.stop()

        if not prev_closes:
            raise RuntimeError(
                f"V15 scan: failed to get prev_close for {prev_trade_date} "
                f"from both OSS cache and tsanghi API"
            )
        logger.info(f"V15 scan: prev_close ({prev_trade_date}): {len(prev_closes)} stocks")

        # Build PriceSnapshot dict
        price_snapshots: dict[str, PriceSnapshot] = {}
        for code, quote in quotes.items():
            if not quote.is_trading:
                continue
            price_snapshots[code] = PriceSnapshot(
                stock_code=code,
                stock_name="",
                open_price=quote.open_price,
                prev_close=prev_closes.get(code, 0.0),
                latest_price=quote.early_close,
                early_volume=quote.early_volume,
                high_price=quote.early_high,
                low_price=quote.early_low,
            )

        scanner = V15Scanner(
            historical_adapter=_state["historical_adapter"],
            fundamentals_db=_state["fundamentals_db"],
            concept_mapper=_state["concept_mapper"],
        )

        scan_result = await scanner.scan(price_snapshots)
        today = datetime.now(BEIJING_TZ).date()

        # Persist top-5 scored stocks (non-critical, never blocks trading)
        if scan_result.all_scored and _state.get("v15_scan_db"):
            try:
                await _state["v15_scan_db"].save_top_n(
                    today, scan_result.all_scored, scan_result.final_candidates, n=10
                )
            except Exception as e:
                logger.warning(f"V15ScanDB save failed: {e}")

        # Push top-5 report to Feishu (non-critical)
        await _notify_feishu_v15_top5(scan_result)

        rec: V15ScoredStock | None = scan_result.recommended
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

    # --- Monitoring helpers ---

    SIGNAL_TIMEOUT_MINUTES = 5  # alert if signal not acked within this time
    SIGNAL_EXPIRY_MINUTES = 10  # auto-expire signal after this time (no execution)
    HEARTBEAT_TIMEOUT_MINUTES = 3  # alert if iQuant stops polling for this long
    TRADING_HOURS = (time(9, 30), time(15, 0))

    async def _check_signal_timeout(now_bj: datetime) -> None:
        """Alert if any pending signal has not been acked within SIGNAL_TIMEOUT_MINUTES."""
        for sig in _state["pending_signals"]:
            pushed_at = sig.get("pushed_at")
            if not pushed_at:
                continue
            age_minutes = (now_bj - pushed_at).total_seconds() / 60
            # Only alert once per signal (mark it)
            if age_minutes >= SIGNAL_TIMEOUT_MINUTES and not sig.get("_timeout_alerted"):
                sig["_timeout_alerted"] = True
                direction = "买入" if sig["type"] == "buy" else "卖出"
                detail = (
                    f"{direction}信号未执行!\n"
                    f"股票: {sig['stock_code']} {sig.get('stock_name', '')}\n"
                    f"推送时间: {sig.get('created_at', '')}\n"
                    f"已等待: {age_minutes:.0f}分钟\n"
                    f"可能原因: iQuant/QMT掉线或未运行"
                )
                logger.error(f"V15 signal timeout: {sig['stock_code']} ({age_minutes:.0f}min)")
                await _notify_feishu_error("信号超时未执行", detail)

    async def _expire_stale_signals(now_bj: datetime) -> None:
        """Remove signals older than SIGNAL_EXPIRY_MINUTES. Alert + discard."""
        still_pending: list[dict] = []
        for sig in _state["pending_signals"]:
            pushed_at = sig.get("pushed_at")
            if not pushed_at:
                still_pending.append(sig)
                continue
            age_minutes = (now_bj - pushed_at).total_seconds() / 60
            if age_minutes >= SIGNAL_EXPIRY_MINUTES:
                direction = "买入" if sig["type"] == "buy" else "卖出"
                detail = (
                    f"{direction}信号已过期作废!\n"
                    f"股票: {sig['stock_code']} {sig.get('stock_name', '')}\n"
                    f"推送时间: {sig.get('created_at', '')}\n"
                    f"过期时长: {age_minutes:.0f}分钟\n"
                    f"信号已自动移除，如需交易请手动下单"
                )
                logger.error(
                    f"V15 signal expired: {sig['stock_code']} ({age_minutes:.0f}min), removed"
                )
                await _notify_feishu_error("信号过期作废", detail)
            else:
                still_pending.append(sig)
        _state["pending_signals"] = still_pending

    async def _check_heartbeat(now_bj: datetime, alert_sent_date: str) -> str:
        """Alert if iQuant has not polled during trading hours. Returns updated alert date."""
        ex_date = now_bj.strftime("%Y-%m-%d")
        ex_time = now_bj.time()

        # Only check during trading hours
        if not (TRADING_HOURS[0] <= ex_time <= TRADING_HOURS[1]):
            return alert_sent_date

        # Don't re-alert same day
        if alert_sent_date == ex_date:
            return alert_sent_date

        last_poll = _state.get("last_poll_time")

        if last_poll is None:
            # Never polled today — alert after 09:33 (give 3min for startup)
            if ex_time >= time(9, 33):
                logger.error("V15 heartbeat: iQuant has NEVER polled today")
                await _notify_feishu_error(
                    "iQuant未连接",
                    "iQuant脚本今天从未连接服务器\n请检查QMT是否已启动并运行iquant_live.py",
                )
                return ex_date
        else:
            gap_minutes = (now_bj - last_poll).total_seconds() / 60
            if gap_minutes >= HEARTBEAT_TIMEOUT_MINUTES:
                last_str = last_poll.strftime("%H:%M:%S")
                logger.error(
                    f"V15 heartbeat: iQuant offline {gap_minutes:.0f}min (last={last_str})"
                )
                await _notify_feishu_error(
                    "iQuant掉线",
                    f"iQuant已失联 {gap_minutes:.0f} 分钟\n"
                    f"最后心跳: {last_str}\n"
                    f"请检查QMT是否正常运行",
                )
                return ex_date

        return alert_sent_date

    async def _send_readiness_report(now_bj: datetime) -> None:
        """Send daily readiness report at 09:30."""
        holdings = _state["holdings"]
        last_poll = _state.get("last_poll_time")
        poll_status = "未连接"
        if last_poll:
            gap = (now_bj - last_poll).total_seconds()
            if gap < 120:
                poll_status = f"在线 (最近{gap:.0f}秒前)"
            else:
                poll_status = f"离线 ({gap / 60:.0f}分钟未响应)"

        lines = [
            "[V15] 每日就绪报告",
            f"日期: {now_bj.strftime('%Y-%m-%d %H:%M')}",
            f"iQuant状态: {poll_status}",
            f"当前持仓: {len(holdings)}只",
        ]
        if holdings:
            for h in holdings:
                buy_date = h.get("buy_date", "?")
                lines.append(f"  - {h['code']} {h.get('name', '')} (买入: {buy_date})")
            lines.append("今日将跳过扫描(持仓中)")
        else:
            lines.append("今日将执行V15扫描(09:38-10:00)")

        msg = "\n".join(lines)
        logger.info("V15 readiness report sent")

        try:
            from src.common.feishu_bot import FeishuBot

            bot = FeishuBot()
            if bot.is_configured():
                await bot.send_message(msg)
        except Exception:
            logger.warning("Failed to send readiness report", exc_info=True)

    # --- Monitoring scheduler (independent, no trading resources needed) ---

    async def _monitoring_scheduler() -> None:
        """Lightweight monitoring loop — runs from server startup.

        Does NOT need Tushare, DB, or any trading resources.
        Checks heartbeat, signal timeout/expiry, readiness report.
        """
        readiness_done_date = ""
        heartbeat_alert_date = ""

        logger.info("V15 monitoring scheduler started")

        try:
            while True:
                now_bj = datetime.now(BEIJING_TZ)
                ex_date = now_bj.strftime("%Y-%m-%d")
                ex_time = now_bj.time().replace(second=0, microsecond=0)

                # --- READINESS REPORT: 09:30 ---
                if (
                    readiness_done_date != ex_date
                    and ex_time >= time(9, 30)
                    and ex_time <= time(9, 35)
                ):
                    readiness_done_date = ex_date
                    await _send_readiness_report(now_bj)

                if readiness_done_date != ex_date and ex_time > time(9, 35):
                    readiness_done_date = ex_date

                # --- CONTINUOUS MONITORING ---
                await _expire_stale_signals(now_bj)
                await _check_signal_timeout(now_bj)
                heartbeat_alert_date = await _check_heartbeat(now_bj, heartbeat_alert_date)

                await asyncio.sleep(30)

        except asyncio.CancelledError:
            logger.info("V15 monitoring scheduler stopped")
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            logger.critical(f"V15 monitoring scheduler CRASHED: {error_detail}")
            await _notify_feishu_error(
                "V15监控调度器崩溃",
                f"监控调度器意外退出!\n{error_detail}\n心跳检测/信号超时/就绪报告将全部停止",
            )

    def _start_monitoring() -> None:
        """Start the monitoring scheduler. Safe to call at any time."""
        if _state.get("monitoring_task") and not _state["monitoring_task"].done():
            return  # already running
        _state["monitoring_task"] = asyncio.create_task(_monitoring_scheduler())

    router._start_monitoring = _start_monitoring  # type: ignore[attr-defined]

    def _get_status() -> dict:
        """Return iQuant connection status (no auth required)."""
        now = datetime.now(BEIJING_TZ)
        last_poll = _state.get("last_poll_time")
        if last_poll is not None:
            gap_seconds = (now - last_poll).total_seconds()
            connected = gap_seconds < HEARTBEAT_TIMEOUT_MINUTES * 60
            last_poll_str = last_poll.strftime("%Y-%m-%d %H:%M:%S")
        else:
            gap_seconds = None
            connected = False
            last_poll_str = None
        return {
            "connected": connected,
            "last_poll_time": last_poll_str,
            "gap_seconds": round(gap_seconds) if gap_seconds is not None else None,
            "holdings_count": len(_state["broker_positions"]),
            "pending_count": len(_state["pending_signals"]),
        }

    router._get_status = _get_status  # type: ignore[attr-defined]

    def _get_holdings_list() -> list[dict]:
        """Return current V15 holdings (for dashboard trading module)."""
        return _state["holdings"]

    router._get_holdings = _get_holdings_list  # type: ignore[attr-defined]

    def _push_order(signal: dict) -> dict:
        """Push a manual order signal from dashboard (no auth).

        Same as /manual-order but callable internally without HTTP.
        Returns the signal dict with assigned ID.
        """
        _push_signal(signal)
        return _state["pending_signals"][-1]

    router._push_order = _push_order  # type: ignore[attr-defined]

    # --- V15 Background scheduler (trading operations only) ---

    async def _signal_scheduler() -> None:
        """V15 trading scheduler: T+2 adaptive sell.

        Trading windows:
        1. GAP_CHECK (09:31-09:35): Check holdings for gap-down or T+2 sell
        2. SCAN (09:38-10:00): If no holdings, run V15 scan → BUY signal
        3. SELL (14:50-14:58): Push SELL signals for marked holdings

        Monitoring (heartbeat, timeout, readiness) runs in _monitoring_scheduler.
        """
        GAP_CHECK_WINDOW = (time(9, 31), time(9, 35))
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

                # --- GAP CHECK: 09:31-09:35 ---
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

                    # Always scan + push Feishu top-5 report (auto-trading disabled)
                    try:
                        rec = await _run_v15_scan()
                        if rec:
                            logger.info(
                                f"V15: scan recommends {rec['stock_code']} "
                                f"(auto-trading disabled, no signal pushed)"
                            )
                        else:
                            logger.info("V15 scan: no recommendation today")
                            await _notify_feishu_error(
                                "V15扫描结果",
                                "今日V15扫描完成，无符合条件的推荐股票",
                            )
                    except Exception as e:
                        error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                        logger.error(f"V15 scan failed: {error_detail}")
                        await _notify_feishu_error("V15扫描失败", error_detail)

                # Scan deadline
                if scan_done_date != ex_date and ex_time > SCAN_WINDOW[1]:
                    scan_done_date = ex_date

                # --- SELL: 14:50-14:58 (auto-trading disabled) ---
                if sell_done_date != ex_date and SELL_WINDOW[0] <= ex_time <= SELL_WINDOW[1]:
                    marked = [h for h in _state["holdings"] if h.get("marked_sell_today")]
                    if marked:
                        sell_done_date = ex_date
                        codes = [h["code"] for h in marked]
                        logger.info(
                            f"V15: {len(marked)} holdings marked for sell {codes} "
                            f"(auto-trading disabled, no signal pushed)"
                        )

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
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            logger.critical(f"V15 signal scheduler CRASHED: {error_detail}")
            await _notify_feishu_error(
                "V15交易调度器崩溃",
                f"信号调度器意外退出!\n{error_detail}\nV15今日将无法自动交易，请立即检查",
            )

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
        """Return all pending (unacknowledged) signals.

        Safety: filters out expired signals so QMT never executes stale orders.
        """
        now = datetime.now(BEIJING_TZ)
        _state["last_poll_time"] = now
        expiry_seconds = SIGNAL_EXPIRY_MINUTES * 60
        active = [
            s
            for s in _state["pending_signals"]
            if not s.get("pushed_at") or (now - s["pushed_at"]).total_seconds() < expiry_seconds
        ]
        return {
            "signals": active,
            "count": len(active),
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
                    "quantity": found.get("quantity", 0),
                    "marked_sell_today": False,
                    "early_exit": False,
                }
            )
            _save_holdings(_state["holdings"])
            logger.info(
                f"V15: BUY acked {found['stock_code']} @ {found.get('latest_price', '?')}, "
                f"added to holdings ({len(_state['holdings'])} total)"
            )
            await _notify_feishu_ack(found)
        elif found["type"] == "sell":
            _state["holdings"] = [h for h in _state["holdings"] if h["code"] != found["stock_code"]]
            _save_holdings(_state["holdings"])
            logger.info(
                f"V15: SELL acked {found['stock_code']}, "
                f"removed from holdings ({len(_state['holdings'])} remaining)"
            )
            await _notify_feishu_ack(found)

        return {"success": True, "signal": found}

    @router.post("/report-status")
    async def report_status(
        request: Request,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Receive iQuant startup status (balance) and notify via Feishu."""
        body = await request.json()
        cash = body.get("available_cash", 0)
        _state["available_cash"] = cash
        now_str = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

        msg = (
            f"[V15] iQuant脚本已启动\n"
            f"可用资金: {cash:,.2f}\n"
            f"持仓数: {len(_state['holdings'])}\n"
            f"时间: {now_str}"
        )
        logger.info(f"iQuant report-status: cash={cash:.2f}")

        try:
            from src.common.feishu_bot import FeishuBot

            bot = FeishuBot()
            if bot.is_configured():
                await bot.send_message(msg)
        except Exception:
            logger.warning("Failed to send Feishu startup notification", exc_info=True)

        return {"success": True, "message": msg}

    @router.post("/report-trade")
    async def report_trade(
        request: Request,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Receive trade execution from iQuant and notify via Feishu."""
        body = await request.json()
        msg = body.get("message", "")
        stock_name = body.get("stock_name", "")
        reason = body.get("reason", "")
        now_str = datetime.now(BEIJING_TZ).strftime("%H:%M:%S")

        text = f"[V15] 下单已执行\n{msg}"
        if stock_name:
            text += f"\n名称: {stock_name}"
        if reason:
            text += f"\n原因: {reason}"
        text += f"\n时间: {now_str}"

        logger.info(f"iQuant report-trade: {msg}")

        try:
            from src.common.feishu_bot import FeishuBot

            bot = FeishuBot()
            if bot.is_configured():
                await bot.send_message(text)
        except Exception:
            logger.warning("Failed to send Feishu trade notification", exc_info=True)

        return {"success": True}

    @router.post("/report-error")
    async def report_error(
        request: Request,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Receive error from iQuant script and notify via Feishu."""
        body = await request.json()
        error_msg = body.get("error", "unknown")
        logger.error(f"iQuant report-error: {error_msg}")
        await _notify_feishu_error("iQuant执行异常", error_msg)
        return {"success": True}

    @router.post("/sync-positions")
    async def sync_positions(
        request: Request,
        api_key: str = Depends(_verify_api_key),
    ) -> dict:
        """Receive actual broker positions from iQuant script."""
        body = await request.json()
        positions = body.get("positions", [])
        _state["broker_positions"] = positions
        logger.info(f"iQuant sync-positions: {len(positions)} positions")
        return {"success": True, "count": len(positions)}

    @router.get("/holdings")
    async def holdings(api_key: str = Depends(_verify_api_key)) -> dict:
        """Return current holdings (for monitoring)."""
        return {"holdings": _state["holdings"]}

    @router.delete("/holdings")
    async def clear_holdings(api_key: str = Depends(_verify_api_key)) -> dict:
        """Clear all holdings (admin use only)."""
        count = len(_state["holdings"])
        _state["holdings"] = []
        _save_holdings(_state["holdings"])
        logger.info(f"V15 holdings cleared ({count} items removed)")
        return {"success": True, "cleared": count}

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

    @router.post("/trigger-scan")
    async def trigger_scan(api_key: str = Depends(_verify_api_key)) -> dict:
        """Manually trigger V15 scan + Feishu top-5 report (bypasses time window)."""
        await _ensure_resources()

        try:
            rec = await _run_v15_scan()
        except Exception as e:
            error_detail = f"{type(e).__name__}: {e}"
            raise HTTPException(status_code=500, detail=error_detail)

        result: dict = {"success": True, "recommendation": None}
        if rec:
            result["recommendation"] = rec
            result["signal_pushed"] = False
            result["reason"] = "auto-trading disabled"
        return result

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
        """Run V15 scan for a specific historical date."""
        from src.data.clients.greptime_backtest_cache import GreptimeHistoricalAdapter
        from src.data.sources.local_concept_mapper import LocalConceptMapper
        from src.strategy.strategies.v15_scanner import V15Scanner
        from src.web.routes import MinuteDataMissingError, _build_snapshots_from_cache

        try:
            trade_date = datetime.strptime(body.trade_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid date: {body.trade_date}")

        await _ensure_resources()

        if body.data_source == "tsanghi":
            bt_cache = getattr(request.app.state, "backtest_cache", None)
            if not bt_cache or not bt_cache.is_ready:
                raise HTTPException(
                    status_code=503,
                    detail="GreptimeDB 缓存未连接。请先在 web 页面的回测页下载数据。",
                )

            date_key = trade_date.strftime("%Y-%m-%d")
            try:
                price_snapshots = await _build_snapshots_from_cache(bt_cache, date_key)
            except MinuteDataMissingError as e:
                raise HTTPException(status_code=400, detail=str(e))

            if not price_snapshots:
                return {"recommendation": None, "reason": f"No data for {date_key}"}

            adapter = GreptimeHistoricalAdapter(bt_cache)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported data_source: {body.data_source}. Use 'tsanghi'.",
            )

        concept_mapper = LocalConceptMapper()
        scanner = V15Scanner(
            historical_adapter=adapter,
            fundamentals_db=_state["fundamentals_db"],
            concept_mapper=concept_mapper,
        )

        scan_result = await scanner.scan(price_snapshots, trade_date=trade_date)
        rec = scan_result.recommended

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
                "composite_score": round(rec.v3_score, 4),
            }
        }

    @router.get("/status")
    async def iquant_status() -> dict:
        """Public iQuant connection status (no auth required)."""
        return _get_status()

    return router
