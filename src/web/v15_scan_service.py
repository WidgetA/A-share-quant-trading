# === MODULE PURPOSE ===
# Autonomous V15 scan scheduler — runs independently from app startup.
# Decoupled from trading: always scans + pushes Feishu, never checks holdings.
#
# === ARCHITECTURE ===
# Owns scan resources (Tushare, FundamentalsDB, HistAdapter, ConceptMapper).
# Writes today's recommendation to V15ScanState for trading module to read.
# Resource initialization retries on failure (resilient to transient errors).
#
# === DATA FLOW ===
# 09:38-10:00  → Run V15 7-layer scan → push Feishu top-5 + recommendation
#              → Write result to scan_state.today_recommendation
# Trading scheduler (in iquant_routes.py) reads scan_state to decide BUY/SELL.

from __future__ import annotations

import asyncio
import logging
import traceback
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


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
    tsanghi_cache: Any = None
    universe_cache: list[str] | None = None

    # Scheduler task reference
    scheduler_task: Any = None


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


async def _notify_feishu_v15_top5(scan_result: Any) -> None:
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


# --- Resource management ---


async def init_scan_resources(scan_state: V15ScanState) -> None:
    """Initialize scan-specific resources: Tushare, FundamentalsDB, HistAdapter, etc.

    Raises on failure (caller should retry).
    """
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
            f"Tushare实时行情客户端启动失败\n错误: {e}\nV15扫描功能不可用",
        )
        raise
    scan_state.realtime_client = tushare

    fdb = create_fundamentals_db_from_config()
    try:
        await fdb.connect()
    except Exception as e:
        # Clean up tushare if DB fails
        await tushare.stop()
        await _notify_feishu_error(
            "数据库连接失败",
            f"PostgreSQL基本面数据库连接失败\n错误: {e}\nV15扫描功能不可用",
        )
        raise
    scan_state.fundamentals_db = fdb

    # V15 scan history DB (non-critical)
    try:
        v15db = create_v15_scan_db_from_config()
        await v15db.connect()
        scan_state.v15_scan_db = v15db
    except Exception as e:
        logger.warning(f"V15ScanDB init failed (scan history disabled): {e}")
        scan_state.v15_scan_db = None

    # Build historical adapter with OSS cache if available
    cache = scan_state.tsanghi_cache
    scan_state.historical_adapter = IQuantHistoricalAdapter(tushare, cache=cache)
    scan_state.concept_mapper = LocalConceptMapper()
    # V15 filter: main board + SME (002), exclude ChiNext (300) + STAR (688) + BSE
    scan_state.stock_filter = StockFilter(
        StockFilterConfig(
            exclude_bse=True,
            exclude_chinext=True,
            exclude_star=True,
            exclude_sme=False,
        )
    )

    scan_state.initialized = True
    logger.info("V15 scan resources initialized")


async def cleanup_scan_resources(scan_state: V15ScanState) -> None:
    """Cleanup scan resources on shutdown."""
    task = scan_state.scheduler_task
    if task and not task.done():
        task.cancel()
    rt_client = scan_state.realtime_client
    if rt_client:
        await rt_client.stop()
    fdb = scan_state.fundamentals_db
    if fdb:
        await fdb.close()
    v15db = scan_state.v15_scan_db
    if v15db:
        await v15db.close()
    scan_state.initialized = False
    logger.info("V15 scan resources cleaned up")


# --- Universe ---


async def get_universe(scan_state: V15ScanState) -> list[str]:
    """Get stock codes for V15 universe (main board + SME, cached)."""
    if scan_state.universe_cache:
        return scan_state.universe_cache

    import akshare as ak

    df = await asyncio.to_thread(ak.stock_info_a_code_name)
    stock_filter = scan_state.stock_filter
    codes = [
        row["code"]
        for _, row in df.iterrows()
        if isinstance(row["code"], str)
        and len(row["code"]) == 6
        and stock_filter.is_allowed(row["code"])
    ]
    scan_state.universe_cache = codes
    logger.info(f"V15 universe cached: {len(codes)} codes")
    return codes


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


# --- V15 scan logic ---


async def run_v15_scan(scan_state: V15ScanState) -> dict[str, Any] | None:
    """Run V15 scan via Tushare + V15Scanner. Returns recommendation dict or None."""
    from src.strategy.strategies.momentum_sector_scanner import PriceSnapshot
    from src.strategy.strategies.v15_scanner import V15Scanner

    universe = await get_universe(scan_state)
    if not universe:
        raise RuntimeError("Universe is empty")

    rt_client = scan_state.realtime_client
    quotes = await rt_client.batch_get_early_quotes(universe)
    logger.info(f"V15 scan: Tushare rt_min_daily returned {len(quotes)} quotes")

    if not quotes:
        return None

    # Get prev_close (previous trading day's close).
    # L6.5 limit-up check needs real prev_close — NEVER approximate with open_price
    prev_closes: dict[str, float] = {}
    today = datetime.now(BEIJING_TZ).date()
    calendar = await get_trade_calendar()
    prev_dates = [d for d in calendar if d < today]
    if not prev_dates:
        raise RuntimeError("V15 scan: no previous trading day found in calendar")
    prev_trade_date = prev_dates[-1].strftime("%Y-%m-%d")

    # Source 1: OSS cache (instant, no API call)
    cache = scan_state.tsanghi_cache
    if cache and cache.is_ready:
        all_daily = cache.get_all_codes_with_daily(prev_trade_date)
        for code, daily in all_daily.items():
            close_val = daily.get("close")
            if close_val and close_val > 0:
                prev_closes[code] = close_val

    # Source 2: tsanghi API fallback (2 calls for XSHG+XSHE)
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
        historical_adapter=scan_state.historical_adapter,
        fundamentals_db=scan_state.fundamentals_db,
        concept_mapper=scan_state.concept_mapper,
    )

    scan_result = await scanner.scan(price_snapshots)
    today = datetime.now(BEIJING_TZ).date()

    # Persist top-5 scored stocks (non-critical, never blocks)
    if scan_result.all_scored and scan_state.v15_scan_db:
        try:
            await scan_state.v15_scan_db.save_top_n(
                today, scan_result.all_scored, scan_result.final_candidates
            )
        except Exception as e:
            logger.warning(f"V15ScanDB save failed: {e}")

    # Push top-5 report to Feishu (always, non-critical)
    await _notify_feishu_v15_top5(scan_result)

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


# --- Scan scheduler ---


async def _scan_scheduler(scan_state: V15ScanState) -> None:
    """Autonomous scan scheduler. Runs from app startup, independent of iQuant.

    Time window: 09:38-10:00
    - Always runs V15 scan
    - Always pushes Feishu top-5 report + recommendation
    - Writes result to scan_state.today_recommendation
    - Does NOT check holdings or push trading signals
    """
    SCAN_WINDOW = (time(9, 38), time(10, 0))
    scan_done_date = ""

    logger.info("V15 scan scheduler started (autonomous)")

    # Initialize resources with retry
    while not scan_state.initialized:
        try:
            await init_scan_resources(scan_state)
        except Exception as e:
            logger.error(f"V15 scan resource init failed, retry in 60s: {e}")
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

            # --- SCAN: 09:38-10:00 ---
            if scan_done_date != ex_date and SCAN_WINDOW[0] <= ex_time <= SCAN_WINDOW[1]:
                scan_done_date = ex_date
                scan_state.scan_done_date = ex_date

                try:
                    rec = await run_v15_scan(scan_state)
                    scan_state.today_recommendation = rec
                    scan_state.scan_error = None

                    if rec:
                        now_str = datetime.now(BEIJING_TZ).strftime("%H:%M:%S")
                        rec_signal = {
                            "type": "buy",
                            "stock_code": rec["stock_code"],
                            "stock_name": rec["stock_name"],
                            "board_name": rec["board_name"],
                            "latest_price": rec["latest_price"],
                            "v3_score": rec["v3_score"],
                            "reason": f"V15推荐 (板块={rec['board_name']}, "
                            f"score={rec['v3_score']:.4f})",
                            "created_at": now_str,
                        }
                        await _notify_feishu_signal(rec_signal)
                    else:
                        logger.info("V15 scan: no recommendation today")
                        await _notify_feishu_error(
                            "V15扫描结果",
                            "今日V15扫描完成，无符合条件的推荐股票",
                        )
                except Exception as e:
                    error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                    scan_state.scan_error = error_detail
                    scan_state.today_recommendation = None
                    logger.error(f"V15 scan failed: {error_detail}")
                    await _notify_feishu_error("V15扫描失败", error_detail)

            # Scan deadline
            if scan_done_date != ex_date and ex_time > SCAN_WINDOW[1]:
                scan_done_date = ex_date
                scan_state.scan_done_date = ex_date

            # Adaptive sleep
            await asyncio.sleep(30 if scan_done_date == ex_date else 15)

    except asyncio.CancelledError:
        logger.info("V15 scan scheduler stopped")
    except Exception as e:
        error_detail = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        logger.critical(f"V15 scan scheduler CRASHED: {error_detail}")
        await _notify_feishu_error(
            "V15扫描调度器崩溃",
            f"扫描调度器意外退出!\n{error_detail}\n今日将无法推送V15扫描结果",
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

    scan_state.tsanghi_cache = cache
    if scan_state.realtime_client:
        scan_state.historical_adapter = IQuantHistoricalAdapter(
            scan_state.realtime_client, cache=cache
        )
        logger.info("V15 scan: OSS cache injected, historical adapter rebuilt")
    else:
        logger.info("V15 scan: OSS cache stored (adapter will be built on init)")
