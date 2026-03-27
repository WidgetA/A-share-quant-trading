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
import logging
import traceback
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


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
            await bot.send_alert(f"[V16] {title}", detail)
    except Exception:
        logger.warning("Failed to send Feishu error notification", exc_info=True)


async def _notify_feishu_v16_top10(scan_result: Any) -> None:
    """Send V16 top-10 scored report to Feishu. Best-effort, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_v16_top10_report(scan_result)
    except Exception:
        logger.warning("Failed to send Feishu V16 top-10 report", exc_info=True)


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
            f"Tushare实时行情客户端启动失败\n错误: {e}\nV16扫描功能不可用",
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
            f"PostgreSQL基本面数据库连接失败\n错误: {e}\nV16扫描功能不可用",
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
    # V16 filter: main board + SME (002), exclude ChiNext (300) + STAR (688) + BSE
    scan_state.stock_filter = StockFilter(
        StockFilterConfig(
            exclude_bse=True,
            exclude_chinext=True,
            exclude_star=True,
            exclude_sme=False,
        )
    )

    scan_state.initialized = True
    logger.info("V16 scan resources initialized")


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
    logger.info("V16 scan resources cleaned up")


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
    scan_state: V15ScanState, today: date, calendar: list[date]
) -> dict[str, float]:
    """Fetch prev_close for all stocks. Returns code → prev_close."""
    prev_dates = [d for d in calendar if d < today]
    if not prev_dates:
        raise RuntimeError("V16 scan: no previous trading day found in calendar")
    prev_trade_date = prev_dates[-1].strftime("%Y-%m-%d")

    prev_closes: dict[str, float] = {}

    # Source 1: OSS cache (instant, no API call)
    cache = scan_state.tsanghi_cache
    if cache and cache.is_ready:
        all_daily = cache.get_all_codes_with_daily(prev_trade_date)
        for code, daily in all_daily.items():
            close_val = daily.get("close")
            if close_val and close_val > 0:
                prev_closes[code] = close_val

    # Source 2: tsanghi API fallback
    if len(prev_closes) < 100:
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
            f"V16 scan: failed to get prev_close for {prev_trade_date} "
            f"from both OSS cache and tsanghi API"
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
            first_date = datetime.strptime(time_vals[0], "%Y-%m-%d").date()
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
        avg_daily_volume=avg_daily_volume,
        trend_5d=trend_5d,
        trend_10d=trend_10d,
        avg_daily_return_20d=avg_daily_return_20d,
        volatility_20d=volatility_20d,
        consecutive_up_days=consecutive_up_days,
        history_df=hist_df,
    )


# --- V16 scan logic ---


async def run_v16_scan(scan_state: V15ScanState) -> dict[str, Any] | None:
    """Run V16 scan via Tushare + V16Scanner + LGBRank. Returns recommendation dict or None.

    IMPORTANT — price semantics:
    Quotes are fetched via ``batch_get_early_quotes`` which aggregates
    minute bars for 09:30-09:40 only (``rt_min_daily``).  The resulting
    ``latest_price`` in the returned dict is the **09:40 early_close**,
    NOT the real-time price at the moment of the scan invocation.
    """
    from src.strategy.lgbrank_scorer import LGBRankScorer
    from src.strategy.strategies.v16_scanner import V16Scanner

    # Initialize LGBRank scorer
    model_path = _PROJECT_ROOT / "models" / "lgbrank_latest.txt"
    feature_path = _PROJECT_ROOT / "models" / "feature_list.json"
    scorer = LGBRankScorer(model_path, feature_path)

    # Build V16 scanner
    scanner = V16Scanner(
        fundamentals_db=scan_state.fundamentals_db,
        concept_mapper=scan_state.concept_mapper,
        stock_filter=scan_state.stock_filter,
        scorer=scorer,
    )

    # Step 0: Get universe from board cleaning
    clean_boards, universe_codes = scanner.get_universe()
    if not universe_codes:
        raise RuntimeError("V16 scan: universe is empty after board cleaning")
    logger.info(f"V16 scan: universe = {len(universe_codes)} stocks")

    # Fetch 9:40 quotes from Tushare (only for universe stocks)
    rt_client = scan_state.realtime_client
    universe_list = sorted(universe_codes)
    quotes = await rt_client.batch_get_early_quotes(universe_list)
    logger.info(f"V16 scan: Tushare returned {len(quotes)} quotes")

    if not quotes:
        await _notify_feishu_error(
            "9:40行情全空",
            f"Tushare batch_get_early_quotes 返回空\n请求股票数: {len(universe_list)}\n扫描中止",
        )
        raise RuntimeError(f"V16 scan: Tushare returned 0 quotes for {len(universe_list)} stocks")

    # Fetch prev_close
    today = datetime.now(BEIJING_TZ).date()
    calendar = await get_trade_calendar()
    prev_closes = await _fetch_prev_closes(scan_state, today, calendar)

    # Fetch 37d OHLCV history for ALL universe stocks with quotes
    trading_codes = [c for c, q in quotes.items() if q.is_trading]
    logger.info(f"V16 scan: {len(trading_codes)} stocks trading, fetching history...")
    hist_raw = await _fetch_history_ohlcv(scan_state.historical_adapter, trading_codes, today)
    logger.info(f"V16 scan: history fetched for {len(hist_raw)} stocks")

    # Build V16StockData for each stock
    from src.strategy.strategies.v16_scanner import V16StockData  # noqa: F811

    stock_data: dict[str, V16StockData] = {}
    errors_no_prev_close: list[str] = []
    errors_no_hist: list[str] = []
    errors_build: list[str] = []
    skipped_new = 0

    for code in trading_codes:
        quote = quotes.get(code)
        if not quote or not quote.is_trading:
            continue

        pc = prev_closes.get(code)
        if not pc or pc <= 0:
            errors_no_prev_close.append(code)
            continue

        hr = hist_raw.get(code)
        if not hr:
            errors_no_hist.append(code)
            continue

        try:
            sd = _build_stock_data(code, "", quote, pc, hr, today)
        except RuntimeError as e:
            errors_build.append(f"{code}: {e}")
            continue

        if sd is None:
            skipped_new += 1
            continue

        stock_data[code] = sd

    # --- Data failure reporting: never silent ---
    total_errors = len(errors_no_prev_close) + len(errors_no_hist) + len(errors_build)
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
        await _notify_feishu_error(
            "数据缺失报警",
            f"交易中股票: {len(trading_codes)}\n"
            f"数据错误: {total_errors} 只\n"
            f"新股跳过: {skipped_new} 只\n"
            f"成功构建: {len(stock_data)} 只\n\n{detail}",
        )

    # If error rate > 20%, halt scan — data source likely broken
    if total_errors > 0 and total_errors > len(trading_codes) * 0.2:
        raise RuntimeError(
            f"V16 scan: data error rate {total_errors}/{len(trading_codes)} "
            f"exceeds 20% threshold — data source likely broken, halting"
        )

    logger.info(
        f"V16 scan: built {len(stock_data)} V16StockData "
        f"(errors={total_errors}, new_listing={skipped_new})"
    )

    if not stock_data:
        await _notify_feishu_error(
            "无有效股票数据",
            f"交易中股票: {len(trading_codes)}\n全部数据缺失或为新股, 无法执行扫描",
        )
        raise RuntimeError("V16 scan: no valid stock data after building")

    # Run V16 scan
    scan_result = await scanner.scan(stock_data, clean_boards)

    # Push top-10 report to Feishu (always, non-critical)
    await _notify_feishu_v16_top10(scan_result)

    recommended = scan_result.recommended
    if not recommended:
        return None

    # Return top-1 for trading module (today_recommendation)
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


# --- Scan scheduler ---


async def _scan_scheduler(scan_state: V15ScanState) -> None:
    """Autonomous scan scheduler. Runs from app startup, independent of iQuant.

    Time window: 09:38-10:00
    - Always runs V16 scan
    - Always pushes Feishu top-10 report + recommendation
    - Writes result to scan_state.today_recommendation
    - Does NOT check holdings or push trading signals
    """
    SCAN_WINDOW = (time(9, 38), time(10, 0))
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

            # --- SCAN: 09:38-10:00 ---
            if scan_done_date != ex_date and SCAN_WINDOW[0] <= ex_time <= SCAN_WINDOW[1]:
                scan_done_date = ex_date
                scan_state.scan_done_date = ex_date

                try:
                    rec = await run_v16_scan(scan_state)
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

            # Scan deadline
            if scan_done_date != ex_date and ex_time > SCAN_WINDOW[1]:
                scan_done_date = ex_date
                scan_state.scan_done_date = ex_date

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

    scan_state.tsanghi_cache = cache
    if scan_state.realtime_client:
        scan_state.historical_adapter = IQuantHistoricalAdapter(
            scan_state.realtime_client, cache=cache
        )
        logger.info("V16 scan: OSS cache injected, historical adapter rebuilt")
    else:
        logger.info("V16 scan: OSS cache stored (adapter will be built on init)")
