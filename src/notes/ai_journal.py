# === MODULE PURPOSE ===
# AI 交易日志代写 (NOTE-002, 线上版) — 给 trade_notes 里正文为空的 买入/卖出 事件
# 批量代写「交易原因」(对内 content + 对外 content_external),风格仿用户手写范文。
#
# 数据流(每笔一个「事实包」):
#   - 推票事实: 159 的 V16 回测端点 POST /api/v16/backtest {trade_date}(用户口径:
#     每日推票以 159 的 V16 为准;纯查询、无副作用、无鉴权,对 159 只做这一种只读调用)。
#     只有「在当日 Top-10 榜单」的买入(以及买入腿在榜单的卖出)才代写;
#     不在榜单的交易**绝不冒认策略信号**,归入 manual_list 由用户手动处理。
#   - 行情事实: 本机 GreptimeDB backtest_daily/backtest_minute (日线 CCI14/持仓表现,
#     分钟线早盘/卖出时段走势) + Tushare index_daily 大盘背景(best-effort)。
#   - 持仓事实: trade_notes 全量买卖 FIFO 配对 → 成本价/T+n/推算清仓收益(只进文本,
#     不回填 realized_pnl 字段——那是既有计算逻辑的领地)。
#
# kimi 调用复用路径 B 的模式: spawn `kimi --print --afk`,结果走临时文件(不从 trace 刮),
# 错误分类如实报告(kimi_listing_verifier._classify_unparseable / _looks_like_api_auth_failure)。
# 串行跑 + 时间预算,单飞锁防并发。只写正文仍为空的事件,绝不覆盖手写内容。

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from src.notes.note_store import NoteEvent, TradeNoteStore

logger = logging.getLogger(__name__)

_BEIJING_TZ = ZoneInfo("Asia/Shanghai")
_INSTRUCTIONS_PATH = Path(__file__).resolve().parent / "ai_journal_instructions.md"

# 选股来源 = 159 的 V16 回测端点 POST /api/v16/backtest {trade_date}(用户指定口径:
# 每日推票以 159 的 V16 为准)。纯查询、无副作用、无需鉴权;对 159 只做这一种只读调用,
# 绝不发任何修改类请求(见 MEMORY feedback_never_touch_159_prod)。
V16_SCAN_BASE = os.environ.get("V16_SCAN_BASE", "http://8.159.150.224:8000")


KIMI_JOURNAL_TIMEOUT_SEC = int(os.environ.get("KIMI_JOURNAL_TIMEOUT_SEC", "1200"))
DEFAULT_TIME_BUDGET_SEC = 1800
DEFAULT_MAX_EVENTS = 50
TOP_LIST_SIZE = 10  # 「在榜单」判定 = 当日排名 <= 10 (飞书推送口径)

# ── 运行状态(单进程单飞) ─────────────────────────────────────────

_state: dict[str, Any] = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "total": 0,
    "done": 0,
    "ok": 0,
    "failed": 0,
    "manual": 0,
    "deferred": 0,  # 159 缺榜单数据、挂起待补数重跑的事件数
    "current": None,
    "results": [],  # [{event_id, code, name, type, date, status, reason}]
    "manual_list": [],  # 不在榜单、需用户手动处理的交易
    "gap_dates": [],  # 159 缺数据的日期(YYYY-MM-DD)
}


def get_state() -> dict[str, Any]:
    return dict(_state)


# ── FIFO 买卖腿配对(纯函数,可单测) ──────────────────────────────


def build_fifo_ledger(events: list[NoteEvent]) -> dict[str, dict]:
    """按代码回放全部买卖,给每笔卖出腿算 FIFO 成本/买入日期/买入费用分摊.

    返回 {sell_event_id: {cost_avg, matched_qty, unmatched_qty, buy_dates, buy_fee_alloc}}
    买入日期用北京时区日历日(YYYY-MM-DD)。
    """
    by_code: dict[str, list[NoteEvent]] = {}
    for e in events:
        if e.event_type in ("买入", "卖出"):
            by_code.setdefault(e.code, []).append(e)

    result: dict[str, dict] = {}
    for _code, evs in by_code.items():
        evs.sort(key=lambda x: x.ts)
        lots: list[dict] = []
        for e in evs:
            qty = e.qty or 0
            fees = (e.commission or 0) + (e.transfer_fee or 0)
            bj_date = e.ts.astimezone(_BEIJING_TZ).strftime("%Y-%m-%d")
            if e.event_type == "买入":
                if e.price and qty:
                    lots.append(
                        {
                            "price": e.price,
                            "qty_left": qty,
                            "qty0": qty,
                            "fees": fees,
                            "date": bj_date,
                        }
                    )
                continue
            need, cost, buy_fee_alloc = qty, 0.0, 0.0
            buy_dates: list[str] = []
            while need > 0 and lots:
                lot = lots[0]
                take = min(need, lot["qty_left"])
                cost += take * lot["price"]
                buy_fee_alloc += lot["fees"] * take / lot["qty0"]
                if lot["date"] not in buy_dates:
                    buy_dates.append(lot["date"])
                lot["qty_left"] -= take
                need -= take
                if lot["qty_left"] == 0:
                    lots.pop(0)
            matched = qty - need
            result[e.event_id] = {
                "cost_avg": round(cost / matched, 4) if matched else None,
                "matched_qty": matched,
                "unmatched_qty": need,
                "buy_dates": buy_dates,
                "buy_fee_alloc": round(buy_fee_alloc, 2),
            }
    return result


def _is_handwritten(x: NoteEvent) -> bool:
    return bool(
        ((x.content or "").strip() or (x.content_external or "").strip())
        and x.author not in ("ai", "ai_journal")
    )


def collect_user_buy_notes(
    event: NoteEvent, all_trades: list[NoteEvent], fifo_leg: dict | None
) -> list[dict]:
    """卖出事件对应买入腿里用户手写的记录(2026-07-05 用户定的规则:
    「买的我写了卖的没写你也补,可以看我买的写了啥」)。

    返回给事实包,让 kimi 衔接用户买入时的口径写卖出;但卖出理由独立判断
    ——卖早了可以与买入理由完全无关。用户写的内容只读,绝不修改。
    """
    buy_dates = set((fifo_leg or {}).get("buy_dates") or [])
    notes = []
    for x in all_trades:
        if (
            x.code == event.code
            and x.event_type == "买入"
            and x.ts.astimezone(_BEIJING_TZ).strftime("%Y-%m-%d") in buy_dates
            and _is_handwritten(x)
        ):
            notes.append(
                {
                    "date": x.ts.astimezone(_BEIJING_TZ).strftime("%Y-%m-%d"),
                    "content": x.content,
                    "content_external": x.content_external,
                }
            )
    return notes


# ── 策略票判定(纯函数,可单测) ────────────────────────────────────


def classify_event(
    event_type: str,
    code: str,
    event_date: str,
    fifo_leg: dict | None,
    scan_rows_by_date: dict[str, list[dict] | None],
) -> tuple[bool, str, dict | None]:
    """判定这笔交易是否「策略票」(可代写)。

    买入: 当日榜单里排名 <= TOP_LIST_SIZE。
    卖出: FIFO 首个买入腿的 (code, 日期) 在该日榜单排名 <= TOP_LIST_SIZE。
    返回 (is_strategy, reason, scan_row_of_the_buy)。scan_rows None = 该日无推票数据。
    """
    if event_type == "买入":
        check_date = event_date
    else:
        buy_dates = (fifo_leg or {}).get("buy_dates") or []
        if not buy_dates:
            return False, "找不到对应的买入腿(历史买入不在笔记里)", None
        check_date = buy_dates[0]

    rows = scan_rows_by_date.get(check_date)
    if rows is None:
        return False, f"{check_date} 无推票数据(选股接口查不到当日榜单)", None
    mine = next((r for r in rows if r.get("stock_code") == code), None)
    if mine is None or (mine.get("rank") or 999) > TOP_LIST_SIZE:
        which = "买入当日" if event_type == "买入" else f"买入日 {check_date}"
        return False, f"{which}不在 V16 Top-{TOP_LIST_SIZE} 榜单", None
    return True, "", mine


# ── 行情/推票数据获取 ────────────────────────────────────────────


# 159 回不出榜单但属于「它算不了」而非「当日真没榜单」的 message 关键词——
# 这些必须报错中止,不许当「无数据」归手动(否则数据缺口会把策略票误判成手动票)
_SCAN_DATA_GAP_MARKERS = (
    "无可用日线数据",
    "数据未下载",
    "无法构建股票数据",
    "股票池为空",
    "预下载",
)


def _normalize_scan_rows(payload: dict) -> list[dict] | None:
    """把 159 /api/v16/backtest 的应答规整成 classify/facts 用的行。

    纯函数,可单测。优先 all_scored(全量榜单,手动清单能写清"排第 15"还是
    "不在漏斗"),回退 recommended(Top-10)。字段: code→stock_code, name→stock_name,
    board→board_name, score, rank。
    真·当日无推荐(扫描跑了但空)→ None;**带数据缺口 message 的空应答绝不当
    「无数据」**——那是 159 缓存缺该日期,吞掉会把策略票误判成手动票
    (2026-07-05 两次踩坑:推荐开关误用 + error 被吞)。
    """
    rows_raw = payload.get("all_scored") or payload.get("recommended")
    if not rows_raw:
        msg = str(payload.get("message") or payload.get("error") or "")
        if any(marker in msg for marker in _SCAN_DATA_GAP_MARKERS):
            raise RuntimeError(f"159 算不了这天的榜单: {msg}")
        if payload.get("success") is False:
            raise RuntimeError(f"159 回测接口报错: {msg or payload}")
        return None
    final_candidates = payload.get("final_candidates")
    rows: list[dict] = []
    for i, r in enumerate(rows_raw, 1):
        rows.append(
            {
                "rank": r.get("rank") or i,
                "stock_code": r.get("code") or r.get("stock_code"),
                "stock_name": r.get("name") or r.get("stock_name"),
                "board_name": r.get("board") or r.get("board_name"),
                "score": r.get("score"),
                "final_candidates": final_candidates,
            }
        )
    return rows


async def fetch_scan_rows(day: str) -> list[dict] | None:
    """只读调用 159 的 V16 回测端点拿某日推票榜单。日期 YYYY-MM-DD。

    真·当日无榜单返回 None。「159 缓存缺数据 / 接口不可达 / 报错」一律中止整批
    (绝不静默当「无数据」)。对 159 只有这一个 POST 查询,无任何副作用。
    """
    import httpx

    url = f"{V16_SCAN_BASE}/api/v16/backtest"
    try:
        async with httpx.AsyncClient(timeout=300) as client:
            resp = await client.post(url, json={"trade_date": day})
        if resp.status_code == 400:
            detail = ""
            try:
                detail = resp.json().get("detail") or ""
            except Exception:
                pass
            raise RuntimeError(f"159 回测接口拒绝请求: {detail or 'HTTP 400'}")
        resp.raise_for_status()
        return _normalize_scan_rows(resp.json())
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"选股接口(159)不可达: {exc}") from exc


def _cci14(daily_rows: list[dict]) -> float | None:
    """线上同款 CCI: TP=(H+L+C)/3, (TP-SMA14)/(0.015*MeanDev14)。传截至 T-1 的日线。"""
    if len(daily_rows) < 14:
        return None
    tps = [(r["high"] + r["low"] + r["close"]) / 3 for r in daily_rows]
    tail = tps[-14:]
    sma = sum(tail) / 14
    mean_dev = sum(abs(x - sma) for x in tail) / 14
    if mean_dev == 0:
        return 0.0
    return round((tps[-1] - sma) / (0.015 * mean_dev), 1)


def _ts_to_trade_date(ts_val: Any) -> str:
    """日线行 ts → YYYYMMDD。asyncpg 对 TIMESTAMP 列回 datetime(naive UTC 约定,
    日线 bar 落在交易日 00:00 UTC),部分路径回 epoch ms 整数——两种都接。"""
    if isinstance(ts_val, datetime):
        return ts_val.strftime("%Y%m%d")
    return datetime.fromtimestamp(ts_val / 1000, tz=ZoneInfo("UTC")).strftime("%Y%m%d")


async def _get_daily_rows(storage: Any, code: str, start: str, end: str) -> list[dict]:
    """backtest_daily 日线 → [{trade_date, o/h/l/c, pre_close, pct_chg, vol_shares}]。

    start/end 为 YYYY-MM-DD(parse_date_str 口径)。
    vol 原始单位=手(Tushare daily 原生),读层 ×100 转股(见 CLAUDE.md §9)。
    """
    records = await storage.get_daily_for_code(code, start, end)
    rows: list[dict] = []
    for r in sorted(records, key=lambda x: _ts_to_trade_date(x["ts"])):
        d = _ts_to_trade_date(r["ts"])
        close = float(r["close_price"])
        pre_close = float(r["pre_close"]) if r["pre_close"] is not None else None
        pct = round((close - pre_close) / pre_close * 100, 2) if pre_close else None
        rows.append(
            {
                "trade_date": d,
                "open": float(r["open_price"]),
                "high": float(r["high_price"]),
                "low": float(r["low_price"]),
                "close": close,
                "pre_close": pre_close,
                "pct_chg": pct,
                "vol_shares": float(r["vol"]) * 100 if r["vol"] is not None else None,
            }
        )
    return rows


def _minute_summary(bars: list[dict], focus_hhmm: str | None) -> dict | None:
    """压缩分钟线: 早盘 09:31-09:45 逐分钟 + 全天 10 分钟粒度 + 关注时刻 ±6 分钟。

    bars 为 storage.get_minute_bars_for_day 输出(vol 单位=股)。
    """
    if not bars:
        return None

    def hhmm(b: dict) -> str:
        return str(b["trade_time"])[11:16]

    def _min_of(t: str) -> int:
        return int(t[:2]) * 60 + int(t[3:5])

    early = [b for b in bars if "09:31" <= hhmm(b) <= "09:45"]
    win = [b for b in bars if "09:31" <= hhmm(b) <= "09:40"]
    coarse = [b for b in bars if hhmm(b)[3:] in ("00", "10", "20", "30", "40", "50")]
    around = (
        [b for b in bars if abs(_min_of(hhmm(b)) - _min_of(focus_hhmm)) <= 6] if focus_hhmm else []
    )

    def slim(bs: list[dict]) -> list[dict]:
        return [
            {
                "t": hhmm(b),
                "o": b["open"],
                "h": b["high"],
                "l": b["low"],
                "c": b["close"],
                "vol_shares": b["vol"],
                "amount": b["amount"],
            }
            for b in bs
        ]

    day_high = max(bars, key=lambda b: b["high"] or 0)
    day_low = min(bars, key=lambda b: b["low"] or 1e18)
    return {
        "early_0930_0940": {
            "volume_shares": sum(b["vol"] or 0 for b in win),
            "amount_yuan": sum(b["amount"] or 0 for b in win),
        },
        "early_bars_0931_0945": slim(early),
        "day_path_10min": slim(coarse),
        "around_trade_time": slim(around),
        "day_high": {"t": hhmm(day_high), "price": day_high["high"]},
        "day_low": {"t": hhmm(day_low), "price": day_low["low"]},
    }


async def _fetch_index_daily(start: str, end: str) -> list[dict] | None:
    """上证指数日线(大盘背景),Tushare HTTP,best-effort,失败返回 None(事实包里省略)。"""
    import httpx

    from src.common.config import get_tushare_token

    try:
        body = {
            "api_name": "index_daily",
            "token": get_tushare_token(),
            "params": {"ts_code": "000001.SH", "start_date": start, "end_date": end},
            "fields": "trade_date,close,pct_chg",
        }
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post("http://api.tushare.pro", json=body)
        payload = resp.json()
        if payload.get("code") != 0:
            logger.warning("index_daily fetch failed: %s", payload.get("msg"))
            return None
        data = payload["data"]
        rows = [dict(zip(data["fields"], row)) for row in data["items"]]
        return sorted(rows, key=lambda r: r["trade_date"])
    except Exception:
        logger.warning("index_daily fetch failed", exc_info=True)
        return None


# ── 事实包组装 ───────────────────────────────────────────────────


async def assemble_fact(
    storage: Any,
    event: NoteEvent,
    all_trades: list[NoteEvent],
    fifo: dict[str, dict],
    scan_row: dict | None,
    scan_rows_same_day: list[dict] | None,
) -> dict:
    bj = event.ts.astimezone(_BEIJING_TZ)
    d10 = bj.strftime("%Y-%m-%d")
    ymd = bj.strftime("%Y%m%d")
    hhmm = bj.strftime("%H:%M")
    is_buy = event.event_type == "买入"

    start_hist = (bj.date() - timedelta(days=90)).strftime("%Y-%m-%d")
    daily = await _get_daily_rows(storage, event.code, start_hist, d10)
    daily_before = [r for r in daily if r["trade_date"] < ymd]
    day_row = next((r for r in daily if r["trade_date"] == ymd), None)
    minute_bars = await storage.get_minute_bars_for_day(event.code, bj.date())

    fact: dict[str, Any] = {
        "event": {
            "event_id": event.event_id,
            "type": event.event_type,
            "code": event.code,
            "datetime": bj.isoformat(),
            "price": event.price,
            "qty": event.qty,
            "commission": event.commission,
            "transfer_fee": event.transfer_fee,
            "stamp_tax": event.stamp_tax,
        },
        "day_ohlc": day_row,
        "cci14_at_prev_day": _cci14(daily_before[-40:]),
        "recent_daily_10d": daily_before[-10:],
        "intraday": _minute_summary(minute_bars, hhmm),
        "data_gaps": [],
    }
    if not minute_bars:
        fact["data_gaps"].append("当日分钟线缺失,不要写逐分钟走势")
    if day_row is None:
        fact["data_gaps"].append("当日日线缺失")

    if is_buy:
        fact["v16_scan"] = {
            "date": d10,
            "my_rank": scan_row.get("rank") if scan_row else None,
            "my_score": scan_row.get("score") if scan_row else None,
            "my_board": scan_row.get("board_name") if scan_row else None,
            "final_candidates": scan_row.get("final_candidates") if scan_row else None,
            "top_list": [
                {k: r.get(k) for k in ("rank", "stock_code", "stock_name", "board_name")}
                for r in (scan_rows_same_day or [])[:TOP_LIST_SIZE]
            ],
        }
        same_day_buys = [
            x
            for x in all_trades
            if x.event_type == "买入"
            and x.ts.astimezone(_BEIJING_TZ).strftime("%Y-%m-%d") == d10
            and x.event_id != event.event_id
        ]
        fact["same_day_other_buys"] = [{"code": x.code, "price": x.price} for x in same_day_buys]
    else:
        leg = fifo.get(event.event_id) or {}
        fact["position"] = dict(leg)
        if leg.get("buy_dates"):
            first_buy_ymd = leg["buy_dates"][0].replace("-", "")
            holding = [r for r in daily if first_buy_ymd <= r["trade_date"] <= ymd]
            fact["position"]["holding_trade_days_T_plus"] = max(len(holding) - 1, 0)
            fact["holding_daily"] = holding
            idx = await _fetch_index_daily(first_buy_ymd, ymd)
            if idx:
                fact["index_daily_shanghai"] = idx
        cost_avg = leg.get("cost_avg")
        if cost_avg and event.price and leg.get("matched_qty"):
            gross = (event.price - cost_avg) * leg["matched_qty"]
            sell_fees = (event.commission or 0) + (event.stamp_tax or 0) + (event.transfer_fee or 0)
            net = gross - sell_fees - leg.get("buy_fee_alloc", 0)
            fact["computed_pnl"] = {
                "cost_avg": cost_avg,
                "gross": round(gross, 2),
                "net_after_fees": round(net, 2),
                "return_pct": round(net / (cost_avg * leg["matched_qty"]) * 100, 2),
            }
        siblings = [
            x
            for x in all_trades
            if x.event_type == "卖出"
            and x.code == event.code
            and x.ts.astimezone(_BEIJING_TZ).strftime("%Y-%m-%d") == d10
            and x.event_id != event.event_id
        ]
        fact["same_day_sell_legs"] = [
            {"time": x.ts.astimezone(_BEIJING_TZ).strftime("%H:%M"), "price": x.price, "qty": x.qty}
            for x in sorted(siblings, key=lambda x: x.ts)
        ]
        buy_notes = collect_user_buy_notes(event, all_trades, leg)
        if buy_notes:
            fact["your_buy_note"] = buy_notes
        # 同日已有用户手写的其他卖出腿 → 原文给 kimi 衔接口吻(只读)
        hand_sells = [x for x in siblings if _is_handwritten(x)]
        if hand_sells:
            fact["your_same_day_sell_notes"] = [
                {
                    "time": x.ts.astimezone(_BEIJING_TZ).strftime("%H:%M"),
                    "content": x.content,
                    "content_external": x.content_external,
                }
                for x in sorted(hand_sells, key=lambda x: x.ts)
            ]
    return fact


def build_exemplars(all_trades: list[NoteEvent], n_each: int = 4) -> str:
    """从用户手写过正文的买/卖事件里抽最近 N 篇当范文(排除 AI 写的)。"""
    hand = [
        e
        for e in all_trades
        if (e.content or e.content_external) and e.author not in ("ai", "ai_journal")
    ]
    hand.sort(key=lambda e: e.ts, reverse=True)
    buys = [e for e in hand if e.event_type == "买入"][:n_each]
    sells = [e for e in hand if e.event_type == "卖出"][:n_each]
    parts: list[str] = []
    for label, group in (("买入", buys), ("卖出", sells)):
        for i, e in enumerate(group, 1):
            parts.append(f"### {label}·范文{i}")
            if e.content_external:
                parts.append(f"对外:{e.content_external}")
            if e.content:
                parts.append(f"对内:{e.content}")
            parts.append("")
    return "\n".join(parts)


def build_prompt(fact: dict, exemplars: str, result_path: str) -> str:
    instructions = _INSTRUCTIONS_PATH.read_text(encoding="utf-8")
    return (
        f"{instructions}\n\n"
        f"## 范文(他本人手写)\n\n{exemplars}\n\n"
        f"## 事实包 JSON\n\n```json\n{json.dumps(fact, ensure_ascii=False)}\n```\n\n"
        f"把结果 JSON(仅 content 与 content_external 两个键)用 Shell 写入文件 "
        f"{result_path} (UTF-8,无 BOM)。"
    )


# ── kimi 调用(复用路径 B 的可靠模式) ────────────────────────────


async def run_kimi_journal(prompt: str, timeout_sec: int = KIMI_JOURNAL_TIMEOUT_SEC) -> dict:
    """spawn kimi 写一篇日志。成功返回 {content, content_external};失败抛 RuntimeError。

    结果走临时文件(不从 trace 刮 JSON);超时只是卡死兜底。错误如实分类,
    kimi 工具/认证故障用 KimiToolError 区分(调用方应中止整批而非继续烧)。
    """
    from src.data.services.kimi_listing_verifier import (
        KimiToolError,
        _classify_unparseable,
        _looks_like_api_auth_failure,
    )

    fd, result_path = tempfile.mkstemp(prefix="ai_journal_", suffix=".json")
    os.close(fd)
    try:
        full_prompt = prompt.replace("{RESULT_PATH}", result_path)
        env = dict(os.environ)
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        proc = await asyncio.create_subprocess_exec(
            "kimi",
            "--print",
            "--afk",
            "-p",
            full_prompt,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )

        async def _drain() -> bytes:
            assert proc.stdout is not None
            return await proc.stdout.read()

        try:
            raw = (await asyncio.wait_for(_drain(), timeout=timeout_sec)).decode(
                "utf-8", errors="replace"
            )
            await proc.wait()
        except TimeoutError:
            proc.kill()
            raise KimiToolError(f"kimi 超过 {timeout_sec}s 未退出(卡死兜底)") from None

        if _looks_like_api_auth_failure(raw):
            raise KimiToolError("kimi API 认证被拒(精确响应短语命中),请检查 KIMI_API_KEY")

        result_file = Path(result_path)
        if result_file.exists() and result_file.stat().st_size > 0:
            try:
                draft = json.loads(result_file.read_text(encoding="utf-8-sig"))
            except Exception as exc:
                raise RuntimeError(f"结果文件不是合法 JSON: {exc}") from exc
            if not (draft.get("content_external") or "").strip():
                raise RuntimeError("结果文件缺 content_external")
            return {
                "content": str(draft.get("content") or ""),
                "content_external": str(draft.get("content_external") or ""),
            }
        raise RuntimeError(
            f"kimi 跑完但没写结果文件({_classify_unparseable(raw, proc.returncode)})"
        )
    finally:
        try:
            os.unlink(result_path)
        except OSError:
            pass


# ── 批量执行 ─────────────────────────────────────────────────────


def _is_empty_content(e: NoteEvent) -> bool:
    return not (e.content or "").strip() and not (e.content_external or "").strip()


async def run_ai_journal_batch(
    storage: Any,
    event_ids: list[str] | None = None,
    time_budget_sec: int = DEFAULT_TIME_BUDGET_SEC,
    max_events: int = DEFAULT_MAX_EVENTS,
) -> dict[str, Any]:
    """批量代写主流程。串行 kimi + 时间预算,进度写 _state。"""
    store = TradeNoteStore(storage)
    started = datetime.now(_BEIJING_TZ)
    _state.update(
        running=True,
        started_at=started.isoformat(),
        finished_at=None,
        total=0,
        done=0,
        ok=0,
        failed=0,
        manual=0,
        deferred=0,
        current=None,
        results=[],
        manual_list=[],
        gap_dates=[],
        error=None,
    )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + time_budget_sec
    try:
        all_events = await store.list_events_in_range()
        trades = [e for e in all_events if e.event_type in ("买入", "卖出")]
        targets = [e for e in trades if _is_empty_content(e)]
        if event_ids:
            wanted = set(event_ids)
            targets = [e for e in targets if e.event_id in wanted]
        targets.sort(key=lambda e: e.ts)
        targets = targets[:max_events]
        _state["total"] = len(targets)

        fifo = build_fifo_ledger(trades)
        exemplars = build_exemplars(trades)

        # 预取涉及的全部榜单日期(买入日 + 卖出的 FIFO 买入日)
        scan_dates: set[str] = set()
        for e in targets:
            d10 = e.ts.astimezone(_BEIJING_TZ).strftime("%Y-%m-%d")
            if e.event_type == "买入":
                scan_dates.add(d10)
            else:
                for bd in (fifo.get(e.event_id) or {}).get("buy_dates", []):
                    scan_dates.add(bd)
        # 159 缓存缺某些日期时: 该日期涉及的事件整体挂起(deferred),其余日期照跑。
        # 绝不把「159 缺数据」当「不在榜单」归手动;其他错误(不可达等)仍中止整批。
        scan_rows_by_date: dict[str, list[dict] | None] = {}
        gap_dates: dict[str, str] = {}
        for d in sorted(scan_dates):
            try:
                scan_rows_by_date[d] = await fetch_scan_rows(d)
            except RuntimeError as exc:
                if "算不了这天的榜单" in str(exc):
                    gap_dates[d] = str(exc)
                    logger.warning("ai_journal: %s 榜单数据缺口,该日事件挂起: %s", d, exc)
                else:
                    raise
        if gap_dates:
            _state["gap_dates"] = sorted(gap_dates)

        for e in targets:
            if loop.time() > deadline:
                logger.info(
                    "ai_journal: time budget exhausted, %s left", _state["total"] - _state["done"]
                )
                break
            bj = e.ts.astimezone(_BEIJING_TZ)
            d10 = bj.strftime("%Y-%m-%d")
            _state["current"] = f"{e.event_type} {e.code} {d10}"
            entry = {
                "event_id": e.event_id,
                "code": e.code,
                "type": e.event_type,
                "date": d10,
                "status": "",
                "reason": "",
            }
            try:
                check_date = (
                    d10
                    if e.event_type == "买入"
                    else ((fifo.get(e.event_id) or {}).get("buy_dates") or [d10])[0]
                )
                if check_date in gap_dates:
                    entry["status"] = "deferred"
                    entry["reason"] = f"159 缺 {check_date} 的榜单数据,补数后重跑即可"
                    _state["deferred"] += 1
                    _state["results"].append(entry)
                    _state["done"] += 1
                    continue

                is_strategy, why_not, scan_row = classify_event(
                    e.event_type, e.code, d10, fifo.get(e.event_id), scan_rows_by_date
                )
                if not is_strategy:
                    entry["status"] = "manual"
                    entry["reason"] = why_not
                    _state["manual"] += 1
                    _state["manual_list"].append(entry)
                    _state["done"] += 1
                    continue

                buy_date = (
                    d10
                    if e.event_type == "买入"
                    else (fifo.get(e.event_id) or {}).get("buy_dates", [d10])[0]
                )
                fact = await assemble_fact(
                    storage, e, trades, fifo, scan_row, scan_rows_by_date.get(buy_date)
                )
                prompt = build_prompt(fact, exemplars, "{RESULT_PATH}")
                draft = await run_kimi_journal(prompt)

                # 写回前二次确认仍为空(防止用户在跑批期间手写了内容被覆盖)
                fresh = await store.get_event(e.code, e.event_id)
                if fresh is None or not _is_empty_content(fresh):
                    entry["status"] = "skipped"
                    entry["reason"] = "写回前发现正文已非空,放弃写入"
                else:
                    await store.update_event(
                        e.code,
                        e.event_id,
                        content=draft["content"],
                        content_external=draft["content_external"],
                        author="ai",  # 页面可见标记 + 范文抽取排除 AI 文
                    )
                    entry["status"] = "ok"
                    _state["ok"] += 1
            except Exception as exc:
                from src.data.services.kimi_listing_verifier import KimiToolError

                entry["status"] = "failed"
                entry["reason"] = str(exc)
                _state["failed"] += 1
                _state["results"].append(entry)
                _state["done"] += 1
                if isinstance(exc, KimiToolError):
                    # kimi 本身故障(认证/卡死)——继续烧只会连环失败,如实中止
                    logger.error("ai_journal: kimi tool error, aborting batch: %s", exc)
                    break
                logger.warning("ai_journal: %s %s failed: %s", e.event_type, e.code, exc)
                continue
            # 成功/跳过/手动 路径统一在此记账(异常路径已在 except 里记完)
            if entry["status"] != "manual":
                _state["results"].append(entry)
            _state["done"] += 1
        return get_state()
    except Exception as exc:
        # 整批级失败(选股接口不可达/读库失败等)——写进状态给页面显示 + 飞书,
        # 不再向上抛(这是 create_task 的顶层,抛了只会变成 never-retrieved 噪音)
        _state["error"] = str(exc)
        logger.exception("ai_journal: batch aborted")
        return get_state()
    finally:
        _state["running"] = False
        _state["current"] = None
        _state["finished_at"] = datetime.now(_BEIJING_TZ).isoformat()
        await _notify_feishu_summary()


async def _notify_feishu_summary() -> None:
    """跑批结束给飞书发一条大白话总结。best-effort,失败只 log。"""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        manual = _state["manual_list"]
        lines = [
            "[笔记] AI 写日志跑批结束",
            f"成功写入 {_state['ok']} 笔,失败 {_state['failed']} 笔,"
            f"另有 {len(manual)} 笔不在推票榜单、留给你手动写",
        ]
        if _state.get("error"):
            lines.append(f"整批中止原因: {_state['error']}")
        if _state.get("deferred"):
            lines.append(
                f"另有 {_state['deferred']} 笔因 159 缺榜单数据挂起"
                f"(缺数据日期: {', '.join(_state.get('gap_dates', []))}),补数后再点一次即可"
            )
        for m in manual[:10]:
            lines.append(f"  手动: {m['date']} {m['type']} {m['code']} ({m['reason']})")
        if len(manual) > 10:
            lines.append(f"  …… 还有 {len(manual) - 10} 笔,详见补作业页")
        await bot.send_message("\n".join(lines))
    except Exception:
        logger.warning("ai_journal: feishu summary failed", exc_info=True)
