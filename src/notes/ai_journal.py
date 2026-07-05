# === MODULE PURPOSE ===
# AI 交易日志代写 (NOTE-002, 线上版) — 给 trade_notes 里正文为空的 买入/卖出 事件
# 批量代写「交易原因」(对内 content + 对外 content_external),风格仿用户手写范文。
#
# 数据流(每笔一个「事实包」):
#   - 推票事实: 159 的**现成**选股接口 GET /api/trading/recommendations?date= (X-API-Key,
#     只读调用;159 是生产选股机,绝不改动它——见 MEMORY feedback_never_touch_159_prod)。
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

# 159 的现成选股接口(只读)。地址与 key 都可用环境变量覆盖;
# key 缺省复用本服务自己的 TRADING_API_KEY(两台若不同,配 SCAN_API_KEY)。
V16_SCAN_BASE = os.environ.get("V16_SCAN_BASE", "http://8.159.150.224:8000")


def _scan_api_key() -> str:
    return os.environ.get("SCAN_API_KEY") or os.environ.get("TRADING_API_KEY") or ""


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
    "current": None,
    "results": [],  # [{event_id, code, name, type, date, status, reason}]
    "manual_list": [],  # 不在榜单、需用户手动处理的交易
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


def _normalize_scan_rows(payload: dict) -> list[dict] | None:
    """把 /api/trading/recommendations 的应答规整成 classify/facts 用的行。

    纯函数,可单测。行字段对齐: rank / stock_code / stock_name / board_name /
    score / final_candidates(评分字段名各版本可能是 ml_score/lgb_score/v3_score)。
    明确「当日无推荐」→ None。
    """
    recs = payload.get("recommendations")
    if not recs:
        return None
    rows: list[dict] = []
    for i, r in enumerate(recs, 1):
        rows.append(
            {
                "rank": r.get("rank") or i,
                "stock_code": r.get("stock_code"),
                "stock_name": r.get("stock_name"),
                "board_name": r.get("board_name"),
                "score": r.get("ml_score", r.get("lgb_score", r.get("v3_score"))),
                "final_candidates": r.get("final_candidates"),
            }
        )
    return rows


async def fetch_scan_rows(day: str) -> list[dict] | None:
    """只读调用 159 现成选股接口拉某日推票。日期 YYYY-MM-DD。当日无推荐返回 None。

    「无数据」与「接口不可达/鉴权失败」必须分清——后者中止整批,否则会把
    策略票误判成手动票。这里绝不向 159 发任何修改类请求。
    """
    import httpx

    key = _scan_api_key()
    if not key:
        raise RuntimeError("选股接口需要 X-API-Key,但 SCAN_API_KEY/TRADING_API_KEY 都没配")
    url = f"{V16_SCAN_BASE}/api/trading/recommendations"
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(url, params={"date": day}, headers={"X-API-Key": key})
        if resp.status_code in (401, 403):
            raise RuntimeError(f"选股接口拒绝了 API Key(HTTP {resp.status_code}),请核对 key")
        resp.raise_for_status()
        return _normalize_scan_rows(resp.json())
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"选股接口不可达: {exc}") from exc


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


async def _get_daily_rows(storage: Any, code: str, start: str, end: str) -> list[dict]:
    """backtest_daily 日线 → [{trade_date, o/h/l/c, pre_close, pct_chg, vol_shares}]。

    start/end 为 YYYY-MM-DD(parse_date_str 口径)。
    vol 原始单位=手(Tushare daily 原生),读层 ×100 转股(见 CLAUDE.md §9)。
    """
    records = await storage.get_daily_for_code(code, start, end)
    rows: list[dict] = []
    for r in sorted(records, key=lambda x: x["ts"]):
        d = datetime.fromtimestamp(r["ts"] / 1000, tz=ZoneInfo("UTC")).strftime("%Y%m%d")
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
    return fact


def build_exemplars(all_trades: list[NoteEvent], n_each: int = 4) -> str:
    """从用户手写过正文的买/卖事件里抽最近 N 篇当范文(排除 AI 写的)。"""
    hand = [e for e in all_trades if (e.content or e.content_external) and e.author != "ai_journal"]
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
        current=None,
        results=[],
        manual_list=[],
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
        scan_rows_by_date: dict[str, list[dict] | None] = {}
        for d in sorted(scan_dates):
            scan_rows_by_date[d] = await fetch_scan_rows(d)

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
        for m in manual[:10]:
            lines.append(f"  手动: {m['date']} {m['type']} {m['code']} ({m['reason']})")
        if len(manual) > 10:
            lines.append(f"  …… 还有 {len(manual) - 10} 笔,详见补作业页")
        await bot.send_message("\n".join(lines))
    except Exception:
        logger.warning("ai_journal: feishu summary failed", exc_info=True)
