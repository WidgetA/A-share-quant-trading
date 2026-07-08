# === MODULE PURPOSE ===
# TRD-001 历史净值本地账本重建 (features.md 0.22.1)。
#
# 券商接口没有历史资产,但本地有完整账本:trade_notes 成交流水(含费用/股息/
# 逆回购利息)+ backtest_daily 收盘价。以最早一笔券商快照为锚,从当前持仓
# 按流水逐日倒推持仓,逐日算盈亏,`E(d−1) = E(d) − pnl(d)` 推回账本完整起点。
#
# 硬性原则(对不上就截断,绝不显示错的历史):
# - 倒推出现负持仓 = 更早有漏记成交 → 从该日截断,warning 上报页面
# - 成交 qty 缺失 = 无法倒推 → 同样截断
# - realized_pnl 字段不参与(mark-to-market 已含平仓收益,再加就双算)
# - 银证转账不在账本 → 转账日之前绝对值整体平移(文档已声明,不自动处理)

from __future__ import annotations

import logging
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import timedelta

logger = logging.getLogger(__name__)

# 账本完整起点:用户从此日起对着券商交割单逐笔补全过 trade_notes(补作业流程)。
# 更早的漏买入倒推检不出来(只有漏卖出会推出负持仓),所以不往更早重建。
LEDGER_COMPLETE_SINCE = "2026-04-27"

# QMT 把国债逆回购报成聚合持仓码(888880=沪市聚合,市值恒 0);204xxx/1318xx
# 是逆回购单码。这些都不是股票:不参与持仓倒推,收益走 repo_income。
_REPO_AGGREGATE_CODES = {"888880", "888881"}


def is_non_stock_position_code(code: str) -> bool:
    """逆回购聚合码/单码 — 不参与股票持仓倒推与市值计算。"""
    from src.notes.note_store import is_reverse_repo_code

    bare = code.split(".")[0]
    return bare in _REPO_AGGREGATE_CODES or is_reverse_repo_code(bare)


@dataclass
class ReconstructionResult:
    """points: [{date, total_asset}] 升序,全部严格早于 anchor_date(锚点日本身
    由快照段提供)。truncated_at 非空 = 该日期之前的账本对不上、已截断。"""

    points: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    truncated_at: str | None = None
    anchor_date: str = ""


class _CloseBook:
    """按码收盘价查询,停牌/缺行向前补最近收盘价;完全无价时回退成交价。"""

    def __init__(self, closes_by_code: dict[str, list[tuple[str, float]]]):
        self._dates: dict[str, list[str]] = {}
        self._vals: dict[str, list[float]] = {}
        for code, rows in closes_by_code.items():
            rows = sorted(rows)
            self._dates[code] = [d for d, _ in rows]
            self._vals[code] = [v for _, v in rows]
        self._fallback_price: dict[str, float] = {}
        self.warned_codes: set[str] = set()

    def note_fill_price(self, code: str, price: float) -> None:
        self._fallback_price.setdefault(code, price)

    def close_at(self, code: str, date_str: str) -> float:
        dates = self._dates.get(code) or []
        if dates:
            i = bisect_right(dates, date_str)
            if i > 0:
                return self._vals[code][i - 1]  # 最近一个 ≤ date 的收盘(前向补)
            # date 之前没有任何收盘(如刚上市):用之后最近一笔近似
            self.warned_codes.add(code)
            return self._vals[code][0]
        self.warned_codes.add(code)
        return self._fallback_price.get(code, 0.0)


def reconstruct_equity(
    *,
    anchor_equity: float,
    positions_at_axis_end: dict[str, int],
    fills_by_date: dict[str, list[dict]],
    cash_events_by_date: dict[str, dict],
    closes_by_code: dict[str, list[tuple[str, float]]],
    axis: list[str],
) -> ReconstructionResult:
    """纯函数核心:沿交易日轴倒推持仓与净值。

    参数:
    - anchor_equity: axis[-1] 收盘时刻的总资产(券商快照真值)
    - positions_at_axis_end: axis[-1] 收盘时的股票持仓 {裸码: 股数}
    - fills_by_date: {日期: [{code, side('buy'|'sell'), qty(int|None), price(float|None)}]}
      仅股票成交;日期必须都在 axis 内
    - cash_events_by_date: {日期: {fees, dividend, repo_income}}(不在 axis 的日期忽略并告警)
    - closes_by_code: {裸码: [(日期, 收盘价>0)]},可含早于 axis 的日期(用于前向补价)
    - axis: 交易日轴升序,axis[-1] 即锚点日

    返回 points 覆盖 [截断日 .. axis[-2]](锚点日本身不含,由快照段提供)。
    """
    result = ReconstructionResult(anchor_date=axis[-1] if axis else "")
    n = len(axis)
    if n < 2:
        return result

    book = _CloseBook(closes_by_code)
    for fills in fills_by_date.values():
        for f in fills:
            if f.get("price"):
                book.note_fill_price(f["code"], float(f["price"]))

    axis_set = set(axis)
    for d in cash_events_by_date:
        if d not in axis_set:
            result.warnings.append(f"{d} 有费用/收益记录但不在交易日轴上,已忽略该笔")

    # ---- 持仓倒推:pos[i] = axis[i] 收盘时持仓 ----
    pos: list[dict[str, int] | None] = [None] * n
    pos[n - 1] = {c: q for c, q in positions_at_axis_end.items() if q != 0}
    truncate_idx = 0
    for i in range(n - 1, 0, -1):
        prev = dict(pos[i])  # type: ignore[arg-type]
        bad: str | None = None
        for f in fills_by_date.get(axis[i], []):
            qty = f.get("qty")
            if qty is None:
                bad = f"{axis[i]} {f['code']} 成交缺股数,无法倒推"
                break
            qty = int(qty)
            code = f["code"]
            if f["side"] == "buy":
                prev[code] = prev.get(code, 0) - qty
            else:
                prev[code] = prev.get(code, 0) + qty
        if bad is None:
            for code, q in prev.items():
                if q < 0:
                    bad = f"{axis[i]} 之前 {code} 持仓推为负({q}),该日前有漏记的买入/卖出"
                    break
        if bad is not None:
            result.warnings.append(f"账本对不上,历史截断到 {axis[i]}:{bad}")
            result.truncated_at = axis[i]
            truncate_idx = i
            break
        pos[i - 1] = {c: q for c, q in prev.items() if q != 0}

    # ---- 逐日盈亏 + 净值倒推 ----
    def mv(i: int) -> float:
        return sum(q * book.close_at(c, axis[i]) for c, q in pos[i].items())  # type: ignore[union-attr]

    equity = [0.0] * n
    equity[n - 1] = anchor_equity
    price_proxy_warned = False
    for i in range(n - 1, truncate_idx, -1):
        buy_cost = 0.0
        sell_proceeds = 0.0
        for f in fills_by_date.get(axis[i], []):
            price = f.get("price")
            if price is None:
                price = book.close_at(f["code"], axis[i])
                if not price_proxy_warned:
                    result.warnings.append(f"{axis[i]} {f['code']} 等成交缺价格,按当日收盘价近似")
                    price_proxy_warned = True
            amt = float(price) * int(f["qty"])
            if f["side"] == "buy":
                buy_cost += amt
            else:
                sell_proceeds += amt
        ce = cash_events_by_date.get(axis[i], {})
        pnl = (
            (mv(i) - mv(i - 1))
            - buy_cost
            + sell_proceeds
            - float(ce.get("fees", 0.0))
            + float(ce.get("dividend", 0.0))
            + float(ce.get("repo_income", 0.0))
        )
        equity[i - 1] = equity[i] - pnl

    if book.warned_codes:
        result.warnings.append(
            "以下代码本地缺收盘价,已用邻近价/成交价近似: " + ", ".join(sorted(book.warned_codes))
        )

    result.points = [
        {"date": axis[i], "total_asset": round(equity[i], 2)} for i in range(truncate_idx, n - 1)
    ]
    return result


async def build_reconstructed_history(
    storage,
    *,
    positions_now: dict[str, int],
    anchor_date: str,
    anchor_equity: float,
    today_bj: str,
    window_start: str = LEDGER_COMPLETE_SINCE,
) -> ReconstructionResult:
    """取数编排:trade_notes 流水 + backtest_daily 收盘价 → reconstruct_equity。

    positions_now: 当前(此刻)股票持仓 {裸码: 股数},调用方已剔除逆回购等非股票码。
    anchor_date/anchor_equity: 最早一笔券商快照(该日收盘总资产真值)。
    display 路径:任何数据问题走 warnings/截断,不抛给页面 500。
    """
    from datetime import date as _date

    from src.notes.note_store import TradeNoteStore

    result_empty = ReconstructionResult(anchor_date=anchor_date)
    if anchor_date <= window_start:
        return result_empty

    events = await TradeNoteStore(storage).list_events_in_range(window_start, today_bj)

    fills_by_date: dict[str, list[dict]] = {}
    cash_events: dict[str, dict] = {}

    def _cash(d: str) -> dict:
        return cash_events.setdefault(d, {"fees": 0.0, "dividend": 0.0, "repo_income": 0.0})

    for ev in events:
        d = (ev.ts + timedelta(hours=8)).strftime("%Y-%m-%d")  # UTC → 北京日
        fees = sum(v for v in (ev.commission, ev.transfer_fee, ev.stamp_tax) if v is not None)
        if ev.event_type == "逆回购":
            ce = _cash(d)
            ce["fees"] += fees
            if ev.repo_income is not None:
                ce["repo_income"] += float(ev.repo_income)
            continue
        if ev.event_type not in ("买入", "卖出") or ev.side not in ("buy", "sell"):
            continue  # 评论/复盘等无资金事件
        if is_non_stock_position_code(ev.code):
            ce = _cash(d)
            ce["fees"] += fees
            continue
        ce = _cash(d)
        ce["fees"] += fees
        if ev.dividend is not None:
            ce["dividend"] += float(ev.dividend)
        fills_by_date.setdefault(d, []).append(
            {
                "code": ev.code.split(".")[0],
                "side": ev.side,
                "qty": ev.qty,
                "price": ev.price,
            }
        )

    codes = {c.split(".")[0] for c in positions_now}
    for fills in fills_by_date.values():
        codes.update(f["code"] for f in fills)
    if not codes and not cash_events:
        return result_empty

    # 收盘价:窗口前多取 40 天,给窗口起点处停牌的票留前向补价的余量
    y, m, dd = (int(p) for p in window_start.split("-"))
    close_query_start = (_date(y, m, dd) - timedelta(days=40)).isoformat()
    closes_by_code: dict[str, list[tuple[str, float]]] = {}
    close_dates: set[str] = set()
    for code in sorted(codes):
        rows = await storage.get_daily_for_code(code, close_query_start, anchor_date)
        series: list[tuple[str, float]] = []
        for r in rows:
            close = r["close_price"]
            if close is None or float(close) <= 0:
                continue
            d = _ts_to_date_str(r["ts"])
            series.append((d, float(close)))
            if d >= window_start:
                close_dates.add(d)
        closes_by_code[code] = series

    fill_dates_in_window = {d for d in fills_by_date if d <= anchor_date}
    axis = sorted(
        {d for d in close_dates if d <= anchor_date} | fill_dates_in_window | {anchor_date}
    )
    result = ReconstructionResult(anchor_date=anchor_date)
    if axis and axis[-1] != anchor_date:
        # 理论上 anchor_date 已并入 axis;保底防御
        axis.append(anchor_date)

    # 锚点日之后(通常是今天)的成交:把当前持仓倒推回锚点日收盘
    positions_at_anchor = {c.split(".")[0]: int(q) for c, q in positions_now.items() if q}
    for d in sorted((d for d in fills_by_date if d > anchor_date), reverse=True):
        for f in fills_by_date[d]:
            if f["qty"] is None:
                result.warnings.append(
                    f"{d} {f['code']} 成交缺股数,无法从当前持仓倒推,历史重建放弃"
                )
                return result
            q = int(f["qty"])
            c = f["code"]
            if f["side"] == "buy":
                positions_at_anchor[c] = positions_at_anchor.get(c, 0) - q
            else:
                positions_at_anchor[c] = positions_at_anchor.get(c, 0) + q
    negative = {c: q for c, q in positions_at_anchor.items() if q < 0}
    if negative:
        result.warnings.append(f"锚点日后成交与当前持仓对不上(推出负持仓 {negative}),历史重建放弃")
        return result

    fills_in_axis = {d: v for d, v in fills_by_date.items() if d <= anchor_date}
    cash_in_axis = {d: v for d, v in cash_events.items() if d <= anchor_date}
    core = reconstruct_equity(
        anchor_equity=anchor_equity,
        positions_at_axis_end=positions_at_anchor,
        fills_by_date=fills_in_axis,
        cash_events_by_date=cash_in_axis,
        closes_by_code=closes_by_code,
        axis=axis,
    )
    core.warnings = result.warnings + core.warnings
    return core


def _ts_to_date_str(ts) -> str:
    """backtest_daily 的 ts(datetime 或 epoch ms)→ 'YYYY-MM-DD'(UTC 日=交易日)。"""
    from datetime import datetime, timezone

    if isinstance(ts, datetime):
        dt = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    else:
        dt = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")
