"""Frozen V22 exit signals from completed facts; no orders or assumed sales."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from math import isfinite
from typing import Any


@dataclass(frozen=True)
class Bar:
    at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float

    @property
    def valid(self) -> bool:
        values = (self.open, self.high, self.low, self.close, self.volume, self.amount)
        return (
            all(isfinite(v) for v in values)
            and min(values[:4]) > 0
            and min(values[4:]) >= 0
            and self.high >= max(self.open, self.close, self.low)
            and self.low <= min(self.open, self.close, self.high)
        )


@dataclass(frozen=True)
class Signal:
    reason: str
    at: datetime
    price: float | None
    detail: str


REASONS = {
    "D1_STOP": "买入后第1个交易日，跌幅达到8%",
    "D2_STOP": "买入后第2个交易日，跌幅达到12%",
    "D2_RISK_STOP": "市场风险预警生效，买入后第2个交易日跌幅达到5%",
    "RECOVERY_FAILED": "买入后第1个交易日，先走弱、反弹后再次跌破短均线和成交均价",
    "D2_PLAN": "已到买入后第2个交易日尾盘，执行计划卖出",
    "D3_PLAN": "强势延持结束，已到下一交易日卖出时间",
}


def adjusted_days(rows: list[dict[str, Any]]) -> list[dict[str, float]]:
    result = []
    factor = 1.0
    for row in rows:
        values = [float(row[k]) for k in ("open", "high", "low", "close", "pre_close")]
        if not all(isfinite(v) and v > 0 for v in values):
            raise ValueError("V22 daily price chain is incomplete")
        item = {
            k: factor * float(row[k]) / float(row["pre_close"])
            for k in ("open", "high", "low", "close")
        }
        result.append(item)
        factor = item["close"]
    return result


def prior_atr(rows: list[dict[str, Any]]) -> float | None:
    if len(rows) < 15:
        return None
    adjusted = adjusted_days(rows[-15:])
    ranges = [
        max(r["high"] - r["low"], abs(r["high"] - p["close"]), abs(r["low"] - p["close"]))
        for p, r in zip(adjusted, adjusted[1:], strict=False)
    ]
    value = sum(ranges) / 14 / adjusted[-1]["close"]
    return value if isfinite(value) and value > 0 else None


def strong_at_1456(bars: list[Bar], prior: list[dict[str, Any]], pre_close: float) -> bool:
    if not bars or len(prior) < 3 or not isfinite(pre_close) or pre_close <= 0:
        return False
    day = bars[0].at.replace(hour=9, minute=30, second=0, microsecond=0)
    expected = [day + timedelta(minutes=i) for i in range(121)]
    expected += [day.replace(hour=13, minute=1) + timedelta(minutes=i) for i in range(116)]
    prefix = [b for b in bars if b.at.strftime("%H:%M") <= "14:56"]
    if [b.at for b in prefix] != expected or not all(b.valid for b in prefix):
        return False
    if sum(b.volume for b in prefix) <= 0 or sum(b.amount for b in prefix) <= 0:
        return False
    current = dict(
        open=prefix[0].open,
        high=max(b.high for b in prefix),
        low=min(b.low for b in prefix),
        close=prefix[-1].close,
        pre_close=pre_close,
    )
    p3, p2, p1, c = adjusted_days([*prior[-3:], current])

    def shape(row: dict[str, float]) -> dict[str, Any]:
        o, h, low, close = (row[k] for k in ("open", "high", "low", "close"))
        top, bottom = max(o, close), min(o, close)
        body, upper, lower = top - bottom, max(0.0, h - top), max(0.0, bottom - low)
        return dict(
            row,
            top=top,
            bottom=bottom,
            body=body,
            upper=upper,
            lower=lower,
            white=close > o,
            black=close < o,
            dominant=body > upper + lower,
        )

    p3, p2, p1, c = map(shape, (p3, p2, p1, c))
    up_pre = p1["close"] > p3["close"] and p1["high"] > p3["high"] and p1["low"] >= p3["low"]
    up_now = c["close"] > p2["close"] and c["high"] > p2["high"] and c["low"] >= p2["low"]
    strong = (
        (up_now and c["white"] and c["dominant"] and c["close"] > p1["close"])
        or (
            up_now
            and p1["black"]
            and c["white"]
            and c["open"] <= p1["close"]
            and c["close"] >= p1["open"]
            and (c["open"] < p1["close"] or c["close"] > p1["open"])
            and c["close"] > p2["close"]
        )
        or (up_now and c["low"] > p1["high"] and c["close"] >= c["open"])
    )
    weak = (
        c["close"] < min(p1["low"], p2["low"])
        or (
            up_pre
            and p1["white"]
            and c["black"]
            and c["open"] >= p1["close"]
            and c["close"] <= p1["open"]
            and (c["open"] > p1["close"] or c["close"] < p1["open"])
        )
        or (
            up_pre
            and p1["white"]
            and p1["dominant"]
            and c["black"]
            and c["open"] > p1["close"]
            and p1["open"] < c["close"] < (p1["open"] + p1["close"]) / 2
        )
    )
    warning = (
        (up_pre and c["upper"] > 0 and c["upper"] >= 2 * c["body"] and c["lower"] <= c["body"])
        or (up_pre and c["lower"] > 0 and c["lower"] >= 2 * c["body"] and c["upper"] <= c["body"])
        or (
            up_pre
            and p1["white"]
            and p1["dominant"]
            and c["top"] < p1["top"]
            and c["bottom"] > p1["bottom"]
            and (c["black"] or c["body"] <= c["upper"] + c["lower"])
        )
        or (c["black"] and c["close"] < p1["close"])
        or (not strong and not up_now)
    )
    return bool(strong and not weak and not warning)


def recovery_failure(
    bars: list[Bar],
    *,
    before_factor: float,
    pre_close: float,
    entry: float,
    up_limit: float,
    atr: float | None,
) -> Signal | None:
    if atr is None or not all(
        isfinite(v) and v > 0 for v in (before_factor, pre_close, entry, up_limit)
    ):
        return None
    if abs(pre_close / entry - before_factor) > 1e-9:
        return None
    state, count, continuous = 0, 0, 0
    previous: datetime | None = None
    ema5 = ema20 = 0.0
    history20: list[float] = []
    volume = amount = 0.0
    vwap_ok = True
    for b in bars:
        clock = b.at.strftime("%H:%M")
        pair_ok = b.valid and (
            (b.volume == b.amount == 0)
            or (
                b.volume > 0
                and b.amount > 0
                and b.low - 0.011 <= b.amount / b.volume <= b.high + 0.011
            )
        )
        vwap_ok = vwap_ok and pair_ok
        if pair_ok:
            volume += b.volume
            amount += b.amount
        active = "09:31" <= clock <= "11:30" or "13:01" <= clock <= "14:56"
        contiguous = previous is not None and b.at - previous == timedelta(minutes=1)
        if not active or not b.valid:
            state, count, continuous = 0, 0, 0
            previous = None
            history20 = []
            continue
        if not contiguous:
            state, count, continuous = 0, 0, 0
            history20 = []
        ema5 = b.close if continuous == 0 else b.close / 3 + ema5 * 2 / 3
        ema20 = b.close if continuous == 0 else b.close * 2 / 21 + ema20 * 19 / 21
        continuous += 1
        history20.append(ema20)
        previous = b.at
        if not vwap_ok or volume <= 0 or len(history20) <= 5:
            state, count = 0, 0
            continue
        vwap = amount / volume
        net = before_factor * b.close / pre_close * 0.999 * 0.999 / 1.001 - 1
        conditions = (
            net <= 0 and b.close < ema20 and b.close < vwap and ema20 < history20[-6],
            net > 0 and b.close > ema5 and b.close > vwap,
            b.close < ema5 and b.close < vwap,
        )
        count = count + 1 if conditions[state] else 0
        if count < 2:
            continue
        count = 0
        if state < 2:
            state += 1
        elif abs(b.close - up_limit) > 1e-6:
            return Signal("RECOVERY_FAILED", b.at, b.close, REASONS["RECOVERY_FAILED"])
    return None


def evaluate_day(
    bars: list[Bar],
    *,
    phase: int,
    now: datetime,
    entry: float,
    prior: list[dict[str, Any]],
    pre_close: float,
    before_factor: float,
    up_limit: float,
    danger: bool = False,
    extended: bool = False,
) -> tuple[Signal | None, bool]:
    """Return first signal and whether the T+2 STRONG extension applies."""
    if phase <= 0:
        return None, False
    bars = sorted(
        (b for b in bars if b.at.date() == now.date() and b.at <= now), key=lambda b: b.at
    )
    if len({b.at for b in bars}) != len(bars):
        raise ValueError("V22 duplicate minute input")
    if not isfinite(entry) or entry <= 0:
        raise ValueError("V22 buy price is missing")
    signals = []
    if phase in (1, 2):
        threshold = 0.92 if phase == 1 else 0.95 if danger else 0.88
        reason = "D1_STOP" if phase == 1 else "D2_RISK_STOP" if danger else "D2_STOP"
        for b in bars:
            clock = b.at.strftime("%H:%M")
            within = "09:30" <= clock <= "11:30" or "13:01" <= clock <= "14:57"
            if phase == 2 and clock >= "14:57":
                within = False
            if (
                within
                and b.valid
                and b.volume > 0
                and b.amount > 0
                and b.close / entry <= threshold
            ):
                signals.append(Signal(reason, b.at, b.close, REASONS[reason]))
                break
    if phase == 1:
        rebound = recovery_failure(
            bars,
            before_factor=before_factor,
            pre_close=pre_close,
            entry=entry,
            up_limit=up_limit,
            atr=prior_atr(prior),
        )
        if rebound:
            signals.append(rebound)
    if signals:
        return min(signals, key=lambda s: s.at), False
    if phase == 2 and now.strftime("%H:%M") >= "14:57":
        strong = strong_at_1456(bars, prior, pre_close)
        if strong:
            return None, True
        at = now.replace(hour=14, minute=57, second=0, microsecond=0)
        observed = [b for b in bars if b.at == at and b.valid]
        return Signal(
            "D2_PLAN", at, observed[0].close if observed else None, REASONS["D2_PLAN"]
        ), False
    if phase >= 3 and extended:
        first = next(
            (
                b
                for b in bars
                if b.valid
                and b.volume > 0
                and b.amount > 0
                and (
                    "09:30" <= b.at.strftime("%H:%M") <= "11:30"
                    or "13:01" <= b.at.strftime("%H:%M") <= "14:57"
                )
            ),
            None,
        )
        if first:
            return Signal("D3_PLAN", first.at, first.close, REASONS["D3_PLAN"]), True
    return None, extended
