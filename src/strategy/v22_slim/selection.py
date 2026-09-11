"""Frozen entry data projection, isolated from production V16 resources."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date
from math import isfinite
from pathlib import Path

import numpy as np
import pandas as pd

from src.data.clients.tushare_realtime import TushareEarlyMarketData, TushareQuote
from src.strategy.filters.stock_filter import StockFilter, StockFilterConfig
from src.strategy.v20.selection_scanner import V16Scanner, V16StockData
from src.strategy.v20.selection_scorer import LGBRankScorer

ASSET_ROOT = Path(__file__).resolve().parents[3] / "models" / "v22_slim"
LABELS = tuple(f"09:{minute:02}" for minute in range(30, 40))


def read_asset(name: str) -> bytes:
    manifest = json.loads((ASSET_ROOT / "manifest.json").read_text(encoding="utf-8"))
    content = (ASSET_ROOT / name).read_bytes()
    if hashlib.sha256(content).hexdigest() != manifest["sha256"][name]:
        raise ValueError(f"V22-slim asset integrity failure: {name}")
    return content


class FrozenBoards:
    def __init__(self):
        raw = json.loads(read_asset("board_constituents.json"))
        self._board_stocks = {
            board: [(str(row[0])[:6], str(row[1]) if len(row) > 1 else "") for row in rows if row]
            for board, rows in raw.items()
        }
        self.names = {code: name for rows in self._board_stocks.values() for code, name in rows}

    def _ensure_loaded(self):
        """The constructor has already validated and loaded the fixed asset."""

    async def batch_filter_st(self, codes):
        return [
            code for code in codes if code in self.names and "ST" not in self.names[code].upper()
        ]


def make_scanner():
    boards = FrozenBoards()
    read_asset("lgbrank_latest.txt")
    read_asset("feature_list.json")
    scorer = LGBRankScorer(ASSET_ROOT / "lgbrank_latest.txt", ASSET_ROOT / "feature_list.json")
    scanner = V16Scanner(
        boards,
        boards,
        StockFilter(
            StockFilterConfig(
                exclude_bse=True, exclude_chinext=True, exclude_star=True, exclude_sme=False
            )
        ),
        scorer,
    )
    return scanner, scorer, boards


def exact_early(data: TushareEarlyMarketData, day: date) -> TushareEarlyMarketData | None:
    rows = [bar for bar in data.early_bars if bar.end_label in LABELS and bar.bar_end.date() == day]
    rows.sort(key=lambda bar: bar.end_label)
    if tuple(bar.end_label for bar in rows) != LABELS:
        return None
    code = data.quote.stock_code
    for bar in rows:
        prices = (bar.open_price, bar.high_price, bar.low_price, bar.close_price)
        if (
            bar.stock_code != code
            or any(not isfinite(v) or v <= 0 for v in prices)
            or bar.high_price < max(prices)
            or bar.low_price > min(prices)
            or not isfinite(bar.volume)
            or not isfinite(bar.amount)
            or bar.volume < 0
            or bar.amount < 0
        ):
            return None
        pair_valid = bar.volume == 0 and bar.amount == 0
        if bar.volume > 0 and bar.amount > 0:
            pair_valid = bar.low_price - 0.011 <= bar.amount / bar.volume <= bar.high_price + 0.011
        if not pair_valid:
            return None
    volume = sum(bar.volume for bar in rows)
    amount = sum(bar.amount for bar in rows)
    quote = TushareQuote(
        stock_code=code,
        open_price=rows[0].open_price,
        latest_price=rows[-1].close_price,
        high_price=max(bar.high_price for bar in rows),
        low_price=min(bar.low_price for bar in rows),
        volume=volume,
        amount=amount,
        early_close=rows[-1].close_price,
        early_high=max(bar.high_price for bar in rows),
        early_low=min(bar.low_price for bar in rows),
        early_volume=volume,
        volume_937=sum(bar.volume for bar in rows if bar.end_label <= "09:37"),
        early_bar_end=rows[-1].bar_end,
    )
    # Preserve raw evidence, including optional auction rows, for receipt audit.
    # Only the exact ten-row projection is eligible for features and amount.
    return replace(data, quote=quote)


def build_stock(code: str, name: str, quote, history: dict, calendar: tuple[date, ...], day: date):
    previous = [d.isoformat() for d in calendar if d < day][-37:]
    if len(previous) != 37:
        raise ValueError("V22-slim requires 37 previous exchange sessions")
    fields = ("time", "open", "high", "low", "close", "volume")
    if len({len(history.get(field, [])) for field in fields}) != 1:
        raise ValueError("unequal history columns")
    frame = pd.DataFrame({field: history[field] for field in fields})
    frame["time"] = frame.time.map(lambda value: str(value)[:10])
    frame = frame.loc[frame.time.isin(previous)].sort_values("time")
    if frame.time.duplicated().any():
        raise ValueError("duplicate historical daily bar")
    if len(frame) < 5 or previous[-1] not in set(frame.time):
        return None
    values = frame[list(fields[1:])].to_numpy(dtype=float)
    if (
        not np.isfinite(values).all()
        or not (values[:, :4] > 0).all()
        or not (values[:, 4] >= 0).all()
    ):
        return None
    if (
        not (values[:, 1] >= values[:, :4].max(axis=1)).all()
        or not (values[:, 2] <= values[:, :4].min(axis=1)).all()
    ):
        return None
    closes, volumes = values[:, 3], values[:, 4]
    if volumes.mean() <= 0 or quote.early_volume <= 0 or quote.amount <= 0:
        return None
    returns = (np.diff(closes) / closes[:-1])[-20:]
    consecutive = 0
    for index in range(len(closes) - 1, 0, -1):
        if closes[index] <= closes[index - 1]:
            break
        consecutive += 1
    return V16StockData(
        code=code,
        name=name,
        open_price=quote.open_price,
        prev_close=float(closes[-1]),
        price_940=quote.early_close,
        high_940=quote.early_high,
        low_940=quote.early_low,
        volume_940=quote.early_volume,
        volume_937=quote.volume_937,
        avg_daily_volume=float(volumes.mean()),
        trend_5d=float(closes[-1] / closes[-6] - 1) if len(closes) >= 6 else 0.0,
        trend_10d=float(closes[-1] / closes[-11] - 1) if len(closes) >= 11 else 0.0,
        avg_daily_return_20d=float(returns.mean()),
        volatility_20d=float(returns.std()) if len(returns) >= 2 else 0.0,
        consecutive_up_days=consecutive,
        history_df=frame[list(fields[1:])].reset_index(drop=True),
    )


def market_projection(early_bars, day: date) -> dict:
    result = {}
    for code, bars in early_bars.items():
        if not code.startswith(("00", "60")):
            continue
        placeholder = TushareQuote(code, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        projected = exact_early(TushareEarlyMarketData(placeholder, tuple(bars), ""), day)
        if projected is not None:
            result[code] = {"close": projected.quote.early_close, "amount": projected.quote.amount}
    if len(result) < 1000:
        raise ValueError("V22-slim whole-market early snapshot has insufficient coverage")
    return result
