"""Fixed, notification-only V22 entry timing facts and their source boundary.

No selection, gate, position, order, persistence, or delivery is performed here.
Stock features consume the original 09:30--09:39 raw labels without shifting.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from math import isfinite
from numbers import Real
from typing import Any
from zoneinfo import ZoneInfo

import httpx

TZ = ZoneInfo("Asia/Shanghai")
LABELS = tuple(f"09:{minute:02d}" for minute in range(30, 40))
INDEX_CODE = "000001.SH"
THRESHOLDS = {
    "max_drop_3m_pct": 1.61,
    "prefix_amount_yuan": 418_000_000.0,
    "last3_amount_share": 0.3647,
}
TENCENT_URL = "https://ifzq.gtimg.cn/appstock/app/minute/query"


class _Tencent0940Pending(ValueError):
    """The validated current-day sequence has not published 09:40 yet."""


def _number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, Real) or not isfinite(float(value)):
        raise ValueError("non-finite or non-numeric market value")
    return float(value)


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("entry timing requires an aware timestamp")
    return value.astimezone(TZ)


def project_features(
    early_bars: Mapping[str, Sequence[Any]], trade_date: date, codes: Sequence[str]
) -> dict[str, dict[str, float]]:
    """Return only complete valid requested prefixes; absent codes stay absent.

    max_drop_3m_pct is the maximum decline across closing-price pairs whose
    labels differ by 1--3 minutes. The 09:30 open is not a timed close pair.
    Amount is yuan; the final-three-minute numerator is 09:37--09:39 inclusive.
    """
    result = {}
    for code in dict.fromkeys(codes):
        try:
            rows = []
            for bar in early_bars.get(code, ()):
                if bar.end_label not in LABELS:
                    continue
                stamp = _aware(bar.bar_end)
                if stamp.date() != trade_date:
                    continue
                if stamp.second or stamp.microsecond or stamp.strftime("%H:%M") != bar.end_label:
                    raise ValueError("minute label differs from source timestamp")
                if bar.stock_code != code:
                    raise ValueError("mixed instrument prefix")
                prices = [
                    _number(getattr(bar, key))
                    for key in ("open_price", "high_price", "low_price", "close_price")
                ]
                op, high, low, close = prices
                volume, amount = _number(bar.volume), _number(bar.amount)
                if (
                    min(prices) <= 0
                    or high < max(prices)
                    or low > min(prices)
                    or volume < 0
                    or amount < 0
                ):
                    raise ValueError("invalid minute OHLC or flow")
                if not (
                    (volume == 0 and amount == 0)
                    or (
                        volume > 0 and amount > 0 and low - 0.011 <= amount / volume <= high + 0.011
                    )
                ):
                    raise ValueError("minute amount/volume price differs from OHLC")
                rows.append((bar.end_label, close, amount))
            rows.sort()
            if tuple(row[0] for row in rows) != LABELS:
                continue
            amount = sum(row[2] for row in rows)
            if amount <= 0 or not isfinite(amount):
                continue
            closes = [row[1] for row in rows]
            maximum = max(
                [0.0]
                + [
                    100 * (1 - closes[later] / closes[earlier])
                    for earlier in range(9)
                    for later in range(earlier + 1, min(earlier + 4, 10))
                ]
            )
            result[code] = {
                "max_drop_3m_pct": maximum,
                "prefix_amount_yuan": amount,
                "last3_amount_share": sum(row[2] for row in rows[-3:]) / amount,
            }
        except (AttributeError, TypeError, ValueError, OverflowError):
            # A missing feature is explicit to the evaluator; it is never
            # substituted with a neutral shape or an old stock's observation.
            continue
    return result


def _rows(response: Any) -> list[dict[str, Any]]:
    data = response["data"]
    fields, items = data["fields"], data["items"]
    if (
        not isinstance(fields, list)
        or len(fields) != len(set(fields))
        or not isinstance(items, list)
    ):
        raise ValueError("invalid provider field set")
    return [dict(zip(fields, row, strict=True)) for row in items]


def _source_stamp(value: Any) -> datetime:
    if not isinstance(value, str) or not (" " in value or "T" in value):
        raise ValueError("index timestamp lacks date and time")
    stamp = datetime.fromisoformat(value)
    return stamp.replace(tzinfo=TZ) if stamp.tzinfo is None else stamp.astimezone(TZ)


def _symbol_value(symbol: Any, key: str, default: Any = None) -> Any:
    return (
        symbol.get(key, default) if isinstance(symbol, Mapping) else getattr(symbol, key, default)
    )


def matches_wait_rule(feature: Mapping[str, Any]) -> bool:
    """The displayed fixed thresholds, with missing/invalid input never a hit."""
    try:
        drop, amount, share = (_number(feature[key]) for key in THRESHOLDS)
        if drop < 0 or amount <= 0 or not 0 <= share <= 1:
            return False
        return (drop > 1.61 and amount <= 418_000_000.0) or (drop <= 1.61 and share > 0.3647)
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


async def _evaluate_tushare_entry_timing(
    client: Any,
    *,
    trade_date: date,
    prior_trade_date: date,
    symbols: Sequence[Any],
    features: Mapping[str, Mapping[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    """Evaluate the fixed advisory from exact source facts, without side effects.

    The caller supplies only the original allowed Top3 and owns any wait for
    09:40 publication. Current-date requests never use historical minutes.
    """
    observed_at = _aware(now)
    result: dict[str, Any] = {
        "schema": "v22-entry-timing/v1",
        "trade_date": trade_date.isoformat(),
        "prior_trade_date": prior_trade_date.isoformat(),
        "evaluated_at": observed_at.isoformat(),
        "status": "UNAVAILABLE",
        "reason": "",
        "symbols": [],
        "index": {},
        "thresholds": dict(THRESHOLDS),
        "stock_window": ["09:30", "09:39"],
    }

    def unavailable(reason: str, **details: Any) -> dict[str, Any]:
        return {**result, "status": "UNAVAILABLE", "reason": reason, **details}

    if not symbols:
        return {**result, "status": "NO_WAIT", "reason": "NO_ALLOWED_SYMBOLS"}
    selected: list[dict[str, Any]] = []
    try:
        for symbol in symbols:
            code = _symbol_value(symbol, "code")
            if not isinstance(code, str) or len(code) != 6 or not code.isdigit():
                raise ValueError("invalid allowed symbol")
            fact = {key: _number(features[code][key]) for key in THRESHOLDS}
            if (
                fact["max_drop_3m_pct"] < 0
                or fact["prefix_amount_yuan"] <= 0
                or not 0 <= fact["last3_amount_share"] <= 1
            ):
                raise ValueError("invalid stock feature range")
            selected.append(
                {"code": code, "name": str(_symbol_value(symbol, "name", "")), "features": fact}
            )
        if len({item["code"] for item in selected}) != len(selected):
            raise ValueError("duplicate allowed symbol")
    except (KeyError, TypeError, ValueError, OverflowError):
        return unavailable("STOCK_FEATURES_UNAVAILABLE")
    result["evaluated_symbols"] = selected
    if not any(matches_wait_rule(item["features"]) for item in selected):
        return {**result, "status": "NO_WAIT", "reason": "NO_STOCK_RULE_MATCH"}
    cutoff = datetime.combine(trade_date, time(9, 40), TZ)
    if observed_at < cutoff:
        return unavailable("INDEX_0940_PENDING")
    if prior_trade_date >= trade_date:
        return unavailable("PRIOR_INDEX_DATE_INVALID")

    historical = trade_date < observed_at.date()
    api = "idx_mins" if historical else "rt_idx_min_daily"
    params = (
        {
            "ts_code": INDEX_CODE,
            "freq": "1min",
            "start_date": f"{trade_date} 09:40:00",
            "end_date": f"{trade_date} 09:40:00",
        }
        if historical
        else {"ts_code": INDEX_CODE, "freq": "1MIN"}
    )
    time_field = "trade_time" if historical else "time"
    result["index"] = {
        "ts_code": INDEX_CODE,
        "source": "TUSHARE_INDEX_MINUTE",
        "vendor": "Tushare",
        "minute_api": api,
        "minute_params": params,
        "prior_daily_api": "index_daily",
        "prior_daily_params": {
            "ts_code": INDEX_CODE,
            "trade_date": prior_trade_date.strftime("%Y%m%d"),
        },
    }
    try:
        minute_rows = _rows(
            await client._api_call(
                api, params, fields=f"ts_code,{time_field},open,close,high,low,vol,amount"
            )
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        return unavailable("INDEX_MINUTE_SOURCE_ERROR", source_error_type=type(exc).__name__)
    try:
        exact = []
        for row in minute_rows:
            if row.get("ts_code") != INDEX_CODE:
                return unavailable("INDEX_IDENTITY_MISMATCH")
            stamp = _source_stamp(row[time_field])
            if stamp.date() != trade_date:
                return unavailable("INDEX_TRADE_DATE_MISMATCH")
            if stamp == cutoff:
                exact.append(row)
        if not exact:
            return unavailable("INDEX_0940_PENDING")
        if len(exact) != 1:
            return unavailable("INDEX_0940_DUPLICATE")
        minute_row = exact[0]
        close = _number(minute_row["close"])
        if close <= 0:
            raise ValueError("index close must be positive")
        result["index"].update(
            minute_row=dict(minute_row), close_0940=close, bar_time=cutoff.isoformat()
        )
    except (KeyError, TypeError, ValueError, OverflowError):
        return unavailable("INDEX_MINUTE_INVALID")

    try:
        daily_rows = _rows(
            await client._api_call(
                "index_daily",
                result["index"]["prior_daily_params"],
                fields="ts_code,trade_date,close",
            )
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        return unavailable("INDEX_DAILY_SOURCE_ERROR", source_error_type=type(exc).__name__)
    try:
        if len(daily_rows) != 1:
            return unavailable("PRIOR_INDEX_DAILY_UNAVAILABLE")
        daily_row = daily_rows[0]
        if daily_row.get("ts_code") != INDEX_CODE or daily_row.get(
            "trade_date"
        ) != prior_trade_date.strftime("%Y%m%d"):
            return unavailable("PRIOR_INDEX_DAILY_MISMATCH")
        prior = _number(daily_row["close"])
        if prior <= 0:
            raise ValueError("prior index close must be positive")
        result["index"].update(
            prior_daily_row=dict(daily_row),
            prior_close=prior,
            return_from_prior_close=close / prior - 1,
            green=close < prior,
        )
    except (KeyError, TypeError, ValueError, OverflowError):
        return unavailable("PRIOR_INDEX_DAILY_INVALID")
    if close >= prior:
        return {**result, "status": "NO_WAIT", "reason": "INDEX_NOT_GREEN"}

    matches = []
    for item in selected:
        if matches_wait_rule(item["features"]):
            matches.append(item)
    return {
        **result,
        "status": "WAIT" if matches else "NO_WAIT",
        "symbols": matches,
        "reason": "FIXED_RULE_MATCHED" if matches else "NO_STOCK_RULE_MATCH",
    }


async def _fetch_tencent_index() -> Any:
    """One public current-day GET, using no market-client token or headers."""
    async with httpx.AsyncClient(timeout=8.0, follow_redirects=False, trust_env=False) as public:
        response = await public.get(TENCENT_URL, params={"code": "sh000001"})
        response.raise_for_status()
        return response.json()


def _tencent_facts(payload: Any, trade_date: date) -> dict[str, Any]:
    if type(payload.get("code")) is not int or payload["code"] != 0:
        raise ValueError("Tencent response did not succeed")
    instrument = payload["data"]["sh000001"]
    minute = instrument["data"]
    expected_date = trade_date.strftime("%Y%m%d")
    if minute["date"] != expected_date:
        raise ValueError("Tencent current-minute date differs from target date")
    qt = instrument["qt"]["sh000001"]
    if not isinstance(qt, list) or len(qt) <= 30 or qt[1] != "上证指数" or qt[2] != "000001":
        raise ValueError("Tencent index identity differs from Shanghai Composite")
    quote_time = datetime.strptime(qt[30], "%Y%m%d%H%M%S").replace(tzinfo=TZ)
    if quote_time.date() != trade_date:
        raise ValueError("Tencent quote date differs from minute date")
    lines = minute["data"]
    if not isinstance(lines, list) or not all(isinstance(line, str) for line in lines):
        raise ValueError("Tencent minute sequence is invalid")
    selected = [line for line in lines if line.split() and line.split()[0] == "0940"]
    if not selected:
        raise _Tencent0940Pending("Tencent current sequence has no 09:40 observation")
    if len(selected) > 1:
        raise ValueError("Tencent requires exactly one 09:40 observation")
    parts = selected[0].split()
    if len(parts) != 4:
        raise ValueError("Tencent 09:40 observation has invalid fields")
    close, cumulative_volume, cumulative_amount = [_number(float(value)) for value in parts[1:]]
    if isinstance(qt[4], bool):
        raise ValueError("Tencent prior close is boolean")
    prior = _number(float(qt[4]))
    if min(close, prior) <= 0 or min(cumulative_volume, cumulative_amount) < 0:
        raise ValueError("Tencent prices or cumulative flow are invalid")
    return {
        "source": "TENCENT_CURRENT_MINUTE",
        "vendor": "Tencent",
        "ts_code": INDEX_CODE,
        "source_url": TENCENT_URL,
        "source_params": {"code": "sh000001"},
        "minute_date": minute["date"],
        "minute_row": selected[0],
        "bar_time": datetime.combine(trade_date, time(9, 40), TZ).isoformat(),
        "quote_identity": {"code": qt[2], "name": qt[1], "time": qt[30]},
        "prior_close_raw": qt[4],
        "prior_close_source": "Tencent qt[4] previous close",
        "close_0940": close,
        "prior_close": prior,
        "return_from_prior_close": close / prior - 1,
        "green": close < prior,
    }


async def evaluate_entry_timing(
    client: Any,
    *,
    trade_date: date,
    prior_trade_date: date,
    symbols: Sequence[Any],
    features: Mapping[str, Mapping[str, Any]],
    now: datetime,
    tencent_fetch: Any = None,
) -> dict[str, Any]:
    """Prefer official minutes, then validated current-day public index facts.

    The public source is a separate vendor's same-clock sample, not an assertion
    of exact numeric equivalence to Tushare. Its source and raw facts are saved.
    Historical evaluations never consult a current-day public quote.
    """
    result = await _evaluate_tushare_entry_timing(
        client,
        trade_date=trade_date,
        prior_trade_date=prior_trade_date,
        symbols=symbols,
        features=features,
        now=now,
    )
    observed_at = _aware(now)
    if (
        result["status"] != "UNAVAILABLE"
        or trade_date != observed_at.date()
        or observed_at < datetime.combine(trade_date, time(9, 40), TZ)
        or result["reason"] in {"STOCK_FEATURES_UNAVAILABLE", "PRIOR_INDEX_DATE_INVALID"}
    ):
        return result
    fallback_from = {"reason": result["reason"], "index": result["index"]}
    if "source_error_type" in result:
        fallback_from["source_error_type"] = result["source_error_type"]
    try:
        payload = await (tencent_fetch or _fetch_tencent_index)()
        facts = _tencent_facts(payload, trade_date)
    except asyncio.CancelledError:
        raise
    except _Tencent0940Pending:
        return {
            **result,
            "reason": "INDEX_0940_PENDING",
            "fallback_pending": {
                "source": "TENCENT_CURRENT_MINUTE",
                "trade_date": trade_date.isoformat(),
                "reason": "INDEX_0940_PENDING",
            },
        }
    except Exception as exc:
        return {
            **result,
            "fallback_unavailable": {
                "source": "TENCENT_CURRENT_MINUTE",
                "error_type": type(exc).__name__,
            },
        }
    facts["fallback_from"] = fallback_from
    matches = (
        [item for item in result["evaluated_symbols"] if matches_wait_rule(item["features"])]
        if facts["green"]
        else []
    )
    return {
        **result,
        "index": facts,
        "status": "WAIT" if matches else "NO_WAIT",
        "symbols": matches,
        "reason": "FIXED_RULE_MATCHED"
        if matches
        else "NO_STOCK_RULE_MATCH"
        if facts["green"]
        else "INDEX_NOT_GREEN",
    }
