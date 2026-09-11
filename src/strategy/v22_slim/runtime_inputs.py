"""Bind live V22 inputs to the existing durable V20 decision snapshot.

The original service owns the client, pool, calendar and single decision lane.
This adapter creates no scheduler, connection, notification or order operation.
The checkpoint contains reference facts and state, never an open-date lookup.
"""

from __future__ import annotations

import gzip
import json
from dataclasses import replace
from datetime import date
from math import isfinite

from src.data.database.v20_repository import sha256_json
from src.strategy.v20.decision_engine import CompletedHealth, CompletedRolling
from src.strategy.v20.models import deserialize_health_snapshot, serialize_health_snapshot
from src.strategy.v22_slim.policy import close_risk_count, h90, strong_condition
from src.strategy.v22_slim.selection import read_asset


def checkpoint() -> dict:
    result = json.loads(gzip.decompress(read_asset("reference_checkpoint.json.gz")))
    if result.get("schema") != "v22-slim-reference-checkpoint/v1":
        raise ValueError("unsupported V22-slim checkpoint")
    deserialize_health_snapshot(result["health"])
    return result


async def api_rows(client, api: str, params: dict) -> list[dict]:
    # pre_close is explicitly opt-in in the official stk_limit contract.
    # https://tushare.pro/document/2?doc_id=183
    response = await client._api_call(
        api,
        params,
        **(
            {"fields": "trade_date,ts_code,pre_close,up_limit,down_limit"}
            if api == "stk_limit"
            else {}
        ),
    )
    data = response["data"]
    rows = [dict(zip(data["fields"], values, strict=True)) for values in data["items"]]
    if not rows or len(rows) >= (5800 if api == "stk_limit" else 6000):
        raise ValueError(f"V22-slim {api}: empty or possibly truncated response")
    return rows


async def build_inputs(service, bundle, health, rolling, gaps):
    seed = checkpoint()
    day = bundle.trade_date
    anchor = date.fromisoformat(seed["as_of"])
    if day <= anchor:
        raise ValueError("V22-slim decision must follow the frozen state checkpoint")
    calendar = tuple(bundle.computation_calendar)
    index = calendar.index(day)
    preceding = calendar[index - 20 : index]
    if len(preceding) != 20:
        raise ValueError("incomplete V22-slim market calendar")
    client = service._scan_state.realtime_client
    cache = getattr(service, "_v22_input_api_cache", None)
    if cache is None:
        cache = {}
        service._v22_input_api_cache = cache
    for key in tuple(cache):
        if key[0] != day:
            del cache[key]

    async def daily(target):
        if target >= day:
            raise ValueError("cannot read future daily values for entry decisions")
        key = (day, "daily", target)
        if key not in cache:
            rows = await api_rows(client, "daily", {"trade_date": target.strftime("%Y%m%d")})
            if len(rows) < 1000 or any(
                row["trade_date"] != target.strftime("%Y%m%d") for row in rows
            ):
                raise ValueError("invalid completed daily market snapshot")
            if len({row["ts_code"] for row in rows}) != len(rows):
                raise ValueError("duplicate daily identity")
            cache[key] = rows
        return cache[key]

    past = {}
    amount_history = []
    for previous in preceding:
        key = previous.isoformat()
        if previous <= anchor:
            amounts = seed["market_history"].get(key)
            if amounts is None:
                raise ValueError(f"V22-slim checkpoint lacks market session {key}")
        else:
            status = await service._repository.get_entry_status(
                service.config.official_stream_id, previous
            )
            if (
                status is None
                or status.strategy_version != "V22-slim"
                or "v22_market" not in status.snapshot
            ):
                raise ValueError(f"V22-slim needs reference-state recovery for {key}")
            service._verify_entry_binding(status)
            past[previous] = status
            amounts = {
                code: value["amount"] for code, value in status.snapshot["v22_market"].items()
            }
        amount_history.append(amounts)
    market = bundle.snapshot.get("v22_market")
    if not isinstance(market, dict):
        raise ValueError("V22-slim exact early market projection is missing")
    h = h90({code: row["amount"] for code, row in market.items()}, amount_history)
    prior = preceding[-1]
    if prior == anchor:
        risk = seed["risk_after"]
        health_before = seed["health"]
    else:
        previous = past[prior]
        semantic = previous.semantic
        health_before = semantic["health_after"]
        symbols = semantic["reference_symbols"]
        reference_return = None
        if symbols:
            codes = [item["code"] for item in symbols]
            raw = await client.batch_get_minute_history_for_date(codes, prior)
            refs = {}
            for code in codes:
                bars = [
                    bar
                    for bar in raw.get(code, ())
                    if bar.end_label == "09:41" and bar.bar_end.date() == prior
                ]
                if len(bars) == 1 and bars[0].open_price > 0 and isfinite(bars[0].open_price):
                    refs[code] = bars[0].open_price
            closes = {row["ts_code"][:6]: row["close"] for row in await daily(prior)}
            if all(code in refs and code in closes and closes[code] > 0 for code in codes):
                reference_return = (
                    sum(closes[code] / refs[code] - 1 for code in codes) / len(codes) - 0.002
                )
        risk = close_risk_count(
            semantic["risk_streak_before"],
            has_signal=bool(symbols),
            normal_open=semantic["normal_open"],
            reference_return=reference_return,
        )

    # Seed baskets and new production baskets belong to one frozen reference
    # definition. Old V20 picks are never substituted into this stream.
    health = [item for item in health if item.signal_date > anchor]
    rolling = [item for item in rolling if item.signal_date > anchor]
    gaps = [item for item in gaps if item.signal_date > anchor]
    for item in seed["batches"]:
        signal = date.fromisoformat(item["day"])
        maturity = date.fromisoformat(item["t2"])
        if maturity >= day:
            continue
        gross, relative = item["gross_return"], item["relative_return"]
        if not item["complete"]:
            closes = {row["ts_code"][:6]: row["close"] for row in await daily(maturity)}
            returns = {
                code: closes[code] / price - 1
                for code, price in item["references"].items()
                if code in closes and price > 0 and closes[code] > 0
            }
            codes = item["codes"]
            if not all(code in returns for code in codes):
                raise ValueError("incomplete bootstrap rolling basket requires recovery")
            gross = sum(returns[code] for code in codes) / len(codes)
            top3 = codes[:3]
            relative = (
                (sum(returns[code] for code in top3) / 3 - sum(returns.values()) / len(returns))
                if len(top3) == 3 and len(returns) >= 1000
                else None
            )
        identity = "v22-bootstrap:" + item["day"]
        rolling.append(CompletedRolling(identity, signal, maturity, gross))
        if relative is not None:
            health.append(CompletedHealth(identity, signal, maturity, relative, True))

    prior_rows, earlier_rows = await daily(prior), await daily(preceding[-2])

    def total(rows):
        values = [
            float(row["amount"]) * 1000 for row in rows if row["ts_code"].endswith((".SH", ".SZ"))
        ]
        if any(not isfinite(value) or value < 0 for value in values):
            raise ValueError("invalid market turnover")
        return sum(values)

    index_rows = await api_rows(
        client,
        "index_daily",
        {
            "ts_code": "932000.CSI",
            "start_date": preceding[0].strftime("%Y%m%d"),
            "end_date": prior.strftime("%Y%m%d"),
        },
    )
    index_map = {row["trade_date"]: float(row["close"]) for row in index_rows}
    if any(row["ts_code"] != "932000.CSI" for row in index_rows):
        raise ValueError("incorrect CSI 2000 index identity")
    if len(index_map) != len(index_rows) or set(index_map) != {
        d.strftime("%Y%m%d") for d in preceding
    }:
        raise ValueError("index history does not cover twenty exact exchange sessions")
    index_closes = [index_map[d.strftime("%Y%m%d")] for d in preceding]
    strong = strong_condition(
        prior_amount=total(prior_rows),
        earlier_amount=total(earlier_rows),
        index_closes=index_closes,
    )

    # Today's official price-limit table supplies the point-in-time pre_close;
    # previous close cannot be substituted across a corporate action.
    limits = await api_rows(client, "stk_limit", {"trade_date": day.strftime("%Y%m%d")})
    if len({row["ts_code"] for row in limits}) != len(limits) or any(
        row["trade_date"] != day.strftime("%Y%m%d") for row in limits
    ):
        raise ValueError("invalid current-session pre_close snapshot")
    previous_prices = {row["ts_code"][:6]: float(row["pre_close"]) for row in limits}
    valid = [
        code
        for code in market
        if code in previous_prices and isfinite(previous_prices[code]) and previous_prices[code] > 0
    ]
    down = sum(market[code]["close"] < previous_prices[code] for code in valid)
    inputs = {
        "schema": "v22-slim-inputs/v1",
        "checkpoint_as_of": seed["as_of"],
        "health_before": serialize_health_snapshot(deserialize_health_snapshot(health_before)),
        "risk_before": risk,
        "h90": h,
        "strong": strong,
        "index_closes": index_closes,
        "prior_market_amount": total(prior_rows),
        "earlier_market_amount": total(earlier_rows),
        "breadth_pre_close": {code: previous_prices[code] for code in valid},
    }
    snapshot = {
        **dict(bundle.snapshot),
        "v22_slim_inputs": inputs,
        "breadth_valid_n": len(valid),
        "breadth_down_n": down,
    }
    return (
        replace(
            bundle,
            snapshot=snapshot,
            snapshot_hash=sha256_json(snapshot),
            breadth_valid_n=len(valid),
            breadth_down_n=down,
        ),
        health,
        rolling,
        gaps,
    )
