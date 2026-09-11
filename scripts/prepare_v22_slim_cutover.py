"""Advance the accepted reference checkpoint through the completed cutover day.

This is an offline migration. It does not commit an official trading slot or
send a notification, and it never substitutes old V20 recommendations.
"""

import argparse
import asyncio
import gzip
import hashlib
import json
import sys
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--research-workspace", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    from src.data.clients.tushare_realtime import (
        TushareEarlyMarketData,
        TushareMinuteBar,
        TushareQuote,
    )
    from src.data.database.v20_repository import StateRecord, sha256_json
    from src.strategy.v20.artifacts import load_g_artifacts
    from src.strategy.v20.decision_engine import genesis_state, prepare_entry
    from src.strategy.v20.models import V20_V16_SNAPSHOT_SCHEMA
    from src.strategy.v20.runtime_config import load_v20_runtime_config
    from src.strategy.v22_slim.policy import close_risk_count
    from src.strategy.v22_slim.runtime_inputs import build_inputs, checkpoint
    from src.strategy.v22_slim.selection import (
        ASSET_ROOT,
        build_stock,
        exact_early,
        make_scanner,
        market_projection,
    )
    from src.web.v20_scan_pipeline import FrozenV16ScanBundle

    evidence_dir = (
        args.research_workspace
        / "strategy-research/kangdie/explore/v22_ai_sector_avoidance_20260911"
    )
    sys.path.insert(0, str(evidence_dir))
    import strict_week_gates as evidence

    folder = root / "reports/v22_slim/cutover"

    def read(name):
        return json.loads((folder / (name + ".json")).read_text(encoding="utf-8"))

    day = date(2026, 9, 11)
    seed = checkpoint()
    if seed["as_of"] != "2026-09-10":
        raise ValueError("cutover must start from the accepted Thursday checkpoint")
    calendar = tuple(
        sorted(
            date(int(r["cal_date"][:4]), int(r["cal_date"][4:6]), int(r["cal_date"][6:]))
            for r in read("trade_cal")
        )
    )
    raw = json.loads(gzip.decompress((folder / "early_raw.json.gz").read_bytes()))
    bars = {
        code: tuple(
            TushareMinuteBar(**{**bar, "bar_end": datetime.fromisoformat(bar["bar_end"])})
            for bar in rows
        )
        for code, rows in raw.items()
    }
    market = market_projection(bars, day)
    history = evidence.Daily().history("20260911")
    groups = {
        str(code): rows.sort_values("trade_date") for code, rows in history.groupby("stock_code")
    }
    scanner, scorer, mapper = make_scanner()
    boards, universe = scanner.get_universe()
    stocks = {}
    for code in sorted(universe.intersection(bars)):
        hist = groups.get(code)
        if hist is None:
            continue
        quote = exact_early(
            TushareEarlyMarketData(
                TushareQuote(code, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), bars[code], ""
            ),
            day,
        )
        if quote is None:
            continue
        hr = {"time": [f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in hist.trade_date.astype(str)]}
        hr.update(
            {name: hist[name + "_price"].tolist() for name in ("open", "high", "low", "close")}
        )
        hr["volume"] = (hist.vol * 100).tolist()
        stock = build_stock(code, mapper.names[code], quote.quote, hr, calendar, day)
        if stock is not None:
            stocks[code] = stock
    result = await scanner.scan(stocks, boards)
    snapshot = {
        "schema_version": V20_V16_SNAPSHOT_SCHEMA,
        "last_complete_bar": "09:39",
        "v22_market": market,
        "funnel": {
            "step0_universe_count": len(universe),
            "step2_hot_board_count": result.step2_hot_board_count,
            "final_candidates": result.final_candidates,
        },
        "board_avg_gains": result.step2_board_avg_gains,
        "symbols": [
            {
                "rank": stock.rank,
                "code": stock.code,
                "name": stock.name,
                "score": stock.score,
                "snapshot_price": stock.buy_price,
                "boards": result.stock_all_boards[stock.code],
                "best_board": result.stock_best_board[stock.code],
                "is_driver": result.stock_is_driver[stock.code],
                "cci": result.stock_cci.get(stock.code),
                "volume_937": stocks[stock.code].volume_937,
                "history_hash": sha256_json(groups[stock.code].trade_date.tolist()),
                "early_source_hash": sha256_json(raw[stock.code]),
            }
            for stock in result.recommended
        ],
    }

    class OfflineClient:
        async def _api_call(self, api, params, **kwargs):
            if api == "daily" and params["trade_date"] != "20260911":
                rows = pd.read_parquet(
                    evidence.OUT / "daily" / (params["trade_date"] + ".parquet")
                ).to_dict("records")
            else:
                rows = read(api)
            if api == "index_daily":
                rows = [
                    row
                    for row in rows
                    if params["start_date"] <= row["trade_date"] <= params["end_date"]
                ]
            fields = list(rows[0])
            return {
                "data": {
                    "fields": fields,
                    "items": [[row[field] for field in fields] for row in rows],
                }
            }

    config = replace(load_v20_runtime_config(root), strategy_version="V22-slim")
    service = SimpleNamespace(
        config=config, _scan_state=SimpleNamespace(realtime_client=OfflineClient())
    )
    prior = read("daily")  # Actual G uses Thursday amounts below, never Friday's.
    prior = pd.read_parquet(evidence.OUT / "daily/20260910.parquet").to_dict("records")
    bundle = FrozenV16ScanBundle(
        day,
        datetime.now(ZoneInfo("Asia/Shanghai")),
        result,
        stocks,
        tuple(sorted(universe)),
        0,
        0,
        date(2026, 9, 10),
        {row["ts_code"][:6]: row["amount"] * 1000 for row in prior},
        snapshot,
        sha256_json(snapshot),
        calendar,
    )
    bundle, health, rolling, gaps = await build_inputs(service, bundle, [], [], [])
    state = genesis_state()
    prepared = prepare_entry(
        config=config,
        state=StateRecord(config.state_lineage_id, 0, sha256_json(state), state),
        bundle=bundle,
        completed_health=health,
        completed_rolling=rolling,
        maturity_gaps=gaps,
        artifacts=load_g_artifacts(
            config.artifact_manifest_path.parent,
            expected_manifest_sha256=config.artifact_manifest_sha256,
        ),
        calendar=calendar,
    )
    semantic = prepared.commit.semantic
    refs = {}
    for code in universe.intersection(bars):
        eligible = [
            bar for bar in bars[code] if bar.end_label == "09:41" and bar.bar_end.date() == day
        ]
        if len(eligible) == 1 and eligible[0].open_price > 0:
            refs[code] = eligible[0].open_price
    closes = {row["ts_code"][:6]: row["close"] for row in read("daily")}
    codes = [stock.code for stock in result.recommended]
    if codes and not all(code in refs and code in closes for code in codes):
        raise ValueError("cutover D0 reference basket incomplete")
    d0 = (
        sum(closes[code] / refs[code] - 1 for code in codes) / len(codes) - 0.002 if codes else None
    )
    seed["as_of"] = day.isoformat()
    seed["health"] = semantic["health_after"]
    seed["risk_after"] = close_risk_count(
        semantic["risk_streak_before"],
        has_signal=bool(codes),
        normal_open=semantic["normal_open"],
        reference_return=d0,
    )
    seed["market_history"][day.isoformat()] = {code: row["amount"] for code, row in market.items()}
    seed["market_history"] = dict(sorted(seed["market_history"].items())[-20:])
    if codes:
        seed["batches"].append(
            {
                "day": day.isoformat(),
                "t2": calendar[calendar.index(day) + 2].isoformat(),
                "complete": False,
                "gross_return": None,
                "relative_return": None,
                "references": refs,
                "codes": codes,
            }
        )
    seed["source"] += (
        "; advanced with current-day rt_min_daily and official completed daily data through 2026-09-11"
    )
    content = gzip.compress(json.dumps(seed, ensure_ascii=False, allow_nan=False).encode(), mtime=0)
    path = ASSET_ROOT / "reference_checkpoint.json.gz"
    path.write_bytes(content)
    manifest_path = ASSET_ROOT / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["sha256"][path.name] = hashlib.sha256(content).hexdigest()
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    report = {
        "day": day.isoformat(),
        "top3": codes[:3],
        "full_list": codes,
        "action": prepared.action,
        "risk_before": semantic["risk_streak_before"],
        "risk_after": seed["risk_after"],
        "h90": semantic["h90"],
        "strong": semantic["strong_gate_hit"],
        "base": semantic["base_multiplier"],
        "rolling": semantic["rolling7_state"],
        "d0_reference_return": d0,
        "checkpoint_sha256": manifest["sha256"][path.name],
        "notifications_sent": 0,
    }
    (folder / "CUTOVER_AUDIT.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report))


if __name__ == "__main__":
    asyncio.run(main())
