"""Offline four-day policy acceptance, using prior state and causal raw facts.

The ranking parity is independently checked by verify_v22_slim_week.py. This
script consumes that full list, never the expected opening decisions as input.
No production database writes, notifications, or network requests are made.
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd


def dt(value):
    return datetime.strptime(str(value), "%Y%m%d").date()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--research-workspace", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    sys.path.insert(
        0,
        str(
            args.research_workspace
            / "strategy-research/kangdie/explore/v22_ai_sector_avoidance_20260911"
        ),
    )
    import strict_week_gates as e

    from src.data.database.v20_repository import StateRecord, sha256_json
    from src.strategy.v20.artifacts import load_g_artifacts
    from src.strategy.v20.decision_engine import (
        CompletedHealth,
        CompletedRolling,
        genesis_state,
        prepare_entry,
    )
    from src.strategy.v20.models import (
        V20_V16_SNAPSHOT_SCHEMA,
        HealthObservation,
        HealthSnapshot,
        HealthStatus,
        serialize_health_snapshot,
    )
    from src.strategy.v20.runtime_config import load_v20_runtime_config
    from src.strategy.v20.selection_scanner import V16ScanResult
    from src.strategy.v20.selection_scorer import ScoredStock
    from src.strategy.v22_slim.policy import close_risk_count
    from src.strategy.v22_slim.runtime_inputs import build_inputs
    from src.strategy.v22_slim.selection import make_scanner
    from src.web.v20_scan_pipeline import FrozenV16ScanBundle

    expected = pd.read_parquet(e.OUT / "continued_gate_state.parquet").set_index("trade_date")
    papers = pd.concat(
        [
            pd.read_parquet(e.OLD / "period_gates/2026/paper_labels.parquet"),
            *[
                pd.read_parquet(p)
                for p in sorted((e.OUT / "gate_days").glob("*/paper_labels.parquet"))
            ],
        ]
    ).drop_duplicates(["trade_date", "slot_id"], keep="last")
    papers = papers.loc[papers.slot_id.eq("09:40")].set_index("trade_date")
    anchor = "20260904"
    baseline = expected.loc[anchor]
    scanner, _, _ = make_scanner()
    _, universe = scanner.get_universe()
    observations = tuple(
        HealthObservation(
            "v22-bootstrap:" + dt(d).isoformat(),
            dt(d),
            dt(papers.loc[d].exit_day),
            float(papers.loc[d].relative_return),
        )
        for d in baseline.health_last3_vintage.split("|")
    )
    health = serialize_health_snapshot(
        HealthSnapshot(
            HealthStatus.PAUSED_R0,
            0,
            observations,
            observations[-1].order_key,
        )
    )
    seed = {
        "as_of": dt(anchor).isoformat(),
        "health": health,
        "risk_after": int(baseline.risk_streak_after),
        "market_history": {},
        "batches": [],
    }
    for day in e.CAL[e.CAL.index(anchor) - 19 : e.CAL.index(anchor) + 1]:
        market = (
            e.market(day)
            if day > "20260824"
            else e.s.read_market(e.FULL / "cache/market_asof/days" / (day + ".parquet"))
        )
        valid = market.loc[
            market.slot_id.eq("09:40")
            & market.input_available
            & market.stock_code.str.startswith(("00", "60"))
        ]
        seed["market_history"][dt(day).isoformat()] = dict(
            zip(valid.stock_code, valid.early_amount)
        )
    for day, row in papers.loc[(papers.index >= "20260825") & (papers.index <= anchor)].iterrows():
        if not row.pick_n:
            continue
        complete = row.exit_day <= anchor and bool(row.complete)
        item = {
            "day": dt(day).isoformat(),
            "t2": dt(row.exit_day).isoformat(),
            "complete": complete,
            "gross_return": float(row.reference_return) if complete else None,
            "relative_return": float(row.relative_return) if complete else None,
        }
        if not complete:
            m = e.market(day)
            m = m.loc[
                m.stock_code.isin(universe) & m.entry_bar_count.eq(1) & m.entry_raw_open.gt(0)
            ]
            item["references"] = dict(zip(m.stock_code, m.entry_raw_open))
            ranks = pd.read_parquet(e.OUT / "gate_days" / day / "ranked_signals.parquet")
            item["codes"] = ranks.head(10).code.tolist()
        seed["batches"].append(item)

    statuses = {}
    calendar = tuple(dt(d) for d in e.CAL)
    config = load_v20_runtime_config(root, root / "config/v22-slim.yaml")
    artifacts = load_g_artifacts(
        config.artifact_manifest_path.parent,
        expected_manifest_sha256=config.artifact_manifest_sha256,
    )
    payload = genesis_state()
    state = StateRecord(config.state_lineage_id, 0, sha256_json(payload), payload)
    report = []
    current_day = dt("20260907")

    class Client:
        async def _api_call(self, api, params, **kwargs):
            if api in ("daily", "stk_limit"):
                target = params["trade_date"]
                assert dt(target) < current_day if api == "daily" else dt(target) == current_day
                rows = pd.read_parquet(e.OUT / "daily" / (target + ".parquet")).to_dict("records")
            else:
                assert api == "index_daily" and dt(params["end_date"]) < current_day
                rows = pd.read_parquet(e.OUT / "csi2000.parquet")
                rows = rows.loc[
                    rows.trade_date.between(params["start_date"], params["end_date"])
                ].to_dict("records")
            fields = list(rows[0])
            return {"data": {"fields": fields, "items": [[r[f] for f in fields] for r in rows]}}

        async def batch_get_minute_history_for_date(self, codes, day):
            assert day < current_day
            m = e.market(day.strftime("%Y%m%d")).set_index("stock_code")
            return {
                code: [
                    SimpleNamespace(
                        end_label="09:41",
                        bar_end=datetime.combine(day, datetime.min.time()),
                        open_price=float(m.loc[code].entry_raw_open),
                    )
                ]
                for code in codes
                if m.loc[code].entry_bar_count == 1
            }

    class Repo:
        async def get_entry_status(self, stream, day):
            return statuses.get(day)

    for day in ("20260907", "20260908", "20260909", "20260910"):
        current_day = dt(day)
        ranks = pd.read_parquet(e.OUT / "gate_days" / day / "ranked_signals.parquet").head(10)
        stocks = [ScoredStock(r.code, r.code, r.score, r.rank, 20.0) for r in ranks.itertuples()]
        # Exact label sets from the frozen rank output are inputs to G.
        best = {r.code: r.best_board for r in ranks.itertuples()}
        routes = {r.code: str(r.hot_route_boards_raw).split(" || ") for r in ranks.itertuples()}
        scan = V16ScanResult(
            recommended=stocks,
            final_candidates=len(stocks),
            stock_best_board=best,
            stock_all_boards=routes,
        )
        m = e.market(day)
        m = m.loc[m.input_available & m.stock_code.str.startswith(("00", "60"))]
        market = {
            r.stock_code: {"close": r.close_price, "amount": r.early_amount} for r in m.itertuples()
        }
        symbols = [
            {
                "rank": s.rank,
                "code": s.code,
                "name": s.name,
                "score": s.score,
                "snapshot_price": s.buy_price,
                "boards": routes[s.code],
                "best_board": best[s.code],
                "is_driver": True,
                "cci": None,
                "volume_937": None,
                "history_hash": "a" * 64,
                "early_source_hash": "b" * 64,
            }
            for s in stocks
        ]
        snapshot = {
            "schema_version": V20_V16_SNAPSHOT_SCHEMA,
            "trade_date": current_day.isoformat(),
            "last_complete_bar": "09:39",
            "symbols": symbols,
            "v22_market": market,
            "funnel": {
                "step0_universe_count": len(universe),
                "step2_hot_board_count": 1,
                "final_candidates": len(stocks),
            },
            "board_avg_gains": {board: 1.0 for route in routes.values() for board in route},
        }
        prior_day = calendar[calendar.index(current_day) - 1]
        prior = pd.read_parquet(e.OUT / "daily" / (prior_day.strftime("%Y%m%d") + ".parquet"))
        bundle = FrozenV16ScanBundle(
            current_day,
            datetime.now(ZoneInfo("Asia/Shanghai")),
            scan,
            {},
            tuple(sorted(universe)),
            0,
            0,
            prior_day,
            dict(zip(prior.ts_code.str[:6], prior.amount * 1000)),
            snapshot,
            sha256_json(snapshot),
            calendar,
        )
        completed_h, completed_r = [], []
        for d, row in papers.loc[(papers.index > anchor) & (papers.index < day)].iterrows():
            if row.exit_day < day and row.complete and row.pick_n:
                identity = "entry:" + d
                completed_r.append(
                    CompletedRolling(identity, dt(d), dt(row.exit_day), float(row.reference_return))
                )
                if pd.notna(row.relative_return):
                    completed_h.append(
                        CompletedHealth(
                            identity, dt(d), dt(row.exit_day), float(row.relative_return), True
                        )
                    )
        # Fresh service each day exercises persisted continuation after a restart.
        service = SimpleNamespace(
            config=config,
            _scan_state=SimpleNamespace(realtime_client=Client()),
            _repository=Repo(),
            _verify_entry_binding=lambda status: None,
        )
        with patch("src.strategy.v22_slim.runtime_inputs.checkpoint", lambda: seed):
            bundle, completed_h, completed_r, gaps = await build_inputs(
                service, bundle, completed_h, completed_r, []
            )
        prepared = prepare_entry(
            config=config,
            state=state,
            bundle=bundle,
            completed_health=completed_h,
            completed_rolling=completed_r,
            maturity_gaps=gaps,
            artifacts=artifacts,
            calendar=calendar,
        )
        actual = prepared.commit.semantic
        target = expected.loc[day]
        checks = {
            "base_multiplier": "base_weight",
            "defense_multiplier": "defense_multiplier",
            "normal_open": "normal_open",
            "risk_streak_before": "risk_streak_before",
            "strong_gate_hit": "strong_gate_hit",
            "rolling7_state": "rolling7_state",
            "breadth_valid_n": "wilson_valid_n",
            "breadth_down_n": "wilson_down_n",
        }
        for field, target_field in checks.items():
            assert actual[field] == target[target_field], (
                day,
                field,
                actual[field],
                target[target_field],
            )
        assert (prepared.action == "ENTER") == bool(target.final_open)
        assert abs(actual["rolling7_r7"] - target.rolling7_sum) < 1e-10
        for horizon in (10, 20):
            key = f"h{horizon}_ratio"
            assert abs(actual["h90"][key] - target[key]) < 1e-10
        row = papers.loc[day]
        risk_after = close_risk_count(
            actual["risk_streak_before"],
            has_signal=bool(stocks),
            normal_open=actual["normal_open"],
            reference_return=float(row.d0_reference_return) if row.d0_complete else None,
        )
        assert risk_after == target.risk_streak_after
        statuses[current_day] = SimpleNamespace(
            strategy_version="V22-slim", semantic=actual, snapshot=prepared.commit.snapshot
        )
        state = StateRecord(
            config.state_lineage_id,
            state.revision + 1,
            prepared.commit.next_state_hash,
            prepared.commit.next_state,
        )
        report.append(
            {
                "day": day,
                "action": prepared.action,
                "all_gate_inputs_match": True,
                "risk_before": actual["risk_streak_before"],
                "risk_after": risk_after,
            }
        )
        print(json.dumps(report[-1]), flush=True)
    (root / "reports/v22_slim/week_gate_acceptance.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    asyncio.run(main())
