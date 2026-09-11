"""Publish factual reference-state bootstrap; never publish an open-day table."""

import argparse
import gzip
import hashlib
import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd


def iso(value):
    text = str(value)
    return f"{text[:4]}-{text[4:6]}-{text[6:]}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--research-workspace", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    from src.strategy.v20.models import (
        HealthObservation,
        HealthSnapshot,
        HealthStatus,
        serialize_health_snapshot,
    )
    from src.strategy.v22_slim.selection import ASSET_ROOT, make_scanner

    source = (
        args.research_workspace
        / "strategy-research/kangdie/explore/v22_ai_sector_avoidance_20260911"
    )
    sys.path.insert(0, str(source))
    import strict_week_gates as evidence

    through = "20260910"
    state = (
        pd.read_parquet(evidence.OUT / "continued_gate_state.parquet")
        .set_index("trade_date")
        .loc[through]
    )
    papers = pd.concat(
        [
            pd.read_parquet(evidence.OLD / "period_gates/2026/paper_labels.parquet"),
            *[
                pd.read_parquet(path)
                for path in sorted((evidence.OUT / "gate_days").glob("*/paper_labels.parquet"))
            ],
        ]
    ).drop_duplicates(["trade_date", "slot_id"], keep="last")
    papers = papers.loc[papers.slot_id.eq("09:40")].set_index("trade_date")
    observations = []
    for day in state.health_last3_vintage.split("|"):
        row = papers.loc[day]
        observations.append(
            HealthObservation(
                "v22-bootstrap:" + iso(day),
                date.fromisoformat(iso(day)),
                date.fromisoformat(iso(row.exit_day)),
                float(row.relative_return),
            )
        )
    health = HealthSnapshot(
        HealthStatus.PAUSED_R0 if state.c3_paused else HealthStatus.HEALTHY,
        int(state.c3_confirmation_count),
        tuple(observations),
        observations[-1].order_key,
    )
    index = evidence.CAL.index(through)
    markets = {}
    for day in evidence.CAL[index - 19 : index + 1]:
        frame = (
            evidence.market(day)
            if day > "20260824"
            else evidence.s.read_market(
                evidence.FULL / "cache/market_asof/days" / (day + ".parquet")
            )
        )
        frame = frame.loc[
            frame.slot_id.eq("09:40")
            & frame.input_available
            & frame.stock_code.str.startswith(("00", "60"))
        ]
        markets[iso(day)] = dict(zip(frame.stock_code, frame.early_amount, strict=True))
    scanner, _, _ = make_scanner()
    _, universe = scanner.get_universe()
    batches = []
    for day, row in papers.loc[papers.index >= "20260825"].sort_index().iterrows():
        if not row.pick_n:
            continue
        record = {
            "day": iso(day),
            "t2": iso(row.exit_day),
            "complete": bool(row.complete),
            "gross_return": float(row.reference_return) if row.complete else None,
            "relative_return": float(row.relative_return)
            if np.isfinite(row.relative_return)
            else None,
        }
        # Unmatured baskets retain full valid comparison references, not just Top3.
        if row.exit_day > through:
            market = evidence.market(day)
            valid = (
                market.entry_bar_count.eq(1)
                & market.entry_raw_open.gt(0)
                & np.isfinite(market.entry_raw_open)
            )
            refs = market.loc[valid & market.stock_code.isin(universe)]
            record["references"] = dict(zip(refs.stock_code, refs.entry_raw_open, strict=True))
            ranks = pd.read_parquet(evidence.OUT / "gate_days" / day / "ranked_signals.parquet")
            record["codes"] = ranks.head(10).code.tolist()
        batches.append(record)
    checkpoint = {
        "schema": "v22-slim-reference-checkpoint/v1",
        "as_of": iso(through),
        "health": serialize_health_snapshot(health),
        "risk_after": int(state.risk_streak_after),
        "batches": batches,
        "market_history": markets,
        "source": "Frozen V22 audited reference-state continuation; quotes and labels through 2026-09-10",
    }
    content = gzip.compress(
        json.dumps(checkpoint, ensure_ascii=False, allow_nan=False).encode(), mtime=0
    )
    path = ASSET_ROOT / "reference_checkpoint.json.gz"
    path.write_bytes(content)
    manifest_path = ASSET_ROOT / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["sha256"][path.name] = hashlib.sha256(content).hexdigest()
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "as_of": checkpoint["as_of"],
                "risk_after": checkpoint["risk_after"],
                "market_days": len(markets),
                "batches": len(batches),
                "bytes": len(content),
            }
        )
    )


if __name__ == "__main__":
    main()
