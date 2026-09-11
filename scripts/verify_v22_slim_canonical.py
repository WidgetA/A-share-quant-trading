"""Exercise the actual canonical adapter with captured cutover facts, offline."""

import argparse
import asyncio
import gzip
import json
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd


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

    from src.data.clients.tushare_realtime import (
        TushareDailyBar,
        TushareMinuteBar,
        tushare_minute_bars_to_early_market_data,
    )
    from src.strategy.v20.runtime_config import load_v20_runtime_config
    from src.strategy.v22_slim.selection import make_scanner
    from src.web.v20_canonical_selection import (
        V20CanonicalSelectionState,
        compute_canonical_v16_scan,
    )
    from src.web.v20_service import V20Service

    folder = root / "reports/v22_slim/cutover"
    day = date(2026, 9, 11)
    raw = json.loads(gzip.decompress((folder / "early_raw.json.gz").read_bytes()))
    early = {}
    for code, rows in raw.items():
        bars = tuple(
            TushareMinuteBar(**{**r, "bar_end": datetime.fromisoformat(r["bar_end"])})
            for r in rows
            if r["end_label"] <= "09:39"
        )
        value = tushare_minute_bars_to_early_market_data(code, bars, day)
        if value is not None:
            early[code] = value
    scanner, _, mapper = make_scanner()
    boards, universe = scanner.get_universe()
    calendar = tuple(
        sorted(
            datetime.strptime(r["cal_date"], "%Y%m%d").date()
            for r in json.loads((folder / "trade_cal.json").read_text(encoding="utf-8"))
        )
    )
    prior = pd.read_parquet(e.OUT / "daily/20260910.parquet")
    daily_rows = {
        r.ts_code[:6]: TushareDailyBar(r.ts_code[:6], "20260910", r.close, r.amount * 1000)
        for r in prior.itertuples()
        if r.close > 0 and r.amount > 0
    }
    histories = {}
    history = e.Daily().history("20260911")
    for code, group in history.groupby("stock_code"):
        group = group.sort_values("trade_date")
        hr = {
            "time": [
                datetime.strptime(str(d), "%Y%m%d").date().isoformat() for d in group.trade_date
            ]
        }
        hr.update(
            {name: group[name + "_price"].tolist() for name in ("open", "high", "low", "close")}
        )
        hr["volume"] = (group.vol * 100).tolist()
        histories[str(code)] = hr
    state = V20CanonicalSelectionState(initialized=True, selection_version="V22-slim")
    kwargs = dict(
        universe_override=tuple(sorted(universe)),
        clean_boards_override=boards,
        prev_closes_override={code: r.close_price for code, r in daily_rows.items()},
        history_raw_override=histories,
        names_override=mapper.names,
        calendar_override=calendar,
        prior_daily_override=daily_rows,
        st_eligible_codes_override=tuple(universe),
        early_data_seed=early,
        allow_realtime_fetch=False,
    )
    first = await compute_canonical_v16_scan(state, day, **kwargs)
    second = await compute_canonical_v16_scan(
        V20CanonicalSelectionState(initialized=True, selection_version="V22-slim"), day, **kwargs
    )
    codes = [s.code for s in first.scan_result.recommended]
    expected = json.loads((folder / "CUTOVER_AUDIT.json").read_text(encoding="utf-8"))
    assert codes == expected["full_list"], (codes, expected["full_list"])
    assert [(s.code, s.score) for s in first.scan_result.recommended] == [
        (s.code, s.score) for s in second.scan_result.recommended
    ]
    service = object.__new__(V20Service)
    service.config = load_v20_runtime_config(root, root / "config/v22-slim.yaml")
    projected = service._project_canonical_v16(first, calendar=calendar)
    assert len(projected.snapshot["v22_market"]) >= 1000
    assert (
        projected.snapshot_hash
        == service._project_canonical_v16(second, calendar=calendar).snapshot_hash
    )
    report = {
        "day": day.isoformat(),
        "top10_match": True,
        "fresh_restart_match": True,
        "whole_market_count": len(projected.snapshot["v22_market"]),
        "top3": codes[:3],
        "model_sha256": first.model_sha256,
        "notifications_sent": 0,
    }
    (root / "reports/v22_slim/canonical_acceptance.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report))


if __name__ == "__main__":
    asyncio.run(main())
