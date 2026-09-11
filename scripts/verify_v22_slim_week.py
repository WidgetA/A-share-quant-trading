"""Offline migration acceptance against the already audited four-day week.

Research imports are confined to this exporter/acceptance script. Deployed
strategy modules consume neither these files nor any historical decision table.
"""

import argparse
import asyncio
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--research-workspace", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    from src.data.clients.tushare_realtime import TushareQuote
    from src.strategy.v22_slim.selection import build_stock, make_scanner

    research_dir = (
        args.research_workspace
        / "strategy-research/kangdie/explore/v22_ai_sector_avoidance_20260911"
    )
    sys.path.insert(0, str(research_dir))
    import strict_week_gates as evidence

    daily = evidence.Daily()
    calendar = tuple(date.fromisoformat(f"{d[:4]}-{d[4:6]}-{d[6:]}") for d in evidence.CAL)
    report = []
    for day in ("20260907", "20260908", "20260909", "20260910"):
        scanner, scorer, mapper = make_scanner()
        boards, universe = scanner.get_universe()
        market = evidence.market(day)
        history = daily.history(day)
        groups = {
            str(code): rows.sort_values("trade_date")
            for code, rows in history.groupby("stock_code")
        }
        stocks = {}
        for row in market.loc[
            market.input_available & market.stock_code.isin(universe)
        ].itertuples():
            hist = groups.get(row.stock_code)
            if hist is None:
                continue
            quote = TushareQuote(
                row.stock_code,
                row.open_price,
                row.close_price,
                row.high_price,
                row.low_price,
                row.volume_940,
                row.early_amount,
                row.close_price,
                row.high_price,
                row.low_price,
                row.volume_940,
                row.volume_937,
            )
            raw = {"time": [f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in hist.trade_date.astype(str)]}
            raw.update(
                {name: hist[name + "_price"].tolist() for name in ("open", "high", "low", "close")}
            )
            raw["volume"] = (hist.vol * 100).tolist()
            stock = build_stock(
                row.stock_code,
                mapper.names[row.stock_code],
                quote,
                raw,
                calendar,
                date(int(day[:4]), int(day[4:6]), int(day[6:])),
            )
            if stock is not None:
                stocks[row.stock_code] = stock
        result = await scanner.scan(stocks, boards)
        expected = pd.read_parquet(evidence.OUT / "gate_days" / day / "ranked_signals.parquet")
        actual_codes = [stock.code for stock in result.recommended]
        expected_codes = expected.head(10).code.tolist()
        if actual_codes != expected_codes:
            raise AssertionError({"day": day, "actual": actual_codes, "expected": expected_codes})
        score_errors = [
            abs(stock.score - float(row.score))
            for stock, row in zip(result.recommended, expected.head(10).itertuples(), strict=True)
        ]
        if score_errors and max(score_errors) > 1e-10:
            raise AssertionError({"day": day, "score_errors": score_errors})
        report.append(
            {
                "day": day,
                "top3": actual_codes[:3],
                "top10_match": True,
                "maximum_score_error": max(score_errors, default=0.0),
            }
        )
        print(json.dumps(report[-1]), flush=True)
    output = root / "reports/v22_slim"
    output.mkdir(parents=True, exist_ok=True)
    (output / "week_selection_acceptance.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    asyncio.run(main())
