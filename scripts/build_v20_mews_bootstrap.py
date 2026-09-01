"""Build the compact V20 MEWS incremental state from the frozen SQLite history."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import sqlite3
from collections import defaultdict
from pathlib import Path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build(source: Path, output: Path) -> None:
    connection = sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    dates = [
        str(row["trade_date"])
        for row in connection.execute(
            "SELECT trade_date FROM metric_margin_systemic_risk_v2_daily "
            "ORDER BY trade_date DESC LIMIT 550"
        )
    ][::-1]
    if len(dates) != 550:
        raise RuntimeError("MEWS bootstrap requires exactly 550 historical trading days")
    state_date = dates[-1]
    market_fields = (
        "trade_date",
        "market_total_margin_balance",
        "market_total_financing_buy_amount",
        "market_total_financing_repayment_amount",
        "ordinary_a_share_margin_balance",
        "ordinary_a_share_financing_buy_amount",
        "ordinary_a_share_financing_repayment_amount",
        "ordinary_a_share_margin_coverage",
        "ffmv_stock",
        "nib_breadth_v2",
        "nib_magnitude_v2",
        "deleveraging_breadth",
        "data_status",
        "mews_v2_score",
        "exhaustion_path",
        "persistent_deleveraging_path",
        "net_outflow_level_score",
        "risk_state_v2",
    )
    placeholders = ",".join("?" for _ in dates)
    market = [
        dict(row)
        for row in connection.execute(
            f"SELECT {','.join(market_fields)} FROM metric_margin_systemic_risk_v2_daily "
            f"WHERE trade_date IN ({placeholders}) ORDER BY trade_date",
            dates,
        )
    ]
    recent_dates = dates[-120:]
    rows_by_code: dict[str, dict[str, dict]] = defaultdict(dict)
    for row in connection.execute(
        "SELECT f.trade_date,f.ts_code,f.financing_balance,f.financing_buy_amount,"
        "f.financing_repayment_amount,x.flow_rate_ema_5,x.flow_rate_ema_20,x.impulse_raw "
        "FROM fact_margin_security_daily AS f LEFT JOIN feature_margin_security_daily AS x "
        "ON x.trade_date=f.trade_date AND x.ts_code=f.ts_code AND x.index_version='mews_v1' "
        "WHERE f.trade_date>=? AND f.trade_date<=? ORDER BY f.ts_code,f.trade_date",
        (recent_dates[0], recent_dates[-1]),
    ):
        rows_by_code[str(row["ts_code"])][str(row["trade_date"])] = dict(row)

    states: dict[str, dict] = {}
    for code, by_day in rows_by_code.items():
        current = by_day.get(state_date)
        valid_history = []
        net_flow_history = []
        for day in recent_dates[-25:]:
            row = by_day.get(day)
            valid = bool(
                row
                and row["financing_balance"] is not None
                and row["financing_buy_amount"] is not None
                and row["financing_repayment_amount"] is not None
            )
            valid_history.append(valid)
        for day in recent_dates[-5:]:
            row = by_day.get(day)
            if (
                row
                and row["financing_buy_amount"] is not None
                and row["financing_repayment_amount"] is not None
            ):
                net_flow_history.append(
                    float(row["financing_buy_amount"]) - float(row["financing_repayment_amount"])
                )
            else:
                net_flow_history.append(None)
        impulses = [
            float(by_day[day]["impulse_raw"])
            for day in recent_dates
            if day in by_day and by_day[day]["impulse_raw"] is not None
        ][-60:]

        def ema_state(field: str, span: int) -> tuple[float | None, float]:
            last_day = next(
                (
                    day
                    for day in reversed(recent_dates)
                    if day in by_day and by_day[day][field] is not None
                ),
                None,
            )
            if last_day is None:
                return None, 1.0
            gap = len(recent_dates) - 1 - recent_dates.index(last_day)
            return float(by_day[last_day][field]), math.pow(1.0 - 2.0 / (span + 1.0), gap)

        fast, fast_weight = ema_state("flow_rate_ema_5", 5)
        slow, slow_weight = ema_state("flow_rate_ema_20", 20)
        states[code] = {
            "current_balance": float(current["financing_balance"]) if current else None,
            "ema_fast_state": fast,
            "ema_fast_old_weight": fast_weight,
            "ema_slow_state": slow,
            "ema_slow_old_weight": slow_weight,
            "valid_history": valid_history,
            "net_flow_history": net_flow_history,
            "impulse_history": impulses,
        }

    clear_threshold = 49.5389677189997
    clear_streak = 0
    for row in reversed(market):
        if (
            row["data_status"] == "OK"
            and row["mews_v2_score"] is not None
            and float(row["mews_v2_score"]) < clear_threshold
        ):
            clear_streak += 1
        else:
            break
    payload = {
        "schema": "v20-mews-incremental-state/v1",
        "model_version": "mews_v2",
        "state_date": state_date,
        "source_sqlite_sha256": _sha256(source),
        "market_history": market,
        "security_states": states,
        "risk_state": str(market[-1]["risk_state_v2"]),
        "clear_streak": clear_streak,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    output.write_bytes(gzip.compress(encoded.encode("utf-8"), compresslevel=9, mtime=0))
    connection.close()
    print(f"wrote {output} state_date={state_date} securities={len(states)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    build(args.source.resolve(), args.output.resolve())


if __name__ == "__main__":
    main()
