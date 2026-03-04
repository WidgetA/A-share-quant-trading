#!/usr/bin/env python
"""
实验：turnover_amp 下限阈值精扫 (0.10 ~ 0.50, 步长 0.01)。

在粗扫结论 0.3x 基础上，以 ±0.2 范围、0.01 步长做精细参数扫描，
对比 EOD / Next-day 收益、胜率、综合得分，找出最优下限。

均量窗口: 5, 7, 15, 20 交易日
子集: 全市场 / 趋势下行 / 趋势上行

用法:
    uv run python scripts/experiment_turnover_amp_fine_sweep.py
"""

from __future__ import annotations

import logging
import pickle
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

# ---- Configuration ----
ANALYSIS_START = date(2025, 12, 1)
ANALYSIS_END = date(2026, 2, 28)

VOLUME_WINDOWS = [5, 7, 15, 20]
TREND_LOOKBACK_DAYS = 5
EARLY_FRACTION = 0.125

THRESHOLDS = np.arange(0.10, 0.505, 0.01)  # 0.10 ~ 0.50

CACHE_DIR = Path(__file__).resolve().parent.parent / "data"
CACHE_FILE = CACHE_DIR / "experiment_turnover_amp_cache.pkl"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def build_features(
    daily: dict[str, dict[str, dict]],
    minute: dict[str, dict[str, tuple[float, float]]],
    a_start: date,
    a_end: date,
    vol_windows: list[int],
) -> pd.DataFrame:
    rows: list[dict] = []
    max_lookback = max(max(vol_windows), TREND_LOOKBACK_DAYS + 1)

    for code, dates_data in daily.items():
        sorted_ds = sorted(dates_data.keys())
        n = len(sorted_ds)
        if n < max_lookback + 2:
            continue

        code_minute = minute.get(code, {})

        for idx in range(max_lookback, n - 1):
            ds = sorted_ds[idx]
            try:
                d = datetime.strptime(ds, "%Y-%m-%d").date()
            except ValueError:
                continue
            if d < a_start or d > a_end:
                continue

            today = dates_data[ds]

            prev_close = dates_data[sorted_ds[idx - 1]]["close"]
            close_n_ago = dates_data[sorted_ds[idx - TREND_LOOKBACK_DAYS - 1]]["close"]
            if close_n_ago <= 0 or prev_close <= 0:
                continue
            trend_pct = (prev_close - close_n_ago) / close_n_ago * 100

            avg_vols: dict[int, float] = {}
            skip = False
            for w in vol_windows:
                vols = []
                for j in range(max(0, idx - w), idx):
                    v = dates_data[sorted_ds[j]]["volume"]
                    if v > 0:
                        vols.append(v)
                if len(vols) < max(3, w // 2):
                    skip = True
                    break
                avg_vols[w] = sum(vols) / len(vols)
            if skip:
                continue

            mdata = code_minute.get(ds)
            if not mdata:
                continue
            price_940, early_vol = mdata
            if early_vol <= 0 or price_940 <= 0:
                continue

            amps: dict[str, float] = {}
            for w in vol_windows:
                expected = avg_vols[w] * EARLY_FRACTION
                if expected <= 0:
                    skip = True
                    break
                amps[f"amp_{w}d"] = early_vol / expected
            if skip:
                continue

            close_today = today["close"]
            open_today = today["open"]
            if close_today <= 0 or open_today <= 0:
                continue
            return_to_eod = close_today / price_940 - 1

            next_ds = sorted_ds[idx + 1]
            next_close = dates_data[next_ds]["close"]
            if next_close <= 0:
                continue
            return_to_next = next_close / price_940 - 1

            row = {
                "code": code,
                "date": ds,
                "trend_pct": trend_pct,
                "return_to_eod": return_to_eod,
                "return_to_next": return_to_next,
                **amps,
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    logger.info(
        f"Feature matrix: {len(df):,} obs, "
        f"{df['code'].nunique():,} stocks, "
        f"{df['date'].nunique()} days"
    )
    return df


def sweep_one(sdf: pd.DataFrame, amp_col: str) -> pd.DataFrame:
    """Fine sweep for one subset × one window. Returns DataFrame of results."""
    rows = []
    for t in THRESHOLDS:
        fl = sdf[sdf[amp_col] < t]
        kp = sdf[sdf[amp_col] >= t]
        if len(fl) < 20 or len(kp) < 20:
            continue

        n_fl = len(fl)
        pct_fl = n_fl / len(sdf) * 100

        avg_eod_fl = fl["return_to_eod"].mean() * 100
        avg_eod_kp = kp["return_to_eod"].mean() * 100
        lift_eod = avg_eod_kp - avg_eod_fl

        avg_nxt_fl = fl["return_to_next"].mean() * 100
        avg_nxt_kp = kp["return_to_next"].mean() * 100
        lift_nxt = avg_nxt_kp - avg_nxt_fl

        wr_eod_fl = (fl["return_to_eod"] > 0).mean() * 100
        wr_eod_kp = (kp["return_to_eod"] > 0).mean() * 100
        wr_nxt_fl = (fl["return_to_next"] > 0).mean() * 100
        wr_nxt_kp = (kp["return_to_next"] > 0).mean() * 100

        # 综合得分: 保留组绝对收益 (EOD + Next)
        # 核心逻辑: 过滤掉弱票后，剩下的票综合收益是否更好
        kept_return = avg_eod_kp + avg_nxt_kp
        # 辅助: 被过滤组收益越低越好 (说明过滤对了)
        filtered_return = avg_eod_fl + avg_nxt_fl

        rows.append(
            {
                "threshold": round(float(t), 2),
                "n_filtered": n_fl,
                "pct_filtered": pct_fl,
                "avg_eod_filtered": avg_eod_fl,
                "avg_eod_kept": avg_eod_kp,
                "lift_eod": lift_eod,
                "avg_nxt_filtered": avg_nxt_fl,
                "avg_nxt_kept": avg_nxt_kp,
                "lift_nxt": lift_nxt,
                "wr_eod_filtered": wr_eod_fl,
                "wr_eod_kept": wr_eod_kp,
                "wr_nxt_filtered": wr_nxt_fl,
                "wr_nxt_kept": wr_nxt_kp,
                "kept_return": kept_return,
                "filtered_return": filtered_return,
            }
        )

    return pd.DataFrame(rows)


def analyze(df: pd.DataFrame) -> str:
    out: list[str] = []

    def p(s: str = ""):
        out.append(s)

    p("=" * 90)
    p("  turnover_amp 下限精扫 (0.10 ~ 0.50, step=0.01)")
    p("=" * 90)
    p()
    p(f"  期间: {ANALYSIS_START} ~ {ANALYSIS_END}")
    p(f"  观测数: {len(df):,}  股票数: {df['code'].nunique():,}  交易日: {df['date'].nunique()}")
    p()

    subsets = {
        "全市场": df,
        "趋势下行": df[df["trend_pct"] < 0].copy(),
        "趋势上行": df[df["trend_pct"] >= 0].copy(),
    }

    # Store best results for final summary
    summary_rows: list[dict] = []

    for subset_name, sdf in subsets.items():
        if len(sdf) < 100:
            continue

        p("=" * 90)
        p(f"  子集: {subset_name}  (N={len(sdf):,})")
        p("=" * 90)
        p()

        for window in VOLUME_WINDOWS:
            amp_col = f"amp_{window}d"
            if amp_col not in sdf.columns:
                continue

            sweep_df = sweep_one(sdf, amp_col)
            if sweep_df.empty:
                continue

            p(f"  ---- 均量窗口 = {window}d ----")
            p()

            # 基线 (不过滤)
            base_eod = sdf["return_to_eod"].mean() * 100
            base_nxt = sdf["return_to_next"].mean() * 100
            base_wr_eod = (sdf["return_to_eod"] > 0).mean() * 100
            base_wr_nxt = (sdf["return_to_next"] > 0).mean() * 100

            p(
                f"  基线(不过滤): EOD={base_eod:+.3f}%  Nxt={base_nxt:+.3f}%  "
                f"wrEOD={base_wr_eod:.1f}%  wrNxt={base_wr_nxt:.1f}%"
            )
            p()

            # Header — 重点看保留组收益 + 被过滤组收益
            hdr = (
                f"  {'阈值':>5} {'过滤N':>7} {'过滤%':>6} "
                f"{'|保留EOD':>9} {'保留Nxt':>8} {'保留合计':>8} "
                f"{'wrE_k':>6} {'wrN_k':>6} "
                f"{'|过滤EOD':>9} {'过滤Nxt':>8} {'过滤合计':>8}"
            )
            p(hdr)
            p("  " + "-" * (len(hdr) - 2))

            for _, r in sweep_df.iterrows():
                p(
                    f"  {r['threshold']:>5.2f}x {r['n_filtered']:>7,.0f} {r['pct_filtered']:>5.1f}% "
                    f"|{r['avg_eod_kept']:>+7.3f}% {r['avg_nxt_kept']:>+7.3f}% {r['kept_return']:>+7.3f}% "
                    f"{r['wr_eod_kept']:>5.1f}% {r['wr_nxt_kept']:>5.1f}% "
                    f"|{r['avg_eod_filtered']:>+7.3f}% {r['avg_nxt_filtered']:>+7.3f}% {r['filtered_return']:>+7.3f}%"
                )

            # Best by kept_return (保留组综合收益最高)
            best_kept = sweep_df.loc[sweep_df["kept_return"].idxmax()]
            # Best by filtered_return being lowest (过滤掉的越差越好)
            best_filter_quality = sweep_df.loc[sweep_df["filtered_return"].idxmin()]

            p()
            p(
                f"  ★ 保留组收益最高: {best_kept['threshold']:.2f}x  "
                f"保留(EOD+Nxt)={best_kept['kept_return']:+.3f}%  "
                f"(EOD={best_kept['avg_eod_kept']:+.3f}% Nxt={best_kept['avg_nxt_kept']:+.3f}%)  "
                f"过滤{best_kept['pct_filtered']:.1f}%  "
                f"wrEOD={best_kept['wr_eod_kept']:.1f}% wrNxt={best_kept['wr_nxt_kept']:.1f}%"
            )
            p(
                f"    vs 基线: EOD {best_kept['avg_eod_kept'] - base_eod:+.3f}%  "
                f"Nxt {best_kept['avg_nxt_kept'] - base_nxt:+.3f}%"
            )
            p(
                f"  ◆ 过滤质量最好: {best_filter_quality['threshold']:.2f}x  "
                f"过滤组(EOD+Nxt)={best_filter_quality['filtered_return']:+.3f}%"
            )
            p()

            summary_rows.append(
                {
                    "subset": subset_name,
                    "window": window,
                    "best_t": best_kept["threshold"],
                    "kept_return": best_kept["kept_return"],
                    "kept_eod": best_kept["avg_eod_kept"],
                    "kept_nxt": best_kept["avg_nxt_kept"],
                    "pct_filtered": best_kept["pct_filtered"],
                    "wr_eod": best_kept["wr_eod_kept"],
                    "wr_nxt": best_kept["wr_nxt_kept"],
                    "base_return": base_eod + base_nxt,
                }
            )

    # Final summary table
    p()
    p("=" * 90)
    p("  最终汇总")
    p("=" * 90)
    p()

    sh = (
        f"  {'子集':<10} {'窗口':>4} {'最优阈值':>8} {'保留合计':>8} "
        f"{'保留EOD':>8} {'保留Nxt':>8} {'过滤%':>6} "
        f"{'wrEOD':>6} {'wrNxt':>6} {'基线合计':>8} {'提升':>7}"
    )
    p(sh)
    p("  " + "-" * (len(sh) - 2))

    for r in summary_rows:
        improve = r["kept_return"] - r["base_return"]
        p(
            f"  {r['subset']:<10} {r['window']:>4}d {r['best_t']:>7.2f}x "
            f"{r['kept_return']:>+7.3f}% "
            f"{r['kept_eod']:>+7.3f}% {r['kept_nxt']:>+7.3f}% "
            f"{r['pct_filtered']:>5.1f}% "
            f"{r['wr_eod']:>5.1f}% {r['wr_nxt']:>5.1f}% "
            f"{r['base_return']:>+7.3f}% {improve:>+6.3f}%"
        )

    # Cross-window consensus for 趋势下行
    p()
    p("  ---- 趋势下行: 各窗口最优阈值一致性 ----")
    dec_rows = [r for r in summary_rows if r["subset"] == "趋势下行"]
    if dec_rows:
        ts = [r["best_t"] for r in dec_rows]
        p(f"  各窗口: {[f'{t:.2f}x' for t in ts]}")
        p(f"  均值: {np.mean(ts):.2f}x  中位数: {np.median(ts):.2f}x  std: {np.std(ts):.2f}x")
        p(f"  范围: [{min(ts):.2f}x, {max(ts):.2f}x]")

    p()

    # Bootstrap for consensus threshold (趋势下行, 保留组收益)
    p("  ---- Bootstrap (N=3000) 趋势下行 — 保留组收益最高的阈值 ----")
    p()
    dec = df[df["trend_pct"] < 0].copy()
    np.random.seed(42)

    for window in VOLUME_WINDOWS:
        amp_col = f"amp_{window}d"
        boot: list[float] = []

        for _ in range(3000):
            samp = dec.sample(n=len(dec), replace=True)
            best_t, best_kr = 0.30, -np.inf
            for t in THRESHOLDS:
                kp = samp[samp[amp_col] >= t]
                if len(kp) < 50:
                    continue
                kr = kp["return_to_eod"].mean() + kp["return_to_next"].mean()
                if kr > best_kr:
                    best_kr = kr
                    best_t = float(t)
            boot.append(best_t)

        ba = np.array(boot)
        p(
            f"  {window}d:  mean={ba.mean():.3f}x  median={np.median(ba):.2f}x  "
            f"5th={np.percentile(ba, 5):.2f}x  25th={np.percentile(ba, 25):.2f}x  "
            f"75th={np.percentile(ba, 75):.2f}x  95th={np.percentile(ba, 95):.2f}x"
        )

        # Mini histogram
        hb = np.arange(0.095, 0.515, 0.01)
        counts, _ = np.histogram(ba, bins=hb)
        mx = max(counts) if max(counts) > 0 else 1
        top_bins = sorted(range(len(counts)), key=lambda i: -counts[i])[:8]
        top_bins.sort()
        p("    TOP分布:")
        for i in top_bins:
            if counts[i] > 0:
                bar = "█" * int(counts[i] / mx * 25)
                p(f"      {hb[i]:>5.2f}x: {bar} ({counts[i]})")
        p()

    p("=" * 90)
    return "\n".join(out)


def main() -> None:
    if not CACHE_FILE.exists():
        logger.error(
            f"Cache not found: {CACHE_FILE}\nRun experiment_turnover_amp.py first to download data."
        )
        sys.exit(1)

    logger.info(f"Loading cache: {CACHE_FILE}")
    with open(CACHE_FILE, "rb") as f:
        cache = pickle.load(f)  # noqa: S301
    daily = cache["daily"]
    minute = cache.get("minute", {})
    logger.info(f"Cache: {len(daily)} daily, {len(minute)} minute stocks")

    logger.info("Building features...")
    df = build_features(daily, minute, ANALYSIS_START, ANALYSIS_END, VOLUME_WINDOWS)

    if len(df) < 100:
        logger.error(f"Only {len(df)} observations — aborting")
        sys.exit(1)

    logger.info("Running fine sweep + bootstrap...")
    report = analyze(df)

    print()
    print(report)

    rpt = CACHE_DIR / "experiment_turnover_amp_fine_sweep_report.txt"
    with open(rpt, "w", encoding="utf-8") as f:
        f.write(report)
    logger.info(f"Report → {rpt}")


if __name__ == "__main__":
    main()
