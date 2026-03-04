#!/usr/bin/env python
"""
实验：turnover_amp 下限阈值 × 均量窗口 多维度交叉验证。

维度：
  - 均量窗口: 5, 7, 15, 20 日
  - 下限阈值: 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1
  - 子集: 全市场 / 趋势下行(trend<0) / 趋势上行(trend>=0)

数据源: 复用 experiment_turnover_amp_cache.pkl (tsanghi 日线 + baostock 5分钟线)

用法:
    uv run python scripts/experiment_turnover_amp_lower_bound.py
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

THRESHOLDS = np.arange(0.2, 1.15, 0.1)  # 0.2 ~ 1.1

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
    """Build feature matrix with turnover_amp computed for each volume window."""
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

            # --- trend_pct (5-day) ---
            prev_close = dates_data[sorted_ds[idx - 1]]["close"]
            close_n_ago = dates_data[sorted_ds[idx - TREND_LOOKBACK_DAYS - 1]]["close"]
            if close_n_ago <= 0 or prev_close <= 0:
                continue
            trend_pct = (prev_close - close_n_ago) / close_n_ago * 100

            # --- avg_daily_volume for each window ---
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

            # --- minute data ---
            mdata = code_minute.get(ds)
            if not mdata:
                continue
            price_940, early_vol = mdata
            if early_vol <= 0 or price_940 <= 0:
                continue

            # --- turnover_amp for each window ---
            amps: dict[str, float] = {}
            for w in vol_windows:
                expected = avg_vols[w] * EARLY_FRACTION
                if expected <= 0:
                    skip = True
                    break
                amps[f"amp_{w}d"] = early_vol / expected
            if skip:
                continue

            # --- returns ---
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
                "early_volume": early_vol,
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


def analyze(df: pd.DataFrame) -> str:
    out: list[str] = []

    def p(s: str = ""):
        out.append(s)

    p("=" * 80)
    p("  turnover_amp 下限阈值 × 均量窗口 交叉验证")
    p("=" * 80)
    p()
    p(f"  期间: {ANALYSIS_START} ~ {ANALYSIS_END}")
    p(f"  观测数: {len(df):,}  股票数: {df['code'].nunique():,}  交易日: {df['date'].nunique()}")
    p(f"  均量窗口: {VOLUME_WINDOWS}")
    p(f"  下限阈值: {[f'{t:.1f}' for t in THRESHOLDS]}")
    p()

    # Subsets
    subsets = {
        "全市场": df,
        "趋势下行(trend<0)": df[df["trend_pct"] < 0].copy(),
        "趋势上行(trend≥0)": df[df["trend_pct"] >= 0].copy(),
    }

    for subset_name, sdf in subsets.items():
        if len(sdf) < 100:
            continue

        p("=" * 80)
        p(f"  子集: {subset_name}  (N={len(sdf):,})")
        p("=" * 80)
        p()

        for window in VOLUME_WINDOWS:
            amp_col = f"amp_{window}d"
            if amp_col not in sdf.columns:
                continue

            p(f"  ---- 均量窗口 = {window} 日 ----")
            p(
                f"  {amp_col} 统计: mean={sdf[amp_col].mean():.2f}x  "
                f"median={sdf[amp_col].median():.2f}x  "
                f"std={sdf[amp_col].std():.2f}"
            )
            p()

            # --- Binning ---
            bins = [
                0,
                0.2,
                0.3,
                0.4,
                0.5,
                0.6,
                0.7,
                0.8,
                0.9,
                1.0,
                1.1,
                1.3,
                1.6,
                2.0,
                3.0,
                float("inf"),
            ]
            labels = [
                "<0.2",
                "0.2~0.3",
                "0.3~0.4",
                "0.4~0.5",
                "0.5~0.6",
                "0.6~0.7",
                "0.7~0.8",
                "0.8~0.9",
                "0.9~1.0",
                "1.0~1.1",
                "1.1~1.3",
                "1.3~1.6",
                "1.6~2.0",
                "2.0~3.0",
                "≥3.0",
            ]

            sdf[f"bin_{window}"] = pd.cut(
                sdf[amp_col],
                bins=bins,
                labels=labels,
                right=False,
            )
            hdr = f"  {'区间':<10} {'N':>7} {'avgEOD%':>9} {'avgNxt%':>9} {'wrEOD':>7} {'wrNxt':>7}"
            p(hdr)
            p("  " + "-" * (len(hdr) - 2))

            for lb in labels:
                s = sdf[sdf[f"bin_{window}"] == lb]
                if len(s) == 0:
                    continue
                ae = s["return_to_eod"].mean() * 100
                an = s["return_to_next"].mean() * 100
                we = (s["return_to_eod"] > 0).mean() * 100
                wn = (s["return_to_next"] > 0).mean() * 100
                p(f"  {lb:<10} {len(s):>7,} {ae:>+8.2f}% {an:>+8.2f}% {we:>6.1f}% {wn:>6.1f}%")
            p()

            # --- Parameter sweep (lower bound) ---
            p("  参数扫描: amp < threshold → 过滤")
            sh = (
                f"  {'阈值':>5} {'过滤N':>7} {'过滤%':>6} "
                f"{'过滤EOD':>8} {'保留EOD':>8} {'liftEOD':>8} "
                f"{'过滤Nxt':>8} {'保留Nxt':>8} {'liftNxt':>8} "
                f"{'wrEOD_f':>7} {'wrEOD_k':>7}"
            )
            p(sh)
            p("  " + "-" * (len(sh) - 2))

            best_lift_eod = -np.inf
            best_t_eod = 0.0
            best_lift_nxt = -np.inf
            best_t_nxt = 0.0

            for t in THRESHOLDS:
                fl = sdf[sdf[amp_col] < t]
                kp = sdf[sdf[amp_col] >= t]
                if len(fl) < 30 or len(kp) < 30:
                    continue
                nf = len(fl)
                pf = nf / len(sdf) * 100
                af_eod = fl["return_to_eod"].mean() * 100
                ak_eod = kp["return_to_eod"].mean() * 100
                lt_eod = ak_eod - af_eod
                af_nxt = fl["return_to_next"].mean() * 100
                ak_nxt = kp["return_to_next"].mean() * 100
                lt_nxt = ak_nxt - af_nxt
                wf = (fl["return_to_eod"] > 0).mean() * 100
                wk = (kp["return_to_eod"] > 0).mean() * 100

                if lt_eod > best_lift_eod:
                    best_lift_eod = lt_eod
                    best_t_eod = float(t)
                if lt_nxt > best_lift_nxt:
                    best_lift_nxt = lt_nxt
                    best_t_nxt = float(t)

                p(
                    f"  {t:>5.1f}x {nf:>7,} {pf:>5.1f}% "
                    f"{af_eod:>+7.2f}% {ak_eod:>+7.2f}% {lt_eod:>+7.3f}% "
                    f"{af_nxt:>+7.2f}% {ak_nxt:>+7.2f}% {lt_nxt:>+7.3f}% "
                    f"{wf:>6.1f}% {wk:>6.1f}%"
                )

            p()
            p(f"  ★ EOD最大lift: 阈值={best_t_eod:.1f}x  lift={best_lift_eod:+.3f}%")
            p(f"  ★ Nxt最大lift: 阈值={best_t_nxt:.1f}x  lift={best_lift_nxt:+.3f}%")
            p()

    # --- Cross-comparison summary ---
    p()
    p("=" * 80)
    p("  汇总: 各窗口最优下限阈值")
    p("=" * 80)
    p()

    summary_hdr = (
        f"  {'子集':<20} {'窗口':>4} {'bestEOD':>8} {'liftEOD':>9} "
        f"{'bestNxt':>8} {'liftNxt':>9} {'过滤%EOD':>8}"
    )
    p(summary_hdr)
    p("  " + "-" * (len(summary_hdr) - 2))

    for subset_name, sdf in subsets.items():
        if len(sdf) < 100:
            continue
        for window in VOLUME_WINDOWS:
            amp_col = f"amp_{window}d"
            if amp_col not in sdf.columns:
                continue

            best_t_eod, best_l_eod = 0.0, -np.inf
            best_t_nxt, best_l_nxt = 0.0, -np.inf
            best_pct = 0.0

            for t in THRESHOLDS:
                fl = sdf[sdf[amp_col] < t]
                kp = sdf[sdf[amp_col] >= t]
                if len(fl) < 30 or len(kp) < 30:
                    continue
                lt_eod = kp["return_to_eod"].mean() * 100 - fl["return_to_eod"].mean() * 100
                lt_nxt = kp["return_to_next"].mean() * 100 - fl["return_to_next"].mean() * 100
                if lt_eod > best_l_eod:
                    best_l_eod = lt_eod
                    best_t_eod = float(t)
                    best_pct = len(fl) / len(sdf) * 100
                if lt_nxt > best_l_nxt:
                    best_l_nxt = lt_nxt
                    best_t_nxt = float(t)

            p(
                f"  {subset_name:<20} {window:>4}d {best_t_eod:>7.1f}x {best_l_eod:>+8.3f}% "
                f"{best_t_nxt:>7.1f}x {best_l_nxt:>+8.3f}% {best_pct:>7.1f}%"
            )

    p()

    # --- Bootstrap for top 2 windows ---
    p("=" * 80)
    p("  Bootstrap (N=2000) — 趋势下行子集")
    p("=" * 80)
    p()

    dec = df[df["trend_pct"] < 0].copy()
    np.random.seed(42)

    for window in VOLUME_WINDOWS:
        amp_col = f"amp_{window}d"
        boot_eod: list[float] = []
        boot_nxt: list[float] = []

        for _ in range(2000):
            samp = dec.sample(n=len(dec), replace=True)
            # EOD
            best_t, best_l = 0.5, -np.inf
            for t in THRESHOLDS:
                fl = samp[samp[amp_col] < t]
                kp = samp[samp[amp_col] >= t]
                if len(fl) < 50 or len(kp) < 50:
                    continue
                lt = kp["return_to_eod"].mean() - fl["return_to_eod"].mean()
                if lt > best_l:
                    best_l = lt
                    best_t = float(t)
            boot_eod.append(best_t)

            # Next-day
            best_t, best_l = 0.5, -np.inf
            for t in THRESHOLDS:
                fl = samp[samp[amp_col] < t]
                kp = samp[samp[amp_col] >= t]
                if len(fl) < 50 or len(kp) < 50:
                    continue
                lt = kp["return_to_next"].mean() - fl["return_to_next"].mean()
                if lt > best_l:
                    best_l = lt
                    best_t = float(t)
            boot_nxt.append(best_t)

        be = np.array(boot_eod)
        bn = np.array(boot_nxt)

        p(f"  均量窗口 = {window}d:")
        p(
            f"    EOD:  mean={be.mean():.2f}x  median={np.median(be):.2f}x  "
            f"5th={np.percentile(be, 5):.2f}x  95th={np.percentile(be, 95):.2f}x"
        )
        p(
            f"    Next: mean={bn.mean():.2f}x  median={np.median(bn):.2f}x  "
            f"5th={np.percentile(bn, 5):.2f}x  95th={np.percentile(bn, 95):.2f}x"
        )

        # Compact histogram
        hb = np.arange(0.15, 1.25, 0.1)
        counts_e, _ = np.histogram(be, bins=hb)
        counts_n, _ = np.histogram(bn, bins=hb)
        mx_e = max(counts_e) if max(counts_e) > 0 else 1
        mx_n = max(counts_n) if max(counts_n) > 0 else 1
        p("    EOD分布:")
        for i in range(len(hb) - 1):
            if counts_e[i] > 0:
                bar = "█" * int(counts_e[i] / mx_e * 30)
                p(f"      {hb[i]:>4.1f}x: {bar} ({counts_e[i]})")
        p("    Next分布:")
        for i in range(len(hb) - 1):
            if counts_n[i] > 0:
                bar = "█" * int(counts_n[i] / mx_n * 30)
                p(f"      {hb[i]:>4.1f}x: {bar} ({counts_n[i]})")
        p()

    p("=" * 80)
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

    logger.info("Building features (multi-window)...")
    df = build_features(daily, minute, ANALYSIS_START, ANALYSIS_END, VOLUME_WINDOWS)

    if len(df) < 100:
        logger.error(f"Only {len(df)} observations — aborting")
        sys.exit(1)

    logger.info("Running analysis...")
    report = analyze(df)

    print()
    print(report)

    rpt = CACHE_DIR / "experiment_turnover_amp_lower_bound_report.txt"
    with open(rpt, "w", encoding="utf-8") as f:
        f.write(report)
    logger.info(f"Report → {rpt}")


if __name__ == "__main__":
    main()
