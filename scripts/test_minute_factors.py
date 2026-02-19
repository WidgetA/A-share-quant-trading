"""Test intraday minute-level factors for predicting close < open.

Uses akshare 5-minute bars (9:35 + 9:40) to construct early-session factors.
Tests classification accuracy: can 9:30-9:40 patterns predict 日内亏钱 (close < open)?

Factors (all computed from first two 5-min bars of the day):
1. Early Fade: (peak - close_9:40) / (peak - open) — how much initial push faded
2. Volume Decay: vol_9:40 / vol_9:35 — is buying pressure dying?
3. Gain Erosion: how much of opening gap given back by 9:40
4. Price Position: (close_9:40 - low) / (high - low) in first 10 minutes
5. Bar Pattern: are the first two bars red candles?
"""

import sys
import time
import warnings

import akshare as ak
import numpy as np
import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")
warnings.filterwarnings("ignore")

# === CONFIG ===
N_STOCKS = 60
MIN_OPEN_GAP_PCT = 1.0  # Only analyze days where stock gaps up >= 1%
OPEN_LIMIT_UP_PCT = 9.5  # Exclude opening limit-up
SLEEP = 0.5  # seconds between API calls
DAILY_START = "20251201"  # daily data start (for prev_close)
DAILY_END = "20260218"
MIN_START = "2025-12-01 09:30:00"
MIN_END = "2026-02-18 15:00:00"


def main():
    # ── Step 1: Sample liquid stocks ──
    print("=" * 60)
    print("Step 1: Getting liquid stock sample")
    print("=" * 60)

    spot = ak.stock_zh_a_spot_em()
    spot = spot[~spot["名称"].str.contains("ST|退", na=False)]
    spot = spot[~spot["代码"].str.startswith(("68", "4", "8", "9"))]
    spot = spot[spot["成交额"] > 1e8]
    spot = spot.sort_values("成交额", ascending=False)

    codes = spot.head(300)["代码"].sample(n=N_STOCKS, random_state=42).tolist()
    print(f"Sampled {len(codes)} stocks from top 300 by trading volume")

    # ── Step 2: Fetch daily + 5-min data ──
    print(f"\n{'=' * 60}")
    print("Step 2: Fetching daily + 5-min data")
    print(f"{'=' * 60}")

    all_records = []
    fetch_ok = 0
    fetch_fail = 0

    for i, code in enumerate(codes):
        pct = (i + 1) / len(codes) * 100
        print(f"\r  [{i + 1}/{len(codes)}] {pct:.0f}% Fetching {code}...", end="", flush=True)

        # ── Daily data ──
        try:
            daily = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=DAILY_START,
                end_date=DAILY_END,
                adjust="qfq",
            )
        except Exception:
            fetch_fail += 1
            continue

        if daily is None or len(daily) < 5:
            fetch_fail += 1
            continue

        time.sleep(SLEEP)

        # ── 5-min data ──
        try:
            minute = ak.stock_zh_a_hist_min_em(
                symbol=code,
                start_date=MIN_START,
                end_date=MIN_END,
                period="5",
                adjust="",
            )
        except Exception:
            fetch_fail += 1
            continue

        if minute is None or len(minute) < 10:
            fetch_fail += 1
            continue

        time.sleep(SLEEP)
        fetch_ok += 1

        # ── Process ──
        daily = daily.copy()
        daily["日期"] = pd.to_datetime(daily["日期"])
        daily["date"] = daily["日期"].dt.date
        daily["prev_close"] = daily["收盘"].shift(1)
        daily["open_gap_pct"] = (daily["开盘"] - daily["prev_close"]) / daily["prev_close"] * 100

        minute = minute.copy()
        minute["时间"] = pd.to_datetime(minute["时间"])
        minute["date"] = minute["时间"].dt.date
        minute["time_str"] = minute["时间"].dt.strftime("%H:%M")

        # Build lookup: date → first two 5-min bars (9:35, 9:40)
        min_by_date = {}
        for d, grp in minute.groupby("date"):
            morning = grp[grp["time_str"].isin(["09:35", "09:40"])].sort_values("时间")
            if len(morning) >= 2:
                min_by_date[d] = morning.head(2)

        # Filter high-open days
        high_open = daily[
            (daily["open_gap_pct"] >= MIN_OPEN_GAP_PCT)
            & (daily["open_gap_pct"] < OPEN_LIMIT_UP_PCT)
            & (daily["prev_close"].notna())
        ]

        for _, day_row in high_open.iterrows():
            trade_date = day_row["date"]
            if trade_date not in min_by_date:
                continue

            bars = min_by_date[trade_date]
            bar1 = bars.iloc[0]  # 9:35 bar (covers 9:30-9:35)
            bar2 = bars.iloc[1]  # 9:40 bar (covers 9:35-9:40)

            day_open = day_row["开盘"]
            day_close = day_row["收盘"]
            prev_close = day_row["prev_close"]

            # 10-min aggregates
            max_high = max(bar1["最高"], bar2["最高"])
            min_low = min(bar1["最低"], bar2["最低"])
            close_940 = bar2["收盘"]
            vol_1 = bar1["成交量"]
            vol_2 = bar2["成交量"]

            # ── Factor 1: Early Fade ──
            if max_high > day_open:
                early_fade = (max_high - close_940) / (max_high - day_open + 1e-6)
                early_fade = max(0, early_fade)  # clamp negative (still rising)
            else:
                early_fade = 0.0

            # ── Factor 2: Volume Decay ──
            if vol_1 > 0:
                vol_decay = vol_2 / vol_1
            else:
                vol_decay = 1.0

            # ── Factor 3: Gain Erosion ──
            gap_at_open = (day_open - prev_close) / prev_close * 100
            gap_at_940 = (close_940 - prev_close) / prev_close * 100
            if gap_at_open > 0:
                erosion = 1.0 - gap_at_940 / gap_at_open
            else:
                erosion = 0.0

            # ── Factor 4: Price Position ──
            range_10 = max_high - min_low
            if range_10 > 0:
                price_pos = (close_940 - min_low) / range_10
            else:
                price_pos = 0.5

            # ── Factor 5: Bar Pattern ──
            # How many of the 2 bars are red (close < open)?
            red_count = int(bar1["收盘"] < bar1["开盘"]) + int(bar2["收盘"] < bar2["开盘"])

            # ── Label ──
            is_loss = 1 if day_close < day_open else 0

            all_records.append(
                {
                    "code": code,
                    "date": trade_date,
                    "open_gap_pct": day_row["open_gap_pct"],
                    "early_fade": early_fade,
                    "vol_decay": vol_decay,
                    "erosion": erosion,
                    "price_pos": price_pos,
                    "red_count": red_count,
                    "is_loss": is_loss,
                    "day_open": day_open,
                    "day_close": day_close,
                    "prev_close": prev_close,
                    "close_940": close_940,
                    "vol_1": vol_1,
                    "vol_2": vol_2,
                }
            )

    print(f"\n\nFetch: {fetch_ok} OK, {fetch_fail} failed")
    print(f"Collected {len(all_records)} high-open stock-day samples")

    if len(all_records) < 30:
        print("Not enough samples for analysis!")
        return

    df = pd.DataFrame(all_records)

    # ── Step 3: Analysis ──
    print(f"\n{'=' * 60}")
    print("Step 3: Factor Classification Analysis")
    print(f"{'=' * 60}")

    base_rate = df["is_loss"].mean()
    total = len(df)
    print(f"\nTotal samples: {total}")
    print(f"Base rate (close < open): {base_rate:.1%}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Unique stocks: {df['code'].nunique()}")
    print(f"Opening gap range: {df['open_gap_pct'].min():.1f}% ~ {df['open_gap_pct'].max():.1f}%")

    # ── Individual factor analysis ──
    factors = [
        ("early_fade", "Early Fade (冲高回落幅度)", True),  # Higher = worse
        ("vol_decay", "Volume Decay (量能衰减)", False),  # Lower = worse
        ("erosion", "Gain Erosion (涨幅回吐)", True),  # Higher = worse
        ("price_pos", "Price Position (价格位置)", False),  # Lower = worse
    ]

    for col, name, higher_is_worse in factors:
        print(f"\n{'─' * 55}")
        print(f"Factor: {name}")
        vals = df[col]
        print(
            f"  Distribution: mean={vals.mean():.4f}, std={vals.std():.4f}, "
            f"median={vals.median():.4f}"
        )

        # Test at percentile thresholds
        print(f"\n  {'Threshold':>15} {'Flagged':>8} {'Precision':>10} {'Recall':>8} {'Lift':>6}")
        print(f"  {'─' * 52}")

        for pct in [90, 80, 70, 60, 50]:
            if higher_is_worse:
                thresh = np.percentile(vals, pct)
                flagged = df[df[col] >= thresh]
            else:
                thresh = np.percentile(vals, 100 - pct)
                flagged = df[df[col] <= thresh]

            if len(flagged) < 5:
                continue

            prec = flagged["is_loss"].mean()
            recall = flagged["is_loss"].sum() / max(1, df["is_loss"].sum())
            lift = prec / base_rate if base_rate > 0 else 0

            label = f"P{pct}({thresh:.3f})"
            print(f"  {label:>15} {len(flagged):>8} {prec:>10.1%} {recall:>8.1%} {lift:>6.2f}x")

    # ── Red candle count analysis ──
    print(f"\n{'─' * 55}")
    print("Factor: Red Candle Count (前10分钟阴线数)")
    for n in [1, 2]:
        sub = df[df["red_count"] >= n]
        if len(sub) >= 5:
            prec = sub["is_loss"].mean()
            lift = prec / base_rate if base_rate > 0 else 0
            print(f"  red_count >= {n}: n={len(sub)}, precision={prec:.1%}, lift={lift:.2f}x")

    # ── Combined factor analysis ──
    print(f"\n{'=' * 55}")
    print("Combined Factor Analysis (median-split flags)")
    print(f"{'=' * 55}")

    df["f_fade"] = df["early_fade"] > df["early_fade"].median()
    df["f_vol"] = df["vol_decay"] < df["vol_decay"].median()
    df["f_erosion"] = df["erosion"] > df["erosion"].median()
    df["f_pos"] = df["price_pos"] < df["price_pos"].median()
    df["f_red"] = df["red_count"] >= 2

    flag_cols = ["f_fade", "f_vol", "f_erosion", "f_pos", "f_red"]
    df["n_flags"] = df[flag_cols].sum(axis=1)

    print(f"\n  {'N_flags':>8} {'Count':>8} {'Precision':>10} {'Lift':>6}")
    print(f"  {'─' * 35}")
    for n in range(6):
        sub = df[df["n_flags"] >= n]
        if len(sub) < 5:
            continue
        prec = sub["is_loss"].mean()
        lift = prec / base_rate if base_rate > 0 else 0
        print(f"  {'>=' + str(n):>8} {len(sub):>8} {prec:>10.1%} {lift:>6.2f}x")

    # ── Specific combos ──
    print(f"\n{'─' * 55}")
    print("Key Combinations")

    combos = [
        ("Fade高 + Erosion高", df["f_fade"] & df["f_erosion"]),
        ("Fade高 + Vol衰减", df["f_fade"] & df["f_vol"]),
        ("Erosion高 + 位置低", df["f_erosion"] & df["f_pos"]),
        ("Fade高 + Erosion高 + Vol衰减", df["f_fade"] & df["f_erosion"] & df["f_vol"]),
        (
            "全部4因子",
            df["f_fade"] & df["f_erosion"] & df["f_vol"] & df["f_pos"],
        ),
    ]

    for label, mask in combos:
        sub = df[mask]
        if len(sub) < 3:
            print(f"  {label}: n={len(sub)} (too few)")
            continue
        prec = sub["is_loss"].mean()
        lift = prec / base_rate if base_rate > 0 else 0
        print(f"  {label}: n={len(sub)}, precision={prec:.1%}, lift={lift:.2f}x")

    # ── Examples ──
    print(f"\n{'=' * 55}")
    print("Examples: Highest risk (n_flags >= 4)")
    print(f"{'=' * 55}")

    high_risk = df[df["n_flags"] >= 4].sort_values("n_flags", ascending=False).head(10)
    if len(high_risk) > 0:
        for _, r in high_risk.iterrows():
            result = "亏 ✗" if r["is_loss"] else "赚 ✓"
            print(
                f"  {r['code']} {r['date']} gap={r['open_gap_pct']:.1f}% "
                f"fade={r['early_fade']:.2f} erosion={r['erosion']:.2f} "
                f"volD={r['vol_decay']:.2f} pos={r['price_pos']:.2f} → {result}"
            )
    else:
        print("  (No stocks with >= 4 flags)")

    print("\nExamples: Lowest risk (n_flags <= 1)")
    low_risk = df[df["n_flags"] <= 1].head(10)
    for _, r in low_risk.iterrows():
        result = "亏 ✗" if r["is_loss"] else "赚 ✓"
        print(
            f"  {r['code']} {r['date']} gap={r['open_gap_pct']:.1f}% "
            f"fade={r['early_fade']:.2f} erosion={r['erosion']:.2f} "
            f"volD={r['vol_decay']:.2f} pos={r['price_pos']:.2f} → {result}"
        )

    # ── Absolute threshold analysis ──
    print(f"\n{'=' * 55}")
    print("Absolute Threshold Analysis")
    print(f"{'=' * 55}")

    print("\nErosion (涨幅回吐比例):")
    for thresh in [0.3, 0.5, 0.7, 1.0]:
        sub = df[df["erosion"] >= thresh]
        if len(sub) >= 5:
            prec = sub["is_loss"].mean()
            lift = prec / base_rate if base_rate > 0 else 0
            print(f"  erosion >= {thresh}: n={len(sub)}, precision={prec:.1%}, lift={lift:.2f}x")

    print("\nEarly Fade (回落幅度):")
    for thresh in [0.3, 0.5, 0.7, 1.0]:
        sub = df[df["early_fade"] >= thresh]
        if len(sub) >= 5:
            prec = sub["is_loss"].mean()
            lift = prec / base_rate if base_rate > 0 else 0
            print(f"  early_fade >= {thresh}: n={len(sub)}, precision={prec:.1%}, lift={lift:.2f}x")

    print("\nVolume Decay (量比):")
    for thresh in [0.3, 0.5, 0.7]:
        sub = df[df["vol_decay"] <= thresh]
        if len(sub) >= 5:
            prec = sub["is_loss"].mean()
            lift = prec / base_rate if base_rate > 0 else 0
            print(f"  vol_decay <= {thresh}: n={len(sub)}, precision={prec:.1%}, lift={lift:.2f}x")


if __name__ == "__main__":
    main()
