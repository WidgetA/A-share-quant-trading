"""
Validate v7 GapFade filter using akshare data.
For a set of stocks × dates, compute 10-min volume ratio and 10-min turnover,
apply the filter, and check against actual next-day open outcome.
"""

import random
import time

import akshare as ak
import pandas as pd

# === CONFIG ===
DAILY_START = "20251201"
DAILY_END = "20260218"
MIN_GAP_PCT = 1.5
MAX_GAP_PCT = 7.0  # 排除涨停开盘
LOOKBACK = 20


def get_volatile_stocks(n: int = 60) -> list[str]:
    """Pick main-board stocks with recent high volatility."""
    # Get all A-share spot data
    print("Fetching stock list...")
    spot = ak.stock_zh_a_spot_em()
    # Main board only: 6xxxxx (SH) or 000xxx (SZ)
    spot = spot[
        spot["代码"].str.match(r"^(6\d{5}|000\d{3})$")
        & (spot["涨跌幅"].notna())
    ].copy()
    # Sort by recent turnover (higher = more volatile/active)
    spot = spot.sort_values("换手率", ascending=False)
    codes = spot["代码"].head(n * 2).tolist()
    random.seed(42)
    random.shuffle(codes)
    return codes[:n]


def fetch_daily(code: str) -> pd.DataFrame | None:
    try:
        df = ak.stock_zh_a_hist(
            symbol=code, period="daily",
            start_date=DAILY_START, end_date=DAILY_END, adjust=""
        )
        if df is None or df.empty:
            return None
        df = df.rename(columns={
            "日期": "date", "开盘": "open", "收盘": "close",
            "最高": "high", "最低": "low", "成交量": "volume",
            "换手率": "turnover", "涨跌幅": "change_pct", "成交额": "amount",
        })
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.sort_values("date").reset_index(drop=True)
        df["prev_close"] = df["close"].shift(1)
        df["next_open"] = df["open"].shift(-1)
        return df
    except Exception as e:
        print(f"  daily failed {code}: {e}")
        return None


def fetch_minute(code: str) -> pd.DataFrame | None:
    """Fetch 1-min bars. akshare returns ~recent 5 trading days of 1-min data."""
    try:
        df = ak.stock_zh_a_hist_min_em(symbol=code, period="1", adjust="")
        if df is None or df.empty:
            return None
        df = df.rename(columns={
            "时间": "time", "开盘": "open", "收盘": "close",
            "最高": "high", "最低": "low", "成交量": "volume",
            "成交额": "amount",
        })
        df["time"] = pd.to_datetime(df["time"])
        df["date"] = df["time"].dt.date
        df["hhmm"] = df["time"].dt.strftime("%H:%M")
        return df
    except Exception as e:
        print(f"  minute failed {code}: {e}")
        return None


def fetch_minute_5(code: str) -> pd.DataFrame | None:
    """Fetch 5-min bars — goes further back than 1-min."""
    try:
        df = ak.stock_zh_a_hist_min_em(symbol=code, period="5", adjust="")
        if df is None or df.empty:
            return None
        df = df.rename(columns={
            "时间": "time", "开盘": "open", "收盘": "close",
            "最高": "high", "最低": "low", "成交量": "volume",
            "成交额": "amount",
        })
        df["time"] = pd.to_datetime(df["time"])
        df["date"] = df["time"].dt.date
        df["hhmm"] = df["time"].dt.strftime("%H:%M")
        return df
    except Exception as e:
        print(f"  minute-5 failed {code}: {e}")
        return None


def compute_early_from_5min(df: pd.DataFrame) -> dict:
    """
    From 5-min bars, get 9:30-9:40 data + 9:40 price (buy price).
    5-min bars: 09:35 (covers 9:30-9:35), 09:40 (covers 9:35-9:40).
    """
    early = df[df["hhmm"].isin(["09:35", "09:40"])].copy()
    if early.empty:
        return {}

    # 9:40 bar's close = price at 9:40 (buy price)
    bar_940 = df[df["hhmm"] == "09:40"][["date", "close"]].rename(columns={"close": "price_940"})

    grouped = early.groupby("date").agg(
        early_vol=("volume", "sum"),
        early_amount=("amount", "sum"),
    )
    grouped = grouped.join(bar_940.set_index("date"))
    return grouped.to_dict("index")


def run_test():
    stocks = get_volatile_stocks(60)
    print(f"Selected {len(stocks)} stocks\n")

    all_results = []

    for i, code in enumerate(stocks):
        print(f"[{i+1}/{len(stocks)}] {code}", end="")
        daily = fetch_daily(code)
        if daily is None or len(daily) < LOOKBACK + 5:
            print(" — skip (no daily)")
            continue

        # Gap-up days
        daily["gap_pct"] = (daily["open"] - daily["prev_close"]) / daily["prev_close"] * 100
        gap_days = daily[
            (daily["gap_pct"] >= MIN_GAP_PCT)
            & (daily["gap_pct"] <= MAX_GAP_PCT)
            & daily["prev_close"].notna()
            & daily["next_open"].notna()
        ]

        if gap_days.empty:
            print(f" — no gaps >= {MIN_GAP_PCT}%")
            continue

        print(f" — {len(gap_days)} gap days, fetching 5-min bars...", end="")
        time.sleep(0.3)

        min_df = fetch_minute_5(code)
        if min_df is None or min_df.empty:
            print(" no minute data")
            continue

        early_data = compute_early_from_5min(min_df)
        min_dates = sorted(early_data.keys())
        print(f" got {len(min_dates)} days of minute data")

        if len(min_dates) < 5:
            continue

        for _, row in gap_days.iterrows():
            d = row["date"]
            if d not in early_data:
                continue

            ed = early_data[d]
            today_ev = ed["early_vol"]
            today_ea = ed["early_amount"]
            price_940 = ed.get("price_940")

            if not price_940 or price_940 <= 0:
                continue

            # 冲高回落: 盘中最高到收盘跌了多少
            fade_pct = (row["high"] - row["close"]) / row["close"] * 100 if row["close"] > 0 else 0

            # Historical early data: past LOOKBACK days before this gap day
            hist_dates = [dd for dd in min_dates if dd < d][-LOOKBACK:]
            if len(hist_dates) < 5:
                continue

            hist_vols = [early_data[dd]["early_vol"] for dd in hist_dates if early_data[dd]["early_vol"] > 0]
            hist_amounts = [early_data[dd]["early_amount"] for dd in hist_dates if early_data[dd]["early_amount"] > 0]

            if not hist_vols or not hist_amounts:
                continue

            avg_early_vol = sum(hist_vols) / len(hist_vols)
            vol_ratio = today_ev / avg_early_vol if avg_early_vol > 0 else 0

            # p85 of historical early amounts (proxy for turnover)
            sorted_amounts = sorted(hist_amounts)
            p85_idx = min(int(0.85 * len(sorted_amounts)), len(sorted_amounts) - 1)
            p85_amount = sorted_amounts[p85_idx]
            turn_high = today_ea > p85_amount

            vol_high = vol_ratio > 2.0
            filtered = vol_high and turn_high

            # 冲高回落: high到close跌幅>3%算真正的冲高回落
            FADE_THRESHOLD = 3.0
            faded = fade_pct > FADE_THRESHOLD

            if filtered and faded:
                tag = "OK"
            elif filtered and not faded:
                tag = "FALSE+"
            elif not filtered and faded:
                tag = "MISS"
            else:
                tag = "-"

            all_results.append({
                "code": code,
                "date": d,
                "gap%": row["gap_pct"],
                "vol_ratio": vol_ratio,
                "amt_vs_p85": today_ea / p85_amount if p85_amount > 0 else 0,
                "filtered": filtered,
                "fade%": fade_pct,
                "faded": faded,
                "tag": tag,
            })

        time.sleep(0.3)

    # === RESULTS ===
    if not all_results:
        print("\nNo results collected!")
        return

    df = pd.DataFrame(all_results)
    print(f"\n{'='*80}")
    print(f"RESULTS: {len(df)} gap-up stock-days across {df['code'].nunique()} stocks")
    print(f"{'='*80}")

    print(f"\n{'Code':<8} {'Date':>12} {'Gap%':>6} {'VolR':>6} {'Amt/P85':>8} "
          f"{'Filter':>7} {'Fade%':>7} {'Tag':>7}")
    print("-" * 70)
    for _, r in df.sort_values(["date", "code"]).iterrows():
        print(f"{r['code']:<8} {str(r['date']):>12} {r['gap%']:>+5.1f}% "
              f"{r['vol_ratio']:>5.1f}x {r['amt_vs_p85']:>7.2f}x "
              f"{'YES' if r['filtered'] else 'no':>7} {r['fade%']:>6.1f}% "
              f"{r['tag']:>7}")

    n_total = len(df)
    n_faded = int(df["faded"].sum())
    n_filtered = int(df["filtered"].sum())
    n_ok = int(((df["filtered"]) & (df["faded"])).sum())
    n_false_pos = int(((df["filtered"]) & (~df["faded"])).sum())
    n_miss = int(((~df["filtered"]) & (df["faded"])).sum())

    print(f"\n--- Summary (冲高回落 = high到close跌幅>3%) ---")
    print(f"Total gap-up days:    {n_total}")
    print(f"Actually faded:       {n_faded} ({n_faded/n_total*100:.0f}%)")
    print(f"Filter triggered:     {n_filtered}")
    print(f"  Correct (OK):       {n_ok}")
    print(f"  False positive:     {n_false_pos}")
    print(f"  Missed fades:       {n_miss}")

    if n_filtered > 0:
        precision = n_ok / n_filtered * 100
        print(f"\nPrecision (filter抓到的中真冲高回落): {precision:.0f}%")
    if n_faded > 0:
        recall = n_ok / n_faded * 100
        print(f"Recall (真冲高回落中被抓到的):        {recall:.0f}%")

    filtered_fade = df[df["filtered"]]["fade%"]
    kept_fade = df[~df["filtered"]]["fade%"]
    print(f"\n--- Fade% (high到close回落幅度) Comparison ---")
    if len(filtered_fade) > 0:
        print(f"Filtered stocks avg fade:  {filtered_fade.mean():.2f}% (n={len(filtered_fade)})")
    if len(kept_fade) > 0:
        print(f"Kept stocks avg fade:      {kept_fade.mean():.2f}% (n={len(kept_fade)})")
    if len(filtered_fade) > 0 and len(kept_fade) > 0:
        diff = filtered_fade.mean() - kept_fade.mean()
        print(f"Difference (filtered - kept): {diff:+.2f}%  (正=filter抓到的回落更大)")


if __name__ == "__main__":
    run_test()
