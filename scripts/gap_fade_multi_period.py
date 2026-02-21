"""
高开低走AND组合多时段验证。

候选组合 (上轮实验的top方案):
  A: vol>2.0x AND amplitude>top15%
  B: vol>2.0x AND turnover>top15%
  C: vol>2.0x AND (turn|amp|shadow任一)
  D: vol>2.5x AND amplitude>top15%
  E: vol>3.0x AND amplitude>top15%
  F: vol>2.0x (单独, 作为baseline)

跨5个时段验证稳定性。
"""

import random
import time

import akshare as ak
import numpy as np
import pandas as pd


def get_main_board_codes(n: int = 300) -> list[str]:
    stock_list = ak.stock_info_a_code_name()
    mask = stock_list["code"].str.match(r"^(60\d{4}|00[0-3]\d{3})$")
    codes = stock_list[mask]["code"].tolist()
    return random.sample(codes, min(n, len(codes)))


def fetch_all_data(codes: list[str], start: str, end: str) -> pd.DataFrame:
    dfs = []
    for i, code in enumerate(codes):
        try:
            df = ak.stock_zh_a_hist(
                symbol=code, period="daily",
                start_date=start, end_date=end, adjust="qfq",
            )
            if df is not None and len(df) > 10:
                df = df.rename(columns={
                    "日期": "date", "开盘": "open", "收盘": "close",
                    "最高": "high", "最低": "low", "成交量": "volume",
                    "换手率": "turnover_pct", "涨跌幅": "change_pct",
                })
                df["code"] = code
                dfs.append(df[["code", "date", "open", "close", "high", "low",
                               "volume", "turnover_pct", "change_pct"]])
        except Exception:
            pass
        if (i + 1) % 50 == 0:
            print(f"    进度: {i + 1}/{len(codes)}")
        time.sleep(0.03)
    if not dfs:
        raise RuntimeError("No data")
    return pd.concat(dfs, ignore_index=True)


def prepare(data: pd.DataFrame) -> pd.DataFrame:
    data = data.sort_values(["code", "date"]).reset_index(drop=True)
    g = data.groupby("code")

    data["prev_close"] = g["close"].shift(1)
    data["next_open"] = g["open"].shift(-1)
    data = data.dropna(subset=["prev_close", "next_open"]).copy()

    data["overnight_return"] = (data["next_open"] - data["close"]) / data["close"] * 100
    data["open_gap_pct"] = (data["open"] - data["prev_close"]) / data["prev_close"] * 100

    g = data.groupby("code")

    data["avg_vol_20d"] = g["volume"].transform(
        lambda s: s.shift(1).rolling(20, min_periods=5).mean()
    )
    data["volume_ratio"] = np.where(
        data["avg_vol_20d"] > 0, data["volume"] / data["avg_vol_20d"], np.nan
    )

    data["amplitude"] = (data["high"] - data["low"]) / data["prev_close"] * 100

    hl_range = data["high"] - data["low"]
    data["upper_shadow"] = np.where(hl_range > 0, (data["high"] - data["close"]) / hl_range, 0)

    return data


def permutation_test(overnight: np.ndarray, mask: np.ndarray, n_sim: int = 10000) -> dict:
    k = mask.sum()
    if k < 5:
        return {"skip": True, "k": k}

    strategy_mean = overnight[mask].mean()
    strategy_fk = (overnight[mask] > 0).mean()
    random_fk = (overnight > 0).mean()

    rng = np.random.default_rng(42)
    rand_means = np.array([
        overnight[rng.choice(len(overnight), size=k, replace=False)].mean()
        for _ in range(n_sim)
    ])

    p_value = (rand_means <= strategy_mean).mean()

    return {
        "skip": False,
        "k": k,
        "total": len(overnight),
        "strategy_return": strategy_mean,
        "random_return": rand_means.mean(),
        "p_value": p_value,
        "strategy_fk": strategy_fk,
        "random_fk": random_fk,
    }


def build_masks(valid: pd.DataFrame) -> dict[str, np.ndarray]:
    """构建所有候选组合的mask."""
    vr = valid["volume_ratio"].values
    tp = valid["turnover_pct"].values
    amp = valid["amplitude"].values
    us = valid["upper_shadow"].values

    tp_thresh = np.nanpercentile(tp, 85)
    amp_thresh = np.nanpercentile(amp, 85)
    us_thresh = np.nanpercentile(us, 85)

    tp_high = tp > tp_thresh
    amp_high = amp > amp_thresh
    us_high = us > us_thresh
    any_high = tp_high | amp_high | us_high

    return {
        "F: vol>2.0 (baseline)":          vr > 2.0,
        "A: vol>2.0 AND amp":             (vr > 2.0) & amp_high,
        "B: vol>2.0 AND turn":            (vr > 2.0) & tp_high,
        "C: vol>2.0 AND (any)":           (vr > 2.0) & any_high,
        "D: vol>2.5 AND amp":             (vr > 2.5) & amp_high,
        "E: vol>3.0 AND amp":             (vr > 3.0) & amp_high,
        "G: vol>2.0 AND shadow":          (vr > 2.0) & us_high,
        "H: vol>2.5 AND turn":            (vr > 2.5) & tp_high,
    }


def run_period(codes: list[str], start: str, end: str, period_name: str) -> dict:
    print(f"\n  [{period_name}] 下载数据...")

    raw = fetch_all_data(codes, start, end)
    data = prepare(raw)

    all_dates = sorted(data["date"].unique())
    if len(all_dates) > 20:
        data = data[data["date"].isin(all_dates[-20:])].copy()

    universe = data[data["open_gap_pct"] >= 1.0].copy()
    valid = universe.dropna(subset=["volume_ratio", "turnover_pct", "amplitude", "upper_shadow"]).copy()

    overnight = valid["overnight_return"].values
    n_total = len(valid)
    base_mean = overnight.mean()
    base_pos = (overnight > 0).mean()

    print(f"  [{period_name}] 高开>=1%: {n_total}个, "
          f"次日均值: {base_mean:+.3f}%, >0占比: {base_pos:.1%}")

    masks = build_masks(valid)
    period_results = {}
    for label, mask in masks.items():
        r = permutation_test(overnight, mask)
        r["label"] = label
        period_results[label] = r

    return period_results


def main():
    random.seed(2026)

    periods = [
        ("20251201", "20260214", "2025-12~2026-02"),
        ("20250715", "20250930", "2025-08~2025-09"),
        ("20250401", "20250615", "2025-04~2025-06"),
        ("20250101", "20250315", "2025-01~2025-03"),
        ("20241001", "20241215", "2024-10~2024-12"),
    ]

    print("=" * 90)
    print("  高开低走AND组合 — 5个时段稳定性验证")
    print("=" * 90)

    codes = get_main_board_codes(300)
    print(f"抽样 {len(codes)} 只主板股票\n")

    all_results: dict[str, list] = {}  # label → list of period results

    for start, end, name in periods:
        pr = run_period(codes, start, end, name)
        for label, r in pr.items():
            all_results.setdefault(label, []).append((name, r))

    # ============================================================
    # 汇总表
    # ============================================================
    labels = list(build_masks.__code__.co_consts)  # hack; just use first period's keys
    labels = list(all_results.keys())

    print(f"\n{'=' * 90}")
    print("  汇总: 各组合在5个时段的表现")
    print(f"{'=' * 90}")

    # Header
    period_names = [p[2] for p in periods]
    header_p = "  "
    header_f = "  "
    for pn in period_names:
        short = pn.split("~")[0][-5:]  # e.g. "25-12"
        header_p += f" {short:>12s}"
        header_f += f" {'':>12s}"

    print("\n  策略次日收益 (过滤组):")
    print(f"  {'组合':<28s}", end="")
    for pn in period_names:
        print(f" {pn:>14s}", end="")
    print(f" {'全部显著?':>10s}")
    print("  " + "-" * 110)

    for label in labels:
        print(f"  {label:<28s}", end="")
        all_sig = True
        for pn, r in all_results[label]:
            if r["skip"]:
                print(f" {'n<5':>14s}", end="")
                all_sig = False
            else:
                sig = "***" if r["p_value"] < 0.01 else "** " if r["p_value"] < 0.05 else "   "
                print(f" {r['strategy_return']:>+6.2f}%{sig:>4s}  ", end="")
                if r["p_value"] >= 0.05:
                    all_sig = False
        print(f" {'YES' if all_sig else 'no':>10s}")

    print("\n  p-value:")
    print(f"  {'组合':<28s}", end="")
    for pn in period_names:
        print(f" {pn:>14s}", end="")
    print(f" {'全<0.05?':>10s}")
    print("  " + "-" * 110)

    for label in labels:
        print(f"  {label:<28s}", end="")
        all_sig = True
        for pn, r in all_results[label]:
            if r["skip"]:
                print(f" {'n<5':>14s}", end="")
                all_sig = False
            else:
                print(f" {r['p_value']:>14.3f}", end="")
                if r["p_value"] >= 0.05:
                    all_sig = False
        print(f" {'YES' if all_sig else 'no':>10s}")

    print("\n  误杀率 (过滤组中次日>0的比例):")
    print(f"  {'组合':<28s}", end="")
    for pn in period_names:
        print(f" {pn:>14s}", end="")
    print(f" {'平均误杀':>10s}")
    print("  " + "-" * 110)

    for label in labels:
        print(f"  {label:<28s}", end="")
        fks = []
        for pn, r in all_results[label]:
            if r["skip"]:
                print(f" {'n<5':>14s}", end="")
            else:
                print(f" {r['strategy_fk']:>13.1%}", end="")
                fks.append(r["strategy_fk"])
        avg_fk = sum(fks) / len(fks) if fks else float("nan")
        print(f" {avg_fk:>9.1%}")

    print("\n  过滤数:")
    print(f"  {'组合':<28s}", end="")
    for pn in period_names:
        print(f" {pn:>14s}", end="")
    print(f" {'平均过滤':>10s}")
    print("  " + "-" * 110)

    for label in labels:
        print(f"  {label:<28s}", end="")
        ks = []
        for pn, r in all_results[label]:
            k = r.get("k", 0)
            print(f" {k:>14d}", end="")
            ks.append(k)
        avg_k = sum(ks) / len(ks) if ks else 0
        print(f" {avg_k:>9.1f}")


if __name__ == "__main__":
    main()
