#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
冲高回落因子识别准确率测试。

核心问题: 因子标记"冲高回落风险"的股票，是不是真的冲高回落了？

冲高回落定义:
  - 冲高: (high - open) / open >= 0.3% (日内确实涨过)
  - 回落: close < open (但收盘低于开盘)
  → 涨上去又跌回来 = 冲高回落

样本: 开盘涨幅 > 0.5% 的沪深主板股票 (排除开盘涨停)

用法:
    uv run python scripts/test_reversal_factors.py
    uv run python scripts/test_reversal_factors.py --n-stocks 300 --period 20251001,20260218
"""

import argparse
import io
import random
import sys
import time

import akshare as ak
import numpy as np
import pandas as pd

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")


def get_main_board_codes(n: int = 200) -> list[str]:
    stock_list = ak.stock_info_a_code_name()
    mask = stock_list["code"].str.match(r"^(60\d{4}|00[0-3]\d{3})$")
    codes = stock_list[mask]["code"].tolist()
    return random.sample(codes, min(n, len(codes)))


def fetch_data(codes: list[str], start: str, end: str) -> pd.DataFrame:
    dfs = []
    for i, code in enumerate(codes):
        try:
            df = ak.stock_zh_a_hist(
                symbol=code, period="daily", start_date=start, end_date=end, adjust="qfq"
            )
            if df is not None and len(df) > 25:
                df = df.rename(
                    columns={
                        "日期": "date", "开盘": "open", "收盘": "close",
                        "最高": "high", "最低": "low", "成交量": "volume",
                        "换手率": "turnover_pct",
                    }
                )
                df["code"] = code
                cols = ["code", "date", "open", "high", "low", "close", "volume", "turnover_pct"]
                dfs.append(df[cols])
        except Exception:
            pass
        if (i + 1) % 50 == 0:
            print(f"    进度: {i + 1}/{len(codes)}")
        time.sleep(0.03)
    if not dfs:
        raise RuntimeError("No data fetched")
    return pd.concat(dfs, ignore_index=True)


def compute_factors(data: pd.DataFrame) -> pd.DataFrame:
    data = data.sort_values(["code", "date"]).reset_index(drop=True)
    g = data.groupby("code")

    data["prev_close"] = g["close"].shift(1)

    # 开盘涨幅 (vs prev_close)
    data["open_gap_pct"] = np.where(
        data["prev_close"] > 0,
        (data["open"] - data["prev_close"]) / data["prev_close"] * 100, np.nan,
    )

    # 排除开盘涨停 (>=9.5%)
    data["is_open_limit_up"] = data["open_gap_pct"] >= 9.5

    # === 日内亏损标签 (冲高回落 + 高开低走都算) ===
    rush_up = (data["high"] - data["open"]) / data["open"] * 100  # 日内冲高幅度%
    data["rush_up_pct"] = rush_up
    data["is_reversal"] = data["close"] < data["open"]  # 收盘<开盘 = 日内亏钱

    # 日内收益 (仅用于展示)
    data["intraday_return"] = np.where(
        data["open"] > 0, (data["close"] - data["open"]) / data["open"] * 100, np.nan,
    )
    hl_range = data["high"] - data["low"]
    data["pullback_ratio"] = np.where(hl_range > 0, (data["high"] - data["close"]) / hl_range, 0)

    # === Factor 1: 上影线因子 (20日std) ===
    oc_max = np.maximum(data["open"], data["close"])
    data["upper_shadow_ratio"] = np.where(hl_range > 0, (data["high"] - oc_max) / hl_range, 0)
    data["f_shadow"] = g["upper_shadow_ratio"].transform(
        lambda s: s.shift(1).rolling(20, min_periods=15).std()
    )

    # === Factor 2: V_high振幅因子 ===
    data["amplitude"] = np.where(data["low"] > 0, (data["high"] - data["low"]) / data["low"], 0)
    v_high_values: dict[int, float] = {}
    for _code, sub_df in g:
        sub = sub_df.sort_values("date")
        closes = sub["close"].values
        amps = sub["amplitude"].values
        orig_idx = sub.index.values
        for j in range(21, len(sub)):
            c_window = closes[j - 20 : j]
            a_window = amps[j - 20 : j]
            w = len(c_window)
            if w < 15:
                continue
            top_n = max(1, int(w * 0.4))
            sorted_i = np.argsort(c_window)
            v_h = a_window[sorted_i[-top_n:]].mean()
            v_l = a_window[sorted_i[:top_n]].mean()
            v_high_values[orig_idx[j]] = v_h - v_l
    data["f_vhigh"] = pd.Series(v_high_values, dtype=float)

    # === Factor 3: 隔夜Gap (直接用open_gap_pct) ===
    data["f_gap"] = data["open_gap_pct"]

    return data


def classification_report(
    universe: pd.DataFrame,
    col: str,
    label: str,
    thresholds: list[float],
) -> None:
    """对一个因子，在多个阈值下测试识别准确率。"""
    is_rev = universe["is_reversal"].values
    vals = universe[col].values
    base_rate = is_rev.mean()
    n = len(universe)

    print(f"\n  {label}  (base日内亏损率: {base_rate:.1%}, 样本{n})")
    print(
        f"  {'阈值':<16}  {'标记数':>5}  {'标中':>4}  {'漏掉':>4}  "
        f"{'精确率':>6}  {'召回率':>6}  {'提升':>5}  {'误杀率':>6}"
    )
    print(f"  {'─' * 82}")

    for thresh in thresholds:
        flagged = vals > thresh
        n_flagged = flagged.sum()
        if n_flagged == 0:
            print(f"  >{thresh:<14}  {0:>5}  —")
            continue

        tp = (flagged & is_rev).sum()        # true positive: flagged AND actually reversed
        fn = (~flagged & is_rev).sum()       # false negative: not flagged BUT actually reversed
        fp = (flagged & ~is_rev).sum()       # false positive: flagged BUT did NOT reverse

        precision = tp / n_flagged           # 标记的里面多少是真冲高回落
        recall = tp / is_rev.sum() if is_rev.sum() > 0 else 0  # 真冲高回落里多少被标记
        lift = precision / base_rate if base_rate > 0 else 0    # 比随机好多少倍
        false_alarm = fp / n_flagged         # 标记的里面多少其实没回落 (误杀)

        print(
            f"  >{thresh:<14}  {n_flagged:>5}  {tp:>4}  {fn:>4}  "
            f"{precision:>5.1%}  {recall:>5.1%}  {lift:>4.1f}x  {false_alarm:>5.1%}"
        )

    # 百分位阈值补充
    for pct in [70, 80, 90]:
        p_val = np.nanpercentile(vals, pct)
        flagged = vals > p_val
        n_flagged = flagged.sum()
        if n_flagged < 3:
            continue
        tp = (flagged & is_rev).sum()
        fn = (~flagged & is_rev).sum()
        fp = (flagged & ~is_rev).sum()
        precision = tp / n_flagged
        recall = tp / is_rev.sum() if is_rev.sum() > 0 else 0
        lift = precision / base_rate if base_rate > 0 else 0
        false_alarm = fp / n_flagged
        print(
            f"  >P{pct}({p_val:.4f})  {n_flagged:>5}  {tp:>4}  {fn:>4}  "
            f"{precision:>5.1%}  {recall:>5.1%}  {lift:>4.1f}x  {false_alarm:>5.1%}"
        )


def show_examples(universe: pd.DataFrame, col: str, label: str, thresh: float) -> None:
    """展示因子标记的具体案例，让用户直观判断。"""
    flagged = universe[universe[col] > thresh].sort_values("intraday_return")
    not_flagged_rev = universe[(universe[col] <= thresh) & universe["is_reversal"]].sort_values(
        "intraday_return"
    )

    def fmt_row(r):
        tag = "亏损" if r["is_reversal"] else "盈利"
        return (
            f"  {r['code']:>8} {r['date']}  "
            f"Gap{r['open_gap_pct']:>+5.1f}%  日内{r['intraday_return']:>+6.2f}%  "
            f"冲高{r['rush_up_pct']:>4.1f}%  回落比{r['pullback_ratio']:.2f}  "
            f"{col}={r[col]:.4f}  → {tag}"
        )

    print(f"\n  {label} > {thresh} 标记的股票 (抽样展示):")
    if len(flagged) == 0:
        print("    (无)")
        return

    # 标记中确实冲高回落的 (正确标记)
    correct = flagged[flagged["is_reversal"]]
    wrong = flagged[~flagged["is_reversal"]]

    n_show = min(8, len(correct))
    if n_show > 0:
        print(f"    ✓ 正确标记 ({len(correct)}只中展示{n_show}只):")
        for _, r in correct.head(n_show).iterrows():
            print(f"  {fmt_row(r)}")

    n_show = min(5, len(wrong))
    if n_show > 0:
        print(f"    ✗ 误杀 ({len(wrong)}只中展示{n_show}只):")
        for _, r in wrong.head(n_show).iterrows():
            print(f"  {fmt_row(r)}")

    n_show = min(5, len(not_flagged_rev))
    if n_show > 0:
        print(f"    ○ 漏掉的亏损 ({len(not_flagged_rev)}只中展示{n_show}只):")
        for _, r in not_flagged_rev.head(n_show).iterrows():
            print(f"  {fmt_row(r)}")


def combined_classification(universe: pd.DataFrame) -> None:
    """测试组合因子 (>=N个因子同时触发)。"""
    is_rev = universe["is_reversal"].values
    base_rate = is_rev.mean()

    # 动态阈值: 用P70 for shadow/vhigh, fixed for gap
    us_p70 = np.nanpercentile(universe["f_shadow"].values, 70)
    vh_p70 = np.nanpercentile(universe["f_vhigh"].values, 70)

    configs = [
        ("V_high>0.02 + Gap>3%", (universe["f_vhigh"] > 0.02) & (universe["f_gap"] > 3.0)),
        ("V_high>0.02 + Gap>2%", (universe["f_vhigh"] > 0.02) & (universe["f_gap"] > 2.0)),
        ("V_high>P70 + Gap>3%", (universe["f_vhigh"] > vh_p70) & (universe["f_gap"] > 3.0)),
        ("V_high>P70 + Gap>2%", (universe["f_vhigh"] > vh_p70) & (universe["f_gap"] > 2.0)),
        (
            "三因子>=2触发",
            (
                (universe["f_shadow"] > us_p70).astype(int)
                + (universe["f_vhigh"] > vh_p70).astype(int)
                + (universe["f_gap"] > 3.0).astype(int)
            )
            >= 2,
        ),
    ]

    print(f"\n  组合因子  (base日内亏损率: {base_rate:.1%})")
    print(
        f"  {'组合':<24}  {'标记数':>5}  {'标中':>4}  "
        f"{'精确率':>6}  {'召回率':>6}  {'提升':>5}  {'误杀率':>6}"
    )
    print(f"  {'─' * 72}")

    for name, mask in configs:
        n_flagged = mask.sum()
        if n_flagged == 0:
            print(f"  {name:<24}  {0:>5}  —")
            continue
        tp = (mask & is_rev).sum()
        fp = (mask & ~is_rev).sum()
        precision = tp / n_flagged
        recall = tp / is_rev.sum() if is_rev.sum() > 0 else 0
        lift = precision / base_rate if base_rate > 0 else 0
        false_alarm = fp / n_flagged

        print(
            f"  {name:<24}  {n_flagged:>5}  {tp:>4}  "
            f"{precision:>5.1%}  {recall:>5.1%}  {lift:>4.1f}x  {false_alarm:>5.1%}"
        )


def analyze_period(data: pd.DataFrame, period_label: str) -> None:
    # 样本: 高开>0.5% 且 非开盘涨停
    universe = data[
        (data["open_gap_pct"] >= 0.5)
        & (~data["is_open_limit_up"])
        & data["f_shadow"].notna()
        & data["f_vhigh"].notna()
    ].copy()

    if len(universe) < 50:
        print(f"\n  [{period_label}] 样本不足: {len(universe)}")
        return

    n = len(universe)
    n_rev = universe["is_reversal"].sum()
    base_rate = n_rev / n

    print(f"\n{'=' * 110}")
    print(f"  {period_label}")
    print(f"  样本: {n}只 (高开>0.5%, 排除开盘涨停)")
    n_win = n - n_rev
    print(f"  日内亏损: {n_rev}只 ({base_rate:.1%})   日内盈利: {n_win}只 ({1 - base_rate:.1%})")
    print("  日内亏损定义: 收盘 < 开盘")
    print(f"{'=' * 110}")

    # 各因子分类报告
    classification_report(
        universe, "f_shadow", "因子1: 上影线因子",
        [0.15, 0.20, 0.25, 0.30],
    )
    classification_report(
        universe, "f_vhigh", "因子2: V_high振幅因子",
        [0.01, 0.015, 0.02, 0.03, 0.04],
    )
    classification_report(
        universe, "f_gap", "因子3: 隔夜Gap (%)",
        [1.0, 1.5, 2.0, 3.0, 5.0],
    )

    # 组合因子
    combined_classification(universe)

    # 具体案例展示
    show_examples(universe, "f_vhigh", "V_high", 0.02)
    show_examples(universe, "f_gap", "隔夜Gap", 3.0)


def main():
    parser = argparse.ArgumentParser(description="冲高回落因子识别准确率测试")
    parser.add_argument("--n-stocks", type=int, default=300, help="抽样股票数 (默认300)")
    parser.add_argument(
        "--period", type=str, default="20251001,20260218",
        help="数据区间 start,end (默认20251001,20260218)",
    )
    args = parser.parse_args()
    start_str, end_str = args.period.split(",")
    random.seed(2026)

    print("=" * 110)
    print("  冲高回落因子识别准确率测试")
    print("  问题: 因子标记的'冲高回落风险'股票，实际有多少真的冲高回落了？")
    print("  日内亏损 = 收盘<开盘 (含冲高回落+高开低走)  |  排除开盘涨停")
    print("=" * 110)

    codes = get_main_board_codes(args.n_stocks)
    print(f"\n  抽样 {len(codes)} 只主板股票, 下载 {start_str}~{end_str} ...")
    raw = fetch_data(codes, start_str, end_str)
    print(f"  {len(raw)} 条记录, {raw['code'].nunique()} 只股票")

    print("  计算因子...")
    data = compute_factors(raw)

    all_dates = sorted(data["date"].unique())
    if len(all_dates) < 25:
        print("  交易日不足")
        return

    # 分两个时段检验稳定性
    valid_dates = all_dates[22:]  # skip warmup
    if len(valid_dates) >= 20:
        mid = len(valid_dates) // 2
        d1 = data[data["date"].isin(valid_dates[:mid])]
        d2 = data[data["date"].isin(valid_dates[mid:])]
        analyze_period(d1, f"前半段 ({valid_dates[0]} ~ {valid_dates[mid - 1]})")
        analyze_period(d2, f"后半段 ({valid_dates[mid]} ~ {valid_dates[-1]})")

    data_all = data[data["date"].isin(valid_dates)]
    analyze_period(data_all, f"全样本 ({valid_dates[0]} ~ {valid_dates[-1]})")


if __name__ == "__main__":
    main()
