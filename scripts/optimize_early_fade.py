#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Early Fade 阈值优化实验。

目标: 找到 reversal_factor_filter 中 early_fade_threshold 的最优值。

样本条件 (模拟策略实际选股条件):
  - 沪深主板 (60xxxx, 000~003xxx)
  - 非ST
  - 9:40 时 gain_from_open >= 0.56% (策略 GAIN_FROM_OPEN_THRESHOLD)

标签:
  - 主标签: close < open (日内亏损, 即买入当天就亏钱)
  - 辅助: 日内收益率 (close - open) / open

实验设计:
  - 200只股票, 2025-10-01 ~ 2026-02-18 (~95个交易日)
  - early_fade 阈值: 0.30 ~ 0.95, 步长0.05 (14个测试点)
  - 分前半/后半段验证稳定性
  - 同时测试 price_position 阈值和组合策略
  - 计算净收益提升 (过滤后 vs 过滤前)

用法:
    uv run python scripts/optimize_early_fade.py
    uv run python scripts/optimize_early_fade.py --n-stocks 300
"""

import argparse
import io
import random
import sys
import time
import warnings

import akshare as ak
import numpy as np
import pandas as pd

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

warnings.filterwarnings("ignore")

# === CONFIG ===
GAIN_FROM_OPEN_THRESHOLD = 0.56  # 策略实际阈值
OPEN_LIMIT_UP_PCT = 9.5
DAILY_START = "20251001"
DAILY_END = "20260218"
MIN_START = "2025-10-01 09:30:00"
MIN_END = "2026-02-18 15:00:00"
SLEEP = 0.35


def get_main_board_sample(n: int) -> list[str]:
    """获取沪深主板非ST股票样本。"""
    spot = ak.stock_zh_a_spot_em()
    # 排除ST和退市
    spot = spot[~spot["名称"].str.contains("ST|退", na=False)]
    # 沪深主板: 60xxxx, 000xxx~003xxx
    mask = spot["代码"].str.match(r"^(60\d{4}|00[0-3]\d{3})$")
    spot = spot[mask]
    # 按成交额排序取流动性好的
    spot = spot[spot["成交额"] > 5e7]
    spot = spot.sort_values("成交额", ascending=False)
    pool = spot.head(600)["代码"].tolist()
    return random.sample(pool, min(n, len(pool)))


def fetch_all_data(codes: list[str]) -> pd.DataFrame:
    """获取日线+5分钟线，合并为分析数据集。"""
    all_records = []
    ok = 0
    fail = 0

    for i, code in enumerate(codes):
        pct = (i + 1) / len(codes) * 100
        print(f"\r  [{i+1}/{len(codes)}] {pct:.0f}% {code}", end="", flush=True)

        # 日线
        try:
            daily = ak.stock_zh_a_hist(
                symbol=code, period="daily",
                start_date=DAILY_START, end_date=DAILY_END, adjust="qfq",
            )
        except Exception:
            fail += 1
            continue
        if daily is None or len(daily) < 10:
            fail += 1
            continue

        time.sleep(SLEEP)

        # 5分钟线
        try:
            minute = ak.stock_zh_a_hist_min_em(
                symbol=code, start_date=MIN_START, end_date=MIN_END,
                period="5", adjust="",
            )
        except Exception:
            fail += 1
            continue
        if minute is None or len(minute) < 20:
            fail += 1
            continue

        time.sleep(SLEEP)
        ok += 1

        # 处理日线
        daily = daily.copy()
        daily["日期"] = pd.to_datetime(daily["日期"])
        daily["date"] = daily["日期"].dt.date
        daily["prev_close"] = daily["收盘"].shift(1)

        # 处理分钟线
        minute = minute.copy()
        minute["时间"] = pd.to_datetime(minute["时间"])
        minute["date"] = minute["时间"].dt.date
        minute["time_str"] = minute["时间"].dt.strftime("%H:%M")

        # 按日构建 9:35 + 9:40 查找表
        min_by_date: dict = {}
        for d, grp in minute.groupby("date"):
            morning = grp[grp["time_str"].isin(["09:35", "09:40"])].sort_values("时间")
            if len(morning) >= 2:
                min_by_date[d] = morning.head(2)

        # 遍历每个交易日
        for _, day_row in daily.iterrows():
            trade_date = day_row["date"]
            if trade_date not in min_by_date:
                continue

            prev_close = day_row["prev_close"]
            if pd.isna(prev_close) or prev_close <= 0:
                continue

            day_open = day_row["开盘"]
            day_close = day_row["收盘"]
            day_high = day_row["最高"]
            day_low = day_row["最低"]

            # 开盘涨幅 (排除涨停开盘)
            open_gap_pct = (day_open - prev_close) / prev_close * 100
            if open_gap_pct >= OPEN_LIMIT_UP_PCT:
                continue

            bars = min_by_date[trade_date]
            bar1 = bars.iloc[0]  # 9:35
            bar2 = bars.iloc[1]  # 9:40

            close_940 = bar2["收盘"]
            max_high = max(bar1["最高"], bar2["最高"])
            min_low = min(bar1["最低"], bar2["最低"])
            vol_1 = bar1["成交量"]
            vol_2 = bar2["成交量"]

            # 9:40 vs 开盘涨幅
            gain_from_open = (close_940 - day_open) / day_open * 100

            # 只保留满足策略条件的样本
            if gain_from_open < GAIN_FROM_OPEN_THRESHOLD:
                continue

            # === 计算因子 ===
            # Early Fade
            if max_high > day_open:
                early_fade = (max_high - close_940) / (max_high - day_open)
                early_fade = max(0.0, early_fade)
            else:
                early_fade = 0.0

            # Price Position
            range_10 = max_high - min_low
            if range_10 > 0:
                price_pos = (close_940 - min_low) / range_10
            else:
                price_pos = 0.5

            # Volume Decay
            vol_decay = vol_2 / vol_1 if vol_1 > 0 else 1.0

            # Gain Erosion (开盘gap回吐)
            if open_gap_pct > 0:
                erosion = 1.0 - (close_940 - prev_close) / prev_close * 100 / open_gap_pct
            else:
                erosion = 0.0

            # 日内收益
            intraday_return = (day_close - day_open) / day_open * 100

            all_records.append({
                "code": code,
                "date": trade_date,
                "open_gap_pct": open_gap_pct,
                "gain_from_open": gain_from_open,
                "early_fade": early_fade,
                "price_pos": price_pos,
                "vol_decay": vol_decay,
                "erosion": erosion,
                "intraday_return": intraday_return,
                "is_loss": int(day_close < day_open),
                "day_open": day_open,
                "day_close": day_close,
                "close_940": close_940,
                "max_high_10m": max_high,
                "min_low_10m": min_low,
                "day_high": day_high,
                "day_low": day_low,
            })

    print(f"\n  获取成功: {ok}, 失败: {fail}")
    print(f"  满足条件的样本: {len(all_records)}")
    return pd.DataFrame(all_records)


def threshold_analysis(
    df: pd.DataFrame,
    col: str,
    label: str,
    thresholds: list[float],
    higher_is_worse: bool = True,
) -> pd.DataFrame:
    """对单因子在多个阈值下计算精确率/召回率/收益提升。"""
    base_rate = df["is_loss"].mean()
    base_return = df["intraday_return"].mean()
    n_total = len(df)
    rows = []

    for t in thresholds:
        if higher_is_worse:
            flagged_mask = df[col] >= t
        else:
            flagged_mask = df[col] <= t

        n_flagged = flagged_mask.sum()
        if n_flagged < 3 or n_flagged == n_total:
            continue

        kept_mask = ~flagged_mask
        n_kept = kept_mask.sum()

        # 精确率: 被过滤的里面多少真亏了
        tp = (flagged_mask & (df["is_loss"] == 1)).sum()
        fp = (flagged_mask & (df["is_loss"] == 0)).sum()
        fn = (kept_mask & (df["is_loss"] == 1)).sum()

        precision = tp / n_flagged if n_flagged > 0 else 0
        recall = tp / df["is_loss"].sum() if df["is_loss"].sum() > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
        lift = precision / base_rate if base_rate > 0 else 0

        # 收益分析
        avg_return_kept = df.loc[kept_mask, "intraday_return"].mean() if n_kept > 0 else 0
        avg_return_filtered = df.loc[flagged_mask, "intraday_return"].mean() if n_flagged > 0 else 0
        return_improvement = avg_return_kept - base_return

        # 过滤误杀: 被过滤的里面多少其实是赚的
        false_alarm_rate = fp / n_flagged if n_flagged > 0 else 0
        # 留下的亏损率
        kept_loss_rate = fn / n_kept if n_kept > 0 else 0

        rows.append({
            "threshold": t,
            "n_flagged": n_flagged,
            "pct_flagged": n_flagged / n_total * 100,
            "n_kept": n_kept,
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "lift": lift,
            "false_alarm": false_alarm_rate,
            "kept_loss_rate": kept_loss_rate,
            "avg_return_kept": avg_return_kept,
            "avg_return_filtered": avg_return_filtered,
            "return_improvement": return_improvement,
        })

    return pd.DataFrame(rows)


def print_threshold_table(result: pd.DataFrame, label: str, base_rate: float, base_return: float):
    """打印阈值分析结果表。"""
    print(f"\n  {label}")
    print(f"  基础亏损率: {base_rate:.1%}  基础日内收益: {base_return:+.2f}%")
    print(f"  {'阈值':>6}  {'过滤数':>5} {'过滤%':>5}  {'精确率':>6} {'召回率':>6} "
          f"{'F1':>5}  {'提升':>5}  {'误杀率':>6}  {'留下亏损率':>8}  "
          f"{'留下均收益':>8}  {'被滤均收益':>8}  {'收益提升':>6}")
    print(f"  {'─' * 115}")

    for _, r in result.iterrows():
        print(
            f"  {r['threshold']:>6.2f}  {r['n_flagged']:>5.0f} {r['pct_flagged']:>4.1f}%  "
            f"{r['precision']:>5.1%} {r['recall']:>5.1%} "
            f"{r['f1']:>5.3f}  {r['lift']:>4.2f}x  {r['false_alarm']:>5.1%}  "
            f"{r['kept_loss_rate']:>7.1%}  "
            f"{r['avg_return_kept']:>+7.2f}%  {r['avg_return_filtered']:>+7.2f}%  "
            f"{r['return_improvement']:>+5.2f}%"
        )


def find_optimal(result: pd.DataFrame) -> dict:
    """从多个维度找最优阈值。"""
    if result.empty:
        return {}

    # 策略1: 最大 F1
    best_f1_idx = result["f1"].idxmax()
    best_f1 = result.loc[best_f1_idx]

    # 策略2: 最大收益提升
    best_return_idx = result["return_improvement"].idxmax()
    best_return = result.loc[best_return_idx]

    # 策略3: 最大 precision * recall (几何均值)
    result["geo_mean"] = np.sqrt(result["precision"] * result["recall"])
    best_geo_idx = result["geo_mean"].idxmax()
    best_geo = result.loc[best_geo_idx]

    # 策略4: 最大 "净价值" = return_improvement * n_kept / n_total_approx
    # (收益提升大但不能过滤太多)
    total_approx = result.iloc[0]["n_kept"] + result.iloc[0]["n_flagged"]
    result["net_value"] = result["return_improvement"] * result["n_kept"] / total_approx
    best_nv_idx = result["net_value"].idxmax()
    best_nv = result.loc[best_nv_idx]

    return {
        "best_f1": {"threshold": best_f1["threshold"], "f1": best_f1["f1"],
                     "precision": best_f1["precision"], "recall": best_f1["recall"]},
        "best_return": {"threshold": best_return["threshold"],
                        "improvement": best_return["return_improvement"],
                        "avg_kept": best_return["avg_return_kept"]},
        "best_geo": {"threshold": best_geo["threshold"], "geo_mean": best_geo["geo_mean"]},
        "best_net_value": {"threshold": best_nv["threshold"],
                           "net_value": best_nv["net_value"],
                           "improvement": best_nv["return_improvement"],
                           "pct_filtered": best_nv["pct_flagged"]},
    }


def bootstrap_ci(df: pd.DataFrame, col: str, threshold: float,
                  n_bootstrap: int = 1000, ci: float = 0.95) -> dict:
    """Bootstrap 置信区间。"""
    n = len(df)
    precisions = []
    returns_kept = []

    for _ in range(n_bootstrap):
        sample = df.sample(n=n, replace=True)
        flagged = sample[sample[col] >= threshold]
        kept = sample[sample[col] < threshold]

        if len(flagged) > 0:
            precisions.append(flagged["is_loss"].mean())
        if len(kept) > 0:
            returns_kept.append(kept["intraday_return"].mean())

    alpha = (1 - ci) / 2
    result = {}
    if precisions:
        result["precision_ci"] = (
            np.percentile(precisions, alpha * 100),
            np.percentile(precisions, (1 - alpha) * 100),
        )
        result["precision_mean"] = np.mean(precisions)
    if returns_kept:
        result["return_kept_ci"] = (
            np.percentile(returns_kept, alpha * 100),
            np.percentile(returns_kept, (1 - alpha) * 100),
        )
        result["return_kept_mean"] = np.mean(returns_kept)
    return result


def main():
    parser = argparse.ArgumentParser(description="Early Fade阈值优化实验")
    parser.add_argument("--n-stocks", type=int, default=200, help="抽样股票数")
    parser.add_argument("--seed", type=int, default=2026, help="随机种子")
    args = parser.parse_args()

    random.seed(args.seed)
    np.random.seed(args.seed)

    print("=" * 120)
    print("  Early Fade 阈值优化实验")
    print("  目标: 找到最优 early_fade_threshold, 最大化过滤冲高回落票的效果")
    print("  样本: 9:40 涨幅 >= 0.56% 的沪深主板非ST股票 (模拟策略实际条件)")
    print("=" * 120)

    # Step 1: 获取数据
    print(f"\n[Step 1] 抽样 {args.n_stocks} 只主板股票...")
    codes = get_main_board_sample(args.n_stocks)
    print(f"  抽样完成: {len(codes)} 只")

    print(f"\n[Step 2] 获取 {DAILY_START}~{DAILY_END} 日线+5分钟线数据...")
    df = fetch_all_data(codes)

    if len(df) < 50:
        print("样本不足，退出")
        return

    # 基本统计
    n = len(df)
    n_loss = df["is_loss"].sum()
    base_rate = n_loss / n
    base_return = df["intraday_return"].mean()
    n_stocks = df["code"].nunique()
    n_dates = df["date"].nunique()

    print(f"\n{'=' * 120}")
    print(f"[Step 3] 样本概况")
    print(f"{'=' * 120}")
    print(f"  总样本: {n} 条 ({n_stocks}只股票 × {n_dates}个交易日)")
    print(f"  日期范围: {df['date'].min()} ~ {df['date'].max()}")
    print(f"  日内亏损 (close<open): {n_loss} ({base_rate:.1%})")
    print(f"  日内盈利: {n - n_loss} ({1-base_rate:.1%})")
    print(f"  平均日内收益: {base_return:+.3f}%")
    print(f"  开盘涨幅分布: {df['open_gap_pct'].describe().to_dict()}")
    print(f"  9:40涨幅分布: mean={df['gain_from_open'].mean():.2f}%, "
          f"median={df['gain_from_open'].median():.2f}%, "
          f"max={df['gain_from_open'].max():.2f}%")

    # Early Fade 分布
    print(f"\n  Early Fade 分布:")
    for p in [10, 25, 50, 75, 80, 85, 90, 95]:
        v = np.percentile(df["early_fade"], p)
        print(f"    P{p}: {v:.4f}", end="")
    print()

    # === 全样本分析 ===
    print(f"\n{'=' * 120}")
    print(f"[Step 4] Early Fade 全样本阈值分析 (n={n})")
    print(f"{'=' * 120}")

    thresholds_fine = [round(x, 2) for x in np.arange(0.20, 1.01, 0.05)]
    result_all = threshold_analysis(df, "early_fade", "Early Fade", thresholds_fine)
    print_threshold_table(result_all, "Early Fade (全样本)", base_rate, base_return)

    optimal_all = find_optimal(result_all)
    print(f"\n  最优阈值 (全样本):")
    for method, info in optimal_all.items():
        print(f"    {method}: {info}")

    # === 分段验证 ===
    print(f"\n{'=' * 120}")
    print(f"[Step 5] 分段验证 (前半 vs 后半)")
    print(f"{'=' * 120}")

    dates_sorted = sorted(df["date"].unique())
    mid = len(dates_sorted) // 2
    df1 = df[df["date"].isin(dates_sorted[:mid])]
    df2 = df[df["date"].isin(dates_sorted[mid:])]

    print(f"\n  前半段: {dates_sorted[0]} ~ {dates_sorted[mid-1]}, n={len(df1)}, "
          f"亏损率={df1['is_loss'].mean():.1%}, 均收益={df1['intraday_return'].mean():+.3f}%")
    result_h1 = threshold_analysis(df1, "early_fade", "前半段", thresholds_fine)
    print_threshold_table(result_h1, "Early Fade (前半段)", df1["is_loss"].mean(), df1["intraday_return"].mean())

    print(f"\n  后半段: {dates_sorted[mid]} ~ {dates_sorted[-1]}, n={len(df2)}, "
          f"亏损率={df2['is_loss'].mean():.1%}, 均收益={df2['intraday_return'].mean():+.3f}%")
    result_h2 = threshold_analysis(df2, "early_fade", "后半段", thresholds_fine)
    print_threshold_table(result_h2, "Early Fade (后半段)", df2["is_loss"].mean(), df2["intraday_return"].mean())

    # === 按开盘涨幅分层 ===
    print(f"\n{'=' * 120}")
    print(f"[Step 6] 按开盘涨幅分层分析")
    print(f"{'=' * 120}")

    for lo, hi, label in [(0.56, 2.0, "小涨 0.56~2%"), (2.0, 5.0, "中涨 2~5%"), (5.0, 10.0, "大涨 5%+")]:
        sub = df[(df["gain_from_open"] >= lo) & (df["gain_from_open"] < hi)]
        if len(sub) < 30:
            print(f"\n  {label}: 样本不足 ({len(sub)})")
            continue
        print(f"\n  {label}: n={len(sub)}, 亏损率={sub['is_loss'].mean():.1%}, "
              f"均收益={sub['intraday_return'].mean():+.3f}%")
        result_sub = threshold_analysis(sub, "early_fade", label, thresholds_fine)
        print_threshold_table(result_sub, f"Early Fade ({label})", sub["is_loss"].mean(), sub["intraday_return"].mean())

    # === Price Position 阈值分析 ===
    print(f"\n{'=' * 120}")
    print(f"[Step 7] Price Position 阈值分析 (越低越危险)")
    print(f"{'=' * 120}")

    pp_thresholds = [round(x, 2) for x in np.arange(0.10, 0.61, 0.05)]
    result_pp = threshold_analysis(df, "price_pos", "Price Position", pp_thresholds, higher_is_worse=False)
    print_threshold_table(result_pp, "Price Position ≤ 阈值 (全样本)", base_rate, base_return)

    # === 组合策略 ===
    print(f"\n{'=' * 120}")
    print(f"[Step 8] 组合策略分析 (Early Fade + Price Position)")
    print(f"{'=' * 120}")

    combo_rows = []
    for ef_t in [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]:
        for pp_t in [0.20, 0.25, 0.30, 0.35, 0.40]:
            for logic in ["OR", "AND"]:
                if logic == "OR":
                    flagged = (df["early_fade"] >= ef_t) | (df["price_pos"] <= pp_t)
                else:
                    flagged = (df["early_fade"] >= ef_t) & (df["price_pos"] <= pp_t)

                n_flagged = flagged.sum()
                if n_flagged < 3 or n_flagged == n:
                    continue

                kept = ~flagged
                n_kept = kept.sum()
                tp = (flagged & (df["is_loss"] == 1)).sum()
                fp = (flagged & (df["is_loss"] == 0)).sum()
                precision = tp / n_flagged
                recall = tp / n_loss if n_loss > 0 else 0
                f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
                avg_ret_kept = df.loc[kept, "intraday_return"].mean()
                improvement = avg_ret_kept - base_return

                combo_rows.append({
                    "ef_thresh": ef_t, "pp_thresh": pp_t, "logic": logic,
                    "n_flagged": n_flagged, "pct_flagged": n_flagged / n * 100,
                    "precision": precision, "recall": recall, "f1": f1,
                    "false_alarm": fp / n_flagged,
                    "avg_return_kept": avg_ret_kept, "improvement": improvement,
                })

    combo_df = pd.DataFrame(combo_rows)
    if not combo_df.empty:
        # 按收益提升排序, 取 top 15
        print(f"\n  组合策略 Top 15 (按收益提升排序):")
        print(f"  {'EF阈值':>6} {'PP阈值':>6} {'逻辑':>4}  {'过滤数':>5} {'过滤%':>5}  "
              f"{'精确率':>6} {'召回率':>6} {'F1':>5}  {'误杀率':>6}  "
              f"{'留下均收益':>8}  {'收益提升':>6}")
        print(f"  {'─' * 95}")

        top = combo_df.nlargest(15, "improvement")
        for _, r in top.iterrows():
            print(
                f"  {r['ef_thresh']:>6.2f} {r['pp_thresh']:>6.2f} {r['logic']:>4}  "
                f"{r['n_flagged']:>5.0f} {r['pct_flagged']:>4.1f}%  "
                f"{r['precision']:>5.1%} {r['recall']:>5.1%} {r['f1']:>5.3f}  "
                f"{r['false_alarm']:>5.1%}  "
                f"{r['avg_return_kept']:>+7.2f}%  {r['improvement']:>+5.2f}%"
            )

        # 按 F1 排序, 取 top 10
        print(f"\n  组合策略 Top 10 (按F1排序):")
        print(f"  {'EF阈值':>6} {'PP阈值':>6} {'逻辑':>4}  {'过滤数':>5} {'过滤%':>5}  "
              f"{'精确率':>6} {'召回率':>6} {'F1':>5}  "
              f"{'留下均收益':>8}  {'收益提升':>6}")
        print(f"  {'─' * 85}")

        top_f1 = combo_df.nlargest(10, "f1")
        for _, r in top_f1.iterrows():
            print(
                f"  {r['ef_thresh']:>6.2f} {r['pp_thresh']:>6.2f} {r['logic']:>4}  "
                f"{r['n_flagged']:>5.0f} {r['pct_flagged']:>4.1f}%  "
                f"{r['precision']:>5.1%} {r['recall']:>5.1%} {r['f1']:>5.3f}  "
                f"{r['avg_return_kept']:>+7.2f}%  {r['improvement']:>+5.2f}%"
            )

    # === Bootstrap 置信区间 ===
    print(f"\n{'=' * 120}")
    print(f"[Step 9] Bootstrap 置信区间 (关键阈值)")
    print(f"{'=' * 120}")

    # 取全样本最优阈值和几个候选
    candidates = set()
    for info in optimal_all.values():
        candidates.add(info["threshold"])
    # 加上当前阈值和常见候选
    candidates.update([0.50, 0.55, 0.60, 0.65, 0.70])
    candidates = sorted(candidates)

    print(f"\n  {'阈值':>6}  {'精确率均值':>8}  {'精确率95%CI':>16}  "
          f"{'留下收益均值':>10}  {'留下收益95%CI':>18}")
    print(f"  {'─' * 75}")

    for t in candidates:
        ci = bootstrap_ci(df, "early_fade", t, n_bootstrap=2000)
        if "precision_ci" in ci:
            print(
                f"  {t:>6.2f}  {ci['precision_mean']:>7.1%}  "
                f"[{ci['precision_ci'][0]:.1%}, {ci['precision_ci'][1]:.1%}]  "
                f"{ci['return_kept_mean']:>+9.3f}%  "
                f"[{ci['return_kept_ci'][0]:>+.3f}%, {ci['return_kept_ci'][1]:>+.3f}%]"
            )

    # === 600678 案例验证 ===
    print(f"\n{'=' * 120}")
    print(f"[Step 10] 600678 四川金顶案例验证")
    print(f"{'=' * 120}")
    case = df[df["code"] == "600678"]
    if len(case) > 0:
        print(f"  600678 在样本中有 {len(case)} 条记录:")
        for _, r in case.iterrows():
            ef_flag = "← 会被过滤" if r["early_fade"] >= 0.63 else ""
            print(
                f"    {r['date']}  gap={r['open_gap_pct']:+.1f}%  9:40涨幅={r['gain_from_open']:.2f}%  "
                f"early_fade={r['early_fade']:.3f}  price_pos={r['price_pos']:.3f}  "
                f"日内收益={r['intraday_return']:+.2f}%  {'亏' if r['is_loss'] else '赚'}  {ef_flag}"
            )
    else:
        print("  600678 不在本次样本中 (可重新运行扩大样本)")

    # === 最终推荐 ===
    print(f"\n{'=' * 120}")
    print(f"[结论] 最终推荐")
    print(f"{'=' * 120}")

    if not result_all.empty:
        # 综合推荐: 优先收益提升, 兼顾F1
        best = result_all.loc[result_all["return_improvement"].idxmax()]
        print(f"\n  推荐 early_fade_threshold = {best['threshold']:.2f}")
        print(f"    过滤比例: {best['pct_flagged']:.1f}%")
        print(f"    精确率: {best['precision']:.1%} (被过滤的里{best['precision']:.0%}真亏了)")
        print(f"    召回率: {best['recall']:.1%} (亏损票中{best['recall']:.0%}被拦住)")
        print(f"    误杀率: {best['false_alarm']:.1%}")
        print(f"    日内收益提升: {best['return_improvement']:+.3f}% (过滤后均值 vs 过滤前)")
        print(f"    对比当前 0.70: ", end="")
        curr = result_all[result_all["threshold"] == 0.70]
        if not curr.empty:
            c = curr.iloc[0]
            print(f"精确率{c['precision']:.1%}, 召回率{c['recall']:.1%}, "
                  f"收益提升{c['return_improvement']:+.3f}%")
        else:
            print("(0.70不在结果中)")


if __name__ == "__main__":
    main()
