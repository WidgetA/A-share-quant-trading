"""
高开低走过滤器生产参数校准实验。

目的:
1. 验证实验最佳组合 B (vol>2x AND turnover top15%) 的稳定性
2. 对比旧生产参数 (20日均换手 > 阈值) vs 新参数 (当日换手绝对阈值)
3. 测试生产代理指标: est_daily_turnover = volume_ratio * avg_turnover_20d

AND 组合:
  - B: vol>2.0 AND turnover_pct top15% (实验最佳, 全5段显著)
  - P1: vol>2.0 AND avg_turn20d top15% (旧生产, 不稳定)
  - P2: vol>2.0 AND turnover_pct > 10% (绝对阈值版)
  - P3: vol>2.0 AND turnover_pct > 8%  (更宽松)
  - P4: vol>2.0 AND est_daily_turn > 10% (生产代理, 用于live模式)
  - P5: vol>2.0 AND est_daily_turn > 8%  (更宽松代理)

验证方法: 5个时段 × 10000次随机排列, p<0.05 才算有效。
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
                symbol=code,
                period="daily",
                start_date=start,
                end_date=end,
                adjust="qfq",
            )
            if df is not None and len(df) > 10:
                df = df.rename(
                    columns={
                        "日期": "date",
                        "开盘": "open",
                        "收盘": "close",
                        "最高": "high",
                        "最低": "low",
                        "成交量": "volume",
                        "换手率": "turnover_pct",
                        "涨跌幅": "change_pct",
                    }
                )
                df["code"] = code
                dfs.append(
                    df[
                        [
                            "code",
                            "date",
                            "open",
                            "close",
                            "high",
                            "low",
                            "volume",
                            "turnover_pct",
                            "change_pct",
                        ]
                    ]
                )
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

    # 目标变量
    data["overnight_return"] = (data["next_open"] - data["close"]) / data["close"] * 100
    data["open_gap_pct"] = (data["open"] - data["prev_close"]) / data["prev_close"] * 100

    g = data.groupby("code")

    # --- 原有因子 ---

    # 量比: 当日量 / 20日均量
    data["avg_vol_20d"] = g["volume"].transform(
        lambda s: s.shift(1).rolling(20, min_periods=5).mean()
    )
    data["volume_ratio"] = np.where(
        data["avg_vol_20d"] > 0, data["volume"] / data["avg_vol_20d"], np.nan
    )

    # 当日换手率 (直接用)
    # 振幅
    data["amplitude"] = (data["high"] - data["low"]) / data["prev_close"] * 100

    # 上影线比例
    hl_range = data["high"] - data["low"]
    data["upper_shadow"] = np.where(hl_range > 0, (data["high"] - data["close"]) / hl_range, 0)

    # 前一日涨跌幅
    data["prev_change"] = g["change_pct"].shift(1)

    # --- 新增因子 ---

    # 20日平均换手率 (生产环境实际使用的指标, 替代单日换手率)
    data["avg_turnover_20d"] = g["turnover_pct"].transform(
        lambda s: s.shift(1).rolling(20, min_periods=5).mean()
    )

    # 换手放大倍数: 当日换手 / 20日均换手
    data["turnover_amp"] = np.where(
        data["avg_turnover_20d"] > 0,
        data["turnover_pct"] / data["avg_turnover_20d"],
        np.nan,
    )

    # 近3日/5日累计涨跌幅 (用 prev_close 计算, 不含当天)
    data["close_3d_ago"] = g["close"].shift(3)
    data["close_5d_ago"] = g["close"].shift(5)
    data["n3_gain"] = np.where(
        data["close_3d_ago"] > 0,
        (data["prev_close"] - data["close_3d_ago"]) / data["close_3d_ago"] * 100,
        np.nan,
    )
    data["n5_gain"] = np.where(
        data["close_5d_ago"] > 0,
        (data["prev_close"] - data["close_5d_ago"]) / data["close_5d_ago"] * 100,
        np.nan,
    )

    return data


def permutation_test(overnight: np.ndarray, mask: np.ndarray, n_sim: int = 10000) -> dict:
    k = mask.sum()
    if k < 5:
        return {"skip": True, "k": k}

    strategy_mean = overnight[mask].mean()
    strategy_fk = (overnight[mask] > 0).mean()
    random_fk = (overnight > 0).mean()

    rng = np.random.default_rng(42)
    rand_means = np.array(
        [overnight[rng.choice(len(overnight), size=k, replace=False)].mean() for _ in range(n_sim)]
    )

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


def test_factor(
    universe: pd.DataFrame, factor_col: str, direction: str, quantile: float = 0.15
) -> dict:
    """测试单个因子: 取最极端的 quantile 比例作为过滤组, vs 10000次随机过滤."""
    valid = universe[universe[factor_col].notna()].copy()
    if len(valid) < 50:
        return {"factor": factor_col, "direction": direction, "n": 0, "skip": True}

    overnight = valid["overnight_return"].values
    vals = valid[factor_col].values

    if direction == "high":
        threshold = np.nanpercentile(vals, (1 - quantile) * 100)
        mask = vals > threshold
    else:
        threshold = np.nanpercentile(vals, quantile * 100)
        mask = vals < threshold

    k = mask.sum()
    if k < 5:
        return {"factor": factor_col, "direction": direction, "n": k, "skip": True}

    strategy_mean = overnight[mask].mean()

    rng = np.random.default_rng(42)
    rand_means = np.array(
        [overnight[rng.choice(len(overnight), size=k, replace=False)].mean() for _ in range(n_sim)]
        if (n_sim := 10000)
        else []
    )

    p_value = (rand_means <= strategy_mean).mean()
    strategy_fk = (overnight[mask] > 0).mean()
    random_fk = (overnight > 0).mean()

    return {
        "factor": factor_col,
        "direction": direction,
        "k": k,
        "total": len(valid),
        "threshold": threshold,
        "strategy_return": strategy_mean,
        "random_return": rand_means.mean(),
        "p_value": p_value,
        "strategy_fk": strategy_fk,
        "random_fk": random_fk,
        "skip": False,
    }


def build_and_masks(valid: pd.DataFrame) -> dict[str, np.ndarray]:
    """构建所有 AND 组合的 mask."""
    vr = valid["volume_ratio"].values
    tp = valid["turnover_pct"].values
    amp = valid["amplitude"].values
    ta = valid["turnover_amp"].values
    at20 = valid["avg_turnover_20d"].values
    n3 = valid["n3_gain"].values
    n5 = valid["n5_gain"].values

    # 百分位阈值
    tp_high = tp > np.nanpercentile(tp, 85)
    amp_high = amp > np.nanpercentile(amp, 85)
    ta_low = ta < np.nanpercentile(ta, 15)
    at20_low = at20 < np.nanpercentile(at20, 15)
    at20_high = at20 > np.nanpercentile(at20, 85)
    n3_low = n3 < np.nanpercentile(n3, 15)
    n5_low = n5 < np.nanpercentile(n5, 15)

    # 生产代理: 估算当日换手 = volume_ratio * avg_turnover_20d
    est_daily_turn = np.where(at20 > 0, vr * at20, np.nan)

    return {
        # --- 原有 gap-fade 组合 ---
        "F: vol>2.0 (baseline)": vr > 2.0,
        "B: vol>2.0 AND turn_pct": (vr > 2.0) & tp_high,
        "A: vol>2.0 AND amp": (vr > 2.0) & amp_high,
        # --- gap-fade 旧生产 (20日均换手, 已证明不可靠) ---
        "P1: vol>2.0 AND avg_turn20d": (vr > 2.0) & at20_high,
        # --- gap-fade v5 生产: 用当日换手绝对阈值 ---
        "P2: vol>2.0 AND tp>10%": (vr > 2.0) & (tp > 10.0),
        "P3: vol>2.0 AND tp>8%": (vr > 2.0) & (tp > 8.0),
        # --- gap-fade v5 代理: vol_ratio * avg_turn > 阈值 ---
        "P4: vol>2 AND est_t>10%": (vr > 2.0) & (est_daily_turn > 10.0),
        "P5: vol>2 AND est_t>8%": (vr > 2.0) & (est_daily_turn > 8.0),
    }


def run_period(codes: list[str], start: str, end: str, period_name: str) -> dict:
    print(f"\n  [{period_name}] 下载数据...")

    raw = fetch_all_data(codes, start, end)
    data = prepare(raw)

    all_dates = sorted(data["date"].unique())
    if len(all_dates) > 20:
        data = data[data["date"].isin(all_dates[-20:])].copy()

    universe = data[data["open_gap_pct"] >= 1.0].copy()
    valid = universe.dropna(
        subset=["volume_ratio", "turnover_pct", "amplitude", "turnover_amp", "n3_gain", "n5_gain"]
    ).copy()

    overnight = valid["overnight_return"].values
    n_total = len(valid)
    base_mean = overnight.mean()
    base_pos = (overnight > 0).mean()

    print(
        f"  [{period_name}] 高开>=1%: {n_total}个, "
        f"次日均值: {base_mean:+.3f}%, >0占比: {base_pos:.1%}"
    )

    # --- Part 1: 单因子测试 (包含新因子) ---
    factors = [
        ("volume_ratio", "high"),
        ("volume_ratio", "low"),
        ("turnover_pct", "high"),
        ("turnover_pct", "low"),
        ("amplitude", "high"),
        ("upper_shadow", "high"),
        ("prev_change", "high"),
        ("prev_change", "low"),
        ("turnover_amp", "high"),
        ("turnover_amp", "low"),
        ("avg_turnover_20d", "high"),
        ("avg_turnover_20d", "low"),
        ("n3_gain", "high"),
        ("n3_gain", "low"),
        ("n5_gain", "high"),
        ("n5_gain", "low"),
    ]

    single_results = []
    for factor_col, direction in factors:
        r = test_factor(valid, factor_col, direction)
        if not r["skip"]:
            single_results.append(r)

    single_results.sort(key=lambda x: x["p_value"])

    print("\n  单因子测试 (top 15%):")
    print(
        f"  {'因子':<22s} {'方向':>4s} {'过滤数':>5s} {'阈值':>8s} "
        f"{'策略次日':>8s} {'随机次日':>8s} {'p-value':>8s} {'误杀率':>7s} {'结论'}"
    )
    print("  " + "-" * 100)

    for r in single_results:
        sig = (
            "***"
            if r["p_value"] < 0.01
            else "**"
            if r["p_value"] < 0.05
            else "*"
            if r["p_value"] < 0.10
            else ""
        )
        dir_cn = "高" if r["direction"] == "high" else "低"
        print(
            f"  {r['factor']:<22s} {dir_cn:>4s} {r['k']:>5d} {r['threshold']:>8.2f} "
            f"{r['strategy_return']:>+7.2f}% {r['random_return']:>+7.2f}% "
            f"{r['p_value']:>8.3f} {r['strategy_fk']:>6.1%} {sig}"
        )

    # --- Part 2: AND 组合测试 ---
    masks = build_and_masks(valid)
    combo_results = {}
    for label, mask in masks.items():
        r = permutation_test(overnight, mask)
        r["label"] = label
        combo_results[label] = r

    print("\n  AND组合测试:")
    print(
        f"  {'组合':<30s} {'过滤数':>5s} {'策略次日':>8s} {'随机次日':>8s} "
        f"{'p-value':>8s} {'误杀率':>7s} {'结论'}"
    )
    print("  " + "-" * 90)

    for label in masks:
        r = combo_results[label]
        if r["skip"]:
            print(f"  {label:<30s} {'n<5':>5s}")
            continue
        sig = (
            "***"
            if r["p_value"] < 0.01
            else "**"
            if r["p_value"] < 0.05
            else "*"
            if r["p_value"] < 0.10
            else ""
        )
        print(
            f"  {label:<30s} {r['k']:>5d} "
            f"{r['strategy_return']:>+7.2f}% {r['random_return']:>+7.2f}% "
            f"{r['p_value']:>8.3f} {r['strategy_fk']:>6.1%} {sig}"
        )

    return combo_results


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
    print("  动量质量因子筛查 + 高开低走生产指标校准")
    print("  (300只股票, 5个时段, 单因子+AND组合)")
    print("=" * 90)

    codes = get_main_board_codes(300)
    print(f"抽样 {len(codes)} 只主板股票\n")

    all_results: dict[str, list] = {}

    for start, end, name in periods:
        pr = run_period(codes, start, end, name)
        for label, r in pr.items():
            all_results.setdefault(label, []).append((name, r))

    # ============================================================
    # 汇总表
    # ============================================================
    labels = list(all_results.keys())
    period_names = [p[2] for p in periods]

    print(f"\n{'=' * 90}")
    print("  汇总: 各组合在5个时段的表现")
    print(f"{'=' * 90}")

    # 策略次日收益
    print("\n  策略次日收益 (过滤组):")
    print(f"  {'组合':<30s}", end="")
    for pn in period_names:
        print(f" {pn:>14s}", end="")
    print(f" {'全部显著?':>10s}")
    print("  " + "-" * 115)

    for label in labels:
        print(f"  {label:<30s}", end="")
        all_sig = True
        for _pn, r in all_results[label]:
            if r["skip"]:
                print(f" {'n<5':>14s}", end="")
                all_sig = False
            else:
                sig = "***" if r["p_value"] < 0.01 else "** " if r["p_value"] < 0.05 else "   "
                print(f" {r['strategy_return']:>+6.2f}%{sig:>4s}  ", end="")
                if r["p_value"] >= 0.05:
                    all_sig = False
        print(f" {'YES' if all_sig else 'no':>10s}")

    # p-value
    print("\n  p-value:")
    print(f"  {'组合':<30s}", end="")
    for pn in period_names:
        print(f" {pn:>14s}", end="")
    print(f" {'全<0.05?':>10s}")
    print("  " + "-" * 115)

    for label in labels:
        print(f"  {label:<30s}", end="")
        all_sig = True
        for _pn, r in all_results[label]:
            if r["skip"]:
                print(f" {'n<5':>14s}", end="")
                all_sig = False
            else:
                print(f" {r['p_value']:>14.3f}", end="")
                if r["p_value"] >= 0.05:
                    all_sig = False
        print(f" {'YES' if all_sig else 'no':>10s}")

    # 误杀率
    print("\n  误杀率 (过滤组中次日>0的比例):")
    print(f"  {'组合':<30s}", end="")
    for pn in period_names:
        print(f" {pn:>14s}", end="")
    print(f" {'平均误杀':>10s}")
    print("  " + "-" * 115)

    for label in labels:
        print(f"  {label:<30s}", end="")
        fks = []
        for _pn, r in all_results[label]:
            if r["skip"]:
                print(f" {'n<5':>14s}", end="")
            else:
                print(f" {r['strategy_fk']:>13.1%}", end="")
                fks.append(r["strategy_fk"])
        avg_fk = sum(fks) / len(fks) if fks else float("nan")
        print(f" {avg_fk:>9.1%}")

    # 过滤数
    print("\n  过滤数:")
    print(f"  {'组合':<30s}", end="")
    for pn in period_names:
        print(f" {pn:>14s}", end="")
    print(f" {'平均过滤':>10s}")
    print("  " + "-" * 115)

    for label in labels:
        print(f"  {label:<30s}", end="")
        ks = []
        for _pn, r in all_results[label]:
            k = r.get("k", 0)
            print(f" {k:>14d}", end="")
            ks.append(k)
        avg_k = sum(ks) / len(ks) if ks else 0
        print(f" {avg_k:>9.1f}")


if __name__ == "__main__":
    main()
