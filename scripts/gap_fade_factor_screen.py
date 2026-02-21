"""
高开低走因子全面筛查。

从日线数据能构造的所有因子中，找出能显著区分"高开后次日差"的票的因子。
验证方法：策略过滤 vs 10000次随机过滤，p<0.05才算有效。

候选因子：
1. volume_ratio: 当日量 / 20日均量 (分高/低两端)
2. turnover_pct: 当日换手率
3. amplitude: 当日振幅 (high-low)/prev_close
4. close_to_high: 收盘离最高的距离 (high-close)/(high-low), 越大=上影线越长
5. prev_day_change: 前一日涨跌幅
6. prev_day_amplitude: 前一日振幅
7. prev_day_turnover: 前一日换手率
8. gap_size: 跳空幅度本身
9. n3_day_gain: 近3日累计涨幅
10. n5_day_gain: 近5日累计涨幅
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
            print(f"  进度: {i + 1}/{len(codes)}")
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

    # 候选因子
    g = data.groupby("code")

    # 1. 量比
    data["avg_vol_20d"] = g["volume"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).mean())
    data["volume_ratio"] = np.where(data["avg_vol_20d"] > 0, data["volume"] / data["avg_vol_20d"], np.nan)

    # 2. 换手率 (直接用)
    # 3. 振幅
    data["amplitude"] = (data["high"] - data["low"]) / data["prev_close"] * 100

    # 4. 上影线比例 (收盘离最高的距离)
    hl_range = data["high"] - data["low"]
    data["upper_shadow"] = np.where(hl_range > 0, (data["high"] - data["close"]) / hl_range, 0)

    # 5-7. 前一日指标
    data["prev_change"] = g["change_pct"].shift(1)
    data["prev_amplitude"] = g["amplitude"].shift(1)
    data["prev_turnover"] = g["turnover_pct"].shift(1)

    # 8. gap size 就是 open_gap_pct

    # 9-10. 近N日累计涨幅
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


def test_factor(universe: pd.DataFrame, factor_col: str, direction: str,
                quantile: float, n_sim: int = 10000) -> dict:
    """
    测试单个因子：取最极端的quantile比例的票作为过滤组，vs 随机过滤同等数量。

    direction: "high" = 取最高的那些过滤, "low" = 取最低的过滤
    quantile: 0.1 = 取最极端的10%
    """
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
    kept_mean = overnight[~mask].mean()

    rng = np.random.default_rng(42)
    rand_means = np.array([
        overnight[rng.choice(len(overnight), size=k, replace=False)].mean()
        for _ in range(n_sim)
    ])

    p_value = (rand_means <= strategy_mean).mean()

    # 随机误杀率 = 总体次日>0的比例
    random_false_kill = (overnight > 0).mean()
    strategy_false_kill = (overnight[mask] > 0).mean()

    return {
        "factor": factor_col,
        "direction": direction,
        "k": k,
        "total": len(valid),
        "threshold": threshold,
        "strategy_return": strategy_mean,
        "random_return": rand_means.mean(),
        "p_value": p_value,
        "strategy_fk": strategy_false_kill,
        "random_fk": random_false_kill,
        "skip": False,
    }


def run_period(codes: list[str], start: str, end: str, period_name: str):
    print(f"\n{'#' * 72}")
    print(f"  {period_name}")
    print(f"{'#' * 72}")

    raw = fetch_all_data(codes, start, end)
    data = prepare(raw)

    all_dates = sorted(data["date"].unique())
    if len(all_dates) > 20:
        data = data[data["date"].isin(all_dates[-20:])].copy()

    universe = data[data["open_gap_pct"] >= 1.0].copy()
    print(f"高开>=1%: {len(universe)}个股票日")
    print(f"全体次日隔夜均值: {universe['overnight_return'].mean():+.3f}%")
    print(f"全体次日>0占比: {(universe['overnight_return'] > 0).mean():.1%}")

    # 测试所有因子, 取最极端15%
    factors = [
        ("volume_ratio", "high"),   # 放量
        ("volume_ratio", "low"),    # 缩量
        ("turnover_pct", "high"),   # 高换手
        ("turnover_pct", "low"),    # 低换手
        ("amplitude", "high"),      # 大振幅
        ("upper_shadow", "high"),   # 长上影线
        ("prev_change", "high"),    # 前日大涨
        ("prev_change", "low"),     # 前日大跌
        ("prev_amplitude", "high"), # 前日大振幅
        ("prev_turnover", "high"),  # 前日高换手
        ("open_gap_pct", "high"),   # 大跳空
        ("n3_gain", "high"),        # 3日连涨
        ("n5_gain", "high"),        # 5日连涨
    ]

    results = []
    for factor_col, direction in factors:
        r = test_factor(universe, factor_col, direction, quantile=0.15)
        if not r["skip"]:
            results.append(r)

    # 按 p-value 排序
    results.sort(key=lambda x: x["p_value"])

    print(f"\n  {'因子':<22s} {'方向':>4s} {'过滤数':>5s} {'阈值':>8s} "
          f"{'策略次日':>8s} {'随机次日':>8s} {'p-value':>8s} {'策略误杀':>8s} {'随机误杀':>8s} {'结论'}")
    print("  " + "-" * 110)

    for r in results:
        sig = "***" if r["p_value"] < 0.01 else "**" if r["p_value"] < 0.05 else "*" if r["p_value"] < 0.10 else ""
        dir_cn = "高" if r["direction"] == "high" else "低"
        print(
            f"  {r['factor']:<22s} {dir_cn:>4s} {r['k']:>5d} {r['threshold']:>8.2f} "
            f"{r['strategy_return']:>+7.2f}% {r['random_return']:>+7.2f}% "
            f"{r['p_value']:>8.3f} {r['strategy_fk']:>7.1%} {r['random_fk']:>8.1%} {sig}"
        )

    return results


def main():
    random.seed(2026)
    print("=" * 72)
    print("  高开低走因子全面筛查 (300只股票, 2个时段, 13个因子)")
    print("  验证: 策略过滤 vs 10000次随机过滤")
    print("=" * 72)

    codes = get_main_board_codes(300)
    print(f"抽样 {len(codes)} 只主板股票")

    r1 = run_period(codes, "20251201", "20260214", "时段1: 2025-12 ~ 2026-02")
    r2 = run_period(codes, "20250715", "20250930", "时段2: 2025-08 ~ 2025-09")

    # 找两轮都显著的因子
    sig1 = {r["factor"] + "_" + r["direction"] for r in r1 if r["p_value"] < 0.05}
    sig2 = {r["factor"] + "_" + r["direction"] for r in r2 if r["p_value"] < 0.05}
    both = sig1 & sig2

    print(f"\n{'=' * 72}")
    print(f"  两轮都 p<0.05 的因子: {both if both else '无'}")
    print(f"  仅第1轮显著: {sig1 - sig2 if sig1 - sig2 else '无'}")
    print(f"  仅第2轮显著: {sig2 - sig1 if sig2 - sig1 else '无'}")
    print(f"{'=' * 72}")


if __name__ == "__main__":
    main()
