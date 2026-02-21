"""
统计 2025 年全年 A 股"高开下杀"的比例。

定义：
- 高开：开盘价 > 昨收（分不同幅度档位统计）
- 下杀：收盘价 < 昨收（高开了但最终收绿）

统计维度：
- 分母：所有股票×交易日（stock-day 总数）
- 分子：符合"高开下杀"条件的 stock-day 数量
"""

import random
import time

import akshare as ak
import pandas as pd


def get_all_stock_codes() -> list[str]:
    """获取全部 A 股代码，排除 ST 和北交所。"""
    df = ak.stock_zh_a_spot_em()
    codes = df["代码"].tolist()
    names = df["名称"].tolist()

    filtered = []
    for code, name in zip(codes, names):
        # 排除北交所 (8xxxxx, 4xxxxx)
        if code.startswith("8") or code.startswith("4"):
            continue
        # 排除 ST
        if "ST" in name or "st" in name:
            continue
        filtered.append(code)

    return filtered


def fetch_stock_data(code: str) -> pd.DataFrame | None:
    """获取单只股票 2025 年日线数据。"""
    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date="20250101",
            end_date="20251231",
            adjust="qfq",
        )
        if df is None or len(df) < 10:
            return None
        return df
    except Exception:
        return None


def analyze(sample_size: int = 500) -> None:
    print("正在获取 A 股代码列表...")
    all_codes = get_all_stock_codes()
    print(f"共 {len(all_codes)} 只股票（排除 ST 和北交所）")

    # 抽样
    if len(all_codes) > sample_size:
        codes = random.sample(all_codes, sample_size)
        print(f"随机抽样 {sample_size} 只")
    else:
        codes = all_codes

    all_rows = []
    success = 0
    fail = 0

    for i, code in enumerate(codes):
        df = fetch_stock_data(code)
        if df is not None and len(df) > 1:
            # 计算昨收
            df = df.copy()
            df["prev_close"] = df["收盘"].shift(1)
            df = df.iloc[1:].copy()  # 去掉第一行（无昨收）

            df["code"] = code
            df["open"] = df["开盘"]
            df["close"] = df["收盘"]
            df["high"] = df["最高"]
            df["low"] = df["最低"]

            all_rows.append(df[["code", "日期", "open", "close", "high", "low", "prev_close"]])
            success += 1
        else:
            fail += 1

        if (i + 1) % 50 == 0:
            print(f"  进度: {i + 1}/{len(codes)} (成功 {success}, 失败 {fail})")
        time.sleep(0.05)

    print(f"\n数据获取完成: 成功 {success} 只, 失败 {fail} 只")

    # 合并
    data = pd.concat(all_rows, ignore_index=True)
    total_stock_days = len(data)
    print(f"总 stock-day 数: {total_stock_days:,}")

    # ========== 统计 ==========

    # 基础指标
    data["gap_pct"] = (data["open"] - data["prev_close"]) / data["prev_close"] * 100
    data["gap_up"] = data["open"] > data["prev_close"]
    data["close_below_prev"] = data["close"] < data["prev_close"]
    data["close_below_open"] = data["close"] < data["open"]
    data["intraday_drop_pct"] = (data["close"] - data["open"]) / data["open"] * 100

    print("\n" + "=" * 70)
    print("一、总体高开比例")
    print("=" * 70)

    gap_up_count = data["gap_up"].sum()
    print(
        f"高开（open > 昨收）: {gap_up_count:,} / {total_stock_days:,} = {gap_up_count / total_stock_days * 100:.1f}%"
    )

    for threshold in [1, 2, 3, 5]:
        cnt = (data["gap_pct"] > threshold).sum()
        print(
            f"高开 > {threshold}%:  {cnt:,} / {total_stock_days:,} = {cnt / total_stock_days * 100:.1f}%"
        )

    print("\n" + "=" * 70)
    print("二、高开下杀（高开后收盘 < 昨收，即高开收绿）")
    print("=" * 70)

    gap_up_data = data[data["gap_up"]].copy()
    gap_up_total = len(gap_up_data)
    gap_up_drop = gap_up_data["close_below_prev"].sum()

    print("\n全部高开中：")
    print(f"  高开 stock-day 数: {gap_up_total:,}")
    print(
        f"  高开后收绿:       {gap_up_drop:,} / {gap_up_total:,} = {gap_up_drop / gap_up_total * 100:.1f}%"
    )
    print(
        f"  占全部 stock-day: {gap_up_drop:,} / {total_stock_days:,} = {gap_up_drop / total_stock_days * 100:.1f}%"
    )

    print("\n按高开幅度分档：")
    bins = [(0, 1), (1, 2), (2, 3), (3, 5), (5, 100)]
    for lo, hi in bins:
        label = f"{lo}%-{hi}%" if hi < 100 else f"{lo}%+"
        subset = gap_up_data[(gap_up_data["gap_pct"] > lo) & (gap_up_data["gap_pct"] <= hi)]
        if hi == 100:
            subset = gap_up_data[gap_up_data["gap_pct"] > lo]
        if len(subset) == 0:
            continue
        drop_cnt = subset["close_below_prev"].sum()
        print(
            f"  高开 {label:>8s}: {drop_cnt:,} / {len(subset):,} = {drop_cnt / len(subset) * 100:.1f}% 收绿"
        )

    print("\n" + "=" * 70)
    print("三、高开后盘中回落（高开后收盘 < 开盘，即冲高回落）")
    print("=" * 70)

    gap_up_retreat = gap_up_data["close_below_open"].sum()
    print("\n全部高开中：")
    print(
        f"  高开后收 < 开盘:  {gap_up_retreat:,} / {gap_up_total:,} = {gap_up_retreat / gap_up_total * 100:.1f}%"
    )

    print("\n按高开幅度分档：")
    for lo, hi in bins:
        label = f"{lo}%-{hi}%" if hi < 100 else f"{lo}%+"
        subset = gap_up_data[(gap_up_data["gap_pct"] > lo) & (gap_up_data["gap_pct"] <= hi)]
        if hi == 100:
            subset = gap_up_data[gap_up_data["gap_pct"] > lo]
        if len(subset) == 0:
            continue
        retreat_cnt = subset["close_below_open"].sum()
        avg_drop = subset.loc[subset["close_below_open"], "intraday_drop_pct"].mean()
        avg_drop_str = f"  平均跌 {avg_drop:.1f}%" if pd.notna(avg_drop) else ""
        print(
            f"  高开 {label:>8s}: {retreat_cnt:,} / {len(subset):,} = "
            f"{retreat_cnt / len(subset) * 100:.1f}% 收<开{avg_drop_str}"
        )

    print("\n" + "=" * 70)
    print("四、高开下杀的严重程度（高开后最终跌幅分布）")
    print("=" * 70)

    # 高开后收绿的那些，最终跌了多少？
    bad_cases = gap_up_data[gap_up_data["close_below_prev"]].copy()
    bad_cases["final_drop"] = (
        (bad_cases["close"] - bad_cases["prev_close"]) / bad_cases["prev_close"] * 100
    )
    if len(bad_cases) > 0:
        print(f"\n高开收绿的 {len(bad_cases):,} 个 stock-day 中：")
        for pct in [-1, -2, -3, -5]:
            cnt = (bad_cases["final_drop"] < pct).sum()
            print(f"  收盘跌幅 < {pct}%: {cnt:,} ({cnt / len(bad_cases) * 100:.1f}%)")
        print(f"  平均跌幅: {bad_cases['final_drop'].mean():.2f}%")
        print(f"  中位数跌幅: {bad_cases['final_drop'].median():.2f}%")


if __name__ == "__main__":
    random.seed(42)
    analyze(sample_size=500)
