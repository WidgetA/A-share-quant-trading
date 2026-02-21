"""早盘 top15% vs 全天 top15% 一致性验证。

不用任何估算公式/绝对阈值，直接比较:
- 全天量比 top15% vs 早盘量比 top15%
- 全天换手率 top15% vs 早盘成交量 top15%
- AND 组合的一致率

按天分组做百分位排名，验证早盘排名能否代表全天排名。
"""

import random
import time
from datetime import time as dtime

import akshare as ak
import numpy as np
import pandas as pd

random.seed(2026)

stock_list = ak.stock_info_a_code_name()
mask = stock_list["code"].str.match(r"^(60\d{4}|00[0-3]\d{3})$")
codes = stock_list[mask]["code"].tolist()
sample_codes = random.sample(codes, 200)

print("=== 早盘top15% vs 全天top15% 一致性验证 ===\n")

rows = []
for i, code in enumerate(sample_codes):
    try:
        daily = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date="20260101",
            end_date="20260218",
            adjust="qfq",
        )
        if daily is None or len(daily) < 25:
            continue
        minute = ak.stock_zh_a_hist_min_em(symbol=code, period="1", adjust="qfq")
        if minute is None or len(minute) == 0:
            continue

        daily = daily.rename(
            columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "换手率": "turnover_pct",
            }
        )
        daily["date"] = pd.to_datetime(daily["date"]).dt.date
        daily["prev_close"] = daily["close"].shift(1)
        daily["next_open"] = daily["open"].shift(-1)
        daily["avg_vol_20d"] = daily["volume"].shift(1).rolling(20, min_periods=5).mean()

        minute["datetime"] = pd.to_datetime(minute["时间"])
        minute["date"] = minute["datetime"].dt.date
        minute["time"] = minute["datetime"].dt.time
        minute_dates = set(minute["date"].unique())

        for _, row in daily.iterrows():
            d = row["date"]
            if d not in minute_dates:
                continue
            if pd.isna(row["prev_close"]) or pd.isna(row["next_open"]):
                continue
            if pd.isna(row["avg_vol_20d"]) or row["avg_vol_20d"] <= 0:
                continue
            open_gap = (row["open"] - row["prev_close"]) / row["prev_close"] * 100
            if open_gap < 1.0:
                continue

            day_min = minute[minute["date"] == d]
            early = day_min[(day_min["time"] >= dtime(9, 31)) & (day_min["time"] <= dtime(9, 40))]
            early_vol = early["成交量"].sum()

            overnight = (row["next_open"] - row["close"]) / row["close"] * 100
            vol_ratio = row["volume"] / row["avg_vol_20d"]
            early_vol_ratio = early_vol / row["avg_vol_20d"]

            rows.append(
                {
                    "code": code,
                    "date": str(d),
                    "overnight": overnight,
                    "vol_ratio": vol_ratio,
                    "early_vol_ratio": early_vol_ratio,
                    "turnover_pct": row["turnover_pct"],
                    "early_vol": early_vol,
                }
            )
    except Exception:
        pass
    if (i + 1) % 50 == 0:
        print(f"  进度: {i + 1}/200, 高开样本: {len(rows)}")
    time.sleep(0.03)

df = pd.DataFrame(rows)
print(f"\n高开>=1%样本: {len(df)} 个")

# 按天分组做 top15% 排名
days = df.groupby("date")
day_results = []

for date_str, day_df in days:
    n = len(day_df)
    if n < 10:
        continue

    # 全天 top15%
    full_vol_top = day_df["vol_ratio"] > day_df["vol_ratio"].quantile(0.85)
    full_turn_top = day_df["turnover_pct"] > day_df["turnover_pct"].quantile(0.85)

    # 早盘 top15%
    early_vol_top = day_df["early_vol_ratio"] > day_df["early_vol_ratio"].quantile(0.85)
    early_turn_top = day_df["early_vol"] > day_df["early_vol"].quantile(0.85)

    day_results.append(
        {
            "date": date_str,
            "n": n,
            "full_vol": full_vol_top.values,
            "early_vol": early_vol_top.values,
            "full_turn": full_turn_top.values,
            "early_turn": early_turn_top.values,
            "full_and": (full_vol_top & full_turn_top).values,
            "early_and": (early_vol_top & early_turn_top).values,
            "overnight": day_df["overnight"].values,
        }
    )

print(f"有效天数: {len(day_results)} (每天>=10个高开样本)")

# 汇总
fv = np.concatenate([r["full_vol"] for r in day_results])
ev = np.concatenate([r["early_vol"] for r in day_results])
ft = np.concatenate([r["full_turn"] for r in day_results])
et = np.concatenate([r["early_turn"] for r in day_results])
fa = np.concatenate([r["full_and"] for r in day_results])
ea = np.concatenate([r["early_and"] for r in day_results])
ov = np.concatenate([r["overnight"] for r in day_results])
N = len(ov)

print(f"汇总样本: {N}\n")

print("=== 信号1: 量比 top15% ===")
print(f"  一致率: {(fv == ev).mean():.1%}")
print(f"  都是top15%: {(fv & ev).sum()}, 仅全天: {(fv & ~ev).sum()}, 仅早盘: {(~fv & ev).sum()}")

print("\n=== 信号2: 换手 top15% ===")
print(f"  一致率: {(ft == et).mean():.1%}")
print(f"  都是top15%: {(ft & et).sum()}, 仅全天: {(ft & ~et).sum()}, 仅早盘: {(~ft & et).sum()}")

print("\n=== AND组合 ===")
print(f"  一致率: {(fa == ea).mean():.1%}")
print(
    f"  都过滤: {(fa & ea).sum()}, "
    f"仅全天: {(fa & ~ea).sum()}, "
    f"仅早盘: {(~fa & ea).sum()}, "
    f"都不过滤: {(~fa & ~ea).sum()}"
)

# 各组次日收益 + permutation test
print("\n=== 各组次日收益 (permutation test) ===")
rng = np.random.default_rng(42)
n_sim = 10000

groups = [
    ("全天AND过滤", fa),
    ("早盘AND过滤", ea),
    ("都过滤(交集)", fa & ea),
    ("仅早盘多抓", ~fa & ea),
    ("仅全天漏抓", fa & ~ea),
]

for label, m in groups:
    k = int(m.sum())
    if k < 3:
        print(f"  {label}: n={k} (太少)")
        continue
    mr = ov[m].mean()
    fk = (ov[m] > 0).mean()
    if k >= 5:
        rm = np.array([ov[rng.choice(N, size=k, replace=False)].mean() for _ in range(n_sim)])
        p = (rm <= mr).mean()
        sig = "***" if p < 0.01 else "**" if p < 0.05 else ""
        print(f"  {label:<16s}: n={k:>3d}, 次日={mr:+.2f}%, 误杀={fk:.0%}, p={p:.3f} {sig}")
    else:
        print(f"  {label:<16s}: n={k:>3d}, 次日={mr:+.2f}%, 误杀={fk:.0%}")
