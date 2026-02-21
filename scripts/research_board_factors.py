#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
板块选择因子有效性研究

用 akshare 拉取2025年东方财富概念板块指数日线数据，验证动量板块策略的改进方向。

核心问题：选板块时，哪些因子能预测次日收益？

测试因子：
  F1: 当日涨幅排名 — 涨幅最高的板块次日是涨还是跌？
  F2: 成分股数量 — 小板块(纯度高)是否优于大板块？
  F3: 振幅 — 走势坚决(低振幅) vs 分歧大(高振幅)
  F4: 量能放大 — 成交额相对20日均值放大倍数
  F5: 5日趋势 — 反转启动 vs 追高
  F6: 跳空 vs 盘中拉升

方法：
  1. 获取东方财富概念板块2025年日线数据（板块指数，非个股）
  2. 每个交易日筛选"热门板块"(日涨幅>阈值)
  3. 按因子分组，比较次日平均收益、胜率
  4. 模拟不同选板策略的累计收益

数据缓存到 scripts/research_cache/，首次运行约20~30分钟，之后秒出。

Usage:
  uv run python scripts/research_board_factors.py
  uv run python scripts/research_board_factors.py --threshold 0.5
  uv run python scripts/research_board_factors.py --skip-download
"""

import argparse
import io
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

CACHE_DIR = Path(__file__).parent / "research_cache"


# ============================================================
# Phase 1: Data Download (with aggressive caching)
# ============================================================


def get_board_list() -> pd.DataFrame:
    """获取东方财富概念板块列表 (板块名称 + 板块代码)"""
    cache = CACHE_DIR / "em_board_names.csv"
    if cache.exists():
        df = pd.read_csv(cache, dtype=str)
        print(f"  [cache] {len(df)} 个板块")
        return df

    import akshare as ak

    print("  正在获取东方财富概念板块列表...")
    df = ak.stock_board_concept_name_em()
    # columns: 排名, 板块名称, 板块代码, 最新价, ...
    df.to_csv(cache, index=False)
    print(f"  获取 {len(df)} 个板块")
    return df


def filter_junk_boards(df: pd.DataFrame) -> pd.DataFrame:
    """过滤垃圾板块（复用项目里的 board_filter）"""
    from src.strategy.filters.board_filter import is_junk_board

    name_col = "板块名称"
    if name_col not in df.columns:
        # fallback: try to find it
        name_col = next((c for c in df.columns if "名称" in c or c == "name"), None)
    if name_col is None:
        print(f"  WARNING: 找不到板块名称列, columns={list(df.columns)}")
        return df

    before = len(df)
    mask = df[name_col].apply(lambda x: not is_junk_board(str(x)))
    filtered = df[mask].copy()
    print(f"  垃圾板块过滤: {before} → {len(filtered)} (过滤 {before - len(filtered)} 个)")
    return filtered


def download_board_daily(board_name: str) -> pd.DataFrame | None:
    """下载单个板块的日线数据 (东方财富, 按板块名称查询)"""
    # Use board name as cache key (sanitize for filename)
    safe_name = board_name.replace("/", "_").replace("\\", "_").replace(":", "_")
    cache = CACHE_DIR / f"board_daily_{safe_name}.csv"
    if cache.exists():
        df = pd.read_csv(cache)
        return df if len(df) > 0 else None

    import akshare as ak

    try:
        df = ak.stock_board_concept_hist_em(
            symbol=board_name,
            period="daily",
            start_date="20250101",
            end_date="20260201",
            adjust="",
        )
        if df is not None and len(df) > 0:
            df.to_csv(cache, index=False)
            return df
        pd.DataFrame().to_csv(cache, index=False)
        return None
    except Exception:
        pd.DataFrame().to_csv(cache, index=False)
        return None
    finally:
        time.sleep(0.25)


def download_cons_count(board_name: str) -> int:
    """获取板块成分股数量 (东方财富, 按板块名称查询)"""
    safe_name = board_name.replace("/", "_").replace("\\", "_").replace(":", "_")
    cache = CACHE_DIR / f"cons_count_{safe_name}.txt"
    if cache.exists():
        try:
            return int(cache.read_text().strip())
        except ValueError:
            return 0

    import akshare as ak

    try:
        df = ak.stock_board_concept_cons_em(symbol=board_name)
        count = len(df) if df is not None else 0
    except Exception:
        count = 0
    cache.write_text(str(count))
    time.sleep(0.25)
    return count


def download_all_data() -> dict:
    """下载所有板块的日线数据和成分股数量"""
    print("\n=== Phase 1: 数据下载 ===")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    boards = get_board_list()
    boards = filter_junk_boards(boards)

    # Find name column
    name_col = "板块名称"
    if name_col not in boards.columns:
        name_col = next((c for c in boards.columns if "名称" in c or c == "name"), None)
    if name_col is None:
        print(f"  ERROR: 找不到板块名称列, columns={list(boards.columns)}")
        return {}

    board_names = boards[name_col].dropna().unique().tolist()
    total = len(board_names)

    # 1) Download daily OHLCV
    print(f"\n  下载 {total} 个板块日线数据...")
    all_data: dict = {}
    for i, name in enumerate(board_names):
        pct = (i + 1) / total * 100
        print(f"\r  [{i+1}/{total}] ({pct:.0f}%) {name:<20}", end="", flush=True)

        df = download_board_daily(name)
        if df is not None and len(df) > 10:
            all_data[name] = {"name": name, "daily": df}

    print(f"\n  成功加载 {len(all_data)} 个板块")

    # 2) Download constituent counts
    print(f"  获取板块成分股数量...")
    for i, (name, info) in enumerate(all_data.items()):
        pct = (i + 1) / len(all_data) * 100
        print(f"\r  [{i+1}/{len(all_data)}] ({pct:.0f}%)", end="", flush=True)
        info["cons_count"] = download_cons_count(name)
    print()

    # Save metadata for --skip-download
    meta = {name: {"cons_count": info["cons_count"]} for name, info in all_data.items()}
    (CACHE_DIR / "board_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2))
    print(f"  元数据已保存到 {CACHE_DIR / 'board_meta.json'}")

    return all_data


def load_from_cache() -> dict:
    """从缓存加载数据"""
    meta_file = CACHE_DIR / "board_meta.json"
    if not meta_file.exists():
        print("  ERROR: 缓存不存在, 请先不带 --skip-download 运行")
        return {}

    meta = json.loads(meta_file.read_text())
    all_data: dict = {}
    loaded = 0
    for name, info in meta.items():
        safe_name = name.replace("/", "_").replace("\\", "_").replace(":", "_")
        cache = CACHE_DIR / f"board_daily_{safe_name}.csv"
        if cache.exists():
            df = pd.read_csv(cache)
            if len(df) > 0:
                all_data[name] = {
                    "name": name,
                    "daily": df,
                    "cons_count": info.get("cons_count", 0),
                }
                loaded += 1

    print(f"  [cache] 加载 {loaded} 个板块")
    return all_data


# ============================================================
# Phase 2: Build Panel Dataset
# ============================================================


def build_panel(all_data: dict) -> pd.DataFrame:
    """构建面板数据: (日期, 板块) → 各种指标 + 次日收益"""
    print("\n=== Phase 2: 构建面板数据 ===")
    rows = []

    for key, info in all_data.items():
        df = info["daily"].copy()
        name = info["name"]
        cons_count = info.get("cons_count", 0)

        # EM columns: 日期, 开盘, 收盘, 最高, 最低, 涨跌幅, 涨跌额, 成交量, 成交额, 振幅, 换手率
        col_map = {}
        for col in df.columns:
            cl = str(col)
            if cl == "日期":
                col_map[col] = "date"
            elif cl == "开盘":
                col_map[col] = "open"
            elif cl == "最高":
                col_map[col] = "high"
            elif cl == "最低":
                col_map[col] = "low"
            elif cl == "收盘":
                col_map[col] = "close"
            elif cl == "成交量":
                col_map[col] = "volume"
            elif cl == "成交额":
                col_map[col] = "amount"
            elif cl == "涨跌幅":
                col_map[col] = "pct_change"
            elif cl == "振幅":
                col_map[col] = "amplitude_raw"
            elif cl == "换手率":
                col_map[col] = "turnover"
        df = df.rename(columns=col_map)

        if "close" not in df.columns or "date" not in df.columns:
            continue

        df["date"] = pd.to_datetime(df["date"])
        for col in ["open", "high", "low", "close", "volume", "amount", "pct_change", "amplitude_raw", "turnover"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.sort_values("date").reset_index(drop=True)

        # 基础指标
        df["prev_close"] = df["close"].shift(1)
        # 日涨幅: 优先用EM自带的涨跌幅, 否则自算
        if "pct_change" in df.columns:
            df["daily_return"] = df["pct_change"]
        else:
            df["daily_return"] = (df["close"] - df["prev_close"]) / df["prev_close"] * 100

        df["intraday_return"] = (df["close"] - df["open"]) / df["open"] * 100
        df["open_gap"] = (df["open"] - df["prev_close"]) / df["prev_close"] * 100

        # 振幅: 优先EM自带, 否则自算
        if "amplitude_raw" in df.columns:
            df["amplitude"] = df["amplitude_raw"]
        else:
            df["amplitude"] = (df["high"] - df["low"]) / df["open"] * 100

        # 次日收益
        df["next_day_return"] = df["daily_return"].shift(-1)
        df["next_open"] = df["open"].shift(-1)
        df["next_open_return"] = (df["next_open"] - df["close"]) / df["close"] * 100

        # 量能放大 (成交额 vs 20日均值)
        if "amount" in df.columns:
            df["amt_ma20"] = df["amount"].rolling(20, min_periods=5).mean()
            df["vol_ratio"] = df["amount"] / df["amt_ma20"]
        else:
            df["vol_ratio"] = np.nan

        # 5日趋势
        df["trend_5d"] = df["close"].pct_change(5) * 100

        for _, row in df.iterrows():
            if pd.isna(row.get("daily_return")) or pd.isna(row.get("next_day_return")):
                continue
            rows.append(
                {
                    "date": row["date"],
                    "board_name": name,
                    "cons_count": cons_count,
                    "daily_return": row["daily_return"],
                    "intraday_return": row.get("intraday_return"),
                    "open_gap": row.get("open_gap"),
                    "amplitude": row.get("amplitude"),
                    "vol_ratio": row.get("vol_ratio"),
                    "trend_5d": row.get("trend_5d"),
                    "next_day_return": row["next_day_return"],
                    "next_open_return": row.get("next_open_return"),
                }
            )

    panel = pd.DataFrame(rows)
    panel["date"] = pd.to_datetime(panel["date"])

    # 限定2025年
    panel = panel[(panel["date"] >= "2025-01-01") & (panel["date"] <= "2025-12-31")]

    print(
        f"  面板数据: {len(panel)} 行, "
        f"{panel['board_name'].nunique()} 个板块, "
        f"{panel['date'].nunique()} 个交易日"
    )
    return panel


# ============================================================
# Phase 3: Factor Analysis
# ============================================================


def print_group_stats(sub: pd.DataFrame, label: str, extra: str = ""):
    """打印一个分组的统计指标"""
    if len(sub) == 0:
        return
    avg = sub["next_day_return"].mean()
    med = sub["next_day_return"].median()
    wr = (sub["next_day_return"] > 0).mean() * 100
    std = sub["next_day_return"].std()
    print(
        f"  {label:20s}: 均值 {avg:+.3f}%, 中位数 {med:+.3f}%, "
        f"胜率 {wr:.1f}%, 波动 {std:.2f}%, n={len(sub)} {extra}"
    )


def analyze_factors(panel: pd.DataFrame, threshold: float = 1.0):
    """分析各因子对次日收益的预测能力"""

    hot = panel[panel["daily_return"] > threshold].copy()

    n_days = hot["date"].nunique()
    if n_days == 0:
        print(f"\n  ERROR: 没有日涨幅>{threshold}%的板块数据")
        return

    print(f"\n  热门板块筛选: 日涨幅>{threshold}%")
    print(f"  总记录: {len(hot)}, 交易日: {n_days}, 板块: {hot['board_name'].nunique()}")
    print(f"  日均热门板块数: {len(hot) / n_days:.1f}")

    # ------ 基准 ------
    print(f"\n  {'─' * 60}")
    print(f"  基准 (所有日涨幅>{threshold}%的板块)")
    print_group_stats(hot, "全部热门板块")

    # =========================================================
    # F1: 当日涨幅排名
    # =========================================================
    _section("F1: 当日涨幅因子", "涨幅最高的热门板块，次日还能继续涨吗？")

    try:
        hot["return_q"] = pd.qcut(
            hot["daily_return"],
            q=3,
            labels=["低涨幅(1/3)", "中涨幅(2/3)", "高涨幅(3/3)"],
            duplicates="drop",
        )
        for q in ["高涨幅(3/3)", "中涨幅(2/3)", "低涨幅(1/3)"]:
            sub = hot[hot["return_q"] == q]
            avg_today = sub["daily_return"].mean()
            print_group_stats(sub, q, f"当日均涨 {avg_today:+.2f}%")
    except ValueError:
        print("  (分位数计算失败, 数据不足)")

    hot["day_rank"] = hot.groupby("date")["daily_return"].rank(ascending=False, method="first")
    for label, lo, hi in [("Top-1", 1, 1), ("Top-2~3", 2, 3), ("Top-4~10", 4, 10), ("11名之后", 11, 999)]:
        sub = hot[(hot["day_rank"] >= lo) & (hot["day_rank"] <= hi)]
        print_group_stats(sub, label)

    # =========================================================
    # F2: 成分股数量（板块纯度）
    # =========================================================
    _section("F2: 板块纯度因子", "成分股少的小而精板块, 板块效应更强?")

    hot_cons = hot[hot["cons_count"] > 0].copy()
    if len(hot_cons) > 0:
        bins = [0, 20, 50, 100, 200, 9999]
        labels = ["极小(≤20)", "小(21~50)", "中(51~100)", "大(101~200)", "超大(>200)"]
        hot_cons["size_group"] = pd.cut(hot_cons["cons_count"], bins=bins, labels=labels)
        for g in labels:
            sub = hot_cons[hot_cons["size_group"] == g]
            avg_cons = sub["cons_count"].mean() if len(sub) > 0 else 0
            print_group_stats(sub, g, f"均{avg_cons:.0f}只")

    # =========================================================
    # F3: 振幅
    # =========================================================
    _section("F3: 振幅因子", "涨得坚决(低振幅) vs 分歧大(高振幅)")

    hot_amp = hot.dropna(subset=["amplitude"]).copy()
    if len(hot_amp) > 0:
        try:
            hot_amp["amp_q"] = pd.qcut(
                hot_amp["amplitude"],
                q=3,
                labels=["低振幅(坚决)", "中振幅", "高振幅(分歧)"],
                duplicates="drop",
            )
            for q in ["低振幅(坚决)", "中振幅", "高振幅(分歧)"]:
                sub = hot_amp[hot_amp["amp_q"] == q]
                avg_amp = sub["amplitude"].mean()
                print_group_stats(sub, q, f"均振幅 {avg_amp:.2f}%")
        except ValueError:
            print("  (分位数计算失败)")

    # =========================================================
    # F4: 量能放大
    # =========================================================
    _section("F4: 量能放大因子", "放量上涨的板块次日更强？")

    hot_vol = hot.dropna(subset=["vol_ratio"]).copy()
    hot_vol = hot_vol[hot_vol["vol_ratio"] > 0]
    if len(hot_vol) > 0:
        try:
            hot_vol["vol_q"] = pd.qcut(
                hot_vol["vol_ratio"],
                q=3,
                labels=["缩量", "平量", "放量"],
                duplicates="drop",
            )
            for q in ["放量", "平量", "缩量"]:
                sub = hot_vol[hot_vol["vol_q"] == q]
                avg_vr = sub["vol_ratio"].mean()
                print_group_stats(sub, q, f"量比 {avg_vr:.2f}x")
        except ValueError:
            print("  (分位数计算失败)")

    # =========================================================
    # F5: 5日趋势
    # =========================================================
    _section("F5: 5日趋势因子", "前5日下跌后启动(反转) vs 已涨5日再涨(追高)")

    hot_trend = hot.dropna(subset=["trend_5d"]).copy()
    if len(hot_trend) > 0:
        try:
            hot_trend["trend_q"] = pd.qcut(
                hot_trend["trend_5d"],
                q=3,
                labels=["前5日跌(反转)", "前5日平", "前5日涨(追高)"],
                duplicates="drop",
            )
            for q in ["前5日跌(反转)", "前5日平", "前5日涨(追高)"]:
                sub = hot_trend[hot_trend["trend_q"] == q]
                avg_trend = sub["trend_5d"].mean()
                print_group_stats(sub, q, f"5日趋势 {avg_trend:+.2f}%")
        except ValueError:
            print("  (分位数计算失败)")

    # =========================================================
    # F6: 跳空 vs 盘中拉升
    # =========================================================
    _section("F6: 跳空 vs 盘中拉升", "高开高走 vs 低开拉升，哪种更持续？")

    hot_gap = hot.dropna(subset=["open_gap", "intraday_return"]).copy()
    if len(hot_gap) > 0:
        med_gap = hot_gap["open_gap"].median()
        med_intra = hot_gap["intraday_return"].median()

        groups = {
            "高开+盘中强": hot_gap[(hot_gap["open_gap"] > med_gap) & (hot_gap["intraday_return"] > med_intra)],
            "低开+盘中强": hot_gap[(hot_gap["open_gap"] <= med_gap) & (hot_gap["intraday_return"] > med_intra)],
            "高开+盘中弱": hot_gap[(hot_gap["open_gap"] > med_gap) & (hot_gap["intraday_return"] <= med_intra)],
            "低开+盘中弱": hot_gap[(hot_gap["open_gap"] <= med_gap) & (hot_gap["intraday_return"] <= med_intra)],
        }
        for label, sub in groups.items():
            print_group_stats(sub, label)

    # =========================================================
    # Strategy Simulation
    # =========================================================
    _section("策略模拟", "每天选板块，比较累计收益")

    daily_groups = hot.groupby("date")
    strategies: dict[str, list[float]] = {
        "S1: 涨幅Top-1": [],
        "S2: Top-5中小板块优先": [],
        "S3: Top-3均仓": [],
        "S4: 随机选1(baseline)": [],
    }
    strategy_dates: list = []

    for dt, group in daily_groups:
        if len(group) < 2:
            continue
        strategy_dates.append(dt)
        ranked = group.sort_values("daily_return", ascending=False)

        # S1: Top-1
        strategies["S1: 涨幅Top-1"].append(ranked.iloc[0]["next_day_return"])

        # S2: Top-5中成分股最少的
        top5 = ranked.head(5)
        top5_cons = top5[top5["cons_count"] > 0]
        if len(top5_cons) > 0:
            best = top5_cons.sort_values("cons_count").iloc[0]
        else:
            best = ranked.iloc[0]
        strategies["S2: Top-5中小板块优先"].append(best["next_day_return"])

        # S3: Top-3均仓
        top3 = ranked.head(min(3, len(ranked)))
        strategies["S3: Top-3均仓"].append(top3["next_day_return"].mean())

        # S4: 随机
        seed = int(pd.Timestamp(dt).timestamp()) % (2**31)
        strategies["S4: 随机选1(baseline)"].append(
            group.sample(1, random_state=seed)["next_day_return"].iloc[0]
        )

    print(f"  模拟交易日数: {len(strategy_dates)}\n")

    for sname, returns in strategies.items():
        s = pd.Series(returns)
        cum = (1 + s / 100).prod() - 1
        avg = s.mean()
        med = s.median()
        wr = (s > 0).mean() * 100
        std = s.std()
        sharpe = avg / std * np.sqrt(250) if std > 0 else 0
        max_dd = (s.cumsum() - s.cumsum().cummax()).min()
        print(
            f"  {sname:26s}: "
            f"累计 {cum * 100:+.1f}%, 日均 {avg:+.3f}%, "
            f"胜率 {wr:.1f}%, 夏普 {sharpe:.2f}, 最大回撤 {max_dd:.2f}%"
        )

    # =========================================================
    # 多因子组合
    # =========================================================
    _section("综合: 因子组合", "多因子交叉效果")

    combo = hot.dropna(subset=["vol_ratio", "trend_5d"]).copy()
    combo = combo[combo["cons_count"] > 0]
    combo = combo[combo["vol_ratio"] > 0]

    if len(combo) > 50:
        med_ret = combo["daily_return"].median()
        med_vol = combo["vol_ratio"].median()

        print_group_stats(
            combo[(combo["cons_count"] <= 50) & (combo["vol_ratio"] > med_vol) & (combo["daily_return"] > med_ret)],
            "小板块+放量+高涨幅",
        )
        print_group_stats(
            combo[(combo["cons_count"] > 100) & (combo["vol_ratio"] < med_vol)],
            "大板块+缩量",
        )
        print_group_stats(
            combo[(combo["trend_5d"] < 0) & (combo["vol_ratio"] > med_vol)],
            "反转+放量",
        )
        print_group_stats(
            combo[(combo["trend_5d"] > 5) & (combo["vol_ratio"] < med_vol)],
            "追高+缩量",
        )

    # =========================================================
    # 极端案例
    # =========================================================
    _section("极端案例", "人工复盘参考")

    if strategy_dates:
        top1_detail = []
        for dt, group in daily_groups:
            if len(group) < 2:
                continue
            best = group.sort_values("daily_return", ascending=False).iloc[0]
            top1_detail.append(
                {
                    "date": dt,
                    "board": best["board_name"],
                    "cons_count": int(best["cons_count"]),
                    "today_return": best["daily_return"],
                    "next_return": best["next_day_return"],
                }
            )
        detail_df = pd.DataFrame(top1_detail)

        if len(detail_df) > 0:
            print("\n  Top-1策略 最赚钱5天:")
            for _, r in detail_df.nlargest(5, "next_return").iterrows():
                d = pd.Timestamp(r["date"]).strftime("%Y-%m-%d")
                print(
                    f"    {d} {r['board']:<14} ({r['cons_count']:>3}只) "
                    f"当日 {r['today_return']:+.2f}% → 次日 {r['next_return']:+.2f}%"
                )

            print("\n  Top-1策略 最亏钱5天:")
            for _, r in detail_df.nsmallest(5, "next_return").iterrows():
                d = pd.Timestamp(r["date"]).strftime("%Y-%m-%d")
                print(
                    f"    {d} {r['board']:<14} ({r['cons_count']:>3}只) "
                    f"当日 {r['today_return']:+.2f}% → 次日 {r['next_return']:+.2f}%"
                )


def _section(title: str, subtitle: str):
    """Print a section header."""
    print(f"\n{'=' * 65}")
    print(f"  {title}")
    print(f"  问: {subtitle}")
    print(f"{'=' * 65}")


# ============================================================
# Main
# ============================================================


def main():
    parser = argparse.ArgumentParser(description="板块选择因子有效性研究")
    parser.add_argument("--threshold", "-t", type=float, default=1.0, help="热门板块日涨幅阈值(%%), 默认1.0")
    parser.add_argument("--skip-download", action="store_true", help="跳过下载, 只跑分析(需已有缓存)")
    args = parser.parse_args()

    if args.skip_download:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        all_data = load_from_cache()
    else:
        all_data = download_all_data()

    if not all_data:
        print("\nERROR: 没有数据可分析")
        return

    panel = build_panel(all_data)
    if panel.empty:
        print("\nERROR: 面板数据为空")
        return

    analyze_factors(panel, threshold=args.threshold)

    print(f"\n\n{'=' * 65}")
    print(f"  分析完成")
    print(f"  数据缓存: {CACHE_DIR}")
    print(f"  改参数重跑: uv run python scripts/research_board_factors.py --skip-download -t 0.5")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    main()
