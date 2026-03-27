"""LGBRank 生产推理模块 — 加载预训练模型，对候选股评分排名。

这是一个 **独立模块**，不依赖任何训练代码。
只需要：
  1. 模型文件: models/lgbrank_latest.txt (LightGBM 原生格式)
  2. 特征列表: models/feature_list.json
  3. 候选股的 StockSnapshot + OHLCV 历史数据

用法:
    scorer = LGBRankScorer("models/lgbrank_latest.txt", "models/feature_list.json")
    ranked = scorer.score_and_rank(snapshots, history_map, avg_market_open_gain)
    # ranked = [(code, name, score, rank), ...] 按分数降序

集成到实盘:
    在 9:40 完成 L1~L6.6 漏斗过滤后，把通过的候选股送入此模块评分。
    取 top-N 买入。
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════
#  特征计算（自包含，不依赖 src.strategy.features）
# ══════════════════════════════════════════════════════════════════════

@dataclass
class CandidateSnapshot:
    """候选股快照 — 生产环境需要填充的字段。

    所有字段含义和单位：
      code:             股票代码 (6位，如 "600519")
      name:             股票名称
      open_price:       今日开盘价 (元)
      prev_close:       昨日收盘价 (元)
      price_at_940:     9:40 实时价 (元)
      high_price:       9:30~9:40 最高价 (元)
      low_price:        9:30~9:40 最低价 (元)
      early_volume:     9:30~9:40 累计成交量 (股，不是手!)
      avg_daily_volume: 过去20日日均成交量 (股)
      trend_pct:        近5个交易日涨幅 (如 0.05 = 5%)
      trend_10d:        近10个交易日涨幅
      avg_daily_return_20d: 过去20日日均收益率
      volatility_20d:   过去20日收益率标准差
      consecutive_up_days: 连续上涨天数 (整数)
    """
    code: str
    name: str
    open_price: float
    prev_close: float
    price_at_940: float
    high_price: float
    low_price: float
    early_volume: float
    avg_daily_volume: float
    trend_pct: float
    trend_10d: float
    avg_daily_return_20d: float
    volatility_20d: float
    consecutive_up_days: int


def _compute_base_features(s: CandidateSnapshot, avg_market_open_gain: float) -> dict:
    """计算 13 个基础特征。"""
    gain_from_open_pct = (s.price_at_940 - s.open_price) / s.open_price if s.open_price > 0 else 0.0

    expected_vol = s.avg_daily_volume * 0.125  # 9:40 应占全天 12.5%
    turnover_amp = s.early_volume / expected_vol if expected_vol > 0 else 0.0

    trend_consistency = s.avg_daily_return_20d / (s.volatility_20d + 0.001)

    open_gap_pct = (s.open_price - s.prev_close) / s.prev_close if s.prev_close > 0 else 0.0

    top = max(s.open_price, s.price_at_940)
    if s.high_price > top and s.open_price > 0:
        upper_shadow = (s.high_price - top) / s.open_price
    else:
        upper_shadow = 0.0

    volume_to_avg = s.early_volume / s.avg_daily_volume if s.avg_daily_volume > 0 else 0.0

    intraday_range_940 = 0.0
    if s.open_price > 0:
        intraday_range_940 = (s.high_price - s.low_price) / s.open_price

    return {
        "gain_from_open_pct": gain_from_open_pct,
        "turnover_amp": turnover_amp,
        "consecutive_up_days": float(s.consecutive_up_days),
        "trend_pct": s.trend_pct,
        "trend_10d": s.trend_10d,
        "avg_daily_return_20d": s.avg_daily_return_20d,
        "volatility_20d": s.volatility_20d,
        "intraday_range_940": intraday_range_940,
        "avg_market_open_gain": avg_market_open_gain,
        "trend_consistency": trend_consistency,
        "open_gap_pct": open_gap_pct,
        "upper_shadow": upper_shadow,
        "volume_to_avg": volume_to_avg,
    }


def _compute_advanced_features(hist: pd.DataFrame) -> dict:
    """计算 14 个高级 OHLCV 特征。

    hist 需要至少 5 行，列: open, high, low, close, volume
    """
    result = {}
    if hist is None or len(hist) < 5:
        return {k: 0.0 for k in [
            "open_pattern_consistency", "vol_price_divergence_5d",
            "intraday_momentum_continuation", "volume_concentration_ratio",
            "relative_strength_vs_high", "gain_consistency_score",
            "amplitude_decay_trend", "turnover_stability_index",
            "close_to_vwap_position", "vol_weighted_gain_ratio",
            "price_channel_pct", "up_days_pct_20d", "amplitude_20d",
            "vol_ratio_5d_20d",
        ]}

    opens = hist["open"].values.astype(float)
    highs = hist["high"].values.astype(float)
    lows = hist["low"].values.astype(float)
    closes = hist["close"].values.astype(float)
    volumes = hist["volume"].values.astype(float)
    n = len(hist)

    # 1. open_pattern_consistency
    ranges = highs - lows + 1e-8
    open_positions = (opens - lows) / ranges
    result["open_pattern_consistency"] = float(1 - np.std(open_positions[-min(20, n):]))

    # 2. vol_price_divergence_5d
    if n >= 10:
        close_5d_ago = closes[-6] if n >= 6 else closes[0]
        price_chg = (closes[-1] - close_5d_ago) / (close_5d_ago + 1e-8)
        vol_recent = volumes[-5:].mean()
        vol_prev = volumes[-10:-5].mean()
        vol_chg = (vol_recent - vol_prev) / (vol_prev + 1e-8)
        result["vol_price_divergence_5d"] = float(vol_chg - price_chg)
    else:
        result["vol_price_divergence_5d"] = 0.0

    # 3. intraday_momentum_continuation
    last10 = min(10, n)
    h, lo, o, c = highs[-last10:], lows[-last10:], opens[-last10:], closes[-last10:]
    r10 = h - lo + 1e-8
    upper_s = np.where(c > o, (h - c) / r10, (h - o) / r10)
    lower_s = np.where(c > o, (o - lo) / r10, (c - lo) / r10)
    result["intraday_momentum_continuation"] = float(lower_s.mean() - upper_s.mean())

    # 4. volume_concentration_ratio
    v20 = volumes[-min(20, n):]
    sorted_v = np.sort(v20)[::-1]
    top_n = max(1, int(len(v20) * 0.1))
    result["volume_concentration_ratio"] = float(1 - sorted_v[:top_n].sum() / (v20.sum() + 1e-8))

    # 5. relative_strength_vs_high
    high_20d = highs[-min(20, n):].max()
    result["relative_strength_vs_high"] = float(closes[-1] / (high_20d + 1e-8))

    # 6. gain_consistency_score
    if n >= 3:
        daily_rets = np.diff(closes) / (closes[:-1] + 1e-8)
        rets_20 = daily_rets[-min(20, len(daily_rets)):]
        cv = np.std(rets_20) / (np.abs(np.mean(rets_20)) + 1e-6)
        result["gain_consistency_score"] = float(1 / (1 + cv))
    else:
        result["gain_consistency_score"] = 0.0

    # 7. amplitude_decay_trend
    if n >= 5:
        amps = (highs[-min(10, n):] - lows[-min(10, n):]) / (lows[-min(10, n):] + 1e-8)
        x = np.arange(len(amps))
        if len(amps) >= 3:
            slope = np.polyfit(x, amps, 1)[0]
            result["amplitude_decay_trend"] = float(-slope * 10)
        else:
            result["amplitude_decay_trend"] = 0.0
    else:
        result["amplitude_decay_trend"] = 0.0

    # 8. turnover_stability_index
    if n >= 5:
        v_recent = volumes[-min(20, n):]
        vol_changes = np.diff(v_recent) / (v_recent[:-1] + 1e-8)
        result["turnover_stability_index"] = float(max(0, 1 - np.std(vol_changes)))
    else:
        result["turnover_stability_index"] = 0.0

    # 9. close_to_vwap_position
    if n >= 5:
        c5, h5, l5, v5 = closes[-5:], highs[-5:], lows[-5:], volumes[-5:]
        typical = (h5 + l5 + c5) / 3
        vwap = (typical * v5).sum() / (v5.sum() + 1e-8)
        result["close_to_vwap_position"] = float((closes[-1] - vwap) / (vwap + 1e-8))
    else:
        result["close_to_vwap_position"] = 0.0

    # 10. vol_weighted_gain_ratio
    if n >= 3:
        dr = np.diff(closes) / (closes[:-1] + 1e-8)
        dr10 = dr[-min(10, len(dr)):]
        vr10 = volumes[-len(dr10):]
        result["vol_weighted_gain_ratio"] = float((dr10 * vr10).sum() / (vr10.sum() + 1e-8) * 100)
    else:
        result["vol_weighted_gain_ratio"] = 0.0

    # 11. price_channel_pct
    low_20d = lows[-min(20, n):].min()
    result["price_channel_pct"] = float((closes[-1] - low_20d) / (high_20d - low_20d + 1e-8))

    # 12. up_days_pct_20d
    if n >= 3:
        dr_all = np.diff(closes[-min(21, n):])
        result["up_days_pct_20d"] = float((dr_all > 0).sum() / (len(dr_all) + 1e-8))
    else:
        result["up_days_pct_20d"] = 0.0

    # 13. amplitude_20d
    amps_20 = (highs[-min(20, n):] - lows[-min(20, n):]) / (opens[-min(20, n):] + 1e-8)
    result["amplitude_20d"] = float(amps_20.mean())

    # 14. vol_ratio_5d_20d
    if n >= 10:
        vol_5d = volumes[-5:].mean()
        vol_20d = volumes[-min(20, n):].mean()
        result["vol_ratio_5d_20d"] = float(vol_5d / (vol_20d + 1e-8))
    else:
        result["vol_ratio_5d_20d"] = 1.0

    return result


def _compute_engineered_features(feats: dict) -> dict:
    """计算 11 个衍生特征。"""
    def _g(key):
        return feats.get(key, 0.0)

    return {
        "momentum_contrarian": -_g("avg_daily_return_20d") * (1 + _g("consecutive_up_days") / 10),
        "trend_acceleration": _g("trend_10d") - _g("trend_pct"),
        "momentum_quality": (
            _g("trend_10d") / (_g("volatility_20d") + 0.001)
            * max(0, _g("avg_daily_return_20d"))
        ),
        "vol_normalized_return": _g("avg_daily_return_20d") / (_g("volatility_20d") + 0.001),
        "range_to_vol": _g("intraday_range_940") / (_g("volatility_20d") + 0.001),
        "volume_price_efficiency": _g("gain_from_open_pct") / (_g("volume_to_avg") + 0.001),
        "gap_to_trend": _g("open_gap_pct") / (abs(_g("trend_10d")) + 0.001),
        "gap_quality": _g("open_gap_pct") / (_g("consecutive_up_days") + 1),
        "trend_x_vol": _g("trend_10d") * _g("volatility_20d"),
        "consec_x_range": _g("consecutive_up_days") * _g("intraday_range_940"),
        "return_x_turnover": _g("avg_daily_return_20d") * _g("turnover_amp"),
    }


def _add_zscore(feature_dicts: list[dict], raw_names: list[str]) -> None:
    """给一批候选股的特征字典添加 z-score 归一化版本（当日池内归一化）。"""
    if not feature_dicts:
        return
    for fname in raw_names:
        vals = np.array([d.get(fname, 0.0) for d in feature_dicts])
        std = vals.std()
        z_vals = (vals - vals.mean()) / (std + 1e-10) if std > 1e-10 else np.zeros(len(vals))
        z_name = f"z_{fname}"
        for d, z in zip(feature_dicts, z_vals):
            d[z_name] = float(z)


# ══════════════════════════════════════════════════════════════════════
#  LGBRankScorer 主类
# ══════════════════════════════════════════════════════════════════════

@dataclass
class ScoredStock:
    """评分结果"""
    code: str
    name: str
    score: float
    rank: int           # 1-based, 1 = best
    buy_price: float    # 9:40 价格


class LGBRankScorer:
    """LGBRank 生产评分器。

    加载预训练的 LightGBM LambdaRank 模型，对漏斗通过的候选股评分排名。

    Args:
        model_path: LightGBM 模型文件路径 (.txt 格式)
        feature_list_path: 特征列表 JSON 文件路径
    """

    def __init__(self, model_path: str | Path, feature_list_path: str | Path):
        model_path = Path(model_path)
        feature_list_path = Path(feature_list_path)

        if not model_path.exists():
            raise FileNotFoundError(f"Model file not found: {model_path}")
        if not feature_list_path.exists():
            raise FileNotFoundError(f"Feature list not found: {feature_list_path}")

        # 加载模型
        self.model = lgb.Booster(model_file=str(model_path))
        size_kb = model_path.stat().st_size / 1024
        logger.info(f"[LGBRank] Loaded model: {model_path.name} ({size_kb:.1f} KB)")

        # 加载特征列表（顺序敏感！）
        with open(feature_list_path, "r", encoding="utf-8") as f:
            feat_info = json.load(f)
        self.features: list[str] = feat_info["features"]
        self.raw_features: list[str] = feat_info["raw_features"]
        logger.info(f"[LGBRank] {len(self.features)} features loaded")

    def score_and_rank(
        self,
        candidates: list[CandidateSnapshot],
        history_map: dict[str, pd.DataFrame],
        avg_market_open_gain: float,
    ) -> list[ScoredStock]:
        """对候选股评分并按分数降序排名。

        Args:
            candidates: 通过 L1~L6.6 漏斗的候选股快照列表
            history_map: {code: DataFrame} 每只股票的近 20~40 天 OHLCV 日线
                         DataFrame 列: open, high, low, close, volume
            avg_market_open_gain: 当日全市场平均开盘涨幅
                (sum of (open-prev_close)/prev_close for all stocks) / count

        Returns:
            按 LGBRank 分数降序排列的 ScoredStock 列表
        """
        if not candidates:
            return []

        # Step 1: 计算每只股票的 38 个原始特征
        feature_dicts = []
        for s in candidates:
            base = _compute_base_features(s, avg_market_open_gain)
            hist = history_map.get(s.code)
            advanced = _compute_advanced_features(hist)
            merged = {**base, **advanced}
            engineered = _compute_engineered_features(merged)
            merged.update(engineered)
            feature_dicts.append(merged)

        # Step 2: 当日池内 z-score 归一化 (38 raw -> 38 z_xxx)
        _add_zscore(feature_dicts, self.raw_features)

        # Step 3: 按照模型要求的特征顺序构建矩阵
        n = len(candidates)
        X = np.zeros((n, len(self.features)))
        for i, fd in enumerate(feature_dicts):
            for j, fname in enumerate(self.features):
                X[i, j] = fd.get(fname, 0.0)

        # Step 4: NaN/inf 检查
        if np.isnan(X).any() or np.isinf(X).any():
            bad = []
            for j, fname in enumerate(self.features):
                col = X[:, j]
                n_na, n_inf = int(np.isnan(col).sum()), int(np.isinf(col).sum())
                if n_na > 0 or n_inf > 0:
                    bad.append(f"{fname}: {n_na} NaN, {n_inf} inf")
            raise RuntimeError(f"Feature matrix contains NaN/inf: {bad}")

        # Step 5: 模型推理
        scores = self.model.predict(X)

        # Step 6: 排名
        order = np.argsort(-scores)
        results = []
        for rank_0, idx in enumerate(order):
            s = candidates[idx]
            results.append(ScoredStock(
                code=s.code,
                name=s.name,
                score=float(scores[idx]),
                rank=rank_0 + 1,
                buy_price=s.price_at_940,
            ))

        return results
