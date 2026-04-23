"""Serverless ML training endpoint for A-share quant trading.

Receives a callback URL from trading-service, fetches raw daily OHLCV data
via streaming NDJSON, builds training dataset (feature extraction + labeling),
trains LightGBM LambdaRank model, and uploads the model to S3.

The caller (trading-service) only needs to:
1. Check data completeness
2. POST trigger with callback_url + S3 config (no data in payload)
3. Serve a streaming NDJSON endpoint for this service to pull data from

This service handles everything else: data fetching, feature extraction,
train/val split, quintile labeling, model training, and S3 upload.
"""

from __future__ import annotations

import base64
import json
import logging
import tempfile
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import requests as http_requests
from flask import Flask, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

# ── ML Constants (must match ml_scanner.py) ────────────────

FEATURE_NAMES_BASIC = [
    "open_gain",
    "volume_amp",
    "consecutive_up_days",
    "trend_5d",
    "trend_10d",
    "avg_return_20d",
    "volatility_20d",
    "early_price_range",
    "market_open_gain",
    "trend_consistency",
    "gap",
    "upper_shadow_ratio",
    "volume_ratio",
]

FEATURE_NAMES_ADVANCED = [
    "open_position_consistency",
    "volume_price_divergence",
    "intraday_momentum_cont",
    "volume_concentration",
    "relative_strength",
    "return_consistency",
    "amplitude_decay",
    "volume_stability",
    "close_vs_vwap",
    "volume_weighted_return",
    "price_channel_position",
    "up_day_ratio_20d",
    "amplitude_20d",
    "volume_ratio_5d_20d",
]

FEATURE_NAMES_CROSS = [
    "momentum_x_mean_reversion",
    "trend_acceleration",
    "momentum_quality",
    "volume_trend_interaction",
    "gap_volume_interaction",
    "strength_persistence",
    "volatility_adj_return",
    "volume_price_momentum",
    "gap_reversion",
    "trend_volume_divergence",
    "momentum_stability",
]

FEATURE_NAMES_RAW = FEATURE_NAMES_BASIC + FEATURE_NAMES_ADVANCED + FEATURE_NAMES_CROSS
FEATURE_NAMES_ALL = FEATURE_NAMES_RAW + [f"z_{name}" for name in FEATURE_NAMES_RAW]
assert len(FEATURE_NAMES_ALL) == 76  # noqa: S101

TOTAL_FEE_PCT = 0.09  # (0.02% commission × 2 + 0.05% stamp tax) × 100
LABEL_BINS = 5
FORWARD_DAYS = 2
TRAIN_VAL_SPLIT = 0.8
MIN_TRAIN_DAYS = 60

LGB_PARAMS = {
    "objective": "lambdarank",
    "metric": "ndcg",
    "eval_at": [5, 10],
    "num_leaves": 15,
    "learning_rate": 0.05,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "lambda_l1": 0.1,
    "lambda_l2": 1.0,
    "verbose": -1,
}
MAX_BOOST_ROUNDS = 500
EARLY_STOPPING = 50


# ── Feature Extraction ─────────────────────────────────────


def extract_features(
    bar: dict,
    history: list[dict],
    prev_bar: dict | None,
    market_open_gain: float,
) -> list[float] | None:
    """Extract 38 raw features from a daily OHLCV bar + history.

    IMPORTANT: This replicates lgbrank_scorer.py's _compute_base_features +
    _compute_advanced_features + _compute_engineered_features EXACTLY.
    Feature names are mapped to FEATURE_NAMES_RAW via _COMPUTE_TO_MODEL.

    Key conventions (matching lgbrank_scorer.py):
    - All ratios, NOT percentages (no ×100)
    - volume_amp uses ×0.125 early session ratio
    - market_open_gain = avg gap (open-pc)/pc, NOT intraday gain
    - trend_consistency = Sharpe-like ratio, NOT fraction of positive days
    - Advanced features use numpy arrays over full history (matching lgbrank_scorer)
    - Cross features match lgbrank_scorer._compute_engineered_features exactly

    Args:
        bar: Today's {open, high, low, close, volume} bar.
        history: Past ~37 bars (oldest first), each {open, high, low, close, volume}.
        prev_bar: Yesterday's bar (for gap, open_position). None if no previous day.
        market_open_gain: Average gap ratio across all stocks: mean((open-pc)/pc).

    Returns:
        38 raw feature values in FEATURE_NAMES_RAW order, or None if bar invalid.
    """
    import math
    import statistics

    o = bar["open"]
    h = bar["high"]
    lo = bar["low"]
    c = bar["close"]
    vol = bar.get("volume", 0)

    if not all([o > 0, h > 0, lo > 0, c > 0]):
        return None

    prev_close = prev_bar["close"] if prev_bar and prev_bar.get("close", 0) > 0 else 0.0

    raw: dict[str, float] = {}
    closes = [b["close"] for b in history if b.get("close", 0) > 0]

    # ── 13 Basic Features (lgbrank_scorer._compute_base_features) ──

    # open_gain = (close - open) / open  [RATIO, not %]
    raw["open_gain"] = (c - o) / o

    # volume_amp = volume / (avg_volume × 0.125)  [with early session ratio]
    hist_vols = [b.get("volume", 0) for b in history if b.get("volume", 0) > 0]
    avg_vol = statistics.mean(hist_vols) if hist_vols else 0.0
    expected_vol = avg_vol * 0.125
    raw["volume_amp"] = vol / expected_vol if expected_vol > 0 else 0.0

    # consecutive_up_days
    cup = 0
    if len(closes) >= 2:
        for i in range(len(closes) - 1, 0, -1):
            if closes[i] > closes[i - 1]:
                cup += 1
            else:
                break
    raw["consecutive_up_days"] = float(cup)

    # trend_5d  [RATIO, not %]
    if len(closes) >= 6:
        raw["trend_5d"] = (closes[-1] - closes[-6]) / closes[-6]
    else:
        raw["trend_5d"] = 0.0

    # trend_10d  [RATIO, not %]
    if len(closes) >= 11:
        raw["trend_10d"] = (closes[-1] - closes[-11]) / closes[-11]
    else:
        raw["trend_10d"] = 0.0

    # avg_return_20d  [RATIO, not %]
    if len(closes) >= 21:
        rets_20 = [
            (closes[i] - closes[i - 1]) / closes[i - 1]
            for i in range(len(closes) - 20, len(closes))
            if closes[i - 1] > 0
        ]
        raw["avg_return_20d"] = statistics.mean(rets_20) if rets_20 else 0.0
    else:
        raw["avg_return_20d"] = 0.0

    # volatility_20d  [RATIO, not %]
    if len(closes) >= 21:
        rets_20 = [
            (closes[i] - closes[i - 1]) / closes[i - 1]
            for i in range(len(closes) - 20, len(closes))
            if closes[i - 1] > 0
        ]
        raw["volatility_20d"] = statistics.stdev(rets_20) if len(rets_20) >= 2 else 0.0
    else:
        raw["volatility_20d"] = 0.0

    # early_price_range = (high - low) / open  [RATIO, not %]
    raw["early_price_range"] = (h - lo) / o

    # market_open_gain: passed in (avg gap ratio across all stocks)
    raw["market_open_gain"] = market_open_gain

    # trend_consistency = avg_return / (volatility + 0.001)  [Sharpe-like]
    raw["trend_consistency"] = raw["avg_return_20d"] / (raw["volatility_20d"] + 0.001)

    # gap = (open - prev_close) / prev_close  [RATIO, not %]
    raw["gap"] = (o - prev_close) / prev_close if prev_close > 0 else 0.0

    # upper_shadow_ratio = (high - max(open, close)) / open  [RATIO, not %]
    body_top = max(o, c)
    raw["upper_shadow_ratio"] = (h - body_top) / o if h > body_top else 0.0

    # volume_ratio = volume / avg_volume  [no 0.125]
    raw["volume_ratio"] = vol / avg_vol if avg_vol > 0 else 0.0

    # ── 14 Advanced Features (lgbrank_scorer._compute_advanced_features) ──
    # Uses numpy arrays over full history, matching lgbrank_scorer exactly.

    # Build history OHLCV arrays
    hist_opens: list[float] = []
    hist_highs: list[float] = []
    hist_lows: list[float] = []
    hist_closes: list[float] = []
    hist_volumes: list[float] = []
    for b in history:
        if b.get("close", 0) > 0:
            hist_opens.append(float(b.get("open", 0)))
            hist_highs.append(float(b.get("high", 0)))
            hist_lows.append(float(b.get("low", 0)))
            hist_closes.append(float(b["close"]))
            hist_volumes.append(float(b.get("volume", 0)))

    n_hist = len(hist_closes)

    if n_hist >= 5:
        opens_arr = np.array(hist_opens)
        highs_arr = np.array(hist_highs)
        lows_arr = np.array(hist_lows)
        closes_arr = np.array(hist_closes)
        volumes_arr = np.array(hist_volumes)
        n = n_hist

        # 1. open_position_consistency: 1 - std(open_positions[-20:])
        ranges = highs_arr - lows_arr + 1e-8
        open_positions = (opens_arr - lows_arr) / ranges
        raw["open_position_consistency"] = float(1 - np.std(open_positions[-min(20, n) :]))

        # 2. volume_price_divergence
        if n >= 10:
            close_5d_ago = closes_arr[-6] if n >= 6 else closes_arr[0]
            price_chg = (closes_arr[-1] - close_5d_ago) / (close_5d_ago + 1e-8)
            vol_recent = volumes_arr[-5:].mean()
            vol_prev = volumes_arr[-10:-5].mean()
            vol_chg = (vol_recent - vol_prev) / (vol_prev + 1e-8)
            raw["volume_price_divergence"] = float(vol_chg - price_chg)
        else:
            raw["volume_price_divergence"] = 0.0

        # 3. intraday_momentum_cont: lower_shadow - upper_shadow over 10 bars
        last10 = min(10, n)
        h_s, lo_s = highs_arr[-last10:], lows_arr[-last10:]
        o_s, c_s = opens_arr[-last10:], closes_arr[-last10:]
        r10 = h_s - lo_s + 1e-8
        upper_s = np.where(c_s > o_s, (h_s - c_s) / r10, (h_s - o_s) / r10)
        lower_s = np.where(c_s > o_s, (o_s - lo_s) / r10, (c_s - lo_s) / r10)
        raw["intraday_momentum_cont"] = float(lower_s.mean() - upper_s.mean())

        # 4. volume_concentration: 1 - top10%_vol / total_vol over 20d
        v20 = volumes_arr[-min(20, n) :]
        sorted_v = np.sort(v20)[::-1]
        top_n = max(1, int(len(v20) * 0.1))
        raw["volume_concentration"] = float(1 - sorted_v[:top_n].sum() / (v20.sum() + 1e-8))

        # 5. relative_strength: close / 20d_high
        high_20d = highs_arr[-min(20, n) :].max()
        raw["relative_strength"] = float(closes_arr[-1] / (high_20d + 1e-8))

        # 6. return_consistency: 1 / (1 + CV)
        if n >= 3:
            daily_rets = np.diff(closes_arr) / (closes_arr[:-1] + 1e-8)
            rets_20d = daily_rets[-min(20, len(daily_rets)) :]
            cv = np.std(rets_20d) / (np.abs(np.mean(rets_20d)) + 1e-6)
            raw["return_consistency"] = float(1 / (1 + cv))
        else:
            raw["return_consistency"] = 0.0

        # 7. amplitude_decay: -slope(amplitudes) × 10 over 10 bars
        amps = (highs_arr[-min(10, n) :] - lows_arr[-min(10, n) :]) / (
            lows_arr[-min(10, n) :] + 1e-8
        )
        x = np.arange(len(amps))
        if len(amps) >= 3:
            slope = np.polyfit(x, amps, 1)[0]
            raw["amplitude_decay"] = float(-slope * 10)
        else:
            raw["amplitude_decay"] = 0.0

        # 8. volume_stability: max(0, 1 - std(vol_changes))
        v_recent = volumes_arr[-min(20, n) :]
        vol_changes = np.diff(v_recent) / (v_recent[:-1] + 1e-8)
        raw["volume_stability"] = float(max(0, 1 - np.std(vol_changes)))

        # 9. close_vs_vwap: (close - vwap_5d) / vwap_5d
        if n >= 5:
            c5 = closes_arr[-5:]
            h5 = highs_arr[-5:]
            l5 = lows_arr[-5:]
            v5 = volumes_arr[-5:]
            typical = (h5 + l5 + c5) / 3
            vwap = (typical * v5).sum() / (v5.sum() + 1e-8)
            raw["close_vs_vwap"] = float((closes_arr[-1] - vwap) / (vwap + 1e-8))
        else:
            raw["close_vs_vwap"] = 0.0

        # 10. volume_weighted_return: sum(ret×vol) / sum(vol) × 100 over 10d
        if n >= 3:
            dr = np.diff(closes_arr) / (closes_arr[:-1] + 1e-8)
            dr10 = dr[-min(10, len(dr)) :]
            vr10 = volumes_arr[-len(dr10) :]
            raw["volume_weighted_return"] = float((dr10 * vr10).sum() / (vr10.sum() + 1e-8) * 100)
        else:
            raw["volume_weighted_return"] = 0.0

        # 11. price_channel_position: (close - 20d_low) / (20d_high - 20d_low)
        low_20d = lows_arr[-min(20, n) :].min()
        raw["price_channel_position"] = float(
            (closes_arr[-1] - low_20d) / (high_20d - low_20d + 1e-8)
        )

        # 12. up_day_ratio_20d: fraction of up days
        if n >= 3:
            dr_all = np.diff(closes_arr[-min(21, n) :])
            raw["up_day_ratio_20d"] = float((dr_all > 0).sum() / (len(dr_all) + 1e-8))
        else:
            raw["up_day_ratio_20d"] = 0.0

        # 13. amplitude_20d: mean of (high-low)/open over 20d
        amps_20 = (highs_arr[-min(20, n) :] - lows_arr[-min(20, n) :]) / (
            opens_arr[-min(20, n) :] + 1e-8
        )
        raw["amplitude_20d"] = float(amps_20.mean())

        # 14. volume_ratio_5d_20d
        if n >= 10:
            vol_5d = volumes_arr[-5:].mean()
            vol_20d = volumes_arr[-min(20, n) :].mean()
            raw["volume_ratio_5d_20d"] = float(vol_5d / (vol_20d + 1e-8))
        else:
            raw["volume_ratio_5d_20d"] = 1.0
    else:
        for k in [
            "open_position_consistency",
            "volume_price_divergence",
            "intraday_momentum_cont",
            "volume_concentration",
            "relative_strength",
            "return_consistency",
            "amplitude_decay",
            "volume_stability",
            "close_vs_vwap",
            "volume_weighted_return",
            "price_channel_position",
            "up_day_ratio_20d",
            "amplitude_20d",
            "volume_ratio_5d_20d",
        ]:
            raw[k] = 0.0

    # ── 11 Cross Features (lgbrank_scorer._compute_engineered_features) ──

    def _g(key: str) -> float:
        return raw.get(key, 0.0)

    raw["momentum_x_mean_reversion"] = -_g("avg_return_20d") * (1 + _g("consecutive_up_days") / 10)
    raw["trend_acceleration"] = _g("trend_10d") - _g("trend_5d")
    raw["momentum_quality"] = (
        _g("trend_10d") / (_g("volatility_20d") + 0.001) * max(0, _g("avg_return_20d"))
    )
    raw["volume_trend_interaction"] = _g("trend_10d") * _g("volatility_20d")
    raw["gap_volume_interaction"] = _g("gap") / (_g("consecutive_up_days") + 1)
    raw["strength_persistence"] = _g("early_price_range") / (_g("volatility_20d") + 0.001)
    raw["volatility_adj_return"] = _g("avg_return_20d") / (_g("volatility_20d") + 0.001)
    raw["volume_price_momentum"] = _g("open_gain") / (_g("volume_ratio") + 0.001)
    raw["gap_reversion"] = _g("gap") / (abs(_g("trend_10d")) + 0.001)
    raw["trend_volume_divergence"] = _g("avg_return_20d") * _g("volume_amp")
    raw["momentum_stability"] = _g("consecutive_up_days") * _g("early_price_range")

    # Sanitize: replace NaN/Inf with 0
    for k, v in raw.items():
        if math.isnan(v) or math.isinf(v):
            raw[k] = 0.0

    return [raw.get(name, 0.0) for name in FEATURE_NAMES_RAW]


# ── Dataset Building ───────────────────────────────────────


def _build_stock_history(
    code: str,
    daily_data: dict[str, dict[str, dict]],
    all_dates: list[str],
    day_idx: int,
    lookback: int = 37,
) -> list[dict]:
    """Build history list for one stock up to (not including) day_idx.

    Returns list of bar dicts (oldest first), up to `lookback` bars.
    """
    history: list[dict] = []
    start = max(0, day_idx - lookback)
    for i in range(start, day_idx):
        bar = daily_data.get(all_dates[i], {}).get(code)
        if bar and bar.get("close", 0) > 0 and not bar.get("is_suspended"):
            history.append(bar)
    return history


def build_day_data(
    daily_data: dict[str, dict[str, dict]],
    all_dates: list[str],
    day_idx: int,
    future_map: dict[str, dict],
) -> dict | None:
    """Build features + labels for one trading day, using full history context.

    Args:
        daily_data: Full {date_str: {stock_code: bar}} dataset.
        all_dates: Sorted list of all date strings.
        day_idx: Index of current day in all_dates.
        future_map: {stock_code: {close, ...}} for day T+FORWARD_DAYS.

    Returns:
        {"features": [...], "labels": [...], "group_size": int} or None.
    """
    import statistics

    current_date = all_dates[day_idx]
    daily_map = daily_data.get(current_date, {})

    if not daily_map or not future_map or len(daily_map) < 50:
        return None

    # Previous day for gap/open_position features
    prev_date_map = daily_data.get(all_dates[day_idx - 1], {}) if day_idx > 0 else {}

    # Collect close prices (skip suspended)
    today_close: dict[str, float] = {}
    for code, bar in daily_map.items():
        if bar.get("is_suspended"):
            continue
        if bar.get("close", 0) > 0:
            today_close[code] = bar["close"]

    future_close: dict[str, float] = {}
    for code, bar in future_map.items():
        if bar.get("close", 0) > 0:
            future_close[code] = bar["close"]

    # Forward returns
    codes_with_return: list[str] = []
    returns: list[float] = []
    for code in today_close:
        if code in future_close:
            ret = (future_close[code] / today_close[code] - 1) * 100 - TOTAL_FEE_PCT
            codes_with_return.append(code)
            returns.append(ret)

    if len(codes_with_return) < 20:
        return None

    # Quintile labels
    sorted_rets = sorted(returns)
    bin_size = len(sorted_rets) / LABEL_BINS
    labels: list[int] = []
    for ret in returns:
        for b in range(LABEL_BINS):
            threshold_idx = min(int((b + 1) * bin_size), len(sorted_rets) - 1)
            if ret <= sorted_rets[threshold_idx]:
                labels.append(b)
                break
        else:
            labels.append(LABEL_BINS - 1)

    # Compute market_open_gain: avg gap ratio (open - prev_close) / prev_close
    # Matches lgbrank_scorer / v16_scanner._compute_avg_market_open_gain exactly.
    # NOT intraday gain — this is the overnight gap.
    gap_ratios: list[float] = []
    for code, bar in daily_map.items():
        if bar.get("is_suspended"):
            continue
        o = bar.get("open", 0)
        if o <= 0:
            continue
        prev = prev_date_map.get(code)
        if prev and prev.get("close", 0) > 0:
            gap_ratios.append((o - prev["close"]) / prev["close"])
    market_open_gain = statistics.mean(gap_ratios) if gap_ratios else 0.0

    # Extract raw features per stock
    raw_features_per_code: dict[str, list[float]] = {}
    code_to_idx: dict[str, int] = {}
    for i, code in enumerate(codes_with_return):
        bar = daily_map.get(code)
        if not bar:
            continue
        history = _build_stock_history(code, daily_data, all_dates, day_idx)
        prev_bar = prev_date_map.get(code)
        fv = extract_features(bar, history, prev_bar, market_open_gain)
        if fv is not None:
            raw_features_per_code[code] = fv
            code_to_idx[code] = i

    if not raw_features_per_code:
        return None

    # Z-score normalize across this day's candidates
    # Uses numpy std (ddof=0), matching lgbrank_scorer._add_zscore
    n_raw = len(FEATURE_NAMES_RAW)
    codes_list = list(raw_features_per_code.keys())
    raw_matrix = np.array([raw_features_per_code[c] for c in codes_list])

    means = raw_matrix.mean(axis=0)
    stds = raw_matrix.std(axis=0)  # ddof=0, matching lgbrank_scorer

    features: list[list[float]] = []
    valid_labels: list[int] = []
    for i, code in enumerate(codes_list):
        raw_vec = list(raw_matrix[i])
        z_vec = [
            float((raw_matrix[i, j] - means[j]) / stds[j]) if stds[j] > 1e-10 else 0.0
            for j in range(n_raw)
        ]
        features.append(raw_vec + z_vec)
        valid_labels.append(labels[code_to_idx[code]])

    return {
        "features": features,
        "labels": valid_labels,
        "group_size": len(features),
    }


def build_training_data(
    daily_data: dict[str, dict[str, dict]],
    mode: str,
) -> dict:
    """Build full training dataset from raw daily OHLCV.

    Args:
        daily_data: {date_str: {stock_code: {open, high, low, close, ...}}}.
        mode: "full" or "finetune".

    Returns:
        {"features", "labels", "groups", "val_features", "val_labels", "val_groups"}.
    """
    all_dates = sorted(daily_data.keys())

    if len(all_dates) < MIN_TRAIN_DAYS:
        raise ValueError(f"Not enough data: {len(all_dates)} < {MIN_TRAIN_DAYS} days")

    # For finetune: only train/val on last 120 days, but keep full date list
    # so _build_stock_history can look back 37 days for feature computation.
    if mode == "finetune":
        train_val_dates = all_dates[-120:]
    else:
        train_val_dates = list(all_dates)

    split_idx = int(len(train_val_dates) * TRAIN_VAL_SPLIT)
    train_dates = train_val_dates[:split_idx]
    val_dates = train_val_dates[split_idx:]

    def _process_dates(dates):
        features, labels, groups = [], [], []
        skipped = 0
        for d in dates:
            # Use full all_dates for index (so history lookback works)
            d_idx = all_dates.index(d)
            if d_idx + FORWARD_DAYS >= len(all_dates):
                skipped += 1
                continue
            future_d = all_dates[d_idx + FORWARD_DAYS]

            day_data = build_day_data(
                daily_data,
                all_dates,
                d_idx,
                daily_data.get(future_d, {}),
            )
            if day_data is None:
                skipped += 1
                continue

            features.extend(day_data["features"])
            labels.extend(day_data["labels"])
            groups.append(day_data["group_size"])

        if skipped > 0:
            logger.warning("Skipped %d/%d dates (no data or no future date)", skipped, len(dates))
        return features, labels, groups

    logger.info(
        "Building training data: %d train dates, %d val dates", len(train_dates), len(val_dates)
    )
    train_features, train_labels, train_groups = _process_dates(train_dates)
    val_features, val_labels, val_groups = _process_dates(val_dates)

    if not train_features:
        raise ValueError("Failed to build any training features")

    logger.info(
        "Dataset ready: train %d samples/%d days, val %d samples/%d days",
        len(train_labels),
        len(train_groups),
        len(val_labels),
        len(val_groups),
    )

    return {
        "features": train_features,
        "labels": train_labels,
        "groups": train_groups,
        "val_features": val_features if val_features else None,
        "val_labels": val_labels if val_labels else None,
        "val_groups": val_groups if val_groups else None,
    }


# ── Data Fetching (callback to trading-service) ───────────


def _fetch_training_data(callback_url: str) -> dict[str, dict[str, dict]]:
    """Fetch NDJSON training data from trading-service streaming endpoint.

    Returns: {date_str: {stock_code: {open, high, low, close, volume, amount, is_suspended}}}
    """
    daily_data: dict[str, dict[str, dict]] = {}

    with http_requests.get(callback_url, stream=True, timeout=(10, 300)) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("__meta__"):
                logger.info(
                    "Training data: %d days, range %s",
                    obj.get("total_days"),
                    obj.get("date_range"),
                )
                continue
            daily_data[obj["date"]] = obj["stocks"]

    if not daily_data:
        raise ValueError("No data received from callback")

    logger.info("Fetched %d days of training data via callback", len(daily_data))
    return daily_data


# ── S3 Upload ──────────────────────────────────────────────


def _get_s3_client(s3_config: dict):
    """Create boto3 S3 client from config. Returns (client, bucket) or (None, None)."""
    import boto3
    from botocore.config import Config

    endpoint_url = s3_config.get("endpoint_url", "")
    access_key = s3_config.get("access_key", "")
    secret_key = s3_config.get("secret_key", "")
    bucket = s3_config.get("bucket", "")

    if not all([endpoint_url, access_key, secret_key, bucket]):
        return None, None

    # OSS requires virtual hosted style (bucket.endpoint, not endpoint/bucket)
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version="s3",
            s3={"addressing_style": "virtual"},
        ),
    )
    return client, bucket


def download_from_s3(s3_key: str, local_path: Path, s3_config: dict) -> bool:
    """Download file from S3. Returns True if successful."""
    client, bucket = _get_s3_client(s3_config)
    if not client:
        logger.warning("S3 config incomplete, skipping download")
        return False
    try:
        client.download_file(bucket, s3_key, str(local_path))
        logger.info("Downloaded s3://%s/%s -> %s", bucket, s3_key, local_path.name)
        return True
    except Exception as e:
        logger.warning("S3 download failed for %s: %s", s3_key, e)
        return False


def upload_to_s3(local_path: Path, s3_key: str, s3_config: dict) -> str | None:
    """Upload file to S3. Returns S3 URI or None."""
    client, bucket = _get_s3_client(s3_config)
    if not client:
        logger.warning("S3 config incomplete, skipping upload")
        return None

    client.put_object(Bucket=bucket, Key=s3_key, Body=local_path.read_bytes())
    uri = f"s3://{bucket}/{s3_key}"
    logger.info("Uploaded %s -> %s", local_path.name, uri)
    return uri


# ── Routes ─────────────────────────────────────────────────


@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "DELETE"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT", "DELETE"])
def handler(path):
    """Single entry point for serverless platform.

    Routes:
        GET  /*        -> health check
        POST /* (/invoke) -> full training pipeline
    """
    if request.method == "GET":
        return {"status": "ok", "service": "ml-training-serverless"}

    if request.method == "POST":
        return _handle_train()

    return {"error": f"Unknown route: {request.method} /{path}"}, 404


def _handle_train():
    """Full ML training pipeline: data -> features -> train -> S3.

    Request JSON (callback mode — preferred):
    {
        "mode": "full" | "finetune",
        "callback_url": "https://trading-service/api/model/training-data?token=xxx",
        "init_model_b64": "..." | null,
        "s3_config": { ... }
    }

    Request JSON (legacy direct mode):
    {
        "mode": "full" | "finetune",
        "daily_data": { ... },
        "init_model_b64": "..." | null,
        "s3_config": { ... }
    }

    Response JSON:
    {
        "success": true,
        "model_b64": "...",
        "s3_uri": "s3://bucket/models/full_20260406.lgb" | null,
        "rounds": 150,
        "n_samples": 50000,
        "n_days": 100,
        "elapsed_seconds": 12.3
    }
    """
    import lightgbm as lgb

    t0 = time.time()
    data = request.get_json(force=True, silent=True)
    if not data:
        return {"success": False, "error": "Empty or invalid JSON body"}, 400

    mode = data.get("mode", "full")
    result_callback_url = data.get("result_callback_url")

    # Run training and deliver result via callback
    result = _run_training(data, mode, lgb, t0)
    _deliver_result(result_callback_url, result)
    return result


def _deliver_result(result_callback_url: str | None, result: dict) -> None:
    """POST training result to trading-service callback URL."""
    if not result_callback_url:
        return
    try:
        resp = http_requests.post(result_callback_url, json=result, timeout=30)
        logger.info("Result callback: HTTP %d", resp.status_code)
    except Exception as e:
        logger.error("Result callback failed: %s", e)


def _run_training(data: dict, mode: str, lgb, t0: float) -> dict:
    """Execute the full training pipeline. Always returns a result dict."""
    try:
        return _run_training_inner(data, mode, lgb, t0)
    except Exception as e:
        logger.error("Training failed: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


def _run_training_inner(data: dict, mode: str, lgb, t0: float) -> dict:
    """Inner training logic that may raise exceptions."""
    # Prefer callback mode (streaming from trading-service), fallback to direct data
    callback_url = data.get("callback_url")
    daily_data = data.get("daily_data")

    if callback_url:
        logger.info("Fetching training data via callback: %s", callback_url[:80])
        daily_data = _fetch_training_data(callback_url)

    if not daily_data:
        return {"success": False, "error": "Missing daily_data and no callback_url"}

    logger.info("Train request: mode=%s, %d dates", mode, len(daily_data))

    # ── Step 1: Build training dataset ──
    logger.info("Step 1/4: Building training dataset...")
    t_step = time.time()
    dataset = build_training_data(daily_data, mode)

    features = dataset["features"]
    labels = dataset["labels"]
    groups = dataset["groups"]
    logger.info(
        "Step 1/4 done: %d samples, %d days (%.1fs)",
        len(labels),
        len(groups),
        time.time() - t_step,
    )

    # ── Step 2: Build LightGBM datasets ──
    logger.info("Step 2/4: Creating LightGBM datasets...")
    t_step = time.time()
    train_data = lgb.Dataset(
        np.array(features, dtype=np.float32),
        label=np.array(labels, dtype=np.int32),
        group=groups,
        feature_name=FEATURE_NAMES_ALL,
        free_raw_data=False,
    )
    logger.info("Step 2/4 done (%.1fs)", time.time() - t_step)

    def _log_eval(period=50):
        """LightGBM callback that logs via logger instead of stdout."""

        def _callback(env):
            if env.iteration % period == 0 or env.iteration == env.end_iteration - 1:
                metrics = []
                for data_name, eval_name, result, _ in env.evaluation_result_list:
                    metrics.append(f"{data_name} {eval_name}={result:.4f}")
                logger.info("  [%d/%d] %s", env.iteration, env.end_iteration, " | ".join(metrics))

        _callback.order = 10
        return _callback

    callbacks = [_log_eval(period=25)]
    valid_sets = [train_data]
    valid_names = ["train"]

    if dataset["val_features"]:
        val_data = lgb.Dataset(
            np.array(dataset["val_features"], dtype=np.float32),
            label=np.array(dataset["val_labels"], dtype=np.int32),
            group=dataset["val_groups"],
            feature_name=FEATURE_NAMES_ALL,
            free_raw_data=False,
        )
        valid_sets.append(val_data)
        valid_names.append("val")
        callbacks.append(lgb.early_stopping(EARLY_STOPPING))

    # ── Step 3: Load init model for fine-tuning ──
    logger.info("Step 3/4: Preparing init model...")
    init_model = None
    tmp_init = None
    s3_config = data.get("s3_config")

    if mode == "finetune" and s3_config:
        tmp_init = tempfile.NamedTemporaryFile(suffix=".lgb", delete=False)
        tmp_init.close()
        if download_from_s3("models/full_latest.lgb", Path(tmp_init.name), s3_config):
            init_model = tmp_init.name
            logger.info("Downloaded init model from S3")
        else:
            Path(tmp_init.name).unlink(missing_ok=True)
            tmp_init = None
            logger.warning("No init model found in S3, training from scratch")

    # ── Step 4: Train ──
    logger.info(
        "Step 4/4: Training LightGBM (max %d rounds, early_stop=%d)...",
        MAX_BOOST_ROUNDS,
        EARLY_STOPPING,
    )
    t_step = time.time()
    try:
        booster = lgb.train(
            LGB_PARAMS,
            train_data,
            num_boost_round=MAX_BOOST_ROUNDS,
            valid_sets=valid_sets,
            valid_names=valid_names,
            callbacks=callbacks,
            init_model=init_model,
        )
    finally:
        if tmp_init:
            Path(tmp_init.name).unlink(missing_ok=True)

    rounds = booster.current_iteration()
    logger.info("Step 4/4 done: %d rounds (%.1fs)", rounds, time.time() - t_step)
    elapsed = time.time() - t0
    logger.info("Training complete: %d rounds in %.1fs total", rounds, elapsed)

    # ── Step 5: Save model to temp file ──
    today_str = datetime.now(BEIJING_TZ).strftime("%Y%m%d")
    model_name = f"full_{today_str}" if mode == "full" else f"finetune_{today_str}"

    tmp_out = tempfile.NamedTemporaryFile(suffix=".lgb", delete=False)
    tmp_out.close()
    tmp_out_path = Path(tmp_out.name)
    try:
        booster.save_model(str(tmp_out_path))
        model_bytes = tmp_out_path.read_bytes()

        # ── Step 6: Upload to S3 ──
        s3_uri = None
        s3_config = data.get("s3_config")
        if s3_config:
            logger.info(
                "Step 6: Uploading to S3 (endpoint=%s, bucket=%s)",
                s3_config.get("endpoint_url", "?"),
                s3_config.get("bucket", "?"),
            )
            try:
                s3_uri = upload_to_s3(tmp_out_path, f"models/{model_name}.lgb", s3_config)
                if mode == "full":
                    upload_to_s3(tmp_out_path, "models/full_latest.lgb", s3_config)
                logger.info("Step 6 done: s3_uri=%s", s3_uri)
            except Exception as e:
                logger.error("S3 upload failed: %s", e, exc_info=True)
        else:
            logger.warning("Step 6: SKIPPED — no s3_config in payload")
    finally:
        tmp_out_path.unlink(missing_ok=True)

    model_b64 = base64.b64encode(model_bytes).decode("ascii")

    return {
        "success": True,
        "model_b64": model_b64,
        "model_name": model_name,
        "s3_uri": s3_uri,
        "rounds": rounds,
        "n_samples": len(labels),
        "n_days": len(groups),
        "elapsed_seconds": round(elapsed, 2),
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000)
