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

    Matches ml_scanner.py compute_raw_features() as closely as possible,
    using daily OHLCV as proxy for intraday 9:40 snapshot features.

    Args:
        bar: Today's {open, high, low, close, volume} bar.
        history: Past ~37 bars (oldest first), each {open, high, low, close, volume}.
        prev_bar: Yesterday's bar (for gap, open_position). None if no previous day.
        market_open_gain: Average open_gain across all stocks on this day.

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

    raw: dict[str, float] = {}
    closes = [b["close"] for b in history if b.get("close", 0) > 0]

    # ── 13 Basic Features ──

    # open_gain: daily proxy for intraday (latest_940 - open) / open × 100
    raw["open_gain"] = (c - o) / o * 100

    # volume_amp: volume / avg_volume (normalized to ~1 for typical day)
    hist_vols = [b.get("volume", 0) for b in history if b.get("volume", 0) > 0]
    avg_vol = statistics.mean(hist_vols) if hist_vols else 0.0
    raw["volume_amp"] = vol / avg_vol if avg_vol > 0 else 0.0

    # consecutive_up_days: count from history
    cup = 0
    if len(closes) >= 2:
        for i in range(len(closes) - 1, 0, -1):
            if closes[i] > closes[i - 1]:
                cup += 1
            else:
                break
    raw["consecutive_up_days"] = float(cup)

    # trend_5d: (last close - close 5 days ago) / close 5 days ago × 100
    if len(closes) >= 6:
        raw["trend_5d"] = (closes[-1] - closes[-6]) / closes[-6] * 100
    else:
        raw["trend_5d"] = 0.0

    # trend_10d
    if len(closes) >= 11:
        raw["trend_10d"] = (closes[-1] - closes[-11]) / closes[-11] * 100
    else:
        raw["trend_10d"] = 0.0

    # avg_return_20d: mean daily return over last 20 days (%)
    if len(closes) >= 21:
        rets_20 = [
            (closes[i] - closes[i - 1]) / closes[i - 1] * 100
            for i in range(len(closes) - 20, len(closes))
            if closes[i - 1] > 0
        ]
        raw["avg_return_20d"] = statistics.mean(rets_20) if rets_20 else 0.0
    else:
        raw["avg_return_20d"] = 0.0

    # volatility_20d: stdev of daily returns over last 20 days (%)
    if len(closes) >= 21:
        rets_20 = [
            (closes[i] - closes[i - 1]) / closes[i - 1] * 100
            for i in range(len(closes) - 20, len(closes))
            if closes[i - 1] > 0
        ]
        raw["volatility_20d"] = statistics.stdev(rets_20) if len(rets_20) >= 2 else 0.0
    else:
        raw["volatility_20d"] = 0.0

    # early_price_range: daily proxy = (high - low) / open × 100
    raw["early_price_range"] = (h - lo) / o * 100

    # market_open_gain: passed in (avg across all stocks today)
    raw["market_open_gain"] = market_open_gain

    # trend_consistency: fraction of last 5 days with positive returns
    if len(closes) >= 6:
        recent_5 = closes[-6:]
        pos_days = sum(1 for i in range(1, len(recent_5)) if recent_5[i] > recent_5[i - 1])
        raw["trend_consistency"] = pos_days / 5.0
    else:
        raw["trend_consistency"] = 0.0

    # gap: (open - prev_close) / prev_close × 100
    if prev_bar and prev_bar.get("close", 0) > 0:
        raw["gap"] = (o - prev_bar["close"]) / prev_bar["close"] * 100
    else:
        raw["gap"] = 0.0

    # upper_shadow_ratio: (high - max(open, close)) / open × 100
    body_top = max(o, c)
    raw["upper_shadow_ratio"] = (h - body_top) / o * 100 if o > 0 else 0.0

    # volume_ratio: volume / avg_volume (raw, not normalized by session ratio)
    raw["volume_ratio"] = vol / avg_vol if avg_vol > 0 else 0.0

    # ── 14 Advanced Features ──

    # open_position_consistency: where today's open sits in yesterday's range
    if prev_bar and prev_bar.get("high", 0) > 0:
        day_range = prev_bar["high"] - prev_bar["low"]
        raw["open_position_consistency"] = (
            (o - prev_bar["low"]) / day_range if day_range > 0 else 0.5
        )
    else:
        raw["open_position_consistency"] = 0.5

    # volume_price_divergence: return × volume change (last day)
    if len(history) >= 2:
        prev_c = history[-2].get("close", 0)
        last_ret = (history[-1]["close"] - prev_c) / prev_c if prev_c > 0 else 0.0
        prev_v = history[-2].get("volume", 0)
        last_vol_chg = (history[-1]["volume"] - prev_v) / prev_v if prev_v > 0 else 0.0
        raw["volume_price_divergence"] = last_ret * last_vol_chg
    else:
        raw["volume_price_divergence"] = 0.0

    # intraday_momentum_cont: open_gain × gap (continuation signal)
    raw["intraday_momentum_cont"] = raw["open_gain"] * raw["gap"]

    # volume_concentration: volume_amp × early_session_ratio
    raw["volume_concentration"] = raw["volume_amp"] * 0.125

    # relative_strength: stock gain vs market gain
    raw["relative_strength"] = raw["open_gain"] - market_open_gain

    # return_consistency: |mean(ret)| / std(ret) over 20d
    if len(closes) >= 21:
        rets = [
            (closes[i + 1] - closes[i]) / closes[i] * 100
            for i in range(len(closes) - 21, len(closes) - 1)
            if closes[i] > 0
        ]
        if len(rets) >= 2:
            mean_r = statistics.mean(rets)
            std_r = statistics.stdev(rets)
            raw["return_consistency"] = abs(mean_r) / std_r if std_r > 0 else 0.0
        else:
            raw["return_consistency"] = 0.0
    else:
        raw["return_consistency"] = 0.0

    # amplitude_decay: avg amplitude 5d / avg amplitude 20d
    if len(history) >= 20:
        amp_5 = [
            (b["high"] - b["low"]) / b["open"] * 100
            for b in history[-5:]
            if b.get("open", 0) > 0
        ]
        amp_20 = [
            (b["high"] - b["low"]) / b["open"] * 100
            for b in history[-20:]
            if b.get("open", 0) > 0
        ]
        mean_5 = statistics.mean(amp_5) if amp_5 else 0.0
        mean_20 = statistics.mean(amp_20) if amp_20 else 0.0
        raw["amplitude_decay"] = mean_5 / mean_20 if mean_20 > 0 else 1.0
    else:
        raw["amplitude_decay"] = 1.0

    # volume_stability: CV of volumes over 20d
    if len(history) >= 20:
        vols_20 = [b.get("volume", 0) for b in history[-20:]]
        mean_v = statistics.mean(vols_20)
        std_v = statistics.stdev(vols_20) if len(vols_20) >= 2 else 0.0
        raw["volume_stability"] = std_v / mean_v if mean_v > 0 else 0.0
    else:
        raw["volume_stability"] = 0.0

    # close_vs_vwap: close vs typical price proxy of last historical bar
    if len(history) >= 1:
        last = history[-1]
        typical = (last["high"] + last["low"] + last["close"]) / 3
        raw["close_vs_vwap"] = (c - typical) / typical * 100 if typical > 0 else 0.0
    else:
        raw["close_vs_vwap"] = 0.0

    # volume_weighted_return over 20d
    if len(history) >= 21:
        total_vw = 0.0
        total_vol = 0.0
        for i in range(len(history) - 20, len(history)):
            if i > 0 and history[i - 1].get("close", 0) > 0:
                prev_cl = history[i - 1]["close"]
                ret = (history[i]["close"] - prev_cl) / prev_cl * 100
                total_vw += ret * history[i].get("volume", 0)
                total_vol += history[i].get("volume", 0)
        raw["volume_weighted_return"] = total_vw / total_vol if total_vol > 0 else 0.0
    else:
        raw["volume_weighted_return"] = 0.0

    # price_channel_position: (close - 20d_low) / (20d_high - 20d_low)
    if len(history) >= 20:
        highs_20 = [b["high"] for b in history[-20:] if b.get("high", 0) > 0]
        lows_20 = [b["low"] for b in history[-20:] if b.get("low", 0) > 0]
        if highs_20 and lows_20:
            h20, l20 = max(highs_20), min(lows_20)
            channel = h20 - l20
            raw["price_channel_position"] = (c - l20) / channel if channel > 0 else 0.5
        else:
            raw["price_channel_position"] = 0.5
    else:
        raw["price_channel_position"] = 0.5

    # up_day_ratio_20d
    if len(closes) >= 21:
        up_days = sum(
            1 for i in range(len(closes) - 20, len(closes)) if closes[i] > closes[i - 1]
        )
        raw["up_day_ratio_20d"] = up_days / 20.0
    else:
        raw["up_day_ratio_20d"] = 0.5

    # amplitude_20d: avg daily amplitude over 20d (%)
    if len(history) >= 20:
        amps = [
            (b["high"] - b["low"]) / b["open"] * 100
            for b in history[-20:]
            if b.get("open", 0) > 0
        ]
        raw["amplitude_20d"] = statistics.mean(amps) if amps else 0.0
    else:
        raw["amplitude_20d"] = 0.0

    # volume_ratio_5d_20d: avg vol 5d / avg vol 20d
    if len(history) >= 20:
        vol_5d = statistics.mean([b.get("volume", 0) for b in history[-5:]])
        vol_20d = statistics.mean([b.get("volume", 0) for b in history[-20:]])
        raw["volume_ratio_5d_20d"] = vol_5d / vol_20d if vol_20d > 0 else 1.0
    else:
        raw["volume_ratio_5d_20d"] = 1.0

    # ── 11 Cross Features ──
    vol_20 = raw["volatility_20d"]
    raw["momentum_x_mean_reversion"] = (
        raw["trend_5d"] * (1.0 - raw["avg_return_20d"] / vol_20) if vol_20 > 0 else 0.0
    )
    raw["trend_acceleration"] = raw["trend_5d"] - raw["trend_10d"]
    raw["momentum_quality"] = raw["trend_5d"] * raw["volume_amp"]
    raw["volume_trend_interaction"] = raw["volume_ratio_5d_20d"] * raw["trend_5d"]
    raw["gap_volume_interaction"] = raw["gap"] * raw["volume_amp"]
    raw["strength_persistence"] = raw["relative_strength"] * raw["consecutive_up_days"]
    raw["volatility_adj_return"] = raw["avg_return_20d"] / vol_20 if vol_20 > 0 else 0.0
    raw["volume_price_momentum"] = raw["volume_amp"] * raw["open_gain"]
    raw["gap_reversion"] = raw["gap"] * (raw["open_gain"] - raw["gap"])
    vr_5_20 = raw["volume_ratio_5d_20d"]
    raw["trend_volume_divergence"] = raw["trend_5d"] / vr_5_20 if vr_5_20 > 0 else 0.0
    raw["momentum_stability"] = raw["return_consistency"] * raw["trend_5d"]

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

    # Compute market_open_gain (average open_gain across all non-suspended stocks)
    open_gains: list[float] = []
    for code, bar in daily_map.items():
        if bar.get("is_suspended"):
            continue
        o = bar.get("open", 0)
        c = bar.get("close", 0)
        if o > 0 and c > 0:
            open_gains.append((c - o) / o * 100)
    market_open_gain = statistics.mean(open_gains) if open_gains else 0.0

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

    # Z-score normalize across this day's candidates (matching ml_scanner)
    n_raw = len(FEATURE_NAMES_RAW)
    codes_list = list(raw_features_per_code.keys())
    raw_matrix = [raw_features_per_code[c] for c in codes_list]

    means = []
    stds = []
    for j in range(n_raw):
        vals = [row[j] for row in raw_matrix]
        m = statistics.mean(vals)
        s = statistics.stdev(vals) if len(vals) >= 2 else 1.0
        means.append(m)
        stds.append(s)

    features: list[list[float]] = []
    valid_labels: list[int] = []
    for code in codes_list:
        raw_vec = raw_features_per_code[code]
        z_vec = [
            (raw_vec[j] - means[j]) / stds[j] if stds[j] > 0 else 0.0
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
