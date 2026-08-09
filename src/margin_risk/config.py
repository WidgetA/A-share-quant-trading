"""MEWS 的集中参数配置；公式和阈值不得散落到业务模块。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


@dataclass(frozen=True, slots=True)
class MarginRiskConfig:
    """可复现的融资耗竭指数配置。"""

    history_start: date = date(2014, 9, 22)
    ema_fast: int = 5
    ema_slow: int = 20
    rank_window: int = 500
    rank_min_periods: int = 120
    load_base_window: int = 20
    security_valid_window: int = 25
    security_min_valid: int = 20
    deleveraging_window: int = 5
    watch_percentile: float = 85.0
    warning_percentile: float = 95.0
    confirm_percentile: float = 85.0
    emergency_dlb: float = 75.0
    emergency_rpp: float = 99.0
    clear_percentile: float = 75.0
    clear_days: int = 5
    episode_gap_days: int = 5
    backtest_horizon: int = 20
    crash_stock_drawdown: float = -0.20
    crash_market_quantile: float = 0.05
    crash_breadth_quantile: float = 0.95
    development_end: date = date(2021, 12, 31)
    validation_start: date = date(2022, 1, 1)
    validation_end: date = date(2023, 12, 31)
    out_of_sample_start: date = date(2024, 1, 1)
    event_search_window: int = 60
    index_version: str = "mews_v1"
    v2_index_version: str = "mews_v2"
    nib_scale_window: int = 60
    nib_scale_min_periods: int = 40
    negative_impulse_z_threshold: float = -0.25
    nib_magnitude_normalizer: float = 2.75
    persistent_weakness_horizon: int = 60
    persistent_weakness_quantile: float = 0.10
    monte_carlo_sets: int = 10_000
    bootstrap_resamples: int = 5_000
    bootstrap_block_length: int = 20
    validation_random_seed: int = 20_260_806
    detail_coverage_window: int = 60
    detail_coverage_min_history: int = 20
    detail_coverage_drop: float = 0.03
    identity_absolute_tolerance: float = 1000.0
    identity_relative_tolerance: float = 1e-9
    calculation_lookback_days: int = 550
    stock_batch_size: int = 100
    write_batch_size: int = 500
    request_interval_seconds: float = 0.25
    max_retries: int = 4
    retry_backoff_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> "MarginRiskConfig":
        """Only operational knobs are environment-overridable.

        Formula parameters remain fixed in source and are versioned through
        ``index_version`` so one experiment run cannot silently change the index.
        """

        return cls(
            request_interval_seconds=max(
                0.0,
                _env_float("MARGIN_RISK_REQUEST_INTERVAL_SECONDS", 0.25),
            ),
            max_retries=max(1, _env_int("MARGIN_RISK_MAX_RETRIES", 4)),
        )


DEFAULT_CONFIG = MarginRiskConfig.from_env()

# Public constants mirror the fixed specification and make accidental drift
# obvious in code review and test failures.
EMA_FAST = 5
EMA_SLOW = 20
RANK_WINDOW = 500
RANK_MIN_PERIODS = 120
LOAD_BASE_WINDOW = 20
SECURITY_VALID_WINDOW = 25
SECURITY_MIN_VALID = 20
DELEVERAGING_WINDOW = 5
WATCH_PERCENTILE = 85
WARNING_PERCENTILE = 95
CONFIRM_PERCENTILE = 85
EMERGENCY_DLB = 75
EMERGENCY_RPP = 99
CLEAR_PERCENTILE = 75
CLEAR_DAYS = 5
EPISODE_GAP_DAYS = 5
BACKTEST_HORIZON = 20
CRASH_STOCK_DRAWDOWN = -0.20
CRASH_MARKET_QUANTILE = 0.05
CRASH_BREADTH_QUANTILE = 0.95
INDEX_VERSION = "mews_v1"
INDEX_VERSION_V2 = "mews_v2"
NIB_SCALE_WINDOW = 60
NIB_SCALE_MIN_PERIODS = 40
NEGATIVE_IMPULSE_Z_THRESHOLD = -0.25
NIB_MAGNITUDE_NORMALIZER = 2.75
PERSISTENT_WEAKNESS_HORIZON = 60
PERSISTENT_WEAKNESS_QUANTILE = 0.10
MONTE_CARLO_SETS = 10_000
BOOTSTRAP_RESAMPLES = 5_000
BOOTSTRAP_BLOCK_LENGTH = 20
VALIDATION_RANDOM_SEED = 20_260_806
