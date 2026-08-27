"""Production ingestion and calculation service for the MEWS risk curve."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from src.common.config import get_tushare_token
from src.data.clients.greptime_margin_risk import GreptimeMarginRiskStore
from src.margin_risk.calculations import prior_rolling_median
from src.margin_risk.config import MarginRiskConfig
from src.margin_risk.models import DataStatus, RiskState
from src.margin_risk.publication import latest_published_trade_date
from src.margin_risk.tushare_source import TushareMarginRiskSource
from src.margin_risk.universe import is_active_on, is_ordinary_a_stock
from src.margin_risk.v2_calculations import (
    calculate_v2_market_metrics,
    robust_impulse_features,
)
from src.margin_risk.v2_state_machine import V2Thresholds

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

# Frozen after the documented 2014-09-22..2021-12-31 development sample.
# Production never re-fits thresholds from newer observations.
PRODUCTION_THRESHOLDS = V2Thresholds(
    watch=57.864792713230436,
    warning=68.01853488854591,
    clear=49.5389677189997,
    persistent_danger=57.31569647269194,
)

_FFMV_MIN_COVERAGE = 0.98
_SECURITY_BATCH = 100


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sum(rows: Sequence[Mapping[str, Any]], field: str) -> float:
    return sum(value for row in rows if (value := _number(row.get(field))) is not None)


def _history(value: Any) -> list[Any]:
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _empty_security_state(stock_code: str) -> dict[str, Any]:
    return {
        "stock_code": stock_code,
        "current_balance": None,
        "ema_fast_state": None,
        "ema_fast_old_weight": 1.0,
        "ema_slow_state": None,
        "ema_slow_old_weight": 1.0,
        "valid_history": [],
        "net_flow_history": [],
        "impulse_history": [],
    }


def _decode_security_state(row: Mapping[str, Any]) -> dict[str, Any]:
    state = _empty_security_state(str(row["stock_code"]))
    for field in (
        "current_balance",
        "ema_fast_state",
        "ema_fast_old_weight",
        "ema_slow_state",
        "ema_slow_old_weight",
    ):
        state[field] = _number(row.get(field))
    if state["ema_fast_old_weight"] is None:
        state["ema_fast_old_weight"] = 1.0
    if state["ema_slow_old_weight"] is None:
        state["ema_slow_old_weight"] = 1.0
    state["valid_history"] = [bool(value) for value in _history(row.get("valid_history"))]
    state["net_flow_history"] = [
        _number(value) if value is not None else None
        for value in _history(row.get("net_flow_history"))
    ]
    state["impulse_history"] = [
        value for raw in _history(row.get("impulse_history")) if (value := _number(raw)) is not None
    ]
    return state


def _encode_security_state(state: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "stock_code": state["stock_code"],
        "current_balance": state.get("current_balance"),
        "ema_fast_state": state.get("ema_fast_state"),
        "ema_fast_old_weight": state.get("ema_fast_old_weight"),
        "ema_slow_state": state.get("ema_slow_state"),
        "ema_slow_old_weight": state.get("ema_slow_old_weight"),
        "valid_history": json.dumps(state.get("valid_history", []), separators=(",", ":")),
        "net_flow_history": json.dumps(state.get("net_flow_history", []), separators=(",", ":")),
        "impulse_history": json.dumps(state.get("impulse_history", []), separators=(",", ":")),
    }


def _advance_ema(
    value: float | None,
    state: float | None,
    old_weight: float,
    span: int,
) -> tuple[float | None, float | None, float]:
    """Advance the persisted form of ``ema_adjust_false`` by one trading day."""

    if state is None:
        if value is None:
            return None, None, 1.0
        return value, value, 1.0
    alpha = 2.0 / (span + 1.0)
    old_weight *= 1.0 - alpha
    if value is None:
        return None, state, old_weight
    if state != value:
        state = (old_weight * state + alpha * value) / (old_weight + alpha)
    return state, state, 1.0


def _empty_aggregate_bucket() -> dict[str, float]:
    return {
        "valid_balance": 0.0,
        "negative_balance": 0.0,
        "magnitude_balance": 0.0,
        "dlb_valid_balance": 0.0,
        "deleveraging_balance": 0.0,
    }


def _add_feature_to_bucket(bucket: dict[str, float], feature: Mapping[str, Any]) -> None:
    balance = _number(feature.get("financing_balance_prev"))
    if balance is None or balance <= 0:
        return
    impulse_z = _number(feature.get("impulse_z"))
    magnitude = _number(feature.get("negative_impulse_magnitude"))
    if impulse_z is not None:
        bucket["valid_balance"] += balance
        if bool(feature.get("is_negative_impulse_v2")):
            bucket["negative_balance"] += balance
        if magnitude is not None:
            bucket["magnitude_balance"] += balance * magnitude
    net_flow_5d = _number(feature.get("net_flow_5d"))
    if net_flow_5d is not None:
        bucket["dlb_valid_balance"] += balance
        if net_flow_5d < 0:
            bucket["deleveraging_balance"] += balance


def _finish_aggregate_bucket(
    trade_date: date,
    bucket: Mapping[str, float],
    *,
    state_count: int,
    source_updated_at: int | None,
) -> dict[str, Any]:
    valid = bucket["valid_balance"]
    dlb_valid = bucket["dlb_valid_balance"]
    return {
        "trade_date": trade_date,
        "valid_balance": valid if valid > 0 else None,
        "nib_breadth": 100.0 * bucket["negative_balance"] / valid if valid > 0 else None,
        "nib_magnitude": 100.0 * bucket["magnitude_balance"] / valid if valid > 0 else None,
        "dlb": 100.0 * bucket["deleveraging_balance"] / dlb_valid if dlb_valid > 0 else None,
        "state_count": state_count,
        "source_updated_at": source_updated_at,
        "calculation_status": "READY",
    }


class MarginRiskDataError(RuntimeError):
    """An upstream completeness error that must not be read as market safety."""


class MarginRiskProductionService:
    """Audit, backfill, calculate and publish MEWS in production GreptimeDB."""

    def __init__(
        self,
        storage: Any,
        *,
        config: MarginRiskConfig | None = None,
        source_factory: Callable[[], TushareMarginRiskSource] | None = None,
    ) -> None:
        self.store = GreptimeMarginRiskStore(storage)
        self.config = config or MarginRiskConfig.from_env()
        self._source_factory = source_factory or (
            lambda: TushareMarginRiskSource(
                self.config,
                token=get_tushare_token(),
            )
        )
        self._lock = asyncio.Lock()
        self.last_result: dict[str, Any] | None = None

    async def ensure_schema(self) -> None:
        await self.store.ensure_schema()

    @property
    def is_running(self) -> bool:
        """Whether an ingestion/recalculation pass currently owns the service."""

        return self._lock.locked()

    async def audit_and_fill(
        self,
        *,
        start: date | None = None,
        end: date | None = None,
        max_days: int | None = None,
    ) -> dict[str, Any]:
        """Fill every missing/failed trading day, then recompute affected metrics.

        Manual data maintenance calls this without ``max_days`` so the first run
        builds the full history.  Scheduled runs use a small bound and are
        resume-safe through the per-day ``ingestion_status='OK'`` checkpoint.

        The target end is always clamped to the last trading day whose margin
        data upstream has actually published (09:10 Beijing on the next trading
        day).  That publication time is the data's own availability boundary,
        not a failure: runs that start earlier — the 3am maintenance pass, a
        restart bootstrap — simply have nothing newer to fetch, so an
        unpublished day is never counted as a gap or recorded as FAILED.
        """

        if self._lock.locked():
            return {"status": "BUSY", "message": "MEWS data maintenance is already running"}
        async with self._lock:
            await self.ensure_schema()
            start = start or self.config.history_start
            now = datetime.now(BEIJING_TZ)
            requested_end = end

            source = self._source_factory()
            await source.start()
            try:
                # Keep enough future calendar to assign signal_available_date
                # across Spring Festival and other extended exchange closures.
                calendar_end = max(now.date(), requested_end or now.date()) + timedelta(days=31)
                sse = await source.fetch_trade_calendar("SSE", start, calendar_end)
                szse = await source.fetch_trade_calendar("SZSE", start, calendar_end)
                sse_open = {row["cal_date"] for row in sse if row["is_open"]}
                szse_open = {row["cal_date"] for row in szse if row["is_open"]}
                if sse_open != szse_open:
                    raise MarginRiskDataError("SSE/SZSE trade calendars are inconsistent")
                open_days = sorted(sse_open)
                published_through = latest_published_trade_date(open_days, now=now)
                if published_through is None:
                    return self._empty_result(published_through=None)
                end = min(requested_end, published_through) if requested_end else published_through
                if end < start:
                    return self._empty_result(published_through=published_through)
                target_dates = [day for day in open_days if start <= day <= end]
                completed = await self.store.get_complete_dates(start, end)
                missing = [day for day in target_dates if day not in completed]
                if max_days is not None:
                    # Bounded unattended runs prioritize the live end of the
                    # curve; an unbounded manual run still builds oldest-first.
                    limit = max(0, int(max_days))
                    missing = missing[-limit:] if limit else []

                ordinary_stocks: list[Mapping[str, Any]] = []
                ordinary_codes: set[str] = set()
                if missing:
                    stocks = await source.fetch_stock_basic()
                    ordinary_stocks = [row for row in stocks if is_ordinary_a_stock(row)]
                    ordinary_codes = {str(row["ts_code"]) for row in ordinary_stocks}

                filled: list[date] = []
                failed: list[date] = []
                consecutive_failures = 0
                for index, day in enumerate(missing, start=1):
                    logger.info(
                        "MEWS production fill %s (%d/%d)",
                        day,
                        index,
                        len(missing),
                    )
                    try:
                        await self._ingest_day(
                            source,
                            day,
                            ordinary_stocks,
                            ordinary_codes,
                        )
                        filled.append(day)
                        consecutive_failures = 0
                    except Exception as exc:  # noqa: BLE001 - persist failure and resume later
                        failed.append(day)
                        consecutive_failures += 1
                        logger.error("MEWS production ingestion failed for %s: %s", day, exc)
                        await self.store.upsert_market_day(
                            day,
                            {
                                "sse_complete": False,
                                "szse_complete": False,
                                "ingestion_status": "FAILED",
                                "error_message": f"{type(exc).__name__}: {str(exc)[:180]}",
                            },
                        )
                        if consecutive_failures >= 3:
                            break

                metrics = 0
                raw_start, raw_end = await self.store.get_raw_date_range()
                latest_metric = await self.store.get_latest_metric()
                latest_aggregate = await self.store.get_latest_aggregate()
                changed_from: date | None = min(filled) if filled else None
                if changed_from is None and raw_start is not None and raw_end is not None:
                    metric_end = (latest_metric or {}).get("trade_date")
                    if not isinstance(metric_end, date):
                        changed_from = raw_start
                    elif metric_end < raw_end:
                        changed_from = next(
                            (day for day in target_dates if metric_end < day <= raw_end),
                            raw_end,
                        )
                    elif (latest_aggregate or {}).get("trade_date") != raw_end:
                        # Deploying the materialized layer onto an already-current
                        # metric table still needs one bootstrap pass. Recalculate
                        # only the live endpoint after the aggregate/state build.
                        changed_from = raw_end
                if changed_from is not None and raw_end is not None:
                    next_open = next(
                        (day for day in open_days if day > raw_end),
                        None,
                    )
                    metrics = await self.recompute(
                        changed_from=changed_from,
                        next_open=next_open,
                    )
                stored = {day for day in target_dates if day in completed} | set(filled)
                remaining = max(0, len(target_dates) - len(stored))
                # ``latest_complete == published_through`` is the only honest
                # "we are caught up" test: a call can succeed while the newest
                # published day is still missing, and the refresh scheduler
                # retries on exactly that difference.
                latest_complete = max(stored) if stored else None
                result = {
                    "status": "OK" if not failed else "PARTIAL",
                    "target_days": len(target_dates),
                    "filled": len(filled),
                    "failed": [day.isoformat() for day in failed],
                    "remaining": remaining,
                    "metrics": metrics,
                    "published_through": published_through.isoformat(),
                    "latest_complete": latest_complete.isoformat() if latest_complete else None,
                }
                self.last_result = result
                return result
            finally:
                await source.stop()

    def _empty_result(self, *, published_through: date | None) -> dict[str, Any]:
        """Nothing to do — no published day yet, or the window ends before it starts."""

        result: dict[str, Any] = {
            "status": "OK",
            "target_days": 0,
            "filled": 0,
            "failed": [],
            "remaining": 0,
            "metrics": 0,
            "published_through": published_through.isoformat() if published_through else None,
            "latest_complete": None,
        }
        self.last_result = result
        return result

    async def _ingest_day(
        self,
        source: TushareMarginRiskSource,
        day: date,
        ordinary_stocks: Sequence[Mapping[str, Any]],
        ordinary_codes: set[str],
    ) -> None:
        margin_rows = await source.fetch_margin(day)
        by_exchange: dict[str, Mapping[str, Any]] = {}
        for row in margin_rows:
            exchange = str(row.get("exchange_id") or "").upper()
            if exchange in {"SSE", "SZSE"} and all(
                _number(row.get(field)) is not None for field in ("rzye", "rzmre", "rzche")
            ):
                by_exchange[exchange] = row
        if set(by_exchange) != {"SSE", "SZSE"}:
            missing = sorted({"SSE", "SZSE"} - set(by_exchange))
            raise MarginRiskDataError(f"margin missing exchanges: {','.join(missing)}")

        detail_rows = await source.fetch_margin_detail(day)
        security_rows: list[dict[str, Any]] = []
        for row in detail_rows:
            ts_code = str(row.get("ts_code") or "").upper()
            if ts_code not in ordinary_codes:
                continue
            balance = _number(row.get("rzye"))
            buy = _number(row.get("rzmre"))
            repay = _number(row.get("rzche"))
            if balance is None or buy is None or repay is None:
                continue
            security_rows.append(
                {
                    "stock_code": ts_code,
                    "financing_balance": balance,
                    "financing_buy_amount": buy,
                    "financing_repayment_amount": repay,
                }
            )
        if not security_rows:
            raise MarginRiskDataError("margin_detail contains no ordinary A-share rows")

        basic_rows = await source.fetch_daily_basic(day)
        active_codes = {str(row["ts_code"]) for row in ordinary_stocks if is_active_on(row, day)}
        ffmv = 0.0
        ffmv_valid = 0
        for row in basic_rows:
            ts_code = str(row.get("ts_code") or "").upper()
            if ts_code not in active_codes:
                continue
            close = _number(row.get("close"))
            free_share = _number(row.get("free_share"))
            if close is None or close <= 0 or free_share is None or free_share <= 0:
                continue
            ffmv += close * free_share * 10_000.0
            ffmv_valid += 1
        if ffmv <= 0 or ffmv_valid == 0:
            raise MarginRiskDataError("daily_basic contains no usable free-float market cap")

        market_balance = sum(float(by_exchange[key]["rzye"]) for key in ("SSE", "SZSE"))
        market_buy = sum(float(by_exchange[key]["rzmre"]) for key in ("SSE", "SZSE"))
        market_repay = sum(float(by_exchange[key]["rzche"]) for key in ("SSE", "SZSE"))
        stock_balance = _sum(security_rows, "financing_balance")
        stock_buy = _sum(security_rows, "financing_buy_amount")
        stock_repay = _sum(security_rows, "financing_repayment_amount")
        ffmv_expected = len(active_codes)

        await self.store.replace_security_day(day, security_rows)
        await self.store.upsert_market_day(
            day,
            {
                "market_financing_balance": market_balance,
                "market_financing_buy_amount": market_buy,
                "market_financing_repayment_amount": market_repay,
                "stock_financing_balance": stock_balance,
                "stock_financing_buy_amount": stock_buy,
                "stock_financing_repayment_amount": stock_repay,
                "free_float_market_cap": ffmv,
                "margin_security_count": len(security_rows),
                "ffmv_valid_count": ffmv_valid,
                "ffmv_expected_count": ffmv_expected,
                "ordinary_margin_coverage": (
                    stock_balance / market_balance if market_balance > 0 else None
                ),
                "ffmv_coverage": ffmv_valid / ffmv_expected if ffmv_expected > 0 else None,
                "sse_complete": True,
                "szse_complete": True,
                "ingestion_status": "OK",
                "error_message": None,
            },
        )

    def _advance_security_state(
        self,
        state: dict[str, Any],
        current: Mapping[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Advance one stock by one trading day and return today's feature row."""

        balance = _number(current.get("financing_balance")) if current else None
        buy = _number(current.get("financing_buy_amount")) if current else None
        repayment = _number(current.get("financing_repayment_amount")) if current else None
        valid = current is not None and None not in (balance, buy, repayment)
        previous_balance = _number(state.get("current_balance"))
        net_flow = buy - repayment if valid and buy is not None and repayment is not None else None
        flow_rate = (
            net_flow / previous_balance
            if net_flow is not None and previous_balance is not None and previous_balance > 0
            else None
        )

        valid_history = [*state.get("valid_history", []), valid][
            -self.config.security_valid_window :
        ]
        net_flow_history = [*state.get("net_flow_history", []), net_flow][
            -self.config.deleveraging_window :
        ]
        fast_output, fast_state, fast_weight = _advance_ema(
            flow_rate,
            _number(state.get("ema_fast_state")),
            (
                fast_old_weight
                if (fast_old_weight := _number(state.get("ema_fast_old_weight"))) is not None
                else 1.0
            ),
            self.config.ema_fast,
        )
        slow_output, slow_state, slow_weight = _advance_ema(
            flow_rate,
            _number(state.get("ema_slow_state")),
            (
                slow_old_weight
                if (slow_old_weight := _number(state.get("ema_slow_old_weight"))) is not None
                else 1.0
            ),
            self.config.ema_slow,
        )
        eligible = (
            previous_balance is not None
            and previous_balance > 0
            and sum(valid_history) >= self.config.security_min_valid
            and fast_output is not None
            and slow_output is not None
            and flow_rate is not None
        )
        impulse = (
            fast_output - slow_output
            if eligible and fast_output is not None and slow_output is not None
            else None
        )

        impulse_history = list(state.get("impulse_history", []))
        robust_feature: dict[str, Any] = {
            "impulse_z": None,
            "is_negative_impulse_v2": None,
            "negative_impulse_magnitude": None,
        }
        if impulse is not None:
            impulse_history = [*impulse_history, impulse][-self.config.nib_scale_window :]
            robust_feature = robust_impulse_features(
                impulse_history,
                window=self.config.nib_scale_window,
                min_periods=self.config.nib_scale_min_periods,
                threshold=self.config.negative_impulse_z_threshold,
                magnitude_normalizer=self.config.nib_magnitude_normalizer,
            )[-1]

        net_flow_5d: float | None = None
        if len(net_flow_history) == self.config.deleveraging_window and all(
            value is not None for value in net_flow_history
        ):
            net_flow_5d = sum(float(value) for value in net_flow_history if value is not None)

        state.update(
            {
                # The batch formula uses the prior row's balance even when its
                # buy/repayment fields are incomplete, so persist balance
                # independently from today's full-row validity.
                "current_balance": balance,
                "ema_fast_state": fast_state,
                "ema_fast_old_weight": fast_weight,
                "ema_slow_state": slow_state,
                "ema_slow_old_weight": slow_weight,
                "valid_history": valid_history,
                "net_flow_history": net_flow_history,
                "impulse_history": impulse_history,
            }
        )
        if current is None:
            return None
        return {
            "financing_balance_prev": previous_balance,
            "net_flow_5d": net_flow_5d,
            "impulse_z": robust_feature.get("impulse_z"),
            "is_negative_impulse_v2": robust_feature.get("is_negative_impulse_v2"),
            "negative_impulse_magnitude": robust_feature.get("negative_impulse_magnitude"),
        }

    async def recompute(self, *, changed_from: date, next_open: date | None = None) -> int:
        raw_start, raw_end = await self.store.get_raw_date_range()
        if raw_start is None or raw_end is None:
            return 0
        all_market = await self.store.get_market_rows(raw_start, raw_end)
        trading_dates = [row["trade_date"] for row in all_market]
        if not trading_dates:
            return 0

        target_index = next(
            (index for index, day in enumerate(trading_dates) if day >= changed_from),
            len(trading_dates) - 1,
        )
        window_index = max(0, target_index - self.config.calculation_lookback_days)
        window_start = trading_dates[window_index]
        market_rows = all_market[window_index:]
        dates = trading_dates[window_index:]
        count = len(dates)

        await self._ensure_security_materialization(
            raw_start,
            raw_end,
            trading_dates,
            all_market,
        )
        aggregates = await self._security_aggregates(window_start, raw_end, dates)
        ordinary_coverage = [_number(row.get("ordinary_margin_coverage")) for row in market_rows]
        coverage_base = prior_rolling_median(
            ordinary_coverage,
            self.config.detail_coverage_window,
            min_periods=self.config.detail_coverage_min_history,
        )
        coverage_deviation = [
            current - base if current is not None and base is not None else None
            for current, base in zip(ordinary_coverage, coverage_base, strict=True)
        ]
        statuses: list[DataStatus] = []
        history_truncated = raw_start > self.config.history_start
        for index, row in enumerate(market_rows):
            if str(row.get("ingestion_status") or "") != "OK":
                statuses.append(DataStatus.FAILED)
                continue
            ffmv_coverage = _number(row.get("ffmv_coverage"))
            deviation = coverage_deviation[index]
            if (
                ffmv_coverage is None
                or ffmv_coverage < _FFMV_MIN_COVERAGE
                or (deviation is not None and deviation < -self.config.detail_coverage_drop)
                or (history_truncated and index < self.config.calculation_lookback_days)
            ):
                statuses.append(DataStatus.PARTIAL)
            else:
                statuses.append(DataStatus.OK)

        def series(field: str) -> list[float | None]:
            return [_number(row.get(field)) for row in market_rows]

        stock_balance = series("stock_financing_balance")
        previous_balance = [None, *stock_balance[:-1]]
        breadth_coverage = [
            valid / previous if valid is not None and previous and previous > 0 else None
            for valid, previous in zip(
                aggregates["valid_balance"],
                previous_balance,
                strict=True,
            )
        ]
        values: dict[str, Sequence[Any]] = {
            "market_total_balance": series("market_financing_balance"),
            "market_total_buy": series("market_financing_buy_amount"),
            "market_total_repay": series("market_financing_repayment_amount"),
            "stock_balance": stock_balance,
            "stock_buy": series("stock_financing_buy_amount"),
            "stock_repay": series("stock_financing_repayment_amount"),
            "ffmv_stock": series("free_float_market_cap"),
            "nib_sign_v1": [None] * count,
            "nib_breadth_v2": aggregates["nib_breadth"],
            "nib_magnitude_v2": aggregates["nib_magnitude"],
            "dlb": aggregates["dlb"],
            "data_status": statuses,
            "residual_balance": [
                total - stock if total is not None and stock is not None else None
                for total, stock in zip(
                    series("market_financing_balance"),
                    stock_balance,
                    strict=True,
                )
            ],
            "ordinary_coverage": ordinary_coverage,
            "coverage_deviation_60d": coverage_deviation,
            "detail_coverage": ordinary_coverage,
            "breadth_coverage": breadth_coverage,
            "ffmv_coverage": series("ffmv_coverage"),
        }

        prior = await self.store.get_metric_before(window_start)
        try:
            initial_state = RiskState(str((prior or {}).get("risk_state") or "NORMAL"))
        except ValueError:
            initial_state = RiskState.NORMAL
        metrics, thresholds = calculate_v2_market_metrics(
            dates,
            values,
            self.config,
            fixed_thresholds=PRODUCTION_THRESHOLDS,
            initial_risk_state=initial_state,
        )
        if metrics and next_open is not None:
            metrics[-1]["signal_available_date"] = next_open
        for metric in metrics:
            metric.update(
                {
                    "watch_threshold": thresholds.watch,
                    "warning_threshold": thresholds.warning,
                    "clear_threshold": thresholds.clear,
                    "persistent_danger_threshold": thresholds.persistent_danger,
                }
            )
        target_metrics = [row for row in metrics if row["trade_date"] >= changed_from]
        if not target_metrics:
            return 0
        return await self.store.replace_metrics(
            target_metrics[0]["trade_date"],
            target_metrics[-1]["trade_date"],
            target_metrics,
        )

    @staticmethod
    def _source_marker(market_row: Mapping[str, Any]) -> int:
        value = market_row.get("updated_at")
        return int(value) if value is not None else 0

    async def _ensure_security_materialization(
        self,
        raw_start: date,
        raw_end: date,
        trading_dates: Sequence[date],
        market_rows: Sequence[Mapping[str, Any]],
    ) -> None:
        """Build once, then extend the daily security aggregate materialization."""

        stored = await self.store.get_aggregate_rows(raw_start, raw_end)
        stored_by_date = {row["trade_date"]: row for row in stored}
        market_by_date = {row["trade_date"]: row for row in market_rows}
        stale_dates = [
            day
            for day in trading_dates
            if day not in stored_by_date
            or int(stored_by_date[day].get("source_updated_at") or 0)
            != self._source_marker(market_by_date[day])
        ]
        if not stale_dates:
            return

        latest = await self.store.get_latest_aggregate()
        latest_day = (latest or {}).get("trade_date")
        if (
            isinstance(latest_day, date)
            and all(day > latest_day for day in stale_dates)
            and not any(day <= latest_day and day not in stored_by_date for day in trading_dates)
        ):
            state_rows = await self.store.get_security_states(latest_day)
            expected = int((latest or {}).get("state_count") or 0)
            if expected > 0 and len(state_rows) == expected:
                pending_dates = [day for day in trading_dates if day > latest_day]
                await self._extend_security_materialization(
                    latest_day,
                    pending_dates,
                    state_rows,
                    market_by_date,
                )
                return
            logger.warning(
                "MEWS materialized state missing/incomplete at %s: expected=%d actual=%d; "
                "rebuilding",
                latest_day,
                expected,
                len(state_rows),
            )

        logger.info(
            "MEWS rebuilding security materialization %s..%s (first stale day=%s)",
            raw_start,
            raw_end,
            stale_dates[0],
        )
        await self._rebuild_security_materialization(
            raw_start,
            raw_end,
            trading_dates,
            market_by_date,
        )

    async def _rebuild_security_materialization(
        self,
        start: date,
        end: date,
        trading_dates: Sequence[date],
        market_by_date: Mapping[date, Mapping[str, Any]],
    ) -> None:
        """Full bootstrap/repair path; subsequent daily runs use the state checkpoint."""

        buckets = {day: _empty_aggregate_bucket() for day in trading_dates}
        codes = await self.store.get_security_codes(start, end)
        encoded_states: list[dict[str, Any]] = []
        for offset in range(0, len(codes), _SECURITY_BATCH):
            batch = codes[offset : offset + _SECURITY_BATCH]
            rows = await self.store.get_security_rows(start, end, batch)
            by_code: dict[str, dict[date, dict[str, Any]]] = defaultdict(dict)
            for row in rows:
                by_code[str(row["stock_code"])][row["trade_date"]] = row
            for code in batch:
                state = _empty_security_state(code)
                for day in trading_dates:
                    feature = self._advance_security_state(state, by_code[code].get(day))
                    if feature is not None:
                        _add_feature_to_bucket(buckets[day], feature)
                encoded_states.append(_encode_security_state(state))

        state_count = len(encoded_states)
        aggregate_rows = [
            _finish_aggregate_bucket(
                day,
                buckets[day],
                state_count=state_count,
                source_updated_at=self._source_marker(market_by_date[day]),
            )
            for day in trading_dates
        ]
        # The aggregate row is the READY checkpoint, so publish it only after
        # the complete state generation has been stored.
        await self.store.replace_security_states(end, encoded_states)
        await self.store.replace_aggregate_rows(start, end, aggregate_rows)
        await self.store.prune_security_states_before(end)
        logger.info(
            "MEWS security materialization rebuilt: dates=%d states=%d",
            len(aggregate_rows),
            state_count,
        )

    async def _extend_security_materialization(
        self,
        previous_day: date,
        pending_dates: Sequence[date],
        stored_states: Sequence[Mapping[str, Any]],
        market_by_date: Mapping[date, Mapping[str, Any]],
    ) -> None:
        if not pending_dates:
            return
        states = {str(row["stock_code"]): _decode_security_state(row) for row in stored_states}
        rows = await self.store.get_all_security_rows(pending_dates[0], pending_dates[-1])
        by_date: dict[date, dict[str, dict[str, Any]]] = defaultdict(dict)
        for row in rows:
            by_date[row["trade_date"]][str(row["stock_code"])] = row

        last_day = previous_day
        for day in pending_dates:
            current_rows = by_date.get(day, {})
            for code in current_rows:
                states.setdefault(code, _empty_security_state(code))
            bucket = _empty_aggregate_bucket()
            for code in sorted(states):
                feature = self._advance_security_state(states[code], current_rows.get(code))
                if feature is not None:
                    _add_feature_to_bucket(bucket, feature)

            encoded_states = [_encode_security_state(states[code]) for code in sorted(states)]
            aggregate = _finish_aggregate_bucket(
                day,
                bucket,
                state_count=len(encoded_states),
                source_updated_at=self._source_marker(market_by_date[day]),
            )
            await self.store.replace_security_states(day, encoded_states)
            await self.store.replace_aggregate_day(day, aggregate)
            await self.store.prune_security_states_before(day)
            last_day = day
            logger.info(
                "MEWS security materialization advanced: %s states=%d",
                day,
                len(encoded_states),
            )
        logger.info(
            "MEWS security materialization caught up: %s -> %s",
            previous_day,
            last_day,
        )

    async def _security_aggregates(
        self,
        start: date,
        end: date,
        trading_dates: Sequence[date],
    ) -> dict[str, list[float | None]]:
        rows = await self.store.get_aggregate_rows(start, end)
        by_date = {row["trade_date"]: row for row in rows}
        missing = [day for day in trading_dates if day not in by_date]
        if missing:
            raise MarginRiskDataError(
                "MEWS security materialization missing dates: "
                + ",".join(day.isoformat() for day in missing[:5])
            )
        return {
            "valid_balance": [_number(by_date[day].get("valid_balance")) for day in trading_dates],
            "nib_breadth": [_number(by_date[day].get("nib_breadth")) for day in trading_dates],
            "nib_magnitude": [_number(by_date[day].get("nib_magnitude")) for day in trading_dates],
            "dlb": [_number(by_date[day].get("dlb")) for day in trading_dates],
        }
