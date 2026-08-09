"""Production ingestion and calculation service for the MEWS risk curve."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from src.common.config import get_tushare_token
from src.data.clients.greptime_margin_risk import GreptimeMarginRiskStore
from src.margin_risk.calculations import (
    calculate_security_features,
    prior_rolling_median,
)
from src.margin_risk.config import MarginRiskConfig
from src.margin_risk.models import DataStatus, RiskState
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
        """

        if self._lock.locked():
            return {"status": "BUSY", "message": "MEWS data maintenance is already running"}
        async with self._lock:
            await self.ensure_schema()
            start = start or self.config.history_start
            end = end or (datetime.now(BEIJING_TZ).date() - timedelta(days=1))
            if end < start:
                result = {"status": "OK", "filled": 0, "failed": [], "metrics": 0}
                self.last_result = result
                return result

            source = self._source_factory()
            await source.start()
            try:
                # Keep enough future calendar to assign signal_available_date
                # across Spring Festival and other extended exchange closures.
                calendar_end = end + timedelta(days=31)
                sse = await source.fetch_trade_calendar("SSE", start, calendar_end)
                szse = await source.fetch_trade_calendar("SZSE", start, calendar_end)
                sse_open = {row["cal_date"] for row in sse if row["is_open"]}
                szse_open = {row["cal_date"] for row in szse if row["is_open"]}
                if sse_open != szse_open:
                    raise MarginRiskDataError("SSE/SZSE trade calendars are inconsistent")
                target_dates = sorted(day for day in sse_open if start <= day <= end)
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
                if changed_from is not None and raw_end is not None:
                    next_open = next(
                        (day for day in sorted(sse_open) if day > raw_end),
                        None,
                    )
                    metrics = await self.recompute(
                        changed_from=changed_from,
                        next_open=next_open,
                    )
                remaining = max(0, len(target_dates) - len(completed | set(filled)))
                result = {
                    "status": "OK" if not failed else "PARTIAL",
                    "target_days": len(target_dates),
                    "filled": len(filled),
                    "failed": [day.isoformat() for day in failed],
                    "remaining": remaining,
                    "metrics": metrics,
                }
                self.last_result = result
                return result
            finally:
                await source.stop()

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
        active_codes = {
            str(row["ts_code"]) for row in ordinary_stocks if is_active_on(row, day)
        }
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

        aggregates = await self._security_aggregates(window_start, raw_end, dates)
        ordinary_coverage = [
            _number(row.get("ordinary_margin_coverage")) for row in market_rows
        ]
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

    async def _security_aggregates(
        self,
        start: date,
        end: date,
        trading_dates: Sequence[date],
    ) -> dict[str, list[float | None]]:
        count = len(trading_dates)
        date_index = {day: index for index, day in enumerate(trading_dates)}
        valid_balance = [0.0] * count
        negative_balance = [0.0] * count
        magnitude_balance = [0.0] * count
        dlb_valid_balance = [0.0] * count
        deleveraging_balance = [0.0] * count
        codes = await self.store.get_security_codes(start, end)

        for offset in range(0, len(codes), _SECURITY_BATCH):
            batch = codes[offset : offset + _SECURITY_BATCH]
            rows = await self.store.get_security_rows(start, end, batch)
            by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in rows:
                by_code[str(row["stock_code"])].append(row)
            for code in batch:
                features = calculate_security_features(
                    trading_dates,
                    by_code.get(code, []),
                    self.config,
                )
                robust = robust_impulse_features(
                    [_number(feature.get("impulse_raw")) for feature in features],
                    window=self.config.nib_scale_window,
                    min_periods=self.config.nib_scale_min_periods,
                    threshold=self.config.negative_impulse_z_threshold,
                    magnitude_normalizer=self.config.nib_magnitude_normalizer,
                )
                for feature, robust_feature in zip(features, robust, strict=True):
                    index = date_index[feature["trade_date"]]
                    balance = _number(feature.get("financing_balance_prev"))
                    if balance is None or balance <= 0:
                        continue
                    impulse_z = _number(robust_feature.get("impulse_z"))
                    magnitude = _number(robust_feature.get("negative_impulse_magnitude"))
                    if impulse_z is not None:
                        valid_balance[index] += balance
                        if bool(robust_feature.get("is_negative_impulse_v2")):
                            negative_balance[index] += balance
                        if magnitude is not None:
                            magnitude_balance[index] += balance * magnitude
                    net_flow_5d = _number(feature.get("net_flow_5d"))
                    if net_flow_5d is not None:
                        dlb_valid_balance[index] += balance
                        if net_flow_5d < 0:
                            deleveraging_balance[index] += balance

        def percent(numerator: Sequence[float], denominator: Sequence[float]) -> list[float | None]:
            return [
                100.0 * left / right if right > 0 else None
                for left, right in zip(numerator, denominator, strict=True)
            ]

        return {
            "valid_balance": [value if value > 0 else None for value in valid_balance],
            "nib_breadth": percent(negative_balance, valid_balance),
            "nib_magnitude": percent(magnitude_balance, valid_balance),
            "dlb": percent(deleveraging_balance, dlb_valid_balance),
        }
