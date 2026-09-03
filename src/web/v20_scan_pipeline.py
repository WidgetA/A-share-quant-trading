"""Prewarmed, notification-free V16 scan pipeline consumed by V20."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Sequence

from src.data.clients.tushare_realtime import TushareDailyBar, TushareQuote
from src.data.clients.v20_market_data import ExactEarlySnapshot
from src.strategy.lgbrank_scorer import LGBRankScorer
from src.strategy.strategies.v16_scanner import V16Scanner, V16ScanResult, V16StockData
from src.strategy.v20.models import V20_V16_SNAPSHOT_SCHEMA
from src.web.v20_canonical_selection import (
    BEIJING_TZ,
    LOOKBACK_DAYS,
    V20CanonicalSelectionState,
    _build_stock_data,
    _fetch_history_ohlcv,
    _fetch_prev_closes,
)


class V20ScanPipelineError(RuntimeError):
    pass


MINIMUM_HISTORY_COVERAGE = 0.8
HISTORY_PROFILE_ID = "STRICT_LAST_37_EXCHANGE_SESSIONS_V1"


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _history_hash(raw: Mapping[str, Any]) -> str:
    return _canonical_hash(
        {key: raw.get(key, []) for key in ("time", "open", "high", "low", "close", "volume")}
    )


def _normalize_history(
    raw: Mapping[str, Any],
    *,
    trade_date: date,
    allowed_dates: frozenset[date] | None = None,
    required_dates: Sequence[date] | None = None,
) -> dict[str, list[Any]] | None:
    """Validate and freeze the last 37 actual trading bars for one stock.

    V20 requires one real row for every one of the latest 37 exchange sessions.
    It never pads a suspended/missing day or substitutes an older observation.
    Malformed arrays, duplicate/out-of-order dates, future rows, non-finite
    values, and impossible OHLC relations make that stock ineligible instead
    of silently changing its model features.
    """

    fields = ("time", "open", "high", "low", "close", "volume")
    values: dict[str, Sequence[Any]] = {}
    for field in fields:
        item = raw.get(field)
        if not isinstance(item, Sequence) or isinstance(item, (str, bytes, bytearray)):
            return None
        values[field] = item
    lengths = {len(item) for item in values.values()}
    if len(lengths) != 1:
        return None

    normalized: dict[str, list[Any]] = {field: [] for field in fields}
    previous_date: date | None = None
    for index, raw_day in enumerate(values["time"]):
        if not isinstance(raw_day, str):
            return None
        try:
            row_date = datetime.strptime(raw_day, "%Y-%m-%d").date()
            numeric_values = tuple(
                values[field][index] for field in ("open", "high", "low", "close", "volume")
            )
            # ``bool`` is an ``int`` subclass in Python.  Accepting True as
            # 1.0 would let a malformed provider/adapter row enter the model.
            if any(isinstance(item, bool) for item in numeric_values):
                return None
            o, h, low, close, volume = (float(item) for item in numeric_values)
        except (TypeError, ValueError, OverflowError):
            return None
        if previous_date is not None and row_date <= previous_date:
            return None
        if row_date >= trade_date:
            return None
        if allowed_dates is not None and row_date not in allowed_dates:
            return None
        if not all(math.isfinite(item) for item in (o, h, low, close, volume)):
            return None
        if (
            min(o, h, low, close) <= 0
            or volume <= 0
            or low > min(o, close)
            or h < max(o, close)
            or low > h
        ):
            return None
        normalized["time"].append(row_date.isoformat())
        normalized["open"].append(o)
        normalized["high"].append(h)
        normalized["low"].append(low)
        normalized["close"].append(close)
        normalized["volume"].append(volume)
        previous_date = row_date

    if len(normalized["time"]) < LOOKBACK_DAYS:
        return None
    frozen = {field: rows[-LOOKBACK_DAYS:] for field, rows in normalized.items()}
    if required_dates is not None and frozen["time"] != [
        item.isoformat() for item in required_dates
    ]:
        return None
    return frozen


def _history_date_coverage(
    history: Mapping[str, Mapping[str, Sequence[Any]]],
    expected_dates: Sequence[date],
    *,
    universe_size: int,
) -> tuple[dict[str, int], float]:
    if universe_size <= 0 or not expected_dates:
        raise ValueError("history coverage needs a universe and expected dates")
    counts = {
        day.isoformat(): sum(day.isoformat() in raw["time"] for raw in history.values())
        for day in expected_dates
    }
    return counts, min(counts.values()) / universe_size


@dataclass(frozen=True)
class V20PrewarmedScan:
    trade_date: date
    calendar: tuple[date, ...]
    scanner: V16Scanner
    scorer_model_sha256: str
    scorer_feature_sha256: str
    clean_boards: Mapping[str, list[tuple[str, str]]]
    universe_codes: tuple[str, ...]
    breadth_codes: tuple[str, ...]
    required_minute_codes: tuple[str, ...]
    prev_closes: Mapping[str, float]
    prior_daily: Mapping[str, TushareDailyBar]
    prior_trade_date: date
    history: Mapping[str, Mapping[str, Any]]
    history_hashes: Mapping[str, str]
    history_date_valid_counts: Mapping[str, int]
    history_min_date_coverage: float
    names: Mapping[str, str]
    prepared_at: datetime


@dataclass(frozen=True)
class FrozenV16ScanBundle:
    trade_date: date
    frozen_at: datetime
    scan_result: V16ScanResult
    stock_data: Mapping[str, V16StockData]
    comparison_pool_codes: tuple[str, ...]
    breadth_valid_n: int
    breadth_down_n: int
    prior_trade_date: date
    prior_amount_yuan: Mapping[str, float]
    snapshot: Mapping[str, Any]
    snapshot_hash: str
    computation_calendar: tuple[date, ...] = ()
    # Portable artifact v2 persists the already-computed legacy V16 Top-1
    # projection here.  It is deliberately separate from ``stock_data``:
    # restart hydration omits the full raw/history-heavy stock objects.
    legacy_recommendation: Mapping[str, Any] | None = None


class V20ScanPipeline:
    def __init__(self, scan_state: V20CanonicalSelectionState, project_root: Any) -> None:
        self._scan_state = scan_state
        self._project_root = project_root

    async def prewarm(
        self,
        trade_date: date,
        *,
        calendar: Sequence[date],
    ) -> V20PrewarmedScan:
        state = self._scan_state
        if not state.initialized:
            raise V20ScanPipelineError("scan resources are not initialized")
        scorer = LGBRankScorer(
            self._project_root / "models" / "lgbrank_latest.txt",
            self._project_root / "models" / "feature_list.json",
        )
        scanner = V16Scanner(
            fundamentals_db=state.fundamentals_db,
            concept_mapper=state.concept_mapper,
            stock_filter=state.stock_filter,
            scorer=scorer,
        )
        clean_boards, raw_universe = scanner.get_universe()
        universe = tuple(sorted(raw_universe))
        if not universe:
            raise V20ScanPipelineError("V16 clean universe is empty")

        frozen_calendar = tuple(calendar)
        if trade_date not in frozen_calendar:
            raise V20ScanPipelineError(f"{trade_date} is not an exchange trade date")
        previous = [day for day in frozen_calendar if day < trade_date]
        if not previous:
            raise V20ScanPipelineError("previous trade date is unavailable")
        configure_calendar = getattr(state.historical_adapter, "set_exchange_trade_calendar", None)
        if callable(configure_calendar):
            configure_calendar(frozen_calendar)
        prior_trade_date = previous[-1]
        prev_closes = await _fetch_prev_closes(state, trade_date, list(frozen_calendar))
        breadth_codes = tuple(
            sorted(code for code in prev_closes if len(code) == 6 and code.startswith(("00", "60")))
        )
        if not breadth_codes:
            raise V20ScanPipelineError("main-board breadth universe is empty")
        required = tuple(sorted(set(universe).union(breadth_codes)))

        if len(previous) < LOOKBACK_DAYS:
            raise V20ScanPipelineError("fewer than 37 prior exchange sessions are available")
        expected_dates = tuple(previous[-LOOKBACK_DAYS:])
        raw_history = await _fetch_history_ohlcv(
            state.historical_adapter, list(universe), trade_date
        )
        history = {
            code: normalized
            for code in universe
            if (
                normalized := _normalize_history(
                    raw_history.get(code, {}),
                    trade_date=trade_date,
                    allowed_dates=frozenset(previous),
                    required_dates=expected_dates,
                )
            )
            is not None
        }
        history_coverage = len(history) / len(universe)
        if history_coverage < MINIMUM_HISTORY_COVERAGE:
            raise V20ScanPipelineError(
                f"legal 37-bar history coverage {len(history)}/{len(universe)} below 80%"
            )

        history_date_valid_counts, history_min_date_coverage = _history_date_coverage(
            history,
            expected_dates,
            universe_size=len(universe),
        )
        if history_min_date_coverage < MINIMUM_HISTORY_COVERAGE:
            weakest_day = min(
                history_date_valid_counts,
                key=lambda item: history_date_valid_counts[item],
            )
            weakest_count = history_date_valid_counts[weakest_day]
            raise V20ScanPipelineError(
                "daily history source coverage "
                f"{weakest_count}/{len(universe)} on {weakest_day} below 80%"
            )
        history_hashes = {code: _history_hash(raw) for code, raw in history.items()}

        names: dict[str, str] = {}
        if state.fundamentals_db is not None:
            fundamentals = await state.fundamentals_db.batch_get_fundamentals(list(universe))
            names = {code: value.company_name for code, value in fundamentals.items()}

        prior_daily = await state.realtime_client.fetch_daily_bars(
            prior_trade_date.strftime("%Y%m%d")
        )
        return V20PrewarmedScan(
            trade_date=trade_date,
            calendar=frozen_calendar,
            scanner=scanner,
            scorer_model_sha256=scorer.model_sha256,
            scorer_feature_sha256=scorer.feature_list_sha256,
            clean_boards=clean_boards,
            universe_codes=universe,
            breadth_codes=breadth_codes,
            required_minute_codes=required,
            prev_closes=prev_closes,
            prior_daily=prior_daily,
            prior_trade_date=prior_trade_date,
            history=history,
            history_hashes=history_hashes,
            history_date_valid_counts=history_date_valid_counts,
            history_min_date_coverage=history_min_date_coverage,
            names=names,
            prepared_at=datetime.now(BEIJING_TZ),
        )

    async def scan(
        self,
        prewarmed: V20PrewarmedScan,
        early: ExactEarlySnapshot,
        *,
        breadth_early: ExactEarlySnapshot,
        minimum_quote_coverage: float,
    ) -> FrozenV16ScanBundle:
        if early.trade_date != prewarmed.trade_date:
            raise V20ScanPipelineError("prewarm and early snapshot dates differ")
        if early.last_complete_label != "09:39":
            raise V20ScanPipelineError("V20 entry snapshot must end at raw 09:39")
        if breadth_early.trade_date != prewarmed.trade_date:
            raise V20ScanPipelineError("prewarm and breadth snapshot dates differ")
        if breadth_early.last_complete_label != "09:39":
            raise V20ScanPipelineError("V20 breadth snapshot must end at raw 09:39")

        universe_quotes = {
            code: quote for code, quote in early.quotes.items() if code in prewarmed.universe_codes
        }
        coverage = len(universe_quotes) / len(prewarmed.universe_codes)
        if coverage < minimum_quote_coverage:
            raise V20ScanPipelineError(
                f"exact-09:39 coverage {len(universe_quotes)}/{len(prewarmed.universe_codes)} "
                f"below {minimum_quote_coverage:.0%}"
            )

        stock_data: dict[str, V16StockData] = {}
        failures: list[str] = []
        for code in prewarmed.universe_codes:
            quote = universe_quotes.get(code)
            previous_close = prewarmed.prev_closes.get(code)
            history = prewarmed.history.get(code)
            if quote is None or previous_close is None or history is None:
                failures.append(code)
                continue
            try:
                built = _build_stock_data(
                    code,
                    prewarmed.names.get(code, ""),
                    quote,
                    previous_close,
                    dict(history),
                    prewarmed.trade_date,
                )
            except RuntimeError:
                failures.append(code)
                continue
            if built is not None:
                stock_data[code] = built

        failures = sorted(set(prewarmed.universe_codes) - set(stock_data))
        if not stock_data or len(failures) > len(prewarmed.universe_codes) * 0.2:
            raise V20ScanPipelineError(
                f"V16 stock-data failures {len(failures)}/{len(prewarmed.universe_codes)}"
            )
        result = await prewarmed.scanner.scan(stock_data, dict(prewarmed.clean_boards))
        frozen_at = datetime.now(BEIJING_TZ)

        breadth_valid_n = 0
        breadth_down_n = 0
        for code in prewarmed.breadth_codes:
            quote = breadth_early.quotes.get(code)
            previous_close = prewarmed.prev_closes.get(code)
            if not _valid_breadth_pair(quote, previous_close):
                continue
            assert quote is not None and quote.early_close is not None
            assert previous_close is not None
            breadth_valid_n += 1
            if quote.early_close < float(previous_close):
                breadth_down_n += 1

        # HEALTH's comparison pool is the scanner's static clean universe, not
        # the subset that happened to have enough 09:39/history data to enter
        # today's ranking.  Reference/T+2 price validity is applied per code at
        # maturity, exactly once, by the shadow evaluator.
        comparison_codes = prewarmed.universe_codes
        symbols = []
        for stock in result.recommended:
            data = stock_data[stock.code]
            symbols.append(
                {
                    "rank": stock.rank,
                    "code": stock.code,
                    "name": stock.name,
                    "score": stock.score,
                    "snapshot_price": stock.buy_price,
                    "boards": list(result.stock_all_boards.get(stock.code, [])),
                    "best_board": result.stock_best_board.get(stock.code),
                    "is_driver": result.stock_is_driver.get(stock.code),
                    "cci": result.stock_cci.get(stock.code),
                    "volume_937": result.stock_early_vol.get(stock.code, data.volume_937),
                    "history_hash": prewarmed.history_hashes[stock.code],
                }
            )
        snapshot: dict[str, Any] = {
            "schema_version": V20_V16_SNAPSHOT_SCHEMA,
            "trade_date": prewarmed.trade_date.isoformat(),
            "last_complete_bar": early.last_complete_label,
            "early_market_source_hash": early.source_hash,
            "early_market_conflict_codes": list(early.conflict_codes),
            "breadth_market_source_hash": breadth_early.source_hash,
            "breadth_market_missing_codes": list(breadth_early.missing_codes),
            "breadth_market_conflict_codes": list(breadth_early.conflict_codes),
            "scorer_model_sha256": prewarmed.scorer_model_sha256,
            "scorer_feature_sha256": prewarmed.scorer_feature_sha256,
            "list_complete": True,
            "list_n": len(symbols),
            "symbols": symbols,
            "scan_input_codes": sorted(stock_data),
            "scan_input_failure_codes": sorted(failures),
            "scan_input_coverage": len(stock_data) / len(prewarmed.universe_codes),
            "history_profile_id": HISTORY_PROFILE_ID,
            "history_input_hashes": dict(prewarmed.history_hashes),
            "history_date_valid_counts": dict(prewarmed.history_date_valid_counts),
            "history_min_date_coverage": prewarmed.history_min_date_coverage,
            "comparison_pool_codes": list(comparison_codes),
            "comparison_pool_hash": _canonical_hash(comparison_codes),
            "breadth_valid_n": breadth_valid_n,
            "breadth_down_n": breadth_down_n,
            "prior_trade_date": prewarmed.prior_trade_date.isoformat(),
            "prior_amount_yuan": {
                code: prewarmed.prior_daily[code].amount_yuan
                for code in sorted(prewarmed.prior_daily)
                if code in {item["code"] for item in symbols}
            },
            "funnel": {
                "step0_universe_count": result.step0_universe_count,
                "step2_hot_board_count": result.step2_hot_board_count,
                "final_candidates": result.final_candidates,
            },
            # Keep the same board-gain evidence shown by the online V16
            # Feishu report.  Freezing it in the snapshot prevents a later
            # renderer/retry from looking up a different intraday value.
            "board_avg_gains": dict(sorted(result.step2_board_avg_gains.items())),
        }
        snapshot_hash = _canonical_hash(snapshot)
        return FrozenV16ScanBundle(
            trade_date=prewarmed.trade_date,
            frozen_at=frozen_at,
            scan_result=result,
            stock_data=stock_data,
            comparison_pool_codes=comparison_codes,
            breadth_valid_n=breadth_valid_n,
            breadth_down_n=breadth_down_n,
            prior_trade_date=prewarmed.prior_trade_date,
            prior_amount_yuan={
                code: row.amount_yuan for code, row in prewarmed.prior_daily.items()
            },
            snapshot=snapshot,
            snapshot_hash=snapshot_hash,
        )


def _valid_breadth_pair(quote: TushareQuote | None, previous_close: float | None) -> bool:
    if quote is None or previous_close is None:
        return False
    return quote.early_close > 0 and previous_close > 0


__all__ = [
    "FrozenV16ScanBundle",
    "V20PrewarmedScan",
    "V20ScanPipeline",
    "V20ScanPipelineError",
]
