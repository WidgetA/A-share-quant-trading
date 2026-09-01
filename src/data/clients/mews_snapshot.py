"""Calculate the frozen MEWS v2 indicator from Tushare source material.

V20 owns this calculation.  It does not call another service for a computed
MEWS value and it does not depend on positions, orders, or the model ledger.
The compact bootstrap contains only the trailing state needed to extend the
frozen formula one trading day at a time.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time
from pathlib import Path
from statistics import median
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from src.data.clients.tushare_realtime import TushareRealtimeClient

SHANGHAI = ZoneInfo("Asia/Shanghai")
MEWS_MODEL_VERSION = "mews_v2"
MEWS_PUBLISH_TIME = time(9, 10)
MEWS_STATE_SCHEMA = "v20-mews-incremental-state/v1"

EMA_FAST = 5
EMA_SLOW = 20
RANK_WINDOW = 500
RANK_MIN_PERIODS = 120
LOAD_BASE_WINDOW = 20
SECURITY_VALID_WINDOW = 25
SECURITY_MIN_VALID = 20
DELEVERAGING_WINDOW = 5
NIB_SCALE_WINDOW = 60
NIB_SCALE_MIN_PERIODS = 40
NEGATIVE_IMPULSE_Z_THRESHOLD = -0.25
NIB_MAGNITUDE_NORMALIZER = 2.75
DETAIL_COVERAGE_WINDOW = 60
DETAIL_COVERAGE_MIN_HISTORY = 20
DETAIL_COVERAGE_DROP = 0.03
FFMV_MIN_COVERAGE = 0.98
CLEAR_DAYS = 5

WATCH_THRESHOLD = 57.864792713230436
WARNING_THRESHOLD = 68.01853488854591
CLEAR_THRESHOLD = 49.5389677189997
PERSISTENT_DANGER_THRESHOLD = 57.31569647269194

_LEVEL = {"NORMAL": 0, "WATCH": 1, "WARNING": 2, "DANGER": 3}
_ONE_DOWN = {
    "DANGER": "WARNING",
    "WARNING": "WATCH",
    "WATCH": "WATCH",
    "NORMAL": "NORMAL",
}
_ORDINARY_MARKETS = {"\u4e3b\u677f", "\u521b\u4e1a\u677f", "\u79d1\u521b\u677f"}
_SSE_PREFIXES = ("600", "601", "603", "605", "688")
_SZSE_PREFIXES = ("000", "001", "002", "003", "300", "301")


class MewsSnapshotSourceError(RuntimeError):
    """Raw source material or persisted calculation state is unusable."""


class _StateRepository(Protocol):
    async def load_mews_calculation_state(self) -> Mapping[str, Any] | None: ...

    async def save_mews_calculation_state(self, state: Mapping[str, Any]) -> str: ...


class _RawClient(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def query(
        self,
        api_name: str,
        params: Mapping[str, Any],
        fields: Sequence[str],
        *,
        allow_empty: bool = False,
    ) -> list[dict[str, Any]]: ...


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _finite(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_day(value: Any) -> date | None:
    raw = str(value or "").replace("-", "")
    if len(raw) != 8 or not raw.isdigit():
        return None
    return datetime.strptime(raw, "%Y%m%d").date()


def _ema(values: Sequence[float | None], span: int) -> list[float | None]:
    alpha = 2.0 / (span + 1.0)
    state: float | None = None
    old_weight = 1.0
    output: list[float | None] = []
    for value in values:
        if state is None:
            if value is not None:
                state = value
                old_weight = 1.0
            output.append(state)
            continue
        old_weight *= 1.0 - alpha
        if value is None:
            output.append(None)
            continue
        if state != value:
            state = (old_weight * state + alpha * value) / (old_weight + alpha)
        old_weight = 1.0
        output.append(state)
    return output


def _advance_ema(
    value: float | None,
    state: float | None,
    old_weight: float,
    span: int,
) -> tuple[float | None, float | None, float]:
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


def _midrank(current: float | None, values: Sequence[float | None]) -> float | None:
    if current is None:
        return None
    sample = [value for value in values[-RANK_WINDOW:] if value is not None]
    if len(sample) < RANK_MIN_PERIODS:
        return None
    lower = sum(value < current for value in sample)
    equal = sum(value == current for value in sample)
    return (lower + 0.5 * equal) / len(sample) * 100.0


def _robust_impulse(impulses: Sequence[float]) -> tuple[float | None, bool, float | None]:
    sample = list(impulses[-NIB_SCALE_WINDOW:])
    if len(sample) < NIB_SCALE_MIN_PERIODS:
        return None, False, None
    center = float(median(sample))
    scale = 1.4826 * float(median(abs(value - center) for value in sample))
    if not math.isfinite(scale) or scale <= 1e-15:
        return None, False, None
    impulse_z = sample[-1] / scale
    magnitude = min(
        1.0,
        max(
            0.0,
            (-impulse_z + NEGATIVE_IMPULSE_Z_THRESHOLD) / NIB_MAGNITUDE_NORMALIZER,
        ),
    )
    return impulse_z, impulse_z < NEGATIVE_IMPULSE_Z_THRESHOLD, magnitude


def _ordinary_stock(row: Mapping[str, Any]) -> bool:
    exchange = str(row.get("exchange") or "").upper()
    market = str(row.get("market") or "")
    code = str(row.get("ts_code") or "").upper()
    symbol = str(row.get("symbol") or code.split(".")[0])
    name = str(row.get("name") or "").upper()
    if exchange not in {"SSE", "SZSE"} or market not in _ORDINARY_MARKETS:
        return False
    if code.endswith(".BJ") or "CDR" in name:
        return False
    prefixes = _SSE_PREFIXES if exchange == "SSE" else _SZSE_PREFIXES
    return symbol.startswith(prefixes)


def _active_on(row: Mapping[str, Any], day: date) -> bool:
    listed = _parse_day(row.get("list_date"))
    delisted = _parse_day(row.get("delist_date"))
    return bool(
        _ordinary_stock(row)
        and listed is not None
        and listed <= day
        and (delisted is None or day <= delisted)
    )


class _TushareRawClient:
    def __init__(self, token: str) -> None:
        self._client = TushareRealtimeClient(token)

    async def start(self) -> None:
        await self._client.start()

    async def stop(self) -> None:
        await self._client.stop()

    async def query(
        self,
        api_name: str,
        params: Mapping[str, Any],
        fields: Sequence[str],
        *,
        allow_empty: bool = False,
    ) -> list[dict[str, Any]]:
        payload = await self._client._api_call(  # noqa: SLF001 - same owned adapter layer
            api_name,
            dict(params),
            fields=",".join(fields),
        )
        data = payload.get("data") or {}
        names = data.get("fields") or []
        items = data.get("items") or []
        rows = [dict(zip(names, item, strict=False)) for item in items if names]
        if not rows and not allow_empty:
            raise MewsSnapshotSourceError(f"Tushare {api_name} returned no rows")
        return rows


class LocalMewsSnapshotCalculator:
    """Extend and persist the canonical MEWS v2 state from raw Tushare rows."""

    def __init__(
        self,
        token: str,
        repository: _StateRepository,
        *,
        bootstrap_path: Path,
        client_factory: Callable[[], _RawClient] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not token.strip():
            raise ValueError("Tushare token is required for local MEWS calculation")
        self._token = token.strip()
        self._repository = repository
        self._bootstrap_path = bootstrap_path
        self._client_factory = client_factory or (lambda: _TushareRawClient(self._token))
        self._clock = clock or (lambda: datetime.now(SHANGHAI))

    @staticmethod
    def default_bootstrap_path(project_root: Path) -> Path:
        bundled = project_root / "bundled_data" / "v20_mews_bootstrap.json.gz"
        return bundled if bundled.exists() else project_root / "data" / "v20_mews_bootstrap.json.gz"

    def _load_bootstrap(self) -> dict[str, Any]:
        try:
            with gzip.open(self._bootstrap_path, "rt", encoding="utf-8") as handle:
                state = json.load(handle)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise MewsSnapshotSourceError(
                f"MEWS bootstrap cannot be loaded: {type(exc).__name__}"
            ) from None
        self._validate_state(state)
        return state

    @staticmethod
    def _validate_state(state: Mapping[str, Any]) -> None:
        if state.get("schema") != MEWS_STATE_SCHEMA:
            raise MewsSnapshotSourceError("MEWS calculation state schema is invalid")
        if state.get("model_version") != MEWS_MODEL_VERSION:
            raise MewsSnapshotSourceError("MEWS calculation state model is invalid")
        if _parse_day(state.get("state_date")) is None:
            raise MewsSnapshotSourceError("MEWS calculation state date is invalid")
        if not isinstance(state.get("market_history"), list) or not isinstance(
            state.get("security_states"), Mapping
        ):
            raise MewsSnapshotSourceError("MEWS calculation state payload is invalid")
        if str(state.get("risk_state")) not in _LEVEL:
            raise MewsSnapshotSourceError("MEWS calculation risk state is invalid")

    async def _load_state(self) -> dict[str, Any]:
        stored = await self._repository.load_mews_calculation_state()
        if stored is None:
            return self._load_bootstrap()
        state = json.loads(_canonical_json(stored))
        self._validate_state(state)
        return state

    async def fetch_snapshot(
        self,
        *,
        source_trade_date: date,
        availability_date: date,
    ) -> Mapping[str, Any]:
        state = await self._load_state()
        state_date = _parse_day(state["state_date"])
        assert state_date is not None
        if state_date > source_trade_date:
            raise MewsSnapshotSourceError("MEWS state is ahead of the requested source date")

        if state_date < source_trade_date:
            client = self._client_factory()
            await client.start()
            try:
                pending = await self._pending_dates(
                    client,
                    state_date,
                    source_trade_date,
                )
                stocks = await self._stock_basic(client)
                for day in pending:
                    await self._advance_day(client, state, stocks, day)
                    await self._repository.save_mews_calculation_state(state)
            finally:
                await client.stop()

        final_date = _parse_day(state["state_date"])
        if final_date != source_trade_date:
            raise MewsSnapshotSourceError(
                "MEWS source date was not calculated: "
                f"expected {source_trade_date}, got {final_date}"
            )
        return self._snapshot(state, source_trade_date, availability_date)

    async def _pending_dates(
        self,
        client: _RawClient,
        state_date: date,
        target: date,
    ) -> list[date]:
        params = {
            "start_date": state_date.strftime("%Y%m%d"),
            "end_date": target.strftime("%Y%m%d"),
        }
        fields = ("exchange", "cal_date", "is_open", "pretrade_date")
        calendars: list[set[date]] = []
        for exchange in ("SSE", "SZSE"):
            rows = await client.query(
                "trade_cal",
                {**params, "exchange": exchange},
                fields,
            )
            calendars.append(
                {
                    day
                    for row in rows
                    if str(row.get("is_open")) == "1"
                    and (day := _parse_day(row.get("cal_date"))) is not None
                }
            )
        if calendars[0] != calendars[1]:
            raise MewsSnapshotSourceError("SSE/SZSE MEWS trade calendars disagree")
        if target not in calendars[0]:
            raise MewsSnapshotSourceError("requested MEWS source date is not an open day")
        return sorted(day for day in calendars[0] if state_date < day <= target)

    async def _stock_basic(self, client: _RawClient) -> list[dict[str, Any]]:
        fields = (
            "ts_code",
            "symbol",
            "name",
            "market",
            "exchange",
            "list_status",
            "list_date",
            "delist_date",
        )
        stocks: dict[str, dict[str, Any]] = {}
        for exchange in ("SSE", "SZSE"):
            for status in ("L", "D", "P", "G"):
                rows = await client.query(
                    "stock_basic",
                    {"exchange": exchange, "list_status": status},
                    fields,
                    allow_empty=status in {"P", "G"},
                )
                for row in rows:
                    code = str(row.get("ts_code") or "").upper()
                    if code:
                        stocks[code] = dict(row)
        ordinary = [row for row in stocks.values() if _ordinary_stock(row)]
        if not ordinary:
            raise MewsSnapshotSourceError("Tushare stock_basic has no ordinary A shares")
        return ordinary

    async def _advance_day(
        self,
        client: _RawClient,
        state: dict[str, Any],
        stocks: Sequence[Mapping[str, Any]],
        day: date,
    ) -> None:
        margin_rows = await client.query(
            "margin",
            {"trade_date": day.strftime("%Y%m%d")},
            ("trade_date", "exchange_id", "rzye", "rzmre", "rzche"),
        )
        exchanges: dict[str, tuple[float, float, float]] = {}
        for row in margin_rows:
            exchange = str(row.get("exchange_id") or "").upper()
            values = tuple(_finite(row.get(field)) for field in ("rzye", "rzmre", "rzche"))
            if exchange in {"SSE", "SZSE"} and all(value is not None for value in values):
                exchanges[exchange] = values  # type: ignore[assignment]
        if set(exchanges) != {"SSE", "SZSE"}:
            raise MewsSnapshotSourceError("Tushare margin is missing SSE or SZSE")

        ordinary_codes = {str(stock["ts_code"]).upper() for stock in stocks}
        active_codes = {str(stock["ts_code"]).upper() for stock in stocks if _active_on(stock, day)}
        detail_rows = await client.query(
            "margin_detail",
            {"trade_date": day.strftime("%Y%m%d")},
            ("trade_date", "ts_code", "rzye", "rzmre", "rzche"),
        )
        details: dict[str, dict[str, float]] = {}
        for row in detail_rows:
            code = str(row.get("ts_code") or "").upper()
            balance, buy, repay = (_finite(row.get(key)) for key in ("rzye", "rzmre", "rzche"))
            if (
                code in ordinary_codes
                and balance is not None
                and buy is not None
                and repay is not None
            ):
                details[code] = {
                    "financing_balance": balance,
                    "financing_buy_amount": buy,
                    "financing_repayment_amount": repay,
                }
        if not details:
            raise MewsSnapshotSourceError("Tushare margin_detail has no ordinary A-share rows")

        daily_basic = await client.query(
            "daily_basic",
            {"trade_date": day.strftime("%Y%m%d")},
            ("trade_date", "ts_code", "close", "free_share", "circ_mv", "total_mv"),
        )
        ffmv = 0.0
        ffmv_valid = 0
        for row in daily_basic:
            if str(row.get("ts_code") or "").upper() not in active_codes:
                continue
            close = _finite(row.get("close"))
            free_share = _finite(row.get("free_share"))
            if close is not None and close > 0 and free_share is not None and free_share > 0:
                ffmv += close * free_share * 10_000.0
                ffmv_valid += 1
        expected = len(active_codes)
        ffmv_coverage = ffmv_valid / expected if expected else None
        if ffmv <= 0 or ffmv_coverage is None:
            raise MewsSnapshotSourceError("Tushare daily_basic has no usable free-float value")

        security_states = state["security_states"]
        for code in ordinary_codes:
            security_states.setdefault(code, self._empty_security_state())
        bucket = {
            "valid": 0.0,
            "negative": 0.0,
            "magnitude": 0.0,
            "dlb_valid": 0.0,
            "deleveraging": 0.0,
        }
        for code, security_state in security_states.items():
            feature = self._advance_security(security_state, details.get(code))
            if feature is not None:
                self._add_feature(bucket, feature)

        stock_balance = sum(row["financing_balance"] for row in details.values())
        stock_buy = sum(row["financing_buy_amount"] for row in details.values())
        stock_repay = sum(row["financing_repayment_amount"] for row in details.values())
        market_balance = sum(exchanges[key][0] for key in ("SSE", "SZSE"))
        market_buy = sum(exchanges[key][1] for key in ("SSE", "SZSE"))
        market_repay = sum(exchanges[key][2] for key in ("SSE", "SZSE"))
        ordinary_coverage = stock_balance / market_balance if market_balance > 0 else None
        if bucket["valid"] <= 0 or bucket["dlb_valid"] <= 0:
            raise MewsSnapshotSourceError("MEWS security breadth has no valid balance")
        metric = {
            "trade_date": day.isoformat(),
            "market_total_margin_balance": market_balance,
            "market_total_financing_buy_amount": market_buy,
            "market_total_financing_repayment_amount": market_repay,
            "ordinary_a_share_margin_balance": stock_balance,
            "ordinary_a_share_financing_buy_amount": stock_buy,
            "ordinary_a_share_financing_repayment_amount": stock_repay,
            "ordinary_a_share_margin_coverage": ordinary_coverage,
            "ffmv_stock": ffmv,
            "nib_breadth_v2": 100.0 * bucket["negative"] / bucket["valid"],
            "nib_magnitude_v2": 100.0 * bucket["magnitude"] / bucket["valid"],
            "deleveraging_breadth": (100.0 * bucket["deleveraging"] / bucket["dlb_valid"]),
            "ffmv_coverage": ffmv_coverage,
        }

        history = state["market_history"]
        prior_coverage = [
            _finite(row.get("ordinary_a_share_margin_coverage"))
            for row in history[-DETAIL_COVERAGE_WINDOW:]
        ]
        valid_coverage = [value for value in prior_coverage if value is not None]
        coverage_base = (
            float(median(valid_coverage))
            if len(valid_coverage) >= DETAIL_COVERAGE_MIN_HISTORY
            else None
        )
        coverage_deviation = (
            ordinary_coverage - coverage_base
            if ordinary_coverage is not None and coverage_base is not None
            else None
        )
        metric["coverage_deviation_60d"] = coverage_deviation
        metric["data_status"] = (
            "OK"
            if ffmv_coverage >= FFMV_MIN_COVERAGE
            and (coverage_deviation is None or coverage_deviation >= -DETAIL_COVERAGE_DROP)
            else "PARTIAL"
        )
        history.append(metric)
        del history[:-550]
        self._calculate_latest(state)
        state["state_date"] = day.isoformat()
        state["calculated_at"] = self._clock().astimezone(SHANGHAI).isoformat()

    @staticmethod
    def _empty_security_state() -> dict[str, Any]:
        return {
            "current_balance": None,
            "ema_fast_state": None,
            "ema_fast_old_weight": 1.0,
            "ema_slow_state": None,
            "ema_slow_old_weight": 1.0,
            "valid_history": [],
            "net_flow_history": [],
            "impulse_history": [],
        }

    @staticmethod
    def _advance_security(
        state: dict[str, Any],
        current: Mapping[str, Any] | None,
    ) -> dict[str, Any] | None:
        balance = _finite(current.get("financing_balance")) if current else None
        buy = _finite(current.get("financing_buy_amount")) if current else None
        repay = _finite(current.get("financing_repayment_amount")) if current else None
        valid = None not in (balance, buy, repay) and current is not None
        previous = _finite(state.get("current_balance"))
        net_flow = buy - repay if valid and buy is not None and repay is not None else None
        flow_rate = (
            net_flow / previous if net_flow is not None and previous and previous > 0 else None
        )
        valid_history = [*state.get("valid_history", []), valid][-SECURITY_VALID_WINDOW:]
        net_history = [*state.get("net_flow_history", []), net_flow][-DELEVERAGING_WINDOW:]
        fast_output, fast_state, fast_weight = _advance_ema(
            flow_rate,
            _finite(state.get("ema_fast_state")),
            _finite(state.get("ema_fast_old_weight")) or 1.0,
            EMA_FAST,
        )
        slow_output, slow_state, slow_weight = _advance_ema(
            flow_rate,
            _finite(state.get("ema_slow_state")),
            _finite(state.get("ema_slow_old_weight")) or 1.0,
            EMA_SLOW,
        )
        eligible = bool(
            previous
            and previous > 0
            and sum(valid_history) >= SECURITY_MIN_VALID
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
        impulse_z: float | None = None
        is_negative = False
        magnitude: float | None = None
        if impulse is not None:
            impulse_history = [*impulse_history, impulse][-NIB_SCALE_WINDOW:]
            impulse_z, is_negative, magnitude = _robust_impulse(impulse_history)
        net_flow_5d = (
            sum(float(value) for value in net_history)
            if len(net_history) == DELEVERAGING_WINDOW
            and all(value is not None for value in net_history)
            else None
        )
        state.update(
            {
                "current_balance": balance,
                "ema_fast_state": fast_state,
                "ema_fast_old_weight": fast_weight,
                "ema_slow_state": slow_state,
                "ema_slow_old_weight": slow_weight,
                "valid_history": valid_history,
                "net_flow_history": net_history,
                "impulse_history": impulse_history,
            }
        )
        if current is None:
            return None
        return {
            "previous_balance": previous,
            "impulse_z": impulse_z,
            "is_negative": is_negative,
            "magnitude": magnitude,
            "net_flow_5d": net_flow_5d,
        }

    @staticmethod
    def _add_feature(bucket: dict[str, float], feature: Mapping[str, Any]) -> None:
        balance = _finite(feature.get("previous_balance"))
        if balance is None or balance <= 0:
            return
        impulse_z = _finite(feature.get("impulse_z"))
        if impulse_z is not None:
            bucket["valid"] += balance
            if feature.get("is_negative") is True:
                bucket["negative"] += balance
            magnitude = _finite(feature.get("magnitude"))
            if magnitude is not None:
                bucket["magnitude"] += balance * magnitude
        net_flow_5d = _finite(feature.get("net_flow_5d"))
        if net_flow_5d is not None:
            bucket["dlb_valid"] += balance
            if net_flow_5d < 0:
                bucket["deleveraging"] += balance

    @staticmethod
    def _calculate_latest(state: dict[str, Any]) -> None:
        history = state["market_history"]
        latest = history[-1]
        if latest.get("data_status") != "OK":
            raise MewsSnapshotSourceError("MEWS raw material failed quality checks")
        balances = [_finite(row.get("ordinary_a_share_margin_balance")) for row in history]
        buys = [_finite(row.get("ordinary_a_share_financing_buy_amount")) for row in history]
        repays = [
            _finite(row.get("ordinary_a_share_financing_repayment_amount")) for row in history
        ]
        flow_rates: list[float | None] = []
        for index, (buy, repay) in enumerate(zip(buys, repays, strict=True)):
            previous = balances[index - 1] if index else None
            flow_rates.append(
                (buy - repay) / previous
                if buy is not None and repay is not None and previous and previous > 0
                else None
            )
        flow_fast = _ema(flow_rates, EMA_FAST)
        flow_slow = _ema(flow_rates, EMA_SLOW)
        pulses = [
            fast - slow if fast is not None and slow is not None else None
            for fast, slow in zip(flow_fast, flow_slow, strict=True)
        ]
        mpi = _midrank(pulses[-1], pulses)
        ffmv = [_finite(row.get("ffmv_stock")) for row in history]
        valid_base = [value for value in ffmv[-LOAD_BASE_WINDOW - 1 : -1] if value is not None]
        ffmv_base = float(median(valid_base)) if len(valid_base) == LOAD_BASE_WINDOW else None
        leverage = [
            balance / float(median([value for value in ffmv[max(0, i - 20) : i] if value]))
            if balance is not None
            and len([value for value in ffmv[max(0, i - 20) : i] if value]) == 20
            else None
            for i, balance in enumerate(balances)
        ]
        current_load = balances[-1] / ffmv_base if balances[-1] is not None and ffmv_base else None
        if leverage:
            leverage[-1] = current_load
        mls = _midrank(current_load, leverage)
        outflows = [-value if value is not None else None for value in flow_fast]
        outflow_score = _midrank(outflows[-1], outflows)
        nib_breadth = _finite(latest.get("nib_breadth_v2"))
        nib_magnitude = _finite(latest.get("nib_magnitude_v2"))
        dlb = _finite(latest.get("deleveraging_breadth"))
        if (
            mpi is None
            or mls is None
            or outflow_score is None
            or nib_breadth is None
            or nib_magnitude is None
            or dlb is None
        ):
            raise MewsSnapshotSourceError("MEWS trailing calculation history is incomplete")
        nib = math.sqrt(max(0.0, nib_breadth * nib_magnitude))
        exhaustion = max(0.0, (100.0 - mpi) * mls * nib) ** (1.0 / 3.0)
        persistent = max(0.0, mls * dlb * outflow_score) ** (1.0 / 3.0)
        score = max(exhaustion, persistent)
        latest.update(
            {
                "mpi_stock_v2": mpi,
                "mls_stock_v2": mls,
                "net_outflow_level_score": outflow_score,
                "nib_v2": nib,
                "exhaustion_path": exhaustion,
                "persistent_deleveraging_path": persistent,
                "mews_v2_score": score,
            }
        )
        LocalMewsSnapshotCalculator._advance_risk_state(state)

    @staticmethod
    def _advance_risk_state(state: dict[str, Any]) -> None:
        rows = state["market_history"]
        current = rows[-1]
        score = float(current["mews_v2_score"])
        persistent = float(current["persistent_deleveraging_path"])
        dlb = float(current["deleveraging_breadth"])
        outflow = float(current["net_outflow_level_score"])
        candidate = "NORMAL"
        if dlb >= 75.0 and outflow >= 99.0:
            candidate = "DANGER"
        elif len(rows) >= 2:
            previous = rows[-2]
            previous_score = _finite(previous.get("mews_v2_score"))
            previous_persistent = _finite(previous.get("persistent_deleveraging_path"))
            if (
                score >= WARNING_THRESHOLD
                and persistent >= PERSISTENT_DANGER_THRESHOLD
                and previous.get("data_status") == "OK"
                and previous_score is not None
                and previous_score >= WARNING_THRESHOLD
                and previous_persistent is not None
                and previous_persistent >= PERSISTENT_DANGER_THRESHOLD
            ):
                candidate = "DANGER"
        if candidate == "NORMAL":
            recent = [_finite(row.get("mews_v2_score")) for row in rows[-3:]]
            if sum(value is not None and value >= WARNING_THRESHOLD for value in recent) >= 2:
                candidate = "WARNING"
            elif sum(value is not None and value >= WATCH_THRESHOLD for value in recent) >= 2:
                candidate = "WATCH"

        clear_streak = int(state.get("clear_streak") or 0)
        clear_streak = clear_streak + 1 if score < CLEAR_THRESHOLD else 0
        previous_state = str(state.get("risk_state") or "NORMAL")
        if clear_streak >= CLEAR_DAYS:
            risk_state = "NORMAL"
        elif _LEVEL[candidate] >= _LEVEL[previous_state]:
            risk_state = candidate
        elif candidate == "NORMAL":
            risk_state = _ONE_DOWN[previous_state]
        else:
            one_down = _ONE_DOWN[previous_state]
            risk_state = candidate if _LEVEL[candidate] >= _LEVEL[one_down] else one_down
        state["risk_state"] = risk_state
        state["clear_streak"] = clear_streak
        current["risk_state_v2"] = risk_state

    def _snapshot(
        self,
        state: Mapping[str, Any],
        source_trade_date: date,
        availability_date: date,
    ) -> Mapping[str, Any]:
        metric = state["market_history"][-1]
        raw_generated_at = state.get("calculated_at")
        try:
            generated_at = (
                datetime.fromisoformat(str(raw_generated_at)).astimezone(SHANGHAI)
                if raw_generated_at is not None
                else self._clock().astimezone(SHANGHAI)
            )
        except ValueError:
            raise MewsSnapshotSourceError("MEWS calculated_at is invalid") from None
        if (
            generated_at.date() != availability_date
            or generated_at.timetz().replace(tzinfo=None) < MEWS_PUBLISH_TIME
        ):
            raise MewsSnapshotSourceError("MEWS calculation ran outside its publication boundary")
        evidence = {
            "profile": "LOCAL_TUSHARE_MEWS_V2_0910_V1",
            "source_trade_date": source_trade_date.isoformat(),
            "signal_available_date": availability_date.isoformat(),
            "risk_state": state["risk_state"],
            "data_status": metric["data_status"],
            "mews": metric["mews_v2_score"],
            "exhaustion_path": metric["exhaustion_path"],
            "persistent_deleveraging_path": metric["persistent_deleveraging_path"],
            "nib_breadth_v2": metric["nib_breadth_v2"],
            "nib_magnitude_v2": metric["nib_magnitude_v2"],
            "deleveraging_breadth": metric["deleveraging_breadth"],
            "ffmv_coverage": metric["ffmv_coverage"],
        }
        data_version = hashlib.sha256(_canonical_json(evidence).encode()).hexdigest()
        return {
            "snapshot_id": f"mews-v2-{source_trade_date.isoformat()}-{data_version[:16]}",
            "source_trade_date": source_trade_date.isoformat(),
            "generated_at": generated_at.isoformat(),
            "fast_state": "DANGER" if state["risk_state"] == "DANGER" else "NORMAL",
            "model_version": MEWS_MODEL_VERSION,
            "data_version": data_version,
            "evidence": evidence,
        }


__all__ = [
    "LocalMewsSnapshotCalculator",
    "MEWS_MODEL_VERSION",
    "MEWS_PUBLISH_TIME",
    "MewsSnapshotSourceError",
]
