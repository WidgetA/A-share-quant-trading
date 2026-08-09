"""GreptimeDB persistence for the production MEWS risk curve.

The tables in this module are deliberately separate from ``backtest_daily``:
daily prices and margin facts have different coverage rules, publication times
and repair semantics.  All writes are idempotent on (primary key, trade date).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

from src.data.clients.greptime_storage import date_to_epoch_ms, ts_to_date

_CREATE_SECURITY_SQL = """
CREATE TABLE IF NOT EXISTS margin_risk_security_daily (
    stock_code STRING,
    ts TIMESTAMP TIME INDEX,
    financing_balance FLOAT64,
    financing_buy_amount FLOAT64,
    financing_repayment_amount FLOAT64,
    PRIMARY KEY (stock_code)
)
"""

_CREATE_MARKET_SQL = """
CREATE TABLE IF NOT EXISTS margin_risk_market_daily (
    series STRING,
    ts TIMESTAMP TIME INDEX,
    market_financing_balance FLOAT64,
    market_financing_buy_amount FLOAT64,
    market_financing_repayment_amount FLOAT64,
    stock_financing_balance FLOAT64,
    stock_financing_buy_amount FLOAT64,
    stock_financing_repayment_amount FLOAT64,
    free_float_market_cap FLOAT64,
    margin_security_count INT32,
    ffmv_valid_count INT32,
    ffmv_expected_count INT32,
    ordinary_margin_coverage FLOAT64,
    ffmv_coverage FLOAT64,
    sse_complete BOOLEAN,
    szse_complete BOOLEAN,
    ingestion_status STRING,
    error_message STRING,
    updated_at INT64,
    PRIMARY KEY (series)
)
"""

_CREATE_METRIC_SQL = """
CREATE TABLE IF NOT EXISTS margin_risk_metric_daily (
    index_version STRING,
    ts TIMESTAMP TIME INDEX,
    signal_available_ts TIMESTAMP,
    market_financing_balance FLOAT64,
    market_financing_buy_amount FLOAT64,
    market_financing_repayment_amount FLOAT64,
    stock_financing_balance FLOAT64,
    stock_financing_buy_amount FLOAT64,
    stock_financing_repayment_amount FLOAT64,
    ordinary_margin_coverage FLOAT64,
    coverage_deviation_60d FLOAT64,
    stock_flow_rate FLOAT64,
    pulse_raw FLOAT64,
    mpi FLOAT64,
    free_float_market_cap FLOAT64,
    ffmv_base FLOAT64,
    leverage_load_raw FLOAT64,
    mls FLOAT64,
    buy_shortfall_score FLOAT64,
    repay_level_score FLOAT64,
    net_flow_level_raw FLOAT64,
    net_outflow_level_score FLOAT64,
    nib_breadth FLOAT64,
    nib_magnitude FLOAT64,
    nib FLOAT64,
    deleveraging_breadth FLOAT64,
    exhaustion_path FLOAT64,
    persistent_deleveraging_path FLOAT64,
    mews FLOAT64,
    mews_rolling_percentile FLOAT64,
    detail_coverage FLOAT64,
    breadth_coverage FLOAT64,
    ffmv_coverage FLOAT64,
    risk_state STRING,
    data_status STRING,
    watch_threshold FLOAT64,
    warning_threshold FLOAT64,
    clear_threshold FLOAT64,
    persistent_danger_threshold FLOAT64,
    signal_reason STRING,
    updated_at INT64,
    PRIMARY KEY (index_version)
)
"""

_SERIES = "MEWS"
_VERSION = "mews_v2"
_WRITE_BATCH = 100


def _q(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _num(value: Any) -> str:
    return "NULL" if value is None else repr(float(value))


def _integer(value: Any) -> str:
    return "NULL" if value is None else str(int(value))


def _boolean(value: Any) -> str:
    if value is None:
        return "NULL"
    return "TRUE" if bool(value) else "FALSE"


def _text(value: Any) -> str:
    return "NULL" if value is None else _q(str(value))


def _timestamp(value: date | None) -> str:
    return "NULL" if value is None else str(date_to_epoch_ms(value))


def _chunks[T](values: Sequence[T], size: int = _WRITE_BATCH) -> list[Sequence[T]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


class GreptimeMarginRiskStore:
    """Schema and CRUD using the application's shared Greptime connection pool."""

    def __init__(self, storage: Any) -> None:
        self._db = storage.db

    async def ensure_schema(self) -> None:
        await self._db.execute(_CREATE_SECURITY_SQL)
        await self._db.execute(_CREATE_MARKET_SQL)
        await self._db.execute(_CREATE_METRIC_SQL)

    async def replace_security_day(
        self,
        trade_date: date,
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        ts_ms = date_to_epoch_ms(trade_date)
        await self._db.execute(f"DELETE FROM margin_risk_security_daily WHERE ts = {ts_ms}")
        written = 0
        columns = "stock_code,ts,financing_balance,financing_buy_amount,financing_repayment_amount"
        for batch in _chunks(rows):
            values = ",".join(
                "("
                + ",".join(
                    (
                        _q(str(row["stock_code"])),
                        str(ts_ms),
                        _num(row.get("financing_balance")),
                        _num(row.get("financing_buy_amount")),
                        _num(row.get("financing_repayment_amount")),
                    )
                )
                + ")"
                for row in batch
            )
            if values:
                await self._db.execute(
                    f"INSERT INTO margin_risk_security_daily ({columns}) VALUES {values}"
                )
                written += len(batch)
        return written

    async def upsert_market_day(self, trade_date: date, row: Mapping[str, Any]) -> None:
        ts_ms = date_to_epoch_ms(trade_date)
        updated_at = int(datetime.now(timezone.utc).timestamp() * 1000)
        columns = (
            "series,ts,market_financing_balance,market_financing_buy_amount,"
            "market_financing_repayment_amount,stock_financing_balance,"
            "stock_financing_buy_amount,stock_financing_repayment_amount,"
            "free_float_market_cap,margin_security_count,ffmv_valid_count,"
            "ffmv_expected_count,ordinary_margin_coverage,ffmv_coverage,"
            "sse_complete,szse_complete,ingestion_status,error_message,updated_at"
        )
        values = (
            _q(_SERIES),
            str(ts_ms),
            _num(row.get("market_financing_balance")),
            _num(row.get("market_financing_buy_amount")),
            _num(row.get("market_financing_repayment_amount")),
            _num(row.get("stock_financing_balance")),
            _num(row.get("stock_financing_buy_amount")),
            _num(row.get("stock_financing_repayment_amount")),
            _num(row.get("free_float_market_cap")),
            _integer(row.get("margin_security_count")),
            _integer(row.get("ffmv_valid_count")),
            _integer(row.get("ffmv_expected_count")),
            _num(row.get("ordinary_margin_coverage")),
            _num(row.get("ffmv_coverage")),
            _boolean(row.get("sse_complete")),
            _boolean(row.get("szse_complete")),
            _text(row.get("ingestion_status")),
            _text(row.get("error_message")),
            str(updated_at),
        )
        await self._db.execute(
            f"INSERT INTO margin_risk_market_daily ({columns}) VALUES ({','.join(values)})"
        )

    async def get_complete_dates(self, start: date, end: date) -> set[date]:
        rows = await self._db.fetch(
            "SELECT ts FROM margin_risk_market_daily "
            f"WHERE ts >= {date_to_epoch_ms(start)} AND ts <= {date_to_epoch_ms(end)} "
            "AND ingestion_status = 'OK'"
        )
        return {ts_to_date(row["ts"]) for row in rows}

    async def get_raw_date_range(self) -> tuple[date | None, date | None]:
        row = await self._db.fetchrow(
            "SELECT MIN(ts) AS min_ts, MAX(ts) AS max_ts "
            "FROM margin_risk_market_daily WHERE ingestion_status = 'OK'"
        )
        if not row or row["min_ts"] is None:
            return None, None
        return ts_to_date(row["min_ts"]), ts_to_date(row["max_ts"])

    async def get_market_rows(self, start: date, end: date) -> list[dict[str, Any]]:
        rows = await self._db.fetch(
            "SELECT * FROM margin_risk_market_daily "
            f"WHERE ts >= {date_to_epoch_ms(start)} AND ts <= {date_to_epoch_ms(end)} "
            "ORDER BY ts"
        )
        output: list[dict[str, Any]] = []
        for raw in rows:
            item = dict(raw)
            item["trade_date"] = ts_to_date(item.pop("ts"))
            output.append(item)
        return output

    async def get_security_codes(self, start: date, end: date) -> list[str]:
        rows = await self._db.fetch(
            "SELECT DISTINCT stock_code FROM margin_risk_security_daily "
            f"WHERE ts >= {date_to_epoch_ms(start)} AND ts <= {date_to_epoch_ms(end)} "
            "ORDER BY stock_code"
        )
        return [str(row["stock_code"]) for row in rows]

    async def get_security_rows(
        self,
        start: date,
        end: date,
        codes: Sequence[str],
    ) -> list[dict[str, Any]]:
        if not codes:
            return []
        code_sql = ",".join(_q(code) for code in codes)
        rows = await self._db.fetch(
            "SELECT stock_code,ts,financing_balance,financing_buy_amount,"
            "financing_repayment_amount FROM margin_risk_security_daily "
            f"WHERE ts >= {date_to_epoch_ms(start)} AND ts <= {date_to_epoch_ms(end)} "
            f"AND stock_code IN ({code_sql}) ORDER BY stock_code,ts"
        )
        output: list[dict[str, Any]] = []
        for raw in rows:
            item = dict(raw)
            item["trade_date"] = ts_to_date(item.pop("ts"))
            item["ts_code"] = item["stock_code"]
            output.append(item)
        return output

    async def replace_metrics(
        self,
        start: date,
        end: date,
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        await self._db.execute(
            "DELETE FROM margin_risk_metric_daily "
            f"WHERE index_version = {_q(_VERSION)} "
            f"AND ts >= {date_to_epoch_ms(start)} AND ts <= {date_to_epoch_ms(end)}"
        )
        columns = (
            "index_version,ts,signal_available_ts,market_financing_balance,"
            "market_financing_buy_amount,market_financing_repayment_amount,"
            "stock_financing_balance,stock_financing_buy_amount,"
            "stock_financing_repayment_amount,ordinary_margin_coverage,"
            "coverage_deviation_60d,stock_flow_rate,pulse_raw,mpi,"
            "free_float_market_cap,ffmv_base,leverage_load_raw,mls,"
            "buy_shortfall_score,repay_level_score,net_flow_level_raw,"
            "net_outflow_level_score,nib_breadth,nib_magnitude,nib,"
            "deleveraging_breadth,exhaustion_path,persistent_deleveraging_path,"
            "mews,mews_rolling_percentile,detail_coverage,breadth_coverage,"
            "ffmv_coverage,risk_state,data_status,watch_threshold,warning_threshold,"
            "clear_threshold,persistent_danger_threshold,signal_reason,updated_at"
        )
        updated_at = int(datetime.now(timezone.utc).timestamp() * 1000)

        def values(row: Mapping[str, Any]) -> str:
            ordered = (
                _q(_VERSION),
                str(date_to_epoch_ms(row["trade_date"])),
                _timestamp(row.get("signal_available_date")),
                _num(row.get("market_total_margin_balance")),
                _num(row.get("market_total_financing_buy_amount")),
                _num(row.get("market_total_financing_repayment_amount")),
                _num(row.get("ordinary_a_share_margin_balance")),
                _num(row.get("ordinary_a_share_financing_buy_amount")),
                _num(row.get("ordinary_a_share_financing_repayment_amount")),
                _num(row.get("ordinary_a_share_margin_coverage")),
                _num(row.get("coverage_deviation_60d")),
                _num(row.get("stock_flow_rate")),
                _num(row.get("pulse_raw_stock")),
                _num(row.get("mpi_stock_v2")),
                _num(row.get("ffmv_stock")),
                _num(row.get("ffmv_stock_base")),
                _num(row.get("leverage_load_stock_raw")),
                _num(row.get("mls_stock_v2")),
                _num(row.get("buy_shortfall_score")),
                _num(row.get("repay_level_score")),
                _num(row.get("net_flow_level_raw")),
                _num(row.get("net_outflow_level_score")),
                _num(row.get("nib_breadth_v2")),
                _num(row.get("nib_magnitude_v2")),
                _num(row.get("nib_v2")),
                _num(row.get("deleveraging_breadth")),
                _num(row.get("exhaustion_path")),
                _num(row.get("persistent_deleveraging_path")),
                _num(row.get("mews_v2_score")),
                _num(row.get("mews_v2_rolling_percentile")),
                _num(row.get("detail_coverage")),
                _num(row.get("breadth_coverage")),
                _num(row.get("ffmv_coverage")),
                _text(row.get("risk_state_v2")),
                _text(row.get("data_status")),
                _num(row.get("watch_threshold")),
                _num(row.get("warning_threshold")),
                _num(row.get("clear_threshold")),
                _num(row.get("persistent_danger_threshold")),
                _text(row.get("signal_reason_v2")),
                str(updated_at),
            )
            return "(" + ",".join(ordered) + ")"

        written = 0
        for batch in _chunks(rows):
            if not batch:
                continue
            await self._db.execute(
                f"INSERT INTO margin_risk_metric_daily ({columns}) VALUES "
                + ",".join(values(row) for row in batch)
            )
            written += len(batch)
        return written

    async def get_metric_before(self, day: date) -> dict[str, Any] | None:
        row = await self._db.fetchrow(
            "SELECT * FROM margin_risk_metric_daily "
            f"WHERE index_version = {_q(_VERSION)} AND ts < {date_to_epoch_ms(day)} "
            "ORDER BY ts DESC LIMIT 1"
        )
        if row is None:
            return None
        item = dict(row)
        item["trade_date"] = ts_to_date(item.pop("ts"))
        return item

    async def get_latest_metric(self) -> dict[str, Any] | None:
        row = await self._db.fetchrow(
            "SELECT * FROM margin_risk_metric_daily "
            f"WHERE index_version = {_q(_VERSION)} ORDER BY ts DESC LIMIT 1"
        )
        if row is None:
            return None
        item = dict(row)
        item["trade_date"] = ts_to_date(item.pop("ts"))
        return item

    async def list_metrics(self, days: int = 5000) -> list[dict[str, Any]]:
        limit = max(1, min(int(days), 5000))
        rows = await self._db.fetch(
            "SELECT * FROM margin_risk_metric_daily "
            f"WHERE index_version = {_q(_VERSION)} ORDER BY ts DESC LIMIT {limit}"
        )
        output: list[dict[str, Any]] = []
        for raw in reversed(rows):
            item = dict(raw)
            item["date"] = ts_to_date(item.pop("ts")).isoformat()
            signal_ts = item.pop("signal_available_ts", None)
            item["signal_available_date"] = (
                ts_to_date(signal_ts).isoformat() if signal_ts is not None else None
            )
            output.append(item)
        return output

    async def status(self) -> dict[str, Any]:
        raw_start, raw_end = await self.get_raw_date_range()
        latest = await self.get_latest_metric()
        failures = await self._db.fetchrow(
            "SELECT COUNT(*) AS count FROM margin_risk_market_daily WHERE ingestion_status != 'OK'"
        )
        latest_ingestion = await self._db.fetchrow(
            "SELECT ts,ingestion_status FROM margin_risk_market_daily ORDER BY ts DESC LIMIT 1"
        )
        latest_ingestion_item = dict(latest_ingestion) if latest_ingestion else {}
        return {
            "raw_start": raw_start.isoformat() if raw_start else None,
            "raw_end": raw_end.isoformat() if raw_end else None,
            "metric_end": (latest["trade_date"].isoformat() if latest is not None else None),
            "failed_days": int(failures["count"] or 0) if failures else 0,
            "latest_raw_date": (
                ts_to_date(latest_ingestion_item["ts"]).isoformat()
                if latest_ingestion_item.get("ts") is not None
                else None
            ),
            "latest_ingestion_status": (
                str(latest_ingestion_item["ingestion_status"])
                if latest_ingestion_item.get("ingestion_status") is not None
                else None
            ),
        }
