"""Dedicated Greptime HTTP storage for sourced cross-market prices and references."""

import json
import math
from collections.abc import Iterable, Mapping
from typing import Any, Literal, overload

import httpx

PRICE_TABLE = "cross_market_index_prices"
REFERENCE_TABLE = "cross_market_industry_indices"
PRICE_KEYS = ("provider", "market", "symbol", "interval", "data_kind", "ts")
REFERENCE_KEYS = ("provider", "market", "sw_code", "reference_at")
PRICE_COLUMNS = (
    "provider",
    "market",
    "symbol",
    "interval",
    "data_kind",
    "ts",
    "name",
    "currency",
    "exchange_timezone",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "adjusted_close",
    "volume",
    "is_final",
    "fetched_at",
)
REFERENCE_COLUMNS = (
    "provider",
    "market",
    "sw_code",
    "reference_at",
    "mapping_json",
    "fetched_at",
)
_PRICE_NUMBERS = {"open", "high", "low", "close", "adjusted_close", "volume"}
_PRICE_TEXT = {"name", "currency", "exchange_timezone", "trade_date"}

PRICE_DDL = f"""CREATE TABLE IF NOT EXISTS {PRICE_TABLE} (
  "provider" STRING NOT NULL,
  "market" STRING NOT NULL,
  "symbol" STRING NOT NULL,
  "interval" STRING NOT NULL,
  "data_kind" STRING NOT NULL,
  "ts" TIMESTAMP(3) NOT NULL,
  "name" STRING NULL,
  "currency" STRING NULL,
  "exchange_timezone" STRING NULL,
  "trade_date" STRING NULL,
  "open" DOUBLE NULL,
  "high" DOUBLE NULL,
  "low" DOUBLE NULL,
  "close" DOUBLE NULL,
  "adjusted_close" DOUBLE NULL,
  "volume" DOUBLE NULL,
  "is_final" BOOLEAN NULL,
  "fetched_at" BIGINT NOT NULL,
  TIME INDEX ("ts"),
  PRIMARY KEY ("provider", "market", "symbol", "interval", "data_kind")
) ENGINE=mito WITH ('merge_mode'='last_row')"""

REFERENCE_DDL = f"""CREATE TABLE IF NOT EXISTS {REFERENCE_TABLE} (
  "provider" STRING NOT NULL,
  "market" STRING NOT NULL,
  "sw_code" STRING NOT NULL,
  "reference_at" TIMESTAMP(3) NOT NULL,
  "mapping_json" STRING NOT NULL,
  "fetched_at" BIGINT NOT NULL,
  TIME INDEX ("reference_at"),
  PRIMARY KEY ("provider", "market", "sw_code")
) ENGINE=mito WITH ('merge_mode'='last_row')"""


class GreptimeError(RuntimeError):
    """An HTTP SQL response did not establish successful execution."""


class GreptimeWriteError(GreptimeError):
    """A write failed; preceding batches may already have persisted."""

    def __init__(self, message: str, *, confirmed_rows: int):
        super().__init__(message)
        self.confirmed_rows = confirmed_rows


class GreptimeReadbackError(GreptimeError):
    """Persisted keys or values differ from the expected source rows."""


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise ValueError(f"Unsupported SQL value type: {type(value).__name__}")


@overload
def _text(value: Any, field: str, *, required: Literal[True]) -> str: ...


@overload
def _text(value: Any, field: str, *, required: Literal[False] = False) -> str | None: ...


def _text(value: Any, field: str, *, required: bool = False) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or (required and not value):
        raise ValueError(f"{field} must be {'nonempty ' if required else ''}text")
    return value


def _millis(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be integer epoch milliseconds")
    if not -(2**63) <= value < 2**63:
        raise ValueError(f"{field} exceeds signed 64-bit milliseconds")
    return value


def _normalise_price(point: Mapping[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for field in PRICE_KEYS[:-1]:
        row[field] = _text(point.get(field), field, required=True)
    row["ts"] = _millis(point.get("ts"), "ts")
    row["fetched_at"] = _millis(point.get("fetched_at"), "fetched_at")
    for field in _PRICE_TEXT:
        row[field] = _text(point.get(field), field)
    for field in _PRICE_NUMBERS:
        value = point.get(field)
        if value is not None:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{field} must be a finite number or null")
            try:
                value = float(value)
            except OverflowError as exc:
                raise ValueError(f"{field} must be a finite number or null") from exc
            if not math.isfinite(value):
                raise ValueError(f"{field} must be a finite number or null")
        row[field] = value
    final = point.get("is_final")
    if final is not None and not isinstance(final, bool):
        raise ValueError("is_final must be a boolean or null")
    row["is_final"] = final
    return row


def _normalise_reference(reference: Mapping[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {
        field: _text(reference.get(field), field, required=True) for field in REFERENCE_KEYS[:-1]
    }
    row["reference_at"] = _millis(reference.get("reference_at"), "reference_at")
    row["fetched_at"] = _millis(reference.get("fetched_at"), "fetched_at")
    mapping = reference.get("mapping_json")
    if isinstance(mapping, Mapping):
        row["mapping_json"] = json.dumps(
            dict(mapping),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    elif isinstance(mapping, str):
        if not isinstance(json.loads(mapping, parse_constant=_reject_json_constant), dict):
            raise ValueError("mapping_json must contain a JSON object")
        row["mapping_json"] = mapping
    else:
        raise ValueError("mapping_json must be a JSON object or encoded object")
    return row


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"mapping_json contains a non-JSON number: {value}")


def _deduplicate(rows: Iterable[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    # The source's last supplied row wins under the same identity as Greptime.
    return list({tuple(row[field] for field in keys): row for row in rows}.values())


class CrossMarketStore:
    """Async HTTP SQL writer; retry and successful-window checkpoints belong to the caller.

    ``base_url`` is the Greptime host or existing SQL proxy host, without ``/v1/sql``.
    A supplied httpx client remains owned by its caller and can carry proxy/auth settings.
    ``upsert_*`` returns unique rows only after every batch is confirmed; verify methods
    independently read and compare every expected key/value before callers advance state.
    """

    def __init__(
        self,
        base_url: str,
        *,
        client: httpx.AsyncClient | None = None,
        database: str = "public",
        batch_size: int = 100,
        timeout: float = 30.0,
    ):
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        self._url = base_url.rstrip("/") + "/v1/sql"
        self._database = database
        self._batch_size = batch_size
        self._timeout = timeout
        self._owns_client = client is None
        self._client = client if client is not None else httpx.AsyncClient()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "CrossMarketStore":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.aclose()

    async def _execute(self, sql: str) -> dict[str, Any]:
        response = await self._client.post(
            self._url,
            params={"db": self._database},
            data={"sql": sql},
            headers={"X-Greptime-Timezone": "+00:00"},
            timeout=self._timeout,
        )
        try:
            body = response.json()
        except ValueError as exc:
            raise GreptimeError(f"Greptime returned non-JSON HTTP {response.status_code}") from exc
        if not isinstance(body, dict):
            raise GreptimeError("Greptime returned an invalid SQL result object")
        if not response.is_success or "error" in body or body.get("code", 0) != 0:
            detail = str(body.get("error", "request failed"))[:500]
            raise GreptimeError(f"Greptime HTTP {response.status_code}: {detail}")
        output = body.get("output")
        if not isinstance(output, list) or len(output) != 1 or not isinstance(output[0], dict):
            raise GreptimeError("Greptime returned no single SQL execution result")
        result = output[0]
        if "error" in result or result.get("code", 0) != 0:
            raise GreptimeError(f"Greptime SQL error: {str(result.get('error', 'failed'))[:500]}")
        return result

    async def ensure_schema(self) -> None:
        for ddl in (PRICE_DDL, REFERENCE_DDL):
            result = await self._execute(ddl)
            acknowledged = result.get("affectedrows")
            if (
                isinstance(acknowledged, bool)
                or not isinstance(acknowledged, int)
                or acknowledged < 0
            ):
                raise GreptimeError("Greptime did not acknowledge schema execution")

    async def _upsert(
        self, table: str, columns: tuple[str, ...], rows: list[dict[str, Any]]
    ) -> int:
        confirmed = 0
        column_sql = ", ".join(_identifier(field) for field in columns)
        for start in range(0, len(rows), self._batch_size):
            batch = rows[start : start + self._batch_size]
            values = ", ".join(
                "(" + ", ".join(_literal(row[field]) for field in columns) + ")" for row in batch
            )
            sql = f"INSERT INTO {table} ({column_sql}) VALUES {values}"
            try:
                result = await self._execute(sql)
                affected = result.get("affectedrows")
                if isinstance(affected, bool) or not isinstance(affected, int):
                    raise GreptimeError("Greptime returned no integer affected-row count")
                if affected != len(batch):
                    raise GreptimeError(
                        f"Greptime acknowledged {affected} rows; expected {len(batch)}"
                    )
            except (GreptimeError, httpx.RequestError) as exc:
                raise GreptimeWriteError(
                    f"{table} write failed after {confirmed} confirmed rows: {exc}",
                    confirmed_rows=confirmed,
                ) from exc
            confirmed += len(batch)
        return confirmed

    async def upsert_prices(self, points: Iterable[Mapping[str, Any]]) -> int:
        rows = _deduplicate((_normalise_price(point) for point in points), PRICE_KEYS)
        return await self._upsert(PRICE_TABLE, PRICE_COLUMNS, rows)

    async def upsert_industry_indices(self, references: Iterable[Mapping[str, Any]]) -> int:
        rows = _deduplicate(
            (_normalise_reference(reference) for reference in references), REFERENCE_KEYS
        )
        return await self._upsert(REFERENCE_TABLE, REFERENCE_COLUMNS, rows)

    @staticmethod
    def _records(result: dict[str, Any]) -> list[dict[str, Any]]:
        try:
            records = result["records"]
            names = [column["name"] for column in records["schema"]["column_schemas"]]
            values = records["rows"]
        except (KeyError, TypeError) as exc:
            raise GreptimeError("Greptime returned an invalid records result") from exc
        if not isinstance(values, list) or len(names) != len(set(names)):
            raise GreptimeError("Greptime returned invalid record columns or rows")
        if any(not isinstance(row, list) or len(row) != len(names) for row in values):
            raise GreptimeError("Greptime returned misaligned record values")
        return [dict(zip(names, row, strict=True)) for row in values]

    async def _read(
        self,
        table: str,
        columns: tuple[str, ...],
        keys: tuple[str, ...],
        expected: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        identities = list(
            {
                (
                    tuple(_text(row.get(field), field, required=True) for field in keys[:-1]),
                    _millis(row.get(keys[-1]), keys[-1]),
                ): None
                for row in expected
            }
        )
        select = ", ".join(
            f"CAST({_identifier(field)} AS BIGINT) AS {_identifier(field)}"
            if field == keys[-1]
            else _identifier(field)
            for field in columns
        )
        rows = []
        for start in range(0, len(identities), self._batch_size):
            series: dict[tuple[str, ...], list[int]] = {}
            for tags, stamp in identities[start : start + self._batch_size]:
                series.setdefault(tags, []).append(stamp)
            clauses = []
            for tags, stamps in series.items():
                tag_clause = " AND ".join(
                    f"{_identifier(field)} = {_literal(value)}"
                    for field, value in zip(keys[:-1], tags, strict=True)
                )
                timestamps = ", ".join(f"CAST({stamp} AS TIMESTAMP(3))" for stamp in stamps)
                clauses.append(f"({tag_clause} AND {_identifier(keys[-1])} IN ({timestamps}))")
            result = await self._execute(
                f"SELECT {select} FROM {table} WHERE " + " OR ".join(clauses)
            )
            rows.extend(self._records(result))
        return rows

    async def read_prices(self, points: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Read exact supplied identities; input rows only need the six price keys."""
        return await self._read(PRICE_TABLE, PRICE_COLUMNS, PRICE_KEYS, points)

    async def read_industry_indices(
        self, references: Iterable[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        return await self._read(REFERENCE_TABLE, REFERENCE_COLUMNS, REFERENCE_KEYS, references)

    @staticmethod
    def _verify(
        expected: list[dict[str, Any]], actual: list[dict[str, Any]], keys: tuple[str, ...]
    ) -> int:
        wanted = {tuple(row[field] for field in keys): row for row in expected}
        found: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in actual:
            try:
                key = tuple(row[field] for field in keys)
            except KeyError as exc:
                raise GreptimeReadbackError("Readback omitted an identity column") from exc
            if isinstance(key[-1], bool) or not isinstance(key[-1], int):
                raise GreptimeReadbackError("Readback timestamp is not integer milliseconds")
            if key in found:
                raise GreptimeReadbackError(f"Readback returned a duplicate identity: {key}")
            found[key] = row
        if wanted.keys() != found.keys():
            raise GreptimeReadbackError(
                f"Readback key mismatch: expected {len(wanted)}, found {len(found)}, "
                f"missing {len(wanted.keys() - found.keys())}, "
                f"unexpected {len(found.keys() - wanted.keys())}"
            )
        for key, row in wanted.items():
            changed = [
                field
                for field in row
                if row[field] != found[key].get(field)
                or (isinstance(row[field], bool) and not isinstance(found[key].get(field), bool))
            ]
            if row != found[key] or changed:
                raise GreptimeReadbackError(f"Readback value mismatch for {key}: {changed}")
        return len(wanted)

    async def verify_prices(self, points: Iterable[Mapping[str, Any]]) -> int:
        rows = _deduplicate((_normalise_price(point) for point in points), PRICE_KEYS)
        return self._verify(rows, await self.read_prices(rows), PRICE_KEYS)

    async def verify_industry_indices(self, references: Iterable[Mapping[str, Any]]) -> int:
        rows = _deduplicate(
            (_normalise_reference(reference) for reference in references), REFERENCE_KEYS
        )
        return self._verify(rows, await self.read_industry_indices(rows), REFERENCE_KEYS)

    async def _count(self, table: str, keys: tuple[str, ...], filters: Mapping[str, Any]) -> int:
        clauses = []
        for field, value in filters.items():
            if field not in keys[:-1]:
                raise ValueError(f"Unsupported series filter: {field}")
            clauses.append(f"{_identifier(field)} = {_literal(_text(value, field, required=True))}")
        sql = f'SELECT COUNT(*) AS "row_count" FROM {table}'
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        rows = self._records(await self._execute(sql))
        if len(rows) != 1 or isinstance(rows[0].get("row_count"), bool):
            raise GreptimeError("Greptime returned an invalid count result")
        count = rows[0].get("row_count")
        if not isinstance(count, int) or count < 0:
            raise GreptimeError("Greptime returned an invalid count result")
        return count

    async def count_prices(self, **series_filters: str) -> int:
        return await self._count(PRICE_TABLE, PRICE_KEYS, series_filters)

    async def count_industry_indices(self, **series_filters: str) -> int:
        return await self._count(REFERENCE_TABLE, REFERENCE_KEYS, series_filters)
