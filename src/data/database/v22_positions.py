"""Assumed Top3 records and audited corrections, separate from orders and V20 lots."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

from src.data.database.v20_repository import V20StateConflict, canonical_json, sha256_json
from src.strategy.v20.models import V20_DATA_ALERT_SEMANTIC_SCHEMA, V20_FEISHU_FORMATTER_PROFILE


def public(row: Any) -> dict[str, Any]:
    return json.loads(json.dumps(dict(row), default=str))


async def register_selection(connection: Any, schema: str, commit: Any, event_id: str) -> None:
    if commit.strategy_version != "V22-slim" or commit.action != "ENTER":
        return
    batch_id = sha256_json(
        ["V22_ASSUMED_BUYS", commit.official_stream_id, commit.lineage_id, str(commit.trade_date)]
    )
    inserted = await connection.fetchval(
        f"INSERT INTO {schema}.v22_alert_batches "
        "(batch_id,official_stream_id,lineage_id,trade_date,source_event_id) "
        "VALUES ($1,$2,$3,$4,$5) "
        "ON CONFLICT (official_stream_id,lineage_id,trade_date) DO NOTHING RETURNING batch_id",
        batch_id,
        commit.official_stream_id,
        commit.lineage_id,
        commit.trade_date,
        event_id,
    )
    if inserted is None:
        return
    symbols = commit.semantic["symbols"][:3]
    for stock in symbols:
        await connection.execute(
            f"INSERT INTO {schema}.v22_alert_positions "
            "(position_id,batch_id,code,stock_name,entry_date) VALUES ($1,$2,$3,$4,$5)",
            sha256_json([batch_id, stock["code"]]),
            batch_id,
            stock["code"],
            stock.get("name") or stock["code"],
            commit.trade_date,
        )


class V22PositionStore:
    def __init__(self, repository: Any, config: Any):
        self.repository, self.config = repository, config
        self.schema = repository.schema

    async def list(self, *, active: bool = False) -> list[dict[str, Any]]:
        async with self.repository.pool.acquire() as connection:
            rows = await connection.fetch(
                f"SELECT p.*,b.source_event_id,b.trade_date AS recommendation_date "
                f"FROM {self.schema}.v22_alert_positions p "
                f"JOIN {self.schema}.v22_alert_batches b USING(batch_id) "
                "WHERE b.official_stream_id=$1 AND b.lineage_id=$2 "
                + ("AND p.status='MONITORING' AND p.alert_event_id IS NULL " if active else "")
                + "ORDER BY p.entry_date,p.position_id",
                self.config.official_stream_id,
                self.config.state_lineage_id,
            )
        return [public(row) for row in rows]

    async def _locked(self, connection: Any, position_id: str) -> Any:
        row = await connection.fetchrow(
            f"SELECT p.* FROM {self.schema}.v22_alert_positions p "
            f"JOIN {self.schema}.v22_alert_batches b USING(batch_id) "
            "WHERE p.position_id=$1 AND b.official_stream_id=$2 "
            "AND b.lineage_id=$3 FOR UPDATE OF p",
            position_id,
            self.config.official_stream_id,
            self.config.state_lineage_id,
        )
        if row is None:
            raise V20StateConflict("V22 holding record does not exist")
        return row

    async def calibrate(
        self, position_id: str, request_id: str, change: dict[str, Any]
    ) -> dict[str, Any]:
        fingerprint = sha256_json([position_id, change])
        async with self.repository.pool.acquire() as connection:
            async with connection.transaction():
                row = await self._locked(connection, position_id)
                previous = await connection.fetchrow(
                    f"SELECT request_hash,after_json FROM {self.schema}.v22_position_calibrations "
                    "WHERE request_id=$1",
                    request_id,
                )
                if previous is not None:
                    if previous["request_hash"] != fingerprint:
                        raise V20StateConflict(
                            "calibration request ID already has different content"
                        )
                    result = previous["after_json"]
                    return json.loads(result) if isinstance(result, str) else dict(result)
                if row["revision"] != change["expected_revision"]:
                    raise V20StateConflict(
                        "holding record changed; reload its revision before correction"
                    )
                before = public(row)
                entry_date = date.fromisoformat(change.get("entry_date", str(row["entry_date"])))
                price = change.get("entry_price", row["entry_price"])
                price_source = "MANUAL" if "entry_price" in change else row["price_source"]
                if (
                    entry_date != row["entry_date"]
                    and "entry_price" not in change
                    and price_source != "MANUAL"
                ):
                    price, price_source = None, "PENDING_REFERENCE"
                quantity = change.get("quantity", row["quantity"])
                status = change.get("status", row["status"])
                if quantity == 0:
                    status = "CLOSED" if status != "NOT_BOUGHT" else status
                after = await connection.fetchrow(
                    f"UPDATE {self.schema}.v22_alert_positions "
                    "SET entry_date=$2,entry_price=$3,quantity=$4,"
                    "status=$5,calibrated=TRUE,price_source=$6,extended=$7,revision=revision+1,"
                    "updated_at=clock_timestamp() WHERE position_id=$1 RETURNING *",
                    position_id,
                    entry_date,
                    price,
                    quantity,
                    status,
                    price_source,
                    bool(row["extended"])
                    and entry_date == row["entry_date"]
                    and price == row["entry_price"],
                )
                result = public(after)
                await connection.execute(
                    f"INSERT INTO {self.schema}.v22_position_calibrations "
                    "(request_id,position_id,request_hash,before_json,after_json) "
                    "VALUES ($1,$2,$3,$4::jsonb,$5::jsonb)",
                    request_id,
                    position_id,
                    fingerprint,
                    canonical_json(before),
                    canonical_json(result),
                )
                return result

    async def apply(
        self,
        position: dict[str, Any],
        *,
        reference: float | None = None,
        extended: bool = False,
        signal: Any = None,
    ) -> str | None:
        async with self.repository.pool.acquire() as connection:
            async with connection.transaction():
                row = await self._locked(connection, position["position_id"])
                if row["revision"] != position["revision"] or row["status"] != "MONITORING":
                    return None
                if row["alert_event_id"] is not None:
                    return str(row["alert_event_id"])
                price = row["entry_price"] if row["entry_price"] is not None else reference
                price_source = (
                    row["price_source"]
                    if row["entry_price"] is not None or reference is None
                    else "STRATEGY_REFERENCE"
                )
                alert_id = None
                if signal is not None:
                    alert_id = sha256_json(["V22_EXIT_ALERT", position["position_id"]])
                    semantic = {
                        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
                        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
                        "event_id": alert_id,
                        "event_type": "DATA_ALERT",
                        "alert_code": "V22_EXIT_ALERT",
                        "deployment_mode": self.config.deployment_mode,
                        "strategy_version": "V22-slim",
                        "event_trade_date": signal.at.date().isoformat(),
                        "position_id": position["position_id"],
                        "code": row["code"],
                        "stock_name": row["stock_name"],
                        "entry_date": str(row["entry_date"]),
                        "entry_price": price,
                        "quantity": row["quantity"],
                        "price_source": price_source,
                        "calibrated": row["calibrated"],
                        "trigger_at": signal.at.isoformat(),
                        "trigger_price": signal.price,
                        "reason": signal.reason,
                        "message": signal.detail,
                        "delivery_priority_class": "OPERATOR_NOTIFICATION",
                        "orders_changed": False,
                    }
                    await connection.execute(
                        f"INSERT INTO {self.schema}.outbox_events "
                        "(event_id,event_type,route_id,official_stream_id,lineage_id,"
                        "semantic_content_hash,semantic_json) "
                        "VALUES ($1,'DATA_ALERT',$2,$3,$4,$5,$6::jsonb)",
                        alert_id,
                        self.config.route_id,
                        self.config.official_stream_id,
                        self.config.state_lineage_id,
                        sha256_json(semantic),
                        canonical_json(semantic),
                    )
                await connection.execute(
                    f"UPDATE {self.schema}.v22_alert_positions "
                    "SET entry_price=$2,price_source=$3,extended=$4,alert_event_id=$5,"
                    "revision=revision+1,updated_at=clock_timestamp() WHERE position_id=$1",
                    position["position_id"],
                    price,
                    price_source,
                    extended,
                    alert_id,
                )
                return alert_id
