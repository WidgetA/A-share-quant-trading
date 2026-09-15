"""Audited user holdings for legacy recommendations; never invent an execution."""

import json
from typing import Any

from src.data.database.v20_repository import (
    V20StateConflict,
    _model_batch_authorization_sql,
    canonical_json,
    sha256_json,
)


class LegacyPositionStore:
    def __init__(self, repository: Any, config: Any):
        self.repository, self.config = repository, config
        self.schema = repository.schema

    def _select(self) -> str:
        return f"""
            SELECT leg.model_leg_id AS position_id,leg.code,leg.stock_name,
                   batch.signal_date AS recommendation_date,
                   leg.user_position_status AS status,
                   leg.user_remaining_quantity AS quantity,
                   leg.user_position_revision AS revision,
                   leg.user_position_updated_at AS updated_at
            FROM {self.schema}.model_legs AS leg
            JOIN {self.schema}.model_batches AS batch USING (model_batch_id)
            JOIN {self.schema}.outbox_events AS source
              ON source.event_id=batch.source_event_id
            WHERE batch.official_stream_id=$1 AND batch.lineage_id=$2
              AND source.official_stream_id=$1 AND source.lineage_id=$2
              AND source.seal_status='SEALED' AND batch.evaluation_only=FALSE
              AND {_model_batch_authorization_sql(self.schema)}
        """

    @property
    def scope(self) -> tuple[str, str]:
        return self.config.official_stream_id, self.config.state_lineage_id

    @staticmethod
    def public(row: Any) -> dict[str, Any]:
        return json.loads(json.dumps(dict(row), default=str))

    async def list(self) -> list[dict[str, Any]]:
        async with self.repository.pool.acquire() as connection:
            rows = await connection.fetch(
                self._select() + " ORDER BY batch.signal_date,leg.rank,leg.model_leg_id",
                *self.scope,
            )
        return [self.public(row) for row in rows]

    async def calibrate(
        self, position_id: str, request_id: str, change: dict[str, Any]
    ) -> dict[str, Any]:
        fingerprint = sha256_json([self.scope, position_id, change])
        async with self.repository.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    self._select() + " AND leg.model_leg_id=$3 FOR UPDATE OF leg",
                    *self.scope,
                    position_id,
                )
                if row is None:
                    raise V20StateConflict("legacy holding record does not exist in this scope")
                previous = await connection.fetchrow(
                    "SELECT request_hash,after_json "
                    f"FROM {self.schema}.legacy_position_calibrations WHERE request_id=$1",
                    request_id,
                )
                if previous is not None:
                    if previous["request_hash"] != fingerprint:
                        raise V20StateConflict("calibration request ID has different content")
                    result = previous["after_json"]
                    return json.loads(result) if isinstance(result, str) else dict(result)
                if row["revision"] != change["expected_revision"]:
                    raise V20StateConflict("holding changed; reload its revision before correction")
                before = self.public(row)
                quantity = change.get("quantity", row["quantity"])
                status = change.get("status")
                if status in ("CLOSED", "NOT_BOUGHT"):
                    quantity = 0
                elif "quantity" in change:
                    status = "CLOSED" if quantity == 0 else "MONITORING"
                else:
                    status = status or row["status"]
                    if status == "MONITORING" and quantity == 0:
                        raise V20StateConflict("reopening requires a positive remaining quantity")
                await connection.execute(
                    f"UPDATE {self.schema}.model_legs SET user_position_status=$2,"
                    "user_remaining_quantity=$3,user_position_revision=user_position_revision+1,"
                    "user_position_updated_at=clock_timestamp() WHERE model_leg_id=$1",
                    position_id,
                    status,
                    quantity,
                )
                after = await connection.fetchrow(
                    self._select() + " AND leg.model_leg_id=$3", *self.scope, position_id
                )
                result = self.public(after)
                await connection.execute(
                    f"INSERT INTO {self.schema}.legacy_position_calibrations "
                    "(request_id,model_leg_id,request_hash,before_json,after_json) "
                    "VALUES ($1,$2,$3,$4::jsonb,$5::jsonb)",
                    request_id,
                    position_id,
                    fingerprint,
                    canonical_json(before),
                    canonical_json(result),
                )
                return result
