"""V24.5 holding references and switch intentions; no orders, fills or cash book.

The selection repository calls register_v245_selection in its existing serializable
transaction after saving the entry event. This module does not publish another
message. The primary selection message can render build_switch_plan and the exit
monitor can read pending_switch_positions after a T+1 deferral or restart.
"""

from __future__ import annotations

import re
from datetime import date, datetime, time
from math import isfinite
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from src.data.database.v20_repository import sha256_json

_ROOT = Path(__file__).resolve().parents[3]


def migration_sql(schema: str = "v20") -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError("invalid PostgreSQL schema identifier")
    return (
        (_ROOT / "migrations/v20/008_v245_positions.sql")
        .read_text(encoding="utf-8")
        .replace("v20.", f"{schema}.")
    )


def reference_clock(value: Any) -> time | None:
    if value is None:
        return None
    parsed = value if isinstance(value, time) else time.fromisoformat(str(value))
    if parsed.tzinfo is not None or not (
        time(9, 30) <= parsed <= time(11, 30) or time(13) <= parsed <= time(15)
    ):
        raise ValueError("invalid local trading reference clock")
    return parsed


def _date(value: Any) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _unclosed(position: Mapping[str, Any]) -> bool:
    return position.get("status") == "MONITORING" and position.get("quantity") != 0


def build_switch_plan(
    positions: Sequence[Mapping[str, Any]],
    *,
    day: date,
    permit: bool | None,
    calendar: Sequence[date],
) -> dict[str, Any] | None:
    """Describe an action sequence, never assume the sequence has executed.

    T+1 eligibility only means the buy date is an earlier confirmed exchange day;
    a real-time quote, price limit and actual remaining quantity still govern sale.
    An unknown quantity remains unknown, including an assumed bank reference.
    """
    if permit is None:
        return None
    if type(permit) is not bool:
        raise ValueError("permit must be an explicit boolean or unknown")
    day = _date(day)
    sessions = tuple(_date(d) for d in calendar)
    if day not in sessions or tuple(sorted(set(sessions))) != sessions:
        raise ValueError("missing or invalid exchange calendar")
    target = "STOCKS" if permit else "BANK"
    sell_kind = "BANK" if permit else "STOCK"
    selected = []
    for p in positions:
        if not _unclosed(p) or p.get("asset_kind", "STOCK") != sell_kind:
            continue
        entry = _date(p["entry_date"])
        if entry not in sessions or entry > day:
            raise ValueError("holding buy date is outside the exchange calendar")
        next_day = next((d for d in sessions if d > entry), None)
        eligible = entry < day
        selected.append(
            {
                "position_id": p["position_id"],
                "code": p["code"],
                "stock_name": p.get("stock_name") or p["code"],
                "entry_date": str(entry),
                "quantity": p.get("quantity"),
                "quantity_confirmed": p.get("quantity") is not None,
                "asset_kind": sell_kind,
                "t1_eligible": eligible,
                "earliest_sell_date": (
                    str(day if eligible else next_day) if eligible or next_day else None
                ),
                "requires_tradable_quote": True,
                "prior_exit_event_id": p.get("alert_event_id"),
            }
        )
    return {
        "schema": "v24.5-switch-plan/v1",
        "trade_date": str(day),
        "target": target,
        "notification_only": True,
        "orders_changed": False,
        "sell_positions": selected,
        "buy_code": "601398" if not permit else None,
        "buy_instruction": (
            "ORIGINAL_STOCK_ALLOCATION_AFTER_BANK_PROCEEDS"
            if permit
            else "ICBC_WITH_ACTUALLY_AVAILABLE_CASH"
        ),
        "requires_actual_proceeds": True,
        "reference_clock": None,
        "cash_scope": (
            "ORIGINAL_STOCK_ALLOCATION"
            if permit
            else "ALL_ACTUALLY_AVAILABLE_CASH_INCLUDING_LOCKED_PROFITS"
        ),
        "dividend_receivable_is_available_cash": False,
    }


async def register_v245_selection(
    connection: Any, schema: str, commit: Any, event_id: str
) -> dict[str, Any] | None:
    """Persist KNOWN target intentions; reruns never overwrite holding corrections.

    Called beside (not instead of) v22_positions.register_selection. No current
    unknown/failed input creates a bank purchase reference or a sell obligation.
    A new bank phase creates one *assumed* reference, not a confirmed holding.
    """
    if commit.strategy_version != "V24.5":
        return None
    transition = commit.semantic.get("v245_transition", {})
    if transition.get("signal_status") != "KNOWN":
        return None
    permit = transition.get("permit")
    if type(permit) is not bool or transition.get("trade_date") != str(commit.trade_date):
        raise ValueError("V24.5 transition is not bound to its selection date")
    if commit.action == "INPUT_INVALID":
        raise ValueError("invalid input cannot create a known switch intention")
    target = "STOCKS" if permit else "BANK"
    previous = await connection.fetchrow(
        f"SELECT target,phase_event_id,last_event_id FROM {schema}.v245_switch_intents "
        "WHERE official_stream_id=$1 AND lineage_id=$2 AND trade_date<=$3 "
        "ORDER BY trade_date DESC,updated_at DESC,intent_id DESC LIMIT 1",
        commit.official_stream_id,
        commit.lineage_id,
        commit.trade_date,
    )
    intent_id = sha256_json(
        [
            "V245_SWITCH_INTENT",
            commit.official_stream_id,
            commit.lineage_id,
            str(commit.trade_date),
            target,
        ]
    )
    phase_event_id = event_id
    if previous is not None and previous["target"] == target:
        phase_event_id = previous.get("phase_event_id") or previous.get("last_event_id") or event_id
    inserted = await connection.fetchval(
        f"INSERT INTO {schema}.v245_switch_intents "
        "(intent_id,official_stream_id,lineage_id,trade_date,target,source_event_id,"
        "last_event_id,phase_event_id) VALUES ($1,$2,$3,$4,$5,$6,$6,$7) ON CONFLICT "
        "(official_stream_id,lineage_id,trade_date,target) DO UPDATE SET "
        "last_event_id=EXCLUDED.last_event_id,phase_event_id=EXCLUDED.phase_event_id,"
        "updated_at=clock_timestamp() "
        f"WHERE {schema}.v245_switch_intents.last_event_id<>EXCLUDED.last_event_id "
        "RETURNING intent_id",
        intent_id,
        commit.official_stream_id,
        commit.lineage_id,
        commit.trade_date,
        target,
        event_id,
        phase_event_id,
    )
    if inserted is None:
        return None
    sell_kind = "BANK" if permit else "STOCK"
    positions = await connection.fetch(
        f"SELECT p.* FROM {schema}.v22_alert_positions p "
        f"JOIN {schema}.v22_alert_batches b USING(batch_id) "
        "WHERE b.official_stream_id=$1 AND b.lineage_id=$2 "
        "AND p.status='MONITORING' AND (p.quantity IS NULL OR p.quantity>0) "
        "AND p.asset_kind=$3 ORDER BY p.entry_date,p.position_id FOR UPDATE OF p",
        commit.official_stream_id,
        commit.lineage_id,
        sell_kind,
    )
    for p in positions:
        await connection.execute(
            f"INSERT INTO {schema}.v245_position_switches "
            "(position_id,intent_id,requested_date,phase_event_id) VALUES ($1,$2,$3,$4) "
            "ON CONFLICT(position_id) DO NOTHING",
            p["position_id"],
            intent_id,
            commit.trade_date,
            phase_event_id,
        )
    if not permit and (previous is None or previous["target"] != "BANK"):
        batch_id = sha256_json(
            [
                "V245_ASSUMED_BANK",
                commit.official_stream_id,
                commit.lineage_id,
                str(commit.trade_date),
            ]
        )
        created = await connection.fetchval(
            f"INSERT INTO {schema}.v22_alert_batches "
            "(batch_id,official_stream_id,lineage_id,trade_date,source_event_id,reference_kind) "
            "VALUES ($1,$2,$3,$4,$5,'BANK') ON CONFLICT "
            "(official_stream_id,lineage_id,trade_date,reference_kind) "
            "DO NOTHING RETURNING batch_id",
            batch_id,
            commit.official_stream_id,
            commit.lineage_id,
            commit.trade_date,
            event_id,
        )
        if created is not None:
            await connection.execute(
                f"INSERT INTO {schema}.v22_alert_positions "
                "(position_id,batch_id,code,stock_name,entry_date,asset_kind,"
                "strategy_version,reference_clock) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                sha256_json([batch_id, "601398"]),
                batch_id,
                "601398",
                "工商银行",
                commit.trade_date,
                "BANK",
                "V24.5",
                None,
            )
    return {"intent_id": intent_id, "target": target, "notification_only": True}


async def pending_switch_positions(connection: Any, schema: str, config: Any) -> list[dict]:
    """All still-unclosed switch obligations, including previously alerted stocks."""
    from src.data.database.v22_positions import public

    rows = await connection.fetch(
        f"SELECT p.*,s.requested_date,s.intent_id,s.switch_alert_event_id,s.phase_event_id,"
        "i.target AS switch_target,current_intent.target AS current_target,"
        "current_intent.last_event_id AS current_target_event_id,"
        "current_intent.updated_at AS current_target_available_at,"
        "current_intent.phase_event_id AS current_phase_event_id,"
        "suppressed.reason AS switch_suppression_reason,"
        "old_alert.delivery_status AS switch_delivery_status "
        f"FROM {schema}.v245_position_switches s "
        f"JOIN {schema}.v245_switch_intents i USING(intent_id) "
        f"JOIN {schema}.v22_alert_positions p USING(position_id) "
        f"JOIN {schema}.v22_alert_batches b USING(batch_id) "
        f"LEFT JOIN {schema}.v245_delivery_suppressions suppressed "
        "ON suppressed.event_id=s.switch_alert_event_id "
        f"LEFT JOIN {schema}.outbox_events old_alert ON old_alert.event_id=s.switch_alert_event_id "
        "JOIN LATERAL (SELECT target,last_event_id,updated_at,phase_event_id "
        f"FROM {schema}.v245_switch_intents ci "
        "WHERE ci.official_stream_id=b.official_stream_id AND ci.lineage_id=b.lineage_id "
        "ORDER BY ci.trade_date DESC,ci.updated_at DESC,ci.intent_id DESC LIMIT 1) "
        "current_intent ON TRUE "
        "WHERE b.official_stream_id=$1 AND b.lineage_id=$2 AND p.status='MONITORING' "
        "AND (p.quantity IS NULL OR p.quantity>0) ORDER BY s.requested_date,p.position_id",
        config.official_stream_id,
        config.state_lineage_id,
    )
    return [public(row) for row in rows]


def can_replace_suppressed_switch(position: Mapping[str, Any]) -> bool:
    """Only a genuinely new target phase may replace an obsolete unsent alert."""
    phase = position.get("current_phase_event_id")
    return bool(
        phase
        and phase != position.get("phase_event_id")
        and position.get("switch_suppression_reason") == "V245_TARGET_SUPERSEDED"
        and position.get("switch_delivery_status") != "SENT"
    )


async def apply_switch(
    store: Any,
    position: Mapping[str, Any],
    *,
    trigger_at: datetime,
    trigger_price: float,
    quote_volume: float,
    down_limit: float,
    quote_source: str,
) -> str | None:
    """Atomically persist an actionable sell *notification*, never an execution.

    The monitor owns live quote acquisition and the exchange-calendar check.
    Recheck holding revision, T+1, latest target and basic quote eligibility under
    the selection transaction's state lock before creating the separate alert.
    """
    from src.data.database.v20_repository import canonical_json
    from src.strategy.v20.models import (
        V20_DATA_ALERT_SEMANTIC_SCHEMA,
        V20_FEISHU_FORMATTER_PROFILE,
    )

    if not isinstance(trigger_at, datetime) or trigger_at.tzinfo is None:
        raise ValueError("switch quote requires an aware signal timestamp")
    trigger_at = trigger_at.astimezone(ZoneInfo("Asia/Shanghai"))
    for value in (trigger_price, quote_volume, down_limit):
        if isinstance(value, bool) or not isfinite(value) or value <= 0:
            raise ValueError("switch quote has invalid price, volume or price limit")
    if trigger_price <= down_limit or not isinstance(quote_source, str) or not quote_source:
        raise ValueError("switch quote is unavailable or at the sell-side lower limit")
    wall = trigger_at.timetz().replace(tzinfo=None)
    if not (time(9, 30) <= wall <= time(11, 30) or time(13) <= wall <= time(15)):
        raise ValueError("switch quote is outside a trading session")
    day = trigger_at.date()
    schema = store.schema
    async with store.repository.pool.acquire() as con:
        async with con.transaction():
            # Match selection lock ordering. A concurrent target change must not
            # create a contradictory alert from a stale monitor snapshot.
            await con.fetchrow(
                f"SELECT revision FROM {schema}.official_state WHERE lineage_id=$1 FOR UPDATE",
                store.config.state_lineage_id,
            )
            row = await store._locked(con, position["position_id"])
            if (
                not _unclosed(row)
                or row["revision"] != position["revision"]
                or _date(row["entry_date"]) >= day
            ):
                return None
            pending = await con.fetchrow(
                f"SELECT s.*,i.target AS switch_target,ci.target AS current_target,"
                "ci.last_event_id AS current_target_event_id,ci.trade_date AS current_target_date,"
                "ci.updated_at AS current_target_available_at,"
                "ci.phase_event_id AS current_phase_event_id,ci.intent_id AS current_intent_id,"
                "suppressed.reason AS switch_suppression_reason,"
                "old_alert.delivery_status AS switch_delivery_status "
                f"FROM {schema}.v245_position_switches s "
                f"JOIN {schema}.v245_switch_intents i USING(intent_id) "
                f"LEFT JOIN {schema}.v245_delivery_suppressions suppressed "
                "ON suppressed.event_id=s.switch_alert_event_id "
                f"LEFT JOIN {schema}.outbox_events old_alert "
                "ON old_alert.event_id=s.switch_alert_event_id "
                "JOIN LATERAL (SELECT target,last_event_id,trade_date,updated_at,"
                "phase_event_id,intent_id "
                f"FROM {schema}.v245_switch_intents latest "
                "WHERE latest.official_stream_id=i.official_stream_id "
                "AND latest.lineage_id=i.lineage_id ORDER BY latest.trade_date DESC,"
                "latest.updated_at DESC,latest.intent_id DESC LIMIT 1) ci ON TRUE "
                "WHERE s.position_id=$1 FOR UPDATE OF s",
                position["position_id"],
            )
            if pending is None:
                return None
            available_at = pending.get("current_target_available_at")
            if available_at is not None:
                if isinstance(available_at, str):
                    available_at = datetime.fromisoformat(available_at)
                if available_at.tzinfo is None or trigger_at < available_at:
                    return None
            target = pending["current_target"]
            expected_target = "STOCKS" if row.get("asset_kind", "STOCK") == "BANK" else "BANK"
            if (
                target != pending["switch_target"]
                or target != expected_target
                or _date(pending.get("current_target_date", day)) > day
                or _date(pending.get("requested_date", day)) > day
            ):
                return None
            old_alert_id = pending["switch_alert_event_id"]
            if old_alert_id is not None and not can_replace_suppressed_switch(pending):
                return None if pending.get("switch_suppression_reason") else str(old_alert_id)
            phase_event_id = (
                pending.get("current_phase_event_id") or pending["current_target_event_id"]
            )
            event_id = sha256_json(["V245_SWITCH_EXIT_ALERT", row["position_id"], phase_event_id])
            semantic = {
                "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
                "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
                "event_id": event_id,
                "event_type": "DATA_ALERT",
                "alert_code": "V245_SWITCH_EXIT_ALERT",
                "strategy_version": "V24.5",
                "deployment_mode": getattr(store.config, "deployment_mode", "forward_shadow"),
                "event_trade_date": str(day),
                "position_id": row["position_id"],
                "code": row["code"],
                "stock_name": row["stock_name"],
                "asset_kind": row.get("asset_kind", "STOCK"),
                "entry_date": str(row["entry_date"]),
                "entry_price": row.get("entry_price"),
                "quantity": row["quantity"],
                "quantity_confirmed": row["quantity"] is not None,
                "trigger_at": trigger_at.isoformat(),
                "trigger_price": float(trigger_price),
                "quote_volume": float(quote_volume),
                "down_limit": float(down_limit),
                "quote_source": quote_source,
                "switch_target": target,
                "target_event_id": pending["current_target_event_id"],
                "switch_phase_event_id": phase_event_id,
                "replaces_suppressed_alert_id": old_alert_id,
                "reason": "SWITCH_TO_STOCKS" if target == "STOCKS" else "SWITCH_TO_ICBC",
                "notification_only": True,
                "orders_changed": False,
                "delivery_priority_class": "OPERATOR_NOTIFICATION",
            }
            await con.execute(
                f"INSERT INTO {schema}.outbox_events "
                "(event_id,event_type,route_id,official_stream_id,lineage_id,"
                "semantic_content_hash,semantic_json) "
                "VALUES ($1,'DATA_ALERT',$2,$3,$4,$5,$6::jsonb)",
                event_id,
                getattr(store.config, "route_id", ""),
                store.config.official_stream_id,
                store.config.state_lineage_id,
                sha256_json(semantic),
                canonical_json(semantic),
            )
            await con.execute(
                f"UPDATE {schema}.v245_position_switches SET switch_alert_event_id=$2,"
                "phase_event_id=$4,intent_id=$5 WHERE position_id=$1 "
                "AND switch_alert_event_id IS NOT DISTINCT FROM $3",
                row["position_id"],
                event_id,
                old_alert_id,
                phase_event_id,
                pending.get("current_intent_id") or pending["intent_id"],
            )
            return event_id
