"""Stable V20 identifiers derived only from immutable strategy semantics."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any


def named_hash(domain: str, fields: Mapping[str, Any]) -> str:
    if not domain.startswith("V20_"):
        raise ValueError("V20 hash domain must start with V20_")
    encoded = json.dumps(
        [domain, dict(fields)],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def official_slot_id(official_stream_id: str, trade_date: str) -> str:
    return named_hash(
        "V20_OFFICIAL_SLOT_ID_V1",
        {"official_stream_id": official_stream_id, "trade_date": trade_date},
    )


def decision_id(slot_id: str, config_hash: str, snapshot_hash: str, state_before_hash: str) -> str:
    return named_hash(
        "V20_DECISION_ID_V1",
        {
            "slot_id": slot_id,
            "config_hash": config_hash,
            "snapshot_hash": snapshot_hash,
            "state_before_hash": state_before_hash,
        },
    )


def event_id(event_type: str, owner_id: str) -> str:
    return named_hash(
        f"V20_{event_type}_EVENT_ID_V1",
        {"owner_id": owner_id},
    )


def batch_id(decision: str, kind: str) -> str:
    return named_hash(
        "V20_SHADOW_BATCH_ID_V1",
        {"decision_id": decision, "kind": kind},
    )


def model_batch_id(decision: str) -> str:
    return named_hash("V20_MODEL_BATCH_ID_V1", {"decision_id": decision})


__all__ = [
    "batch_id",
    "decision_id",
    "event_id",
    "model_batch_id",
    "named_hash",
    "official_slot_id",
]
