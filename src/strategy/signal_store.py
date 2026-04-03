# === MODULE PURPOSE ===
# In-memory trading signal queue.
# Extracted from iquant_routes.py to decouple signal management from HTTP routes.
#
# === KEY CONCEPTS ===
# - Signal: a buy/sell order waiting for execution by iQuant
# - SignalStore: push/poll/ack/expire lifecycle management
# - No persistence — signals that miss their window are useless

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    """A trading signal in the pending/executed queue.

    Wire format (to_wire_dict) must match what iquant_live.py consumes
    from GET /pending-signals.
    """

    id: str
    type: str  # "buy" or "sell"
    stock_code: str
    stock_name: str = ""
    quantity: int = 0
    price: float | None = None
    price_type: str = "market"  # "market" or "limit"
    reason: str = ""
    manual: bool = False
    created_at: str = ""  # HH:MM:SS string
    pushed_at: datetime | None = None  # for timeout tracking
    acked_at: str | None = None  # HH:MM:SS when acked

    # Scanner-specific fields (buy signals from momentum scanner)
    latest_price: float = 0.0
    board_name: str = ""
    v3_score: float = 0.0

    # Internal tracking (not serialized)
    _timeout_alerted: bool = field(default=False, repr=False)

    def to_wire_dict(self) -> dict[str, Any]:
        """Serialize for JSON API response.

        Produces the exact format consumed by iquant_live.py and dashboard.
        Internal fields (_timeout_alerted, pushed_at) are excluded.
        """
        d: dict[str, Any] = {
            "id": self.id,
            "type": self.type,
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "quantity": self.quantity,
            "price": self.price,
            "price_type": self.price_type,
            "reason": self.reason,
            "manual": self.manual,
            "created_at": self.created_at,
        }
        # Include scanner fields only if set (backward compat)
        if self.latest_price:
            d["latest_price"] = self.latest_price
        if self.board_name:
            d["board_name"] = self.board_name
        if self.v3_score:
            d["v3_score"] = self.v3_score
        if self.acked_at:
            d["acked_at"] = self.acked_at
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any], now: datetime | None = None) -> Signal:
        """Create Signal from a raw dict (backward compat with manual-order callers).

        Assigns ID and timestamps if missing.
        """
        if now is None:
            now = datetime.now()
        return cls(
            id=data.get("id", str(uuid.uuid4())[:8]),
            type=data["type"],
            stock_code=data["stock_code"],
            stock_name=data.get("stock_name", ""),
            quantity=data.get("quantity", 0),
            price=data.get("price"),
            price_type=data.get("price_type", "market"),
            reason=data.get("reason", ""),
            manual=data.get("manual", False),
            created_at=data.get("created_at", now.strftime("%H:%M:%S")),
            pushed_at=now,
            latest_price=data.get("latest_price", 0.0),
            board_name=data.get("board_name", ""),
            v3_score=data.get("v3_score", 0.0),
        )


class SignalStore:
    """In-memory signal queue with timeout and expiry management.

    Thread-safe for concurrent access from HTTP handlers and background tasks
    (single-threaded asyncio — no locking needed).

    Signal lifecycle:
        push() → pending → get_active() by iQuant → ack() → executed history
                        ↘ expire_stale() → discarded (if too old)
    """

    TIMEOUT_MINUTES = 5  # alert if not acked within this time
    EXPIRY_MINUTES = 10  # auto-expire after this time

    def __init__(self) -> None:
        self._pending: list[Signal] = []
        self._executed: list[Signal] = []

    def push(self, signal: Signal) -> Signal:
        """Add signal to pending queue."""
        self._pending.append(signal)
        logger.info(f"Signal pushed: {signal.type} {signal.stock_code} (id={signal.id})")
        return signal

    def push_dict(self, data: dict[str, Any], now: datetime | None = None) -> Signal:
        """Create Signal from raw dict and push to pending queue.

        Convenience method for callers that build signal dicts (manual-order, etc.).
        """
        signal = Signal.from_dict(data, now=now)
        return self.push(signal)

    def get_active(self, now: datetime) -> list[Signal]:
        """Return pending signals that have not expired.

        Used by GET /pending-signals to return only actionable signals.
        """
        expiry_seconds = self.EXPIRY_MINUTES * 60
        return [
            s
            for s in self._pending
            if not s.pushed_at or (now - s.pushed_at).total_seconds() < expiry_seconds
        ]

    def get_active_wire(self, now: datetime) -> list[dict[str, Any]]:
        """Return active pending signals as wire-format dicts."""
        return [s.to_wire_dict() for s in self.get_active(now)]

    def get_pending_dicts(self) -> list[dict[str, Any]]:
        """Return all pending signals as dicts for dashboard display."""
        return [
            {
                "id": s.id,
                "type": s.type,
                "stock_code": s.stock_code,
                "stock_name": s.stock_name,
                "quantity": s.quantity,
                "price_type": s.price_type,
                "price": s.price,
                "reason": s.reason,
                "created_at": s.created_at,
                "manual": s.manual,
            }
            for s in self._pending
        ]

    def ack(self, signal_id: str, now: datetime | None = None) -> Signal | None:
        """Mark signal as executed. Moves from pending to executed.

        Returns the acked signal, or None if not found.
        """
        for i, sig in enumerate(self._pending):
            if sig.id == signal_id:
                found = self._pending.pop(i)
                if now is None:
                    now = datetime.now()
                found.acked_at = now.strftime("%H:%M:%S")
                self._executed.append(found)
                logger.info(f"{found.type.upper()} acked {found.stock_code}")
                return found
        return None

    def cancel(self, signal_id: str) -> bool:
        """Remove signal from pending queue. Returns True if found."""
        for i, sig in enumerate(self._pending):
            if sig.id == signal_id:
                removed = self._pending.pop(i)
                logger.info(f"Signal cancelled: {removed.stock_code} (id={signal_id})")
                return True
        return False

    def expire_stale(self, now: datetime) -> list[Signal]:
        """Remove signals older than EXPIRY_MINUTES.

        Returns the list of expired signals (for alerting by caller).
        """
        still_pending: list[Signal] = []
        expired: list[Signal] = []
        for sig in self._pending:
            if not sig.pushed_at:
                still_pending.append(sig)
                continue
            age_minutes = (now - sig.pushed_at).total_seconds() / 60
            if age_minutes >= self.EXPIRY_MINUTES:
                expired.append(sig)
                logger.error(f"Signal expired: {sig.stock_code} ({age_minutes:.0f}min), removed")
            else:
                still_pending.append(sig)
        self._pending = still_pending
        return expired

    def get_timed_out(self, now: datetime) -> list[Signal]:
        """Return signals past TIMEOUT_MINUTES that haven't been alerted yet.

        Marks them as alerted so each signal only triggers one timeout alert.
        """
        timed_out: list[Signal] = []
        for sig in self._pending:
            if not sig.pushed_at:
                continue
            age_minutes = (now - sig.pushed_at).total_seconds() / 60
            if age_minutes >= self.TIMEOUT_MINUTES and not sig._timeout_alerted:
                sig._timeout_alerted = True
                timed_out.append(sig)
        return timed_out

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def executed_signals(self) -> list[Signal]:
        return list(self._executed)
