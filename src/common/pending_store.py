# === MODULE PURPOSE ===
# Thread-safe in-memory store for pending user confirmations.
# Bridges strategy engine and Web UI for trading decisions.

# === KEY CONCEPTS ===
# - PendingConfirmation: A request waiting for user response
# - Uses asyncio.Event for async waiting
# - Automatic expiration cleanup

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


class ConfirmationType(str, Enum):
    """Types of confirmation requests."""

    PREMARKET = "premarket"  # Premarket signal selection
    INTRADAY = "intraday"  # Intraday buy confirmation
    MORNING = "morning"  # Morning sell confirmation
    LIMIT_UP = "limit_up"  # Limit-up situation handling


@dataclass
class PendingConfirmation:
    """
    A confirmation request waiting for user response.

    Lifecycle: PENDING -> COMPLETED/EXPIRED

    The strategy engine creates this and waits on `completed` event.
    Web UI submits result via `submit_result()`.
    """

    id: str
    confirm_type: ConfirmationType
    title: str
    data: dict[str, Any]  # Type-specific data (signals, holdings, etc.)
    created_at: datetime
    expires_at: datetime
    completed: asyncio.Event = field(default_factory=asyncio.Event)
    result: Any | None = None

    def is_expired(self) -> bool:
        """Check if this confirmation has expired."""
        return datetime.now(BEIJING_TZ) > self.expires_at

    def remaining_seconds(self) -> float:
        """Get remaining time in seconds."""
        remaining = (self.expires_at - datetime.now(BEIJING_TZ)).total_seconds()
        return max(0, remaining)

    def to_summary_dict(self) -> dict[str, Any]:
        """Convert to summary dict for listing."""
        return {
            "id": self.id,
            "type": self.confirm_type.value,
            "title": self.title,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "remaining_seconds": int(self.remaining_seconds()),
            "is_expired": self.is_expired(),
        }

    def to_detail_dict(self) -> dict[str, Any]:
        """Convert to detailed dict for single confirmation view."""
        return {
            **self.to_summary_dict(),
            "data": self.data,
        }


class PendingConfirmationStore:
    """
    Thread-safe store for pending user confirmations.

    Usage:
        store = PendingConfirmationStore()

        # Strategy side: create and wait
        confirm = store.create("premarket", {...}, timeout=300)
        result = await store.wait_for_result(confirm.id)

        # Web UI side: submit result
        store.submit_result(confirm_id, user_selection)
    """

    def __init__(self):
        """Initialize store."""
        self._pending: dict[str, PendingConfirmation] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task | None = None

    def start_cleanup_task(self) -> None:
        """Start background cleanup task."""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.debug("Started pending confirmation cleanup task")

    def stop_cleanup_task(self) -> None:
        """Stop background cleanup task."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            self._cleanup_task = None
            logger.debug("Stopped pending confirmation cleanup task")

    async def _cleanup_loop(self) -> None:
        """Background loop to clean up expired confirmations."""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                await self.cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")

    async def create(
        self,
        confirm_type: ConfirmationType,
        title: str,
        data: dict[str, Any],
        timeout: float,
    ) -> PendingConfirmation:
        """
        Create a new pending confirmation.

        Args:
            confirm_type: Type of confirmation.
            title: Display title.
            data: Type-specific data.
            timeout: Timeout in seconds.

        Returns:
            Created PendingConfirmation.
        """
        now = datetime.now(BEIJING_TZ)
        confirm = PendingConfirmation(
            id=str(uuid.uuid4())[:8],
            confirm_type=confirm_type,
            title=title,
            data=data,
            created_at=now,
            expires_at=now + timedelta(seconds=timeout),
        )

        async with self._lock:
            self._pending[confirm.id] = confirm

        logger.info(
            f"Created pending confirmation: {confirm.id} "
            f"type={confirm_type.value} timeout={timeout}s"
        )
        return confirm

    async def wait_for_result(
        self,
        confirm_id: str,
        default_result: Any = None,
    ) -> Any:
        """
        Wait for user to submit result.

        Args:
            confirm_id: Confirmation ID.
            default_result: Result to return on timeout.

        Returns:
            User's submitted result, or default_result on timeout.
        """
        async with self._lock:
            confirm = self._pending.get(confirm_id)

        if not confirm:
            logger.warning(f"Confirmation {confirm_id} not found")
            return default_result

        try:
            timeout = confirm.remaining_seconds()
            if timeout <= 0:
                logger.info(f"Confirmation {confirm_id} already expired")
                return default_result

            await asyncio.wait_for(confirm.completed.wait(), timeout=timeout)
            return confirm.result

        except asyncio.TimeoutError:
            logger.info(f"Confirmation {confirm_id} timed out, using default")
            return default_result

        finally:
            # Clean up after wait completes
            async with self._lock:
                self._pending.pop(confirm_id, None)

    def submit_result(self, confirm_id: str, result: Any) -> bool:
        """
        Submit user's result for a confirmation.

        This is called from Web UI when user makes a selection.

        Args:
            confirm_id: Confirmation ID.
            result: User's selection/decision.

        Returns:
            True if submission succeeded.
        """
        confirm = self._pending.get(confirm_id)
        if not confirm:
            logger.warning(f"Cannot submit: confirmation {confirm_id} not found")
            return False

        if confirm.is_expired():
            logger.warning(f"Cannot submit: confirmation {confirm_id} expired")
            return False

        confirm.result = result
        confirm.completed.set()
        logger.info(f"Submitted result for confirmation {confirm_id}")
        return True

    def get_pending_list(self) -> list[dict[str, Any]]:
        """
        Get list of all pending confirmations.

        Returns:
            List of confirmation summaries.
        """
        result = []
        for confirm in self._pending.values():
            if not confirm.is_expired() and not confirm.completed.is_set():
                result.append(confirm.to_summary_dict())
        return sorted(result, key=lambda x: x["created_at"])

    def get_confirmation(self, confirm_id: str) -> PendingConfirmation | None:
        """
        Get a specific confirmation by ID.

        Args:
            confirm_id: Confirmation ID.

        Returns:
            PendingConfirmation or None if not found.
        """
        confirm = self._pending.get(confirm_id)
        if confirm and not confirm.is_expired() and not confirm.completed.is_set():
            return confirm
        return None

    async def cleanup_expired(self) -> int:
        """
        Remove expired confirmations.

        Returns:
            Number of confirmations removed.
        """
        async with self._lock:
            expired_ids = [
                cid
                for cid, confirm in self._pending.items()
                if confirm.is_expired() or confirm.completed.is_set()
            ]

            for cid in expired_ids:
                del self._pending[cid]

            if expired_ids:
                logger.debug(f"Cleaned up {len(expired_ids)} expired confirmations")

            return len(expired_ids)

    def __len__(self) -> int:
        """Get number of pending confirmations."""
        return len(self._pending)


# Global singleton instance
_store: PendingConfirmationStore | None = None


def get_pending_store() -> PendingConfirmationStore:
    """Get or create global pending store instance."""
    global _store
    if _store is None:
        _store = PendingConfirmationStore()
    return _store
