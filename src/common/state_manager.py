# === MODULE PURPOSE ===
# System state persistence and recovery manager.
# Enables the system to recover from interruptions by persisting state to SQLite.

# === DEPENDENCIES ===
# - aiosqlite: Async SQLite operations
# - System modules save checkpoints via this manager

# === KEY CONCEPTS ===
# - SystemState: Overall system running state
# - Checkpoint: Module-specific recovery data
# - Recovery: Restore system to last known good state

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import aiosqlite

logger = logging.getLogger(__name__)


class SystemState(Enum):
    """System running states."""

    STARTING = "starting"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class CheckpointData:
    """
    Module checkpoint for recovery.

    Stores module-specific state that can be used to resume
    operations after a system restart.
    """

    checkpoint_id: str
    module_name: str
    created_at: datetime
    state_data: dict[str, Any]

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "checkpoint_id": self.checkpoint_id,
            "module_name": self.module_name,
            "created_at": self.created_at.isoformat(),
            "state_data": self.state_data,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CheckpointData":
        """Create from dictionary."""
        return cls(
            checkpoint_id=data["checkpoint_id"],
            module_name=data["module_name"],
            created_at=datetime.fromisoformat(data["created_at"]),
            state_data=data["state_data"],
        )


@dataclass
class SystemStateRecord:
    """
    System state record for persistence.

    Captures the overall system state at a point in time.
    """

    state: SystemState
    started_at: datetime
    updated_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "state": self.state.value,
            "started_at": self.started_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SystemStateRecord":
        """Create from dictionary."""
        return cls(
            state=SystemState(data["state"]),
            started_at=datetime.fromisoformat(data["started_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            metadata=data.get("metadata", {}),
        )


class StateManager:
    """
    Manages system state persistence and recovery.

    Provides:
        - System state tracking (starting, running, stopped, etc.)
        - Module checkpoints for granular recovery
        - Automatic recovery detection on startup

    Database Schema:
        - system_state: Overall system state
        - module_checkpoints: Per-module recovery points

    Usage:
        state_manager = StateManager("data/system_state.db")
        await state_manager.connect()

        # Check for recovery on startup
        last_state = await state_manager.get_last_state()
        if last_state and last_state.state == SystemState.RUNNING:
            # System crashed - need recovery
            checkpoints = await state_manager.get_all_checkpoints()
            # ... restore from checkpoints

        # Update state during operation
        await state_manager.save_state(SystemState.RUNNING)

        # Save module checkpoints periodically
        await state_manager.save_checkpoint("message_service", {
            "last_fetch_time": "2026-01-28T10:30:00",
            "processed_count": 1234
        })

    Recovery Flow:
        1. On startup, check get_last_state()
        2. If state was RUNNING, system crashed - recover
        3. Load checkpoints for each module
        4. Resume from checkpoint data
        5. Clear old checkpoints after successful recovery
    """

    # Schema version for migrations
    SCHEMA_VERSION = 1

    def __init__(
        self,
        db_path: str | Path,
        checkpoint_max_age_hours: int = 24,
    ):
        """
        Initialize state manager.

        Args:
            db_path: Path to SQLite database file.
            checkpoint_max_age_hours: Maximum age of checkpoints to consider
                                     for recovery. Older checkpoints are ignored.
        """
        self.db_path = Path(db_path)
        self.checkpoint_max_age = timedelta(hours=checkpoint_max_age_hours)
        self._db: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Connect to database and initialize schema."""
        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row

        await self._init_schema()
        logger.info(f"StateManager connected to {self.db_path}")

    async def close(self) -> None:
        """Close database connection."""
        if self._db:
            await self._db.close()
            self._db = None
            logger.info("StateManager connection closed")

    async def _init_schema(self) -> None:
        """Initialize database schema."""
        if not self._db:
            return

        await self._db.executescript(
            """
            -- System state table
            CREATE TABLE IF NOT EXISTS system_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT NOT NULL,
                started_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                metadata TEXT
            );

            -- Module checkpoints table
            CREATE TABLE IF NOT EXISTS module_checkpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                module_name TEXT UNIQUE NOT NULL,
                checkpoint_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                state_data TEXT NOT NULL
            );

            -- Create index for checkpoint lookup
            CREATE INDEX IF NOT EXISTS idx_checkpoints_module
            ON module_checkpoints(module_name);

            -- Schema version tracking
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY
            );
            """
        )
        await self._db.commit()

    async def save_state(
        self,
        state: SystemState,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Save current system state.

        Args:
            state: Current system state.
            metadata: Optional metadata to store with state.
        """
        if not self._db:
            raise RuntimeError("StateManager not connected")

        async with self._lock:
            now = datetime.now()

            # Check if we have an existing state
            async with self._db.execute(
                "SELECT id, started_at FROM system_state ORDER BY id DESC LIMIT 1"
            ) as cursor:
                row = await cursor.fetchone()

            if row:
                # Update existing state
                await self._db.execute(
                    """
                    UPDATE system_state
                    SET state = ?, updated_at = ?, metadata = ?
                    WHERE id = ?
                    """,
                    (
                        state.value,
                        now.isoformat(),
                        json.dumps(metadata or {}),
                        row["id"],
                    ),
                )
            else:
                # Insert new state record
                await self._db.execute(
                    """
                    INSERT INTO system_state (state, started_at, updated_at, metadata)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        state.value,
                        now.isoformat(),
                        now.isoformat(),
                        json.dumps(metadata or {}),
                    ),
                )

            await self._db.commit()
            logger.debug(f"Saved system state: {state.value}")

    async def get_last_state(self) -> SystemStateRecord | None:
        """
        Get the last saved system state.

        Returns:
            SystemStateRecord if found, None if no state exists.
        """
        if not self._db:
            raise RuntimeError("StateManager not connected")

        async with self._db.execute(
            """
            SELECT state, started_at, updated_at, metadata
            FROM system_state
            ORDER BY id DESC LIMIT 1
            """
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            return None

        return SystemStateRecord(
            state=SystemState(row["state"]),
            started_at=datetime.fromisoformat(row["started_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            metadata=json.loads(row["metadata"] or "{}"),
        )

    async def save_checkpoint(
        self,
        module_name: str,
        state_data: dict[str, Any],
    ) -> str:
        """
        Save a module checkpoint.

        Checkpoints are upserted - each module has only one checkpoint.

        Args:
            module_name: Name of the module saving checkpoint.
            state_data: Module-specific state data for recovery.

        Returns:
            Checkpoint ID.
        """
        if not self._db:
            raise RuntimeError("StateManager not connected")

        async with self._lock:
            now = datetime.now()
            checkpoint_id = f"{module_name}_{now.strftime('%Y%m%d_%H%M%S')}"

            await self._db.execute(
                """
                INSERT INTO module_checkpoints (module_name, checkpoint_id, created_at, state_data)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(module_name) DO UPDATE SET
                    checkpoint_id = excluded.checkpoint_id,
                    created_at = excluded.created_at,
                    state_data = excluded.state_data
                """,
                (
                    module_name,
                    checkpoint_id,
                    now.isoformat(),
                    json.dumps(state_data),
                ),
            )
            await self._db.commit()

            logger.debug(f"Saved checkpoint for {module_name}: {checkpoint_id}")
            return checkpoint_id

    async def get_checkpoint(self, module_name: str) -> CheckpointData | None:
        """
        Get the latest checkpoint for a module.

        Args:
            module_name: Name of the module.

        Returns:
            CheckpointData if found and not expired, None otherwise.
        """
        if not self._db:
            raise RuntimeError("StateManager not connected")

        async with self._db.execute(
            """
            SELECT checkpoint_id, module_name, created_at, state_data
            FROM module_checkpoints
            WHERE module_name = ?
            """,
            (module_name,),
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            return None

        created_at = datetime.fromisoformat(row["created_at"])

        # Check if checkpoint is too old
        if datetime.now() - created_at > self.checkpoint_max_age:
            logger.warning(f"Checkpoint for {module_name} is too old ({created_at}), ignoring")
            return None

        return CheckpointData(
            checkpoint_id=row["checkpoint_id"],
            module_name=row["module_name"],
            created_at=created_at,
            state_data=json.loads(row["state_data"]),
        )

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        """
        Get all valid (non-expired) checkpoints.

        Returns:
            List of CheckpointData for all modules with valid checkpoints.
        """
        if not self._db:
            raise RuntimeError("StateManager not connected")

        cutoff = datetime.now() - self.checkpoint_max_age

        async with self._db.execute(
            """
            SELECT checkpoint_id, module_name, created_at, state_data
            FROM module_checkpoints
            WHERE created_at > ?
            """,
            (cutoff.isoformat(),),
        ) as cursor:
            rows = await cursor.fetchall()

        return [
            CheckpointData(
                checkpoint_id=row["checkpoint_id"],
                module_name=row["module_name"],
                created_at=datetime.fromisoformat(row["created_at"]),
                state_data=json.loads(row["state_data"]),
            )
            for row in rows
        ]

    async def clear_checkpoint(self, module_name: str) -> None:
        """
        Clear checkpoint for a module.

        Call after successful recovery to prevent re-recovery.

        Args:
            module_name: Name of the module.
        """
        if not self._db:
            raise RuntimeError("StateManager not connected")

        async with self._lock:
            await self._db.execute(
                "DELETE FROM module_checkpoints WHERE module_name = ?",
                (module_name,),
            )
            await self._db.commit()
            logger.debug(f"Cleared checkpoint for {module_name}")

    async def clear_all_checkpoints(self) -> None:
        """Clear all module checkpoints."""
        if not self._db:
            raise RuntimeError("StateManager not connected")

        async with self._lock:
            await self._db.execute("DELETE FROM module_checkpoints")
            await self._db.commit()
            logger.info("Cleared all checkpoints")

    async def needs_recovery(self) -> bool:
        """
        Check if the system needs recovery.

        Returns True if the last state was RUNNING, indicating
        the system crashed without proper shutdown.

        Returns:
            True if recovery is needed.
        """
        last_state = await self.get_last_state()

        if last_state is None:
            return False

        # If last state was RUNNING, system crashed
        if last_state.state == SystemState.RUNNING:
            logger.warning(f"System was RUNNING at {last_state.updated_at}, may need recovery")
            return True

        return False

    async def get_recovery_info(self) -> dict[str, Any]:
        """
        Get comprehensive recovery information.

        Returns:
            Dictionary with recovery status and available checkpoints.
        """
        last_state = await self.get_last_state()
        checkpoints = await self.get_all_checkpoints()

        return {
            "needs_recovery": await self.needs_recovery(),
            "last_state": last_state.to_dict() if last_state else None,
            "checkpoints": [cp.to_dict() for cp in checkpoints],
            "checkpoint_count": len(checkpoints),
        }

    async def __aenter__(self) -> "StateManager":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()
