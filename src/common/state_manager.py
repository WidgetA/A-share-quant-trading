# === MODULE PURPOSE ===
# System state persistence and recovery manager.
# Enables the system to recover from interruptions by persisting state to PostgreSQL.

# === DEPENDENCIES ===
# - asyncpg: Async PostgreSQL operations
# - System modules save checkpoints via this manager

# === KEY CONCEPTS ===
# - SystemState: Overall system running state
# - Checkpoint: Module-specific recovery data
# - Recovery: Restore system to last known good state

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import asyncpg

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


@dataclass
class StateManagerConfig:
    """Configuration for PostgreSQL state manager."""

    host: str = "localhost"
    port: int = 5432
    database: str = "messages"
    user: str = "reader"
    password: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 5
    schema: str = "trading"
    checkpoint_max_age_hours: int = 24


# SQL for schema and table creation
SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS {schema};
"""

TABLES_SQL = """
-- System state table
CREATE TABLE IF NOT EXISTS {schema}.system_state (
    id SERIAL PRIMARY KEY,
    state VARCHAR(20) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    metadata JSONB DEFAULT '{{}}'
);

-- Module checkpoints table
CREATE TABLE IF NOT EXISTS {schema}.module_checkpoints (
    id SERIAL PRIMARY KEY,
    module_name VARCHAR(100) UNIQUE NOT NULL,
    checkpoint_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    state_data JSONB NOT NULL
);

-- Create index for checkpoint lookup
CREATE INDEX IF NOT EXISTS idx_checkpoints_module
ON {schema}.module_checkpoints(module_name);
"""


class StateManager:
    """
    Manages system state persistence and recovery using PostgreSQL.

    Provides:
        - System state tracking (starting, running, stopped, etc.)
        - Module checkpoints for granular recovery
        - Automatic recovery detection on startup

    Database Schema:
        - system_state: Overall system state
        - module_checkpoints: Per-module recovery points

    Usage:
        config = StateManagerConfig(host="localhost", ...)
        state_manager = StateManager(config)
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

    def __init__(self, config: StateManagerConfig):
        """
        Initialize state manager.

        Args:
            config: PostgreSQL connection configuration.
        """
        self._config = config
        self._pool: asyncpg.Pool | None = None
        self._is_connected = False
        self._schema = config.schema
        self.checkpoint_max_age = timedelta(hours=config.checkpoint_max_age_hours)
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Connect to database and initialize schema."""
        if self._is_connected:
            return

        try:
            logger.info(
                f"Connecting StateManager to PostgreSQL: {self._config.host}:{self._config.port}"
                f"/{self._config.database} (schema: {self._schema})"
            )

            self._pool = await asyncpg.create_pool(
                host=self._config.host,
                port=self._config.port,
                database=self._config.database,
                user=self._config.user,
                password=self._config.password,
                min_size=self._config.pool_min_size,
                max_size=self._config.pool_max_size,
            )

            # Initialize schema and tables
            await self._init_schema()

            self._is_connected = True
            logger.info("StateManager connected to PostgreSQL")

        except Exception as e:
            logger.error(f"Failed to connect StateManager to PostgreSQL: {e}")
            raise ConnectionError(f"Cannot connect to state database: {e}") from e

    async def close(self) -> None:
        """Close database connection."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            self._is_connected = False
            logger.info("StateManager connection closed")

    async def _init_schema(self) -> None:
        """Initialize database schema."""
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            # Create schema
            await conn.execute(SCHEMA_SQL.format(schema=self._schema))
            # Create tables
            await conn.execute(TABLES_SQL.format(schema=self._schema))
            logger.info(f"Initialized state management schema: {self._schema}")

    def _ensure_connected(self) -> None:
        """Ensure manager is connected."""
        if not self._is_connected or not self._pool:
            raise RuntimeError("StateManager is not connected. Call connect() first.")

    @property
    def _db_pool(self) -> asyncpg.Pool:
        """Get the database pool, raising if not connected."""
        self._ensure_connected()
        assert self._pool is not None
        return self._pool

    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._is_connected

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
        async with self._lock:
            now = datetime.now()

            async with self._db_pool.acquire() as conn:
                # Check if we have an existing state
                row = await conn.fetchrow(
                    f"SELECT id, started_at FROM {self._schema}.system_state "
                    "ORDER BY id DESC LIMIT 1"
                )

                if row:
                    # Update existing state
                    await conn.execute(
                        f"""
                        UPDATE {self._schema}.system_state
                        SET state = $1, updated_at = $2, metadata = $3
                        WHERE id = $4
                        """,
                        state.value,
                        now,
                        json.dumps(metadata or {}),
                        row["id"],
                    )
                else:
                    # Insert new state record
                    await conn.execute(
                        f"""
                        INSERT INTO {self._schema}.system_state
                            (state, started_at, updated_at, metadata)
                        VALUES ($1, $2, $3, $4)
                        """,
                        state.value,
                        now,
                        now,
                        json.dumps(metadata or {}),
                    )

            logger.debug(f"Saved system state: {state.value}")

    async def get_last_state(self) -> SystemStateRecord | None:
        """
        Get the last saved system state.

        Returns:
            SystemStateRecord if found, None if no state exists.
        """
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT state, started_at, updated_at, metadata
                FROM {self._schema}.system_state
                ORDER BY id DESC LIMIT 1
                """
            )

        if not row:
            return None

        metadata = row["metadata"]
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        return SystemStateRecord(
            state=SystemState(row["state"]),
            started_at=row["started_at"],
            updated_at=row["updated_at"],
            metadata=metadata or {},
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
        async with self._lock:
            now = datetime.now()
            checkpoint_id = f"{module_name}_{now.strftime('%Y%m%d_%H%M%S')}"

            async with self._db_pool.acquire() as conn:
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema}.module_checkpoints
                        (module_name, checkpoint_id, created_at, state_data)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (module_name) DO UPDATE SET
                        checkpoint_id = EXCLUDED.checkpoint_id,
                        created_at = EXCLUDED.created_at,
                        state_data = EXCLUDED.state_data
                    """,
                    module_name,
                    checkpoint_id,
                    now,
                    json.dumps(state_data),
                )

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
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT checkpoint_id, module_name, created_at, state_data
                FROM {self._schema}.module_checkpoints
                WHERE module_name = $1
                """,
                module_name,
            )

        if not row:
            return None

        created_at = row["created_at"]

        # Check if checkpoint is too old
        if datetime.now() - created_at > self.checkpoint_max_age:
            logger.warning(f"Checkpoint for {module_name} is too old ({created_at}), ignoring")
            return None

        state_data = row["state_data"]
        if isinstance(state_data, str):
            state_data = json.loads(state_data)

        return CheckpointData(
            checkpoint_id=row["checkpoint_id"],
            module_name=row["module_name"],
            created_at=created_at,
            state_data=state_data,
        )

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        """
        Get all valid (non-expired) checkpoints.

        Returns:
            List of CheckpointData for all modules with valid checkpoints.
        """
        cutoff = datetime.now() - self.checkpoint_max_age

        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT checkpoint_id, module_name, created_at, state_data
                FROM {self._schema}.module_checkpoints
                WHERE created_at > $1
                """,
                cutoff,
            )

        result = []
        for row in rows:
            state_data = row["state_data"]
            if isinstance(state_data, str):
                state_data = json.loads(state_data)

            result.append(
                CheckpointData(
                    checkpoint_id=row["checkpoint_id"],
                    module_name=row["module_name"],
                    created_at=row["created_at"],
                    state_data=state_data,
                )
            )

        return result

    async def clear_checkpoint(self, module_name: str) -> None:
        """
        Clear checkpoint for a module.

        Call after successful recovery to prevent re-recovery.

        Args:
            module_name: Name of the module.
        """
        async with self._lock:
            async with self._db_pool.acquire() as conn:
                await conn.execute(
                    f"DELETE FROM {self._schema}.module_checkpoints WHERE module_name = $1",
                    module_name,
                )
            logger.debug(f"Cleared checkpoint for {module_name}")

    async def clear_all_checkpoints(self) -> None:
        """Clear all module checkpoints."""
        async with self._lock:
            async with self._db_pool.acquire() as conn:
                await conn.execute(f"DELETE FROM {self._schema}.module_checkpoints")
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


def create_state_manager_from_config() -> StateManager:
    """
    Create StateManager from configuration file.

    Returns:
        Configured StateManager instance.
    """
    from src.common.config import load_config

    config = load_config("config/database-config.yaml")
    db_config = config.get_dict("database.trading", {})

    if not db_config:
        raise ValueError("Trading database configuration not found")

    # Support environment variable substitution
    def resolve_env(value: Any) -> Any:
        if isinstance(value, str) and value.startswith("${"):
            # Parse ${VAR:default} or ${VAR}
            inner = value[2:-1]
            if ":" in inner:
                var_name, default = inner.split(":", 1)
            else:
                var_name, default = inner, ""
            return os.environ.get(var_name, default)
        return value

    state_config = StateManagerConfig(
        host=resolve_env(db_config.get("host", "localhost")),
        port=int(resolve_env(db_config.get("port", 5432))),
        database=resolve_env(db_config.get("database", "messages")),
        user=resolve_env(db_config.get("user", "reader")),
        password=resolve_env(db_config.get("password", "")),
        pool_min_size=db_config.get("pool_min_size", 2),
        pool_max_size=db_config.get("pool_max_size", 5),
        schema=db_config.get("schema", "trading"),
        checkpoint_max_age_hours=db_config.get("checkpoint_max_age_hours", 24),
    )

    return StateManager(state_config)
