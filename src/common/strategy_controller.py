# === MODULE PURPOSE ===
# Strategy execution state controller for Web UI start/stop functionality.
# Provides thread-safe control over strategy loop execution.

# === DEPENDENCIES ===
# - Used by SystemManager to check if strategy should run
# - Used by Web UI to display and control strategy state

import asyncio
import logging
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class StrategyState(Enum):
    """Strategy running state."""

    RUNNING = "running"  # Strategy loop is active
    STOPPED = "stopped"  # Strategy loop is paused


class StrategyController:
    """
    Controls strategy execution state.

    Provides thread-safe start/stop control that can be accessed
    from Web UI without affecting the rest of the system.

    Data Flow:
        Web UI -> StrategyController -> SystemManager._strategy_loop()

    Usage:
        controller = StrategyController()
        await controller.start()  # Enable strategy loop
        await controller.stop()   # Disable strategy loop

        # In strategy loop:
        if controller.is_running:
            # Execute strategy logic
    """

    def __init__(self, initial_state: StrategyState = StrategyState.STOPPED):
        """
        Initialize controller.

        Args:
            initial_state: Starting state. Default is STOPPED for safety.
        """
        self._state: StrategyState = initial_state
        self._lock = asyncio.Lock()
        self._state_changed_at: datetime = datetime.now()
        self._start_count: int = 0
        self._stop_count: int = 0

    @property
    def state(self) -> StrategyState:
        """Current strategy state."""
        return self._state

    @property
    def is_running(self) -> bool:
        """Whether strategy loop should execute."""
        return self._state == StrategyState.RUNNING

    @property
    def is_stopped(self) -> bool:
        """Whether strategy loop is stopped."""
        return self._state == StrategyState.STOPPED

    async def start(self) -> bool:
        """
        Start strategy execution.

        Returns:
            True if state changed, False if already running.
        """
        async with self._lock:
            if self._state == StrategyState.STOPPED:
                self._state = StrategyState.RUNNING
                self._state_changed_at = datetime.now()
                self._start_count += 1
                logger.info("Strategy started")
                return True
            logger.warning("Strategy already running")
            return False

    async def stop(self) -> bool:
        """
        Stop strategy execution.

        Returns:
            True if state changed, False if already stopped.
        """
        async with self._lock:
            if self._state == StrategyState.RUNNING:
                self._state = StrategyState.STOPPED
                self._state_changed_at = datetime.now()
                self._stop_count += 1
                logger.info("Strategy stopped")
                return True
            logger.warning("Strategy already stopped")
            return False

    def to_dict(self) -> dict:
        """
        Convert to dictionary for API response.

        Returns:
            Dictionary with state information.
        """
        return {
            "state": self._state.value,
            "is_running": self.is_running,
            "state_changed_at": self._state_changed_at.isoformat(),
            "start_count": self._start_count,
            "stop_count": self._stop_count,
        }

    def __repr__(self) -> str:
        return f"StrategyController(state={self._state.value})"
