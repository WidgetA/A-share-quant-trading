# === MODULE PURPOSE ===
# Historical simulation trading system.
# Allows replaying past trading days with historical data.

# === EXPORTS ===
from src.simulation.clock import SimulationClock
from src.simulation.context import SimulationContext
from src.simulation.historical_message_reader import HistoricalMessageReader
from src.simulation.historical_price_service import HistoricalPriceService
from src.simulation.manager import (
    SimulationManager,
    SimulationSettings,
    get_simulation_manager,
    reset_simulation_manager,
)
from src.simulation.models import (
    PendingSignal,
    SimulatedTransaction,
    SimulationConfig,
    SimulationHolding,
    SimulationPhase,
    SimulationResult,
    SimulationState,
)
from src.simulation.position_manager import SimulationPositionManager

__all__ = [
    # Models
    "SimulationPhase",
    "SimulatedTransaction",
    "SimulationHolding",
    "PendingSignal",
    "SimulationState",
    "SimulationResult",
    "SimulationConfig",
    # Components
    "SimulationClock",
    "SimulationContext",
    "HistoricalMessageReader",
    "HistoricalPriceService",
    "SimulationPositionManager",
    # Manager
    "SimulationManager",
    "SimulationSettings",
    "get_simulation_manager",
    "reset_simulation_manager",
]
