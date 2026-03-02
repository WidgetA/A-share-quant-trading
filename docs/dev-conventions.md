# Development Conventions Reference

> This document contains detailed development conventions extracted from CLAUDE.md.
> For core rules, see [CLAUDE.md](../CLAUDE.md).

## Code Comments (AI-Friendly)

Write comments that help AI quickly understand the codebase:

```python
# === MODULE PURPOSE ===
# This module handles real-time order execution for the trading system.
# It connects to broker APIs and manages order lifecycle.

# === DEPENDENCIES ===
# - strategy_engine: Receives trading signals from strategy module
# - data_service: Gets real-time market data for order validation

# === KEY CONCEPTS ===
# - Order: A buy/sell instruction with price, volume, and timing
# - Position: Current holdings of a specific stock

class OrderExecutor:
    """
    Executes trading orders received from strategy engine.

    Data Flow:
        Strategy Signal -> OrderExecutor -> Broker API -> Order Confirmation

    State Machine:
        PENDING -> SUBMITTED -> PARTIAL_FILLED -> FILLED/CANCELLED

    Hot-Reload Support:
        This class subscribes to strategy updates via message queue.
        Strategy changes take effect immediately without restart.
    """
```

Comment principles:
- Explain **WHY**, not just WHAT
- Document data flow and dependencies between modules
- Describe state machines and lifecycle
- Mark hot-reload boundaries
- Use `# ===` sections for module-level organization

## System Architecture

The system is a **strategy platform** with decoupled modules. Message collection is handled by an **external project** that streams data into PostgreSQL.

```
┌─────────────────────────────────────────────────────────────────┐
│           External Message Collector (Separate Project)         │
│         CLS / East Money / Sina / Akshare → PostgreSQL          │
└─────────────────────────────────────────────────────────────────┘
                              ↓ (streaming)
                    ┌─────────────────────┐
                    │     PostgreSQL      │
                    │   (messages table)  │
                    └─────────────────────┘
                              ↓ (read-only)
┌─────────────────────────────────────────────────────────────────┐
│              A-Share Quant Trading System (This Project)        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   Strategy   │    │   Trading    │    │  Data/Info   │      │
│  │    Module    │◄──►│    Module    │◄──►│    Module    │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│         │                   │                   │               │
│         │            ┌──────┴──────┐           │               │
│         │            │             │           │               │
│         ▼            ▼             ▼           ▼               │
│    [Strategies]  [Live Trade] [Paper Trade] [Market Data]      │
│    - NewsAnalysis                            [MessageReader]    │
│    - (Future...)                             (from PostgreSQL)  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

Communication: Message Queue (Redis/ZeroMQ) for real-time decoupling
```

### Module Responsibilities

| Module | Responsibility | Hot-Reload |
|--------|---------------|------------|
| **Strategy** | Signal generation, risk rules, position sizing | Yes - strategies can be updated during trading hours |
| **Trading** | Order execution, position management, P&L tracking | Partial - receives strategy updates in real-time |
| **Data/Info** | Market data (iFinD), message reading (PostgreSQL) | No - runs continuously |

### Decoupling Requirements

- Modules communicate via **message queue** (not direct function calls)
- Strategy changes must propagate to Trading module **without restart**
- Each module can be deployed and scaled independently
- Use configuration-driven design for runtime parameter changes

## CI/CD Pipeline

```yaml
# .github/workflows/ci.yml structure
Pipeline:
  1. Lint & Format Check (ruff, black)
  2. Type Check (mypy)
  3. Unit Tests (pytest)
  4. Integration Tests (strategy + trading simulation)
  5. Build Docker Images
  6. Deploy to Staging (optional)
```

CI Requirements:
- All PRs must pass CI before merge
- Test coverage threshold: 80%
- No type errors allowed
- Code must be formatted with black

## Project Structure

```
A-share-quant-trading/
├── CLAUDE.md                 # AI development guide (core rules)
├── docs/
│   ├── features.md          # Feature specifications (check before dev)
│   ├── dev-conventions.md   # This file - detailed conventions
│   ├── trading-safety-patterns.md  # Forbidden/correct code patterns
│   └── datetime-timezone-guide.md  # Date/time & asyncpg TZ handling
├── src/
│   ├── strategy/            # Strategy module
│   │   ├── base.py          # Base strategy interface
│   │   ├── engine.py        # Strategy engine with hot-reload
│   │   ├── signals.py       # Signal generation
│   │   ├── strategies/      # Concrete strategy implementations
│   │   ├── analyzers/       # LLM-based analyzers
│   │   └── filters/         # Stock filters
│   ├── trading/             # Trading module
│   │   ├── executor.py      # Order execution
│   │   ├── position_manager.py  # Slot-based position management
│   │   ├── holding_tracker.py   # Overnight holding tracking
│   │   ├── live/            # Live trading implementation
│   │   └── paper/           # Paper trading simulation
│   ├── data/                # Data module
│   │   ├── models/          # Data models (message.py, etc.)
│   │   ├── readers/         # Data readers (PostgreSQL message reader)
│   │   │   └── message_reader.py  # Read messages from external PostgreSQL
│   │   └── sources/         # iFinD data sources (market data only)
│   │       └── ifind_limit_up.py  # Limit-up stocks via iFinD
│   └── common/              # Shared utilities
│       ├── coordinator.py   # Event-driven module coordination
│       ├── scheduler.py     # Trading session scheduler
│       ├── state_manager.py # State persistence and recovery
│       ├── llm_service.py   # LLM integration (Silicon Flow)
│       ├── user_interaction.py  # Command-line user interaction
│       └── config.py        # Configuration management
├── tests/
│   ├── unit/
│   └── integration/
├── config/
│   ├── main-config.yaml     # System configuration
│   ├── database-config.yaml # PostgreSQL connection config
│   └── news-strategy-config.yaml  # Strategy parameters
├── scripts/
│   └── main.py              # Main entry point
└── .github/
    └── workflows/
        └── ci.yml
```

## Development Checklist

Before starting any development task:

- [ ] Read `docs/features.md` to understand current feature specifications
- [ ] Confirm the feature/change aligns with documented requirements
- [ ] If requirements are unclear, update `docs/features.md` first
- [ ] Check which module(s) will be affected
- [ ] Ensure changes maintain module decoupling
- [ ] Write AI-friendly comments for new code
- [ ] Add/update tests
- [ ] Verify CI passes

## Key Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Language | Python 3.11+ | Ecosystem, quant libraries |
| Package Manager | uv | Fast, reliable, replaces pip/venv/pip-tools |
| Message Queue | Redis Pub/Sub or ZeroMQ | Low latency, simple setup |
| Trading Data | PostgreSQL (trading schema) | Unified with messages DB |
| Message Data | PostgreSQL (external, read-only) | Messages streamed by external collector |
| Historical Data | PostgreSQL + TimescaleDB (optional) | Time-series optimized |
| Config Format | YAML | Human-readable, supports hot-reload |
| Market Data SDK | THS iFinD | A-share real-time and historical data |
| PostgreSQL Client | asyncpg | Async PostgreSQL access |

## Environment Management (uv)

```bash
# Install uv
# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Quick start
uv venv && uv sync

# Common commands
uv add <package>          # Add dependency
uv add --dev <package>    # Add dev dependency
uv remove <package>       # Remove dependency
uv lock --upgrade         # Update all
uv sync                   # Sync with lock file
uv run python script.py   # Run without activating venv
uv run pytest             # Run tests
```

## Glossary

| Term | Definition |
|------|-----------|
| **Signal** | Trading recommendation from strategy (BUY/SELL/HOLD) |
| **Position** | Current stock holdings with quantity and cost basis |
| **Order** | Instruction to buy/sell at specific price/quantity |
| **Paper Trading** | Simulated trading without real money |
| **Live Trading** | Real trading with actual broker connection |
| **Hot-Reload** | Ability to update code/config without system restart |

## Message Reader Testing

Message collection is in an external project. This project only reads from PostgreSQL.

Required test types per reader component:

| Test Type | Purpose |
|-----------|---------|
| **Connection** | Verify PostgreSQL connection works |
| **Query** | Verify message queries return expected format |
| **Incremental** | Verify incremental queries work correctly |
| **Error Handling** | Verify graceful handling of connection errors |

```bash
uv run pytest tests/unit/data/readers/ -v              # All reader tests
uv run pytest tests/unit/data/readers/ -v -m "not live" # Mocked DB (CI)
uv run pytest tests/unit/data/readers/ -v -m live       # Live tests (needs DATABASE_URL)
```

Guidelines:
- Use `pytest.mark.asyncio` for async tests
- Use `pytest-mock` to mock database connections
- Include at least one `@pytest.mark.live` test connecting to real PostgreSQL
