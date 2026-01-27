# A-Share Quantitative Trading System

## Project Overview

A quantitative trading system for China A-share market, designed with modular architecture for strategy development, trading execution, and data management.

## Development Guidelines

### 1. File Naming Convention

- All file names must be in **English**
- Use snake_case for Python files: `strategy_manager.py`
- Use kebab-case for config files: `trading-config.yaml`
- Use PascalCase for class files if needed: `BaseStrategy.py`

### 2. Feature Documentation Workflow

- Feature specifications are maintained in [docs/features.md](docs/features.md)
- **Before any development**: Always check `docs/features.md` to ensure alignment with requirements
- **For new features**:
  1. Update `docs/features.md` first
  2. Get confirmation on the feature spec
  3. Then implement the code
- **For bug fixes**: Update feature doc if the fix changes expected behavior

### 3. Code Comments (AI-Friendly)

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

### 4. System Architecture

The system is divided into three **decoupled** modules:

```
┌─────────────────────────────────────────────────────────────────┐
│                    A-Share Quant Trading System                 │
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
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

Communication: Message Queue (Redis/ZeroMQ) for real-time decoupling
```

#### Module Responsibilities

| Module | Responsibility | Hot-Reload |
|--------|---------------|------------|
| **Strategy** | Signal generation, risk rules, position sizing | Yes - strategies can be updated during trading hours |
| **Trading** | Order execution, position management, P&L tracking | Partial - receives strategy updates in real-time |
| **Data/Info** | Market data, fundamentals, news, historical data | No - runs continuously |

#### Decoupling Requirements

- Modules communicate via **message queue** (not direct function calls)
- Strategy changes must propagate to Trading module **without restart**
- Each module can be deployed and scaled independently
- Use configuration-driven design for runtime parameter changes

### 5. CI/CD Pipeline

Maintain a CI pipeline with the following stages:

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

### 6. Project Structure

```
A-share-quant-trading/
├── claude.md                 # This file - AI development guide
├── docs/
│   └── features.md          # Feature specifications (check before dev)
├── src/
│   ├── strategy/            # Strategy module
│   │   ├── base.py          # Base strategy interface
│   │   ├── signals.py       # Signal generation
│   │   └── strategies/      # Concrete strategy implementations
│   ├── trading/             # Trading module
│   │   ├── executor.py      # Order execution
│   │   ├── live/            # Live trading implementation
│   │   └── paper/           # Paper trading simulation
│   ├── data/                # Data module
│   │   ├── market.py        # Real-time market data
│   │   ├── historical.py    # Historical data
│   │   └── sources/         # Data source adapters
│   └── common/              # Shared utilities
│       ├── messaging.py     # Message queue abstraction
│       ├── config.py        # Configuration management
│       └── models.py        # Shared data models
├── tests/
│   ├── unit/
│   └── integration/
├── config/
│   └── trading-config.yaml
├── scripts/
│   └── start_system.py
└── .github/
    └── workflows/
        └── ci.yml
```

### 7. Development Checklist

Before starting any development task:

- [ ] Read `docs/features.md` to understand current feature specifications
- [ ] Confirm the feature/change aligns with documented requirements
- [ ] If requirements are unclear, update `docs/features.md` first
- [ ] Check which module(s) will be affected
- [ ] Ensure changes maintain module decoupling
- [ ] Write AI-friendly comments for new code
- [ ] Add/update tests
- [ ] Verify CI passes

### 8. Key Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Language | Python 3.11+ | Ecosystem, quant libraries |
| Package Manager | uv | Fast, reliable, replaces pip/venv/pip-tools |
| Message Queue | Redis Pub/Sub or ZeroMQ | Low latency, simple setup |
| Trading Data | SQLite | Zero-config, positions/orders/transactions |
| Historical Data | PostgreSQL + TimescaleDB (optional) | Time-series optimized, for large datasets |
| Task Queue | Celery (optional) | Async task processing |
| Config Format | YAML | Human-readable, supports hot-reload |
| Market Data SDK | THS iFinD | A-share real-time and historical data |

### 9. Environment Management (uv)

This project uses **uv** for Python environment and package management.

#### Quick Start

```bash
# Install uv (if not installed)
# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv
uv sync
```

#### Common Commands

```bash
# Activate virtual environment
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

# Add a new dependency
uv add <package>

# Add a dev dependency
uv add --dev <package>

# Remove a dependency
uv remove <package>

# Update all dependencies
uv lock --upgrade

# Sync environment with lock file
uv sync

# Run a command in the virtual environment (without activating)
uv run python script.py
uv run pytest
```

#### Why uv?

- **10-100x faster** than pip for dependency resolution and installation
- **Built-in virtual environment** management (replaces venv)
- **Lock file support** (`uv.lock`) for reproducible builds
- **Drop-in replacement** for pip commands

### 10. Glossary

- **Signal**: Trading recommendation from strategy (BUY/SELL/HOLD)
- **Position**: Current stock holdings with quantity and cost basis
- **Order**: Instruction to buy/sell at specific price/quantity
- **Paper Trading**: Simulated trading without real money
- **Live Trading**: Real trading with actual broker connection
- **Hot-Reload**: Ability to update code/config without system restart
