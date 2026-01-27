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

### 11. Data Source Testing Requirements

**MANDATORY: Every new data source MUST have accompanying tests. Do not merge/commit a data source without its test file.**

Every data source must include the following tests to detect when external APIs become unavailable:

| Test Type | Purpose | Example |
|-----------|---------|---------|
| **Connectivity** | Verify API is accessible | `test_connectivity()` |
| **Data Format** | Verify response structure matches expectations | `test_message_format()` |
| **Deduplication** | Verify duplicate messages are filtered | `test_deduplication()` |
| **Error Handling** | Verify graceful handling of network errors | `test_network_error()` |

Run data source tests:

```bash
# Run all data source tests
uv run pytest tests/unit/data/sources/ -v

# Run specific source test
uv run pytest tests/unit/data/sources/test_cls_news.py -v
```

Test implementation guidelines:
- **New data source = New test file** (e.g., `xxx_news.py` → `test_xxx_news.py`)
- Use `pytest.mark.asyncio` for async tests
- Use `pytest-mock` to mock external API calls for unit tests
- Include at least one "live" test (marked with `@pytest.mark.live`) that actually hits the API
- Live tests should be skipped in CI but run locally for debugging

Test file naming convention:
```
src/data/sources/xxx_news.py      → tests/unit/data/sources/test_xxx_news.py
src/data/sources/yyy_source.py    → tests/unit/data/sources/test_yyy_source.py
```

### 12. Trading Safety Priority Principle

**Core Principle: Trading Safety > Program Robustness**

When facing design decisions where:
- Option A: May cause program crash
- Option B: May cause financial loss

**ALWAYS choose Option A (let it crash)**

#### Guidelines

1. **Stop rather than trade incorrectly**
   - If order status is uncertain, halt trading instead of guessing
   - If data is incomplete, skip the trading decision

2. **Miss opportunities rather than take risks**
   - Do not execute trades with stale or cached data
   - Do not assume market conditions when data fetch fails

3. **Communicate immediately**
   - Any design decision that may affect fund safety MUST be discussed with user first
   - Do not silently implement degraded fallback for trading-related code

4. **Fail fast**
   - Terminate immediately on anomalies instead of attempting recovery
   - Prefer explicit errors over silent degradation

5. **Audit first**
   - All trading operations must have complete logs
   - Never skip logging to "improve performance"

#### Prohibited Degradation Patterns

These patterns are **FORBIDDEN** in trading-related code:

```python
# FORBIDDEN: Using cache when real-time data fails
def get_price(stock_code):
    try:
        return fetch_realtime_price(stock_code)
    except NetworkError:
        return self.cache.get(stock_code)  # DANGEROUS!

# FORBIDDEN: Assuming order status on timeout
def check_order_status(order_id):
    try:
        return broker.get_status(order_id)
    except Timeout:
        return "FILLED"  # DANGEROUS ASSUMPTION!

# FORBIDDEN: Auto-retry that may cause duplicate orders
def place_order(order):
    for _ in range(3):  # DANGEROUS RETRY!
        try:
            return broker.submit(order)
        except NetworkError:
            continue
```

#### Correct Patterns

```python
# CORRECT: Fail explicitly when data unavailable
def get_price(stock_code):
    try:
        return fetch_realtime_price(stock_code)
    except NetworkError as e:
        logger.error(f"Failed to fetch price for {stock_code}: {e}")
        raise TradingHaltError("Cannot proceed without real-time data")

# CORRECT: Mark status as unknown and halt
def check_order_status(order_id):
    try:
        return broker.get_status(order_id)
    except Timeout:
        logger.critical(f"Order {order_id} status unknown - HALTING")
        raise OrderStatusUnknownError(order_id)

# CORRECT: No retry for order submission
def place_order(order):
    try:
        return broker.submit(order)
    except NetworkError as e:
        logger.error(f"Order submission failed: {e}")
        raise OrderSubmissionError("Manual intervention required")
```
