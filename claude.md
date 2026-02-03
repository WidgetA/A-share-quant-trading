# A-Share Quantitative Trading System

## Project Overview

A quantitative trading system for China A-share market, designed with modular architecture for strategy development, trading execution, and data management.

## Development Guidelines

### 0. Development Workflow Rules

#### Rule 1: Documentation First
**CRITICAL: Always update documentation BEFORE implementing code changes.**
- Update `README.md` for user-facing changes
- Update `docs/features.md` for feature changes
- Update `CLAUDE.md` for development process changes

#### Rule 2: CI Status Tracking
**CRITICAL: After every `git push`, track CI status until success.**
- Check GitHub Actions workflow status after push
- If CI fails, fix the issue immediately
- Do NOT consider the task complete until CI passes
- Command to check: `gh run list --limit 1` or check GitHub Actions page

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

#### Module Responsibilities

| Module | Responsibility | Hot-Reload |
|--------|---------------|------------|
| **Strategy** | Signal generation, risk rules, position sizing | Yes - strategies can be updated during trading hours |
| **Trading** | Order execution, position management, P&L tracking | Partial - receives strategy updates in real-time |
| **Data/Info** | Market data (iFinD), message reading (PostgreSQL) | No - runs continuously |

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
├── CLAUDE.md                 # This file - AI development guide
├── docs/
│   └── features.md          # Feature specifications (check before dev)
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
| Message Data | PostgreSQL (external, read-only) | Messages streamed by external collector project |
| Historical Data | PostgreSQL + TimescaleDB (optional) | Time-series optimized, for large datasets |
| Task Queue | Celery (optional) | Async task processing |
| Config Format | YAML | Human-readable, supports hot-reload |
| Market Data SDK | THS iFinD | A-share real-time and historical data |
| PostgreSQL Client | asyncpg | Async PostgreSQL access for message reading |

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

### 11. Message Reader Testing Requirements

**Note**: Message collection has been moved to an external project. This project only reads messages from PostgreSQL.

Every message reader component must include the following tests:

| Test Type | Purpose | Example |
|-----------|---------|---------|
| **Connection** | Verify PostgreSQL connection works | `test_connection()` |
| **Query** | Verify message queries return expected format | `test_query_messages()` |
| **Incremental** | Verify incremental queries work correctly | `test_incremental_fetch()` |
| **Error Handling** | Verify graceful handling of connection errors | `test_connection_error()` |

Run message reader tests:

```bash
# Run all reader tests
uv run pytest tests/unit/data/readers/ -v

# Run with mocked database (default in CI)
uv run pytest tests/unit/data/readers/ -v -m "not live"

# Run live tests against real PostgreSQL (local debugging)
uv run pytest tests/unit/data/readers/ -v -m live
```

Test implementation guidelines:
- Use `pytest.mark.asyncio` for async tests
- Use `pytest-mock` to mock database connections for unit tests
- Include at least one "live" test (marked with `@pytest.mark.live`) that connects to real PostgreSQL
- Live tests require `DATABASE_URL` environment variable

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

### 13. Date and Time Handling (Beijing Time)

**Core Principle: All date/time operations MUST use Beijing Time (UTC+8), and MUST be calculated via code execution.**

AI models have unreliable internal date awareness. To ensure accuracy, **always run Python code** to determine current date/time and calculate relative dates.

#### Mandatory Rules

1. **Never trust AI's internal date** - Always execute code to get current time
2. **Always use Beijing timezone** - A-share market operates on Beijing time
3. **Calculate relative dates via code** - "yesterday", "this Tuesday", "last week" must be computed

#### How to Get Current Beijing Time

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Get current Beijing time
beijing_tz = ZoneInfo("Asia/Shanghai")
now = datetime.now(beijing_tz)
print(f"Current Beijing time: {now}")
```

#### Examples of Relative Date Calculation

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

beijing_tz = ZoneInfo("Asia/Shanghai")
today = datetime.now(beijing_tz).date()

# Yesterday
yesterday = today - timedelta(days=1)

# This Tuesday (current week)
days_since_monday = today.weekday()
this_tuesday = today - timedelta(days=days_since_monday) + timedelta(days=1)

# Last Friday
days_since_last_friday = (today.weekday() - 4) % 7
if days_since_last_friday == 0:
    days_since_last_friday = 7
last_friday = today - timedelta(days=days_since_last_friday)

# Start of this month
start_of_month = today.replace(day=1)
```

#### Required Workflow

When user mentions any date/time reference:

1. **Run code first** to determine the actual date
2. **Confirm with user** if the calculated date matches their intent
3. **Use the calculated date** for all subsequent operations

| User Says | Action Required |
|-----------|-----------------|
| "今天" / "today" | Run `datetime.now(beijing_tz).date()` |
| "昨天" / "yesterday" | Run `today - timedelta(days=1)` |
| "本周二" / "this Tuesday" | Calculate from current weekday |
| "上周" / "last week" | Calculate week boundaries |
| "这个月" / "this month" | Get month start/end dates |
| Any specific date | Verify it's a valid trading day |

### 14. Market Data Source Policy

**Core Principle: All trading-related price data MUST use THS iFinD.**

#### Data Source Selection Rules

| Data Type | Required Source | Rationale |
|-----------|----------------|-----------|
| **Real-time quotes** | iFinD | Trading execution requires accurate, low-latency data |
| **Historical OHLCV** | iFinD | Backtesting and position tracking need consistent data |
| **Opening/Closing prices** | iFinD | Order price calculation must be accurate |
| **Limit up/down prices** | iFinD | Risk control requires precise limit prices |
| **News/Announcements** | akshare/other | Non-trading data, accuracy less critical |
| **Sector/Industry mapping** | akshare/other | Reference data, not directly used for trading |

#### Why iFinD for Trading Prices?

1. **Data accuracy** - iFinD is an official data vendor with verified data quality
2. **Consistency** - Using single source avoids price discrepancies
3. **Liability** - Official data source provides audit trail
4. **Real-time capability** - iFinD supports real-time streaming

#### Prohibited Patterns

```python
# FORBIDDEN: Using akshare for trading price data - NO EXCEPTIONS
import akshare as ak
price = ak.stock_zh_a_hist(symbol='600489')  # NEVER USE for any price data!

# CORRECT: Use iFinD for ALL price data
from iFinDPy import THS_HQ, THS_HistoryQuotes
price = THS_HQ('600489.SH', 'open')  # Use iFinD API
```

#### NO EXCEPTIONS - Including Tests

**There are NO exceptions to this rule.** akshare MUST NOT be used for price data in ANY scenario:

- Production environment
- Development environment
- Unit tests
- Integration tests
- Analysis scripts
- Backtesting

**Rationale:** Using different data sources in test vs production creates hidden risks. A strategy that passes tests with akshare data may behave differently with iFinD data in production. This violates the Trading Safety Priority Principle (Section 12).

If iFinD is unavailable, the correct action is to **halt** and fix the data source issue, NOT to fall back to akshare.
