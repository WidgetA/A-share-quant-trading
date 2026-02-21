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
| Trading Data | PostgreSQL (trading schema) | Unified with messages DB, positions/orders/transactions/state |
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

**Core Principle: Follow the `data_source` toggle — use whichever source the user selects.**

The UI provides a `data_source` radio toggle (`"ifind"` or `"akshare"`). All code paths (strategies, filters, backtests) MUST respect this toggle and use the selected data source consistently for ALL data types (price, volume, turnover, etc.).

#### Data Source Toggle

| Toggle Value | Price/Volume Source | Adapter |
|-------------|---------------------|---------|
| `"ifind"` | THS iFinD HTTP API | `IFinDHttpClient` |
| `"akshare"` | akshare + baostock (pre-downloaded cache) | `AkshareHistoricalAdapter` |

#### Rules

1. **One source per session** — when the toggle says akshare, ALL data (OHLCV, turnover, etc.) comes from akshare. No mixing sources within a single backtest/scan.
2. **Adapter must support all indicators** — `AkshareHistoricalAdapter` must return the same indicator set as `IFinDHttpClient` (including `turnoverRatio`). If akshare provides the data, store and serve it.
3. **Live trading uses iFinD** — the toggle only applies to backtesting. Real-time live trading always uses iFinD for accuracy and latency.
4. **Non-trading data** (news, sector mapping, fundamentals) uses its own dedicated source regardless of toggle (PostgreSQL for fundamentals, local JSON for boards).

### 15. Volume Unit Convention (Critical)

**Core Principle: ALL volume data in the system MUST be in 股 (shares), never in 手 (lots).**

Different data sources use different volume units:

| Data Source | Function | Native Unit | Conversion |
|------------|----------|-------------|------------|
| **akshare** | `stock_zh_a_hist()` | **手** (1手=100股) | ×100 at storage time |
| **baostock** | `query_history_k_data_plus()` | **股** | None needed |
| **iFinD** | `history_quotes` / `high_frequency` | **股** | None needed |

#### Why This Matters

A 100x unit mismatch between daily volume (手) and minute volume (股) caused:
- `early_volume / avg_daily_volume` ratios to be 100x too large
- Reversal filter's surge_volume_ratio to be 100x too small (never triggered)
- Step 6 turnover_amp display values to be inflated (ranking unaffected due to Z-score)

#### Implementation

The conversion is done at the **storage layer** in `akshare_backtest_cache.py`:

```python
# akshare 成交量 is in 手 (lots of 100 shares);
# baostock minute volume is in 股 (shares).
# Convert to 股 here so all volume fields are consistent.
"volume": float(row["成交量"]) * 100,
```

All downstream code (quality filter, reversal filter, Step 6 scoring) receives volume in 股 via pass-through adapters. No conversion needed anywhere else.

#### Rules

1. **Never store volume in 手** — convert to 股 at the data ingestion layer
2. **When adding a new data source**, check its volume unit documentation before integrating
3. **Cross-verify**: For any stock, `early_volume (10min) / avg_daily_volume` should be ~0.05-0.30 (5-30% of daily in first 10 minutes). If >1.0, units are likely mismatched.

### 16. asyncpg Timezone Handling (Critical)

**Core Principle: ALWAYS use timezone-aware datetimes when passing parameters to asyncpg queries.**

#### Root Cause

asyncpg has **asymmetric timezone behavior** that causes silent data corruption:

| Direction | Behavior |
|-----------|----------|
| **Sending** (query parameters) | Naive datetimes are interpreted using the **system timezone** (`TZ` env var) |
| **Receiving** (query results) | Always returns **UTC-aware** datetimes (`tzinfo=UTC`) for `timestamptz` columns |

When the Docker container has `TZ=Asia/Shanghai`, this asymmetry causes a **16-hour offset** for naive datetimes:

```
Naive 01:30 → asyncpg treats as 01:30 CST → converts to 17:30 UTC (previous day!)
Expected: 01:30 UTC
Actual:   17:30 UTC (WRONG by 16 hours)
```

#### Prohibited Patterns

```python
# FORBIDDEN: Subtracting hours from naive datetime for "manual UTC conversion"
start_bj = datetime(2026, 2, 8, 9, 30)  # 9:30 Beijing
start_utc = start_bj - timedelta(hours=8)  # 01:30 naive — WRONG!
# asyncpg will interpret 01:30 as 01:30 CST → 17:30 UTC

# FORBIDDEN: Any naive datetime as asyncpg query parameter
await conn.fetch("SELECT * FROM t WHERE ts >= $1", naive_datetime)
```

#### Correct Patterns

```python
from zoneinfo import ZoneInfo

beijing_tz = ZoneInfo("Asia/Shanghai")

# CORRECT: Use timezone-aware datetime for query parameters
start_bj = datetime(2026, 2, 8, 9, 30)
start_aware = start_bj.replace(tzinfo=beijing_tz)  # 09:30+08:00
# asyncpg correctly converts to 01:30 UTC

await conn.fetch("SELECT * FROM t WHERE ts >= $1", start_aware)

# CORRECT: Display conversion (UTC result → Beijing time for display)
utc_time = row["publish_time"]  # e.g., 2026-02-07 22:58:00+00:00
beijing_display = (utc_time + timedelta(hours=8)).replace(tzinfo=None)
# Result: 2026-02-08 06:58:00 (clean Beijing time string)
```

#### Summary of Rules

1. **Query parameters**: Always use `datetime.replace(tzinfo=beijing_tz)` — NEVER subtract 8 hours from naive datetimes
2. **Display results**: Always use `+ timedelta(hours=8)` then `.replace(tzinfo=None)` for clean Beijing time
3. **Never assume system TZ**: Code must work correctly regardless of `TZ` environment variable
4. **Test with TZ=Asia/Shanghai**: Always verify datetime handling in an environment matching production

### 17. iFinD HTTP API Reference (Critical)

**Core Principle: Before writing ANY iFinD API call, ALWAYS consult the API manual first.**

The official API manual is stored in the project:
- **PDF**: [docs/iFinD-HTTP-API-manual.pdf](docs/iFinD-HTTP-API-manual.pdf)
- **Text extract**: [docs/iFinD-HTTP-API-manual.txt](docs/iFinD-HTTP-API-manual.txt) (machine-readable, use for quick lookup)

#### Why This Rule Exists

AI models frequently hallucinate API parameter names and indicator names. Past bugs include:
- Using `vol` instead of `volume` (history_quotes indicator)
- Using `name` as a `real_time_quotation` indicator (does not exist)
- Confusing indicators between different endpoints

#### Mandatory Workflow

When writing or modifying any iFinD HTTP API call:
1. **Read the manual text file** (`docs/iFinD-HTTP-API-manual.txt`) to find the correct endpoint section
2. **Verify the endpoint URL** matches the operation
3. **Verify ALL parameter names** (keys in formData) are correct
4. **Verify ALL indicator names** are valid for that specific endpoint
5. **Do NOT guess indicator names** — different endpoints have different indicator sets

#### Quick Reference: Endpoints

| Function | Endpoint | Key Parameters |
|----------|----------|----------------|
| Basic Data | `/api/v1/basic_data_service` | codes, indipara |
| Date Sequence | `/api/v1/date_sequence` | codes, indipara, startdate, enddate, functionpara |
| Historical Quotes | `/api/v1/cmd_history_quotation` | codes, indicators, startdate, enddate, functionpara |
| High Frequency | `/api/v1/high_frequency` | codes, indicators, starttime, endtime, functionpara |
| Real-time Quotes | `/api/v1/real_time_quotation` | codes, indicators, functionpara |
| Snapshot | `/api/v1/snap_shot` | codes, indicators, starttime, endtime |
| Smart Stock Picking | `/api/v1/smart_stock_picking` | searchstring, searchtype |
| Trade Dates | `/api/v1/get_trade_dates` | marketcode, functionpara, startdate, enddate |
| Token | `/api/v1/get_access_token` | (refresh_token in header) |
| Report Query | `/api/v1/report_query` | codes, functionpara, outputpara |

#### Common Indicator Names (verify against manual before use!)

**cmd_history_quotation**: `preClose`, `open`, `high`, `low`, `close`, `avgPrice`, `change`, `changeRatio`, `volume`, `amount`, `turnoverRatio`, `transactionAmount`, `totalShares`, `totalCapital`, `pe_ttm`, `pe`, `pb`, `ps`, `pcf`

**real_time_quotation**: `tradeDate`, `tradeTime`, `preClose`, `open`, `high`, `low`, `latest`, `latestAmount`, `latestVolume`, `avgPrice`, `change`, `changeRatio`, `upperLimit`, `downLimit`, `amount`, `volume`, `turnoverRatio`, `pe_ttm`, `mv`, `vol_ratio`, `swing`

**Important differences**:
- History uses `close` for current close; real-time uses `latest` for current price
- History uses `volume`/`amount`; snapshot uses `vol`/`amt` (single tick) AND `volume`/`amount` (cumulative)
- `name` is NOT a valid indicator for any quote endpoint — stock names come from `smart_stock_picking` responses
