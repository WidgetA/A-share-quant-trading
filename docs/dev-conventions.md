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
| **Data/Info** | Market data (Tushare/tsanghi), boards/stock names (local JSON), backtest cache (GreptimeDB) | No - runs continuously |

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
│   │   ├── models.py        # Shared data models (PriceSnapshot, etc.)
│   │   ├── base.py          # Base strategy interface
│   │   ├── signals.py       # Signal types
│   │   ├── signal_store.py  # In-memory signal queue (push/poll/ack/expire)
│   │   ├── momentum_strategy_service.py  # Stateless momentum scan (backtest + live)
│   │   ├── strategies/
│   │   │   └── momentum_scanner.py  # Momentum 7-layer funnel + V3 scoring
│   │   ├── aggregators/     # Business-defined minute aggregation (injected into pipeline)
│   │   │   └── early_window_aggregator.py  # 09:31~09:40 early-window snapshot
│   │   └── filters/         # Stock/quality filters
│   │       ├── momentum_quality_filter.py  # Volume filter
│   │       ├── reversal_factor_filter.py   # 冲高回落 filter
│   │       └── stock_filter.py             # Exchange filter
│   ├── trading/             # Trading module
│   │   ├── position_manager.py  # Slot-based position management
│   │   ├── holding_tracker.py   # Overnight holding tracking
│   │   └── repository.py       # Trading DB repository
│   ├── data/                # Data module
│   │   ├── clients/         # Storage + read-only adapters (no upstream API calls)
│   │   │   ├── greptime_storage.py            # Pure GreptimeDB storage (CRUD only)
│   │   │   ├── greptime_historical_adapter.py # Read-only adapter (HistoricalDataProvider Protocol)
│   │   │   ├── iquant_historical_adapter.py   # Live historical adapter
│   │   │   ├── tushare_realtime.py            # Tushare realtime quotes
│   │   │   └── sina_realtime.py               # Sina realtime (fallback)
│   │   ├── sources/         # Upstream API wrappers (one source per feed)
│   │   │   ├── tsanghi_daily_source.py        # tsanghi daily_latest
│   │   │   ├── tushare_minute_source.py       # Tushare stk_mins (1min bars)
│   │   │   ├── tushare_metadata_source.py     # Tushare bak_basic / suspend_d / trade_cal
│   │   │   └── local_concept_mapper.py        # Board ↔ stock mapping
│   │   └── services/        # Orchestration / scheduling
│   │       ├── cache_pipeline.py            # Storage write orchestration (sources → aggregator → storage)
│   │       ├── cache_progress_reporter.py   # Phase enum + Feishu notifications
│   │       ├── cache_scheduler.py           # 3am daily storage gap-fill
│   │       └── model_training_scheduler.py  # ML model finetune scheduler
│   ├── web/                 # Web UI
│   │   ├── app.py           # FastAPI application
│   │   ├── routes.py        # Main routes + backtest + settings
│   │   ├── iquant_routes.py # iQuant communication + monitoring
│   │   └── templates/       # Jinja2 templates
│   └── common/              # Shared utilities
│       ├── config.py        # Configuration + credential management
│       ├── feishu_bot.py    # Feishu notifications
│       ├── scheduler.py     # Trading session scheduler
│       └── pending_store.py # Pending confirmation store
├── data/                    # Runtime data files
│   ├── sectors.json         # THS board names
│   └── board_constituents.json  # Board → stock mapping + stock names
├── config/
│   └── database-config.yaml # GreptimeDB connection config
├── scripts/
│   ├── iquant_live.py       # iQuant live trading script
│   └── audit_trading_safety.py  # Safety audit
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
| Backtest Cache | GreptimeDB (asyncpg pgwire port 4003) | Time-series optimized, OSS object storage, no in-memory caching |
| Config Format | YAML | Human-readable, supports hot-reload |
| Market Data | Tushare Pro (realtime + trade_cal + stock_basic + suspend_d + `stk_mins` 1min for backtest minute), tsanghi (backtest daily, **max concurrency=2**) | A-share real-time and historical data |
| GreptimeDB Client | asyncpg | Async PostgreSQL wire protocol access |

## GreptimeDB Rules (CRITICAL)

GreptimeDB is an LSM-tree engine. Write path: INSERT → memtable + WAL → FLUSH → SST (persisted to OSS).

### Core Rule: FLUSH After Every Write

**Data is only durable after FLUSH to SST.** WAL is on `/tmp` (lost on container rebuild). `auto_flush_interval=1m` is a fallback, not a guarantee.

```python
# ✅ Correct: explicit FLUSH after writes
await db.execute(f"INSERT INTO t{cols} VALUES {val}")
await db.execute("ADMIN FLUSH_TABLE('t')")

# ✅ Batch writes: FLUSH once after all INSERTs (not per row)
for code, rec in records:
    await db.execute(f"INSERT INTO t{cols} VALUES {val}")
await db.execute("ADMIN FLUSH_TABLE('t')")  # one FLUSH is enough
```

Applies to **all write scenarios**: download, backfill, repair — no exceptions.

### ALTER TABLE Add Column: DELETE → FLUSH → INSERT → FLUSH

Old SST rows lack the new column. Plain upsert gets overwritten by old SST on merge. Must delete old rows first, FLUSH each step.

```python
for code in codes:
    await db.execute(f"DELETE FROM t WHERE stock_code='{code}' AND ts={ts_ms}")
await db.execute("ADMIN FLUSH_TABLE('t')")  # persist tombstones
for code in codes:
    await db.execute(f"INSERT INTO t{cols} VALUES {val_map[code]}")
await db.execute("ADMIN FLUSH_TABLE('t')")  # persist new data
```

### Batch Write Limits

| Operation | Safe limit | Beyond |
|-----------|-----------|--------|
| Single-row INSERT (1 VALUES row) | Unlimited | — |
| Multi-row INSERT (N VALUES rows) | ~200 rows | Silent data loss |

### Connection Pool

- Must use `asyncpg.create_pool(min_size=0, max_size=3, statement_cache_size=0)`
- Must override `reset()` to no-op (GreptimeDB doesn't support `RESET ALL` / `DEALLOCATE ALL`)
- Single connection doesn't support concurrent ops — must use pool

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
