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

GreptimeDB exposes a PostgreSQL wire protocol on port 4003, but its compatibility is incomplete in places asyncpg's pool exercises every few thousand queries. The result is **silent hangs** that take hours to diagnose. The rules below encode hard-won lessons — do NOT skip any of them.

### asyncpg Connection Pool: Three Mandatory Overrides

GreptimeDB's PG protocol has three holes that asyncpg's pool will hit. Missing any one causes a silent wedge.

**1. `statement_cache_size=0`** — GreptimeDB does not support `PREPARE` / `DEALLOCATE`. asyncpg's default 1024-entry prepared-statement cache trips this on the first query.

**2. Override `Connection.reset()` → no-op** — asyncpg calls `reset()` when releasing a connection back to the pool. The default issues `RESET ALL` and `DEALLOCATE ALL`, both rejected by GreptimeDB.

**3. Override `Connection.close()` → `terminate()`** — THE BIG ONE. asyncpg's pool recycles a connection after `max_queries` (default 50000) by calling `await connection.close()`. Default `close()` sends a PG `Terminate` message and **awaits the server's socket close**. GreptimeDB never closes the socket, so `close()` blocks forever. Symptom: download hangs at ~13 stock_list dates × ~5000 INSERTs each ≈ 65k queries. The hang is **completely invisible** because:
- `asyncio.wait_for(runner)` already returned successfully
- the slow-SQL watchdog was cancelled in the finally block
- `pool.release()` itself has no timeout

Fix: override `close()` to call `self.terminate()` (TCP reset, no handshake).

```python
class _GreptimeConnection(asyncpg.Connection):
    async def reset(self, *, timeout: float | None = None) -> None:
        pass

    async def close(self, *, timeout: float | None = None) -> None:
        # Do NOT call super().close() — it sends a PG Terminate and waits
        # for the server socket to close, which hangs forever on GreptimeDB.
        if not self.is_closed():
            self.terminate()

await asyncpg.create_pool(
    host=..., port=4003, user="greptime",
    min_size=0, max_size=3,
    statement_cache_size=0,
    connection_class=_GreptimeConnection,
)
```

### Slow-Query Watchdog (Mandatory)

asyncpg's C extension can hang on `socket.recv` and **swallow `CancelledError`**. Even if `asyncio.wait_for(..., timeout=120)` says the query timed out, the underlying syscall may still be blocked. To make hangs visible, run a sibling task that logs every 30s while the SQL is in flight:

```python
async def _slow_query_watchdog():
    while True:
        await asyncio.sleep(30)
        logger.warning("SQL still running after %.0fs: op=%s sql=%r",
                       time.monotonic() - t_query, op_name, sql_preview)
```

Without this, "the download hangs" is your only signal. With it, you get a per-30s log line naming the exact stuck SQL.

### Classify acquire vs query timeout separately

Wrap pool acquire and query execution in **separate** `wait_for()` blocks. When something hangs you must know whether the pool is exhausted (all 3 connections held) or the DB is genuinely stuck. A single combined timeout doesn't tell you which.

```python
conn = await asyncio.wait_for(pool.acquire(), timeout=30)
try:
    return await asyncio.wait_for(runner(conn), timeout=120)
finally:
    await asyncio.wait_for(pool.release(conn), timeout=10)  # belt-and-suspenders
```

### Belt-and-suspenders: `pool.release()` timeout

Even with the `close()` fix, wrap `pool.release(conn)` in `wait_for(..., timeout=10)`. If a future regression reintroduces a release-time hang, log loudly and `conn.terminate()` instead of stalling silently.

### Root logger must be configured at app startup

uvicorn only attaches handlers to its own `uvicorn` / `uvicorn.access` loggers. Without explicit root-logger config, every `logging.getLogger(__name__).warning(...)` in the project is silently dropped — including the slow-SQL watchdog above. Add a `StreamHandler` to the root logger at app import time (idempotent for pytest).

### Single connection ≠ pool

`asyncpg.connect()` returns one connection that does **not** support concurrent ops. Two awaiters (e.g. download task + status poll) will hit `InterfaceError: another operation is in progress`. Always use `create_pool()`.

### Reserved words: must double-quote

`open`, `close`, `volume` are reserved in GreptimeDB's SQL parser. Use snake_case alternatives (`open_price`, `close_price`, `vol`) or always wrap in double quotes.

### Batch Write Limits

| Operation | Safe limit | Beyond |
|-----------|-----------|--------|
| Single-row INSERT (1 VALUES row) | Unlimited | — |
| Multi-row INSERT (N VALUES rows) | ~200 rows | Silent data loss |

GreptimeDB silently drops rows past ~200 in a multi-row INSERT. Always one INSERT per row, or batch ≤ 200 rows.

### ALTER TABLE: New Column Has NULL for Old Rows

Adding a column via `ALTER TABLE` leaves the column NULL for every existing row. Two consequences:

1. **WHERE clauses must tolerate NULL.** `WHERE is_suspended = false` excludes NULL rows. Use `WHERE (is_suspended = false OR is_suspended IS NULL)`, or backfill old rows before relying on the column for filtering.
2. **To populate old rows**, DELETE then re-INSERT. Plain UPDATE has surprising semantics under LSM merge with partial column overwrites; the natural-upsert (last_row merge) of a fresh INSERT is the safe path.

### No application-layer caching

Database client does no in-memory caching. One row written = one row dropped from RAM. GreptimeDB manages its own cache. Do not add `deploy.resources.limits.memory` to the GreptimeDB container — let it manage its own working set.

### Cache pipeline phase order

`CachePipeline.download_prices()` runs the following phases in order. Resume / audit semantics are documented per-phase:

| Phase | Resume granularity | Audit / backfill |
|-------|--------------------|------------------|
| `daily` | by **day** — `get_existing_daily_dates()` skips any date that has at least one row in `backtest_daily` | `_backfill_daily_gaps()` compares per-day COUNT(stock_list) vs COUNT(backtest_daily) and refetches the diff codes |
| `minute` | by **stock** — `get_existing_minute_codes(start, end)` skips any code that has *any* bar in the range | `_backfill_minute_gaps()` runs `audit_minute_gaps_in_range()`: per day, diffs the active daily code set against the distinct minute code set, then refetches each missing `(day, code)` pair via single-day `stk_mins` requests |

The two-phase minute design exists because Tushare's `stk_mins` is a per-stock API: the natural unit of one call is "one stock × range", so the main download uses per-stock resume for throughput. The per-day audit phase then catches any holes the coarse resume cannot detect — e.g. a stock that has bars on most days but is missing one transient-failure day.

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
