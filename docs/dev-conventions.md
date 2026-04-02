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
| **Data/Info** | Market data (Tushare/tsanghi), fundamentals (PostgreSQL), boards (local JSON) | No - runs continuously |

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
│   │   ├── strategies/
│   │   │   └── v15_scanner.py  # V15 7-layer funnel + V3 scoring
│   │   └── filters/         # Stock/quality filters
│   │       ├── momentum_quality_filter.py  # Volume filter
│   │       ├── reversal_factor_filter.py   # 冲高回落 filter
│   │       ├── board_relevance_filter.py   # LLM board relevance
│   │       └── stock_filter.py             # Exchange filter
│   ├── trading/             # Trading module
│   │   ├── position_manager.py  # Slot-based position management
│   │   ├── holding_tracker.py   # Overnight holding tracking
│   │   └── repository.py       # Trading DB repository
│   ├── data/                # Data module
│   │   ├── clients/         # Data source adapters
│   │   │   ├── greptime_backtest_cache.py   # Backtest cache (GreptimeDB)
│   │   │   ├── iquant_historical_adapter.py # Live historical adapter
│   │   │   ├── tushare_realtime.py         # Tushare realtime quotes
│   │   │   └── sina_realtime.py            # Sina realtime (fallback)
│   │   ├── database/        # Database layers
│   │   │   └── fundamentals_db.py  # Stock fundamentals reader
│   │   ├── services/
│   │   │   └── cache_scheduler.py  # 3am daily cache gap-fill
│   │   └── sources/
│   │       └── local_concept_mapper.py  # Board ↔ stock mapping
│   ├── web/                 # Web UI
│   │   ├── app.py           # FastAPI application
│   │   ├── routes.py        # Main routes + backtest + settings
│   │   ├── iquant_routes.py # iQuant live trading API
│   │   └── templates/       # Jinja2 templates
│   └── common/              # Shared utilities
│       ├── config.py        # Configuration + credential management
│       ├── feishu_bot.py    # Feishu notifications
│       ├── scheduler.py     # Trading session scheduler
│       └── pending_store.py # Pending confirmation store
├── data/                    # Runtime data files
│   ├── sectors.json         # THS board names
│   └── board_constituents.json  # Board → stock mapping
├── config/
│   └── database-config.yaml # PostgreSQL connection config
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
| Trading Data | PostgreSQL (trading schema) | Unified with messages DB |
| Message Data | PostgreSQL (external, read-only) | Messages streamed by external collector |
| Backtest Cache | GreptimeDB (asyncpg pgwire port 4003) | Time-series optimized, OSS object storage, no in-memory caching |
| Config Format | YAML | Human-readable, supports hot-reload |
| Market Data | Tushare Pro (realtime + trade_cal + stock_basic + suspend_d), tsanghi (backtest daily + 5min, **max concurrency=2**) | A-share real-time and historical data |
| PostgreSQL Client | asyncpg | Async PostgreSQL access |

## GreptimeDB 操作规范（CRITICAL）

GreptimeDB 是 LSM-tree 存储引擎，数据分 memtable（内存）和 SST 文件（磁盘）两层。
理解这个分层对正确写入数据至关重要。

### ALTER TABLE 新增列后更新旧行

**旧 SST 文件中的行没有新列**。直接 INSERT 同 PK 的行（upsert），新列的值只存在于 memtable。
当 memtable flush 到 SST 并与旧 SST 合并时，**旧行会覆盖新列的值**，导致新列回退到 NULL。

正确做法：**DELETE → FLUSH（持久化 tombstone）→ INSERT → FLUSH（持久化新数据）**

大量行（3000+）时，单次 FLUSH 不够：memtable 在 INSERT 过程中可能自动刷盘，
tombstone 和新行分到不同 SST，旧数据在 merge 时赢回来。**必须两次 FLUSH**。

```python
# ❌ 错误：纯 upsert — flush 后新列值丢失
await db.execute(f"INSERT INTO t{cols} VALUES {val}")

# ❌ 错误：DELETE + INSERT + 单次 FLUSH — 3000+ 行时部分数据仍丢失
for code in codes:
    await db.execute(f"DELETE FROM t WHERE stock_code='{code}' AND ts={ts_ms}")
for code in codes:
    await db.execute(f"INSERT INTO t{cols} VALUES {val_map[code]}")
await db.execute("ADMIN FLUSH_TABLE('t')")

# ✅ 正确：DELETE → FLUSH → INSERT → FLUSH（两次刷盘）
for code in codes:
    await db.execute(f"DELETE FROM t WHERE stock_code='{code}' AND ts={ts_ms}")
await db.execute("ADMIN FLUSH_TABLE('t')")  # tombstone 落盘
for code in codes:
    await db.execute(f"INSERT INTO t{cols} VALUES {val_map[code]}")
await db.execute("ADMIN FLUSH_TABLE('t')")  # 新数据落盘
```

### 批量写入规则

| 操作 | 可靠上限 | 超过后果 |
|------|---------|---------|
| 单条 INSERT (VALUES 1 行) | 无限制 | — |
| 批量 INSERT (VALUES N 行) | ~200 行 | 静默丢数据 |
| 批量 DELETE (IN 子句) | ~200 code | 未验证，建议同上 |

### ADMIN 管理命令

```sql
ADMIN FLUSH_TABLE('backtest_daily')    -- memtable → SST，确保写入持久化
ADMIN COMPACT_TABLE('backtest_daily')  -- 合并 SST 文件，回收空间
```

### 连接池注意事项

- 必须用 `asyncpg.create_pool(min_size=0, max_size=3, statement_cache_size=0)`
- 必须自定义连接类禁用 `reset()`（GreptimeDB 不支持 `RESET ALL` / `DEALLOCATE ALL`）
- 单连接不支持并发操作，必须用连接池

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
