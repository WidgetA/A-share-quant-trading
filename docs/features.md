# Feature Specifications

> **Important**: This document is the single source of truth for feature requirements.
> Always update this document BEFORE implementing any new feature or change.

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 0.1.0 | 2026-01-27 | - | Initial document structure |
| 0.1.1 | 2026-01-27 | - | Add THS SDK installation scripts |
| 0.1.2 | 2026-01-27 | - | TRD-002: Add SQLite as trading data storage |
| 0.1.3 | 2026-01-27 | - | DAT-003: Message collection module implemented |
| 0.1.4 | 2026-01-27 | - | DAT-004: Real data sources (akshare announcements, CLS, eastmoney, sina) |
| 0.1.5 | 2026-01-27 | - | DAT-005: Limit-up stocks data collection via iFinD |
| 0.2.0 | 2026-01-28 | - | SYS-001/002/003: Main program, state management, scheduler |
| 0.2.1 | 2026-01-28 | - | STR-001/002: Strategy base interface and hot-reload |
| 0.3.0 | 2026-01-28 | - | STR-003: News analysis strategy with LLM (Silicon Flow Qwen) |
| 0.3.1 | 2026-01-28 | - | STR-003: Add sector buying support (multiple stocks per slot) |
| 0.3.2 | 2026-01-28 | - | STR-003: Add limit-up price check to avoid buying at ceiling |
| 0.3.3 | 2026-01-28 | - | STR-003: Add user confirmation when sector has limit-up stocks |
| 0.4.0 | 2026-02-03 | - | Architecture refactor: Message collection moved to external project, this project reads from PostgreSQL |
| 0.4.1 | 2026-02-03 | - | INF-002: Add GitHub Actions CI pipeline |
| 0.5.0 | 2026-02-03 | - | TRD-000: Trading data persistence with PostgreSQL (trading schema) |
| 0.5.1 | 2026-02-03 | - | SYS-002/DAT-005: Migrate StateManager and LimitUpDatabase from SQLite to PostgreSQL |

---

## Module: System

### [SYS-001] Main Program Entry

**Status**: Completed

**Description**: Central entry point that orchestrates all modules for 24/7 operation.

**Requirements**:
- Single entry point for entire trading system
- Coordinate startup/shutdown of all modules
- Signal handling (SIGINT, SIGTERM) for graceful shutdown
- Periodic statistics logging
- Event-driven module coordination

**Technical Design**:
- `SystemManager` class orchestrates all components
- `ModuleCoordinator` provides event pub/sub for loose coupling
- Event types: NEWS_RECEIVED, SIGNAL_GENERATED, SESSION_CHANGED, etc.

**Files**:
- `scripts/main.py` - Main entry point
- `src/common/coordinator.py` - Event coordinator
- `config/main-config.yaml` - System configuration

**Usage**:
```bash
# Start the system
uv run python scripts/main.py

# With custom config
uv run python scripts/main.py --config config/main-config.yaml
```

**Acceptance Criteria**:
- [x] SystemManager class implemented
- [x] ModuleCoordinator with event pub/sub
- [x] Signal handlers for graceful shutdown
- [x] Statistics logging
- [x] YAML configuration support

---

### [SYS-002] State Persistence and Recovery

**Status**: Completed

**Description**: Enable system recovery from crashes by persisting state to PostgreSQL.

**Requirements**:
- Persist system state (STARTING, RUNNING, STOPPED, etc.)
- Save module checkpoints periodically
- Detect crash on startup (last state was RUNNING)
- Restore from checkpoints after crash
- Configurable checkpoint age limit

**Technical Design**:
- `StateManager` class manages PostgreSQL persistence
- Database: PostgreSQL (same instance as trading, `trading` schema)
- Tables:
  - `trading.system_state` - Overall system state
  - `trading.module_checkpoints` - Per-module recovery data
- Connection pooling via asyncpg
- Checkpoint interval: configurable (default 60s)
- Recovery: load checkpoints, resume modules, clear old checkpoints

**Files**:
- `src/common/state_manager.py` - State persistence manager
- `config/database-config.yaml` - Database configuration

**Acceptance Criteria**:
- [x] SystemState enum (STARTING, RUNNING, STOPPED, etc.)
- [x] Checkpoint save/load operations
- [x] Crash detection on startup
- [x] Recovery flow implemented
- [x] Configurable checkpoint age limit
- [x] PostgreSQL with asyncpg connection pool

---

### [SYS-003] Trading Session Scheduler

**Status**: Completed

**Description**: Automatic detection of A-share trading sessions for session-aware scheduling.

**Requirements**:
- Detect current trading session (PRE_MARKET, MORNING, LUNCH_BREAK, AFTERNOON, AFTER_HOURS, CLOSED)
- Check if market is in trading hours
- Check if date is a trading day (exclude weekends)
- Calculate time until next session
- Callback registration for session changes

**Technical Design**:
- `TradingScheduler` class with session detection
- `MarketSession` enum for session types
- Trading hours (Beijing Time):
  - Morning Auction: 9:15-9:30
  - Morning Session: 9:30-11:30
  - Lunch Break: 11:30-13:00
  - Afternoon Session: 13:00-15:00
- Note: Holiday calendar not included (integrate with external source for production)

**Files**:
- `src/common/scheduler.py` - Trading session scheduler

**Usage**:
```python
from src.common.scheduler import TradingScheduler, MarketSession

scheduler = TradingScheduler()

# Check current session
session = scheduler.get_current_session()
if scheduler.is_trading_hours():
    # Execute trading logic
    pass

# Get time until next session
next_session, time_delta = scheduler.time_until_next_session()
print(f"Next: {next_session.value} in {time_delta}")

# Register callback
def on_change(old, new):
    print(f"Session: {old.value} -> {new.value}")
scheduler.add_session_callback(on_change)
```

**Acceptance Criteria**:
- [x] MarketSession enum defined
- [x] Session detection implemented
- [x] Trading hours check
- [x] Trading day check (weekday)
- [x] Time until next session calculation
- [x] Session change callbacks

---

## Module: Strategy

### [STR-001] Base Strategy Interface

**Status**: Completed

**Description**: Define the base interface that all trading strategies must implement.

**Requirements**:
- Strategies must implement `generate_signals(context) -> AsyncIterator[TradingSignal]`
- Strategies must be loadable at runtime without system restart
- Support strategy parameter configuration via YAML
- Lifecycle hooks: on_load(), on_unload(), on_reload()

**Technical Design**:
- `BaseStrategy` abstract class defines the interface
- `TradingSignal` dataclass with: signal_type, stock_code, quantity, price, confidence, reason
- `SignalType` enum: BUY, SELL, HOLD
- `StrategyContext` provides market data, messages, positions to strategies
  - Messages are provided by platform-layer `MessageReader` (reads from PostgreSQL)
  - Strategies should NOT directly access database; use `context.get_messages_since()` instead

**Files**:
- `src/strategy/__init__.py` - Module exports
- `src/strategy/base.py` - BaseStrategy abstract class
- `src/strategy/signals.py` - TradingSignal and SignalType
- `src/strategy/strategies/example_strategy.py` - Example implementation

**Usage**:
```python
from src.strategy.base import BaseStrategy, StrategyContext
from src.strategy.signals import TradingSignal, SignalType

class MyStrategy(BaseStrategy):
    @property
    def strategy_name(self) -> str:
        return "my_strategy"

    async def generate_signals(
        self, context: StrategyContext
    ) -> AsyncIterator[TradingSignal]:
        if some_condition:
            yield TradingSignal(
                signal_type=SignalType.BUY,
                stock_code="000001.SZ",
                quantity=100,
                strategy_name=self.strategy_name,
            )
```

**Acceptance Criteria**:
- [x] BaseStrategy abstract class defined
- [x] TradingSignal dataclass with metadata
- [x] SignalType enum (BUY/SELL/HOLD)
- [x] StrategyContext for market data/news
- [x] Lifecycle hooks (on_load, on_unload, on_reload)
- [x] Example strategy implementation

---

### [STR-002] Strategy Hot-Reload

**Status**: Completed

**Description**: Enable updating strategies during market hours without stopping the trading system.

**Requirements**:
- File watcher detects strategy file changes
- New strategy takes effect on next signal generation cycle
- Rollback mechanism if new strategy fails validation
- Strategy validation before activation

**Technical Design**:
- `StrategyEngine` manages strategy lifecycle
- File watcher checks strategy files every 5 seconds
- Hot-reload flow:
  1. Detect file change via hash comparison
  2. Load new strategy class
  3. Validate new strategy
  4. Call on_unload() on old strategy
  5. Swap strategy instance
  6. Call on_load() and on_reload() on new strategy
  7. On failure, keep old strategy

**Files**:
- `src/strategy/engine.py` - StrategyEngine with hot-reload
- `src/strategy/strategies/` - Strategy files directory

**Usage**:
```python
from src.strategy.engine import StrategyEngine

engine = StrategyEngine(
    strategy_dir=Path("src/strategy/strategies"),
    hot_reload=True
)
await engine.start()

# Strategies are loaded automatically from directory
# Modify a .py file -> engine detects and reloads

async for signal in engine.generate_all_signals(context):
    # Process signals
    pass

await engine.stop()
```

**Acceptance Criteria**:
- [x] StrategyEngine class implemented
- [x] Strategy file change detection
- [x] Graceful strategy swap without interruption
- [x] Strategy validation before activation
- [x] Rollback on reload failure

---

### [STR-003] News Analysis Strategy

**Status**: Completed

**Description**: News-driven trading strategy using LLM (Silicon Flow Qwen) to analyze financial news and announcements for trading signals.

**Requirements**:
- Analyze overnight news/announcements at 8:30 AM (premarket)
- Real-time monitoring during trading hours
- LLM-based sentiment analysis identifying: dividends, earnings, restructuring
- Stock filtering: exclude BSE (北交所) and ChiNext (创业板)
- Sector-level signal resolution to individual stocks
- Slot-based position management (5 slots: 3 premarket + 2 intraday)
- **Sector buying support**: Each slot can hold either one stock OR multiple stocks from the same sector
  - Single stock: 20% capital on one stock
  - Sector buying: 20% capital split equally among sector stocks (e.g., 光通信板块: 烽火通信 + 亨通光电 = 10% each)
- Overnight holding tracking with morning sell confirmation
- Command-line user interaction for buy/sell decisions
- **Limit-up price check**: Skip buying stocks already at limit-up (涨停价) to avoid:
  - Buying at the highest possible price with zero upside
  - Next-day drop risk when limit-up cannot continue
  - Detection methods: direct limit_up_price, calculated from prev_close, or change_ratio
- **Limit-up user confirmation**: When sector has some stocks at limit-up:
  - Show summary of limit-up vs available stocks
  - Let user choose which available stocks to buy, or skip the sector entirely
  - Prevents blindly buying the "leftover" stocks without user awareness

**Technical Design**:
- LLM Service: Silicon Flow Qwen model via OpenAI-compatible API
- News Analyzer: Batch analysis with caching and confidence thresholds
- Stock Filter: Pattern-based exchange detection and filtering
- Sector Mapper: akshare-based industry board mapping
- Position Manager: Slot-based capital allocation (20% per slot)
  - `PositionSlot.holdings: list[StockHolding]` - Supports multiple stocks per slot
  - `StockHolding`: stock_code, stock_name, quantity, entry_price
  - Sector buying: capital divided equally among holdings
  - State persistence: `save_to_file()` / `load_from_file()` for crash recovery
- Holding Tracker: Overnight position tracking for next-day confirmation
- User Interaction: Async command-line interface with timeout handling
- Limit-Up Check: Before buying, verify stock is not at limit-up price
  - `_get_limit_up_ratio()`: Returns 10% for main board, 20% for ChiNext/STAR
  - `_is_at_limit_up()`: Checks current_price vs limit_up_price with 0.5% tolerance
  - Classifies stocks into limit-up vs available lists
  - `confirm_limit_up_situation()`: Shows user which stocks are at limit-up,
    lets user select from available stocks or skip the sector

**Files**:
- `src/common/llm_service.py` - Silicon Flow LLM integration
- `src/common/user_interaction.py` - Command-line user interaction
- `src/strategy/filters/stock_filter.py` - Exchange-based stock filtering
- `src/strategy/analyzers/news_analyzer.py` - LLM-based news analysis
- `src/data/sources/sector_mapper.py` - Stock-to-sector mapping
- `src/trading/position_manager.py` - Slot-based position management
- `src/trading/holding_tracker.py` - Overnight holding tracking
- `src/strategy/strategies/news_analysis_strategy.py` - Main strategy implementation
- `config/news-strategy-config.yaml` - Strategy configuration
- `config/secrets.yaml` - Silicon Flow API key (add `siliconflow.api_key`)

**Usage**:
```python
# Strategy is auto-loaded by StrategyEngine
# Configure in config/news-strategy-config.yaml

# Manual testing:
from src.strategy.strategies.news_analysis_strategy import NewsAnalysisStrategy
from src.strategy.base import StrategyConfig, StrategyContext

config = StrategyConfig(
    name="news_analysis",
    enabled=True,
    parameters={
        "total_capital": 10_000_000,
        "min_confidence": 0.7,
    }
)
strategy = NewsAnalysisStrategy(config)
await strategy.on_load()

# Generate signals
async for signal in strategy.generate_signals(context):
    print(f"Signal: {signal}")
```

**Configuration** (`config/news-strategy-config.yaml`):
```yaml
strategy:
  news_analysis:
    enabled: true
    llm:
      model: "Qwen/Qwen2.5-72B-Instruct"
    analysis:
      min_confidence: 0.7
      signal_types: [dividend, earnings, restructure]
    position:
      total_capital: 10000000
      premarket_slots: 3
      intraday_slots: 2
    filter:
      exclude_bse: true
      exclude_chinext: true
      exclude_star: false
    schedule:
      premarket_analysis_time: "08:30"
      morning_confirmation_time: "09:00"
```

**Acceptance Criteria**:
- [x] LLM service with Silicon Flow integration
- [x] Stock filter excluding BSE/ChiNext
- [x] Sector mapper using akshare
- [x] News analyzer with LLM analysis
- [x] Position manager with slot allocation
- [x] Holding tracker for overnight positions
- [x] User interaction via command line
- [x] Main strategy implementation
- [x] Configuration file
- [x] Limit-up price check before buying
- [x] User confirmation when sector has limit-up stocks

---

## Module: Trading

### [TRD-000] Trading Data Persistence

**Status**: Completed

**Description**: PostgreSQL-based persistence for all trading data including positions, orders, and transactions. Uses separate schema for isolation from messages.

**Requirements**:
- Store position slots and their state
- Track stock holdings within slots (supports sector buying)
- Record orders and their lifecycle
- Record transactions (fills)
- Track overnight holdings for morning review
- Schema isolation from message data

**Technical Design**:
- Database: PostgreSQL (same instance as messages, separate schema)
- Schema: `trading` (isolated from `public` schema)
- Tables:
  - `position_slots` - Slot state (id, type, state, entry_time, reason, sector)
  - `stock_holdings` - Holdings per slot (stock_code, quantity, price)
  - `orders` - Order records (id, type, stock, qty, price, status)
  - `transactions` - Fill records (order_id, price, qty, amount, commission)
  - `overnight_holdings` - Overnight tracking for morning review

**Files**:
- `src/trading/repository.py` - TradingRepository class
- `src/trading/position_manager.py` - Updated with DB support
- `src/trading/holding_tracker.py` - Updated with DB support
- `config/database-config.yaml` - Database configuration

**Usage**:
```python
from src.trading import (
    TradingRepository,
    create_position_manager_with_db,
    create_trading_repository_from_config,
)

# Create repository and connect
repo = create_trading_repository_from_config()
await repo.connect()

# Create position manager with DB persistence
manager = await create_position_manager_with_db(config, repo)

# All operations auto-persist to PostgreSQL
slot = manager.get_available_slot("premarket")
manager.allocate_slot(slot, "000001", 15.50, "利好消息")
await manager.save_slot_to_db(slot.slot_id)
```

**Configuration** (`config/database-config.yaml`):
```yaml
database:
  trading:
    host: "${DB_HOST:localhost}"
    port: ${DB_PORT:5432}
    database: "${DB_NAME:messages}"
    user: "${DB_USER:reader}"
    password: "${DB_PASSWORD}"
    schema: "trading"
    auto_create_schema: true
```

**Acceptance Criteria**:
- [x] TradingRepository class with asyncpg
- [x] Schema and table auto-creation
- [x] Position slot CRUD operations
- [x] Stock holdings persistence
- [x] Order management methods
- [x] Transaction recording
- [x] Overnight holdings tracking
- [x] PositionManager DB integration
- [x] HoldingTracker DB integration

---

### [TRD-001] Order Executor

**Status**: Planned

**Description**: Core component that receives signals and executes orders.

**Requirements**:
- Receive signals from strategy module via message queue
- Support order types: MARKET, LIMIT
- Track order status: PENDING → SUBMITTED → FILLED/CANCELLED
- Position management and P&L calculation

**Acceptance Criteria**:
- [ ] Order executor class implemented
- [ ] Message queue integration
- [ ] Order state machine implemented

---

### [TRD-002] Paper Trading

**Status**: Planned

**Description**: Simulated trading environment for strategy testing.

**Requirements**:
- Simulate order fills with realistic slippage model
- Track virtual positions and P&L
- Use real market data for simulation
- Export trading records for analysis

**Technical Design**:
- Database: PostgreSQL (trading schema, reuse TRD-000 infrastructure)
- Additional tables if needed:
  - `paper_account` - Virtual cash balance, total value

**Acceptance Criteria**:
- [ ] Paper trading executor implemented
- [ ] Slippage model configurable
- [ ] Trade log export functionality

---

### [TRD-003] Live Trading

**Status**: Planned

**Description**: Real trading with broker API integration.

**Requirements**:
- Broker API adapter (support multiple brokers)
- Risk checks before order submission
- Order confirmation and error handling
- Real-time position sync with broker

**Acceptance Criteria**:
- [ ] At least one broker adapter implemented
- [ ] Pre-trade risk checks
- [ ] Order lifecycle management

---

## Module: Data/Info

### [DAT-001] Real-time Market Data

**Status**: Planned

**Description**: Provide real-time stock quotes and market data.

**Requirements**:
- Subscribe to real-time quotes for watchlist stocks
- Data format: OHLCV + bid/ask
- Publish data to message queue for other modules
- Support A-share trading hours (9:30-11:30, 13:00-15:00)

**Acceptance Criteria**:
- [ ] Real-time data source adapter
- [ ] Data normalization to standard format
- [ ] Message queue publishing

---

### [DAT-002] Historical Data

**Status**: Planned

**Description**: Historical OHLCV data for backtesting and analysis.

**Requirements**:
- Daily and minute-level historical data
- Data storage in TimescaleDB
- API for querying historical data
- Data update scheduler

**Acceptance Criteria**:
- [ ] Historical data fetcher implemented
- [ ] Database schema designed
- [ ] Query API available

---

### [DAT-003] Message Reader (Platform Layer)

**Status**: Planned

**Description**: Platform-level component that reads messages from external PostgreSQL database. Message collection is handled by a separate project that streams data into PostgreSQL.

**Architecture**:
```
┌─────────────────────────────────────────────────────────────┐
│              External Message Collector Project             │
│  (CLS, East Money, Sina, Akshare → PostgreSQL streaming)   │
└─────────────────────────────────────────────────────────────┘
                              ↓
                    PostgreSQL (messages table)
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                 This Project (Strategy Platform)            │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              MessageReader (Platform Layer)          │   │
│  │    - Connects to PostgreSQL                         │   │
│  │    - Polls for new messages                         │   │
│  │    - Provides unified query interface               │   │
│  └─────────────────────────────────────────────────────┘   │
│                              ↓                              │
│                    StrategyContext.messages                 │
│                              ↓                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Strategy A  │  │  Strategy B  │  │  Strategy C  │      │
│  │ (NewsAnalysis)│  │   (Future)   │  │   (Future)   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

**Requirements**:
- Connect to external PostgreSQL database (read-only)
- Polling-based message retrieval with incremental queries
- Provide unified interface for all strategies via StrategyContext
- Support time-range queries (e.g., messages since last close)
- Support source-type filtering (announcement, news, social)
- Connection pooling for efficiency

**Technical Design**:
- Library: `asyncpg` for async PostgreSQL access
- Query Strategy: Polling with `fetch_time > last_check_time`
- PostgreSQL Table Schema (read from external DB):
  - `id` (TEXT PRIMARY KEY)
  - `source_type` (TEXT) - announcement/news/social
  - `source_name` (TEXT) - cls/eastmoney/sina/akshare
  - `title` (TEXT)
  - `content` (TEXT)
  - `url` (TEXT)
  - `stock_codes` (TEXT) - JSON array
  - `publish_time` (TIMESTAMP)
  - `fetch_time` (TIMESTAMP)
  - `raw_data` (JSONB)
- Indexes (managed by external project): source_type, publish_time, fetch_time

**Files**:
- `src/data/models/message.py` - Message data model (unchanged)
- `src/data/readers/message_reader.py` - PostgreSQL message reader
- `src/data/readers/__init__.py` - Reader exports
- `config/database-config.yaml` - PostgreSQL connection config

**Usage**:
```python
# Platform layer initializes MessageReader
from src.data.readers import MessageReader

reader = MessageReader(config)
await reader.connect()

# Strategies access messages via StrategyContext
class MyStrategy(BaseStrategy):
    async def generate_signals(self, context: StrategyContext):
        # Get messages since last market close
        messages = await context.get_messages_since(last_close_time)

        # Get messages by source type
        news = await context.get_messages(source_type="news", limit=100)

        # Process messages...
```

**Configuration** (`config/database-config.yaml`):
```yaml
database:
  messages:
    host: "localhost"
    port: 5432
    database: "messages"
    user: "reader"
    password: "${MESSAGES_DB_PASSWORD}"  # From environment
    pool_size: 5
    read_only: true
```

**Acceptance Criteria**:
- [ ] MessageReader class with async PostgreSQL connection
- [ ] Connection pooling configured
- [ ] Incremental query support (by fetch_time)
- [ ] Source type filtering
- [ ] StrategyContext integration
- [ ] Configuration via YAML
- [ ] Unit tests with mocked database

---

### [DAT-004] Real Data Sources

**Status**: Removed (Migrated to External Project)

**Description**: ~~Production-ready data sources for fetching real financial news and announcements.~~

**Migration Note**:
Message collection has been moved to a separate project. This project now only reads messages from PostgreSQL. The external project handles:
- Akshare announcements
- CLS (财联社) telegraph
- East Money (东方财富) news
- Sina Finance (新浪财经) news
- Deduplication and streaming to PostgreSQL

**Removed Files**:
- `src/data/sources/akshare_announcement.py`
- `src/data/sources/cls_news.py`
- `src/data/sources/eastmoney_news.py`
- `src/data/sources/sina_news.py`
- `src/data/sources/base.py`
- `src/data/sources/registry.py`
- `src/data/services/message_service.py`
- `src/data/database/message_db.py`
- `config/message-config.yaml`
- Related tests in `tests/unit/data/sources/`

---

### [DAT-005] Limit-Up Stocks Data Collection

**Status**: Completed

**Description**: Collect daily limit-up (涨停) stock information after market close using iFinD API, storing the data in PostgreSQL for analysis and strategy development.

**Requirements**:
- Fetch all limit-up stocks for a given trading day after market close (15:00)
- Capture comprehensive limit-up information: stock code, name, price, time, reason, etc.
- Store data in PostgreSQL database with proper indexing
- Support historical data backfill
- Idempotent: re-running for the same date updates existing records

**Technical Design**:
- Data Source: iFinD `THS_DataPool` API for limit-up board data
- Database: PostgreSQL (same instance as trading, `trading` schema)
- Table Schema:
  - `trading.limit_up_stocks`:
    - `id` (VARCHAR PRIMARY KEY) - Composite: date + stock_code
    - `trade_date` (DATE) - Trading date
    - `stock_code` (VARCHAR) - Stock code (e.g., "000001.SZ")
    - `stock_name` (VARCHAR) - Stock name
    - `limit_up_price` (DECIMAL) - Limit-up price
    - `limit_up_time` (VARCHAR) - First limit-up time (HH:MM:SS)
    - `open_count` (INTEGER) - Number of times limit opened
    - `last_limit_up_time` (VARCHAR) - Last limit-up time if reopened
    - `turnover_rate` (DECIMAL) - Turnover rate percentage
    - `amount` (DECIMAL) - Trading amount (yuan)
    - `circulation_mv` (DECIMAL) - Circulating market value
    - `reason` (TEXT) - Limit-up reason/concept
    - `industry` (VARCHAR) - Industry classification
    - `created_at` (TIMESTAMP) - Record creation timestamp
    - `updated_at` (TIMESTAMP) - Record update timestamp
  - Indexes: trade_date, stock_code, limit_up_time

**Files**:
- `src/data/models/limit_up.py` - LimitUpStock data model
- `src/data/database/limit_up_db.py` - PostgreSQL database layer
- `src/data/sources/ifind_limit_up.py` - iFinD data source
- `tests/unit/data/sources/test_ifind_limit_up.py` - Test file
- `config/database-config.yaml` - Database configuration
- `scripts/fetch_limit_up.py` - CLI script for fetching data

**Usage**:
```python
from src.data.database.limit_up_db import create_limit_up_db_from_config

# Create database from config and connect
db = create_limit_up_db_from_config()
await db.connect()

# Save limit-up stocks
await db.save(stock)
await db.save_batch(stocks)

# Query by date
stocks = await db.query_by_date("2026-01-24")

await db.close()
```

**Acceptance Criteria**:
- [x] LimitUpStock data model defined
- [x] PostgreSQL database layer with asyncpg
- [x] iFinD data source implementation
- [x] Comprehensive test coverage
- [x] Configuration via YAML
- [x] Historical backfill support

---

## Infrastructure

### [INF-001] Message Queue Setup

**Status**: Planned

**Description**: Inter-module communication via message queue.

**Requirements**:
- Redis Pub/Sub or ZeroMQ for low-latency messaging
- Message format: JSON with schema validation
- Topics: `signals`, `orders`, `market_data`, `system_events`

**Acceptance Criteria**:
- [ ] Message queue infrastructure configured
- [ ] Publisher/subscriber abstractions implemented
- [ ] Message schema defined

---

### [INF-002] CI Pipeline

**Status**: Completed

**Description**: Continuous integration pipeline for code quality.

**Requirements**:
- Lint check (ruff)
- Format check (ruff format)
- Type check (mypy)
- Unit tests (pytest)

**Technical Design**:
- GitHub Actions workflow on push/PR to main branch
- Uses `uv` for fast dependency installation
- Three parallel jobs: lint, typecheck, test

**Files**:
- `.github/workflows/ci.yml` - GitHub Actions workflow

**Acceptance Criteria**:
- [x] GitHub Actions workflow configured
- [x] Lint and format checks (ruff)
- [x] Type check (mypy)
- [x] Unit tests (pytest)

---

### [INF-003] Configuration Management

**Status**: Planned

**Description**: Centralized configuration with hot-reload support.

**Requirements**:
- YAML-based configuration files
- Environment-specific configs (dev, staging, prod)
- Runtime config reload without restart
- Secret management (API keys, credentials)

**Acceptance Criteria**:
- [ ] Config loader implemented
- [ ] Config hot-reload mechanism
- [ ] Secrets handled securely

---

### [INF-004] THS SDK Installation

**Status**: Completed

**Description**: Installation scripts for TongHuaShun iFinD SDK on Linux servers.

**Requirements**:
- Auto-detect system architecture (32/64-bit)
- Extract SDK tarball to installation directory
- Check library dependencies via ldd
- Install missing dependencies via apt-get/yum
- Run official installiFinDPy.py installer
- Configure LD_LIBRARY_PATH for runtime

**Files**:
- `scripts/install_ths_sdk.sh` - Main installation script
- `scripts/check_ths_deps.sh` - Dependency checker utility

**Usage**:
```bash
# Full installation
sudo ./scripts/install_ths_sdk.sh

# With custom options
sudo ./scripts/install_ths_sdk.sh -f /path/to/sdk.tar.gz -d /opt/ths_sdk

# Check dependencies only
./scripts/check_ths_deps.sh /opt/ths_sdk
```

**Acceptance Criteria**:
- [x] Installation script created
- [x] Dependency checking implemented
- [x] Support for apt-get and yum package managers
- [x] Auto-detection of 32/64-bit architecture

---

## Backlog

Features under consideration (not yet planned):

- [ ] Web dashboard for monitoring
- [ ] Telegram/WeChat notifications
- [ ] Backtesting framework
- [ ] Multi-account support
- [ ] Performance analytics
