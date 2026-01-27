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

**Description**: Enable system recovery from crashes by persisting state to SQLite.

**Requirements**:
- Persist system state (STARTING, RUNNING, STOPPED, etc.)
- Save module checkpoints periodically
- Detect crash on startup (last state was RUNNING)
- Restore from checkpoints after crash
- Configurable checkpoint age limit

**Technical Design**:
- `StateManager` class manages SQLite persistence
- Database: `data/system_state.db`
- Tables:
  - `system_state` - Overall system state
  - `module_checkpoints` - Per-module recovery data
- Checkpoint interval: configurable (default 60s)
- Recovery: load checkpoints, resume modules, clear old checkpoints

**Files**:
- `src/common/state_manager.py` - State persistence manager

**Acceptance Criteria**:
- [x] SystemState enum (STARTING, RUNNING, STOPPED, etc.)
- [x] Checkpoint save/load operations
- [x] Crash detection on startup
- [x] Recovery flow implemented
- [x] Configurable checkpoint age limit

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
- `StrategyContext` provides market data, news, positions to strategies

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
  - If at limit-up, skip to next stock in sector's target_stocks list

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

---

## Module: Trading

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
- Database: SQLite (zero-config, single file)
- Tables:
  - `account` - Cash balance, total value
  - `positions` - Current holdings (stock_code, qty, avg_cost)
  - `orders` - Order records (id, stock, direction, price, status)
  - `transactions` - Fill records (order_id, fill_price, fill_qty, timestamp)

**Acceptance Criteria**:
- [ ] Paper trading executor implemented
- [ ] SQLite database schema implemented
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

### [DAT-003] Message Collection

**Status**: Completed

**Description**: Continuous collection of news, announcements, and social media content for sentiment analysis and event-driven trading.

**Requirements**:
- Support multiple message source types: stock announcements, financial news, social media
- Plugin architecture for dynamic source addition/removal at runtime
- Stream-based message fetching with real-time database storage
- Continuous background operation with configurable polling intervals
- SQLite storage for collected messages

**Technical Design**:
- Architecture: Plugin-based with BaseMessageSource interface
- Components:
  - `MessageService`: Main service orchestrating all sources
  - `SourceRegistry`: Dynamic source registration/removal
  - `MessageDatabase`: Async SQLite storage layer
  - `BaseMessageSource`: Abstract base for all sources
- Database: SQLite with messages table
  - Fields: id, source_type, source_name, title, content, url, stock_codes, publish_time, fetch_time, raw_data
  - Indexes: source_type, publish_time, source_name

**Files**:
- `src/data/models/message.py` - Message data model
- `src/data/database/message_db.py` - SQLite database layer
- `src/data/sources/base.py` - Base source interface
- `src/data/sources/registry.py` - Source registry
- `src/data/sources/announcement.py` - Announcement source
- `src/data/sources/news.py` - News source
- `src/data/sources/social.py` - Social media source
- `src/data/services/message_service.py` - Main service
- `src/common/config.py` - Configuration loader
- `config/message-config.yaml` - Module configuration
- `scripts/run_message_service.py` - Startup script

**Usage**:
```bash
# Start the message service
uv run python scripts/run_message_service.py

# With custom config
uv run python scripts/run_message_service.py --config config/message-config.yaml
```

**Acceptance Criteria**:
- [x] Plugin architecture for message sources
- [x] Dynamic source add/remove at runtime
- [x] Async stream-based message fetching
- [x] SQLite database storage
- [x] YAML configuration support
- [x] Sample implementations for all three source types

---

### [DAT-004] Real Data Sources

**Status**: Completed

**Description**: Production-ready data sources for fetching real financial news and announcements.

**Requirements**:
- Akshare announcements: All A-share stock announcements from East Money (全部/重大事项/财务报告/融资公告/风险提示/资产重组/信息变更/持股变动)
- CLS (财联社): Real-time financial telegraph
- East Money (东方财富): Global financial news
- Sina Finance (新浪财经): Financial news
- Historical batch fetch support for initial data population
- Content-based deduplication to prevent duplicate messages
- Comprehensive tests to detect API failures

**Technical Design**:
- Libraries:
  - `akshare` for all data sources (announcements, CLS, East Money, Sina news)
- Deduplication:
  - Content-based ID using SHA256(source_name + title + publish_time)
  - LRU cache (10,000 entries) for session deduplication
  - SQLite UNIQUE constraint for cross-session deduplication
- Historical Fetch:
  - `fetch_historical(days)` method for batch data retrieval
  - Configurable via `message.historical.days` in YAML

**Files**:
- `src/data/sources/akshare_announcement.py` - A-share stock announcements (via akshare/East Money)
- `src/data/sources/cls_news.py` - CLS telegraph
- `src/data/sources/eastmoney_news.py` - East Money news
- `src/data/sources/sina_news.py` - Sina news
- `tests/unit/data/sources/test_*.py` - Tests for each source

**Testing**:
Each source includes 4 types of tests:
1. Connectivity test - API accessibility
2. Data format test - Response structure validation
3. Deduplication test - Duplicate filtering
4. Error handling test - Network error recovery

Run tests:
```bash
# All source tests
uv run pytest tests/unit/data/sources/ -v

# Skip live API tests in CI
uv run pytest tests/unit/data/sources/ -v -m "not live"

# Run only live tests (for debugging)
uv run pytest tests/unit/data/sources/ -v -m live
```

**Acceptance Criteria**:
- [x] Akshare announcement source implemented (全部公告类型)
- [x] CLS news source implemented
- [x] East Money news source implemented
- [x] Sina news source implemented
- [x] Content-based deduplication
- [x] Historical batch fetch support
- [x] Unit tests for all sources
- [x] Live connectivity tests (marked with @pytest.mark.live)

---

### [DAT-005] Limit-Up Stocks Data Collection

**Status**: Completed

**Description**: Collect daily limit-up (涨停) stock information after market close using iFinD API, storing the data in SQLite for analysis and strategy development.

**Requirements**:
- Fetch all limit-up stocks for a given trading day after market close (15:00)
- Capture comprehensive limit-up information: stock code, name, price, time, reason, etc.
- Store data in SQLite database with proper indexing
- Support historical data backfill
- Idempotent: re-running for the same date updates existing records

**Technical Design**:
- Data Source: iFinD `THS_DataPool` API for limit-up board data
- Database: SQLite (separate from messages, in `data/limit_up.db`)
- Table Schema:
  - `limit_up_stocks`:
    - `id` (TEXT PRIMARY KEY) - Composite: date + stock_code
    - `trade_date` (TEXT) - Trading date (YYYY-MM-DD)
    - `stock_code` (TEXT) - Stock code (e.g., "000001.SZ")
    - `stock_name` (TEXT) - Stock name
    - `limit_up_price` (REAL) - Limit-up price
    - `limit_up_time` (TEXT) - First limit-up time (HH:MM:SS)
    - `open_count` (INTEGER) - Number of times limit opened
    - `last_limit_up_time` (TEXT) - Last limit-up time if reopened
    - `turnover_rate` (REAL) - Turnover rate percentage
    - `amount` (REAL) - Trading amount (yuan)
    - `circulation_mv` (REAL) - Circulating market value
    - `reason` (TEXT) - Limit-up reason/concept
    - `industry` (TEXT) - Industry classification
    - `created_at` (TEXT) - Record creation timestamp
    - `updated_at` (TEXT) - Record update timestamp
  - Indexes: trade_date, stock_code, limit_up_time

**Files**:
- `src/data/models/limit_up.py` - LimitUpStock data model
- `src/data/database/limit_up_db.py` - SQLite database layer
- `src/data/sources/ifind_limit_up.py` - iFinD data source
- `tests/unit/data/sources/test_ifind_limit_up.py` - Test file
- `config/market-data-config.yaml` - Configuration
- `scripts/fetch_limit_up.py` - CLI script for fetching data

**Usage**:
```python
from src.data.sources.ifind_limit_up import IFinDLimitUpSource

# Fetch today's limit-up stocks
source = IFinDLimitUpSource()
await source.start()
stocks = await source.fetch_limit_up_stocks()  # Uses today's date
await source.stop()

# Fetch historical data
stocks = await source.fetch_limit_up_stocks(date="2026-01-24")

# Backfill multiple days
await source.backfill(days=30)
```

**Acceptance Criteria**:
- [x] LimitUpStock data model defined
- [x] SQLite database layer with async operations
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

**Status**: Planned

**Description**: Continuous integration pipeline for code quality.

**Requirements**:
- Lint check (ruff)
- Format check (black)
- Type check (mypy)
- Unit tests (pytest)
- Coverage report

**Acceptance Criteria**:
- [ ] GitHub Actions workflow configured
- [ ] All checks pass on main branch
- [ ] Coverage threshold enforced

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
