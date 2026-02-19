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
| 0.6.0 | 2026-02-03 | - | STR-003: Use pre-analyzed messages from external project (no LLM calls needed) |
| 0.6.1 | 2026-02-03 | - | SYS-004: Feishu alert notifications for errors and critical events |
| 0.7.0 | 2026-02-03 | - | SYS-005: Web UI for trading confirmations (containerized deployment) |
| 0.7.1 | 2026-02-04 | - | SYS-004: Enhanced startup notification with git commit info for CD tracking |
| 0.8.0 | 2026-02-04 | - | SIM-001: Historical simulation trading feature |
| 0.9.0 | 2026-02-06 | - | OA-001: Order assistant (real-time news dashboard) |
| 0.10.0 | 2026-02-12 | - | STR-004: Momentum sector strategy (backtest + intraday alert) |
| 0.10.1 | 2026-02-19 | - | STR-004: Remove gap-fade filter (ineffective in validation, precision ~50%) |

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

### [SYS-004] Feishu Alert Notifications

**Status**: Completed

**Description**: Send Feishu (飞书) notifications when errors or critical events occur, enabling real-time monitoring of the trading system.

**Requirements**:
- Send alerts for exceptions and critical errors
- Send startup/shutdown notifications
- Configurable via environment variables
- Graceful degradation when Feishu is not configured
- Retry mechanism with exponential backoff

**Technical Design**:
- `FeishuBot` class for sending messages via external bot service
- Uses external Feishu bot relay service (LeapCell or self-hosted)
- Async HTTP requests with retry support
- Message types: text alerts, error alerts, startup/shutdown notifications

**Configuration** (Environment Variables):
| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `FEISHU_APP_ID` | No | - | Feishu app ID |
| `FEISHU_APP_SECRET` | No | - | Feishu app secret |
| `FEISHU_CHAT_ID` | No | - | Target chat ID for alerts |
| `FEISHU_BOT_URL` | No | `https://feishugroupbot-widgetinp950-g352rogo.leapcell.dev` | Bot relay service URL |

**Alert Types**:
| Type | Trigger | Format |
|------|---------|--------|
| Startup | System starts successfully | `✅ A股交易系统已启动\n⏰ 启动时间: {time}\n📦 版本: {commit_hash}` |
| Shutdown | System stops | `⚠️ A股交易系统已停止\n⏰ 停止时间: {time}` |
| Error | Exception or critical error | `🚨 {title}\n\n{content}` |

**CD Deployment Tracking** (v0.7.1):
- Git commit hash is embedded during Docker image build
- Startup notification includes commit info for deployment verification
- Enables tracking of which version is deployed after watchtower auto-update
- Version file: `VERSION` at image build time, read on startup

**Files**:
- `src/common/feishu_bot.py` - FeishuBot class
- `src/common/config.py` - get_feishu_config() function

**Usage**:
```python
from src.common.feishu_bot import FeishuBot

# Create bot instance
bot = FeishuBot()

# Check if configured
if bot.is_configured():
    # Send alert
    await bot.send_alert("Database Error", "Connection timeout after 30s")

    # Send startup notification
    await bot.send_startup_notification()

    # Send shutdown notification
    await bot.send_shutdown_notification()
```

**Integration Points**:
- `scripts/main.py` - Startup/shutdown notifications
- `src/common/state_manager.py` - State persistence errors
- `src/trading/repository.py` - Database connection errors
- `src/strategy/engine.py` - Strategy execution errors

**Acceptance Criteria**:
- [x] FeishuBot class with async HTTP support
- [x] Configuration via environment variables
- [x] Retry mechanism with exponential backoff
- [x] Graceful skip when not configured
- [x] Error alert method
- [x] Startup/shutdown notifications
- [x] Unit tests

---

### [SYS-005] Web UI for Trading Confirmations

**Status**: Completed

**Description**: Web-based user interaction for trading confirmations, replacing command-line stdin input to support containerized deployment.

**Requirements**:
- Web UI for premarket signal selection and morning sell confirmation
- Feishu notifications with Web links for intraday signals
- Timeout handling with configurable default behaviors
- Support both CLI mode (local development) and Web mode (container)

**Technical Design**:
- `WebUserInteraction` class extends `UserInteraction` for Web-based input
- `PendingConfirmationStore` manages pending confirmation requests
- FastAPI + Jinja2 for Web UI and API
- Strategy engine creates confirmation → waits for user response via Web
- Feishu notifications include clickable confirmation links

**Architecture**:
```
┌─────────────────────────────────────────────────────────────┐
│                   Trading System (Container)                 │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   FastAPI    │◄──►│  Pending     │◄──►│  Strategy    │  │
│  │   :8000      │    │  Store       │    │  Engine      │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                   │                    │          │
│         ▼                   ▼                    ▼          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  HTML Pages  │    │  Feishu Bot  │    │  Web User    │  │
│  │              │    │  (通知+链接) │    │  Interaction │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**Interaction Points**:
| Interaction | Time | Notification | Confirmation |
|-------------|------|--------------|--------------|
| Premarket signal selection | 08:30 | - | Web UI |
| Morning sell confirmation | 09:00 | - | Web UI |
| Intraday buy confirmation | Trading hours | Feishu | Web UI |
| Limit-up stock selection | Trading hours | Feishu | Web UI |

**API Endpoints**:
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `GET /` | GET | Main dashboard (pending list) |
| `GET /confirm/{id}` | GET | Confirmation page |
| `GET /api/pending` | GET | List pending confirmations |
| `GET /api/pending/{id}` | GET | Get confirmation details |
| `POST /api/pending/{id}/submit` | POST | Submit user decision |
| `GET /api/status` | GET | Health check |

**Configuration** (Environment Variables):
| Variable | Default | Description |
|----------|---------|-------------|
| `WEB_ENABLED` | `true` | Enable/disable Web UI |
| `WEB_HOST` | `0.0.0.0` | Bind host |
| `WEB_PORT` | `8000` | Bind port |
| `WEB_BASE_URL` | auto | Base URL for notification links |
| `INTERACTION_MODE` | `web` | `web` or `cli` |

**Timeout Defaults** (unchanged from CLI):
| Type | Timeout | Default Action |
|------|---------|----------------|
| premarket | 5 min | Skip all signals |
| intraday | 1 min | Don't buy |
| morning | 5 min | Sell all holdings |
| limit_up | 1 min | Skip sector |

**Files**:
- `src/common/pending_store.py` - Pending confirmation store
- `src/common/user_interaction.py` - Extended with `WebUserInteraction`
- `src/web/__init__.py` - Web module
- `src/web/app.py` - FastAPI application
- `src/web/routes.py` - API routes
- `src/web/templates/` - Jinja2 HTML templates
- `src/web/static/` - CSS styles

**Usage**:
```python
# Web mode (container deployment)
from src.common.user_interaction import WebUserInteraction, WebInteractionConfig
from src.common.pending_store import get_pending_store

config = WebInteractionConfig(web_base_url="http://your-server:8000")
ui = WebUserInteraction(config, get_pending_store(), feishu_bot)

# Same interface as UserInteraction
selected = await ui.premarket_review(signals)
confirmed = await ui.intraday_confirm(signal)
```

**Docker Deployment**:
```yaml
# docker-compose.prod.yml
services:
  trading-service:
    ports:
      - "8000:8000"  # Web UI port
    environment:
      - WEB_ENABLED=true
      - WEB_BASE_URL=http://your-server:8000
      - INTERACTION_MODE=web
```

**Acceptance Criteria**:
- [x] PendingConfirmationStore for confirmation state management
- [x] FastAPI Web application with API endpoints
- [x] HTML templates for confirmation pages
- [x] WebUserInteraction class extending UserInteraction
- [x] Feishu notifications with Web confirmation links
- [x] Docker configuration with port exposure
- [x] Environment variable configuration
- [x] Timeout handling with default behaviors

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

**Description**: News-driven trading strategy that uses pre-analyzed messages from external message collection project. Analysis results (sentiment, confidence, reasoning) are stored in `message_analysis` table and read via MessageReader JOIN.

**Architecture Change (v0.6.0)**:
- **Before**: Strategy called LLM (Silicon Flow Qwen) to analyze each message
- **After**: External project analyzes messages; this project only filters and acts on results
- **Benefit**: Faster execution (no API calls), analysis quality controlled by external project

**Requirements**:
- Filter positive messages (bullish/strong_bullish) at 8:30 AM (premarket)
- Real-time monitoring for new positive messages during trading hours
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
- Message Model: `Message` with `MessageAnalysis` containing sentiment, confidence, reasoning
- MessageReader: JOINs `messages` with `message_analysis` table to get pre-analyzed results
- News Analyzer: Filters messages by sentiment (bullish/strong_bullish) and confidence threshold
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

**Sentiment Values** (from external `message_analysis` table):
| Value | Meaning | Action |
|-------|---------|--------|
| `strong_bullish` | 强利好 | Buy signal |
| `bullish` | 利好 | Buy signal |
| `neutral` | 中性 | Ignore |
| `bearish` | 利空 | Ignore |
| `strong_bearish` | 强利空 | Ignore |

**Files**:
- `src/data/models/message.py` - Message and MessageAnalysis models
- `src/data/readers/message_reader.py` - PostgreSQL reader with analysis JOIN
- `src/common/user_interaction.py` - Command-line user interaction
- `src/strategy/filters/stock_filter.py` - Exchange-based stock filtering
- `src/strategy/analyzers/news_analyzer.py` - Filters pre-analyzed messages
- `src/data/sources/sector_mapper.py` - Stock-to-sector mapping
- `src/trading/position_manager.py` - Slot-based position management
- `src/trading/holding_tracker.py` - Overnight holding tracking
- `src/strategy/strategies/news_analysis_strategy.py` - Main strategy implementation
- `config/news-strategy-config.yaml` - Strategy configuration

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

# Generate signals (uses pre-analyzed messages from DB)
async for signal in strategy.generate_signals(context):
    print(f"Signal: {signal}")
```

**Configuration** (`config/news-strategy-config.yaml`):
```yaml
strategy:
  news_analysis:
    enabled: true
    analysis:
      min_confidence: 0.7
      actionable_sentiments: ["strong_bullish", "bullish"]
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
- [x] Message model with analysis fields (sentiment, confidence, reasoning)
- [x] MessageReader JOINs message_analysis table
- [x] Stock filter excluding BSE/ChiNext
- [x] Sector mapper using akshare
- [x] News analyzer filters pre-analyzed messages
- [x] Position manager with slot allocation
- [x] Holding tracker for overnight positions
- [x] User interaction via command line
- [x] Main strategy implementation
- [x] Configuration file
- [x] Limit-up price check before buying
- [x] User confirmation when sector has limit-up stocks

---

### [STR-004] Momentum Sector Strategy (动量板块策略)

**Status**: In Progress

**Description**: Identifies "hot" concept boards by finding stocks with momentum after open, then selects constituent gainers from those boards.

**Strategy Flow**:
1. **Pre-filter (iwencai)**: Get main board non-ST stocks with opening gain > -0.5% (broad candidate pool)
2. **9:40 Filter**: Keep stocks where (9:40 price - open) / open > 0.56%
3. **Reverse Concept Lookup**: For each qualified stock, find its concept boards via iwencai, filter junk boards
4. **Hot Board Detection**: Find boards containing ≥2 qualified stocks from step 2
5. **Board Constituents**: Get ALL stocks in each hot board
6. **Gain Filter**: Select constituents with 9:40 gain from open >0.56%
7. **Recommend (推股)**: From the board with the most selected stocks, pick the one with highest earnings growth (同比季度收入增长率 via iwencai). Highlighted in UI + Feishu notification
9. **Notification**: Send selection + recommendation via Feishu

**Data Sources**:
- Price (backtest): iFinD `history_quotes` + `high_frequency` (9:40 price)
- Price (live): iFinD `real_time_quotation`
- Concept boards: iFinD iwencai (`smart_stock_picking`)

**Key Files**:
- `src/strategy/strategies/momentum_sector_scanner.py` — Core scanner logic
- `src/data/sources/concept_mapper.py` — Stock ↔ concept board mapping
- `src/data/database/fundamentals_db.py` — stock_fundamentals reader
- `scripts/backtest_momentum.py` — Backtest script
- `scripts/intraday_momentum_alert.py` — Live monitoring + Feishu alert

**Backtest Modes**:

| Mode | Description |
|------|-------------|
| **Single-day** | Run strategy for one date, show selected stocks + recommendation |
| **Range** | Run strategy for a date range (max 90 trading days), simulate daily buy/sell with real costs |

**Range Backtest Details**:
- Input: start date, end date, initial capital (yuan)
- Each trading day: run strategy → buy recommended stock at 9:40 price, sell at next day's open
- Trading costs:
  - **Commission**: 0.3% of trade amount, minimum 5 yuan (both buy and sell)
  - **Transfer fee**: 0.001% (1/100,000) of trade amount (both buy and sell)
  - **Stamp tax**: 0.05% (0.5/1,000) of trade amount (sell only)
- Minimum trading unit: 1 lot (100 shares)
- Buy with max affordable lots each day
- If no recommendation or capital insufficient for 1 lot: skip day
- SSE streaming for real-time progress display
- Results: per-day trade details + summary (total return, win rate, etc.)

**Checklist**:
- [x] StockFilter: add `exclude_sme` + `create_main_board_only_filter()`
- [x] FundamentalsDB: read-only access to stock_fundamentals table
- [x] ConceptMapper: iwencai-based stock-to-board and board-to-stock lookups
- [x] MomentumSectorScanner: 5-step pipeline + step 6 recommendation
- [x] Feishu notification: `send_momentum_scan_result()` with recommendation
- [x] Backtest script with `--notify` option
- [x] Intraday monitoring script (polls 9:30-9:40)
- [x] Range backtest with trading cost simulation (SSE streaming)
- [x] Tab-based UI for single-day vs range backtest
- [ ] Unit tests

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

## Module: Simulation

### [SIM-001] Historical Simulation Trading

**Status**: Completed

**Description**: Replay historical trading days using stored messages and price data. Allows users to practice trading decisions in a fast-forward mode and calculate P&L.

**Requirements**:
- Select a historical date to start simulation
- Optionally load historical position snapshot
- Replay messages based on simulated time
- Fast-forward through trading day phases (no real-time waiting)
- User interaction for stock selection and sell decisions via Web UI
- Calculate and display P&L at simulation end

**Technical Design**:
- **SimulationClock**: Virtual time controller that can jump to key time points
- **HistoricalPriceService**: Fetches historical OHLCV from iFinD
- **HistoricalMessageReader**: Wraps MessageReader to filter by simulated time
- **SimulationContext**: Extended StrategyContext for simulation mode
- **SimulationPositionManager**: Isolated position manager (no DB persistence)
- **SimulationManager**: Main orchestrator for simulation lifecycle

**Architecture**:
```
┌─────────────────────────────────────────────────────────────────┐
│                    Historical Simulation System                  │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐    ┌────────────────────────┐            │
│  │ SimulationClock  │◄──►│ HistoricalPriceService │            │
│  │ (虚拟时间控制)   │    │ (iFinD历史价格)        │            │
│  └────────┬─────────┘    └────────────────────────┘            │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────┐    ┌────────────────────────┐            │
│  │ SimulationContext│◄──►│HistoricalMessageReader │            │
│  │ (模拟上下文)     │    │(按模拟时间过滤消息)    │            │
│  └────────┬─────────┘    └────────────────────────┘            │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────┐    ┌────────────────────────┐            │
│  │SimulationManager │◄──►│ Web UI (/api/simulation)│            │
│  │ (主协调器)       │    └────────────────────────┘            │
│  └────────┬─────────┘                                          │
│           │                                                     │
│           ▼                                                     │
│  ┌──────────────────┐                                          │
│  │SimPositionManager│                                          │
│  │ (隔离的仓位管理) │                                          │
│  └──────────────────┘                                          │
└─────────────────────────────────────────────────────────────────┘
```

**Simulation Phases**:
| Phase | Time | Description |
|-------|------|-------------|
| PREMARKET_ANALYSIS | 08:30 | Review overnight messages, select signals |
| MORNING_AUCTION | 09:25 | Execute pending buys, check limit-up |
| TRADING_HOURS | 09:30-15:00 | Monitor positions, check intraday messages for buying opportunities |
| MARKET_CLOSE | 15:00 | Day summary, P&L calculation, decide sell/hold for next-day open |
| MORNING_CONFIRMATION | 09:00 (next day) | Decide sell/hold for positions, sell at next-day opening price |
| COMPLETED | - | Simulation finished, show results |

**Trading Hours Actions**:
During trading hours, users can:
- View and select from historical messages for buying
- Check intraday messages for new buying opportunities
- Fast forward to market close

**API Endpoints**:
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/simulation/start` | POST | Start new simulation |
| `/api/simulation/state` | GET | Get current state |
| `/api/simulation/advance` | POST | Advance to next phase |
| `/api/simulation/select` | POST | Submit signal selection (premarket) |
| `/api/simulation/sell` | POST | Submit sell decision (market close or morning confirmation) |
| `/api/simulation/intraday/select` | POST | Select intraday signals for buying |
| `/api/simulation/intraday/skip` | POST | Skip intraday messages |
| `/api/simulation/messages` | GET | Get historical messages |
| `/api/simulation/messages/select` | POST | Select messages for buying |
| `/api/simulation/result` | GET | Get final result |
| `/api/simulation/sync` | POST | Sync results to database |
| `/api/simulation` | DELETE | Cancel simulation |

**Files**:
- `src/simulation/__init__.py` - Module exports
- `src/simulation/models.py` - Data models (phases, state, result)
- `src/simulation/clock.py` - Virtual time controller
- `src/simulation/historical_price_service.py` - iFinD historical prices
- `src/simulation/historical_message_reader.py` - Time-filtered messages
- `src/simulation/context.py` - Simulation context
- `src/simulation/position_manager.py` - Isolated position manager
- `src/simulation/manager.py` - Main orchestrator
- `src/web/routes.py` - API routes (extended)
- `config/simulation-config.yaml` - Simulation configuration

**Usage**:
```bash
# Start simulation via API
curl -X POST http://localhost:8000/api/simulation/start \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2026-01-29", "num_days": 2}'

# Get current state
curl http://localhost:8000/api/simulation/state

# Select signals (1-based indices)
curl -X POST http://localhost:8000/api/simulation/select \
  -d '{"selected_indices": [1, 2]}'

# Advance to next phase
curl -X POST http://localhost:8000/api/simulation/advance

# Get final result
curl http://localhost:8000/api/simulation/result
```

**Configuration** (`config/simulation-config.yaml`):
```yaml
simulation:
  default_capital: 10000000
  num_slots: 5
  premarket_slots: 3
  intraday_slots: 2
  min_order_amount: 10000
  lot_size: 100
  price_source: "ifind"
```

**Acceptance Criteria**:
- [x] SimulationClock with virtual time control
- [x] HistoricalPriceService with iFinD integration
- [x] HistoricalMessageReader with time filtering
- [x] SimulationContext compatible with StrategyContext
- [x] SimulationPositionManager with transaction tracking
- [x] SimulationManager orchestrating all components
- [x] Web API endpoints for simulation control
- [x] Configuration file
- [x] P&L calculation and result display

---

### [OA-001] Order Assistant (Real-time News Dashboard)

**Status**: Completed

**Description**: Real-time news analysis dashboard for manual trading decisions. Shows pre-analyzed messages from the database based on current Beijing time. No position management — purely informational.

**Requirements**:
- Auto-detect market phase based on Beijing time (pre-market / trading / closed)
- Pre-market mode (before 9:30): show overnight messages (last trading day 15:00 to now)
- Trading mode (9:30–15:00): show both pre-market (static) and intraday messages (auto-refresh every 30s)
- After hours (after 15:00): show both sections statically
- Same message card design as simulation (sentiment badges, stock tags, confidence %, reasoning)
- Filter: "仅显示正面消息" toggle per section
- Pagination: "加载更多" button per section
- No position management, no buying/selling — read-only display

**Technical Design**:
- `create_order_assistant_router()` factory function (same pattern as simulation)
- Lazy-init `MessageReader` singleton for real-time DB access
- Uses `MessageReader.get_messages_in_range()` and `count_messages_in_range()` directly
- Phase detection based on `datetime.now(ZoneInfo("Asia/Shanghai"))`
- Frontend `setInterval` polling for intraday auto-refresh
- Stock name lookup via `SectorMapper`

**Files**:
- `src/web/routes.py` - Order assistant routes (`create_order_assistant_router()`)
- `src/web/templates/order_assistant.html` - Page template
- `src/web/templates/base.html` - Nav link added
- `src/web/static/style.css` - Additional styles
- `src/web/app.py` - Router registration

**API Endpoints**:
```
GET  /order-assistant                     - Page (HTML)
GET  /api/order-assistant/state           - Current phase and time info (JSON)
GET  /api/order-assistant/messages        - Messages with pagination (JSON)
     ?mode=premarket|intraday
     &only_positive=false
     &limit=50
     &offset=0
```

**Acceptance Criteria**:
- [x] Phase auto-detection based on Beijing time
- [x] Pre-market message display with analysis
- [x] Intraday message display with auto-refresh
- [x] Filter and pagination support
- [x] Same message card design as simulation
- [x] Navigation link in header

---

## Backlog

Features under consideration (not yet planned):

- [x] Web dashboard for monitoring → See SYS-005
- [ ] Telegram/WeChat notifications
- [x] Backtesting framework → See SIM-001 (Historical Simulation)
- [x] Order assistant → See OA-001
- [ ] Multi-account support
- [ ] Performance analytics
