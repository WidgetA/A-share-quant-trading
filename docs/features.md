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
| 0.1.4 | 2026-01-27 | - | DAT-004: Real data sources (baostock, CLS, eastmoney, sina) |

---

## Module: Strategy

### [STR-001] Base Strategy Interface

**Status**: Planned

**Description**: Define the base interface that all trading strategies must implement.

**Requirements**:
- Strategies must implement `generate_signal(market_data) -> Signal`
- Strategies must be loadable at runtime without system restart
- Support strategy parameter configuration via YAML

**Acceptance Criteria**:
- [ ] Base strategy class defined
- [ ] Signal data model defined (BUY/SELL/HOLD with metadata)
- [ ] Strategy hot-reload mechanism implemented

---

### [STR-002] Strategy Hot-Reload

**Status**: Planned

**Description**: Enable updating strategies during market hours without stopping the trading system.

**Requirements**:
- Trading module subscribes to strategy update events
- New strategy takes effect on next signal generation cycle
- Rollback mechanism if new strategy fails validation

**Acceptance Criteria**:
- [ ] Strategy file change detection
- [ ] Graceful strategy swap without order interruption
- [ ] Strategy validation before activation

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
- Baostock announcements: Performance reports and forecasts
- CLS (财联社): Real-time financial telegraph
- East Money (东方财富): Global financial news
- Sina Finance (新浪财经): Financial news
- Historical batch fetch support for initial data population
- Content-based deduplication to prevent duplicate messages
- Comprehensive tests to detect API failures

**Technical Design**:
- Libraries:
  - `akshare` for CLS, East Money, Sina news
  - `baostock` for announcements
- Deduplication:
  - Content-based ID using SHA256(source_name + title + publish_time)
  - LRU cache (10,000 entries) for session deduplication
  - SQLite UNIQUE constraint for cross-session deduplication
- Historical Fetch:
  - `fetch_historical(days)` method for batch data retrieval
  - Configurable via `message.historical.days` in YAML

**Files**:
- `src/data/sources/baostock_announcement.py` - Baostock announcements
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
- [x] Baostock announcement source implemented
- [x] CLS news source implemented
- [x] East Money news source implemented
- [x] Sina news source implemented
- [x] Content-based deduplication
- [x] Historical batch fetch support
- [x] Unit tests for all sources
- [x] Live connectivity tests (marked with @pytest.mark.live)

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
