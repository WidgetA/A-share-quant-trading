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
| 0.10.2 | 2026-02-20 | - | STR-004: Sync docs with code (fix scoring formula, add Step 5.5/5.6/limit-up docs) |
| 0.10.3 | 2026-03-05 | - | STR-004: Add Step 5.7 LLM board relevance filter + flip scoring to +Z(gfo)+Z(amp) + board leader bonus |
| 0.11.0 | 2026-03-18 | - | SYS-005/STR-005: Replace Strategy Status with iQuant connection status on dashboard, remove strategy controller |
| 0.11.1 | 2026-03-18 | - | SYS-005: Inline download log + calendar on backtest page, fix download_prices call, fix monitoring startup order |
| 0.11.1 | 2026-03-18 | - | SYS-005: Isolate dashboard and iQuant trading caches — dashboard download no longer affects live trading |
| 0.11.4 | 2026-03-20 | - | SYS-005: Share single cache on startup (halve memory); only copy on manual download to protect trading |
| 0.11.2 | 2026-03-19 | - | ~~SYS-005: OSS cache prefix~~ (obsolete — replaced by GreptimeDB in 0.12.0) |
| 0.12.0 | 2026-03-24 | - | SYS-005: Replace pickle+OSS backtest cache with GreptimeDB (asyncpg pgwire), no in-memory caching |
| 0.11.3 | 2026-03-19 | - | SYS-005: Real-time download progress (asyncio.Queue SSE), descriptive Chinese logs, stop download button |
| 0.12.1 | 2026-03-24 | - | SYS-005: Dashboard cache scheduler status card with toggle, next run time, last result display |
| 0.12.2 | 2026-03-26 | - | SYS-005: Cache scheduler download timeout (4h/range), per-range Feishu progress, align manual/scheduler gap detection |
| 0.13.0 | 2026-04-06 | - | STR-006: ML Scanner — 8-layer filter + LightGBM LambdaRank scoring, model management (train/finetune/S3/scheduler) |
| 0.13.1 | 2026-04-07 | - | STR-006: FC serverless async training (X-Fc-Invocation-Type: Async), remove local training code |
| 0.13.2 | 2026-04-08 | - | STR-006: Wire up ML inference — replace V15 momentum scan with LightGBM scoring (live + backtest) |
| 0.13.3 | 2026-04-17 | - | DAT-001: Fix silent skip of suspended stocks without prev_close in daily cache — always write is_suspended=true row |
| 0.13.4 | 2026-04-28 | - | STR-006: Stock-level blacklist — hardcoded codes removed at top of funnel (live scan + replicate_v16) and stripped from training data stream |
| 0.14.0 | 2026-05-04 | - | ANA-001: K-line technical analysis via overseas Lambda renderer + 柏拉图AI vision LLM (POST /api/analyze-kline) |

---

## Module: System

### [SYS-001] Application Entry Point

**Status**: Completed

**Description**: FastAPI web application as the sole entry point, serving dashboard UI, backtest API, iQuant trading API, and background schedulers.

**Entry Point**:
```bash
uv run uvicorn src.web.app:create_app --factory --host 0.0.0.0 --port 8000
```

**Startup Flow** (`src/web/app.py` → `create_app()`):
1. Configure root logger (idempotent for pytest)
2. Mount 6 routers (main, momentum, settings, trade-backtest, trading, model)
3. Initialize GreptimeDB storage + CachePipeline
4. Start background schedulers: iQuant monitoring, cache scheduler (3am), model training scheduler
5. Run trading safety audit
6. Send Feishu startup notification

**Files**:
- `src/web/app.py` - FastAPI application factory
- `Dockerfile` - Container build (uvicorn CMD)

---

### [SYS-002] State Management

**Status**: Completed

**Description**: Lightweight state management using GreptimeDB, JSON files, and in-memory stores.

**State Storage**:

| State | Storage | File/Table |
|-------|---------|------------|
| Backtest cache (daily/minute OHLCV) | GreptimeDB | `backtest_daily`, `backtest_minute`, `stock_list` |
| iQuant holdings | JSON file | `data/v15_holdings.json` |
| Trading signals | In-memory | `SignalStore` (no persistence — signals expire) |
| Configuration | Environment vars + YAML | `config/secrets.yaml`, `config/database-config.yaml` |
| ML models | Local files + S3 | `data/models/*.lgb` |
| Cache scheduler state | Disk file | `data/cache_scheduler_enabled.txt`, `data/last_finetune_date.txt` |

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
- `src/web/app.py` - Startup/shutdown notifications
- `src/web/iquant_routes.py` - iQuant signal/heartbeat alerts
- `src/data/services/cache_pipeline.py` - Download progress/failure alerts
- `src/data/services/model_training_scheduler.py` - Training success/failure alerts

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

**Description**: FastAPI web application serving dashboard UI, backtest tools, iQuant trading API, and background schedulers. All user interaction via browser; Feishu for push notifications.

**Architecture**:
```
┌─────────────────────────────────────────────────────────────┐
│                   Trading System (Container)                 │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   FastAPI    │◄──►│  Pending     │◄──►│  ML/Momentum │  │
│  │   :8000      │    │  Store       │    │  Services    │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                   │                    │          │
│         ▼                   ▼                    ▼          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  HTML Pages  │    │  Feishu Bot  │    │  SignalStore  │  │
│  │  (Jinja2)    │    │  (通知+链接) │    │  (push/poll) │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**Pages**:

| Page | URL | Content |
|------|-----|---------|
| Dashboard | `/` | iQuant connection status, broker positions/cash, data engine status, model management, recommendations |
| Backtest | `/backtest` | Single-day scan, range backtest (SSE), CSV analysis |
| Settings | `/settings` | tsanghi token, cache scheduler toggle, FC URL, S3 config |
| Database | `/database` | Embedded GreptimeDB dashboard |

**Key API Endpoints**:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/status` | GET | Health check |
| `/api/iquant/status` | GET | iQuant connection status |
| `/api/trading/recommendations` | GET | On-demand ML scan results (top-10) |
| `/api/momentum/tsanghi-prepare` | POST | Download cache data (SSE stream) |
| `/api/momentum/backtest` | POST | Single-day momentum scan |
| `/api/momentum/combined-analysis` | POST | Range backtest (SSE stream) |
| `/api/model/full-train` | POST | Trigger full ML training (SSE stream) |
| `/api/model/finetune` | POST | Trigger ML fine-tuning (SSE stream) |
| `/api/iquant/pending-signals` | GET | iQuant polls for signals |
| `/api/iquant/ack-signal` | POST | iQuant acknowledges signal execution |
| `/api/iquant/heartbeat` | POST | iQuant heartbeat + positions/cash sync |

**Configuration** (Environment Variables):

| Variable | Default | Description |
|----------|---------|-------------|
| `WEB_BASE_URL` | auto | Base URL for Feishu notification links |
| `GREPTIME_HOST` | `localhost` | GreptimeDB host |
| `FEISHU_BOT_URL` | (leapcell) | Feishu bot relay URL |
| `FEISHU_APP_ID` / `FEISHU_APP_SECRET` / `FEISHU_CHAT_ID` | - | Feishu credentials |
| `FC_URL` | - | Alibaba Cloud FC training endpoint |

**Files**:
- `src/web/app.py` - FastAPI application factory + startup lifecycle
- `src/web/routes.py` - Dashboard, backtest, settings, model management routes
- `src/web/iquant_routes.py` - iQuant communication + monitoring (isolated, own DB pool)
- `src/common/pending_store.py` - Pending confirmation store
- `src/web/templates/` - Jinja2 HTML templates
- `src/web/static/` - CSS styles

**Docker Deployment**:
```yaml
services:
  trading-service:
    ports:
      - "8000:8000"
    environment:
      - WEB_BASE_URL=http://your-server:8000
      - GREPTIME_HOST=greptimedb
```

**Checklist**:
- [x] FastAPI Web application with 6 routers
- [x] Dashboard: iQuant status, broker positions, data engine card, model management
- [x] Backtest page: single-day scan, range backtest, CSV analysis
- [x] Settings page: tsanghi token, cache scheduler, FC URL, S3 config
- [x] Database page: embedded GreptimeDB dashboard
- [x] On-demand recommendations (ML scan, no DB persistence)
- [x] SSE streaming for downloads, training, backtests
- [x] Feishu notifications with clickable links
- [x] Docker deployment with environment variable config

---

## Module: Strategy

### [STR-001] Strategy Architecture

**Status**: Completed

**Description**: Stateless strategy services with dependency injection. Scanners produce scored stock lists; strategy services handle data preparation and scanner invocation.

**Architecture**:
```
iquant_routes.py / routes.py
        │
        ▼
ml_strategy_service.py / momentum_strategy_service.py   (data prep + invocation)
        │
        ▼
ml_scanner.py / momentum_scanner.py                      (filter pipeline + scoring)
        │
        ▼
SignalStore                                               (push signal → iQuant polls)
```

**Key Components**:

| Component | File | Role |
|-----------|------|------|
| `MLScanner` | `src/strategy/strategies/ml_scanner.py` | 8-layer filter + LightGBM scoring |
| `MomentumScanner` | `src/strategy/strategies/momentum_scanner.py` | 7-layer funnel + V3 regression scoring |
| `MLStrategyService` | `src/strategy/ml_strategy_service.py` | Stateless: `run_ml_live()` / `run_ml_backtest()` |
| `MomentumStrategyService` | `src/strategy/momentum_strategy_service.py` | Stateless: `run_momentum_live()` / `run_momentum_backtest()` |
| `SignalStore` | `src/strategy/signal_store.py` | In-memory signal queue (push/poll/ack/expire lifecycle) |
| `EarlyWindowAggregator` | `src/strategy/aggregators/early_window_aggregator.py` | Aggregates raw 1-min bars → 09:31~09:40 snapshot |

**Shared Data Models** (`src/strategy/models.py`):
- `PriceSnapshot` - 9:40 snapshot (close, volume, high, low)
- `DailyBar` - Daily OHLCV NamedTuple
- `HistoricalDataProvider` - Protocol for historical data access

**Files**:
- `src/strategy/base.py` - BaseStrategy abstract class
- `src/strategy/signals.py` - TradingSignal and SignalType
- `src/strategy/filters/` - Stock/quality/reversal filters

---

### [STR-003] News Analysis Strategy

**Status**: Removed (superseded by STR-005/006)

---

### [STR-004] Momentum Sector Strategy (动量板块策略)

**Status**: In Progress

**Description**: Identifies "hot" concept boards by finding stocks with momentum after open, then selects constituent gainers from those boards.

**Strategy Flow**:
1. **Pre-filter (iwencai)**: Get main board non-ST stocks with opening gain > -0.5% (broad candidate pool)
2. **Step 1 — 9:40 Filter**: Keep stocks where (9:40 price - open) / open > 0.56%, main board, non-ST
3. **Step 2 — Reverse Concept Lookup**: For each qualified stock, find its concept boards from local `data/board_constituents.json`, filter junk boards
4. **Step 3 — Hot Board Detection**: Find boards containing ≥2 qualified stocks from step 1
5. **Step 4 — Board Constituents**: Get ALL stocks in each hot board from local `data/board_constituents.json`
6. **Step 5 — Gain Filter**: Select constituents with 9:40 gain from open >0.56%, main board, non-ST
7. **Step 5.5 — Momentum Quality Filter**: Remove stocks with abnormal early volume. Upper bound: turnover_amp > 3.0x (冲高回落 risk, based on 9-month study: >3x → avg return negative, win rate 42.9%). Lower bound: turnover_amp < 0.4x (缩量弱势, based on fine-grained sweep across 5/7/15/20d windows: Bootstrap median 0.42~0.47x, kept-group return +0.020% improvement). Turnover amp = early_volume (9:40 cumulative) / (avg_daily_volume × 0.125). Also computes trend_pct, consecutive_up_days, avg_daily_volume for Step 6
8. **Step 5.6 — Reversal Factor Filter**: Remove stocks showing 冲高回落 at 9:40 — early fade (gave back >70% of intraday surge from high) OR price position in bottom 25% of 10-min range
9. **Step 6 — Recommend (推股)**: Across all candidates:
   - Exclude stocks already at limit-up (9:40 price ≥ prev_close × 1.10)
   - Composite score = +Z(gain_from_open) + Z(turnover_amp) - cup_penalty - trend_penalty + leader_bonus
   - 涨幅靠前 + 量能充足 = 板块龙头优先（翻转自旧公式，旧公式选最弱票导致连续亏损）
   - Board leader bonus: 每个板块内 gain_from_open 最高的票加 +0.5
   - Cup penalty: 连涨天数 × 0.3（软惩罚，趋势疲劳）
   - Trend penalty: max(0, 5日累计涨幅%) × 0.05（软惩罚，高位追涨风险）
   - Pick the highest-scored stock. Highlighted in UI + Feishu notification
10. **Notification**: Send selection + recommendation via Feishu

**Data Sources**:
- Price (backtest): 日线 from tsanghi 沧海数据, 9:40 快照 from Tushare Pro `stk_mins` 1min (聚合 09:31~09:40 by `EarlyWindowAggregator`), via `GreptimeBacktestStorage` + `CachePipeline`
- Price (live): Tushare Pro `rt_min_daily`
- Concept boards: Local JSON files (`data/sectors.json` + `data/board_constituents.json`), zero runtime API calls

**Key Files**:
- `src/strategy/strategies/momentum_scanner.py` — Legacy momentum scanner (replaced by ml_scanner.py in STR-006)
- `src/strategy/filters/momentum_quality_filter.py` — Step 5.5: volume filter (turnover_amp bounds)
- `src/strategy/filters/reversal_factor_filter.py` — Step 5.6: 冲高回落 filter
- `src/data/sources/local_concept_mapper.py` — Stock ↔ concept board mapping + stock names (reads local JSON)
- `data/sectors.json` — THS concept board name list (390 boards)
- `data/board_constituents.json` — Board → constituent stocks mapping (pre-downloaded)

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
- [x] LocalConceptMapper: local JSON-based stock-to-board and board-to-stock lookups
- [x] MomentumScanner: 7-layer funnel + V3 regression scoring (replaced MomentumSectorScanner)
- [x] MomentumQualityFilter: Step 5.5 volume filter (turnover_amp bounds)
- [x] ReversalFactorFilter: Step 5.6 冲高回落 filter (early fade + price position)
- [x] Feishu notification: `send_momentum_scan_result()` with recommendation
- [x] Range backtest with trading cost simulation (SSE streaming)
- [x] Unified backtest page with 3 tabs (single-day, range, CSV analysis)
- [x] Cache management scheduler (3am daily auto-fill)
- [x] Cache scheduler toggle on Settings page (enable/disable, persisted to disk)
- [x] Cache download resume: skip already-downloaded daily dates and minute stocks on retry
- [x] Cache scheduler detects minute data gaps (not just daily gaps)
- [x] Per-day minute audit + backfill: after the coarse per-stock minute download, the pipeline audits each stock's bar count per day against the expected 241 bars (9:30-15:00, verified via Tushare API). Stocks with zero bars (missing) or fewer than 241 bars (incomplete, e.g. only afternoon session) are refetched via single-day `stk_mins` calls
- [x] Cache scheduler Feishu notifications (all scenarios: start / progress / success / failure / timeout / exception / no_gaps / disabled)
- [x] Data integrity validation: stock count, per-day consistency, minute coverage
- [x] Data integrity validation on write (GreptimeDB)
- [x] Dashboard: cache scheduler status card (next run time, last result, enable/disable toggle)
- [x] Cache scheduler per-range download timeout (4 hours max)
- [x] Manual download gap check aligned with scheduler (boundaries + minute gaps via `missing_ranges()`)
- [ ] Unit tests

---

### [STR-005] Momentum Live Trading Interface (iQuant API)

**Status**: In Progress

**Description**: Live trading interface for momentum board scanning strategy, deployed as a set of HTTP API endpoints (iQuant API). QMT polls these endpoints for buy/sell signals and acknowledges execution. Uses T+2 adaptive sell with T+1 early exit on large gap-down.

**Strategy: Seven-Layer Parametric Funnel + V3 Regression Scoring**

| Layer | Name | Logic |
|-------|------|-------|
| L1 | Gain Filter | gain_from_open ≥ 0.2578%, main board + SME (00/60), non-ST, price ≥ ¥12 |
| L2 | Board Lookup | Reverse concept lookup via LocalConceptMapper |
| L3 | Hot Boards | Boards with ≥2 L1-qualifying stocks, exclude blacklist (物联网/医疗器械概念/特高压/冷链物流/特钢概念/三胎概念) |
| L4 | Constituent Expansion | All stocks in hot boards, re-apply L1 gain + price + ST filters |
| L5 | Volume Filter | turnover_amp = early_vol/(avg_vol×0.125), keep [0.498, 6.0] |
| L6 | Reversal Filter | ReversalFactorFilter(percentile=95, floor=0.15, min_sample=10) |
| L6.5 | Limit-Up Filter | open ≥ limit_price OR price_940 ≥ limit_price → remove |
| L6.6 | Upper Shadow Filter | (high - max(open, price_940))/open > 3% → remove (gain≥9.5% exempt) |
| L7 | V3 Score | Linear regression, pick highest-scored stock |

**V3 Scoring Model**:

| Factor | Computation | Coefficient |
|--------|-------------|-------------|
| intercept | — | +0.0106 |
| trend_10d | (close[-1]-close[-11])/close[-11] | -0.1034 |
| avg_daily_return_20d | mean(daily returns over 20d) | +1.0699 |
| intraday_range_940 | (high_940-low_940)/open | -0.3293 |
| consecutive_up_days | consecutive close > prev_close | +0.0089 |
| avg_market_open_gain | market-wide avg (open-prev_close)/prev_close | +0.4792 |
| trend_consistency | avg_daily_return / (volatility + 0.001) | +0.002 |

**T+2 Adaptive Sell**:
- **T+1 gap check** (09:25-09:35): if gap = (open - entry_price) / entry_price < -3% → mark early exit, sell at 14:50-14:58
- **T+2** (default): mark sell at gap check, sell at 14:50-14:58
- **Multi-day outage**: any holding with trading_days ≥ 2 since buy → sell

**Three-Window Scheduler**:

| Window | Time | Action |
|--------|------|--------|
| GAP_CHECK | 09:25-09:35 | Check holdings: T+1 gap → early exit; T+2 → mark sell |
| SCAN | 09:39-10:00 | If no holdings → run momentum scan → push BUY signal |
| SELL | 14:50-14:58 | Push SELL signal for all marked holdings |

**Signal Flow**:
1. Server pushes signal to `_state["pending_signals"]`
2. QMT polls `GET /api/iquant/pending-signals` → receives signal
3. QMT executes order, then `POST /api/iquant/ack-signal` with signal_id
4. BUY ack: record entry_price, save holdings to disk
5. SELL ack: remove from holdings, save to disk

**Holdings Persistence** (trading safety):
- Written to `data/v15_holdings.json` after every mutation
- Loaded on startup from file (survives service restart)
- File corruption → raise RuntimeError (fail-fast, no silent degradation)
- Structure: `{code, name, buy_date, entry_price, marked_sell_today, early_exit}`

**Data Sources**:
- Real-time quotes: Tushare via `TushareRealtimeClient` / `SinaRealtimeClient`
- Historical data: GreptimeDB (`GreptimeBacktestStorage`) via `IQuantHistoricalAdapter`
- Board data: `LocalConceptMapper` (local JSON files)
- Trade calendar: Tushare `trade_cal` API (cached in memory)

**Monitoring & Alerting** (Feishu notifications):

| Alert | Trigger | Content |
|-------|---------|---------|
| 每日就绪报告 | 09:30 | iQuant连接状态、持仓数、今日计划(扫描/跳过) |
| 买入/卖出信号推送 | Signal pushed | 股票代码、价格、板块、ML评分 |
| 信号执行确认 | Signal acked | 股票代码、价格、推送→执行时间差 |
| 信号超时未执行 | Pending > 5min | 股票代码、等待时长、可能原因(QMT掉线) |
| iQuant未连接 | 09:33 无心跳 | 提示检查QMT是否启动 |
| iQuant掉线 | Poll间隔 > 3min | 最后心跳时间、失联时长 |
| Dashboard状态 | 页面每10s刷新 | 在线/离线、最后心跳、持仓/待执行数 |
| 扫描无推荐 | Scan returns None | 通知今日无符合条件股票 |
| 扫描/跳空失败 | Exception | 完整错误堆栈 |

**Dashboard Recommendations (On-Demand Compute)**:
- `GET /api/trading/recommendations?date=YYYY-MM-DD` — returns top-10 ML scan results
- **Past dates**: uses GreptimeDB cache via `run_ml_backtest()`, ~1-3s
- **Today**: uses Tushare `batch_get_minute_bars` (rt_min_daily) → `EarlyWindowAggregator` via `run_ml_live()`, ~30-60s (only after 09:39)
- No PostgreSQL persistence — computed on-demand each time

**Key Files**:
- `src/strategy/strategies/ml_scanner.py` — ML 8-layer filter + LightGBM LambdaRank scoring
- `src/strategy/ml_strategy_service.py` — Stateless ML scan service (backtest + live)
- `src/strategy/signal_store.py` — In-memory signal queue (push/poll/ack/expire lifecycle)
- `src/web/iquant_routes.py` — iQuant communication + monitoring
- `src/web/routes.py` — Dashboard routes, delegates scan to ml_strategy_service
- `src/web/app.py` — GreptimeDB cache injection into iQuant router
- `src/data/clients/iquant_historical_adapter.py` — Historical data adapter

**Differences from STR-004**:
- Pure parametric (no LLM board relevance filter)
- V3 regression scoring (not Z-score composite)
- T+2 adaptive sell (not T+1 sell at open)
- 7-layer funnel with tighter thresholds (gain 0.2578% vs 0.56%, price ≥ ¥12, turnover [0.498, 6.0])
- Includes L6.5 limit-up filter + L6.6 upper shadow filter (not in STR-004)
- Includes SME (002) stocks (STR-004 excludes them)
- Single position only (STR-004's backtest also holds 1 stock but uses different position management)

**Checklist**:
- [x] MomentumScanner: 7-layer funnel + V3 scoring
- [x] iQuant routes: T+2 adaptive scheduler with three windows
- [x] Holdings persistence to JSON file
- [x] Trade calendar via Tushare trade_cal
- [x] GreptimeDB cache injection from app.py
- [x] Feishu notification
- [x] Monitoring: signal timeout, heartbeat, readiness report, ack confirmation
- [ ] Unit tests
- [ ] Production deployment verification

---

### [STR-006] ML Scanner Strategy (8-Layer Filter + LightGBM LambdaRank)

**Status**: In Progress

**Description**: New stock selection strategy replacing the momentum scanner. Uses an 8-layer parametric filter pipeline to narrow ~5000 stocks down to ~200 candidates, then scores them with a LightGBM LambdaRank model trained on historical forward returns.

**ML Model Architecture**:
- **Algorithm**: LightGBM LambdaRank (learning-to-rank)
- **Features**: 76 dimensions = 38 raw (13 basic + 14 advanced + 11 cross) + 38 Z-score normalized
- **Label**: 2-day forward return after fees (0.09%), bucketed into 5 quintiles (0-4) per day
- **Training**: ~1 year data, ~200 candidates/day, ~57k training records
- **Hyperparams**: 15 leaves, lr 0.05, 80% feature/sample fraction, L1=0.1, L2=1.0, max 500 rounds, early stop 50

**Feature Alignment (CRITICAL)**:
FC training (`serverless/app.py`) MUST replicate `lgbrank_scorer.py`'s feature computation exactly:
- **Units**: All ratios (0.01 = 1%), NOT percentages. `open_gain = (close-open)/open`, not `×100`
- **volume_amp**: Uses `×0.125` early session ratio: `vol / (avg_vol × 0.125)`
- **market_open_gain**: Average **gap** ratio `(open-prev_close)/prev_close`, NOT intraday gain
- **trend_consistency**: Sharpe-like `avg_return/(volatility+0.001)`, NOT fraction of positive days
- **Advanced features**: Numpy arrays over full history, matching `_compute_advanced_features` exactly
- **Cross features**: Match `_compute_engineered_features` exactly (e.g. `trend_acceleration = trend_10d - trend_5d`)
- **Z-score**: numpy std (ddof=0), matching `_add_zscore`
- **Reference**: `lgbrank_scorer.py` in main worktree is the source of truth for all feature formulas

**Model Management**:
- Training runs on Alibaba Cloud FC serverless (async invocation, `X-Fc-Invocation-Type: Async`)
- Flow: trading-service triggers FC → FC fetches data via callback → trains → POSTs result back
- Dashboard card with full-train / manual finetune buttons + training log display
- Auto finetune every 20 trading days at 3am (background scheduler, toggle on/off)
- Pre-training data completeness check: 3 retry gap-fill attempts, abort with Feishu alert on failure
- Model files saved locally (`data/models/`) and uploaded to S3 (by FC)
- Fine-tune: FC downloads init model from S3, no local training code
- Feishu alerts for: training success/failure, S3 upload, data issues, scheduler status

**Stock Blacklist** (added 2026-04-28):
- Individual codes that violate strategy assumptions (low-volatility heavyweights, repeat false-positive names) are hardcoded in `src/strategy/filters/stock_blacklist.py` as `BLACKLISTED_STOCKS: dict[str, str]` (code → reason).
- Applied at the top of the funnel in three places:
  - `MLScanner.build_universe` Step 0 — alongside the ST filter, before any other gate runs
  - `replicate_v16.py` Step 0 — keeps offline replication consistent with prod
  - `/api/model/training-data` NDJSON stream — strips blacklisted codes from every day's bars so the LambdaRank model never sees their data
- To add a code: edit the dict, fill in the reason, commit, push. Watchtower picks up the new image on prod within ~60s of CI passing.

**Key Files**:
- `src/strategy/strategies/ml_scanner.py` — 8-layer filter pipeline + feature engineering + model inference
- `src/strategy/filters/stock_blacklist.py` — Hardcoded individual-stock blacklist (code → reason)
- `src/data/services/model_training_scheduler.py` — Scheduler + FC async orchestration (trigger/callback/save)
- `serverless/app.py` — FC serverless training endpoint (LightGBM train, S3 upload, result callback)
- `src/strategy/ml_strategy_service.py` — Stateless runners: `run_ml_live()` + `run_ml_backtest()`
- `src/common/s3_client.py` — S3-compatible upload/download (boto3)
- `src/common/config.py` — S3 config + FC URL + finetune scheduler toggle
- `src/web/routes.py` — `create_model_router()` SSE endpoints + FC callback endpoints
- `src/web/templates/index.html` — Model management dashboard card

**Dependencies**: `lightgbm>=4.0` (FC only), `boto3>=1.35.0`

**Checklist**:
- [x] MLScanner: 8-layer filter pipeline (Step 0-7)
- [x] Feature engineering: 76 features (38 raw + 38 Z-scored)
- [x] LightGBM LambdaRank scoring (inference via load_model + score_candidates)
- [x] FC serverless training (full train + finetune, async invocation)
- [x] FC result callback architecture (data token + result token)
- [x] ModelTrainingScheduler (every 20 trading days at 3am)
- [x] S3 client for model upload/download
- [x] Dashboard model management card (train/finetune buttons, log area, status)
- [x] SSE streaming for training progress
- [x] Feishu alerts for all training events
- [x] Data completeness check with 3-retry gap-fill
- [x] Live inference: 9:39 scan via `ml_strategy_service.run_ml_live()` → `MLScanner.scan()` → LightGBM scoring
- [x] Backtest inference: `run_ml_backtest()` from GreptimeDB cache
- [x] `MLScanResult` / `MLScoredStock` dataclasses for structured output
- [x] Feishu ML top-5 report (`send_ml_top5_report`)
- [x] `/api/ml/model-info` endpoint for model listing
- [x] V15 momentum scan fully replaced in iquant_routes + routes
- [ ] Sell strategy
- [ ] Unit tests
- [ ] Production deployment

---

## Module: Trading

Trading is handled entirely through the iQuant interface (STR-005). There is no standalone trading module — the `src/web/iquant_routes.py` manages signal pushing, position tracking, and order acknowledgment. Holdings are persisted to `data/v15_holdings.json`.

---

## Module: Data

### [DAT-001] Market Data Sources

**Status**: Completed

**Description**: Market data access through multiple sources, unified behind adapters.

**Data Sources**:

| Purpose | Source | Adapter / File |
|---------|--------|----------------|
| Backtest daily OHLCV | tsanghi 沧海数据 | `GreptimeHistoricalAdapter` via `CachePipeline` |
| Backtest minute bars | Tushare Pro `stk_mins` 1min | `GreptimeBacktestStorage` via `CachePipeline` |
| Live realtime quotes | Tushare Pro `rt_min_daily` | `TushareRealtimeClient` |
| Live historical | GreptimeDB cache | `IQuantHistoricalAdapter` |
| Stock metadata | Tushare Pro `bak_basic` / `suspend_d` / `trade_cal` | `TushareMetadataSource` |
| Board/concept mapping | Local JSON files | `LocalConceptMapper` |
| Stock names | Local JSON files | `LocalConceptMapper.get_stock_name()` |

**Files**:
- `src/data/clients/greptime_storage.py` - GreptimeDB storage (asyncpg, CRUD)
- `src/data/clients/greptime_historical_adapter.py` - Read-only adapter (HistoricalDataProvider Protocol)
- `src/data/clients/iquant_historical_adapter.py` - Live historical adapter
- `src/data/clients/tushare_realtime.py` - Tushare realtime quotes
- `src/data/clients/sina_realtime.py` - Sina realtime (fallback)
- `src/data/sources/tsanghi_daily_source.py` - tsanghi daily OHLCV
- `src/data/sources/tushare_minute_source.py` - Tushare 1-min bars
- `src/data/sources/tushare_metadata_source.py` - Stock metadata (trade_cal, suspend, stock_basic)
- `src/data/sources/local_concept_mapper.py` - Board ↔ stock mapping (local JSON)

---

### [DAT-002] Cache Pipeline (GreptimeDB)

**Status**: Completed

**Description**: Automated data download and caching pipeline. Downloads daily OHLCV from tsanghi and minute bars from Tushare Pro into GreptimeDB for backtesting and ML training.

**GreptimeDB Tables**:

| Table | Content | Key |
|-------|---------|-----|
| `backtest_daily` | Daily OHLCV + is_suspended | `(ts, stock_code)` |
| `backtest_minute` | 9:40 snapshot (close_940, cum_volume, max_high, min_low) | `(ts, stock_code)` |
| `stock_list` | Per-day stock metadata (name, exchange, is_suspended) | `(ts, stock_code)` |

**Pipeline Phases** (`CachePipeline.download_prices()`):

| # | Phase | What it does |
|---|-------|-------------|
| 1 | `compact` | GreptimeDB housekeeping |
| 2 | `backfill` | Fix NULL `is_suspended` rows (one-time) |
| 3 | `stock_list` | Sync stock metadata per trading day |
| 4 | `daily` | Download daily OHLCV (resume by day) |
| 5 | `daily_backfill` | Audit + refetch missing stocks per day |
| 6 | `minute` | Download 1-min bars per stock (resume by stock) |
| 7 | `minute_backfill` | Audit + refetch missing (day, code) pairs |
| 8 | `download` | Final verification + Feishu report |

**Scheduling**: 3am daily auto-fill via `CacheScheduler` (toggle on/off in Settings page).

**Files**:
- `src/data/services/cache_pipeline.py` - Download orchestration
- `src/data/services/cache_scheduler.py` - 3am auto gap-fill
- `src/data/services/cache_progress_reporter.py` - Phase tracking + Feishu notifications
- `src/data/services/download_task.py` - Background task state machine
- `src/data/clients/greptime_storage.py` - GreptimeDB storage (asyncpg pool)

**Checklist**:
- [x] GreptimeDB storage with asyncpg (3 mandatory overrides — see dev-conventions.md)
- [x] Cache pipeline with 8 phases
- [x] Cache scheduler (3am daily, configurable toggle)
- [x] Download resume by day (daily) and by stock (minute)
- [x] Per-day minute audit + backfill
- [x] Integrity validation on write
- [x] Feishu notifications for all scenarios
- [x] Dashboard data engine status card
- [x] Manual download with SSE progress + stop button
- [x] Suspended stock prev_close chaining (per-stock latest close, not global MAX date)

---

### [DAT-003] Stock Blacklist

**Status**: Completed

**Description**: Global stock blacklist. Blacklisted stocks are skipped in data download, scanning, and trading.

**Implementation**: Hardcoded `STOCK_BLACKLIST` frozenset in `src/common/config.py`. Applied at download, scan, and signal generation layers.

---

---

## Infrastructure

### [INF-002] CI Pipeline

**Status**: Completed

**Description**: Continuous integration pipeline for code quality.

**Technical Design**:
- GitHub Actions workflow on push/PR to main branch
- Uses `uv` for fast dependency installation
- Parallel jobs: lint (ruff), format (ruff format), typecheck (mypy), test (pytest)
- Auto-deploys trading-service Docker image and FC serverless function

**Files**:
- `.github/workflows/ci.yml` - GitHub Actions workflow

---

## Module: Analysis

### [ANA-001] K-line Technical Analysis (Vision LLM)

**Status**: Completed (infrastructure); UI/trigger TBD

**Description**: 端到端 K 线技术面分析。主程序从 GreptimeDB 取最近 N 个交易日的日 K，
POST 给海外 AWS Lambda；Lambda 用 matplotlib 画图，存到公开读 S3，返回 URL；
主程序拿 URL 调柏拉图AI vision LLM（OpenAI 兼容，gpt-5.5-pro），返回中文技术面分析。

**Architecture & Why**:

```
trading-service (cn-shanghai VPC)        Lambda (us-east-1)
─────────────────────────────────        ─────────────────────────
1. trigger (HTTP POST)
2. GreptimeDB → OHLCV[N days]
3. POST {code, days, ohlcv[]} ─────────► 4. matplotlib 渲染 PNG
                                          5. S3 put_object (ACL=public-read)
   ◄────── { url, key } ─────────────── 6. 返回公开 URL
7. POST chat/completions (柏拉图AI)
   { image_url: { url } }
   ◄──── { analysis text }              [bltcy → GPT-5.5-pro]
```

**为什么数据国内查、画图存海外**：阿里云 OSS 大陆 region bucket 的默认域名从
2025-03-20 起对新用户禁止做数据 API 操作，必须绑定备案过的自定义域名才能给
公网（包括 LLM provider）拉图。把渲染 + 存储放到 AWS us-east-1 直接绕开 ICP
备案、`Content-Disposition: attachment` 强制下载、跨网延迟等一连串问题，
trading-service 只发 JSON over HTTPS。

**Components**:

| 部分 | 路径 | 说明 |
|------|------|------|
| Lambda 渲染服务 | `lambda-kline/` | Container Lambda，POST OHLCV → PNG → 公开 S3 URL |
| Orchestrator | `src/analysis/kline_llm.py` | 取数据 → 调 Lambda → 调柏拉图，对外是 `analyze_kline()` |
| HTTP 入口 | `src/web/analysis_routes.py` | `POST /api/analyze-kline` (body: code, days, prompt?) |
| 配置 helpers | `src/common/config.py` | `get_lambda_kline_url/token`, `get_bltcy_api_key/base_url/model` |
| CI/CD | `.github/workflows/ci.yml::deploy-lambda-kline` | push 到 main / refactor 分支自动构建并更新 Lambda 镜像 |
| AWS bootstrap 文档 | `lambda-kline/README.md` | S3/ECR/IAM/Lambda 一次性建资源 + 鉴权说明 |

**Required env on trading-service** (production .env):
- `LAMBDA_KLINE_URL` — Lambda Function URL
- `LAMBDA_KLINE_TOKEN` — 与 Lambda 端 `UPLOAD_TOKEN` 相同的 secret，作为 `x-upload-token` header
- `BLTCY_API_KEY` — 柏拉图AI API key（可选 `BLTCY_BASE_URL` / `BLTCY_MODEL` 覆盖默认值）

**Required GitHub Secrets** (for CI auto-deploy):
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION`
- `AWS_LAMBDA_KLINE_ECR` (ECR repo name) / `AWS_LAMBDA_KLINE_FUNCTION` (Lambda 函数名)
- Repo Variable `LAMBDA_KLINE_ENABLED=true` 才开启 deploy 这个 job

**接口**:

```
POST /api/analyze-kline
{ "code": "000001.SZ", "days": 30, "prompt": null }
↓
{
  "code": "000001.SZ",
  "days": 30,
  "bars": 30,
  "image_url": "https://<bucket>.s3.us-east-1.amazonaws.com/kline/...png",
  "model": "gpt-5.5-pro",
  "analysis": "...中文分析文本..."
}
```

**Trigger 方式**：当前只暴露 HTTP endpoint，UI 按钮 / 飞书命令 / 定时任务后续接入。

**Backlog**:
- 渲染加分钟级、加 RSI/MACD 等指标
- 历史分析归档（落 GreptimeDB），支持 web UI 回看
- prompt 工程：把基本面/资金流/板块温度等结构化信号一起喂进去，不只看图

---

## Backlog

- [ ] Multi-account support
- [ ] Performance analytics
- [ ] Unit test coverage improvement
