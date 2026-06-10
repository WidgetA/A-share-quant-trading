# Feature Specifications

> **Important**: This document is the single source of truth for feature requirements.
> Always update this document BEFORE implementing any new feature or change.

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 0.18.3 | 2026-06-08 | - | STR-004/006 修复: 区间回测资金模型适配 T+2 持仓。改 T+2 卖后,旧模型每天仍用「全部资金」买,但 T 日的票要到 T+2 才卖、T+1 根本没钱再买(等于自己骗自己)。改为**资金分 3 份轮动**:持仓 2 个交易日→任一时刻最多 3 笔重叠持仓,故初始资金均分 3 个独立子仓,按交易日序号 %3 轮流用、每日只投 1/3;第 N 天用第 N%3 份买的票在 N+2 卖,该子仓 N+3 天才轮到下一笔,资金不冲突。各子仓自负盈亏复利,`final_capital`=3 份之和;summary 增 `num_sleeves`。 |
| 0.18.2 | 2026-06-08 | - | STR-004/006: 删除区间回测的「最多 250 个交易日」截断上限——数据全在本地 GreptimeDB,没有外部 API 配额限制,逐日 SSE 流式输出不会空闲超时,任意长度区间(几年)都能跑完。原上限只是怕一次跑太久的人为软保护,并非数据限制。(注:逐日全市场扫描的单日耗时未变,长区间仍是线性时间,后续若要提速另议) |
| 0.18.1 | 2026-06-08 | - | STR-004/006: 区间回测卖出口径从「T+1 开盘」改为 **T+2 adaptive sell**,完整对齐 STR-005 实盘(买入仍 T 日 9:40):①T+1 早盘开盘比买入价低开 >3% → 当天 T+1 收盘提前止损;②否则持到 T+2 收盘卖。回测此前卖在 T+1 开盘,恰是实盘文档明令「不是」的那套。summary 增 `early_exit_days`(止损触发天数);日历缓冲 +10→+20 天,防区间末尾买入日的 T+2 跨长假被静默跳过;前端逐日日志显示真实卖出原因(T+2 收 / T+1 低开止损)+ 卖出日期。 |
| 0.18.0 | 2026-06-02 | - | DAT-006: 交易日历真值表 `trading_calendar` —— 每「交易日 × 股票」一行的物化权威真值(在册/停牌/日线状态/预留分钟状态),由 roster(Tushare 上市日) ∩ suspend_d ∩ Tushare daily ∩ backtest_daily 复合而来;存全+读时筛(视图);检查/补全改为对照它,手动触发=重建。配套: `_to_ts_code` 北交所→`.BJ`(原 `.SZ` 致分钟空);日线-only 历史回填 `POST /api/audit/backfill-daily`;诊断报告飞书消息体积上限(超限静默丢弃修复);`stock_listing_info` 固定 ts + 重建前 truncate(同码双行/读闪修复)。 |
| 0.17.0 | 2026-05-31 | - | DAT-002: 缓存补全/完整性检查结束后自动发送“按天详报”到飞书; `/api/audit/diagnose-gaps` 手动触发同一报告; 日线问题逐日展开,分钟 B 类库漏存逐日摘要,C/PENDING 类归类汇总,完整逐天明细写入 `data/audit/gap_diagnosis_report.{json,md}`。 |
| 0.16.9 | 2026-05-07 | - | NOTE-001: 买入/卖出 事件正文拆 对内 / 对外 两栏（schema 加 `content_external` 列，幂等 ALTER 兼容老部署）；新增/编辑表单都给两个 textarea，单事件查看 + 篇 view trade card 都展示双栏（两栏都填时显示「对内/对外」分节标签）。 |
| 0.16.8 | 2026-05-07 | - | NOTE-001: 手插带时间戳的卡片改为独立持久化（新表 `note_cards`，与 `trade_notes` 解耦）；带 4 个新 endpoint（GET/POST/PATCH/DELETE `/api/notes/{code}/cards`），按自己的 ts 在 篇 view 与 买入/卖出 交错排序。点卡片可直接编辑/删除（modal 重用）。每个 cmt 现在也是独立 segment（之前同一时段多个 cmt 会合并丢数据）。 |
| 0.16.7 | 2026-05-07 | - | NOTE-001: 篇 view 的 买入/卖出 卡片渲染自身的 `content`（之前只画 label+meta，content 被藏在单事件编辑器里）。卡片改成 column flex：header 行（label+meta）+ 可选 body（markdown 只读）。 |
| 0.16.6 | 2026-05-07 | - | NOTE-001: 左栏股票头部加 ✎ 按钮——纠正输错的股票代码（`PATCH /api/notes/stocks/{code} {new_code}`）。把当前股票的所有 live 事件迁到新代码，旧代码下软删；新代码若已有事件则按 ts 合并。 |
| 0.16.5 | 2026-05-07 | - | NOTE-001: 批量下单（/api/trading/buy-batch-by-amount）后自动写入 trade_notes（拉 broker.get_orders 的 FILLED 腿，按 broker_<order_id> 幂等）；同时单笔 hook 改用 upsert_broker_event_by_order_id，修掉单笔 hook + ⤓ 回补的重复计数 bug。 |
| 0.16.4 | 2026-05-07 | - | NOTE-001: 事件 tag 颜色改按 event_type 上色——买入=绿，卖出=黄（之前按 source broker/user/ai 分色，意义不直观）。来源仍可在右栏「来源:」行查到。 |
| 0.16.3 | 2026-05-07 | - | NOTE-001: 编辑表单加「类型」select——可在 买入/卖出 之间切换 event_type；后端 `update_event` 同步翻转 `side` 保持数据一致（PATCH `/api/notes/{code}/events/{event_id}` 接受 `event_type` 字段）。 |
| 0.16.2 | 2026-05-07 | - | NOTE-001: 篇 view 加 `+卡片` 按钮——弹窗输入时间+内容，作为 `【YYYY-MM-DD HH:mm】 …` 内联标记插入光标处；存为周边 `评论` 内容的一部分，不新建事件；时间戳标记在渲染时获得视觉样式。 |
| 0.16.1 | 2026-05-07 | - | NOTE-001: Add 篇 document view for trade notes: stock selection opens one contenteditable document, trade events render as locked cards, and prose persists as `评论` segments between trades. |
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
| 0.14.1 | 2026-05-05 | - | ANA-001: web UI Settings for lambda-kline URL/token + bltcy key (zero-restart config); orchestrator reuses `app.state.storage`; gpt-5.5-pro locked; production-verified end-to-end |
| 0.15.0 | 2026-05-05 | - | ANA-002: 盘前持仓日报——交易日 8am 自动扫描 broker 持仓→对每只调 ANA-001→飞书逐条推送 K 线 + 技术面分析；附 `POST /api/pre-market-report/run` 手动触发 |
| 0.16.0 | 2026-05-07 | - | NOTE-001: 交易笔记——三栏 master-detail（股票/事件/正文），GreptimeDB `trade_notes` 表存储；`place_order` 成功 hook 自动追加买入/卖出事件；用户/AI 可自由追加思考/复盘事件 |
| 0.17.0 | 2026-05-07 | - | SYS-005: `/api/trading/*` 加 `X-API-Key` 鉴权（`TRADING_API_KEY` 配置可选；未配置时仅在启动日志告警，配置后立即生效）；Settings 页可生成/保存 key；Dashboard JS 自动从 localStorage 注入 |
| 0.17.1 | 2026-05-28 | - | NOTE-001: 买入/卖出 表单加 佣金、过户费；卖出额外加 印花税、股息、平仓收益 + 一键「计算」按钮（按上一次买入按比例分摊成本：`(sell_qty/buy_qty) × buy_fees + buy_price×sell_qty` 与 `sell_amt − sell_fees + dividend` 做差）。schema 加 5 列 `commission/transfer_fee/stamp_tax/dividend/realized_pnl FLOAT64`，幂等 ALTER 兼容老库。 |

---

## Module: System

### [SYS-001] Application Entry Point

**Status**: Completed

**Description**: FastAPI web application as the sole entry point, serving dashboard UI, backtest API, ML strategy API, broker order API, and background schedulers.

**Entry Point**:
```bash
uv run uvicorn src.web.app:create_app --factory --host 0.0.0.0 --port 8000
```

**Startup Flow** (`src/web/app.py` → `create_app()` + `startup`):
1. Configure root logger (idempotent for pytest)
2. Mount routers (main, momentum, settings, trade-backtest, broker-order-cache, trading, model, ML, analysis, notes, audit)
3. Initialize broker client (xtquant-trade-server) + start position/order poll loops (every 30s)
4. Send Feishu startup notification
5. Start ML monitoring scheduler (daily readiness report + broker health check) — **before** cache loading + audit (safety-critical, see trading-safety-patterns.md)
6. Connect GreptimeDB storage + CachePipeline (background retry if unavailable)
7. Run trading safety audit (Feishu alert on CRITICAL)
8. Start cache scheduler (3am), model training scheduler, pre-market report scheduler (8am), intraday momentum monitor

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
| Broker positions / cash | In-memory | `app.state.broker_positions` / `available_cash` (polled from xtquant-trade-server every 30s) |
| Broker orders | In-memory | `app.state.broker_orders` (polled every 30s; fills imported into trade notes) |
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
| `FEISHU_BOT_URL` | No | `https://feishu-groupbot.fly.dev` | Bot relay service URL |

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
- `src/web/ml_routes.py` - ML monitoring: daily readiness report + broker health alerts
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

**Description**: FastAPI web application serving dashboard UI, backtest tools, ML strategy API, broker order API, and background schedulers. All user interaction via browser; Feishu for push notifications.

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
│  │  HTML Pages  │    │  Feishu Bot  │    │ BrokerClient │  │
│  │  (Jinja2)    │    │  (通知+链接) │    │ (xtquant)    │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**Pages**:

| Page | URL | Content |
|------|-----|---------|
| Dashboard | `/` | broker connection status, broker positions/cash, data engine status, model management, recommendations |
| Backtest | `/backtest` | Single-day scan, range backtest (SSE), CSV analysis |
| Settings | `/settings` | Tushare token, cache scheduler toggle, FC URL, S3 config |
| Database | `/database` | Embedded GreptimeDB dashboard |

**Key API Endpoints**:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/status` | GET | Health check |
| `/api/stock/status` | GET | Broker connection status (public, no key) |
| `/api/trading/recommendations` | GET | On-demand ML scan results (top-10) |
| `/api/momentum/tsanghi-prepare` | POST | Download cache data (SSE stream; legacy URL — daily source is now Tushare) |
| `/api/momentum/backtest` | POST | Single-day momentum scan |
| `/api/momentum/combined-analysis` | POST | Range backtest (SSE stream) |
| `/api/model/full-train` | POST | Trigger full ML training (SSE stream) |
| `/api/model/finetune` | POST | Trigger ML fine-tuning (SSE stream) |
| `/api/stock/trigger-scan` | POST | Manually run ML scan + Feishu top-5 report; **需 `X-API-Key`** (STOCK_API_KEY) |
| `/api/stock/quote` | POST | Real-time quotes for given stock codes; **需 `X-API-Key`** |
| `/api/stock/backtest-scan` | POST | Run ML backtest scan for a past trade date; **需 `X-API-Key`** |
| `/api/trading/buy` / `sell` / `buy-batch-by-amount` | POST | 下单（dashboard + 外部服务）；**需 `X-API-Key`** |
| `/api/trading/holdings` / `recommendations` / `orders` | GET | 持仓/推荐/委托查询；**需 `X-API-Key`** |
| `/api/trading/orders/{order_id}` | DELETE | 撤单；**需 `X-API-Key`** |
| `/api/settings/trading-api-key` | GET / POST | 查/设当前的 trading API key（持久化到 `data/trading_api_key.txt`） |

**Trading API 鉴权（v0.17.0）**:

所有 `/api/trading/*` 路由通过路由级 `Depends(verify_trading_api_key)` 校验请求头 `X-API-Key`。

- **Key 来源优先级**：`data/trading_api_key.txt` (Settings 页保存) > `TRADING_API_KEY` 环境变量
- **未配置时**：启动日志输出 WARNING，路由放行（保留向后兼容，避免升级即破坏部署）
- **配置后**：立即强制校验，无 / 错 key → 401
- **Dashboard JS**：从 `localStorage.tradingApiKey` 读取并注入；首次 401 → `prompt()` 让用户输入并保存
- **外部服务调用示例**：
  ```bash
  curl -X POST http://<host>:8000/api/trading/buy-batch-by-amount \
    -H 'X-API-Key: <key>' \
    -H 'Content-Type: application/json' \
    -d '{"amount":30000,"orders":[{"code":"601398","ref_price":5.20}]}'
  ```

**Configuration** (Environment Variables):

| Variable | Default | Description |
|----------|---------|-------------|
| `WEB_BASE_URL` | auto | Base URL for Feishu notification links |
| `GREPTIME_HOST` | `localhost` | GreptimeDB host |
| `FEISHU_BOT_URL` | (leapcell) | Feishu bot relay URL |
| `FEISHU_APP_ID` / `FEISHU_APP_SECRET` / `FEISHU_CHAT_ID` | - | Feishu credentials |
| `FC_URL` | - | Alibaba Cloud FC training endpoint |
| `STOCK_API_KEY` | - | `/api/stock/*` ML 策略端点鉴权 key（亦可经 Settings 持久化到 `data/stock_api_key.txt`） |
| `TRADING_API_KEY` | - | `/api/trading/*` 鉴权 key（亦可经 Settings 持久化到 `data/trading_api_key.txt`） |

**Files**:
- `src/web/app.py` - FastAPI application factory + startup lifecycle
- `src/web/routes.py` - Dashboard, backtest, settings, model management, `/api/trading/*` order routes
- `src/web/ml_routes.py` - ML strategy API (`/api/stock/*`: scan / quote / backtest) + monitoring scheduler
- `src/web/broker_order_routes.py` - Read-only cached broker order list (`/api/trading/orders`)
- `src/trading/broker_client.py` - HTTP client for xtquant-trade-server (positions / cash / orders / place / cancel)
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
- [x] FastAPI Web application with multiple routers (dashboard, momentum, settings, backtest, broker orders, trading, model, ML, analysis, notes, audit)
- [x] Dashboard: broker connection status, broker positions, data engine card, model management
- [x] Backtest page: single-day scan, range backtest, CSV analysis
- [x] Settings page: Tushare token, cache scheduler, FC URL, S3 config
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
ml_routes.py / routes.py
        │
        ▼
ml_strategy_service.py / momentum_strategy_service.py   (data prep + invocation)
        │
        ▼
ml_scanner.py / momentum_scanner.py                      (filter pipeline + scoring)
        │
        ▼
scored stock list → dashboard / Feishu report           (returned to caller; no auto order push)
```

**Key Components**:

| Component | File | Role |
|-----------|------|------|
| `MLScanner` | `src/strategy/strategies/ml_scanner.py` | 8-layer filter + LightGBM scoring |
| `MomentumScanner` | `src/strategy/strategies/momentum_scanner.py` | 7-layer funnel + V3 regression scoring |
| `MLStrategyService` | `src/strategy/ml_strategy_service.py` | Stateless: `run_ml_live()` / `run_ml_backtest()` |
| `MomentumStrategyService` | `src/strategy/momentum_strategy_service.py` | Stateless: `run_momentum_live()` / `run_momentum_backtest()` |
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
- Price (backtest): 日线 from Tushare Pro `daily`, 9:40 快照 from Tushare Pro `stk_mins` 1min (聚合 09:31~09:40 by `EarlyWindowAggregator`), via `GreptimeBacktestStorage` + `CachePipeline`
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
| **Range** | Run strategy for a date range (任意长度,无交易日上限——数据全在本地 GreptimeDB,逐日 SSE 流式跑完), simulate daily buy/sell with real costs |

**Range Backtest Details**:
- Input: start date, end date, initial capital (yuan)
- Each trading day: run strategy → buy recommended stock at 9:40 price, **sell via T+2 adaptive rule**(对齐 STR-005 实盘):
  - **T+1 早盘低开止损**:若 T+1 开盘价比买入价低开 > 3% → 当天 T+1 收盘卖出止损
  - **T+2 默认**:否则持到 T+2 收盘卖出(持仓 2 个交易日)
  - 两条分支都取日线收盘价模拟尾盘成交;summary 里 `early_exit_days` 记录止损触发了几天
- **资金分 3 份轮动**(持仓 T+2 占 2 个交易日,任一时刻最多 3 笔重叠持仓):初始资金均分 3 个独立子仓,按交易日序号 %3 轮流用,每日只投 1/3。第 N 天用第 N%3 份买的票在第 N+2 天卖出,该子仓第 N+3 天才轮到下一笔,资金不冲突;各子仓自负盈亏独立复利,`final_capital` = 3 份之和。**为什么是 3 份**:旧 T+1 卖时(次日开盘卖在当天 9:40 买之前)同一份钱就够轮,改 T+2 后一笔钱要压 2 天,不分仓就会"第二天没钱买"
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

### [STR-005] Live Trading Interface (Broker / xtquant-trade-server)

**Status**: In Progress

**Description**: Live trading interface for the scanning strategy. Order execution goes through `BrokerClient` → an `xtquant-trade-server` running on a Windows QMT box (positions/cash/orders polled every 30s, orders placed via `/api/trading/*`). Recommendations are computed on demand and surfaced on the dashboard / via Feishu; auto-order push is currently disabled (`trigger-scan` returns `signal_pushed: false`).

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

**Order Flow** (via xtquant-trade-server):
1. Dashboard / external service calls `POST /api/trading/buy|sell|buy-batch-by-amount` with `X-API-Key` (TRADING_API_KEY)
2. `BrokerClient` forwards the order to xtquant-trade-server, which places it through QMT
3. A 30s poll loop (`_broker_order_poll_loop`) refreshes `app.state.broker_orders`; new fills are imported into trade notes
4. A 30s poll loop (`_broker_position_poll_loop`) refreshes `app.state.broker_positions` / `available_cash`
5. Positions/cash are held only in memory (no holdings file); they are re-fetched from the broker on every restart

**Data Sources**:
- Real-time quotes: Tushare via `TushareRealtimeClient`
- Historical data (37d): **临时**走 Tushare `daily` 实时并发拉 (`_fetch_history_live` in `ml_strategy_service.py`),解耦 cache 风险;cache 完整闭环后会回退到 `GreptimeBacktestStorage.get_multi_day_history`
- prev_close: live from Tushare `daily` (never cached — see `_resolve_prev_close`)
- Board data: `LocalConceptMapper` (local JSON files)
- Trade calendar: Tushare `trade_cal` API (cached in memory)

**Monitoring & Alerting** (Feishu notifications):

| Alert | Trigger | Content |
|-------|---------|---------|
| 每日就绪报告 | 09:30 | Broker就绪状态、当前持仓数、今日计划 |
| Broker未就绪 | 交易时段内 `broker.is_ready()` (xtquant `/readyz`) 失败 | 检查 Windows 上 xtquant-trade-server,30 分钟冷却去重 |
| ML扫描Top5报告 | 扫描产出结果 | top-5 股票代码、价格、ML评分 |
| 扫描失败 | Exception | 完整错误堆栈 |
| Dashboard状态 | 页面每10s刷新 `/api/stock/status` | Broker 在线/离线、持仓数、可用现金 |

**Dashboard Recommendations (On-Demand Compute)**:
- `GET /api/trading/recommendations?date=YYYY-MM-DD` — returns top-10 ML scan results
- **Past dates**: uses GreptimeDB cache via `run_ml_backtest()`, ~1-3s
- **Today**: uses Tushare `batch_get_minute_bars` (rt_min_daily) → `EarlyWindowAggregator` via `run_ml_live()`, ~30-60s (only after 09:39)
- No PostgreSQL persistence — computed on-demand each time

**Key Files**:
- `src/strategy/strategies/ml_scanner.py` — ML 8-layer filter + LightGBM LambdaRank scoring
- `src/strategy/ml_strategy_service.py` — Stateless ML scan service (backtest + live)
- `src/web/ml_routes.py` — `/api/stock/*` ML strategy API + monitoring scheduler (readiness + broker health)
- `src/web/broker_order_routes.py` — Read-only cached broker orders (`/api/trading/orders`)
- `src/trading/broker_client.py` — HTTP client for xtquant-trade-server (positions/cash/orders/place/cancel)
- `src/web/routes.py` — Dashboard routes + `/api/trading/*` order placement, delegates scan to ml_strategy_service
- `src/web/app.py` — Broker init + position/order poll loops; GreptimeDB cache injection into ML router
- `src/data/clients/greptime_historical_adapter.py` — Historical data read adapter (HistoricalDataProvider)

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
- [x] Broker order routes (`/api/trading/buy|sell|batch`) via BrokerClient → xtquant-trade-server
- [x] Position/order poll loops (30s) into app.state
- [x] Trade calendar via Tushare trade_cal
- [x] GreptimeDB cache injection from app.py
- [x] Feishu notification
- [x] Monitoring: daily readiness report + broker health check
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
- [x] V15 momentum scan fully replaced by ML scan in ml_routes + routes
- [ ] Sell strategy
- [ ] Unit tests
- [ ] Production deployment

---

## Module: Trading

Trading is handled through the broker interface (STR-005). Order placement lives in `src/web/routes.py` (`/api/trading/*`) backed by `src/trading/broker_client.py`, which talks to an `xtquant-trade-server` on a Windows QMT box. Positions, cash, and orders are polled from the broker every 30s into `app.state` (no local holdings file).

---

## Module: Data

### [DAT-001] Market Data Sources

**Status**: Completed

**Description**: Market data access through multiple sources, unified behind adapters.

**Data Sources**:

| Purpose | Source | Adapter / File |
|---------|--------|----------------|
| Backtest daily OHLCV | Tushare Pro `daily` | `TushareDailySource` via `CachePipeline` |
| Backtest minute bars | Tushare Pro `stk_mins` 1min | `GreptimeBacktestStorage` via `CachePipeline` |
| Live realtime quotes | Tushare Pro `rt_min_daily` | `TushareRealtimeClient` |
| Live prev_close | Tushare Pro `daily` (live, not cached) | `_resolve_prev_close` in `ml_strategy_service` |
| Live 37d history | **临时**: Tushare Pro `daily` 实时并发拉 | `_fetch_history_live` in `ml_strategy_service` (cache 闭环后回退 `storage.get_multi_day_history`) |
| Stock metadata | Tushare Pro `bak_basic` / `suspend_d` / `trade_cal` | `TushareMetadataSource` |
| Board/concept mapping | Local JSON files | `LocalConceptMapper` |
| Stock names | Local JSON files | `LocalConceptMapper.get_stock_name()` |

**Files**:
- `src/data/clients/greptime_storage.py` - GreptimeDB storage (asyncpg, CRUD)
- `src/data/clients/greptime_historical_adapter.py` - Read-only adapter (HistoricalDataProvider Protocol)
- `src/data/clients/tushare_realtime.py` - Tushare realtime quotes + `daily` full-market OHLCV
- `src/data/sources/tushare_daily_source.py` - Tushare `daily` full-market OHLCV
- `src/data/sources/tushare_minute_source.py` - Tushare 1-min bars
- `src/data/sources/tushare_metadata_source.py` - Stock metadata (trade_cal, suspend, stock_basic)
- `src/data/sources/local_concept_mapper.py` - Board ↔ stock mapping (local JSON)

---

### [DAT-002] Cache Pipeline (GreptimeDB)

**Status**: Completed

**Description**: Automated data download and caching pipeline. Downloads daily OHLCV (Tushare `daily`) and minute bars (Tushare `stk_mins`) into GreptimeDB for backtesting and ML training.

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

**Scheduling**: 凌晨 3 点 `CacheScheduler` 跑**统一的每日数据维护流水线**(toggle in Settings):刷名单(load-tushare)→ kimi 核新代码 + 回填 code_alias → 重建真值表(全历史**查漏**)→ 索引驱动**补缺**(只补 missing/wrong_suspended,绝不全量重下)→ 重建被补过的天确认 → 一条飞书汇总。查漏全量、补缺精准;步骤失败隔离;只在 3 点跑、不在 startup 跑;分钟线不在本流程(阶段三,单独触发)。详见 `docs/data-integrity-pipeline.md` §4.1。

**Gap Diagnosis Report**:
- 手动: `POST /api/audit/diagnose-gaps` 后台运行同一套诊断并发飞书。
- 自动: `CacheScheduler` 每次补全/完整性检查汇总发送后,立即运行按天详报并发飞书。
- 飞书正文: 日线问题日逐日列出“问题 / 根因 / 正确数字 / 怎么修”;分钟线只逐日展开 B 类“库漏存、可重下”的真错,C/PENDING 类按源头不足、半天/低成交量口径、待核对归类汇总,避免几百天明细刷屏。
- 完整明细: 每次运行覆盖写入 `data/audit/gap_diagnosis_report.json` 与 `data/audit/gap_diagnosis_report.md`,包含所有问题日、关键股票、分类原因、本地/源头分钟根数和修复动作。

**Files**:
- `src/data/services/cache_pipeline.py` - Download orchestration
- `src/data/services/cache_scheduler.py` - 3am auto gap-fill
- `src/data/services/cache_progress_reporter.py` - Phase tracking + Feishu notifications
- `src/data/services/download_task.py` - Background task state machine
- `src/data/clients/greptime_storage.py` - GreptimeDB storage (asyncpg pool)
- `scripts/diagnose_gaps.py` - 按天详报生成、完整明细落盘、飞书通知

**Checklist**:
- [x] GreptimeDB storage with asyncpg (3 mandatory overrides — see dev-conventions.md)
- [x] Cache pipeline with 8 phases
- [x] Cache scheduler (3am daily, configurable toggle)
- [x] Download resume by day (daily) and by stock (minute)
- [x] Per-day minute audit + backfill
- [x] Integrity validation on write
- [x] Feishu notifications for all scenarios
- [x] Post-run per-day gap diagnosis report to Feishu + full detail files
- [x] Dashboard data engine status card
- [x] Manual download with SSE progress + stop button
- [x] Suspended stock prev_close chaining (per-stock latest close, not global MAX date)

---

### [DAT-003] Stock Blacklist

**Status**: Completed

**Description**: Global stock blacklist. Blacklisted stocks are skipped in data download, scanning, and trading.

**Implementation**: Hardcoded `STOCK_BLACKLIST` frozenset in `src/common/config.py`. Applied at download, scan, and signal generation layers.

---

### [DAT-004] Listing-Info Auto-Verification (路径 B)

**Status**: Implemented (2026-05-30)

**Description**: 服务端后台 job,自动把 `stock_snapshot` 里尚未验证的代码喂给容器内 kimi-cli 查真实上市日 (IPO 首日),写回 `stock_listing_info`。`audit_daily_gaps` 用 `effective_universe = stock_snapshot − (list_date 未到 / delist_date 已过 的代码)` 算干净缺口;没有这张表,审计只能打 coverage warning。这是 CLAUDE.md §8#5 "live 扫描回退到读 cache" 三个前置条件里的第二条。

**GreptimeDB Tables** (补 DAT-002 未列的两张):

| Table | Content | Key |
|-------|---------|-----|
| `stock_snapshot` | 每日 B∪D∪S 三源并集 (bak_basic ∪ daily ∪ suspend_d) | `(ts, stock_code)` |
| `stock_listing_info` | 每代码 list_date / delist_date / verified / source | `stock_code` |

**验证逻辑**: 共享模块 `src/data/services/kimi_listing_verifier.py` 的 `run_kimi_for_code()` —— spawn `kimi --print --afk`(thinking **留开**,关了它搜索失败就放弃不绕道),让 kimi 用**全部工具**(SearchWeb→FetchURL→Shell)实证查清代码身份(在交易/退市/迁移/更名)+ 上市/退市日;解析失败**绝不猜** list_date。脱机脚本 `scripts/verify_list_date_kimi.py` 复用同一模块。

**Scheduling (已并入统一流水线, 2026-06)**: kimi 核验 `verify_unverified()` 现在是**每日凌晨 3 点数据维护流水线的第 ② 步**(`CacheScheduler` 顺序调用,刷名单后/重建前),原独立的 4 点 `ListingVerifyScheduler.run()` loop **已退休**(无独立 4 点跑、无 startup 跑)。守卫:`get_listing_verify_enabled()` 开关(管第 ② 步是否跑 kimi)、`kimi_available()`+`KIMI_API_KEY` 探测、单次 `MAX_CODES_PER_RUN`(默认 500)上限、**并发 1**、`upsert_listing_info` 小批 ≤200 行。(原 `cache_fill_running` 互斥锁已无意义——顺序跑不再并发抢内存。)`ListingVerifyScheduler` 类与方法保留,供流水线调用 + 手动端点 + 状态卡片。详见 `docs/data-integrity-pipeline.md` §4.1。

**失败处理**: 查不到/超时/解析失败 → 写 `verified=false` 占位行 (离开"未验证集",不再每天重烧 kimi) + 飞书通知该批 failed 代码清单。手动 `?include_failed=1` 可重验占位行。

**Endpoints**:
- `POST /api/audit/listing-info/verify` (X-API-Key) — 后台触发一次验证 (支持 `?include_failed=1`)
- `POST /api/audit/listing-info/verify-problems?states=&max=` — 把真值表异常代码喂 kimi 查清「怎么回事」
- `GET /api/audit/listing-info/status` — 覆盖率 + scheduler 状态 (设置页卡片消费)
- `GET /api/audit/listing-info/findings[?code=]` — kimi 逐只查到的「怎么回事」(名字/状态/说明/上市退市日)
- `GET /api/audit/listing-info/kimi-raw?code=` — 某代码上次 kimi 验证的完整原始 trace
- **认证**: 静态 Kimi-Code API key,容器启动时由 `KIMI_API_KEY` 环境变量生成 `~/.kimi/config.toml` (`kimi_config.ensure_kimi_config_from_env`);无 OAuth、无凭证上传端点 (旧的 upload/auth-bundle 端点已删)

**Files**:
- `src/data/services/kimi_listing_verifier.py` - 共享 kimi 验证 (run_kimi_for_code / finding_from_result / kimi_available)
- `src/data/services/kimi_config.py` - 启动时从 KIMI_API_KEY 现生成 kimi 配置 (静态 key,无 OAuth)
- `src/data/services/listing_verify_scheduler.py` - kimi 验证逻辑 `verify_unverified()`(现由 3 点统一流水线调用;含 kimi findings 报告)
- `src/web/audit_routes.py` - 触发 / 状态 / findings / kimi-raw 端点
- `src/data/clients/greptime_storage.py` - upsert_listing_info / get_unverified_codes_in_snapshot / get_failed_verified_codes / get_effective_universe_for_date

**Checklist**:
- [x] 共享 kimi 验证模块 (脚本 + scheduler 复用)
- [x] kimi 验证并入 3 点统一流水线第 ② 步(独立 4 点 loop 已退休;enable 开关保留)
- [x] 单次上限截断 (不静默,log + 飞书)
- [x] 失败写占位行 + 飞书 failed 清单
- [x] 手动触发端点 + 状态端点 + 设置页卡片
- [x] kimi 输出解析 + verify_unverified 流程单测

**临时工具 — 新旧索引对照验证 (TEMPORARY)**: 在用新索引 (三合一−kimi = `effective_universe`) 驱动历史数据的**补全/清理**之前,先把它跟旧索引 (之前用的 Tushare 列表 = `bak_basic` 每日权威列表) 逐日对照,确认新索引可信。
- 脚本 `scripts/compare_index_old_new.py` (`--start/--end/--feishu/--max-days`) + 临时接口 `POST /api/audit/index-compare` (X-API-Key,后台跑→飞书),共用 `compare_index_range`。
- 输出每日 `only_in_old` (新索引丢/挡的) 与 `only_in_new` (bak_basic 漏的,如北交所),delta 用 listing_info 标注原因 (未上市/已退市/未验证)。
- **修复 (补缺 + 清理 `backtest_daily` 里不该存在的行) 为后续步骤**,必须先通过本对照验证、且带人工确认闸 (删除不可逆) 才执行。确认完毕后此脚本 + 接口可删。

**stock_snapshot 历史回填**: `stock_snapshot` (索引基表) 是 2026-05-12 新增,历史几乎为空 → 索引在历史上不存在。`POST /api/audit/snapshot-backfill?start=2023-01-01&end=...` 把它回填到历史:只跑 snapshot (B∪D∪S,不碰分钟线)、resume-safe (跳已存在日期)、撞 Tushare 限频自动 sleep 续跑、后台执行起止发飞书。复用 `CachePipeline._sync_stock_snapshot`。与缓存补全互斥 (共享 `daily_source`/`metadata_source` httpx client,不能并发)。

**数据缺口诊断报告**: `POST /api/audit/diagnose-gaps` (+ `scripts/diagnose_gaps.py`) 把每日补全报的三类问题(股票数偏多 / 日线缺 / 分钟缺)逐条核查,产出"问题→根因→真实应该是多少→怎么修"的飞书报告。把每个缺的 (天,股) 用 `gap_classifier` 归成 **A 名单错**(未上市/已退市还挂在名单 → AI 上市日剔除)、**B 真缺**(在市未停牌却无数据 → 精准重下)、**C 本就无完整数据**(停牌补占位 / 上市首日半天交易记为已知正常)、**待 AI 核对**。分钟缺口天数多,详细分类抽样最近 N 天(透明标注,不静默截断)。
- 配套修复 `validate_integrity` 的"股票数"检查:从"全库历史累计 > 5500 报警"(误报,累计含退市股属正常)改为"**单日**在册数 > 5500 才报警";累计数仅作说明。

---

### [DAT-006] 交易日历真值表 (Trading Calendar Truth Table)

**Status**: In progress (2026-06-02)

**Description**: 维护一张**物化的、每天的权威真值表** `trading_calendar`,每个「交易日 × 股票」一行,如实记录这只票这天的真实状态。把"每天该有哪些票、各是什么状态"一次性算清存下,**所有缺口检查和数据补全都对照它做**,不再每次临时推断——"一查即知,不靠复杂推断"。取代 DAT-002/DAT-004 里 `audit_daily_gaps` 的即时重算 + 逐个堵窟窿。

**核心性质**: 表的"正确"= 每天的股票集合对 + 每只的真实状态如实记。某只票停牌、或源头无数据,**不影响表的正确性**——"停牌""源头无"本身就是被如实记录的状态,不是缺陷。

**复合 (reconcile) —— 全用权威源,不推断**。每个交易日 D:
1. **在册名单 (roster)** = `stock_listing_info` 中 `list_date ≤ D < delist_date` 的代码(Tushare stock_basic 官方上市/退市日)。
2. **停牌** = Tushare `suspend_d`(D)。
3. **真有成交** = Tushare `daily`(D)(整批,含所有板块)。成交量>0 是主集;**成交量=0 但有真实价格 bar**(一字板无成交等)且**不在停牌名单**的,也按"交易了"算——与写入端 `_process_daily_date` 的"有真实 bar 就存真实行"**同一口径**,否则这类票会被永久误判成 `wrong_traded`(库有真实行、reconcile 却认为源头查无)。成交量=0 且在停牌名单 → 按停牌算(写入端写的也是占位)。分钟完整性(`minute_state`)只对成交量>0 的票要求(vol=0 的票不强求 241 根,避免空转重试)。
4. **库里有什么** = `backtest_daily`(D)(区分 `is_suspended`)。
三者一碰定状态:
- daily 有真实成交(含上述 vol=0 真实 bar 且未停牌)→ `trade_status=trading`;库有真实行=`ok` / 库标成停牌=`wrong_suspended` / 库无=`missing`(真缺待补)。
- suspend_d 停牌、daily 无成交 → `trade_status=suspended`;库有占位=`ok` / 库无=`missing`(停牌没占位待补)。
- 在册、不停牌、daily 也查无 → `source_none`(源头也无,长期挂,不假装解决)。
- 有库数据但**不在当天 roster** → `orphan`(有数据却不在册,`listed=false`)。

**Schema** (`trading_calendar`, GreptimeDB):

| Column | 说明 |
|--------|------|
| `stock_code` | PRIMARY KEY (tag) |
| `ts` | TIME INDEX = **交易日本身** (epoch ms, naive-UTC)。`(stock_code, 交易日)` 唯一一行,重建某天**原样覆盖**——**不用写入时刻当 ts**(避免 `stock_listing_info` 那种同码双行/读闪 bug) |
| `listed` | 当天是否在册 (roster=true / orphan=false) |
| `trade_status` | `trading` 正常交易 / `suspended` 停牌 / `unknown` 源头无法定性 —— 这只票这天**共享**的标记 |
| `daily_state` | `ok` / `missing` 真缺 / `wrong_suspended` 标了停牌却有数据 / `orphan` 有数据却不在册 / `source_none` 源头也无 |
| `minute_state` | **预留**(后续):`ok` / `missing` / `source_short` 源头不足241 / `pending` |
| `reason` | 备注 |

**存全 + 读时筛(不在写入期筛选)**: 凡当天在任一源冒头的代码都进表打状态;写入**不做"该不该要"判断**——躲开历史上"筛选悄悄漏行"的坑(`is_suspended=NULL` 被 `WHERE` 排除、北交所被排除)。筛选只在"当索引用"时(读)发生,是这张全量表的视图:
- 「当天该有日线的名单」= `listed=true AND daily_state≠source_none`;
- 「当天能交易的名单」= `trade_status=trading`;
- 「当天数据问题」= `daily_state IN (missing, wrong_suspended, orphan)`。

**写入守卫(输入可疑就不写,宁可跳过)**:
- **空 traded 守卫**:某天(出自 `trade_cal` 的真交易日)Tushare `daily` 返回空、而在册名单非空 → 几乎可断定是源头取数失败(真交易日不可能零成交)→ **跳过该天 reconcile、不覆盖**,记 ERROR + 进 `skipped_days`(汇总可见)。不依赖"库里已有多少行"——新交易日首次 reconcile 时库里还没数据,同样受保护(否则一次空响应把整天写成 source_none,增量重建永不回头,整天数据静默永失)。
- **名单守卫**:`stock_listing_info` 行数异常少(< 3000,正常 L+D 约 7000+)→ 名单疑似被截断(如刷名单中途被打断)→ **整个 build 直接报错中止**,绝不在残缺名单上 reconcile(否则正常代码全成 orphan,而 orphan 是 purge 可删的)。
- **流水线依赖**:3 点流水线里 **① 刷名单失败 → ③④⑤⑥ 全跳过**(同 ③ 失败跳 ④⑤⑥ 的逻辑——绝不在可能残缺的名单/过期的索引上重建或补数)。

**用法**: 检查/补全对照"该有日线"视图比 `backtest_daily`,只补/只纠差异;**手动触发 = 重建索引**(重新复合);每天新补全从它起步。历史某天复合好即不变真值,只最近/新增日需重做。

**Endpoints** (规划): `POST /api/audit/calendar/rebuild?start=&end=`(后台重建→飞书);`GET /api/audit/calendar/status`(概览)。

**Files** (规划): `src/data/services/trading_calendar.py`(复合纯逻辑,可单测) / `greptime_storage.py`(建表 + upsert + 视图查询) / `audit_routes.py`(端点)。

**Checklist**:
- [ ] 建表(`stock_code` PK + 交易日 TIME INDEX,固定交易日为 ts 避免双行)
- [ ] 复合流程(roster ∩ 停牌 ∩ 成交 ∩ 库 → 状态),纯逻辑单测
- [ ] 全历史重建端点 + 状态端点
- [ ] 检查/补全改为对照真值表视图
- [ ] `minute_state` 接入(后续)

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

**Status**: Production-verified end-to-end (2026-05-05). Trigger 当前只暴露
HTTP endpoint，UI 按钮 / 飞书 / 定时任务接入在 backlog。

**Description**: 端到端 K 线技术面分析。主程序从 GreptimeDB 取最近 N 个交易日的
日 K，POST 给海外 AWS Lambda；Lambda 用 matplotlib 画图、上传公开读 S3、返回
URL；主程序拿 URL 调柏拉图AI vision LLM（OpenAI-compatible chat/completions，
**锁定 gpt-5.5-pro**——其他模型质量太差，不要切），返回中文技术面分析。

**Architecture & Why**:

```
trading-service (cn-shanghai VPC)        Lambda (us-east-1)
─────────────────────────────────        ─────────────────────────
1. POST /api/analyze-kline {code, days?, prompt?}
2. app.state.storage → OHLCV[N days from GreptimeDB]
3. POST {code, days, ohlcv[]} ─────────► 4. matplotlib 渲染 PNG
                                          5. S3 put_object (ACL=public-read)
   ◄────── { url, key } ─────────────── 6. 返回公开 URL
7. POST /v1/chat/completions (柏拉图AI)
   { image_url: { url } }
   ◄──── { analysis text }              [bltcy → gpt-5.5-pro]
```

**为什么数据国内查、画图存海外**：阿里云 OSS 大陆 region bucket 的默认域名从
2025-03-20 起对新用户禁止做数据 API 操作，必须绑定备案过的自定义域名才能给
公网（包括 LLM provider）拉图。把渲染 + 存储放到 AWS us-east-1 直接绕开 ICP
备案、`Content-Disposition: attachment` 强制下载、跨网延迟等一连串问题，
trading-service 只发 JSON over HTTPS。

**Components**:

| 部分 | 路径 | 说明 |
|------|------|------|
| Lambda 渲染服务 | `lambda-kline/` | Container Lambda，POST OHLCV → PNG → 公开 S3 URL，鉴权 `x-upload-token` |
| Orchestrator | `src/analysis/kline_llm.py` | `analyze_kline(storage, code, days, prompt?)`——**`storage` 必须由调用方传入**（复用 `app.state.storage`，不要在函数内 start/stop） |
| HTTP 入口 | `src/web/analysis_routes.py` | `POST /api/analyze-kline`，从 `request.app.state.storage` 注入连接池 |
| 配置 helpers | `src/common/config.py` | `get_/set_lambda_kline_url`, `get_/set_lambda_kline_token`, `get_/set_bltcy_api_key`, `get_bltcy_base_url`, `get_bltcy_model` |
| Web UI 配置入口 | `src/web/templates/settings.html` 「K 线技术面分析」卡片 | 三栏 URL / Token / Key，自带「测试」按钮，零重启生效 |
| Lambda CI 流水线 | `.github/workflows/lambda-kline-bootstrap.yml` | path-filtered（只在 `lambda-kline/**` 改动时跑）→ ECR build & push → `update-function-code`（function 不存在时跳过并打 warning） |
| AWS 一次性 bootstrap 手册 | `lambda-kline/README.md` | S3 / ECR / IAM role / Lambda function / Function URL 创建命令 |

**配置（trading-service 侧）** —— 优先级 持久化文件 > env var：

| 配置项 | 持久化文件（web UI 写入） | env var fallback |
|---|---|---|
| Lambda Function URL | `data/lambda_kline_url.txt` | `LAMBDA_KLINE_URL` |
| Lambda upload token | `data/lambda_kline_token.txt` | `LAMBDA_KLINE_TOKEN` |
| 柏拉图 API key | `data/bltcy_api_key.txt` | `BLTCY_API_KEY` |
| 柏拉图 base URL | （env-only） | `BLTCY_BASE_URL`（默认 `https://api.bltcy.ai/v1`） |
| 柏拉图模型 | （锁死，不要改） | `BLTCY_MODEL`（默认 `gpt-5.5-pro`，仅作应急逃生口） |

> 首选 Settings 页面配置——文件持久化重启不丢，「测试」按钮可即时验证。
> env var 留作 docker-compose 部署模板的 fallback。

**GitHub Secrets / Variables**（CI 自动构建 Lambda 镜像用）：
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` /
`AWS_LAMBDA_KLINE_ECR` / `AWS_LAMBDA_KLINE_FUNCTION` /
Repo Variable `LAMBDA_KLINE_ENABLED=true`（关闭即禁用 deploy job）。

**HTTP 接口**：

```
POST /api/analyze-kline
{ "code": "000001", "days": 30, "prompt": null }     # code 接受 6 位裸码或 .SZ/.SH 后缀（自动 strip）
↓ 200
{
  "code": "000001",
  "days": 30,
  "bars": 30,
  "image_url": "https://<bucket>.s3.us-east-1.amazonaws.com/kline/...png",
  "model": "gpt-5.5-pro",
  "analysis": "...中文分析文本..."
}
```

错误码：400 = 输入或配置缺失；502 = Lambda 或 bltcy 上游错误；503 = `app.state.storage` 未初始化（GreptimeDB 没连上）。

**默认超时**：Lambda render 60s，bltcy LLM 600s。新 prompt 下 LLM 端到端波动很
大：240s → 360s 都踩到过 timeout，600s 给足 buffer，不影响调用方（ANA-002 串行
调用，每只就算跑满 10 分钟也只有几个持仓，8am 开市前肯定跑完）。

**Backlog**:
- UI trigger（dashboard 按钮 / 飞书命令）—— ✅ 定时任务已由 ANA-002 兜住
- 渲染加分钟级、加 RSI / MACD 等指标
- 历史分析归档（落 GreptimeDB），支持 web UI 回看
- prompt 工程：把基本面 / 资金流 / 板块温度等结构化信号一起喂进去，不只看图

---

### [ANA-002] 盘前持仓日报 (Pre-Market Holdings Report)

**Status**: Completed (2026-05-05)

**Description**: 交易日早 8 点自动扫描当前 broker 持仓，对每只持仓股调用 ANA-001
生成 K 线图 + 技术面分析，并按【卖出 / 持有 / 增持】信号给出当日操作建议，逐只推送到
飞书群。**为什么放 8am 不放 15:00**：Tushare 日线接口 T-1 出数据，
`CacheScheduler` 3am 才把昨日数据补全；8am 跑可以直接用昨日收盘的完整 K 线，避免在
15:00 触发还要额外补一次今日数据。

**触发机制**:

| 入口 | 触发方式 | 备注 |
|------|---------|------|
| 自动调度 | 工作日 8:00 Beijing | `PreMarketReportScheduler.run()`，`asyncio` 后台任务 |
| 手动触发 | `POST /api/pre-market-report/run` | 立即执行一次（异步，立即返回 `{"started": true}`），**永远可用**——不受定时开关或交易日过滤影响，用于补发或调试 |
| 定时开关 | `POST /api/pre-market-report/toggle {enabled: bool}` | **仅控制 8am 自动**——关闭后 scheduler 写一行 `skipped` 日志，手动触发不受影响 |
| 持久化 | `data/pre_market_report_enabled.txt`（默认 true） | env fallback `PRE_MARKET_REPORT_ENABLED` |

**交易日判定**: 复用 `cache_scheduler._get_trading_calendar()`（Tushare `trade_cal` →
weekday fallback），周末/节假日不跑。

**消息格式**（每只持仓发 2 条飞书消息：原生图片 + Markdown 富文本卡片，串行发送）:

1. **图片消息** —— K 线 PNG 通过 `FeishuBot.send_image()` 上传到飞书
   `/api/send_image`，原生 image type，群里直接展示图片，不是 URL 链接。
2. **Markdown 卡片** —— 通过 `FeishuBot.send_markdown()` 走 `/api/send_markdown`，
   飞书富文本卡片渲染。结构：

```markdown
## 📊 {code} {name} ({YYYY-MM-DD})

| | |
|---|---|
| 持仓 | **{volume}** 股 |
| 成本 | ¥{avg_price} |
| 市值 | ¥{market_value} |

---

{LLM analysis with 卖出 / 持有 / 增持 三档信号}
```

> 早期版本曾用 text 消息 + URL 拼接，渲染丑且图片不展开，2026-05-05 改为
> 原生 image + markdown 双消息（FeishuBot 加 `send_image` / `send_markdown`）。

**边界处理**:

| 场景 | 行为 |
|------|------|
| 无持仓 | 静默跳过（不发飞书），`scheduler_log` 记 `no_positions` |
| Broker 未连接 | 发飞书失败告警 + `scheduler_log` 记 `failed` |
| 单只股票分析失败 | 跳过该股，发飞书错误消息，继续下一只（不让一只挂掉整个日报） |
| ANA-001 配置缺失（lambda/bltcy） | 发飞书告警 + `scheduler_log` 记 `failed` |
| 节假日 | 不触发 |

**速率**: 持仓串行处理（避免 LLM 同时跑撞超时），单只
~30s（图渲染 < 10s + LLM ~20s），10 只持仓约 5 分钟内全部发完。

**Files**:
- `src/data/services/pre_market_report_scheduler.py` — 主逻辑（参考 `cache_scheduler.py` 模式）
- `src/web/analysis_routes.py` — 新增 `POST /api/pre-market-report/run`
- `src/common/config.py` — `get_/set_pre_market_report_enabled()`
- `src/web/app.py` — startup 启动 task、shutdown cancel

**配置**:

| 配置项 | 持久化文件 | env var fallback | 默认 |
|--------|----------|------------------|------|
| 启用开关 | `data/pre_market_report_enabled.txt` | `PRE_MARKET_REPORT_ENABLED` | `true` |

**HTTP 接口**:

```
POST /api/pre-market-report/run                            # 全部持仓批量
POST /api/pre-market-report/run-one  {code}                # 单只持仓
POST /api/pre-market-report/toggle   {enabled: bool}       # 仅控 8am 自动
GET  /api/pre-market-report/status                         # 查状态
↓ 200
{ "started": true, "trigger": "manual" }                   # /run, /run-one
{ "enabled": true }                                        # /toggle
```

错误码：503 = `app.state.storage` 未就绪；409 = 上一次执行还没结束；
400 = `/run-one` 参数缺失。

**Dashboard UI**:「盘前持仓日报」卡片（在 index.html 主页），含开关、下次执行时间、
上次结果显示、「触发全部持仓 → 飞书」按钮、「单票 AI 评价」每只持仓一个按钮（点击
→ 推送该股票分析到飞书，30s 自动刷新持仓列表）。

**调度日志**: 与 cache/model 调度器共用 `scheduler_log` 表，name=`pre_market_report`，
result ∈ `{success, no_positions, failed, skipped}`。

---

## Module: Notes

### [NOTE-001] 交易笔记 (Trade Notes)

**Status**: Implemented (2026-05-07)

**Description**: 按股票组织的"一脉相承"交易历史 + 思考日志。三栏 master-detail UI：
左栏所有有事件的股票（按最近活跃排序）→ 中栏选中股票的全部事件（时间倒序）→
右栏选中事件的正文（markdown 渲染 + 编辑）。事件分两类：

- **自动事件（broker source）**: `POST /api/trading/buy` / `POST /api/trading/sell` 成功后，
  `routes.py` 立刻 INSERT 一行（`event_type='买入'/'卖出'`，price/qty/side 填入），
  正文为空——你点进去再补思路/反思。批量 `/buy-batch-by-amount` 暂不 hook（V1 简化）。
- **手动事件（user / ai source）**: 思考、盘中观察、复盘、AI 总结等，从前端 "+ 新事件"
  按钮或后端 API `POST /api/notes/{code}/events` 创建。

**Why GreptimeDB**: 数据 access pattern 是"按 code tag 过滤 + 按 ts 排序，append-mostly"，
正好命中 GreptimeDB 时序工作负载。**与之前否决用 GreptimeDB 替换 Trilium SQLite 不矛盾**——
那是 OLTP 全套（频繁 update / joins / FTS5 / triggers），这里只用 INSERT + UPSERT by
PRIMARY KEY + 简单 WHERE，完全在 GreptimeDB 舒适区。

**Schema**:

```sql
CREATE TABLE trade_notes (
    ts          TIMESTAMP TIME INDEX,        -- 事件时间
    code        STRING,                      -- 股票代码（无后缀，如 '605299'）
    event_id    STRING,                      -- uuid，编辑/删除主键
    event_type  STRING,                      -- 买入/卖出/盘中/复盘/思考/AI总结/其他
    source      STRING,                      -- 'broker' / 'user' / 'ai'
    title       STRING,                      -- 中栏短标签（如 "@18.30 x 500"）
    price       FLOAT64,                     -- 成交价（broker 事件填，其他 NULL）
    qty         INT64,                       -- 数量
    side        STRING,                      -- 'buy' / 'sell'
    content     STRING,                      -- 对内 markdown（私房话）
    content_external STRING,                 -- 对外 markdown（分享版本，仅 买入/卖出 用）
    author      STRING,                      -- 创建者标识
    deleted     BOOLEAN,                     -- 软删除（NULL 也视为未删，兼容老 ALTER）
    commission   FLOAT64,                    -- 佣金 (买/卖)，元
    transfer_fee FLOAT64,                    -- 过户费 (买/卖)，元
    stamp_tax    FLOAT64,                    -- 印花税 (仅卖)，元
    dividend     FLOAT64,                    -- 股息/股息税 净到手 (派息额 − 已扣税)，可负，仅卖出登记，元
    realized_pnl FLOAT64,                    -- 平仓收益 (仅卖)，元；按 上一次买入 比例分摊成本计算，用户可改
    PRIMARY KEY (code, event_id)
)
PARTITION ON COLUMNS (code) ()

-- 篇 view 手插的带时间戳卡片：和 trade_notes 解耦的独立 schema。
-- 卡片不是事件——既不出现在中栏「事件」list，也不参与买卖统计。
CREATE TABLE note_cards (
    ts       TIMESTAMP TIME INDEX,    -- 用户选的时间，决定篇 view 排序
    code     STRING,
    card_id  STRING,                  -- uuid
    content  STRING,                  -- markdown
    deleted  BOOLEAN,
    PRIMARY KEY (code, card_id)
)
```

**HTTP 接口**:

```
GET    /trade-notes                           # 三栏页面（HTML）
GET    /api/notes/stocks                      # 左栏：所有有事件的股票，按 last_ts DESC
PATCH  /api/notes/stocks/{code}               # 改股票代码：所有 live 事件迁到 {new_code}
GET    /api/notes/{code}/events               # 中栏：股票全部事件，按 ts DESC
GET    /api/notes/{code}/events/{event_id}    # 右栏：单条事件正文
POST   /api/notes/{code}/events               # 创建手动事件
PATCH  /api/notes/{code}/events/{event_id}    # 编辑（content/title/event_type/ts）
DELETE /api/notes/{code}/events/{event_id}    # 软删除
GET    /api/notes/{code}/cards                # 篇 view 手插的带时间卡片
POST   /api/notes/{code}/cards                # 创建卡片（content + ts_ms）
PATCH  /api/notes/{code}/cards/{card_id}      # 编辑卡片
DELETE /api/notes/{code}/cards/{card_id}      # 软删除卡片
```

**自动 hook 位置**:
- `src/web/routes.py` 的两个单笔 `place_order` 调用点（buy / sell endpoint），下单成功
  立刻 `store.upsert_broker_event_by_order_id(order_id=result.order_id, ...)`。
- `submit_buy_batch_by_amount` 返回后调 `store.import_today_filled_orders(broker, code_filter=…)`
  把当批 codes 的 FILLED 订单从 broker 拉回 trade_notes。
- 所有路径都用 `broker_<order_id>` 当 event_id → 单笔 hook、批量 hook、`/backfill-today`
  三处之间天然幂等，不会重复计数。
- 简化语义：单笔下单成功即写入（提交价 + 提交数量），不等成交回报；批量下单走
  broker.get_orders() 拿真实成交腿数据。少数部分成交/撤单情况会失真，后期加 reconciliation。

**为什么不分两张表（`trade_fills` vs `trade_notes`）**: 中栏需要把自动事件和手动事件
混排按时间倒序，单表 + `source` 列查询最简单；结构化字段（price/qty/side）对 manual
事件设为 NULL 即可，GreptimeDB 原生支持。

**未持久化历史交易**: broker 接口（`get_trades`）只返回今日成交，无历史 API。
**从部署日开始累积**——过去交易需要的话，前端 "+ 新事件" 手动补一条 `event_type='买入'`
即可。

**Files**:
- `src/notes/note_store.py` — GreptimeDB CRUD（复用 `app.state.storage` 连接池）
- `src/web/notes_routes.py` — 7 个 endpoint（HTML + 6 个 API）
- `src/web/templates/trade_notes.html` — 三栏布局 + vanilla JS
- `src/web/static/style.css` — `.trade-notes-*` 三栏样式（响应式：手机下变 drill-down）
- `src/web/routes.py` — buy/sell endpoint 内嵌 hook 调用（best-effort，失败只 log
  不影响下单返回）

**Backlog**:
- 成交回报 reconciliation（修正委托价 → 实际成交价）
- AI 自动写入（接入 AI 服务，定期/事件触发自动生成"复盘"事件）
- T+n 复盘提醒（cron 扫所有股票最近一次买入，T+1/T+3/T+5 飞书推链接）
- 全文搜索（GreptimeDB 暂无 FTS，先靠浏览器 ctrl+F；未来可加 like 模糊匹配）
- 跨股票分析（"按 signal 类型回归胜率"——需要 event_type 标签更结构化）

---

## Backlog

- [ ] Multi-account support
- [ ] Performance analytics
- [ ] Unit test coverage improvement
