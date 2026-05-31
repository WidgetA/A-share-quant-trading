# A-Share Quantitative Trading System

A quantitative trading system for China A-share market with modular architecture for strategy, trading, and data management.

> **Detailed references** (read when working on related areas):
> - [docs/dev-conventions.md](docs/dev-conventions.md) — Architecture, project structure, CI/CD, code style, uv setup, glossary, testing
> - [docs/trading-safety-patterns.md](docs/trading-safety-patterns.md) — Forbidden/correct code patterns with examples
> - [docs/datetime-timezone-guide.md](docs/datetime-timezone-guide.md) — Date calculation examples, asyncpg timezone pitfalls
> - [docs/features.md](docs/features.md) — Feature specifications (check before any development)

## 1. Development Workflow Rules

#### Rule 1: Documentation First
**CRITICAL: Always update documentation BEFORE implementing code changes.**
- Update `README.md` for user-facing changes
- Update `docs/features.md` for feature changes
- Update `CLAUDE.md` for development process changes

#### Rule 2: CI Status Tracking
**CRITICAL: After every `git push`, track CI status until success.**
- Command to check: `gh run list --limit 1`
- Do NOT consider the task complete until CI passes

## 2. File Naming Convention

- All file names in **English**
- Python files: `snake_case.py` | Config files: `kebab-case.yaml` | Class files: `PascalCase.py`

## 3. Feature Documentation Workflow

- **Before any development**: Check `docs/features.md` for alignment
- **New features**: Update `docs/features.md` first → get confirmation → implement
- **Bug fixes**: Update feature doc if behavior changes

## 4. Development Checklist

Before starting any task: read `docs/features.md` → confirm alignment → check affected modules → ensure decoupling → write comments → add tests → verify CI.

> Full checklist and project structure: [docs/dev-conventions.md](docs/dev-conventions.md)

## 5. Trading Safety Priority Principle (CRITICAL)

**Core Principle: Trading Safety > Program Robustness. Let it crash rather than trade incorrectly.**

Rules:
1. **Stop rather than trade incorrectly** — uncertain order status → halt, incomplete data → skip
2. **Miss opportunities rather than take risks** — no stale/cached data for trading decisions
3. **Fail fast** — terminate on anomalies, explicit errors over silent degradation
4. **Audit first** — all trading ops must have complete logs. Run: `uv run python scripts/audit_trading_safety.py`
5. **NEVER** write `except Exception` that swallows errors silently in trading paths
6. **NEVER** return `{}`, `[]`, `None` as fallback when data fetch fails in trading paths

> Forbidden/correct code patterns with examples: [docs/trading-safety-patterns.md](docs/trading-safety-patterns.md)

## 6. Date and Time Handling (Beijing Time)

**Core Rules:**
1. **Never trust AI's internal date** — always execute `datetime.now(ZoneInfo("Asia/Shanghai"))` to get current time
2. **Always use Beijing timezone** — A-share market operates on UTC+8
3. **Calculate relative dates via code** — "yesterday", "this Tuesday" must be computed, not guessed

> Full examples and calculation patterns: [docs/datetime-timezone-guide.md](docs/datetime-timezone-guide.md)

## 7. asyncpg + GreptimeDB Timezone Handling (CRITICAL)

**Core Rules (all GreptimeDB queries use epoch ms integers to avoid asyncpg's TZ pitfalls):**
1. **Query parameters**: Convert dates/times to epoch ms via `calendar.timegm()` — NEVER pass datetime objects
2. **Result conversion**: Use `datetime.fromtimestamp(ms/1000, tz=timezone.utc)` — no manual `+8h` arithmetic
3. **ts column convention**: Timestamp strings treated as naive UTC → `calendar.timegm()` → epoch ms
4. **FORBIDDEN**: Any datetime object as asyncpg query parameter; any `- timedelta(hours=8)` offset hack

> Detailed root cause, prohibited/correct patterns: [docs/datetime-timezone-guide.md](docs/datetime-timezone-guide.md)

## 8. Market Data Source Policy

**Data sources by purpose:**

| Purpose | Source | Adapter |
|---------|--------|---------|
| Backtest (daily) | Tushare Pro `daily` | `TushareDailySource` → `CachePipeline` → `GreptimeBacktestStorage` |
| Backtest (minute) | Tushare Pro `stk_mins` 1min | via `GreptimeBacktestStorage` + `CachePipeline` |
| Live (realtime) | Tushare Pro `rt_min_daily` | `TushareRealtimeClient` |
| Live (prev_close) | Tushare Pro `daily` (live, not cached) | `_resolve_prev_close` in `ml_strategy_service.py` |
| Live (37d history) | **临时**: Tushare Pro `daily` 实时并发拉 (37 calls/扫描) | `_fetch_history_live` in `ml_strategy_service.py` |
| Board/concept + stock names | Local JSON files | `LocalConceptMapper` |

Rules:
1. **Non-trading data** has its own sources: local JSON for boards and stock names
2. **Single Tushare token** powers realtime quotes, daily OHLCV, suspend_d, bak_basic, stk_mins
3. **Cache scheduler** auto-fills missing dates at 3am daily (from 2024-01-01)
4. **Live prev_close is always fetched live from Tushare `daily`** — never read from cache. Stale cache caused silent limit-up filter bypass on 2026-05-11 (002975 incident — see `MEMORY.md`)
5. **⚠️ Live 37d history 当前是临时方案** —— cache (stock_snapshot + stock_listing_info + 自动验证) 没完全跑通前,live 扫描走实时 Tushare `daily` 拉 37 次以解耦 cache 风险。等以下条件满足后**回退到读 cache**:
   - `stock_snapshot` 三源并集稳定 (B∪D∪S 每天可靠入库) ✅
   - `stock_listing_info` 服务端自动验证流程上线 (路径 B, kimi-cli 自动跑覆盖未验证代码) ✅ `ListingVerifyScheduler` (每日 4am, 见 §12)
   - 至少跑过一周连续无人工干预的 daily audit, gaps==0 ← **剩余唯一门槛**
   切回 cache 时:  `_fetch_history_live` 删除, run_ml_live 改回 `storage.get_multi_day_history`

## 12. Listing-Info Auto-Verification (路径 B)

**目的**: 把 `stock_snapshot` 里还没验证的代码自动喂给容器内 kimi-cli 查真实上市日,写回 `stock_listing_info`,让 `audit_daily_gaps` 能算出干净的 effective universe。

- **Scheduler**: `ListingVerifyScheduler` (`src/data/services/listing_verify_scheduler.py`),每日 **4am** (3am 缓存补全之后,snapshot 已新鲜) + startup 各跑一次。
- **kimi-cli 是镜像里的硬依赖 (CRITICAL)**: kimi-cli 要 Python ≥3.13,所以整个 service 迁到 **Python 3.13**(`Dockerfile` 用 `python:3.13-slim`,`requires-python>=3.13`)。kimi-cli 是 `pyproject` 声明依赖 → `uv sync` 装进镜像。**CI 强制**:`tests/unit/data/services/test_kimi_cli_installed.py` 断言 `kimi --version` 能跑,Dockerfile builder 也 `RUN kimi --version`——没装/装坏直接 CI 红、镜像 build 失败,**杜绝部署出 path B 跑不起来的镜像**。
- **运行期安全网**: app.py 启动仍 `kimi_available()` 探测,万一缺失则不启动调度器 + 发一条启动告警(正常情况 kimi 已在镜像里,这条不该触发)。是否真跑由 `get_listing_verify_enabled()` 开关决定。
- **教训**: 别上线一个运行依赖不在部署镜像里的常驻调度器;CI 绿 ≠ 线上能跑——所以把"依赖可运行"做成 CI 断言。见 `MEMORY.md`。
- **验证逻辑**: 共享模块 `src/data/services/kimi_listing_verifier.py` 的 `verify_one_code()` —— spawn `kimi --print --afk --no-thinking`,强制 SearchWeb 实证,解析失败**绝不猜** list_date。脱机脚本 `scripts/verify_list_date_kimi.py` 复用同一模块。
- **失败处理**: 查不到/超时/解析失败 → 写 `verified=false` 占位行 (离开"未验证集",不再每天重烧 kimi) + 飞书通知该批 failed 代码清单。手动 `?include_failed=1` 可重验占位行。
- **守卫**: `get_listing_verify_enabled()` 开关、`kimi_available()` 探测、与 `cache_fill_running` 互斥 (1.58G 小机器)、单次 `MAX_CODES_PER_RUN` 上限 (默认 500,截断必 log + 飞书,不静默)、**并发 1**(每个 kimi 是完整 LLM-agent 子进程,1.58G 小机器跑多个会 OOM→重启死循环,串行慢但安全)、`upsert_listing_info` 小批 ≤200 行。
- **手动触发**: `POST /api/audit/listing-info/verify` (X-API-Key);状态见 `GET /api/audit/listing-info/status` + 设置页卡片。

## 9. Volume Unit Convention (CRITICAL)

**ALL volume data in the system MUST be in 股 (shares), never in 手 (lots).**

| Data Source | Native Unit | Conversion |
|------------|-------------|------------|
| **Tushare Pro** `daily` | **手** (1手=100股) | ×100 at read time in `GreptimeHistoricalAdapter` |
| **Tushare Pro** `stk_mins` | **股** | None (already in 股) |
| **Tushare Pro** `rt_min_daily` | **股** | None |

- Conversion at **adapter read layer**, not storage (raw cache keeps original values).
- **Cross-verify**: `early_volume(10min) / avg_daily_volume` should be ~0.05-0.30. If >1.0, units are wrong.

## 10. Board/Concept Data — ALL from Local Files

- `data/sectors.json` — THS board names (concept/industry/region)
- `data/board_constituents.json` — board → constituent stocks mapping
- `src/data/sources/local_concept_mapper.py` — reads both files, builds forward+reverse index
- **FORBIDDEN**: Runtime API calls for board data (no iwencai, no AkshareConceptMapper)

## 11. Stock Names — from Local JSON

- Stock code → company name mapping: `LocalConceptMapper.get_stock_name(code)`
- Source: `data/board_constituents.json` (same file used for board/concept data)
- **No PostgreSQL dependency** — fundamentals_db was removed (ML scanner doesn't need PE/growth data)
