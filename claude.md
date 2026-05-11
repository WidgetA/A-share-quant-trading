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
   - `stock_snapshot` 三源并集稳定 (B∪D∪S 每天可靠入库)
   - `stock_listing_info` 服务端自动验证流程上线 (路径 B, kimi-cli 自动跑覆盖未验证代码)
   - 至少跑过一周连续无人工干预的 daily audit, gaps==0
   切回 cache 时:  `_fetch_history_live` 删除, run_ml_live 改回 `storage.get_multi_day_history`

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
