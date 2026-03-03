# A-Share Quantitative Trading System

A quantitative trading system for China A-share market with modular architecture for strategy, trading, and data management.

> **Detailed references** (read when working on related areas):
> - [docs/dev-conventions.md](docs/dev-conventions.md) — Architecture, project structure, CI/CD, code style, uv setup, glossary, testing
> - [docs/trading-safety-patterns.md](docs/trading-safety-patterns.md) — Forbidden/correct code patterns with examples
> - [docs/datetime-timezone-guide.md](docs/datetime-timezone-guide.md) — Date calculation examples, asyncpg timezone pitfalls
> - [docs/features.md](docs/features.md) — Feature specifications (check before any development)
> - [docs/iFinD-HTTP-API-manual.txt](docs/iFinD-HTTP-API-manual.txt) — iFinD API reference (MUST read before any API call)

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

## 7. asyncpg Timezone Handling (CRITICAL)

**Core Rules (asyncpg has asymmetric TZ behavior that causes silent data corruption):**
1. **Query parameters**: Always use `datetime.replace(tzinfo=beijing_tz)` — NEVER subtract 8h from naive datetimes
2. **Display results**: Always use `+ timedelta(hours=8)` then `.replace(tzinfo=None)` for clean Beijing time
3. **Never assume system TZ**: Code must work correctly regardless of `TZ` environment variable
4. **FORBIDDEN**: Any naive datetime as asyncpg query parameter

> Detailed root cause, prohibited/correct patterns: [docs/datetime-timezone-guide.md](docs/datetime-timezone-guide.md)

## 8. Market Data Source Policy

**Core Principle: Follow the `data_source` toggle — use whichever source the user selects.**

| Toggle Value | Source | Adapter |
|-------------|--------|---------|
| `"ifind"` | THS iFinD HTTP API | `IFinDHttpClient` |
| `"akshare"` | tsanghi 沧海数据 (日线) + baostock (分钟线) | `AkshareHistoricalAdapter` |

Rules:
1. **One source per session** — no mixing sources within a single backtest/scan
2. **Adapter parity** — `AkshareHistoricalAdapter` must support same indicator set as `IFinDHttpClient`
3. **Live trading always uses iFinD** — toggle only affects backtesting
4. **Non-trading data** has its own sources: PostgreSQL for fundamentals, local JSON for boards
5. **tsanghi token** — configured via Settings page, persisted in `data/tsanghi_token.txt`

## 9. Volume Unit Convention (CRITICAL)

**ALL volume data in the system MUST be in 股 (shares), never in 手 (lots).**

| Data Source | Native Unit | Conversion |
|------------|-------------|------------|
| **tsanghi** `/daily/latest` | **手** (1手=100股) | ×100 at read time in `AkshareHistoricalAdapter` |
| **baostock** `query_history_k_data_plus()` | **股** | None |
| **iFinD** | **股** | None |

- Conversion at **adapter read layer**, not storage (raw cache keeps original values)
- **Cross-verify**: `early_volume(10min) / avg_daily_volume` should be ~0.05-0.30. If >1.0, units are wrong.

## 10. iFinD HTTP API Reference (CRITICAL)

**Before writing ANY iFinD API call, ALWAYS read the manual first:**
- [docs/iFinD-HTTP-API-manual.txt](docs/iFinD-HTTP-API-manual.txt) (machine-readable, for quick lookup)
- [docs/iFinD-HTTP-API-manual.pdf](docs/iFinD-HTTP-API-manual.pdf) (official PDF)

**Mandatory**: Verify endpoint URL, ALL parameter names, ALL indicator names against manual. Do NOT guess.

Key endpoints: `basic_data_service`, `date_sequence`, `cmd_history_quotation`, `high_frequency`, `real_time_quotation`, `snap_shot`, `smart_stock_picking`, `get_trade_dates`, `report_query`

**Important differences** (common source of bugs):
- History uses `close`; real-time uses `latest` for current price
- History uses `volume`/`amount`; snapshot uses `vol`/`amt` (tick) AND `volume`/`amount` (cumulative)
- `name` is NOT a valid indicator for any quote endpoint

## 11. Board/Concept Data — ALL from Local Files

- `data/sectors.json` — THS board names (concept/industry/region)
- `data/board_constituents.json` — board → constituent stocks mapping
- `src/data/sources/local_concept_mapper.py` — reads both files, builds forward+reverse index
- **FORBIDDEN**: Runtime API calls for board data (no iwencai, no AkshareConceptMapper)

## 12. Fundamentals Data — ALL from PostgreSQL

- **All fundamentals (PE, growth, etc.) from PG table `stock_fundamentals`** — NEVER call iwencai/smart_stock_picking at runtime
- Read methods in `src/data/database/fundamentals_db.py`
- Fields: stock_code, company_name, pe_ttm, ps_ttm, pb, total_market_cap, roe, annual_revenue_yoy, quarterly_revenue_yoy, annual_net_profit_yoy, quarterly_net_profit_yoy, report_date_annual, report_date_quarterly, updated_at
