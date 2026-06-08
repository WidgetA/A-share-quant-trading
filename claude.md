# A-Share Quantitative Trading System

A quantitative trading system for China A-share market with modular architecture for strategy, trading, and data management.

> **Detailed references** (read when working on related areas):
> - [docs/dev-conventions.md](docs/dev-conventions.md) — Architecture, project structure, CI/CD, code style, uv setup, glossary, testing
> - [docs/trading-safety-patterns.md](docs/trading-safety-patterns.md) — Forbidden/correct code patterns with examples
> - [docs/datetime-timezone-guide.md](docs/datetime-timezone-guide.md) — Date calculation examples, asyncpg timezone pitfalls
> - [docs/features.md](docs/features.md) — Feature specifications (check before any development)
> - [docs/backtest-data-engine.md](docs/backtest-data-engine.md) — **Backtest data engine architecture (English, start here for the data engine)**: mental model, module map, storage schema, write/reconcile path, truth table, nightly pipeline, read/adapter layer, safety invariants
> - [docs/data-integrity-pipeline.md](docs/data-integrity-pipeline.md) — 真值表 (trading_calendar) 为中心的数据完整性架构:三阶段流程、状态定义、复合规则、索引驱动补全、关键修复与坑(改数据管线前必读;backtest-data-engine.md 的深入细节)

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
3. **每日数据维护流水线** runs at 3am daily (`CacheScheduler`): 刷名单 → kimi 核新代码 → **增量**重建真值表(只 reconcile `max_date+1→T-1` 的新交易日,信任已物化的历史索引)→ 索引驱动补缺 → 确认。**增量查漏、精准补缺;绝不每晚全表重扫历史**(全量重建是手动端点的事——建底/换号/索引出错时)。见 `docs/data-integrity-pipeline.md` §4.1
4. **Live prev_close is always fetched live from Tushare `daily`** — never read from cache. Stale cache caused silent limit-up filter bypass on 2026-05-11 (002975 incident — see `MEMORY.md`)
5. **⚠️ Live 37d history 当前是临时方案** —— cache (stock_snapshot + stock_listing_info + 自动验证) 没完全跑通前,live 扫描走实时 Tushare `daily` 拉 37 次以解耦 cache 风险。等以下条件满足后**回退到读 cache**:
   - `stock_snapshot` 三源并集稳定 (B∪D∪S 每天可靠入库) ✅
   - `stock_listing_info` 服务端自动验证流程上线 (路径 B, kimi-cli 自动跑覆盖未验证代码) ✅ 已并入每日 3 点统一数据维护流水线第 ② 步 (见 §12 + `docs/data-integrity-pipeline.md` §4.1)
   - 至少跑过一周连续无人工干预的 daily audit, gaps==0 ← **剩余唯一门槛**
   切回 cache 时:  `_fetch_history_live` 删除, run_ml_live 改回 `storage.get_multi_day_history`

## 12. Listing-Info Auto-Verification (路径 B)

**目的**: 把 `stock_snapshot` 里还没验证的代码自动喂给容器内 kimi-cli 查真实上市日,写回 `stock_listing_info`,让 `audit_daily_gaps` 能算出干净的 effective universe。

- **Scheduler (已并入统一流水线, 2026-06)**: kimi 核验逻辑 `verify_unverified()` 现在是**每日凌晨 3 点「数据维护流水线」的第 ② 步**(`CacheScheduler` 顺序调用,见 `docs/data-integrity-pipeline.md` §4.1)——刷名单后、重建真值表前核新代码 + 回填 `code_alias`。原来独立的 4 点 `ListingVerifyScheduler.run()` loop **已退休**(不再有独立 4 点跑、不再有 startup 跑);`ListingVerifyScheduler` 类与 `verify_unverified()` 方法保留,供流水线调用 + 手动端点 + 状态卡片。
- **kimi-cli 是镜像里的硬依赖 (CRITICAL)**: kimi-cli 要 Python ≥3.13,所以整个 service 迁到 **Python 3.13**(`Dockerfile` 用 `python:3.13-slim`,`requires-python>=3.13`)。kimi-cli 是 `pyproject` 声明依赖 → `uv sync` 装进镜像。**CI 强制**:`tests/unit/data/services/test_kimi_cli_installed.py` 断言 `kimi --version` 能跑,Dockerfile builder 也 `RUN kimi --version`——没装/装坏直接 CI 红、镜像 build 失败,**杜绝部署出 path B 跑不起来的镜像**。
- **认证 = 静态 API key,不走 OAuth (CRITICAL, 2026-06 改)**: kimi 的认证用一个 **Kimi-Code API key**(`sk-kimi-…`,授权 `api.kimi.com/coding/v1`)。容器启动时 `ensure_kimi_config_from_env()`(`src/data/services/kimi_config.py`)读环境变量 `KIMI_API_KEY` **现生成** `~/.kimi/config.toml`:provider `type="kimi"` + `api_key=<key>` + **不写 `oauth` 字段**(无 oauth → kimi 直接用明文 key,源码 `llm.py:create_llm`),并把 `[services.moonshot_search]`/`[services.moonshot_fetch]` 也配上同一 key → **原生 SearchWeb/FetchURL 照常可用**(实测同一 key 同时授权聊天和搜索端点)。
  - **为什么换掉 OAuth**: 旧方案上传 OAuth 凭证,access_token 只活 **15 分钟**、refresh 设备绑定,无人值守跑着跑着 refresh_token 失效就废,只能人工重登。静态 key **永不过期、零交互登录**,这套故障路径整个消失。
  - **key 不进仓库/不进镜像**:只经 `KIMI_API_KEY` 环境变量注入(线上写在 `docker-compose.yml`,该文件 gitignore 且用户手管)。仓库和镜像里都没有 key。缺 key → 启动不起调度器 + 发一条告警(同"无 kimi-cli"的处理)。
- **运行期安全网**: app.py 启动 `kimi_available()` 探测 + `KIMI_API_KEY` 是否就绪;缺任一则不启动调度器 + 发一条启动告警。是否真跑由 `get_listing_verify_enabled()` 开关决定。
- **教训**: ① 别上线一个运行依赖不在部署镜像里的常驻调度器;CI 绿 ≠ 线上能跑——把"依赖可运行"做成 CI 断言。② 别给无人值守的常驻任务用 15 分钟过期、要交互续期的凭证;能用静态 key 就用静态 key。见 `MEMORY.md`。
- **验证逻辑**: 共享模块 `src/data/services/kimi_listing_verifier.py` 的 `run_kimi_for_code()` —— spawn `kimi --print --afk`(thinking 留开,关了它搜索失败就放弃不绕道),让 kimi 用全部工具(SearchWeb→FetchURL→Shell curl)实证查清**这代码是什么、现在什么情况(在交易/已退市/迁移/更名)+ 上市退市日**,解析失败**绝不猜**。脱机脚本 `scripts/verify_list_date_kimi.py` 复用同一模块。
- **失败处理**: 查不到/超时/解析失败 → 写 `verified=false` 占位行 (离开"未验证集",不再每天重烧 kimi) + 飞书通知。**kimi 工具/认证错误**(key 失效等)单独识别,**绝不**当"查不到"写占位,而是报错中止。手动 `?include_failed=1` 可重验占位行。
- **错误如实报告 (是什么错就报什么错, 2026-06)**: 失败原因**不准乱喊"凭证"**。① 认证只用精确 API 响应短语判定(`_AUTH_FAIL_PHRASES`: `error code: 401`/`invalid authentication`/`invalid_api_key`…)——**绝不**拿裸 `401`/`403`/`登录`/`凭证` 去全文 substring 匹配(它们在财经网页/URL 里到处都是,曾把成功跑的 100K trace 误报成"请检查 kimi 凭证")。② kimi 跑了但没答案 → 按真实情况报:`超时`/`退出码 N`/`无任何输出`/`输出解析失败`;输出极短(疑似启动/认证/配置类失败)单独标、提示看原始 trace(不猜具体认证串,靠可观测性)。③ 飞书 ② 步 + 中止告警显示**真实原因**,不再硬贴"请检查凭证"。教训见 `MEMORY.md`。
- **不靠超时判失败 + 时间预算批量 (2026-06)**: kimi-cli 失败也会给明确反馈(成功 TurnEnd / `401 invalid_authentication` / 明确查不到),所以**等它自己退出、从明确输出判定**,时间不当失败判据。`KIMI_VERIFY_TIMEOUT_SEC=1800` 只是**卡死兜底**(远超任何正常运行;触发也保留已产出部分写进 raw + 报"未退出",不丢弃)。单只深搜要几分钟,旧 180s 把答案掐断 → 假"超时"工具错误 + 攒成积压。流水线 ② **按时间预算 `time_budget_sec=_STEP_TIMEOUT_KIMI*0.85`**(不是按个数封顶)——能核多少核多少、用满预算优雅停(剩的下次继续),一晚清掉大积压而非每晚挤几个;慢的夜只少核几个、绝不撞 6h 硬上限算失败。`verify_unverified` 的 `max_codes` 退为高位安全上限。
- **报告 = kimi 自己写的「怎么回事」**: 每只代码 kimi 回一句人话(名字/状态/说明/日期)→ 落盘 `data/audit/kimi_findings/<code>.json`,飞书报告逐条列出,完整清单 `GET /api/audit/listing-info/findings`。**不准我自己拿 Tushare/网页逐只核异常代码当结论**——放 kimi 上去就是让它查、它写。
- **守卫**: `get_listing_verify_enabled()` 开关(管流水线第 ② 步是否跑 kimi)、`kimi_available()`+`KIMI_API_KEY` 探测、单次 `MAX_CODES_PER_RUN` 上限 (默认 500,截断必 log + 飞书,不静默)、**并发 1**、`upsert_listing_info` 小批 ≤200 行。(原 `cache_fill_running` 互斥锁已无意义——流水线顺序跑,kimi 与补全不再并发抢内存。)
- **手动触发**: `POST /api/audit/listing-info/verify` (X-API-Key) 或 `POST /api/audit/listing-info/verify-problems?states=` (查真值表异常代码);状态见 `GET /api/audit/listing-info/status` + 设置页卡片;kimi 逐只结论见 `GET /api/audit/listing-info/findings`,原始 trace 见 `?code=` 的 `kimi-raw`。

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
