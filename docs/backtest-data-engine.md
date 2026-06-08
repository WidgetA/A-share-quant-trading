# Backtest Data Engine — Architecture

> **Read this first when working on historical (backtest) market data.** This is the
> top-level design + architecture of the engine that ingests A‑share daily and minute
> bars, stores them correctly in GreptimeDB, keeps them provably complete via a
> per‑`(day × stock)` truth table, and serves them back to backtests / the ML scanner.
>
> Scope: **historical / backtest data only.** Live trading uses a separate realtime path
> (`TushareRealtimeClient.rt_min_daily`, see CLAUDE.md §8) and is out of scope here.
>
> Companion docs (deeper detail on specific layers):
> - `docs/data-integrity-pipeline.md` — exhaustive truth‑table reconcile rules + the full
>   list of hard‑won pitfalls (§8). **Chinese, but the definitive lesson log.** Read it
>   before changing reconcile / fill logic.
> - `docs/greptimedb-guide.md` — GreptimeDB usage rules.
> - `docs/datetime-timezone-guide.md` — asyncpg + epoch‑ms timezone handling.
> - CLAUDE.md §7 (asyncpg/TZ), §8 (data‑source policy), §9 (volume units).

---

## 1. Mental model (the one thing to understand)

The engine is built around **one materialized truth table, `trading_calendar`**, that says —
for every trading day × every stock — *what the data **should** be* and *whether the DB has
it right*. Everything else (gap detection, backfill, audits) is a cheap comparison against
this table instead of an ad‑hoc recompute.

```
  Authoritative sources (Tushare)            Materialized truth            Consumers
  ───────────────────────────────            ──────────────────            ─────────
  stock_basic   ─┐ list/delist (roster)
  suspend_d     ─┤ suspended that day
  daily (by day)─┼──►  reconcile (pure fn) ──►  trading_calendar   ──►  gap check / backfill
  ──────────────│      per (day, stock)         (1 row per day×code)      audits / reports
  backtest_daily┘ what the DB actually has
                                                       ▲
  backtest_daily / backtest_minute  ── the actual data; truth table indexes its correctness
```

Two responsibilities are deliberately split:

- **`backtest_daily` / `backtest_minute` = the data** (a row exists ⇒ we have that bar).
- **`trading_calendar` = the index/truth** (what each `(day, code)` *should* be + the DB's
  current correctness state). It can express "should exist but is genuinely missing" — a
  state the data tables alone cannot represent (no row = ambiguous).

Gap check / backfill = compare *should* (calendar) vs *actual* (data tables); the diff is
exactly what to fill or fix. This replaced an older approach that recomputed
`effective_universe − backtest_daily` on every check and could only fill all‑empty days.

---

## 2. Module map (where everything lives)

```
src/data/
├── sources/                         # Tushare API wrappers (own lifecycle + normalization)
│   ├── tushare_daily_source.py      # daily OHLCV  → fetch_day(date) -> (records, failures)
│   ├── tushare_minute_source.py     # stk_mins 1m  → fetch_batches(codes, s, e) (30/req, 0.5s)
│   ├── tushare_metadata_source.py   # trade_cal / suspend_d / bak_basic
│   └── local_concept_mapper.py      # board/concept + stock names (local JSON; not this engine)
├── clients/
│   ├── tushare_realtime.py          # HTTP client → Tushare Pro (fetch_daily, stk_mins,
│   │                                #   suspend_d, trade_cal, stock_basic; _to_ts_code; retry)
│   ├── greptime_storage.py          # GreptimeBacktestStorage: all DDL + CRUD + audits +
│   │                                #   gap detection; owns GreptimeClient (asyncpg pool)
│   └── greptime_historical_adapter.py  # READ-only projection for the ML scanner (×100 vol)
└── services/
    ├── cache_pipeline.py            # CachePipeline: write orchestration. download_prices()
    │                                #   (coarse, legacy) + fill_daily/minute_from_calendar()
    │                                #   (index-driven, current). _process_daily_date() = the
    │                                #   real-vs-suspended decision.
    ├── trading_calendar.py          # reconcile_day() (pure) + build_calendar()/rebuild_calendar()
    ├── cache_scheduler.py           # CacheScheduler: the nightly 3am maintenance pipeline
    ├── cache_progress_reporter.py   # Phase enum + progress callbacks + Feishu (silenceable)
    ├── listing_verify_scheduler.py  # kimi verify_unverified() (step ② of the nightly)
    └── kimi_listing_verifier.py     # kimi-cli backstop for unresolved codes
scripts/
└── load_listing_from_tushare.py     # run_load_listing(): rebuild the roster (stock_basic L+D)
src/web/audit_routes.py              # all /api/audit/* manual endpoints (rebuild, backfill, ...)
```

---

## 3. Storage schema (GreptimeDB, all in `greptime_storage.py`)

GreptimeDB tables are `PRIMARY KEY (tag)` + `TIME INDEX (ts)`. **`(tag, ts)` deduplicates —
last write wins.** All `ts` are **epoch milliseconds, naive‑UTC** (see §4).

| Table | Key | `ts` means | Columns / purpose |
|-------|-----|-----------|-------------------|
| `backtest_daily` | `stock_code` | **trade date** (day) | `open/high/low/close/pre_close FLOAT64`, `vol`, `amount`, `turnover_ratio`, `is_suspended BOOL`. The daily data. `vol` in **手 (lots)**. |
| `backtest_minute` | `stock_code` | **bar minute** | `open/high/low/close`, `vol`, `amount`. Raw 1‑min bars, no aggregation. `vol` in **股 (shares)**. |
| `trading_calendar` | `stock_code` | **trade date** | `listed BOOL`, `trade_status`, `daily_state`, `minute_state`, `reason`. **The truth table** (see §6). |
| `stock_listing_info` | `stock_code` | **fixed `0`** | `name`, `list_date`, `delist_date`, `verified BOOL`, `source`. The roster source. ts pinned to 0 so each code is one upsert‑able row. |
| `code_alias` | `old_code` | **fixed `0`** | `new_code`, `change_date`, `note`, `source`. Old→canonical code map (renames/migrations). |
| `stock_snapshot` | `stock_code` | day | Per‑day union `bak_basic ∪ daily ∪ suspend_d`. Legacy audit input; truth‑table roster now comes from `stock_listing_info`. |
| `scheduler_log` | `name` | run time | `trigger_type`, `result`, `message`. Survives restarts so the dashboard shows last run. |

**Why `ts` must be the business date, never the write time:** GreptimeDB dedups on
`(PRIMARY KEY, ts)`. If `trading_calendar` / `stock_listing_info` stamped `now()` as `ts`,
the same code would accumulate multiple rows (old placeholder + new) and reads would
randomly pick one → flickering counts. Fix: calendar `ts = trade date`; listing/alias
`ts = 0` (fixed). See `data-integrity-pipeline.md` §8 (listing dual‑row bug).

---

## 4. Conventions: time & volume units (get these wrong → silent corruption)

**Time — epoch ms, naive UTC** (helpers in `greptime_storage.py`):
- `date_to_epoch_ms(d) = calendar.timegm(d.timetuple()) * 1000` — date → midnight‑UTC ms.
- `minute_str_to_epoch_ms("YYYY-MM-DD HH:MM:SS")` — same `calendar.timegm` convention, so
  `minute_str_to_epoch_ms("2026-04-09 00:00:00") == date_to_epoch_ms(date(2026,4,9))`. Day
  boundary math is therefore trivial: `day_start = date_to_epoch_ms(d); day_end = day_start + 86_400_000`.
- `ts_to_date` / `ts_to_epoch_ms` convert asyncpg results back.
- **Never pass a `datetime` object as an asyncpg query param, and never do `±8h` offset
  hacks** — always epoch‑ms ints (CLAUDE.md §7).

**Volume — daily is 手 (lots), minute is 股 (shares):**
- `backtest_daily.vol` is stored in **手** (Tushare `daily` native unit). It is converted
  **×100 → 股 at the read layer**, at every daily read site:
  - `greptime_historical_adapter.py` `history_quotes` (the `volume` indicator)
  - `greptime_storage.get_multi_day_history` (`vol * 100`)
  - and the live‑history / kline paths.
- `backtest_minute.vol` is stored in **股** already (Tushare `stk_mins` native) — **no
  conversion** on read.
- Sanity check (CLAUDE.md §9): `early_volume(10min) / avg_daily_volume ≈ 0.05–0.30`. If `>1`,
  a unit is wrong.

---

## 5. The connection layer (asyncpg ⟷ GreptimeDB)

`GreptimeClient` (in `greptime_storage.py`) wraps an asyncpg pool. GreptimeDB speaks the
PostgreSQL wire protocol but is **not** full Postgres; three overrides are mandatory or the
pool silently wedges (each cost a full day of debugging — see CLAUDE.md §7 / MEMORY):

- `statement_cache_size=0` — GreptimeDB has no `PREPARE` / `DEALLOCATE`.
- `connection_class=_GreptimeConnection`:
  - `reset()` → **no‑op** (pool's `RESET ALL` / `DEALLOCATE ALL` are rejected).
  - `close()` → **`terminate()`** (TCP reset). Plain `close()` sends PG `Terminate` and waits
    for the server to close the socket — GreptimeDB never does → hangs forever. The pool
    recycles a connection after `max_queries` (default 50000), so this *will* trigger under
    sustained writes if not overridden.
- A **slow‑SQL watchdog** sibling task logs `"SQL still running…"` every 30s, and
  `pool.acquire` / query / `pool.release` each get their own `wait_for` timeout (acquire 30s,
  query 600s, release 10s) — because asyncpg's C extension can hang on `socket.recv` and
  swallow `CancelledError`, so a bare `wait_for` cancel is not reliable. Without the watchdog
  a hang is invisible (uvicorn doesn't configure the root logger either — the app adds a
  `StreamHandler` at startup).

**Write batching (GreptimeDB silently drops INSERTs > ~200 rows):** daily = 1 row/insert;
minute = **100**/batch; `trading_calendar` / `code_alias` = **200**/batch; listing = 1/insert.
Stay under the cliff — exceeding it loses rows with no error.

---

## 6. The truth table & reconcile (`trading_calendar.py`)

### States

`trade_status` (this stock, this day — shared across data types): `trading` / `suspended` /
`unknown` (source can't classify).

`daily_state` (does the DB match the source?):

| value | meaning | fixable? |
|-------|---------|----------|
| `ok` | DB matches source | — |
| `missing` | source (Tushare `daily`) has it, DB doesn't → genuine gap | ✅ backfill |
| `wrong_suspended` | DB has a suspended placeholder, but source traded that day → should be a real bar | ✅ re‑download over placeholder |
| `wrong_traded` | DB has a real row, but source shows no trade & no suspension → suspicious | ⚠️ inspect |
| `orphan` | DB has data but the code is **not on that day's roster** (pre‑list / delisted) | ⚠️ inspect (usually delist boundary) |
| `source_none` | rostered, not suspended, source *also* has nothing → unfillable | ❌ skip (self‑heals, see §8) |

`minute_state` (**only for codes that traded that day**; `NULL` otherwise = no minute expected):
`ok` (≥ `MINUTE_BARS_FULL = 241` bars) / `missing` (< 241, fillable) / `source_short`
(re‑downloaded and the source genuinely returns < 241 = half‑day session; never retried).

### `reconcile_day()` — a pure function (fully unit‑tested, no I/O)

For trading day `D`, given five authoritative inputs:

```
roster       = stock_listing_info where list_date ≤ D < delist_date
suspended    = Tushare suspend_d(D)
traded       = Tushare daily(D) with volume > 0
db_normal    = backtest_daily(D) where is_suspended = false
db_suspended = backtest_daily(D) where is_suspended = true

for each code in roster:
    if code in traded:        trade_status = trading
                              daily_state = ok | wrong_suspended | missing
    elif code in suspended:   trade_status = suspended
                              daily_state = ok | wrong_traded | missing
    else (rostered, source shows neither trade nor suspension):
                              trade_status = suspended(if db placeholder) else unknown
                              daily_state  = wrong_traded(if db real) else source_none
for codes in DB but NOT in roster:  listed = false, daily_state = orphan
```

**Truth of "did it trade" is Tushare `daily`.** The DB is judged *against* that, never the
reverse. `build_calendar()` is the thin I/O runner that pulls the inputs per day and
upserts; `rebuild_calendar()` wires the Tushare client and resolves a date range to trading
days. Minute reconcile (`with_minute=True`) additionally reads `get_minute_bar_counts(day)`
(one GROUP BY over the ~1.8B‑row minute table, ~15–30 s/day → deliberate, not blanket).

### Store‑all, filter‑on‑read (the core anti‑corruption principle)

- **Write phase makes no "should we keep this?" decision.** Every code that appears in any
  source that day is written with a state. The class of bug we keep eliminating is a filter
  silently dropping rows (`is_suspended IS NULL` excluded by a `WHERE`; 北交所 codes
  excluded). Don't filter at write time.
- **Filtering happens only when the table is *read as an index*** (it's a view over the full
  table): "should have daily today" = `listed AND daily_state ≠ source_none`; "tradable" =
  `trade_status = trading`; "has a problem" = `daily_state IN (missing, wrong_suspended, orphan)`.

---

## 7. The write path

### 7.1 Sources

- **`TushareDailySource.fetch_day(date)`** → `(records, failures)`; one Tushare `daily` call
  returns the **entire market** for that date (all boards incl. 北交所). Volume in 手.
  ⚠️ `daily` only goes to **T‑1** — today's bars aren't published, so any rebuild/fill must
  end at **yesterday** or it will mark all of today's roster `missing`.
- **`TushareMinuteSource.fetch_batches(codes, start, end)`** → yields `MinuteBatchResult`
  per HTTP call. Per‑**code** API (`stk_mins`), batched **30 codes/request** (`BATCH_SIZE=30`,
  30×241 = 7230 rows < 8000 cap), `REQUEST_DELAY=0.5s` (≤ 200 req/min). Minute volume in 股.
- **`TushareMetadataSource`** → `fetch_trade_calendar` (`trade_cal`), `fetch_suspended`
  (`suspend_d`), `fetch_listed_stocks` (`bak_basic`).
- **`TushareRealtimeClient`** (shared HTTP client): `TIMEOUT=60s`, `MAX_RETRIES=3` with
  exponential backoff; **API‑level errors (`code != 0`) raise immediately and are NOT
  retried** (only network errors retry). `_to_ts_code` suffix rule is critical:
  `6xxxxx→.SH`, `4x/8x/92xxxx→.BJ` (北交所), else `.SZ`. A wrong suffix → Tushare returns
  empty → per‑code minute looks "missing" forever.

### 7.2 The roster — `scripts/load_listing_from_tushare.py`

`run_load_listing()` rebuilds `stock_listing_info` authoritatively from Tushare
`stock_basic` (list_status `L` + `D`), with `list_date` / `delist_date`. It **fails fast on
an empty listed list** (API hiccup must not wipe the roster), **truncates then re‑inserts**,
and **preserves kimi‑written rows** (查不到 placeholders, plus the new‑IPO/active codes kimi
resolved that `stock_basic` doesn't yet carry) across the truncate — **EXCEPT it DROPS any
`code_alias` old_code**. A migrated/renamed code's identity + history belong to the new code,
so the old code must leave the roster entirely; this is what makes `code_alias` authoritative
for roster exclusion (wired in at the one build point → `roster_for_day` /
`get_effective_universe_for_date` / `audit_daily_gaps` all exclude it for free), so migrated
老北交所 codes stop lingering as perpetual `source_none`. This is the roster `reconcile_day` reads.

### 7.3 `CachePipeline._process_daily_date()` — real‑vs‑suspended decision

Wired with `(storage, daily_source, minute_source, metadata_source, reporter)`. The decision
for each ticker on a day:

1. **`code_alias` remap first** — re‑key an old/migrated code to its canonical code so its
   history lands under the live code (never becomes an orphan).
2. **`volume > 0` ⇒ write the real bar** — **trust the real trade over `suspend_d`.** Tushare
   `daily` and `suspend_d` can contradict each other (a code with real volume also listed as
   suspended); writing a suspended placeholder there would corrupt backtest data.
3. else if in `suspended` **and on the roster** ⇒ write a suspended placeholder. **Roster
   gating** is essential: `suspend_d` still returns de‑rostered old 北交所 codes; writing
   placeholders for them fabricates orphans.
4. else (null open/close, not suspended) ⇒ skip.

### 7.4 Index‑driven fill (the current backfill — `cache_pipeline.py`)

- **`fill_daily_from_calendar()`** reads `get_calendar_fillable_by_date()`
  (`daily_state IN (missing, wrong_suspended)`) and re‑downloads only those `(day, code)`.
  `ok` / `source_none` are skipped (never re‑burned). Resume granularity = day.
- **`fill_minute_from_calendar(max_codes=None)`** reads `minute_state = 'missing'`, re‑downloads
  per code via `stk_mins`, **auto‑commits per code** (resume granularity = stock — a crash
  keeps every already‑written stock). Per code: ≥241 bars → `ok`; 1–240 → inserted + reported
  `source_short` (deduped by `trade_time` so the count matches the DB read‑back, else a
  permanent missing↔retry loop); 0 → stays `missing`. `max_codes` caps an unattended run; a
  per‑batch heartbeat log proves progress (slow ≠ hung).
- Fill writes **only the data tables, not the calendar.** To refresh the index, re‑reconcile
  the touched days (a "confirm" rebuild).

> The older `download_prices()` / `_download_daily_unified()` path (coarse whole‑day audit via
> `audit_daily_gaps`) is **legacy**, kept only for the model‑training pre‑fill
> (`check_and_fill_gaps`). The nightly + manual backfill use the index‑driven path above.

---

## 8. The nightly maintenance pipeline (`cache_scheduler.py`, 3am Beijing)

One sequential, failure‑isolated pass that converges the whole engine. **Incremental detect,
precise fill** — never re‑scan all history nightly.

```
① load-tushare        run_load_listing                          refresh roster (preserve kimi rows)
② verify (kimi)       listing_verify_scheduler.verify_unverified  resolve unknown codes + backfill code_alias
③ detect (incremental) rebuild_calendar(max_date+1 → yesterday, with_minute=True)  reconcile ONLY new days
④ fill daily          fill_daily_from_calendar                   only missing / wrong_suspended
⑤ fill minute         fill_minute_from_calendar(max_codes=NIGHTLY_MINUTE_MAX_CODES)  capped + heartbeat
⑥ confirm             rebuild touched days: minute-touched WITH minute (bounded by ⑤'s cap),
                      daily-only days WITHOUT minute (cheap)     → re-reconcile, persist source_short
⑦ one Feishu summary  per-step result + final state counts + named problem codes
```

Non‑negotiable invariants:

- **③ is incremental, not full.** The truth table is materialized — history reconciled once
  stays trusted. The nightly reconciles only `max_date+1 → T‑1` (≈ yesterday). A full re‑scan
  is both slow *and* re‑creates GreptimeDB "ghost rows" (un‑flushed deletes / broken tag
  indexes) as orphans every night (the 300114 case, §9). **Full rebuild is a manual endpoint**
  (bootstrap / migration / index repair). If the table is empty, ③ warns and waits for a
  manual bootstrap — it does not silently full‑rebuild on every restart.
- **Failure‑isolated.** A failed step is logged + recorded + the pass continues — *except*
  ④⑤⑥ are skipped if ③ failed (never fill against a stale index).
- **⑤ minute is capped** (`NIGHTLY_MINUTE_MAX_CODES ≈ 2.4 trading days`). Steady state ≈ one
  day (~5000 code‑days, ~15 min). A surprise backlog fills up to the cap, reports
  "N remaining, will continue next run", and **never runs unattended for hours**.
- **⑥ keeps minute reconcile bounded.** Minute reconcile is expensive (one GROUP BY/day), so
  only the days ⑤ actually touched get `with_minute=True`; days ④ touched but ⑤ didn't get a
  cheap daily‑only confirm. Feeding the *uncapped* historical daily‑gap set into a
  `with_minute=True` confirm would blow the step timeout.
- **3am only, no startup run.** watchtower redeploys frequently; a startup run would re‑fire
  on every restart.
- **kimi has no on/off toggle.** Installed + `KIMI_API_KEY` present ⇒ it runs; otherwise the
  summary shows a ⚠️ warning (a missing capability is visible, never silently disabled).
- Manual `POST /api/cache/trigger` runs the *same* pipeline as a **background task** (returns
  immediately; never blocks the HTTP request — a synchronous await once wedged a 40‑min run).
  The task ref is rooted on `app.state` (a bare `create_task` can be GC‑cancelled mid‑run).

---

## 9. Safety invariants & the empty‑source guard

This engine follows CLAUDE.md §5 (Trading Safety > Robustness) at the data layer: **incomplete
data → skip; never substitute an empty/failed fetch as truth.**

- **Empty `fetch_traded` guard (`build_calendar`).** Tushare `daily` occasionally returns
  `code=0` with empty data (a transient hiccup, often after a dense `stk_mins` barrage).
  Reconciling that day naively would mark every truly‑trading code `wrong_traded` (source
  shows no trade) — corrupting good state from one blip. The guard: if `traded` is empty
  **but the DB already holds ≥ 100 real rows** for that day, treat it as a **fetch failure**,
  **skip the day (don't overwrite)**, log ERROR, and surface it as `skipped_days` → a ⚠️
  warning in the ③/⑥ summary (never silent). A genuine non‑trading day (0 traded **and** ~0
  DB rows) sails through.
- **`source_none` self‑heals.** It's skipped by backfill (no pointless retry), but every
  rebuild re‑checks the source; the day the source has data, the state flips `source_none →
  missing` and the next fill picks it up. "Don't busy‑retry when the source is empty; pick it
  up automatically once it isn't."
- **`wrong_traded` is state corruption, not data loss.** It is never auto‑filled and never
  auto‑purged (only `orphan` rows are purge‑eligible). Recovery = rebuild that day once the
  source recovers.
- **`upsert_trading_calendar` DELETE‑then‑INSERTs the whole day.** A plain upsert only
  overwrites codes it re‑emits; a code that *left* the roster would linger forever. The
  day‑range delete makes the day exactly match the reconcile (empty rows included).

---

## 10. The read path (how backtests consume the data)

- **`GreptimeHistoricalAdapter`** (`greptime_historical_adapter.py`) — read‑only projection
  for the ML scanner. `history_quotes(codes, indicators, start, end)` returns a THS‑shaped
  response; **this is the single point that applies `volume ×100`** for daily reads.
- **`GreptimeBacktestStorage` read methods** (`greptime_storage.py`):
  - daily: `get_daily`, `get_all_codes_with_daily(date)`, `get_multi_day_history(start, end)`
    (returns `vol*100`), `get_daily_for_code(code, start, end)` (raw rows).
  - minute: `get_minute_bars_for_day(code, day)` / `get_minute_bars_for_codes_on_day(...)`
    (Tushare‑shaped `{trade_time, open, high, low, close, vol, amount}`, vol already in 股).
  - shape: `DailyBar` NamedTuple `(open, high, low, close, preClose, volume, amount,
    turnoverRatio, is_suspended)`.
- **Consumers:**
  - `ml_strategy_service.run_ml_backtest()` — `get_all_codes_with_daily` (trade date) +
    `get_multi_day_history` (37‑day history) + per‑candidate `get_minute_bars_for_day`
    (semaphore‑bounded, aggregated then discarded by `EarlyWindowAggregator`).
  - `analysis/kline_llm.py` — `get_daily_for_code` for chart analysis.
  - ⚠️ `run_ml_live()` currently fetches its 37‑day history **live from Tushare**, not from
    cache (a temporary decoupling — CLAUDE.md §8.5). It returns to `get_multi_day_history`
    once the cache is proven (one week of clean daily audits).

---

## 11. Hard‑won lessons (top of the list — full log in `data-integrity-pipeline.md` §8)

These caused real, hard‑to‑see bugs. Re‑read before changing the relevant area.

1. **A tag‑equality `SELECT` can miss a row with a broken tag index, but a full‑scan
   (reconcile's `WHERE ts=…`, and `DELETE`) sees it.** "No Data" from `WHERE stock_code='X'`
   ≠ the row doesn't exist. Delete such ghosts with `DELETE … WHERE ts=… AND stock_code='X'`
   (DELETE full‑scans). The incremental‑rebuild invariant (§8) is the root defense. (300114.)
2. **北交所 2025‑10‑09 mass code migration (43x/83x/87x → 920x).** Tushare re‑homed *all*
   history (incl. 2023) under the new 920 codes; old codes return nothing. Don't backfill old
   codes — reload the roster (`load-tushare`) so they vanish, and `purge-orphan-rows` for the
   stale daily rows. (`code_alias` handles single‑stock renames where the vendor keeps history
   under the old code, e.g. 300114→302132.)
3. **Don't trust `suspend_d` over real volume.** They contradict; `volume > 0` wins.
4. **Empty Tushare `daily` → mass `wrong_traded`** — guarded (§9).
5. **`ts` must be the business date / fixed, never write‑time** (dual‑row flicker).
6. **GreptimeDB batch INSERT > ~200 rows silently drops** — batch under it.
7. **asyncpg + GreptimeDB**: the three connection overrides + watchdog + epoch‑ms params
   (§5, CLAUDE.md §7) — any one missing → silent hang.
8. **watchtower auto‑deploys on CI‑green and restarts the container**, interrupting a running
   long fill/rebuild. Push stable code first, *then* run long tasks; don't push during a fill.

---

## 12. Endpoint reference (`src/web/audit_routes.py`, prefix `/api/audit`)

| Endpoint | Purpose |
|----------|---------|
| `POST /listing-info/load-tushare` | Reload the authoritative roster from `stock_basic`. |
| `POST /calendar/rebuild?start=&end=&with_minute=1` | Full/range rebuild of the truth table (manual bootstrap / repair). |
| `GET  /calendar/status` | `by_daily_state` + `by_minute_state` counts, date range, rebuild flag. |
| `GET  /calendar/minute-coverage?date=` | `backtest_minute` row count for one day — watch a minute fill climb (progress signal; truth‑table `by_minute_state` only flips after the confirm). |
| `GET  /calendar/problems?state=&limit=` | The concrete `(date, code)` rows for a state. |
| `POST /backfill-daily` | Index‑driven daily fill (`missing` + `wrong_suspended`). `?mode=full` = legacy coarse re‑download. |
| `POST /backfill-minute` | Index‑driven minute fill (`minute_state='missing'`), **uncapped** (deliberate historical bootstrap), background. |
| `POST /api/cache/trigger` (in `routes.py`) | Run the whole nightly pipeline now, **background**. |
| `POST /calendar/purge-orphan-rows[?execute=1]` | Delete `orphan` daily rows (dry‑run without `execute`). |
| `POST /calendar/purge-codes-data {codes:[...]}` | Delete all daily rows for explicit dead codes. |
| `GET/POST /calendar/code-alias` | View / set old→new code aliases. |
| `GET  /listing-info/findings` / `kimi-raw?code=` | kimi's per‑code verdicts + raw trace (observability — don't guess, read the trace). |
| `POST /diagnose-gaps` | Per‑day diagnosis report → Feishu. |

---

## 13. The convergence loop (summary)

```
build index (reconcile) → fill what's missing → re-reconcile touched days (confirm)
        rebuild_calendar       fill_*_from_calendar        rebuild_calendar
```

Steady state: nightly ③→⑥ does this incrementally for the new day in minutes; the truth
table stays "0 problems except known `source_none`", daily reads are correct (×100), and
minute is complete (241 bars/day) for everything that traded.
