# Repository agent rules

## Tushare minute-data API time boundary

- Treat the Tushare realtime minute API family (`rt_*`) as current-trading-day APIs only.
- `rt_min` accepts multiple comma-separated stock codes and returns the latest current-day minute bar for each requested stock. Use it for a batch snapshot, not for reconstructing an earlier multi-minute window from one call.
- `rt_min_daily` accepts one stock code per request and returns that stock's current-day minute history available so far. Use it when the current trading day needs the complete 09:31-09:39 window; it is the canonical source for the live 09:39 morning selection.
- `stk_mins` is the historical-minute API. For the V20 early window it may batch multiple stock codes, but it may be used only for a completed historical trading day (T-1 or earlier), never for the current trading day.
- Select the provider endpoint from the target trading date before fetching data. Scheduled runs, manual triggers, retries, cold starts, and recovery/backfill paths must all obey the same date boundary.
- Do not route a current-day request to `stk_mins` as a fallback. Missing current-day realtime data must remain a visible current-day input failure or be retried through the appropriate realtime API.
- Tests for any minute-data routing change must assert both sides of the boundary: today uses realtime APIs and never `stk_mins`; T-1 or earlier uses historical APIs and never `rt_*`.
- Keep the two full-market current-day pulls staggered: V16 starts at 09:38 and V20 may start its complete `rt_min_daily` acquisition only at or after 09:39. Do not move V20's full pull earlier.
- A V16 manual scan must not start during `09:39 <= Shanghai wall time < 09:45`; before 09:39 its realtime fan-out must settle by the absolute 09:39 cutoff, while a request after that protected window (including after hours) remains a fresh V16-only scan.
- User correction on 2026-11-02: do not use 40 concurrent minute requests; the shared minute client default is 5. The later 2026-11-02 selection instruction sets only V22-slim early-market acquisition to 10 concurrent requests; other minute methods and V16/V20 retain 5. One `rt_min_daily` call serves one stock, and a free slot is filled as soon as a request completes. Same-provider-minute scheduled, manual, cold-start, and retry contenders must join the same in-process singleflight rather than starting another acquisition; bounded per-stock transport retries remain inside the client.
- Never overlap two full-market V20 acquisitions or combine the V16 and V20 paths to increase fan-out. Earlier measurements at concurrency 40/80 are historical observations, not authorization to exceed the applicable default of 5 or the V22-slim selection ceiling of 10.

## Selection task contract — user correction, 2026-09-12

- Source of truth: [execution-contract.md](docs/strategies/v22-slim/execution-contract.md), based on the user's explicit instructions. Older implementation notes and tests cannot override it.
- The timer and manual button invoke the same COMPLETE selection task: input acquisition, calculation, persistence, message generation, and delivery. Sharing only the calculation helper is insufficient.
- Current V22-slim instruction, 2026-11-09: V22-slim scheduled and manual selection must share the same exchange-calendar check and skip non-trading dates, including exchange holidays, before requesting market data. Do not use a weekday-only check or change the requested date to the latest trading date. For V22-slim, this supersedes only the 2026-09-12 instruction against weekday/holiday admission checks; it does not permit historical minute data or old tickets to fill a current-day response. On a trading date, invoke the current realtime source; an empty realtime result ends selection without producing or pushing tickets. Historical daily features retain their own existing purpose. This V22-slim correction does not change the existing V16/V20 runtime-isolation contract.
- A new manual request runs the task again, including when the day's scheduled run already exists. It must not select a read-only/check-only path, replay a stored message instead of calculating, or use a manual-only message template.
- Only the trigger source differs. Run IDs, actual execution timestamps and retry deduplication may differ; selection, gates, persistence contract, and message body may not depend on trigger source.
- Preserve date-based market API routing and once-per-day state advancement as shared task rules, not manual-only bypasses. A repeated HTTP request with the SAME run ID is a retry; a NEW button click gets a new run ID and recomputes.
- Before repairing a recurrence, add a behavioral test which FAILS against the reported broken behavior. Do not rewrite that test to approve the existing behavior. Correct conflicting documentation in the same change.
- Do not report the task as fixed/onlined merely because CI, health checks, a calculation helper, or an operator notification succeeded. Require the shared task's persisted result and confirmed delivery on the deployed commit, plus scheduler/button parity tests.
- Public trading messages must describe stocks, recommendation dates, buy/sell quantities or proportions in ordinary language. Do not expose "模型腿", "整腿", "监控腿", D0/rank labels or opaque model IDs as user instructions.

## V22 notification-only holdings — user instruction, 2026-09-13

- Automatically record the first allowed Top3 once per selection day, in the
  same transaction for both timer and button. BLOCK/NO_SIGNAL/empty data cannot
  create holdings. A new rerun cannot duplicate records or overwrite corrections.
- Initial cost is the strategy's raw 09:41 open when available; quantity is
  unknown until corrected. Never invent an actual fill or quantity.
- API corrections are audited and retry-idempotent. A notification is not a
  sale; keep the record until the user marks it closed or not bought.
- Apply frozen V22 stops, recovery-failure and STRONG extension as alerts only.
  Keep original V20 positions under their original rules. See
  [exit-alerts.md](docs/strategies/v22-slim/exit-alerts.md).

## V16/V20 runtime isolation

- A V20 change must never require a production V16 source, model, configuration,
  route, or test change. Production V16 modules must not import V20 modules.
- V20 owns its scanner, scorer, and model artifacts under V20-specific paths.
  V20 must not import `src.strategy.strategies.v16_scanner`,
  `src.strategy.lgbrank_scorer`, or load models from the V16 `models/` root.
- V16 and V20 may share only generic, pure, stateless utilities that are not
  owned by either strategy. V20-specific behavior must be implemented under a
  V20 path, never by changing a shared utility consumed by V16.
- V16 and V20 must never share a `V15ScanState` instance, market-data client, historical adapter, cache object, database object or connection pool, calendar provider, scheduler, lifecycle owner, initialization task, or cleanup task.
- Embedded and forward-shadow modes may reuse configuration values or credentials, but every mutable runtime resource must be constructed, owned, and closed independently.
- Every V20 runtime-resource change must include a fault-isolation test proving that V20 initialization failure or shutdown cannot mutate or stop V16 state/resources, and that V16 shutdown cannot mutate or stop V20 state/resources.
