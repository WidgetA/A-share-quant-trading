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
- One `rt_min_daily` call serves one stock. The approved client fan-out is one service-level acquisition with at most 40 concurrent per-stock requests. Same-provider-minute scheduled, manual, cold-start, and retry contenders must join the same in-process singleflight rather than starting another acquisition; bounded per-stock transport retries remain inside the client.
- Never overlap two 40-worker V20 acquisitions or combine the V16 and V20 paths into an intentional 80-worker burst. The measured stable case is about 3,000 symbols at concurrency 40; about 6,000 requests at concurrency 80 produced terminal failures under the provider's per-minute limit.

## V20 morning-selection parity

- The scheduled 09:39 V20 run and a manual trigger must call the same canonical strategy-calculation entry point. Time may change actionability or message wrapping after the calculation, but must not select a different data or strategy algorithm.

## V16/V20 isolation

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
