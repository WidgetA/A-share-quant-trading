# Repository agent rules

## Tushare minute-data API time boundary

- Treat the Tushare realtime minute API family (`rt_*`) as current-trading-day APIs only.
- `rt_min` accepts multiple comma-separated stock codes and returns the latest current-day minute bar for each requested stock. Use it for a batch snapshot, not for reconstructing an earlier multi-minute window from one call.
- `rt_min_daily` accepts one stock code per request and returns that stock's current-day minute history available so far. Use it when the current trading day needs the complete 09:31-09:39 window; it is the canonical source for the live 09:39 morning selection.
- `stk_mins` is the historical-minute API. For the V20 early window it may batch multiple stock codes, but it may be used only for a completed historical trading day (T-1 or earlier), never for the current trading day.
- Select the provider endpoint from the target trading date before fetching data. Scheduled runs, manual triggers, retries, cold starts, and recovery/backfill paths must all obey the same date boundary.
- Do not route a current-day request to `stk_mins` as a fallback. Missing current-day realtime data must remain a visible current-day input failure or be retried through the appropriate realtime API.
- Tests for any minute-data routing change must assert both sides of the boundary: today uses realtime APIs and never `stk_mins`; T-1 or earlier uses historical APIs and never `rt_*`.

## V20 morning-selection parity

- The scheduled 09:39 V20 run and a manual trigger must call the same canonical strategy-calculation entry point. Time may change actionability or message wrapping after the calculation, but must not select a different data or strategy algorithm.
