# Trading Safety — Forbidden & Correct Patterns

> Core principle is in [CLAUDE.md](../CLAUDE.md) Section 5.
> This file has the detailed code examples for reference.

## Prohibited Degradation Patterns

These patterns are **FORBIDDEN** in trading-related code:

```python
# FORBIDDEN: Using cache when real-time data fails
def get_price(stock_code):
    try:
        return fetch_realtime_price(stock_code)
    except NetworkError:
        return self.cache.get(stock_code)  # DANGEROUS!

# FORBIDDEN: Assuming order status on timeout
def check_order_status(order_id):
    try:
        return broker.get_status(order_id)
    except Timeout:
        return "FILLED"  # DANGEROUS ASSUMPTION!

# FORBIDDEN: Auto-retry that may cause duplicate orders
def place_order(order):
    for _ in range(3):  # DANGEROUS RETRY!
        try:
            return broker.submit(order)
        except NetworkError:
            continue
```

## Correct Patterns

```python
# CORRECT: Fail explicitly when data unavailable
def get_price(stock_code):
    try:
        return fetch_realtime_price(stock_code)
    except NetworkError as e:
        logger.error(f"Failed to fetch price for {stock_code}: {e}")
        raise TradingHaltError("Cannot proceed without real-time data")

# CORRECT: Mark status as unknown and halt
def check_order_status(order_id):
    try:
        return broker.get_status(order_id)
    except Timeout:
        logger.critical(f"Order {order_id} status unknown - HALTING")
        raise OrderStatusUnknownError(order_id)

# CORRECT: No retry for order submission
def place_order(order):
    try:
        return broker.submit(order)
    except NetworkError as e:
        logger.error(f"Order submission failed: {e}")
        raise OrderSubmissionError("Manual intervention required")
```

## Audit

Run before ANY code change in `src/strategy/`, `src/trading/`, `src/data/`:

```bash
uv run python scripts/audit_trading_safety.py
```

## Startup Ordering Rule

Safety-critical background tasks (monitoring, heartbeat, alerting) **MUST** start before non-critical tasks (audit, cache loading) in `app.py` startup. Non-critical tasks must be wrapped in `try/except` so they cannot block safety tasks.

```python
# CORRECT: monitoring first, audit wrapped
async def startup():
    iquant_rtr._start_monitoring()  # safety-critical, start first
    try:
        run_audit()                 # non-critical, wrapped
    except Exception:
        logger.error("audit failed")

# FORBIDDEN: unprotected non-critical task before monitoring
async def startup():
    run_audit()                     # if this crashes...
    iquant_rtr._start_monitoring()  # ...this never runs!
```

## QMT API Thread Safety

QMT's `get_trade_detail_data` and `passorder` **only work on the main thread**. Calling from `threading.Thread` silently returns empty results (no exception).

```python
# FORBIDDEN: QMT API from background thread
def _heartbeat_loop():
    while True:
        positions = get_trade_detail_data(accID, 'stock', 'position')  # Always []!
        cash = get_trade_detail_data(accID, 'stock', 'account')        # Always []!
        send_to_server(positions, cash)
        time.sleep(30)

# CORRECT: Main thread queries broker → shared dict → background thread sends HTTP
_state = {"positions": [], "cash": 0}

def handlebar(ContextInfo):          # main thread
    _state["positions"] = _get_all_positions(accID)
    _state["cash"] = _get_available_cash(accID)

def _state_sync_loop():              # background thread
    while True:
        _api_call("/heartbeat", "POST", {    # pure HTTP, no QMT API
            "positions": _state["positions"],
            "available_cash": _state["cash"],
        })
        time.sleep(30)
```

After trade execution, **immediately** refresh and sync:
```python
if success:
    _ack_signal(sig_id)
    _refresh_broker_state()   # re-query broker (main thread)
    _sync_state_now()         # push to server immediately
```

## Silent Skip — Data Pipeline

Never silently skip writing a record because supplementary data is missing. The record itself (even with NULL fields) carries semantic meaning.

```python
# FORBIDDEN: skip suspended stock because prev_close unavailable
pre_close = prev_close_map.get(susp_code, 0.0)
if pre_close <= 0:
    continue  # backtest engine now has NO IDEA this stock is suspended

# CORRECT: always write the row — price fields can be NULL
pre_close = prev_close_map.get(susp_code, 0.0)
await storage.insert_daily_record(code, day, _suspended_record(pre_close))
# _suspended_record uses None for prices when pre_close <= 0
```

## Past Incidents

- **momentum_quality_filter.py**: Used fail-open pattern — API failure → no filtering → bad trades went through undetected. L2/L3 filters showed +0.00% because they silently did nothing.
- **app.py startup ordering** (2026-03-18): `run_audit()` was unprotected before `_start_monitoring()` — audit crash silently skipped monitoring startup, no heartbeat alerts sent all day.
- **iQuant heartbeat thread** (2026-04-02): Moved `get_trade_detail_data` to background thread → silently returned empty → dashboard showed 0 positions after deploy. Fix: QMT API calls stay on main thread, background thread only does HTTP.
- **cache_pipeline suspended skip** (2026-04-17): `if pre_close <= 0: continue` silently skipped ~17 delisted/long-suspended stocks. Tushare `suspend_d` correctly reported them, but no row was written → backtest engine couldn't distinguish "not tradable" from "doesn't exist". Fix: always write `is_suspended=true` row, price fields NULL when prev_close unavailable.
