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

## Past Incidents

- **momentum_quality_filter.py**: Used fail-open pattern — API failure → no filtering → bad trades went through undetected. L2/L3 filters showed +0.00% because they silently did nothing.
- **app.py startup ordering** (2026-03-18): `run_audit()` was unprotected before `_start_monitoring()` — audit crash silently skipped monitoring startup, no heartbeat alerts sent all day.
