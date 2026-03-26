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

## Known Issues — V15 Scanner (`v15_scanner.py`)

以下问题**同时存在于线上推送和回测**，尚未修复：

### 1. `_fetch_historical()` 历史数据缺失时默认填 0（CRITICAL）
- **位置**: `v15_scanner.py` `_fetch_historical()` ~L666-731
- **现象**: 某只股票的历史行情拿不到时，trend_10d / avg_daily_return / consecutive_up_days 等全填 0.0，不报错
- **后果**: V3 评分基于假数据，可能推错股票
- **应修复为**: 数据缺失时从候选池移除该股票并记录日志

### 2. `_fetch_constituent_prices()` 静默丢弃成分股（CRITICAL）
- **位置**: `v15_scanner.py` `_fetch_constituent_prices()` ~L735-789
- **现象**: 价格获取失败的成分股直接 `continue`，没有日志、没有计数
- **后果**: L4 扩展的候选池可能被悄悄缩小，不知道丢了多少股票

### 3. `preClose` 不可用导致 L4 扩展完全失效（CRITICAL）
- **位置**: `v15_scanner.py:768` + `tushare_realtime.py:459`
- **现象**: TushareRealtimeClient 的 rt_min 接口没有昨收价（preClose），返回 None。`_fetch_constituent_prices()` 中 `if not prev_vals[0]` 跳过该股票
- **后果**: 线上 L4 成分股扩展实际上**不会新增任何股票**，所有通过 `real_time_quotation` 获取的成分股全被跳过。L4 扩展形同虚设
- **应修复为**: 从其他数据源补充昨收价（如 tsanghi 缓存或 GreptimeDB）

### 4. trend_10d 不足 11 天时默认 0.0（MEDIUM）
- **位置**: `v15_scanner.py` ~L715-720
- **现象**: 上市不足 11 个交易日的股票，trend_10d 填 0.0（中性值）
- **后果**: 新股在 V3 评分中获得不公平的中性趋势分

### 5. L3 板块静默跳过（MEDIUM）
- **位置**: `v15_scanner.py` ~L370-372
- **现象**: 板块内所有股票都不在 gainers 中时，板块被静默跳过，无日志
- **后果**: 无法区分"板块确实没有涨幅股"和"数据缺失导致板块被跳过"

## Known Issues — 回测独有（已修复）

### [已修复] `_build_snapshots_from_cache()` 覆盖率计数 bug
- **提交**: `0283245`
- **原问题**: 9:40 价格为 0 的股票先被计入 minute_hits，再被跳过，导致覆盖率检查虚高

### [已修复] 分钟线聚合 low=0 静默接受
- **提交**: `0283245`
- **原问题**: baostock 分钟线 low 为 0 时静默聚合，导致 PriceSnapshot.low_price 无效

## Past Incidents

- **momentum_quality_filter.py**: Used fail-open pattern — API failure → no filtering → bad trades went through undetected. L2/L3 filters showed +0.00% because they silently did nothing.
- **V15 回测数据管道不一致 (2026-03-26)**: 回测使用 MomentumSectorScanner 的 `_build_snapshots_from_cache()`（含 open_gain > -0.5% 预过滤），且 `TsanghiHistoricalAdapter.real_time_quotation()` 返回空数据，导致 L4 扩展失效。回测推 002014 永新股份，线上推 600332 白云山。根因：回测和线上用了不同的数据管道。
