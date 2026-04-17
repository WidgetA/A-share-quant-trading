# Date/Time & asyncpg Timezone Handling Guide

> Core rules are in [CLAUDE.md](../CLAUDE.md) Sections 6-7.
> This file has the detailed code examples and explanations.

## Date and Time Handling (Beijing Time)

**Core Principle: All date/time operations MUST use Beijing Time (UTC+8), and MUST be calculated via code execution.**

AI models have unreliable internal date awareness. Always run Python code to determine current date/time.

### How to Get Current Beijing Time

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

beijing_tz = ZoneInfo("Asia/Shanghai")
now = datetime.now(beijing_tz)
today = now.date()
```

### Relative Date Calculation Examples

```python
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

beijing_tz = ZoneInfo("Asia/Shanghai")
today = datetime.now(beijing_tz).date()

# Yesterday
yesterday = today - timedelta(days=1)

# This Tuesday (current week)
days_since_monday = today.weekday()
this_tuesday = today - timedelta(days=days_since_monday) + timedelta(days=1)

# Last Friday
days_since_last_friday = (today.weekday() - 4) % 7
if days_since_last_friday == 0:
    days_since_last_friday = 7
last_friday = today - timedelta(days=days_since_last_friday)

# Start of this month
start_of_month = today.replace(day=1)
```

### Required Workflow

When user mentions any date/time reference:

1. **Run code first** to determine the actual date
2. **Confirm with user** if the calculated date matches their intent
3. **Use the calculated date** for all subsequent operations

| User Says | Action Required |
|-----------|-----------------|
| "今天" / "today" | Run `datetime.now(beijing_tz).date()` |
| "昨天" / "yesterday" | Run `today - timedelta(days=1)` |
| "本周二" / "this Tuesday" | Calculate from current weekday |
| "上周" / "last week" | Calculate week boundaries |
| "这个月" / "this month" | Get month start/end dates |
| Any specific date | Verify it's a valid trading day |

---

## asyncpg + GreptimeDB Timezone Handling (Critical)

**Core Principle: All GreptimeDB queries use epoch milliseconds, never datetime objects — this sidesteps asyncpg's timezone pitfalls entirely.**

### Why Epoch Ms (Background)

asyncpg has **asymmetric timezone behavior** when using datetime objects:
- **Sending** (query parameters): Naive datetimes interpreted using **system timezone** (`TZ` env var)
- **Receiving** (query results): Returns **UTC-aware** datetimes for `timestamptz` columns

This can silently shift timestamps by hours. Our codebase avoids this by **never passing datetime objects as query parameters** — all timestamps are converted to epoch ms integers first.

### ts Column Convention

All tables (`backtest_daily`, `backtest_minute`, `stock_list`) store timestamps as epoch ms with the string interpreted as naive UTC:

```python
import calendar
from datetime import date, datetime

# date → epoch ms (midnight UTC)
def date_to_epoch_ms(d: date) -> int:
    return calendar.timegm(d.timetuple()) * 1000

# "2026-04-09 09:31:00" → epoch ms (treated as naive UTC)
def minute_str_to_epoch_ms(s: str) -> int:
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    return calendar.timegm(dt.timetuple()) * 1000
```

Day boundary math is trivial: `day_end_ms = day_start_ms + 86_400_000`.

### Actual Query Patterns (from greptime_storage.py)

```python
# CORRECT: All queries use epoch ms integers, no datetime objects
start_ms = date_to_epoch_ms(parse_date_str(start_date))
end_ms = date_to_epoch_ms(parse_date_str(end_date))
rows = await db.fetch(
    f"SELECT ts, ... FROM backtest_daily "
    f"WHERE stock_code = '{code}' AND ts >= {start_ms} AND ts <= {end_ms}"
)

# CORRECT: Minute bar day range
day_start = date_to_epoch_ms(day)
day_end = day_start + 86_400_000
rows = await db.fetch(
    f"SELECT ts, ... FROM backtest_minute "
    f"WHERE stock_code = '{code}' AND ts >= {day_start} AND ts < {day_end}"
)
```

### Result Conversion Patterns

```python
from datetime import datetime, timezone

# epoch ms → date
def epoch_ms_to_date(ms: int | float) -> date:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()

# epoch ms → minute string
def epoch_ms_to_minute_str(ms: int | float) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# asyncpg datetime result → epoch ms (when asyncpg returns datetime objects)
def ts_to_epoch_ms(val: Any) -> int:
    if isinstance(val, datetime):
        return int(val.replace(tzinfo=timezone.utc).timestamp() * 1000)
```

### Prohibited Patterns

```python
# FORBIDDEN: Passing datetime objects as asyncpg query parameters
await conn.fetch("SELECT * FROM t WHERE ts >= $1", naive_datetime)

# FORBIDDEN: Manual UTC offset arithmetic for query params
start_utc = start_bj - timedelta(hours=8)  # Don't do this

# FORBIDDEN: + timedelta(hours=8) for display conversion
# (not needed — our epoch ms convention already treats strings as naive UTC)
```

### Past Incident

In Feb 2026, incorrectly changed UTC→Beijing conversion multiple times. Root cause took many iterations: the QUERY was wrong (searching yesterday's data), not the display. Always run diagnostic SQL inside the container to check actual DB values before assuming.
