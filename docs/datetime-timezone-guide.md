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

## asyncpg Timezone Handling (Critical)

**Core Principle: ALWAYS use timezone-aware datetimes when passing parameters to asyncpg queries.**

### Root Cause

asyncpg has **asymmetric timezone behavior** that causes silent data corruption:

| Direction | Behavior |
|-----------|----------|
| **Sending** (query parameters) | Naive datetimes are interpreted using the **system timezone** (`TZ` env var) |
| **Receiving** (query results) | Always returns **UTC-aware** datetimes (`tzinfo=UTC`) for `timestamptz` columns |

When the Docker container has `TZ=Asia/Shanghai`, this asymmetry causes a **16-hour offset** for naive datetimes:

```
Naive 01:30 → asyncpg treats as 01:30 CST → converts to 17:30 UTC (previous day!)
Expected: 01:30 UTC
Actual:   17:30 UTC (WRONG by 16 hours)
```

### Prohibited Patterns

```python
# FORBIDDEN: Subtracting hours from naive datetime for "manual UTC conversion"
start_bj = datetime(2026, 2, 8, 9, 30)  # 9:30 Beijing
start_utc = start_bj - timedelta(hours=8)  # 01:30 naive — WRONG!
# asyncpg will interpret 01:30 as 01:30 CST → 17:30 UTC

# FORBIDDEN: Any naive datetime as asyncpg query parameter
await conn.fetch("SELECT * FROM t WHERE ts >= $1", naive_datetime)
```

### Correct Patterns

```python
from zoneinfo import ZoneInfo

beijing_tz = ZoneInfo("Asia/Shanghai")

# CORRECT: Use timezone-aware datetime for query parameters
start_bj = datetime(2026, 2, 8, 9, 30)
start_aware = start_bj.replace(tzinfo=beijing_tz)  # 09:30+08:00
# asyncpg correctly converts to 01:30 UTC

await conn.fetch("SELECT * FROM t WHERE ts >= $1", start_aware)

# CORRECT: Display conversion (UTC result → Beijing time for display)
utc_time = row["publish_time"]  # e.g., 2026-02-07 22:58:00+00:00
beijing_display = (utc_time + timedelta(hours=8)).replace(tzinfo=None)
# Result: 2026-02-08 06:58:00 (clean Beijing time string)
```

### Past Incident

In Feb 2026, incorrectly changed UTC→Beijing conversion multiple times. Root cause took many iterations: the QUERY was wrong (searching yesterday's data), not the display. The display (+8h) was correct all along. Always run diagnostic SQL inside the container to check actual DB values before assuming.
