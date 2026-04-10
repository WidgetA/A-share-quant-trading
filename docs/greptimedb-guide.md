# GreptimeDB Technical Guide

> Based on official docs (https://docs.greptime.com), version 1.0.0-rc.2.
> This file is the SOLE reference for implementing GreptimeDB integration. Do not guess — if something isn't here, read the official docs.

## 1. HTTP API (Port 4000)

### Endpoint
```
POST /v1/sql?db=<database>
```

### Request
- **Content-Type**: `application/x-www-form-urlencoded`
- **Body**: `sql=<SQL statement>`
- **Auth** (optional): `Authorization: Basic <base64(user:password)>`
- **Timeout header**: `X-Greptime-Timeout: 120s`
- **Timezone header**: `X-Greptime-Timezone: +8:00`

### Response — SELECT
```json
{
  "output": [{
    "records": {
      "schema": {
        "column_schemas": [
          {"name": "column_name", "data_type": "DataType"}
        ]
      },
      "rows": [["value1", "value2"]],
      "total_rows": 1
    }
  }],
  "execution_time_ms": 7
}
```

### Response — INSERT
```json
{
  "output": [{"affectedrows": 3}],
  "execution_time_ms": 11
}
```

### Response — Error
```json
{
  "error": "error message"
}
```

### Output Formats
Add `&format=<format>` to URL. Options: `greptimedb_v1` (default), `csv`, `table`, `influxdb_v1`, `arrow`.

---

## 2. Data Model

Every table has exactly three semantic column types:

| Type | Declaration | Purpose |
|------|-------------|---------|
| **Tag** | `PRIMARY KEY(col1, col2)` | Identifies the time-series. Indexed. |
| **Timestamp** | `TIME INDEX(col)` | Required. One per table. When data was generated. |
| **Field** | Everything else | Data values (metrics, measurements). |

**Deduplication**: Rows with same (PRIMARY KEY + TIME INDEX) are merged. Default `merge_mode='last_row'` — last write wins (natural upsert, no INSERT OR REPLACE needed).

**No transactions**: GreptimeDB has NO ACID transactions. No BEGIN/COMMIT/ROLLBACK. Each INSERT is independently persisted.

---

## 3. Data Types

### Numeric
| Type | Size | Alias |
|------|------|-------|
| `Float64` | 8B | `Double`, `Float8` |
| `Float32` | 4B | `Float`, `Float4` |
| `Int64` | 8B | `BigInt` |
| `Int32` | 4B | `Int`, `Int4` |
| `String` | var | `Text`, `Varchar` |
| `Boolean` | 1B | — |

### Timestamps
| Type | Precision | Alias |
|------|-----------|-------|
| `TimestampMillisecond` | ms | `Timestamp`, `Timestamp(3)` |
| `TimestampSecond` | s | `Timestamp(0)` |
| `TimestampMicroSecond` | us | `Timestamp(6)` |
| `TimestampNanosecond` | ns | `Timestamp(9)` |

**IMPORTANT**: `TIMESTAMP` without precision = `TimestampMillisecond`. Timestamp values in INSERT are **integer milliseconds since epoch**.

---

## 4. SQL Reference

### CREATE TABLE
```sql
CREATE TABLE IF NOT EXISTS my_table (
    tag_col STRING,
    ts TIMESTAMP TIME INDEX,
    field1 FLOAT64,
    field2 FLOAT64,
    PRIMARY KEY (tag_col)
);
```

- `TIME INDEX` is mandatory (exactly one timestamp column)
- `PRIMARY KEY` cannot include the TIME INDEX column (it's implicitly added)
- Field columns are everything not in PRIMARY KEY or TIME INDEX

### INSERT
```sql
-- Single row
INSERT INTO my_table (tag_col, ts, field1, field2)
VALUES ('code1', 1667446797462, 10.5, 20.3);

-- Multiple rows
INSERT INTO my_table (tag_col, ts, field1, field2)
VALUES
  ('code1', 1667446797460, 10.5, 20.3),
  ('code2', 1667446797461, 11.8, 30.1);
```

Timestamp values are **integer milliseconds** (for TIMESTAMP / TimestampMillisecond columns).

### SELECT
```sql
-- Basic
SELECT * FROM my_table WHERE tag_col = 'code1' AND ts = 1667446797462;

-- Aggregates (all supported)
SELECT tag_col, COUNT(*), AVG(field1), MIN(field1), MAX(field1), SUM(field1)
FROM my_table
GROUP BY tag_col;

-- ORDER BY, LIMIT, OFFSET
SELECT * FROM my_table ORDER BY ts DESC LIMIT 10 OFFSET 5;

-- WHERE operators: =, !=, >, <, >=, <=, AND, OR, NOT, LIKE, IN, BETWEEN
SELECT * FROM my_table WHERE tag_col IN ('code1', 'code2') AND field1 > 10.0;

-- HAVING
SELECT tag_col, COUNT(*) as cnt FROM my_table GROUP BY tag_col HAVING cnt > 5;

-- JOIN
SELECT a.*, b.field1 FROM table1 a JOIN table2 b ON a.tag_col = b.tag_col;
```

### Supported SQL Features
- `GROUP BY` — fully supported
- `ORDER BY` — fully supported (ASC/DESC)
- `DISTINCT` — supported (per docs, standard SQL)
- `COUNT`, `SUM`, `AVG`, `MAX`, `MIN` — all supported
- `LIMIT` / `OFFSET` — supported
- `HAVING` — supported
- `JOIN` — supported
- `INSERT INTO ... SELECT` — supported

### NOT Supported
- `BEGIN` / `COMMIT` / `ROLLBACK` (no transactions)
- `DELETE` — supported but not optimized for high-frequency use

---

## 5. Docker Deployment

### Official Docker Command
```bash
docker run -p 4000-4003:4000-4003 \
  -v "$(pwd)/greptimedb_data:/greptimedb_data" \
  --name greptime \
  greptime/greptimedb:v1.0.0-rc.2 standalone start \
  --http-addr 0.0.0.0:4000 \
  --rpc-bind-addr 0.0.0.0:4001 \
  --mysql-addr 0.0.0.0:4002 \
  --postgres-addr 0.0.0.0:4003
```

### Ports
| Port | Protocol |
|------|----------|
| 4000 | **HTTP API** (primary for this project) |
| 4001 | gRPC |
| 4002 | MySQL |
| 4003 | PostgreSQL wire protocol |

### Data Volume
- Container path: `/greptimedb_data`
- Must be mounted for data persistence

### Docker Compose (for this project)
```yaml
greptimedb:
  image: greptime/greptimedb:latest
  restart: always
  expose:
    - "4000"
    - "4003"
  volumes:
    - greptime-data:/greptimedb_data
    - ./config/greptimedb.toml:/etc/greptimedb/config.toml:ro
  command: >
    standalone start
    --config-file /etc/greptimedb/config.toml
    --http-addr 0.0.0.0:4000
    --rpc-bind-addr 0.0.0.0:4001
    --mysql-addr 0.0.0.0:4002
    --postgres-addr 0.0.0.0:4003
```

**Rules**:
- Do NOT add `deploy.resources.limits.memory` — let GreptimeDB manage its own memory
- Use `expose` to declare ports accessible within Docker network

---

## 6. Python Integration

### No Official Python SDK
There is **no** official Python gRPC ingester SDK. The `greptimedb-ingester-py` repo is an empty placeholder (1 commit, no code).

### Recommended Python Approach
Use **HTTP API** (port 4000) with `httpx`:
```python
import httpx

async with httpx.AsyncClient() as client:
    resp = await client.post(
        "http://greptimedb:4000/v1/sql?db=public",
        data={"sql": "SELECT * FROM my_table LIMIT 10"}
    )
    body = resp.json()
    rows = body["output"][0]["records"]["rows"]
```

### PostgreSQL Wire Protocol (port 4003) — Known Limitations

GreptimeDB's PG wire compatibility is incomplete. Three issues hit asyncpg's pool every few thousand queries and **silently wedge** the whole client:

| Issue | What asyncpg does | What GreptimeDB does | Required override |
|-------|------------------|---------------------|-------------------|
| `RESET ALL` / `DEALLOCATE ALL` | Calls `Connection.reset()` on every pool release | Rejects both | Override `reset()` → no-op |
| `PREPARE` / `DEALLOCATE` | Caches up to 1024 prepared statements | Not supported | `statement_cache_size=0` |
| `Terminate` handshake | `Connection.close()` sends Terminate and **awaits** the server socket close | Never closes the socket → hangs forever | Override `close()` → `terminate()` (TCP reset) |

The `close()` issue is the killer: asyncpg's pool recycles a connection after `max_queries=50000`. For a stock_list write doing ~5000 INSERTs per date, the pool trips that threshold at ~13 dates, then `pool.release()` calls `close()` and silently hangs forever — no warning, no timeout.

**Other rules:**
- Not supported: `UNLISTEN`, `BEGIN` / `COMMIT` / `ROLLBACK`, transactions
- A single `asyncpg.connect()` does not support concurrent ops — must use `create_pool(min_size=0, max_size=3)`
- Reserved words: `open`, `close`, `volume` — must double-quote all column names, or use snake_case (`open_price`, `close_price`, `vol`)
- Wrap `pool.acquire()`, query execution, and `pool.release()` in **separate** `asyncio.wait_for()` blocks so timeouts are classifiable
- Run a slow-query watchdog task — asyncpg's C extension can swallow `CancelledError` on `socket.recv`, so a sibling `await asyncio.sleep(30)` warning loop is the only reliable way to surface a stuck SQL

See `src/data/clients/greptime_storage.py:_GreptimeConnection` and `GreptimeClient._run` for the canonical implementation.

### Available PyPI Packages
- `greptimedb-sqlalchemy` (v0.1.7) — SQLAlchemy connector, uses psycopg2 underneath (pgwire)
- `greptimedb-mcp-server` (v0.4.7) — MCP server for AI assistants, read-only

---

## 7. Key Design Decisions for This Project

1. **Use HTTP API** (port 4000) — most stable, best documented, no driver quirks
2. **Use `httpx.AsyncClient`** — async, matches existing FastAPI architecture
3. **Timestamps as milliseconds** — `TIMESTAMP` = `TimestampMillisecond`, INSERT integer ms
4. **Natural upsert** — same (stock_code, ts) = last write wins, no special syntax needed
5. **No in-app caching** — all reads go through SQL, let GreptimeDB manage memory
6. **No memory limits** — GreptimeDB handles resource management internally
7. **No transactions needed** — each INSERT is independent, last-row merge gives natural upsert
8. **asyncpg pool requires custom connection class** — see "PostgreSQL Wire Protocol" above; missing any of the three overrides causes silent hangs
