# A-share-quant-trading

A quantitative trading system for China A-share market.

## Quick Start

```bash
# Install dependencies
uv sync

# Run the system
uv run python scripts/main.py
```

## Configuration

### Environment Variables

#### Feishu Alert Notifications (Optional)

The system can send alerts to Feishu (飞书) when errors or critical events occur.

| Variable | Required | Description |
|----------|----------|-------------|
| `FEISHU_APP_ID` | No | Feishu app ID |
| `FEISHU_APP_SECRET` | No | Feishu app secret |
| `FEISHU_CHAT_ID` | No | Target chat ID for alerts |
| `FEISHU_BOT_URL` | No | Bot relay service URL (has default) |

To enable Feishu alerts, set the first three environment variables:

```bash
export FEISHU_APP_ID="your_app_id"
export FEISHU_APP_SECRET="your_app_secret"
export FEISHU_CHAT_ID="your_chat_id"
```

When configured, the system will send:
- ✅ Startup notifications when the system starts
- ⚠️ Shutdown notifications when the system stops
- 🚨 Error alerts for exceptions and critical errors

#### iFinD API Credentials

| Variable | Required | Description |
|----------|----------|-------------|
| `IFIND_USERNAME` | Yes | iFinD API username |
| `IFIND_PASSWORD` | Yes | iFinD API password |

### V20 decision notifications

V20 reuses the V16 scanner, adds the frozen entry-defense and exit rules, and
publishes decisions through a durable Feishu outbox. It is a decision and
notification service only: it does not read account capacity and does not place,
cancel, or confirm broker orders.

The normal `main` image starts V20 in an embedded `forward_shadow` profile beside
V16. This is the production integration used by the existing host: it reuses the
working `DB_*`, Tushare token resolver, and `FEISHU_*` destination, while keeping
all V20 decisions and outbox rows in the separate PostgreSQL `v20` schema. The
input boundary remains the completed 09:39 bar. Set
`V20_EMBEDDED_ENABLED=false` only to disable this integration explicitly.

The two strategy deployment modes remain:

- `forward_shadow`: V20 records and sends decisions while the legacy V16 scan
  notification remains active. The embedded main profile uses the existing V16
  Feishu relay/chat and labels its messages as V20 shadow observations.
- `production_push`: V20 owns the official scan decision notification and the
  legacy V16 scan scheduler is disabled, including when V20 startup fails.

An optional isolated formal deployment is protected by three explicit settings:

```text
V20_ENABLED=true
V20_MODE=production_push
V20_ALLOW_PRODUCTION_PUSH=true
```

Those settings are necessary but not sufficient. Isolated production mode also requires
an accepted checkpoint in `config/v20.yaml` and a completed forward-shadow
acceptance. Do not use the retrospective research seed as a production
checkpoint.

The isolated profile uses a dedicated PostgreSQL writer (`V20_DB_HOST`,
`V20_DB_PORT`, `V20_DB_NAME`, `V20_DB_USER`, `V20_DB_PASSWORD`), reviewed CA
bundles, separate shadow/formal Feishu destinations, and two pairwise-distinct
HTTP secrets. The embedded profile intentionally uses the main runtime's
existing credentials and relay contract; it does not require separate V20 API
keys merely to run the scheduler or use the manual trigger. MEWS/ack writes and
the detailed V20 status endpoint remain protected by `V20_INGEST_API_KEY` and
`V20_STATUS_API_KEY` when those endpoints are used. See
[`config/v20.env.example`](config/v20.env.example) for the complete variable
names and [`docs/strategy-v20-runbook.md`](docs/strategy-v20-runbook.md) for
migration, activation, monitoring, API examples, and rollback.

Explicit `production_push` still runs only through `scripts/v20_main.py` (or the
Docker `v20` target), preserving the strict isolated boundary. The normal main
host never turns an implicit embedded profile into `production_push`.

The fourth endpoint, `POST /api/v20/trigger-scan`, is a manual deployment check.
It intentionally has no application-layer authentication for now and needs no
request header by default. If `Idempotency-Key` is omitted, the server assigns
`manual-<uuid>`; each additional headerless call creates a new non-actionable
verification receipt. A caller may instead supply a valid `Idempotency-Key` and
reuse it to make timeout retries idempotent. HTTP 202 means that the receipt is
durably queued, not that Feishu has accepted it. The endpoint never accepts a
request body, caller time, trade date, or force flag, and at or after 09:40 it
cannot recompute or replace the frozen decision with late data. Health, leader,
timing, and concurrency guards remain mandatory. See the runbook for response
and delivery verification.

Push CI publishes the normal runtime as `latest` and the commit SHA. The existing
main host's image watcher deploys that image, so embedded V20 follows the same
update path as V16; there is no second container to configure. Public
`/api/status` reports only safe V20 startup fields (`configured`, `enabled`,
`mode`, `started`, `startup_stage`, `retrying`, and sanitized error fields). Embedded startup
retries transient dependency failures in the background while V16 stays live. After deployment,
call the manual trigger
and verify both its HTTP response and the Feishu receipt. Startup failures also
include a sanitized `start_error_code`; raw connection text is never exposed.

## Development

See [CLAUDE.md](CLAUDE.md) for development guidelines.

See [docs/features.md](docs/features.md) for feature specifications

See [docs/strategy-v20.md](docs/strategy-v20.md) for the frozen V20 decision contract
