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

V20 is disabled by default. The two supported deployment modes are:

- `forward_shadow`: V20 records and sends only to the isolated shadow route;
  the legacy V16 scan notification remains active.
- `production_push`: V20 owns the official scan decision notification and the
  legacy V16 scan scheduler is disabled, including when V20 startup fails.

Formal push is protected by three explicit settings:

```text
V20_ENABLED=true
V20_MODE=production_push
V20_ALLOW_PRODUCTION_PUSH=true
```

Those settings are necessary but not sufficient. Production mode also requires
an accepted checkpoint in `config/v20.yaml` and a completed forward-shadow
acceptance. Do not use the retrospective research seed as a production
checkpoint.

V20 uses a dedicated PostgreSQL writer (`V20_DB_HOST`, `V20_DB_PORT`,
`V20_DB_NAME`, `V20_DB_USER`, `V20_DB_PASSWORD`), hostname-verifying reviewed
CA bundles for both database roles, separate reviewed shadow/formal Feishu
destinations, and two pairwise-distinct HTTP secrets: `V20_INGEST_API_KEY`
for evidence writes and `V20_STATUS_API_KEY` for status reads. See
[`config/v20.env.example`](config/v20.env.example) for the complete variable
names and [`docs/strategy-v20-runbook.md`](docs/strategy-v20-runbook.md) for
migration, activation, monitoring, API examples, and rollback.

Formal V20 must run through the dedicated `scripts/v20_main.py` process (or the
Docker `v20` target). That host exposes only four `/api/v20` endpoints and
does not construct the platform PositionManager, order/holding APIs, or iQuant
router. The default Docker target remains the legacy platform/V16 process, and
the checked-in V20 configuration remains disabled.

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

Passing the repository push CI does not deploy this endpoint. CI publishes the
legacy target as `latest`/the commit SHA and the dedicated V20 target as
`v20-latest`/`v20-<commit-sha>`, while V20 remains disabled by default. A host
must explicitly run the V20 image with the reviewed environment and secret
mounts. Before triggering, verify `/api/v20/status` reports the expected
enabled/running service identity, and afterwards verify the durable outbox and
the target Feishu chat.

## Development

See [CLAUDE.md](CLAUDE.md) for development guidelines.

See [docs/features.md](docs/features.md) for feature specifications

See [docs/strategy-v20.md](docs/strategy-v20.md) for the frozen V20 decision contract
