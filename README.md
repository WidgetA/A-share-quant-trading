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

## Development

See [CLAUDE.md](CLAUDE.md) for development guidelines.

See [docs/features.md](docs/features.md) for feature specifications