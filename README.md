# A-share-quant-trading

A quantitative trading system for China A-share market.

## Quick Start (Local)

```bash
uv sync
uv run uvicorn src.web.app:create_app --factory --host 0.0.0.0 --port 8000
```

## Docker Deployment

### Prerequisites

- Docker
- Docker Compose plugin (`docker compose`) or standalone (`docker-compose`)
- ACR login: `docker login crpi-mwcsioo1h0fmepx3.cn-shanghai.personal.cr.aliyuncs.com`

### Install Docker Compose plugin (if needed)

```bash
mkdir -p /usr/local/lib/docker/cli-plugins
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
```

### Deploy

```bash
# Start (first time or after config change)
docker compose up -d

# Restart with latest image
docker compose down && docker compose pull && docker compose up -d

# View logs
docker logs root_trading-service_1 --tail 100 -f
```

Watchtower auto-pulls new images every 60 seconds. Manual restart is only needed if the container is stuck.

### Image Tags

| Tag | Branch | Usage |
|-----|--------|-------|
| `latest` | main | Production |
| `test` | feature branches | Testing |

## Configuration

### Environment Variables

All config is in `docker-compose.yml`. Key variables:

| Variable | Description |
|----------|-------------|
| `DB_HOST/PORT/USER/PASSWORD/NAME` | PolarDB connection |
| `FEISHU_APP_ID/SECRET/CHAT_ID` | Alert notifications |
| `GREPTIME_HOST/PORT` | GreptimeDB backtest cache (default: greptimedb:4003) |
| `WEB_BASE_URL` | Public URL for the web UI |
| `LAMBDA_KLINE_URL` | Overseas K-line render Lambda Function URL (see [lambda-kline/README.md](lambda-kline/README.md)) |
| `LAMBDA_KLINE_TOKEN` | Shared secret for the Lambda renderer (matches Lambda env `UPLOAD_TOKEN`) |
| `BLTCY_API_KEY` | 柏拉图AI key for vision-LLM K-line analysis |

## K-line Technical Analysis

`POST /api/analyze-kline {"code":"000001.SZ","days":30}` — pulls OHLCV from
GreptimeDB, renders the chart in an overseas AWS Lambda (sidesteps mainland
ICP filing rules for public image URLs), then asks 柏拉图AI's vision model for
a Chinese technical analysis. See [docs/features.md ANA-001](docs/features.md)
for the full architecture and [lambda-kline/README.md](lambda-kline/README.md)
for one-time AWS bootstrap.

## Development

See [CLAUDE.md](CLAUDE.md) for development guidelines.

See [docs/features.md](docs/features.md) for feature specifications.
