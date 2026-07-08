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

API keys / endpoints (Tushare, xtquant broker, AWS Lambda render
service, 柏拉图AI etc.) are configured **via the Settings page** at runtime
and persisted under `data/`. Env vars (`LAMBDA_KLINE_URL`, `LAMBDA_KLINE_TOKEN`,
`BLTCY_API_KEY`, …) remain a fallback for fresh container bootstrap before
the web UI is reachable.

## K-line Technical Analysis

`POST /api/analyze-kline {"code":"000001","days":30}` — pulls OHLCV from
GreptimeDB, renders the chart in an overseas AWS Lambda (sidesteps mainland
OSS ICP-filing rules for public image URLs), then asks 柏拉图AI's vision model
(`gpt-5.5-pro`, locked) for a Chinese technical analysis. Configure the three
endpoints/keys on the Settings page → "K 线技术面分析（Lambda + 柏拉图AI）"
card. Full architecture: [docs/features.md ANA-001](docs/features.md). Initial
AWS resource bootstrap (S3 / ECR / IAM / Lambda function):
[lambda-kline/README.md](lambda-kline/README.md).

## Feishu AI Assistant (飞书 AI 助手)

在飞书群里 @机器人 发斜杠命令,容器内的 kimi-cli 作为"大脑"执行仓库里的技能
(`kimi-skills/`,随镜像发布,push → CI 绿 → watchtower 部署即生效)并把结果回到群里。
目前支持 `/持仓`(查当前证券账户持仓)、`/帮助`(能力列表,秒回;只 @ 不带命令也回它)。

- 接入方式:飞书官方长连接(WebSocket),不需要公网回调地址
- 群里能 @到机器人的人都能用(白名单功能暂屏蔽);助手只持有专用只读 key,
  交易接口物理上不可达
- **配置走 Settings 页「飞书 AI 助手」卡片**(App ID/Secret、只读 Key),落盘
  `data/assistant_config.json`(挂载卷,重部署不丢);配齐保存即自动启动,只读 Key
  改动即时生效,App ID/Secret 改动需重启。环境变量
  (`FEISHU_ASSISTANT_APP_ID/APP_SECRET`、`ASSISTANT_READONLY_KEY`)仅作首次引导兜底。
  缺配置时助手不启动,其余功能不受影响
- 详细设计:[docs/features.md AST-001](docs/features.md)

## Development

See [CLAUDE.md](CLAUDE.md) for development guidelines.

See [docs/features.md](docs/features.md) for feature specifications.
