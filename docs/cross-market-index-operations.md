# 跨市场行业指数采集运行

部署定义是 [`deploy/cross-market/docker-compose.yml`](../deploy/cross-market/docker-compose.yml)，所有命令通过 `-p ashare-cross-market` 指定独立项目。生产主机现有工具为 `docker-compose 1.29.2`、Docker Engine `29.1.3`，没有 `docker compose` 插件；文件使用其支持的 `version: "3.7"` 格式。它运行指数采集器和专用 Yahoo 出口代理，接入生产已有的 `root_default` 网络；数据库使用 `http://greptimedb:4000`，代理使用 `http://yahoo-index-proxy:17897`。两个服务均不发布宿主端口。已有交易服务和 GreptimeDB 容器不在这个 Compose 项目中。

## 宿主文件

```text
/opt/ashare-cross-market/
  docker-compose.yml
  .env
  proxy/
    mihomo
    config.yaml
  runtime/
    scripts/collect_cross_market_indices.py
    src/__init__.py
    src/data/__init__.py
    src/data/yahoo_indices.py
    src/data/cross_market_store.py
    src/data/cross_market_ingest.py
    src/data/reference/cross_market/industry_boards.json
    src/data/reference/cross_market/industry_indices.json
  state/
```

`runtime` 是本次采集所需的最小代码和参考数据包，两个 `__init__.py` 为空文件。容器中 `/collector` 只读，`/state` 持久可写。宿主挂载路径和包中全部文件应在启动前存在，避免旧版 Compose 把缺少的文件路径自动创建成目录。更新运行包时停止采集器，替换包后再启动；保留 `state`，已有待处理窗口会先重放。

`.env` 中的 `CROSS_MARKET_RUNTIME_IMAGE` 填入生产 v15 镜像的完整不可变 image ID（`sha256:` 加 64 位十六进制摘要）。两个服务复用宿主已经存在的该镜像，采集入口覆盖镜像原入口。生产只读核对的 image ID 为 `sha256:5a6907a2e0a2ae63f6b05431f4a920da65a87a91d0605e1678521603919c1115`，其中 Python `3.13.15`、httpx `0.28.1`、两个交易所 ZoneInfo 及显式代理构造均可用。更换镜像后应重新核对依赖。

```dotenv
CROSS_MARKET_RUNTIME_IMAGE=sha256:<实际生产v15镜像ID>
CROSS_MARKET_SOURCE_REVISION=<采集代码的完整提交ID>
CROSS_MARKET_BUNDLE_SHA256=<实际部署runtime包的SHA256>
CROSS_MARKET_CONCURRENCY=2
CROSS_MARKET_LOOP_SECONDS=300
CROSS_MARKET_BATCH_SIZE=100
```

源提交和实际运行包摘要写入容器 labels，配合只读包核对部署版本。并发数和循环等待时间是可调整的工程参数，未限制行业或行情样本。一次循环完成后再等待设定秒数，因此开始时间之间还包含本轮取数和入库耗时。

`proxy/mihomo` 使用已核对官方发布摘要的 Linux 架构版本并设置执行权限；当前部署采用 [Mihomo v1.19.32 官方发布](https://github.com/MetaCubeX/mihomo/releases/tag/v1.19.32)。`proxy/config.yaml` 由部署环境提供，权限设为 `600`，节点地址和认证材料不进入仓库。配置 HTTP 代理端口 `17897`、`allow-lan: true`、`bind-address: 0.0.0.0`，使用已实测能取得 Yahoo 数据的节点。关闭 TUN、系统代理与外部控制端口；仅这个容器网络内的采集请求经过该代理。[Mihomo 通用配置说明](https://wiki.metacubex.one/en/config/general/)

## 启动与检查

在已准备好上述宿主文件后运行：

```sh
cd /opt/ashare-cross-market
test -x proxy/mihomo && test -f proxy/config.yaml && test -d state
for file in scripts/collect_cross_market_indices.py src/__init__.py src/data/__init__.py src/data/yahoo_indices.py src/data/cross_market_store.py src/data/cross_market_ingest.py src/data/reference/cross_market/industry_boards.json src/data/reference/cross_market/industry_indices.json; do test -f "runtime/$file" || exit 1; done
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml config --quiet
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml up -d
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml ps
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml logs --tail 20 cross-market-collector
```

采集器每轮向 stdout 输出一条 JSON：总状态、参考版本 SHA256、映射结果以及每个指数的写入行数、回放行数、最后核验源时间和未补齐时间。`verified` 表示该轮原始点写入后逐字段读回一致；`verified_with_gaps` 表示这些源点已一致入库，但仍有来源缺少收盘值的时间。映射的 `previously_verified` 表示同一参考 SHA256 已有成功核验状态，本轮复用该状态而未重写、重读静态映射；价格仍逐轮获取并核验。参考改变或映射状态文件丢失会重新完整写入和核验映射，已有 pending 始终先重放。`partial_failure` 保留独立失败项，其余指数继续采集。错误只输出类型和阶段，避免异常文本包含代理认证材料。

容器启动或存活不能证明行情更新。核查首轮、下一轮以及重启后的结果，并同时检查源时间、待处理文件和库中对应行。Greptime HTTP SQL 响应还须核对 `code`、`error` 和返回行；HTTP 200 本身不代表 SQL 成功。库内概览可使用：

```sql
SELECT market, symbol, interval, data_kind, COUNT(*) AS rows,
       MIN(ts) AS first_source_time, MAX(ts) AS last_source_time
FROM cross_market_index_prices
GROUP BY market, symbol, interval, data_kind;

SELECT market, reference_at, COUNT(*) AS industry_rows
FROM cross_market_industry_indices
GROUP BY market, reference_at;
```

每个参考版本应有 US、KR 各 134 行，共 268 行；未找到已核实指数的行业也保留完整对应状态和原因。实际行情只请求这些对应项引用的去重指数。行数或 `MAX(ts)` 只是概览，采集器的成功状态以整批精确键和全部字段回读为依据。

## 失败与重放

采集器在写价格前将完整原始 Yahoo JSON、源摘要、解析点和窗口存入 `/state/*.pending.json`。写库部分成功、HTTP 错误、读回不一致或状态保存失败时，成功游标不前进。下轮或进程重启先按原始整批重放，逐字段核验后才保存成功状态并清理 pending；不要以数据库最大时间替代状态，也不要删除 pending 来掩盖失败。

首次获取源端可用的完整日线历史；之后从成功源时间回溯一天覆盖可更新日线。未补齐源点把拉取起点提前到最早缺口。指数身份为 `index_id/market/symbol`，参考升级为支持日线历史时保留原待处理窗口并在其重放后完整取历史，无需清空状态。

代理临时不可用时保留原始 pending 和源时间；同一 Yahoo 客户端共享 HTTP 429 冷却时间并做有限退避重试。循环模式会继续后续轮次。恢复代理可单独重启 `yahoo-index-proxy`；要核验采集重启，可重启 `cross-market-collector`，核对 `replayed_rows` 和状态文件。

手工执行一轮时先停止持续采集器，避免两个进程同时使用同一状态目录：

```sh
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml stop cross-market-collector
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml run --rm --no-deps cross-market-collector --proxy http://yahoo-index-proxy:17897 --greptime-url http://greptimedb:4000 --state-dir /state --concurrency 2 --loop-seconds 0
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml start cross-market-collector
```

一次执行全部核验成功返回 0，有失败返回非 0。普通进程重启使用 `docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml restart cross-market-collector`，不重建容器。更新容器定义时对这个项目使用 `stop`、`rm -f`、`up -d`，避免 Compose v1 在新 Docker 上对已有容器做 recreate 时读取已移除的 `ContainerConfig` 字段：

```sh
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml stop
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml rm -f
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml up -d
```

上述命令只处理独立项目的两个容器，宿主状态目录和既有外部网络保留。停止这个独立项目也可使用 `docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml down`。服务使用 `restart: unless-stopped`。配置解析成功仅证明该 CLI 接受定义，实际创建、重启及下一轮行情仍须核验。[Docker Compose 服务定义](https://docs.docker.com/reference/compose-file/services/)、[外部网络定义](https://docs.docker.com/reference/compose-file/networks/)

## 源数据边界

Yahoo 当前实测的部分韩国行业指数只发布最新快照，未提供可回补的历史日线。这些参考项标为 `snapshot_only`，按发布的源时间收集快照，重复源时间幂等覆盖；未发布的数据不生成历史 OHLC。原接口没有的 OHLC、成交量等保持空值，存在的真实值保留。

当前行情是否最终确认由源交易时段证据决定，`is_final` 可为 `False` 或空值，抓取发生在收盘后不单独证明最终确认。`ts` 是源时间毫秒，`fetched_at` 是取回时间，两者分别保存；接口延迟不得冒充新行情。Yahoo 非官方接口和代理未来可用性没有 SLA，本部署的持续结果应由实际每轮记录及源/库核验确认。
