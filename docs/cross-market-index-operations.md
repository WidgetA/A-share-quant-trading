# 跨市场行业指数采集运行

国内采集器通过阿里云官方 FC SDK 同步调用美国 `us-west-1` 的独立函数 `ashare_yahoo_indices_v15`。美国函数直接访问 Yahoo，返回完整原始响应；国内校验身份、SHA256 和源数据，再写入已有 GreptimeDB。映射、pending、成功游标和读回核验均在国内处理。FC 的构建和部署见 [`serverless/yahoo_indices/README.md`](../serverless/yahoo_indices/README.md)。

部署定义为 [`deploy/cross-market/docker-compose.yml`](../deploy/cross-market/docker-compose.yml)，所有命令通过 `-p ashare-cross-market` 指定独立项目。生产主机现有工具为 `docker-compose 1.29.2`、Docker Engine `29.1.3`，没有 `docker compose` 插件；文件使用其支持的 `version: "3.7"` 格式。项目运行 `cross-market-collector` 和 `cross-market-intraday-collector`，接入已有 `root_default` 网络，数据库地址为 `http://greptimedb:4000`，不发布宿主端口。已有交易服务和 GreptimeDB 容器不在该项目中。

## 宿主文件与依赖

```text
/opt/ashare-cross-market/
  docker-compose.yml
  .env
  fc.credentials.env
  runtime/
    vendor/                         # 官方 FC SDK 及其 Linux 依赖
    scripts/collect_cross_market_indices.py
    scripts/collect_cross_market_intraday.py
    src/__init__.py
    src/data/__init__.py
    src/data/yahoo_indices.py
    src/data/yahoo_intraday_indices.py
    src/data/fc_yahoo_indices.py
    src/data/fc_intraday_indices.py
    src/data/cross_market_store.py
    src/data/cross_market_ingest.py
    src/data/cross_market_intraday_ingest.py
    src/data/reference/cross_market/industry_boards.json
    src/data/reference/cross_market/industry_indices.json
    src/data/reference/cross_market/intraday_indices.json
  state-fc/                         # 当前 FC 采集状态
  state-intraday-fc/                 # 每指数、分钟粒度独立的窗口状态
  state/                            # 原代理路线历史状态，保留
  proxy/                            # 原代理路线历史文件，保留
```

`runtime` 是采集所需的最小代码、参考数据和 SDK 包，两个 `__init__.py` 为空文件。容器中 `/collector` 只读，`/state` 挂载宿主 `state-fc` 并持久可写；`PYTHONPATH=/collector/vendor:/collector` 使独立 SDK 包先于镜像内依赖加载。当前路线从独立 `state-fc` 开始，原 `state` 不迁移、不删除。

SDK 依赖定义为 [`deploy/cross-market/requirements-fc.txt`](../deploy/cross-market/requirements-fc.txt)，固定官方 `alibabacloud-fc20230330==4.8.2`。构建 vendor 时使用匹配生产 Linux x86_64、CPython 3.13 的 wheels（包括适用的 `abi3` 与纯 Python wheels），不能把 Windows 的 `.pyd`、`.dll` 或本机虚拟环境复制进容器。可在匹配的 Linux Python 3.13 构建环境执行 `python3.13 -m pip install --only-binary=:all: --target <独立构建目录>/vendor -r deploy/cross-market/requirements-fc.txt`，再将结果打包到宿主 `runtime/vendor`；不在生产镜像中安装或覆盖包。已在现有生产镜像内验证 SDK `4.8.2`、Darabonba `1.0.9`、Tea OpenAPI `0.4.6` 和 cryptography `50.0.2` 可导入，见 [SDK 依赖实测](../dev-tools/cross-market-yahoo/production_probe/fc_domestic_dependency_probe.json)。更新 vendor 后重新核对导入与 SDK 流式响应读取。

宿主挂载路径和全部包文件应在启动前存在，避免旧版 Compose 把缺少的文件路径创建成目录。更新运行包时先停止采集器，替换包后再启动；保留 `state-fc`，已有待处理窗口先重放。

`.env` 中的 `CROSS_MARKET_RUNTIME_IMAGE` 填入生产 v15 的完整不可变 image ID。当前使用 `sha256:5a6907a2e0a2ae63f6b05431f4a920da65a87a91d0605e1678521603919c1115`，Python 为 `3.13.15`；这个采集部署不重建或替换交易镜像。

```dotenv
CROSS_MARKET_RUNTIME_IMAGE=sha256:<实际生产v15镜像ID>
CROSS_MARKET_SOURCE_REVISION=<采集代码的完整提交ID>
CROSS_MARKET_BUNDLE_SHA256=<实际部署runtime包的SHA256>
CROSS_MARKET_FC_ENDPOINT=https://<账号ID>.us-west-1.fc.aliyuncs.com
CROSS_MARKET_CONCURRENCY=2
CROSS_MARKET_LOOP_SECONDS=300
CROSS_MARKET_BATCH_SIZE=100
CROSS_MARKET_INTRADAY_CONCURRENCY=2
CROSS_MARKET_INTRADAY_LOOP_SECONDS=300
CROSS_MARKET_INTRADAY_BATCH_SIZE=1000
```

`CROSS_MARKET_FC_ENDPOINT` 是已验证账号的 FC API 接入点，不是公开 HTTP 触发器。Compose 已指定函数名 `ashare_yahoo_indices_v15` 和区域 `us-west-1`。源提交和实际运行包摘要写入容器 labels，配合只读包核对部署版本。并发数、写库批量和循环等待时间是工程参数，不限制行业或行情样本；一次循环完成后才等待设定秒数。

宿主 `fc.credentials.env` 权限为 `600`，由部署过程复用既有训练配置中的 AccessKey ID/Secret，内容使用 SDK 适配器的变量名：

```dotenv
ALIYUN_ACCESS_KEY_ID=<既有训练凭证的AccessKey ID>
ALIYUN_ACCESS_KEY_SECRET=<既有训练凭证的AccessKey Secret>
```

该文件不进入仓库、镜像、FC ZIP 或日志。复用凭证只用于独立采集函数的签名调用，不启动、更新或改变训练函数。美国函数自身不需要国内数据库地址或这份签名凭证。

## 启动与检查

分钟采集沿用全部已采用的美国 131、韩国 39 个行业指数，独立能力表 [`intraday_indices.json`](../src/data/reference/cross_market/intraday_indices.json) 绑定既有指数映射 SHA256。两个市场的每个指数均已用本机显式代理验证真实 `1m` 序列和旧 `5m` 序列，之后才部署 FC。原日线接口的 `snapshot_only` 只描述该接口日线历史，不限制分钟接口。

源端实测限制为：`1m` 最近 30 天、单次最多 8 天；`5m` 最近 60 天。因此首次一分钟补灌分成最多 7 天的连续请求，五分钟覆盖更早可取历史；不能用 Yahoo 补出 2023 年分钟行情。起点随实际请求时间校正，已过保留期的范围记录在 `retention_unavailable_ranges`，不伪造覆盖。完成全轮后等待 300 秒自动续补，回取最新真实分钟前一小时并重试保留期内的源 NULL 缺口。300 秒是轮间等待，不代表每个指数固定五分钟内更新完。

分钟容器的 `/state` 挂载 `state-intraday-fc`；日线状态仍在 `state-fc`。原始响应、请求窗口与完整解析点先原子落 pending，再写库并逐字段读回，成功才推进 `covered_until_s` 和删除 pending。覆盖游标使用已核验的请求窗口终点；实时追加报价不能推进历史覆盖。重启先回放已保存的原始窗口，即便该窗口现在已超出 Yahoo 保留期。

分钟 SQL 请求允许等待 120 秒。初灌中实际出现过 30 秒客户端读超时，原始窗口保留后完整重放；超时仍不推进游标。每个序列结束立即输出不含原始响应或凭证的进度 JSON，整轮结果另外输出，不能把单个进度事件当作全部 340 个序列完成。

库中 `interval='1m'/'5m'`、`data_kind='minute_bar'` 是真实分钟行情；源端追加最新报价单独标记 `minute_quote_snapshot`，保留原始时钟与数值。休市窗口只有经过身份、粒度、SHA 和原始空数组确认的 HTTP 200 回执才允许推进零行覆盖；404、报价单点和取数错误继续保留为失败。源 OHLC/成交量和缺值按实际返回保存，不能据指数 volume 推断股票成交量，也不合成源未发布的尾盘分钟。

```sh
cd /opt/ashare-cross-market
test -d state-fc && test -d runtime/vendor && test -f fc.credentials.env
test "$(stat -c %a fc.credentials.env)" = 600
for file in scripts/collect_cross_market_indices.py src/__init__.py src/data/__init__.py src/data/yahoo_indices.py src/data/fc_yahoo_indices.py src/data/cross_market_store.py src/data/cross_market_ingest.py src/data/reference/cross_market/industry_boards.json src/data/reference/cross_market/industry_indices.json; do test -f "runtime/$file" || exit 1; done
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml config --quiet
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml up -d
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml ps
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml logs --tail 20 cross-market-collector
```

旧代理容器在切换时停止。当前 Compose 不再定义或引用它；保留历史代理文件和原状态，不以重启旧代理作为恢复 FC 路线的步骤。

采集器每轮向 stdout 输出一条 JSON：总状态、参考 SHA256、映射结果和每个指数的写入行数、回放行数、最后核验源时间、未补齐时间。`verified` 表示该轮原始点写入后逐字段读回一致；`verified_with_gaps` 表示这些源点已一致入库，但仍有来源缺少收盘值的时间。映射的 `previously_verified` 表示同一参考 SHA256 已有成功核验状态，本轮未重写、重读静态映射；价格仍逐轮获取并核验。参考改变或映射状态文件丢失会重新完整写入和核验映射。

`partial_failure` 保留独立失败项，其余指数继续采集。错误输出类型和阶段；写库错误另带此前已确认行数 `confirmed_rows` 和直接原因类型 `cause_type`。已确认行数不代表后续未确认的行没有落库，仍须整批重放并回读。FC HTTP 200 还须核对函数错误标记、完整响应流、请求回显、运行区域及源 SHA256，不能单凭调用受理判成功。

容器启动或存活不能证明行情更新。核查首轮、下一轮和重启后的结果，同时检查源时间、pending 和库中对应行。Greptime HTTP SQL 响应须核对 `code`、`error` 和返回行；HTTP 200 本身不代表 SQL 成功。库内概览可使用：

```sql
SELECT market, symbol, "interval", data_kind, COUNT(*) AS rows,
       MIN(ts) AS first_source_time, MAX(ts) AS last_source_time
FROM cross_market_index_prices
GROUP BY market, symbol, "interval", data_kind;

SELECT market, reference_at, COUNT(*) AS industry_rows
FROM cross_market_industry_indices
GROUP BY market, reference_at;
```

每个参考版本应有 US、KR 各 134 行，共 268 行；未找到已核实指数的行业也保留完整对应状态和原因。行情请求覆盖对应项引用的全部去重指数。行数或 `MAX(ts)` 只是概览，采集器成功状态以整批精确键和全部字段回读为依据。

## 失败与重放

写价格前，采集器把完整原始 Yahoo JSON、源摘要、解析点和窗口存入 `/state/*.pending.json`。写库部分成功、HTTP 错误、读回不一致或状态保存失败时，成功游标不前进。下轮或进程重启先按原始整批重放，逐字段核验后保存成功状态并清理 pending；不以数据库最大时间替代状态，不删除 pending 来掩盖失败。

支持日线的指数首次获取源端可用的完整日线历史；之后从成功源时间回溯一天覆盖可更新日线。未补齐源点把拉取起点提前到最早缺口。指数身份为 `index_id/market/symbol`，参考升级为支持日线时保留原待处理窗口，并在重放后完整取历史，无需清空状态。

FC 调用或 Yahoo 暂时失败时保留 pending 和成功源时间，循环模式继续后续轮次。国内 SDK 适配器对临时网络错误、429 和 5xx 做有限重试；429 遵守 `Retry-After` 冷却。美国函数同一实例复用专属事件循环和 Yahoo 客户端，也保留该实例的 429 冷却；该状态不跨实例共享，也不持久化。函数抛错不返回成功行情包；国内不会把函数失败、错身份或错摘要当作可写数据。

手工执行一轮时先停止持续采集器，避免两个进程同时使用同一状态目录。下例从宿主非凭证 `.env` 载入 FC 接入点，凭证仍由 Compose 的 `env_file` 注入容器：

```sh
. ./.env
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml stop cross-market-collector
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml run --rm --no-deps cross-market-collector --fc-endpoint "${CROSS_MARKET_FC_ENDPOINT:?Set the US FC endpoint}" --fc-function ashare_yahoo_indices_v15 --greptime-url http://greptimedb:4000 --state-dir /state --concurrency 2 --loop-seconds 0
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml start cross-market-collector
```

一次执行全部核验成功返回 0，有失败返回非 0。普通进程重启使用 `docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml restart cross-market-collector`。更新容器定义时使用 `stop`、`rm -f`、`up -d`，避免 Compose v1 在新 Docker 上重建已有容器时读取已移除的 `ContainerConfig` 字段：

```sh
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml stop
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml rm -f
docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml up -d
```

这些命令只处理当前项目的采集容器，宿主状态、历史代理文件和已有外部网络保留。停止项目也可使用 `docker-compose -p ashare-cross-market --env-file .env -f docker-compose.yml down`。服务使用 `restart: unless-stopped`；配置解析成功仅说明 CLI 接受定义，实际创建、重启和下一轮行情仍须核验。

## 源数据边界与已有证据

当前参考有 170 个去重行业指数，US 131 个、KR 39 个。其中 7 个 US 指数可取真实历史日线，其余 124 个 US 和 39 个 KR 指数的日线接口只提供快照；这些指数的分钟接口已另行核实并采集。`snapshot_only` 按发布的源时间收集快照，重复源时间幂等覆盖；未发布的数据不生成历史 OHLC。原接口没有的 OHLC、成交量保持空值，真实值保留。历史响应中的全空点保留源时间并记录为缺口。

源交易时段证据决定行情是否最终确认，`is_final` 可为 `False` 或空值，抓取发生在收盘后不能单独证明最终确认。`ts` 保存源时间毫秒，`fetched_at` 保存取回时间；接口延迟不冒充新行情。Yahoo 可能在同一交易日修改未结束的日线点及其源时间，库按实际 `ts` 保存观测，`trade_date` 保存交易所当地日期；总行数不等于独立交易日数。

2026-10-06 的[生产验收记录](cross-market-fc-verification.json)确认美国 FC 路线完整执行全部 170 指数；国内无代理，Greptime 中 268 条完整映射 JSON 与正式参考一致，170 个最新源键独立读回通过。真实中断的 SOX 8161 点待写窗口在重启后整批重放成功；随后更新至 `07ee8a6ae767f4617a36478983faf01d01b120c3`，新完整轮次仍无失败和 pending。154 项行为测试通过，原训练函数与原交易、数据库服务保留原版本和运行状态。

保存的 SOX 8161 点与 XAU 10783 点原始响应经过 SHA 校验和逐点重新解析，与原 pending 完全一致；库内全部源键存在，分别 8159、10781 个未被增量更新的历史行全部 18 字段一致。各窗口最近两个源行后来正常刷新，记录保留了实际字段差异，并未把原窗口价格改成新期望值。XAU 原始响应为 1,122,244 字节，实际完整回传；不据此推断平台未核实的统一响应上限。历史来源原有的 8 个空收盘点仍按原始源时间保留。行情总行数包含实际同日不同源时间的观测，不等于独立交易日数。Yahoo 接口的持续可用性由每轮实际源/库记录核验。

## 分钟生产验收

2026-10-07 04:02（新加坡时间）的[分钟验收记录](cross-market-intraday-verification.json)确认全部 170 个行业指数的 `1m`、`5m` 共 340 个序列完成源端可取历史的初次补灌。独立库内核对有 1,866,537 条 `minute_bar`：US 一分钟 1,073,020 条、五分钟 420,053 条；KR 一分钟 266,760 条、五分钟 106,704 条。源端追加的 `minute_quote_snapshot` 单独保存，不计入上述分钟行情条数。

当前国内实际源码为 `fa832d80e3014e80d7a6d3410dae6551545384dd`，运行包 SHA256 为 `8accd1be2f9be4814b334cc6737d06544d8544a96389f998ed0c95e0d789b327`，1685 个包文件已逐个核对；两台采集容器均无代理。美国 FC 实际 ZIP 下载与上传包逐字节相同，SHA256 为 `726170f11e4b522d122d780f30e9994cab23fc97cc1c3ec8855aaf1b463b8c06`，原训练函数未变。当前版本连续完成三次覆盖全部 340 个序列的实际轮次，后两次由运行中的循环自动触发，所有源窗口写入及读回行数一致；独立检查时失败项、pending 均为 0。300 秒为整轮结束后的等待时间。

真实中断并重启的四个待写窗口共 7420 个源键已独立读回，原始响应及 SHA 保留。其中 KOSPI-7 五分钟有 9 个成交量后来被源端新响应更新，实际同窗口 FC 再取的原始值均与库内新值一致；不把来源刷新或窗口差异猜成确定原因。此前错误混入分钟行情的 78 个韩国 15:00 尾点已按原始证据归为快照，原价格保留，错误分钟键已精确删除；前一分钟的最终确认标记也按实际证据纠正。初灌时两个真实 SQL 读超时窗口完整保留并在调整超时后核验重放成功。

验收时剩余 19 个收盘缺值均在 `^DRG` 的两个粒度中，库内相应源行存在且为空，继续在源保留期内重试，不伪造价格。其中 9 月 23 日纽约时间 12:21 的旧空点，经只读原生 FC 短窗口再取确认源数组仍明确返回空 OHLC／成交量，不猜测缺值原因。分钟源仅提供最近约 30 天／60 天，不能补到 2023 年；滚动保留期和历史请求起点的边界记录保留在每个序列状态中，不据此声称交易所全部时段均有源数据。原 268 条完整映射 JSON 与 170 个日线／快照最新源键再次核对通过，US、KR 两种分钟粒度均通过已有 SQL 查询转发接口实际查询。229 项相关行为测试及 Ruff 通过；实际生产验收另有原始响应、逐字段入库检查、独立查询与重放证据。没有 Git push。

查询真实分钟行情时显式限定粒度和数据类型，例如：

```sql
SELECT ts, open, high, low, close, volume, is_final, fetched_at
FROM cross_market_index_prices
WHERE provider = 'yahoo' AND market = 'KR' AND symbol = 'KOSPI-10.KS'
  AND "interval" = '1m' AND data_kind = 'minute_bar'
  AND ts >= '2026-10-06T00:00:00Z' AND ts < '2026-10-07T00:00:00Z'
ORDER BY ts;
```
