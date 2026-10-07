# 跨市场行业指数采集运行

## v15 共用索引和数据来源

这套基础设施随 `refactor/cleanup-v15-only` 开发和发布。索引保持现有 170 个原始指数及其 A 股二级行业对应关系；Massive 的供应商代码单独记录在 `src/data/reference/cross_market/massive_indices.json`，不改变原 `index_id`、Yahoo `symbol`、编制方或指数口径。不会用 ETF、期货、CFD 或其他指数补位。

美股已核实同原价格指数身份的 124 个指数，已由 Massive 免费 Indices Basic 完成本次历史五分钟回灌，未来增量沿用美国 FC 的 Yahoo 分钟采集。两种来源统一写入 `cross_market_index_prices`：历史来源为 `provider='massive'`，Yahoo 为 `provider='yahoo'`，同一原指数仍用同一个原 `symbol`。两种供应商在重叠日期可能各有一份观测，查询时显式选择来源，不将两份观测当作两个原指数或直接相加。

历史任务从 2023-01-01 发起请求，以接口实际发布的时间和数值为准。季度窗口遇到 `queryCount` 达到 50000 个基础聚合或返回分页信号时继续拆窗，不将截断结果算作窗口完成。数据先保存原始响应，再写入并读回全部 18 个字段；源状态 `OK` 且核验成功后才推进完整窗口游标。`DELAYED` 返回的真实数据同样写入并读回，另存原始响应和回执，但该请求窗口继续留在待补队列，不把已收到的末条价格当作窗口已完整发布。旧版本误标完成的 `DELAYED` 窗口依据已保存的原始响应重新打开。权限拒绝、限流、请求错误及成功返回空数组分别记录，不制造缺少的价格或时间。

整个临时 Key 只由一个历史任务使用，HTTP 请求启动间隔至少 13 秒，重试也进入同一队列；429 按服务端冷却时间等待。Key 从 Git 目录之外的本地文件读取，不写入源码、参考数据、日志、发布包或 Git。已有短窗口验证不代表整个历史范围已经回灌；具体完成范围以实际任务记录和 Greptime 数据为准。

韩国在真实一分钟、五分钟增量之外，增加 39 个原指数的 730 天小时历史，用于回测。`interval='1h'`、`data_kind='hour_bar'` 保存真实小时线，源端尾部报价保存为 `hour_quote_snapshot`。小时线只做一次历史种子写入、读回和原始源归档，之后的自动循环继续采集分钟线，不每轮重拉 730 天。种子成功后的小时状态为 `previously_verified`，原种子覆盖终点和源时间保留；434 个原始空收盘点也不会导致每轮重新请求整个种子。源返回的空值保留，不能用它生成不存在的分钟行情。

## 同分支发布与回灌

CI 根据整个 push 的前后提交判断是否有基础设施变化。实现、参考数据、FC worker、采集器、依赖和部署脚本一起维护；文档变更不单独触发 FC 或采集器重新部署。原训练函数只随训练源码变化发布。

发布先通过检查并构建带完整 Git SHA 的镜像，再由 `deploy/cross-market/build_release.py` 从同一提交构建 Linux FC ZIP、国内 runtime 和摘要清单。`deploy/cross-market/deploy_release.py` 在同一发布队列内先核对实际已部署版本，再依次更新并核对美国 FC 和两个国内采集器。已经上线的新提交不会被迟到的旧任务覆盖；首次迁移兼容旧 FC 没有提交标记的事实，后续发布写入明确版本。v15 的 `test` 镜像别名单独排队，在不可变镜像构建完成后读取真实分支头，再更新当前提交的别名。实际成功仍须有新完整采集轮次及 Greptime 读回证据。

临时 Massive 回灌使用 `scripts/backfill_cross_market_massive.py`。本次任务使用本机代理取得历史数据，国内自动增量仍由美国 FC 取数。回灌状态目录保留每个原指数的完整源响应、验证回执、实际源起止日期、空窗口及尚未完成的窗口；再次运行先重放已保存待写源，再继续剩余请求，不重复推进成功计数。数据库失败不会被当作来源无数据。

临时历史任务写库和回读经已有 SSH 链路访问宿主 Docker 网络内的 `http://greptimedb:4000`，避免公开交易服务 SQL 转发层约 30 秒的等待边界。真实小时汇总 SQL 曾用时 32,649 毫秒，原生数据库返回成功；这类长查询不能仅增加转发客户端超时解决。此路径不公开数据库端口、不更换数据库，也不改变持续采集器的库地址。日线及日内采集的 Greptime HTTP 客户端超时均为 120 秒。

维护时可按原索引直接查两种真实粒度，例如：

```sql
SELECT ts, "open", high, low, "close", volume, fetched_at
FROM cross_market_index_prices
WHERE provider = 'massive' AND market = 'US' AND symbol = '^SOX'
  AND "interval" = '5m' AND data_kind = 'minute_bar'
  AND ts >= '2023-01-01T00:00:00Z'
ORDER BY ts;

SELECT ts, "open", high, low, "close", volume, fetched_at
FROM cross_market_index_prices
WHERE provider = 'yahoo' AND market = 'KR' AND symbol = 'KOSPI-10.KS'
  AND "interval" = '1h' AND data_kind = 'hour_bar'
ORDER BY ts;
```

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
    scripts/backfill_cross_market_massive.py
    src/__init__.py
    src/data/__init__.py
    src/data/yahoo_indices.py
    src/data/yahoo_intraday_indices.py
    src/data/fc_yahoo_indices.py
    src/data/fc_intraday_indices.py
    src/data/cross_market_store.py
    src/data/cross_market_ingest.py
    src/data/cross_market_intraday_ingest.py
    src/data/massive_indices.py
    src/data/cross_market_massive_ingest.py
    src/data/reference/cross_market/industry_boards.json
    src/data/reference/cross_market/industry_indices.json
    src/data/reference/cross_market/intraday_indices.json
    src/data/reference/cross_market/massive_indices.json
  state-fc/                         # 当前 FC 采集状态
  state-intraday-fc/                 # 每指数、日内粒度独立的窗口状态
    hour-seed-sources/               # 39 个一次性小时种子的原始源归档
  state/                            # 原代理路线历史状态，保留
  proxy/                            # 原代理路线历史文件，保留
```

`runtime` 按 [`build_release.py`](../deploy/cross-market/build_release.py) 的清单发布 16 个源码／参考文件及两个空 `__init__.py`，共 18 个文件，另带 Linux SDK vendor。FC worker 单独进入云 ZIP，不是国内 runtime 中的文件。容器中 `/collector` 只读；日线容器 `/state` 挂载宿主 `state-fc`，日内容器挂载 `state-intraday-fc`，均持久可写。`PYTHONPATH=/collector/vendor:/collector` 使独立 SDK 包先于镜像内依赖加载。最初 FC 路线从独立 `state-fc` 开始；后续发布保留现有日线、分钟和小时状态、pending 及种子归档，原代理 `state` 不迁移、不删除。

SDK 依赖定义为 [`deploy/cross-market/requirements-fc.txt`](../deploy/cross-market/requirements-fc.txt)，固定官方 `alibabacloud-fc20230330==4.8.2`。构建 vendor 时使用匹配生产 Linux x86_64、CPython 3.13 的 wheels（包括适用的 `abi3` 与纯 Python wheels），不能把 Windows 的 `.pyd`、`.dll` 或本机虚拟环境复制进容器。可在匹配的 Linux Python 3.13 构建环境执行 `python3.13 -m pip install --only-binary=:all: --target <独立构建目录>/vendor -r deploy/cross-market/requirements-fc.txt`，再将结果打包到宿主 `runtime/vendor`；不在生产镜像中安装或覆盖包。最初 FC 部署已在当时生产镜像内验证 SDK `4.8.2`、Darabonba `1.0.9`、Tea OpenAPI `0.4.6` 和 cryptography `50.0.2` 可导入，见 [SDK 依赖实测](../dev-tools/cross-market-yahoo/production_probe/fc_domestic_dependency_probe.json)。后续更新 vendor 仍核对实际导入与 SDK 流式响应读取。

宿主挂载路径和全部包文件应在启动前存在，避免旧版 Compose 把缺少的文件路径创建成目录。更新运行包时先停止采集器，替换包后再启动；保留 `state-fc`、`state-intraday-fc` 及其小时归档，已有待处理窗口先重放。

`.env` 中的 `CROSS_MARKET_RUNTIME_IMAGE` 使用同提交 CI 镜像实际拉取后的完整不可变 image ID。037 验收时两个采集器均为 `sha256:0f400ea910599714c6651227abba9a1771c7db2bc5d09c609516f4b6ec338cc5`；每次发布以 manifest、运行 labels 和实际 image ID 核对，不沿用旧阶段的固定镜像。采集部署脚本只更新两个采集器；主交易服务镜像由现有 Watchtower 流程更新。

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

日线及日内 SQL 请求允许等待 120 秒。分钟初灌中实际出现过 30 秒客户端读超时，原始窗口保留后完整重放；超时仍不推进游标。每个序列结束立即输出不含原始响应或凭证的进度 JSON，整轮结果另外输出，不能把单个进度事件当作全部序列完成。当前日内整轮为 340 个连续分钟序列加 39 个一次性小时种子，共 379 个序列；小时种子已完成后逐轮返回保留原覆盖终点的 `previously_verified`。

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

采集器每轮向 stdout 输出一条 JSON：总状态、参考 SHA256、映射结果和每个指数的写入行数、回放行数、最后核验源时间、未补齐时间。`verified` 表示该轮原始点写入后逐字段读回一致；`verified_with_gaps` 表示这些源点已一致入库，但仍有来源缺少收盘值的时间。映射的 `previously_verified` 表示同一参考 SHA256 已有成功核验状态，本轮未重写、重读静态映射；日线和连续分钟价格仍逐轮获取并核验。小时 `history_seed` 的 `previously_verified` 表示沿用已写入、读回并归档的初始种子，不声称本轮重新获取小时价格或推进到本轮时间。参考改变或映射状态文件丢失会重新完整写入和核验映射。

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

小时 `history_seed` 首次先完成整批写入及全部 18 字段读回，再将原始响应、身份、请求范围、取回时间和 SHA 保存到 `/state/hour-seed-sources/`，成功后才保存种子状态并删除 pending。归档失败仍保留 pending，重启从同一原始窗口恢复；成功后不重复取得 730 天数据，源空值及原始归档也保持不变。分钟采集不按此规则另存每轮原始归档。

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

## 2026-10-07 初次分钟生产验收

2026-10-07 04:02（新加坡时间）的[分钟验收记录](cross-market-intraday-verification.json)确认全部 170 个行业指数的 `1m`、`5m` 共 340 个序列完成源端可取历史的初次补灌。独立库内核对有 1,866,537 条 `minute_bar`：US 一分钟 1,073,020 条、五分钟 420,053 条；KR 一分钟 266,760 条、五分钟 106,704 条。源端追加的 `minute_quote_snapshot` 单独保存，不计入上述分钟行情条数。

该次验收的国内实际源码为 `fa832d80e3014e80d7a6d3410dae6551545384dd`，运行包 SHA256 为 `8accd1be2f9be4814b334cc6737d06544d8544a96389f998ed0c95e0d789b327`，1685 个包文件已逐个核对；两台采集容器均无代理。该阶段美国 FC 实际 ZIP 下载与上传包逐字节相同，SHA256 为 `726170f11e4b522d122d780f30e9994cab23fc97cc1c3ec8855aaf1b463b8c06`，原训练函数未变。该版本连续完成三次覆盖全部 340 个序列的实际轮次，后两次由运行中的循环自动触发，所有源窗口写入及读回行数一致；独立检查时失败项、pending 均为 0。300 秒为整轮结束后的等待时间。

真实中断并重启的四个待写窗口共 7420 个源键已独立读回，原始响应及 SHA 保留。其中 KOSPI-7 五分钟有 9 个成交量后来被源端新响应更新，实际同窗口 FC 再取的原始值均与库内新值一致；不把来源刷新或窗口差异猜成确定原因。此前错误混入分钟行情的 78 个韩国 15:00 尾点已按原始证据归为快照，原价格保留，错误分钟键已精确删除；前一分钟的最终确认标记也按实际证据纠正。初灌时两个真实 SQL 读超时窗口完整保留并在调整超时后核验重放成功。

该次验收时剩余 19 个收盘缺值均在 `^DRG` 的两个粒度中，库内相应源行存在且为空，继续在源保留期内重试，不伪造价格。其中 9 月 23 日纽约时间 12:21 的旧空点，经只读原生 FC 短窗口再取确认源数组仍明确返回空 OHLC／成交量，不猜测缺值原因。分钟源仅提供最近约 30 天／60 天，不能补到 2023 年；滚动保留期和历史请求起点的边界记录保留在每个序列状态中，不据此声称交易所全部时段均有源数据。原 268 条完整映射 JSON 与 170 个日线／快照最新源键再次核对通过，US、KR 两种分钟粒度均通过已有 SQL 查询转发接口实际查询。229 项相关行为测试及 Ruff 通过；实际生产验收另有原始响应、逐字段入库检查、独立查询与重放证据。当时尚未 Git push；后续已按用户指令合入共用 v15 分支并通过统一 CI 发布，当前版本见下文。

## 当前共享 v15 发布及小时验收

2026-10-07 08:51:16（新加坡时间），共享 `refactor/cleanup-v15-only` 分支提交 `037f306d7b3b6957e33011e40744f755151a7b49` 的 [CI 37553331701](https://github.com/WidgetA/A-share-quant-trading/actions/runs/37553331701) 成功。美国 FC 及国内两个采集器已实际部署同一提交；1068 个 runtime 文件与提交／manifest 一致，817 个云 worker 包文件及下载 ZIP 均核对通过，runtime 包 SHA256 为 `b0a6a691e11feeda238f2f71ea3830a8942018d0b00894d2c4b9742dd5dac7c9`，云 ZIP SHA256 为 `01022d680b2e5a96c86bc7699e9cda407e3e122f2050a820d49f7561f91200ae`。国内两个采集器均无代理；原训练配置和 Greptime 容器的镜像、启动时间及重启计数未变。

037 部署后实际完成两轮全部 170 个日线／快照指数和两轮全部 379 个日内序列；第二轮分别于 08:57:14、08:57:55（新加坡时间）完成。各轮 39 个小时种子均为 `previously_verified`，340 个分钟序列继续推进。原 268 条映射业务 JSON／键再次独立核对，170 个日线及 379 个日内最新源键均核对全部 18 字段；源 NULL 仍保留，成功轮次不表示源端没有缺值。

39 个韩国小时种子的首次源来自 `b07d77308f744ca36357ede6afa4761adb1c5406` 部署，真实原始响应与 Greptime 全部 113,763 个键、每键 18 字段一致：其中 `hour_bar` 113,724 条，`hour_quote_snapshot` 39 条，小时收盘原始 NULL 434 条。实际小时交易日期为 2024-10-07 至 2026-10-06；尾部 15:00 报价单独保存，不算成回测小时线。39 份原始归档及种子状态在后续 bf、bd、037 发布中整字节保持，未重复取 730 天历史。037 的 10 个相关运行文件与 bd 相同；相对初始 b07，9 个文件同字节，另一个共享 Yahoo 模块只改日线解析，小时依赖函数／类保持相同。

`^DRG` 此前的日线源曾返回真实当日时间、收盘／复权收盘，却把开高低全部发布为 0。解析只在 `daily_history` 的最后一点、该点位于当前会话开始、最新报价同当地日期且不早于该点、volume 为 0、close 与 `regularMarketPrice` 的 float32 值相同这些证据同时成立时，将占位开高低转为 NULL，保留真实 close、adjusted_close、volume 和源时间。该点仍为 `daily_bar`，不把其他零 OHLC 当作历史价格，也不单凭收盘后的抓取时间标为最终确认。此前验收的真实源与库中 17 个非取回时间字段已独立一致；不同真实请求的 `fetched_at` 分别保存，不冒称相同。

随后在纽约午夜之后，2026-10-07 12:03:03（新加坡时间）的新日线轮次完成全部 170 个指数，失败为 0；`^DRG` 生产源于 12:01:44 取回，12:05 的独立原生 RAW 证实会话开始时间已滚动，供应商已补齐真实开高低并将收盘修订 `0.0009765625`，新源 17 个非取回时间字段与 SQL 一致。该次使用正常完整 OHLC 日线解析，不将旧零 OHL 处理规则说成午夜后的成功原因，也不据此承诺未来可用性。

本次发布的[可提交验收摘要](cross-market-v15-production-verification.json)保存 CI 成功时间、两组实际新轮次、运行包及源码 SHA、独立入库读回、小时种子保留和 Massive 完整窗口回灌事实；原始大凭据以本机保留的相对路径及 SHA 索引，不进入该摘要。FC 实例并发的云端读回为 null，不能将请求值 1 写成已证实配置。初次源版本、后续运行版本和每次真实轮次分别保留，不把旧轮次改记为新版本成功，也不把验收时点写成实时状态。本次 124 个 Massive 同原指数的回灌已完成，实际来源日期及未补范围如下。

## Massive 历史五分钟回灌完成

2026-10-07 14:52（新加坡时间，记录精度到分钟），真实 run5 退出 0；14:56 的严格终审确认全部 124 个已核实同原 `price_return` 指数的 1,984 个请求窗口完成写入和全部 18 字段读回，共 8,854,906 条五分钟行情。当前 todo、pending、DELAYED 待补及失败均为 0，最终窗口回执全部为 `OK`；历史 DELAYED 原始响应及失败诊断仍保留。124 个 Massive 数据库聚合各自与已验证源回执一致，总计 8,854,906 条，数据库错误为 0；另有独立原始数值与实际 SQL 的 18 个源键、每键 18 字段核对，差异为 0。

请求从 2023-01-01 开始，实际首日和本次末日如下；完成指每个请求窗口的真实源响应和写入读回，不表示补齐交易所每个日历分钟。

| 来源与粒度 | 同原指数数量 | 实际首个来源日期 | 本次最后来源日期 |
| --- | ---: | --- | --- |
| Massive，美国五分钟 | 123 | 2023-02-15 | 2026-10-06 |
| Massive，美国 Railroad Equipment（原 `^NQUSB50206025`）五分钟 | 1 | 2025-03-24 | 2026-10-06 |
| Yahoo，韩国小时种子 | 39 | 2024-10-07 | 2026-10-06 |

`^NQUSB50206025` 的 2023、2024 年共 8 个季度请求真实返回空数组，回执保留；不能据此推断指数当时不存在。韩国仍使用前述 113,724 条真实小时线及单独 39 条尾部报价，434 个原始空收盘点保留。Yahoo 最新分钟继续自动累积，不能将 Massive 五分钟历史称为一分钟历史，也不能用其他指数补这些源端没有的数据。

原美国 131 个指数中，另 6 个原指数（`^DJUSAL`、`^DJUSCF`、`^DJUSDB`、`^DJUSHD`、`^DJUSNF`、`^DWCFRP`）的候选 Massive 代码返回 403，且其同原价格收益口径尚未证实，本次未采用或回灌；`^DRG` 未在已保存目录中找到同原指数。韩国 39 个原指数同样未在该 Massive 目录中找到对应项，保留已验证的 Yahoo 来源；不把目录未匹配说成供应商永久不支持。逐项身份、实际覆盖及原因见[完整覆盖表](cross-market-index-coverage.html)和[机器可读覆盖记录](cross-market-index-coverage.json)。

实际回灌链为 run4 加载 `b07d77308f744ca36357ede6afa4761adb1c5406`，run5 加载 `037f306d7b3b6957e33011e40744f755151a7b49`；Massive 客户端、回灌器、CLI 和共享 store 四个核心文件在两提交中的 Git 字节相同，并与 run5 实际加载 SHA 一致。run5 沿用已有状态和原始回执恢复、完成剩余窗口，不把早先源响应或其 `fetched_at` 改称 037 新取。最终终审、实际运行元数据及独立源／SQL 核对的 SHA 保存于验收摘要。

例如可直接查询两个真实起点的原指数数据：

```sql
SELECT ts, "open", high, low, "close", volume, fetched_at
FROM cross_market_index_prices
WHERE provider = 'massive' AND market = 'US' AND symbol = '^SOX'
  AND "interval" = '5m' AND data_kind = 'minute_bar'
  AND ts >= '2023-02-15T00:00:00Z' AND ts < '2023-02-16T00:00:00Z'
ORDER BY ts;

SELECT ts, "open", high, low, "close", volume, fetched_at
FROM cross_market_index_prices
WHERE provider = 'massive' AND market = 'US' AND symbol = '^NQUSB50206025'
  AND "interval" = '5m' AND data_kind = 'minute_bar'
  AND ts >= '2025-03-24T00:00:00Z' AND ts < '2025-03-25T00:00:00Z'
ORDER BY ts;
```

查询真实分钟行情时显式限定粒度和数据类型，例如：

```sql
SELECT ts, open, high, low, close, volume, is_final, fetched_at
FROM cross_market_index_prices
WHERE provider = 'yahoo' AND market = 'KR' AND symbol = 'KOSPI-10.KS'
  AND "interval" = '1m' AND data_kind = 'minute_bar'
  AND ts >= '2026-10-06T00:00:00Z' AND ts < '2026-10-07T00:00:00Z'
ORDER BY ts;
```


## 已启用 Basic 账户的七个美股历史缺口

2026-10-07 15:45–15:51（新加坡时间）使用已确认启用的同一免费 Indices Basic 账户重新请求。六个 Dow 候选原指数分别请求 2023-03-06 至 03-07、2024-06-03 至 06-04、2026-10-01 至 10-02，共 18 个窗口均实际返回 HTTP403 `NOT_AUTHORIZED`；六个候选的一次组合快照请求也为 403。同账号、同接口与相同 2023 窗口的原 `I:SOX` 返回 `OK` 和 158 条真实五分钟线，因此不能把这批拒绝归因为整个 Key 未启用、所有 2023 分钟不可读、限流或日期空数据。

六个精确候选代码仍在 Massive 官方 Basic 清单中，公开清单与当前账号实际权限矛盾的原因尚未核实。当前证据只证明账号拒绝这些请求，不能写成“这些指数必定付费”或“升级就一定能解决”。没有购买、自动升级或更换原指数。全部 24 次请求沿用唯一持久队列，实际最短间隔至少 13 秒，任意 60 秒最多 5 次，未收到 429。昨天的 18 个 403 原证据另存；其激活前后时序未被证实，不用它冒充本次启用后的请求。

原 `^DRG` 再按 `NYSE Arca Pharmaceutical` 与 `Arca Pharmaceutical Index` 两种完整名称分别查询 active/inactive，4 次 HTTP200 均无结果，仍不把未匹配说成供应商永久不支持。身份来源改正为真实 HTTP200 的 ICE 原 DRG 资料：市值加权，另有分红再投资的全收益版本。旧表误引的等权 DGE 文档已从 DRG 身份证据中移除。新方法文件的搜索缓存片段没有冒充已下载全文。FirstRate 的免费样本实际只有 2026-09-21 至 2026-10-06 的分钟线，没有 2023；付费长史未购买或下载，ICE 历史接口所需认证账户和授权也未取得。

现有 124 个原美股指数的 8,854,906 条历史与 39 个韩国指数的小时种子保持前述已入库事实，未来 Yahoo 日线及分钟采集沿用已验证的生产 FC 和共享 v15 发布流程。完整 131 个原美股指数的历史目标尚未达成：这 7 个原指数历史分钟仍缺，不能用本轮覆盖核验结束代替全部历史已经灌入。表中的历史 SQL 验收时间保留原值，本轮权限请求没有冒称重新读取了生产数据库。


DRG 的正式基础引用及复制说明也已纠正，`intraday_indices.json` 和 `massive_indices.json` 同步绑定新基础文件的实际 LF 字节 SHA。170 个原指数、379 个日内身份及 124 个 Massive 价格身份和状态路径均与原提交一致，现有 124 个完成游标和 39 个小时种子无需重置。这项正式参考数据变更将走共享 v15 源发布，区别于前次文档单发；新部署及新轮次的实际验收另录。已证历史窗口、源请求和 SQL 的原版本及时间保留，不能用新索引文件 SHA 冒称旧源批次是新取。


## DRG 正式来源纠正后的生产验收

当前采集基础设施实际版本为 `a2c89213b526278847a2e6d362fcea75bc1b4896`。[CI37592996598](https://github.com/WidgetA/A-share-quant-trading/actions/runs/37592996598)于2026-10-07 16:28:13（新加坡时间）完成成功，跨市场基础设施发布成功，训练发布跳过。国内两采集器和美国FC版本标记均为a2，1068个实际runtime文件、817个云worker文件及云ZIP与对应manifest逐项一致；runtime包摘要为`113d412aaa4343dfadb859484e7d84b5538188b0db5805696aeb7bcd17330a7c`。云worker源码未改变，ZIP仍为`01022d680b2e5a96c86bc7699e9cda407e3e122f2050a820d49f7561f91200ae`，云端实际版本标记已更新。国内仍无代理，训练完整配置和数据库容器基线未变。

新容器16:24:25启动后，真正的170日线完整轮次于16:27:18、16:34:14完成，379日内完整轮次于16:27:32、16:34:44完成，均无失败且pending为0。340分钟序列到达两个真实新目标，39小时种子两轮均跳过，无新小时源行或重放；39份小时归档和39份种子状态仍逐字节等于16:17发布前基线。268条新映射全部键和业务JSON读回与纠正后的基础索引一致，170个最新日线源键的18列和元数据检查通过。没有把037旧轮次算成a2新轮次。

Root另于16:27:49至16:28:08经原生Greptime完成两条只读SQL：124个Massive历史分组的行数和实际日期再次与已保存源证据一致，合计8,854,906条；18个先前已由原始响应核实的源键全部18字段、原OHLC及原fetched_at仍完全一致。该查询是新的实际数据库读回，原源响应和其采集版本、时钟仍保留，不冒称重新获取价格源。

随后按官方迁移说明，在同一13秒持久队列经旧`api.polygon.io`对照请求一次原DJUSAL2023窗口和一次SOX2023窗口。旧域名当前SOX同样实际返回158条，铝业仍403，未找到切换旧域名即可解决其权限的证据；没有将另外5个未单独测试的旧域名请求写成已拒绝。本轮原24次加这2次对照共26次，仍未新增7个缺口指数历史。123项真实从2023-02-15起，铁路设备仅从2025-03-24起；7项没有采用的Massive历史和铁路设备2023–2024空窗分别保留，完整131项从2023起的目标仍未完成。

两个本轮临时Python环境已无进程使用，原项目现有Python用于本轮请求、检查和生产读证。删除`tools-env`及`v15-ci-env`均被自动工具政策拒绝，目录仍在；返回只有`blocked by policy`，没有具体原因。未绕过该拒绝，未删除原项目环境、临时Key或历史源回执。
