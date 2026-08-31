# V20 部署与运维手册

本手册只说明 V20 的部署、观察和回滚。策略语义以
[冻结规则](./strategy-v20.md)为准；历史结果和限制以
[证据附录](./strategy-v20-evidence.md)为准。

## 1. 边界与默认状态

V20 是“策略决定 + 飞书推送”服务，不是交易执行系统。它输出当天是否建立
模型批次、相对标准批次的投入倍率、V16 完整推荐列表，以及 D1/D2 退出提醒。
它不读取账户可用资金，不计算股数，不调用券商下单/撤单接口，也不把人工成交
写回策略状态。推送成功只代表决定已送达，不代表成交。

仓库默认配置为：

```text
enabled: false
deployment_mode: forward_shadow
production_activation_guard: false
bootstrap.mode: EMPTY_FORWARD_SHADOW
```

上述值是 YAML 与专用进程的安全默认，不直接开启 `production_push`。现行 main 镜像在
没有任何显式 V20 激活变量时，会自动创建内嵌 `forward_shadow` profile：复用线上 V16
已有的 `DB_*`、Tushare token 和 `FEISHU_*`，但 V20 账本、outbox、连接池和 leader 仍
隔离在 `v20` schema。设置 `V20_EMBEDDED_ENABLED=false` 可以明确关闭它。

仓库 push CI 只发布正常 `runtime` 的 `latest`/提交 SHA tag；现有 main 主机按原 V16
机制自动更新该镜像，因此无需第二个 V20 容器。CI 变绿仍不等于运行验收：必须检查线上
`/api/status` 的安全 V20 启动字段并调用人工触发。显式 `production_push` 仍使用专用
`v20` Docker target/`scripts/v20_main.py` 和后文全部严格配置，但不由默认 CI 自动部署。

运行模式与旧 V16 扫描通知的关系：

| V20 设置 | V20 行为 | 旧 V16 扫描通知 |
|---|---|---|
| main 未设置显式 V20 变量（默认） | 内嵌影子账本；复用现有飞书 | 同时启动 |
| `V20_EMBEDDED_ENABLED=false` | 不启动 | 启动 |
| `V20_ENABLED=true`, `V20_MODE=forward_shadow` | 影子账本及影子飞书 | 同时启动 |
| legacy `runtime` + `V20_MODE=production_push` | 拒绝承载正式 V20 | 一律禁用，进程 fail closed |
| Docker `v20` target + `production_push` | V20 正式账本及正式飞书 | 不启动旧 V16 通知、iQuant 或交易接口；内部仍复用 V16 选股算法 |

最后一行是 fail-closed 设计：正式模式故障时不能静默改由另一套策略推票。

## 2. 信息时钟

所有策略时间使用 `Asia/Shanghai`，行情原始分钟时间是分钟结束标签：

1. 09:15 预热 V16 模型、股票池和历史输入；单次预热调用最多占用 60 秒并可重试，
   且至少在 09:40 前预留 2 秒把控制权交回截止处理，不能由挂死的历史请求越过
   每日“不买”终态。09:31 起采集分钟行情。
2. 先以交易所日历冻结 D0 前最近 37 个交易日。每只进入 V16 打分的代码必须逐日
   精确覆盖这 37 日的真实合法 OHLCV；缺任一日即排除该票。对于已经进入冻结 V16
   股票池的代码，停牌、新股和来源缺失都不得补零、前向填充、用更老的第 38 日替代，
   也不得再从分母删除。历史合格票数以冻结 V16 股票池总数为固定分母，必须达到
   80%；单票不合格不直接否决全日，整体低于 80% 才 fail closed。
3. 每只进入 V16 扫描的代码还必须具有原始 `09:31..09:39` 连续路径及合法昨收，
   最终实际扫描集合对同一个冻结 V16 股票池的覆盖也必须达到 80%；低于阈值不得
   计算，不允许用 09:38 或“最后一条可得数据”回退。宽度样本独立采集，不得混入
   历史或最终扫描的 80% 分母。
4. 快照必须保存 `history_profile_id=STRICT_LAST_37_EXCHANGE_SESSIONS_V1`、逐票
   `history_input_hashes`、37 日各自的 `history_date_valid_counts`、
   `history_min_date_coverage`，以及 `scan_input_codes`、
   `scan_input_failure_codes` 和 `scan_input_coverage`。缺失代码、实际历史合格集合和
   实际扫描集合都必须能从快照复核。
5. 正常决定事务的 PostgreSQL terminal guard 必须严格早于 09:40:00。terminal guard
   不是对其后真实 COMMIT 可见时点的虚假声明；核心提交后 outbox 密封会再取数据库
   时钟，达到或晚于 09:40 的可行动买入文案只能替换成“已过期、不要追买”；本来就是
   `BLOCK/NO_SIGNAL/INPUT_INVALID` 的零倍率终态可保留完整不买原因和故障详情；
6. 09:40 到达时如果仍没有 durable 正常决定，立即以数据库时钟门控提交并密封
   `INPUT_INVALID` 的“不买”终态；达到或晚于 09:40 才形成或被 terminal guard 拒绝的
   结果只记评价/故障，不再建议追买。09:45 只保留异常恢复/漏槽兜底，不是常规等待点，
   也不延长买入窗口。
7. 日历 09:40 开盘参考价取原始结束标签 `09:41` bar 的 `open`。它在决定发布
   后锁入模型腿，用于后续理论收益和退出阈值，不参与同日 09:40 前的决定。

生产排障时必须同时检查源事件标签、服务端首次持久接收时间、事务 terminal guard
和 post-commit outbox 密封时间，不能用日志打印时间或把 guard 冒充 COMMIT 时间。

09:41 参考价先过滤合法性再选修订：模型腿截止为 D1 09:30，HEALTH/ROLLING7
全市场影子样本截止为 D0 09:45。某一分钟行情缺失或非法只记缺口，不能屏蔽后续
独立合法分钟的止损；D1 14:57 bar 触发时，建议从 D2 09:31 起执行。

## 3. 镜像和不可变制品

Docker 镜像必须包含：

```text
/app/config/v20.yaml
/app/pyproject.toml
/app/uv.lock
/app/docs/strategy-v20-artifacts/manifest-v1.json
/app/docs/strategy-v20-artifacts/*
/app/migrations/v20/001_v20.sql
```

V20 启动时会校验 `config/v20.yaml` 中的 manifest SHA-256。缺文件、文件内容被
改动或 hash 不匹配都会拒绝启动。构建后可做只读检查：

```bash
docker run --rm --entrypoint sh IMAGE_TAG -c \
  'test -f /app/config/v20.yaml && test -f /app/pyproject.toml && test -f /app/uv.lock && test -f /app/scripts/export_v20_checkpoint.py && test -f /app/docs/strategy-v20-artifacts/manifest-v1.json && test -f /app/migrations/v20/001_v20.sql && test -f /app/bundled_data/sectors.json && test -f /app/bundled_data/board_constituents.json'
```

构建使用固定 digest 的 Python/uv 基础镜像以及 `uv sync --frozen`；锁文件不一致必须让镜像构建失败，不能在构建时临时
重新解析依赖。`.dockerignore` 必须排除 `.env`、`config/*.env`、`config/secrets*.yaml`、私钥和本地
缓存；accepted checkpoint 是唯一允许进入 `config/v20-checkpoints/` 的部署输入。
不要在容器启动时下载、覆盖或动态生成冻结制品。

## 4. 环境变量和密钥

变量模板见 [`config/v20.env.example`](../config/v20.env.example)。模板只有占位符，
真实值应由部署平台的 secret manager 注入，不得提交到 Git 或写入镜像层。

### 4.1 PostgreSQL

生产必须为 V20 使用专用 writer：

```text
V20_DB_HOST
V20_DB_PORT
V20_DB_NAME
V20_DB_USER
V20_DB_PASSWORD
V20_DB_SSLMODE=verify-full
V20_DB_SSLROOTCERT
V20_DB_SSLROOTCERT_SHA256
```

该账号只服务 `v20` schema 的策略决定账本和 durable outbox，不应获得券商、
账户、订单、持仓、成交表或其他业务 schema 的权限。可以使用独立数据库，也可
使用同库隔离 schema；无论哪种方式，都要独立账号、独立密码和最小权限。

上段是专用 profile 的要求。main 内嵌 profile 有意复用已经承担 `trading` 写入的
`DB_*` 身份和 fundamentals 的 TLS/超时设置，同时把 schema 固定为 `v20`、连接池固定
为 1/8；它不会把表写进 `trading` 或 `public`。TLS 最低为 `require`，若现行 DB 配置为
`verify-full` 则仍校验 CA。若该线上身份没有创建 `v20` schema 的权限，启动会明确失败，
V16 保持运行，公开 `/api/status` 只显示 `start_error_type` 而不泄露连接异常正文。

正式 V20 必须使用独立进程 `scripts/v20_main.py`。该进程只创建 V20 service 和四个
`/api/v20` 接口，不创建平台 state manager、PositionManager、订单/持仓接口或 iQuant
router。V20 初始化自身仍会读取 fundamentals 并创建 Tushare 客户端，因此除专用
writer 外还需要为 fundamentals 提供最小只读 `DB_HOST/PORT/NAME/USER/PASSWORD`、
`DB_SSLMODE=verify-full`、`DB_SSLROOTCERT`、`DB_SSLROOTCERT_SHA256` 和
`TUSHARE_TOKEN`。两路数据库账号必须不同。CA 文件通过 secret mount 注入，不得写入
镜像；其小写 SHA-256 必须同时写入环境变量和 `config/v20.yaml` 的受审字段，文件内容、
环境摘要、`v20-runtime/v2` 配置摘要三者任何一个不一致都会在连接前失败。不需要、也
不应向正式 V20 容器注入券商或 iQuant 凭据。共享 FundamentalsDB 的 legacy runtime
仍兼容原 `require` 配置，但启用 V20 一律拒绝它，必须验证证书和主机名。
启动门禁会把 `config/database-config.yaml` 解析后的 writer/fundamentals 实际连接参数
逐字段与上述显式环境变量核对（密码只做常量时间比较且不记录），该 YAML 的文件摘要也
进入 `config_hash`；任一 literal、别名环境变量或超时值造成偏离，都会在创建连接池前
失败。V20 的实时、历史日线和 ST/公司名查询三条 Tushare 路径只消费同一个显式
`TUSHARE_TOKEN`，不会使用 legacy 的本地 token 文件回退。

交易日历同样通过该 Tushare 客户端的 `trade_cal` 接口取得；生产网络必须放行
`https://api.tushare.pro`。V20 不依赖 legacy AkShare/Sina 日历缓存。日历请求有 15 秒
上限并每日刷新；只有仍能证明历史前驱及至少两个未来交易日的进程内缓存才可在刷新
失败时继续使用，否则调度 fail closed。跨年发布前必须在 status 核对未来 horizon。

V20 service 取得数据库 leader 后会启动五个相互独立的长期 `asyncio` 任务：

1. `v20-decision-scheduler`：日历、预热、成熟事实、入场和参考价状态推进；
2. `v20-live-exit-scheduler`：当天 D1/D2 模型腿的实时保护；
3. `v20-stale-exit-scheduler`：有界恢复已经过期的退出积压；
4. `v20-outbox-recovery-scheduler`：独立密封已提交但尚未密封的 outbox 事件；
5. `v20-outbox-publisher`：按固定 route/stream/lineage 租赁并投递已密封事件。

这五项是同一专用进程内的独立任务，不是五个进程。慢预热、历史退出积压、旧 outbox
密封或飞书投递不得排在当天止损及 09:40 入场之前；任一长期任务意外退出都会令整个
runtime fail closed、停止其他任务，不能静默降级成只剩部分功能。
decision scheduler 内置基于事件循环单调时钟的 09:40 截止看门狗：它会取消仍在执行的
预热、扫描或账本对账，重新采样本机时钟，再由 PostgreSQL 时钟门控提交
`INPUT_INVALID`。若有序账本的前序状态暂时使该槽无法提交，则先以稳定事件 ID 持久化
`ENTRY_CUTOFF_NO_BUY`，明确“今天不买”，后续调度继续幂等重试并正常补槽。若首次
交易日历请求也阻塞跨点，周一至周五先持久化 `ENTRY_CALENDAR_UNKNOWN_NO_BUY`；已由
成功加载的交易日历确认休市则不报。

启动任务前，repository 必须持有 PostgreSQL session advisory lock。锁键只由公开
`route_id` 决定，因此即使 stream 或 lineage 不同，同一个正式/影子公开 route 也只能
有一个 V20 runtime；stream/lineage 仍是账本和 outbox 的不可变作用域，但不能借换
lineage 绕过公开副作用单例。leader 连接会在进程整个生命周期独占一个池连接，
decision、live exit、stale exit、outbox recovery 和 publisher 均持续探测该 session；锁连接丢失
时服务失败关闭。

数据库池配置必须满足 `1 <= pool_min_size <= pool_max_size` 且
`pool_max_size >= 7`，并与 `config/v20.yaml` 和 `config/database-config.yaml` 完全一致。
当前两处 `pool_max_size` 都是 `8`；不得因平时空闲连接较多而降到 7 以下，因为 leader
常驻连接与并发决定、退出、密封、后台状态采样和 publisher 租约需要独立余量。配置不一致或
上限不足会在启动前被拒绝。

Docker 有两个明确目标：默认 `runtime` 保持原平台/V16 的 `scripts/main.py` 入口；
`v20` 目标使用独立入口。构建和启动正式 V20 示例：

```bash
docker build --target v20 -t a-share-v20:REVIEWED_DIGEST .
docker run --rm --env-file /run/secrets/v20.env -p 8000:8000 a-share-v20:REVIEWED_DIGEST
```

上例中的两个 `SSLROOTCERT` 路径必须由部署平台以只读 secret mount 放入容器；env 文件
只记录路径和受审 SHA-256，不能把 CA 或其他秘密烘焙进镜像。

不要通过覆盖默认平台镜像的环境变量来承载正式 V20；平台入口会拒绝启用
`production_push`。影子观察若仍需与旧平台同进程运行，可继续使用默认 `runtime`，
但正式切换前必须改用 `v20` 目标。

当前 repository 在连接后会幂等执行与
[`migrations/v20/001_v20.sql`](../migrations/v20/001_v20.sql)等价的 DDL。因此可选：

- 让 DBA 预建由 `v20_writer` 持有的 `v20` schema，再由 writer 执行迁移；或
- 允许该账号首次启动时创建 `v20` schema、sequence、table 和 index。

共享 schema 升级 outbox 作用域列时，先停止所有 shadow/formal V20 worker，再执行
迁移和下方核验，最后只启动新版本；禁止旧二进制与新二进制滚动混跑。旧二进制
不会写新作用域列，而新 worker 已按完整作用域租约，两者并存会造成写入失败或旧
worker 绕过新隔离边界。

如果采用预迁移，必须以最终 owner/writer 身份执行脚本（或由 DBA 同步转移 schema、
sequence、table 的所有权并授予所需权限），示例命令为：

```bash
PGPASSWORD="$V20_DB_PASSWORD" psql \
  -h "$V20_DB_HOST" -p "$V20_DB_PORT" \
  -U "$V20_DB_USER" -d "$V20_DB_NAME" \
  -v ON_ERROR_STOP=1 -f migrations/v20/001_v20.sql
```

随后用 V20 writer 连接执行只读检查：

```sql
SELECT to_regclass('v20.runtime_configs') AS runtime_configs,
       to_regclass('v20.official_state') AS official_state,
       to_regclass('v20.outbox_events') AS outbox_events;
```

三列都必须非空。不要通过清表、删 schema 或改历史行来“重新初始化”。

### 4.2 飞书路由

影子与正式通知必须使用隔离路由：

| 路由 | 环境变量前缀 | 用途 |
|---|---|---|
| `V20_SHADOW_FEISHU` | `V20_SHADOW_FEISHU_` | `forward_shadow` |
| `V20_FORMAL_FEISHU` | `V20_FEISHU_` | `production_push` |

每个前缀提供 `BOT_URL`、`APP_ID`、`APP_SECRET`、`CHAT_ID`。正式路由的实际变量
名是 `V20_FEISHU_*`，不是 `V20_FORMAL_FEISHU_*`。两个模式不能共用 chat 或
APP 凭据，以防影子事件进入正式决策群；启动时会拒绝重复的 chat、APP ID 或 APP
secret。V20 专用变量缺失时不会回退到旧的
`FEISHU_BOT_URL/APP_ID/APP_SECRET/CHAT_ID`。当前启用模式所选中的路由必须显式
提供其前缀下的 `BOT_URL` 和三项凭据，否则启动失败；另一条未启用路由可以暂不配置，
但切换到该模式前也必须独立配置并通过隔离验收。

`config/v20.yaml` 的每条 route 是 `v20-runtime/v2` 受审 binding 对象，包含固定
`route_id`、严格 HTTPS origin、APP ID SHA-256 和 chat ID SHA-256。默认值全部是
`UNCONFIGURED`，且只能在 `V20_ENABLED=false` 时加载；启用前必须填入当前模式的受审
值。APP secret 只从 secret manager 注入且不得写入配置或配置 hash。运行环境的实际
origin/APP/chat 与受审 binding 不同会在数据库连接前失败；即使 shadow/formal 分别
部署，也不能绕过各自的目的地绑定。

relay 必须实现专用 `POST /api/v20/send`，请求 schema 固定为
`v20-relay-request/v1`，携带 event/route/idempotency key/payload hash、目的地指纹、
`delivery_class`、`action_expiry_ts`、原消息和过期消息。只有 `ENTER + 正倍率` 属于
`ACTIONABLE_ENTRY`；BLOCK/NO_SIGNAL/INPUT_INVALID 属于 `NON_ACTIONABLE_ENTRY`，即使
09:40 后也必须保留完整诊断。relay 以 `route_id:event_id` 原子去重，同一 key 不同
payload hash 必须冲突；它必须在真实飞书 API 接受后才返回
`v20-relay-response/v1`，严格回显 event/route/key/hash/目的地、布尔 duplicate、带时区
accepted_at 和合法 delivery status。客户端只有在所有字段和时效关系精确成立时才把
outbox 标为 SENT（布尔 `false` 不能冒充整数 code 0）。

对于 `ACTIONABLE_ENTRY`，relay 必须在实际调用飞书之前按 relay 服务器时钟再次比较
09:40：截止前发送原文，截止后只发送“已过期、今天不要追买”。客户端 HTTP 超时/取消
只是延迟保护，不是安全边界。非入场消息单次 HTTP 最长 2 秒；超时、未知响应或错误
回显均由 durable outbox 重试。在 relay 完成上述版本化、去重、过期和严格回显合同的
端到端验收前，不得打开生产三重门。

### 4.3 HTTP API 密钥

`V20_INGEST_API_KEY` 只保护 MEWS 快照和停止提醒确认两个证据写接口，调用方通过
`X-V20-API-Key` 发送。`V20_STATUS_API_KEY` 只保护状态读接口，通过
`X-V20-Status-Key` 发送。两者必须显式配置、至少 32 字符且彼此不同，不能跨接口
授权，也不应复用飞书或数据库密码；任一未配置返回 503，错误密钥返回 401。

`POST /api/v20/trigger-scan` 当前有意不做应用层鉴权，不配置或发送 API key；
默认调用也不需要 `Idempotency-Key`。未提供该 header 时服务端自动生成
`manual-<uuid>`；调用方只有在需要为超时或未知结果去重重试时，才自行提供稳定的
`Idempotency-Key`。取消触发接口的 key 不会取消 V20 服务 health、PostgreSQL leader、
决策时点、串行并发和 durable outbox 门禁，也不会允许请求 body 覆盖时钟、交易日或
正式决策。

## 5. HTTP 运维与验收接口

四个接口分别用于状态、证据、停止提醒确认和部署验收；没有账户或订单 API。

### 5.1 状态/健康

```bash
curl --fail-with-body http://127.0.0.1:8000/api/v20/status \
  -H "X-V20-Status-Key: $V20_STATUS_API_KEY"
```

状态端点使用独立密钥，且只读取由 startup/outbox-recovery lane 有界刷新的内存快照，
HTTP 请求本身不会占用 PostgreSQL 连接。快照缺失、刷新失败或超过 freshness 上限时
`healthy=false`。网络层仍应由内网、反向代理或服务网格限制访问，不应直接暴露到公网。

部署探针应同时判断 HTTP 成功和返回 JSON 中的启用模式、服务启动状态、完整
`config_hash`、`route_id`、`official_stream_id`、`state_lineage_id`、日历首末日期、
账本、outbox 及 `runtime_lanes`；不要仅以进程存活作为 V20 健康。正式模式配置或启动
失败时，旧 V16 不会自动接管，应立即按第 8 节处理。

`runtime_lanes` 当前逐项报告 `decision`、`live_exit`、`stale_exit`、
`outbox_recovery`、`publisher` 的
`healthy`、`last_success_at`、`success_age_seconds`、`freshness_limit_seconds`、
`last_error` 和 `last_error_at`。当前 freshness 判定为：

| lane | freshness 上限 |
|---|---:|
| `decision` | 90 秒 |
| `live_exit` | `2 × exit_poll_seconds + 2`，当前 32 秒 |
| `stale_exit` | 65 秒 |
| `outbox_recovery` | 5 秒 |
| `publisher` | 7 秒 |

任一上述 lane 未曾成功、超出 freshness 或保留错误时，启用状态下总 `healthy` 都必须为
`false`。publisher 意外停止会令 `running=false` 并终止其余任务；relay 返回失败、超时
或租约异常会保留 publisher lane 错误。`outbox.delivery_error_n > 0` 也会直接令 publisher
及总健康为红，避免指数退避期间一次空租约把故障暂时洗绿。因此探针必须同时要求
`running=true`、`healthy=true`、五条 lane 新鲜且无错，并检查 outbox 未出现持续增长的
未密封、租约重试或投递失败。

### 5.2 写入 MEWS 快照

```bash
curl --fail-with-body -X POST http://127.0.0.1:8000/api/v20/mews-snapshots \
  -H "Content-Type: application/json" \
  -H "X-V20-API-Key: $V20_INGEST_API_KEY" \
  -d '{
    "snapshot_id":"mews-example-2026-08-28",
    "source_trade_date":"2026-08-28",
    "generated_at":"2026-08-28T18:00:00+08:00",
    "fast_state":"NORMAL",
    "model_version":"example-model-version",
    "data_version":"example-data-version"
  }'
```

`generated_at` 必须带时区。V20 会以服务端接收/持久化时间执行 PIT 截止判断；调用
方不能通过回填 payload 时间让迟到快照变成当时可得。

### 5.3 停止某个退出事件的后续提醒

```bash
curl --fail-with-body -X POST http://127.0.0.1:8000/api/v20/reminder-stop-acks \
  -H "Content-Type: application/json" \
  -H "X-V20-API-Key: $V20_INGEST_API_KEY" \
  -d '{
    "ack_id":"ack-example-001",
    "original_exit_event_id":"EXIT_EVENT_ID_FROM_FEISHU",
    "consumer_id":"operator-name-or-system-id",
    "ack_ts":"2026-08-31T10:00:00+08:00"
  }'
```

ack 只停止该退出事件未来的提醒，不代表订单已提交或成交，也不修改退出决定。

### 5.4 人工触发一次部署验收

人工接口路径为 `POST /api/v20/trigger-scan`。默认调用不携带任何 header，服务端会生成
`manual-<uuid>` 作为 `manual_request_id`。每次无 header 调用都会生成新的 ID，并创建
一条新的非交易回执。

`Idempotency-Key` 是可选 header。需要在 curl 超时、连接中断或结果未知时安全重试，
可在首次调用前生成 `deploy-<git-sha>` 或部署系统的不可重复 run ID；调用方提供的 key
必须为 8–128 字符，首字符是字母或数字，其余只允许字母、数字、`.`、`_`、`:`、`-`。

main 内嵌部署先读取无需密钥的安全启动摘要，确认提交 SHA 与 `v20.started=true`：

```bash
curl --fail-with-body http://127.0.0.1:8000/api/status
```

若部署了 `V20_STATUS_API_KEY`，或正在验收专用 profile，再读取完整 status：

```bash
curl --fail-with-body http://127.0.0.1:8000/api/v20/status \
  -H "X-V20-Status-Key: $V20_STATUS_API_KEY"
```

完整 status 可用时，应同时确认：

- `enabled=true`、`running=true`、`healthy=true`；
- `mode` 是部署单预期的 `forward_shadow` 或 `production_push`；
- `strategy_version`、完整 `config_hash`、`route_id`、`official_stream_id`、
  `state_lineage_id` 与部署单完全一致；
- 五条 `runtime_lanes` 新鲜且无错误，outbox 没有异常积压；
- 当前运行的是预期进程/镜像。CI 通过不能替代本检查。

内嵌 profile 没有配置 status key 时，详细 status 保持 503 是预期鉴权行为，不会阻止
无 key 人工触发；人工触发自身仍会检查 repository leader 和五条 runtime lane，尚未就绪
会返回 503，而不是绕过健康门禁。

触发命令：

```bash
curl --fail-with-body -X POST http://127.0.0.1:8000/api/v20/trigger-scan
```

若本次验收需要支持未知结果去重重试，可显式提供可选 header：

```bash
V20_TRIGGER_REQUEST_ID="deploy-REVIEWED_GIT_SHA"
curl --fail-with-body -X POST http://127.0.0.1:8000/api/v20/trigger-scan \
  -H "Idempotency-Key: $V20_TRIGGER_REQUEST_ID"
```

接口不接受请求 body、调用方时间、交易日或 `force` 参数。09:15 至 09:40 前且当日尚无
终态时，它至多加速一次与自动 scheduler 相同的串行 decision cycle；所有交易日历、
原始 09:39、覆盖率、leader、数据库时钟和 09:40 门禁仍然有效。09:40 起接口对正式
策略状态只读：只能返回已经持久化的 exact-09:39 正式结果；若没有结果，回执明确写
“不可用”，绝不会拿晚到行情补算、替换或新建正式决定。

无论当时是否存在正式决定，人工接口都会把一条
`DATA_ALERT/OPERATOR_NOTIFICATION` 回执写入并密封到 durable outbox。飞书标题明确标记
“人工触发验证（非交易指令）”；该回执不会创建或修改订单、持仓、卖出信号或券商侧
状态。若触发发生在 09:40 前并合法推进了正常 decision cycle，由该 cycle 产生的正式
入场决定仍是独立的正常 V20 事件，不能与人工验收回执混为一条消息。

典型首次响应为：

```json
{
  "accepted": true,
  "created": true,
  "manual_request_id": "manual-550e8400-e29b-41d4-a716-446655440000",
  "manual_event_id": "FULL_STABLE_EVENT_ID",
  "trade_date": "2026-08-31",
  "cycle_result": "ALREADY_TERMINAL",
  "formal_decision_available": true,
  "entry_action": "ENTER",
  "entry_event_id": "FORMAL_ENTRY_EVENT_ID",
  "official_state_changed": false,
  "manual_notice_actionable": false,
  "sealed": true,
  "delivery_status": "PENDING",
  "feishu_delivery_confirmed": false
}
```

HTTP 202 只表示非交易回执已持久写入并密封、等待 outbox publisher 投递；不表示飞书
已经接受消息。`accepted=true` 也不是交易确认。只有 `delivery_status=SENT` 且
`feishu_delivery_confirmed=true` 的同请求 ID 重试响应，或数据库/目标飞书的独立核对，
才能证明最终投递。首次响应后使用完整 `manual_event_id` 查询：

```sql
SELECT event_id, event_type, route_id, official_stream_id, lineage_id,
       seal_status, delivery_status, attempt_count, last_error, delivered_at
FROM v20.outbox_events
WHERE event_id = 'FULL_STABLE_EVENT_ID';
```

验收完成必须同时满足：该行 `seal_status='SEALED'`、最终
`delivery_status='SENT'`、`last_error IS NULL`，并且受审目标飞书群收到对应“非交易
指令”回执；影子/正式另一个群不得收到它。仅看到 HTTP 202 不算通过。

若首次请求显式提供了 `Idempotency-Key`，curl 超时、连接中断或返回结果未知时必须用
原值重试，不能换 key 猜测。相同 route、stream、lineage、config 下，同 key 会返回同一
`manual_event_id`，`created=false`，不会重新运行 decision cycle，也不会创建第二条回执。
若首次请求没有 header 且结果未知，调用方无法复用服务端生成但未收到的 ID；再次无
header 调用会成为新请求并创建新的非交易回执。成功收到响应后，可以把返回的
`manual_request_id` 作为后续 `Idempotency-Key` 重试。任一人工请求仍在处理时，并发请求
（包括尚未完成首次落盘的同 key）可能返回 409；待首个请求结束后按原 key 重试即可读取
同一结果。失去 leader、任一 status 健康条件为红（含 lane 不新鲜/有错或 outbox 投递
错误）、runtime lane 停止、服务未启用或持久层不可用返回 503，均不得当作验收成功。

## 6. 前向影子部署

使用以下环境覆盖并部署：

```text
V20_ENABLED=true
V20_MODE=forward_shadow
V20_ALLOW_PRODUCTION_PUSH=false
```

推荐使用 Docker `v20` 目标或直接执行 `python scripts/v20_main.py`；独立 host 不读取
`WEB_ENABLED`。若影子仍由默认平台入口托管，则还必须保持 `WEB_ENABLED=true`，避免
出现“容器存活、V20 实际从未创建”的假健康状态。

影子阶段继续保留旧 V16 扫描通知。确认影子消息只进入影子群，并逐日保存：

- 09:31..09:39 路径覆盖、缺失代码、首次 durable receipt、terminal guard 与密封证据；
- D0 前最后 37 个交易日逐票完整历史、停牌/新股排除、固定 V16 分母 80% 覆盖，
  以及 history profile/hash/逐日覆盖审计字段；
- V16 扫描覆盖与市场宽度覆盖是两个分离集合，宽度缺口不得污染 80% 扫描门槛；
- 把同一份通过 V20 历史/分钟合法性校验的输入分别交给 V20 和线上 main V16 后，
  推荐及排序零差异；被输入门槛排除的代码另表核对，不混入算法差异；
- 09:41 模型腿 D1 09:30 截止、影子批次 D0 09:45 截止、合法性过滤和缺口；
- V16 完整推荐名单、BASE/rolling7/G 决定和状态 hash；
- D1/D2 退出、MEWS 选择、提醒/ack、outbox 重试与重复投递；
- 停牌、缺 bar、行情修订、重启、数据库/飞书短暂失败和公司行动样本。

开始影子前应冻结观察周期、允许缺口和通过标准，不能看完结果后修改门槛。完整
验收项见冻结规则第 12 节。回顾性 09:41 回放标记为
`production_bootstrap_eligible=false`，不能代替具有真实首次接收时钟的前向影子。

## 7. 正式启用

正式推送必须同时满足以下前置条件：

1. 冻结规则第 12 节的生产验收项全部关闭，前向影子已经覆盖时间边界、数据缺口、
   行情修订、重启、消息重试、退出兜底和公司行动样本；
2. 从已验收前向影子生成并审核合法 checkpoint，完整保留成熟窗口、待成熟批次、
   gap、来源映射和首次接收证据；
3. `config/v20.yaml` 使用新生产 lineage，并把已接受 checkpoint 放入镜像会复制的
   `config/v20-checkpoints/`，然后配置：

   ```yaml
   bootstrap:
     mode: CHECKPOINT
     checkpoint_path: config/v20-checkpoints/accepted-checkpoint.json
     checkpoint_sha256: exact-lowercase-sha256
   ```

4. checkpoint 文件进入部署制品，路径和 SHA-256 在候选镜像内复核通过；
5. 数据库迁移、专用 writer、正式飞书路由和两把受保护接口的 API key 均通过预检；
6. 选择下一交易日作为生效日，完成双人复核和回滚演练。

checkpoint 必须直接从 V20 durable ledger 导出，不得手工拼装或编辑。先确认
第 4.1 节迁移已经完成、`--as-of` 是正式生效日前一个交易日，并且该日前向影子槽
已经终态，然后执行。导出命令本身以 `migrate=False` 连接，只读账本，不会顺带执行
DDL：

```bash
python scripts/export_v20_checkpoint.py \
  --database-config config/database-config.yaml \
  --source-stream V20_A_SHARE_FORWARD_SHADOW \
  --source-lineage V20_FORWARD_SHADOW_GENESIS_20260831 \
  --target-stream V20_A_SHARE_PRODUCTION \
  --target-lineage V20_PRODUCTION_GENESIS_YYYYMMDD \
  --as-of YYYY-MM-DD \
  --output config/v20-checkpoints/v20-production-YYYYMMDD.json
```

导出器会在一个只读串行事务中校验来源 stream/lineage、官方 state hash、与目标
配置一致的 `state_semantics_hash`、as-of
终态前驱和完整的 7 个有效 rolling 批次；它还会迁移健康水位、活动 rolling gap、
所有仍待成熟的 HEALTH/ROLLING7 批次，以及在 as-of 决策后才形成、尚未被下一次
决策消费的 HEALTH 终态和 `COMPLETE_INVALID` rolling 事实，并为目标 lineage 生成
确定性的 source→target batch ID 映射。任一被 state 引用的健康/活动 gap 事实缺失都会
拒绝导出。目标 state
固定从 revision 0 开始，`last_terminal_slot_id` 和 `last_terminal_trade_date` 固定为空，
不会用 shadow 槽伪造生产前驱。

命令只会新建 checkpoint；同路径已有不同内容时会拒绝覆盖。把命令输出的
`checkpoint_sha256` 原样填入 `bootstrap.checkpoint_sha256`，并把 target stream/lineage
原样填入活动配置。候选镜像内再次执行字节 hash 校验，例如：

```bash
sha256sum config/v20-checkpoints/v20-production-YYYYMMDD.json
```

首次生产启动会在同一个数据库串行事务内建立 lineage registry、revision-0 state 和
迁移的 shadow facts。重复启动只接受完全相同的 checkpoint/source 映射；同一 target
lineage 若已绑定不同 stream、checkpoint hash、state 或 batch 语义会 fail closed。不要
通过改文件、复用旧 target lineage、删除 registry/state 行或手工补 shadow batch 绕过。
目标服务即使在 checkpoint 的 `as_of_trade_date` 当天启动，也只应显示
`BOOTSTRAP_AS_OF_DAY`，不得产生同日 entry slot；必须等到日历中的下一个交易日。

最后才设置三重开关并重新部署：

```text
V20_ENABLED=true
V20_MODE=production_push
V20_ALLOW_PRODUCTION_PUSH=true
```

三项缺一不可；但三项齐全也不能替代 checkpoint 和前向影子门槛。正式启动后应
确认容器来自 Docker `v20` target、`/api/v20/status` 是预期 lineage/config hash，
除四个 `/api/v20` 路径外没有平台、订单、持仓或 iQuant 路由，
影子群没有收到正式事件，正式群也没有收到影子事件。

共享 schema 升级后，outbox worker 只扫描和租赁与当前
`route_id + official_stream_id + lineage_id` 完全相同的行。切换到正式 lineage 前，
先确认迁移已回填旧决策事件的作用域；无法可靠归属的旧告警会进入
`LEGACY_UNSCOPED` 隔离区，不会阻塞或被新 worker 投递。正式 09:40 事件不得等待
影子 route/lineage 的积压。可用以下查询分别核对隔离 backlog：

```sql
SELECT route_id, official_stream_id, lineage_id, seal_status, delivery_status, count(*)
FROM v20.outbox_events
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;
```

## 8. 回滚

回滚只切换服务所有权，不回写或删除 V20 账本：

1. 保存故障时间、当前 status、镜像 digest、config/lineage hash、最后 outbox 事件
   和数据库提交水位；
2. 设置 `V20_ENABLED=false` 后重新部署或停止 Docker `v20` 实例，停止 V20 新计算与推送；
3. 明确选择是否恢复旧 V16。Docker `v20` target 的专用入口不会启动旧 V16 调度器或
   平台执行面；镜像内仍包含 V20 复用的 V16 选股源码，因此不能仅通过修改它的 mode
   恢复旧扫描：

   - 不恢复旧 V16：保持默认平台 `runtime` 未部署/扫描关闭，系统保持无官方扫描推送；
   - 需要恢复旧 V16：另行部署默认 `runtime` target，设置
     `V20_ENABLED=false`、`V20_MODE=forward_shadow`、`WEB_ENABLED=true`，再验证旧 V16
     scheduler 的单实例所有权。

4. 验证 `/api/v20/status`/启动日志、旧 V16 scheduler 状态和两个飞书群；
5. 保留全部 `v20` schema、checkpoint、输入快照、outbox 和投递尝试供审计。

禁止通过删除 schema、truncate、篡改状态 hash、伪造 ack 或重发旧决定完成回滚。
再次启用 V20 前，应先审计停机期间的待处理 outbox、状态水位和 checkpoint/lineage，
按新生效日重新走启用评审，不能把服务重启等同于策略恢复。

## 9. 最小当日值守检查

- 09:15 后：预热成功，D0 前最后 37 个交易日历史 profile 正确，逐票合格集合和逐日
  覆盖均以冻结 V16 股票池为分母达到 80%，停牌/新股没有被填充或拿更老日期替代；
  数据库、行情、V16 版本和冻结制品无告警；status 日历末日至少晚于当前两个交易日，
  route/stream/lineage/config hash 与部署单一致。
- 09:39 后：合法扫描代码都有完整 09:31..09:39 路径、总覆盖达到阈值，没有
  09:38 回退；缺失集合已进入快照，宽度集合没有混入 80% 分母。
- 09:40 前：terminal guard 已放行；若没有正常决定，截止一到应立即出现 durable
  `INPUT_INVALID`/不买终态。post-commit 密封若迟到，正式群只能收到不买或过期告警，
  不能继续等待到 09:45 才作常规判断。故障演练必须覆盖预热或 missed-slot reconciliation
  从 09:39 阻塞跨过 09:40，并验证任务被截止看门狗取消、数据库门禁不被本机时钟绕过；
  若补槽暂时失败，则稳定 ID 的 `ENTRY_CUTOFF_NO_BUY` 必须先到达且可幂等重试；首次
  日历加载跨点还须验证工作日发 `ENTRY_CALENDAR_UNKNOWN_NO_BUY`、周末和已确认休市不报。
- 09:41 后：模型腿参考价来自结束标签 09:41 的 open，不是 09:40 标签；核对模型腿
  与影子批次各自固定截止。
- D1/D2：每条模型腿均有明确退出求值；单分钟缺口不能关闭后续止损，触发后检查退出
  推送和后续提醒。
- 全日：检查 `running/healthy/runtime_lanes` freshness、数据缺口、数据库
  CAS/semantic conflict、route 级 leader、outbox 密封/投递重试和飞书路由隔离；任何
  未知状态按 fail-closed 处理，不临时改阈值或补写历史。
