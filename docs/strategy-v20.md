# V20 策略决策与推送规范

> 冻结日期：2026-08-31
> 策略版本：`V20_BAD_E50_G_BASE_V1`
> 策略规则：`FROZEN`
> 生产实现：`COMPLETE_EMBEDDED_FORWARD_SHADOW`
> main 运行状态：`AUTO_EMBEDDED / forward_shadow`
> 订单执行：`OUT_OF_SCOPE`

研究数字、反证和制品来源见[研究证据附录](./strategy-v20-evidence.md)，部署步骤见
[运维手册](./strategy-v20-runbook.md)。本文件只定义实际生产代码必须执行的规则。

## 1. V20 到底做什么

V20 不替换 V16 的选股算法。V20 先按第 3.1 节冻结并校验一份时点合法输入；在同一份
已经通过 V20 合法性校验的输入上，每天选哪些股票、排序如何，仍由 `main` 分支的
`V16Scanner.scan()` 决定且必须零差异。因停牌、新股或数据缺失而排除不合格输入，
属于生产输入校验，不是另建一套排序规则。V20 在同一次扫描返回的完整
`recommended` 列表上做两件事：

1. 决定当天新模型批次投入一份标准批次的 `100% / 50% / 25% / 0%`；
2. 维护每只模型腿的 D1/D2 退出规则并通过飞书推送。

这里“V16 完整推荐列表”是同一次 `V16ScanResult.recommended` 的全部结果，当前最多
10 只；不是 `all_scored`，不是只取第一只，也不表示全市场每只股票都有实时行情。

V20 只维护模型账本，不读取账户资金或真实持仓，不计算买卖股数，不调用下单、
撤单或成交接口。飞书消息是策略建议，不是成交确认。手续费、税费、佣金和人为
滑点一律不进入策略计算。

## 2. 总公式

若当天 V16 推荐 `n` 只股票，定义一份标准日批次本金为 `B`：

```text
final_multiplier = base_multiplier × defense_multiplier
每只模型腿的相对投入 = B / n × final_multiplier
```

- `base_multiplier ∈ {1, 0.5, 0}`；
- `defense_multiplier ∈ {1, 0.5, 0}`；
- 两层相乘，因此最终可能为 `1 / 0.5 / 0.25 / 0`；
- 被减掉的投入不分配给其他股票、其他日期或其他批次；
- 没有推荐票时为 `NO_SIGNAL`；输入不合法时为 `INPUT_INVALID`；两者都不建可行动模型批次。
  ROLLING7 是否形成理论事实只取决于 canonical V16 制品是否完整，与是否实际开仓无关。

## 3. 时间与行情边界

全部时间使用 `Asia/Shanghai`，原始分钟时间按“分钟结束标签”解释。

### 3.1 每日入场

1. 09:15 起预热 V16 模型、股票池、历史行情和板块映射；
2. 09:31 起采集原始分钟行情；
3. V16 入场输入严格截止到原始结束标签 `09:39`，不得用 09:38 或任意最新行回退；
4. 历史输入口径固定为 `STRICT_LAST_37_EXCHANGE_SESSIONS_V1`。交易所日历先冻结 D0
   之前最近 37 个交易日；每只可用于打分的股票必须对这 37 日逐日各有一条真实、合法
   OHLCV。各字段数组必须等长，日期严格递增且唯一，价格和成交量必须有限、为正并
   满足 OHLC 关系。允许供应商响应携带更老记录，但实际冻结输入只能是上述 37 日；
   缺少其中任一日时，该票不得打分，不得前向填充、补零或用更老的第 38 日替代；
5. 对已经进入当天冻结 V16 股票池的代码，停牌、新股和来源缺失都按逐票不合格处理，
   不因为单票不合格直接否决全日，也不因“已知停牌/尚未上市”再降低分母。历史完整
   票数除以当天冻结的 V16 股票池总数必须 `>=80%`，低于 80% 为输入异常；
6. 一只最终可用于本次扫描的股票还必须具有连续的原始 `09:31..09:39` 路径、合法
   昨收及上述完整历史。实际进入 `V16Scanner.scan()` 的股票数除以同一个冻结 V16
   股票池总数也必须 `>=80%`；历史、分钟或昨收缺失代码、实际扫描代码和覆盖率全部
   写入快照，低于 80% 为输入异常；
7. 上述两个 80% 门槛的固定分母都是冻结的 V16 股票池，不是“全市场列表完整”，
   也不是先删掉停牌、新股或缺数票之后的动态可用集合。市场宽度股票另用独立
   collector；宽度缺失或冲突不进入任何 V16 80% 分母，也不能在健康状态下否决合法
   V16 扫描。消息里的“完整推荐”只表示没有截断本次合法 V16 输出；
8. 决策快照至少记录 `history_profile_id`、逐票 `history_input_hashes`、37 日各自的
   `history_date_valid_counts`、`history_min_date_coverage`，以及
   `scan_input_codes / scan_input_failure_codes / scan_input_coverage`。这些字段与快照
   hash 一起冻结，重试不得悄悄换历史窗口或合格股票集合；
9. 正常 `ENTER/BLOCK/NO_SIGNAL` 的串行事务必须由最后一条 PostgreSQL terminal guard
   在数据库时钟严格早于 09:40:00 时放行；
10. 到达 09:40 仍没有 durable 正常决定时，立即以数据库时钟门控失败关闭为
   `INPUT_INVALID` 并密封“不买”通知；09:40 以后形成或被 terminal guard 拒绝的正常
   决定同样失败关闭，不建 HEALTH 或可行动模型批次，也不允许提示追买。若完整 canonical
   V16 制品已经形成，独立 ROLLING7 理论事实仍照常记录；否则记录可恢复 `DATA_GAP`。
   decision scheduler 的单调时钟看门狗会在 09:40 取消仍阻塞的预热、扫描或账本对账；
   若有序账本暂时不能补入 `INPUT_INVALID`，必须先持久化稳定 ID 的
   `ENTRY_CUTOFF_NO_BUY` 告警并继续幂等补槽。若交易日历首次加载也阻塞跨点，工作日
   先发稳定 ID 的 `ENTRY_CALENDAR_UNKNOWN_NO_BUY`；成功加载的日历已确认休市则不报。
   09:45 只用于异常恢复/最终故障补槽，不是常规等待点或买入窗口的延长。

terminal guard 是事务内的资格裁决时点，不冒充其后真实 COMMIT/WAL 可见时点。本地
调度时钟只能提前拒绝，不能绕过数据库边界；核心事务失败则整体回滚。核心事务可见后，
outbox 密封会再次采样 PostgreSQL 时钟：若此时已经到 09:40，买入文案必须替换成
“已过期、今天不要追买”的故障消息。通知/下单本来就与理论模型账本解耦，因此迟到
投递不得反向改写已经提交的策略决定或删除其模型腿；后续退出仍按该理论腿推送，
操作者结合自己是否实际买入处理。

### 3.2 入场参考价

生产参考价口径固定为：

```text
CALENDAR_0940_OPEN_END_LABEL_0941_V1
```

即日历 09:40 的开盘参考价取原始结束标签 `09:41` bar 的 `open`。该价格只用于模型
评价和退出阈值，不参与 09:40 前的入场决定。所有原始候选都保留审计，仲裁必须先
剔除不合法候选，再按首次持久接收时间排序：

- 可行动模型腿选择严格早于 D1 09:30 的最后一个合法修订，以便单腿在次日做有限历史恢复；
- ROLLING7 是由每个完整 canonical V16 制品独立触发的市场健康事实流，不依赖
  `ENTER`、模型腿、真实订单、持仓或任何交易/shadow ledger；
- ROLLING7 每腿的 D0 参考价是法定 D0 截止前冻结的原始 `09:41 bar.open`；
- 同一截止口径下，同一接收时点出现不同合法内容视为该代码冲突；缺失/冲突不得填前值。

模型腿与影子批次分别生成证据 hash；某只模型腿缺价时仅该腿标为 `UNAVAILABLE`。

## 4. BASE 基础倍率

### 4.1 健康标签

每个有推荐票的 D0 都保留一个 HEALTH 影子批次；不足 3 只时该批次终态无效。合法
批次以 D0 的 09:40 参考价到 D2 收盘的零成本毛价格收益计算：

```text
health_return
= Top3 等权平均收益 - 当日冻结比较池的有效股票等权平均收益
```

比较池有效股票少于 1000 只、Top3 缺价或参考价未锁定，该标签终态无效，不伪造
数值。状态只消费按日期顺序到达且尚未处理的终态标签：

- 少于 3 个有效标签：`WARMUP`；
- 最近 3 个有效标签均值 `< 0`：进入/保持暂停 `PAUSED_R0`；
- 暂停后每出现一次最近三标签均值 `>= 0`，依次经过 `R1 → R2 → HEALTHY`；
- 恢复过程中再次 `< 0`，回到 `R0`。

`WARMUP` 和 `HEALTHY` 的 `base_multiplier=1`。

### 4.2 暂停时的 09:39 市场广度

只有健康状态为暂停时才使用市场广度。统计主板 `00/60` 股票中 09:39 价格低于昨收
的只数，合法样本少于 1000 只则基础倍率为 0。否则计算下跌比例的单侧 Wilson 95%
下界 `W`（`z=1.645`）：

| W | base_multiplier |
|---:|---:|
| `W <= 0.50` | 1 |
| `0.50 < W <= 0.60` | 0.5 |
| `W > 0.60` | 0 |

边界值按表中包含关系处理。

## 5. rolling7 与超级防护 G

### 5.1 rolling7

每个完整 canonical V16 制品都产生一个 ROLLING7 事实，即使决策为 `INPUT_INVALID`
或最终倍率为 0；迟到的恢复制品也产生同一理论事实。成功且完整的空推荐列表是
`NO_SIGNAL`，不占 7 个信号批次；缺失、失败或不完整制品是 `DATA_GAP`。

对每个非空 canonical 完整推荐列表，取全部腿等权。批次收益是该完整列表从法定 D0
截止冻结的原始 `09:41 bar.open` 到官方 D2 日线收盘的零成本毛价格收益等权平均值；
任一腿缺失是可恢复 `DATA_GAP`，不得删除该腿后计算。

scheduled/manual 决策必须使用同一个冻结 as-of 窗口：取预计 D2 严格早于决策日期的
最近 7 个已经成熟完整 `SIGNAL` 批次：

```text
R7 = 7 个批次收益之和
L7 = 其中收益 < 0 的批次数
BAD = (R7 < 0) AND (L7 >= 5)
```

少于 7 个是 `WARMUP`；窗口内未解决缺口是 `DATA_GAP`，且该缺口只在被解决或被之后
7 个完整信号批次推出窗口后消失。二者不得渲染成泛化 `UNKNOWN`。`WARMUP` 与
`NON_BAD` 都不启动新增防御，即 `defense_multiplier=1`；`DATA_GAP` 按输入缺口处理。

历史恢复/回填只允许运行在非交易关键路径。它从 as-of 决策窗口向后扫描，直到取得
7 个完整 `SIGNAL` 批次或显式有界上限，持久化部分结果和 `DATA_GAP` 供重试，且不得
阻塞 09:40 入场或实时退出。

### 5.2 G：只在 BAD 时判断

rolling7=`BAD` 时，先把 `defense_multiplier` 降为 0.5，再判断 G：

1. 必须恰有合法 Top10；不足 10 只时 G=`UNKNOWN`；
2. 每只股票使用同次 V16 的 `best_board + hot_route`，经冻结语义映射转为规范主题；
3. 两只股票只要共享一个允许聚类的规范主题就连边，取连通分量中的最大股票数
   `max_component_size`；
4. 使用 Top10 在 D-1 的绝对成交额，计算总额、中位数、最低 3 只合计；
5. 三项分别与“决策日前已冻结的同半年 Q25”比较，`<= Q25` 记为一项弱；
6. 同时满足 `max_component_size <= 3` 且至少 2 项绝对成交额指标弱，G=`TRIGGERED`。

结果：

| rolling7 | G | defense_multiplier |
|---|---|---:|
| `NON_BAD` / `WARMUP` / `DATA_GAP` | 不计算 | 1 |
| `BAD` | `CLEAR` / `UNKNOWN` | 0.5 |
| `BAD` | `TRIGGERED` | 0 |

G 缺映射、缺 D-1 成交额、缺当前半年阈值或 Top10 不完整时为 `UNKNOWN`，保持 BAD
半仓，不扩大成全局输入故障。G 的 manifest 和引用文件必须逐字节通过 SHA-256 校验。
当前制品最后覆盖 `2026H2`；若后续半年尚未发布新制品，代码会按上述 `UNKNOWN` 规则
继续 BAD 半仓并在每日决策原因中明确显示 `Q25_THRESHOLD_MISSING`，不得偷偷沿用旧半年阈值。

## 6. 实际动作解释

| final_multiplier | 消息含义 |
|---:|---|
| 1 | 建立 100% 标准模型批次 |
| 0.5 | 建立 50% 标准模型批次 |
| 0.25 | 建立 25% 标准模型批次 |
| 0 | 当天不建立新模型批次 |

倍率只针对当天新批次，不修改过去已经建立的模型腿。飞书必须同时展示 BASE、
rolling7、G、最终倍率、原因码和完整 V16 推荐列表；有旧批次当天到 D2 时，还要在
每日消息中列出计划退出股票。

## 7. 退出规则

`D0` 是推荐日，`D1/D2` 是之后第一/第二个交易所交易日。每只模型腿独立判断，
参考价都是该腿已锁定的 D0 09:40 参考价。保护条件以合法完整一分钟 bar 的收盘价
触碰或跌破阈值为准，动作从下一连续交易分钟起生效。

| 时段 | 条件 | 动作 |
|---|---|---|
| D1 | 分钟收盘 / 参考价 `<= 0.92` | 整腿退出 |
| D2 | 合法 MEWS=`DANGER` 且分钟收盘 / 参考价 `<= 0.95` | 整腿退出 |
| D2 | 其他情况分钟收盘 / 参考价 `<= 0.88` | 整腿退出 |
| D2 14:57 | 尚无更早退出 intent | 整腿计划退出 |

D2 退出求值消费当前 D2 可得、且来源交易日为 D1 的快照；该快照不是在 D1 前预先
选择的快照。每条腿在第一次 D2 求值时原子冻结选择。若存在 legacy 较早
选择且没有任何退出 intent，可升级为该 D2 选择；已存在退出 intent 或已冻结的 D2
选择永不改变。没有合格快照时冻结常规 -12% 兜底并告警。MEWS 永不影响入场。
V20 在每个 D2 交易日 09:10 后使用 Tushare 的 `margin`、`margin_detail` 和 `daily_basic`
原始素材，按冻结的 `mews_v2` 公式在本地续算上一交易日结果。若当天快照缺失，则从持久化
的紧凑状态现场补齐，校验来源日、数据质量和发布时点后写入自己的 PostgreSQL 快照；成功
后当天不再访问上游。盘中退出路径只读该快照，不依赖持仓或账本来计算 MEWS。

分钟 bar 必须代码、日期和标签匹配，OHLC、成交量、成交额均为有限正数且数据源确认
完整。退出使用每个标签“第一次持久收到的合法版本”；后续修订保留审计但不能撤销或
制造历史止损。某一分钟缺失、非法或仍待恢复只形成诊断缺口，不得屏蔽其后任何独立
合法分钟的 D1/D2 保护；同一腿只允许第一个退出 intent 成为正式退出建议。D1 的
14:57 bar 若触发，因该 bar 完成时同刻已过去，动作时点固定为 D2 首个连续分钟 09:31。

D2 14:57 是最后一道闸门：即使此前分钟窗口有缺口或参考价不可用，也仍产生计划
退出，同时明确附加数据缺口原因并告警。这样数据故障不会把模型腿无限期留在账本中。
保护触发和计划退出都建议退出该模型腿 100%，不是退出账户里该代码的全部真实持仓。

若退出事件未收到停止提醒确认，后续交易日 09:35 继续提醒。确认只停止提醒，不代表
下单或成交，也不改写退出决定。

## 8. 状态、幂等与恢复

- PostgreSQL 是唯一正式状态源；不使用进程内猜测状态继续决策；
- 每个 lineage 在建立时永久绑定显式的状态/事件/快照 schema 版本；只有这些 schema
  版本、scope/lineage/stream 作用域或状态内容完整性的变化才要求新 lineage 并重新走
  影子与 checkpoint。规则或状态口径变化必须通过升级显式 schema 版本表达；Python 源码、
  格式化、注释或文档变化本身不构成状态不兼容，不得因此强制迁移或新建 lineage；
- 同一 `official_stream_id + trade_date` 只有一个终态槽；状态更新使用 revision/hash
  CAS；同一公开 `route_id` 只允许一个 advisory-lock leader，不能通过更换 stream 或
  lineage 在同一飞书路由上启动第二个 runtime；
- 入场槽原子写入配置绑定、输入快照、决定、下一状态、HEALTH 批次、
  模型腿和 outbox 骨架；退出 intent 与退出 outbox 骨架同样原子写入；
- V16 原始快照、当次实际消费的 HEALTH/rolling7/gap 事实、前置状态 hash 和显式
  状态/事件/快照 schema 版本共同进入决策输入快照及决定 ID；重试不能悄悄换一组
  状态输入；
- `v20-runtime/v2` 的 `config_hash` 不是纯审计元数据：它是规范化运行时配置与保留
  非代码制品身份的规范身份，记录冻结规则、时钟、G manifest、受审飞书 relay
  origin/APP ID hash/chat ID hash、两路 PostgreSQL CA 内容 hash 以及部署制品摘要。
  它标识当前 config registry 记录，并被事件、决定和幂等绑定引用，用于确认当前运行
  的配置。它不包含任何 Python 源码或 Git 字节，且不得作为历史状态兼容或源码过渡
  授权的依据；
- 只有 Git commit/build SHA 和镜像 digest 是纯审计/日志元数据；它们连同
  `config_hash` 一律不得作为启动、状态兼容、交易、回放或迁移授权的依据，不能决定
  策略是否运行；
- 禁止把 Python/源文件字节 hash 用作运行时启动、状态兼容、交易、回放或迁移授权；
  禁止 commit/hash 到 hash 的过渡 allowlist 和兼容性证据/回执；禁止因为格式化、
  注释或文档变化要求迁移；禁止据此制造运行时“版本分身”或绕过唯一正式槽；
- 状态兼容只由显式的状态/事件/快照 schema 版本、scope/lineage/stream 作用域、内容
  完整性，以及 schema 变化时的显式数据库迁移授权；
- 模型、feature list、板块数据和 G 制品等非代码策略制品保留 checksum 完整性校验；
  这些 checksum 只证明内容未被改动，不构成源版本兼容链；
- 既有 `state_semantics_compatibility` 表运行时不读、不写，也不做破坏性清理迁移；
  registry `state_semantics_hash` 仅可在 genesis 写入非授权审计 metadata，绝不
  作为兼容、启动、交易或 replay 门禁被读取、比较或重写；
- 行情、MEWS 和成熟日线先以不可变候选保存，再按各自固定截止时间选用；
- 核心事务提交后由 durable outbox 密封并投递；崩溃重启会继续密封和重试，不重算
  已提交决定；
- outbox 按 `route_id + stream + lineage` 隔离，影子积压不能阻塞正式 09:40 消息；
- V20 outbox 对 relay 是至少一次请求；专用 `/api/v20/send` relay 必须用
  `route_id:event_id` 原子去重，并在真实飞书接受后严格回显 event、route、payload hash、
  目的地指纹和接受时刻。未知/错误回显不标记 SENT；可执行入场由 relay 自身时钟在
  09:40 再判定原文或“已过期、不要追买”，不能依靠客户端取消 HTTP 请求兜底；
- main 内嵌旧 relay 不具备上述 V20 回显协议，因此 publisher 必须先用 PostgreSQL
  lease 时钟判断剩余窗口：已到截止点时只发送非行动性的过期文案；尚未截止时 HTTP
  超时必须在 09:40 前额外留出保护余量，适配器本地时钟再做一次只会收紧的过期检查；
- 服务停机漏过交易日时，正式终态按交易日顺序补 `INPUT_INVALID`，不能跳日恢复；
  独立 ROLLING7 缺口由非交易关键路径持久化并重试。全新影子 lineage 以显式空前驱
  开始，生产 lineage 必须从 checkpoint 开始。
- 交易日历由 V20 已依赖的 Tushare `trade_cal` 独立获取，不复用 legacy 永久缓存；
  每日有界刷新，进程内安全缓存只在仍有历史前驱和至少两个未来交易日时降级复用。

## 9. checkpoint 与生产切换

空状态只允许 `forward_shadow`。生产 checkpoint 必须由已经验收的 V20 影子 ledger
只读导出，至少包含：

- 截止日官方状态、最近健康水位和活动 gap；
- 尚未成熟的 HEALTH 批次；
- as-of 决策后才形成、尚未被下一次决策消费的 HEALTH 终态；
- source→target 批次 ID 确定性映射；
- 来源 stream/lineage、截止交易日、来源状态 hash；
- 与目标运行时一致的显式状态/事件/快照 schema 版本与 scope/lineage/stream 绑定。

ROLLING7 独立理论事实不随交易/shadow ledger 或 lineage checkpoint 迁移；目标运行时
从其独立持久事实恢复，并在非交易关键路径按上述规则回填缺口。

checkpoint 导出文件使用 `v20-bootstrap-checkpoint/v3` schema：在 v2 的基础上移除已
退役的来源配置/状态语义审计 hash 字段（`source_config_hash`、
`source_state_semantics_hash`、`resolved_state_semantics_hash`）。导入器同时接受
历史 v2 和 v3 文件；v2 中遗留的来源 config/state-semantics 审计 hash 只作历史审计
记录被忽略，绝不用于状态兼容或任何其他授权。

导入会把目标状态置为 revision 0，目标最后终态槽保持空；不得把影子槽伪装成生产
前驱。checkpoint 文件本身和配置中声明的 SHA-256 必须一致，同一目标 lineage 不能
绑定另一份 checkpoint。`as_of_trade_date` 已经被该 checkpoint 消费；即使目标服务在
当天启动，也必须显示 `BOOTSTRAP_AS_OF_DAY` 且不得再次开同日槽，第一天只能是下一个
交易所交易日。

## 10. 对外接口与生产边界

现行 main 部署使用内嵌 `forward_shadow`：V20 与 V16 同容器运行，复用已经工作的
`DB_*`、Tushare token 来源和 `FEISHU_*` 机器人配置，但仍使用独立 `v20` schema、
leader、状态账本和 durable outbox。它只产生决策与推送，不接入下单。
内嵌账本优先借用 main 已连接的 fundamentals pool；pool 生命周期仍由 main 所有，V20
关闭时只释放 advisory leader，不关闭共享 pool。共享 pool 不可用时，才按
`database.trading` 建立 V20 自有 pool；现行 trading/state 未向 asyncpg 启用 SSL，
因此该 fallback 明确传入 `ssl=False`。专用 V20 仍必须显式配置 `verify-full`。main 中的
V16 调度继续运行；V20 输入仍严格截止到 09:39 完整 bar。

显式 `production_push` 仍只允许专用进程 `scripts/v20_main.py`/Docker `v20` 目标。该进程
只暴露：

```text
GET  /api/v20/status
POST /api/v20/mews-snapshots
POST /api/v20/reminder-stop-acks
POST /api/v20/trigger-scan
POST /api/v20/manual-monitor
```

专用进程不加载平台 `SystemManager`、`PositionManager`、iQuant、订单、持仓或成交路由。
MEWS 和停止提醒确认两个 POST 接口由 `V20_INGEST_API_KEY`/`X-V20-API-Key`
保护；status 由独立 `V20_STATUS_API_KEY`/`X-V20-Status-Key` 保护。两套密钥必须显式
配置、至少 32 字符且彼此不同，不能跨接口授权。人工触发当前有意不做应用层鉴权，
调用时不需要 API key；这一选择不改变其 health、leader、时点和并发门禁。
status 只返回后台有界采样的内存快照，请求本身不得查询 PostgreSQL；快照缺失、采样
失败或过期时 `healthy=false`。网络层仍应限制在内网。影子和正式飞书使用不同 chat、
APP 凭据及 outbox 作用域。只有明确的 main 内嵌 profile 会复用 V16 飞书目的地和旧
`/api/send` relay；专用 V20 profile 仍禁止隐式回退。

人工触发接口兼作早盘选股触发与部署验收，默认无需任何请求 header。未提供
`Idempotency-Key` 时，服务端生成 `manual-<uuid>` 作为本次请求 ID；调用方也可以提供
8–128 字符的 key，并在超时或结果未知时使用同一值安全重试。接口不接受调用方指定的
request body、时钟、交易日或强制参数。09:15 至 09:40 前且当日尚无终态时，它加速的
就是自动 scheduler 的同一条串行 official decision cycle，仍受交易日历、原始 09:39、
数据完整性、PostgreSQL leader 和 09:40 截止约束；飞书只出现正常
`ENTRY_DECISION` 正文，不再附加人工回执。若结果为 `ENTER`，同一正式提交会照常创建
模型批次和模型腿，供 D1/D2 盘中退出链使用。

09:40 起，正式策略槽位和正式状态严格只读，绝不用晚到行情替换或新建正式决定。已有
正常正式结果时，接口把已密封的官方 `payload.message` 原字节复发，不重新渲染、不加
标题、前缀、后缀或验收说明。若正式槽是 `INPUT_INVALID` 或没有可复发的正常消息，服务
重拉并严格截取 raw 09:31–09:39，基于持久化事实和失败槽冻结的 policy inputs 只读重算，
再调用正常早盘 `render_entry_message` 输出。HTTP 必须标记
`retrospective_expired=true`、`manual_notice_actionable=false`；正文自身仍保留“仅在
09:40 前有效、迟到不得追买”。盘后两种路径都不得创建或修改正式状态、shadow batch、
模型腿、MEWS 选择、卖出信号、订单或券商状态。重算失败时只能发送明确的失败报警，
不能伪造一条不存在的成功早盘消息。

上述盘后重算仍然不会自行创建模型腿。只有操作人随后显式调用
`POST /api/v20/manual-monitor`，并提交该条已密封 `PASS/ENTER` 重算事件的完整 64 位
`source_event_id`，才允许在 D1 09:30 前建立独立的 `MANUAL_MONITOR` 批次。调用方不能
改票、权重、交易日、参考价或退出规则；全部票的 D0 原始 09:41 bar 必须先完整合法落库，
最终参考价仍在 D1 09:30 仲裁锁定为 bar.open。人工腿只复用既有 D1/D2 卖出提醒，不修改
正式入场槽位和状态，不创建账户持仓、订单或成交。详细操作与验收见运行手册 5.5。

复盘分钟线必须先按数据库真实接收时钟持久化，再由持久化 raw bar 计算；`data_cutoff=09:39`
不等于这些字节在 09:40 前已经收到。BASE/滚动7读取失败槽内冻结的 `policy_inputs`，不得
在下午重新查询成熟事实。复盘/复发事件按 route、stream、lineage、config、请求 ID 和
来源证据固定唯一 ID；同一 `Idempotency-Key` 重试返回同一事件，不同无 header 请求允许
再次推送。当前内嵌 `EMPTY_FORWARD_SHADOW` 从空状态前向
暖机，因此它能重建当日 V16 票单及当前 shadow lineage 的判断，但不能冒充研究回测或
尚未导入的生产 checkpoint 状态。

接口返回 HTTP 202 表示对应事件已经进入 durable outbox 并完成密封，不表示飞书已经接收。
调用方必须用响应中的 `entry_event_id` 或 `replay_event_id` 检查 outbox 最终进入 `SENT`，
并核对目标飞书群。
如果需要让网络超时或未知结果可以去重重试，调用方应在首次请求前自行生成并携带
`Idempotency-Key`，随后始终原样重用；在相同 route、stream、lineage 和 config 下，
相同 key 返回同一事件且不会重新运行盘后重算或重复创建运输事件。若不携带 header，
服务端会在每次调用生成新 key，因此盘后再次无 header 调用会创建新的复发事件。

仓库安全默认固定为：

```text
enabled: false
deployment_mode: forward_shadow
production_activation_guard: false
bootstrap.mode: EMPTY_FORWARD_SHADOW
routes.*.expected_*: UNCONFIGURED
database.*_tls_ca_sha256: UNCONFIGURED
```

这些是配置文件和专用进程的安全默认。main 未提供任何显式 V20 激活变量时，由代码创建
内嵌 `forward_shadow` profile；可用 `V20_EMBEDDED_ENABLED=false` 明确关闭。内嵌 profile
动态绑定现有 V16 目的地，并将绑定摘要和 `legacy_main_embedded/v1` 写入 `config_hash`，
不会把 app secret、数据库密码或 token 写入账本。

任何显式 `V20_ENABLED=true` 都必须先填入受审目的地
binding、两路 CA 摘要、两组独立数据库身份和两套独立 HTTP API key，否则在数据库
连接前失败。
启用时还会核对 `config/database-config.yaml` 解析出的两路实际连接参数与显式环境完全
一致，并将该文件摘要纳入 `config_hash`；实时、历史日线和 ST 查询统一使用显式
`TUSHARE_TOKEN`，不读取 legacy token 文件；main 内嵌 profile 则与 V16 一样允许读取
已经由设置页持久化的 token。

正式模式必须由环境同时显式设置：

```text
V20_ENABLED=true
V20_MODE=production_push
V20_ALLOW_PRODUCTION_PUSH=true
```

并使用非 SHADOW 的 stream/lineage、合法 checkpoint、专用数据库 writer 和正式飞书
路由。默认平台进程拒绝承载 `production_push`；正式模式只能运行专用 V20 进程。

## 11. 已知边界

1. 历史回放能验证策略逻辑，不能证明当年首次接收时钟；生产启用前必须完成前向影子；
2. 当前行情源没有供应商原生 revision 序号，因此分别冻结“入场冲突代码剔除后重算
   80% 覆盖、参考价各自截止前最后持久合法版本、退出首个持久合法版本”三种可解释
   仲裁规则；
3. 收益与阈值使用原始价格连续链，不做手续费和人为滑点，也不做现金分红总回报；
   除权、送转等样本必须在前向阶段单独核查，保护规则仍以实际原始价格执行；
4. V20 是风险投入和退出纪律，不是自动交易，也不能保证每月都优于 BASE；
5. `UNKNOWN` 是明确的策略状态，不允许人工看完当天走势后回填成 `BAD/NON_BAD`。

## 12. 生产验收清单

代码实现完成不等于已经启用。正式切换前至少完成：

1. 全量单测、静态检查、迁移与镜像只读 smoke 通过；
2. 独立 V20 进程确认只有五个 `/api/v20` 路由，没有账户、订单、持仓或 iQuant 路由；
3. 专用 writer 的 schema 权限、单 leader、重启补槽和 checkpoint 演练通过；
4. 连续前向影子覆盖正常日、无票日、BAD/G、09:40 边界、行情缺口/修订、重启、
   Feishu 失败、D1/D2 止损、MEWS 缺失及 D2 14:57 兜底；
5. 逐日核对 V16 推荐列表与线上 main V16；同一份通过第 3.1 节 V20 合法性校验的
   输入交给两边时，推荐和排序必须零差异，并单独核对被历史/分钟门槛排除的代码；
6. 验证影子/正式路由隔离、过期买入消息替换和事件 ID 去重；
7. 验证人工触发无应用层鉴权、无 header 自动生成请求 ID、可选 key 重试；验证 09:40
   前只产生正常早盘正文且 `ENTER` 建立模型腿，09:40 后原消息逐字节复发或只读重算，
   并核对 HTTP 202 后 outbox/飞书的最终投递；
8. 审核并接受 checkpoint 后，再选择未来交易日人工切换三重开关。

日历验收还必须确认 Tushare `trade_cal` 网络放行、15 秒有界失败、跨年未来至少两个
交易日，以及 `/api/v20/status` 返回预期的 calendar horizon、route、stream、lineage
和完整 config hash。

任何策略阈值、参考价、状态口径、模型或板块制品变化都必须升级版本并重新走影子，
不得直接修改历史 ledger。
