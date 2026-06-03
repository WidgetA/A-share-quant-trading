# 数据完整性管线架构 (Data Integrity Pipeline)

> 这套管线负责维护一个**正确的、可直接查询的每日数据源**。改动前先读本文——逻辑复杂,
> 多处是踩坑后才定下来的设计,改错一处会静默丢数据。配套:`docs/features.md` DAT-006、
> CLAUDE.md §8(数据源)/§9(成交量单位)。

---

## 1. 目标

一句话:**一查就知道"某天某只票该不该有数据、库里对不对、错在哪类",不用每次复杂推断。**

之前的做法是每次缺口检查都临时重算 `effective_universe − backtest_daily`,名单不全时只能标
"待核对",补全只补整空的日子、补不了"有一半缺一半"的日子,而且对补不出来的票每次都空跑重试。
现在改成**物化一张真值表 (`trading_calendar`),所有检查/补全都对照它做**。

---

## 2. 核心架构:真值表为中心

```
  权威源 (Tushare)                        物化真值                 用途
  ─────────────────                       ──────────              ──────
  stock_basic  ──┐  上市/退市日(roster)
  suspend_d    ──┤  当天停牌
  daily(按日)  ──┼──►  复合 reconcile  ──►  trading_calendar  ──►  检查 / 补全 / 回测
  ───────────    │     (每天每只定状态)      (每天×每只一行)        都对照它
  backtest_daily ┘  库里实际有什么
```

- **`trading_calendar` 是索引/真值**:每个「交易日 × 股票」一行,记录这只票这天**应该**是什么状态。
- **`backtest_daily` 是数据**:库里**实际**存了什么(有行=有数据)。
- **检查/补全 = 拿"应该"(日历)比"实际"(backtest_daily),差异就是要补/要纠的。**
- 两张表职责分开:日历能表达"该有、但真缺"(那种情况 `backtest_daily` 根本没有行)。

---

## 3. 数据源与各自的坑

| 源 | 接口 | 用途 | 坑(必读) |
|----|------|------|-----------|
| 上市/退市日 | Tushare `stock_basic`(L+D 两次调用) | roster 基准 = 权威在册名单 | 含北交所;由 `scripts/load_listing_from_tushare.py` 灌入 `stock_listing_info`,**无需 kimi 逐只查** |
| 停牌 | Tushare `suspend_d`(按日) | 当天停牌名单 | — |
| 日线 | Tushare `daily`(**按交易日整批**) | 当天真有成交的票 + OHLCV | **按日期查,自带所有板块**(主板/创业板/科创板/北交所);单位**手**,读层 ×100 转股;**接口最新只到 T-1**——今天的日线还没发布,补全/重建的结束日默认必须 = 昨天(T-1),否则会把今天整批误标 missing |
| 分钟 | Tushare `stk_mins`(**按单只代码**) | 分钟 bar | 按代码查,代码后缀错=查空(见 §8 北交所 .BJ) |
| 三合一快照 | `stock_snapshot` = bak_basic ∪ daily ∪ suspend_d | 历史每日候选(旧审计用) | 真值表的 roster 现在用 `stock_listing_info` 而非 snapshot |

---

## 4. 三阶段流程(每阶段只发一条飞书:成功与否 + 摘要)

1. **阶段1 索引建设** —— 重建 `trading_calendar`:对照 Tushare 重新核每天每只的状态。
   `POST /api/audit/calendar/rebuild`。
2. **阶段2 补日线** —— **读索引**,只补 `daily_state=missing`(真缺),跳过 `ok` 和 `source_none`。
   `POST /api/audit/backfill-daily`(只日线,不碰分钟)。
3. **阶段3 补分钟** —— 同理,补分钟(依赖日线先齐;分钟"该补谁"是从日线推的)。**(待建)**

**手动触发 = 重新做一遍索引(阶段1),然后补全(阶段2/3)对照新索引。每日增量也从索引起步。**

> 飞书噪音治理:补全过程中的逐日"停牌记录/数据异常/lifecycle"**不再发**(`download_prices(quiet=True)`
> → reporter `silent_feishu()`,feishu=None)。每阶段只发一条由端点自己拼的总结。

---

## 5. 真值表 schema 与状态定义

表 `trading_calendar`(GreptimeDB):

| 列 | 含义 |
|----|------|
| `stock_code` | PRIMARY KEY (tag) |
| `ts` | TIME INDEX = **交易日本身**(epoch ms)。`(stock_code, 交易日)` 唯一一行,重建某天**原样覆盖**。**绝不用写入时刻当 ts**(否则同码多版本、读出来闪——见 §8 listing 双行 bug) |
| `listed` | 当天是否在册(roster=true / orphan=false) |
| `trade_status` | `trading` 正常交易 / `suspended` 停牌 / `unknown` 源头无法定性 —— 这只票这天**共享**的标记 |
| `daily_state` | 见下表 |
| `minute_state` | **预留**:`ok` / `missing` / `source_short`(源头不足241) / `pending` |
| `reason` | 备注 |

`daily_state` 取值(= "不全的原因"):

| 值 | 含义 | 可补? |
|----|------|-------|
| `ok` | 库里数据和源头对得上 | — |
| `missing` | 源头(Tushare daily)**有**、库里**没有** → 真缺 | ✅ 补(阶段2) |
| `wrong_suspended` | 库标了停牌、但源头当天**有成交** → 该是真实行 | ✅ 纠正(阶段2 重下真实行覆盖占位) |
| `wrong_traded` | 库有真实行、但源头当天**查无**且停牌 → 可疑 | ⚠️ 核查 |
| `orphan` | 有数据、却**不在当天在册名单**(未上市/已退市) | ⚠️ 核查(多为退市边界) |
| `source_none` | 在册、不停牌、源头**也查无** → 不可抗 | ❌ 跳过(见 §7) |

---

## 6. 复合逻辑 (reconcile) —— 全用权威源,不推断

`src/data/services/trading_calendar.py::reconcile_day()`(纯函数,单测覆盖)。每个交易日 D:

```
roster      = stock_listing_info 中 list_date ≤ D < delist_date 的代码
suspended   = Tushare suspend_d(D)
traded      = Tushare daily(D) 中成交量>0 的代码
db_normal   = backtest_daily(D) 中 is_suspended=false
db_suspended= backtest_daily(D) 中 is_suspended=true

对 roster 里每只:
  若 traded:    trade_status=trading
                daily_state = ok(库有真实) / wrong_suspended(库标停牌) / missing(库无)
  elif suspended: trade_status=suspended
                daily_state = ok(库有占位) / wrong_traded(库有真实) / missing(库无)
  else(在册,源头既无成交也未停牌):
                trade_status = suspended(库有占位) 否则 unknown
                daily_state  = wrong_traded(库有真实) 否则 source_none

对"库有数据但不在 roster"的: listed=false, daily_state=orphan
```

要点:**"它到底交易没交易"以 Tushare daily 为准**;库里有没有、标得对不对,是拿库去比这个权威结果。

---

## 7. 存全 + 读时筛(关键原则)

- **写入不筛**:凡当天在任一源冒头的代码都进表打状态。**写入阶段不做"该不该要"的判断**——
  历史上反复踩的坑就是"筛选时悄悄把行漏掉"(`is_suspended=NULL` 被 `WHERE` 排除、北交所被排除)。
- **筛选只在"当索引用"时(读)发生**,是这张全量表的视图:
  - 「当天该有日线的名单」= `listed=true AND daily_state≠source_none`
  - 「当天能交易的名单」= `trade_status=trading`
  - 「当天数据问题」= `daily_state IN (missing, wrong_suspended, orphan)`

## 7.1 索引驱动的补全(阶段2 行为)

- 补全**读索引**(`get_calendar_fillable_by_date()`),对两种"重下即可修"的(天,股)动手:
  - `missing` —— 库里根本没有这只的当天行 → 按那天 `daily(D)` 重下、插库。
  - `wrong_suspended` —— 库里是停牌占位、但源头当天其实在交易 → 重下真实行**覆盖占位**。
    这只代码已在库里(占位),所以补全**不把它放进 skip_codes**(`skip = 已有 - 可补`),
    让真实 bar upsert 盖掉占位(`insert_daily_record` 按主键+时间索引覆盖)。
- `ok` 跳过(已齐);`source_none` 跳过(源头不可抗,**下次也不空跑重试**)。
- **`source_none` 为什么能安全地长期跳过、又不会永久放弃**:每次"阶段1 建索引"都重新拿 Tushare 核一遍——
  源头还是没有就保持 `source_none`、补全跳过;**哪天源头真有了,重建会把它从 `source_none` 翻成 `missing`,
  下一轮补全自动捡起来补**。即"源头没有就不空跑,源头一有就自动补"。

---

## 8. 关键修复与教训(改之前先看这条,别重蹈)

- **北交所代码后缀必须 `.BJ`(`tushare_realtime._to_ts_code`)**:北交所 = `4xxxxx / 8xxxxx / 92xxxx`。
  曾把"非6开头"一律拼 `.SZ` → `920000.SZ` 是不存在的代码 → Tushare 返回空 → 按代码查的**分钟**全空,
  被误判成"源头没有北交所"。其实 Tushare **有**(920000 在 2026-04-15 有 241 根分钟)。日线不受影响
  (按日期整批拉)。
- **`stock_listing_info` / `trading_calendar` 的 ts 必须是"业务日期/固定值",不能是写入时刻**:
  GreptimeDB 按 `PRIMARY KEY + TIME INDEX` 去重。listing 表曾用 `now_ms` 当 ts → 同一只票老占位行
  (verified=false)和新行(verified=true)成了两行 → 读出来随机挑一行 → 状态数字来回闪。修法:
  listing 用固定 `ts=0`;calendar 用 `ts=交易日`;重建前对全量表先 truncate。
- **飞书消息有体积上限,超了会被静默拒收**:大报告(每天几百代码全塞一条)发不出去,`send_message`
  还闷头重试 20 次、错误被吞 → "啥都没有"。修法:报告瘦身(逐日上限 + 每类代码上限)+ 硬截断
  (`_FEISHU_MAX_CHARS`)+ 完整明细落文件。
- **Tushare `trade_cal` 返回的是 `YYYY-MM-DD`(带横杠)**:别用 `%Y%m%d` 解析(会崩在第一个日期)。
- **缓存调度器 (`cache_scheduler.check_and_fill_gaps`) 只认"整天一行都没有"的缺口**,
  看不见"有一半缺一半"的部分缺口 → 这类历史窟窿它永远不补。部分缺口的补全只发生在
  `download_prices → audit_daily_gaps → _fill_partial_gaps` 里,或现在的索引驱动补全里。
- **watchtower(机器 2026-06 升级后能自动部署了)CI 一绿就重启容器**,会**打断正在跑的长补全/重建**。
  规则:跑长任务期间不推代码;先把代码 push 稳定,再触发长任务。见 MEMORY.md。
- **调度器:不在 startup 补全,只在 3am 补,且只补最近窗口(`AUTO_FILL_LOOKBACK_DAYS=30` 天)**。
  教训:`CACHE_START=2023` 后,启动补全里 `find_minute_gaps` 扒出 2023→今天一大片历史分钟缺口 →
  每次重启就想下几小时分钟 → 把 kimi/路径B(`cache_fill_running` 互斥)死卡。watchtower 还频繁重启,
  于是**每次重启都卡一次**。所以:**① 干脆不在 startup 跑补全(重启不再触发任何 fill,kimi 立刻可用);
  ② 3am 那次也只顶最近 30 天**。历史补全(日线/分钟)是 deliberate 端点的事;手动 `/api/cache/trigger`
  仍走全量(`start_date=None → CACHE_START`)。
- **历史日线曾只存主板**:创业板(300/301)/科创板(688)/北交所(920)在早期一大段没下进来
  (2026-03-03 实测:Tushare 有 5472 只交易,库里只 3194)。根因 = 部分缺口没人补 + 名单不全无法定性。
  靠"干净 roster + 索引驱动补全"修掉。

---

## 9. 表与键一览

| 表 | 内容 | 键 |
|----|------|----|
| `backtest_daily` | 日线 OHLCV + is_suspended | `(stock_code, ts=交易日)` |
| `backtest_minute` | 分钟 bar | `(stock_code, ts=bar时间)` |
| `stock_snapshot` | 每日 B∪D∪S 三源并集 | `(stock_code, ts=日)` |
| `stock_listing_info` | 每代码 上市/退市日/verified/source | `stock_code`,ts 固定 0 |
| `trading_calendar` | **真值表**:每(日×股)状态 | `stock_code` + ts=交易日 |

---

## 10. 端点一览(都在 `src/web/audit_routes.py`,前缀 `/api/audit`)

| 端点 | 阶段 | 作用 |
|------|------|------|
| `POST /listing-info/load-tushare` | 前置 | 从 Tushare stock_basic 灌权威上市索引 |
| `POST /calendar/rebuild?start=&end=` | 阶段1 | 重建真值表(默认 2023-01-01~今天) |
| `GET  /calendar/status` | — | 查真值表:分状态/分交易状态计数 + 日期范围 + 是否在重建 |
| `GET  /calendar/problems?state=&limit=` | — | 列出某状态(missing/wrong_suspended/orphan/source_none…)的具体(日期×代码),供核查/修复 |
| `POST /backfill-daily` | 阶段2 | **默认索引驱动**:补真值表 `missing`+`wrong_suspended`(先跑阶段1);`ok`+`source_none` 跳过 |
| `POST /backfill-daily?mode=full&start=&end=` | bootstrap | 旧的全量审计式重下(建底/扩新范围,默认 CACHE_START~今天) |
| `POST /diagnose-gaps` | — | 逐日诊断报告(问题→根因→正确数字→修法)→ 飞书 |
| `POST /listing-info/verify-problems?states=&max=` | kimi兜底 | 把真值表 source_none/orphan 代码喂 kimi 查清「这代码是什么、现在什么情况」 |
| `GET  /listing-info/kimi-raw?code=` | **可观测** | 拉某代码上次 kimi 验证的**完整原始输出(工具调用全过程)**——"查不到"时调它看 kimi 到底做了什么,**别猜** |
| `GET  /listing-info/findings[?code=]` | **可观测** | kimi 逐只查到的**"怎么回事"清单**(名字/状态/一句话说明/上市退市日)——这才是放 kimi 上去的目的 |

**kimi 兜底的教训(别把大模型捆死、别靠"查不到"猜)**: kimi 本能查到北交所代码(本地实测 920039=国义招标),
但服务端 path-B 曾(1)prompt **强制 SearchWeb-only** + `--no-thinking` → SearchWeb 一挂就"查不到",不会绕道;
(2)容器**没 curl** → kimi 的 Shell 退路也断。修法:prompt 放开用全工具(搜索失败→FetchURL 抓财经页→Shell)、
去掉 `--no-thinking`、镜像装 curl,并**把每只代码的 kimi 原始 trace 落盘 + 开 `kimi-raw` 接口**——
以后失败先调 trace 看,不猜。

**kimi 要自己把「怎么回事」写进报告,不是给我私下看(2026-06)**: 放大模型上去的目的,是让它**逐只查清**
这些异常代码到底是什么(公司名)、现在什么状态(在交易/已退市/迁去新代码/更名)、关键日期,并把这句
**人能看懂的结论直接写进报告**。所以 prompt 除了上市日,还要 kimi 回 `status`+`note`(一句中文说明)+
`delist_date`;每只的结论落盘 `data/audit/kimi_findings/<code>.json`,飞书报告**逐条列出 kimi 的「怎么回事」**,
完整清单走 `findings` 接口。**禁止**我自己拿 Tushare/网页去逐只核异常代码当结论——那又变成"我猜/我查",
kimi 就白放了。`delist_date` 查到即写回 `stock_listing_info` → 下次重建,这些"有数据却不在册"的退市/迁移老代码
被名单正确排除,orphan 自动收敛。

**kimi 认证 = 静态 API key,告别 OAuth(2026-06)**: 旧方案上传 OAuth 凭证,access_token 只活 **15 分钟**、
refresh 设备绑定,无人值守跑着 refresh_token 一失效就废、只能人工重登(这正是之前 path-B 老报认证失败的根因)。
新方案:一把 **Kimi-Code API key**(`sk-kimi-…`,授权 `api.kimi.com/coding/v1`),容器启动时
`kimi_config.ensure_kimi_config_from_env()` 读环境变量 `KIMI_API_KEY` **现生成** `~/.kimi/config.toml`——
provider `type="kimi"` + `api_key=<key>` + **不写 `oauth`**(源码:无 oauth 就直接用明文 key),且
`[services.moonshot_search]`/`[services.moonshot_fetch]` 配同一 key → **原生 SearchWeb/FetchURL 照常能用**
(实测同一 key 同时授权聊天端点和搜索端点,920039 端到端跑通)。key **只走 `KIMI_API_KEY` 环境变量**(线上写在
gitignore 的 `docker-compose.yml`),不进仓库不进镜像;缺 key → 不启动调度器 + 告警。**静态 key 永不过期、零交互
登录**,15 分钟过期那套故障路径整个消失。**教训**:别给无人值守常驻任务用 15 分钟过期、要交互续期的凭证。

> 索引驱动补全 = `CachePipeline.fill_daily_from_calendar()`(读 `get_calendar_fillable_by_date()` = missing∪wrong_suspended,
> 复用 `_process_daily_date` 写真实行+停牌占位)。补全只改 `backtest_daily`,**不更新日历**——
> 要刷新索引状态,补完再跑一次阶段1(重建)。收敛环:阶段1 建 → 阶段2 补 → 阶段1 重建确认。

> 默认范围由 `cache_scheduler.CACHE_START_DATE`(= 2023-01-01)统一,改它即改全部默认;`?start=` 可补更早。
