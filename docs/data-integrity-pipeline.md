# 数据完整性管线架构 (Data Integrity Pipeline)

> **本文是 [`backtest-data-engine.md`](backtest-data-engine.md)(英文·回测数据引擎总架构)的深入细节篇**
> ——专讲真值表 reconcile 规则与全部踩坑教训(§8)。先看那篇拿整体心智模型 + 模块地图,本文再深挖。
>
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
3. **阶段3 补分钟** —— 同理,**跟着索引走**:reconcile 顺手填 `minute_state`(当天**日线交易了**的票
   才该有分钟,分钟 bar ≥241=`ok`、否则 `missing`);索引驱动按 `minute_state=missing` 的(天,股)用
   `stk_mins` **按股**重下覆盖(resume 按股、按股 auto-commit)。**不预判半天交易**——重下后源头真给不满
   241(新股首日/复牌)才标 `source_short`、不再重试(跟日线 `missing→source_none` 一个道理)。
   增量并进 3 点流水线(日线那套后追加),全量建底走手动端点。

**手动触发 = 重新做一遍索引(阶段1),然后补全(阶段2/3)对照新索引。每日增量也从索引起步。**

> 飞书噪音治理:补全过程中的逐日"停牌记录/数据异常/lifecycle"**不再发**(`download_prices(quiet=True)`
> → reporter `silent_feishu()`,feishu=None)。每阶段只发一条由端点自己拼的总结。

### 4.1 每日自动数据维护(统一流水线,凌晨 3 点)

过去拆成「3 点缓存补全 + 4 点 kimi 核名单 + 手动刷名单 + 手动重建」四块各跑各的,还要互斥锁防抢。
现在**捏成一条**:`CacheScheduler` 每天凌晨 3 点按顺序跑完整个收敛环,4 点那条独立调度**退休**
(kimi 变成其中一步)。一句话定位:**增量查漏(只把新交易日补进索引)、再精准补缺。**

```
① 刷名单      run_load_listing                  —— 新股/退市进 stock_listing_info;保留 kimi 占位行
② 核身份      kimi verify_unverified            —— 只核新代码 + 回填 code_alias(接住换号);通常很少
③ 查漏(增量)  build_calendar(max_date+1→T-1, with_minute) —— 只 reconcile 新交易日的日线+分钟状态,信任历史索引
④ 补日线      fill_daily_from_calendar          —— 只补 daily_state=missing/wrong_suspended(绝不全量重下)
⑤ 补分钟      fill_minute_from_calendar(每晚上限) —— 只补 minute_state=missing;逐批心跳日志;超上限下次续补;源头不满 241 → 标 source_short
⑥ 确认        build_calendar(④⑤补过的天, with_minute) —— 重建被补过的天,确认缺口归零 + 持久化 source_short
⑦ 一条飞书汇总(逐步结果 + 最终分类计数 + 点名待修代码)
```

设计要点:
- **③ 是增量,不是全量(关键)**:真值表是**物化**的——历史做对一次就存着、之后直接信。3 点只 reconcile
  `max_date+1 → T-1`(基本就昨天)。**绝不每晚全表重扫 2023→今**:那既慢,又会把历史里 GreptimeDB
  没落地删除 / 坏 tag 索引留下的「幽灵行」反复重造成 orphan(见 §8 的 300114 案)。**全量/历史重建是
  手动端点**(`/calendar/rebuild`)的事——建底、重组换号、或发现索引有毛病时手动跑一次。真值表为空时
  ③ 不自动全量建底(发警告、等手动),免得每次重启偷偷重扫全史。
- **补缺精准**:④/⑤ 只动索引标了缺的那几只那几天,不是 `mode=full` 的全量重下;日线通常每天 0 缺口 → 几乎瞬间;
  ⑥ 只重建被补过的天(日线+分钟一起确认),无缺口则跳过。
- **顺序跑 → 不再需要互斥锁**(当初 `cache_fill_running` vs kimi 的锁,是两者分开跑才会抢)。
- **步骤失败隔离**:某步失败 → 记日志 + 进汇总 + 继续;两个例外:**① 刷名单失败 → ③④⑤⑥ 全跳过**
  (刷名单是 truncate→重灌,失败/超时可能把 `stock_listing_info` 留成残缺半截;在残名单上 reconcile
  会把全市场判成 orphan——而 orphan 是 purge 可删的,绝不能发生);**③ 失败 → 跳过 ④⑤⑥**(不在过期
  索引上乱补)。另有兜底:`build_calendar` 自带**名单守卫**(listing 行数 < 3000 直接报错,见 §8)。
- **kimi 无开关**:装了 + 有 `KIMI_API_KEY` 就跑;用不了 → 汇总里报「⚠️ 警告」(不被静默关掉)。
- **只在 3 点跑,不在 startup 跑**(watchtower 频繁重启,见 §8)。
- **分钟线在本流程,但带每晚上限**:⑤ 跟着索引补当晚该补的分钟,设上限 `NIGHTLY_MINUTE_MAX_CODES`
  (≈2.4 个交易日的量)。稳态每天就一天约 5000 只(天)、十几分钟补完;万一积压(分钟久没跑),当晚补到
  上限就停、汇总报「还剩多少下次续补」,**绝不无人值守跑几小时**。`fill_minute` 内部**逐批心跳日志**
  (docker logs 看得到在动 = 慢≠挂——2026-06「手动触发像卡死」其实是在真下整天 5000 只,这是当时的修法)。
  **一次性历史建底**仍走手动 `POST /backfill-minute`(无上限·后台 create_task)。手动 `POST /api/cache/trigger`
  也是后台跑(不再同步阻塞 HTTP,免再次卡住请求)。逐日刷屏的老快照诊断报告已**不再每晚发**(它走旧的
  `audit_daily_gaps`、跟真值表两套打架);每日就这一条真值表汇总、末尾点名待修代码,深挖走手动
  `POST /api/audit/diagnose-gaps`。
- 手动端点(`/calendar/rebuild`、`/backfill-daily`、`/listing-info/*`)全保留。

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
| `minute_state` | **当天日线交易了的票才填**(没交易=`NULL`,不该有分钟):`ok`(分钟 bar ≥241) / `missing`(<241,可补) / `source_short`(重下后源头真给不满 241=半天交易,不再补) |
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
traded_zero = Tushare daily(D) 中有真实价格 bar 但成交量=0 的代码(一字板无成交等)
db_normal   = backtest_daily(D) 中 is_suspended=false
db_suspended= backtest_daily(D) 中 is_suspended=true

算"交易了" = code in traded,或 (code in traded_zero 且 不在 suspended)
  —— 与写入端 _process_daily_date 完全同口径:vol=0 真实 bar 且未停牌 → 存真实行;
     vol=0 且停牌 → 存占位。口径不一致时,这类真实行会被永久误判 wrong_traded。

对 roster 里每只:
  若 算"交易了": trade_status=trading
                daily_state = ok(库有真实) / wrong_suspended(库标停牌) / missing(库无)
  elif suspended: trade_status=suspended
                daily_state = ok(库有占位) / wrong_traded(库有真实) / missing(库无)
  else(在册,源头既无成交也未停牌):
                trade_status = suspended(库有占位) 否则 unknown
                daily_state  = wrong_traded(库有真实) 否则 source_none

对"库有数据但不在 roster"的: listed=false, daily_state=orphan

minute_state 只对成交量>0 的票要求(vol=0 没有分钟成交可言,强求 241 根只会空转重试)。
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
- **旧缓存调度器 (`check_and_fill_gaps`) 只认"整天一行都没有"的缺口**,看不见"有一半缺一半"的部分缺口
  → 这类历史窟窿它永远不补。**已废弃**:每日自动流程(§4.1)改走真值表——重建(查漏)精确到每只每天,
  索引驱动补全(`fill_daily_from_calendar`)按真值表只补 `missing`/`wrong_suspended`,部分缺口不再漏。
- **watchtower(机器 2026-06 升级后能自动部署了)CI 一绿就重启容器**,会**打断正在跑的长补全/重建**。
  规则:跑长任务期间不推代码;先把代码 push 稳定,再触发长任务。见 MEMORY.md。
- **调度器只在 3am 跑、不在 startup 跑(watchtower 频繁重启的教训,仍然成立)**:`CACHE_START=2023` 后,
  startup 跑全量会**每次重启都重来一遍**;旧坑里 startup 的 `find_minute_gaps` 还会扒出一大片历史分钟缺口、
  下几小时、把 kimi 死卡。现在统一流水线(§4.1)**只在 3am 触发,无 startup 跑**,重启不再触发任何重活。
  **为什么现在敢全量查漏**:查漏(重建)只是 reconcile + 有界 Tushare 调用(全历史 ~1640 次,几分钟);
  补缺是索引驱动(只补查出来的几只,通常每天 0 缺口);分钟线已移出本流程(阶段三)→ 当初"只顶最近 30 天
  避免拖垮 kimi"的约束随之消失(kimi 现在是流水线内的第②步,没有独立 loop 可被卡)。
- **历史日线曾只存主板**:创业板(300/301)/科创板(688)/北交所(920)在早期一大段没下进来
  (2026-03-03 实测:Tushare 有 5472 只交易,库里只 3194)。根因 = 部分缺口没人补 + 名单不全无法定性。
  靠"干净 roster + 索引驱动补全"修掉。
- **北交所 2025-10-09 全面改代码 → 老代码(43x/83x/87x)整体迁到 920x(2026-06 大坑)**:
  Tushare **把这些公司的全部历史(含 2023)都重挂到 920 新代码下,老代码彻底查不到**
  (实测:`430198.BJ` 2023H1 返回 0 条,`920198.BJ` 返回 118 条;`daily` 按交易日在**任何**日期都只
  返回 920 代码;`stock_basic` 里 920198 在市、原始上市日 2020-07-27,而 430198 根本不在名单)。
  后果:kimi 把老代码查清后写回名单 → 重建时老代码"在册却源头永远查不到"(daily 不返回它)→
  **source_none 暴涨 5.5 万**。**正解 = 认 Tushare 为准、统一到 920**:`load-tushare` 重载权威名单
  (老代码自动消失)+ `purge-orphan-rows` 删老代码冗余日线(数据已在 920 下)。**别去补老代码**(Tushare 没有)。
  同理 300114(中航电测)2025 重组更名迁 302132、个股退市后残留占位行,都靠"权威名单 + 删孤儿"收敛。
- **代码对应表 `code_alias`(老号→新号)接住"重组改名换号"(2026-06)**:北交所那种数据商已把历史迁
  新号(老号冗余)→ 删即可、不复发。但**个股重组改名换号**(如 300114→302132)数据商**历史仍挂老号**,
  删了就丢真历史。正解 = 一张 `code_alias` 表:① 入库 `_process_daily_date` 用它把记录的老号**改写成新号
  再存**(`code_alias` 参数;表空时 no-op)→ 历史落到在册新号下、永不成孤儿;② path-B 的 kimi 查到迁号/换号
  会**自动回填新号**进表(prompt 多问一个 `new_code`,findings 带它,跑完 `upsert_code_alias`)→ 接住未来;
  ③ 端点 `GET/POST /api/audit/calendar/code-alias` 看/手填。**这张表单独存,load-tushare 刷名单不碰它**
  (必须活下来)。302132 在 Tushare 上市日被回填成原始 2010-08-27,所以老号历史重归新号后落在在册窗口内 = 正常。
- **补全别盲信 Tushare `suspend_d`(2026-06)**:Tushare `daily` 与 `suspend_d` 会**自相矛盾**——
  实测 `688435 @2023-01-19` daily 有成交量(确实交易了),`suspend_d` 却也列它为停牌。补全若按
  `suspend_d` 写成停牌占位 → 把"交易过的票"存成停牌(`wrong_suspended`,且回测数据错)。修法:
  `_process_daily_date` 里**有真实成交(volume>0)就写真实行,无视 suspend_d**(只有真没成交才回退占位)。
  另外 `suspend_d` **仍返回老北交所代码**(43x/83x/87x,没像 daily 那样迁 920)→ 给这些**不在册**的码
  写停牌占位 = 凭空造 orphan。修法:`_process_daily_date` 收一个 `roster`(当天权威在册集),
  **只给在册的码写停牌占位**;三个调用点(索引补全/部分缺口/全量补全)都把 roster 传进去。
- **重建真值表必须"写前先清当天旧行",否则 de-roster 的代码残留(2026-06 大坑)**:
  `upsert_trading_calendar` 原本是纯 INSERT,按 `(代码,日期)` 只**覆盖**它重新算出的行。一旦某代码
  **离开名单**(如 load-tushare 用 920 替换老代码),reconcile 不再产出它 → 它的旧行(source_none 等)
  **永远残留**,重建多少次 source_none 都降不下来(卡在 5.5 万)。修法:`upsert_trading_calendar` 写前先
  `DELETE FROM trading_calendar WHERE ts=当天`,再插 reconcile 结果,使每天真值表与 reconcile 完全一致
  (空 rows 也照清)。**教训:做"快照/重算"型写入时,要么先清范围再写、要么显式删差集,纯 upsert 会留残留。**
- **GreptimeDB 等号查询会漏「坏 tag 索引」的行,全表扫描却读得到(2026-06,300114 案,折腾最久的一个)**:
  300114 重组迁 302132 后,backtest_daily 残留一条 `300114 @ 2024-06-06` 的**真行**,但它的 tag 索引坏了:
  `SELECT … WHERE stock_code='300114'`(走 tag 索引)**查不到**(于是被一路误判成"没数据的幽灵"),
  可 reconcile 的 `WHERE ts=…`(**全表扫描**)**读得到** → 每次重建都把它判成 orphan;删了真值表标记,
  下次全量重建又从这条真行盖回来。**教训**:① **按 tag 等号查不到 ≠ 数据不存在**;reconcile/DELETE 走的是
  全表扫描,真相以全表扫描为准,别信等号 SELECT 的 "No Data"。② 删这种行用**带 ts 的等号 DELETE**
  (`DELETE … WHERE ts=… AND stock_code='X'`)能删掉(DELETE 也走全表扫描、够得着),删完重建即净;实在
  删不动再 `ADMIN flush_table` + `ADMIN compact_table` 落地。③ **根上的防线 = 每日只增量重建(§4.1)**:
  历史索引建对一次就信、不每晚全表重扫,这类历史脏行就不会被反复重造成 orphan。
- **Tushare daily 一次空响应 → reconcile 把整天好状态刷成 wrong_traded(2026-06,守卫已加)**:
  重建/确认某天时,`fetch_traded(day)` = 调 Tushare `daily`。Tushare 偶发返回 `code=0` 但空数据
  (瞬时抖动,常在密集调用如分钟补全 barrage 后),`fetch_daily` 按约定返回 `[]` → reconcile 看到
  "这天没人交易",把库里**本来在交易**的几千只全判成 `wrong_traded`(源头查无),好端端的真值表被一次
  源头打嗝刷烂(实测 2026-06-03:5511 只 ok → wrong_traded;真实日线/分钟数据没丢,只状态表错)。
  **守卫(2026-06 二次收紧)**:`build_calendar` 的 fail-safe —— 某天 `traded` 为空、而 roster 非空 →
  判为**取数失败**而非真·无交易日,**跳过该天 reconcile(不覆盖)** + 记 ERROR + 进 `skipped_days`,③/⑥
  汇总里标「⚠️ 跳过 N 天」(绝不静默)。喂进 build_calendar 的天本来就出自 `trade_cal`(真交易日),
  真交易日不可能零成交,所以这个条件没有误拦。**初版守卫曾要求"库里已有 ≥100 行真实日线"才拦——那对
  「全新交易日」不生效**(3 点 ③ reconcile 昨天时补全还没跑、库里 0 行):一次空响应会把整个 roster 写成
  source_none,而 source_none 不算 problem、汇总标"可接受"、增量重建永不回访这天 → **整天数据静默永久
  缺失**。去掉库行数条件后,新天也受保护;被跳过的新天因 max_date 没推进,下晚 ③ 自动重试。
  **教训**:① 外部源返回空 ≠ 真的没有;**写真值表前要分清"源头确实没有"和"这次没取到",后者必须跳过
  而不是拿空覆盖**(CLAUDE.md 交易安全:数据不全就跳过,别拿空兜底)。
  ② 守卫条件要对"表还是空的第一次"也成立——只保护"已有数据"的守卫,保不住最关键的增量新天。
  ③ 这种 wrong_traded 是**状态腐蚀、非数据丢失**(`wrong_traded` 不进补全集、也不被 purge 自动删),
  Tushare 恢复后**重建那天**即复原(`POST /calendar/rebuild?start=&end=&with_minute=1`)。
  ④ 分钟补全 barrage(stk_mins 上百次)易触发 Tushare 限频/抖动 → 当晚可能只补一部分(其余 missing
  下晚续补)+ 抬高紧接其后的 daily 取数抖动概率;每晚 ⑤ 带上限、⑥ 有守卫,所以慢/部分/抖动都不致命。
- **刷名单是 truncate→重灌,中途被打断 = 残名单;在残名单上 reconcile 会把全市场判成 orphan(2026-06,预防性修)**:
  `load_listing` 先 `TRUNCATE stock_listing_info` 再重灌 ~7000 行。重灌途中崩溃/被 ① 的步骤超时 cancel →
  名单只剩半截甚至全空;若 ③ 照常增量重建,roster 里没有的正常代码全部写成 `orphan`——而 orphan 正是
  `purge-orphan-rows` 的删除对象,操作员照汇总清理就会**删掉一整天好数据**。三层防:**(a)** 流水线 ① 失败
  → ③④⑤⑥ 全跳过(见 §4.1);**(b)** `build_calendar` 名单守卫:listing 总行数 < `MIN_LISTING_GUARD=3000`
  (正常 L+D ≈ 7000+)直接 RuntimeError,手动重建端点同样被保护;**(c)** `upsert_listing_info` 改成
  ≤200 行/条的多行 INSERT(原先逐行),truncate→重灌窗口从分钟级缩到秒级。**教训**:对"先清后写"的权威表,
  下游读它做判定前要先问一句"这表现在完整吗"。
  北交所迁号后,`code_alias`(老→新)虽由 kimi 填好,但**只接到"下载 re-key"一处**;`load_listing`/
  `roster_for_day`/`effective_universe`/`audit` 都不读它 → 迁移老码仍按 list_date 在册、daily 查无 →
  永久 `source_none`,且每晚被当"未验证/source_none"喂回 kimi。更坑的是 kimi 把"迁移新代码"判定**当成
  正常上市码、以 verified=True+list_date 写回名单**,亲手把老码顶成在册(死循环)。**通用修(四处)**:
  ① `load_listing` 读 code_alias,**凡 old_code 一律不进 listing_info**(一处生效、roster/effective/audit 全排除);
  ② 验证器遇到带 `new_code` 的迁号 finding **只写 code_alias、把老码写成 list_date=None 的"已迁移"占位**(绝不写在册行);
  ③ `get_unverified_codes_in_snapshot` 排除 code_alias old_code(不再喂 kimi),`verify-problems` 默认 states
  去掉 `source_none`(数据缺口不是身份问题、kimi 填不了);④ 每晚 `alignment_suspects` 守卫:**kimi 给了上市日
  但 Tushare 无数据**的 source_none 码 = 疑似对齐缺陷 → 飞书告警(正常应为空,出现即新偏差)。**教训**:对应关系
  建好了**没接到决定"在不在册"的链路上 = 等于没用**;`source_none` 是数据缺口、别拿身份工具(kimi)去追。
  B 类(新码挂原始上市日、但 Tushare 该新码无迁移前历史,如 920039)是**真·源头也无**,接受为 source_none、不追。

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
| `POST /calendar/rebuild?start=&end=` | 阶段1 | 重建真值表(默认 2023-01-01~今天)。`?with_minute=1` 同时算 minute_state(阶段3 建底,每天一次分钟 GROUP BY、慢) |
| `GET  /calendar/status` | — | 查真值表:日线分状态(`by_daily_state`)+ 分钟分状态(`by_minute_state`)计数 + 日期范围 + 是否在重建 |
| `GET  /calendar/minute-coverage?date=` | 可观测 | 某天 `backtest_minute` 行数(默认 max_date)。**看分钟补全是否在涨**(进度,非终态;`by_minute_state` 要补完确认才翻) |
| `GET  /calendar/problems?state=&limit=` | — | 列出某状态(missing/wrong_suspended/orphan/source_none…)的具体(日期×代码),供核查/修复 |
| `POST /backfill-daily` | 阶段2 | **默认索引驱动**:补真值表 `missing`+`wrong_suspended`(先跑阶段1);`ok`+`source_none` 跳过 |
| `POST /backfill-minute` | 阶段3 | **索引驱动**:补真值表 `minute_state=missing`(先跑 `rebuild?with_minute=1` 建底);stk_mins 按股重下,补完确认重建标 ok/source_short |
| `POST /backfill-daily?mode=full&start=&end=` | bootstrap | 旧的全量审计式重下(建底/扩新范围,默认 CACHE_START~今天) |
| `POST /calendar/purge-orphan-rows[?execute=1]` | 清理 | 删真值表 `orphan` 对应的 backtest_daily 行(只删孤儿行,退市股保留退市前历史)。不带 execute = dry-run 出清单 |
| `POST /calendar/purge-codes-data {codes:[...]}` | 清理 | 按显式代码清单删其全部日线(删整只死代码用;上限 500) |
| `GET/POST /calendar/code-alias` | 对应表 | 看/手填老号→新号(重组改名换号);kimi 自动回填,入库时据此改号 → 老号历史归新号、不成孤儿 |
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
