# Feature Specifications

> **Important**: This document is the single source of truth for feature requirements.
> Always update this document BEFORE implementing any new feature or change.

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 0.23.4 | 2026-07-08 | - | AST-001 新命令 **`/帮助`**(别名 /help、/能力):@机器人 发 /帮助、只 @ 不带命令、发不认识的命令/自由文本 → 都秒回**能力列表**。列表从命令表(`SLASH_COMMANDS` 的 desc 字段)自动生成——以后加新命令,/帮助 自动跟着更新。帮助是静态元信息,派单器直接回,不进 kimi 队列。 |
| 0.23.3 | 2026-07-07 | - | AST-001 **白名单功能屏蔽**(用户拍板:目前不需要)——群本身即信任边界,能 @到机器人的人都能用;白名单不再是启动必要条件,设置页卡片撤下该字段。**休眠≠删除**:显式配了(env/接口)仍然生效,以后要启用直接填回;每次请求照旧记 open_id 审计日志。 |
| 0.23.2 | 2026-07-07 | - | AST-001 助手配置**改走 Settings 页**(用户要求,不放 docker-compose):设置页新增「飞书 AI 助手」卡片(App ID/Secret/白名单/只读 Key,新端点 `GET/POST /api/settings/assistant`),落盘 `data/assistant_config.json`(挂载卷),环境变量降为首次引导兜底(每字段:文件 > env)。**配齐保存即热启动**(不用重启);白名单/只读 Key 派单器按次读取、保存立即生效;App ID/Secret 绑在长连接线程上、改动需重启(卡片有说明)。空字段保存=保持原值;秘钥状态只回打码。 |
| 0.23.1 | 2026-07-07 | - | AST-001 **M1 实现**(待真实凭证联调):`src/assistant/` 派单器(飞书长连接关进守护线程——lark SDK 在 import 时绑事件循环,必须线程内自建 loop 再首次 import;白名单/event_id 去重/队列上限 4/秒回)+ `kimi-skills/check-holdings`(/持仓 技能,CI 用 kimi 自带解析器校验 + Dockerfile COPY 进镜像)+ 助手只读 key(仅 `GET /api/trading/holdings` 认,7 个鉴权单测锁死)+ **全局 kimi 串行锁**(`src/common/kimi_lock.py`,流水线②/AI 写日志/助手三方共锁,按单个任务持锁、长批间可插队)。启动守卫细化:助手环境变量一个没配=没打算开,只记日志;配了一部分缺一部分才发飞书告警(防 watchtower 每次重启骚扰)。新依赖 lark-oapi。 |
| 0.23.0 | 2026-07-07 | - | AST-001 **飞书 AI 助手**方案立项(设计文档,未实现):容器内 kimi-cli 当"大脑",飞书机器人长连接对话;已知需求走技能(kimi-cli 原生 skills,与 Claude Code 格式兼容),未知需求 kimi 现场实现。技能在仓库 `kimi-skills/` 编写、随镜像 CD(push→CI 绿→watchtower 自动部署=技能上线,CI 校验坏技能不出镜像)。硬性定位:只读助手(查数/分析/报告),白名单用户,禁交易/部署/写库;kimi 全局串行锁(与流水线②、NOTE-002 共用)。首个里程碑 M1:群里 @机器人 发 `/持仓` 查当前持仓——**用户拍板斜杠命令也走完整 kimi 技能链路**(不走原生捷径,从第一天验证全链);持仓查询给助手专用只读 key(`ASSISTANT_READONLY_KEY`,只认查询端点),交易 key 绝不交给 kimi。分期:M1 技能库基建+机器人骨架+/持仓 技能 → ②自由文本通道 → ③自学技能层。 |
| 0.22.0 | 2026-07-07 | - | TRD-001 **主页账户概览模块**:dashboard 新增账户概览卡片——①持仓明细表(股数/成本/现价/市值/浮动盈亏,数据来自 broker 30s 轮询缓存,持仓缓存补 `last_price`);②净值曲线(inline SVG,无第三方库);③每周收益率(近 12 周,ISO 周,周末快照环比)。数据底座:新表 `account_equity_snapshot`(每账户每北京日一行,ts 固定为北京日 00:00 的 UTC epoch ms,broker 轮询成功后节流 ≥5 分钟 upsert 覆写,当日最后一笔即收盘值);历史无法回填,曲线从上线当天开始积累。新端点 `GET /api/trading/equity-curve`(X-API-Key),holdings 端点补成本/市值/盈亏字段。快照写失败只告警不打断持仓轮询(轮询是交易路径)。 |
| 0.21.0 | 2026-07-05 | - | NOTE-002 **AI 交易日志代写**上线:补作业页「AI 写日志」按钮 → `POST /api/notes/ai-journal/run` 后台批量代写正文为空的买卖事件(对内+对外两栏,仿用户手写范文)。策略票判定查 159 选股接口(`/api/v16/scan-history?date=`,买入日在 V16 Top-10 才代写);**不在榜单的绝不冒认策略信号**,归 manual_list 留用户手动写。事实包=榜单+本机日线/分钟线+FIFO 配对推算收益;kimi 串行+时间预算+单飞;只写仍为空的正文(写回前二次确认);收益只进文本不回填 realized_pnl 字段。 |
| 0.20.1 | 2026-07-05 | - | NOTE-001 逆回购**佣金(手续费)可填**(用户核实逆回购有手续费):commission 列买卖/逆回购通用、不参与收益互斥;过户费/印花税/股息对逆回购仍拒收;类型互转时佣金保留、其余照旧清空。补作业逆回购待补判定改 = **佣金+收益**都要填;三栏页/补作业逆回购行佣金格开放。 |
| 0.20.0 | 2026-07-05 | - | NOTE-001 逆回购收益改存**独立列 `repo_income`**,与平仓收益 realized_pnl **物理隔离**(用户要求 100% 任何情况分得清)。硬性不变量(存储层守卫强制):逆回购行 realized_pnl/费项/股息 恒 NULL;非逆回购行 repo_income 恒 NULL;**类型切换时不属于新类型的数值自动清空、绝不搬家**;客户端往错误字段塞非空值 → 400 拒收。UI 仍共用一个「收益」输入格,按行类型落到对应列。幂等 ALTER 兼容老库。 |
| 0.19.2 | 2026-07-05 | - | NOTE-001 补作业表头「平仓收益(卖)」改**「收益」**(tooltip: 卖出=平仓收益;逆回购=利息收益)——括号里的"卖"让用户以为逆回购不能填这列,实际逆回购的利息收益就登记在这一列。 |
| 0.19.1 | 2026-07-05 | - | NOTE-001 三栏页/补作业页 HTML 响应加 `Cache-Control: no-cache`——页面 JS 内嵌,浏览器启发式缓存让用户部署后拿旧脚本(0.19.0 上线后补作业看不到逆回购行即此因),强制每次重新验证。 |
| 0.19.0 | 2026-07-04 | - | NOTE-001 新事件类型**逆回购**:QMT 仍按"卖出成交"送数(不动接收路径),写笔记时按代码自动识别(沪 204xxx / 深 1318xx)记成 逆回购,side 置空、title「逆回购 @利率 x 数量」。逆回购不算买卖:只记**收益**(存 realized_pnl 列,净到手),不填费项;补作业待补判定=收益没填;导出/核算按 event_type 区分、收益照常进 realized_pnl 汇总。三栏页蓝 tag + 编辑表单只给 价格/数量/收益;把类型改成 逆回购 会自动清掉 side。存量 11 笔 204001 误记卖出的部署后转换。 |
| 0.18.10 | 2026-07-04 | - | NOTE-001 事件/卡片删除从软删改**硬删**:`DELETE /api/notes/.../events|cards/...` 现在物理 `DELETE FROM`,按 (code, event_id/card_id) 连带清掉历史墓碑/影子行(QMT 故障产生的错误成交记录能真正删掉,不再永久躺库)。删前整行内容写日志留审计痕迹。编辑迁移 ts 的内部软删影子机制不变。 |
| 0.18.10 | 2026-07-08 | - | TRD-001 持仓明细修假盈亏:QMT 部分券商通道 `avg_price` 回 0,0 被当真成本 → 亏损仓显示成「+整个市值」假盈利(002851 实测,用户发现)。修正:成本价三级来源——broker `avg_price`(>0) → **交易日志买入记录加权摊算**(最近买入往回摊到持仓股数) → null 显示 `--`;现价缺失用 市值÷股数 换算。 |
| 0.18.9 | 2026-07-04 | - | NOTE-001 补作业表格加**方向键单元格导航**(类 Excel):上/下任何格直接跳行(屏蔽 number 加减值/select 换选项);左/右 number 直接跳格、text 光标到头才跳(全选态视为在边上)、date/time 不劫持(原生切段);行首尾绕行;禁用格自动跳过;跳入自动全选。Shift/Ctrl/Alt 组合不劫持。 |
| 0.18.8 | 2026-07-04 | - | NOTE-001 补作业表格改「滚动卡片」:表格装进固定高度卡片(`max-height: calc(100vh−250px)`),横竖滚动条都在卡片自己边上(此前横向滚动条在 121 行表格的文档底部,根本够不着);表头卡片内吸顶(`border-collapse: separate` 保住吸顶时的边框线)。列宽从 min-width 改**固定紧凑宽度**——浏览器默认 date/number 控件把列撑到两倍肥。 |
| 0.18.7 | 2026-07-04 | - | NOTE-001 待补判定再修:查线上真实数据核实,用户券商交割单**不单列过户费**(已填全的行 transfer_fee 全为 NULL),判定不再要求过户费。必填费项=买入:佣金;卖出:佣金+印花税。 |
| 0.18.6 | 2026-07-04 | - | NOTE-001 补作业「待补」判定修正:只看**费项**,且买卖费项不同——买入 = 佣金+过户费,卖出 = 佣金+过户费+印花税。此前错把平仓收益也当必填,费用已填全、只差没算收益的卖出行被误列为待补。平仓收益是算出来的、股息是偶发的,均不进判定。 |
| 0.18.5 | 2026-07-04 | - | NOTE-001 补作业页面改进(用户实测反馈):①去掉日期区间选择——打开页面自动加载**全部**待补记录(`events-range` 不带参数=全部事件,`list_events_in_range(None, None)` 不加时间过滤);「只看待补的」切换从内存缓存重画、不重复拉库,保存成功会同步回缓存。②修表格挤压:去掉 `width:100%` 让表格按列需要撑开、容器横向滚动,数字输入去掉步进箭头,每列给最小宽度(类型/价格列原来挤到只剩一个字)。 |
| 0.18.4 | 2026-07-04 | - | NOTE-001: 新增「补作业」页面 `/trade-notes/backfill`——按日期排列的表格式批量补录界面,用于事后手工补上历史买卖流水和佣金/过户费/印花税等费用(对着券商 App 成交记录一条条抄)。加载区间内已有 买入/卖出 事件为可编辑行(PATCH 更新,默认「只看待补的」——费用没填全的才铺出来),可批量加新行(POST 创建,ts 按北京时间回填);卖出行支持「算」按钮按上一次买入分摊成本算平仓收益(先找表格行,再查数据库)。配套 2 个新 API:`GET /api/notes/events-range?start&end`(区间内全部事件 JSON,北京时区)、`GET /api/notes/stock-name/{code}`。**为什么不从 QMT 自动拉**:xtquant 成交接口只返回当日成交、无历史查询,且成交回报字段本身不含佣金/手续费——历史费用只能人工补。 |
| 0.18.3 | 2026-06-08 | - | STR-004/006 修复: 区间回测资金模型适配 T+2 持仓。改 T+2 卖后,旧模型每天仍用「全部资金」买,但 T 日的票要到 T+2 才卖、T+1 根本没钱再买(等于自己骗自己)。改为**资金分 3 份轮动**:持仓 2 个交易日→任一时刻最多 3 笔重叠持仓,故初始资金均分 3 个独立子仓,按交易日序号 %3 轮流用、每日只投 1/3;第 N 天用第 N%3 份买的票在 N+2 卖,该子仓 N+3 天才轮到下一笔,资金不冲突。各子仓自负盈亏复利,`final_capital`=3 份之和;summary 增 `num_sleeves`。 |
| 0.18.2 | 2026-06-08 | - | STR-004/006: 删除区间回测的「最多 250 个交易日」截断上限——数据全在本地 GreptimeDB,没有外部 API 配额限制,逐日 SSE 流式输出不会空闲超时,任意长度区间(几年)都能跑完。原上限只是怕一次跑太久的人为软保护,并非数据限制。(注:逐日全市场扫描的单日耗时未变,长区间仍是线性时间,后续若要提速另议) |
| 0.18.1 | 2026-06-08 | - | STR-004/006: 区间回测卖出口径从「T+1 开盘」改为 **T+2 adaptive sell**,完整对齐 STR-005 实盘(买入仍 T 日 9:40):①T+1 早盘开盘比买入价低开 >3% → 当天 T+1 收盘提前止损;②否则持到 T+2 收盘卖。回测此前卖在 T+1 开盘,恰是实盘文档明令「不是」的那套。summary 增 `early_exit_days`(止损触发天数);日历缓冲 +10→+20 天,防区间末尾买入日的 T+2 跨长假被静默跳过;前端逐日日志显示真实卖出原因(T+2 收 / T+1 低开止损)+ 卖出日期。 |
| 0.18.0 | 2026-06-02 | - | DAT-006: 交易日历真值表 `trading_calendar` —— 每「交易日 × 股票」一行的物化权威真值(在册/停牌/日线状态/预留分钟状态),由 roster(Tushare 上市日) ∩ suspend_d ∩ Tushare daily ∩ backtest_daily 复合而来;存全+读时筛(视图);检查/补全改为对照它,手动触发=重建。配套: `_to_ts_code` 北交所→`.BJ`(原 `.SZ` 致分钟空);日线-only 历史回填 `POST /api/audit/backfill-daily`;诊断报告飞书消息体积上限(超限静默丢弃修复);`stock_listing_info` 固定 ts + 重建前 truncate(同码双行/读闪修复)。 |
| 0.17.0 | 2026-05-31 | - | DAT-002: 缓存补全/完整性检查结束后自动发送“按天详报”到飞书; `/api/audit/diagnose-gaps` 手动触发同一报告; 日线问题逐日展开,分钟 B 类库漏存逐日摘要,C/PENDING 类归类汇总,完整逐天明细写入 `data/audit/gap_diagnosis_report.{json,md}`。 |
| 0.16.9 | 2026-05-07 | - | NOTE-001: 买入/卖出 事件正文拆 对内 / 对外 两栏（schema 加 `content_external` 列，幂等 ALTER 兼容老部署）；新增/编辑表单都给两个 textarea，单事件查看 + 篇 view trade card 都展示双栏（两栏都填时显示「对内/对外」分节标签）。 |
| 0.16.8 | 2026-05-07 | - | NOTE-001: 手插带时间戳的卡片改为独立持久化（新表 `note_cards`，与 `trade_notes` 解耦）；带 4 个新 endpoint（GET/POST/PATCH/DELETE `/api/notes/{code}/cards`），按自己的 ts 在 篇 view 与 买入/卖出 交错排序。点卡片可直接编辑/删除（modal 重用）。每个 cmt 现在也是独立 segment（之前同一时段多个 cmt 会合并丢数据）。 |
| 0.16.7 | 2026-05-07 | - | NOTE-001: 篇 view 的 买入/卖出 卡片渲染自身的 `content`（之前只画 label+meta，content 被藏在单事件编辑器里）。卡片改成 column flex：header 行（label+meta）+ 可选 body（markdown 只读）。 |
| 0.16.6 | 2026-05-07 | - | NOTE-001: 左栏股票头部加 ✎ 按钮——纠正输错的股票代码（`PATCH /api/notes/stocks/{code} {new_code}`）。把当前股票的所有 live 事件迁到新代码，旧代码下软删；新代码若已有事件则按 ts 合并。 |
| 0.16.5 | 2026-05-07 | - | NOTE-001: 批量下单（/api/trading/buy-batch-by-amount）后自动写入 trade_notes（拉 broker.get_orders 的 FILLED 腿，按 broker_<order_id> 幂等）；同时单笔 hook 改用 upsert_broker_event_by_order_id，修掉单笔 hook + ⤓ 回补的重复计数 bug。 |
| 0.16.4 | 2026-05-07 | - | NOTE-001: 事件 tag 颜色改按 event_type 上色——买入=绿，卖出=黄（之前按 source broker/user/ai 分色，意义不直观）。来源仍可在右栏「来源:」行查到。 |
| 0.16.3 | 2026-05-07 | - | NOTE-001: 编辑表单加「类型」select——可在 买入/卖出 之间切换 event_type；后端 `update_event` 同步翻转 `side` 保持数据一致（PATCH `/api/notes/{code}/events/{event_id}` 接受 `event_type` 字段）。 |
| 0.16.2 | 2026-05-07 | - | NOTE-001: 篇 view 加 `+卡片` 按钮——弹窗输入时间+内容，作为 `【YYYY-MM-DD HH:mm】 …` 内联标记插入光标处；存为周边 `评论` 内容的一部分，不新建事件；时间戳标记在渲染时获得视觉样式。 |
| 0.16.1 | 2026-05-07 | - | NOTE-001: Add 篇 document view for trade notes: stock selection opens one contenteditable document, trade events render as locked cards, and prose persists as `评论` segments between trades. |
| 0.1.0 | 2026-01-27 | - | Initial document structure |
| 0.1.1 | 2026-01-27 | - | Add THS SDK installation scripts |
| 0.1.2 | 2026-01-27 | - | TRD-002: Add SQLite as trading data storage |
| 0.1.3 | 2026-01-27 | - | DAT-003: Message collection module implemented |
| 0.1.4 | 2026-01-27 | - | DAT-004: Real data sources (akshare announcements, CLS, eastmoney, sina) |
| 0.1.5 | 2026-01-27 | - | DAT-005: Limit-up stocks data collection via iFinD |
| 0.2.0 | 2026-01-28 | - | SYS-001/002/003: Main program, state management, scheduler |
| 0.2.1 | 2026-01-28 | - | STR-001/002: Strategy base interface and hot-reload |
| 0.3.0 | 2026-01-28 | - | STR-003: News analysis strategy with LLM (Silicon Flow Qwen) |
| 0.3.1 | 2026-01-28 | - | STR-003: Add sector buying support (multiple stocks per slot) |
| 0.3.2 | 2026-01-28 | - | STR-003: Add limit-up price check to avoid buying at ceiling |
| 0.3.3 | 2026-01-28 | - | STR-003: Add user confirmation when sector has limit-up stocks |
| 0.4.0 | 2026-02-03 | - | Architecture refactor: Message collection moved to external project, this project reads from PostgreSQL |
| 0.4.1 | 2026-02-03 | - | INF-002: Add GitHub Actions CI pipeline |
| 0.5.0 | 2026-02-03 | - | TRD-000: Trading data persistence with PostgreSQL (trading schema) |
| 0.5.1 | 2026-02-03 | - | SYS-002/DAT-005: Migrate StateManager and LimitUpDatabase from SQLite to PostgreSQL |
| 0.6.0 | 2026-02-03 | - | STR-003: Use pre-analyzed messages from external project (no LLM calls needed) |
| 0.6.1 | 2026-02-03 | - | SYS-004: Feishu alert notifications for errors and critical events |
| 0.7.0 | 2026-02-03 | - | SYS-005: Web UI for trading confirmations (containerized deployment) |
| 0.7.1 | 2026-02-04 | - | SYS-004: Enhanced startup notification with git commit info for CD tracking |
| 0.8.0 | 2026-02-04 | - | SIM-001: Historical simulation trading feature |
| 0.9.0 | 2026-02-06 | - | OA-001: Order assistant (real-time news dashboard) |
| 0.10.0 | 2026-02-12 | - | STR-004: Momentum sector strategy (backtest + intraday alert) |
| 0.10.1 | 2026-02-19 | - | STR-004: Remove gap-fade filter (ineffective in validation, precision ~50%) |
| 0.10.2 | 2026-02-20 | - | STR-004: Sync docs with code (fix scoring formula, add Step 5.5/5.6/limit-up docs) |
| 0.10.3 | 2026-03-05 | - | STR-004: Add Step 5.7 LLM board relevance filter + flip scoring to +Z(gfo)+Z(amp) + board leader bonus |
| 0.11.0 | 2026-03-18 | - | SYS-005/STR-005: Replace Strategy Status with iQuant connection status on dashboard, remove strategy controller |
| 0.11.1 | 2026-03-18 | - | SYS-005: Inline download log + calendar on backtest page, fix download_prices call, fix monitoring startup order |
| 0.11.1 | 2026-03-18 | - | SYS-005: Isolate dashboard and iQuant trading caches — dashboard download no longer affects live trading |
| 0.11.4 | 2026-03-20 | - | SYS-005: Share single cache on startup (halve memory); only copy on manual download to protect trading |
| 0.11.2 | 2026-03-19 | - | ~~SYS-005: OSS cache prefix~~ (obsolete — replaced by GreptimeDB in 0.12.0) |
| 0.12.0 | 2026-03-24 | - | SYS-005: Replace pickle+OSS backtest cache with GreptimeDB (asyncpg pgwire), no in-memory caching |
| 0.11.3 | 2026-03-19 | - | SYS-005: Real-time download progress (asyncio.Queue SSE), descriptive Chinese logs, stop download button |
| 0.12.1 | 2026-03-24 | - | SYS-005: Dashboard cache scheduler status card with toggle, next run time, last result display |
| 0.12.2 | 2026-03-26 | - | SYS-005: Cache scheduler download timeout (4h/range), per-range Feishu progress, align manual/scheduler gap detection |
| 0.13.0 | 2026-04-06 | - | STR-006: ML Scanner — 8-layer filter + LightGBM LambdaRank scoring, model management (train/finetune/S3/scheduler) |
| 0.13.1 | 2026-04-07 | - | STR-006: FC serverless async training (X-Fc-Invocation-Type: Async), remove local training code |
| 0.13.2 | 2026-04-08 | - | STR-006: Wire up ML inference — replace V15 momentum scan with LightGBM scoring (live + backtest) |
| 0.13.3 | 2026-04-17 | - | DAT-001: Fix silent skip of suspended stocks without prev_close in daily cache — always write is_suspended=true row |
| 0.13.4 | 2026-04-28 | - | STR-006: Stock-level blacklist — hardcoded codes removed at top of funnel (live scan + replicate_v16) and stripped from training data stream |
| 0.14.0 | 2026-05-04 | - | ANA-001: K-line technical analysis via overseas Lambda renderer + 柏拉图AI vision LLM (POST /api/analyze-kline) |
| 0.14.1 | 2026-05-05 | - | ANA-001: web UI Settings for lambda-kline URL/token + bltcy key (zero-restart config); orchestrator reuses `app.state.storage`; gpt-5.5-pro locked; production-verified end-to-end |
| 0.15.0 | 2026-05-05 | - | ANA-002: 盘前持仓日报——交易日 8am 自动扫描 broker 持仓→对每只调 ANA-001→飞书逐条推送 K 线 + 技术面分析；附 `POST /api/pre-market-report/run` 手动触发 |
| 0.16.0 | 2026-05-07 | - | NOTE-001: 交易笔记——三栏 master-detail（股票/事件/正文），GreptimeDB `trade_notes` 表存储；`place_order` 成功 hook 自动追加买入/卖出事件；用户/AI 可自由追加思考/复盘事件 |
| 0.17.0 | 2026-05-07 | - | SYS-005: `/api/trading/*` 加 `X-API-Key` 鉴权（`TRADING_API_KEY` 配置可选；未配置时仅在启动日志告警，配置后立即生效）；Settings 页可生成/保存 key；Dashboard JS 自动从 localStorage 注入 |
| 0.17.1 | 2026-05-28 | - | NOTE-001: 买入/卖出 表单加 佣金、过户费；卖出额外加 印花税、股息、平仓收益 + 一键「计算」按钮（按上一次买入按比例分摊成本：`(sell_qty/buy_qty) × buy_fees + buy_price×sell_qty` 与 `sell_amt − sell_fees + dividend` 做差）。schema 加 5 列 `commission/transfer_fee/stamp_tax/dividend/realized_pnl FLOAT64`，幂等 ALTER 兼容老库。 |

---

## Module: System

### [SYS-001] Application Entry Point

**Status**: Completed

**Description**: FastAPI web application as the sole entry point, serving dashboard UI, backtest API, ML strategy API, broker order API, and background schedulers.

**Entry Point**:
```bash
uv run uvicorn src.web.app:create_app --factory --host 0.0.0.0 --port 8000
```

**Startup Flow** (`src/web/app.py` → `create_app()` + `startup`):
1. Configure root logger (idempotent for pytest)
2. Mount routers (main, momentum, settings, trade-backtest, broker-order-cache, trading, model, ML, analysis, notes, audit)
3. Initialize broker client (xtquant-trade-server) + start position/order poll loops (every 30s)
4. Send Feishu startup notification
5. Start ML monitoring scheduler (daily readiness report + broker health check) — **before** cache loading + audit (safety-critical, see trading-safety-patterns.md)
6. Connect GreptimeDB storage + CachePipeline (background retry if unavailable)
7. Run trading safety audit (Feishu alert on CRITICAL)
8. Start cache scheduler (3am), model training scheduler, pre-market report scheduler (8am), intraday momentum monitor

**Files**:
- `src/web/app.py` - FastAPI application factory
- `Dockerfile` - Container build (uvicorn CMD)

---

### [SYS-002] State Management

**Status**: Completed

**Description**: Lightweight state management using GreptimeDB, JSON files, and in-memory stores.

**State Storage**:

| State | Storage | File/Table |
|-------|---------|------------|
| Backtest cache (daily/minute OHLCV) | GreptimeDB | `backtest_daily`, `backtest_minute`, `stock_list` |
| Broker positions / cash | In-memory | `app.state.broker_positions` / `available_cash` (polled from xtquant-trade-server every 30s) |
| Broker orders | In-memory | `app.state.broker_orders` (polled every 30s; fills imported into trade notes) |
| Configuration | Environment vars + YAML | `config/secrets.yaml`, `config/database-config.yaml` |
| ML models | Local files + S3 | `data/models/*.lgb` |
| Cache scheduler state | Disk file | `data/cache_scheduler_enabled.txt`, `data/last_finetune_date.txt` |

---

### [SYS-003] Trading Session Scheduler

**Status**: Completed

**Description**: Automatic detection of A-share trading sessions for session-aware scheduling.

**Requirements**:
- Detect current trading session (PRE_MARKET, MORNING, LUNCH_BREAK, AFTERNOON, AFTER_HOURS, CLOSED)
- Check if market is in trading hours
- Check if date is a trading day (exclude weekends)
- Calculate time until next session
- Callback registration for session changes

**Technical Design**:
- `TradingScheduler` class with session detection
- `MarketSession` enum for session types
- Trading hours (Beijing Time):
  - Morning Auction: 9:15-9:30
  - Morning Session: 9:30-11:30
  - Lunch Break: 11:30-13:00
  - Afternoon Session: 13:00-15:00
- Note: Holiday calendar not included (integrate with external source for production)

**Files**:
- `src/common/scheduler.py` - Trading session scheduler

**Usage**:
```python
from src.common.scheduler import TradingScheduler, MarketSession

scheduler = TradingScheduler()

# Check current session
session = scheduler.get_current_session()
if scheduler.is_trading_hours():
    # Execute trading logic
    pass

# Get time until next session
next_session, time_delta = scheduler.time_until_next_session()
print(f"Next: {next_session.value} in {time_delta}")

# Register callback
def on_change(old, new):
    print(f"Session: {old.value} -> {new.value}")
scheduler.add_session_callback(on_change)
```

**Acceptance Criteria**:
- [x] MarketSession enum defined
- [x] Session detection implemented
- [x] Trading hours check
- [x] Trading day check (weekday)
- [x] Time until next session calculation
- [x] Session change callbacks

---

### [SYS-004] Feishu Alert Notifications

**Status**: Completed

**Description**: Send Feishu (飞书) notifications when errors or critical events occur, enabling real-time monitoring of the trading system.

**Requirements**:
- Send alerts for exceptions and critical errors
- Send startup/shutdown notifications
- Configurable via environment variables
- Graceful degradation when Feishu is not configured
- Retry mechanism with exponential backoff

**Technical Design**:
- `FeishuBot` class for sending messages via external bot service
- Uses external Feishu bot relay service (LeapCell or self-hosted)
- Async HTTP requests with retry support
- Message types: text alerts, error alerts, startup/shutdown notifications

**Configuration** (Environment Variables):
| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `FEISHU_APP_ID` | No | - | Feishu app ID |
| `FEISHU_APP_SECRET` | No | - | Feishu app secret |
| `FEISHU_CHAT_ID` | No | - | Target chat ID for alerts |
| `FEISHU_BOT_URL` | No | `https://feishu-groupbot.fly.dev` | Bot relay service URL |

**Alert Types**:
| Type | Trigger | Format |
|------|---------|--------|
| Startup | System starts successfully | `✅ A股交易系统已启动\n⏰ 启动时间: {time}\n📦 版本: {commit_hash}` |
| Shutdown | System stops | `⚠️ A股交易系统已停止\n⏰ 停止时间: {time}` |
| Error | Exception or critical error | `🚨 {title}\n\n{content}` |

**CD Deployment Tracking** (v0.7.1):
- Git commit hash is embedded during Docker image build
- Startup notification includes commit info for deployment verification
- Enables tracking of which version is deployed after watchtower auto-update
- Version file: `VERSION` at image build time, read on startup

**Files**:
- `src/common/feishu_bot.py` - FeishuBot class
- `src/common/config.py` - get_feishu_config() function

**Usage**:
```python
from src.common.feishu_bot import FeishuBot

# Create bot instance
bot = FeishuBot()

# Check if configured
if bot.is_configured():
    # Send alert
    await bot.send_alert("Database Error", "Connection timeout after 30s")

    # Send startup notification
    await bot.send_startup_notification()

    # Send shutdown notification
    await bot.send_shutdown_notification()
```

**Integration Points**:
- `src/web/app.py` - Startup/shutdown notifications
- `src/web/ml_routes.py` - ML monitoring: daily readiness report + broker health alerts
- `src/data/services/cache_pipeline.py` - Download progress/failure alerts
- `src/data/services/model_training_scheduler.py` - Training success/failure alerts

**Acceptance Criteria**:
- [x] FeishuBot class with async HTTP support
- [x] Configuration via environment variables
- [x] Retry mechanism with exponential backoff
- [x] Graceful skip when not configured
- [x] Error alert method
- [x] Startup/shutdown notifications
- [x] Unit tests

---

### [SYS-005] Web UI for Trading Confirmations

**Status**: Completed

**Description**: Web-based user interaction for trading confirmations, replacing command-line stdin input to support containerized deployment.

**Description**: FastAPI web application serving dashboard UI, backtest tools, ML strategy API, broker order API, and background schedulers. All user interaction via browser; Feishu for push notifications.

**Architecture**:
```
┌─────────────────────────────────────────────────────────────┐
│                   Trading System (Container)                 │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   FastAPI    │◄──►│  Pending     │◄──►│  ML/Momentum │  │
│  │   :8000      │    │  Store       │    │  Services    │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                   │                    │          │
│         ▼                   ▼                    ▼          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  HTML Pages  │    │  Feishu Bot  │    │ BrokerClient │  │
│  │  (Jinja2)    │    │  (通知+链接) │    │ (xtquant)    │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**Pages**:

| Page | URL | Content |
|------|-----|---------|
| Dashboard | `/` | broker connection status, broker positions/cash, data engine status, model management, recommendations |
| Backtest | `/backtest` | Single-day scan, range backtest (SSE), CSV analysis |
| Settings | `/settings` | Tushare token, cache scheduler toggle, FC URL, S3 config |
| Database | `/database` | Embedded GreptimeDB dashboard |

**Key API Endpoints**:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/status` | GET | Health check |
| `/api/stock/status` | GET | Broker connection status (public, no key) |
| `/api/trading/recommendations` | GET | On-demand ML scan results (top-10) |
| `/api/momentum/tsanghi-prepare` | POST | Download cache data (SSE stream; legacy URL — daily source is now Tushare) |
| `/api/momentum/backtest` | POST | Single-day momentum scan |
| `/api/momentum/combined-analysis` | POST | Range backtest (SSE stream) |
| `/api/model/full-train` | POST | Trigger full ML training (SSE stream) |
| `/api/model/finetune` | POST | Trigger ML fine-tuning (SSE stream) |
| `/api/stock/trigger-scan` | POST | Manually run ML scan + Feishu top-5 report; **需 `X-API-Key`** (STOCK_API_KEY) |
| `/api/stock/quote` | POST | Real-time quotes for given stock codes; **需 `X-API-Key`** |
| `/api/stock/backtest-scan` | POST | Run ML backtest scan for a past trade date; **需 `X-API-Key`** |
| `/api/trading/buy` / `sell` / `buy-batch-by-amount` | POST | 下单（dashboard + 外部服务）；**需 `X-API-Key`** |
| `/api/trading/holdings` / `recommendations` / `orders` | GET | 持仓/推荐/委托查询；**需 `X-API-Key`** |
| `/api/trading/orders/{order_id}` | DELETE | 撤单；**需 `X-API-Key`** |
| `/api/settings/trading-api-key` | GET / POST | 查/设当前的 trading API key（持久化到 `data/trading_api_key.txt`） |

**Trading API 鉴权（v0.17.0）**:

所有 `/api/trading/*` 路由通过路由级 `Depends(verify_trading_api_key)` 校验请求头 `X-API-Key`。

- **Key 来源优先级**：`data/trading_api_key.txt` (Settings 页保存) > `TRADING_API_KEY` 环境变量
- **未配置时**：启动日志输出 WARNING，路由放行（保留向后兼容，避免升级即破坏部署）
- **配置后**：立即强制校验，无 / 错 key → 401
- **Dashboard JS**：从 `localStorage.tradingApiKey` 读取并注入；首次 401 → `prompt()` 让用户输入并保存
- **外部服务调用示例**：
  ```bash
  curl -X POST http://<host>:8000/api/trading/buy-batch-by-amount \
    -H 'X-API-Key: <key>' \
    -H 'Content-Type: application/json' \
    -d '{"amount":30000,"orders":[{"code":"601398","ref_price":5.20}]}'
  ```

**Configuration** (Environment Variables):

| Variable | Default | Description |
|----------|---------|-------------|
| `WEB_BASE_URL` | auto | Base URL for Feishu notification links |
| `GREPTIME_HOST` | `localhost` | GreptimeDB host |
| `FEISHU_BOT_URL` | (leapcell) | Feishu bot relay URL |
| `FEISHU_APP_ID` / `FEISHU_APP_SECRET` / `FEISHU_CHAT_ID` | - | Feishu credentials |
| `FC_URL` | - | Alibaba Cloud FC training endpoint |
| `STOCK_API_KEY` | - | `/api/stock/*` ML 策略端点鉴权 key（亦可经 Settings 持久化到 `data/stock_api_key.txt`） |
| `TRADING_API_KEY` | - | `/api/trading/*` 鉴权 key（亦可经 Settings 持久化到 `data/trading_api_key.txt`） |

**Files**:
- `src/web/app.py` - FastAPI application factory + startup lifecycle
- `src/web/routes.py` - Dashboard, backtest, settings, model management, `/api/trading/*` order routes
- `src/web/ml_routes.py` - ML strategy API (`/api/stock/*`: scan / quote / backtest) + monitoring scheduler
- `src/web/broker_order_routes.py` - Read-only cached broker order list (`/api/trading/orders`)
- `src/trading/broker_client.py` - HTTP client for xtquant-trade-server (positions / cash / orders / place / cancel)
- `src/common/pending_store.py` - Pending confirmation store
- `src/web/templates/` - Jinja2 HTML templates
- `src/web/static/` - CSS styles

**Docker Deployment**:
```yaml
services:
  trading-service:
    ports:
      - "8000:8000"
    environment:
      - WEB_BASE_URL=http://your-server:8000
      - GREPTIME_HOST=greptimedb
```

**Checklist**:
- [x] FastAPI Web application with multiple routers (dashboard, momentum, settings, backtest, broker orders, trading, model, ML, analysis, notes, audit)
- [x] Dashboard: broker connection status, broker positions, data engine card, model management
- [x] Backtest page: single-day scan, range backtest, CSV analysis
- [x] Settings page: Tushare token, cache scheduler, FC URL, S3 config
- [x] Database page: embedded GreptimeDB dashboard
- [x] On-demand recommendations (ML scan, no DB persistence)
- [x] SSE streaming for downloads, training, backtests
- [x] Feishu notifications with clickable links
- [x] Docker deployment with environment variable config

---

## Module: Strategy

### [STR-001] Strategy Architecture

**Status**: Completed

**Description**: Stateless strategy services with dependency injection. Scanners produce scored stock lists; strategy services handle data preparation and scanner invocation.

**Architecture**:
```
ml_routes.py / routes.py
        │
        ▼
ml_strategy_service.py / momentum_strategy_service.py   (data prep + invocation)
        │
        ▼
ml_scanner.py / momentum_scanner.py                      (filter pipeline + scoring)
        │
        ▼
scored stock list → dashboard / Feishu report           (returned to caller; no auto order push)
```

**Key Components**:

| Component | File | Role |
|-----------|------|------|
| `MLScanner` | `src/strategy/strategies/ml_scanner.py` | 8-layer filter + LightGBM scoring |
| `MomentumScanner` | `src/strategy/strategies/momentum_scanner.py` | 7-layer funnel + V3 regression scoring |
| `MLStrategyService` | `src/strategy/ml_strategy_service.py` | Stateless: `run_ml_live()` / `run_ml_backtest()` |
| `MomentumStrategyService` | `src/strategy/momentum_strategy_service.py` | Stateless: `run_momentum_live()` / `run_momentum_backtest()` |
| `EarlyWindowAggregator` | `src/strategy/aggregators/early_window_aggregator.py` | Aggregates raw 1-min bars → 09:31~09:40 snapshot |

**Shared Data Models** (`src/strategy/models.py`):
- `PriceSnapshot` - 9:40 snapshot (close, volume, high, low)
- `DailyBar` - Daily OHLCV NamedTuple
- `HistoricalDataProvider` - Protocol for historical data access

**Files**:
- `src/strategy/base.py` - BaseStrategy abstract class
- `src/strategy/signals.py` - TradingSignal and SignalType
- `src/strategy/filters/` - Stock/quality/reversal filters

---

### [STR-003] News Analysis Strategy

**Status**: Removed (superseded by STR-005/006)

---

### [STR-004] Momentum Sector Strategy (动量板块策略)

**Status**: In Progress

**Description**: Identifies "hot" concept boards by finding stocks with momentum after open, then selects constituent gainers from those boards.

**Strategy Flow**:
1. **Pre-filter (iwencai)**: Get main board non-ST stocks with opening gain > -0.5% (broad candidate pool)
2. **Step 1 — 9:40 Filter**: Keep stocks where (9:40 price - open) / open > 0.56%, main board, non-ST
3. **Step 2 — Reverse Concept Lookup**: For each qualified stock, find its concept boards from local `data/board_constituents.json`, filter junk boards
4. **Step 3 — Hot Board Detection**: Find boards containing ≥2 qualified stocks from step 1
5. **Step 4 — Board Constituents**: Get ALL stocks in each hot board from local `data/board_constituents.json`
6. **Step 5 — Gain Filter**: Select constituents with 9:40 gain from open >0.56%, main board, non-ST
7. **Step 5.5 — Momentum Quality Filter**: Remove stocks with abnormal early volume. Upper bound: turnover_amp > 3.0x (冲高回落 risk, based on 9-month study: >3x → avg return negative, win rate 42.9%). Lower bound: turnover_amp < 0.4x (缩量弱势, based on fine-grained sweep across 5/7/15/20d windows: Bootstrap median 0.42~0.47x, kept-group return +0.020% improvement). Turnover amp = early_volume (9:40 cumulative) / (avg_daily_volume × 0.125). Also computes trend_pct, consecutive_up_days, avg_daily_volume for Step 6
8. **Step 5.6 — Reversal Factor Filter**: Remove stocks showing 冲高回落 at 9:40 — early fade (gave back >70% of intraday surge from high) OR price position in bottom 25% of 10-min range
9. **Step 6 — Recommend (推股)**: Across all candidates:
   - Exclude stocks already at limit-up (9:40 price ≥ prev_close × 1.10)
   - Composite score = +Z(gain_from_open) + Z(turnover_amp) - cup_penalty - trend_penalty + leader_bonus
   - 涨幅靠前 + 量能充足 = 板块龙头优先（翻转自旧公式，旧公式选最弱票导致连续亏损）
   - Board leader bonus: 每个板块内 gain_from_open 最高的票加 +0.5
   - Cup penalty: 连涨天数 × 0.3（软惩罚，趋势疲劳）
   - Trend penalty: max(0, 5日累计涨幅%) × 0.05（软惩罚，高位追涨风险）
   - Pick the highest-scored stock. Highlighted in UI + Feishu notification
10. **Notification**: Send selection + recommendation via Feishu

**Data Sources**:
- Price (backtest): 日线 from Tushare Pro `daily`, 9:40 快照 from Tushare Pro `stk_mins` 1min (聚合 09:31~09:40 by `EarlyWindowAggregator`), via `GreptimeBacktestStorage` + `CachePipeline`
- Price (live): Tushare Pro `rt_min_daily`
- Concept boards: Local JSON files (`data/sectors.json` + `data/board_constituents.json`), zero runtime API calls

**Key Files**:
- `src/strategy/strategies/momentum_scanner.py` — Legacy momentum scanner (replaced by ml_scanner.py in STR-006)
- `src/strategy/filters/momentum_quality_filter.py` — Step 5.5: volume filter (turnover_amp bounds)
- `src/strategy/filters/reversal_factor_filter.py` — Step 5.6: 冲高回落 filter
- `src/data/sources/local_concept_mapper.py` — Stock ↔ concept board mapping + stock names (reads local JSON)
- `data/sectors.json` — THS concept board name list (390 boards)
- `data/board_constituents.json` — Board → constituent stocks mapping (pre-downloaded)

**Backtest Modes**:

| Mode | Description |
|------|-------------|
| **Single-day** | Run strategy for one date, show selected stocks + recommendation |
| **Range** | Run strategy for a date range (任意长度,无交易日上限——数据全在本地 GreptimeDB,逐日 SSE 流式跑完), simulate daily buy/sell with real costs |

**Range Backtest Details**:
- Input: start date, end date, initial capital (yuan)
- Each trading day: run strategy → buy recommended stock at 9:40 price, **sell via T+2 adaptive rule**(对齐 STR-005 实盘):
  - **T+1 早盘低开止损**:若 T+1 开盘价比买入价低开 > 3% → 当天 T+1 收盘卖出止损
  - **T+2 默认**:否则持到 T+2 收盘卖出(持仓 2 个交易日)
  - 两条分支都取日线收盘价模拟尾盘成交;summary 里 `early_exit_days` 记录止损触发了几天
- **资金分 3 份轮动**(持仓 T+2 占 2 个交易日,任一时刻最多 3 笔重叠持仓):初始资金均分 3 个独立子仓,按交易日序号 %3 轮流用,每日只投 1/3。第 N 天用第 N%3 份买的票在第 N+2 天卖出,该子仓第 N+3 天才轮到下一笔,资金不冲突;各子仓自负盈亏独立复利,`final_capital` = 3 份之和。**为什么是 3 份**:旧 T+1 卖时(次日开盘卖在当天 9:40 买之前)同一份钱就够轮,改 T+2 后一笔钱要压 2 天,不分仓就会"第二天没钱买"
- Trading costs:
  - **Commission**: 0.3% of trade amount, minimum 5 yuan (both buy and sell)
  - **Transfer fee**: 0.001% (1/100,000) of trade amount (both buy and sell)
  - **Stamp tax**: 0.05% (0.5/1,000) of trade amount (sell only)
- Minimum trading unit: 1 lot (100 shares)
- Buy with max affordable lots each day
- If no recommendation or capital insufficient for 1 lot: skip day
- SSE streaming for real-time progress display
- Results: per-day trade details + summary (total return, win rate, etc.)

**Checklist**:
- [x] StockFilter: add `exclude_sme` + `create_main_board_only_filter()`
- [x] FundamentalsDB: read-only access to stock_fundamentals table
- [x] LocalConceptMapper: local JSON-based stock-to-board and board-to-stock lookups
- [x] MomentumScanner: 7-layer funnel + V3 regression scoring (replaced MomentumSectorScanner)
- [x] MomentumQualityFilter: Step 5.5 volume filter (turnover_amp bounds)
- [x] ReversalFactorFilter: Step 5.6 冲高回落 filter (early fade + price position)
- [x] Feishu notification: `send_momentum_scan_result()` with recommendation
- [x] Range backtest with trading cost simulation (SSE streaming)
- [x] Unified backtest page with 3 tabs (single-day, range, CSV analysis)
- [x] Cache management scheduler (3am daily auto-fill)
- [x] Cache scheduler toggle on Settings page (enable/disable, persisted to disk)
- [x] Cache download resume: skip already-downloaded daily dates and minute stocks on retry
- [x] Cache scheduler detects minute data gaps (not just daily gaps)
- [x] Per-day minute audit + backfill: after the coarse per-stock minute download, the pipeline audits each stock's bar count per day against the expected 241 bars (9:30-15:00, verified via Tushare API). Stocks with zero bars (missing) or fewer than 241 bars (incomplete, e.g. only afternoon session) are refetched via single-day `stk_mins` calls
- [x] Cache scheduler Feishu notifications (all scenarios: start / progress / success / failure / timeout / exception / no_gaps / disabled)
- [x] Data integrity validation: stock count, per-day consistency, minute coverage
- [x] Data integrity validation on write (GreptimeDB)
- [x] Dashboard: cache scheduler status card (next run time, last result, enable/disable toggle)
- [x] Cache scheduler per-range download timeout (4 hours max)
- [x] Manual download gap check aligned with scheduler (boundaries + minute gaps via `missing_ranges()`)
- [ ] Unit tests

---

### [STR-005] Live Trading Interface (Broker / xtquant-trade-server)

**Status**: In Progress

**Description**: Live trading interface for the scanning strategy. Order execution goes through `BrokerClient` → an `xtquant-trade-server` running on a Windows QMT box (positions/cash/orders polled every 30s, orders placed via `/api/trading/*`). Recommendations are computed on demand and surfaced on the dashboard / via Feishu; auto-order push is currently disabled (`trigger-scan` returns `signal_pushed: false`).

**Strategy: Seven-Layer Parametric Funnel + V3 Regression Scoring**

| Layer | Name | Logic |
|-------|------|-------|
| L1 | Gain Filter | gain_from_open ≥ 0.2578%, main board + SME (00/60), non-ST, price ≥ ¥12 |
| L2 | Board Lookup | Reverse concept lookup via LocalConceptMapper |
| L3 | Hot Boards | Boards with ≥2 L1-qualifying stocks, exclude blacklist (物联网/医疗器械概念/特高压/冷链物流/特钢概念/三胎概念) |
| L4 | Constituent Expansion | All stocks in hot boards, re-apply L1 gain + price + ST filters |
| L5 | Volume Filter | turnover_amp = early_vol/(avg_vol×0.125), keep [0.498, 6.0] |
| L6 | Reversal Filter | ReversalFactorFilter(percentile=95, floor=0.15, min_sample=10) |
| L6.5 | Limit-Up Filter | open ≥ limit_price OR price_940 ≥ limit_price → remove |
| L6.6 | Upper Shadow Filter | (high - max(open, price_940))/open > 3% → remove (gain≥9.5% exempt) |
| L7 | V3 Score | Linear regression, pick highest-scored stock |

**V3 Scoring Model**:

| Factor | Computation | Coefficient |
|--------|-------------|-------------|
| intercept | — | +0.0106 |
| trend_10d | (close[-1]-close[-11])/close[-11] | -0.1034 |
| avg_daily_return_20d | mean(daily returns over 20d) | +1.0699 |
| intraday_range_940 | (high_940-low_940)/open | -0.3293 |
| consecutive_up_days | consecutive close > prev_close | +0.0089 |
| avg_market_open_gain | market-wide avg (open-prev_close)/prev_close | +0.4792 |
| trend_consistency | avg_daily_return / (volatility + 0.001) | +0.002 |

**T+2 Adaptive Sell**:
- **T+1 gap check** (09:25-09:35): if gap = (open - entry_price) / entry_price < -3% → mark early exit, sell at 14:50-14:58
- **T+2** (default): mark sell at gap check, sell at 14:50-14:58
- **Multi-day outage**: any holding with trading_days ≥ 2 since buy → sell

**Three-Window Scheduler**:

| Window | Time | Action |
|--------|------|--------|
| GAP_CHECK | 09:25-09:35 | Check holdings: T+1 gap → early exit; T+2 → mark sell |
| SCAN | 09:39-10:00 | If no holdings → run momentum scan → push BUY signal |
| SELL | 14:50-14:58 | Push SELL signal for all marked holdings |

**Order Flow** (via xtquant-trade-server):
1. Dashboard / external service calls `POST /api/trading/buy|sell|buy-batch-by-amount` with `X-API-Key` (TRADING_API_KEY)
2. `BrokerClient` forwards the order to xtquant-trade-server, which places it through QMT
3. A 30s poll loop (`_broker_order_poll_loop`) refreshes `app.state.broker_orders`; new fills are imported into trade notes
4. A 30s poll loop (`_broker_position_poll_loop`) refreshes `app.state.broker_positions` / `available_cash`
5. Positions/cash are held only in memory (no holdings file); they are re-fetched from the broker on every restart

**Data Sources**:
- Real-time quotes: Tushare via `TushareRealtimeClient`
- Historical data (37d): **临时**走 Tushare `daily` 实时并发拉 (`_fetch_history_live` in `ml_strategy_service.py`),解耦 cache 风险;cache 完整闭环后会回退到 `GreptimeBacktestStorage.get_multi_day_history`
- prev_close: live from Tushare `daily` (never cached — see `_resolve_prev_close`)
- Board data: `LocalConceptMapper` (local JSON files)
- Trade calendar: Tushare `trade_cal` API (cached in memory)

**Monitoring & Alerting** (Feishu notifications):

| Alert | Trigger | Content |
|-------|---------|---------|
| 每日就绪报告 | 09:30 | Broker就绪状态、当前持仓数、今日计划 |
| Broker未就绪 | 交易时段内 `broker.is_ready()` (xtquant `/readyz`) 失败 | 检查 Windows 上 xtquant-trade-server,30 分钟冷却去重 |
| ML扫描Top5报告 | 扫描产出结果 | top-5 股票代码、价格、ML评分 |
| 扫描失败 | Exception | 完整错误堆栈 |
| Dashboard状态 | 页面每10s刷新 `/api/stock/status` | Broker 在线/离线、持仓数、可用现金 |

**Dashboard Recommendations (On-Demand Compute)**:
- `GET /api/trading/recommendations?date=YYYY-MM-DD` — returns top-10 ML scan results
- **Past dates**: uses GreptimeDB cache via `run_ml_backtest()`, ~1-3s
- **Today**: uses Tushare `batch_get_minute_bars` (rt_min_daily) → `EarlyWindowAggregator` via `run_ml_live()`, ~30-60s (only after 09:39)
- No PostgreSQL persistence — computed on-demand each time

**Key Files**:
- `src/strategy/strategies/ml_scanner.py` — ML 8-layer filter + LightGBM LambdaRank scoring
- `src/strategy/ml_strategy_service.py` — Stateless ML scan service (backtest + live)
- `src/web/ml_routes.py` — `/api/stock/*` ML strategy API + monitoring scheduler (readiness + broker health)
- `src/web/broker_order_routes.py` — Read-only cached broker orders (`/api/trading/orders`)
- `src/trading/broker_client.py` — HTTP client for xtquant-trade-server (positions/cash/orders/place/cancel)
- `src/web/routes.py` — Dashboard routes + `/api/trading/*` order placement, delegates scan to ml_strategy_service
- `src/web/app.py` — Broker init + position/order poll loops; GreptimeDB cache injection into ML router
- `src/data/clients/greptime_historical_adapter.py` — Historical data read adapter (HistoricalDataProvider)

**Differences from STR-004**:
- Pure parametric (no LLM board relevance filter)
- V3 regression scoring (not Z-score composite)
- T+2 adaptive sell (not T+1 sell at open)
- 7-layer funnel with tighter thresholds (gain 0.2578% vs 0.56%, price ≥ ¥12, turnover [0.498, 6.0])
- Includes L6.5 limit-up filter + L6.6 upper shadow filter (not in STR-004)
- Includes SME (002) stocks (STR-004 excludes them)
- Single position only (STR-004's backtest also holds 1 stock but uses different position management)

**Checklist**:
- [x] MomentumScanner: 7-layer funnel + V3 scoring
- [x] Broker order routes (`/api/trading/buy|sell|batch`) via BrokerClient → xtquant-trade-server
- [x] Position/order poll loops (30s) into app.state
- [x] Trade calendar via Tushare trade_cal
- [x] GreptimeDB cache injection from app.py
- [x] Feishu notification
- [x] Monitoring: daily readiness report + broker health check
- [ ] Unit tests
- [ ] Production deployment verification

---

### [STR-006] ML Scanner Strategy (8-Layer Filter + LightGBM LambdaRank)

**Status**: In Progress

**Description**: New stock selection strategy replacing the momentum scanner. Uses an 8-layer parametric filter pipeline to narrow ~5000 stocks down to ~200 candidates, then scores them with a LightGBM LambdaRank model trained on historical forward returns.

**ML Model Architecture**:
- **Algorithm**: LightGBM LambdaRank (learning-to-rank)
- **Features**: 76 dimensions = 38 raw (13 basic + 14 advanced + 11 cross) + 38 Z-score normalized
- **Label**: 2-day forward return after fees (0.09%), bucketed into 5 quintiles (0-4) per day
- **Training**: ~1 year data, ~200 candidates/day, ~57k training records
- **Hyperparams**: 15 leaves, lr 0.05, 80% feature/sample fraction, L1=0.1, L2=1.0, max 500 rounds, early stop 50

**Feature Alignment (CRITICAL)**:
FC training (`serverless/app.py`) MUST replicate `lgbrank_scorer.py`'s feature computation exactly:
- **Units**: All ratios (0.01 = 1%), NOT percentages. `open_gain = (close-open)/open`, not `×100`
- **volume_amp**: Uses `×0.125` early session ratio: `vol / (avg_vol × 0.125)`
- **market_open_gain**: Average **gap** ratio `(open-prev_close)/prev_close`, NOT intraday gain
- **trend_consistency**: Sharpe-like `avg_return/(volatility+0.001)`, NOT fraction of positive days
- **Advanced features**: Numpy arrays over full history, matching `_compute_advanced_features` exactly
- **Cross features**: Match `_compute_engineered_features` exactly (e.g. `trend_acceleration = trend_10d - trend_5d`)
- **Z-score**: numpy std (ddof=0), matching `_add_zscore`
- **Reference**: `lgbrank_scorer.py` in main worktree is the source of truth for all feature formulas

**Model Management**:
- Training runs on Alibaba Cloud FC serverless (async invocation, `X-Fc-Invocation-Type: Async`)
- Flow: trading-service triggers FC → FC fetches data via callback → trains → POSTs result back
- Dashboard card with full-train / manual finetune buttons + training log display
- Auto finetune every 20 trading days at 3am (background scheduler, toggle on/off)
- Pre-training data completeness check: 3 retry gap-fill attempts, abort with Feishu alert on failure
- Model files saved locally (`data/models/`) and uploaded to S3 (by FC)
- Fine-tune: FC downloads init model from S3, no local training code
- Feishu alerts for: training success/failure, S3 upload, data issues, scheduler status

**Stock Blacklist** (added 2026-04-28):
- Individual codes that violate strategy assumptions (low-volatility heavyweights, repeat false-positive names) are hardcoded in `src/strategy/filters/stock_blacklist.py` as `BLACKLISTED_STOCKS: dict[str, str]` (code → reason).
- Applied at the top of the funnel in three places:
  - `MLScanner.build_universe` Step 0 — alongside the ST filter, before any other gate runs
  - `replicate_v16.py` Step 0 — keeps offline replication consistent with prod
  - `/api/model/training-data` NDJSON stream — strips blacklisted codes from every day's bars so the LambdaRank model never sees their data
- To add a code: edit the dict, fill in the reason, commit, push. Watchtower picks up the new image on prod within ~60s of CI passing.

**Key Files**:
- `src/strategy/strategies/ml_scanner.py` — 8-layer filter pipeline + feature engineering + model inference
- `src/strategy/filters/stock_blacklist.py` — Hardcoded individual-stock blacklist (code → reason)
- `src/data/services/model_training_scheduler.py` — Scheduler + FC async orchestration (trigger/callback/save)
- `serverless/app.py` — FC serverless training endpoint (LightGBM train, S3 upload, result callback)
- `src/strategy/ml_strategy_service.py` — Stateless runners: `run_ml_live()` + `run_ml_backtest()`
- `src/common/s3_client.py` — S3-compatible upload/download (boto3)
- `src/common/config.py` — S3 config + FC URL + finetune scheduler toggle
- `src/web/routes.py` — `create_model_router()` SSE endpoints + FC callback endpoints
- `src/web/templates/index.html` — Model management dashboard card

**Dependencies**: `lightgbm>=4.0` (FC only), `boto3>=1.35.0`

**Checklist**:
- [x] MLScanner: 8-layer filter pipeline (Step 0-7)
- [x] Feature engineering: 76 features (38 raw + 38 Z-scored)
- [x] LightGBM LambdaRank scoring (inference via load_model + score_candidates)
- [x] FC serverless training (full train + finetune, async invocation)
- [x] FC result callback architecture (data token + result token)
- [x] ModelTrainingScheduler (every 20 trading days at 3am)
- [x] S3 client for model upload/download
- [x] Dashboard model management card (train/finetune buttons, log area, status)
- [x] SSE streaming for training progress
- [x] Feishu alerts for all training events
- [x] Data completeness check with 3-retry gap-fill
- [x] Live inference: 9:39 scan via `ml_strategy_service.run_ml_live()` → `MLScanner.scan()` → LightGBM scoring
- [x] Backtest inference: `run_ml_backtest()` from GreptimeDB cache
- [x] `MLScanResult` / `MLScoredStock` dataclasses for structured output
- [x] Feishu ML top-5 report (`send_ml_top5_report`)
- [x] `/api/ml/model-info` endpoint for model listing
- [x] V15 momentum scan fully replaced by ML scan in ml_routes + routes
- [ ] Sell strategy
- [ ] Unit tests
- [ ] Production deployment

---

## Module: Trading

Trading is handled through the broker interface (STR-005). Order placement lives in `src/web/routes.py` (`/api/trading/*`) backed by `src/trading/broker_client.py`, which talks to an `xtquant-trade-server` on a Windows QMT box. Positions, cash, and orders are polled from the broker every 30s into `app.state` (no local holdings file).

### [TRD-001] 主页账户概览 (Account Overview: 持仓明细 / 每周收益率 / 净值曲线)

**Status**: Completed (2026-07-07)

**Description**: Dashboard 首屏新增「账户概览」模块,三块内容:

1. **持仓明细** — 代码/名称/股数/成本价/现价/市值/浮动盈亏(额+%)。数据来自 broker 30s 轮询缓存(`app.state.broker_positions`,本功能给缓存补了 `last_price` 字段);浮动盈亏 = `market_value − avg_price × volume`。
   **成本价三级来源(2026-07-08)**: QMT 不少券商通道 `avg_price` 回 **0**,0 当成本会把亏损仓显示成「+整个市值」的假盈利(002851 实测)。成本价按优先级取:① broker `avg_price`(>0 才有效);② **交易日志 `trade_notes` 的买入记录**——从最近的买入往回加权摊到当前持仓股数(自己每笔买卖都记着,数据本来就在手里);③ 都没有 → null,前端显示 `--`,绝不拿 0 编造盈亏。现价缺失时用 `market_value ÷ volume` 换算(QMT 的 market_value 本就是 最新价×股数)。
2. **净值曲线** — 总资产按日曲线,inline SVG 绘制(无第三方图表库)。数据来自 `account_equity_snapshot` 表。
3. **每周收益率** — 近 12 个 ISO 周(周一为一周开始,北京日期),周收益 = 本周最后一笔快照 ÷ 上周最后一笔快照 − 1;最早一周以该周第一笔快照为基数。红涨绿跌(A 股惯例,沿用页面已有 `gain-pos`/`gain-neg`)。

**数据底座 — `account_equity_snapshot` 表** (GreptimeDB):

| Column | Type | 说明 |
|--------|------|------|
| ts | TIMESTAMP TIME INDEX | 北京日 00:00 对应的 UTC epoch ms(每日固定,upsert 覆写键的一部分) |
| account_id | STRING (PK) | 资金账号 |
| trade_date | STRING (PK) | 北京日期 `YYYY-MM-DD`(读取端直接用,避免 ts 转换) |
| total_asset | FLOAT64 | 总资产(净值曲线数据源) |
| cash | FLOAT64 | 可用资金 |
| market_value | FLOAT64 | 持仓市值 |
| updated_at | FLOAT64 | 最后写入时刻 UTC epoch ms(观测用) |

**写入路径**: broker 持仓轮询 (`_broker_fetch_once`) 成功后调用,节流 ≥5 分钟一次,同一天反复 upsert(PK+ts 相同 → mito 原地覆写),当日最后一笔即当日收盘值。快照写失败**只 log + 记 `app.state.equity_snapshot_last_error`,绝不打断轮询**(持仓缓存是交易路径,快照是旁路观测)。GreptimeDB 未连接时跳过。

**已知口径限制**(如实展示,不做修正):
- 历史无法回填(broker 无历史资产接口),曲线从部署当天开始积累;
- 银证转账(入金/出金)会直接体现在总资产里,周收益率会被出入金失真——后续如需净值口径需登记现金流,暂不做;
- 非交易日若 broker 在线也会落快照(值与上一交易日持平),曲线出现平段,属实。

**API**:
- `GET /api/trading/equity-curve` (X-API-Key) → `{snapshots: [{date, total_asset, cash, market_value}], weekly: [{year, week, start_date, end_date, return_pct, end_asset}], current: {total_asset, cash, market_value, prev_close_asset, today_pnl, today_pnl_pct}}`
- `GET /api/trading/holdings` 增补字段: `avg_price` / `last_price` / `market_value` / `pnl` / `pnl_pct`(旧字段不变,向后兼容)

**Files**:
- `src/trading/equity_snapshot.py` — `EquitySnapshotStore`(建表/upsert/查询)+ `compute_weekly_returns()` 纯函数
- `src/web/app.py` — startup 建表、轮询挂快照写入
- `src/web/routes.py` — equity-curve 端点、holdings 增补
- `src/web/templates/index.html` — 账户概览模块(SVG 曲线 + 周收益 + 持仓表)

---

## Module: Data

### [DAT-001] Market Data Sources

**Status**: Completed

**Description**: Market data access through multiple sources, unified behind adapters.

**Data Sources**:

| Purpose | Source | Adapter / File |
|---------|--------|----------------|
| Backtest daily OHLCV | Tushare Pro `daily` | `TushareDailySource` via `CachePipeline` |
| Backtest minute bars | Tushare Pro `stk_mins` 1min | `GreptimeBacktestStorage` via `CachePipeline` |
| Live realtime quotes | Tushare Pro `rt_min_daily` | `TushareRealtimeClient` |
| Live prev_close | Tushare Pro `daily` (live, not cached) | `_resolve_prev_close` in `ml_strategy_service` |
| Live 37d history | **临时**: Tushare Pro `daily` 实时并发拉 | `_fetch_history_live` in `ml_strategy_service` (cache 闭环后回退 `storage.get_multi_day_history`) |
| Stock metadata | Tushare Pro `bak_basic` / `suspend_d` / `trade_cal` | `TushareMetadataSource` |
| Board/concept mapping | Local JSON files | `LocalConceptMapper` |
| Stock names | Local JSON files | `LocalConceptMapper.get_stock_name()` |

**Files**:
- `src/data/clients/greptime_storage.py` - GreptimeDB storage (asyncpg, CRUD)
- `src/data/clients/greptime_historical_adapter.py` - Read-only adapter (HistoricalDataProvider Protocol)
- `src/data/clients/tushare_realtime.py` - Tushare realtime quotes + `daily` full-market OHLCV
- `src/data/sources/tushare_daily_source.py` - Tushare `daily` full-market OHLCV
- `src/data/sources/tushare_minute_source.py` - Tushare 1-min bars
- `src/data/sources/tushare_metadata_source.py` - Stock metadata (trade_cal, suspend, stock_basic)
- `src/data/sources/local_concept_mapper.py` - Board ↔ stock mapping (local JSON)

---

### [DAT-002] Cache Pipeline (GreptimeDB)

**Status**: Completed

**Description**: Automated data download and caching pipeline. Downloads daily OHLCV (Tushare `daily`) and minute bars (Tushare `stk_mins`) into GreptimeDB for backtesting and ML training.

**GreptimeDB Tables**:

| Table | Content | Key |
|-------|---------|-----|
| `backtest_daily` | Daily OHLCV + is_suspended | `(ts, stock_code)` |
| `backtest_minute` | 9:40 snapshot (close_940, cum_volume, max_high, min_low) | `(ts, stock_code)` |
| `stock_list` | Per-day stock metadata (name, exchange, is_suspended) | `(ts, stock_code)` |

**Pipeline Phases** (`CachePipeline.download_prices()`):

| # | Phase | What it does |
|---|-------|-------------|
| 1 | `compact` | GreptimeDB housekeeping |
| 2 | `backfill` | Fix NULL `is_suspended` rows (one-time) |
| 3 | `stock_list` | Sync stock metadata per trading day |
| 4 | `daily` | Download daily OHLCV (resume by day) |
| 5 | `daily_backfill` | Audit + refetch missing stocks per day |
| 6 | `minute` | Download 1-min bars per stock (resume by stock) |
| 7 | `minute_backfill` | Audit + refetch missing (day, code) pairs |
| 8 | `download` | Final verification + Feishu report |

**Scheduling**: 凌晨 3 点 `CacheScheduler` 跑**统一的每日数据维护流水线**(toggle in Settings):刷名单(load-tushare)→ kimi 核新代码 + 回填 code_alias → 重建真值表(全历史**查漏**)→ 索引驱动**补缺**(只补 missing/wrong_suspended,绝不全量重下)→ 重建被补过的天确认 → 一条飞书汇总。查漏全量、补缺精准;步骤失败隔离;只在 3 点跑、不在 startup 跑;分钟线不在本流程(阶段三,单独触发)。详见 `docs/data-integrity-pipeline.md` §4.1。

**Gap Diagnosis Report**:
- 手动: `POST /api/audit/diagnose-gaps` 后台运行同一套诊断并发飞书。
- 自动: `CacheScheduler` 每次补全/完整性检查汇总发送后,立即运行按天详报并发飞书。
- 飞书正文: 日线问题日逐日列出“问题 / 根因 / 正确数字 / 怎么修”;分钟线只逐日展开 B 类“库漏存、可重下”的真错,C/PENDING 类按源头不足、半天/低成交量口径、待核对归类汇总,避免几百天明细刷屏。
- 完整明细: 每次运行覆盖写入 `data/audit/gap_diagnosis_report.json` 与 `data/audit/gap_diagnosis_report.md`,包含所有问题日、关键股票、分类原因、本地/源头分钟根数和修复动作。

**Files**:
- `src/data/services/cache_pipeline.py` - Download orchestration
- `src/data/services/cache_scheduler.py` - 3am auto gap-fill
- `src/data/services/cache_progress_reporter.py` - Phase tracking + Feishu notifications
- `src/data/services/download_task.py` - Background task state machine
- `src/data/clients/greptime_storage.py` - GreptimeDB storage (asyncpg pool)
- `scripts/diagnose_gaps.py` - 按天详报生成、完整明细落盘、飞书通知

**Checklist**:
- [x] GreptimeDB storage with asyncpg (3 mandatory overrides — see dev-conventions.md)
- [x] Cache pipeline with 8 phases
- [x] Cache scheduler (3am daily, configurable toggle)
- [x] Download resume by day (daily) and by stock (minute)
- [x] Per-day minute audit + backfill
- [x] Integrity validation on write
- [x] Feishu notifications for all scenarios
- [x] Post-run per-day gap diagnosis report to Feishu + full detail files
- [x] Dashboard data engine status card
- [x] Manual download with SSE progress + stop button
- [x] Suspended stock prev_close chaining (per-stock latest close, not global MAX date)

---

### [DAT-003] Stock Blacklist

**Status**: Completed

**Description**: Global stock blacklist. Blacklisted stocks are skipped in data download, scanning, and trading.

**Implementation**: Hardcoded `STOCK_BLACKLIST` frozenset in `src/common/config.py`. Applied at download, scan, and signal generation layers.

---

### [DAT-004] Listing-Info Auto-Verification (路径 B)

**Status**: Implemented (2026-05-30)

**Description**: 服务端后台 job,自动把 `stock_snapshot` 里尚未验证的代码喂给容器内 kimi-cli 查真实上市日 (IPO 首日),写回 `stock_listing_info`。`audit_daily_gaps` 用 `effective_universe = stock_snapshot − (list_date 未到 / delist_date 已过 的代码)` 算干净缺口;没有这张表,审计只能打 coverage warning。这是 CLAUDE.md §8#5 "live 扫描回退到读 cache" 三个前置条件里的第二条。

**GreptimeDB Tables** (补 DAT-002 未列的两张):

| Table | Content | Key |
|-------|---------|-----|
| `stock_snapshot` | 每日 B∪D∪S 三源并集 (bak_basic ∪ daily ∪ suspend_d) | `(ts, stock_code)` |
| `stock_listing_info` | 每代码 list_date / delist_date / verified / source | `stock_code` |

**验证逻辑**: 共享模块 `src/data/services/kimi_listing_verifier.py` 的 `run_kimi_for_code()` —— spawn `kimi --print --afk`(thinking **留开**,关了它搜索失败就放弃不绕道),让 kimi 用**全部工具**(SearchWeb→FetchURL→Shell)实证查清代码身份(在交易/退市/迁移/更名)+ 上市/退市日;解析失败**绝不猜** list_date。脱机脚本 `scripts/verify_list_date_kimi.py` 复用同一模块。

**Scheduling (已并入统一流水线, 2026-06)**: kimi 核验 `verify_unverified()` 现在是**每日凌晨 3 点数据维护流水线的第 ② 步**(`CacheScheduler` 顺序调用,刷名单后/重建前),原独立的 4 点 `ListingVerifyScheduler.run()` loop **已退休**(无独立 4 点跑、无 startup 跑)。守卫:`get_listing_verify_enabled()` 开关(管第 ② 步是否跑 kimi)、`kimi_available()`+`KIMI_API_KEY` 探测、单次 `MAX_CODES_PER_RUN`(默认 500)上限、**并发 1**、`upsert_listing_info` 小批 ≤200 行。(原 `cache_fill_running` 互斥锁已无意义——顺序跑不再并发抢内存。)`ListingVerifyScheduler` 类与方法保留,供流水线调用 + 手动端点 + 状态卡片。详见 `docs/data-integrity-pipeline.md` §4.1。

**失败处理**: 查不到/超时/解析失败 → 写 `verified=false` 占位行 (离开"未验证集",不再每天重烧 kimi) + 飞书通知该批 failed 代码清单。手动 `?include_failed=1` 可重验占位行。

**Endpoints**:
- `POST /api/audit/listing-info/verify` (X-API-Key) — 后台触发一次验证 (支持 `?include_failed=1`)
- `POST /api/audit/listing-info/verify-problems?states=&max=` — 把真值表异常代码喂 kimi 查清「怎么回事」
- `GET /api/audit/listing-info/status` — 覆盖率 + scheduler 状态 (设置页卡片消费)
- `GET /api/audit/listing-info/findings[?code=]` — kimi 逐只查到的「怎么回事」(名字/状态/说明/上市退市日)
- `GET /api/audit/listing-info/kimi-raw?code=` — 某代码上次 kimi 验证的完整原始 trace
- **认证**: 静态 Kimi-Code API key,容器启动时由 `KIMI_API_KEY` 环境变量生成 `~/.kimi/config.toml` (`kimi_config.ensure_kimi_config_from_env`);无 OAuth、无凭证上传端点 (旧的 upload/auth-bundle 端点已删)

**Files**:
- `src/data/services/kimi_listing_verifier.py` - 共享 kimi 验证 (run_kimi_for_code / finding_from_result / kimi_available)
- `src/data/services/kimi_config.py` - 启动时从 KIMI_API_KEY 现生成 kimi 配置 (静态 key,无 OAuth)
- `src/data/services/listing_verify_scheduler.py` - kimi 验证逻辑 `verify_unverified()`(现由 3 点统一流水线调用;含 kimi findings 报告)
- `src/web/audit_routes.py` - 触发 / 状态 / findings / kimi-raw 端点
- `src/data/clients/greptime_storage.py` - upsert_listing_info / get_unverified_codes_in_snapshot / get_failed_verified_codes / get_effective_universe_for_date

**Checklist**:
- [x] 共享 kimi 验证模块 (脚本 + scheduler 复用)
- [x] kimi 验证并入 3 点统一流水线第 ② 步(独立 4 点 loop 已退休;enable 开关保留)
- [x] 单次上限截断 (不静默,log + 飞书)
- [x] 失败写占位行 + 飞书 failed 清单
- [x] 手动触发端点 + 状态端点 + 设置页卡片
- [x] kimi 输出解析 + verify_unverified 流程单测

**临时工具 — 新旧索引对照验证 (TEMPORARY)**: 在用新索引 (三合一−kimi = `effective_universe`) 驱动历史数据的**补全/清理**之前,先把它跟旧索引 (之前用的 Tushare 列表 = `bak_basic` 每日权威列表) 逐日对照,确认新索引可信。
- 脚本 `scripts/compare_index_old_new.py` (`--start/--end/--feishu/--max-days`) + 临时接口 `POST /api/audit/index-compare` (X-API-Key,后台跑→飞书),共用 `compare_index_range`。
- 输出每日 `only_in_old` (新索引丢/挡的) 与 `only_in_new` (bak_basic 漏的,如北交所),delta 用 listing_info 标注原因 (未上市/已退市/未验证)。
- **修复 (补缺 + 清理 `backtest_daily` 里不该存在的行) 为后续步骤**,必须先通过本对照验证、且带人工确认闸 (删除不可逆) 才执行。确认完毕后此脚本 + 接口可删。

**stock_snapshot 历史回填**: `stock_snapshot` (索引基表) 是 2026-05-12 新增,历史几乎为空 → 索引在历史上不存在。`POST /api/audit/snapshot-backfill?start=2023-01-01&end=...` 把它回填到历史:只跑 snapshot (B∪D∪S,不碰分钟线)、resume-safe (跳已存在日期)、撞 Tushare 限频自动 sleep 续跑、后台执行起止发飞书。复用 `CachePipeline._sync_stock_snapshot`。与缓存补全互斥 (共享 `daily_source`/`metadata_source` httpx client,不能并发)。

**数据缺口诊断报告**: `POST /api/audit/diagnose-gaps` (+ `scripts/diagnose_gaps.py`) 把每日补全报的三类问题(股票数偏多 / 日线缺 / 分钟缺)逐条核查,产出"问题→根因→真实应该是多少→怎么修"的飞书报告。把每个缺的 (天,股) 用 `gap_classifier` 归成 **A 名单错**(未上市/已退市还挂在名单 → AI 上市日剔除)、**B 真缺**(在市未停牌却无数据 → 精准重下)、**C 本就无完整数据**(停牌补占位 / 上市首日半天交易记为已知正常)、**待 AI 核对**。分钟缺口天数多,详细分类抽样最近 N 天(透明标注,不静默截断)。
- 配套修复 `validate_integrity` 的"股票数"检查:从"全库历史累计 > 5500 报警"(误报,累计含退市股属正常)改为"**单日**在册数 > 5500 才报警";累计数仅作说明。

---

### [DAT-006] 交易日历真值表 (Trading Calendar Truth Table)

**Status**: In progress (2026-06-02)

**Description**: 维护一张**物化的、每天的权威真值表** `trading_calendar`,每个「交易日 × 股票」一行,如实记录这只票这天的真实状态。把"每天该有哪些票、各是什么状态"一次性算清存下,**所有缺口检查和数据补全都对照它做**,不再每次临时推断——"一查即知,不靠复杂推断"。取代 DAT-002/DAT-004 里 `audit_daily_gaps` 的即时重算 + 逐个堵窟窿。

**核心性质**: 表的"正确"= 每天的股票集合对 + 每只的真实状态如实记。某只票停牌、或源头无数据,**不影响表的正确性**——"停牌""源头无"本身就是被如实记录的状态,不是缺陷。

**复合 (reconcile) —— 全用权威源,不推断**。每个交易日 D:
1. **在册名单 (roster)** = `stock_listing_info` 中 `list_date ≤ D < delist_date` 的代码(Tushare stock_basic 官方上市/退市日)。
2. **停牌** = Tushare `suspend_d`(D)。
3. **真有成交** = Tushare `daily`(D)(整批,含所有板块)。成交量>0 是主集;**成交量=0 但有真实价格 bar**(一字板无成交等)且**不在停牌名单**的,也按"交易了"算——与写入端 `_process_daily_date` 的"有真实 bar 就存真实行"**同一口径**,否则这类票会被永久误判成 `wrong_traded`(库有真实行、reconcile 却认为源头查无)。成交量=0 且在停牌名单 → 按停牌算(写入端写的也是占位)。分钟完整性(`minute_state`)只对成交量>0 的票要求(vol=0 的票不强求 241 根,避免空转重试)。
4. **库里有什么** = `backtest_daily`(D)(区分 `is_suspended`)。
三者一碰定状态:
- daily 有真实成交(含上述 vol=0 真实 bar 且未停牌)→ `trade_status=trading`;库有真实行=`ok` / 库标成停牌=`wrong_suspended` / 库无=`missing`(真缺待补)。
- suspend_d 停牌、daily 无成交 → `trade_status=suspended`;库有占位=`ok` / 库无=`missing`(停牌没占位待补)。
- 在册、不停牌、daily 也查无 → `source_none`(源头也无,长期挂,不假装解决)。
- 有库数据但**不在当天 roster** → `orphan`(有数据却不在册,`listed=false`)。

**Schema** (`trading_calendar`, GreptimeDB):

| Column | 说明 |
|--------|------|
| `stock_code` | PRIMARY KEY (tag) |
| `ts` | TIME INDEX = **交易日本身** (epoch ms, naive-UTC)。`(stock_code, 交易日)` 唯一一行,重建某天**原样覆盖**——**不用写入时刻当 ts**(避免 `stock_listing_info` 那种同码双行/读闪 bug) |
| `listed` | 当天是否在册 (roster=true / orphan=false) |
| `trade_status` | `trading` 正常交易 / `suspended` 停牌 / `unknown` 源头无法定性 —— 这只票这天**共享**的标记 |
| `daily_state` | `ok` / `missing` 真缺 / `wrong_suspended` 标了停牌却有数据 / `orphan` 有数据却不在册 / `source_none` 源头也无 |
| `minute_state` | **预留**(后续):`ok` / `missing` / `source_short` 源头不足241 / `pending` |
| `reason` | 备注 |

**存全 + 读时筛(不在写入期筛选)**: 凡当天在任一源冒头的代码都进表打状态;写入**不做"该不该要"判断**——躲开历史上"筛选悄悄漏行"的坑(`is_suspended=NULL` 被 `WHERE` 排除、北交所被排除)。筛选只在"当索引用"时(读)发生,是这张全量表的视图:
- 「当天该有日线的名单」= `listed=true AND daily_state≠source_none`;
- 「当天能交易的名单」= `trade_status=trading`;
- 「当天数据问题」= `daily_state IN (missing, wrong_suspended, orphan)`。

**写入守卫(输入可疑就不写,宁可跳过)**:
- **空 traded 守卫**:某天(出自 `trade_cal` 的真交易日)Tushare `daily` 返回空、而在册名单非空 → 几乎可断定是源头取数失败(真交易日不可能零成交)→ **跳过该天 reconcile、不覆盖**,记 ERROR + 进 `skipped_days`(汇总可见)。不依赖"库里已有多少行"——新交易日首次 reconcile 时库里还没数据,同样受保护(否则一次空响应把整天写成 source_none,增量重建永不回头,整天数据静默永失)。
- **名单守卫**:`stock_listing_info` 行数异常少(< 3000,正常 L+D 约 7000+)→ 名单疑似被截断(如刷名单中途被打断)→ **整个 build 直接报错中止**,绝不在残缺名单上 reconcile(否则正常代码全成 orphan,而 orphan 是 purge 可删的)。
- **流水线依赖**:3 点流水线里 **① 刷名单失败 → ③④⑤⑥ 全跳过**(同 ③ 失败跳 ④⑤⑥ 的逻辑——绝不在可能残缺的名单/过期的索引上重建或补数)。

**用法**: 检查/补全对照"该有日线"视图比 `backtest_daily`,只补/只纠差异;**手动触发 = 重建索引**(重新复合);每天新补全从它起步。历史某天复合好即不变真值,只最近/新增日需重做。

**Endpoints** (规划): `POST /api/audit/calendar/rebuild?start=&end=`(后台重建→飞书);`GET /api/audit/calendar/status`(概览)。

**Files** (规划): `src/data/services/trading_calendar.py`(复合纯逻辑,可单测) / `greptime_storage.py`(建表 + upsert + 视图查询) / `audit_routes.py`(端点)。

**Checklist**:
- [ ] 建表(`stock_code` PK + 交易日 TIME INDEX,固定交易日为 ts 避免双行)
- [ ] 复合流程(roster ∩ 停牌 ∩ 成交 ∩ 库 → 状态),纯逻辑单测
- [ ] 全历史重建端点 + 状态端点
- [ ] 检查/补全改为对照真值表视图
- [ ] `minute_state` 接入(后续)

---

---

## Infrastructure

### [INF-002] CI Pipeline

**Status**: Completed

**Description**: Continuous integration pipeline for code quality.

**Technical Design**:
- GitHub Actions workflow on push/PR to main branch
- Uses `uv` for fast dependency installation
- Parallel jobs: lint (ruff), format (ruff format), typecheck (mypy), test (pytest)
- Auto-deploys trading-service Docker image and FC serverless function

**Files**:
- `.github/workflows/ci.yml` - GitHub Actions workflow

---

## Module: Analysis

### [ANA-001] K-line Technical Analysis (Vision LLM)

**Status**: Production-verified end-to-end (2026-05-05). Trigger 当前只暴露
HTTP endpoint，UI 按钮 / 飞书 / 定时任务接入在 backlog。

**Description**: 端到端 K 线技术面分析。主程序从 GreptimeDB 取最近 N 个交易日的
日 K，POST 给海外 AWS Lambda；Lambda 用 matplotlib 画图、上传公开读 S3、返回
URL；主程序拿 URL 调柏拉图AI vision LLM（OpenAI-compatible chat/completions，
**锁定 gpt-5.5-pro**——其他模型质量太差，不要切），返回中文技术面分析。

**Architecture & Why**:

```
trading-service (cn-shanghai VPC)        Lambda (us-east-1)
─────────────────────────────────        ─────────────────────────
1. POST /api/analyze-kline {code, days?, prompt?}
2. app.state.storage → OHLCV[N days from GreptimeDB]
3. POST {code, days, ohlcv[]} ─────────► 4. matplotlib 渲染 PNG
                                          5. S3 put_object (ACL=public-read)
   ◄────── { url, key } ─────────────── 6. 返回公开 URL
7. POST /v1/chat/completions (柏拉图AI)
   { image_url: { url } }
   ◄──── { analysis text }              [bltcy → gpt-5.5-pro]
```

**为什么数据国内查、画图存海外**：阿里云 OSS 大陆 region bucket 的默认域名从
2025-03-20 起对新用户禁止做数据 API 操作，必须绑定备案过的自定义域名才能给
公网（包括 LLM provider）拉图。把渲染 + 存储放到 AWS us-east-1 直接绕开 ICP
备案、`Content-Disposition: attachment` 强制下载、跨网延迟等一连串问题，
trading-service 只发 JSON over HTTPS。

**Components**:

| 部分 | 路径 | 说明 |
|------|------|------|
| Lambda 渲染服务 | `lambda-kline/` | Container Lambda，POST OHLCV → PNG → 公开 S3 URL，鉴权 `x-upload-token` |
| Orchestrator | `src/analysis/kline_llm.py` | `analyze_kline(storage, code, days, prompt?)`——**`storage` 必须由调用方传入**（复用 `app.state.storage`，不要在函数内 start/stop） |
| HTTP 入口 | `src/web/analysis_routes.py` | `POST /api/analyze-kline`，从 `request.app.state.storage` 注入连接池 |
| 配置 helpers | `src/common/config.py` | `get_/set_lambda_kline_url`, `get_/set_lambda_kline_token`, `get_/set_bltcy_api_key`, `get_bltcy_base_url`, `get_bltcy_model` |
| Web UI 配置入口 | `src/web/templates/settings.html` 「K 线技术面分析」卡片 | 三栏 URL / Token / Key，自带「测试」按钮，零重启生效 |
| Lambda CI 流水线 | `.github/workflows/lambda-kline-bootstrap.yml` | path-filtered（只在 `lambda-kline/**` 改动时跑）→ ECR build & push → `update-function-code`（function 不存在时跳过并打 warning） |
| AWS 一次性 bootstrap 手册 | `lambda-kline/README.md` | S3 / ECR / IAM role / Lambda function / Function URL 创建命令 |

**配置（trading-service 侧）** —— 优先级 持久化文件 > env var：

| 配置项 | 持久化文件（web UI 写入） | env var fallback |
|---|---|---|
| Lambda Function URL | `data/lambda_kline_url.txt` | `LAMBDA_KLINE_URL` |
| Lambda upload token | `data/lambda_kline_token.txt` | `LAMBDA_KLINE_TOKEN` |
| 柏拉图 API key | `data/bltcy_api_key.txt` | `BLTCY_API_KEY` |
| 柏拉图 base URL | （env-only） | `BLTCY_BASE_URL`（默认 `https://api.bltcy.ai/v1`） |
| 柏拉图模型 | （锁死，不要改） | `BLTCY_MODEL`（默认 `gpt-5.5-pro`，仅作应急逃生口） |

> 首选 Settings 页面配置——文件持久化重启不丢，「测试」按钮可即时验证。
> env var 留作 docker-compose 部署模板的 fallback。

**GitHub Secrets / Variables**（CI 自动构建 Lambda 镜像用）：
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` /
`AWS_LAMBDA_KLINE_ECR` / `AWS_LAMBDA_KLINE_FUNCTION` /
Repo Variable `LAMBDA_KLINE_ENABLED=true`（关闭即禁用 deploy job）。

**HTTP 接口**：

```
POST /api/analyze-kline
{ "code": "000001", "days": 30, "prompt": null }     # code 接受 6 位裸码或 .SZ/.SH 后缀（自动 strip）
↓ 200
{
  "code": "000001",
  "days": 30,
  "bars": 30,
  "image_url": "https://<bucket>.s3.us-east-1.amazonaws.com/kline/...png",
  "model": "gpt-5.5-pro",
  "analysis": "...中文分析文本..."
}
```

错误码：400 = 输入或配置缺失；502 = Lambda 或 bltcy 上游错误；503 = `app.state.storage` 未初始化（GreptimeDB 没连上）。

**默认超时**：Lambda render 60s，bltcy LLM 600s。新 prompt 下 LLM 端到端波动很
大：240s → 360s 都踩到过 timeout，600s 给足 buffer，不影响调用方（ANA-002 串行
调用，每只就算跑满 10 分钟也只有几个持仓，8am 开市前肯定跑完）。

**Backlog**:
- UI trigger（dashboard 按钮 / 飞书命令）—— ✅ 定时任务已由 ANA-002 兜住
- 渲染加分钟级、加 RSI / MACD 等指标
- 历史分析归档（落 GreptimeDB），支持 web UI 回看
- prompt 工程：把基本面 / 资金流 / 板块温度等结构化信号一起喂进去，不只看图

---

### [ANA-002] 盘前持仓日报 (Pre-Market Holdings Report)

**Status**: Completed (2026-05-05)

**Description**: 交易日早 8 点自动扫描当前 broker 持仓，对每只持仓股调用 ANA-001
生成 K 线图 + 技术面分析，并按【卖出 / 持有 / 增持】信号给出当日操作建议，逐只推送到
飞书群。**为什么放 8am 不放 15:00**：Tushare 日线接口 T-1 出数据，
`CacheScheduler` 3am 才把昨日数据补全；8am 跑可以直接用昨日收盘的完整 K 线，避免在
15:00 触发还要额外补一次今日数据。

**触发机制**:

| 入口 | 触发方式 | 备注 |
|------|---------|------|
| 自动调度 | 工作日 8:00 Beijing | `PreMarketReportScheduler.run()`，`asyncio` 后台任务 |
| 手动触发 | `POST /api/pre-market-report/run` | 立即执行一次（异步，立即返回 `{"started": true}`），**永远可用**——不受定时开关或交易日过滤影响，用于补发或调试 |
| 定时开关 | `POST /api/pre-market-report/toggle {enabled: bool}` | **仅控制 8am 自动**——关闭后 scheduler 写一行 `skipped` 日志，手动触发不受影响 |
| 持久化 | `data/pre_market_report_enabled.txt`（默认 true） | env fallback `PRE_MARKET_REPORT_ENABLED` |

**交易日判定**: 复用 `cache_scheduler._get_trading_calendar()`（Tushare `trade_cal` →
weekday fallback），周末/节假日不跑。

**消息格式**（每只持仓发 2 条飞书消息：原生图片 + Markdown 富文本卡片，串行发送）:

1. **图片消息** —— K 线 PNG 通过 `FeishuBot.send_image()` 上传到飞书
   `/api/send_image`，原生 image type，群里直接展示图片，不是 URL 链接。
2. **Markdown 卡片** —— 通过 `FeishuBot.send_markdown()` 走 `/api/send_markdown`，
   飞书富文本卡片渲染。结构：

```markdown
## 📊 {code} {name} ({YYYY-MM-DD})

| | |
|---|---|
| 持仓 | **{volume}** 股 |
| 成本 | ¥{avg_price} |
| 市值 | ¥{market_value} |

---

{LLM analysis with 卖出 / 持有 / 增持 三档信号}
```

> 早期版本曾用 text 消息 + URL 拼接，渲染丑且图片不展开，2026-05-05 改为
> 原生 image + markdown 双消息（FeishuBot 加 `send_image` / `send_markdown`）。

**边界处理**:

| 场景 | 行为 |
|------|------|
| 无持仓 | 静默跳过（不发飞书），`scheduler_log` 记 `no_positions` |
| Broker 未连接 | 发飞书失败告警 + `scheduler_log` 记 `failed` |
| 单只股票分析失败 | 跳过该股，发飞书错误消息，继续下一只（不让一只挂掉整个日报） |
| ANA-001 配置缺失（lambda/bltcy） | 发飞书告警 + `scheduler_log` 记 `failed` |
| 节假日 | 不触发 |

**速率**: 持仓串行处理（避免 LLM 同时跑撞超时），单只
~30s（图渲染 < 10s + LLM ~20s），10 只持仓约 5 分钟内全部发完。

**Files**:
- `src/data/services/pre_market_report_scheduler.py` — 主逻辑（参考 `cache_scheduler.py` 模式）
- `src/web/analysis_routes.py` — 新增 `POST /api/pre-market-report/run`
- `src/common/config.py` — `get_/set_pre_market_report_enabled()`
- `src/web/app.py` — startup 启动 task、shutdown cancel

**配置**:

| 配置项 | 持久化文件 | env var fallback | 默认 |
|--------|----------|------------------|------|
| 启用开关 | `data/pre_market_report_enabled.txt` | `PRE_MARKET_REPORT_ENABLED` | `true` |

**HTTP 接口**:

```
POST /api/pre-market-report/run                            # 全部持仓批量
POST /api/pre-market-report/run-one  {code}                # 单只持仓
POST /api/pre-market-report/toggle   {enabled: bool}       # 仅控 8am 自动
GET  /api/pre-market-report/status                         # 查状态
↓ 200
{ "started": true, "trigger": "manual" }                   # /run, /run-one
{ "enabled": true }                                        # /toggle
```

错误码：503 = `app.state.storage` 未就绪；409 = 上一次执行还没结束；
400 = `/run-one` 参数缺失。

**Dashboard UI**:「盘前持仓日报」卡片（在 index.html 主页），含开关、下次执行时间、
上次结果显示、「触发全部持仓 → 飞书」按钮、「单票 AI 评价」每只持仓一个按钮（点击
→ 推送该股票分析到飞书，30s 自动刷新持仓列表）。

**调度日志**: 与 cache/model 调度器共用 `scheduler_log` 表，name=`pre_market_report`，
result ∈ `{success, no_positions, failed, skipped}`。

---

## Module: Notes

### [NOTE-001] 交易笔记 (Trade Notes)

**Status**: Implemented (2026-05-07)

**Description**: 按股票组织的"一脉相承"交易历史 + 思考日志。三栏 master-detail UI：
左栏所有有事件的股票（按最近活跃排序）→ 中栏选中股票的全部事件（时间倒序）→
右栏选中事件的正文（markdown 渲染 + 编辑）。事件分两类：

- **自动事件（broker source）**: `POST /api/trading/buy` / `POST /api/trading/sell` 成功后，
  `routes.py` 立刻 INSERT 一行（`event_type='买入'/'卖出'`，price/qty/side 填入），
  正文为空——你点进去再补思路/反思。批量 `/buy-batch-by-amount` 暂不 hook（V1 简化）。
- **手动事件（user / ai source）**: 思考、盘中观察、复盘、AI 总结等，从前端 "+ 新事件"
  按钮或后端 API `POST /api/notes/{code}/events` 创建。

**逆回购（event_type='逆回购'，2026-07-04）**: 国债逆回购 QMT 仍按「卖出成交」送数
（接收路径不动），但**写笔记那一步**按代码识别——沪市 `204xxx` / 深市 `1318xx` —— 自动记
`event_type='逆回购'`、`side=NULL`、title「逆回购 @利率 x 数量」。逆回购**不算买卖**：
登记**佣金(手续费)+收益**：收益为利息（存独立列 `repo_income`，与平仓收益
`realized_pnl` **物理隔离**——两种收益任何情况都分得清）；佣金列买卖/逆回购通用。
**硬性不变量（存储层守卫强制）**：逆回购行 realized_pnl/过户费/印花税/股息 恒 NULL；
非逆回购行 repo_income 恒 NULL；类型切换时不属于新类型的数值自动清空、绝不搬家（佣金
通用保留）；往错误字段塞非空值 → 400 拒收。导出每行两列各自独立，核算按列取数天然
分开。手动把某事件类型改成 逆回购 时后端自动
清掉 side。三栏页蓝色 tag，编辑表单显示 价格(利率)/数量/佣金/收益；补作业页逆回购行的
待补判定=佣金+收益没填全，过户费/印花税/股息列禁用。

**Why GreptimeDB**: 数据 access pattern 是"按 code tag 过滤 + 按 ts 排序，append-mostly"，
正好命中 GreptimeDB 时序工作负载。**与之前否决用 GreptimeDB 替换 Trilium SQLite 不矛盾**——
那是 OLTP 全套（频繁 update / joins / FTS5 / triggers），这里只用 INSERT + UPSERT by
PRIMARY KEY + 简单 WHERE，完全在 GreptimeDB 舒适区。

**Schema**:

```sql
CREATE TABLE trade_notes (
    ts          TIMESTAMP TIME INDEX,        -- 事件时间
    code        STRING,                      -- 股票代码（无后缀，如 '605299'）
    event_id    STRING,                      -- uuid，编辑/删除主键
    event_type  STRING,                      -- 买入/卖出/盘中/复盘/思考/AI总结/其他
    source      STRING,                      -- 'broker' / 'user' / 'ai'
    title       STRING,                      -- 中栏短标签（如 "@18.30 x 500"）
    price       FLOAT64,                     -- 成交价（broker 事件填，其他 NULL）
    qty         INT64,                       -- 数量
    side        STRING,                      -- 'buy' / 'sell'
    content     STRING,                      -- 对内 markdown（私房话）
    content_external STRING,                 -- 对外 markdown（分享版本，仅 买入/卖出 用）
    author      STRING,                      -- 创建者标识
    deleted     BOOLEAN,                     -- 软删除（NULL 也视为未删，兼容老 ALTER）
    commission   FLOAT64,                    -- 佣金 (买/卖)，元
    transfer_fee FLOAT64,                    -- 过户费 (买/卖)，元
    stamp_tax    FLOAT64,                    -- 印花税 (仅卖)，元
    dividend     FLOAT64,                    -- 股息/股息税 净到手 (派息额 − 已扣税)，可负，仅卖出登记，元
    realized_pnl FLOAT64,                    -- 平仓收益 (仅卖)，元；按 上一次买入 比例分摊成本计算，用户可改
    repo_income  FLOAT64,                    -- 逆回购利息收益 (仅逆回购)，元；与 realized_pnl 物理隔离，守卫强制互斥
    PRIMARY KEY (code, event_id)
)
PARTITION ON COLUMNS (code) ()

-- 篇 view 手插的带时间戳卡片：和 trade_notes 解耦的独立 schema。
-- 卡片不是事件——既不出现在中栏「事件」list，也不参与买卖统计。
CREATE TABLE note_cards (
    ts       TIMESTAMP TIME INDEX,    -- 用户选的时间，决定篇 view 排序
    code     STRING,
    card_id  STRING,                  -- uuid
    content  STRING,                  -- markdown
    deleted  BOOLEAN,
    PRIMARY KEY (code, card_id)
)
```

**HTTP 接口**:

```
GET    /trade-notes                           # 三栏页面（HTML）
GET    /trade-notes/backfill                  # 补作业页面（HTML）：按日期排列的表格式批量补录
GET    /api/notes/events-range?start&end      # 区间内全部 live 事件（JSON，北京时区 ts + 股票名）；不带参数=全部事件
GET    /api/notes/stock-name/{code}           # 代码 → 股票名（补作业表格输码后确认用）
GET    /api/notes/stocks                      # 左栏：所有有事件的股票，按 last_ts DESC
PATCH  /api/notes/stocks/{code}               # 改股票代码：所有 live 事件迁到 {new_code}
GET    /api/notes/{code}/events               # 中栏：股票全部事件，按 ts DESC
GET    /api/notes/{code}/events/{event_id}    # 右栏：单条事件正文
POST   /api/notes/{code}/events               # 创建手动事件
PATCH  /api/notes/{code}/events/{event_id}    # 编辑（content/title/event_type/ts）
DELETE /api/notes/{code}/events/{event_id}    # 硬删除（物理 DELETE，含该 event_id 的全部墓碑/影子行）
GET    /api/notes/{code}/cards                # 篇 view 手插的带时间卡片
POST   /api/notes/{code}/cards                # 创建卡片（content + ts_ms）
PATCH  /api/notes/{code}/cards/{card_id}      # 编辑卡片
DELETE /api/notes/{code}/cards/{card_id}      # 硬删除卡片（物理 DELETE）
```

**自动 hook 位置**:
- `src/web/routes.py` 的两个单笔 `place_order` 调用点（buy / sell endpoint），下单成功
  立刻 `store.upsert_broker_event_by_order_id(order_id=result.order_id, ...)`。
- `submit_buy_batch_by_amount` 返回后调 `store.import_today_filled_orders(broker, code_filter=…)`
  把当批 codes 的 FILLED 订单从 broker 拉回 trade_notes。
- 所有路径都用 `broker_<order_id>` 当 event_id → 单笔 hook、批量 hook、`/backfill-today`
  三处之间天然幂等，不会重复计数。
- 简化语义：单笔下单成功即写入（提交价 + 提交数量），不等成交回报；批量下单走
  broker.get_orders() 拿真实成交腿数据。少数部分成交/撤单情况会失真，后期加 reconciliation。

**为什么不分两张表（`trade_fills` vs `trade_notes`）**: 中栏需要把自动事件和手动事件
混排按时间倒序，单表 + `source` 列查询最简单；结构化字段（price/qty/side）对 manual
事件设为 NULL 即可，GreptimeDB 原生支持。

**未持久化历史交易**: broker 接口（`get_trades`）只返回今日成交，无历史 API；且
xtquant 成交回报字段（成交价/量/金额/订单号）**不含佣金/手续费/印花税**——连当日
费用都拿不到，历史费用更无从自动补。**从部署日开始累积**——过去交易用
**补作业页面 `/trade-notes/backfill`** 批量手工补录（见下），单条也可用 "+ 新事件"。

**补作业页面（`/trade-notes/backfill`，2026-07-04）**: 按日期排列的表格式批量补录界面，
对着券商 App 的历史成交记录一条条抄。行为：

- **打开页面即自动加载全部待补记录**（无日期选择，`events-range` 不带参数拉全部
  事件）——已有 买入/卖出 事件铺成可编辑行（改完 PATCH 保存，日期/时间改动即 ts
  迁移；title 若是自动生成格式会随价格/数量同步重生成，自定义 title 保留不动）。
  给已有 broker 自动事件补费用就是在这里改。
- **默认「只看待补的」**：费用已填全的行滤掉，只铺作业。待补判定**只看必填费项**，
  按用户实际交割单口径（2026-07-04 线上数据核实）——买入必填 = 佣金；卖出必填 =
  佣金+印花税；**逆回购必填 = 佣金+收益**（过户费/印花税/股息列对逆回购禁用）。**过户费该券商不单列，
  用户不填，不拿它判缺**；平仓收益是算出来的、股息是偶发的，都不是费项、不算缺。
  取消勾选看全部
  买卖——切换从内存缓存重画（保存成功的值会同步回缓存），「↻ 重新加载」才重新拉库。
  汇总栏显示「共 N 笔买卖 + R 笔逆回购，待补 M 笔」。
- 「+ 加一行」追加空行（日期沿用上一行），填 日期/时间/代码/类型/价格/数量/佣金/
  过户费/印花税/股息/平仓收益/备注 → 保存即 POST 创建（`ts_ms` 按北京时间
  `+08:00` 显式换算，与浏览器时区无关）。代码输完自动查股票名确认没输错。
- 卖出行有「算」按钮：先在**当前表格的行里**（含未保存行）找同代码上一次买入，
  找不到（被「只看待补的」滤掉，或买入在区间之外）再查数据库该股全部事件；按
  `sell_qty/buy_qty` 比例分摊成本算平仓收益——口径与三栏页的「计算」按钮一致。
- 「全部保存」逐行顺序提交（单行 INSERT，避开 GreptimeDB 批量丢数据坑）；每行有
  状态列（未保存/已保存/失败原因）。删除：已存行走 DELETE（硬删，物理清行），未存行直接移除。
- **方向键单元格导航（类 Excel）**：上/下任何格直接跳行；左/右在数字格直接跳、
  文本格光标到头才跳、日期/时间格不劫持（原生左右切年月日段）；行首尾绕到上/下
  一行，禁用格自动跳过，跳入自动全选（直接打字覆盖）。

**Files**:
- `src/notes/note_store.py` — GreptimeDB CRUD（复用 `app.state.storage` 连接池）
- `src/web/notes_routes.py` — 7 个 endpoint（HTML + 6 个 API）
- `src/web/templates/trade_notes.html` — 三栏布局 + vanilla JS
- `src/web/templates/trade_notes_backfill.html` — 补作业表格页 + vanilla JS（样式内嵌，独立于三栏页）
- `src/web/static/style.css` — `.trade-notes-*` 三栏样式（响应式：手机下变 drill-down）
- `src/web/routes.py` — buy/sell endpoint 内嵌 hook 调用（best-effort，失败只 log
  不影响下单返回）

**Backlog**:
- 成交回报 reconciliation（修正委托价 → 实际成交价）
- AI 自动写入（接入 AI 服务，定期/事件触发自动生成"复盘"事件）
- T+n 复盘提醒（cron 扫所有股票最近一次买入，T+1/T+3/T+5 飞书推链接）
- 全文搜索（GreptimeDB 暂无 FTS，先靠浏览器 ctrl+F；未来可加 like 模糊匹配）
- 跨股票分析（"按 signal 类型回归胜率"——需要 event_type 标签更结构化）

---

### [NOTE-002] AI 交易日志代写 (AI Journal Writer)

**Status**: Implemented — 线上功能 + 离线补作业工具 (2026-07-05)

**Description**: 给 trade_notes 里正文为空的 买入/卖出 事件批量代写「交易原因」日志
（对内 `content` + 对外 `content_external` 两栏），风格仿照用户已手写的范文（范文从
库里动态抽最近手写正文，风格随用户更新）。买入原因基于当日 V16 推票（排名/板块/评分）
+ 早盘分钟走势 + CCI14；卖出原因基于 T+2 持仓原则 + 实际持仓天数/个股与大盘走势/
FIFO 推算收益，弹性叙事（提前止损/到期出货/多拿）。

**策略票判定（用户定的铁律）**: 只有「买入当日在 V16 Top-10 榜单」的买入（及买入腿
在榜单的卖出）才代写；**不在榜单的交易绝不冒认策略信号**，归入 manual_list 返回给
用户手动写。榜单按日期只读调用 **159 的 V16 回测端点**
`POST /api/v16/backtest {trade_date}`（用户定的口径：每日推票以 159 的 V16 为准；
优先取 `all_scored` 全量榜单；无鉴权、纯查询、无副作用——**对 159 只做这一种只读
调用，绝不发任何修改类请求**；地址可用 `V16_SCAN_BASE` 环境变量覆盖）。
注意该端点用**当前**板块数据重算历史，与当日实际推送可能有出入（板块表更新导致）
——用户已拍板以重算结果为准（2026-07-05）。**159 缓存缺该日期数据/接口不可达/报错
≠ 当日无榜单**——一律中止整批（否则会把策略票误判成手动票，当天已两次踩坑）。

**线上触发（补作业页按钮「AI 写日志」）**:

```
POST /api/notes/ai-journal/run     {event_ids?, max_events<=200, time_budget_sec<=6h}
GET  /api/notes/ai-journal/status  进度/结果/manual_list
```

- 守卫：kimi_available() + KIMI_API_KEY 就绪才启动；单飞（已在跑 → 409）；
  kimi 串行 + 时间预算（预算耗尽优雅停，剩的下次再点）。
- 事实包（服务端组装）：159 榜单 + 本机 GreptimeDB backtest_daily（CCI14/持仓表现，
  vol×100 手→股）/backtest_minute（早盘与卖出时段分时）+ Tushare index_daily 大盘
  （best-effort）+ trade_notes 全量 FIFO 配对。数据缺口显式写进 `data_gaps`
  告诉 kimi 不许写对应内容。
- kimi 调用复用路径 B 模式：`kimi --print --afk` + 结果临时文件（不从 trace 刮）+
  错误如实分类；kimi 工具/认证故障（KimiToolError）→ 中止整批不连环烧。
- **只写正文仍为空的事件**：写回前二次读库确认，跑批期间用户手写了就放弃该笔。
- 跑完飞书大白话总结（成功/失败/手动清单）。

**硬性规则**（`src/notes/ai_journal_instructions.md` 对 kimi 的约束）:
- 只准引用事实包里的数字，一个数都不许编；缺数据的信息不写。
- 卖出理由必须与数据自洽：T+2 到期 / <2 提前（止损/落袋）/ >2 多拿（技术面强），
  分批腿体现分批。
- 清仓收益/收益率用 FIFO 推算值（含买卖费用分摊，口径对齐用户手写范文），
  **不回填** `realized_pnl` 字段（那是系统既有计算逻辑的领地）。

**Files**: `src/notes/ai_journal.py`（服务）+ `ai_journal_instructions.md`（任务书）+
`src/web/notes_routes.py`（run/status 端点）+ `trade_notes_backfill.html`（按钮+进度）；
测试 `tests/unit/notes/test_ai_journal.py`、`tests/unit/web/test_ai_journal_routes.py`。

**离线补作业工具（历史积压用，与线上共用任务书思路）**:
`dev-tools/ai_journal/{fetch_materials,run_kimi_writer,apply_drafts}.py` —— 素材从
V16 离线复刻（`dev-tools/replicate_v16.py`，静态模型与线上排名一致）+ Tushare 拉，
本机 kimi 并发 2（启动错峰防 `~/.kimi` 写锁竞态）。复刻结果可经 main 的回填端点
上传成 159 的正式榜单数据。

---

## Module: Assistant

### [AST-001] 飞书 AI 助手 (kimi 大脑 + 技能库)

**Status**: In Progress — M1 代码已实现(单测 300 全绿),待飞书应用凭证联调 (2026-07-07)

**Description**: 把容器里已有的 kimi-cli 当"大脑",通过飞书机器人对话使用:用户在飞书发需求
→ 派单器 spawn kimi 处理 → 结果发回飞书。已知需求沉淀为**技能**(kimi-cli 原生 skills 机制,
已源码确认,格式与 Claude Code 兼容);没有技能覆盖的需求 kimi 现场实现。技能在本仓库
`kimi-skills/` 编写、随镜像发布——**push → CI 绿 → watchtower 自动部署 = 线上技能即最新**,
零线上手工操作,回滚镜像即回滚技能。

**首个里程碑 M1 (2026-07-07 定)**: 群里 @机器人 后发斜杠命令 `/持仓` → **走完整 kimi 技能
链路**(用户拍板:斜杠命令不走原生捷径,从第一天验证「@机器人→派单→kimi→技能→回复」全链)。
`/持仓` 技能指导 kimi 用 curl 调本机只读端点 `GET /api/trading/holdings`(数据与 TRD-001
主页账户概览同源,30s 轮询缓存),把持仓明细(股票名/代码/股数/成本/现价/市值/浮动盈亏)+
可用现金整理成大白话回群;broker 离线/数据拿不到 → 如实说,**一个数都不许编**(同 NOTE-002
铁律)。鉴权用**助手专用只读 key**(见安全边界),TRADING_API_KEY 绝不交给 kimi。kimi 链路是
分钟级,收到命令先秒回「收到,查询中」。(白名单已按用户决定屏蔽,见 0.23.3——群本身即
信任边界。)

**定位与安全边界 (CRITICAL,硬性护栏)**: 查数、分析、写报告的**只读助手**,不是运维机器人。
任意飞书消息驱动一个能跑 Shell 的模型,存在提示注入风险,以下限制不可协商:
- **绝不给**:下单/撤单、任何交易路径、docker/部署/重启操作、写业务库
- **助手专用只读 key** (`ASSISTANT_READONLY_KEY`):只被明确圈定的只读 GET 端点接受
  (首批:`/api/trading/holdings`);下单/撤单等修改类端点**绝不**认这把 key——kimi 拿到它
  也发不出任何交易指令。TRADING_API_KEY 永远不进 kimi 的环境/prompt/技能文本
- 数据库只读;kimi 工作目录圈死在专用临时目录
- 群聊只响应 @机器人;每次请求记发送者 open_id 审计日志。**使用者白名单已屏蔽**
  (用户拍板 2026-07-07:群本身即信任边界);休眠不删除——显式配置(env/接口)仍生效,
  以后要启用直接填回

**架构**:

```
飞书用户 ──消息──▶ 飞书开放平台
                      │ 长连接(lark-oapi SDK WebSocket;无需公网回调地址/验签)
                      ▼
              AssistantDispatcher (常驻协程, src/assistant/)
                      │ ① event_id 去重(飞书事件会重推)+ open_id 审计日志(白名单休眠)
                      │ ② 秒回「收到,处理中」 ③ 入队(kimi 单次要几分钟,不能同步等)
                      ▼
              kimi --print --afk --skills-dir /app/kimi-skills
                      │ 复用路径 B 模式:结果写临时文件(不刮 trace)、错误如实分类
                      ▼
              结果(大白话中文) ──▶ 飞书回复
```

**技能库机制** (kimi-cli 原生,源码 `kimi_cli/skill/__init__.py` 已确认):
- 一个技能 = `kimi-skills/<技能名>/SKILL.md`(YAML frontmatter `name`/`description` + 正文
  步骤说明),可带配套脚本;也支持平铺 `<名字>.md` 单文件
- kimi 启动时扫描技能目录,把「名字+路径+描述」注入 system prompt,模型匹配后自行读正文照做
- `--skills-dir` 可重复传;同名技能,先传的目录赢(为 Phase 3 双层技能留的口子)

**技能 CD 闭环** (核心诉求:本地写技能 → push → CD 完即生效):
1. 仓库根新建 `kimi-skills/` 目录,技能进版本控制
2. Dockerfile 加 `COPY kimi-skills/ ./kimi-skills/`(放挂载卷 `/app/data` 外面,同
   `bundled_data` 先例)
3. **CI 校验技能**:单测用 kimi_cli 自带解析器(`kimi_cli.skill.parse_skill_text`)过一遍
   每个 SKILL.md——frontmatter 齐全、名字唯一;坏技能 CI 直接红,不出镜像(同
   `test_kimi_cli_installed.py` 防 DOA 思路)
4. (可选加固)Dockerfile build 门:构建时跑一次技能发现,同 `RUN kimi --version` 先例

**派单器要求**:
- **统一走 kimi 技能链路**(用户拍板 2026-07-07,否决斜杠命令原生捷径):**斜杠命令** =
  确定性派单——`/持仓` 直接在任务 prompt 里指定执行对应技能;**自由文本** = kimi 自己
  匹配技能或现场实现。两者都经队列,都是分钟级,收到一律先秒回「收到,处理中」
- **kimi 全局串行**:与流水线 ② kimi 核验、NOTE-002 AI 写日志共用一把全局锁——kimi 消费方
  已有 3 个,不能再各管各的
- 时间预算 + 卡死兜底超时,沿用路径 B 铁律:**不拿超时判失败**,等 kimi 自己退出读明确反馈,
  超时只当卡死保险且保留已产出内容
- 队列长度上限,满了直接回复"排队满了,稍后再试",不静默丢
- **回复硬性规则**:大白话中文,禁英文状态码/内部代号/黑话(用户既定要求);错误如实报告
  是什么错就报什么错,绝不猜
- 任务书(dispatcher prompt)写明:先翻技能库,有匹配的照做;没有就现场实现;
  (Phase 3 后)跑通的做法存回自学技能目录

**Configuration** (0.23.2 改:**Settings 页配置为主**,用户拍板不放 docker-compose):

- **入口**: 设置页「飞书 AI 助手」卡片 → `GET/POST /api/settings/assistant`;落盘
  `data/assistant_config.json`(挂载卷,重部署不丢)。空字段保存 = 保持原值;状态查询
  秘钥只回打码。
- **生效时机**: 配齐保存**即热启动**(不用重启);白名单/只读 Key 派单器按次读取,保存立即
  生效;App ID/Secret 绑在长连接线程上,改动需重启服务。
- **优先级**: 每字段独立取值,`assistant_config.json` > 环境变量(env 只作首次引导兜底)。

| 字段 | 兜底环境变量 | Description |
|------|--------------|-------------|
| App ID / App Secret | `FEISHU_ASSISTANT_APP_ID` / `FEISHU_ASSISTANT_APP_SECRET` | 自建应用凭证(收消息+回消息;与现有推送用的 relay 应用是否同一个,接入时确认) |
| 使用者白名单(**屏蔽中**) | `FEISHU_ASSISTANT_ALLOWED_USERS` | 0.23.3 起休眠:为空 = 不限制(群里都能用),设置页无此字段;显式配了(env/接口)仍生效 |
| 只读查询 Key | `ASSISTANT_READONLY_KEY` | 助手专用只读 key,仅圈定的只读 GET 端点接受;缺省 = kimi 查不了持仓,技能如实报错 |
| (kimi 认证) | `KIMI_API_KEY` | 复用现有机制,仍走部署环境变量(kimi 配置在容器启动时生成) |

**分期实施**:
- **M1 机器人骨架 + `/持仓` 技能**(先做,含技能库基建全套——`/持仓` 走 kimi 链路,技能
  基建是前置): ① 技能库基建:`kimi-skills/` 目录 + `/持仓` 技能 + Dockerfile COPY +
  CI 校验单测 + kimi spawn 封装支持 `--skills-dir`;② 机器人:lark-oapi 长连接(新增
  pyproject 依赖)+ @机器人解析 + 白名单/event_id 去重;③ kimi 通道最小闭环:队列 +
  全局串行锁 + 卡死兜底超时 + 秒回「处理中」;④ 助手只读 key(holdings 端点)。
  启动守卫:飞书凭证/白名单/kimi 可用性任一缺 → 不启动机器人;此外在设置页配齐保存
  可随时热启动,不依赖重启
- **Phase 2 自由文本通道**: 任务书完善(技能匹配/现场实现约定/大白话输出规则)+
  时间预算 + 安全护栏加固(工作目录圈死、禁清单)
- **Phase 3 自学技能层**(可选,做前另行确认): 挂载卷目录存 kimi 现场实现后自存的技能,
  `--skills-dir` 传两层(仓库技能优先);人工定期 review,值得的转正进仓库

**Checklist**:
- [x] M1: `kimi-skills/` 目录 + `/持仓` 技能(check-holdings)
- [x] M1: Dockerfile COPY(可选 build 门未做——CI 技能校验已覆盖,build 门收益有限)
- [x] M1: CI 技能校验单测(用 kimi_cli 自带 frontmatter 解析器;另断言命令表↔技能目录一致)
- [x] M1: kimi spawn 封装支持 `--skills-dir`(`src/assistant/kimi_runner.py`)
- [x] M1: 助手只读 key(`ASSISTANT_READONLY_KEY`,仅 holdings GET;7 个单测锁死各种组合)
- [x] M1: lark-oapi 长连接接入(@解析/白名单/event_id 去重)——代码完成,待真实凭证联调
- [x] M1: kimi 通道最小闭环(队列上限 4/全局串行锁/卡死兜底 900s/秒回)
- [x] M1: 启动守卫 + 飞书告警(细化:全没配=没打算开只记日志;配了一部分才告警,防 watchtower 重启骚扰)
- [ ] M1: 真实飞书应用端到端联调(建应用/切长连接/进群实测 /持仓)
- [ ] Phase 2: kimi 任务书(技能匹配/现场实现/大白话输出规则)+ 时间预算
- [ ] Phase 2: 安全护栏加固(工作目录已圈死临时目录;禁清单已进任务书;待加固项另列)
- [ ] Phase 3: 自学技能挂载卷 + 双层 skills-dir

**实现要点(M1, 2026-07-07)**:
- **lark SDK 线程模型(踩坑记录)**: `lark_oapi` 包在 **import 时**就抓当前线程的事件循环
  (`lark_oapi/__init__.py` 第一行 `from . import ws`,ws 模块顶层 `get_event_loop()`),
  `ws.Client.start()` 是阻塞的 `run_until_complete`。在 uvicorn 主循环里 import 会把它绑死到
  uvicorn 的 loop 上直接炸。**解法**:整个长连接生命周期(含首次 import)关进守护线程,线程内
  先 `new_event_loop()+set_event_loop()` 再 import;事件经 `run_coroutine_threadsafe` 跳回
  主循环(同 baostock 子线程教训);出方向回复用 SDK 同步 REST 客户端包 `asyncio.to_thread`。
- **全局 kimi 串行锁**: `src/common/kimi_lock.py` 一把锁,流水线②(`run_kimi_for_code`)、
  AI 写日志(`run_kimi_journal`)、助手(`run_kimi_assistant_task`)三方共用;按单个任务持锁,
  夜批逐码间隙助手可插队,不会被饿几小时。
- **交易 key 隔离**: kimi 子进程环境里 `TRADING_API_KEY` 被显式剔除,只注入只读 key +
  `ASSISTANT_API_BASE`;技能文本引用环境变量而非明文 key。

**Files**:
- `kimi-skills/check-holdings/SKILL.md` — `/持仓` 技能(目录/文件名英文)
- `src/assistant/dispatcher.py` — 守护线程长连接 + 路由(斜杠命令→技能)+ 队列 + 回复
- `src/assistant/kimi_runner.py` — kimi spawn(--skills-dir/--work-dir/全局锁/结果文件)
- `src/assistant/assistant_instructions.md` — kimi 任务书(只读禁令/不编数/大白话)
- `src/common/kimi_lock.py` — 全局 kimi 串行锁(三消费方共用)
- `src/common/config.py` — 助手配置封装(`data/assistant_config.json` > env,分字段)
- `src/web/templates/settings.html` — 设置页「飞书 AI 助手」卡片
- `tests/unit/assistant/` — 技能校验 + 派单器行为 + 配置优先级/Settings 端点单测
- `tests/unit/web/test_assistant_readonly_key.py` — 只读 key 鉴权矩阵
- `Dockerfile` — COPY kimi-skills
- `src/web/routes.py` — `verify_trading_api_key` 增只读 key 通道(GET 路径白名单)
- `src/web/app.py` — 启动守卫 + shutdown 停止

---

## Backlog

- [ ] Multi-account support
- [ ] Performance analytics
- [ ] Unit test coverage improvement
