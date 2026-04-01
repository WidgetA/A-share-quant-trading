# 个股盈亏分析实验指引

> 本文档记录分析单只推荐股票盈亏原因的标准流程。
> 基于 2026-03-05 对 7 连亏案例的分析实践总结而成。

---

## 一、分析目标

回答三个问题：
1. **为什么选了这只票？** — 它经过了哪些过滤步骤、得分多少、排名为何第一
2. **这只票当天走势如何？** — 买入后是继续涨还是冲高回落
3. **根因是什么？** — 是选股逻辑有缺陷，还是市场随机波动

---

## 二、数据源清单

### 2.1 数据库：历史推荐记录

| 数据 | 来源 | 访问方式 |
|------|------|----------|
| 每日推荐股票记录 | PostgreSQL `trading.momentum_scan_stocks` | CSV 导出接口 / SQL |
| 字段 | `trade_date, stock_code, stock_name, board_name, open_gain_pct, pe_ttm, board_avg_pe, open_price, prev_close, buy_price, next_day_open, return_pct, growth_rate` | |

```sql
-- 查某天推荐记录
SELECT * FROM trading.momentum_scan_stocks
WHERE trade_date = '2026-02-26'
ORDER BY return_pct;
```

CSV 导出：`GET /api/momentum/scan-stocks/csv?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`

### 2.2 日志：Pipeline 各步骤输出

Scanner 日志按 Step 编号输出，关键步骤：

| Step | 内容 | 关键日志关键词 |
|------|------|---------------|
| 1 | 初始筛选涨幅 > 0.56% 的票 | `Step 1: N stocks with gain from open` |
| 2 | 反查所属概念板块 | `Step 2: Found concept boards` |
| 3 | 找热门板块 (>=2 只涨) | `Step 3: N hot boards found` |
| 4 | 获取板块全部成分股 | `Step 4: N total constituent stocks` |
| 5 | 从成分股中选候选 | `Step 5: N stocks selected` |
| 5.5 | 动量质量过滤 | `QualityFilter: FILTERED / KEPT` |
| 5.6 | 冲高回落过滤 | `ReversalFilter` |
| 5.7 | LLM 板块相关度过滤 | `Step 5.7: Filtered N low-relevance` |
| 6 | 评分排名 | `Step 6: Top 3: CODE(GFO=... amp=... cup=... score=...)` |
| 7 | 发送通知 | `Step 7: Notification` |

### 2.3 缓存文件

| 文件 | 内容 | 格式 |
|------|------|------|
| `data/board_relevance_cache.json` | LLM 板块相关度判断结果 | `{"板块名::股票代码": {"level": "高/中/低", "reason": "..."}}` |
| `data/board_constituents.json` | 板块成分股映射 | `{"板块名": ["代码1", "代码2", ...]}` |
| `data/sectors.json` | THS 板块名称列表 | 概念/行业/地域板块 |

### 2.3.1 本地行情缓存（pkl）

实验脚本运行后会在 `data/` 下生成 pkl 缓存，后续分析可直接复用，无需重新下载：

| 文件 | 内容 | 来源 |
|------|------|------|
| GreptimeDB `backtest_daily` 表 | 日线 OHLCV（主板全量） | tsanghi 日线 |
| GreptimeDB `backtest_minute` 表 | 9:40 快照（close_940, cum_volume, max_high, min_low） | tsanghi 5 分钟线 |

**加载方式（asyncpg）：**
```python
# 通过 GreptimeBacktestCache 读取
cache = app.state.backtest_cache
bar = await cache.get_daily("600519", "2024-06-01")
# bar = {"open": ..., "high": ..., "low": ..., "close": ..., "volume": ..., ...}
snap = await cache.get_940_price("600519", "2024-06-01")
# snap = (close_940, cum_volume, max_high, min_low)
```

> **历史**: v0.12.0 之前使用 pickle 文件 (`experiment_turnover_amp_cache.pkl`)，已迁移至 GreptimeDB。

### 2.4 行情数据接口

| 数据 | iFinD 接口 | 替代方案 |
|------|-----------|---------|
| 9:30-9:40 分钟线 | `high_frequency(Interval=1)` | tsanghi `/5min` |
| 实时快照 | `real_time_quotation` | Tushare `rt_min_daily` |
| 日线历史 | `cmd_history_quotation` | tsanghi `/daily` |
| 全天分钟线(复盘) | `high_frequency(09:30~15:00)` | tsanghi `/5min` |

---

## 三、分析流程（Step-by-step）

### Step 1: 拉取推荐记录

确认当天推荐了哪只票、买卖价格、收益率。

```sql
SELECT stock_code, stock_name, board_name,
       buy_price, next_day_open, return_pct
FROM trading.momentum_scan_stocks
WHERE trade_date = '2026-02-26';
```

或用前端 CSV 导出功能。

### Step 2: 查看当天分钟线走势

获取推荐股票全天 1 分钟 K 线，观察买入后的走势模式：
- 买入价是 9:40 的 `latest_price`
- 卖出价是次日开盘价 `next_day_open`
- 关注模式：冲高回落？持续下跌？尾盘拉升但次日低开？

**iFinD 方式：**
```python
await ifind.high_frequency(
    codes="600519.SH",
    indicators="close,volume,high,low",
    start_time="2026-02-26 09:30:00",
    end_time="2026-02-26 15:00:00",
    function_para={"Interval": "1"}
)
```

**Web 接口方式：**
```
POST /api/momentum/loss-analysis
Body: {"losing_trades": [...], "data_source": "ifind"}
返回: stock_day_trend (全天1分钟线) + LLM分析
```

### Step 3: 检查评分细节

从日志或 `ScoredCandidate` 数据中提取：

| 指标 | 含义 | 正常范围 |
|------|------|----------|
| `gain_from_open_pct` | 9:40 相对开盘涨幅 | 0.5% ~ 5% |
| `turnover_amp` | 早盘量能放大倍数 (`early_volume / avg_daily_volume`) | 0.05 ~ 0.30 |
| `consecutive_up_days` | 连涨天数 | 0~2 为佳，>=3 有惩罚 |
| `composite_score` | 最终综合得分 | 越高越好 |
| `leader_bonus` | 板块龙头加分 | 0 或 +0.5 |

**评分公式（v0.10.3+）：**
```
composite = Z(gain_from_open) + Z(turnover_amp) - cup_days * 0.3 + leader_bonus
```
- Z() = 跨候选股的 Z-score 标准化
- `cup_days` = max(0, consecutive_up_days - 1)，连涨 >= 2 天开始惩罚
- `leader_bonus` = 0.5（板块内 gain_from_open 最高且板块 >= 2 只候选时）

### Step 4: 检查板块相关度

查看 `data/board_relevance_cache.json`：

```json
{
  "工业互联网::603709": {"level": "低", "reason": "中源家居主营沙发家具，与工业互联网无直接关联"},
  "工业互联网::000938": {"level": "高", "reason": "紫光股份主营ICT基础设施，是工业互联网核心标的"}
}
```

**判断标准：**
- `"高"` — 主营业务直接相关，板块核心标的
- `"中"` — 有一定关联但非核心（保留）
- `"低"` — 主营与板块无关（过滤掉）

如果缓存中没有该 stock-board 对，说明是新组合，下次扫描时会触发 LLM 调用。

### Step 5: 检查过滤器是否起作用

逐层检查哪些过滤器放行 / 拦截了该票：

1. **Step 5.5 动量质量过滤** — 看日志 `QualityFilter: FILTERED/KEPT`
   - `turnover_amp > 3.0` → 冲高回落，过滤
   - `turnover_amp < 0.4` → 缩量弱势，过滤
2. **Step 5.6 冲高回落过滤** — 看日志 `ReversalFilter`
3. **Step 5.7 板块相关度** — 看缓存 + 日志 `Step 5.7: Filtered`
4. **Step 6 负面新闻** — 看 `news_check_passed` 和 `news_check_detail`

### Step 6: 漏斗分析（批量）

对一段日期范围做漏斗分析，看各层过滤的效果：

```bash
uv run python scripts/analyze_funnel.py -s 2026-02-10 -e 2026-02-28
```

输出：
- 每层股票数量、平均收益、胜率、中位数收益
- "误杀"统计 — 被过滤掉的票里有多少其实是赚钱的

**Web 接口：**
```
POST /api/momentum/combined-analysis
返回: 每天的 backtest + funnel 层级数据 (SSE streaming)
```

---

## 四、常见亏损模式

### 模式 A：蹭概念股（已通过 Step 5.7 解决）

**特征：** 股票主营业务与所属概念板块无关
**案例：** 中源家居(603709) 被归入"工业互联网" — 主营沙发
**诊断：** 查 `board_relevance_cache.json`，level 应为 "低"
**根因：** 同花顺板块归类过于宽泛，很多票只是"沾边"

### 模式 B：冲高回落

**特征：** 早盘快速拉升后回落，9:40 买在高点
**案例：** turnover_amp 异常高（> 3x），gain_from_open 也高
**诊断：** 看分钟线走势，确认是否 9:35 后开始回落
**根因：** 游资拉升出货，非主力真正建仓

### 模式 C：连涨疲劳

**特征：** 已连涨 3+ 天，动能衰减
**案例：** consecutive_up_days >= 3，评分有 cup_penalty
**诊断：** 看日线趋势，确认连涨天数
**根因：** 追高买入，获利盘抛压导致次日低开

### 模式 D：板块退潮

**特征：** 板块当天早盘热但午后退潮，成分股全面回落
**诊断：** 看板块内其他股票的走势，确认是否板块级别退潮
**根因：** 板块轮动，资金撤离

### 模式 E：个股利空

**特征：** 公司有负面新闻（业绩暴雷、诉讼、高管变动等）
**诊断：** 查 `news_check_detail` 字段
**根因：** 新闻过滤器未检测到，或新闻在推荐后才发布

---

## 五、分析报告模板

```markdown
## [日期] [股票代码] [股票名称] 盈亏分析

### 基本信息
- 所属板块: XXX
- 买入价 (9:40): X.XX
- 卖出价 (次日开盘): X.XX
- 收益率: -X.XX%

### 选股依据
- gain_from_open: +X.XX%
- turnover_amp: X.Xx
- consecutive_up_days: N
- composite_score: X.XX
- 板块相关度: 高/中/低 (reason)
- 负面新闻检查: 通过/未通过

### 当天走势
[描述分钟线走势模式]

### 亏损模式
[模式 A/B/C/D/E，附具体证据]

### 改进建议
[针对此模式应调整哪个过滤器/参数]
```

---

## 六、关键文件索引

| 文件 | 用途 |
|------|------|
| `src/strategy/strategies/momentum_sector_scanner.py` | 主扫描器，Step 1-7 全流程 |
| `src/strategy/filters/momentum_quality_filter.py` | Step 5.5 动量质量过滤 |
| `src/strategy/filters/reversal_factor_filter.py` | Step 5.6 冲高回落过滤 |
| `src/strategy/filters/board_relevance_filter.py` | Step 5.7 LLM 板块相关度过滤 |
| `src/data/database/momentum_scan_db.py` | 推荐记录数据库读写 |
| `src/web/routes.py` | Web API 路由（含 loss-analysis） |
| `scripts/analyze_funnel.py` | 漏斗分析脚本 |
| `data/board_relevance_cache.json` | 板块相关度缓存 |
| `data/board_constituents.json` | 板块成分股映射 |
