# A股融资耗竭风险指数（MEWS）

## 它是什么

MEWS（Margin Exhaustion Warning System）是一个观察A股融资链条的市场级风险指数。它用于识别两类状态：

1. 市场融资存量较高，同时新增融资购买力正在广泛减弱；
2. 融资负债已经转入广泛、持续的净偿还。

MEWS回答的是：**当前市场是否形成了由融资负荷、融资购买力耗竭或融资负债收缩共同驱动的系统性去杠杆风险。**

它不是大盘涨跌预测，也不是覆盖所有风险来源的综合指数。收盘价仅用于计算自由流通市值；价格涨跌、收益率、波动率、技术指标、新闻、公募与ETF资金等信号不进入指数计算。

## 数据口径

指数只统计沪深主板、创业板和科创板的普通A股，并按照各历史时点实际存在的上市股票计算。退市股票保留退市前历史；北交所股票、ETF、基金、可转债、债券、REITs、CDR等不纳入。

| 数据 | 用途 |
|---|---|
| Tushare `margin_detail` | 普通A股逐股融资余额、融资买入额和融资偿还额 |
| Tushare `daily_basic` | 自由流通股本和自由流通市值 |
| Tushare `stock_basic` | 股票范围及上市、退市边界 |
| Tushare `trade_cal` | 沪深交易日序列 |
| Tushare `margin` | 校验市场融资总量和普通A股明细覆盖率，不进入核心公式 |

融资偿还额可能包含直接还款、卖券还款、其他负债调整或强制平仓，因此只能解释为融资负债偿还，不能直接等同于卖股金额。

上述数据均可由Tushare Pro现有接口提供，不需要采购额外数据源。生产账号需具备`margin`、
`margin_detail`、`daily_basic`、`stock_basic`和`trade_cal`权限；这些接口当前均要求至少2000积分。
按交易日请求时，`margin_detail`和`daily_basic`的单次上限均覆盖当前A股数量。系统仍会检查沪深
交易所完整性、普通A股明细覆盖率和自由流通市值覆盖率，不因接口请求成功就默认数据完整。

## 基础变量和共同规则

对全部普通A股汇总：

```text
M_t = t日融资余额
B_t = t日融资买入额
R_t = t日融资偿还额

NetFlow_t = B_t - R_t
FlowRate_t = NetFlow_t / M_(t-1)
```

逐股变量相应记为 `M_i,t`、`B_i,t`、`R_i,t`。只有前一交易日融资余额大于0时才计算融资流率。

所有窗口均按交易日计算。EMA固定使用 `adjust=False`、`ignore_na=False`；缺失数据保持缺失，不填0。

需要历史标准化的指标采用最近500个交易日的mid-rank分位，至少需要120个有效值：

```text
Percentile_t =
    [count(x < x_t) + 0.5 * count(x = x_t)]
    / count(valid x)
    * 100
```

分位数包含当日，不包含未来数据。它表示当前值在历史窗口中的相对位置，不表示风险发生概率。

交易日`t`的融资数据在下一交易日才可用于决策，因此指数同时记录：

```text
signal_trade_date = t
signal_available_date = t之后的下一个交易日
```

## 指标计算

### MPI：融资脉冲

```text
PulseRaw_t = EMA5(FlowRate_t) - EMA20(FlowRate_t)
MPI_t = rolling_percentile_500(PulseRaw_t)
```

MPI衡量近期融资扩张速度相对过去一个月的强弱。MPI越低，表示新增融资购买力的边际动能越弱；核心公式使用`100 - MPI`作为耗竭强度。

### MLS：融资负荷

逐股自由流通市值汇总为：

```text
FFMV_t = SUM_i(close_i,t * free_share_i,t * 10000)
```

为避免当日价格下跌机械抬高融资负荷，分母只使用此前20个交易日：

```text
FFMVBase_t = median(FFMV_(t-20), ..., FFMV_(t-1))
LeverageLoadRaw_t = M_t / FFMVBase_t
MLS_t = rolling_percentile_500(LeverageLoadRaw_t)
```

MLS越高，表示融资余额相对自由流通市值处于越高的历史位置，即市场融资脆弱性越高。MLS高本身不表示风险正在发生。

### NIB：鲁棒负脉冲扩散

先计算每只股票的融资脉冲：

```text
FlowRate_i,t = (B_i,t - R_i,t) / M_i,t-1
Impulse_i,t = EMA5(FlowRate_i,t) - EMA20(FlowRate_i,t)
```

每只股票使用最近60个有效脉冲值计算鲁棒尺度，至少需要40个有效值：

```text
ImpulseScale_i,t = 1.4826 * MAD(Impulse_i)
ImpulseZ_i,t = Impulse_i,t / ImpulseScale_i,t
```

尺度无效的股票不进入NIB。固定负脉冲条件及幅度为：

```text
is_negative_i,t = ImpulseZ_i,t < -0.25
NegativeMagnitude_i,t = clip((-ImpulseZ_i,t - 0.25) / 2.75, 0, 1)
```

使用前一交易日融资余额对有效股票加权：

```text
NIBBreadth_t = 100
    * SUM_i[M_i,t-1 * I(is_negative_i,t)]
    / SUM_i[M_i,t-1]

NIBMagnitude_t = 100
    * SUM_i[M_i,t-1 * NegativeMagnitude_i,t]
    / SUM_i[M_i,t-1]

NIB_t = sqrt(NIBBreadth_t * NIBMagnitude_t)
```

NIB同时衡量融资动能减弱覆盖了多少融资持仓，以及减弱程度有多深。NIB越高，负融资脉冲越广泛、越严重。

### DLB：融资负债收缩扩散

```text
NetFlow5_i,t = SUM(NetFlow_i,t-4, ..., NetFlow_i,t)
is_deleveraging_i,t = NetFlow5_i,t < 0

DLB_t = 100
    * SUM_i[M_i,t-1 * I(is_deleveraging_i,t)]
    / SUM_i[M_i,t-1]
```

参与计算的股票必须具有连续五个交易日的真实融资观测。DLB越高，表示五日累计净融资偿还已扩散到越大比例的融资持仓。

### NetOutflowLevelScore：持续净偿还水平

```text
NetFlowLevelRaw_t = EMA5(FlowRate_t)
NetOutflowLevelScore_t =
    rolling_percentile_500(-NetFlowLevelRaw_t)
```

该指标越高，表示市场整体持续净融资偿还的程度在历史上越异常。它用于识别净流量长期为负、但快慢EMA差已经收敛的情形。

### 辅助诊断指标

看板同时提供三个不进入MEWS核心公式的诊断分数：

```text
BuyIntensity_t = B_t / M_(t-1)
RepayIntensity_t = R_t / M_(t-1)

BuyShortfallScore_t = 100 - rolling_percentile_500(EMA5(BuyIntensity_t))
RepayLevelScore_t = rolling_percentile_500(EMA5(RepayIntensity_t))
MEWSRollingPercentile_t = rolling_percentile_500(MEWS_t)
```

`BuyShortfallScore`越高，表示近期融资买入水平越低；`RepayLevelScore`越高，表示近期融资偿还
水平越高。`MEWSRollingPercentile`只表示当前MEWS在最近历史窗口的位置。三者用于拆解观察，
不进入两条风险路径，也不参与固定状态阈值判断。

## 两条风险路径和主指数

融资购买力耗竭路径为：

```text
ExhaustionPath_t = cubic_root(
    (100 - MPI_t) * MLS_t * NIB_t
)
```

它表示高融资负荷下，新增融资购买力正在减弱，并且这种减弱已广泛扩散。

持续去杠杆路径为：

```text
PersistentDeleveragingPath_t = cubic_root(
    MLS_t * DLB_t * NetOutflowLevelScore_t
)
```

它表示高融资负荷下，融资净偿还已经广泛扩散且持续存在。

MEWS取两条路径中的较高值：

```text
MEWS_t = max(ExhaustionPath_t, PersistentDeleveragingPath_t)
```

两条路径均使用几何平均，使每条路径中的三个条件必须共同抬升。主指数范围为0—100，不再对组合结果做第二次历史分位。取最大值是为了保留风险所处的不同阶段：一条路径回落时，不会平均掉另一条仍然显著的风险。

## 风险状态

状态阈值固定取2014-09-22至2021-12-31开发样本的分布：

| 状态用途 | 定义 | 当前阈值 |
|---|---|---:|
| WATCH | MEWS第85分位 | 57.8648 |
| WARNING | MEWS第95分位 | 68.0185 |
| CLEAR | MEWS第75分位 | 49.5390 |
| Persistent DANGER | 持续去杠杆路径第85分位 | 57.3157 |

状态机按以下规则运行：

- `NORMAL`：没有形成持续的高融资风险组合。
- `WATCH`：最近三个交易日中至少两日MEWS不低于WATCH阈值。
- `WARNING`：最近三个交易日中至少两日MEWS不低于WARNING阈值。
- `DANGER`：连续两日同时满足MEWS不低于WARNING阈值、持续去杠杆路径不低于Persistent DANGER阈值；或者当日`DLB >= 75`且`NetOutflowLevelScore >= 99`。
- 完全解除：MEWS连续五个交易日低于CLEAR阈值后恢复`NORMAL`。此前每个交易日最多下降一级，避免阈值附近频繁跳变。

数据不完整或指数无法计算时维持上一有效状态，不把数据异常解释为市场安全或市场风险。

## 如何解释

| 指标组合 | 含义 |
|---|---|
| MLS高，MPI高 | 融资存量较重，但融资动能仍强；有脆弱性，尚未形成耗竭信号 |
| MPI低，MLS低 | 融资动能减弱，但融资存量压力有限 |
| MLS高、MPI低、NIB高 | 融资购买力耗竭路径占主导，属于较早的风险阶段 |
| MLS高、DLB高、NetOutflowLevelScore高 | 持续去杠杆路径占主导，融资负债收缩已广泛扩散 |
| MEWS高 | 至少一条融资风险路径显著，不表示市场未来必然下跌 |
| MEWS低 | 融资链条没有明显风险信号，不表示市场不存在其他风险 |

MEWS描述的是融资风险条件的组合强度，不是未来下跌概率。使用时应同时查看主导路径、组成指标、数据有效性和`signal_available_date`。

## 生产实现

GreptimeDB使用三张独立时序表保存生产数据：

| 表 | 保存内容 |
|---|---|
| `margin_risk_security_daily` | 逐股融资余额、融资买入额、融资偿还额 |
| `margin_risk_market_daily` | 沪深汇总、普通A股汇总、自由流通市值、覆盖率和摄取状态 |
| `margin_risk_metric_daily` | MEWS、两条路径、全部组成指标、风险状态、数据状态、阈值和信号可用日 |

启动时自动幂等建表，并立即在后台触发一次完整历史补全；GreptimeDB未就绪或数据维护占用时会
等待后自动执行。统一“数据检查和补充”也会查找并补齐缺失或失败的交易日；手动触发可重新检查
完整历史，凌晨3点任务有界续补。由于Tushare两融数据约在次日08:30更新，系统另于每天08:50
刷新最新缺口；有上限的自动任务优先最近日期，再逐步向前补历史。原始事实已经写入但指标重算
中断时，下一次检查会比较两者末日并恢复重算。

交易看板在账户净值板块下方显示MEWS风险曲线。所有风险分数使用0—100统一纵轴，可切换组成
指标；直接在图内使用滚轮或双指缩放、按住拖动平移、双击恢复全部，并支持逐日悬浮明细。标题旁
“指标说明”弹窗逐项说明14条曲线是什么、数值高低表明什么以及它如何导向最终状态。
只读接口为`GET /api/trading/margin-risk-curve?days=5000`。
