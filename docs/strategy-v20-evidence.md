# V20 研究证据附录

> 对应规范：[V20 策略决策与推送规范](./strategy-v20.md)
> 冻结日期：2026-08-31
> 冻结策略：`BAD_E50_G_BASE`
> 历史审计：`CURRENT_0941_RETROSPECTIVE_FULL_CHAIN_PASS / NO_HISTORICAL_DURABLE_RECEIPT`
> 生产实现：`COMPLETE_DEFAULT_DISABLED`
> 正式启用：`REQUIRES_ACCEPTED_FORWARD_SHADOW_CHECKPOINT_AND_OPERATOR_APPROVAL`

本文件保存为什么选择 V20、历史结果如何解释、哪些反证必须长期保留，以及本次研究使用的制品。它不参与每日决策；若本文件与主规范的动作定义冲突，以主规范为准。

## 1. 选择来源与证据等级

V20 最终采用：

```text
BAD_E50_G_BASE
= rolling7 BAD 时把基础新批次投入减半
+ BAD 且 G=1 时当日新批次归零
+ 保留完整 BASE 退出
+ 不使用 X08
```

原预注册唯一主候选是含 X08 的 `BAD_E50_G_X08`，正式集成裁决为 `LIMITED_HISTORICAL_SUPPORT`。不含 X08 的 `BAD_E50_G_BASE` 原本是预注册机制对照，结果揭晓后才依据风险偏好、边际贡献和复杂度被选作 V20，不能倒称为原预注册冠军。它自己的独立审计结论是：

```text
LIMITED_HISTORICAL_COMPONENT_SUPPORT / NO_GO_FORWARD_SHADOW_ONLY
```

业务上已选定规则，不等于历史证据等级被升级，也不等于授权自动下单。V20 当前适合开发可重放的决策推送服务并做前向 shadow。

## 2. 回测口径

当前生产规则冻结为：

```text
reference_profile_id = CALENDAR_0940_OPEN_END_LABEL_0941_V1
return_profile_id = ZERO_COST_GROSS_PRICE_RETURN_V1
```

即入场参考价是日历09:40开盘；当前分钟结束标签源必须读取原始09:41 bar的open。收益只计算 `退出价/参考入场价-1`，手续费、税费、佣金和人为滑点全部为0。

当前09:41参考价已经完成回顾性全链重放：从原始09:41 bar重新计算health、rolling7、BASE开仓、D1/D2/D3+退出和V20仓位，而不是只给旧退出收益换分母。第3—5节均以该新结果为主；旧raw 09:40零延迟诊断只保留在第2.1节作审计对照，不能再充当当前策略数字。

新重放比较同一 BASE 与 V20 `BAD_E50_G_BASE`：

- 每个信号日独立投入一份标准日批次本金；
- 跨日批次并发不设上限；
- “累计贡献”是各批次盈亏相加，不是固定账户复利年化收益；
- 加法 MDD 是按信号日/入场批次顺序累加标准批次贡献后的峰谷差，不是实际退出日每日盯市净值或账户权益回撤；
- 每只股票按纯价格毛收益评价，手续费和人为滑点均为0；
- 冻结主报告使用 `FROZEN_RAW_CONTINUITY` 原始价格连续链，不计现金分红，也不是复权总回报；因此历史零差异复现与生产公司行动正确性是两个不同问题。

因此：

- `+216.85 个标准批次百分点` 表示全样本累计赚约 `2.1685 × 一份标准日批次本金`；
- `-44.00 个标准批次百分点` 表示加法累计曲线从局部高点到低点减少 `0.4400 × 一份标准日批次本金`；
- 它不表示某一天账户直接亏损44.00%；
- 该口径不能与按单票逐笔累加得到的早期约 300% 结果直接比较。

### 2.1 已被替代的零成本 legacy 重放

旧raw 09:40诊断由下列脚本与manifest生成并冻结，仅用于证明旧研究链可以复现：

```text
strategy-research/kangdie/explore/v16_rolling7_true_defense_20260831/zero_cost_gross_replay/run_zero_cost_gross_replay.py
SHA256=8c07e8c148b6c53ad75fffe72c3ee40675f282901f5aab77fa4276bbf0db6ddf

strategy-research/kangdie/explore/v16_rolling7_true_defense_20260831/zero_cost_gross_replay/MANIFEST.json
SHA256=beea2c48cf5b7dd87b40a31ab2f5d53017d94913cea1eb6b39934817dba29ef1
status=PASS
schema=v16-zero-cost-gross-price-return-replay/v1
```

manifest固定 `fees=0/slippage=0`、444个信号批次和4299条股票腿，并记录rolling7在零成本重建后的611个决策日中有21日由旧 `BAD` 变为 `NON_BAD`、无反向变化。主要输出hash为：

```text
rolling7_state_zero_cost_gross.csv       0936ebfb2c8d2e6a9663a6506a089b14f7d40c302df3a7c12ca34cfd4fb27c58
candidate_batch_results_zero_cost_gross.csv 90665a9f1d5e0ad09dc56196e6b01698376d0b6f409646a2a02ae7cf5e8edd4b
metrics_zero_cost_gross.csv              3665e60a4b68a43fcacb10e253a2ff13b07e3063c074053b5ffa69f5b8ac07b6
marginals_zero_cost_gross.csv            cbba172c09cdb7c971669d134e0645f6b7e2c462422ea988a489aeb8aa86c0a4
x08_revalidation_zero_cost_gross.csv     9ed1c6e88d741ed97fac9c4c810b85fc34a4bee6aed550adcf65addcd40717d4
daily_detail_zero_cost_gross.csv         adfec452eae1ead9db4bd0900c035d6eeb5527bcdc8d682424cd7c445d3570eb
```

该旧重放只把旧冻结腿改按 `ZERO_COST_GROSS_PRICE_RETURN_V1` 重算；其入场参考价仍是 `RETROSPECTIVE_ZERO_LATENCY_0940_END_LABEL_OPEN_V1`。旧全样本BASE `+222.08`、V20 `+225.60`、V20 MDD `-42.40` 已被第2.2节09:41全链数字替代，不得继续引用为当前策略结果。

### 2.2 当前09:41参考价回顾性全链重放

本次重放实际执行：

```powershell
py -3.14 strategy-research\kangdie\explore\v16_rolling7_true_defense_20260831\entry_label_0941_replay\fetch_market_0941.py --workers 4
py -3.14 strategy-research\kangdie\explore\v16_rolling7_true_defense_20260831\entry_label_0941_replay\rebuild_state_inputs.py
py -3.14 strategy-research\kangdie\explore\v16_rolling7_true_defense_20260831\entry_label_0941_replay\replay_exit_and_v20.py
```

关键覆盖和路径校验：

- 全市场raw 09:41捕获覆盖2023-04-03至2026-08-28，共825个交易日、2,622,334行，每个 `(trade_date, stock_code)` 恰有一根bar；
- 原BASE 444批/4,299腿的09:41参考价覆盖率均为100%；换参考价后health使2025-05-30由空仓变满仓，故最终评价总体为445批/4,309腿，退出全部解决；
- rolling7的6,774腿覆盖6,754腿；仅缺2026-06-01/02各10腿，这两批旧口径原本也是 `INCOMPLETE_OUTCOME`。原本完整的719批仍为719/719完整，窗口成员差异为0；
- health新旧均有694个有效相对标签，scanner eligible比较池最少2,944只；
- 2026-08-27参考价完整但T+2尚未成熟，不进入截至2026-08-26的收益评价；
- D1触发标志改变31腿，最终退出事件改变294腿；239条新旧入场价相同的腿，其退出状态、日期、时钟和factor 239/239精确复现，证明退出路径确已全链重跑。

机器摘要已纳入仓库：

```text
docs/strategy-v20-artifacts/retrospective-0941-zero-cost-replay-v1.json
SHA256=9ba0de5f12c4a4a7c00c8f4e2488b021f78d43d1a0bab6d33ce210d3e46d28f8
production_bootstrap_eligible=false
historical_receipt_clock_status=UNAVAILABLE_RETROSPECTIVE_RESEARCH_CLOCK
```

本机详细证据位于忽略目录，摘要记录其路径、命令和hash：

```text
MARKET_0941_CAPTURE.json SHA256=731aa2504a8d2565f25c9c7b3e248ea9f3c5056405d3387c9dd4bd34ac614dd9
STATE_INPUT_REBUILD.json SHA256=4a52b2f074176d38e673f91e7cf1df36d0474f5f2371edea1e9c631a5d49471d
FULL_CHAIN_REPLAY.json   SHA256=94f655f58aee9e148a97708a8b38acef76e9bd7c821384ee20487e09a52da9fc
REPORT.md                SHA256=12fe7262aef9be454b56d66efe8735ca22c5091fe4f3a3ba0dbd596b9549cd6c
```

这项结果已经验证当前参考价的历史策略数字，但仍是回顾性研究证据：raw 09:41行情没有当年首次durable接收回执，2023—2025信号和G仍继承current-taxonomy回顾代理，MEWS历史也大部分不是严格在线PIT。因此该摘要明确 `production_bootstrap_eligible=false`，不得用它建立生产genesis、证明deadline内可见或放行正式推送；这些事项只能由真实durable时钟、合法状态迁移和前向shadow完成。

## 3. 主结果

| 区间 | BASE 累计贡献 | V20 累计贡献 | V20 相对变化 | BASE 加法 MDD | V20 加法 MDD |
|---|---:|---:|---:|---:|---:|
| 2024 全年 | +69.23 | +87.63 | +18.40 | -53.97 | -44.00 |
| 2025 全年 | +111.74 | +105.16 | -6.58 | -38.99 | -25.37 |
| 2026-02-27 至成熟信号 2026-08-26 | +29.05 | +24.06 | -4.99 | -44.03 | -29.48 |
| 2026 年 7 月 | -23.99 | -14.16 | +9.83 | -31.97 | -18.50 |
| 2026 年 8 月成熟样本（信号日至 8 月 26 日） | +45.22 | +29.49 | -15.73 | -5.68 | -5.68 |
| 2024—2026YTD 全样本 | +210.01 | +216.85 | +6.83 | -53.97 | -44.00 |

除“区间”外，表中数字均以“个标准批次百分点”为单位，不能直接加百分号解释成账户收益率或账户回撤率。

全样本共445个有效BASE批次：

- V20有431个批次开仓，非零开仓率96.9%；
- 340个批次为满仓、91个批次为半仓、14个批次被G完全拦截；
- 最差单批由-10.66%改善到-8.55%；
- ES5（最差5%批次平均值）由-6.87%改善到-5.81%；
- 相对BASE的+6.83个标准批次百分点中，rolling7半仓边际+3.65，G在半仓基础上的追加空仓边际+3.18。

这些金额风险改善主要来自少投入，不是被保留股票每一元本金的质量变好了。V20是仓位保险，不是选股准确率升级。相较已被替代的raw 09:40诊断，新口径全样本BASE由+222.08变为+210.01、V20由+225.60变为+216.85、V20 MDD由-42.40变为-44.00；方向仍成立，但旧数字已经失效。

## 4. 接受理由与必须保留的反证

V20 的业务目标不是每个月都战胜 BASE，而是在 V16 已持续失效时减少新投入，并在更极端的“板块分散 + 绝对成交弱”组合下空仓。

当前09:41重放中，2026年7月累计亏损贡献由-23.99降至-14.16个标准批次百分点，加法MDD由-31.97降至-18.50。策略负责人明确接受2026年8月成熟样本少赚15.73个标准批次百分点，视为持续恶化保险的机会成本。

反证必须同时保留：

- 2025年V20比BASE少6.58个标准批次百分点，2026YTD少4.99，跨年并非每年增益；
- G的边际贡献在2024、2025、2026YTD分别为 `+3.04 / -1.98 / +2.13`，方向不稳定；
- 2026 年 8 月成熟样本中的 5 个 `BAD` 批次（8 月 3、4、24、25、26 日）事后全部盈利；
- 8月25日G全停错过的BASE批次相当于标准批次本金+3.68%；
- BASE在该月亏损的8月11、12、18、20日全部为 `NON_BAD`，各自贡献约为-0.12%、-0.24%、-3.90%、-1.78%，V20均未拦截；
- 因而该月BASE与V20加法MDD同为-5.68个标准批次百分点。

准确结论是：rolling7 能表示“持续恶化已经发生”，不能预测所有突然出现的坏日；G 也尚未证明是跨年稳定的收益预测器。

## 5. G 与 MEWS 的证据边界

- 2024/2025 的 G 语义分类使用回顾性的 current-board 代理，不是严格历史时点成分；
- 2026 使用仓库按日期路由的快照，也不是交易所原生不可变历史；
- BASE 退出中的 MEWS 信息时钟要求来源日严格早于 D1，但原444批研究总体里有434批来自2026-08-09的回顾性SQLite重建，只有少量批次带当时在线时间戳；09:41重放沿用这条冻结MEWS血缘，没有把它升级成在线PIT；
- 冻结退出主口径没有经济调整除权、送转、拆并股；生产公司行动口径尚需另行冻结并重新验证，不能把诊断用 `CANONICAL_TOTAL_RETURN` 静默替换进V20；
- 因此 G taxonomy 与 MEWS 的历史结果都不能被包装成完整在线 PIT 证据。

## 6. 为什么不采用 X08

X08 指在 `BAD` 批次把 D2 常驻止损从 -12% 收紧为 -8%。当前09:41全链重放只验证了冻结的无X08路径，没有重新运行X08反事实；下列数值仅是已被替代的raw 09:40 legacy诊断，不能冒充当前09:41比较：

- 全样本只多贡献约 +0.35 个标准批次百分点；
- 只有 8 个批次、11 只股票真正改变退出；
- 2026 年 7 月和 8 月没有股票因此改变退出；
- 加入后全样本加法MDD为-42.52，略差于不加时的-42.40；
- 新增分支没有换来足够明确的边际价值。

V20 仍保留 BASE 的 D2 常驻 -12% 与 MEWS 危险时 -5%；冻结规则不含 BAD 批次额外 -8% 这一层。当前V20主结果不依赖X08数据，若未来要重新考虑X08，必须按09:41参考价另行全链反事实重放。

## 7. 当前不进入 V20 的研究方向

以下内容可继续研究，但不是冻结 V20 输入：

- 开盘广度或候选票成交额快速恢复；
- 板块重新聚合；
- KOSPI/KOSDAQ 与海外对应板块消息；
- 开盘后一小时走势；
- 移除龙头后重新运行 V16。

这些规则必须先冻结、再跨 2024/2025/2026 验证，不得根据已知结果临时解除 `BAD`。

## 8. 冻结 G 制品

生产/重放使用的规范制品：

```text
docs/strategy-v20-artifacts/manifest-v1.json
SHA256=377cf1181539ad7d7b2e0407c27e6529e1c911e06052c7968caf057cb0131d32

docs/strategy-v20-artifacts/g-theme-mapping-v1.csv
SHA256=35aa230ae480a0b2d543264063ebded9aca4ebb73d2d135c1242483a69025160

docs/strategy-v20-artifacts/causal-half-year-q25-v1.csv
SHA256=c1315836ad2dd7f07fb7933bc6608ff0949e7dfa4cfb460e5dd299979bcd8d0f

docs/strategy-v20-artifacts/causal-half-year-q25-samples-v1.csv
SHA256=1e5dad588b3adb9ac987dc8a8231cc8c49dff029b1c0a53604076ee91bf2be24
```

G 原始研究制品清单：

```text
strategy-research/kangdie/explore/v16_rolling7_regime_switch_20260830/gate_masks/MANIFEST.json
SHA256=e8af1c8c442ed11b952c0ec4baa83ee1af126845e02f2ad1b552c693bd48e0db
```

该 manifest 绑定 301 个原始标签的语义映射及扩展文件。半年度成交额阈值表：

```text
strategy-research/kangdie/explore/v16_rolling7_regime_switch_20260830/gate_masks/CAUSAL_HALF_YEAR_Q25.csv
SHA256=4e80d8009501151fb0243c6b23667f9d0900f76259e20fb004c68e1555e5ff2b
```

2026H2 使用冻结时已有的 73 个完整样本：

| 指标 | Q25（人民币元） |
|---|---:|
| Top10 D-1 成交额总和 | 11,338,548,013.96 |
| Top10 D-1 单票成交额中位数 | 711,477,010.50 |
| Top10 D-1 最低 3 只成交额合计 | 691,205,838.00 |

规范samples制品按 `decision_half=2026H2` 精确筛出73行，首尾为2026-02-27/2026-06-30，并能按冻结线性插值零差异重算上表。73行的原始daily panel SHA256为 `ed38be8f947f8d73e00b5d340b2f066f511178231092155ad781ea703fff737d`，阈值摘要源SHA256为 `7ad6fb075b4233e1825319cab9d59efafb8faea3814eb7d4182b308583cb8547`。

必须保留时间证据限制：73行研究产物的本机mtime为2026-08-30 17:43:24 +08:00，但没有加密可信时间戳；23:44后续重建已经有75行。后补的2026-06-01、2026-06-02只用于诊断，不把已注册的73样本阈值改成75样本重算。该阈值只允许在17:43注册点以后作为前向冻结制品，不能据此把此前历史结果升级成严格在线PIT证据。

## 9. 2024 历史结果的预热边界

2024回测不是在2024-01-02清空状态后启动。研究流水线从2023-04-10开始推进决策日、从2023-04-11开始积累信号批次，并把决策时已经可得的2023结果带入2024-01-02；截至当时已有164个完整成熟批次，另保留2023-12-28和2023-12-29两个待成熟批次。若从空状态启动，会得到 `BASE=WARMUP/rolling7=UNKNOWN`，无法复现冻结结果。

当前09:41零成本重放的2024-01-02首日向量为：最近3个有效信号日仍是2023-12-22/25/27，相对收益 `+0.0212144349/-0.0134273403/-0.0147555466`，均值 `-0.0023228173`，所以 `PAUSED_R0/recovery_count=0`；当日Wilson下界仍为 `0.4165432908`，故 `base_multiplier=1`。rolling7窗口仍为2023-12-18/19/20/21/22/25/27，按09:41参考价重算后 `R7=-0.1489987690、L7=6`，所以BAD；G完整但CLEAR，最终倍率0.5。该向量已经写入第2.2节机器摘要，明确证明当前历史结果使用了2023预热，而非2024空状态启动。

旧seed中的同日U8b向量为：上述3日相对收益 `+0.0210268433/-0.0148977399/-0.0140117992`，均值 `-0.0026275653`；rolling7为 `R7=-0.1545104691、L7=6`。这些旧值来自raw 09:40参考价和每腿20bps，只用于legacy适配器诊断，不得替换当前09:41数值。两条回顾链的成熟结果都满足 `t2_exit_date < decision_date`，最新使用退出日为2023-12-29，不是未来价格。

但旧seed和新09:41回顾重放都没有V20 durable inbox的首次 `received_at`，两份制品均明确标记 `historical_receipt_clock_status=UNAVAILABLE_RETROSPECTIVE_RESEARCH_CLOCK`。因此它们不能证明这些标签满足生产固定cutoff，也不能直接充当生产bootstrap；正式状态种子必须来自具有真实首次接收证据的历史重放或已验收前向shadow。

完整逐标签、逐批值和来源hash已经冻结在：

```text
docs/strategy-v20-artifacts/historical-seed-2024-v1.json
SHA256=3a4e561a0fd2b6936d49485c7285709dd2238069f57925576e7f9be9236e2c7a
artifact_type=HISTORICAL_EVIDENCE_SEED_ONLY
artifact_status=LEGACY_DIAGNOSTIC_SUPERSEDED_FOR_PRODUCTION
production_bootstrap_eligible=false
current_v20_rule_compatible=false
reference_profile_id=RETROSPECTIVE_ZERO_LATENCY_0940_END_LABEL_OPEN_V1
return_profile_id=LEGACY_NET_PRICE_RETURN_MINUS_20BPS_V1
historical_receipt_clock_status=UNAVAILABLE_RETROSPECTIVE_RESEARCH_CLOCK
```

该文件是被当前V20规则取代的legacy研究链诊断向量，不是当前规则零差异向量，也不是可直接搬入生产的bootstrap checkpoint。旧 `health_state_audit.csv` 记录绝对Top3 C3状态，不是V20相对健康的冻结来源，已在seed中明确排除。

## 10. 2026-08-31 代码快照说明

本次文档整理时检查的 `main` 工作树基准 commit 为：

```text
06c2c1d4125368c9963f47794fcc903468e5d55a
```

当时文件 SHA256：

| 文件 | SHA256 |
|---|---|
| `src/strategy/strategies/v16_scanner.py` | `898fc16de390065419d0c62869de402176ec2ec0ad4aa340b24fbd22634d2b15` |
| `src/strategy/lgbrank_scorer.py` | `25293e3309e92400a1ab1f777f9164c577187056c018d21dab64651f61adaac0` |
| `src/web/v15_scan_service.py` | `73bd5ace0935ba235aff4b8a09e61b9ad355dc309378b555dbb3978e3ff508a8` |

这些值只是研究快照，不是未来生产版本。该工作树当时存在未提交改动；正式决策服务必须记录实际部署的不可变 V16 版本和输入制品哈希。

当时的实现差距是：V16 Scanner 可以生成完整 Top10，但旧正式返回与共享状态仍压成 Top1；基础健康、rolling7、G 和可靠的 V20 决策 outbox 尚未进入正式推送链。现有 `v16_day_gate.py` 是另一套未校准的 shadow 集中度图门，不等于 V20。

## 11. 研究来源

下列路径位于仓库根目录的本地研究档案 `strategy-research/`。该目录按现有 `.gitignore` 不推送，路径仅用于本机证据定位，不是生产运行依赖；主规范和本附录已保存实施所需规则、数值与制品哈希。

- 基础仓位策略收口：`strategy-research/kangdie/explore/v16_unified_risk_defense_20260829/USABLE_STRATEGY_FINAL_SYNTHESIS.md`
- 无豁口退出预注册：`strategy-research/kangdie/explore/v16_t1_intraday_stop_20260830/full_hard_stop/PREREGISTRATION.md`
- 无豁口退出结果：`strategy-research/kangdie/explore/v16_t1_intraday_stop_20260830/full_hard_stop/REPORT.md`
- D2 常驻闸门与 D1 MEWS 分层预注册：`strategy-research/kangdie/explore/v16_t2_conditional_gate_20260830/mews_gate/PREREGISTRATION_LAYERED_D1.md`
- D2 常驻闸门与 D1 MEWS 分层结果：`strategy-research/kangdie/explore/v16_t2_conditional_gate_20260830/mews_gate/LAYERED_REPORT.md`
- rolling7 真防御预注册：`strategy-research/kangdie/explore/v16_rolling7_true_defense_20260831/protocol/PREREGISTRATION.md`
- 不含 X08 的暴露与 G 结果：`strategy-research/kangdie/explore/v16_rolling7_true_defense_20260831/exposure_gate/REPORT.md`
- 不含 X08 组件独立审计：`strategy-research/kangdie/explore/v16_rolling7_true_defense_20260831/exposure_gate/INDEPENDENT_AUDIT.json`
- 七候选集成结果：`strategy-research/kangdie/explore/v16_rolling7_true_defense_20260831/integrated/REPORT.md`
- 独立审计：`strategy-research/kangdie/explore/v16_rolling7_true_defense_20260831/integrated/independent_audit/REPORT.md`
- 原研究裁决：`strategy-research/kangdie/explore/v16_rolling7_true_defense_20260831/FINAL_CONCLUSION_ZH.md`
- raw 09:41行情捕获与覆盖：`strategy-research/kangdie/explore/v16_rolling7_true_defense_20260831/entry_label_0941_replay/MARKET_0941_CAPTURE.json`
- 09:41 health/rolling7重建：`strategy-research/kangdie/explore/v16_rolling7_true_defense_20260831/entry_label_0941_replay/STATE_INPUT_REBUILD.json`
- 09:41退出与V20全链结果：`strategy-research/kangdie/explore/v16_rolling7_true_defense_20260831/entry_label_0941_replay/FULL_CHAIN_REPLAY.json`
- 09:41人类可读报告：`strategy-research/kangdie/explore/v16_rolling7_true_defense_20260831/entry_label_0941_replay/REPORT.md`
