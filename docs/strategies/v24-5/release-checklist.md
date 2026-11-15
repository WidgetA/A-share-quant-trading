# V24.5 发布与验收

这是内部执行记录与验收方法，不增加用户操作。业务范围见
[implementation-plan.md](implementation-plan.md)。它替换现有通知策略，不代表已接券商自动下单。

## 已核生产入口

- 2026-11-15 只读核实：生产为 `8.159.150.224:8000`，`GET /api/status` 与远端 Docker 的 `GIT_COMMIT` 都是 `e80abb8ebc9324de08a2943c3489e7b75e9fbdb6`；当前 V22-slim、embedded forward-shadow、运行健康。
- 实际容器 `root_trading-service_1`，命令 `python scripts/main.py`；镜像为 ACR `ashare/trading-service:latest`，原镜像 ID `sha256:a59eb0db3805778e7b8e708d90727e7a1645804cd116b30941079df56d23a484`。
- `/root/docker-compose.yml` 管理服务。必须保留 `root_trading-data` 和 `root_trading-logs` 两个命名卷、数据库及既有 outbox；不能用删除卷或清表实现升级。
- `root_watchtower_1` 以 `--interval 60 --label-enable --cleanup` 自动更新 `latest`。现为单服务容器，更新包含重启；没有零中断或双活发布的证据。
- 原研究树 `.env.local` 的 `SERVER_IP` 指旧测试机 `8.133.23.9`。不得用它选择生产目标。凭据只能在进程中读取，不输出内容。
- 已核部署包装脚本是生产树 `reports/non-trading-day-20261109/deploy_verified.py`：固定连接上述生产机，核精确 SHA 的 CI 后调用 `reports/deploy_v22_board_blacklist_verified_20260930.py`。后者接受完整 SHA：`python3 /tmp/<已核部署脚本>.py <40位提交SHA>`。它拉取 SHA 镜像、核镜像提交、停止旧容器并保留卷重建。不得直接运行旧包装脚本部署其历史固定提交。

## 激活与状态连续性

`src/web/app.py:_create_default_v20_service` 默认 `V20_EMBEDDED_ENABLED=true`；没有显式 `V20_ENABLED`、`V20_MODE`、`V20_ALLOW_PRODUCTION_PUSH` 时走 `from_legacy_runtime`。
V24.5 两个 service factory 改读 `config/v24-5.yaml`，保留现 embedded profile、通知路由、`official_stream_id` 和 `state_lineage_id`。不能为了版本名称改为 `production_push` 并切换流。

原 V22 风险状态、历史 paper 输入、旧持仓和待发消息保留；新的龟状态是分开的持久化数据。
若原状态语义中包含版本名，应以明确的原信号语义合同兼容，不能通过伪造 hash、删除状态、重建历史或清空持仓绕过。
新增迁移必须能在含旧记录的 schema 上重入；新镜像启动后旧未成交记录仍须可见。

## 行为验收

| 用户所求结果 | 必须看到的行为与反例 |
|---|---|
| 原信号上加龟状态 | 同一输入原排名、Top3 和原 gate 与 V22 相同；龟 permit 不反写 paper 或原风险状态。初始 TURTLE；第三个连续原开日可恢复，第五个连续原关日转龟。正常状态前四个关日仍执行换工商目标，次个原开日可恢复。 |
| 状态连续、不重复推进 | 连续交易日而非自然日；UNKNOWN 不当关。重启恢复；同日新按钮仍重算发消息而计数不再增加；同请求重试不重复提交；并发与事务失败不留下半套状态/持仓/outbox。 |
| 换仓提醒可执行且不假成交 | 旧已提醒但未卖股票仍在；T+1、未知数量、不可成交均保留。先卖可卖股票、仅实际可用款买工商；恢复先卖工商再按原股票分配。未付股息不是可用现金；参考价不是确认成交，工商不套股票 R33。 |
| 版本完整切换 | 实际两个 factory、状态 API、shared task、模型、canonical 编解码、realtime/current-date 边界均识别 V24.5；不得改名后退回 V20 模型、跳过日历或丢掉持仓登记。 |
| 原有运行不受损 | V16 保持独立的客户端、状态、日历、scheduler、数据库池及 lifecycle；V24.5 初始化失败或停止不停止 V16。原出口持仓继续原规则。 |
| 完整生产任务 | 新请求实际当前取数、计算、同事务持久化、生成消息；入场说明和票单各自确认 SENT。HTTP 受理、CI、健康、单条通知或离线算例均不能代替。 |

独立新版本日历/入口回归：`tests/unit/web/test_v24_5_release_contract.py`。
旧实现真实失败保存在 `reports/v245_release/before.txt`：新版本绕过非交易日检查、直接任务触发 MEWS、live-exit 仍运行；相邻正常交易日共享入口当时通过。

## CI 与真实数据库

`.github/workflows/ci.yml` 保留五个发布前置门禁：selection-contract、lint、typecheck、test、postgres-integration。
selection-contract 显式运行新增 V24.5 验收；PostgreSQL 16 job 显式运行新增策略状态/换仓事务测试。
真实 PG 使用独立测试 schema；不连接生产表。必须检查日志的实际执行和数量，不能将环境缺失导致 skip 记为通过。
本地验证环境与 CI Python 3.11 可能不同，必须以目标提交的 CI 结果补齐运行时验证。

只有 push 到 main 且 `ACR_ENABLED=true` 才构建推送 `latest` 与完整 SHA 标签；镜像发布依赖五个门禁。
由于生产 Watchtower 自动跟随，push/merge main 会触发实际上线，不能把它当作只上传代码。
历史已核 CI：[42553377206](https://github.com/WidgetA/A-share-quant-trading/actions/runs/42553377206)；它只证明旧 e80abb8 提交，不证明新代码。

## 部署后检查与非交易日边界

1. 核实际容器的 commit、镜像 ID、命名卷和公网状态均对应新提交，启动/后台任务/通知队列健康；保留旧镜像和状态快照标识。
2. 只读健康证据可用：`python scripts/verify_v24_5_release.py --status-url http://8.159.150.224:8000/api/status --expected-commit <SHA> --output <receipt.json>`。该脚本只有 GET，不触发任务、不发送消息，不能单独宣布完整上线验收。
3. 用真实当天按钮调用现有 `POST /api/v20/trigger-scan`，新 `Idempotency-Key` 表示新任务；同编号重试用于验证去重。不得改日期或从历史补票。请求响应、selection_runs、对应 outbox 及投递回执绑定到当前部署提交。
4. 2026-11-15 非交易日应是 `NON_TRADING_DAY`、`NOT_RUN`、`NOT_REQUIRED`，无行情请求、无票单、无新持仓或策略计数推进。日历取不到是失败，不能伪装为休市。该验收只证明休市分支。
5. 有数据的真实交易日，继续由助手核当前 realtime、结果持久化、入场说明与票单两条 `SENT`。保存响应可加 `--task-file <response.json> --expected-date <真实当天ISO日期>` 校核；还须核实际存储的 event IDs。非交易日和离线 fixture 不替代这一项，不要求用户额外点击或启用。

## 回滚

先保留旧 commit、镜像 ID/完整 SHA 标签、compose 配置、数据库迁移版本与状态/outbox 边界。
**本次迁移后不能直接运行旧 e80abb8 镜像**：`008_v245_positions.sql` 将持仓批次唯一键从三列变为包含 `reference_kind` 的四列；旧 V22 的三列 `ON CONFLICT` 会因没有匹配约束而失败。

必要回退应以本次代码为底，保留兼容数据库 adapter、007/008 迁移、新持仓/抑制记录和 V22 family 支持，仅将 `V20Service.from_default_config` 与 `from_legacy_runtime` 默认配置改回 `config/v22-slim.yaml`，形成一个新的兼容回退提交及镜像。[准备补丁](rollback-default-factory.patch)只供在准确目标版本上检查、测试和制作提交；未应用、未发布。不得删除新数据或回退唯一键来迎合旧代码。

该兼容回退镜像须通过真实 PG 的已有状态恢复、旧 V22 新批次登记、旧持仓/工商记录保留和 outbox 检查。部署时固定到这个**兼容回退提交**的不可变 SHA/摘要并保持原卷。
只把本地 `latest` retag 成任何旧镜像仍不是可靠回滚：Watchtower 会再次拉 registry 的 `latest`。须固定服务镜像或使用等价的既有版本固定机制。
恢复后核实际回退 SHA、运行状态和 outbox，再处理失败任务；已确认送达的消息不可重复冒充新结果。
