# 旧推荐持仓的卖出状态同步

2026-09-15 用户确认：9 月 11 日推荐的 10 只股票已全部卖完，要求同步到系统。
旧每日卖出清单读取 V20 推荐记录，原先没有用户剩余持仓状态，因此实际卖完后仍会列出。

## 接口

沿用 `X-V20-API-Key` 鉴权。`GET /api/v20/legacy-positions` 返回旧推荐的股票、
推荐日期、`position_id`、剩余股数 `quantity`、状态 `status`、当前版本 `revision`。
未同步的记录为 `UNCONFIRMED`，数量未知；策略参考价不作为用户实际成交价。

```http
POST /api/v20/legacy-positions/<position_id>/calibrate
X-V20-API-Key: <配置的密钥>
Idempotency-Key: sold-20260915-001
Content-Type: application/json

{"expected_revision": 0, "quantity": 0}
```

数量 0 表示已全部卖出（`CLOSED`）；正数表示实际剩余股数（`MONITORING`）；
未买入使用 `status: NOT_BOUGHT`。每笔推荐单独同步，不按股票代码关闭其他日期的记录。
重新启用已关闭记录需要提供正数余量。同请求编号重试返回原结果，内容改变或版本过期返回 409。
每次修改保存前后值与服务端接收时间；不虚构卖出价、实际卖出时间、成交单或策略退出信号。

## 清单与提醒

已卖完、未买入的记录不进入新生成的每日卖出清单，不生成后续卖出提醒。
投递队列领取及发送前均检查最新状态；领取后才同步的提醒也不会再开始发送。
已经跨过发送边界的请求和已发出的消息不能撤回，送达未知仍保留未知，不能伪记已送达。
历史推荐、策略参考收益及已保存消息保留原记录；新请求重新计算时读取当前持仓状态。
新 V22 持仓继续使用现有 `/api/v20/v22-positions` 校准接口。

## 回归证据与发布检查

修改实现前，在 main `579440f` 上新增
`tests/unit/web/test_legacy_position_sync.py`，运行结果为 9 failed：
已卖完请求应返回 200，实际返回 404 `{"detail":"Not Found"}`；参数校验同样没有入口。
之后保留原断言实现接口。

`tests/integration/data/database/test_legacy_position_sync_postgres.py` 使用真实 PostgreSQL，
通过 HTTP 验证校准、并发重试、版本冲突、重启读取、每日消息过滤、修改审计与跨策略隔离；
同时验证已排队和已领取的退出消息、后续提醒在关闭后不再发送，也不伪造 SENT。
CI 的 Selection Task Contract 与 PostgreSQL Integration Tests 均纳入这些检查，
镜像发布依赖两项通过。上线后须核对部署提交、10 条实际校准回读，并按完整任务合同验收。
