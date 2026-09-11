# 防回归检查的失败证据

原生产提交：`a87fe0fccf43135642070950c1bef7f7f130e4b4`。
2026-09-12 在未修改生产代码时新增 `tests/unit/web/test_selection_task_contract.py`，
运行 `python -m pytest tests/unit/web/test_selection_task_contract.py -q --tb=short`。

结果：4 failed。

| 场景 | 原实现的实际行为 | 违反的用户要求 |
| --- | --- | --- |
| 当日已运行，09:45 再按按钮 | 保存 DATA_ALERT 核查通知 | 应再次执行普通选股任务并保存选股结果 |
| 当日已运行，14:45 再按按钮 | 保存 DATA_ALERT 核查通知 | 同上，不得按时间分出手工核查链路 |
| 当日已运行，19:45 再按按钮 | 保存 DATA_ALERT 核查通知 | 同上 |
| 长江通信卖出提醒 | 使用“模型腿”、D0/rank、内部编号 | 应用股票、推荐日期和卖出范围说明操作 |

这份记录证明检查在修复前能够抓住问题，不代表已修复。
发布工作流的 `build-and-push` 已增加对 `selection-contract` 的依赖。
修复后仍需真实数据库、两种入口的完整流程对照和部署后投递验收。
