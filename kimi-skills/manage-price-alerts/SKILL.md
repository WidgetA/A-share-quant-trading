---
name: manage-price-alerts
description: 创建、查看或取消持仓股票的价格预警。用户说“某只持仓跌破/低于某价提醒我”“涨到/突破某价通知我”“看看当前预警”“取消某只股票的预警”或使用 /预警 时使用；规则由服务端持续监控，命中后发飞书消息，不执行交易。
---

# 管理持仓价格预警

只调用下面的确定性脚本管理规则。不要使用 Kimi 的 Cron 工具，不要自己常驻轮询，不要直接改数据库或其他文件。

脚本路径：

```bash
"$ASSISTANT_SKILLS_DIR/manage-price-alerts/scripts/manage_price_alert.py"
```

## 创建预警

1. 从用户原话中提取股票名称或六位代码、方向和价格。
2. “跌破、低于、到 68 以下”映射为 `below`；“突破、涨到、到 68 以上”映射为 `above`。
3. 股票或价格缺失时，只向用户追问缺失项，不猜。
4. 原样执行一种命令：

```bash
python "$ASSISTANT_SKILLS_DIR/manage-price-alerts/scripts/manage_price_alert.py" create --stock "立航科技" --direction below --price "68"
python "$ASSISTANT_SKILLS_DIR/manage-price-alerts/scripts/manage_price_alert.py" create --stock "603261" --direction above --price "72.50"
```

脚本会从只读持仓接口核对股票；不是当前持仓、名称有歧义、价格非法时会拒绝。成功后用大白话回复股票、条件、预警编号和“一次性触发、只通知不交易”。如果返回 `created=false`，明确告诉用户同一规则已经存在，不要说又新建了一条。

## 查看预警

用户说“预警列表、现在监控哪些、看看预警”时执行：

```bash
python "$ASSISTANT_SKILLS_DIR/manage-price-alerts/scripts/manage_price_alert.py" list --status active
```

如用户明确要历史记录，改用 `--status all`。逐条报告编号、股票、触发条件和状态；空列表就说当前没有生效中的价格预警。

## 取消预警

优先使用用户给出的预警编号；也可以按股票取消该股票全部生效中的规则：

```bash
python "$ASSISTANT_SKILLS_DIR/manage-price-alerts/scripts/manage_price_alert.py" cancel --id "12ab34cd56ef"
python "$ASSISTANT_SKILLS_DIR/manage-price-alerts/scripts/manage_price_alert.py" cancel --stock "立航科技"
```

只按脚本实际返回的取消数量回复。数量为 0 时，说明没有找到对应的生效中预警。

## 硬规则

- 价格阈值完全照抄用户数字，不四舍五入之外推、不替用户决定。
- 每条规则只触发一次；飞书发送失败由服务端保留并重试。
- 预警不是委托，不得调用买入、卖出、撤单或交易接口。
- 脚本报错就把具体原因翻成大白话，不得假装创建成功。
