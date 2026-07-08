---
name: check-holdings
description: 查询当前证券账户概览与持仓。用户在飞书发「/持仓」或问持仓/仓位/账户情况时用。调用本机只读接口,按主页「账户概览」的版式整理成大白话中文回复。
---

# 查账户概览 + 当前持仓

## 步骤

1. 用 Shell 依次执行两条命令(环境变量已由派单器注入,不要自己编造 key):

   ```
   curl -s -m 10 "$ASSISTANT_API_BASE/api/trading/equity-curve?days=30" -H "X-API-Key: $ASSISTANT_READONLY_KEY"
   curl -s -m 10 "$ASSISTANT_API_BASE/api/trading/holdings" -H "X-API-Key: $ASSISTANT_READONLY_KEY"
   ```

2. 第一条(账户概览)取两块:
   - `current`:`total_asset` 总资产、`market_value` 持仓市值、`cash` 可用资金、
     `today_pnl` / `today_pnl_pct` 今日盈亏额和百分比(为 null 说明还没有昨日快照,
     写「今日变动暂无」);
   - `weekly`(按周升序,每项 `{year, week, end_asset, return_pct}`):最后一项是本周,
     `return_pct` 即本周收益率(%);若存在倒数第二项,本周金额 = 最后一项 `end_asset`
     − 倒数第二项 `end_asset`,否则只报百分比。

3. 第二条返回 `holdings` 数组,每只:`code` / `name` / `quantity`(股数)/ `avg_price`
   (成本价)/ `last_price`(现价)/ `market_value`(市值)/ `pnl` / `pnl_pct`(浮动盈亏
   额和百分比)。字段为 null 就写「--」,不许估算。

4. 按主页版式回复(大白话中文,金额保留两位小数带千分位,盈亏带正负号),示例:

   ```
   账户概览
   总资产 994,373.70(今日 +1,431.10,+0.14%)
   持仓市值 0.00 | 可用资金 373.70
   本周 -8,509.07(-0.85%)

   持仓明细(共 1 只)
   国债逆回购(888880):9,940 股,成本 --,现价 --,市值 --,盈亏 --
   ```

   `holdings` 为空数组 → 明细部分如实写「当前没有持仓」。

## 硬性规则

- **一个数都不许编**。接口给什么报什么;null 写「--」或「暂无」,不准估算不准脑补。
- 任一条 curl 失败 / 超时 / 返回错误(401、500、503、连接拒绝等)→ 如实报告哪个接口
  没通 + 具体原因(用人话解释,比如「净值接口没通(返回码 503,数据库没连上)」),
  另一条正常就照常报它的部分,**绝不**输出任何编造的数字。
- 回复全部大白话中文,不出现英文字段名。
- 只调上面这两个接口。不要尝试其他端点、不要读写任何文件(结果文件除外)、不要碰数据库。
