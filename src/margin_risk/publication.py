"""两融数据的发布时点：决定「此刻已经发布的最新交易日」。

两融数据由交易所在盘后统一汇总，Tushare 的 ``margin`` / ``margin_detail`` 要到
**下一交易日 09:10（北京时间）**才拿得到上一交易日的数据。这是数据本身的可用边界，
不是故障：凌晨 3 点的数据维护流水线、容器重启后的启动补全都早于这个时点，此时上一
交易日的数据本来就还不存在，不该被当成缺口去抓、更不该记成摄取失败。

这里是唯一的判定入口：所有补数路径都用它截断目标末日，调度器则排在发布时点之后。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

# 上游发布时点（北京时间）。早于它，上一交易日的数据一定还不存在。
MARGIN_PUBLISH_TIME = time(9, 10)


def latest_published_trade_date(
    open_days: Sequence[date],
    *,
    now: datetime,
) -> date | None:
    """返回 ``now`` 时刻两融数据已发布的最新交易日；没有任何一天可用则返回 ``None``。

    ``open_days`` 必须是升序排列的交易日（非交易日不在其中）。

    交易日 D 的数据在 D 之后的第一个交易日 09:10 发布，因此判定分两步：
    先找出「最近一次发布实际发生过的交易日 P」——P 早于今天，或 P 就是今天且已过
    09:10；P 当天发布的正是 P 的**前一个交易日**。

    这样跨周末/长假也正确：周一 03:00 时 P 是上周五（上周五 09:10 发布过上周四的
    数据），可用末日是上周四；上周五的数据要等周一 09:10。
    """

    if now.tzinfo is None:
        raise ValueError("latest_published_trade_date requires a timezone-aware datetime")

    now_beijing = now.astimezone(BEIJING_TZ)
    today = now_beijing.date()
    published_on_index: int | None = None
    for index in range(len(open_days) - 1, -1, -1):
        day = open_days[index]
        if day > today:
            continue
        if day == today and now_beijing.time() < MARGIN_PUBLISH_TIME:
            continue
        published_on_index = index
        break

    if published_on_index is None or published_on_index == 0:
        return None
    return open_days[published_on_index - 1]
