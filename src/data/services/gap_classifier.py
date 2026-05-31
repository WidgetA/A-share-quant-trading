# === MODULE PURPOSE ===
# Classify每一个数据缺口 (某天某只股票缺数据) 成三类,好对症修:
#   A = 名单错了    : 那天这只票根本不该在名单上 (还没上市 / 已退市)
#                     → 修名单 (AI 核对上市/退市日),不用下数据
#   B = 真的漏了    : 那天这只票确实在交易,但我们没下到 → 精准重下
#   C = 本就没全数据: 停牌 (该补占位) / 上市首日半天交易 (正常) → 补占位或记为已知正常
#   PENDING = 还没法判: 这只票还没经 AI 核对上市日,先核对再归类
#
# 纯函数,不连库/不发网络,方便单测。取数 + 上报在 runner 里 (endpoint/script)。

from __future__ import annotations

from datetime import date

# 分类标签
CLASS_A = "A"  # 名单不该有它
CLASS_B = "B"  # 真缺,重下
CLASS_C = "C"  # 本就无完整数据(停牌/半天)
PENDING = "PENDING"  # 待 AI 核对上市日

_EXPECTED_BARS_PER_DAY = 241


def classify_daily_gap(
    day: date,
    listing_meta: dict | None,
    suspended: bool,
) -> tuple[str, str]:
    """某只票在名单上、却没有当天日线记录 —— 归类 + 给出人话原因。

    listing_meta: AI 核对结果 {list_date, delist_date, verified} 或 None(没核过)。
    suspended:    当天是否在停牌列表里。
    """
    ld = listing_meta.get("list_date") if listing_meta else None
    dd = listing_meta.get("delist_date") if listing_meta else None
    verified = bool(listing_meta.get("verified")) if listing_meta else False

    if ld is not None and day < ld:
        return CLASS_A, f"名单错:当天还没上市(上市日 {ld})→ 从名单剔除"
    if dd is not None and day >= dd:
        return CLASS_A, f"名单错:当天已退市(退市日 {dd})→ 从名单剔除"
    if suspended:
        return CLASS_C, "停牌却没占位记录 → 补一条停牌占位"
    if listing_meta is None or not verified:
        return PENDING, "待 AI 核对上市/退市日后再判"
    return CLASS_B, "已上市、未停牌却无日线 → 真缺,精准重下日线"


def classify_minute_gap(
    day: date,
    listing_meta: dict | None,
    minute_bar_count: int,
    source_bar_count: int | None,
) -> tuple[str, str]:
    """某只票有当天日线(在交易)、但分钟不足 241 根 —— 归类 + 原因。

    minute_bar_count: 当天已存的分钟根数 (0 表示一根都没有)。
    source_bar_count: Tushare stk_mins 当天实际返回的 1min 根数。
    """
    ld = listing_meta.get("list_date") if listing_meta else None

    if ld is not None and day == ld:
        return CLASS_C, "上市首日,当天半天交易,分钟本就不满 241 根 → 已知正常"

    if source_bar_count is None:
        return PENDING, "待核对数据商 stk_mins 当天实际根数后再判"

    if source_bar_count >= _EXPECTED_BARS_PER_DAY:
        if minute_bar_count <= 0:
            return CLASS_B, "数据商有 241 根,库里一根都没有 → 库漏存,重下分钟"
        if minute_bar_count < _EXPECTED_BARS_PER_DAY:
            return (
                CLASS_B,
                f"数据商有 241 根,库里只有 {minute_bar_count}/{_EXPECTED_BARS_PER_DAY}"
                " → 库漏存,重下分钟",
            )
        return CLASS_B, "数据商有完整分钟,库侧缺口 → 重下分钟"

    if minute_bar_count < source_bar_count:
        return (
            CLASS_B,
            f"库里 {minute_bar_count} 根少于数据商 {source_bar_count} 根"
            " → 库漏存,重下到源头实际根数",
        )

    if source_bar_count == 0:
        return CLASS_C, "数据商当天也无 1min 数据 → 源头不足/停牌口径,不是库漏存"

    return (
        CLASS_C,
        f"数据商当天仅返回 {source_bar_count}/{_EXPECTED_BARS_PER_DAY} 根,"
        "库里已与源头一致 → 源头不足/低成交量按成交分钟返回,不是错",
    )


def summarize_classes(items: list[dict]) -> dict[str, int]:
    """统计各类计数。items 每条形如 {"class": "A"/"B"/"C"/"PENDING", ...}。"""
    out = {CLASS_A: 0, CLASS_B: 0, CLASS_C: 0, PENDING: 0}
    for it in items:
        c = it.get("class")
        if c in out:
            out[c] += 1
    return out
