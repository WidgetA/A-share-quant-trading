# === MODULE PURPOSE ===
# 数据缺口诊断报告:把每日自动补全报的三类"错"(股票数偏多 / 日线缺 / 分钟缺)
# 逐条用新名单系统核查,产出一份"问题 → 根因 → 真实应该是多少 → 怎么修"的飞书报告。
#
# 复用:
#   - storage.audit_daily_gaps / audit_minute_gaps  (哪些天有缺口)
#   - storage.get_effective_universe_for_date / get_codes_for_daily_date (日线缺哪些票)
#   - storage.find_missing_minute_stocks / get_minute_bar_counts (分钟缺哪些票)
#   - storage.get_listing_info_all (AI 核对的上市/退市日) + 当天停牌列表
#   - gap_classifier.classify_daily_gap / classify_minute_gap (归 A/B/C/PENDING)
#
# build_report 是纯函数(吃诊断数据出文本),单测覆盖。diagnose_gaps 负责取数。

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Awaitable, Callable
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.clients.greptime_storage import create_storage_from_config  # noqa: E402
from src.data.services.gap_classifier import (  # noqa: E402
    CLASS_A,
    CLASS_B,
    CLASS_C,
    PENDING,
    classify_daily_gap,
    classify_minute_gap,
    summarize_classes,
)

REPORT_JSON_FILE = Path("data/audit/gap_diagnosis_report.json")
REPORT_MARKDOWN_FILE = Path("data/audit/gap_diagnosis_report.md")
REPORT_FILE = REPORT_JSON_FILE
_MINUTE_DETAIL_DAYS = 0  # 0 = 分钟缺口全量分类;完整逐天明细写入 data/audit/
_SAMPLE_LINES = 8  # 报告里每类列几条样本
_DAILY_CODES_IN_FEISHU = 10
_DAILY_DAYS_IN_FEISHU = 10  # cap per-day daily blocks in the Feishu msg (full list → file)
_MINUTE_B_DAYS_IN_FEISHU = 10
# Hard ceiling on the Feishu message. The relay/Feishu silently rejects very
# large text (then send_message burns 20 retries and _notify_feishu swallows
# the failure → "报告啥都没有"). Keep the message well under the limit; the
# complete per-day detail always lives in the markdown file.
_FEISHU_MAX_CHARS = 8000


MinuteSourceCountFetcher = Callable[[date, str], Awaitable[int]]


async def diagnose_gaps(
    storage,
    fetch_suspended,
    fetch_minute_source_count: MinuteSourceCountFetcher | None = None,
    *,
    daily_detail_days: int | None = None,
    minute_detail_days: int = _MINUTE_DETAIL_DAYS,
    max_minute_source_checks: int | None = None,
):
    """取数 + 逐条归类。fetch_suspended: async (day)->set[str] 当天停牌代码。"""
    listing_info = await storage.get_listing_info_all()
    all_time_count = await storage.get_daily_stock_count()
    per_day_counts = await storage.get_daily_counts_per_day()

    # --- 日线缺口分类 ---
    # 每个缺口天要查一次 suspend_d(Tushare 限 500 次/分钟)。AI 未核完前缺口天
    # 可能成百上千,所以 daily_detail_days 限定只详分最近 N 天(其余只计数),
    # suspend_d 调用随之被限,绝不超频。完整逐天明细另写文件。
    daily_gaps = await storage.audit_daily_gaps()  # [(date, expected, actual)]
    daily_sorted = sorted(daily_gaps, key=lambda g: g[0])
    daily_detail = daily_sorted[-daily_detail_days:] if daily_detail_days else daily_sorted
    daily_items: list[dict] = []
    daily_days: list[dict] = []
    for gday, expected_count, actual_count in daily_detail:
        expected_codes = await storage.get_effective_universe_for_date(gday)
        have = await storage.get_codes_for_daily_date(gday)
        missing = expected_codes - have
        suspended = await fetch_suspended(gday) if missing else set()
        day_items: list[dict] = []
        for code in sorted(missing):
            cls, why = classify_daily_gap(gday, listing_info.get(code), code in suspended)
            item = {"date": gday.isoformat(), "code": code, "cls": cls, "reason": why}
            daily_items.append(item)
            day_items.append(item)
        by_class = _group_codes_by_class(day_items)
        true_missing = len(by_class[CLASS_B]) + len(by_class[CLASS_C])
        correct_count = expected_count - len(by_class[CLASS_A])
        daily_days.append(
            {
                "date": gday.isoformat(),
                "expected_count": expected_count,
                "actual_count": actual_count,
                "reported_missing_count": max(0, expected_count - actual_count),
                "classified_missing_count": len(day_items),
                "correct_count_after_fix": correct_count,
                "rows_to_add": true_missing,
                "class_counts": _class_counts(day_items),
                "missing_by_class": by_class,
                "problem": _daily_problem_text(by_class),
                "root_cause": _daily_root_cause_text(by_class),
                "correct_number": (
                    f"当天正确日线股票数应为 {correct_count} 只"
                    f"(原期望 {expected_count} - 名单误报 {len(by_class[CLASS_A])});"
                    f"库内现有 {actual_count} 只,需补 {true_missing} 条。"
                ),
                "fix": _daily_fix_text(by_class),
            }
        )

    # --- 分钟缺口:天数多,全量写文件;飞书只展开可修的库漏存(B) ---
    minute_gaps = await storage.audit_minute_gaps()  # [(date, expected, complete)]
    minute_sorted = sorted(minute_gaps, key=lambda g: g[0])
    detail_days = minute_sorted[-minute_detail_days:] if minute_detail_days else minute_sorted
    minute_items: list[dict] = []
    minute_days: list[dict] = []
    source_checks = 0  # bound Tushare stk_mins calls per run (small box / rate limit)
    for gday, expected_count, complete_count in detail_days:
        incomplete, _ = await storage.find_missing_minute_stocks(gday)
        if not incomplete:
            continue
        barcounts = await storage.get_minute_bar_counts(gday)
        day_items = []
        for code in sorted(incomplete):
            local_count = barcounts.get(code, 0)
            # Source-check is one Tushare call per stock; cap it so the report
            # never hammers the API / the 1.58G box. Beyond the cap, leave
            # source_count=None → classified PENDING ("待核对数据商根数"), honest.
            source_count = None
            if fetch_minute_source_count is not None and (
                max_minute_source_checks is None or source_checks < max_minute_source_checks
            ):
                source_count = await fetch_minute_source_count(gday, code)
                source_checks += 1
            cls, why = classify_minute_gap(gday, listing_info.get(code), local_count, source_count)
            item = {
                "date": gday.isoformat(),
                "code": code,
                "cls": cls,
                "local_bar_count": local_count,
                "source_bar_count": source_count,
                "reason": why,
            }
            minute_items.append(item)
            day_items.append(item)
        by_class = _group_codes_by_class(day_items)
        bars_to_add = _minute_bars_to_add(day_items)
        minute_days.append(
            {
                "date": gday.isoformat(),
                "expected_stock_count": expected_count,
                "complete_stock_count": complete_count,
                "reported_incomplete_count": max(0, expected_count - complete_count),
                "classified_incomplete_count": len(day_items),
                "correct_complete_count_after_fix": complete_count + len(by_class[CLASS_B]),
                "bars_to_add": bars_to_add,
                "class_counts": _class_counts(day_items),
                "missing_by_class": by_class,
                "problem": _minute_problem_text(day_items),
                "root_cause": _minute_root_cause_text(by_class),
                "correct_number": _minute_correct_number_text(
                    complete_count, by_class, bars_to_add
                ),
                "fix": _minute_fix_text(by_class),
            }
        )

    verified = sum(1 for v in listing_info.values() if v.get("verified"))
    pd_vals = list(per_day_counts.values())
    return {
        "all_time_count": all_time_count,
        "per_day_min": min(pd_vals) if pd_vals else 0,
        "per_day_max": max(pd_vals) if pd_vals else 0,
        "per_day_recent": per_day_counts[max(per_day_counts)] if per_day_counts else 0,
        "delisted_in_listing": sum(1 for v in listing_info.values() if v.get("delist_date")),
        "listing_total": len(listing_info),
        "listing_verified": verified,
        "daily_gap_days": len(daily_gaps),
        "daily_detail_days": len(daily_detail),
        "daily_detail_is_full": len(daily_detail) == len(daily_gaps),
        "daily_days": daily_days,
        "daily_items": daily_items,
        "minute_gap_days": len(minute_gaps),
        "minute_detail_days": len(detail_days),
        "minute_detail_is_full": len(detail_days) == len(minute_gaps),
        "minute_source_checks": source_checks,
        "minute_days": minute_days,
        "minute_items": minute_items,
    }


def _class_counts(items: list[dict]) -> dict[str, int]:
    return summarize_classes([{"class": it["cls"]} for it in items])


def _group_codes_by_class(items: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {CLASS_A: [], CLASS_B: [], CLASS_C: [], PENDING: []}
    for it in items:
        grouped.setdefault(it["cls"], []).append(
            {
                "code": it["code"],
                "reason": it["reason"],
                **({"local_bar_count": it["local_bar_count"]} if "local_bar_count" in it else {}),
                **(
                    {"source_bar_count": it["source_bar_count"]} if "source_bar_count" in it else {}
                ),
            }
        )
    return grouped


def _codes(items: list[dict], limit: int | None = None) -> str:
    if not items:
        return "无"
    selected = items if limit is None else items[:limit]
    suffix = f" 等{len(items)}只" if limit is not None and len(items) > limit else ""
    return "、".join(str(it["code"]) for it in selected) + suffix


def _daily_problem_text(by_class: dict[str, list[dict]]) -> str:
    parts = []
    if by_class[CLASS_A]:
        parts.append(f"名单误报 {_codes(by_class[CLASS_A])}")
    if by_class[CLASS_B]:
        parts.append(f"日线真缺 {_codes(by_class[CLASS_B])}")
    if by_class[CLASS_C]:
        parts.append(f"停牌占位缺失 {_codes(by_class[CLASS_C])}")
    if by_class[PENDING]:
        parts.append(f"待核对 {_codes(by_class[PENDING])}")
    return "；".join(parts) if parts else "无缺口"


def _daily_root_cause_text(by_class: dict[str, list[dict]]) -> str:
    parts = []
    if by_class[CLASS_A]:
        parts.append("stock_snapshot/effective universe 含当天不该在册股票")
    if by_class[CLASS_B]:
        parts.append("Tushare daily 入库漏了在册且未停牌股票")
    if by_class[CLASS_C]:
        parts.append("停牌股票没有写 is_suspended=true 占位行")
    if by_class[PENDING]:
        parts.append("上市/退市日尚未 AI 核对,不能安全定性")
    return "；".join(parts) if parts else "无"


def _daily_fix_text(by_class: dict[str, list[dict]]) -> str:
    parts = []
    if by_class[CLASS_A]:
        parts.append("A: 按已核上市/退市日修正名单,当天剔除这些代码")
    if by_class[CLASS_B]:
        parts.append("B: 对这些(日期,股票)精准重下日线")
    if by_class[CLASS_C]:
        parts.append("C: 用 prev_close 补停牌占位记录")
    if by_class[PENDING]:
        parts.append("PENDING: 先跑 listing-info verify,再重新诊断")
    return "；".join(parts) if parts else "无"


def _minute_bars_to_add(items: list[dict]) -> int | None:
    total = 0
    for it in items:
        if it["cls"] != CLASS_B:
            continue
        source_count = it.get("source_bar_count")
        if source_count is None:
            return None
        total += max(0, int(source_count) - int(it.get("local_bar_count", 0)))
    return total


def _minute_problem_text(items: list[dict]) -> str:
    by_class = _group_codes_by_class(items)
    parts = []
    if by_class[CLASS_B]:
        parts.append(f"库漏存 {_codes(by_class[CLASS_B])}")
    if by_class[CLASS_C]:
        parts.append(f"口径误报/源头不足 {_codes(by_class[CLASS_C])}")
    if by_class[PENDING]:
        parts.append(f"待源核对 {_codes(by_class[PENDING])}")
    return "；".join(parts) if parts else "无缺口"


def _minute_root_cause_text(by_class: dict[str, list[dict]]) -> str:
    parts = []
    if by_class[CLASS_B]:
        parts.append("本地分钟根数少于 Tushare stk_mins 当天实际返回根数")
    if by_class[CLASS_C]:
        parts.append("源头本身不足 241 根、上市首日半天或低成交量按成交分钟返回")
    if by_class[PENDING]:
        parts.append("未做源头根数核对,不能安全定性")
    return "；".join(parts) if parts else "无"


def _minute_correct_number_text(
    complete_count: int, by_class: dict[str, list[dict]], bars_to_add: int | None
) -> str:
    bars = "待源核对后确定" if bars_to_add is None else f"需补 {bars_to_add} 根分钟线"
    return (
        f"当天可修真错修完后,完整股票数应至少为 "
        f"{complete_count + len(by_class[CLASS_B])} 只;"
        f"C 类按源头实际根数/已知口径记账,不强行补到 241 根;{bars}。"
    )


def _minute_fix_text(by_class: dict[str, list[dict]]) -> str:
    parts = []
    if by_class[CLASS_B]:
        parts.append("B: 对这些(日期,股票)单日重下分钟线,补到源头实际根数")
    if by_class[CLASS_C]:
        parts.append("C: 记录为源头不足/半天/低成交量口径,不要按 241 根继续报警")
    if by_class[PENDING]:
        parts.append("PENDING: 先核对 stk_mins 源头根数")
    return "；".join(parts) if parts else "无"


def _samples(items: list[dict], n: int = _SAMPLE_LINES) -> str:
    rows = [f"    {it['date']} {it['code']} [{it['cls']}] {it['reason']}" for it in items[:n]]
    more = f"\n    …还有 {len(items) - n} 条" if len(items) > n else ""
    return ("\n".join(rows) + more) if rows else "    (无)"


def _minute_source_stats(items: list[dict]) -> dict[str, int]:
    out = {"source_full": 0, "source_short": 0, "source_pending": 0, "local_less": 0}
    for it in items:
        source_count = it.get("source_bar_count")
        local_count = int(it.get("local_bar_count", 0))
        if source_count is None:
            out["source_pending"] += 1
            continue
        if source_count >= 241:
            out["source_full"] += 1
        else:
            out["source_short"] += 1
        if local_count < source_count:
            out["local_less"] += 1
    return out


def _clip(text: object, n: int = 160) -> str:
    """Clip a per-day field for the Feishu message (full text → file)."""
    s = str(text)
    return s if len(s) <= n else s[:n] + "…"


def _daily_day_lines(days: list[dict]) -> list[str]:
    if not days:
        return ["  逐日: 无"]
    lines = ["  逐日:"]
    shown = days[:_DAILY_DAYS_IN_FEISHU]
    for day in shown:
        by_class = day["missing_by_class"]
        lines += [
            f"  - {day['date']}: {_clip(day['problem'])}",
            f"    根因: {_clip(day['root_cause'])}",
            f"    正确数字: {_clip(day['correct_number'])}",
            f"    怎么修: {_clip(day['fix'])}",
        ]
        if by_class[CLASS_B]:
            lines.append(f"    关键真缺: {_codes(by_class[CLASS_B], _DAILY_CODES_IN_FEISHU)}")
        if by_class[CLASS_C]:
            lines.append(f"    关键停牌占位: {_codes(by_class[CLASS_C], _DAILY_CODES_IN_FEISHU)}")
        if by_class[CLASS_A]:
            lines.append(f"    关键名单误报: {_codes(by_class[CLASS_A], _DAILY_CODES_IN_FEISHU)}")
    if len(days) > len(shown):
        lines.append(f"  - 其余 {len(days) - len(shown)} 个问题日见完整明细文件。")
    return lines


def _minute_b_day_lines(days: list[dict]) -> list[str]:
    b_days = [d for d in days if d["class_counts"][CLASS_B] > 0]
    if not b_days:
        return ["  可修真错逐日: 无"]
    lines = ["  可修真错逐日(B=库漏存):"]
    shown = b_days[:_MINUTE_B_DAYS_IN_FEISHU]
    for day in shown:
        by_class = day["missing_by_class"]
        lines += [
            f"  - {day['date']}: 库漏存 {day['class_counts'][CLASS_B]} 只"
            f"({_codes(by_class[CLASS_B], _DAILY_CODES_IN_FEISHU)})",
            f"    根因: {day['root_cause']}",
            f"    正确数字: {day['correct_number']}",
            f"    怎么修: {day['fix']}",
        ]
    if len(b_days) > len(shown):
        lines.append(f"  - 其余 {len(b_days) - len(shown)} 个 B 类问题日见完整明细文件。")
    return lines


def _minute_non_b_summary(days: list[dict]) -> list[str]:
    c_days = sum(1 for d in days if d["class_counts"][CLASS_C] > 0)
    p_days = sum(1 for d in days if d["class_counts"][PENDING] > 0)
    a_days = sum(1 for d in days if d["class_counts"][CLASS_A] > 0)
    return [
        f"  口径/源头类汇总: C 类 {c_days} 天 / PENDING {p_days} 天 / A {a_days} 天。",
        "  这些不在飞书逐日展开,完整逐日问题、根因、正确数字、修法见文件。",
    ]


def build_report(diag: dict) -> str:
    """把诊断数据拼成"问题→根因→该多少→怎么修"的飞书报告(纯函数)。"""
    daily = summarize_classes([{"class": it["cls"]} for it in diag["daily_items"]])
    minute = summarize_classes([{"class": it["cls"]} for it in diag["minute_items"]])
    minute_source = _minute_source_stats(diag["minute_items"])
    daily_days = diag.get("daily_days") or []
    minute_days = diag.get("minute_days") or []
    detail_file = diag.get("detail_markdown_file") or str(REPORT_MARKDOWN_FILE)
    lines: list[str] = ["[数据诊断报告] 自动补全后的按天详报", ""]

    # 问题1:股票数
    lines += [
        f"问题1 · 日线股票数 {diag['all_time_count']} (报警阈值 ≤5500)",
        "  根因:这个数是「历史累计出现过的所有代码」,含已退市的票;三年多累计超 5500 属正常,"
        "是报警阈值用错了(该按「每天在册」算,不该按历史累计)。",
        f"  真实应该:每天在册约 {diag['per_day_min']}~{diag['per_day_max']} 只"
        f"(最近一天 {diag['per_day_recent']} 只);累计里 AI 已核出已退市 "
        f"{diag['delisted_in_listing']} 只。",
        "  怎么修:报警阈值改成「按天计数」,不再用历史累计;多出来的核实都真实存在过(退市股)。",
        "",
    ]

    # 问题2:日线缺
    lines += [
        f"问题2 · 日线缺失 {diag['daily_gap_days']} 天,共 {len(diag['daily_items'])} 只次",
        f"  根因(逐条分类):A 名单错 {daily[CLASS_A]} / B 真缺 {daily[CLASS_B]} / "
        f"C 停牌没占位 {daily[CLASS_C]} / 待AI核对 {daily[PENDING]}",
        "  总体修法:A→按 AI 上市/退市日从名单剔除;B→精准重下那只那天日线;C→补停牌占位记录。",
    ]
    lines.extend(_daily_day_lines(daily_days))
    lines.append("")

    # 问题3:分钟缺
    detail_scope = "全量" if diag.get("minute_detail_is_full") else "抽样"
    lines += [
        f"问题3 · 分钟缺失 {diag['minute_gap_days']} 天"
        f"({detail_scope}分类 {diag['minute_detail_days']} 天,共 {len(diag['minute_items'])} 只次)",
        f"  根因:先核对 Tushare stk_mins 源头。源头有 241 根 {minute_source['source_full']} 只次 / "
        f"源头也不足 241 根 {minute_source['source_short']} 只次 / "
        f"待源核对 {minute_source['source_pending']} 只次;库少于源头 {minute_source['local_less']} 只次。",
        f"  分类:B 库漏存需重下 {minute[CLASS_B]} / "
        f"C 源头不足或半天、低成交量口径 {minute[CLASS_C]} / 待 {minute[PENDING]}",
        "  真实应该:库里应至少等于源头当天实际返回根数;源头本身不足 241 的,不是库错,"
        "也不能靠重下补成 241。",
        "  怎么修:B→单(天,股)重下分钟到源头实际根数;C→标记「源头不足/非完整交易日/低成交量口径」,"
        "不再按 241 根报警。",
    ]
    lines.extend(_minute_b_day_lines(minute_days))
    lines.extend(_minute_non_b_summary(minute_days))
    lines += ["", f"完整逐天明细: {detail_file}", ""]

    lines.append(
        f"底注:AI 上市/退市日已核对 {diag['listing_verified']}/{diag['listing_total']} 只。"
        "覆盖越全,A 类(名单错)判定越准——覆盖完再跑一次本报告,数字会更实。"
    )
    return "\n".join(lines)


def build_detail_markdown(diag: dict) -> str:
    """完整逐天明细文件:日线全列;分钟全列,含 B/C/PENDING。"""
    lines = [
        "# 数据缺口按天详报",
        "",
        "## 日线问题日",
        "",
    ]
    daily_days = diag.get("daily_days") or []
    if not daily_days:
        lines.append("无")
    for day in daily_days:
        lines += [
            f"### {day['date']}",
            f"- 问题: {day['problem']}",
            f"- 根因: {day['root_cause']}",
            f"- 正确数字: {day['correct_number']}",
            f"- 怎么修: {day['fix']}",
        ]
        for cls, title in (
            (CLASS_A, "A 名单误报"),
            (CLASS_B, "B 库漏存/真缺"),
            (CLASS_C, "C 停牌占位/口径"),
            (PENDING, "PENDING 待核对"),
        ):
            items = day["missing_by_class"][cls]
            if items:
                rendered = "、".join(f"{it['code']}({it['reason']})" for it in items)
                lines.append(f"- {title}: {rendered}")
        lines.append("")

    lines += ["## 分钟问题日", ""]
    minute_days = diag.get("minute_days") or []
    if not minute_days:
        lines.append("无")
    for day in minute_days:
        lines += [
            f"### {day['date']}",
            f"- 问题: {day['problem']}",
            f"- 根因: {day['root_cause']}",
            f"- 正确数字: {day['correct_number']}",
            f"- 怎么修: {day['fix']}",
        ]
        for cls, title in (
            (CLASS_B, "B 可修真错"),
            (CLASS_C, "C 口径误报/源头不足"),
            (CLASS_A, "A 名单误报"),
            (PENDING, "PENDING 待核对"),
        ):
            items = day["missing_by_class"][cls]
            if not items:
                continue
            rendered_parts = []
            for it in items:
                local = it.get("local_bar_count")
                source = it.get("source_bar_count")
                counts = f" local={local}, source={source}" if local is not None else ""
                rendered_parts.append(f"{it['code']}({it['reason']}{counts})")
            lines.append(f"- {title}: " + "、".join(rendered_parts))
        lines.append("")
    return "\n".join(lines)


def write_report_files(
    diag: dict,
    *,
    json_file: Path = REPORT_JSON_FILE,
    markdown_file: Path = REPORT_MARKDOWN_FILE,
) -> dict[str, str]:
    json_file.parent.mkdir(parents=True, exist_ok=True)
    markdown_file.parent.mkdir(parents=True, exist_ok=True)
    diag["detail_json_file"] = str(json_file)
    diag["detail_markdown_file"] = str(markdown_file)
    json_file.write_text(json.dumps(diag, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_file.write_text(build_detail_markdown(diag), encoding="utf-8")
    return {"json": str(json_file), "markdown": str(markdown_file)}


async def _notify_feishu(message: str) -> None:
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_message(message)
    except Exception as e:  # noqa: BLE001 — diagnostic tool, just warn
        print(f"⚠ 飞书发送失败: {e}", file=sys.stderr, flush=True)


async def run_diagnosis_report(
    storage,
    *,
    feishu: bool,
    daily_detail_days: int | None = None,
    minute_detail_days: int = _MINUTE_DETAIL_DAYS,
    max_minute_source_checks: int | None = None,
    stop_storage: bool = False,
) -> dict:
    """Run diagnosis, write full details, optionally notify Feishu.

    ``storage`` is usually the app's already-started Greptime storage. CLI passes
    a freshly-created storage and sets ``stop_storage=True``.
    """
    from src.common.config import get_tushare_token
    from src.data.clients.tushare_realtime import TushareRealtimeClient

    client = TushareRealtimeClient(token=get_tushare_token())
    await client.start()

    async def _fetch_suspended(day: date) -> set[str]:
        return await client.fetch_suspended_stocks(day.strftime("%Y%m%d"))

    async def _fetch_minute_source_count(day: date, code: str) -> int:
        ts_code = TushareRealtimeClient._to_ts_code(code)
        start = day.strftime("%Y-%m-%d") + " 09:00:00"
        end = day.strftime("%Y-%m-%d") + " 15:00:00"
        bars = await client.stk_mins(
            ts_code,
            freq="1min",
            start_date=start,
            end_date=end,
            limit=100000,
        )
        return len(bars)

    try:
        diag = await diagnose_gaps(
            storage,
            _fetch_suspended,
            _fetch_minute_source_count,
            daily_detail_days=daily_detail_days,
            minute_detail_days=minute_detail_days,
            max_minute_source_checks=max_minute_source_checks,
        )
    finally:
        await client.stop()
        if stop_storage:
            await storage.stop()

    files = write_report_files(diag)
    report = build_report(diag)
    if feishu:
        feishu_msg = report
        if len(feishu_msg) > _FEISHU_MAX_CHARS:
            feishu_msg = (
                feishu_msg[:_FEISHU_MAX_CHARS]
                + f"\n\n…(报告过长已截断,完整逐日明细见文件 {files['markdown']})"
            )
        await _notify_feishu(feishu_msg)
    return {"diag": diag, "report": report, "files": files}


async def main(*, feishu: bool, minute_detail_days: int) -> None:
    storage = create_storage_from_config()
    await storage.start()
    result = await run_diagnosis_report(
        storage,
        feishu=feishu,
        minute_detail_days=minute_detail_days,
        stop_storage=True,
    )
    print(result["report"], file=sys.stderr, flush=True)
    print(f"\nJSON明细: {result['files']['json']}", file=sys.stderr, flush=True)
    print(f"Markdown明细: {result['files']['markdown']}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="数据缺口诊断报告(临时工具)")
    parser.add_argument("--feishu", action="store_true", help="把报告发飞书")
    parser.add_argument("--minute-detail-days", type=int, default=_MINUTE_DETAIL_DAYS)
    args = parser.parse_args()
    asyncio.run(main(feishu=args.feishu, minute_detail_days=args.minute_detail_days))
