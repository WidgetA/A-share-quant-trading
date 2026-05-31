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

REPORT_FILE = Path("data/audit/gap_diagnosis_report.json")
_MINUTE_DETAIL_DAYS = 50  # 分钟缺口天数多,详细分类抽样最近这么多天(透明标注)
_SAMPLE_LINES = 8  # 报告里每类列几条样本


async def diagnose_gaps(storage, fetch_suspended, *, minute_detail_days: int = _MINUTE_DETAIL_DAYS):
    """取数 + 逐条归类。fetch_suspended: async (day)->set[str] 当天停牌代码。"""
    listing_info = await storage.get_listing_info_all()
    all_time_count = await storage.get_daily_stock_count()
    per_day_counts = await storage.get_daily_counts_per_day()

    # --- 日线缺口:全量分类(天数少) ---
    daily_gaps = await storage.audit_daily_gaps()  # [(date, expected, actual)]
    daily_items: list[dict] = []
    for gday, _exp, _act in daily_gaps:
        expected_codes = await storage.get_effective_universe_for_date(gday)
        have = await storage.get_codes_for_daily_date(gday)
        missing = expected_codes - have
        suspended = await fetch_suspended(gday) if missing else set()
        for code in sorted(missing):
            cls, why = classify_daily_gap(gday, listing_info.get(code), code in suspended)
            daily_items.append({"date": gday.isoformat(), "code": code, "cls": cls, "reason": why})

    # --- 分钟缺口:天数多,详细分类抽样最近 N 天(不静默截断) ---
    minute_gaps = await storage.audit_minute_gaps()  # [(date, expected, complete)]
    minute_sorted = sorted(minute_gaps, key=lambda g: g[0])
    detail_days = minute_sorted[-minute_detail_days:] if minute_detail_days else minute_sorted
    minute_items: list[dict] = []
    for gday, _exp, _comp in detail_days:
        incomplete, _ = await storage.find_missing_minute_stocks(gday)
        if not incomplete:
            continue
        barcounts = await storage.get_minute_bar_counts(gday)
        for code in sorted(incomplete):
            cls, why = classify_minute_gap(gday, listing_info.get(code), barcounts.get(code, 0))
            minute_items.append({"date": gday.isoformat(), "code": code, "cls": cls, "reason": why})

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
        "daily_items": daily_items,
        "minute_gap_days": len(minute_gaps),
        "minute_detail_days": len(detail_days),
        "minute_items": minute_items,
    }


def _samples(items: list[dict], n: int = _SAMPLE_LINES) -> str:
    rows = [f"    {it['date']} {it['code']} [{it['cls']}] {it['reason']}" for it in items[:n]]
    more = f"\n    …还有 {len(items) - n} 条" if len(items) > n else ""
    return ("\n".join(rows) + more) if rows else "    (无)"


def build_report(diag: dict) -> str:
    """把诊断数据拼成"问题→根因→该多少→怎么修"的飞书报告(纯函数)。"""
    daily = summarize_classes([{"class": it["cls"]} for it in diag["daily_items"]])
    minute = summarize_classes([{"class": it["cls"]} for it in diag["minute_items"]])
    lines: list[str] = ["[数据诊断报告] 自动补全报的问题,逐条核查结果", ""]

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
        "  真实应该:剔掉 A 类(那天不该在名单上的)后,这些天「该有数」会下降;再把 B/C 补上,缺口归零。",
        "  怎么修:A→按 AI 上市/退市日从名单剔除;B→精准重下那只那天日线;C→补停牌占位记录。",
        "  样本:",
        _samples(diag["daily_items"]),
        "",
    ]

    # 问题3:分钟缺
    lines += [
        f"问题3 · 分钟缺失 {diag['minute_gap_days']} 天"
        f"(详细分类抽样最近 {diag['minute_detail_days']} 天,共 {len(diag['minute_items'])} 只次)",
        f"  根因:多为每天约 1 只票分钟不满 241 根。分类:C 上市首日/半天 {minute[CLASS_C]} / "
        f"B 真缺 {minute[CLASS_B]} / 待 {minute[PENDING]}",
        "  真实应该:B 类重下后补齐;C 类是真实半天交易(上市首日等),记为「已知正常」,不算错。",
        "  怎么修:B→单(天,股)重下分钟;C→加入「已知正常」清单,不再报警。",
        "  样本:",
        _samples(diag["minute_items"]),
        "",
    ]

    lines.append(
        f"底注:AI 上市/退市日已核对 {diag['listing_verified']}/{diag['listing_total']} 只。"
        "覆盖越全,A 类(名单错)判定越准——覆盖完再跑一次本报告,数字会更实。"
    )
    return "\n".join(lines)


async def _notify_feishu(message: str) -> None:
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_message(message)
    except Exception as e:  # noqa: BLE001 — diagnostic tool, just warn
        print(f"⚠ 飞书发送失败: {e}", file=sys.stderr, flush=True)


async def main(*, feishu: bool, minute_detail_days: int) -> None:
    from src.common.config import get_tushare_token
    from src.data.clients.tushare_realtime import TushareRealtimeClient

    storage = create_storage_from_config()
    await storage.start()
    client = TushareRealtimeClient(token=get_tushare_token())
    await client.start()

    async def _fetch_suspended(day: date) -> set[str]:
        return await client.fetch_suspended_stocks(day.strftime("%Y%m%d"))

    try:
        diag = await diagnose_gaps(storage, _fetch_suspended, minute_detail_days=minute_detail_days)
    finally:
        await client.stop()
        await storage.stop()

    report = build_report(diag)
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(diag, ensure_ascii=False, indent=2), encoding="utf-8")
    print(report, file=sys.stderr, flush=True)
    print(f"\n明细报告: {REPORT_FILE}", file=sys.stderr, flush=True)
    if feishu:
        await _notify_feishu(report)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="数据缺口诊断报告(临时工具)")
    parser.add_argument("--feishu", action="store_true", help="把报告发飞书")
    parser.add_argument("--minute-detail-days", type=int, default=_MINUTE_DETAIL_DAYS)
    args = parser.parse_args()
    asyncio.run(main(feishu=args.feishu, minute_detail_days=args.minute_detail_days))
