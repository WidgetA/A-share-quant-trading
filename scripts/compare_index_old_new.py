# === MODULE PURPOSE (TEMPORARY / 一次性验证工具) ===
# 新旧索引对照验证。在用「新索引」(三合一 stock_snapshot 减去 kimi
# stock_listing_info 黑名单 = effective_universe) 去驱动历史数据的补全/清理
# 之前,先把它跟「旧索引」(之前用的 Tushare 列表 = bak_basic 每日权威列表)
# 逐日对照,看两边差在哪,确认新索引可信。
#
#   旧索引(day) = client.fetch_bak_basic(day)        — Tushare 权威日列表
#   新索引(day) = storage.get_effective_universe_for_date(day)
#   only_in_old = 旧 - 新   (新索引丢了的 — kimi 误删? bak 含未来码?)
#   only_in_new = 新 - 旧   (新索引多的 — daily∪suspend_d 覆盖、bak 漏的,如北交所)
#
# 结果可打印 / 写 data/audit/index_compare_report.json / 发飞书。
# 同名 compare_index_range / format_feishu_summary 也被临时接口
# POST /api/audit/index-compare 复用。确认完毕后整个文件 + 接口可删。

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Allow running directly from repo root (uv run python scripts/...).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.common.config import get_tushare_token  # noqa: E402
from src.data.clients.greptime_storage import create_storage_from_config  # noqa: E402
from src.data.clients.tushare_realtime import (  # noqa: E402
    TushareRealtimeClient,
    TushareRealtimeError,
    get_tushare_trade_calendar,
)

REPORT_FILE = Path("data/audit/index_compare_report.json")
_SAMPLE = 20  # how many delta codes to surface per direction
_CONCURRENCY = 4  # bak_basic is one API call per day
# Tushare's per-minute rate limit comes back as an API-level error (code!=0),
# which TushareRealtimeClient does NOT retry. A full-history run (~820 days)
# will hit it, so we retry per-day here with backoff instead of dying.
_RATE_LIMIT_RETRIES = 6
_RATE_LIMIT_SLEEP = 15.0  # seconds


def _reason_for_code(code: str, day: date, listing_info: dict) -> str:
    """Best-effort explanation for why a code differs, from listing_info."""
    meta = listing_info.get(code)
    if not meta:
        return "无 listing_info"
    ld = meta.get("list_date")
    dd = meta.get("delist_date")
    if ld is not None and day < ld:
        return f"kimi:未上市(list_date={ld})"
    if dd is not None and day >= dd:
        return f"kimi:已退市(delist_date={dd})"
    if not meta.get("verified"):
        return "未验证占位"
    return "已验证在市"


async def compare_index_range(
    storage,
    client: TushareRealtimeClient,
    dates: list[date],
    *,
    concurrency: int = _CONCURRENCY,
) -> dict:
    """Compare old index (bak_basic) vs new index (effective_universe) per day.

    Returns a dict with per-day deltas + aggregate. Pure data — no Feishu/print
    (the caller decides). Fail-fast on bak_basic API error (audit must be
    reliable; we don't silently skip days).
    """
    listing_info = await storage.get_listing_info_all()
    sem = asyncio.Semaphore(concurrency)

    async def _fetch_bak_with_retry(day: date) -> set[str]:
        """bak_basic with rate-limit retry (client itself doesn't retry code!=0)."""
        yyyymmdd = day.strftime("%Y%m%d")
        for attempt in range(1, _RATE_LIMIT_RETRIES + 1):
            try:
                return set(await client.fetch_bak_basic(yyyymmdd))
            except TushareRealtimeError as e:
                if attempt >= _RATE_LIMIT_RETRIES:
                    raise
                print(
                    f"⚠ bak_basic {yyyymmdd} 限频/失败 (第{attempt}次): {e}; "
                    f"{_RATE_LIMIT_SLEEP:.0f}s 后重试",
                    file=sys.stderr,
                    flush=True,
                )
                await asyncio.sleep(_RATE_LIMIT_SLEEP)
        raise TushareRealtimeError("unreachable")

    async def _one(day: date) -> dict:
        async with sem:
            old = await _fetch_bak_with_retry(day)
        new = await storage.get_effective_universe_for_date(day)
        return {
            "date": day.isoformat(),
            "old_n": len(old),
            "new_n": len(new),
            "only_in_old": sorted(old - new),
            "only_in_new": sorted(new - old),
        }

    per_day = await asyncio.gather(*[_one(d) for d in dates])
    per_day.sort(key=lambda r: r["date"])

    # Aggregate: distinct codes + total occurrences across the range.
    distinct_old: set[str] = set()
    distinct_new: set[str] = set()
    total_only_old = 0
    total_only_new = 0
    # First-seen (date, code) for sampling with a reason.
    sample_old: list[tuple[str, str, str]] = []  # (date, code, reason)
    sample_new: list[tuple[str, str, str]] = []
    for r in per_day:
        d = date.fromisoformat(r["date"])
        for c in r["only_in_old"]:
            total_only_old += 1
            if c not in distinct_old:
                distinct_old.add(c)
                if len(sample_old) < _SAMPLE:
                    sample_old.append((r["date"], c, _reason_for_code(c, d, listing_info)))
        for c in r["only_in_new"]:
            total_only_new += 1
            if c not in distinct_new:
                distinct_new.add(c)
                if len(sample_new) < _SAMPLE:
                    sample_new.append((r["date"], c, _reason_for_code(c, d, listing_info)))

    return {
        "range": {"start": per_day[0]["date"], "end": per_day[-1]["date"]} if per_day else {},
        "days": len(per_day),
        "per_day": per_day,
        "agg": {
            "distinct_only_old": len(distinct_old),
            "distinct_only_new": len(distinct_new),
            "total_only_old": total_only_old,
            "total_only_new": total_only_new,
            "sample_only_old": sample_old,
            "sample_only_new": sample_new,
        },
    }


def format_feishu_summary(result: dict) -> str:
    """Compact Feishu summary of an index-compare run."""
    if not result.get("days"):
        return "[索引对照] 无可对照的交易日"
    rng = result.get("range", {})
    agg = result["agg"]
    lines = [
        "[新旧索引对照] 完成",
        f"范围: {rng.get('start')} ~ {rng.get('end')} ({result['days']} 天)",
        f"旧索引(bak_basic) 独有: distinct {agg['distinct_only_old']} / 累计 {agg['total_only_old']}",
        f"新索引(三合一-kimi) 独有: distinct {agg['distinct_only_new']} / 累计 {agg['total_only_new']}",
    ]

    def _fmt(samples: list) -> str:
        return "\n".join(f"  {d} {c} [{why}]" for d, c, why in samples) or "  (无)"

    lines.append("— 仅旧索引有(新索引丢/挡):")
    lines.append(_fmt(agg["sample_only_old"]))
    lines.append("— 仅新索引有(bak_basic 漏):")
    lines.append(_fmt(agg["sample_only_new"]))
    return "\n".join(lines)


async def _notify_feishu(message: str) -> None:
    """Best-effort Feishu send, never raises (mirrors scheduler helpers)."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_message(message)
    except Exception as e:  # noqa: BLE001 — temp tool, just warn to stderr
        print(f"⚠ 飞书发送失败: {e}", file=sys.stderr, flush=True)


async def main(start: str, end: str, *, feishu: bool, max_days: int) -> None:
    cal_strs = await get_tushare_trade_calendar(start, end)
    dates = sorted(datetime.strptime(s, "%Y-%m-%d").date() for s in cal_strs)
    if len(dates) > max_days:
        print(
            f"⚠ 交易日 {len(dates)} 超过 max-days={max_days},只对照最近 {max_days} 天 "
            f"(其余跳过,非静默)",
            file=sys.stderr,
            flush=True,
        )
        dates = dates[-max_days:]
    if not dates:
        print("无交易日可对照", file=sys.stderr, flush=True)
        return

    print(f"对照 {len(dates)} 个交易日 ({dates[0]} ~ {dates[-1]}) ...", file=sys.stderr, flush=True)

    storage = create_storage_from_config()
    await storage.start()
    client = TushareRealtimeClient(token=get_tushare_token())
    await client.start()
    try:
        result = await compare_index_range(storage, client, dates)
    finally:
        await client.stop()
        await storage.stop()

    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(format_feishu_summary(result), file=sys.stderr, flush=True)
    print(f"\n完整报告: {REPORT_FILE}", file=sys.stderr, flush=True)

    if feishu:
        await _notify_feishu(format_feishu_summary(result))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="新旧索引对照验证 (临时工具)")
    _default_end = (datetime.now().date()).isoformat()
    _default_start = (datetime.now().date() - timedelta(days=45)).isoformat()
    parser.add_argument("--start", default=_default_start, help="YYYY-MM-DD (默认 45 天前)")
    parser.add_argument("--end", default=_default_end, help="YYYY-MM-DD (默认今天)")
    parser.add_argument("--feishu", action="store_true", help="把摘要发飞书")
    parser.add_argument("--max-days", type=int, default=30, help="最多对照多少个交易日")
    args = parser.parse_args()
    asyncio.run(main(args.start, args.end, feishu=args.feishu, max_days=args.max_days))
