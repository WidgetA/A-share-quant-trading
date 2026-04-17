#!/usr/bin/env python
"""
缓存缺口诊断脚本：检测 → 补全 → 记录 → 导出 CSV。

流程：
  1. 调用 CachePipeline.download_prices() 执行正常的补全流程
     （日线用 tsanghi，分钟线用 Tushare）
  2. 补全完成后，逐日扫描 stock_list / backtest_daily / backtest_minute
     把仍然缺失的股票+原因写入 CSV

CSV 列：date, stock_code, gap_type, reason, detail

gap_type / reason 分类：
  daily  | not_in_daily        — 在 stock_list 但不在 backtest_daily
  minute | suspended           — 停牌 (is_suspended=true)
  minute | zero_volume         — 非停牌但 vol=0
  minute | no_bars             — 非停牌有成交但无分钟数据
  minute | partial_bars        — 分钟线不完整 (actual/241)
  minute | api_error           — Tushare API 报错（来自 pipeline no_data_reasons）
  minute | api_empty           — API 返回空（来自 pipeline no_data_reasons）
  minute | unknown_exchange    — 未知交易所
  minute | all_dates_suspended — 所有交易日均停牌

用法：
    uv run python scripts/diagnose_cache_gaps.py
    uv run python scripts/diagnose_cache_gaps.py --start 2024-01-01 --end 2024-06-30
    uv run python scripts/diagnose_cache_gaps.py --skip-fill  # 不调 API，只扫现有数据
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# Ensure project root is on sys.path
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.data.clients.greptime_storage import (
    GreptimeBacktestStorage,
    create_storage_from_config,
    date_to_epoch_ms,
)
from src.data.services.cache_pipeline import CachePipeline
from src.data.services.cache_progress_reporter import CacheProgressReporter
from src.data.sources.tsanghi_daily_source import TsanghiDailySource
from src.data.sources.tushare_metadata_source import TushareMetadataSource
from src.data.sources.tushare_minute_source import TushareMinuteSource

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
EXPECTED_BARS = 241
DEFAULT_START = date(2024, 1, 1)

logger = logging.getLogger(__name__)


def _progress_cb(phase: str, current: int, total: int, detail: str) -> None:
    """Print pipeline progress to console."""
    if phase == "status":
        print(f"  [状态] {detail}")
    elif total > 0:
        print(f"  [{phase}] {current}/{total}  {detail}")
    elif detail:
        print(f"  [{phase}] {detail}")


async def _run_fill(
    storage: GreptimeBacktestStorage,
    start_date: date,
    end_date: date,
) -> dict[str, str]:
    """Run the standard cache pipeline to attempt filling gaps.

    Returns the no_data_reasons dict collected during the minute download phase.
    """
    reporter = CacheProgressReporter(progress_cb=_progress_cb, feishu=None)
    pipeline = CachePipeline(
        storage=storage,
        daily_source=TsanghiDailySource(),
        minute_source=TushareMinuteSource(),
        metadata_source=TushareMetadataSource(),
        reporter=reporter,
    )

    # We need no_data_reasons which download_prices doesn't expose in its return.
    # Run the two phases manually so we can capture it.
    async with (
        pipeline.daily_source,
        pipeline.minute_source,
        pipeline.metadata_source,
    ):
        await pipeline._download_daily_unified(start_date, end_date, None)
        no_data_reasons = await pipeline._download_minute_unified(
            start_date, end_date, None
        )

    return no_data_reasons


async def _audit_and_export(
    storage: GreptimeBacktestStorage,
    start_date: date,
    end_date: date,
    no_data_reasons: dict[str, str],
    output_path: Path,
) -> int:
    """Scan every trading day in range and export remaining gaps to CSV.

    Returns the number of gap rows written.
    """
    # Collect all dates that have stock_list entries
    rows = await storage.db.fetch("SELECT DISTINCT ts FROM stock_list ORDER BY ts")
    from src.data.clients.greptime_storage import ts_to_date

    all_dates = sorted(
        d for r in rows if start_date <= (d := ts_to_date(r["ts"])) <= end_date
    )
    if not all_dates:
        print("stock_list 中无数据，无法审计")
        return 0

    print(f"\n开始逐日审计: {len(all_dates)} 个交易日 ({all_dates[0]} ~ {all_dates[-1]})")

    gap_rows: list[dict[str, str]] = []

    for i, day in enumerate(all_dates):
        if (i + 1) % 50 == 0 or (i + 1) == len(all_dates):
            print(f"  审计进度: {i + 1}/{len(all_dates)} ({day})")

        ts_ms = date_to_epoch_ms(day)
        day_str = day.strftime("%Y-%m-%d")

        # --- Daily gaps ---
        expected_codes = await storage.get_stock_list_codes_for_date(day)
        existing_daily = await storage.get_codes_for_daily_date(day)
        missing_daily = expected_codes - existing_daily

        for code in sorted(missing_daily):
            gap_rows.append({
                "date": day_str,
                "stock_code": code,
                "gap_type": "daily",
                "reason": "not_in_daily",
                "detail": "在stock_list但不在backtest_daily",
            })

        # --- Minute gaps ---
        # Get daily detail for this date (is_suspended, vol)
        daily_detail = await storage.get_daily_rows_for_date(day)
        detail_map = {r["stock_code"]: r for r in daily_detail}

        # Get minute bar counts per stock
        day_end_ms = ts_ms + 86_400_000
        minute_rows = await storage.db.fetch(
            f"SELECT stock_code, COUNT(*) as cnt FROM backtest_minute "
            f"WHERE ts >= {ts_ms} AND ts < {day_end_ms} GROUP BY stock_code"
        )
        minute_counts = {r["stock_code"]: int(r["cnt"]) for r in minute_rows}

        for code in sorted(existing_daily):
            info = detail_map.get(code)
            if info is None:
                continue

            is_suspended = info.get("is_suspended")
            vol = info.get("vol", 0) or 0
            bar_count = minute_counts.get(code, 0)

            # Suspended → expected no minute data
            if is_suspended is True:
                if bar_count < EXPECTED_BARS:
                    gap_rows.append({
                        "date": day_str,
                        "stock_code": code,
                        "gap_type": "minute",
                        "reason": "suspended",
                        "detail": f"停牌(is_suspended=true), bars={bar_count}",
                    })
                continue

            # Zero volume → probably no minute data
            if vol == 0:
                if bar_count < EXPECTED_BARS:
                    gap_rows.append({
                        "date": day_str,
                        "stock_code": code,
                        "gap_type": "minute",
                        "reason": "zero_volume",
                        "detail": f"成交量为0(vol=0), bars={bar_count}",
                    })
                continue

            # Active stock: should have 241 bars
            if bar_count >= EXPECTED_BARS:
                continue

            # Still missing — check pipeline reason
            pipeline_reason = no_data_reasons.get(code, "")
            if bar_count == 0:
                reason_key = pipeline_reason.split(":")[0] if pipeline_reason else "no_bars"
                detail_str = pipeline_reason if pipeline_reason else "非停牌有成交但无分钟数据"
                gap_rows.append({
                    "date": day_str,
                    "stock_code": code,
                    "gap_type": "minute",
                    "reason": reason_key,
                    "detail": detail_str,
                })
            else:
                gap_rows.append({
                    "date": day_str,
                    "stock_code": code,
                    "gap_type": "minute",
                    "reason": "partial_bars",
                    "detail": f"分钟线不完整(actual={bar_count}/expected={EXPECTED_BARS})"
                    + (f", pipeline={pipeline_reason}" if pipeline_reason else ""),
                })

    # Write CSV
    if not gap_rows:
        print("\n所有数据完整，无缺口。")
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "stock_code", "gap_type", "reason", "detail"])
        writer.writeheader()
        writer.writerows(gap_rows)

    # Summary
    from collections import Counter

    by_type = Counter((r["gap_type"], r["reason"]) for r in gap_rows)
    print(f"\n共 {len(gap_rows)} 条缺口记录，已导出到 {output_path}")
    print("分类统计：")
    for (gtype, reason), count in sorted(by_type.items()):
        print(f"  {gtype}/{reason}: {count}")

    return len(gap_rows)


async def main() -> None:
    parser = argparse.ArgumentParser(description="缓存缺口诊断：检测→补全→记录→导出CSV")
    parser.add_argument("--start", type=str, default=None, help="起始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--skip-fill", action="store_true", help="跳过补全，只扫描现有数据")
    parser.add_argument("-o", "--output", type=str, default=None, help="CSV 输出路径")
    args = parser.parse_args()

    now = datetime.now(BEIJING_TZ)
    start_date = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else DEFAULT_START
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else (now.date() - timedelta(days=1))
    output_path = Path(args.output) if args.output else (_project_root / "data" / "cache_gap_diagnosis.csv")

    print(f"日期范围: {start_date} ~ {end_date}")
    print(f"输出路径: {output_path}")

    storage = create_storage_from_config()
    await storage.start()

    try:
        no_data_reasons: dict[str, str] = {}

        if not args.skip_fill:
            print("\n===== 阶段 1: 补全 =====")
            no_data_reasons = await _run_fill(storage, start_date, end_date)
            print(f"\npipeline 记录了 {len(no_data_reasons)} 只股票无法补全的原因")
        else:
            print("\n跳过补全，直接扫描现有数据")

        print("\n===== 阶段 2: 审计导出 =====")
        count = await _audit_and_export(storage, start_date, end_date, no_data_reasons, output_path)

        if count > 0:
            print(f"\n完成。{count} 条缺口记录已导出到 {output_path}")
        else:
            print("\n完成。数据完整，无缺口。")
    finally:
        await storage.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    asyncio.run(main())
