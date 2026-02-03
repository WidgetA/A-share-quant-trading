"""
分析隔夜消息（昨天收盘到今天开盘）的脚本。
"""

import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import aiosqlite

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.llm_service import create_llm_service_from_config


async def main():
    beijing_tz = ZoneInfo("Asia/Shanghai")
    now = datetime.now(beijing_tz)
    today = now.date()
    yesterday = today - timedelta(days=1)

    start_time = datetime(yesterday.year, yesterday.month, yesterday.day, 15, 0, tzinfo=beijing_tz)
    end_time = datetime(today.year, today.month, today.day, 9, 30, tzinfo=beijing_tz)

    print(f"查询时间范围: {start_time} 到 {end_time}")

    messages = []
    async with aiosqlite.connect("data/messages.db") as db:
        db.row_factory = aiosqlite.Row

        query = """
            SELECT id, source_type, source_name, title, content, publish_time, stock_codes
            FROM messages
            WHERE publish_time >= ? AND publish_time <= ?
            ORDER BY publish_time DESC
        """
        async with db.execute(query, (start_time.isoformat(), end_time.isoformat())) as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                messages.append(dict(row))

    print(f"开始分析全部 {len(messages)} 条消息...")
    sys.stdout.flush()

    llm = create_llm_service_from_config()
    await llm.start()

    results = []
    try:
        for i, m in enumerate(messages):
            if (i + 1) % 50 == 0:
                print(f"已分析 {i + 1}/{len(messages)}...")
                sys.stdout.flush()
                await asyncio.sleep(2)

            result = await llm.analyze_news(m["title"], m["content"] or m["title"])
            results.append(
                {
                    "message": {
                        "title": m["title"],
                        "publish_time": m["publish_time"],
                        "source_name": m["source_name"],
                        "source_type": m["source_type"],
                        "stock_codes": m["stock_codes"],
                    },
                    "analysis": result.to_dict(),
                }
            )
            await asyncio.sleep(0.15)

    finally:
        await llm.stop()

    # 保存结果到文件
    output_file = Path("data/overnight_analysis.json")
    output_file.parent.mkdir(exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n分析完成，结果已保存到 {output_file}")

    # 统计并输出
    positive = [r for r in results if r["analysis"]["sentiment"] == "positive"]
    negative = [r for r in results if r["analysis"]["sentiment"] == "negative"]
    neutral = [r for r in results if r["analysis"]["sentiment"] == "neutral"]

    print(f"利好: {len(positive)} 条 | 利空: {len(negative)} 条 | 中性: {len(neutral)} 条")

    # 输出利好
    print("\n" + "=" * 70)
    print("【利好消息】按置信度排序")
    print("=" * 70)
    for r in sorted(positive, key=lambda x: x["analysis"]["confidence"], reverse=True):
        m = r["message"]
        a = r["analysis"]
        print(f"\n[{a['signal_type'].upper()}] 置信度: {a['confidence']:.0%}")
        print(f"标题: {m['title']}")
        print(f"时间: {m['publish_time']}")
        print(f"来源: {m['source_name']} ({m['source_type']})")
        if a["stock_codes"]:
            print(f"股票: {a['stock_codes']}")
        if a["sectors"]:
            print(f"板块: {a['sectors']}")
        print(f"分析: {a['reason']}")
        print("-" * 50)

    # 输出利空
    print("\n" + "=" * 70)
    print("【利空消息】按置信度排序")
    print("=" * 70)
    for r in sorted(negative, key=lambda x: x["analysis"]["confidence"], reverse=True):
        m = r["message"]
        a = r["analysis"]
        print(f"\n[{a['signal_type'].upper()}] 置信度: {a['confidence']:.0%}")
        print(f"标题: {m['title']}")
        print(f"时间: {m['publish_time']}")
        print(f"来源: {m['source_name']} ({m['source_type']})")
        if a["stock_codes"]:
            print(f"股票: {a['stock_codes']}")
        print(f"分析: {a['reason']}")
        print("-" * 50)


if __name__ == "__main__":
    asyncio.run(main())
