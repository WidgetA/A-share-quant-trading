# -*- coding: utf-8 -*-
"""格式化隔夜分析报告"""

import json
import sys
from pathlib import Path

# 设置输出编码
sys.stdout.reconfigure(encoding="utf-8")

with open("data/overnight_analysis.json", "r", encoding="utf-8") as f:
    results = json.load(f)

positive = [
    r
    for r in results
    if r["analysis"]["sentiment"] == "positive" and r["analysis"]["confidence"] >= 0.7
]
negative = [
    r
    for r in results
    if r["analysis"]["sentiment"] == "negative" and r["analysis"]["confidence"] >= 0.7
]

# 按置信度排序
positive = sorted(positive, key=lambda x: x["analysis"]["confidence"], reverse=True)
negative = sorted(negative, key=lambda x: x["analysis"]["confidence"], reverse=True)

print("=" * 70)
print("昨日收盘到今日开盘 新闻/公告 LLM分析报告")
print("时间范围: 2026-01-27 15:00 到 2026-01-28 09:30")
print("=" * 70)
print(f"总消息: 1777 | 利好: {len(positive)} | 利空: {len(negative)}")
print("=" * 70)
print()

print("【利好消息 TOP 50】")
print("=" * 70)
for i, r in enumerate(positive[:50], 1):
    m = r["message"]
    a = r["analysis"]
    stocks = ", ".join(a["stock_codes"][:3]) if a["stock_codes"] else "-"
    sectors = ", ".join(a["sectors"][:2]) if a["sectors"] else "-"
    title = m["title"][:60] + "..." if len(m["title"]) > 60 else m["title"]
    print(f"{i:2}. [{a['signal_type'].upper():10}] {a['confidence'] * 100:3.0f}% | {title}")
    print(f"    股票: {stocks} | 板块: {sectors}")
    print(f"    分析: {a['reason']}")
    print()

print()
print("【利空消息 TOP 30】")
print("=" * 70)
for i, r in enumerate(negative[:30], 1):
    m = r["message"]
    a = r["analysis"]
    stocks = ", ".join(a["stock_codes"][:3]) if a["stock_codes"] else "-"
    title = m["title"][:60] + "..." if len(m["title"]) > 60 else m["title"]
    print(f"{i:2}. [{a['signal_type'].upper():10}] {a['confidence'] * 100:3.0f}% | {title}")
    print(f"    股票: {stocks}")
    print(f"    分析: {a['reason']}")
    print()
