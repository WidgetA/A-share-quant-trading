#!/usr/bin/env python
"""
获取A股公告数据的便捷脚本。

这是 src.data.sources.akshare_announcement 模块的包装脚本。
也可以直接使用模块: python -m src.data.sources.akshare_announcement

Usage:
    python scripts/fetch_announcements.py                         # 获取今日公告
    python scripts/fetch_announcements.py -d 20260124             # 获取指定日期
    python scripts/fetch_announcements.py -s 20260124 -e 20260126 # 日期范围
    python scripts/fetch_announcements.py -c major                # 只获取重大事项
    python scripts/fetch_announcements.py -f json                 # JSON格式输出

公告类型 (-c/--category):
    all          全部公告
    major        重大事项
    financial    财务报告
    financing    融资公告
    risk         风险提示
    restructuring 资产重组
    info_change  信息变更
    shareholding 持股变动
"""

import sys
from pathlib import Path

# Add project root to path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.sources.akshare_announcement import main

if __name__ == "__main__":
    main()
