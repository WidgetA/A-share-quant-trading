#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
用 iFinD (问财) 重建概念板块成分数据 —— 单次调用，全表反转。

背景与方法:
    问财查询 "概念板块" (searchtype=zhishu) 会返回全市场个股 + 其 "所属概念" 列
    (分号分隔的板块名)。把这一列反转，即得到 板块 -> 成分股 的完整映射。
    实测 (2026-06-17): 69111 条 membership、每股最多 62 个概念、分布平滑无截断,
    且用 "5G概念股" 逐板块直查 (455 只) 与反转结果 (455) 交叉验证一致 —— 数据准确。

    这条路只依赖 iFinD HTTP token (api), 不依赖 THS 网站 cookie,
    因此可定期重跑刷新本地板块数据。

为什么不走 data_pool 按 .TI 板块码拉:
    实测 data_pool 各参数形态均返回 -4001 no data,该端点是专题报表函数,
    我们的手册/token 下未暴露板块成分池。故采用 "所属概念" 全表反转。

安全:
    默认写到并行的新文件 (data/board_constituents.ifind.json / data/sectors.ifind.json),
    绝不覆盖既有 data/board_constituents.json / data/sectors.json。
    确认无误后由人工 mv 替换。

用法:
    export IFIND_REFRESH_TOKEN=...   # 或配置在 data/ifind_token.txt
    uv run python scripts/rebuild_board_data_ifind.py
    uv run python scripts/rebuild_board_data_ifind.py --diff   # 额外打印与旧文件的差异
"""

import argparse
import asyncio
import io
import json
import logging
import re
import sys
from pathlib import Path

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.data.clients.ifind_http_client import IFinDHttpClient, IFinDHttpError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = PROJECT_ROOT / "data"
OLD_CONSTITUENTS = DATA_DIR / "board_constituents.json"
OLD_SECTORS = DATA_DIR / "sectors.json"
# 默认输出: 并行新文件, 不覆盖旧的
NEW_CONSTITUENTS = DATA_DIR / "board_constituents.ifind.json"
NEW_SECTORS = DATA_DIR / "sectors.ifind.json"


def norm_code(code: str) -> str:
    """去掉 .SH/.SZ/.BJ 后缀, 返回 6 位代码 (与旧文件格式一致)。"""
    return re.split(r"[.]", code)[0].strip()


async def fetch_concept_membership() -> tuple[dict[str, list[list[str]]], int]:
    """
    拉取并反转 "所属概念" 列。

    Returns:
        (board2stocks, total_memberships)
        board2stocks: {板块名: [[code6, name], ...]}  (code 升序)
    """
    client = IFinDHttpClient()
    await client.start()
    try:
        result = await client.smart_stock_picking("概念板块", "zhishu")
    finally:
        await client.stop()

    code_col = name_col = concept_col = None
    for wrapper in result.get("tables", []):
        table = wrapper.get("table", wrapper) if isinstance(wrapper, dict) else {}
        if isinstance(table, dict) and "所属概念" in table:
            code_col = table.get("股票代码")
            name_col = table.get("股票简称")
            concept_col = table["所属概念"]
            break

    if not code_col or concept_col is None:
        # Fail fast: 交易/数据决策类脚本宁可崩, 不可静默产出残缺数据
        raise RuntimeError(
            "问财 '概念板块' 未返回预期的 股票代码/所属概念 列 —— 拒绝产出残缺板块数据"
        )

    board2set: dict[str, set[tuple[str, str]]] = {}
    total = 0
    for i, raw in enumerate(concept_col):
        code = norm_code(code_col[i])
        name = name_col[i] if name_col and i < len(name_col) else ""
        if not raw:
            continue
        for board in raw.split(";"):
            board = board.strip()
            if not board:
                continue
            board2set.setdefault(board, set()).add((code, name))
            total += 1

    board2stocks = {
        b: sorted(([c, n] for c, n in members), key=lambda x: x[0])
        for b, members in board2set.items()
    }
    return board2stocks, total


def rebuild_sectors(board_names: list[str]) -> dict:
    """
    基于新的概念板块名重建 sectors.json 结构。
    - concept: 用新名单; .TI 码若旧文件能对上则保留, 否则留空 (运行时不读 code)。
    - industry / region: 原样保留旧文件 (本方法不覆盖这两类, 运行时也不读)。
    """
    old = {}
    if OLD_SECTORS.exists():
        with open(OLD_SECTORS, encoding="utf-8") as f:
            old = json.load(f)
    old_code_by_name = {c["name"]: c.get("code", "") for c in old.get("concept", [])}

    concept = [
        {"code": old_code_by_name.get(name, ""), "name": name} for name in sorted(board_names)
    ]
    return {
        "industry": old.get("industry", []),
        "concept": concept,
        "region": old.get("region", []),
        "summary": {
            "industry_count": len(old.get("industry", [])),
            "concept_count": len(concept),
            "region_count": len(old.get("region", [])),
            "note": "concept rebuilt from iFinD 所属概念 inversion; industry/region carried over from old file",
        },
    }


def print_diff(new_boards: dict[str, list[list[str]]]) -> None:
    if not OLD_CONSTITUENTS.exists():
        print("(无旧 board_constituents.json, 跳过 diff)")
        return
    with open(OLD_CONSTITUENTS, encoding="utf-8") as f:
        old_raw = json.load(f)
    old = {b: {norm_code(r[0]) for r in rows} for b, rows in old_raw.items()}
    new = {b: {r[0] for r in rows} for b, rows in new_boards.items()}

    added_b = sorted(set(new) - set(old))
    removed_b = sorted(set(old) - set(new))
    common = sorted(set(new) & set(old))
    tot_a = sum(len(new[b] - old[b]) for b in common)
    tot_r = sum(len(old[b] - new[b]) for b in common)
    changed = sum(1 for b in common if new[b] != old[b])

    print("\n--- 与旧文件 diff ---")
    print(f"板块: 旧 {len(old)} -> 新 {len(new)} (共有 {len(common)})")
    print(f"新增板块 {len(added_b)}: {' / '.join(added_b) or '(无)'}")
    print(f"消失板块 {len(removed_b)}: {' / '.join(removed_b) or '(无)'}")
    print(f"成分有变动的板块: {changed}/{len(common)}; 跨板块 新进 {tot_a} / 移出 {tot_r}")


async def async_main(args: argparse.Namespace) -> None:
    logger.info("拉取并反转问财 '概念板块' 所属概念列 ...")
    board2stocks, total = await fetch_concept_membership()
    logger.info("得到 %d 个概念板块, %d 条成分 membership", len(board2stocks), total)

    NEW_CONSTITUENTS.parent.mkdir(parents=True, exist_ok=True)
    with open(NEW_CONSTITUENTS, "w", encoding="utf-8") as f:
        json.dump(board2stocks, f, ensure_ascii=False, indent=1)
    logger.info("写出 %s", NEW_CONSTITUENTS)

    sectors = rebuild_sectors(list(board2stocks.keys()))
    with open(NEW_SECTORS, "w", encoding="utf-8") as f:
        json.dump(sectors, f, ensure_ascii=False, indent=1)
    logger.info("写出 %s", NEW_SECTORS)

    print("\n完成。新文件 (未覆盖旧数据):")
    print(f"  {NEW_CONSTITUENTS}   ({len(board2stocks)} 板块)")
    print(f"  {NEW_SECTORS}        (concept {len(sectors['concept'])} 个)")

    if args.diff:
        print_diff(board2stocks)


def main() -> None:
    parser = argparse.ArgumentParser(description="用 iFinD 重建概念板块成分数据 (并行新文件)")
    parser.add_argument("--diff", action="store_true", help="额外打印与旧文件的差异")
    args = parser.parse_args()
    try:
        asyncio.run(async_main(args))
    except IFinDHttpError as e:
        logger.error("iFinD HTTP 错误: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
