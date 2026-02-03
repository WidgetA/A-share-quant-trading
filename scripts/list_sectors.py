#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
查询同花顺板块列表

使用iFinD问财接口查询A股市场的各类板块：
- 行业板块（同花顺行业分类）
- 概念板块
- 地域板块

Usage:
    uv run python scripts/list_sectors.py
    uv run python scripts/list_sectors.py --type concept  # 只查概念板块
    uv run python scripts/list_sectors.py --type industry  # 只查行业板块
    uv run python scripts/list_sectors.py --type region  # 只查地域板块
"""

import argparse
import io
import logging
import sys
from pathlib import Path

# Fix Windows console encoding for Chinese characters
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.common.config import get_ifind_credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def login_ifind() -> bool:
    """登录iFinD接口"""
    try:
        from iFinDPy import THS_iFinDLogin

        username, password = get_ifind_credentials()
        result = THS_iFinDLogin(username, password)

        if result == 0:
            logger.info("iFinD 登录成功")
            return True
        else:
            logger.error(f"iFinD 登录失败，错误码: {result}")
            return False
    except ImportError as e:
        logger.error(f"iFinDPy 模块不可用: {e}")
        return False
    except Exception as e:
        logger.error(f"登录错误: {e}")
        return False


def logout_ifind() -> None:
    """登出iFinD接口"""
    try:
        from iFinDPy import THS_iFinDLogout

        THS_iFinDLogout()
        logger.info("iFinD 已登出")
    except Exception as e:
        logger.warning(f"登出错误: {e}")


def fetch_sectors_by_iwencai(
    query: str, sector_type: str, search_type: str = "stock"
) -> list[dict]:
    """
    使用问财查询板块

    Args:
        query: 问财查询语句
        sector_type: 板块类型名称
        search_type: 搜索类型 (stock/zhishu/fund等)

    Returns:
        板块列表
    """
    try:
        from iFinDPy import THS_iwencai

        result = THS_iwencai(query, search_type)

        if isinstance(result, dict):
            if result.get("errorcode", 0) != 0:
                logger.error(
                    f"问财查询{sector_type}失败: {result.get('errmsg')} "
                    f"(code: {result.get('errorcode')})"
                )
                return []

            tables = result.get("tables", [])
            if not tables:
                logger.warning(f"{sector_type}返回空数据")
                return []

            sectors = []
            for table_wrapper in tables:
                if not isinstance(table_wrapper, dict):
                    continue

                table = table_wrapper.get("table", table_wrapper)
                if not isinstance(table, dict):
                    continue

                # 找到代码和名称列
                codes = None
                names = None
                changes = None
                for col_name, col_data in table.items():
                    col_lower = col_name.lower()
                    if "代码" in col_name or "thscode" in col_lower:
                        codes = col_data
                    if "简称" in col_name or "名称" in col_name:
                        names = col_data
                    if "涨跌幅" in col_name:
                        changes = col_data

                if codes:
                    for i in range(len(codes)):
                        sector = {
                            "code": codes[i] if i < len(codes) else "",
                            "name": names[i] if names and i < len(names) else "",
                        }
                        if changes and i < len(changes):
                            sector["change"] = changes[i]
                        sectors.append(sector)

            logger.info(f"获取到 {len(sectors)} 个{sector_type}")
            return sectors
        else:
            logger.warning(f"THS_iwencai 返回类型异常: {type(result)}")
            return []

    except Exception as e:
        logger.error(f"问财查询{sector_type}错误: {e}")
        import traceback

        traceback.print_exc()
        return []


def fetch_industry_sectors() -> list[dict]:
    """获取行业板块（同花顺行业分类）"""
    # 使用问财查询所有同花顺行业板块
    return fetch_sectors_by_iwencai("同花顺行业板块", "行业板块", "zhishu")


def fetch_concept_sectors() -> list[dict]:
    """获取概念板块"""
    # 使用问财查询所有概念板块
    return fetch_sectors_by_iwencai("概念板块", "概念板块", "zhishu")


def fetch_region_sectors() -> list[dict]:
    """获取地域板块"""
    # 使用问财查询所有地域板块
    return fetch_sectors_by_iwencai("地域板块", "地域板块", "zhishu")


def print_sectors(sectors: list[dict], title: str) -> None:
    """打印板块列表"""
    print(f"\n{'=' * 60}")
    print(f"{title} (共 {len(sectors)} 个)")
    print("=" * 60)

    if not sectors:
        print("  (无数据)")
        return

    # 按名称排序
    sorted_sectors = sorted(sectors, key=lambda x: x.get("name", ""))

    for i, sector in enumerate(sorted_sectors, 1):
        code = sector.get("code", "")
        name = sector.get("name", "")
        print(f"  {i:3d}. {code:15s} {name}")


def export_to_json(data: dict, output_path: Path) -> None:
    """导出数据到JSON文件"""
    import json

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n数据已导出到: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="查询同花顺板块列表")
    parser.add_argument(
        "--type",
        "-t",
        choices=["industry", "concept", "region", "all"],
        default="all",
        help="板块类型: industry=行业, concept=概念, region=地域, all=全部",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="导出JSON文件路径 (例如: data/sectors.json)",
    )
    parser.add_argument(
        "--debug",
        "-d",
        action="store_true",
        help="显示调试信息",
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # 登录
    if not login_ifind():
        sys.exit(1)

    try:
        # 查询所有类型的板块
        industry = fetch_industry_sectors()
        concept = fetch_concept_sectors()
        region = fetch_region_sectors()

        # 根据类型过滤显示
        if args.type == "all":
            print_sectors(industry, "行业板块")
            print_sectors(concept, "概念板块")
            print_sectors(region, "地域板块")
            print(f"\n总计: 行业{len(industry)}个, 概念{len(concept)}个, 地域{len(region)}个")
        elif args.type == "industry":
            print_sectors(industry, "行业板块")
        elif args.type == "concept":
            print_sectors(concept, "概念板块")
        elif args.type == "region":
            print_sectors(region, "地域板块")

        # 导出JSON
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            data = {
                "industry": industry,
                "concept": concept,
                "region": region,
                "summary": {
                    "industry_count": len(industry),
                    "concept_count": len(concept),
                    "region_count": len(region),
                    "total": len(industry) + len(concept) + len(region),
                },
            }
            export_to_json(data, output_path)

    finally:
        logout_ifind()


if __name__ == "__main__":
    main()
