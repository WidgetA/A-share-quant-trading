# === MODULE PURPOSE ===
# Filter out "junk" concept boards that have no real industry/theme significance.
# These boards are index components, fund holdings, technical indicators,
# financial classifications, etc. — not actionable trading themes.

# === KEY CONCEPTS ===
# - JUNK_BOARDS: Set of board names to exclude from strategy signal generation
# - Used by news analyzer and sector mapper to skip irrelevant boards
# - Covers: index membership, fund holdings, technical/quant labels,
#   financial classifications, seasonal report categories, regional concepts

# === MAINTENANCE ===
# - Seasonal entries (e.g. "2025中报预增") need periodic updates
# - Use partial matching (via is_junk_board) to catch year-variant patterns

import re

# Boards that look like concept/theme boards but carry no actionable trading signal.
# Matching is exact against this set; for fuzzy/pattern matching use is_junk_board().
JUNK_BOARDS: set[str] = {
    # ---- 指数成分 ----
    "AB股",
    "AH股",
    "B股",
    "HS300_",
    "上证50_",
    "上证180_",
    "上证380",
    "深成500",
    "深证100R",
    "中证500",
    "创业成份",
    "创业板综",
    "央视50_",
    "MSCI中国",
    "富时罗素",
    "标准普尔",
    "GDR",
    # ---- 资金持仓 ----
    "社保重仓",
    "QFII重仓",
    "基金重仓",
    "机构重仓",
    "证金持股",
    "养老金",
    "茅指数",
    "宁组合",
    # ---- 通道类 ----
    "沪股通",
    "深股通",
    "融资融券",
    # ---- 昨日行情/技术面 ----
    "昨日涨停",
    "昨日连板",
    "昨日触板",
    "昨日涨停_含一字",
    "昨日连板_含一字",
    "昨日首板",
    "昨日炸板",
    "昨日高换手",
    "昨日高振幅",
    "最近多板",
    "东方财富热股",
    # ---- 财务分类 ----
    "ST股",
    "低价股",
    "百元股",
    "微盘股",
    "微利股",
    "破净股",
    "长期破净",
    "红利破净股",
    "周期股",
    "价值股",
    "红利股",
    # ---- 季报分类（会随时间变化，由 _SEASONAL_PATTERNS 兜底）----
    "2025中报预增",
    "2025中报预减",
    "2025中报预增",
    "2025三季报预增",
    "2025三季报预减",
    "2025三季报扭亏",
    # ---- 参股类（不是行业主题）----
    "参股券商",
    "参股期货",
    "参股银行",
    "参股保险",
    "参股新三板",
    # ---- 股权/上市结构 ----
    "举牌",
    "股权激励",
    "股权转让",
    "转债标的",
    "IPO受益",
    "科创板做市商",
    "科创板做市股",
    # ---- 次新股 ----
    "次新股",
    "注册制次新股",
    # ---- 地域概念（无行业属性）----
    "成渝特区",
    "深圳特区",
    "滨海新区",
    "长江三角",
    "东北振兴",
    "西部大开发",
    "京津冀",
    # ---- 太宽泛/无实际主题 ----
    "创投",
    "中字头",
    "稀缺资源",
    "反内卷概念",
    "VPN",
    "北交所概念",
    "国企改革",
}

# Regex patterns that catch time-variant junk boards (e.g. "2026中报预增")
_SEASONAL_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"^\d{4}(中报|三季报|年报|一季报)(预增|预减|扭亏|预亏|续盈)$"),
]


def is_junk_board(board_name: str) -> bool:
    """
    Check whether a board name is a "junk" board with no actionable theme.

    Uses both exact set lookup and regex patterns (for seasonal entries).

    Args:
        board_name: Board/concept name to check.

    Returns:
        True if the board should be filtered out.
    """
    if board_name in JUNK_BOARDS:
        return True

    for pattern in _SEASONAL_PATTERNS:
        if pattern.match(board_name):
            return True

    return False


def filter_boards(board_names: list[str]) -> list[str]:
    """
    Filter out junk boards from a list.

    Args:
        board_names: List of board/concept names.

    Returns:
        List with junk boards removed, preserving order.
    """
    return [name for name in board_names if not is_junk_board(name)]
