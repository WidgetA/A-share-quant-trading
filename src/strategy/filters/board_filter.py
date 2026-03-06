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
    "同花顺漂亮100",
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
    "高股息",
    "高股息精选",
    "高息股",
    "低波红利",
    "红利低波",
    "中特估",
    "同花顺中特估100",
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
    "股权转让(并购重组)",
    "并购重组",
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
    "京津冀一体化",
    "福建自贸区",
    "海峡西岸",
    "海峡两岸",
    "雄安新区",
    "新疆振兴",
    "智慧城市",
    "共同富裕示范区",
    "乡村振兴",
    # ---- 企业概念（跟随特定公司，不是行业主题）----
    "苹果概念",
    "特斯拉概念",
    "阿里巴巴概念",
    "腾讯概念",
    "小米概念",
    "小米汽车",
    "富士康概念",
    "宁德时代概念",
    "百度概念",
    "华为概念",
    "华为海思概念股",
    "华为汽车",
    "华为鲲鹏",
    "华为欧拉",
    "华为昇腾",
    "华为手机",
    "华为数字能源",
    "华为盘古",
    "鸿蒙概念",
    "抖音概念(字节概念)",
    "蚂蚁集团概念",
    "中芯国际概念",
    "拼多多概念",
    "快手概念",
    "比亚迪概念",
    "英伟达概念",
    "长安汽车概念",
    "智谱AI",
    "小红书概念",
    "DeepSeek概念",
    "ChatGPT概念",
    "Sora概念(文生视频)",
    "成飞概念",
    "中船系",
    "兵装重组概念",
    # ---- 政策/金融概念（太宽泛）----
    "跨境支付(CIPS)",
    # ---- 太宽泛/无实际主题 ----
    "创投",
    "中字头",
    "中字头股票",
    "超级品牌",
    "独角兽概念",
    "稀缺资源",
    "反内卷概念",
    "VPN",
    "北交所概念",
    "国企改革",
    "央国企改革",
    "央企国企改革",
    "上海国企改革",
    "深圳国企改革",
    "人民币贬值受益",
    "一带一路",
    "回购增持再贷款概念",
    "ST板块",
    "新股与次新股",
    "科创次新股",
    "摘帽",
}

# Boards that ARE real technology/industry themes but have too many constituents
# (≥400 stocks) to produce meaningful "hot board" signals — the ≥2 gainer
# threshold is trivially met every day for these boards.
# Kept as a separate constant so we can study alternative handling later
# (e.g. higher gainer ratio threshold instead of outright exclusion).
BROAD_CONCEPT_BOARDS: set[str] = {
    # ---- 真概念但成分股过多（≥400），信号被稀释 ----
    "机器人概念",  # 1166
    "人工智能",  # 1053
    "新能源汽车",  # 1012
    "芯片概念",  # 849
    "储能",  # 807
    "军工",  # 595
    "光伏概念",  # 578
    "数据中心",  # 576
    "锂电池概念",  # 571
    "低空经济",  # 474
    "消费电子概念",  # 472
    "商业航天",  # 468
    "AI应用",  # 463
    "物联网",  # 458
    "无人机",  # 448
    "工业互联网",  # 423
    "人形机器人",  # 418
    "AI智能体",  # 416
    "风电",  # 413
    "医疗器械概念",  # 400
    # ---- 政策标签/地区概念，成分股过多 ----
    "专精特新",  # 1160
    "数字经济",  # 503
    "粤港澳大湾区",  # 472
}

# Regex patterns that catch time-variant junk boards (e.g. "2026中报预增")
_SEASONAL_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"^\d{4}(中报|三季报|年报|一季报)(预增|预减|扭亏|预亏|续盈)$"),
]


def is_junk_board(board_name: str) -> bool:
    """
    Check whether a board name should be filtered out.

    Covers two categories:
    - JUNK_BOARDS: no actionable theme (index, fund holdings, etc.)
    - Seasonal patterns: time-variant report categories

    Note: BROAD_CONCEPT_BOARDS is defined but NOT filtered here.
    These are real themes with many constituents — kept for future
    ratio-based filtering in Step 3.

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
