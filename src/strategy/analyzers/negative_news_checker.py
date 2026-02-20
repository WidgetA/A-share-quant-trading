# === MODULE PURPOSE ===
# Checks whether a candidate stock has recent negative news.
# Used in Step 6 to potentially skip the #1 ranked candidate
# and fall back to #2.

# === DATA FLOW ===
# stock_code + stock_name → Tavily search → search results
# → Silicon Flow LLM → judgment (has_negative_news: bool, reason: str)

# === FAIL BEHAVIOR ===
# Fail-open: if Tavily or LLM fails, has_negative_news=False + error field set.
# Caller decides whether to proceed based on error.

from __future__ import annotations

import logging
from dataclasses import dataclass

from src.common.siliconflow_client import SiliconFlowClient
from src.common.tavily_client import TavilyClient

logger = logging.getLogger(__name__)


@dataclass
class NewsCheckResult:
    """Result of negative news check for one stock."""

    stock_code: str
    stock_name: str
    has_negative_news: bool
    reason: str  # LLM explanation
    search_result_count: int  # How many Tavily results found
    error: str = ""  # Non-empty if check failed


class NegativeNewsChecker:
    """
    Checks for negative news about a stock using Tavily search + LLM judgment.

    Flow:
        1. Search Tavily for "[stock_code] [stock_name] 负面 利空" in last N days
        2. If results found, feed snippets to Silicon Flow LLM
        3. LLM judges: is there genuine negative news?
        4. Return structured result

    Usage:
        checker = NegativeNewsChecker(tavily_client, siliconflow_client)
        result = await checker.check("600519", "贵州茅台")
        if result.has_negative_news:
            # skip this stock, fall back to #2
    """

    def __init__(
        self,
        tavily_client: TavilyClient,
        siliconflow_client: SiliconFlowClient,
        search_days: int = 3,
        max_search_results: int = 5,
    ):
        self._tavily = tavily_client
        self._llm = siliconflow_client
        self._search_days = search_days
        self._max_results = max_search_results

    async def check(self, stock_code: str, stock_name: str) -> NewsCheckResult:
        """Check for negative news about a stock in recent days."""
        query = f"{stock_code} {stock_name} 负面 利空 风险"
        logger.info(f"NewsCheck: Searching '{query}' (last {self._search_days} days)")

        # Step 1: Tavily search
        try:
            search_resp = await self._tavily.search(
                query=query,
                max_results=self._max_results,
                days=self._search_days,
            )
        except Exception as e:
            logger.error(f"NewsCheck: Tavily search failed for {stock_code}: {e}")
            return NewsCheckResult(
                stock_code=stock_code,
                stock_name=stock_name,
                has_negative_news=False,
                reason="",
                search_result_count=0,
                error=f"Tavily search failed: {e}",
            )

        if not search_resp.results:
            logger.info(f"NewsCheck: No search results for {stock_code}, considered clean")
            return NewsCheckResult(
                stock_code=stock_code,
                stock_name=stock_name,
                has_negative_news=False,
                reason="无近期搜索结果",
                search_result_count=0,
            )

        logger.info(f"NewsCheck: {len(search_resp.results)} results found, sending to LLM")

        # Step 2: Feed results to LLM for judgment
        snippets = "\n\n".join(
            f"【{r.title}】\n{r.content}" for r in search_resp.results[: self._max_results]
        )

        system_prompt = (
            "你是一个A股短线交易风控分析师。\n"
            "判断以下搜索结果中是否包含对该股票的【真实负面消息】。\n\n"
            "注意区分：\n"
            "- 真正的负面：业绩暴雷、重大诉讼、高管违规、产品安全问题、"
            "监管处罚、大股东违规减持、财务造假等\n"
            "- 不算负面：普通股价下跌、大盘调整、行业正常波动、过时旧闻、"
            "一般性行业分析\n\n"
            "请用以下格式回复（严格遵守）：\n"
            "判断: 有负面 或 无负面\n"
            "理由: 一句话说明\n"
        )

        user_prompt = (
            f"股票: {stock_code} {stock_name}\n近{self._search_days}天搜索结果:\n\n{snippets}"
        )

        try:
            llm_response = await self._llm.chat(
                system=system_prompt,
                user=user_prompt,
                max_tokens=200,
            )
        except Exception as e:
            logger.error(f"NewsCheck: LLM call failed for {stock_code}: {e}")
            return NewsCheckResult(
                stock_code=stock_code,
                stock_name=stock_name,
                has_negative_news=False,
                reason="",
                search_result_count=len(search_resp.results),
                error=f"LLM call failed: {e}",
            )

        has_negative = "有负面" in llm_response
        logger.info(
            f"NewsCheck: {stock_code} → {'有负面' if has_negative else '无负面'}: "
            f"{llm_response.strip()[:80]}"
        )

        return NewsCheckResult(
            stock_code=stock_code,
            stock_name=stock_name,
            has_negative_news=has_negative,
            reason=llm_response.strip(),
            search_result_count=len(search_resp.results),
        )
