# === MODULE PURPOSE ===
# LLM-based board relevance filter (Step 5.7).
# Judges whether each candidate stock's main business is truly related
# to its assigned concept board.  Filters out "低" (low) relevance stocks
# that are peripheral / irrelevant to the board theme.

# === DEPENDENCIES ===
# - Aliyun DashScope API (OpenAI-compatible, qwen-plus model)
# - httpx: Async HTTP client
# - Persistent file cache: data/board_relevance_cache.json

# === KEY CONCEPTS ===
# - Batch call: one LLM call per board (all stocks in that board together)
# - Cache: board-stock relevance is essentially static, cached permanently
# - Trading safety: LLM API failure → RuntimeError (halt, don't trade blind)

# === TODO (MUST FIX) ===
# 当前仅靠 LLM 自身知识判断主营业务，未提供外部数据。
# 已知风险：
#   1. LLM 训练数据有截止日期，公司转型后判断可能过时
#   2. 冷门小票 LLM 可能不认识（当前默认保留"中"，不会误杀但可能漏过）
# 后续优化：接入公司经营范围数据（如沧海数据 company/info 接口的
#   business_scope 字段），在 prompt 中提供实际主营业务描述，
#   而非依赖 LLM 内部知识。

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

# Default cache location
DEFAULT_CACHE_PATH = "data/board_relevance_cache.json"

SYSTEM_PROMPT = (
    "你是A股市场分析师。判断以下股票的主营业务是否与所属概念板块真正相关。\n"
    "对每只股票回复一行，格式：股票代码 | 高/中/低 | 一句话理由\n"
    "- 高：主营业务直接相关，是板块核心标的\n"
    "- 中：有一定关联但非核心业务\n"
    "- 低：主营业务与板块概念基本无关\n\n"
    "只输出表格，不要额外解释。"
)


@dataclass
class DashScopeConfig:
    """Configuration for Aliyun DashScope text API."""

    api_key: str
    base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    model: str = "qwen-plus"
    max_tokens: int = 500
    temperature: float = 0.1
    timeout: float = 30.0


@dataclass
class RelevanceResult:
    """Relevance judgment for one stock-board pair."""

    stock_code: str
    board_name: str
    level: str  # "高", "中", "低"
    reason: str


class BoardRelevanceFilter:
    """
    LLM-based filter that removes stocks irrelevant to their board theme.

    Uses Aliyun DashScope (qwen-plus) to judge whether each stock's main
    business is truly related to the concept board it was assigned to.
    Results are permanently cached since board-stock relevance is static.

    Usage:
        filter = BoardRelevanceFilter(config)
        kept, relevance_map = await filter.filter_stocks(selected_stocks)
    """

    def __init__(
        self,
        config: DashScopeConfig,
        cache_path: str = DEFAULT_CACHE_PATH,
    ):
        self._config = config
        self._cache_path = Path(cache_path)
        self._cache: dict[str, dict[str, str]] = {}  # key → {level, reason}
        self._load_cache()

    def _cache_key(self, board_name: str, stock_code: str) -> str:
        return f"{board_name}::{stock_code}"

    def _load_cache(self) -> None:
        """Load persistent cache from disk."""
        if self._cache_path.exists():
            try:
                self._cache = json.loads(self._cache_path.read_text(encoding="utf-8"))
                logger.info(f"BoardRelevanceFilter: loaded {len(self._cache)} cached entries")
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"BoardRelevanceFilter: cache load failed: {e}")
                self._cache = {}

    def _save_cache(self) -> None:
        """Persist cache to disk."""
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._cache_path.write_text(
                json.dumps(self._cache, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError as e:
            logger.warning(f"BoardRelevanceFilter: cache save failed: {e}")

    async def filter_stocks(
        self,
        stocks: list,
    ) -> tuple[list, list[RelevanceResult]]:
        """
        Filter stocks by board relevance.

        Args:
            stocks: List of SelectedStock objects with stock_code, stock_name,
                    board_name attributes.

        Returns:
            Tuple of (kept_stocks, all_relevance_results).
            Stocks with level="低" are removed.

        Raises:
            RuntimeError: If LLM API call fails (trading safety: halt).
        """
        if not stocks:
            return [], []

        # Group stocks by board for batch LLM calls
        board_groups: dict[str, list] = defaultdict(list)
        for s in stocks:
            board_groups[s.board_name].append(s)

        all_results: list[RelevanceResult] = []
        cache_updated = False

        # Separate cached vs uncached per board
        uncached_tasks: list[tuple[str, list]] = []
        for board_name, board_stocks in board_groups.items():
            uncached = []
            for s in board_stocks:
                key = self._cache_key(board_name, s.stock_code)
                cached = self._cache.get(key)
                if cached:
                    all_results.append(
                        RelevanceResult(
                            stock_code=s.stock_code,
                            board_name=board_name,
                            level=cached["level"],
                            reason=cached["reason"],
                        )
                    )
                else:
                    uncached.append(s)

            if uncached:
                uncached_tasks.append((board_name, uncached))

        # Call LLM for all uncached boards concurrently (max 5 parallel)
        if uncached_tasks:
            sem = asyncio.Semaphore(5)

            async def _call_with_limit(bn: str, stks: list) -> list[RelevanceResult]:
                async with sem:
                    return await self._call_llm(bn, stks)

            batch_results = await asyncio.gather(
                *[_call_with_limit(bn, stks) for bn, stks in uncached_tasks]
            )
            for llm_results in batch_results:
                for r in llm_results:
                    all_results.append(r)
                    key = self._cache_key(r.board_name, r.stock_code)
                    self._cache[key] = {"level": r.level, "reason": r.reason}
                    cache_updated = True

        if cache_updated:
            self._save_cache()

        # Build lookup: (stock_code, board_name) → level
        # A stock may appear with different boards and have different relevance levels.
        relevance_map = {(r.stock_code, r.board_name): r.level for r in all_results}

        # Filter: keep 高 and 中, remove 低
        kept = []
        removed = []
        for s in stocks:
            level = relevance_map.get((s.stock_code, s.board_name), "中")  # default keep
            if level == "低":
                removed.append(s)
            else:
                kept.append(s)

        if removed:
            removed_info = ", ".join(
                f"{s.stock_code}({s.stock_name})[{s.board_name}]" for s in removed
            )
            logger.info(f"Step 5.7: Filtered {len(removed)} low-relevance stocks: {removed_info}")

        logger.info(f"Step 5.7: {len(kept)}/{len(stocks)} stocks kept after board relevance filter")
        return kept, all_results

    async def _call_llm(
        self,
        board_name: str,
        stocks: list,
    ) -> list[RelevanceResult]:
        """
        Call DashScope LLM to judge relevance for one board's stocks.

        Raises:
            RuntimeError: On API failure (trading safety).
        """
        stock_lines = "\n".join(f"{s.stock_code} {s.stock_name}" for s in stocks)
        user_prompt = f"板块：{board_name}\n股票：\n{stock_lines}"

        logger.info(f"Step 5.7: Calling LLM for board '{board_name}' ({len(stocks)} stocks)")

        # ~30 tokens per stock line (code | level | reason)
        dynamic_max_tokens = max(self._config.max_tokens, len(stocks) * 35)

        try:
            async with httpx.AsyncClient(timeout=self._config.timeout) as client:
                resp = await client.post(
                    f"{self._config.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self._config.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": self._config.model,
                        "messages": [
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": user_prompt},
                        ],
                        "max_tokens": dynamic_max_tokens,
                        "temperature": self._config.temperature,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"]["content"]
        except Exception as e:
            raise RuntimeError(
                f"Step 5.7: 板块相关度 LLM 调用失败 (board={board_name}): {e}。"
                f"无法判断股票与板块的相关性 — 停止推荐。"
            ) from e

        return self._parse_response(content, board_name, stocks)

    def _parse_response(
        self,
        content: str,
        board_name: str,
        stocks: list,
    ) -> list[RelevanceResult]:
        """Parse LLM response into RelevanceResult list."""
        # Build code→stock lookup for matching
        stock_codes = {s.stock_code for s in stocks}
        results: list[RelevanceResult] = []
        parsed_codes: set[str] = set()

        for line in content.strip().split("\n"):
            line = line.strip()
            if not line or "|" not in line:
                continue

            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 3:
                continue

            # Extract stock code (first 6 digits found in first part)
            code_part = parts[0]
            code = ""
            for word in code_part.split():
                if len(word) == 6 and word.isdigit():
                    code = word
                    break

            if not code or code not in stock_codes:
                continue

            level = parts[1].strip()
            reason = parts[2].strip() if len(parts) >= 3 else ""

            # Validate level
            if level not in ("高", "中", "低"):
                level = "中"  # Default: keep (don't over-filter)

            results.append(
                RelevanceResult(
                    stock_code=code,
                    board_name=board_name,
                    level=level,
                    reason=reason,
                )
            )
            parsed_codes.add(code)

        # Any stock not in LLM response defaults to "中" (keep)
        for s in stocks:
            if s.stock_code not in parsed_codes:
                logger.warning(
                    f"Step 5.7: LLM did not return result for {s.stock_code} "
                    f"({s.stock_name}) in board '{board_name}', defaulting to '中'"
                )
                results.append(
                    RelevanceResult(
                        stock_code=s.stock_code,
                        board_name=board_name,
                        level="中",
                        reason="LLM未返回结果，默认保留",
                    )
                )

        return results


def create_board_relevance_filter(
    cache_path: str = DEFAULT_CACHE_PATH,
) -> BoardRelevanceFilter:
    """Create BoardRelevanceFilter using Aliyun API key.

    Raises:
        ValueError: If Aliyun API key is not configured.
    """
    from src.common.config import get_aliyun_api_key

    api_key = get_aliyun_api_key()  # raises ValueError if missing
    config = DashScopeConfig(api_key=api_key)
    return BoardRelevanceFilter(config, cache_path=cache_path)
