# === MODULE PURPOSE ===
# Maps stocks to concept boards and vice versa using iFinD iwencai API.
# Used by momentum sector strategy to reverse-lookup concept boards for stocks
# and to get all constituent stocks of a concept board.

# === DEPENDENCIES ===
# - IFinDHttpClient: iwencai natural language query API
# - board_filter: Filters out junk/irrelevant concept boards

# === KEY CONCEPTS ===
# - Reverse lookup: Given a stock, find its concept boards
# - Board constituents: Given a board name, find all member stocks
# - Daily cache: Concept board membership rarely changes intraday

import asyncio
import logging
from datetime import date
from typing import Any

from src.data.clients.ifind_http_client import IFinDHttpClient, IFinDHttpError
from src.strategy.filters.board_filter import filter_boards

logger = logging.getLogger(__name__)

# Max concurrent iwencai requests to stay well within 600/min rate limit
_CONCURRENCY_LIMIT = 10


class ConceptMapper:
    """
    Maps stocks to/from concept boards via iFinD iwencai queries.

    Methods:
        get_stock_concepts(stock_code) -> list[str]
            Reverse lookup: which concept boards does this stock belong to?

        get_board_stocks(board_name) -> list[tuple[str, str]]
            Forward lookup: which stocks are in this concept board?

        batch_get_stock_concepts(stock_codes) -> dict[str, list[str]]
            Batch reverse lookup with concurrency control.

    Caching:
        Results are cached for the current day. Call clear_cache() to reset.

    Usage:
        mapper = ConceptMapper(ifind_client)
        concepts = await mapper.get_stock_concepts("600519")
        # -> ["白酒", "消费龙头", ...]

        stocks = await mapper.get_board_stocks("白酒")
        # -> [("600519", "贵州茅台"), ("000858", "五粮液"), ...]
    """

    def __init__(self, ifind_client: IFinDHttpClient):
        self._client = ifind_client

        # Cache: stock_code -> list of concept board names (filtered)
        self._stock_concepts_cache: dict[str, list[str]] = {}
        # Cache: board_name -> list of (stock_code, stock_name)
        self._board_stocks_cache: dict[str, list[tuple[str, str]]] = {}
        # Cache date — invalidate if day changes
        self._cache_date: date | None = None

    def _check_cache_date(self) -> None:
        """Invalidate cache if day has changed."""
        today = date.today()
        if self._cache_date != today:
            self._stock_concepts_cache.clear()
            self._board_stocks_cache.clear()
            self._cache_date = today

    def clear_cache(self) -> None:
        """Manually clear all caches."""
        self._stock_concepts_cache.clear()
        self._board_stocks_cache.clear()
        self._cache_date = None

    async def get_stock_concepts(self, stock_code: str) -> list[str]:
        """
        Get concept boards that a stock belongs to.

        Uses iwencai query: "XXXXXX所属同花顺概念"
        Results are filtered through board_filter to remove junk boards.

        Args:
            stock_code: 6-digit stock code (e.g., "600519") or with suffix.

        Returns:
            List of concept board names after junk board filtering.
        """
        self._check_cache_date()

        # Normalize to bare code for cache key
        bare_code = self._normalize_code(stock_code)
        if not bare_code:
            return []

        if bare_code in self._stock_concepts_cache:
            return self._stock_concepts_cache[bare_code]

        try:
            result = await self._client.smart_stock_picking(f"{bare_code}所属同花顺概念", "stock")

            concepts = self._parse_concept_names(result)
            # Filter out junk boards
            filtered = filter_boards(concepts)

            self._stock_concepts_cache[bare_code] = filtered
            logger.debug(f"{bare_code}: {len(concepts)} concepts -> {len(filtered)} after filter")
            return filtered

        except IFinDHttpError:
            logger.error(f"Failed to get concepts for {bare_code}")
            raise
        except Exception:
            logger.error(f"Unexpected error getting concepts for {bare_code}")
            raise

    async def batch_get_stock_concepts(self, stock_codes: list[str]) -> dict[str, list[str]]:
        """
        Batch reverse lookup with concurrency control.

        Args:
            stock_codes: List of stock codes.

        Returns:
            Dict mapping stock_code to list of concept board names.
        """
        results: dict[str, list[str]] = {}
        semaphore = asyncio.Semaphore(_CONCURRENCY_LIMIT)

        async def _lookup(code: str) -> None:
            async with semaphore:
                concepts = await self.get_stock_concepts(code)
                results[self._normalize_code(code)] = concepts

        tasks = [_lookup(code) for code in stock_codes]
        await asyncio.gather(*tasks)

        return results

    async def get_board_stocks(self, board_name: str) -> list[tuple[str, str]]:
        """
        Get all constituent stocks of a concept board.

        Uses iwencai query: "XX概念板块成分股"

        Args:
            board_name: Concept board name (e.g., "白酒", "人形机器人").

        Returns:
            List of (stock_code, stock_name) tuples.
            stock_code is bare 6-digit format (no suffix).
        """
        self._check_cache_date()

        if board_name in self._board_stocks_cache:
            return self._board_stocks_cache[board_name]

        try:
            result = await self._client.smart_stock_picking(f"{board_name}概念板块成分股", "stock")

            stocks = self._parse_stock_list(result)
            self._board_stocks_cache[board_name] = stocks
            logger.debug(f"Board '{board_name}': {len(stocks)} constituent stocks")
            return stocks

        except IFinDHttpError:
            logger.error(f"Failed to get stocks for board '{board_name}'")
            raise
        except Exception:
            logger.error(f"Unexpected error getting stocks for board '{board_name}'")
            raise

    async def batch_get_board_stocks(
        self, board_names: list[str]
    ) -> dict[str, list[tuple[str, str]]]:
        """
        Batch get constituent stocks for multiple boards.

        Args:
            board_names: List of concept board names.

        Returns:
            Dict mapping board_name to list of (stock_code, stock_name).
        """
        results: dict[str, list[tuple[str, str]]] = {}
        semaphore = asyncio.Semaphore(_CONCURRENCY_LIMIT)

        async def _lookup(name: str) -> None:
            async with semaphore:
                stocks = await self.get_board_stocks(name)
                results[name] = stocks

        tasks = [_lookup(name) for name in board_names]
        await asyncio.gather(*tasks)

        return results

    def _parse_concept_names(self, result: dict[str, Any]) -> list[str]:
        """
        Parse concept board names from iwencai response.

        iwencai returns a table where one column contains concept names.
        The column name varies — look for columns containing "概念" or "所属".
        """
        tables = result.get("tables", [])
        if not tables:
            return []

        concepts: list[str] = []

        for table_wrapper in tables:
            if not isinstance(table_wrapper, dict):
                continue

            table = table_wrapper.get("table", table_wrapper)
            if not isinstance(table, dict):
                continue

            # Try to find a column with concept board names
            for col_name, col_data in table.items():
                if not isinstance(col_data, list):
                    continue
                # iwencai may return concept names as a semicolon-separated string
                # in a single cell, or as separate rows
                for item in col_data:
                    if isinstance(item, str):
                        # Split by common separators
                        for name in item.replace("；", ";").split(";"):
                            name = name.strip()
                            if name:
                                concepts.append(name)

        return concepts

    def _parse_stock_list(self, result: dict[str, Any]) -> list[tuple[str, str]]:
        """
        Parse stock code and name from iwencai response.

        Looks for columns containing "代码" and "简称"/"名称".
        """
        tables = result.get("tables", [])
        if not tables:
            return []

        stocks: list[tuple[str, str]] = []

        for table_wrapper in tables:
            if not isinstance(table_wrapper, dict):
                continue

            table = table_wrapper.get("table", table_wrapper)
            if not isinstance(table, dict):
                continue

            codes = None
            names = None
            for col_name, col_data in table.items():
                col_lower = col_name.lower()
                if "代码" in col_name or "thscode" in col_lower:
                    codes = col_data
                if "简称" in col_name or "名称" in col_name:
                    names = col_data

            if codes:
                for i in range(len(codes)):
                    raw_code = str(codes[i]).strip()
                    bare = self._normalize_code(raw_code)
                    if not bare:
                        continue
                    name = str(names[i]).strip() if names and i < len(names) else ""
                    stocks.append((bare, name))

        return stocks

    @staticmethod
    def _normalize_code(stock_code: str) -> str:
        """Normalize stock code to bare 6-digit format."""
        if not stock_code:
            return ""
        code = str(stock_code).strip()
        for suffix in (".SZ", ".SH", ".BJ", ".sz", ".sh", ".bj"):
            if code.endswith(suffix):
                code = code[: -len(suffix)]
                break
        if len(code) == 6 and code.isdigit():
            return code
        return ""
