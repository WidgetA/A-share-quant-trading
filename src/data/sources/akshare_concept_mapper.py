# === MODULE PURPOSE ===
# Maps stocks to/from concept boards using East Money (东财) data via akshare
# and direct EM API. Drop-in replacement for ConceptMapper when iFinD is unavailable.

# === DEPENDENCIES ===
# - akshare: stock_board_concept_cons_em() for board→stocks
# - East Money API: datacenter API for stock→boards (reverse lookup)
# - board_filter: Reuses same junk board filtering as ConceptMapper

# === KEY CONCEPTS ===
# - Uses 东财 concept boards (different names from 同花顺/THS boards)
# - On-the-fly API calls with in-memory session cache (no upfront bulk download)
# - ConceptMapper interface: batch_get_stock_concepts, batch_get_board_stocks

from __future__ import annotations

import asyncio
import logging

import httpx

from src.strategy.filters.board_filter import filter_boards

logger = logging.getLogger(__name__)

_CONCURRENCY_LIMIT = 10

# East Money API for stock → concept boards reverse lookup
_EM_BOARD_API = "https://datacenter.eastmoney.com/securities/api/data/v1/get"


class AkshareConceptMapper:
    """
    Maps stocks to/from concept boards via East Money data.

    Same interface as ConceptMapper so MomentumSectorScanner works
    without modification.

    Caching:
        Results are cached in memory for the session. No disk I/O.
    """

    def __init__(self) -> None:
        self._stock_concepts_cache: dict[str, list[str]] = {}
        self._board_stocks_cache: dict[str, list[tuple[str, str]]] = {}

    def clear_cache(self) -> None:
        self._stock_concepts_cache.clear()
        self._board_stocks_cache.clear()

    async def get_stock_concepts(self, stock_code: str) -> list[str]:
        """Get concept boards that a stock belongs to (via East Money API)."""
        bare = _normalize_code(stock_code)
        if not bare:
            return []

        if bare in self._stock_concepts_cache:
            return self._stock_concepts_cache[bare]

        try:
            boards = await _fetch_stock_boards_em(bare)
            filtered = filter_boards(boards)
            self._stock_concepts_cache[bare] = filtered
            logger.debug(f"{bare}: {len(boards)} EM concepts -> {len(filtered)} after filter")
            return filtered
        except Exception as e:
            logger.error(f"Failed to get EM concepts for {bare}: {e}")
            return []

    async def batch_get_stock_concepts(self, stock_codes: list[str]) -> dict[str, list[str]]:
        """Batch reverse lookup with concurrency control."""
        results: dict[str, list[str]] = {}
        semaphore = asyncio.Semaphore(_CONCURRENCY_LIMIT)

        async def _lookup(code: str) -> None:
            async with semaphore:
                concepts = await self.get_stock_concepts(code)
                results[_normalize_code(code)] = concepts

        tasks = [_lookup(code) for code in stock_codes]
        await asyncio.gather(*tasks)
        return results

    async def get_board_stocks(self, board_name: str) -> list[tuple[str, str]]:
        """Get all constituent stocks of a concept board (via akshare)."""
        if board_name in self._board_stocks_cache:
            return self._board_stocks_cache[board_name]

        try:
            stocks = await _fetch_board_constituents_ak(board_name)
            self._board_stocks_cache[board_name] = stocks
            logger.debug(f"EM Board '{board_name}': {len(stocks)} constituent stocks")
            return stocks
        except Exception as e:
            logger.error(f"Failed to get EM constituents for '{board_name}': {e}")
            return []

    async def batch_get_board_stocks(
        self, board_names: list[str]
    ) -> dict[str, list[tuple[str, str]]]:
        """Batch get constituent stocks for multiple boards."""
        results: dict[str, list[tuple[str, str]]] = {}
        semaphore = asyncio.Semaphore(_CONCURRENCY_LIMIT)

        async def _lookup(name: str) -> None:
            async with semaphore:
                stocks = await self.get_board_stocks(name)
                results[name] = stocks

        tasks = [_lookup(name) for name in board_names]
        await asyncio.gather(*tasks)
        return results


async def _fetch_stock_boards_em(bare_code: str) -> list[str]:
    """
    Fetch concept boards for a stock via East Money datacenter API.

    This is the reverse lookup: stock_code → [board_name, ...].
    Very fast (~0.2s per call).
    """
    suffix = ".SH" if bare_code.startswith("6") else ".SZ"
    secucode = f"{bare_code}{suffix}"

    params = {
        "reportName": "RPT_F10_CORETHEME_BOARDTYPE",
        "columns": "SECUCODE,BOARD_NAME,BOARD_CODE",
        "quoteColumns": "",
        "filter": f'(SECUCODE="{secucode}")',
        "pageNumber": "1",
        "pageSize": "100",
        "sortTypes": "-1",
        "sortColumns": "BOARD_RANK",
        "source": "HSF10",
        "client": "PC",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(_EM_BOARD_API, params=params)
        resp.raise_for_status()
        data = resp.json()

    boards: list[str] = []
    result = data.get("result")
    if result and result.get("data"):
        for item in result["data"]:
            name = item.get("BOARD_NAME", "")
            if name:
                boards.append(name)

    return boards


async def _fetch_board_constituents_ak(board_name: str) -> list[tuple[str, str]]:
    """
    Fetch constituent stocks of a concept board via akshare.

    Returns list of (bare_code, stock_name).
    """
    import akshare as ak

    df = await asyncio.to_thread(ak.stock_board_concept_cons_em, symbol=board_name)
    if df is None or df.empty:
        return []

    stocks: list[tuple[str, str]] = []
    for _, row in df.iterrows():
        code = str(row.get("代码", "")).strip()
        name = str(row.get("名称", "")).strip()
        bare = _normalize_code(code)
        if bare:
            stocks.append((bare, name))

    return stocks


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
