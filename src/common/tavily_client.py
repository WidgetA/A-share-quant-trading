# === MODULE PURPOSE ===
# Thin async client for Tavily Search API.
# Used by NegativeNewsChecker to search for recent stock-related news.

# === DEPENDENCIES ===
# - httpx: Async HTTP client (already a project dependency)

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)

TAVILY_SEARCH_URL = "https://api.tavily.com/search"


@dataclass
class TavilySearchResult:
    """A single search result from Tavily."""

    title: str
    url: str
    content: str  # snippet
    score: float


@dataclass
class TavilySearchResponse:
    """Full response from Tavily search."""

    query: str
    results: list[TavilySearchResult] = field(default_factory=list)


class TavilyClient:
    """
    Async client for Tavily Search API.

    Usage:
        client = TavilyClient(api_key="tvly-xxx")
        await client.start()
        response = await client.search("600519 贵州茅台 负面新闻", max_results=5, days=3)
        await client.stop()
    """

    def __init__(self, api_key: str, timeout: float = 30.0):
        self._api_key = api_key
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=self._timeout)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def search(
        self,
        query: str,
        max_results: int = 5,
        days: int = 3,
        search_depth: str = "basic",
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> TavilySearchResponse:
        """
        Search Tavily for recent web results.

        Args:
            query: Search query string.
            max_results: Maximum number of results to return.
            days: Limit results to last N days (ignored if start_date/end_date set).
            search_depth: "basic" or "advanced".
            start_date: Filter results after this date (YYYY-MM-DD).
            end_date: Filter results before this date (YYYY-MM-DD).

        Returns:
            TavilySearchResponse with results.

        Raises:
            httpx.HTTPStatusError: If API returns error.
            RuntimeError: If client not started.
        """
        if not self._client:
            raise RuntimeError("TavilyClient not started. Call start() first.")

        payload: dict = {
            "api_key": self._api_key,
            "query": query,
            "max_results": max_results,
            "search_depth": search_depth,
            "include_answer": False,
        }
        if start_date and end_date:
            payload["start_date"] = start_date
            payload["end_date"] = end_date
        else:
            payload["days"] = days

        resp = await self._client.post(TAVILY_SEARCH_URL, json=payload)
        resp.raise_for_status()
        data = resp.json()

        results = [
            TavilySearchResult(
                title=r.get("title", ""),
                url=r.get("url", ""),
                content=r.get("content", ""),
                score=r.get("score", 0.0),
            )
            for r in data.get("results", [])
        ]

        return TavilySearchResponse(query=query, results=results)
